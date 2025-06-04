//! raftmem_rs 0.2 — **embedded Python service + shared NumPy array**
//!
//! One `import raftmem` call boots a background Tokio runtime **inside the
//! Python process**.  You get:
//!   • A configurable‑length `f64` ring mapped into NumPy zero‑copy.
//!   • Transparent replication to peer nodes over TCP using simple
//!     leader‑less broadcast (good‑enough demo).  Change one element → every
//!     node converges.
//!
//! ## Python demo (two machines / two shells)
//! ```bash
//! # shell A (machine A)
//! python - <<'PY'
//! import raftmem, numpy as np, random, time
//! node = raftmem.start(name="a", listen="0.0.0.0:7000", peers=["nodeB:7001"])
//! arr  = node.ndarray     # shared   [f64; n]
//! while True:
//!     n = len(arr)
//!     idx = random.randrange(n); val = random.random()*10
//!     arr[idx] = val       # ← real NumPy write, replicated
//!     node.flush(idx)      # tell raftmem to broadcast dirty slot
//!     print("A", arr)
//!     time.sleep(1)
//! PY
//! ```
//! ```bash
//! # shell B (machine B)
//! python - <<'PY'
//! import raftmem, numpy as np, random, time
//! node = raftmem.start(name="b", listen="0.0.0.0:7001", peers=["nodeA:7000"])
//! arr  = node.ndarray
//! while True:
//!     print("B", arr)     # will follow writes from A
//!     time.sleep(1)
//! PY
//! ```
//!
//! ---------------------------------------------------------------------------
//! Cargo.toml (abbreviated)
//! ---------------------------------------------------------------------------
//! [dependencies]
//! pyo3        = { version = "0.20", features=["extension-module", "auto-initialize"] }
//! numpy       = "0.19"
//! tokio       = { version="1", features=["rt", "macros", "net"] }
//! anyhow      = "1"
//! serde       = { version="1", features=["derive"] }
//! serde_json  = "1"
//! parking_lot = "0.12"
//! once_cell   = "1"
//! ---------------------------------------------------------------------------
//! src/lib.rs — full single‑file library
//! ---------------------------------------------------------------------------

use anyhow::Result;
use memmap2::{MmapMut, MmapOptions};
use libc::{mprotect, PROT_READ, PROT_WRITE};
use once_cell::sync::Lazy;
use numpy::{Element, PyArray1};
use numpy::npyffi::{PY_ARRAY_API, NpyTypes, NPY_ARRAY_WRITEABLE, npy_intp};
use pyo3::prelude::*;
use pyo3::types::PyModule;
use pyo3::exceptions::PyRuntimeError;
use pyo3::exceptions::PyValueError;
use parking_lot::{RwLock, RwLockReadGuard, RwLockWriteGuard};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use std::net::SocketAddr;
use tokio::{net::{TcpListener, TcpStream, UdpSocket}, runtime::Runtime};
use std::io::ErrorKind;
use std::sync::Arc;
use std::os::raw::{c_int, c_void};
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

// ---------- global Tokio runtime (one per process) -------------------------
static RUNTIME: Lazy<Runtime> = Lazy::new(|| Runtime::new().expect("tokio"));

// ---------- in‑memory state -------------------------------------------------
struct MmapBuf {
    mm: MmapMut,
    shape: Vec<usize>,
    len: usize,
}

impl MmapBuf {
    fn new(shape: Vec<usize>) -> Result<Self> {
        let len: usize = shape.iter().product();
        let layout = len * std::mem::size_of::<f64>();
        let mm = MmapOptions::new().len(layout).map_anon()?;
        Ok(Self { mm, shape, len })
    }

    fn ptr(&self) -> *mut c_void {
        self.mm.as_ptr() as *mut _
    }

    fn byte_len(&self) -> usize {
        self.mm.len()
    }

    fn elems(&self) -> usize {
        self.len
    }

    fn shape(&self) -> &[usize] {
        &self.shape
    }
}

#[derive(Clone)]
struct Shared {
    mm: Arc<MmapBuf>,
    lock: Arc<RwLock<()>>,    // reader/writer lock for memory access
}

impl Shared {
    fn new(mm: MmapBuf) -> Self {
        Self { mm: Arc::new(mm), lock: Arc::new(RwLock::new(())) }
    }

    fn apply(&self, idx: usize, val: f64) {
        if idx < self.mm.elems() {
            unsafe {
                let base = self.mm.mm.as_ptr() as *mut f64;
                *base.add(idx) = val;
            }
        }
    }

    fn get(&self, idx: usize) -> f64 {
        unsafe {
            let base = self.mm.mm.as_ptr() as *const f64;
            *base.add(idx)
        }
    }

    fn elems(&self) -> usize {
        self.mm.elems()
    }

    fn shape(&self) -> &[usize] {
        self.mm.shape()
    }
}

// ---------- wire protocol ---------------------------------------------------
#[derive(Debug, Clone)]
struct FullUpdate {
    shape: Vec<u32>,
    arr: Vec<f64>,
}

#[derive(Debug, Clone)]
struct DiffChunk {
    start: u32,
    vals: Vec<f64>,
}

#[derive(Debug, Clone)]
struct DiffUpdate {
    chunks: Vec<DiffChunk>,
}

#[derive(Debug, Clone)]
enum WireMsg {
    Full(FullUpdate),
    Diff(DiffUpdate),
}

#[derive(Debug, Clone)]
enum RaftMsg {
    Heartbeat { term: u64, from: String },
    RequestVote { term: u64, from: String },
    Vote { term: u64, from: String, granted: bool },
}

impl RaftMsg {
    fn to_bytes(&self) -> Vec<u8> {
        match self {
            RaftMsg::Heartbeat { term, from } => {
                format!("H,{},{}\n", term, from).into_bytes()
            }
            RaftMsg::RequestVote { term, from } => {
                format!("R,{},{}\n", term, from).into_bytes()
            }
            RaftMsg::Vote { term, from, granted } => {
                format!("V,{},{},{}\n", term, from, if *granted { 1 } else { 0 }).into_bytes()
            }
        }
    }

    fn from_bytes(data: &[u8]) -> Option<Self> {
        let s = std::str::from_utf8(data).ok()?;
        let parts: Vec<&str> = s.trim_end().split(',').collect();
        match parts.get(0)? {
            &"H" => Some(RaftMsg::Heartbeat {
                term: parts.get(1)?.parse().ok()?,
                from: parts.get(2)?.to_string(),
            }),
            &"R" => Some(RaftMsg::RequestVote {
                term: parts.get(1)?.parse().ok()?,
                from: parts.get(2)?.to_string(),
            }),
            &"V" => Some(RaftMsg::Vote {
                term: parts.get(1)?.parse().ok()?,
                from: parts.get(2)?.to_string(),
                granted: parts.get(3).map_or(false, |v| *v == "1"),
            }),
            _ => None,
        }
    }
}

#[derive(PartialEq)]
enum Role { Follower, Candidate, Leader }

struct RaftState {
    id: String,
    term: u64,
    role: Role,
    voted_for: Option<String>,
    peers: Vec<SocketAddr>,
    is_leader: Arc<AtomicBool>,
}

impl WireMsg {
    fn to_bytes(&self) -> Vec<u8> {
        match self {
            WireMsg::Full(u) => {
                let shape_len = u.shape.len() as u32;
                let mut buf = Vec::with_capacity(1 + 4 + u.shape.len() * 4 + u.arr.len() * 8);
                buf.push(0);
                buf.extend_from_slice(&shape_len.to_le_bytes());
                for d in &u.shape {
                    buf.extend_from_slice(&d.to_le_bytes());
                }
                for v in &u.arr {
                    buf.extend_from_slice(&v.to_le_bytes());
                }
                buf
            }
            WireMsg::Diff(d) => {
                let n = d.chunks.len() as u32;
                let mut buf = Vec::new();
                buf.push(1);
                buf.extend_from_slice(&n.to_le_bytes());
                for ch in &d.chunks {
                    buf.extend_from_slice(&ch.start.to_le_bytes());
                    let len = ch.vals.len() as u32;
                    buf.extend_from_slice(&len.to_le_bytes());
                    for v in &ch.vals {
                        buf.extend_from_slice(&v.to_le_bytes());
                    }
                }
                buf
            }
        }
    }
}

async fn handle_peer(mut sock: TcpStream, state: Shared, update_cb: Option<PyObject>) -> Result<()> {
    let addr = sock.peer_addr().ok();
    println!("peer {:?} connected", addr);
    loop {
        let mut msg_type = [0u8; 1];
        match sock.read_exact(&mut msg_type).await {
            Ok(_) => {}
            Err(ref e) if e.kind() == ErrorKind::UnexpectedEof => break,
            Err(ref e) if e.kind() == ErrorKind::ConnectionReset => break,
            Err(e) => return Err(e.into()),
        }
        if msg_type[0] == 0 {
            let mut shape_len_buf = [0u8; 4];
            if sock.read_exact(&mut shape_len_buf).await.is_err() { break; }
            let shape_len = u32::from_le_bytes(shape_len_buf) as usize;
            let mut shape_bytes = vec![0u8; shape_len * 4];
            if sock.read_exact(&mut shape_bytes).await.is_err() { break; }
            let mut shape = Vec::with_capacity(shape_len);
            for i in 0..shape_len {
                let mut d = [0u8; 4];
                d.copy_from_slice(&shape_bytes[i * 4..(i + 1) * 4]);
                shape.push(u32::from_le_bytes(d) as usize);
            }
            let len: usize = shape.iter().product();
            let mut data = vec![0u8; len * 8];
            if sock.read_exact(&mut data).await.is_err() { break; }
            let mut arr = Vec::with_capacity(len);
            for i in 0..len {
                let mut val_bytes = [0u8; 8];
                val_bytes.copy_from_slice(&data[i * 8..(i + 1) * 8]);
                arr.push(f64::from_le_bytes(val_bytes));
            }
            if shape != state.shape() { continue; }
            let mut guard = state.lock.write();
            let lenb = state.mm.byte_len();
            unsafe { mprotect(state.mm.mm.as_ptr() as *mut _, lenb, PROT_READ | PROT_WRITE); }
            for (i, v) in arr.iter().enumerate() { state.apply(i, *v); }
            unsafe { mprotect(state.mm.mm.as_ptr() as *mut _, lenb, PROT_READ); }
            drop(guard);
            if let Some(cb) = &update_cb {
                Python::with_gil(|py| { let _ = cb.call0(py); });
            }
        } else {
            let mut count_buf = [0u8; 4];
            if sock.read_exact(&mut count_buf).await.is_err() { break; }
            let n = u32::from_le_bytes(count_buf) as usize;
            let mut chunks = Vec::with_capacity(n);
            for _ in 0..n {
                let mut start_buf = [0u8;4];
                if sock.read_exact(&mut start_buf).await.is_err() { break; }
                let start = u32::from_le_bytes(start_buf) as usize;
                let mut len_buf = [0u8;4];
                if sock.read_exact(&mut len_buf).await.is_err() { break; }
                let len = u32::from_le_bytes(len_buf) as usize;
                let mut vals_bytes = vec![0u8; len * 8];
                if sock.read_exact(&mut vals_bytes).await.is_err() { break; }
                let mut vals = Vec::with_capacity(len);
                for i in 0..len {
                    let mut vb = [0u8;8];
                    vb.copy_from_slice(&vals_bytes[i*8..(i+1)*8]);
                    vals.push(f64::from_le_bytes(vb));
                }
                chunks.push((start, vals));
            }
            let mut guard = state.lock.write();
            let lenb = state.mm.byte_len();
            unsafe { mprotect(state.mm.mm.as_ptr() as *mut _, lenb, PROT_READ | PROT_WRITE); }
            for (start, vals) in chunks {
                for (off, v) in vals.iter().enumerate() {
                    state.apply(start + off, *v);
                }
            }
            unsafe { mprotect(state.mm.mm.as_ptr() as *mut _, lenb, PROT_READ); }
            drop(guard);
            if let Some(cb) = &update_cb {
                Python::with_gil(|py| { let _ = cb.call0(py); });
            }
        }
    }
    println!("peer {:?} disconnected", addr);
    Ok(())
}

async fn broadcaster(peers: Vec<SocketAddr>, rx: async_channel::Receiver<WireMsg>) -> Result<()> {
    let mut conns: Vec<(SocketAddr, TcpStream)> = Vec::new();
    for p in &peers {
        println!("connecting to peer {}", p);
        match TcpStream::connect(*p).await {
            Ok(s) => {
                println!("connected to {}", p);
                conns.push((*p, s));
            }
            Err(e) => println!("failed to connect to {}: {}", p, e),
        }
    }
    while let Some(u) = rx.recv().await.ok() {
        let data = u.to_bytes();

        for p in &peers {
            if !conns.iter().any(|(addr, _)| addr == p) {
                println!("reconnecting to {}", p);
                match TcpStream::connect(*p).await {
                    Ok(s) => {
                        println!("connected to {}", p);
                        conns.push((*p, s));
                    }
                    Err(e) => println!("connect failed to {}: {}", p, e),
                }
            }
        }

        let mut alive = Vec::new();
        for (addr, mut s) in conns {
            match s.write_all(&data).await {
                Ok(_) => {
                    println!("sent to {}", addr);
                    alive.push((addr, s));
                }
                Err(e) => {
                    println!("send failed to {}: {}", addr, e);
                }
            }
        }
        conns = alive;
    }
    Ok(())
}

fn port_plus(addr: SocketAddr, off: u16) -> SocketAddr {
    if let SocketAddr::V4(mut a) = addr { a.set_port(a.port() + off); SocketAddr::V4(a) }
    else if let SocketAddr::V6(mut a) = addr { a.set_port(a.port() + off); SocketAddr::V6(a) }
    else { addr }
}

fn rand_timeout() -> Duration {
    let n = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().subsec_nanos() % 150;
    Duration::from_millis(150 + n as u64)
}

async fn raft_service(addr: SocketAddr, peers: Vec<SocketAddr>, st: Arc<RwLock<RaftState>>, cb: Option<PyObject>) -> Result<()> {
    let sock = UdpSocket::bind(addr).await?;
    let mut buf = [0u8; 512];
    let mut deadline = tokio::time::Instant::now() + rand_timeout();
    let mut hb = tokio::time::interval(Duration::from_millis(50));
    loop {
        tokio::select! {
            Ok((n,_)) = sock.recv_from(&mut buf) => {
                if let Some(msg) = RaftMsg::from_bytes(&buf[..n]) {
                    if let RaftMsg::Heartbeat { term, .. } = msg {
                        let mut s = st.write();
                        if term >= s.term { s.term = term; s.role = Role::Follower; s.is_leader.store(false, Ordering::SeqCst); }
                        deadline = tokio::time::Instant::now() + rand_timeout();
                    }
                }
            }
            _ = hb.tick(), if { st.read().role == Role::Leader } => {
                let term = { st.read().term };
                let id = { st.read().id.clone() };
                let data = RaftMsg::Heartbeat { term, from: id }.to_bytes();
                for p in &peers { let _ = sock.send_to(&data, p).await; }
            }
            _ = tokio::time::sleep_until(deadline) => {
                let mut s = st.write();
                s.term += 1;
                s.role = Role::Leader;
                s.is_leader.store(true, Ordering::SeqCst);
                deadline = tokio::time::Instant::now() + rand_timeout();
                let term = s.term;
                let id = s.id.clone();
                drop(s);
                if let Some(cb) = &cb { Python::with_gil(|py| { let _ = cb.call0(py); }); }
                let data = RaftMsg::Heartbeat { term, from: id }.to_bytes();
                for p in &peers { let _ = sock.send_to(&data, p).await; }
            }
        }
    }
}

async fn listener(addr: SocketAddr, state: Shared, cb: Option<PyObject>) -> Result<()> {
    println!("listening on {}", addr);
    let lst = TcpListener::bind(addr).await?;
    loop {
        let (sock, peer) = lst.accept().await?;
        println!("accepted connection from {}", peer);
        let st = state.clone();
        let cb_clone = cb.clone();
        tokio::spawn(async move { let _ = handle_peer(sock, st, cb_clone).await; });
    }
}

// ---------- exposed Python face -------------------------------------------
#[pyclass]
struct Node {
    name: String,
    state: Shared,
    tx: async_channel::Sender<WireMsg>,
    shape: Vec<usize>,
    len: usize,
    is_leader: Arc<AtomicBool>,
    leader_cb: Option<PyObject>,
    update_cb: Option<PyObject>,
}

#[pymethods]
impl Node {
    #[getter]
    fn ndarray<'py>(&'py self, py: Python<'py>) -> &'py PyArray1<f64> {
        let dims: [npy_intp; 1] = [self.len as npy_intp];
        let strides: [npy_intp; 1] = [std::mem::size_of::<f64>() as npy_intp];
        unsafe {
            let subtype = PY_ARRAY_API.get_type_object(py, NpyTypes::PyArray_Type);
            let descr = <f64 as Element>::get_dtype(py).into_dtype_ptr();
            let arr_ptr = PY_ARRAY_API.PyArray_NewFromDescr(
                py,
                subtype,
                descr,
                dims.len() as c_int,
                dims.as_ptr() as *mut npy_intp,
                strides.as_ptr() as *mut npy_intp,
                self.state.mm.ptr(),
                NPY_ARRAY_WRITEABLE,
                std::ptr::null_mut(),
            );
            PyArray1::from_owned_ptr(py, arr_ptr)
        }
    }

    #[getter]
    fn leader(&self) -> bool {
        self.is_leader.load(Ordering::SeqCst)
    }
    fn flush(&self, _idx: usize) {
        let mut arr = Vec::with_capacity(self.len);
        for i in 0..self.len {
            arr.push(self.state.get(i));
        }
        println!("{} flushing full array", self.name);
        let shape: Vec<u32> = self.shape.iter().map(|&d| d as u32).collect();
        let _ = self.tx.try_send(WireMsg::Full(FullUpdate { shape, arr }));
    }

    fn write<'py>(slf: PyRef<'py, Self>) -> PyResult<WriteGuard> {
        if !slf.is_leader.load(Ordering::SeqCst) {
            return Err(PyRuntimeError::new_err("only leader may obtain write guard"));
        }
        let guard = slf.state.lock.write();
        let len_bytes = slf.state.mm.byte_len();
        unsafe {
            let ret = mprotect(slf.state.mm.mm.as_ptr() as *mut _, len_bytes, PROT_READ | PROT_WRITE);
            if ret != 0 {
                return Err(PyRuntimeError::new_err("mprotect(PROT_READ|WRITE) failed in write()"));
            }
        }
        let size = slf.len.next_power_of_two();
        let any_tree = vec![false; 2 * size];
        let all_tree = vec![false; 2 * size];
        let guard: RwLockWriteGuard<'static, ()> = unsafe { std::mem::transmute::<RwLockWriteGuard<'_, ()>, RwLockWriteGuard<'static, ()>>(guard) };
        Ok(WriteGuard { node: slf.into(), guard: Some(guard), any_tree, all_tree, size })
    }

    fn read<'py>(slf: PyRef<'py, Self>) -> PyResult<ReadGuard> {
        let guard = slf.state.lock.read();
        let len = slf.state.mm.byte_len();
        unsafe {
            let ret = mprotect(slf.state.mm.mm.as_ptr() as *mut _, len, PROT_READ);
            if ret != 0 {
                return Err(PyRuntimeError::new_err("mprotect(PROT_READ) failed in read()"));
            }
        }
        let guard: RwLockReadGuard<'static, ()> = unsafe { std::mem::transmute::<RwLockReadGuard<'_, ()>, RwLockReadGuard<'static, ()>>(guard) };
        Ok(ReadGuard { node: slf.into(), guard: Some(guard) })
    }
}

#[pyclass(unsendable)]
struct WriteGuard {
    node: Py<Node>,
    guard: Option<RwLockWriteGuard<'static, ()>>,
    any_tree: Vec<bool>,
    all_tree: Vec<bool>,
    size: usize,
}

#[pymethods]
impl WriteGuard {
    #[getter]
    fn ndarray<'py>(&self, py: Python<'py>) -> &'py PyArray1<f64> {
        let cell = self.node.as_ref(py).borrow();
        let dims: [npy_intp; 1] = [cell.len as npy_intp];
        let strides: [npy_intp; 1] = [std::mem::size_of::<f64>() as npy_intp];
        unsafe {
            let subtype = PY_ARRAY_API.get_type_object(py, NpyTypes::PyArray_Type);
            let descr = <f64 as Element>::get_dtype(py).into_dtype_ptr();
            let arr_ptr = PY_ARRAY_API.PyArray_NewFromDescr(
                py,
                subtype,
                descr,
                dims.len() as c_int,
                dims.as_ptr() as *mut npy_intp,
                strides.as_ptr() as *mut npy_intp,
                cell.state.mm.ptr(),
                NPY_ARRAY_WRITEABLE,
                cell.as_ptr(),
            );
            PyArray1::from_owned_ptr(py, arr_ptr)
        }
    }

    fn dirty(&mut self, idx: usize) {
        if idx >= self.size { return; }
        let mut i = idx + self.size;
        self.any_tree[i] = true;
        self.all_tree[i] = true;
        while i > 1 {
            i /= 2;
            let left = i * 2;
            let right = left + 1;
            self.any_tree[i] = self.any_tree[left] || self.any_tree[right];
            self.all_tree[i] = self.all_tree[left] && self.all_tree[right];
        }
    }

    fn __enter__(slf: PyRefMut<'_, Self>) -> PyResult<Py<WriteGuard>> {
        Ok(slf.into())
    }

    fn __exit__(&mut self, _t: &PyAny, _v: &PyAny, _tb: &PyAny) -> PyResult<()> {
        Python::with_gil(|py| {
            let cell = self.node.as_ref(py).borrow();
            fn collect(idx: usize, start: usize, len: usize, len_orig: usize,
                       any_tree: &[bool], all_tree: &[bool], out: &mut Vec<(usize, usize)>) {
                if !any_tree[idx] { return; }
                if len == 1 || all_tree[idx] {
                    let l = len.min(len_orig - start);
                    if l > 0 { out.push((start, l)); }
                } else {
                    let half = len / 2;
                    collect(idx * 2, start, half, len_orig, any_tree, all_tree, out);
                    collect(idx * 2 + 1, start + half, half, len_orig, any_tree, all_tree, out);
                }
            }

            let len = cell.len;
            let mut ranges = Vec::new();
            collect(1, 0, self.size, len, &self.any_tree, &self.all_tree, &mut ranges);

            let mut chunks = Vec::new();
            for (start, l) in ranges {
                let mut vals = Vec::with_capacity(l);
                for i in 0..l { vals.push(cell.state.get(start + i)); }
                chunks.push(DiffChunk { start: start as u32, vals });
            }
            if cell.is_leader.load(Ordering::SeqCst) && !chunks.is_empty() {
                let _ = cell.tx.try_send(WireMsg::Diff(DiffUpdate { chunks }));
            }
            if let Some(cb) = &cell.update_cb {
                let _ = cb.call0(py);
            }
            let len = cell.state.mm.byte_len();
            let ret = unsafe { mprotect(cell.state.mm.mm.as_ptr() as *mut _, len, PROT_READ) };
            if ret != 0 {
                return Err(PyRuntimeError::new_err("mprotect(PROT_READ) failed in write() exit"));
            }
            Ok(())
        })?;
        // drop the write lock
        if let Some(g) = self.guard.take() {
            drop(g);
        }
        Ok(())
    }
}

#[pyclass(unsendable)]
struct ReadGuard {
    node: Py<Node>,
    guard: Option<RwLockReadGuard<'static, ()>>,
}

#[pymethods]
impl ReadGuard {
    fn __enter__<'py>(slf: PyRefMut<'py, Self>, py: Python<'py>) -> &'py PyArray1<f64> {
        let cell = slf.node.as_ref(py).borrow();
        let dims: [npy_intp; 1] = [cell.len as npy_intp];
        let strides: [npy_intp; 1] = [std::mem::size_of::<f64>() as npy_intp];
        unsafe {
            let subtype = PY_ARRAY_API.get_type_object(py, NpyTypes::PyArray_Type);
            let descr = <f64 as Element>::get_dtype(py).into_dtype_ptr();
            let arr_ptr = PY_ARRAY_API.PyArray_NewFromDescr(
                py,
                subtype,
                descr,
                dims.len() as c_int,
                dims.as_ptr() as *mut npy_intp,
                strides.as_ptr() as *mut npy_intp,
                cell.state.mm.ptr(),
                0,
                cell.as_ptr(),
            );
            PyArray1::from_owned_ptr(py, arr_ptr)
        }
    }

    fn __exit__(&mut self, _t: &PyAny, _v: &PyAny, _tb: &PyAny) -> PyResult<()> {
        Python::with_gil(|py| {
            let cell = self.node.as_ref(py).borrow();
            let len = cell.state.mm.byte_len();
            let ret = unsafe { mprotect(cell.state.mm.mm.as_ptr() as *mut _, len, PROT_READ) };
            if ret != 0 {
                return Err(PyRuntimeError::new_err("mprotect(PROT_READ) failed in read() exit"));
            }
            Ok(())
        })?;
        if let Some(g) = self.guard.take() {
            drop(g);
        }
        Ok(())
    }
}

#[pyfunction]
#[pyo3(signature = (name, listen, peers, shape=None, on_leader=None, on_update=None))]
fn start(_py: Python<'_>, name: &str, listen: &str, peers: Vec<&str>, shape: Option<Vec<usize>>, on_leader: Option<PyObject>, on_update: Option<PyObject>) -> PyResult<Node> {
    let shape = shape.unwrap_or_else(|| vec![10]);
    let len: usize = shape.iter().product();
    let buf = MmapBuf::new(shape.clone()).map_err(|e| PyValueError::new_err(e.to_string()))?;
    let state = Shared::new(buf);
    let (tx, rx) = async_channel::bounded(1024);
    let is_leader = Arc::new(AtomicBool::new(false));

    let listen_addr: SocketAddr = listen.parse()
        .map_err(|e: std::net::AddrParseError| PyValueError::new_err(e.to_string()))?;
    let peer_addrs: Vec<SocketAddr> = peers.into_iter().filter_map(|p| p.parse().ok()).collect();

    let st_clone = state.clone();
    RUNTIME.spawn(listener(listen_addr, st_clone, on_update.clone()));
    RUNTIME.spawn(broadcaster(peer_addrs.clone(), rx));

    let raft_addr = port_plus(listen_addr, 1000);
    let peer_raft_addrs: Vec<SocketAddr> = peer_addrs.iter().map(|p| port_plus(*p, 1000)).collect();
    let raft_state = Arc::new(RwLock::new(RaftState { id: name.to_string(), term: 0, role: Role::Follower, voted_for: None, peers: peer_raft_addrs.clone(), is_leader: is_leader.clone() }));
    let cb_for_task = on_leader.clone();
    RUNTIME.spawn(raft_service(raft_addr, peer_raft_addrs, raft_state, cb_for_task));

    unsafe {
        let len0 = state.mm.byte_len();
        let ret = mprotect(state.mm.mm.as_ptr() as *mut _, len0, PROT_READ);
        if ret != 0 {
            return Err(PyRuntimeError::new_err("mprotect(PROT_READ) failed in start()"));
        }
    }

    println!("node {} running on {} with shape {:?}", name, listen, shape);
    Ok(Node { name: name.to_string(), state, tx, shape, len, is_leader, leader_cb: on_leader, update_cb: on_update })
}

// ---------- module init ----------------------------------------------------
#[pymodule]
fn raftmem(_py: Python<'_>, m: &PyModule) -> PyResult<()> {
    m.add_class::<Node>()?;
    m.add_class::<WriteGuard>()?;
    m.add_class::<ReadGuard>()?;
    m.add_function(wrap_pyfunction!(start, m)?)?;
    Ok(())
}

