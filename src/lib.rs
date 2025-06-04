//! raftmem_rs 0.2 — **embedded Python service + shared NumPy array**
//!
//! One `import raftmem` call boots a background Tokio runtime **inside the
//! Python process**.  You get:
//!   • A 10‑element `f64` ring mapped into NumPy zero‑copy.
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
//! arr  = node.ndarray     # shared   [f64;10]
//! while True:
//!     idx = random.randrange(10); val = random.random()*10
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
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use std::net::SocketAddr;
use tokio::{net::{TcpListener, TcpStream}, runtime::Runtime};
use std::io::ErrorKind;
use std::sync::Arc;
use std::os::raw::{c_int, c_void};

// ---------- global Tokio runtime (one per process) -------------------------
static RUNTIME: Lazy<Runtime> = Lazy::new(|| Runtime::new().expect("tokio"));

// ---------- in‑memory state -------------------------------------------------
struct MmapBuf {
    mm: MmapMut,
}

impl MmapBuf {
    fn new() -> Result<Self> {
        let layout = 10 * std::mem::size_of::<f64>();
        let mm = MmapOptions::new().len(layout).map_anon()?;
        Ok(Self { mm })
    }

    fn ptr(&self) -> *mut c_void {
        self.mm.as_ptr() as *mut _
    }

    fn len(&self) -> usize {
        self.mm.len()
    }
}

#[derive(Clone)]
struct Shared(Arc<MmapBuf>);

impl Shared {
    fn apply(&self, idx: usize, val: f64) {
        if idx < 10 {
            unsafe {
                let base = self.0.mm.as_mut_ptr() as *mut f64;
                *base.add(idx) = val;
            }
        }
    }

    fn get(&self, idx: usize) -> f64 {
        unsafe {
            let base = self.0.mm.as_ptr() as *const f64;
            *base.add(idx)
        }
    }
}

// ---------- wire protocol ---------------------------------------------------
#[derive(Debug, Clone, Copy)]
struct Update { idx: u32, val: f64 }

impl Update {
    const SIZE: usize = 4 + 8; // u32 + f64

    fn to_bytes(&self) -> [u8; Self::SIZE] {
        let mut buf = [0u8; Self::SIZE];
        buf[..4].copy_from_slice(&self.idx.to_le_bytes());
        buf[4..].copy_from_slice(&self.val.to_le_bytes());
        buf
    }

    fn from_bytes(buf: &[u8; Self::SIZE]) -> Self {
        let mut idx_bytes = [0u8; 4];
        idx_bytes.copy_from_slice(&buf[..4]);
        let idx = u32::from_le_bytes(idx_bytes);

        let mut val_bytes = [0u8; 8];
        val_bytes.copy_from_slice(&buf[4..]);
        let val = f64::from_le_bytes(val_bytes);

        Self { idx, val }
    }
}

async fn handle_peer(mut sock: TcpStream, state: Shared) -> Result<()> {
    let addr = sock.peer_addr().ok();
    println!("peer {:?} connected", addr);
    let mut buf = [0u8; Update::SIZE];
    loop {
        match sock.read_exact(&mut buf).await {
            Ok(_) => {
                let upd = Update::from_bytes(&buf);
                println!("recv from {:?}: {} -> {}", addr, upd.idx, upd.val);
                state.apply(upd.idx as usize, upd.val);
            }
            Err(ref e) if e.kind() == ErrorKind::UnexpectedEof => break,
            Err(ref e) if e.kind() == ErrorKind::ConnectionReset => break,
            Err(e) => return Err(e.into()),
        }
    }
    println!("peer {:?} disconnected", addr);
    Ok(())
}

async fn broadcaster(peers: Vec<SocketAddr>, rx: async_channel::Receiver<Update>) -> Result<()> {
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
        println!("broadcasting {} -> {}", u.idx, u.val);
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

async fn listener(addr: SocketAddr, state: Shared) -> Result<()> {
    println!("listening on {}", addr);
    let lst = TcpListener::bind(addr).await?;
    loop {
        let (sock, peer) = lst.accept().await?;
        println!("accepted connection from {}", peer);
        let st = state.clone();
        tokio::spawn(async move { let _ = handle_peer(sock, st).await; });
    }
}

// ---------- exposed Python face -------------------------------------------
#[pyclass]
struct Node {
    name: String,
    state: Shared,
    tx: async_channel::Sender<Update>,
}

#[pymethods]
impl Node {
    #[getter]
    fn ndarray<'py>(&'py self, py: Python<'py>) -> &'py PyArray1<f64> {
        let dims: [npy_intp; 1] = [10];
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
                self.state.0.ptr(),
                NPY_ARRAY_WRITEABLE,
                std::ptr::null_mut(),
            );
            PyArray1::from_owned_ptr(py, arr_ptr)
        }
    }
    fn flush(&self, idx: usize) {
        let val = self.state.get(idx);
        println!("{} flushing idx {} with {}", self.name, idx, val);
        let _ = self.tx.try_send(Update { idx: idx as u32, val });
    }

    fn write<'py>(slf: PyRef<'py, Self>) -> PyResult<WriteGuard> {
        let len = slf.state.0.len();
        unsafe {
            let ret = mprotect(slf.state.0.mm.as_ptr() as *mut _, len, PROT_READ | PROT_WRITE);
            if ret != 0 {
                return Err(PyRuntimeError::new_err("mprotect(PROT_READ|WRITE) failed in write()"));
            }
        }
        Ok(WriteGuard { node: slf.into() })
    }

    fn read<'py>(slf: PyRef<'py, Self>) -> PyResult<ReadGuard> {
        let len = slf.state.0.len();
        unsafe {
            let ret = mprotect(slf.state.0.mm.as_ptr() as *mut _, len, PROT_READ);
            if ret != 0 {
                return Err(PyRuntimeError::new_err("mprotect(PROT_READ) failed in read()"));
            }
        }
        Ok(ReadGuard { node: slf.into() })
    }
}

fn flush_now(node: &Node) {
    for idx in 0..10 {
        node.flush(idx);
    }
}

#[pyclass]
struct WriteGuard {
    node: Py<Node>,
}

#[pymethods]
impl WriteGuard {
    fn __enter__<'py>(slf: PyRefMut<'py, Self>, py: Python<'py>) -> &'py PyArray1<f64> {
        let cell = slf.node.as_ref(py).borrow();
        let dims: [npy_intp; 1] = [10];
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
                cell.state.0.ptr(),
                NPY_ARRAY_WRITEABLE,
                cell.as_ptr(),
            );
            PyArray1::from_owned_ptr(py, arr_ptr)
        }
    }

    fn __exit__(&mut self, _t: &PyAny, _v: &PyAny, _tb: &PyAny) -> PyResult<()> {
        Python::with_gil(|py| {
            let cell = self.node.as_ref(py).borrow();
            flush_now(&*cell);
            let len = cell.state.0.len();
            let ret = unsafe { mprotect(cell.state.0.mm.as_ptr() as *mut _, len, PROT_READ) };
            if ret != 0 {
                return Err(PyRuntimeError::new_err("mprotect(PROT_READ) failed in write() exit"));
            }
            Ok(())
        })
    }
}

#[pyclass]
struct ReadGuard {
    node: Py<Node>,
}

#[pymethods]
impl ReadGuard {
    fn __enter__<'py>(slf: PyRefMut<'py, Self>, py: Python<'py>) -> &'py PyArray1<f64> {
        let cell = slf.node.as_ref(py).borrow();
        let dims: [npy_intp; 1] = [10];
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
                cell.state.0.ptr(),
                0,
                cell.as_ptr(),
            );
            PyArray1::from_owned_ptr(py, arr_ptr)
        }
    }

    fn __exit__(&mut self, _t: &PyAny, _v: &PyAny, _tb: &PyAny) -> PyResult<()> {
        Python::with_gil(|py| {
            let cell = self.node.as_ref(py).borrow();
            let len = cell.state.0.len();
            let ret = unsafe { mprotect(cell.state.0.mm.as_ptr() as *mut _, len, PROT_READ | PROT_WRITE) };
            if ret != 0 {
                return Err(PyRuntimeError::new_err("mprotect(PROT_READ|WRITE) failed in read() exit"));
            }
            Ok(())
        })
    }
}

#[pyfunction]
#[pyo3(signature = (name, listen, peers))]
fn start(py: Python<'_>, name: &str, listen: &str, peers: Vec<&str>) -> PyResult<Node> {
    let buf = MmapBuf::new().map_err(|e| PyValueError::new_err(e.to_string()))?;
    let state = Shared(Arc::new(buf));
    let (tx, rx) = async_channel::bounded(1024);

    let listen_addr: SocketAddr = listen.parse()
        .map_err(|e: std::net::AddrParseError| PyValueError::new_err(e.to_string()))?;
    let peer_addrs: Vec<SocketAddr> = peers.into_iter().filter_map(|p| p.parse().ok()).collect();

    let st_clone = state.clone();
    RUNTIME.spawn(listener(listen_addr, st_clone));
    RUNTIME.spawn(broadcaster(peer_addrs, rx));

    unsafe {
        let len0 = state.0.len();
        let ret = mprotect(state.0.mm.as_ptr() as *mut _, len0, PROT_READ);
        if ret != 0 {
            return Err(PyRuntimeError::new_err("mprotect(PROT_READ) failed in start()"));
        }
    }

    println!("node {} running on {}", name, listen);
    Ok(Node { name: name.to_string(), state, tx })
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

