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
use tokio::{net::{TcpListener, TcpStream}, runtime::Runtime};
use std::io::ErrorKind;
use std::sync::Arc;
use std::os::raw::{c_int, c_void};

pub mod ring;

// ---------- global Tokio runtime (one per process) -------------------------
static RUNTIME: Lazy<Runtime> = Lazy::new(|| Runtime::new().expect("tokio"));

// ---------- in‑memory state -------------------------------------------------
struct MmapBuf {
    mm: MmapMut,
    shape: Vec<usize>,
}

impl MmapBuf {
    fn new(shape: Vec<usize>) -> Result<Self> {
        let len: usize = shape.iter().product();
        let layout = len * std::mem::size_of::<f64>();
        let mm = MmapOptions::new().len(layout).map_anon()?;
        Ok(Self { mm, shape })
    }

    fn ptr(&self) -> *mut c_void {
        self.mm.as_ptr() as *mut _
    }

    fn byte_len(&self) -> usize {
        self.mm.len()
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

    fn get(&self, idx: usize) -> f64 {
        unsafe {
            let base = self.mm.mm.as_ptr() as *const f64;
            *base.add(idx)
        }
    }

    fn shape(&self) -> &[usize] {
        self.mm.shape()
    }
}

// ---------- wire protocol ---------------------------------------------------

async fn handle_peer(mut sock: TcpStream, state: Shared) -> Result<()> {
    let addr = sock.peer_addr().ok();
    println!("peer {:?} connected", addr);
    let local_bytes = state.mm.byte_len();

    loop {
        let mut hdr = [0u8; 16];
        match sock.read_exact(&mut hdr).await {
            Ok(_) => {}
            Err(ref e) if e.kind() == ErrorKind::UnexpectedEof => break,
            Err(ref e) if e.kind() == ErrorKind::ConnectionReset => break,
            Err(e) => return Err(e.into()),
        }

        let len = u32::from_le_bytes(hdr[8..12].try_into().unwrap()) as usize;
        let kind = hdr[12];
        let mut payload = vec![0u8; len];
        match sock.read_exact(&mut payload).await {
            Ok(_) => {}
            Err(ref e) if e.kind() == ErrorKind::UnexpectedEof => break,
            Err(ref e) if e.kind() == ErrorKind::ConnectionReset => break,
            Err(e) => return Err(e.into()),
        }

        match kind {
            1 => {
                if len != local_bytes {
                    println!("size mismatch: recv {} local {}", len, local_bytes);
                    continue;
                }
                let len0 = state.mm.byte_len();
                {
                    let _guard = state.lock.write();
                    unsafe { mprotect(state.mm.mm.as_ptr() as *mut _, len0, PROT_READ | PROT_WRITE); }
                }
                let dst: &mut [u8] = unsafe {
                    std::slice::from_raw_parts_mut(state.mm.mm.as_ptr() as *mut u8, local_bytes)
                };
                dst.copy_from_slice(&payload);
                {
                    let _guard = state.lock.write();
                    unsafe { mprotect(state.mm.mm.as_ptr() as *mut _, len0, PROT_READ); }
                }
            }
            0 => {
                if len < 8 {
                    println!("delta message too short: {} bytes", len);
                    continue;
                }
                let mut off_buf = [0u8; 8];
                off_buf.copy_from_slice(&payload[..8]);
                let off = u64::from_le_bytes(off_buf) as usize;
                let data = &payload[8..];
                if off + data.len() > local_bytes {
                    println!("delta out of bounds: off {} len {}", off, data.len());
                    continue;
                }
                let len0 = state.mm.byte_len();
                {
                    let _guard = state.lock.write();
                    unsafe { mprotect(state.mm.mm.as_ptr() as *mut _, len0, PROT_READ | PROT_WRITE); }
                }
                unsafe {
                    let dst = state.mm.mm.as_ptr().add(off) as *mut u8;
                    std::ptr::copy_nonoverlapping(data.as_ptr(), dst, data.len());
                }
                {
                    let _guard = state.lock.write();
                    unsafe { mprotect(state.mm.mm.as_ptr() as *mut _, len0, PROT_READ); }
                }
            }
            _ => {}
        }
    }
    println!("peer {:?} disconnected", addr);
    Ok(())
}

async fn broadcaster(peers: Vec<SocketAddr>, ring: Arc<ring::Ring>, rx: async_channel::Receiver<()>) -> Result<()> {
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
    let mut cursor = ring.cursor();
    while rx.recv().await.is_ok() {
        while let Some(delta) = cursor.next() {
            let mut buf = Vec::with_capacity(16 + delta.payload.len());
            buf.extend_from_slice(&delta.seq.to_le_bytes());
            buf.extend_from_slice(&(delta.payload.len() as u32).to_le_bytes());
            buf.push(delta.kind);
            buf.extend_from_slice(&[0u8;3]);
            buf.extend_from_slice(delta.payload);

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
                match s.write_all(&buf).await {
                    Ok(_) => {
                        alive.push((addr, s));
                    }
                    Err(e) => {
                        println!("send failed to {}: {}", addr, e);
                    }
                }
            }
            conns = alive;
        }
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
    ring: Arc<ring::Ring>,
    tx: async_channel::Sender<()>,
    shape: Vec<usize>,
    len: usize,
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
    fn flush(&self, _idx: usize) {
        let bytes = unsafe { std::slice::from_raw_parts(self.state.mm.mm.as_ptr(), self.state.mm.byte_len()) };
        println!("{} pushing checkpoint", self.name);
        if self.ring.push(1, bytes).is_ok() {
            let _ = self.tx.try_send(());
        }
    }

    fn write<'py>(slf: PyRef<'py, Self>) -> PyResult<WriteGuard> {
        let guard = slf.state.lock.write();
        let len = slf.state.mm.byte_len();
        unsafe {
            let ret = mprotect(slf.state.mm.mm.as_ptr() as *mut _, len, PROT_READ | PROT_WRITE);
            if ret != 0 {
                return Err(PyRuntimeError::new_err("mprotect(PROT_READ|WRITE) failed in write()"));
            }
        }
        // extend lifetime of guard to 'static for storage in Python object
        let guard: RwLockWriteGuard<'static, ()> = unsafe { std::mem::transmute::<RwLockWriteGuard<'_, ()>, RwLockWriteGuard<'static, ()>>(guard) };
        Ok(WriteGuard { node: slf.into(), guard: Some(guard) })
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

fn flush_now(node: &Node) {
    node.flush(0);
}

#[pyclass(unsendable)]
struct WriteGuard {
    node: Py<Node>,
    guard: Option<RwLockWriteGuard<'static, ()>>,
}

#[pymethods]
impl WriteGuard {
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
#[pyo3(signature = (name, listen, peers, shape=None))]
fn start(_py: Python<'_>, name: &str, listen: &str, peers: Vec<&str>, shape: Option<Vec<usize>>) -> PyResult<Node> {
    let shape = shape.unwrap_or_else(|| vec![10]);
    let len: usize = shape.iter().product();
    let buf = MmapBuf::new(shape.clone()).map_err(|e| PyValueError::new_err(e.to_string()))?;
    let state = Shared::new(buf);
    let (tx, rx) = async_channel::bounded(1024);
    let ring_path = std::env::temp_dir().join(format!("{}-ring", name));
    let ring = Arc::new(ring::Ring::new(&ring_path).map_err(|e| PyValueError::new_err(e.to_string()))?);
    {
        let bytes = unsafe { std::slice::from_raw_parts(state.mm.mm.as_ptr(), state.mm.byte_len()) };
        ring.push(1, bytes).map_err(|e| PyValueError::new_err(e.to_string()))?;
        // trigger initial broadcast of the checkpoint
        let _ = tx.try_send(());
    }

    let listen_addr: SocketAddr = listen.parse()
        .map_err(|e: std::net::AddrParseError| PyValueError::new_err(e.to_string()))?;
    let peer_addrs: Vec<SocketAddr> = peers.iter().filter_map(|p| p.parse().ok()).collect();


    let st_clone = state.clone();
    RUNTIME.spawn(listener(listen_addr, st_clone));
    let ring_clone = ring.clone();
    RUNTIME.spawn(broadcaster(peer_addrs, ring_clone, rx));

    unsafe {
        let len0 = state.mm.byte_len();
        let ret = mprotect(state.mm.mm.as_ptr() as *mut _, len0, PROT_READ);
        if ret != 0 {
            return Err(PyRuntimeError::new_err("mprotect(PROT_READ) failed in start()"));
        }
    }

    println!("node {} running on {} with shape {:?}", name, listen, shape);
    Ok(Node { name: name.to_string(), state, ring, tx, shape, len })
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

