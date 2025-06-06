use anyhow::Result;
use memmap2::{MmapMut, MmapOptions};
use libc::{mprotect, PROT_READ, PROT_WRITE};
use numpy::{Element, PyArray1};
use numpy::npyffi::{PY_ARRAY_API, NpyTypes, NPY_ARRAY_WRITEABLE, npy_intp};
use once_cell::sync::Lazy;
use pyo3::{prelude::*, wrap_pyfunction, types::PyModule};
use pyo3::exceptions::PyRuntimeError;
use std::{
    net::SocketAddr,
    os::raw::{c_int, c_void},
    sync::Arc,
};
use std::slice;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    runtime::{Builder, Runtime},
    sync::Mutex,
};

// ---------- Tokio runtime (shared) ------------------------------------
static RT: Lazy<Runtime> = Lazy::new(|| {
    Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .expect("Failed to create Tokio runtime")
});

// ---------- shared mmap buffer ----------------------------------------
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

    fn len(&self) -> usize {
        self.len
    }

    fn shape(&self) -> &[usize] {
        &self.shape
    }
}

#[derive(Clone)]
struct Shared(Arc<MmapBuf>);

// ---------- wire format -----------------------------------------------
// ---------- networking helpers ----------------------------------------

// ---------- Python bindings -------------------------------------------
#[pyclass]
struct Node {
    shared: Shared,
    mode: NodeMode,
}

enum NodeMode {
    Server {
        clients: Arc<tokio::sync::Mutex<Vec<TcpStream>>>,
        last_data: Arc<tokio::sync::Mutex<Vec<u8>>>,
    },
    Client { addr: SocketAddr, on_update: Option<Py<PyAny>> },
}

#[pymethods]
impl Node {
    /// Return a numpy.ndarray\<f64\> view of our shared buffer.
    #[getter]
    fn ndarray<'py>(slf: PyRef<'py, Node>, py: Python<'py>) -> PyResult<&'py PyAny> {
        let shape: Vec<npy_intp> = slf
            .shared
            .0
            .shape()
            .iter()
            .map(|&d| d as npy_intp)
            .collect();

        let mut strides: Vec<npy_intp> = vec![0; shape.len()];
        if !shape.is_empty() {
            strides[shape.len() - 1] = std::mem::size_of::<f64>() as npy_intp;
            for i in (0..shape.len() - 1).rev() {
                strides[i] = strides[i + 1] * shape[i + 1];
            }
        }

        unsafe {
            let subtype = PY_ARRAY_API.get_type_object(py, NpyTypes::PyArray_Type);
            let descr = <f64 as Element>::get_dtype(py).into_dtype_ptr();
            let arr_ptr = PY_ARRAY_API.PyArray_NewFromDescr(
                py,
                subtype,
                descr,
                shape.len() as c_int,
                shape.as_ptr() as *mut npy_intp,
                strides.as_ptr() as *mut npy_intp,
                slf.shared.0.ptr(),
                NPY_ARRAY_WRITEABLE,
                slf.as_ptr(),
            );
            Ok(py.from_owned_ptr(arr_ptr))
        }
    }

    /// Begin a write context: mmap becomes writable and writes will be flushed on exit.
    fn write<'py>(slf: PyRef<'py, Self>) -> PyResult<WriteGuard> {
        let len = slf.shared.0.mm.len();
        unsafe {
            let ret = mprotect(
                slf.shared.0.mm.as_ptr() as *mut _,
                len,
                PROT_READ | PROT_WRITE,
            );
            if ret != 0 {
                return Err(PyRuntimeError::new_err("mprotect(PROT_READ|WRITE) failed in write()"));
            }
        }
        Ok(WriteGuard { node: slf.into(), data: None })
    }

    /// Begin a read context: updates are blocked until exit.
    fn read<'py>(slf: PyRef<'py, Self>) -> PyResult<ReadGuard> {
        let len = slf.shared.0.mm.len();
        unsafe {
            let ret = mprotect(slf.shared.0.mm.as_ptr() as *mut _, len, PROT_READ);
            if ret != 0 {
                return Err(PyRuntimeError::new_err("mprotect(PROT_READ) failed in read()"));
            }
        }
        Ok(ReadGuard { node: slf.into() })
    }

    /// Manual flush helper if the user wants to push immediately.
    fn flush(&self) -> PyResult<()> {
        flush_now(self, Vec::new());
        Ok(())
    }
}


/// Start a new Node that listens on `listen` (and immediately spawns a Tokio task for it)
/// and also knows how to broadcast to the given `peers`.
#[pyfunction]
#[pyo3(signature = (name, listen=None, server=None, shape=None, on_update=None))]
pub fn start(
    py: Python<'_>,
    name: &str,
    listen: Option<&str>,
    server: Option<&str>,
    shape: Option<Vec<usize>>,
    on_update: Option<PyObject>,
) -> PyResult<Node> {
    let shape = shape.unwrap_or_else(|| vec![10]);
    let _ = name;
    let buf = MmapBuf::new(shape).map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
    let shared = Shared(Arc::new(buf));

    unsafe {
        let ret = mprotect(shared.0.mm.as_ptr() as *mut _, shared.0.mm.len(), PROT_READ);
        if ret != 0 {
            return Err(PyRuntimeError::new_err("mprotect(PROT_READ) failed in start()"));
        }
    }

    if let Some(addr_str) = listen {
        let addr: SocketAddr = addr_str
            .parse::<SocketAddr>()
            .map_err(|e: std::net::AddrParseError| PyRuntimeError::new_err(e.to_string()))?;
        let std_listener = std::net::TcpListener::bind(addr)
            .map_err(|e| PyRuntimeError::new_err(format!("bind {addr}: {e}")))?;
        std_listener.set_nonblocking(true)
            .map_err(|e| PyRuntimeError::new_err(format!("non-blocking: {e}")))?;
        let tokio_listener = {
            let _g = RT.enter();
            tokio::net::TcpListener::from_std(std_listener)
                .map_err(|e| PyRuntimeError::new_err(format!("to-tokio: {e}")))?
        };
        let clients = Arc::new(Mutex::new(Vec::new()));
        let last_data = Arc::new(Mutex::new(Vec::new()));
        let clients2 = clients.clone();
        let shared2 = shared.clone();
        let last_data2 = last_data.clone();
        RT.spawn(async move {
            loop {
                if let Ok((mut sock, _)) = tokio_listener.accept().await {
                    let bytes: &[u8] = unsafe { slice::from_raw_parts(shared2.0.mm.as_ptr(), shared2.0.mm.len()) };
                    let mut ok = true;
                    if sock.write_all(bytes).await.is_err() { ok = false; }
                    if ok {
                        let data = last_data2.lock().await;
                        let len = data.len() as u32;
                        if sock.write_all(&len.to_le_bytes()).await.is_err() { ok = false; }
                        if ok && sock.write_all(&data).await.is_err() { ok = false; }
                    }
                    if ok {
                        clients2.lock().await.push(sock);
                    }
                }
            }
        });
        println!("raftmem: server running on {}", addr);
        Ok(Node { shared, mode: NodeMode::Server { clients, last_data } })
    } else if let Some(addr_str) = server {
        let addr: SocketAddr = addr_str
            .parse::<SocketAddr>()
            .map_err(|e: std::net::AddrParseError| PyRuntimeError::new_err(e.to_string()))?;
        let cb = on_update.map(|o| o.into_py(py));
        let shared2 = shared.clone();
        let cb2 = cb.clone();
        RT.spawn(async move {
            loop {
                match TcpStream::connect(addr).await {
                    Ok(mut sock) => {
                        let len = shared2.0.mm.len();
                        let mut buf = vec![0u8; len];
                        loop {
                            if sock.read_exact(&mut buf).await.is_err() {
                                break;
                            }
                            unsafe { mprotect(shared2.0.mm.as_ptr() as *mut _, len, PROT_READ | PROT_WRITE); }
                            unsafe {
                                std::ptr::copy_nonoverlapping(
                                    buf.as_ptr(),
                                    shared2.0.mm.as_ptr() as *mut u8,
                                    len,
                                );
                            }
                            unsafe { mprotect(shared2.0.mm.as_ptr() as *mut _, len, PROT_READ); }
                            let mut len_buf = [0u8; 4];
                            if sock.read_exact(&mut len_buf).await.is_err() {
                                break;
                            }
                            let dlen = u32::from_le_bytes(len_buf) as usize;
                            let mut data = vec![0u8; dlen];
                            if sock.read_exact(&mut data).await.is_err() {
                                break;
                            }
                            if let Some(cb) = &cb2 {
                                Python::with_gil(|py| {
                                    if let Ok(json) = py.import("json") {
                                        if let Ok(loads) = json.getattr("loads") {
                                            if let Ok(obj) = loads.call1((String::from_utf8_lossy(&data).to_string(),)) {
                                                let _ = cb.call1(py, (obj,));
                                            }
                                        }
                                    }
                                });
                            }
                        }
                    }
                    Err(e) => {
                        eprintln!("connect error: {}", e);
                        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                    }
                }
            }
        });
        println!("raftmem: client connected to {}", addr);
        Ok(Node { shared, mode: NodeMode::Client { addr, on_update: cb } })
    } else {
        Err(PyRuntimeError::new_err("either listen or server must be provided"))
    }
}

async fn broadcast_raw(clients: Arc<Mutex<Vec<TcpStream>>>, shared: Shared, data: Vec<u8>) {
    let bytes: &[u8] = unsafe { std::slice::from_raw_parts(shared.0.mm.as_ptr(), shared.0.mm.len()) };
    let mut conns = {
        let mut g = clients.lock().await;
        std::mem::take(&mut *g)
    };
    let mut alive = Vec::new();
    for mut s in conns.drain(..) {
        let mut ok = true;
        if s.write_all(bytes).await.is_err() { ok = false; }
        if ok {
            let len = data.len() as u32;
            if s.write_all(&len.to_le_bytes()).await.is_err() { ok = false; }
        }
        if ok && s.write_all(&data).await.is_err() { ok = false; }
        if ok { alive.push(s); }
    }
    clients.lock().await.extend(alive);
}

// ---------- helper -----------------------------------------------------
fn flush_now(node: &Node, data: Vec<u8>) {
    if let NodeMode::Server { clients, last_data } = &node.mode {
        {
            let _g = RT.enter();
            let mut ld = RT.block_on(last_data.lock());
            *ld = data.clone();
        }
        let clients = clients.clone();
        let shared = node.shared.clone();
        RT.spawn(async move { broadcast_raw(clients, shared, data).await; });
    }
}

#[pyclass]
struct WriteGuard {
    node: Py<Node>,
    data: Option<PyObject>,
}

#[pymethods]
impl WriteGuard {
    fn __enter__(slf: PyRefMut<'_, Self>) -> PyRefMut<'_, Self> {
        slf
    }

    fn __exit__(&mut self, _t: &PyAny, _v: &PyAny, _tb: &PyAny) -> PyResult<()> {
        Python::with_gil(|py| {
            let cell = self.node.as_ref(py).borrow();
            let data = if let Some(d) = self.data.take() {
                let json = py.import("json")?;
                let dumps = json.getattr("dumps")?;
                let s: String = dumps.call1((d,))?.extract()?;
                s.into_bytes()
            } else { Vec::new() };
            flush_now(&*cell, data);
            let len = cell.shared.0.mm.len();
            let ret = unsafe { mprotect(cell.shared.0.mm.as_ptr() as *mut _, len, PROT_READ) };
            if ret != 0 {
                return Err(PyRuntimeError::new_err("mprotect(PROT_READ) failed in write() exit"));
            }
            Ok(())
        })
    }

    fn __setitem__(&mut self, idx: usize, value: f64) {
        Python::with_gil(|py| {
            let cell = self.node.as_ref(py).borrow();
            unsafe {
                let base = cell.shared.0.mm.as_ptr() as *mut f64;
                *base.add(idx) = value;
            }
        });
    }

    fn __getitem__(&self, idx: usize) -> f64 {
        Python::with_gil(|py| {
            let cell = self.node.as_ref(py).borrow();
            unsafe {
                let base = cell.shared.0.mm.as_ptr() as *mut f64;
                *base.add(idx)
            }
        })
    }

    fn __len__(&self) -> usize {
        Python::with_gil(|py| {
            let cell = self.node.as_ref(py).borrow();
            // return shape[0]
            cell.shared.0.shape[0]
        })
    }

    fn shape(&self) -> Vec<usize> {
        Python::with_gil(|py| {
            let cell = self.node.as_ref(py).borrow();
            cell.shared.0.shape.clone()
        })
    }

    fn update(&mut self, obj: PyObject) {
        self.data = Some(obj);
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
        // Return a read-only ndarray view.
        let dims: [npy_intp; 1] = [cell.shared.0.len() as npy_intp];
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
                cell.shared.0.ptr(),
                0,
                cell.as_ptr(),
            );
            PyArray1::from_owned_ptr(py, arr_ptr)
        }
    }

    fn __getitem__(&self, idx: usize) -> f64 {
        Python::with_gil(|py| {
            let cell = self.node.as_ref(py).borrow();
            unsafe {
                let base = cell.shared.0.mm.as_ptr() as *mut f64;
                *base.add(idx)
            }
        })
    }

    fn __len__(&self) -> usize {
        Python::with_gil(|py| {
            let cell = self.node.as_ref(py).borrow();
            cell.shared.0.len()
        })
    }

    fn shape(&self) -> Vec<usize> {
        Python::with_gil(|py| {
            let cell = self.node.as_ref(py).borrow();
            cell.shared.0.shape.clone()
        })
    }

    fn __exit__(&mut self, _t: &PyAny, _v: &PyAny, _tb: &PyAny) -> PyResult<()> {
        Python::with_gil(|py| {
            let cell = self.node.as_ref(py).borrow();
            let len = cell.shared.0.mm.len();
            let ret = unsafe { mprotect(cell.shared.0.mm.as_ptr() as *mut _, len, PROT_READ | PROT_WRITE) };
            if ret != 0 {
                return Err(PyRuntimeError::new_err("mprotect(PROT_READ|WRITE) failed in read() exit"));
            }
            Ok(())
        })
    }
}

#[pymodule]
fn raftmem(_py: Python<'_>, m: &PyModule) -> PyResult<()> {
    m.add_class::<Node>()?;
    m.add_class::<WriteGuard>()?;
    m.add_class::<ReadGuard>()?;
    m.add_function(wrap_pyfunction!(start, m)?)?;
    Ok(())
}

