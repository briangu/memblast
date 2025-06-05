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
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    runtime::{Builder, Runtime},
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
}

impl MmapBuf {
    fn new() -> Result<Self> {
        // We only need to hold 10 f64s (80 bytes), no page-align requirement here.
        let layout = 10 * std::mem::size_of::<f64>();
        let mm = MmapOptions::new().len(layout).map_anon()?;
        Ok(Self { mm })
    }

    fn ptr(&self) -> *mut c_void {
        self.mm.as_ptr() as *mut _
    }
}

#[derive(Clone)]
struct Shared(Arc<MmapBuf>);

// ---------- wire format -----------------------------------------------
struct Full {
    vals: Vec<f64>,
}

// ---------- networking helpers ----------------------------------------
async fn handle_peer(mut sock: TcpStream, shared: Shared) -> Result<()> {
    let len = shared.0.mm.len();

    loop {
        // A mutable view onto this process’ own mmap:
        let dst: &mut [u8] = unsafe {
            std::slice::from_raw_parts_mut(shared.0.mm.as_ptr() as *mut u8, len)
        };

        // read_exact fills the mmap slice in-place – no intermediate buffer.
        if sock.read_exact(dst).await.is_err() {
            break;       // peer closed
        }
    }
    Ok(())
}


async fn listener(listener: TcpListener, shared: Shared) -> Result<()> {
    loop {
        let (sock, _) = listener.accept().await?;
        let shared2 = shared.clone();
        tokio::spawn(async move {
            let _ = handle_peer(sock, shared2).await;
        });
    }
}

// ---------- Python bindings -------------------------------------------
#[pyclass]
struct Node {
    shared: Shared,
    peers: Vec<SocketAddr>,
}

#[pymethods]
impl Node {
    /// Return a numpy.ndarray\<f64\> view of our shared buffer (length 10).
    #[getter]
    fn ndarray<'py>(slf: PyRef<'py, Node>, py: Python<'py>) -> &'py PyArray1<f64> {
        // We expose a 1‐D array of length 10.
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
                slf.shared.0.ptr(),
                NPY_ARRAY_WRITEABLE,
                slf.as_ptr(),
            );
            PyArray1::from_owned_ptr(py, arr_ptr)
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
        Ok(WriteGuard { node: slf.into() })
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
        flush_now(self);
        Ok(())
    }
    /// Construct a new Node by binding to `listen` and connecting to `peers`.
    #[new]
    fn new(listen: String, peers: Vec<String>) -> PyResult<Self> {
        Python::with_gil(|py| {
            let peer_slices: Vec<&str> = peers.iter().map(String::as_str).collect();
            start(py, &listen, peer_slices)
        })
    }
}


/// Start a new Node that listens on `listen` (and immediately spawns a Tokio task for it)
/// and also knows how to broadcast to the given `peers`.
#[pyfunction]
fn start(_py: Python<'_>, listen: &str, peers: Vec<&str>) -> PyResult<Node> {
    // 1) Create an anonymous mmap for 10 f64s:
    let buf = MmapBuf::new().map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
    let shared = Shared(Arc::new(buf));

    // 2) Parse each peer address as SocketAddr:
    let peer_addrs: Vec<SocketAddr> = peers
        .into_iter()
        .filter_map(|p| p.parse().ok())
        .collect();


    // 3) Make mmap read-only until a write() context, then bind the listener:
    let len0 = shared.0.mm.len();
    unsafe {
        let ret = mprotect(shared.0.mm.as_ptr() as *mut _, len0, PROT_READ);
        if ret != 0 {
            return Err(PyRuntimeError::new_err("mprotect(PROT_READ) failed in start()"));
        }
    }
    // Bind the listener synchronously so EADDRINUSE pops up at import time:
    // …bind std_listener…
    let std_listener = std::net::TcpListener::bind(listen)
        .map_err(|e| PyRuntimeError::new_err(format!("bind {listen}: {e}")))?;
    std_listener.set_nonblocking(true)
        .map_err(|e| PyRuntimeError::new_err(format!("non-blocking: {e}")))?;

    // — Remove the semicolon at the end, and move `?` outside the closure:
    let tokio_listener = {
        let _g = RT.enter();
        tokio::net::TcpListener::from_std(std_listener)
            .map_err(|e| PyRuntimeError::new_err(format!("to-tokio: {e}")))?
    };

    let shared2 = shared.clone();
    RT.spawn(async move {
        if let Err(e) = listener(tokio_listener, shared2).await {
            eprintln!("raftmem listener error: {e}");
        }
    });

    println!("raftmem: started on {}", listen);
    Ok(Node {
        shared,
        peers: peer_addrs,
    })
}

async fn broadcast_raw(peers: &[SocketAddr], shared: Shared) {
    // Unsafe is just to cast the mmap pointer to a byte slice.
    let bytes: &[u8] = unsafe {
        std::slice::from_raw_parts(shared.0.mm.as_ptr(), shared.0.mm.len())
    };

    for &addr in peers {
        if let Ok(mut s) = TcpStream::connect(addr).await {
            let _ = s.write_all(bytes).await;      // <-- write the mmap slice itself
        }
    }
}

// ---------- helper -----------------------------------------------------
fn flush_now(node: &Node) {
    let peers  = node.peers.clone();
    let shared = node.shared.clone();          // keep the mmap alive for the async task

    RT.spawn(async move {
        broadcast_raw(&peers, shared).await;
    });
}

#[pyclass]
struct WriteGuard {
    node: Py<Node>,
}

#[pymethods]
impl WriteGuard {
    fn __enter__<'py>(slf: PyRefMut<'py, Self>, py: Python<'py>) -> &'py PyArray1<f64> {
        let cell = slf.node.as_ref(py).borrow();
        // Return a writable ndarray view.
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
                cell.shared.0.ptr(),
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
            let len = cell.shared.0.mm.len();
            let ret = unsafe { mprotect(cell.shared.0.mm.as_ptr() as *mut _, len, PROT_READ) };
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
        // Return a read-only ndarray view.
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
                cell.shared.0.ptr(),
                0,
                cell.as_ptr(),
            );
            PyArray1::from_owned_ptr(py, arr_ptr)
        }
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

