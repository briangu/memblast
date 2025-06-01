// ===============================================================
// raftmem_rs 0.6 — explicit‐batch flush only, no signal handling
// ===============================================================

use anyhow::Result;
use memmap2::{MmapMut, MmapOptions};
use libc::{mprotect, PROT_READ, PROT_WRITE};
use numpy::{Element, PyArray1};
use numpy::npyffi::{PY_ARRAY_API, NpyTypes, NPY_ARRAY_WRITEABLE, npy_intp};
use once_cell::sync::Lazy;
use parking_lot::RwLock;
use pyo3::{prelude::*, wrap_pyfunction, types::PyModule};
use pyo3::exceptions::PyRuntimeError;
use serde::{Deserialize, Serialize};
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
        let mm = unsafe { MmapOptions::new().len(layout).map_anon()? };
        Ok(Self { mm })
    }

    fn ptr(&self) -> *mut c_void {
        self.mm.as_ptr() as *mut _
    }
}

#[derive(Clone)]
struct Shared(Arc<MmapBuf>);

// ---------- wire format -----------------------------------------------
#[derive(Serialize, Deserialize)]
struct Full {
    vals: Vec<f64>,
}

// ---------- networking helpers ----------------------------------------
async fn broadcast_full(peers: &[SocketAddr], payload: &Full) {
    if let Ok(buf) = serde_json::to_vec(payload) {
        for &p in peers {
            if let Ok(mut s) = TcpStream::connect(p).await {
                let _ = s.write_all(&buf).await;
            }
        }
    }
}

async fn handle_peer(mut sock: TcpStream, shared: Shared) -> Result<()> {
    let mut buf = vec![0u8; 256];
    loop {
        let n = sock.read(&mut buf).await?;
        if n == 0 {
            break;
        }
        if let Ok(msg) = serde_json::from_slice::<Full>(&buf[..n]) {
            // Write incoming values into our mmap'd region
            unsafe {
                let base = shared.0.mm.as_ptr() as *mut f64;
                for (i, v) in msg.vals.iter().enumerate().take(10) {
                    *base.add(i) = *v;
                }
            }
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
    batching: Arc<RwLock<bool>>,
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

    /// Begin a “batch” context. While in batch, the underlying mmap is kept writable.
    fn batch<'py>(slf: PyRef<'py, Self>) -> PyResult<BatchGuard> {
        let len = slf.shared.0.mm.len();
        // Make sure the page is READ|WRITE while we’re in a batch.
        unsafe {
            let ret = mprotect(
                slf.shared.0.mm.as_ptr() as *mut _,
                len,
                PROT_READ | PROT_WRITE,
            );
            if ret != 0 {
                return Err(PyRuntimeError::new_err("mprotect(PROT_READ|WRITE) failed in batch()"));
            }
        }

        *slf.batching.write() = true;
        Ok(BatchGuard {
            node: slf.into(),
        })
    }

    /// Manual flush helper if the user wants to push immediately.
    fn flush(&self) -> PyResult<()> {
        flush_now(self);
        Ok(())
    }
}

#[pyclass]
struct BatchGuard {
    node: Py<Node>,
}

#[pymethods]
impl BatchGuard {
    fn __enter__<'py>(slf: PyRefMut<'py, Self>) -> PyResult<Py<Node>> {
        Ok(slf.node.clone())
    }

    fn __exit__(
        &mut self,
        _t: &PyAny,
        _v: &PyAny,
        _tb: &PyAny,
    ) -> PyResult<()> {
        Python::with_gil(|py| {
            let cell = self.node.as_ref(py).borrow();
            // Flush the current values to peers before making it read-only again:
            flush_now(&*cell);
            *cell.batching.write() = false;
            let len = cell.shared.0.mm.len();
            // After flushing, mark the mmap read‐only again:
            let ret = unsafe { mprotect(cell.shared.0.mm.as_ptr() as *mut _, len, PROT_READ) };
            if ret != 0 {
                return Err(PyRuntimeError::new_err("mprotect(PROT_READ) failed in __exit__"));
            }
            Ok(())
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

    let batching = Arc::new(RwLock::new(false));

    // 3) Bind the listener synchronously so EADDRINUSE pops up at import time:
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
        batching,
    })
}

// ---------- helper -----------------------------------------------------
fn flush_now(node: &Node) {
    let vals = unsafe {
        let base = node.shared.0.mm.as_ptr() as *const f64;
        let mut v = [0.0f64; 10];
        for i in 0..10 {
            v[i] = *base.add(i);
        }
        v
    };

    let peers = node.peers.clone();
    RT.spawn(async move {
        broadcast_full(&peers, &Full { vals: vals.to_vec() }).await;
    });
}

#[pymodule]
fn raftmem(_py: Python<'_>, m: &PyModule) -> PyResult<()> {
    m.add_class::<Node>()?;
    m.add_class::<BatchGuard>()?;
    m.add_function(wrap_pyfunction!(start, m)?)?;
    Ok(())
}

