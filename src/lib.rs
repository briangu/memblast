use anyhow::Result;
use memmap2::{MmapMut, MmapOptions};
use libc::{mprotect, PROT_READ, PROT_WRITE};
use numpy::{Element, PyArray1};
use numpy::npyffi::{NpyTypes, NPY_ARRAY_WRITEABLE, npy_intp};
use numpy::PY_ARRAY_API;
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
        let layout = 10 * std::mem::size_of::<f64>(); // 80 bytes
        let mm = unsafe { MmapOptions::new().len(layout).map_anon()? };
        Ok(Self { mm })
    }
}

#[derive(Clone)]
struct Shared(Arc<MmapBuf>);

// ---------- networking helpers ----------------------------------------
async fn handle_peer(mut sock: TcpStream, shared: Shared) -> Result<()> {
    // We know exactly 80 bytes per message (10 * 8)
    let mut buf = [0u8; 10 * std::mem::size_of::<f64>()];
    loop {
        // Read exactly 80 bytes
        let mut read_so_far = 0;
        while read_so_far < buf.len() {
            let n = sock.read(&mut buf[read_so_far..]).await?;
            if n == 0 {
                // connection closed
                return Ok(());
            }
            read_so_far += n;
        }
        // Convert each 8‐byte chunk into an f64
        let mut arr = [0f64; 10];
        for i in 0..10 {
            let start = i * std::mem::size_of::<f64>();
            // Interpret as little‐endian. If your machines are all little‐endian, you can skip this.
            arr[i] = f64::from_le_bytes(buf[start..start + 8].try_into().unwrap());
        }
        // Write into mmap
        unsafe {
            let base = shared.0.mm.as_ptr() as *mut f64;
            for i in 0..10 {
                *base.add(i) = arr[i];
            }
        }
    }
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
    /// Return a numpy.ndarray<f64> view of our shared buffer (length 10).
    #[getter]
    fn ndarray<'py>(slf: PyRef<'py, Node>, py: Python<'py>) -> &'py numpy::PyArray1<f64> {
        let dims: [npyffi::npy_intp; 1] = [10];
        let strides: [npyffi::npy_intp; 1] = [std::mem::size_of::<f64>() as npyffi::npy_intp];
        unsafe {
            let subtype = PY_ARRAY_API.get_type_object(py, NpyTypes::PyArray_Type);
            let descr = <f64 as numpy::Element>::get_dtype(py).into_dtype_ptr();
            let arr_ptr = PY_ARRAY_API.PyArray_NewFromDescr(
                py,
                subtype,
                descr,
                dims.len() as c_int,
                dims.as_ptr() as *mut npyffi::npy_intp,
                strides.as_ptr() as *mut npyffi::npy_intp,
                slf.shared.0.mm.as_ptr(),
                NPY_ARRAY_WRITEABLE,
                slf.as_ptr(),
            );
            numpy::PyArray1::from_owned_ptr(py, arr_ptr)
        }
    }

    /// Begin a “batch” context: make mmap READ|WRITE so user can mutate.
    fn batch<'py>(slf: PyRef<'py, Self>) -> PyResult<BatchGuard> {
        let len = slf.shared.0.mm.len();
        unsafe {
            let ret = mprotect(
                slf.shared.0.mm.as_ptr() as *mut _,
                len,
                PROT_READ | PROT_WRITE,
            );
            if ret != 0 {
                return Err(PyRuntimeError::new_err("mprotect(PROT_READ|WRITE) failed"));
            }
        }
        *slf.batching.write() = true;
        Ok(BatchGuard {
            node: slf.into(),
        })
    }

    /// Manual flush: push raw bytes to peers
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
            // Flush the page before marking it readonly
            flush_now(&*cell);
            *cell.batching.write() = false;
            let len = cell.shared.0.mm.len();
            let ret = unsafe { mprotect(cell.shared.0.mm.as_ptr() as *mut _, len, PROT_READ) };
            if ret != 0 {
                return Err(PyRuntimeError::new_err("mprotect(PROT_READ) failed"));
            }
            Ok(())
        })
    }
}

/// Start a new Node that listens on `listen` and knows how to broadcast to `peers`.
#[pyfunction]
fn start(_py: Python<'_>, listen: &str, peers: Vec<&str>) -> PyResult<Node> {
    let buf = MmapBuf::new().map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
    let shared = Shared(Arc::new(buf));

    // parse peers
    let peer_addrs: Vec<SocketAddr> = peers
        .into_iter()
        .filter_map(|p| p.parse().ok())
        .collect();

    let batching = Arc::new(RwLock::new(false));

    // Bind & convert to Tokio listener
    let std_listener = std::net::TcpListener::bind(listen)
        .map_err(|e| PyRuntimeError::new_err(format!("bind {listen}: {e}")))?;
    std_listener
        .set_nonblocking(true)
        .map_err(|e| PyRuntimeError::new_err(format!("non-blocking: {e}")))?;

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

// Helper that sends raw bytes from our mmap to all peers
fn flush_now(node: &Node) {
    // Grab a &[u8] over the entire 80‐byte region
    let len = node.shared.0.mm.len();
    let raw_bytes: &[u8] = unsafe {
        std::slice::from_raw_parts(node.shared.0.mm.as_ptr(), len)
    };
    let peers = node.peers.clone();

    RT.spawn(async move {
        for &p in &peers {
            if let Ok(mut s) = TcpStream::connect(p).await {
                // Single call that writes the raw mmap contents
                let _ = s.write_all(raw_bytes).await;
            }
        }
    });
}

#[pymodule]
fn raftmem(_py: Python<'_>, m: &PyModule) -> PyResult<()> {
    m.add_class::<Node>()?;
    m.add_class::<BatchGuard>()?;
    m.add_function(wrap_pyfunction!(start, m)?)?;
    Ok(())
}

