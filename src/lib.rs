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
use once_cell::sync::Lazy;
use numpy::PyArray1;
use numpy::Element;
use numpy::npyffi::{PY_ARRAY_API, NpyTypes, NPY_ARRAY_WRITEABLE, npy_intp};
use std::os::raw::c_void;
use std::io::Write;
use pyo3::prelude::*;
use pyo3::types::PyModule;
use pyo3::exceptions::PyValueError;
use serde::{Deserialize, Serialize};
use std::net::SocketAddr;
use std::sync::Arc;
use parking_lot::RwLock;
use tokio::{net::{TcpListener, TcpStream}, runtime::Runtime};
use std::io::ErrorKind;

// ---------- global Tokio runtime (one per process) -------------------------
static RUNTIME: Lazy<Runtime> = Lazy::new(|| Runtime::new().expect("tokio"));

// ---------- in‑memory state -------------------------------------------------
#[derive(Clone)]
struct Shared {
    buf: Arc<RwLock<[f64; 10]>>,
}
impl Shared {
    fn apply(&self, idx: usize, val: f64) {
        if idx < 10 { self.buf.write()[idx] = val; }
    }
    fn snapshot(&self) -> [f64; 10] { *self.buf.read() }
}

// ---------- wire protocol ---------------------------------------------------
#[derive(Debug, Serialize, Deserialize)]
struct Update { idx: usize, val: f64 }

async fn handle_peer(mut sock: TcpStream, state: Shared) -> Result<()> {
    eprintln!("raftmem: handle_peer connected from {:?}", sock.peer_addr());
    let mut buf = vec![0u8; 128];
    loop {
        sock.readable().await?;
        match sock.try_read(&mut buf) {
            Ok(0) => {
                eprintln!("raftmem: handle_peer connection closed");
                break;
            }
            Ok(n) => {
                eprintln!("raftmem: handle_peer read {} bytes", n);
                if let Ok(upd) = serde_json::from_slice::<Update>(&buf[..n]) {
                    eprintln!("raftmem: handle_peer update idx={} val={}", upd.idx, upd.val);
                    state.apply(upd.idx, upd.val);
                } else {
                    eprintln!("raftmem: handle_peer failed to parse update");
                }
            }
            Err(ref e) if e.kind() == ErrorKind::WouldBlock => continue,
            Err(e) => return Err(e.into()),
        }
    }
    Ok(())
}

async fn broadcaster(peers: Vec<SocketAddr>, rx: async_channel::Receiver<Update>) -> Result<()> {
    // For each update, connect to each peer and send data (ephemeral connections)
    while let Ok(u) = rx.recv().await {
        eprintln!("raftmem: broadcaster got update idx={} val={}", u.idx, u.val);
        let data = match serde_json::to_vec(&u) {
            Ok(d) => d,
            Err(_) => continue,
        };
        for addr in &peers {
            eprintln!("raftmem: broadcaster sending to {}", addr);
            let data = data.clone();
            let addr = *addr;
            tokio::spawn(async move {
                if let Ok(mut s) = TcpStream::connect(addr).await {
                    eprintln!("raftmem: broadcaster connected to {}", addr);
                    // Wait until writable, then send
                    let _ = s.writable().await;
                    let _ = s.try_write(&data);
                }
            });
        }
    }
    Ok(())
}

async fn listener(addr: SocketAddr, state: Shared) -> Result<()> {
    let lst = TcpListener::bind(addr).await?;
    loop {
        let (sock, _) = lst.accept().await?;
        let st = state.clone();
        tokio::spawn(async move { let _ = handle_peer(sock, st).await; });
    }
}

// ---------- exposed Python face -------------------------------------------
#[pyclass]
struct Node {
    state: Shared,
    /// Peer socket addresses for broadcasting updates
    peers: Vec<SocketAddr>,
}

#[pymethods]
impl Node {
    #[getter]
    fn ndarray<'py>(self_: PyRef<'py, Node>, py: Python<'py>) -> &'py PyArray1<f64> {
        // Zero-copy NumPy view into the shared buffer.
        // Dimensions (1D array of length 10)
        let dims: [npy_intp; 1] = [10];
        // Stride (in bytes) for f64
        let strides: [npy_intp; 1] = [std::mem::size_of::<f64>() as npy_intp];
        // Pointer to shared buffer
        let data_ptr = self_.state.buf.read().as_ptr() as *mut c_void;
        unsafe {
            // Get the NumPy array type and dtype descriptor
            let subtype = PY_ARRAY_API.get_type_object(py, NpyTypes::PyArray_Type);
            let descr = <f64 as Element>::get_dtype(py).into_dtype_ptr();
            // Create array from existing data, writeable, base object = this Node
            let arr_ptr = PY_ARRAY_API.PyArray_NewFromDescr(
                py,
                subtype,
                descr,
                1,
                dims.as_ptr() as *mut npy_intp,
                strides.as_ptr() as *mut npy_intp,
                data_ptr,
                NPY_ARRAY_WRITEABLE,
                self_.as_ptr(),
            );
            // Wrap raw pointer as PyArray1
            PyArray1::from_owned_ptr(py, arr_ptr)
        }
    }
    /// Broadcast an update for the given index to all peers
    fn flush(&self, idx: usize) {
        let val = self.state.snapshot()[idx];
        // Serialize update
        if let Ok(data) = serde_json::to_vec(&Update { idx, val }) {
            for addr in &self.peers {
                // Attempt connection
                println!("raftmem: flush connecting to {}", addr);
                match std::net::TcpStream::connect(addr) {
                    Ok(mut s) => {
                        println!("raftmem: flush connected to {}", addr);
                        if let Err(e) = s.write_all(&data) {
                            println!("raftmem: flush write error to {}: {}", addr, e);
                        }
                    }
                    Err(e) => {
                        println!("raftmem: flush connect error to {}: {}", addr, e);
                    }
                }
            }
        }
    }
}

#[pyfunction]
#[pyo3(signature = (name, listen, peers))]
fn start(py: Python<'_>, name: &str, listen: &str, peers: Vec<&str>) -> PyResult<Node> {
    let state = Shared { buf: Arc::new(RwLock::new([0.0; 10])) };
    // Channel-based broadcaster removed; using synchronous broadcast in flush

    let listen_addr: SocketAddr = listen.parse()
        .map_err(|e: std::net::AddrParseError| PyValueError::new_err(e.to_string()))?;
    // Resolve peer addresses (allow DNS names or IP literals)
    use std::net::ToSocketAddrs;
    let mut peer_addrs = Vec::new();
    for p in peers {
        match p.to_socket_addrs() {
            Ok(addrs) => peer_addrs.extend(addrs),
            Err(e) => eprintln!("raftmem: warning: failed to resolve peer '{}': {}", p, e),
        }
    }

    // Start listener for incoming updates
    let st_clone = state.clone();
    RUNTIME.spawn(listener(listen_addr, st_clone));

    println!("node {} running on {}", name, listen);
    Ok(Node { state, peers: peer_addrs })
}

// ---------- module init ----------------------------------------------------
#[pymodule]
fn raftmem(_py: Python<'_>, m: &PyModule) -> PyResult<()> {
    m.add_class::<Node>()?;
    m.add_function(wrap_pyfunction!(start, m)?)?;
    Ok(())
}

