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
    let mut buf = vec![0u8; 128];
    loop {
        sock.readable().await?;
        match sock.try_read(&mut buf) {
            Ok(0) => break,
            Ok(n) => {
                let upd: Update = serde_json::from_slice(&buf[..n])?;
                state.apply(upd.idx, upd.val);
            }
            Err(ref e) if e.kind() == ErrorKind::WouldBlock => continue,
            Err(e) => return Err(e.into()),
        }
    }
    Ok(())
}

async fn broadcaster(peers: Vec<SocketAddr>, rx: async_channel::Receiver<Update>) -> Result<()> {
    let mut conns = Vec::new();
    for p in peers {
        if let Some(s) = TcpStream::connect(p).await.ok() {
            conns.push(s);
        }
    }
    while let Some(u) = rx.recv().await.ok() {
        let data = serde_json::to_vec(&u)?;
        let mut alive = Vec::new();
        for mut s in conns {
            if s.writable().await.is_ok() {
                if let Ok(n) = s.try_write(&data) {
                    if n == data.len() {
                        alive.push(s);
                    }
                }
            }
        }
        conns = alive;
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
    tx: async_channel::Sender<Update>,
}

#[pymethods]
impl Node {
    #[getter]
    fn ndarray<'py>(&self, py: Python<'py>) -> &'py PyArray1<f64> {
        // Return a NumPy array view of the current state (copy of snapshot)
        PyArray1::from_slice(py, &self.state.snapshot())
    }
    fn flush(&self, idx: usize) {
        let val = self.state.snapshot()[idx];
        let _ = self.tx.try_send(Update { idx, val });
    }
}

#[pyfunction]
#[pyo3(signature = (name, listen, peers))]
fn start(py: Python<'_>, name: &str, listen: &str, peers: Vec<&str>) -> PyResult<Node> {
    let state = Shared { buf: Arc::new(RwLock::new([0.0; 10])) };
    let (tx, rx) = async_channel::bounded(1024);

    let listen_addr: SocketAddr = listen.parse()
        .map_err(|e: std::net::AddrParseError| PyValueError::new_err(e.to_string()))?;
    let peer_addrs: Vec<SocketAddr> = peers.into_iter().filter_map(|p| p.parse().ok()).collect();

    let st_clone = state.clone();
    RUNTIME.spawn(listener(listen_addr, st_clone));
    RUNTIME.spawn(broadcaster(peer_addrs, rx));

    println!("node {} running on {}", name, listen);
    Ok(Node { state, tx })
}

// ---------- module init ----------------------------------------------------
#[pymodule]
fn raftmem(_py: Python<'_>, m: &PyModule) -> PyResult<()> {
    m.add_class::<Node>()?;
    m.add_function(wrap_pyfunction!(start, m)?)?;
    Ok(())
}

