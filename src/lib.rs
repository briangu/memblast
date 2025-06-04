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
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use std::net::SocketAddr;
use tokio::{net::{TcpListener, TcpStream}, runtime::Runtime};
use std::io::ErrorKind;

// ---------- global Tokio runtime (one per process) -------------------------
static RUNTIME: Lazy<Runtime> = Lazy::new(|| Runtime::new().expect("tokio"));

// ---------- in‑memory state -------------------------------------------------
#[derive(Clone)]
struct Shared {
    arr: Py<PyArray1<f64>>,
}
impl Shared {
    fn apply(&self, idx: usize, val: f64) {
        Python::with_gil(|py| {
            let array = self.arr.as_ref(py);
            if let Ok(mut view) = array.try_readwrite() {
                if let Ok(slice) = view.as_slice_mut() {
                    if idx < slice.len() {
                        slice[idx] = val;
                    }
                }
            }
        });
    }
    fn get(&self, idx: usize) -> f64 {
        Python::with_gil(|py| {
            let array = self.arr.as_ref(py);
            array.readonly().as_slice().unwrap()[idx]
        })
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
        // Expose the shared NumPy array itself
        self.state.arr.as_ref(py)
    }
    fn flush(&self, idx: usize) {
        let val = self.state.get(idx);
        println!("{} flushing idx {} with {}", self.name, idx, val);
        let _ = self.tx.try_send(Update { idx: idx as u32, val });
    }
}

#[pyfunction]
#[pyo3(signature = (name, listen, peers))]
fn start(py: Python<'_>, name: &str, listen: &str, peers: Vec<&str>) -> PyResult<Node> {
    let array = PyArray1::<f64>::zeros(py, 10, false);
    let state = Shared { arr: array.into_py(py) };
    let (tx, rx) = async_channel::bounded(1024);

    let listen_addr: SocketAddr = listen.parse()
        .map_err(|e: std::net::AddrParseError| PyValueError::new_err(e.to_string()))?;
    let peer_addrs: Vec<SocketAddr> = peers.into_iter().filter_map(|p| p.parse().ok()).collect();

    let st_clone = state.clone();
    RUNTIME.spawn(listener(listen_addr, st_clone));
    RUNTIME.spawn(broadcaster(peer_addrs, rx));

    println!("node {} running on {}", name, listen);
    Ok(Node { name: name.to_string(), state, tx })
}

// ---------- module init ----------------------------------------------------
#[pymodule]
fn raftmem(_py: Python<'_>, m: &PyModule) -> PyResult<()> {
    m.add_class::<Node>()?;
    m.add_function(wrap_pyfunction!(start, m)?)?;
    Ok(())
}

