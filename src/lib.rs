// ===============================================================
// raftmem_rs 0.6 — explicit‐batch flush only, no signal handling
// ===============================================================

use anyhow::Result;
use memmap2::{MmapMut, MmapOptions};
use libc::{mprotect, PROT_READ, PROT_WRITE};
use numpy::{Element, PyArray1};
use numpy::npyffi::{PY_ARRAY_API, NpyTypes, NPY_ARRAY_WRITEABLE, npy_intp};
use once_cell::sync::Lazy;
use pyo3::{prelude::*, wrap_pyfunction, types::PyModule};
use pyo3::exceptions::PyRuntimeError;
use serde::{Deserialize, Serialize};
use serde_json;
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

mod storage;
mod network;
use storage::{new_mem_store, ClientRequest, ClientResponse};
use network::NetworkFactory;
use openraft::{Config, Raft};
use openraft::impls::TokioRuntime;
use std::collections::HashMap;

// Declare the application TypeConfig for OpenRaft using our buffer request/response.
openraft::declare_raft_types!(
    /// Type configuration for raftmem: full-buffer snapshot entries.
    pub TypeConfig:
        D = ClientRequest,
        R = ClientResponse,
        AsyncRuntime = TokioRuntime,
);

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

/// Handle an incoming RPC over TCP, dispatching to the Raft node and returning a response.
async fn handle_rpc(raft: Arc<Raft<TypeConfig>>, mut sock: TcpStream) {
    use crate::network::RpcEnvelope;
    // RPCOption not needed as v0.10 network traits no longer use options.
    use openraft::raft::{AppendEntriesRequest, InstallSnapshotRequest, VoteRequest};

    // Read request length prefix
    let mut len_buf = [0u8; 4];
    if sock.read_exact(&mut len_buf).await.is_err() {
        return;
    }
    let req_len = u32::from_be_bytes(len_buf) as usize;
    let mut req_buf = vec![0u8; req_len];
    if sock.read_exact(&mut req_buf).await.is_err() {
        return;
    }
    // Deserialize envelope with generic JSON value
    let raw: RpcEnvelope<serde_json::Value> = match serde_json::from_slice(&req_buf) {
        Ok(r) => r,
        Err(_) => return,
    };
    // Dispatch based on URI and convert response to JSON value
    let rpc_response = match raw.uri.as_str() {
        "append" => {
            let req: AppendEntriesRequest<TypeConfig> = match serde_json::from_value(raw.rpc) {
                Ok(r) => r,
                Err(_) => return,
            };
            let res = match raft.append_entries(req).await {
                Ok(res) => res,
                Err(_) => return,
            };
            match serde_json::to_value(res) {
                Ok(v) => v,
                Err(_) => return,
            }
        }
        "snapshot" => {
            let req: InstallSnapshotRequest<TypeConfig> = match serde_json::from_value(raw.rpc) {
                Ok(r) => r,
                Err(_) => return,
            };
            let res = match raft.install_snapshot(req).await {
                Ok(res) => res,
                Err(_) => return,
            };
            match serde_json::to_value(res) {
                Ok(v) => v,
                Err(_) => return,
            }
        }
        "vote" => {
            let req: VoteRequest<TypeConfig> = match serde_json::from_value(raw.rpc) {
                Ok(r) => r,
                Err(_) => return,
            };
            let res = match raft.vote(req).await {
                Ok(res) => res,
                Err(_) => return,
            };
            match serde_json::to_value(res) {
                Ok(v) => v,
                Err(_) => return,
            }
        }
        _ => return,
    };
    let resp_env = RpcEnvelope { uri: raw.uri, rpc: rpc_response };
    let send_buf = match serde_json::to_vec(&resp_env) {
        Ok(buf) => buf,
        Err(_) => return,
    };
    let len_bytes = (send_buf.len() as u32).to_be_bytes();
    let _ = sock.write_all(&len_bytes).await;
    let _ = sock.write_all(&send_buf).await;
}


// ---------- Python bindings -------------------------------------------
#[pyclass]
struct Node {
    shared: Shared,
    raft: Arc<Raft<TypeConfig>>,
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

    /// Manual flush helper if the user wants to push immediately via Raft.
    fn flush(&self) -> PyResult<()> {
        let raft = self.raft.clone();
        let shared = self.shared.clone();
        RT.spawn(async move {
            let len = shared.0.mm.len();
            let data = unsafe { std::slice::from_raw_parts(shared.0.mm.as_ptr(), len).to_vec() };
            let _ = raft.client_write(ClientRequest { data }).await;
        });
        Ok(())
    }
    /// Construct a new Node by binding to `listen`, specifying our `node_id`, and peers to join.
    #[new]
    fn new(node_id: u64, listen: String, peers: Vec<String>) -> PyResult<Self> {
        Python::with_gil(|py| {
            let peer_slices: Vec<&str> = peers.iter().map(String::as_str).collect();
            start(py, node_id, &listen, peer_slices)
        })
    }
}


/// Start a new Node that listens on `listen` (and immediately spawns a Tokio task for it)
/// and also knows how to broadcast to the given `peers`.
/// Start a new Raft node that listens for Raft RPCs, joins the given peers, and applies state to the mmap.
#[pyfunction]
fn start(_py: Python<'_>, node_id: u64, listen: &str, peers: Vec<&str>) -> PyResult<Node> {
    // 1) Create an anonymous mmap for 10 f64s and wrap in Shared.
    let buf = MmapBuf::new().map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
    let shared = Shared(Arc::new(buf));

    // 2) Initialize in-memory Raft log store and state machine store.
    let (log_store, state_machine) = new_mem_store(shared.clone());

    // 3) Parse peer addresses and build network factory.
    let mut peers_map = HashMap::new();
    for p in peers {
        let addr: SocketAddr = p.parse().map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
        peers_map.insert(addr.port() as u64, addr);
    }
    let network = NetworkFactory::new(peers_map.clone());

    // 4) Build and validate Raft config.
    let mut cfg = Config::default();
    cfg.validate().map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
    let cfg = Arc::new(cfg);

    // 5) Create Raft instance.
    let raft = RT.block_on(async move {
        Raft::new(node_id, cfg.clone(), network, log_store.clone(), state_machine.clone())
            .await
            .map_err(|e| PyRuntimeError::new_err(format!("Raft init error: {e}")))
            .map(Arc::new)
    })?;

    // 6) Spawn RPC listener for Raft network-v1 TCP envelope protocol.
    let std_listener = std::net::TcpListener::bind(listen)
        .map_err(|e| PyRuntimeError::new_err(format!("bind {listen}: {e}")))?;
    std_listener.set_nonblocking(true)
        .map_err(|e| PyRuntimeError::new_err(format!("non-blocking: {e}")))?;
    let tokio_listener = {
        let _g = RT.enter();
        TcpListener::from_std(std_listener)
            .map_err(|e| PyRuntimeError::new_err(format!("to-tokio: {e}")))?
    };
    let raft2 = raft.clone();
    RT.spawn(async move {
        loop {
            let (sock, _) = tokio_listener.accept().await.unwrap();
            let raft_cloned = raft2.clone();
            tokio::spawn(async move { handle_rpc(raft_cloned, sock).await });
        }
    });

    // 7) Bootstrap cluster membership (including self).
    let mut membership: Vec<u64> = Vec::with_capacity(peers_map.len() + 1);
    membership.push(node_id);
    membership.extend(peers_map.keys().cloned());
    RT.block_on(async { raft.change_membership(membership).await.map_err(|e| PyRuntimeError::new_err(format!("change_membership error: {e}"))) })?;

    Ok(Node { shared, raft })
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
        // Propose the current buffer as a Raft entry and wait for commit, then restore read-only protection.
        let (raft, shared) = Python::with_gil(|py| {
            let cell = self.node.as_ref(py).borrow();
            (cell.raft.clone(), cell.shared.clone())
        });
        let len = shared.0.mm.len();
        let data = unsafe { std::slice::from_raw_parts(shared.0.mm.as_ptr(), len).to_vec() };
        RT.block_on(async {
            raft.client_write(ClientRequest { data })
                .await
                .map_err(|e| PyRuntimeError::new_err(format!("client_write error: {e}")))?;
            Ok::<(), PyRuntimeError>(())
        })?;
        let ret = unsafe { mprotect(shared.0.mm.as_ptr() as *mut _, len, PROT_READ) };
        if ret != 0 {
            return Err(PyRuntimeError::new_err("mprotect(PROT_READ) failed in write() exit"));
        }
        Ok(())
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

