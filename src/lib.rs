mod memory;
mod net;

use memory::{MmapBuf, Shared};
use net::{client, serve, Update, UpdatePacket, Subscription, Mapping};

use once_cell::sync::Lazy;
use pyo3::prelude::*;
use pyo3::types::PyModule;
use pyo3::exceptions::{PyRuntimeError, PyValueError};
use numpy::{Element, PyArray1};
use numpy::npyffi::{PY_ARRAY_API, NpyTypes, NPY_ARRAY_WRITEABLE, npy_intp};
use std::cell::RefCell;
use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::runtime::Runtime;
use std::net::SocketAddr;
use std::os::raw::{c_int, c_void};
use libc::{PROT_READ};
use std::collections::HashMap;

static RUNTIME: Lazy<Runtime> = Lazy::new(|| Runtime::new().expect("tokio"));

#[pyclass]
struct Node {
    state: Shared,
    tx: async_channel::Sender<UpdatePacket>,
    #[pyo3(get)]
    name: String,
    shape: Vec<usize>,
    len: usize,
    scratch: RefCell<Vec<f64>>,
    meta_queue: Arc<Mutex<Vec<String>>>,
    pending_meta: Arc<Mutex<Option<String>>>,
    callback: RefCell<Option<Py<PyAny>>>,
    named: Arc<HashMap<String, Shared>>,
    version: Arc<AtomicU64>,
}

#[pymethods]
impl Node {
    #[getter]
    fn version(&self) -> u64 {
        self.version.load(Ordering::SeqCst)
    }
    fn ndarray<'py>(&'py self, py: Python<'py>, name: Option<&str>) -> Option<&'py PyArray1<f64>> {
        let (ptr, shape) = if let Some(n) = name {
            let shared = self.named.get(n)?;
            (shared.mm.ptr(), shared.shape())
        } else {
            (self.state.mm.ptr(), self.shape.as_slice())
        };

        let len: usize = shape.iter().product();
        let dims: [npy_intp; 1] = [len as npy_intp];
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
                ptr,
                NPY_ARRAY_WRITEABLE,
                std::ptr::null_mut(),
            );
            Some(PyArray1::from_owned_ptr(py, arr_ptr))
        }
    }

    fn flush(&self, _idx: usize) {
        let mut scratch = self.scratch.borrow_mut();
        if scratch.len() != self.len {
            scratch.resize(self.len, 0.0);
        }
        let mut ranges: Vec<(usize, usize)> = Vec::new();
        let mut start: Option<usize> = None;
        let mut end = 0usize;
        for i in 0..self.len {
            let v = self.state.get(i);
            if scratch[i] != v {
                if start.is_none() {
                    start = Some(i);
                }
                end = i;
                scratch[i] = v;
            } else if let Some(s) = start {
                ranges.push((s, end));
                start = None;
            }
        }
        if let Some(s) = start {
            ranges.push((s, end));
        }
        let meta = self.pending_meta.lock().unwrap().clone();
        if ranges.is_empty() && meta.is_none() {
            return;
        }
        let updates: Vec<Update> = ranges
            .into_iter()
            .map(|(s, e)| {
                let len = e - s + 1;
                Update { start: s as u32, len: len as u32 }
            })
            .collect();
        let version = self.version.fetch_add(1, Ordering::SeqCst) + 1;
        let packet = UpdatePacket { updates, meta, version };
        let _ = self.tx.try_send(packet);
    }

    fn send_meta(&self, meta: &PyAny) -> PyResult<()> {
        let py = meta.py();
        let json = PyModule::import(py, "json")?.call_method1("dumps", (meta,))?.extract::<String>()?;
        *self.pending_meta.lock().unwrap() = Some(json);
        Ok(())
    }

    fn on_update(&self, cb: PyObject) {
        *self.callback.borrow_mut() = Some(cb);
    }

    fn process_meta(&self, py: Python<'_>) -> PyResult<()> {
        if let Some(cb) = self.callback.borrow().as_ref() {
            let loads = PyModule::import(py, "json")?.getattr("loads")?;
            let mut q = self.meta_queue.lock().unwrap();
            for item in q.drain(..) {
                let obj = loads.call1((item,))?;
                cb.call1(py, (obj,))?;
            }
        }
        Ok(())
    }

    fn write<'py>(slf: PyRef<'py, Self>) -> PyResult<WriteGuard> {
        slf.state.start_write().map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
        Ok(WriteGuard { node: slf.into(), active: true })
    }

    fn read<'py>(slf: PyRef<'py, Self>) -> PyResult<ReadGuard> {
        let py = slf.py();
        slf.process_meta(py)?;
        let node_ref: Py<Node> = unsafe { Py::from_borrowed_ptr(py, slf.as_ptr()) };
        let mut scratch = slf.scratch.borrow_mut();
        if scratch.len() != slf.len {
            scratch.resize(slf.len, 0.0);
        }
        slf.state.read_snapshot(&mut scratch);
        let data = std::mem::take(&mut *scratch);
        Ok(ReadGuard { node: node_ref, arr: Some(data) })
    }
}

fn flush_now(node: &Node) {
    node.flush(0);
}

#[pyclass(unsendable)]
struct WriteGuard {
    node: Py<Node>,
    active: bool,
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
            cell.state.end_write().map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
            PyResult::Ok(())
        })?;
        self.active = false;
        Ok(())
    }
}

#[pyclass(unsendable)]
struct ReadGuard {
    node: Py<Node>,
    arr: Option<Vec<f64>>,
}

#[pymethods]
impl ReadGuard {
    fn __enter__<'py>(mut slf: PyRefMut<'py, Self>, py: Python<'py>) -> &'py PyArray1<f64> {
        let arr = slf.arr.as_mut().unwrap();
        let dims: [npy_intp; 1] = [arr.len() as npy_intp];
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
                arr.as_mut_ptr() as *mut c_void,
                0,
                std::ptr::null_mut(),
            );
            PyArray1::from_owned_ptr(py, arr_ptr)
        }
    }

    fn __exit__(&mut self, _t: &PyAny, _v: &PyAny, _tb: &PyAny) -> PyResult<()> {
        Python::with_gil(|py| {
            if let Some(arr) = self.arr.take() {
                let cell = self.node.as_ref(py).borrow();
                *cell.scratch.borrow_mut() = arr;
            }
            Ok(())
        })
    }
}

#[pyfunction]
#[pyo3(signature = (name, listen=None, server=None, shape=None, maps=None, on_update=None, on_update_async=None, event_loop=None, check_hash=false))]
fn start(
    py: Python<'_>,
    name: &str,
    listen: Option<&str>,
    server: Option<&str>,
    shape: Option<Vec<usize>>,
    maps: Option<Vec<(Vec<usize>, Vec<usize>, Option<Vec<usize>>, Option<String>)>>,
    on_update: Option<PyObject>,
    on_update_async: Option<PyObject>,
    event_loop: Option<PyObject>,
    check_hash: bool,
) -> PyResult<Py<Node>> {
    let shape = shape.unwrap_or_else(|| vec![10]);
    let len: usize = shape.iter().product();
    let buf = MmapBuf::new(shape.clone()).map_err(|e| PyValueError::new_err(e.to_string()))?;
    let state = Shared::new(buf);
    let (tx, rx) = async_channel::bounded::<UpdatePacket>(1024);
    let meta_queue = Arc::new(Mutex::new(Vec::new()));
    let pending_meta = Arc::new(Mutex::new(None));
    let version = Arc::new(AtomicU64::new(0));
    let mut named_map: HashMap<String, Shared> = HashMap::new();
    let subscription: Option<Vec<Mapping>> = if let Some(v) = maps {
        let mut out = Vec::new();
        for (srv_start, region_shape, client_start, name) in v {
            let ss_u32: Vec<u32> = srv_start.iter().map(|&d| d as u32).collect();
            let sh_u32: Vec<u32> = region_shape.iter().map(|&d| d as u32).collect();
            let target = if let Some(nm) = name {
                let buf = MmapBuf::new(region_shape.clone()).map_err(|e| PyValueError::new_err(e.to_string()))?;
                let shared = Shared::new(buf);
                named_map.insert(nm.clone(), shared);
                net::Target::Named(nm)
            } else {
                let cs = client_start.unwrap_or_else(|| vec![0; srv_start.len()]);
                let cs_u32: Vec<u32> = cs.iter().map(|&d| d as u32).collect();
                net::Target::Region(cs_u32)
            };
            out.push(Mapping { server_start: ss_u32, shape: sh_u32, target });
        }
        Some(out)
    } else { None };
    let named_arc = Arc::new(named_map);

    if let Some(addr) = listen {
        let listen_addr: SocketAddr = addr.parse()
            .map_err(|e: std::net::AddrParseError| PyValueError::new_err(e.to_string()))?;
        let st_clone = state.clone();
        let rx_clone = rx.clone();
        let pm = pending_meta.clone();
        RUNTIME.spawn(serve(listen_addr, rx_clone, st_clone, pm));
    }

    if let Some(addr) = server {
        let server_addr: SocketAddr = addr.parse()
            .map_err(|e: std::net::AddrParseError| PyValueError::new_err(e.to_string()))?;
        let st_clone = state.clone();
        let mq = meta_queue.clone();
        let sub_maps = subscription.clone().unwrap_or_else(|| {
            vec![Mapping {
                server_start: vec![0u32; shape.len()],
                shape: shape.iter().map(|&d| d as u32).collect(),
                target: net::Target::Region(vec![0u32; shape.len()])
            }]
        });
        let sub = Subscription { name: name.to_string(), client_shape: shape.iter().map(|&d| d as u32).collect(), maps: sub_maps, hash_check: check_hash };
        let named_clone = named_arc.clone();
        let ver_clone = version.clone();
        RUNTIME.spawn(client(server_addr, st_clone, named_clone, mq, ver_clone, sub));
    }

    state.protect(PROT_READ).map_err(|e| PyRuntimeError::new_err(e.to_string()))?;

    println!("node {} running with listen={:?} server={:?} shape {:?}", name, listen, server, shape);
    let node = Py::new(py, Node {
        state,
        tx,
        name: name.to_string(),
        shape,
        len,
        scratch: RefCell::new(vec![0.0; len]),
        meta_queue: meta_queue.clone(),
        pending_meta: pending_meta.clone(),
        callback: RefCell::new(None),
        named: named_arc.clone(),
        version: version.clone(),
    })?;

    if let Some(cb) = on_update {
        node.as_ref(py).call_method1("on_update", (cb,))?;
        let node_clone = node.clone();
        std::thread::spawn(move || {
            loop {
                Python::with_gil(|py| {
                    let cell = node_clone.as_ref(py).borrow();
                    let _ = cell.process_meta(py);
                });
                std::thread::sleep(std::time::Duration::from_millis(10));
            }
        });
    }

    if let (Some(cb), Some(loop_obj)) = (on_update_async, event_loop) {
        let mq = meta_queue.clone();
        let node_clone = node.clone();
        std::thread::spawn(move || {
            loop {
                let metas = {
                    let mut q = mq.lock().unwrap();
                    if q.is_empty() { None } else { Some(q.drain(..).collect::<Vec<_>>()) }
                };
                if let Some(items) = metas {
                    Python::with_gil(|py| {
                        let loads = py.import("json").unwrap().getattr("loads").unwrap();
                        for m in items {
                            let obj = loads.call1((m,)).unwrap();
                            let coro = cb.as_ref(py).call1((node_clone.clone_ref(py), obj)).unwrap();
                            py.import("asyncio").unwrap()
                                .call_method1("run_coroutine_threadsafe", (coro, loop_obj.as_ref(py)))
                                .unwrap();
                        }
                    });
                }
                std::thread::sleep(std::time::Duration::from_millis(10));
            }
        });
    }

    Ok(node)
}

#[pymodule]
fn memblast(_py: Python<'_>, m: &PyModule) -> PyResult<()> {
    m.add_class::<Node>()?;
    m.add_class::<WriteGuard>()?;
    m.add_class::<ReadGuard>()?;
    m.add_function(wrap_pyfunction!(start, m)?)?;
    Ok(())
}

