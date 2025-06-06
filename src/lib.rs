mod memory;
mod net;

use memory::{MmapBuf, Shared};
use net::{broadcaster, listener, Update};

use once_cell::sync::Lazy;
use pyo3::prelude::*;
use pyo3::types::PyModule;
use pyo3::exceptions::{PyRuntimeError, PyValueError};
use numpy::{Element, PyArray1};
use numpy::npyffi::{PY_ARRAY_API, NpyTypes, NPY_ARRAY_WRITEABLE, npy_intp};
use std::cell::RefCell;
use tokio::runtime::Runtime;
use std::net::SocketAddr;
use std::os::raw::{c_int, c_void};
use libc::{PROT_READ};

static RUNTIME: Lazy<Runtime> = Lazy::new(|| Runtime::new().expect("tokio"));

#[pyclass]
struct Node {
    name: String,
    state: Shared,
    tx: async_channel::Sender<Update>,
    shape: Vec<usize>,
    len: usize,
    scratch: RefCell<Vec<f64>>,
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
        let mut scratch = self.scratch.borrow_mut();
        if scratch.len() != self.len {
            scratch.resize(self.len, 0.0);
        }
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
            }
        }
        if let Some(s) = start {
            let vals = scratch[s..=end].to_vec();
            println!(
                "{} flushing range {}..{} ({} values)",
                self.name,
                s,
                end,
                vals.len()
            );
            let shape: Vec<u32> = self.shape.iter().map(|&d| d as u32).collect();
            let _ = self.tx.try_send(Update { shape, start: s as u32, vals });
        }
    }

    fn write<'py>(slf: PyRef<'py, Self>) -> PyResult<WriteGuard> {
        slf.state.start_write().map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
        Ok(WriteGuard { node: slf.into(), active: true })
    }

    fn read<'py>(slf: PyRef<'py, Self>) -> PyResult<ReadGuard> {
        let py = slf.py();
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
#[pyo3(signature = (name, listen, peers, shape=None))]
fn start(_py: Python<'_>, name: &str, listen: &str, peers: Vec<&str>, shape: Option<Vec<usize>>) -> PyResult<Node> {
    let shape = shape.unwrap_or_else(|| vec![10]);
    let len: usize = shape.iter().product();
    let buf = MmapBuf::new(shape.clone()).map_err(|e| PyValueError::new_err(e.to_string()))?;
    let state = Shared::new(buf);
    let (tx, rx) = async_channel::bounded(1024);

    let listen_addr: SocketAddr = listen.parse()
        .map_err(|e: std::net::AddrParseError| PyValueError::new_err(e.to_string()))?;
    let peer_addrs: Vec<SocketAddr> = peers.iter().filter_map(|p| p.parse().ok()).collect();

    let st_clone = state.clone();
    RUNTIME.spawn(listener(listen_addr, st_clone));
    RUNTIME.spawn(broadcaster(peer_addrs, rx));

    state.protect(PROT_READ).map_err(|e| PyRuntimeError::new_err(e.to_string()))?;

    println!("node {} running on {} with shape {:?}", name, listen, shape);
    Ok(Node {
        name: name.to_string(),
        state,
        tx,
        shape,
        len,
        scratch: RefCell::new(vec![0.0; len]),
    })
}

#[pymodule]
fn raftmem(_py: Python<'_>, m: &PyModule) -> PyResult<()> {
    m.add_class::<Node>()?;
    m.add_class::<WriteGuard>()?;
    m.add_class::<ReadGuard>()?;
    m.add_function(wrap_pyfunction!(start, m)?)?;
    Ok(())
}

