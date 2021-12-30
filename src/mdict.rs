use crate::encoder::{decode_value, encode_value};
use ahash::AHashMap;
use pyo3::exceptions::PyException;
use pyo3::prelude::*;
use pyo3::types::PyList;
use pyo3::{PyAny, PyObject, PyResult, Python};
use std::ops::{Deref, DerefMut};

#[pyclass]
pub(crate) struct Mdict(AHashMap<Box<[u8]>, Box<[u8]>>);

impl Deref for Mdict {
    type Target = AHashMap<Box<[u8]>, Box<[u8]>>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for Mdict {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

#[pymethods]
impl Mdict {
    #[new]
    fn new() -> Self {
        Mdict(AHashMap::new())
    }

    /// support get_batch
    fn __getitem__(&self, key: &PyAny, py: Python) -> PyResult<PyObject> {
        if let Ok(keys) = <PyList as PyTryFrom>::try_from(key) {
            let result = PyList::empty(py);
            // type annotation
            for key in keys {
                match self.get(&encode_value(key)?) {
                    None => result.append(py.None())?,
                    Some(slice) => result.append(decode_value(py, slice.as_ref())?)?,
                }
            }
            return Ok(result.to_object(py));
        }
        let key = encode_value(key)?;
        match self.get(&key[..]) {
            None => Err(PyException::new_err("key not found")),
            Some(slice) => decode_value(py, &slice[..]),
        }
    }

    fn __setitem__(&mut self, key: &PyAny, value: &PyAny) -> PyResult<()> {
        let key = encode_value(key)?;
        match encode_value(value) {
            Ok(value) => {
                self.insert(key, value);
                Ok(())
            }
            Err(e) => Err(PyException::new_err(e.to_string())),
        }
    }

    fn __contains__(&self, key: &PyAny) -> PyResult<bool> {
        let key = encode_value(key)?;
        Ok(self.contains_key(&key[..]))
    }

    fn __delitem__(&mut self, key: &PyAny) -> PyResult<()> {
        let key = encode_value(key)?;
        self.remove(&key[..]);
        Ok(())
    }

    fn __len__(&self) -> usize {
        self.len()
    }
}
