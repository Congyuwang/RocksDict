use crate::encoder::{decode_value, encode_value};
use crate::{FlushOptionsPy, OptionsPy, WriteOptionsPy};
use pyo3::exceptions::PyException;
use pyo3::prelude::*;
use pyo3::types::PyList;
use rocksdb::{FlushOptions, WriteOptions, DB};
use std::fs::create_dir_all;
use std::path::Path;

/// Option<DB> so it can be destroyed
#[pyclass]
pub(crate) struct Rdict {
    db: Option<DB>,
    write_opt: WriteOptions,
    flush_opt: FlushOptionsPy,
    read_opt: ReadOptions,
}

///
/// Note that we do not support __len__()
///
#[pymethods]
impl Rdict {
    #[new]
    fn new(path: &str, options: PyRef<OptionsPy>) -> PyResult<Self> {
        let path = Path::new(path);
        let write_opt = WriteOptions::default();
        match create_dir_all(path) {
            Ok(_) => match DB::open(&options.0, &path) {
                Ok(db) => Ok(Rdict {
                    db: Some(db),
                    write_opt: WriteOptions::default(),
                    flush_opt: FlushOptionsPy::new(),
                    read_opt: ReadOptions::default(),
                }),
                Err(e) => Err(PyException::new_err(e.to_string())),
            },
            Err(e) => Err(PyException::new_err(e.to_string())),
        }
    }

    fn set_write_options(&mut self, write_opt: PyRef<WriteOptionsPy>) {
        self.write_opt = write_opt.to_rust()
    }

    fn set_flush_options(&mut self, flush_opt: PyRef<FlushOptionsPy>) {
        self.flush_opt = flush_opt.clone()
    }

    ///
    /// Supports batch get
    ///
    fn __getitem__(&self, key: &PyAny, py: Python) -> PyResult<PyObject> {
        if let Some(db) = &self.db {
            let key = encode_value(key)?;
            match db.get_pinned_opt(&key[..], &self.read_opt) {
                Ok(value) => match value {
                    None => Err(PyException::new_err("key not found")),
                    Some(slice) => decode_value(py, slice.as_ref()),
                },
                Err(e) => Err(PyException::new_err(e.to_string())),
            }
        } else {
            Err(PyException::new_err("DB already closed"))
        }
    }

    fn get_batch<'a>(&self, keys: &'a PyList, py: Python<'a>) -> PyResult<&'a PyList> {
        if let Some(db) = &self.db {
            let mut keys_batch = Vec::new();
            for key in keys {
                keys_batch.push(encode_value(key)?);
            }
            let values = db.multi_get(keys_batch);
            let result = PyList::empty(py);
            for v in values {
                match v {
                    Ok(value) => match value {
                        None => result.append(py.None())?,
                        Some(slice) => result.append(decode_value(py, slice.as_ref())?)?,
                    },
                    Err(e) => return Err(PyException::new_err(e.to_string())),
                }
            }
            Ok(result)
        } else {
            Err(PyException::new_err("DB already closed"))
        }
    }

    fn __setitem__(&self, key: &PyAny, value: &PyAny) -> PyResult<()> {
        if let Some(db) = &self.db {
            let key = encode_value(key)?;
            let value = encode_value(value)?;
            match db.put_opt(&key[..], value, &self.write_opt) {
                Ok(_) => Ok(()),
                Err(e) => Err(PyException::new_err(e.to_string())),
            }
        } else {
            Err(PyException::new_err("DB already closed"))
        }
    }

    fn __contains__(&self, key: &PyAny) -> PyResult<bool> {
        if let Some(db) = &self.db {
            let key = encode_value(key)?;
            match db.get_pinned_opt(&key[..], &self.read_opt) {
                Ok(value) => match value {
                    None => Ok(false),
                    Some(_) => Ok(true),
                },
                Err(e) => Err(PyException::new_err(e.to_string())),
            }
        } else {
            Err(PyException::new_err("DB already closed"))
        }
    }

    fn __delitem__(&self, key: &PyAny) -> PyResult<()> {
        if let Some(db) = &self.db {
            let key = encode_value(key)?;
            match db.delete_opt(&key[..], &self.write_opt) {
                Ok(_) => Ok(()),
                Err(e) => Err(PyException::new_err(e.to_string())),
            }
        } else {
            Err(PyException::new_err("DB already closed"))
        }
    }

    /// flush mem-table, drop database
    fn close(&mut self) -> PyResult<()> {
        if let Some(db) = &self.db {
            match db.flush_opt(&self.flush_opt.to_rust()) {
                Ok(_) => Ok(drop(self.db.take().unwrap())),
                Err(e) => {
                    drop(self.db.take().unwrap());
                    Err(PyException::new_err(e.to_string()))
                }
            }
        } else {
            Err(PyException::new_err("DB already closed"))
        }
    }

    /// destroy database
    fn destroy(&mut self, options: PyRef<OptionsPy>) -> PyResult<()> {
        if let Some(db) = &self.db {
            let path = db.path().to_owned();
            drop(self.db.take().unwrap());
            match DB::destroy(&options.0, path) {
                Ok(_) => Ok(()),
                Err(e) => Err(PyException::new_err(e.to_string())),
            }
        } else {
            Err(PyException::new_err("DB already closed"))
        }
    }
}

impl Drop for Rdict {
    fn drop(&mut self) {
        if let Some(db) = self.db.take() {
            let _ = db.flush_opt(&self.flush_opt.to_rust());
        }
    }
}
