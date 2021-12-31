use crate::encoder::{decode_value, encode_value};
use crate::{FlushOptionsPy, OptionsPy, RdictIter, ReadOpt, ReadOptionsPy, WriteOptionsPy};
use pyo3::exceptions::PyException;
use pyo3::prelude::*;
use pyo3::types::PyList;
use rocksdb::{ReadOptions, WriteOptions, DB};
use std::fs::create_dir_all;
use std::ops::Deref;
use std::path::Path;
use std::sync::Arc;
use rocksdb::db::DBAccess;

///
/// A persistent on-disk dictionary. Supports string, int, float, bytes as key, values.
///
/// # Example
///
/// ```python
///
/// from rocksdict import Rdict
///
/// db = Rdict("./test_dir")
/// db[0] = 1
///
/// db = None
/// db = Rdict("./test_dir")
/// assert(db[0] == 1)
/// ```
///
#[pyclass(name = "RdictInner")]
#[pyo3(text_signature = "(path, options)")]
pub(crate) struct Rdict {
    db: Option<Arc<DB>>,
    write_opt: WriteOptions,
    flush_opt: FlushOptionsPy,
    read_opt: ReadOptions,
}

#[pymethods]
impl Rdict {
    #[new]
    #[args(options = "Python::with_gil(|py| Py::new(py, OptionsPy::new()).unwrap())")]
    fn new(path: &str, options: Py<OptionsPy>, py: Python) -> PyResult<Self> {
        let path = Path::new(path);
        match create_dir_all(path) {
            Ok(_) => match DB::open(&options.borrow(py).0, &path) {
                Ok(db) => Ok(Rdict {
                    db: Some(Arc::new(db)),
                    write_opt: WriteOptions::default(),
                    flush_opt: FlushOptionsPy::new(),
                    read_opt: ReadOptions::default(),
                }),
                Err(e) => Err(PyException::new_err(e.to_string())),
            },
            Err(e) => Err(PyException::new_err(e.to_string())),
        }
    }

    /// Optionally disable WAL or sync for this write.
    ///
    /// # Examples
    ///
    /// Making an unsafe write of a batch:
    ///
    /// ```python
    /// from rocksdict import Rdict, Options, WriteBatch, WriteOptions
    ///
    /// db = Rdict("_path_for_rocksdb_storageY1", Options())
    ///
    /// # set write options
    /// write_options = WriteOptions()
    /// write_options.set_sync(False)
    /// write_options.disable_wal(True)
    /// db.set_write_options(write_options)
    ///
    /// # write to db
    /// db["my key"] = "my value"
    /// db["key2"] = "value2"
    /// db["key3"] = "value3"
    ///
    /// # remove db
    /// db.destroy(Options())
    /// ```
    #[pyo3(text_signature = "($self, write_opt)")]
    fn set_write_options(&mut self, write_opt: PyRef<WriteOptionsPy>) {
        self.write_opt = write_opt.deref().into()
    }

    /// Optionally wait for the memtable flush to be performed.
    ///
    /// # Examples
    ///
    /// Manually flushing the memtable:
    ///
    /// ```python
    /// from rocksdb import Rdict, Options, FlushOptions
    ///
    /// path = "_path_for_rocksdb_storageY2"
    /// db = Rdict(path, Options())
    ///
    /// flush_options = FlushOptions()
    /// flush_options.set_wait(True)
    ///
    /// db.flush_opt(flush_options)
    /// db.destroy(Options())
    /// ```
    #[pyo3(text_signature = "($self, flush_opt)")]
    fn set_flush_options(&mut self, flush_opt: PyRef<FlushOptionsPy>) {
        self.flush_opt = *flush_opt.deref()
    }

    #[pyo3(text_signature = "($self, read_opt)")]
    fn set_read_options(&mut self, read_opt: &ReadOptionsPy) {
        self.read_opt = read_opt.into()
    }

    ///
    /// Supports batch get
    ///
    fn __getitem__(&self, key: &PyAny, py: Python) -> PyResult<PyObject> {
        if let Some(db) = &self.db {
            // batch_get
            if let Ok(keys) = PyTryFrom::try_from(key) {
                return Ok(get_batch_inner(db, keys, py, &self.read_opt)?.to_object(py));
            }
            // single get
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
    #[pyo3(text_signature = "($self)")]
    fn close(&mut self) -> PyResult<()> {
        if let Some(db) = &self.db {
            let f_opt = &self.flush_opt;
            match db.flush_opt(&f_opt.into()) {
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
    #[pyo3(text_signature = "($self, options)")]
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

    /// Iterate Over the Key-Value pairs.
    ///
    /// # Examples
    ///
    /// ```python
    /// from rocksdict import Rdict, Options, ReadOptions
    ///
    /// path = "_path_for_rocksdb_storage5"
    /// db = Rdict(path, Options())
    /// iter = db.iter(ReadOptions())
    ///
    /// # Iterate all keys from the start in lexicographic order
    /// iter.seek_to_first()
    ///
    /// while iter.valid():
    ///     print(f"{iter.key()} {iter.value()}")
    ///     iter.next()
    ///
    /// # Read just the first key
    /// iter.seek_to_first();
    /// print(f"{iter.key()} {iter.value()}")
    ///
    /// db.destroy(Options())
    /// ```
    #[pyo3(text_signature = "($self, read_opt)")]
    fn iter(&self, read_opt: &ReadOptionsPy) -> PyResult<RdictIter> {
        if let Some(db) = &self.db {
            let readopts: ReadOpt = read_opt.into();
            Ok(unsafe {
                RdictIter {
                    db: db.clone(),
                    inner: librocksdb_sys::rocksdb_create_iterator(db.inner(), readopts.0),
                    readopts,
                }
            })
        } else {
            Err(PyException::new_err("DB already closed"))
        }
    }
}

#[inline(always)]
fn get_batch_inner<'a>(
    db: &DB,
    keys: &'a PyList,
    py: Python<'a>,
    read_opt: &ReadOptions,
) -> PyResult<&'a PyList> {
    let mut keys_batch = Vec::new();
    for key in keys {
        keys_batch.push(encode_value(key)?);
    }
    let values = db.multi_get_opt(keys_batch, read_opt);
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
}

impl Drop for Rdict {
    fn drop(&mut self) {
        if let Some(db) = self.db.take() {
            let f_opt = &self.flush_opt;
            let _ = db.flush_opt(&f_opt.into());
        }
    }
}
