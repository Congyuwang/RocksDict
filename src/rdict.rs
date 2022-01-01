use crate::encoder::{decode_value, encode_key, encode_value};
use crate::{FlushOptionsPy, OptionsPy, RdictIter, ReadOpt, ReadOptionsPy, WriteOptionsPy};
use pyo3::exceptions::PyException;
use pyo3::prelude::*;
use pyo3::types::PyList;
use rocksdb::db::DBAccess;
use rocksdb::{ReadOptions, WriteOptions, DB};
use std::fs::create_dir_all;
use std::ops::Deref;
use std::path::Path;
use std::sync::Arc;

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
#[pyclass(name = "RdictInner", subclass)]
#[pyo3(text_signature = "(path, options)")]
pub(crate) struct Rdict {
    db: Option<Arc<DB>>,
    write_opt: WriteOptions,
    flush_opt: FlushOptionsPy,
    read_opt: ReadOptions,
    pickle_loads: PyObject,
    pickle_dumps: PyObject,
}

#[pymethods]
impl Rdict {
    /// Create a new database or open an existing one.
    //
    // Args:
    //     path: path to the database
    //     options: Options object
    #[new]
    #[args(options = "Python::with_gil(|py| Py::new(py, OptionsPy::new()).unwrap())")]
    fn new(path: &str, options: Py<OptionsPy>, py: Python) -> PyResult<Self> {
        let path = Path::new(path);
        let pickle = PyModule::import(py, "pickle")?.to_object(py);
        match create_dir_all(path) {
            Ok(_) => match DB::open(&options.borrow(py).0, &path) {
                Ok(db) => Ok(Rdict {
                    db: Some(Arc::new(db)),
                    write_opt: WriteOptions::default(),
                    flush_opt: FlushOptionsPy::new(),
                    read_opt: ReadOptions::default(),
                    pickle_loads: pickle.getattr(py, "loads")?,
                    pickle_dumps: pickle.getattr(py, "dumps")?,
                }),
                Err(e) => Err(PyException::new_err(e.to_string())),
            },
            Err(e) => Err(PyException::new_err(e.to_string())),
        }
    }

    /// Optionally disable WAL or sync for this write.
    ///
    /// Example:
    ///
    /// Making an unsafe write of a batch:
    /// ::
    ///     ```python
    ///     from rocksdict import Rdict, Options, WriteBatch, WriteOptions
    ///
    ///     path = "_path_for_rocksdb_storageY1"
    ///     db = Rdict(path, Options())
    ///
    ///     # set write options
    ///     write_options = WriteOptions()
    ///     write_options.set_sync(False)
    ///     write_options.disable_wal(True)
    ///     db.set_write_options(write_options)
    ///
    ///     # write to db
    ///     db["my key"] = "my value"
    ///     db["key2"] = "value2"
    ///     db["key3"] = "value3"
    ///
    ///     # remove db
    ///     del db
    ///     Rdict.destroy(path, Options())
    /// ```
    #[pyo3(text_signature = "($self, write_opt)")]
    fn set_write_options(&mut self, write_opt: PyRef<WriteOptionsPy>) {
        self.write_opt = write_opt.deref().into()
    }

    /// Optionally wait for the memtable flush to be performed.
    ///
    /// Example:
    ///
    /// Manually flushing the memtable:
    /// ::
    ///     ```python
    ///     from rocksdb import Rdict, Options, FlushOptions
    ///
    ///     path = "_path_for_rocksdb_storageY2"
    ///     db = Rdict(path, Options())
    ///
    ///     flush_options = FlushOptions()
    ///     flush_options.set_wait(True)
    ///
    ///     db.set_flush_opt(flush_options)
    ///     del db
    ///     Rdict.destroy(path, Options())
    /// ```
    #[pyo3(text_signature = "($self, flush_opt)")]
    fn set_flush_options(&mut self, flush_opt: PyRef<FlushOptionsPy>) {
        self.flush_opt = *flush_opt.deref()
    }

    /// Configure Read Options.
    #[pyo3(text_signature = "($self, read_opt)")]
    fn set_read_options(&mut self, read_opt: &ReadOptionsPy) {
        self.read_opt = read_opt.into()
    }

    /// Supports batch get:
    fn __getitem__(&self, key: &PyAny, py: Python) -> PyResult<PyObject> {
        if let Some(db) = &self.db {
            // batch_get
            if let Ok(keys) = PyTryFrom::try_from(key) {
                return Ok(
                    get_batch_inner(db, keys, py, &self.read_opt, &self.pickle_loads)?
                        .to_object(py),
                );
            }
            // single get
            let key = encode_key(key)?;
            match db.get_pinned_opt(&key[..], &self.read_opt) {
                Ok(value) => match value {
                    None => Err(PyException::new_err("key not found")),
                    Some(slice) => decode_value(py, slice.as_ref(), &self.pickle_loads),
                },
                Err(e) => Err(PyException::new_err(e.to_string())),
            }
        } else {
            Err(PyException::new_err("DB already closed"))
        }
    }

    fn __setitem__(&self, key: &PyAny, value: &PyAny, py: Python) -> PyResult<()> {
        if let Some(db) = &self.db {
            let key = encode_key(key)?;
            let value = encode_value(value, &self.pickle_dumps, py)?;
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
            let key = encode_key(key)?;
            if db.key_may_exist_opt(&key[..], &self.read_opt) {
                match db.get_pinned_opt(&key[..], &self.read_opt) {
                    Ok(value) => match value {
                        None => Ok(false),
                        Some(_) => Ok(true),
                    },
                    Err(e) => Err(PyException::new_err(e.to_string())),
                }
            } else {
                Ok(false)
            }
        } else {
            Err(PyException::new_err("DB already closed"))
        }
    }

    fn __delitem__(&self, key: &PyAny) -> PyResult<()> {
        if let Some(db) = &self.db {
            let key = encode_key(key)?;
            match db.delete_opt(&key[..], &self.write_opt) {
                Ok(_) => Ok(()),
                Err(e) => Err(PyException::new_err(e.to_string())),
            }
        } else {
            Err(PyException::new_err("DB already closed"))
        }
    }

    /// Flush memory to disk, and drop the database.
    //
    // Notes:
    //     Setting Rdict to `None` does not always immediately close
    //     the database depending on the garbage collector of python.
    //     Calling `close()` is a more reliable method to ensure
    //     that the database is correctly closed.
    //
    //     The database would not be usable after `close()` is called.
    //     Calling method after `close()` will throw exception.
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

    /// Delete the database.
    //
    // Args:
    //     path (str): path to this database
    //     options (rocksdict.Options): Rocksdb options object
    #[staticmethod]
    #[pyo3(text_signature = "(path, options)")]
    fn destroy(path: &str, options: PyRef<OptionsPy>) -> PyResult<()> {
        match DB::destroy(&options.0, path) {
            Ok(_) => Ok(()),
            Err(e) => Err(PyException::new_err(e.to_string())),
        }
    }

    /// Reversible for iterating over keys and values.
    //
    // Examples:
    //     ::
    //
    //     ```python
    //     from rocksdict import Rdict, Options, ReadOptions
    //
    //     path = "_path_for_rocksdb_storage5"
    //     db = Rdict(path, Options())
    //
    //     for i in range(50):
    //         db[i] = i ** 2
    //
    //     iter = db.iter(ReadOptions())
    //
    //     iter.seek_to_first()
    //
    //     j = 0
    //     while iter.valid():
    //         assert iter.key() == j
    //         assert iter.value() == j ** 2
    //         print(f"{iter.key()} {iter.value()}")
    //         iter.next()
    //         j += 1
    //
    //     iter.seek_to_first();
    //     assert iter.key() == 0
    //     assert iter.value() == 0
    //     print(f"{iter.key()} {iter.value()}")
    //
    //     iter.seek(25)
    //     assert iter.key() == 25
    //     assert iter.value() == 625
    //     print(f"{iter.key()} {iter.value()}")
    //
    //     del iter, db
    //     Rdict.destroy(path, Options())
    //     ```
    //
    // Args:
    //     read_opt: ReadOptions
    //
    // Returns: Reversible
    #[pyo3(text_signature = "($self, read_opt)")]
    fn iter(&self, read_opt: &ReadOptionsPy) -> PyResult<RdictIter> {
        if let Some(db) = &self.db {
            let readopts: ReadOpt = read_opt.into();
            Ok(unsafe {
                RdictIter {
                    db: db.clone(),
                    inner: librocksdb_sys::rocksdb_create_iterator(db.inner(), readopts.0),
                    readopts,
                    pickle_loads: self.pickle_loads.clone(),
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
    pickle_loads: &PyObject,
) -> PyResult<&'a PyList> {
    let mut keys_batch = Vec::new();
    for key in keys {
        keys_batch.push(encode_key(key)?);
    }
    let values = db.multi_get_opt(keys_batch, read_opt);
    let result = PyList::empty(py);
    for v in values {
        match v {
            Ok(value) => match value {
                None => result.append(py.None())?,
                Some(slice) => result.append(decode_value(py, slice.as_ref(), pickle_loads)?)?,
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
