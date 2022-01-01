use crate::encoder::{decode_value, encode_key};
use crate::util::error_message;
use crate::ReadOpt;
use core::slice;
use libc::{c_char, c_uchar, size_t};
use librocksdb_sys;
use pyo3::exceptions::PyException;
use pyo3::prelude::*;
use pyo3::PyIterProtocol;
use rocksdb::db::DBAccess;
use rocksdb::DB;
use std::ptr::null_mut;
use std::sync::Arc;

#[pyclass]
#[allow(dead_code)]
pub(crate) struct RdictIter {
    /// iterator must keep a reference count of DB to keep DB alive.
    pub(crate) db: Arc<DB>,

    pub(crate) inner: *mut librocksdb_sys::rocksdb_iterator_t,

    /// When iterate_upper_bound is set, the inner C iterator keeps a pointer to the upper bound
    /// inside `_readopts`. Storing this makes sure the upper bound is always alive when the
    /// iterator is being used.
    pub(crate) readopts: ReadOpt,

    /// use pickle loads to convert bytes to pyobjects
    pub(crate) pickle_loads: PyObject,
}

#[pyclass]
pub(crate) struct RdictItems {
    inner: RdictIter,
    backwards: bool,
}

#[pyclass]
pub(crate) struct RdictKeys {
    inner: RdictIter,
    backwards: bool,
}

#[pyclass]
pub(crate) struct RdictValues {
    inner: RdictIter,
    backwards: bool,
}

impl RdictIter {
    pub(crate) fn new(db: &Arc<DB>, readopts: ReadOpt, pickle_loads: &PyObject) -> Self {
        unsafe {
            RdictIter {
                db: db.clone(),
                inner: librocksdb_sys::rocksdb_create_iterator(db.inner(), readopts.0),
                readopts,
                pickle_loads: pickle_loads.clone(),
            }
        }
    }
}

#[pymethods]
impl RdictIter {
    /// Returns `true` if the iterator is valid. An iterator is invalidated when
    /// it reaches the end of its defined range, or when it encounters an error.
    ///
    /// To check whether the iterator encountered an error after `valid` has
    /// returned `false`, use the [`status`](DBRawIteratorWithThreadMode::status) method. `status` will never
    /// return an error when `valid` is `true`.
    #[pyo3(text_signature = "($self)")]
    pub fn valid(&self) -> bool {
        unsafe { librocksdb_sys::rocksdb_iter_valid(self.inner) != 0 }
    }

    /// Returns an error `Result` if the iterator has encountered an error
    /// during operation. When an error is encountered, the iterator is
    /// invalidated and [`valid`](DBRawIteratorWithThreadMode::valid) will return `false` when called.
    ///
    /// Performing a seek will discard the current status.
    #[pyo3(text_signature = "($self)")]
    pub fn status(&self) -> PyResult<()> {
        let mut err: *mut c_char = null_mut();
        unsafe {
            librocksdb_sys::rocksdb_iter_get_error(self.inner, &mut err);
        }
        if !err.is_null() {
            Err(PyException::new_err(error_message(err)))
        } else {
            Ok(())
        }
    }

    /// Seeks to the first key in the database.
    ///
    /// Example:
    ///     ::
    ///
    ///         from rocksdict import Rdict, Options, ReadOptions
    ///
    ///         path = "_path_for_rocksdb_storage5"
    ///         db = Rdict(path, Options())
    ///         iter = db.iter(ReadOptions())
    ///
    ///         # Iterate all keys from the start in lexicographic order
    ///         iter.seek_to_first()
    ///
    ///         while iter.valid():
    ///             print(f"{iter.key()} {iter.value()}")
    ///             iter.next()
    ///
    ///         # Read just the first key
    ///         iter.seek_to_first();
    ///         print(f"{iter.key()} {iter.value()}")
    ///
    ///         del iter, db
    ///         Rdict.destroy(path, Options())
    #[pyo3(text_signature = "($self)")]
    pub fn seek_to_first(&mut self) {
        unsafe {
            librocksdb_sys::rocksdb_iter_seek_to_first(self.inner);
        }
    }

    /// Seeks to the last key in the database.
    ///
    /// Example:
    ///     ::
    ///
    ///         from rocksdict import Rdict, Options, ReadOptions
    ///
    ///         path = "_path_for_rocksdb_storage6"
    ///         db = Rdict(path, Options())
    ///         iter = db.iter(ReadOptions())
    ///
    ///         # Iterate all keys from the start in lexicographic order
    ///         iter.seek_to_last()
    ///
    ///         while iter.valid():
    ///             print(f"{iter.key()} {iter.value()}")
    ///             iter.prev()
    ///
    ///         # Read just the last key
    ///         iter.seek_to_last();
    ///         print(f"{iter.key()} {iter.value()}")
    ///
    ///         del iter, db
    ///         Rdict.destroy(path, Options())
    #[pyo3(text_signature = "($self)")]
    pub fn seek_to_last(&mut self) {
        unsafe {
            librocksdb_sys::rocksdb_iter_seek_to_last(self.inner);
        }
    }

    /// Seeks to the specified key or the first key that lexicographically follows it.
    ///
    /// This method will attempt to seek to the specified key. If that key does not exist, it will
    /// find and seek to the key that lexicographically follows it instead.
    ///
    /// Example:
    ///     ::
    ///
    ///         from rocksdict import Rdict, Options, ReadOptions
    ///
    ///         path = "_path_for_rocksdb_storage6"
    ///         db = Rdict(path, Options())
    ///         iter = db.iter(ReadOptions())
    ///
    ///         # Read the first string key that starts with 'a'
    ///         iter.seek("a");
    ///         print(f"{iter.key()} {iter.value()}")
    ///
    ///         del iter, db
    ///         Rdict.destroy(path, Options())
    #[pyo3(text_signature = "($self, key)")]
    pub fn seek(&mut self, key: &PyAny) -> PyResult<()> {
        let key = encode_key(key)?;

        Ok(unsafe {
            librocksdb_sys::rocksdb_iter_seek(
                self.inner,
                key.as_ptr() as *const c_char,
                key.len() as size_t,
            );
        })
    }

    /// Seeks to the specified key, or the first key that lexicographically precedes it.
    ///
    /// Like ``.seek()`` this method will attempt to seek to the specified key.
    /// The difference with ``.seek()`` is that if the specified key do not exist, this method will
    /// seek to key that lexicographically precedes it instead.
    ///
    /// Example:
    ///     ::
    ///
    ///         from rocksdict import Rdict, Options, ReadOptions
    ///
    ///         path = "_path_for_rocksdb_storage6"
    ///         db = Rdict(path, Options())
    ///         iter = db.iter(ReadOptions())
    ///
    ///         # Read the last key that starts with 'a'
    ///         seek_for_prev("b")
    ///         print(f"{iter.key()} {iter.value()}")
    ///
    ///         del iter, db
    ///         Rdict.destroy(path, Options())
    #[pyo3(text_signature = "($self, key)")]
    pub fn seek_for_prev(&mut self, key: &PyAny) -> PyResult<()> {
        let key = encode_key(key)?;

        Ok(unsafe {
            librocksdb_sys::rocksdb_iter_seek_for_prev(
                self.inner,
                key.as_ptr() as *const c_char,
                key.len() as size_t,
            );
        })
    }

    /// Seeks to the next key.
    #[pyo3(text_signature = "($self)")]
    pub fn next(&mut self) {
        unsafe {
            librocksdb_sys::rocksdb_iter_next(self.inner);
        }
    }

    /// Seeks to the previous key.
    #[pyo3(text_signature = "($self)")]
    pub fn prev(&mut self) {
        unsafe {
            librocksdb_sys::rocksdb_iter_prev(self.inner);
        }
    }

    /// Returns the current key.
    #[pyo3(text_signature = "($self)")]
    pub fn key(&self, py: Python) -> PyResult<PyObject> {
        if self.valid() {
            // Safety Note: This is safe as all methods that may invalidate the buffer returned
            // take `&mut self`, so borrow checker will prevent use of buffer after seek.
            unsafe {
                let mut key_len: size_t = 0;
                let key_len_ptr: *mut size_t = &mut key_len;
                let key_ptr =
                    librocksdb_sys::rocksdb_iter_key(self.inner, key_len_ptr) as *const c_uchar;
                let key = slice::from_raw_parts(key_ptr, key_len as usize);
                Ok(decode_value(py, key, &self.pickle_loads)?)
            }
        } else {
            Ok(py.None())
        }
    }

    /// Returns the current value.
    #[pyo3(text_signature = "($self)")]
    pub fn value(&self, py: Python) -> PyResult<PyObject> {
        if self.valid() {
            // Safety Note: This is safe as all methods that may invalidate the buffer returned
            // take `&mut self`, so borrow checker will prevent use of buffer after seek.
            unsafe {
                let mut val_len: size_t = 0;
                let val_len_ptr: *mut size_t = &mut val_len;
                let val_ptr =
                    librocksdb_sys::rocksdb_iter_value(self.inner, val_len_ptr) as *const c_uchar;
                let value = slice::from_raw_parts(val_ptr, val_len as usize);
                Ok(decode_value(py, value, &self.pickle_loads)?)
            }
        } else {
            Ok(py.None())
        }
    }
}

impl Drop for RdictIter {
    fn drop(&mut self) {
        unsafe {
            librocksdb_sys::rocksdb_iter_destroy(self.inner);
        }
    }
}

unsafe impl Send for RdictIter {}

macro_rules! impl_iter {
    ($iter_name: ident, $($field: ident),*) => {
        #[pyproto]
        impl PyIterProtocol for $iter_name {
            fn __iter__(slf: PyRef<Self>) -> PyRef<Self> {
                slf
            }

            fn __next__(mut slf: PyRefMut<Self>) -> PyResult<Option<PyObject>> {
                if slf.inner.valid() {
                    $(let $field = Python::with_gil(|py| slf.inner.$field(py))?;)*
                    if slf.backwards {
                        slf.inner.prev();
                    } else {
                        slf.inner.next();
                    }
                    Ok(Some(Python::with_gil(|py| ($($field),*).to_object(py))))
                } else {
                    Ok(None)
                }
            }
        }

        impl $iter_name {
            pub(crate) fn new(inner: RdictIter, backwards: bool, from_key: &PyAny) -> PyResult<Self> {
                let mut inner = inner;
                if from_key.is_none() {
                    if backwards {
                        inner.seek_to_last();
                    } else {
                        inner.seek_to_first();
                    }
                } else if backwards {
                    inner.seek_for_prev(from_key)?;
                } else {
                    inner.seek(from_key)?;
                }
                Ok(Self {
                    inner,
                    backwards,
                })
            }
        }
    };
}

impl_iter!(RdictKeys, key);
impl_iter!(RdictValues, value);
impl_iter!(RdictItems, key, value);
