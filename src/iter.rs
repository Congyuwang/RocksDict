use core::slice;
use std::ptr::null_mut;
use std::sync::Arc;
use libc::{c_char, c_uchar, size_t};
use librocksdb_sys;
use pyo3::exceptions::PyException;
use rocksdb::DB;
use pyo3::prelude::*;
use crate::encoder::{decode_value, encode_value};
use crate::ReadOpt;
use crate::util::error_message;


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
    pub fn status(&self) -> PyResult<()> {
        let mut err: *mut c_char = null_mut();
        unsafe { librocksdb_sys::rocksdb_iter_get_error(self.inner, &mut err); }
        if !err.is_null() {
            Err(PyException::new_err(error_message(err)))
        } else {
            Ok(())
        }
    }

    /// Seeks to the first key in the database.
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
    pub fn seek_to_first(&mut self) {
        unsafe {
            librocksdb_sys::rocksdb_iter_seek_to_first(self.inner);
        }
    }

    /// Seeks to the last key in the database.
    ///
    /// # Examples
    ///
    /// ```python
    /// from rocksdict import Rdict, Options, ReadOptions
    ///
    /// path = "_path_for_rocksdb_storage6"
    /// db = Rdict(path, Options())
    /// iter = db.iter(ReadOptions())
    ///
    /// # Iterate all keys from the start in lexicographic order
    /// iter.seek_to_last()
    ///
    /// while iter.valid():
    ///     print(f"{iter.key()} {iter.value()}")
    ///     iter.prev()
    ///
    /// # Read just the last key
    /// iter.seek_to_last();
    /// print(f"{iter.key()} {iter.value()}")
    ///
    /// db.destroy(Options())
    /// ```
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
    /// # Examples
    ///
    /// ```python
    /// from rocksdict import Rdict, Options, ReadOptions
    ///
    /// path = "_path_for_rocksdb_storage6"
    /// db = Rdict(path, Options())
    /// iter = db.iter(ReadOptions())
    ///
    /// # Read the first string key that starts with 'a'
    /// iter.seek("a");
    /// print(f"{iter.key()} {iter.value()}")
    ///
    /// db.destroy(Options())
    /// ```
    pub fn seek(&mut self, key: &PyAny) -> PyResult<()> {
        let key = encode_value(key)?;

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
    /// # Examples
    ///
    /// ```python
    /// from rocksdict import Rdict, Options, ReadOptions
    ///
    /// path = "_path_for_rocksdb_storage6"
    /// db = Rdict(path, Options())
    /// iter = db.iter(ReadOptions())
    ///
    /// # Read the last key that starts with 'a'
    /// seek_for_prev("b")
    /// print(f"{iter.key()} {iter.value()}")
    ///
    /// db.destroy(Options())
    /// ```
    pub fn seek_for_prev(&mut self, key: &PyAny) -> PyResult<()> {
        let key = encode_value(key)?;

        Ok(unsafe {
            librocksdb_sys::rocksdb_iter_seek_for_prev(
                self.inner,
                key.as_ptr() as *const c_char,
                key.len() as size_t,
            );
        })
    }

    /// Seeks to the next key.
    pub fn next(&mut self) {
        unsafe {
            librocksdb_sys::rocksdb_iter_next(self.inner);
        }
    }

    /// Seeks to the previous key.
    pub fn prev(&mut self) {
        unsafe {
            librocksdb_sys::rocksdb_iter_prev(self.inner);
        }
    }

    /// Returns the current key.
    pub fn key(&self, py: Python) -> PyResult<PyObject> {
        if self.valid() {
            // Safety Note: This is safe as all methods that may invalidate the buffer returned
            // take `&mut self`, so borrow checker will prevent use of buffer after seek.
            unsafe {
                let mut key_len: size_t = 0;
                let key_len_ptr: *mut size_t = &mut key_len;
                let key_ptr = librocksdb_sys::rocksdb_iter_key(self.inner, key_len_ptr) as *const c_uchar;
                let key = slice::from_raw_parts(key_ptr, key_len as usize);
                Ok(decode_value(py, key)?)
            }
        } else {
            Ok(py.None())
        }
    }

    /// Returns the current value.
    pub fn value(&self, py: Python) -> PyResult<PyObject> {
        if self.valid() {
            // Safety Note: This is safe as all methods that may invalidate the buffer returned
            // take `&mut self`, so borrow checker will prevent use of buffer after seek.
            unsafe {
                let mut val_len: size_t = 0;
                let val_len_ptr: *mut size_t = &mut val_len;
                let val_ptr = librocksdb_sys::rocksdb_iter_value(self.inner, val_len_ptr) as *const c_uchar;
                let value = slice::from_raw_parts(val_ptr, val_len as usize);
                Ok(decode_value(py, value)?)
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
