use crate::db_reference::DbReferenceHolder;
use crate::encoder::{decode_value, encode_key};
use crate::exceptions::DbClosedError;
use crate::util::{error_message, SendMutPtr};
use crate::{ReadOpt, ReadOptionsPy};
use core::slice;
use libc::{c_char, c_uchar, size_t};
use pyo3::exceptions::{PyException, PyRuntimeError};
use pyo3::prelude::*;
use rocksdb::{AsColumnFamilyRef, UnboundColumnFamily};
use std::ops::Deref;
use std::ptr::null_mut;
use std::sync::{Arc, Mutex, MutexGuard};

#[pyclass]
#[allow(dead_code)]
pub(crate) struct RdictIter {
    /// iterator must keep a reference count of DB to keep DB alive.
    pub(crate) db: DbReferenceHolder,

    // This is wrapped in a lock, since this iterator can theoretically be shared between Python
    // threads.
    pub(crate) inner: Mutex<SendMutPtr<librocksdb_sys::rocksdb_iterator_t>>,

    /// When iterate_upper_bound is set, the inner C iterator keeps a pointer to the upper bound
    /// inside `_readopts`. Storing this makes sure the upper bound is always alive when the
    /// iterator is being used.
    pub(crate) readopts: ReadOpt,

    /// use pickle loads to convert bytes to pyobjects
    pub(crate) pickle_loads: PyObject,

    pub(crate) raw_mode: bool,
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
    pub(crate) fn new(
        db: &DbReferenceHolder,
        cf: &Option<Arc<UnboundColumnFamily>>,
        readopts: ReadOptionsPy,
        pickle_loads: &PyObject,
        raw_mode: bool,
        py: Python,
    ) -> PyResult<Self> {
        let readopts = readopts.to_read_opt(raw_mode, py)?;

        let db_inner = db
            .get()
            .ok_or_else(|| DbClosedError::new_err("DB instance already closed"))?
            .inner();

        let inner = unsafe {
            match cf {
                None => SendMutPtr::new(librocksdb_sys::rocksdb_create_iterator(
                    db_inner, readopts.0,
                )),
                Some(cf) => SendMutPtr::new(librocksdb_sys::rocksdb_create_iterator_cf(
                    db_inner,
                    readopts.0,
                    cf.inner(),
                )),
            }
        };

        Ok(RdictIter {
            db: db.clone(),
            inner: Mutex::new(inner),
            readopts,
            pickle_loads: pickle_loads.clone(),
            raw_mode,
        })
    }

    fn is_valid_locked(
        &self,
        inner_locked: &MutexGuard<'_, SendMutPtr<librocksdb_sys::rocksdb_iterator_t>>,
    ) -> bool {
        unsafe { librocksdb_sys::rocksdb_iter_valid(inner_locked.deref().get()) != 0 }
    }

    fn prev_locked(
        &self,
        inner_locked: &MutexGuard<'_, SendMutPtr<librocksdb_sys::rocksdb_iterator_t>>,
    ) {
        unsafe {
            librocksdb_sys::rocksdb_iter_prev(inner_locked.deref().get());
        }
    }

    fn next_locked(
        &self,
        inner_locked: &MutexGuard<'_, SendMutPtr<librocksdb_sys::rocksdb_iterator_t>>,
    ) {
        unsafe {
            librocksdb_sys::rocksdb_iter_next(inner_locked.deref().get());
        }
    }

    fn get_inner_locked(
        &self,
    ) -> PyResult<MutexGuard<'_, SendMutPtr<librocksdb_sys::rocksdb_iterator_t>>> {
        self.inner
            .lock()
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))
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
    #[inline]
    pub fn valid(&self) -> PyResult<bool> {
        let inner_locked = self.get_inner_locked()?;
        Ok(self.is_valid_locked(&inner_locked))
    }

    /// Returns an error `Result` if the iterator has encountered an error
    /// during operation. When an error is encountered, the iterator is
    /// invalidated and [`valid`](DBRawIteratorWithThreadMode::valid) will return `false` when called.
    ///
    /// Performing a seek will discard the current status.
    pub fn status(&self) -> PyResult<()> {
        let mut err: *mut c_char = null_mut();
        let inner_locked = self.get_inner_locked()?;
        unsafe {
            librocksdb_sys::rocksdb_iter_get_error(inner_locked.deref().get(), &mut err);
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
    pub fn seek_to_first(&mut self) -> PyResult<()> {
        let inner_locked = self.get_inner_locked()?;
        unsafe {
            librocksdb_sys::rocksdb_iter_seek_to_first(inner_locked.deref().get());
        }

        Ok(())
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
    pub fn seek_to_last(&mut self) -> PyResult<()> {
        let inner_locked = self.get_inner_locked()?;
        unsafe {
            librocksdb_sys::rocksdb_iter_seek_to_last(inner_locked.deref().get());
        }

        Ok(())
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
    pub fn seek(&mut self, key: &PyAny) -> PyResult<()> {
        let key = encode_key(key, self.raw_mode)?;

        let inner_locked = self.get_inner_locked()?;
        unsafe {
            librocksdb_sys::rocksdb_iter_seek(
                inner_locked.deref().get(),
                key.as_ptr() as *const c_char,
                key.len() as size_t,
            );
        }
        Ok(())
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
    pub fn seek_for_prev(&mut self, key: &PyAny) -> PyResult<()> {
        let key = encode_key(key, self.raw_mode)?;
        let inner_locked = self.get_inner_locked()?;
        unsafe {
            librocksdb_sys::rocksdb_iter_seek_for_prev(
                inner_locked.deref().get(),
                key.as_ptr() as *const c_char,
                key.len() as size_t,
            );
        }
        Ok(())
    }

    /// Seeks to the next key.
    pub fn next(&mut self) -> PyResult<()> {
        let inner_locked = self.get_inner_locked()?;
        self.next_locked(&inner_locked);
        Ok(())
    }

    /// Seeks to the previous key.
    pub fn prev(&mut self) -> PyResult<()> {
        let inner_locked = self.get_inner_locked()?;
        self.prev_locked(&inner_locked);
        Ok(())
    }

    /// Returns the current key.
    pub fn key(&self, py: Python) -> PyResult<PyObject> {
        let inner_locked = self.get_inner_locked()?;
        if self.is_valid_locked(&inner_locked) {
            let inner_locked = self.get_inner_locked()?;

            // Safety Note: This is safe as all methods that may invalidate the buffer returned
            // take `&mut self`, so borrow checker will prevent use of buffer after seek.
            unsafe {
                let mut key_len: size_t = 0;
                let key_len_ptr: *mut size_t = &mut key_len;
                let key_ptr =
                    librocksdb_sys::rocksdb_iter_key(inner_locked.deref().get(), key_len_ptr)
                        as *const c_uchar;
                let key = slice::from_raw_parts(key_ptr, key_len);
                Ok(decode_value(py, key, &self.pickle_loads, self.raw_mode)?)
            }
        } else {
            Ok(py.None())
        }
    }

    /// Returns the current value.
    pub fn value(&self, py: Python) -> PyResult<PyObject> {
        let inner_locked = self.get_inner_locked()?;
        if self.is_valid_locked(&inner_locked) {
            // Safety Note: This is safe as all methods that may invalidate the buffer returned
            // take `&mut self`, so borrow checker will prevent use of buffer after seek.
            unsafe {
                let mut val_len: size_t = 0;
                let val_len_ptr: *mut size_t = &mut val_len;
                let val_ptr =
                    librocksdb_sys::rocksdb_iter_value(inner_locked.deref().get(), val_len_ptr)
                        as *const c_uchar;
                let value = slice::from_raw_parts(val_ptr, val_len);
                Ok(decode_value(py, value, &self.pickle_loads, self.raw_mode)?)
            }
        } else {
            Ok(py.None())
        }
    }

    /// Returns a chunk of keys from the iterator.
    ///
    /// This is more efficient than calling the iterator per element and will drop the GIL while
    /// fetching the chunk.
    ///
    /// Args:
    ///     chunk_size: the number of items to return. If `None`, items will be returned until the
    ///         iterator is exhausted.
    ///    backwards: if `True`, iterator will traverse backwards.
    #[pyo3(signature = (chunk_size = None, backwards = false))]
    pub fn get_chunk_keys(
        &mut self,
        chunk_size: Option<usize>,
        backwards: bool,
        py: Python,
    ) -> PyResult<Vec<PyObject>> {
        let raw_keys = py.allow_threads(|| -> PyResult<Vec<Box<[u8]>>> {
            let mut raw_keys = Vec::new();
            let inner_locked = self.get_inner_locked()?;

            while self.is_valid_locked(&inner_locked)
                && raw_keys.len() < chunk_size.unwrap_or(usize::MAX)
            {
                // Safety: This is safe for multiple reasons:
                //   * It makes a copy of the buffer before returning.
                //   * This `allow_threads` block does not outlive the iterator's lifetime.
                let key = unsafe {
                    let mut key_len: size_t = 0;
                    let key_len_ptr: *mut size_t = &mut key_len;
                    let key_ptr =
                        librocksdb_sys::rocksdb_iter_key(inner_locked.deref().get(), key_len_ptr)
                            as *const c_uchar;
                    slice::from_raw_parts(key_ptr, key_len)
                        .to_vec()
                        .into_boxed_slice()
                };
                raw_keys.push(key);

                if backwards {
                    self.prev_locked(&inner_locked);
                } else {
                    self.next_locked(&inner_locked);
                }
            }

            Ok(raw_keys)
        })?;

        raw_keys
            .into_iter()
            .map(|key| decode_value(py, &key, &self.pickle_loads, self.raw_mode))
            .collect()
    }

    /// Returns a chunk of values from the iterator.
    ///
    /// This is more efficient than calling the iterator per element and will drop the GIL while
    /// fetching the chunk.
    ///
    /// Args:
    ///     chunk_size: the number of items to return. If `None`, items will be returned until the
    ///         iterator is exhausted.
    ///    backwards: if `True`, iterator will traverse backwards.
    #[pyo3(signature = (chunk_size = None, backwards = false))]
    pub fn get_chunk_values(
        &mut self,
        chunk_size: Option<usize>,
        backwards: bool,
        py: Python,
    ) -> PyResult<Vec<PyObject>> {
        let raw_values = py.allow_threads(|| -> PyResult<Vec<Box<[u8]>>> {
            let mut raw_values = Vec::new();
            let inner_locked = self.get_inner_locked()?;
            while self.is_valid_locked(&inner_locked)
                && raw_values.len() < chunk_size.unwrap_or(usize::MAX)
            {
                // Safety: This is safe for multiple reasons:
                //   * It makes a copy of the buffer before returning.
                //   * This `allow_threads` block does not outlive the iterator's lifetime.
                let value = unsafe {
                    let mut value_len: size_t = 0;
                    let value_len_ptr: *mut size_t = &mut value_len;
                    let value_ptr = librocksdb_sys::rocksdb_iter_value(
                        inner_locked.deref().get(),
                        value_len_ptr,
                    ) as *const c_uchar;
                    slice::from_raw_parts(value_ptr, value_len)
                        .to_vec()
                        .into_boxed_slice()
                };
                raw_values.push(value);

                if backwards {
                    self.prev_locked(&inner_locked)
                } else {
                    self.next_locked(&inner_locked);
                }
            }

            Ok(raw_values)
        })?;

        raw_values
            .into_iter()
            .map(|value| decode_value(py, &value, &self.pickle_loads, self.raw_mode))
            .collect()
    }

    /// Returns a chunk of key-value pairs from the iterator.
    ///
    /// This is more efficient than calling the iterator per element and will drop the GIL while
    /// fetching the chunk.
    ///
    /// Args:
    ///     chunk_size: the number of items to return. If `None`, items will be returned until the
    ///         iterator is exhausted.
    ///    backwards: if `True`, iterator will traverse backwards.
    #[pyo3(signature = (chunk_size = None, backwards = false))]
    pub fn get_chunk_items(
        &mut self,
        chunk_size: Option<usize>,
        backwards: bool,
        py: Python,
    ) -> PyResult<Vec<(PyObject, PyObject)>> {
        let raw_items = py.allow_threads(|| -> PyResult<Vec<(Box<[u8]>, Box<[u8]>)>> {
            let mut raw_items = Vec::new();
            let inner_locked = self.get_inner_locked()?;
            while self.is_valid_locked(&inner_locked)
                && raw_items.len() < chunk_size.unwrap_or(usize::MAX)
            {
                // Safety: This is safe for multiple reasons:
                //   * It makes a copy of the buffer before returning.
                //   * This `allow_threads` block does not outlive the iterator's lifetime.
                let key = unsafe {
                    let mut key_len: size_t = 0;
                    let key_len_ptr: *mut size_t = &mut key_len;
                    let key_ptr =
                        librocksdb_sys::rocksdb_iter_key(inner_locked.deref().get(), key_len_ptr)
                            as *const c_uchar;
                    slice::from_raw_parts(key_ptr, key_len)
                        .to_vec()
                        .into_boxed_slice()
                };

                // Safety: This is safe for multiple reasons:
                //   * It makes a copy of the buffer before returning.
                //   * This `allow_threads` block does not outlive the iterator's lifetime.
                let value = unsafe {
                    let mut value_len: size_t = 0;
                    let value_len_ptr: *mut size_t = &mut value_len;
                    let value_ptr = librocksdb_sys::rocksdb_iter_value(
                        inner_locked.deref().get(),
                        value_len_ptr,
                    ) as *const c_uchar;
                    slice::from_raw_parts(value_ptr, value_len)
                        .to_vec()
                        .into_boxed_slice()
                };

                raw_items.push((key, value));

                if backwards {
                    self.prev_locked(&inner_locked);
                } else {
                    self.next_locked(&inner_locked);
                }
            }

            Ok(raw_items)
        })?;

        raw_items
            .into_iter()
            .map(|(key, value)| {
                let key = decode_value(py, &key, &self.pickle_loads, self.raw_mode)?;
                let value = decode_value(py, &value, &self.pickle_loads, self.raw_mode)?;
                Ok((key, value))
            })
            .collect()
    }
}

impl Drop for RdictIter {
    fn drop(&mut self) {
        if let Ok(inner_locked) = self.get_inner_locked() {
            unsafe {
                librocksdb_sys::rocksdb_iter_destroy(inner_locked.deref().get());
            }
        }
    }
}

unsafe impl Send for RdictIter {}

macro_rules! impl_iter {
    ($iter_name: ident, $($field: ident),*) => {
        #[pymethods]
        impl $iter_name {
            fn __iter__(slf: PyRef<Self>) -> PyRef<Self> {
                slf
            }

            fn __next__(mut slf: PyRefMut<Self>, py: Python) -> PyResult<Option<PyObject>> {
                if slf.inner.valid()? {
                    $(let $field = slf.inner.$field(py)?;)*
                    if slf.backwards {
                        slf.inner.prev()?;
                    } else {
                        slf.inner.next()?;
                    }
                    Ok(Some(($($field),*).to_object(py)))
                } else {
                    Ok(None)
                }
            }
        }

        impl $iter_name {
            pub(crate) fn new(inner: RdictIter, backwards: bool, from_key: Option<&PyAny>) -> PyResult<Self> {
                let mut inner = inner;
                if let Some(from_key) = from_key {
                    if backwards {
                        inner.seek_for_prev(from_key)?;
                    } else {
                        inner.seek(from_key)?;
                    }
                } else {
                    if backwards {
                        inner.seek_to_last()?;
                    } else {
                        inner.seek_to_first()?;
                    }
                }
                Ok(Self {
                    inner,
                    backwards,
                })
            }
        }
    };
}

macro_rules! impl_chunked_iter {
    ($iter_name: ident, $iter_chunk_fn: ident) => {
        #[pyclass]
        pub(crate) struct $iter_name {
            inner: RdictIter,
            backwards: bool,
            chunk_size: Option<usize>,
        }

        #[pymethods]
        impl $iter_name {
            fn __iter__(slf: PyRef<Self>) -> PyRef<Self> {
                slf
            }

            fn __next__(&mut self, py: Python) -> PyResult<Option<PyObject>> {
                if self.inner.valid()? {
                    Ok(Some(
                        self.inner
                            .$iter_chunk_fn(self.chunk_size, self.backwards, py)
                            .map(|v| v.to_object(py))?,
                    ))
                } else {
                    Ok(None)
                }
            }
        }

        impl $iter_name {
            pub(crate) fn new(
                inner: RdictIter,
                chunk_size: Option<usize>,
                backwards: bool,
                from_key: Option<&PyAny>,
            ) -> PyResult<Self> {
                let mut inner = inner;
                if let Some(from_key) = from_key {
                    if backwards {
                        inner.seek_for_prev(from_key)?;
                    } else {
                        inner.seek(from_key)?;
                    }
                } else {
                    if backwards {
                        inner.seek_to_last()?;
                    } else {
                        inner.seek_to_first()?;
                    }
                }
                Ok(Self {
                    inner,
                    backwards,
                    chunk_size,
                })
            }
        }
    };
}

impl_iter!(RdictKeys, key);
impl_iter!(RdictValues, value);
impl_iter!(RdictItems, key, value);

impl_chunked_iter!(RdictChunkedKeys, get_chunk_keys);
impl_chunked_iter!(RdictChunkedValues, get_chunk_values);
impl_chunked_iter!(RdictChunkedItems, get_chunk_items);
