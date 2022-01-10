use crate::encoder::{encode_key, encode_raw, encode_value};
use crate::ColumnFamilyPy;
use pyo3::exceptions::PyException;
use pyo3::prelude::*;
use rocksdb::WriteBatch;
use std::ops::Deref;

/// WriteBatch class. Use db.write() to ingest WriteBatch.
///
/// Notes:
///     A WriteBatch instance can only be ingested once,
///     otherwise an Exception will be raised.
///
/// Args:
///     raw_mode (bool): make sure that this is consistent with the Rdict.
#[pyclass(name = "WriteBatch")]
pub(crate) struct WriteBatchPy {
    inner: Option<WriteBatch>,
    default_column_family: Option<ColumnFamilyPy>,
    pickle_dumps: PyObject,
    pub(crate) raw_mode: bool,
}

#[pymethods]
impl WriteBatchPy {
    /// WriteBatch class. Use db.write() to ingest WriteBatch.
    ///
    /// Notes:
    ///     A WriteBatch instance can only be ingested once,
    ///     otherwise an Exception will be raised.
    ///
    /// Args:
    ///     raw_mode (bool): make sure that this is consistent with the Rdict.
    #[new]
    #[args(raw_mode = "false")]
    pub fn default(py: Python, raw_mode: bool) -> PyResult<Self> {
        let pickle = PyModule::import(py, "pickle")?.to_object(py);
        Ok(WriteBatchPy {
            inner: Some(WriteBatch::default()),
            default_column_family: None,
            pickle_dumps: pickle.getattr(py, "dumps")?,
            raw_mode,
        })
    }

    pub fn __len__(&self) -> PyResult<usize> {
        self.len()
    }

    pub fn __setitem__(&mut self, key: &PyAny, value: &PyAny, py: Python) -> PyResult<()> {
        if let Some(inner) = &mut self.inner {
            if self.raw_mode {
                let key = encode_raw(key)?;
                let value = encode_raw(value)?;
                match &self.default_column_family {
                    None => inner.put(key, value),
                    Some(cf) => inner.put_cf(cf.cf.deref(), key, value),
                }
            } else {
                let key = encode_key(key, self.raw_mode)?;
                let value = encode_value(value, &self.pickle_dumps, self.raw_mode, py)?;
                match &self.default_column_family {
                    None => inner.put(key, value),
                    Some(cf) => inner.put_cf(cf.cf.deref(), key, value),
                }
            }
            Ok(())
        } else {
            Err(PyException::new_err(
                "this batch is already consumed, create a new one by calling `WriteBatch()`",
            ))
        }
    }

    pub fn __delitem__(&mut self, key: &PyAny) -> PyResult<()> {
        if let Some(inner) = &mut self.inner {
            if self.raw_mode {
                let key = encode_raw(key)?;
                match &self.default_column_family {
                    None => inner.delete(key),
                    Some(cf) => inner.delete_cf(cf.cf.deref(), key),
                }
            } else {
                let key = encode_key(key, self.raw_mode)?;
                match &self.default_column_family {
                    None => inner.delete(key),
                    Some(cf) => inner.delete_cf(cf.cf.deref(), key),
                }
            }
            Ok(())
        } else {
            Err(PyException::new_err(
                "this batch is already consumed, create a new one by calling `WriteBatch()`",
            ))
        }
    }

    /// Set the default item for `a[i] = j` and `del a[i]` syntax.
    ///
    /// You can also use `put(key, value, column_family)` to explicitly choose column family.
    ///
    /// Args:
    ///     - column_family (ColumnFamily | None): column family descriptor or None (for default family).
    #[pyo3(text_signature = "($self, column_family)")]
    pub fn set_default_column_family(
        &mut self,
        column_family: Option<ColumnFamilyPy>,
    ) -> PyResult<()> {
        if let Some(_) = &self.inner {
            Ok(self.default_column_family = column_family)
        } else {
            Err(PyException::new_err(
                "this batch is already consumed, create a new one by calling `WriteBatch()`",
            ))
        }
    }

    /// length of the batch
    #[pyo3(text_signature = "($self)")]
    pub fn len(&self) -> PyResult<usize> {
        if let Some(inner) = &self.inner {
            Ok(inner.len())
        } else {
            Err(PyException::new_err(
                "this batch is already consumed, create a new one by calling `WriteBatch()`",
            ))
        }
    }

    /// Return WriteBatch serialized size (in bytes).
    #[pyo3(text_signature = "($self)")]
    pub fn size_in_bytes(&self) -> PyResult<usize> {
        if let Some(inner) = &self.inner {
            Ok(inner.size_in_bytes())
        } else {
            Err(PyException::new_err(
                "this batch is already consumed, create a new one by calling `WriteBatch()`",
            ))
        }
    }

    /// Check whether the batch is empty.
    #[pyo3(text_signature = "($self)")]
    pub fn is_empty(&self) -> PyResult<bool> {
        if let Some(inner) = &self.inner {
            Ok(inner.is_empty())
        } else {
            Err(PyException::new_err(
                "this batch is already consumed, create a new one by calling `WriteBatch()`",
            ))
        }
    }

    /// Insert a value into the database under the given key.
    ///
    /// Args:
    ///     column_family: override the default column family set by set_default_column_family
    #[pyo3(text_signature = "($self, key, value, column_family)")]
    #[args(column_family = "None")]
    pub fn put(
        &mut self,
        key: &PyAny,
        value: &PyAny,
        column_family: Option<ColumnFamilyPy>,
        py: Python,
    ) -> PyResult<()> {
        if let Some(inner) = &mut self.inner {
            if self.raw_mode {
                let key = encode_raw(key)?;
                let value = encode_raw(value)?;
                match column_family {
                    Some(cf) => inner.put_cf(cf.cf.deref(), key, value),
                    None => inner.put(key, value),
                }
            } else {
                let key = encode_key(key, self.raw_mode)?;
                let value = encode_value(value, &self.pickle_dumps, self.raw_mode, py)?;
                match column_family {
                    Some(cf) => inner.put_cf(cf.cf.deref(), key, value),
                    None => inner.put(key, value),
                }
            }
            Ok(())
        } else {
            Err(PyException::new_err(
                "this batch is already consumed, create a new one by calling `WriteBatch()`",
            ))
        }
    }

    /// Removes the database entry for key. Does nothing if the key was not found.
    ///
    /// Args:
    ///     column_family: override the default column family set by set_default_column_family
    #[pyo3(text_signature = "($self, key, column_family)")]
    #[args(column_family = "None")]
    pub fn delete(&mut self, key: &PyAny, column_family: Option<ColumnFamilyPy>) -> PyResult<()> {
        if let Some(inner) = &mut self.inner {
            if self.raw_mode {
                let key = encode_raw(key)?;
                match column_family {
                    Some(cf) => inner.delete_cf(cf.cf.deref(), key),
                    None => inner.delete(key),
                }
            } else {
                let key = encode_key(key, self.raw_mode)?;
                match column_family {
                    Some(cf) => inner.delete_cf(cf.cf.deref(), key),
                    None => inner.delete(key),
                }
            }
            Ok(())
        } else {
            Err(PyException::new_err(
                "this batch is already consumed, create a new one by calling `WriteBatch()`",
            ))
        }
    }

    /// Remove database entries in column family from start key to end key.
    ///
    /// Notes:
    ///     Removes the database entries in the range ["begin_key", "end_key"), i.e.,
    ///     including "begin_key" and excluding "end_key". It is not an error if no
    ///     keys exist in the range ["begin_key", "end_key").
    ///
    /// Args:
    ///     begin: begin key
    ///     end: end key
    ///     column_family: override the default column family set by set_default_column_family
    #[pyo3(text_signature = "($self, begin, end)")]
    #[args(column_family = "None")]
    pub fn delete_range(
        &mut self,
        begin: &PyAny,
        end: &PyAny,
        column_family: Option<ColumnFamilyPy>,
    ) -> PyResult<()> {
        if let Some(inner) = &mut self.inner {
            let from = encode_key(begin, self.raw_mode)?;
            let to = encode_key(end, self.raw_mode)?;
            match column_family {
                Some(cf) => inner.delete_range_cf(cf.cf.deref(), from, to),
                None => inner.delete_range(from, to),
            }
            Ok(())
        } else {
            Err(PyException::new_err(
                "this batch is already consumed, create a new one by calling `WriteBatch()`",
            ))
        }
    }

    /// Clear all updates buffered in this batch.
    #[pyo3(text_signature = "($self)")]
    pub fn clear(&mut self) -> PyResult<()> {
        if let Some(inner) = &mut self.inner {
            Ok(inner.clear())
        } else {
            Err(PyException::new_err(
                "this batch is already consumed, create a new one by calling `WriteBatch()`",
            ))
        }
    }
}

impl WriteBatchPy {
    #[inline]
    pub(crate) fn consume(&mut self) -> PyResult<WriteBatch> {
        if let Some(inner) = self.inner.take() {
            drop(self.default_column_family.take());
            Ok(inner)
        } else {
            Err(PyException::new_err(
                "this batch is already consumed, create a new one by calling `WriteBatch()`",
            ))
        }
    }
}
