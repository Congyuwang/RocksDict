use crate::encoder::{decode_value, encode_key, encode_raw};
use crate::{Rdict, RdictItems, RdictIter, RdictKeys, RdictValues, ReadOpt, ReadOptionsPy};
use pyo3::exceptions::PyException;
use pyo3::prelude::*;
use rocksdb::{ColumnFamily, ReadOptions, DB};
use std::cell::RefCell;
use std::ops::Deref;
use std::sync::Arc;

/// A consistent view of the database at the point of creation.
///
/// Examples:
///     ::
///
///         from rocksdict import Rdict
///
///         db = Rdict("tmp")
///         for i in range(100):
///             db[i] = i
///
///         # take a snapshot
///         snapshot = db.snapshot()
///
///         for i in range(90):
///             del db[i]
///
///         # 0-89 are no longer in db
///         for k, v in db.items():
///             print(f"{k} -> {v}")
///
///         # but they are still in the snapshot
///         for i in range(100):
///             assert snapshot[i] == i
///
///         # drop the snapshot
///         del snapshot, db
///
///         Rdict.destroy("tmp")
#[pyclass]
pub struct Snapshot {
    pub(crate) inner: *const librocksdb_sys::rocksdb_snapshot_t,
    pub(crate) column_family: Option<Arc<ColumnFamily>>,
    pub(crate) pickle_loads: PyObject,
    pub(crate) read_opt: ReadOptions,
    // decrease db Rc last
    pub(crate) db: Arc<RefCell<DB>>,
    pub(crate) raw_mode: bool,
}

#[pymethods]
impl Snapshot {
    /// Creates an iterator over the data in this snapshot under the given column family, using
    /// the default read options.
    ///
    /// Args:
    ///     read_opt: ReadOptions, must have the same `raw_mode` argument.
    #[pyo3(text_signature = "($self, read_opt)")]
    #[args(read_opt = "None")]
    fn iter(&self, read_opt: Option<&ReadOptionsPy>, py: Python) -> PyResult<RdictIter> {
        let read_opt: ReadOptionsPy = match read_opt {
            None => ReadOptionsPy::default(self.raw_mode, py)?,
            Some(opt) => opt.clone(),
        };
        let opt_pointer = ReadOpt::from(&read_opt);
        unsafe {
            set_snapshot(opt_pointer.0, self.inner);
        }
        Ok(RdictIter::new(
            &self.db,
            &self.column_family,
            read_opt,
            &self.pickle_loads,
            self.raw_mode,
        )?)
    }

    /// Iterate through all keys and values pairs.
    ///
    /// Args:
    ///     backwards: iteration direction, forward if `False`.
    ///     from_key: iterate from key, first seek to this key
    ///         or the nearest next key for iteration
    ///         (depending on iteration direction).
    ///     read_opt: ReadOptions, must have the same `raw_mode` argument.
    #[pyo3(text_signature = "($self, backwards, from_key, read_opt)")]
    #[args(backwards = "false", from_key = "None", read_opt = "None")]
    fn items(
        &self,
        backwards: bool,
        from_key: Option<&PyAny>,
        read_opt: Option<&ReadOptionsPy>,
        py: Python,
    ) -> PyResult<RdictItems> {
        Ok(RdictItems::new(
            self.iter(read_opt, py)?,
            backwards,
            from_key,
        )?)
    }

    /// Iterate through all keys.
    ///
    /// Args:
    ///     backwards: iteration direction, forward if `False`.
    ///     from_key: iterate from key, first seek to this key
    ///         or the nearest next key for iteration
    ///         (depending on iteration direction).
    ///     read_opt: ReadOptions, must have the same `raw_mode` argument.
    #[pyo3(text_signature = "($self, backwards, from_key, read_opt)")]
    #[args(backwards = "false", from_key = "None", read_opt = "None")]
    fn keys(
        &self,
        backwards: bool,
        from_key: Option<&PyAny>,
        read_opt: Option<&ReadOptionsPy>,
        py: Python,
    ) -> PyResult<RdictKeys> {
        Ok(RdictKeys::new(
            self.iter(read_opt, py)?,
            backwards,
            from_key,
        )?)
    }

    /// Iterate through all values.
    ///
    /// Args:
    ///     backwards: iteration direction, forward if `False`.
    ///     from_key: iterate from key, first seek to this key
    ///         or the nearest next key for iteration
    ///         (depending on iteration direction).
    ///     read_opt: ReadOptions, must have the same `raw_mode` argument.
    #[pyo3(text_signature = "($self, backwards, from_key, read_opt)")]
    #[args(backwards = "false", from_key = "None", read_opt = "None")]
    fn values(
        &self,
        backwards: bool,
        from_key: Option<&PyAny>,
        read_opt: Option<&ReadOptionsPy>,
        py: Python,
    ) -> PyResult<RdictValues> {
        Ok(RdictValues::new(
            self.iter(read_opt, py)?,
            backwards,
            from_key,
        )?)
    }

    /// read from snapshot
    fn __getitem__(&self, key: &PyAny, py: Python) -> PyResult<PyObject> {
        let db = self.db.borrow();
        let value_result = if self.raw_mode {
            let key = encode_raw(key)?;
            if let Some(cf) = &self.column_family {
                db.get_pinned_cf_opt(cf.deref(), &key[..], &self.read_opt)
            } else {
                db.get_pinned_opt(&key[..], &self.read_opt)
            }
        } else {
            let key = encode_key(key, self.raw_mode)?;
            if let Some(cf) = &self.column_family {
                db.get_pinned_cf_opt(cf.deref(), &key[..], &self.read_opt)
            } else {
                db.get_pinned_opt(&key[..], &self.read_opt)
            }
        };
        match value_result {
            Ok(value) => match value {
                None => Err(PyException::new_err("key not found")),
                Some(slice) => decode_value(py, slice.as_ref(), &self.pickle_loads, self.raw_mode),
            },
            Err(e) => Err(PyException::new_err(e.to_string())),
        }
    }
}

impl Snapshot {
    pub(crate) fn new(rdict: &Rdict) -> PyResult<Self> {
        if let Some(db) = &rdict.db {
            let db_borrow = db.borrow();
            let snapshot = unsafe { librocksdb_sys::rocksdb_create_snapshot(db_borrow.inner()) };
            let r_opt: ReadOptions = (&rdict.read_opt_py).into();
            unsafe {
                set_snapshot(r_opt.inner(), snapshot);
            }
            Ok(Snapshot {
                inner: snapshot,
                column_family: rdict.column_family.clone(),
                pickle_loads: rdict.pickle_loads.clone(),
                read_opt: r_opt,
                db: db.clone(),
                raw_mode: rdict.opt_py.raw_mode,
            })
        } else {
            Err(PyException::new_err("DB already closed"))
        }
    }
}

impl Drop for Snapshot {
    fn drop(&mut self) {
        unsafe {
            librocksdb_sys::rocksdb_release_snapshot(self.db.borrow().inner(), self.inner);
        }
    }
}

/// `Send` and `Sync` implementations for `SnapshotWithThreadMode` are safe, because `SnapshotWithThreadMode` is
/// immutable and can be safely shared between threads.
unsafe impl Send for Snapshot {}
unsafe impl Sync for Snapshot {}

#[inline]
pub(crate) unsafe fn set_snapshot(
    read_opt: *mut librocksdb_sys::rocksdb_readoptions_t,
    snapshot_inner: *const librocksdb_sys::rocksdb_snapshot_t,
) {
    librocksdb_sys::rocksdb_readoptions_set_snapshot(read_opt, snapshot_inner);
}
