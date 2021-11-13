use num_cpus;
use pyo3::exceptions::PyException;
use pyo3::prelude::*;
use pyo3::types::PyBytes;
use rocksdb::{Options, PlainTableFactoryOptions, SliceTransform, WriteOptions, DB};
use std::fs::{create_dir_all, remove_dir_all};
use std::ops::Deref;
use std::path::Path;

#[pyclass]
struct Rdict {
    db: DB,
    len: usize,
    write_opt: WriteOptions,
}

impl Deref for Rdict {
    type Target = DB;

    fn deref(&self) -> &Self::Target {
        &self.db
    }
}

#[pymethods]
impl Rdict {
    #[new]
    fn new(path: &str) -> PyResult<Self> {
        let path = Path::new(path);
        let mut write_opt = WriteOptions::default();
        write_opt.disable_wal(true);
        match create_dir_all(path) {
            Ok(_) => match DB::open(&default_options(), &path) {
                Ok(db) => Ok(Rdict {
                    db,
                    len: 0,
                    write_opt,
                }),
                Err(e) => Err(PyException::new_err(e.to_string())),
            },
            Err(e) => Err(PyException::new_err(e.to_string())),
        }
    }

    fn __len__(&self) -> PyResult<usize> {
        Ok(self.len)
    }

    fn __getitem__<'a>(&self, key: &PyBytes, py: Python<'a>) -> PyResult<&'a PyBytes> {
        match self.get_pinned(key.as_bytes()) {
            Ok(value) => match value {
                None => Err(PyException::new_err("key not found")),
                Some(slice) => Ok(PyBytes::new(py, slice.as_ref())),
            },
            Err(e) => Err(PyException::new_err(e.to_string())),
        }
    }

    fn __setitem__(&self, key: &PyBytes, value: &PyBytes) -> PyResult<()> {
        match self.put_opt(key.as_bytes(), value.as_bytes(), &self.write_opt) {
            Ok(_) => Ok(()),
            Err(e) => Err(PyException::new_err(e.to_string())),
        }
    }

    fn __contains__(&self, key: &PyBytes) -> PyResult<bool> {
        match self.get_pinned(key.as_bytes()) {
            Ok(value) => match value {
                None => Ok(false),
                Some(_) => Ok(true),
            },
            Err(e) => Err(PyException::new_err(e.to_string())),
        }
    }

    fn __delitem__(&self, key: &PyBytes) -> PyResult<()> {
        match self.delete_opt(key.as_bytes(), &self.write_opt) {
            Ok(_) => Ok(()),
            Err(e) => Err(PyException::new_err(e.to_string())),
        }
    }

    fn destroy(&self) -> PyResult<()> {
        match remove_dir_all(self.path()) {
            Ok(_) => Ok(()),
            Err(e) => Err(PyException::new_err(e.to_string())),
        }
    }
}

#[pymodule]
fn rocksdict(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<Rdict>()?;
    Ok(())
}

fn default_options() -> Options {
    let mut options = Options::default();
    // create table
    options.create_if_missing(true);
    // config to more jobs
    options.set_max_background_jobs(num_cpus::get() as i32);
    // configure mem-table to a large value (256 MB)
    options.set_write_buffer_size(0x10000000);
    // configure l0 and l1 size, let them have the same size (1 GB)
    options.set_level_zero_file_num_compaction_trigger(4);
    options.set_max_bytes_for_level_base(0x40000000);
    // 256MB file size
    options.set_target_file_size_base(0x10000000);
    // use a smaller compaction multiplier
    options.set_max_bytes_for_level_multiplier(4.0);
    // use 8-byte prefix (2 ^ 64 is far enough for transaction counts)
    options.set_prefix_extractor(SliceTransform::create_fixed_prefix(8));
    // set to plain-table for better performance
    options.set_plain_table_factory(&PlainTableFactoryOptions {
        // 16 (compressed txid) + 4 (i32 out n)
        user_key_length: 0,
        bloom_bits_per_key: 10,
        hash_table_ratio: 0.75,
        index_sparseness: 16,
    });
    options
}
