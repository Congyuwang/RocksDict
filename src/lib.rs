use num_cpus;
use pyo3::exceptions::PyException;
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyFloat, PyInt, PyString};
use rocksdb::{Options, PlainTableFactoryOptions, SliceTransform, WriteOptions, DB};
use std::fs::{create_dir_all, remove_dir_all};
use std::ops::Deref;
use std::path::Path;
use pyo3::{PyTypeInfo, PyTryFrom};

#[pyclass]
struct Rdict {
    db: DB,
    write_opt: WriteOptions,
}

impl Deref for Rdict {
    type Target = DB;

    fn deref(&self) -> &Self::Target {
        &self.db
    }
}

///
/// Note that we do not support __len__()
///
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
                    write_opt,
                }),
                Err(e) => Err(PyException::new_err(e.to_string())),
            },
            Err(e) => Err(PyException::new_err(e.to_string())),
        }
    }

    fn __getitem__(&self, key: &PyAny, py: Python) -> PyResult<PyObject> {
        let key = convert_key(key, py)?;
        match self.get_pinned(key.as_bytes()) {
            Ok(value) => match value {
                None => Err(PyException::new_err("key not found")),
                Some(slice) => decode_value(py, slice.as_ref()),
            },
            Err(e) => Err(PyException::new_err(e.to_string())),
        }
    }

    fn __setitem__(&self, key: &PyAny, value: &PyAny, py: Python) -> PyResult<()> {
        let key = convert_key(key, py)?;
        match encode_value(value) {
            Ok(value) => {
                match self.put_opt(key.as_bytes(), value, &self.write_opt) {
                    Ok(_) => Ok(()),
                    Err(e) => Err(PyException::new_err(e.to_string())),
                }
            }
            Err(e) => Err(PyException::new_err(e.to_string()))
        }
    }

    fn __contains__(&self, key: &PyAny, py: Python) -> PyResult<bool> {
        let key = convert_key(key, py)?;
        match self.get_pinned(key.as_bytes()) {
            Ok(value) => match value {
                None => Ok(false),
                Some(_) => Ok(true),
            },
            Err(e) => Err(PyException::new_err(e.to_string())),
        }
    }

    fn __delitem__(&self, key: &PyAny, py: Python) -> PyResult<()> {
        let key = convert_key(key, py)?;
        match self.delete_opt(key.as_bytes(), &self.write_opt) {
            Ok(_) => Ok(()),
            Err(e) => Err(PyException::new_err(e.to_string())),
        }
    }

    fn close(&self) -> PyResult<()> {
        match self.db.flush() {
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

enum ValueTypes<'a> {
    Bytes(&'a[u8]),
    String(String),
    Int(i64),
    Float(f64),
    Unsupported,
}

fn encoding_byte(v_type: &ValueTypes) -> u8 {
    match v_type {
        ValueTypes::Bytes(_) => 1,
        ValueTypes::String(_) => 2,
        ValueTypes::Int(_) => 3,
        ValueTypes::Float(_) => 4,
        ValueTypes::Unsupported => 0,
    }
}

///
/// Convert string, int, float, bytes to byte encodings.
///
/// The first byte is used for encoding value types
///
fn encode_value(value: &PyAny) -> Result<Box<[u8]>, &str> {
    let bytes = if PyBytes::is_type_of(value) {
        let bytes: &PyBytes = PyTryFrom::try_from(value).unwrap();
        ValueTypes::Bytes(bytes.as_bytes())
    } else if PyString::is_type_of(value) {
        let value: &PyString = PyTryFrom::try_from(value).unwrap();
        ValueTypes::String(value.to_string())
    } else if PyInt::is_type_of(value) {
        let value: &PyInt = PyTryFrom::try_from(value).unwrap();
        let value: i64 = value.extract().unwrap();
        ValueTypes::Int(value)
    } else if PyFloat::is_type_of(value) {
        let value: &PyFloat = PyTryFrom::try_from(value).unwrap();
        let value: f64 = value.extract().unwrap();
        ValueTypes::Float(value)
    } else {
        ValueTypes::Unsupported
    };
    let type_encoding = encoding_byte(&bytes);
    match bytes {
        ValueTypes::Bytes(value) => {
            Ok(concat_type_encoding(type_encoding, value))
        }
        ValueTypes::String(value) => {
            Ok(concat_type_encoding(type_encoding, value.as_bytes()))
        }
        ValueTypes::Int(value) => {
            Ok(concat_type_encoding(type_encoding, &value.to_be_bytes()[..]))
        }
        ValueTypes::Float(value) => {
            Ok(concat_type_encoding(type_encoding, &value.to_be_bytes()[..]))
        }
        ValueTypes::Unsupported => {
            Err("Only support `string`, `int`, `float`, and `bytes` as values")
        }
    }
}

fn decode_value(py: Python, bytes: &[u8]) -> PyResult<PyObject> {
    match bytes.get(0) {
        None => Err(PyException::new_err("Unknown value type")),
        Some(byte) => {
            match byte {
                1 => Ok(PyBytes::new(py, &bytes[1..]).to_object(py)),
                2 => {
                    let string = match String::from_utf8(bytes[1..].to_vec()) {
                        Ok(s) => s,
                        Err(_) => {
                            return Err(PyException::new_err("utf-8 decoding error"))
                        }
                    };
                    Ok(string.into_py(py))
                },
                3 => {
                    let int: i64 = i64::from_be_bytes(bytes[1..].try_into().unwrap());
                    Ok(int.into_py(py))
                },
                4 => {
                    let int: f64 = f64::from_be_bytes(bytes[1..].try_into().unwrap());
                    Ok(int.into_py(py))
                },
                _ => Err(PyException::new_err("Unknown value type")),
            }
        }
    }
}

fn concat_type_encoding(encoding: u8, payload: &[u8]) -> Box<[u8]> {
    let mut output = Vec::with_capacity(payload.len() + 1);
    output.push(encoding);
    output.extend_from_slice(payload);
    output.into_boxed_slice()
}

///
/// Convert string, int, bytes as values and keys.
///
fn convert_key<'a>(key: &'a PyAny, py: Python<'a>) -> PyResult<&'a PyBytes> {
    if PyString::is_type_of(key) {
        let key: &PyString = PyTryFrom::try_from(key).unwrap();
        let key: String = key.to_string();
        Ok(PyBytes::new(py, key.as_bytes()))
    } else if PyBytes::is_type_of(key) {
        let bytes: &PyBytes = PyTryFrom::try_from(key).unwrap();
        Ok(bytes)
    } else if PyInt::is_type_of(key) {
        let key: &PyInt = PyTryFrom::try_from(key).unwrap();
        let key: i64 = key.extract().unwrap();
        Ok(PyBytes::new(py, &key.to_be_bytes()[..]))
    } else if PyFloat::is_type_of(key) {
        let key: &PyFloat = PyTryFrom::try_from(key).unwrap();
        let key: f64 = key.extract().unwrap();
        Ok(PyBytes::new(py, &key.to_be_bytes()[..]))
    } else {
        Err(PyException::new_err("Only support `string`, `int`, `float`, and `bytes` as keys"))
    }
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
