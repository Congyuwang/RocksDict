use num_cpus;
use pyo3::exceptions::PyException;
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyFloat, PyInt, PyString};
use rocksdb::{Options, PlainTableFactoryOptions, SliceTransform, WriteOptions, DB};
use std::fs::{create_dir_all, remove_dir_all};
use std::ops::{Deref, DerefMut};
use std::path::Path;
use ahash::AHashMap;
use pyo3::{PyTypeInfo, PyTryFrom};
use integer_encoding::VarInt;

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

#[pyclass]
struct Mdict(AHashMap<Box<[u8]>, Box<[u8]>>);

impl Deref for Mdict {
    type Target = AHashMap<Box<[u8]>, Box<[u8]>>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for Mdict {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

#[pymethods]
impl Mdict {
    #[new]
    fn new() -> Self {
        Mdict(AHashMap::new())
    }

    fn __getitem__(&self, key: &PyAny, py: Python) -> PyResult<PyObject> {
        let key = encode_value(key)?;
        match self.get(&key[..]) {
            None => Err(PyException::new_err("key not found")),
            Some(slice) => decode_value(py, &slice[..]),
        }
    }

    fn __setitem__(&mut self, key: &PyAny, value: &PyAny) -> PyResult<()> {
        let key = encode_value(key)?;
        match encode_value(value) {
            Ok(value) => {
                self.insert(key, value);
                Ok(())
            }
            Err(e) => Err(PyException::new_err(e.to_string()))
        }
    }

    fn __contains__(&self, key: &PyAny) -> PyResult<bool> {
        let key = encode_value(key)?;
        Ok(self.contains_key(&key[..]))
    }

    fn __delitem__(&mut self, key: &PyAny) -> PyResult<()> {
        let key = encode_value(key)?;
        self.remove(&key[..]);
        Ok(())
    }

    fn __len__(&self) -> usize {
        self.len()
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
        let key = encode_value(key)?;
        match self.get_pinned(&key[..]) {
            Ok(value) => match value {
                None => Err(PyException::new_err("key not found")),
                Some(slice) => decode_value(py, slice.as_ref()),
            },
            Err(e) => Err(PyException::new_err(e.to_string())),
        }
    }

    fn __setitem__(&self, key: &PyAny, value: &PyAny) -> PyResult<()> {
        let key = encode_value(key)?;
        let value = encode_value(value)?;
        match self.put_opt(&key[..], value, &self.write_opt) {
            Ok(_) => Ok(()),
            Err(e) => Err(PyException::new_err(e.to_string())),
        }
    }

    fn __contains__(&self, key: &PyAny) -> PyResult<bool> {
        let key = encode_value(key)?;
        match self.get_pinned(&key[..]) {
            Ok(value) => match value {
                None => Ok(false),
                Some(_) => Ok(true),
            },
            Err(e) => Err(PyException::new_err(e.to_string())),
        }
    }

    fn __delitem__(&self, key: &PyAny) -> PyResult<()> {
        let key = encode_value(key)?;
        match self.delete_opt(&key[..], &self.write_opt) {
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
    m.add_class::<Mdict>()?;
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
fn encode_value(value: &PyAny) -> PyResult<Box<[u8]>> {
    let bytes = if PyBytes::is_type_of(value) {
        let bytes: &PyBytes = unsafe { PyTryFrom::try_from_unchecked(value) };
        let bytes = bytes.as_bytes();
        ValueTypes::Bytes(bytes)
    } else if PyString::is_type_of(value) {
        let value: &PyString = unsafe{ PyTryFrom::try_from_unchecked(value) };
        let string = value.to_string();
        ValueTypes::String(string)
    } else if PyInt::is_type_of(value) {
        let value: &PyInt = unsafe{ PyTryFrom::try_from_unchecked(value) };
        let value: i64 = value.extract().unwrap();
        ValueTypes::Int(value)
    } else if PyFloat::is_type_of(value) {
        let value: &PyFloat = unsafe{ PyTryFrom::try_from_unchecked(value) };
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
            Ok(concat_type_encoding(type_encoding, &value.encode_var_vec()[..]))
        }
        ValueTypes::Float(value) => {
            Ok(concat_type_encoding(type_encoding, &value.to_be_bytes()[..]))
        }
        ValueTypes::Unsupported => {
            Err(PyException::new_err("Only support `string`, `int`, `float`, and `bytes` as keys / values"))
        }
    }
}

fn decode_value(py: Python, bytes: &[u8]) -> PyResult<PyObject> {
    match bytes.get(0) {
        None => Err(PyException::new_err("Unknown value type")),
        Some(byte) => {
            match byte {
                1 => {
                    Ok(PyBytes::new(py, &bytes[1..]).to_object(py))
                },
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
                    if let Some((int, _)) = i64::decode_var(bytes[1..].try_into().unwrap()) {
                        Ok(int.into_py(py))
                    } else {
                        Err(PyException::new_err("varint decoding error"))
                    }
                },
                4 => {
                    let float: f64 = f64::from_be_bytes(bytes[1..].try_into().unwrap());
                    Ok(float.into_py(py))
                },
                _ => Err(PyException::new_err("Unknown value type")),
            }
        }
    }
}

#[inline(always)]
fn concat_type_encoding(encoding: u8, payload: &[u8]) -> Box<[u8]> {
    let mut output = Vec::with_capacity(payload.len() + 1);
    output.push(encoding);
    output.extend_from_slice(payload);
    output.into_boxed_slice()
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
