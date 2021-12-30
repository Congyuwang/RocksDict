mod encoder;
mod mdict;
mod options;
mod rdict;

use crate::mdict::Mdict;
use crate::options::*;
use crate::rdict::Rdict;
use pyo3::prelude::*;

#[pymodule]
fn rocksdict(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<Rdict>()?;
    m.add_class::<Mdict>()?;
    m.add_class::<OptionsPy>()?;
    m.add_class::<MemtableFactoryPy>()?;
    m.add_class::<BlockBasedOptionsPy>()?;
    m.add_class::<CuckooTableOptionsPy>()?;
    m.add_class::<PlainTableFactoryOptionsPy>()?;
    m.add_class::<CachePy>()?;
    m.add_class::<BlockBasedIndexTypePy>()?;
    m.add_class::<DataBlockIndexTypePy>()?;
    m.add_class::<SliceTransformPy>()?;
    m.add_class::<DBPathPy>()?;
    m.add_class::<WriteOptionsPy>()?;
    m.add_class::<FlushOptionsPy>()?;
    m.add_class::<ReadOptionsPy>()?;
    Ok(())
}
