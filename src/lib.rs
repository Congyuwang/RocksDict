mod encoder;
mod iter;
mod options;
mod rdict;
mod util;

use crate::iter::RdictIter;
use crate::options::*;
use crate::rdict::Rdict;
use pyo3::prelude::*;

#[pymodule]
fn rocksdict(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<Rdict>()?;
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
    m.add_class::<DBCompressionTypePy>()?;
    m.add_class::<DBCompactionStylePy>()?;
    m.add_class::<DBRecoveryModePy>()?;
    m.add_class::<UniversalCompactOptionsPy>()?;
    m.add_class::<UniversalCompactionStopStylePy>()?;
    m.add_class::<EnvPy>()?;
    m.add_class::<FifoCompactOptionsPy>()?;
    m.add_class::<RdictIter>()?;
    Ok(())
}
