mod encoder;
mod mdict;
mod options;
mod rdict;

use crate::mdict::Mdict;
use crate::options::OptionsPy;
use crate::rdict::Rdict;
use pyo3::prelude::*;

#[pymodule]
fn rocksdict(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<Rdict>()?;
    m.add_class::<Mdict>()?;
    m.add_class::<OptionsPy>()?;
    Ok(())
}
