use pyo3::create_exception;
use pyo3::exceptions::PyException;

create_exception!(
    rocksdict,
    DbClosedError,
    PyException,
    "Raised when accessing a closed database instance."
);
