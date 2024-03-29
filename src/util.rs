use libc::{c_char, c_void};
use pyo3::exceptions::PyException;
use pyo3::PyResult;
use std::ffi::{CStr, CString};
use std::path::Path;

pub(crate) fn error_message(ptr: *const c_char) -> String {
    unsafe {
        let s = from_cstr(ptr);
        libc::free(ptr as *mut c_void);
        s
    }
}

pub(crate) unsafe fn from_cstr(ptr: *const c_char) -> String {
    let cstr = CStr::from_ptr(ptr as *const _);
    String::from_utf8_lossy(cstr.to_bytes()).into_owned()
}

pub(crate) fn to_cpath<P: AsRef<Path>>(path: P) -> PyResult<CString> {
    match CString::new(path.as_ref().to_string_lossy().as_bytes()) {
        Ok(c) => Ok(c),
        Err(e) => Err(PyException::new_err(format!(
            "Failed to convert path to CString: {e}",
        ))),
    }
}
