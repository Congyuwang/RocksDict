use libc::{c_char, c_void};
use std::ffi::CStr;

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
