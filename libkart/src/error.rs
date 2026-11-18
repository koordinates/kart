//! Error type and thread-local "last error" storage for the C ABI.

use std::cell::RefCell;
use std::ffi::CString;
use std::os::raw::c_char;

#[derive(Debug)]
pub enum Error {
    NotImplemented(&'static str),
    NotFound(String),
    Format(String),
    Git(git2::Error),
    Msgpack(String),
    Json(String),
    Utf8(String),
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::NotImplemented(what) => write!(f, "not implemented: {what}"),
            Error::NotFound(m) => write!(f, "not found: {m}"),
            Error::Format(m) => write!(f, "format error: {m}"),
            Error::Git(e) => write!(f, "git error: {e}"),
            Error::Msgpack(m) => write!(f, "msgpack error: {m}"),
            Error::Json(m) => write!(f, "json error: {m}"),
            Error::Utf8(m) => write!(f, "utf-8 error: {m}"),
        }
    }
}

impl std::error::Error for Error {}

impl From<git2::Error> for Error {
    fn from(e: git2::Error) -> Self {
        Error::Git(e)
    }
}

impl From<serde_json::Error> for Error {
    fn from(e: serde_json::Error) -> Self {
        Error::Json(e.to_string())
    }
}

impl From<std::str::Utf8Error> for Error {
    fn from(e: std::str::Utf8Error) -> Self {
        Error::Utf8(e.to_string())
    }
}

pub type Result<T> = std::result::Result<T, Error>;

thread_local! {
    static LAST_ERROR: RefCell<CString> = RefCell::new(CString::new("").unwrap());
}

/// Record the most recent error message for the current thread.
pub fn set_last_error(msg: &str) {
    // Replace interior NULs so CString::new can't fail.
    let cleaned: String = msg.replace('\0', " ");
    let c = CString::new(cleaned).unwrap_or_else(|_| CString::new("error").unwrap());
    LAST_ERROR.with(|slot| *slot.borrow_mut() = c);
}

/// Pointer to the current thread's last error string. Valid until the next libkart
/// call on this thread. Never NULL.
pub fn last_error_ptr() -> *const c_char {
    LAST_ERROR.with(|slot| slot.borrow().as_ptr())
}
