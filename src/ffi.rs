//! The small pieces of FFI plumbing `lib.rs` needs: a C-repr error
//! struct, a borrowed C-string wrapper usable directly as an
//! `extern "C"` parameter type, and a panic-catching call wrapper.
//!
//! `err()`'s message lives in a thread-local scratch buffer rather
//! than a fresh heap allocation per call: every call site in
//! `lualib/resty/grpc.lua` reads `err.message` with `ffi.string()`
//! immediately after the FFI call returns, with no yield in between,
//! so the buffer is never read after a later call has overwritten it.

use std::{
  cell::RefCell,
  ffi::{CStr, CString, c_char},
  marker::PhantomData,
  panic,
};

use crate::handle::HandleError;

/// Mirrors `grpc_client_error` in `grpc.lua`'s `ffi.cdef`. Field order
/// and types are part of that ABI; do not reorder.
#[repr(C)]
pub struct GrpcError {
  code: i32,
  message: *const c_char,
}

impl GrpcError {
  fn success() -> Self {
    Self {
      code: 0,
      message: std::ptr::null(),
    }
  }
}

impl From<HandleError> for GrpcError {
  fn from(_: HandleError) -> Self {
    err("invalid or stale handle")
  }
}

thread_local! {
  static ERROR_MESSAGE: RefCell<Option<CString>> = const { RefCell::new(None) };
}

pub fn err(msg: impl Into<String>) -> GrpcError {
  // `err()` also runs from `call_with_result`'s panic branch, outside
  // any `catch_unwind` of its own, so this must never itself panic;
  // an embedded NUL (never expected in practice) is stripped rather
  // than rejected.
  let msg = CString::new(msg.into().replace('\0', "")).unwrap();
  let ptr = ERROR_MESSAGE.with(|cell| {
    *cell.borrow_mut() = Some(msg);
    cell.borrow().as_ref().unwrap().as_ptr()
  });
  GrpcError {
    code: 1,
    message: ptr,
  }
}

/// A borrowed, possibly-null C string received across the FFI
/// boundary. `#[repr(transparent)]` over the raw pointer so it can be
/// used directly in an `extern "C"` function's parameter list.
#[repr(transparent)]
#[derive(Clone, Copy)]
pub struct FfiStr<'a> {
  ptr: *const c_char,
  _marker: PhantomData<&'a ()>,
}

impl FfiStr<'_> {
  pub fn into_opt_string(self) -> Option<String> {
    if self.ptr.is_null() {
      return None;
    }
    Some(
      unsafe { CStr::from_ptr(self.ptr) }
        .to_string_lossy()
        .into_owned(),
    )
  }
}

/// Runs `callback`, catching a panic and reporting it through
/// `out_error` the same way a returned `Err` is, so a panic degrades
/// to one failed call instead of unwinding across the FFI boundary.
/// `out_error` is set to success first, so every path (`Ok`, `Err`, or
/// panic) leaves it in a defined state.
pub fn call_with_result<R: Default, E: Into<GrpcError>>(
  out_error: &mut GrpcError,
  callback: impl FnOnce() -> Result<R, E> + panic::UnwindSafe,
) -> R {
  *out_error = GrpcError::success();
  match panic::catch_unwind(callback) {
    Ok(Ok(value)) => value,
    Ok(Err(e)) => {
      *out_error = e.into();
      R::default()
    }
    Err(payload) => {
      let msg = payload
        .downcast_ref::<&str>()
        .map(|s| s.to_string())
        .or_else(|| payload.downcast_ref::<String>().cloned())
        .unwrap_or_else(|| "panic in FFI call".to_string());
      *out_error = err(msg);
      R::default()
    }
  }
}
