//! FFI surface for `lualib/resty/grpc.lua`.
//!
//! This is a synchronous, non-blocking API: Lua starts a call, then
//! blocks the calling coroutine on an `ngx.semaphore` (`sem:wait()`, a
//! real yield, not a poll loop) until Rust posts it from whatever
//! nginx event handler produced the next message or the final status.
//! The worker process itself is never blocked; other coroutines keep
//! running on nginx's event loop in the meantime. See
//! `client::CallState::push` and `ngx::lua_sema_post`. There is no
//! callback surface here at all.
//!
//! Every payload here is raw bytes (protobuf, already encoded/decoded by
//! Lua via lua-protobuf). Rust never parses a message; it only frames
//! it (`codec::BytesCodec`) and moves it.

use std::{
  cell::RefCell,
  ffi::{CString, c_char, c_void},
};

mod client;
mod codec;
mod ffi;
mod handle;
mod ngx;
mod pool;
mod runtime;
mod transport;

use ffi::{FfiStr, call_with_result, err};
use handle::{HandleError, HandleMap};

#[allow(non_camel_case_types)]
pub type grpc_client_handle = u64;
#[allow(non_camel_case_types)]
pub type grpc_call_handle = u64;
#[allow(non_camel_case_types)]
pub type grpc_error = ffi::GrpcError;

thread_local! {
  static CLIENTS: RefCell<HandleMap<client::GrpcClient>> = RefCell::new(HandleMap::new());
  static CALLS: RefCell<HandleMap<client::GrpcCall>> = RefCell::new(HandleMap::new());
}

/// # Safety
/// `ptr` must be valid for `len` bytes, or `len == 0`, in which case
/// `ptr` may be null or dangling, for the duration of this call. The
/// bytes are copied out immediately; nothing here retains `ptr` itself
/// past this function.
unsafe fn bytes_from_raw(ptr: *const u8, len: usize) -> bytes::Bytes {
  if len == 0 || ptr.is_null() {
    return bytes::Bytes::new();
  }
  bytes::Bytes::copy_from_slice(unsafe { std::slice::from_raw_parts(ptr, len) })
}

#[unsafe(no_mangle)]
pub extern "C" fn grpc_client_version() -> *const c_char {
  // Intentionally never freed: this is a static string for the
  // lifetime of the process, called at most once at module load.
  static VERSION: std::sync::OnceLock<CString> = std::sync::OnceLock::new();
  VERSION
    .get_or_init(|| CString::new(env!("CARGO_PKG_VERSION")).unwrap())
    .as_ptr()
}

/// Creates a client bound to one endpoint. `sni` may be an empty string
/// (meaning "no SNI" / not applicable for plaintext).
///
/// # Safety
/// `addr`/`scheme`/`sni` must be valid `FfiStr`s for the call's
/// duration.
#[unsafe(no_mangle)]
pub extern "C" fn grpc_client_new(
  addr: FfiStr,
  scheme: FfiStr,
  sni: FfiStr,
  out_err: &mut grpc_error,
) -> grpc_client_handle {
  call_with_result(out_err, || -> Result<u64, grpc_error> {
    let addr = addr.into_opt_string().ok_or_else(|| err("invalid addr"))?;
    let scheme = scheme
      .into_opt_string()
      .ok_or_else(|| err("invalid scheme"))?;
    let sni = sni.into_opt_string().filter(|s| !s.is_empty());

    let tls = match scheme.as_str() {
      "http" => false,
      "https" => true,
      other => return Err(err(format!("unsupported scheme {other:?}"))),
    };

    let key = pool::EndpointKey { addr, tls, sni };
    Ok(CLIENTS.with(|m| m.borrow_mut().insert(client::GrpcClient::new(key))))
  })
}

#[unsafe(no_mangle)]
pub extern "C" fn grpc_client_free(handle: grpc_client_handle, out_err: &mut grpc_error) {
  call_with_result(out_err, || -> Result<(), HandleError> {
    CLIENTS.with(|m| m.borrow_mut().delete(handle))
  });
}

fn insert_call(
  state: std::rc::Rc<client::CallState>,
  outbound: Option<tokio::sync::mpsc::Sender<bytes::Bytes>>,
) -> u64 {
  CALLS.with(|m| {
    m.borrow_mut()
      .insert(client::GrpcCall::new(state, outbound))
  })
}

/// Starts a unary call.
///
/// # Safety
/// `path` must be a valid `FfiStr`. `body_ptr`/`body_len` must describe
/// a valid (possibly zero-length) byte buffer for the call's duration.
/// `sem` is an opaque pointer Lua's `ngx.semaphore` FFI binding produced
/// (never dereferenced here, only forwarded to `ngx_grpc_lua_sema_post`);
/// it must stay valid until the call completes and is polled to
/// completion or freed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn grpc_call_unary(
  client_handle: grpc_client_handle,
  path: FfiStr,
  body_ptr: *const u8,
  body_len: usize,
  sem: *mut c_void,
  is_stream_subsys: u8,
  out_err: &mut grpc_error,
) -> grpc_call_handle {
  call_with_result(out_err, || -> Result<u64, grpc_error> {
    let path = path.into_opt_string().ok_or_else(|| err("invalid path"))?;
    let body = unsafe { bytes_from_raw(body_ptr, body_len) };

    CLIENTS.with(|m| -> Result<u64, grpc_error> {
      let clients = m.borrow();
      let client = clients.get(client_handle)?;
      let state = client::start_unary(client, path, body, sem, is_stream_subsys != 0);
      Ok(insert_call(state, None))
    })
  })
}

/// Starts a server-streaming call.
///
/// # Safety
/// Same contract as [`grpc_call_unary`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn grpc_call_server_streaming(
  client_handle: grpc_client_handle,
  path: FfiStr,
  body_ptr: *const u8,
  body_len: usize,
  sem: *mut c_void,
  is_stream_subsys: u8,
  out_err: &mut grpc_error,
) -> grpc_call_handle {
  call_with_result(out_err, || -> Result<u64, grpc_error> {
    let path = path.into_opt_string().ok_or_else(|| err("invalid path"))?;
    let body = unsafe { bytes_from_raw(body_ptr, body_len) };

    CLIENTS.with(|m| -> Result<u64, grpc_error> {
      let clients = m.borrow();
      let client = clients.get(client_handle)?;
      let state = client::start_server_streaming(client, path, body, sem, is_stream_subsys != 0);
      Ok(insert_call(state, None))
    })
  })
}

/// Starts a client-streaming call. Request messages are fed in
/// afterwards via [`grpc_call_send`], terminated by [`grpc_call_done`].
///
/// # Safety
/// `path` must be a valid `FfiStr`; `sem` per [`grpc_call_unary`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn grpc_call_client_streaming(
  client_handle: grpc_client_handle,
  path: FfiStr,
  sem: *mut c_void,
  is_stream_subsys: u8,
  out_err: &mut grpc_error,
) -> grpc_call_handle {
  call_with_result(out_err, || -> Result<u64, grpc_error> {
    let path = path.into_opt_string().ok_or_else(|| err("invalid path"))?;

    CLIENTS.with(|m| -> Result<u64, grpc_error> {
      let clients = m.borrow();
      let client = clients.get(client_handle)?;
      let (state, tx) = client::start_client_streaming(client, path, sem, is_stream_subsys != 0);
      Ok(insert_call(state, Some(tx)))
    })
  })
}

/// Starts a bidirectional-streaming call. Same shape as
/// [`grpc_call_client_streaming`], except responses stream back too.
///
/// # Safety
/// Same as [`grpc_call_client_streaming`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn grpc_call_streaming(
  client_handle: grpc_client_handle,
  path: FfiStr,
  sem: *mut c_void,
  is_stream_subsys: u8,
  out_err: &mut grpc_error,
) -> grpc_call_handle {
  call_with_result(out_err, || -> Result<u64, grpc_error> {
    let path = path.into_opt_string().ok_or_else(|| err("invalid path"))?;

    CLIENTS.with(|m| -> Result<u64, grpc_error> {
      let clients = m.borrow();
      let client = clients.get(client_handle)?;
      let (state, tx) = client::start_streaming(client, path, sem, is_stream_subsys != 0);
      Ok(insert_call(state, Some(tx)))
    })
  })
}

/// Pushes one request message on a client-streaming/bidi call. A call
/// with no `outbound` sink (unary/server-streaming) silently ignores
/// this. Lua's wrapper is expected to never call it for those, but a
/// hard error here would just be one more thing to handle for no
/// benefit.
///
/// # Safety
/// `body_ptr`/`body_len` per [`grpc_call_unary`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn grpc_call_send(
  handle: grpc_call_handle,
  body_ptr: *const u8,
  body_len: usize,
  out_err: &mut grpc_error,
) {
  call_with_result(out_err, || -> Result<(), HandleError> {
    let body = unsafe { bytes_from_raw(body_ptr, body_len) };
    CALLS.with(|m| -> Result<(), HandleError> {
      let calls = m.borrow();
      let call = calls.get(handle)?;
      if let Some(tx) = call.outbound.borrow().as_ref() {
        // Backpressure/closed-channel errors surface to Lua through the
        // eventual Done event (the driver task's `send_request`/stream
        // will itself fail and produce a status), not here.
        let _ = tx.try_send(body);
      }
      Ok(())
    })
  });
  // `try_send` only wakes the task reading this channel. Nothing else
  // polls it afterward, so it must be driven explicitly here, the same
  // as every nginx event handler in this crate.
  runtime::worker_reactor().drive_once(runtime::DRIVE_BUDGET);
}

/// Signals no more request messages on a client-streaming/bidi call.
#[unsafe(no_mangle)]
pub extern "C" fn grpc_call_done(handle: grpc_call_handle, out_err: &mut grpc_error) {
  call_with_result(out_err, || -> Result<(), HandleError> {
    CALLS.with(|m| -> Result<(), HandleError> {
      let calls = m.borrow();
      let call = calls.get(handle)?;
      // Dropping the sender ends the ReceiverStream the driver task is
      // reading from.
      *call.outbound.borrow_mut() = None;
      Ok(())
    })
  });
  runtime::worker_reactor().drive_once(runtime::DRIVE_BUDGET);
}

/// Cooperative cancellation. See `CallState::cancelled`. Meant to be
/// called from Lua's `ngx.on_abort` handler when the client disconnects
/// mid-call.
#[unsafe(no_mangle)]
pub extern "C" fn grpc_call_cancel(handle: grpc_call_handle, out_err: &mut grpc_error) {
  call_with_result(out_err, || -> Result<(), HandleError> {
    CALLS.with(|m| -> Result<(), HandleError> {
      let calls = m.borrow();
      calls.get(handle)?.state.cancel();
      Ok(())
    })
  });
}

#[unsafe(no_mangle)]
pub extern "C" fn grpc_call_free(handle: grpc_call_handle, out_err: &mut grpc_error) {
  call_with_result(out_err, || -> Result<(), HandleError> {
    // `GrpcCall::drop` cancels the driver task cooperatively.
    CALLS.with(|m| m.borrow_mut().delete(handle))
  });
}

/// Poll result tags, mirrored in `lualib/resty/grpc.lua`.
const GRPC_POLL_PENDING: u8 = 0;
const GRPC_POLL_MESSAGE: u8 = 1;
const GRPC_POLL_DONE: u8 = 2;

/// Drains one pending event. Lua calls this after `sem:wait()` returns
/// (and in a loop until it returns `GRPC_POLL_PENDING` again, since one
/// `sem:post()` only guarantees "at least one new event", not exactly
/// one).
///
/// On `GRPC_POLL_MESSAGE`, `*out_msg_ptr`/`*out_msg_len` describe the
/// message bytes. On `GRPC_POLL_DONE`, `*out_status_code` is the gRPC
/// status code (0 = OK) and `*out_status_msg_ptr`/`*out_status_msg_len`
/// describe the status message (possibly zero-length). Every out-buffer
/// is owned by the call and only valid until the next `grpc_call_poll`
/// on the same handle. Lua must copy it out (`ffi.string`) before
/// calling this again.
///
/// # Safety
/// All four `out_*` pointers must be valid, aligned, writable pointers
/// of their respective types.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn grpc_call_poll(
  handle: grpc_call_handle,
  out_msg_ptr: *mut *const u8,
  out_msg_len: *mut usize,
  out_status_code: *mut i32,
  out_status_msg_ptr: *mut *const u8,
  out_status_msg_len: *mut usize,
  out_err: &mut grpc_error,
) -> u8 {
  call_with_result(out_err, || -> Result<u8, HandleError> {
    CALLS.with(|m| -> Result<u8, HandleError> {
      let calls = m.borrow();
      let call = calls.get(handle)?;

      match call.state.poll() {
        None => Ok(GRPC_POLL_PENDING),
        Some(client::PollResult::Message) => {
          let msg = call.state.last_message();
          let msg = msg.as_ref().expect("poll() just set last_message");
          unsafe {
            *out_msg_ptr = msg.as_ptr();
            *out_msg_len = msg.len();
          }
          Ok(GRPC_POLL_MESSAGE)
        }
        Some(client::PollResult::Done(code)) => {
          let status_msg = call.state.last_status_message();
          let status_msg = status_msg
            .as_ref()
            .expect("poll() just set last_status_message");
          unsafe {
            *out_status_code = code;
            *out_status_msg_ptr = status_msg.as_ptr();
            *out_status_msg_len = status_msg.len();
          }
          Ok(GRPC_POLL_DONE)
        }
      }
    })
  })
}
