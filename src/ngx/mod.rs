//! Safe(r) Rust surface over `ngx_module/ngx_grpc_shim.{c,h}`.
//!
//! This module owns every `unsafe` call into nginx; the rest of the crate
//! should never need to reach for `raw` directly.
//!
//! Hard constraint: nothing in here may spawn a thread, block, or touch
//! any timer/IO API other than the ones re-exported from this module.
//! This whole crate is driven exclusively by nginx's own event loop, on
//! a single thread. [`assert_worker_thread`] enforces the single-thread
//! half of that at every FFI entry point and event handler.

use std::os::raw::c_void;

pub mod conn;
pub mod logger;
pub mod ssl;
pub mod timer;

/// `ngx_int_t` is `intptr_t` on every platform nginx supports.
#[allow(non_camel_case_types)]
pub type ngx_int_t = isize;
/// `ngx_uint_t` is `uintptr_t`.
#[allow(non_camel_case_types)]
pub type ngx_uint_t = usize;
/// `ngx_msec_t` is `ngx_rbtree_key_t`, itself `ngx_uint_t`.
#[allow(non_camel_case_types)]
pub type ngx_msec_t = usize;
#[allow(non_camel_case_types)]
pub type ngx_socket_t = i32;

pub const NGX_OK: ngx_int_t = 0;
pub const NGX_ERROR: ngx_int_t = -1;
pub const NGX_AGAIN: ngx_int_t = -2;
#[allow(dead_code)]
pub const NGX_BUSY: ngx_int_t = -3;
#[allow(dead_code)]
pub const NGX_DECLINED: ngx_int_t = -5;

/// `NGX_LOG_*` constants (`src/core/ngx_log.h`): the plain `err_levels[]`
/// scale `ngx_log_error` compares `log->log_level` against. Not the
/// `NGX_LOG_DEBUG_*` per-subsystem bitmask, which is for `ngx_log_debug`
/// and is never used by this module.
#[allow(dead_code)]
pub const NGX_LOG_EMERG: ngx_uint_t = 1;
#[allow(dead_code)]
pub const NGX_LOG_ALERT: ngx_uint_t = 2;
#[allow(dead_code)]
pub const NGX_LOG_CRIT: ngx_uint_t = 3;
pub const NGX_LOG_ERR: ngx_uint_t = 4;
pub const NGX_LOG_WARN: ngx_uint_t = 5;
#[allow(dead_code)]
pub const NGX_LOG_NOTICE: ngx_uint_t = 6;
pub const NGX_LOG_INFO: ngx_uint_t = 7;
pub const NGX_LOG_DEBUG: ngx_uint_t = 8;

/// Mirrors `ngx_str_t { size_t len; u_char *data; }`.
#[repr(C)]
pub struct ngx_str_t {
  pub len: usize,
  pub data: *mut u8,
}

impl ngx_str_t {
  pub fn empty() -> Self {
    Self {
      len: 0,
      data: std::ptr::null_mut(),
    }
  }
}

macro_rules! opaque {
  ($name:ident) => {
    #[repr(C)]
    pub struct $name {
      _private: [u8; 0],
    }
  };
}

opaque!(ngx_cycle_t);
opaque!(ngx_log_t);
opaque!(ngx_pool_t);
opaque!(ngx_connection_t);
opaque!(ngx_event_t);
opaque!(ngx_grpc_ssl_ctx_t);

// Matches `sockaddr`. Its fields are never read on the Rust side; the
// pointer is only handed back to nginx.
opaque!(sockaddr_opaque);

#[allow(non_camel_case_types)]
pub type ngx_event_handler_pt = unsafe extern "C" fn(ev: *mut ngx_event_t);
#[allow(non_camel_case_types)]
pub type ngx_connection_handler_pt = unsafe extern "C" fn(c: *mut ngx_connection_t);

/// Raw `extern "C"` declarations matching `ngx_grpc_shim.h` one-to-one.
/// Kept in their own module so call sites read as `raw::ngx_grpc_foo(...)`.
/// Every call here is unsafe by construction; safety is established by
/// the wrappers in the parent module and in `conn.rs`.
pub(crate) mod raw {
  use super::{
    c_void, ngx_connection_handler_pt, ngx_connection_t, ngx_event_handler_pt, ngx_event_t,
    ngx_grpc_ssl_ctx_t, ngx_int_t, ngx_log_t, ngx_msec_t, ngx_pool_t, ngx_socket_t, ngx_str_t,
    ngx_uint_t, sockaddr_opaque,
  };

  unsafe extern "C" {
    pub(crate) fn ngx_grpc_cycle_log() -> *mut ngx_log_t;
    #[allow(dead_code)]
    pub(crate) fn ngx_grpc_cycle_pool() -> *mut ngx_pool_t;
    pub(crate) fn ngx_grpc_worker_exiting() -> ngx_int_t;

    pub(crate) fn ngx_grpc_create_pool(log: *mut ngx_log_t) -> *mut ngx_pool_t;
    pub(crate) fn ngx_grpc_destroy_pool(pool: *mut ngx_pool_t);

    pub(crate) fn ngx_grpc_add_timer(ev: *mut ngx_event_t, timer: ngx_msec_t);
    pub(crate) fn ngx_grpc_del_timer(ev: *mut ngx_event_t);
    #[allow(dead_code)]
    pub(crate) fn ngx_grpc_ev_timer_set(ev: *mut ngx_event_t) -> ngx_int_t;

    pub(crate) fn ngx_grpc_alloc_event(pool: *mut ngx_pool_t) -> *mut ngx_event_t;
    pub(crate) fn ngx_grpc_event_init(
      ev: *mut ngx_event_t,
      handler: ngx_event_handler_pt,
      log: *mut ngx_log_t,
      data: *mut c_void,
    );

    #[allow(dead_code)]
    pub(crate) fn ngx_grpc_ev_ready(ev: *mut ngx_event_t) -> ngx_int_t;
    #[allow(dead_code)]
    pub(crate) fn ngx_grpc_ev_eof(ev: *mut ngx_event_t) -> ngx_int_t;
    #[allow(dead_code)]
    pub(crate) fn ngx_grpc_ev_error(ev: *mut ngx_event_t) -> ngx_int_t;
    #[allow(dead_code)]
    pub(crate) fn ngx_grpc_ev_timedout(ev: *mut ngx_event_t) -> ngx_int_t;
    #[allow(dead_code)]
    pub(crate) fn ngx_grpc_ev_active(ev: *mut ngx_event_t) -> ngx_int_t;

    pub(crate) fn ngx_grpc_conn_set_data(c: *mut ngx_connection_t, data: *mut c_void);
    #[allow(dead_code)]
    pub(crate) fn ngx_grpc_conn_get_data(c: *mut ngx_connection_t) -> *mut c_void;
    pub(crate) fn ngx_grpc_ev_data(ev: *mut ngx_event_t) -> *mut c_void;
    pub(crate) fn ngx_grpc_conn_set_handlers(
      c: *mut ngx_connection_t,
      read_handler: ngx_event_handler_pt,
      write_handler: ngx_event_handler_pt,
    );
    pub(crate) fn ngx_grpc_conn_read_event(c: *mut ngx_connection_t) -> *mut ngx_event_t;
    pub(crate) fn ngx_grpc_conn_write_event(c: *mut ngx_connection_t) -> *mut ngx_event_t;
    #[allow(dead_code)]
    pub(crate) fn ngx_grpc_conn_fd(c: *mut ngx_connection_t) -> ngx_socket_t;

    pub(crate) fn ngx_grpc_parse_addr(
      pool: *mut ngx_pool_t,
      text: *const u8,
      len: usize,
      sockaddr: *mut *mut sockaddr_opaque,
      socklen: *mut u32,
      name_out: *mut ngx_str_t,
    ) -> ngx_int_t;

    pub(crate) fn ngx_grpc_connect(
      log: *mut ngx_log_t,
      sockaddr: *mut sockaddr_opaque,
      socklen: u32,
      name: *mut ngx_str_t,
      out_c: *mut *mut ngx_connection_t,
    ) -> ngx_int_t;

    pub(crate) fn ngx_grpc_check_connect_error(c: *mut ngx_connection_t) -> ngx_int_t;
    pub(crate) fn ngx_grpc_close_connection(c: *mut ngx_connection_t);
    #[allow(dead_code)]
    pub(crate) fn ngx_grpc_reusable_connection(c: *mut ngx_connection_t, reusable: ngx_uint_t);

    pub(crate) fn ngx_grpc_conn_recv(c: *mut ngx_connection_t, buf: *mut u8, size: usize) -> isize;
    pub(crate) fn ngx_grpc_conn_send(
      c: *mut ngx_connection_t,
      buf: *const u8,
      size: usize,
    ) -> isize;
    pub(crate) fn ngx_grpc_handle_read_event(rev: *mut ngx_event_t, flags: ngx_uint_t)
    -> ngx_int_t;
    pub(crate) fn ngx_grpc_handle_write_event(wev: *mut ngx_event_t, lowat: usize) -> ngx_int_t;

    #[allow(dead_code)]
    pub(crate) fn ngx_grpc_ssl_ctx_new(
      pool: *mut ngx_pool_t,
      log: *mut ngx_log_t,
    ) -> *mut ngx_grpc_ssl_ctx_t;
    #[allow(dead_code)]
    pub(crate) fn ngx_grpc_ssl_ctx_free(ctx: *mut ngx_grpc_ssl_ctx_t);
    #[allow(dead_code)]
    pub(crate) fn ngx_grpc_ssl_create_connection(
      ctx: *mut ngx_grpc_ssl_ctx_t,
      c: *mut ngx_connection_t,
      sni: *const u8,
      sni_len: usize,
    ) -> ngx_int_t;
    #[allow(dead_code)]
    pub(crate) fn ngx_grpc_ssl_handshake(c: *mut ngx_connection_t) -> ngx_int_t;
    #[allow(dead_code)]
    pub(crate) fn ngx_grpc_ssl_handshaked(c: *mut ngx_connection_t) -> ngx_int_t;
    #[allow(dead_code)]
    pub(crate) fn ngx_grpc_ssl_set_handshake_handler(
      c: *mut ngx_connection_t,
      handler: ngx_connection_handler_pt,
    );
    #[allow(dead_code)]
    pub(crate) fn ngx_grpc_ssl_get_alpn_protocol(
      c: *mut ngx_connection_t,
      buf: *mut u8,
      buf_len: usize,
    ) -> ngx_int_t;
    #[allow(dead_code)]
    pub(crate) fn ngx_grpc_ssl_shutdown(c: *mut ngx_connection_t) -> ngx_int_t;

    pub(crate) fn ngx_grpc_lua_sema_post(
      sem: *mut c_void,
      is_stream_subsys: ngx_int_t,
      n: ngx_int_t,
    ) -> ngx_int_t;

    pub(crate) fn ngx_grpc_log_error(
      level: ngx_uint_t,
      log: *mut ngx_log_t,
      msg: *const u8,
      msg_len: usize,
    );
    pub(crate) fn ngx_grpc_log_get_level(log: *mut ngx_log_t) -> ngx_uint_t;
  }
}

/// Posts an `ngx.semaphore` from an event handler. There is no Lua
/// coroutine to post from at this point; `sem` is the raw pointer Lua's
/// FFI layer already extracted from the semaphore object, and
/// `is_stream_subsys` picks the http-lua or stream-lua copy of this
/// call.
pub fn lua_sema_post(sem: *mut c_void, is_stream_subsys: bool, n: i64) {
  unsafe {
    raw::ngx_grpc_lua_sema_post(sem, is_stream_subsys as ngx_int_t, n as ngx_int_t);
  }
}

/// Thread that first called [`record_worker_thread`]. Every FFI entry
/// point and every nginx event handler must run on this same thread.
/// This crate is driven exclusively by nginx's own event loop and must
/// never spawn a thread of its own.
static INIT_THREAD: std::sync::OnceLock<std::thread::ThreadId> = std::sync::OnceLock::new();

pub fn record_worker_thread() {
  INIT_THREAD.get_or_init(|| std::thread::current().id());
}

/// Checks that we are still being called from the thread that ran
/// `init_process`. Cheap enough to leave enabled in release builds too,
/// so it stays unconditional.
pub fn assert_worker_thread() {
  if let Some(expected) = INIT_THREAD.get() {
    let current = std::thread::current().id();
    assert_eq!(
      *expected, current,
      "ngx_grpc: called from a different thread than init_process; this must never happen \
       (Rust side is required to be single-threaded, driven only by nginx's event loop)"
    );
  }
}

pub fn cycle_log() -> *mut ngx_log_t {
  unsafe { raw::ngx_grpc_cycle_log() }
}

pub fn worker_exiting() -> bool {
  unsafe { raw::ngx_grpc_worker_exiting() != 0 }
}

/// Recovers the `ngx_connection_t` an event handler was invoked for.
/// Valid for any event belonging to a connection obtained through
/// `ngx_get_connection` (every connection this module creates), which
/// sets both `c->read->data` and `c->write->data` to `c` itself.
pub fn event_connection(ev: *mut ngx_event_t) -> *mut ngx_connection_t {
  unsafe { raw::ngx_grpc_ev_data(ev) as *mut ngx_connection_t }
}

/// Called once per worker process, from `ngx_grpc_client_module`'s
/// `init_process` hook (ngx_module/ngx_grpc_module.c).
#[unsafe(no_mangle)]
pub extern "C" fn ngx_grpc_rs_init_process(_cycle: *mut ngx_cycle_t) -> ngx_int_t {
  record_worker_thread();
  logger::init();
  log::debug!(
    "ngx_grpc: init_process on {:?}",
    std::thread::current().id()
  );
  NGX_OK
}

/// Called once per worker process on shutdown. Must not panic: this runs
/// during graceful shutdown and a panic here would abort the worker
/// mid-exit.
#[unsafe(no_mangle)]
pub extern "C" fn ngx_grpc_rs_exit_process(_cycle: *mut ngx_cycle_t) {
  assert_worker_thread();
  log::debug!("ngx_grpc: exit_process");
  crate::pool::on_worker_exit();
}
