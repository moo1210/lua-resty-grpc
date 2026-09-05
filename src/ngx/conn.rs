//! Non-blocking connect via `ngx_event_connect_peer`, wrapped through the
//! C shim in `ngx_module/ngx_grpc_shim.c`.
//!
//! Address parsing only accepts numeric `ip:port` (or `[ipv6]:port`).
//! `ngx_grpc_parse_addr` is backed by `ngx_parse_addr_port`, which never
//! resolves a hostname and never blocks. A real hostname is rejected
//! with `NGX_DECLINED`; DNS resolution is not implemented.

use super::{
  NGX_AGAIN, NGX_OK, ngx_connection_t, ngx_event_t, ngx_int_t, ngx_str_t, ngx_uint_t,
  sockaddr_opaque,
};
use crate::runtime::{self, Reactor};
use std::{
  cell::{Cell, RefCell},
  future::Future,
  os::raw::c_void,
  pin::Pin,
  ptr,
  rc::Rc,
  task::{Context, Poll, Waker},
};

/// Owns a connected (or connecting) `ngx_connection_t`. Dropping it
/// closes the connection and destroys its pool. Code that hands the
/// connection off to a longer-lived owner (the worker-level connection
/// pool in `pool.rs`) must `mem::forget` this wrapper, or drive the raw
/// pointer directly instead of constructing one. This type is mainly a
/// convenience for the initial connect sequence.
pub struct Connection(*mut ngx_connection_t);

/// Why a connect attempt failed. Formatted via `Debug` into the
/// transport-error status shown to the caller.
#[derive(Debug)]
#[allow(dead_code)]
pub enum ConnectError {
  /// The address text is not a numeric `ip:port` (or is malformed).
  BadAddress,
  /// `ngx_event_connect_peer` itself failed (NGX_ERROR/NGX_DECLINED/NGX_BUSY).
  Connect(ngx_int_t),
}

/// Outcome of a connect attempt that didn't fail outright.
///
/// This distinction is not cosmetic: on `Connected`, nginx's
/// `ngx_event_connect_peer` sets `wev->ready = 1` directly and returns
/// without registering the write event with epoll. Only the
/// `NGX_AGAIN` path calls `ngx_add_event(wev, ...)`. A caller that
/// always waits for a write event before proceeding hangs forever on a
/// connect that completes synchronously, which is typical for loopback.
/// Callers must branch on this and, for `Connected`, proceed
/// immediately instead of arming a handler and waiting.
pub enum ConnectOutcome {
  /// TCP handshake already completed synchronously.
  Connected(Connection),
  /// In progress; the write event has been registered with epoll and
  /// will fire once connected or failed. That handler checks the
  /// result via `ngx_grpc_check_connect_error`.
  Pending(Connection),
}

impl Connection {
  /// Starts a non-blocking connect to `addr_text` (must be `ip:port`).
  /// See [`ConnectOutcome`] for why the caller must not treat the two
  /// outcomes the same way.
  pub fn connect(addr_text: &str) -> Result<ConnectOutcome, ConnectError> {
    super::assert_worker_thread();

    unsafe {
      // ngx_event_connect_peer never retains pc.sockaddr/pc.name past
      // the synchronous connect() syscall, so this pool only needs to
      // live for the duration of this call and is destroyed before we
      // return.
      let scratch = super::raw::ngx_grpc_create_pool(super::cycle_log());
      if scratch.is_null() {
        return Err(ConnectError::Connect(super::NGX_ERROR));
      }

      let mut sockaddr: *mut sockaddr_opaque = ptr::null_mut();
      let mut socklen: u32 = 0;
      let mut name = ngx_str_t::empty();

      let rc = super::raw::ngx_grpc_parse_addr(
        scratch,
        addr_text.as_ptr(),
        addr_text.len(),
        &mut sockaddr,
        &mut socklen,
        &mut name,
      );

      if rc != NGX_OK {
        super::raw::ngx_grpc_destroy_pool(scratch);
        return Err(ConnectError::BadAddress);
      }

      let mut out_c: *mut ngx_connection_t = ptr::null_mut();
      let rc =
        super::raw::ngx_grpc_connect(super::cycle_log(), sockaddr, socklen, &mut name, &mut out_c);

      // Safe to drop now regardless of `rc`. connect_peer only reads
      // sockaddr/name synchronously and never aliases them into `c`.
      super::raw::ngx_grpc_destroy_pool(scratch);

      if rc != NGX_OK && rc != NGX_AGAIN {
        return Err(ConnectError::Connect(rc));
      }

      debug_assert!(!out_c.is_null());
      let conn = Connection(out_c);
      Ok(if rc == NGX_OK {
        ConnectOutcome::Connected(conn)
      } else {
        ConnectOutcome::Pending(conn)
      })
    }
  }

  pub fn as_ptr(&self) -> *mut ngx_connection_t {
    self.0
  }

  /// Reclaims ownership of a raw pointer produced by
  /// [`Connection::into_raw`], for example after waiting out a connect
  /// completion via a hand-installed event handler, or when the
  /// worker-level connection pool hands a connection back for a fresh
  /// I/O wrapper. `c` must be a live, non-null connection this code is
  /// solely responsible for from this point on.
  pub fn from_raw(c: *mut ngx_connection_t) -> Self {
    debug_assert!(!c.is_null());
    Self(c)
  }

  /// Marks the connection reusable in nginx's global reusable-
  /// connections queue, so it can be reclaimed under connection
  /// pressure.
  #[allow(dead_code)]
  pub fn mark_reusable(&self, reusable: bool) {
    unsafe { super::raw::ngx_grpc_reusable_connection(self.0, reusable as ngx_uint_t) };
  }

  /// Consumes the wrapper without closing the connection, for handing
  /// it off to the worker-level connection pool, which owns the raw
  /// pointer from then on.
  pub fn into_raw(self) -> *mut ngx_connection_t {
    let ptr = self.0;
    std::mem::forget(self);
    ptr
  }

  /// Waits out a [`ConnectOutcome::Pending`] connect. Consumes `self`;
  /// resolves to the same connection on success (handlers installed
  /// here are left in place until the caller sets its own), or closes it
  /// and resolves to an error on failure.
  pub fn wait_connected(self, reactor: Reactor) -> ConnectWaitFuture {
    let c = self.into_raw();
    let state = Rc::new(ConnectWaitState {
      waker: RefCell::new(None),
      result: Cell::new(None),
      reactor,
    });
    unsafe {
      super::raw::ngx_grpc_conn_set_data(c, Rc::as_ptr(&state) as *mut c_void);
      super::raw::ngx_grpc_conn_set_handlers(c, on_connect_wait_event, on_connect_wait_event);
    }
    ConnectWaitFuture { c: Some(c), state }
  }
}

impl Drop for Connection {
  fn drop(&mut self) {
    if !self.0.is_null() {
      unsafe { super::raw::ngx_grpc_close_connection(self.0) };
    }
  }
}

struct ConnectWaitState {
  waker: RefCell<Option<Waker>>,
  /// `None` = still pending; `Some(true/false)` = settled.
  result: Cell<Option<bool>>,
  reactor: Reactor,
}

pub struct ConnectWaitFuture {
  c: Option<*mut ngx_connection_t>,
  state: Rc<ConnectWaitState>,
}

impl Future for ConnectWaitFuture {
  type Output = Result<Connection, ConnectError>;

  fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
    if let Some(ok) = self.state.result.get() {
      let c = self
        .c
        .take()
        .expect("ConnectWaitFuture polled after completion");
      return Poll::Ready(if ok {
        Ok(Connection(c))
      } else {
        unsafe { super::raw::ngx_grpc_close_connection(c) };
        Err(ConnectError::Connect(super::NGX_ERROR))
      });
    }
    *self.state.waker.borrow_mut() = Some(cx.waker().clone());
    Poll::Pending
  }
}

impl Drop for ConnectWaitFuture {
  fn drop(&mut self) {
    // `c` is only still `Some` if the future was dropped before
    // resolving. Close whatever it was waiting on rather than leak the
    // fd and connection slot.
    if let Some(c) = self.c.take() {
      unsafe { super::raw::ngx_grpc_close_connection(c) };
    }
  }
}

extern "C" fn on_connect_wait_event(ev: *mut ngx_event_t) {
  super::assert_worker_thread();
  let c = super::event_connection(ev);
  let state = unsafe { &*(super::raw::ngx_grpc_conn_get_data(c) as *const ConnectWaitState) };
  if state.result.get().is_some() {
    return;
  }
  let ok = unsafe { super::raw::ngx_grpc_check_connect_error(c) } == NGX_OK;
  state.result.set(Some(ok));
  // The extracted `Option<Waker>` must be bound to a variable before
  // acting on it. Otherwise the `RefMut` temporary stays borrowed
  // across `drive_once`'s re-entrant poll and panics.
  let woken = state.waker.borrow_mut().take();
  if let Some(w) = woken {
    w.wake();
    state.reactor.drive_once(runtime::DRIVE_BUDGET);
  }
}
