//! Safe(r) wrapper over the TLS half of `ngx_grpc_shim.{c,h}`. This is
//! the piece that actually solves the ALPN problem this crate exists
//! for: the `SSL_CTX` here is entirely ours, so we can call
//! `SSL_CTX_set_alpn_protos` directly instead of going through whatever
//! a cosocket happens to expose.

use super::{
  NGX_AGAIN, NGX_OK, conn::Connection, ngx_connection_t, ngx_grpc_ssl_ctx_t, ngx_int_t, ngx_log_t,
  ngx_pool_t,
};
use crate::runtime::{self, Reactor};
use std::{
  cell::{Cell, RefCell},
  future::Future,
  os::raw::c_void,
  pin::Pin,
  rc::Rc,
  task::{Context, Poll, Waker},
};

/// Owns an `ngx_ssl_t` (SSL_CTX plus ALPN and session-cache setup).
/// Meant to live at worker-pool granularity, one per distinct TLS
/// configuration, not per-connection. See how `pool.rs` caches these by
/// SNI.
pub struct SslCtx(*mut ngx_grpc_ssl_ctx_t);

impl SslCtx {
  pub fn new(pool: *mut ngx_pool_t, log: *mut ngx_log_t) -> Option<Self> {
    let ptr = unsafe { super::raw::ngx_grpc_ssl_ctx_new(pool, log) };
    if ptr.is_null() { None } else { Some(Self(ptr)) }
  }
}

impl Drop for SslCtx {
  fn drop(&mut self) {
    if !self.0.is_null() {
      unsafe { super::raw::ngx_grpc_ssl_ctx_free(self.0) };
    }
  }
}

/// Attaches TLS to an already-connected `ngx_connection_t`. `sni`, if
/// given, sets SNI via `SSL_set_tlsext_host_name`.
pub fn create_connection(
  ctx: &SslCtx,
  c: *mut ngx_connection_t,
  sni: Option<&str>,
) -> Result<(), ngx_int_t> {
  let (ptr, len) = match sni {
    Some(s) => (s.as_ptr(), s.len()),
    None => (std::ptr::null(), 0),
  };
  let rc = unsafe { super::raw::ngx_grpc_ssl_create_connection(ctx.0, c, ptr, len) };
  if rc == NGX_OK { Ok(()) } else { Err(rc) }
}

pub enum HandshakeOutcome {
  /// Completed synchronously. Rare, but must be handled: nothing will
  /// invoke the handshake handler in this case.
  Done,
  /// In progress; nginx has taken over `c->read`/`c->write` with its own
  /// internal handshake handler and will call the handler installed via
  /// `ngx_grpc_ssl_set_handshake_handler` once it settles.
  Pending,
}

pub fn handshake(c: *mut ngx_connection_t) -> Result<HandshakeOutcome, ngx_int_t> {
  match unsafe { super::raw::ngx_grpc_ssl_handshake(c) } {
    NGX_OK => Ok(HandshakeOutcome::Done),
    NGX_AGAIN => Ok(HandshakeOutcome::Pending),
    rc => Err(rc),
  }
}

/// Only meaningful once the handshake handler has actually fired, or
/// [`handshake`] returned `Done`. `false` means the handshake failed or
/// was rejected.
pub fn handshaked(c: *mut ngx_connection_t) -> bool {
  unsafe { super::raw::ngx_grpc_ssl_handshaked(c) != 0 }
}

/// The negotiated ALPN protocol, if any. `None` covers both "peer
/// doesn't support ALPN" and "no protocol overlap": nginx's own
/// `ngx_ssl_get_alpn_protocol` doesn't distinguish the two either.
pub fn alpn_protocol(c: *mut ngx_connection_t) -> Option<String> {
  let mut buf = [0u8; 64];
  let n = unsafe { super::raw::ngx_grpc_ssl_get_alpn_protocol(c, buf.as_mut_ptr(), buf.len()) };
  if n <= 0 {
    None
  } else {
    Some(String::from_utf8_lossy(&buf[..n as usize]).into_owned())
  }
}

#[allow(dead_code)]
pub fn shutdown(c: *mut ngx_connection_t) {
  unsafe {
    super::raw::ngx_grpc_ssl_shutdown(c);
  }
}

struct HandshakeWaitState {
  waker: RefCell<Option<Waker>>,
  /// `None` = still pending; `Some(true/false)` = settled.
  result: Cell<Option<bool>>,
  reactor: Reactor,
}

/// Waits out a [`HandshakeOutcome::Pending`] TLS handshake. `c` must
/// already have had [`create_connection`] and [`handshake`] called on
/// it. This only installs the completion handler and awaits it.
pub fn wait_handshake(c: *mut ngx_connection_t, reactor: Reactor) -> HandshakeWaitFuture {
  let state = Rc::new(HandshakeWaitState {
    waker: RefCell::new(None),
    result: Cell::new(None),
    reactor,
  });
  unsafe {
    // Overwrites whatever `c->data` held before, for example a
    // since-dropped `ConnectWaitState` from `Connection::wait_connected`.
    // That Rc's pointer goes dangling the moment its future resolves,
    // but nothing reads `c->data` in between; every consumer of a
    // `Connection` that touches it sets it fresh first, this function
    // included.
    super::raw::ngx_grpc_conn_set_data(c, Rc::as_ptr(&state) as *mut c_void);
    super::raw::ngx_grpc_ssl_set_handshake_handler(c, on_handshake_wait_event);
  }
  HandshakeWaitFuture { c: Some(c), state }
}

pub struct HandshakeWaitFuture {
  c: Option<*mut ngx_connection_t>,
  state: Rc<HandshakeWaitState>,
}

impl Future for HandshakeWaitFuture {
  type Output = Result<Connection, ngx_int_t>;

  fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
    if let Some(ok) = self.state.result.get() {
      let c = self
        .c
        .take()
        .expect("HandshakeWaitFuture polled after completion");
      return Poll::Ready(if ok {
        Ok(Connection::from_raw(c))
      } else {
        unsafe { super::raw::ngx_grpc_close_connection(c) };
        Err(super::NGX_ERROR)
      });
    }
    *self.state.waker.borrow_mut() = Some(cx.waker().clone());
    Poll::Pending
  }
}

impl Drop for HandshakeWaitFuture {
  fn drop(&mut self) {
    if let Some(c) = self.c.take() {
      unsafe { super::raw::ngx_grpc_close_connection(c) };
    }
  }
}

extern "C" fn on_handshake_wait_event(c: *mut ngx_connection_t) {
  super::assert_worker_thread();
  let state = unsafe { &*(super::raw::ngx_grpc_conn_get_data(c) as *const HandshakeWaitState) };
  let ok = handshaked(c);
  state.result.set(Some(ok));
  // Bind before acting: the `RefMut` temporary would otherwise outlive
  // this if-let body and panic on re-entrant borrow.
  let woken = state.waker.borrow_mut().take();
  if let Some(w) = woken {
    w.wake();
    state.reactor.drive_once(runtime::DRIVE_BUDGET);
  }
}
