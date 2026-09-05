//! `AsyncRead`/`AsyncWrite` directly over an `ngx_connection_t`.
//!
//! Ownership model: [`NgxIo`] owns the connection and closes it on
//! `Drop`. It is consumed by `h2::client::handshake` and lives inside
//! the spawned h2 connection-driver task from that point on (see
//! `pool.rs`), so nothing outside that task can reach it directly.
//! [`NgxIo::handle`] and [`NgxIoHandle`] give the pool a way to
//! force-close it anyway, for example on worker shutdown, without
//! going through the h2 protocol.
//!
//! Self-driving: `Waker::wake()` only re-queues a task, it never polls
//! it. So each event handler here calls `reactor.drive_once()` itself
//! after waking the task blocked on this connection; each `NgxIo`
//! carries a `Reactor` handle for that purpose.

use std::{
  cell::{Cell, RefCell},
  io,
  os::raw::c_void,
  panic::{AssertUnwindSafe, catch_unwind},
  pin::Pin,
  rc::Rc,
  task::{Context, Poll, Waker},
};

use tokio::io::ReadBuf;

use crate::{
  ngx::{self, NGX_AGAIN, conn::Connection, ngx_connection_t, ngx_event_t},
  runtime::{self, Reactor},
};

struct Inner {
  c: *mut ngx_connection_t,
  read_waker: RefCell<Option<Waker>>,
  write_waker: RefCell<Option<Waker>>,
  /// Set once `c` has been closed (EOF, error, or `poll_shutdown`) so it
  /// is never touched again. Events can still be in flight against a
  /// connection this has logically given up on until the last Rc drops.
  closed: Cell<bool>,
  reactor: Reactor,
}

pub struct NgxIo {
  inner: Rc<Inner>,
}

impl NgxIo {
  /// Takes ownership of `conn` and wires up the read/write event
  /// handlers. `reactor` is redriven from those handlers, and must be
  /// the same reactor the task using this `NgxIo` is spawned on.
  pub fn new(conn: Connection, reactor: Reactor) -> Self {
    ngx::assert_worker_thread();

    let c = conn.into_raw();
    let inner = Rc::new(Inner {
      c,
      read_waker: RefCell::new(None),
      write_waker: RefCell::new(None),
      closed: Cell::new(false),
      reactor,
    });

    // Non-owning: `c->data` holds no refcount of its own. It is valid
    // exactly as long as `c` itself is open, and it is never
    // dereferenced after `close()` runs, which also clears the
    // handlers.
    unsafe {
      ngx::raw::ngx_grpc_conn_set_data(c, Rc::as_ptr(&inner) as *mut c_void);
      ngx::raw::ngx_grpc_conn_set_handlers(c, on_read_event, on_write_event);
    }

    Self { inner }
  }

  fn close(&self) {
    if self.inner.closed.replace(true) {
      return;
    }
    unsafe {
      ngx::raw::ngx_grpc_close_connection(self.inner.c);
    }
  }

  /// A cheap, cloneable handle that can force this connection closed
  /// from outside the task driving it. Used by `pool::on_worker_exit`
  /// to close idle connections immediately, rather than waiting for
  /// whatever is driving them to notice on its own.
  pub fn handle(&self) -> NgxIoHandle {
    NgxIoHandle(self.inner.clone())
  }
}

#[derive(Clone)]
pub struct NgxIoHandle(Rc<Inner>);

impl NgxIoHandle {
  /// Closes the underlying nginx connection right now, regardless of
  /// what the owning `NgxIo` and whatever future is polling it are
  /// doing. Safe to call more than once. Does not itself wake or
  /// re-poll that future: at the call site in `pool.rs`, worker shutdown
  /// only needs nginx's own accounting of open connections to drop,
  /// which this alone accomplishes.
  pub fn force_close(&self) {
    if self.0.closed.replace(true) {
      return;
    }
    unsafe {
      ngx::raw::ngx_grpc_close_connection(self.0.c);
    }
  }
}

impl Drop for NgxIo {
  fn drop(&mut self) {
    self.close();
  }
}

impl tokio::io::AsyncRead for NgxIo {
  fn poll_read(
    self: Pin<&mut Self>,
    cx: &mut Context<'_>,
    buf: &mut ReadBuf<'_>,
  ) -> Poll<io::Result<()>> {
    let inner = &self.inner;
    if inner.closed.get() {
      return Poll::Ready(Ok(()));
    }

    let dst = buf.initialize_unfilled();
    if dst.is_empty() {
      return Poll::Ready(Ok(()));
    }

    let n = unsafe { ngx::raw::ngx_grpc_conn_recv(inner.c, dst.as_mut_ptr(), dst.len()) };

    if n > 0 {
      buf.advance(n as usize);
      return Poll::Ready(Ok(()));
    }
    if n == 0 {
      // EOF. tokio's AsyncRead contract for this is Ready(Ok(())) with
      // nothing advanced.
      return Poll::Ready(Ok(()));
    }
    if n == NGX_AGAIN {
      *inner.read_waker.borrow_mut() = Some(cx.waker().clone());
      let rev = unsafe { ngx::raw::ngx_grpc_conn_read_event(inner.c) };
      unsafe { ngx::raw::ngx_grpc_handle_read_event(rev, 0) };
      return Poll::Pending;
    }
    // NGX_ERROR, or anything else unexpected. nginx has already logged
    // the specifics (ngx_recv/ngx_ssl_recv do their own ngx_log_error);
    // the errno is not available here without another shim round-trip.
    Poll::Ready(Err(io::Error::other("ngx_grpc: connection read error")))
  }
}

impl tokio::io::AsyncWrite for NgxIo {
  fn poll_write(
    self: Pin<&mut Self>,
    cx: &mut Context<'_>,
    data: &[u8],
  ) -> Poll<io::Result<usize>> {
    let inner = &self.inner;
    if inner.closed.get() {
      return Poll::Ready(Err(io::Error::other("ngx_grpc: connection closed")));
    }
    if data.is_empty() {
      return Poll::Ready(Ok(0));
    }

    let n = unsafe { ngx::raw::ngx_grpc_conn_send(inner.c, data.as_ptr(), data.len()) };

    if n > 0 {
      return Poll::Ready(Ok(n as usize));
    }
    if n == NGX_AGAIN {
      *inner.write_waker.borrow_mut() = Some(cx.waker().clone());
      let wev = unsafe { ngx::raw::ngx_grpc_conn_write_event(inner.c) };
      unsafe { ngx::raw::ngx_grpc_handle_write_event(wev, 0) };
      return Poll::Pending;
    }
    Poll::Ready(Err(io::Error::other("ngx_grpc: connection write error")))
  }

  fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
    // c->send and c->recv issue the real write(2)/writev(2), or the TLS
    // equivalent, synchronously inside poll_write itself. There is no
    // separate userspace buffer to flush.
    Poll::Ready(Ok(()))
  }

  fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
    self.close();
    Poll::Ready(Ok(()))
  }
}

extern "C" fn on_read_event(ev: *mut ngx_event_t) {
  ngx::assert_worker_thread();
  // Panics must never unwind across an FFI boundary back into C. nginx
  // itself would be left in an undefined state.
  if catch_unwind(AssertUnwindSafe(|| wake(ev, WhichWaker::Read))).is_err() {
    log::error!("ngx_grpc: panic in NgxIo read event handler, caught at the FFI boundary");
  }
}

extern "C" fn on_write_event(ev: *mut ngx_event_t) {
  ngx::assert_worker_thread();
  if catch_unwind(AssertUnwindSafe(|| wake(ev, WhichWaker::Write))).is_err() {
    log::error!("ngx_grpc: panic in NgxIo write event handler, caught at the FFI boundary");
  }
}

enum WhichWaker {
  Read,
  Write,
}

fn wake(ev: *mut ngx_event_t, which: WhichWaker) {
  let c = ngx::event_connection(ev);
  // Non-owning borrow, see NgxIo::new. Valid because
  // ngx_grpc_close_connection tears the connection down entirely,
  // clearing the handlers, before the Inner it points to can be
  // dropped.
  let inner = unsafe { &*(ngx::raw::ngx_grpc_conn_get_data(c) as *const Inner) };
  if inner.closed.get() {
    return;
  }
  let slot = match which {
    WhichWaker::Read => &inner.read_waker,
    WhichWaker::Write => &inner.write_waker,
  };
  // Not `if let Some(w) = slot.borrow_mut().take() { ... }`. Under
  // Rust's temporary-lifetime-extension rules, the `RefMut` temporary
  // from `borrow_mut()` lives for the whole if-let body, and
  // `drive_once` below re-enters this same connection's
  // `poll_read`/`poll_write`, which tries to borrow this very `RefCell`
  // again and would panic with "already borrowed". Binding the
  // extracted value first ends the borrow immediately.
  let woken = slot.borrow_mut().take();
  if let Some(w) = woken {
    w.wake();
    // The Waker (see runtime::Reactor::make_waker) only re-queues the
    // task, so it must be polled explicitly here.
    inner.reactor.drive_once(runtime::DRIVE_BUDGET);
  }
}
