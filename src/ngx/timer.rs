//! A one-shot async delay backed by an nginx timer, for anything that
//! needs to wait without touching tokio's time machinery.
//!
//! Tokio's own timers (`tokio::time::sleep` and friends, and h2's
//! built-in `keep_alive_interval`/`keep_alive_timeout` which are
//! implemented on top of them) require a live `tokio::runtime` with its
//! time driver running. This crate has no `rt` feature and spawns no
//! threads; it is driven exclusively by nginx's own event loop on a
//! single thread (see `ngx::assert_worker_thread`), so calling tokio's
//! timers here would panic the first time they are polled. Anywhere
//! this crate needs a delay, PING keepalive and reconnect backoff
//! included, it goes through this instead.

use std::{
  cell::{Cell, RefCell},
  future::Future,
  os::raw::c_void,
  pin::Pin,
  rc::Rc,
  task::{Context, Poll, Waker},
};

use crate::runtime::{self, Reactor};

use super::{ngx_event_t, ngx_msec_t};

struct DelayState {
  waker: RefCell<Option<Waker>>,
  fired: Cell<bool>,
  reactor: Reactor,
}

/// Resolves once `ms` milliseconds have passed. Dropping it before then
/// cancels the underlying nginx timer.
pub struct Delay {
  ev: *mut ngx_event_t,
  state: Rc<DelayState>,
}

impl Delay {
  pub fn new(ms: ngx_msec_t, reactor: Reactor) -> Self {
    super::assert_worker_thread();

    let pool = unsafe { super::raw::ngx_grpc_cycle_pool() };
    let ev = unsafe { super::raw::ngx_grpc_alloc_event(pool) };
    let state = Rc::new(DelayState {
      waker: RefCell::new(None),
      fired: Cell::new(false),
      reactor,
    });

    unsafe {
      super::raw::ngx_grpc_event_init(
        ev,
        on_timer,
        super::cycle_log(),
        Rc::as_ptr(&state) as *mut c_void,
      );
      super::raw::ngx_grpc_add_timer(ev, ms);
    }

    Self { ev, state }
  }
}

impl Future for Delay {
  type Output = ();

  fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
    if self.state.fired.get() {
      return Poll::Ready(());
    }
    *self.state.waker.borrow_mut() = Some(cx.waker().clone());
    Poll::Pending
  }
}

impl Drop for Delay {
  fn drop(&mut self) {
    if !self.state.fired.get() {
      unsafe { super::raw::ngx_grpc_del_timer(self.ev) };
    }
    // The event itself is pool-allocated (ngx_grpc_alloc_event) and not
    // individually freed. It is reclaimed in bulk when the owning pool
    // (the worker-cycle pool) is destroyed, the same idiom nginx's own
    // long-lived one-shot timers use.
  }
}

extern "C" fn on_timer(ev: *mut ngx_event_t) {
  super::assert_worker_thread();
  let state = unsafe { &*(super::raw::ngx_grpc_ev_data(ev) as *const DelayState) };
  state.fired.set(true);
  // Bind before acting: the `RefMut` temporary would otherwise outlive
  // this if-let body and panic on re-entrant borrow.
  let woken = state.waker.borrow_mut().take();
  if let Some(w) = woken {
    w.wake();
    // Same reasoning as NgxIo: the Waker only re-queues the task, we
    // still have to actually poll it.
    state.reactor.drive_once(runtime::DRIVE_BUDGET);
  }
}
