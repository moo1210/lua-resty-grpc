use std::{
  cell::RefCell,
  collections::VecDeque,
  pin::Pin,
  rc::Rc,
  task::{Context, Poll, RawWaker, RawWakerVTable, Waker},
};

/// Budget every `drive_once` call site passes. `drive_once` already
/// returns (via its `None => break` on an empty ready queue) as soon as
/// there's nothing left to poll, so this is a ceiling for a pathological
/// self-waking loop, not a throttle real workloads ever hit: even a
/// connection at its stream cap (`DEFAULT_MAX_CONCURRENT_STREAMS` in
/// pool.rs, 100 by default) waking every one of its calls at once is two
/// orders of magnitude under this. Callers never recheck `drive_once`'s
/// return value to call it again, so this budget must stay comfortably
/// above the largest burst of tasks any single wake can produce, or the
/// excess is left stranded in the queue until an unrelated event drains
/// it.
pub const DRIVE_BUDGET: usize = 10_000;

struct Task {
  fut: Pin<Box<dyn Future<Output = ()> + 'static>>,
  queued: bool,
}
#[derive(Default)]
struct ReactorInner {
  tasks: Vec<Option<Task>>,
  ready: VecDeque<usize>,
  /// Slots left `None` by a task that ran to completion, reused by the
  /// next `spawn` instead of growing `tasks` forever. A long-lived
  /// worker driving thousands of calls over its lifetime would
  /// otherwise accumulate a `tasks` vec that never shrinks.
  free: Vec<usize>,
}

/// Lightweight single-step task executor
#[derive(Default, Clone)]
pub struct Reactor {
  inner: Rc<RefCell<ReactorInner>>,
}
impl Reactor {
  pub fn new() -> Self {
    Self {
      inner: Rc::new(RefCell::new(ReactorInner::default())),
    }
  }

  /// Spawn a task
  pub fn spawn<F>(&self, fut: F)
  where
    F: Future<Output = ()> + 'static,
  {
    let mut inner = self.inner.borrow_mut();

    let id = match inner.free.pop() {
      Some(id) => id,
      None => {
        inner.tasks.push(None);
        inner.tasks.len() - 1
      }
    };
    inner.tasks[id] = Some(Task {
      fut: Box::pin(fut),
      queued: false,
    });
    inner.ready.push_back(id);
    log::trace!(
      "Reactor.spawn -> task {} queued (ready_len={})",
      id,
      inner.ready.len()
    );
  }

  /// Construct a waker to wake up suspended tasks
  fn make_waker(id: usize, inner: Rc<RefCell<ReactorInner>>) -> Waker {
    #[derive(Clone)]
    struct Wd {
      id: usize,
      inner: Rc<RefCell<ReactorInner>>,
    }
    unsafe fn clone(data: *const ()) -> RawWaker {
      let wd = unsafe { &*(data as *const Wd) };
      let boxed = Box::new(wd.clone());
      RawWaker::new(Box::into_raw(boxed) as *const (), &VTABLE)
    }
    unsafe fn wake(data: *const ()) {
      let wd: Box<Wd> = unsafe { Box::from_raw(data as *mut Wd) };
      let mut inner = wd.inner.borrow_mut();
      if let Some(Some(task)) = inner.tasks.get_mut(wd.id)
        && !task.queued
      {
        task.queued = true;
        inner.ready.push_back(wd.id);
      }
    }
    unsafe fn wake_by_ref(data: *const ()) {
      let wd = unsafe { &*(data as *const Wd) };
      let mut inner = wd.inner.borrow_mut();
      if let Some(Some(task)) = inner.tasks.get_mut(wd.id)
        && !task.queued
      {
        task.queued = true;
        inner.ready.push_back(wd.id);
      }
    }
    unsafe fn drop(data: *const ()) {
      let _ = unsafe { Box::from_raw(data as *mut Wd) };
    }
    static VTABLE: RawWakerVTable = RawWakerVTable::new(clone, wake, wake_by_ref, drop);
    let wd = Box::new(Wd { id, inner });
    let raw = RawWaker::new(Box::into_raw(wd) as *const (), &VTABLE);
    unsafe { Waker::from_raw(raw) }
  }

  /// Execute the specified number of poll operations
  pub fn drive_once(&self, max: usize) -> bool {
    let mut polled = 0usize;
    loop {
      if polled >= max {
        break;
      }
      let (id, mut task) = {
        let mut inner = self.inner.borrow_mut();
        match inner.ready.pop_front() {
          Some(id) => {
            log::trace!("Reactor.drive_once -> poll task {}", id);
            let mut task = inner.tasks[id].take().expect("task should exist");
            task.queued = false;
            (id, task)
          }
          None => break,
        }
      };
      let waker = Self::make_waker(id, self.inner.clone());
      let mut cx = Context::from_waker(&waker);
      match task.fut.as_mut().poll(&mut cx) {
        Poll::Ready(()) => {
          let mut inner = self.inner.borrow_mut();
          inner.free.push(id);
        }
        Poll::Pending => {
          let mut inner = self.inner.borrow_mut();
          inner.tasks[id] = Some(task);
        }
      }
      polled += 1;
    }
    let inner = self.inner.borrow();
    !inner.ready.is_empty()
  }
}

thread_local! {
  /// One `Reactor` per worker, not one per client or connection.
  /// nginx's own event loop drives everything, so every connection's
  /// event handlers and every one-shot timer in this crate wake into
  /// the same executor regardless of which call or pool entry they
  /// belong to. A pooled h2 connection shared across multiple calls
  /// has no single client to belong to anyway.
  static WORKER_REACTOR: Reactor = Reactor::new();
}

pub fn worker_reactor() -> Reactor {
  WORKER_REACTOR.with(|r| r.clone())
}
