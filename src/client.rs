//! Call-state machinery sitting between the FFI surface (`lib.rs`) and
//! the worker-level connection pool (`pool.rs`).
//!
//! One `GrpcClient` per `grpc_client_new()`. It is just an
//! `EndpointKey`; the actual h2 connections live in the pool and are
//! shared across every client with the same endpoint. One `GrpcCall`
//! per RPC, holding the queue of decoded response messages Lua drains
//! via `grpc_call_poll`, and the semaphore Rust posts to wake the Lua
//! coroutine waiting on it. See `ngx::lua_sema_post`.

use std::{
  cell::{Cell, RefCell},
  collections::VecDeque,
  os::raw::c_void,
  rc::Rc,
  str::FromStr,
};

use bytes::Bytes;
use http::uri::PathAndQuery;
use tokio::sync::mpsc;
use tokio_stream::{StreamExt, wrappers::ReceiverStream};

use crate::{
  codec::BytesCodec,
  ngx,
  pool::{self, EndpointKey, H2Conn},
  runtime::{self, worker_reactor},
  transport::channel::Channel,
};

pub struct GrpcClient {
  pub key: EndpointKey,
}

impl GrpcClient {
  pub fn new(key: EndpointKey) -> Self {
    Self { key }
  }
}

/// Enough of a gRPC status for the FFI surface to hand back to Lua.
/// `code` follows https://grpc.io/docs/guides/status-codes/ (0 = OK).
#[derive(Clone)]
pub struct CallStatus {
  pub code: i32,
  pub message: String,
}

impl CallStatus {
  fn ok() -> Self {
    Self {
      code: tonic::Code::Ok as i32,
      message: String::new(),
    }
  }

  fn from_tonic(status: &tonic::Status) -> Self {
    Self {
      code: status.code() as i32,
      message: status.message().to_string(),
    }
  }

  /// For failures that never reached tonic at all (pool, connect, TLS,
  /// or URI errors), reported as UNAVAILABLE, matching how gRPC clients
  /// conventionally surface transport-level failures.
  fn transport_error(message: impl Into<String>) -> Self {
    Self {
      code: tonic::Code::Unavailable as i32,
      message: message.into(),
    }
  }
}

enum CallEvent {
  Message(Bytes),
  Done(CallStatus),
}

pub struct CallState {
  events: RefCell<VecDeque<CallEvent>>,
  sem: *mut c_void,
  sem_stream_subsys: bool,
  /// Cooperative cancellation: `grpc_call_cancel`/`grpc_call_free` set
  /// this (see `ngx.on_abort` on the Lua side, for when the client
  /// disconnects mid-call). The streaming drivers check it between
  /// messages, not mid-`.await`, so a single already-started
  /// `unary()`/`client_streaming()` call always runs to its natural
  /// completion. Once noticed, the driver stops pushing further events,
  /// since nothing is listening any more, and lets the stream wind down.
  cancelled: Cell<bool>,
  /// Keeps the most recently returned `grpc_call_poll` payload alive
  /// across the FFI boundary. Lua must copy it out (`ffi.string`)
  /// before polling again, same contract as any FFI call returning a
  /// pointer into memory the callee still owns.
  last_message: RefCell<Option<Bytes>>,
  last_status_message: RefCell<Option<Vec<u8>>>,
}

impl CallState {
  fn new(sem: *mut c_void, sem_stream_subsys: bool) -> Rc<Self> {
    Rc::new(Self {
      events: RefCell::new(VecDeque::new()),
      sem,
      sem_stream_subsys,
      cancelled: Cell::new(false),
      last_message: RefCell::new(None),
      last_status_message: RefCell::new(None),
    })
  }

  fn push(self: &Rc<Self>, ev: CallEvent) {
    self.events.borrow_mut().push_back(ev);
    ngx::lua_sema_post(self.sem, self.sem_stream_subsys, 1);
  }

  /// Drains one event, if any, for `grpc_call_poll`. Returns `None` when
  /// there is nothing new yet (Lua should keep waiting on the
  /// semaphore).
  pub fn poll(&self) -> Option<PollResult> {
    match self.events.borrow_mut().pop_front() {
      Some(CallEvent::Message(b)) => {
        *self.last_message.borrow_mut() = Some(b);
        Some(PollResult::Message)
      }
      Some(CallEvent::Done(status)) => {
        *self.last_status_message.borrow_mut() = Some(status.message.clone().into_bytes());
        Some(PollResult::Done(status.code))
      }
      None => None,
    }
  }

  /// Valid only immediately after `poll()` returned `Message`.
  pub fn last_message(&self) -> std::cell::Ref<'_, Option<Bytes>> {
    self.last_message.borrow()
  }

  /// Valid only immediately after `poll()` returned `Done`.
  pub fn last_status_message(&self) -> std::cell::Ref<'_, Option<Vec<u8>>> {
    self.last_status_message.borrow()
  }

  pub fn cancel(&self) {
    self.cancelled.set(true);
  }
}

pub enum PollResult {
  Message,
  /// gRPC status code; 0 = OK. The message text itself is fetched
  /// separately via `CallState::last_status_message`.
  Done(i32),
}

/// One in-flight (or finished-but-not-yet-fully-polled) RPC, as handed
/// to Lua via `grpc_call_start`.
pub struct GrpcCall {
  pub state: Rc<CallState>,
  /// Present only for client-streaming/bidi calls. `grpc_call_send`
  /// pushes into this; dropping it via `grpc_call_done` signals EOF to
  /// the request stream. `RefCell` so `grpc_call_done` doesn't need a
  /// mutable handle-map borrow.
  pub outbound: RefCell<Option<mpsc::Sender<Bytes>>>,
}

impl GrpcCall {
  pub fn new(state: Rc<CallState>, outbound: Option<mpsc::Sender<Bytes>>) -> Self {
    Self {
      state,
      outbound: RefCell::new(outbound),
    }
  }
}

impl Drop for GrpcCall {
  fn drop(&mut self) {
    // Tells the spawned driver task, if it is still running, to stop at
    // its next checkpoint; normal completion already stopped pushing
    // events by itself. The h2 stream-count reservation is released by
    // that task itself once it notices, via `H2Conn::end_stream`.
    self.state.cancel();
  }
}

fn make_grpc(h2c: &H2Conn, key: &EndpointKey) -> tonic::client::Grpc<Channel> {
  let scheme = if key.tls { "https" } else { "http" };
  let sender = Rc::new(RefCell::new(Some(h2c.sender())));
  tonic::client::Grpc::new(Channel::new(
    sender,
    key.addr.clone(),
    scheme.to_string(),
    worker_reactor(),
  ))
}

fn parse_path(path: &str, state: &Rc<CallState>) -> Option<PathAndQuery> {
  match PathAndQuery::from_str(path) {
    Ok(p) => Some(p),
    Err(e) => {
      state.push(CallEvent::Done(CallStatus::transport_error(format!(
        "invalid path {path:?}: {e}"
      ))));
      None
    }
  }
}

/// `tonic::Status` wraps the underlying h2/hyper error in `source()`.
/// Downcasting recovers it, so the pool can tell a GOAWAY apart from an
/// ordinary application-level error status.
fn report_send_error_if_h2(h2c: &H2Conn, status: &tonic::Status) {
  if let Some(source) = std::error::Error::source(status)
    && let Some(h2_err) = source.downcast_ref::<h2::Error>()
  {
    h2c.report_send_error(h2_err);
  }
}

/// Starts a unary call. `sem`/`sem_stream_subsys` identify the
/// `ngx.semaphore` Rust posts to on every new event.
pub fn start_unary(
  client: &GrpcClient,
  path: String,
  body: Bytes,
  sem: *mut c_void,
  sem_stream_subsys: bool,
) -> Rc<CallState> {
  let state = CallState::new(sem, sem_stream_subsys);
  spawn_unary_like(client.key.clone(), path, body, state.clone(), false);
  state
}

pub fn start_server_streaming(
  client: &GrpcClient,
  path: String,
  body: Bytes,
  sem: *mut c_void,
  sem_stream_subsys: bool,
) -> Rc<CallState> {
  let state = CallState::new(sem, sem_stream_subsys);
  spawn_unary_like(client.key.clone(), path, body, state.clone(), true);
  state
}

fn spawn_unary_like(
  key: EndpointKey,
  path: String,
  body: Bytes,
  state: Rc<CallState>,
  streaming: bool,
) {
  let reactor = worker_reactor();
  reactor.spawn(async move {
    let h2c = match pool::get_connection(key.clone(), worker_reactor()).await {
      Ok(c) => c,
      Err(e) => {
        state.push(CallEvent::Done(CallStatus::transport_error(format!(
          "{e:?}"
        ))));
        return;
      }
    };
    h2c.begin_stream();

    let Some(path) = parse_path(&path, &state) else {
      h2c.end_stream();
      return;
    };

    let mut grpc = make_grpc(&h2c, &key);

    if streaming {
      match grpc
        .server_streaming(tonic::Request::new(body), path, BytesCodec)
        .await
      {
        Ok(resp) => {
          let mut stream = resp.into_inner();
          loop {
            if state.cancelled.get() {
              break;
            }
            match stream.next().await {
              Some(Ok(msg)) => state.push(CallEvent::Message(msg)),
              Some(Err(status)) => {
                report_send_error_if_h2(&h2c, &status);
                state.push(CallEvent::Done(CallStatus::from_tonic(&status)));
                break;
              }
              None => {
                state.push(CallEvent::Done(CallStatus::ok()));
                break;
              }
            }
          }
        }
        Err(status) => {
          report_send_error_if_h2(&h2c, &status);
          state.push(CallEvent::Done(CallStatus::from_tonic(&status)));
        }
      }
    } else {
      match grpc
        .unary(tonic::Request::new(body), path, BytesCodec)
        .await
      {
        Ok(resp) => {
          state.push(CallEvent::Message(resp.into_inner()));
          state.push(CallEvent::Done(CallStatus::ok()));
        }
        Err(status) => {
          report_send_error_if_h2(&h2c, &status);
          state.push(CallEvent::Done(CallStatus::from_tonic(&status)));
        }
      }
    }

    h2c.end_stream();
  });
  reactor.drive_once(runtime::DRIVE_BUDGET);
}

/// Starts a client-streaming call. Returns the call state plus the sink
/// `grpc_call_send`/`grpc_call_done` write into.
pub fn start_client_streaming(
  client: &GrpcClient,
  path: String,
  sem: *mut c_void,
  sem_stream_subsys: bool,
) -> (Rc<CallState>, mpsc::Sender<Bytes>) {
  let state = CallState::new(sem, sem_stream_subsys);
  let (tx, rx) = mpsc::channel::<Bytes>(32);

  let key = client.key.clone();
  let state_task = state.clone();
  let reactor = worker_reactor();
  reactor.spawn(async move {
    let h2c = match pool::get_connection(key.clone(), worker_reactor()).await {
      Ok(c) => c,
      Err(e) => {
        state_task.push(CallEvent::Done(CallStatus::transport_error(format!(
          "{e:?}"
        ))));
        return;
      }
    };
    h2c.begin_stream();

    let Some(path) = parse_path(&path, &state_task) else {
      h2c.end_stream();
      return;
    };

    let mut grpc = make_grpc(&h2c, &key);
    let req = tonic::Request::new(ReceiverStream::new(rx));

    match grpc.client_streaming(req, path, BytesCodec).await {
      Ok(resp) => {
        state_task.push(CallEvent::Message(resp.into_inner()));
        state_task.push(CallEvent::Done(CallStatus::ok()));
      }
      Err(status) => {
        report_send_error_if_h2(&h2c, &status);
        state_task.push(CallEvent::Done(CallStatus::from_tonic(&status)));
      }
    }

    h2c.end_stream();
  });
  reactor.drive_once(runtime::DRIVE_BUDGET);

  (state, tx)
}

/// Starts a bidirectional-streaming call.
pub fn start_streaming(
  client: &GrpcClient,
  path: String,
  sem: *mut c_void,
  sem_stream_subsys: bool,
) -> (Rc<CallState>, mpsc::Sender<Bytes>) {
  let state = CallState::new(sem, sem_stream_subsys);
  let (tx, rx) = mpsc::channel::<Bytes>(32);

  let key = client.key.clone();
  let state_task = state.clone();
  let reactor = worker_reactor();
  reactor.spawn(async move {
    let h2c = match pool::get_connection(key.clone(), worker_reactor()).await {
      Ok(c) => c,
      Err(e) => {
        state_task.push(CallEvent::Done(CallStatus::transport_error(format!(
          "{e:?}"
        ))));
        return;
      }
    };
    h2c.begin_stream();

    let Some(path) = parse_path(&path, &state_task) else {
      h2c.end_stream();
      return;
    };

    let mut grpc = make_grpc(&h2c, &key);
    let req = tonic::Request::new(ReceiverStream::new(rx));

    match grpc.streaming(req, path, BytesCodec).await {
      Ok(resp) => {
        let mut stream = resp.into_inner();
        loop {
          if state_task.cancelled.get() {
            break;
          }
          match stream.next().await {
            Some(Ok(msg)) => state_task.push(CallEvent::Message(msg)),
            Some(Err(status)) => {
              report_send_error_if_h2(&h2c, &status);
              state_task.push(CallEvent::Done(CallStatus::from_tonic(&status)));
              break;
            }
            None => {
              state_task.push(CallEvent::Done(CallStatus::ok()));
              break;
            }
          }
        }
      }
      Err(status) => {
        report_send_error_if_h2(&h2c, &status);
        state_task.push(CallEvent::Done(CallStatus::from_tonic(&status)));
      }
    }

    h2c.end_stream();
  });
  reactor.drive_once(runtime::DRIVE_BUDGET);

  (state, tx)
}
