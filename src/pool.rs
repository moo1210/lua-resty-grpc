//! Worker-level h2 connection pool.
//!
//! Scope of this implementation:
//! - keyed by `(addr, tls, sni)`, thread_local. There is one pool per
//!   worker; fds and epoll registrations are per-process, so no
//!   cross-worker sharing is possible.
//! - multiple connections per endpoint, capped at
//!   [`MAX_CONNS_PER_ENDPOINT`], opened as existing ones fill up.
//! - per-connection capacity uses the peer's real, acknowledged
//!   `SETTINGS_MAX_CONCURRENT_STREAMS`
//!   (`h2::client::Connection::max_concurrent_send_streams`, read once
//!   right after handshake), falling back to
//!   [`DEFAULT_MAX_CONCURRENT_STREAMS`] only when that comes back as 0,
//!   meaning the peer's SETTINGS has not arrived yet.
//! - GOAWAY handling: detected via `h2::Error::is_go_away()` on a failed
//!   `send_request`. The connection is then marked draining and never
//!   handed out again; in-flight streams on it are unaffected.
//! - reconnect backoff: exponential with a ceiling, per endpoint.
//! - PING keepalive: manual, via [`crate::ngx::timer::Delay`]. h2's
//!   built-in `keep_alive_interval`/`keep_alive_timeout` are not used;
//!   they run on tokio's own timer, which this crate never starts (see
//!   the single-threaded, nginx-driven-only constraint asserted via
//!   `ngx::assert_worker_thread`).
//!
//! What this does not do (documented gaps, not silent ones):
//! - waiting for capacity when every connection is full polls every
//!   [`CAPACITY_POLL_MS`] rather than being woken precisely when a
//!   stream completes. Correct, not maximally efficient.
//! - TLS session resumption is not wired up. It needs `pc.set_session`
//!   and `pc.save_session`, not just `ngx_ssl_client_session_cache`.
//! - idle connections are not marked `ngx_reusable_connection`.
//!   `NgxIo`, and the raw connection pointer with it, is consumed by
//!   `h2::client::handshake` and lives inside the spawned connection-
//!   driving task from that point on, with no handle left in `H2Conn`
//!   to toggle it from `begin_stream`/`end_stream`. Threading that
//!   through is mechanical (give `NgxIo` a `mark_reusable` passthrough
//!   and keep a reference to it in `H2Conn`) but is not done.

use std::{
  cell::{Cell, RefCell},
  collections::HashMap,
  rc::Rc,
};

use bytes::Bytes;

use crate::{
  ngx::{
    conn::{ConnectError, ConnectOutcome, Connection},
    ssl::{self, SslCtx},
    timer::Delay,
  },
  runtime::Reactor,
  transport::ngx_io::NgxIo,
};

const MAX_CONNS_PER_ENDPOINT: usize = 2;
/// See the module doc comment: not the real negotiated value, just a
/// conservative default matching common server configs (grpc-go's
/// default is 100; Envoy's is 128).
const DEFAULT_MAX_CONCURRENT_STREAMS: u32 = 100;
const PING_INTERVAL_MS: usize = 30_000;
/// Granularity the PING loop re-checks `ngx::worker_exiting()` at.
/// Bounds how long graceful shutdown can be stalled by this task.
const PING_CHECK_INTERVAL_MS: usize = 1_000;
const CAPACITY_POLL_MS: usize = 20;
const RECONNECT_BACKOFF_INITIAL_MS: usize = 100;
const RECONNECT_BACKOFF_MAX_MS: usize = 10_000;

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub struct EndpointKey {
  pub addr: String,
  pub tls: bool,
  pub sni: Option<String>,
}

pub struct H2Conn {
  send_request: RefCell<h2::client::SendRequest<Bytes>>,
  active_streams: Cell<u32>,
  /// The peer's real, acknowledged `SETTINGS_MAX_CONCURRENT_STREAMS`,
  /// read once right after handshake via
  /// `h2::client::Connection::max_concurrent_send_streams`. Not an
  /// approximation, unlike [`DEFAULT_MAX_CONCURRENT_STREAMS`], which
  /// only backstops the case where the peer's SETTINGS has not arrived
  /// yet at that point.
  max_streams: Cell<u32>,
  draining: Cell<bool>,
  broken: Cell<bool>,
  /// For `force_close` at worker shutdown. See `on_worker_exit`.
  io_handle: crate::transport::ngx_io::NgxIoHandle,
}

impl H2Conn {
  fn has_capacity(&self) -> bool {
    !self.draining.get() && !self.broken.get() && self.active_streams.get() < self.max_streams.get()
  }

  fn is_idle(&self) -> bool {
    self.active_streams.get() == 0
  }

  /// Closes the underlying connection immediately, regardless of the h2
  /// connection driver task's own state. Only safe to call on an idle
  /// connection. See [`H2Conn::is_idle`] and `on_worker_exit`.
  fn force_close(&self) {
    self.io_handle.force_close();
    self.broken.set(true);
  }

  pub fn sender(&self) -> h2::client::SendRequest<Bytes> {
    self.send_request.borrow().clone()
  }

  /// Call once per gRPC call started on this connection. Pair with
  /// [`H2Conn::end_stream`] when it finishes, whether by success, error,
  /// or cancellation.
  pub fn begin_stream(&self) {
    self.active_streams.set(self.active_streams.get() + 1);
  }

  pub fn end_stream(&self) {
    self
      .active_streams
      .set(self.active_streams.get().saturating_sub(1));
  }

  /// Report a `send_request`/`poll_ready` failure observed by a caller.
  /// If it was a GOAWAY, retires this connection from future selection.
  pub fn report_send_error(&self, err: &h2::Error) {
    if err.is_go_away() || err.is_io() {
      self.draining.set(true);
    }
  }

  pub fn is_broken(&self) -> bool {
    self.broken.get()
  }
}

/// Formatted via `Debug` into the transport-error status shown to the
/// caller (see `client.rs`'s `get_connection` error arms).
#[derive(Debug)]
#[allow(dead_code)]
pub enum PoolError {
  Connect(ConnectError),
  Tls(String),
  Handshake(String),
}

struct EndpointState {
  conns: RefCell<Vec<Rc<H2Conn>>>,
  backoff_ms: Cell<usize>,
  /// Connect attempts in flight but not yet in `conns`. Reserved the
  /// instant `get_connection` decides to open a new connection, and
  /// released via `ConnectingGuard` once it either lands in `conns` or
  /// fails. Without this, concurrent callers that all observe
  /// `conns.len() < MAX_CONNS_PER_ENDPOINT` in the same reactor tick,
  /// before any of their connects has landed, would each start their
  /// own connection and blow straight through the cap.
  connecting: Cell<usize>,
}

impl Default for EndpointState {
  fn default() -> Self {
    Self {
      conns: RefCell::new(Vec::new()),
      backoff_ms: Cell::new(0),
      connecting: Cell::new(0),
    }
  }
}

/// RAII decrement for `EndpointState::connecting`. Covers every early
/// return in `connect_new` (bad address, TLS failure, handshake
/// failure) as well as the success path, without needing a matching
/// decrement at each one.
struct ConnectingGuard(Rc<EndpointState>);

impl Drop for ConnectingGuard {
  fn drop(&mut self) {
    self
      .0
      .connecting
      .set(self.0.connecting.get().saturating_sub(1));
  }
}

thread_local! {
  static POOL: RefCell<HashMap<EndpointKey, Rc<EndpointState>>> = RefCell::new(HashMap::new());
  static SSL_CTXS: RefCell<HashMap<Option<String>, Rc<SslCtx>>> = RefCell::new(HashMap::new());
}

fn endpoint_state(key: &EndpointKey) -> Rc<EndpointState> {
  POOL.with(|p| {
    p.borrow_mut()
      .entry(key.clone())
      .or_insert_with(|| Rc::new(EndpointState::default()))
      .clone()
  })
}

fn find_with_capacity(key: &EndpointKey) -> Option<Rc<H2Conn>> {
  let state = POOL.with(|p| p.borrow().get(key).cloned())?;
  let mut conns = state.conns.borrow_mut();
  conns.retain(|c| !c.is_broken());
  conns.iter().find(|c| c.has_capacity()).cloned()
}

/// Gets a connection with spare stream capacity for `key`, connecting
/// (and, if every existing connection is full, opening an additional
/// one up to [`MAX_CONNS_PER_ENDPOINT`]) as needed.
pub async fn get_connection(key: EndpointKey, reactor: Reactor) -> Result<Rc<H2Conn>, PoolError> {
  loop {
    if let Some(c) = find_with_capacity(&key) {
      return Ok(c);
    }

    let state = endpoint_state(&key);
    // Claims the reservation and re-checks under it in one synchronous
    // step, with no `.await` between the check and the claim, so no
    // other caller can observe stale capacity in between.
    if state.conns.borrow().len() + state.connecting.get() < MAX_CONNS_PER_ENDPOINT {
      state.connecting.set(state.connecting.get() + 1);
      let guard = ConnectingGuard(state);
      return connect_new(&key, reactor, guard).await;
    }

    // Every connection is at capacity. Wait for one to free up. See the
    // module doc comment on why this polls instead of waiting on a
    // precise per-stream-completion signal.
    Delay::new(CAPACITY_POLL_MS, reactor.clone()).await;
  }
}

async fn connect_new(
  key: &EndpointKey,
  reactor: Reactor,
  _connecting_guard: ConnectingGuard,
) -> Result<Rc<H2Conn>, PoolError> {
  let state = endpoint_state(key);

  let backoff = state.backoff_ms.get();
  if backoff > 0 {
    Delay::new(backoff, reactor.clone()).await;
  }

  let bump_backoff = || {
    let next = if backoff == 0 {
      RECONNECT_BACKOFF_INITIAL_MS
    } else {
      (backoff * 2).min(RECONNECT_BACKOFF_MAX_MS)
    };
    state.backoff_ms.set(next);
  };

  let outcome = match Connection::connect(&key.addr) {
    Ok(o) => o,
    Err(e) => {
      bump_backoff();
      return Err(PoolError::Connect(e));
    }
  };

  let conn = match outcome {
    ConnectOutcome::Connected(c) => c,
    ConnectOutcome::Pending(c) => match c.wait_connected(reactor.clone()).await {
      Ok(c) => c,
      Err(e) => {
        bump_backoff();
        return Err(PoolError::Connect(e));
      }
    },
  };

  let conn = if key.tls {
    match do_tls_handshake(conn, key.sni.as_deref(), reactor.clone()).await {
      Ok(c) => c,
      Err(e) => {
        bump_backoff();
        return Err(e);
      }
    }
  } else {
    conn
  };

  let io = NgxIo::new(conn, reactor.clone());
  let io_handle = io.handle();

  match h2::client::handshake(io).await {
    Ok((send_request, mut connection)) => {
      state.backoff_ms.set(0);

      // Both come off the connection driver, not SendRequest, so grab
      // them before moving `connection` into the spawned drive task
      // below.
      let ping_pong = connection.ping_pong();
      let max_streams = connection.max_concurrent_send_streams() as u32;
      let max_streams = if max_streams == 0 {
        DEFAULT_MAX_CONCURRENT_STREAMS
      } else {
        max_streams
      };

      let h2c = Rc::new(H2Conn {
        send_request: RefCell::new(send_request),
        active_streams: Cell::new(0),
        max_streams: Cell::new(max_streams),
        draining: Cell::new(false),
        io_handle,
        broken: Cell::new(false),
      });

      let h2c_drive = h2c.clone();
      reactor.spawn(async move {
        if let Err(e) = connection.await {
          log::debug!("ngx_grpc: h2 connection error: {e:?}");
        }
        h2c_drive.broken.set(true);
      });

      if let Some(mut pp) = ping_pong {
        let h2c_ping = h2c.clone();
        let reactor_ping = reactor.clone();
        reactor.spawn(async move {
          'outer: loop {
            // Sleeps in short increments rather than one
            // `PING_INTERVAL_MS` chunk, checking `ngx::worker_exiting()`
            // each time. A `Delay` is a real nginx timer, and
            // `on_worker_exit` force-closing idle connections does not
            // by itself wake a task sleeping in an unrelated timer, so
            // one sitting at the full interval would stall graceful
            // worker shutdown by up to `PING_INTERVAL_MS`.
            let mut waited = 0usize;
            while waited < PING_INTERVAL_MS {
              if crate::ngx::worker_exiting() || h2c_ping.is_broken() {
                break 'outer;
              }
              let step = PING_CHECK_INTERVAL_MS.min(PING_INTERVAL_MS - waited);
              Delay::new(step, reactor_ping.clone()).await;
              waited += step;
            }
            if h2c_ping.is_broken() {
              break;
            }
            if pp.ping(h2::Ping::opaque()).await.is_err() {
              h2c_ping.broken.set(true);
              break;
            }
          }
        });
      }

      state.conns.borrow_mut().push(h2c.clone());
      Ok(h2c)
    }
    Err(e) => {
      bump_backoff();
      Err(PoolError::Handshake(format!("{e:?}")))
    }
  }
}

async fn do_tls_handshake(
  conn: Connection,
  sni: Option<&str>,
  reactor: Reactor,
) -> Result<Connection, PoolError> {
  let ssl_ctx = SSL_CTXS.with(|c| c.borrow().get(&sni.map(str::to_string)).cloned());
  let ssl_ctx = match ssl_ctx {
    Some(c) => c,
    None => {
      let pool = unsafe { crate::ngx::raw::ngx_grpc_cycle_pool() };
      let new_ctx = SslCtx::new(pool, crate::ngx::cycle_log())
        .ok_or_else(|| PoolError::Tls("ngx_grpc_ssl_ctx_new failed".into()))?;
      let rc = Rc::new(new_ctx);
      SSL_CTXS.with(|c| {
        c.borrow_mut().insert(sni.map(str::to_string), rc.clone());
      });
      rc
    }
  };

  let c = conn.as_ptr();
  ssl::create_connection(&ssl_ctx, c, sni).map_err(|rc| PoolError::Tls(format!("{rc}")))?;

  let conn = match ssl::handshake(c) {
    Ok(ssl::HandshakeOutcome::Done) => {
      if !ssl::handshaked(c) {
        return Err(PoolError::Tls(
          "handshake reported done but not handshaked".into(),
        ));
      }
      conn
    }
    Ok(ssl::HandshakeOutcome::Pending) => {
      let fut = ssl::wait_handshake(c, reactor);
      // `conn`'s Drop-closes-connection behavior must not race the
      // handshake, so the wrapper is leaked here; the future takes over
      // responsibility for closing on failure and hands back a fresh
      // `Connection` on success.
      std::mem::forget(conn);
      fut
        .await
        .map_err(|rc| PoolError::Tls(format!("handshake failed rc={rc}")))?
    }
    Err(rc) => return Err(PoolError::Tls(format!("ngx_ssl_handshake failed rc={rc}"))),
  };

  if ssl::alpn_protocol(c).as_deref() != Some("h2") {
    return Err(PoolError::Tls("ALPN did not negotiate h2".into()));
  }

  Ok(conn)
}

/// Called from `ngx_grpc_client_module`'s `exit_process`, via
/// `ngx::ngx_grpc_rs_exit_process`. Marks every connection draining and
/// closes idle ones immediately, since an open outbound connection can
/// stall graceful worker shutdown. Connections with active streams are
/// left alone: letting an in-flight call finish is the point of
/// graceful shutdown, and nginx's own `worker_shutdown_timeout` still
/// force-terminates one that does not wrap up in time.
pub fn on_worker_exit() {
  POOL.with(|p| {
    for state in p.borrow().values() {
      for c in state.conns.borrow().iter() {
        c.draining.set(true);
        if c.is_idle() {
          c.force_close();
        }
      }
    }
  });
}
