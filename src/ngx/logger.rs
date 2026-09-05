//! Bridges Rust's `log` crate to nginx's own `error_log`. Without a
//! registered backend, every `log::debug!`/`log::error!` call in the
//! crate is a silent no-op, since `log` is a facade that does nothing
//! until `log::set_logger` is called.
//!
//! The `log` crate does not call `Log::enabled()` before `Log::log()`
//! for you; a `Log` impl must do that filtering itself. `enabled()`
//! below reads `ngx_cycle->log`'s `log_level` field and replicates
//! nginx's own `log->log_level >= level` comparison before anything
//! gets formatted, so a message nginx would discard never pays a
//! `format!` and FFI-call cost.

use super::{NGX_LOG_DEBUG, NGX_LOG_ERR, NGX_LOG_INFO, NGX_LOG_WARN, ngx_uint_t};

fn map_level(level: log::Level) -> ngx_uint_t {
  match level {
    log::Level::Error => NGX_LOG_ERR,
    log::Level::Warn => NGX_LOG_WARN,
    log::Level::Info => NGX_LOG_INFO,
    // nginx's plain error_log scale has no separate "trace" level, so
    // both map to "debug".
    log::Level::Debug | log::Level::Trace => NGX_LOG_DEBUG,
  }
}

pub struct NginxLogger;

impl log::Log for NginxLogger {
  fn enabled(&self, metadata: &log::Metadata) -> bool {
    let log = super::cycle_log();
    if log.is_null() {
      // Nothing to log to yet. Should not happen in practice; failing
      // safe here costs nothing.
      return false;
    }
    let have = unsafe { super::raw::ngx_grpc_log_get_level(log) };
    have >= map_level(metadata.level()) as ngx_uint_t
  }

  fn log(&self, record: &log::Record) {
    if !self.enabled(record.metadata()) {
      return;
    }
    let msg = record.args().to_string();
    let level = map_level(record.level());
    unsafe {
      super::raw::ngx_grpc_log_error(level, super::cycle_log(), msg.as_ptr(), msg.len());
    }
  }

  fn flush(&self) {
    // ngx_grpc_log_error writes synchronously on every call. There is no
    // separate buffer to flush.
  }
}

static LOGGER: NginxLogger = NginxLogger;

/// Registers `NginxLogger` as the global `log` backend. A second call
/// returns `Err` from `log::set_logger`, which is ignored rather than
/// treated as fatal.
pub fn init() {
  let _ = log::set_logger(&LOGGER);
  // `enabled()` reflects nginx's configured error_log level directly,
  // so the crate-wide max level is left fully open here.
  log::set_max_level(log::LevelFilter::Trace);
}
