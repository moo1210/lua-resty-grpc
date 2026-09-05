/*
 * ngx_grpc_shim: the only C code that is allowed to know about nginx's
 * bitfields, static-inline functions and struct layout. Everything exported
 * from here is a plain function taking/returning opaque pointers or
 * integers, so the Rust side never needs bindgen and never touches a
 * bitfield directly.
 */

#ifndef _NGX_GRPC_SHIM_H_INCLUDED_
#define _NGX_GRPC_SHIM_H_INCLUDED_

#include <ngx_config.h>
#include <ngx_core.h>
#include <ngx_event.h>
#include <ngx_event_connect.h>

#if (NGX_SSL)
#include <ngx_event_openssl.h>
#endif


/* ngx_int_t return-code mirrors: NGX_OK=0 NGX_ERROR=-1 NGX_AGAIN=-2
 * NGX_DECLINED=-5 NGX_BUSY=-3. Rust reads these as plain i32. */

ngx_log_t *ngx_grpc_cycle_log(void);
ngx_pool_t *ngx_grpc_cycle_pool(void);
ngx_int_t ngx_grpc_worker_exiting(void);

/* -------------------------------------------------------------------- */
/* pool                                                                  */
/* -------------------------------------------------------------------- */

ngx_pool_t *ngx_grpc_create_pool(ngx_log_t *log);
void ngx_grpc_destroy_pool(ngx_pool_t *pool);

/* -------------------------------------------------------------------- */
/* timers. ngx_event_add_timer/ngx_event_del_timer are static inline    */
/* -------------------------------------------------------------------- */

void ngx_grpc_add_timer(ngx_event_t *ev, ngx_msec_t timer);
void ngx_grpc_del_timer(ngx_event_t *ev);
ngx_int_t ngx_grpc_ev_timer_set(ngx_event_t *ev);

/*
 * Allocates and initializes a standalone event (not tied to any
 * connection). Must only be armed once the event loop is running --
 * i.e. not directly from a core module's init_process, since
 * ngx_event_core_module's own init_process (which sets up
 * cycle->connections/free_connections and the timer machinery) is not
 * guaranteed to have run yet at that point.
 */
ngx_event_t *ngx_grpc_alloc_event(ngx_pool_t *pool);
void ngx_grpc_event_init(ngx_event_t *ev, ngx_event_handler_pt handler,
    ngx_log_t *log, void *data);

/* -------------------------------------------------------------------- */
/* event/connection bitfield accessors                                  */
/* -------------------------------------------------------------------- */

ngx_int_t ngx_grpc_ev_ready(ngx_event_t *ev);
ngx_int_t ngx_grpc_ev_eof(ngx_event_t *ev);
ngx_int_t ngx_grpc_ev_error(ngx_event_t *ev);
ngx_int_t ngx_grpc_ev_timedout(ngx_event_t *ev);
ngx_int_t ngx_grpc_ev_active(ngx_event_t *ev);

void ngx_grpc_conn_set_data(ngx_connection_t *c, void *data);
void *ngx_grpc_conn_get_data(ngx_connection_t *c);

/*
 * ngx_get_connection() sets both c->read->data and c->write->data to `c`
 * itself, so any event handler installed via ngx_grpc_conn_set_handlers
 * can recover the connection from the ngx_event_t it was called with.
 */
void *ngx_grpc_ev_data(ngx_event_t *ev);
void ngx_grpc_conn_set_handlers(ngx_connection_t *c,
    ngx_event_handler_pt read_handler, ngx_event_handler_pt write_handler);
ngx_event_t *ngx_grpc_conn_read_event(ngx_connection_t *c);
ngx_event_t *ngx_grpc_conn_write_event(ngx_connection_t *c);
ngx_socket_t ngx_grpc_conn_fd(ngx_connection_t *c);

/* -------------------------------------------------------------------- */
/* connect                                                               */
/* -------------------------------------------------------------------- */

/* Parses "ip:port" (IPv4 or IPv6 with brackets) into a peer connection
 * descriptor allocated out of `pool`. Returns NGX_OK/NGX_ERROR. */
ngx_int_t ngx_grpc_parse_addr(ngx_pool_t *pool, const u_char *text, size_t len,
    struct sockaddr **sockaddr, socklen_t *socklen, ngx_str_t *name_out);

/*
 * Starts a non-blocking connect. `log` must be a long-lived log (e.g.
 * ngx_cycle->log), never a request's. On NGX_OK/NGX_AGAIN, `*out_c` is
 * set to the new connection with a freshly created `c->pool` (allocated
 * from `log`, not from the caller's `pool`); the caller still owns
 * wiring `c->data` and the read/write handlers via
 * ngx_grpc_conn_set_data / ngx_grpc_conn_set_handlers. On NGX_ERROR or
 * NGX_DECLINED, `*out_c` is untouched.
 */
ngx_int_t ngx_grpc_connect(ngx_log_t *log, struct sockaddr *sockaddr,
    socklen_t socklen, ngx_str_t *name, ngx_connection_t **out_c);

/* SO_ERROR check after the first writable event fires during connect. */
ngx_int_t ngx_grpc_check_connect_error(ngx_connection_t *c);

void ngx_grpc_close_connection(ngx_connection_t *c);
void ngx_grpc_reusable_connection(ngx_connection_t *c, ngx_uint_t reusable);

/* -------------------------------------------------------------------- */
/* plain I/O. Dispatches through c->recv / c->send, which nginx swaps   */
/* to the TLS versions transparently once ngx_ssl_create_connection has  */
/* run, so callers never need to branch on TLS.                         */
/* -------------------------------------------------------------------- */

ssize_t ngx_grpc_conn_recv(ngx_connection_t *c, u_char *buf, size_t size);
ssize_t ngx_grpc_conn_send(ngx_connection_t *c, u_char *buf, size_t size);

/* Re-arms epoll interest after a recv/send returns NGX_AGAIN. Required
 * with edge-triggered epoll, or the next readable/writable transition
 * can go unnoticed. */
ngx_int_t ngx_grpc_handle_read_event(ngx_event_t *rev, ngx_uint_t flags);
ngx_int_t ngx_grpc_handle_write_event(ngx_event_t *wev, size_t lowat);

#if (NGX_SSL)
/* -------------------------------------------------------------------- */
/* TLS                                                                   */
/* -------------------------------------------------------------------- */

typedef struct {
  ngx_ssl_t ssl;
} ngx_grpc_ssl_ctx_t;

/*
 * Creates an ngx_ssl_t configured for client use with ALPN "h2"
 * advertised and client session cache enabled. `pool` should be a
 * long-lived pool (worker-level), not a per-connection one, since the
 * SSL_CTX outlives any single connection.
 */
ngx_grpc_ssl_ctx_t *ngx_grpc_ssl_ctx_new(ngx_pool_t *pool, ngx_log_t *log);
void ngx_grpc_ssl_ctx_free(ngx_grpc_ssl_ctx_t *ctx);

ngx_int_t ngx_grpc_ssl_create_connection(ngx_grpc_ssl_ctx_t *ctx,
    ngx_connection_t *c, const u_char *sni, size_t sni_len);
ngx_int_t ngx_grpc_ssl_handshake(ngx_connection_t *c);
/* 1 on handshake success, 0 on failure/rejection. */
ngx_int_t ngx_grpc_ssl_handshaked(ngx_connection_t *c);
/* Invoked once the handshake completes or fails. */
void ngx_grpc_ssl_set_handshake_handler(ngx_connection_t *c,
    ngx_connection_handler_pt handler);

/*
 * Copies the negotiated ALPN protocol name into `buf` (caller-provided,
 * `buf_len` bytes). Returns the protocol length, 0 if none was
 * negotiated, or -1 on error / buffer too small.
 */
ngx_int_t ngx_grpc_ssl_get_alpn_protocol(ngx_connection_t *c, u_char *buf,
    size_t buf_len);

ngx_int_t ngx_grpc_ssl_shutdown(ngx_connection_t *c);
#endif /* NGX_SSL */

/*
 * Posts an ngx_lua semaphore. Callable from any nginx event handler,
 * not just from within a Lua coroutine. `sem` is the pointer Lua's
 * `ngx.semaphore` object holds (forwarded, never dereferenced here);
 * `is_stream_subsys` selects between ngx_http_lua's and ngx_stream_lua's
 * copy of this FFI call.
 */
ngx_int_t ngx_grpc_lua_sema_post(void *sem, ngx_int_t is_stream_subsys, ngx_int_t n);

/*
 * Bridges Rust's `log` crate to nginx's error_log. `level` is an
 * NGX_LOG_* constant (NGX_LOG_EMERG=1 .. NGX_LOG_DEBUG=8); always goes
 * through the plain `ngx_log_error` path, never `ngx_log_debug`. `log`
 * is typically ngx_cycle->log (see ngx_grpc_cycle_log()).
 * `msg`/`msg_len` need not be NUL-terminated.
 */
void ngx_grpc_log_error(ngx_uint_t level, ngx_log_t *log, const u_char *msg,
    size_t msg_len);

/* `log->log_level`, so callers can skip formatting a message the
 * configured error_log level would discard anyway. */
ngx_uint_t ngx_grpc_log_get_level(ngx_log_t *log);

#endif /* _NGX_GRPC_SHIM_H_INCLUDED_ */
