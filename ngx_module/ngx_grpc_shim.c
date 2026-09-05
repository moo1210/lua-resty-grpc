#include "ngx_grpc_shim.h"

/* ngx_cycle.h already declares `extern volatile ngx_cycle_t *ngx_cycle;` */

ngx_log_t *
ngx_grpc_cycle_log(void)
{
  return ngx_cycle->log;
}


ngx_pool_t *
ngx_grpc_cycle_pool(void)
{
  return ngx_cycle->pool;
}


ngx_int_t
ngx_grpc_worker_exiting(void)
{
  return ngx_exiting ? 1 : 0;
}


ngx_pool_t *
ngx_grpc_create_pool(ngx_log_t *log)
{
  return ngx_create_pool(512, log);
}


void
ngx_grpc_destroy_pool(ngx_pool_t *pool)
{
  ngx_destroy_pool(pool);
}


void
ngx_grpc_add_timer(ngx_event_t *ev, ngx_msec_t timer)
{
  ngx_add_timer(ev, timer);
}


void
ngx_grpc_del_timer(ngx_event_t *ev)
{
  if (ev->timer_set) {
    ngx_del_timer(ev);
  }
}


ngx_int_t
ngx_grpc_ev_timer_set(ngx_event_t *ev)
{
  return ev->timer_set ? 1 : 0;
}


ngx_event_t *
ngx_grpc_alloc_event(ngx_pool_t *pool)
{
  return ngx_pcalloc(pool, sizeof(ngx_event_t));
}


void
ngx_grpc_event_init(ngx_event_t *ev, ngx_event_handler_pt handler,
    ngx_log_t *log, void *data)
{
  ev->handler = handler;
  ev->log = log;
  ev->data = data;
}


ngx_int_t
ngx_grpc_ev_ready(ngx_event_t *ev)
{
  return ev->ready ? 1 : 0;
}


ngx_int_t
ngx_grpc_ev_eof(ngx_event_t *ev)
{
  return ev->eof ? 1 : 0;
}


ngx_int_t
ngx_grpc_ev_error(ngx_event_t *ev)
{
  return ev->error ? 1 : 0;
}


ngx_int_t
ngx_grpc_ev_timedout(ngx_event_t *ev)
{
  return ev->timedout ? 1 : 0;
}


ngx_int_t
ngx_grpc_ev_active(ngx_event_t *ev)
{
  return ev->active ? 1 : 0;
}


void
ngx_grpc_conn_set_data(ngx_connection_t *c, void *data)
{
  c->data = data;
}


void *
ngx_grpc_conn_get_data(ngx_connection_t *c)
{
  return c->data;
}


void *
ngx_grpc_ev_data(ngx_event_t *ev)
{
  return ev->data;
}


void
ngx_grpc_conn_set_handlers(ngx_connection_t *c,
    ngx_event_handler_pt read_handler, ngx_event_handler_pt write_handler)
{
  c->read->handler = read_handler;
  c->write->handler = write_handler;
}


ngx_event_t *
ngx_grpc_conn_read_event(ngx_connection_t *c)
{
  return c->read;
}


ngx_event_t *
ngx_grpc_conn_write_event(ngx_connection_t *c)
{
  return c->write;
}


ngx_socket_t
ngx_grpc_conn_fd(ngx_connection_t *c)
{
  return c->fd;
}


ngx_int_t
ngx_grpc_parse_addr(ngx_pool_t *pool, const u_char *text, size_t len,
    struct sockaddr **sockaddr, socklen_t *socklen, ngx_str_t *name_out)
{
  ngx_addr_t addr;
  ngx_int_t  rc;

  ngx_memzero(&addr, sizeof(ngx_addr_t));

  rc = ngx_parse_addr_port(pool, &addr, (u_char *) text, len);
  if (rc != NGX_OK) {
    return NGX_ERROR;
  }

  *sockaddr = addr.sockaddr;
  *socklen = addr.socklen;
  if (name_out != NULL) {
    *name_out = addr.name;
  }

  return NGX_OK;
}


ngx_int_t
ngx_grpc_connect(ngx_log_t *log, struct sockaddr *sockaddr, socklen_t socklen,
    ngx_str_t *name, ngx_connection_t **out_c)
{
  ngx_peer_connection_t  pc;
  ngx_int_t              rc;
  ngx_connection_t      *c;

  ngx_memzero(&pc, sizeof(ngx_peer_connection_t));

  pc.sockaddr = sockaddr;
  pc.socklen = socklen;
  pc.name = name;
  pc.get = ngx_event_get_peer;
  pc.log = log;
  pc.log_error = NGX_ERROR_ERR;

  rc = ngx_event_connect_peer(&pc);

  if (rc == NGX_ERROR || rc == NGX_DECLINED || rc == NGX_BUSY) {
    return rc;
  }

  /* NGX_OK or NGX_AGAIN: pc.connection is valid. */
  c = pc.connection;
  c->data = NULL;
  c->pool = ngx_create_pool(512, log);
  if (c->pool == NULL) {
    ngx_close_connection(c);
    return NGX_ERROR;
  }

  *out_c = c;
  return rc;
}


ngx_int_t
ngx_grpc_check_connect_error(ngx_connection_t *c)
{
  int        err;
  socklen_t  len;

  if (c->write->timedout) {
    return NGX_ERROR;
  }

#if (NGX_HAVE_KQUEUE)
  if (ngx_event_flags & NGX_USE_KQUEUE_EVENT) {
    if (c->write->pending_eof || c->read->pending_eof) {
      return NGX_ERROR;
    }
  } else
#endif
  {
    err = 0;
    len = sizeof(int);

    if (getsockopt(c->fd, SOL_SOCKET, SO_ERROR, (void *) &err, &len) == -1) {
      return NGX_ERROR;
    }

    if (err) {
      return NGX_ERROR;
    }
  }

  return NGX_OK;
}


void
ngx_grpc_close_connection(ngx_connection_t *c)
{
  ngx_pool_t *pool;

  pool = c->pool;
  ngx_close_connection(c);
  if (pool != NULL) {
    ngx_destroy_pool(pool);
  }
}


void
ngx_grpc_reusable_connection(ngx_connection_t *c, ngx_uint_t reusable)
{
  ngx_reusable_connection(c, reusable);
}


ssize_t
ngx_grpc_conn_recv(ngx_connection_t *c, u_char *buf, size_t size)
{
  return c->recv(c, buf, size);
}


ssize_t
ngx_grpc_conn_send(ngx_connection_t *c, u_char *buf, size_t size)
{
  return c->send(c, buf, size);
}


ngx_int_t
ngx_grpc_handle_read_event(ngx_event_t *rev, ngx_uint_t flags)
{
  return ngx_handle_read_event(rev, flags);
}


ngx_int_t
ngx_grpc_handle_write_event(ngx_event_t *wev, size_t lowat)
{
  return ngx_handle_write_event(wev, lowat);
}


#if (NGX_SSL)

ngx_grpc_ssl_ctx_t *
ngx_grpc_ssl_ctx_new(ngx_pool_t *pool, ngx_log_t *log)
{
  ngx_grpc_ssl_ctx_t  *ctx;
  static const unsigned char alpn_h2[] = { 2, 'h', '2' };

  ctx = ngx_pcalloc(pool, sizeof(ngx_grpc_ssl_ctx_t));
  if (ctx == NULL) {
    return NULL;
  }

  if (ngx_ssl_create(&ctx->ssl, NGX_SSL_TLSv1_2 | NGX_SSL_TLSv1_3, NULL)
      != NGX_OK)
  {
    return NULL;
  }

  if (SSL_CTX_set_alpn_protos(ctx->ssl.ctx, alpn_h2, sizeof(alpn_h2)) != 0) {
    ngx_log_error(NGX_LOG_ERR, log, 0,
                  "ngx_grpc: SSL_CTX_set_alpn_protos() failed");
    return NULL;
  }

  /* No ngx_ssl_client_session_cache() / session resumption here: that
   * is connection-pool policy (see pool.rs), not this layer's job. */

  return ctx;
}


void
ngx_grpc_ssl_ctx_free(ngx_grpc_ssl_ctx_t *ctx)
{
  if (ctx != NULL) {
    ngx_ssl_cleanup_ctx(&ctx->ssl);
  }
}


ngx_int_t
ngx_grpc_ssl_create_connection(ngx_grpc_ssl_ctx_t *ctx, ngx_connection_t *c,
    const u_char *sni, size_t sni_len)
{
  ngx_int_t  rc;

  rc = ngx_ssl_create_connection(&ctx->ssl, c, NGX_SSL_CLIENT);
  if (rc != NGX_OK) {
    return rc;
  }

  if (sni != NULL && sni_len > 0) {
    char *host = ngx_pnalloc(c->pool, sni_len + 1);
    if (host == NULL) {
      return NGX_ERROR;
    }
    ngx_memcpy(host, sni, sni_len);
    host[sni_len] = '\0';

    if (SSL_set_tlsext_host_name(c->ssl->connection, host) == 0) {
      ngx_ssl_error(NGX_LOG_ERR, c->log, 0,
                    "SSL_set_tlsext_host_name() failed");
      return NGX_ERROR;
    }
  }

  return NGX_OK;
}


ngx_int_t
ngx_grpc_ssl_handshake(ngx_connection_t *c)
{
  return ngx_ssl_handshake(c);
}


ngx_int_t
ngx_grpc_ssl_handshaked(ngx_connection_t *c)
{
  return c->ssl->handshaked ? 1 : 0;
}


void
ngx_grpc_ssl_set_handshake_handler(ngx_connection_t *c,
    ngx_connection_handler_pt handler)
{
  c->ssl->handler = handler;
}


ngx_int_t
ngx_grpc_ssl_get_alpn_protocol(ngx_connection_t *c, u_char *buf,
    size_t buf_len)
{
  const unsigned char *data;
  unsigned int          len;

  SSL_get0_alpn_selected(c->ssl->connection, &data, &len);

  if (len == 0) {
    return 0;
  }

  if (len > buf_len) {
    return NGX_ERROR;
  }

  ngx_memcpy(buf, data, len);
  return (ngx_int_t) len;
}


ngx_int_t
ngx_grpc_ssl_shutdown(ngx_connection_t *c)
{
  return ngx_ssl_shutdown(c);
}

#endif /* NGX_SSL */


/* Forward-declared instead of including ngx_http_lua_common.h /
 * ngx_stream_lua_common.h. The real signatures take a typed
 * `ngx_http_lua_sema_t *` / `ngx_stream_lua_sema_t *`; `void *` is
 * ABI-identical since this only forwards the pointer, never
 * dereferences it. */
extern int ngx_http_lua_ffi_sema_post(void *sem, int n);
extern int ngx_stream_lua_ffi_sema_post(void *sem, int n);


ngx_int_t
ngx_grpc_lua_sema_post(void *sem, ngx_int_t is_stream_subsys, ngx_int_t n)
{
  if (is_stream_subsys) {
    return ngx_stream_lua_ffi_sema_post(sem, (int) n);
  }
  return ngx_http_lua_ffi_sema_post(sem, (int) n);
}


void
ngx_grpc_log_error(ngx_uint_t level, ngx_log_t *log, const u_char *msg,
    size_t msg_len)
{
  /* "%*s" is nginx's length-prefixed %s (ngx_string.c); msg need not
   * be NUL-terminated. err=0 omits the "(%d: %s)" errno suffix. */
  ngx_log_error(level, log, 0, "%*s", msg_len, msg);
}


ngx_uint_t
ngx_grpc_log_get_level(ngx_log_t *log)
{
  return log->log_level;
}
