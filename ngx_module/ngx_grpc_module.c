/*
 * ngx_grpc_client_module: a bare NGX_CORE_MODULE whose only job is to give
 * the Rust side init_process/exit_process hooks so the worker-level
 * connection pool (see pool.rs) has a well-defined lifecycle that is
 * independent of any request.
 */

#include <ngx_config.h>
#include <ngx_core.h>

#include "ngx_grpc_shim.h"


static ngx_int_t ngx_grpc_client_init_process(ngx_cycle_t *cycle);
static void ngx_grpc_client_exit_process(ngx_cycle_t *cycle);

extern ngx_int_t ngx_grpc_rs_init_process(ngx_cycle_t *cycle);
extern void ngx_grpc_rs_exit_process(ngx_cycle_t *cycle);


static ngx_core_module_t ngx_grpc_client_module_ctx = {
  ngx_string("grpc_client"),
  NULL, /* create_conf */
  NULL, /* init_conf */
};


ngx_module_t ngx_grpc_client_module = {
  NGX_MODULE_V1,
  &ngx_grpc_client_module_ctx,   /* module context */
  NULL,                          /* module directives */
  NGX_CORE_MODULE,               /* module type */
  NULL,                          /* init master */
  NULL,                          /* init module */
  ngx_grpc_client_init_process,  /* init process */
  NULL,                          /* init thread */
  NULL,                          /* exit thread */
  ngx_grpc_client_exit_process,  /* exit process */
  NULL,                          /* exit master */
  NGX_MODULE_V1_PADDING
};


static ngx_int_t
ngx_grpc_client_init_process(ngx_cycle_t *cycle)
{
  return ngx_grpc_rs_init_process(cycle);
}


static void
ngx_grpc_client_exit_process(ngx_cycle_t *cycle)
{
  ngx_grpc_rs_exit_process(cycle);
}
