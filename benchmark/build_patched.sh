#!/bin/bash
# Build a patched copy of the new module with TCP_NODELAY set on the
# upstream gRPC connection, keeping the original module .so intact.
set -euo pipefail
export PATH=/tmp/grpc-bench/bin:$PATH

SRC=/home/carterschuller/Projects/Personal/GitHub/lua-resty-grpc/new
DST=/tmp/grpc-bench/new-patched
MODDIR=/tmp/grpc-bench/openresty/nginx/modules

rm -rf "$DST"
cp -a "$SRC" "$DST"
echo "copied module source to $DST"

python3 - "$DST/ngx_module/ngx_grpc_shim.c" <<'EOF'
import sys
p = sys.argv[1]
s = open(p).read()
old = """  *out_c = c;
  return rc;
}"""
new = """  /*
   * gRPC writes the request as several small frames (HEADERS, then DATA).
   * Without TCP_NODELAY the second small write waits for the first to be
   * ACKed; against a peer using delayed ACK that is a ~40ms stall on every
   * request after the first on a pooled connection.
   */
  (void) ngx_tcp_nodelay(c);

  *out_c = c;
  return rc;
}"""
assert s.count(old) == 1, "patch site not unique/found"
open(p, "w").write(s.replace(old, new))
print("patched:", p)
EOF

cp "$MODDIR/ngx_grpc_client_module.so" "$MODDIR/ngx_grpc_client_module_orig.so"

cd /tmp/grpc-bench/build/openresty-1.27.1.2
./configure --prefix=/tmp/grpc-bench/openresty \
  --with-http_ssl_module --with-http_v2_module \
  --with-stream --with-stream_ssl_module \
  --add-dynamic-module="$DST" -j32 > /tmp/grpc-bench/logs/configure-patched.log 2>&1
make -j32 > /tmp/grpc-bench/logs/make-patched.log 2>&1
make install > /tmp/grpc-bench/logs/install-patched.log 2>&1

cp "$MODDIR/ngx_grpc_client_module.so" "$MODDIR/ngx_grpc_client_module_patched.so"
cp "$MODDIR/ngx_grpc_client_module_orig.so" "$MODDIR/ngx_grpc_client_module.so"

/tmp/grpc-bench/openresty/bin/openresty -t -c /tmp/grpc-bench/conf/new_patched.conf
echo "PATCHED BUILD OK"
ls -la "$MODDIR"
