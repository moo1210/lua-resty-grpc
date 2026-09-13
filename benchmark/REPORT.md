# lua-resty-grpc: `old/` vs `new/` — architecture review and load-test report

Date: 2026-09-12. Author: benchmark session (temp harness in `/tmp/grpc-bench`).

## 1. What actually changed

| | `old/` (last cosocket commit `72b4037`) | `new/` (nginx-module commit `db3f20c`) |
|---|---|---|
| Integration | Lua library + Rust `cdylib` (`librestygrpc.so`) loaded from `package.cpath`; installable via luarocks | nginx CORE module: C shim + Rust `staticlib` linked into nginx; `--add-module` / `--add-dynamic-module`; requires rebuilding nginx |
| Network I/O | cosockets (`ngx.socket.tcp`) driven through a Lua callback (`on_socket_write`) and a per-client read `ngx.thread` | raw `ngx_event_connect_peer` connection; nginx read/write event handlers drive the Rust executor directly |
| Waiting | Lua busy-polls: `while ... do grpc_client_drive_once(); ngx.sleep(0) end`; Rust `poke_all()` re-queues every task on every drive | `ngx.semaphore:wait()` real yield; Rust wakes only the affected task; `drive_once` budget 10 000 |
| Protobuf | Rust-side: Lua table → `cjson` → JSON → `protobuf_json_mapping` → protobuf bytes, and back (4 conversions/round trip); proto files parsed per `grpc.new()` | Lua-side lua-protobuf (`pb.encode`/`pb.decode`), isolated `pb.state` per client; proto parse cached per worker; Rust sees opaque gRPC frames only |
| Connections | none: every `grpc.new()` = TCP connect (+ TLS) + h2 handshake; `close()` kills the read thread but never closes the socket | worker-level h2 pool keyed by `(addr,tls,sni)`, ≤2 conns/endpoint, stream-capacity aware, GOAWAY drain, reconnect backoff, PING keepalive |
| TLS | cosocket `sslhandshake(..., {"h2"})`; ALPN needs custom, unmerged OpenResty patches (old README) | own `SSL_CTX` (`SSL_CTX_set_alpn_protos`) inside the module → works on stock OpenResty; **no certificate verification, no session cache** |
| Errors | gRPC status errors only logged; caller spins forever (worker exit) | status code + message returned to Lua; `cancel()` support |
| API | callbacks (`on_message`), `new_streaming_request` builder | iterator / writer objects; same `grpc.new` + `cli:unary` surface |
| Regressions | — | no DNS (literal IP only), no TLS verification, no connect/handshake timeout, no `grpc-timeout` deadline, lua-protobuf dependency |

## 2. Test environment

- OpenResty 1.27.1.2 built from source (32-core Linux box), one worker.
- gRPC backend: Go `grpc-go` h2c server on 127.0.0.1:9000, unary `hello.HelloService/SayHello`, `Reply` = "Hello world".
- Load generator: custom Go HTTP/1.1 keepalive client, latency percentiles, N successful requests or fixed duration.
- Patterns: **A** = client per request + close (documented usage); **B** = one client per worker reused across requests; **C** = 4 unary calls on one client per request.
- CPU measured from `/proc/<pid>/stat` (utime+stime) of the nginx worker over the measurement window.
- Old version requires a **fresh worker per measurement point** (see 3.1) — every old burst below restarts nginx first; new-version bursts do the same for symmetry.

## 3. Results

### 3.1 Old version cannot sustain load at all (fatal, confirmed)

- A fresh worker serves exactly **448 requests**, then returns HTTP 500 forever with
  `grpc.lua:198: too many callbacks` (`ffi_cast` in `grpc.new`). Continued hammering produced
  158 821 more requests, all failures; no recovery.
- Same budget (~896 callbacks) with pattern C: 179 requests (~895 callbacks) → death.
- Cause: LuaJIT's FFI callback slots are a fixed-size pool on x64
  (`CALLBACK_MAX_SLOT = ((4096-8)/(17+128))*32 = 896`), the old library allocates
  1 write-callback per client + 1 reply-callback per call, and they are never reclaimed —
  a standalone LuaJIT `ffi.cast` loop dies the same way even with explicit `collectgarbage()`.
- Client reuse (pattern B) hangs the HTTP request (the per-client read `ngx.thread` keeps the
  request alive); a follow-up request can abort the worker (`Rust cannot catch foreign exceptions`).
  So there is no usage pattern that survives: client-per-request dies at ~448; reuse dies immediately.

### 3.2 Old version, healthy phase (fresh worker, capped 350 requests, medians of 3)

| concurrency | RPS | p50 | p90 | p99 | worker CPU µs/call |
|---|---|---|---|---|---|
| 1 | 1769 | 0.54 ms | 0.64 ms | 0.81 ms | 457 |
| 8 | 2569 | 2.93 ms | 3.76 ms | 4.68 ms | 364 |
| 32 | 2404 | 11.7 ms | 13.5 ms | 57.5 ms | 367 |

### 3.3 New version as shipped (medians of 3)

Burst, 350 requests:

| concurrency | RPS | p50 | p99 | worker CPU µs/call |
|---|---|---|---|---|
| 1 | 22.7 | 44.0 ms | 47.4 ms | ~286* |
| 8 | 184.2 | 44.0 ms | 46.8 ms | 56.0 |
| 32 | 705.1 | 44.1 ms | 48.1 ms | 52.5 |

Sustained 10 s:

| concurrency | RPS | p50 | p99 | CPU µs/call |
|---|---|---|---|---|
| 1 | 22.7 | 44.0 ms | 45.0 ms | ~310* |
| 8 | 181.6 | 44.0 ms | 47.1 ms | 82.6 |
| 32 | 732.9 | 44.0 ms | 48.0 ms | 54.6 |
| 64 | 1465.3 | 44.0 ms | 47.2 ms | 47.2 |

*CPU/call at c=1 includes idle time in the measurement window; effective CPU
cost is the high-concurrency figure.

Pattern C (4 unary calls per client, 10 s sustained):
| concurrency | HTTP req RPS | p50/request | worker CPU µs per gRPC call |
|---|---|---|---|
| 1 | 5.7 | 176.0 ms (4 × 44 ms) | 219-263 |
| 16 | 93.2-110.7 | 176.0 ms | 40.7-48.3 |

Pattern B (client created once per worker, reused across requests, c=1):
| RPS | p50 | CPU µs/call |
|---|---|---|
| 22.7 | 44.0 ms | ~352* |

- Pattern C confirms the stall is per gRPC call (4 calls → 176 ms), and pattern B
  confirms pooling does not help while `TCP_NODELAY` is missing: every request after the
  first on the pooled connection pays the same ~43 ms.


- Every request on a pooled connection pays a **~43 ms stall**; first request on a fresh connection
  is ~3 ms. RPS = concurrency / 43 ms (latency-bound), e.g. 32/0.044 ≈ 727, 64/0.044 ≈ 1454.

- Syscall trace: on a reused connection the module writes HEADERS (37 B) and DATA (30 B) as two
  small `send()`s; the response arrives 42.9 ms later. The socket is created through
  `ngx_event_connect_peer`, which leaves `c->tcp_nodelay` unset, and the module never sets
  `TCP_NODELAY` → classic Nagle + delayed-ACK interaction (peer ACKs the lone small segment
  only after the 40 ms delayed-ACK timer; the DATA segment stays queued until then).

### 3.4 New version with one-line TCP_NODELAY fix

Fix: in `ngx_grpc_connect()` (`new/ngx_module/ngx_grpc_shim.c`), call
`(void) ngx_tcp_nodelay(c);` before returning the connection. (Do **not** set
`c->tcp_nodelay = NGX_TCP_NODELAY_SET` first — `ngx_tcp_nodelay()` returns early
when the state is not `UNSET` and would never call `setsockopt`. This is exactly
why the first attempted patch was a no-op.)

Patch file: `/tmp/grpc-bench/nodelay.patch.txt`. Patched module built from
`/tmp/grpc-bench/new-patched` (same source, one line added), loaded as a
separate dynamic module; nothing in the user's repo was modified.

Keepalive client, 4 requests on one connection (was 3.0 / 43.0 / 43.3 / 43.1 ms):

```
patched keepalive: 0.0033  0.000385  0.000461  0.000329   (seconds)
```

Sustained 10 s (medians of 3):

| concurrency | old (burst only, dies) | new shipped | new + TCP_NODELAY |
|---|---|---|---|
| 1 | 1769 rps, p50 0.54 ms, 457 µs CPU/call | 22.7 rps, p50 44.0 ms | **2598 rps, p50 0.369 ms, p99 0.50 ms, 192 µs CPU/call** |
| 8 | 2569 rps, p50 2.93 ms, 364 µs CPU/call | 182 rps, p50 44.0 ms | **15505 rps, p50 0.489 ms, p99 1.00 ms, 70 µs CPU/call** |
| 32 | 2404 rps, p50 11.7 ms, 367 µs CPU/call | 731 rps, p50 44.0 ms | **17924 rps, p50 1.61 ms, p99 3.61 ms, 59 µs CPU/call** |
| 64 | n/a (worker dies) | 1465 rps, p50 44.0 ms | **20338 rps, p50 3.01 ms, p99 5.55 ms, 56 µs CPU/call** |

Notes:
- Old numbers at c≥8 are capped bursts on a fresh worker (350-381 requests); they cannot be
  sustained because of the callback ceiling (3.1). New numbers are full 10 s runs.
- Burst p99s for the patched build are noisy (26-42 ms windows + first-connection cold start);
  the sustained table above is the reliable one.
- The patched worker is ~85% busy at c=64 (11.4 s CPU per ~13.5 s run), so the single-worker
  ceiling is close; scaling beyond that means more workers.


## 4. Analysis

### 4.1 The old version's real limit is the FFI callback table, and it is fatal
- ~448 client-per-request calls per worker lifetime, then *permanent* failure. This is cumulative,
  not load-dependent: even 1 req/s bricks a worker in ~8 minutes, and nothing recovers short of
  a reload. It also explains why a "worker reuse" pattern is not a workaround — persisting a
  client hangs the request, and unloading it (via `close()`) doesn't return the callback slots.
- The benchmark therefore cannot compare "steady state" for `old/` at all. Its healthy-phase
  numbers (0.5 ms p50 at c=1, ~2.4-2.6 k requests/s at c=8/32 before the burst cap) describe a
  worker that is at most 448 requests away from death.

### 4.2 CPU efficiency: the rewrite's effect is real and large
- Worker CPU per unary call: old ~364-457 µs vs new ~47-83 µs (c≥8) as shipped and ~56-70 µs
  patched, i.e. **~6-7x less CPU per call** once the new version is fixed. This is the
  combination of: no `poke_all()` + `sleep(0)` polling, no per-request proto parse (old parsed
  the `.proto` inside Rust on every `grpc.new`), no JSON bridge (4 conversions/round-trip vs 2),
  and pooled connections instead of connect-per-request.
- Old per-connection setup (measured via pattern C: 4 calls/client, p50 1.08 ms vs 0.54 ms for
  1 call) is ~0.35 ms of connect+handshake+proto-parse per client; the marginal old unary call
  is ~0.18 ms. Patched new does the whole call (pool lookup + encode + round trip + decode) in
  ~0.37 ms p50, ~0.5 ms p99 at c=1, and 15.5-20.3k rps at c=8-64, which old cannot approach
  (2.4-2.6k rps bursts, then death).
- Note the user's premise ("it still has to parse protobuf from Lua"): true, but the new code
  parses the schema once per worker and uses lua-protobuf in LuaJIT; the old code paid a JSON
  encode + Rust JSON-parse + protobuf encode (and reverse) *per message*, plus a schema reparse
  per client. The protobuf location was not where the cost was.

### 4.3 The new version's shipped latency bug dominates its profile
- ~43 ms p50 on every pooled request vs 0.54 ms for `old/` at c=1 — a 80x latency regression for
  the common unary call. Throughput is exactly concurrency / 43 ms, so a single worker is capped
  at ~23 req/s per concurrent stream; at c=64 it reaches only ~1.4 k req/s while burning just
  0.7 s CPU per 10 s (i.e. the bottleneck is the stall, not CPU).
- Root cause: no `TCP_NODELAY` on the module's own upstream socket (see 3.3). One line in
  `ngx_grpc_connect()` fixes it (validated in 3.4 — p50 drops from 44 ms to 0.37 ms and
  throughput rises from 23 to ~2.6k rps at c=1, 20.3k rps at c=64). Until then, users can only
  work around it by making the module create a new connection for each request (not
  configurable) or by lowering expectations.

### 4.4 So why did the author migrate?
Evidence in the repos (no explicit design doc; commit history and code comments):
1. **TLS/ALPN**: old requires custom, unmerged OpenResty cosocket patches for `ssl=true`
   (old README); the new module owns its `SSL_CTX` and advertises `h2` itself
   (`new/src/ngx/ssl.rs:1-5` calls this "the piece that actually solves the ALPN problem this
   crate exists for"). This alone justifies a native module for TLS users.
2. **Removing the cosocket impedance mismatch**: commits before the rewrite
   (`split write and read evloop`, `move write buf to socket write callback`) show workarounds
   accumulating; `old/src/transport/memory_shim.rs:51-56` documents that a full cosocket
   buffer suspends the Rust call stack mid-callback.
3. **Polling → events**: the old per-call `while ... drive_once(); sleep(0) end` and Rust
   `poke_all()` are replaced by semaphore wakeups and precise nginx event handlers.
4. **Pooling and error propagation** (gRPC status codes, cancellation) don't exist in the old
   design at all.

### 4.5 Where the new version regresses
- No TLS certificate verification (`ssl=true` accepts any certificate).
- No DNS (literal IP only), no connect/handshake timeout, no `grpc-timeout`.
- Requires rebuilding nginx and installing lua-protobuf.
- Shipped-latency bug above.

## 5. Recommendation

- **Do not use `old/` in production.** The ~448-call ceiling is architectural (LuaJIT's
  896-slot FFI callback table, callbacks never reclaimed) and cannot be fixed from
  configuration or by changing usage patterns; persisting clients hangs requests instead.
  Even ignoring that, it needs patched OpenResty for TLS and uses ~6-7x more CPU per call.
- **Use `new/`, with the `TCP_NODELAY` fix applied** to `new/ngx_module/ngx_grpc_shim.c` (patch
  in `/tmp/grpc-bench/nodelay.patch.txt`), and report it upstream — the fix improves the new
  module's own unary p50 at c=1 from 44 ms to 0.37 ms (~120x) and raises single-worker
  throughput from ~1.5k to ~20k rps, making it faster at c=1 than even the old version's
  healthy phase (0.37 ms vs 0.54 ms) and ~6-7x more CPU-efficient. The fix is one line and has
  been built and measured, not just theorized.
- **Mind the new version's security gaps**: if you need TLS to endpoints you don't fully trust,
  the module as shipped has no certificate verification; either add verification
  (`ngx_ssl_trusted_certificate` + verify callback) or terminate TLS in a local sidecar and
  speak h2c to it. Also resolve DNS yourself (Lua `resolver`/`resty.dns.resolver`) and add
  timeouts in application code until the module grows them.

