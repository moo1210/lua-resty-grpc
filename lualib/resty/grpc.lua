-- lua-resty-grpc: a synchronous, non-blocking gRPC client bound
-- directly to nginx connections.
--
-- The client lives in a static nginx module (ngx_grpc_client_module)
-- linked into the nginx binary itself, so every symbol below is
-- already in `ffi.C`.
--
-- Protobuf encoding and decoding happen entirely here, using
-- lua-protobuf. Rust only ever sees framed, opaque bytes. Connections
-- are pooled at the worker level inside Rust, so `new()` is cheap to
-- call per request; it just resolves or creates a pool entry.
--
-- Concurrency model: every blocking call here uses a real
-- `ngx.semaphore:wait()`, a coroutine yield, never a poll loop. This
-- blocks only the calling coroutine, not the worker process; other
-- coroutines keep running on nginx's event loop in the meantime. Rust
-- posts the semaphore from whatever nginx event handler produced the
-- next message or the final status.

local ffi       = require("ffi")
local pb        = require("pb")
local protoc    = require("protoc")
local semaphore = require("ngx.semaphore")

local ffi_new   = ffi.new
local ffi_str   = ffi.string
local C         = ffi.C

local subsystem_is_stream = (ngx.config.subsystem == "stream")

ffi.cdef [[
  typedef struct {
    int32_t code;
    char *message;
  } grpc_client_error;

  const char *grpc_client_version(void);

  uint64_t grpc_client_new(const char *addr, const char *scheme,
      const char *sni, grpc_client_error *err);
  void grpc_client_free(uint64_t handle, grpc_client_error *err);

  uint64_t grpc_call_unary(uint64_t client_handle, const char *path,
      const char *body_ptr, size_t body_len,
      void *sem, uint8_t is_stream_subsys, grpc_client_error *err);
  uint64_t grpc_call_server_streaming(uint64_t client_handle, const char *path,
      const char *body_ptr, size_t body_len,
      void *sem, uint8_t is_stream_subsys, grpc_client_error *err);
  uint64_t grpc_call_client_streaming(uint64_t client_handle, const char *path,
      void *sem, uint8_t is_stream_subsys, grpc_client_error *err);
  uint64_t grpc_call_streaming(uint64_t client_handle, const char *path,
      void *sem, uint8_t is_stream_subsys, grpc_client_error *err);

  void grpc_call_send(uint64_t call, const char *body_ptr, size_t body_len,
      grpc_client_error *err);
  void grpc_call_done(uint64_t call, grpc_client_error *err);
  void grpc_call_cancel(uint64_t call, grpc_client_error *err);
  void grpc_call_free(uint64_t call, grpc_client_error *err);

  uint8_t grpc_call_poll(uint64_t call,
      const char **out_msg_ptr, size_t *out_msg_len,
      int32_t *out_status_code,
      const char **out_status_msg_ptr, size_t *out_status_msg_len,
      grpc_client_error *err);
]]

local POLL_PENDING = 0
local POLL_MESSAGE = 1
local POLL_DONE = 2

local DEFAULT_TIMEOUT = 60 -- seconds; sem:wait() budget per iteration

local _M = { _VERSION = ffi_str(C.grpc_client_version()) }

-- lua-protobuf keeps a single global "active" descriptor state (see
-- pb.state()); pb.encode/pb.decode/pb.load all operate against
-- whichever one is currently active. Other code in the same worker,
-- notably APISIX's grpc-transcode plugin, loads its own proto sets
-- into that same global slot. Left unmanaged, two independently loaded
-- proto sets that happen to declare a same-named message with a
-- different shape would collide: lua-protobuf's pb_newtype() returns
-- the existing type object for an already-registered name instead of
-- erroring, so encoding or decoding one client's messages could
-- silently use the other's field layout.
--
-- The fix, matching the pattern grpc-transcode itself uses in
-- request.lua/response.lua: each client gets its own isolated pb
-- state, swapped in immediately before every pb.encode/pb.decode call
-- and swapped back out immediately after. pb.encode/pb.decode are
-- synchronous C calls with no yield point inside them, so this swap
-- window never crosses a coroutine switch and is safe under nginx's
-- cooperative scheduling even with many requests in flight at once.

--- Runs `fn(...)` with `pb_state` swapped in as lua-protobuf's active
--- state, then swaps the previous one back, whether `fn` succeeded or
--- raised. Errors from `fn` are re-raised so callers see the same
--- failure they would from calling `pb.encode`/`pb.decode` directly.
local function with_pb_state(pb_state, fn, ...)
  local old_state = pb.state(pb_state)
  local ok, result = pcall(fn, ...)
  pb.state(old_state)
  if not ok then
    error(result, 0)
  end
  return result
end

-------------------------------------------------------------------
-- proto loading: worker-level cache, keyed by the sorted set of
-- proto file paths a client was constructed with. This is a real,
-- synchronous protoc parse, so it must be called from
-- init_worker_by_lua or otherwise before any hot-path use.
-------------------------------------------------------------------

local proto_index_cache = {}

--- Returns `index, pb_state`, or `nil, err` on failure. `pb_state` is
--- the isolated lua-protobuf state holding every type this proto set
--- declared; callers must swap it in (via `with_pb_state`) around any
--- pb.encode/pb.decode that uses it.
local function load_protos(paths)
  local key = table.concat(paths, ";")
  local cached = proto_index_cache[key]
  if cached then
    return cached.index, cached.pb_state
  end

  -- protoc.reload() itself calls pb.load() to register the bootstrap
  -- google.protobuf.* descriptor types, so the isolated state has to
  -- be swapped in before that call, not just around the user's own
  -- .proto files.
  local old_state = pb.state(nil)

  local ok, index_or_err, load_err = pcall(function()
    protoc.reload()
    local p = protoc.new()
    for _, path in ipairs(paths) do
      local loaded_ok, perr = pcall(p.loadfile, p, path)
      if not loaded_ok then
        return nil, "failed to load proto file " .. path .. ": " .. tostring(perr)
      end
    end

    local index = {}
    for _, compiled in pairs(p.loaded) do
      if type(compiled) == "table" and compiled.service then
        local pkg = compiled.package
        for _, s in ipairs(compiled.service) do
          local svc_name = pkg and (pkg .. "." .. s.name) or s.name
          local methods = {}
          for _, m in ipairs(s.method) do
            methods[m.name] = m
          end
          index[svc_name] = methods
        end
      end
    end

    return index
  end)

  -- Swap back regardless of outcome, capturing the isolated state that
  -- was just populated (empty on failure, but still fine to discard).
  local pb_state = pb.state(old_state)

  if not ok then
    return nil, "failed to load protos: " .. tostring(index_or_err)
  end
  if not index_or_err then
    return nil, load_err
  end

  proto_index_cache[key] = { index = index_or_err, pb_state = pb_state }
  return index_or_err, pb_state
end

-------------------------------------------------------------------
-- client
-------------------------------------------------------------------

local client_mt = { __index = _M }

--- Creates a client bound to one endpoint (`opts.host`/`opts.port`,
--- IP:port only, no DNS). `opts.protos` is a list of `.proto` file
--- paths. `opts.ssl` turns on TLS with ALPN "h2" advertised;
--- `opts.ssl_server_name` sets SNI (defaults to `opts.host`).
---
--- NOTE (security gap, not silently shipped): certificate verification
--- is not implemented, regardless of `opts.ssl_verify`. The TLS
--- handshake (`src/ngx/ssl.rs`) never loads a trusted CA bundle or
--- checks `SSL_get_verify_result`. Only use `opts.ssl` today against
--- endpoints you trust the network path to, or add
--- `ngx_ssl_trusted_certificate` and a verify callback before relying
--- on this for anything adversarial.
function _M.new(opts)
  local index, pb_state_or_err = load_protos(opts.protos or {})
  if not index then
    return nil, pb_state_or_err
  end
  local pb_state = pb_state_or_err

  local scheme = opts.ssl and "https" or "http"
  local sni = ""
  if opts.ssl then
    sni = opts.ssl_server_name or opts.host or ""
  end

  local addr = opts.host .. ":" .. opts.port
  local errbuf = ffi_new("grpc_client_error")
  local handle = C.grpc_client_new(addr, scheme, sni, errbuf)
  if handle == 0 then
    return nil, "failed to create grpc client: " .. ffi_str(errbuf.message)
  end

  return setmetatable({
    grpc_client = handle,
    index = index,
    pb_state = pb_state,
    err = errbuf,
    timeout = opts.timeout or DEFAULT_TIMEOUT,
    closed = false,
  }, client_mt)
end

function _M.close(self)
  if self.closed then
    return
  end
  self.closed = true
  C.grpc_client_free(self.grpc_client, self.err)
end

client_mt.__gc = _M.close

local function method_descriptor(self, service, method)
  local svc = self.index[service]
  if not svc then
    return nil, "unknown service " .. service
  end
  local md = svc[method]
  if not md then
    return nil, "unknown method " .. service .. "/" .. method
  end
  return md
end

-------------------------------------------------------------------
-- shared poll/wait plumbing
-------------------------------------------------------------------

-- One of these per in-flight call. Cheap, but not free; fine at gRPC
-- call frequency, not per-message.
local function new_poll_bufs()
  return {
    msg_ptr = ffi_new("const char*[1]"),
    msg_len = ffi_new("size_t[1]"),
    status_code = ffi_new("int32_t[1]"),
    status_msg_ptr = ffi_new("const char*[1]"),
    status_msg_len = ffi_new("size_t[1]"),
  }
end

--- Blocks (via `sem:wait`, a real yield) until the next event, then
--- polls it. Returns `"message", bytes` / `"done", code, message` /
--- `"timeout"`.
local function wait_next(self, call, sem, bufs)
  while true do
    local kind = C.grpc_call_poll(call, bufs.msg_ptr, bufs.msg_len,
      bufs.status_code, bufs.status_msg_ptr, bufs.status_msg_len, self.err)

    if kind == POLL_MESSAGE then
      return "message", ffi_str(bufs.msg_ptr[0], tonumber(bufs.msg_len[0]))
    elseif kind == POLL_DONE then
      return "done", tonumber(bufs.status_code[0]),
        ffi_str(bufs.status_msg_ptr[0], tonumber(bufs.status_msg_len[0]))
    end

    -- POLL_PENDING: wait for the next post. One post only guarantees
    -- "there is at least one new event", so we loop back to poll
    -- rather than assuming exactly one is ready.
    local ok, werr = sem:wait(self.timeout)
    if not ok then
      return "timeout", werr
    end
  end
end

-------------------------------------------------------------------
-- unary / server-streaming (single request message)
-------------------------------------------------------------------

function _M.unary(self, service, method, data)
  if self.closed then
    return nil, "grpc client is closed"
  end
  local md, merr = method_descriptor(self, service, method)
  if not md then
    return nil, merr
  end

  local body = with_pb_state(self.pb_state, pb.encode, md.input_type, data)
  local path = "/" .. service .. "/" .. method
  local sem = semaphore.new()

  local call = C.grpc_call_unary(self.grpc_client, path, body, #body,
    sem.sem, subsystem_is_stream and 1 or 0, self.err)
  if call == 0 then
    return nil, "failed to start unary call: " .. ffi_str(self.err.message)
  end

  local bufs = new_poll_bufs()
  local reply

  while true do
    local kind, a, b = wait_next(self, call, sem, bufs)
    if kind == "message" then
      reply = with_pb_state(self.pb_state, pb.decode, md.output_type, a)
    elseif kind == "done" then
      C.grpc_call_free(call, self.err)
      if a ~= 0 then
        return nil, b, a
      end
      return reply
    else -- timeout
      C.grpc_call_cancel(call, self.err)
      C.grpc_call_free(call, self.err)
      return nil, "timeout waiting for unary response"
    end
  end
end

--- Returns an iterator function. IMPORTANT: because a plain Lua
--- generic `for` loop stops as soon as the *first* returned value is
--- nil (silently dropping any error that comes with it), call this
--- directly instead of with `for ... in`:
---
---   local it = c:server_streaming(service, method, req)
---   while true do
---     local msg, err = it()
---     if err then
---       -- handle err, then break
---       break
---     end
---     if msg == nil then
---       break -- clean end of stream
---     end
---     -- use msg
---   end
function _M.server_streaming(self, service, method, data)
  if self.closed then
    return function() return nil, "grpc client is closed" end
  end
  local md, merr = method_descriptor(self, service, method)
  if not md then
    return function() return nil, merr end
  end

  local body = with_pb_state(self.pb_state, pb.encode, md.input_type, data)
  local path = "/" .. service .. "/" .. method
  local sem = semaphore.new()

  local call = C.grpc_call_server_streaming(self.grpc_client, path, body, #body,
    sem.sem, subsystem_is_stream and 1 or 0, self.err)
  if call == 0 then
    local msg = "failed to start server_streaming call: " .. ffi_str(self.err.message)
    return function() return nil, msg end
  end

  local bufs = new_poll_bufs()
  local done = false

  return function()
    if done then
      return nil
    end
    local kind, a, b = wait_next(self, call, sem, bufs)
    if kind == "message" then
      return with_pb_state(self.pb_state, pb.decode, md.output_type, a)
    elseif kind == "done" then
      done = true
      C.grpc_call_free(call, self.err)
      if a ~= 0 then
        return nil, b
      end
      return nil
    else -- timeout
      done = true
      C.grpc_call_cancel(call, self.err)
      C.grpc_call_free(call, self.err)
      return nil, "timeout waiting for stream message"
    end
  end
end

-------------------------------------------------------------------
-- client-streaming / bidi streaming (request messages sent over time)
-------------------------------------------------------------------

local stream_writer_mt = {}
stream_writer_mt.__index = stream_writer_mt

--- Sends one request message. Safe to call repeatedly until `:done()`.
function stream_writer_mt:send(data)
  if self.send_done then
    return nil, "already called done()"
  end
  local body = with_pb_state(self.client.pb_state, pb.encode, self.md.input_type, data)
  C.grpc_call_send(self.call, body, #body, self.client.err)
  return true
end

--- Signals no more request messages.
function stream_writer_mt:done_sending()
  if self.send_done then
    return
  end
  self.send_done = true
  C.grpc_call_done(self.call, self.client.err)
end

--- Cooperative cancellation. Wire this to `ngx.on_abort` if the caller
--- wants prompt cleanup when the client disconnects mid-call. The
--- library does not register `ngx.on_abort` itself; it is a
--- per-request singleton the application may also need.
function stream_writer_mt:cancel()
  C.grpc_call_cancel(self.call, self.client.err)
end

function stream_writer_mt:free()
  if self.freed then
    return
  end
  self.freed = true
  C.grpc_call_free(self.call, self.client.err)
end

stream_writer_mt.__gc = stream_writer_mt.free

--- Client-streaming: `:send(tbl)` any number of times, then
--- `:result()` once to signal EOF and block for the single response.
function _M.client_streaming(self, service, method)
  if self.closed then
    return nil, "grpc client is closed"
  end
  local md, merr = method_descriptor(self, service, method)
  if not md then
    return nil, merr
  end

  local path = "/" .. service .. "/" .. method
  local sem = semaphore.new()

  local call = C.grpc_call_client_streaming(self.grpc_client, path,
    sem.sem, subsystem_is_stream and 1 or 0, self.err)
  if call == 0 then
    return nil, "failed to start client_streaming call: " .. ffi_str(self.err.message)
  end

  local writer = setmetatable({
    call = call,
    client = self,
    md = md,
    sem = sem,
    send_done = false,
    freed = false,
  }, stream_writer_mt)

  function writer:result()
    self:done_sending()
    local bufs = new_poll_bufs()
    local reply
    while true do
      local kind, a, b = wait_next(self.client, self.call, self.sem, bufs)
      if kind == "message" then
        reply = with_pb_state(self.client.pb_state, pb.decode, md.output_type, a)
      elseif kind == "done" then
        self:free()
        if a ~= 0 then
          return nil, b, a
        end
        return reply
      else
        self:cancel()
        self:free()
        return nil, "timeout waiting for client_streaming response"
      end
    end
  end

  return writer
end

--- Bidirectional streaming: `:send(tbl)` any number of times,
--- `:done_sending()` when finished sending, `:recv()` repeatedly for
--- responses. Same nil/err contract as `server_streaming`'s iterator:
--- check `err` before treating `msg == nil` as clean EOF.
function _M.streaming(self, service, method)
  if self.closed then
    return nil, "grpc client is closed"
  end
  local md, merr = method_descriptor(self, service, method)
  if not md then
    return nil, merr
  end

  local path = "/" .. service .. "/" .. method
  local sem = semaphore.new()

  local call = C.grpc_call_streaming(self.grpc_client, path,
    sem.sem, subsystem_is_stream and 1 or 0, self.err)
  if call == 0 then
    return nil, "failed to start streaming call: " .. ffi_str(self.err.message)
  end

  local writer = setmetatable({
    call = call,
    client = self,
    md = md,
    sem = sem,
    send_done = false,
    freed = false,
    recv_done = false,
    bufs = new_poll_bufs(),
  }, stream_writer_mt)

  function writer:recv()
    if self.recv_done then
      return nil
    end
    local kind, a, b = wait_next(self.client, self.call, self.sem, self.bufs)
    if kind == "message" then
      return with_pb_state(self.client.pb_state, pb.decode, md.output_type, a)
    elseif kind == "done" then
      self.recv_done = true
      self:free()
      if a ~= 0 then
        return nil, b
      end
      return nil
    else
      self.recv_done = true
      self:cancel()
      self:free()
      return nil, "timeout waiting for streaming message"
    end
  end

  return writer
end

return _M
