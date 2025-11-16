local ffi          = require("ffi")
local str_buf      = require("string.buffer")
local cjson        = require("cjson")
local resty_base   = require("resty.core.base")
local ffi_new      = ffi.new
local ffi_cast     = ffi.cast
local ffi_str      = ffi.string

local sleep        = ngx.sleep
local tcp          = ngx.socket.tcp
local is_exiting   = ngx.worker.exiting
local table_new    = resty_base.new_tab
local table_insert = table.insert
local table_remove = table.remove
local json_encode  = cjson.encode
local json_decode  = cjson.decode


ffi.cdef [[
  typedef void *grpc_client;
  typedef void (*grpc_client_on_done)();
  typedef void (*grpc_client_on_reply)(const char*, uintptr_t);
  typedef void (*grpc_client_on_socket_write)(const char*, uintptr_t);

  void grpc_client_loaded_protos(grpc_client handle);

  typedef struct {
    int32_t code;
    char* message;
  } grpc_client_error;

  typedef uint64_t grpc_client_handle;
  typedef uint64_t grpc_client_streaming_request_handle;

  char* grpc_client_version();

  grpc_client_handle grpc_client_new(const char* authority, const char* scheme, grpc_client_error *err);
  void grpc_client_free(grpc_client_handle handle, grpc_client_error *err);
  void grpc_client_set_on_socket_write_cb(grpc_client_handle handle, grpc_client_on_socket_write on_socket_write_cb, grpc_client_error *err);

  void grpc_client_load_proto_file(grpc_client_handle handle, const char* proto_path, grpc_client_error *err);
  void grpc_client_connect(grpc_client_handle handle, grpc_client_error *err);
  void grpc_client_on_receive(grpc_client_handle handle, const char *data, uintptr_t n, grpc_client_error *err);
  uint8_t grpc_client_drive_once(grpc_client_handle handle, grpc_client_error *err);

  // unary
  void grpc_client_unary(grpc_client_handle handle,
                         const char *service,
                         const char *method,
                         const char *data,
                         grpc_client_on_reply on_reply_cb,
                         grpc_client_error *err);

  // server streaming
  void grpc_client_server_streaming(grpc_client_handle handle,
                                  const char *service,
                                  const char *method,
                                  const char *data,
                                  grpc_client_on_reply on_reply_cb,
                                  grpc_client_on_done on_done_cb,
                                  grpc_client_error *err);

  // client streaming
  grpc_client_streaming_request_handle grpc_client_streaming_request_new(grpc_client_handle handle,
                                                                         const char* service,
                                                                         const char* method,
                                                                         grpc_client_error *err);
  void grpc_client_streaming_request_free(grpc_client_streaming_request_handle request_handle, grpc_client_error *err);
  void grpc_client_streaming_request_push(grpc_client_streaming_request_handle request_handle, const char* data, grpc_client_error *err);
  void grpc_client_streaming_request_done(grpc_client_streaming_request_handle request_handle, grpc_client_error *err);
  void grpc_client_client_streaming(grpc_client_handle handle,
                                    const char* service,
                                    const char* method,
                                    grpc_client_streaming_request_handle request_handle,
                                    grpc_client_on_reply on_reply_cb,
                                    grpc_client_error *err);

  // bidirectional streaming
  void grpc_client_streaming(grpc_client_handle handle,
                             const char* service,
                             const char* method,
                             grpc_client_streaming_request_handle request_handle,
                             grpc_client_on_reply cb,
                             grpc_client_on_done on_done,
                             grpc_client_error *err);
]]

local so_name = "librestygrpc.so"
if ffi.os == "OSX" then
  so_name = "librestygrpc.dylib"
end
local function load_shared_lib()
  local tried_paths = table_new(32, 0)
  local i = 1

  for k, _ in string.gmatch(package.cpath, "[^;]+") do
    local fpath = string.match(k, "(.*/)")
    fpath = fpath .. so_name

    local f = io.open(fpath)
    if f ~= nil then
      io.close(f)
      return ffi.load(fpath)
    end
    tried_paths[i] = fpath
    i = i + 1
  end

  return nil, tried_paths
end

local C, tried_paths = load_shared_lib()
if not C then
  tried_paths[#tried_paths + 1] = 'tried above paths but can not load '
      .. so_name
  error(table.concat(tried_paths, '\r\n', 1, #tried_paths))
end

local grpc_client_version                = C.grpc_client_version
local grpc_client_new                    = C.grpc_client_new
local grpc_client_free                   = C.grpc_client_free
local grpc_client_set_on_socket_write_cb = C.grpc_client_set_on_socket_write_cb
local grpc_client_load_proto_file        = C.grpc_client_load_proto_file
local grpc_client_connect                = C.grpc_client_connect
local grpc_client_on_receive             = C.grpc_client_on_receive
local grpc_client_drive_once             = C.grpc_client_drive_once
local grpc_client_unary                  = C.grpc_client_unary
local grpc_client_server_streaming       = C.grpc_client_server_streaming
local grpc_client_streaming_request_new  = C.grpc_client_streaming_request_new
local grpc_client_streaming_request_free = C.grpc_client_streaming_request_free
local grpc_client_streaming_request_push = C.grpc_client_streaming_request_push
local grpc_client_streaming_request_done = C.grpc_client_streaming_request_done
local grpc_client_client_streaming       = C.grpc_client_client_streaming
local grpc_client_streaming              = C.grpc_client_streaming


local function ffi_call(fn, ...)
  local args = { ... };
  local err = ffi_new("grpc_client_error")
  table_insert(args, err)
  local res = fn(unpack(args))
  return res, err.message ~= nil and ffi_str(err.message) or nil, err.code or nil
end

-- override setmetatable to support __gc metamethod on table
local orig_setmetatable = setmetatable
local function setmetatable(t, mt)
  if mt.__gc then
    local proxy = newproxy(true)
    getmetatable(proxy).__gc = function() mt.__gc(t) end
    t[proxy] = true
    return orig_setmetatable(t, mt)
  end
  return orig_setmetatable(t, mt)
end

local _M = { _VERSION = '0.1.0 (' .. ffi_str(grpc_client_version()) .. ')' }

function _M.new(opts)
  -- create grpc client
  local authority = opts.host .. ":" .. opts.port
  local scheme = opts.ssl and "https" or "http"
  local handle, err = ffi_call(grpc_client_new, authority, scheme)
  if handle == 0 then
    return nil, "failed to allocate grpc client: " .. err
  end

  -- set metatable for client methods and gc
  local cli = setmetatable({ tx_buf = str_buf.new(), grpc_client = handle, closed = false }, {
    __index = _M,
    __gc = function(self)
      ngx.log(ngx.DEBUG, "freeing grpc client: ", tostring(self.grpc_client))
      local _, err = ffi_call(grpc_client_free, self.grpc_client)
      if err then
        ngx.log(ngx.ERR, "failed to free grpc client: ", err)
      end
    end
  })

  -- initialize TCP socket and connect
  local sock = tcp()
  sock:settimeouts(1000, 1000, 1000)

  local ok, err = sock:connect(opts.host, opts.port)
  if not ok then
    return nil, "failed to connect: " .. err
  end

  if opts.ssl then
    local _, err = sock:sslhandshake(true, opts.ssl_server_name or opts.host, opts.ssl_verify, false, { "h2" })
    if err ~= nil then
      return nil, "failed to do ssl handshake: " .. err
    end
  end

  -- set on_socket_write callback
  ffi_call(grpc_client_set_on_socket_write_cb, cli.grpc_client,
    ffi_cast("grpc_client_on_socket_write", function(ptr, n)
      local bytes_send = 0
      local data = ffi_str(ptr, n)
      while not is_exiting() do
        if cli.closed then
          break
        end

        local sent, err = sock:send(data:sub(bytes_send + 1))
        if not sent and err == "closed" then
          ngx.log(ngx.ERR, "failed to send data: ", err)
          cli.closed = true
          return
        end

        bytes_send = bytes_send + sent
        if bytes_send >= n then
          break
        else
          ngx.log(ngx.ERR, "partial send in on_socket_write callback: ", bytes_send, " of ", n)
        end
      end
    end)
  )

  -- load pre-defined proto files
  for _, path in ipairs(opts.protos) do
    local _, err = ffi_call(grpc_client_load_proto_file, cli.grpc_client, path)
    if err then
      return nil, "failed to load proto file: " .. err
    end
  end

  -- send HTTP/2 handshake
  local _, err = ffi_call(grpc_client_connect, cli.grpc_client)
  if err then
    return nil, "failed to initialize grpc connection: " .. err
  end

  -- drive once to produce initial bytes to be sent
  ffi_call(grpc_client_drive_once, cli.grpc_client)

  -- start read event loop
  cli.read_evloop = ngx.thread.spawn(function()
    while not is_exiting() do
      if cli.closed then
        break
      end

      local data, err = sock:receiveany(8192)
      if not data and err == "closed" then
        ngx.log(ngx.ERR, "socket closed")
        break
      end
      if data and #data > 0 then
        local _, err = ffi_call(grpc_client_on_receive, cli.grpc_client, data, #data)
        if err then
          ngx.log(ngx.ERR, "failed to process received data: ", err)
          break
        end
        ffi_call(grpc_client_drive_once, cli.grpc_client)
      end

      sleep(0)
    end

    cli:close()
  end)

  return cli
end

function _M.unary(self, service, method, data)
  if self.closed then
    return nil, "grpc client is closed"
  end

  local reply = nil
  local on_reply_cb = ffi_cast("grpc_client_on_reply", function(ptr, n)
    reply = ffi_str(ptr, n)
  end)

  local _, err = ffi_call(grpc_client_unary, self.grpc_client, service, method, json_encode(data), on_reply_cb)
  if err then
    return nil, "failed to send unary request: " .. err
  end
  while not is_exiting() and reply == nil do
    ffi_call(grpc_client_drive_once, self.grpc_client)
    sleep(0)
  end
  return json_decode(reply)
end

function _M.server_streaming(self, service, method, data, on_message)
  if self.closed then
    return "grpc client is closed"
  end

  local done = false
  local on_reply_cb = ffi_cast("grpc_client_on_reply", function(ptr, n)
    local reply = ffi_str(ptr, n)
    on_message(json_decode(reply))
  end)
  local on_done_cb = ffi_cast("grpc_client_on_done", function() done = true end)
  local _, err = ffi_call(grpc_client_server_streaming, self.grpc_client, service, method, json_encode(data),
    on_reply_cb, on_done_cb)
  if err then
    return "failed to send server streaming request: " .. err
  end
  while not is_exiting() and not done do
    ffi_call(grpc_client_drive_once, self.grpc_client)
    sleep(0)
  end
end

function _M.new_streaming_request(self, service, method)
  local handle, err = ffi_call(grpc_client_streaming_request_new, self.grpc_client, service, method)
  if handle == 0 then
    return nil, "failed to create streaming request: " .. err
  end

  return setmetatable({ request = handle }, {
    __index = {
      send = function(sself, data)
        ffi_call(grpc_client_streaming_request_push, sself.request, json_encode(data))
        ffi_call(grpc_client_drive_once, self.grpc_client)
      end,
      done = function(sself)
        ffi_call(grpc_client_streaming_request_done, sself.request)
        ffi_call(grpc_client_drive_once, self.grpc_client)
      end,
      build = function(sself)
        return sself.request
      end,
    },
    __gc = function(self)
      ngx.log(ngx.DEBUG, "freeing streaming request: ", tostring(self.request))
      local _, err = ffi_call(grpc_client_streaming_request_free, self.request)
      if err then
        ngx.log(ngx.ERR, "failed to free streaming request: ", err)
      end
    end,
  })
end

function _M.client_streaming(self, service, method, request)
  if self.closed then
    return nil, "grpc client is closed"
  end

  local reply = nil
  local on_reply_cb = ffi_cast("grpc_client_on_reply", function(ptr, n)
    reply = ffi_str(ptr, n)
  end)
  local _, err = ffi_call(grpc_client_client_streaming, self.grpc_client, service, method, request:build(), on_reply_cb)
  if err then
    return nil, "failed to send client streaming request: " .. err
  end
  while not is_exiting() and reply == nil do
    ffi_call(grpc_client_drive_once, self.grpc_client)
    sleep(0)
  end
  return json_decode(reply)
end

function _M.streaming(self, service, method, request, on_message, opts)
  if self.closed then
    return "grpc client is closed"
  end

  local replies = {}
  local done = false
  local on_reply_cb = ffi_cast("grpc_client_on_reply", function(ptr, n)
    local reply_str = ffi_str(ptr, n)
    if (opts and opts.sync_mode) then
      table_insert(replies, json_decode(reply_str))
    else
      on_message(json_decode(reply_str))
    end
  end)
  local on_done_cb = ffi_cast("grpc_client_on_done", function() done = true end)
  local _, err = ffi_call(grpc_client_streaming, self.grpc_client, service, method, request:build(), on_reply_cb,
    on_done_cb)
  if err then
    return "failed to send streaming request: " .. err
  end

  if opts and opts.sync_mode then
    local poll_fn
    poll_fn = function()
      while not is_exiting() and not done do
        if #replies > 0 then
          local reply = table_remove(replies, 1)
          return reply, poll_fn
        end
        ffi_call(grpc_client_drive_once, self.grpc_client)
        sleep(0.1)
      end
    end

    return nil, poll_fn
  else
    while not is_exiting() and not done do
      ffi_call(grpc_client_drive_once, self.grpc_client)
      sleep(0)
    end
  end
end

function _M.close(self)
  self.closed = true
  if self.write_evloop then
    ngx.thread.kill(self.write_evloop)
    self.write_evloop = nil
  end
  if self.read_evloop then
    ngx.thread.kill(self.read_evloop)
    self.read_evloop = nil
  end
end

return _M
