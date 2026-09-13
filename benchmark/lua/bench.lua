-- Shared benchmark handler for both old (cosocket+cdylib) and new (nginx
-- native module) versions of lua-resty-grpc. The Lua-side API is the same,
-- so the same file is used for both; `require("resty.grpc")` resolves to
-- whichever version lua_package_path points at.
--
-- handle_a: "documented usage" - create a client, run one unary call, close.
--           Old version: TCP connect + proto parse + h2 handshake per request.
--           New version: pooled connection, cached protos.
-- handle_b: reuse one persistent client per worker (isolates per-call
--           overhead from connection/proto setup).

local grpc = require("resty.grpc")

local _M = {}

local OPTS = {
  host = "127.0.0.1",
  port = 9000,
  ssl = false,
  protos = { "/tmp/grpc-bench/proto/hello.proto" },
}

local function call(cli)
  local res, err = cli:unary("hello.HelloService", "SayHello", { greeting = "world" })
  if not res then
    ngx.status = 500
    ngx.say("unary error: ", err)
    return
  end
  ngx.say(res.reply)
end

function _M.handle_a()
  local cli, err = grpc.new(OPTS)
  if not cli then
    ngx.status = 500
    ngx.say("new error: ", err)
    return
  end
  call(cli)
  cli:close()
end

local persistent

function _M.handle_b()
  if not persistent then
    local cli, err = grpc.new(OPTS)
    if not cli then
      ngx.status = 500
      ngx.say("new error: ", err)
      return
    end
    persistent = cli
  end
  call(persistent)
end

-- handle_c: N unary calls on a single client within one request. Connection
-- setup and proto loading are amortized across the calls, isolating the
-- per-call cost (protobuf conversion + HTTP/2 round trip + glue).
local CALLS_PER_REQUEST = 4

function _M.handle_c()
  local cli, err = grpc.new(OPTS)
  if not cli then
    ngx.status = 500
    ngx.say("new error: ", err)
    return
  end
  local last
  for _ = 1, CALLS_PER_REQUEST do
    local res, cerr = cli:unary("hello.HelloService", "SayHello", { greeting = "world" })
    if not res then
      ngx.status = 500
      ngx.say("unary error: ", cerr)
      cli:close()
      return
    end
    last = res
  end
  ngx.say(last.reply)
  cli:close()
end

return _M
