-- Debug handlers to isolate old-version behavior.
local grpc = require("resty.grpc")

local _M = {}

local OPTS = {
  host = "127.0.0.1",
  port = 9000,
  ssl = false,
  protos = { "/tmp/grpc-bench/proto/hello.proto" },
}

local persistent

function _M.handle_persistent()
  if not persistent then
    ngx.log(ngx.ERR, "DBG persistent: creating client")
    local cli, err = grpc.new(OPTS)
    if not cli then
      ngx.log(ngx.ERR, "DBG persistent: new failed: ", err)
      ngx.status = 500
      ngx.say("new error: ", err)
      return
    end
    persistent = cli
    ngx.log(ngx.ERR, "DBG persistent: client created")
  end
  ngx.log(ngx.ERR, "DBG persistent: calling unary")
  local res, err = persistent:unary("hello.HelloService", "SayHello", { greeting = "world" })
  ngx.log(ngx.ERR, "DBG persistent: unary returned res=", tostring(res and res.reply), " err=", tostring(err))
  ngx.say(res and res.reply or ("ERR: " .. tostring(err)))
end

function _M.handle_two_calls()
  ngx.log(ngx.ERR, "DBG two: creating client")
  local cli, err = grpc.new(OPTS)
  if not cli then
    ngx.log(ngx.ERR, "DBG two: new failed: ", err)
    ngx.status = 500
    ngx.say("new error: ", err)
    return
  end
  local res1, err1 = cli:unary("hello.HelloService", "SayHello", { greeting = "one" })
  ngx.log(ngx.ERR, "DBG two: call1 res=", tostring(res1 and res1.reply), " err=", tostring(err1))
  local res2, err2 = cli:unary("hello.HelloService", "SayHello", { greeting = "two" })
  ngx.log(ngx.ERR, "DBG two: call2 res=", tostring(res2 and res2.reply), " err=", tostring(err2))
  ngx.say((res1 and res1.reply or "ERR1"), " / ", (res2 and res2.reply or "ERR2"))
  cli:close()
end

return _M
