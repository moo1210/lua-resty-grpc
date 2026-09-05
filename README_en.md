# lua-resty-grpc

语言：[简体中文](README.md) | English

A gRPC client for OpenResty, built as a native nginx module rather than a cosocket-based Lua library.

## Table of Contents

- [Synopsis](#synopsis)
- [Limitations](#limitations)
- [Installation](#installation)
- [Methods](#methods)
  - [new](#new)
  - [close](#close)
  - [unary](#unary)
  - [server\_streaming](#server_streaming)
  - [client\_streaming](#client_streaming)
  - [streaming](#streaming)

## Synopsis

```nginx
http {
    server {
        listen 8080;

        location /unary {
            content_by_lua_block {
                local grpc = require("resty.grpc")
                local cli, err = grpc.new({
                    host   = "127.0.0.1",
                    port   = 9000,
                    ssl    = false,
                    protos = { "/path/to/hello.proto" },
                })
                if not cli then
                    ngx.say("new client error: ", err)
                    return
                end

                local res, err = cli:unary("hello.HelloService", "SayHello", { greeting = "world" })
                if not res then
                    ngx.say("unary error: ", err)
                    return
                end
                ngx.say("unary reply: ", res.reply)
                cli:close()
            }
        }
    }
}
```

## Limitations

- DNS resolution is not supported; `host` must be a literal IP address.
- TLS certificate verification is not implemented; any certificate presented by the peer is accepted. Use `ssl = true` only for endpoints reachable over a trusted network path.
- There is no connect or TLS handshake timeout, and no `grpc-timeout` deadline is sent to the server. The only timeout is the client-side idle wait between events (`opts.timeout`; see [new](#new)).

## Installation

Building requires Rust 1.85 or newer (this crate uses the 2024 edition) and `cargo`, an nginx or OpenResty source tree built with `ngx_http_lua_module` and/or `ngx_stream_lua_module` (for `ngx.semaphore`, plus `--with-http_ssl_module`/`--with-stream_ssl_module` if TLS is used), and the [lua-protobuf](https://github.com/starwing/lua-protobuf) rock for protobuf encoding and decoding in Lua.

This module is compiled directly into nginx; it is not installed via luarocks.

```bash
git clone https://github.com/bzp2010/lua-resty-grpc.git

cd /path/to/nginx-or-openresty-src
./configure --add-module=/path/to/lua-resty-grpc ...(your other flags)
make && make install

cp -r /path/to/lua-resty-grpc/lualib/resty /path/to/lua_package_path/
```

`./configure` builds this crate with `cargo build --release` as part of the nginx build; a separate Rust build step is not required. `--add-dynamic-module` is also supported, for shorter rebuild cycles during development.

`--add-module` and `make install` build and install only the nginx binary. The `cp` command above installs `grpc.lua` onto the Lua search path (`lua_package_path`).

## Methods

### new

**syntax:** `cli, err = grpc.new(opts)`

Creates a client bound to one endpoint. `opts` is a Lua table:

- `host` (string, required): a literal IP address (no DNS).
- `port` (number, required): server port.
- `protos` (string[], required): `.proto` file paths, loaded once and cached per client.
- `ssl` (boolean, optional): enable TLS with ALPN `h2`. Default `false`.
- `ssl_server_name` (string, optional): SNI; defaults to `host`.
- `timeout` (number, optional): seconds to wait for the next event (message, or the call ending) before giving up. Default `60`.

Connections to the same endpoint are pooled at the worker level; calling `new()` on every request does not necessarily open a new TCP connection.

### close

**syntax:** `cli:close()`

Releases the client. Idempotent; also runs automatically when the client is garbage-collected.

### unary

**syntax:** `res, err, code = cli:unary(service, method, data)`

Sends a unary request, where `service` is like `hello.HelloService` and `method` is `SayHello`. `data` is a Lua table, encoded per the proto definition.

Returns the decoded response table, or `nil` plus an error message (and, for a non-OK gRPC status, the numeric status code as a third value).

### server_streaming

**syntax:** `it = cli:server_streaming(service, method, data)`

Sends a server-streaming request and returns an iterator function. Call it directly in a loop rather than with a generic `for`: a plain `for msg, err in it do ... end` stops as soon as the first returned value is `nil`, silently discarding any accompanying error.

```lua
local it = cli:server_streaming("hello.HelloService", "LotsOfReplies", { greeting = "world" })
while true do
    local msg, err = it()
    if err then
        -- handle err, then break
        break
    end
    if msg == nil then
        break -- clean end of stream
    end
    -- use msg
end
```

### client_streaming

**syntax:** `writer, err = cli:client_streaming(service, method)`

Starts a client-streaming call and returns a writer:

- `writer:send(data)` sends one request message. Safe to call repeatedly.
- `writer:result()` signals end-of-stream and blocks for the server's single response, returning it the same way `unary` does.

```lua
local writer, err = cli:client_streaming("hello.HelloService", "LotsOfGreetings")
writer:send({ greeting = "msg1" })
writer:send({ greeting = "msg2" })
writer:send({ greeting = "msg3" })

local res, err = writer:result()
```

### streaming

**syntax:** `writer, err = cli:streaming(service, method)`

Starts a bidirectional-streaming call and returns a writer:

- `writer:send(data)` sends one request message.
- `writer:done_sending()` signals no more request messages.
- `writer:recv()` blocks for the next response message, `nil` (with no error) on a clean end of stream, following the same nil/err contract as `server_streaming`'s iterator.

`:send()` and `:recv()` are independent calls that do not block each other:

```lua
local writer, err = cli:streaming("hello.HelloService", "BidiHello")

writer:send({ greeting = "msg-1" })
writer:send({ greeting = "msg-2" })
writer:send({ greeting = "msg-3" })
writer:done_sending()

while true do
    local msg, err = writer:recv()
    if err then
        break
    end
    if msg == nil then
        break
    end
    -- use msg
end
```
