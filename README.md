lua-resty-grpc
===============

语言：简体中文 | [English](README_en.md)

面向 OpenResty 的 gRPC 客户端。它是一个直接编入 nginx 的原生模块,而非基于 cosocket 的纯 Lua 库。

目录
----

- [快速示例](#快速示例)
- [限制](#限制)
- [安装](#安装)
- [方法](#方法)
  - [new](#new)
  - [close](#close)
  - [unary](#unary)
  - [server_streaming](#server_streaming)
  - [client_streaming](#client_streaming)
  - [streaming](#streaming)

快速示例
--------

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

限制
----

- 不支持 DNS 解析,`host` 必须是字面量 IP 地址。
- 未实现 TLS 证书校验,对端提供的任何证书都会被接受。仅应在网络路径可信的端点上使用 `ssl = true`。
- 没有单独的建连或 TLS 握手超时,也不会向服务端发送 `grpc-timeout` deadline。唯一的超时机制是客户端侧两次事件之间的空闲等待(`opts.timeout`,见 [new](#new))。

安装
----

构建需要 Rust 1.85 或更高版本(此 crate 使用 2024 edition)及 `cargo`,一份已启用 `ngx_http_lua_module` 和/或 `ngx_stream_lua_module`(提供 `ngx.semaphore`;若使用 TLS,还需启用 `--with-http_ssl_module`/`--with-stream_ssl_module`)的 nginx 或 OpenResty 源码树,以及 [lua-protobuf](https://github.com/starwing/lua-protobuf) 这个 rock 供 Lua 侧完成 protobuf 编解码。

此模块直接编入 nginx 本体,并非通过 luarocks 安装。

```bash
git clone https://github.com/bzp2010/lua-resty-grpc.git

cd /path/to/nginx-or-openresty-src
./configure --add-module=/path/to/lua-resty-grpc ...(your other flags)
make && make install

cp -r /path/to/lua-resty-grpc/lualib/resty /path/to/lua_package_path/
```

`./configure` 会作为 nginx 构建的一部分,用 `cargo build --release` 构建此 crate,不需要单独执行 Rust 构建步骤。`--add-dynamic-module` 同样受支持,可在开发阶段缩短重新构建的周期。

`--add-module` 与 `make install` 只构建并安装 nginx 二进制本身,上面的 `cp` 命令才是将 `grpc.lua` 安装到 Lua 搜索路径(`lua_package_path`)的步骤。

方法
----

### new

**语法:** `cli, err = grpc.new(opts)`

创建一个绑定到单个端点的客户端。`opts` 是一个 Lua 表:

- `host`(string,必填):字面量 IP 地址,不支持域名。
- `port`(number,必填):服务端端口。
- `protos`(string[],必填):`.proto` 文件路径列表,每个客户端只加载一次并缓存。
- `ssl`(boolean,可选):是否启用 TLS 并协商 ALPN `h2`,默认 `false`。
- `ssl_server_name`(string,可选):SNI,默认使用 `host`。
- `timeout`(number,可选):等待下一个事件(消息,或调用结束)的秒数上限,默认 `60`。

同一端点的连接以 worker 为粒度池化;每次请求调用 `new()` 不一定会新建 TCP 连接。

### close

**语法:** `cli:close()`

释放客户端,幂等;客户端被垃圾回收时也会自动调用。

### unary

**语法:** `res, err, code = cli:unary(service, method, data)`

发送一元请求,`service` 形如 `hello.HelloService`,`method` 形如 `SayHello`。`data` 是一个 Lua 表,按 proto 定义编码。

成功时返回解码后的响应表;失败时返回 `nil` 与错误信息,若 gRPC 状态码非 0,第三个返回值为该状态码。

### server_streaming

**语法:** `it = cli:server_streaming(service, method, data)`

发送 server streaming 请求,返回一个迭代器函数。应直接在循环中调用它,而非使用 `for ... in`:普通的 `for msg, err in it do ... end` 一旦第一个返回值为 `nil` 就会终止,同时静默丢弃随之而来的错误信息。

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

**语法:** `writer, err = cli:client_streaming(service, method)`

发起 client streaming 调用,返回一个 writer:

- `writer:send(data)` 发送一条请求消息,可重复调用。
- `writer:result()` 标记请求侧结束,并阻塞等待服务端返回的唯一响应,返回值形式与 `unary` 相同。

```lua
local writer, err = cli:client_streaming("hello.HelloService", "LotsOfGreetings")
writer:send({ greeting = "msg1" })
writer:send({ greeting = "msg2" })
writer:send({ greeting = "msg3" })

local res, err = writer:result()
```

### streaming

**语法:** `writer, err = cli:streaming(service, method)`

发起双向流式调用,返回一个 writer:

- `writer:send(data)` 发送一条请求消息。
- `writer:done_sending()` 标记不再发送请求消息。
- `writer:recv()` 阻塞等待下一条响应消息,流正常结束时返回 `nil`(不带错误),与 `server_streaming` 迭代器的 nil/err 约定一致。

`:send()` 与 `:recv()` 是两个互不阻塞的独立调用:

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
