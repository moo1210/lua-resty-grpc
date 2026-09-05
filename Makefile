# This Makefile builds the Rust crate (target/release/librestygrpc.a)
# for local iteration: cargo check/clippy/fmt/test against the crate on
# its own. The nginx module itself is built and linked via nginx's
# --add-module flow (see `config`):
#
#   ./configure --add-module=/path/to/lua-resty-grpc ...
#   make && make install
#
# from an nginx (or openresty) source tree. The `config` script invokes
# `cargo build --release` itself, so a plain --add-module build does not
# need this Makefile run first.
#
# lualib/resty/grpc.lua is installed alongside the resulting nginx or
# openresty by whatever assembles the full runtime (e.g.
# apisix-build-tools), the same way it already does for
# lua-resty-events.

.PHONY: all
all: build

.PHONY: build
build:
	@utils/check-rust.sh
	cargo build --release

.PHONY: check
check:
	cargo fmt --check
	cargo clippy --release -- -D warnings

.PHONY: fmt
fmt:
	cargo fmt

.PHONY: test
test:
	cargo test --release

.PHONY: clean
clean:
	cargo clean

### help:         Show Makefile rules
.PHONY: help
help:
	@echo Makefile rules:
	@echo
	@grep -E '^### [-A-Za-z0-9_]+:' Makefile | sed 's/###/   /'
