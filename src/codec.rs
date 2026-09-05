//! gRPC message codec: pure `Bytes` passthrough.
//!
//! Protobuf encoding and decoding happen in Lua, via lua-protobuf. What
//! stays on the Rust side is the gRPC Length-Prefixed-Message framing
//! (1 byte compression flag, 4 byte big-endian length, then payload),
//! handled by tonic's `Encoder`/`Decoder` machinery. A message frame's
//! boundary can span multiple HTTP/2 DATA frames, and reassembling that
//! requires the connection's byte stream, which Lua never sees.
//!
//! `decode` uses `copy_to_bytes` rather than `chunk()` plus a manual
//! copy: when the decode buffer is backed by a plain `Bytes` (the usual
//! case for tonic), this is a real `Bytes::split_to` under the hood,
//! O(1) and refcounted, not a memcpy.

use bytes::{Buf, BufMut, Bytes};

#[derive(Clone, Copy, Default)]
pub struct BytesCodec;

impl tonic::codec::Codec for BytesCodec {
  type Encode = Bytes;
  type Decode = Bytes;

  type Encoder = BytesEncoder;
  type Decoder = BytesDecoder;

  fn encoder(&mut self) -> Self::Encoder {
    BytesEncoder
  }

  fn decoder(&mut self) -> Self::Decoder {
    BytesDecoder
  }
}

#[derive(Clone, Copy, Default)]
pub struct BytesEncoder;

impl tonic::codec::Encoder for BytesEncoder {
  type Item = Bytes;
  type Error = tonic::Status;

  fn encode(
    &mut self,
    item: Self::Item,
    dst: &mut tonic::codec::EncodeBuf,
  ) -> Result<(), Self::Error> {
    // This one copy is unavoidable here: `item` and `dst` are backed by
    // different allocations (`dst` is tonic's shared frame-encode
    // buffer), and tonic's `Encoder` trait gives no way to hand it an
    // already-owned `Bytes` instead of copying into its buffer.
    dst.put_slice(&item);
    Ok(())
  }
}

#[derive(Clone, Copy, Default)]
pub struct BytesDecoder;

impl tonic::codec::Decoder for BytesDecoder {
  type Item = Bytes;
  type Error = tonic::Status;

  fn decode(
    &mut self,
    src: &mut tonic::codec::DecodeBuf,
  ) -> Result<Option<Self::Item>, Self::Error> {
    let len = src.remaining();
    Ok(Some(src.copy_to_bytes(len)))
  }
}
