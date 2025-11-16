use std::os::raw::c_char;
use std::{
  cell::RefCell,
  pin::Pin,
  rc::Rc,
  task::{Context, Poll, Waker},
};

use tokio::io::ReadBuf;

pub struct MemoryShim {
  pub read_buf: Rc<RefCell<Vec<u8>>>,
  pub waker: Rc<RefCell<Option<Waker>>>,
  pub on_socket_write_cb: crate::grpc_client_on_reply,
}

impl tokio::io::AsyncRead for MemoryShim {
  fn poll_read(
    self: Pin<&mut Self>,
    cx: &mut Context<'_>,
    buf: &mut ReadBuf<'_>,
  ) -> Poll<std::io::Result<()>> {
    let mut rb = self.read_buf.borrow_mut();
    if rb.is_empty() {
      *self.waker.borrow_mut() = Some(cx.waker().clone());
      // pending until new bytes arrive from C side
      return Poll::Pending;
    }
    let n = std::cmp::min(buf.remaining(), rb.len());
    buf.put_slice(&rb[..n]);
    rb.drain(..n);
    // read fulfilled
    Poll::Ready(Ok(()))
  }
}

impl tokio::io::AsyncWrite for MemoryShim {
  fn poll_write(
    self: Pin<&mut Self>,
    _cx: &mut Context<'_>,
    data: &[u8],
  ) -> Poll<std::io::Result<usize>> {
    unsafe {
      (self.on_socket_write_cb)(data.as_ptr() as *const c_char, data.len());
    }
    Poll::Ready(Ok(data.len()))
  }

  fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
    // Data is always sent immediately without requiring a flush.
    // Technically, once the buffer on the cosocket side is full, OpenResty will suspend the sock:write call.
    // Then, the Rust-side callback call stack will also pause until sock:write returns.
    // We always ensure a batch of data is sent within the on_socket_write callback,
    // thus this flush is unnecessary.
    Poll::Ready(Ok(()))
  }

  fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
    Poll::Ready(Ok(()))
  }
}
