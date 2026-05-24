pub fn block_on<F: core::future::Future>(future: F) -> F::Output {
  use core::pin::pin;
  use core::task::{Context, Poll, Waker};

  let waker = Waker::noop();
  let mut context = Context::from_waker(waker);
  let mut future = pin!(future);

  loop {
    match future.as_mut().poll(&mut context) {
      Poll::Ready(output) => return output,
      Poll::Pending => {
        if cfg!(feature = "std") {
          std::thread::yield_now();
        } else {
          core::hint::spin_loop();
        }
      }
    }
  }
}

#[cfg(test)]
mod tests {
  use super::block_on;

  #[test]
  fn ready_future_returns_output() {
    assert_eq!(block_on(async { 42_u8 }), 42);
  }
}
