use core::future::Future;

use futures::executor::block_on;

/// Blocks on a future using the embedded executor; the entry point for
/// generated test functions.
pub fn run<T>(future: impl Future<Output = T>) -> T {
    block_on(future)
}
