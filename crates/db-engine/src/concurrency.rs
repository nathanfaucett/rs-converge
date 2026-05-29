use core::future::Future;

use futures::Stream;

#[cfg(target_arch = "wasm32")]
pub trait MaybeSend {}

#[cfg(target_arch = "wasm32")]
impl<T> MaybeSend for T {}

#[cfg(not(target_arch = "wasm32"))]
pub trait MaybeSend: Send {}

#[cfg(not(target_arch = "wasm32"))]
impl<T> MaybeSend for T where T: Send {}

#[cfg(target_arch = "wasm32")]
pub trait MaybeSync {}

#[cfg(target_arch = "wasm32")]
impl<T> MaybeSync for T {}

#[cfg(not(target_arch = "wasm32"))]
pub trait MaybeSync: Sync {}

#[cfg(not(target_arch = "wasm32"))]
impl<T> MaybeSync for T where T: Sync {}

#[cfg(target_arch = "wasm32")]
pub trait MaybeSendFuture: Future {}

#[cfg(target_arch = "wasm32")]
impl<T> MaybeSendFuture for T where T: Future {}

#[cfg(not(target_arch = "wasm32"))]
pub trait MaybeSendFuture: Future + Send {}

#[cfg(not(target_arch = "wasm32"))]
impl<T> MaybeSendFuture for T where T: Future + Send {}

#[cfg(target_arch = "wasm32")]
pub trait MaybeSendStream: Stream {}

#[cfg(target_arch = "wasm32")]
impl<T> MaybeSendStream for T where T: Stream {}

#[cfg(not(target_arch = "wasm32"))]
pub trait MaybeSendStream: Stream + Send {}

#[cfg(not(target_arch = "wasm32"))]
impl<T> MaybeSendStream for T where T: Stream + Send {}
