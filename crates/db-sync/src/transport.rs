use alloc::vec::Vec;

pub trait SyncTransport {
    type Error;

    fn receive(&mut self) -> impl Future<Output = Result<Vec<u8>, Self::Error>>;
    fn send(&mut self, frame: Vec<u8>) -> impl Future<Output = Result<(), Self::Error>>;
}
