use core::fmt;

use futures::{StreamExt, channel::mpsc};

use db_sync::SyncTransport;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum InMemoryTransportError {
    Closed,
    Interrupted,
}

impl fmt::Display for InMemoryTransportError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Closed => "channel closed",
            Self::Interrupted => "transport interrupted",
        })
    }
}

pub struct InMemoryTransport {
    receiver: mpsc::UnboundedReceiver<Vec<u8>>,
    sender: Option<mpsc::UnboundedSender<Vec<u8>>>,
    fail_at: Option<usize>,
    sent: usize,
}

impl SyncTransport for InMemoryTransport {
    type Error = InMemoryTransportError;

    async fn receive(&mut self) -> Result<Vec<u8>, Self::Error> {
        self.receiver
            .next()
            .await
            .ok_or(InMemoryTransportError::Closed)
    }

    async fn send(&mut self, frame: Vec<u8>) -> Result<(), Self::Error> {
        if self.fail_at == Some(self.sent) {
            self.sender = None;
            return Err(InMemoryTransportError::Interrupted);
        }
        self.sent += 1;
        self.sender
            .as_ref()
            .ok_or(InMemoryTransportError::Closed)?
            .unbounded_send(frame)
            .map_err(|_| InMemoryTransportError::Closed)
    }
}

pub fn in_memory_transport_pair() -> (InMemoryTransport, InMemoryTransport) {
    in_memory_transport_pair_failing_at(None)
}

pub fn in_memory_transport_pair_failing_at(
    fail_at: Option<usize>,
) -> (InMemoryTransport, InMemoryTransport) {
    let (left_sender, right_receiver) = mpsc::unbounded();
    let (right_sender, left_receiver) = mpsc::unbounded();
    (
        InMemoryTransport {
            receiver: left_receiver,
            sender: Some(left_sender),
            fail_at,
            sent: 0,
        },
        InMemoryTransport {
            receiver: right_receiver,
            sender: Some(right_sender),
            fail_at: None,
            sent: 0,
        },
    )
}

#[cfg(test)]
mod tests {
    use futures::executor::block_on;

    use super::{InMemoryTransportError, SyncTransport, in_memory_transport_pair_failing_at};

    #[test]
    fn failed_send_closes_the_peer() {
        block_on(async {
            let (mut left, mut right) = in_memory_transport_pair_failing_at(Some(0));

            assert_eq!(
                left.send(vec![]).await,
                Err(InMemoryTransportError::Interrupted)
            );
            assert_eq!(right.receive().await, Err(InMemoryTransportError::Closed));
        });
    }
}
