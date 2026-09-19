use core::fmt;
use std::{cell::RefCell, rc::Rc};

use db_engine::{DirectRowCodec, Engine, Frontier, InMemoryKernel};
use db_schema::{ColumnSchema, TableSchema};
use db_sync::{
    PROTOCOL_VERSION, SessionConfig, SyncError, SyncHello, SyncMessage, SyncRole, SyncTransport,
    synchronize,
};
use db_value::{Value, ValueType};
use futures::{StreamExt, channel::mpsc, executor::block_on};

#[derive(Debug)]
struct Closed;

impl fmt::Display for Closed {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("channel closed")
    }
}

struct ChannelTransport {
    receiver: mpsc::UnboundedReceiver<Vec<u8>>,
    sender: mpsc::UnboundedSender<Vec<u8>>,
    sent: Rc<RefCell<Vec<SyncMessage>>>,
}

impl SyncTransport for ChannelTransport {
    type Error = Closed;

    async fn receive(&mut self) -> Result<Vec<u8>, Self::Error> {
        self.receiver.next().await.ok_or(Closed)
    }

    async fn send(&mut self, frame: Vec<u8>) -> Result<(), Self::Error> {
        self.sent
            .borrow_mut()
            .push(postcard::from_bytes(&frame).unwrap());
        self.sender.unbounded_send(frame).map_err(|_| Closed)
    }
}

fn transport_pair() -> (ChannelTransport, ChannelTransport) {
    let (left_sender, right_receiver) = mpsc::unbounded();
    let (right_sender, left_receiver) = mpsc::unbounded();
    (
        ChannelTransport {
            receiver: left_receiver,
            sender: left_sender,
            sent: Rc::new(RefCell::new(Vec::new())),
        },
        ChannelTransport {
            receiver: right_receiver,
            sender: right_sender,
            sent: Rc::new(RefCell::new(Vec::new())),
        },
    )
}

fn table(name: &str) -> TableSchema {
    TableSchema {
        name: name.into(),
        columns: vec![ColumnSchema {
            name: "id".into(),
            r#type: ValueType::Uuid,
            default: Value::Null,
            primary_key: true,
        }],
    }
}

async fn sync(
    left: &Engine<InMemoryKernel, DirectRowCodec>,
    right: &Engine<InMemoryKernel, DirectRowCodec>,
) {
    let config = SessionConfig::new();
    let _ = sync_with(left, right, &config).await;
}

async fn sync_with(
    left: &Engine<InMemoryKernel, DirectRowCodec>,
    right: &Engine<InMemoryKernel, DirectRowCodec>,
    config: &SessionConfig,
) -> Vec<SyncMessage> {
    let (mut left_transport, mut right_transport) = transport_pair();
    let (left_result, right_result) = futures::join!(
        synchronize(left, &mut left_transport, config, SyncRole::Initiator),
        synchronize(right, &mut right_transport, config, SyncRole::Responder),
    );
    left_result.unwrap();
    right_result.unwrap();
    left_transport.sent.borrow().clone()
}

struct WriteAfterCheckpoint<'a> {
    inner: ChannelTransport,
    engine: &'a Engine<InMemoryKernel, DirectRowCodec>,
    wrote: bool,
}

impl SyncTransport for WriteAfterCheckpoint<'_> {
    type Error = Closed;

    async fn receive(&mut self) -> Result<Vec<u8>, Self::Error> {
        self.inner.receive().await
    }

    async fn send(&mut self, frame: Vec<u8>) -> Result<(), Self::Error> {
        let checkpoint = matches!(postcard::from_bytes(&frame), Ok(SyncMessage::Checkpoint(_)));
        self.inner.send(frame).await?;
        if checkpoint && !self.wrote {
            self.engine
                .create_table(table("written_during_bootstrap"))
                .await
                .unwrap();
            self.wrote = true;
        }
        Ok(())
    }
}

#[test]
fn catches_up_a_new_replica() {
    block_on(async {
        let left = Engine::new(InMemoryKernel::new(), DirectRowCodec);
        let right = Engine::new(InMemoryKernel::new(), DirectRowCodec);
        left.create_table(table("users")).await.unwrap();

        sync(&left, &right).await;

        assert_eq!(
            left.frontier().await.unwrap(),
            right.frontier().await.unwrap()
        );
        assert_eq!(right.table_schema("users").await.unwrap(), table("users"));
    });
}

#[test]
fn converges_offline_concurrent_writes() {
    block_on(async {
        let left = Engine::new(InMemoryKernel::new(), DirectRowCodec);
        let right = Engine::new(InMemoryKernel::new(), DirectRowCodec);
        left.create_table(table("users")).await.unwrap();
        sync(&left, &right).await;

        left.create_table(table("left_only")).await.unwrap();
        right.create_table(table("right_only")).await.unwrap();
        sync(&left, &right).await;

        assert_eq!(
            left.frontier().await.unwrap(),
            right.frontier().await.unwrap()
        );
        assert_eq!(
            left.table_schema("right_only").await.unwrap(),
            table("right_only")
        );
        assert_eq!(
            right.table_schema("left_only").await.unwrap(),
            table("left_only")
        );
    });
}

#[test]
fn bootstraps_an_empty_replica_with_a_checkpoint() {
    block_on(async {
        let left = Engine::new(InMemoryKernel::new(), DirectRowCodec);
        let right = Engine::new(InMemoryKernel::new(), DirectRowCodec);
        left.create_table(table("users")).await.unwrap();
        let mut config = SessionConfig::new();
        config.checkpoint_threshold = Some(0);

        let frames = sync_with(&left, &right, &config).await;

        assert!(
            frames
                .iter()
                .any(|frame| matches!(frame, SyncMessage::Checkpoint(_)))
        );
        assert_eq!(right.table_schema("users").await.unwrap(), table("users"));
    });
}

#[test]
fn disabled_threshold_uses_envelopes_only() {
    block_on(async {
        let left = Engine::new(InMemoryKernel::new(), DirectRowCodec);
        let right = Engine::new(InMemoryKernel::new(), DirectRowCodec);
        left.create_table(table("users")).await.unwrap();

        let frames = sync_with(&left, &right, &SessionConfig::new()).await;

        assert!(
            !frames
                .iter()
                .any(|frame| matches!(frame, SyncMessage::Checkpoint(_)))
        );
    });
}

#[test]
fn transfers_writes_made_during_checkpoint_bootstrap() {
    block_on(async {
        let left = Engine::new(InMemoryKernel::new(), DirectRowCodec);
        let right = Engine::new(InMemoryKernel::new(), DirectRowCodec);
        left.create_table(table("users")).await.unwrap();
        let mut config = SessionConfig::new();
        config.checkpoint_threshold = Some(0);
        let (left_transport, mut right_transport) = transport_pair();
        let mut left_transport = WriteAfterCheckpoint {
            inner: left_transport,
            engine: &left,
            wrote: false,
        };

        let (left_result, right_result) = futures::join!(
            synchronize(&left, &mut left_transport, &config, SyncRole::Initiator),
            synchronize(&right, &mut right_transport, &config, SyncRole::Responder),
        );

        left_result.unwrap();
        right_result.unwrap();
        assert!(left_transport.wrote);
        assert_eq!(
            right
                .table_schema("written_during_bootstrap")
                .await
                .unwrap(),
            table("written_during_bootstrap")
        );
    });
}

#[test]
fn rejects_malformed_checkpoint_frames() {
    block_on(async {
        let engine = Engine::new(InMemoryKernel::new(), DirectRowCodec);
        engine.create_table(table("users")).await.unwrap();
        let mut config = SessionConfig::new();
        config.checkpoint_threshold = Some(0);
        let (mut transport, peer) = transport_pair();
        let hello = SyncMessage::Hello(SyncHello {
            protocol_version: PROTOCOL_VERSION,
            frontier: Frontier::default(),
        });
        for message in [
            hello,
            SyncMessage::Frontier(Frontier::default()),
            SyncMessage::Checkpoint(vec![0]),
        ] {
            peer.sender
                .unbounded_send(postcard::to_allocvec(&message).unwrap())
                .unwrap();
        }

        let error = synchronize(&engine, &mut transport, &config, SyncRole::Initiator)
            .await
            .unwrap_err();

        assert!(matches!(error, SyncError::Protocol(_)));
    });
}
