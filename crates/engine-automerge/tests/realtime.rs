use core::{cell::Cell, fmt};
use std::{cell::RefCell, rc::Rc};

use engine::{Engine, InMemoryKernel};
use engine_automerge::AutomergeRowCodec;
use futures::{StreamExt, channel::mpsc, executor::block_on};
use schema::{ColumnSchema, TableSchema};
use sql_translator::SqlTranslator;
use sync::{
    SessionConfig, SyncError, SyncMessage, SyncResult, SyncRole, SyncTransport, synchronize,
};
use value::{Value, ValueType};

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
        let frame = self.receiver.next().await.ok_or(Closed)?;
        Ok(frame)
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

async fn sync(
    left: &Engine<InMemoryKernel, AutomergeRowCodec>,
    right: &Engine<InMemoryKernel, AutomergeRowCodec>,
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

#[derive(Clone, Default)]
struct SyncRequest(Rc<Cell<bool>>);

impl SyncRequest {
    fn request(&self) {
        self.0.set(true);
    }
}

struct Connection<T> {
    transport: T,
    request: SyncRequest,
}

impl<T> Connection<T> {
    fn connected(transport: T) -> (Self, SyncRequest) {
        let request = SyncRequest::default();
        request.request();
        (
            Self {
                transport,
                request: request.clone(),
            },
            request,
        )
    }

    fn pending(&self) -> bool {
        self.request.0.get()
    }

    async fn synchronize<K, R>(
        &mut self,
        engine: &Engine<K, R>,
        config: &SessionConfig,
        role: SyncRole,
    ) -> Result<Option<SyncResult>, SyncError<T::Error>>
    where
        K: engine::Kernel,
        R: sync::SyncRowCodec<K::Transaction>,
        T: SyncTransport,
        T::Error: fmt::Display,
    {
        if !self.request.0.replace(false) {
            return Ok(None);
        }

        synchronize(engine, &mut self.transport, config, role)
            .await
            .map(Some)
    }
}

struct RequestOnFirstSend {
    inner: ChannelTransport,
    request: SyncRequest,
    requested: bool,
}

impl SyncTransport for RequestOnFirstSend {
    type Error = Closed;

    async fn receive(&mut self) -> Result<Vec<u8>, Self::Error> {
        self.inner.receive().await
    }

    async fn send(&mut self, frame: Vec<u8>) -> Result<(), Self::Error> {
        self.inner.send(frame).await?;
        if !self.requested {
            self.request.request();
            self.requested = true;
        }
        Ok(())
    }
}

fn people_table() -> TableSchema {
    TableSchema {
        name: "people".into(),
        columns: vec![
            ColumnSchema {
                name: "id".into(),
                r#type: ValueType::Uuid,
                default: Value::Null,
                primary_key: true,
            },
            ColumnSchema {
                name: "name".into(),
                r#type: ValueType::Text,
                default: Value::Null,
                primary_key: false,
            },
        ],
    }
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

fn config() -> SessionConfig {
    SessionConfig::default()
}

#[test]
fn synchronizes_on_connect() {
    block_on(async {
        let left = Engine::new(InMemoryKernel::new(), AutomergeRowCodec::new());
        let right = Engine::new(InMemoryKernel::new(), AutomergeRowCodec::new());
        left.create_table(table("users")).await.unwrap();
        let (left_transport, right_transport) = transport_pair();
        let (mut left_connection, _) = Connection::connected(left_transport);
        let (mut right_connection, _) = Connection::connected(right_transport);
        let config = config();

        let (left_result, right_result) = futures::join!(
            left_connection.synchronize(&left, &config, SyncRole::Initiator),
            right_connection.synchronize(&right, &config, SyncRole::Responder),
        );

        assert!(left_result.unwrap().is_some());
        assert!(right_result.unwrap().is_some());
        assert_eq!(
            sync::sync_manifest_for(&left).await.unwrap(),
            sync::sync_manifest_for(&right).await.unwrap()
        );
    });
}

#[test]
fn coalesces_duplicate_requests() {
    block_on(async {
        let left = Engine::new(InMemoryKernel::new(), AutomergeRowCodec::new());
        let right = Engine::new(InMemoryKernel::new(), AutomergeRowCodec::new());
        let (left_transport, right_transport) = transport_pair();
        let (mut left_connection, left_request) = Connection::connected(left_transport);
        let (mut right_connection, right_request) = Connection::connected(right_transport);
        left_request.request();
        left_request.request();
        right_request.request();
        let config = config();

        let (left_result, right_result) = futures::join!(
            left_connection.synchronize(&left, &config, SyncRole::Initiator),
            right_connection.synchronize(&right, &config, SyncRole::Responder),
        );

        assert!(left_result.unwrap().is_some());
        assert!(right_result.unwrap().is_some());
        assert!(!left_connection.pending());
        assert!(!right_connection.pending());
    });
}

#[test]
fn dependency_failure_triggers_snapshot_recovery() {
    block_on(async {
        let left = Engine::new(InMemoryKernel::new(), AutomergeRowCodec::new());
        let right = Engine::new(InMemoryKernel::new(), AutomergeRowCodec::new());
        left.create_table(people_table()).await.unwrap();
        let translator = SqlTranslator;
        left.translate_and_execute(
            "INSERT INTO people (id, name) VALUES (CAST('018f0f8e-7b6d-7c4a-8f12-123456789abc' AS UUID), 'Ada');",
            &translator,
        )
        .await
        .unwrap();
        let _ = sync(&left, &right, &SessionConfig::default()).await;

        right
            .translate_and_execute(
                "DELETE FROM people WHERE id = CAST('018f0f8e-7b6d-7c4a-8f12-123456789abc' AS UUID);",
                &translator,
            )
            .await
            .unwrap();
        left.translate_and_execute(
            "UPDATE people SET name = 'Grace' WHERE id = CAST('018f0f8e-7b6d-7c4a-8f12-123456789abc' AS UUID);",
            &translator,
        )
        .await
        .unwrap();

        let frames = sync(&left, &right, &SessionConfig::default()).await;
        assert!(
            frames
                .iter()
                .any(|frame| matches!(frame, SyncMessage::Changes(_)))
        );
        assert!(
            frames
                .iter()
                .any(|frame| matches!(frame, SyncMessage::RequestSnapshots(_)))
        );
        assert!(
            frames
                .iter()
                .any(|frame| matches!(frame, SyncMessage::State(_)))
        );
    });
}

#[test]
fn request_during_a_session_runs_once_more() {
    block_on(async {
        let left = Engine::new(InMemoryKernel::new(), AutomergeRowCodec::new());
        let right = Engine::new(InMemoryKernel::new(), AutomergeRowCodec::new());
        let (left_transport, right_transport) = transport_pair();
        let left_request = SyncRequest::default();
        left_request.request();
        let mut left_connection = Connection {
            transport: RequestOnFirstSend {
                inner: left_transport,
                request: left_request.clone(),
                requested: false,
            },
            request: left_request,
        };
        let (mut right_connection, right_request) = Connection::connected(right_transport);
        let config = config();

        let (left_result, right_result) = futures::join!(
            left_connection.synchronize(&left, &config, SyncRole::Initiator),
            right_connection.synchronize(&right, &config, SyncRole::Responder),
        );

        assert!(left_result.unwrap().is_some());
        assert!(right_result.unwrap().is_some());
        assert!(left_connection.pending());
        right_request.request();
        let (left_result, right_result) = futures::join!(
            left_connection.synchronize(&left, &config, SyncRole::Initiator),
            right_connection.synchronize(&right, &config, SyncRole::Responder),
        );
        assert!(left_result.unwrap().is_some());
        assert!(right_result.unwrap().is_some());
        assert!(!left_connection.pending());
    });
}

#[test]
fn periodic_repair_after_reconnect_converges_offline_writes() {
    block_on(async {
        let left = Engine::new(InMemoryKernel::new(), AutomergeRowCodec::new());
        let right = Engine::new(InMemoryKernel::new(), AutomergeRowCodec::new());
        let (left_transport, right_transport) = transport_pair();
        let (mut left_connection, _) = Connection::connected(left_transport);
        let (mut right_connection, _) = Connection::connected(right_transport);
        let config = config();

        let (left_result, right_result) = futures::join!(
            left_connection.synchronize(&left, &config, SyncRole::Initiator),
            right_connection.synchronize(&right, &config, SyncRole::Responder),
        );
        left_result.unwrap();
        right_result.unwrap();
        drop(left_connection);
        drop(right_connection);

        left.create_table(table("left_only")).await.unwrap();
        right.create_table(table("right_only")).await.unwrap();
        let (left_transport, right_transport) = transport_pair();
        let (mut left_connection, _) = Connection::connected(left_transport);
        let (mut right_connection, _) = Connection::connected(right_transport);

        let (left_result, right_result) = futures::join!(
            left_connection.synchronize(&left, &config, SyncRole::Initiator),
            right_connection.synchronize(&right, &config, SyncRole::Responder),
        );

        left_result.unwrap();
        right_result.unwrap();
        assert_eq!(
            sync::sync_manifest_for(&left).await.unwrap(),
            sync::sync_manifest_for(&right).await.unwrap()
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

struct FailingTransport;

impl SyncTransport for FailingTransport {
    type Error = Closed;

    async fn receive(&mut self) -> Result<Vec<u8>, Self::Error> {
        Err(Closed)
    }

    async fn send(&mut self, _: Vec<u8>) -> Result<(), Self::Error> {
        Err(Closed)
    }
}

#[test]
fn transport_failure_leaves_the_engine_unchanged() {
    block_on(async {
        let engine = Engine::new(InMemoryKernel::new(), AutomergeRowCodec::new());
        engine.create_table(table("users")).await.unwrap();
        let manifest = sync::sync_manifest_for(&engine).await.unwrap();
        let (transport, _) = Connection::connected(FailingTransport);
        let mut connection = transport;

        let error = connection
            .synchronize(&engine, &config(), SyncRole::Initiator)
            .await
            .unwrap_err();

        assert!(matches!(error, SyncError::Transport(Closed)));
        assert_eq!(sync::sync_manifest_for(&engine).await.unwrap(), manifest);
    });
}
