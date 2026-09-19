use core::{cell::Cell, fmt};
use std::rc::Rc;

use db_engine::{DirectRowCodec, Engine, InMemoryKernel};
use db_schema::{ColumnSchema, TableSchema};
use db_sync::{SessionConfig, SyncError, SyncResult, SyncRole, SyncTransport, synchronize};
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
}

impl SyncTransport for ChannelTransport {
    type Error = Closed;

    async fn receive(&mut self) -> Result<Vec<u8>, Self::Error> {
        self.receiver.next().await.ok_or(Closed)
    }

    async fn send(&mut self, frame: Vec<u8>) -> Result<(), Self::Error> {
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
        },
        ChannelTransport {
            receiver: right_receiver,
            sender: right_sender,
        },
    )
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
        K: db_engine::Kernel,
        R: db_engine::RowCodec<K::Transaction>,
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
    SessionConfig::new([1; 32], [2; 32])
}

#[test]
fn synchronizes_on_connect() {
    block_on(async {
        let left = Engine::new(InMemoryKernel::new(), DirectRowCodec);
        let right = Engine::new(InMemoryKernel::new(), DirectRowCodec);
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
            left.frontier().await.unwrap(),
            right.frontier().await.unwrap()
        );
    });
}

#[test]
fn coalesces_duplicate_requests() {
    block_on(async {
        let left = Engine::new(InMemoryKernel::new(), DirectRowCodec);
        let right = Engine::new(InMemoryKernel::new(), DirectRowCodec);
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
fn request_during_a_session_runs_once_more() {
    block_on(async {
        let left = Engine::new(InMemoryKernel::new(), DirectRowCodec);
        let right = Engine::new(InMemoryKernel::new(), DirectRowCodec);
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
        let left = Engine::new(InMemoryKernel::new(), DirectRowCodec);
        let right = Engine::new(InMemoryKernel::new(), DirectRowCodec);
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
        let engine = Engine::new(InMemoryKernel::new(), DirectRowCodec);
        engine.create_table(table("users")).await.unwrap();
        let frontier = engine.frontier().await.unwrap();
        let (transport, _) = Connection::connected(FailingTransport);
        let mut connection = transport;

        let error = connection
            .synchronize(&engine, &config(), SyncRole::Initiator)
            .await
            .unwrap_err();

        assert!(matches!(error, SyncError::Transport(Closed)));
        assert_eq!(engine.frontier().await.unwrap(), frontier);
    });
}
