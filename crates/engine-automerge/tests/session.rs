use core::fmt;
use std::{cell::RefCell, rc::Rc};

use engine::{Engine, InMemoryKernel};
use engine_automerge::AutomergeRowCodec;
use futures::{StreamExt, channel::mpsc, executor::block_on};
use schema::{ColumnSchema, TableSchema};
use sql_translator::SqlTranslator;
use sync::{SessionConfig, SyncMessage, SyncRole, SyncRowCodec, SyncTransport, synchronize};
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

#[test]
fn synchronizes_catalog_state_and_converges() {
    block_on(async {
        let left = Engine::new(InMemoryKernel::new(), AutomergeRowCodec::new());
        let right = Engine::new(InMemoryKernel::new(), AutomergeRowCodec::new());
        left.create_table(table("users")).await.unwrap();

        let frames = sync(&left, &right, &SessionConfig::default()).await;

        assert_eq!(right.table_schema("users").await.unwrap(), table("users"));
        assert!(
            frames
                .iter()
                .any(|frame| matches!(frame, SyncMessage::State(_)))
        );
        assert_eq!(
            sync::sync_manifest_for(&left).await.unwrap(),
            sync::sync_manifest_for(&right).await.unwrap()
        );
    });
}

#[test]
fn realtime_update_sends_one_incremental_without_snapshot() {
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
        left.translate_and_execute(
            "UPDATE people SET name = 'Grace' WHERE id = CAST('018f0f8e-7b6d-7c4a-8f12-123456789abc' AS UUID);",
            &translator,
        )
        .await
        .unwrap();

        let (mut left_transport, mut right_transport) = transport_pair();
        let config = SessionConfig::default();
        let (left_result, right_result) = futures::join!(
            synchronize(&left, &mut left_transport, &config, SyncRole::Initiator),
            synchronize(&right, &mut right_transport, &config, SyncRole::Responder),
        );
        let left_result = left_result.unwrap();
        right_result.unwrap();
        let frames = left_transport.sent.borrow().clone();
        assert_eq!(left_result.sent_changes, 1);
        assert_eq!(left_result.sent_snapshots, 0);
        assert!(
            frames
                .iter()
                .any(|frame| matches!(frame, SyncMessage::Changes(changes) if changes.len() == 1))
        );
        assert!(
            frames
                .iter()
                .all(|frame| !matches!(frame, SyncMessage::State(_)))
        );
        assert_eq!(
            sync::sync_manifest_for(&left).await.unwrap(),
            sync::sync_manifest_for(&right).await.unwrap()
        );
    });
}

#[test]
fn propagates_row_deletion_without_resurrecting_the_row() {
    block_on(async {
        let left = Engine::new(InMemoryKernel::new(), AutomergeRowCodec::new());
        let right = Engine::new(InMemoryKernel::new(), AutomergeRowCodec::new());
        let translator = SqlTranslator;
        left.create_table(people_table()).await.unwrap();
        left.translate_and_execute(
            "INSERT INTO people (id, name) VALUES (CAST('018f0f8e-7b6d-7c4a-8f12-123456789abc' AS UUID), 'Ada');",
            &translator,
        )
        .await
        .unwrap();
        sync(&left, &right, &SessionConfig::default()).await;

        left.translate_and_execute(
            "DELETE FROM people WHERE id = CAST('018f0f8e-7b6d-7c4a-8f12-123456789abc' AS UUID);",
            &translator,
        )
        .await
        .unwrap();
        sync(&left, &right, &SessionConfig::default()).await;

        let rows = right
            .translate_and_execute("SELECT * FROM people;", &translator)
            .await
            .unwrap();
        assert!(rows.iter().all(|result| result.rows.is_empty()));
    });
}

#[test]
fn synchronizes_concurrent_branches() {
    block_on(async {
        let left = Engine::new(InMemoryKernel::new(), AutomergeRowCodec::new());
        let right = Engine::new(InMemoryKernel::new(), AutomergeRowCodec::new());
        let translator = SqlTranslator;
        left.create_table(people_table()).await.unwrap();
        left.translate_and_execute(
            "INSERT INTO people (id, name) VALUES (CAST('018f0f8e-7b6d-7c4a-8f12-123456789abc' AS UUID), 'Ada');",
            &translator,
        )
        .await
        .unwrap();
        sync(&left, &right, &SessionConfig::default()).await;

        left.translate_and_execute(
            "UPDATE people SET name = 'Grace' WHERE id = CAST('018f0f8e-7b6d-7c4a-8f12-123456789abc' AS UUID);",
            &translator,
        )
        .await
        .unwrap();
        right.translate_and_execute(
            "UPDATE people SET name = 'Lin' WHERE id = CAST('018f0f8e-7b6d-7c4a-8f12-123456789abc' AS UUID);",
            &translator,
        )
        .await
        .unwrap();
        sync(&left, &right, &SessionConfig::default()).await;

        assert_eq!(
            sync::sync_manifest_for(&left).await.unwrap(),
            sync::sync_manifest_for(&right).await.unwrap()
        );
    });
}

#[test]
fn duplicate_incremental_frames_are_idempotent() {
    block_on(async {
        let left = Engine::new(InMemoryKernel::new(), AutomergeRowCodec::new());
        let right = Engine::new(InMemoryKernel::new(), AutomergeRowCodec::new());
        let translator = SqlTranslator;
        left.create_table(people_table()).await.unwrap();
        left.translate_and_execute(
            "INSERT INTO people (id, name) VALUES (CAST('018f0f8e-7b6d-7c4a-8f12-123456789abc' AS UUID), 'Ada');",
            &translator,
        )
        .await
        .unwrap();
        sync(&left, &right, &SessionConfig::default()).await;
        left.translate_and_execute(
            "UPDATE people SET name = 'Grace' WHERE id = CAST('018f0f8e-7b6d-7c4a-8f12-123456789abc' AS UUID);",
            &translator,
        )
        .await
        .unwrap();

        let (mut left_transport, mut right_transport) = transport_pair();
        let config = SessionConfig::default();
        let (left_result, right_result) = futures::join!(
            synchronize(&left, &mut left_transport, &config, SyncRole::Initiator),
            synchronize(&right, &mut right_transport, &config, SyncRole::Responder),
        );
        left_result.unwrap();
        right_result.unwrap();
        let change = left_transport
            .sent
            .borrow()
            .iter()
            .find_map(|message| match message {
                SyncMessage::Changes(changes) => changes.first().cloned(),
                _ => None,
            })
            .unwrap();
        let table = change.table;
        let row = change.row;
        let id = change.id;
        let payload = change.payload;
        right
            .mutate_transaction(table, row, move |codec, transaction, _| {
                Box::pin(async move {
                    let value = codec
                        .apply_change(transaction, table.0, row, &id, &payload)
                        .await?;
                    Ok(((), value))
                })
            })
            .await
            .unwrap();
        assert_eq!(
            sync::sync_manifest_for(&right).await.unwrap(),
            sync::sync_manifest_for(&left).await.unwrap()
        );
    });
}

#[test]
fn batches_state_units_without_checkpoint_or_envelope_messages() {
    block_on(async {
        let left = Engine::new(InMemoryKernel::new(), AutomergeRowCodec::new());
        let right = Engine::new(InMemoryKernel::new(), AutomergeRowCodec::new());
        for name in ["one", "two", "three"] {
            left.create_table(table(name)).await.unwrap();
        }

        let frames = sync(
            &left,
            &right,
            &SessionConfig {
                max_units_per_frame: 2,
            },
        )
        .await;
        let batches = frames
            .iter()
            .filter_map(|frame| match frame {
                SyncMessage::State(units) => Some(units.len()),
                _ => None,
            })
            .collect::<Vec<_>>();

        assert!(batches.len() > 1);
        assert!(batches.iter().all(|size| *size <= 2));
        assert!(frames.iter().all(|frame| {
            matches!(
                frame,
                SyncMessage::Hello(_)
                    | SyncMessage::Manifest(_)
                    | SyncMessage::Inventory(_)
                    | SyncMessage::State(_)
                    | SyncMessage::Changes(_)
                    | SyncMessage::RequestSnapshots(_)
                    | SyncMessage::Done
            )
        }));
    });
}
