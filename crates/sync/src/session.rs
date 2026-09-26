use alloc::{
    boxed::Box,
    collections::BTreeMap,
    format,
    string::{String, ToString},
    vec,
    vec::Vec,
};

use engine::{CatalogEntry, CatalogEntryKind, Engine, EngineError, Kernel};
use thiserror::Error;

use crate::{
    PROTOCOL_VERSION, SyncHello, SyncIncrementalChange, SyncKey, SyncManifest, SyncMessage,
    SyncRowCodec, SyncRowInventory, SyncSnapshotRequest, SyncStateUnit, SyncTransport,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SyncRole {
    Initiator,
    Responder,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SessionConfig {
    pub max_units_per_frame: usize,
}

impl Default for SessionConfig {
    fn default() -> Self {
        Self {
            max_units_per_frame: 64,
        }
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct SyncResult {
    pub received_units: usize,
    pub sent_units: usize,
    pub received_changes: usize,
    pub sent_changes: usize,
    pub received_snapshots: usize,
    pub sent_snapshots: usize,
}

#[derive(Debug, Error)]
pub enum SyncError<E> {
    #[error("engine error: {0}")]
    Engine(#[from] EngineError),

    #[error("transport error: {0}")]
    Transport(E),

    #[error("invalid session configuration")]
    InvalidConfiguration,

    #[error("protocol codec error: {0}")]
    Protocol(String),

    #[error("incompatible protocol version: {0}")]
    IncompatibleProtocol(u16),

    #[error("unexpected sync message")]
    UnexpectedMessage,

    #[error("remote sync aborted: {0}")]
    RemoteAbort(String),
}

pub async fn synchronize<K, R, T>(
    engine: &Engine<K, R>,
    transport: &mut T,
    config: &SessionConfig,
    role: SyncRole,
) -> Result<SyncResult, SyncError<T::Error>>
where
    K: Kernel,
    R: SyncRowCodec<K::Transaction>,
    T: SyncTransport,
    T::Error: core::fmt::Display,
{
    if config.max_units_per_frame == 0 {
        return Err(SyncError::InvalidConfiguration);
    }

    exchange_hello(engine, transport, role).await?;

    let remote_manifest = exchange_manifest(engine, transport, role).await?;
    let local_units = export_sync_state_for(engine).await?;

    let local_inventories = inventories_for(engine, &local_units).await?;

    let remote_inventories =
        exchange_inventories(transport, role, local_inventories.clone()).await?;

    let remote_inventories = remote_inventories
        .into_iter()
        .map(|inventory| (inventory.key(), inventory))
        .collect::<BTreeMap<_, _>>();
    let outbound = build_outbound(engine, local_units, remote_manifest, remote_inventories).await?;

    let (sent, mut received) = match role {
        SyncRole::Initiator => {
            let sent = send_outbound(transport, &outbound, config.max_units_per_frame).await?;
            let received = receive_outbound(engine, transport).await?;
            (sent, received)
        }
        SyncRole::Responder => {
            let received = receive_outbound(engine, transport).await?;
            let sent = send_outbound(transport, &outbound, config.max_units_per_frame).await?;
            (sent, received)
        }
    };

    let remote_requests = exchange_snapshot_requests(transport, role, received.requests).await?;
    let recovery = recovery_snapshots(engine, remote_requests).await?;
    let (sent_recovery, (received_recovery, retried_changes)) = match role {
        SyncRole::Initiator => {
            let sent = send_snapshots(transport, &recovery, config.max_units_per_frame).await?;
            let received = receive_recovery(engine, transport, &mut received.pending).await?;
            (sent, received)
        }
        SyncRole::Responder => {
            let received = receive_recovery(engine, transport, &mut received.pending).await?;
            let sent = send_snapshots(transport, &recovery, config.max_units_per_frame).await?;
            (sent, received)
        }
    };

    Ok(SyncResult {
        sent_units: sent.snapshots + sent_recovery,
        received_units: received.snapshots + received_recovery,
        sent_changes: sent.changes,
        received_changes: received.changes + retried_changes,
        sent_snapshots: sent.snapshots + sent_recovery,
        received_snapshots: received.snapshots + received_recovery,
    })
}

#[derive(Default)]
struct Outbound {
    snapshots: Vec<SyncStateUnit>,
    changes: Vec<SyncIncrementalChange>,
}

#[derive(Default)]
struct TransferCount {
    snapshots: usize,
    changes: usize,
}

struct Received {
    snapshots: usize,
    changes: usize,
    requests: Vec<SyncSnapshotRequest>,
    pending: Vec<SyncIncrementalChange>,
}

pub async fn sync_manifest_for<K, R>(engine: &Engine<K, R>) -> Result<SyncManifest, EngineError>
where
    K: Kernel,
    R: SyncRowCodec<K::Transaction>,
{
    Ok(SyncManifest::new(
        export_sync_state_for(engine)
            .await?
            .into_iter()
            .map(|unit| (unit.key, unit.digest))
            .collect(),
    ))
}

pub async fn export_sync_state_for<K, R>(
    engine: &Engine<K, R>,
) -> Result<Vec<SyncStateUnit>, EngineError>
where
    K: Kernel,
    R: SyncRowCodec<K::Transaction>,
{
    let mut units = Vec::new();
    for entry in engine.export_catalog_entries().await? {
        let key = match entry.kind {
            CatalogEntryKind::Table => SyncKey::Table {
                name: entry
                    .key
                    .values
                    .first()
                    .and_then(value::Value::to_text)
                    .ok_or(EngineError::custom("Invalid catalog key"))?
                    .into(),
            },
            CatalogEntryKind::TableField => SyncKey::TableField {
                table: entry
                    .key
                    .values
                    .first()
                    .and_then(value::Value::to_text)
                    .ok_or(EngineError::custom("Invalid catalog key"))?
                    .into(),
                column: entry
                    .key
                    .values
                    .get(1)
                    .and_then(value::Value::to_text)
                    .ok_or(EngineError::custom("Invalid catalog key"))?
                    .into(),
            },
            CatalogEntryKind::Index => SyncKey::Index {
                name: entry
                    .key
                    .values
                    .first()
                    .and_then(value::Value::to_text)
                    .ok_or(EngineError::custom("Invalid catalog key"))?
                    .into(),
            },
            CatalogEntryKind::IndexField => SyncKey::IndexField {
                index: entry
                    .key
                    .values
                    .first()
                    .and_then(value::Value::to_text)
                    .ok_or(EngineError::custom("Invalid catalog key"))?
                    .into(),
                position: u32::try_from(
                    entry
                        .key
                        .values
                        .get(1)
                        .and_then(value::Value::to_integer)
                        .ok_or(EngineError::custom("Invalid catalog key"))?,
                )
                .map_err(|_| EngineError::custom("Invalid catalog key"))?,
            },
        };
        let state = postcard::to_allocvec(&entry.value).map_err(EngineError::custom)?;
        units.push(SyncStateUnit::new(key, state, Vec::new()));
    }
    for table in engine.table_names().await? {
        let table_for_rows = table.clone();
        let rows = engine
            .read_transaction(|codec, transaction| {
                Box::pin(async move { codec.row_ids(transaction, &table_for_rows).await })
            })
            .await?;
        for row in rows {
            let table_for_row = table.clone();
            let (state, metadata) = engine
                .read_transaction(|codec, transaction| {
                    Box::pin(async move {
                        Ok((
                            codec
                                .export_state(transaction, &table_for_row, row)
                                .await?
                                .unwrap_or_default(),
                            codec
                                .export_metadata(transaction, &table_for_row, row)
                                .await?,
                        ))
                    })
                })
                .await?;
            if !state.is_empty() || !metadata.is_empty() {
                units.push(SyncStateUnit::new(
                    SyncKey::Row {
                        table: table.clone(),
                        row,
                    },
                    state,
                    metadata,
                ));
            }
        }
    }
    units.sort_unstable_by(|left, right| left.key.cmp(&right.key));
    Ok(units)
}

pub async fn apply_sync_state_for<K, R>(
    engine: &Engine<K, R>,
    unit: SyncStateUnit,
) -> Result<(), EngineError>
where
    K: Kernel,
    R: SyncRowCodec<K::Transaction>,
{
    if !unit.verify_digest() {
        return Err(EngineError::custom("Invalid sync state digest"));
    }
    match unit.key {
        SyncKey::Row { table, row } => {
            let table_for_merge = table.clone();
            engine
                .mutate_transaction(&table, row, |codec, transaction, _| {
                    Box::pin(async move {
                        if !unit.metadata.is_empty() {
                            codec
                                .merge_metadata(transaction, &table_for_merge, row, &unit.metadata)
                                .await?;
                        }
                        let value = if unit.state.is_empty() {
                            None
                        } else {
                            codec
                                .merge_state(transaction, &table_for_merge, row, &unit.state)
                                .await?
                        };
                        Ok(((), value))
                    })
                })
                .await
        }
        key => {
            let (kind, entry_key) = match key {
                SyncKey::Table { name } => (
                    CatalogEntryKind::Table,
                    value::Row::new(vec![value::Value::from(name)]),
                ),
                SyncKey::TableField { table, column } => (
                    CatalogEntryKind::TableField,
                    value::Row::new(vec![value::Value::from(table), value::Value::from(column)]),
                ),
                SyncKey::Index { name } => (
                    CatalogEntryKind::Index,
                    value::Row::new(vec![value::Value::from(name)]),
                ),
                SyncKey::IndexField { index, position } => (
                    CatalogEntryKind::IndexField,
                    value::Row::new(vec![
                        value::Value::from(index),
                        value::Value::Integer(i64::from(position)),
                    ]),
                ),
                SyncKey::Row { .. } => unreachable!(),
            };
            let value = postcard::from_bytes(&unit.state).map_err(EngineError::custom)?;
            engine
                .apply_catalog_entry(CatalogEntry {
                    kind,
                    key: entry_key,
                    value,
                })
                .await
        }
    }
}

fn validate_state_batch(batch: &[SyncStateUnit]) -> Result<(), EngineError> {
    if batch.iter().all(SyncStateUnit::verify_digest) {
        Ok(())
    } else {
        Err(EngineError::custom("Invalid sync state digest"))
    }
}

pub async fn apply_incremental_changes_for<K, R>(
    engine: &Engine<K, R>,
    batch: &[SyncIncrementalChange],
) -> Result<(), EngineError>
where
    K: Kernel,
    R: SyncRowCodec<K::Transaction>,
{
    validate_change_batch(batch)?;
    let rows = batch
        .iter()
        .map(|change| (change.table.clone(), change.row))
        .collect::<Vec<_>>();
    let changes = batch.to_vec();
    engine
        .mutate_rows(&rows, |codec, transaction| {
            Box::pin(async move {
                let mut mutations = Vec::with_capacity(changes.len());
                for change in &changes {
                    let old = codec
                        .get_row(transaction, &change.table, &change.row)
                        .await?;
                    let new = codec
                        .apply_change(
                            transaction,
                            &change.table,
                            change.row,
                            &change.id,
                            &change.payload,
                        )
                        .await?;
                    mutations.push(engine::RowMutation {
                        table: change.table.clone(),
                        row: change.row,
                        old,
                        new,
                    });
                }
                Ok(((), mutations))
            })
        })
        .await
}

fn validate_change_batch(batch: &[SyncIncrementalChange]) -> Result<(), EngineError> {
    for change in batch {
        if change.id.0.is_empty() || change.payload.is_empty() {
            return Err(EngineError::custom("Invalid incremental change frame"));
        }
    }
    Ok(())
}

async fn inventories_for<K, R>(
    engine: &Engine<K, R>,
    units: &[SyncStateUnit],
) -> Result<Vec<SyncRowInventory>, EngineError>
where
    K: Kernel,
    R: SyncRowCodec<K::Transaction>,
{
    let mut result = Vec::new();
    for unit in units {
        let SyncKey::Row { table, row } = unit.key.clone() else {
            continue;
        };
        result.push(SyncRowInventory {
            table: table.clone(),
            row,
            changes: engine
                .read_transaction(|codec, transaction| {
                    Box::pin(async move { codec.change_inventory(transaction, &table, row).await })
                })
                .await?,
        });
    }
    result.sort_by_key(SyncRowInventory::key);
    result.dedup_by(|left, right| left.key() == right.key());
    Ok(result)
}

async fn exchange_inventories<T>(
    transport: &mut T,
    role: SyncRole,
    local: Vec<SyncRowInventory>,
) -> Result<Vec<SyncRowInventory>, SyncError<T::Error>>
where
    T: SyncTransport,
    T::Error: core::fmt::Display,
{
    let message = SyncMessage::Inventory(local);
    match role {
        SyncRole::Initiator => {
            send_message(transport, &message).await?;
            receive_inventory(transport).await
        }
        SyncRole::Responder => {
            let remote = receive_inventory(transport).await?;
            send_message(transport, &message).await?;
            Ok(remote)
        }
    }
}

async fn receive_inventory<T>(
    transport: &mut T,
) -> Result<Vec<SyncRowInventory>, SyncError<T::Error>>
where
    T: SyncTransport,
    T::Error: core::fmt::Display,
{
    match receive_message(transport).await? {
        SyncMessage::Inventory(inventory) => Ok(inventory),
        _ => Err(SyncError::UnexpectedMessage),
    }
}

async fn build_outbound<K, R>(
    engine: &Engine<K, R>,
    units: Vec<SyncStateUnit>,
    remote_manifest: SyncManifest,
    remote_inventories: BTreeMap<SyncKey, SyncRowInventory>,
) -> Result<Outbound, EngineError>
where
    K: Kernel,
    R: SyncRowCodec<K::Transaction>,
{
    let mut outbound = Outbound::default();
    for unit in units {
        let SyncKey::Row { table, row } = unit.key.clone() else {
            if !remote_manifest.contains(&unit.key, unit.digest) {
                outbound.snapshots.push(unit);
            }
            continue;
        };
        if remote_manifest.contains(&unit.key, unit.digest) {
            continue;
        }
        let Some(remote) = remote_inventories.get(&unit.key) else {
            outbound.snapshots.push(unit);
            continue;
        };
        let table_for_inventory = table.clone();
        let local = engine
            .read_transaction(|codec, transaction| {
                Box::pin(async move {
                    codec
                        .change_inventory(transaction, &table_for_inventory, row)
                        .await
                })
            })
            .await?;
        let mut exported = 0;
        for key in local {
            if remote.changes.iter().any(|candidate| candidate == &key) {
                continue;
            }
            let export_id = key.clone();
            let table_for_export = table.clone();
            if let Some(payload) = engine
                .read_transaction(move |codec, transaction| {
                    Box::pin(async move {
                        codec
                            .export_change(transaction, &table_for_export, row, &export_id)
                            .await
                    })
                })
                .await?
            {
                outbound.changes.push(SyncIncrementalChange {
                    table: table.clone(),
                    row,
                    id: key,
                    payload,
                });
                exported += 1;
            }
        }
        if exported == 0 {
            outbound.snapshots.push(unit);
        }
    }
    outbound
        .snapshots
        .sort_unstable_by(|left, right| left.key.cmp(&right.key));
    outbound
        .changes
        .sort_unstable_by_key(|left| left.id.clone());
    Ok(outbound)
}

async fn send_outbound<T>(
    transport: &mut T,
    outbound: &Outbound,
    batch_size: usize,
) -> Result<TransferCount, SyncError<T::Error>>
where
    T: SyncTransport,
    T::Error: core::fmt::Display,
{
    for batch in outbound.snapshots.chunks(batch_size) {
        send_message(transport, &SyncMessage::State(batch.to_vec())).await?;
    }
    for batch in outbound.changes.chunks(batch_size) {
        send_message(transport, &SyncMessage::Changes(batch.to_vec())).await?;
    }
    send_message(transport, &SyncMessage::Done).await?;
    Ok(TransferCount {
        snapshots: outbound.snapshots.len(),
        changes: outbound.changes.len(),
    })
}

async fn exchange_snapshot_requests<T>(
    transport: &mut T,
    role: SyncRole,
    local: Vec<SyncSnapshotRequest>,
) -> Result<Vec<SyncSnapshotRequest>, SyncError<T::Error>>
where
    T: SyncTransport,
    T::Error: core::fmt::Display,
{
    let message = SyncMessage::RequestSnapshots(local);
    match role {
        SyncRole::Initiator => {
            send_message(transport, &message).await?;
            receive_snapshot_requests(transport).await
        }
        SyncRole::Responder => {
            let remote = receive_snapshot_requests(transport).await?;
            send_message(transport, &message).await?;
            Ok(remote)
        }
    }
}

async fn receive_snapshot_requests<T>(
    transport: &mut T,
) -> Result<Vec<SyncSnapshotRequest>, SyncError<T::Error>>
where
    T: SyncTransport,
    T::Error: core::fmt::Display,
{
    match receive_message(transport).await? {
        SyncMessage::RequestSnapshots(requests) => Ok(requests),
        _ => Err(SyncError::UnexpectedMessage),
    }
}

async fn recovery_snapshots<K, R>(
    engine: &Engine<K, R>,
    requests: Vec<SyncSnapshotRequest>,
) -> Result<Vec<SyncStateUnit>, EngineError>
where
    K: Kernel,
    R: SyncRowCodec<K::Transaction>,
{
    let mut snapshots = Vec::new();
    for request in requests {
        let snapshot = engine
            .read_transaction(|codec, transaction| {
                Box::pin(async move {
                    let state = codec
                        .export_state(transaction, &request.table, request.row)
                        .await?
                        .unwrap_or_default();
                    let metadata = codec
                        .export_metadata(transaction, &request.table, request.row)
                        .await?;
                    Ok(if state.is_empty() && metadata.is_empty() {
                        None
                    } else {
                        Some(SyncStateUnit::new(
                            SyncKey::Row {
                                table: request.table,
                                row: request.row,
                            },
                            state,
                            metadata,
                        ))
                    })
                })
            })
            .await?;
        if let Some(snapshot) = snapshot {
            snapshots.push(snapshot);
        }
    }
    Ok(snapshots)
}

async fn send_snapshots<T>(
    transport: &mut T,
    snapshots: &[SyncStateUnit],
    batch_size: usize,
) -> Result<usize, SyncError<T::Error>>
where
    T: SyncTransport,
    T::Error: core::fmt::Display,
{
    for batch in snapshots.chunks(batch_size) {
        send_message(transport, &SyncMessage::State(batch.to_vec())).await?;
    }
    send_message(transport, &SyncMessage::Done).await?;
    Ok(snapshots.len())
}

async fn receive_recovery<K, R, T>(
    engine: &Engine<K, R>,
    transport: &mut T,
    pending: &mut Vec<SyncIncrementalChange>,
) -> Result<(usize, usize), SyncError<T::Error>>
where
    K: Kernel,
    R: SyncRowCodec<K::Transaction>,
    T: SyncTransport,
    T::Error: core::fmt::Display,
{
    let mut snapshots = 0;
    loop {
        match receive_message(transport).await? {
            SyncMessage::State(batch) => {
                if let Err(error) = validate_state_batch(&batch) {
                    let reason = format!("{} (recovery snapshot batch)", error);
                    let _ = send_message(transport, &SyncMessage::Abort(reason)).await;
                    return Err(error.into());
                }
                for unit in batch {
                    let unit_key = unit.key.clone();
                    if let Err(error) = apply_sync_state_for(engine, unit).await {
                        let reason = format!("{} (recovery snapshot {:?})", error, unit_key);
                        let _ = send_message(transport, &SyncMessage::Abort(reason)).await;
                        return Err(error.into());
                    }
                    snapshots += 1;
                }
            }
            SyncMessage::Done => break,
            SyncMessage::Abort(reason) => return Err(SyncError::RemoteAbort(reason)),
            _ => return Err(SyncError::UnexpectedMessage),
        }
    }

    // A recovery snapshot is the sender's complete current row state. It includes
    // the history needed by the pending changes and supersedes their payloads.
    pending.clear();
    Ok((snapshots, 0))
}

async fn receive_outbound<K, R, T>(
    engine: &Engine<K, R>,
    transport: &mut T,
) -> Result<Received, SyncError<T::Error>>
where
    K: Kernel,
    R: SyncRowCodec<K::Transaction>,
    T: SyncTransport,
    T::Error: core::fmt::Display,
{
    let mut count = Received {
        snapshots: 0,
        changes: 0,
        requests: Vec::new(),
        pending: Vec::new(),
    };
    loop {
        match receive_message(transport).await? {
            SyncMessage::State(batch) => {
                if let Err(error) = validate_state_batch(&batch) {
                    return abort(transport, error, String::from("snapshot batch")).await;
                }
                for unit in batch {
                    let unit_key = unit.key.clone();
                    if let Err(error) = apply_sync_state_for(engine, unit).await {
                        return abort(transport, error, format!("snapshot {:?}", unit_key)).await;
                    }
                    count.snapshots += 1;
                }
            }
            SyncMessage::Changes(batch) => {
                if let Err(error) = validate_change_batch(&batch) {
                    return abort(transport, error, String::from("incremental batch")).await;
                }
                let change_count = batch.len();
                let result = apply_incremental_changes_for(engine, &batch).await;
                if let Err(error) = result {
                    if matches!(error, EngineError::SyncDependencyUnavailable) {
                        for change in batch {
                            count.requests.push(SyncSnapshotRequest {
                                table: change.table.clone(),
                                row: change.row,
                            });
                            count.pending.push(change);
                        }
                    } else {
                        return abort(transport, error, String::from("incremental batch")).await;
                    }
                } else {
                    count.changes += change_count;
                }
            }
            SyncMessage::Done => {
                count.requests.sort_unstable();
                count.requests.dedup();
                return Ok(count);
            }
            SyncMessage::Abort(reason) => return Err(SyncError::RemoteAbort(reason)),
            SyncMessage::Hello(_)
            | SyncMessage::Manifest(_)
            | SyncMessage::Inventory(_)
            | SyncMessage::RequestSnapshots(_) => {
                return Err(SyncError::UnexpectedMessage);
            }
        }
    }
}

async fn abort<T>(
    transport: &mut T,
    error: EngineError,
    context: String,
) -> Result<Received, SyncError<T::Error>>
where
    T: SyncTransport,
    T::Error: core::fmt::Display,
{
    let reason = format!("{} ({})", error, context);
    let _ = send_message(transport, &SyncMessage::Abort(reason)).await;
    Err(error.into())
}

async fn exchange_hello<K, R, T>(
    engine: &Engine<K, R>,
    transport: &mut T,
    role: SyncRole,
) -> Result<(), SyncError<T::Error>>
where
    K: Kernel,
    R: SyncRowCodec<K::Transaction>,
    T: SyncTransport,
    T::Error: core::fmt::Display,
{
    let hello = SyncMessage::Hello(SyncHello {
        protocol_version: PROTOCOL_VERSION,
        manifest: sync_manifest_for(engine).await?,
    });
    match role {
        SyncRole::Initiator => {
            send_message(transport, &hello).await?;
            validate_hello(receive_message(transport).await?)
        }
        SyncRole::Responder => {
            validate_hello(receive_message(transport).await?)?;
            send_message(transport, &hello).await
        }
    }
}

fn validate_hello<E>(message: SyncMessage) -> Result<(), SyncError<E>> {
    let SyncMessage::Hello(hello) = message else {
        return Err(SyncError::UnexpectedMessage);
    };
    if hello.protocol_version != PROTOCOL_VERSION {
        return Err(SyncError::IncompatibleProtocol(hello.protocol_version));
    }
    Ok(())
}

async fn exchange_manifest<K, R, T>(
    engine: &Engine<K, R>,
    transport: &mut T,
    role: SyncRole,
) -> Result<SyncManifest, SyncError<T::Error>>
where
    K: Kernel,
    R: SyncRowCodec<K::Transaction>,
    T: SyncTransport,
    T::Error: core::fmt::Display,
{
    let message = SyncMessage::Manifest(sync_manifest_for(engine).await?);
    match role {
        SyncRole::Initiator => {
            send_message(transport, &message).await?;
            receive_manifest(transport).await
        }
        SyncRole::Responder => {
            let remote = receive_manifest(transport).await?;
            send_message(transport, &message).await?;
            Ok(remote)
        }
    }
}

async fn receive_manifest<T>(transport: &mut T) -> Result<SyncManifest, SyncError<T::Error>>
where
    T: SyncTransport,
    T::Error: core::fmt::Display,
{
    match receive_message(transport).await? {
        SyncMessage::Manifest(manifest) => Ok(manifest),
        _ => Err(SyncError::UnexpectedMessage),
    }
}

async fn send_message<T>(
    transport: &mut T,
    message: &SyncMessage,
) -> Result<(), SyncError<T::Error>>
where
    T: SyncTransport,
    T::Error: core::fmt::Display,
{
    let frame =
        postcard::to_allocvec(message).map_err(|error| SyncError::Protocol(error.to_string()))?;
    transport.send(frame).await.map_err(SyncError::Transport)
}

async fn receive_message<T>(transport: &mut T) -> Result<SyncMessage, SyncError<T::Error>>
where
    T: SyncTransport,
    T::Error: core::fmt::Display,
{
    let frame = transport.receive().await.map_err(SyncError::Transport)?;
    postcard::from_bytes(&frame).map_err(|error| SyncError::Protocol(error.to_string()))
}
