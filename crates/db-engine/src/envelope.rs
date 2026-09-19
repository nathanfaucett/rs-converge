use alloc::{
    collections::{BTreeMap, BTreeSet},
    string::String,
    vec,
    vec::Vec,
};

use db_value::{Row, Value};
use futures::{StreamExt, pin_mut};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::{
    Change, EngineError, EngineResult, KernelTransaction, RowCodec, SchemaChange,
    TableGenerationId,
    catalog::{
        ENGINE_ENVELOPE_FRONTIER, ENGINE_ENVELOPE_HEADERS, ENGINE_ENVELOPE_LOG,
        ENGINE_ENVELOPE_SEQUENCE, ENGINE_ENVELOPE_STATUS, ENGINE_ENVELOPES,
        ENGINE_QUARANTINED_ENVELOPES,
    },
    change::materialize_change,
    index::rebuild_table,
    schema::{active_table_ids, checkpoint_changes, materialize as materialize_schema},
};

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct EnvelopeId(pub [u8; 32]);

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct Frontier {
    pub heads: Vec<EnvelopeId>,
}

impl Frontier {
    pub fn new(mut heads: Vec<EnvelopeId>) -> Self {
        heads.sort_unstable();
        heads.dedup();
        Self { heads }
    }
}

const WIRE_VERSION: u8 = 3;
const CHECKPOINT_WIRE_VERSION: u8 = 1;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct TransactionEnvelope {
    pub id: EnvelopeId,
    pub parents: Vec<EnvelopeId>,
    pub changes: Vec<Change>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct EnvelopeHeader {
    pub id: EnvelopeId,
    pub parents: Vec<EnvelopeId>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct CheckpointRow {
    pub table: TableGenerationId,
    pub row: Uuid,
    pub state: Vec<u8>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct RowTombstone {
    pub table: TableGenerationId,
    pub row: Uuid,
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct Checkpoint {
    pub frontier: Frontier,
    pub headers: Vec<EnvelopeHeader>,
    pub schema: Vec<SchemaChange>,
    pub rows: Vec<CheckpointRow>,
    pub row_tombstones: Vec<RowTombstone>,
}

impl Checkpoint {
    pub fn encode(&self) -> EngineResult<Vec<u8>> {
        postcard::to_allocvec(&WireCheckpoint {
            version: CHECKPOINT_WIRE_VERSION,
            checkpoint: self.clone(),
        })
        .map_err(EngineError::custom)
    }

    pub fn decode(bytes: &[u8]) -> EngineResult<Self> {
        let wire: WireCheckpoint = postcard::from_bytes(bytes).map_err(EngineError::custom)?;
        if wire.version != CHECKPOINT_WIRE_VERSION {
            return Err(EngineError::custom("Unsupported checkpoint wire version"));
        }
        Ok(wire.checkpoint)
    }
}

#[derive(Serialize, Deserialize)]
struct WireEnvelope {
    version: u8,
    envelope: TransactionEnvelope,
}

#[derive(Serialize, Deserialize)]
struct WireCheckpoint {
    version: u8,
    checkpoint: Checkpoint,
}

impl TransactionEnvelope {
    pub fn new(parents: Vec<EnvelopeId>, changes: Vec<Change>) -> EngineResult<Self> {
        let parents = Frontier::new(parents).heads;
        let id = Self::id_for(&parents, &changes)?;
        Ok(Self {
            id,
            parents,
            changes,
        })
    }

    pub fn validate(&self) -> EngineResult<()> {
        let expected = Self::id_for(&self.parents, &self.changes)?;
        if self.id != expected {
            return Err(EngineError::custom("Invalid envelope ID"));
        }
        if self.parents != Frontier::new(self.parents.clone()).heads {
            return Err(EngineError::custom("Envelope parents are not canonical"));
        }
        Ok(())
    }

    pub fn encode(&self) -> EngineResult<Vec<u8>> {
        self.validate()?;
        postcard::to_allocvec(&WireEnvelope {
            version: WIRE_VERSION,
            envelope: self.clone(),
        })
        .map_err(EngineError::custom)
    }

    pub fn decode(bytes: &[u8]) -> EngineResult<Self> {
        let wire: WireEnvelope = postcard::from_bytes(bytes).map_err(EngineError::custom)?;
        if wire.version != WIRE_VERSION {
            return Err(EngineError::custom("Unsupported envelope wire version"));
        }
        wire.envelope.validate()?;
        Ok(wire.envelope)
    }

    fn id_for(parents: &[EnvelopeId], changes: &[Change]) -> EngineResult<EnvelopeId> {
        let bytes = postcard::to_allocvec(&(parents, changes)).map_err(EngineError::custom)?;
        Ok(EnvelopeId(Sha256::digest(bytes).into()))
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum EnvelopeOutcome {
    Applied,
    Pending,
    Superseded,
    Quarantined { reason: String },
}

pub(crate) async fn ensure_envelope_log<T>(transaction: &mut T) -> EngineResult<()>
where
    T: KernelTransaction,
{
    for table in [
        ENGINE_ENVELOPES,
        ENGINE_ENVELOPE_FRONTIER,
        ENGINE_ENVELOPE_LOG,
        ENGINE_ENVELOPE_SEQUENCE,
        ENGINE_ENVELOPE_STATUS,
        ENGINE_ENVELOPE_HEADERS,
        ENGINE_QUARANTINED_ENVELOPES,
    ] {
        transaction.ensure_table(table).await?;
    }
    Ok(())
}

pub(crate) async fn frontier<T>(transaction: &T) -> EngineResult<Frontier>
where
    T: KernelTransaction,
{
    let entries = transaction.scan_entries(ENGINE_ENVELOPE_FRONTIER);
    pin_mut!(entries);
    let mut heads = Vec::new();
    while let Some(entry) = entries.next().await {
        let (key, _) = entry?;
        heads.push(id_from_key(&key)?);
    }
    Ok(Frontier::new(heads))
}

pub(crate) async fn record_local<T>(
    transaction: &mut T,
    changes: Vec<Change>,
) -> EngineResult<Option<TransactionEnvelope>>
where
    T: KernelTransaction,
{
    if changes.is_empty() {
        return Ok(None);
    }
    let envelope = TransactionEnvelope::new(frontier(transaction).await?.heads, changes)?;
    store(transaction, &envelope, EnvelopeOutcome::Applied).await?;
    Ok(Some(envelope))
}

pub(crate) async fn import<T, R>(
    transaction: &mut T,
    codec: &R,
    envelope: TransactionEnvelope,
) -> EngineResult<EnvelopeOutcome>
where
    T: KernelTransaction,
    R: RowCodec<T>,
{
    envelope.validate()?;
    if let Some(outcome) = outcome(transaction, envelope.id).await? {
        return Ok(outcome);
    }
    if !parents_applied(transaction, &envelope.parents).await? {
        store(transaction, &envelope, EnvelopeOutcome::Pending).await?;
        return Ok(EnvelopeOutcome::Pending);
    }
    let outcome = apply(transaction, codec, &envelope).await?;
    retry_pending(transaction, codec).await?;
    Ok(outcome)
}

pub(crate) async fn import_checkpoint<T, R>(
    transaction: &mut T,
    codec: &R,
    checkpoint: Checkpoint,
) -> EngineResult<()>
where
    T: KernelTransaction,
    R: RowCodec<T>,
{
    for header in &checkpoint.headers {
        store_header(transaction, header).await?;
    }
    for change in &checkpoint.schema {
        let _ = materialize_schema(transaction, change).await?;
        if let SchemaChange::CreateTable { table, .. } = change {
            codec.ensure_table(transaction, *table).await?;
        }
    }
    for tombstone in &checkpoint.row_tombstones {
        codec
            .tombstone_row(transaction, &tombstone.table, &tombstone.row)
            .await?;
    }
    let mut rebuild = BTreeSet::new();
    for row in &checkpoint.rows {
        if codec
            .merge_row_state(transaction, &row.table, row.row, &row.state)
            .await?
            .is_some()
        {
            rebuild.insert(row.table);
        }
    }
    for table in rebuild {
        rebuild_table(transaction, codec, table, false).await?;
    }
    for header in &checkpoint.headers {
        for parent in &header.parents {
            transaction
                .remove_entry(ENGINE_ENVELOPE_FRONTIER, &key(*parent))
                .await?;
        }
    }
    for head in checkpoint.frontier.heads {
        transaction
            .put_entry(ENGINE_ENVELOPE_FRONTIER, key(head), Row::default())
            .await?;
    }
    retry_pending(transaction, codec).await
}

pub(crate) async fn quarantine<T>(
    transaction: &mut T,
    bytes: Vec<u8>,
    reason: String,
) -> EngineResult<EnvelopeOutcome>
where
    T: KernelTransaction,
{
    let id = EnvelopeId(Sha256::digest(&bytes).into());
    let key = key(id);
    let outcome = EnvelopeOutcome::Quarantined {
        reason: reason.clone(),
    };
    transaction
        .put_entry(
            ENGINE_QUARANTINED_ENVELOPES,
            key.clone(),
            Row::new(vec![Value::Blob(bytes), Value::from(reason.as_str())]),
        )
        .await?;
    transaction
        .put_entry(ENGINE_ENVELOPE_STATUS, key, status_row(&outcome)?)
        .await?;
    Ok(outcome)
}

pub(crate) async fn checkpoint<T, R>(transaction: &T, codec: &R) -> EngineResult<Checkpoint>
where
    T: KernelTransaction,
    R: RowCodec<T>,
{
    let mut headers = headers(transaction).await?;
    headers.sort_by_key(|header| header.id);
    let schema = checkpoint_changes(transaction).await?;
    let tables = active_table_ids(transaction).await?;
    let mut rows = Vec::new();
    let mut row_tombstones = Vec::new();
    for table in tables {
        let table_rows = codec.scan_rows(transaction, &table);
        pin_mut!(table_rows);
        while let Some(row) = table_rows.next().await {
            let (row, _) = row?;
            if let Some(state) = codec.export_row_state(transaction, &table, &row).await? {
                rows.push(CheckpointRow { table, row, state });
            }
        }
        let tombstones = codec.row_tombstones(transaction, &table);
        pin_mut!(tombstones);
        while let Some(row) = tombstones.next().await {
            row_tombstones.push(RowTombstone { table, row: row? });
        }
    }
    Ok(Checkpoint {
        frontier: frontier(transaction).await?,
        headers,
        schema,
        rows,
        row_tombstones,
    })
}

pub(crate) async fn envelopes_missing<T>(
    transaction: &T,
    frontier: &Frontier,
) -> EngineResult<Vec<TransactionEnvelope>>
where
    T: KernelTransaction,
{
    let entries = transaction.scan_entries(ENGINE_ENVELOPES);
    pin_mut!(entries);
    let mut stored = BTreeMap::new();
    while let Some(entry) = entries.next().await {
        let (key, value) = entry?;
        let id = id_from_key(&key)?;
        let Some(bytes) = value.values.first().and_then(Value::as_blob) else {
            return Err(EngineError::custom("Invalid envelope"));
        };
        stored.insert(id, TransactionEnvelope::decode(bytes)?);
    }

    let mut known = BTreeSet::new();
    let mut pending = frontier.heads.clone();
    while let Some(id) = pending.pop() {
        if !known.insert(id) {
            continue;
        }
        if let Some(envelope) = stored.get(&id) {
            pending.extend(envelope.parents.iter().copied());
        }
    }

    Ok(stored
        .into_iter()
        .filter_map(|(id, envelope)| (!known.contains(&id)).then_some(envelope))
        .collect())
}

async fn apply<T, R>(
    transaction: &mut T,
    codec: &R,
    envelope: &TransactionEnvelope,
) -> EngineResult<EnvelopeOutcome>
where
    T: KernelTransaction,
    R: RowCodec<T>,
{
    let mut superseded = !envelope.changes.is_empty();
    for change in &envelope.changes {
        superseded &= materialize_change(transaction, codec, change, false).await?;
    }
    let outcome = if superseded {
        EnvelopeOutcome::Superseded
    } else {
        EnvelopeOutcome::Applied
    };
    store(transaction, envelope, outcome.clone()).await?;
    Ok(outcome)
}

async fn retry_pending<T, R>(transaction: &mut T, codec: &R) -> EngineResult<()>
where
    T: KernelTransaction,
    R: RowCodec<T>,
{
    loop {
        let ready = {
            let entries = transaction.scan_entries(ENGINE_ENVELOPES);
            pin_mut!(entries);
            let mut ready = None;
            while let Some(entry) = entries.next().await {
                let (key, value) = entry?;
                let id = id_from_key(&key)?;
                if outcome(transaction, id).await? != Some(EnvelopeOutcome::Pending) {
                    continue;
                }
                let Some(bytes) = value.values.first().and_then(Value::as_blob) else {
                    return Err(EngineError::custom("Invalid envelope"));
                };
                let envelope = TransactionEnvelope::decode(bytes)?;
                if parents_applied(transaction, &envelope.parents).await? {
                    ready = Some(envelope);
                    break;
                }
            }
            ready
        };
        let Some(envelope) = ready else {
            return Ok(());
        };
        let _ = apply(transaction, codec, &envelope).await?;
    }
}

async fn store<T>(
    transaction: &mut T,
    envelope: &TransactionEnvelope,
    outcome: EnvelopeOutcome,
) -> EngineResult<()>
where
    T: KernelTransaction,
{
    let envelope_key = key(envelope.id);
    store_header(
        transaction,
        &EnvelopeHeader {
            id: envelope.id,
            parents: envelope.parents.clone(),
        },
    )
    .await?;
    transaction
        .put_entry(
            ENGINE_ENVELOPES,
            envelope_key.clone(),
            Row::new(vec![Value::Blob(envelope.encode()?)]),
        )
        .await?;
    transaction
        .put_entry(
            ENGINE_ENVELOPE_STATUS,
            envelope_key.clone(),
            status_row(&outcome)?,
        )
        .await?;
    if !matches!(
        outcome,
        EnvelopeOutcome::Applied | EnvelopeOutcome::Superseded
    ) {
        return Ok(());
    }
    for parent in &envelope.parents {
        transaction
            .remove_entry(ENGINE_ENVELOPE_FRONTIER, &key(*parent))
            .await?;
    }
    let has_known_child = headers(transaction)
        .await?
        .iter()
        .any(|header| header.parents.contains(&envelope.id));
    if !has_known_child {
        transaction
            .put_entry(
                ENGINE_ENVELOPE_FRONTIER,
                envelope_key.clone(),
                Row::default(),
            )
            .await?;
    }
    let sequence_key = Row::default();
    let sequence = match transaction
        .get_entry(ENGINE_ENVELOPE_SEQUENCE, &sequence_key)
        .await?
    {
        Some(value) => match value.values.as_slice() {
            [Value::Integer(sequence)] => sequence
                .checked_add(1)
                .ok_or_else(|| EngineError::custom("Envelope sequence overflow"))?,
            _ => return Err(EngineError::custom("Invalid envelope sequence")),
        },
        None => 1,
    };
    transaction
        .put_entry(
            ENGINE_ENVELOPE_SEQUENCE,
            sequence_key,
            Row::new(vec![Value::Integer(sequence)]),
        )
        .await?;
    transaction
        .put_entry(
            ENGINE_ENVELOPE_LOG,
            Row::new(vec![Value::Integer(sequence)]),
            Row::new(vec![Value::Blob(envelope.id.0.to_vec())]),
        )
        .await
}

async fn parents_applied<T>(transaction: &T, parents: &[EnvelopeId]) -> EngineResult<bool>
where
    T: KernelTransaction,
{
    for parent in parents {
        if matches!(
            outcome(transaction, *parent).await?,
            Some(EnvelopeOutcome::Applied | EnvelopeOutcome::Superseded)
        ) || transaction
            .get_entry(ENGINE_ENVELOPE_HEADERS, &key(*parent))
            .await?
            .is_some()
        {
            continue;
        }
        return Ok(false);
    }
    Ok(true)
}

async fn store_header<T>(transaction: &mut T, header: &EnvelopeHeader) -> EngineResult<()>
where
    T: KernelTransaction,
{
    if header.parents != Frontier::new(header.parents.clone()).heads {
        return Err(EngineError::custom(
            "Envelope header parents are not canonical",
        ));
    }
    let key = key(header.id);
    let value = postcard::to_allocvec(header).map_err(EngineError::custom)?;
    let value = Row::new(vec![Value::Blob(value)]);
    match transaction.get_entry(ENGINE_ENVELOPE_HEADERS, &key).await? {
        Some(existing) if existing != value => {
            Err(EngineError::custom("Conflicting envelope header"))
        }
        Some(_) => Ok(()),
        None => {
            transaction
                .put_entry(ENGINE_ENVELOPE_HEADERS, key, value)
                .await
        }
    }
}

async fn headers<T>(transaction: &T) -> EngineResult<Vec<EnvelopeHeader>>
where
    T: KernelTransaction,
{
    let entries = transaction.scan_entries(ENGINE_ENVELOPE_HEADERS);
    pin_mut!(entries);
    let mut headers = Vec::new();
    while let Some(entry) = entries.next().await {
        let (_, value) = entry?;
        let Some(Value::Blob(bytes)) = value.values.first() else {
            return Err(EngineError::custom("Invalid envelope header"));
        };
        let header: EnvelopeHeader = postcard::from_bytes(bytes).map_err(EngineError::custom)?;
        if header.parents != Frontier::new(header.parents.clone()).heads {
            return Err(EngineError::custom(
                "Envelope header parents are not canonical",
            ));
        }
        headers.push(header);
    }
    Ok(headers)
}

pub(crate) async fn outcomes<T>(transaction: &T) -> EngineResult<Vec<(EnvelopeId, EnvelopeOutcome)>>
where
    T: KernelTransaction,
{
    let entries = transaction.scan_entries(ENGINE_ENVELOPE_STATUS);
    pin_mut!(entries);
    let mut result = Vec::new();
    while let Some(entry) = entries.next().await {
        let (key, row) = entry?;
        let id = id_from_key(&key)?;
        let Some(Value::Blob(bytes)) = row.values.first() else {
            return Err(EngineError::custom("Invalid envelope status"));
        };
        result.push((
            id,
            postcard::from_bytes(bytes).map_err(EngineError::custom)?,
        ));
    }
    Ok(result)
}

pub(crate) async fn outcome<T>(
    transaction: &T,
    id: EnvelopeId,
) -> EngineResult<Option<EnvelopeOutcome>>
where
    T: KernelTransaction,
{
    transaction
        .get_entry(ENGINE_ENVELOPE_STATUS, &key(id))
        .await?
        .map(|row| {
            let Some(Value::Blob(bytes)) = row.values.first() else {
                return Err(EngineError::custom("Invalid envelope status"));
            };
            postcard::from_bytes(bytes).map_err(EngineError::custom)
        })
        .transpose()
}

fn key(id: EnvelopeId) -> Row {
    Row::new(vec![Value::Blob(id.0.to_vec())])
}

fn id_from_key(key: &Row) -> EngineResult<EnvelopeId> {
    let Some(Value::Blob(bytes)) = key.values.first() else {
        return Err(EngineError::custom("Invalid envelope ID"));
    };
    let id = bytes
        .as_slice()
        .try_into()
        .map_err(|_| EngineError::custom("Invalid envelope ID"))?;
    Ok(EnvelopeId(id))
}

fn status_row(outcome: &EnvelopeOutcome) -> EngineResult<Row> {
    postcard::to_allocvec(outcome)
        .map(|bytes| Row::new(vec![Value::Blob(bytes)]))
        .map_err(EngineError::custom)
}

#[cfg(test)]
mod tests {
    use alloc::vec::Vec;

    use super::{Checkpoint, TransactionEnvelope, WireCheckpoint, WireEnvelope};

    #[test]
    fn rejects_legacy_and_unknown_wire_versions() {
        let envelope = TransactionEnvelope::new(Vec::new(), Vec::new()).unwrap();
        assert!(TransactionEnvelope::decode(&postcard::to_allocvec(&envelope).unwrap()).is_err());
        assert!(
            TransactionEnvelope::decode(
                &postcard::to_allocvec(&WireEnvelope {
                    version: 2,
                    envelope,
                })
                .unwrap(),
            )
            .is_err()
        );
    }

    #[test]
    fn checkpoints_are_versioned() {
        let checkpoint = Checkpoint::default();
        let bytes = checkpoint.encode().unwrap();
        assert_eq!(Checkpoint::decode(&bytes).unwrap(), checkpoint);
        assert!(Checkpoint::decode(&postcard::to_allocvec(&checkpoint).unwrap()).is_err());
        assert!(
            Checkpoint::decode(
                &postcard::to_allocvec(&WireCheckpoint {
                    version: 2,
                    checkpoint,
                })
                .unwrap()
            )
            .is_err()
        );
    }
}
