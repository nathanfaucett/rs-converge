use alloc::{collections::BTreeMap, vec, vec::Vec};

use futures::{StreamExt, pin_mut};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::{
    EngineError, EngineResult, KernelTransaction, RowCodec, RowTable, TableGenerationId,
    catalog::{
        ENGINE_INDEX_FIELDS_STORAGE, ENGINE_INDICES_STORAGE, ENGINE_TABLE_FIELDS_STORAGE,
        ENGINE_TABLES_STORAGE,
    },
    index::update_row,
    schema::{active_table_ids, ensure as ensure_schema},
};

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub enum SyncKey {
    Table { id: Uuid },
    TableField { id: Uuid },
    Index { id: Uuid },
    IndexField { index: Uuid, position: u32 },
    Row { table: TableGenerationId, row: Uuid },
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct StateDigest(pub [u8; 32]);

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct DocumentChangeKey {
    pub document_id: Vec<u8>,
    pub change_hash: [u8; 32],
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IncrementalChange {
    pub table: TableGenerationId,
    pub row: Uuid,
    pub key: DocumentChangeKey,
    pub payload: Vec<u8>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct SyncStateUnit {
    pub key: SyncKey,
    pub state: Vec<u8>,
    pub metadata: Vec<u8>,
    pub digest: StateDigest,
}

impl SyncStateUnit {
    pub fn new(key: SyncKey, state: Vec<u8>, metadata: Vec<u8>) -> Self {
        let mut hasher = Sha256::new();
        hasher.update((state.len() as u64).to_le_bytes());
        hasher.update(&state);
        hasher.update((metadata.len() as u64).to_le_bytes());
        hasher.update(&metadata);
        Self {
            key,
            state,
            metadata,
            digest: StateDigest(hasher.finalize().into()),
        }
    }

    pub fn verify_digest(&self) -> bool {
        Self::new(self.key.clone(), self.state.clone(), self.metadata.clone()).digest == self.digest
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct SyncManifest {
    pub entries: Vec<(SyncKey, StateDigest)>,
}

impl SyncManifest {
    pub fn new(mut entries: Vec<(SyncKey, StateDigest)>) -> Self {
        entries.sort_unstable_by(|left, right| left.0.cmp(&right.0));
        entries.dedup_by(|left, right| left.0 == right.0);
        Self { entries }
    }

    pub fn contains(&self, key: &SyncKey, digest: StateDigest) -> bool {
        self.entries
            .binary_search_by(|(candidate, _)| candidate.cmp(key))
            .is_ok_and(|index| self.entries[index].1 == digest)
    }
}

impl<K, R> crate::Engine<K, R>
where
    K: crate::Kernel,
    R: RowCodec<K::Transaction> + Send + Sync,
{
    pub async fn export_row_sync_state(
        &self,
        table: TableGenerationId,
        row: Uuid,
    ) -> EngineResult<Option<SyncStateUnit>> {
        let transaction = self.kernel.transaction().await?;
        let unit = export_unit(&transaction, self.reconciler.as_ref(), table, row).await?;
        transaction.rollback().await?;
        Ok(unit)
    }

    pub async fn export_sync_state(&self) -> EngineResult<Vec<SyncStateUnit>> {
        let mut transaction = self.kernel.transaction().await?;
        for storage in [
            ENGINE_TABLES_STORAGE,
            ENGINE_TABLE_FIELDS_STORAGE,
            ENGINE_INDICES_STORAGE,
            ENGINE_INDEX_FIELDS_STORAGE,
        ] {
            transaction.ensure_table(storage).await?;
        }
        let tables = active_table_ids(&transaction).await?;
        let mut rows = BTreeMap::new();

        for table in tables {
            let state_rows = self.reconciler.scan_rows(&transaction, table.0);
            pin_mut!(state_rows);
            while let Some(entry) = state_rows.next().await {
                rows.insert((table, entry?.0), ());
            }

            let metadata_rows = self.reconciler.export_row_metadata(&transaction, table.0);
            pin_mut!(metadata_rows);
            while let Some(entry) = metadata_rows.next().await {
                rows.insert((table, entry?.0), ());
            }
        }

        let mut units = Vec::with_capacity(rows.len());
        for ((table, row), ()) in rows {
            if let Some(unit) =
                export_unit(&transaction, self.reconciler.as_ref(), table, row).await?
            {
                units.push(unit);
            }
        }
        export_catalog_units(&transaction, &mut units).await?;
        units.sort_unstable_by(|left, right| left.key.cmp(&right.key));
        transaction.rollback().await?;
        Ok(units)
    }

    pub async fn sync_manifest(&self) -> EngineResult<SyncManifest> {
        let units = self.export_sync_state().await?;
        Ok(SyncManifest::new(
            units
                .into_iter()
                .map(|unit| (unit.key, unit.digest))
                .collect(),
        ))
    }

    pub async fn export_sync_state_for_manifest(
        &self,
        remote: &SyncManifest,
    ) -> EngineResult<Vec<SyncStateUnit>> {
        Ok(self
            .export_sync_state()
            .await?
            .into_iter()
            .filter(|unit| !remote.contains(&unit.key, unit.digest))
            .collect())
    }

    pub async fn sync_table_generations(&self) -> EngineResult<Vec<TableGenerationId>> {
        let transaction = self.kernel.transaction().await?;
        let result = active_table_ids(&transaction).await;
        transaction.rollback().await?;
        result
    }

    pub async fn sync_change_inventory(
        &self,
        table: TableGenerationId,
        row: Uuid,
    ) -> EngineResult<Vec<DocumentChangeKey>> {
        let transaction = self.kernel.transaction().await?;
        let result = self
            .reconciler
            .sync_change_inventory(&transaction, table.0, &row)
            .await;
        transaction.rollback().await?;
        result
    }

    pub async fn export_incremental_change(
        &self,
        table: TableGenerationId,
        key: &DocumentChangeKey,
    ) -> EngineResult<Option<Vec<u8>>> {
        let transaction = self.kernel.transaction().await?;
        let result = self
            .reconciler
            .export_incremental_change(&transaction, table.0, key)
            .await;
        transaction.rollback().await?;
        result
    }

    pub async fn apply_incremental_change(
        &self,
        table: TableGenerationId,
        row: Uuid,
        key: DocumentChangeKey,
        payload: &[u8],
    ) -> EngineResult<()> {
        self.apply_incremental_changes(&[IncrementalChange {
            table,
            row,
            key,
            payload: payload.to_vec(),
        }])
        .await
    }

    pub async fn apply_incremental_changes(
        &self,
        changes: &[IncrementalChange],
    ) -> EngineResult<()> {
        let mut transaction = self.kernel.transaction().await?;
        let result: EngineResult<()> = async {
            ensure_schema(&mut transaction).await?;
            for change in changes {
                transaction.ensure_table(change.table.0).await?;
                self.reconciler
                    .ensure_table(&mut transaction, change.table.0)
                    .await?;
                let old = self
                    .reconciler
                    .get_row(&transaction, change.table.0, &change.row)
                    .await?;
                let value = self
                    .reconciler
                    .apply_incremental_change(
                        &mut transaction,
                        change.table.0,
                        change.row,
                        &change.key,
                        &change.payload,
                    )
                    .await?;
                update_row(
                    &mut transaction,
                    change.table,
                    old.as_ref(),
                    value.as_ref(),
                    false,
                )
                .await?;
            }
            Ok(())
        }
        .await;
        match result {
            Ok(()) => transaction.commit().await,
            Err(error) => {
                transaction.rollback().await?;
                Err(error)
            }
        }
    }

    pub async fn apply_row_sync_state(&self, unit: SyncStateUnit) -> EngineResult<()> {
        if !unit.verify_digest() {
            return Err(EngineError::custom("Invalid sync state digest"));
        }
        let mut transaction = self.kernel.transaction().await?;
        ensure_schema(&mut transaction).await?;
        let result = match unit.key {
            SyncKey::Row { table, row } => {
                transaction.ensure_table(table.0).await?;
                self.reconciler
                    .ensure_table(&mut transaction, table.0)
                    .await?;
                let old = self.reconciler.get_row(&transaction, table.0, &row).await?;
                if !unit.metadata.is_empty() {
                    self.reconciler
                        .merge_row_metadata(&mut transaction, table.0, row, &unit.metadata)
                        .await?;
                }
                let value = if unit.state.is_empty() {
                    None
                } else {
                    self.reconciler
                        .merge_row_state(&mut transaction, table.0, row, &unit.state)
                        .await?
                };
                update_row(&mut transaction, table, old.as_ref(), value.as_ref(), false).await
            }
            key => {
                apply_catalog_unit(&mut transaction, self.reconciler.as_ref(), key, &unit.state)
                    .await
            }
        };
        match result {
            Ok(()) => transaction.commit().await,
            Err(error) => {
                transaction.rollback().await?;
                Err(error)
            }
        }
    }
}

async fn export_catalog_units<T>(
    transaction: &T,
    units: &mut Vec<SyncStateUnit>,
) -> EngineResult<()>
where
    T: KernelTransaction,
{
    export_catalog_table(
        transaction,
        ENGINE_TABLES_STORAGE,
        CatalogKind::Table,
        units,
    )
    .await?;
    export_catalog_table(
        transaction,
        ENGINE_TABLE_FIELDS_STORAGE,
        CatalogKind::TableField,
        units,
    )
    .await?;
    export_catalog_table(
        transaction,
        ENGINE_INDICES_STORAGE,
        CatalogKind::Index,
        units,
    )
    .await?;

    let entries = transaction.scan_entries(ENGINE_INDEX_FIELDS_STORAGE);
    pin_mut!(entries);
    while let Some(entry) = entries.next().await {
        let (key, value) = entry?;
        let Some(index) = key
            .values
            .first()
            .and_then(|value| value.as_uuid())
            .copied()
        else {
            return Err(EngineError::custom("Invalid index field index"));
        };
        let Some(position) = key.values.get(1).and_then(|value| value.to_integer()) else {
            return Err(EngineError::custom("Invalid index field position"));
        };
        let position = u32::try_from(position)
            .map_err(|_| EngineError::custom("Invalid index field position"))?;
        units.push(SyncStateUnit::new(
            SyncKey::IndexField { index, position },
            postcard::to_allocvec(&value).map_err(EngineError::custom)?,
            Vec::new(),
        ));
    }
    Ok(())
}

enum CatalogKind {
    Table,
    TableField,
    Index,
}

async fn export_catalog_table<T>(
    transaction: &T,
    storage: Uuid,
    kind: CatalogKind,
    units: &mut Vec<SyncStateUnit>,
) -> EngineResult<()>
where
    T: KernelTransaction,
{
    let entries = transaction.scan_entries(storage);
    pin_mut!(entries);
    while let Some(entry) = entries.next().await {
        let (key, value) = entry?;
        let Some(id) = key
            .values
            .first()
            .and_then(|value| value.as_uuid())
            .copied()
        else {
            return Err(EngineError::custom("Invalid catalog key"));
        };
        let key = match kind {
            CatalogKind::Table => SyncKey::Table { id },
            CatalogKind::TableField => SyncKey::TableField { id },
            CatalogKind::Index => SyncKey::Index { id },
        };
        units.push(SyncStateUnit::new(
            key,
            postcard::to_allocvec(&value).map_err(EngineError::custom)?,
            Vec::new(),
        ));
    }
    Ok(())
}

async fn apply_catalog_unit<T, R>(
    transaction: &mut T,
    codec: &R,
    key: SyncKey,
    state: &[u8],
) -> EngineResult<()>
where
    T: KernelTransaction,
    R: RowCodec<T>,
{
    let row_value: value::Row = postcard::from_bytes(state).map_err(EngineError::custom)?;
    let data_table = matches!(key, SyncKey::Table { .. } | SyncKey::Index { .. });
    let row_table = matches!(key, SyncKey::Table { .. });
    let (storage, entry_key) = match key {
        SyncKey::Table { id } => (
            ENGINE_TABLES_STORAGE,
            value::Row::new(vec![value::Value::Uuid(id)]),
        ),
        SyncKey::TableField { id } => (
            ENGINE_TABLE_FIELDS_STORAGE,
            value::Row::new(vec![value::Value::Uuid(id)]),
        ),
        SyncKey::Index { id } => (
            ENGINE_INDICES_STORAGE,
            value::Row::new(vec![value::Value::Uuid(id)]),
        ),
        SyncKey::IndexField { index, position } => (
            ENGINE_INDEX_FIELDS_STORAGE,
            value::Row::new(vec![
                value::Value::Uuid(index),
                value::Value::Integer(i64::from(position)),
            ]),
        ),
        SyncKey::Row { .. } => return Err(EngineError::custom("Invalid catalog sync key")),
    };
    let data_table_id = if data_table {
        Some(
            *entry_key
                .values
                .first()
                .and_then(value::Value::as_uuid)
                .ok_or(EngineError::custom("Invalid catalog key"))?,
        )
    } else {
        None
    };
    transaction.ensure_table(storage).await?;
    transaction.put_entry(storage, entry_key, row_value).await?;
    if let Some(id) = data_table_id {
        transaction.ensure_table(id).await?;
        if row_table {
            codec.ensure_table(transaction, id).await?;
        }
    }
    Ok(())
}

async fn export_unit<T, R>(
    transaction: &T,
    codec: &R,
    table: TableGenerationId,
    row: Uuid,
) -> EngineResult<Option<SyncStateUnit>>
where
    T: KernelTransaction,
    R: RowCodec<T>,
{
    let state = codec
        .export_row_state(transaction, table.0, &row)
        .await?
        .unwrap_or_default();
    let metadata = metadata_for(transaction, codec, table.0, row).await?;
    if state.is_empty() && metadata.is_empty() {
        return Ok(None);
    }
    Ok(Some(SyncStateUnit::new(
        SyncKey::Row { table, row },
        state,
        metadata,
    )))
}

async fn metadata_for<T, R>(
    transaction: &T,
    codec: &R,
    table: Uuid,
    row: Uuid,
) -> EngineResult<Vec<u8>>
where
    T: KernelTransaction,
    R: RowCodec<T>,
{
    let metadata = codec.export_row_metadata(transaction, table);
    pin_mut!(metadata);
    while let Some(entry) = metadata.next().await {
        let (candidate, bytes) = entry?;
        if candidate == row {
            return Ok(bytes);
        }
    }
    Ok(Vec::new())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn manifest_is_sorted_and_digest_is_verified() {
        let first = SyncKey::Row {
            table: TableGenerationId(Uuid::from_u128(2)),
            row: Uuid::from_u128(1),
        };
        let second = SyncKey::Row {
            table: TableGenerationId(Uuid::from_u128(1)),
            row: Uuid::from_u128(1),
        };
        let unit = SyncStateUnit::new(first.clone(), vec![1], vec![2]);
        assert!(unit.verify_digest());
        let manifest = SyncManifest::new(vec![(first, unit.digest), (second.clone(), unit.digest)]);
        assert_eq!(manifest.entries[0].0, second);
    }
}
