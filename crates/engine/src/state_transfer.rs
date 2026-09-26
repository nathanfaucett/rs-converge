use alloc::{boxed::Box, string::String, vec::Vec};
use core::{future::Future, pin::Pin};

use futures::{StreamExt, pin_mut};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use value::{Row, Value};

use crate::{
    Engine, EngineError, EngineResult, Kernel, KernelTransaction, RowCodec, RowTable,
    catalog::{
        ENGINE_INDEX_FIELDS_STORAGE, ENGINE_INDICES_STORAGE, ENGINE_TABLE_FIELDS_STORAGE,
        ENGINE_TABLES_STORAGE,
    },
    schema::active_table_names,
};

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub enum CatalogEntryKind {
    Table,
    TableField,
    Index,
    IndexField,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct CatalogEntry {
    pub kind: CatalogEntryKind,
    pub key: Row,
    pub value: Row,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RowMutation {
    pub table: String,
    pub row: Uuid,
    pub old: Option<Row>,
    pub new: Option<Row>,
}

impl<K, R> Engine<K, R>
where
    K: Kernel,
    R: RowCodec<K::Transaction> + Send + Sync,
{
    pub async fn export_catalog_entries(&self) -> EngineResult<Vec<CatalogEntry>> {
        let mut transaction = self.kernel.transaction().await?;
        let mut result = Vec::new();
        for (storage, kind) in [
            (ENGINE_TABLES_STORAGE, CatalogEntryKind::Table),
            (ENGINE_TABLE_FIELDS_STORAGE, CatalogEntryKind::TableField),
            (ENGINE_INDICES_STORAGE, CatalogEntryKind::Index),
            (ENGINE_INDEX_FIELDS_STORAGE, CatalogEntryKind::IndexField),
        ] {
            transaction.ensure_table(storage).await?;
            let entries = transaction.scan_entries(storage);
            pin_mut!(entries);
            while let Some(entry) = entries.next().await {
                let (key, value) = entry?;
                result.push(CatalogEntry { kind, key, value });
            }
        }
        transaction.rollback().await?;
        result.sort_by(|a, b| a.kind.cmp(&b.kind).then_with(|| a.key.cmp(&b.key)));
        Ok(result)
    }

    pub async fn apply_catalog_entry(&self, entry: CatalogEntry) -> EngineResult<()> {
        let mut transaction = self.kernel.transaction().await?;
        let result = async {
            let storage = match entry.kind {
                CatalogEntryKind::Table => ENGINE_TABLES_STORAGE,
                CatalogEntryKind::TableField => ENGINE_TABLE_FIELDS_STORAGE,
                CatalogEntryKind::Index => ENGINE_INDICES_STORAGE,
                CatalogEntryKind::IndexField => ENGINE_INDEX_FIELDS_STORAGE,
            };
            transaction.ensure_table(storage).await?;
            transaction
                .put_entry(storage, entry.key.clone(), entry.value)
                .await?;
            if matches!(
                entry.kind,
                CatalogEntryKind::Table | CatalogEntryKind::Index
            ) {
                let id = entry
                    .key
                    .values
                    .first()
                    .and_then(Value::to_text)
                    .ok_or(EngineError::custom("Invalid catalog key"))?;
                transaction.ensure_table(&id).await?;
                if matches!(entry.kind, CatalogEntryKind::Table) {
                    self.reconciler.ensure_table(&mut transaction, &id).await?;
                }
            }
            Ok::<_, EngineError>(())
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

    pub async fn read_transaction<F, O>(&self, operation: F) -> EngineResult<O>
    where
        F: for<'a> FnOnce(
            &'a R,
            &'a K::Transaction,
        ) -> Pin<Box<dyn Future<Output = EngineResult<O>> + Send + 'a>>,
    {
        let transaction = self.kernel.transaction().await?;
        let result = operation(self.reconciler.as_ref(), &transaction).await;
        transaction.rollback().await?;
        result
    }

    pub async fn mutate_transaction<F, O>(
        &self,
        table: &str,
        row: Uuid,
        operation: F,
    ) -> EngineResult<O>
    where
        F: for<'a> FnOnce(
            &'a R,
            &'a mut K::Transaction,
            Option<value::Row>,
        ) -> Pin<
            Box<dyn Future<Output = EngineResult<(O, Option<value::Row>)>> + Send + 'a>,
        >,
    {
        let mut transaction = self.kernel.transaction().await?;
        let result: EngineResult<O> = async {
            crate::schema::ensure(&mut transaction).await?;
            transaction.ensure_table(table).await?;
            self.reconciler
                .ensure_table(&mut transaction, table)
                .await?;
            let old = self.reconciler.get_row(&transaction, table, &row).await?;
            let (output, new) =
                operation(self.reconciler.as_ref(), &mut transaction, old.clone()).await?;
            crate::index::update_row(&mut transaction, table, old.as_ref(), new.as_ref(), false)
                .await?;
            Ok(output)
        }
        .await;
        match result {
            Ok(output) => {
                transaction.commit().await?;
                Ok(output)
            }
            Err(error) => {
                transaction.rollback().await?;
                Err(error)
            }
        }
    }

    pub async fn mutate_rows<F, O>(&self, rows: &[(String, Uuid)], operation: F) -> EngineResult<O>
    where
        F: for<'a> FnOnce(
            &'a R,
            &'a mut K::Transaction,
        ) -> Pin<
            Box<dyn Future<Output = EngineResult<(O, Vec<RowMutation>)>> + Send + 'a>,
        >,
    {
        let mut transaction = self.kernel.transaction().await?;
        let result: EngineResult<O> = async {
            crate::schema::ensure(&mut transaction).await?;
            for (table, _) in rows {
                transaction.ensure_table(table).await?;
                self.reconciler
                    .ensure_table(&mut transaction, table)
                    .await?;
            }
            let (output, mutations) = operation(self.reconciler.as_ref(), &mut transaction).await?;
            for mutation in mutations {
                crate::index::update_row(
                    &mut transaction,
                    &mutation.table,
                    mutation.old.as_ref(),
                    mutation.new.as_ref(),
                    false,
                )
                .await?;
            }
            Ok(output)
        }
        .await;
        match result {
            Ok(output) => {
                transaction.commit().await?;
                Ok(output)
            }
            Err(error) => {
                transaction.rollback().await?;
                Err(error)
            }
        }
    }

    pub async fn table_names(&self) -> EngineResult<Vec<String>> {
        let mut transaction = self.kernel.transaction().await?;
        transaction.ensure_table(ENGINE_TABLES_STORAGE).await?;
        let result = active_table_names(&transaction).await;
        transaction.rollback().await?;
        result
    }
}
