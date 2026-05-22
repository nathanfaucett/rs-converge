#![allow(clippy::manual_async_fn)]

use crate::{EngineError, EngineKey, EngineRow, IndexSchema, PrimaryKey, TableSchema};
use async_stream::stream;
use core::future::Future;
use db_core::{MaybeSend, MaybeSync, NamedTreeProvider, NamedTreeTransaction};
use db_types::persistence::{
  INDEX_SCHEMA_TREE, TABLE_SCHEMA_TREE, decode_index_schema_rows, decode_table_schema_rows,
  encode_index_schema, encode_table_schema, index_schema_entry_key, index_tree, row_tree,
  table_schema_entry_key,
};
use futures::{Stream, StreamExt, pin_mut};

mod backend_contract;
mod contract;
mod helpers;
mod transaction;

pub use backend_contract::{BackendCapability, TransactionContract};
pub(crate) use contract::{
  decode_row_bytes, encode_row_bytes, primary_key_from_engine_key, schema_decode_error,
};
pub(crate) use helpers::{
  collect_table_rows, delete_row, find_conflicting_index_entry, remove_index_entries,
  remove_table_rows,
};
pub use helpers::{fetch_rows_by_primary_keys, lookup_primary_keys_by_index_predicate};
pub use transaction::{
  EngineStoreTransaction, IndexStore, RowStore, SchemaStore, TransactionControl,
};

async fn collect_tree_rows<T>(tx: &T, tree_name: &str) -> Result<Vec<EngineRow>, EngineError>
where
  T: NamedTreeTransaction<EngineKey, Vec<u8>>,
{
  let stream = tx.range(tree_name, ..);
  pin_mut!(stream);

  let mut rows = Vec::new();
  while let Some(item) = stream.next().await {
    let (_key, row_bytes) = item.map_err(EngineError::from)?;
    rows.push(decode_row_bytes(&row_bytes)?);
  }
  Ok(rows)
}

pub trait EngineStore: Clone + MaybeSend + MaybeSync + 'static {
  type Transaction: EngineStoreTransaction + MaybeSend + 'static;

  fn engine_transaction(&self) -> impl Future<Output = Result<Self::Transaction, EngineError>>;

  /// Return the transactional contract this backend honors.
  /// Must be consistent across all calls and implementations.
  fn transaction_contract(&self) -> TransactionContract {
    // Default: assume coupled multi-tree atomicity (safest assumption).
    TransactionContract::coupled_multi_tree()
  }
}

/// Engine transaction that routes operations to named trees via a
/// `NamedTreeTransaction<EngineKey, Vec<u8>>`.
pub struct NamedTreeEngineTransaction<T>
where
  T: NamedTreeTransaction<EngineKey, Vec<u8>>,
{
  inner: T,
}

impl<T> NamedTreeEngineTransaction<T>
where
  T: NamedTreeTransaction<EngineKey, Vec<u8>>,
{
  pub fn new(inner: T) -> Self {
    Self { inner }
  }
}

impl<T> RowStore for NamedTreeEngineTransaction<T>
where
  T: NamedTreeTransaction<EngineKey, Vec<u8>> + 'static,
{
  fn get_table_row<'a>(
    &'a mut self,
    table_name: &'a str,
    primary_key: &'a PrimaryKey,
  ) -> impl Future<Output = Result<Option<EngineRow>, EngineError>> + 'a {
    async move {
      let storage_key = primary_key.to_engine_key();
      self
        .inner
        .get(&row_tree(table_name), &storage_key)
        .await
        .map_err(EngineError::from)
        .and_then(|row| row.map(|bytes| decode_row_bytes(&bytes)).transpose())
    }
  }

  fn insert_table_row<'a>(
    &'a mut self,
    table_name: &'a str,
    primary_key: PrimaryKey,
    row: EngineRow,
  ) -> impl Future<Output = Result<(), EngineError>> + 'a {
    async move {
      let storage_key = primary_key.to_engine_key();
      let row_bytes = encode_row_bytes(&row);
      self
        .inner
        .insert(&row_tree(table_name), storage_key, row_bytes)
        .await
        .map_err(EngineError::from)
    }
  }

  fn remove_table_row<'a>(
    &'a mut self,
    table_name: &'a str,
    primary_key: &'a PrimaryKey,
  ) -> impl Future<Output = Result<Option<EngineRow>, EngineError>> + 'a {
    async move {
      let storage_key = primary_key.to_engine_key();
      self
        .inner
        .remove(&row_tree(table_name), &storage_key)
        .await
        .map_err(EngineError::from)
        .and_then(|row| row.map(|bytes| decode_row_bytes(&bytes)).transpose())
    }
  }

  fn range_table_rows<'a>(
    &'a self,
    table_name: &'a str,
  ) -> impl Stream<Item = Result<(PrimaryKey, EngineRow), EngineError>> + 'a {
    let tree = row_tree(table_name);
    let inner = &self.inner;
    stream! {
      let s = inner.range(&tree, ..);
      pin_mut!(s);
      while let Some(item) = s.next().await {
        yield item
          .map_err(EngineError::from)
          .and_then(|(key, row_bytes)| {
            let row = decode_row_bytes(&row_bytes)?;
            primary_key_from_engine_key(&key).map(|pk| (pk, row))
          });
      }
    }
  }
}

impl<T> SchemaStore for NamedTreeEngineTransaction<T>
where
  T: NamedTreeTransaction<EngineKey, Vec<u8>> + 'static,
{
  fn insert_table_schema<'a>(
    &'a mut self,
    schema: TableSchema,
  ) -> impl Future<Output = Result<(), EngineError>> + 'a {
    async move {
      let key = table_schema_entry_key(schema.name.clone());
      let value = encode_row_bytes(&encode_table_schema(&schema));
      self
        .inner
        .insert(TABLE_SCHEMA_TREE, key, value)
        .await
        .map_err(EngineError::from)
    }
  }

  fn remove_table_schema<'a>(
    &'a mut self,
    table_name: &'a str,
  ) -> impl Future<Output = Result<(), EngineError>> + 'a {
    async move {
      let key = table_schema_entry_key(table_name);
      self
        .inner
        .remove(TABLE_SCHEMA_TREE, &key)
        .await
        .map_err(EngineError::from)?;
      Ok(())
    }
  }

  fn insert_index_schema<'a>(
    &'a mut self,
    schema: IndexSchema,
  ) -> impl Future<Output = Result<(), EngineError>> + 'a {
    async move {
      let key = index_schema_entry_key(schema.name.clone());
      let value = encode_row_bytes(&encode_index_schema(&schema));
      self
        .inner
        .insert(INDEX_SCHEMA_TREE, key, value)
        .await
        .map_err(EngineError::from)
    }
  }

  fn remove_index_schema<'a>(
    &'a mut self,
    index_name: &'a str,
  ) -> impl Future<Output = Result<(), EngineError>> + 'a {
    async move {
      let key = index_schema_entry_key(index_name);
      self
        .inner
        .remove(INDEX_SCHEMA_TREE, &key)
        .await
        .map_err(EngineError::from)?;
      Ok(())
    }
  }

  fn load_catalog<'a>(
    &'a mut self,
  ) -> impl Future<Output = Result<(Vec<TableSchema>, Vec<IndexSchema>), EngineError>> + 'a {
    async move {
      let table_rows = collect_tree_rows(&self.inner, TABLE_SCHEMA_TREE).await?;
      let index_rows = collect_tree_rows(&self.inner, INDEX_SCHEMA_TREE).await?;
      let tables = decode_table_schema_rows(table_rows).map_err(schema_decode_error)?;
      let indexes = decode_index_schema_rows(index_rows).map_err(schema_decode_error)?;
      Ok((tables, indexes))
    }
  }
}

impl<T> IndexStore for NamedTreeEngineTransaction<T>
where
  T: NamedTreeTransaction<EngineKey, Vec<u8>> + 'static,
{
  fn insert_index_entry<'a>(
    &'a mut self,
    index: &'a IndexSchema,
    index_key: &'a EngineKey,
    row_pk: &'a PrimaryKey,
  ) -> impl Future<Output = Result<(), EngineError>> + 'a {
    async move {
      let row_pk_key = row_pk.to_engine_key();
      let composite = index.make_entry_key(index_key, &row_pk_key);
      self
        .inner
        .insert(&index_tree(&index.name), composite, Vec::new())
        .await
        .map_err(EngineError::from)
    }
  }

  fn delete_index_entry<'a>(
    &'a mut self,
    index: &'a IndexSchema,
    index_key: &'a EngineKey,
    row_pk: &'a PrimaryKey,
  ) -> impl Future<Output = Result<(), EngineError>> + 'a {
    async move {
      let row_pk_key = row_pk.to_engine_key();
      let composite = index.make_entry_key(index_key, &row_pk_key);
      self
        .inner
        .remove(&index_tree(&index.name), &composite)
        .await
        .map_err(EngineError::from)?;
      Ok(())
    }
  }

  fn range_index_entries<'a>(
    &'a self,
    index: &'a IndexSchema,
  ) -> impl Stream<Item = Result<(EngineKey, PrimaryKey), EngineError>> + 'a {
    let tree = index_tree(&index.name);
    let inner = &self.inner;
    stream! {
      let s = inner.range(&tree, ..);
      pin_mut!(s);
      while let Some(item) = s.next().await {
        yield item.map_err(EngineError::from).and_then(|(composite, _)| {
          index.split_entry_key(&composite)
            .map_err(|_e| EngineError::SchemaMismatch("failed to split entry key".into()))
            .and_then(|(index_key, row_pk_key)| {
              primary_key_from_engine_key(&row_pk_key).map(|row_pk| (index_key, row_pk))
            })
        });
      }
    }
  }
}

impl<T> TransactionControl for NamedTreeEngineTransaction<T>
where
  T: NamedTreeTransaction<EngineKey, Vec<u8>> + 'static,
{
  fn commit(self) -> impl Future<Output = Result<(), EngineError>> {
    async move { self.inner.commit().await.map_err(EngineError::from) }
  }

  fn rollback(self) -> impl Future<Output = Result<(), EngineError>> {
    async move { self.inner.rollback().await.map_err(EngineError::from) }
  }
}

/// Blanket impl: any `NamedTreeProvider<EngineKey, EngineRow>` is a valid
/// engine store.
impl<T> EngineStore for T
where
  T: Clone + NamedTreeProvider<EngineKey, Vec<u8>> + MaybeSend + MaybeSync + 'static,
{
  type Transaction = NamedTreeEngineTransaction<T::Transaction>;

  fn engine_transaction(&self) -> impl Future<Output = Result<Self::Transaction, EngineError>> {
    async move {
      self
        .begin_transaction()
        .await
        .map(NamedTreeEngineTransaction::new)
        .map_err(EngineError::from)
    }
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use crate::{
    ColumnSchema, EngineKey, EngineType, EngineValue, IndexSchema, PrimaryKey, TableSchema,
  };
  use db_core::block_on;
  use db_in_memory::InMemoryNamedBTree;

  fn sample_table_schema() -> TableSchema {
    TableSchema {
      name: "users".into(),
      columns: vec![
        ColumnSchema {
          name: "id".into(),
          data_type: EngineType::Integer,
        },
        ColumnSchema {
          name: "name".into(),
          data_type: EngineType::Text,
        },
      ],
      primary_key: vec![0],
    }
  }

  #[test]
  fn load_catalog_returns_table_and_index_schemas() {
    block_on(async {
      let store: InMemoryNamedBTree<EngineKey, Vec<u8>> = InMemoryNamedBTree::new();
      let mut tx = store.engine_transaction().await.expect("open tx");

      let table_schema = sample_table_schema();
      tx.insert_table_schema(table_schema.clone())
        .await
        .expect("insert table schema");

      let index_schema = IndexSchema {
        name: "users_name_idx".into(),
        table_name: "users".into(),
        column_indices: vec![1],
        unique: true,
      };
      tx.insert_index_schema(index_schema.clone())
        .await
        .expect("insert index schema");

      let (tables, indexes) = tx.load_catalog().await.expect("load catalog");
      assert_eq!(tables, vec![table_schema]);
      assert_eq!(indexes, vec![index_schema]);
    });
  }

  #[test]
  fn index_lookup_and_row_materialization_are_separate() {
    block_on(async {
      let store: InMemoryNamedBTree<EngineKey, Vec<u8>> = InMemoryNamedBTree::new();
      let mut tx = store.engine_transaction().await.expect("open tx");

      let row = vec![EngineValue::Integer(1), EngineValue::Text("Alice".into())];
      let primary_key = PrimaryKey::from([1_u8; 16]);

      tx.insert_table_row("users", primary_key, row.clone())
        .await
        .expect("insert row");

      let index_schema = IndexSchema {
        name: "users_name_idx".into(),
        table_name: "users".into(),
        column_indices: vec![1],
        unique: true,
      };
      let index_key = index_schema.key_for(&row).expect("build index key");
      tx.insert_index_entry(&index_schema, &index_key, &primary_key)
        .await
        .expect("insert index entry");

      tx.commit().await.expect("commit");

      let mut tx2 = store.engine_transaction().await.expect("open tx2");
      let predicate = crate::query::QualifiedPredicate::Equals(
        crate::query::QualifiedOperand::Column(crate::query::QualifiedColumn {
          table: "users".into(),
          column_index: 1,
        }),
        crate::query::QualifiedOperand::Value(EngineValue::Text("Alice".into())),
      );
      let row_pks =
        crate::store_adapter::helpers::lookup_index_row_pks(&mut tx2, &index_schema, &predicate)
          .await
          .expect("lookup pks");
      let rows =
        crate::store_adapter::helpers::materialize_rows_by_primary_keys(&mut tx2, "users", row_pks)
          .await
          .expect("materialize rows");

      assert_eq!(rows, vec![row]);
    });
  }
}
