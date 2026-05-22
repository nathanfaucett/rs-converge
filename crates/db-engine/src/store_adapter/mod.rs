#![allow(clippy::manual_async_fn)]

use crate::{EngineError, EngineKey, EngineRow};
use core::future::Future;
use db_core::{MaybeSend, MaybeSync, NamedTreeProvider, NamedTreeTransaction};
use futures::{StreamExt, pin_mut};

mod backend_contract;
mod contract;
mod helpers;
mod named_tree;
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
use named_tree::NamedTreeEngineTransaction;
pub use transaction::EngineStoreTransaction;

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

/// Explicit adapter for named-tree backends.
///
/// This is the engine-level wrapper that maps raw named-tree storage into the
/// engine store contract.
pub struct NamedTreeEngineStore<T>
where
  T: Clone + NamedTreeProvider<EngineKey, Vec<u8>> + MaybeSend + MaybeSync + 'static,
{
  inner: T,
}

impl<T> NamedTreeEngineStore<T>
where
  T: Clone + NamedTreeProvider<EngineKey, Vec<u8>> + MaybeSend + MaybeSync + 'static,
{
  pub fn new(inner: T) -> Self {
    Self { inner }
  }

  pub fn inner(&self) -> &T {
    &self.inner
  }
}

impl<T> Clone for NamedTreeEngineStore<T>
where
  T: Clone + NamedTreeProvider<EngineKey, Vec<u8>> + MaybeSend + MaybeSync + 'static,
{
  fn clone(&self) -> Self {
    Self {
      inner: self.inner.clone(),
    }
  }
}

impl<T> From<T> for NamedTreeEngineStore<T>
where
  T: Clone + NamedTreeProvider<EngineKey, Vec<u8>> + MaybeSend + MaybeSync + 'static,
{
  fn from(inner: T) -> Self {
    NamedTreeEngineStore::new(inner)
  }
}

impl<T> EngineStore for NamedTreeEngineStore<T>
where
  T: Clone + NamedTreeProvider<EngineKey, Vec<u8>> + MaybeSend + MaybeSync + 'static,
{
  type Transaction = NamedTreeEngineTransaction<T::Transaction>;

  fn engine_transaction(&self) -> impl Future<Output = Result<Self::Transaction, EngineError>> {
    let inner = &self.inner;
    async move {
      inner
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
      let store = NamedTreeEngineStore::new(store);
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
      let store = NamedTreeEngineStore::new(store);
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
