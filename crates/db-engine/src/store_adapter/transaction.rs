use core::future::Future;
use db_core::MaybeSend;
use futures::Stream;

use crate::{EngineError, EngineKey, EngineRow, IndexSchema, PrimaryKey, TableSchema};

/// A minimal read-only engine-level storage transaction.
///
/// This contract exposes row access, catalog schema management, and index reads
/// without any write lifecycle control.
pub trait EngineStoreReadTransaction: MaybeSend + 'static {
  fn get_table_row<'a>(
    &'a mut self,
    table_name: &'a str,
    primary_key: &'a PrimaryKey,
  ) -> impl Future<Output = Result<Option<EngineRow>, EngineError>> + 'a;

  fn range_table_rows<'a>(
    &'a self,
    table_name: &'a str,
  ) -> impl Stream<Item = Result<(PrimaryKey, EngineRow), EngineError>> + 'a;

  fn load_catalog<'a>(
    &'a mut self,
  ) -> impl Future<Output = Result<(Vec<TableSchema>, Vec<IndexSchema>), EngineError>> + 'a;

  fn range_index_entries<'a>(
    &'a self,
    index: &'a IndexSchema,
  ) -> impl Stream<Item = Result<(EngineKey, PrimaryKey), EngineError>> + 'a;
}

/// A full engine-level storage transaction.
///
/// This contract exposes row access, catalog schema management, index maintenance,
/// and lifecycle control in one cohesive transaction interface.
pub trait EngineStoreTransaction: EngineStoreReadTransaction + MaybeSend + 'static {
  fn insert_table_row<'a>(
    &'a mut self,
    table_name: &'a str,
    primary_key: PrimaryKey,
    row: EngineRow,
  ) -> impl Future<Output = Result<(), EngineError>> + 'a;

  fn remove_table_row<'a>(
    &'a mut self,
    table_name: &'a str,
    primary_key: &'a PrimaryKey,
  ) -> impl Future<Output = Result<Option<EngineRow>, EngineError>> + 'a;

  fn insert_table_schema<'a>(
    &'a mut self,
    schema: TableSchema,
  ) -> impl Future<Output = Result<(), EngineError>> + 'a;

  fn remove_table_schema<'a>(
    &'a mut self,
    table_name: &'a str,
  ) -> impl Future<Output = Result<(), EngineError>> + 'a;

  fn insert_index_schema<'a>(
    &'a mut self,
    schema: IndexSchema,
  ) -> impl Future<Output = Result<(), EngineError>> + 'a;

  fn remove_index_schema<'a>(
    &'a mut self,
    index_name: &'a str,
  ) -> impl Future<Output = Result<(), EngineError>> + 'a;

  fn insert_index_entry<'a>(
    &'a mut self,
    index: &'a IndexSchema,
    index_key: &'a EngineKey,
    row_pk: &'a PrimaryKey,
  ) -> impl Future<Output = Result<(), EngineError>> + 'a;

  fn delete_index_entry<'a>(
    &'a mut self,
    index: &'a IndexSchema,
    index_key: &'a EngineKey,
    row_pk: &'a PrimaryKey,
  ) -> impl Future<Output = Result<(), EngineError>> + 'a;

  fn commit(self) -> impl Future<Output = Result<(), EngineError>>;

  fn rollback(self) -> impl Future<Output = Result<(), EngineError>>;
}
