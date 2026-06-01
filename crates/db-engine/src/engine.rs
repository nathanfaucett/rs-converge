#[cfg(not(feature = "std"))]
use alloc::{
  string::{String, ToString},
  sync::Arc,
  vec::Vec,
};
#[cfg(feature = "std")]
use std::sync::Arc;

use futures::{StreamExt, pin_mut};
use thiserror::Error;

use crate::{
  BTree, BTreeDefinition, BTreeError, BTreeManager, BTreeReadExecutor, BTreeTransaction,
  BTreeWriteExecutor, DescribeSchema, IndexSchema, Query, QueryParams, Row, TableSchema,
  TranslateError, Translator, Value,
  catalog::{
    ENGINE_INDEX_FIELDS, ENGINE_INDICES, ENGINE_TABLE_FIELDS, ENGINE_TABLES, IndexFieldRow,
    TableFieldRow, decode_index_field_row, decode_index_row, decode_table_field_row,
    decode_table_row, encode_index_field_row, encode_index_row, encode_table_field_row,
    encode_table_row, index_field_key, index_key, index_schema_from_rows, table_field_key,
    table_key, table_schema_from_rows,
  },
};

#[derive(Error, Debug)]
pub enum EngineError {
  #[error("Translate error: {0}")]
  TranslateError(#[from] TranslateError),

  #[error("BTree error: {0}")]
  BTreeError(#[from] BTreeError),

  #[error("Unsupported query shape for MVP executor: {0}")]
  Unsupported(&'static str),

  #[error("Invalid query: {0}")]
  InvalidQuery(&'static str),
}

pub type EngineResult<T> = Result<T, EngineError>;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct EngineBTreeDefinition {
  pub table_name: String,
}

impl From<String> for EngineBTreeDefinition {
  fn from(table_name: String) -> Self {
    Self { table_name }
  }
}

impl From<&str> for EngineBTreeDefinition {
  fn from(table_name: &str) -> Self {
    Self {
      table_name: table_name.to_string(),
    }
  }
}

impl BTreeDefinition for EngineBTreeDefinition {
  type Key = Vec<Value>;
  type Value = Row;

  fn id(&self) -> &str {
    &self.table_name
  }
}

pub struct Engine<M> {
  pub manager: Arc<M>,
}

impl<M> From<M> for Engine<M> {
  fn from(manager: M) -> Self {
    Self {
      manager: Arc::new(manager),
    }
  }
}

impl<M> Engine<M> {
  pub fn new(manager: M) -> Self {
    Self::from(manager)
  }
}

impl<M> DescribeSchema for Engine<M>
where
  M: BTreeManager,
{
  async fn describe_table(&self, table_name: &str) -> Option<TableSchema> {
    let definition = EngineBTreeDefinition::from(ENGINE_TABLES);
    let btree = self.manager.get(&definition).await.ok()?;

    let mut exists = false;
    {
      let stream = btree.range(..);
      pin_mut!(stream);
      while let Some(entry) = stream.next().await {
        let (_key, row) = entry.ok()?;
        if decode_table_row(&row).is_some_and(|name| name == table_name) {
          exists = true;
          break;
        }
      }
    }

    if !exists {
      return None;
    }

    let field_definition = EngineBTreeDefinition::from(ENGINE_TABLE_FIELDS);
    let field_btree = self.manager.get(&field_definition).await.ok()?;

    let mut fields = Vec::new();
    {
      let stream = field_btree.range(..);
      pin_mut!(stream);
      while let Some(entry) = stream.next().await {
        let (_key, row) = entry.ok()?;
        if let Some(field) = decode_table_field_row(&row)
          && field.table_name == table_name
        {
          fields.push(field);
        }
      }
    }

    Some(table_schema_from_rows(table_name, &fields))
  }
}

impl<M> Engine<M>
where
  M: BTreeManager,
{
  pub async fn describe_index(&self, index_name: &str) -> Option<IndexSchema> {
    let definition = EngineBTreeDefinition::from(ENGINE_INDICES);
    let btree = self.manager.get(&definition).await.ok()?;

    let mut index_row = None;
    {
      let stream = btree.range(..);
      pin_mut!(stream);
      while let Some(entry) = stream.next().await {
        let (_key, row) = entry.ok()?;
        if let Some(index) = decode_index_row(&row)
          && index.index_name == index_name
        {
          index_row = Some(index);
          break;
        }
      }
    }

    let index_row = index_row?;

    let field_definition = EngineBTreeDefinition::from(ENGINE_INDEX_FIELDS);
    let field_btree = self.manager.get(&field_definition).await.ok()?;

    let mut fields = Vec::new();
    {
      let stream = field_btree.range(..);
      pin_mut!(stream);
      while let Some(entry) = stream.next().await {
        let (_key, row) = entry.ok()?;
        if let Some(field) = decode_index_field_row(&row)
          && field.index_name == index_name
        {
          fields.push(field);
        }
      }
    }

    Some(index_schema_from_rows(index_row, &fields))
  }

  pub async fn register_table_schema(&self, schema: &TableSchema) -> EngineResult<()> {
    let definition = EngineBTreeDefinition::from(ENGINE_TABLES);
    let btree = self.manager.get(&definition).await?;
    let mut tx = btree.transaction().await?;
    tx.insert(table_key(&schema.name), encode_table_row(&schema.name))
      .await?;
    tx.commit().await?;

    let definition = EngineBTreeDefinition::from(ENGINE_TABLE_FIELDS);
    let btree = self.manager.get(&definition).await?;
    let mut tx = btree.transaction().await?;

    let mut keys_to_delete = Vec::new();
    {
      let stream = tx.range(..);
      pin_mut!(stream);
      while let Some(entry) = stream.next().await {
        let (key, row) = entry?;
        if let Some(field_row) = decode_table_field_row(&row)
          && field_row.table_name == schema.name
        {
          keys_to_delete.push(key);
        }
      }
    }

    for key in keys_to_delete {
      tx.remove(key).await?;
    }

    for (offset, column) in schema.columns.iter().enumerate() {
      let column_index = u8::try_from(offset)
        .map_err(|_| EngineError::InvalidQuery("table schema has too many columns"))?;
      let field_row = TableFieldRow {
        table_name: schema.name.clone(),
        column_index,
        column_name: column.name.clone(),
        value_type: column.r#type,
        primary_key: schema.primary_key_index.contains(&column_index),
      };

      tx.insert(
        table_field_key(&schema.name, column_index),
        encode_table_field_row(&schema.name, &field_row),
      )
      .await?;
    }

    tx.commit().await?;
    Ok(())
  }

  pub async fn register_index_schema(&self, schema: &IndexSchema) -> EngineResult<()> {
    let definition = EngineBTreeDefinition::from(ENGINE_INDICES);
    let btree = self.manager.get(&definition).await?;
    let mut tx = btree.transaction().await?;
    tx.insert(index_key(&schema.name), encode_index_row(schema))
      .await?;
    tx.commit().await?;

    let definition = EngineBTreeDefinition::from(ENGINE_INDEX_FIELDS);
    let btree = self.manager.get(&definition).await?;
    let mut tx = btree.transaction().await?;

    let mut keys_to_delete = Vec::new();
    {
      let stream = tx.range(..);
      pin_mut!(stream);
      while let Some(entry) = stream.next().await {
        let (key, row) = entry?;
        if let Some(field_row) = decode_index_field_row(&row)
          && field_row.index_name == schema.name
        {
          keys_to_delete.push(key);
        }
      }
    }

    for key in keys_to_delete {
      tx.remove(key).await?;
    }

    for (offset, column_index) in schema.column_indices.iter().enumerate() {
      let field_order = u8::try_from(offset)
        .map_err(|_| EngineError::InvalidQuery("index schema has too many columns"))?;
      let field_row = IndexFieldRow {
        index_name: schema.name.clone(),
        field_order,
        column_index: *column_index,
      };

      tx.insert(
        index_field_key(&schema.name, field_order),
        encode_index_field_row(&schema.name, &field_row),
      )
      .await?;
    }

    tx.commit().await?;
    Ok(())
  }

  pub async fn translate_and_execute_with_params<T>(
    &self,
    query: &str,
    params: Option<&QueryParams>,
    translator: T,
  ) -> EngineResult<Vec<Row>>
  where
    T: Translator,
  {
    let q = translator
      .translate_with_params(query, params, self)
      .await?;
    self.execute(q).await
  }

  pub async fn translate_and_execute<T>(&self, query: &str, translator: T) -> EngineResult<Vec<Row>>
  where
    T: Translator,
  {
    self
      .translate_and_execute_with_params(query, None, translator)
      .await
  }

  pub async fn execute(&self, query: Query) -> EngineResult<Vec<Row>> {
    crate::executor::execute(self, query).await
  }
}

#[cfg(all(test, feature = "in-memory"))]
mod tests {
  use super::*;

  use futures::{StreamExt, executor::block_on, pin_mut};

  use crate::{ColumnSchema, InMemoryBTreeManager, ValueType};

  #[test]
  fn describe_table_reads_normalized_catalogs() {
    block_on(async {
      let engine = Engine::new(InMemoryBTreeManager::new());
      let schema = TableSchema {
        name: "users".to_string(),
        columns: vec![
          ColumnSchema {
            name: "id".to_string(),
            r#type: ValueType::Uuid,
          },
          ColumnSchema {
            name: "name".to_string(),
            r#type: ValueType::Text,
          },
        ],
        primary_key_index: vec![0],
      };

      engine
        .register_table_schema(&schema)
        .await
        .expect("register table schema");

      let described = engine.describe_table("users").await;
      assert_eq!(described, Some(schema));
      assert_eq!(engine.describe_table("missing").await, None);
    });
  }

  #[test]
  fn describe_index_reads_normalized_catalogs() {
    block_on(async {
      let engine = Engine::new(InMemoryBTreeManager::new());
      let schema = IndexSchema {
        name: "users_name_idx".to_string(),
        table_name: "users".to_string(),
        column_indices: vec![1, 0],
        unique: true,
      };

      engine
        .register_index_schema(&schema)
        .await
        .expect("register index schema");

      let described = engine.describe_index("users_name_idx").await;
      assert_eq!(described, Some(schema));
      assert_eq!(engine.describe_index("missing_idx").await, None);
    });
  }

  #[test]
  fn registration_writes_normalized_system_tables() {
    block_on(async {
      let engine = Engine::new(InMemoryBTreeManager::new());

      let table_schema = TableSchema {
        name: "users".to_string(),
        columns: vec![
          ColumnSchema {
            name: "id".to_string(),
            r#type: ValueType::Uuid,
          },
          ColumnSchema {
            name: "name".to_string(),
            r#type: ValueType::Text,
          },
          ColumnSchema {
            name: "email".to_string(),
            r#type: ValueType::Text,
          },
        ],
        primary_key_index: vec![0],
      };
      let index_schema = IndexSchema {
        name: "users_email_idx".to_string(),
        table_name: "users".to_string(),
        column_indices: vec![2],
        unique: true,
      };

      engine
        .register_table_schema(&table_schema)
        .await
        .expect("register table schema");
      engine
        .register_index_schema(&index_schema)
        .await
        .expect("register index schema");

      assert_eq!(count_rows(&engine, ENGINE_TABLES).await, 1);
      assert_eq!(count_rows(&engine, ENGINE_TABLE_FIELDS).await, 3);
      assert_eq!(count_rows(&engine, ENGINE_INDICES).await, 1);
      assert_eq!(count_rows(&engine, ENGINE_INDEX_FIELDS).await, 1);
    });
  }

  async fn count_rows(engine: &Engine<InMemoryBTreeManager>, table_name: &str) -> usize {
    let definition = EngineBTreeDefinition::from(table_name);
    let btree = engine
      .manager
      .get(&definition)
      .await
      .expect("get btree for table");

    let stream = btree.range(..);
    pin_mut!(stream);

    let mut count = 0;
    while let Some(entry) = stream.next().await {
      entry.expect("range item");
      count += 1;
    }

    count
  }
}
