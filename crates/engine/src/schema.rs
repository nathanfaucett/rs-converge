use alloc::{
    string::{String, ToString},
    vec,
    vec::Vec,
};

use futures::{StreamExt, pin_mut};
use schema::{ColumnSchema, TableSchema};
use serde::{Deserialize, Serialize};
use value::{Row, Value, ValueType};

use crate::{
    EngineError, EngineResult, KernelTransaction, RowTable,
    catalog::{
        ENGINE_INDEX_FIELDS_STORAGE, ENGINE_INDICES_STORAGE, ENGINE_TABLE_FIELDS_STORAGE,
        ENGINE_TABLES_STORAGE,
    },
};

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum SchemaChange {
    CreateTable {
        table: String,
    },
    AddColumn {
        table: String,
        column: String,
        value_type: ValueType,
        default: Value,
        position: u32,
        primary_key: bool,
    },
    CreateIndex {
        index: String,
        table: String,
        unique: bool,
        columns: Vec<String>,
    },
    TombstoneTable(String),
    TombstoneColumn {
        table: String,
        column: String,
    },
    TombstoneIndex(String),
}

pub(crate) async fn ensure<T: KernelTransaction>(transaction: &mut T) -> EngineResult<()> {
    for table in [
        ENGINE_TABLES_STORAGE,
        ENGINE_TABLE_FIELDS_STORAGE,
        ENGINE_INDICES_STORAGE,
        ENGINE_INDEX_FIELDS_STORAGE,
    ] {
        transaction.ensure_table(table).await?;
    }
    Ok(())
}

fn text(value: &str) -> Value {
    Value::from(value)
}

fn table_key(table: &str) -> Row {
    Row::new(vec![text(table)])
}

fn column_key(table: &str, column: &str) -> Row {
    Row::new(vec![text(table), text(column)])
}

fn deleted(value: &Row, position: usize) -> EngineResult<bool> {
    match value.values.get(position) {
        None => Ok(false),
        Some(value) => value
            .to_bool()
            .ok_or(EngineError::custom("Invalid schema deletion state")),
    }
}

async fn set_deleted<T: KernelTransaction>(
    transaction: &mut T,
    catalog: &str,
    key: Row,
    position: usize,
) -> EngineResult<bool> {
    let mut value = transaction
        .get_entry(catalog, &key)
        .await?
        .ok_or(EngineError::InvalidQuery("Schema name not found"))?;
    if deleted(&value, position)? {
        return Ok(false);
    }
    value.values.resize(position + 1, Value::Bool(false));
    value.values[position] = Value::Bool(true);
    transaction.put_entry(catalog, key, value).await?;
    Ok(false)
}

async fn table_deleted<T: KernelTransaction>(transaction: &T, table: &str) -> EngineResult<bool> {
    let Some(value) = transaction
        .get_entry(ENGINE_TABLES_STORAGE, &table_key(table))
        .await?
    else {
        return Ok(false);
    };
    deleted(&value, 0)
}

async fn column_deleted<T: KernelTransaction>(
    transaction: &T,
    table: &str,
    column: &str,
) -> EngineResult<bool> {
    let Some(value) = transaction
        .get_entry(ENGINE_TABLE_FIELDS_STORAGE, &column_key(table, column))
        .await?
    else {
        return Ok(false);
    };
    deleted(&value, 4)
}

pub(crate) async fn index_field_deleted<T: KernelTransaction>(
    transaction: &T,
    index: &str,
    position: i64,
) -> EngineResult<bool> {
    let key = Row::new(vec![text(index), Value::Integer(position)]);
    let Some(value) = transaction
        .get_entry(ENGINE_INDEX_FIELDS_STORAGE, &key)
        .await?
    else {
        return Ok(false);
    };
    deleted(&value, 1)
}

pub(crate) async fn index_deleted<T: KernelTransaction>(
    transaction: &T,
    index: &str,
) -> EngineResult<bool> {
    let Some(value) = transaction
        .get_entry(ENGINE_INDICES_STORAGE, &table_key(index))
        .await?
    else {
        return Ok(false);
    };
    deleted(&value, 2)
}

pub(crate) async fn materialize<T: KernelTransaction>(
    transaction: &mut T,
    change: &SchemaChange,
) -> EngineResult<bool> {
    match change {
        SchemaChange::CreateTable { table } => {
            let key = table_key(table);
            if let Some(mut value) = transaction.get_entry(ENGINE_TABLES_STORAGE, &key).await? {
                value.values.resize(1, Value::Bool(false));
                value.values[0] = Value::Bool(false);
                transaction
                    .put_entry(ENGINE_TABLES_STORAGE, key, value)
                    .await?;
            } else {
                transaction
                    .put_entry(
                        ENGINE_TABLES_STORAGE,
                        key,
                        Row::new(vec![Value::Bool(false)]),
                    )
                    .await?;
            }
            Ok(false)
        }
        SchemaChange::AddColumn {
            table,
            column,
            value_type,
            default,
            position,
            primary_key,
        } => {
            let key = column_key(table, column);
            if transaction
                .get_entry(ENGINE_TABLE_FIELDS_STORAGE, &key)
                .await?
                .is_some()
            {
                return Ok(false);
            }
            transaction
                .put_entry(
                    ENGINE_TABLE_FIELDS_STORAGE,
                    key,
                    Row::new(vec![
                        (*value_type).into(),
                        default.clone(),
                        Value::Integer(i64::from(*position)),
                        Value::Bool(*primary_key),
                        Value::Bool(false),
                    ]),
                )
                .await?;
            Ok(false)
        }
        SchemaChange::CreateIndex {
            index,
            table,
            unique,
            columns,
        } => {
            let key = table_key(index);
            transaction.ensure_table(index).await?;
            transaction
                .put_entry(
                    ENGINE_INDICES_STORAGE,
                    key,
                    Row::new(vec![text(table), Value::Bool(*unique), Value::Bool(false)]),
                )
                .await?;
            for (position, column) in columns.iter().enumerate() {
                transaction
                    .put_entry(
                        ENGINE_INDEX_FIELDS_STORAGE,
                        Row::new(vec![
                            text(index),
                            Value::Integer(i64::try_from(position).map_err(|_| {
                                EngineError::custom("Index column position overflow")
                            })?),
                        ]),
                        Row::new(vec![text(column), Value::Bool(false)]),
                    )
                    .await?;
            }
            Ok(false)
        }
        SchemaChange::TombstoneTable(table) => {
            set_deleted(transaction, ENGINE_TABLES_STORAGE, table_key(table), 0).await
        }
        SchemaChange::TombstoneColumn { table, column } => {
            set_deleted(
                transaction,
                ENGINE_TABLE_FIELDS_STORAGE,
                column_key(table, column),
                4,
            )
            .await
        }
        SchemaChange::TombstoneIndex(index) => {
            set_deleted(transaction, ENGINE_INDICES_STORAGE, table_key(index), 2).await?;
            let keys = {
                let entries = transaction.scan_entries_owned(ENGINE_INDEX_FIELDS_STORAGE);
                pin_mut!(entries);
                let mut keys = Vec::new();
                while let Some(entry) = entries.next().await {
                    let (key, _) = entry?;
                    if key.values.first().and_then(Value::as_text) == Some(index.as_str()) {
                        keys.push(key);
                    }
                }
                keys
            };
            for key in keys {
                let mut value = transaction
                    .get_entry(ENGINE_INDEX_FIELDS_STORAGE, &key)
                    .await?
                    .ok_or(EngineError::InvalidQuery("Index field not found"))?;
                value.values.resize(2, Value::Bool(false));
                value.values[1] = Value::Bool(true);
                transaction
                    .put_entry(ENGINE_INDEX_FIELDS_STORAGE, key, value)
                    .await?;
            }
            transaction.drop_table(index).await?;
            Ok(false)
        }
    }
}

pub(crate) async fn lookup_table_name<T: KernelTransaction>(
    transaction: &T,
    name: &str,
) -> EngineResult<String> {
    let Some(_) = transaction
        .get_entry(ENGINE_TABLES_STORAGE, &table_key(name))
        .await?
    else {
        return Err(EngineError::InvalidQuery("Table not found"));
    };
    if table_deleted(transaction, name).await? {
        return Err(EngineError::InvalidQuery("Table not found"));
    }
    Ok(name.to_string())
}

pub(crate) async fn columns<T: KernelTransaction>(
    transaction: &T,
    table: &str,
) -> EngineResult<Vec<(String, ColumnSchema)>> {
    if table_deleted(transaction, table).await? {
        return Err(EngineError::InvalidQuery("Table not found"));
    }
    let entries = transaction.scan_entries(ENGINE_TABLE_FIELDS_STORAGE);
    pin_mut!(entries);
    let mut result = Vec::new();
    while let Some(entry) = entries.next().await {
        let (key, value) = entry?;
        if key.values.first().and_then(Value::as_text) != Some(table) {
            continue;
        }
        let Some(name) = key.values.get(1).and_then(Value::to_text) else {
            return Err(EngineError::custom("Invalid column name"));
        };
        if column_deleted(transaction, table, &name).await? {
            continue;
        }
        let value_type = value
            .values
            .first()
            .and_then(Value::to_type)
            .ok_or(EngineError::custom("Invalid column type"))?;
        let default = value
            .values
            .get(1)
            .cloned()
            .ok_or(EngineError::custom("Invalid column default"))?;
        let position = value
            .values
            .get(2)
            .and_then(Value::to_integer)
            .ok_or(EngineError::custom("Invalid column position"))?;
        let primary_key = value
            .values
            .get(3)
            .and_then(Value::to_bool)
            .ok_or(EngineError::custom("Invalid column primary key"))?;
        result.push((
            position,
            name.clone(),
            ColumnSchema {
                name,
                r#type: value_type,
                default,
                primary_key,
            },
        ));
    }
    result.sort_by(|a, b| (a.0, &a.1).cmp(&(b.0, &b.1)));
    Ok(result
        .into_iter()
        .map(|(_, name, schema)| (name, schema))
        .collect())
}

pub(crate) async fn table_schema<T: KernelTransaction>(
    transaction: &T,
    name: &str,
) -> EngineResult<TableSchema> {
    lookup_table_name(transaction, name).await?;
    table_schema_for(transaction, name, name.to_string()).await
}

pub(crate) async fn table_schema_for<T: KernelTransaction>(
    transaction: &T,
    table: &str,
    name: String,
) -> EngineResult<TableSchema> {
    Ok(TableSchema {
        name,
        columns: columns(transaction, table)
            .await?
            .into_iter()
            .map(|(_, column)| column)
            .collect(),
    })
}

pub(crate) async fn active_table_names<T: KernelTransaction>(
    transaction: &T,
) -> EngineResult<Vec<String>> {
    let entries = transaction.scan_entries(ENGINE_TABLES_STORAGE);
    pin_mut!(entries);
    let mut tables = Vec::new();
    while let Some(entry) = entries.next().await {
        let (key, _) = entry?;
        let name = key
            .values
            .first()
            .and_then(Value::to_text)
            .ok_or(EngineError::custom("Invalid table name"))?;
        if !table_deleted(transaction, &name).await? {
            tables.push(name);
        }
    }
    Ok(tables)
}

#[cfg(all(test, feature = "in-memory"))]
mod tests {
    use futures::executor::block_on;
    use value::ValueType;

    use super::{
        SchemaChange, active_table_names, columns, index_deleted, materialize, table_schema,
    };
    use crate::{InMemoryKernel, Kernel, KernelTransaction, index::index_schema};

    #[test]
    fn dropped_schema_names_are_hidden_and_can_be_recreated() {
        block_on(async {
            let kernel = InMemoryKernel::new();
            let mut transaction = kernel.transaction().await.unwrap();
            super::ensure(&mut transaction).await.unwrap();
            assert!(table_schema(&transaction, "missing").await.is_err());
            materialize(
                &mut transaction,
                &SchemaChange::CreateTable {
                    table: "items".into(),
                },
            )
            .await
            .unwrap();
            materialize(
                &mut transaction,
                &SchemaChange::AddColumn {
                    table: "items".into(),
                    column: "id".into(),
                    value_type: ValueType::Uuid,
                    default: value::Value::Null,
                    position: 0,
                    primary_key: true,
                },
            )
            .await
            .unwrap();
            materialize(
                &mut transaction,
                &SchemaChange::TombstoneColumn {
                    table: "items".into(),
                    column: "id".into(),
                },
            )
            .await
            .unwrap();
            materialize(
                &mut transaction,
                &SchemaChange::TombstoneTable("items".into()),
            )
            .await
            .unwrap();

            assert!(table_schema(&transaction, "items").await.is_err());
            assert!(columns(&transaction, "items").await.is_err());
            assert!(active_table_names(&transaction).await.unwrap().is_empty());

            materialize(
                &mut transaction,
                &SchemaChange::CreateTable {
                    table: "items".into(),
                },
            )
            .await
            .unwrap();
            assert!(table_schema(&transaction, "items").await.is_ok());
            assert!(columns(&transaction, "items").await.unwrap().is_empty());
            assert_eq!(active_table_names(&transaction).await.unwrap(), ["items"]);
            transaction.rollback().await.unwrap();
        });
    }

    #[test]
    fn dropped_index_is_hidden_and_recreation_resets_its_schema() {
        block_on(async {
            let kernel = InMemoryKernel::new();
            let mut transaction = kernel.transaction().await.unwrap();
            super::ensure(&mut transaction).await.unwrap();
            assert!(
                index_schema(&transaction, "missing")
                    .await
                    .unwrap()
                    .is_none()
            );
            materialize(
                &mut transaction,
                &SchemaChange::CreateTable {
                    table: "items".into(),
                },
            )
            .await
            .unwrap();
            materialize(
                &mut transaction,
                &SchemaChange::AddColumn {
                    table: "items".into(),
                    column: "id".into(),
                    value_type: ValueType::Uuid,
                    default: value::Value::Null,
                    position: 0,
                    primary_key: true,
                },
            )
            .await
            .unwrap();
            let index = SchemaChange::CreateIndex {
                index: "items_by_id".into(),
                table: "items".into(),
                unique: true,
                columns: vec!["id".into()],
            };
            materialize(&mut transaction, &index).await.unwrap();
            materialize(
                &mut transaction,
                &SchemaChange::TombstoneIndex("items_by_id".into()),
            )
            .await
            .unwrap();
            assert!(index_deleted(&transaction, "items_by_id").await.unwrap());
            assert!(
                index_schema(&transaction, "items_by_id")
                    .await
                    .unwrap()
                    .is_none()
            );

            materialize(&mut transaction, &index).await.unwrap();
            let schema = index_schema(&transaction, "items_by_id")
                .await
                .unwrap()
                .unwrap();
            assert_eq!(schema.table_name, "items");
            assert_eq!(schema.column_indices, [0]);
            assert!(!index_deleted(&transaction, "items_by_id").await.unwrap());
            transaction.rollback().await.unwrap();
        });
    }
}
