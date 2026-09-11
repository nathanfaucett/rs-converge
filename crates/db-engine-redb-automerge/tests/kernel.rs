use std::{
    path::PathBuf,
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use db_engine::{Engine, Kernel, KernelTransaction};
use db_engine_redb_automerge::RedbAutomergeKernel;
use db_query::{
    DataDefinition, Query, QueryColumn, QueryFrom, QueryInsert, QuerySelect, Statement,
};
use db_schema::{ColumnSchema, TableSchema};
use db_value::{Row, Value, ValueType};
use futures::executor::block_on;

fn database_path() -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    std::env::temp_dir().join(format!("db-engine-redb-automerge-{nanos}.redb"))
}

#[test]
fn logical_rows_persist_in_one_kernel_transaction() {
    let path = database_path();
    let kernel = RedbAutomergeKernel::new(Arc::new(redb::Database::create(&path).unwrap()));
    block_on(async {
        let mut transaction = kernel.transaction().await.unwrap();
        transaction.create_table("people").await.unwrap();
        transaction
            .put_row(
                "people",
                Row::new(vec![Value::Integer(1)]),
                Row::new(vec![Value::Integer(1), Value::from("Ada")]),
            )
            .await
            .unwrap();
        transaction.commit().await.unwrap();

        let transaction = kernel.transaction().await.unwrap();
        assert_eq!(
            transaction
                .get_row("people", &Row::new(vec![Value::Integer(1)]))
                .await
                .unwrap(),
            Some(Row::new(vec![Value::Integer(1), Value::from("Ada")]))
        );
        transaction.rollback().await.unwrap();
    });
    std::fs::remove_file(path).unwrap();
}

#[test]
fn engine_persists_a_logical_row_through_the_public_transaction_seam() {
    let path = database_path();
    let database = Arc::new(redb::Database::create(&path).unwrap());
    let schema = TableSchema {
        name: "people".into(),
        columns: vec![
            ColumnSchema {
                name: "id".into(),
                r#type: ValueType::Integer,
                primary_key: true,
            },
            ColumnSchema {
                name: "name".into(),
                r#type: ValueType::Text,
                primary_key: false,
            },
        ],
    };
    block_on(async {
        let engine = Engine::new(RedbAutomergeKernel::new(database.clone()));
        engine
            .execute(vec![Statement::DataDefinition(
                DataDefinition::CreateTable {
                    schema,
                    if_not_exists: false,
                },
            )])
            .await
            .unwrap();
        engine
            .execute(vec![Statement::Query(Query::Insert(QueryInsert {
                table: "people".into(),
                row: Row::new(vec![Value::Integer(1), Value::from("Ada")]),
                returning: None,
            }))])
            .await
            .unwrap();

        let engine = Engine::new(RedbAutomergeKernel::new(database));
        let result = engine
            .execute(vec![Statement::Query(Query::Select(QuerySelect {
                from: QueryFrom {
                    table: "people".into(),
                    joins: vec![],
                },
                projection: vec![QueryColumn::new("people".into(), "name".into())],
                ..Default::default()
            }))])
            .await
            .unwrap();
        assert_eq!(result[0].rows, vec![Row::new(vec![Value::from("Ada")])]);
    });
    std::fs::remove_file(path).unwrap();
}

#[test]
fn rollback_discards_catalog_and_logical_row_changes() {
    let path = database_path();
    let kernel = RedbAutomergeKernel::new(Arc::new(redb::Database::create(&path).unwrap()));
    block_on(async {
        let mut transaction = kernel.transaction().await.unwrap();
        transaction
            .put_record(
                "tables",
                Row::new(vec![Value::from("people")]),
                Row::new(vec![Value::from("people")]),
            )
            .await
            .unwrap();
        transaction.create_table("people").await.unwrap();
        transaction
            .put_row(
                "people",
                Row::new(vec![Value::Integer(1)]),
                Row::new(vec![Value::Integer(1), Value::from("Ada")]),
            )
            .await
            .unwrap();
        transaction.rollback().await.unwrap();

        let transaction = kernel.transaction().await.unwrap();
        assert!(
            transaction
                .get_record("tables", &Row::new(vec![Value::from("people")]))
                .await
                .unwrap()
                .is_none()
        );
        transaction.rollback().await.unwrap();
    });
    std::fs::remove_file(path).unwrap();
}
