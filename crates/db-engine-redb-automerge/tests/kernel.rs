use std::{
    path::PathBuf,
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use db_engine::{ChangeReplication, DirectRowCodec, Engine, Kernel, KernelTransaction, RowCodec};
use db_engine_redb_automerge::{AutomergeRowCodec, RedbKernel};
use db_query::{
    AlterTableOperation, DataDefinition, Query, QueryColumn, QueryDelete, QueryExpr,
    QueryExprValue, QueryFrom, QueryInsert, QuerySelect, QueryUpdate, QueryUpdateAssignment,
    Statement,
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

fn people_schema() -> TableSchema {
    TableSchema {
        name: "people".into(),
        columns: vec![
            ColumnSchema {
                name: "id".into(),
                r#type: ValueType::Integer,
                default: Value::Null,
                primary_key: true,
            },
            ColumnSchema {
                name: "name".into(),
                r#type: ValueType::Text,
                default: Value::Null,
                primary_key: false,
            },
            ColumnSchema {
                name: "city".into(),
                r#type: ValueType::Text,
                default: Value::Null,
                primary_key: false,
            },
        ],
    }
}

fn replica(path: &PathBuf) -> Engine<RedbKernel, AutomergeRowCodec> {
    Engine::new(
        RedbKernel::new(Arc::new(redb::Database::create(path).unwrap())),
        AutomergeRowCodec,
    )
}

fn id(value: i64) -> Option<QueryExpr> {
    Some(QueryExpr::Equals(
        Box::new(QueryExpr::Value(QueryExprValue::Column(QueryColumn::new(
            "people".into(),
            "id".into(),
        )))),
        Box::new(QueryExpr::Value(QueryExprValue::Value(Value::Integer(
            value,
        )))),
    ))
}

async fn people_rows(engine: &Engine<RedbKernel, AutomergeRowCodec>, columns: &[&str]) -> Vec<Row> {
    engine
        .execute(vec![Statement::Query(Query::Select(QuerySelect {
            from: QueryFrom {
                table: "people".into(),
                joins: vec![],
            },
            projection: columns
                .iter()
                .map(|column| QueryColumn::new("people".into(), (*column).into()))
                .collect(),
            ..Default::default()
        }))])
        .await
        .unwrap()[0]
        .rows
        .clone()
}

async fn update(engine: &Engine<RedbKernel, AutomergeRowCodec>, column: &str, value: Value) {
    engine
        .execute(vec![Statement::Query(Query::Update(QueryUpdate {
            from: QueryFrom {
                table: "people".into(),
                joins: vec![],
            },
            assignments: vec![QueryUpdateAssignment {
                column: QueryColumn::new("people".into(), column.into()),
                value: QueryExprValue::Value(value),
            }],
            predicate: id(1),
            returning: None,
        }))])
        .await
        .unwrap();
}

async fn add_column(engine: &Engine<RedbKernel, AutomergeRowCodec>, name: &str, default: Value) {
    engine
        .execute(vec![Statement::DataDefinition(
            DataDefinition::AlterTable {
                table_name: "people".into(),
                operations: vec![AlterTableOperation::AddColumn(ColumnSchema {
                    name: name.into(),
                    r#type: ValueType::Text,
                    default,
                    primary_key: false,
                })],
                if_exists: false,
            },
        )])
        .await
        .unwrap();
}

#[test]
fn logical_rows_persist_in_one_kernel_transaction() {
    let path = database_path();
    let kernel = RedbKernel::new(Arc::new(redb::Database::create(&path).unwrap()));
    block_on(async {
        let reconciler = AutomergeRowCodec;
        let mut transaction = kernel.transaction().await.unwrap();
        reconciler
            .ensure_table(&mut transaction, "people")
            .await
            .unwrap();
        reconciler
            .put_row(
                &mut transaction,
                "people",
                Row::new(vec![Value::Integer(1)]),
                Row::new(vec![Value::Integer(1), Value::from("Ada")]),
            )
            .await
            .unwrap();
        transaction.commit().await.unwrap();

        let transaction = kernel.transaction().await.unwrap();
        assert_eq!(
            reconciler
                .get_row(&transaction, "people", &Row::new(vec![Value::Integer(1)]))
                .await
                .unwrap(),
            Some(Row::new(vec![Value::Integer(1), Value::from("Ada")]))
        );
        transaction.rollback().await.unwrap();
    });
    std::fs::remove_file(path).unwrap();
}

#[test]
fn redb_kernel_pairs_with_a_non_automerge_reconciler() {
    let path = database_path();
    let kernel = RedbKernel::new(Arc::new(redb::Database::create(&path).unwrap()));
    block_on(async {
        let reconciler = DirectRowCodec;
        let key = Row::new(vec![Value::Integer(1)]);
        let row = Row::new(vec![Value::Integer(1), Value::from("Ada")]);
        let mut transaction = kernel.transaction().await.unwrap();
        reconciler
            .ensure_table(&mut transaction, "people")
            .await
            .unwrap();
        reconciler
            .put_row(&mut transaction, "people", key.clone(), row.clone())
            .await
            .unwrap();
        transaction.commit().await.unwrap();

        let transaction = kernel.transaction().await.unwrap();
        assert_eq!(
            reconciler
                .get_row(&transaction, "people", &key)
                .await
                .unwrap(),
            Some(row)
        );
        transaction.rollback().await.unwrap();
    });
    std::fs::remove_file(path).unwrap();
}

#[test]
fn removing_a_logical_row_writes_a_tombstone() {
    let path = database_path();
    let kernel = RedbKernel::new(Arc::new(redb::Database::create(&path).unwrap()));
    block_on(async {
        let reconciler = AutomergeRowCodec;
        let key = Row::new(vec![Value::Integer(1)]);
        let mut transaction = kernel.transaction().await.unwrap();
        reconciler
            .ensure_table(&mut transaction, "people")
            .await
            .unwrap();
        reconciler
            .put_row(
                &mut transaction,
                "people",
                key.clone(),
                Row::new(vec![Value::Integer(1), Value::from("Ada")]),
            )
            .await
            .unwrap();
        assert!(
            reconciler
                .remove_row(&mut transaction, "people", &key)
                .await
                .unwrap()
                .is_some()
        );
        transaction.commit().await.unwrap();

        let transaction = kernel.transaction().await.unwrap();
        assert!(
            reconciler
                .get_row(&transaction, "people", &key)
                .await
                .unwrap()
                .is_none()
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
                default: Value::Null,
                primary_key: true,
            },
            ColumnSchema {
                name: "name".into(),
                r#type: ValueType::Text,
                default: Value::Null,
                primary_key: false,
            },
        ],
    };
    block_on(async {
        let engine = Engine::new(RedbKernel::new(database.clone()), AutomergeRowCodec);
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

        let engine = Engine::new(RedbKernel::new(database), AutomergeRowCodec);
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
fn received_automerge_incremental_change_materializes_a_row() {
    let source_path = database_path();
    let destination_path = database_path();
    block_on(async {
        let schema = TableSchema {
            name: "people".into(),
            columns: vec![
                ColumnSchema {
                    name: "id".into(),
                    r#type: ValueType::Integer,
                    default: Value::Null,
                    primary_key: true,
                },
                ColumnSchema {
                    name: "name".into(),
                    r#type: ValueType::Text,
                    default: Value::Null,
                    primary_key: false,
                },
            ],
        };
        let source = Engine::new(
            RedbKernel::new(Arc::new(redb::Database::create(&source_path).unwrap())),
            AutomergeRowCodec,
        );
        let destination = Engine::new(
            RedbKernel::new(Arc::new(redb::Database::create(&destination_path).unwrap())),
            AutomergeRowCodec,
        );
        for engine in [&source, &destination] {
            engine.create_table(schema.clone()).await.unwrap();
        }
        source
            .execute(vec![Statement::Query(Query::Insert(QueryInsert {
                table: "people".into(),
                row: Row::new(vec![Value::Integer(1), Value::from("Ada")]),
                returning: None,
            }))])
            .await
            .unwrap();

        let (_, changes) = source.changes_since(None).await.unwrap();
        destination.apply_changes(changes).await.unwrap();
        let results = destination
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
        assert_eq!(results[0].rows, vec![Row::new(vec![Value::from("Ada")])]);
    });
    std::fs::remove_file(source_path).unwrap();
    std::fs::remove_file(destination_path).unwrap();
}

#[test]
fn rollback_discards_catalog_and_logical_row_changes() {
    let path = database_path();
    let kernel = RedbKernel::new(Arc::new(redb::Database::create(&path).unwrap()));
    block_on(async {
        let reconciler = AutomergeRowCodec;
        let mut transaction = kernel.transaction().await.unwrap();
        transaction.ensure_table("tables").await.unwrap();
        reconciler
            .ensure_table(&mut transaction, "people")
            .await
            .unwrap();
        transaction
            .put_entry(
                "tables",
                Row::new(vec![Value::from("people")]),
                Row::new(vec![Value::from("people")]),
            )
            .await
            .unwrap();
        reconciler
            .put_row(
                &mut transaction,
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
                .get_entry("tables", &Row::new(vec![Value::from("people")]))
                .await
                .unwrap()
                .is_none()
        );
        transaction.rollback().await.unwrap();
    });
    std::fs::remove_file(path).unwrap();
}

#[test]
fn bootstrap_destination_exclusively_from_source_canonical_changes() {
    let source_path = database_path();
    let destination_path = database_path();
    block_on(async {
        let source = replica(&source_path);
        let destination = replica(&destination_path);
        source.create_table(people_schema()).await.unwrap();
        source
            .execute(vec![Statement::Query(Query::Insert(QueryInsert {
                table: "people".into(),
                row: Row::new(vec![
                    Value::Integer(1),
                    Value::from("Ada"),
                    Value::from("London"),
                ]),
                returning: None,
            }))])
            .await
            .unwrap();

        let (_, changes) = source.changes_since(None).await.unwrap();
        destination.apply_changes(changes).await.unwrap();
        assert_eq!(
            people_rows(&destination, &["id", "name", "city"]).await,
            vec![Row::new(vec![
                Value::Integer(1),
                Value::from("Ada"),
                Value::from("London"),
            ])]
        );
    });
    std::fs::remove_file(source_path).unwrap();
    std::fs::remove_file(destination_path).unwrap();
}

#[test]
fn concurrent_different_column_updates_converge() {
    let source_path = database_path();
    let destination_path = database_path();
    block_on(async {
        let source = replica(&source_path);
        let destination = replica(&destination_path);
        source.create_table(people_schema()).await.unwrap();
        source
            .execute(vec![Statement::Query(Query::Insert(QueryInsert {
                table: "people".into(),
                row: Row::new(vec![Value::Integer(1), Value::from("Ada"), Value::Null]),
                returning: None,
            }))])
            .await
            .unwrap();
        let (_, changes) = source.changes_since(None).await.unwrap();
        destination.apply_changes(changes).await.unwrap();
        let (source_cursor, _) = source.changes_since(None).await.unwrap();
        let (destination_cursor, _) = destination.changes_since(None).await.unwrap();

        update(&source, "name", Value::from("Grace")).await;
        update(&destination, "city", Value::from("Paris")).await;
        let (_, changes) = source.changes_since(Some(&source_cursor)).await.unwrap();
        destination.apply_changes(changes).await.unwrap();
        let (_, changes) = destination
            .changes_since(Some(&destination_cursor))
            .await
            .unwrap();
        source.apply_changes(changes).await.unwrap();

        let expected = vec![Row::new(vec![Value::from("Grace"), Value::from("Paris")])];
        assert_eq!(people_rows(&source, &["name", "city"]).await, expected);
        assert_eq!(
            people_rows(&destination, &["name", "city"]).await,
            people_rows(&source, &["name", "city"]).await
        );
    });
    std::fs::remove_file(source_path).unwrap();
    std::fs::remove_file(destination_path).unwrap();
}

#[test]
fn concurrent_same_column_updates_reject_atomically() {
    let source_path = database_path();
    let destination_path = database_path();
    block_on(async {
        let source = replica(&source_path);
        let destination = replica(&destination_path);
        source.create_table(people_schema()).await.unwrap();
        source
            .execute(vec![Statement::Query(Query::Insert(QueryInsert {
                table: "people".into(),
                row: Row::new(vec![Value::Integer(1), Value::from("Ada"), Value::Null]),
                returning: None,
            }))])
            .await
            .unwrap();
        let (_, changes) = source.changes_since(None).await.unwrap();
        destination.apply_changes(changes).await.unwrap();
        let (source_cursor, _) = source.changes_since(None).await.unwrap();

        update(&source, "name", Value::from("Grace")).await;
        update(&destination, "name", Value::from("Linus")).await;
        let (_, changes) = source.changes_since(Some(&source_cursor)).await.unwrap();
        assert!(destination.apply_changes(changes).await.is_err());
        assert_eq!(
            people_rows(&destination, &["name"]).await,
            vec![Row::new(vec![Value::from("Linus")])]
        );
    });
    std::fs::remove_file(source_path).unwrap();
    std::fs::remove_file(destination_path).unwrap();
}

#[test]
fn tombstoned_incoming_updates_reject() {
    let source_path = database_path();
    let destination_path = database_path();
    block_on(async {
        let source = replica(&source_path);
        let destination = replica(&destination_path);
        source.create_table(people_schema()).await.unwrap();
        source
            .execute(vec![Statement::Query(Query::Insert(QueryInsert {
                table: "people".into(),
                row: Row::new(vec![Value::Integer(1), Value::from("Ada"), Value::Null]),
                returning: None,
            }))])
            .await
            .unwrap();
        let (_, changes) = source.changes_since(None).await.unwrap();
        destination.apply_changes(changes).await.unwrap();
        let (source_cursor, _) = source.changes_since(None).await.unwrap();

        update(&source, "name", Value::from("Grace")).await;
        destination
            .execute(vec![Statement::Query(Query::Delete(QueryDelete {
                from: QueryFrom {
                    table: "people".into(),
                    joins: vec![],
                },
                predicate: id(1),
                returning: None,
            }))])
            .await
            .unwrap();
        let (_, changes) = source.changes_since(Some(&source_cursor)).await.unwrap();
        assert!(destination.apply_changes(changes).await.is_err());
        assert!(people_rows(&destination, &["id"]).await.is_empty());
    });
    std::fs::remove_file(source_path).unwrap();
    std::fs::remove_file(destination_path).unwrap();
}

#[test]
fn schema_add_column_then_row_mutation_replicates() {
    let source_path = database_path();
    let destination_path = database_path();
    block_on(async {
        let source = replica(&source_path);
        let destination = replica(&destination_path);
        source.create_table(people_schema()).await.unwrap();
        source
            .execute(vec![Statement::Query(Query::Insert(QueryInsert {
                table: "people".into(),
                row: Row::new(vec![Value::Integer(1), Value::from("Ada"), Value::Null]),
                returning: None,
            }))])
            .await
            .unwrap();
        let (_, changes) = source.changes_since(None).await.unwrap();
        destination.apply_changes(changes).await.unwrap();
        let (source_cursor, _) = source.changes_since(None).await.unwrap();

        add_column(&source, "role", Value::from("member")).await;
        update(&source, "role", Value::from("admin")).await;
        let (_, changes) = source.changes_since(Some(&source_cursor)).await.unwrap();
        destination.apply_changes(changes).await.unwrap();
        assert_eq!(
            people_rows(&destination, &["role"]).await,
            vec![Row::new(vec![Value::from("admin")])]
        );
    });
    std::fs::remove_file(source_path).unwrap();
    std::fs::remove_file(destination_path).unwrap();
}

#[test]
fn concurrent_add_column_converges_with_defaults_and_later_updates() {
    let source_path = database_path();
    let destination_path = database_path();
    block_on(async {
        let source = replica(&source_path);
        let destination = replica(&destination_path);
        source.create_table(people_schema()).await.unwrap();
        source
            .execute(vec![Statement::Query(Query::Insert(QueryInsert {
                table: "people".into(),
                row: Row::new(vec![Value::Integer(1), Value::from("Ada"), Value::Null]),
                returning: None,
            }))])
            .await
            .unwrap();
        let (_, changes) = source.changes_since(None).await.unwrap();
        destination.apply_changes(changes).await.unwrap();
        let (source_cursor, _) = source.changes_since(None).await.unwrap();
        let (destination_cursor, _) = destination.changes_since(None).await.unwrap();

        add_column(&source, "role", Value::from("member")).await;
        add_column(&destination, "team", Value::from("core")).await;
        let (_, changes) = source.changes_since(Some(&source_cursor)).await.unwrap();
        destination.apply_changes(changes).await.unwrap();
        let (_, changes) = destination
            .changes_since(Some(&destination_cursor))
            .await
            .unwrap();
        source.apply_changes(changes).await.unwrap();
        let expected = vec![Row::new(vec![Value::from("member"), Value::from("core")])];
        assert_eq!(people_rows(&source, &["role", "team"]).await, expected);
        assert_eq!(
            people_rows(&destination, &["role", "team"]).await,
            people_rows(&source, &["role", "team"]).await
        );

        let (source_cursor, _) = source.changes_since(None).await.unwrap();
        let (destination_cursor, _) = destination.changes_since(None).await.unwrap();
        update(&source, "role", Value::from("admin")).await;
        update(&destination, "team", Value::from("storage")).await;
        let (_, changes) = source.changes_since(Some(&source_cursor)).await.unwrap();
        destination.apply_changes(changes).await.unwrap();
        let (_, changes) = destination
            .changes_since(Some(&destination_cursor))
            .await
            .unwrap();
        source.apply_changes(changes).await.unwrap();
        let expected = vec![Row::new(vec![Value::from("admin"), Value::from("storage")])];
        assert_eq!(people_rows(&source, &["role", "team"]).await, expected);
        assert_eq!(
            people_rows(&destination, &["role", "team"]).await,
            people_rows(&source, &["role", "team"]).await
        );
    });
    std::fs::remove_file(source_path).unwrap();
    std::fs::remove_file(destination_path).unwrap();
}
