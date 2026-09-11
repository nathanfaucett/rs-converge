#![cfg(feature = "in-memory")]

use db_engine::{Engine, InMemoryKernel};
use db_query::{
    DataDefinition, Query, QueryColumn, QueryFrom, QueryInsert, QuerySelect, Statement,
};
use db_schema::{ColumnSchema, TableSchema};
use db_value::{Row, Value, ValueType};
use futures::executor::block_on;

#[test]
fn creates_inserts_and_selects_rows() {
    block_on(async {
        let engine = Engine::new(InMemoryKernel::new());
        let schema = TableSchema {
            name: "users".into(),
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
                table: "users".into(),
                row: Row::new(vec![Value::Integer(1), Value::from("Ada")]),
                returning: None,
            }))])
            .await
            .unwrap();

        let results = engine
            .execute(vec![Statement::Query(Query::Select(QuerySelect {
                from: QueryFrom {
                    table: "users".into(),
                    joins: vec![],
                },
                projection: vec![QueryColumn::new("users".into(), "name".into())],
                ..Default::default()
            }))])
            .await
            .unwrap();

        assert_eq!(results[0].rows, vec![Row::new(vec![Value::from("Ada")])]);
    });
}
