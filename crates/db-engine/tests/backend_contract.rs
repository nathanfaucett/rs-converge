#![cfg(feature = "in-memory")]

/// Integration test for transaction contract validation.
/// Verifies that backends honor their declared transactional guarantees.
use db_engine::{
  BackendCapability, ColumnSchema, EngineDatabase, EngineQuery, EngineStore, EngineType,
  EngineValue, NamedTreeEngineStore, TableSchema, UpdateAssignment,
};

use futures::executor::block_on;

type TestDb =
  EngineDatabase<NamedTreeEngineStore<InMemoryNamedBTree<db_engine::EngineKey, Vec<u8>>>>;

fn uuid_value(id: u128) -> EngineValue {
  EngineValue::Uuid(id.to_be_bytes())
}

fn make_test_db() -> TestDb {
  let store: InMemoryNamedBTree<db_engine::EngineKey, Vec<u8>> = InMemoryNamedBTree::new();
  EngineDatabase::new(store)
}

#[derive(Clone)]
struct InvalidContractStore {
  inner: NamedTreeEngineStore<InMemoryNamedBTree<db_engine::EngineKey, Vec<u8>>>,
}

impl EngineStore for InvalidContractStore {
  type Transaction =
    <NamedTreeEngineStore<InMemoryNamedBTree<db_engine::EngineKey, Vec<u8>>> as EngineStore>::Transaction;

  async fn engine_transaction(&self) -> Result<Self::Transaction, db_engine::EngineError> {
    self.inner.engine_transaction().await
  }

  fn transaction_contract(&self) -> db_engine::TransactionContract {
    db_engine::TransactionContract {
      atomicity: db_engine::BackendCapability::MultiTreeAtomicity,
      multi_tree_write_atomicity: false,
      schema_mutation_atomicity: true,
    }
  }
}

#[test]
fn transaction_contract_defaults_to_multi_tree_atomicity() {
  block_on(async {
    let db = make_test_db();
    let contract = db.store().transaction_contract();
    assert_eq!(contract.atomicity, BackendCapability::MultiTreeAtomicity);
    assert!(contract.multi_tree_write_atomicity);
    assert!(contract.validate().is_ok());
  });
}

#[test]
fn invalid_store_contract_fails_checked_new() {
  block_on(async {
    let store = InvalidContractStore {
      inner: InMemoryNamedBTree::new(),
    };

    let result = EngineDatabase::new_checked(store);
    assert!(result.is_err());
  });
}

#[test]
fn invalid_store_contract_fails_open() {
  block_on(async {
    let store = InvalidContractStore {
      inner: InMemoryNamedBTree::new(),
    };

    let result = EngineDatabase::open(store).await;
    assert!(result.is_err());
  });
}

#[test]
fn execute_with_scope_rejects_disallowed_tables() {
  block_on(async {
    let mut db = make_test_db();

    db.register_table(
      TableSchema {
        name: "users".into(),
        columns: vec![
          ColumnSchema {
            name: "id".into(),
            data_type: EngineType::Uuid,
          },
          ColumnSchema {
            name: "name".into(),
            data_type: EngineType::Text,
          },
        ],
        primary_key: vec![0],
      },
      false,
    )
    .await
    .expect("register users");

    let query = EngineQuery::select_simple("users".into(), vec![0, 1], None);

    let result = db.execute(query).await;
    assert!(result.is_err());
  });
}

#[test]
fn execute_with_scope_applies_main_table_filter() {
  block_on(async {
    let mut db = make_test_db();

    db.register_table(
      TableSchema {
        name: "users".into(),
        columns: vec![
          ColumnSchema {
            name: "id".into(),
            data_type: EngineType::Uuid,
          },
          ColumnSchema {
            name: "name".into(),
            data_type: EngineType::Text,
          },
        ],
        primary_key: vec![0],
      },
      false,
    )
    .await
    .expect("register users");

    db.execute(EngineQuery::Insert {
      table: "users".into(),
      row: vec![
        EngineValue::Uuid([0; 16]),
        EngineValue::Text("Alice".into()),
      ],
      returning: None,
    })
    .await
    .expect("insert alice");

    db.execute(EngineQuery::Insert {
      table: "users".into(),
      row: vec![EngineValue::Uuid([1; 16]), EngineValue::Text("Bob".into())],
      returning: None,
    })
    .await
    .expect("insert bob");

    let query = EngineQuery::select_simple("users".into(), vec![0, 1], None);

    let result = db
      .execute_with_scope(query, &scope)
      .await
      .expect("execute with scope");
    assert_eq!(result.rows.len(), 1);
    assert_eq!(
      result.rows[0],
      vec![
        EngineValue::Uuid([0; 16]),
        EngineValue::Text("Alice".into())
      ]
    );
  });
}

#[test]
fn multi_tree_atomicity_enables_cross_table_updates() {
  block_on(async {
    let mut db = make_test_db();

    // Create two tables
    db.register_table(
      TableSchema {
        name: "t1".into(),
        columns: vec![
          ColumnSchema {
            name: "id".into(),
            data_type: EngineType::Uuid,
          },
          ColumnSchema {
            name: "value".into(),
            data_type: EngineType::Integer,
          },
        ],
        primary_key: vec![0],
      },
      false,
    )
    .await
    .expect("register t1");

    db.register_table(
      TableSchema {
        name: "t2".into(),
        columns: vec![
          ColumnSchema {
            name: "id".into(),
            data_type: EngineType::Uuid,
          },
          ColumnSchema {
            name: "value".into(),
            data_type: EngineType::Integer,
          },
        ],
        primary_key: vec![0],
      },
      false,
    )
    .await
    .expect("register t2");

    // Insert a row in each table
    db.execute(EngineQuery::Insert {
      table: "t1".into(),
      row: vec![uuid_value(1), EngineValue::Integer(1)],
      returning: None,
    })
    .await
    .expect("insert t1");

    db.execute(EngineQuery::Insert {
      table: "t2".into(),
      row: vec![uuid_value(2), EngineValue::Integer(2)],
      returning: None,
    })
    .await
    .expect("insert t2");

    // Use a transaction to mutate both tables atomically
    let mut txn = db.transaction();
    txn
      .update_rows_with_sources(
        "t1",
        vec![UpdateAssignment {
          column_index: 1,
          value: db_engine::UpdateValueExpr::Value(EngineValue::Integer(100)),
        }],
        Some(db_engine::QualifiedPredicate::Equals(
          db_engine::QualifiedOperand::Column(db_engine::QualifiedColumn {
            table: "t1".into(),
            column_index: 0,
          }),
          db_engine::QualifiedOperand::Value(uuid_value(1)),
        )),
        vec![],
        vec![],
      )
      .await
      .expect("update t1");

    txn
      .update_rows_with_sources(
        "t2",
        vec![UpdateAssignment {
          column_index: 1,
          value: db_engine::UpdateValueExpr::Value(EngineValue::Integer(200)),
        }],
        Some(db_engine::QualifiedPredicate::Equals(
          db_engine::QualifiedOperand::Column(db_engine::QualifiedColumn {
            table: "t2".into(),
            column_index: 0,
          }),
          db_engine::QualifiedOperand::Value(uuid_value(2)),
        )),
        vec![],
        vec![],
      )
      .await
      .expect("update t2");

    txn.commit().await.expect("commit");

    // Verify both updates were applied
    let r1 = db
      .execute(EngineQuery::select_simple(
        "t1".into(),
        vec![1],
        Some(db_engine::QualifiedPredicate::Equals(
          db_engine::QualifiedOperand::Column(db_engine::QualifiedColumn {
            table: "t1".into(),
            column_index: 1,
          }),
          db_engine::QualifiedOperand::Value(EngineValue::Integer(1)),
        )),
      ))
      .await
      .expect("select t1");

    assert!(r1.rows.is_empty(), "original row should not exist");

    let r1_updated = db
      .execute(EngineQuery::select_simple(
        "t1".into(),
        vec![1],
        Some(db_engine::QualifiedPredicate::Equals(
          db_engine::QualifiedOperand::Column(db_engine::QualifiedColumn {
            table: "t1".into(),
            column_index: 1,
          }),
          db_engine::QualifiedOperand::Value(EngineValue::Integer(100)),
        )),
      ))
      .await
      .expect("select t1 updated");

    assert_eq!(r1_updated.rows.len(), 1, "updated row should exist");
  });
}
