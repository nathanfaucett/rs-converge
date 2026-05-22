// Join example: registers two tables and performs an INNER JOIN with a projection.
use futures::executor::block_on;

use db_engine::{
  ColumnSchema, EngineDatabase, EngineQuery, EngineType, EngineValue, NamedTreeEngineStore,
  TableSchema,
};
use db_engine::{JoinClause, JoinKind, JoinOn, QualifiedColumn, SelectOptions};
use db_in_memory::InMemoryNamedBTree;

fn main() {
  block_on(async {
    let store: InMemoryNamedBTree<_, _> = InMemoryNamedBTree::new();
    let mut db = EngineDatabase::new(NamedTreeEngineStore::new(store));

    let users = TableSchema {
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
    };

    let orders = TableSchema {
      name: "orders".into(),
      columns: vec![
        ColumnSchema {
          name: "id".into(),
          data_type: EngineType::Integer,
        },
        ColumnSchema {
          name: "user_id".into(),
          data_type: EngineType::Integer,
        },
        ColumnSchema {
          name: "amount".into(),
          data_type: EngineType::Integer,
        },
      ],
      primary_key: vec![0],
    };

    db.register_table(users, false)
      .await
      .expect("register users");
    db.register_table(orders, false)
      .await
      .expect("register orders");

    // Insert some users
    db.execute(EngineQuery::Insert {
      table: "users".into(),
      row: vec![EngineValue::Integer(1), EngineValue::Text("Alice".into())],
      returning: None,
    })
    .await
    .expect("insert user 1");

    db.execute(EngineQuery::Insert {
      table: "users".into(),
      row: vec![EngineValue::Integer(2), EngineValue::Text("Bob".into())],
      returning: None,
    })
    .await
    .expect("insert user 2");

    // Insert some orders
    db.execute(EngineQuery::Insert {
      table: "orders".into(),
      row: vec![
        EngineValue::Integer(1),
        EngineValue::Integer(1),
        EngineValue::Integer(100),
      ],
      returning: None,
    })
    .await
    .expect("insert order 1");

    db.execute(EngineQuery::Insert {
      table: "orders".into(),
      row: vec![
        EngineValue::Integer(2),
        EngineValue::Integer(2),
        EngineValue::Integer(200),
      ],
      returning: None,
    })
    .await
    .expect("insert order 2");

    let left_col = QualifiedColumn {
      table: "users".into(),
      column_index: 0,
    };
    let right_col = QualifiedColumn {
      table: "orders".into(),
      column_index: 1,
    };

    let join = JoinClause {
      kind: JoinKind::Inner,
      left_table: "users".into(),
      right_table: "orders".into(),
      on: JoinOn::ColumnEq {
        left: left_col.clone(),
        right: right_col.clone(),
      },
    };

    let projection = vec![
      QualifiedColumn {
        table: "users".into(),
        column_index: 1,
      }, // name
      QualifiedColumn {
        table: "orders".into(),
        column_index: 2,
      }, // amount
    ];

    let options = SelectOptions {
      joins: vec![join],
      aggregates: vec![],
      group_by: vec![],
      order_by: vec![],
      limit: None,
      offset: None,
      distinct: false,
      having: None,
    };

    let res = db
      .execute(EngineQuery::Select {
        table: "users".into(),
        projection,
        predicate: None,
        options: Box::new(options),
      })
      .await
      .expect("execute join");

    println!("Joined rows: {}", res.rows.len());
    for row in res.rows {
      println!("row: {:?}", row);
    }
  });
}
