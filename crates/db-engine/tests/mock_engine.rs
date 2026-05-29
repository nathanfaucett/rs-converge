#![cfg(feature = "in-memory")]

use futures::executor::block_on;

use db_engine::{
  Aggregate, ColumnSchema, EngineDatabase, EngineKey, EngineQuery, EngineResult, EngineType,
  EngineValue, HavingPredicate, OrderBy, QualifiedColumn, QualifiedOperand, QualifiedPredicate,
  RefOrAgg, SelectOptions, SortDirection, TableSchema,
};

type TestDb = EngineDatabase<InMemoryNamedBTree<EngineKey, Vec<u8>>>;

fn uuid_value(id: u128) -> EngineValue {
  EngineValue::Uuid(id.to_be_bytes())
}

fn make_db_with_items() -> TestDb {
  let store: InMemoryNamedBTree<EngineKey, Vec<u8>> = InMemoryNamedBTree::new();
  let mut db = EngineDatabase::new(store);
  block_on(async {
    db.register_table(
      TableSchema {
        name: "items".into(),
        columns: vec![
          ColumnSchema {
            name: "id".into(),
            data_type: EngineType::Uuid,
          },
          ColumnSchema {
            name: "name".into(),
            data_type: EngineType::Text,
          },
          ColumnSchema {
            name: "score".into(),
            data_type: EngineType::Integer,
          },
        ],
        primary_key: vec![0],
      },
      false,
    )
    .await
    .expect("register table");

    for (id, name, score) in [
      (1i64, "Alice", 80i64),
      (2, "Bob", 90),
      (3, "Carol", 70),
      (4, "Dave", 90),
      (5, "Eve", 80),
    ] {
      db.execute(EngineQuery::Insert {
        table: "items".into(),
        row: vec![
          uuid_value(id as u128),
          EngineValue::Text(name.into()),
          EngineValue::Integer(score),
        ],
        returning: None,
      })
      .await
      .expect("insert");
    }
  });
  db
}

fn qcol(table: &str, col: usize) -> QualifiedColumn {
  QualifiedColumn {
    table: table.into(),
    column_index: col,
  }
}

async fn select_items(
  db: &TestDb,
  projection: Vec<QualifiedColumn>,
  predicate: Option<QualifiedPredicate>,
  options: SelectOptions,
) -> EngineResult {
  db.execute(EngineQuery::Select {
    table: "items".into(),
    projection,
    predicate,
    options: Box::new(options),
  })
  .await
  .expect("select")
}

macro_rules! select_items_test {
  ($name:ident, $projection:expr, $options:expr, |$res:ident| $assertions:block) => {
    #[test]
    fn $name() {
      let db = make_db_with_items();
      block_on(async {
        let $res = select_items(&db, $projection, None, $options).await;
        $assertions
      });
    }
  };
}

#[test]
fn engine_works_with_mock_btree() {
  block_on(async {
    let store: InMemoryNamedBTree<_, _> = InMemoryNamedBTree::new();
    let mut db = EngineDatabase::new(store);

    let schema = TableSchema {
      name: "items".into(),
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
    };

    db.register_table(schema, false)
      .await
      .expect("register table");

    db.execute(EngineQuery::Insert {
      table: "items".into(),
      row: vec![uuid_value(1), EngineValue::Text("One".into())],
      returning: None,
    })
    .await
    .expect("insert");

    let res = db
      .execute(EngineQuery::select_simple(
        "items".into(),
        vec![1],
        Some(QualifiedPredicate::Equals(
          QualifiedOperand::Column(QualifiedColumn {
            table: "items".into(),
            column_index: 0,
          }),
          QualifiedOperand::Value(uuid_value(1)),
        )),
      ))
      .await
      .expect("select");

    assert_eq!(res.rows.len(), 1);
    assert_eq!(res.rows[0], vec![EngineValue::Text("One".into())]);
  });
}

select_items_test!(
  order_by_asc_with_limit_and_offset,
  vec![qcol("items", 0), qcol("items", 2)],
  SelectOptions {
    order_by: vec![OrderBy {
      expr: qcol("items", 2),
      direction: SortDirection::Asc,
    }],
    limit: Some(2),
    offset: Some(1),
    ..Default::default()
  },
  |res| {
    // Scores sorted asc: 70, 80, 80, 90, 90 — skip 1 (score=70 → Carol id=3), take 2
    // After offset=1 we get the two score=80 rows (id=1 Alice, id=5 Eve) in some order
    assert_eq!(res.rows.len(), 2);
    for row in &res.rows {
      assert_eq!(row[1], EngineValue::Integer(80));
    }
  }
);

select_items_test!(
  order_by_desc,
  vec![qcol("items", 2)],
  SelectOptions {
    order_by: vec![OrderBy {
      expr: qcol("items", 2),
      direction: SortDirection::Desc,
    }],
    limit: Some(1),
    ..Default::default()
  },
  |res| {
    assert_eq!(res.rows.len(), 1);
    assert_eq!(res.rows[0][0], EngineValue::Integer(90));
  }
);

select_items_test!(
  distinct_removes_duplicates,
  vec![qcol("items", 2)],
  SelectOptions {
    distinct: true,
    ..Default::default()
  },
  |res| {
    // scores: 70, 80, 90 — three distinct values
    assert_eq!(res.rows.len(), 3);
  }
);

select_items_test!(
  count_star_returns_row_count,
  vec![],
  SelectOptions {
    aggregates: vec![Aggregate::Count(None)],
    ..Default::default()
  },
  |res| {
    assert_eq!(res.rows.len(), 1);
    assert_eq!(res.rows[0][0], EngineValue::Integer(5));
  }
);

select_items_test!(
  sum_column_aggregation,
  vec![],
  SelectOptions {
    aggregates: vec![Aggregate::Sum(qcol("items", 2))],
    ..Default::default()
  },
  |res| {
    // 80 + 90 + 70 + 90 + 80 = 410
    assert_eq!(res.rows.len(), 1);
    assert_eq!(res.rows[0][0], EngineValue::Float(410.0));
  }
);

select_items_test!(
  avg_column_aggregation,
  vec![],
  SelectOptions {
    aggregates: vec![Aggregate::Avg(qcol("items", 2))],
    ..Default::default()
  },
  |res| {
    assert_eq!(res.rows.len(), 1);
    assert_eq!(res.rows[0][0], EngineValue::Float(82.0));
  }
);

select_items_test!(
  min_max_aggregation,
  vec![],
  SelectOptions {
    aggregates: vec![
      Aggregate::Min(qcol("items", 2)),
      Aggregate::Max(qcol("items", 2)),
    ],
    ..Default::default()
  },
  |res| {
    assert_eq!(res.rows.len(), 1);
    assert_eq!(res.rows[0][0], EngineValue::Integer(70));
    assert_eq!(res.rows[0][1], EngineValue::Integer(90));
  }
);

select_items_test!(
  group_by_score_count,
  vec![],
  SelectOptions {
    group_by: vec![qcol("items", 2)],
    aggregates: vec![Aggregate::Count(None)],
    ..Default::default()
  },
  |res| {
    // Groups: 70→1, 80→2, 90→2
    assert_eq!(res.rows.len(), 3);
    let mut rows = res.rows.clone();
    rows.sort_by_key(|r| match &r[0] {
      EngineValue::Integer(i) => *i,
      _ => 0,
    });
    assert_eq!(
      rows[0],
      vec![EngineValue::Integer(70), EngineValue::Integer(1)]
    );
    assert_eq!(
      rows[1],
      vec![EngineValue::Integer(80), EngineValue::Integer(2)]
    );
    assert_eq!(
      rows[2],
      vec![EngineValue::Integer(90), EngineValue::Integer(2)]
    );
  }
);

select_items_test!(
  group_by_having_filters_groups,
  vec![],
  SelectOptions {
    group_by: vec![qcol("items", 2)],
    aggregates: vec![Aggregate::Count(None)],
    having: Some(HavingPredicate::GreaterThan(
      RefOrAgg::AggregateIndex(0),
      EngineValue::Integer(1),
    )),
    ..Default::default()
  },
  |res| {
    // Only groups with count > 1: score=80 (2) and score=90 (2)
    assert_eq!(res.rows.len(), 2);
    for row in &res.rows {
      assert!(matches!(row[1], EngineValue::Integer(2)));
    }
  }
);

#[test]
fn in_subquery_filters_rows() {
  let db = make_db_with_items();
  block_on(async {
    // "SELECT id FROM items WHERE score IN (SELECT score FROM items WHERE id = 2)"
    // id=2 has score=90, so IN subquery should return rows with score=90: id=2, id=4
    let subquery = EngineQuery::select_simple(
      "items".into(),
      vec![2], // project score column
      Some(QualifiedPredicate::Equals(
        QualifiedOperand::Column(qcol("items", 0)),
        QualifiedOperand::Value(uuid_value(2)),
      )),
    );

    let res = select_items(
      &db,
      vec![qcol("items", 0)],
      Some(QualifiedPredicate::InSubquery {
        expr: qcol("items", 2),
        subquery: Box::new(subquery),
        negated: false,
      }),
      SelectOptions::default(),
    )
    .await;

    let mut ids: Vec<u128> = res
      .rows
      .iter()
      .map(|r| match r[0] {
        EngineValue::Uuid(bytes) => u128::from_be_bytes(bytes),
        _ => 0,
      })
      .collect();
    ids.sort();
    assert_eq!(ids, vec![2, 4]);
  });
}

#[test]
fn not_in_subquery_excludes_rows() {
  let db = make_db_with_items();
  block_on(async {
    // "SELECT id FROM items WHERE score NOT IN (SELECT score FROM items WHERE id = 2)"
    // score=90 excluded → remaining: id=1(80), id=3(70), id=5(80)
    let subquery = EngineQuery::select_simple(
      "items".into(),
      vec![2],
      Some(QualifiedPredicate::Equals(
        QualifiedOperand::Column(qcol("items", 0)),
        QualifiedOperand::Value(uuid_value(2)),
      )),
    );

    let res = select_items(
      &db,
      vec![qcol("items", 0)],
      Some(QualifiedPredicate::InSubquery {
        expr: qcol("items", 2),
        subquery: Box::new(subquery),
        negated: true,
      }),
      SelectOptions::default(),
    )
    .await;

    let mut ids: Vec<u128> = res
      .rows
      .iter()
      .map(|r| match r[0] {
        EngineValue::Uuid(bytes) => u128::from_be_bytes(bytes),
        _ => 0,
      })
      .collect();
    ids.sort();
    assert_eq!(ids, vec![1, 3, 5]);
  });
}
