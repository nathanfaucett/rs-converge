#[cfg(all(not(feature = "std"), feature = "wasm"))]
use alloc::string::ToString;
#[cfg(not(feature = "std"))]
use alloc::{borrow::ToOwned, boxed::Box, string::String, vec, vec::Vec};

use crate::{
  ColumnIndex, FromRow, IndexSchema, Row, TableSchema, Value,
  from_row::{FromRowResult, RowDeserializeError},
};

pub type TableIndex = u16;

#[derive(Debug, Clone, Eq, PartialEq)]
#[cfg_attr(
  feature = "wasm",
  derive(serde::Serialize, serde::Deserialize, tsify::Tsify)
)]
#[cfg_attr(feature = "wasm", tsify(into_wasm_abi, from_wasm_abi))]
pub enum DdlOp {
  CreateTable {
    schema: TableSchema,
    if_not_exists: bool,
  },
  DropTable {
    table_name: String,
    if_exists: bool,
  },
  CreateIndex {
    schema: IndexSchema,
    if_not_exists: bool,
  },
  DropIndex {
    index_name: String,
    if_exists: bool,
  },
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(
  feature = "wasm",
  derive(serde::Serialize, serde::Deserialize, tsify::Tsify)
)]
#[cfg_attr(feature = "wasm", tsify(into_wasm_abi, from_wasm_abi))]
pub struct Column {
  pub table_index: TableIndex,
  pub column_index: ColumnIndex,
}

impl Column {
  pub fn new(table_index: TableIndex, column_index: ColumnIndex) -> Self {
    Self {
      table_index,
      column_index,
    }
  }
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(
  feature = "wasm",
  derive(serde::Serialize, serde::Deserialize, tsify::Tsify)
)]
#[cfg_attr(feature = "wasm", tsify(into_wasm_abi, from_wasm_abi))]
pub enum JoinKind {
  Inner,
  Left,
  Right,
  Full,
}

#[derive(Debug, Clone, Eq, PartialEq)]
#[cfg_attr(
  feature = "wasm",
  derive(serde::Serialize, serde::Deserialize, tsify::Tsify)
)]
#[cfg_attr(feature = "wasm", tsify(into_wasm_abi, from_wasm_abi))]
pub struct Join {
  pub kind: JoinKind,
  pub table_index: TableIndex,
  pub on: Expr,
}

#[derive(Debug, Clone, Eq, PartialEq)]
#[cfg_attr(
  feature = "wasm",
  derive(serde::Serialize, serde::Deserialize, tsify::Tsify)
)]
#[cfg_attr(feature = "wasm", tsify(into_wasm_abi, from_wasm_abi))]
pub enum SortDirection {
  Asc,
  Desc,
}

#[derive(Debug, Clone, Eq, PartialEq)]
#[cfg_attr(
  feature = "wasm",
  derive(serde::Serialize, serde::Deserialize, tsify::Tsify)
)]
#[cfg_attr(feature = "wasm", tsify(into_wasm_abi, from_wasm_abi))]
pub struct OrderBy {
  pub expr: Column,
  pub direction: SortDirection,
}

#[derive(Debug, Clone, Eq, PartialEq)]
#[cfg_attr(
  feature = "wasm",
  derive(serde::Serialize, serde::Deserialize, tsify::Tsify)
)]
#[cfg_attr(feature = "wasm", tsify(into_wasm_abi, from_wasm_abi))]
pub enum ExprValue {
  Column(Column),
  Value(Value),
}

#[derive(Debug, Clone, Eq, PartialEq)]
#[cfg_attr(
  feature = "wasm",
  derive(serde::Serialize, serde::Deserialize, tsify::Tsify)
)]
#[cfg_attr(feature = "wasm", tsify(into_wasm_abi, from_wasm_abi))]
pub enum Expr {
  Equals(ExprValue, ExprValue),
  NotEquals(ExprValue, ExprValue),
  LessThan(ExprValue, ExprValue),
  LessThanOrEquals(ExprValue, ExprValue),
  GreaterThan(ExprValue, ExprValue),
  GreaterThanOrEquals(ExprValue, ExprValue),
  IsNull(ExprValue),
  IsNotNull(ExprValue),
  InList {
    expr: ExprValue,
    list: Vec<Value>,
    negated: bool,
  },
  InSubquery {
    expr: ExprValue,
    subquery: Box<Query>,
    negated: bool,
  },
  Like {
    expr: ExprValue,
    pattern: ExprValue,
    negated: bool,
  },
  And(Box<Expr>, Box<Expr>),
  Or(Box<Expr>, Box<Expr>),
  Not(Box<Expr>),
}

#[derive(Debug, Clone, Eq, PartialEq)]
#[cfg_attr(
  feature = "wasm",
  derive(serde::Serialize, serde::Deserialize, tsify::Tsify)
)]
pub enum CountTarget {
  AllRows,                    // COUNT(*)
  Single(String),             // COUNT(col)
  Distinct(String),           // COUNT(DISTINCT col)
  DistinctMulti(Vec<String>), // COUNT(DISTINCT col1, col2)
}

#[derive(Debug, Clone, Eq, PartialEq)]
#[cfg_attr(
  feature = "wasm",
  derive(serde::Serialize, serde::Deserialize, tsify::Tsify)
)]
pub enum Aggregate {
  Count(CountTarget),
  Sum(Column),
  Avg(Column),
  Min(Column),
  Max(Column),
}

#[derive(Debug, Clone, Eq, PartialEq, Default)]
#[cfg_attr(
  feature = "wasm",
  derive(serde::Serialize, serde::Deserialize, tsify::Tsify)
)]
#[cfg_attr(feature = "wasm", tsify(into_wasm_abi, from_wasm_abi))]
pub struct SelectOptions {
  pub joins: Vec<Join>,
  pub aggregates: Vec<Aggregate>,
  pub group_by: Vec<Column>,
  pub order_by: Vec<OrderBy>,
  pub limit: Option<usize>,
  pub offset: Option<usize>,
  pub distinct: bool,
  pub having: Option<Expr>,
}

impl SelectOptions {
  pub fn is_simple(&self) -> bool {
    self.joins.is_empty()
      && self.aggregates.is_empty()
      && self.group_by.is_empty()
      && self.order_by.is_empty()
      && self.limit.is_none()
      && self.offset.is_none()
      && !self.distinct
      && self.having.is_none()
  }
}

#[derive(Debug, Clone, Eq, PartialEq)]
#[cfg_attr(
  feature = "wasm",
  derive(serde::Serialize, serde::Deserialize, tsify::Tsify)
)]
#[cfg_attr(feature = "wasm", tsify(into_wasm_abi, from_wasm_abi))]
pub struct UpdateAssignment {
  pub column: Column,
  pub value: ExprValue,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(
  feature = "wasm",
  derive(serde::Serialize, serde::Deserialize, tsify::Tsify)
)]
#[cfg_attr(feature = "wasm", tsify(into_wasm_abi, from_wasm_abi))]
pub struct QueryResultColumn {
  pub name: String,
  pub source_table: Option<String>,
  pub source_column_index: Option<ColumnIndex>,
}

#[derive(Debug, Clone, Default)]
#[cfg_attr(
  feature = "wasm",
  derive(serde::Serialize, serde::Deserialize, tsify::Tsify)
)]
#[cfg_attr(feature = "wasm", tsify(into_wasm_abi, from_wasm_abi))]
pub struct QueryResult {
  pub rows: Vec<Row>,
  pub columns: Vec<QueryResultColumn>,
}

impl QueryResult {
  pub fn new(rows: Vec<Row>) -> Self {
    Self {
      rows,
      columns: Vec::new(),
    }
  }

  pub fn new_with_columns(rows: Vec<Row>, columns: Vec<QueryResultColumn>) -> Self {
    Self { rows, columns }
  }

  pub fn typed<T: FromRow>(&self) -> FromRowResult<Vec<T>> {
    if self.columns.is_empty() {
      return Err(RowDeserializeError::SchemaError(
        "result has no column metadata".to_owned(),
      ));
    }

    self
      .rows
      .iter()
      .map(|row| T::from_named_row(&self.columns, row))
      .collect()
  }
}

#[derive(Debug, Clone, Eq, PartialEq)]
#[cfg_attr(
  feature = "wasm",
  derive(serde::Serialize, serde::Deserialize, tsify::Tsify)
)]
#[cfg_attr(feature = "wasm", tsify(into_wasm_abi, from_wasm_abi))]
pub enum Query {
  Select {
    tables: Vec<String>,
    table_index: TableIndex,
    projection: Vec<Column>,
    predicate: Option<Expr>,
    options: Box<SelectOptions>,
  },
  Insert {
    tables: Vec<String>,
    table_index: TableIndex,
    row: Row,
    returning: Option<Vec<Column>>,
  },
  Update {
    tables: Vec<String>,
    table_index: TableIndex,
    assignments: Vec<UpdateAssignment>,
    predicate: Option<Expr>,
    joins: Vec<Join>,
    from_table_indexes: Vec<TableIndex>,
    returning: Option<Vec<Column>>,
  },
  Delete {
    tables: Vec<String>,
    table_index: TableIndex,
    predicate: Option<Expr>,
    returning: Option<Vec<Column>>,
  },
}

impl Query {
  pub fn select_simple(table: String, projection: Vec<usize>, predicate: Option<Expr>) -> Self {
    let table_index = 0;
    let proj = projection
      .into_iter()
      .map(|i| Column {
        table_index,
        column_index: i as ColumnIndex,
      })
      .collect();

    Query::Select {
      tables: vec![table],
      table_index,
      projection: proj,
      predicate,
      options: Box::new(SelectOptions::default()),
    }
  }

  pub fn table_name(&self, table_index: TableIndex) -> Option<&str> {
    self
      .tables()
      .get(usize::from(table_index))
      .map(|name| name.as_str())
  }

  pub fn tables(&self) -> &[String] {
    match self {
      Query::Select { tables, .. }
      | Query::Insert { tables, .. }
      | Query::Update { tables, .. }
      | Query::Delete { tables, .. } => tables,
    }
  }
}

#[derive(Debug, Clone, Eq, PartialEq)]
#[cfg_attr(
  feature = "wasm",
  derive(serde::Serialize, serde::Deserialize, tsify::Tsify)
)]
#[cfg_attr(feature = "wasm", tsify(into_wasm_abi, from_wasm_abi))]
pub enum Statement {
  Query(Query),
  Ddl(DdlOp),
}

impl Statement {
  pub fn into_query(self) -> Option<Query> {
    match self {
      Statement::Query(query) => Some(query),
      Statement::Ddl(_) => None,
    }
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn build_select_ex_shape() {
    let left_col = Column {
      table_index: 0,
      column_index: 0,
    };
    let right_col = Column {
      table_index: 1,
      column_index: 0,
    };

    let join = Join {
      kind: JoinKind::Inner,
      table_index: 1,
      on: Expr::Equals(
        ExprValue::Column(left_col.clone()),
        ExprValue::Column(right_col.clone()),
      ),
    };

    let options = SelectOptions {
      joins: vec![join],
      aggregates: vec![Aggregate::Count(CountTarget::AllRows)],
      group_by: vec![left_col.clone()],
      order_by: vec![OrderBy {
        expr: left_col.clone(),
        direction: SortDirection::Asc,
      }],
      limit: Some(10),
      offset: Some(0),
      distinct: false,
      having: None,
    };

    let q = Query::Select {
      tables: vec!["users".into(), "orders".into()],
      table_index: 0,
      projection: vec![left_col],
      predicate: None,
      options: Box::new(options),
    };

    match q {
      Query::Select { .. } => {}
      _ => panic!("expected Select variant"),
    }
  }

  #[test]
  fn select_options_is_simple_and_non_simple() {
    let mut options = SelectOptions::default();
    assert!(options.is_simple());

    options.order_by.push(OrderBy {
      expr: Column {
        table_index: 0,
        column_index: 0,
      },
      direction: SortDirection::Asc,
    });
    assert!(!options.is_simple());
  }
}
