#[cfg(all(not(feature = "std"), feature = "wasm"))]
use alloc::string::ToString;
#[cfg(not(feature = "std"))]
use alloc::{borrow::ToOwned, boxed::Box, string::String, vec, vec::Vec};

use crate::{
  ColumnIndex, FromRow, Row, Value,
  from_row::{FromRowResult, RowDeserializeError},
};

pub trait ExtractTables {
  fn extract_tables(&self) -> Vec<String>;
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(
  feature = "wasm",
  derive(serde::Serialize, serde::Deserialize, tsify::Tsify)
)]
#[cfg_attr(feature = "wasm", tsify(into_wasm_abi, from_wasm_abi))]
pub struct Column {
  pub table: String,
  pub column_index: ColumnIndex,
}

impl ExtractTables for Column {
  fn extract_tables(&self) -> Vec<String> {
    vec![self.table.clone()]
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
  pub table: String,
  pub on: Expr,
}

impl ExtractTables for Join {
  fn extract_tables(&self) -> Vec<String> {
    let mut tables = vec![self.table.clone()];
    tables.extend(self.on.extract_tables());
    tables
  }
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

impl ExtractTables for OrderBy {
  fn extract_tables(&self) -> Vec<String> {
    vec![self.expr.table.clone()]
  }
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

impl ExtractTables for ExprValue {
  fn extract_tables(&self) -> Vec<String> {
    match self {
      ExprValue::Column(col) => vec![col.table.clone()],
      ExprValue::Value(_) => vec![],
    }
  }
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

impl ExtractTables for Expr {
  fn extract_tables(&self) -> Vec<String> {
    match self {
      Expr::Equals(left, right)
      | Expr::NotEquals(left, right)
      | Expr::LessThan(left, right)
      | Expr::LessThanOrEquals(left, right)
      | Expr::GreaterThan(left, right)
      | Expr::GreaterThanOrEquals(left, right) => {
        let mut tables = left.extract_tables();
        tables.extend(right.extract_tables());
        tables
      }
      Expr::IsNull(expr) | Expr::IsNotNull(expr) => expr.extract_tables(),
      Expr::InList { expr, .. } => expr.extract_tables(),
      Expr::InSubquery { expr, subquery, .. } => {
        let mut tables = expr.extract_tables();
        tables.extend(subquery.extract_tables());
        tables
      }
      Expr::Like { expr, pattern, .. } => {
        let mut tables = expr.extract_tables();
        tables.extend(pattern.extract_tables());
        tables
      }
      Expr::And(left, right) | Expr::Or(left, right) => {
        let mut tables = left.extract_tables();
        tables.extend(right.extract_tables());
        tables
      }
      Expr::Not(inner) => inner.extract_tables(),
    }
  }
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

impl ExtractTables for SelectOptions {
  fn extract_tables(&self) -> Vec<String> {
    let mut tables = Vec::new();
    for join in &self.joins {
      tables.extend(join.extract_tables());
    }
    for order in &self.order_by {
      tables.extend(order.extract_tables());
    }
    if let Some(having) = &self.having {
      tables.extend(having.extract_tables());
    }
    tables
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

impl ExtractTables for UpdateAssignment {
  fn extract_tables(&self) -> Vec<String> {
    let mut tables = self.column.extract_tables();
    tables.extend(self.value.extract_tables());
    tables
  }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(
  feature = "wasm",
  derive(serde::Serialize, serde::Deserialize, tsify::Tsify)
)]
#[cfg_attr(feature = "wasm", tsify(into_wasm_abi, from_wasm_abi))]
pub struct ResultColumn {
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
pub struct Result {
  pub rows: Vec<Row>,
  pub columns: Vec<ResultColumn>,
}

impl Result {
  pub fn new(rows: Vec<Row>) -> Self {
    Self {
      rows,
      columns: Vec::new(),
    }
  }

  pub fn new_with_columns(rows: Vec<Row>, columns: Vec<ResultColumn>) -> Self {
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
    table: String,
    projection: Vec<Column>,
    predicate: Option<Expr>,
    options: Box<SelectOptions>,
  },
  Insert {
    table: String,
    row: Row,
    returning: Option<Vec<Column>>,
  },
  Update {
    table: String,
    assignments: Vec<UpdateAssignment>,
    predicate: Option<Expr>,
    joins: Vec<Join>,
    from_tables: Vec<String>,
    returning: Option<Vec<Column>>,
  },
  Delete {
    table: String,
    predicate: Option<Expr>,
    returning: Option<Vec<Column>>,
  },
}

impl Query {
  pub fn select_simple(table: String, projection: Vec<usize>, predicate: Option<Expr>) -> Self {
    let proj = projection
      .into_iter()
      .map(|i| Column {
        table: table.clone(),
        column_index: i as ColumnIndex,
      })
      .collect();

    Query::Select {
      table,
      projection: proj,
      predicate,
      options: Box::new(SelectOptions::default()),
    }
  }
}

impl ExtractTables for Query {
  fn extract_tables(&self) -> Vec<String> {
    match self {
      Query::Select { table, options, .. } => {
        let mut tables = vec![table.clone()];
        tables.extend(options.extract_tables());
        tables
      }
      Query::Insert { table, .. } => vec![table.clone()],
      Query::Update {
        table,
        joins,
        from_tables,
        ..
      } => {
        let mut tables = vec![table.clone()];
        for join in joins {
          tables.extend(join.extract_tables());
        }
        tables.extend(from_tables.clone());
        tables
      }
      Query::Delete { table, .. } => vec![table.clone()],
    }
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn build_select_ex_shape() {
    let left_col = Column {
      table: "users".into(),
      column_index: 0,
    };
    let right_col = Column {
      table: "orders".into(),
      column_index: 0,
    };

    let join = Join {
      kind: JoinKind::Inner,
      table: "orders".into(),
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
      table: "users".into(),
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
        table: "users".into(),
        column_index: 0,
      },
      direction: SortDirection::Asc,
    });
    assert!(!options.is_simple());
  }
}
