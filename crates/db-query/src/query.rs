#[cfg(not(feature = "std"))]
use alloc::{boxed::Box, string::String, vec::Vec};
#[cfg(all(not(feature = "std"), feature = "wasm"))]
use alloc::{format, string::ToString};

use db_schema::{ColumnSchemaIndex, IndexSchema, TableSchema};
use db_value::{Row, Value};

pub type QueryTableIndex = u32;

#[derive(Debug, Clone, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
#[cfg_attr(
  feature = "wasm",
  derive(tsify::Tsify),
  tsify(into_wasm_abi, from_wasm_abi)
)]
pub struct QueryColumn {
  pub table_index: QueryTableIndex,
  pub column_index: ColumnSchemaIndex,
}

impl QueryColumn {
  pub fn new(table_index: QueryTableIndex, column_index: ColumnSchemaIndex) -> Self {
    Self {
      table_index,
      column_index,
    }
  }
}

#[derive(Debug, Clone, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
#[cfg_attr(
  feature = "wasm",
  derive(tsify::Tsify),
  tsify(into_wasm_abi, from_wasm_abi)
)]
pub enum QueryJoinKind {
  Inner,
  Left,
  Right,
  Full,
}

#[derive(Debug, Clone, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
#[cfg_attr(
  feature = "wasm",
  derive(tsify::Tsify),
  tsify(into_wasm_abi, from_wasm_abi)
)]
pub struct QueryJoin {
  pub kind: QueryJoinKind,
  pub table_index: QueryTableIndex,
  pub on: QueryExpr,
}

#[derive(Debug, Clone, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
#[cfg_attr(
  feature = "wasm",
  derive(tsify::Tsify),
  tsify(into_wasm_abi, from_wasm_abi)
)]
pub enum QuerySortDirection {
  Asc,
  Desc,
}

#[derive(Debug, Clone, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
#[cfg_attr(
  feature = "wasm",
  derive(tsify::Tsify),
  tsify(into_wasm_abi, from_wasm_abi)
)]
pub struct QueryOrderBy {
  pub expr: QueryColumn,
  pub direction: QuerySortDirection,
}

#[derive(Debug, Clone, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
#[cfg_attr(
  feature = "wasm",
  derive(tsify::Tsify),
  tsify(into_wasm_abi, from_wasm_abi)
)]
pub enum QueryExprValue {
  Column(QueryColumn),
  Value(Value),
}

#[derive(Debug, Clone, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
#[cfg_attr(
  feature = "wasm",
  derive(tsify::Tsify),
  tsify(into_wasm_abi, from_wasm_abi)
)]
pub enum QueryExpr {
  Equals(QueryExprValue, QueryExprValue),
  NotEquals(QueryExprValue, QueryExprValue),
  LessThan(QueryExprValue, QueryExprValue),
  LessThanOrEquals(QueryExprValue, QueryExprValue),
  GreaterThan(QueryExprValue, QueryExprValue),
  GreaterThanOrEquals(QueryExprValue, QueryExprValue),
  IsNull(QueryExprValue),
  IsNotNull(QueryExprValue),
  InList {
    expr: QueryExprValue,
    list: Vec<Value>,
    negated: bool,
  },
  InSubquery {
    expr: QueryExprValue,
    subquery: Box<Query>,
    negated: bool,
  },
  Like {
    expr: QueryExprValue,
    pattern: QueryExprValue,
    negated: bool,
  },
  And(Box<QueryExpr>, Box<QueryExpr>),
  Or(Box<QueryExpr>, Box<QueryExpr>),
  Not(Box<QueryExpr>),
}

#[derive(Debug, Clone, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
#[cfg_attr(
  feature = "wasm",
  derive(tsify::Tsify),
  tsify(into_wasm_abi, from_wasm_abi)
)]
pub enum QueryCountTarget {
  AllRows,                    // COUNT(*)
  Single(String),             // COUNT(col)
  Distinct(String),           // COUNT(DISTINCT col)
  DistinctMulti(Vec<String>), // COUNT(DISTINCT col1, col2)
}

#[derive(Debug, Clone, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
#[cfg_attr(
  feature = "wasm",
  derive(tsify::Tsify),
  tsify(into_wasm_abi, from_wasm_abi)
)]
pub enum QueryAggregate {
  Count(QueryCountTarget),
  Sum(QueryColumn),
  Avg(QueryColumn),
  Min(QueryColumn),
  Max(QueryColumn),
}

#[derive(Debug, Default, Clone, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
#[cfg_attr(
  feature = "wasm",
  derive(tsify::Tsify),
  tsify(into_wasm_abi, from_wasm_abi)
)]
pub struct QuerySelectOptions {
  pub joins: Vec<QueryJoin>,
  pub aggregates: Vec<QueryAggregate>,
  pub group_by: Vec<QueryColumn>,
  pub order_by: Vec<QueryOrderBy>,
  pub limit: Option<usize>,
  pub offset: Option<usize>,
  pub distinct: bool,
  pub having: Option<QueryExpr>,
}

impl QuerySelectOptions {
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

#[derive(Debug, Clone, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
#[cfg_attr(
  feature = "wasm",
  derive(tsify::Tsify),
  tsify(into_wasm_abi, from_wasm_abi)
)]
pub struct QueryUpdateAssignment {
  pub column: QueryColumn,
  pub value: QueryExprValue,
}

#[derive(Debug, Clone, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
#[cfg_attr(
  feature = "wasm",
  derive(tsify::Tsify),
  tsify(into_wasm_abi, from_wasm_abi)
)]
pub struct QueryResultColumn {
  pub name: String,
  pub source_table: Option<String>,
  pub source_column_index: Option<ColumnSchemaIndex>,
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
}

#[derive(Debug, Clone, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
#[cfg_attr(
  feature = "wasm",
  derive(tsify::Tsify),
  tsify(into_wasm_abi, from_wasm_abi)
)]
pub enum Query {
  Select {
    tables: Vec<String>,
    table_index: QueryTableIndex,
    projection: Vec<QueryColumn>,
    predicate: Option<QueryExpr>,
    options: Option<Box<QuerySelectOptions>>,
  },
  Insert {
    tables: Vec<String>,
    table_index: QueryTableIndex,
    row: Row,
    returning: Option<Vec<QueryColumn>>,
  },
  Update {
    tables: Vec<String>,
    table_index: QueryTableIndex,
    assignments: Vec<QueryUpdateAssignment>,
    predicate: Option<QueryExpr>,
    joins: Vec<QueryJoin>,
    from_table_indexes: Vec<QueryTableIndex>,
    returning: Option<Vec<QueryColumn>>,
  },
  Delete {
    tables: Vec<String>,
    table_index: QueryTableIndex,
    predicate: Option<QueryExpr>,
    returning: Option<Vec<QueryColumn>>,
  },
}

impl Query {
  pub fn table_name(&self, table_index: QueryTableIndex) -> Option<&str> {
    self
      .tables()
      .get(table_index as usize)
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

#[derive(Debug, Clone, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
#[cfg_attr(
  feature = "wasm",
  derive(tsify::Tsify),
  tsify(into_wasm_abi, from_wasm_abi)
)]
pub enum DataDefinition {
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

#[derive(Debug, Clone, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
#[cfg_attr(
  feature = "wasm",
  derive(tsify::Tsify),
  tsify(into_wasm_abi, from_wasm_abi)
)]
pub enum Statement {
  Query(Query),
  DataDefinition(DataDefinition),
}

impl Statement {
  pub fn into_query(self) -> Option<Query> {
    match self {
      Statement::Query(query) => Some(query),
      Statement::DataDefinition(_) => None,
    }
  }

  pub fn into_data_definition(self) -> Option<DataDefinition> {
    match self {
      Statement::Query(_) => None,
      Statement::DataDefinition(def) => Some(def),
    }
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  #[cfg(not(feature = "std"))]
  use alloc::vec;

  #[test]
  fn build_select_ex_shape() {
    let left_col = QueryColumn {
      table_index: 0,
      column_index: 0,
    };
    let right_col = QueryColumn {
      table_index: 1,
      column_index: 0,
    };

    let join = QueryJoin {
      kind: QueryJoinKind::Inner,
      table_index: 1,
      on: QueryExpr::Equals(
        QueryExprValue::Column(left_col.clone()),
        QueryExprValue::Column(right_col.clone()),
      ),
    };

    let options = QuerySelectOptions {
      joins: vec![join],
      aggregates: vec![QueryAggregate::Count(QueryCountTarget::AllRows)],
      group_by: vec![left_col.clone()],
      order_by: vec![QueryOrderBy {
        expr: left_col.clone(),
        direction: QuerySortDirection::Asc,
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
      options: Some(Box::new(options)),
    };

    match q {
      Query::Select { .. } => {}
      _ => panic!("expected Select variant"),
    }
  }

  #[test]
  fn select_options_is_simple_and_non_simple() {
    let mut options = QuerySelectOptions::default();
    assert!(options.is_simple());

    options.order_by.push(QueryOrderBy {
      expr: QueryColumn {
        table_index: 0,
        column_index: 0,
      },
      direction: QuerySortDirection::Asc,
    });
    assert!(!options.is_simple());
  }
}
