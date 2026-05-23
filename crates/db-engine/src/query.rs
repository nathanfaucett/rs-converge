#[cfg(not(feature = "std"))]
use alloc::boxed::Box;
#[cfg(not(feature = "std"))]
use alloc::string::String;
#[cfg(not(feature = "std"))]
use alloc::vec::Vec;
#[cfg(feature = "std")]
use std::vec::Vec;

#[cfg(not(feature = "std"))]
use alloc::collections::BTreeMap;
#[cfg(feature = "std")]
use std::collections::BTreeMap;

use db_types::TableSchema;

use crate::{EngineRow, EngineValue, FromRow, RowDeserializeError};

#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(
  feature = "wasm",
  derive(serde::Serialize, serde::Deserialize, tsify::Tsify)
)]
#[cfg_attr(feature = "wasm", tsify(into_wasm_abi, from_wasm_abi))]
pub enum UpdateValueExpr {
  Value(EngineValue),
  Column(QualifiedColumn),
  Add(Box<UpdateValueExpr>, Box<UpdateValueExpr>),
  Subtract(Box<UpdateValueExpr>, Box<UpdateValueExpr>),
  Multiply(Box<UpdateValueExpr>, Box<UpdateValueExpr>),
  Divide(Box<UpdateValueExpr>, Box<UpdateValueExpr>),
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(
  feature = "wasm",
  derive(serde::Serialize, serde::Deserialize, tsify::Tsify)
)]
#[cfg_attr(feature = "wasm", tsify(into_wasm_abi, from_wasm_abi))]
pub struct UpdateAssignment {
  pub column_index: usize,
  pub value: UpdateValueExpr,
}

impl UpdateAssignment {
  pub fn value(column_index: usize, value: EngineValue) -> Self {
    Self {
      column_index,
      value: UpdateValueExpr::Value(value),
    }
  }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(
  feature = "wasm",
  derive(serde::Serialize, serde::Deserialize, tsify::Tsify)
)]
#[cfg_attr(feature = "wasm", tsify(into_wasm_abi, from_wasm_abi))]
pub struct QualifiedColumn {
  pub table: String,
  pub column_index: usize,
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

#[derive(Debug, Clone)]
#[cfg_attr(
  feature = "wasm",
  derive(serde::Serialize, serde::Deserialize, tsify::Tsify)
)]
#[cfg_attr(feature = "wasm", tsify(into_wasm_abi, from_wasm_abi))]
pub enum JoinOn {
  ColumnEq {
    left: QualifiedColumn,
    right: QualifiedColumn,
  },
  ColumnEqList {
    pairs: Vec<(QualifiedColumn, QualifiedColumn)>,
  },
}

#[derive(Debug, Clone)]
#[cfg_attr(
  feature = "wasm",
  derive(serde::Serialize, serde::Deserialize, tsify::Tsify)
)]
#[cfg_attr(feature = "wasm", tsify(into_wasm_abi, from_wasm_abi))]
pub struct JoinClause {
  pub kind: JoinKind,
  pub left_table: String,
  pub right_table: String,
  pub on: JoinOn,
}

#[derive(Debug, Clone)]
#[cfg_attr(
  feature = "wasm",
  derive(serde::Serialize, serde::Deserialize, tsify::Tsify)
)]
#[cfg_attr(feature = "wasm", tsify(into_wasm_abi, from_wasm_abi))]
pub enum Aggregate {
  Count(Option<QualifiedColumn>),
  Sum(QualifiedColumn),
  Min(QualifiedColumn),
  Max(QualifiedColumn),
  Avg(QualifiedColumn),
}

#[derive(Debug, Clone)]
#[cfg_attr(
  feature = "wasm",
  derive(serde::Serialize, serde::Deserialize, tsify::Tsify)
)]
#[cfg_attr(feature = "wasm", tsify(into_wasm_abi, from_wasm_abi))]
pub enum SortDirection {
  Asc,
  Desc,
}

#[derive(Debug, Clone)]
#[cfg_attr(
  feature = "wasm",
  derive(serde::Serialize, serde::Deserialize, tsify::Tsify)
)]
#[cfg_attr(feature = "wasm", tsify(into_wasm_abi, from_wasm_abi))]
pub struct OrderBy {
  pub expr: QualifiedColumn,
  pub direction: SortDirection,
}
#[derive(Debug, Clone)]
#[cfg_attr(
  feature = "wasm",
  derive(serde::Serialize, serde::Deserialize, tsify::Tsify)
)]
#[cfg_attr(feature = "wasm", tsify(into_wasm_abi, from_wasm_abi))]
pub enum QualifiedOperand {
  Column(QualifiedColumn),
  Value(crate::EngineValue),
  Lower(Box<QualifiedOperand>),
}

#[derive(Debug, Clone)]
#[cfg_attr(
  feature = "wasm",
  derive(serde::Serialize, serde::Deserialize, tsify::Tsify)
)]
#[cfg_attr(feature = "wasm", tsify(into_wasm_abi, from_wasm_abi))]
pub enum QualifiedPredicate {
  Equals(QualifiedOperand, QualifiedOperand),
  NotEquals(QualifiedOperand, QualifiedOperand),
  LessThan(QualifiedOperand, QualifiedOperand),
  LessThanOrEquals(QualifiedOperand, QualifiedOperand),
  GreaterThan(QualifiedOperand, QualifiedOperand),
  GreaterThanOrEquals(QualifiedOperand, QualifiedOperand),
  IsNull(QualifiedColumn),
  IsNotNull(QualifiedColumn),
  InList {
    expr: QualifiedColumn,
    list: Vec<crate::EngineValue>,
    negated: bool,
  },
  InSubquery {
    expr: QualifiedColumn,
    subquery: Box<crate::EngineQuery>,
    negated: bool,
  },
  Like {
    expr: QualifiedOperand,
    pattern: QualifiedOperand,
    negated: bool,
  },
  And(Box<QualifiedPredicate>, Box<QualifiedPredicate>),
  Or(Box<QualifiedPredicate>, Box<QualifiedPredicate>),
  Not(Box<QualifiedPredicate>),
}

#[derive(Debug, Clone)]
#[cfg_attr(
  feature = "wasm",
  derive(serde::Serialize, serde::Deserialize, tsify::Tsify)
)]
#[cfg_attr(feature = "wasm", tsify(into_wasm_abi, from_wasm_abi))]
pub enum RefOrAgg {
  Column(QualifiedColumn),
  AggregateIndex(usize),
}

#[derive(Debug, Clone)]
#[cfg_attr(
  feature = "wasm",
  derive(serde::Serialize, serde::Deserialize, tsify::Tsify)
)]
#[cfg_attr(feature = "wasm", tsify(into_wasm_abi, from_wasm_abi))]
pub enum HavingPredicate {
  Equals(RefOrAgg, crate::EngineValue),
  NotEquals(RefOrAgg, crate::EngineValue),
  LessThan(RefOrAgg, crate::EngineValue),
  LessThanOrEquals(RefOrAgg, crate::EngineValue),
  GreaterThan(RefOrAgg, crate::EngineValue),
  GreaterThanOrEquals(RefOrAgg, crate::EngineValue),
  IsNull(RefOrAgg),
  IsNotNull(RefOrAgg),
  And(Box<HavingPredicate>, Box<HavingPredicate>),
  Or(Box<HavingPredicate>, Box<HavingPredicate>),
  Not(Box<HavingPredicate>),
}

#[derive(Debug, Clone, Default)]
#[cfg_attr(
  feature = "wasm",
  derive(serde::Serialize, serde::Deserialize, tsify::Tsify)
)]
#[cfg_attr(feature = "wasm", tsify(into_wasm_abi, from_wasm_abi))]
pub struct SelectOptions {
  pub joins: Vec<JoinClause>,
  pub aggregates: Vec<Aggregate>,
  pub group_by: Vec<QualifiedColumn>,
  pub order_by: Vec<OrderBy>,
  pub limit: Option<usize>,
  pub offset: Option<usize>,
  pub distinct: bool,
  pub having: Option<HavingPredicate>,
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

#[derive(Debug, Clone)]
#[cfg_attr(
  feature = "wasm",
  derive(serde::Serialize, serde::Deserialize, tsify::Tsify)
)]
#[cfg_attr(feature = "wasm", tsify(into_wasm_abi, from_wasm_abi))]
pub enum EngineQuery {
  Select {
    table: String,
    projection: Vec<QualifiedColumn>,
    predicate: Option<QualifiedPredicate>,
    options: Box<SelectOptions>,
  },
  Insert {
    table: String,
    row: EngineRow,
    returning: Option<Vec<UpdateValueExpr>>,
  },
  Update {
    table: String,
    assignments: Vec<UpdateAssignment>,
    predicate: Option<QualifiedPredicate>,
    joins: Vec<JoinClause>,
    from_tables: Vec<String>,
    returning: Option<Vec<UpdateValueExpr>>,
  },
  Delete {
    table: String,
    predicate: Option<QualifiedPredicate>,
    returning: Option<Vec<UpdateValueExpr>>,
  },
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
  pub source_column_index: Option<usize>,
}

impl ResultColumn {
  pub fn new(
    name: impl Into<String>,
    source_table: Option<String>,
    source_column_index: Option<usize>,
  ) -> Self {
    Self {
      name: name.into(),
      source_table,
      source_column_index,
    }
  }
}

#[derive(Debug, Clone, Default)]
#[cfg_attr(
  feature = "wasm",
  derive(serde::Serialize, serde::Deserialize, tsify::Tsify)
)]
#[cfg_attr(feature = "wasm", tsify(into_wasm_abi, from_wasm_abi))]
pub struct EngineResult {
  pub rows: Vec<EngineRow>,
  pub columns: Vec<ResultColumn>,
}

impl EngineResult {
  pub fn new(rows: Vec<EngineRow>) -> Self {
    Self {
      rows,
      columns: Vec::new(),
    }
  }

  pub fn new_with_columns(rows: Vec<EngineRow>, columns: Vec<ResultColumn>) -> Self {
    Self { rows, columns }
  }

  pub fn named_rows(&self) -> Result<Vec<BTreeMap<String, EngineValue>>, RowDeserializeError> {
    if self.columns.is_empty() {
      return Err(RowDeserializeError::schema_error(
        "result has no column metadata",
      ));
    }

    self
      .rows
      .iter()
      .map(|row| {
        if row.len() != self.columns.len() {
          return Err(RowDeserializeError::schema_error(format!(
            "row has {} values but result has {} columns",
            row.len(),
            self.columns.len()
          )));
        }

        let mut map = BTreeMap::new();
        for (column, value) in self.columns.iter().zip(row.iter()) {
          map.insert(column.name.clone(), value.clone());
        }
        Ok(map)
      })
      .collect()
  }

  pub fn into_typed_named<T: FromRow>(self) -> Result<Vec<T>, RowDeserializeError> {
    if self.columns.is_empty() {
      return Err(RowDeserializeError::schema_error(
        "result has no column metadata",
      ));
    }

    self
      .rows
      .into_iter()
      .map(|row| T::from_named_row(&self.columns, &row))
      .collect()
  }

  pub fn typed_rows_named<T: FromRow>(&self) -> Result<Vec<T>, RowDeserializeError> {
    if self.columns.is_empty() {
      return Err(RowDeserializeError::schema_error(
        "result has no column metadata",
      ));
    }

    self
      .rows
      .iter()
      .map(|row| T::from_named_row(&self.columns, row))
      .collect()
  }

  /// Deserialize all rows into typed structs using a table schema.
  ///
  /// # Arguments
  ///
  /// * `schema` - The table schema that describes the row structure
  ///
  /// # Returns
  ///
  /// A vector of typed structs, one per row, or an error if deserialization fails
  ///
  /// # Example
  ///
  /// ```ignore
  /// use serde::Deserialize;
  /// use db_engine::FromRow;
  ///
  /// #[derive(Deserialize)]
  /// struct User {
  ///   id: i64,
  ///   name: String,
  /// }
  ///
  /// let schema = engine.describe_table("users")?;
  /// let result = engine.execute_query(query)?;
  /// let users: Vec<User> = result.into_typed::<User>(&schema)?;
  /// ```
  pub fn into_typed<T: FromRow>(self, schema: &TableSchema) -> Result<Vec<T>, RowDeserializeError> {
    self
      .rows
      .into_iter()
      .map(|row| T::from_row(schema, &row))
      .collect()
  }

  /// Deserialize row references into typed structs using a table schema.
  ///
  /// Similar to `into_typed` but borrows self instead of consuming it.
  pub fn typed_rows<T: FromRow>(
    &self,
    schema: &TableSchema,
  ) -> Result<Vec<T>, RowDeserializeError> {
    self
      .rows
      .iter()
      .map(|row| T::from_row(schema, row))
      .collect()
  }
}

impl EngineQuery {
  pub fn select_simple(
    table: String,
    projection: Vec<usize>,
    predicate: Option<QualifiedPredicate>,
  ) -> Self {
    let proj = projection
      .into_iter()
      .map(|i| QualifiedColumn {
        table: table.clone(),
        column_index: i,
      })
      .collect();

    EngineQuery::Select {
      table,
      projection: proj,
      predicate,
      options: Box::new(SelectOptions::default()),
    }
  }

  /// Get all tables referenced by this query.
  pub fn tables(&self) -> Vec<String> {
    match self {
      EngineQuery::Select { table, options, .. } => {
        let mut tables = vec![table.clone()];
        for join in &options.joins {
          if !tables.contains(&join.left_table) {
            tables.push(join.left_table.clone());
          }
          if !tables.contains(&join.right_table) {
            tables.push(join.right_table.clone());
          }
        }
        tables
      }
      EngineQuery::Insert { table, .. }
      | EngineQuery::Update { table, .. }
      | EngineQuery::Delete { table, .. } => vec![table.clone()],
    }
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn build_select_ex_shape() {
    let left_col = QualifiedColumn {
      table: "users".into(),
      column_index: 0,
    };
    let right_col = QualifiedColumn {
      table: "orders".into(),
      column_index: 0,
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

    let options = SelectOptions {
      joins: vec![join],
      aggregates: vec![Aggregate::Count(None)],
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

    let q = EngineQuery::Select {
      table: "users".into(),
      projection: vec![left_col],
      predicate: None,
      options: Box::new(options),
    };

    match q {
      EngineQuery::Select { .. } => {}
      _ => panic!("expected Select variant"),
    }
  }

  #[test]
  fn select_options_is_simple_and_non_simple() {
    let mut options = SelectOptions::default();
    assert!(options.is_simple());

    options.order_by.push(OrderBy {
      expr: QualifiedColumn {
        table: "users".into(),
        column_index: 0,
      },
      direction: SortDirection::Asc,
    });
    assert!(!options.is_simple());
  }
}
