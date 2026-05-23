#[cfg(not(feature = "std"))]
use alloc::{
  format,
  string::{String, ToString},
};

use hashbrown::HashMap;
use sqlparser::ast::{Expr as SqlExpr, Value as SqlValue};
use uuid::Uuid;

use super::TranslateError;

/// Extract an identifier from a sqlparser Expr.
pub fn extract_identifier(expr: &SqlExpr) -> Result<(Option<String>, String), TranslateError> {
  match expr {
    SqlExpr::Identifier(ident) => Ok((None, ident.value.clone())),
    SqlExpr::CompoundIdentifier(idents) => {
      if idents.is_empty() {
        Err(TranslateError::UnsupportedFeature(
          "empty compound identifier".into(),
        ))
      } else if idents.len() == 1 {
        Ok((None, idents[0].value.clone()))
      } else if idents.len() == 2 {
        Ok((Some(idents[0].value.clone()), idents[1].value.clone()))
      } else {
        Err(TranslateError::UnsupportedFeature(
          "compound identifiers with >2 parts unsupported".into(),
        ))
      }
    }
    _ => Err(TranslateError::UnsupportedFeature(
      "expected column identifier".into(),
    )),
  }
}

/// Convert a sqlparser literal expression into an EngineValue.
pub fn sql_value_to_engine_value(expr: &SqlExpr) -> Result<db_engine::EngineValue, TranslateError> {
  match expr {
    SqlExpr::Cast {
      kind: sqlparser::ast::CastKind::DoubleColon,
      expr,
      data_type,
      ..
    } => parse_cast_expr(expr, data_type),
    SqlExpr::Value(v) => parse_value_literal(&v.value),
    _ => Err(TranslateError::UnsupportedFeature(
      "expected literal value on RHS".into(),
    )),
  }
}

fn parse_cast_expr(
  expr: &SqlExpr,
  data_type: &sqlparser::ast::DataType,
) -> Result<db_engine::EngineValue, TranslateError> {
  match (expr, data_type) {
    (SqlExpr::Value(value), sqlparser::ast::DataType::Uuid) => match &value.value {
      SqlValue::SingleQuotedString(input) => {
        let parsed = Uuid::parse_str(input).map_err(|e| {
          TranslateError::UnsupportedFeature(format!("invalid UUID literal: {}", e))
        })?;
        Ok(db_engine::EngineValue::Uuid(*parsed.as_bytes()))
      }
      _ => Err(TranslateError::UnsupportedFeature(
        "UUID cast requires a single-quoted string literal".into(),
      )),
    },
    _ => Err(TranslateError::UnsupportedFeature(
      "unsupported explicit cast expression".into(),
    )),
  }
}

fn parse_value_literal(value: &SqlValue) -> Result<db_engine::EngineValue, TranslateError> {
  match value {
    SqlValue::Number(s, _) => parse_numeric_literal(s),
    SqlValue::SingleQuotedString(s) => Ok(db_engine::EngineValue::Text(s.clone())),
    SqlValue::Null => Ok(db_engine::EngineValue::Null),
    other => Err(TranslateError::UnsupportedFeature(format!(
      "unsupported literal: {:?}",
      other
    ))),
  }
}

fn parse_numeric_literal(s: &str) -> Result<db_engine::EngineValue, TranslateError> {
  if s.contains('.') {
    s.parse::<f64>()
      .map(db_engine::EngineValue::Float)
      .map_err(|e| TranslateError::UnsupportedFeature(format!("invalid float literal: {}", e)))
  } else {
    s.parse::<i64>()
      .map(db_engine::EngineValue::Integer)
      .map_err(|e| TranslateError::UnsupportedFeature(format!("invalid integer literal: {}", e)))
  }
}

/// Resolve a qualified or aliased column expression to a `QualifiedColumn`.
pub fn resolve_qualified_column(
  expr: &SqlExpr,
  alias_map: &HashMap<String, String>,
  table_schemas: &HashMap<String, db_engine::TableSchema>,
) -> Result<db_engine::QualifiedColumn, TranslateError> {
  let (opt_table, col) = extract_identifier(expr)?;
  let table_key = opt_table
    .map(|t| alias_map.get(&t).cloned().unwrap_or(t))
    .ok_or_else(|| {
      TranslateError::UnsupportedFeature("qualified column reference required".into())
    })?;

  let schema = table_schemas
    .get(&table_key)
    .ok_or_else(|| TranslateError::UnknownTable(table_key.clone()))?;
  let idx = schema
    .columns
    .iter()
    .position(|c| c.name == col)
    .ok_or_else(|| TranslateError::UnknownColumn(col.clone()))?;

  Ok(db_engine::QualifiedColumn {
    table: table_key,
    column_index: idx,
  })
}

/// Resolve a column expression across tables, allowing unqualified references when unambiguous.
pub fn resolve_column(
  expr: &SqlExpr,
  alias_map: &HashMap<String, String>,
  referenced_tables: &[String],
  table_schemas: &HashMap<String, db_engine::TableSchema>,
) -> Result<db_engine::QualifiedColumn, TranslateError> {
  let (opt_table, col) = extract_identifier(expr)?;
  let table_key = if let Some(table_name) = opt_table {
    alias_map.get(&table_name).cloned().unwrap_or(table_name)
  } else {
    let mut found_table: Option<String> = None;
    for t in referenced_tables {
      if let Some(schema) = table_schemas.get(t)
        && schema.columns.iter().any(|c| c.name == col)
      {
        if found_table.is_some() {
          return Err(TranslateError::UnsupportedFeature(format!(
            "ambiguous column reference: {}",
            col
          )));
        }
        found_table = Some(t.clone());
      }
    }
    found_table.ok_or_else(|| TranslateError::UnknownColumn(col.clone()))?
  };

  let schema = table_schemas
    .get(&table_key)
    .ok_or_else(|| TranslateError::UnknownTable(table_key.clone()))?;
  let idx = schema
    .columns
    .iter()
    .position(|c| c.name == col)
    .ok_or_else(|| TranslateError::UnknownColumn(col.clone()))?;

  Ok(db_engine::QualifiedColumn {
    table: table_key,
    column_index: idx,
  })
}

fn lookup_column_index(
  schema: &db_engine::TableSchema,
  col: &str,
) -> Result<usize, TranslateError> {
  schema
    .columns
    .iter()
    .position(|c| c.name == col)
    .ok_or_else(|| TranslateError::UnknownColumn(col.to_string()))
}

/// Resolve a SQL column identifier across all tables in scope, rejecting ambiguity.
pub fn resolve_column_local(
  expr: &SqlExpr,
  alias_map: &HashMap<String, String>,
  table_schemas: &HashMap<String, db_engine::TableSchema>,
) -> Result<db_engine::QualifiedColumn, TranslateError> {
  match expr {
    SqlExpr::Identifier(ident) => {
      let col = ident.value.clone();
      let mut found_table: Option<String> = None;
      for (table_name, schema) in table_schemas.iter() {
        if schema.columns.iter().any(|c| c.name == col) {
          if found_table.is_some() {
            return Err(TranslateError::UnsupportedFeature(format!(
              "ambiguous column reference: {}",
              col
            )));
          }
          found_table = Some(table_name.clone());
        }
      }
      let table_key = found_table.ok_or_else(|| TranslateError::UnknownColumn(col.clone()))?;
      let schema = table_schemas
        .get(&table_key)
        .ok_or_else(|| TranslateError::UnknownTable(table_key.clone()))?;
      let idx = lookup_column_index(schema, &col)?;
      Ok(db_engine::QualifiedColumn {
        table: table_key,
        column_index: idx,
      })
    }
    SqlExpr::CompoundIdentifier(idents) => {
      if idents.len() != 2 {
        return Err(TranslateError::UnsupportedFeature(
          "compound identifiers with >2 parts unsupported".into(),
        ));
      }
      let table = idents[0].value.clone();
      let col = idents[1].value.clone();
      let table_key = alias_map.get(&table).cloned().unwrap_or(table.clone());
      let schema = table_schemas
        .get(&table_key)
        .ok_or_else(|| TranslateError::UnknownTable(table_key.clone()))?;
      let idx = lookup_column_index(schema, &col)?;
      Ok(db_engine::QualifiedColumn {
        table: table_key,
        column_index: idx,
      })
    }
    _ => Err(TranslateError::UnsupportedFeature(
      "expected column identifier".into(),
    )),
  }
}
