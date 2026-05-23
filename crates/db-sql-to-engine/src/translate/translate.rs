#[cfg(not(feature = "std"))]
use alloc::{
  boxed::Box,
  format,
  string::{String, ToString},
  vec,
  vec::Vec,
};
use core::cell::Cell;

pub use db_engine::SchemaResolver;
use hashbrown::HashMap;
use sqlparser::ast::{
  AssignmentTarget, BinaryOperator, ColumnOption, CreateIndex, CreateTable, DataType,
  Delete as SqlDelete, Expr as SqlExpr, FromTable, FunctionArg, FunctionArgExpr, FunctionArguments,
  GroupByExpr, JoinConstraint, JoinOperator, LimitClause, ObjectName, ObjectType, Query,
  SelectItem, SetExpr, Statement, TableConstraint, TableFactor, Update as SqlUpdate,
  UpdateTableFromKind, Value as SqlValue,
};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;
use thiserror::Error;

use super::having as having_module;
use super::helpers;
use super::helpers::sql_value_to_engine_value;
use super::params::SqlParams;
use super::predicates as predicates_module;

// Reduce verbosity in signatures by aliasing the projection parse result.
type ProjectionParseResult = (
  Vec<db_engine::QualifiedColumn>,
  Vec<db_engine::Aggregate>,
  HashMap<String, db_engine::QualifiedColumn>,
);

/// Errors returned by the translator.
#[derive(Error, Debug)]
pub enum TranslateError {
  #[error("sql parse error: {0}")]
  SqlParse(String),
  #[error("unsupported statement")]
  UnsupportedStatement,
  #[error("unknown table: {0}")]
  UnknownTable(String),
  #[error("unknown column: {0}")]
  UnknownColumn(String),
  #[error("unsupported feature: {0}")]
  UnsupportedFeature(String),
  #[error("missing positional parameter: ${0}")]
  MissingPositionalParameter(usize),
  #[error("missing named parameter: :{0}")]
  MissingNamedParameter(String),
  #[error("cannot mix positional and named parameters in one SQL statement")]
  MixedParameterStyles,
  #[error("invalid parameter placeholder: {0}")]
  InvalidParameterPlaceholder(String),
}

/// Pluggable mapper for converting `sqlparser` literal expressions into `EngineValue`.
pub trait ValueMapper {
  fn map_sql_value(&self, expr: &SqlExpr) -> Result<db_engine::EngineValue, TranslateError>;
}

/// Default implementation of `ValueMapper` that uses the existing helper.
#[derive(Clone, Debug)]
pub struct DefaultValueMapper;

impl ValueMapper for DefaultValueMapper {
  fn map_sql_value(&self, expr: &SqlExpr) -> Result<db_engine::EngineValue, TranslateError> {
    sql_value_to_engine_value(expr)
  }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum PlaceholderStyle {
  Positional,
  Named,
}

enum PlaceholderToken {
  Positional(usize),
  Named(String),
}

struct ParamsValueMapper<'a> {
  params: &'a SqlParams,
  style: Cell<Option<PlaceholderStyle>>,
}

impl<'a> ParamsValueMapper<'a> {
  fn new(params: &'a SqlParams) -> Self {
    Self {
      params,
      style: Cell::new(None),
    }
  }

  fn extract_placeholder(expr: &SqlExpr) -> Option<&str> {
    match expr {
      SqlExpr::Value(value_with_span) => match &value_with_span.value {
        SqlValue::Placeholder(raw) => Some(raw.as_str()),
        _ => None,
      },
      SqlExpr::Cast { expr, .. } => Self::extract_placeholder(expr),
      _ => None,
    }
  }

  fn parse_placeholder(raw: &str) -> Result<PlaceholderToken, TranslateError> {
    if let Some(index) = raw.strip_prefix('$') {
      let parsed = index
        .parse::<usize>()
        .map_err(|_| TranslateError::InvalidParameterPlaceholder(raw.to_string()))?;
      if parsed == 0 {
        return Err(TranslateError::InvalidParameterPlaceholder(raw.to_string()));
      }
      return Ok(PlaceholderToken::Positional(parsed));
    }

    if let Some(name) = raw.strip_prefix(':') {
      if name.is_empty() {
        return Err(TranslateError::InvalidParameterPlaceholder(raw.to_string()));
      }
      return Ok(PlaceholderToken::Named(name.to_string()));
    }

    if raw == "?" {
      return Err(TranslateError::InvalidParameterPlaceholder(raw.to_string()));
    }

    if let Ok(parsed) = raw.parse::<usize>() {
      if parsed == 0 {
        return Err(TranslateError::InvalidParameterPlaceholder(raw.to_string()));
      }
      return Ok(PlaceholderToken::Positional(parsed));
    }

    if raw.is_empty() {
      return Err(TranslateError::InvalidParameterPlaceholder(raw.to_string()));
    }

    Ok(PlaceholderToken::Named(raw.to_string()))
  }

  fn enforce_style(&self, style: PlaceholderStyle) -> Result<(), TranslateError> {
    match self.style.get() {
      Some(previous) if previous != style => Err(TranslateError::MixedParameterStyles),
      Some(_) => Ok(()),
      None => {
        self.style.set(Some(style));
        Ok(())
      }
    }
  }
}

impl ValueMapper for ParamsValueMapper<'_> {
  fn map_sql_value(&self, expr: &SqlExpr) -> Result<db_engine::EngineValue, TranslateError> {
    let Some(raw) = Self::extract_placeholder(expr) else {
      return sql_value_to_engine_value(expr);
    };

    match Self::parse_placeholder(raw)? {
      PlaceholderToken::Positional(index) => {
        self.enforce_style(PlaceholderStyle::Positional)?;
        self
          .params
          .get_positional(index)
          .cloned()
          .ok_or(TranslateError::MissingPositionalParameter(index))
      }
      PlaceholderToken::Named(name) => {
        self.enforce_style(PlaceholderStyle::Named)?;
        self
          .params
          .get_named(&name)
          .cloned()
          .ok_or(TranslateError::MissingNamedParameter(name))
      }
    }
  }
}

/// Parse a SQL string and translate the first statement into an `EngineQuery`.
pub fn parse_and_translate(
  sql: &str,
  resolver: &dyn SchemaResolver,
) -> Result<db_engine::EngineQuery, TranslateError> {
  match parse_and_translate_statement_to_ir(sql, resolver)? {
    crate::ir::CanonicalStatement::Query(query) => Ok(query),
    crate::ir::CanonicalStatement::Ddl(_) => Err(TranslateError::UnsupportedStatement),
  }
}

/// Variant of `parse_and_translate` that accepts a custom `ValueMapper`.
pub fn parse_and_translate_with_mapper(
  sql: &str,
  resolver: &dyn SchemaResolver,
  mapper: &dyn ValueMapper,
) -> Result<db_engine::EngineQuery, TranslateError> {
  match parse_and_translate_statement_to_ir_with_mapper(sql, resolver, mapper)? {
    crate::ir::CanonicalStatement::Query(query) => Ok(query),
    crate::ir::CanonicalStatement::Ddl(_) => Err(TranslateError::UnsupportedStatement),
  }
}

/// Variant of `parse_and_translate` that resolves placeholders using `SqlParams`.
pub fn parse_and_translate_with_params(
  sql: &str,
  resolver: &dyn SchemaResolver,
  params: &SqlParams,
) -> Result<db_engine::EngineQuery, TranslateError> {
  let mapper = ParamsValueMapper::new(params);
  parse_and_translate_with_mapper(sql, resolver, &mapper)
}

/// Parse a SQL string and translate the first statement into a canonical statement.
pub fn parse_and_translate_statement(
  sql: &str,
  resolver: &dyn SchemaResolver,
) -> Result<crate::ir::CanonicalStatement, TranslateError> {
  parse_and_translate_statement_to_ir(sql, resolver)
}

/// Variant of `parse_and_translate_statement` that resolves placeholders using `SqlParams`.
pub fn parse_and_translate_statement_with_params(
  sql: &str,
  resolver: &dyn SchemaResolver,
  params: &SqlParams,
) -> Result<crate::ir::CanonicalStatement, TranslateError> {
  let mapper = ParamsValueMapper::new(params);
  parse_and_translate_statement_to_ir_with_mapper(sql, resolver, &mapper)
}

/// Variant of `parse_and_translate_statement` that accepts a custom `ValueMapper`.
#[allow(dead_code)]
pub fn parse_and_translate_statement_with_mapper(
  sql: &str,
  resolver: &dyn SchemaResolver,
  mapper: &dyn ValueMapper,
) -> Result<crate::ir::CanonicalStatement, TranslateError> {
  parse_and_translate_statement_to_ir_with_mapper(sql, resolver, mapper)
}

/// Parse a SQL string and translate the first statement into the canonical SQL IR.
pub fn parse_and_translate_to_ir(
  sql: &str,
  resolver: &dyn SchemaResolver,
) -> Result<crate::ir::CanonicalQuery, TranslateError> {
  parse_and_translate_to_ir_with_mapper(sql, resolver, &DefaultValueMapper)
}

/// Variant of `parse_and_translate_to_ir` that resolves placeholders using `SqlParams`.
pub fn parse_and_translate_to_ir_with_params(
  sql: &str,
  resolver: &dyn SchemaResolver,
  params: &SqlParams,
) -> Result<crate::ir::CanonicalQuery, TranslateError> {
  let mapper = ParamsValueMapper::new(params);
  parse_and_translate_to_ir_with_mapper(sql, resolver, &mapper)
}

/// Variant of `parse_and_translate_to_ir` that accepts a custom `ValueMapper`.
pub fn parse_and_translate_to_ir_with_mapper(
  sql: &str,
  resolver: &dyn SchemaResolver,
  mapper: &dyn ValueMapper,
) -> Result<crate::ir::CanonicalQuery, TranslateError> {
  let dialect = GenericDialect {}; // generic ANSI SQL
  let stmts =
    Parser::parse_sql(&dialect, sql).map_err(|e| TranslateError::SqlParse(e.to_string()))?;
  if stmts.len() != 1 {
    return Err(TranslateError::UnsupportedFeature(
      "only a single statement is supported".into(),
    ));
  }
  translate_statement_to_ir_with_mapper(&stmts[0], resolver, mapper)
}

/// Parse a SQL string and translate the first statement into a canonical statement.
pub fn parse_and_translate_statement_to_ir(
  sql: &str,
  resolver: &dyn SchemaResolver,
) -> Result<crate::ir::CanonicalStatement, TranslateError> {
  parse_and_translate_statement_to_ir_with_mapper(sql, resolver, &DefaultValueMapper)
}

/// Variant of `parse_and_translate_statement_to_ir` that accepts a custom `ValueMapper`.
pub fn parse_and_translate_statement_to_ir_with_mapper(
  sql: &str,
  resolver: &dyn SchemaResolver,
  mapper: &dyn ValueMapper,
) -> Result<crate::ir::CanonicalStatement, TranslateError> {
  let dialect = GenericDialect {}; // generic ANSI SQL
  let stmts =
    Parser::parse_sql(&dialect, sql).map_err(|e| TranslateError::SqlParse(e.to_string()))?;
  if stmts.len() != 1 {
    return Err(TranslateError::UnsupportedFeature(
      "only a single statement is supported".into(),
    ));
  }
  translate_statement_to_canonical(&stmts[0], resolver, mapper)
}

/// Translate a `sqlparser` AST `Statement` into a canonical statement.
pub fn translate_statement_to_canonical(
  stmt: &Statement,
  resolver: &dyn SchemaResolver,
  mapper: &dyn ValueMapper,
) -> Result<crate::ir::CanonicalStatement, TranslateError> {
  match stmt {
    Statement::Query(_) | Statement::Insert(_) | Statement::Update(_) | Statement::Delete(_) => Ok(
      crate::ir::CanonicalStatement::Query(translate_statement(stmt, resolver, mapper)?),
    ),
    Statement::CreateTable(create_table) => Ok(crate::ir::CanonicalStatement::Ddl(
      crate::ir::DdlOp::CreateTable(
        translate_create_table(create_table)?,
        create_table.if_not_exists,
      ),
    )),
    Statement::CreateIndex(create_index) => Ok(crate::ir::CanonicalStatement::Ddl(
      crate::ir::DdlOp::CreateIndex(translate_create_index(create_index, resolver)?),
    )),
    Statement::Drop {
      object_type,
      names,
      table,
      if_exists,
      ..
    } => Ok(crate::ir::CanonicalStatement::Ddl(
      translate_drop_statement(object_type, names, table, *if_exists)?,
    )),
    _ => Err(TranslateError::UnsupportedStatement),
  }
}

/// Translate a `sqlparser` `CREATE TABLE` into an engine `TableSchema`.
fn translate_create_table(
  create_table: &CreateTable,
) -> Result<db_engine::TableSchema, TranslateError> {
  let table_name = object_name_to_string(&create_table.name);
  let mut columns: Vec<db_engine::ColumnSchema> = Vec::new();
  let mut pk_names: Vec<String> = Vec::new();

  for column in &create_table.columns {
    let data_type = sql_type_to_engine_type(&column.data_type)?;
    let column_name = column.name.value.clone();
    for option in &column.options {
      match &option.option {
        ColumnOption::PrimaryKey(_) => pk_names.push(column_name.clone()),
        ColumnOption::Unique(_) => {}
        _ => {}
      }
    }

    columns.push(db_engine::ColumnSchema {
      name: column_name,
      data_type,
    });
  }

  for constraint in &create_table.constraints {
    if let TableConstraint::PrimaryKey(pk) = constraint {
      for column in &pk.columns {
        let name = match &column.column.expr {
          SqlExpr::Identifier(ident) => ident.value.clone(),
          SqlExpr::CompoundIdentifier(idents) => idents
            .iter()
            .map(|ident| ident.value.clone())
            .collect::<Vec<_>>()
            .join("."),
          other => {
            return Err(TranslateError::UnsupportedFeature(format!(
              "unsupported primary key column expression: {other:?}"
            )));
          }
        };
        pk_names.push(name);
      }
    }
  }

  let primary_key: Vec<usize> = if pk_names.is_empty() {
    return Err(TranslateError::UnsupportedFeature(
      "CREATE TABLE must define exactly one PRIMARY KEY column".into(),
    ));
  } else {
    pk_names
      .iter()
      .filter_map(|pk| columns.iter().position(|c| &c.name == pk))
      .collect()
  };

  if primary_key.is_empty() {
    return Err(TranslateError::UnsupportedFeature(
      "CREATE TABLE primary key columns not found".into(),
    ));
  }

  if primary_key.len() != 1 {
    return Err(TranslateError::UnsupportedFeature(
      "CREATE TABLE must define exactly one PRIMARY KEY column".into(),
    ));
  }

  let pk_index = primary_key[0];
  if columns
    .get(pk_index)
    .map(|column| column.data_type != db_engine::EngineType::Uuid)
    .unwrap_or(true)
  {
    return Err(TranslateError::UnsupportedFeature(
      "PRIMARY KEY column must use UUID type".into(),
    ));
  }

  Ok(db_engine::TableSchema {
    name: table_name,
    columns,
    primary_key,
  })
}

fn sql_type_to_engine_type(data_type: &DataType) -> Result<db_engine::EngineType, TranslateError> {
  use sqlparser::ast::DataType as SqlDataType;
  let ty = match data_type {
    SqlDataType::Int(_)
    | SqlDataType::Int2(_)
    | SqlDataType::Int4(_)
    | SqlDataType::Int8(_)
    | SqlDataType::Integer(_)
    | SqlDataType::SmallInt(_)
    | SqlDataType::BigInt(_)
    | SqlDataType::MediumInt(_)
    | SqlDataType::TinyInt(_)
    | SqlDataType::Unsigned
    | SqlDataType::UnsignedInteger
    | SqlDataType::Signed
    | SqlDataType::SignedInteger
    | SqlDataType::IntUnsigned(_)
    | SqlDataType::Int4Unsigned(_)
    | SqlDataType::BigIntUnsigned(_)
    | SqlDataType::MediumIntUnsigned(_)
    | SqlDataType::TinyIntUnsigned(_)
    | SqlDataType::UInt8
    | SqlDataType::UInt16
    | SqlDataType::UInt32
    | SqlDataType::UInt64
    | SqlDataType::UInt128
    | SqlDataType::UInt256
    | SqlDataType::UBigInt
    | SqlDataType::UHugeInt
    | SqlDataType::SmallIntUnsigned(_)
    | SqlDataType::UTinyInt
    | SqlDataType::Int2Unsigned(_) => db_engine::EngineType::Integer,
    SqlDataType::Float(_)
    | SqlDataType::FloatUnsigned(_)
    | SqlDataType::Float4
    | SqlDataType::Float32
    | SqlDataType::Float64 => db_engine::EngineType::Float,
    SqlDataType::Char(_)
    | SqlDataType::Character(_)
    | SqlDataType::CharacterVarying(_)
    | SqlDataType::CharVarying(_)
    | SqlDataType::Varchar(_)
    | SqlDataType::Nvarchar(_)
    | SqlDataType::String(_)
    | SqlDataType::Text
    | SqlDataType::TinyText
    | SqlDataType::MediumText
    | SqlDataType::LongText
    | SqlDataType::Clob(_)
    | SqlDataType::CharacterLargeObject(_)
    | SqlDataType::CharLargeObject(_) => db_engine::EngineType::Text,
    SqlDataType::JSON => db_engine::EngineType::Json,
    SqlDataType::Uuid => db_engine::EngineType::Uuid,
    SqlDataType::Binary(_)
    | SqlDataType::Varbinary(_)
    | SqlDataType::Blob(_)
    | SqlDataType::TinyBlob
    | SqlDataType::MediumBlob
    | SqlDataType::LongBlob
    | SqlDataType::Bytes(_) => db_engine::EngineType::Blob,
    _ => {
      return Err(TranslateError::UnsupportedFeature(format!(
        "unsupported CREATE TABLE data type: {data_type:?}"
      )));
    }
  };
  Ok(ty)
}

/// Translate a `sqlparser` `CREATE INDEX` into an engine `IndexSchema`.
fn translate_create_index(
  create_index: &CreateIndex,
  resolver: &dyn SchemaResolver,
) -> Result<db_engine::IndexSchema, TranslateError> {
  let table_name = object_name_to_string(&create_index.table_name);
  let table_schema = resolver
    .describe_table(&table_name)
    .ok_or_else(|| TranslateError::UnknownTable(table_name.clone()))?;

  let index_name = if let Some(name) = &create_index.name {
    object_name_to_string(name)
  } else {
    return Err(TranslateError::UnsupportedFeature(
      "CREATE INDEX without explicit name is unsupported".into(),
    ));
  };

  let mut column_indices = Vec::new();
  for index_column in &create_index.columns {
    let column_name = match &index_column.column.expr {
      SqlExpr::Identifier(ident) => ident.value.clone(),
      SqlExpr::CompoundIdentifier(idents) => idents
        .iter()
        .map(|ident| ident.value.clone())
        .collect::<Vec<_>>()
        .join("."),
      other => {
        return Err(TranslateError::UnsupportedFeature(format!(
          "unsupported index column expression: {other:?}"
        )));
      }
    };

    let idx = table_schema
      .columns
      .iter()
      .position(|c| c.name == column_name)
      .ok_or_else(|| TranslateError::UnknownColumn(column_name.clone()))?;
    column_indices.push(idx);
  }

  if column_indices.is_empty() {
    return Err(TranslateError::UnsupportedFeature(
      "CREATE INDEX must specify at least one column".into(),
    ));
  }

  Ok(db_engine::IndexSchema {
    name: index_name,
    table_name,
    column_indices,
    unique: create_index.unique,
  })
}

fn translate_drop_statement(
  object_type: &ObjectType,
  names: &[ObjectName],
  _table: &Option<ObjectName>,
  if_exists: bool,
) -> Result<crate::ir::DdlOp, TranslateError> {
  if names.len() != 1 {
    return Err(TranslateError::UnsupportedFeature(
      "DROP only supports a single object".into(),
    ));
  }

  let object_name = object_name_to_string(&names[0]);

  match object_type {
    ObjectType::Table => Ok(crate::ir::DdlOp::DropTable(object_name, if_exists)),
    ObjectType::Index => Ok(crate::ir::DdlOp::DropIndex(object_name)),
    _ => Err(TranslateError::UnsupportedStatement),
  }
}

/// Translate a `sqlparser` AST `Statement` into the canonical SQL IR.
pub fn translate_statement_to_ir(
  stmt: &Statement,
  resolver: &dyn SchemaResolver,
) -> Result<crate::ir::CanonicalQuery, TranslateError> {
  translate_statement_to_ir_with_mapper(stmt, resolver, &DefaultValueMapper)
}

pub fn translate_statement_to_ir_with_mapper(
  stmt: &Statement,
  resolver: &dyn SchemaResolver,
  mapper: &dyn ValueMapper,
) -> Result<crate::ir::CanonicalQuery, TranslateError> {
  Ok(crate::ir::CanonicalQuery::from(translate_statement(
    stmt, resolver, mapper,
  )?))
}

/// Translate a `sqlparser` AST `Statement` into an `EngineQuery`.
pub fn translate_statement(
  stmt: &Statement,
  resolver: &dyn SchemaResolver,
  mapper: &dyn ValueMapper,
) -> Result<db_engine::EngineQuery, TranslateError> {
  match stmt {
    Statement::Query(boxed_q) => {
      let Query {
        body,
        order_by,
        limit_clause,
        ..
      } = &**boxed_q;
      match &**body {
        SetExpr::Select(select) => {
          if select.from.len() != 1 {
            return Err(TranslateError::UnsupportedFeature(
              "only single FROM with optional JOINs supported".into(),
            ));
          }

          let from = &select.from[0];

          let mut alias_map: HashMap<String, String> = HashMap::new();
          let mut referenced_tables: Vec<String> = Vec::new();
          let mut table_schemas: HashMap<String, db_engine::TableSchema> = HashMap::new();

          let base_table = parse_from_clause(
            from,
            resolver,
            &mut alias_map,
            &mut referenced_tables,
            &mut table_schemas,
          )?;

          let joins = parse_joins(
            &from.joins,
            &mut alias_map,
            &mut referenced_tables,
            &mut table_schemas,
            resolver,
            &base_table,
          )?;

          let (projection_qc, aggregates, proj_alias_map) = parse_projection(
            &select.projection,
            &alias_map,
            &referenced_tables,
            &table_schemas,
          )?;

          let group_by = parse_group_by(
            &select.group_by,
            &alias_map,
            &referenced_tables,
            &table_schemas,
          )?;

          let order_by_vec = parse_order_by(
            order_by,
            &proj_alias_map,
            &alias_map,
            &referenced_tables,
            &table_schemas,
          )?;

          let (limit_val, offset_val) = parse_limit_clause(limit_clause)?;

          // WHERE/predicate: build a qualified predicate for extended use
          let qualified_pred = if let Some(selection) = &select.selection {
            Some(expr_to_qualified_predicate(
              selection,
              &alias_map,
              &table_schemas,
              resolver,
              mapper,
            )?)
          } else {
            None
          };

          // HAVING support: translate having expression (after aggregates/group_by known)
          let having_pred = if let Some(h) = &select.having {
            let ctx = having_module::HavingContext {
              group_by: &group_by,
              aggregates: &aggregates,
              proj_alias_map: &proj_alias_map,
              alias_map: &alias_map,
              table_schemas: &table_schemas,
              resolver,
              mapper,
            };
            Some(having_module::expr_to_having_predicate(h, &ctx)?)
          } else {
            None
          };

          let mut options = db_engine::SelectOptions {
            joins,
            aggregates,
            group_by,
            order_by: order_by_vec,
            limit: limit_val,
            offset: offset_val,
            distinct: false,
            having: None,
          };
          options.having = having_pred;

          // Detect DISTINCT
          if let Some(_distinct) = &select.distinct {
            options.distinct = true;
          }

          // Decide between simple Select and SelectEx
          let want_simple = options.joins.is_empty()
            && options.aggregates.is_empty()
            && options.group_by.is_empty()
            && options.order_by.is_empty()
            && options.limit.is_none()
            && options.offset.is_none()
            && !options.distinct;

          if want_simple {
            // simple projection must only reference base_table columns
            let mut simple_proj: Vec<usize> = Vec::new();
            for qc in projection_qc {
              if qc.table != base_table {
                return Err(TranslateError::UnsupportedFeature(
                  "projection references non-base table but no JOIN present".into(),
                ));
              }
              simple_proj.push(qc.column_index);
            }
            return Ok(db_engine::EngineQuery::select_simple(
              base_table,
              simple_proj,
              qualified_pred,
            ));
          }

          // Use extended select
          let final_projection = if !options.aggregates.is_empty() || !options.group_by.is_empty() {
            Vec::new()
          } else {
            projection_qc
          };

          Ok(db_engine::EngineQuery::Select {
            table: base_table,
            projection: final_projection,
            predicate: qualified_pred,
            options: Box::new(options),
          })
        }
        _ => Err(TranslateError::UnsupportedStatement),
      }
    }
    Statement::Update(update) => translate_update(update, resolver, mapper),
    Statement::Delete(delete) => translate_delete(delete, resolver, mapper),
    Statement::Insert(insert) => translate_insert(insert, resolver, mapper),
    _ => Err(TranslateError::UnsupportedStatement),
  }
}

fn translate_update(
  update: &SqlUpdate,
  resolver: &dyn SchemaResolver,
  mapper: &dyn ValueMapper,
) -> Result<db_engine::EngineQuery, TranslateError> {
  if update.limit.is_some() {
    return Err(TranslateError::UnsupportedFeature(
      "UPDATE LIMIT is not supported".into(),
    ));
  }

  let mut alias_map: HashMap<String, String> = HashMap::new();
  let mut referenced_tables: Vec<String> = Vec::new();
  let mut table_schemas: HashMap<String, db_engine::TableSchema> = HashMap::new();

  let table = parse_from_clause(
    &update.table,
    resolver,
    &mut alias_map,
    &mut referenced_tables,
    &mut table_schemas,
  )?;

  let mut joins = parse_joins(
    &update.table.joins,
    &mut alias_map,
    &mut referenced_tables,
    &mut table_schemas,
    resolver,
    &table,
  )?;

  let mut from_tables: Vec<String> = Vec::new();
  if let Some(update_from) = &update.from {
    let from_items = match update_from {
      UpdateTableFromKind::BeforeSet(items) | UpdateTableFromKind::AfterSet(items) => items,
    };

    for from_item in from_items {
      let from_base = parse_from_clause(
        from_item,
        resolver,
        &mut alias_map,
        &mut referenced_tables,
        &mut table_schemas,
      )?;
      from_tables.push(from_base.clone());
      joins.extend(parse_joins(
        &from_item.joins,
        &mut alias_map,
        &mut referenced_tables,
        &mut table_schemas,
        resolver,
        &from_base,
      )?);
    }
  }

  let schema = resolver
    .describe_table(&table)
    .ok_or_else(|| TranslateError::UnknownTable(table.clone()))?;

  let mut resolved_assignments: Vec<db_engine::UpdateAssignment> = Vec::new();
  for assignment in &update.assignments {
    let column = assignment_target_column_name(&assignment.target, &alias_map, &table)?;
    let column_index = schema
      .columns
      .iter()
      .position(|column_schema| column_schema.name == column)
      .ok_or_else(|| TranslateError::UnknownColumn(column.clone()))?;
    let value =
      sql_expr_to_update_value_expr(&assignment.value, &alias_map, &table_schemas, mapper)?;
    let value = if let db_engine::UpdateValueExpr::Value(literal) = value {
      db_engine::UpdateValueExpr::Value(convert_value_to_column_type(
        literal,
        &schema.columns[column_index].data_type,
      )?)
    } else {
      value
    };
    resolved_assignments.push(db_engine::UpdateAssignment {
      column_index,
      value,
    });
  }

  let predicate = if let Some(selection) = &update.selection {
    Some(expr_to_qualified_predicate(
      selection,
      &alias_map,
      &table_schemas,
      resolver,
      mapper,
    )?)
  } else {
    None
  };

  let returning = if let Some(returning_items) = &update.returning {
    Some(translate_returning_projection(
      returning_items,
      &table,
      &alias_map,
      &table_schemas,
      mapper,
    )?)
  } else {
    None
  };

  Ok(db_engine::EngineQuery::Update {
    table,
    assignments: resolved_assignments,
    predicate,
    joins,
    from_tables,
    returning,
  })
}

fn sql_expr_to_update_value_expr(
  expr: &SqlExpr,
  alias_map: &HashMap<String, String>,
  table_schemas: &HashMap<String, db_engine::TableSchema>,
  mapper: &dyn ValueMapper,
) -> Result<db_engine::UpdateValueExpr, TranslateError> {
  match expr {
    SqlExpr::Value(_) | SqlExpr::Cast { .. } => Ok(db_engine::UpdateValueExpr::Value(
      mapper.map_sql_value(expr)?,
    )),
    SqlExpr::Identifier(_) | SqlExpr::CompoundIdentifier(_) => {
      let column = helpers::resolve_column_local(expr, alias_map, table_schemas)?;
      Ok(db_engine::UpdateValueExpr::Column(column))
    }
    SqlExpr::BinaryOp { left, op, right } => {
      let left_expr = sql_expr_to_update_value_expr(left, alias_map, table_schemas, mapper)?;
      let right_expr = sql_expr_to_update_value_expr(right, alias_map, table_schemas, mapper)?;
      match op {
        BinaryOperator::Plus => Ok(db_engine::UpdateValueExpr::Add(
          Box::new(left_expr),
          Box::new(right_expr),
        )),
        BinaryOperator::Minus => Ok(db_engine::UpdateValueExpr::Subtract(
          Box::new(left_expr),
          Box::new(right_expr),
        )),
        BinaryOperator::Multiply => Ok(db_engine::UpdateValueExpr::Multiply(
          Box::new(left_expr),
          Box::new(right_expr),
        )),
        BinaryOperator::Divide => Ok(db_engine::UpdateValueExpr::Divide(
          Box::new(left_expr),
          Box::new(right_expr),
        )),
        _ => Err(TranslateError::UnsupportedFeature(
          "unsupported operator in UPDATE assignment expression".into(),
        )),
      }
    }
    _ => Err(TranslateError::UnsupportedFeature(
      "unsupported UPDATE assignment expression".into(),
    )),
  }
}

fn translate_delete(
  delete: &SqlDelete,
  resolver: &dyn SchemaResolver,
  mapper: &dyn ValueMapper,
) -> Result<db_engine::EngineQuery, TranslateError> {
  if !delete.tables.is_empty() {
    return Err(TranslateError::UnsupportedFeature(
      "multi-table DELETE is not supported".into(),
    ));
  }
  if delete.using.is_some() {
    return Err(TranslateError::UnsupportedFeature(
      "DELETE USING is not supported".into(),
    ));
  }
  if !delete.order_by.is_empty() {
    return Err(TranslateError::UnsupportedFeature(
      "DELETE ORDER BY is not supported".into(),
    ));
  }
  if delete.limit.is_some() {
    return Err(TranslateError::UnsupportedFeature(
      "DELETE LIMIT is not supported".into(),
    ));
  }

  let from_tables = match &delete.from {
    FromTable::WithFromKeyword(tables) | FromTable::WithoutKeyword(tables) => tables,
  };
  if from_tables.len() != 1 {
    return Err(TranslateError::UnsupportedFeature(
      "DELETE requires exactly one target table".into(),
    ));
  }

  let target = &from_tables[0];
  if !target.joins.is_empty() {
    return Err(TranslateError::UnsupportedFeature(
      "DELETE with JOIN is not supported".into(),
    ));
  }

  let mut alias_map: HashMap<String, String> = HashMap::new();
  let mut referenced_tables: Vec<String> = Vec::new();
  let mut table_schemas: HashMap<String, db_engine::TableSchema> = HashMap::new();

  let table = parse_from_clause(
    target,
    resolver,
    &mut alias_map,
    &mut referenced_tables,
    &mut table_schemas,
  )?;

  let predicate = if let Some(selection) = &delete.selection {
    Some(expr_to_qualified_predicate(
      selection,
      &alias_map,
      &table_schemas,
      resolver,
      mapper,
    )?)
  } else {
    None
  };

  let returning = if let Some(returning_items) = &delete.returning {
    Some(translate_returning_projection(
      returning_items,
      &table,
      &alias_map,
      &table_schemas,
      mapper,
    )?)
  } else {
    None
  };

  Ok(db_engine::EngineQuery::Delete {
    table,
    predicate,
    returning,
  })
}

fn translate_returning_projection(
  returning: &[SelectItem],
  target_table: &str,
  alias_map: &HashMap<String, String>,
  table_schemas: &HashMap<String, db_engine::TableSchema>,
  mapper: &dyn ValueMapper,
) -> Result<Vec<db_engine::UpdateValueExpr>, TranslateError> {
  let schema = table_schemas
    .get(target_table)
    .ok_or_else(|| TranslateError::UnknownTable(target_table.to_string()))?;

  let mut projection: Vec<db_engine::UpdateValueExpr> = Vec::new();
  for item in returning {
    match item {
      SelectItem::Wildcard(_) => {
        projection.extend((0..schema.columns.len()).map(|index| {
          db_engine::UpdateValueExpr::Column(db_engine::QualifiedColumn {
            table: target_table.to_string(),
            column_index: index,
          })
        }));
      }
      SelectItem::QualifiedWildcard(kind, _) => {
        let table_name = match kind {
          sqlparser::ast::SelectItemQualifiedWildcardKind::ObjectName(name) => {
            let raw = object_name_to_string(name);
            alias_map.get(&raw).cloned().unwrap_or(raw)
          }
          _ => {
            return Err(TranslateError::UnsupportedFeature(
              "RETURNING qualified wildcard expression is not supported".into(),
            ));
          }
        };
        if table_name != target_table {
          return Err(TranslateError::UnsupportedFeature(
            "RETURNING can reference only target table columns".into(),
          ));
        }
        projection.extend((0..schema.columns.len()).map(|index| {
          db_engine::UpdateValueExpr::Column(db_engine::QualifiedColumn {
            table: target_table.to_string(),
            column_index: index,
          })
        }));
      }
      SelectItem::UnnamedExpr(expr) => {
        let returning_expr = sql_expr_to_update_value_expr(expr, alias_map, table_schemas, mapper)?;
        ensure_returning_expr_uses_target(&returning_expr, target_table)?;
        projection.push(returning_expr);
      }
      SelectItem::ExprWithAlias { expr, .. } => {
        let returning_expr = sql_expr_to_update_value_expr(expr, alias_map, table_schemas, mapper)?;
        ensure_returning_expr_uses_target(&returning_expr, target_table)?;
        projection.push(returning_expr);
      }
    }
  }

  Ok(projection)
}

fn ensure_returning_expr_uses_target(
  expr: &db_engine::UpdateValueExpr,
  target_table: &str,
) -> Result<(), TranslateError> {
  match expr {
    db_engine::UpdateValueExpr::Value(_) => Ok(()),
    db_engine::UpdateValueExpr::Column(column) => {
      if column.table != target_table {
        return Err(TranslateError::UnsupportedFeature(
          "RETURNING can reference only target table columns".into(),
        ));
      }
      Ok(())
    }
    db_engine::UpdateValueExpr::Add(left, right)
    | db_engine::UpdateValueExpr::Subtract(left, right)
    | db_engine::UpdateValueExpr::Multiply(left, right)
    | db_engine::UpdateValueExpr::Divide(left, right) => {
      ensure_returning_expr_uses_target(left, target_table)?;
      ensure_returning_expr_uses_target(right, target_table)
    }
  }
}

fn assignment_target_column_name(
  target: &AssignmentTarget,
  alias_map: &HashMap<String, String>,
  target_table: &str,
) -> Result<String, TranslateError> {
  match target {
    AssignmentTarget::ColumnName(name) => object_name_to_column(name, alias_map, target_table),
    AssignmentTarget::Tuple(_) => Err(TranslateError::UnsupportedFeature(
      "tuple assignment in UPDATE is not supported".into(),
    )),
  }
}

fn object_name_to_column(
  name: &ObjectName,
  alias_map: &HashMap<String, String>,
  target_table: &str,
) -> Result<String, TranslateError> {
  let parts = name
    .0
    .iter()
    .map(|part| {
      part
        .as_ident()
        .map(|ident| ident.value.clone())
        .ok_or_else(|| TranslateError::UnsupportedFeature("unsupported object name part".into()))
    })
    .collect::<Result<Vec<_>, _>>()?;

  match parts.as_slice() {
    [column] => Ok(column.clone()),
    [table, column] => {
      let resolved_table = alias_map
        .get(table)
        .cloned()
        .unwrap_or_else(|| table.clone());
      if resolved_table != target_table {
        return Err(TranslateError::UnsupportedFeature(
          "assignment target must reference the update table".into(),
        ));
      }
      Ok(column.clone())
    }
    _ => Err(TranslateError::UnsupportedFeature(
      "assignment target must be column or table.column".into(),
    )),
  }
}

/// Converts an EngineValue to the appropriate type for a given column.
/// E.g., converts Text to Json if the column type is Json.
fn convert_value_to_column_type(
  value: db_engine::EngineValue,
  column_type: &db_engine::EngineType,
) -> Result<db_engine::EngineValue, TranslateError> {
  match (value, column_type) {
    // Text -> Uuid conversion for UUID columns
    (db_engine::EngineValue::Text(s), db_engine::EngineType::Uuid) => {
      let parsed = uuid::Uuid::parse_str(&s)
        .map_err(|e| TranslateError::UnsupportedFeature(format!("invalid UUID literal: {}", e)))?;
      Ok(db_engine::EngineValue::Uuid(*parsed.as_bytes()))
    }
    // Text -> Json conversion for JSON columns
    (db_engine::EngineValue::Text(s), db_engine::EngineType::Json) => {
      // Validate that the string is valid JSON
      if serde_json::from_str::<serde_json::Value>(&s).is_err() {
        return Err(TranslateError::UnsupportedFeature(format!(
          "invalid JSON literal: {}",
          s
        )));
      }
      Ok(db_engine::EngineValue::Json(s))
    }
    // Keep value as-is if types match or are null
    (db_engine::EngineValue::Null, _) => Ok(db_engine::EngineValue::Null),
    (v, _) => Ok(v),
  }
}

fn translate_insert(
  insert: &sqlparser::ast::Insert,
  resolver: &dyn SchemaResolver,
  mapper: &dyn ValueMapper,
) -> Result<db_engine::EngineQuery, TranslateError> {
  if !insert.assignments.is_empty() {
    return Err(TranslateError::UnsupportedFeature(
      "INSERT ... SET is not supported".into(),
    ));
  }
  if let Some(on) = &insert.on {
    return Err(TranslateError::UnsupportedFeature(format!(
      "INSERT ON is not supported: {on:?}"
    )));
  }
  let table = match &insert.table {
    sqlparser::ast::TableObject::TableName(name) => object_name_to_string(name),
    _ => {
      return Err(TranslateError::UnsupportedFeature(
        "only table name inserts supported".into(),
      ));
    }
  };

  let schema = resolver
    .describe_table(&table)
    .ok_or_else(|| TranslateError::UnknownTable(table.clone()))?;

  let columns = if insert.columns.is_empty() {
    schema
      .columns
      .iter()
      .map(|c| c.name.clone())
      .collect::<Vec<_>>()
  } else {
    insert
      .columns
      .iter()
      .map(|ident| ident.value.clone())
      .collect()
  };

  let source = insert.source.as_ref().ok_or_else(|| {
    TranslateError::UnsupportedFeature("INSERT without source unsupported".into())
  })?;

  let values = match &*source.body {
    SetExpr::Values(values) => values,
    _ => {
      return Err(TranslateError::UnsupportedFeature(
        "only INSERT ... VALUES (...) is supported".into(),
      ));
    }
  };

  if values.rows.len() != 1 {
    return Err(TranslateError::UnsupportedFeature(
      "only single-row INSERT VALUES supported".into(),
    ));
  }

  let row_exprs = &values.rows[0];
  if row_exprs.len() != columns.len() {
    return Err(TranslateError::UnsupportedFeature(
      "INSERT column count does not match VALUES count".into(),
    ));
  }

  let mut row = vec![db_engine::EngineValue::Null; schema.columns.len()];
  for (i, expr) in row_exprs.iter().enumerate() {
    let col_name = &columns[i];
    let idx = schema
      .columns
      .iter()
      .position(|c| c.name == *col_name)
      .ok_or_else(|| TranslateError::UnknownColumn(col_name.clone()))?;
    let mut value = mapper.map_sql_value(expr)?;

    // Convert to appropriate type based on schema
    let column_type = &schema.columns[idx].data_type;
    value = convert_value_to_column_type(value, column_type)?;

    row[idx] = value;
  }

  let mut alias_map: HashMap<String, String> = HashMap::new();
  alias_map.insert(table.clone(), table.clone());

  let mut table_schemas: HashMap<String, db_engine::TableSchema> = HashMap::new();
  table_schemas.insert(table.clone(), schema.clone());

  let returning = if let Some(returning_items) = &insert.returning {
    Some(translate_returning_projection(
      returning_items,
      &table,
      &alias_map,
      &table_schemas,
      mapper,
    )?)
  } else {
    None
  };

  Ok(db_engine::EngineQuery::Insert {
    table,
    row,
    returning,
  })
}

// `extract_identifier` moved to `translate/helpers.rs`.

// `sql_value_to_engine_value` moved to `translate/helpers.rs`.
fn expr_to_qualified_predicate(
  expr: &SqlExpr,
  alias_map: &HashMap<String, String>,
  table_schemas: &HashMap<String, db_engine::TableSchema>,
  resolver: &dyn SchemaResolver,
  mapper: &dyn ValueMapper,
) -> Result<db_engine::QualifiedPredicate, TranslateError> {
  predicates_module::expr_to_qualified_predicate(expr, alias_map, table_schemas, resolver, mapper)
}

fn object_name_to_string(name: &ObjectName) -> String {
  name
    .0
    .iter()
    .map(|part| {
      part
        .as_ident()
        .map(|ident| ident.value.clone())
        .expect("unsupported object name part")
    })
    .collect::<Vec<_>>()
    .join(".")
}

fn parse_from_clause(
  from: &sqlparser::ast::TableWithJoins,
  resolver: &dyn SchemaResolver,
  alias_map: &mut HashMap<String, String>,
  referenced_tables: &mut Vec<String>,
  table_schemas: &mut HashMap<String, db_engine::TableSchema>,
) -> Result<String, TranslateError> {
  let (base_table, base_alias) = match &from.relation {
    TableFactor::Table { name, alias, .. } => {
      let real = object_name_to_string(name);
      let alias_name = alias
        .as_ref()
        .map(|a| a.name.value.clone())
        .unwrap_or_else(|| real.clone());
      (real, alias_name)
    }
    _ => {
      return Err(TranslateError::UnsupportedFeature(
        "unsupported table factor".into(),
      ));
    }
  };

  alias_map.insert(base_alias.clone(), base_table.clone());
  referenced_tables.push(base_table.clone());
  let base_schema = resolver
    .describe_table(&base_table)
    .ok_or_else(|| TranslateError::UnknownTable(base_table.clone()))?;
  table_schemas.insert(base_table.clone(), base_schema.clone());

  Ok(base_table)
}

fn parse_joins(
  joins: &[sqlparser::ast::Join],
  alias_map: &mut HashMap<String, String>,
  referenced_tables: &mut Vec<String>,
  table_schemas: &mut HashMap<String, db_engine::TableSchema>,
  resolver: &dyn SchemaResolver,
  base_table: &str,
) -> Result<Vec<db_engine::JoinClause>, TranslateError> {
  let mut out: Vec<db_engine::JoinClause> = Vec::new();
  let mut current_left = base_table.to_string();

  for join in joins {
    let (right_table, right_alias) = match &join.relation {
      TableFactor::Table { name, alias, .. } => {
        let real = object_name_to_string(name);
        let alias_name = alias
          .as_ref()
          .map(|a| a.name.value.clone())
          .unwrap_or_else(|| real.clone());
        (real, alias_name)
      }
      _ => {
        return Err(TranslateError::UnsupportedFeature(
          "unsupported join relation".into(),
        ));
      }
    };

    alias_map.insert(right_alias.clone(), right_table.clone());
    referenced_tables.push(right_table.clone());
    let right_schema = resolver
      .describe_table(&right_table)
      .ok_or_else(|| TranslateError::UnknownTable(right_table.clone()))?;
    table_schemas.insert(right_table.clone(), right_schema.clone());

    let (kind, constraint) = match &join.join_operator {
      JoinOperator::Join(c) | JoinOperator::Inner(c) => (db_engine::JoinKind::Inner, c),
      JoinOperator::Left(c) | JoinOperator::LeftOuter(c) => (db_engine::JoinKind::Left, c),
      JoinOperator::Right(c) | JoinOperator::RightOuter(c) => (db_engine::JoinKind::Right, c),
      JoinOperator::FullOuter(c) => (db_engine::JoinKind::Full, c),
      _ => {
        return Err(TranslateError::UnsupportedFeature(
          "unsupported join operator".into(),
        ));
      }
    };

    let on = parse_join_on(constraint, alias_map, table_schemas)?;

    out.push(db_engine::JoinClause {
      kind,
      left_table: current_left.clone(),
      right_table: right_table.clone(),
      on,
    });
    current_left = right_table.clone();
  }

  Ok(out)
}

fn parse_join_on(
  constraint: &JoinConstraint,
  alias_map: &HashMap<String, String>,
  table_schemas: &HashMap<String, db_engine::TableSchema>,
) -> Result<db_engine::JoinOn, TranslateError> {
  match constraint {
    JoinConstraint::On(expr) => match expr {
      SqlExpr::BinaryOp { left, op, right } => match op {
        BinaryOperator::Eq => {
          let left_qc = helpers::resolve_qualified_column(left, alias_map, table_schemas)?;
          let right_qc = helpers::resolve_qualified_column(right, alias_map, table_schemas)?;

          Ok(db_engine::JoinOn::ColumnEq {
            left: left_qc,
            right: right_qc,
          })
        }
        _ => Err(TranslateError::UnsupportedFeature(
          "only equality ON joins supported".into(),
        )),
      },
      _ => Err(TranslateError::UnsupportedFeature(
        "unsupported join ON expression".into(),
      )),
    },
    _ => Err(TranslateError::UnsupportedFeature(
      "only ON join constraints supported".into(),
    )),
  }
}

fn parse_projection(
  projection: &[SelectItem],
  alias_map: &HashMap<String, String>,
  referenced_tables: &[String],
  table_schemas: &HashMap<String, db_engine::TableSchema>,
) -> Result<ProjectionParseResult, TranslateError> {
  let mut projection_qc: Vec<db_engine::QualifiedColumn> = Vec::new();
  let mut aggregates: Vec<db_engine::Aggregate> = Vec::new();
  let mut proj_alias_map: HashMap<String, db_engine::QualifiedColumn> = HashMap::new();

  for item in projection {
    parse_projection_item(
      item,
      alias_map,
      referenced_tables,
      table_schemas,
      &mut projection_qc,
      &mut aggregates,
      &mut proj_alias_map,
    )?;
  }

  Ok((projection_qc, aggregates, proj_alias_map))
}

fn parse_projection_item(
  item: &SelectItem,
  alias_map: &HashMap<String, String>,
  referenced_tables: &[String],
  table_schemas: &HashMap<String, db_engine::TableSchema>,
  projection_qc: &mut Vec<db_engine::QualifiedColumn>,
  aggregates: &mut Vec<db_engine::Aggregate>,
  proj_alias_map: &mut HashMap<String, db_engine::QualifiedColumn>,
) -> Result<(), TranslateError> {
  match item {
    SelectItem::Wildcard(_) => {
      for t in referenced_tables {
        if let Some(schema) = table_schemas.get(t) {
          for (i, _) in schema.columns.iter().enumerate() {
            projection_qc.push(db_engine::QualifiedColumn {
              table: t.clone(),
              column_index: i,
            });
          }
        }
      }
    }
    SelectItem::QualifiedWildcard(kind, _) => {
      let table_name = match kind {
        sqlparser::ast::SelectItemQualifiedWildcardKind::ObjectName(name) => {
          let raw = object_name_to_string(name);
          alias_map.get(&raw).cloned().unwrap_or(raw)
        }
        _ => {
          return Err(TranslateError::UnsupportedFeature(
            "unsupported qualified wildcard projection item".into(),
          ));
        }
      };
      let schema = table_schemas
        .get(&table_name)
        .ok_or_else(|| TranslateError::UnknownTable(table_name.clone()))?;
      for (i, _) in schema.columns.iter().enumerate() {
        projection_qc.push(db_engine::QualifiedColumn {
          table: table_name.clone(),
          column_index: i,
        });
      }
    }
    SelectItem::UnnamedExpr(expr) => {
      parse_projection_expression(
        expr,
        alias_map,
        referenced_tables,
        table_schemas,
        aggregates,
        proj_alias_map,
        projection_qc,
        None,
      )?;
    }
    SelectItem::ExprWithAlias { expr, alias } => match expr {
      SqlExpr::Function(_) => {
        parse_projection_expression(
          expr,
          alias_map,
          referenced_tables,
          table_schemas,
          aggregates,
          proj_alias_map,
          projection_qc,
          Some(&alias.value),
        )?;
      }
      _ => {
        let qc = helpers::resolve_column(expr, alias_map, referenced_tables, table_schemas)?;
        proj_alias_map.insert(alias.value.clone(), qc.clone());
        projection_qc.push(qc);
      }
    },
  }
  Ok(())
}

#[allow(clippy::too_many_arguments)]
fn parse_projection_expression(
  expr: &SqlExpr,
  alias_map: &HashMap<String, String>,
  referenced_tables: &[String],
  table_schemas: &HashMap<String, db_engine::TableSchema>,
  aggregates: &mut Vec<db_engine::Aggregate>,
  proj_alias_map: &mut HashMap<String, db_engine::QualifiedColumn>,
  projection_qc: &mut Vec<db_engine::QualifiedColumn>,
  alias_name: Option<&str>,
) -> Result<(), TranslateError> {
  match expr {
    SqlExpr::Function(func) => parse_aggregate_function(
      func,
      alias_name,
      alias_map,
      referenced_tables,
      table_schemas,
      aggregates,
      proj_alias_map,
    ),
    _ => {
      let qc = helpers::resolve_column(expr, alias_map, referenced_tables, table_schemas)?;
      projection_qc.push(qc);
      Ok(())
    }
  }
}

fn parse_aggregate_function(
  func: &sqlparser::ast::Function,
  alias_name: Option<&str>,
  alias_map: &HashMap<String, String>,
  referenced_tables: &[String],
  table_schemas: &HashMap<String, db_engine::TableSchema>,
  aggregates: &mut Vec<db_engine::Aggregate>,
  proj_alias_map: &mut HashMap<String, db_engine::QualifiedColumn>,
) -> Result<(), TranslateError> {
  let fname = func.name.to_string().to_lowercase();
  let first = fname.split('.').next().unwrap_or("");
  let args = match &func.args {
    FunctionArguments::List(list) => &list.args[..],
    FunctionArguments::None => &[],
    FunctionArguments::Subquery(_) => {
      return Err(TranslateError::UnsupportedFeature(
        "function with subquery args unsupported".into(),
      ));
    }
  };

  if args.len() != 1 {
    return Err(TranslateError::UnsupportedFeature(
      "aggregate takes one argument".into(),
    ));
  }

  match first {
    "count" => match &args[0] {
      FunctionArg::Unnamed(FunctionArgExpr::Wildcard) => {
        aggregates.push(db_engine::Aggregate::Count(None));
        Ok(())
      }
      FunctionArg::Unnamed(FunctionArgExpr::Expr(arg_expr)) => {
        let qc = helpers::resolve_column(arg_expr, alias_map, referenced_tables, table_schemas)?;
        aggregates.push(db_engine::Aggregate::Count(Some(qc.clone())));
        if let Some(alias) = alias_name {
          proj_alias_map.insert(alias.to_string(), qc);
        }
        Ok(())
      }
      FunctionArg::Unnamed(FunctionArgExpr::QualifiedWildcard(_)) => Err(
        TranslateError::UnsupportedFeature("qualified wildcard in aggregate not supported".into()),
      ),
      _ => Err(TranslateError::UnsupportedFeature(
        "unsupported COUNT args".into(),
      )),
    },
    "sum" | "min" | "max" | "avg" => match &args[0] {
      FunctionArg::Unnamed(FunctionArgExpr::Expr(arg_expr)) => {
        let qc = helpers::resolve_column(arg_expr, alias_map, referenced_tables, table_schemas)?;
        match first {
          "sum" => aggregates.push(db_engine::Aggregate::Sum(qc.clone())),
          "min" => aggregates.push(db_engine::Aggregate::Min(qc.clone())),
          "max" => aggregates.push(db_engine::Aggregate::Max(qc.clone())),
          "avg" => aggregates.push(db_engine::Aggregate::Avg(qc.clone())),
          _ => {}
        }
        if let Some(alias) = alias_name {
          proj_alias_map.insert(alias.to_string(), qc);
        }
        Ok(())
      }
      _ => Err(TranslateError::UnsupportedFeature(
        "unsupported aggregate arg".into(),
      )),
    },
    _ => Err(TranslateError::UnsupportedFeature(format!(
      "unsupported function: {}",
      first,
    ))),
  }
}

fn parse_group_by(
  group_by: &GroupByExpr,
  alias_map: &HashMap<String, String>,
  referenced_tables: &[String],
  table_schemas: &HashMap<String, db_engine::TableSchema>,
) -> Result<Vec<db_engine::QualifiedColumn>, TranslateError> {
  match group_by {
    GroupByExpr::Expressions(exprs, _modifiers) => exprs
      .iter()
      .map(|gb| helpers::resolve_column(gb, alias_map, referenced_tables, table_schemas))
      .collect(),
    GroupByExpr::All(_) => Err(TranslateError::UnsupportedFeature(
      "GROUP BY ALL not supported".into(),
    )),
  }
}

fn parse_order_by(
  order_by: &Option<sqlparser::ast::OrderBy>,
  proj_alias_map: &HashMap<String, db_engine::QualifiedColumn>,
  alias_map: &HashMap<String, String>,
  referenced_tables: &[String],
  table_schemas: &HashMap<String, db_engine::TableSchema>,
) -> Result<Vec<db_engine::OrderBy>, TranslateError> {
  let mut order_by_vec: Vec<db_engine::OrderBy> = Vec::new();
  if let Some(ob_clause) = order_by {
    match &ob_clause.kind {
      sqlparser::ast::OrderByKind::Expressions(exprs) => {
        for ob in exprs {
          let qc = match &ob.expr {
            SqlExpr::Identifier(ident) => {
              if let Some(qc) = proj_alias_map.get(&ident.value) {
                qc.clone()
              } else {
                helpers::resolve_column(&ob.expr, alias_map, referenced_tables, table_schemas)?
              }
            }
            SqlExpr::CompoundIdentifier(_) => {
              helpers::resolve_column(&ob.expr, alias_map, referenced_tables, table_schemas)?
            }
            _ => {
              return Err(TranslateError::UnsupportedFeature(
                "unsupported ORDER BY expression".into(),
              ));
            }
          };

          let dir = match ob.options.asc {
            Some(true) | None => db_engine::SortDirection::Asc,
            Some(false) => db_engine::SortDirection::Desc,
          };
          order_by_vec.push(db_engine::OrderBy {
            expr: qc,
            direction: dir,
          });
        }
      }
      sqlparser::ast::OrderByKind::All(_all) => {
        return Err(TranslateError::UnsupportedFeature(
          "ORDER BY ALL not supported".into(),
        ));
      }
    }
  }
  Ok(order_by_vec)
}

fn parse_limit_clause(
  limit_clause: &Option<LimitClause>,
) -> Result<(Option<usize>, Option<usize>), TranslateError> {
  let mut limit_val: Option<usize> = None;
  let mut offset_val: Option<usize> = None;

  if let Some(lc) = limit_clause {
    match lc {
      LimitClause::LimitOffset { limit, offset, .. } => {
        if let Some(lim_expr) = limit {
          match lim_expr {
            SqlExpr::Value(v) => match &v.value {
              SqlValue::Number(s, _) => {
                limit_val =
                  Some(s.parse::<usize>().map_err(|_| {
                    TranslateError::UnsupportedFeature("invalid LIMIT value".into())
                  })?);
              }
              _ => {
                return Err(TranslateError::UnsupportedFeature(
                  "unsupported LIMIT expression".into(),
                ));
              }
            },
            _ => {
              return Err(TranslateError::UnsupportedFeature(
                "unsupported LIMIT expression".into(),
              ));
            }
          }
        }
        if let Some(off_struct) = offset {
          match &off_struct.value {
            SqlExpr::Value(v) => match &v.value {
              SqlValue::Number(s, _) => {
                offset_val = Some(s.parse::<usize>().map_err(|_| {
                  TranslateError::UnsupportedFeature("invalid OFFSET value".into())
                })?);
              }
              _ => {
                return Err(TranslateError::UnsupportedFeature(
                  "unsupported OFFSET expression".into(),
                ));
              }
            },
            _ => {
              return Err(TranslateError::UnsupportedFeature(
                "unsupported OFFSET expression".into(),
              ));
            }
          }
        }
      }
      LimitClause::OffsetCommaLimit { offset, limit } => {
        match offset {
          SqlExpr::Value(v) => match &v.value {
            SqlValue::Number(s, _) => {
              offset_val =
                Some(s.parse::<usize>().map_err(|_| {
                  TranslateError::UnsupportedFeature("invalid OFFSET value".into())
                })?);
            }
            _ => {
              return Err(TranslateError::UnsupportedFeature(
                "unsupported OFFSET expression".into(),
              ));
            }
          },
          _ => {
            return Err(TranslateError::UnsupportedFeature(
              "unsupported OFFSET expression".into(),
            ));
          }
        }
        match limit {
          SqlExpr::Value(v) => match &v.value {
            SqlValue::Number(s, _) => {
              limit_val = Some(
                s.parse::<usize>()
                  .map_err(|_| TranslateError::UnsupportedFeature("invalid LIMIT value".into()))?,
              );
            }
            _ => {
              return Err(TranslateError::UnsupportedFeature(
                "unsupported LIMIT expression".into(),
              ));
            }
          },
          _ => {
            return Err(TranslateError::UnsupportedFeature(
              "unsupported LIMIT expression".into(),
            ));
          }
        }
      }
    }
  }

  Ok((limit_val, offset_val))
}

#[cfg(test)]
mod tests {
  use super::*;
  #[cfg(not(feature = "std"))]
  use alloc::string::String;
  #[cfg(not(feature = "std"))]
  use alloc::vec::Vec;
  use db_engine::{
    ColumnSchema, EngineQuery, EngineType, EngineValue, JoinKind, JoinOn, QualifiedColumn,
    QualifiedOperand, QualifiedPredicate, TableSchema, UpdateAssignment, UpdateValueExpr,
  };
  #[cfg(not(feature = "std"))]
  use hashbrown::HashMap;
  #[cfg(feature = "std")]
  use std::collections::HashMap;
  use uuid::Uuid;

  struct DummyResolver {
    tables: HashMap<String, TableSchema>,
  }

  impl SchemaResolver for DummyResolver {
    fn describe_table(&self, name: &str) -> Option<TableSchema> {
      self.tables.get(name).cloned()
    }
  }

  #[test]
  fn translate_to_ir_returns_canonical_query() {
    let mut tables = HashMap::new();
    tables.insert(
      "users".into(),
      TableSchema {
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
      },
    );

    let resolver = DummyResolver { tables };

    let canon = parse_and_translate_to_ir("SELECT id FROM users WHERE id = 42;", &resolver)
      .expect("translate to IR");

    match canon.engine_query {
      EngineQuery::Select {
        table,
        projection,
        predicate,
        options: _,
      } => {
        assert_eq!(table, "users");
        assert_eq!(projection.len(), 1);
        assert_eq!(projection[0].table, "users");
        assert_eq!(projection[0].column_index, 0);
        match predicate {
          Some(QualifiedPredicate::Equals(
            QualifiedOperand::Column(qc),
            QualifiedOperand::Value(v),
          )) => {
            assert_eq!(qc.column_index, 0);
            assert_eq!(v, EngineValue::Integer(42));
          }
          other => panic!("unexpected predicate: {:?}", other),
        }
      }
      other => panic!("unexpected query kind: {:?}", other),
    }
  }

  #[test]
  fn translate_simple_select() {
    let mut tables = HashMap::new();
    tables.insert(
      "users".into(),
      TableSchema {
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
      },
    );

    let resolver = DummyResolver { tables };

    let q = parse_and_translate("SELECT id, name FROM users WHERE id = 42;", &resolver)
      .expect("translate");

    match q {
      EngineQuery::Select {
        table,
        projection,
        predicate,
        options: _,
      } => {
        assert_eq!(table, "users");
        assert_eq!(projection.len(), 2usize);
        assert_eq!(
          projection[0],
          QualifiedColumn {
            table: "users".into(),
            column_index: 0
          }
        );
        assert_eq!(
          projection[1],
          QualifiedColumn {
            table: "users".into(),
            column_index: 1
          }
        );
        match predicate {
          Some(QualifiedPredicate::Equals(
            QualifiedOperand::Column(qc),
            QualifiedOperand::Value(v),
          )) => {
            assert_eq!(qc.column_index, 0usize);
            assert_eq!(v, EngineValue::Integer(42));
          }
          other => panic!("unexpected predicate: {:?}", other),
        }
      }
      other => panic!("unexpected query kind: {:?}", other),
    }
  }

  #[test]
  fn translate_join_select() {
    let mut tables = HashMap::new();
    tables.insert(
      "users".into(),
      TableSchema {
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
      },
    );

    tables.insert(
      "orders".into(),
      TableSchema {
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
      },
    );

    let resolver = DummyResolver { tables };

    let q = parse_and_translate(
      "SELECT u.name, o.amount FROM users u JOIN orders o ON u.id = o.user_id;",
      &resolver,
    )
    .expect("translate");

    match q {
      EngineQuery::Select {
        table,
        projection,
        predicate: _,
        options,
      } => {
        assert_eq!(table, "users");
        // projection: users.name then orders.amount
        assert_eq!(projection.len(), 2);
        assert_eq!(projection[0].table, "users");
        assert_eq!(projection[0].column_index, 1);
        assert_eq!(projection[1].table, "orders");
        assert_eq!(projection[1].column_index, 2);

        assert_eq!(options.joins.len(), 1);
        let j = &options.joins[0];
        assert_eq!(j.kind, JoinKind::Inner);
        match &j.on {
          JoinOn::ColumnEq { left, right } => {
            assert_eq!(left.table, "users");
            assert_eq!(left.column_index, 0);
            assert_eq!(right.table, "orders");
            assert_eq!(right.column_index, 1);
          }
        }
      }
      other => panic!("unexpected query: {:?}", other),
    }
  }

  #[test]
  fn translate_join_select_with_qualified_wildcard_projection() {
    let mut tables = HashMap::new();
    tables.insert(
      "recipes".into(),
      TableSchema {
        name: "recipes".into(),
        columns: vec![
          ColumnSchema {
            name: "id".into(),
            data_type: EngineType::Integer,
          },
          ColumnSchema {
            name: "updatedAt".into(),
            data_type: EngineType::Text,
          },
        ],
        primary_key: vec![0],
      },
    );

    tables.insert(
      "comments".into(),
      TableSchema {
        name: "comments".into(),
        columns: vec![
          ColumnSchema {
            name: "id".into(),
            data_type: EngineType::Integer,
          },
          ColumnSchema {
            name: "recipeId".into(),
            data_type: EngineType::Integer,
          },
        ],
        primary_key: vec![0],
      },
    );

    let resolver = DummyResolver { tables };

    let q = parse_and_translate(
      "SELECT t0.* FROM recipes AS t0 LEFT JOIN comments AS j1 ON t0.id = j1.recipeId ORDER BY t0.updatedAt DESC;",
      &resolver,
    )
    .expect("translate");

    match q {
      EngineQuery::Select {
        table,
        projection,
        options,
        ..
      } => {
        assert_eq!(table, "recipes");
        assert_eq!(projection.len(), 2);
        assert_eq!(projection[0].table, "recipes");
        assert_eq!(projection[0].column_index, 0);
        assert_eq!(projection[1].table, "recipes");
        assert_eq!(projection[1].column_index, 1);
        assert_eq!(options.joins.len(), 1);
        assert_eq!(options.joins[0].kind, JoinKind::Left);
      }
      other => panic!("unexpected query: {:?}", other),
    }
  }

  #[test]
  fn translate_group_by_aggregate_order_limit() {
    let mut tables = HashMap::new();
    tables.insert(
      "users".into(),
      TableSchema {
        name: "users".into(),
        columns: vec![
          ColumnSchema {
            name: "id".into(),
            data_type: EngineType::Integer,
          },
          ColumnSchema {
            name: "city".into(),
            data_type: EngineType::Text,
          },
        ],
        primary_key: vec![0],
      },
    );

    tables.insert(
      "orders".into(),
      TableSchema {
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
      },
    );

    let resolver = DummyResolver { tables };

    let sql = "SELECT city, COUNT(*) as cnt, SUM(o.amount) as total FROM users u JOIN orders o ON u.id = o.user_id GROUP BY city ORDER BY total DESC LIMIT 5 OFFSET 2;";

    let q = parse_and_translate(sql, &resolver).expect("translate");

    match q {
      EngineQuery::Select {
        table,
        projection,
        predicate: _,
        options,
      } => {
        assert_eq!(table, "users");
        // for grouped queries projection may be empty; aggregates and group_by should be set
        assert!(projection.is_empty());
        assert_eq!(options.group_by.len(), 1);
        assert_eq!(options.aggregates.len(), 2);

        // order by should reference the aggregate (SUM) or group key; ensure limit/offset set
        assert_eq!(options.limit, Some(5));
        assert_eq!(options.offset, Some(2));
      }
      other => panic!("unexpected query: {:?}", other),
    }
  }

  #[test]
  fn default_value_mapper_converts_literals() {
    let vm = DefaultValueMapper;

    // integer
    let int_expr: SqlExpr =
      sqlparser::ast::Expr::Value(sqlparser::ast::Value::Number("123".into(), false).into());
    let v = vm.map_sql_value(&int_expr).expect("int");
    assert_eq!(v, db_engine::EngineValue::Integer(123));

    // float
    let float_expr: SqlExpr =
      sqlparser::ast::Expr::Value(sqlparser::ast::Value::Number("1.5".into(), false).into());
    let v = vm.map_sql_value(&float_expr).expect("float");
    assert_eq!(v, db_engine::EngineValue::Float(1.5));

    // string
    let s_expr: SqlExpr =
      sqlparser::ast::Expr::Value(sqlparser::ast::Value::SingleQuotedString("hello".into()).into());
    let v = vm.map_sql_value(&s_expr).expect("string");
    assert_eq!(v, db_engine::EngineValue::Text("hello".into()));

    // null
    let n_expr: SqlExpr = sqlparser::ast::Expr::Value(sqlparser::ast::Value::Null.into());
    let v = vm.map_sql_value(&n_expr).expect("null");
    assert_eq!(v, db_engine::EngineValue::Null);
  }

  #[test]
  fn default_value_mapper_supports_explicit_uuid_cast_only() {
    let vm = DefaultValueMapper;

    let raw_uuid_expr: SqlExpr = sqlparser::ast::Expr::Value(
      sqlparser::ast::Value::SingleQuotedString("550e8400-e29b-41d4-a716-446655440000".into())
        .into(),
    );
    let raw = vm.map_sql_value(&raw_uuid_expr).expect("raw uuid string");
    assert_eq!(
      raw,
      db_engine::EngineValue::Text("550e8400-e29b-41d4-a716-446655440000".into())
    );

    let cast_uuid_expr = SqlExpr::Cast {
      kind: sqlparser::ast::CastKind::DoubleColon,
      expr: Box::new(raw_uuid_expr),
      data_type: sqlparser::ast::DataType::Uuid,
      array: false,
      format: None,
    };
    let cast = vm.map_sql_value(&cast_uuid_expr).expect("cast uuid");
    let expected = *Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000")
      .expect("valid uuid")
      .as_bytes();
    assert_eq!(cast, db_engine::EngineValue::Uuid(expected));
  }

  #[test]
  fn translate_create_table_requires_uuid_primary_key() {
    let resolver = DummyResolver {
      tables: HashMap::new(),
    };

    let err = parse_and_translate_statement(
      "CREATE TABLE users (id INT PRIMARY KEY, name TEXT);",
      &resolver,
    )
    .expect_err("int primary key should be rejected");

    assert!(matches!(
      err,
      TranslateError::UnsupportedFeature(message)
      if message.contains("PRIMARY KEY column must use UUID type")
    ));
  }

  #[test]
  fn translate_create_table_accepts_uuid_primary_key() {
    let resolver = DummyResolver {
      tables: HashMap::new(),
    };

    let statement = parse_and_translate_statement(
      "CREATE TABLE users (id UUID PRIMARY KEY, name TEXT);",
      &resolver,
    )
    .expect("uuid primary key should be accepted");

    match statement {
      crate::ir::CanonicalStatement::Ddl(crate::ir::DdlOp::CreateTable(table, _)) => {
        assert_eq!(table.primary_key, vec![0]);
        assert_eq!(table.columns[0].data_type, EngineType::Uuid);
      }
      other => panic!("unexpected statement: {:?}", other),
    }
  }

  #[test]
  fn translate_create_table_preserves_unquoted_table_name() {
    let resolver = DummyResolver {
      tables: HashMap::new(),
    };

    let statement = parse_and_translate_statement(
      r#"CREATE TABLE "userSettings" (id UUID PRIMARY KEY, theme TEXT);"#,
      &resolver,
    )
    .expect("quoted table name should be accepted");

    match statement {
      crate::ir::CanonicalStatement::Ddl(crate::ir::DdlOp::CreateTable(table, _)) => {
        assert_eq!(table.name, "userSettings");
      }
      other => panic!("unexpected statement: {:?}", other),
    }
  }

  #[test]
  fn translate_with_custom_mapper() {
    struct MyMapper;
    impl ValueMapper for MyMapper {
      fn map_sql_value(&self, expr: &SqlExpr) -> Result<db_engine::EngineValue, TranslateError> {
        // Map all numeric literals to text for test visibility
        match expr {
          SqlExpr::Value(v) => match &v.value {
            SqlValue::Number(s, _) => Ok(db_engine::EngineValue::Text(s.clone())),
            SqlValue::SingleQuotedString(s) => Ok(db_engine::EngineValue::Text(s.clone())),
            SqlValue::Null => Ok(db_engine::EngineValue::Null),
            _ => sql_value_to_engine_value(expr),
          },
          _ => Err(TranslateError::UnsupportedFeature(
            "expected literal".into(),
          )),
        }
      }
    }

    let mut tables = HashMap::new();
    tables.insert(
      "users".into(),
      TableSchema {
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
      },
    );

    let resolver = DummyResolver { tables };

    // Use custom mapper which turns numeric 42 into text "42"
    let canon = parse_and_translate_to_ir_with_mapper(
      "SELECT id FROM users WHERE id = 42;",
      &resolver,
      &MyMapper,
    )
    .expect("translate with mapper");

    match canon.engine_query {
      EngineQuery::Select { predicate, .. } => match predicate {
        Some(QualifiedPredicate::Equals(_, QualifiedOperand::Value(v))) => {
          assert_eq!(v, EngineValue::Text("42".into()));
        }
        other => panic!("unexpected predicate: {:?}", other),
      },
      other => panic!("unexpected query kind: {:?}", other),
    }
  }

  #[test]
  fn translate_with_positional_params() {
    let mut tables = HashMap::new();
    tables.insert(
      "users".into(),
      TableSchema {
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
      },
    );

    let resolver = DummyResolver { tables };
    let params = SqlParams::from(vec![EngineValue::Integer(7)]);

    let canon = parse_and_translate_to_ir_with_params(
      "SELECT id FROM users WHERE id = $1;",
      &resolver,
      &params,
    )
    .expect("translate with positional params");

    match canon.engine_query {
      EngineQuery::Select { predicate, .. } => match predicate {
        Some(QualifiedPredicate::Equals(_, QualifiedOperand::Value(v))) => {
          assert_eq!(v, EngineValue::Integer(7));
        }
        other => panic!("unexpected predicate: {:?}", other),
      },
      other => panic!("unexpected query kind: {:?}", other),
    }
  }

  #[test]
  fn translate_insert_with_uuid_positional_param_coerces_text_to_uuid() {
    let mut tables = HashMap::new();
    tables.insert(
      "users".into(),
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
    );

    let resolver = DummyResolver { tables };
    let raw_id = "550e8400-e29b-41d4-a716-446655440000";
    let params = SqlParams::from(vec![
      EngineValue::Text(raw_id.into()),
      EngineValue::Text("Ada".into()),
    ]);

    let q = parse_and_translate_with_params(
      "INSERT INTO users (id, name) VALUES ($1, $2);",
      &resolver,
      &params,
    )
    .expect("translate insert with uuid params");

    match q {
      EngineQuery::Insert { row, .. } => {
        let expected = *Uuid::parse_str(raw_id).expect("valid uuid").as_bytes();
        assert_eq!(row[0], EngineValue::Uuid(expected));
        assert_eq!(row[1], EngineValue::Text("Ada".into()));
      }
      other => panic!("unexpected query kind: {:?}", other),
    }
  }

  #[test]
  fn translate_insert_with_invalid_uuid_positional_param_is_rejected() {
    let mut tables = HashMap::new();
    tables.insert(
      "users".into(),
      TableSchema {
        name: "users".into(),
        columns: vec![ColumnSchema {
          name: "id".into(),
          data_type: EngineType::Uuid,
        }],
        primary_key: vec![0],
      },
    );

    let resolver = DummyResolver { tables };
    let params = SqlParams::from(vec![EngineValue::Text("not-a-uuid".into())]);

    let err =
      parse_and_translate_with_params("INSERT INTO users (id) VALUES ($1);", &resolver, &params)
        .expect_err("invalid uuid should be rejected");

    assert!(matches!(
      err,
      TranslateError::UnsupportedFeature(message) if message.contains("invalid UUID literal")
    ));
  }

  #[test]
  fn translate_with_named_params() {
    let mut tables = HashMap::new();
    tables.insert(
      "users".into(),
      TableSchema {
        name: "users".into(),
        columns: vec![
          ColumnSchema {
            name: "id".into(),
            data_type: EngineType::Integer,
          },
          ColumnSchema {
            name: "score".into(),
            data_type: EngineType::Integer,
          },
        ],
        primary_key: vec![0],
      },
    );

    let resolver = DummyResolver { tables };
    let params = SqlParams::named([("user_id", EngineValue::Integer(11))]);

    let canon = parse_and_translate_to_ir_with_params(
      "SELECT id FROM users WHERE id = :user_id OR score = :user_id;",
      &resolver,
      &params,
    )
    .expect("translate with named params");

    match canon.engine_query {
      EngineQuery::Select { predicate, .. } => match predicate {
        Some(QualifiedPredicate::Or(left, right)) => {
          assert!(matches!(
            *left,
            QualifiedPredicate::Equals(_, QualifiedOperand::Value(EngineValue::Integer(11)))
          ));
          assert!(matches!(
            *right,
            QualifiedPredicate::Equals(_, QualifiedOperand::Value(EngineValue::Integer(11)))
          ));
        }
        other => panic!("unexpected predicate: {:?}", other),
      },
      other => panic!("unexpected query kind: {:?}", other),
    }
  }

  #[test]
  fn translate_with_mixed_param_styles_is_rejected() {
    let mut tables = HashMap::new();
    tables.insert(
      "users".into(),
      TableSchema {
        name: "users".into(),
        columns: vec![ColumnSchema {
          name: "id".into(),
          data_type: EngineType::Integer,
        }],
        primary_key: vec![0],
      },
    );

    let resolver = DummyResolver { tables };
    let params = SqlParams::named([("id", EngineValue::Integer(1))]);
    let err = parse_and_translate_to_ir_with_params(
      "SELECT id FROM users WHERE id = :id OR id = $1;",
      &resolver,
      &params,
    )
    .expect_err("mixed styles should fail");

    assert!(matches!(err, TranslateError::MixedParameterStyles));
  }

  #[test]
  fn translate_with_missing_named_param_is_rejected() {
    let mut tables = HashMap::new();
    tables.insert(
      "users".into(),
      TableSchema {
        name: "users".into(),
        columns: vec![ColumnSchema {
          name: "id".into(),
          data_type: EngineType::Integer,
        }],
        primary_key: vec![0],
      },
    );

    let resolver = DummyResolver { tables };
    let err = parse_and_translate_to_ir_with_params(
      "SELECT id FROM users WHERE id = :missing;",
      &resolver,
      &SqlParams::default(),
    )
    .expect_err("missing named parameter should fail");

    assert!(matches!(err, TranslateError::MissingNamedParameter(name) if name == "missing"));
  }

  #[test]
  fn translate_with_missing_positional_param_is_rejected() {
    let mut tables = HashMap::new();
    tables.insert(
      "users".into(),
      TableSchema {
        name: "users".into(),
        columns: vec![ColumnSchema {
          name: "id".into(),
          data_type: EngineType::Integer,
        }],
        primary_key: vec![0],
      },
    );

    let resolver = DummyResolver { tables };
    let err = parse_and_translate_to_ir_with_params(
      "SELECT id FROM users WHERE id = $1;",
      &resolver,
      &SqlParams::default(),
    )
    .expect_err("missing positional parameter should fail");

    assert!(matches!(err, TranslateError::MissingPositionalParameter(1)));
  }

  #[test]
  fn translate_insert_values() {
    let mut tables = HashMap::new();
    tables.insert(
      "items".into(),
      TableSchema {
        name: "items".into(),
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
      },
    );

    let resolver = DummyResolver { tables };
    let q = parse_and_translate("INSERT INTO items (id, name) VALUES (1, 'One');", &resolver)
      .expect("translate insert");

    match q {
      EngineQuery::Insert {
        table,
        row,
        returning,
      } => {
        assert_eq!(table, "items");
        assert!(returning.is_none());
        assert_eq!(
          row,
          vec![EngineValue::Integer(1), EngineValue::Text("One".into())]
        );
      }
      other => panic!("unexpected query kind: {:?}", other),
    }
  }

  #[test]
  fn translate_insert_returning_projection() {
    let mut tables = HashMap::new();
    tables.insert(
      "items".into(),
      TableSchema {
        name: "items".into(),
        columns: vec![
          ColumnSchema {
            name: "id".into(),
            data_type: EngineType::Integer,
          },
          ColumnSchema {
            name: "score".into(),
            data_type: EngineType::Integer,
          },
        ],
        primary_key: vec![0],
      },
    );

    let resolver = DummyResolver { tables };
    let q = parse_and_translate(
      "INSERT INTO items (id, score) VALUES (1, 9) RETURNING id, score + 1;",
      &resolver,
    )
    .expect("translate insert returning");

    match q {
      EngineQuery::Insert {
        table,
        row,
        returning,
      } => {
        assert_eq!(table, "items");
        assert_eq!(row, vec![EngineValue::Integer(1), EngineValue::Integer(9)]);
        let returning = returning.expect("returning projection");
        assert_eq!(
          returning,
          vec![
            UpdateValueExpr::Column(QualifiedColumn {
              table: "items".into(),
              column_index: 0,
            }),
            UpdateValueExpr::Add(
              Box::new(UpdateValueExpr::Column(QualifiedColumn {
                table: "items".into(),
                column_index: 1,
              })),
              Box::new(UpdateValueExpr::Value(EngineValue::Integer(1))),
            ),
          ]
        );
      }
      other => panic!("unexpected query kind: {:?}", other),
    }
  }

  #[test]
  fn translate_insert_returning_rejects_aggregate() {
    let mut tables = HashMap::new();
    tables.insert(
      "items".into(),
      TableSchema {
        name: "items".into(),
        columns: vec![ColumnSchema {
          name: "id".into(),
          data_type: EngineType::Integer,
        }],
        primary_key: vec![0],
      },
    );

    let resolver = DummyResolver { tables };
    let error = parse_and_translate(
      "INSERT INTO items (id) VALUES (1) RETURNING COUNT(id);",
      &resolver,
    )
    .expect_err("RETURNING aggregate should be rejected");

    assert!(matches!(
      error,
      TranslateError::UnsupportedFeature(message)
      if message.contains("unsupported UPDATE assignment expression")
    ));
  }

  #[test]
  fn translate_insert_returning_rejects_subquery() {
    let mut tables = HashMap::new();
    tables.insert(
      "items".into(),
      TableSchema {
        name: "items".into(),
        columns: vec![ColumnSchema {
          name: "id".into(),
          data_type: EngineType::Integer,
        }],
        primary_key: vec![0],
      },
    );

    let resolver = DummyResolver { tables };
    let error = parse_and_translate(
      "INSERT INTO items (id) VALUES (1) RETURNING (SELECT 1);",
      &resolver,
    )
    .expect_err("RETURNING subquery should be rejected");

    assert!(matches!(
      error,
      TranslateError::UnsupportedFeature(message)
      if message.contains("unsupported UPDATE assignment expression")
    ));
  }

  #[test]
  fn translate_update_values() {
    let mut tables = HashMap::new();
    tables.insert(
      "users".into(),
      TableSchema {
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
          ColumnSchema {
            name: "score".into(),
            data_type: EngineType::Integer,
          },
        ],
        primary_key: vec![0],
      },
    );

    let resolver = DummyResolver { tables };
    let q = parse_and_translate("UPDATE users SET score = 11 WHERE id = 1;", &resolver)
      .expect("translate update");

    match q {
      EngineQuery::Update {
        table,
        assignments,
        predicate,
        joins,
        from_tables,
        returning,
      } => {
        assert_eq!(table, "users");
        assert_eq!(assignments.len(), 1);
        assert!(joins.is_empty());
        assert!(from_tables.is_empty());
        assert!(returning.is_none());
        assert_eq!(
          assignments[0],
          UpdateAssignment {
            column_index: 2,
            value: UpdateValueExpr::Value(EngineValue::Integer(11)),
          }
        );

        match predicate {
          Some(QualifiedPredicate::Equals(
            QualifiedOperand::Column(qc),
            QualifiedOperand::Value(v),
          )) => {
            assert_eq!(qc.table, "users");
            assert_eq!(qc.column_index, 0);
            assert_eq!(v, EngineValue::Integer(1));
          }
          other => panic!("unexpected predicate: {:?}", other),
        }
      }
      other => panic!("unexpected query kind: {:?}", other),
    }
  }

  #[test]
  fn translate_update_expression_value() {
    let mut tables = HashMap::new();
    tables.insert(
      "users".into(),
      TableSchema {
        name: "users".into(),
        columns: vec![
          ColumnSchema {
            name: "id".into(),
            data_type: EngineType::Integer,
          },
          ColumnSchema {
            name: "score".into(),
            data_type: EngineType::Integer,
          },
        ],
        primary_key: vec![0],
      },
    );

    let resolver = DummyResolver { tables };
    let q = parse_and_translate(
      "UPDATE users SET score = score + 1 WHERE id = 1;",
      &resolver,
    )
    .expect("translate update expression");

    match q {
      EngineQuery::Update { assignments, .. } => {
        assert_eq!(assignments.len(), 1);
        assert_eq!(assignments[0].column_index, 1);
        assert_eq!(
          assignments[0].value,
          UpdateValueExpr::Add(
            Box::new(UpdateValueExpr::Column(QualifiedColumn {
              table: "users".into(),
              column_index: 1,
            })),
            Box::new(UpdateValueExpr::Value(EngineValue::Integer(1)))
          )
        );
      }
      other => panic!("unexpected query kind: {:?}", other),
    }
  }

  #[test]
  fn translate_update_join_expression_value() {
    let mut tables = HashMap::new();
    tables.insert(
      "users".into(),
      TableSchema {
        name: "users".into(),
        columns: vec![
          ColumnSchema {
            name: "id".into(),
            data_type: EngineType::Integer,
          },
          ColumnSchema {
            name: "team_id".into(),
            data_type: EngineType::Integer,
          },
          ColumnSchema {
            name: "score".into(),
            data_type: EngineType::Integer,
          },
        ],
        primary_key: vec![0],
      },
    );
    tables.insert(
      "teams".into(),
      TableSchema {
        name: "teams".into(),
        columns: vec![
          ColumnSchema {
            name: "id".into(),
            data_type: EngineType::Integer,
          },
          ColumnSchema {
            name: "bonus".into(),
            data_type: EngineType::Integer,
          },
        ],
        primary_key: vec![0],
      },
    );

    let resolver = DummyResolver { tables };
    let q = parse_and_translate(
      "UPDATE users u JOIN teams t ON u.team_id = t.id SET score = score + t.bonus WHERE u.id = 1;",
      &resolver,
    )
    .expect("translate update join expression");

    match q {
      EngineQuery::Update {
        assignments,
        joins,
        from_tables,
        returning,
        ..
      } => {
        assert_eq!(joins.len(), 1);
        assert!(from_tables.is_empty());
        assert!(returning.is_none());
        assert_eq!(assignments.len(), 1);
        assert_eq!(assignments[0].column_index, 2);
        assert_eq!(
          assignments[0].value,
          UpdateValueExpr::Add(
            Box::new(UpdateValueExpr::Column(QualifiedColumn {
              table: "users".into(),
              column_index: 2,
            })),
            Box::new(UpdateValueExpr::Column(QualifiedColumn {
              table: "teams".into(),
              column_index: 1,
            }))
          )
        );
      }
      other => panic!("unexpected query kind: {:?}", other),
    }
  }

  #[test]
  fn translate_update_from_expression_value() {
    let mut tables = HashMap::new();
    tables.insert(
      "users".into(),
      TableSchema {
        name: "users".into(),
        columns: vec![
          ColumnSchema {
            name: "id".into(),
            data_type: EngineType::Integer,
          },
          ColumnSchema {
            name: "team_id".into(),
            data_type: EngineType::Integer,
          },
          ColumnSchema {
            name: "score".into(),
            data_type: EngineType::Integer,
          },
        ],
        primary_key: vec![0],
      },
    );
    tables.insert(
      "teams".into(),
      TableSchema {
        name: "teams".into(),
        columns: vec![
          ColumnSchema {
            name: "id".into(),
            data_type: EngineType::Integer,
          },
          ColumnSchema {
            name: "bonus".into(),
            data_type: EngineType::Integer,
          },
        ],
        primary_key: vec![0],
      },
    );

    let resolver = DummyResolver { tables };
    let q = parse_and_translate(
      "UPDATE users SET score = score + teams.bonus FROM teams WHERE users.team_id = teams.id AND users.id = 1;",
      &resolver,
    )
    .expect("translate update from expression");

    match q {
      EngineQuery::Update {
        assignments,
        joins,
        from_tables,
        returning,
        ..
      } => {
        assert!(joins.is_empty());
        assert_eq!(from_tables, vec!["teams".to_string()]);
        assert!(returning.is_none());
        assert_eq!(assignments.len(), 1);
        assert_eq!(assignments[0].column_index, 2);
        assert_eq!(
          assignments[0].value,
          UpdateValueExpr::Add(
            Box::new(UpdateValueExpr::Column(QualifiedColumn {
              table: "users".into(),
              column_index: 2,
            })),
            Box::new(UpdateValueExpr::Column(QualifiedColumn {
              table: "teams".into(),
              column_index: 1,
            }))
          )
        );
      }
      other => panic!("unexpected query kind: {:?}", other),
    }
  }

  #[test]
  fn translate_delete_values() {
    let mut tables = HashMap::new();
    tables.insert(
      "users".into(),
      TableSchema {
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
      },
    );

    let resolver = DummyResolver { tables };
    let q =
      parse_and_translate("DELETE FROM users WHERE id = 2;", &resolver).expect("translate delete");

    match q {
      EngineQuery::Delete {
        table,
        predicate,
        returning,
      } => {
        assert_eq!(table, "users");
        assert!(returning.is_none());
        match predicate {
          Some(QualifiedPredicate::Equals(
            QualifiedOperand::Column(qc),
            QualifiedOperand::Value(v),
          )) => {
            assert_eq!(qc.table, "users");
            assert_eq!(qc.column_index, 0);
            assert_eq!(v, EngineValue::Integer(2));
          }
          other => panic!("unexpected predicate: {:?}", other),
        }
      }
      other => panic!("unexpected query kind: {:?}", other),
    }
  }

  #[test]
  fn translate_delete_returning_projection() {
    let mut tables = HashMap::new();
    tables.insert(
      "users".into(),
      TableSchema {
        name: "users".into(),
        columns: vec![ColumnSchema {
          name: "id".into(),
          data_type: EngineType::Integer,
        }],
        primary_key: vec![0],
      },
    );

    let resolver = DummyResolver { tables };
    let q = parse_and_translate("DELETE FROM users RETURNING id;", &resolver)
      .expect("delete returning should translate");

    match q {
      EngineQuery::Delete {
        table,
        predicate,
        returning,
      } => {
        assert_eq!(table, "users");
        assert!(predicate.is_none());
        let projection = returning.expect("returning projection");
        assert_eq!(
          projection,
          vec![UpdateValueExpr::Column(QualifiedColumn {
            table: "users".into(),
            column_index: 0,
          })]
        );
      }
      other => panic!("unexpected query kind: {:?}", other),
    }
  }

  #[test]
  fn translate_update_returning_projection() {
    let mut tables = HashMap::new();
    tables.insert(
      "users".into(),
      TableSchema {
        name: "users".into(),
        columns: vec![
          ColumnSchema {
            name: "id".into(),
            data_type: EngineType::Integer,
          },
          ColumnSchema {
            name: "score".into(),
            data_type: EngineType::Integer,
          },
        ],
        primary_key: vec![0],
      },
    );

    let resolver = DummyResolver { tables };
    let q = parse_and_translate(
      "UPDATE users SET score = score + 1 RETURNING id, score;",
      &resolver,
    )
    .expect("update returning should translate");

    match q {
      EngineQuery::Update {
        returning,
        joins,
        from_tables,
        ..
      } => {
        assert!(joins.is_empty());
        assert!(from_tables.is_empty());
        let projection = returning.expect("returning projection");
        assert_eq!(
          projection,
          vec![
            UpdateValueExpr::Column(QualifiedColumn {
              table: "users".into(),
              column_index: 0,
            }),
            UpdateValueExpr::Column(QualifiedColumn {
              table: "users".into(),
              column_index: 1,
            }),
          ]
        );
      }
      other => panic!("unexpected query kind: {:?}", other),
    }
  }
}
