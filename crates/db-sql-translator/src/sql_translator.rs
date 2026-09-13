#[cfg(not(feature = "std"))]
use alloc::{
    boxed::Box,
    collections::BTreeMap,
    format,
    string::{String, ToString},
    vec,
    vec::Vec,
};
#[cfg(feature = "std")]
use std::collections::BTreeMap;

use db_query::{
    AlterIndexOperation, AlterTableOperation, DataDefinition, Query, QueryColumn, QueryDelete,
    QueryExpr, QueryExprValue, QueryFrom, QueryInsertValue, QueryInsertValues, QueryJoin,
    QueryJoinKind, QueryParams, QuerySelect, QueryUpdate, Statement, TranslateError,
    TranslateResult, Translator,
};

use db_schema::TableSchema;
use db_value::{Value, ValueType};
use sqlparser::{
    ast::{
        self, Expr, JoinConstraint, JoinOperator, ObjectName, ObjectNamePart, SelectItem,
        TableFactor, TableObject, TableWithJoins,
    },
    dialect::PostgreSqlDialect,
    parser::Parser,
};
use uuid::Uuid;

#[derive(Debug, Clone, Copy)]
pub struct SqlTranslator;

impl Translator for SqlTranslator {
    async fn translate_with_params(
        &self,
        query: &str,
        params: Option<&QueryParams>,
    ) -> TranslateResult<Vec<Statement>> {
        let stmts =
            Parser::parse_sql(&PostgreSqlDialect {}, query).map_err(TranslateError::custom)?;

        let mut results = Vec::with_capacity(stmts.len());
        for stmt in stmts {
            results.push(translate_stmt(stmt, params)?);
        }
        Ok(results)
    }
}

fn table_name_to_string(name: &TableObject) -> TranslateResult<String> {
    match name {
        TableObject::TableName(object_name) => {
            if object_name.0.len() == 1 {
                object_name_part_to_string(&object_name.0[0])
            } else {
                Err(TranslateError::custom(format!(
                    "Unsupported table name with {} parts: {:?}",
                    object_name.0.len(),
                    object_name
                )))
            }
        }
        _ => Err(TranslateError::custom(format!(
            "Unsupported table object: {:?}",
            name
        ))),
    }
}

fn object_name_to_string(name: &ObjectName) -> TranslateResult<String> {
    if name.0.len() == 1 {
        object_name_part_to_string(&name.0[0])
    } else {
        Err(TranslateError::custom(format!(
            "Unsupported object name with {} parts: {:?}",
            name.0.len(),
            name
        )))
    }
}

fn object_name_part_to_string(part: &ObjectNamePart) -> TranslateResult<String> {
    match part {
        ObjectNamePart::Identifier(ident) => Ok(ident.value.clone()),
        ObjectNamePart::Function(_) => Err(TranslateError::custom(
            "Function names not supported in object names",
        )),
    }
}

fn translate_stmt(
    stmt: ast::Statement,
    params: Option<&QueryParams>,
) -> TranslateResult<Statement> {
    match stmt {
        ast::Statement::Query(query) => translate_query(*query, params),
        ast::Statement::Insert(insert) => translate_insert(insert, params),
        ast::Statement::Update(update) => translate_update(update, params),
        ast::Statement::Delete(delete) => translate_delete(delete, params),
        ast::Statement::CreateTable(create_table) => translate_create_table(create_table),
        ast::Statement::Drop {
            object_type,
            if_exists,
            names,
            ..
        } => translate_drop(object_type, if_exists, names),
        ast::Statement::AlterTable(alter_table) => translate_alter_table(alter_table),
        ast::Statement::CreateIndex(create_index) => translate_create_index(create_index),
        ast::Statement::AlterIndex { name, operation } => translate_alter_index(name, operation),
        _ => Err(TranslateError::custom(format!(
            "Unsupported SQL statement: {:?}",
            stmt
        ))),
    }
}

fn translate_query(query: ast::Query, _params: Option<&QueryParams>) -> TranslateResult<Statement> {
    if query.with.is_some() {
        return Err(TranslateError::custom("CTEs not yet supported"));
    }
    if !matches!(*query.body, ast::SetExpr::Select(_)) {
        return Err(TranslateError::custom(
            "Only plain SELECT queries supported",
        ));
    }

    let select = match *query.body {
        ast::SetExpr::Select(select) => select,
        _ => unreachable!(),
    };

    let (from, aliases) = translate_from(&select.from)?;
    let projection = translate_projection(&aliases, &select.projection)?;
    let predicate = select
        .selection
        .map(|e| translate_expr(&aliases, e))
        .transpose()?;
    let having = select
        .having
        .map(|e| translate_expr(&aliases, e))
        .transpose()?;

    // TODO: Expand these as needed
    let aggregates = vec![];
    let group_by = vec![];
    let order_by = vec![];

    let limit = None; // TODO: implement properly
    let offset = None; // TODO: implement properly

    Ok(Statement::Query(Query::Select(QuerySelect {
        from,
        projection,
        predicate,
        aggregates,
        group_by,
        order_by,
        limit,
        offset,
        having,
    })))
}

fn translate_from(
    from: &[TableWithJoins],
) -> TranslateResult<(QueryFrom, BTreeMap<String, String>)> {
    let twj = from
        .first()
        .ok_or(TranslateError::custom("Missing FROM clause"))?;
    let (table, alias) = translate_table_factor(
        &twj.relation,
        "Only simple table references supported in FROM",
    )?;
    let mut aliases = BTreeMap::new();
    insert_alias(&mut aliases, alias, &table);

    for join in &twj.joins {
        let (table, alias) =
            translate_table_factor(&join.relation, "Complex table in JOIN not supported")?;
        insert_alias(&mut aliases, alias, &table);
    }

    let joins = twj
        .joins
        .iter()
        .map(|join| translate_join(&aliases, join))
        .collect::<TranslateResult<Vec<_>>>()?;

    Ok((QueryFrom { table, joins }, aliases))
}

fn translate_table_factor(
    factor: &TableFactor,
    error: &'static str,
) -> TranslateResult<(String, Option<String>)> {
    match factor {
        TableFactor::Table { name, alias, .. } => Ok((
            object_name_to_string(name)?,
            alias.as_ref().map(|alias| alias.name.value.clone()),
        )),
        _ => Err(TranslateError::custom(error)),
    }
}

fn insert_alias(aliases: &mut BTreeMap<String, String>, alias: Option<String>, table: &str) {
    if let Some(alias) = alias {
        aliases.insert(alias, table.into());
    }
}

fn translate_join(
    aliases: &BTreeMap<String, String>,
    join: &ast::Join,
) -> TranslateResult<QueryJoin> {
    let (table, _) = translate_table_factor(&join.relation, "Complex table in JOIN not supported")?;
    let (kind, constraint) = match &join.join_operator {
        JoinOperator::Inner(constraint) => (QueryJoinKind::Inner, constraint),
        JoinOperator::Left(constraint) => (QueryJoinKind::Left, constraint),
        JoinOperator::Right(constraint) => (QueryJoinKind::Right, constraint),
        JoinOperator::FullOuter(constraint) => (QueryJoinKind::Full, constraint),
        _ => return Err(TranslateError::custom("Unsupported JOIN type")),
    };

    Ok(QueryJoin {
        kind,
        table,
        on: parse_join_constraint(aliases, constraint)?,
    })
}

fn parse_join_constraint(
    aliases: &BTreeMap<String, String>,
    constraint: &JoinConstraint,
) -> TranslateResult<QueryExpr> {
    match constraint {
        JoinConstraint::On(expr) => translate_expr(aliases, expr.clone()),
        JoinConstraint::Using(_) => Err(TranslateError::custom("USING joins not yet supported")),
        JoinConstraint::Natural => Err(TranslateError::custom("NATURAL joins not yet supported")),
        JoinConstraint::None => Err(TranslateError::custom("JOIN without ON condition")),
    }
}

fn translate_projection(
    aliases: &BTreeMap<String, String>,
    projection: &[SelectItem],
) -> TranslateResult<Vec<QueryColumn>> {
    projection
        .iter()
        .map(|item| match item {
            SelectItem::UnnamedExpr(expr) | SelectItem::ExprWithAlias { expr, .. } => {
                Ok(translate_projection_expr(aliases, expr))
            }
            SelectItem::Wildcard(_) => Ok(QueryColumn::new("".to_string(), "*".to_string())),
            _ => Err(TranslateError::custom("Unsupported projection item")),
        })
        .collect()
}

fn translate_projection_expr(aliases: &BTreeMap<String, String>, expr: &Expr) -> QueryColumn {
    match expr {
        Expr::Identifier(ident) => QueryColumn::new("".to_string(), ident.value.clone()),
        Expr::CompoundIdentifier(idents) if idents.len() == 2 => QueryColumn::new(
            aliases
                .get(&idents[0].value)
                .cloned()
                .unwrap_or_else(|| idents[0].value.clone()),
            idents[1].value.clone(),
        ),
        Expr::CompoundIdentifier(_) => QueryColumn::new("".to_string(), format!("{:?}", expr)),
        _ => QueryColumn::new("".to_string(), format!("expr:{:?}", expr)),
    }
}

fn translate_expr(aliases: &BTreeMap<String, String>, expr: Expr) -> TranslateResult<QueryExpr> {
    match expr {
        Expr::BinaryOp { left, op, right } => translate_binary_expr(aliases, *left, op, *right),
        Expr::Identifier(ident) => Ok(column_expr("".into(), ident.value)),
        Expr::CompoundIdentifier(idents) if idents.len() == 2 => Ok(column_expr(
            aliases
                .get(&idents[0].value)
                .cloned()
                .unwrap_or_else(|| idents[0].value.clone()),
            idents[1].value.clone(),
        )),
        Expr::Nested(inner) => translate_expr(aliases, *inner),
        Expr::IsNull(inner) => Ok(QueryExpr::IsNull(Box::new(translate_expr(
            aliases, *inner,
        )?))),
        Expr::IsNotNull(inner) => Ok(QueryExpr::IsNotNull(Box::new(translate_expr(
            aliases, *inner,
        )?))),
        Expr::Value(ast::ValueWithSpan { value, .. }) => Ok(QueryExpr::Value(
            QueryExprValue::Value(translate_value(value)?),
        )),
        Expr::Cast {
            kind: ast::CastKind::Cast,
            expr,
            data_type: ast::DataType::Uuid,
            array: false,
            format: None,
        } => {
            let QueryExpr::Value(QueryExprValue::Value(Value::Text(value))) =
                translate_expr(aliases, *expr)?
            else {
                return Err(TranslateError::custom("UUID casts require text values"));
            };
            let uuid = Uuid::parse_str(&value)
                .map_err(|_| TranslateError::custom("Invalid UUID literal"))?;
            Ok(QueryExpr::Value(QueryExprValue::Value(Value::Uuid(uuid))))
        }
        Expr::Cast { .. } => Err(TranslateError::custom("Unsupported cast")),
        _ => Err(TranslateError::custom(format!(
            "Unsupported expression: {:?}",
            expr
        ))),
    }
}

fn column_expr(table: String, column: String) -> QueryExpr {
    QueryExpr::Value(QueryExprValue::Column(QueryColumn::new(table, column)))
}

fn translate_binary_expr(
    aliases: &BTreeMap<String, String>,
    left: Expr,
    op: ast::BinaryOperator,
    right: Expr,
) -> TranslateResult<QueryExpr> {
    let left = Box::new(translate_expr(aliases, left)?);
    let right = Box::new(translate_expr(aliases, right)?);

    match op {
        ast::BinaryOperator::Eq => Ok(QueryExpr::Equals(left, right)),
        ast::BinaryOperator::NotEq => Ok(QueryExpr::NotEquals(left, right)),
        ast::BinaryOperator::Lt => Ok(QueryExpr::LessThan(left, right)),
        ast::BinaryOperator::LtEq => Ok(QueryExpr::LessThanOrEquals(left, right)),
        ast::BinaryOperator::Gt => Ok(QueryExpr::GreaterThan(left, right)),
        ast::BinaryOperator::GtEq => Ok(QueryExpr::GreaterThanOrEquals(left, right)),
        ast::BinaryOperator::And => Ok(QueryExpr::And(left, right)),
        ast::BinaryOperator::Or => Ok(QueryExpr::Or(left, right)),
        _ => Err(TranslateError::custom(format!(
            "Unsupported binary operator: {:?}",
            op
        ))),
    }
}

fn translate_value(value: ast::Value) -> TranslateResult<Value> {
    match value {
        ast::Value::Number(number, _) if number.contains('.') => number
            .parse()
            .map(Value::Float)
            .map_err(|_| TranslateError::custom(format!("Invalid float literal: {}", number))),
        ast::Value::Number(number, _) => number
            .parse()
            .map(Value::Integer)
            .map_err(|_| TranslateError::custom(format!("Invalid integer literal: {}", number))),
        ast::Value::SingleQuotedString(value) => Ok(Value::Text(value)),
        ast::Value::Boolean(value) => Ok(Value::Bool(value)),
        ast::Value::Null => Ok(Value::Null),
        value => Ok(Value::Text(format!("{:?}", value))),
    }
}

fn translate_insert(
    insert: ast::Insert,
    _params: Option<&QueryParams>,
) -> TranslateResult<Statement> {
    Ok(Statement::Query(Query::InsertValues(QueryInsertValues {
        table: table_name_to_string(&insert.table)?,
        columns: insert
            .columns
            .into_iter()
            .map(|column| object_name_to_string(&column))
            .collect::<TranslateResult<Vec<_>>>()?,
        values: translate_insert_values(insert.source)?,
        returning: insert.returning.map(translate_returning).transpose()?,
    })))
}

fn translate_insert_values(
    source: Option<Box<ast::Query>>,
) -> TranslateResult<Vec<QueryInsertValue>> {
    let Some(source) = source else {
        return Ok(vec![]);
    };
    let ast::SetExpr::Values(mut values) = *source.body else {
        return Ok(vec![]);
    };
    let Some(ast::Parens { content, .. }) = values.rows.get_mut(0) else {
        return Err(TranslateError::custom("INSERT with no VALUES"));
    };

    content
        .drain(..)
        .map(|expr| match translate_expr(&BTreeMap::new(), expr)? {
            QueryExpr::Value(QueryExprValue::Value(value)) => Ok(QueryInsertValue::Value(value)),
            QueryExpr::Value(QueryExprValue::Column(column)) if column.column == "DEFAULT" => {
                Ok(QueryInsertValue::Default)
            }
            _ => Err(TranslateError::custom("Unsupported expression in VALUES")),
        })
        .collect()
}

fn translate_returning(items: Vec<SelectItem>) -> TranslateResult<Vec<String>> {
    items.into_iter().map(translate_returning_item).collect()
}

fn translate_returning_item(item: SelectItem) -> TranslateResult<String> {
    match item {
        SelectItem::UnnamedExpr(Expr::Identifier(ident)) => Ok(ident.value),
        SelectItem::UnnamedExpr(Expr::CompoundIdentifier(idents)) if idents.len() == 1 => {
            Ok(idents[0].value.clone())
        }
        SelectItem::UnnamedExpr(Expr::CompoundIdentifier(idents)) => Err(TranslateError::custom(
            format!("Unsupported RETURNING identifier: {:?}", idents),
        )),
        SelectItem::UnnamedExpr(expr) => Err(TranslateError::custom(format!(
            "Unsupported RETURNING expression: {:?}",
            expr
        ))),
        SelectItem::Wildcard(_) => Ok("*".to_string()),
        _ => Err(TranslateError::custom("Unsupported RETURNING item")),
    }
}

fn translate_update(
    update: ast::Update,
    _params: Option<&QueryParams>,
) -> TranslateResult<Statement> {
    let table = match &update.table.relation {
        TableFactor::Table { name, .. } => object_name_to_string(name)?,
        _ => {
            return Err(TranslateError::custom(
                "Only simple table references supported in UPDATE",
            ));
        }
    };

    let predicate = update
        .selection
        .map(|e| translate_expr(&BTreeMap::new(), e))
        .transpose()?;

    // TODO: Implement proper assignment parsing from update.assignments
    let assignments = vec![];

    Ok(Statement::Query(Query::Update(QueryUpdate {
        from: QueryFrom {
            table,
            joins: vec![], // TODO: handle joins in UPDATE
        },
        assignments,
        predicate,
        returning: None, // TODO: handle RETURNING clause in UPDATE
    })))
}

fn translate_delete(
    delete: ast::Delete,
    _params: Option<&QueryParams>,
) -> TranslateResult<Statement> {
    if delete.tables.is_empty() {
        return Err(TranslateError::custom("No tables in DELETE"));
    }
    if delete.tables.len() > 1 {
        return Err(TranslateError::custom(
            "Multiple tables in DELETE not supported",
        ));
    }
    let table = object_name_to_string(&delete.tables[0])?;

    let predicate = delete
        .selection
        .map(|e| translate_expr(&BTreeMap::new(), e))
        .transpose()?;

    Ok(Statement::Query(Query::Delete(QueryDelete {
        from: QueryFrom {
            table,
            joins: vec![], // TODO: handle joins in DELETE
        },
        predicate,
        returning: None, // TODO: handle RETURNING clause in DELETE
    })))
}

fn translate_column_data_type(data_type: &ast::DataType) -> TranslateResult<ValueType> {
    match data_type {
        ast::DataType::Char(_) | ast::DataType::Varchar(_) | ast::DataType::Text => {
            Ok(ValueType::Text)
        }
        ast::DataType::Int(_) | ast::DataType::Integer(_) | ast::DataType::BigInt(_) => {
            Ok(ValueType::Integer)
        }
        ast::DataType::Float(_) | ast::DataType::Double(_) => Ok(ValueType::Float),
        ast::DataType::Boolean => Ok(ValueType::Bool),
        ast::DataType::Blob(_) => Ok(ValueType::Blob),
        ast::DataType::Uuid => Ok(ValueType::Uuid),
        ast::DataType::JSON | ast::DataType::JSONB => Ok(ValueType::Json),
        _ => Err(TranslateError::custom(format!(
            "Unsupported column data type: {:?}",
            data_type
        ))),
    }
}

fn translate_column_schema(column: &ast::ColumnDef) -> TranslateResult<db_schema::ColumnSchema> {
    let default = column
        .options
        .iter()
        .find_map(|option| match &option.option {
            ast::ColumnOption::Default(expr) => Some(expr.clone()),
            _ => None,
        })
        .map(|expr| match translate_expr(&BTreeMap::new(), expr)? {
            QueryExpr::Value(QueryExprValue::Value(value)) => Ok(value),
            _ => Err(TranslateError::custom(
                "Column default must be a literal value",
            )),
        })
        .transpose()?
        .unwrap_or_default();

    Ok(db_schema::ColumnSchema {
        name: column.name.value.clone(),
        r#type: translate_column_data_type(&column.data_type)?,
        default,
        primary_key: column
            .options
            .iter()
            .any(|option| matches!(option.option, ast::ColumnOption::PrimaryKey(_))),
    })
}

fn unique_index_name(table: &str, columns: &[String], ordinal: usize) -> String {
    format!("{}_unique_{}_{}", table, ordinal, columns.join("_"))
}

fn unique_columns(columns: &[ast::IndexColumn]) -> TranslateResult<Vec<String>> {
    columns
        .iter()
        .map(|column| match &column.column.expr {
            Expr::Identifier(identifier)
                if column.operator_class.is_none()
                    && column.column.options.asc.is_none()
                    && column.column.options.nulls_first.is_none()
                    && column.column.with_fill.is_none() =>
            {
                Ok(identifier.value.clone())
            }
            _ => Err(TranslateError::custom(
                "UNIQUE constraints only support bare identifier columns",
            )),
        })
        .collect()
}

fn translate_create_table(create_table: ast::CreateTable) -> TranslateResult<Statement> {
    let table_name = object_name_to_string(&create_table.name)?;

    let columns = create_table
        .columns
        .iter()
        .map(translate_column_schema)
        .collect::<TranslateResult<Vec<_>>>()?;

    let schema = TableSchema {
        name: table_name,
        columns,
    };

    let mut indexes = Vec::new();
    for (ordinal, column) in create_table.columns.iter().enumerate() {
        if column
            .options
            .iter()
            .any(|option| matches!(option.option, ast::ColumnOption::Unique { .. }))
        {
            let columns = vec![column.name.value.clone()];
            let name = column.options.iter().find_map(|option| {
                let ast::ColumnOption::Unique(unique) = &option.option else {
                    return None;
                };
                unique
                    .name
                    .as_ref()
                    .or(unique.index_name.as_ref())
                    .map(|name| name.value.clone())
            });
            indexes.push(db_schema::IndexSchema {
                name: name.unwrap_or_else(|| unique_index_name(&schema.name, &columns, ordinal)),
                table_name: schema.name.clone(),
                column_indices: vec![ordinal as u32],
                unique: true,
            });
        }
    }
    for (ordinal, constraint) in create_table.constraints.iter().enumerate() {
        if let ast::TableConstraint::Unique(unique) = constraint {
            let names = unique_columns(&unique.columns)?;
            indexes.push(db_schema::IndexSchema {
                name: unique
                    .name
                    .as_ref()
                    .or(unique.index_name.as_ref())
                    .map(|name| name.value.clone())
                    .unwrap_or_else(|| unique_index_name(&schema.name, &names, ordinal)),
                table_name: schema.name.clone(),
                column_indices: names
                    .iter()
                    .map(|name| {
                        schema
                            .columns
                            .iter()
                            .position(|column| column.name == *name)
                            .map(|index| index as u32)
                            .ok_or(TranslateError::custom("UNIQUE column not found"))
                    })
                    .collect::<TranslateResult<Vec<_>>>()?,
                unique: true,
            });
        }
    }

    if indexes.is_empty() {
        Ok(Statement::DataDefinition(DataDefinition::CreateTable {
            schema,
            if_not_exists: create_table.if_not_exists,
        }))
    } else {
        Ok(Statement::DataDefinition(
            DataDefinition::CreateTableWithIndexes {
                schema,
                indexes,
                if_not_exists: create_table.if_not_exists,
            },
        ))
    }
}

fn translate_drop(
    object_type: ast::ObjectType,
    if_exists: bool,
    names: Vec<ast::ObjectName>,
) -> TranslateResult<Statement> {
    if names.is_empty() {
        return Err(TranslateError::custom("No object names in DROP"));
    }
    let name = object_name_to_string(&names[0])?;

    match object_type {
        ast::ObjectType::Table => Err(TranslateError::custom("DROP TABLE is not supported")),
        ast::ObjectType::Index => Ok(Statement::DataDefinition(DataDefinition::DropIndex {
            index_name: name,
            if_exists,
        })),
        _ => Err(TranslateError::custom(format!(
            "DROP {:?} not supported",
            object_type
        ))),
    }
}

fn translate_alter_table(alter_table: ast::AlterTable) -> TranslateResult<Statement> {
    let table_name = object_name_to_string(&alter_table.name)?;
    let operations = alter_table
        .operations
        .iter()
        .map(|operation| match operation {
            ast::AlterTableOperation::AddColumn { column_def, .. } => Ok(
                AlterTableOperation::AddColumn(translate_column_schema(column_def)?),
            ),
            _ => Err(TranslateError::custom(
                "Only ALTER TABLE ADD COLUMN is supported",
            )),
        })
        .collect::<TranslateResult<Vec<_>>>()?;

    Ok(Statement::DataDefinition(DataDefinition::AlterTable {
        table_name,
        operations,
        if_exists: alter_table.if_exists,
    }))
}

fn translate_create_index(create_index: ast::CreateIndex) -> TranslateResult<Statement> {
    let index_name = create_index
        .name
        .as_ref()
        .ok_or(TranslateError::custom(
            "CREATE INDEX requires an index name",
        ))
        .and_then(object_name_to_string)?;
    let table_name = object_name_to_string(&create_index.table_name)?;
    let column_names = create_index
        .columns
        .iter()
        .map(|column| match &column.column.expr {
            Expr::Identifier(identifier)
                if column.operator_class.is_none()
                    && column.column.options.asc.is_none()
                    && column.column.options.nulls_first.is_none()
                    && column.column.with_fill.is_none() =>
            {
                Ok(identifier.value.clone())
            }
            _ => Err(TranslateError::custom(
                "CREATE INDEX only supports bare identifier columns",
            )),
        })
        .collect::<TranslateResult<Vec<_>>>()?;

    Ok(Statement::DataDefinition(
        DataDefinition::CreateIndexUnresolved {
            index_name,
            table_name,
            column_names,
            unique: create_index.unique,
            if_not_exists: create_index.if_not_exists,
        },
    ))
}

fn translate_alter_index(
    name: ast::ObjectName,
    operation: ast::AlterIndexOperation,
) -> TranslateResult<Statement> {
    let index_name = object_name_to_string(&name)?;

    let op = match operation {
        ast::AlterIndexOperation::RenameIndex { index_name } => AlterIndexOperation::Rename {
            new_name: object_name_to_string(&index_name)?,
        },
    };

    Ok(Statement::DataDefinition(DataDefinition::AlterIndex {
        index_name,
        operation: op,
        if_exists: false,
    }))
}

#[cfg(test)]
mod tests {
    use futures::executor::block_on;

    use super::*;

    #[test]
    fn translates_alter_table_add_column_with_default() {
        block_on(async {
            let statements = SqlTranslator
                .translate_with_params(
                    "ALTER TABLE users ADD COLUMN role TEXT DEFAULT 'member'",
                    None,
                )
                .await
                .unwrap();
            assert_eq!(
                statements,
                vec![Statement::DataDefinition(DataDefinition::AlterTable {
                    table_name: "users".into(),
                    operations: vec![AlterTableOperation::AddColumn(db_schema::ColumnSchema {
                        name: "role".into(),
                        r#type: ValueType::Text,
                        default: Value::from("member"),
                        primary_key: false,
                    })],
                    if_exists: false,
                })]
            );
        });
    }

    #[test]
    fn translates_create_index() {
        block_on(async {
            let statements = SqlTranslator
                .translate_with_params(
                    "CREATE UNIQUE INDEX IF NOT EXISTS users_email ON users (email)",
                    None,
                )
                .await
                .unwrap();
            assert_eq!(
                statements,
                vec![Statement::DataDefinition(
                    DataDefinition::CreateIndexUnresolved {
                        index_name: "users_email".into(),
                        table_name: "users".into(),
                        column_names: vec!["email".into()],
                        unique: true,
                        if_not_exists: true,
                    }
                )]
            );
        });
    }

    #[test]
    fn rejects_create_index_expressions() {
        block_on(async {
            let error = SqlTranslator
                .translate_with_params("CREATE INDEX users_email ON users (lower(email))", None)
                .await
                .unwrap_err();
            assert!(error.to_string().contains("bare identifier columns"));
        });
    }

    fn translate(sql: &str) -> Vec<Statement> {
        block_on(async {
            SqlTranslator
                .translate_with_params(sql, None)
                .await
                .unwrap()
        })
    }

    #[test]
    fn translates_remaining_statement_types() {
        assert!(matches!(
            translate("INSERT INTO users VALUES (1, 'Ada', TRUE, NULL) RETURNING id, *")[0],
            Statement::Query(Query::InsertValues(_))
        ));
        assert!(matches!(
            translate("UPDATE users SET name = 'Ada' WHERE id <> 1")[0],
            Statement::Query(Query::Update(_))
        ));
        let error = block_on(async {
            SqlTranslator
                .translate_with_params("DELETE FROM users WHERE id <= 1", None)
                .await
                .unwrap_err()
        });
        assert!(error.to_string().contains("No tables in DELETE"));
        assert!(matches!(
            translate("DROP INDEX IF EXISTS users_name")[0],
            Statement::DataDefinition(DataDefinition::DropIndex { .. })
        ));
        assert!(matches!(
            translate("ALTER INDEX users_name RENAME TO members_name")[0],
            Statement::DataDefinition(DataDefinition::AlterIndex { .. })
        ));
    }

    #[test]
    fn translates_uuid_casts() {
        let uuid = Uuid::parse_str("018f0f8e-7b6d-7c4a-8f12-123456789abc").unwrap();
        let statements = translate(
            "SELECT id FROM users WHERE id = CAST('018f0f8e-7b6d-7c4a-8f12-123456789abc' AS UUID)",
        );
        let Statement::Query(Query::Select(select)) = &statements[0] else {
            panic!("expected SELECT");
        };
        let Some(QueryExpr::Equals(_, right)) = &select.predicate else {
            panic!("expected equality predicate");
        };
        assert_eq!(
            right.as_ref(),
            &QueryExpr::Value(QueryExprValue::Value(Value::Uuid(uuid)))
        );
    }

    #[test]
    fn translates_insert_columns_and_defaults() {
        let statements =
            translate("INSERT INTO users (name, id) VALUES ('Ada', DEFAULT) RETURNING id, name");
        let Statement::Query(Query::InsertValues(insert)) = &statements[0] else {
            panic!("expected INSERT VALUES");
        };
        assert_eq!(insert.columns, vec!["name", "id"]);
        assert_eq!(
            insert.values,
            vec![
                QueryInsertValue::Value(Value::from("Ada")),
                QueryInsertValue::Default,
            ]
        );
    }

    #[test]
    fn translates_unique_constraints_to_named_indexes() {
        let statements = translate(
            "CREATE TABLE users (id UUID PRIMARY KEY, email TEXT UNIQUE, UNIQUE (id, email))",
        );
        let Statement::DataDefinition(DataDefinition::CreateTableWithIndexes {
            schema,
            indexes,
            ..
        }) = &statements[0]
        else {
            panic!("expected table with indexes");
        };
        assert_eq!(schema.columns.len(), 2);
        assert_eq!(indexes.len(), 2);
        assert_eq!(indexes[0].column_indices, vec![1]);
        assert_eq!(indexes[1].column_indices, vec![0, 1]);
        assert!(indexes.iter().all(|index| index.unique));
    }

    #[test]
    fn rejects_invalid_uuid_casts() {
        let error = block_on(async {
            SqlTranslator
                .translate_with_params(
                    "SELECT id FROM users WHERE id = CAST('not-a-uuid' AS UUID)",
                    None,
                )
                .await
                .unwrap_err()
        });
        assert!(error.to_string().contains("Invalid UUID literal"));
    }

    #[test]
    fn translates_data_types_and_reports_unsupported_types() {
        assert!(matches!(
            translate(
                "CREATE TABLE values (\
                    a CHAR, b VARCHAR, c TEXT, d INT, e INTEGER, f BIGINT, \
                    g FLOAT, h DOUBLE, i BOOLEAN, j BLOB, k UUID, l JSON, m JSONB\
                )"
            )[0],
            Statement::DataDefinition(DataDefinition::CreateTable { .. })
        ));

        let error = block_on(async {
            SqlTranslator
                .translate_with_params("CREATE TABLE values (created DATE)", None)
                .await
                .unwrap_err()
        });
        assert!(error.to_string().contains("Unsupported column data type"));
    }

    #[test]
    fn translates_join_kinds_and_expression_variants() {
        for sql in [
            "SELECT * FROM users u LEFT JOIN posts p ON p.user_id = u.id",
            "SELECT * FROM users u RIGHT JOIN posts p ON p.user_id = u.id",
            "SELECT * FROM users u FULL OUTER JOIN posts p ON p.user_id = u.id",
            "SELECT id, u.id, u.id + 1 FROM users u WHERE id < 1 OR id <= 2 OR id <> 3 OR id >= 4 AND active = TRUE AND name = 'Ada' AND deleted_at IS NOT NULL",
        ] {
            assert!(matches!(
                translate(sql)[0],
                Statement::Query(Query::Select(_))
            ));
        }
    }

    #[test]
    fn test_simple_select() {
        block_on(async {
            let translator = SqlTranslator;
            let sql = "SELECT u.id as user_id, u.name as username, p.created_at FROM users as u INNER JOIN posts as p ON p.user_id = u.id WHERE u.age > 30 AND p.created_at IS NULL";
            let result = translator
                .translate_with_params(sql, None)
                .await
                .expect("Failed to translate SQL");
            assert_eq!(result.len(), 1);

            if let Statement::Query(Query::Select(select)) = &result[0] {
                assert_eq!(select.from.table, "users");

                assert_eq!(select.from.joins.len(), 1);
                let join = &select.from.joins[0];
                assert_eq!(join.kind, QueryJoinKind::Inner);
                assert_eq!(join.table, "posts");
                if let QueryExpr::Equals(left, right) = &join.on {
                    match (left.as_ref(), right.as_ref()) {
                        (
                            QueryExpr::Value(QueryExprValue::Column(left_col)),
                            QueryExpr::Value(QueryExprValue::Column(right_col)),
                        ) => {
                            assert_eq!(left_col.table, "posts");
                            assert_eq!(left_col.column, "user_id");
                            assert_eq!(right_col.table, "users");
                            assert_eq!(right_col.column, "id");
                        }
                        _ => panic!("Expected column equality in JOIN condition"),
                    }
                } else {
                    panic!("Expected equality in JOIN condition");
                }

                assert_eq!(select.projection.len(), 3);
                let user_id_col = &select.projection[0];
                assert_eq!(user_id_col.table, "users");
                assert_eq!(user_id_col.column, "id");
                let username_col = &select.projection[1];
                assert_eq!(username_col.table, "users");
                assert_eq!(username_col.column, "name");
                let created_at_col = &select.projection[2];
                assert_eq!(created_at_col.table, "posts");
                assert_eq!(created_at_col.column, "created_at");

                if let Some(QueryExpr::And(lhs, rhs)) = &select.predicate {
                    match lhs.as_ref() {
                        QueryExpr::GreaterThan(left, right) => {
                            match (left.as_ref(), right.as_ref()) {
                                (
                                    QueryExpr::Value(QueryExprValue::Column(col)),
                                    QueryExpr::Value(QueryExprValue::Value(val)),
                                ) => {
                                    assert_eq!(col.table, "users");
                                    assert_eq!(col.column, "age");
                                    assert_eq!(val, &Value::Integer(30));
                                }
                                _ => panic!("Expected column and value in age > 30 predicate"),
                            }
                        }
                        _ => panic!("Expected age > 30 in predicate"),
                    }
                    match rhs.as_ref() {
                        QueryExpr::IsNull(inner) => match inner.as_ref() {
                            QueryExpr::Value(QueryExprValue::Column(col)) => {
                                assert_eq!(col.table, "posts");
                                assert_eq!(col.column, "created_at");
                            }
                            _ => panic!("Expected column in p.created_at IS NULL predicate"),
                        },
                        _ => panic!("Expected p.created_at IS NULL in predicate"),
                    }
                } else {
                    panic!(
                        "Expected SELECT statement with predicate: {:?}",
                        select.predicate
                    );
                }
            } else {
                panic!("Expected SELECT statement: {:?}", result[0]);
            }
        });
    }
}
