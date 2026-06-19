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
    AlterIndexOperation, DataDefinition, Query, QueryColumn, QueryDelete, QueryExpr,
    QueryExprValue, QueryFrom, QueryInsert, QueryJoin, QueryJoinKind, QueryParams, QuerySelect,
    QueryUpdate, Statement, TranslateError, TranslateResult, Translator,
};

use db_schema::{IndexSchema, TableSchema};
use db_value::{Row, Value, ValueType};
use sqlparser::{
    ast::{
        self, Expr, JoinConstraint, JoinOperator, ObjectName, ObjectNamePart, SelectItem,
        TableFactor, TableObject, TableWithJoins,
    },
    dialect::PostgreSqlDialect,
    parser::Parser,
};

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
    if from.is_empty() {
        return Err(TranslateError::custom("Missing FROM clause"));
    }

    let mut aliases = BTreeMap::new();

    let twj = &from[0];
    let table = match &twj.relation {
        TableFactor::Table { name, alias, .. } => {
            let table_name = object_name_to_string(name)?;
            let alias_name = alias.as_ref().map(|a| a.name.value.clone());
            if let Some(alias) = alias_name {
                aliases.insert(alias.clone(), table_name.clone());
            }

            table_name
        }
        _ => {
            return Err(TranslateError::custom(
                "Only simple table references supported in FROM",
            ));
        }
    };

    let mut joins = Vec::with_capacity(twj.joins.len());
    for join in &twj.joins {
        let (table, alias) = match &join.relation {
            TableFactor::Table { name, alias, .. } => (
                object_name_to_string(name)?,
                alias.as_ref().map(|a| a.name.value.clone()),
            ),
            _ => {
                return Err(TranslateError::custom(
                    "Complex table in JOIN not supported",
                ));
            }
        };
        if let Some(alias) = alias {
            aliases.insert(alias, table.clone());
        }
    }
    for join in &twj.joins {
        let (kind, on) = match &join.join_operator {
            JoinOperator::Inner(join_constraint) => (
                QueryJoinKind::Inner,
                parse_join_constraint(&aliases, join_constraint)?,
            ),
            JoinOperator::Left(join_constraint) => (
                QueryJoinKind::Left,
                parse_join_constraint(&aliases, join_constraint)?,
            ),
            JoinOperator::Right(join_constraint) => (
                QueryJoinKind::Right,
                parse_join_constraint(&aliases, join_constraint)?,
            ),
            JoinOperator::FullOuter(join_constraint) => (
                QueryJoinKind::Full,
                parse_join_constraint(&aliases, join_constraint)?,
            ),
            _ => return Err(TranslateError::custom("Unsupported JOIN type")),
        };
        let table = match &join.relation {
            TableFactor::Table { name, .. } => object_name_to_string(name)?,
            _ => {
                return Err(TranslateError::custom(
                    "Complex table in JOIN not supported",
                ));
            }
        };

        joins.push(QueryJoin { kind, table, on });
    }

    Ok((QueryFrom { table, joins }, aliases))
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
    let mut cols = Vec::new();
    for item in projection {
        match item {
            SelectItem::UnnamedExpr(expr) => {
                if let Expr::Identifier(ident) = expr {
                    cols.push(QueryColumn::new("".to_string(), ident.value.clone()));
                } else if let Expr::CompoundIdentifier(idents) = expr {
                    if idents.len() == 2 {
                        let table = if let Some(real_table) = aliases.get(&idents[0].value) {
                            real_table.clone()
                        } else {
                            idents[0].value.clone()
                        };
                        let column = idents[1].value.clone();
                        cols.push(QueryColumn::new(table, column));
                    } else {
                        cols.push(QueryColumn::new("".to_string(), format!("{:?}", expr)));
                    }
                } else {
                    cols.push(QueryColumn::new("".to_string(), format!("expr:{:?}", expr)));
                }
            }
            SelectItem::Wildcard(_) => {
                cols.push(QueryColumn::new("".to_string(), "*".to_string()));
            }
            SelectItem::ExprWithAlias { expr, .. } => {
                if let Expr::Identifier(ident) = expr {
                    cols.push(QueryColumn::new("".to_string(), ident.value.clone()));
                } else if let Expr::CompoundIdentifier(idents) = expr {
                    if idents.len() == 2 {
                        let table = if let Some(real_table) = aliases.get(&idents[0].value) {
                            real_table.clone()
                        } else {
                            idents[0].value.clone()
                        };
                        let column = idents[1].value.clone();
                        cols.push(QueryColumn::new(table, column));
                    } else {
                        cols.push(QueryColumn::new("".to_string(), format!("{:?}", expr)));
                    }
                } else {
                    cols.push(QueryColumn::new("".to_string(), format!("expr:{:?}", expr)));
                }
            }
            _ => return Err(TranslateError::custom("Unsupported projection item")),
        }
    }
    Ok(cols)
}

fn translate_expr(aliases: &BTreeMap<String, String>, expr: Expr) -> TranslateResult<QueryExpr> {
    match expr {
        Expr::BinaryOp { left, op, right } => {
            let left_val = Box::new(translate_expr(aliases, *left)?);
            let right_val = Box::new(translate_expr(aliases, *right)?);

            match op {
                ast::BinaryOperator::Eq => Ok(QueryExpr::Equals(left_val, right_val)),
                ast::BinaryOperator::NotEq => Ok(QueryExpr::NotEquals(left_val, right_val)),
                ast::BinaryOperator::Lt => Ok(QueryExpr::LessThan(left_val, right_val)),
                ast::BinaryOperator::LtEq => Ok(QueryExpr::LessThanOrEquals(left_val, right_val)),
                ast::BinaryOperator::Gt => Ok(QueryExpr::GreaterThan(left_val, right_val)),
                ast::BinaryOperator::GtEq => {
                    Ok(QueryExpr::GreaterThanOrEquals(left_val, right_val))
                }
                ast::BinaryOperator::And => Ok(QueryExpr::And(left_val, right_val)),
                ast::BinaryOperator::Or => Ok(QueryExpr::Or(left_val, right_val)),
                _ => Err(TranslateError::custom(format!(
                    "Unsupported binary operator: {:?}",
                    op
                ))),
            }
        }
        Expr::Identifier(ident) => {
            let col = QueryColumn::new("".into(), ident.value);
            Ok(QueryExpr::Value(QueryExprValue::Column(col)))
        }
        Expr::CompoundIdentifier(idents) if idents.len() == 2 => {
            let table = if let Some(real_table) = aliases.get(&idents[0].value) {
                real_table.clone()
            } else {
                idents[0].value.clone()
            };
            let col = QueryColumn::new(table, idents[1].value.clone());
            Ok(QueryExpr::Value(QueryExprValue::Column(col)))
        }
        Expr::IsNull(inner) => Ok(QueryExpr::IsNull(Box::new(translate_expr(
            aliases, *inner,
        )?))),
        Expr::IsNotNull(inner) => Ok(QueryExpr::IsNotNull(Box::new(translate_expr(
            aliases, *inner,
        )?))),
        Expr::Value(ast::ValueWithSpan { value, .. }) => {
            let db_value = match value {
                ast::Value::Number(n, _) => {
                    if n.contains('.') {
                        Value::Float(n.parse().map_err(|_| {
                            TranslateError::custom(format!("Invalid float literal: {}", n))
                        })?)
                    } else {
                        Value::Integer(n.parse().map_err(|_| {
                            TranslateError::custom(format!("Invalid integer literal: {}", n))
                        })?)
                    }
                }
                ast::Value::SingleQuotedString(s) => Value::Text(s),
                ast::Value::Boolean(b) => Value::Bool(b),
                _ => Value::Text(format!("{:?}", value)),
            };
            Ok(QueryExpr::Value(QueryExprValue::Value(db_value)))
        }
        _ => Err(TranslateError::custom(format!(
            "Unsupported expression: {:?}",
            expr
        ))),
    }
}

fn translate_insert(
    insert: ast::Insert,
    _params: Option<&QueryParams>,
) -> TranslateResult<Statement> {
    let table = table_name_to_string(&insert.table)?;

    let values = if let Some(source) = insert.source {
        if let ast::SetExpr::Values(mut values) = *source.body {
            if values.rows.is_empty() {
                return Err(TranslateError::custom("INSERT with no VALUES"));
            }
            let mut out = Vec::with_capacity(values.rows.len());
            let ast::Parens { content, .. } = values.rows.remove(0);
            for expr in content {
                let value = match translate_expr(&BTreeMap::new(), expr)? {
                    QueryExpr::Value(QueryExprValue::Value(val)) => val,
                    _ => {
                        return Err(TranslateError::custom("Unsupported expression in VALUES"));
                    }
                };
                out.push(value);
            }
            out
        } else {
            vec![]
        }
    } else {
        vec![]
    };
    let row = Row::new(values);

    let returning = if let Some(returning_items) = insert.returning {
        let mut returning = Vec::new();

        for item in returning_items {
            match item {
                SelectItem::UnnamedExpr(expr) => {
                    if let Expr::Identifier(ident) = expr {
                        returning.push(ident.value.clone());
                    } else if let Expr::CompoundIdentifier(idents) = expr {
                        if idents.len() == 1 {
                            let column = idents[0].value.clone();
                            returning.push(column);
                        } else {
                            return Err(TranslateError::custom(format!(
                                "Unsupported RETURNING identifier: {:?}",
                                idents
                            )));
                        }
                    } else {
                        return Err(TranslateError::custom(format!(
                            "Unsupported RETURNING expression: {:?}",
                            expr
                        )));
                    }
                }
                SelectItem::Wildcard(_) => {
                    returning.push("*".to_string());
                }
                _ => return Err(TranslateError::custom("Unsupported RETURNING item")),
            }
        }

        Some(returning)
    } else {
        None
    };

    Ok(Statement::Query(Query::Insert(QueryInsert {
        table,
        row,
        returning,
    })))
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

fn translate_create_table(create_table: ast::CreateTable) -> TranslateResult<Statement> {
    let table_name = object_name_to_string(&create_table.name)?;

    let columns = {
        let mut columns = Vec::with_capacity(create_table.columns.len());
        for col_def in &create_table.columns {
            columns.push(db_schema::ColumnSchema {
                name: col_def.name.value.clone(),
                r#type: translate_column_data_type(&col_def.data_type)?,
                primary_key: col_def
                    .options
                    .iter()
                    .any(|opt| matches!(opt.option, ast::ColumnOption::PrimaryKey(_))),
            });
        }
        columns
    };

    let schema = TableSchema {
        name: table_name,
        columns,
    };

    Ok(Statement::DataDefinition(DataDefinition::CreateTable {
        schema,
        if_not_exists: create_table.if_not_exists,
    }))
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
        ast::ObjectType::Table => Ok(Statement::DataDefinition(DataDefinition::DropTable {
            table_name: name,
            if_exists,
        })),
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

    // TODO: proper mapping of operations
    let operations = vec![];

    Ok(Statement::DataDefinition(DataDefinition::AlterTable {
        table_name,
        operations,
        if_exists: alter_table.if_exists,
    }))
}

fn translate_create_index(create_index: ast::CreateIndex) -> TranslateResult<Statement> {
    let index_name = create_index
        .name
        .map(|n| object_name_to_string(&n))
        .unwrap_or(Ok(String::new()))?;

    let table_name = object_name_to_string(&create_index.table_name)?;

    let column_indices: Vec<u32> = create_index
        .columns
        .into_iter()
        .map(|_| 0u32) // TODO: proper mapping to actual column indices
        .collect();

    let schema = IndexSchema {
        name: index_name,
        table_name,
        column_indices,
        unique: create_index.unique,
    };

    Ok(Statement::DataDefinition(DataDefinition::CreateIndex {
        schema,
        if_not_exists: create_index.if_not_exists,
    }))
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
