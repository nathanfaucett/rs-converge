#[cfg(not(feature = "std"))]
use alloc::{boxed::Box, string::String, vec::Vec};
#[cfg(all(not(feature = "std"), feature = "wasm"))]
use alloc::{format, string::ToString};

use db_schema::{ColumnSchema, IndexSchema, TableSchema};
use db_value::{Row, Value};

#[derive(Debug, Clone, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
#[cfg_attr(
    feature = "wasm",
    derive(tsify::Tsify),
    tsify(into_wasm_abi, from_wasm_abi)
)]
pub struct QueryColumn {
    pub table: String,
    pub column: String,
}

impl QueryColumn {
    pub fn new(table: String, column: String) -> Self {
        Self { table, column }
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
    pub table: String,
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
    pub by: QueryColumn,
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
    Value(QueryExprValue),
    Not(Box<QueryExpr>),
    Exists(Box<QueryExpr>),
    IsNull(Box<QueryExpr>),
    IsNotNull(Box<QueryExpr>),
    Equals(Box<QueryExpr>, Box<QueryExpr>),
    NotEquals(Box<QueryExpr>, Box<QueryExpr>),
    LessThan(Box<QueryExpr>, Box<QueryExpr>),
    LessThanOrEquals(Box<QueryExpr>, Box<QueryExpr>),
    GreaterThan(Box<QueryExpr>, Box<QueryExpr>),
    GreaterThanOrEquals(Box<QueryExpr>, Box<QueryExpr>),
    InList {
        expr: Box<QueryExpr>,
        list: Vec<QueryExpr>,
        negated: bool,
    },
    InSubquery {
        expr: Box<QueryExpr>,
        subquery: Box<QuerySelect>,
    },
    Like {
        expr: Box<QueryExpr>,
        pattern: Box<QueryExpr>,
    },
    And(Box<QueryExpr>, Box<QueryExpr>),
    Or(Box<QueryExpr>, Box<QueryExpr>),
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
pub struct QueryFrom {
    pub table: String,
    pub joins: Vec<QueryJoin>,
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
    pub source_column: Option<String>,
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

#[derive(Debug, Default, Clone, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
#[cfg_attr(
    feature = "wasm",
    derive(tsify::Tsify),
    tsify(into_wasm_abi, from_wasm_abi)
)]
pub struct QuerySelect {
    pub from: QueryFrom,
    pub projection: Vec<QueryColumn>,
    pub predicate: Option<QueryExpr>,
    pub aggregates: Vec<QueryAggregate>,
    pub group_by: Vec<QueryColumn>,
    pub order_by: Vec<QueryOrderBy>,
    pub limit: Option<usize>,
    pub offset: Option<usize>,
    pub having: Option<QueryExpr>,
}
#[derive(Debug, Clone, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
#[cfg_attr(
    feature = "wasm",
    derive(tsify::Tsify),
    tsify(into_wasm_abi, from_wasm_abi)
)]
pub struct QueryInsert {
    pub table: String,
    pub row: Row,
    pub returning: Option<Vec<String>>,
}

#[derive(Debug, Clone, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
#[cfg_attr(
    feature = "wasm",
    derive(tsify::Tsify),
    tsify(into_wasm_abi, from_wasm_abi)
)]
pub enum QueryInsertValue {
    Value(Value),
    Default,
}

#[derive(Debug, Clone, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
#[cfg_attr(
    feature = "wasm",
    derive(tsify::Tsify),
    tsify(into_wasm_abi, from_wasm_abi)
)]
pub struct QueryInsertValues {
    pub table: String,
    pub columns: Vec<String>,
    pub values: Vec<QueryInsertValue>,
    pub returning: Option<Vec<String>>,
}
#[derive(Debug, Clone, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
#[cfg_attr(
    feature = "wasm",
    derive(tsify::Tsify),
    tsify(into_wasm_abi, from_wasm_abi)
)]
pub struct QueryUpdate {
    pub from: QueryFrom,
    pub assignments: Vec<QueryUpdateAssignment>,
    pub predicate: Option<QueryExpr>,
    pub returning: Option<Vec<QueryColumn>>,
}
#[derive(Debug, Clone, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
#[cfg_attr(
    feature = "wasm",
    derive(tsify::Tsify),
    tsify(into_wasm_abi, from_wasm_abi)
)]
pub struct QueryDelete {
    pub from: QueryFrom,
    pub predicate: Option<QueryExpr>,
    pub returning: Option<Vec<QueryColumn>>,
}

#[derive(Debug, Clone, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
#[cfg_attr(
    feature = "wasm",
    derive(tsify::Tsify),
    tsify(into_wasm_abi, from_wasm_abi)
)]
pub enum Query {
    Select(QuerySelect),
    Insert(QueryInsert),
    InsertValues(QueryInsertValues),
    Update(QueryUpdate),
    Delete(QueryDelete),
}

#[derive(Debug, Clone, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
#[cfg_attr(
    feature = "wasm",
    derive(tsify::Tsify),
    tsify(into_wasm_abi, from_wasm_abi)
)]
pub enum AlterTableOperation {
    AddColumn(ColumnSchema),
    DropColumn(String),
    RenameColumn { old_name: String, new_name: String },
    RenameTable { new_name: String },
    AddIndex(IndexSchema),
    RenameIndex { old_name: String, new_name: String },
    DropIndex(String),
}

#[derive(Debug, Clone, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
#[cfg_attr(
    feature = "wasm",
    derive(tsify::Tsify),
    tsify(into_wasm_abi, from_wasm_abi)
)]
pub enum AlterIndexOperation {
    Rename { new_name: String },
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
    CreateTableWithIndexes {
        schema: TableSchema,
        indexes: Vec<IndexSchema>,
        if_not_exists: bool,
    },
    AlterTable {
        table_name: String,
        operations: Vec<AlterTableOperation>,
        if_exists: bool,
    },
    DropTable {
        table_name: String,
        if_exists: bool,
    },
    CreateIndex {
        schema: IndexSchema,
        if_not_exists: bool,
    },
    CreateIndexUnresolved {
        index_name: String,
        table_name: String,
        column_names: Vec<String>,
        unique: bool,
        if_not_exists: bool,
    },
    AlterIndex {
        index_name: String,
        operation: AlterIndexOperation,
        if_exists: bool,
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
