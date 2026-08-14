//! Narrow SQL syntax handoff for application admission.
//!
//! Frontend and application services may normalize or parse an admitted
//! statement through this vocabulary. Parser state and implementation modules
//! stay private; the custom statement carriers below are the only exposed DDL
//! syntax surface.

pub use super::parser::{normalize_for_raw_parse, parse_normalized_sql_raw};
pub use crate::parser::ast::{
    AlterIcebergPartitionSpecStmt, ColumnAggregation, CreateCatalogStmt, CreateTableKind,
    CreateTableStmt, DefaultLiteral, DeleteStmt, DropCatalogStmt, DropDatabaseStmt, DropTableStmt,
    IcebergPartitionFieldExpr, Literal, MergeMatchedAction, MergeNotMatchedAction, MergeStmt,
    MergeWhenClause, MutationSource, ObjectName, TableColumnDef, TableKeyDesc, TableKeyKind,
    UpdateAssignment, UpdateStmt,
};
pub use crate::parser::ast::{AlterIcebergRefAction, AlterIcebergRefStmt, SnapshotAnchor};
pub use crate::parser::dialect::StarRocksDialect;

pub use crate::parser::dialect::substitute_user_variables;

use sqlparser::parser::Parser;

pub fn parse_sql_raw(sql: &str) -> Result<sqlparser::ast::Statement, String> {
    crate::parser::parse_sql_raw(sql)
}

pub fn extract_allow_throw_exception_hint(sql: &str) -> bool {
    crate::parser::set_var_hint::extract_allow_throw_exception(sql)
}

pub fn convert_object_name(name: sqlparser::ast::ObjectName) -> Result<ObjectName, String> {
    crate::parser::dialect::convert_object_name(name)
}

pub fn convert_sql_type(
    data_type: sqlparser::ast::DataType,
) -> Result<novarocks_catalog::schema::SqlType, String> {
    crate::parser::dialect::convert_sql_type(data_type)
}

pub fn literal_from_batch(
    column: &arrow::array::ArrayRef,
    row_idx: usize,
) -> Result<Literal, String> {
    crate::literal::literal_from_batch(column, row_idx)
}

/// Hashable syntax value used only to group admitted aggregate-table rows.
/// The representation deliberately stays independent from the literal module's
/// internal key type.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub enum AggregateLiteralKey {
    Null,
    Bool(bool),
    Int(i64),
    Float(u64),
    String(String),
}

pub fn aggregate_literal_key(literal: &Literal) -> AggregateLiteralKey {
    match literal {
        Literal::Null => AggregateLiteralKey::Null,
        Literal::Bool(value) => AggregateLiteralKey::Bool(*value),
        Literal::Int(value) => AggregateLiteralKey::Int(*value),
        Literal::Float(value) => AggregateLiteralKey::Float(value.to_bits()),
        Literal::String(value) | Literal::Date(value) => AggregateLiteralKey::String(value.clone()),
        Literal::Array(values) => AggregateLiteralKey::String(
            values
                .iter()
                .map(|value| format!("{value:?}"))
                .collect::<Vec<_>>()
                .join(","),
        ),
        Literal::Map(entries) => AggregateLiteralKey::String(format!("{entries:?}")),
        Literal::Struct(values) => AggregateLiteralKey::String(format!("{values:?}")),
    }
}

pub fn compare_aggregate_literals(
    left: &Literal,
    right: &Literal,
) -> Result<std::cmp::Ordering, String> {
    crate::literal::compare_literals(left, right)
}

pub fn column_default_to_literal(
    value: &novarocks_catalog::schema::ColumnDefault,
    data_type: &novarocks_catalog::schema::SqlType,
) -> Result<Literal, String> {
    crate::literal::column_default_to_ast_literal(value, data_type)
}

pub fn latin1_string_to_bytes(value: &str) -> Result<Vec<u8>, String> {
    crate::literal::latin1_string_to_bytes(value)
}

pub fn bytes_to_latin1_string(bytes: &[u8]) -> String {
    crate::literal::bytes_to_latin1_string(bytes)
}

pub fn parse_date_string_to_days(value: &str) -> Result<i32, String> {
    crate::literal::parse_date_string_to_days(value)
}

pub fn parse_datetime_string_to_micros(value: &str) -> Result<i64, String> {
    crate::literal::parse_datetime_string_to_micros(value)
}

pub fn sqlparser_expr_to_literal(expr: &sqlparser::ast::Expr) -> Result<Literal, String> {
    crate::literal::sqlparser_expr_to_literal(expr)
}

pub fn peek_word_eq(parser: &Parser<'_>, offset: usize, word: &str) -> bool {
    crate::parser::dialect::peek_word_eq(parser, offset, word)
}

pub fn looks_like_create_catalog(parser: &Parser<'_>) -> bool {
    crate::parser::dialect::looks_like_create_catalog(parser)
}

pub fn looks_like_create_table(parser: &Parser<'_>) -> bool {
    crate::parser::dialect::looks_like_create_table(parser)
}

pub fn looks_like_create_database(parser: &Parser<'_>) -> bool {
    crate::parser::dialect::looks_like_create_database(parser)
}

pub fn looks_like_drop_statement(parser: &Parser<'_>) -> bool {
    crate::parser::dialect::looks_like_drop_statement(parser)
}

pub fn looks_like_call_procedure(sql: &str) -> bool {
    crate::parser::procedure::looks_like_call_procedure(sql)
}

pub fn parse_create_database_name(parser: &mut Parser<'_>) -> Result<(ObjectName, bool), String> {
    crate::parser::dialect::parse_create_database_name(parser)
}

pub fn parse_create_catalog_statement(
    parser: &mut Parser<'_>,
) -> Result<CreateCatalogStmt, String> {
    crate::parser::dialect::create_catalog::parse_create_catalog_statement(parser)
}

pub fn parse_create_table_statement(parser: &mut Parser<'_>) -> Result<CreateTableStmt, String> {
    crate::parser::dialect::create_table::parse_create_table_statement(parser)
}

pub fn parse_sql_type_definition(
    parser: &mut Parser<'_>,
) -> Result<novarocks_catalog::schema::SqlType, String> {
    crate::parser::dialect::create_table::parse_sql_type_definition(parser)
}

pub fn parse_default_literal(
    parser: &mut Parser<'_>,
    data_type: &novarocks_catalog::schema::SqlType,
) -> Result<DefaultLiteral, String> {
    crate::parser::dialect::create_table::parse_default_literal(parser, data_type)
}

pub fn parse_partition_field_expr(
    parser: &mut Parser<'_>,
) -> Result<IcebergPartitionFieldExpr, String> {
    crate::parser::dialect::create_table::parse_partition_field_expr(parser)
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DropStatement {
    Table(DropTableStmt),
    Database(DropDatabaseStmt),
    Catalog(DropCatalogStmt),
}

pub fn parse_drop_statement(parser: &mut Parser<'_>) -> Result<DropStatement, String> {
    match crate::parser::dialect::drop::parse_drop_statement(parser)? {
        crate::parser::dialect::drop::DropResult::Table(statement) => {
            Ok(DropStatement::Table(statement))
        }
        crate::parser::dialect::drop::DropResult::Database(statement) => {
            Ok(DropStatement::Database(statement))
        }
        crate::parser::dialect::drop::DropResult::Catalog(statement) => {
            Ok(DropStatement::Catalog(statement))
        }
    }
}

pub fn parse_alter_iceberg_ref(sql: &str) -> Result<Option<AlterIcebergRefStmt>, String> {
    let mut statements = crate::parser::parse_sql(sql)?;
    if statements.len() != 1 {
        return Err("Iceberg ref command accepts exactly one statement".to_string());
    }
    match statements.pop().expect("one checked statement") {
        crate::parser::ast::Statement::AlterIcebergRef(statement) => Ok(Some(statement)),
        _ => Ok(None),
    }
}
