//! Narrow SQL syntax handoff for application admission.
//!
//! Frontend and application services may normalize or parse an admitted
//! statement through this vocabulary. Parser state and implementation modules
//! stay private; the custom statement carriers below are the only exposed DDL
//! syntax surface.

pub use super::parser::{normalize_for_raw_parse, parse_normalized_sql_raw};
pub use crate::legacy_mv_ast::{
    AlterMaterializedViewAction, AlterMaterializedViewStmt, CreateMaterializedViewStmt,
    DropMaterializedViewStmt, MaterializedViewDistribution, MaterializedViewRefreshPolicy,
    RefreshMaterializedViewStmt, ShowMaterializedViewsStmt,
};
pub use crate::parser::ast::{
    AlterIcebergPartitionSpecStmt, ColumnAggregation, CreateTableKind, CreateTableStmt,
    DefaultLiteral, IcebergPartitionFieldExpr, Literal, ObjectName, TableColumnDef, TableKeyDesc,
    TableKeyKind,
};
pub use crate::parser::dialect::StarRocksDialect;

pub use crate::parser::dialect::substitute_user_variables;

pub fn parse_sql_raw(sql: &str) -> Result<sqlparser::ast::Statement, String> {
    crate::parser::parse_sql_raw(sql)
}

/// Return every three-part table reference in one admitted SELECT statement.
///
/// Raw sqlparser nodes remain inside the SQL crate; callers receive only the
/// normalized `(catalog, namespace, table)` facts they need for admission.
pub fn three_part_table_ref_occurrences(
    sql: &str,
) -> Result<Vec<(String, String, String)>, String> {
    let statement = crate::parser::parse_sql_raw(sql)?;
    let sqlparser::ast::Statement::Query(query) = statement else {
        return Err("three-part table reference extraction requires a SELECT query".to_string());
    };
    Ok(crate::parser::query_refs::extract_three_part_table_ref_occurrences(&query))
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

/// Convert an admitted SQL type to its Arrow value representation.
pub fn sql_type_to_arrow_type(
    sql_type: &novarocks_catalog::schema::SqlType,
) -> Result<arrow::datatypes::DataType, String> {
    crate::literal::sql_type_to_arrow_type(sql_type)
}

/// Convert an Arrow value representation back to its admitted SQL type.
///
/// This is the inverse of [`sql_type_to_arrow_type`] and is used to infer a
/// declared table schema from a produced Arrow schema (CTAS and view columns).
pub fn arrow_data_type_to_sql_type(
    data_type: &arrow::datatypes::DataType,
) -> Result<novarocks_catalog::schema::SqlType, String> {
    crate::literal::arrow_data_type_to_sql_type(data_type)
}

/// Compare Arrow value shapes while ignoring non-semantic field metadata.
pub fn arrow_type_equals_ignoring_metadata(
    left: &arrow::datatypes::DataType,
    right: &arrow::datatypes::DataType,
) -> bool {
    crate::literal::arrow_type_equals_ignoring_metadata(left, right)
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

#[cfg(test)]
mod tests {
    use super::{arrow_type_equals_ignoring_metadata, sql_type_to_arrow_type};

    #[test]
    fn syntax_value_helpers_keep_sql_type_conversion_and_metadata_tolerant_shape_equality() {
        use arrow::datatypes::DataType;
        use novarocks_catalog::schema::SqlType;

        assert_eq!(
            sql_type_to_arrow_type(&SqlType::BigInt).expect("SQL type conversion"),
            DataType::Int64
        );
        assert!(arrow_type_equals_ignoring_metadata(
            &DataType::Utf8,
            &DataType::Utf8
        ));
        assert!(!arrow_type_equals_ignoring_metadata(
            &DataType::Utf8,
            &DataType::Int64
        ));
    }
}
