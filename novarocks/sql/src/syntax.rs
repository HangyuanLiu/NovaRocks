//! Narrow SQL syntax handoff for application admission.
//!
//! Frontend and application services consume typed parser values through this
//! vocabulary. SQL-owned semantic value carriers stay private to this crate.

pub use crate::legacy_mv_ast::{
    AlterMaterializedViewAction, AlterMaterializedViewStmt, CreateMaterializedViewStmt,
    DropMaterializedViewStmt, MaterializedViewDistribution, MaterializedViewRefreshPolicy,
    RefreshMaterializedViewStmt, ShowMaterializedViewsStmt,
};
pub use crate::syntax_ast::{
    AlterIcebergPartitionSpecStmt, ColumnAggregation, CreateTableKind, CreateTableStmt,
    DefaultLiteral, IcebergPartitionFieldExpr, Literal, ObjectName, TableColumnDef, TableKeyDesc,
    TableKeyKind,
};

/// Return every three-part table reference in one admitted SELECT statement.
///
/// Native typed nodes remain inside the SQL crate; callers receive only the
/// normalized `(catalog, namespace, table)` facts they need for admission.
pub fn three_part_table_ref_occurrences(
    sql: &str,
) -> Result<Vec<(String, String, String)>, String> {
    let statements = novarocks_parser::parse(sql).map_err(|error| error.to_string())?;
    let [novarocks_parser::ast::Statement::Query(query)] = statements.as_slice() else {
        return Err("three-part table reference extraction requires a SELECT query".to_string());
    };
    Ok(crate::parser::query_refs::extract_three_part_table_ref_occurrences(query))
}

pub fn extract_allow_throw_exception_hint(query: &novarocks_parser::ast::Query) -> bool {
    use novarocks_parser::ast::{BinaryOperator, Expr, LiteralKind, SelectHintValue, SetExpr};

    let mut body = query.body.as_ref();
    while let SetExpr::Query(nested) = body {
        body = nested.body.as_ref();
    }
    let SetExpr::Select(select) = body else {
        return false;
    };
    select.hints.iter().any(|hint| {
        hint.name.value.eq_ignore_ascii_case("set_var")
            && matches!(&hint.value, SelectHintValue::Call { arguments } if arguments.iter().any(|argument| {
                matches!(argument,
                    Expr::Binary(binary)
                        if binary.operator == BinaryOperator::Equal
                            && matches!(binary.left.as_ref(), Expr::Identifier(name) if name.value.eq_ignore_ascii_case("sql_mode"))
                            && matches!(binary.right.as_ref(), Expr::Literal(literal) if matches!(&literal.kind, LiteralKind::String(value) if value.to_ascii_lowercase().contains("allow_throw_exception")))
                )
            }))
    })
}

pub fn literal_from_batch(
    column: &arrow::array::ArrayRef,
    row_idx: usize,
) -> Result<Literal, String> {
    crate::literal::literal_from_batch(column, row_idx)
}

/// Convert an admitted SQL type to its Arrow value representation.
pub fn sql_type_to_arrow_type(
    sql_type: &novarocks_types::schema::SqlType,
) -> Result<arrow::datatypes::DataType, String> {
    crate::literal::sql_type_to_arrow_type(sql_type)
}

/// Convert an Arrow value representation back to its admitted SQL type.
///
/// This is the inverse of [`sql_type_to_arrow_type`] and is used to infer a
/// declared table schema from a produced Arrow schema (CTAS and view columns).
pub fn arrow_data_type_to_sql_type(
    data_type: &arrow::datatypes::DataType,
) -> Result<novarocks_types::schema::SqlType, String> {
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
    value: &novarocks_types::schema::ColumnDefault,
    data_type: &novarocks_types::schema::SqlType,
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

pub fn expr_to_literal(expr: &novarocks_parser::ast::Expr) -> Result<Literal, String> {
    crate::literal::expr_to_literal(expr)
}

#[cfg(test)]
mod tests {
    use super::{
        arrow_type_equals_ignoring_metadata, extract_allow_throw_exception_hint,
        sql_type_to_arrow_type,
    };

    #[test]
    fn syntax_value_helpers_keep_sql_type_conversion_and_metadata_tolerant_shape_equality() {
        use arrow::datatypes::DataType;
        use novarocks_types::schema::SqlType;

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

    #[test]
    fn allow_throw_exception_uses_typed_set_var_hints() {
        let mut statements =
            novarocks_parser::parse("SELECT /*+ SET_VAR(sql_mode = 'ALLOW_THROW_EXCEPTION') */ 1")
                .expect("typed hint fixture parses");
        let [novarocks_parser::ast::Statement::Query(query)] = statements.as_mut_slice() else {
            panic!("expected query");
        };
        assert!(extract_allow_throw_exception_hint(query));
    }
}
