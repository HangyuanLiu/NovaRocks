//! Narrow SQL syntax handoff for application admission.
//!
//! Frontend and application services may normalize or parse an admitted
//! statement through this vocabulary. Parser state and implementation modules
//! stay private; the custom statement carriers below are the only exposed DDL
//! syntax surface.

pub use super::parser::{normalize_for_raw_parse, parse_normalized_sql_raw};
pub use crate::parser::ast::{
    AlterIcebergPartitionSpecStmt, AlterMaterializedViewAction, AlterMaterializedViewStmt,
    ColumnAggregation, CreateMaterializedViewStmt, CreateTableKind, CreateTableStmt,
    DefaultLiteral, DeleteStmt, DropMaterializedViewStmt, IcebergPartitionFieldExpr, Literal,
    MaterializedViewDistribution, MaterializedViewRefreshPolicy, MergeMatchedAction,
    MergeNotMatchedAction, MergeStmt, MergeWhenClause, MutationSource, ObjectName,
    RefreshMaterializedViewStmt, ShowMaterializedViewsStmt, TableColumnDef, TableKeyDesc,
    TableKeyKind, UpdateAssignment, UpdateStmt,
};
pub use crate::parser::dialect::StarRocksDialect;

pub use crate::parser::dialect::substitute_user_variables;

use sqlparser::parser::Parser;

pub fn parse_sql_raw(sql: &str) -> Result<sqlparser::ast::Statement, String> {
    crate::parser::parse_sql_raw(sql)
}

/// Typed, closed materialized-view command admission surface.
///
/// This intentionally does not expose the parser's generic statement enum:
/// consumers can handle only the five MV command forms supported by this
/// contract.
#[derive(Clone, Debug, PartialEq)]
pub enum MvAdmittedStatement {
    Create(CreateMaterializedViewStmt),
    Drop(DropMaterializedViewStmt),
    Alter(AlterMaterializedViewStmt),
    Refresh(RefreshMaterializedViewStmt),
    Show(ShowMaterializedViewsStmt),
}

/// Parse an MV command when the parser recognizes an MV statement form.
///
/// `Ok(None)` is a route miss: the input is not one of the materialized-view
/// statement forms. Once the parser recognizes one of those forms, parse
/// rejection remains an error instead of being converted into a route miss.
// Design: ADR-0088 (docs/adr/ADR-0088-domain-owned-sql-error-contracts.md)
pub fn parse_optional_mv_admitted_statement(
    sql: &str,
) -> Result<Option<MvAdmittedStatement>, String> {
    let normalized = crate::parser::dialect::normalize_for_raw_parse(sql)?;
    let dialect = crate::parser::dialect::StarRocksDialect;
    let parser = Parser::new(&dialect)
        .try_with_sql(&normalized)
        .map_err(|error| error.to_string())?;
    let recognized =
        crate::parser::dialect::materialized_view::looks_like_create_materialized_view(&parser)
            || crate::parser::dialect::materialized_view::looks_like_drop_materialized_view(
                &parser,
            )
            || crate::parser::dialect::materialized_view::looks_like_refresh_materialized_view(
                &parser,
            )
            || crate::parser::dialect::materialized_view::looks_like_show_materialized_views(
                &parser,
            )
            || crate::parser::dialect::materialized_view::looks_like_alter_materialized_view(
                &parser,
            );
    if !recognized {
        return Ok(None);
    }

    parse_recognized_mv_admitted_statement(sql).map(Some)
}

pub fn parse_mv_admitted_statement(sql: &str) -> Result<MvAdmittedStatement, String> {
    parse_optional_mv_admitted_statement(sql)?
        .ok_or_else(|| "statement is not a materialized-view command".to_string())
}

fn parse_recognized_mv_admitted_statement(sql: &str) -> Result<MvAdmittedStatement, String> {
    let mut statements = crate::parser::parse_sql(sql)?;
    if statements.len() != 1 {
        return Err("materialized-view command accepts exactly one statement".to_string());
    }
    match statements.pop().expect("one checked statement") {
        crate::parser::ast::Statement::CreateMaterializedView(statement) => {
            Ok(MvAdmittedStatement::Create(statement))
        }
        crate::parser::ast::Statement::DropMaterializedView(statement) => {
            Ok(MvAdmittedStatement::Drop(statement))
        }
        crate::parser::ast::Statement::AlterMaterializedView(statement) => {
            Ok(MvAdmittedStatement::Alter(statement))
        }
        crate::parser::ast::Statement::RefreshMaterializedView(statement) => {
            Ok(MvAdmittedStatement::Refresh(statement))
        }
        crate::parser::ast::Statement::ShowMaterializedViews(statement) => {
            Ok(MvAdmittedStatement::Show(statement))
        }
        _ => Err("statement is not a materialized-view command".to_string()),
    }
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

pub fn peek_word_eq(parser: &Parser<'_>, offset: usize, word: &str) -> bool {
    crate::parser::dialect::peek_word_eq(parser, offset, word)
}

pub fn looks_like_create_table(parser: &Parser<'_>) -> bool {
    crate::parser::dialect::looks_like_create_table(parser)
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

#[cfg(test)]
mod tests {
    use super::{
        MvAdmittedStatement, arrow_type_equals_ignoring_metadata, parse_mv_admitted_statement,
        parse_optional_mv_admitted_statement, sql_type_to_arrow_type,
    };

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

    #[test]
    fn mv_admission_exposes_only_typed_mv_syntax() {
        let statement = parse_mv_admitted_statement("REFRESH MATERIALIZED VIEW analytics.mv")
            .expect("MV refresh should be admitted");
        let MvAdmittedStatement::Refresh(statement) = statement else {
            panic!("expected typed REFRESH MATERIALIZED VIEW statement");
        };
        assert_eq!(statement.name.parts, ["analytics", "mv"]);
        assert!(!statement.full);
    }

    #[test]
    fn mv_admission_rejects_non_mv_statement_without_exposing_parser_enum() {
        let error = parse_mv_admitted_statement("SELECT 1")
            .expect_err("non-MV syntax must not be admitted through the MV contract");
        assert_eq!(error, "statement is not a materialized-view command");
    }

    #[test]
    fn optional_mv_admission_distinguishes_route_misses_from_mv_parse_errors() {
        assert_eq!(
            parse_optional_mv_admitted_statement("SELECT 1").expect("non-MV probe"),
            None
        );

        let statement =
            parse_optional_mv_admitted_statement("REFRESH MATERIALIZED VIEW analytics.mv")
                .expect("valid MV command")
                .expect("MV command should be recognized");
        assert!(matches!(statement, MvAdmittedStatement::Refresh(_)));

        let error = parse_optional_mv_admitted_statement(
            "CREATE MATERIALIZED VIEW mv \
             DISTRIBUTED BY HASH(k1) BUCKETS 1 \
             PRIMARY KEY () \
             AS SELECT k1 FROM source_table",
        )
        .expect_err("recognized MV syntax must preserve its parser error");
        assert_eq!(error, "PRIMARY KEY clause requires at least one column");
    }
}
