//! DDL/DML statement handlers for the standalone engine.
//!
//! Top-level dispatchers (`execute_create_database_statement`,
//! `execute_insert_statement`, etc.) route statements to the in-memory
//! catalog, the iceberg registry, or the managed lake based on the parsed name
//! and current catalog/database session context.

use std::sync::Arc;

use crate::connector::truncate_managed_table as truncate_managed_lake_table;
use crate::engine::catalog::normalize_identifier;
use crate::engine::name_resolve::resolve_local_table_name;
use crate::engine::{
    StandaloneState, StatementResult, delete_iceberg_catalog_if_needed,
    delete_iceberg_namespace_if_needed, delete_iceberg_table_if_needed,
    persist_iceberg_namespace_if_needed, persist_iceberg_table_if_needed,
};
use crate::sql::parser::ast::{
    CreateTableKind, Expr, GenerateSeriesSelect, InsertSource, Literal, ObjectName, SqlType,
};
use sqlparser::keywords::Keyword;
use sqlparser::parser::Parser;
use sqlparser::tokenizer::Token;

use super::generate_series::parse_generate_series_function_expr;
use super::sql_expr::{sqlparser_expr_to_custom_expr, sqlparser_expr_to_literal};

fn convert_set_expr_to_insert_source(
    body: &sqlparser::ast::SetExpr,
) -> Result<InsertSource, String> {
    use sqlparser::ast as sqlast;
    match body {
        sqlast::SetExpr::Values(values) => {
            let mut rows = Vec::new();
            for row in &values.rows {
                let literal_row: Vec<Literal> = row
                    .iter()
                    .map(sqlparser_expr_to_literal)
                    .collect::<Result<_, _>>()?;
                rows.push(literal_row);
            }
            Ok(InsertSource::Values(rows))
        }
        sqlast::SetExpr::Select(select) => {
            if select.from.is_empty() {
                let row: Vec<Literal> = select
                    .projection
                    .iter()
                    .map(|item| match item {
                        sqlast::SelectItem::UnnamedExpr(expr) => sqlparser_expr_to_literal(expr),
                        _ => Err("INSERT SELECT source only supports unnamed expressions".into()),
                    })
                    .collect::<Result<_, _>>()?;
                Ok(InsertSource::SelectLiteralRow(row))
            } else if select.from.len() == 1 {
                let table_with_joins = &select.from[0];
                if table_with_joins.joins.is_empty() {
                    if let sqlparser::ast::TableFactor::TableFunction {
                        ref expr,
                        ref alias,
                    } = table_with_joins.relation
                    {
                        let (start, end, step) = parse_generate_series_function_expr(expr)?;
                        let column_name = alias
                            .as_ref()
                            .and_then(|a| a.columns.first().map(|c| c.name.value.clone()))
                            .unwrap_or_else(|| "generate_series".to_string());
                        let projection: Vec<Expr> = select
                            .projection
                            .iter()
                            .map(|item| match item {
                                sqlast::SelectItem::UnnamedExpr(expr) => {
                                    sqlparser_expr_to_custom_expr(expr)
                                }
                                _ => {
                                    Err("INSERT SELECT source only supports unnamed expressions"
                                        .into())
                                }
                            })
                            .collect::<Result<_, _>>()?;
                        Ok(InsertSource::GenerateSeriesSelect(GenerateSeriesSelect {
                            column_name,
                            start,
                            end,
                            step,
                            projection,
                        }))
                    } else {
                        Err("unsupported INSERT SELECT source".into())
                    }
                } else {
                    Err("INSERT SELECT with joins is not supported in this path".into())
                }
            } else {
                Err("INSERT SELECT with multiple tables is not supported".into())
            }
        }
        sqlast::SetExpr::SetOperation {
            op,
            set_quantifier,
            left,
            right,
        } => {
            // Only UNION ALL is handled at this layer. UNION (distinct) would
            // need output-level dedup with the target schema's types, which we
            // don't have at parse time.
            if !matches!(op, sqlast::SetOperator::Union) {
                return Err("INSERT SELECT set operation is only UNION ALL here".into());
            }
            if !matches!(
                set_quantifier,
                sqlast::SetQuantifier::All | sqlast::SetQuantifier::AllByName
            ) {
                return Err(
                    "INSERT SELECT UNION requires UNION ALL (UNION/UNION DISTINCT unsupported)"
                        .into(),
                );
            }
            let mut parts = Vec::new();
            flatten_union_all(left, &mut parts)?;
            flatten_union_all(right, &mut parts)?;
            Ok(InsertSource::UnionAll(parts))
        }
        sqlast::SetExpr::Query(query) => convert_set_expr_to_insert_source(query.body.as_ref()),
        _ => Err("unsupported INSERT source".into()),
    }
}

fn flatten_union_all(
    body: &sqlparser::ast::SetExpr,
    out: &mut Vec<InsertSource>,
) -> Result<(), String> {
    use sqlparser::ast as sqlast;
    if let sqlast::SetExpr::SetOperation {
        op: sqlast::SetOperator::Union,
        set_quantifier: sqlast::SetQuantifier::All | sqlast::SetQuantifier::AllByName,
        left,
        right,
    } = body
    {
        flatten_union_all(left, out)?;
        flatten_union_all(right, out)?;
        Ok(())
    } else {
        out.push(convert_set_expr_to_insert_source(body)?);
        Ok(())
    }
}

/// Decide whether an INSERT's source Query should be executed via the full
/// plan pipeline (returning `InsertSource::FromQuery`) instead of being
/// collapsed into literal rows or a generate_series short-form.
///
/// We route via the full pipeline whenever the Query carries clauses the
/// literal fast path can't represent (WITH/ORDER BY/LIMIT/FETCH/locks), or
/// when the body is a SELECT that reads from at least one real relation
/// (i.e. something other than `TABLE(generate_series(...))`).
fn should_route_insert_via_from_query(query: &sqlparser::ast::Query) -> bool {
    if query.with.is_some()
        || query.order_by.is_some()
        || query.limit_clause.is_some()
        || query.fetch.is_some()
        || !query.locks.is_empty()
    {
        return true;
    }
    body_reads_from_real_relation(query.body.as_ref())
}

fn body_reads_from_real_relation(body: &sqlparser::ast::SetExpr) -> bool {
    use sqlparser::ast as sqlast;
    match body {
        sqlast::SetExpr::Select(select) => {
            if select.from.is_empty() {
                return false;
            }
            for table_with_joins in &select.from {
                if !table_with_joins.joins.is_empty() {
                    return true;
                }
                match &table_with_joins.relation {
                    sqlast::TableFactor::TableFunction { .. } => {
                        // generate_series is handled by the literal fast path.
                    }
                    _ => return true,
                }
            }
            false
        }
        sqlast::SetExpr::Query(inner) => should_route_insert_via_from_query(inner.as_ref()),
        sqlast::SetExpr::SetOperation { left, right, .. } => {
            body_reads_from_real_relation(left) || body_reads_from_real_relation(right)
        }
        _ => false,
    }
}

/// Convert a sqlparser INSERT AST to our custom InsertStmt.
/// Used for Iceberg tables which need the custom AST's InsertSource types.
pub(crate) fn convert_sqlparser_insert_to_custom(
    insert: &sqlparser::ast::Insert,
) -> Result<crate::sql::parser::ast::InsertStmt, String> {
    use sqlparser::ast as sqlast;

    let table = match &insert.table {
        sqlast::TableObject::TableName(name) => {
            crate::sql::parser::dialect::convert_object_name(name.clone())?
        }
        other => return Err(format!("unsupported INSERT target: {other}")),
    };
    let columns: Vec<String> = insert.columns.iter().map(|c| c.value.clone()).collect();
    let source_query = insert
        .source
        .as_ref()
        .ok_or_else(|| "INSERT requires a source".to_string())?;
    // If the body is a SELECT that reads from a real relation (not a
    // generate_series table function), or carries a WITH/ORDER BY/LIMIT that
    // the literal fast-path can't express, hand the whole Query to the
    // analyzer/planner/pipeline stack via `FromQuery`. This keeps the INSERT
    // entry point aligned with how StarRocks wraps INSERT ... SELECT as a
    // normal plan with a sink, rather than evaluating SELECT here.
    let source = if should_route_insert_via_from_query(source_query) {
        crate::sql::parser::ast::InsertSource::FromQuery(source_query.clone())
    } else {
        convert_set_expr_to_insert_source(source_query.body.as_ref())?
    };
    Ok(crate::sql::parser::ast::InsertStmt {
        table,
        columns,
        source,
        overwrite: insert.overwrite,
    })
}

/// Convert a sqlparser DELETE AST to our custom DeleteStmt.
///
/// Phase 1 restrictions:
/// - Exactly one table in `FROM`; `USING` clauses are rejected.
/// - `WHERE` is mandatory. `DELETE FROM t` (no filter) is rejected — the
///   spec recommends `INSERT OVERWRITE t SELECT * FROM t WHERE FALSE` instead.
/// - `LIMIT` and `ORDER BY` are rejected.
pub(crate) fn convert_sqlparser_delete_to_custom(
    delete: &sqlparser::ast::Delete,
) -> Result<crate::sql::parser::ast::DeleteStmt, String> {
    use sqlparser::ast as sqlast;

    let tables = match &delete.from {
        sqlast::FromTable::WithFromKeyword(tables) => tables,
        sqlast::FromTable::WithoutKeyword(tables) => tables,
    };
    if tables.len() != 1 {
        return Err(format!(
            "phase 1 DELETE supports exactly one table in FROM, got {}",
            tables.len()
        ));
    }
    if !tables[0].joins.is_empty() {
        return Err("phase 1 DELETE does not support JOIN in FROM".to_string());
    }
    let table = match &tables[0].relation {
        sqlast::TableFactor::Table { name, .. } => {
            crate::sql::parser::dialect::convert_object_name(name.clone())?
        }
        other => {
            return Err(format!(
                "phase 1 DELETE source must be a table, got {other:?}"
            ));
        }
    };
    if delete.using.as_ref().is_some_and(|u| !u.is_empty()) {
        return Err("phase 1 DELETE does not support USING".to_string());
    }
    if delete.limit.is_some() {
        return Err("phase 1 DELETE does not support LIMIT".to_string());
    }
    if !delete.order_by.is_empty() {
        return Err("phase 1 DELETE does not support ORDER BY".to_string());
    }
    let where_clause = delete.selection.clone().ok_or_else(|| {
        "DELETE requires a WHERE clause; for full table replacement use \
         INSERT OVERWRITE t SELECT * FROM t WHERE FALSE"
            .to_string()
    })?;
    Ok(crate::sql::parser::ast::DeleteStmt {
        table,
        where_clause,
    })
}

// ---------------------------------------------------------------------------
// DDL handlers
// ---------------------------------------------------------------------------

pub(crate) fn execute_create_database_statement(
    state: &Arc<StandaloneState>,
    name: &ObjectName,
    current_catalog: Option<&str>,
) -> Result<StatementResult, String> {
    let target =
        crate::engine::backend_resolver::resolve_namespace_target(state, name, current_catalog)?;
    let backend = state
        .connectors
        .read()
        .expect("connector registry read")
        .catalog_backend(target.backend_name)?;
    backend.create_namespace(&target.catalog, &target.namespace)?;
    if target.backend_name == "iceberg" {
        persist_iceberg_namespace_if_needed(state, &target.catalog, &target.namespace)?;
    }
    Ok(StatementResult::Ok)
}

pub(crate) fn execute_create_table_statement(
    state: &Arc<StandaloneState>,
    stmt: crate::sql::parser::ast::CreateTableStmt,
    current_catalog: Option<&str>,
    current_database: &str,
) -> Result<StatementResult, String> {
    match stmt.kind {
        CreateTableKind::Iceberg {
            columns,
            key_desc,
            bucket_count,
            properties,
        } => {
            if current_catalog.is_none()
                && stmt.name.parts.len() <= 2
                && state.managed_lake_config.is_none()
            {
                return Err(
                    "managed lake is not configured; set `warehouse_uri` to run CREATE TABLE"
                        .to_string(),
                );
            }

            let target = crate::engine::backend_resolver::resolve_table_target(
                state,
                &stmt.name,
                current_catalog,
                current_database,
            )?;
            let backend = state
                .connectors
                .read()
                .expect("connector registry read")
                .catalog_backend(target.backend_name)?;
            backend.create_table(crate::connector::backend::CreateTableRequest {
                catalog: target.catalog.clone(),
                namespace: target.namespace.clone(),
                table: target.table.clone(),
                columns,
                key_desc,
                bucket_count,
                properties,
            })?;
            if target.backend_name == "iceberg" {
                persist_iceberg_table_if_needed(
                    state,
                    &target.catalog,
                    &target.namespace,
                    &target.table,
                )?;
            }
            Ok(StatementResult::Ok)
        }
    }
}

pub(crate) fn execute_drop_catalog_statement(
    state: &Arc<StandaloneState>,
    catalog_name: &str,
    if_exists: bool,
) -> Result<StatementResult, String> {
    let mut guard = state
        .iceberg_catalogs
        .write()
        .expect("standalone iceberg catalog write lock");
    match guard.drop_catalog(catalog_name) {
        Ok(()) => {
            drop(guard);
            delete_iceberg_catalog_if_needed(state, &normalize_identifier(catalog_name)?)?;
            Ok(StatementResult::Ok)
        }
        Err(err) if if_exists && err.contains("unknown catalog") => Ok(StatementResult::Ok),
        Err(err) => Err(err),
    }
}

pub(crate) fn execute_drop_database_statement(
    state: &Arc<StandaloneState>,
    name: &ObjectName,
    current_catalog: Option<&str>,
    if_exists: bool,
    force: bool,
) -> Result<StatementResult, String> {
    let target =
        crate::engine::backend_resolver::resolve_namespace_target(state, name, current_catalog)?;
    let backend = state
        .connectors
        .read()
        .expect("connector registry read")
        .catalog_backend(target.backend_name)?;
    if target.backend_name == "iceberg"
        && !backend.namespace_exists(&target.catalog, &target.namespace)?
    {
        return if if_exists {
            Ok(StatementResult::Ok)
        } else {
            Err(format!("unknown database `{}`", name.parts.join(".")))
        };
    }
    match backend.drop_namespace(&target.catalog, &target.namespace, force) {
        Ok(()) => {
            if target.backend_name == "iceberg" {
                delete_iceberg_namespace_if_needed(state, &target.catalog, &target.namespace)?;
            }
            Ok(StatementResult::Ok)
        }
        Err(err) if if_exists && err.contains("unknown") => Ok(StatementResult::Ok),
        Err(err) if if_exists && target.backend_name == "iceberg" && err.contains("namespace") => {
            Ok(StatementResult::Ok)
        }
        Err(err) => Err(err),
    }
}

pub(crate) fn execute_drop_table_statement(
    state: &Arc<StandaloneState>,
    name: &ObjectName,
    current_catalog: Option<&str>,
    current_database: &str,
    if_exists: bool,
    _force: bool,
) -> Result<StatementResult, String> {
    let target = match crate::engine::backend_resolver::resolve_existing_table_target(
        state,
        name,
        current_catalog,
        current_database,
    ) {
        Ok(target) => target,
        Err(_) if current_catalog.is_none() && name.parts.len() <= 2 => {
            // External parquet tables registered through the embedding API are
            // still catalog-only entries. Dropping them does not involve a
            // connector backend.
            return drop_local_catalog_table(state, name, current_database, if_exists);
        }
        Err(err) => return Err(err),
    };
    let backend = state
        .connectors
        .read()
        .expect("connector registry read")
        .catalog_backend(target.backend_name)?;
    match backend.drop_table(&target.catalog, &target.namespace, &target.table, if_exists) {
        Ok(()) => {
            if target.backend_name == "iceberg" {
                delete_iceberg_table_if_needed(
                    state,
                    &target.catalog,
                    &target.namespace,
                    &target.table,
                )?;
            }
            Ok(StatementResult::Ok)
        }
        Err(err) if if_exists && err.contains("table") => Ok(StatementResult::Ok),
        Err(err) => Err(err),
    }
}

fn drop_local_catalog_table(
    state: &Arc<StandaloneState>,
    name: &ObjectName,
    current_database: &str,
    if_exists: bool,
) -> Result<StatementResult, String> {
    let resolved = resolve_local_table_name(name, current_database)?;
    let mut guard = state
        .catalog
        .write()
        .expect("standalone catalog write lock");
    match guard.drop_table(&resolved.database, &resolved.table) {
        Ok(()) => Ok(StatementResult::Ok),
        Err(err) if if_exists && err.contains("unknown") => Ok(StatementResult::Ok),
        Err(err) => Err(err),
    }
}

// ---------------------------------------------------------------------------
// DML handlers
// ---------------------------------------------------------------------------

pub(crate) fn execute_truncate_table_statement(
    state: &Arc<StandaloneState>,
    name: &ObjectName,
    current_database: &str,
) -> Result<StatementResult, String> {
    let resolved = resolve_local_table_name(name, current_database)?;
    if state
        .managed_lake
        .read()
        .expect("standalone managed lake read lock")
        .contains_table(&resolved.database, &resolved.table)?
    {
        return truncate_managed_lake_table(state, &resolved.database, &resolved.table);
    }
    Err(format!(
        "TRUNCATE TABLE only supports managed-lake tables: {}.{}",
        resolved.database, resolved.table
    ))
}

pub(crate) fn execute_insert_statement(
    state: &Arc<StandaloneState>,
    name: &ObjectName,
    columns: &[String],
    source: &InsertSource,
    overwrite: bool,
    current_catalog: Option<&str>,
    current_database: &str,
) -> Result<StatementResult, String> {
    crate::engine::insert_flow::run_insert(
        state,
        name,
        columns,
        source,
        overwrite,
        current_catalog,
        current_database,
    )
}

// ---------------------------------------------------------------------------
// ADD FILES SQL parsing
// ---------------------------------------------------------------------------

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct AddEqualityDeleteStmt {
    pub(crate) table: ObjectName,
    pub(crate) columns: Vec<String>,
    pub(crate) rows: Vec<Vec<Literal>>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct AlterIcebergSchemaStmt {
    pub(crate) table: ObjectName,
    pub(crate) change: IcebergSchemaChange,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum IcebergSchemaChange {
    AddColumn {
        name: String,
        data_type: SqlType,
        default_null: bool,
    },
    DropColumn {
        name: String,
    },
    RenameColumn {
        old_name: String,
        new_name: String,
    },
    ModifyColumn {
        name: String,
        new_type: SqlType,
    },
}

/// Check if SQL looks like ALTER TABLE ... ADD FILES FROM ...
pub(crate) fn looks_like_add_files(sql: &str) -> bool {
    let upper = sql.trim().to_ascii_uppercase();
    upper.starts_with("ALTER TABLE") && upper.contains("ADD FILES FROM")
}

/// Parse: ALTER TABLE [catalog.db.]table ADD FILES FROM 's3://...'
pub(crate) fn parse_add_files_sql(sql: &str) -> Result<(Vec<String>, String), String> {
    // Extract the part between ALTER TABLE and ADD FILES FROM
    let upper = sql.to_ascii_uppercase();
    let alter_idx = upper.find("ALTER TABLE").ok_or("missing ALTER TABLE")?;
    let add_files_idx = upper
        .find("ADD FILES FROM")
        .ok_or("missing ADD FILES FROM")?;

    let table_str = sql[alter_idx + 11..add_files_idx].trim();
    let table_parts: Vec<String> = table_str
        .split('.')
        .map(|s| s.trim().trim_matches('`').to_lowercase())
        .collect();

    // Extract the path after ADD FILES FROM
    let after_from = &sql[add_files_idx + 14..];
    let path = after_from
        .trim()
        .trim_end_matches(';')
        .trim()
        .trim_matches('\'')
        .trim_matches('"')
        .to_string();

    if path.is_empty() {
        return Err("ADD FILES FROM requires a path".to_string());
    }

    Ok((table_parts, path))
}

pub(crate) fn looks_like_alter_iceberg_schema(sql: &str) -> bool {
    let Ok(normalized) = crate::sql::parser::dialect::normalize_for_raw_parse(sql) else {
        return false;
    };
    let dialect = crate::sql::parser::dialect::StarRocksDialect;
    let Ok(mut parser) = Parser::new(&dialect).try_with_sql(&normalized) else {
        return false;
    };

    if !parser.parse_keyword(Keyword::ALTER) || !parser.parse_keyword(Keyword::TABLE) {
        return false;
    }
    if parser.parse_object_name(false).is_err() {
        return false;
    }

    if parser.parse_keyword(Keyword::ADD) {
        return parser.parse_keyword(Keyword::COLUMN);
    }
    if parser.parse_keyword(Keyword::DROP) {
        return parser.parse_keyword(Keyword::COLUMN);
    }
    if parser.parse_keyword(Keyword::RENAME) {
        return parser.parse_keyword(Keyword::COLUMN);
    }
    if crate::sql::parser::dialect::peek_word_eq(&parser, 0, "MODIFY") {
        parser.next_token();
        return parser.parse_keyword(Keyword::COLUMN);
    }
    false
}

pub(crate) fn parse_alter_iceberg_schema_sql(sql: &str) -> Result<AlterIcebergSchemaStmt, String> {
    let normalized = crate::sql::parser::dialect::normalize_for_raw_parse(sql)?;
    let dialect = crate::sql::parser::dialect::StarRocksDialect;
    let mut parser = Parser::new(&dialect)
        .try_with_sql(&normalized)
        .map_err(|e| format!("parse ALTER TABLE schema DDL: {e}"))?;

    parser
        .expect_keyword(Keyword::ALTER)
        .map_err(|e| e.to_string())?;
    parser
        .expect_keyword(Keyword::TABLE)
        .map_err(|e| e.to_string())?;
    let table = crate::sql::parser::dialect::convert_object_name(
        parser.parse_object_name(false).map_err(|e| e.to_string())?,
    )?;

    let change = if parser.parse_keywords(&[Keyword::ADD, Keyword::COLUMN]) {
        parse_add_column_change(&mut parser)?
    } else if parser.parse_keywords(&[Keyword::DROP, Keyword::COLUMN]) {
        let name = parser.parse_identifier().map_err(|e| e.to_string())?.value;
        IcebergSchemaChange::DropColumn { name }
    } else if parser.parse_keywords(&[Keyword::RENAME, Keyword::COLUMN]) {
        let old_name = parser.parse_identifier().map_err(|e| e.to_string())?.value;
        parser
            .expect_keyword(Keyword::TO)
            .map_err(|e| e.to_string())?;
        let new_name = parser.parse_identifier().map_err(|e| e.to_string())?.value;
        IcebergSchemaChange::RenameColumn { old_name, new_name }
    } else if crate::sql::parser::dialect::peek_word_eq(&parser, 0, "MODIFY") {
        parser.next_token();
        parser
            .expect_keyword(Keyword::COLUMN)
            .map_err(|e| e.to_string())?;
        let name = parser.parse_identifier().map_err(|e| e.to_string())?.value;
        let new_type = crate::sql::parser::dialect::convert_sql_type(
            parser.parse_data_type().map_err(|e| e.to_string())?,
        )?;
        IcebergSchemaChange::ModifyColumn { name, new_type }
    } else {
        return Err("unsupported ALTER TABLE schema evolution clause".to_string());
    };

    if parser.peek_token_ref().token == Token::SemiColon {
        parser.next_token();
    }
    if parser.peek_token_ref().token != Token::EOF {
        return Err(format!(
            "unsupported trailing ALTER TABLE schema tokens starting at {}",
            parser.peek_token_ref().token
        ));
    }

    Ok(AlterIcebergSchemaStmt { table, change })
}

fn parse_add_column_change(parser: &mut Parser<'_>) -> Result<IcebergSchemaChange, String> {
    let name = parser.parse_identifier().map_err(|e| e.to_string())?.value;
    let data_type = crate::sql::parser::dialect::convert_sql_type(
        parser.parse_data_type().map_err(|e| e.to_string())?,
    )?;
    let mut default_null = false;
    let mut seen_null = false;
    let mut seen_default_null = false;
    loop {
        if parser.parse_keywords(&[Keyword::NOT, Keyword::NULL]) {
            return Err(
                "ADD COLUMN NOT NULL is not supported for Iceberg schema evolution".to_string(),
            );
        }
        if parser.parse_keyword(Keyword::NULL) {
            if seen_null {
                return Err("duplicate NULL clause in ADD COLUMN".to_string());
            }
            seen_null = true;
            continue;
        }
        if parser.parse_keyword(Keyword::DEFAULT) {
            if parser.parse_keyword(Keyword::NULL) {
                if seen_default_null {
                    return Err("duplicate DEFAULT NULL clause in ADD COLUMN".to_string());
                }
                seen_default_null = true;
                default_null = true;
                continue;
            }
            return Err("ADD COLUMN default values other than NULL are not supported".to_string());
        }
        break;
    }
    Ok(IcebergSchemaChange::AddColumn {
        name,
        data_type,
        default_null,
    })
}

/// Check if SQL looks like ALTER TABLE ... ADD EQUALITY DELETE (...) VALUES ...
pub(crate) fn looks_like_add_equality_delete(sql: &str) -> bool {
    let upper = sql.trim().to_ascii_uppercase();
    upper.starts_with("ALTER TABLE") && upper.contains("ADD EQUALITY DELETE")
}

/// Parse: ALTER TABLE [catalog.db.]table ADD EQUALITY DELETE (k1, k2) VALUES (...)
pub(crate) fn parse_add_equality_delete_sql(sql: &str) -> Result<AddEqualityDeleteStmt, String> {
    const ALTER_TABLE: &str = "ALTER TABLE";
    const ADD_EQ_DELETE: &str = "ADD EQUALITY DELETE";
    const VALUES: &str = "VALUES";

    let upper = sql.to_ascii_uppercase();
    let alter_idx = upper.find(ALTER_TABLE).ok_or("missing ALTER TABLE")?;
    let add_idx = upper
        .find(ADD_EQ_DELETE)
        .ok_or("missing ADD EQUALITY DELETE")?;
    let values_idx = upper[add_idx + ADD_EQ_DELETE.len()..]
        .find(VALUES)
        .map(|idx| add_idx + ADD_EQ_DELETE.len() + idx)
        .ok_or("missing VALUES")?;

    let table_str = sql[alter_idx + ALTER_TABLE.len()..add_idx].trim();
    let table_parts = table_str
        .split('.')
        .map(normalize_identifier)
        .collect::<Result<Vec<_>, _>>()?;
    if table_parts.is_empty() {
        return Err("ADD EQUALITY DELETE requires a table name".to_string());
    }

    let columns_part = sql[add_idx + ADD_EQ_DELETE.len()..values_idx].trim();
    let columns_inner = columns_part
        .strip_prefix('(')
        .and_then(|s| s.strip_suffix(')'))
        .ok_or_else(|| "ADD EQUALITY DELETE requires columns in parentheses".to_string())?;
    let columns = columns_inner
        .split(',')
        .map(normalize_identifier)
        .collect::<Result<Vec<_>, _>>()?;
    if columns.is_empty() {
        return Err("ADD EQUALITY DELETE requires at least one equality column".to_string());
    }

    let values_part = sql[values_idx + VALUES.len()..]
        .trim()
        .trim_end_matches(';');
    if values_part.is_empty() {
        return Err("ADD EQUALITY DELETE VALUES requires at least one row".to_string());
    }
    let fake_sql = format!(
        "INSERT INTO __eq_delete ({}) VALUES {values_part}",
        columns.join(", ")
    );
    let stmt = crate::sql::parser::parse_normalized_sql_raw(&fake_sql)
        .map_err(|e| format!("parse ADD EQUALITY DELETE VALUES: {e}"))?;
    let insert = match stmt {
        sqlparser::ast::Statement::Insert(insert) => insert,
        other => {
            return Err(format!(
                "internal ADD EQUALITY DELETE VALUES parser expected INSERT, got {other:?}"
            ));
        }
    };
    let converted = convert_sqlparser_insert_to_custom(&insert)?;
    let rows = match converted.source {
        InsertSource::Values(rows) => rows,
        other => {
            return Err(format!(
                "ADD EQUALITY DELETE expects literal VALUES rows, got {other:?}"
            ));
        }
    };
    Ok(AddEqualityDeleteStmt {
        table: ObjectName { parts: table_parts },
        columns,
        rows,
    })
}

#[cfg(test)]
mod tests {
    use crate::sql::parser::ast::Literal;

    #[test]
    fn parse_add_equality_delete_values_statement() {
        let stmt = super::parse_add_equality_delete_sql(
            "ALTER TABLE ice.db.orders ADD EQUALITY DELETE (id, category) VALUES (2, 'B'), (4, 'A')",
        )
        .expect("parse");

        assert_eq!(stmt.table.parts, vec!["ice", "db", "orders"]);
        assert_eq!(stmt.columns, vec!["id", "category"]);
        assert_eq!(
            stmt.rows,
            vec![
                vec![Literal::Int(2), Literal::String("B".to_string())],
                vec![Literal::Int(4), Literal::String("A".to_string())],
            ]
        );
    }

    #[test]
    fn parse_alter_iceberg_schema_add_column_default_null() {
        let stmt = super::parse_alter_iceberg_schema_sql(
            "ALTER TABLE ice.db.orders ADD COLUMN discount INT DEFAULT NULL",
        )
        .expect("parse");

        assert_eq!(stmt.table.parts, vec!["ice", "db", "orders"]);
        assert_eq!(
            stmt.change,
            super::IcebergSchemaChange::AddColumn {
                name: "discount".to_string(),
                data_type: crate::sql::parser::ast::SqlType::Int,
                default_null: true,
            }
        );
    }

    #[test]
    fn parse_alter_iceberg_schema_drop_rename_modify() {
        let drop_stmt =
            super::parse_alter_iceberg_schema_sql("ALTER TABLE ice.db.orders DROP COLUMN old_col")
                .expect("drop");
        assert_eq!(
            drop_stmt.change,
            super::IcebergSchemaChange::DropColumn {
                name: "old_col".to_string(),
            }
        );

        let rename_stmt = super::parse_alter_iceberg_schema_sql(
            "ALTER TABLE ice.db.orders RENAME COLUMN old_col TO new_col",
        )
        .expect("rename");
        assert_eq!(
            rename_stmt.change,
            super::IcebergSchemaChange::RenameColumn {
                old_name: "old_col".to_string(),
                new_name: "new_col".to_string(),
            }
        );

        let modify_stmt = super::parse_alter_iceberg_schema_sql(
            "ALTER TABLE ice.db.orders MODIFY COLUMN id BIGINT",
        )
        .expect("modify");
        assert_eq!(
            modify_stmt.change,
            super::IcebergSchemaChange::ModifyColumn {
                name: "id".to_string(),
                new_type: crate::sql::parser::ast::SqlType::BigInt,
            }
        );
    }

    #[test]
    fn parse_alter_iceberg_schema_rejects_unsupported_add_forms() {
        let not_null = super::parse_alter_iceberg_schema_sql(
            "ALTER TABLE ice.db.orders ADD COLUMN discount INT NOT NULL",
        )
        .expect_err("not null should fail");
        assert!(not_null.contains("ADD COLUMN NOT NULL is not supported"));

        let non_null_default = super::parse_alter_iceberg_schema_sql(
            "ALTER TABLE ice.db.orders ADD COLUMN discount INT DEFAULT 1",
        )
        .expect_err("non-null default should fail");
        assert!(non_null_default.contains("default values other than NULL"));
    }

    #[test]
    fn parse_alter_iceberg_schema_probe_matches_only_schema_clauses() {
        for sql in [
            "ALTER TABLE ice.db.orders ADD COLUMN discount INT",
            "ALTER TABLE ice.db.orders DROP COLUMN old_col",
            "ALTER TABLE ice.db.orders RENAME COLUMN old_col TO new_col",
            "ALTER TABLE ice.db.orders MODIFY COLUMN id BIGINT",
        ] {
            assert!(
                super::looks_like_alter_iceberg_schema(sql),
                "expected schema DDL probe to match {sql}"
            );
        }

        for sql in [
            "ALTER TABLE ice.db.orders ADD FILES FROM 's3://bucket/path'",
            "ALTER TABLE ice.db.orders ADD EQUALITY DELETE (id) VALUES (1)",
            "ALTER TABLE ice.db.orders SET COMMENT = 'ADD COLUMN c INT'",
            "ALTER TABLE ice.db.orders /* ADD COLUMN c INT */ ADD FILES FROM 's3://bucket/path'",
            "ALTER TABLE ice.db.orders ADD PARTITION p1 VALUES LESS THAN (10)",
        ] {
            assert!(
                !super::looks_like_alter_iceberg_schema(sql),
                "expected schema DDL probe not to match {sql}"
            );
        }
    }

    #[test]
    fn parse_alter_iceberg_schema_rejects_trailing_unsupported_syntax() {
        let err = super::parse_alter_iceberg_schema_sql(
            "ALTER TABLE ice.db.orders ADD COLUMN c INT COMMENT 'x'",
        )
        .expect_err("comment should fail");
        assert!(err.contains("unsupported trailing ALTER TABLE schema tokens"));
    }

    #[test]
    fn parse_alter_iceberg_schema_rejects_duplicate_add_column_attributes() {
        let duplicate_null = super::parse_alter_iceberg_schema_sql(
            "ALTER TABLE ice.db.orders ADD COLUMN c INT NULL NULL",
        )
        .expect_err("duplicate null should fail");
        assert!(duplicate_null.contains("duplicate NULL"));

        let duplicate_default = super::parse_alter_iceberg_schema_sql(
            "ALTER TABLE ice.db.orders ADD COLUMN c INT DEFAULT NULL DEFAULT NULL",
        )
        .expect_err("duplicate default should fail");
        assert!(duplicate_default.contains("duplicate DEFAULT NULL"));
    }
}
