// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Native Iceberg materialized-view analysis and display helpers.

use std::collections::HashSet;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use arrow::array::{ArrayRef, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;

use crate::catalog::identifier::normalize_identifier;
use crate::engine::StandaloneState;
use crate::engine::mv::agg_state::mv_shape::AggregateMvShape;
use crate::engine::mv::agg_state::physical_column::StarRocksPhysicalColumn;
use crate::engine::mv::agg_state::sql_type::arrow_data_type_to_sql_type;
use crate::engine::mv::lifecycle::MvListRow;
use crate::engine::query_prep::drop_local_table_registration_if_exists;
use crate::meta::MetaReadTxn;
use crate::meta::repository::mv::MvRefreshState;
use crate::mv::model::MvStorageEngine;
use crate::mv::persistence::definition::{StoredMvDefinition, StoredMvRefreshPolicy};
use crate::runtime::query_result::{QueryResult, QueryResultColumn, record_batch_to_chunk};
use crate::sql::analysis::{OutputColumn, QueryBody, ResolvedQuery};
use crate::sql::column_id::ColumnId;
use crate::sql::parser::ast::{
    IcebergPartitionFieldExpr, MaterializedViewDistribution, ObjectName, ShowMaterializedViewsStmt,
    TableColumnDef,
};

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum ResolvedTableRef {
    Iceberg {
        catalog: String,
        namespace: String,
        table: String,
    },
    StarRocks {
        database: String,
        table: String,
    },
}

pub(crate) fn validate_unique_aggregate_physical_column_names(
    physical_columns: &[StarRocksPhysicalColumn],
) -> Result<(), String> {
    let mut names = HashSet::with_capacity(physical_columns.len());
    for column in physical_columns {
        let normalized = normalize_identifier(&column.column.name)?;
        if !names.insert(normalized.clone()) {
            return Err(format!(
                "aggregate MV physical column name collision: hidden column name collision or duplicate physical column `{normalized}`"
            ));
        }
    }
    Ok(())
}

/// Lightweight projection of the iceberg base table that
/// `validate_ivm_primary_key` needs. Built once at the top of `create_mv`
/// from the loaded iceberg table; passing this struct keeps validation
/// pure and easy to unit-test.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct BaseColumnDescriptor {
    pub name: String,
    pub data_type: DataType,
    /// Uppercased SQL type as the analyzer/iceberg-schema mapper produced
    /// it (e.g. `BIGINT`, `STRING`, `DECIMAL(18,2)`, `ARRAY<STRING>`).
    pub sql_type: String,
    pub nullable: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct BaseTableDescriptor {
    pub format_version: i32,
    pub columns: Vec<BaseColumnDescriptor>,
}

/// Validate that a parsed `PRIMARY KEY (col, ...)` clause on a CREATE
/// MATERIALIZED VIEW statement satisfies the IVM Phase-2 contract:
///
/// 1. The base table is iceberg format-version 2.
/// 2. Every PK column exists on the base table.
/// 3. Every PK column is NOT NULL on the base table.
/// 4. Every PK column has a hashable scalar type.
///
/// Errors fail fast in declared column order — the first mismatch wins.
/// Returns `Ok(())` on success and discards the PK list (PR-1 does not
/// persist it; PR-3 will).
pub(crate) fn validate_ivm_primary_key(
    pk_columns: &[String],
    base: &BaseTableDescriptor,
) -> Result<(), crate::connector::iceberg::changes::ChangeError> {
    use crate::connector::iceberg::changes::ChangeError;

    if base.format_version != 2 && base.format_version != 3 {
        return Err(ChangeError::IcebergFormatUnsupported {
            format_version: base.format_version,
        });
    }
    for pk in pk_columns {
        let col = base
            .columns
            .iter()
            .find(|c| c.name.eq_ignore_ascii_case(pk))
            .ok_or_else(|| ChangeError::PrimaryKeyMissingFromBase { pk_col: pk.clone() })?;
        if col.nullable {
            return Err(ChangeError::PrimaryKeyNullable {
                pk_col: col.name.clone(),
            });
        }
        if !is_hashable_pk_type(&col.sql_type) {
            return Err(ChangeError::PrimaryKeyTypeUnsupported {
                pk_col: col.name.clone(),
                ty: col.sql_type.clone(),
            });
        }
    }
    Ok(())
}

/// Hashable scalar-type predicate for IVM Phase-2 PRIMARY KEY columns.
/// Accepts: BIGINT, INT, SMALLINT, TINYINT, STRING, VARCHAR, DATE,
/// DATETIME, DECIMAL (with or without precision/scale).
/// Rejects: BOOLEAN, FLOAT, DOUBLE, ARRAY, MAP, STRUCT, JSON.
fn is_hashable_pk_type(sql_type: &str) -> bool {
    let upper = sql_type.to_ascii_uppercase();
    let head = upper.split(['(', '<']).next().unwrap_or("").trim();
    matches!(
        head,
        "BIGINT"
            | "INT"
            | "INTEGER"
            | "SMALLINT"
            | "TINYINT"
            | "STRING"
            | "VARCHAR"
            | "CHAR"
            | "DATE"
            | "DATETIME"
            | "TIMESTAMP"
            | "DECIMAL"
    )
}

/// Map an Arrow `DataType` to the SQL head token that
/// `is_hashable_pk_type` recognizes. Returns the token only — no
/// precision/scale or element-type tail. Anything not on the accepted
/// list falls through to the Arrow Debug form (e.g. `Float32`,
/// `List(...)`), which `is_hashable_pk_type` will then reject.
fn arrow_data_type_pk_head(dt: &arrow::datatypes::DataType) -> String {
    use arrow::datatypes::DataType;
    match dt {
        DataType::Int8 => "TINYINT".to_string(),
        DataType::Int16 => "SMALLINT".to_string(),
        DataType::Int32 => "INT".to_string(),
        DataType::Int64 => "BIGINT".to_string(),
        DataType::Utf8 | DataType::LargeUtf8 => "STRING".to_string(),
        DataType::Decimal128(_, _) | DataType::Decimal256(_, _) => "DECIMAL".to_string(),
        DataType::Date32 | DataType::Date64 => "DATE".to_string(),
        DataType::Timestamp(_, _) => "DATETIME".to_string(),
        // Explicitly unsupported as PK: floats (NaN equality), booleans
        // (degenerate cardinality), composites (no stable hash). Fall
        // through to Debug form so is_hashable_pk_type rejects them.
        other => format!("{other:?}"),
    }
}

/// Build the `BaseTableDescriptor` projection from an already-loaded
/// iceberg table. Used by `create_mv` and `create_iceberg_mv` before
/// invoking `validate_ivm_primary_key`.
pub(crate) fn descriptor_from_loaded(
    loaded: &crate::connector::iceberg::catalog::IcebergLoadedTable,
) -> BaseTableDescriptor {
    let format_version = loaded.table.metadata().format_version() as i32;
    let columns = loaded
        .columns
        .iter()
        .map(|col| BaseColumnDescriptor {
            name: col.name.clone(),
            data_type: col.data_type.clone(),
            sql_type: arrow_data_type_pk_head(&col.data_type),
            nullable: col.nullable,
        })
        .collect();
    BaseTableDescriptor {
        format_version,
        columns,
    }
}
pub(crate) fn list_mv_rows(
    state: &Arc<StandaloneState>,
    current_catalog: Option<&str>,
    stmt: &ShowMaterializedViewsStmt,
    storage_filter: Option<MvStorageEngine>,
) -> Result<Vec<MvListRow>, String> {
    let Some(provider) = state.metadata_provider.as_ref() else {
        return Ok(vec![]);
    };
    // Share a single read transaction across `list_definitions` and every
    // per-row `dependency_display_for_mv` lookup. This avoids M+1 RAII
    // open/close cycles for M materialized views and, more importantly,
    // gives the entire SHOW MATERIALIZED VIEWS result a consistent
    // metadata snapshot: concurrent CREATE/DROP MV writers cannot make
    // dependency display drift away from the MV list we just read.
    let read = provider
        .begin_read()
        .map_err(|e| format!("open metadata read transaction failed: {e}"))?;
    let definitions = state
        .mv_repo
        .list_definitions(read.as_ref())
        .map_err(|e| format!("load materialized view definitions failed: {e}"))?;
    let now_ms = now_ms();

    let mut rows = Vec::new();
    for mv in &definitions {
        if let Some(filter) = storage_filter
            && !mv.storage_engine.eq_ignore_ascii_case(filter.as_sql_str())
        {
            continue;
        }
        let engine = MvStorageEngine::from_sql_str(&mv.storage_engine)?;
        let (refresh_state, retry_after_time) =
            refresh_status_for_mv(state, read.as_ref(), mv, now_ms)?;
        if engine != MvStorageEngine::Iceberg {
            continue;
        }
        let Some(target_catalog) = mv.target_catalog.as_deref() else {
            continue;
        };
        if let Some(current_catalog) = current_catalog
            && !target_catalog.eq_ignore_ascii_case(current_catalog)
        {
            continue;
        };
        let Some(target_namespace) = mv.target_namespace.clone() else {
            continue;
        };
        if let Some(filter_db) = stmt.database.as_deref()
            && !target_namespace.eq_ignore_ascii_case(filter_db)
        {
            continue;
        }
        let Some(target_table) = mv.target_table.clone() else {
            continue;
        };
        rows.push(MvListRow {
            name: target_table,
            database: target_namespace,
            storage_engine: mv.storage_engine.clone(),
            refresh_mode: mv.refresh_policy.as_sql_str().to_string(),
            last_refresh_time: mv.last_refresh_ms.map(|value| value.to_string()),
            last_refresh_rows: mv.last_refresh_rows.map(|value| value.to_string()),
            base_tables: mv.base_table_refs.join(", "),
            select_text: mv.select_sql.clone(),
            dependencies: dependency_display_for_mv(state, read.as_ref(), mv.mv_id)?,
            refresh_paused: mv.refresh_paused.to_string(),
            next_refresh_time: mv.next_refresh_after_ms.map(|value| value.to_string()),
            last_scheduler_error: mv.last_scheduler_error.clone(),
            max_staleness_ms: mv.max_staleness_ms.map(|value| value.to_string()),
            refresh_state,
            retry_after_time,
        });
    }
    Ok(rows)
}

fn refresh_status_for_mv(
    state: &Arc<StandaloneState>,
    read: &dyn MetaReadTxn,
    mv: &StoredMvDefinition,
    now_ms: i64,
) -> Result<(String, Option<String>), String> {
    let retry_after_time = mv
        .last_scheduler_error
        .as_ref()
        .and_then(|_| mv.next_refresh_after_ms)
        .filter(|next| *next > now_ms)
        .map(|value| value.to_string());
    if mv.refresh_paused {
        return Ok(("PAUSED".to_string(), retry_after_time));
    }
    if let Some(refresh_id) = mv.active_refresh_id {
        let refresh = state
            .mv_repo
            .load_refresh(read, refresh_id)
            .map_err(|e| format!("load active MV refresh failed: {e}"))?;
        if refresh
            .as_ref()
            .map(|refresh| refresh.state == MvRefreshState::CommitUnknown)
            .unwrap_or(false)
        {
            return Ok(("BLOCKED_RECOVERY".to_string(), retry_after_time));
        }
        return Ok(("RUNNING".to_string(), retry_after_time));
    }
    if mv.refresh_in_progress {
        return Ok(("RUNNING".to_string(), retry_after_time));
    }
    if mv
        .last_scheduler_error
        .as_ref()
        .map(|err| err.trim_start().starts_with("USER_ERROR: "))
        .unwrap_or(false)
    {
        return Ok(("FAILED_USER_ERROR".to_string(), retry_after_time));
    }
    if mv.last_scheduler_error.is_some()
        && mv
            .next_refresh_after_ms
            .map(|next| next > now_ms)
            .unwrap_or(false)
    {
        return Ok(("FAILED_BACKOFF".to_string(), retry_after_time));
    }
    if matches!(mv.refresh_policy, StoredMvRefreshPolicy::Manual) {
        return Ok(("MANUAL".to_string(), retry_after_time));
    }
    if mv
        .next_refresh_after_ms
        .map(|next| next > now_ms)
        .unwrap_or(false)
    {
        Ok(("SUCCEEDED".to_string(), retry_after_time))
    } else {
        Ok(("PENDING".to_string(), retry_after_time))
    }
}

/// Render the dependency-column text for a single MV row. Callers must pass
/// the shared read transaction opened by `list_mv_rows` so that every row
/// observes the same metadata snapshot and we avoid M+1 transaction opens.
fn dependency_display_for_mv(
    state: &Arc<StandaloneState>,
    read: &dyn MetaReadTxn,
    mv_id: i64,
) -> Result<String, String> {
    let dependencies = state
        .mv_repo
        .list_dependencies_by_downstream(read, mv_id)
        .map_err(|e| format!("load MV dependencies for display failed: {e}"))?;
    Ok(dependencies
        .iter()
        .map(|dep| dep.upstream.display_name())
        .collect::<Vec<_>>()
        .join(", "))
}

#[derive(Clone, Debug)]
pub(crate) struct MvAnalysis {
    pub resolved_refs: Vec<ResolvedTableRef>,
    pub output_columns: Vec<OutputColumn>,
    pub resolved_query: ResolvedQuery,
}

pub(crate) fn analyze_mv_select(
    state: &Arc<StandaloneState>,
    current_catalog: Option<&str>,
    current_database: &str,
    query: &sqlparser::ast::Query,
) -> Result<MvAnalysis, String> {
    validate_mv_select_raw_query_clauses(query)?;
    let resolved_refs = collect_table_refs_from_query(query, current_catalog, current_database);
    let mut analyzed_query = query.clone();
    register_iceberg_tables_for_mv_analysis(state, &resolved_refs)?;
    if has_three_part_refs(&resolved_refs) {
        crate::sql::parser::query_refs::strip_catalog_from_three_part_names(&mut analyzed_query);
    }
    let catalog = state
        .catalog_service
        .local()
        .read()
        .expect("standalone catalog read lock");
    let (resolved, _, _factory) =
        crate::sql::analyzer::analyze(&analyzed_query, &*catalog, current_database)?;
    drop(catalog);

    let mut output_columns = resolved.output_columns.clone();
    if output_columns.is_empty() {
        output_columns = resolved_output_columns_from_body(&resolved);
    }

    Ok(MvAnalysis {
        resolved_refs,
        output_columns,
        resolved_query: resolved,
    })
}

fn validate_mv_select_raw_query_clauses(query: &sqlparser::ast::Query) -> Result<(), String> {
    if query.with.is_some() {
        return Err(unsupported_mv_query_clause("WITH"));
    }
    if query.order_by.is_some() {
        return Err(unsupported_mv_query_clause("ORDER BY"));
    }
    if query.limit_clause.is_some() {
        return Err(unsupported_mv_query_clause("LIMIT or OFFSET"));
    }
    if query.fetch.is_some() {
        return Err(unsupported_mv_query_clause("FETCH"));
    }
    if !query.locks.is_empty() {
        return Err(unsupported_mv_query_clause("locking clauses"));
    }
    if query.for_clause.is_some() {
        return Err(unsupported_mv_query_clause("FOR clauses"));
    }
    if query.settings.is_some() {
        return Err(unsupported_mv_query_clause("SETTINGS"));
    }
    if query.format_clause.is_some() {
        return Err(unsupported_mv_query_clause("FORMAT"));
    }
    if !query.pipe_operators.is_empty() {
        return Err(unsupported_mv_query_clause("pipe operators"));
    }
    validate_mv_select_raw_clauses_in_set_expr(query.body.as_ref())
}

fn validate_mv_select_raw_clauses_in_set_expr(
    expr: &sqlparser::ast::SetExpr,
) -> Result<(), String> {
    match expr {
        sqlparser::ast::SetExpr::Select(select) => {
            validate_mv_select_raw_select_clauses(select)?;
            for from in &select.from {
                validate_mv_select_raw_clauses_in_table_with_joins(from)?;
            }
            Ok(())
        }
        sqlparser::ast::SetExpr::SetOperation { left, right, .. } => {
            validate_mv_select_raw_clauses_in_set_expr(left.as_ref())?;
            validate_mv_select_raw_clauses_in_set_expr(right.as_ref())
        }
        sqlparser::ast::SetExpr::Query(query) => validate_mv_select_raw_query_clauses(query),
        sqlparser::ast::SetExpr::Values(_)
        | sqlparser::ast::SetExpr::Insert(_)
        | sqlparser::ast::SetExpr::Update(_)
        | sqlparser::ast::SetExpr::Delete(_)
        | sqlparser::ast::SetExpr::Merge(_)
        | sqlparser::ast::SetExpr::Table(_) => Ok(()),
    }
}

fn validate_mv_select_raw_select_clauses(select: &sqlparser::ast::Select) -> Result<(), String> {
    if select.select_modifiers.is_some() {
        return Err(unsupported_mv_select_clause("SELECT modifiers"));
    }
    if select.top.is_some() {
        return Err(unsupported_mv_select_clause("TOP"));
    }
    if select.exclude.is_some() {
        return Err(unsupported_mv_select_clause("EXCLUDE"));
    }
    if select.into.is_some() {
        return Err(unsupported_mv_select_clause("SELECT INTO"));
    }
    if !select.lateral_views.is_empty() {
        return Err(unsupported_mv_select_clause("LATERAL VIEW"));
    }
    if select.prewhere.is_some() {
        return Err(unsupported_mv_select_clause("PREWHERE"));
    }
    if !select.connect_by.is_empty() {
        return Err(unsupported_mv_select_clause("CONNECT BY"));
    }
    if !select.cluster_by.is_empty() {
        return Err(unsupported_mv_select_clause("CLUSTER BY"));
    }
    if !select.distribute_by.is_empty() {
        return Err(unsupported_mv_select_clause("DISTRIBUTE BY"));
    }
    if !select.sort_by.is_empty() {
        return Err(unsupported_mv_select_clause("SORT BY"));
    }
    if !select.named_window.is_empty() {
        return Err(unsupported_mv_select_clause("named WINDOW clauses"));
    }
    if select.qualify.is_some() {
        return Err(unsupported_mv_select_clause("QUALIFY"));
    }
    if select.value_table_mode.is_some() {
        return Err(unsupported_mv_select_clause("SELECT AS VALUE or STRUCT"));
    }
    Ok(())
}

fn validate_mv_select_raw_clauses_in_table_with_joins(
    table: &sqlparser::ast::TableWithJoins,
) -> Result<(), String> {
    validate_mv_select_raw_clauses_in_factor(&table.relation)?;
    for join in &table.joins {
        validate_mv_select_raw_clauses_in_factor(&join.relation)?;
    }
    Ok(())
}

fn validate_mv_select_raw_clauses_in_factor(
    factor: &sqlparser::ast::TableFactor,
) -> Result<(), String> {
    match factor {
        sqlparser::ast::TableFactor::Table {
            args,
            with_hints,
            version,
            with_ordinality,
            partitions,
            json_path,
            sample,
            index_hints,
            ..
        } => {
            if args.is_some() {
                return Err(unsupported_mv_from_clause("table function arguments"));
            }
            if !with_hints.is_empty() {
                return Err(unsupported_mv_from_clause("table hints"));
            }
            if version.is_some() {
                return Err(unsupported_mv_from_clause("table version qualifiers"));
            }
            if *with_ordinality {
                return Err(unsupported_mv_from_clause("WITH ORDINALITY"));
            }
            if !partitions.is_empty() {
                return Err(unsupported_mv_from_clause("partition selection"));
            }
            if json_path.is_some() {
                return Err(unsupported_mv_from_clause("JSON path table access"));
            }
            if sample.is_some() {
                return Err(unsupported_mv_from_clause("TABLESAMPLE"));
            }
            if !index_hints.is_empty() {
                return Err(unsupported_mv_from_clause("index hints"));
            }
            Ok(())
        }
        sqlparser::ast::TableFactor::Derived {
            lateral,
            subquery,
            sample,
            ..
        } => {
            if *lateral {
                return Err(unsupported_mv_from_clause("LATERAL derived tables"));
            }
            if sample.is_some() {
                return Err(unsupported_mv_from_clause("TABLESAMPLE"));
            }
            validate_mv_select_raw_query_clauses(subquery)
        }
        sqlparser::ast::TableFactor::NestedJoin {
            table_with_joins, ..
        } => validate_mv_select_raw_clauses_in_table_with_joins(table_with_joins),
        sqlparser::ast::TableFactor::Pivot { table, .. }
        | sqlparser::ast::TableFactor::Unpivot { table, .. }
        | sqlparser::ast::TableFactor::MatchRecognize { table, .. } => {
            validate_mv_select_raw_clauses_in_factor(table)
        }
        sqlparser::ast::TableFactor::TableFunction { .. }
        | sqlparser::ast::TableFactor::Function { .. }
        | sqlparser::ast::TableFactor::UNNEST { .. }
        | sqlparser::ast::TableFactor::JsonTable { .. }
        | sqlparser::ast::TableFactor::OpenJsonTable { .. }
        | sqlparser::ast::TableFactor::XmlTable { .. }
        | sqlparser::ast::TableFactor::SemanticView { .. } => {
            Err(unsupported_mv_from_clause("table functions"))
        }
    }
}

fn unsupported_mv_query_clause(clause: &str) -> String {
    format!("materialized view SELECT does not support {clause}")
}

fn unsupported_mv_select_clause(clause: &str) -> String {
    format!("materialized view SELECT does not support {clause}")
}

fn unsupported_mv_from_clause(clause: &str) -> String {
    format!("materialized view SELECT does not support {clause} in FROM")
}

pub(crate) fn canonicalize_iceberg_mv_select_query(
    query: &sqlparser::ast::Query,
    current_catalog: Option<&str>,
    current_database: &str,
) -> sqlparser::ast::Query {
    let mut query = query.clone();
    let Some(catalog) = current_catalog else {
        return query;
    };
    qualify_current_catalog_refs_in_query(
        &mut query,
        &catalog.to_ascii_lowercase(),
        &current_database.to_ascii_lowercase(),
    );
    query
}

fn qualify_current_catalog_refs_in_query(
    query: &mut sqlparser::ast::Query,
    catalog: &str,
    current_database: &str,
) {
    if let Some(with) = &mut query.with {
        for cte in &mut with.cte_tables {
            qualify_current_catalog_refs_in_set_expr(
                cte.query.body.as_mut(),
                catalog,
                current_database,
            );
        }
    }
    qualify_current_catalog_refs_in_set_expr(query.body.as_mut(), catalog, current_database);
}

fn qualify_current_catalog_refs_in_set_expr(
    expr: &mut sqlparser::ast::SetExpr,
    catalog: &str,
    current_database: &str,
) {
    match expr {
        sqlparser::ast::SetExpr::Select(select) => {
            for from in &mut select.from {
                qualify_current_catalog_refs_in_factor(
                    &mut from.relation,
                    catalog,
                    current_database,
                );
                for join in &mut from.joins {
                    qualify_current_catalog_refs_in_factor(
                        &mut join.relation,
                        catalog,
                        current_database,
                    );
                }
            }
        }
        sqlparser::ast::SetExpr::SetOperation { left, right, .. } => {
            qualify_current_catalog_refs_in_set_expr(left.as_mut(), catalog, current_database);
            qualify_current_catalog_refs_in_set_expr(right.as_mut(), catalog, current_database);
        }
        sqlparser::ast::SetExpr::Query(query) => {
            qualify_current_catalog_refs_in_set_expr(
                query.body.as_mut(),
                catalog,
                current_database,
            );
        }
        _ => {}
    }
}

fn qualify_current_catalog_refs_in_factor(
    factor: &mut sqlparser::ast::TableFactor,
    catalog: &str,
    current_database: &str,
) {
    match factor {
        sqlparser::ast::TableFactor::Table { name, .. } => {
            let parts = name
                .0
                .iter()
                .filter_map(|part| match part {
                    sqlparser::ast::ObjectNamePart::Identifier(ident) => {
                        Some(ident.value.to_ascii_lowercase())
                    }
                    _ => None,
                })
                .collect::<Vec<_>>();
            let qualified = match parts.as_slice() {
                [table] => Some((
                    catalog.to_string(),
                    current_database.to_string(),
                    table.clone(),
                )),
                [namespace, table] => Some((catalog.to_string(), namespace.clone(), table.clone())),
                _ => None,
            };
            if let Some((catalog, namespace, table)) = qualified {
                name.0 = vec![
                    sqlparser::ast::ObjectNamePart::Identifier(sqlparser::ast::Ident::new(catalog)),
                    sqlparser::ast::ObjectNamePart::Identifier(sqlparser::ast::Ident::new(
                        namespace,
                    )),
                    sqlparser::ast::ObjectNamePart::Identifier(sqlparser::ast::Ident::new(table)),
                ];
            }
        }
        sqlparser::ast::TableFactor::Derived { subquery, .. } => {
            qualify_current_catalog_refs_in_set_expr(
                subquery.body.as_mut(),
                catalog,
                current_database,
            );
        }
        _ => {}
    }
}

fn register_iceberg_tables_for_mv_analysis(
    state: &Arc<StandaloneState>,
    resolved_refs: &[ResolvedTableRef],
) -> Result<(), String> {
    let (catalog_backend, table_source) = {
        let registry = state
            .connectors
            .read()
            .expect("standalone connector registry read lock");
        (
            registry.catalog_backend("iceberg")?,
            registry.table_source("iceberg")?,
        )
    };

    for table_ref in resolved_refs {
        let ResolvedTableRef::Iceberg {
            catalog,
            namespace,
            table,
        } = table_ref
        else {
            continue;
        };
        drop_local_table_registration_if_exists(state, namespace, table)?;
        let resolved = catalog_backend
            .load_table_for_read(catalog, namespace, table)
            .map_err(|err| {
                format!("load iceberg table {catalog}.{namespace}.{table} failed: {err}")
            })?;
        let mut table_def = table_source.build_table_def(&resolved)?;
        table_def.name = table.clone();
        let mut local_catalog = state
            .catalog_service
            .local()
            .write()
            .map_err(|e| format!("standalone catalog write lock: {e}"))?;
        local_catalog.create_database(namespace)?;
        local_catalog.register(namespace, table_def)?;
    }
    Ok(())
}

fn resolved_output_columns_from_body(resolved: &ResolvedQuery) -> Vec<OutputColumn> {
    match &resolved.body {
        QueryBody::Select(select) => select
            .projection
            .iter()
            .map(|item| OutputColumn {
                column_id: ColumnId::UNSET,
                name: item.output_name.clone(),
                data_type: item.expr.data_type.clone(),
                nullable: item.expr.nullable,
                is_internal: false,
            })
            .collect(),
        _ => resolved.output_columns.clone(),
    }
}

fn validate_distribution_columns(
    distribution: &MaterializedViewDistribution,
    output_columns: &[OutputColumn],
) -> Result<(), String> {
    for column in &distribution.hash_columns {
        let exists = output_columns
            .iter()
            .any(|output| output.name.eq_ignore_ascii_case(column));
        if !exists {
            return Err(format!(
                "DISTRIBUTED BY column `{column}` not in MV output schema"
            ));
        }
    }
    Ok(())
}

fn validate_aggregate_distribution_columns(
    distribution: &MaterializedViewDistribution,
    shape: &AggregateMvShape,
) -> Result<(), String> {
    let group_key_outputs = shape
        .group_keys
        .iter()
        .map(|group_key| normalize_identifier(&group_key.output_name))
        .collect::<Result<HashSet<_>, _>>()?;
    for column in &distribution.hash_columns {
        let normalized = normalize_identifier(column)?;
        if !group_key_outputs.contains(&normalized) {
            return Err(format!(
                "aggregate MV distribution column `{column}` must be a GROUP BY key output column; DISTRIBUTED BY HASH for aggregate MV can only reference GROUP BY keys"
            ));
        }
    }
    Ok(())
}

pub(crate) fn resolve_mv_name(
    name: &ObjectName,
    current_database: &str,
) -> Result<(String, String), String> {
    match name.parts.as_slice() {
        [table] => Ok((
            normalize_identifier(current_database)?,
            normalize_identifier(table)?,
        )),
        [database, table] => Ok((
            normalize_identifier(database)?,
            normalize_identifier(table)?,
        )),
        [catalog, database, table] => {
            let catalog = normalize_identifier(catalog)?;
            if catalog != "default_catalog" {
                return Err(format!(
                    "materialized view name catalog must be `default_catalog`, got `{catalog}`"
                ));
            }
            Ok((
                normalize_identifier(database)?,
                normalize_identifier(table)?,
            ))
        }
        _ => Err(format!(
            "materialized view name must be `<name>`, `<db>.<name>`, or `default_catalog.<db>.<name>`; got `{}`",
            name.parts.join(".")
        )),
    }
}

pub(crate) fn validate_mv_partition_columns(
    partition_by: Option<&[IcebergPartitionFieldExpr]>,
    output_columns: &[OutputColumn],
) -> Result<(), String> {
    let Some(partition_by) = partition_by else {
        return Ok(());
    };
    let output_names = output_columns
        .iter()
        .map(|column| normalize_identifier(&column.name))
        .collect::<Result<HashSet<_>, _>>()?;
    for field in partition_by {
        let column = mv_partition_source_column(field);
        let normalized = normalize_identifier(column)?;
        if !output_names.contains(&normalized) {
            return Err(format!(
                "materialized view PARTITION BY column `{column}` must be an output column"
            ));
        }
    }
    Ok(())
}

fn validate_starrocks_mv_partition_columns(
    partition_by: Option<&[IcebergPartitionFieldExpr]>,
    output_columns: &[OutputColumn],
) -> Result<(), String> {
    if let Some(fields) = partition_by {
        for field in fields {
            if !matches!(field, IcebergPartitionFieldExpr::Identity { .. }) {
                return Err(
                    "StarRocks table materialized view PARTITION BY only supports identity columns"
                        .to_string(),
                );
            }
        }
    }
    validate_mv_partition_columns(partition_by, output_columns)
}

fn mv_partition_source_column(field: &IcebergPartitionFieldExpr) -> &str {
    match field {
        IcebergPartitionFieldExpr::Identity { column }
        | IcebergPartitionFieldExpr::Year { column }
        | IcebergPartitionFieldExpr::Month { column }
        | IcebergPartitionFieldExpr::Day { column }
        | IcebergPartitionFieldExpr::Hour { column }
        | IcebergPartitionFieldExpr::Bucket { column, .. }
        | IcebergPartitionFieldExpr::Truncate { column, .. }
        | IcebergPartitionFieldExpr::Void { column } => column,
    }
}

fn collect_table_refs_from_query(
    query: &sqlparser::ast::Query,
    current_catalog: Option<&str>,
    current_database: &str,
) -> Vec<ResolvedTableRef> {
    let mut refs = Vec::new();
    if let Some(with) = &query.with {
        for cte in &with.cte_tables {
            collect_table_refs_from_set_expr(
                cte.query.body.as_ref(),
                current_catalog,
                current_database,
                &mut refs,
            );
        }
    }
    collect_table_refs_from_set_expr(
        query.body.as_ref(),
        current_catalog,
        current_database,
        &mut refs,
    );
    refs
}

fn collect_table_refs_from_set_expr(
    expr: &sqlparser::ast::SetExpr,
    current_catalog: Option<&str>,
    current_database: &str,
    refs: &mut Vec<ResolvedTableRef>,
) {
    match expr {
        sqlparser::ast::SetExpr::Select(select) => {
            for from in &select.from {
                collect_table_refs_from_factor(
                    &from.relation,
                    current_catalog,
                    current_database,
                    refs,
                );
                for join in &from.joins {
                    collect_table_refs_from_factor(
                        &join.relation,
                        current_catalog,
                        current_database,
                        refs,
                    );
                }
            }
        }
        sqlparser::ast::SetExpr::SetOperation { left, right, .. } => {
            collect_table_refs_from_set_expr(left, current_catalog, current_database, refs);
            collect_table_refs_from_set_expr(right, current_catalog, current_database, refs);
        }
        sqlparser::ast::SetExpr::Query(query) => {
            collect_table_refs_from_set_expr(
                query.body.as_ref(),
                current_catalog,
                current_database,
                refs,
            );
        }
        _ => {}
    }
}

fn collect_table_refs_from_factor(
    factor: &sqlparser::ast::TableFactor,
    current_catalog: Option<&str>,
    current_database: &str,
    refs: &mut Vec<ResolvedTableRef>,
) {
    match factor {
        sqlparser::ast::TableFactor::Table { name, .. } => {
            let parts: Vec<String> = name
                .0
                .iter()
                .filter_map(|part| match part {
                    sqlparser::ast::ObjectNamePart::Identifier(ident) => {
                        Some(ident.value.to_ascii_lowercase())
                    }
                    _ => None,
                })
                .collect();
            let resolved = match parts.as_slice() {
                [catalog, namespace, table] => ResolvedTableRef::Iceberg {
                    catalog: catalog.clone(),
                    namespace: namespace.clone(),
                    table: table.clone(),
                },
                [table] => match current_catalog {
                    Some(catalog) => ResolvedTableRef::Iceberg {
                        catalog: catalog.to_ascii_lowercase(),
                        namespace: current_database.to_ascii_lowercase(),
                        table: table.clone(),
                    },
                    None => ResolvedTableRef::StarRocks {
                        database: current_database.to_ascii_lowercase(),
                        table: table.clone(),
                    },
                },
                [database, table] => match current_catalog {
                    Some(catalog) => ResolvedTableRef::Iceberg {
                        catalog: catalog.to_ascii_lowercase(),
                        namespace: database.clone(),
                        table: table.clone(),
                    },
                    None => ResolvedTableRef::StarRocks {
                        database: database.clone(),
                        table: table.clone(),
                    },
                },
                _ => {
                    let rendered = parts.join(".");
                    ResolvedTableRef::StarRocks {
                        database: current_database.to_ascii_lowercase(),
                        table: rendered,
                    }
                }
            };
            if !refs.contains(&resolved) {
                refs.push(resolved);
            }
        }
        sqlparser::ast::TableFactor::Derived { subquery, .. } => {
            if let Some(with) = &subquery.with {
                for cte in &with.cte_tables {
                    collect_table_refs_from_set_expr(
                        cte.query.body.as_ref(),
                        current_catalog,
                        current_database,
                        refs,
                    );
                }
            }
            collect_table_refs_from_set_expr(
                subquery.body.as_ref(),
                current_catalog,
                current_database,
                refs,
            );
        }
        _ => {}
    }
}

fn has_three_part_refs(resolved_refs: &[ResolvedTableRef]) -> bool {
    resolved_refs
        .iter()
        .any(|table_ref| matches!(table_ref, ResolvedTableRef::Iceberg { .. }))
}

pub(crate) fn output_column_to_table_column(
    column: &OutputColumn,
) -> Result<TableColumnDef, String> {
    Ok(TableColumnDef {
        name: column.name.clone(),
        data_type: arrow_data_type_to_sql_type(&column.data_type)?,
        nullable: column.nullable,
        aggregation: None,
        default: None,
    })
}

pub(crate) fn build_mv_rows_result(rows: &[MvListRow]) -> Result<QueryResult, String> {
    let columns = vec![
        QueryResultColumn {
            name: "Name".to_string(),
            data_type: DataType::Utf8,
            nullable: false,
            logical_type: None,
        },
        QueryResultColumn {
            name: "Database".to_string(),
            data_type: DataType::Utf8,
            nullable: false,
            logical_type: None,
        },
        QueryResultColumn {
            name: "StorageEngine".to_string(),
            data_type: DataType::Utf8,
            nullable: false,
            logical_type: None,
        },
        QueryResultColumn {
            name: "RefreshMode".to_string(),
            data_type: DataType::Utf8,
            nullable: false,
            logical_type: None,
        },
        QueryResultColumn {
            name: "LastRefreshTime".to_string(),
            data_type: DataType::Utf8,
            nullable: true,
            logical_type: None,
        },
        QueryResultColumn {
            name: "LastRefreshRows".to_string(),
            data_type: DataType::Utf8,
            nullable: true,
            logical_type: None,
        },
        QueryResultColumn {
            name: "BaseTables".to_string(),
            data_type: DataType::Utf8,
            nullable: false,
            logical_type: None,
        },
        QueryResultColumn {
            name: "SelectText".to_string(),
            data_type: DataType::Utf8,
            nullable: false,
            logical_type: None,
        },
        QueryResultColumn {
            name: "Dependencies".to_string(),
            data_type: DataType::Utf8,
            nullable: false,
            logical_type: None,
        },
        QueryResultColumn {
            name: "RefreshPaused".to_string(),
            data_type: DataType::Utf8,
            nullable: false,
            logical_type: None,
        },
        QueryResultColumn {
            name: "NextRefreshTime".to_string(),
            data_type: DataType::Utf8,
            nullable: true,
            logical_type: None,
        },
        QueryResultColumn {
            name: "LastSchedulerError".to_string(),
            data_type: DataType::Utf8,
            nullable: true,
            logical_type: None,
        },
        QueryResultColumn {
            name: "MaxStalenessMs".to_string(),
            data_type: DataType::Utf8,
            nullable: true,
            logical_type: None,
        },
        QueryResultColumn {
            name: "RefreshState".to_string(),
            data_type: DataType::Utf8,
            nullable: false,
            logical_type: None,
        },
        QueryResultColumn {
            name: "RetryAfterTime".to_string(),
            data_type: DataType::Utf8,
            nullable: true,
            logical_type: None,
        },
    ];

    let schema = Arc::new(Schema::new(vec![
        Field::new("Name", DataType::Utf8, false),
        Field::new("Database", DataType::Utf8, false),
        Field::new("StorageEngine", DataType::Utf8, false),
        Field::new("RefreshMode", DataType::Utf8, false),
        Field::new("LastRefreshTime", DataType::Utf8, true),
        Field::new("LastRefreshRows", DataType::Utf8, true),
        Field::new("BaseTables", DataType::Utf8, false),
        Field::new("SelectText", DataType::Utf8, false),
        Field::new("Dependencies", DataType::Utf8, false),
        Field::new("RefreshPaused", DataType::Utf8, false),
        Field::new("NextRefreshTime", DataType::Utf8, true),
        Field::new("LastSchedulerError", DataType::Utf8, true),
        Field::new("MaxStalenessMs", DataType::Utf8, true),
        Field::new("RefreshState", DataType::Utf8, false),
        Field::new("RetryAfterTime", DataType::Utf8, true),
    ]));
    let arrays: Vec<ArrayRef> = vec![
        Arc::new(StringArray::from(
            rows.iter()
                .map(|row| Some(row.name.clone()))
                .collect::<Vec<_>>(),
        )),
        Arc::new(StringArray::from(
            rows.iter()
                .map(|row| Some(row.database.clone()))
                .collect::<Vec<_>>(),
        )),
        Arc::new(StringArray::from(
            rows.iter()
                .map(|row| Some(row.storage_engine.clone()))
                .collect::<Vec<_>>(),
        )),
        Arc::new(StringArray::from(
            rows.iter()
                .map(|row| Some(row.refresh_mode.clone()))
                .collect::<Vec<_>>(),
        )),
        Arc::new(StringArray::from(
            rows.iter()
                .map(|row| row.last_refresh_time.clone())
                .collect::<Vec<_>>(),
        )),
        Arc::new(StringArray::from(
            rows.iter()
                .map(|row| row.last_refresh_rows.clone())
                .collect::<Vec<_>>(),
        )),
        Arc::new(StringArray::from(
            rows.iter()
                .map(|row| Some(row.base_tables.clone()))
                .collect::<Vec<_>>(),
        )),
        Arc::new(StringArray::from(
            rows.iter()
                .map(|row| Some(row.select_text.clone()))
                .collect::<Vec<_>>(),
        )),
        Arc::new(StringArray::from(
            rows.iter()
                .map(|row| Some(row.dependencies.clone()))
                .collect::<Vec<_>>(),
        )),
        Arc::new(StringArray::from(
            rows.iter()
                .map(|row| Some(row.refresh_paused.clone()))
                .collect::<Vec<_>>(),
        )),
        Arc::new(StringArray::from(
            rows.iter()
                .map(|row| row.next_refresh_time.clone())
                .collect::<Vec<_>>(),
        )),
        Arc::new(StringArray::from(
            rows.iter()
                .map(|row| row.last_scheduler_error.clone())
                .collect::<Vec<_>>(),
        )),
        Arc::new(StringArray::from(
            rows.iter()
                .map(|row| row.max_staleness_ms.clone())
                .collect::<Vec<_>>(),
        )),
        Arc::new(StringArray::from(
            rows.iter()
                .map(|row| Some(row.refresh_state.clone()))
                .collect::<Vec<_>>(),
        )),
        Arc::new(StringArray::from(
            rows.iter()
                .map(|row| row.retry_after_time.clone())
                .collect::<Vec<_>>(),
        )),
    ];
    let batch = RecordBatch::try_new(schema, arrays)
        .map_err(|e| format!("build SHOW MATERIALIZED VIEWS batch failed: {e}"))?;
    Ok(QueryResult {
        columns,
        chunks: vec![record_batch_to_chunk(batch)?],
    })
}

pub(crate) fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}
