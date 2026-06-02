//! Query preparation that materializes external connector tables into the
//! standalone in-memory catalog before planning.

use std::sync::Arc;

use crate::engine::StandaloneState;
use crate::engine::StatementResult;
use crate::engine::backend_resolver::resolve_table_target;
use crate::engine::build_string_query_result;
use crate::engine::statement::parse_add_files_sql;
use crate::sql::analyzer::iceberg_ref::resolve_read_binding;
use crate::sql::catalog::{ColumnDef, ScanSource, TableDef};
use crate::sql::parser::ast::ObjectName;
use crate::sql::parser::query_refs::{
    extract_table_names_from_query, extract_three_part_table_refs, extract_two_part_table_refs,
};

#[derive(Clone, Debug)]
pub(crate) struct IcebergFileForQuery {
    pub(crate) path: String,
    pub(crate) size: i64,
    pub(crate) record_count: Option<i64>,
    pub(crate) partition_spec_id: Option<i32>,
    pub(crate) partition_key: Option<String>,
    pub(crate) first_row_id: Option<i64>,
    pub(crate) data_sequence_number: Option<i64>,
    pub(crate) change_op: Option<i8>,
    pub(crate) row_id_allow_list: Option<std::collections::BTreeSet<i64>>,
}

pub(crate) fn delete_temp_iceberg_file_for_query(
    path: String,
    size: i64,
    record_count: Option<i64>,
    change_op: Option<i8>,
) -> IcebergFileForQuery {
    IcebergFileForQuery {
        path,
        size,
        record_count,
        partition_spec_id: None,
        partition_key: None,
        first_row_id: None,
        data_sequence_number: None,
        change_op,
        row_id_allow_list: None,
    }
}

pub(crate) fn add_files(
    state: &Arc<StandaloneState>,
    sql: &str,
    current_catalog: Option<&str>,
    current_database: &str,
) -> Result<StatementResult, String> {
    let (table_parts, s3_path) = parse_add_files_sql(sql)?;

    let (catalog_name, namespace, table_name) = match table_parts.len() {
        1 => {
            let cat =
                current_catalog.ok_or("ADD FILES requires a catalog context (use SET catalog)")?;
            (
                cat.to_string(),
                current_database.to_string(),
                table_parts[0].clone(),
            )
        }
        2 => {
            let cat = current_catalog.ok_or("ADD FILES requires a catalog context")?;
            (
                cat.to_string(),
                table_parts[0].clone(),
                table_parts[1].clone(),
            )
        }
        3 => (
            table_parts[0].clone(),
            table_parts[1].clone(),
            table_parts[2].clone(),
        ),
        _ => return Err("invalid table name in ADD FILES".to_string()),
    };

    let guard = state
        .iceberg_catalogs
        .read()
        .expect("iceberg catalog read lock");
    let entry = guard.get(&catalog_name)?;
    drop(guard);
    let count = crate::connector::iceberg::catalog::add_files::add_files(
        &entry,
        &namespace,
        &table_name,
        &s3_path,
    )?;
    let msg = format!("Added {count} file(s)");
    build_string_query_result("status", vec![msg]).map(StatementResult::Query)
}

// ---------------------------------------------------------------------------
// Time-travel (FOR VERSION/TIMESTAMP AS OF) AST rewrite
// ---------------------------------------------------------------------------

/// Returns true if the query contains any `TableFactor::Table` node with a
/// `version: Some(...)` clause. Used as a cheap pre-check before cloning.
pub(crate) fn has_time_travel_refs(query: &sqlparser::ast::Query) -> bool {
    if let Some(with) = &query.with {
        for cte in &with.cte_tables {
            if has_time_travel_in_set_expr(cte.query.body.as_ref()) {
                return true;
            }
        }
    }
    has_time_travel_in_set_expr(query.body.as_ref())
}

fn has_time_travel_in_set_expr(expr: &sqlparser::ast::SetExpr) -> bool {
    match expr {
        sqlparser::ast::SetExpr::Select(select) => {
            for tw in &select.from {
                if has_time_travel_in_factor(&tw.relation) {
                    return true;
                }
                for join in &tw.joins {
                    if has_time_travel_in_factor(&join.relation) {
                        return true;
                    }
                }
            }
            false
        }
        sqlparser::ast::SetExpr::SetOperation { left, right, .. } => {
            has_time_travel_in_set_expr(left) || has_time_travel_in_set_expr(right)
        }
        sqlparser::ast::SetExpr::Query(q) => has_time_travel_in_set_expr(q.body.as_ref()),
        _ => false,
    }
}

fn has_time_travel_in_factor(factor: &sqlparser::ast::TableFactor) -> bool {
    match factor {
        sqlparser::ast::TableFactor::Table { version, .. } => version.is_some(),
        sqlparser::ast::TableFactor::Derived { subquery, .. } => {
            has_time_travel_in_set_expr(subquery.body.as_ref())
        }
        _ => false,
    }
}

/// Walk the query AST in-place and rewrite each `TableFactor::Table` that has
/// a `version: Some(...)` clause:
///
/// 1. Resolve `version` → `snapshot_id` via `resolve_read_binding`.
/// 2. Build a synthetic `TableDef` for that snapshot and register it in the
///    in-memory catalog under the name `<table>__at_<snapshot_id>`.
/// 3. Rewrite the `TableFactor::Table`:
///    - Replace `name` with the synthetic 1-part name.
///    - Clear `version` (set to `None`).
///    - Preserve any existing alias; if none, set `alias` = original table name
///      so that `SELECT t.col FROM t FOR VERSION AS OF ...` resolves `t.col`.
///
/// Tables without a version clause are left untouched.
pub(crate) fn rewrite_time_travel_refs(
    state: &Arc<StandaloneState>,
    current_catalog: Option<&str>,
    current_database: &str,
    query: &mut sqlparser::ast::Query,
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

    // Walk CTEs
    if let Some(with) = &mut query.with {
        for cte in &mut with.cte_tables {
            rewrite_time_travel_in_set_expr(
                state,
                current_catalog,
                current_database,
                &catalog_backend,
                &table_source,
                cte.query.body.as_mut(),
            )?;
        }
    }
    rewrite_time_travel_in_set_expr(
        state,
        current_catalog,
        current_database,
        &catalog_backend,
        &table_source,
        query.body.as_mut(),
    )
}

fn rewrite_time_travel_in_set_expr(
    state: &Arc<StandaloneState>,
    current_catalog: Option<&str>,
    current_database: &str,
    catalog_backend: &Arc<dyn crate::connector::backend::CatalogBackend>,
    table_source: &Arc<dyn crate::connector::backend::TableSource>,
    expr: &mut sqlparser::ast::SetExpr,
) -> Result<(), String> {
    match expr {
        sqlparser::ast::SetExpr::Select(select) => {
            for tw in &mut select.from {
                rewrite_time_travel_in_factor(
                    state,
                    current_catalog,
                    current_database,
                    catalog_backend,
                    table_source,
                    &mut tw.relation,
                )?;
                for join in &mut tw.joins {
                    rewrite_time_travel_in_factor(
                        state,
                        current_catalog,
                        current_database,
                        catalog_backend,
                        table_source,
                        &mut join.relation,
                    )?;
                }
            }
            Ok(())
        }
        sqlparser::ast::SetExpr::SetOperation { left, right, .. } => {
            rewrite_time_travel_in_set_expr(
                state,
                current_catalog,
                current_database,
                catalog_backend,
                table_source,
                left.as_mut(),
            )?;
            rewrite_time_travel_in_set_expr(
                state,
                current_catalog,
                current_database,
                catalog_backend,
                table_source,
                right.as_mut(),
            )
        }
        sqlparser::ast::SetExpr::Query(q) => rewrite_time_travel_in_set_expr(
            state,
            current_catalog,
            current_database,
            catalog_backend,
            table_source,
            q.body.as_mut(),
        ),
        _ => Ok(()),
    }
}

fn rewrite_time_travel_in_factor(
    state: &Arc<StandaloneState>,
    current_catalog: Option<&str>,
    current_database: &str,
    catalog_backend: &Arc<dyn crate::connector::backend::CatalogBackend>,
    table_source: &Arc<dyn crate::connector::backend::TableSource>,
    factor: &mut sqlparser::ast::TableFactor,
) -> Result<(), String> {
    match factor {
        sqlparser::ast::TableFactor::Table {
            name,
            version,
            alias,
            ..
        } if version.is_some() => {
            let version_clause = version.take().expect("checked is_some above");

            // Extract name parts for our ObjectName lookup
            let parts: Vec<String> = name
                .0
                .iter()
                .filter_map(|p| match p {
                    sqlparser::ast::ObjectNamePart::Identifier(ident) => {
                        Some(ident.value.to_ascii_lowercase())
                    }
                    _ => None,
                })
                .collect();

            if parts.is_empty() {
                return Err("iceberg time travel: table name has no identifier parts".to_string());
            }

            // Reject the combination of branch/tag suffix with FOR VERSION/TIMESTAMP AS OF.
            if let Some(last) = parts.last() {
                for prefix in &["branch_", "tag_"] {
                    if let Some(ref_name) = last.strip_prefix(prefix)
                        && !ref_name.is_empty()
                    {
                        return Err(format!(
                            "iceberg ref: branch suffix '.{}_{}' conflicts with FOR VERSION AS OF clause",
                            prefix.trim_end_matches('_'),
                            ref_name,
                        ));
                    }
                }
            }

            let our_name = ObjectName { parts };
            let target = resolve_table_target(state, &our_name, current_catalog, current_database)?;

            if target.backend_name != "iceberg" {
                return Err(format!(
                    "iceberg time travel: table '{}' is not an Iceberg table; time travel is only supported for Iceberg",
                    our_name.leaf()
                ));
            }

            // Load metadata to resolve the version clause
            let metadata = {
                let registry = state
                    .iceberg_catalogs
                    .read()
                    .map_err(|e| format!("iceberg catalog registry read lock: {e}"))?;
                let entry = registry.get(&target.catalog)?;
                let loaded = crate::connector::iceberg::catalog::load_table(
                    &entry,
                    &target.namespace,
                    &target.table,
                )?;
                loaded.table.metadata().clone()
            };

            let fqn = format!("{}.{}.{}", target.catalog, target.namespace, target.table);
            let binding = resolve_read_binding(&version_clause, &metadata, &fqn)?;
            let snapshot_id = binding.snapshot_id;

            // Build and register the synthetic table def
            let synthetic_table_name = format!("{}__at_{}", target.table, snapshot_id);
            {
                let resolved = catalog_backend.load_table(
                    &target.catalog,
                    &target.namespace,
                    &target.table,
                )?;
                let table_def = table_source.build_table_def_at(&resolved, Some(snapshot_id))?;
                // Build a new TableDef with the synthetic name
                let synthetic_def = TableDef {
                    name: synthetic_table_name.clone(),
                    ..table_def
                };
                register_external_table(state, &target.namespace, synthetic_def)?;
            }

            // Rewrite the AST node in-place:
            // - Set alias to original table name if user didn't specify one
            // - Replace name with the synthetic name resolved against the target namespace
            // - version is already cleared (we took it above)
            if alias.is_none() {
                // Infer the original table alias from the last non-catalog part of the name
                let original_leaf = our_name.leaf().to_string();
                *alias = Some(sqlparser::ast::TableAlias {
                    name: sqlparser::ast::Ident::new(original_leaf),
                    columns: vec![],
                    explicit: false,
                });
            }

            // Replace with a 2-part namespace-qualified synthetic name so the
            // rewritten query resolves correctly even when `current_database` is
            // empty or does not match the table's namespace.  The analyzer
            // accepts `<namespace>.<table>` 2-part references in the same way it
            // handles non-time-travel tables found via register_external_tables.
            *name = sqlparser::ast::ObjectName(vec![
                sqlparser::ast::ObjectNamePart::Identifier(sqlparser::ast::Ident::new(
                    target.namespace.clone(),
                )),
                sqlparser::ast::ObjectNamePart::Identifier(sqlparser::ast::Ident::new(
                    synthetic_table_name,
                )),
            ]);

            Ok(())
        }
        sqlparser::ast::TableFactor::Table { .. } => Ok(()),
        sqlparser::ast::TableFactor::Derived { subquery, .. } => rewrite_time_travel_in_set_expr(
            state,
            current_catalog,
            current_database,
            catalog_backend,
            table_source,
            subquery.body.as_mut(),
        ),
        _ => Ok(()),
    }
}

pub(crate) fn register_external_tables_for_query(
    state: &Arc<StandaloneState>,
    current_catalog: Option<&str>,
    current_database: &str,
    query: &sqlparser::ast::Query,
) -> Result<(), String> {
    register_external_tables_for_query_impl(
        state,
        current_catalog,
        current_database,
        query,
        false,
        QueryRegistrationMode::SchemaOnly,
    )
}

pub(crate) fn register_external_tables_for_query_with_scan_bindings(
    state: &Arc<StandaloneState>,
    current_catalog: Option<&str>,
    current_database: &str,
    query: &sqlparser::ast::Query,
) -> Result<(), String> {
    register_external_tables_for_query_impl(
        state,
        current_catalog,
        current_database,
        query,
        true,
        QueryRegistrationMode::ScanBinding,
    )
}

pub(crate) fn refresh_external_tables_for_query(
    state: &Arc<StandaloneState>,
    current_catalog: Option<&str>,
    current_database: &str,
    query: &sqlparser::ast::Query,
) -> Result<(), String> {
    register_external_tables_for_query_impl(
        state,
        current_catalog,
        current_database,
        query,
        true,
        QueryRegistrationMode::SchemaOnly,
    )
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum QueryRegistrationMode {
    SchemaOnly,
    ScanBinding,
}

fn build_registration_table_def(
    source: &dyn crate::connector::backend::TableSource,
    resolved: &crate::connector::backend::ResolvedTable,
) -> Result<TableDef, String> {
    source.build_schema_table_def(resolved)
}

fn build_query_registration_table_def(
    source: &dyn crate::connector::backend::TableSource,
    resolved: &crate::connector::backend::ResolvedTable,
    mode: QueryRegistrationMode,
) -> Result<TableDef, String> {
    match mode {
        QueryRegistrationMode::SchemaOnly => build_registration_table_def(source, resolved),
        QueryRegistrationMode::ScanBinding => source.build_table_def(resolved),
    }
}

/// Materialize a single external connector table into the standalone in-memory
/// catalog so that statement paths which do not run through the SELECT
/// query-prep flow (e.g. `ANALYZE TABLE` / `ANALYZE FULL TABLE`) can still
/// resolve its schema.
///
/// This mirrors the iceberg branch of `register_external_tables_for_query_impl`
/// but operates on an explicitly-named table rather than names extracted from a
/// query. Iceberg tables register lazily per-SELECT, so a table that was
/// `CREATE`d and `INSERT`ed into but never `SELECT`ed from has no local-catalog
/// entry yet; `ANALYZE` against it would otherwise fail with "unknown table".
///
/// Non-iceberg backends (local / StarRocks) register themselves on `CREATE`, so
/// this is a no-op for them — the caller's existing local-catalog lookup
/// already succeeds. Unlike the best-effort query-prep loop, a load failure for
/// an iceberg table here is surfaced as an error: the table was named
/// explicitly by the statement, so an unresolvable name is a real error.
pub(crate) fn register_external_table_by_name(
    state: &Arc<StandaloneState>,
    current_catalog: Option<&str>,
    current_database: &str,
    name: &ObjectName,
) -> Result<(), String> {
    let target = resolve_table_target(state, name, current_catalog, current_database)?;
    if target.backend_name != "iceberg" {
        // Local / StarRocks tables register themselves on CREATE; nothing to
        // materialize here.
        return Ok(());
    }
    // Synthetic time-travel tables live only in the in-memory catalog and are
    // unknown to the iceberg backend; never attempt to reload them.
    if is_synthetic_time_travel_table(&target.table) {
        return Ok(());
    }

    let (catalog, source) = {
        let registry = state
            .connectors
            .read()
            .expect("standalone connector registry read lock");
        (
            registry.catalog_backend("iceberg")?,
            registry.table_source("iceberg")?,
        )
    };

    {
        let registry = state
            .iceberg_catalogs
            .read()
            .map_err(|e| format!("iceberg catalog registry read lock: {e}"))?;
        let entry = registry.get(&target.catalog)?;
        entry.invalidate_table_cache(&target.namespace, &target.table);
    }
    drop_registered_external_table(state, &target.namespace, &target.table)?;

    let resolved = catalog
        .load_table(&target.catalog, &target.namespace, &target.table)
        .map_err(|err| {
            format!(
                "load iceberg table {}.{}.{} failed: {err}",
                target.catalog, target.namespace, target.table
            )
        })?;
    let table_def = build_registration_table_def(source.as_ref(), &resolved)?;
    register_external_table(state, &target.namespace, table_def)
}

fn register_external_tables_for_query_impl(
    state: &Arc<StandaloneState>,
    current_catalog: Option<&str>,
    current_database: &str,
    query: &sqlparser::ast::Query,
    force_refresh: bool,
    mode: QueryRegistrationMode,
) -> Result<(), String> {
    let mut names = query_table_names(current_catalog, query);
    if names.is_empty() {
        return Ok(());
    }
    names.sort_by(|left, right| left.parts.cmp(&right.parts));
    names.dedup_by(|left, right| left.parts == right.parts);
    let partition_metadata_scan_binding_targets =
        partition_metadata_scan_binding_targets(state, current_catalog, current_database, query);

    let (catalog, source) = {
        let registry = state
            .connectors
            .read()
            .expect("standalone connector registry read lock");
        (
            registry.catalog_backend("iceberg")?,
            registry.table_source("iceberg")?,
        )
    };

    for name in names {
        let original_parts_len = name.parts.len();
        let Ok(target) = resolve_table_target(state, &name, current_catalog, current_database)
        else {
            continue;
        };
        if target.backend_name != "iceberg" {
            let local = state.catalog.read().expect("catalog read lock");
            if !force_refresh && local.get(&target.namespace, &target.table).is_ok() {
                continue;
            }
            continue;
        }
        // Skip synthetic time-travel tables registered by `rewrite_time_travel_refs`
        // (name pattern: `<table>__at_<snapshot_id>`).  These live only in the
        // InMemory catalog and must not be dropped or re-looked-up from the iceberg
        // catalog backend, which doesn't know about them.
        if is_synthetic_time_travel_table(&target.table) {
            continue;
        }
        {
            let registry = state
                .iceberg_catalogs
                .read()
                .map_err(|e| format!("iceberg catalog registry read lock: {e}"))?;
            let entry = registry.get(&target.catalog)?;
            entry.invalidate_table_cache(&target.namespace, &target.table);
        }
        drop_registered_external_table(state, &target.namespace, &target.table)?;

        let resolved = match catalog.load_table(&target.catalog, &target.namespace, &target.table) {
            Ok(resolved) => resolved,
            Err(err) => {
                // 3-part names came from explicit `cat.db.tbl` references in the
                // SQL — a load failure there is a real query error. 1-part /
                // 2-part fallbacks are inferred from the session's catalog +
                // database and are best-effort: the SELECT may legitimately
                // target a table in a different catalog (e.g. an MV target
                // table in `default_catalog` while the session catalog is an
                // iceberg catalog used to read the base). Swallow those so
                // the downstream relation resolver can route the lookup
                // through the correct catalog.
                if original_parts_len >= 3 {
                    return Err(format!(
                        "load iceberg table {}.{}.{} failed: {err}",
                        target.catalog, target.namespace, target.table
                    ));
                }
                continue;
            }
        };
        let registration_mode = if partition_metadata_scan_binding_targets.contains(&(
            target.catalog.clone(),
            target.namespace.clone(),
            target.table.clone(),
        )) {
            QueryRegistrationMode::ScanBinding
        } else {
            mode
        };
        let table_def =
            build_query_registration_table_def(source.as_ref(), &resolved, registration_mode)?;
        register_external_table(state, &target.namespace, table_def)?;
    }

    Ok(())
}

fn query_table_names(
    current_catalog: Option<&str>,
    query: &sqlparser::ast::Query,
) -> Vec<ObjectName> {
    // Always collect fully-qualified 3-part references (including 4-part
    // __nr_meta_*__ forms reduced to 3-part). They register against the
    // catalog encoded in the name regardless of session catalog.
    let three_part_refs = extract_three_part_table_refs(query);
    let three_part_tables: std::collections::HashSet<&str> = three_part_refs
        .iter()
        .map(|(_, _, table)| table.as_str())
        .collect();
    let mut names: Vec<ObjectName> = three_part_refs
        .iter()
        .map(|(catalog, namespace, table)| ObjectName {
            parts: vec![catalog.clone(), namespace.clone(), table.clone()],
        })
        .collect();

    // When the session has a current catalog, also collect 1-part and 2-part
    // names so that unqualified and db-qualified references in the query
    // register through the session catalog.
    //
    // 2-part `db.table` references are collected with both parts preserved so
    // they resolve against the explicit namespace rather than the session's
    // current database. This is the critical fix for INSERT-SELECT and SELECT
    // that reference `db.table` when `db` differs from the current database:
    // the name is resolved against the active catalog's `db` namespace, not
    // against `current_database`.
    //
    // Skip any table whose name already appears as a 3-part ref: registering
    // it again with the (current_catalog, current_database) target would
    // resolve to the wrong db when the SQL explicitly named a different one,
    // and abort the query with a spurious "no metadata files" failure even
    // though the 3-part registration above already loaded the table
    // successfully.
    if current_catalog.is_some() {
        // Collect 2-part (namespace, table) references first.
        let two_part_refs = extract_two_part_table_refs(query);
        // Build a set of table names covered by 2-part refs so we can skip
        // them in the 1-part pass (avoids double-registration under the wrong
        // namespace when the same table appears as both `db.t` and bare `t`).
        let two_part_tables: std::collections::HashSet<String> = two_part_refs
            .iter()
            .map(|(_, table)| table.clone())
            .collect();
        for (namespace, table) in two_part_refs {
            if three_part_tables.contains(table.as_str()) {
                continue;
            }
            names.push(ObjectName {
                parts: vec![namespace, table],
            });
        }

        // Collect 1-part (unqualified) table names, skipping any that were
        // already captured via the 2-part or 3-part paths above.
        for table in extract_table_names_from_query(query) {
            if three_part_tables.contains(table.as_str())
                || two_part_tables.contains(table.as_str())
            {
                continue;
            }
            names.push(ObjectName { parts: vec![table] });
        }
    }

    // Stable de-duplication on (parts) so the downstream registration loop
    // does not redundantly hit the iceberg backend for the same target.
    names.sort_by(|a, b| a.parts.cmp(&b.parts));
    names.dedup_by(|a, b| a.parts == b.parts);
    names
}

type ScanBindingTargetKey = (String, String, String);

fn partition_metadata_scan_binding_targets(
    state: &Arc<StandaloneState>,
    current_catalog: Option<&str>,
    current_database: &str,
    query: &sqlparser::ast::Query,
) -> std::collections::BTreeSet<ScanBindingTargetKey> {
    partition_metadata_table_refs(current_catalog, query)
        .into_iter()
        .filter_map(|parts| {
            let name = ObjectName { parts };
            let target =
                resolve_table_target(state, &name, current_catalog, current_database).ok()?;
            (target.backend_name == "iceberg").then_some((
                target.catalog,
                target.namespace,
                target.table,
            ))
        })
        .collect()
}

fn query_requires_partition_metadata_files(query: &sqlparser::ast::Query) -> bool {
    let mut refs = std::collections::BTreeSet::new();
    collect_partition_metadata_refs_from_query(query, true, &mut refs);
    !refs.is_empty()
}

fn partition_metadata_table_refs(
    current_catalog: Option<&str>,
    query: &sqlparser::ast::Query,
) -> std::collections::BTreeSet<Vec<String>> {
    let mut refs = std::collections::BTreeSet::new();
    collect_partition_metadata_refs_from_query(query, current_catalog.is_some(), &mut refs);
    refs
}

fn collect_partition_metadata_refs_from_query(
    query: &sqlparser::ast::Query,
    include_session_refs: bool,
    refs: &mut std::collections::BTreeSet<Vec<String>>,
) {
    if let Some(with) = &query.with {
        for cte in &with.cte_tables {
            collect_partition_metadata_refs_from_query(&cte.query, include_session_refs, refs);
        }
    }
    collect_partition_metadata_refs_from_set_expr(query.body.as_ref(), include_session_refs, refs);
    if let Some(order_by) = &query.order_by {
        collect_partition_metadata_refs_from_order_by(order_by, include_session_refs, refs);
    }
    collect_partition_metadata_refs_from_limit_clause(
        query.limit_clause.as_ref(),
        include_session_refs,
        refs,
    );
}

fn collect_partition_metadata_refs_from_set_expr(
    expr: &sqlparser::ast::SetExpr,
    include_session_refs: bool,
    refs: &mut std::collections::BTreeSet<Vec<String>>,
) {
    match expr {
        sqlparser::ast::SetExpr::Select(select) => {
            for from in &select.from {
                collect_partition_metadata_refs_from_table_factor(
                    &from.relation,
                    include_session_refs,
                    refs,
                );
                for join in &from.joins {
                    collect_partition_metadata_refs_from_table_factor(
                        &join.relation,
                        include_session_refs,
                        refs,
                    );
                    if let Some(on_expr) = join_on_constraint_expr(&join.join_operator) {
                        collect_partition_metadata_refs_from_expr(
                            on_expr,
                            include_session_refs,
                            refs,
                        );
                    }
                }
            }
            if let Some(prewhere) = &select.prewhere {
                collect_partition_metadata_refs_from_expr(prewhere, include_session_refs, refs);
            }
            if let Some(selection) = &select.selection {
                collect_partition_metadata_refs_from_expr(selection, include_session_refs, refs);
            }
            for connect_by in &select.connect_by {
                collect_partition_metadata_refs_from_connect_by(
                    connect_by,
                    include_session_refs,
                    refs,
                );
            }
            collect_partition_metadata_refs_from_group_by(
                &select.group_by,
                include_session_refs,
                refs,
            );
            for expr in &select.cluster_by {
                collect_partition_metadata_refs_from_expr(expr, include_session_refs, refs);
            }
            for expr in &select.distribute_by {
                collect_partition_metadata_refs_from_expr(expr, include_session_refs, refs);
            }
            for order_by in &select.sort_by {
                collect_partition_metadata_refs_from_order_by_expr(
                    order_by,
                    include_session_refs,
                    refs,
                );
            }
            if let Some(having) = &select.having {
                collect_partition_metadata_refs_from_expr(having, include_session_refs, refs);
            }
            if let Some(qualify) = &select.qualify {
                collect_partition_metadata_refs_from_expr(qualify, include_session_refs, refs);
            }
            for projection in &select.projection {
                match projection {
                    sqlparser::ast::SelectItem::UnnamedExpr(expr)
                    | sqlparser::ast::SelectItem::ExprWithAlias { expr, .. } => {
                        collect_partition_metadata_refs_from_expr(expr, include_session_refs, refs);
                    }
                    _ => {}
                }
            }
            for lateral_view in &select.lateral_views {
                collect_partition_metadata_refs_from_expr(
                    &lateral_view.lateral_view,
                    include_session_refs,
                    refs,
                );
            }
            for named_window in &select.named_window {
                collect_partition_metadata_refs_from_named_window(
                    named_window,
                    include_session_refs,
                    refs,
                );
            }
        }
        sqlparser::ast::SetExpr::SetOperation { left, right, .. } => {
            collect_partition_metadata_refs_from_set_expr(left, include_session_refs, refs);
            collect_partition_metadata_refs_from_set_expr(right, include_session_refs, refs);
        }
        sqlparser::ast::SetExpr::Query(query) => {
            collect_partition_metadata_refs_from_query(query, include_session_refs, refs);
        }
        sqlparser::ast::SetExpr::Values(values) => {
            for row in &values.rows {
                for expr in row {
                    collect_partition_metadata_refs_from_expr(expr, include_session_refs, refs);
                }
            }
        }
        _ => {}
    }
}

fn join_on_constraint_expr(
    join_operator: &sqlparser::ast::JoinOperator,
) -> Option<&sqlparser::ast::Expr> {
    use sqlparser::ast::{JoinConstraint, JoinOperator};
    let constraint = match join_operator {
        JoinOperator::Join(c)
        | JoinOperator::Inner(c)
        | JoinOperator::Left(c)
        | JoinOperator::LeftOuter(c)
        | JoinOperator::Right(c)
        | JoinOperator::RightOuter(c)
        | JoinOperator::FullOuter(c)
        | JoinOperator::CrossJoin(c)
        | JoinOperator::Semi(c)
        | JoinOperator::LeftSemi(c)
        | JoinOperator::RightSemi(c)
        | JoinOperator::Anti(c)
        | JoinOperator::LeftAnti(c)
        | JoinOperator::RightAnti(c)
        | JoinOperator::StraightJoin(c)
        | JoinOperator::AsOf { constraint: c, .. } => c,
        JoinOperator::CrossApply | JoinOperator::OuterApply => return None,
    };
    match constraint {
        JoinConstraint::On(expr) => Some(expr),
        JoinConstraint::Using(_) | JoinConstraint::Natural | JoinConstraint::None => None,
    }
}

fn collect_partition_metadata_refs_from_table_factor(
    factor: &sqlparser::ast::TableFactor,
    include_session_refs: bool,
    refs: &mut std::collections::BTreeSet<Vec<String>>,
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
            let (_, metadata_suffix) =
                crate::sql::analyzer::iceberg_metadata::split_metadata_suffix(&parts);
            if matches!(
                metadata_suffix,
                Some(crate::connector::iceberg::IcebergMetadataTableType::Partitions)
            ) {
                let (base_parts, _) =
                    crate::sql::analyzer::iceberg_metadata::split_metadata_suffix(&parts);
                if base_parts.len() == 3
                    || (include_session_refs && matches!(base_parts.len(), 1 | 2))
                {
                    refs.insert(base_parts.to_vec());
                }
            }
        }
        sqlparser::ast::TableFactor::Derived { subquery, .. } => {
            collect_partition_metadata_refs_from_query(subquery, include_session_refs, refs);
        }
        sqlparser::ast::TableFactor::TableFunction { expr, .. } => {
            collect_partition_metadata_refs_from_expr(expr, include_session_refs, refs);
        }
        sqlparser::ast::TableFactor::Function { args, .. } => {
            for arg in args {
                collect_partition_metadata_refs_from_function_arg(arg, include_session_refs, refs);
            }
        }
        sqlparser::ast::TableFactor::UNNEST { array_exprs, .. } => {
            for expr in array_exprs {
                collect_partition_metadata_refs_from_expr(expr, include_session_refs, refs);
            }
        }
        sqlparser::ast::TableFactor::NestedJoin {
            table_with_joins, ..
        } => {
            collect_partition_metadata_refs_from_table_with_joins(
                table_with_joins,
                include_session_refs,
                refs,
            );
        }
        _ => {}
    }
}

fn collect_partition_metadata_refs_from_table_with_joins(
    table_with_joins: &sqlparser::ast::TableWithJoins,
    include_session_refs: bool,
    refs: &mut std::collections::BTreeSet<Vec<String>>,
) {
    collect_partition_metadata_refs_from_table_factor(
        &table_with_joins.relation,
        include_session_refs,
        refs,
    );
    for join in &table_with_joins.joins {
        collect_partition_metadata_refs_from_table_factor(
            &join.relation,
            include_session_refs,
            refs,
        );
        if let Some(on_expr) = join_on_constraint_expr(&join.join_operator) {
            collect_partition_metadata_refs_from_expr(on_expr, include_session_refs, refs);
        }
    }
}

fn collect_partition_metadata_refs_from_expr(
    expr: &sqlparser::ast::Expr,
    include_session_refs: bool,
    refs: &mut std::collections::BTreeSet<Vec<String>>,
) {
    use sqlparser::ast::Expr;
    match expr {
        Expr::Subquery(query)
        | Expr::Exists {
            subquery: query, ..
        } => collect_partition_metadata_refs_from_query(query, include_session_refs, refs),
        Expr::InSubquery { subquery, expr, .. } => {
            collect_partition_metadata_refs_from_query(subquery, include_session_refs, refs);
            collect_partition_metadata_refs_from_expr(expr, include_session_refs, refs);
        }
        Expr::BinaryOp { left, right, .. } => {
            collect_partition_metadata_refs_from_expr(left, include_session_refs, refs);
            collect_partition_metadata_refs_from_expr(right, include_session_refs, refs);
        }
        Expr::UnaryOp { expr, .. } | Expr::Nested(expr) => {
            collect_partition_metadata_refs_from_expr(expr, include_session_refs, refs);
        }
        Expr::Between {
            expr, low, high, ..
        } => {
            collect_partition_metadata_refs_from_expr(expr, include_session_refs, refs);
            collect_partition_metadata_refs_from_expr(low, include_session_refs, refs);
            collect_partition_metadata_refs_from_expr(high, include_session_refs, refs);
        }
        Expr::Function(function) => {
            collect_partition_metadata_refs_from_function(function, include_session_refs, refs);
        }
        Expr::Case {
            operand,
            conditions,
            else_result,
            ..
        } => {
            if let Some(op) = operand {
                collect_partition_metadata_refs_from_expr(op, include_session_refs, refs);
            }
            for case_when in conditions {
                collect_partition_metadata_refs_from_expr(
                    &case_when.condition,
                    include_session_refs,
                    refs,
                );
                collect_partition_metadata_refs_from_expr(
                    &case_when.result,
                    include_session_refs,
                    refs,
                );
            }
            if let Some(else_expr) = else_result {
                collect_partition_metadata_refs_from_expr(else_expr, include_session_refs, refs);
            }
        }
        Expr::Cast { expr, .. } => {
            collect_partition_metadata_refs_from_expr(expr, include_session_refs, refs);
        }
        _ => {}
    }
}

fn collect_partition_metadata_refs_from_group_by(
    group_by: &sqlparser::ast::GroupByExpr,
    include_session_refs: bool,
    refs: &mut std::collections::BTreeSet<Vec<String>>,
) {
    if let sqlparser::ast::GroupByExpr::Expressions(exprs, _) = group_by {
        for expr in exprs {
            collect_partition_metadata_refs_from_expr(expr, include_session_refs, refs);
        }
    }
}

fn collect_partition_metadata_refs_from_order_by(
    order_by: &sqlparser::ast::OrderBy,
    include_session_refs: bool,
    refs: &mut std::collections::BTreeSet<Vec<String>>,
) {
    if let sqlparser::ast::OrderByKind::Expressions(exprs) = &order_by.kind {
        for expr in exprs {
            collect_partition_metadata_refs_from_order_by_expr(expr, include_session_refs, refs);
        }
    }
    if let Some(interpolate) = &order_by.interpolate
        && let Some(exprs) = &interpolate.exprs
    {
        for expr in exprs {
            if let Some(inner) = &expr.expr {
                collect_partition_metadata_refs_from_expr(inner, include_session_refs, refs);
            }
        }
    }
}

fn collect_partition_metadata_refs_from_order_by_expr(
    order_by: &sqlparser::ast::OrderByExpr,
    include_session_refs: bool,
    refs: &mut std::collections::BTreeSet<Vec<String>>,
) {
    collect_partition_metadata_refs_from_expr(&order_by.expr, include_session_refs, refs);
    if let Some(with_fill) = &order_by.with_fill {
        if let Some(expr) = &with_fill.from {
            collect_partition_metadata_refs_from_expr(expr, include_session_refs, refs);
        }
        if let Some(expr) = &with_fill.to {
            collect_partition_metadata_refs_from_expr(expr, include_session_refs, refs);
        }
        if let Some(expr) = &with_fill.step {
            collect_partition_metadata_refs_from_expr(expr, include_session_refs, refs);
        }
    }
}

fn collect_partition_metadata_refs_from_limit_clause(
    limit_clause: Option<&sqlparser::ast::LimitClause>,
    include_session_refs: bool,
    refs: &mut std::collections::BTreeSet<Vec<String>>,
) {
    match limit_clause {
        Some(sqlparser::ast::LimitClause::LimitOffset {
            limit,
            offset,
            limit_by,
        }) => {
            if let Some(limit) = limit {
                collect_partition_metadata_refs_from_expr(limit, include_session_refs, refs);
            }
            if let Some(offset) = offset {
                collect_partition_metadata_refs_from_expr(
                    &offset.value,
                    include_session_refs,
                    refs,
                );
            }
            for expr in limit_by {
                collect_partition_metadata_refs_from_expr(expr, include_session_refs, refs);
            }
        }
        Some(sqlparser::ast::LimitClause::OffsetCommaLimit { offset, limit }) => {
            collect_partition_metadata_refs_from_expr(offset, include_session_refs, refs);
            collect_partition_metadata_refs_from_expr(limit, include_session_refs, refs);
        }
        None => {}
    }
}

fn collect_partition_metadata_refs_from_connect_by(
    connect_by: &sqlparser::ast::ConnectByKind,
    include_session_refs: bool,
    refs: &mut std::collections::BTreeSet<Vec<String>>,
) {
    match connect_by {
        sqlparser::ast::ConnectByKind::ConnectBy { relationships, .. } => {
            for expr in relationships {
                collect_partition_metadata_refs_from_expr(expr, include_session_refs, refs);
            }
        }
        sqlparser::ast::ConnectByKind::StartWith { condition, .. } => {
            collect_partition_metadata_refs_from_expr(condition, include_session_refs, refs);
        }
    }
}

fn collect_partition_metadata_refs_from_named_window(
    named_window: &sqlparser::ast::NamedWindowDefinition,
    include_session_refs: bool,
    refs: &mut std::collections::BTreeSet<Vec<String>>,
) {
    if let sqlparser::ast::NamedWindowExpr::WindowSpec(spec) = &named_window.1 {
        collect_partition_metadata_refs_from_window_spec(spec, include_session_refs, refs);
    }
}

fn collect_partition_metadata_refs_from_window_spec(
    spec: &sqlparser::ast::WindowSpec,
    include_session_refs: bool,
    refs: &mut std::collections::BTreeSet<Vec<String>>,
) {
    for expr in &spec.partition_by {
        collect_partition_metadata_refs_from_expr(expr, include_session_refs, refs);
    }
    for order_by in &spec.order_by {
        collect_partition_metadata_refs_from_order_by_expr(order_by, include_session_refs, refs);
    }
}

fn collect_partition_metadata_refs_from_function(
    function: &sqlparser::ast::Function,
    include_session_refs: bool,
    refs: &mut std::collections::BTreeSet<Vec<String>>,
) {
    collect_partition_metadata_refs_from_function_arguments(
        &function.parameters,
        include_session_refs,
        refs,
    );
    collect_partition_metadata_refs_from_function_arguments(
        &function.args,
        include_session_refs,
        refs,
    );
    if let Some(filter) = &function.filter {
        collect_partition_metadata_refs_from_expr(filter, include_session_refs, refs);
    }
    if let Some(over) = &function.over {
        collect_partition_metadata_refs_from_window_type(over, include_session_refs, refs);
    }
    for order_by in &function.within_group {
        collect_partition_metadata_refs_from_order_by_expr(order_by, include_session_refs, refs);
    }
}

fn collect_partition_metadata_refs_from_window_type(
    window: &sqlparser::ast::WindowType,
    include_session_refs: bool,
    refs: &mut std::collections::BTreeSet<Vec<String>>,
) {
    if let sqlparser::ast::WindowType::WindowSpec(spec) = window {
        collect_partition_metadata_refs_from_window_spec(spec, include_session_refs, refs);
    }
}

fn collect_partition_metadata_refs_from_function_arguments(
    args: &sqlparser::ast::FunctionArguments,
    include_session_refs: bool,
    refs: &mut std::collections::BTreeSet<Vec<String>>,
) {
    match args {
        sqlparser::ast::FunctionArguments::Subquery(query) => {
            collect_partition_metadata_refs_from_query(query, include_session_refs, refs);
        }
        sqlparser::ast::FunctionArguments::List(arg_list) => {
            for arg in &arg_list.args {
                collect_partition_metadata_refs_from_function_arg(arg, include_session_refs, refs);
            }
            for clause in &arg_list.clauses {
                collect_partition_metadata_refs_from_function_clause(
                    clause,
                    include_session_refs,
                    refs,
                );
            }
        }
        sqlparser::ast::FunctionArguments::None => {}
    }
}

fn collect_partition_metadata_refs_from_function_arg(
    arg: &sqlparser::ast::FunctionArg,
    include_session_refs: bool,
    refs: &mut std::collections::BTreeSet<Vec<String>>,
) {
    let inner = match arg {
        sqlparser::ast::FunctionArg::Unnamed(sqlparser::ast::FunctionArgExpr::Expr(expr)) => {
            Some(expr)
        }
        sqlparser::ast::FunctionArg::Named {
            arg: sqlparser::ast::FunctionArgExpr::Expr(expr),
            ..
        } => Some(expr),
        sqlparser::ast::FunctionArg::ExprNamed {
            arg: sqlparser::ast::FunctionArgExpr::Expr(expr),
            ..
        } => Some(expr),
        _ => None,
    };
    if let Some(expr) = inner {
        collect_partition_metadata_refs_from_expr(expr, include_session_refs, refs);
    }
}

fn collect_partition_metadata_refs_from_function_clause(
    clause: &sqlparser::ast::FunctionArgumentClause,
    include_session_refs: bool,
    refs: &mut std::collections::BTreeSet<Vec<String>>,
) {
    match clause {
        sqlparser::ast::FunctionArgumentClause::OrderBy(order_by) => {
            for expr in order_by {
                collect_partition_metadata_refs_from_order_by_expr(
                    expr,
                    include_session_refs,
                    refs,
                );
            }
        }
        sqlparser::ast::FunctionArgumentClause::Limit(expr) => {
            collect_partition_metadata_refs_from_expr(expr, include_session_refs, refs);
        }
        sqlparser::ast::FunctionArgumentClause::Having(bound) => {
            collect_partition_metadata_refs_from_expr(&bound.1, include_session_refs, refs);
        }
        _ => {}
    }
}

/// Returns true if `table_name` was produced by the time-travel rewriter.
/// Synthetic names follow the pattern `<original_table>__at_<snapshot_id>`
/// where `snapshot_id` is a decimal integer (i64).
fn is_synthetic_time_travel_table(table_name: &str) -> bool {
    if let Some(at_pos) = table_name.rfind("__at_") {
        let suffix = &table_name[at_pos + "__at_".len()..];
        !suffix.is_empty() && suffix.chars().all(|c| c.is_ascii_digit() || c == '-')
    } else {
        false
    }
}

fn register_external_table(
    state: &Arc<StandaloneState>,
    namespace: &str,
    table_def: TableDef,
) -> Result<(), String> {
    let mut guard = state.catalog.write().expect("catalog write lock");
    guard.create_database(namespace).ok();
    guard
        .register(namespace, table_def)
        .map_err(|e| format!("register external table: {e}"))
}

pub(crate) fn drop_registered_external_table(
    state: &Arc<StandaloneState>,
    namespace: &str,
    table: &str,
) -> Result<(), String> {
    let mut guard = state
        .catalog
        .write()
        .map_err(|e| format!("standalone catalog write lock: {e}"))?;
    match guard.drop_table(namespace, table) {
        Ok(()) => Ok(()),
        Err(err) if err.contains("unknown") => Ok(()),
        Err(err) => Err(format!("drop registered external table: {err}")),
    }
}

/// IVM-A1 helper: build an `InMemoryCatalog`-compatible `TableDef` for the
/// base table of an MV refresh without registering any data files.
/// Advertises Iceberg v3 row-lineage virtual columns (`_row_id`, etc.) so
/// the analyzer can resolve apply-key references; the actual per-snapshot
/// files come from the `IcebergDeltaScan` operator at runtime.
pub(crate) fn build_iceberg_table_def_for_delta_scan(
    state: &Arc<StandaloneState>,
    catalog_name: &str,
    namespace: &str,
    table_name: &str,
) -> Result<TableDef, String> {
    let entry = {
        let registry = state
            .iceberg_catalogs
            .read()
            .expect("iceberg registry read lock");
        registry.get(catalog_name)?
    };
    let loaded = crate::connector::iceberg::catalog::load_table(&entry, namespace, table_name)?;
    crate::connector::iceberg::catalog::build_iceberg_table_def_for_delta_scan(
        catalog_name,
        namespace,
        table_name,
        loaded,
    )
}

pub(crate) fn build_iceberg_table_def_with_files(
    state: &Arc<StandaloneState>,
    catalog_name: &str,
    namespace: &str,
    table_name: &str,
    data_files: Vec<IcebergFileForQuery>,
) -> Result<TableDef, String> {
    let entry = {
        let registry = state
            .iceberg_catalogs
            .read()
            .expect("iceberg registry read lock");
        registry.get(catalog_name)?
    };
    let loaded = crate::connector::iceberg::catalog::load_table(&entry, namespace, table_name)?;
    let data_files = data_files
        .into_iter()
        .map(
            |file| crate::connector::iceberg::catalog::registry::DataFileWithStats {
                path: file.path,
                size: file.size,
                record_count: file.record_count,
                column_stats: None,
                partition_spec_id: file.partition_spec_id,
                partition_key: file.partition_key,
                partition_values: None,
                manifest_path: None,
                partition_field_values: vec![],
                first_row_id: file.first_row_id,
                data_sequence_number: file.data_sequence_number,
                delete_files: vec![],
            },
        )
        .collect();
    crate::connector::iceberg::catalog::build_iceberg_table_def_with_files(
        &entry,
        catalog_name,
        namespace,
        table_name,
        loaded,
        data_files,
    )
}

pub(crate) fn build_iceberg_delta_table_def_with_files(
    entry: &crate::connector::iceberg::catalog::IcebergCatalogEntry,
    catalog_name: &str,
    namespace: &str,
    table_name: &str,
    loaded: crate::connector::iceberg::catalog::IcebergLoadedTable,
    data_files: Vec<IcebergFileForQuery>,
) -> Result<TableDef, String> {
    let change_ops = validate_delta_file_change_ops(&data_files)?;
    let data_files = iceberg_files_for_query_to_stats(data_files);
    let mut table_def = crate::connector::iceberg::catalog::build_iceberg_table_def_with_files(
        entry,
        catalog_name,
        namespace,
        table_name,
        loaded,
        data_files,
    )?;
    stamp_delta_table_def_change_ops(&mut table_def, &change_ops)?;
    Ok(table_def)
}

fn iceberg_files_for_query_to_stats(
    data_files: Vec<IcebergFileForQuery>,
) -> Vec<crate::connector::iceberg::catalog::registry::DataFileWithStats> {
    data_files
        .into_iter()
        .map(
            |file| crate::connector::iceberg::catalog::registry::DataFileWithStats {
                path: file.path,
                size: file.size,
                record_count: file.record_count,
                column_stats: None,
                partition_spec_id: file.partition_spec_id,
                partition_key: file.partition_key,
                partition_values: None,
                manifest_path: None,
                partition_field_values: vec![],
                first_row_id: file.first_row_id,
                data_sequence_number: file.data_sequence_number,
                delete_files: vec![],
            },
        )
        .collect()
}

fn validate_delta_file_change_ops(data_files: &[IcebergFileForQuery]) -> Result<Vec<i8>, String> {
    data_files
        .iter()
        .enumerate()
        .map(|(idx, file)| {
            let op = file.change_op.ok_or_else(|| {
                format!(
                    "iceberg delta source file {} ({}) missing {}",
                    idx,
                    file.path,
                    crate::exec::change_op::CHANGE_OP_COLUMN
                )
            })?;
            crate::exec::change_op::validate_change_op_value(op)?;
            Ok(op)
        })
        .collect()
}

fn stamp_delta_table_def_change_ops(
    table_def: &mut TableDef,
    change_ops: &[i8],
) -> Result<(), String> {
    if table_def.columns.iter().any(|col| {
        col.name
            .eq_ignore_ascii_case(crate::exec::change_op::CHANGE_OP_COLUMN)
    }) {
        return Err(format!(
            "iceberg delta source base table already has reserved column {}",
            crate::exec::change_op::CHANGE_OP_COLUMN
        ));
    }
    if table_def
        .iceberg_row_lineage_metadata_columns
        .iter()
        .any(|col| {
            col.name
                .eq_ignore_ascii_case(crate::exec::change_op::CHANGE_OP_COLUMN)
        })
    {
        return Err(format!(
            "iceberg delta source metadata already contains reserved column {}",
            crate::exec::change_op::CHANGE_OP_COLUMN
        ));
    }

    let field = crate::exec::change_op::change_op_field();
    table_def
        .iceberg_row_lineage_metadata_columns
        .push(ColumnDef {
            name: field.name().clone(),
            data_type: field.data_type().clone(),
            nullable: field.is_nullable(),
            write_default: None,
            logical_type: None,
        });

    let ScanSource::IcebergDataFiles { files, .. } = &mut table_def.source else {
        return Err(
            "iceberg delta source requires Iceberg data-file storage for synthetic files"
                .to_string(),
        );
    };
    if files.len() != change_ops.len() {
        return Err(format!(
            "iceberg delta source file count mismatch: table storage has {}, input has {}",
            files.len(),
            change_ops.len()
        ));
    }
    for (file, op) in files.iter_mut().zip(change_ops.iter().copied()) {
        file.ivm_change_op = Some(op);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::engine::query_prep::IcebergFileForQuery;
    use crate::sql::catalog::{IcebergSchemaDef, IcebergTableInfo, ScanSource, TableDef};
    use crate::sql::parser::ast::ObjectName;

    fn test_iceberg_table_info() -> IcebergTableInfo {
        IcebergTableInfo {
            catalog: "test_catalog".to_string(),
            namespace: "test_db".to_string(),
            table: "test_table".to_string(),
            table_uuid: Some("00000000-0000-0000-0000-000000000001".to_string()),
            current_snapshot_id: Some(7),
            schema_id: 1,
            location: "file:///tmp/test_table".to_string(),
            schema: IcebergSchemaDef { fields: vec![] },
            serialized_metadata: None,
        }
    }

    #[test]
    fn registration_table_def_uses_schema_only_table_source() {
        struct SchemaOnlySource;
        impl crate::connector::backend::TableSource for SchemaOnlySource {
            fn name(&self) -> &'static str {
                "iceberg"
            }

            fn build_table_def(
                &self,
                _table: &crate::connector::backend::ResolvedTable,
            ) -> Result<TableDef, String> {
                Err("scan-binding path must not be used for registration".to_string())
            }

            fn build_schema_table_def(
                &self,
                table: &crate::connector::backend::ResolvedTable,
            ) -> Result<TableDef, String> {
                Ok(TableDef {
                    name: table.table.clone(),
                    columns: table.columns.clone(),
                    iceberg_row_lineage_metadata_columns: vec![],
                    source: ScanSource::IcebergDataFiles {
                        table: test_iceberg_table_info(),
                        files: vec![],
                        cloud_properties: Default::default(),
                        binding: crate::sql::catalog::IcebergDataFileBinding::CurrentSnapshot,
                    },
                })
            }
        }

        let resolved = crate::connector::backend::ResolvedTable {
            catalog: "ice".to_string(),
            namespace: "db".to_string(),
            table: "t".to_string(),
            columns: vec![],
        };
        let table_def = super::build_registration_table_def(&SchemaOnlySource, &resolved)
            .expect("schema-only registration");

        let ScanSource::IcebergDataFiles { files, .. } = table_def.source else {
            panic!("expected iceberg source");
        };
        assert!(files.is_empty());
    }

    #[test]
    fn scan_binding_registration_uses_table_def_source() {
        struct ScanBindingSource;
        impl crate::connector::backend::TableSource for ScanBindingSource {
            fn name(&self) -> &'static str {
                "iceberg"
            }

            fn build_table_def(
                &self,
                table: &crate::connector::backend::ResolvedTable,
            ) -> Result<TableDef, String> {
                Ok(TableDef {
                    name: table.table.clone(),
                    columns: table.columns.clone(),
                    iceberg_row_lineage_metadata_columns: vec![],
                    source: ScanSource::IcebergDataFiles {
                        table: test_iceberg_table_info(),
                        files: vec![],
                        cloud_properties: Default::default(),
                        binding: crate::sql::catalog::IcebergDataFileBinding::ExplicitFiles,
                    },
                })
            }

            fn build_schema_table_def(
                &self,
                _table: &crate::connector::backend::ResolvedTable,
            ) -> Result<TableDef, String> {
                Err("schema-only path must not be used for scan binding".to_string())
            }
        }

        let resolved = crate::connector::backend::ResolvedTable {
            catalog: "ice".to_string(),
            namespace: "db".to_string(),
            table: "t".to_string(),
            columns: vec![],
        };
        let table_def = super::build_query_registration_table_def(
            &ScanBindingSource,
            &resolved,
            super::QueryRegistrationMode::ScanBinding,
        )
        .expect("scan-binding registration");

        let ScanSource::IcebergDataFiles { binding, .. } = table_def.source else {
            panic!("expected iceberg source");
        };
        assert_eq!(
            binding,
            crate::sql::catalog::IcebergDataFileBinding::ExplicitFiles
        );
    }

    struct PerTableBindingBackend;

    impl crate::connector::backend::CatalogBackend for PerTableBindingBackend {
        fn name(&self) -> &'static str {
            "iceberg"
        }

        fn namespace_exists(&self, _: &str, _: &str) -> Result<bool, String> {
            Err("unused".to_string())
        }

        fn create_namespace(&self, _: &str, _: &str) -> Result<(), String> {
            Err("unused".to_string())
        }

        fn drop_namespace(&self, _: &str, _: &str, _: bool) -> Result<(), String> {
            Err("unused".to_string())
        }

        fn create_table(
            &self,
            _: crate::connector::backend::CreateTableRequest,
        ) -> Result<(), String> {
            Err("unused".to_string())
        }

        fn table_exists(&self, _: &str, _: &str, _: &str) -> Result<bool, String> {
            Err("unused".to_string())
        }

        fn drop_table(&self, _: &str, _: &str, _: &str, _: bool) -> Result<(), String> {
            Err("unused".to_string())
        }

        fn load_table(
            &self,
            catalog: &str,
            namespace: &str,
            table: &str,
        ) -> Result<crate::connector::backend::ResolvedTable, String> {
            Ok(crate::connector::backend::ResolvedTable {
                catalog: catalog.to_string(),
                namespace: namespace.to_string(),
                table: table.to_string(),
                columns: vec![],
            })
        }
    }

    struct PerTableBindingSource;

    impl crate::connector::backend::TableSource for PerTableBindingSource {
        fn name(&self) -> &'static str {
            "iceberg"
        }

        fn build_table_def(
            &self,
            table: &crate::connector::backend::ResolvedTable,
        ) -> Result<TableDef, String> {
            Ok(table_def_with_binding(
                table,
                crate::sql::catalog::IcebergDataFileBinding::ExplicitFiles,
            ))
        }

        fn build_schema_table_def(
            &self,
            table: &crate::connector::backend::ResolvedTable,
        ) -> Result<TableDef, String> {
            Ok(table_def_with_binding(
                table,
                crate::sql::catalog::IcebergDataFileBinding::CurrentSnapshot,
            ))
        }
    }

    fn table_def_with_binding(
        table: &crate::connector::backend::ResolvedTable,
        binding: crate::sql::catalog::IcebergDataFileBinding,
    ) -> TableDef {
        let mut iceberg = test_iceberg_table_info();
        iceberg.catalog = table.catalog.clone();
        iceberg.namespace = table.namespace.clone();
        iceberg.table = table.table.clone();
        TableDef {
            name: table.table.clone(),
            columns: table.columns.clone(),
            iceberg_row_lineage_metadata_columns: vec![],
            source: ScanSource::IcebergDataFiles {
                table: iceberg,
                files: vec![],
                cloud_properties: Default::default(),
                binding,
            },
        }
    }

    fn state_with_per_table_binding_source() -> std::sync::Arc<crate::engine::StandaloneState> {
        let state = std::sync::Arc::new(crate::engine::StandaloneState::default());
        {
            let mut catalogs = state.iceberg_catalogs.write().expect("iceberg catalogs");
            catalogs
                .create_catalog(
                    "ice",
                    &[
                        ("type".to_string(), "iceberg".to_string()),
                        ("iceberg.catalog.type".to_string(), "memory".to_string()),
                        (
                            "iceberg.catalog.warehouse".to_string(),
                            "file:///tmp/novarocks-per-table-binding".to_string(),
                        ),
                    ],
                )
                .expect("create iceberg catalog");
        }
        {
            let mut connectors = state.connectors.write().expect("connector registry write");
            connectors.register_catalog_backend(std::sync::Arc::new(PerTableBindingBackend));
            connectors.register_table_source(std::sync::Arc::new(PerTableBindingSource));
        }
        state
    }

    fn registered_binding(
        state: &crate::engine::StandaloneState,
        namespace: &str,
        table: &str,
    ) -> crate::sql::catalog::IcebergDataFileBinding {
        let guard = state.catalog.read().expect("catalog read");
        let table_def = crate::sql::catalog::CatalogProvider::get_table(&*guard, namespace, table)
            .expect("registered table");
        let ScanSource::IcebergDataFiles { binding, .. } = table_def.source else {
            panic!("expected iceberg source");
        };
        binding
    }

    #[test]
    fn partition_metadata_scan_binding_is_per_table() {
        let state = state_with_per_table_binding_source();
        let query = parse_query_for_table_names(
            "SELECT * FROM plain JOIN ice.db.parted.__nr_meta_partitions__ ON true",
        );

        super::register_external_tables_for_query(&state, Some("ice"), "db", &query)
            .expect("register external tables");

        assert_eq!(
            registered_binding(&state, "db", "plain"),
            crate::sql::catalog::IcebergDataFileBinding::CurrentSnapshot
        );
        assert_eq!(
            registered_binding(&state, "db", "parted"),
            crate::sql::catalog::IcebergDataFileBinding::ExplicitFiles
        );
    }

    fn parse_query_for_table_names(sql: &str) -> sqlparser::ast::Query {
        let stmt = crate::sql::parser::parse_sql_raw(sql).expect("parse sql");
        let sqlparser::ast::Statement::Query(query) = stmt else {
            panic!("expected query statement");
        };
        *query
    }

    // --- query_table_names tests ---
    // These tests verify that 1-part, 2-part and 3-part table references are
    // collected correctly under various catalog / database contexts.

    #[test]
    fn query_table_names_one_part_with_catalog() {
        // 1-part unqualified name: collected as 1-part when catalog is set.
        let query = parse_query_for_table_names("SELECT * FROM t");
        let names = super::query_table_names(Some("mycat"), &query);
        assert!(
            names.iter().any(|n| n.parts == vec!["t"]),
            "expected 1-part name 't', got {names:?}",
        );
    }

    #[test]
    fn query_table_names_one_part_without_catalog() {
        // Without a catalog nothing is collected (no iceberg session context).
        let query = parse_query_for_table_names("SELECT * FROM t");
        let names = super::query_table_names(None, &query);
        // Only 3-part refs are ever collected regardless of catalog; a bare
        // 1-part name with no catalog should not appear.
        assert!(
            names.iter().all(|n| n.parts.len() == 3),
            "expected only 3-part names without catalog, got {names:?}",
        );
    }

    #[test]
    fn query_table_names_two_part_with_catalog() {
        // 2-part `db.table` reference: collected as 2-part ObjectName so the
        // registration resolves against `db`, not the session current_database.
        let query = parse_query_for_table_names("SELECT * FROM testdb.t_src");
        let names = super::query_table_names(Some("mycat"), &query);
        assert!(
            names.iter().any(|n| n.parts == vec!["testdb", "t_src"]),
            "expected 2-part name ['testdb', 't_src'], got {names:?}",
        );
        // Must not also appear as a bare 1-part name.
        assert!(
            !names.iter().any(|n| n.parts == vec!["t_src"]),
            "2-part ref must not also appear as 1-part, got {names:?}",
        );
    }

    #[test]
    fn query_table_names_two_part_without_catalog() {
        // Without a catalog context 2-part names are not collected either.
        let query = parse_query_for_table_names("SELECT * FROM testdb.t_src");
        let names = super::query_table_names(None, &query);
        assert!(
            names.iter().all(|n| n.parts.len() == 3),
            "expected only 3-part names without catalog, got {names:?}",
        );
    }

    #[test]
    fn query_table_names_three_part_always_collected() {
        // 3-part names are always collected regardless of catalog.
        let query = parse_query_for_table_names("SELECT * FROM cat.db.t");
        let names_with_cat = super::query_table_names(Some("other"), &query);
        let names_no_cat = super::query_table_names(None, &query);
        let expected = ObjectName {
            parts: vec!["cat".to_string(), "db".to_string(), "t".to_string()],
        };
        assert!(
            names_with_cat.iter().any(|n| n == &expected),
            "3-part ref missing with catalog, got {names_with_cat:?}",
        );
        assert!(
            names_no_cat.iter().any(|n| n == &expected),
            "3-part ref missing without catalog, got {names_no_cat:?}",
        );
    }

    #[test]
    fn partitions_metadata_query_requires_scan_binding_table_def() {
        let query = parse_query_for_table_names("SELECT * FROM ice.db.t.__nr_meta_partitions__");

        assert!(super::query_requires_partition_metadata_files(&query));
    }

    #[test]
    fn partitions_metadata_query_detects_projection_subquery() {
        let query = parse_query_for_table_names(
            "SELECT EXISTS (SELECT 1 FROM ice.db.t.__nr_meta_partitions__)",
        );

        assert!(super::query_requires_partition_metadata_files(&query));
    }

    #[test]
    fn partitions_metadata_query_detects_where_subquery() {
        let query = parse_query_for_table_names(
            "SELECT 1 WHERE EXISTS (SELECT 1 FROM ice.db.t.__nr_meta_partitions__)",
        );

        assert!(super::query_requires_partition_metadata_files(&query));
    }

    #[test]
    fn partitions_metadata_query_detects_join_on_subquery() {
        let query = parse_query_for_table_names(
            "SELECT * FROM base JOIN other ON EXISTS (SELECT 1 FROM ice.db.t.__nr_meta_partitions__)",
        );

        assert!(super::query_requires_partition_metadata_files(&query));
    }

    #[test]
    fn partitions_metadata_query_detects_group_by_subquery() {
        let query = parse_query_for_table_names(
            "SELECT count(*) FROM base GROUP BY EXISTS (SELECT 1 FROM ice.db.t.__nr_meta_partitions__)",
        );

        assert!(super::query_requires_partition_metadata_files(&query));
    }

    #[test]
    fn partitions_metadata_query_detects_query_order_by_subquery() {
        let query = parse_query_for_table_names(
            "SELECT * FROM base ORDER BY EXISTS (SELECT 1 FROM ice.db.t.__nr_meta_partitions__)",
        );

        assert!(super::query_requires_partition_metadata_files(&query));
    }

    #[test]
    fn query_table_names_two_part_not_duplicated_as_one_part() {
        // When the query has `testdb.t_src`, only the 2-part form should be in
        // the output. The 1-part name `t_src` must not also appear.
        let query =
            parse_query_for_table_names("SELECT * FROM testdb.t_src JOIN testdb.t_sink ON true");
        let names = super::query_table_names(Some("mycat"), &query);
        let one_part_count = names.iter().filter(|n| n.parts.len() == 1).count();
        assert_eq!(
            one_part_count, 0,
            "expected no 1-part names when all refs are 2-part, got {names:?}",
        );
        assert!(
            names.iter().any(|n| n.parts == vec!["testdb", "t_src"]),
            "missing ['testdb', 't_src'], got {names:?}",
        );
        assert!(
            names.iter().any(|n| n.parts == vec!["testdb", "t_sink"]),
            "missing ['testdb', 't_sink'], got {names:?}",
        );
    }

    fn file(change_op: Option<i8>) -> IcebergFileForQuery {
        IcebergFileForQuery {
            path: "file:///tmp/data.parquet".to_string(),
            size: 10,
            record_count: Some(1),
            partition_spec_id: None,
            partition_key: None,
            first_row_id: None,
            data_sequence_number: None,
            change_op,
            row_id_allow_list: None,
        }
    }

    #[test]
    fn delta_table_builder_rejects_untagged_file() {
        let err = super::validate_delta_file_change_ops(&[file(None)])
            .expect_err("untagged delta file must fail");

        assert!(err.contains("__change_op"));
        assert!(err.contains("missing"));
    }

    #[test]
    fn delta_table_builder_rejects_invalid_change_op() {
        let err = super::validate_delta_file_change_ops(&[file(Some(0))])
            .expect_err("invalid delta file must fail");

        assert!(err.contains("__change_op"));
        assert!(err.contains("invalid value 0"));
    }

    #[test]
    fn delta_table_builder_stamps_s3_files_and_adds_virtual_column() {
        let mut table_def = TableDef {
            name: "t".to_string(),
            columns: vec![],
            iceberg_row_lineage_metadata_columns: vec![],
            source: ScanSource::IcebergDataFiles {
                table: test_iceberg_table_info(),
                files: vec![crate::sql::catalog::IcebergDataFileInfo {
                    path: "file:///tmp/data.parquet".to_string(),
                    size: 10,
                    row_count: Some(1),
                    column_stats: None,
                    partition_spec_id: None,
                    partition_key: None,
                    first_row_id: None,
                    data_sequence_number: None,
                    ivm_change_op: None,
                    delete_files: vec![],
                    manifest_path: None,
                    partition_values: vec![],
                }],
                cloud_properties: Default::default(),
                binding: crate::sql::catalog::IcebergDataFileBinding::ExplicitFiles,
            },
        };

        super::stamp_delta_table_def_change_ops(&mut table_def, &[1]).expect("stamp");

        assert_eq!(
            table_def
                .iceberg_row_lineage_metadata_columns
                .iter()
                .map(|col| (col.name.as_str(), &col.data_type, col.nullable))
                .collect::<Vec<_>>(),
            vec![("__change_op", &arrow::datatypes::DataType::Int8, false)]
        );
        let ScanSource::IcebergDataFiles { files, .. } = &table_def.source else {
            panic!("expected s3 parquet storage");
        };
        assert_eq!(files[0].ivm_change_op, Some(1));
    }

    #[test]
    fn delta_table_builder_preserves_row_lineage_metadata_and_adds_change_op() {
        let mut table_def = TableDef {
            name: "t".to_string(),
            columns: vec![],
            iceberg_row_lineage_metadata_columns: vec![
                crate::sql::catalog::ColumnDef {
                    name: "_file".to_string(),
                    data_type: arrow::datatypes::DataType::Utf8,
                    nullable: false,
                    write_default: None,
                    logical_type: None,
                },
                crate::sql::catalog::ColumnDef {
                    name: "_pos".to_string(),
                    data_type: arrow::datatypes::DataType::Int64,
                    nullable: false,
                    write_default: None,
                    logical_type: None,
                },
                crate::sql::catalog::ColumnDef {
                    name: "_row_id".to_string(),
                    data_type: arrow::datatypes::DataType::Int64,
                    nullable: false,
                    write_default: None,
                    logical_type: None,
                },
                crate::sql::catalog::ColumnDef {
                    name: "_last_updated_sequence_number".to_string(),
                    data_type: arrow::datatypes::DataType::Int64,
                    nullable: false,
                    write_default: None,
                    logical_type: None,
                },
            ],
            source: ScanSource::IcebergDataFiles {
                table: test_iceberg_table_info(),
                files: vec![crate::sql::catalog::IcebergDataFileInfo {
                    path: "file:///tmp/data.parquet".to_string(),
                    size: 10,
                    row_count: Some(1),
                    column_stats: None,
                    partition_spec_id: None,
                    partition_key: None,
                    first_row_id: None,
                    data_sequence_number: None,
                    ivm_change_op: None,
                    delete_files: vec![],
                    manifest_path: None,
                    partition_values: vec![],
                }],
                cloud_properties: Default::default(),
                binding: crate::sql::catalog::IcebergDataFileBinding::ExplicitFiles,
            },
        };

        super::stamp_delta_table_def_change_ops(&mut table_def, &[-1]).expect("stamp");

        assert_eq!(
            table_def
                .iceberg_row_lineage_metadata_columns
                .iter()
                .map(|col| (col.name.as_str(), &col.data_type, col.nullable))
                .collect::<Vec<_>>(),
            vec![
                ("_file", &arrow::datatypes::DataType::Utf8, false),
                ("_pos", &arrow::datatypes::DataType::Int64, false),
                ("_row_id", &arrow::datatypes::DataType::Int64, false),
                (
                    "_last_updated_sequence_number",
                    &arrow::datatypes::DataType::Int64,
                    false,
                ),
                ("__change_op", &arrow::datatypes::DataType::Int8, false),
            ]
        );
        let ScanSource::IcebergDataFiles { files, .. } = &table_def.source else {
            panic!("expected s3 parquet storage");
        };
        assert_eq!(files[0].ivm_change_op, Some(-1));
    }

    #[test]
    fn delta_table_builder_accepts_empty_iceberg_storage() {
        // The IVM-A1 delta source `stamp_delta_table_def_change_ops`
        // requires the base table to be backed by `IcebergDataFiles`
        // (real or synthetic). An empty Iceberg snapshot legitimately
        // produces `IcebergDataFiles { files: vec![] }` (see
        // `connector/iceberg/catalog/backend.rs::empty_iceberg_scan_source`);
        // ensure that path round-trips correctly when stamping with an
        // empty change-op slice.
        let mut table_def = TableDef {
            name: "t".to_string(),
            columns: vec![],
            iceberg_row_lineage_metadata_columns: vec![],
            source: ScanSource::IcebergDataFiles {
                table: test_iceberg_table_info(),
                files: Vec::new(),
                cloud_properties: Default::default(),
                binding: crate::sql::catalog::IcebergDataFileBinding::ExplicitFiles,
            },
        };

        super::stamp_delta_table_def_change_ops(&mut table_def, &[])
            .expect("stamp empty delta over empty iceberg storage");

        assert_eq!(
            table_def
                .iceberg_row_lineage_metadata_columns
                .iter()
                .map(|col| (col.name.as_str(), &col.data_type, col.nullable))
                .collect::<Vec<_>>(),
            vec![("__change_op", &arrow::datatypes::DataType::Int8, false)]
        );
        let ScanSource::IcebergDataFiles { files, .. } = &table_def.source else {
            panic!("expected empty delta to use s3 parquet storage");
        };
        assert!(files.is_empty());
    }
}
