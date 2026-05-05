//! Engine dispatch for `ALTER TABLE … (CREATE|DROP) BRANCH|TAG`.
//!
//! Bridges parser AST → analyzer → commit/ref_action.
//! Mirrors the `mv_flow` pattern: no ExecNode, no pipeline, just a small flow function.

use std::sync::Arc;

use crate::engine::{StandaloneState, StatementResult};
use crate::sql::parser::ast::{AlterIcebergRefStmt, ObjectName};

pub(crate) fn execute(
    state: &Arc<StandaloneState>,
    _current_database: &str,
    stmt: &AlterIcebergRefStmt,
) -> Result<StatementResult, String> {
    // 1. Resolve qualified name — must be 3-part (catalog.namespace.table).
    let (catalog_name, namespace, table_name) = resolve_table_parts(&stmt.table)?;

    // 2. Load iceberg catalog entry.
    let registry = state
        .iceberg_catalogs
        .read()
        .expect("iceberg catalogs read");
    let entry = registry.get(&catalog_name)?;

    // 3. Build a Catalog handle and load the table metadata for the analyzer.
    //    We use `load_table` (cached) to get metadata for the analyzer, then
    //    build a fresh HadoopFileSystemCatalog for the async commit path.
    let loaded =
        crate::connector::iceberg::catalog::registry::load_table(&entry, &namespace, &table_name)?;
    let metadata = loaded.table.metadata();

    // 4. Run the analyzer: validates the action against current snapshot state.
    let analyzer_plan = crate::sql::analyzer::alter_iceberg_ref::analyze_alter_iceberg_ref(
        stmt,
        &catalog_name,
        &namespace,
        &table_name,
        metadata,
    )?;

    // 5. Translate analyzer-side RefAction to connector-side RefAction.
    //    Both enums have identical variant/field layouts but live in different modules.
    //    The connector types are re-exported from `crate::connector::iceberg::commit`.
    use crate::connector::iceberg::commit::{RefAction, RefActionPlan};
    use crate::sql::analyzer::alter_iceberg_ref::RefAction as ARefAction;
    let connector_action = match analyzer_plan.action {
        ARefAction::CreateBranch {
            name,
            snapshot_id,
            replace,
            if_not_exists,
        } => RefAction::CreateBranch {
            name,
            snapshot_id,
            replace,
            if_not_exists,
        },
        ARefAction::CreateTag {
            name,
            snapshot_id,
            replace,
            if_not_exists,
        } => RefAction::CreateTag {
            name,
            snapshot_id,
            replace,
            if_not_exists,
        },
        ARefAction::DropBranch { name, if_exists } => RefAction::DropBranch { name, if_exists },
        ARefAction::DropTag { name, if_exists } => RefAction::DropTag { name, if_exists },
    };
    let connector_plan = RefActionPlan {
        catalog: catalog_name,
        namespace: namespace.clone(),
        table: table_name.clone(),
        action: connector_action,
    };

    // 6. Execute via async bridge.
    //    build_hadoop_catalog builds a HadoopFileSystemCatalog implementing `dyn Catalog`.
    let catalog = crate::connector::iceberg::catalog::registry::build_hadoop_catalog(&entry)?;
    crate::connector::iceberg::catalog::registry::block_on_iceberg(async {
        crate::connector::iceberg::commit::execute_ref_action(&catalog, &connector_plan).await
    })
    .map_err(|e| format!("iceberg ref: async runtime error: {e}"))??;

    // Invalidate the cached table metadata so subsequent reads (e.g. time-travel
    // ref resolution in `rewrite_time_travel_refs`) see the updated snapshot refs.
    entry.invalidate_table_cache(&namespace, &table_name);

    Ok(StatementResult::Ok)
}

fn resolve_table_parts(name: &ObjectName) -> Result<(String, String, String), String> {
    let parts = &name.parts;
    match parts.len() {
        3 => Ok((parts[0].clone(), parts[1].clone(), parts[2].clone())),
        2 => Err(format!(
            "iceberg ref: qualify table with catalog (got '{}.{}')",
            parts[0], parts[1]
        )),
        1 => Err(format!(
            "iceberg ref: qualify table with catalog and namespace (got '{}')",
            parts[0]
        )),
        _ => Err(format!(
            "iceberg ref: invalid table name (parts: {})",
            parts.len()
        )),
    }
}
