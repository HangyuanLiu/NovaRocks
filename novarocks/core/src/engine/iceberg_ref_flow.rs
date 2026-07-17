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
    let target = crate::engine::backend_resolver::TargetBackend {
        backend_name: "iceberg",
        catalog: catalog_name.clone(),
        namespace: namespace.clone(),
        table: table_name.clone(),
    };
    crate::engine::mv::iceberg_guard::reject_if_iceberg_mv_properties(
        &target,
        loaded.table.metadata().properties(),
        crate::engine::mv::iceberg_guard::IcebergMvUserMutation::AlterTable,
    )?;
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
    //    build_iceberg_catalog dispatches Hadoop / REST / Hive.
    let catalog = crate::connector::iceberg::catalog::registry::build_iceberg_catalog(&entry)?;
    crate::connector::iceberg::catalog::registry::block_on_iceberg(async {
        crate::connector::iceberg::commit::execute_ref_action(catalog.as_ref(), &connector_plan)
            .await
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

#[cfg(test)]
mod tests {
    #[test]
    fn ref_action_guard_runs_after_load_and_before_analysis_or_commit() {
        let src = include_str!("iceberg_ref_flow.rs");
        let execute_body = src
            .split("#[cfg(test)]")
            .next()
            .expect("source before tests");
        let load_pos = execute_body
            .find("let loaded =")
            .expect("execute must load table metadata");
        let guard_pos = execute_body
            .find("reject_if_iceberg_mv_properties")
            .expect("ref actions must guard Iceberg MV tables");
        let analysis_pos = execute_body
            .find("analyze_alter_iceberg_ref")
            .expect("execute must analyze ref action");
        let commit_pos = execute_body
            .find("execute_ref_action")
            .expect("execute must commit ref action");

        assert!(load_pos < guard_pos, "guard must run after table load");
        assert!(
            guard_pos < analysis_pos,
            "guard must run before ref action analysis"
        );
        assert!(
            guard_pos < commit_pos,
            "guard must run before ref action commit"
        );
    }
}
