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

//! MV rewrite candidate preparation (engine side).
//!
//! Runs after plan_query and before optimize(): discovers fresh Iceberg MVs
//! related to the query's base tables, re-analyzes their defining SQL with
//! the query's ColumnRefFactory, validates the SPJG shape, builds the
//! executable target TableDef, and loads target-table statistics.
//! Every failure is a warn-and-skip: rewrite is an optional optimization.

use std::sync::Arc;

use crate::sql::catalog::PlannerTableProvider;
use crate::sql::column_id::ColumnRefFactory;
use crate::sql::compiler::mv_rewrite::{
    MvRewriteBaseTableState, MvRewriteDefinition, MvRewriteDefinitionIndex,
};
use crate::sql::optimizer::cascades_rules::mv_rewrite::{
    MvRewriteCandidate, descriptor::SpjgDescriptor,
};
use crate::sql::planner::logical::LogicalPlanNode;
use crate::sql::planner::table::ScanSource;

use super::StandaloneState;

/// Upper bound on candidates per query; aligned with the StarRocks default
/// cbo_materialized_view_rewrite_related_mvs_limit = 16.
const MAX_MV_CANDIDATES: usize = 16;

/// Freeze the repository-order MV definition set and every base-table
/// freshness fact required by rewrite selection.
///
/// This is deliberately an application-facade operation: it is the only
/// place in this module that names `StandaloneState`, the MV repository, or
/// an Iceberg catalog. The compiler-facing preparation below receives the
/// resulting immutable value and therefore cannot observe repository or
/// connector changes part way through one query.
pub(crate) fn freeze_mv_rewrite_definition_index(
    state: &Arc<StandaloneState>,
) -> Result<MvRewriteDefinitionIndex, String> {
    let definitions = state
        .mv_repository
        .list_definitions()
        .map_err(|error| format!("list mv definitions: {error}"))?;

    Ok(MvRewriteDefinitionIndex::new(
        definitions
            .into_iter()
            .map(|definition| freeze_mv_rewrite_definition(state, definition))
            .collect(),
    ))
}

fn freeze_mv_rewrite_definition(
    state: &Arc<StandaloneState>,
    definition: crate::mv::persistence::definition::StoredMvDefinition,
) -> MvRewriteDefinition {
    let mut base_table_states = std::collections::BTreeMap::new();
    if definition.storage_engine == "iceberg" {
        for fqn in &definition.base_table_refs {
            let state = freeze_base_table_state(state, fqn)
                .unwrap_or_else(MvRewriteBaseTableState::Unavailable);
            base_table_states.insert(fqn.clone(), state);
        }
    }

    MvRewriteDefinition {
        mv_id: definition.mv_id,
        select_sql: definition.select_sql,
        base_table_refs: definition.base_table_refs,
        storage_engine: definition.storage_engine,
        target_catalog: definition.target_catalog,
        target_namespace: definition.target_namespace,
        target_table: definition.target_table,
        last_refresh_snapshots: definition.last_refresh_snapshots,
        last_refresh_table_uuids: definition.last_refresh_table_uuids,
        base_table_states,
    }
}

fn freeze_base_table_state(
    state: &Arc<StandaloneState>,
    fqn: &str,
) -> Result<MvRewriteBaseTableState, String> {
    let table_ref = crate::engine::mv::refresh_io::parse_iceberg_table_refs(&[fqn.to_string()])?
        .into_iter()
        .next()
        .expect("one table reference produces one parsed identity");
    let entry = {
        let registry = state
            .iceberg_catalogs
            .read()
            .expect("iceberg catalogs read lock");
        registry.get(&table_ref.catalog)?
    };
    let loaded = crate::connector::iceberg::catalog::load_table(
        &entry,
        &table_ref.namespace,
        &table_ref.table,
    )?;
    Ok(MvRewriteBaseTableState::Resolved {
        snapshot_id: loaded
            .table
            .metadata()
            .current_snapshot()
            .map(|snapshot| snapshot.snapshot_id()),
        table_uuid: Some(loaded.table.metadata().uuid().to_string()),
    })
}

struct PreparedMvRewriteCandidate {
    mv_name: String,
    mv: SpjgDescriptor,
    mv_scalars: crate::sql::optimizer::scalar::ScalarArena,
    target_database: String,
    target_table: crate::sql::planner::table::TableDef,
}

fn supports_current_mv_rewrite_shape(desc: &SpjgDescriptor) -> bool {
    desc.joins.is_none()
}

pub(crate) fn prepare_mv_rewrite_candidates(
    definitions: &MvRewriteDefinitionIndex,
    analyzer_catalog: &dyn PlannerTableProvider,
    current_database: &str,
    logical: &LogicalPlanNode,
    factory: &mut ColumnRefFactory,
    functions: &dyn crate::sql::compiler::SqlFunctionCatalog,
    statistics_context: &dyn crate::sql::compiler::SqlStatisticsSnapshot,
    query_stats: &mut crate::sql::compiler::SqlStatisticsPlan,
    optimizer_settings: &crate::sql::optimizer::options::SessionOptimizerSettings,
) -> Vec<MvRewriteCandidate> {
    if !optimizer_settings.mv_rewrite_enabled() {
        return Vec::new();
    }
    match try_prepare(
        definitions,
        analyzer_catalog,
        current_database,
        logical,
        factory,
        functions,
        statistics_context,
        query_stats,
    ) {
        Ok(c) => c,
        Err(e) => {
            tracing::warn!("mv rewrite candidate preparation failed: {e}");
            Vec::new()
        }
    }
}

impl crate::sql::compiler::SqlMvRewriteSnapshot for MvRewriteDefinitionIndex {
    fn prepare_candidates(
        &self,
        catalog: &dyn PlannerTableProvider,
        current_database: &str,
        logical: &LogicalPlanNode,
        factory: &mut ColumnRefFactory,
        functions: &dyn crate::sql::compiler::SqlFunctionCatalog,
        statistics: &dyn crate::sql::compiler::SqlStatisticsSnapshot,
        query_stats: &mut crate::sql::compiler::SqlStatisticsPlan,
        optimizer_settings: &crate::sql::optimizer::options::SessionOptimizerSettings,
    ) -> Vec<MvRewriteCandidate> {
        prepare_mv_rewrite_candidates(
            self,
            catalog,
            current_database,
            logical,
            factory,
            functions,
            statistics,
            query_stats,
            optimizer_settings,
        )
    }
}

fn try_prepare(
    definitions: &MvRewriteDefinitionIndex,
    analyzer_catalog: &dyn PlannerTableProvider,
    current_database: &str,
    logical: &LogicalPlanNode,
    factory: &mut ColumnRefFactory,
    functions: &dyn crate::sql::compiler::SqlFunctionCatalog,
    statistics_context: &dyn crate::sql::compiler::SqlStatisticsSnapshot,
    query_stats: &mut crate::sql::compiler::SqlStatisticsPlan,
) -> Result<Vec<MvRewriteCandidate>, String> {
    // 1. Iceberg base tables referenced by the query, as "cat.ns.tbl" FQNs
    //    (the exact format of StoredMvDefinition.base_table_refs, produced
    //    by TableIdentity::fqn at MV creation).
    let mut query_fqns: Vec<String> = Vec::new();
    collect_iceberg_fqns(logical, &mut query_fqns);
    if query_fqns.is_empty() {
        return Ok(Vec::new());
    }

    let mut candidates = Vec::new();
    for def in definitions.definitions() {
        if candidates.len() >= MAX_MV_CANDIDATES {
            tracing::warn!("mv rewrite: candidate cap {MAX_MV_CANDIDATES} reached, rest skipped");
            break;
        }
        // Storage filter only. In-flight refresh does NOT disqualify a
        // candidate: pins always point at committed snapshots.
        if def.storage_engine != "iceberg" {
            continue;
        }
        if !def.base_table_refs.iter().any(|r| query_fqns.contains(r)) {
            continue;
        }
        match build_candidate(analyzer_catalog, current_database, def, factory, functions) {
            Ok(Some(c)) => {
                let (target_label, target_stats) = statistics_context
                    .collect_table_statistics(&c.target_database, &c.target_table);
                let target_stats_ref = query_stats.add_stats(target_label, target_stats);
                candidates.push(MvRewriteCandidate {
                    mv_name: c.mv_name,
                    mv: c.mv,
                    mv_scalars: c.mv_scalars,
                    target_database: c.target_database,
                    target_table: c.target_table,
                    target_stats_ref,
                });
            }
            Ok(None) => {}
            Err(e) => tracing::warn!("mv rewrite: skipping mv {}: {e}", def.mv_id),
        }
    }
    Ok(candidates)
}

fn build_candidate(
    analyzer_catalog: &dyn PlannerTableProvider,
    current_database: &str,
    def: &MvRewriteDefinition,
    factory: &mut ColumnRefFactory,
    functions: &dyn crate::sql::compiler::SqlFunctionCatalog,
) -> Result<Option<PreparedMvRewriteCandidate>, String> {
    // 2b. Strict freshness: every base table's CURRENT snapshot must equal
    //     the pinned snapshot from the last refresh. Never refreshed -> skip.
    if def.last_refresh_snapshots.is_empty() {
        return Ok(None);
    }
    if !definition_is_fresh(def)? {
        return Ok(None);
    }

    // 3. Re-analyze the defining SQL on a CLONE of the query's factory, then
    //    adopt the advanced factory only on success. A parse/plan failure here
    //    is an expected warn-and-skip (design §9: "MV SQL parse failure"), and
    //    it MUST be side-effect-free: an earlier version used
    //    `std::mem::take(factory)` and only wrote the factory back on success,
    //    so any `?` left the caller's `*factory` as a fresh Default (next_id =
    //    1). That reset factory then flowed into `optimize()`, whose RBO
    //    column-pruning auto-fill (and the MvRewrite rule) mint ColumnIds from
    //    it — colliding with the query's existing columns and corrupting even
    //    the non-rewritten plan. Cloning keeps `*factory` untouched until we
    //    have a fully analyzed+planned MV; on success the write-back threads
    //    the advanced ids so the query and every candidate stay collision-free.
    let select = crate::engine::mv::iceberg_refresh::parse_mv_select_query(&def.select_sql)?;
    let (resolved, ctes, returned) =
        crate::sql::analyzer::analyze_with_factory_and_function_catalog(
            &select,
            analyzer_catalog,
            current_database,
            factory.clone(),
            functions,
        )?;
    let mut returned = returned;
    let mv_logical = crate::sql::planner::plan_query(resolved, ctes, &mut returned)?;
    *factory = returned;
    let mut mv_scalars = crate::sql::optimizer::scalar::ScalarArena::new();
    let mv_opt_expr = crate::sql::planner::optimizer_bridge::logical::try_to_optimizer_expr(
        &mv_logical,
        &mut mv_scalars,
    )?;
    let mv_desc = SpjgDescriptor::from_opt_expr(&mv_opt_expr, &mut mv_scalars)?;
    if !supports_current_mv_rewrite_shape(&mv_desc) {
        return Ok(None);
    }

    // 3b. Fail closed on name-resolution drift: the analyzed scan must be
    //     one of the recorded base tables.
    let ScanSource::IcebergDataFiles { table, .. } = &mv_desc.table.source else {
        return Ok(None);
    };
    let scan_fqn = format!("{}.{}.{}", table.catalog, table.namespace, table.table);
    if !def.base_table_refs.contains(&scan_fqn) {
        return Err(format!(
            "mv select resolved to {scan_fqn}, not in recorded base refs"
        ));
    }

    // 4. Resolve the executable target through the same query-scoped table
    // binding store as analysis and statistics. This preserves the exact
    // control generation and avoids a second current/latest acquire.
    let (Some(cat), Some(ns), Some(tbl)) = (
        &def.target_catalog,
        &def.target_namespace,
        &def.target_table,
    ) else {
        return Ok(None);
    };
    let target_table = analyzer_catalog
        .resolve_table_for_analysis(Some(cat), ns, tbl)?
        .planner;

    // Duplicate output names break the by-name visible-column mapping.
    let mut names: Vec<&str> = mv_desc.outputs.iter().map(|o| o.name.as_str()).collect();
    names.sort_unstable();
    if names.windows(2).any(|w| w[0] == w[1]) {
        return Ok(None);
    }

    Ok(Some(PreparedMvRewriteCandidate {
        mv_name: rewrite_candidate_display_name(tbl),
        mv: mv_desc,
        mv_scalars,
        target_database: ns.clone(),
        target_table,
    }))
}

fn definition_is_fresh(def: &MvRewriteDefinition) -> Result<bool, String> {
    for fqn in &def.base_table_refs {
        let Some(pinned) = def.last_refresh_snapshots.get(fqn) else {
            return Ok(false);
        };
        match def.base_table_states.get(fqn) {
            Some(MvRewriteBaseTableState::Resolved {
                snapshot_id,
                table_uuid,
            }) => {
                if *snapshot_id != Some(*pinned) {
                    return Ok(false); // stale -> strict mode skips
                }
                if let Some(pinned_uuid) = def.last_refresh_table_uuids.get(fqn)
                    && table_uuid.as_deref() != Some(pinned_uuid.as_str())
                {
                    // table was dropped & recreated
                    return Ok(false);
                }
            }
            Some(MvRewriteBaseTableState::Unavailable(error)) => {
                return Err(format!("read frozen base table {fqn}: {error}"));
            }
            None => return Err(format!("missing frozen base table state for {fqn}")),
        }
    }
    Ok(true)
}

fn rewrite_candidate_display_name(target_table: &str) -> String {
    target_table.to_string()
}

/// Recursively collect "cat.ns.tbl" FQNs of every Iceberg data-file scan in
/// the plan. Mirrors query-stats collector scan-source coverage.
fn collect_iceberg_fqns(plan: &LogicalPlanNode, out: &mut Vec<String>) {
    match &plan.kind {
        crate::sql::planner::logical::LogicalPlanKind::Scan(s) => {
            if let ScanSource::IcebergDataFiles { table, .. } = &s.table.source {
                let fqn = format!("{}.{}.{}", table.catalog, table.namespace, table.table);
                if !out.contains(&fqn) {
                    out.push(fqn);
                }
            }
        }
        crate::sql::planner::logical::LogicalPlanKind::ImvDelta(_)
        | crate::sql::planner::logical::LogicalPlanKind::ImvVersion(_) => {
            // IMV markers never appear on the standalone query path; ignore.
        }
        _ => {}
    }
    for child in &plan.children {
        collect_iceberg_fqns(child, out);
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use arrow::datatypes::DataType;

    use super::*;
    use crate::sql::column_id::ColumnId;
    use crate::sql::common::OutputColumn;
    use crate::sql::optimizer::cascades_rules::mv_rewrite::descriptor::{
        EquiEdge, JoinInput, JoinShape,
    };

    fn table(name: &str) -> crate::sql::planner::table::TableDef {
        crate::sql::planner::table::TableDef {
            name: name.to_string(),
            columns: Vec::new(),
            iceberg_row_lineage_metadata_columns: Vec::new(),
            source: ScanSource::ConnectorPinned,
        }
    }

    fn output_column(id: u32, name: &str) -> OutputColumn {
        OutputColumn {
            column_id: ColumnId(id),
            name: name.to_string(),
            data_type: DataType::Int64,
            nullable: true,
            is_internal: false,
        }
    }

    fn descriptor_with_joins(joins: Option<JoinShape>) -> SpjgDescriptor {
        SpjgDescriptor {
            table: table("t"),
            scan_columns: Vec::new(),
            predicates: Vec::new(),
            aggregate: None,
            outputs: Vec::new(),
            joins,
        }
    }

    fn frozen_definition(state: MvRewriteBaseTableState) -> MvRewriteDefinition {
        MvRewriteDefinition {
            mv_id: 1,
            select_sql: "select 1".to_string(),
            base_table_refs: vec!["iceberg.db.base".to_string()],
            storage_engine: "iceberg".to_string(),
            target_catalog: Some("iceberg".to_string()),
            target_namespace: Some("db".to_string()),
            target_table: Some("mv_target".to_string()),
            last_refresh_snapshots: BTreeMap::from([("iceberg.db.base".to_string(), 42)]),
            last_refresh_table_uuids: BTreeMap::from([(
                "iceberg.db.base".to_string(),
                "original-uuid".to_string(),
            )]),
            base_table_states: BTreeMap::from([("iceberg.db.base".to_string(), state)]),
        }
    }

    #[test]
    fn current_mv_rewrite_shape_support_accepts_single_table_descriptor() {
        let desc = descriptor_with_joins(None);

        assert!(supports_current_mv_rewrite_shape(&desc));
    }

    #[test]
    fn current_mv_rewrite_shape_support_rejects_join_descriptor() {
        let desc = descriptor_with_joins(Some(JoinShape {
            inputs: vec![JoinInput {
                table: table("t2"),
                scan_columns: vec![output_column(2, "c")],
            }],
            equi_edges: vec![EquiEdge {
                left: ColumnId(1),
                right: ColumnId(2),
            }],
        }));

        assert!(!supports_current_mv_rewrite_shape(&desc));
    }

    #[test]
    fn rewrite_candidate_display_name_uses_target_table_name_directly() {
        assert_eq!(rewrite_candidate_display_name("agg_mv"), "agg_mv");
        assert_eq!(
            rewrite_candidate_display_name("target_agg_mv"),
            "target_agg_mv"
        );
    }

    #[test]
    fn sqlx1_mv_rewrite_uses_the_frozen_snapshot_and_uuid() {
        let definition = frozen_definition(MvRewriteBaseTableState::Resolved {
            snapshot_id: Some(42),
            table_uuid: Some("original-uuid".to_string()),
        });

        assert_eq!(definition_is_fresh(&definition), Ok(true));
    }

    #[test]
    fn sqlx1_mv_rewrite_rejects_stale_or_recreated_frozen_base_table() {
        let stale = frozen_definition(MvRewriteBaseTableState::Resolved {
            snapshot_id: Some(43),
            table_uuid: Some("original-uuid".to_string()),
        });
        let recreated = frozen_definition(MvRewriteBaseTableState::Resolved {
            snapshot_id: Some(42),
            table_uuid: Some("replacement-uuid".to_string()),
        });

        assert_eq!(definition_is_fresh(&stale), Ok(false));
        assert_eq!(definition_is_fresh(&recreated), Ok(false));
    }

    #[test]
    fn sqlx1_mv_rewrite_keeps_frozen_read_errors_as_warn_and_skip_inputs() {
        let definition = frozen_definition(MvRewriteBaseTableState::Unavailable(
            "catalog unavailable".to_string(),
        ));

        assert!(matches!(
            definition_is_fresh(&definition),
            Err(error) if error.contains("catalog unavailable")
        ));
    }
}
