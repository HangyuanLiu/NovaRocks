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

//! Immutable materialized-view rewrite facts frozen by application admission.
//!
//! The compiler uses this value as data only. Repository enumeration and
//! connector/catalog reads happen before construction, in the application
//! facade, so one statement never observes a changing MV definition set.

use std::collections::BTreeMap;

use crate::sql::catalog::PlannerTableProvider;
use crate::sql::column_id::ColumnRefFactory;
use crate::sql::optimizer::cascades_rules::mv_rewrite::{
    MvRewriteCandidate, descriptor::SpjgDescriptor,
};
use crate::sql::planner::logical::LogicalPlanNode;
use crate::sql::planner::table::ScanSource;

use super::{SqlFunctionCatalog, SqlStatisticsPlan, SqlStatisticsSnapshot};

/// The maximum number of successfully prepared candidates considered by one
/// statement. Failed or stale definitions do not consume this budget.
pub(crate) const MAX_SUCCESSFUL_MV_REWRITE_CANDIDATES: usize = 16;

/// An optional-rewrite failure recorded by the SQL kernel. The application
/// owns logging policy and may render these diagnostics without handing the
/// compiler an ambient logger.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct SqlMvRewriteDiagnostic {
    pub(crate) mv_id: Option<i64>,
    pub(crate) message: String,
}

pub(crate) struct SqlMvRewritePreparation {
    pub(crate) candidates: Vec<MvRewriteCandidate>,
    pub(crate) diagnostics: Vec<SqlMvRewriteDiagnostic>,
}

/// One captured base-table identity at statement admission.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum MvRewriteBaseTableState {
    Resolved {
        snapshot_id: Option<i64>,
        table_uuid: Option<String>,
    },
    Unavailable(String),
}

/// Immutable facts required to assess one persisted MV as a rewrite candidate.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct MvRewriteDefinition {
    pub(crate) mv_id: i64,
    pub(crate) select_sql: String,
    pub(crate) base_table_refs: Vec<String>,
    pub(crate) storage_engine: String,
    pub(crate) target_catalog: Option<String>,
    pub(crate) target_namespace: Option<String>,
    pub(crate) target_table: Option<String>,
    pub(crate) last_refresh_snapshots: BTreeMap<String, i64>,
    pub(crate) last_refresh_table_uuids: BTreeMap<String, String>,
    /// Per-base-table reads (including failures) captured while admission
    /// froze this definition. The map is keyed by canonical `cat.ns.tbl`.
    pub(crate) base_table_states: BTreeMap<String, MvRewriteBaseTableState>,
}

/// Repository-order-preserving MV definition snapshot for one compiler request.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(crate) struct MvRewriteDefinitionIndex {
    definitions: Vec<MvRewriteDefinition>,
}

impl MvRewriteDefinitionIndex {
    pub(crate) fn new(definitions: Vec<MvRewriteDefinition>) -> Self {
        Self { definitions }
    }

    pub(crate) fn definitions(&self) -> &[MvRewriteDefinition] {
        &self.definitions
    }
}

struct PreparedMvRewriteCandidate {
    mv_name: String,
    mv: SpjgDescriptor,
    mv_scalars: crate::sql::optimizer::scalar::ScalarArena,
    target_database: String,
    target_table: crate::sql::planner::table::TableDef,
}

/// Prepare optional MV rewrite candidates from one immutable, repository-order
/// definition index. This is deliberately SQL-owned: application admission
/// freezes definitions and base-table observations, while parse/analyze,
/// descriptor construction, statistics, and warn-and-skip selection happen in
/// the canonical compiler kernel.
pub(crate) fn prepare_candidates(
    definitions: &MvRewriteDefinitionIndex,
    analyzer_catalog: &dyn PlannerTableProvider,
    current_database: &str,
    logical: &LogicalPlanNode,
    factory: &mut ColumnRefFactory,
    functions: &dyn SqlFunctionCatalog,
    statistics_context: &dyn SqlStatisticsSnapshot,
    query_stats: &mut SqlStatisticsPlan,
    optimizer_settings: &crate::sql::optimizer::options::SessionOptimizerSettings,
) -> SqlMvRewritePreparation {
    if !optimizer_settings.mv_rewrite_enabled() {
        return SqlMvRewritePreparation {
            candidates: Vec::new(),
            diagnostics: Vec::new(),
        };
    }

    let mut query_fqns = Vec::new();
    collect_iceberg_fqns(logical, &mut query_fqns);
    if query_fqns.is_empty() {
        return SqlMvRewritePreparation {
            candidates: Vec::new(),
            diagnostics: Vec::new(),
        };
    }

    let mut candidates = Vec::new();
    let mut diagnostics = Vec::new();
    for definition in definitions.definitions() {
        if candidates.len() >= MAX_SUCCESSFUL_MV_REWRITE_CANDIDATES {
            diagnostics.push(SqlMvRewriteDiagnostic {
                mv_id: None,
                message: format!(
                    "mv rewrite: candidate cap {MAX_SUCCESSFUL_MV_REWRITE_CANDIDATES} reached, rest skipped"
                ),
            });
            break;
        }
        if definition.storage_engine != "iceberg"
            || !definition
                .base_table_refs
                .iter()
                .any(|base| query_fqns.contains(base))
        {
            continue;
        }
        match build_candidate(
            analyzer_catalog,
            current_database,
            definition,
            factory,
            functions,
        ) {
            Ok(Some(candidate)) => {
                let (label, stats) = statistics_context
                    .collect_table_statistics(&candidate.target_database, &candidate.target_table);
                let target_stats_ref = query_stats.add_stats(label, stats);
                candidates.push(MvRewriteCandidate {
                    mv_name: candidate.mv_name,
                    mv: candidate.mv,
                    mv_scalars: candidate.mv_scalars,
                    target_database: candidate.target_database,
                    target_table: candidate.target_table,
                    target_stats_ref,
                });
            }
            Ok(None) => {}
            Err(error) => diagnostics.push(SqlMvRewriteDiagnostic {
                mv_id: Some(definition.mv_id),
                message: format!("mv rewrite: skipping frozen candidate: {error}"),
            }),
        }
    }
    SqlMvRewritePreparation {
        candidates,
        diagnostics,
    }
}

fn build_candidate(
    analyzer_catalog: &dyn PlannerTableProvider,
    current_database: &str,
    definition: &MvRewriteDefinition,
    factory: &mut ColumnRefFactory,
    functions: &dyn SqlFunctionCatalog,
) -> Result<Option<PreparedMvRewriteCandidate>, String> {
    if definition.last_refresh_snapshots.is_empty() || !definition_is_fresh(definition)? {
        return Ok(None);
    }

    let select = parse_select_query(&definition.select_sql)?;
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
    let mut mv_scalars = crate::sql::optimizer::scalar::ScalarArena::new();
    let mv_opt_expr = crate::sql::planner::optimizer_bridge::logical::try_to_optimizer_expr(
        &mv_logical,
        &mut mv_scalars,
    )?;
    let mv = SpjgDescriptor::from_opt_expr(&mv_opt_expr, &mut mv_scalars)?;
    if mv.joins.is_some() {
        return Ok(None);
    }
    let ScanSource::IcebergDataFiles { table, .. } = &mv.table.source else {
        return Ok(None);
    };
    let scan_fqn = format!("{}.{}.{}", table.catalog, table.namespace, table.table);
    if !definition.base_table_refs.contains(&scan_fqn) {
        return Err(format!(
            "mv select resolved to {scan_fqn}, not in recorded base refs"
        ));
    }
    let (Some(catalog), Some(namespace), Some(table)) = (
        &definition.target_catalog,
        &definition.target_namespace,
        &definition.target_table,
    ) else {
        return Ok(None);
    };
    let target_table = analyzer_catalog
        .resolve_table_for_analysis(Some(catalog), namespace, table)?
        .planner;
    let mut names = mv
        .outputs
        .iter()
        .map(|output| output.name.as_str())
        .collect::<Vec<_>>();
    names.sort_unstable();
    if names.windows(2).any(|pair| pair[0] == pair[1]) {
        return Ok(None);
    }
    *factory = returned;
    Ok(Some(PreparedMvRewriteCandidate {
        mv_name: table.to_string(),
        mv,
        mv_scalars,
        target_database: namespace.to_string(),
        target_table,
    }))
}

fn parse_select_query(sql: &str) -> Result<sqlparser::ast::Query, String> {
    let normalized = crate::sql::parser::dialect::normalize_for_raw_parse(sql)
        .map_err(|error| format!("stored MV SELECT normalize error: {error}"))?;
    let statement = crate::sql::parser::parse_normalized_sql_raw(&normalized)
        .map_err(|error| format!("stored MV SQL parse error: {error}"))?;
    let sqlparser::ast::Statement::Query(query) = statement else {
        return Err("stored MV SQL must be a SELECT query".to_string());
    };
    Ok(*query)
}

fn definition_is_fresh(definition: &MvRewriteDefinition) -> Result<bool, String> {
    for base in &definition.base_table_refs {
        let Some(pinned_snapshot) = definition.last_refresh_snapshots.get(base) else {
            return Ok(false);
        };
        match definition.base_table_states.get(base) {
            Some(MvRewriteBaseTableState::Resolved {
                snapshot_id,
                table_uuid,
            }) => {
                if *snapshot_id != Some(*pinned_snapshot) {
                    return Ok(false);
                }
                if let Some(pinned_uuid) = definition.last_refresh_table_uuids.get(base)
                    && table_uuid.as_deref() != Some(pinned_uuid.as_str())
                {
                    return Ok(false);
                }
            }
            Some(MvRewriteBaseTableState::Unavailable(error)) => {
                return Err(format!("read frozen base table {base}: {error}"));
            }
            None => return Err(format!("missing frozen base table state for {base}")),
        }
    }
    Ok(true)
}

fn collect_iceberg_fqns(plan: &LogicalPlanNode, output: &mut Vec<String>) {
    if let crate::sql::planner::logical::LogicalPlanKind::Scan(scan) = &plan.kind
        && let ScanSource::IcebergDataFiles { table, .. } = &scan.table.source
    {
        let fqn = format!("{}.{}.{}", table.catalog, table.namespace, table.table);
        if !output.contains(&fqn) {
            output.push(fqn);
        }
    }
    for child in &plan.children {
        collect_iceberg_fqns(child, output);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
    fn sqlx1_mv_rewrite_definition_index_preserves_application_order() {
        let index = MvRewriteDefinitionIndex::new(vec![
            MvRewriteDefinition {
                mv_id: 7,
                select_sql: "select 1".to_string(),
                base_table_refs: Vec::new(),
                storage_engine: "iceberg".to_string(),
                target_catalog: None,
                target_namespace: None,
                target_table: None,
                last_refresh_snapshots: BTreeMap::new(),
                last_refresh_table_uuids: BTreeMap::new(),
                base_table_states: BTreeMap::new(),
            },
            MvRewriteDefinition {
                mv_id: 3,
                select_sql: "select 2".to_string(),
                base_table_refs: Vec::new(),
                storage_engine: "iceberg".to_string(),
                target_catalog: None,
                target_namespace: None,
                target_table: None,
                last_refresh_snapshots: BTreeMap::new(),
                last_refresh_table_uuids: BTreeMap::new(),
                base_table_states: BTreeMap::new(),
            },
        ]);

        assert_eq!(
            index
                .definitions()
                .iter()
                .map(|definition| definition.mv_id)
                .collect::<Vec<_>>(),
            vec![7, 3]
        );
    }

    #[test]
    fn sqlx2_mv_frozen_snapshot_and_uuid_decide_candidate_freshness() {
        let fresh = frozen_definition(MvRewriteBaseTableState::Resolved {
            snapshot_id: Some(42),
            table_uuid: Some("original-uuid".to_string()),
        });
        let stale = frozen_definition(MvRewriteBaseTableState::Resolved {
            snapshot_id: Some(43),
            table_uuid: Some("original-uuid".to_string()),
        });
        let recreated = frozen_definition(MvRewriteBaseTableState::Resolved {
            snapshot_id: Some(42),
            table_uuid: Some("replacement-uuid".to_string()),
        });

        assert_eq!(definition_is_fresh(&fresh), Ok(true));
        assert_eq!(definition_is_fresh(&stale), Ok(false));
        assert_eq!(definition_is_fresh(&recreated), Ok(false));
    }

    #[test]
    fn sqlx2_mv_frozen_read_failure_stays_a_warn_and_skip_input() {
        let unavailable = frozen_definition(MvRewriteBaseTableState::Unavailable(
            "catalog unavailable".to_string(),
        ));

        assert!(matches!(
            definition_is_fresh(&unavailable),
            Err(error) if error.contains("catalog unavailable")
        ));
    }

    #[test]
    fn sqlx2_mv_candidate_limit_is_sixteen_successes() {
        assert_eq!(MAX_SUCCESSFUL_MV_REWRITE_CANDIDATES, 16);
    }
}
