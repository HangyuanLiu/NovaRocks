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

use std::sync::Arc;

use crate::catalog::identifier::TableIdentity;
#[cfg(feature = "compat")]
use crate::connector::starrocks::table::model::StarRocksTableKind;
use crate::engine::StandaloneState;
use crate::meta::repository::mv::CreateMvDependencyRequest;
use crate::mv::analysis::ResolvedTableRef;
use crate::mv::dependency::graph::{
    topological_upstream_order_for_edges, validate_no_cycle_for_edges,
};
use crate::mv::dependency::model::{
    MvDependencyObjectRef, MvDependencyObjectType, MvDependencyStorageEngine,
    iceberg_mv_dependency_ref, iceberg_table_dependency_ref, starrocks_mv_dependency_ref,
};
use crate::mv::dependency::refresh::{MvRefreshDependencyStep, refresh_step_for_dependency_object};
use crate::mv::persistence::definition::StoredMvDefinition;
#[cfg(test)]
use crate::mv::persistence::definition::StoredMvRefreshPolicy;

pub(crate) struct ResolvedCreateMvDependencies {
    pub(crate) base_refs: Vec<TableIdentity>,
    pub(crate) dependencies: Vec<CreateMvDependencyRequest>,
}

pub(crate) fn ensure_no_downstream_dependencies(
    state: &Arc<StandaloneState>,
    upstream: &MvDependencyObjectRef,
) -> Result<(), String> {
    let Some(provider) = state.metadata_provider.as_ref() else {
        return Ok(());
    };
    let read = provider
        .begin_read()
        .map_err(|e| format!("open MV dependency drop guard read failed: {e}"))?;
    state
        .mv_repo
        .ensure_no_downstream_dependencies(read.as_ref(), upstream)
        .map_err(|e| e.to_string())
}

pub(crate) fn stored_definition_dependency_ref(
    definition: &StoredMvDefinition,
    starrocks_name: Option<(&str, &str)>,
) -> Result<MvDependencyObjectRef, String> {
    if definition.storage_engine.eq_ignore_ascii_case("iceberg") {
        let catalog = definition
            .target_catalog
            .as_deref()
            .ok_or_else(|| "iceberg MV definition missing target catalog".to_string())?;
        let namespace = definition
            .target_namespace
            .as_deref()
            .ok_or_else(|| "iceberg MV definition missing target namespace".to_string())?;
        let table = definition
            .target_table
            .as_deref()
            .ok_or_else(|| "iceberg MV definition missing target table".to_string())?;
        return Ok(iceberg_mv_dependency_ref(catalog, namespace, table));
    }
    let (database, table) = starrocks_name.ok_or_else(|| {
        "StarRocks table MV definition requires database/table name for dependency ref".to_string()
    })?;
    Ok(starrocks_mv_dependency_ref(database, table))
}

pub(crate) fn resolve_create_mv_dependencies(
    state: &Arc<StandaloneState>,
    resolved_refs: &[ResolvedTableRef],
    created_at_ms: i64,
) -> Result<ResolvedCreateMvDependencies, String> {
    let provider = state.metadata_provider.as_ref().ok_or_else(|| {
        "materialized view dependency resolution requires metadata provider".to_string()
    })?;
    let read = provider
        .begin_read()
        .map_err(|e| format!("open MV dependency metadata read transaction failed: {e}"))?;

    let mut base_refs = Vec::new();
    let mut dependencies = Vec::new();
    for table_ref in resolved_refs {
        match table_ref {
            ResolvedTableRef::Iceberg {
                catalog,
                namespace,
                table,
            } => {
                let is_mv_dependency = state
                    .mv_repo
                    .find_by_target(read.as_ref(), catalog, namespace, table)
                    .map_err(|e| format!("load MV target dependency failed: {e}"))?
                    .is_some();
                let base = TableIdentity {
                    catalog: catalog.clone(),
                    namespace: namespace.clone(),
                    table: table.clone(),
                };
                if !base_refs.contains(&base) {
                    base_refs.push(base.clone());
                }
                let upstream = if is_mv_dependency {
                    iceberg_mv_dependency_ref(catalog, namespace, table)
                } else {
                    iceberg_table_dependency_ref(&base)
                };
                dependencies.push(CreateMvDependencyRequest {
                    upstream,
                    created_at_ms,
                });
            }
            ResolvedTableRef::StarRocks { database, table } => {
                #[cfg(not(feature = "compat"))]
                {
                    return Err(format!(
                        "StarRocks table MV dependency `{database}.{table}` requires the compat feature"
                    ));
                }
                #[cfg(feature = "compat")]
                {
                    let starrocks = state
                        .starrocks_table
                        .read()
                        .expect("standalone StarRocks table read lock");
                    let runtime = starrocks.table(database, table).map_err(|err| {
                        format!(
                            "resolve StarRocks table MV dependency {database}.{table} failed: {err}"
                        )
                    })?;
                    if runtime.table.kind != StarRocksTableKind::MaterializedView {
                        return Err(format!(
                            "materialized view base tables must be Iceberg tables or materialized views; found StarRocks table `{database}.{table}`"
                        ));
                    }
                    return Err(format!(
                        "StarRocks table MV-on-MV dependency `{database}.{table}` is recognized but cannot be used as an incremental Iceberg base in this release"
                    ));
                }
            }
        }
    }
    if base_refs.is_empty() {
        return Err("materialized view base tables must be Iceberg tables".to_string());
    }
    Ok(ResolvedCreateMvDependencies {
        base_refs,
        dependencies,
    })
}

fn object_in_iceberg_scope(
    object: &MvDependencyObjectRef,
    scope_catalog: &str,
    scope_namespace: Option<&str>,
) -> bool {
    if object.storage_engine != MvDependencyStorageEngine::Iceberg {
        return false;
    }
    let Some(obj_catalog) = object.catalog.as_deref() else {
        return false;
    };
    if !obj_catalog.eq_ignore_ascii_case(scope_catalog) {
        return false;
    }
    if let Some(ns) = scope_namespace
        && !object.database_or_namespace.eq_ignore_ascii_case(ns)
    {
        return false;
    }
    true
}

fn iceberg_mv_target_in_scope(
    definition: &StoredMvDefinition,
    scope_catalog: &str,
    scope_namespace: Option<&str>,
) -> Option<String> {
    if !definition.storage_engine.eq_ignore_ascii_case("iceberg") {
        return None;
    }
    let catalog = definition.target_catalog.as_deref()?;
    let namespace = definition.target_namespace.as_deref()?;
    let table = definition.target_table.as_deref()?;
    if !catalog.eq_ignore_ascii_case(scope_catalog) {
        return None;
    }
    if let Some(ns) = scope_namespace
        && !namespace.eq_ignore_ascii_case(ns)
    {
        return None;
    }
    Some(format!("{catalog}.{namespace}.{table}"))
}

pub(crate) fn validate_no_iceberg_mv_targets_in_scope(
    scope_catalog: &str,
    scope_namespace: Option<&str>,
    definitions: &[StoredMvDefinition],
) -> Result<(), String> {
    let mut in_scope_targets = definitions
        .iter()
        .filter_map(|definition| {
            iceberg_mv_target_in_scope(definition, scope_catalog, scope_namespace)
        })
        .collect::<Vec<_>>();
    if in_scope_targets.is_empty() {
        return Ok(());
    }
    in_scope_targets.sort();
    in_scope_targets.dedup();
    let scope_str = match scope_namespace {
        Some(ns) => format!("`{scope_catalog}.{ns}`"),
        None => format!("`{scope_catalog}`"),
    };
    Err(format!(
        "cannot drop {scope_str}: contains materialized views: {}; use DROP MATERIALIZED VIEW first",
        in_scope_targets.join(", ")
    ))
}

pub(crate) fn ensure_no_iceberg_mv_targets_in_scope(
    state: &Arc<StandaloneState>,
    scope_catalog: &str,
    scope_namespace: Option<&str>,
) -> Result<(), String> {
    let Some(provider) = state.metadata_provider.as_ref() else {
        return Ok(());
    };
    let read = provider
        .begin_read()
        .map_err(|e| format!("open MV target drop scope read failed: {e}"))?;
    let definitions = state
        .mv_repo
        .list_definitions(read.as_ref())
        .map_err(|e| format!("load MV definitions for drop target scope check failed: {e}"))?;

    validate_no_iceberg_mv_targets_in_scope(scope_catalog, scope_namespace, &definitions)
}

/// Pure orphan-prevention check: given the full set of MV targets and their
/// upstream dependencies, reject the scope drop if any MV outside the scope
/// depends on an upstream inside the scope.
pub(crate) fn validate_no_external_dependents_for_scope(
    scope_catalog: &str,
    scope_namespace: Option<&str>,
    definitions_with_deps: &[(MvDependencyObjectRef, Vec<MvDependencyObjectRef>)],
) -> Result<(), String> {
    let mut external_dependents: Vec<String> = Vec::new();
    for (target, upstreams) in definitions_with_deps {
        let target_in_scope = object_in_iceberg_scope(target, scope_catalog, scope_namespace);
        if target_in_scope {
            continue;
        }
        for upstream in upstreams {
            if object_in_iceberg_scope(upstream, scope_catalog, scope_namespace) {
                external_dependents.push(format!(
                    "{} depends on {}",
                    target.display_name(),
                    upstream.display_name(),
                ));
                break;
            }
        }
    }

    if external_dependents.is_empty() {
        return Ok(());
    }
    external_dependents.sort();
    let scope_str = match scope_namespace {
        Some(ns) => format!("`{scope_catalog}.{ns}`"),
        None => format!("`{scope_catalog}`"),
    };
    Err(format!(
        "cannot drop {scope_str}: would orphan downstream materialized views: {}",
        external_dependents.join(", ")
    ))
}

/// State-aware wrapper around `validate_no_external_dependents_for_scope`:
/// loads MV definitions and their upstream dependencies from the repository,
/// then delegates to the pure helper.
pub(crate) fn ensure_no_external_iceberg_dependents(
    state: &Arc<StandaloneState>,
    scope_catalog: &str,
    scope_namespace: Option<&str>,
) -> Result<(), String> {
    let Some(provider) = state.metadata_provider.as_ref() else {
        return Ok(());
    };
    let read = provider
        .begin_read()
        .map_err(|e| format!("open MV dependency drop scope read failed: {e}"))?;

    let definitions = state
        .mv_repo
        .list_definitions(read.as_ref())
        .map_err(|e| format!("load MV definitions for drop scope check failed: {e}"))?;

    let mut edges: Vec<(MvDependencyObjectRef, Vec<MvDependencyObjectRef>)> =
        Vec::with_capacity(definitions.len());
    for def in &definitions {
        let mv_target = stored_definition_dependency_ref_from_state(state, def)?;
        let upstreams = state
            .mv_repo
            .list_dependencies_by_downstream(read.as_ref(), def.mv_id)
            .map_err(|e| format!("load MV dependencies for drop scope check failed: {e}"))?
            .into_iter()
            .map(|dep| dep.upstream)
            .collect::<Vec<_>>();
        edges.push((mv_target, upstreams));
    }

    validate_no_external_dependents_for_scope(scope_catalog, scope_namespace, &edges)
}

pub(crate) fn build_upstream_refresh_steps(
    state: &Arc<StandaloneState>,
    requested: &MvDependencyObjectRef,
) -> Result<Vec<MvRefreshDependencyStep>, String> {
    let Some(provider) = state.metadata_provider.as_ref() else {
        return Ok(vec![refresh_step_for_dependency_object(requested)?]);
    };
    let read = provider
        .begin_read()
        .map_err(|e| format!("open MV dependency refresh graph read failed: {e}"))?;
    let definitions = state
        .mv_repo
        .list_definitions(read.as_ref())
        .map_err(|e| format!("load MV definitions for refresh graph failed: {e}"))?;

    let mut edges = Vec::new();
    for definition in definitions {
        let target = stored_definition_dependency_ref_from_state(state, &definition)?;
        let upstream_mvs = state
            .mv_repo
            .list_dependencies_by_downstream(read.as_ref(), definition.mv_id)
            .map_err(|e| format!("load MV dependencies for refresh graph failed: {e}"))?
            .into_iter()
            .filter(|dep| dep.upstream.object_type == MvDependencyObjectType::MaterializedView)
            .map(|dep| dep.upstream)
            .collect::<Vec<_>>();
        edges.push((target, upstream_mvs));
    }

    topological_upstream_order_for_edges(requested, &edges)?
        .iter()
        .map(refresh_step_for_dependency_object)
        .collect()
}

pub(crate) fn validate_no_create_cycle(
    state: &Arc<StandaloneState>,
    new_target: &MvDependencyObjectRef,
    new_dependencies: &[CreateMvDependencyRequest],
) -> Result<(), String> {
    let Some(provider) = state.metadata_provider.as_ref() else {
        return Ok(());
    };
    let read = provider
        .begin_read()
        .map_err(|e| format!("open MV dependency graph read failed: {e}"))?;
    let definitions = state
        .mv_repo
        .list_definitions(read.as_ref())
        .map_err(|e| format!("load MV definitions for dependency cycle check failed: {e}"))?;
    let mut edges = Vec::new();
    for definition in definitions {
        let target = stored_definition_dependency_ref_from_state(state, &definition)?;
        let dependencies = state
            .mv_repo
            .list_dependencies_by_downstream(read.as_ref(), definition.mv_id)
            .map_err(|e| format!("load MV dependencies for cycle check failed: {e}"))?
            .into_iter()
            .filter(|dep| dep.upstream.object_type == MvDependencyObjectType::MaterializedView)
            .map(|dep| dep.upstream)
            .collect::<Vec<_>>();
        edges.push((target, dependencies));
    }
    let new_upstreams = new_dependencies
        .iter()
        .filter(|dep| dep.upstream.object_type == MvDependencyObjectType::MaterializedView)
        .map(|dep| dep.upstream.clone())
        .collect::<Vec<_>>();
    validate_no_cycle_for_edges(new_target, &new_upstreams, &edges)
}

fn stored_definition_dependency_ref_from_state(
    state: &Arc<StandaloneState>,
    definition: &StoredMvDefinition,
) -> Result<MvDependencyObjectRef, String> {
    if definition.storage_engine.eq_ignore_ascii_case("iceberg") {
        return stored_definition_dependency_ref(definition, None);
    }
    let starrocks = state
        .starrocks_table
        .read()
        .expect("standalone StarRocks table read lock");
    let table = starrocks
        .snapshot
        .tables
        .iter()
        .find(|table| table.table_id == definition.mv_id)
        .ok_or_else(|| {
            format!(
                "StarRocks table MV definition {} is missing runtime table metadata",
                definition.mv_id
            )
        })?;
    let database = starrocks
        .snapshot
        .databases
        .iter()
        .find(|database| database.db_id == table.db_id)
        .ok_or_else(|| {
            format!(
                "StarRocks table MV definition {} is missing runtime database metadata",
                definition.mv_id
            )
        })?;
    stored_definition_dependency_ref(definition, Some((&database.name, &table.name)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mv::dependency::model::{
        iceberg_mv_dependency_ref, iceberg_table_object_ref, starrocks_table_object_ref,
    };

    fn stored_mv_definition(
        storage_engine: &str,
        target_catalog: Option<&str>,
        target_namespace: Option<&str>,
        target_table: Option<&str>,
    ) -> StoredMvDefinition {
        StoredMvDefinition {
            mv_id: 1,
            select_sql: "select 1".to_string(),
            base_table_refs: Vec::new(),
            primary_key_columns: Vec::new(),
            storage_engine: storage_engine.to_string(),
            target_catalog: target_catalog.map(str::to_string),
            target_namespace: target_namespace.map(str::to_string),
            target_table: target_table.map(str::to_string),
            schema_contract: None,
            partition_spec: None,
            partition_state_complete: false,
            last_refresh_ms: None,
            last_refresh_rows: None,
            last_refresh_snapshots: std::collections::BTreeMap::new(),
            last_refresh_table_uuids: std::collections::BTreeMap::new(),
            last_refreshed_iceberg_snapshot_id: None,
            refresh_in_progress: false,
            active_refresh_id: None,
            refresh_target_snapshots: std::collections::BTreeMap::new(),
            refresh_policy: StoredMvRefreshPolicy::Manual,
            refresh_paused: false,
            refresh_interval_ms: None,
            max_staleness_ms: None,
            last_scheduler_error: None,
            next_refresh_after_ms: None,
            created_at_ms: 0,
        }
    }

    #[test]
    fn iceberg_mv_target_projection_tolerates_legacy_definitions() {
        let definitions = [
            stored_mv_definition(
                "starrocks",
                Some("Catalog"),
                Some("Namespace"),
                Some("Table"),
            ),
            stored_mv_definition("iceberg", None, Some("Namespace"), Some("Table")),
            stored_mv_definition("iceberg", Some("Catalog"), None, Some("Table")),
            stored_mv_definition("iceberg", Some("Catalog"), Some("Namespace"), None),
            stored_mv_definition("Iceberg", Some("Catalog"), Some("Namespace"), Some("Table")),
        ];

        let projected = definitions
            .iter()
            .filter_map(|definition| {
                iceberg_mv_target_in_scope(definition, "catalog", Some("namespace"))
            })
            .collect::<Vec<_>>();

        assert_eq!(projected, vec!["Catalog.Namespace.Table"]);
        assert!(!projected[0].contains("mv:"));
    }

    #[test]
    fn iceberg_mv_targets_scope_rejects_namespace_scope() {
        let definitions = vec![stored_mv_definition(
            "iceberg",
            Some("ice"),
            Some("analytics"),
            Some("mv_orders"),
        )];

        let err = validate_no_iceberg_mv_targets_in_scope("ice", Some("analytics"), &definitions)
            .expect_err("namespace drop containing an iceberg MV must be rejected");
        assert!(err.contains("cannot drop `ice.analytics`"), "err: {err}");
        assert!(err.contains("ice.analytics.mv_orders"), "err: {err}");
        assert!(err.contains("DROP MATERIALIZED VIEW"), "err: {err}");
    }

    #[test]
    fn iceberg_mv_targets_scope_rejects_catalog_scope() {
        let definitions = vec![stored_mv_definition(
            "iceberg",
            Some("ice"),
            Some("analytics"),
            Some("mv_orders"),
        )];

        let err = validate_no_iceberg_mv_targets_in_scope("ice", None, &definitions)
            .expect_err("catalog drop containing an iceberg MV must be rejected");
        assert!(err.contains("cannot drop `ice`"), "err: {err}");
        assert!(err.contains("ice.analytics.mv_orders"), "err: {err}");
    }

    #[test]
    fn iceberg_mv_targets_scope_ignores_non_iceberg_and_outside_scope() {
        let definitions = vec![
            stored_mv_definition(
                "starrocks",
                Some("ice"),
                Some("analytics"),
                Some("mv_starrocks"),
            ),
            stored_mv_definition("iceberg", Some("ice"), Some("other"), Some("mv_other")),
            stored_mv_definition(
                "iceberg",
                Some("other_catalog"),
                Some("analytics"),
                Some("mv_other_catalog"),
            ),
        ];

        validate_no_iceberg_mv_targets_in_scope("ice", Some("analytics"), &definitions)
            .expect("only in-scope iceberg MV targets should block the drop");
    }

    #[test]
    fn iceberg_mv_targets_scope_case_insensitive_matching() {
        let definitions = vec![stored_mv_definition(
            "Iceberg",
            Some("ICE"),
            Some("Analytics"),
            Some("mv_orders"),
        )];

        let err = validate_no_iceberg_mv_targets_in_scope("ice", Some("analytics"), &definitions)
            .expect_err("case-insensitive scope match must reject the drop");
        assert!(err.contains("ICE.Analytics.mv_orders"), "err: {err}");
    }

    #[test]
    fn external_dependents_scope_passes_when_scope_is_self_contained() {
        // Downstream MV is *inside* the scope (cat1.db1), so dropping the
        // scope also drops the downstream — no orphan risk.
        let mv_target = iceberg_mv_dependency_ref("cat1", "db1", "mv_inside");
        let upstream = iceberg_table_object_ref("cat1", "db1", "orders");
        let edges = vec![(mv_target, vec![upstream])];

        validate_no_external_dependents_for_scope("cat1", Some("db1"), &edges)
            .expect("scope-internal MV must not block the drop");
    }

    #[test]
    fn external_dependents_scope_rejects_external_dependent() {
        // Downstream MV lives outside the scope but depends on a table inside
        // it — dropping the scope would orphan the MV.
        let mv_target = iceberg_mv_dependency_ref("cat2", "db2", "mv_outside");
        let upstream = iceberg_table_object_ref("cat1", "db1", "orders");
        let edges = vec![(mv_target, vec![upstream])];

        let err = validate_no_external_dependents_for_scope("cat1", Some("db1"), &edges)
            .expect_err("orphaning MV must be rejected");
        assert!(
            err.contains("cannot drop `cat1.db1`"),
            "err missing scope label: {err}"
        );
        assert!(
            err.contains("mv:cat2.db2.mv_outside depends on cat1.db1.orders"),
            "err missing dependent detail: {err}"
        );
    }

    #[test]
    fn external_dependents_scope_at_catalog_granularity() {
        // DROP CATALOG cat1 — same risk, but the scope spans every namespace
        // under cat1. An MV in cat2.* depending on anything under cat1.*
        // must block the drop.
        let mv_target = iceberg_mv_dependency_ref("cat2", "db2", "mv_outside");
        let upstream_a = iceberg_table_object_ref("cat1", "ns1", "events");
        let upstream_b = iceberg_table_object_ref("cat1", "ns2", "orders");
        let edges = vec![(mv_target, vec![upstream_a.clone(), upstream_b.clone()])];

        let err = validate_no_external_dependents_for_scope("cat1", None, &edges)
            .expect_err("catalog-wide drop must reject the orphan");
        assert!(err.contains("cannot drop `cat1`"), "err: {err}");

        // Reverse: dropping cat2 should be fine — cat2.mv depends only on
        // cat1.* upstreams; nothing inside cat2 has external dependents.
        validate_no_external_dependents_for_scope("cat2", None, &edges)
            .expect("dropping the catalog that contains only an MV is allowed");
    }

    #[test]
    fn external_dependents_scope_ignores_non_iceberg_upstreams() {
        // StarRocks table upstreams are never in an Iceberg scope, even if the
        // catalog/namespace strings happen to match.
        let mv_target = iceberg_mv_dependency_ref("cat2", "db2", "mv_outside");
        let upstream = starrocks_table_object_ref("cat1", "orders");
        let edges = vec![(mv_target, vec![upstream])];

        validate_no_external_dependents_for_scope("cat1", Some("orders"), &edges)
            .expect("non-iceberg upstreams must not block iceberg-scope drops");
    }

    #[test]
    fn external_dependents_scope_case_insensitive_matching() {
        // Catalog/namespace identifiers are normalized to lowercase by the
        // resolver; ensure the scope check also works when the caller passes
        // mixed-case values.
        let mv_target = iceberg_mv_dependency_ref("cat2", "db2", "mv_outside");
        let upstream = iceberg_table_object_ref("cat1", "db1", "orders");
        let edges = vec![(mv_target, vec![upstream])];

        let err = validate_no_external_dependents_for_scope("CAT1", Some("DB1"), &edges)
            .expect_err("case-insensitive scope match must still reject orphan");
        assert!(err.contains("cannot drop `CAT1.DB1`"), "err: {err}");
    }
}
