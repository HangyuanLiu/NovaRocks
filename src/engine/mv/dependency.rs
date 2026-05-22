use std::sync::Arc;

use crate::connector::starrocks::managed::model::IcebergTableRef;
use crate::connector::starrocks::managed::mv_ddl::ResolvedTableRef;
use crate::engine::StandaloneState;
use crate::meta::repository::mv::{
    CreateMvDependencyRequest, MvDependencyObjectRef, MvDependencyObjectType,
    MvDependencyStorageEngine, StoredMvDefinition,
};

pub(crate) struct ResolvedCreateMvDependencies {
    pub(crate) base_refs: Vec<IcebergTableRef>,
    pub(crate) dependencies: Vec<CreateMvDependencyRequest>,
}

pub(crate) fn iceberg_table_dependency_ref(base: &IcebergTableRef) -> MvDependencyObjectRef {
    MvDependencyObjectRef {
        catalog: Some(base.catalog.clone()),
        database_or_namespace: base.namespace.clone(),
        name: base.table.clone(),
        object_type: MvDependencyObjectType::Table,
        storage_engine: MvDependencyStorageEngine::Iceberg,
    }
}

pub(crate) fn iceberg_mv_dependency_ref(
    catalog: &str,
    namespace: &str,
    table: &str,
) -> MvDependencyObjectRef {
    MvDependencyObjectRef {
        catalog: Some(catalog.to_string()),
        database_or_namespace: namespace.to_string(),
        name: table.to_string(),
        object_type: MvDependencyObjectType::MaterializedView,
        storage_engine: MvDependencyStorageEngine::Iceberg,
    }
}

pub(crate) fn managed_mv_dependency_ref(database: &str, table: &str) -> MvDependencyObjectRef {
    MvDependencyObjectRef {
        catalog: None,
        database_or_namespace: database.to_string(),
        name: table.to_string(),
        object_type: MvDependencyObjectType::MaterializedView,
        storage_engine: MvDependencyStorageEngine::ManagedLake,
    }
}

pub(crate) fn stored_definition_dependency_ref(
    definition: &StoredMvDefinition,
    managed_name: Option<(&str, &str)>,
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
    let (database, table) = managed_name.ok_or_else(|| {
        "managed-lake MV definition requires database/table name for dependency ref".to_string()
    })?;
    Ok(managed_mv_dependency_ref(database, table))
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
                let base = IcebergTableRef {
                    catalog: catalog.clone(),
                    namespace: namespace.clone(),
                    table: table.clone(),
                };
                if !base_refs.contains(&base) {
                    base_refs.push(base.clone());
                }
                let upstream = if state
                    .mv_repo
                    .find_by_target(read.as_ref(), catalog, namespace, table)
                    .map_err(|e| format!("load MV target dependency failed: {e}"))?
                    .is_some()
                {
                    iceberg_mv_dependency_ref(catalog, namespace, table)
                } else {
                    iceberg_table_dependency_ref(&base)
                };
                dependencies.push(CreateMvDependencyRequest {
                    upstream,
                    created_at_ms,
                });
            }
            ResolvedTableRef::ManagedLake { database, table } => {
                let managed = state
                    .managed_lake
                    .read()
                    .expect("standalone managed lake read lock");
                let runtime = managed.table(database, table).map_err(|err| {
                    format!("resolve managed-lake MV dependency {database}.{table} failed: {err}")
                })?;
                if runtime.table.kind
                    != crate::connector::starrocks::managed::model::ManagedTableKind::MaterializedView
                {
                    return Err(format!(
                        "materialized view base tables must be Iceberg tables or materialized views; found managed lake table `{database}.{table}`"
                    ));
                }
                return Err(format!(
                    "managed-lake MV-on-MV dependency `{database}.{table}` is recognized but cannot be used as an incremental Iceberg base in this release"
                ));
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

pub(crate) fn validate_no_cycle_for_edges(
    new_target: &MvDependencyObjectRef,
    new_upstreams: &[MvDependencyObjectRef],
    existing_edges: &[(MvDependencyObjectRef, Vec<MvDependencyObjectRef>)],
) -> Result<(), String> {
    let mut graph: std::collections::BTreeMap<MvDependencyObjectRef, Vec<MvDependencyObjectRef>> =
        std::collections::BTreeMap::new();
    for (downstream, upstreams) in existing_edges {
        graph.insert(downstream.clone(), upstreams.clone());
    }
    graph.insert(new_target.clone(), new_upstreams.to_vec());

    fn visit(
        graph: &std::collections::BTreeMap<MvDependencyObjectRef, Vec<MvDependencyObjectRef>>,
        node: &MvDependencyObjectRef,
        target: &MvDependencyObjectRef,
        path: &mut Vec<MvDependencyObjectRef>,
    ) -> Option<Vec<MvDependencyObjectRef>> {
        if path.contains(node) {
            return None;
        }
        path.push(node.clone());
        for upstream in graph.get(node).cloned().unwrap_or_default() {
            if &upstream == target {
                let mut cycle = path.clone();
                cycle.push(upstream);
                return Some(cycle);
            }
            if upstream.object_type == MvDependencyObjectType::MaterializedView
                && let Some(cycle) = visit(graph, &upstream, target, path)
            {
                return Some(cycle);
            }
        }
        path.pop();
        None
    }

    if let Some(cycle) = visit(&graph, new_target, new_target, &mut Vec::new()) {
        let display = cycle
            .iter()
            .map(MvDependencyObjectRef::display_name)
            .collect::<Vec<_>>()
            .join(" -> ");
        return Err(format!("dependency cycle detected: {display}"));
    }
    Ok(())
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
    let managed = state
        .managed_lake
        .read()
        .expect("standalone managed lake read lock");
    let table = managed
        .snapshot
        .tables
        .iter()
        .find(|table| table.table_id == definition.mv_id)
        .ok_or_else(|| {
            format!(
                "managed-lake MV definition {} is missing runtime table metadata",
                definition.mv_id
            )
        })?;
    let database = managed
        .snapshot
        .databases
        .iter()
        .find(|database| database.db_id == table.db_id)
        .ok_or_else(|| {
            format!(
                "managed-lake MV definition {} is missing runtime database metadata",
                definition.mv_id
            )
        })?;
    stored_definition_dependency_ref(definition, Some((&database.name, &table.name)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dependency_ref_display_distinguishes_table_and_mv() {
        let table = iceberg_table_dependency_ref(&IcebergTableRef {
            catalog: "ice".to_string(),
            namespace: "sales".to_string(),
            table: "orders".to_string(),
        });
        let mv = iceberg_mv_dependency_ref("ice", "sales", "orders_mv");

        assert_eq!(table.display_name(), "ice.sales.orders");
        assert_eq!(mv.display_name(), "mv:ice.sales.orders_mv");
    }

    #[test]
    fn dependency_cycle_detector_rejects_new_back_edge() {
        let mv_a = iceberg_mv_dependency_ref("ice", "sales", "mv_a");
        let mv_b = iceberg_mv_dependency_ref("ice", "sales", "mv_b");
        let mv_c = iceberg_mv_dependency_ref("ice", "sales", "mv_c");
        let existing = vec![
            (mv_a.clone(), vec![mv_b.clone()]),
            (mv_b.clone(), vec![mv_c.clone()]),
        ];

        let err = validate_no_cycle_for_edges(&mv_c, &[mv_a.clone()], &existing)
            .expect_err("c -> a should form a cycle");
        assert_eq!(
            err,
            "dependency cycle detected: mv:ice.sales.mv_c -> mv:ice.sales.mv_a -> mv:ice.sales.mv_b -> mv:ice.sales.mv_c"
        );
    }

    #[test]
    fn dependency_cycle_detector_accepts_dag() {
        let mv_a = iceberg_mv_dependency_ref("ice", "sales", "mv_a");
        let mv_b = iceberg_mv_dependency_ref("ice", "sales", "mv_b");
        let mv_c = iceberg_mv_dependency_ref("ice", "sales", "mv_c");
        let existing = vec![(mv_b.clone(), vec![mv_a.clone()])];

        validate_no_cycle_for_edges(&mv_c, &[mv_b], &existing).expect("dag should be accepted");
        let _ = mv_a;
    }
}

