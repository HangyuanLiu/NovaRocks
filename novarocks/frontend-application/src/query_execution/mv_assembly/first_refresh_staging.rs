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

//! Production preparation and binding for frontend-owned MV refresh writes.

use std::sync::Arc;

use novarocks_spi::connector::{ConnectorControlPlanningLease, ConnectorWriteLease};

use crate::catalog_application::query_bindings::QueryTableBindingStore;
use crate::catalog_application::query_catalog::catalog_service_snapshot;
use crate::mv::domain::iceberg_refresh::IcebergMvCorePorts;
use crate::query_execution::kernels::QueryPreparationKernel;
use crate::query_execution::mv_assembly::refresh_artifact::{
    MvFirstRefreshExecutionArtifact, MvFirstRefreshLogicalContext, PreparedMvFirstRefreshWrite,
};
use crate::query_execution::mv_native_write::{
    PreparedMvNativeWriteAssembly, prepare_completed_mv_write,
};
use crate::query_execution::planning::write_sink::{
    admit_session_connector_write_target, dml_write_plan_input_for_admitted_target,
};
use crate::query_execution::write_session::ConnectorWriteSession;
use novarocks_query_application::admitted_query_context::QueryExecutionContext;
use novarocks_sql::compiler::SqlMvRelationOccurrenceId;
use novarocks_sql::planning::mv::first_refresh::{
    SqlMvFirstRefreshAnalyzeContext, SqlMvJoinFirstRefreshAnalyzeContext, SqlMvSnapshotPin,
    SqlMvSnapshotPinOccurrence, analyze_join_first_refresh_connector_write,
    analyze_mv_first_refresh_connector_write, begin_final_join_first_refresh_connector_write_plan,
    begin_final_mv_first_refresh_connector_write_plan,
};

pub(crate) fn frozen_logical_context_from_rewrite(
    rewrite: &crate::mv::domain::rewrite::context::IcebergMvRewriteContext,
    affected_partitions: crate::mv::domain::model::AffectedTargetPartitions,
    frozen_base_overlays: Option<
        Vec<crate::catalog_application::query_materializer::QueryLocalTableOverlay>,
    >,
) -> Result<MvFirstRefreshLogicalContext, String> {
    let pin = ordered_current_sources(rewrite)?;
    // Validate the exact application facts against the SQL-owned occurrence
    // contract before the logical artifact can outlive this preparation step.
    // The richer source revisions remain in the application artifact because
    // activation must reconstruct the canonical context without decoding IDs.
    let _sql_pin = sql_snapshot_pin(&rewrite.mv_definition, &pin)?;
    Ok(MvFirstRefreshLogicalContext {
        mv_definition: (*rewrite.mv_definition).clone(),
        canonical_select_query: (*rewrite.canonical_select_query).clone(),
        base_refs: rewrite
            .base_refs
            .iter()
            .map(|base| base.table.clone())
            .collect(),
        pin,
        previous: ordered_previous_sources(rewrite)?,
        analysis: rewrite.analysis_facts().clone(),
        target_table_uuid: rewrite.target_table_uuid.clone(),
        affected_partitions,
        frozen_base_overlays,
    })
}

fn ordered_current_sources(
    rewrite: &crate::mv::domain::rewrite::context::IcebergMvRewriteContext,
) -> Result<Vec<crate::mv::domain::rewrite::context::MvRewriteSourceSnapshot>, String> {
    let occurrences = &rewrite
        .mv_definition
        .facts
        .definition()
        .relation_occurrences;
    if occurrences.len() != rewrite.pin.len() {
        return Err("MV first-refresh source pins do not cover every D occurrence".to_string());
    }
    occurrences
        .iter()
        .map(|occurrence| {
            rewrite
                .pin
                .get(&SqlMvRelationOccurrenceId::new(occurrence.occurrence_id))
                .cloned()
                .ok_or_else(|| {
                    format!(
                        "MV first-refresh source pin is missing D occurrence {}",
                        occurrence.occurrence_id
                    )
                })
        })
        .collect()
}

fn ordered_previous_sources(
    rewrite: &crate::mv::domain::rewrite::context::IcebergMvRewriteContext,
) -> Result<Vec<crate::mv::domain::rewrite::context::MvRewriteSourceSnapshot>, String> {
    use novarocks_mv_application::persistence::projection::MvPublicationState;

    match rewrite.mv_definition.facts.publication() {
        MvPublicationState::NeverPublished if rewrite.previous.is_empty() => Ok(Vec::new()),
        MvPublicationState::NeverPublished => {
            Err("never-published MV first-refresh artifact has source history".to_string())
        }
        MvPublicationState::Published(published) => published
            .document()
            .inputs
            .iter()
            .map(|input| {
                rewrite
                    .previous
                    .iter()
                    .find(|source| source.occurrence_id.get() == input.relation_occurrence_id)
                    .cloned()
                    .ok_or_else(|| {
                        format!(
                            "MV first-refresh history is missing D occurrence {}",
                            input.relation_occurrence_id
                        )
                    })
            })
            .collect(),
    }
}

fn sql_snapshot_pin(
    projection: &novarocks_mv_application::persistence::projection::StoredMvProjection,
    sources: &[crate::mv::domain::rewrite::context::MvRewriteSourceSnapshot],
) -> Result<SqlMvSnapshotPin, String> {
    let occurrences = &projection.facts.definition().relation_occurrences;
    if occurrences.len() != sources.len() {
        return Err("MV first-refresh SQL pin does not cover every D occurrence".to_string());
    }
    SqlMvSnapshotPin::try_from_occurrences(
        occurrences
            .iter()
            .zip(sources)
            .map(|(occurrence, source)| {
                let occurrence_id = SqlMvRelationOccurrenceId::new(occurrence.occurrence_id);
                if source.occurrence_id != occurrence_id {
                    return Err(format!(
                        "MV first-refresh SQL pin order differs at D occurrence {}",
                        occurrence.occurrence_id
                    ));
                }
                SqlMvSnapshotPinOccurrence::try_new(
                    occurrence_id,
                    novarocks_types::naming::TableIdentity {
                        catalog: occurrence.catalog_at_binding.clone(),
                        namespace: occurrence.namespace_at_binding.clone(),
                        table: occurrence.relation_at_binding.clone(),
                    },
                    source.snapshot_id,
                    source.table_object_id.clone(),
                )
            })
            .collect::<Result<Vec<_>, String>>()?,
    )
}

/// Reserve the one primary first-refresh write cohort from facts frozen in an
/// MV refresh context. The staging branch must already exist and `exact_lease`
/// must have been derived from the retained target control binding. This is
/// the first point that mutates the provider write-service registry; SQL
/// artifact preparation remains side-effect free.
/// Bind an SQL-shaped first-refresh artifact only after the frontend has
/// retained its exact write lease and admitted an immutable query execution.
/// The result retains the exact native-assembly input for the Frontend; it
/// deliberately does not encode or submit a query, commit a provider mutation,
/// or expose row payloads.
pub(crate) fn bind_prepared_mv_first_refresh_staging(
    query_kernel: &QueryPreparationKernel,
    ports: &IcebergMvCorePorts,
    prepared: PreparedMvFirstRefreshWrite,
    planning_lease: &ConnectorControlPlanningLease,
    exact_lease: &ConnectorWriteLease,
    execution: &QueryExecutionContext,
    connector_context: novarocks_spi::connector::ConnectorRequestContext,
) -> Result<PreparedMvNativeWriteAssembly, String> {
    // The session is opened before the plan is compiled because the plan's
    // writer node carries the recipe it seals: a plan and the session that
    // sealed it must not be separable.
    let write_session = super::iceberg_activation::begin_first_refresh_connector_write_session(
        &prepared,
        connector_context.clone(),
        exact_lease,
        planning_lease,
        query_kernel.typed_connector_control(),
    )?;
    match bind_first_refresh_write_dataflow(
        query_kernel,
        ports,
        prepared,
        planning_lease,
        execution,
        &connector_context,
        &write_session,
    ) {
        Ok(assembly) => Ok(assembly),
        Err(error) => {
            super::iceberg_activation::release_mv_write_session_without_commit(
                &write_session,
                &connector_context,
            );
            Err(error)
        }
    }
}

/// The one logical target a first-refresh publication seals.
///
/// A publication that republishes rows wholesale has exactly one thing to do
/// with every row it is given, so it seals a single unrouted data branch. The
/// plan is compiled against that branch's ordinal, so a session that sealed a
/// different number of them is refused here rather than having its extra
/// branches written by nobody.
fn sole_publication_write_target(
    write_session: &ConnectorWriteSession,
) -> Result<&novarocks_spi::connector::write_stack::ConnectorWriteTargetPlan, String> {
    match write_session.targets() {
        [write_target] => Ok(write_target),
        targets => Err(format!(
            "MV first-refresh publication requires a write session with exactly one target, but the session sealed {}",
            targets.len()
        )),
    }
}

fn bind_first_refresh_write_dataflow(
    query_kernel: &QueryPreparationKernel,
    ports: &IcebergMvCorePorts,
    prepared: PreparedMvFirstRefreshWrite,
    planning_lease: &ConnectorControlPlanningLease,
    execution: &QueryExecutionContext,
    connector_context: &novarocks_spi::connector::ConnectorRequestContext,
    write_session: &Arc<ConnectorWriteSession>,
) -> Result<PreparedMvNativeWriteAssembly, String> {
    let expected_target_snapshot_id = prepared.expected_target_snapshot_id();
    let target_catalog = prepared.target_catalog().to_string();
    let target_namespace = prepared.target_namespace().to_string();
    let target_name = prepared.target_name().to_string();
    let current_catalog = prepared.current_catalog().map(str::to_string);
    let current_database = prepared.current_database().to_string();
    let root_hash_column = prepared.root_hash_column().to_string();
    let write_target = sole_publication_write_target(write_session)?;
    let write_target_ordinal = write_target.ordinal();
    // The recipes are sealed once, here, and travel with the plan they were
    // sealed for, so an encode can never pair one round's plan with another's
    // session.
    let sealed_write_targets = write_session
        .seal_write_targets()
        .map_err(|error| format!("seal MV first-refresh write target: {error}"))?;
    match prepared.into_execution_artifact() {
        MvFirstRefreshExecutionArtifact::Sql(physical_sql) => {
            let bindings = Arc::new(QueryTableBindingStore::try_new()?);
            let target_binding = admit_session_connector_write_target(
                bindings.as_ref(),
                novarocks_sql::planning::query_execution::FrozenConnectorScanIdentity::try_new(
                    target_catalog.clone(),
                    target_namespace.clone(),
                    target_name.clone(),
                )?,
                write_target,
                planning_lease.clone(),
            )?;
            let sink = dml_write_plan_input_for_admitted_target(
                bindings.as_ref(),
                target_binding,
                novarocks_sql::planning::dml::DmlWriteSinkMode::Data,
                novarocks_sql::plan_read::ConnectorWriteInputBinding::RootOutputByOrdinal,
            )?;
            let field_names = std::collections::BTreeMap::from([(
                write_target_ordinal,
                sink.accepted_field_names().into_iter().collect(),
            )]);
            let catalog_service_snapshot = catalog_service_snapshot(query_kernel);
            let materializer =
                crate::catalog_application::query_materializer::CatalogServiceMaterializer::new(
                    None,
                    &catalog_service_snapshot,
                    Arc::clone(&bindings),
                    crate::catalog_application::query_materializer::iceberg_table_binding_loader(
                        query_kernel.connector_control().as_ref(),
                        connector_context.clone(),
                    ),
                );
            let catalog = novarocks_sql::compiler::SqlPlannerTableSnapshot::new(&materializer);
            let compile_control = novarocks_sql::compiler::SqlCompileControl::new(
                execution.deadline(),
                crate::query_execution::planning::sql_cancellation_observation(
                    execution.cancellation().clone(),
                ),
            );
            let analyzed = analyze_mv_first_refresh_connector_write(
                physical_sql,
                SqlMvFirstRefreshAnalyzeContext {
                    current_catalog: current_catalog.clone(),
                    current_database: current_database.clone(),
                    optimizer_settings: execution.optimizer_settings().clone(),
                    environment: novarocks_sql::compiler::SqlPlanningEnvironment::Distributed,
                    catalog: &catalog,
                    functions: query_kernel.function_catalog().as_ref(),
                    constant_evaluator: crate::query_execution::constant_eval::constant_evaluator(),
                    control: compile_control.clone(),
                    sink,
                },
            )?;
            let statistics = crate::query_execution::planning::statistics::QueryStatisticsContext::from_statistics_resolver_with_bindings(
                query_kernel,
                Arc::clone(&bindings),
                connector_context,
            )?;
            let (completion, needs) = begin_final_mv_first_refresh_connector_write_plan(
                analyzed,
                &statistics,
                compile_control,
                write_session
                    .statistics_requirements(write_target_ordinal)
                    .map_err(|error| error.to_string())?,
                write_target_ordinal,
            )?;
            prepare_completed_mv_write(
                query_kernel,
                execution,
                bindings.as_ref(),
                connector_context,
                Arc::clone(write_session),
                sealed_write_targets,
                needs,
                field_names,
                |version, dop, reads, targets| completion.finish(version, dop, reads, targets),
            )
        }
        MvFirstRefreshExecutionArtifact::Logical(logical) => {
            let facts = logical.into_context();
            let frozen_base_overlays = facts.frozen_base_overlays.clone().ok_or_else(|| {
                "MV first-refresh logical artifact is missing its admitted base bindings"
                    .to_string()
            })?;
            let refresh_rewrite = rebuild_frozen_mv_rewrite_context(
                ports,
                expected_target_snapshot_id,
                &target_catalog,
                &target_namespace,
                &target_name,
                &facts,
                planning_lease,
                connector_context,
            )?;
            let bindings = Arc::new(QueryTableBindingStore::try_new()?);
            let target_binding =
                crate::query_execution::mv_assembly::query_local_bindings::bind_imv_target_query_table_in_store_from_rewrite(
                    &refresh_rewrite,
                    &bindings,
                    planning_lease,
                    connector_context,
                )?;
            let write_target_binding = admit_session_connector_write_target(
                bindings.as_ref(),
                novarocks_sql::planning::query_execution::FrozenConnectorScanIdentity::try_new(
                    target_catalog.clone(),
                    target_namespace.clone(),
                    target_name.clone(),
                )?,
                write_target,
                planning_lease.clone(),
            )?;
            let sink = dml_write_plan_input_for_admitted_target(
                bindings.as_ref(),
                write_target_binding,
                novarocks_sql::planning::dml::DmlWriteSinkMode::Data,
                novarocks_sql::plan_read::ConnectorWriteInputBinding::RootOutputByOrdinal,
            )?;
            let field_names = std::collections::BTreeMap::from([(
                write_target_ordinal,
                sink.accepted_field_names().into_iter().collect(),
            )]);
            let catalog_service_snapshot = catalog_service_snapshot(query_kernel);
            let materializer = crate::catalog_application::query_materializer::CatalogServiceMaterializer::new_with_query_local_overlays(
                None,
                &catalog_service_snapshot,
                Arc::clone(&bindings),
                crate::catalog_application::query_materializer::iceberg_table_binding_loader(
                    query_kernel.connector_control().as_ref(),
                    connector_context.clone(),
                ),
                frozen_base_overlays,
            );
            let catalog = novarocks_sql::compiler::SqlPlannerTableSnapshot::new(&materializer);
            let compile_control = novarocks_sql::compiler::SqlCompileControl::new(
                execution.deadline(),
                crate::query_execution::planning::sql_cancellation_observation(
                    execution.cancellation().clone(),
                ),
            );
            let analyzed =
                analyze_join_first_refresh_connector_write(SqlMvJoinFirstRefreshAnalyzeContext {
                    canonical_query: Box::new((*refresh_rewrite.canonical_select_query).clone()),
                    rewrite_snapshot: refresh_rewrite.to_sql_rewrite_snapshot(target_binding)?,
                    expected_root_hash_column: root_hash_column,
                    current_catalog: current_catalog.clone(),
                    current_database: current_database.clone(),
                    optimizer_settings: execution.optimizer_settings().clone(),
                    environment: novarocks_sql::compiler::SqlPlanningEnvironment::Distributed,
                    catalog: &catalog,
                    functions: query_kernel.function_catalog().as_ref(),
                    constant_evaluator: crate::query_execution::constant_eval::constant_evaluator(),
                    control: compile_control.clone(),
                    sink,
                })?;
            let statistics = crate::query_execution::planning::statistics::QueryStatisticsContext::from_statistics_resolver_with_bindings(
                query_kernel,
                materializer.query_table_bindings(),
                connector_context,
            )?;
            let (completion, needs) = begin_final_join_first_refresh_connector_write_plan(
                analyzed,
                &statistics,
                compile_control,
                write_session
                    .statistics_requirements(write_target_ordinal)
                    .map_err(|error| error.to_string())?,
                write_target_ordinal,
            )?;
            prepare_completed_mv_write(
                query_kernel,
                execution,
                bindings.as_ref(),
                connector_context,
                Arc::clone(write_session),
                sealed_write_targets,
                needs,
                field_names,
                |version, dop, reads, targets| completion.finish(version, dop, reads, targets),
            )
        }
    }
}

#[expect(
    clippy::too_many_arguments,
    reason = "Rebuilding a frozen MV rewrite context requires each independently pinned catalog and target fact."
)]
pub(crate) fn rebuild_frozen_mv_rewrite_context(
    ports: &IcebergMvCorePorts,
    expected_target_snapshot_id: Option<i64>,
    target_catalog: &str,
    target_namespace: &str,
    target_name: &str,
    facts: &MvFirstRefreshLogicalContext,
    planning_lease: &ConnectorControlPlanningLease,
    connector_context: &novarocks_spi::connector::ConnectorRequestContext,
) -> Result<Arc<crate::mv::domain::rewrite::context::IcebergMvRewriteContext>, String> {
    let persisted_target = facts.mv_definition.facts.target();
    let target_identity = novarocks_types::naming::TableIdentity {
        catalog: persisted_target
            .catalog()
            .ok_or_else(|| {
                "MV first-refresh logical artifact target has no connector catalog".to_string()
            })?
            .to_string(),
        namespace: persisted_target.namespace().to_string(),
        table: persisted_target.name().to_string(),
    };
    if target_identity.catalog != target_catalog
        || target_identity.namespace != target_namespace
        || target_identity.table != target_name
    {
        return Err(
            "MV refresh logical artifact target does not match its frozen write request"
                .to_string(),
        );
    }
    validate_frozen_join_base_facts(facts)?;
    let target_binding =
        crate::mv::domain::refresh::target_binding::load_mv_target_binding_with_lease_and_ports(
            ports.storage_observation(),
            &target_identity,
            planning_lease.clone(),
            connector_context,
        )?;
    if target_binding.table_uuid() != facts.target_table_uuid {
        return Err(
            "MV refresh logical artifact target UUID drifted after preparation".to_string(),
        );
    }
    if target_binding.current_snapshot_id() != expected_target_snapshot_id {
        return Err(
            "MV refresh logical artifact target snapshot drifted after preparation".to_string(),
        );
    }
    let target_schema = crate::mv::domain::storage_observation::observe_schema_validation(
        ports.storage_observation(),
        target_binding.lease(),
        target_binding.metadata(),
        connector_context.clone(),
    )
    .map_err(|error| format!("observe exact MV target schema for activation: {error}"))?;
    if target_schema.table() != target_binding.identity() {
        return Err(
            "MV first-refresh target schema and retained target binding differ".to_string(),
        );
    }
    crate::mv::domain::refresh::rewrite_context::build_neutral_refresh_rewrite_context(
        Arc::new(facts.mv_definition.clone()),
        facts.pin.clone(),
        facts.previous.clone(),
        facts.target_table_uuid.clone(),
        target_binding.physical_write_schema()?,
        target_schema.exact_schema().clone(),
        facts.analysis.clone(),
    )
}

fn validate_frozen_join_base_facts(facts: &MvFirstRefreshLogicalContext) -> Result<(), String> {
    if facts.base_refs.is_empty() || facts.pin.len() != facts.base_refs.len() {
        return Err(
            "MV first-refresh logical artifact has incomplete base snapshot pins".to_string(),
        );
    }
    let occurrences = &facts.mv_definition.facts.definition().relation_occurrences;
    if occurrences.len() != facts.pin.len()
        || occurrences
            .iter()
            .zip(&facts.pin)
            .any(|(occurrence, source)| {
                source.occurrence_id != SqlMvRelationOccurrenceId::new(occurrence.occurrence_id)
            })
    {
        return Err(
            "MV first-refresh logical artifact does not preserve D occurrence order".to_string(),
        );
    }
    // Production logical first-refresh artifacts retain the materializations
    // admitted during preparation.  Those overlays carry the exact lease,
    // table identity, and pinned input set that activation must use; asking
    // the catalog for the current base here would silently reintroduce a
    // latest-generation acquire.
    facts
        .frozen_base_overlays
        .as_ref()
        .map(|_| ())
        .ok_or_else(|| {
            "MV logical artifact is missing exact-generation frozen base overlays".to_string()
        })
}

#[allow(
    dead_code,
    reason = "Retained for staged MV execution assembly and recovery wiring."
)]
fn parse_query_from_sql(sql: &str) -> Result<novarocks_parser::ast::Query, String> {
    let statements = novarocks_parser::parse(sql).map_err(|error| error.to_string())?;
    let [novarocks_parser::ast::Statement::Query(query)] = statements.as_slice() else {
        return Err("MV first-refresh physical artifact is not a SELECT query".to_string());
    };
    Ok(query.clone())
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use novarocks_mv_application::persistence::{
        projection::StoredMvProjection, test_support::ProjectionFixture,
    };
    use novarocks_mv_application::product::MvTarget;
    use novarocks_spi::connector::{
        ConnectorExactSemanticRevision, ConnectorProviderId, ConnectorTableObjectId,
    };

    fn projection() -> StoredMvProjection {
        StoredMvProjection {
            mv_id: 17,
            facts: ProjectionFixture::new(MvTarget::from_parts(Some("ice"), "sales", "mv"), None)
                .build()
                .unwrap(),
        }
    }

    fn source(occurrence_id: u32) -> crate::mv::domain::rewrite::context::MvRewriteSourceSnapshot {
        let object_id =
            ConnectorTableObjectId::try_new(Bytes::from_static(b"source-object")).unwrap();
        crate::mv::domain::rewrite::context::MvRewriteSourceSnapshot {
            occurrence_id: SqlMvRelationOccurrenceId::new(occurrence_id),
            snapshot_id: 12,
            table_object_id: object_id.clone(),
            semantic_revision: ConnectorExactSemanticRevision::try_from_table_object_and_snapshot(
                ConnectorProviderId::parse("iceberg").unwrap(),
                &object_id,
                Some(12),
            )
            .unwrap(),
        }
    }

    #[test]
    fn sql_pin_preserves_sparse_definition_occurrence_order() {
        let projection = projection();
        let pin = sql_snapshot_pin(&projection, &[source(7), source(8)]).unwrap();
        assert_eq!(pin.occurrences()[0].occurrence_id().get(), 7);
        assert_eq!(pin.occurrences()[1].occurrence_id().get(), 8);
        assert!(sql_snapshot_pin(&projection, &[source(8), source(7)]).is_err());
    }
}
