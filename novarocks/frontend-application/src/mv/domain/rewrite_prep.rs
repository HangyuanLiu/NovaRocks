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

//! Application-side freezing for optional materialized-view rewrite.
//!
//! Repository enumeration and connector metadata reads belong here. The SQL
//! compiler receives the resulting immutable definition index and owns all
//! candidate parse/analyze/statistics/selection work.

use std::{collections::HashSet, fmt, sync::Arc};

use crate::mv::domain::readiness::MvCandidateReader;
use crate::mv::domain::refresh::definition::parse_mv_select_query;
use novarocks_mv_application::persistence::{
    documents::{MvFrozenPublicationReadView, observe_frozen_publication_documents},
    exact_revision::restore_exact_query_revision,
    projection::StoredMvProjection,
    validation::PersistenceDecodeBudget,
};
use novarocks_spi::connector::MvStorageObservationPort;
use novarocks_sql::compiler::{
    MaterializedViewFact, MaterializedViewNeed, MvRewriteDefinitionIndex,
    SqlMvDefinitionResolutionContext, SqlMvRelationOccurrenceId, SqlMvRewriteBaseTableFacts,
    SqlMvRewriteDefinitionFacts, SqlMvRewritePublicationInput, SqlMvRewritePublicationRelation,
    SqlMvRewriteSelectionFacts, SqlMvRewriteSourceOccurrenceFacts,
};
use novarocks_types::naming::TableIdentity;

/// Freeze the optional rewrite facts for exactly one SQL completion need.
///
/// The repository can only enumerate a request-local inventory, so this
/// function filters it before any connector or storage observation. A
/// definition is relevant only when all of its base relations occur in the
/// need: SQL rejects a fact containing an unrequested base relation, and a
/// partial match would otherwise make that compiler contract ambiguous.
///
/// MV discovery and each candidate are optional accelerators. Their failures
/// therefore produce a missing or empty observed fact, never a query failure.
pub(crate) fn freeze_materialized_view_fact_with_ports(
    need: &MaterializedViewNeed,
    candidate_reader: &MvCandidateReader,
    connector_control: &dyn novarocks_spi::connector::ConnectorControlResolver,
    _storage_observation: &dyn MvStorageObservationPort,
) -> MaterializedViewFact {
    let definitions = match candidate_reader.list_candidate_definitions() {
        Ok(definitions) => definitions,
        Err(error) => {
            tracing::debug!(error = %error, "skip MV rewrite discovery because the inventory is unavailable");
            return MaterializedViewFact::missing(
                need,
                "materialized-view rewrite inventory is unavailable",
            )
            .expect("a static MV inventory diagnostic is non-empty");
        }
    };
    let requested_relations = need
        .referenced_relations()
        .iter()
        .map(novarocks_types::naming::TableIdentity::fqn)
        .collect::<HashSet<_>>();
    let relevant = definitions.into_iter().filter(|definition| {
        candidate_base_tables_are_requested(
            &definition_base_names(definition),
            &requested_relations,
        )
    });
    let budget = rewrite_document_budget();
    let report = novarocks_mv_application::candidate::inspect_candidates(
        relevant,
        |definition| definition.mv_id.to_string(),
        |definition| freeze_mv_rewrite_definition(connector_control, &budget, definition),
    );
    for diagnostic in report.diagnostics() {
        tracing::debug!(
            candidate = diagnostic.identity(),
            error = diagnostic.message(),
            "skip unavailable MV rewrite candidate"
        );
    }
    MaterializedViewFact::observed(need, report.into_accepted())
}

fn candidate_base_tables_are_requested(
    base_table_refs: &[String],
    requested_relations: &HashSet<String>,
) -> bool {
    !base_table_refs.is_empty()
        && base_table_refs
            .iter()
            .all(|base_table| requested_relations.contains(base_table))
}

/// Freeze rewrite candidates from the caller's leaf ports.  The frozen index
/// remains request-local.
pub(crate) fn freeze_mv_rewrite_definition_index_with_ports(
    candidate_reader: &MvCandidateReader,
    connector_control: &dyn novarocks_spi::connector::ConnectorControlResolver,
    _storage_observation: &dyn MvStorageObservationPort,
) -> Result<MvRewriteDefinitionIndex, String> {
    let definitions = optional_candidate_inventory(candidate_reader.list_candidate_definitions());
    let budget = rewrite_document_budget();
    let report = novarocks_mv_application::candidate::inspect_candidates(
        definitions,
        |definition| definition.mv_id.to_string(),
        |definition| freeze_mv_rewrite_definition(connector_control, &budget, definition),
    );
    for diagnostic in report.diagnostics() {
        tracing::debug!(
            candidate = diagnostic.identity(),
            error = diagnostic.message(),
            "skip unavailable MV rewrite candidate"
        );
    }
    MvRewriteDefinitionIndex::try_new(report.into_accepted())
}

/// An MV inventory is an optional rewrite input.  Losing it must only remove
/// rewrite candidates; it cannot prevent the required base-table query from
/// being planned.
fn optional_candidate_inventory<T, E>(inventory: Result<Vec<T>, E>) -> Vec<T>
where
    E: fmt::Display,
{
    match inventory {
        Ok(candidates) => candidates,
        Err(error) => {
            tracing::debug!(error = %error, "skip MV rewrite discovery because the inventory is unavailable");
            Vec::new()
        }
    }
}

fn definition_base_names(projection: &StoredMvProjection) -> Vec<String> {
    projection
        .facts
        .definition()
        .relation_occurrences
        .iter()
        .map(occurrence_table)
        .map(|table| table.fqn())
        .collect()
}

fn occurrence_table(
    occurrence: &novarocks_mv_application::persistence::codec::RelationOccurrence,
) -> TableIdentity {
    TableIdentity {
        catalog: occurrence.catalog_at_binding.clone(),
        namespace: occurrence.namespace_at_binding.clone(),
        table: occurrence.relation_at_binding.clone(),
    }
}

fn rewrite_document_budget() -> novarocks_spi::connector::ConnectorDocumentStorageBudget {
    novarocks_spi::connector::ConnectorDocumentStorageBudget::new(
        novarocks_spi::connector::ConnectorDocumentStorageLimits::spec_default(),
    )
}

fn freeze_mv_rewrite_definition(
    connector_control: &dyn novarocks_spi::connector::ConnectorControlResolver,
    budget: &novarocks_spi::connector::ConnectorDocumentStorageBudget,
    projection: StoredMvProjection,
) -> Result<SqlMvRewriteDefinitionFacts, String> {
    use novarocks_spi::connector::{
        ConnectorDocumentObservationRequest, ConnectorReadSelector, ConnectorTableIdentity,
        ConnectorTableObjectRebindRequest, ConnectorTableObjectSelector, ConnectorTableResolution,
    };
    let target = projection.facts.target();
    let target_catalog = target.catalog().ok_or("MV rewrite target has no catalog")?;
    let context = crate::connector::connector_request_context(
        None,
        Arc::new(std::sync::atomic::AtomicBool::new(false)),
    )?;
    let planning =
        crate::connector::acquire_metadata_planning_lease(connector_control, target_catalog)?;
    let documents = planning
        .derive_document_storage_lease()
        .map_err(|error| error.to_string())?;
    let request = ConnectorDocumentObservationRequest::try_new(
        documents.owner().clone(),
        documents.catalog_handle().clone(),
        ConnectorTableIdentity {
            instance_id: documents.owner().instance_id.clone(),
            namespace: Arc::from(target.namespace()),
            table: Arc::from(target.name()),
        },
        projection.facts.source_revision().target_object_id.clone(),
        budget.clone(),
        context.clone(),
    )
    .map_err(|error| error.to_string())?;
    // The repository is discovery only. This sealed read neither enters Current
    // management nor installs readiness, and no retained cache payload is proof.
    let frozen = observe_frozen_publication_documents(
        &documents,
        request,
        PersistenceDecodeBudget::default(),
    )
    .map_err(|error| format!("observe MV rewrite documents: {error}"))?;
    if frozen.definition_revision() != projection.facts.source_revision().definition_revision {
        return Err(
            "MV rewrite discovery definition changed before its frozen publication read".into(),
        );
    }
    let target_binding = planning
        .binding()
        .metadata()
        .rebind_table_object_binding(ConnectorTableObjectRebindRequest {
            table: frozen.target().clone(),
            expected_object_id: frozen.object_id().clone(),
            resolution: ConnectorTableResolution::StrictBaseTable,
            selector: ConnectorTableObjectSelector::Current,
            context,
        })
        .map_err(|error| format!("bind frozen MV rewrite output object: {error}"))?;
    let snapshot = frozen
        .output_version()
        .snapshot_id()
        .ok_or("MV rewrite output provider did not expose an exact snapshot selector")?;
    // The provider interprets its own typed output selector against the retained
    // object handle. Never decode output-version payloads or manufacture a fact.
    let target_revision = planning
        .binding()
        .metadata()
        .exact_semantic_revision(
            &target_binding.metadata.table,
            ConnectorReadSelector::SnapshotId(snapshot),
        )
        .map_err(|error| format!("resolve exact MV rewrite output revision: {error}"))?;
    let definition = frozen.definition();
    let sources = definition
        .relation_occurrences
        .iter()
        .zip(&frozen.publication().inputs)
        .map(|(occurrence, input)| {
            if occurrence.occurrence_id != input.relation_occurrence_id {
                return Err("MV rewrite D/P occurrence order differs".into());
            }
            let table = occurrence_table(occurrence);
            let state = freeze_base_table_state(connector_control, &table)
                .unwrap_or_else(SqlMvRewriteBaseTableFacts::unavailable);
            SqlMvRewriteSourceOccurrenceFacts::try_new(
                SqlMvRelationOccurrenceId::new(occurrence.occurrence_id),
                table,
                occurrence.qualifier_at_binding.clone(),
                Some(
                    restore_exact_query_revision(&input.object_id, &input.native_data_version)
                        .map_err(|error| {
                            format!("restore MV rewrite publication input: {error}")
                        })?,
                ),
                state,
            )
        })
        .collect::<Result<Vec<_>, String>>()?;
    let selection = freeze_mv_rewrite_selection(&frozen, target_revision)?;
    SqlMvRewriteDefinitionFacts::try_new(
        projection.mv_id,
        *frozen.definition_revision().as_bytes(),
        parse_mv_select_query(&definition.query.effective_sql)?,
        SqlMvDefinitionResolutionContext::try_new(
            definition.query.resolution.default_catalog.clone(),
            definition.query.resolution.default_namespace.clone(),
        )?,
        planning
            .binding()
            .descriptor()
            .provider_id
            .as_str()
            .to_string(),
        Some(TableIdentity {
            catalog: target_catalog.to_string(),
            namespace: target.namespace().to_string(),
            table: target.name().to_string(),
        }),
        sources,
    )?
    .with_selection_facts(selection)
}

fn freeze_mv_rewrite_selection(
    frozen: &MvFrozenPublicationReadView,
    target_revision: novarocks_spi::connector::ConnectorExactSemanticRevision,
) -> Result<SqlMvRewriteSelectionFacts, String> {
    let publication = frozen.publication();
    let publication_id = publication
        .publication_id
        .as_bytes()
        .try_into()
        .map_err(|_| "MV SQL rewrite requires a 16-byte publication identity".to_string())?;
    let inputs = frozen
        .definition()
        .relation_occurrences
        .iter()
        .zip(&publication.inputs)
        .map(|(occurrence, input)| {
            if occurrence.occurrence_id != input.relation_occurrence_id {
                return Err("MV rewrite D/P occurrence order differs".into());
            }
            SqlMvRewritePublicationInput::try_new(
                SqlMvRelationOccurrenceId::new(occurrence.occurrence_id),
                SqlMvRewritePublicationRelation::new(
                    occurrence_table(occurrence).fqn(),
                    restore_exact_query_revision(&input.object_id, &input.native_data_version)
                        .map_err(|error| format!("restore MV rewrite input: {error}"))?,
                )?,
            )
        })
        .collect::<Result<Vec<_>, String>>()?;
    let target = frozen.target();
    SqlMvRewriteSelectionFacts::try_new_with_publication(
        publication_id,
        *frozen.definition().computation_identity.as_bytes(),
        *frozen.definition_revision().as_bytes(),
        *frozen.interpretation_revision().as_bytes(),
        Arc::from(format!(
            "document:{}:output:{}",
            hex::encode(frozen.publication_revision().as_bytes()),
            hex::encode(frozen.output_version().digest())
        )),
        inputs,
        SqlMvRewritePublicationRelation::new(
            format!(
                "{}.{}.{}",
                target.instance_id.as_str(),
                target.namespace,
                target.table
            ),
            target_revision,
        )?,
    )
}

fn freeze_base_table_state(
    connector_control: &dyn novarocks_spi::connector::ConnectorControlResolver,
    table: &TableIdentity,
) -> Result<SqlMvRewriteBaseTableFacts, String> {
    let context = crate::connector::connector_request_context(
        None,
        Arc::new(std::sync::atomic::AtomicBool::new(false)),
    )?;
    let lease =
        crate::connector::acquire_metadata_planning_lease(connector_control, &table.catalog)?;
    let metadata = crate::connector::metadata_load_connector_table_with_planning_lease(
        &lease,
        context,
        &table.namespace,
        &table.table,
        novarocks_spi::connector::ConnectorTableResolution::StrictBaseTable,
    )?;
    lease
        .binding()
        .metadata()
        .exact_semantic_revision(
            &metadata.table,
            novarocks_spi::connector::ConnectorReadSelector::Current,
        )
        .map(SqlMvRewriteBaseTableFacts::resolved)
        .map_err(|error| format!("observe MV rewrite exact source {}: {error}", table.fqn()))
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use super::{candidate_base_tables_are_requested, optional_candidate_inventory};

    #[test]
    fn unavailable_inventory_becomes_an_empty_optional_candidate_set() {
        let candidates = optional_candidate_inventory::<u8, _>(Err("StateStore unavailable"));

        assert!(candidates.is_empty());
    }

    #[test]
    fn completion_need_filters_out_partial_mv_base_matches() {
        let requested = HashSet::from([
            "iceberg.sales.orders".to_string(),
            "iceberg.sales.customers".to_string(),
        ]);

        assert!(candidate_base_tables_are_requested(
            &[
                "iceberg.sales.orders".to_string(),
                "iceberg.sales.customers".to_string(),
            ],
            &requested,
        ));
        assert!(!candidate_base_tables_are_requested(
            &[
                "iceberg.sales.orders".to_string(),
                "iceberg.sales.lineitem".to_string(),
            ],
            &requested,
        ));
        assert!(!candidate_base_tables_are_requested(&[], &requested));
    }
}
