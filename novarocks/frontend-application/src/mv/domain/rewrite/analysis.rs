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

//! Freeze one rewrite analysis from canonical D/L and one exact provider
//! observation.
//!
//! Every fact here is either a document fact or a provider fact from the same
//! generation L was validated against. Where the Connector contract does not
//! yet publish a typed fact the rewrite needs, this module fails closed: it
//! never decodes a provider-opaque identity, never rebuilds the retired
//! `MvSchemaContract`, and never degrades silently into wrong pruning.

use std::collections::{BTreeMap, BTreeSet};

use novarocks_mv_application::persistence::codec::internal_retraction_count_aggregate_identity;
use novarocks_mv_application::persistence::identity::AggregateIdentity;
use novarocks_mv_application::persistence::projection::StoredMvProjection;
use novarocks_mv_application::persistence::runtime_bindings::MvRuntimeBindings;
use novarocks_mv_application::persistence::schema::MvPartitionContract;
use novarocks_sql::planning::mv::SqlMvAggregateCalls;
use novarocks_sql::planning::mv_aggregate_layout::SqlMvAggregatePhysicalLayout;
use novarocks_types::mv_aggregate_layout::MvAggregateStateRole;

use crate::mv::domain::rewrite::context::{MvRewriteAggregateAnalysis, MvRewriteAnalysisFacts};

/// Everything one refresh attempt already froze before analysis is derived.
pub(crate) struct MvRewriteAnalysisInput<'a> {
    pub projection: &'a StoredMvProjection,
    /// Reconstructed against the exact target observation for this attempt.
    pub runtime_bindings: &'a MvRuntimeBindings,
    /// The provider's own typed partition observation of the same target
    /// generation. L keeps only the opaque partition-spec version.
    pub observed_target_partition: &'a MvPartitionContract,
    /// D's relation shape, decided by reparsing D's own effective SQL.
    pub has_join: bool,
    /// SQL aggregate calls and the physical layout derived from D's query.
    pub aggregate: Option<(SqlMvAggregateCalls, SqlMvAggregatePhysicalLayout)>,
}

pub(crate) fn freeze_rewrite_analysis_facts(
    input: MvRewriteAnalysisInput<'_>,
) -> Result<MvRewriteAnalysisFacts, String> {
    let facts = &input.projection.facts;
    let interpretation = facts.interpretation();

    // A join rewrite needs each equality predicate expressed as a D occurrence
    // plus that occurrence's opaque source-field identity. The Connector
    // contract publishes no source opaque-field binding for an analyzed
    // predicate, and the analyzer's own numeric field IDs are exactly the
    // retired mapping this contract removed.
    if input.has_join {
        return Err(
            "MV join refresh needs a provider-owned source opaque-field binding for its join \
             predicates; the connector contract exposes none"
                .to_string(),
        );
    }

    let partition = if input.observed_target_partition.fields.is_empty() {
        None
    } else {
        // Affected-partition derivation needs the typed transform and the
        // opaque identity of each partition source field, both published by
        // the provider for this generation. Pruning from anything weaker would
        // silently drop affected partitions.
        return Err(
            "MV refresh of a partitioned target needs provider-owned typed partition transforms \
             and source field bindings; the connector contract exposes none"
                .to_string(),
        );
    };

    let aggregate = match input.aggregate {
        Some((calls, layout)) => {
            if interpretation.aggregates.is_empty() {
                return Err(
                    "MV rewrite analyzed aggregate calls for a definition with no aggregate \
                     interpretation"
                        .to_string(),
                );
            }
            let aggregate_id_by_index =
                aggregate_identity_by_call_index(input.runtime_bindings, &calls, &layout)?;
            Some(MvRewriteAggregateAnalysis {
                calls,
                layout,
                aggregate_id_by_index,
            })
        }
        None => {
            if !interpretation.aggregates.is_empty() {
                return Err(
                    "MV rewrite has no analyzed aggregate execution facts for an aggregate \
                     interpretation"
                        .to_string(),
                );
            }
            None
        }
    };

    Ok(MvRewriteAnalysisFacts {
        definition_revision: facts.source_revision().definition_revision,
        partition_spec_version: interpretation.target.partition_spec_version.clone(),
        join: None,
        partition,
        aggregate,
    })
}

/// Map each SQL aggregate call index onto its exact L identity.
///
/// The bridge is the physical state-column name, which both sides take from
/// the same exact target observation: SQL names the column it will write, and
/// L binds that column's opaque field identity to one aggregate's state slot.
/// The internal retraction count is L's own owner and never a user call.
fn aggregate_identity_by_call_index(
    bindings: &MvRuntimeBindings,
    calls: &SqlMvAggregateCalls,
    layout: &SqlMvAggregatePhysicalLayout,
) -> Result<Vec<AggregateIdentity>, String> {
    let retraction = internal_retraction_count_aggregate_identity();
    let mut identity_by_state_name = BTreeMap::new();
    for binding in &bindings.aggregates {
        for state in &binding.states {
            if identity_by_state_name
                .insert(state.physical.name.as_str(), &binding.aggregate_id)
                .is_some()
            {
                return Err(
                    "MV interpretation binds one physical state column to two aggregates"
                        .to_string(),
                );
            }
        }
    }

    let mut by_index: BTreeMap<usize, &AggregateIdentity> = BTreeMap::new();
    for column in layout.runtime_layout().state_columns() {
        let identity = identity_by_state_name.get(column.name()).ok_or_else(|| {
            format!(
                "MV interpretation has no aggregate state bound to physical column `{}`",
                column.name()
            )
        })?;
        if column.state_role() == MvAggregateStateRole::RetractionCount {
            if **identity != retraction {
                return Err(
                    "MV retraction count column is bound to a user aggregate identity".to_string(),
                );
            }
            continue;
        }
        if **identity == retraction {
            return Err(
                "MV user aggregate column is bound to the internal retraction count".to_string(),
            );
        }
        match by_index.insert(column.aggregate_index(), identity) {
            None => {}
            Some(previous) if previous == *identity => {}
            Some(_) => {
                return Err(
                    "MV aggregate call index maps to two different L identities".to_string()
                );
            }
        }
    }

    let expected = calls.aggregates.len();
    let ordered = (0..expected)
        .map(|index| {
            by_index
                .get(&index)
                .map(|identity| (*identity).clone())
                .ok_or_else(|| {
                    format!("MV aggregate call {index} has no bound L aggregate identity")
                })
        })
        .collect::<Result<Vec<_>, String>>()?;
    if by_index.len() != expected || ordered.iter().collect::<BTreeSet<_>>().len() != ordered.len()
    {
        return Err("MV aggregate analysis has no exact one-to-one L identity mapping".to_string());
    }
    Ok(ordered)
}
