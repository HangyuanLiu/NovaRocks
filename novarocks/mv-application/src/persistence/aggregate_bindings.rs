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

//! CREATE-time aggregate bindings for durable MV interpretation documents.
//!
//! SQL owns aggregate occurrence facts and the runtime layout. The provider
//! owns target field identities. This adapter joins those immutable facts only
//! for one CREATE attempt. In particular, SQL field and aggregate ordinals are
//! lookup coordinates here; no ordinal or hidden physical column name becomes
//! a durable identity.

use std::collections::{BTreeMap, BTreeSet};

use crate::persistence::codec::{
    PhysicalFieldLogicalIdentity, StateEncoding, StateRole,
    internal_retraction_count_aggregate_identity,
};
use crate::persistence::identity::{
    AggregateIdentity, BranchIdentity, FieldIdentity, OutputIdentity, StateSlotIdentity,
};
use crate::persistence::validation::runtime::{
    RuntimeAggregateFacts, RuntimeAggregateLayoutFacts, RuntimePhysicalFieldFacts,
    RuntimeSourceFieldReference, RuntimeStateSlotFacts,
};
use bytes::Bytes;
use novarocks_spi::connector::ConnectorPreparedCreateDocumentTarget;
use novarocks_sql::planning::mv::{
    SqlMvCreatePersistenceFacts, SqlMvPersistenceSourceFieldReference,
};
use novarocks_types::mv_aggregate_layout::{MvAggregateRuntimeLayout, MvAggregateStateRole};
use sha2::{Digest, Sha256};

const AGGREGATE_IDENTITY_DOMAIN: &[u8] = b"novarocks.mv.aggregate.v1";
const BRANCH_IDENTITY_DOMAIN: &[u8] = b"novarocks.mv.branch.v1";
const OUTPUT_IDENTITY_DOMAIN: &[u8] = b"novarocks.mv.output.v1";
const STATE_SLOT_IDENTITY_DOMAIN: &[u8] = b"novarocks.mv.state-slot.v1";

/// One exact source field observed under the same provider generation used to
/// analyze CREATE. `field_ordinal` is deliberately retained only for the
/// in-memory join with SQL's occurrence-qualified fact.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MvCreateSourceFieldObservation {
    pub field_ordinal: u32,
    pub field_name: String,
    pub provider_field_id: Bytes,
    pub type_signature: String,
    pub nullable: bool,
}

/// Source fields for one syntactic relation occurrence. Repeated references to
/// one physical relation remain distinct observations keyed by occurrence ID.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MvCreateRelationObservation {
    pub occurrence_id: u32,
    pub provider_object_id: Bytes,
    pub provider_schema_version: Bytes,
    pub fields: Vec<MvCreateSourceFieldObservation>,
}

/// One exact target field that the CREATE request submitted to the provider.
/// The name and ordinal are transient request coordinates; only the provider
/// field identity is persisted into L.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct MvCreateTargetFieldObservation {
    pub request_ordinal: u32,
    pub physical_name: String,
    pub provider_field_id: Bytes,
    pub type_signature: String,
    pub nullable: bool,
}

/// All facts required to project aggregate state bindings into L.
pub(crate) struct MvAggregateCreateBindingInput<'a> {
    pub sql_facts: &'a SqlMvCreatePersistenceFacts,
    pub runtime_layout: &'a MvAggregateRuntimeLayout,
    pub source_observations: &'a [MvCreateRelationObservation],
    pub target_observations: &'a [MvCreateTargetFieldObservation],
    pub prepared_target: &'a ConnectorPreparedCreateDocumentTarget,
}

/// The aggregate-owned portions of an interpretation's durable facts.
///
/// The caller composes these values with the independently projected output,
/// apply-key, branch, and exact target facts before constructing the complete
/// interpretation document.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct MvAggregateCreateBindings {
    /// Shared semantic identities used by both D and L. The document builder
    /// must consume these values rather than recomputing output or branch
    /// hashes from an adjacent representation.
    pub source_fields: BTreeMap<(u32, u32), FieldIdentity>,
    pub output_identities: BTreeMap<u32, OutputIdentity>,
    pub branch_identities: BTreeMap<u32, BranchIdentity>,
    pub aggregate_layout: RuntimeAggregateLayoutFacts,
    pub target_fields: Vec<RuntimePhysicalFieldFacts>,
}

/// Projects durable aggregate state and physical-target bindings for one
/// CREATE. It is intentionally pure and does not discover metadata, allocate
/// a target, or persist a document.
pub(crate) fn build_mv_aggregate_create_bindings(
    input: MvAggregateCreateBindingInput<'_>,
) -> Result<MvAggregateCreateBindings, String> {
    let source_fields = source_field_map(input.sql_facts, input.source_observations)?;
    let output_identities = output_identities(input.sql_facts, &source_fields)?;
    let branches = branch_identities(input.sql_facts, &output_identities)?;
    let targets = target_field_map(input.target_observations, input.prepared_target)?;

    let mut aggregate_by_layout_index = BTreeMap::new();
    for aggregate in input.sql_facts.aggregates() {
        if aggregate_by_layout_index
            .insert(aggregate.aggregate_ordinal(), aggregate)
            .is_some()
        {
            return Err("SQL aggregate facts contain a duplicate construction ordinal".to_string());
        }
    }

    let mut state_columns_by_aggregate = BTreeMap::<usize, Vec<_>>::new();
    let mut retraction_columns = Vec::new();
    for column in input.runtime_layout.state_columns() {
        match column.state_role() {
            MvAggregateStateRole::RetractionCount => retraction_columns.push(column),
            _ => state_columns_by_aggregate
                .entry(column.aggregate_index())
                .or_default()
                .push(column),
        }
    }
    if retraction_columns.len() > 1 {
        return Err("runtime aggregate layout has multiple retraction-count states".to_string());
    }

    let mut state_slots = Vec::new();
    let mut aggregates = Vec::new();
    let mut target_fields = Vec::new();
    for (layout_index, aggregate) in aggregate_by_layout_index {
        let layout_index = usize::try_from(layout_index)
            .map_err(|_| "SQL aggregate construction ordinal exceeds usize".to_string())?;
        let columns = state_columns_by_aggregate
            .remove(&layout_index)
            .ok_or_else(|| {
                "SQL aggregate fact has no matching runtime state layout entry".to_string()
            })?;
        let source_fields = aggregate_source_fields(aggregate.source_fields(), &source_fields)?;
        let output = aggregate
            .output_ordinal()
            .and_then(|ordinal| output_identities.get(&ordinal))
            .ok_or_else(|| "aggregate fact has no matching durable output identity".to_string())?;
        let branch = aggregate
            .branch_ordinal()
            .and_then(|ordinal| branches.get(&ordinal));
        let aggregate_id = normal_aggregate_identity(
            aggregate.function_identity(),
            branch,
            output,
            &source_fields,
        );
        let state_slot_ids = bind_aggregate_states(
            aggregate.function_identity(),
            &aggregate_id,
            &columns,
            &targets,
            &mut state_slots,
            &mut target_fields,
        )?;
        aggregates.push(RuntimeAggregateFacts {
            aggregate_id,
            function_identity: aggregate.function_identity().to_string(),
            source_fields,
            state_slot_ids,
        });
    }
    if !state_columns_by_aggregate.is_empty() {
        return Err("runtime aggregate layout has an unbound user aggregate state".to_string());
    }

    if let Some(column) = retraction_columns.into_iter().next() {
        let aggregate_id = internal_retraction_count_aggregate_identity();
        let state_slot_ids = bind_retraction_state(
            &aggregate_id,
            column,
            &targets,
            &mut state_slots,
            &mut target_fields,
        )?;
        aggregates.push(RuntimeAggregateFacts {
            aggregate_id,
            function_identity: "count".to_string(),
            source_fields: Vec::new(),
            state_slot_ids,
        });
    }

    Ok(MvAggregateCreateBindings {
        source_fields,
        output_identities,
        branch_identities: branches,
        aggregate_layout: RuntimeAggregateLayoutFacts {
            state_slots,
            aggregates,
        },
        target_fields,
    })
}

fn source_field_map(
    facts: &SqlMvCreatePersistenceFacts,
    observations: &[MvCreateRelationObservation],
) -> Result<BTreeMap<(u32, u32), FieldIdentity>, String> {
    let mut observations_by_occurrence = BTreeMap::new();
    for observation in observations {
        if observations_by_occurrence
            .insert(observation.occurrence_id, observation)
            .is_some()
        {
            return Err("CREATE source observations contain a duplicate occurrence".to_string());
        }
    }
    let expected_occurrences = facts
        .relation_occurrences()
        .iter()
        .map(|relation| relation.occurrence_id().get())
        .collect::<BTreeSet<_>>();
    if observations_by_occurrence
        .keys()
        .copied()
        .collect::<BTreeSet<_>>()
        != expected_occurrences
    {
        return Err(
            "CREATE source observations do not exactly cover SQL relation occurrences".to_string(),
        );
    }

    let mut result = BTreeMap::new();
    for relation in facts.relation_occurrences() {
        let observation = observations_by_occurrence
            .get(&relation.occurrence_id().get())
            .expect("validated exact occurrence coverage");
        let mut fields = BTreeMap::new();
        for field in &observation.fields {
            if fields.insert(field.field_ordinal, field).is_some() {
                return Err("CREATE source observation has a duplicate field ordinal".to_string());
            }
        }
        for field in relation.referenced_fields() {
            let observed = fields.get(&field.field_ordinal()).ok_or_else(|| {
                "CREATE source observation is missing a SQL-referenced field ordinal".to_string()
            })?;
            if !observed.field_name.eq_ignore_ascii_case(field.name()) {
                return Err(
                    "CREATE source field ordinal resolves to a different field name".to_string(),
                );
            }
            result.insert(
                (relation.occurrence_id().get(), field.field_ordinal()),
                FieldIdentity::try_new(observed.provider_field_id.to_vec())
                    .map_err(|error| error.to_string())?,
            );
        }
    }
    Ok(result)
}

fn output_identities(
    facts: &SqlMvCreatePersistenceFacts,
    source_fields: &BTreeMap<(u32, u32), FieldIdentity>,
) -> Result<BTreeMap<u32, OutputIdentity>, String> {
    let mut outputs = BTreeMap::new();
    for output in facts.outputs() {
        let references =
            aggregate_source_fields(output.expression().source_fields(), source_fields)?;
        let mut canonical = CanonicalBytes::new(OUTPUT_IDENTITY_DOMAIN);
        canonical.text(output.name());
        canonical.text(&format!("{:?}", output.data_type()));
        canonical.bool(output.nullable());
        canonical.text(&format!("{:?}", output.expression().kind()));
        canonical.optional_text(output.expression().function_identity());
        canonical.references(&references);
        if outputs
            .insert(
                output.output_ordinal(),
                OutputIdentity::try_new(digest(canonical).to_vec())
                    .expect("SHA-256 identity is nonempty"),
            )
            .is_some()
        {
            return Err("SQL output facts contain a duplicate construction ordinal".to_string());
        }
    }
    Ok(outputs)
}

fn branch_identities(
    facts: &SqlMvCreatePersistenceFacts,
    outputs: &BTreeMap<u32, OutputIdentity>,
) -> Result<BTreeMap<u32, BranchIdentity>, String> {
    let mut branches = BTreeMap::new();
    for branch in facts.union_branches() {
        let mut canonical = CanonicalBytes::new(BRANCH_IDENTITY_DOMAIN);
        canonical.u32s(
            &branch
                .relation_occurrence_ids()
                .iter()
                .map(|id| id.get())
                .collect::<Vec<_>>(),
        );
        let output_ids = branch
            .output_ordinals()
            .iter()
            .map(|ordinal| {
                outputs
                    .get(ordinal)
                    .ok_or_else(|| "UNION branch references an unknown output ordinal".to_string())
            })
            .collect::<Result<Vec<_>, _>>()?;
        canonical.output_identities(&output_ids);
        if branches
            .insert(
                branch.branch_ordinal(),
                BranchIdentity::try_new(digest(canonical).to_vec())
                    .expect("SHA-256 identity is nonempty"),
            )
            .is_some()
        {
            return Err("SQL UNION branches contain a duplicate construction ordinal".to_string());
        }
    }
    Ok(branches)
}

fn aggregate_source_fields(
    references: &[SqlMvPersistenceSourceFieldReference],
    source_fields: &BTreeMap<(u32, u32), FieldIdentity>,
) -> Result<Vec<RuntimeSourceFieldReference>, String> {
    references
        .iter()
        .map(|reference| {
            let field_id = source_fields
                .get(&(reference.occurrence_id().get(), reference.field_ordinal()))
                .ok_or_else(|| {
                    "SQL source reference has no exact provider field observation".to_string()
                })?;
            if reference.field_name().is_empty() {
                return Err("SQL source reference has an empty field name".to_string());
            }
            Ok(RuntimeSourceFieldReference {
                occurrence_id: reference.occurrence_id().get(),
                field_id: field_id.clone(),
            })
        })
        .collect()
}

fn target_field_map<'a>(
    observations: &'a [MvCreateTargetFieldObservation],
    target: &ConnectorPreparedCreateDocumentTarget,
) -> Result<BTreeMap<&'a str, &'a MvCreateTargetFieldObservation>, String> {
    if observations.len() != target.fields().len() {
        return Err(
            "CREATE target observation count does not match prepared target fields".to_string(),
        );
    }
    let mut by_name = BTreeMap::new();
    let mut ordinals = BTreeSet::new();
    for observation in observations {
        if observation.physical_name.is_empty() || observation.type_signature.trim().is_empty() {
            return Err("CREATE target observation has an empty physical name or type".to_string());
        }
        let prepared = target
            .fields()
            .get(
                usize::try_from(observation.request_ordinal)
                    .map_err(|_| "CREATE target request ordinal exceeds usize".to_string())?,
            )
            .ok_or_else(|| {
                "CREATE target observation has an unknown prepared ordinal".to_string()
            })?;
        if prepared.request_ordinal() != observation.request_ordinal
            || prepared.provider_field_id().as_ref() != observation.provider_field_id.as_ref()
        {
            return Err(
                "CREATE target observation disagrees with prepared provider field binding"
                    .to_string(),
            );
        }
        if !ordinals.insert(observation.request_ordinal) {
            return Err(
                "CREATE target observations contain a duplicate request ordinal".to_string(),
            );
        }
        if by_name
            .insert(observation.physical_name.as_str(), observation)
            .is_some()
        {
            return Err("CREATE target observations contain a duplicate physical name".to_string());
        }
    }
    if ordinals.len() != target.fields().len() {
        return Err(
            "CREATE target observations do not exactly cover prepared ordinals".to_string(),
        );
    }
    Ok(by_name)
}

fn normal_aggregate_identity(
    function_identity: &str,
    branch: Option<&BranchIdentity>,
    output: &OutputIdentity,
    source_fields: &[RuntimeSourceFieldReference],
) -> AggregateIdentity {
    let mut canonical = CanonicalBytes::new(AGGREGATE_IDENTITY_DOMAIN);
    canonical.text(function_identity);
    canonical.optional_branch_identity(branch);
    canonical.identity(output.as_bytes());
    canonical.references(source_fields);
    AggregateIdentity::try_new(digest(canonical).to_vec()).expect("SHA-256 identity is nonempty")
}

fn bind_aggregate_states(
    function_identity: &str,
    aggregate_id: &AggregateIdentity,
    columns: &[&novarocks_types::mv_aggregate_layout::MvAggregateStateColumn],
    targets: &BTreeMap<&str, &MvCreateTargetFieldObservation>,
    state_slots: &mut Vec<RuntimeStateSlotFacts>,
    target_fields: &mut Vec<RuntimePhysicalFieldFacts>,
) -> Result<Vec<StateSlotIdentity>, String> {
    let expected_roles: &[MvAggregateStateRole] = if function_identity == "avg" {
        &[MvAggregateStateRole::AvgSum, MvAggregateStateRole::AvgCount]
    } else {
        &[MvAggregateStateRole::Single]
    };
    if columns
        .iter()
        .map(|column| column.state_role())
        .collect::<Vec<_>>()
        != expected_roles
    {
        return Err(
            "runtime aggregate state roles do not match SQL aggregate semantics".to_string(),
        );
    }
    columns
        .iter()
        .map(|column| {
            let role = state_role(column.state_role())?;
            bind_state(
                aggregate_id,
                role,
                column.name(),
                targets,
                state_slots,
                target_fields,
            )
        })
        .collect()
}

fn bind_retraction_state(
    aggregate_id: &AggregateIdentity,
    column: &novarocks_types::mv_aggregate_layout::MvAggregateStateColumn,
    targets: &BTreeMap<&str, &MvCreateTargetFieldObservation>,
    state_slots: &mut Vec<RuntimeStateSlotFacts>,
    target_fields: &mut Vec<RuntimePhysicalFieldFacts>,
) -> Result<Vec<StateSlotIdentity>, String> {
    if column.state_role() != MvAggregateStateRole::RetractionCount {
        return Err("internal retraction aggregate is bound to a non-retraction state".to_string());
    }
    Ok(vec![bind_state(
        aggregate_id,
        StateRole::RetractionCount,
        column.name(),
        targets,
        state_slots,
        target_fields,
    )?])
}

fn bind_state(
    aggregate_id: &AggregateIdentity,
    role: StateRole,
    physical_name: &str,
    targets: &BTreeMap<&str, &MvCreateTargetFieldObservation>,
    state_slots: &mut Vec<RuntimeStateSlotFacts>,
    target_fields: &mut Vec<RuntimePhysicalFieldFacts>,
) -> Result<StateSlotIdentity, String> {
    let target = targets
        .get(physical_name)
        .ok_or_else(|| "runtime state column has no exact prepared target field".to_string())?;
    let slot_id = state_slot_identity(aggregate_id, role);
    let target_field_id = FieldIdentity::try_new(target.provider_field_id.to_vec())
        .map_err(|error| error.to_string())?;
    if state_slots.iter().any(|slot| slot.slot_id == slot_id) {
        return Err("CREATE aggregate bindings derive a duplicate state-slot identity".to_string());
    }
    state_slots.push(RuntimeStateSlotFacts {
        slot_id: slot_id.clone(),
        target_field_id: target_field_id.clone(),
        type_signature: target.type_signature.clone(),
        nullable: target.nullable,
        role,
        encoding: StateEncoding::NativeColumnV1,
    });
    target_fields.push(RuntimePhysicalFieldFacts {
        logical_identity: PhysicalFieldLogicalIdentity::State(slot_id.clone()),
        target_field_id,
        type_signature: target.type_signature.clone(),
        nullable: target.nullable,
    });
    Ok(slot_id)
}

fn state_role(role: MvAggregateStateRole) -> Result<StateRole, String> {
    match role {
        MvAggregateStateRole::Single => Ok(StateRole::Single),
        MvAggregateStateRole::AvgSum => Ok(StateRole::AvgSum),
        MvAggregateStateRole::AvgCount => Ok(StateRole::AvgCount),
        MvAggregateStateRole::RetractionCount => Ok(StateRole::RetractionCount),
    }
}

fn state_slot_identity(aggregate_id: &AggregateIdentity, role: StateRole) -> StateSlotIdentity {
    let mut canonical = CanonicalBytes::new(STATE_SLOT_IDENTITY_DOMAIN);
    canonical.identity(aggregate_id.as_bytes());
    canonical.text(match role {
        StateRole::Single => "single",
        StateRole::AvgSum => "avg-sum",
        StateRole::AvgCount => "avg-count",
        StateRole::RetractionCount => "retraction-count",
    });
    StateSlotIdentity::try_new(digest(canonical).to_vec()).expect("SHA-256 identity is nonempty")
}

fn digest(canonical: CanonicalBytes) -> [u8; 32] {
    Sha256::digest(canonical.into_bytes()).into()
}

struct CanonicalBytes(Vec<u8>);

impl CanonicalBytes {
    fn new(domain: &[u8]) -> Self {
        let mut value = Self(Vec::new());
        value.bytes(domain);
        value
    }

    fn into_bytes(self) -> Vec<u8> {
        self.0
    }

    fn bytes(&mut self, value: &[u8]) {
        self.0
            .extend_from_slice(&(value.len() as u64).to_be_bytes());
        self.0.extend_from_slice(value);
    }

    fn text(&mut self, value: &str) {
        self.bytes(value.as_bytes());
    }

    fn optional_text(&mut self, value: Option<&str>) {
        match value {
            Some(value) => {
                self.0.push(1);
                self.text(value);
            }
            None => self.0.push(0),
        }
    }

    fn bool(&mut self, value: bool) {
        self.0.push(u8::from(value));
    }

    fn identity(&mut self, identity: &[u8]) {
        self.bytes(identity);
    }

    fn optional_branch_identity(&mut self, identity: Option<&BranchIdentity>) {
        match identity {
            Some(identity) => {
                self.0.push(1);
                self.identity(identity.as_bytes());
            }
            None => self.0.push(0),
        }
    }

    fn output_identities(&mut self, identities: &[&OutputIdentity]) {
        self.0
            .extend_from_slice(&(identities.len() as u64).to_be_bytes());
        for identity in identities {
            self.identity(identity.as_bytes());
        }
    }

    fn references(&mut self, references: &[RuntimeSourceFieldReference]) {
        self.0
            .extend_from_slice(&(references.len() as u64).to_be_bytes());
        for reference in references {
            self.0
                .extend_from_slice(&reference.occurrence_id.to_be_bytes());
            self.identity(reference.field_id.as_bytes());
        }
    }

    fn u32s(&mut self, values: &[u32]) {
        self.0
            .extend_from_slice(&(values.len() as u64).to_be_bytes());
        for value in values {
            self.0.extend_from_slice(&value.to_be_bytes());
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::persistence::codec::{
        ApplyKeyKind, InterpretationDocument, internal_retraction_count_aggregate_identity,
    };
    use crate::persistence::identity::{
        ApplyKeyIdentity, ComputationIdentity, DocumentRevision, ObjectIdentity,
        PartitionSpecVersion, SchemaVersion,
    };
    use crate::persistence::validation::{
        runtime::{
            RuntimeApplyKeyComponentFacts, RuntimeApplyKeyFacts, RuntimeInterpretationFacts,
            RuntimeOutputBindingFacts, RuntimeTargetFacts,
        },
        validate_interpretation,
    };
    use novarocks_spi::connector::{
        CatalogHandle, CatalogVersion, ConnectorInstanceId, ConnectorMutationOperationId,
        ConnectorPreparedCreateFieldBinding, ConnectorProviderBindingKey, ConnectorTableIdentity,
        ConnectorTableObjectId, ProviderBindingEpoch,
    };
    use novarocks_types::mv_aggregate_layout::{MvAggregateRuntimeKind, MvAggregateStateColumn};

    fn aggregate_id() -> AggregateIdentity {
        AggregateIdentity::try_new(vec![1]).expect("aggregate identity")
    }

    fn target_observation(
        ordinal: u32,
        name: &str,
        field_id: u8,
    ) -> MvCreateTargetFieldObservation {
        MvCreateTargetFieldObservation {
            request_ordinal: ordinal,
            physical_name: name.to_string(),
            provider_field_id: Bytes::from(vec![field_id]),
            type_signature: "varbinary".to_string(),
            nullable: false,
        }
    }

    fn prepared_target(field_ids: &[u8]) -> ConnectorPreparedCreateDocumentTarget {
        let instance_id = ConnectorInstanceId::parse("ice").expect("instance");
        let owner = ConnectorProviderBindingKey {
            instance_id: instance_id.clone(),
            incarnation: ProviderBindingEpoch::from_bytes([1; 16]),
        };
        ConnectorPreparedCreateDocumentTarget::try_new(
            owner,
            CatalogHandle::new(instance_id.clone(), CatalogVersion::from_bytes([2; 32])),
            ConnectorMutationOperationId::new(),
            ConnectorTableIdentity {
                instance_id,
                namespace: Arc::from("sales"),
                table: Arc::from("mv_orders"),
            },
            ConnectorTableObjectId::try_new(Bytes::from_static(b"target-object"))
                .expect("object identity"),
            Bytes::from_static(b"schema-v1"),
            Bytes::from_static(b"spec-v1"),
            field_ids
                .iter()
                .enumerate()
                .map(|(ordinal, field_id)| {
                    ConnectorPreparedCreateFieldBinding::try_new(
                        u32::try_from(ordinal).expect("small fixture ordinal"),
                        Bytes::from(vec![*field_id]),
                        format!("field_{ordinal}"),
                        "varbinary".to_string(),
                        false,
                    )
                })
                .collect::<Result<Vec<_>, _>>()
                .expect("prepared fields"),
            Bytes::from_static(b"provider-token"),
        )
        .expect("prepared target")
    }

    #[test]
    fn avg_slots_preserve_sum_count_order_and_distinct_provider_fields() {
        let observations = vec![
            target_observation(0, "__agg_state_avg_avg_sum", 11),
            target_observation(1, "__agg_state_avg_avg_count", 12),
        ];
        let columns = vec![
            MvAggregateStateColumn::new(
                "__agg_state_avg_avg_sum".to_string(),
                arrow_schema::DataType::LargeBinary,
                false,
                0,
                0,
                MvAggregateRuntimeKind::Avg,
                MvAggregateStateRole::AvgSum,
                false,
            ),
            MvAggregateStateColumn::new(
                "__agg_state_avg_avg_count".to_string(),
                arrow_schema::DataType::LargeBinary,
                false,
                0,
                0,
                MvAggregateRuntimeKind::Avg,
                MvAggregateStateRole::AvgCount,
                false,
            ),
        ];
        let prepared = prepared_target(&[11, 12]);
        let targets = target_field_map(&observations, &prepared).expect("exact prepared targets");
        let mut slots = Vec::new();
        let mut physical = Vec::new();

        let ids = bind_aggregate_states(
            "avg",
            &aggregate_id(),
            &[&columns[0], &columns[1]],
            &targets,
            &mut slots,
            &mut physical,
        )
        .expect("AVG bindings");

        assert_eq!(
            slots.iter().map(|slot| slot.role).collect::<Vec<_>>(),
            [StateRole::AvgSum, StateRole::AvgCount]
        );
        assert_eq!(ids.len(), 2);
        assert_ne!(ids[0], ids[1]);
        assert_ne!(slots[0].target_field_id, slots[1].target_field_id);
        assert_eq!(physical.len(), 2);
    }

    #[test]
    fn user_count_state_does_not_claim_the_internal_retraction_owner() {
        let observations = vec![target_observation(0, "__agg_state_count", 11)];
        let column = MvAggregateStateColumn::new(
            "__agg_state_count".to_string(),
            arrow_schema::DataType::LargeBinary,
            false,
            0,
            0,
            MvAggregateRuntimeKind::Count,
            MvAggregateStateRole::Single,
            true,
        );
        let prepared = prepared_target(&[11]);
        let targets = target_field_map(&observations, &prepared).expect("exact prepared target");
        let user_count = aggregate_id();
        let mut slots = Vec::new();
        let mut physical = Vec::new();

        let ids = bind_aggregate_states(
            "count",
            &user_count,
            &[&column],
            &targets,
            &mut slots,
            &mut physical,
        )
        .expect("user COUNT bindings");

        assert_ne!(user_count, internal_retraction_count_aggregate_identity());
        assert_eq!(slots[0].role, StateRole::Single);
        assert_eq!(ids.len(), 1);
    }

    #[test]
    fn retraction_slot_uses_the_canonical_internal_owner() {
        let observations = vec![target_observation(0, "__agg_state___ivm_row_count", 13)];
        let column = MvAggregateStateColumn::new(
            "__agg_state___ivm_row_count".to_string(),
            arrow_schema::DataType::Int64,
            false,
            0,
            1,
            MvAggregateRuntimeKind::Count,
            MvAggregateStateRole::RetractionCount,
            true,
        );
        let prepared = prepared_target(&[13]);
        let targets = target_field_map(&observations, &prepared).expect("exact prepared target");
        let owner = internal_retraction_count_aggregate_identity();
        let mut slots = Vec::new();
        let mut physical = Vec::new();

        let ids = bind_retraction_state(&owner, &column, &targets, &mut slots, &mut physical)
            .expect("retraction binding");

        assert_eq!(owner, internal_retraction_count_aggregate_identity());
        assert_eq!(ids.len(), 1);
        assert_eq!(slots[0].role, StateRole::RetractionCount);
        assert_eq!(slots[0].target_field_id.as_bytes(), &[13]);
    }

    #[test]
    fn exact_target_observations_reject_missing_extra_and_mismatched_fields() {
        let prepared = prepared_target(&[11, 12]);
        let missing = vec![target_observation(0, "first", 11)];
        assert!(
            target_field_map(&missing, &prepared)
                .expect_err("missing target field must fail")
                .contains("count")
        );

        let extra = vec![
            target_observation(0, "first", 11),
            target_observation(1, "second", 12),
            target_observation(2, "third", 13),
        ];
        assert!(
            target_field_map(&extra, &prepared)
                .expect_err("extra target field must fail")
                .contains("count")
        );

        let mismatched = vec![
            target_observation(0, "first", 11),
            target_observation(1, "second", 99),
        ];
        assert!(
            target_field_map(&mismatched, &prepared)
                .expect_err("mismatched provider field must fail")
                .contains("disagrees")
        );

        let duplicate_ordinal = vec![
            target_observation(0, "first", 11),
            target_observation(0, "duplicate", 11),
        ];
        assert!(
            target_field_map(&duplicate_ordinal, &prepared)
                .expect_err("duplicate request ordinal must fail")
                .contains("duplicate request ordinal")
        );
    }

    #[test]
    fn avg_and_retraction_bindings_form_a_valid_interpretation_fragment() {
        let observations = vec![
            target_observation(0, "__agg_state_avg_avg_sum", 11),
            target_observation(1, "__agg_state_avg_avg_count", 12),
            target_observation(2, "__agg_state___ivm_row_count", 13),
        ];
        let prepared = prepared_target(&[11, 12, 13]);
        let targets = target_field_map(&observations, &prepared).expect("exact target");
        let avg_columns = vec![
            MvAggregateStateColumn::new(
                "__agg_state_avg_avg_sum".to_string(),
                arrow_schema::DataType::LargeBinary,
                false,
                0,
                0,
                MvAggregateRuntimeKind::Avg,
                MvAggregateStateRole::AvgSum,
                false,
            ),
            MvAggregateStateColumn::new(
                "__agg_state_avg_avg_count".to_string(),
                arrow_schema::DataType::LargeBinary,
                false,
                0,
                0,
                MvAggregateRuntimeKind::Avg,
                MvAggregateStateRole::AvgCount,
                false,
            ),
        ];
        let retraction_column = MvAggregateStateColumn::new(
            "__agg_state___ivm_row_count".to_string(),
            arrow_schema::DataType::Int64,
            false,
            0,
            1,
            MvAggregateRuntimeKind::Count,
            MvAggregateStateRole::RetractionCount,
            true,
        );
        let output_id = OutputIdentity::try_new(vec![20]).expect("output identity");
        let output_field = FieldIdentity::try_new(vec![21]).expect("output field");
        let apply_id = ApplyKeyIdentity::try_new(vec![22]).expect("apply identity");
        let apply_field = FieldIdentity::try_new(vec![23]).expect("apply field");
        let avg_id = aggregate_id();
        let internal_id = internal_retraction_count_aggregate_identity();
        let mut slots = Vec::new();
        let mut state_target_fields = Vec::new();
        let avg_slot_ids = bind_aggregate_states(
            "avg",
            &avg_id,
            &[&avg_columns[0], &avg_columns[1]],
            &targets,
            &mut slots,
            &mut state_target_fields,
        )
        .expect("AVG bindings");
        let retraction_slot_ids = bind_retraction_state(
            &internal_id,
            &retraction_column,
            &targets,
            &mut slots,
            &mut state_target_fields,
        )
        .expect("retraction binding");

        let mut target_fields = state_target_fields;
        target_fields.extend([
            RuntimePhysicalFieldFacts {
                logical_identity: PhysicalFieldLogicalIdentity::Output(output_id.clone()),
                target_field_id: output_field.clone(),
                type_signature: "double".to_string(),
                nullable: true,
            },
            RuntimePhysicalFieldFacts {
                logical_identity: PhysicalFieldLogicalIdentity::ApplyKey(apply_id.clone()),
                target_field_id: apply_field.clone(),
                type_signature: "binary".to_string(),
                nullable: false,
            },
        ]);
        let interpretation = InterpretationDocument::try_from(RuntimeInterpretationFacts {
            definition_revision: DocumentRevision::from_canonical_bytes(b"definition"),
            computation_identity: ComputationIdentity::from_canonical_bytes(b"definition"),
            output_bindings: vec![RuntimeOutputBindingFacts {
                output_id,
                target_field_id: output_field,
                type_signature: "double".to_string(),
                nullable: true,
            }],
            aggregate_layout: RuntimeAggregateLayoutFacts {
                state_slots: slots,
                aggregates: vec![
                    RuntimeAggregateFacts {
                        aggregate_id: avg_id,
                        function_identity: "avg".to_string(),
                        source_fields: Vec::new(),
                        state_slot_ids: avg_slot_ids,
                    },
                    RuntimeAggregateFacts {
                        aggregate_id: internal_id,
                        function_identity: "count".to_string(),
                        source_fields: Vec::new(),
                        state_slot_ids: retraction_slot_ids,
                    },
                ],
            },
            apply_key: RuntimeApplyKeyFacts {
                kind: ApplyKeyKind::GroupRowId,
                ordered_components: vec![RuntimeApplyKeyComponentFacts {
                    logical_id: apply_id,
                    target_field_id: apply_field,
                }],
            },
            definition_branch_identities: Vec::new(),
            branches: Vec::new(),
            target: RuntimeTargetFacts {
                object_id: ObjectIdentity::try_new(vec![24]).expect("target object"),
                schema_version: SchemaVersion::try_new(vec![25]).expect("target schema"),
                partition_spec_version: PartitionSpecVersion::try_new(vec![26])
                    .expect("target spec"),
                fields: target_fields,
            },
        })
        .expect("runtime interpretation facts");

        validate_interpretation(&interpretation).expect("R2-valid interpretation");
    }
}
