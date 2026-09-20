// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

//! Exact provider schema to MV interpretation bindings, without identity decoding.

use super::codec::{
    PhysicalFieldLogicalIdentity, StateEncoding, StateRole, TargetPartitionFieldBinding,
};
use super::identity::{
    AggregateIdentity, BranchIdentity, FieldIdentity, OutputIdentity, PartitionSpecVersion,
    SchemaVersion, StateSlotIdentity,
};
use super::projection::MvDocumentProjection;
use novarocks_spi::connector::{ConnectorCommittedVersion, ConnectorTableObjectId};
use std::collections::{BTreeMap, BTreeSet};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MvExactTargetSchemaFacts {
    pub object_id: ConnectorTableObjectId,
    pub metadata_version: ConnectorCommittedVersion,
    pub schema_version: SchemaVersion,
    pub partition_spec_version: PartitionSpecVersion,
    pub fields: Vec<MvPhysicalFieldFacts>,
    /// Same-generation provider order and opaque identities.
    pub partition_fields: Vec<TargetPartitionFieldBinding>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MvPhysicalFieldFacts {
    pub field_id: FieldIdentity,
    pub name: String,
    pub ordinal: u32,
    pub type_signature: String,
    pub nullable: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MvRuntimeStateBinding {
    pub slot_id: StateSlotIdentity,
    pub role: StateRole,
    pub encoding: StateEncoding,
    pub physical: MvPhysicalFieldFacts,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MvRuntimeAggregateBinding {
    pub aggregate_id: AggregateIdentity,
    /// Algorithm order, not canonical slot-id order or provider column order.
    pub states: Vec<MvRuntimeStateBinding>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MvRuntimeBindings {
    /// Definition output order.
    pub outputs: Vec<(OutputIdentity, MvPhysicalFieldFacts)>,
    pub aggregates: Vec<MvRuntimeAggregateBinding>,
    /// Interpretation composite-key order.
    pub apply_key: Vec<MvPhysicalFieldFacts>,
    /// Interpretation UNION branch order; branches may share the same column.
    pub branches: Vec<(BranchIdentity, MvPhysicalFieldFacts)>,
}

pub fn reconstruct_runtime_bindings(
    projection: &MvDocumentProjection,
    schema: &MvExactTargetSchemaFacts,
) -> Result<MvRuntimeBindings, String> {
    let interpretation = projection.interpretation();
    if schema.object_id != projection.source_revision().target_object_id {
        return Err("MV runtime target object is not from the exact document generation".into());
    }
    if &schema.metadata_version != projection.metadata_version() {
        return Err("MV runtime target metadata is not from the exact document generation".into());
    }
    if schema.schema_version != interpretation.target.schema_version {
        return Err(
            "MV runtime target schema version is not from the exact document generation".into(),
        );
    }
    if schema.partition_spec_version != interpretation.target.partition_spec_version {
        return Err(
            "MV runtime target partition spec is not from the exact document generation".into(),
        );
    }
    if schema.partition_fields != interpretation.target.partition_fields {
        return Err(
            "MV runtime target partition fields are not from the exact document generation".into(),
        );
    }
    let mut by_id = BTreeMap::new();
    let mut ordinals = BTreeSet::new();
    let mut names = BTreeSet::new();
    for field in &schema.fields {
        if field.name.is_empty()
            || field.type_signature.is_empty()
            || !ordinals.insert(field.ordinal)
            || !names.insert(&field.name)
            || by_id.insert(&field.field_id, field).is_some()
        {
            return Err(
                "MV runtime target schema contains missing or duplicate field facts".into(),
            );
        }
    }
    let mut logical = BTreeMap::new();
    for binding in &interpretation.target.fields {
        let field = by_id
            .get(&binding.target_field_id)
            .ok_or("MV runtime target schema is missing a bound physical field")?;
        if field.type_signature != binding.type_signature || field.nullable != binding.nullable {
            return Err("MV runtime target field type or nullability changed".into());
        }
        logical.insert(&binding.logical_identity, (*field).clone());
    }
    let lookup = |id: PhysicalFieldLogicalIdentity| {
        logical.get(&id).cloned().ok_or_else(|| {
            "MV interpretation has no physical binding for a logical identity".to_string()
        })
    };
    let outputs = projection
        .definition()
        .outputs
        .iter()
        .map(|output| {
            Ok((
                output.output_id.clone(),
                lookup(PhysicalFieldLogicalIdentity::Output(
                    output.output_id.clone(),
                ))?,
            ))
        })
        .collect::<Result<_, String>>()?;
    let slots = interpretation
        .state_slots
        .iter()
        .map(|slot| (&slot.slot_id, slot))
        .collect::<BTreeMap<_, _>>();
    let aggregates = interpretation
        .aggregates
        .iter()
        .map(|aggregate| {
            let states = aggregate
                .state_slot_ids
                .iter()
                .map(|id| {
                    let slot = slots
                        .get(id)
                        .ok_or("MV aggregate references a missing state slot")?;
                    Ok(MvRuntimeStateBinding {
                        slot_id: id.clone(),
                        role: slot.role,
                        encoding: slot.encoding,
                        physical: lookup(PhysicalFieldLogicalIdentity::State(id.clone()))?,
                    })
                })
                .collect::<Result<_, String>>()?;
            Ok(MvRuntimeAggregateBinding {
                aggregate_id: aggregate.aggregate_id.clone(),
                states,
            })
        })
        .collect::<Result<_, String>>()?;
    let apply_key = interpretation
        .apply_key
        .components
        .iter()
        .map(|component| {
            lookup(PhysicalFieldLogicalIdentity::ApplyKey(
                component.logical_id.clone(),
            ))
        })
        .collect::<Result<_, _>>()?;
    let branches = interpretation
        .branches
        .iter()
        .map(|branch| {
            Ok((
                branch.branch_id.clone(),
                lookup(PhysicalFieldLogicalIdentity::Branch(
                    branch.branch_id.clone(),
                ))?,
            ))
        })
        .collect::<Result<_, String>>()?;
    Ok(MvRuntimeBindings {
        outputs,
        aggregates,
        apply_key,
        branches,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::persistence::test_support::ProjectionFixture;
    use crate::product::MvTarget;

    fn fixture(retraction: bool) -> (MvDocumentProjection, MvExactTargetSchemaFacts) {
        let mut fixture =
            ProjectionFixture::new(MvTarget::from_parts(Some("ice"), "sales", "mv"), Some(11));
        if retraction {
            fixture = fixture.with_retraction_count();
        }
        let facts = fixture.build().unwrap();
        let schema = schema_for(&facts);
        (facts, schema)
    }

    fn schema_for(facts: &MvDocumentProjection) -> MvExactTargetSchemaFacts {
        let mut seen = BTreeSet::new();
        let fields = facts
            .interpretation()
            .target
            .fields
            .iter()
            .filter(|field| seen.insert(field.target_field_id.clone()))
            .enumerate()
            .map(|(ordinal, field)| MvPhysicalFieldFacts {
                field_id: field.target_field_id.clone(),
                name: format!("physical_{ordinal}"),
                ordinal: ordinal as u32,
                type_signature: field.type_signature.clone(),
                nullable: field.nullable,
            })
            .collect();
        MvExactTargetSchemaFacts {
            object_id: facts.source_revision().target_object_id.clone(),
            metadata_version: facts.metadata_version().clone(),
            schema_version: facts.interpretation().target.schema_version.clone(),
            partition_spec_version: facts.interpretation().target.partition_spec_version.clone(),
            fields,
            partition_fields: facts.interpretation().target.partition_fields.clone(),
        }
    }

    #[test]
    fn reverse_binding_retains_avg_algorithm_order_apply_key_and_shared_branch_field() {
        let (facts, mut schema) = fixture(false);
        schema.fields.reverse();
        let result = reconstruct_runtime_bindings(&facts, &schema).unwrap();
        assert_eq!(result.outputs.len(), 1);
        assert_eq!(result.aggregates[0].states[0].role, StateRole::AvgSum);
        assert_eq!(result.aggregates[0].states[1].role, StateRole::AvgCount);
        assert_eq!(result.apply_key.len(), 1);
        assert_eq!(result.branches.len(), 2);
        assert_eq!(result.branches[0].1, result.branches[1].1);
        assert_ne!(result.branches[0].0, result.branches[1].0);
    }

    #[test]
    fn reverse_binding_retains_internal_retraction_count_and_encoding() {
        let (facts, schema) = fixture(true);
        let result = reconstruct_runtime_bindings(&facts, &schema).unwrap();
        assert_eq!(result.aggregates.len(), 2);
        assert!(result.aggregates.iter().any(|aggregate| {
            aggregate
                .states
                .iter()
                .any(|state| state.role == StateRole::RetractionCount)
        }));
    }

    #[test]
    fn reverse_binding_rejects_generation_drift_missing_and_duplicate_opaque_fields() {
        let (facts, schema) = fixture(false);
        let mut wrong = schema.clone();
        wrong.metadata_version = ConnectorCommittedVersion::try_new(
            bytes::Bytes::from_static(b"other-metadata"),
            Some(11),
        )
        .unwrap();
        assert!(reconstruct_runtime_bindings(&facts, &wrong).is_err());
        let mut missing = schema.clone();
        missing.fields.pop();
        assert!(reconstruct_runtime_bindings(&facts, &missing).is_err());
        let mut duplicate = schema.clone();
        duplicate.fields.push(duplicate.fields[0].clone());
        assert!(reconstruct_runtime_bindings(&facts, &duplicate).is_err());
        let mut wrong_type = schema;
        wrong_type.fields[0].nullable = !wrong_type.fields[0].nullable;
        assert!(reconstruct_runtime_bindings(&facts, &wrong_type).is_err());
    }

    #[test]
    fn reverse_binding_requires_ordered_exact_partition_facts() {
        use crate::persistence::codec::TargetPartitionTransform;

        let mut fixture =
            ProjectionFixture::new(MvTarget::from_parts(Some("ice"), "sales", "mv"), Some(11));
        let source_target_field_id = fixture.interpretation.target.fields[0]
            .target_field_id
            .clone();
        fixture.interpretation.target.partition_fields = vec![
            TargetPartitionFieldBinding {
                partition_field_id: FieldIdentity::try_new(vec![90]).unwrap(),
                source_target_field_id: source_target_field_id.clone(),
                transform: TargetPartitionTransform::Bucket { num_buckets: 8 },
            },
            TargetPartitionFieldBinding {
                partition_field_id: FieldIdentity::try_new(vec![91]).unwrap(),
                source_target_field_id,
                transform: TargetPartitionTransform::Void,
            },
        ];
        let facts = fixture.build().unwrap();
        let mut schema = schema_for(&facts);
        assert!(reconstruct_runtime_bindings(&facts, &schema).is_ok());
        schema.partition_fields.clear();
        assert!(reconstruct_runtime_bindings(&facts, &schema).is_err());
        schema.partition_fields = facts.interpretation().target.partition_fields.clone();
        schema.partition_fields.reverse();
        assert!(reconstruct_runtime_bindings(&facts, &schema).is_err());
        schema.partition_fields.reverse();
        schema.partition_fields[0].transform = TargetPartitionTransform::Bucket { num_buckets: 16 };
        assert!(reconstruct_runtime_bindings(&facts, &schema).is_err());
    }
}
