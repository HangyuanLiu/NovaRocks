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

use super::codec::*;
use super::identity::*;
use bytes::Bytes;
use novarocks_spi::connector::ConnectorTableObjectId;

macro_rules! identity_fixture {
    ($name:ident, $kind:ty) => {
        fn $name(value: u8) -> $kind {
            <$kind>::try_new(vec![value]).expect("valid fixture identity")
        }
    };
}

identity_fixture!(object_id, ObjectIdentity);
identity_fixture!(field_id, FieldIdentity);
identity_fixture!(schema_version, SchemaVersion);
identity_fixture!(partition_spec_version, PartitionSpecVersion);
identity_fixture!(output_id, OutputIdentity);
identity_fixture!(state_slot_id, StateSlotIdentity);
identity_fixture!(aggregate_id, AggregateIdentity);
identity_fixture!(branch_id, BranchIdentity);
identity_fixture!(apply_key_id, ApplyKeyIdentity);

fn field(id: u8, name: &str, type_signature: &str, nullable: bool) -> SourceFieldBinding {
    SourceFieldBinding {
        field_id: field_id(id),
        name_at_binding: name.to_string(),
        type_signature: type_signature.to_string(),
        nullable,
    }
}

/// A source object identity as CREATE records it: the canonical exact-fact
/// envelope around the provider's own object value, not the bare value.
pub fn source_object_identity(object_value: u8) -> ObjectIdentity {
    source_object_identity_bytes(&[object_value])
}

/// A persisted source object identity, encoded the way the documents encode
/// one: the provider's identity inside the application's own fact envelope.
///
/// Fixtures that store the bare identity instead compare equal to a bare probe
/// and unequal to nothing, which is how a dependency guard that never matched
/// went unnoticed.
pub fn source_object_identity_bytes(object: &[u8]) -> ObjectIdentity {
    let object = ConnectorTableObjectId::try_new(Bytes::copy_from_slice(object))
        .expect("fixture source object");
    let revision =
        novarocks_spi::connector::ConnectorExactSemanticRevision::try_from_table_object_and_snapshot(
            novarocks_spi::connector::ConnectorProviderId::parse("iceberg")
                .expect("fixture provider"),
            &object,
            Some(9),
        )
        .expect("fixture source revision");
    crate::persistence::exact_revision::persist_exact_connector_revision(&revision)
        .expect("fixture persisted source identity")
        .0
}

fn relation(occurrence_id: u32, object_value: u8, qualifier: &str) -> RelationOccurrence {
    RelationOccurrence {
        occurrence_id,
        catalog_at_binding: "ice".to_string(),
        namespace_at_binding: "sales".to_string(),
        relation_at_binding: "orders".to_string(),
        qualifier_at_binding: qualifier.to_string(),
        object_id: source_object_identity(object_value),
        schema_version: schema_version(1),
        fields: vec![
            field(2, "amount", "decimal(18,2)", true),
            field(1, "order_id", "bigint", false),
        ],
    }
}

fn sample_definition() -> DefinitionDocument {
    build_definition(
        1_700_000_000_000,
        QuerySource {
            effective_sql: "SELECT AVG(o.amount) AS average_amount FROM ice.sales.orders o UNION ALL SELECT AVG(o2.amount) AS average_amount FROM ice.sales.orders o2".to_string(),
            dialect: QueryDialect::StarRocks,
            resolution: ResolutionContext {
                default_catalog: "ice".to_string(),
                default_namespace: "sales".to_string(),
            },
        },
        vec![relation(7, 11, "o"), relation(8, 11, "o2")],
        vec![OutputDefinition {
            output_id: output_id(21),
            name: "average_amount".to_string(),
            type_signature: "decimal(18,2)".to_string(),
            nullable: true,
            expression: ExpressionShape {
                kind: ExpressionKind::Function,
                function_identity: Some("avg".to_string()),
                source_fields: vec![
                    SourceFieldReference {
                        occurrence_id: 7,
                        field_id: field_id(2),
                    },
                    SourceFieldReference {
                        occurrence_id: 8,
                        field_id: field_id(2),
                    },
                ],
            },
        }],
    )
    .expect("definition")
}

fn physical(
    logical_identity: PhysicalFieldLogicalIdentity,
    target_field_value: u8,
    type_signature: &str,
    nullable: bool,
) -> PhysicalFieldBinding {
    PhysicalFieldBinding {
        logical_identity,
        target_field_id: field_id(target_field_value),
        type_signature: type_signature.to_string(),
        nullable,
    }
}

fn sample_interpretation(definition: &DefinitionDocument) -> InterpretationDocument {
    InterpretationDocument {
        definition_revision: super::keyed_documents::definition_revision(definition)
            .expect("fixture D semantic revision"),
        computation_identity: definition.computation_identity,
        outputs: vec![OutputBinding {
            output_id: output_id(21),
            target_field_id: field_id(31),
            type_signature: "decimal(18,2)".to_string(),
            nullable: true,
        }],
        state_slots: vec![
            StateSlot {
                slot_id: state_slot_id(41),
                target_field_id: field_id(33),
                type_signature: "bigint".to_string(),
                nullable: false,
                role: StateRole::AvgCount,
                encoding: StateEncoding::NativeColumnV1,
            },
            StateSlot {
                slot_id: state_slot_id(42),
                target_field_id: field_id(32),
                type_signature: "decimal(38,2)".to_string(),
                nullable: true,
                role: StateRole::AvgSum,
                encoding: StateEncoding::NativeColumnV1,
            },
        ],
        apply_key: ApplyKey {
            kind: ApplyKeyKind::GroupRowId,
            components: vec![ApplyKeyComponent {
                logical_id: apply_key_id(44),
                target_field_id: field_id(34),
            }],
        },
        aggregates: vec![AggregateInterpretation {
            aggregate_id: aggregate_id(51),
            function_identity: "avg".to_string(),
            source_fields: vec![SourceFieldReference {
                occurrence_id: 7,
                field_id: field_id(2),
            }],
            state_slot_ids: vec![state_slot_id(42), state_slot_id(41)],
            // The fixture declares branches, so its aggregate names the branch
            // it computes.
            branch_id: Some(branch_id(61)),
        }],
        branches: vec![
            BranchInterpretation {
                branch_id: branch_id(61),
                relation_occurrence_ids: vec![7],
                output_ids: vec![output_id(21)],
            },
            BranchInterpretation {
                branch_id: branch_id(62),
                relation_occurrence_ids: vec![8],
                output_ids: vec![output_id(21)],
            },
        ],
        target: TargetBinding {
            object_id: object_id(71),
            schema_version: schema_version(3),
            partition_spec_version: partition_spec_version(4),
            fields: vec![
                physical(
                    PhysicalFieldLogicalIdentity::Output(output_id(21)),
                    31,
                    "decimal(18,2)",
                    true,
                ),
                physical(
                    PhysicalFieldLogicalIdentity::State(state_slot_id(41)),
                    33,
                    "bigint",
                    false,
                ),
                physical(
                    PhysicalFieldLogicalIdentity::State(state_slot_id(42)),
                    32,
                    "decimal(38,2)",
                    true,
                ),
                physical(
                    PhysicalFieldLogicalIdentity::ApplyKey(apply_key_id(44)),
                    34,
                    "binary",
                    false,
                ),
                physical(
                    PhysicalFieldLogicalIdentity::Branch(branch_id(61)),
                    35,
                    "integer",
                    false,
                ),
                physical(
                    PhysicalFieldLogicalIdentity::Branch(branch_id(62)),
                    35,
                    "integer",
                    false,
                ),
            ],
            partition_fields: Vec::new(),
        },
    }
}

fn retraction_count_interpretation(definition: &DefinitionDocument) -> InterpretationDocument {
    let mut interpretation = sample_interpretation(definition);
    interpretation.state_slots = vec![
        StateSlot {
            slot_id: state_slot_id(41),
            target_field_id: field_id(32),
            type_signature: "binary".to_string(),
            nullable: false,
            role: StateRole::Single,
            encoding: StateEncoding::NativeColumnV1,
        },
        StateSlot {
            slot_id: state_slot_id(42),
            target_field_id: field_id(33),
            type_signature: "bigint".to_string(),
            nullable: false,
            role: StateRole::RetractionCount,
            encoding: StateEncoding::NativeColumnV1,
        },
    ];
    interpretation.aggregates = vec![
        AggregateInterpretation {
            aggregate_id: aggregate_id(51),
            function_identity: "sum".to_string(),
            source_fields: vec![SourceFieldReference {
                occurrence_id: 7,
                field_id: field_id(2),
            }],
            state_slot_ids: vec![state_slot_id(41)],
            // The fixture declares branches, so its aggregate names the one it
            // computes: two branches can share a state column, and the branch
            // is what tells the two stored states apart.
            branch_id: Some(branch_id(61)),
        },
        AggregateInterpretation {
            aggregate_id: internal_retraction_count_aggregate_identity(),
            function_identity: INTERNAL_RETRACTION_COUNT_FUNCTION_IDENTITY.to_string(),
            source_fields: Vec::new(),
            state_slot_ids: vec![state_slot_id(42)],
            branch_id: None,
        },
    ];
    interpretation.target.fields = vec![
        physical(
            PhysicalFieldLogicalIdentity::Output(output_id(21)),
            31,
            "decimal(18,2)",
            true,
        ),
        physical(
            PhysicalFieldLogicalIdentity::State(state_slot_id(41)),
            32,
            "binary",
            false,
        ),
        physical(
            PhysicalFieldLogicalIdentity::State(state_slot_id(42)),
            33,
            "bigint",
            false,
        ),
        physical(
            PhysicalFieldLogicalIdentity::ApplyKey(apply_key_id(44)),
            34,
            "binary",
            false,
        ),
        physical(
            PhysicalFieldLogicalIdentity::Branch(branch_id(61)),
            35,
            "integer",
            false,
        ),
        physical(
            PhysicalFieldLogicalIdentity::Branch(branch_id(62)),
            35,
            "integer",
            false,
        ),
    ];
    interpretation
}

fn sample_configuration() -> ConfigurationDocument {
    ConfigurationDocument {
        refresh_policy: RefreshPolicy::AsyncInterval,
        paused: false,
        refresh_interval_ms: Some(60_000),
        max_staleness_ms: Some(120_000),
    }
}

/// Pure D/L/C shapes with exact provider object facts; no publication is made.
/// The target object is a provider identity, separate from D's source facts.
pub struct MvDocumentFixture {
    pub object_id: ConnectorTableObjectId,
    pub definition: DefinitionDocument,
    pub interpretation: InterpretationDocument,
    pub configuration: ConfigurationDocument,
}

impl MvDocumentFixture {
    pub fn new() -> Self {
        let definition = sample_definition();
        let interpretation = sample_interpretation(&definition);
        Self {
            object_id: ConnectorTableObjectId::try_new(Bytes::from_static(&[71]))
                .expect("fixture target object"),
            definition,
            interpretation,
            configuration: sample_configuration(),
        }
    }

    pub fn with_retraction_count(mut self) -> Self {
        self.interpretation = retraction_count_interpretation(&self.definition);
        self
    }
}

impl Default for MvDocumentFixture {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod pure_document_fixture_tests {
    use super::super::codec::keyed::{BindingDocument, DataVersionFormat};
    use super::super::keyed_documents::{build_keyed_versions, definition_revision};
    use super::super::validation::{PersistenceDecodeBudget, validate_definition_interpretation};
    use super::*;
    use novarocks_spi::connector::{ConnectorExactSemanticRevision, ConnectorProviderId};

    #[test]
    fn pure_shapes_keep_full_definition_identity_and_avg_runtime_layout() {
        let f = MvDocumentFixture::new();
        let revision = definition_revision(&f.definition).unwrap();
        validate_definition_interpretation(&f.definition, revision, &f.interpretation).unwrap();
        assert_ne!(
            revision,
            encode_definition(&f.definition).unwrap().revision()
        );
        let bytes =
            super::super::codec::keyed::encode_keyed_interpretation(&f.interpretation).unwrap();
        let decoded = super::super::codec::keyed::decode_keyed_interpretation(
            bytes.as_bytes(),
            PersistenceDecodeBudget::default(),
        )
        .unwrap();
        assert_eq!(decoded, f.interpretation);
        assert_eq!(
            decoded.aggregates[0].state_slot_ids,
            vec![state_slot_id(42), state_slot_id(41)]
        );
        assert_eq!(
            decoded
                .state_slots
                .iter()
                .find(|s| s.slot_id == state_slot_id(41))
                .unwrap()
                .role,
            StateRole::AvgCount
        );
        assert_eq!(
            decoded
                .state_slots
                .iter()
                .find(|s| s.slot_id == state_slot_id(42))
                .unwrap()
                .role,
            StateRole::AvgSum
        );
        assert_eq!(
            decoded
                .target
                .fields
                .iter()
                .filter(|f| f.target_field_id == field_id(35))
                .count(),
            2
        );
    }

    #[test]
    fn pure_shapes_retain_exact_repeated_source_facts_and_binding_bound() {
        let f = MvDocumentFixture::new();
        assert_eq!(
            f.definition
                .relation_occurrences
                .iter()
                .map(|r| r.occurrence_id)
                .collect::<Vec<_>>(),
            vec![7, 8]
        );
        assert_eq!(
            f.definition.relation_occurrences[0].object_id,
            f.definition.relation_occurrences[1].object_id
        );
        let formats = f
            .definition
            .relation_occurrences
            .iter()
            .map(|occurrence| {
                let raw =
                    super::super::exact_revision::restore_persisted_object(&occurrence.object_id)
                        .unwrap();
                assert_eq!(raw.as_bytes().as_ref(), &[11]);
                let exact = ConnectorExactSemanticRevision::try_from_table_object_and_snapshot(
                    ConnectorProviderId::parse("iceberg").unwrap(),
                    &raw,
                    Some(9),
                )
                .unwrap();
                let (persisted, _) =
                    super::super::exact_revision::persist_exact_connector_revision(&exact).unwrap();
                assert_eq!(persisted, occurrence.object_id);
                let format = DataVersionFormat::from_issued_fact(exact.data_version()).unwrap();
                assert_eq!(format.max_value_bytes, 9);
                format
            })
            .collect();
        let versions = build_keyed_versions(
            &f.definition,
            &f.interpretation,
            BindingDocument {
                data_versions: formats,
            },
        )
        .unwrap();
        assert_eq!(
            versions.definition_revision(),
            f.interpretation.definition_revision
        );
        assert_eq!(
            versions.definition_revision(),
            definition_revision(&f.definition).unwrap()
        );
        assert_eq!(
            versions.interpretation.references().get("definition"),
            Some(&versions.definition.identity())
        );
    }

    #[test]
    fn pure_retraction_shape_keeps_internal_owner_and_nonnull_count() {
        let f = MvDocumentFixture::new().with_retraction_count();
        validate_definition_interpretation(
            &f.definition,
            definition_revision(&f.definition).unwrap(),
            &f.interpretation,
        )
        .unwrap();
        let internal = f
            .interpretation
            .aggregates
            .iter()
            .find(|a| a.function_identity == INTERNAL_RETRACTION_COUNT_FUNCTION_IDENTITY)
            .unwrap();
        assert_eq!(
            internal.aggregate_id,
            internal_retraction_count_aggregate_identity()
        );
        assert_eq!(internal.state_slot_ids, vec![state_slot_id(42)]);
        assert!(internal.source_fields.is_empty());
        assert_eq!(internal.branch_id, None);
        let count = f
            .interpretation
            .state_slots
            .iter()
            .find(|s| s.slot_id == state_slot_id(42))
            .unwrap();
        assert_eq!(count.role, StateRole::RetractionCount);
        assert_eq!(count.type_signature, "bigint");
        assert!(!count.nullable);
    }
}

#[path = "keyed_fixture.rs"]
mod keyed_fixture;
pub use keyed_fixture::{KeyedProjectionFixture, observed_current_keyed, sample_keyed_projection};
