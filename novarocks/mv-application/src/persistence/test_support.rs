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
use super::definition::{MvAcceleratorCommittedVersionRevision, MvAcceleratorSourceRevision};
use super::identity::*;
use super::projection::MvDocumentProjection;
use crate::management::{DeploymentOwner, ProcessIncarnation};
use crate::product::MvTarget;
use bytes::Bytes;
use novarocks_spi::connector::{
    ConnectorCommittedVersion, ConnectorInstanceId, ConnectorTableIdentity, ConnectorTableObjectId,
};

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
identity_fixture!(native_data_version, NativeDataVersion);
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

fn sample_interpretation(definition: &EncodedDocument) -> InterpretationDocument {
    InterpretationDocument {
        definition_revision: definition.revision(),
        computation_identity: sample_definition().computation_identity,
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
        },
    }
}

fn retraction_count_interpretation(definition: &EncodedDocument) -> InterpretationDocument {
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

fn sample_publication(
    definition: &EncodedDocument,
    interpretation: &EncodedDocument,
) -> PublicationDocument {
    PublicationDocument {
        publication_prepared_at_ms: 1_700_000_001_000,
        publication_id: PublicationIdentity::try_new(vec![91]).expect("publication id"),
        definition_revision: definition.revision(),
        interpretation_revision: interpretation.revision(),
        inputs: vec![
            PublicationInput {
                relation_occurrence_id: 7,
                object_id: source_object_identity(11),
                native_data_version: native_data_version(101),
            },
            PublicationInput {
                relation_occurrence_id: 8,
                object_id: source_object_identity(11),
                native_data_version: native_data_version(101),
            },
        ],
        output: PublicationOutput {
            object_id: object_id(71),
            empty_result: false,
        },
        kind: PublicationKind::FullRefresh,
        statistics: PublicationStatistics {
            logical_result_rows: Some(1),
            processed_input_rows: Some(100),
        },
    }
}

fn sample_configuration() -> ConfigurationDocument {
    ConfigurationDocument {
        refresh_policy: RefreshPolicy::AsyncInterval,
        paused: false,
        refresh_interval_ms: Some(60_000),
        max_staleness_ms: Some(120_000),
    }
}

/// Mutable fixture inputs. Production code cannot enable raw projection seeding.
pub struct ProjectionFixture {
    pub target: MvTarget,
    pub object_id: ConnectorTableObjectId,
    pub definition: DefinitionDocument,
    pub interpretation: InterpretationDocument,
    pub configuration: ConfigurationDocument,
    pub publication: Option<PublicationDocument>,
    pub metadata_version: ConnectorCommittedVersion,
    pub output_version: Option<ConnectorCommittedVersion>,
    pub storage_rows: Option<u64>,
}

impl ProjectionFixture {
    pub fn new(target: MvTarget, snapshot_id: Option<i64>) -> Self {
        let definition = sample_definition();
        let encoded_definition = encode_definition(&definition).expect("fixture D");
        let interpretation = sample_interpretation(&encoded_definition);
        let encoded_interpretation = encode_interpretation(&interpretation).expect("fixture L");
        Self {
            target,
            object_id: ConnectorTableObjectId::try_new(Bytes::from_static(&[71]))
                .expect("fixture object"),
            definition,
            interpretation,
            configuration: sample_configuration(),
            publication: snapshot_id
                .map(|_| sample_publication(&encoded_definition, &encoded_interpretation)),
            metadata_version: ConnectorCommittedVersion::try_new(
                Bytes::from_static(b"fixture-metadata"),
                snapshot_id,
            )
            .expect("fixture metadata"),
            output_version: snapshot_id.map(|id| {
                ConnectorCommittedVersion::try_new(Bytes::from_static(b"fixture-output"), Some(id))
                    .expect("fixture output")
            }),
            storage_rows: snapshot_id.map(|_| 1),
        }
    }

    pub fn with_retraction_count(mut self) -> Self {
        self.interpretation = retraction_count_interpretation(
            &encode_definition(&self.definition).expect("fixture D"),
        );
        self
    }

    pub fn build(mut self) -> Result<MvDocumentProjection, String> {
        self.definition = build_definition(
            self.definition.created_at_ms,
            self.definition.query.clone(),
            self.definition.relation_occurrences.clone(),
            self.definition.outputs.clone(),
        )
        .map_err(|error| error.to_string())?;
        let definition_revision = encode_definition(&self.definition)
            .map_err(|e| e.to_string())?
            .revision();
        self.interpretation.definition_revision = definition_revision;
        self.interpretation.computation_identity = self.definition.computation_identity;
        self.interpretation.target.object_id =
            ObjectIdentity::try_new(self.object_id.as_bytes().to_vec())
                .map_err(|e| e.to_string())?;
        let interpretation_revision = encode_interpretation(&self.interpretation)
            .map_err(|e| e.to_string())?
            .revision();
        if let Some(publication) = &mut self.publication {
            publication.definition_revision = definition_revision;
            publication.interpretation_revision = interpretation_revision;
            publication.output.object_id = self.interpretation.target.object_id.clone();
        }
        if self.publication.is_some() != self.output_version.is_some() {
            return Err("fixture publication/output presence differ".into());
        }
        let source = MvAcceleratorSourceRevision {
            target: ConnectorTableIdentity {
                instance_id: ConnectorInstanceId::parse(
                    self.target.catalog().ok_or("fixture catalog required")?,
                )
                .map_err(|e| e.to_string())?,
                namespace: self.target.namespace().into(),
                table: self.target.name().into(),
            },
            target_object_id: self.object_id,
            metadata_version: MvAcceleratorCommittedVersionRevision::from_committed(
                &self.metadata_version,
            ),
            definition_revision,
            interpretation_revision,
            publication_revision: self
                .publication
                .as_ref()
                .map(encode_publication)
                .transpose()
                .map_err(|e| e.to_string())?
                .map(|value| value.revision()),
            publication_output_version: self
                .output_version
                .as_ref()
                .map(MvAcceleratorCommittedVersionRevision::from_committed),
            configuration_revision: encode_configuration(&self.configuration)
                .map_err(|e| e.to_string())?
                .revision(),
            deployment_owner: DeploymentOwner::parse("test-deployment").expect("fixture owner"),
            process_incarnation: ProcessIncarnation::parse("test-process")
                .expect("fixture incarnation"),
        };
        MvDocumentProjection::try_from_parts(
            source,
            self.metadata_version,
            self.definition,
            self.interpretation,
            self.configuration,
            self.publication.zip(self.output_version),
            self.storage_rows,
        )
    }
}

pub fn sample_projection(target: MvTarget, snapshot_id: Option<i64>) -> MvDocumentProjection {
    ProjectionFixture::new(target, snapshot_id)
        .build()
        .expect("validated fixture projection")
}

/// Test-only adapter for exercising the production reservation/source protocol.
pub fn observed_current(
    facts: &MvDocumentProjection,
    catalog: novarocks_spi::connector::CatalogHandle,
) -> super::documents::MvObservedCurrentDocuments {
    use super::projection::MvPublicationState;
    let source = facts.source_revision();
    let (publication, output_version) = match facts.publication() {
        MvPublicationState::NeverPublished => (None, None),
        MvPublicationState::Published(value) => (
            Some(value.document().clone()),
            Some(value.output_version().clone()),
        ),
    };
    super::documents::MvObservedCurrentDocuments {
        management_target: crate::management::ManagedMvTarget::try_new(
            catalog,
            source.target.clone(),
            source.target_object_id.clone(),
        )
        .expect("fixture catalog"),
        deployment_owner: source.deployment_owner.clone(),
        process_incarnation: source.process_incarnation.clone(),
        target: source.target.clone(),
        target_object_id: source.target_object_id.clone(),
        metadata_version: facts.metadata_version().clone(),
        definition: facts.definition().clone(),
        definition_revision: source.definition_revision,
        interpretation: facts.interpretation().clone(),
        interpretation_revision: source.interpretation_revision,
        publication,
        publication_revision: source.publication_revision,
        publication_output_version: output_version,
        configuration: facts.configuration().clone(),
        configuration_revision: source.configuration_revision,
    }
}
