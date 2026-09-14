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

use super::*;
use crate::persistence::identity::{
    AggregateIdentity, ApplyKeyIdentity, BranchIdentity, DocumentRevision, FieldIdentity,
    NativeDataVersion, ObjectIdentity, OutputIdentity, PartitionSpecVersion, PublicationIdentity,
    SchemaVersion, StateSlotIdentity,
};
use crate::persistence::validation::runtime::{RuntimeDefinitionFacts, RuntimeInterpretationFacts};
use crate::persistence::validation::{
    DEFAULT_MAX_DOCUMENT_BYTES, PersistenceDecodeBudget, validate_document_set,
    validate_live_relation_binding,
};
use novarocks_query_application::persisted_query_definition::{
    PersistedQueryDefinition, PersistedQueryDialect,
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

fn relation(occurrence_id: u32, object_value: u8, qualifier: &str) -> RelationOccurrence {
    RelationOccurrence {
        occurrence_id,
        catalog_at_binding: "ice".to_string(),
        namespace_at_binding: "sales".to_string(),
        relation_at_binding: "orders".to_string(),
        qualifier_at_binding: qualifier.to_string(),
        object_id: object_id(object_value),
        schema_version: schema_version(1),
        fields: vec![
            field(2, "amount", "decimal(18,2)", true),
            field(1, "order_id", "bigint", false),
        ],
    }
}

fn sample_definition() -> DefinitionDocument {
    build_definition(
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
        },
        AggregateInterpretation {
            aggregate_id: internal_retraction_count_aggregate_identity(),
            function_identity: INTERNAL_RETRACTION_COUNT_FUNCTION_IDENTITY.to_string(),
            source_fields: Vec::new(),
            state_slot_ids: vec![state_slot_id(42)],
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
        publication_id: PublicationIdentity::try_new(vec![91]).expect("publication id"),
        definition_revision: definition.revision(),
        interpretation_revision: interpretation.revision(),
        inputs: vec![
            PublicationInput {
                relation_occurrence_id: 7,
                object_id: object_id(11),
                native_data_version: native_data_version(101),
            },
            PublicationInput {
                relation_occurrence_id: 8,
                object_id: object_id(11),
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

#[test]
fn current_document_set_charges_one_aggregate_item_budget() {
    let definition = encode_definition(&sample_definition()).unwrap();
    let interpretation = encode_interpretation(&sample_interpretation(&definition)).unwrap();
    let publication =
        encode_publication(&sample_publication(&definition, &interpretation)).unwrap();
    let configuration = encode_configuration(&sample_configuration()).unwrap();
    let default = PersistenceDecodeBudget::default();
    let inputs = [
        (definition.as_bytes(), wire::Schema::DefinitionDocument),
        (
            interpretation.as_bytes(),
            wire::Schema::InterpretationDocument,
        ),
        (publication.as_bytes(), wire::Schema::PublicationDocument),
        (
            configuration.as_bytes(),
            wire::Schema::ConfigurationDocument,
        ),
    ];
    let usages = inputs
        .iter()
        .map(|(bytes, schema)| wire::preflight(bytes, *schema, default).unwrap())
        .collect::<Vec<_>>();
    let largest_document_items = usages
        .iter()
        .map(|usage| usage.expanded_items)
        .max()
        .unwrap();
    assert!(
        usages
            .iter()
            .map(|usage| usage.expanded_items)
            .sum::<usize>()
            > largest_document_items
    );
    let budget = PersistenceDecodeBudget {
        max_items: largest_document_items,
        ..default
    };
    for (bytes, schema) in inputs {
        wire::preflight(bytes, schema, budget).expect("each document fits independently");
    }

    assert!(matches!(
        preflight_current_document_set(
            definition.as_bytes(),
            interpretation.as_bytes(),
            Some(publication.as_bytes()),
            configuration.as_bytes(),
            budget,
        ),
        Err(PersistenceCodecError::ResourceBudget {
            resource: "Current expanded items",
            ..
        })
    ));
}

#[test]
fn current_query_runtime_maps_purely_without_reprinting_sql() {
    let runtime = PersistedQueryDefinition::new(
        "/* keep */ SELECT  amount FROM orders -- keep\n",
        PersistedQueryDialect::StarRocks,
        "ice",
        "sales",
    )
    .expect("runtime query");
    let source = QuerySource::try_from(runtime.clone()).expect("document source");
    assert_eq!(source.effective_sql, runtime.raw_query_source);
    assert_eq!(
        PersistedQueryDefinition::try_from(source).expect("runtime projection"),
        runtime
    );
}

#[test]
fn neutral_runtime_facts_exhaustively_rebuild_definition_and_interpretation() {
    let definition = sample_definition();
    assert!(definition.query.effective_sql.contains(" UNION ALL "));
    let runtime_definition =
        RuntimeDefinitionFacts::try_from(&definition).expect("runtime definition facts");
    let rebuilt_definition =
        DefinitionDocument::try_from(runtime_definition).expect("rebuilt definition");
    assert_eq!(rebuilt_definition, definition);

    let encoded_definition = encode_definition(&definition).expect("definition revision");
    let interpretation = sample_interpretation(&encoded_definition);
    let runtime_interpretation = RuntimeInterpretationFacts::from(&interpretation);
    let rebuilt_interpretation =
        InterpretationDocument::try_from(runtime_interpretation).expect("rebuilt interpretation");
    assert_eq!(rebuilt_interpretation, interpretation);
    encode_interpretation(&rebuilt_interpretation).expect("validated rebuilt interpretation");
}

#[test]
fn runtime_interpretation_rejects_a_missing_union_branch_from_definition_input() {
    let encoded_definition = encode_definition(&sample_definition()).expect("definition revision");
    let interpretation = sample_interpretation(&encoded_definition);
    let mut runtime = RuntimeInterpretationFacts::from(&interpretation);
    runtime.branches.pop();
    let error = InterpretationDocument::try_from(runtime)
        .expect_err("dropping a definition branch must fail the D-to-L mapping");
    assert!(error.contains("complete ordered definition branch identity set"));
}

#[test]
fn all_four_documents_round_trip_and_reference_exact_revisions() {
    let definition = sample_definition();
    let encoded_definition = encode_definition(&definition).expect("encode definition");
    assert_eq!(
        decode_definition(
            encoded_definition.as_bytes(),
            PersistenceDecodeBudget::default()
        )
        .expect("decode definition"),
        definition
    );

    let interpretation = sample_interpretation(&encoded_definition);
    let encoded_interpretation =
        encode_interpretation(&interpretation).expect("encode interpretation");
    assert_eq!(
        decode_interpretation(
            encoded_interpretation.as_bytes(),
            PersistenceDecodeBudget::default()
        )
        .expect("decode interpretation"),
        interpretation
    );

    let publication = sample_publication(&encoded_definition, &encoded_interpretation);
    let encoded_publication = encode_publication(&publication).expect("encode publication");
    assert_eq!(
        decode_publication(
            encoded_publication.as_bytes(),
            PersistenceDecodeBudget::default()
        )
        .expect("decode publication"),
        publication
    );

    let configuration = sample_configuration();
    let encoded_configuration = encode_configuration(&configuration).expect("encode configuration");
    assert_eq!(
        decode_configuration(
            encoded_configuration.as_bytes(),
            PersistenceDecodeBudget::default()
        )
        .expect("decode configuration"),
        configuration
    );

    validate_document_set(
        &definition,
        encoded_definition.revision(),
        &interpretation,
        encoded_interpretation.revision(),
        &publication,
    )
    .expect("consistent document set");
}

#[test]
fn configuration_and_external_owner_do_not_change_computation_identity() {
    let definition = sample_definition();
    let identity = definition.computation_identity;
    let mut configuration = sample_configuration();
    configuration.paused = true;
    configuration.max_staleness_ms = Some(240_000);
    encode_configuration(&configuration).expect("changed configuration");
    let owner_a = "deployment-a";
    let owner_b = "deployment-b";
    assert_ne!(owner_a, owner_b);
    assert_eq!(sample_definition().computation_identity, identity);
}

#[test]
fn set_order_is_canonical_but_occurrence_and_branch_order_remain_semantic() {
    let definition = sample_definition();
    let encoded = encode_definition(&definition).expect("definition");
    let mut field_reordered = definition.clone();
    field_reordered.relation_occurrences[0].fields.reverse();
    assert_eq!(
        encode_definition(&field_reordered).expect("reordered fields"),
        encoded
    );

    let mut reordered_occurrences = definition.relation_occurrences.clone();
    reordered_occurrences.reverse();
    let occurrence_reordered = build_definition(
        definition.query.clone(),
        reordered_occurrences,
        definition.outputs.clone(),
    )
    .expect("rebuild reordered definition identity");
    assert_ne!(
        occurrence_reordered.computation_identity,
        definition.computation_identity
    );
    assert_ne!(
        encode_definition(&occurrence_reordered)
            .expect("reordered occurrences")
            .revision(),
        encoded.revision()
    );

    let mut interpretation = sample_interpretation(&encoded);
    let original = encode_interpretation(&interpretation).expect("interpretation");
    interpretation.branches.reverse();
    assert_ne!(
        encode_interpretation(&interpretation)
            .expect("reordered branches")
            .revision(),
        original.revision()
    );
}

#[test]
fn decode_rejects_noncanonical_set_order_instead_of_minting_a_second_revision() {
    let definition = sample_definition();
    let mut dto = definition_to_proto(&definition);
    dto.relation_occurrences[0].fields.reverse();
    let error = decode_definition(&dto.encode_to_vec(), PersistenceDecodeBudget::default())
        .expect_err("noncanonical set order must fail in wire preflight");
    assert!(error.to_string().contains("noncanonical set order"));

    let mut duplicate = definition_to_proto(&definition);
    let duplicate_field = duplicate.relation_occurrences[0].fields[1].clone();
    duplicate.relation_occurrences[0]
        .fields
        .push(duplicate_field);
    let error = decode_definition(
        &duplicate.encode_to_vec(),
        PersistenceDecodeBudget::default(),
    )
    .expect_err("duplicate canonical set key must fail in wire preflight");
    assert!(error.to_string().contains("duplicate or noncanonical"));
}

#[test]
fn wire_preflight_rejects_tag_order_nonminimal_varints_and_illegal_bool() {
    // All values are otherwise a complete MANUAL configuration.
    let noncanonical_tags = [0x18, 0x00, 0x08, 0x01, 0x10, 0x01];
    let error = decode_configuration(&noncanonical_tags, PersistenceDecodeBudget::default())
        .expect_err("descending field tags must fail before prost decode");
    assert!(error.to_string().contains("canonical tag order"));

    // Field-one tag 8 is encoded with an unnecessary continuation byte.
    let nonminimal_tag = [0x88, 0x00, 0x01, 0x10, 0x01, 0x18, 0x00];
    let error = decode_configuration(&nonminimal_tag, PersistenceDecodeBudget::default())
        .expect_err("nonminimal field tag must fail before prost decode");
    assert!(error.to_string().contains("non-minimal varint"));

    // Version 1 is encoded as 0x81 0x00 instead of its one-byte form.
    let nonminimal_value = [0x08, 0x81, 0x00, 0x10, 0x01, 0x18, 0x00];
    let error = decode_configuration(&nonminimal_value, PersistenceDecodeBudget::default())
        .expect_err("nonminimal scalar must fail before prost decode");
    assert!(error.to_string().contains("non-minimal varint"));

    let illegal_bool = [0x08, 0x01, 0x10, 0x01, 0x18, 0x02];
    let error = decode_configuration(&illegal_bool, PersistenceDecodeBudget::default())
        .expect_err("boolean values outside zero and one must fail before prost decode");
    assert!(error.to_string().contains("illegal value 2"));
}

#[test]
fn stable_binding_allows_rename_and_reorder_but_rejects_rebuild_or_schema_drift() {
    let expected = relation(7, 11, "o");
    let renamed_reordered = vec![
        field(1, "renamed_order_id", "bigint", false),
        field(2, "renamed_amount", "decimal(18,2)", true),
    ];
    validate_live_relation_binding(&expected, &object_id(11), &renamed_reordered)
        .expect("stable identities survive rename and reorder");
    assert!(validate_live_relation_binding(&expected, &object_id(12), &renamed_reordered).is_err());
    assert!(
        validate_live_relation_binding(&expected, &object_id(11), &renamed_reordered[..1]).is_err()
    );
    let incompatible = vec![
        field(1, "order_id", "bigint", false),
        field(2, "amount", "double", true),
    ];
    assert!(validate_live_relation_binding(&expected, &object_id(11), &incompatible).is_err());

    let duplicate_live_identity = vec![
        field(1, "order_id", "bigint", false),
        field(1, "duplicate_order_id", "bigint", false),
        field(2, "amount", "decimal(18,2)", true),
    ];
    let error = validate_live_relation_binding(&expected, &object_id(11), &duplicate_live_identity)
        .expect_err("duplicate live field identity must fail closed");
    assert!(
        error
            .to_string()
            .contains("duplicate stable field identity")
    );
}

#[test]
fn avg_state_slots_and_union_branch_identity_survive_round_trip() {
    let definition = encode_definition(&sample_definition()).expect("definition");
    let interpretation = sample_interpretation(&definition);
    let encoded = encode_interpretation(&interpretation).expect("interpretation");
    let restored = decode_interpretation(encoded.as_bytes(), PersistenceDecodeBudget::default())
        .expect("restored");
    assert_eq!(restored.state_slots.len(), 2);
    assert!(
        restored
            .state_slots
            .iter()
            .any(|slot| slot.role == StateRole::AvgSum)
    );
    assert!(
        restored
            .state_slots
            .iter()
            .any(|slot| slot.role == StateRole::AvgCount)
    );
    assert_eq!(
        restored
            .branches
            .iter()
            .map(|branch| branch.branch_id.clone())
            .collect::<Vec<_>>(),
        vec![branch_id(61), branch_id(62)]
    );
}

#[test]
fn aggregate_state_roles_are_exhaustive_for_avg_and_forbidden_elsewhere() {
    let definition = encode_definition(&sample_definition()).expect("definition");

    let mut reversed_avg = sample_interpretation(&definition);
    reversed_avg.aggregates[0].state_slot_ids.reverse();
    let error =
        encode_interpretation(&reversed_avg).expect_err("AVG state must be sum followed by count");
    assert!(error.to_string().contains("exactly one sum slot"));

    let mut non_avg = sample_interpretation(&definition);
    non_avg.aggregates[0].function_identity = "sum".to_string();
    let error =
        encode_interpretation(&non_avg).expect_err("non-AVG aggregate cannot consume AVG roles");
    assert!(error.to_string().contains("non-AVG"));

    let mut aliased_avg = sample_interpretation(&definition);
    aliased_avg.state_slots[0].target_field_id = field_id(32);
    let error = encode_interpretation(&aliased_avg)
        .expect_err("AVG sum and count state require independent target fields");
    assert!(
        error
            .to_string()
            .contains("distinct physical target fields")
    );
}

#[test]
fn canonical_internal_count_owns_the_automatic_retraction_count_state() {
    let definition = encode_definition(&sample_definition()).expect("definition");
    let interpretation = retraction_count_interpretation(&definition);

    assert_eq!(
        internal_retraction_count_aggregate_identity().as_bytes(),
        b"novarocks.mv.internal.aggregate.retraction-count.v1"
    );

    let encoded = encode_interpretation(&interpretation).expect("canonical internal owner");
    let restored = decode_interpretation(encoded.as_bytes(), PersistenceDecodeBudget::default())
        .expect("canonical internal owner round trip");
    let internal = restored
        .aggregates
        .iter()
        .find(|aggregate| aggregate.aggregate_id == internal_retraction_count_aggregate_identity())
        .expect("canonical internal aggregate");
    assert_eq!(
        internal.function_identity,
        INTERNAL_RETRACTION_COUNT_FUNCTION_IDENTITY
    );
    assert!(internal.source_fields.is_empty());
    assert_eq!(internal.state_slot_ids, vec![state_slot_id(42)]);
}

#[test]
fn automatic_retraction_count_state_requires_its_canonical_internal_owner() {
    let definition = encode_definition(&sample_definition()).expect("definition");

    let mut missing_owner = retraction_count_interpretation(&definition);
    missing_owner.aggregates.pop();
    let error = encode_interpretation(&missing_owner)
        .expect_err("automatic retraction state requires its canonical owner");
    assert!(
        error
            .to_string()
            .contains("requires the canonical internal count aggregate owner")
    );

    let mut shared_by_user = retraction_count_interpretation(&definition);
    shared_by_user.aggregates[0]
        .state_slot_ids
        .push(state_slot_id(42));
    let error = encode_interpretation(&shared_by_user)
        .expect_err("a user aggregate cannot share the internal state");
    assert!(
        error
            .to_string()
            .contains("only the canonical internal retraction-count aggregate")
    );

    let mut internal_owns_normal_state = retraction_count_interpretation(&definition);
    internal_owns_normal_state.aggregates[1]
        .state_slot_ids
        .push(state_slot_id(41));
    let error = encode_interpretation(&internal_owns_normal_state)
        .expect_err("the internal owner cannot own a normal state");
    assert!(
        error
            .to_string()
            .contains("must be COUNT(*) and own exactly its automatic retraction-count state slot")
    );

    let mut multiple_automatic_states = retraction_count_interpretation(&definition);
    multiple_automatic_states.state_slots.push(StateSlot {
        slot_id: state_slot_id(43),
        target_field_id: field_id(36),
        type_signature: "bigint".to_string(),
        nullable: false,
        role: StateRole::RetractionCount,
        encoding: StateEncoding::NativeColumnV1,
    });
    multiple_automatic_states.target.fields.push(physical(
        PhysicalFieldLogicalIdentity::State(state_slot_id(43)),
        36,
        "bigint",
        false,
    ));
    let error = encode_interpretation(&multiple_automatic_states)
        .expect_err("only one automatic retraction state is permitted");
    assert!(
        error
            .to_string()
            .contains("contains more than one automatic retraction-count state slot")
    );
}

#[test]
fn canonical_internal_owner_is_forbidden_without_an_automatic_state() {
    let definition = encode_definition(&sample_definition()).expect("definition");
    let mut interpretation = retraction_count_interpretation(&definition);
    interpretation
        .state_slots
        .retain(|slot| slot.slot_id != state_slot_id(42));
    interpretation.target.fields.retain(|field| {
        !matches!(
            field.logical_identity,
            PhysicalFieldLogicalIdentity::State(ref slot_id) if slot_id == &state_slot_id(42)
        )
    });
    interpretation.aggregates[1].state_slot_ids = vec![state_slot_id(41)];
    let error = encode_interpretation(&interpretation)
        .expect_err("internal aggregate without automatic state must fail");
    assert!(
        error
            .to_string()
            .contains("requires an automatic retraction-count state slot")
    );
}

#[test]
fn user_count_without_automatic_retraction_state_remains_normal() {
    let definition = encode_definition(&sample_definition()).expect("definition");
    let mut interpretation = sample_interpretation(&definition);
    interpretation.state_slots = vec![StateSlot {
        slot_id: state_slot_id(41),
        target_field_id: field_id(32),
        type_signature: "binary".to_string(),
        nullable: false,
        role: StateRole::Single,
        encoding: StateEncoding::NativeColumnV1,
    }];
    interpretation.aggregates = vec![AggregateInterpretation {
        aggregate_id: aggregate_id(51),
        function_identity: "count".to_string(),
        source_fields: Vec::new(),
        state_slot_ids: vec![state_slot_id(41)],
    }];
    interpretation.target.fields.retain(|field| {
        !matches!(
            field.logical_identity,
            PhysicalFieldLogicalIdentity::State(ref slot_id) if slot_id == &state_slot_id(42)
        )
    });
    let state = interpretation
        .target
        .fields
        .iter_mut()
        .find(|field| {
            matches!(
                field.logical_identity,
                PhysicalFieldLogicalIdentity::State(ref slot_id) if slot_id == &state_slot_id(41)
            )
        })
        .expect("normal count state binding");
    state.target_field_id = field_id(32);
    state.type_signature = "binary".to_string();
    state.nullable = false;

    encode_interpretation(&interpretation)
        .expect("a user COUNT(*) remains a normal aggregate without the internal owner");
}

#[test]
fn physical_logical_identity_kind_and_apply_key_must_match_exactly() {
    let definition = encode_definition(&sample_definition()).expect("definition");
    let mut interpretation = sample_interpretation(&definition);
    let apply_key = interpretation
        .target
        .fields
        .iter_mut()
        .find(|field| {
            matches!(
                field.logical_identity,
                PhysicalFieldLogicalIdentity::ApplyKey(_)
            )
        })
        .expect("apply-key binding");
    apply_key.logical_identity = PhysicalFieldLogicalIdentity::ApplyKey(apply_key_id(99));
    let error = encode_interpretation(&interpretation)
        .expect_err("apply-key bytes from another logical identity must not be interchangeable");
    assert!(error.to_string().contains("exactly match"));

    let mut interpretation = sample_interpretation(&definition);
    interpretation.target.fields[0].logical_identity =
        PhysicalFieldLogicalIdentity::State(state_slot_id(21));
    let error = encode_interpretation(&interpretation)
        .expect_err("same bytes under a different logical kind must not bind an output");
    assert!(error.to_string().contains("unknown state slot"));
}

#[test]
fn strict_wire_rejects_unknown_duplicate_missing_and_unknown_enum() {
    let encoded = encode_configuration(&sample_configuration()).expect("configuration");

    let mut unknown_field = encoded.as_bytes().to_vec();
    unknown_field.extend_from_slice(&[0x98, 0x06, 0x01]); // field 99, varint 1
    assert!(matches!(
        decode_configuration(&unknown_field, PersistenceDecodeBudget::default()),
        Err(PersistenceCodecError::MalformedWire(_))
    ));

    let mut duplicate_format = vec![0x08, 0x01];
    duplicate_format.extend_from_slice(encoded.as_bytes());
    assert!(matches!(
        decode_configuration(&duplicate_format, PersistenceDecodeBudget::default()),
        Err(PersistenceCodecError::MalformedWire(_))
    ));

    let missing_paused = super::proto::ConfigurationDocument {
        format_version: Some(MV_PERSISTENCE_FORMAT_VERSION),
        refresh_policy: Some(1),
        paused: None,
        refresh_interval_ms: None,
        max_staleness_ms: None,
    }
    .encode_to_vec();
    assert!(matches!(
        decode_configuration(&missing_paused, PersistenceDecodeBudget::default()),
        Err(PersistenceCodecError::MissingField("configuration.paused"))
    ));

    let unknown_enum = super::proto::ConfigurationDocument {
        format_version: Some(MV_PERSISTENCE_FORMAT_VERSION),
        refresh_policy: Some(99),
        paused: Some(false),
        refresh_interval_ms: None,
        max_staleness_ms: None,
    }
    .encode_to_vec();
    assert!(matches!(
        decode_configuration(&unknown_enum, PersistenceDecodeBudget::default()),
        Err(PersistenceCodecError::UnknownEnum { .. })
    ));

    let unknown_version = super::proto::ConfigurationDocument {
        format_version: Some(99),
        refresh_policy: Some(1),
        paused: Some(false),
        refresh_interval_ms: None,
        max_staleness_ms: None,
    }
    .encode_to_vec();
    assert!(matches!(
        decode_configuration(&unknown_version, PersistenceDecodeBudget::default()),
        Err(PersistenceCodecError::UnknownFormatVersion { .. })
    ));

    let definition = encode_definition(&sample_definition()).expect("definition");
    let mut interpretation_dto = interpretation_to_proto(&sample_interpretation(&definition));
    interpretation_dto.state_slots[0].encoding = Some(99);
    assert!(matches!(
        decode_interpretation(
            &interpretation_dto.encode_to_vec(),
            PersistenceDecodeBudget::default()
        ),
        Err(PersistenceCodecError::UnknownEnum {
            field: "interpretation.state_slot.encoding",
            ..
        })
    ));
}

#[test]
fn decode_budget_is_enforced_before_generated_decode() {
    let encoded = encode_definition(&sample_definition()).expect("definition");
    let budget = PersistenceDecodeBudget {
        max_document_bytes: encoded.as_bytes().len() - 1,
        ..PersistenceDecodeBudget::default()
    };
    assert!(matches!(
        decode_definition(encoded.as_bytes(), budget),
        Err(PersistenceCodecError::ResourceBudget {
            resource: "encoded document",
            ..
        })
    ));

    let depth_budget = PersistenceDecodeBudget {
        max_depth: 1,
        ..PersistenceDecodeBudget::default()
    };
    assert!(matches!(
        decode_definition(encoded.as_bytes(), depth_budget),
        Err(PersistenceCodecError::ResourceBudget {
            resource: "structure depth",
            ..
        })
    ));
}

#[test]
fn encode_budget_is_enforced_before_generated_dto_conversion() {
    let mut definition = sample_definition();
    definition.query.effective_sql = "x".repeat(DEFAULT_MAX_DOCUMENT_BYTES + 1);
    assert!(matches!(
        encode_definition(&definition),
        Err(PersistenceCodecError::ResourceBudget {
            resource: "source document estimate",
            ..
        })
    ));
}

#[test]
fn packed_repeated_values_consume_the_decode_item_budget() {
    let dto = super::proto::InterpretationDocument {
        format_version: Some(MV_PERSISTENCE_FORMAT_VERSION),
        definition_revision: None,
        computation_identity: None,
        outputs: Vec::new(),
        state_slots: Vec::new(),
        apply_key: None,
        aggregates: Vec::new(),
        branches: vec![super::proto::BranchInterpretation {
            branch_id: Some(vec![1]),
            relation_occurrence_ids: vec![1; PersistenceDecodeBudget::default().max_items + 1],
            output_ids: Vec::new(),
        }],
        target: None,
    };
    assert!(matches!(
        decode_interpretation(&dto.encode_to_vec(), PersistenceDecodeBudget::default()),
        Err(PersistenceCodecError::ResourceBudget {
            resource: "expanded document items",
            ..
        })
    ));
}

#[test]
fn tiny_nested_headers_consume_the_pre_prost_working_set_budget() {
    let dto = super::proto::DefinitionDocument {
        format_version: Some(MV_PERSISTENCE_FORMAT_VERSION),
        query: None,
        relation_occurrences: Vec::new(),
        outputs: vec![super::proto::OutputDefinition::default(); 32],
        computation_identity: None,
    };
    let bytes = dto.encode_to_vec();
    let old_byte_only_bound = bytes.len() * 4;
    let budget = PersistenceDecodeBudget {
        max_document_bytes: DEFAULT_MAX_DOCUMENT_BYTES,
        max_working_set_bytes: old_byte_only_bound,
        max_items: PersistenceDecodeBudget::default().max_items,
        max_depth: PersistenceDecodeBudget::default().max_depth,
    };
    assert!(matches!(
        decode_definition(&bytes, budget),
        Err(PersistenceCodecError::ResourceBudget {
            resource: "decode working set",
            ..
        })
    ));
}

#[test]
fn stale_definition_or_interpretation_revision_is_rejected() {
    let definition = sample_definition();
    let encoded_definition = encode_definition(&definition).expect("definition");
    let interpretation = sample_interpretation(&encoded_definition);
    let encoded_interpretation = encode_interpretation(&interpretation).expect("interpretation");
    let mut publication = sample_publication(&encoded_definition, &encoded_interpretation);
    publication.definition_revision = DocumentRevision::from_canonical_bytes(b"stale");
    assert!(
        validate_document_set(
            &definition,
            encoded_definition.revision(),
            &interpretation,
            encoded_interpretation.revision(),
            &publication,
        )
        .is_err()
    );
}
