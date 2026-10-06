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
use arrow::datatypes::TimeUnit;
use novarocks_connector_contract as c;
use novarocks_physical_plan as p;
use novarocks_proto_models::plan;
use novarocks_type_contract::{
    ControlShape, EvaluationDomainId, ExpressionControlFlow, ExpressionEffectContext,
    ExpressionEvaluationDomain, ExpressionInvocation, ExpressionUseId,
};
use std::{
    collections::{BTreeMap, HashMap},
    sync::Mutex,
};
use wire::carrier_type_definition::Kind;

// A conservative invoice for these bounded, freshly constructed fixture owners,
// not a measurement of HashMap tombstones or private allocator capacity.
const SOURCE: usize = 128 * 1024 * 1024;
const CAUSES: [CompileControlError; 3] = [
    CompileControlError::Cancelled,
    CompileControlError::DeadlineExceeded,
    CompileControlError::ResourceExhausted,
];
#[derive(Default)]
struct Control {
    stop: Option<(usize, CompileControlError)>,
    events: Mutex<Vec<u32>>,
}
impl PureCompileControl for Control {
    fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        assert_eq!(phase, CompilePhase::Encode);
        assert!(units <= 256);
        let mut events = self.events.lock().unwrap();
        let at = events.len();
        if let Some((stop, _)) = self.stop {
            assert!(at <= stop, "callback after refusal");
        }
        events.push(units);
        match self.stop {
            Some((stop, cause)) if stop == at => Err(cause),
            _ => Ok(()),
        }
    }
}
impl Control {
    fn trace(&self) -> Vec<u32> {
        self.events.lock().unwrap().clone()
    }
}
struct Setup;
impl PureCompileControl for Setup {
    fn checkpoint(&self, _: CompilePhase, _: u32) -> Result<(), CompileControlError> {
        Ok(())
    }
}
fn limits() -> PackageTypeProjectionLimits {
    PackageTypeProjectionLimits {
        max_definitions: 100_000,
        max_expanded_nodes: 1_000_000,
        max_string_bytes: 64 * 1024 * 1024,
        max_allocation_requests: 1_000_000,
        max_allocation_request_bytes: 512 * 1024 * 1024,
        max_coexisting_source_and_request_bytes: 1024 * 1024 * 1024,
        max_work: usize::MAX / 4,
    }
}
fn strict_limits() -> TypeProjectionLimits {
    TypeProjectionLimits {
        max_definitions: 100_000,
        max_expanded_nodes: 1_000_000,
        max_string_bytes: 64 * 1024 * 1024,
    }
}
fn draft(input: c::ConnectorWriteInputShape) -> c::ConnectorWriteRecipeDraft {
    let instance = c::ConnectorInstanceId::try_from_canonical("lake").unwrap();
    let catalog = c::CatalogHandle::new(instance.clone(), c::CatalogVersion::from_bytes([7; 32]));
    let provider = c::ConnectorProviderId::parse("iceberg").unwrap();
    let binding = c::ConnectorWriteBinding::new(
        c::ConnectorInstanceDescriptor {
            provider_id: provider.clone(),
            instance_id: instance,
        },
        catalog.clone(),
    );
    let payload = c::ConnectorEncodedPayload::new(
        c::ConnectorEnvelopeHeader::new(
            provider,
            catalog,
            c::ConnectorCodecCategory::WriteHandle,
            c::ConnectorCodecRevision::try_new(1).unwrap(),
        ),
        vec![7u8].into(),
    );
    c::ConnectorWriteRecipeDraft::try_new(binding, payload, input).unwrap()
}
fn field(token: u8, field: Field) -> c::ConnectorWriteFieldBinding {
    c::ConnectorWriteFieldBinding::new(c::ConnectorWriteFieldToken::from_bytes([token; 32]), field)
}
fn data(field: Field) -> c::ConnectorWriteRecipeDraft {
    draft(c::ConnectorWriteInputShape::Data {
        fields: vec![self::field(1, field)],
    })
}
fn metadata() -> HashMap<String, String> {
    HashMap::from([
        ("z".into(), "last\0value".into()),
        ("a".into(), "雪".repeat(6826) + "ab"),
    ])
}
fn projected<'s>(
    values: &'s [(u32, FunctionValueType)],
    fields: &'s [(u32, Arc<Field>)],
    writers: &'s [WriterTypeSource<'s>],
    control: &Control,
) -> Result<EncodedTypeTable<'s>, TypeCodecError> {
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::Encode)?;
    let result = encode_type_table_writer_sources_observed(
        values,
        fields,
        writers,
        SOURCE,
        limits(),
        &mut |_| Ok(()),
        &mut work,
    );
    if matches!(result, Err(TypeCodecError::Control(_))) {
        return result;
    }
    work.finish()?;
    result
}
fn wire_field(id: u32, carrier: u32, name: &str, nullable: bool) -> wire::FieldDefinition {
    wire::FieldDefinition {
        id,
        name: name.into(),
        nullable,
        carrier_type_id: Some(carrier),
        metadata: vec![],
        dictionary_id: None,
        dictionary_is_ordered: None,
    }
}
fn carrier(id: u32, kind: Kind) -> wire::CarrierTypeDefinition {
    wire::CarrierTypeDefinition {
        id,
        kind: Some(kind),
    }
}
fn prefixes(run: impl Fn(&Control) -> Result<(), TypeCodecError>) {
    let success = Control::default();
    let baseline = run(&success);
    let trace = success.trace();
    assert!(!trace.is_empty());
    for stop in 0..trace.len() {
        for cause in CAUSES {
            let control = Control {
                stop: Some((stop, cause)),
                ..Default::default()
            };
            assert!(
                matches!(run(&control), Err(TypeCodecError::Control(actual)) if actual == cause)
            );
            assert_eq!(control.trace(), trace[..=stop]);
        }
    }
    assert!(!matches!(baseline, Err(TypeCodecError::Control(_))));
}

#[test]
fn sender_writer_metadata_uses_original_inline_field_and_exact_canonical_wire() {
    let recipe = data(Field::new("根\0writer", DataType::Int64, false).with_metadata(metadata()));
    assert_eq!(
        recipe
            .input()
            .fields_iter()
            .next()
            .unwrap()
            .field()
            .metadata()["a"]
            .len(),
        20 * 1024
    );
    let ids = [u32::MAX];
    let writers = [WriterTypeSource::new(&recipe, &ids)];
    let control = Control::default();
    let encoded = projected(&[], &[], &writers, &control).unwrap();
    let mut expected_field = wire_field(u32::MAX, 0, "根\0writer", false);
    expected_field.metadata = vec![
        plan::ArrowFieldMetadataEntry {
            key: "a".into(),
            value: "雪".repeat(6826) + "ab",
        },
        plan::ArrowFieldMetadataEntry {
            key: "z".into(),
            value: "last\0value".into(),
        },
    ];
    assert_eq!(
        encoded.as_wire(),
        &wire::TypeTable {
            carriers: vec![carrier(
                0,
                Kind::Primitive(plan::ArrowPrimitiveType::Int64 as i32)
            )],
            fields: vec![expected_field],
            value_types: vec![]
        }
    );
    let original = recipe.input().fields_iter().next().unwrap().field();
    let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Encode).unwrap();
    assert!(std::ptr::eq(
        encoded
            .field_source_observed(u32::MAX, &mut work)
            .unwrap()
            .unwrap(),
        original
    ));
    assert!(
        encoded
            .field_observed(u32::MAX, &mut work)
            .unwrap()
            .is_none()
    );
    assert!(
        encoded
            .field_source_observed(0, &mut work)
            .unwrap()
            .is_none()
    );
    work.finish().unwrap();
    let strict_fields = [(u32::MAX, Arc::new(original.clone()))];
    assert!(matches!(
        encode_type_table_sources(&[], &strict_fields, strict_limits(), &Control::default()),
        Err(TypeCodecError::InvalidShape(
            "Arrow field metadata entry exceeds its owner bound"
        ))
    ));
    prefixes(|control| projected(&[], &[], &writers, control).map(|_| ()));
}

#[test]
fn sender_writer_struct_5000_and_timezone_2000_preserve_complete_original_occurrences() {
    let wide = data(Field::new(
        "root",
        DataType::Struct(
            (0..5000)
                .map(|n| Field::new(format!("c{n}"), DataType::Int64, false))
                .collect::<Vec<_>>()
                .into(),
        ),
        false,
    ));
    let ids = [u32::MAX];
    let writers = [WriterTypeSource::new(&wide, &ids)];
    let encoded = projected(&[], &[], &writers, &Control::default()).unwrap();
    let mut expected_carriers = vec![carrier(
        0,
        Kind::StructType(wire::StructFields {
            field_ids: (0..5000).collect(),
        }),
    )];
    expected_carriers.extend(
        (1..=5000).map(|id| carrier(id, Kind::Primitive(plan::ArrowPrimitiveType::Int64 as i32))),
    );
    let mut expected_fields = (0..5000)
        .map(|n| wire_field(n, n + 1, &format!("c{n}"), false))
        .collect::<Vec<_>>();
    expected_fields.push(wire_field(u32::MAX, 0, "root", false));
    assert_eq!(
        encoded.as_wire(),
        &wire::TypeTable {
            carriers: expected_carriers,
            fields: expected_fields,
            value_types: vec![]
        }
    );
    let original = wide.input().fields_iter().next().unwrap().field();
    let values = [(
        0,
        FunctionValueType::new(original.data_type().clone(), false),
    )];
    assert!(matches!(
        encode_type_table_sources(&values, &[], strict_limits(), &Control::default()),
        Err(TypeCodecError::ValueType(_))
    ));
    let strict_fields = [(0, Arc::new(original.clone()))];
    assert!(matches!(
        encode_type_table_sources(&[], &strict_fields, strict_limits(), &Control::default()),
        Err(TypeCodecError::ValueType(_))
    ));

    let zone = "UTC".repeat(666) + "ab";
    let timed = data(Field::new(
        "time",
        DataType::Timestamp(TimeUnit::Nanosecond, Some(zone.clone().into())),
        true,
    ));
    let timed_writers = [WriterTypeSource::new(&timed, &ids)];
    let encoded = projected(&[], &[], &timed_writers, &Control::default()).unwrap();
    assert_eq!(
        encoded.as_wire(),
        &wire::TypeTable {
            carriers: vec![carrier(
                0,
                Kind::Timestamp(plan::ArrowTimestampType {
                    unit: plan::ArrowTimeUnit::Nanosecond as i32,
                    timezone: Some(zone)
                })
            )],
            fields: vec![wire_field(u32::MAX, 0, "time", true)],
            value_types: vec![]
        }
    );
    let original = timed.input().fields_iter().next().unwrap().field();
    let values = [(
        0,
        FunctionValueType::new(original.data_type().clone(), true),
    )];
    assert!(matches!(
        encode_type_table_sources(&values, &[], strict_limits(), &Control::default()),
        Err(TypeCodecError::InvalidShape(
            "Arrow timestamp zone exceeds its owner bound"
        ))
    ));
    let strict_fields = [(0, Arc::new(original.clone()))];
    assert!(matches!(
        encode_type_table_sources(&[], &strict_fields, strict_limits(), &Control::default()),
        Err(TypeCodecError::InvalidShape(
            "Arrow timestamp zone exceeds its owner bound"
        ))
    ));
    // Sample actual callbacks only; wide field count is not a promise that an
    // opaque operation contains a cooperative 256-unit quantum.
    let control = Control::default();
    projected(&[], &[], &writers, &control).unwrap();
    let trace = control.trace();
    let mut samples = vec![0, trace.len() / 2, trace.len() - 1];
    if let Some(at) = trace.iter().position(|units| *units == 256) {
        samples.push(at);
    }
    samples.sort_unstable();
    samples.dedup();
    for stop in samples {
        for cause in CAUSES {
            let control = Control {
                stop: Some((stop, cause)),
                ..Default::default()
            };
            assert!(
                matches!(projected(&[], &[], &writers, &control), Err(TypeCodecError::Control(actual)) if actual == cause)
            );
            assert_eq!(control.trace(), trace[..=stop]);
        }
    }
}

#[test]
fn sender_sparse_ids_dictionary_attributes_and_role_order_are_original_source_facts() {
    #[allow(deprecated)]
    let dictionary = Field::new_dict(
        "dictionary",
        DataType::Dictionary(Box::new(DataType::Int8), Box::new(DataType::Utf8)),
        true,
        17,
        false,
    )
    .with_metadata(HashMap::from([("note".into(), "原\0值".into())]));
    let recipe = draft(c::ConnectorWriteInputShape::RowLineage {
        data_fields: vec![field(1, dictionary)],
        row_identity_fields: vec![field(2, Field::new("identity", DataType::Int64, false))],
    });
    let ids = [u32::MAX, 0];
    let writers = [WriterTypeSource::new(&recipe, &ids)];
    let strict = [(91, Arc::new(Field::new("strict", DataType::Boolean, false)))];
    let values = [(u32::MAX, FunctionValueType::new(DataType::Int32, true))];
    let control = Control::default();
    let encoded = projected(&values, &strict, &writers, &control).unwrap();
    let mut expected_dict = wire_field(u32::MAX, 2, "dictionary", true);
    expected_dict.dictionary_id = Some(17);
    expected_dict.dictionary_is_ordered = Some(false);
    expected_dict.metadata.push(plan::ArrowFieldMetadataEntry {
        key: "note".into(),
        value: "原\0值".into(),
    });
    assert_eq!(
        encoded.as_wire(),
        &wire::TypeTable {
            carriers: vec![
                carrier(0, Kind::Primitive(plan::ArrowPrimitiveType::Int32 as i32)),
                carrier(1, Kind::Primitive(plan::ArrowPrimitiveType::Boolean as i32)),
                carrier(
                    2,
                    Kind::Dictionary(wire::DictionaryTypes {
                        key_type_id: Some(3),
                        value_type_id: Some(4)
                    })
                ),
                carrier(3, Kind::Primitive(plan::ArrowPrimitiveType::Int8 as i32)),
                carrier(4, Kind::Primitive(plan::ArrowPrimitiveType::Utf8 as i32)),
                carrier(5, Kind::Primitive(plan::ArrowPrimitiveType::Int64 as i32))
            ],
            fields: vec![
                wire_field(91, 1, "strict", false),
                expected_dict,
                wire_field(0, 5, "identity", false)
            ],
            value_types: vec![wire::ValueTypeDefinition {
                id: u32::MAX,
                carrier_type_id: Some(0),
                nullable: true,
                logical_type: wire::LogicalType::Physical as i32
            }]
        }
    );
    let originals = recipe.input().fields_iter().collect::<Vec<_>>();
    let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Encode).unwrap();
    for (id, original) in ids.iter().zip(originals) {
        assert!(std::ptr::eq(
            encoded
                .field_source_observed(*id, &mut work)
                .unwrap()
                .unwrap(),
            original.field()
        ));
    }
    assert!(Arc::ptr_eq(
        encoded.field_observed(91, &mut work).unwrap().unwrap(),
        &strict[0].1
    ));
    work.finish().unwrap();
}

#[test]
fn sender_id_count_and_duplicate_root_sources_are_refused_with_ordinary_control_tails() {
    let recipe = data(Field::new("v", DataType::Int64, false));
    let short = [];
    let valid = [7];
    let long = [7, 8];
    let strict = [(7, Arc::new(Field::new("strict", DataType::Boolean, true)))];
    let repeated = draft(c::ConnectorWriteInputShape::Data {
        fields: vec![
            field(1, Field::new("a", DataType::Int64, false)),
            field(2, Field::new("b", DataType::Int64, false)),
        ],
    });
    let duplicate = [7, 7];
    for case in 0..5 {
        prefixes(|control| {
            let writers = match case {
                0 => vec![WriterTypeSource::new(&recipe, &short)],
                1 => vec![WriterTypeSource::new(&recipe, &valid)],
                2 => vec![
                    WriterTypeSource::new(&recipe, &valid),
                    WriterTypeSource::new(&recipe, &valid),
                ],
                3 => vec![WriterTypeSource::new(&repeated, &duplicate)],
                _ => vec![WriterTypeSource::new(&recipe, &long)],
            };
            let fields = if case == 1 { strict.as_slice() } else { &[] };
            assert!(matches!(
                projected(&[], fields, &writers, &Control::default()),
                Err(TypeCodecError::InvalidShape(_))
            ));
            projected(&[], fields, &writers, control).map(|_| ())
        });
    }
}

fn property(distribution: p::Distribution) -> p::PhysicalProperties {
    p::PhysicalProperties {
        distribution,
        row_multiplicity: p::RowMultiplicity::SingleCopy,
        ordering: Box::default(),
    }
}
fn dop() -> p::PipelineDopDomain {
    p::PipelineDopDomain {
        min: 1,
        max: 8,
        requires_power_of_two: false,
    }
}
fn relation_fields(
    builder: &mut p::FragmentBuilder,
    owner: p::NodeId,
    root: bool,
) -> Box<[p::WriterRelationField]> {
    let specs = if root {
        vec![
            (
                "kind",
                DataType::Int8,
                false,
                p::WriterRelationFieldRole::Kind,
                p::WriterDerivedKind::RelationKind,
            ),
            (
                "write_target_ordinal",
                DataType::Int32,
                true,
                p::WriterRelationFieldRole::TargetOrdinal,
                p::WriterDerivedKind::WriteTargetOrdinal,
            ),
            (
                "row_count",
                DataType::Int64,
                true,
                p::WriterRelationFieldRole::RowCount,
                p::WriterDerivedKind::AffectedRows,
            ),
            (
                "commit_fragment",
                DataType::Binary,
                true,
                p::WriterRelationFieldRole::CommitFragment,
                p::WriterDerivedKind::CommitFragment,
            ),
            (
                "input_fields",
                DataType::List(Arc::new(Field::new("item", DataType::Int32, false))),
                true,
                p::WriterRelationFieldRole::Auxiliary,
                p::WriterDerivedKind::RelationAuxiliary,
            ),
            (
                "blob_type",
                DataType::Utf8,
                true,
                p::WriterRelationFieldRole::Auxiliary,
                p::WriterDerivedKind::RelationAuxiliary,
            ),
            (
                "body",
                DataType::Binary,
                true,
                p::WriterRelationFieldRole::Auxiliary,
                p::WriterDerivedKind::RelationAuxiliary,
            ),
            (
                "properties",
                DataType::Map(
                    Arc::new(Field::new(
                        "entries",
                        DataType::Struct(
                            vec![
                                Field::new("key", DataType::Utf8, false),
                                Field::new("value", DataType::Utf8, false),
                            ]
                            .into(),
                        ),
                        false,
                    )),
                    false,
                ),
                true,
                p::WriterRelationFieldRole::Auxiliary,
                p::WriterDerivedKind::RelationAuxiliary,
            ),
        ]
    } else {
        vec![
            (
                "kind",
                DataType::Int8,
                false,
                p::WriterRelationFieldRole::Kind,
                p::WriterDerivedKind::RelationKind,
            ),
            (
                "write_target_ordinal",
                DataType::Int32,
                false,
                p::WriterRelationFieldRole::TargetOrdinal,
                p::WriterDerivedKind::WriteTargetOrdinal,
            ),
            (
                "row_count",
                DataType::Int64,
                true,
                p::WriterRelationFieldRole::RowCount,
                p::WriterDerivedKind::AffectedRows,
            ),
            (
                "commit_fragment",
                DataType::Binary,
                true,
                p::WriterRelationFieldRole::CommitFragment,
                p::WriterDerivedKind::CommitFragment,
            ),
        ]
    };
    specs
        .into_iter()
        .map(|(name, carrier, nullable, role, kind)| {
            let ty = FunctionValueType::new(carrier, nullable);
            let value = builder
                .add_value(
                    ty.clone(),
                    p::ValueOrigin::WriterDerived {
                        writer_node: owner,
                        kind,
                    },
                )
                .unwrap();
            p::WriterRelationField {
                value,
                name: name.into(),
                ty,
                role,
            }
        })
        .collect()
}
pub(crate) fn checked_writer_package(recipe: c::ConnectorWriteRecipeDraft) -> p::FragmentPackage {
    checked_writer_package_with(recipe, false)
}

/// The same producer, with its Values row authored either as a legacy literal
/// or as an actual constant pool reference (the publishable v2 form).
pub(crate) fn checked_writer_package_with(
    recipe: c::ConnectorWriteRecipeDraft,
    constant_row: bool,
) -> p::FragmentPackage {
    // A real producer/stream/finisher plan, with original cut derivation and
    // full Package publication. There are no aggregate or function calls.
    let pool = p::ConstantPoolId::new(5);
    let ordinal = c::WriteTargetOrdinal::try_new(0).unwrap();
    let edge = p::EdgeId::new(7);
    let mut builder = p::FragmentBuilder::new(p::FragmentId::new(1));
    let source = builder.reserve_node_id().unwrap();
    let expression = builder
        .add_expression(
            source,
            FunctionValueType::new(DataType::Int64, false),
            if constant_row {
                p::ExprKind::Constant(p::ConstantReference { pool, ordinal: 0 })
            } else {
                p::ExprKind::Literal(p::LiteralValue::Int64(42))
            },
        )
        .unwrap();
    let value = builder
        .add_value(
            FunctionValueType::new(DataType::Int64, false),
            p::ValueOrigin::NodeOutput {
                node: source,
                output_ordinal: 0,
            },
        )
        .unwrap();
    builder
        .insert_node_unchecked(p::PhysicalNode {
            id: source,
            inputs: Box::default(),
            required_inputs: Box::default(),
            output_properties: property(p::Distribution::Singleton),
            output: p::OutputPort {
                node: source,
                columns: Box::from([value]),
            },
            kind: p::NodeKind::Values {
                rows: Box::from([Box::from([expression])]),
            },
        })
        .unwrap();
    let writer = builder.reserve_node_id().unwrap();
    let fields = relation_fields(&mut builder, writer, false);
    let recipe_field = recipe.input().fields_iter().next().unwrap();
    builder
        .insert_node_unchecked(p::PhysicalNode {
            id: writer,
            inputs: Box::from([source]),
            required_inputs: Box::from([property(p::Distribution::Singleton)]),
            output_properties: property(p::Distribution::Unconstrained),
            output: p::OutputPort {
                node: writer,
                columns: fields.iter().map(|field| field.value).collect(),
            },
            kind: p::NodeKind::TableWriter {
                target: p::WriterTarget {
                    handle: recipe.payload().clone(),
                    write_target_ordinal: ordinal,
                    input: Box::from([value]),
                    required_distribution: p::Distribution::Singleton,
                    target_fields: Box::from([p::WriterTargetField {
                        provider_name: recipe_field.field().name().clone().into(),
                        token: recipe_field.token(),
                        input: value,
                        ty: FunctionValueType::new(DataType::Int64, false),
                        hidden: false,
                    }]),
                    output_schema: p::WriterRelationSchema {
                        revision: p::WRITER_MULTIPLEX_SCHEMA_REVISION,
                        fields: fields.clone(),
                    },
                    partial_aggregates: Box::default(),
                },
            },
        })
        .unwrap();
    let producer = builder
        .finish_definition(writer, p::FragmentSink::Stream { edge }, dop())
        .unwrap();
    let mut builder = p::FragmentBuilder::new(p::FragmentId::new(2));
    let exchange = builder.reserve_node_id().unwrap();
    let imported = fields
        .iter()
        .map(|field| {
            let value = builder
                .add_value(
                    field.ty.clone(),
                    p::ValueOrigin::ExchangeImport {
                        edge,
                        source_value: field.value,
                    },
                )
                .unwrap();
            p::WriterRelationField {
                value,
                name: field.name.clone(),
                ty: field.ty.clone(),
                role: field.role,
            }
        })
        .collect::<Box<[_]>>();
    builder
        .insert_node_unchecked(p::PhysicalNode {
            id: exchange,
            inputs: Box::default(),
            required_inputs: Box::default(),
            output_properties: property(p::Distribution::Singleton),
            output: p::OutputPort {
                node: exchange,
                columns: imported.iter().map(|field| field.value).collect(),
            },
            kind: p::NodeKind::ExchangeSource {
                edge,
                imports: fields
                    .iter()
                    .zip(&imported)
                    .map(|(a, b)| (a.value, b.value))
                    .collect(),
            },
        })
        .unwrap();
    let finish = builder.reserve_node_id().unwrap();
    let outputs = relation_fields(&mut builder, finish, true);
    builder
        .insert_node_unchecked(p::PhysicalNode {
            id: finish,
            inputs: Box::from([exchange]),
            required_inputs: Box::from([property(p::Distribution::Singleton)]),
            output_properties: property(p::Distribution::Singleton),
            output: p::OutputPort {
                node: finish,
                columns: outputs.iter().map(|field| field.value).collect(),
            },
            kind: p::NodeKind::TableFinish(p::WriterFinishSpec {
                expected_target_ordinals: Box::from([ordinal]),
                input_schema: p::WriterRelationSchema {
                    revision: p::WRITER_MULTIPLEX_SCHEMA_REVISION,
                    fields: imported.clone(),
                },
                output_schema: p::WriterRelationSchema {
                    revision: p::ROOT_WRITE_RESULT_SCHEMA_REVISION,
                    fields: outputs,
                },
                final_aggregates: Box::default(),
                grouped_unpivot: None,
            }),
        })
        .unwrap();
    let consumer = builder
        .finish_definition(finish, p::FragmentSink::Noop, dop())
        .unwrap();
    let mut plan = p::PlanBuilder::new(p::PlanVersionId::try_new([1; 16]).unwrap());
    if constant_row {
        plan.insert_constant_pool(
            pool,
            novarocks_constant_contract::ConstantPool::try_new(
                Arc::new(Field::new("row", DataType::Int64, false)),
                FunctionValueType::new(DataType::Int64, false),
                arrow::array::Array::to_data(&arrow::array::Int64Array::from(vec![42])),
                p::ConstantPolicy {
                    max_rows: 16,
                    max_array_nodes: 32,
                    max_logical_elements: 128,
                    max_retained_buffer_bytes: 65536,
                    max_type_depth: 16,
                    max_type_nodes: 128,
                    max_dictionary_depth: 8,
                    max_metadata_bytes: 4096,
                    max_library_validation_work: 1_000_000,
                    max_library_validation_bytes: 1_000_000,
                },
                CompilePhase::Validate,
                &Setup,
            )
            .unwrap(),
        )
        .unwrap();
    }
    plan.add_fragment(producer).unwrap();
    plan.add_fragment(consumer).unwrap();
    plan.add_edge(p::Edge {
        id: edge,
        kind: p::EdgeKind::Stream,
        source: p::EdgeSource {
            fragment: p::FragmentId::new(1),
            projection: fields.iter().map(|field| field.value).collect(),
        },
        destination: p::EdgeDestination {
            fragment: p::FragmentId::new(2),
            node: exchange,
            receive_mapping: fields
                .iter()
                .zip(&imported)
                .map(|(a, b)| (a.value, b.value))
                .collect(),
        },
        partitioning: p::EdgePartitioning {
            source: p::Distribution::Singleton,
            source_multiplicity: p::RowMultiplicity::SingleCopy,
            destination: p::Distribution::Singleton,
            destination_multiplicity: p::RowMultiplicity::SingleCopy,
        },
    })
    .unwrap();
    let plan = plan.finish_observed(&Setup).unwrap();
    let mut uses = BTreeMap::new();
    let mut calls = BTreeMap::new();
    let mut pruning = BTreeMap::new();
    let mut admissions = BTreeMap::new();
    for (id, fragment) in plan.fragments() {
        let roots = p::PhysicalExpressionRoots::try_new(fragment, &Setup).unwrap();
        let bindings = roots
            .sites()
            .iter()
            .enumerate()
            .map(|(ordinal, (site, _))| (*site, ExpressionUseId::new(ordinal as u32)))
            .collect::<Vec<_>>();
        let invocations = roots
            .sites()
            .iter()
            .enumerate()
            .map(|(ordinal, (_, root))| {
                assert!(matches!(
                    fragment.expressions().get(root.expr).unwrap().kind,
                    p::ExprKind::Literal(_) | p::ExprKind::Constant(_)
                ));
                ExpressionInvocation {
                    context: ExpressionEffectContext {
                        use_id: ExpressionUseId::new(ordinal as u32),
                        domain: EvaluationDomainId::new(0),
                        demand: root.demand,
                    },
                    definition: root.expr,
                    control: ControlShape::Eager,
                    arguments: Box::default(),
                }
            })
            .collect::<Vec<_>>();
        let domains = if invocations.is_empty() {
            vec![]
        } else {
            vec![ExpressionEvaluationDomain {
                id: EvaluationDomainId::new(0),
                parent: None,
                guard: None,
            }]
        };
        let flow = ExpressionControlFlow::try_new(
            domains,
            invocations,
            fragment.expressions(),
            CompilePhase::Validate,
            &Setup,
        )
        .unwrap();
        let actual = p::PhysicalRootUses::try_new(fragment, flow, bindings, &Setup).unwrap();
        calls.insert(
            *id,
            p::FrozenFragmentCalls::try_new(fragment, &actual, vec![], &Setup).unwrap(),
        );
        uses.insert(*id, actual);
        pruning.insert(
            *id,
            p::FrozenFragmentPruning::try_new(*id, vec![], &Setup).unwrap(),
        );
        admissions.insert(
            *id,
            p::FragmentPackageAdmission {
                plan_limits: p::PlanLimits::FROZEN,
                source_retained_bytes: SOURCE,
                property_projection_limits: p::PropertyProofProjectionLimits {
                    max_request_bytes: 64 * 1024 * 1024,
                    max_coexisting_bytes: 512 * 1024 * 1024,
                    max_projection_work: 128 * 1024 * 1024,
                },
            },
        );
    }
    let mut packages = p::extract_fragment_packages(
        &plan,
        &BTreeMap::new(),
        &BTreeMap::from([(ordinal, recipe)]),
        &uses,
        &calls,
        &pruning,
        &admissions,
        &Setup,
    )
    .unwrap();
    packages.remove(&p::FragmentId::new(1)).unwrap()
}

#[test]
fn sender_checked_writer_package_retains_large_top_level_metadata_without_widening_value_law() {
    let recipe = data(Field::new("v", DataType::Int64, false).with_metadata(metadata()));
    let original = recipe.clone();
    let package = checked_writer_package(recipe);
    let (node, source) = package.writes().iter().next().unwrap();
    let p::NodeKind::TableWriter { target } = &package.fragment().nodes()[node].kind else {
        panic!("actual writer expected")
    };
    assert_eq!(
        target.target_fields[0].ty,
        FunctionValueType::new(DataType::Int64, false)
    );
    let ids = [u32::MAX];
    let writers = [WriterTypeSource::new(source, &ids)];
    let control = Control::default();
    let encoded = projected(&[], &[], &writers, &control).unwrap();
    let actual = source.input().fields_iter().next().unwrap().field();
    let original = original.input().fields_iter().next().unwrap().field();
    assert!(
        std::ptr::eq(actual, original),
        "original draft clone retains the actual input owner"
    );
    let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Encode).unwrap();
    assert!(std::ptr::eq(
        encoded
            .field_source_observed(u32::MAX, &mut work)
            .unwrap()
            .unwrap(),
        actual
    ));
    work.finish().unwrap();
    assert_eq!(
        encoded.as_wire().fields[0].metadata,
        vec![
            plan::ArrowFieldMetadataEntry {
                key: "a".into(),
                value: "雪".repeat(6826) + "ab"
            },
            plan::ArrowFieldMetadataEntry {
                key: "z".into(),
                value: "last\0value".into()
            }
        ]
    );
    assert_eq!(package.writes().len(), 1);
    // These tests prove pure source/component projection, not provider runtime
    // capabilities or acceptance of wide Value carriers in a checked package.
}

#[test]
fn sender_all_five_input_shapes_keep_order_and_clone_loan_without_equal_foreign_substitution() {
    for role in 0..5 {
        let a = field(1, Field::new("first", DataType::Int64, false));
        let b = field(2, Field::new("second", DataType::Utf8, true));
        let input = match role {
            0 => c::ConnectorWriteInputShape::Data { fields: vec![a, b] },
            1 => c::ConnectorWriteInputShape::RowLineage {
                data_fields: vec![a],
                row_identity_fields: vec![b],
            },
            2 => c::ConnectorWriteInputShape::PositionDelete {
                identity_fields: vec![a],
                partition_source_fields: vec![b],
            },
            3 => c::ConnectorWriteInputShape::DeletionVector {
                identity_fields: vec![a],
                partition_source_fields: vec![b],
            },
            _ => c::ConnectorWriteInputShape::EqualityDelete {
                equality_fields: vec![a, b],
            },
        };
        let recipe = draft(input.clone());
        let cloned = recipe.clone();
        let foreign = draft(input);
        let ids = [u32::MAX, 0];
        let writers = [WriterTypeSource::new(&cloned, &ids)];
        let control = Control::default();
        let encoded = projected(&[], &[], &writers, &control).unwrap();
        assert_eq!(
            encoded.as_wire(),
            &wire::TypeTable {
                carriers: vec![
                    carrier(0, Kind::Primitive(plan::ArrowPrimitiveType::Int64 as i32)),
                    carrier(1, Kind::Primitive(plan::ArrowPrimitiveType::Utf8 as i32))
                ],
                fields: vec![
                    wire_field(u32::MAX, 0, "first", false),
                    wire_field(0, 1, "second", true)
                ],
                value_types: vec![],
            }
        );
        let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Encode).unwrap();
        for ((id, original), equal_foreign) in ids
            .iter()
            .zip(recipe.input().fields_iter())
            .zip(foreign.input().fields_iter())
        {
            let actual = encoded
                .field_source_observed(*id, &mut work)
                .unwrap()
                .unwrap();
            assert!(std::ptr::eq(actual, original.field()));
            assert!(!std::ptr::eq(actual, equal_foreign.field()));
            assert!(novarocks_type_contract::arrow_fields_exact(
                actual,
                equal_foreign.field()
            ));
        }
        work.finish().unwrap();
    }
}

#[test]
fn sender_metadata_next_known_string_refusal_precedes_its_actual_late_callback() {
    let recipe = data(
        Field::new("v", DataType::Int64, false)
            .with_metadata(HashMap::from([("k".into(), "雪".repeat(6826) + "ab")])),
    );
    let ids = [7];
    let writers = [WriterTypeSource::new(&recipe, &ids)];
    let control = Control::default();
    let mut gates = Vec::new();
    let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Encode).unwrap();
    encode_type_table_writer_sources_observed(
        &[],
        &[],
        &writers,
        SOURCE,
        limits(),
        &mut |facts| {
            gates.push((*facts, control.trace().len()));
            Ok(())
        },
        &mut work,
    )
    .unwrap();
    work.finish().unwrap();
    let baseline = control.trace();
    // Independent source bytes: root name1 + the sole entry key1 + value20480.
    // Identify the first original numerical gate at that actual iterator result,
    // not a simulated quantum or a callback ordinal copied from implementation.
    let known_string_bytes = 1 + 1 + 20 * 1024;
    let (facts, next_callback) = gates
        .iter()
        .find(|(facts, _)| facts.string_bytes == known_string_bytes)
        .unwrap();
    assert_eq!(facts.string_bytes, known_string_bytes);
    assert!(*next_callback < baseline.len());
    let mut tight = limits();
    tight.max_string_bytes = known_string_bytes - 1;
    for cause in CAUSES {
        let control = Control {
            stop: Some((*next_callback, cause)),
            ..Default::default()
        };
        let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Encode).unwrap();
        let result = encode_type_table_writer_sources_observed(
            &[],
            &[],
            &writers,
            SOURCE,
            tight,
            &mut |_| Ok(()),
            &mut work,
        );
        assert!(matches!(
            result,
            Err(TypeCodecError::Control(
                CompileControlError::ResourceExhausted
            ))
        ));
        assert_eq!(control.trace(), baseline[..*next_callback]);
        // Numeric Resource owns the result. No caller footer or opaque-exit
        // observation may invoke the armed later refusal.
    }
}

#[test]
fn sender_zero_source_invoice_refuses_before_pending_source_callbacks() {
    let recipe = data(Field::new("v", DataType::Int64, false));
    let ids = [0];
    let writers = [WriterTypeSource::new(&recipe, &ids)];
    for pending in [0, 254, 255] {
        for cause in CAUSES {
            let control = Control {
                stop: Some((1, cause)),
                ..Default::default()
            };
            let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Encode).unwrap();
            for _ in 0..pending {
                work.step().unwrap();
            }
            assert_eq!(control.trace(), vec![0]);
            let result = encode_type_table_writer_sources_observed(
                &[],
                &[],
                &writers,
                0,
                limits(),
                &mut |_| Ok(()),
                &mut work,
            );
            assert!(matches!(
                result,
                Err(TypeCodecError::Control(
                    CompileControlError::ResourceExhausted
                ))
            ));
            assert_eq!(control.trace(), vec![0]);
        }
    }
}

#[test]
fn sender_actual_projection_admits_complete_request_inventory_and_refuses_each_axis() {
    let recipe = data(
        Field::new(
            "v",
            DataType::Timestamp(TimeUnit::Nanosecond, Some("z".repeat(2000).into())),
            true,
        )
        .with_metadata(HashMap::from([("m".into(), "x".into())])),
    );
    let ids = [u32::MAX];
    let writers = [WriterTypeSource::new(&recipe, &ids)];
    let control = Control::default();
    let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Encode).unwrap();
    let mut last = None;
    let encoded = encode_type_table_writer_sources_observed(
        &[],
        &[],
        &writers,
        SOURCE,
        limits(),
        &mut |facts| {
            last = Some(*facts);
            Ok(())
        },
        &mut work,
    )
    .unwrap();
    work.finish().unwrap();
    let facts = last.unwrap();
    assert_eq!(facts.definition_count, 2);
    assert_eq!(facts.expanded_node_count, 2);
    assert_eq!(facts.string_bytes, 2003);
    assert_eq!(encoded.as_wire().carriers.len(), 1);
    assert_eq!(encoded.as_wire().fields.len(), 1);
    // One reserved root Set upper, one bounded terminal diagnostic, four
    // nonempty Strings, two metadata Vecs, and two nonempty namespace Vecs.
    assert_eq!(facts.allocation_requests_upper_bound, 1 + 128 + 4 + 2 + 2);
    let pointer = std::mem::size_of::<usize>();
    let align = std::mem::align_of::<usize>().max(std::mem::align_of::<u32>());
    let tree_raw = pointer + 4 + 11 * 4 + 5 * (align - 1) + 12 * pointer + align - 1;
    let tree_bytes = tree_raw.div_ceil(align) * align;
    let expected_bytes = tree_bytes
        + 512
        + 2003
        + std::mem::size_of::<(&str, &str)>()
        + std::mem::size_of::<plan::ArrowFieldMetadataEntry>()
        + std::mem::size_of::<wire::CarrierTypeDefinition>()
        + std::mem::size_of::<wire::FieldDefinition>();
    assert_eq!(facts.allocation_request_bytes_upper_bound, expected_bytes);
    assert_eq!(
        facts.coexisting_source_and_request_bytes_upper_bound,
        SOURCE + expected_bytes
    );
    for axis in 0..7 {
        let mut under = limits();
        match axis {
            0 => under.max_definitions = facts.definition_count - 1,
            1 => under.max_expanded_nodes = facts.expanded_node_count - 1,
            2 => under.max_string_bytes = facts.string_bytes - 1,
            3 => under.max_allocation_requests = facts.allocation_requests_upper_bound - 1,
            4 => {
                under.max_allocation_request_bytes = facts.allocation_request_bytes_upper_bound - 1
            }
            5 => {
                under.max_coexisting_source_and_request_bytes =
                    facts.coexisting_source_and_request_bytes_upper_bound - 1
            }
            _ => under.max_work = facts.cumulative_work_upper_bound - 1,
        }
        let control = Control::default();
        let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Encode).unwrap();
        let result = encode_type_table_writer_sources_observed(
            &[],
            &[],
            &writers,
            SOURCE,
            under,
            &mut |_| Ok(()),
            &mut work,
        );
        assert!(
            matches!(
                result,
                Err(TypeCodecError::Control(
                    CompileControlError::ResourceExhausted
                ))
            ),
            "axis {axis}"
        );
        // Control errors own no success or ordinary footer. The only caller
        // entry remains part of the real trace; no fabricated test quantum.
        assert_eq!(control.trace().first(), Some(&0));
    }
}
