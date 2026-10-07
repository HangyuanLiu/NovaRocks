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

//! Writer-family fragments (`Values -> TableWriter -> Stream` and
//! `ExchangeSource -> TableFinish -> Result`) built through the real physical
//! builders, package extraction and pure provider validation, then compiled
//! into the final LocalProgram owner. The provider compiler is a pure
//! contract fixture that keeps the draft; it is not the installed Iceberg port.

use super::*;
use crate::writer::admit_writer_family;
use arrow_schema::{DataType, Field};
use bytes::Bytes;
use novarocks_connector_contract::*;
use novarocks_functions::{
    ConstantPolicy, EngineFunctionCatalogBuilder, FunctionId, FunctionKind, FunctionOverloadId,
    InstalledPureKernel, PureEngineFunctionCatalog, PureImplementationDeclaration,
    PureImplementationId, PureKernelAbi,
};
use novarocks_local_program::{
    BindingRequirement, KernelAbiVersion, LocalProgram, ProgramChannelLayoutRole,
    ProgramChannelSite, ProgramExpressionRootSite, ProgramLexicalSource, ProgramNodeKind,
    StaticExprKind, StaticSinkProgram,
};
use novarocks_physical_plan::*;
use novarocks_type_contract::{
    ControlShape, EvaluationDomainId, ExpressionControlFlow, ExpressionEffectContext,
    ExpressionEvaluationDomain, ExpressionInvocation, ExpressionUseId, control_argument_semantics,
};
use std::{num::NonZeroUsize, time::Duration};

const WRITER_FRAGMENT: FragmentId = FragmentId::new(1);
const FINISH_FRAGMENT: FragmentId = FragmentId::new(2);
const EDGE: EdgeId = EdgeId::new(7);

struct FixtureControl;
impl PureCompileControl for FixtureControl {
    fn checkpoint(&self, _: CompilePhase, _: u32) -> Result<(), CompileControlError> {
        Ok(())
    }
}

fn ty(data_type: DataType, nullable: bool) -> ValueType {
    ValueType::new(data_type, nullable)
}

fn binding() -> ConnectorWriteBinding {
    let instance = ConnectorInstanceId::parse("lake").unwrap();
    ConnectorWriteBinding::new(
        ConnectorInstanceDescriptor {
            provider_id: ConnectorProviderId::parse("alpha").unwrap(),
            instance_id: instance.clone(),
        },
        CatalogHandle::new(instance, CatalogVersion::from_bytes([3; 32])),
    )
}

/// The provider's own input fields: names, nullability and field metadata
/// are provider facts the compiled projection keeps.
fn target_fields() -> Vec<Field> {
    vec![
        Field::new("c1", DataType::Int64, false)
            .with_metadata([("provider.field-id".to_string(), "1".to_string())].into()),
        Field::new("c2", DataType::Utf8, true),
    ]
}

fn draft() -> ConnectorWriteRecipeDraft {
    let binding = binding();
    let payload = ConnectorEncodedPayload::new(
        ConnectorEnvelopeHeader::new(
            binding.descriptor().provider_id.clone(),
            binding.catalog_handle().clone(),
            ConnectorCodecCategory::WriteHandle,
            ConnectorCodecRevision::try_new(1).unwrap(),
        ),
        Bytes::from_static(b"writer-handle"),
    );
    ConnectorWriteRecipeDraft::try_new(
        binding,
        payload,
        ConnectorWriteInputShape::Data {
            fields: target_fields()
                .into_iter()
                .enumerate()
                .map(|(ordinal, field)| {
                    ConnectorWriteFieldBinding::new(
                        ConnectorWriteFieldToken::from_bytes([ordinal as u8 + 1; 32]),
                        field,
                    )
                })
                .collect(),
        },
    )
    .unwrap()
}

fn property(distribution: Distribution) -> PhysicalProperties {
    PhysicalProperties {
        distribution,
        row_multiplicity: RowMultiplicity::SingleCopy,
        ordering: Box::default(),
    }
}

fn dop_domain() -> PipelineDopDomain {
    PipelineDopDomain {
        min: 1,
        max: 8,
        requires_power_of_two: false,
    }
}

/// The writer multiplex relation (`root == false`) or the Root result
/// relation (`root == true`), exactly as the SPI defines them.
fn relation_fields(
    builder: &mut FragmentBuilder,
    owner: NodeId,
    root: bool,
) -> Box<[WriterRelationField]> {
    use WriterDerivedKind as K;
    use WriterRelationFieldRole as R;
    let mut specs = vec![
        ("kind", DataType::Int8, false, R::Kind, K::RelationKind),
        (
            "write_target_ordinal",
            DataType::Int32,
            root,
            R::TargetOrdinal,
            K::WriteTargetOrdinal,
        ),
        (
            "row_count",
            DataType::Int64,
            true,
            R::RowCount,
            K::AffectedRows,
        ),
        (
            "commit_fragment",
            DataType::Binary,
            true,
            R::CommitFragment,
            K::CommitFragment,
        ),
    ];
    if root {
        specs.extend([
            (
                "input_fields",
                DataType::List(Arc::new(Field::new("item", DataType::Int32, false))),
                true,
                R::Auxiliary,
                K::RelationAuxiliary,
            ),
            (
                "blob_type",
                DataType::Utf8,
                true,
                R::Auxiliary,
                K::RelationAuxiliary,
            ),
            (
                "body",
                DataType::Binary,
                true,
                R::Auxiliary,
                K::RelationAuxiliary,
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
                R::Auxiliary,
                K::RelationAuxiliary,
            ),
        ]);
    }
    specs
        .into_iter()
        .map(|(name, carrier, nullable, role, kind)| {
            let ty = ty(carrier, nullable);
            let value = builder
                .add_value(
                    ty.clone(),
                    ValueOrigin::WriterDerived {
                        writer_node: owner,
                        kind,
                    },
                )
                .unwrap();
            WriterRelationField {
                value,
                name: name.into(),
                ty,
                role,
            }
        })
        .collect()
}

struct Fixture {
    writer: Arc<FragmentPackage>,
    finish: Arc<FragmentPackage>,
    /// Physical TableWriter / TableFinish node IDs.
    writer_node: NodeId,
    finish_node: NodeId,
}

/// `INSERT INTO t VALUES (1, 'a')` without statistics: the writer fragment
/// writes one Values row of a non-null BIGINT and a non-null STRING into a
/// NOT NULL `c1` and a nullable `c2`; the finish fragment publishes the Root
/// result relation.
fn fixture() -> Fixture {
    let draft = draft();
    let ordinal = WriteTargetOrdinal::try_new(0).unwrap();
    let mut builder = FragmentBuilder::new(WRITER_FRAGMENT);
    let source = builder.reserve_node_id().unwrap();
    let cells = [
        (ty(DataType::Int64, false), LiteralValue::Int64(1)),
        (ty(DataType::Utf8, false), LiteralValue::Utf8("a".into())),
    ];
    let mut row = Vec::new();
    let mut values = Vec::new();
    for (ordinal, (value_type, literal)) in cells.into_iter().enumerate() {
        row.push(
            builder
                .add_expression(source, value_type.clone(), ExprKind::Literal(literal))
                .unwrap(),
        );
        values.push(
            builder
                .add_value(
                    value_type,
                    ValueOrigin::NodeOutput {
                        node: source,
                        output_ordinal: ordinal as u32,
                    },
                )
                .unwrap(),
        );
    }
    builder
        .insert_node_unchecked(PhysicalNode {
            id: source,
            inputs: Box::default(),
            required_inputs: Box::default(),
            output_properties: property(Distribution::Singleton),
            output: OutputPort {
                node: source,
                columns: values.clone().into_boxed_slice(),
            },
            kind: NodeKind::Values {
                rows: Box::from([row.into_boxed_slice()]),
            },
        })
        .unwrap();
    let writer = builder.reserve_node_id().unwrap();
    let fields = relation_fields(&mut builder, writer, false);
    builder
        .insert_node_unchecked(PhysicalNode {
            id: writer,
            inputs: Box::from([source]),
            required_inputs: Box::from([property(Distribution::Singleton)]),
            output_properties: property(Distribution::Unconstrained),
            output: OutputPort {
                node: writer,
                columns: fields.iter().map(|field| field.value).collect(),
            },
            kind: NodeKind::TableWriter {
                target: WriterTarget {
                    handle: draft.payload().clone(),
                    write_target_ordinal: ordinal,
                    input: values.clone().into_boxed_slice(),
                    required_distribution: Distribution::Singleton,
                    target_fields: draft
                        .input()
                        .fields_iter()
                        .zip(&values)
                        .map(|(binding, value)| WriterTargetField {
                            provider_name: binding.field().name().clone().into(),
                            token: binding.token(),
                            input: *value,
                            ty: ValueType::new(
                                binding.field().data_type().clone(),
                                binding.field().is_nullable(),
                            ),
                            hidden: false,
                        })
                        .collect(),
                    output_schema: WriterRelationSchema {
                        revision: WRITER_MULTIPLEX_SCHEMA_REVISION,
                        fields: fields.clone(),
                    },
                    partial_aggregates: Box::default(),
                },
            },
        })
        .unwrap();
    let producer = builder
        .finish_definition(writer, FragmentSink::Stream { edge: EDGE }, dop_domain())
        .unwrap();

    let mut builder = FragmentBuilder::new(FINISH_FRAGMENT);
    let exchange = builder.reserve_node_id().unwrap();
    let imported = fields
        .iter()
        .map(|field| {
            let value = builder
                .add_value(
                    field.ty.clone(),
                    ValueOrigin::ExchangeImport {
                        edge: EDGE,
                        source_value: field.value,
                    },
                )
                .unwrap();
            WriterRelationField {
                value,
                name: field.name.clone(),
                ty: field.ty.clone(),
                role: field.role,
            }
        })
        .collect::<Box<[_]>>();
    builder
        .insert_node_unchecked(PhysicalNode {
            id: exchange,
            inputs: Box::default(),
            required_inputs: Box::default(),
            output_properties: property(Distribution::Singleton),
            output: OutputPort {
                node: exchange,
                columns: imported.iter().map(|field| field.value).collect(),
            },
            kind: NodeKind::ExchangeSource {
                edge: EDGE,
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
        .insert_node_unchecked(PhysicalNode {
            id: finish,
            inputs: Box::from([exchange]),
            required_inputs: Box::from([property(Distribution::Singleton)]),
            output_properties: property(Distribution::Singleton),
            output: OutputPort {
                node: finish,
                columns: outputs.iter().map(|field| field.value).collect(),
            },
            kind: NodeKind::TableFinish(WriterFinishSpec {
                expected_target_ordinals: Box::from([ordinal]),
                input_schema: WriterRelationSchema {
                    revision: WRITER_MULTIPLEX_SCHEMA_REVISION,
                    fields: imported.clone(),
                },
                output_schema: WriterRelationSchema {
                    revision: ROOT_WRITE_RESULT_SCHEMA_REVISION,
                    fields: outputs.clone(),
                },
                final_aggregates: Box::default(),
                grouped_unpivot: None,
            }),
        })
        .unwrap();
    let consumer = builder
        .finish_definition(finish, FragmentSink::Result, dop_domain())
        .unwrap();

    let mut plan = PlanBuilder::new(PlanVersionId::try_new([1; 16]).unwrap());
    plan.add_fragment(producer).unwrap();
    plan.add_fragment(consumer).unwrap();
    plan.add_edge(Edge {
        id: EDGE,
        kind: EdgeKind::Stream,
        source: EdgeSource {
            fragment: WRITER_FRAGMENT,
            projection: fields.iter().map(|field| field.value).collect(),
        },
        destination: EdgeDestination {
            fragment: FINISH_FRAGMENT,
            node: exchange,
            receive_mapping: fields
                .iter()
                .zip(&imported)
                .map(|(a, b)| (a.value, b.value))
                .collect(),
        },
        partitioning: EdgePartitioning {
            source: Distribution::Singleton,
            source_multiplicity: RowMultiplicity::SingleCopy,
            destination: Distribution::Singleton,
            destination_multiplicity: RowMultiplicity::SingleCopy,
        },
    })
    .unwrap();
    plan.set_result_port(ResultPort {
        fragment: FINISH_FRAGMENT,
        output: OutputPort {
            node: finish,
            columns: outputs.iter().map(|field| field.value).collect(),
        },
        fields: outputs
            .iter()
            .map(|field| ResultField {
                name: field.name.clone(),
                alias: None,
                value: field.value,
                ty: field.ty.clone(),
            })
            .collect(),
    })
    .unwrap();
    let plan = plan.finish().unwrap();

    let mut uses = BTreeMap::new();
    let mut calls = BTreeMap::new();
    let mut pruning = BTreeMap::new();
    let mut admissions = BTreeMap::new();
    for (&id, fragment) in plan.fragments() {
        let root_uses = root_uses(fragment);
        calls.insert(
            id,
            FrozenFragmentCalls::try_new(fragment, &root_uses, vec![], &FixtureControl).unwrap(),
        );
        uses.insert(id, root_uses);
        pruning.insert(
            id,
            FrozenFragmentPruning::try_new(id, vec![], &FixtureControl).unwrap(),
        );
        admissions.insert(id, package_admission());
    }
    let mut packages = extract_fragment_packages(
        &plan,
        &BTreeMap::new(),
        &BTreeMap::from([(ordinal, draft)]),
        &uses,
        &calls,
        &pruning,
        &admissions,
        &FixtureControl,
    )
    .unwrap();
    Fixture {
        writer: Arc::new(packages.remove(&WRITER_FRAGMENT).unwrap()),
        finish: Arc::new(packages.remove(&FINISH_FRAGMENT).unwrap()),
        writer_node: writer,
        finish_node: finish,
    }
}

/// One complete eager use per literal root site in one root domain.
fn root_uses(fragment: &Fragment) -> PhysicalRootUses {
    let roots = PhysicalExpressionRoots::try_new(fragment, &FixtureControl).unwrap();
    let mut uses = Vec::new();
    let bindings = roots
        .sites()
        .iter()
        .enumerate()
        .map(|(index, (&site, root))| {
            let id = ExpressionUseId::new(index as u32);
            assert!(matches!(
                fragment.expressions().get(root.expr).unwrap().kind,
                ExprKind::Literal(_)
            ));
            // A literal has no argument, so its control shape is trivially eager.
            let _ = control_argument_semantics(ControlShape::Eager, 0, 0, root.demand);
            uses.push(ExpressionInvocation {
                context: ExpressionEffectContext {
                    use_id: id,
                    domain: EvaluationDomainId::new(0),
                    demand: root.demand,
                },
                definition: root.expr,
                control: ControlShape::Eager,
                arguments: Box::default(),
            });
            (site, id)
        })
        .collect();
    let flow = ExpressionControlFlow::<ExprId>::try_new(
        vec![ExpressionEvaluationDomain {
            id: EvaluationDomainId::new(0),
            parent: None,
            guard: None,
        }],
        uses,
        fragment.expressions(),
        CompilePhase::Validate,
        &FixtureControl,
    )
    .unwrap();
    PhysicalRootUses::try_new(fragment, flow, bindings, &FixtureControl).unwrap()
}

// Conservative retained-source invoice and independent projection ceilings for
// these small fixtures only; this is not a production default or a MEM grant.
fn package_admission() -> FragmentPackageAdmission {
    FragmentPackageAdmission {
        plan_limits: PlanLimits::FROZEN,
        source_retained_bytes: 64 * 1024 * 1024,
        property_projection_limits: PropertyProofProjectionLimits {
            max_request_bytes: 16 * 1024 * 1024,
            max_coexisting_bytes: 256 * 1024 * 1024,
            max_projection_work: 16 * 1024 * 1024,
        },
    }
}

/// Keeps the draft unchanged; the public fields are borrowed as they are.
struct Port;
impl ConnectorWriteRecipeCompiler for Port {
    type Error = ConnectorError;
    fn compile_private(
        &self,
        draft: &ConnectorWriteRecipeDraft,
        _: &dyn PureCompileControl,
    ) -> Result<ConnectorWriteRecipeDraft, PureProviderCompileError<ConnectorError>> {
        Ok(draft.clone())
    }
}

fn providers() -> PureProviderProgramCatalog<ConnectorError> {
    let provider = ConnectorProviderId::parse("alpha").unwrap();
    PureProviderProgramCatalog::try_new(
        &[PureProviderManifestEntry::new(
            provider.clone(),
            false,
            true,
        )],
        vec![PureProviderProgramDefinition::new(
            provider,
            None,
            Some(Arc::new(Port)
                as Arc<
                    dyn ConnectorWriteRecipeCompiler<Error = ConnectorError>,
                >),
        )],
        &FixtureControl,
    )
    .unwrap()
}

// A real RAND-only sealed subset; the fixtures call no function.
fn functions() -> PureEngineFunctionCatalog {
    let actual =
        novarocks_functions::builtin::catalogue::build_builtin_engine_function_catalog().unwrap();
    let mut builder = EngineFunctionCatalogBuilder::new();
    builder
        .register(
            actual
                .definition("rand", FunctionKind::Scalar)
                .unwrap()
                .clone(),
        )
        .unwrap();
    builder
        .seal_pure(
            [
                "builtin.scalar/rand/()->f64;strict;legacy",
                "builtin.scalar/rand/(i64)->f64;strict;legacy",
            ]
            .into_iter()
            .map(|overload| InstalledPureKernel {
                function: FunctionId::try_new("builtin.scalar/rand/v1").unwrap(),
                kind: FunctionKind::Scalar,
                implementation: PureImplementationDeclaration {
                    overload: FunctionOverloadId::try_new(overload).unwrap(),
                    implementation: PureImplementationId::try_new(
                        "builtin.scalar/rand/selected-v1",
                    )
                    .unwrap(),
                    abi: PureKernelAbi::ScalarV1,
                },
                aggregate_state_format: None,
            }),
        )
        .unwrap()
}

fn options(pipeline_dop: usize, root_sink_dop: Option<usize>) -> LocalCompileOptions {
    LocalCompileOptions {
        pipeline_dop: NonZeroUsize::new(pipeline_dop).unwrap(),
        root_sink_dop: root_sink_dop.map(|dop| NonZeroUsize::new(dop).unwrap()),
        kernel_abi: KernelAbiVersion::CURRENT,
        exchange_wait: Duration::from_millis(1_000),
        // Explicit fixture admission; these values are not production defaults.
        constants: ConstantPolicy {
            max_rows: 16,
            max_array_nodes: 128,
            max_logical_elements: 1024,
            max_retained_buffer_bytes: 1 << 20,
            max_type_depth: 64,
            max_type_nodes: 4096,
            max_dictionary_depth: 64,
            max_metadata_bytes: 1 << 20,
            max_library_validation_work: 1 << 20,
            max_library_validation_bytes: 1 << 20,
        },
    }
}

fn compile(
    package: &Arc<FragmentPackage>,
    pipeline_dop: usize,
    root_sink_dop: Option<usize>,
) -> Result<LocalProgram, FragmentCompileError> {
    let validated =
        validate_fragment_providers(Arc::clone(package), &providers(), &FixtureControl).unwrap();
    compile_fragment(
        validated,
        &functions(),
        options(pipeline_dop, root_sink_dop),
        &FixtureControl,
    )
}

fn names(layout: &novarocks_local_program::StaticLayout) -> Vec<String> {
    layout
        .schema()
        .fields()
        .iter()
        .map(|field| field.name().clone())
        .collect()
}

const WRITER_RELATION: [&str; 4] = [
    "kind",
    "write_target_ordinal",
    "row_count",
    "commit_fragment",
];

#[test]
fn writer_fragment_compiles_a_positional_relation_over_synthetic_slot_reads() {
    let fixture = fixture();
    for dop in [1, 4] {
        let program = compile(&fixture.writer, dop, None).unwrap();
        let graph = program.graph();
        let root = graph.root();
        let ProgramNodeKind::TableWriter {
            input,
            target,
            expected_layout,
            projection,
            writer_multiplex_layout,
            partial_aggregates,
        } = graph.nodes()[root.index()].kind()
        else {
            panic!("the writer fragment roots at its TableWriter");
        };
        assert!(partial_aggregates.is_empty());
        assert_eq!(target.get(), 0);
        assert!(matches!(
            graph.nodes()[input.index()].kind(),
            ProgramNodeKind::Values { .. }
        ));
        // The projection is the provider's own fields, with the value's
        // nullability: `c1` is fed a non-null value and stays NOT NULL, `c2`
        // is nullable in the provider but fed a non-null value.
        let fields = projection.layout.schema().fields();
        assert_eq!(fields.len(), 2);
        assert_eq!(fields[0].as_ref(), &target_fields()[0]);
        assert_eq!(
            fields[1].as_ref(),
            &target_fields()[1].clone().with_nullable(false)
        );
        assert_eq!(expected_layout.schema(), projection.layout.schema());
        assert_eq!(expected_layout.slots(), projection.layout.slots());
        // Each projected field is a synthetic slot read of its input
        // occurrence, never a slot the projection itself produces.
        let input_slots = graph.nodes()[input.index()].output_layout().slots();
        for (ordinal, expr) in projection.expressions.iter().enumerate() {
            match projection.arena.node(*expr).unwrap().kind() {
                StaticExprKind::SlotId(slot) => assert_eq!(*slot, input_slots[ordinal]),
                other => panic!("writer projection {ordinal} is {other:?}"),
            }
            assert!(!projection.layout.slots().contains(&input_slots[ordinal]));
        }
        // The multiplex relation is named by its frozen fields with fresh slots.
        assert_eq!(names(writer_multiplex_layout), WRITER_RELATION);
        assert_eq!(
            graph.nodes()[root.index()].output_layout().slots(),
            writer_multiplex_layout.slots()
        );
        // The recipe moved into the writer, which is bound as a TableWriter.
        assert_eq!(program.write_recipes().len(), 1);
        assert_eq!(
            program.write_recipes()[&root].draft().input().field_count(),
            2
        );
        assert!(
            graph
                .requirements()
                .entries()
                .iter()
                .any(|requirement| matches!(
                    requirement,
                    BindingRequirement::TableWriter { node, .. } if *node == root
                ))
        );
        assert!(matches!(
            graph.sink(),
            Some(StaticSinkProgram::DataStream { .. })
        ));
        // Every projected read is bound to the writer input's occurrence.
        let channels = program.checked().channels();
        for ordinal in 0..2u32 {
            let site = ProgramExpressionRootSite::WriterProjection {
                node: root,
                expression: ordinal,
            };
            assert_eq!(
                site.arena(),
                novarocks_local_program::ProgramExpressionArena::WriterProjection(root)
            );
            assert!(
                channels
                    .channel_type(ProgramChannelSite::Layout {
                        node: root,
                        role: ProgramChannelLayoutRole::WriterProjection,
                        ordinal,
                    })
                    .is_some()
            );
        }
        let bound = program
            .checked()
            .slots()
            .iter()
            .filter(|(occurrence, _)| {
                occurrence.arena
                    == novarocks_local_program::ProgramExpressionArena::WriterProjection(root)
            })
            .map(|(_, source)| source.clone())
            .collect::<Vec<_>>();
        assert_eq!(
            bound,
            (0..2u32)
                .map(
                    |ordinal| ProgramLexicalSource::Input(ProgramChannelSite::Layout {
                        node: *input,
                        role: ProgramChannelLayoutRole::NodeOutput,
                        ordinal,
                    })
                )
                .collect::<Vec<_>>()
        );
    }
}

#[test]
fn finish_fragment_reads_the_writer_relation_and_publishes_the_root_relation() {
    let fixture = fixture();
    let program = compile(&fixture.finish, 2, Some(1)).unwrap();
    let graph = program.graph();
    let root = graph.root();
    let ProgramNodeKind::TableFinish {
        inputs,
        expected_targets,
        writer_multiplex_layout,
        root_result_layout,
        final_aggregates,
    } = graph.nodes()[root.index()].kind()
    else {
        panic!("the finish fragment roots at its TableFinish");
    };
    assert_eq!(expected_targets.len(), 1);
    assert!(final_aggregates.calls.is_empty() && final_aggregates.unpivot.is_none());
    let [input] = inputs.as_slice() else {
        panic!("one writer-result receiver");
    };
    let receiver = &graph.nodes()[input.index()];
    assert!(matches!(
        receiver.kind(),
        ProgramNodeKind::ExchangeSource { .. }
    ));
    // The receiver is the writer relation by name; the finish reads it as is.
    assert_eq!(names(receiver.output_layout()), WRITER_RELATION);
    assert_eq!(
        writer_multiplex_layout.slots(),
        receiver.output_layout().slots()
    );
    assert_eq!(
        writer_multiplex_layout.schema(),
        receiver.output_layout().schema()
    );
    // The Root relation keeps its exact nested child names.
    assert_eq!(
        names(root_result_layout),
        [
            "kind",
            "write_target_ordinal",
            "row_count",
            "commit_fragment",
            "input_fields",
            "blob_type",
            "body",
            "properties"
        ]
    );
    let properties = root_result_layout.schema().field(7);
    let DataType::Map(entries, false) = properties.data_type() else {
        panic!("properties is a map");
    };
    assert_eq!(entries.name(), "entries");
    let DataType::List(item) = root_result_layout.schema().field(4).data_type() else {
        panic!("input_fields is a list");
    };
    assert_eq!(item.name(), "item");
    assert!(program.write_recipes().is_empty());
    assert!(
        graph
            .requirements()
            .entries()
            .iter()
            .any(|requirement| matches!(
                requirement,
                BindingRequirement::TableFinish { node, .. } if *node == root
            ))
    );
    assert!(matches!(graph.sink(), Some(StaticSinkProgram::Result)));
    assert_eq!(
        program.exchange_inputs()[input].receiver_node,
        fixture.finish.fragment().nodes()[&fixture.finish_node].inputs[0].get()
    );
}

fn unsupported(error: FragmentCompileError) -> &'static str {
    match error {
        FragmentCompileError::Unsupported { feature, .. } => feature,
        other => panic!("expected an explicit refusal, got {other:?}"),
    }
}

/// A writer with partial aggregates, or a finish with final aggregates or a
/// grouped Unpivot, carries statistics, which are refused by name.
#[test]
fn writer_statistics_are_refused_explicitly_until_they_compile() {
    let fixture = fixture();
    let mut control =
        CompileCheckpoints::try_new(&FixtureControl, CompilePhase::LowerProgram).unwrap();
    let dop = NonZeroUsize::new(1).unwrap();
    let recipe =
        validate_fragment_providers(Arc::clone(&fixture.writer), &providers(), &FixtureControl)
            .unwrap();
    let recipe = recipe.writes().get(&fixture.writer_node).cloned();

    let mut writer = fixture.writer.fragment().nodes()[&fixture.writer_node].clone();
    let NodeKind::TableWriter { target } = &mut writer.kind else {
        unreachable!()
    };
    target.partial_aggregates = Box::from([WriterAggregateCall {
        input: target.input[0],
        binding: count_binding(AggregatePhase::Single),
        output: target.output_schema.fields[0].value,
    }]);
    let error = admit_writer_family(&fixture.writer, &writer, recipe.as_ref(), dop, &mut control)
        .unwrap_err();
    assert!(unsupported(error).contains("writer statistics"));

    let mut finish = fixture.finish.fragment().nodes()[&fixture.finish_node].clone();
    let NodeKind::TableFinish(spec) = &mut finish.kind else {
        unreachable!()
    };
    spec.grouped_unpivot = Some(WriterGroupedUnpivotSpec {
        statistics_target_ordinals: spec.expected_target_ordinals.clone(),
        grouping_input: spec.input_schema.fields[1].value,
        grouping_output: spec.output_schema.fields[1].value,
        passthrough_output: spec.output_schema.fields[0].value,
        value_output: spec.output_schema.fields[6].value,
        literal_outputs: Box::default(),
        mappings: Box::default(),
        max_output_rows: 1,
        max_output_bytes: 1,
    });
    let error = admit_writer_family(&fixture.finish, &finish, None, dop, &mut control).unwrap_err();
    assert!(unsupported(error).contains("writer statistics"));
    control.finish().unwrap();
}

/// A partitioned writer input is co-located per instance only, so without a
/// compiled local shuffle it runs on one driver.
#[test]
fn a_partitioned_writer_input_is_refused_at_more_than_one_driver() {
    let fixture = fixture();
    let mut control =
        CompileCheckpoints::try_new(&FixtureControl, CompilePhase::LowerProgram).unwrap();
    let recipe =
        validate_fragment_providers(Arc::clone(&fixture.writer), &providers(), &FixtureControl)
            .unwrap();
    let recipe = recipe.writes().get(&fixture.writer_node).cloned();
    let mut writer = fixture.writer.fragment().nodes()[&fixture.writer_node].clone();
    let NodeKind::TableWriter { target } = &mut writer.kind else {
        unreachable!()
    };
    target.required_distribution = Distribution::Hash {
        keys: Box::from([target.input[0]]),
        scheme: HashPartitionScheme {
            space: PartitionSpaceId::try_new([41; 32]).unwrap(),
            count: PartitionCountParameter {
                id: PartitionCountParameterId::try_new([42; 32]).unwrap(),
                admissible: PartitionCountDomain {
                    min: 1,
                    max: 64,
                    requires_power_of_two: true,
                },
            },
            definition: HashDefinition::native_exchange(),
        },
    };
    admit_writer_family(
        &fixture.writer,
        &writer,
        recipe.as_ref(),
        NonZeroUsize::new(1).unwrap(),
        &mut control,
    )
    .unwrap();
    let error = admit_writer_family(
        &fixture.writer,
        &writer,
        recipe.as_ref(),
        NonZeroUsize::new(2).unwrap(),
        &mut control,
    )
    .unwrap_err();
    assert_eq!(
        unsupported(error),
        "partitioned writer input at pipeline DOP > 1"
    );
    control.finish().unwrap();
}

/// Neither family runs below its fragment root: one writer per fragment, and
/// a finish never shares a fragment with a writer.
#[test]
fn a_writer_family_node_below_its_fragment_root_is_refused() {
    let fixture = fixture();
    let mut control =
        CompileCheckpoints::try_new(&FixtureControl, CompilePhase::LowerProgram).unwrap();
    let recipe =
        validate_fragment_providers(Arc::clone(&fixture.writer), &providers(), &FixtureControl)
            .unwrap();
    let recipe = recipe.writes().get(&fixture.writer_node).cloned();
    let mut writer = fixture.writer.fragment().nodes()[&fixture.writer_node].clone();
    writer.id = NodeId::new(fixture.writer_node.get() + 100);
    let error = admit_writer_family(
        &fixture.writer,
        &writer,
        recipe.as_ref(),
        NonZeroUsize::new(1).unwrap(),
        &mut control,
    )
    .unwrap_err();
    assert!(unsupported(error).contains("one writer per fragment"));
    control.finish().unwrap();
}

fn count_binding(phase: AggregatePhase) -> AggregateBinding {
    AggregateBinding {
        state_argument_contract:
            novarocks_type_contract::AggregateStateArgumentContract::ExactSignature,
        function: BoundFunction {
            legacy_metadata: None,
            function_id: FunctionId::try_new("builtin/test_count/v1").unwrap(),
            overload: FunctionOverloadId::try_new("i64-state").unwrap(),
            kind: FunctionKind::Aggregate,
            argument_types: Box::from([novarocks_type_contract::FunctionArgumentType::Value(ty(
                DataType::Int64,
                false,
            ))]),
            result_type: ty(DataType::Int64, false),
        },
        phase,
        logical_argument_count: 1,
        intermediate_type: ty(DataType::Binary, false),
        state_format: AggregateStateFormatId::try_new("test_count/state-v1").unwrap(),
    }
}
