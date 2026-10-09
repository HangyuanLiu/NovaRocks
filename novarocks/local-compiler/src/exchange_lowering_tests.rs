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

//! Two-fragment exchange plans built through the real physical builders and
//! package extraction: a Values producer streaming over one edge and an
//! ExchangeSource consumer. Both sides compile independently.

use crate::{FragmentCompileError, LocalCompileOptions, compile_fragment, validate_fragment_providers};
use arrow_schema::DataType;
use novarocks_connector_contract::PureProviderProgramCatalog;
use novarocks_functions::{
    ConstantPolicy, EngineFunctionCatalogBuilder, FunctionId, FunctionKind, FunctionOverloadId,
    InstalledPureKernel, PureEngineFunctionCatalog, PureImplementationDeclaration,
    PureImplementationId, PureKernelAbi,
};
use novarocks_local_program::{
    BindingRequirement, CompiledExchangeInput, DataStreamPartitionType, KernelAbiVersion,
    LocalProgram, ProgramChannelLayoutRole, ProgramChannelSite, ProgramExprId,
    ProgramExpressionArena, ProgramExpressionRootSite, ProgramLexicalSource, ProgramNodeId,
    ProgramNodeKind, ProgramUseRef, StaticExprKind, StaticSinkProgram,
};
use novarocks_physical_plan::{
    Distribution, Edge, EdgeDestination, EdgeId, EdgeKind, EdgePartitioning, EdgeSource, ExprId,
    ExprKind, Fragment, FragmentBuilder, FragmentId, FragmentPackage, FragmentPackageAdmission,
    FragmentSink, FrozenFragmentCalls, FrozenFragmentPruning, HashDefinition, HashPartitionScheme,
    LiteralValue, NodeId, OutputPort, PartitionCountDomain, PartitionCountParameter,
    PhysicalExpressionRoots, PhysicalRootUses, PipelineDopDomain, PlanBuilder, PlanLimits,
    PlanVersionId, PropertyProofProjectionLimits, ResultField, ResultPort, RowMultiplicity,
    ValueOrigin, ValueType, extract_fragment_packages,
};
use novarocks_type_contract::{
    CompileControlError, CompilePhase, ControlShape, EvaluationDomainId, ExpressionControlFlow,
    ExpressionEffectContext, ExpressionEvaluationDomain, ExpressionInvocation, ExpressionUseId,
    PartitionCountParameterId, PartitionSpaceId, PureCompileControl,
};
use std::{collections::BTreeMap, num::NonZeroUsize, sync::Arc, time::Duration};

const PRODUCER: FragmentId = FragmentId::new(1);
const CONSUMER: FragmentId = FragmentId::new(2);
const EDGE: EdgeId = EdgeId::new(5);
const VALUES: NodeId = NodeId::new(10);
const RECEIVER: NodeId = NodeId::new(20);
const WAIT: Duration = Duration::from_millis(4_321);

struct FixtureControl;
impl PureCompileControl for FixtureControl {
    fn checkpoint(&self, _: CompilePhase, _: u32) -> Result<(), CompileControlError> {
        Ok(())
    }
}

#[derive(Clone, Copy, PartialEq)]
enum Shape {
    /// Stream edge, Singleton to Singleton, consumer publishes the result.
    Gather,
    /// Stream edge hashed on both produced columns; consumer is a Noop sink.
    Hash,
    /// Stream edge without a partitioning contract; consumer is a Noop sink.
    Unconstrained,
    /// CTE multicast producer; consumer is a Noop sink.
    Multicast,
}

fn hash(keys: [novarocks_physical_plan::ValueId; 2]) -> Distribution {
    Distribution::Hash {
        keys: Box::from(keys),
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
    }
}

fn dop() -> PipelineDopDomain {
    PipelineDopDomain {
        min: 1,
        max: 1,
        requires_power_of_two: false,
    }
}

/// Producer `Values(a = 11, b = 22)` streams the reversed projection `[b, a]`;
/// the consumer receives `[ib, ia]` positionally at `RECEIVER`.
fn packages(shape: Shape) -> BTreeMap<FragmentId, FragmentPackage> {
    let ty = ValueType::new(DataType::Int64, false);
    let mut producer = FragmentBuilder::new(PRODUCER);
    let mut outputs = Vec::new();
    let mut cells = Vec::new();
    for (ordinal, literal) in [11, 22].into_iter().enumerate() {
        outputs.push(
            producer
                .add_value(
                    ty.clone(),
                    ValueOrigin::NodeOutput {
                        node: VALUES,
                        output_ordinal: ordinal as u32,
                    },
                )
                .unwrap(),
        );
        cells.push(
            producer
                .add_expression(
                    VALUES,
                    ty.clone(),
                    ExprKind::Literal(LiteralValue::Int64(literal)),
                )
                .unwrap(),
        );
    }
    let (a, b) = (outputs[0], outputs[1]);
    producer
        .add_values(
            VALUES,
            Box::from([cells.into_boxed_slice()]),
            outputs.into_boxed_slice(),
        )
        .unwrap();
    let multicast = shape == Shape::Multicast;
    let sink = if multicast {
        FragmentSink::Multicast {
            edges: Box::from([EDGE]),
        }
    } else {
        FragmentSink::Stream { edge: EDGE }
    };
    let producer = producer.finish_definition(VALUES, sink, dop()).unwrap();

    let mut consumer = FragmentBuilder::new(CONSUMER);
    let import = |builder: &mut FragmentBuilder, source_value| {
        let origin = if multicast {
            ValueOrigin::CteImport {
                edge: EDGE,
                producer_fragment: PRODUCER,
                producer_value: source_value,
            }
        } else {
            ValueOrigin::ExchangeImport {
                edge: EDGE,
                source_value,
            }
        };
        builder.add_value(ty.clone(), origin).unwrap()
    };
    let ib = import(&mut consumer, b);
    let ia = import(&mut consumer, a);
    let (source, destination) = match shape {
        Shape::Gather => (Distribution::Singleton, Distribution::Singleton),
        Shape::Hash => (hash([b, a]), hash([ib, ia])),
        Shape::Unconstrained | Shape::Multicast => {
            (Distribution::Unconstrained, Distribution::Unconstrained)
        }
    };
    consumer
        .add_exchange_source(
            RECEIVER,
            EDGE,
            Box::from([(b, ib), (a, ia)]),
            Box::from([ib, ia]),
            destination.clone(),
            RowMultiplicity::SingleCopy,
        )
        .unwrap();
    let gather = shape == Shape::Gather;
    let consumer_sink = if gather {
        FragmentSink::Result
    } else {
        FragmentSink::Noop
    };
    let consumer = consumer
        .finish_definition(RECEIVER, consumer_sink, dop())
        .unwrap();

    let mut plan = PlanBuilder::new(PlanVersionId::try_new([7; 16]).unwrap());
    plan.add_fragment(producer).unwrap();
    plan.add_fragment(consumer).unwrap();
    plan.add_edge(Edge {
        id: EDGE,
        kind: if multicast {
            EdgeKind::CteMulticast
        } else {
            EdgeKind::Stream
        },
        source: EdgeSource {
            fragment: PRODUCER,
            projection: Box::from([b, a]),
        },
        destination: EdgeDestination {
            fragment: CONSUMER,
            node: RECEIVER,
            receive_mapping: Box::from([(b, ib), (a, ia)]),
        },
        partitioning: EdgePartitioning {
            source,
            source_multiplicity: RowMultiplicity::SingleCopy,
            destination,
            destination_multiplicity: RowMultiplicity::SingleCopy,
        },
    })
    .unwrap();
    if gather {
        plan.set_result_port(ResultPort {
            fragment: CONSUMER,
            output: OutputPort {
                node: RECEIVER,
                columns: Box::from([ib, ia]),
            },
            fields: Box::from([
                ResultField {
                    name: "b".into(),
                    alias: Some("bee".into()),
                    value: ib,
                    ty: ty.clone(),
                },
                ResultField {
                    name: "a".into(),
                    alias: None,
                    value: ia,
                    ty: ty.clone(),
                },
            ]),
        })
        .unwrap();
    }
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
    extract_fragment_packages(
        &plan,
        &BTreeMap::new(),
        &BTreeMap::new(),
        &uses,
        &calls,
        &pruning,
        &admissions,
        &FixtureControl,
    )
    .unwrap()
}

/// One eager root occurrence per physical root site in one root domain.
fn root_uses(fragment: &Fragment) -> PhysicalRootUses {
    let roots = PhysicalExpressionRoots::try_new(fragment, &FixtureControl).unwrap();
    let domain = EvaluationDomainId::new(0);
    let mut invocations = Vec::new();
    let bindings = roots
        .sites()
        .iter()
        .enumerate()
        .map(|(ordinal, (&site, root))| {
            let use_id = ExpressionUseId::new(ordinal as u32);
            invocations.push(ExpressionInvocation {
                context: ExpressionEffectContext {
                    use_id,
                    domain,
                    demand: root.demand,
                },
                definition: root.expr,
                control: ControlShape::Eager,
                arguments: Box::default(),
            });
            (site, use_id)
        })
        .collect();
    let flow = ExpressionControlFlow::<ExprId>::try_new(
        vec![ExpressionEvaluationDomain {
            id: domain,
            parent: None,
            guard: None,
        }],
        invocations,
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

fn options(root_sink_dop: Option<usize>) -> LocalCompileOptions {
    LocalCompileOptions {
        pipeline_dop: NonZeroUsize::new(1).unwrap(),
        root_sink_dop: root_sink_dop.map(|dop| NonZeroUsize::new(dop).unwrap()),
        kernel_abi: KernelAbiVersion::CURRENT,
        exchange_wait: WAIT,
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
    package: FragmentPackage,
    root_sink_dop: Option<usize>,
) -> Result<LocalProgram, FragmentCompileError> {
    let providers =
        PureProviderProgramCatalog::<std::io::Error>::try_new(&[], vec![], &FixtureControl)
            .unwrap();
    let validated =
        validate_fragment_providers(Arc::new(package), &providers, &FixtureControl).unwrap();
    compile_fragment(
        validated,
        &functions(),
        options(root_sink_dop),
        &FixtureControl,
    )
}

fn refused(result: Result<LocalProgram, FragmentCompileError>, expected: &str) {
    match result {
        Err(FragmentCompileError::Unsupported { feature, .. }) => assert_eq!(feature, expected),
        other => panic!("expected refusal {expected:?}, got {other:?}"),
    }
}

#[test]
fn gather_producer_streams_and_consumer_receives_one_addressed_edge() {
    let mut packages = packages(Shape::Gather);
    let producer = packages.remove(&PRODUCER).unwrap();
    // A stream producer has no result port and borrows no result labels.
    assert!(producer.result().is_none());
    let producer = compile(producer, None).unwrap();
    let root_slots = producer.graph().nodes()[0].output_layout().slots().to_vec();
    let Some(StaticSinkProgram::DataStream { branch, arena }) = producer.graph().sink() else {
        panic!("stream sink");
    };
    assert_eq!(branch.dest_node_id(), RECEIVER.get() as i32);
    assert_eq!(
        branch.partition_type(),
        DataStreamPartitionType::Unpartitioned
    );
    assert!(branch.partition_exprs().is_empty());
    // The reversed cut projection `[b, a]` names root slots in that order.
    assert_eq!(branch.output_columns(), &[root_slots[1], root_slots[0]]);
    assert!(branch.limit().is_none());
    assert!(arena.nodes().is_empty());
    let requirements = producer.graph().requirements().entries();
    assert_eq!(requirements.len(), 1);
    assert!(matches!(
        &requirements[0],
        BindingRequirement::ExchangeOutput { branch: 0, layout }
            if layout.slots() == [root_slots[1], root_slots[0]]
    ));
    // The empty Sink arena still owns its (empty) flow.
    let snapshot = producer
        .checked()
        .channels()
        .expressions()
        .resolved_calls()
        .snapshot();
    assert!(
        snapshot.flows()[&ProgramExpressionArena::Sink]
            .uses()
            .is_empty()
    );
    assert!(producer.exchange_inputs().is_empty());

    let consumer = compile(packages.remove(&CONSUMER).unwrap(), Some(1)).unwrap();
    let receiver = &consumer.graph().nodes()[0];
    let ProgramNodeKind::ExchangeSource {
        timeout,
        runtime_filters,
        hash_partition_exprs,
    } = receiver.kind()
    else {
        panic!("ExchangeSource");
    };
    assert_eq!(*timeout, WAIT);
    assert!(runtime_filters.is_empty() && hash_partition_exprs.is_empty());
    let layout = receiver.output_layout();
    let names = layout
        .schema()
        .fields()
        .iter()
        .map(|field| field.name().as_str())
        .collect::<Vec<_>>();
    assert_eq!(names, ["bee", "a"]);
    assert_eq!(
        consumer.exchange_inputs(),
        &BTreeMap::from([(
            ProgramNodeId::new(0),
            CompiledExchangeInput {
                receiver_node: RECEIVER.get(),
                edge: EDGE.get(),
                source_fragment: PRODUCER.get(),
                hash_partition_slots: Box::default(),
            },
        )])
    );
    let requirements = consumer.graph().requirements().entries();
    assert_eq!(requirements.len(), 2);
    assert!(requirements.iter().any(|requirement| matches!(
        requirement,
        BindingRequirement::ExchangeInput { node, layout: required }
            if *node == ProgramNodeId::new(0) && required.slots() == layout.slots()
    )));
    assert!(
        requirements
            .iter()
            .any(|requirement| matches!(requirement, BindingRequirement::ResultSink { .. }))
    );
    assert!(matches!(
        consumer.graph().sink(),
        Some(StaticSinkProgram::Result)
    ));
}

#[test]
fn hash_producer_authors_one_sink_partition_root_per_key() {
    let producer = compile(packages(Shape::Hash).remove(&PRODUCER).unwrap(), None).unwrap();
    let root_slots = producer.graph().nodes()[0].output_layout().slots().to_vec();
    let Some(StaticSinkProgram::DataStream { branch, arena }) = producer.graph().sink() else {
        panic!("stream sink");
    };
    assert_eq!(
        branch.partition_type(),
        DataStreamPartitionType::HashPartitioned
    );
    assert_eq!(
        branch.partition_exprs(),
        &[ProgramExprId::new(0), ProgramExprId::new(1)]
    );
    // Keys `[b, a]` read the root occurrences 1 and 0 in a separate arena.
    for (key, ordinal) in [(0, 1u32), (1, 0u32)] {
        assert!(matches!(
            arena.nodes()[key].kind(),
            StaticExprKind::SlotId(slot) if *slot == root_slots[ordinal as usize]
        ));
    }
    assert!(!Arc::ptr_eq(arena, producer.graph().expressions()));
    let checked = producer.checked();
    let snapshot = checked.channels().expressions().resolved_calls().snapshot();
    let sites = snapshot
        .bindings()
        .iter()
        .filter(|(site, _)| matches!(site, ProgramExpressionRootSite::SinkPartition { .. }))
        .map(|(site, use_id)| (*site, *use_id))
        .collect::<Vec<_>>();
    assert_eq!(sites.len(), 2);
    assert_eq!(
        snapshot.flows()[&ProgramExpressionArena::Sink].uses().len(),
        2
    );
    for (key, (site, use_id)) in sites.into_iter().enumerate() {
        assert_eq!(
            site,
            ProgramExpressionRootSite::SinkPartition {
                branch: 0,
                key: key as u32,
            }
        );
        let expected = ProgramLexicalSource::Input(ProgramChannelSite::Layout {
            node: ProgramNodeId::new(0),
            role: ProgramChannelLayoutRole::NodeOutput,
            ordinal: if key == 0 { 1 } else { 0 },
        });
        assert_eq!(
            checked.slots()[&ProgramUseRef {
                arena: ProgramExpressionArena::Sink,
                use_id,
            }],
            expected
        );
    }
}

#[test]
fn unsupported_sinks_and_partitionings_are_refused() {
    refused(
        compile(packages(Shape::Multicast).remove(&PRODUCER).unwrap(), None),
        "multicast, router or noop sink",
    );
    let mut unconstrained = packages(Shape::Unconstrained);
    refused(
        compile(unconstrained.remove(&PRODUCER).unwrap(), None),
        "unconstrained or round-robin stream partitioning",
    );
    refused(
        compile(unconstrained.remove(&CONSUMER).unwrap(), None),
        "multicast, router or noop sink",
    );
    // A hash receiver is not a singleton source tree yet.
    let hash_consumer = packages(Shape::Hash).remove(&CONSUMER).unwrap();
    assert!(matches!(
        compile(hash_consumer, None),
        Err(FragmentCompileError::Unsupported { .. })
    ));
}

#[test]
fn result_sink_without_result_port_never_reaches_lowering() {
    // The checked package owns sink/result-port correspondence; the compiler's
    // own `missing result port` refusal is defense behind that boundary.
    let mut input = packages(Shape::Gather)
        .remove(&CONSUMER)
        .unwrap()
        .into_input();
    assert!(input.result.take().is_some());
    assert!(FragmentPackage::try_new(input, package_admission(), &FixtureControl).is_err());
}
