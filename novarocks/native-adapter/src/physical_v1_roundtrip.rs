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
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

use arrow::array::{Array, Int64Array};
use arrow::datatypes::DataType;
use novarocks_execution::exec::chunk::Chunk;
use novarocks_execution::exec::expr::ExprArena;
use novarocks_execution::exec::expr::agg::{
    ExecutionFunctionSetBuilder, contribute_builtin_aggregate_implementations,
};
use novarocks_execution::exec::node::{ExecNodeKind, ExecPlanBuilder};
use novarocks_execution::exec::pipeline::binding::{ExchangeBindings, ScanBindings};
use novarocks_execution::exec::pipeline::executor::execute_native_plan_with_pipeline;
use novarocks_execution::runtime::fragment::io::{
    ExchangeFrame, ExchangeFrameTransmitter, ExchangeTransmitRejection, FragmentIoError,
    FragmentResultSession, FragmentResultWriter, NoopFragmentEventSink, ResultAbort,
    ResultWriteCredit, ResultWriteSpec, RootInputAdmission, RootInputAuthority, RootInputPermit,
    RootProducerState, RootResultSession, RootResultWriteSpec,
};
use novarocks_execution::runtime::runtime_state::RuntimeState;
use novarocks_execution::runtime::{ExecutionRuntime, ExecutionRuntimeConfig};
use novarocks_physical_plan::{
    ChangeEventSpec, ChangeStreamRoute, Distribution, Edge, EdgeDestination, EdgeId, EdgeKind,
    EdgePartitioning, EdgeSource, ExprKind, FragmentBuilder, FragmentId, FragmentSink,
    JoinDistribution, JoinKey, JoinKind, JoinSide, NestLoopJoinDistribution, NodeKind, OutputPort,
    PhysicalNode, PhysicalPlan, PhysicalProperties, PipelineDopDomain, PlanBuilder, PlanVersionId,
    ROOT_WRITE_RESULT_SCHEMA_REVISION, ResultField, ResultPort, RowMultiplicity, SetOperationKind,
    ValueId, ValueOrigin, ValueType, WRITER_MULTIPLEX_SCHEMA_REVISION, WriterDerivedKind,
    WriterFinishSpec, WriterRelationField, WriterRelationFieldRole, WriterRelationSchema,
    WriterTarget, WriterTargetField,
};
use novarocks_plan_codec::{PhysicalV1PrivateFacts, PhysicalV1WriteFact};
use novarocks_proto_models::{connector_write, plan};
use novarocks_spi::connector::write_stack::WriteTargetOrdinal;
use novarocks_spi::connector::{
    CatalogHandle, CatalogVersion, ConnectorCodecCategory, ConnectorCodecRevision,
    ConnectorEncodedPayload, ConnectorEnvelopeHeader, ConnectorInstanceId, ConnectorProviderId,
    ConnectorRowMutationEffect, ConnectorWriteFieldToken, ConnectorWriteRouteId,
};
use novarocks_types::SlotId;

use crate::fragment_decode_context::NativePlanDecodeContext;
/// Exercise the same complete namespace and original-source decoders as task
/// ingress with an actual BE tracker. Returned nodes/fields retain only their
/// independently constructed immutable responsibilities.
fn with_fragment_types<T>(
    source: &plan::PlanFragment,
    operation: impl FnOnce(
        &mut crate::fragment_type_decode::NativeTypeDecoder<'_, '_, '_>,
    ) -> Result<T, crate::fragment_error::NativeFragmentDecodeError>,
) -> Result<T, crate::fragment_error::NativeFragmentDecodeError> {
    let tracker =
        novarocks_execution::runtime::mem_tracker::MemTracker::new_root("native roundtrip fixture");
    let creation = crate::native_type_construction::NativeTypeConstruction::try_with_control(
        tracker,
        compile_control(),
    )?;
    let catalog = novarocks_sql::compiler::build_builtin_engine_function_catalog().unwrap();
    let mut resources = crate::fragment_type_decode::NativeDecodeResources::try_new(&creation)?;
    resources.bind_function_catalog(&catalog);
    let types = crate::fragment_type_decode::decode_fragment_type_owner(source, &mut resources)?;
    let original =
        crate::fragment_original_sources::decode_original_requests(source, &types, &mut resources)?;
    let mut decoder = crate::fragment_type_decode::NativeTypeDecoder::Strong {
        types: &types.execution,
        original_requests: Some(original.as_ref()),
        resources: &mut resources,
    };
    let result = operation(&mut decoder)?;
    drop(decoder);
    resources.finish()?;
    Ok(result)
}

fn decode_node(
    source: &plan::PlanFragment,
    node: &plan::DistributedNode,
    arena: &mut ExprArena,
    ctx: &NativePlanDecodeContext,
) -> Result<
    crate::fragment_plan_decode::DecodedNode,
    crate::fragment_error::NativeFragmentDecodeError,
> {
    with_fragment_types(source, |decoder| {
        crate::fragment_plan_decode::decode_node_with_types(node, arena, ctx, decoder)
    })
}

fn decode_typed_sink_program(
    source: &plan::PlanFragment,
    layout: &novarocks_execution::exec::chunk::SlotLayout,
) -> Result<
    novarocks_execution::exec::fragment::sink::FragmentSinkProgram,
    crate::fragment_error::NativeFragmentDecodeError,
> {
    with_fragment_types(source, |decoder| {
        crate::fragment_sink::decode_fragment_sink_program(source, layout, decoder)
    })
}

fn compile_control() -> &'static dyn novarocks_type_contract::PureCompileControl {
    struct Control;
    impl novarocks_type_contract::PureCompileControl for Control {
        fn checkpoint(
            &self,
            _phase: novarocks_type_contract::CompilePhase,
            _work: u32,
        ) -> Result<(), novarocks_type_contract::CompileControlError> {
            Ok(())
        }
    }
    static CONTROL: Control = Control;
    &CONTROL
}

fn singleton_properties() -> PhysicalProperties {
    PhysicalProperties {
        distribution: Distribution::Singleton,
        row_multiplicity: RowMultiplicity::SingleCopy,
        ordering: Box::default(),
    }
}

/// A fixture's Int64 literal as a checked constant: v1 encoding refuses legacy
/// literals. Each value has its own one-row pool, keyed by the value.
fn i64_constant(value: i64) -> ExprKind {
    ExprKind::Constant(novarocks_physical_plan::ConstantReference {
        pool: fixture_pool(value),
        ordinal: 0,
    })
}

fn fixture_pool(value: i64) -> novarocks_physical_plan::ConstantPoolId {
    novarocks_physical_plan::ConstantPoolId::new(
        7000 + u32::try_from(value).expect("fixture literals are small and nonnegative"),
    )
}

/// Registers exactly the fixture pools `fragment` refers to: the closed-pool
/// publication law refuses a pool nothing references.
fn register_constant_pools(
    builder: &mut PlanBuilder,
    fragment: &novarocks_physical_plan::Fragment,
) {
    for (_, expression) in fragment.expressions().iter() {
        let ExprKind::Constant(reference) = expression.kind else {
            continue;
        };
        if builder.constants().entries().contains_key(&reference.pool) {
            continue;
        }
        let value = i64::from(reference.pool.get() - 7000);
        let ty = novarocks_type_contract::FunctionValueType::new(DataType::Int64, false);
        let pool = novarocks_physical_plan::ConstantPool::try_new(
            Arc::new(ty.try_to_field("literal").unwrap()),
            ty,
            Int64Array::from(vec![value]).to_data(),
            novarocks_functions::ConstantPolicy {
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
            novarocks_type_contract::CompilePhase::Validate,
            compile_control(),
        )
        .unwrap();
        builder.insert_constant_pool(reference.pool, pool).unwrap();
    }
}

fn unconstrained_properties() -> PhysicalProperties {
    PhysicalProperties {
        distribution: Distribution::Unconstrained,
        row_multiplicity: RowMultiplicity::SingleCopy,
        ordering: Box::default(),
    }
}

fn append_i64_values(
    builder: &mut FragmentBuilder,
    literal_value: i64,
) -> (novarocks_physical_plan::NodeId, ValueId) {
    let node = builder.reserve_node_id().unwrap();
    let ty = ValueType::new(DataType::Int64, false);
    let literal = builder
        .add_expression(node, ty.clone(), i64_constant(literal_value))
        .unwrap();
    let value = builder
        .add_value(
            ty,
            ValueOrigin::NodeOutput {
                node,
                output_ordinal: 0,
            },
        )
        .unwrap();
    builder
        .insert_node_unchecked(PhysicalNode {
            id: node,
            inputs: Box::default(),
            required_inputs: Box::default(),
            output_properties: singleton_properties(),
            output: OutputPort {
                node,
                columns: Box::from([value]),
            },
            kind: NodeKind::Values {
                rows: Box::from([Box::from([literal])]),
            },
        })
        .unwrap();
    (node, value)
}

fn append_empty_broadcast_i64_values(
    builder: &mut FragmentBuilder,
) -> (novarocks_physical_plan::NodeId, ValueId) {
    let node = builder.reserve_node_id().unwrap();
    let value = builder
        .add_value(
            ValueType::new(DataType::Int64, false),
            ValueOrigin::NodeOutput {
                node,
                output_ordinal: 0,
            },
        )
        .unwrap();
    builder
        .insert_node_unchecked(PhysicalNode {
            id: node,
            inputs: Box::default(),
            required_inputs: Box::default(),
            output_properties: PhysicalProperties {
                distribution: Distribution::Broadcast,
                row_multiplicity: RowMultiplicity::Replicated,
                ordering: Box::default(),
            },
            output: OutputPort {
                node,
                columns: Box::from([value]),
            },
            kind: NodeKind::Values {
                rows: Box::default(),
            },
        })
        .unwrap();
    (node, value)
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum HashJoinFixtureOutput {
    ReorderedPair,
    PreservedLeft,
    PreservedRight,
    NullableRight,
}

fn finish_hash_join_plan(
    fragment_id: u32,
    kind: JoinKind,
    distribution: JoinDistribution,
    left_literal: i64,
    right_literal: i64,
    output: HashJoinFixtureOutput,
) -> PhysicalPlan {
    let fragment_id = FragmentId::new(fragment_id);
    let mut builder = FragmentBuilder::new(fragment_id);
    let (left, left_value) = append_i64_values(&mut builder, left_literal);
    let (right, right_value) = if distribution == JoinDistribution::BroadcastBuild {
        append_empty_broadcast_i64_values(&mut builder)
    } else {
        append_i64_values(&mut builder, right_literal)
    };
    let join = builder.reserve_node_id().unwrap();
    let ty = ValueType::new(DataType::Int64, false);
    let left_key_kind = if output == HashJoinFixtureOutput::ReorderedPair {
        i64_constant(1)
    } else {
        ExprKind::Value(left_value)
    };
    let right_key_kind = if output == HashJoinFixtureOutput::ReorderedPair {
        i64_constant(1)
    } else {
        ExprKind::Value(right_value)
    };
    let left_key = builder
        .add_expression(join, ty.clone(), left_key_kind)
        .unwrap();
    let right_key = builder
        .add_expression(join, ty.clone(), right_key_kind)
        .unwrap();
    let (outputs, null_extended): (Box<[ValueId]>, Box<[ValueId]>) = match output {
        HashJoinFixtureOutput::ReorderedPair => {
            (Box::from([right_value, left_value]), Box::default())
        }
        HashJoinFixtureOutput::PreservedLeft => (Box::from([left_value]), Box::default()),
        HashJoinFixtureOutput::PreservedRight => (Box::from([right_value]), Box::default()),
        HashJoinFixtureOutput::NullableRight => {
            let nullable_right = builder
                .add_value(
                    ValueType::new(DataType::Int64, true),
                    ValueOrigin::NullExtended {
                        node: join,
                        of: right_value,
                    },
                )
                .unwrap();
            (Box::from([nullable_right]), Box::from([nullable_right]))
        }
    };
    let (build_side, required_inputs) = match (kind, distribution) {
        (JoinKind::RightSemi | JoinKind::RightAnti, _) => (
            JoinSide::Left,
            Box::from([singleton_properties(), singleton_properties()]),
        ),
        (_, JoinDistribution::BroadcastBuild) => (
            JoinSide::Right,
            Box::from([
                singleton_properties(),
                PhysicalProperties {
                    distribution: Distribution::Broadcast,
                    row_multiplicity: RowMultiplicity::Replicated,
                    ordering: Box::default(),
                },
            ]),
        ),
        _ => (
            JoinSide::Right,
            Box::from([singleton_properties(), singleton_properties()]),
        ),
    };
    builder
        .insert_node_unchecked(PhysicalNode {
            id: join,
            inputs: Box::from([left, right]),
            required_inputs,
            output_properties: singleton_properties(),
            output: OutputPort {
                node: join,
                columns: outputs.clone(),
            },
            kind: NodeKind::HashJoin {
                kind,
                build_side,
                keys: Box::from([JoinKey {
                    left: left_key,
                    right: right_key,
                    null_safe: false,
                }]),
                distribution,
                residual: None,
                null_extended,
            },
        })
        .unwrap();
    finish_result_plan(builder, fragment_id, join, outputs)
}

fn finish_nest_loop_projection_plan(fragment_id: u32) -> PhysicalPlan {
    let fragment_id = FragmentId::new(fragment_id);
    let mut builder = FragmentBuilder::new(fragment_id);
    let (left, _) = append_i64_values(&mut builder, 31);
    let (right, right_value) = append_i64_values(&mut builder, 47);
    let join = builder.reserve_node_id().unwrap();
    let outputs: Box<[ValueId]> = Box::from([right_value]);
    builder
        .insert_node_unchecked(PhysicalNode {
            id: join,
            inputs: Box::from([left, right]),
            required_inputs: Box::from([singleton_properties(), singleton_properties()]),
            output_properties: singleton_properties(),
            output: OutputPort {
                node: join,
                columns: outputs.clone(),
            },
            kind: NodeKind::NestLoopJoin {
                kind: JoinKind::Cross,
                distribution: NestLoopJoinDistribution::Singleton,
                predicate: None,
                null_extended: Box::default(),
            },
        })
        .unwrap();
    finish_result_plan(builder, fragment_id, join, outputs)
}

fn finish_result_plan(
    builder: FragmentBuilder,
    fragment_id: FragmentId,
    root: novarocks_physical_plan::NodeId,
    outputs: Box<[ValueId]>,
) -> PhysicalPlan {
    finish_result_plan_with_scalar_proof(builder, fragment_id, root, outputs, true)
}

fn finish_result_plan_with_scalar_proof(
    builder: FragmentBuilder,
    fragment_id: FragmentId,
    root: novarocks_physical_plan::NodeId,
    outputs: Box<[ValueId]>,
    include_scalar_proof: bool,
) -> PhysicalPlan {
    let fragment = builder
        .finish_definition(
            root,
            FragmentSink::Result,
            PipelineDopDomain {
                min: 1,
                max: 1,
                requires_power_of_two: false,
            },
        )
        .unwrap();
    let fields: Box<[ResultField]> = outputs
        .iter()
        .enumerate()
        .map(|(ordinal, value)| ResultField {
            domain: novarocks_physical_plan::ResultValueDomain::Plain,
            name: format!("column_{ordinal}").into(),
            alias: None,
            value: *value,
            ty: fragment.values()[value].ty.clone(),
        })
        .collect();
    let scalar_schema = if include_scalar_proof {
        match fields.as_ref() {
            [field] => fixture_plain_scalar_schema(&field.ty),
            _ => None,
        }
    } else {
        None
    };
    let mut plan_builder =
        PlanBuilder::new(PlanVersionId::try_new([fragment_id.get() as u8; 16]).unwrap());
    register_constant_pools(&mut plan_builder, &fragment);
    plan_builder.add_fragment(fragment).unwrap();
    plan_builder
        .set_result_port(ResultPort {
            scalar_schema,
            fragment: fragment_id,
            output: OutputPort {
                node: root,
                columns: outputs,
            },
            fields,
        })
        .unwrap();
    plan_builder.finish_observed(compile_control()).unwrap()
}

// Test-side Arrow inspection runs on one dedicated consumer. Drivers publish
// only their admitted input; the permit survives until that input really exits.
#[derive(Default)]
struct RoundtripResultState {
    input: Option<(Chunk, RootInputPermit)>,
    rows: Vec<Vec<Option<i64>>>,
    finishing: bool,
    aborted: bool,
    exited: bool,
    failure: Option<String>,
}

struct RoundtripRootSession {
    spec: RootResultWriteSpec,
    authority: RootInputAuthority,
    tracker: Arc<novarocks_execution::runtime::mem_tracker::MemTracker>,
    state: Arc<(Mutex<RoundtripResultState>, std::sync::Condvar)>,
    consumer: Mutex<Option<std::thread::JoinHandle<()>>>,
}

fn roundtrip_credit(
    tracker: &Arc<novarocks_execution::runtime::mem_tracker::MemTracker>,
    bytes: usize,
) -> ResultWriteCredit {
    let charge = i64::try_from(bytes).expect("fixture root credit is representable");
    if let Err(error) = tracker.consume_and_check_limit_typed(charge) {
        tracker.release(charge);
        panic!("fixture root input admission failed: {error}");
    }
    let tracker = tracker.clone();
    ResultWriteCredit::new(bytes, move |released| {
        tracker.release(i64::try_from(released).expect("admitted root credit extent"));
    })
}

impl RoundtripRootSession {
    fn new(
        spec: RootResultWriteSpec,
        output_slots: Vec<SlotId>,
        tracker: Arc<novarocks_execution::runtime::mem_tracker::MemTracker>,
    ) -> Arc<Self> {
        let initial = Arc::new(roundtrip_credit(
            &tracker,
            RootInputAuthority::initial_backing_bytes(),
        ));
        let authority = RootInputAuthority::try_new_retained(&spec, initial)
            .expect("admitted fixture root issuer");
        let state = Arc::new((
            Mutex::new(RoundtripResultState::default()),
            std::sync::Condvar::new(),
        ));
        let consumer_state = state.clone();
        let observable = authority.observable();
        let consumer = std::thread::spawn(move || {
            loop {
                let input = {
                    let (state, ready) = &*consumer_state;
                    let mut guard = state.lock().unwrap();
                    while guard.input.is_none() && !guard.finishing && !guard.aborted {
                        guard = ready.wait(guard).unwrap();
                    }
                    if let Some(input) = guard.input.take() {
                        Some(input)
                    } else {
                        guard.exited = true;
                        None
                    }
                };
                let Some((chunk, permit)) = input else {
                    observable.notify_observers();
                    break;
                };
                let inspected = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    let mut rows = Vec::new();
                    for row in 0..chunk.len() {
                        rows.push(
                            output_slots
                                .iter()
                                .map(|slot| {
                                    let column = chunk
                                        .column_by_slot_id(*slot)
                                        .expect("original output slot");
                                    let array = column
                                        .as_any()
                                        .downcast_ref::<Int64Array>()
                                        .expect("original INT64 test output carrier");
                                    (!array.is_null(row)).then(|| array.value(row))
                                })
                                .collect(),
                        );
                    }
                    rows
                }));
                // Return the exact upstream position after the real Arrow
                // input and every inspection borrow have gone away.
                drop(chunk);
                drop(permit);
                let mut guard = consumer_state.0.lock().unwrap();
                match inspected {
                    Ok(rows) => guard.rows.extend(rows),
                    Err(_) => {
                        guard.failure = Some("fixture Arrow row inspection failed".into());
                        guard.aborted = true;
                    }
                }
                drop(guard);
                observable.notify_observers();
            }
        });
        Arc::new(Self {
            spec,
            authority,
            tracker,
            state,
            consumer: Mutex::new(Some(consumer)),
        })
    }

    fn finish_rows(&self) -> Vec<Vec<Option<i64>>> {
        self.consumer
            .lock()
            .unwrap()
            .take()
            .unwrap()
            .join()
            .expect("the fixture consumer actually exited");
        let mut state = self.state.0.lock().unwrap();
        assert!(state.exited && state.finishing && !state.aborted);
        assert!(state.failure.is_none(), "{:?}", state.failure);
        assert!(state.input.is_none());
        std::mem::take(&mut state.rows)
    }
}

impl RootResultSession for RoundtripRootSession {
    fn spec(&self) -> &RootResultWriteSpec {
        &self.spec
    }
    fn try_acquire_input(&self) -> Result<RootInputAdmission, FragmentIoError> {
        if !self.authority.is_available() {
            return Ok(RootInputAdmission::Blocked);
        }
        self.authority.try_acquire(roundtrip_credit(
            &self.tracker,
            self.authority.required_bytes(),
        ))
    }
    fn writable_observable(&self) -> Arc<novarocks_execution::runtime::observable::Observable> {
        self.authority.observable()
    }
    fn submit_input(&self, chunk: Chunk, permit: RootInputPermit) -> Result<(), FragmentIoError> {
        assert!(self.authority.owns(&permit));
        let mut state = self.state.0.lock().unwrap();
        assert!(!state.finishing && !state.aborted);
        assert!(state.input.replace((chunk, permit)).is_none());
        drop(state);
        self.state.1.notify_one();
        Ok(())
    }
    fn finish_input(&self) -> Result<(), FragmentIoError> {
        self.state.0.lock().unwrap().finishing = true;
        self.state.1.notify_one();
        Ok(())
    }
    fn producer_state(&self) -> RootProducerState {
        let state = self.state.0.lock().unwrap();
        if let Some(failure) = &state.failure {
            RootProducerState::Failed(failure.clone())
        } else if state.exited {
            RootProducerState::ContextHeld
        } else if state.finishing {
            RootProducerState::Finishing
        } else {
            RootProducerState::Accepting
        }
    }
    fn producer_exited(&self) -> bool {
        self.state.0.lock().unwrap().exited
    }
    fn abort(&self, _: ResultAbort) {
        self.state.0.lock().unwrap().aborted = true;
        self.authority.close();
        self.state.1.notify_one();
    }
}

impl Drop for RoundtripRootSession {
    fn drop(&mut self) {
        self.state.0.lock().unwrap().aborted = true;
        self.state.1.notify_one();
        if let Some(consumer) = self.consumer.lock().unwrap().take() {
            consumer.join().expect("fixture consumer shutdown");
        }
    }
}

struct RejectRoundtripLegacyIo;
impl ExchangeFrameTransmitter for RejectRoundtripLegacyIo {
    fn transmit(&self, _: ExchangeFrame) -> Result<(), ExchangeTransmitRejection> {
        panic!("these local join fixtures have no exchange edge")
    }
}
impl FragmentResultWriter for RejectRoundtripLegacyIo {
    fn open(&self, _: ResultWriteSpec) -> Result<Arc<dyn FragmentResultSession>, FragmentIoError> {
        panic!("the frozen root must use its exact bounded session")
    }
}

fn lower_fixture_with_actual_creator(
    plan: novarocks_execution::exec::node::ExecPlan,
    sink: novarocks_local_program::StaticSinkProgram,
) -> (
    novarocks_local_program::LocalProgramGraph,
    novarocks_execution::exec::node::LocalRuntimeBindings,
) {
    try_lower_fixture_with_actual_creator(plan, sink).unwrap()
}

fn try_lower_fixture_with_actual_creator(
    plan: novarocks_execution::exec::node::ExecPlan,
    sink: novarocks_local_program::StaticSinkProgram,
) -> Result<
    (
        novarocks_local_program::LocalProgramGraph,
        novarocks_execution::exec::node::LocalRuntimeBindings,
    ),
    novarocks_execution::exec::node::LocalProgramCreationFailure<
        crate::native_type_construction::NativeTypeConstructionError,
    >,
> {
    use novarocks_execution::exec::node::{ExternalSinkRequirement, LocalLayoutCreation};
    let creation = crate::native_type_construction::NativeTypeConstruction::try_with_control(
        novarocks_execution::runtime::mem_tracker::MemTracker::new_root(
            "native roundtrip local program creator",
        ),
        compile_control(),
    )
    .map_err(novarocks_execution::exec::node::LocalProgramCreationFailure::Creator)?;
    let scope = creation
        .begin_scope()
        .map_err(novarocks_execution::exec::node::LocalProgramCreationFailure::Creator)?;
    let temporary = creation
        .begin_scope()
        .map_err(novarocks_execution::exec::node::LocalProgramCreationFailure::Creator)?;
    let mut temporary_request = |layout| temporary.request_layout(layout);
    let mut request = |layout| scope.request_layout(layout);
    let mut port = LocalLayoutCreation::new(
        scope.storage_lifetime(),
        &mut request,
        &mut temporary_request,
        compile_control(),
    );
    let profile = plan.local_compile_profile_with_creator(
        std::num::NonZeroUsize::new(1).unwrap(),
        None,
        &mut port,
    )?;
    plan.into_local_program_and_bindings_with_creator(
        profile,
        std::collections::BTreeMap::new(),
        vec![ExternalSinkRequirement::Result],
        sink,
        port,
    )
}

// These five fixtures test client row output, with an independently
// declared signed INT64 presentation. Freeze that purpose at the FE plan
// boundary before encoding; the backend never invents or replaces it.
fn freeze_i64_fixture_client_rows(plan: PhysicalPlan) -> PhysicalPlan {
    use novarocks_result_contract::{
        ClientRenderSchema, FrozenRootOutput, NativeRenderType, RenderColumn, RenderField,
        RenderPresentation, RootOutputContract, RootProfileId,
    };
    let result = plan.result_port().expect("fixture client result port");
    let columns = result
        .fields
        .iter()
        .enumerate()
        .map(|(ordinal, field)| {
            assert_eq!(
                field.ty,
                ValueType::new(DataType::Int64, field.ty.nullable()),
                "client INT64 fixture retains its full independently declared value type"
            );
            RenderColumn {
                source_ordinal: u32::try_from(ordinal).expect("fixture result ordinal"),
                source_slot: None,
                name: field.name.to_string(),
                field: RenderField {
                    nullable: field.ty.nullable(),
                    native_type: NativeRenderType::SignedInteger(64),
                    presentation: RenderPresentation::ScalarText,
                },
            }
        })
        .collect::<Vec<_>>();
    let schema = ClientRenderSchema::try_new(columns, result.fields.len())
        .expect("the fixture's explicit ordered client presentation");
    plan.with_root_output(RootOutputContract::new(
        RootProfileId::V1,
        FrozenRootOutput::ClientRows(schema),
    ))
    .expect("the complete FE plan validates its frozen client purpose")
}

fn encode_decode_execute(
    plan: &PhysicalPlan,
) -> (Vec<Vec<Option<i64>>>, Vec<SlotId>, ExecNodeKind) {
    let catalog = novarocks_sql::compiler::build_builtin_engine_function_catalog().unwrap();
    let encoded = novarocks_plan_codec::encode_physical_plan_typed_pure(
        plan,
        &catalog,
        &novarocks_plan_codec::NoPhysicalV1PrivateFacts,
        false,
        compile_control(),
    )
    .unwrap();
    let fragment = &encoded.fragments[0];
    let mut arena = ExprArena::default();
    let decoded = decode_node(
        fragment,
        fragment.root.as_ref().unwrap(),
        &mut arena,
        &NativePlanDecodeContext::default(),
    )
    .unwrap();
    let output_slots = decoded.layout.order().to_vec();
    let root_kind = decoded.node.kind.clone();
    use novarocks_execution::exec::fragment::program::{
        FragmentContractVersion, FragmentProgram, FragmentProgramOptions, RuntimeFilterContract,
    };
    use novarocks_execution::runtime::fragment::{
        BackendNum, ExchangeInputAssignments, FragmentInstanceId, FragmentInstanceSpec,
        FragmentOutcome, FragmentPrepareContext, FragmentRuntimeOptions, FragmentSinkAssignment,
        FragmentSubmission, ScanAssignments, prepare_fragment,
    };
    use novarocks_execution::runtime::query_options::QueryOptions;
    use novarocks_types::{
        AttemptId, BackendProcessId, QueryExecutionId, QueryId, StageId, TaskId, UniqueId,
    };
    use std::collections::{BTreeMap, BTreeSet};
    use std::num::NonZeroUsize;

    let sink = decode_typed_sink_program(fragment, &decoded.layout)
        .unwrap()
        .into_static()
        .unwrap();
    let novarocks_local_program::StaticSinkProgram::RootResult(contract) = &sink else {
        panic!("a complete result port freezes its root contract")
    };
    let contract = contract.clone();
    let exec_plan = ExecPlanBuilder::new(arena, decoded.node).finish().unwrap();
    let (local, bindings) = lower_fixture_with_actual_creator(exec_plan, sink);
    let program = Arc::new(
        FragmentProgram::try_new(
            Arc::new(local),
            FragmentProgramOptions::new(FragmentContractVersion::CURRENT),
            BTreeMap::new(),
            BTreeMap::new(),
            RuntimeFilterContract::new(BTreeSet::new(), BTreeSet::new()),
        )
        .unwrap(),
    );
    let query_id = QueryId::new(6301, 6302);
    let backend = BackendProcessId::new_v7();
    // Each fixture invocation owns a distinct fragment registration, including
    // when the original join cases run concurrently in the native test binary.
    let (high, low) = backend.as_uuid().as_u64_pair();
    let finst_id = UniqueId::new(high as i64, low as i64);
    let task = novarocks_execution_contract::TaskIdentity::new(
        QueryExecutionId::new(query_id, AttemptId::new(1).unwrap()).unwrap(),
        StageId::new(1).unwrap(),
        TaskId::new(1).unwrap(),
        backend,
    );
    let instance = FragmentInstanceSpec::new_native(
        FragmentContractVersion::CURRENT,
        query_id,
        FragmentInstanceId::new(finst_id),
        ScanAssignments::default(),
        ExchangeInputAssignments::default(),
        FragmentSinkAssignment::None,
        FragmentRuntimeOptions::new(QueryOptions::default(), false),
        NonZeroUsize::new(1).unwrap(),
        BackendNum::try_new(1).unwrap(),
    );
    let submission = FragmentSubmission::try_new(program, bindings, instance).unwrap();
    let tracker =
        novarocks_execution::runtime::mem_tracker::MemTracker::new_root("roundtrip execution");
    let session = RoundtripRootSession::new(
        RootResultWriteSpec { task, contract },
        output_slots.clone(),
        tracker.clone(),
    );
    let context = FragmentPrepareContext::without_runtime_filter(
        None,
        Some(tracker.clone()),
        Arc::new(RejectRoundtripLegacyIo),
        Arc::new(RejectRoundtripLegacyIo),
        Arc::new(NoopFragmentEventSink),
    )
    .with_fragment_commit_port(Arc::new(
        novarocks_worker::sink_commit::WorkerSinkCommitPort,
    ))
    .with_execution_runtime(test_execution_runtime())
    .with_result_identity(task)
    .with_root_result_session(session.clone());
    let dormant = prepare_fragment(submission, context).expect("public typed fragment preparation");
    let running = dormant.start();
    let terminal = running.join();
    assert_eq!(terminal.query_id(), query_id);
    assert_eq!(terminal.fragment_instance_id(), finst_id);
    assert_eq!(
        terminal.outcome(),
        &FragmentOutcome::Succeeded,
        "{terminal:?}"
    );
    let rows = session.finish_rows();
    assert!(session.producer_exited());
    drop(running);
    drop(session);
    assert_eq!(
        tracker.current(),
        0,
        "the actual root input and issuer grants exited"
    );
    (rows, output_slots, root_kind)
}

fn test_execution_runtime() -> Arc<ExecutionRuntime> {
    static RUNTIME: OnceLock<Arc<ExecutionRuntime>> = OnceLock::new();
    Arc::clone(RUNTIME.get_or_init(|| {
        let mut functions = ExecutionFunctionSetBuilder::new();
        novarocks_sql::compiler::contribute_builtin_functions(functions.catalog_builder_mut())
            .unwrap();
        contribute_builtin_aggregate_implementations(&mut functions).unwrap();
        Arc::new(
            ExecutionRuntime::new(
                ExecutionRuntimeConfig {
                    driver_threads: 1,
                    exchange_wait_ms: 120_000,
                    exchange_io_threads: 1,
                    exchange_io_max_inflight_bytes: 1024,
                    exchange_max_transmit_batched_bytes: 1024,
                    operator_buffer_chunks: 1,
                    local_exchange_buffer_mem_limit_per_driver: 1024,
                    local_exchange_max_buffered_rows: 1024,
                    runtime_filter_scan_wait_time_ms_override: None,
                    runtime_filter_wait_timeout_ms_override: None,
                    sink_io_worker_threads: 1,
                    sink_io_max_blocking_threads: 1,
                },
                Arc::new(functions.seal().unwrap()),
                crate::backend_test_support::test_memory_authority(),
            )
            .unwrap(),
        )
    }))
}

struct OneWriteFact {
    ordinal: WriteTargetOrdinal,
    fact: PhysicalV1WriteFact,
}

impl PhysicalV1PrivateFacts for OneWriteFact {
    fn scan_fact(
        &self,
        _fragment: FragmentId,
        _node: novarocks_physical_plan::NodeId,
    ) -> Option<&novarocks_plan_codec::PhysicalV1ScanFact> {
        None
    }

    fn write_fact(&self, target: WriteTargetOrdinal) -> Option<&PhysicalV1WriteFact> {
        (target == self.ordinal).then_some(&self.fact)
    }
}

fn write_handle() -> ConnectorEncodedPayload {
    let provider = ConnectorProviderId::parse("iceberg").unwrap();
    let instance = ConnectorInstanceId::try_from_canonical("lakehouse").unwrap();
    let catalog = CatalogHandle::new(instance.clone(), CatalogVersion::from_bytes([3; 32]));
    ConnectorEncodedPayload::new(
        ConnectorEnvelopeHeader::new(
            provider,
            catalog,
            ConnectorCodecCategory::WriteHandle,
            ConnectorCodecRevision::try_new(1).unwrap(),
        ),
        vec![7].into(),
    )
}

fn writer_relation_fields(
    builder: &mut FragmentBuilder,
    owner: novarocks_physical_plan::NodeId,
    root: bool,
) -> Vec<WriterRelationField> {
    use std::sync::Arc;

    let specs = if root {
        vec![
            ("kind", DataType::Int8, false, WriterRelationFieldRole::Kind),
            (
                "write_target_ordinal",
                DataType::Int32,
                true,
                WriterRelationFieldRole::TargetOrdinal,
            ),
            (
                "row_count",
                DataType::Int64,
                true,
                WriterRelationFieldRole::RowCount,
            ),
            (
                "commit_fragment",
                DataType::Binary,
                true,
                WriterRelationFieldRole::CommitFragment,
            ),
            (
                "input_fields",
                DataType::List(Arc::new(arrow::datatypes::Field::new(
                    "item",
                    DataType::Int32,
                    false,
                ))),
                true,
                WriterRelationFieldRole::Auxiliary,
            ),
            (
                "blob_type",
                DataType::Utf8,
                true,
                WriterRelationFieldRole::Auxiliary,
            ),
            (
                "body",
                DataType::Binary,
                true,
                WriterRelationFieldRole::Auxiliary,
            ),
            (
                "properties",
                DataType::Map(
                    Arc::new(arrow::datatypes::Field::new(
                        "entries",
                        DataType::Struct(arrow::datatypes::Fields::from(vec![
                            arrow::datatypes::Field::new("key", DataType::Utf8, false),
                            arrow::datatypes::Field::new("value", DataType::Utf8, false),
                        ])),
                        false,
                    )),
                    false,
                ),
                true,
                WriterRelationFieldRole::Auxiliary,
            ),
        ]
    } else {
        vec![
            ("kind", DataType::Int8, false, WriterRelationFieldRole::Kind),
            (
                "write_target_ordinal",
                DataType::Int32,
                false,
                WriterRelationFieldRole::TargetOrdinal,
            ),
            (
                "row_count",
                DataType::Int64,
                true,
                WriterRelationFieldRole::RowCount,
            ),
            (
                "commit_fragment",
                DataType::Binary,
                true,
                WriterRelationFieldRole::CommitFragment,
            ),
        ]
    };
    specs
        .into_iter()
        .map(|(name, data_type, nullable, role)| {
            let ty = ValueType::new(data_type, nullable);
            let value = builder
                .add_value(
                    ty.clone(),
                    ValueOrigin::WriterDerived {
                        writer_node: owner,
                        kind: match role {
                            WriterRelationFieldRole::Kind => WriterDerivedKind::RelationKind,
                            WriterRelationFieldRole::TargetOrdinal => {
                                WriterDerivedKind::WriteTargetOrdinal
                            }
                            WriterRelationFieldRole::RowCount => WriterDerivedKind::AffectedRows,
                            WriterRelationFieldRole::CommitFragment => {
                                WriterDerivedKind::CommitFragment
                            }
                            WriterRelationFieldRole::Auxiliary => {
                                WriterDerivedKind::RelationAuxiliary
                            }
                        },
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

fn finish_duplicate_router_plan() -> (PhysicalPlan, OneWriteFact) {
    let source_fragment = FragmentId::new(81);
    let writer_fragment = FragmentId::new(82);
    let finish_fragment = FragmentId::new(83);
    let router_edge = EdgeId::new(81);
    let finish_edge = EdgeId::new(82);
    let input_ty = ValueType::new(DataType::Int64, false);
    let token_a = ConnectorWriteFieldToken::from_bytes([1; 32]);
    let token_b = ConnectorWriteFieldToken::from_bytes([2; 32]);
    let ordinal = WriteTargetOrdinal::try_new(0).unwrap();

    let mut source_builder = FragmentBuilder::new(source_fragment);
    let (values, source_value) = append_i64_values(&mut source_builder, 9);
    let expand = source_builder.reserve_node_id().unwrap();
    let assignment = source_builder
        .add_expression(expand, input_ty.clone(), ExprKind::Value(source_value))
        .unwrap();
    let effect = source_builder
        .add_value(
            ValueType::new(DataType::Int8, false),
            ValueOrigin::NodeOutput {
                node: expand,
                output_ordinal: 0,
            },
        )
        .unwrap();
    let routed = source_builder
        .add_value(
            input_ty.clone(),
            ValueOrigin::NodeOutput {
                node: expand,
                output_ordinal: 1,
            },
        )
        .unwrap();
    source_builder
        .insert_node_unchecked(PhysicalNode {
            id: expand,
            inputs: Box::from([values]),
            required_inputs: Box::from([singleton_properties()]),
            output_properties: unconstrained_properties(),
            output: OutputPort {
                node: expand,
                columns: Box::from([effect, routed]),
            },
            kind: NodeKind::ChangeEventExpand {
                events: Box::from([ChangeEventSpec {
                    predicate: None,
                    effect: ConnectorRowMutationEffect::Insert,
                    assignments: Box::from([(routed, Some(assignment))]),
                }]),
                effect_output: effect,
            },
        })
        .unwrap();
    let project = source_builder.reserve_node_id().unwrap();
    let effect_ref = source_builder
        .add_expression(
            project,
            ValueType::new(DataType::Int8, false),
            ExprKind::Value(effect),
        )
        .unwrap();
    let routed_ref = source_builder
        .add_expression(project, input_ty.clone(), ExprKind::Value(routed))
        .unwrap();
    source_builder
        .insert_node_unchecked(PhysicalNode {
            id: project,
            inputs: Box::from([expand]),
            required_inputs: Box::from([unconstrained_properties()]),
            output_properties: unconstrained_properties(),
            output: OutputPort {
                node: project,
                columns: Box::from([effect, routed, routed]),
            },
            kind: NodeKind::Project {
                expressions: Box::from([
                    (effect_ref, effect),
                    (routed_ref, routed),
                    (routed_ref, routed),
                ]),
            },
        })
        .unwrap();
    let route_id = ConnectorWriteRouteId::from_bytes([8; 32]);
    let source = source_builder
        .finish_definition(
            project,
            FragmentSink::Router {
                effect,
                routes: Box::from([ChangeStreamRoute {
                    route_id,
                    write_target_ordinal: ordinal,
                    accepted_effects: Box::from([ConnectorRowMutationEffect::Insert]),
                    input_mapping: Box::from([(token_a, routed), (token_b, routed)]),
                    partition_by: Box::default(),
                    edge: router_edge,
                }]),
            },
            PipelineDopDomain {
                min: 1,
                max: 1,
                requires_power_of_two: false,
            },
        )
        .unwrap();

    let mut writer_builder = FragmentBuilder::new(writer_fragment);
    let exchange = writer_builder.reserve_node_id().unwrap();
    let imported_a = writer_builder
        .add_value(
            input_ty.clone(),
            ValueOrigin::ExchangeImport {
                edge: router_edge,
                source_value: routed,
            },
        )
        .unwrap();
    let imported_b = writer_builder
        .add_value(
            input_ty.clone(),
            ValueOrigin::ExchangeImport {
                edge: router_edge,
                source_value: routed,
            },
        )
        .unwrap();
    writer_builder
        .insert_node_unchecked(PhysicalNode {
            id: exchange,
            inputs: Box::default(),
            required_inputs: Box::default(),
            output_properties: singleton_properties(),
            output: OutputPort {
                node: exchange,
                columns: Box::from([imported_a, imported_b]),
            },
            kind: NodeKind::ExchangeSource {
                edge: router_edge,
                imports: Box::from([(routed, imported_a), (routed, imported_b)]),
            },
        })
        .unwrap();
    let writer = writer_builder.reserve_node_id().unwrap();
    let writer_fields = writer_relation_fields(&mut writer_builder, writer, false);
    let handle = write_handle();
    writer_builder
        .insert_node_unchecked(PhysicalNode {
            id: writer,
            inputs: Box::from([exchange]),
            required_inputs: Box::from([singleton_properties()]),
            output_properties: unconstrained_properties(),
            output: OutputPort {
                node: writer,
                columns: writer_fields.iter().map(|field| field.value).collect(),
            },
            kind: NodeKind::TableWriter {
                target: WriterTarget {
                    handle: handle.clone(),
                    write_target_ordinal: ordinal,
                    input: Box::from([imported_a, imported_b]),
                    required_distribution: Distribution::Singleton,
                    target_fields: Box::from([
                        WriterTargetField {
                            provider_name: "a".into(),
                            token: token_a,
                            input: imported_a,
                            ty: input_ty.clone(),
                            hidden: false,
                        },
                        WriterTargetField {
                            provider_name: "b".into(),
                            token: token_b,
                            input: imported_b,
                            ty: input_ty,
                            hidden: false,
                        },
                    ]),
                    output_schema: WriterRelationSchema {
                        revision: WRITER_MULTIPLEX_SCHEMA_REVISION,
                        fields: writer_fields.clone().into_boxed_slice(),
                    },
                    partial_aggregates: Box::default(),
                },
            },
        })
        .unwrap();
    let writer_stage = writer_builder
        .finish_definition(
            writer,
            FragmentSink::Stream { edge: finish_edge },
            PipelineDopDomain {
                min: 1,
                max: 1,
                requires_power_of_two: false,
            },
        )
        .unwrap();

    let mut finish_builder = FragmentBuilder::new(finish_fragment);
    let finish_exchange = finish_builder.reserve_node_id().unwrap();
    let mut finish_input_fields = Vec::with_capacity(writer_fields.len());
    for field in &writer_fields {
        let imported = finish_builder
            .add_value(
                field.ty.clone(),
                ValueOrigin::ExchangeImport {
                    edge: finish_edge,
                    source_value: field.value,
                },
            )
            .unwrap();
        finish_input_fields.push(WriterRelationField {
            value: imported,
            name: field.name.clone(),
            ty: field.ty.clone(),
            role: field.role,
        });
    }
    let finish_receive_mapping: Box<[(ValueId, ValueId)]> = writer_fields
        .iter()
        .zip(&finish_input_fields)
        .map(|(source, imported)| (source.value, imported.value))
        .collect();
    finish_builder
        .insert_node_unchecked(PhysicalNode {
            id: finish_exchange,
            inputs: Box::default(),
            required_inputs: Box::default(),
            output_properties: singleton_properties(),
            output: OutputPort {
                node: finish_exchange,
                columns: finish_input_fields
                    .iter()
                    .map(|field| field.value)
                    .collect(),
            },
            kind: NodeKind::ExchangeSource {
                edge: finish_edge,
                imports: finish_receive_mapping.clone(),
            },
        })
        .unwrap();
    let finish = finish_builder.reserve_node_id().unwrap();
    let root_fields = writer_relation_fields(&mut finish_builder, finish, true);
    finish_builder
        .insert_node_unchecked(PhysicalNode {
            id: finish,
            inputs: Box::from([finish_exchange]),
            required_inputs: Box::from([singleton_properties()]),
            output_properties: singleton_properties(),
            output: OutputPort {
                node: finish,
                columns: root_fields.iter().map(|field| field.value).collect(),
            },
            kind: NodeKind::TableFinish(WriterFinishSpec {
                expected_target_ordinals: Box::from([ordinal]),
                input_schema: WriterRelationSchema {
                    revision: WRITER_MULTIPLEX_SCHEMA_REVISION,
                    fields: finish_input_fields.into_boxed_slice(),
                },
                output_schema: WriterRelationSchema {
                    revision: ROOT_WRITE_RESULT_SCHEMA_REVISION,
                    fields: root_fields.clone().into_boxed_slice(),
                },
                final_aggregates: Box::default(),
                grouped_unpivot: None,
            }),
        })
        .unwrap();
    let finish_stage = finish_builder
        .finish_definition(
            finish,
            FragmentSink::Result,
            PipelineDopDomain {
                min: 1,
                max: 1,
                requires_power_of_two: false,
            },
        )
        .unwrap();

    let mut plan_builder = PlanBuilder::new(PlanVersionId::try_new([81; 16]).unwrap());
    for fragment in [&source, &writer_stage, &finish_stage] {
        register_constant_pools(&mut plan_builder, fragment);
    }
    plan_builder.add_fragment(source).unwrap();
    plan_builder.add_fragment(writer_stage).unwrap();
    plan_builder.add_fragment(finish_stage).unwrap();
    plan_builder
        .add_edge(Edge {
            id: router_edge,
            kind: EdgeKind::ChangeStreamRouter,
            source: EdgeSource {
                fragment: source_fragment,
                projection: Box::from([routed, routed]),
            },
            destination: EdgeDestination {
                fragment: writer_fragment,
                node: exchange,
                receive_mapping: Box::from([(routed, imported_a), (routed, imported_b)]),
            },
            partitioning: EdgePartitioning {
                source: Distribution::Singleton,
                source_multiplicity: RowMultiplicity::SingleCopy,
                destination: Distribution::Singleton,
                destination_multiplicity: RowMultiplicity::SingleCopy,
            },
        })
        .unwrap();
    plan_builder
        .add_edge(Edge {
            id: finish_edge,
            kind: EdgeKind::Stream,
            source: EdgeSource {
                fragment: writer_fragment,
                projection: writer_fields.iter().map(|field| field.value).collect(),
            },
            destination: EdgeDestination {
                fragment: finish_fragment,
                node: finish_exchange,
                receive_mapping: finish_receive_mapping,
            },
            partitioning: EdgePartitioning {
                source: Distribution::Singleton,
                source_multiplicity: RowMultiplicity::SingleCopy,
                destination: Distribution::Singleton,
                destination_multiplicity: RowMultiplicity::SingleCopy,
            },
        })
        .unwrap();
    plan_builder
        .set_result_port(ResultPort {
            scalar_schema: None,
            fragment: finish_fragment,
            output: OutputPort {
                node: finish,
                columns: root_fields.iter().map(|field| field.value).collect(),
            },
            fields: root_fields
                .iter()
                .map(|field| ResultField {
                    domain: novarocks_physical_plan::ResultValueDomain::Plain,
                    name: field.name.clone(),
                    alias: None,
                    value: field.value,
                    ty: field.ty.clone(),
                })
                .collect(),
        })
        .unwrap();
    let physical = plan_builder.finish_observed(compile_control()).unwrap();
    let fact = PhysicalV1WriteFact {
        handle: connector_write::ConnectorWriterHandle {
            provider_payload: Some(
                novarocks_proto_codec::connector_common::encode_connector_payload_message(&handle),
            ),
        },
        field_names: std::collections::BTreeMap::from([
            (token_a.to_bytes(), Box::<str>::from("a")),
            (token_b.to_bytes(), Box::<str>::from("b")),
        ]),
    };
    (physical, OneWriteFact { ordinal, fact })
}

#[test]
fn physical_plan_finish_encode_decode_preserves_transparent_duplicate_layout() {
    let properties = singleton_properties();
    let mut builder = FragmentBuilder::new(FragmentId::new(71));
    let values = builder.reserve_node_id().unwrap();
    let ty = ValueType::new(DataType::Int64, false);
    let literal = builder
        .add_expression(values, ty.clone(), i64_constant(7))
        .unwrap();
    let value = builder
        .add_value(
            ty.clone(),
            ValueOrigin::NodeOutput {
                node: values,
                output_ordinal: 0,
            },
        )
        .unwrap();
    builder
        .insert_node_unchecked(PhysicalNode {
            id: values,
            inputs: Box::default(),
            required_inputs: Box::default(),
            output_properties: properties.clone(),
            output: OutputPort {
                node: values,
                columns: Box::from([value]),
            },
            kind: NodeKind::Values {
                rows: Box::from([Box::from([literal])]),
            },
        })
        .unwrap();

    let project = builder.reserve_node_id().unwrap();
    let reference = builder
        .add_expression(project, ty.clone(), ExprKind::Value(value))
        .unwrap();
    builder
        .insert_node_unchecked(PhysicalNode {
            id: project,
            inputs: Box::from([values]),
            required_inputs: Box::from([properties.clone()]),
            output_properties: properties.clone(),
            output: OutputPort {
                node: project,
                columns: Box::from([value, value]),
            },
            kind: NodeKind::Project {
                expressions: Box::from([(reference, value), (reference, value)]),
            },
        })
        .unwrap();

    let limit = builder.reserve_node_id().unwrap();
    builder
        .insert_node_unchecked(PhysicalNode {
            id: limit,
            inputs: Box::from([project]),
            required_inputs: Box::from([properties.clone()]),
            output_properties: properties,
            output: OutputPort {
                node: limit,
                columns: Box::from([value, value]),
            },
            kind: NodeKind::Limit {
                limit: Some(1),
                offset: 0,
            },
        })
        .unwrap();
    let fragment = builder
        .finish_definition(
            limit,
            FragmentSink::Result,
            PipelineDopDomain {
                min: 1,
                max: 1,
                requires_power_of_two: false,
            },
        )
        .unwrap();
    let mut plan_builder = PlanBuilder::new(PlanVersionId::try_new([71; 16]).unwrap());
    register_constant_pools(&mut plan_builder, &fragment);
    plan_builder.add_fragment(fragment).unwrap();
    plan_builder
        .set_result_port(ResultPort {
            scalar_schema: None,
            fragment: FragmentId::new(71),
            output: OutputPort {
                node: limit,
                columns: Box::from([value, value]),
            },
            fields: Box::from([
                ResultField {
                    domain: novarocks_physical_plan::ResultValueDomain::Plain,
                    name: "left".into(),
                    alias: None,
                    value,
                    ty: ty.clone(),
                },
                ResultField {
                    domain: novarocks_physical_plan::ResultValueDomain::Plain,
                    name: "right".into(),
                    alias: None,
                    value,
                    ty,
                },
            ]),
        })
        .unwrap();
    let physical = plan_builder.finish_observed(compile_control()).unwrap();
    let catalog = novarocks_sql::compiler::build_builtin_engine_function_catalog().unwrap();
    let encoded = novarocks_plan_codec::encode_physical_plan_typed_pure(
        &physical,
        &catalog,
        &novarocks_plan_codec::NoPhysicalV1PrivateFacts,
        false,
        compile_control(),
    )
    .unwrap();
    let wire_fragment = &encoded.fragments[0];
    let mut arena = ExprArena::default();
    let decoded = decode_node(
        wire_fragment,
        wire_fragment.root.as_ref().unwrap(),
        &mut arena,
        &NativePlanDecodeContext::default(),
    )
    .unwrap();
    assert_eq!(decoded.layout.order().len(), 2);
    assert_ne!(decoded.layout.order()[0], decoded.layout.order()[1]);
    assert_eq!(
        decoded.layout.order(),
        &wire_fragment
            .output_columns
            .iter()
            .map(|column| SlotId::new(column.column_id))
            .collect::<Vec<_>>()
    );
}

#[test]
fn physical_plan_finish_encode_decode_preserves_set_op_fresh_output_layout() {
    let mut builder = FragmentBuilder::new(FragmentId::new(72));
    let (left, left_value) = append_i64_values(&mut builder, 1);
    let (right, right_value) = append_i64_values(&mut builder, 2);
    let set_op = builder.reserve_node_id().unwrap();
    let ty = ValueType::new(DataType::Int64, false);
    let output = builder
        .add_value(
            ty.clone(),
            ValueOrigin::NodeOutput {
                node: set_op,
                output_ordinal: 0,
            },
        )
        .unwrap();
    builder
        .insert_node_unchecked(PhysicalNode {
            id: set_op,
            inputs: Box::from([left, right]),
            required_inputs: Box::from([singleton_properties(), singleton_properties()]),
            output_properties: singleton_properties(),
            output: OutputPort {
                node: set_op,
                columns: Box::from([output]),
            },
            kind: NodeKind::SetOp {
                kind: SetOperationKind::UnionAll,
                input_mappings: Box::from([Box::from([left_value]), Box::from([right_value])]),
            },
        })
        .unwrap();
    let fragment = builder
        .finish_definition(
            set_op,
            FragmentSink::Result,
            PipelineDopDomain {
                min: 1,
                max: 1,
                requires_power_of_two: false,
            },
        )
        .unwrap();
    let mut plan_builder = PlanBuilder::new(PlanVersionId::try_new([72; 16]).unwrap());
    register_constant_pools(&mut plan_builder, &fragment);
    plan_builder.add_fragment(fragment).unwrap();
    plan_builder
        .set_result_port(ResultPort {
            scalar_schema: None,
            fragment: FragmentId::new(72),
            output: OutputPort {
                node: set_op,
                columns: Box::from([output]),
            },
            fields: Box::from([ResultField {
                domain: novarocks_physical_plan::ResultValueDomain::Plain,
                name: "value".into(),
                alias: None,
                value: output,
                ty,
            }]),
        })
        .unwrap();
    let physical = plan_builder.finish_observed(compile_control()).unwrap();
    let catalog = novarocks_sql::compiler::build_builtin_engine_function_catalog().unwrap();
    let encoded = novarocks_plan_codec::encode_physical_plan_typed_pure(
        &physical,
        &catalog,
        &novarocks_plan_codec::NoPhysicalV1PrivateFacts,
        false,
        compile_control(),
    )
    .unwrap();
    let wire_fragment = &encoded.fragments[0];
    let mut arena = ExprArena::default();
    let decoded = decode_node(
        wire_fragment,
        wire_fragment.root.as_ref().unwrap(),
        &mut arena,
        &NativePlanDecodeContext::default(),
    )
    .unwrap();
    assert!(matches!(decoded.node.kind, ExecNodeKind::UnionAll(_)));
    assert_eq!(decoded.layout.order().len(), 1);
    assert_eq!(
        decoded.layout.order()[0],
        SlotId::new(wire_fragment.output_columns[0].column_id)
    );
}

#[test]
fn physical_plan_finish_encode_decode_preserves_duplicate_router_occurrences() {
    let (physical, facts) = finish_duplicate_router_plan();
    let catalog = novarocks_sql::compiler::build_builtin_engine_function_catalog().unwrap();
    let encoded = novarocks_plan_codec::encode_physical_plan_typed_pure(
        &physical,
        &catalog,
        &facts,
        false,
        compile_control(),
    )
    .expect("duplicate router occurrences have distinct v1 slots");
    let source = encoded
        .fragments
        .iter()
        .find(|fragment| fragment.fragment_id == 81)
        .unwrap();
    let plan::data_sink::Kind::ChangeStreamRouter(router) =
        source.sink.as_ref().unwrap().kind.as_ref().unwrap()
    else {
        panic!("expected router sink")
    };
    assert_eq!(router.routes[0].input_ordinals, vec![1, 2]);

    let mut arena = ExprArena::default();
    let decoded_root = decode_node(
        source,
        source.root.as_ref().unwrap(),
        &mut arena,
        &NativePlanDecodeContext::default(),
    )
    .unwrap();
    let program = decode_typed_sink_program(source, &decoded_root.layout).unwrap();
    let novarocks_execution::exec::fragment::sink::FragmentSinkProgram::SplitDataStream(split) =
        program
    else {
        panic!("expected split stream")
    };
    assert_eq!(split.sinks().len(), 1);
    assert_eq!(split.sinks()[0].output_columns().len(), 2);
    assert_ne!(
        split.sinks()[0].output_columns()[0],
        split.sinks()[0].output_columns()[1]
    );
}

fn assert_i64_rows(
    actual: &[Vec<Option<i64>>],
    output_slots: &[SlotId],
    expected: &[Vec<Option<i64>>],
) {
    // The bounded consumer already checked each original slot/carrier while
    // its input permit was live; retain the exact row/NULL/order oracle here.
    assert!(actual.iter().all(|row| row.len() == output_slots.len()));
    assert_eq!(actual, expected);
}

#[test]
fn physical_plan_hash_join_reorder_executes_through_terminal_projection() {
    let physical = finish_hash_join_plan(
        91,
        JoinKind::Inner,
        JoinDistribution::Singleton,
        11,
        23,
        HashJoinFixtureOutput::ReorderedPair,
    );
    let physical = freeze_i64_fixture_client_rows(physical);
    let (chunks, slots, root) = encode_decode_execute(&physical);
    let ExecNodeKind::Project(project) = root else {
        panic!("published HashJoin output must be a terminal projection")
    };
    let ExecNodeKind::Join(join) = &project.input.kind else {
        panic!("terminal projection must directly wrap HashJoin")
    };
    assert_eq!(
        join.distribution_mode,
        novarocks_execution::exec::node::join::JoinDistributionMode::Partitioned
    );
    assert_i64_rows(&chunks, &slots, &[vec![Some(23), Some(11)]]);
}

#[test]
fn physical_plan_hash_join_broadcast_projection_decodes_and_executes() {
    let physical = finish_hash_join_plan(
        92,
        JoinKind::Inner,
        JoinDistribution::BroadcastBuild,
        11,
        0,
        HashJoinFixtureOutput::ReorderedPair,
    );
    let physical = freeze_i64_fixture_client_rows(physical);
    let (chunks, slots, root) = encode_decode_execute(&physical);
    let ExecNodeKind::Project(project) = root else {
        panic!("published HashJoin output must be a terminal projection")
    };
    let ExecNodeKind::Join(join) = &project.input.kind else {
        panic!("terminal projection must directly wrap HashJoin")
    };
    assert_eq!(
        join.distribution_mode,
        novarocks_execution::exec::node::join::JoinDistributionMode::Broadcast
    );
    assert_i64_rows(&chunks, &slots, &[]);
}

#[test]
fn physical_plan_left_outer_nullable_projection_executes_after_residual_scope() {
    let physical = finish_hash_join_plan(
        93,
        JoinKind::LeftOuter,
        JoinDistribution::Singleton,
        11,
        23,
        HashJoinFixtureOutput::NullableRight,
    );
    let physical = freeze_i64_fixture_client_rows(physical);
    let (chunks, slots, root) = encode_decode_execute(&physical);
    let ExecNodeKind::Project(project) = root else {
        panic!("published outer-join output must be a terminal projection")
    };
    assert!(matches!(project.input.kind, ExecNodeKind::Join(_)));
    assert_i64_rows(&chunks, &slots, &[vec![None]]);
}

#[test]
fn physical_plan_hash_join_semi_anti_preserved_sides_execute_losslessly() {
    for (id, kind, left, right, output, expected) in [
        (
            94,
            JoinKind::LeftSemi,
            5,
            5,
            HashJoinFixtureOutput::PreservedLeft,
            5,
        ),
        (
            95,
            JoinKind::LeftAnti,
            5,
            7,
            HashJoinFixtureOutput::PreservedLeft,
            5,
        ),
        (
            96,
            JoinKind::RightSemi,
            5,
            5,
            HashJoinFixtureOutput::PreservedRight,
            5,
        ),
        (
            97,
            JoinKind::RightAnti,
            5,
            7,
            HashJoinFixtureOutput::PreservedRight,
            7,
        ),
    ] {
        let physical =
            finish_hash_join_plan(id, kind, JoinDistribution::Singleton, left, right, output);
        let physical = freeze_i64_fixture_client_rows(physical);
        let (chunks, slots, root) = encode_decode_execute(&physical);
        let ExecNodeKind::Project(project) = root else {
            panic!("published semi/anti output must be a terminal projection")
        };
        assert!(matches!(project.input.kind, ExecNodeKind::Join(_)));
        assert_i64_rows(&chunks, &slots, &[vec![Some(expected)]]);
    }
}

#[test]
fn physical_plan_nest_loop_subset_executes_through_terminal_projection() {
    let physical = finish_nest_loop_projection_plan(98);
    let physical = freeze_i64_fixture_client_rows(physical);
    let (chunks, slots, root) = encode_decode_execute(&physical);
    let ExecNodeKind::Project(project) = root else {
        panic!("published NestLoopJoin output must be a terminal projection")
    };
    assert!(matches!(
        project.input.kind,
        ExecNodeKind::NestedLoopJoin(_)
    ));
    assert_i64_rows(&chunks, &slots, &[vec![Some(47)]]);
}

#[test]
fn root_output_contract_crosses_physical_wire_and_static_sink_without_guessing_slots() {
    use novarocks_execution::exec::fragment::sink::FragmentSinkProgram;
    use novarocks_local_program::StaticSinkProgram;
    use novarocks_result_contract::{
        ClientRenderSchema, FrozenRootOutput, NativeRenderType, RenderColumn, RenderField,
        RenderPresentation, RootOutputContract, RootProfileId, ScalarField, ScalarSchema,
        ScalarValueType,
    };
    let physical = finish_nest_loop_projection_plan(99);
    let columns = physical
        .result_port()
        .unwrap()
        .fields
        .iter()
        .enumerate()
        .map(|(ordinal, field)| RenderColumn {
            source_ordinal: ordinal as u32,
            source_slot: None,
            name: field.name.to_string(),
            field: RenderField {
                nullable: field.ty.nullable(),
                native_type: NativeRenderType::SignedInteger(64),
                presentation: RenderPresentation::ScalarText,
            },
        })
        .collect::<Vec<_>>();
    let schema = ClientRenderSchema::try_new(columns.clone(), columns.len()).unwrap();
    let mut wrong = columns;
    wrong[0].field.native_type = NativeRenderType::String;
    assert!(
        physical
            .clone()
            .with_root_output(RootOutputContract::new(
                RootProfileId::V1,
                FrozenRootOutput::ClientRows(
                    ClientRenderSchema::try_new(wrong, schema.columns().len()).unwrap()
                )
            ))
            .is_err()
    );
    let catalog = novarocks_sql::compiler::build_builtin_engine_function_catalog().unwrap();
    for output in [
        FrozenRootOutput::ClientRows(schema),
        FrozenRootOutput::CountOnly,
        FrozenRootOutput::ScalarValue(
            ScalarSchema::try_new(ScalarField {
                nullable: physical.result_port().unwrap().fields[0].ty.nullable(),
                value_type: ScalarValueType::SignedInteger(64),
            })
            .unwrap(),
        ),
    ] {
        let kind = output.kind();
        let physical = physical
            .clone()
            .with_root_output(RootOutputContract::new(RootProfileId::V1, output))
            .unwrap();
        assert!(
            physical
                .clone()
                .with_root_output(RootOutputContract::new(
                    RootProfileId::V1,
                    FrozenRootOutput::CountOnly
                ))
                .is_err()
        );
        let encoded = novarocks_plan_codec::encode_physical_plan_typed_pure(
            &physical,
            &catalog,
            &novarocks_plan_codec::NoPhysicalV1PrivateFacts,
            false,
            compile_control(),
        )
        .unwrap();
        let mut fragment = encoded.fragments[0].clone();
        let mut arena = ExprArena::default();
        let decoded = decode_node(
            &fragment,
            fragment.root.as_ref().unwrap(),
            &mut arena,
            &NativePlanDecodeContext::default(),
        )
        .unwrap();
        let program = decode_typed_sink_program(&fragment, &decoded.layout).unwrap();
        let FragmentSinkProgram::RootResult(contract) = &program else {
            panic!("expected frozen root");
        };
        assert_eq!(contract.kind(), kind);
        if let FrozenRootOutput::ScalarValue(schema) = contract.output() {
            let [slot] = decoded.layout.order() else {
                panic!("typed scalar must retain exactly one actual Native slot");
            };
            assert_eq!(schema.source_ordinal(), 0);
            assert_eq!(schema.source_slot(), Some(slot.as_u32()));
            assert_eq!(
                schema.field().value_type,
                ScalarValueType::SignedInteger(64)
            );
            assert_eq!(
                schema.field().nullable,
                physical.result_port().unwrap().fields[0].ty.nullable()
            );
        }
        let StaticSinkProgram::RootResult(retained) = program.into_static().unwrap() else {
            panic!("expected pure root sink");
        };
        assert_eq!(retained.kind(), kind);
        let prepare = |fragment: &plan::PlanFragment| {
            let mut arena = ExprArena::default();
            let decoded = decode_node(
                fragment,
                fragment.root.as_ref().unwrap(),
                &mut arena,
                &NativePlanDecodeContext::default(),
            )
            .unwrap();
            let sink = decode_typed_sink_program(fragment, &decoded.layout)
                .unwrap()
                .into_static()
                .unwrap();
            let plan = ExecPlanBuilder::new(arena, decoded.node).finish().unwrap();
            try_lower_fixture_with_actual_creator(plan, sink)
        };
        let (local, _) = prepare(&fragment).unwrap_or_else(|error| {
            panic!("actual {kind:?} root contract lowering failed: {error:?}")
        });
        let Some(StaticSinkProgram::RootResult(local_contract)) = local.sink() else {
            panic!("lowering lost the immutable root output contract");
        };
        assert_eq!(local_contract.as_ref(), retained.as_ref());
        if let Some(plan::data_sink::Kind::RootResult(wire)) =
            fragment.sink.as_ref().unwrap().kind.as_ref()
            && wire.client_schema.is_some()
        {
            for nullable_only in [false, true] {
                let mut corrupt = fragment.clone();
                let Some(plan::data_sink::Kind::RootResult(wire)) =
                    corrupt.sink.as_mut().unwrap().kind.as_mut()
                else {
                    unreachable!();
                };
                let field = &mut wire.client_schema.as_mut().unwrap().field_nodes[0];
                if nullable_only {
                    field.nullable = !field.nullable;
                } else {
                    field.native_type.as_mut().unwrap().kind =
                        novarocks_proto_models::result::RenderTypeKind::String as i32;
                    field.native_type.as_mut().unwrap().bits = 0;
                }
                assert!(
                    prepare(&corrupt).is_err(),
                    "wrong render type or nullable must fail before installation"
                );
            }
        }

        if let Some(plan::data_sink::Kind::RootResult(wire)) =
            fragment.sink.as_mut().unwrap().kind.as_mut()
            && let Some(schema) = wire.client_schema.as_mut()
        {
            schema.columns[0].source_slot = Some(u32::MAX);
            assert!(decode_typed_sink_program(&fragment, &decoded.layout).is_err());
        }
        if let Some(plan::data_sink::Kind::RootResult(wire)) =
            fragment.sink.as_ref().unwrap().kind.as_ref()
            && wire.scalar_schema.is_some()
        {
            for nullable_only in [false, true] {
                let mut corrupt = fragment.clone();
                let Some(plan::data_sink::Kind::RootResult(wire)) =
                    corrupt.sink.as_mut().unwrap().kind.as_mut()
                else {
                    unreachable!()
                };
                let field = &mut wire.scalar_schema.as_mut().unwrap().field_nodes[0];
                if nullable_only {
                    field.nullable = !field.nullable;
                } else {
                    field.value_type.as_mut().unwrap().bits = 32;
                }
                assert!(
                    prepare(&corrupt).is_err(),
                    "scalar type/nullability must match the real LocalProgram source"
                );
            }
            let mut corrupt = fragment.clone();
            let Some(plan::data_sink::Kind::RootResult(wire)) =
                corrupt.sink.as_mut().unwrap().kind.as_mut()
            else {
                unreachable!()
            };
            wire.scalar_schema.as_mut().unwrap().source_slot = Some(u32::MAX);
            assert!(decode_typed_sink_program(&corrupt, &decoded.layout).is_err());
        }
    }
}

fn empty_storage_result_plan(fragment_id: u32, types: &[ValueType]) -> PhysicalPlan {
    empty_storage_result_plan_with_scalar_proof(fragment_id, types, true)
}

fn empty_storage_result_plan_with_scalar_proof(
    fragment_id: u32,
    types: &[ValueType],
    include_scalar_proof: bool,
) -> PhysicalPlan {
    let fragment_id = FragmentId::new(fragment_id);
    let mut builder = FragmentBuilder::new(fragment_id);
    let root = builder.reserve_node_id().unwrap();
    let outputs = types
        .iter()
        .enumerate()
        .map(|(ordinal, ty)| {
            builder
                .add_value(
                    ty.clone(),
                    ValueOrigin::NodeOutput {
                        node: root,
                        output_ordinal: ordinal as u32,
                    },
                )
                .unwrap()
        })
        .collect::<Box<[_]>>();
    builder
        .insert_node_unchecked(PhysicalNode {
            id: root,
            inputs: Box::default(),
            required_inputs: Box::default(),
            output_properties: singleton_properties(),
            output: OutputPort {
                node: root,
                columns: outputs.clone(),
            },
            kind: NodeKind::Values {
                rows: Box::default(),
            },
        })
        .unwrap();
    finish_result_plan_with_scalar_proof(builder, fragment_id, root, outputs, include_scalar_proof)
}

// These hand-built no-I/O fixtures explicitly declare their plain value
// identities. They do not stand in for SQL's declared domain authority.
fn fixture_plain_scalar_schema(ty: &ValueType) -> Option<novarocks_result_contract::ScalarSchema> {
    use arrow::datatypes::TimeUnit;
    use novarocks_result_contract::{
        ScalarField, ScalarSchema, ScalarTimestampUnit as U, ScalarValueType as S,
    };
    let value_type = match ty.data_type() {
        DataType::Null => S::Null,
        DataType::Boolean => S::Boolean,
        DataType::Int8 => S::SignedInteger(8),
        DataType::Int16 => S::SignedInteger(16),
        DataType::Int32 => S::SignedInteger(32),
        DataType::Int64 => S::SignedInteger(64),
        DataType::Float32 => S::Float32,
        DataType::Float64 => S::Float64,
        DataType::Utf8 => S::String,
        DataType::Binary => S::Binary,
        DataType::Date32 => S::Date,
        DataType::Time64(TimeUnit::Microsecond) => S::TimeMicros,
        DataType::Decimal128(p, s) | DataType::Decimal256(p, s) if *s >= 0 => S::Decimal {
            bits: if matches!(ty.data_type(), DataType::Decimal128(..)) {
                128
            } else {
                256
            },
            precision: *p,
            scale: *s as u8,
        },
        DataType::Timestamp(unit @ (TimeUnit::Microsecond | TimeUnit::Nanosecond), zone) => {
            S::Timestamp {
                unit: if *unit == TimeUnit::Microsecond {
                    U::Microsecond
                } else {
                    U::Nanosecond
                },
                timezone: zone.as_ref().map(|zone| zone.to_string()),
            }
        }
        _ => return None,
    };
    ScalarSchema::try_new(ScalarField {
        nullable: ty.nullable(),
        value_type,
    })
    .ok()
}

#[test]
fn scalar_physical_root_refuses_missing_independent_semantic_proof() {
    let physical = empty_storage_result_plan_with_scalar_proof(
        108,
        &[ValueType::new(DataType::Int64, false)],
        false,
    );
    assert!(
        physical
            .with_root_output(scalar_root_contract(
                novarocks_result_contract::ScalarField {
                    nullable: false,
                    value_type: novarocks_result_contract::ScalarValueType::SignedInteger(64),
                }
            ))
            .is_err()
    );
}

fn scalar_root_contract(
    field: novarocks_result_contract::ScalarField,
) -> novarocks_result_contract::RootOutputContract {
    use novarocks_result_contract::{
        FrozenRootOutput, RootOutputContract, RootProfileId, ScalarSchema,
    };
    RootOutputContract::new(
        RootProfileId::V1,
        FrozenRootOutput::ScalarValue(ScalarSchema::try_new(field).unwrap()),
    )
}

#[test]
fn scalar_local_program_rejects_naked_domain_without_wire_guard() {
    use novarocks_local_program::{LocalProgramError, LocalProgramGraph, StaticSinkProgram};
    use novarocks_result_contract::{
        FrozenRootOutput, InternalResultDomain, RootOutputContract, RootProfileId, ScalarField,
        ScalarValueType,
    };
    let physical = empty_storage_result_plan(109, &[ValueType::new(DataType::Int64, false)])
        .with_root_output(scalar_root_contract(ScalarField {
            nullable: false,
            value_type: ScalarValueType::SignedInteger(64),
        }))
        .unwrap();
    let catalog = novarocks_sql::compiler::build_builtin_engine_function_catalog().unwrap();
    let encoded = novarocks_plan_codec::encode_physical_plan_typed_pure(
        &physical,
        &catalog,
        &novarocks_plan_codec::NoPhysicalV1PrivateFacts,
        false,
        compile_control(),
    )
    .unwrap();
    let fragment = &encoded.fragments[0];
    let mut arena = ExprArena::default();
    let decoded = decode_node(
        fragment,
        fragment.root.as_ref().unwrap(),
        &mut arena,
        &NativePlanDecodeContext::default(),
    )
    .unwrap();
    let sink = decode_typed_sink_program(fragment, &decoded.layout)
        .unwrap()
        .into_static()
        .unwrap();
    let plan = ExecPlanBuilder::new(arena, decoded.node).finish().unwrap();
    let (local, _) = lower_fixture_with_actual_creator(plan, sink);
    let rebuild = |sink| {
        LocalProgramGraph::try_new_with_sink(
            local.nodes().to_vec(),
            local.root(),
            Arc::clone(local.expressions()),
            local.profile(),
            local.requirements().clone(),
            Some(sink),
        )
    };
    rebuild(local.sink().unwrap().clone()).unwrap();
    // Only the sink changes. No wire decode or slot binding can reject this
    // malformed purpose before the real LocalProgram validation runs.
    let naked = StaticSinkProgram::RootResult(Arc::new(RootOutputContract::new(
        RootProfileId::V1,
        FrozenRootOutput::InternalFacts(InternalResultDomain::ScalarValueV1),
    )));
    assert!(matches!(
        rebuild(naked),
        Err(LocalProgramError::InvalidSink)
    ));
}

#[test]
fn scalar_physical_root_requires_one_exact_output_occurrence() {
    use novarocks_result_contract::{ScalarField, ScalarValueType};
    for width in [0, 1, 2] {
        let physical = empty_storage_result_plan(
            100 + width,
            &vec![ValueType::new(DataType::Int64, false); width as usize],
        );
        let actual = physical.with_root_output(scalar_root_contract(ScalarField {
            nullable: false,
            value_type: ScalarValueType::SignedInteger(64),
        }));
        assert_eq!(
            actual.is_ok(),
            width == 1,
            "scalar root output width {width}"
        );
    }
}

#[test]
fn scalar_physical_root_checks_width_nullability_temporal_and_decimal_metadata() {
    use arrow::datatypes::TimeUnit;
    use novarocks_result_contract::{
        ScalarField, ScalarTimestampUnit as Unit, ScalarValueType as Type,
    };
    let cases = [
        (
            DataType::Int64,
            false,
            Type::SignedInteger(64),
            false,
            Type::SignedInteger(32),
        ),
        (
            DataType::Int64,
            false,
            Type::SignedInteger(64),
            true,
            Type::SignedInteger(64),
        ),
        (
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            true,
            Type::Timestamp {
                unit: Unit::Microsecond,
                timezone: Some("UTC".into()),
            },
            true,
            Type::Timestamp {
                unit: Unit::Microsecond,
                timezone: Some("Asia/Shanghai".into()),
            },
        ),
        (
            DataType::Timestamp(TimeUnit::Microsecond, None),
            true,
            Type::Timestamp {
                unit: Unit::Microsecond,
                timezone: None,
            },
            true,
            Type::Timestamp {
                unit: Unit::Nanosecond,
                timezone: None,
            },
        ),
        (
            DataType::Decimal128(12, 4),
            false,
            Type::Decimal {
                bits: 128,
                precision: 12,
                scale: 4,
            },
            false,
            Type::Decimal {
                bits: 128,
                precision: 13,
                scale: 4,
            },
        ),
        (
            DataType::Decimal128(12, 4),
            false,
            Type::Decimal {
                bits: 128,
                precision: 12,
                scale: 4,
            },
            false,
            Type::Decimal {
                bits: 128,
                precision: 12,
                scale: 3,
            },
        ),
        (
            DataType::Decimal128(12, 4),
            false,
            Type::Decimal {
                bits: 128,
                precision: 12,
                scale: 4,
            },
            false,
            Type::Decimal {
                bits: 256,
                precision: 12,
                scale: 4,
            },
        ),
        (
            DataType::Decimal256(70, 6),
            true,
            Type::Decimal {
                bits: 256,
                precision: 70,
                scale: 6,
            },
            true,
            Type::Decimal {
                bits: 128,
                precision: 38,
                scale: 6,
            },
        ),
    ];
    for (index, (storage, nullable, correct, wrong_nullable, wrong)) in
        cases.into_iter().enumerate()
    {
        let physical =
            empty_storage_result_plan(110 + index as u32, &[ValueType::new(storage, nullable)]);
        assert!(
            physical
                .clone()
                .with_root_output(scalar_root_contract(ScalarField {
                    nullable,
                    value_type: correct
                }))
                .is_ok()
        );
        assert!(
            physical
                .with_root_output(scalar_root_contract(ScalarField {
                    nullable: wrong_nullable,
                    value_type: wrong
                }))
                .is_err(),
            "mismatch case {index}"
        );
    }
}

#[test]
fn scalar_native_sink_decode_requires_schema_only_for_exact_scalar_purpose_and_slot() {
    use novarocks_execution::exec::chunk::SlotLayout;
    use novarocks_result_contract::{RootOutputKind, ScalarField, ScalarValueType};
    let physical = empty_storage_result_plan(120, &[ValueType::new(DataType::Int64, false)])
        .with_root_output(scalar_root_contract(ScalarField {
            nullable: false,
            value_type: ScalarValueType::SignedInteger(64),
        }))
        .unwrap();
    let catalog = novarocks_sql::compiler::build_builtin_engine_function_catalog().unwrap();
    let encoded = novarocks_plan_codec::encode_physical_plan_typed_pure(
        &physical,
        &catalog,
        &novarocks_plan_codec::NoPhysicalV1PrivateFacts,
        false,
        compile_control(),
    )
    .unwrap();
    let fragment = &encoded.fragments[0];
    let mut arena = ExprArena::default();
    let decoded = decode_node(
        fragment,
        fragment.root.as_ref().unwrap(),
        &mut arena,
        &NativePlanDecodeContext::default(),
    )
    .unwrap();
    assert!(decode_typed_sink_program(fragment, &decoded.layout).is_ok());
    let mut missing = fragment.clone();
    let Some(plan::data_sink::Kind::RootResult(wire)) =
        missing.sink.as_mut().unwrap().kind.as_mut()
    else {
        unreachable!()
    };
    wire.scalar_schema = None;
    assert!(
        decode_typed_sink_program(&missing, &decoded.layout).is_err(),
        "naked ScalarValueV1 has no semantic schema"
    );
    for kind in [
        RootOutputKind::CountOnly,
        RootOutputKind::InternalFacts(
            novarocks_result_contract::InternalResultDomain::StatisticsArtifactV1,
        ),
    ] {
        let mut wrong = fragment.clone();
        let Some(plan::data_sink::Kind::RootResult(wire)) =
            wrong.sink.as_mut().unwrap().kind.as_mut()
        else {
            unreachable!()
        };
        wire.output_kind = Some(novarocks_proto_codec::root_result::encode_kind(kind));
        assert!(
            decode_typed_sink_program(&wrong, &decoded.layout).is_err(),
            "extra scalar schema on {kind:?}"
        );
    }
    let mut both = fragment.clone();
    let Some(plan::data_sink::Kind::RootResult(wire)) = both.sink.as_mut().unwrap().kind.as_mut()
    else {
        unreachable!()
    };
    wire.client_schema = Some(Default::default());
    assert!(
        decode_typed_sink_program(&both, &decoded.layout).is_err(),
        "scalar must not also carry a client schema"
    );
    for slot in [None, Some(u32::MAX)] {
        let mut wrong = fragment.clone();
        let Some(plan::data_sink::Kind::RootResult(wire)) =
            wrong.sink.as_mut().unwrap().kind.as_mut()
        else {
            unreachable!()
        };
        wire.scalar_schema.as_mut().unwrap().source_slot = slot;
        assert!(
            decode_typed_sink_program(&wrong, &decoded.layout).is_err(),
            "scalar source slot {slot:?}"
        );
    }
    let slot = decoded.layout.order()[0];
    for layout in [
        SlotLayout::for_slots([]),
        SlotLayout::for_slots([slot, SlotId::new(slot.as_u32() ^ 1)]),
    ] {
        assert!(
            decode_typed_sink_program(fragment, &layout).is_err(),
            "scalar input layout must have exactly one slot"
        );
    }
}

#[test]
fn scalar_domain_without_semantic_schema_is_not_a_validated_physical_plan() {
    use novarocks_result_contract::{
        FrozenRootOutput, InternalResultDomain, RootOutputContract, RootProfileId,
    };
    let physical = empty_storage_result_plan(241, &[ValueType::new(DataType::Int64, false)]);
    let bare = RootOutputContract::new(
        RootProfileId::V1,
        FrozenRootOutput::InternalFacts(InternalResultDomain::ScalarValueV1),
    );
    assert!(physical.with_root_output(bare).is_err());
}

// These fixtures explicitly own the root's semantic fact. Their identical
// names never establish a domain, and they do not enable the Scalar Host.
fn exact_domain_result_plan(
    fragment_id: u32,
    ty: ValueType,
    domain: novarocks_physical_plan::ResultValueDomain,
    scalar: Option<novarocks_result_contract::ScalarValueType>,
    passthrough_root: bool,
) -> Result<PhysicalPlan, String> {
    let fragment_id = FragmentId::new(fragment_id);
    let mut builder = FragmentBuilder::new(fragment_id);
    let source = builder.reserve_node_id().unwrap();
    let value = builder
        .add_value(
            ty.clone(),
            ValueOrigin::NodeOutput {
                node: source,
                output_ordinal: 0,
            },
        )
        .unwrap();
    builder
        .insert_node_unchecked(PhysicalNode {
            id: source,
            inputs: Box::default(),
            required_inputs: Box::default(),
            output_properties: singleton_properties(),
            output: OutputPort {
                node: source,
                columns: Box::from([value]),
            },
            kind: NodeKind::Values {
                rows: Box::default(),
            },
        })
        .unwrap();
    let root = if passthrough_root {
        let root = builder.reserve_node_id().unwrap();
        builder
            .insert_node_unchecked(PhysicalNode {
                id: root,
                inputs: Box::from([source]),
                required_inputs: Box::from([singleton_properties()]),
                output_properties: singleton_properties(),
                output: OutputPort {
                    node: root,
                    columns: Box::from([value]),
                },
                kind: NodeKind::Limit {
                    limit: Some(1),
                    offset: 0,
                },
            })
            .unwrap();
        root
    } else {
        source
    };
    let fragment = builder
        .finish_definition(
            root,
            FragmentSink::Result,
            PipelineDopDomain {
                min: 1,
                max: 1,
                requires_power_of_two: false,
            },
        )
        .unwrap();
    let scalar_schema = scalar.map(|value_type| {
        novarocks_result_contract::ScalarSchema::try_new(novarocks_result_contract::ScalarField {
            nullable: ty.nullable(),
            value_type,
        })
        .unwrap()
    });
    let mut plan = PlanBuilder::new(PlanVersionId::try_new([fragment_id.get() as u8; 16]).unwrap());
    plan.add_fragment(fragment).unwrap();
    plan.set_result_port(ResultPort {
        scalar_schema,
        fragment: fragment_id,
        output: OutputPort {
            node: root,
            columns: Box::from([value]),
        },
        fields: Box::from([ResultField {
            domain,
            // Same misleading name for every plain and opaque test case.
            name: "json_hll_bitmap_state".into(),
            alias: Some("state".into()),
            value,
            ty,
        }]),
    })
    .unwrap();
    plan.finish().map_err(|error| error.to_string())
}

#[test]
fn complete_root_domains_survive_wire_field_chunk_and_local_program() {
    use novarocks_physical_plan::ResultValueDomain as D;
    use novarocks_proto_models::common::PrimitiveType as P;
    use novarocks_result_contract::{
        FrozenRootOutput, RootOutputContract, RootProfileId, ScalarOpaqueType as O,
        ScalarValueType as S,
    };
    use novarocks_type_contract::ValueLogicalType;
    use novarocks_types::logical::{LogicalType as L, logical_type_of_field};
    let cases = [
        (D::Plain, DataType::Utf8, S::String, P::Varchar, None),
        (D::Plain, DataType::Binary, S::Binary, P::Varbinary, None),
        (D::Json, DataType::Utf8, S::Json, P::Json, Some(L::Json)),
        (
            D::Hll,
            DataType::Binary,
            S::Opaque(O::Hll),
            P::Hll,
            Some(L::Hll),
        ),
        (
            D::Bitmap,
            DataType::Binary,
            S::Opaque(O::Bitmap),
            P::Bitmap,
            Some(L::Bitmap),
        ),
        (
            D::Object,
            DataType::Binary,
            S::Opaque(O::Object),
            P::Object,
            Some(L::Object),
        ),
        (
            D::Percentile,
            DataType::Binary,
            S::Opaque(O::Percentile),
            P::Percentile,
            Some(L::Percentile),
        ),
        (
            D::Variant,
            DataType::LargeBinary,
            S::Variant,
            P::Variant,
            None,
        ),
    ];
    let catalog = novarocks_sql::compiler::build_builtin_engine_function_catalog().unwrap();
    for (index, (domain, storage, scalar, _primitive, _logical)) in cases.into_iter().enumerate() {
        // The actual scalar fixture declaration authors its value independently
        // of the requested root presentation and misleading output name.
        let declared_domain = match &scalar {
            S::String | S::Binary => ValueLogicalType::Physical,
            S::Json => ValueLogicalType::Json,
            S::Variant => ValueLogicalType::Variant,
            S::Opaque(O::Hll) => ValueLogicalType::Hll,
            S::Opaque(O::Bitmap) => ValueLogicalType::Bitmap,
            S::Opaque(O::Object) => ValueLogicalType::Object,
            S::Opaque(O::Percentile) => ValueLogicalType::Percentile,
            _ => unreachable!("fixture declares these scalar domains"),
        };
        let declaration =
            ValueType::try_with_logical_type(storage.clone(), false, declared_domain).unwrap();
        let physical = exact_domain_result_plan(
            150 + index as u32,
            declaration.clone(),
            domain,
            Some(scalar.clone()),
            false,
        )
        .unwrap();
        let proof = physical
            .result_port()
            .unwrap()
            .scalar_schema
            .clone()
            .unwrap();
        let physical = physical
            .with_root_output(RootOutputContract::new(
                RootProfileId::V1,
                FrozenRootOutput::ScalarValue(proof),
            ))
            .unwrap();
        let encoded = novarocks_plan_codec::encode_physical_plan_typed_pure(
            &physical,
            &catalog,
            &novarocks_plan_codec::NoPhysicalV1PrivateFacts,
            false,
            compile_control(),
        )
        .unwrap();
        let fragment = &encoded.fragments[0];
        let column = &fragment.output_columns[0];
        assert_eq!(column.name, "state");
        let desc = column.r#type.as_ref().unwrap();
        assert!(desc.value_binding_id.is_some());
        let output = with_fragment_types(fragment, |decoder| {
            let value = decoder
                .value_type(
                    desc,
                    novarocks_proto_codec::FieldPath::root("output").field("type"),
                )?
                .expect("complete binding");
            assert_eq!(value, declaration);
            decoder.output_layout(
                &fragment.output_columns,
                novarocks_proto_codec::FieldPath::root("output_columns"),
            )
        })
        .unwrap();
        let chunk_schema = output.chunk_schema();
        assert_eq!(
            chunk_schema.slots()[0].field_schema().logical_type(),
            declaration.value_type().domain()
        );
        assert_eq!(chunk_schema.slots()[0].field().data_type(), &storage);
        assert_eq!(chunk_schema.slots()[0].field().metadata().capacity(), 0);
        assert_eq!(logical_type_of_field(chunk_schema.slots()[0].field()), None);
        let mut arena = ExprArena::default();
        let decoded = decode_node(
            &fragment,
            fragment.root.as_ref().unwrap(),
            &mut arena,
            &NativePlanDecodeContext::default(),
        )
        .unwrap();
        let sink = decode_typed_sink_program(fragment, &decoded.layout)
            .unwrap()
            .into_static()
            .unwrap();
        let novarocks_local_program::StaticSinkProgram::RootResult(contract) = &sink else {
            panic!("expected root contract")
        };
        let FrozenRootOutput::ScalarValue(bound) = contract.output() else {
            panic!("expected scalar contract")
        };
        assert_eq!(bound.source_slot(), Some(column.column_id));
        assert_eq!(bound.field().value_type, scalar);
        // Actual leaf validation consumes the independently decoded canonical
        // field/cache and exact wire slot. This fixture grants no source memory.
        let array: arrow::array::ArrayRef = match storage {
            DataType::Utf8 => Arc::new(arrow::array::StringArray::from(vec!["null"])),
            DataType::Binary => Arc::new(arrow::array::BinaryArray::from(vec![&b"x"[..]])),
            DataType::LargeBinary => {
                Arc::new(arrow::array::LargeBinaryArray::from(vec![&b"x"[..]]))
            }
            _ => unreachable!(),
        };
        let batch =
            arrow::record_batch::RecordBatch::try_new(chunk_schema.arrow_schema_ref(), vec![array])
                .unwrap();
        let chunk = Chunk::try_new_with_chunk_schema(batch, chunk_schema).unwrap();
        let mut scalar = crate::root_scalar_leaf_codec::NativeScalarLeafEncoder::try_begin(
            &chunk,
            Arc::new(bound.clone()),
            crate::root_scalar_leaf_codec::NativeScalarLeafEncoder::scratch_capacity_bytes(),
        )
        .unwrap();
        assert!(scalar.encoded_len().is_none());
        let validation = scalar.step(&mut []).unwrap();
        assert_eq!(validation.emitted_bytes, 0);
        assert_eq!(validation.completed_rows, 0);
        assert_eq!(
            validation.status,
            novarocks_result_render::RenderTurnStatus::Yielded
        );
        assert!(scalar.encoded_len().is_some());
        let exec = ExecPlanBuilder::new(arena, decoded.node).finish().unwrap();
        let (local, _) = lower_fixture_with_actual_creator(exec, sink);
        let layout = local.nodes()[local.root().index()].output_layout();
        assert_eq!(
            layout.value_binding_at(0).unwrap(),
            declaration.typed_binding()
        );
        assert_eq!(
            logical_type_of_field(&layout.schema().fields()[0]),
            None,
            "{domain:?}"
        );
        assert_eq!(layout.schema().fields()[0].metadata().capacity(), 0);
    }
}

#[test]
fn exact_root_domain_refuses_inconsistent_storage_without_offset_adaptation() {
    use novarocks_physical_plan::ResultValueDomain as D;
    for (domain, storage) in [
        (D::Json, DataType::Binary),
        (D::Hll, DataType::Utf8),
        (D::Bitmap, DataType::Int64),
        (D::Object, DataType::Utf8),
        (D::Percentile, DataType::Int32),
        (D::Variant, DataType::Binary),
    ] {
        assert!(
            exact_domain_result_plan(160, ValueType::new(storage, false), domain, None, false)
                .is_err(),
            "{domain:?}"
        );
    }
    let catalog = novarocks_sql::compiler::build_builtin_engine_function_catalog().unwrap();
    // Coarse SQL domain compatibility does not authorize changing the Native
    // physical carrier's offset width during descriptor encoding.
    for (domain, storage) in [
        (D::Json, DataType::LargeUtf8),
        (D::Hll, DataType::LargeBinary),
        (D::Bitmap, DataType::LargeBinary),
        (D::Object, DataType::LargeBinary),
        (D::Percentile, DataType::LargeBinary),
    ] {
        let physical =
            exact_domain_result_plan(161, ValueType::new(storage, false), domain, None, false)
                .unwrap();
        assert!(
            novarocks_plan_codec::encode_physical_plan_typed_pure(
                &physical,
                &catalog,
                &novarocks_plan_codec::NoPhysicalV1PrivateFacts,
                false,
                compile_control(),
            )
            .is_err(),
            "{domain:?}"
        );
    }
}

#[test]
fn root_domain_does_not_flow_to_an_intermediate_by_name_or_shared_value() {
    use novarocks_physical_plan::ResultValueDomain as D;
    use novarocks_type_contract::ValueLogicalType;
    let physical = exact_domain_result_plan(
        162,
        ValueType::try_with_logical_type(DataType::Binary, false, ValueLogicalType::Bitmap)
            .unwrap(),
        D::Bitmap,
        None,
        true,
    )
    .unwrap();
    let catalog = novarocks_sql::compiler::build_builtin_engine_function_catalog().unwrap();
    let encoded = novarocks_plan_codec::encode_physical_plan_typed_pure(
        &physical,
        &catalog,
        &novarocks_plan_codec::NoPhysicalV1PrivateFacts,
        false,
        compile_control(),
    )
    .unwrap();
    let fragment = &encoded.fragments[0];
    let root = fragment.root.as_ref().unwrap();
    let physical_columns = |node: &plan::DistributedNode| {
        let plan::distributed_node::Payload::Physical(payload) = node.payload.as_ref().unwrap()
        else {
            panic!("expected physical node")
        };
        payload.output_columns.clone()
    };
    for column in [
        &fragment.output_columns[0],
        &physical_columns(root)[0],
        &physical_columns(&root.children[0])[0],
    ] {
        with_fragment_types(fragment, |decoder| {
            let value = decoder
                .value_type(
                    column.r#type.as_ref().unwrap(),
                    novarocks_proto_codec::FieldPath::root("column"),
                )?
                .unwrap();
            assert_eq!(value.value_type().domain(), Some(ValueLogicalType::Bitmap));
            assert_eq!(value.data_type(), &DataType::Binary);
            Ok(())
        })
        .unwrap();
    }
    // The root presentation is independent of the declaration carried by every
    // occurrence of this actual value, including its intermediate use.
    let wrong = exact_domain_result_plan(
        164,
        ValueType::new(DataType::Binary, false),
        D::Bitmap,
        None,
        true,
    )
    .unwrap();
    assert!(
        novarocks_plan_codec::encode_physical_plan_typed_pure(
            &wrong,
            &catalog,
            &novarocks_plan_codec::NoPhysicalV1PrivateFacts,
            false,
            compile_control()
        )
        .is_err()
    );
    assert_eq!(
        physical_columns(root)[0].column_id,
        physical_columns(&root.children[0])[0].column_id
    );
}

#[test]
fn exact_aggregate_root_occurrence_has_the_same_domain_in_its_own_dto() {
    use novarocks_physical_plan::{AggregateGrouping, ResultValueDomain};
    use novarocks_type_contract::ValueLogicalType;
    let id = FragmentId::new(163);
    let mut builder = FragmentBuilder::new(id);
    let source = builder.reserve_node_id().unwrap();
    let ty = ValueType::try_with_logical_type(DataType::Binary, false, ValueLogicalType::Bitmap)
        .unwrap();
    let source_value = builder
        .add_value(
            ty.clone(),
            ValueOrigin::NodeOutput {
                node: source,
                output_ordinal: 0,
            },
        )
        .unwrap();
    builder
        .insert_node_unchecked(PhysicalNode {
            id: source,
            inputs: Box::default(),
            required_inputs: Box::default(),
            output_properties: singleton_properties(),
            output: OutputPort {
                node: source,
                columns: Box::from([source_value]),
            },
            kind: NodeKind::Values {
                rows: Box::default(),
            },
        })
        .unwrap();
    let root = builder.reserve_node_id().unwrap();
    let group = builder
        .add_expression(root, ty.clone(), ExprKind::Value(source_value))
        .unwrap();
    let value = source_value;
    builder
        .insert_node_unchecked(PhysicalNode {
            id: root,
            inputs: Box::from([source]),
            required_inputs: Box::from([singleton_properties()]),
            output_properties: singleton_properties(),
            output: OutputPort {
                node: root,
                columns: Box::from([value]),
            },
            kind: NodeKind::Aggregate {
                group_by: Box::from([(group, value)]),
                calls: Box::default(),
                grouping: AggregateGrouping::Complete,
            },
        })
        .unwrap();
    let fragment = builder
        .finish_definition(
            root,
            FragmentSink::Result,
            PipelineDopDomain {
                min: 1,
                max: 1,
                requires_power_of_two: false,
            },
        )
        .unwrap();
    let mut builder = PlanBuilder::new(PlanVersionId::try_new([163; 16]).unwrap());
    builder.add_fragment(fragment).unwrap();
    builder
        .set_result_port(ResultPort {
            scalar_schema: None,
            fragment: id,
            output: OutputPort {
                node: root,
                columns: Box::from([value]),
            },
            fields: Box::from([ResultField {
                domain: ResultValueDomain::Bitmap,
                name: "grouped".into(),
                alias: None,
                value,
                ty,
            }]),
        })
        .unwrap();
    let physical = builder.finish().unwrap();
    let catalog = novarocks_sql::compiler::build_builtin_engine_function_catalog().unwrap();
    let encoded = novarocks_plan_codec::encode_physical_plan_typed_pure(
        &physical,
        &catalog,
        &novarocks_plan_codec::NoPhysicalV1PrivateFacts,
        false,
        compile_control(),
    )
    .unwrap();
    let root = encoded.fragments[0].root.as_ref().unwrap();
    let plan::distributed_node::Payload::Physical(payload) = root.payload.as_ref().unwrap() else {
        panic!("expected physical node")
    };
    let plan::plan_node::Kind::HashAggregate(aggregate) = payload.kind.as_ref().unwrap() else {
        panic!("expected aggregate")
    };
    for column in [
        &payload.output_columns[0],
        &aggregate.output_layout.as_ref().unwrap().group_key_columns[0],
    ] {
        with_fragment_types(&encoded.fragments[0], |decoder| {
            let value = decoder
                .value_type(
                    column.r#type.as_ref().unwrap(),
                    novarocks_proto_codec::FieldPath::root("group"),
                )?
                .unwrap();
            assert_eq!(value, physical.result_port().unwrap().fields[0].ty);
            Ok(())
        })
        .unwrap();
    }
    assert_eq!(
        aggregate.output_layout.as_ref().unwrap().group_key_columns[0].column_id,
        payload.output_columns[0].column_id
    );
    let mut arena = ExprArena::default();
    let decoded = decode_node(
        &encoded.fragments[0],
        root,
        &mut arena,
        &NativePlanDecodeContext::default(),
    )
    .unwrap();
    let ExecNodeKind::Aggregate(aggregation) = &decoded.node.kind else {
        panic!("expected aggregation")
    };
    assert_eq!(
        aggregation.output_chunk_schema.slots()[0]
            .field_schema()
            .logical_type(),
        Some(ValueLogicalType::Bitmap)
    );
    assert_eq!(
        aggregation.output_chunk_schema.slots()[0]
            .field()
            .metadata()
            .capacity(),
        0
    );
}

#[path = "physical_v1_roundtrip/default_scalar_tests.rs"]
mod default_scalar_tests;
