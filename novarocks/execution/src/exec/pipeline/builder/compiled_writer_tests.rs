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

//! `INSERT INTO t VALUES (1, 'a'), (2, 'b')` without statistics, run end to
//! end in process through compiled operators only: the writer fragment's
//! compiled Values feed the compiled TableWriter, whose writer relation the
//! compiled stream sink encodes into real exchange frames; a loopback
//! transmitter delivers them to the finish fragment's positional receiver,
//! and the compiled TableFinish publishes the Root relation to its Result
//! sink. The provider is a recording write execution, not an installed one.

use std::collections::BTreeMap;
use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use arrow::array::{Array, BinaryArray, Int8Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use novarocks_connector_contract::{
    ConnectorCodecCategory, ConnectorCodecRevision, ConnectorEncodedPayload,
    ConnectorEnvelopeHeader, ConnectorError, ConnectorWriteFieldBinding, ConnectorWriteFieldToken,
    ConnectorWriteInputShape, ConnectorWriteRecipeCompiler, ConnectorWriteRecipeDraft,
    PureProviderCompileError, PureProviderManifestEntry, PureProviderProgramCatalog,
    PureProviderProgramDefinition,
};
use novarocks_functions::ConstantPolicy;
use novarocks_local_compiler::{
    LocalCompileOptions, compile_fragment, validate_fragment_providers,
};
use novarocks_local_program::{KernelAbiVersion, LocalProgram, ProgramNodeId, ProgramNodeKind};
use novarocks_physical_plan::{
    Distribution, Edge, EdgeDestination, EdgeId, EdgeKind, EdgePartitioning, EdgeSource, ExprKind,
    Fragment, FragmentBuilder, FragmentId, FragmentPackage, FragmentPackageAdmission, FragmentSink,
    FrozenFragmentCalls, FrozenFragmentPruning, LiteralValue, NodeId, NodeKind, OutputPort,
    PhysicalExpressionRoots, PhysicalNode, PhysicalPlan, PhysicalProperties, PhysicalRootUses,
    PipelineDopDomain, PlanBuilder, PlanLimits, PlanVersionId, PropertyProofProjectionLimits,
    ROOT_WRITE_RESULT_SCHEMA_REVISION, ResultField, ResultPort, RowMultiplicity, ValueOrigin,
    WRITER_MULTIPLEX_SCHEMA_REVISION, WriteTargetOrdinal, WriterDerivedKind, WriterFinishSpec,
    WriterRelationField, WriterRelationFieldRole, WriterRelationSchema, WriterTarget,
    WriterTargetField, extract_fragment_packages,
};
use novarocks_spi::connector::write_stack::{
    ConnectorBatchWriter, ConnectorCommitFragment, ConnectorOpenWriterRequest,
    ConnectorWriteExecution, root_write_result_schema, writer_output_schema,
};
use novarocks_spi::connector::{CatalogHandle, ConnectorErrorKind};
use novarocks_type_contract::{
    CompileControlError, CompilePhase, ControlShape, EvaluationDomainId, ExpressionControlFlow,
    ExpressionEffectContext, ExpressionEvaluationDomain, ExpressionInvocation, ExpressionUseId,
    FunctionValueType, PureCompileControl,
};
use novarocks_types::UniqueId;

use crate::exec::chunk::Chunk;
use crate::exec::fragment::program::FragmentNodeId;
use crate::exec::node::table_finish::TableFinishRuntimeBinding;
use crate::exec::node::table_write_relation::ConnectorCommitFragmentCarrierValidator;
use crate::exec::node::table_writer::{
    TableWriterPhysicalContextTemplate, TableWriterRuntimeBinding,
};
use crate::exec::operators::table_writer::tests::{
    TestFragmentEncoder, adapter, catalog_handle, commit_fragment, request_context, writer_handle,
};
use crate::exec::operators::{ResultSinkFactory, ResultSinkHandle};
use crate::exec::pipeline::binding::ExchangeBindings;
use crate::exec::pipeline::executor::prepare_compiled_program_pipeline_execution_with_profiler;
use crate::exec::pipeline::operator_factory::OperatorFactory;
use crate::runtime::endpoint::{FragmentDestination, RuntimeEndpoint};
use crate::runtime::fragment::CompiledWriterBindings;
use crate::runtime::fragment::exchange::{
    CompiledExchangeReceivers, materialize_compiled_exchange_receivers,
};
use crate::runtime::fragment::instance::{
    ExchangeInputAssignment, ExchangeInputAssignments, FragmentSinkAssignment,
};
use crate::runtime::fragment::io::exchange::in_process_test_exchange_receiver_port;
use crate::runtime::fragment::io::{
    ExchangeFrame, ExchangeFrameTransmitter, ExchangeReceiverFrame, ExchangeReceiverKey,
    ExchangeReceiverPort, ExchangeTransmitRejection, FragmentIoError, FragmentIoErrorKind,
    FragmentIoOperation, NoopFragmentEventSink,
};
use crate::runtime::fragment::sink::materialize_compiled_sink;
use crate::runtime::runtime_state::RuntimeState;

const WRITER_FINST: UniqueId = UniqueId::new(0x81, 0x01);
const FINISH_FINST: UniqueId = UniqueId::new(0x82, 0x02);
const EDGE: EdgeId = EdgeId::new(9);

struct FixtureControl;
impl PureCompileControl for FixtureControl {
    fn checkpoint(&self, _: CompilePhase, _: u32) -> Result<(), CompileControlError> {
        Ok(())
    }
}

fn ty(data_type: DataType, nullable: bool) -> FunctionValueType {
    FunctionValueType::new(data_type, nullable)
}

fn property(distribution: Distribution) -> PhysicalProperties {
    PhysicalProperties {
        distribution,
        row_multiplicity: RowMultiplicity::SingleCopy,
        ordering: Box::default(),
    }
}

fn dop() -> PipelineDopDomain {
    PipelineDopDomain {
        min: 1,
        max: 4,
        requires_power_of_two: false,
    }
}

/// The provider's own input fields: a NOT NULL `c1` and a nullable `c2`.
fn target_fields() -> Vec<Field> {
    vec![
        Field::new("c1", DataType::Int64, false),
        Field::new("c2", DataType::Utf8, true),
    ]
}

/// The recipe of the test provider's writer handle: its exact binding, a
/// handle payload under that binding, and the provider's input fields.
fn draft() -> ConnectorWriteRecipeDraft {
    let binding = adapter().binding().clone();
    let payload = ConnectorEncodedPayload::new(
        ConnectorEnvelopeHeader::new(
            binding.descriptor().provider_id.clone(),
            binding.catalog_handle().clone(),
            ConnectorCodecCategory::WriteHandle,
            ConnectorCodecRevision::try_new(1).unwrap(),
        ),
        bytes::Bytes::from_static(b"writer-handle"),
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

/// The writer multiplex relation (`root == false`) or the Root result
/// relation (`root == true`): the SPI's own schema, field by field.
fn relation_fields(
    builder: &mut FragmentBuilder,
    owner: NodeId,
    root: bool,
) -> Box<[WriterRelationField]> {
    let schema = if root {
        root_write_result_schema()
    } else {
        writer_output_schema()
    };
    schema
        .fields()
        .iter()
        .enumerate()
        .map(|(index, field)| {
            let (role, kind) = match index {
                0 => (
                    WriterRelationFieldRole::Kind,
                    WriterDerivedKind::RelationKind,
                ),
                1 => (
                    WriterRelationFieldRole::TargetOrdinal,
                    WriterDerivedKind::WriteTargetOrdinal,
                ),
                2 => (
                    WriterRelationFieldRole::RowCount,
                    WriterDerivedKind::AffectedRows,
                ),
                3 => (
                    WriterRelationFieldRole::CommitFragment,
                    WriterDerivedKind::CommitFragment,
                ),
                _ => (
                    WriterRelationFieldRole::Auxiliary,
                    WriterDerivedKind::RelationAuxiliary,
                ),
            };
            let ty = ty(field.data_type().clone(), field.is_nullable());
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
                name: field.name().as_str().into(),
                ty,
                role,
            }
        })
        .collect()
}

/// The two-fragment plan of `INSERT INTO t VALUES <rows>` without statistics.
/// A `None` cell is a NULL literal of a nullable BIGINT.
fn plan(rows: &[(Option<i64>, &str)]) -> (PhysicalPlan, NodeId) {
    let ordinal = WriteTargetOrdinal::try_new(0).unwrap();
    let draft = draft();
    let mut builder = FragmentBuilder::new(FragmentId::new(1));
    let source = builder.reserve_node_id().unwrap();
    let nullable = rows.iter().any(|(value, _)| value.is_none());
    let types = [ty(DataType::Int64, nullable), ty(DataType::Utf8, false)];
    let values = types
        .iter()
        .enumerate()
        .map(|(ordinal, value_type)| {
            builder
                .add_value(
                    value_type.clone(),
                    ValueOrigin::NodeOutput {
                        node: source,
                        output_ordinal: ordinal as u32,
                    },
                )
                .unwrap()
        })
        .collect::<Vec<_>>();
    let cells = rows
        .iter()
        .map(|(int, text)| {
            let int = builder
                .add_expression(
                    source,
                    types[0].clone(),
                    ExprKind::Literal(match int {
                        Some(value) => LiteralValue::Int64(*value),
                        None => LiteralValue::Null,
                    }),
                )
                .unwrap();
            let text = builder
                .add_expression(
                    source,
                    types[1].clone(),
                    ExprKind::Literal(LiteralValue::Utf8((*text).into())),
                )
                .unwrap();
            Box::from([int, text])
        })
        .collect::<Box<[_]>>();
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
            kind: NodeKind::Values { rows: cells },
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
                            ty: ty(
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
        .finish_definition(writer, FragmentSink::Stream { edge: EDGE }, dop())
        .unwrap();

    let mut builder = FragmentBuilder::new(FragmentId::new(2));
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
    let mapping = fields
        .iter()
        .zip(&imported)
        .map(|(a, b)| (a.value, b.value))
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
                imports: mapping.clone(),
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
                    fields: imported,
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
        .finish_definition(finish, FragmentSink::Result, dop())
        .unwrap();

    let mut plan = PlanBuilder::new(PlanVersionId::try_new([8; 16]).unwrap());
    plan.add_fragment(producer).unwrap();
    plan.add_fragment(consumer).unwrap();
    plan.add_edge(Edge {
        id: EDGE,
        kind: EdgeKind::Stream,
        source: EdgeSource {
            fragment: FragmentId::new(1),
            projection: fields.iter().map(|field| field.value).collect(),
        },
        destination: EdgeDestination {
            fragment: FragmentId::new(2),
            node: exchange,
            receive_mapping: mapping,
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
        fragment: FragmentId::new(2),
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
    (plan.finish().unwrap(), exchange)
}

/// Literal cells are the only roots; each gets one eager use.
fn root_uses(fragment: &Fragment) -> PhysicalRootUses {
    let roots = PhysicalExpressionRoots::try_new(fragment, &FixtureControl).unwrap();
    let domain = EvaluationDomainId::new(0);
    let mut uses = Vec::new();
    let mut bindings = Vec::new();
    for (ordinal, (site, root)) in roots.sites().iter().enumerate() {
        let id = ExpressionUseId::new(u32::try_from(ordinal).unwrap());
        uses.push(ExpressionInvocation {
            context: ExpressionEffectContext {
                use_id: id,
                domain,
                demand: root.demand,
            },
            definition: root.expr,
            control: ControlShape::Eager,
            arguments: Box::default(),
        });
        bindings.push((*site, id));
    }
    let flow = ExpressionControlFlow::try_new(
        vec![ExpressionEvaluationDomain {
            id: domain,
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

// Explicit small-fixture admission; these are test inputs, not defaults.
fn admission() -> FragmentPackageAdmission {
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

fn packages(plan: &PhysicalPlan) -> BTreeMap<FragmentId, FragmentPackage> {
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
        admissions.insert(id, admission());
    }
    extract_fragment_packages(
        plan,
        &BTreeMap::new(),
        &BTreeMap::from([(WriteTargetOrdinal::try_new(0).unwrap(), draft())]),
        &uses,
        &calls,
        &pruning,
        &admissions,
        &FixtureControl,
    )
    .unwrap()
}

/// Keeps the draft: a pure contract fixture, not the installed Iceberg port.
struct KeepDraft;
impl ConnectorWriteRecipeCompiler for KeepDraft {
    type Error = ConnectorError;
    fn compile_private(
        &self,
        draft: &ConnectorWriteRecipeDraft,
        _: &dyn PureCompileControl,
    ) -> Result<ConnectorWriteRecipeDraft, PureProviderCompileError<ConnectorError>> {
        Ok(draft.clone())
    }
}

fn compile(package: FragmentPackage, pipeline_dop: usize, root_sink: bool) -> Arc<LocalProgram> {
    let provider = adapter().binding().descriptor().provider_id.clone();
    let providers = PureProviderProgramCatalog::try_new(
        &[PureProviderManifestEntry::new(
            provider.clone(),
            false,
            true,
        )],
        vec![PureProviderProgramDefinition::new(
            provider,
            None,
            Some(Arc::new(KeepDraft)
                as Arc<
                    dyn ConnectorWriteRecipeCompiler<Error = ConnectorError>,
                >),
        )],
        &FixtureControl,
    )
    .unwrap();
    let validated =
        validate_fragment_providers(Arc::new(package), &providers, &FixtureControl).unwrap();
    Arc::new(
        compile_fragment(
            validated,
            &crate::exec::expr::compiled_program::tests::rng_subset(),
            LocalCompileOptions {
                pipeline_dop: NonZeroUsize::new(pipeline_dop).unwrap(),
                root_sink_dop: root_sink.then(|| NonZeroUsize::new(1).unwrap()),
                kernel_abi: KernelAbiVersion::CURRENT,
                // Explicit fixture admission; not production defaults.
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
                exchange_wait: Duration::from_secs(120),
            },
            &FixtureControl,
        )
        .unwrap_or_else(|error| panic!("fixture fragment compiles: {error}")),
    )
}

/// Records every page the provider writer accepts and stages one artifact
/// per writer.
#[derive(Default)]
struct Recorded {
    opened: usize,
    batches: Vec<RecordBatch>,
}

struct RecordingExecution {
    catalog_handle: CatalogHandle,
    recorded: Arc<Mutex<Recorded>>,
}

#[async_trait::async_trait]
impl ConnectorWriteExecution for RecordingExecution {
    fn catalog_handle(&self) -> &CatalogHandle {
        &self.catalog_handle
    }

    async fn open_writer(
        &self,
        request: ConnectorOpenWriterRequest,
    ) -> Result<Box<dyn ConnectorBatchWriter>, ConnectorError> {
        assert_eq!(request.physical.writer_ordinal(), 0);
        assert_eq!(
            request.expected_schema.as_ref(),
            &Schema::new(target_fields()),
            "the writer opens over the provider's exact input schema"
        );
        self.recorded.lock().unwrap().opened += 1;
        Ok(Box::new(RecordingWriter {
            recorded: Arc::clone(&self.recorded),
        }))
    }
}

struct RecordingWriter {
    recorded: Arc<Mutex<Recorded>>,
}

#[async_trait::async_trait]
impl ConnectorBatchWriter for RecordingWriter {
    async fn append(&mut self, batch: RecordBatch) -> Result<(), ConnectorError> {
        self.recorded.lock().unwrap().batches.push(batch);
        Ok(())
    }

    async fn finish(&mut self) -> Result<Vec<ConnectorCommitFragment>, ConnectorError> {
        Ok(vec![commit_fragment(b"staged".to_vec())])
    }

    async fn abort(&mut self) -> Result<(), ConnectorError> {
        Ok(())
    }
}

/// Accepts every canonical carrier; the real one belongs to the backend.
struct AcceptCarriers;
impl ConnectorCommitFragmentCarrierValidator for AcceptCarriers {
    fn validate(&self, _: WriteTargetOrdinal, encoded: &[u8]) -> Result<(), ConnectorError> {
        if encoded.is_empty() {
            return Err(ConnectorError::new(
                ConnectorErrorKind::CorruptData,
                "empty carrier",
            ));
        }
        Ok(())
    }
}

/// Delivers every encoded frame to the receiver port, as the native data
/// plane does after its wire hop.
struct LoopbackTransmitter {
    port: Arc<dyn ExchangeReceiverPort>,
}

impl ExchangeFrameTransmitter for LoopbackTransmitter {
    fn transmit(&self, frame: ExchangeFrame) -> Result<(), ExchangeTransmitRejection> {
        self.port
            .push(
                ExchangeReceiverKey {
                    fragment_instance_id: frame.destination_fragment_instance_id,
                    node_id: frame.destination_node_id,
                },
                ExchangeReceiverFrame {
                    source_fragment_instance_id: frame.sender_fragment_instance_id,
                    sender_ordinal: frame.sender_ordinal,
                    sender_count: frame.sender_count,
                    sender_id: frame.sender_id,
                    backend_number: frame.backend_number,
                    sequence: frame.sequence,
                    eos: frame.eos,
                    payload: frame.payload,
                },
            )
            .map_err(|error| {
                ExchangeTransmitRejection::Failed(FragmentIoError::new(
                    FragmentIoOperation::ExchangeTransmit,
                    FragmentIoErrorKind::RemoteRejected,
                    error,
                ))
            })
    }
}

fn runtime_state() -> Arc<RuntimeState> {
    Arc::new(RuntimeState::new(
        None,
        None,
        None,
        None,
        None,
        None,
        Some(crate::runtime::execution_runtime::test_execution_runtime()),
    ))
}

fn root_of(program: &LocalProgram) -> ProgramNodeId {
    program.graph().root()
}

fn writer_bindings(
    program: &LocalProgram,
    recorded: &Arc<Mutex<Recorded>>,
) -> CompiledWriterBindings {
    let mut bindings = CompiledWriterBindings::default();
    bindings
        .bind_writer(
            root_of(program),
            TableWriterRuntimeBinding::try_new(
                writer_handle(),
                Arc::new(RecordingExecution {
                    catalog_handle: catalog_handle(),
                    recorded: Arc::clone(recorded),
                }),
                TableWriterPhysicalContextTemplate::new([1; 16], 1, [2; 16], 0),
                request_context(),
                Arc::new(TestFragmentEncoder),
            )
            .unwrap(),
        )
        .unwrap();
    bindings
}

fn finish_bindings(program: &LocalProgram) -> CompiledWriterBindings {
    let mut bindings = CompiledWriterBindings::default();
    bindings
        .bind_finish(
            root_of(program),
            TableFinishRuntimeBinding::new(Arc::new(AcceptCarriers)),
        )
        .unwrap();
    bindings
}

fn run(
    program: &Arc<LocalProgram>,
    sink: Box<dyn OperatorFactory>,
    exchange: ExchangeBindings,
    writers: CompiledWriterBindings,
    instance: UniqueId,
    pipeline_dop: i32,
) -> Result<(), String> {
    prepare_compiled_program_pipeline_execution_with_profiler(
        Arc::clone(program),
        Duration::from_millis(10),
        sink,
        exchange,
        crate::exec::pipeline::binding::ScanBindings::default(),
        writers,
        Some((instance.high(), instance.low())),
        None,
        pipeline_dop,
        runtime_state(),
        Arc::new(NoopFragmentEventSink),
    )
    .map_err(|error| error.to_string())?
    .start()
    .join()
    .map(|_| ())
    .map_err(|error| error.to_string())
}

struct Insert {
    writer: Arc<LocalProgram>,
    finish: Arc<LocalProgram>,
    receiver: NodeId,
}

fn insert(rows: &[(Option<i64>, &str)], writer_dop: usize) -> Insert {
    let (plan, receiver) = plan(rows);
    let mut packages = packages(&plan);
    Insert {
        writer: compile(
            packages.remove(&FragmentId::new(1)).unwrap(),
            writer_dop,
            false,
        ),
        finish: compile(packages.remove(&FragmentId::new(2)).unwrap(), 1, true),
        receiver,
    }
}

/// Run the writer fragment into the finish fragment's receiver and return
/// the writer's outcome; the finish is run only when the writer succeeded.
fn run_insert(
    insert: &Insert,
    writer_dop: i32,
    recorded: &Arc<Mutex<Recorded>>,
) -> Result<Vec<Chunk>, String> {
    let port = in_process_test_exchange_receiver_port();
    let CompiledExchangeReceivers {
        registrations,
        bindings,
    } = materialize_compiled_exchange_receivers(
        &insert.finish,
        FINISH_FINST,
        &ExchangeInputAssignments::new(BTreeMap::from([(
            FragmentNodeId::new(i32::try_from(insert.receiver.get()).unwrap()),
            ExchangeInputAssignment::new(NonZeroUsize::new(1).unwrap()),
        )])),
        Arc::clone(&port),
    )
    .expect("compiled receivers");
    for registration in registrations {
        port.register(registration).expect("register receiver");
    }
    let writer_sink = materialize_compiled_sink(
        &insert.writer,
        &FragmentSinkAssignment::StreamDestinations {
            destinations: vec![
                FragmentDestination::new(
                    FINISH_FINST,
                    RuntimeEndpoint::new("127.0.0.1", 9030).expect("endpoint"),
                    WRITER_FINST,
                    0,
                    1,
                )
                .expect("destination"),
            ],
            sender_id: None,
        },
        WRITER_FINST,
        Arc::new(LoopbackTransmitter {
            port: Arc::clone(&port),
        }),
        None,
        None,
    )
    .expect("compiled stream sink");
    run(
        &insert.writer,
        writer_sink,
        ExchangeBindings::default(),
        writer_bindings(&insert.writer, recorded),
        WRITER_FINST,
        writer_dop,
    )?;
    let output = ResultSinkHandle::new();
    let finish_sink = materialize_compiled_sink(
        &insert.finish,
        &FragmentSinkAssignment::None,
        FINISH_FINST,
        crate::runtime::fragment::io::exchange::discard_exchange_transmitter(),
        Some(Box::new(ResultSinkFactory::new(output.clone()))),
        None,
    )
    .expect("compiled result sink");
    run(
        &insert.finish,
        finish_sink,
        bindings,
        finish_bindings(&insert.finish),
        FINISH_FINST,
        1,
    )?;
    Ok(output.take_chunks())
}

#[test]
fn compiled_insert_values_writes_pages_and_publishes_the_root_relation() {
    let rows = [(Some(1), "a"), (Some(2), "b")];
    let insert = insert(&rows, 1);
    assert!(matches!(
        insert.writer.graph().nodes()[root_of(&insert.writer).index()].kind(),
        ProgramNodeKind::TableWriter { .. }
    ));
    let recorded = Arc::new(Mutex::new(Recorded::default()));
    let chunks = run_insert(&insert, 1, &recorded).expect("the compiled insert runs");

    // The provider writer received the projected rows under its exact schema.
    let recorded = recorded.lock().unwrap();
    assert_eq!(recorded.opened, 1, "one driver opens one writer");
    let mut written = Vec::new();
    for batch in &recorded.batches {
        assert_eq!(batch.schema().as_ref(), &Schema::new(target_fields()));
        let c1 = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let c2 = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        for row in 0..batch.num_rows() {
            written.push((c1.value(row), c2.value(row).to_string()));
        }
    }
    assert_eq!(
        written,
        [(1, "a".to_string()), (2, "b".to_string())],
        "both literal rows are written once, in order"
    );

    // The Root relation is the exact SPI schema, nested child names
    // included, so the frontend's exact-schema decoder reads it: one summary
    // of two rows and one prepared fragment of the writer's staged artifact.
    let mut summary = None;
    let mut fragments = Vec::new();
    for chunk in &chunks {
        assert_eq!(chunk.schema().as_ref(), root_write_result_schema().as_ref());
        let kinds = chunk
            .batch
            .column(0)
            .as_any()
            .downcast_ref::<Int8Array>()
            .unwrap();
        let counts = chunk
            .batch
            .column(2)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let payloads = chunk
            .batch
            .column(3)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .unwrap();
        for row in 0..chunk.len() {
            match kinds.value(row) {
                1 => summary = Some(counts.value(row)),
                2 => fragments.push(payloads.value(row).to_vec()),
                other => panic!("unexpected Root row kind {other}"),
            }
        }
    }
    assert_eq!(summary, Some(2));
    assert_eq!(fragments, [b"staged".to_vec()]);
}

/// A nullable value feeding a NOT NULL provider field is the writer's row
/// obligation: a NULL row refuses the page before the provider writer
/// accepts anything.
#[test]
fn a_null_into_a_not_null_provider_field_is_refused_before_any_page_is_written() {
    let insert = insert(&[(Some(1), "a"), (None, "b")], 1);
    let recorded = Arc::new(Mutex::new(Recorded::default()));
    let error = run_insert(&insert, 1, &recorded).expect_err("the NULL row is refused");
    assert!(
        error.contains("Column 'c1' is declared as non-nullable but contains null values"),
        "{error}"
    );
    assert!(
        recorded.lock().unwrap().batches.is_empty(),
        "no page reaches the provider writer"
    );
}

/// A Task that binds no writer capability, or binds one for a node that is
/// not its writer, is refused before any driver is prepared.
#[test]
fn compiled_writer_bindings_cover_exactly_the_programs_writer_family() {
    let insert = insert(&[(Some(1), "a")], 1);
    let recorded = Arc::new(Mutex::new(Recorded::default()));
    assert!(
        CompiledWriterBindings::default()
            .validate(&insert.writer)
            .unwrap_err()
            .contains("table writers")
    );
    assert!(
        writer_bindings(&insert.writer, &recorded)
            .validate(&insert.finish)
            .unwrap_err()
            .contains("table writers")
    );
    assert!(
        CompiledWriterBindings::default()
            .validate(&insert.finish)
            .unwrap_err()
            .contains("table finishes")
    );
    writer_bindings(&insert.writer, &recorded)
        .validate(&insert.writer)
        .unwrap();
    finish_bindings(&insert.finish)
        .validate(&insert.finish)
        .unwrap();
}
