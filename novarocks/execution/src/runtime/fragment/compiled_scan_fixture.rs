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

//! A compiled provider-scan fragment and a fake Task scan for it.
//!
//! The fragment is `SELECT v1 AS b, v0 AS a FROM t [WHERE v0 > 7]`: a provider
//! Scan with an optional pruning-only residual, a Project and a Result sink.
//! It is authored with the real physical builders, extracted as a checked
//! package, provider-validated and compiled by local-compiler. The provider
//! compiler is a pure contract fixture, not an installed Iceberg or Paimon
//! port, and the Task scan is a scripted stream of public-schema chunks.

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::num::{NonZeroU64, NonZeroUsize};
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::Duration;

use arrow::array::{ArrayRef, Int64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use bytes::Bytes;
use futures::Stream;
use futures::future::BoxFuture;
use novarocks_connector_contract::*;
use novarocks_functions::ConstantPolicy;
use novarocks_local_compiler::{
    LocalCompileOptions, compile_fragment, validate_fragment_providers,
};
use novarocks_local_program::{KernelAbiVersion, LocalProgram};
use novarocks_physical_plan::*;
use novarocks_spi::connector::read_stack::ConnectorPollBudget;
use novarocks_type_contract::{
    CompileCheckpoints, CompileControlError, CompilePhase, ControlShape, DecimalOverflowPolicy,
    EvaluationDemand, EvaluationDomainId, ExpressionControlFlow, ExpressionEffectContext,
    ExpressionEvaluationDomain, ExpressionInvocation, ExpressionUseId, PureCompileControl,
    ValueLogicalType, control_argument_semantics,
};

use crate::exec::chunk::{Chunk, ChunkSchema};
use crate::exec::node::scan::{
    BoundScanRanges, ScanChunkStream, ScanOp, ScanOutputStream, ScanSource, ScanStreamSource,
};
use crate::runtime::profile::RuntimeProfile;

/// The physical scan node, which is also the Task's scan address.
pub(crate) const SCAN_NODE: i32 = 10;
const FRAGMENT: FragmentId = FragmentId::new(1);
const SCAN: NodeId = NodeId::new(10);
const PROJECT: NodeId = NodeId::new(11);

struct FixtureControl;
impl PureCompileControl for FixtureControl {
    fn checkpoint(&self, _: CompilePhase, _: u32) -> Result<(), CompileControlError> {
        Ok(())
    }
}

fn int64() -> ValueType {
    ValueType::new(DataType::Int64, false)
}

fn boolean() -> ValueType {
    ValueType::new(DataType::Boolean, false)
}

fn binding() -> ConnectorReadBinding {
    let instance = ConnectorInstanceId::parse("lake").unwrap();
    ConnectorReadBinding::new(
        ConnectorInstanceDescriptor {
            provider_id: ConnectorProviderId::parse("alpha").unwrap(),
            instance_id: instance.clone(),
        },
        CatalogHandle::new(instance, CatalogVersion::from_bytes([3; 32])),
    )
}

fn payload(
    binding: &ConnectorReadBinding,
    category: ConnectorCodecCategory,
    value: &'static [u8],
) -> ConnectorEncodedPayload {
    ConnectorEncodedPayload::new(
        ConnectorEnvelopeHeader::new(
            binding.descriptor().provider_id.clone(),
            binding.catalog_handle().clone(),
            category,
            ConnectorCodecRevision::try_new(1).unwrap(),
        ),
        Bytes::from_static(value),
    )
}

/// The provider's own projected fields: names, field ids and schema metadata
/// are provider facts the compiled scan layout keeps exactly.
fn public_schema() -> Schema {
    Schema::new_with_metadata(
        ["v0", "v1"]
            .into_iter()
            .enumerate()
            .map(|(id, name)| {
                Field::new(name, DataType::Int64, false).with_metadata(HashMap::from([(
                    "provider.field-id".into(),
                    (id + 1).to_string(),
                )]))
            })
            .collect::<Vec<_>>(),
        HashMap::from([("provider.schema".into(), "generation-1".into())]),
    )
}

/// Compile the fixture fragment for `dop` drivers, with or without the
/// `v0 > 7` residual.
pub(crate) fn scan_program(dop: usize, residual: bool) -> Arc<LocalProgram> {
    let binding = binding();
    let relation_payload = ConnectorReadRelationPayload::new(
        ConnectorReadRelationKind::Table,
        payload(&binding, ConnectorCodecCategory::ReadTable, b"table"),
        payload(&binding, ConnectorCodecCategory::ReadView, b"view"),
    );
    let columns = [b"c0" as &'static [u8], b"c1"].map(|bytes| ProviderColumnReference {
        column_payload: payload(&binding, ConnectorCodecCategory::ReadColumn, bytes),
    });
    let mut builder = FragmentBuilder::new(FRAGMENT);
    let provider = columns
        .iter()
        .map(|column| {
            builder
                .add_value(
                    int64(),
                    ValueOrigin::ProviderField {
                        scan_node: SCAN,
                        field: column.clone(),
                    },
                )
                .unwrap()
        })
        .collect::<Vec<_>>();
    let mut residuals = Vec::new();
    if residual {
        let column = builder
            .add_expression(SCAN, int64(), ExprKind::Value(provider[0]))
            .unwrap();
        let literal = builder
            .add_expression(SCAN, int64(), ExprKind::Literal(LiteralValue::Int64(7)))
            .unwrap();
        residuals.push(
            builder
                .add_expression(
                    SCAN,
                    boolean(),
                    ExprKind::Binary {
                        left: column,
                        op: BinaryOperator::Gt,
                        right: literal,
                        decimal_overflow_policy: DecimalOverflowPolicy::OutputNull,
                        allow_throw_exception: None,
                    },
                )
                .unwrap(),
        );
    }
    // The provider answers the residual for pruning only; the residual still
    // decides every row.
    let predicate_guarantees = residuals
        .iter()
        .map(|&predicate| PredicateGuarantee {
            predicate,
            kind: PredicateGuaranteeKind::PruningOnly,
        })
        .collect::<Box<[_]>>();
    let read = ProviderReadReference {
        binding: binding.clone(),
        input_version: ExactInputVersion::try_new([9]).unwrap(),
        relation: relation_payload.clone(),
    };
    let relation = Relation::Data(DataRelation {
        read,
        work_source: ConnectorReadWorkSource::RuntimeSplits,
        selection_digest: [7; 32],
        schema: columns
            .iter()
            .map(|column| RelationField {
                column: column.clone(),
                ty: int64(),
            })
            .collect(),
        predicate_guarantees,
        provided_properties: PhysicalProperties {
            distribution: Distribution::Unconstrained,
            row_multiplicity: RowMultiplicity::SingleCopy,
            ordering: Box::default(),
        },
    });
    builder
        .add_scan(
            SCAN,
            NodeKind::Scan {
                occurrence: ProviderReadOccurrenceId::new(0),
                relation: Box::new(relation),
                read_budget: ScanReadBudget {
                    max_batch_rows: 100,
                    max_batch_bytes: 4096,
                },
                provider_outputs: columns
                    .iter()
                    .cloned()
                    .zip(provider.iter().copied())
                    .collect(),
                residuals: residuals.into_boxed_slice(),
                derived_values: Box::default(),
            },
            provider.clone().into_boxed_slice(),
        )
        .unwrap();
    // `b = v1, a = v0`: the projection reorders the provider columns under
    // SQL names, so the result is no longer the provider layout.
    let mut assignments = Vec::new();
    let mut projected = Vec::new();
    for &input in [provider[1], provider[0]].iter() {
        let expr = builder
            .add_expression(PROJECT, int64(), ExprKind::Value(input))
            .unwrap();
        let output = builder
            .add_value(
                int64(),
                ValueOrigin::Expr {
                    node: PROJECT,
                    expr,
                },
            )
            .unwrap();
        assignments.push((expr, output));
        projected.push(output);
    }
    builder
        .add_project(
            PROJECT,
            SCAN,
            assignments.into_boxed_slice(),
            projected.clone().into_boxed_slice(),
        )
        .unwrap();
    let fragment = builder
        .finish_definition(
            PROJECT,
            FragmentSink::Result,
            PipelineDopDomain {
                min: 1,
                max: 4,
                requires_power_of_two: false,
            },
        )
        .unwrap();

    let mut plan = PlanBuilder::new(PlanVersionId::try_new([7; 16]).unwrap());
    plan.add_fragment(fragment).unwrap();
    plan.set_result_port(ResultPort {
        scalar_schema: None,
        fragment: FRAGMENT,
        output: OutputPort {
            node: PROJECT,
            columns: projected.clone().into_boxed_slice(),
        },
        fields: projected
            .iter()
            .zip(["b", "a"])
            .map(|(value, label)| ResultField {
                domain: crate::test_result_domain::result_value_domain(&int64()),
                name: label.into(),
                alias: None,
                value: *value,
                ty: int64(),
            })
            .collect(),
    })
    .unwrap();
    let plan = plan.finish().unwrap();

    let draft = ConnectorReadRelationRecipeDraft::try_new(
        binding,
        relation_payload,
        columns
            .iter()
            .map(|column| column.column_payload.clone())
            .collect(),
    )
    .unwrap();
    let scan = FrozenConnectorScan::try_new(
        draft,
        ["v0", "v1"]
            .into_iter()
            .map(|name| StaticScanAssignment::new(Arc::from(name), ConnectorValueType::BigInt))
            .collect(),
        TupleDomain::all(),
        TupleDomain::all(),
        None,
        vec![],
        NonZeroU64::new(100).unwrap(),
        NonZeroU64::new(4096).unwrap(),
        ConnectorReadWorkSource::RuntimeSplits,
    )
    .unwrap();
    let source = ConnectorReadStaticFacts::try_new(
        ConnectorReadInputVersion::try_new([9]).unwrap(),
        [7; 32],
        ConnectorReadProperties::try_new(ConnectorReadDistribution::Unconstrained, vec![]).unwrap(),
        ConnectorReadArtifactCoverage::NoArtifactInputs,
        vec![],
    )
    .unwrap();
    let public = ConnectorReadPublicFacts::try_new(
        source,
        None,
        public_schema(),
        vec![ValueLogicalType::Physical; 2],
    )
    .unwrap();
    let scans = BTreeMap::from([(
        ProviderReadOccurrenceId::new(0),
        FrozenConnectorRead::try_new(scan, public).unwrap(),
    )]);

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
        &scans,
        &BTreeMap::new(),
        &uses,
        &calls,
        &pruning,
        &admissions,
        &FixtureControl,
    )
    .unwrap();
    let package = Arc::new(packages.remove(&FRAGMENT).unwrap());
    let validated = validate_fragment_providers(package, &providers(), &FixtureControl).unwrap();
    let functions = crate::exec::expr::compiled_program::tests::rng_subset();
    Arc::new(
        compile_fragment(validated, &functions, options(dop), &FixtureControl)
            .unwrap_or_else(|error| panic!("the scan fixture compiles: {error}")),
    )
}

/// One complete eager use tree per physical root site in one root domain.
fn root_uses(fragment: &Fragment) -> PhysicalRootUses {
    struct Author<'a> {
        fragment: &'a Fragment,
        next: u32,
        uses: Vec<ExpressionInvocation<ExprId>>,
    }
    impl Author<'_> {
        fn visit(&mut self, expr: ExprId, demand: EvaluationDemand) -> ExpressionUseId {
            let id = ExpressionUseId::new(self.next);
            self.next += 1;
            let args = match &self.fragment.expressions().get(expr).unwrap().kind {
                ExprKind::Binary { left, right, .. } => vec![*left, *right],
                ExprKind::Literal(_) | ExprKind::Value(_) => vec![],
                _ => panic!("the scan fixture has only comparisons, values and literals"),
            };
            let arguments = args
                .iter()
                .enumerate()
                .map(|(ordinal, &child)| {
                    let (child_demand, guard) = control_argument_semantics(
                        ControlShape::Eager,
                        args.len(),
                        ordinal,
                        demand,
                    )
                    .unwrap();
                    assert!(guard.is_none());
                    self.visit(child, child_demand)
                })
                .collect::<Box<[_]>>();
            self.uses.push(ExpressionInvocation {
                context: ExpressionEffectContext {
                    use_id: id,
                    domain: EvaluationDomainId::new(0),
                    demand,
                },
                definition: expr,
                control: ControlShape::Eager,
                arguments,
            });
            id
        }
    }
    let roots = PhysicalExpressionRoots::try_new(fragment, &FixtureControl).unwrap();
    let mut author = Author {
        fragment,
        next: 0,
        uses: vec![],
    };
    let bindings = roots
        .sites()
        .iter()
        .map(|(&site, root)| (site, author.visit(root.expr, root.demand)))
        .collect();
    let flow = ExpressionControlFlow::<ExprId>::try_new(
        vec![ExpressionEvaluationDomain {
            id: EvaluationDomainId::new(0),
            parent: None,
            guard: None,
        }],
        author.uses,
        fragment.expressions(),
        CompilePhase::Validate,
        &FixtureControl,
    )
    .unwrap();
    PhysicalRootUses::try_new(fragment, flow, bindings, &FixtureControl).unwrap()
}

// Conservative retained-source invoice and independent projection ceilings for
// this small fixture only; this is not a production default or a MEM grant.
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

/// Validates the complete frozen read and keeps its private bytes as they
/// are; the public facts are borrowed unchanged.
struct Port;
impl ConnectorReadProgramCompiler for Port {
    type Error = ConnectorError;
    fn compile_private(
        &self,
        input: &FrozenConnectorRead,
        control: &dyn PureCompileControl,
    ) -> Result<ConnectorReadRelationRecipeDraft, PureProviderCompileError<ConnectorError>> {
        let mut work = CompileCheckpoints::try_new(control, CompilePhase::ProviderValidation)?;
        work.step()?;
        work.finish()?;
        Ok(input.scan().recipe().clone())
    }
}

fn providers() -> PureProviderProgramCatalog<ConnectorError> {
    let provider = ConnectorProviderId::parse("alpha").unwrap();
    PureProviderProgramCatalog::try_new(
        &[PureProviderManifestEntry::new(
            provider.clone(),
            true,
            false,
        )],
        vec![PureProviderProgramDefinition::new(
            provider,
            Some(Arc::new(Port)
                as Arc<
                    dyn ConnectorReadProgramCompiler<Error = ConnectorError>,
                >),
            None,
        )],
        &FixtureControl,
    )
    .unwrap()
}

fn options(dop: usize) -> LocalCompileOptions {
    LocalCompileOptions {
        pipeline_dop: NonZeroUsize::new(dop).unwrap(),
        // The result is published by one root sink driver.
        root_sink_dop: Some(NonZeroUsize::new(1).unwrap()),
        kernel_abi: KernelAbiVersion::CURRENT,
        exchange_wait: Duration::from_secs(120),
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

/// One chunk of `(v0, v1)` rows exactly as the provider page converter
/// presents it: the compiled scan layout, provider names and metadata
/// included.
pub(crate) fn scan_chunk(program: &LocalProgram, v0: &[i64], v1: &[i64]) -> Chunk {
    let scan = program
        .scan_inputs()
        .keys()
        .next()
        .expect("the fixture has one scan");
    let layout = program.graph().nodes()[scan.index()].output_layout();
    let schema = ChunkSchema::from_compiled_layout(layout).expect("scan chunk schema");
    let batch = RecordBatch::try_new(
        schema.arrow_schema_ref(),
        vec![
            Arc::new(Int64Array::from(v0.to_vec())) as ArrayRef,
            Arc::new(Int64Array::from(v1.to_vec())) as ArrayRef,
        ],
    )
    .expect("scan batch");
    Chunk::try_new_with_chunk_schema(batch, schema).expect("scan chunk")
}

/// Every output row as `(b, a)` cells, in emission order.
pub(crate) fn rows(chunks: &[Chunk]) -> Vec<(i64, i64)> {
    let mut rows = Vec::new();
    for chunk in chunks {
        let column = |index: usize| {
            chunk.batch.columns()[index]
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("Int64 column")
                .clone()
        };
        let (b, a) = (column(0), column(1));
        for row in 0..chunk.len() {
            rows.push((b.value(row), a.value(row)));
        }
    }
    rows
}

/// A Task scan that hands its scripted chunks to the one driver that claims
/// its stream, then ends; an endless scan stays pending after them.
pub(crate) struct FixtureScanOp {
    source: Arc<FixtureStreamSource>,
    terminations: AtomicUsize,
}

impl FixtureScanOp {
    pub(crate) fn new(chunks: Vec<Chunk>, endless: bool) -> Arc<Self> {
        Arc::new(Self {
            source: Arc::new(FixtureStreamSource {
                chunks: Mutex::new(Some(chunks.into())),
                endless,
                claims: AtomicUsize::new(0),
            }),
            terminations: AtomicUsize::new(0),
        })
    }

    pub(crate) fn terminations(&self) -> usize {
        self.terminations.load(Ordering::Acquire)
    }

    pub(crate) fn claims(&self) -> usize {
        self.source.claims.load(Ordering::Acquire)
    }
}

impl ScanOp for FixtureScanOp {
    fn stream_source(&self) -> Arc<dyn ScanStreamSource> {
        Arc::clone(&self.source) as Arc<dyn ScanStreamSource>
    }

    fn terminate(&self) -> Result<(), String> {
        self.terminations.fetch_add(1, Ordering::AcqRel);
        Ok(())
    }

    fn profile_name(&self) -> Option<String> {
        Some("FixtureCompiledScan".to_string())
    }
}

struct FixtureStreamSource {
    chunks: Mutex<Option<VecDeque<Chunk>>>,
    endless: bool,
    claims: AtomicUsize,
}

impl ScanStreamSource for FixtureStreamSource {
    fn claim(
        &self,
        _budget: ConnectorPollBudget,
        _profile: Option<RuntimeProfile>,
    ) -> Result<ScanOutputStream, String> {
        self.claims.fetch_add(1, Ordering::AcqRel);
        let chunks = self
            .chunks
            .lock()
            .expect("fixture chunks")
            .take()
            .ok_or_else(|| "fixture scan stream already claimed".to_string())?;
        Ok(Box::pin(FixtureStream {
            chunks,
            endless: self.endless,
        }))
    }
}

struct FixtureStream {
    chunks: VecDeque<Chunk>,
    endless: bool,
}

impl Stream for FixtureStream {
    type Item = Result<Chunk, String>;

    fn poll_next(self: Pin<&mut Self>, _context: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        match this.chunks.pop_front() {
            Some(chunk) => Poll::Ready(Some(Ok(chunk))),
            // An endless scan never delivers again; only a stop ends it.
            None if this.endless => Poll::Pending,
            None => Poll::Ready(None),
        }
    }
}

impl ScanChunkStream for FixtureStream {
    fn close(self: Pin<Box<Self>>) -> BoxFuture<'static, Result<(), String>> {
        Box::pin(async { Ok(()) })
    }
}

/// The Task scan source of the fixture: it binds only the empty range
/// binding a split-driven scan is assigned.
pub(crate) struct FixtureScanSource(pub(crate) Arc<FixtureScanOp>);

impl ScanSource for FixtureScanSource {
    fn bind(&self, ranges: BoundScanRanges) -> Result<Arc<dyn ScanOp>, String> {
        match ranges {
            BoundScanRanges::None => Ok(Arc::clone(&self.0) as Arc<dyn ScanOp>),
            other => Err(format!(
                "fixture scan expects no frozen range, got {other:?}"
            )),
        }
    }
}
