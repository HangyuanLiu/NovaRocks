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

//! Consume a transient decoder plan into one bounded, task-independent graph.
//! A scan recipe is supplied by the provider compiler; the old ScanSource is
//! never inspected to guess a relation or retained by the program.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::num::NonZeroUsize;
use std::sync::Arc;

use novarocks_local_program as lp;

use super::{ExecNode, ExecNodeKind, ExecPlan};
use crate::exec::chunk::ChunkSchemaRef;
use crate::exec::expr::ExprId;
use crate::exec::node::runtime_filter::RuntimeFilterConsumerBinding;
use crate::runtime_filter as rf;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LocalProgramLoweringError(String);

impl LocalProgramLoweringError {
    fn new(message: impl Into<String>) -> Self {
        Self(message.into())
    }
}

impl fmt::Display for LocalProgramLoweringError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for LocalProgramLoweringError {}

type Result<T> = std::result::Result<T, LocalProgramLoweringError>;

/// The actual creator's refusal is never folded into a semantic diagnostic.
#[derive(Debug)]
pub enum LocalProgramCreationFailure<H> {
    Lowering(LocalProgramLoweringError),
    Control(novarocks_type_contract::CompileControlError),
    Creator(H),
    Resource(novarocks_type_contract::owned_resources::layout::LayoutResourceError),
}
impl<H> From<LocalProgramLoweringError> for LocalProgramCreationFailure<H> {
    fn from(error: LocalProgramLoweringError) -> Self {
        Self::Lowering(error)
    }
}
impl<H> From<novarocks_type_contract::CompileControlError> for LocalProgramCreationFailure<H> {
    fn from(error: novarocks_type_contract::CompileControlError) -> Self {
        Self::Control(error)
    }
}
impl<H> From<novarocks_type_contract::owned_resources::layout::LayoutResourceError>
    for LocalProgramCreationFailure<H>
{
    fn from(error: novarocks_type_contract::owned_resources::layout::LayoutResourceError) -> Self {
        Self::Resource(error)
    }
}
impl<H> From<lp::LayoutCreationFailure<H>> for LocalProgramCreationFailure<H> {
    fn from(error: lp::LayoutCreationFailure<H>) -> Self {
        match error {
            lp::LayoutCreationFailure::Creator(error) => Self::Creator(error),
            lp::LayoutCreationFailure::Resource(error) => Self::Resource(error),
            lp::LayoutCreationFailure::Layout(lp::LayoutCompileError::Control(error)) => {
                Self::Control(error)
            }
            lp::LayoutCreationFailure::Layout(error) => {
                Self::Lowering(LocalProgramLoweringError::new(error.to_string()))
            }
        }
    }
}
impl<H> From<String> for LocalProgramCreationFailure<H> {
    fn from(cause: String) -> Self {
        Self::Lowering(LocalProgramLoweringError::new(cause))
    }
}
type CreationResult<T, H> = std::result::Result<T, LocalProgramCreationFailure<H>>;

/// This carries only the caller's real construction request and storage hold.
/// It owns no query runtime and turns no numeric profile into capacity.
pub struct LocalLayoutCreation<'a, H> {
    pub storage: novarocks_type_contract::ValueTypeStorageLifetime,
    pub request: &'a mut dyn FnMut(std::alloc::Layout) -> std::result::Result<(), H>,
    pub temporary_request: &'a mut dyn FnMut(std::alloc::Layout) -> std::result::Result<(), H>,
    pub control: &'a dyn novarocks_type_contract::PureCompileControl,
}
impl<'a, H> LocalLayoutCreation<'a, H> {
    pub fn new(
        storage: novarocks_type_contract::ValueTypeStorageLifetime,
        request: &'a mut dyn FnMut(std::alloc::Layout) -> std::result::Result<(), H>,
        temporary_request: &'a mut dyn FnMut(std::alloc::Layout) -> std::result::Result<(), H>,
        control: &'a dyn novarocks_type_contract::PureCompileControl,
    ) -> Self {
        Self {
            storage,
            request,
            temporary_request,
            control,
        }
    }
}

/// Only sink edges outside the root tree may be supplied by the fragment
/// envelope. All node-owned requirements are derived from the frozen nodes.
pub enum ExternalSinkRequirement {
    Result,
    ExchangeOutput { branch: usize },
}

/// The exact runtime capabilities removed while a decoded plan is frozen.
/// They are keyed by local node identity and never enter LocalProgramGraph.
pub struct LocalRuntimeBindings {
    pub(crate) scans: BTreeMap<lp::ProgramNodeId, Arc<dyn super::scan::ScanSource>>,
    pub(crate) writers: BTreeMap<lp::ProgramNodeId, super::table_writer::TableWriterRuntimeBinding>,
    pub(crate) finishers:
        BTreeMap<lp::ProgramNodeId, super::table_finish::TableFinishRuntimeBinding>,
}

impl LocalRuntimeBindings {
    fn new() -> Self {
        Self {
            scans: BTreeMap::new(),
            writers: BTreeMap::new(),
            finishers: BTreeMap::new(),
        }
    }

    pub fn scan_count(&self) -> usize {
        self.scans.len()
    }

    pub fn writer_count(&self) -> usize {
        self.writers.len()
    }

    pub fn finish_count(&self) -> usize {
        self.finishers.len()
    }
}

impl ExecPlan {
    /// Derive a specialization profile from the decoded root's exact slot
    /// metadata and the effective pipeline width for this task.
    pub fn local_compile_profile_with_creator<H>(
        &self,
        pipeline_dop: NonZeroUsize,
        root_sink_dop: Option<NonZeroUsize>,
        creation: &mut LocalLayoutCreation<'_, H>,
    ) -> CreationResult<lp::CompileProfile, H> {
        let schema = crate::exec::pipeline::builder::output_chunk_schema_for_node_with_creator(
            &self.root, creation,
        )?
        .ok_or_else(|| LocalProgramLoweringError::new("root has no output chunk schema"))?;
        let identity = layout_with_creator(&schema, creation)?
            .identity_for_compile(creation.control)
            .map_err(|error| match error {
                lp::LayoutCompileError::Control(error) => {
                    LocalProgramCreationFailure::Control(error)
                }
                error => LocalProgramCreationFailure::Lowering(LocalProgramLoweringError::new(
                    error.to_string(),
                )),
            })?;
        Ok(lp::CompileProfile::new(
            pipeline_dop,
            root_sink_dop,
            identity,
            lp::KernelAbiVersion::CURRENT,
        ))
    }

    pub fn into_local_program_and_bindings_with_creator<H>(
        self,
        profile: lp::CompileProfile,
        scan_sources: BTreeMap<i32, lp::StaticConnectorScan>,
        sink_requirements: Vec<ExternalSinkRequirement>,
        sink: lp::StaticSinkProgram,
        creation: LocalLayoutCreation<'_, H>,
    ) -> CreationResult<(lp::LocalProgramGraph, LocalRuntimeBindings), H> {
        self.lower_with_optional_sink_and_creator(
            profile,
            scan_sources,
            sink_requirements,
            Some(sink),
            creation,
        )
    }

    fn lower_with_optional_sink_and_creator<H>(
        self,
        profile: lp::CompileProfile,
        mut scan_sources: BTreeMap<i32, lp::StaticConnectorScan>,
        sink_requirements: Vec<ExternalSinkRequirement>,
        sink: Option<lp::StaticSinkProgram>,
        creation: LocalLayoutCreation<'_, H>,
    ) -> CreationResult<(lp::LocalProgramGraph, LocalRuntimeBindings), H> {
        preflight(&self.root)?;
        let ExecPlan { arena, root } = self;
        let expressions = Arc::new(arena.into_immutable().map_err(|error| {
            LocalProgramLoweringError::new(format!("freeze local expressions: {error}"))
        })?);
        let mut lowering = Lowering {
            nodes: Vec::new(),
            requirements: Vec::new(),
            filter_bindings: BTreeSet::new(),
            scan_sources: &mut scan_sources,
            runtime: LocalRuntimeBindings::new(),
            creation,
        };
        let root = lowering.node(root)?;
        if !lowering.scan_sources.is_empty() {
            return Err(LocalProgramLoweringError::new(format!(
                "unused static scan recipes for node IDs {:?}",
                lowering.scan_sources.keys().collect::<Vec<_>>()
            ))
            .into());
        }
        let layout = lowering.nodes[root.index()].output_layout().clone();
        for sink_requirement in sink_requirements {
            lowering.requirements.push(match sink_requirement {
                ExternalSinkRequirement::Result => lp::BindingRequirement::ResultSink {
                    layout: layout.clone(),
                },
                ExternalSinkRequirement::ExchangeOutput { branch } => {
                    let branch_layout = if let Some(sink) = &sink {
                        let branch_program = sink.branches().get(branch).ok_or_else(|| {
                            LocalProgramLoweringError::new(
                                "exchange output branch has no static sink branch",
                            )
                        })?;
                        let creation = &mut lowering.creation;
                        layout
                            .project_by_slots_typed_with_creator(
                                branch_program.output_columns(),
                                creation.storage.clone(),
                                |request| {
                                    (creation.request)(request)
                                        .map_err(lp::LayoutCreationFailure::Creator)
                                },
                                creation.control,
                            )
                            .map_err(LocalProgramCreationFailure::from)?
                    } else {
                        layout.clone()
                    };
                    lp::BindingRequirement::ExchangeOutput {
                        branch,
                        layout: branch_layout,
                    }
                }
            });
        }
        let requirements = lp::BindingRequirements::try_new(lowering.requirements)
            .map_err(|error| LocalProgramLoweringError::new(error.to_string()))?;
        let program = lp::LocalProgramGraph::try_new_with_sink(
            lowering.nodes,
            root,
            expressions,
            profile,
            requirements,
            sink,
        )
        .map_err(|error| LocalProgramLoweringError::new(error.to_string()))?;
        Ok((program, lowering.runtime))
    }
}

/// Bound input before a recursive consume. This keeps malformed hand-built
/// trees from overflowing the call stack or allocating a giant graph.
fn preflight(root: &ExecNode) -> Result<()> {
    let mut pending = vec![(root, 1usize)];
    let mut count = 0usize;
    while let Some((node, depth)) = pending.pop() {
        count += 1;
        if count > lp::MAX_PROGRAM_NODES || depth > lp::MAX_PROGRAM_NODE_DEPTH {
            return Err(LocalProgramLoweringError::new(
                "local plan exceeds node/depth limit",
            ));
        }
        macro_rules! push {
            ($child:expr) => {
                pending.push(($child, depth + 1))
            };
        }
        match &node.kind {
            ExecNodeKind::AssertNumRows(n) => push!(&n.input),
            ExecNodeKind::Project(n) => push!(&n.input),
            ExecNodeKind::Unpivot(n) => push!(&n.input),
            ExecNodeKind::Filter(n) => push!(&n.input),
            ExecNodeKind::Repeat(n) => push!(&n.input),
            ExecNodeKind::ChangeEventExpand(n) => push!(&n.input),
            ExecNodeKind::Limit(n) => push!(&n.input),
            ExecNodeKind::Aggregate(n) => push!(&n.input),
            ExecNodeKind::Sort(n) => push!(&n.input),
            ExecNodeKind::TableFunction(n) => push!(&n.input),
            ExecNodeKind::Analytic(n) => push!(&n.input),
            ExecNodeKind::RuntimeFilterConsumer(n) => push!(&n.input),
            ExecNodeKind::TableWriter(n) => push!(&n.input),
            ExecNodeKind::UnionAll(n) => {
                pending.extend(n.inputs.iter().map(|child| (child, depth + 1)))
            }
            ExecNodeKind::SetOp(n) => {
                pending.extend(n.inputs.iter().map(|child| (child, depth + 1)))
            }
            ExecNodeKind::TableFinish(n) => {
                pending.extend(n.inputs.iter().map(|child| (child, depth + 1)))
            }
            ExecNodeKind::Join(n) => {
                push!(&n.left);
                push!(&n.right);
            }
            ExecNodeKind::NestedLoopJoin(n) => {
                push!(&n.left);
                push!(&n.right);
            }
            ExecNodeKind::Values(_) | ExecNodeKind::ExchangeSource(_) | ExecNodeKind::Scan(_) => {}
        }
    }
    Ok(())
}

fn values_creation_failure<H>(
    error: lp::ValuesCreationFailure<H>,
) -> LocalProgramCreationFailure<H> {
    match error {
        lp::ValuesCreationFailure::Creator(cause) => LocalProgramCreationFailure::Creator(cause),
        lp::ValuesCreationFailure::Control(cause) => LocalProgramCreationFailure::Control(cause),
        lp::ValuesCreationFailure::Resource(cause) => LocalProgramCreationFailure::Resource(cause),
        lp::ValuesCreationFailure::Values(cause) => {
            LocalProgramLoweringError::new(cause.to_string()).into()
        }
    }
}

struct Lowering<'a, 'creator, H> {
    nodes: Vec<lp::ProgramNode>,
    requirements: Vec<lp::BindingRequirement>,
    filter_bindings: BTreeSet<i32>,
    scan_sources: &'a mut BTreeMap<i32, lp::StaticConnectorScan>,
    runtime: LocalRuntimeBindings,
    creation: LocalLayoutCreation<'creator, H>,
}

impl<H> Lowering<'_, '_, H> {
    fn node(&mut self, node: ExecNode) -> CreationResult<lp::ProgramNodeId, H> {
        let schema = crate::exec::pipeline::builder::output_chunk_schema_for_node_with_creator(
            &node,
            &mut self.creation,
        )?
        .ok_or_else(|| LocalProgramLoweringError::new("node has no output chunk schema"))?;
        let output = layout_with_creator(&schema, &mut self.creation)?;
        let (node_id, kind) = self.kind(node.kind, &output)?;
        let id = lp::ProgramNodeId::new(self.nodes.len());
        self.nodes.push(lp::ProgramNode::new(node_id, kind, output));
        Ok(id)
    }

    fn filter_requirement(&mut self, id: u32) -> CreationResult<(), H> {
        let id = i32::try_from(id).map_err(|_| {
            LocalProgramLoweringError::new("runtime filter binding ID overflows i32")
        })?;
        if self.filter_bindings.insert(id) {
            self.requirements
                .push(lp::BindingRequirement::RuntimeFilter { binding_id: id });
        }
        Ok(())
    }

    fn consumers(
        &mut self,
        bindings: Vec<RuntimeFilterConsumerBinding>,
    ) -> CreationResult<Vec<lp::FilterConsumerAtExpr>, H> {
        bindings
            .into_iter()
            .map(|binding| {
                self.filter_requirement(binding.binding_id())?;
                let consumer = freeze_consumer(&binding)?;
                Ok(lp::FilterConsumerAtExpr {
                    expr_id: expr(binding.expr_id),
                    consumer,
                })
            })
            .collect()
    }

    fn producers(
        &mut self,
        bindings: impl IntoIterator<Item = (ExprId, usize, rf::RuntimeFilterProducerContract)>,
    ) -> CreationResult<Vec<lp::FilterProducerAtExpr>, H> {
        bindings
            .into_iter()
            .map(|(expr_id, key_ordinal, contract)| {
                self.filter_requirement(contract.binding_id().get())?;
                Ok(lp::FilterProducerAtExpr {
                    expr_id: expr(expr_id),
                    key_ordinal,
                    producer: freeze_producer(&contract)?,
                })
            })
            .collect()
    }

    fn kind(
        &mut self,
        node: ExecNodeKind,
        output: &lp::StaticLayout,
    ) -> CreationResult<(i32, lp::ProgramNodeKind), H> {
        use lp::ProgramNodeKind as P;
        let mapped = match node {
            ExecNodeKind::AssertNumRows(n) => {
                let input = self.node(*n.input)?;
                let mode = match n.mode {
                    super::assert::AssertNumRowsMode::Global {
                        desired_num_rows,
                        assertion,
                        subquery_string,
                    } => lp::AssertRowsMode::Global {
                        desired_num_rows,
                        assertion: match assertion {
                            super::assert::Assertion::Eq => lp::RowAssertion::Eq,
                            super::assert::Assertion::Ne => lp::RowAssertion::Ne,
                            super::assert::Assertion::Lt => lp::RowAssertion::Lt,
                            super::assert::Assertion::Le => lp::RowAssertion::Le,
                            super::assert::Assertion::Gt => lp::RowAssertion::Gt,
                            super::assert::Assertion::Ge => lp::RowAssertion::Ge,
                        },
                        subquery_string: subquery_string.map(Arc::from),
                    },
                    super::assert::AssertNumRowsMode::PerKeyAtMostOne {
                        key_slots,
                        key_labels,
                        message_prefix,
                    } => lp::AssertRowsMode::PerKeyAtMostOne {
                        key_slots,
                        key_labels: key_labels.into_iter().map(Arc::from).collect(),
                        message_prefix: Arc::from(message_prefix),
                    },
                };
                (n.node_id, P::AssertNumRows { input, mode })
            }
            ExecNodeKind::Values(n) => {
                let creation = &mut self.creation;
                creation
                    .control
                    .checkpoint(novarocks_type_contract::CompilePhase::LowerProgram, 0)?;
                let layout=std::alloc::Layout::array::<arrow::array::ArrayRef>(n.chunk.record_batch().num_columns())
                    .map_err(|_|LocalProgramCreationFailure::Resource(novarocks_type_contract::owned_resources::layout::LayoutResourceError::Arithmetic))?;
                (creation.request)(layout).map_err(LocalProgramCreationFailure::Creator)?;
                let source = n
                    .chunk
                    .source_storage_lifetime_observed::<lp::ValuesCreationFailure<H>>(
                        creation.storage.clone(),
                        |layout| {
                            (creation.request)(layout).map_err(lp::ValuesCreationFailure::Creator)
                        },
                    )
                    .map_err(values_creation_failure)?;
                // Clone only the concrete column table; each actual ArrayRef
                // shares its original buffer/type backing under source custody.
                let batch = n.chunk.record_batch().clone();
                let mut request = |layout| {
                    (creation.request)(layout).map_err(LocalProgramCreationFailure::Creator)
                };
                let mut opaque = |_: usize| {
                    Err(LocalProgramCreationFailure::<H>::Lowering(
                        LocalProgramLoweringError::new(
                            "Values batch transfer performs no opaque allocation",
                        ),
                    ))
                };
                let mut values_creation = novarocks_type_contract::ValueSchemaCreation::new(
                    creation.storage.clone(),
                    &mut request,
                    &mut opaque,
                );
                let values = lp::StaticValues::try_new_with_creator(
                    batch,
                    output.clone(),
                    source,
                    &mut values_creation,
                    creation.control,
                )
                .map_err(|error| match error {
                    lp::ValuesCreationFailure::Creator(cause) => cause,
                    lp::ValuesCreationFailure::Control(cause) => {
                        LocalProgramCreationFailure::Control(cause)
                    }
                    lp::ValuesCreationFailure::Resource(cause) => {
                        LocalProgramCreationFailure::Resource(cause)
                    }
                    lp::ValuesCreationFailure::Values(cause) => {
                        LocalProgramLoweringError::new(cause.to_string()).into()
                    }
                })?;
                (n.node_id, P::Values { values })
            }
            ExecNodeKind::Project(n) => {
                let input = self.node(*n.input)?;
                let expr_slot_schemas = if let Some(slots) = n.expr_slot_schemas {
                    use novarocks_type_contract::owned_resources::layout::LayoutResourceError;
                    self.creation
                        .control
                        .checkpoint(novarocks_type_contract::CompilePhase::LowerProgram, 0)?;
                    if !slots.is_empty() {
                        (self.creation.request)(
                            std::alloc::Layout::array::<lp::ProjectExpressionSlot>(slots.len())
                                .map_err(|_| LayoutResourceError::Arithmetic)?,
                        )
                        .map_err(LocalProgramCreationFailure::Creator)?;
                    }
                    let mut saved = Vec::new();
                    saved.try_reserve_exact(slots.len()).map_err(|_| {
                        novarocks_type_contract::CompileControlError::ResourceExhausted
                    })?;
                    for slot in slots {
                        self.creation
                            .control
                            .checkpoint(novarocks_type_contract::CompilePhase::LowerProgram, 1)?;
                        if !slot.is_typed() {
                            return Err(LocalProgramLoweringError::new(
                                "project expression purpose lacks its complete typed binding",
                            )
                            .into());
                        }
                        saved.push(lp::ProjectExpressionSlot {
                            slot_id: slot.slot_id(),
                            field: Arc::clone(slot.field_ref()),
                            binding: slot.typed_binding().clone(),
                            storage: slot.storage_lifetime(),
                            unique_id: slot.unique_id(),
                        });
                    }
                    if saved.capacity() != saved.len() && !saved.is_empty() {
                        (self.creation.request)(
                            std::alloc::Layout::array::<lp::ProjectExpressionSlot>(saved.len())
                                .map_err(|_| LayoutResourceError::Arithmetic)?,
                        )
                        .map_err(LocalProgramCreationFailure::Creator)?;
                    }
                    self.creation
                        .control
                        .checkpoint(novarocks_type_contract::CompilePhase::LowerProgram, 0)?;
                    let boxed = saved.into_boxed_slice();
                    Some(lp::ProjectExpressionSlots::try_from_boxed_observed(
                        boxed,
                        self.creation.storage.clone(),
                        |layout| {
                            self.creation.control.checkpoint(
                                novarocks_type_contract::CompilePhase::LowerProgram,
                                0,
                            )?;
                            (self.creation.request)(layout)
                                .map_err(LocalProgramCreationFailure::Creator)
                        },
                    )?)
                } else {
                    None
                };
                (
                    n.node_id,
                    P::Project {
                        input,
                        is_subordinate: n.is_subordinate,
                        validate_final_result_input: n.validate_final_result_input,
                        exprs: n.exprs.into_iter().map(expr).collect(),
                        expr_slot_ids: n.expr_slot_ids,
                        expr_slot_schemas,
                        output_indices: n.output_indices,
                    },
                )
            }
            ExecNodeKind::Unpivot(n) => {
                let input = self.node(*n.input)?;
                (
                    n.node_id,
                    P::Unpivot {
                        input,
                        passthrough_columns: n
                            .passthrough_columns
                            .into_iter()
                            .map(|col| lp::UnpivotPassthrough {
                                input_slot_id: col.input_slot_id,
                                output_slot_id: col.output_slot_id,
                            })
                            .collect(),
                        value_output_slot_id: n.value_output_slot_id,
                        literal_output_slot_ids: n.literal_output_slot_ids,
                        value_mappings: n
                            .value_mappings
                            .into_iter()
                            .map(|mapping| lp::UnpivotMapping {
                                input_value_slot_id: mapping.input_value_slot_id,
                                constants: mapping
                                    .constants
                                    .into_iter()
                                    .map(freeze_unpivot_constant)
                                    .collect(),
                            })
                            .collect(),
                        max_output_rows: n.max_output_rows,
                        max_output_bytes: n.max_output_bytes,
                    },
                )
            }
            ExecNodeKind::Filter(n) => {
                let input = self.node(*n.input)?;
                (
                    n.node_id,
                    P::Filter {
                        input,
                        predicates: vec![expr(n.predicate)].into_boxed_slice(),
                    },
                )
            }
            ExecNodeKind::Repeat(n) => {
                let input = self.node(*n.input)?;
                (
                    n.node_id,
                    P::Repeat {
                        input,
                        null_slot_ids: n.null_slot_ids,
                        grouping_slot_ids: n.grouping_slot_ids,
                        grouping_list: n.grouping_list,
                        repeat_times: n.repeat_times,
                    },
                )
            }
            ExecNodeKind::ChangeEventExpand(n) => {
                let input = self.node(*n.input)?;
                (
                    n.node_id,
                    P::ChangeEventExpand {
                        input,
                        events: n
                            .events
                            .into_iter()
                            .map(|event| lp::ChangeEventSpec {
                                predicate: event.predicate.map(expr),
                                effect: event.effect,
                                assignments: event
                                    .assignments
                                    .into_iter()
                                    .map(|assignment| lp::ChangeEventOutputExpr {
                                        output_slot_id: assignment.output_slot_id,
                                        expr: assignment.expr.map(expr),
                                    })
                                    .collect(),
                            })
                            .collect(),
                        output_slot_ids: n.output_slot_ids,
                        effect_slot_id: n.effect_slot_id,
                    },
                )
            }
            ExecNodeKind::UnionAll(n) => (
                n.node_id,
                P::UnionAll {
                    inputs: n
                        .inputs
                        .into_iter()
                        .map(|input| self.node(input))
                        .collect::<CreationResult<_, H>>()?,
                },
            ),
            ExecNodeKind::Limit(n) => {
                let input = self.node(*n.input)?;
                (
                    n.node_id,
                    P::Limit {
                        input,
                        limit: n.limit,
                        offset: n.offset,
                    },
                )
            }
            other => return self.kind_remaining(other, output),
        };
        Ok(mapped)
    }

    fn kind_remaining(
        &mut self,
        node: ExecNodeKind,
        output: &lp::StaticLayout,
    ) -> CreationResult<(i32, lp::ProgramNodeKind), H> {
        use lp::ProgramNodeKind as P;
        let mapped = match node {
            ExecNodeKind::Scan(n) => {
                let (runtime_source, (node_id, filter_specs, conjunct_predicate, limit)) =
                    n.into_static_fields_with_source();
                let node_id = node_id
                    .ok_or_else(|| LocalProgramLoweringError::new("scan has no native node ID"))?;
                let source = self.scan_sources.remove(&node_id).ok_or_else(|| {
                    LocalProgramLoweringError::new(format!(
                        "missing static scan recipe for node {node_id}"
                    ))
                })?;
                let relation = source.recipe().draft().relation().table().header().clone();
                let id = lp::ProgramNodeId::new(self.nodes.len());
                self.runtime.scans.insert(id, runtime_source);
                self.requirements.push(lp::BindingRequirement::Scan {
                    node: id,
                    kind: lp::ScanSourceKind::TypedConnector { relation },
                    layout: output.clone(),
                });
                let runtime_filters = self.consumers(filter_specs)?;
                (
                    node_id,
                    P::Scan {
                        source: source.into(),
                        runtime_filters: runtime_filters.into(),
                        residuals: conjunct_predicate.map(expr).into_iter().collect(),
                        limit,
                    },
                )
            }
            ExecNodeKind::ExchangeSource(n) => {
                let id = lp::ProgramNodeId::new(self.nodes.len());
                self.requirements
                    .push(lp::BindingRequirement::ExchangeInput {
                        node: id,
                        layout: output.clone(),
                    });
                let runtime_filters = self.consumers(n.native_runtime_filter_specs)?;
                (
                    n.node_id,
                    P::ExchangeSource {
                        timeout: n.timeout,
                        runtime_filters: runtime_filters.into(),
                        hash_partition_exprs: n
                            .hash_partition_exprs
                            .into_iter()
                            .map(expr)
                            .collect(),
                    },
                )
            }
            ExecNodeKind::Aggregate(n) => {
                let input = self.node(*n.input)?;
                if n.functions.len() != n.resolved_aggregates.len() {
                    return Err(LocalProgramLoweringError::new(
                        "aggregate calls and resolved signatures differ",
                    )
                    .into());
                }
                let functions = n
                    .functions
                    .into_iter()
                    .zip(n.resolved_aggregates)
                    .map(|(function, resolved)| lp::StaticAggregateCall {
                        frozen: function.frozen,
                        state_interpretation: None,
                        name: Arc::from(function.name),
                        inputs: function.inputs.into_iter().map(expr).collect(),
                        input_is_intermediate: function.input_is_intermediate,
                        types: function
                            .types
                            .map(|types| lp::StaticAggregateTypeSignature {
                                intermediate_type: types.intermediate_type,
                                output_type: types.output_type,
                                input_arg_type: types.input_arg_type,
                            }),
                        order: lp::StaticAggregateOrder {
                            is_asc_order: function.order.is_asc_order,
                            nulls_first: function.order.nulls_first,
                            is_distinct: function.order.is_distinct,
                            group_concat_max_len: function.order.group_concat_max_len,
                        },
                        resolved,
                    })
                    .collect();
                let topn_filters = n
                    .runtime_filter_spec
                    .topn_producers
                    .into_iter()
                    .map(|producer| {
                        self.filter_requirement(producer.binding_id())?;
                        Ok(lp::AggregateTopNFilter {
                            group_key_expr: expr(producer.group_key_expr_id),
                            group_key_ordinal: producer.group_key_ordinal,
                            limit: producer.limit,
                            producer: freeze_producer(&producer.contract)?,
                        })
                    })
                    .collect::<CreationResult<Vec<_>, H>>()?
                    .into();
                (n.node_id, P::Aggregate { input, group_by: n.group_by.into_iter().map(expr).collect(), functions,
                    need_finalize: n.need_finalize, input_is_intermediate: n.input_is_intermediate, topn_filters,
                    streaming_preaggregation_mode: n.streaming_preaggregation_mode.map(|mode| match mode {
                        super::aggregate::StreamingPreaggregationMode::Auto => lp::StreamingPreaggregationMode::Auto,
                        super::aggregate::StreamingPreaggregationMode::ForceStreaming => lp::StreamingPreaggregationMode::ForceStreaming,
                        super::aggregate::StreamingPreaggregationMode::ForcePreaggregation => lp::StreamingPreaggregationMode::ForcePreaggregation,
                        super::aggregate::StreamingPreaggregationMode::LimitedMem => lp::StreamingPreaggregationMode::LimitedMem,
                    }) })
            }
            ExecNodeKind::Join(n) => {
                let left = self.node(*n.left)?;
                let right = self.node(*n.right)?;
                let runtime_filters =
                    self.producers(n.runtime_filter_execution.producers.into_iter().map(
                        |producer| {
                            (
                                producer.build_expr_id,
                                producer.build_key_index,
                                producer.contract,
                            )
                        },
                    ))?;
                (
                    n.node_id,
                    P::Join {
                        left,
                        right,
                        join_type: match n.join_type {
                            super::join::JoinType::Inner => lp::JoinType::Inner,
                            super::join::JoinType::LeftOuter => lp::JoinType::LeftOuter,
                            super::join::JoinType::RightOuter => lp::JoinType::RightOuter,
                            super::join::JoinType::FullOuter => lp::JoinType::FullOuter,
                            super::join::JoinType::LeftSemi => lp::JoinType::LeftSemi,
                            super::join::JoinType::RightSemi => lp::JoinType::RightSemi,
                            super::join::JoinType::LeftAnti => lp::JoinType::LeftAnti,
                            super::join::JoinType::RightAnti => lp::JoinType::RightAnti,
                            super::join::JoinType::NullAwareLeftAnti => {
                                lp::JoinType::NullAwareLeftAnti
                            }
                        },
                        distribution_mode: match n.distribution_mode {
                            super::join::JoinDistributionMode::Broadcast => {
                                lp::JoinDistributionMode::Broadcast
                            }
                            super::join::JoinDistributionMode::Partitioned => {
                                lp::JoinDistributionMode::Partitioned
                            }
                        },
                        left_layout: layout_with_creator(&n.left_chunk_schema, &mut self.creation)?,
                        right_layout: layout_with_creator(
                            &n.right_chunk_schema,
                            &mut self.creation,
                        )?,
                        join_scope_layout: layout_with_creator(
                            &n.join_scope_chunk_schema,
                            &mut self.creation,
                        )?,
                        probe_keys: n.probe_keys.into_iter().map(expr).collect(),
                        build_keys: n.build_keys.into_iter().map(expr).collect(),
                        eq_null_safe: n.eq_null_safe,
                        residual_predicate: n.residual_predicate.map(expr),
                        runtime_filters,
                        runtime_filter_consumers: vec![],
                    },
                )
            }
            ExecNodeKind::NestedLoopJoin(n) => {
                let left = self.node(*n.left)?;
                let right = self.node(*n.right)?;
                (
                    n.node_id,
                    P::NestedLoopJoin {
                        left,
                        right,
                        join_type: match n.join_type {
                            super::nljoin::NestedLoopJoinType::Inner => {
                                lp::NestedLoopJoinType::Inner
                            }
                            super::nljoin::NestedLoopJoinType::Cross => {
                                lp::NestedLoopJoinType::Cross
                            }
                            super::nljoin::NestedLoopJoinType::LeftOuter => {
                                lp::NestedLoopJoinType::LeftOuter
                            }
                            super::nljoin::NestedLoopJoinType::RightOuter => {
                                lp::NestedLoopJoinType::RightOuter
                            }
                            super::nljoin::NestedLoopJoinType::FullOuter => {
                                lp::NestedLoopJoinType::FullOuter
                            }
                            super::nljoin::NestedLoopJoinType::LeftSemi => {
                                lp::NestedLoopJoinType::LeftSemi
                            }
                            super::nljoin::NestedLoopJoinType::LeftAnti => {
                                lp::NestedLoopJoinType::LeftAnti
                            }
                            super::nljoin::NestedLoopJoinType::NullAwareLeftAnti => {
                                lp::NestedLoopJoinType::NullAwareLeftAnti
                            }
                        },
                        join_conjunct: n.join_conjunct.map(expr),
                        left_layout: layout_with_creator(&n.left_chunk_schema, &mut self.creation)?,
                        right_layout: layout_with_creator(
                            &n.right_chunk_schema,
                            &mut self.creation,
                        )?,
                        join_scope_layout: layout_with_creator(
                            &n.join_scope_chunk_schema,
                            &mut self.creation,
                        )?,
                    },
                )
            }
            ExecNodeKind::Sort(n) => {
                let input = self.node(*n.input)?;
                let sorts = |items: Vec<super::sort::SortExpression>| {
                    items
                        .into_iter()
                        .map(|item| lp::SortExpression {
                            expr: expr(item.expr),
                            asc: item.asc,
                            nulls_first: item.nulls_first,
                        })
                        .collect()
                };
                (
                    n.node_id,
                    P::Sort {
                        input,
                        use_top_n: n.use_top_n,
                        order_by: sorts(n.order_by),
                        limit: n.limit,
                        offset: n.offset,
                        topn_type: match n.topn_type {
                            super::sort::SortTopNType::RowNumber => lp::SortTopNType::RowNumber,
                            super::sort::SortTopNType::Rank => lp::SortTopNType::Rank,
                            super::sort::SortTopNType::DenseRank => lp::SortTopNType::DenseRank,
                        },
                        max_buffered_rows: n.max_buffered_rows,
                        max_buffered_bytes: n.max_buffered_bytes,
                        partition_exprs: sorts(n.partition_exprs),
                        partition_limit: n.partition_limit,
                    },
                )
            }
            ExecNodeKind::TableFunction(n) => {
                let input = self.node(*n.input)?;
                (
                    n.node_id,
                    P::TableFunction {
                        input,
                        function_name: Arc::from(n.function_name),
                        param_slots: n.param_slots,
                        outer_slots: n.outer_slots,
                        fn_result_slots: n.fn_result_slots,
                        fn_result_required: n.fn_result_required,
                        is_left_join: n.is_left_join,
                        param_types: n.param_types,
                        ret_types: n.ret_types,
                        output_slot_sources: n
                            .output_slot_sources
                            .into_iter()
                            .map(|source| match source {
                                super::table_function::TableFunctionOutputSlot::Outer { slot } => {
                                    lp::TableFunctionOutputSlot::Outer { slot }
                                }
                                super::table_function::TableFunctionOutputSlot::Result {
                                    index,
                                } => lp::TableFunctionOutputSlot::Result { index },
                            })
                            .collect(),
                    },
                )
            }
            ExecNodeKind::Analytic(n) => {
                let input = self.node(*n.input)?;
                let frame = n.window.map(freeze_window_frame);
                (
                    n.node_id,
                    P::Analytic {
                        input,
                        partition_exprs: n.partition_exprs.into_iter().map(expr).collect(),
                        order_by_exprs: n.order_by_exprs.into_iter().map(expr).collect(),
                        // Every legacy call copies its node's one frame.
                        functions: n
                            .functions
                            .into_iter()
                            .map(|function| freeze_window_function(function, frame))
                            .collect(),
                        output_columns: n
                            .output_columns
                            .into_iter()
                            .map(|col| match col {
                                super::analytic::AnalyticOutputColumn::InputSlotId(slot) => {
                                    lp::AnalyticOutputColumn::InputSlotId(slot)
                                }
                                super::analytic::AnalyticOutputColumn::Window(index) => {
                                    lp::AnalyticOutputColumn::Window(index)
                                }
                            })
                            .collect(),
                    },
                )
            }
            ExecNodeKind::SetOp(n) => {
                let inputs = n
                    .inputs
                    .into_iter()
                    .map(|node| self.node(node))
                    .collect::<CreationResult<_, H>>()?;
                (
                    n.node_id,
                    P::SetOp {
                        kind: match n.kind {
                            super::set_op::SetOpKind::Intersect => lp::SetOpKind::Intersect,
                            super::set_op::SetOpKind::Except => lp::SetOpKind::Except,
                        },
                        inputs,
                    },
                )
            }
            ExecNodeKind::RuntimeFilterConsumer(n) => {
                let input = self.node(*n.input)?;
                let bindings = self.consumers(n.bindings)?;
                (
                    n.owner_node_id,
                    P::RuntimeFilterConsumer { input, bindings },
                )
            }
            ExecNodeKind::TableWriter(n) => {
                let node_id = n.node_id;
                let (
                    runtime_binding,
                    (child, target, expected_schema, projection, writer_schema, aggregate),
                ) = n.into_static_parts_with_binding();
                let input = self.node(*child)?;
                let id = lp::ProgramNodeId::new(self.nodes.len());
                self.runtime.writers.insert(id, runtime_binding);
                self.requirements.push(lp::BindingRequirement::TableWriter {
                    node: id,
                    layout: output.clone(),
                });
                let exact_projection_layout =
                    layout_with_creator(projection.chunk_schema(), &mut self.creation)?;
                let expected_layout =
                    layout_with_creator(projection.provider_chunk_schema(), &mut self.creation)?;
                if expected_layout.schema().as_ref() != expected_schema.as_ref() {
                    return Err(LocalProgramLoweringError::new(
                        "writer projection layout differs from expected writer schema",
                    )
                    .into());
                }
                let projection = projection
                    .into_static(exact_projection_layout)
                    .map_err(LocalProgramLoweringError::new)?;
                (
                    node_id,
                    P::TableWriter {
                        input,
                        target,
                        expected_layout,
                        projection,
                        writer_multiplex_layout: layout_with_creator(
                            writer_schema.chunk_schema(),
                            &mut self.creation,
                        )?,
                        partial_preparation: aggregate.prepared,
                        partial_aggregates: aggregate
                            .calls
                            .into_iter()
                            .map(|call| lp::WriterPartialAggregateCall {
                                frozen: call.frozen,
                                input_slot_id: call.input_slot_id,
                                function_name: call.function_name,
                                resolved: call.resolved,
                                intermediate_slot_id: call.intermediate_slot_id,
                            })
                            .collect(),
                    },
                )
            }
            ExecNodeKind::TableFinish(n) => {
                let node_id = n.node_id;
                let (
                    runtime_binding,
                    (children, expected_targets, writer_schema, root_schema, final_aggregate),
                ) = n.into_static_parts_with_binding();
                let inputs = children
                    .into_iter()
                    .map(|node| self.node(node))
                    .collect::<CreationResult<_, H>>()?;
                let id = lp::ProgramNodeId::new(self.nodes.len());
                self.runtime.finishers.insert(id, runtime_binding);
                self.requirements.push(lp::BindingRequirement::TableFinish {
                    node: id,
                    layout: output.clone(),
                });
                (
                    node_id,
                    P::TableFinish {
                        inputs,
                        expected_targets,
                        writer_multiplex_layout: layout_with_creator(
                            writer_schema.chunk_schema(),
                            &mut self.creation,
                        )?,
                        root_result_layout: layout_with_creator(
                            root_schema.chunk_schema(),
                            &mut self.creation,
                        )?,
                        final_aggregates: lp::WriterFinalAggregatePlan {
                            prepared: final_aggregate.prepared,
                            calls: final_aggregate
                                .calls
                                .into_iter()
                                .map(|call| lp::WriterFinalAggregateCall {
                                    frozen: call.frozen,
                                    function_name: call.function_name,
                                    resolved: call.resolved,
                                    intermediate_input_slot_id: call.intermediate_input_slot_id,
                                    final_output_slot_id: call.final_output_slot_id,
                                })
                                .collect(),
                            unpivot: final_aggregate.unpivot.map(|unpivot| {
                                lp::WriterGroupedUnpivotPlan {
                                    grouping_input_slot_id: unpivot.grouping_input_slot_id,
                                    grouping_output_slot_id: unpivot.grouping_output_slot_id,
                                    passthrough_output_slot_id: unpivot.passthrough_output_slot_id,
                                    value_output_slot_id: unpivot.value_output_slot_id,
                                    literal_output_slot_ids: unpivot.literal_output_slot_ids,
                                    mappings: unpivot
                                        .mappings
                                        .into_iter()
                                        .map(|mapping| lp::WriterGroupedUnpivotMapping {
                                            grouping_key: mapping.grouping_key,
                                            input_value_slot_id: mapping.input_value_slot_id,
                                            constants: mapping
                                                .constants
                                                .into_iter()
                                                .map(freeze_unpivot_constant)
                                                .collect(),
                                        })
                                        .collect(),
                                    max_output_rows: unpivot.max_output_rows,
                                    max_output_bytes: unpivot.max_output_bytes,
                                }
                            }),
                        },
                    },
                )
            }
            ExecNodeKind::AssertNumRows(_)
            | ExecNodeKind::Values(_)
            | ExecNodeKind::Project(_)
            | ExecNodeKind::Unpivot(_)
            | ExecNodeKind::Filter(_)
            | ExecNodeKind::Repeat(_)
            | ExecNodeKind::ChangeEventExpand(_)
            | ExecNodeKind::UnionAll(_)
            | ExecNodeKind::Limit(_) => {
                unreachable!("the first lowering match already consumed this variant")
            }
        };
        Ok(mapped)
    }
}

fn layout_with_creator<H>(
    schema: &ChunkSchemaRef,
    creation: &mut LocalLayoutCreation<'_, H>,
) -> CreationResult<lp::StaticLayout, H> {
    use novarocks_type_contract::{
        CompileCheckpoints, CompilePhase, TypedValueBinding, owned_resources::layout::arc_layout,
    };
    let mut work = CompileCheckpoints::try_new(creation.control, CompilePhase::LowerProgram)?;
    let count = schema.slots().len();
    let bindings_layout = std::alloc::Layout::array::<(TypedValueBinding, Option<i32>)>(count)
        .map_err(|_| {
            LocalProgramCreationFailure::Resource(
                novarocks_type_contract::owned_resources::layout::LayoutResourceError::Arithmetic,
            )
        })?;
    work.flush()?;
    (creation.request)(bindings_layout).map_err(LocalProgramCreationFailure::Creator)?;
    let mut bindings = Vec::with_capacity(count);
    for slot in schema.slots() {
        work.step()?;
        bindings.push((
            slot.typed_binding().clone_observed(|| work.step())?,
            slot.unique_id(),
        ));
    }
    let slots_layout = arc_layout(
        std::alloc::Layout::array::<novarocks_types::SlotId>(count).map_err(|_| {
            LocalProgramCreationFailure::Resource(
                novarocks_type_contract::owned_resources::layout::LayoutResourceError::Arithmetic,
            )
        })?,
    )
    .map_err(LocalProgramCreationFailure::Resource)?;
    work.flush()?;
    (creation.request)(slots_layout).map_err(LocalProgramCreationFailure::Creator)?;
    let slots = Arc::from(schema.slot_ids());
    work.step()?;
    let arrow_schema = schema.arrow_schema_ref();
    work.step()?;
    let storage = creation.storage.clone();
    work.finish()?;
    lp::StaticLayout::try_new_typed_with_creator(
        arrow_schema,
        slots,
        bindings,
        storage,
        |layout| (creation.request)(layout).map_err(lp::LayoutCreationFailure::Creator),
        creation.control,
    )
    .map_err(Into::into)
}

fn expr(id: ExprId) -> lp::ProgramExprId {
    lp::ProgramExprId::new(id.0)
}

fn freeze_unpivot_constant(value: super::unpivot::UnpivotConstant) -> lp::UnpivotConstant {
    match value {
        super::unpivot::UnpivotConstant::Scalar { expr_id, nullable } => {
            lp::UnpivotConstant::Scalar {
                expr_id: expr(expr_id),
                nullable,
            }
        }
        super::unpivot::UnpivotConstant::Int32List(values) => {
            lp::UnpivotConstant::Int32List(values)
        }
        super::unpivot::UnpivotConstant::Utf8Map(values) => lp::UnpivotConstant::Utf8Map(
            values
                .into_iter()
                .map(|(key, value)| (Arc::from(key), Arc::from(value)))
                .collect(),
        ),
    }
}

fn freeze_filter_contract(
    contract: &rf::RuntimeFilterExecutionContract,
) -> Result<lp::StaticFilterContract> {
    match contract {
        rf::RuntimeFilterExecutionContract::Membership(schema) => {
            let value_type = schema.value_type().ok_or_else(|| LocalProgramLoweringError::new(
                "runtime-filter membership contract has no complete execution declaration"))?;
            lp::StaticFilterContract::membership_from_canonical(
                schema.canonical_schema().clone(), value_type.clone(),
            ).map_err(|cause| LocalProgramLoweringError::new(cause.to_string()))
        }
        rf::RuntimeFilterExecutionContract::Ordered(order) => Ok(lp::StaticFilterContract::Ordered {
            keys: order.keys().iter().map(|key| Ok(lp::FilterOrderKey {
                value_type: key.value_type().ok_or_else(|| LocalProgramLoweringError::new(
                    "runtime-filter ordered key has no complete execution declaration"))?.clone(),
                direction: match key.direction() {
                    rf::contribution::RuntimeOrderSortDirection::Ascending => lp::FilterSortDirection::Ascending,
                    rf::contribution::RuntimeOrderSortDirection::Descending => lp::FilterSortDirection::Descending,
                },
                null_order: match key.null_order() {
                    rf::contribution::RuntimeOrderNullOrder::First => lp::FilterNullOrder::First,
                    rf::contribution::RuntimeOrderNullOrder::Last => lp::FilterNullOrder::Last,
                },
            })).collect::<Result<Vec<_>>>()?.into(),
            comparator_digest: order.comparator_digest(), contract_digest: order.digest(),
        }),
    }
}

fn freeze_reduction(reduction: rf::RuntimeFilterReduction) -> Result<lp::FilterReduction> {
    Ok(match reduction {
        rf::RuntimeFilterReduction::SetUnion => lp::FilterReduction::SetUnion,
        rf::RuntimeFilterReduction::TightenOrderedBound => lp::FilterReduction::TightenOrderedBound,
        rf::RuntimeFilterReduction::MergeTopKSummary { k, .. } => {
            lp::FilterReduction::MergeTopKSummary {
                k: std::num::NonZeroU32::new(k)
                    .ok_or_else(|| LocalProgramLoweringError::new("zero top-K runtime filter"))?,
            }
        }
    })
}

fn freeze_consumer(binding: &RuntimeFilterConsumerBinding) -> Result<lp::StaticFilterConsumer> {
    let contract = binding.contract();
    let activation = match contract.activation() {
        rf::ConsumerActivation::BlockingSnapshot => lp::FilterConsumerActivation::BlockingSnapshot,
        rf::ConsumerActivation::NonBlockingLive { late_apply } => {
            lp::FilterConsumerActivation::NonBlockingLive {
                late_apply: match late_apply {
                    rf::RuntimeFilterLateApplyGranularity::Row => {
                        lp::FilterLateApplyGranularity::Row
                    }
                    rf::RuntimeFilterLateApplyGranularity::Batch => {
                        lp::FilterLateApplyGranularity::Batch
                    }
                    rf::RuntimeFilterLateApplyGranularity::RowGroup => {
                        lp::FilterLateApplyGranularity::RowGroup
                    }
                    rf::RuntimeFilterLateApplyGranularity::Split => {
                        lp::FilterLateApplyGranularity::Split
                    }
                    rf::RuntimeFilterLateApplyGranularity::File => {
                        lp::FilterLateApplyGranularity::File
                    }
                },
            }
        }
    };
    lp::StaticFilterConsumer::try_new(
        contract.binding_id().get(),
        contract.channel_id().get(),
        activation,
        freeze_filter_contract(contract.contract())?,
        freeze_reduction(contract.reduction())?,
    )
    .map_err(|error| LocalProgramLoweringError::new(error.to_string()))
}

fn freeze_producer(
    contract: &rf::RuntimeFilterProducerContract,
) -> Result<lp::StaticFilterProducer> {
    let kind = match contract.kind() {
        rf::RuntimeFilterProducerKind::Membership => lp::FilterProducerKind::Membership,
        rf::RuntimeFilterProducerKind::OrderedBound => lp::FilterProducerKind::OrderedBound,
        rf::RuntimeFilterProducerKind::TopKSummary => lp::FilterProducerKind::TopKSummary,
        rf::RuntimeFilterProducerKind::FinalDomain => lp::FilterProducerKind::FinalDomain,
    };
    lp::StaticFilterProducer::try_new(
        contract.binding_id().get(),
        contract.channel_id().get(),
        kind,
        freeze_filter_contract(contract.contract())?,
        freeze_reduction(contract.reduction())?,
    )
    .map_err(|error| LocalProgramLoweringError::new(error.to_string()))
}

fn freeze_window_frame(frame: super::analytic::WindowFrame) -> lp::WindowFrame {
    let boundary = |boundary| match boundary {
        super::analytic::WindowBoundary::CurrentRow => lp::WindowBoundary::CurrentRow,
        super::analytic::WindowBoundary::Preceding(value) => lp::WindowBoundary::Preceding(value),
        super::analytic::WindowBoundary::Following(value) => lp::WindowBoundary::Following(value),
    };
    lp::WindowFrame {
        start: frame.start.map(boundary),
        end: frame.end.map(boundary),
        window_type: match frame.window_type {
            super::analytic::WindowType::Rows => lp::WindowType::Rows,
            super::analytic::WindowType::Range => lp::WindowType::Range,
        },
    }
}

fn freeze_window_function(
    function: super::analytic::WindowFunctionSpec,
    frame: Option<lp::WindowFrame>,
) -> lp::StaticWindowFunction {
    use super::analytic::WindowFunctionKind as W;
    let ignore_nulls = match function.kind {
        W::FirstValue { ignore_nulls }
        | W::FirstValueRewrite { ignore_nulls }
        | W::LastValue { ignore_nulls }
        | W::Lead { ignore_nulls }
        | W::Lag { ignore_nulls } => ignore_nulls,
        _ => false,
    };
    lp::StaticWindowFunction {
        kind: match function.kind {
            W::RowNumber => lp::WindowFunctionKind::RowNumber,
            W::Rank => lp::WindowFunctionKind::Rank,
            W::DenseRank => lp::WindowFunctionKind::DenseRank,
            W::CumeDist => lp::WindowFunctionKind::CumeDist,
            W::PercentRank => lp::WindowFunctionKind::PercentRank,
            W::Ntile => lp::WindowFunctionKind::Ntile,
            W::FirstValue { .. } => lp::WindowFunctionKind::FirstValue,
            W::FirstValueRewrite { .. } => lp::WindowFunctionKind::FirstValueRewrite,
            W::LastValue { .. } => lp::WindowFunctionKind::LastValue,
            W::Lead { .. } => lp::WindowFunctionKind::Lead,
            W::Lag { .. } => lp::WindowFunctionKind::Lag,
            W::SessionNumber => lp::WindowFunctionKind::SessionNumber,
            W::Count => lp::WindowFunctionKind::Count,
            W::Sum => lp::WindowFunctionKind::Sum,
            W::Avg => lp::WindowFunctionKind::Avg,
            W::Min => lp::WindowFunctionKind::Min,
            W::Max => lp::WindowFunctionKind::Max,
            W::BitmapUnion => lp::WindowFunctionKind::BitmapUnion,
            W::BitmapUnionCount => lp::WindowFunctionKind::BitmapUnionCount,
            W::MaxBy => lp::WindowFunctionKind::MaxBy,
            W::MinBy => lp::WindowFunctionKind::MinBy,
            W::VarianceSamp => lp::WindowFunctionKind::VarianceSamp,
            W::StddevSamp => lp::WindowFunctionKind::StddevSamp,
            W::BoolOr => lp::WindowFunctionKind::BoolOr,
            W::CovarPop => lp::WindowFunctionKind::CovarPop,
            W::CovarSamp => lp::WindowFunctionKind::CovarSamp,
            W::Corr => lp::WindowFunctionKind::Corr,
            W::ArrayAgg {
                is_distinct,
                is_asc_order,
                nulls_first,
            } => lp::WindowFunctionKind::ArrayAgg {
                is_distinct,
                is_asc_order,
                nulls_first,
            },
            W::ApproxTopK => lp::WindowFunctionKind::ApproxTopK,
        },
        args: function.args.into_iter().map(expr).collect(),
        return_type: function.return_type,
        frozen_aggregate: function
            .aggregate_binding
            .as_ref()
            .and_then(|binding| binding.frozen.clone()),
        aggregate_binding: function
            .aggregate_binding
            .map(|binding| (Arc::from(binding.function_name), binding.resolved)),
        frame,
        ignore_nulls,
    }
}

#[cfg(test)]
mod tests {
    use std::num::{NonZeroU32, NonZeroUsize};

    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use novarocks_types::SlotId;

    use super::*;
    use crate::exec::chunk::Chunk;
    use crate::exec::expr::{ExprArena, ExprNode, LiteralValue};
    use crate::exec::node::filter::FilterNode;
    use crate::exec::node::limit::LimitNode;
    use crate::exec::node::repeat::RepeatNode;
    use crate::exec::node::values::ValuesNode;

    fn typed_values() -> ExecNode {
        let chunk = crate::exec::chunk::typed_fixture::columns(
            &["v"],
            &[SlotId::new(1)],
            &[novarocks_type_contract::FunctionValueType::new(
                DataType::Int64,
                false,
            )],
            vec![Arc::new(Int64Array::from(vec![1]))],
            1,
        );
        ExecNode {
            kind: ExecNodeKind::Values(ValuesNode { chunk, node_id: 1 }),
        }
    }

    fn values() -> ExecNode {
        typed_values()
    }

    #[test]
    fn partition_only_topn_survives_lowering_and_caps_each_partition() {
        use crate::exec::node::sort::{SortExpression, SortNode, SortTopNType};
        use crate::exec::operators::SortProcessorFactory;
        use crate::exec::pipeline::operator::ConstructionFailure;
        use crate::exec::pipeline::operator_factory::OperatorFactory;
        use arrow::array::{Array, ArrayRef};
        use novarocks_type_contract::{FunctionValueType, ValueTypeStorageLifetime};
        struct DeclarationControl;
        impl novarocks_type_contract::PureCompileControl for DeclarationControl {
            fn checkpoint(
                &self,
                _: novarocks_type_contract::CompilePhase,
                _: u32,
            ) -> std::result::Result<(), novarocks_type_contract::CompileControlError> {
                Ok(())
            }
        }

        let schema = Arc::new(Schema::new(vec![
            Field::new("partition", DataType::Int64, false),
            Field::new("value", DataType::Int64, false),
        ]));
        // This pure fixture publishes its explicit source declaration before
        // runtime preparation. No type is inferred from a batch or annotation.
        let value = FunctionValueType::new(DataType::Int64, false);
        let layout = lp::StaticLayout::try_new_typed_for_compile(
            schema,
            Arc::from([SlotId::new(1), SlotId::new(2)]),
            vec![
                (value.typed_binding().clone(), None),
                (value.typed_binding().clone(), None),
            ],
            ValueTypeStorageLifetime::pure(),
            |_| Ok(()),
            &DeclarationControl,
        )
        .unwrap();
        let state = crate::runtime::runtime_state::RuntimeState::default();
        let tracker =
            crate::runtime::mem_tracker::MemTracker::new_root("partition-only-sort-construction");
        let creator = crate::exec::pipeline::operator::input_construction::try_runtime_input_construction_factory(
            crate::exec::pipeline::pipeline::representation_construction_profile().unwrap(),
            tracker.clone(),
            state.error_state(),
        ).unwrap();
        let chunk_schema = creator.create_compiled_chunk_schema(&layout).unwrap();
        let owner = creator.begin_typed_construction(&chunk_schema).unwrap();
        owner
            .request(std::alloc::Layout::array::<ArrayRef>(2).unwrap())
            .unwrap();
        let chunk = Chunk::try_new_typed_with_columns_owned_observed::<ConstructionFailure, _>(
            chunk_schema,
            vec![
                Arc::new(Int64Array::from(vec![2, 1, 2, 1, 2, 1])) as ArrayRef,
                Arc::new(Int64Array::from(vec![20, 10, 21, 11, 22, 12])) as ArrayRef,
            ],
            6,
            owner.storage_lifetime(),
            |step| owner.observe(0, &[], step),
        )
        .unwrap();
        let mut arena = ExprArena::default();
        let partition = arena.push_value_typed(ExprNode::SlotId(SlotId::new(1)), value);
        let plan = ExecPlan {
            arena,
            root: ExecNode {
                kind: ExecNodeKind::Sort(SortNode {
                    input: Box::new(ExecNode {
                        kind: ExecNodeKind::Values(ValuesNode {
                            chunk: chunk.clone(),
                            node_id: 1,
                        }),
                    }),
                    node_id: 2,
                    use_top_n: true,
                    order_by: vec![],
                    limit: None,
                    offset: 0,
                    topn_type: SortTopNType::RowNumber,
                    max_buffered_rows: None,
                    max_buffered_bytes: None,
                    partition_exprs: vec![SortExpression {
                        expr: partition,
                        asc: true,
                        nulls_first: true,
                    }],
                    partition_limit: Some(2),
                }),
            },
        };
        let profile =
            crate::exec::chunk::typed_fixture::profile(&plan, NonZeroUsize::new(1).unwrap(), None);
        let (program, _runtime) = crate::exec::chunk::typed_fixture::lower(
            plan,
            profile,
            vec![ExternalSinkRequirement::Result],
            lp::StaticSinkProgram::Result,
        );
        let lp::ProgramNodeKind::Sort {
            order_by,
            partition_exprs,
            partition_limit,
            limit,
            offset,
            ..
        } = program.nodes()[1].kind()
        else {
            panic!("expected frozen Sort");
        };
        assert!(order_by.is_empty());
        assert_eq!(*partition_limit, Some(2));
        let runtime_partition_exprs = partition_exprs
            .iter()
            .map(|key| SortExpression {
                expr: ExprId(key.expr.index()),
                asc: key.asc,
                nulls_first: key.nulls_first,
            })
            .collect();
        let runtime_arena = Arc::new(
            ExprArena::from_immutable(program.expressions())
                .expect("frozen explicit partition expression"),
        );
        let factory = SortProcessorFactory::new_topn(
            2,
            runtime_arena,
            vec![],
            *limit,
            *offset,
            SortTopNType::RowNumber,
            None,
            None,
            runtime_partition_exprs,
            *partition_limit,
        )
        .with_input_layout(program.nodes()[0].output_layout().clone());
        let mut op = factory.create(1, 0);
        op.set_mem_tracker(tracker);
        op.set_construction_factory(creator);
        op.prepare().unwrap();
        let processor = op.as_processor_mut().unwrap();
        processor.push_chunk(&state, chunk).unwrap();
        processor.set_finishing(&state).unwrap();
        let output = processor.pull_chunk(&state).unwrap().unwrap();
        assert_eq!(output.len(), 4);
        let partitions = output.columns()[0]
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let values = output.columns()[1]
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let rows: BTreeSet<_> = (0..output.len())
            .map(|row| {
                assert!(!partitions.is_null(row));
                assert!(!values.is_null(row));
                (partitions.value(row), values.value(row))
            })
            .collect();
        assert_eq!(rows, BTreeSet::from([(1, 10), (1, 11), (2, 20), (2, 21)]));
        assert!(processor.pull_chunk(&state).unwrap().is_none());
    }

    #[test]
    fn lowering_consumes_values_and_keeps_expression_and_sink_exact() {
        let input = typed_values();
        let mut arena = ExprArena::default();
        let predicate = arena.push_value_typed(
            ExprNode::Literal(LiteralValue::Bool(true)),
            novarocks_type_contract::FunctionValueType::new(DataType::Boolean, false),
        );
        let plan = ExecPlan {
            arena,
            root: ExecNode {
                kind: ExecNodeKind::Filter(FilterNode {
                    input: Box::new(input),
                    node_id: 2,
                    predicate,
                }),
            },
        };
        let profile =
            crate::exec::chunk::typed_fixture::profile(&plan, NonZeroUsize::new(1).unwrap(), None);
        let (program, runtime) = crate::exec::chunk::typed_fixture::lower(
            plan,
            profile,
            vec![ExternalSinkRequirement::Result],
            lp::StaticSinkProgram::Result,
        );
        assert_eq!(runtime.scan_count(), 0);
        assert_eq!(program.nodes().len(), 2);
        assert!(matches!(
            program.nodes()[0].kind(),
            lp::ProgramNodeKind::Values { .. }
        ));
        assert!(
            matches!(program.nodes()[1].kind(), lp::ProgramNodeKind::Filter { predicates, .. } if predicates.as_ref() == [lp::ProgramExprId::new(0)])
        );
        assert_eq!(program.requirements().entries().len(), 1);
        assert!(matches!(
            program.requirements().entries()[0],
            lp::BindingRequirement::ResultSink { .. }
        ));
    }

    #[test]
    fn lowering_rejects_excess_depth_before_recursive_consume() {
        let mut root = typed_values();
        for id in 2..=66 {
            root = ExecNode {
                kind: ExecNodeKind::Limit(LimitNode {
                    input: Box::new(root),
                    node_id: id,
                    limit: Some(1),
                    offset: 0,
                }),
            };
        }
        let plan = ExecPlan {
            arena: ExprArena::default(),
            root,
        };
        let profile =
            crate::exec::chunk::typed_fixture::profile(&plan, NonZeroUsize::new(1).unwrap(), None);
        let error = crate::exec::chunk::typed_fixture::try_lower_with_scans(
            plan,
            profile,
            BTreeMap::new(),
            vec![],
            lp::StaticSinkProgram::Noop,
        )
        .err()
        .expect("original excessive depth is rejected");
        let error = match error {
            LocalProgramCreationFailure::Lowering(error) => error,
            other => panic!("original lowering depth cause was replaced: {other:?}"),
        };
        assert!(error.to_string().contains("node/depth limit"));
    }

    #[test]
    fn repeat_grouping_slot_is_available_to_static_exchange_sink() {
        let input = values();
        let plan = ExecPlan {
            arena: ExprArena::default(),
            root: ExecNode {
                kind: ExecNodeKind::Repeat(RepeatNode {
                    input: Box::new(input),
                    node_id: 2,
                    null_slot_ids: vec![vec![SlotId::new(1)], vec![], vec![SlotId::new(1)]],
                    grouping_slot_ids: vec![SlotId::new(11)],
                    grouping_list: vec![vec![0, 1, 2]],
                    repeat_times: 3,
                }),
            },
        };
        let profile =
            crate::exec::chunk::typed_fixture::profile(&plan, NonZeroUsize::new(1).unwrap(), None);
        let branch = lp::StaticStreamBranch::try_new(
            3,
            novarocks_execution_contract::DataStreamPartitionType::Unpartitioned,
            vec![],
            vec![SlotId::new(1), SlotId::new(11)],
            None,
        )
        .unwrap();
        let sink = lp::StaticSinkProgram::try_data_stream(
            branch,
            Arc::new(
                lp::ImmutableExpressions::try_new(Vec::new(), false, Default::default(), None)
                    .unwrap(),
            ),
        )
        .unwrap();
        let (program, runtime) = crate::exec::chunk::typed_fixture::lower(
            plan,
            profile,
            vec![ExternalSinkRequirement::ExchangeOutput { branch: 0 }],
            sink,
        );
        assert_eq!(runtime.scan_count(), 0);
        assert!(
            program.nodes()[program.root().index()]
                .output_layout()
                .is_typed()
        );
        assert_eq!(
            program.nodes()[program.root().index()]
                .output_layout()
                .slots(),
            &[SlotId::new(1), SlotId::new(11)]
        );
        assert!(
            program.nodes()[program.root().index()]
                .output_layout()
                .schema()
                .field(0)
                .is_nullable()
        );
    }
}
