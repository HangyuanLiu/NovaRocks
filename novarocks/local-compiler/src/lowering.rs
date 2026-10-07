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

//! Physical package lowering into the mandatory checked local owner.

use crate::{
    ProviderValidatedFragment,
    assert_rows::lower_assert_rows,
    change_events::lower_change_events,
    channels::{ChannelLoweringError, resolve_tree_channels},
    exchange::lower_exchange_source,
    expressions::{ExpressionLoweringError, lower_expressions_with_unions, prepare_calls},
    repeat::{RepeatLoweringError, lower_repeat},
    scan::{admit_scan, lower_scan},
    sort::lower_sort,
    stream_sink::lower_stream_sink,
    topn::lower_topn,
    unpivot::{UnpivotLoweringError, UnpivotLoweringInput, lower_unpivot},
    values::{lower_values, retired_values_uses},
};
use arrow_schema::Schema;
use novarocks_functions::{ConstantPolicy, PureEngineFunctionCatalog};
use novarocks_local_program::*;
use novarocks_physical_plan::{
    Distribution, EdgeKind, ExpressionRootRole, FragmentSink, NodeId, NodeKind, RowMultiplicity,
};
use novarocks_type_contract::{
    CompileCheckpoints, CompileControlError, CompilePhase, FunctionValueType, PureCompileControl,
};
use std::{
    collections::{BTreeMap, BTreeSet},
    error::Error,
    fmt,
    num::NonZeroUsize,
    sync::Arc,
    time::Duration,
};

/// Host-admitted values are explicit; the compiler authors the actual layout
/// digest. Neither topology nor resource defaults are inferred from the plan.
#[derive(Clone, Copy, Debug)]
pub struct LocalCompileOptions {
    pub pipeline_dop: NonZeroUsize,
    pub root_sink_dop: Option<NonZeroUsize>,
    pub kernel_abi: KernelAbiVersion,
    pub constants: ConstantPolicy,
    /// Host-admitted receive wait copied into every compiled ExchangeSource.
    /// The compiler never defaults it or derives it from the plan.
    pub exchange_wait: Duration,
}

#[derive(Debug)]
pub enum FragmentCompileError {
    Control(CompileControlError),
    Unsupported {
        node: Option<NodeId>,
        feature: &'static str,
    },
    Invalid(&'static str),
    Owner {
        phase: &'static str,
        error: Box<dyn Error + Send + Sync>,
    },
}
impl fmt::Display for FragmentCompileError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Control(error) => error.fmt(f),
            Self::Unsupported { node, feature } => {
                write!(f, "unsupported local lowering at {node:?}: {feature}")
            }
            Self::Invalid(message) => write!(f, "invalid local lowering: {message}"),
            Self::Owner { phase, error } => write!(f, "local lowering {phase}: {error}"),
        }
    }
}
impl Error for FragmentCompileError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Control(error) => Some(error),
            Self::Owner { error, .. } => Some(error.as_ref()),
            _ => None,
        }
    }
}
impl From<CompileControlError> for FragmentCompileError {
    fn from(error: CompileControlError) -> Self {
        Self::Control(error)
    }
}
impl From<novarocks_type_contract::ValueTypeError> for FragmentCompileError {
    fn from(error: novarocks_type_contract::ValueTypeError) -> Self {
        Self::Owner {
            phase: "value type",
            error: Box::new(error),
        }
    }
}
macro_rules! owner_error {
    ($ty:ident, $phase:literal) => {
        impl From<$ty> for FragmentCompileError {
            fn from(error: $ty) -> Self {
                match error {
                    $ty::Control(error) => Self::Control(error),
                    error => Self::Owner {
                        phase: $phase,
                        error: Box::new(error),
                    },
                }
            }
        }
    };
}
impl From<ChannelLoweringError> for FragmentCompileError {
    fn from(error: ChannelLoweringError) -> Self {
        match error {
            ChannelLoweringError::Control(cause) => Self::Control(cause),
            ChannelLoweringError::Invalid(message) => Self::Invalid(message),
            error => Self::Owner {
                phase: "input channels",
                error: Box::new(error),
            },
        }
    }
}
owner_error!(ExpressionLoweringError, "expressions");
impl From<RepeatLoweringError> for FragmentCompileError {
    fn from(error: RepeatLoweringError) -> Self {
        match error {
            RepeatLoweringError::Control(cause) => Self::Control(cause),
            RepeatLoweringError::Invalid(message) => Self::Invalid(message),
            error => Self::Owner {
                phase: "Repeat",
                error: Box::new(error),
            },
        }
    }
}
impl From<UnpivotLoweringError> for FragmentCompileError {
    fn from(error: UnpivotLoweringError) -> Self {
        match error {
            UnpivotLoweringError::Control(c) => Self::Control(c),
            UnpivotLoweringError::Invalid(m) => Self::Invalid(m),
            e => Self::Owner {
                phase: "Unpivot",
                error: Box::new(e),
            },
        }
    }
}
owner_error!(LayoutCompileError, "layout");
owner_error!(ValuesCompileError, "values");
owner_error!(BindingRequirementsCompileError, "requirements");
owner_error!(ProgramCompileError, "graph");
owner_error!(ProgramControlFlowError, "control flow");
impl From<ProgramRootBindingError> for FragmentCompileError {
    fn from(error: ProgramRootBindingError) -> Self {
        match error {
            ProgramRootBindingError::Control(cause)
            | ProgramRootBindingError::Roots(ProgramExpressionRootError::Control(cause)) => {
                Self::Control(cause)
            }
            error => Self::Owner {
                phase: "root correspondence",
                error: Box::new(error),
            },
        }
    }
}
owner_error!(ProgramResolvedCallsError, "resolved calls");
owner_error!(ProgramExpressionTypeError, "definition types");
owner_error!(ProgramChannelTypeError, "channel types");
owner_error!(ProgramLexicalBindingError, "lexical bindings");
owner_error!(LocalProgramCompileError, "final owner");

/// This entry consumes the provider intermediate rather than retaining a
/// second package beside the resulting graph. Unsupported families remain
/// explicit while the compiler is integrated; they cannot enter legacy decode.
pub fn compile_fragment(
    input: ProviderValidatedFragment,
    functions: &PureEngineFunctionCatalog,
    options: LocalCompileOptions,
    control: &dyn PureCompileControl,
) -> Result<LocalProgram, FragmentCompileError> {
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::LowerProgram)?;
    let result = lower(input, functions, options, &mut work);
    if matches!(&result, Err(FragmentCompileError::Control(_))) {
        return result;
    }
    work.finish()?;
    result
}

fn lower(
    input: ProviderValidatedFragment,
    functions: &PureEngineFunctionCatalog,
    options: LocalCompileOptions,
    work: &mut CompileCheckpoints<'_>,
) -> Result<LocalProgram, FragmentCompileError> {
    // Each validated provider recipe moves into its one lowered owner.
    let (package, mut reads, writes) = input.into_parts();
    let package = &package;
    let physical = package.fragment();
    let dop = u32::try_from(options.pipeline_dop.get())
        .map_err(|_| FragmentCompileError::Invalid("DOP exceeds physical domain"))?;
    let domain = physical.dop_domain();
    work.step()?;
    if dop < domain.min
        || dop > domain.max
        || (domain.requires_power_of_two && !dop.is_power_of_two())
    {
        return Err(FragmentCompileError::Invalid(
            "DOP is outside physical domain",
        ));
    }
    if options.root_sink_dop.is_some_and(|dop| dop.get() != 1) {
        return Err(FragmentCompileError::Unsupported {
            node: None,
            feature: "result sink width for singleton source",
        });
    }
    let outbound = &package.cuts().outbound;
    // A Result sink publishes the result port; a Stream sink publishes its
    // one exact outbound cut. Every other sink family remains explicit.
    let stream_cut = match physical.sink() {
        FragmentSink::Result => {
            if !outbound.is_empty() {
                return Err(FragmentCompileError::Invalid(
                    "result sink has an outbound exchange cut",
                ));
            }
            None
        }
        FragmentSink::Stream { edge } => {
            let [cut] = &outbound[..] else {
                return Err(FragmentCompileError::Invalid(
                    "stream sink requires exactly one outbound cut",
                ));
            };
            if cut.edge != *edge {
                return Err(FragmentCompileError::Invalid(
                    "stream sink edge differs from its outbound cut",
                ));
            }
            if cut.kind != EdgeKind::Stream
                || cut.change_stream_writer.is_some()
                || cut.writer_result.is_some()
            {
                return Err(FragmentCompileError::Unsupported {
                    node: None,
                    feature: "outbound CTE, change-stream or writer-result edge",
                });
            }
            if matches!(
                cut.partitioning.source,
                Distribution::Unconstrained | Distribution::RoundRobin
            ) {
                return Err(FragmentCompileError::Unsupported {
                    node: None,
                    feature: "unconstrained or round-robin stream partitioning",
                });
            }
            Some(cut)
        }
        FragmentSink::Multicast { .. } | FragmentSink::Router { .. } | FragmentSink::Noop => {
            return Err(FragmentCompileError::Unsupported {
                node: None,
                feature: "multicast, router or noop sink",
            });
        }
    };
    for cut in package.cuts().inbound.iter() {
        work.step()?;
        if cut.kind != EdgeKind::Stream
            || cut.change_stream_writer.is_some()
            || cut.writer_result.is_some()
        {
            return Err(FragmentCompileError::Unsupported {
                node: Some(cut.destination_node),
                feature: "inbound CTE, change-stream or writer-result edge",
            });
        }
    }
    // Provider reads are admitted per scan below; writers and runtime-filter
    // graphs remain explicit.
    if !writes.is_empty()
        || !package.cuts().runtime_filters.is_empty()
        || !physical.runtime_filters().is_empty()
    {
        return Err(FragmentCompileError::Unsupported {
            node: None,
            feature: "provider writer or runtime-filter graph",
        });
    }
    // The result port exists exactly for a Result sink; a stream producer
    // has no result labels and never borrows another fragment's port.
    let result = package.result();
    if stream_cut.is_none() && result.is_none() {
        return Err(FragmentCompileError::Invalid("missing result port"));
    }
    if stream_cut.is_some() && result.is_some() {
        return Err(FragmentCompileError::Invalid(
            "stream sink carries a result port",
        ));
    }
    // Borrowed input order determines the bounded postorder. Each physical
    // node has one execution owner; shared subgraphs remain unsupported.
    let mut order = Vec::new();
    let mut visited = BTreeSet::new();
    let mut scans = BTreeSet::new();
    let mut stack = Vec::new();
    let mut expanded_nodes = physical.nodes().len();
    let mut derived_definitions = 0usize;
    let mut channel_count = 0usize;
    for node in physical.nodes().values() {
        let pieces = if matches!(
            node.kind,
            NodeKind::SetOp {
                kind: novarocks_physical_plan::SetOperationKind::UnionAll,
                ..
            }
        ) {
            node.inputs
                .len()
                .checked_add(1)
                .ok_or(CompileControlError::ResourceExhausted)?
        } else {
            1
        };
        channel_count = channel_count
            .checked_add(
                pieces
                    .checked_mul(node.output.columns.len())
                    .ok_or(CompileControlError::ResourceExhausted)?,
            )
            .ok_or(CompileControlError::ResourceExhausted)?;
        if matches!(
            node.kind,
            NodeKind::SetOp {
                kind: novarocks_physical_plan::SetOperationKind::UnionAll,
                ..
            }
        ) {
            expanded_nodes = expanded_nodes
                .checked_add(node.inputs.len())
                .ok_or(CompileControlError::ResourceExhausted)?;
            derived_definitions = derived_definitions
                .checked_add(
                    node.inputs
                        .len()
                        .checked_mul(node.output.columns.len())
                        .ok_or(CompileControlError::ResourceExhausted)?,
                )
                .ok_or(CompileControlError::ResourceExhausted)?;
        }
        work.step()?;
    }
    let original_flow = package.expression_uses().flow();
    let mut references = original_flow
        .uses()
        .len()
        .checked_add(derived_definitions)
        .ok_or(CompileControlError::ResourceExhausted)?;
    for invocation in original_flow.uses().values() {
        references = references
            .checked_add(invocation.arguments.len())
            .ok_or(CompileControlError::ResourceExhausted)?;
        work.step()?;
    }
    if channel_count > MAX_PROGRAM_TYPED_CHANNELS
        || expanded_nodes
            .checked_mul(2)
            .is_none_or(|n| n > MAX_PROFILE_REFERENCES)
        || expanded_nodes > MAX_PROGRAM_NODES
        || expanded_nodes > MAX_PROGRAM_EXPANDED_OCCURRENCES
        || physical
            .expressions()
            .len()
            .checked_add(derived_definitions)
            .is_none_or(|n| n > novarocks_type_contract::MAX_CONTROL_DEFINITIONS)
        || original_flow
            .domains()
            .len()
            .checked_add(derived_definitions)
            .is_none_or(|n| n > novarocks_type_contract::MAX_CONTROL_DEFINITIONS)
        || original_flow
            .uses()
            .len()
            .checked_add(derived_definitions)
            .is_none_or(|n| n > novarocks_type_contract::MAX_CONTROL_DEFINITIONS)
        || references > novarocks_type_contract::MAX_CONTROL_USE_REFERENCES
    {
        return Err(CompileControlError::ResourceExhausted.into());
    }
    std::alloc::Layout::array::<(NodeId, bool, usize)>(expanded_nodes)
        .map_err(|_| CompileControlError::ResourceExhausted)?;
    work.flush()?;
    stack
        .try_reserve_exact(expanded_nodes)
        .map_err(|_| CompileControlError::ResourceExhausted)?;
    order
        .try_reserve_exact(physical.nodes().len())
        .map_err(|_| CompileControlError::ResourceExhausted)?;
    stack.push((physical.root(), false, 1usize));
    while let Some((id, exiting, depth)) = stack.pop() {
        work.step()?;
        if exiting {
            order.push(id);
            continue;
        }
        if depth > MAX_PROGRAM_NODE_DEPTH {
            return Err(CompileControlError::ResourceExhausted.into());
        }
        if !visited.insert(id) {
            return Err(FragmentCompileError::Unsupported {
                node: Some(id),
                feature: "shared or cyclic physical input",
            });
        }
        let node = physical
            .nodes()
            .get(&id)
            .ok_or(FragmentCompileError::Invalid("missing physical node"))?;
        // A partitioned single-copy layout only says how rows are placed
        // across instances. Families with a whole-relation meaning (global
        // Sort, Single/Final TopN, Limit, global row-count assertion) reach
        // here only over the Singleton input the checked physical contract
        // requires, and a per-key assertion only over a key-colocated one. A
        // Partial row-count TopN prunes each instance's own rows wherever they
        // are placed; its gather-and-Final sequence is a checked plan fact. A
        // family that consumes per-driver key co-location must author its own
        // local partitioning instead of relying on this. Copied rows and
        // broadcast placement stay refused.
        if !matches!(
            node.output_properties.distribution,
            Distribution::Singleton
                | Distribution::Unconstrained
                | Distribution::RoundRobin
                | Distribution::Hash { .. }
                | Distribution::BucketShuffle { .. }
        ) || node.output_properties.row_multiplicity != RowMultiplicity::SingleCopy
        {
            return Err(FragmentCompileError::Unsupported {
                node: Some(id),
                feature: "replicated or broadcast source-tree properties",
            });
        }
        let union = matches!(
            node.kind,
            NodeKind::SetOp {
                kind: novarocks_physical_plan::SetOperationKind::UnionAll,
                ..
            }
        );
        let supported = match &node.kind {
            NodeKind::Values { .. } | NodeKind::ExchangeSource { .. } | NodeKind::Scan { .. } => {
                node.inputs.is_empty()
            }
            NodeKind::Project { .. }
            | NodeKind::Limit { .. }
            | NodeKind::AssertOneRow(_)
            | NodeKind::Repeat { .. }
            | NodeKind::Unpivot { .. }
            | NodeKind::ChangeEventExpand { .. } => node.inputs.len() == 1,
            NodeKind::Filter { predicates } => predicates.len() == 1 && node.inputs.len() == 1,
            // Every row-count TopN phase; a grouped-state reduction has no
            // local owner.
            NodeKind::Sort {
                mode: novarocks_physical_plan::SortMode::Global,
                ..
            }
            | NodeKind::TopN {
                reduction: novarocks_physical_plan::TopNReduction::Rows,
                ..
            } => node.inputs.len() == 1,
            NodeKind::SetOp {
                kind: novarocks_physical_plan::SetOperationKind::UnionAll,
                ..
            } => node.inputs.len() >= 2,
            _ => false,
        };
        if !supported {
            return Err(FragmentCompileError::Unsupported {
                node: Some(id),
                feature: "node family or occurrence shape",
            });
        }
        if matches!(node.kind, NodeKind::Scan { .. }) {
            admit_scan(node, reads.get(&id), work)?;
            scans.insert(id);
        }
        stack.push((id, true, depth));
        for &child in node.inputs.iter().rev() {
            stack.push((child, false, depth + if union { 2 } else { 1 }));
            work.step()?;
        }
    }
    order.reverse();
    if order.len() != physical.nodes().len() {
        return Err(FragmentCompileError::Invalid(
            "unrepresented physical nodes",
        ));
    }
    // Each admitted scan found its recipe; a recipe for any other node is
    // never silently left behind.
    work.step()?;
    if reads.len() != scans.len() {
        return Err(FragmentCompileError::Invalid(
            "provider read recipe names no admitted scan node",
        ));
    }
    // Expansion conservatively loses distribution knowledge. It still has
    // the exact singleton child and one driver; only descendants of this
    // actual expansion can consume that uncertainty. A runtime-split scan is
    // an unconstrained source in its own right: its rows land on any instance
    // and driver, and only its transparent Project/Filter/Limit descendants
    // and a Partial row-count TopN inherit that placement. The partial keeps
    // a subset of each instance's rows where they already are; its Final
    // reads them only through a checked gather.
    let mut properties = BTreeMap::<NodeId, (bool, bool, bool)>::new();
    for &id in order.iter().rev() {
        let node = &physical.nodes()[&id];
        let mut expanded = false;
        for child in &node.inputs {
            expanded |= properties.get(child).is_some_and(|p| p.0);
            work.step()?;
        }
        let sorted = node.inputs.len() == 1 && properties.get(&node.inputs[0]).is_some_and(|p| p.1);
        let changes = matches!(node.kind, NodeKind::ChangeEventExpand { .. });
        let transparent = matches!(
            node.kind,
            NodeKind::Project { .. } | NodeKind::Filter { .. } | NodeKind::Limit { .. }
        );
        let partial_rows = matches!(
            node.kind,
            NodeKind::TopN {
                phase: novarocks_physical_plan::TopNPhase::Partial { .. },
                reduction: novarocks_physical_plan::TopNReduction::Rows,
                ..
            }
        );
        let scan_rooted = matches!(node.kind, NodeKind::Scan { .. })
            || ((transparent || partial_rows)
                && node.inputs.len() == 1
                && properties.get(&node.inputs[0]).is_some_and(|p| p.2));
        let unknown = node.output_properties.distribution == Distribution::Unconstrained;
        work.step()?;
        if (unknown && !expanded && !changes && !scan_rooted)
            || ((expanded || changes) && options.pipeline_dop.get() != 1)
        {
            return Err(FragmentCompileError::Unsupported {
                node: Some(id),
                feature: "change-event source-chain distribution or driver count",
            });
        }
        // Every row-count TopN phase emits its instance's window as one
        // ordered stream on one driver, so its declared ordering holds for
        // the whole instance output, as the property law states it.
        let global = matches!(
            node.kind,
            NodeKind::Sort {
                mode: novarocks_physical_plan::SortMode::Global,
                ..
            } | NodeKind::TopN {
                reduction: novarocks_physical_plan::TopNReduction::Rows,
                ..
            }
        );
        if !(node.output_properties.ordering.is_empty() || global || (sorted && transparent)) {
            return Err(FragmentCompileError::Unsupported {
                node: Some(id),
                feature: "ordering lacks supported global-sort source",
            });
        }
        properties.insert(
            id,
            (
                expanded || changes,
                global || (sorted && transparent),
                scan_rooted,
            ),
        );
    }
    work.flush()?;
    let channels_plan = resolve_tree_channels(package, &order, work.control())?;
    work.flush()?;
    let expressions = lower_expressions_with_unions(
        package,
        options.constants,
        &channels_plan.inputs,
        &channels_plan.unions,
        work.control(),
    )?;
    work.flush()?;
    let tokens = prepare_calls(package, &expressions, functions, work.control())?;
    let mut nodes: Vec<ProgramNode> = Vec::new();
    let mut local_ids = BTreeMap::new();
    let mut channels = Vec::new();
    let mut operators = Vec::new();
    let mut allowed = BTreeSet::new();
    let mut union_roots = Vec::new();
    let mut source_requirements = Vec::new();
    let mut exchange_inputs = BTreeMap::new();
    let mut scan_inputs = BTreeMap::new();
    // Local nodes whose layout is a provider scan layout unchanged.
    let mut scan_layouts = BTreeSet::new();
    crate::assert_rows::reserve_vec(&mut nodes, expanded_nodes, work)?;
    crate::assert_rows::reserve_vec(&mut operators, expanded_nodes, work)?;
    crate::assert_rows::reserve_vec(&mut channels, channel_count, work)?;
    crate::assert_rows::reserve_vec(&mut union_roots, derived_definitions, work)?;
    for &source in order.iter().rev() {
        work.step()?;
        let node = &physical.nodes()[&source];
        let planned = channels_plan
            .nodes
            .get(&source)
            .ok_or(FragmentCompileError::Invalid(
                "missing planned node channels",
            ))?;
        let id = planned.local;
        if let Some(branches) = channels_plan.unions.get(&source) {
            let definitions =
                expressions
                    .union_ids
                    .get(&source)
                    .ok_or(FragmentCompileError::Invalid(
                        "missing UnionAll definitions",
                    ))?;
            work.flush()?;
            let emitted = crate::union::lower_union(
                package,
                node,
                id,
                branches,
                definitions,
                &planned.slots,
                work.control(),
            )?;
            let owner = LocalOperatorId::new(
                u32::try_from(id.index()).map_err(|_| CompileControlError::ResourceExhausted)?,
            );
            for (branch, definitions) in branches.iter().zip(definitions) {
                for (ordinal, (input, &definition)) in
                    branch.sources.iter().zip(definitions).enumerate()
                {
                    union_roots.push(crate::union_flow::UnionRoot {
                        node: branch.normalizer,
                        ordinal: u32::try_from(ordinal)
                            .map_err(|_| CompileControlError::ResourceExhausted)?,
                        definition,
                        source: input.input.source,
                    });
                    work.step()?;
                }
            }
            let source_id = DiagnosticSourceNodeId::new(source.get());
            allowed.insert(source_id);
            local_ids.insert(source, id);
            for (piece, emitted_node) in emitted.into_iter().enumerate() {
                let emitted_id = emitted_node
                    .local_id()
                    .ok_or(FragmentCompileError::Invalid(
                        "missing UnionAll local node identity",
                    ))?;
                if emitted_id.index() != nodes.len() {
                    return Err(FragmentCompileError::Invalid("UnionAll schedule differs"));
                }
                for (ordinal, value) in node.output.columns.iter().enumerate() {
                    work.flush()?;
                    channels.push((
                        ProgramChannelSite::Layout {
                            node: emitted_id,
                            role: ProgramChannelLayoutRole::NodeOutput,
                            ordinal: u32::try_from(ordinal)
                                .map_err(|_| CompileControlError::ResourceExhausted)?,
                        },
                        physical.values()[value].ty.clone(),
                    ));
                    work.step()?;
                }
                operators.push(LocalOperatorProvenance {
                    id: LocalOperatorId::new(
                        u32::try_from(emitted_id.index())
                            .map_err(|_| CompileControlError::ResourceExhausted)?,
                    ),
                    lowered_nodes: Box::from([emitted_id]),
                    sources: Box::from([source_id]),
                    origin: LocalOperatorOrigin::Split {
                        piece: u32::try_from(piece)
                            .map_err(|_| CompileControlError::ResourceExhausted)?,
                    },
                    cost_owner: owner,
                    metrics: OperatorMetricAggregation {
                        cpu_time: MetricAggregation::Sum,
                        wall_time: MetricAggregation::Maximum,
                        peak_retained_bytes: MetricAggregation::Maximum,
                    },
                });
                nodes.push(emitted_node);
                work.step()?;
            }
            continue;
        }
        if id.index() != nodes.len() {
            return Err(FragmentCompileError::Invalid(
                "channel schedule differs from node schedule",
            ));
        }
        local_ids.insert(source, id);
        let source_id = DiagnosticSourceNodeId::new(source.get());
        allowed.insert(source_id);
        let (kind, layout) =
            match &node.kind {
                NodeKind::Values { .. } => {
                    work.flush()?;
                    lower_values(package, node, &expressions, &planned.slots, work.control())?
                }
                NodeKind::ExchangeSource { .. } => {
                    work.flush()?;
                    let lowered = lower_exchange_source(
                        package,
                        node,
                        &planned.slots,
                        options.exchange_wait,
                        work.control(),
                    )?;
                    source_requirements.push(BindingRequirement::ExchangeInput {
                        node: id,
                        layout: lowered.layout.clone(),
                    });
                    exchange_inputs.insert(id, lowered.input);
                    (lowered.kind, lowered.layout)
                }
                NodeKind::Scan { .. } => {
                    let recipe = reads.remove(&source).ok_or(FragmentCompileError::Invalid(
                        "missing provider read recipe",
                    ))?;
                    work.flush()?;
                    let lowered = lower_scan(
                        node,
                        id,
                        recipe,
                        &planned.slots,
                        &expressions.ids,
                        work.control(),
                    )?;
                    source_requirements.push(lowered.requirement);
                    scan_inputs.insert(id, lowered.input);
                    scan_layouts.insert(id);
                    (lowered.kind, lowered.layout)
                }
                NodeKind::Sort { .. } => {
                    let child = *local_ids
                        .get(&node.inputs[0])
                        .ok_or(FragmentCompileError::Invalid("missing lowered sort child"))?;
                    work.flush()?;
                    lower_sort(
                        node,
                        child,
                        nodes[child.index()].output_layout(),
                        &expressions.ids,
                        work.control(),
                    )?
                }
                NodeKind::TopN { .. } => {
                    let child = *local_ids
                        .get(&node.inputs[0])
                        .ok_or(FragmentCompileError::Invalid("missing lowered TopN child"))?;
                    work.flush()?;
                    lower_topn(
                        node,
                        child,
                        nodes[child.index()].output_layout(),
                        &expressions.ids,
                        work.control(),
                    )?
                }
                NodeKind::Filter { predicates } => {
                    let child = *local_ids
                        .get(&node.inputs[0])
                        .ok_or(FragmentCompileError::Invalid("missing lowered child"))?;
                    let predicate = *expressions.ids.get(&predicates[0]).ok_or(
                        FragmentCompileError::Invalid("missing predicate definition"),
                    )?;
                    (
                        ProgramNodeKind::Filter {
                            input: child,
                            predicate,
                        },
                        nodes[child.index()].output_layout().clone(),
                    )
                }
                NodeKind::Limit { limit, offset } => {
                    let child = *local_ids
                        .get(&node.inputs[0])
                        .ok_or(FragmentCompileError::Invalid("missing lowered child"))?;
                    let limit = limit
                        .map(usize::try_from)
                        .transpose()
                        .map_err(|_| FragmentCompileError::Invalid("limit exceeds host range"))?;
                    let offset = usize::try_from(*offset)
                        .map_err(|_| FragmentCompileError::Invalid("offset exceeds host range"))?;
                    (
                        ProgramNodeKind::Limit {
                            input: child,
                            limit,
                            offset,
                        },
                        nodes[child.index()].output_layout().clone(),
                    )
                }
                NodeKind::Unpivot { .. } => {
                    let child =
                        *local_ids
                            .get(&node.inputs[0])
                            .ok_or(FragmentCompileError::Invalid(
                                "missing lowered Unpivot child",
                            ))?;
                    let sources = channels_plan.unpivot_sources.get(&source).ok_or(
                        FragmentCompileError::Invalid("missing planned Unpivot input sources"),
                    )?;
                    work.flush()?;
                    lower_unpivot(
                        package,
                        node,
                        id,
                        UnpivotLoweringInput {
                            node: child,
                            layout: nodes[child.index()].output_layout(),
                            sources,
                            expressions: &expressions.ids,
                        },
                        &planned.slots,
                        work.control(),
                    )?
                }
                NodeKind::ChangeEventExpand { .. } => {
                    let child =
                        *local_ids
                            .get(&node.inputs[0])
                            .ok_or(FragmentCompileError::Invalid(
                                "missing lowered change-event child",
                            ))?;
                    work.flush()?;
                    lower_change_events(
                        package,
                        node,
                        id,
                        child,
                        &planned.slots,
                        &expressions.ids,
                        work.control(),
                    )?
                }
                NodeKind::AssertOneRow(_) => {
                    let child =
                        *local_ids
                            .get(&node.inputs[0])
                            .ok_or(FragmentCompileError::Invalid(
                                "missing lowered assertion child",
                            ))?;
                    let keys = channels_plan.assertion_keys.get(&source).ok_or(
                        FragmentCompileError::Invalid("missing planned assertion keys"),
                    )?;
                    work.flush()?;
                    lower_assert_rows(
                        node,
                        child,
                        nodes[child.index()].output_layout(),
                        keys,
                        work.control(),
                    )?
                }
                NodeKind::Repeat { .. } => {
                    let child =
                        *local_ids
                            .get(&node.inputs[0])
                            .ok_or(FragmentCompileError::Invalid(
                                "missing lowered Repeat child",
                            ))?;
                    work.flush()?;
                    lower_repeat(
                        package,
                        node,
                        id,
                        (child, nodes[child.index()].output_layout()),
                        &planned.slots,
                        work.control(),
                    )?
                }
                NodeKind::Project {
                    expressions: projected,
                } => {
                    let child = *local_ids
                        .get(&node.inputs[0])
                        .ok_or(FragmentCompileError::Invalid("missing lowered child"))?;
                    let mut fields = Vec::new();
                    let mut slots = Vec::new();
                    let mut exprs = Vec::new();
                    let mut is_result_output = result.is_some_and(|result| {
                        node.output.columns.len() == result.output.columns.len()
                    });
                    if let Some(result) = result.filter(|_| is_result_output) {
                        for (actual, expected) in
                            node.output.columns.iter().zip(&result.output.columns)
                        {
                            work.step()?;
                            if actual != expected {
                                is_result_output = false;
                                break;
                            }
                        }
                    }
                    for (ordinal, (expr, value)) in projected.iter().enumerate() {
                        work.step()?;
                        let definition = physical.expressions().get(*expr).ok_or(
                            FragmentCompileError::Invalid("missing projection definition"),
                        )?;
                        let value_type = &physical
                            .values()
                            .get(value)
                            .ok_or(FragmentCompileError::Invalid("missing projection value"))?
                            .ty;
                        work.flush()?;
                        if !definition
                            .ty
                            .exactly_equals_observed::<FragmentCompileError>(value_type, || {
                                work.step().map_err(Into::into)
                            })?
                        {
                            return Err(FragmentCompileError::Invalid(
                                "projection output type differs from definition",
                            ));
                        }
                        // Full result labels are authoritative only when the entire
                        // ordered output matches, including repeated occurrences.
                        let name = if is_result_output {
                            let field = result
                                .and_then(|result| result.fields.get(ordinal))
                                .ok_or(FragmentCompileError::Invalid("missing result field"))?;
                            field.alias.as_deref().unwrap_or(&field.name).to_string()
                        } else {
                            format!("local_{}_{}", id.index(), ordinal)
                        };
                        fields.push(value_type.try_to_field(name).map_err(|error| {
                            FragmentCompileError::Owner {
                                phase: "projection field",
                                error: Box::new(error),
                            }
                        })?);
                        work.flush()?;
                        slots.push(*planned.slots.get(ordinal).ok_or(
                            FragmentCompileError::Invalid("missing planned output occurrence"),
                        )?);
                        exprs.push(*expressions.ids.get(expr).ok_or(
                            FragmentCompileError::Invalid("missing projection expression"),
                        )?);
                    }
                    work.flush()?;
                    let layout = StaticLayout::try_new_for_compile(
                        Arc::new(Schema::new(fields)),
                        Arc::from(slots),
                        work.control(),
                    )?;
                    (
                        ProgramNodeKind::Project {
                            input: child,
                            is_subordinate: false,
                            exprs,
                            expr_slot_ids: layout.slots().to_vec(),
                            expr_slot_schemas: None,
                            output_indices: None,
                        },
                        layout,
                    )
                }
                _ => {
                    return Err(FragmentCompileError::Invalid(
                        "validated node family changed",
                    ));
                }
            };
        for (actual, expected) in layout.slots().iter().zip(planned.slots.iter()) {
            work.step()?;
            if actual != expected {
                return Err(FragmentCompileError::Invalid(
                    "materialized channel order differs from plan",
                ));
            }
        }
        if layout.slots().len() != planned.slots.len()
            || layout.slots().len() != node.output.columns.len()
        {
            return Err(FragmentCompileError::Invalid(
                "output occurrence width changed",
            ));
        }
        // A one-input family that reuses the child's provider schema object
        // still publishes the provider's field names, not SQL labels.
        if let [child] = node.inputs.as_ref() {
            let inherited = local_ids.get(child).is_some_and(|child| {
                scan_layouts.contains(child)
                    && Arc::ptr_eq(
                        layout.schema(),
                        nodes[child.index()].output_layout().schema(),
                    )
            });
            work.step()?;
            if inherited {
                scan_layouts.insert(id);
            }
        }
        for (ordinal, value) in node.output.columns.iter().enumerate() {
            work.step()?;
            let ty: FunctionValueType = physical
                .values()
                .get(value)
                .ok_or(FragmentCompileError::Invalid("missing channel value"))?
                .ty
                .clone();
            channels.push((
                ProgramChannelSite::Layout {
                    node: id,
                    role: ProgramChannelLayoutRole::NodeOutput,
                    ordinal: u32::try_from(ordinal)
                        .map_err(|_| FragmentCompileError::Invalid("channel ordinal exhausted"))?,
                },
                ty,
            ));
        }
        let operator = LocalOperatorId::new(
            u32::try_from(id.index())
                .map_err(|_| FragmentCompileError::Invalid("operator identity exhausted"))?,
        );
        operators.push(LocalOperatorProvenance {
            id: operator,
            lowered_nodes: Box::from([id]),
            sources: Box::from([source_id]),
            origin: LocalOperatorOrigin::Direct,
            cost_owner: operator,
            metrics: OperatorMetricAggregation {
                cpu_time: MetricAggregation::Sum,
                wall_time: MetricAggregation::Maximum,
                peak_retained_bytes: MetricAggregation::Maximum,
            },
        });
        nodes.push(ProgramNode::new_local(id, vec![source_id], kind, layout));
    }
    // Each inbound cut is consumed by exactly one lowered receiver, and each
    // provider recipe by exactly one lowered scan.
    if exchange_inputs.len() != package.cuts().inbound.len() {
        return Err(FragmentCompileError::Invalid(
            "inbound exchange cut has no lowered receiver",
        ));
    }
    if !reads.is_empty() {
        return Err(FragmentCompileError::Invalid(
            "provider read recipe has no lowered scan",
        ));
    }
    let root = local_ids[&physical.root()];
    let root_layout = nodes[root.index()].output_layout();
    work.flush()?;
    let profile = CompileProfile::new(
        options.pipeline_dop,
        options.root_sink_dop,
        root_layout.identity_for_compile(work.control())?,
        options.kernel_abi,
    );
    let mut requirement_entries = source_requirements;
    let (sink, stream) = match stream_cut {
        Some(cut) => {
            work.flush()?;
            let lowered = lower_stream_sink(package, cut, root, root_layout, work.control())?;
            requirement_entries.push(lowered.requirement);
            (lowered.sink, Some(lowered.flow))
        }
        None => {
            // A provider layout cannot be relabeled: its fields must equal the
            // public schema. Publishing it as a result therefore requires the
            // result labels to be exactly the provider names.
            if scan_layouts.contains(&root) {
                let result = result.ok_or(FragmentCompileError::Invalid("missing result port"))?;
                let fields = root_layout.schema().fields();
                if result.fields.len() != fields.len() {
                    return Err(FragmentCompileError::Invalid(
                        "result width differs from its root layout",
                    ));
                }
                for (label, field) in result.fields.iter().zip(fields.iter()) {
                    let same = label.alias.as_deref().unwrap_or(&label.name) == field.name();
                    work.step()?;
                    if !same {
                        return Err(FragmentCompileError::Unsupported {
                            node: Some(physical.root()),
                            feature: "result labels differ from the provider scan layout",
                        });
                    }
                }
            }
            requirement_entries.push(BindingRequirement::ResultSink {
                layout: root_layout.clone(),
            });
            (StaticSinkProgram::Result, None)
        }
    };
    work.flush()?;
    let requirements =
        BindingRequirements::try_new_for_compile(requirement_entries, work.control())?;
    work.flush()?;
    let graph = LocalProgramGraph::try_new_with_sink_for_compile(
        nodes,
        root,
        expressions.arena.clone(),
        profile,
        requirements,
        Some(sink),
        work.control(),
    )?;
    // StaticValues has no runtime expression roots. Retire only the actual
    // constant-cell source occurrences whose materialization succeeded above.
    let retired = retired_values_uses(package, &expressions, work)?;
    let mut domains = Vec::new();
    for domain in package.expression_uses().flow().domains().values() {
        work.step()?;
        domains.push(*domain);
    }
    let mut uses = Vec::new();
    for invocation in package.expression_uses().flow().uses().values() {
        work.step()?;
        if retired.contains(&invocation.context.use_id) {
            continue;
        }
        let definition =
            *expressions
                .ids
                .get(&invocation.definition)
                .ok_or(FragmentCompileError::Invalid(
                    "missing invocation definition",
                ))?;
        let mut arguments = Vec::new();
        for argument in &invocation.arguments {
            work.step()?;
            arguments.push(*argument);
        }
        uses.push(ProgramExpressionUse {
            context: invocation.context,
            definition,
            control: invocation.control,
            arguments: arguments.into_boxed_slice(),
        });
    }
    let mut roots = Vec::new();
    let mut union_slot_bindings = Vec::new();
    work.flush()?;
    crate::union_flow::append_union_roots(
        &union_roots,
        &mut domains,
        &mut uses,
        &mut roots,
        &mut union_slot_bindings,
        work.control(),
    )?;
    for (site, use_id) in package.expression_uses().bindings() {
        work.step()?;
        if retired.contains(use_id) {
            continue;
        }
        let node = *local_ids
            .get(&site.node)
            .ok_or(FragmentCompileError::Invalid("missing root node"))?;
        let role = match site.role {
            ExpressionRootRole::ChangePredicate { event } => {
                ProgramNodeExpressionRole::ChangePredicate { event }
            }
            ExpressionRootRole::ChangeAssignment { event, assignment } => {
                ProgramNodeExpressionRole::ChangeAssignment { event, assignment }
            }
            ExpressionRootRole::UnpivotConstant { mapping, constant } => {
                ProgramNodeExpressionRole::UnpivotConstant { mapping, constant }
            }
            ExpressionRootRole::FilterPredicate { predicate: 0 } => {
                ProgramNodeExpressionRole::FilterPredicate
            }
            ExpressionRootRole::ScanResidual { predicate: 0 } => {
                ProgramNodeExpressionRole::ScanResidual
            }
            ExpressionRootRole::SortOrder { key } => ProgramNodeExpressionRole::SortOrder { key },
            ExpressionRootRole::TopNOrder { key } => ProgramNodeExpressionRole::SortOrder { key },
            ExpressionRootRole::ProjectOutput { expression } => {
                ProgramNodeExpressionRole::ProjectOutput { expression }
            }
            _ => {
                return Err(FragmentCompileError::Unsupported {
                    node: Some(site.node),
                    feature: "expression root role",
                });
            }
        };
        roots.push(ProgramRootUseBinding {
            site: ProgramExpressionRootSite::Node { node, role },
            use_id: *use_id,
        });
    }
    work.flush()?;
    let flow = ProgramControlFlow::try_new(
        domains,
        uses,
        expressions.arena.nodes().len(),
        work.control(),
    )?;
    let mut flows = BTreeMap::from([(ProgramExpressionArena::Main, flow)]);
    let mut types = BTreeMap::from([(ProgramExpressionArena::Main, expressions.types)]);
    let mut sink_slot_bindings = Vec::new();
    // A stream sink always owns a Sink arena, so its flow and types are
    // supplied even when the arena is empty (Gather and Broadcast).
    if let Some(stream) = stream {
        crate::assert_rows::reserve_vec(&mut roots, stream.roots.len(), work)?;
        roots.extend(stream.roots);
        flows.insert(ProgramExpressionArena::Sink, stream.flow);
        types.insert(ProgramExpressionArena::Sink, stream.types);
        sink_slot_bindings = stream.slots;
    }
    work.flush()?;
    let snapshot = ProgramRootControlBindings::try_new(graph, flows, roots, work.control())?;
    work.flush()?;
    let calls = ProgramResolvedCalls::try_new(snapshot, tokens, work.control())?;
    work.flush()?;
    let typed = ProgramTypedExpressions::try_new(calls, types, work.control())?;
    work.flush()?;
    let channels = ProgramTypedChannels::try_new(typed, channels, work.control())?;
    work.flush()?;
    let mut slot_bindings = union_slot_bindings;
    if !sink_slot_bindings.is_empty() {
        crate::assert_rows::reserve_vec(&mut slot_bindings, sink_slot_bindings.len(), work)?;
        slot_bindings.extend(sink_slot_bindings);
    }
    for invocation in package.expression_uses().flow().uses().values() {
        work.step()?;
        if let Some(input) = channels_plan.inputs.get(&invocation.definition) {
            slot_bindings.push(ProgramSlotBinding {
                occurrence: ProgramUseRef {
                    arena: ProgramExpressionArena::Main,
                    use_id: invocation.context.use_id,
                },
                source: ProgramLexicalSource::Input(input.source),
            });
        }
    }
    work.flush()?;
    let lexical = ProgramLexicalBindings::try_new(channels, vec![], slot_bindings, work.control())?;
    work.flush()?;
    LocalProgram::try_new(
        lexical,
        operators,
        &allowed,
        BTreeMap::new(),
        exchange_inputs,
        scan_inputs,
        work.control(),
    )
    .map_err(Into::into)
}
