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
    channels::{ChannelLoweringError, resolve_linear_channels},
    expressions::{ExpressionLoweringError, lower_expressions, prepare_calls},
};
use arrow_array::{RecordBatch, RecordBatchOptions};
use arrow_schema::Schema;
use novarocks_functions::{ConstantPolicy, PureEngineFunctionCatalog};
use novarocks_local_program::*;
use novarocks_physical_plan::{
    Distribution, ExpressionRootRole, FragmentSink, NodeId, NodeKind, RowMultiplicity,
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
};

/// Host-admitted values are explicit; the compiler authors the actual layout
/// digest. Neither topology nor resource defaults are inferred from the plan.
#[derive(Clone, Copy, Debug)]
pub struct LocalCompileOptions {
    pub pipeline_dop: NonZeroUsize,
    pub root_sink_dop: Option<NonZeroUsize>,
    pub kernel_abi: KernelAbiVersion,
    pub constants: ConstantPolicy,
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
    let package = input.package();
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
    if !matches!(physical.sink(), FragmentSink::Result) {
        return Err(FragmentCompileError::Unsupported {
            node: None,
            feature: "non-result sink",
        });
    }
    if !input.reads().is_empty()
        || !input.writes().is_empty()
        || !package.cuts().inbound.is_empty()
        || !package.cuts().outbound.is_empty()
        || !package.cuts().runtime_filters.is_empty()
        || !physical.runtime_filters().is_empty()
    {
        return Err(FragmentCompileError::Unsupported {
            node: None,
            feature: "provider, exchange or runtime-filter graph",
        });
    }
    let result = package
        .result()
        .ok_or(FragmentCompileError::Invalid("missing result port"))?;
    // A child-first schedule is independent of arbitrary sparse physical IDs.
    // This initial linear family has one parent per actual input occurrence;
    // shared/multi-input lowering requires its own execution ownership proof.
    let mut order = Vec::new();
    let mut visited = BTreeSet::new();
    let mut next = Some(physical.root());
    while let Some(id) = next {
        work.step()?;
        if !visited.insert(id) {
            return Err(FragmentCompileError::Invalid(
                "shared or cyclic linear input",
            ));
        }
        let node = physical
            .nodes()
            .get(&id)
            .ok_or(FragmentCompileError::Invalid("missing physical node"))?;
        if node.output_properties.distribution != Distribution::Singleton
            || node.output_properties.row_multiplicity != RowMultiplicity::SingleCopy
            || !node.output_properties.ordering.is_empty()
        {
            return Err(FragmentCompileError::Unsupported {
                node: Some(id),
                feature: "non-singleton source-chain properties",
            });
        }
        match &node.kind {
            NodeKind::Project { .. } if node.inputs.len() == 1 => {
                next = Some(node.inputs[0]);
            }
            NodeKind::Values { rows }
                if rows.len() == 1
                    && rows[0].is_empty()
                    && node.output.columns.is_empty()
                    && node.inputs.is_empty() =>
            {
                next = None
            }
            NodeKind::Filter { predicates } if predicates.len() == 1 && node.inputs.len() == 1 => {
                next = Some(node.inputs[0])
            }
            NodeKind::Limit { .. } if node.inputs.len() == 1 => next = Some(node.inputs[0]),
            _ => {
                return Err(FragmentCompileError::Unsupported {
                    node: Some(id),
                    feature: "node family or occurrence shape",
                });
            }
        }
        order.push(id);
    }
    if order.len() != physical.nodes().len() {
        return Err(FragmentCompileError::Invalid(
            "unrepresented physical nodes",
        ));
    }
    work.flush()?;
    let channels_plan = resolve_linear_channels(package, &order, work.control())?;
    work.flush()?;
    let expressions = lower_expressions(
        package,
        options.constants,
        &channels_plan.inputs,
        work.control(),
    )?;
    work.flush()?;
    let tokens = prepare_calls(package, &expressions, functions, work.control())?;
    let mut nodes: Vec<ProgramNode> = Vec::new();
    let mut local_ids = BTreeMap::new();
    let mut channels = Vec::new();
    let mut operators = Vec::new();
    let mut allowed = BTreeSet::new();
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
                    let layout = StaticLayout::try_new_for_compile(
                        Arc::new(Schema::empty()),
                        Arc::from([]),
                        work.control(),
                    )?;
                    work.flush()?;
                    let batch = RecordBatch::try_new_with_options(
                        layout.schema().clone(),
                        vec![],
                        &RecordBatchOptions::new().with_row_count(Some(1)),
                    )
                    .map_err(|error| FragmentCompileError::Owner {
                        phase: "empty-row values",
                        error: Box::new(error),
                    })?;
                    work.flush()?;
                    let values =
                        StaticValues::try_new_for_compile(batch, layout.clone(), work.control())?;
                    (ProgramNodeKind::Values { values }, layout)
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
                NodeKind::Project {
                    expressions: projected,
                } => {
                    let child = *local_ids
                        .get(&node.inputs[0])
                        .ok_or(FragmentCompileError::Invalid("missing lowered child"))?;
                    let mut fields = Vec::new();
                    let mut slots = Vec::new();
                    let mut exprs = Vec::new();
                    let mut is_result_output =
                        node.output.columns.len() == result.output.columns.len();
                    if is_result_output {
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
                                .fields
                                .get(ordinal)
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
    let root = local_ids[&physical.root()];
    let root_layout = nodes[root.index()].output_layout();
    work.flush()?;
    let profile = CompileProfile::new(
        options.pipeline_dop,
        options.root_sink_dop,
        root_layout.identity_for_compile(work.control())?,
        options.kernel_abi,
    );
    work.flush()?;
    let requirements = BindingRequirements::try_new_for_compile(
        vec![BindingRequirement::ResultSink {
            layout: root_layout.clone(),
        }],
        work.control(),
    )?;
    work.flush()?;
    let graph = LocalProgramGraph::try_new_with_sink_for_compile(
        nodes,
        root,
        expressions.arena.clone(),
        profile,
        requirements,
        Some(StaticSinkProgram::Result),
        work.control(),
    )?;
    let mut domains = Vec::new();
    for domain in package.expression_uses().flow().domains().values() {
        work.step()?;
        domains.push(*domain);
    }
    let mut uses = Vec::new();
    for invocation in package.expression_uses().flow().uses().values() {
        work.step()?;
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
    work.flush()?;
    let flow = ProgramControlFlow::try_new(
        domains,
        uses,
        expressions.arena.nodes().len(),
        work.control(),
    )?;
    let mut roots = Vec::new();
    for (site, use_id) in package.expression_uses().bindings() {
        work.step()?;
        let node = *local_ids
            .get(&site.node)
            .ok_or(FragmentCompileError::Invalid("missing root node"))?;
        let role = match site.role {
            ExpressionRootRole::FilterPredicate { predicate: 0 } => {
                ProgramNodeExpressionRole::FilterPredicate
            }
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
    let snapshot = ProgramRootControlBindings::try_new(
        graph,
        BTreeMap::from([(ProgramExpressionArena::Main, flow)]),
        roots,
        work.control(),
    )?;
    work.flush()?;
    let calls = ProgramResolvedCalls::try_new(snapshot, tokens, work.control())?;
    work.flush()?;
    let typed = ProgramTypedExpressions::try_new(
        calls,
        BTreeMap::from([(ProgramExpressionArena::Main, expressions.types)]),
        work.control(),
    )?;
    work.flush()?;
    let channels = ProgramTypedChannels::try_new(typed, channels, work.control())?;
    work.flush()?;
    let mut slot_bindings = Vec::new();
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
        work.control(),
    )
    .map_err(Into::into)
}
