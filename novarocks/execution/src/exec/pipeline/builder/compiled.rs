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

//! Pipeline construction for one compiled LocalProgram (local-compiler
//! output). Expressions are evaluated only through compiled roots; there is no
//! legacy ExprArena thaw and no legacy node identity. A node family without
//! a compiled processor is an explicit refusal, never a legacy fallback.
//!
//! Reuse boundary: families that evaluate expressions (Project, Filter, Sort
//! and every row-count TopN phase, Unpivot, ChangeEventExpand) run compiled
//! processors that own one instance per root and driver. Legacy operators are
//! reused only where they evaluate nothing: the all-constant Values source,
//! Limit, the local gather exchange, the row-count assertion and the UnionAll
//! fan-in queue. A Values with dynamic cells evaluates each cell root once
//! through the compiled Values source. Repeat is a compiled processor too,
//! because the legacy one re-derives its output schema.

use std::collections::BTreeSet;

use super::*;
use crate::exec::chunk::{Chunk, ChunkSchema};
use crate::exec::node::exchange_source::ExchangeSourceNode;
use crate::exec::operators::compiled_change_events::CompiledChangeEventProcessorFactory;
use crate::exec::operators::compiled_expression::{
    CompiledFilterProcessorFactory, CompiledProjectProcessorFactory,
};
use crate::exec::operators::compiled_repeat::CompiledRepeatProcessorFactory;
use crate::exec::operators::compiled_sort::CompiledSortProcessorFactory;
use crate::exec::operators::compiled_unpivot::CompiledUnpivotProcessorFactory;
use crate::runtime::runtime_state::RuntimeErrorState;
use novarocks_local_program::{
    AssertRowsMode, LocalProgram, ProgramExpressionRootSite, ProgramNodeExpressionRole,
    ProgramNodeId, ProgramNodeKind, RowAssertion,
};

/// Operator display identity for a compiled node: its local program index.
/// Profiles keep the program's provenance as the source relation.
fn display_id(id: ProgramNodeId) -> Result<i32, String> {
    i32::try_from(id.index()).map_err(|_| "compiled program node index exceeds i32".to_string())
}

/// The receiver node id a compiled ExchangeSource is addressed by: the
/// physical destination node of its edge, which is also the sender's
/// `dest_node_id` routing key.
fn receiver_node_id(program: &LocalProgram, id: ProgramNodeId) -> Result<i32, String> {
    let input = program.exchange_inputs().get(&id).ok_or_else(|| {
        format!(
            "compiled exchange source at local node {} has no exchange address",
            id.index()
        )
    })?;
    i32::try_from(input.receiver_node).map_err(|_| {
        format!(
            "compiled exchange receiver node {} exceeds i32",
            input.receiver_node
        )
    })
}

/// The Task's exchange bindings must cover exactly the program's compiled
/// exchange sources: no source without its receiver, no binding without a
/// source, and every binding keyed by the receiver it was registered for.
fn validate_compiled_exchange_bindings(
    program: &LocalProgram,
    bindings: &ExchangeBindings,
) -> Result<(), String> {
    let sources = program
        .graph()
        .nodes()
        .iter()
        .enumerate()
        .filter(|(_, node)| matches!(node.kind(), ProgramNodeKind::ExchangeSource { .. }))
        .map(|(index, _)| ProgramNodeId::new(index))
        .collect::<BTreeSet<_>>();
    if sources != program.exchange_inputs().keys().copied().collect() {
        return Err(
            "compiled exchange addresses do not match the program's exchange sources".to_string(),
        );
    }
    let mut receivers = BTreeSet::new();
    for id in &sources {
        let receiver = receiver_node_id(program, *id)?;
        if !receivers.insert(receiver) {
            return Err(format!(
                "compiled exchange receiver node {receiver} is addressed by more than one source"
            ));
        }
        let binding = bindings.get(receiver).ok_or_else(|| {
            format!("missing exchange binding for compiled receiver node {receiver}")
        })?;
        if binding.key.node_id != receiver {
            return Err(format!(
                "exchange binding for compiled receiver node {receiver} is keyed to node {}",
                binding.key.node_id
            ));
        }
    }
    if let Some(extra) = bindings.node_ids().find(|id| !receivers.contains(id)) {
        return Err(format!(
            "exchange binding for node {extra} has no compiled exchange source"
        ));
    }
    Ok(())
}

pub(crate) fn build_compiled_pipeline_graph(
    program: &Arc<LocalProgram>,
    exchange_bindings: ExchangeBindings,
    dep_manager: DependencyManager,
    pipeline_dop: i32,
    root_sink_dop: Option<i32>,
    function_set: Arc<SealedExecutionFunctionSet>,
    error: Arc<RuntimeErrorState>,
) -> Result<PipelineGraph, String> {
    let graph = program.graph();
    if graph
        .nodes()
        .iter()
        .any(|node| node.legacy_native_node_id().is_some())
    {
        return Err("legacy-lowered nodes cannot enter the compiled pipeline".to_string());
    }
    validate_compiled_exchange_bindings(program, &exchange_bindings)?;
    let mut ctx = PipelineBuildContext {
        arena: Arc::new(ExprArena::default()),
        function_set,
        dep_manager,
        runtime_filter_execution: PipelineRuntimeFilterExecution { session: None },
        exchange_bindings,
        scan_bindings: ScanBindings::default(),
        next_pipeline_id: 0,
        pipeline_dop: pipeline_dop.max(1),
        operator_buffer_chunks: 1,
        local_exchange_buffer_mem_limit_per_driver: 1,
        local_exchange_max_buffered_rows: 0,
        precomputed_keyed_assert_keys: std::collections::HashMap::new(),
    };
    let mut build = build_node(program, graph.root(), &mut ctx, &error)?;
    match root_sink_dop {
        None => {}
        // The frozen profile places the root sink on one driver.
        Some(1) => build = gather_to_one(build, &mut ctx, ROOT_SINK_LOCAL_EXCHANGE_NODE_ID),
        Some(other) => {
            return Err(format!(
                "compiled root sink width {other} is not executable yet"
            ));
        }
    }
    build.pipeline.needs_sink = true;
    let root_id = build.pipeline.id;
    let mut pipelines = vec![build.pipeline];
    pipelines.append(&mut build.extra_pipelines);
    Ok(PipelineGraph { pipelines, root_id })
}

fn build_node(
    program: &Arc<LocalProgram>,
    id: ProgramNodeId,
    ctx: &mut PipelineBuildContext,
    error: &Arc<RuntimeErrorState>,
) -> Result<PipelineBuildResult, String> {
    let node = program
        .graph()
        .nodes()
        .get(id.index())
        .ok_or_else(|| format!("missing compiled program node {}", id.index()))?;
    let node_id = display_id(id)?;
    match node.kind() {
        ProgramNodeKind::Values { values } => {
            let source: Box<dyn OperatorFactory> = match values.batch() {
                Some(batch) => {
                    let chunk_schema = ChunkSchema::from_compiled_layout(values.layout())?;
                    let chunk = Chunk::new_with_chunk_schema(batch.clone(), chunk_schema);
                    Box::new(ValuesSourceFactory::new(chunk, node_id))
                }
                // Dynamic cells are evaluated once, at the source's opening
                // turn, by the one compiled evaluator.
                None => Box::new(values_source::CompiledValuesSourceFactory::try_new(
                    Arc::clone(program),
                    id,
                    Arc::clone(error),
                )?),
            };
            let pipeline = new_source_pipeline_with_dop(ctx, source, 1);
            Ok(PipelineBuildResult {
                pipeline,
                extra_pipelines: Vec::new(),
                stream: StreamDesc::any(1),
            })
        }
        ProgramNodeKind::Project { input, .. } => {
            let mut build = build_node(program, *input, ctx, error)?;
            build
                .pipeline
                .factories
                .push(Box::new(CompiledProjectProcessorFactory::try_new(
                    Arc::clone(program),
                    id,
                    Arc::clone(error),
                )?));
            Ok(build)
        }
        ProgramNodeKind::Filter { input, .. } => {
            let mut build = build_node(program, *input, ctx, error)?;
            build
                .pipeline
                .factories
                .push(Box::new(CompiledFilterProcessorFactory::try_new(
                    Arc::clone(program),
                    ProgramExpressionRootSite::Node {
                        node: id,
                        role: ProgramNodeExpressionRole::FilterPredicate,
                    },
                    Arc::clone(error),
                )?));
            Ok(build)
        }
        ProgramNodeKind::ExchangeSource {
            timeout,
            runtime_filters,
            hash_partition_exprs,
        } => {
            if !runtime_filters.is_empty() {
                return Err(format!(
                    "compiled exchange source at local node {} with runtime-filter consumers is not executable yet",
                    id.index()
                ));
            }
            if !hash_partition_exprs.is_empty() {
                return Err(format!(
                    "compiled exchange source at local node {} with hash-key expressions is not executable yet",
                    id.index()
                ));
            }
            let receiver = receiver_node_id(program, id)?;
            let binding = ctx.exchange_bindings.get(receiver).ok_or_else(|| {
                format!("missing exchange binding for compiled receiver node {receiver}")
            })?;
            let exchange = ExchangeSourceNode::new(
                node_id,
                *timeout,
                ChunkSchema::from_compiled_layout(node.output_layout())?,
            );
            let source: Box<dyn OperatorFactory> =
                Box::new(ExchangeSourceFactory::new_compiled(exchange, binding)?);
            // Every driver pulls from the one instance-wide receiver.
            let pipeline = new_source_pipeline(ctx, source);
            Ok(PipelineBuildResult {
                pipeline,
                extra_pipelines: Vec::new(),
                stream: StreamDesc::any(ctx.pipeline_dop),
            })
        }
        ProgramNodeKind::Limit {
            input,
            limit,
            offset,
        } => {
            let build = build_node(program, *input, ctx, error)?;
            let mut build = gather_to_one(build, ctx, node_id);
            build
                .pipeline
                .factories
                .push(Box::new(LimitProcessorFactory::new(
                    node_id, *limit, *offset,
                )));
            build.stream = StreamDesc::single();
            Ok(build)
        }
        ProgramNodeKind::Sort { input, .. } => {
            // Global Sort and every row-count TopN phase order the whole
            // instance input on one driver. Single and Final read a Singleton
            // input, so the instance input is the relation. A Partial keeps
            // its input distribution and declares its order keys as its
            // output ordering, which the TopN sequence trace matches against
            // the Final; one gathered driver is what makes the instance's
            // output one stream in that order. A per-driver partial would
            // still merge correctly at the Final, but it would emit DOP
            // interleaved runs the declared ordering does not describe, and
            // re-pruning them locally would evaluate a key twice.
            let factory =
                CompiledSortProcessorFactory::try_new(Arc::clone(program), id, Arc::clone(error))?;
            let build = build_node(program, *input, ctx, error)?;
            let mut build = gather_to_one(build, ctx, node_id);
            build.pipeline.factories.push(Box::new(factory));
            build.stream = StreamDesc::single();
            Ok(build)
        }
        ProgramNodeKind::UnionAll { inputs } => {
            build_union_all(program, id, node_id, inputs, ctx, error)
        }
        ProgramNodeKind::AssertNumRows { input, mode } => {
            let factory = AssertNumRowsProcessorFactory::new(node_id, assertion_mode(mode))?;
            // Both modes judge the whole instance input: a global count, or
            // at most one row per key, so the assertion runs on one driver.
            // The keyed mode keeps the existing owner's key identity: a NULL
            // key equals a NULL key, and values compare by type and display.
            let build = build_node(program, *input, ctx, error)?;
            let mut build = gather_to_one(build, ctx, node_id);
            build.pipeline.factories.push(Box::new(factory));
            build.stream = StreamDesc::single();
            Ok(build)
        }
        ProgramNodeKind::Repeat { input, .. } => {
            let factory = CompiledRepeatProcessorFactory::try_new(program, id)?;
            let mut build = build_node(program, *input, ctx, error)?;
            build.pipeline.factories.push(Box::new(factory));
            build.stream = StreamDesc::any(build.pipeline.dop);
            Ok(build)
        }
        ProgramNodeKind::Unpivot { input, .. } => {
            let factory = CompiledUnpivotProcessorFactory::try_new(
                Arc::clone(program),
                id,
                Arc::clone(error),
            )?;
            let mut build = build_node(program, *input, ctx, error)?;
            build.pipeline.factories.push(Box::new(factory));
            build.stream = StreamDesc::any(build.pipeline.dop);
            Ok(build)
        }
        ProgramNodeKind::ChangeEventExpand { input, .. } => {
            let factory = CompiledChangeEventProcessorFactory::try_new(
                Arc::clone(program),
                id,
                Arc::clone(error),
            )?;
            let mut build = build_node(program, *input, ctx, error)?;
            build.pipeline.factories.push(Box::new(factory));
            build.stream = StreamDesc::any(build.pipeline.dop);
            Ok(build)
        }
        _ => Err(format!(
            "compiled node family at local node {} has no compiled processor yet",
            id.index()
        )),
    }
}

/// UnionAll as the compiler lowers it: every input is that branch's
/// subordinate normalizing Project, whose layout is exactly the union's
/// output layout, so each branch owns the union's channels in their frozen
/// order and its chunks pass through unchanged. The branches fan in through
/// the shared UnionAll queue, which carries no expression and no ordering.
fn build_union_all(
    program: &Arc<LocalProgram>,
    id: ProgramNodeId,
    node_id: i32,
    inputs: &[ProgramNodeId],
    ctx: &mut PipelineBuildContext,
    error: &Arc<RuntimeErrorState>,
) -> Result<PipelineBuildResult, String> {
    validate_union_branches(program, id, inputs)?;
    let mut builds = Vec::with_capacity(inputs.len());
    let mut producers = 0usize;
    for input in inputs {
        let child = build_node(program, *input, ctx, error)?;
        producers =
            producers.saturating_add(usize::try_from(child.pipeline.dop.max(1)).unwrap_or(1));
        builds.push(child);
    }
    let state = UnionAllSharedState::new(producers, node_id);
    let mut extra_pipelines = Vec::new();
    for mut child in builds {
        child
            .pipeline
            .factories
            .push(Box::new(UnionAllSinkFactory::new(state.clone(), node_id)));
        child.pipeline.needs_sink = false;
        extra_pipelines.push(child.pipeline);
        extra_pipelines.append(&mut child.extra_pipelines);
    }
    // The shared queue has one consumer; one driver drains it.
    let source: Box<dyn OperatorFactory> = Box::new(UnionAllSourceFactory::new(state, node_id));
    let pipeline = new_source_pipeline_with_dop(ctx, source, 1);
    Ok(PipelineBuildResult {
        pipeline,
        extra_pipelines,
        stream: StreamDesc::single(),
    })
}

/// Every UnionAll input must be a subordinate normalizing Project whose layout
/// is exactly the union's: same complete fields, metadata and slot order.
fn validate_union_branches(
    program: &LocalProgram,
    id: ProgramNodeId,
    inputs: &[ProgramNodeId],
) -> Result<(), String> {
    let nodes = program.graph().nodes();
    let layout = nodes
        .get(id.index())
        .ok_or_else(|| format!("missing compiled UnionAll node {}", id.index()))?
        .output_layout();
    if inputs.len() < 2 {
        return Err(format!(
            "compiled UnionAll at local node {} has fewer than two branches",
            id.index()
        ));
    }
    for (ordinal, input) in inputs.iter().enumerate() {
        let branch = nodes
            .get(input.index())
            .ok_or_else(|| format!("missing compiled UnionAll branch {}", input.index()))?;
        let normalizer = matches!(
            branch.kind(),
            ProgramNodeKind::Project {
                is_subordinate: true,
                ..
            }
        );
        let same_layout = branch.output_layout().schema() == layout.schema()
            && branch.output_layout().slots() == layout.slots();
        if !normalizer || !same_layout {
            return Err(format!(
                "compiled UnionAll at local node {} branch {ordinal} is not a normalizer owning the union layout",
                id.index()
            ));
        }
    }
    Ok(())
}

/// The frozen assertion mode in the existing row-count owner's vocabulary.
fn assertion_mode(mode: &AssertRowsMode) -> AssertNumRowsMode {
    use crate::exec::node::assert::Assertion;
    match mode {
        AssertRowsMode::Global {
            desired_num_rows,
            assertion,
            subquery_string,
        } => AssertNumRowsMode::Global {
            desired_num_rows: *desired_num_rows,
            assertion: match assertion {
                RowAssertion::Eq => Assertion::Eq,
                RowAssertion::Ne => Assertion::Ne,
                RowAssertion::Lt => Assertion::Lt,
                RowAssertion::Le => Assertion::Le,
                RowAssertion::Gt => Assertion::Gt,
                RowAssertion::Ge => Assertion::Ge,
            },
            subquery_string: subquery_string.as_ref().map(ToString::to_string),
        },
        AssertRowsMode::PerKeyAtMostOne {
            key_slots,
            key_labels,
            message_prefix,
        } => AssertNumRowsMode::PerKeyAtMostOne {
            key_slots: key_slots.clone(),
            key_labels: key_labels.iter().map(ToString::to_string).collect(),
            message_prefix: message_prefix.to_string(),
        },
    }
}

#[path = "compiled_values.rs"]
mod values_source;

#[cfg(test)]
#[path = "compiled_tests.rs"]
mod tests;

#[cfg(test)]
#[path = "compiled_exchange_tests.rs"]
mod exchange_tests;

#[cfg(test)]
#[path = "compiled_family_fixture.rs"]
mod family_fixture;

#[cfg(test)]
#[path = "compiled_sort_tests.rs"]
mod sort_tests;

#[cfg(test)]
#[path = "compiled_union_tests.rs"]
mod union_tests;

#[cfg(test)]
#[path = "compiled_assert_tests.rs"]
mod assert_tests;

#[cfg(test)]
#[path = "compiled_expand_tests.rs"]
mod expand_tests;

#[cfg(test)]
#[path = "compiled_topn_split_tests.rs"]
mod topn_split_tests;

#[cfg(test)]
#[path = "compiled_values_tests.rs"]
mod values_tests;
