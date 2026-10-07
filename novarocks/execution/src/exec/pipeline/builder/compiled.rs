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

use std::collections::BTreeSet;

use super::*;
use crate::exec::chunk::{Chunk, ChunkSchema};
use crate::exec::node::exchange_source::ExchangeSourceNode;
use crate::exec::operators::compiled_expression::{
    CompiledFilterProcessorFactory, CompiledProjectProcessorFactory,
};
use crate::runtime::runtime_state::RuntimeErrorState;
use novarocks_local_program::{LocalProgram, ProgramNodeId, ProgramNodeKind};

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
            let chunk_schema = ChunkSchema::from_compiled_layout(values.layout())?;
            let chunk = Chunk::new_with_chunk_schema(values.batch().clone(), chunk_schema);
            let source: Box<dyn OperatorFactory> =
                Box::new(ValuesSourceFactory::new(chunk, node_id));
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
                    id,
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
        _ => Err(format!(
            "compiled node family at local node {} has no compiled processor yet",
            id.index()
        )),
    }
}

#[cfg(test)]
#[path = "compiled_tests.rs"]
mod tests;

#[cfg(test)]
#[path = "compiled_exchange_tests.rs"]
mod exchange_tests;
