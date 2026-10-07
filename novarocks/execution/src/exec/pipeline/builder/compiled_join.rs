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

//! Compiled join pipelines.
//!
//! Both join families share one build per instance: the build input is
//! gathered to one driver, which publishes one artifact, and the probe runs at
//! its own pipeline width against it. Every physical distribution has already
//! delivered this instance its whole share of both sides, so this local
//! strategy is correct for each of them. The probe is the program's left input
//! and the build its right input; the compiler normalized that orientation.
//! The build pipeline is an extra pipeline that ends in its build sink.

use super::*;
use crate::exec::operators::compiled_nljoin::{
    CompiledNlJoinPlan, CompiledNlJoinProbeProcessorFactory,
};
use crate::exec::operators::hashjoin::compiled_hash_join::{
    CompiledHashJoinBuildSinkFactory, CompiledHashJoinPlan, CompiledHashJoinProbeProcessorFactory,
};

pub(super) fn build_hash_join(
    program: &Arc<LocalProgram>,
    id: ProgramNodeId,
    node_id: i32,
    probe: ProgramNodeId,
    build: ProgramNodeId,
    ctx: &mut PipelineBuildContext,
    error: &Arc<RuntimeErrorState>,
) -> Result<PipelineBuildResult, String> {
    // A refused node builds no driver.
    let plan = Arc::new(CompiledHashJoinPlan::try_new(program, id)?);
    let probe_build = build_node(program, probe, ctx, error)?;
    let build_build = build_node(program, build, ctx, error)?;
    let mut build_build = gather_to_one(build_build, ctx, node_id);
    let probe_dop = probe_build.pipeline.dop.max(1) as usize;
    let state = Arc::new(BroadcastJoinSharedState::new(
        node_id,
        ctx.dep_manager.clone(),
        probe_dop,
    ));
    let mut probe_build = probe_build;
    probe_build
        .pipeline
        .factories
        .push(Box::new(CompiledHashJoinProbeProcessorFactory::new(
            Arc::clone(program),
            Arc::clone(&plan),
            Arc::clone(&state),
            Arc::clone(error),
        )));
    build_build
        .pipeline
        .factories
        .push(Box::new(CompiledHashJoinBuildSinkFactory::new(
            Arc::clone(program),
            plan,
            state,
            Arc::clone(error),
        )));
    Ok(finish(probe_build, build_build))
}

pub(super) fn build_nested_loop_join(
    program: &Arc<LocalProgram>,
    id: ProgramNodeId,
    node_id: i32,
    probe: ProgramNodeId,
    build: ProgramNodeId,
    ctx: &mut PipelineBuildContext,
    error: &Arc<RuntimeErrorState>,
) -> Result<PipelineBuildResult, String> {
    let plan = Arc::new(CompiledNlJoinPlan::try_new(program, id)?);
    let mut probe_build = build_node(program, probe, ctx, error)?;
    let build_build = build_node(program, build, ctx, error)?;
    let mut build_build = gather_to_one(build_build, ctx, node_id);
    let state = Arc::new(NlJoinSharedState::new(
        node_id,
        probe_build.pipeline.dop.max(1) as usize,
        ctx.dep_manager.clone(),
    ));
    probe_build
        .pipeline
        .factories
        .push(Box::new(CompiledNlJoinProbeProcessorFactory::new(
            Arc::clone(program),
            plan,
            Arc::clone(&state),
            Arc::clone(error),
        )));
    // The nested-loop build sink evaluates nothing; it only retains rows.
    build_build
        .pipeline
        .factories
        .push(Box::new(NlJoinBuildSinkFactory::new(state)));
    Ok(finish(probe_build, build_build))
}

/// The probe pipeline continues; the build pipeline ends in its sink.
fn finish(
    mut probe_build: PipelineBuildResult,
    mut build_build: PipelineBuildResult,
) -> PipelineBuildResult {
    build_build.pipeline.needs_sink = false;
    let mut extra_pipelines = Vec::new();
    extra_pipelines.append(&mut probe_build.extra_pipelines);
    extra_pipelines.append(&mut build_build.extra_pipelines);
    extra_pipelines.push(build_build.pipeline);
    let dop = probe_build.pipeline.dop;
    PipelineBuildResult {
        pipeline: probe_build.pipeline,
        extra_pipelines,
        stream: StreamDesc::any(dop),
    }
}
