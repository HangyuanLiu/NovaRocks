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

//! Task preparation for one compiled LocalProgram (local-compiler output).
//!
//! The same handle, resources, result session, runtime state and failure
//! points as [`prepare_fragment`](super::prepare_fragment) are reused; only
//! the program source differs. Nothing here reads a legacy fragment program,
//! thaws an expression arena or takes expression semantics from the query
//! options: those come from the compiled program alone.

use std::sync::Arc;
use std::time::Duration;

use novarocks_local_program::{LocalProgram, StaticSinkProgram};

use super::*;
use crate::exec::fragment::error::{
    FragmentBindingError, FragmentBindingErrorKind, FragmentBindingTarget,
};
use crate::exec::fragment::program::FragmentSinkKind;
use crate::exec::operators::ResultBufferSinkFactory;
use crate::exec::pipeline::executor::prepare_compiled_program_pipeline_execution_with_profiler;
use crate::exec::pipeline::operator_factory::OperatorFactory;
use crate::runtime::fragment::exchange::materialize_compiled_exchange_receivers;
use crate::runtime::fragment::instance::FragmentInstanceSpec;
use crate::runtime::fragment::sink::materialize_compiled_sink;

/// One compiled fragment instance: a LocalProgram compiled for this Task and
/// the instance facts it runs with.
pub struct CompiledFragmentSubmission {
    program: Arc<LocalProgram>,
    instance: FragmentInstanceSpec,
    sink_kind: FragmentSinkKind,
}

impl std::fmt::Debug for CompiledFragmentSubmission {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("CompiledFragmentSubmission")
            .field("nodes", &self.program.graph().nodes().len())
            .field("sink_kind", &self.sink_kind)
            .field("instance", &self.instance)
            .finish()
    }
}

impl CompiledFragmentSubmission {
    pub fn try_new(
        program: Arc<LocalProgram>,
        instance: FragmentInstanceSpec,
    ) -> Result<Self, FragmentBindingError> {
        let expected_dop = program.graph().profile().pipeline_dop();
        if expected_dop != instance.pipeline_dop() {
            return Err(FragmentBindingError::new(
                FragmentBindingTarget::Instance,
                FragmentBindingErrorKind::InvalidAssignment,
                format!(
                    "compiled program compiled for pipeline DOP {}, got {}",
                    expected_dop.get(),
                    instance.pipeline_dop().get()
                ),
            ));
        }
        // Compiled scans have no runtime binder yet; a scan assignment or a
        // compiled scan source is refused rather than run without its splits.
        if !program.scan_inputs().is_empty() || !instance.scan_assignments().is_empty() {
            return Err(FragmentBindingError::new(
                FragmentBindingTarget::Program,
                FragmentBindingErrorKind::WrongAssignmentKind,
                "compiled scans are not executable yet",
            ));
        }
        let sink_kind = compiled_sink_kind(program.graph().sink())?;
        Ok(Self {
            program,
            instance,
            sink_kind,
        })
    }

    pub fn program(&self) -> &Arc<LocalProgram> {
        &self.program
    }

    pub const fn instance(&self) -> &FragmentInstanceSpec {
        &self.instance
    }

    pub const fn sink_kind(&self) -> FragmentSinkKind {
        self.sink_kind
    }
}

/// The Task-visible sink kind of a compiled program's static sink.
pub fn compiled_sink_kind(
    sink: Option<&StaticSinkProgram>,
) -> Result<FragmentSinkKind, FragmentBindingError> {
    match sink {
        Some(StaticSinkProgram::Result) => Ok(FragmentSinkKind::Result),
        Some(StaticSinkProgram::Noop) => Ok(FragmentSinkKind::Noop),
        Some(StaticSinkProgram::DataStream { .. }) => Ok(FragmentSinkKind::DataStream),
        Some(StaticSinkProgram::MultiCastDataStream { .. }) => {
            Ok(FragmentSinkKind::MultiCastDataStream)
        }
        Some(StaticSinkProgram::SplitDataStream { .. }) => Ok(FragmentSinkKind::SplitDataStream),
        None => Err(FragmentBindingError::new(
            FragmentBindingTarget::Sink,
            FragmentBindingErrorKind::MissingAssignment,
            "compiled program has no static sink",
        )),
    }
}

/// Prepare one compiled fragment instance into the same dormant handle a
/// legacy submission produces, so the Task host starts, observes and cleans
/// it up unchanged.
pub fn prepare_compiled_fragment(
    submission: CompiledFragmentSubmission,
    context: FragmentPrepareContext,
) -> Result<DormantFragmentHandle, FragmentLaunchError> {
    let program = submission.program();
    let instance = submission.instance();
    let query_id = instance.query_id();
    let finst_id = instance.fragment_instance_id().get();
    let pipeline_dop = i32::try_from(instance.pipeline_dop().get()).map_err(|_| {
        FragmentLaunchError::new(
            FragmentLaunchStage::BuildPipelines,
            FragmentLaunchErrorKind::PipelineBuild,
            format!(
                "pipeline DOP {} exceeds runtime representation",
                instance.pipeline_dop()
            ),
        )
    })?;
    // The root sink width is a compiled profile fact; a host override that
    // disagrees with it is refused, never applied.
    let frozen_root_sink_dop = program
        .graph()
        .profile()
        .root_sink_dop()
        .and_then(|dop| i32::try_from(dop.get()).ok());
    if context.root_sink_dop.is_some() && context.root_sink_dop != frozen_root_sink_dop {
        return Err(FragmentLaunchError::new(
            FragmentLaunchStage::ValidateSubmission,
            FragmentLaunchErrorKind::Binding,
            format!(
                "host root sink width {:?} differs from compiled width {frozen_root_sink_dop:?}",
                context.root_sink_dop
            ),
        ));
    }
    let mut resources = FragmentResources::new(
        Arc::clone(&context.commit_port),
        Arc::clone(&context.exchange_receiver_port),
        context.cleanup_faults(),
    );
    let prepare_result = (|| {
        resources.acquire_sink_commit(finst_id)?;
        context.fail_if_injected(PrepareFailurePoint::AfterSinkCommit)?;
        let mut result_spec = context.result_spec.clone().unwrap_or_else(|| {
            ResultWriteSpec::new(
                finst_id,
                ResultPresentation::MysqlText,
                None,
                instance.runtime_options().typed_result_sink(),
            )
        });
        if let Some(identity) = context.result_identity {
            result_spec = result_spec.with_task_identity(identity);
        }
        resources.acquire_result_for(
            submission.sink_kind(),
            &context.result_writer,
            result_spec,
        )?;
        context.fail_if_injected(PrepareFailurePoint::AfterResult)?;
        let receivers = materialize_compiled_exchange_receivers(
            program,
            finst_id,
            instance.exchange_inputs(),
            Arc::clone(&context.exchange_receiver_port),
        )
        .map_err(|detail| {
            FragmentLaunchError::new(
                FragmentLaunchStage::Register,
                FragmentLaunchErrorKind::Binding,
                detail,
            )
        })?;
        resources.acquire_compiled_exchange(receivers.registrations)?;
        context.fail_if_injected(PrepareFailurePoint::AfterExchange)?;

        let runtime_state = build_runtime_state(RuntimeStateInputs {
            query_options: apply_query_option_overrides(
                Some(instance.runtime_options().query_options().clone()),
                context.execution_runtime.as_deref(),
            ),
            query_id: Some(query_id),
            fragment_instance_id: Some(finst_id),
            backend_num: Some(instance.backend_num().get()),
            mem_tracker: context.mem_tracker.clone(),
            runtime_filter_session: context.runtime_filter.clone(),
            execution_runtime: context.execution_runtime.clone(),
        })
        .map_err(|error| {
            FragmentLaunchError::new(
                FragmentLaunchStage::BuildRuntimeState,
                FragmentLaunchErrorKind::ResourceUnavailable,
                error,
            )
        })?;
        let result_sink = match submission.sink_kind() {
            FragmentSinkKind::Result => {
                let session = resources.result_session().ok_or_else(|| {
                    FragmentLaunchError::new(
                        FragmentLaunchStage::Materialize,
                        FragmentLaunchErrorKind::Materialization,
                        "compiled RESULT_SINK requires an opened Fragment result session",
                    )
                })?;
                Some(Box::new(ResultBufferSinkFactory::new(session, None))
                    as Box<dyn OperatorFactory>)
            }
            _ => None,
        };
        let sink = materialize_compiled_sink(
            program,
            instance.sink_assignment(),
            finst_id,
            Arc::clone(&context.exchange_transmitter),
            result_sink,
            context.edge_gates.clone(),
        )?;
        prepare_compiled_program_pipeline_execution_with_profiler(
            Arc::clone(program),
            Duration::from_millis(50),
            sink,
            receivers.bindings,
            Some((finst_id.high(), finst_id.low())),
            context.profiler.clone(),
            pipeline_dop,
            runtime_state,
            Arc::clone(&context.event_sink),
        )
        .map_err(|error| {
            FragmentLaunchError::from_failure(
                FragmentLaunchStage::BuildPipelines,
                FragmentLaunchErrorKind::PipelineBuild,
                error,
            )
        })
    })();
    match prepare_result {
        Ok(prepared) => Ok(DormantFragmentHandle {
            prepared,
            resources,
            query_id,
            fragment_instance_id: finst_id,
            profiler: context.profiler.clone(),
            mem_tracker: context.mem_tracker.clone(),
            start_failure: context.start_failure(),
        }),
        Err(error) => Err(error.with_cleanup_diagnostics(resources.rollback())),
    }
}

#[cfg(test)]
#[path = "compiled_prepare_tests.rs"]
mod tests;
