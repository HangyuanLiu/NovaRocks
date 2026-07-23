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

use std::sync::Arc;
use std::time::Duration;

use crate::common::config::debug_exec_node_output;
use crate::exec::fragment::program::FragmentSinkKind;
use crate::exec::pipeline::executor::execute_compat_plan_with_pipeline_with_root_sink_dop;
use crate::runtime::fragment::error::{FragmentExecutionError, FragmentExecutionErrorKind};
use crate::runtime::fragment::runtime_state::{
    RuntimeStateInputs, apply_query_option_overrides, build_runtime_state,
};
use crate::runtime::fragment::sink::materialize_fragment_sink_components_with_result;
use crate::runtime::fragment::submission::FragmentSubmission;
use crate::runtime::fragment_output::FragmentOutput;
use crate::runtime::mem_tracker::MemTracker;
use crate::runtime::profile::Profiler;
use crate::service::result_batch_wire::{ResultProjection, ResultSinkConfig};

#[derive(Clone, Debug, Default)]
pub(crate) struct StarRocksExecutionMetadata {
    pub(crate) result_override: Option<(ResultSinkConfig, Option<Vec<ResultProjection>>)>,
    pub(crate) root_sink_dop: Option<i32>,
    pub(crate) group_execution_scan_dop: Option<i32>,
}

pub(crate) struct StarRocksExecutionContext {
    pub(crate) profiler: Option<Profiler>,
    pub(crate) mem_tracker: Option<Arc<MemTracker>>,
}

pub(crate) fn execute_starrocks_submission(
    submission: FragmentSubmission,
    metadata: StarRocksExecutionMetadata,
    context: StarRocksExecutionContext,
) -> Result<FragmentOutput, FragmentExecutionError> {
    let instance = submission.instance();
    let program = submission.program();
    let query_id = instance.query_id();
    let fragment_instance_id = instance.fragment_instance_id().get();
    let backend_num = instance.backend_num().get();
    let logical_pipeline_dop = i32::try_from(instance.pipeline_dop().get()).map_err(|_| {
        FragmentExecutionError::new(
            FragmentExecutionErrorKind::Pipeline,
            format!(
                "pipeline DOP {} exceeds runtime representation",
                instance.pipeline_dop()
            ),
        )
    })?;
    let pipeline_dop = crate::runtime::exec_env::calc_pipeline_dop(logical_pipeline_dop);
    let runtime_state = build_runtime_state(
        RuntimeStateInputs {
            query_options: apply_query_option_overrides(Some(
                instance.runtime_options().query_options().clone(),
            )),
            query_id: Some(query_id),
            runtime_filter_params: Some(instance.runtime_filter_params().clone()),
            fragment_instance_id: Some(fragment_instance_id),
            backend_num: Some(backend_num),
            mem_tracker: context.mem_tracker,
            native_runtime_filter_context: None,
        },
        context.profiler.as_ref(),
    )
    .map_err(|error| FragmentExecutionError::new(FragmentExecutionErrorKind::Pipeline, error))?;
    let sink = materialize_fragment_sink_components_with_result(
        program.sink(),
        instance.sink_assignment(),
        fragment_instance_id,
        instance.runtime_options().typed_result_sink(),
        program.root_plan_node_id().get(),
        metadata.result_override,
    )
    .map_err(|error| {
        FragmentExecutionError::new(FragmentExecutionErrorKind::Sink, error.to_string())
    })?;
    if let Some(marker) = materialized_sink_log_marker(program.sink().kind()) {
        // Test-evidence markers go to stderr so the sql-test runner's durable
        // process-log capture can observe them (matching compat_scan /
        // compat_ingress); the rotating tracing files are not visible to it.
        eprintln!("{marker}");
    }
    let _group_execution_scan_dop = metadata.group_execution_scan_dop;
    let exec_plan = program.plan().clone();
    let _timer = context
        .profiler
        .as_ref()
        .map(|profiler| profiler.scoped_timer("PipelineExecuteTime"));
    execute_compat_plan_with_pipeline_with_root_sink_dop(
        exec_plan,
        debug_exec_node_output(),
        Duration::from_millis(50),
        sink,
        Some((fragment_instance_id.hi, fragment_instance_id.lo)),
        context.profiler,
        pipeline_dop,
        runtime_state,
        Some(query_id),
        instance.runtime_options().report_endpoint().cloned(),
        Some(backend_num),
        metadata.root_sink_dop,
    )
    .map_err(|error| FragmentExecutionError::new(FragmentExecutionErrorKind::Pipeline, error))?;
    Ok(FragmentOutput { profile_json: None })
}

pub(crate) fn uses_fetch_result_buffer(submission: &FragmentSubmission) -> bool {
    submission.program().sink().kind() == FragmentSinkKind::Result
}

fn materialized_sink_log_marker(kind: FragmentSinkKind) -> Option<&'static str> {
    (kind == FragmentSinkKind::SplitDataStream)
        .then_some("compat_fragment_sink sink=SPLIT_DATA_STREAM_SINK stage=materialized")
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};
    use std::num::NonZeroUsize;
    use std::sync::Arc;

    use crate::common::types::UniqueId;
    use crate::exec::chunk::Chunk;
    use crate::exec::expr::ExprArena;
    use crate::exec::fragment::program::{
        FragmentContractVersion, FragmentProgram, FragmentProgramOptions, FragmentSinkSpec,
        RuntimeFilterContract,
    };
    use crate::exec::fragment::sink::FragmentSinkProgram;
    use crate::exec::node::values::ValuesNode;
    use crate::exec::node::{ExecNode, ExecNodeKind, ExecPlan};
    use crate::runtime::fragment::instance::{
        BackendNum, ExchangeInputAssignments, FragmentInstanceId, FragmentInstanceSpec,
        FragmentRuntimeOptions, FragmentSinkAssignment, ScanAssignments,
    };
    use crate::runtime::query_context::QueryId;
    use crate::runtime::query_options::QueryOptions;
    use crate::runtime::runtime_filter_params::RuntimeFilterParams;

    use super::*;

    fn noop_values_submission() -> FragmentSubmission {
        let program = Arc::new(FragmentProgram::new(
            ExecPlan {
                arena: ExprArena::default(),
                root: ExecNode {
                    kind: ExecNodeKind::Values(ValuesNode {
                        chunk: Chunk::default(),
                        node_id: 10,
                    }),
                },
            },
            FragmentSinkSpec::try_new(FragmentSinkProgram::Noop).expect("noop sink"),
            FragmentProgramOptions::new(FragmentContractVersion::CURRENT),
            BTreeMap::new(),
            BTreeMap::new(),
            RuntimeFilterContract::new(BTreeSet::new(), BTreeSet::new()),
        ));
        let instance = FragmentInstanceSpec::new_compat(
            FragmentContractVersion::CURRENT,
            QueryId {
                hi: 86_001,
                lo: 86_002,
            },
            FragmentInstanceId::new(UniqueId {
                hi: 86_003,
                lo: 86_004,
            }),
            ScanAssignments::default(),
            ExchangeInputAssignments::default(),
            FragmentSinkAssignment::None,
            RuntimeFilterParams::default(),
            FragmentRuntimeOptions::new(QueryOptions::default(), None, false),
            NonZeroUsize::new(1).expect("non-zero DOP"),
            BackendNum::try_new(1).expect("backend number"),
        );
        FragmentSubmission::try_new(program, instance).expect("valid submission")
    }

    #[test]
    fn executes_noop_submission_without_protocol_inputs() {
        let query_id = QueryId {
            hi: 86_001,
            lo: 86_002,
        };
        let manager = crate::runtime::query_context::query_context_manager();
        manager
            .get_or_register_compat(
                query_id,
                false,
                Duration::from_secs(60),
                Duration::from_secs(60),
            )
            .expect("register runtime context");
        let output = execute_starrocks_submission(
            noop_values_submission(),
            StarRocksExecutionMetadata::default(),
            StarRocksExecutionContext {
                profiler: None,
                mem_tracker: None,
            },
        )
        .expect("noop submission executes");

        assert!(output.profile_json.is_none());
        manager.cancel_query(query_id, "test cleanup".to_string());
        manager.finish_fragment(query_id);
    }

    #[test]
    fn noop_submission_does_not_use_fetch_result_buffer() {
        assert!(!uses_fetch_result_buffer(&noop_values_submission()));
    }

    #[test]
    fn split_data_stream_materialization_has_stable_log_marker() {
        assert_eq!(
            materialized_sink_log_marker(FragmentSinkKind::SplitDataStream),
            Some("compat_fragment_sink sink=SPLIT_DATA_STREAM_SINK stage=materialized")
        );
        assert_eq!(materialized_sink_log_marker(FragmentSinkKind::Noop), None);
    }
}
