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
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{Receiver as ReadinessReceiver, SyncSender as ReadinessSender};
use std::time::Duration;

use crate::common::config::debug_exec_node_output;
use crate::exec::fragment::program::{
    FragmentSinkKind, RuntimeFilterApplyPoint, RuntimeFilterDormancyFact, RuntimeFilterDormancyRole,
};
use crate::exec::pipeline::executor::execute_native_plan_with_pipeline;
use crate::runtime::fragment::error::{FragmentExecutionError, FragmentExecutionErrorKind};
use crate::runtime::fragment::runtime_state::{
    RuntimeStateInputs, apply_query_option_overrides, build_runtime_state,
};
use crate::runtime::fragment::sink::materialize_fragment_sink;
use crate::runtime::fragment::submission::FragmentSubmission;
use crate::runtime::fragment_output::FragmentOutput;
use crate::runtime::mem_tracker::MemTracker;
use crate::runtime::profile::{
    NATIVE_RUNTIME_FILTER_BINDING_COUNT, NATIVE_RUNTIME_FILTER_DEPLOYMENT_NOT_INSTALLED,
    NATIVE_RUNTIME_FILTER_DORMANCY_PROFILE, NATIVE_RUNTIME_FILTER_VALIDATED_LOOKUP,
    NATIVE_RUNTIME_FILTER_ZERO_SIDE_EFFECT_COUNTERS, Profiler,
};
use crate::runtime::result_buffer;
use crate::runtime_filter::service::NativeRuntimeFilterExecutionContext;

#[cfg(test)]
use std::collections::{HashMap, HashSet};
#[cfg(test)]
use std::sync::mpsc::{Receiver, SyncSender};
#[cfg(test)]
use std::sync::{Mutex, OnceLock};

#[cfg(test)]
struct TestResultBufferCreationGateWorker {
    entered: SyncSender<()>,
    release: Receiver<()>,
}

#[cfg(test)]
fn test_result_buffer_creation_gates()
-> &'static Mutex<HashMap<crate::common::types::UniqueId, TestResultBufferCreationGateWorker>> {
    static GATES: OnceLock<
        Mutex<HashMap<crate::common::types::UniqueId, TestResultBufferCreationGateWorker>>,
    > = OnceLock::new();
    GATES.get_or_init(|| Mutex::new(HashMap::new()))
}

#[cfg(test)]
fn test_pre_ready_panics() -> &'static Mutex<HashSet<crate::common::types::UniqueId>> {
    static PANICS: OnceLock<Mutex<HashSet<crate::common::types::UniqueId>>> = OnceLock::new();
    PANICS.get_or_init(|| Mutex::new(HashSet::new()))
}

#[cfg(test)]
pub(crate) fn install_test_pre_ready_panic(finst_id: crate::common::types::UniqueId) {
    test_pre_ready_panics()
        .lock()
        .expect("pre-ready panic set lock")
        .insert(finst_id);
}

#[cfg(test)]
fn maybe_panic_before_ready(finst_id: crate::common::types::UniqueId) {
    if test_pre_ready_panics()
        .lock()
        .expect("pre-ready panic set lock")
        .remove(&finst_id)
    {
        panic!("injected native worker panic before readiness");
    }
}

#[cfg(test)]
pub(crate) struct TestResultBufferCreationGate {
    entered: Receiver<()>,
    release: Option<SyncSender<()>>,
}

#[cfg(test)]
impl TestResultBufferCreationGate {
    pub(crate) fn wait_until_worker_enters(&self) {
        self.entered
            .recv()
            .expect("native worker must reach result-buffer creation gate");
    }

    pub(crate) fn release(mut self) {
        self.release
            .take()
            .expect("result-buffer creation gate released once")
            .send(())
            .expect("native worker must wait for result-buffer gate release");
    }
}

#[cfg(test)]
impl Drop for TestResultBufferCreationGate {
    fn drop(&mut self) {
        if let Some(release) = self.release.take() {
            let _ = release.send(());
        }
    }
}

#[cfg(test)]
pub(crate) fn install_test_result_buffer_creation_gate(
    finst_id: crate::common::types::UniqueId,
) -> TestResultBufferCreationGate {
    let (entered_tx, entered_rx) = std::sync::mpsc::sync_channel(1);
    let (release_tx, release_rx) = std::sync::mpsc::sync_channel(1);
    test_result_buffer_creation_gates()
        .lock()
        .expect("result-buffer creation gate lock")
        .insert(
            finst_id,
            TestResultBufferCreationGateWorker {
                entered: entered_tx,
                release: release_rx,
            },
        );
    TestResultBufferCreationGate {
        entered: entered_rx,
        release: Some(release_tx),
    }
}

#[cfg(test)]
fn wait_at_test_result_buffer_creation_gate(finst_id: crate::common::types::UniqueId) {
    let gate = test_result_buffer_creation_gates()
        .lock()
        .expect("result-buffer creation gate lock")
        .remove(&finst_id);
    if let Some(gate) = gate {
        gate.entered
            .send(())
            .expect("result-buffer gate observer must remain alive");
        gate.release
            .recv()
            .expect("result-buffer gate observer must release worker");
    }
}

pub(crate) struct NativeExecutionContext {
    pub(crate) profiler: Option<Profiler>,
    pub(crate) mem_tracker: Option<Arc<MemTracker>>,
    pub(crate) readiness: NativeExecutionReadiness,
    pub(crate) runtime_filter: Option<NativeRuntimeFilterExecutionContext>,
}

#[derive(Debug)]
pub(crate) enum NativeExecutionStart {
    Ready,
    Failed(FragmentExecutionError),
}

#[derive(Clone, Debug)]
pub(crate) struct NativeExecutionReadiness {
    sender: ReadinessSender<NativeExecutionStart>,
    ready: Arc<AtomicBool>,
}

impl NativeExecutionReadiness {
    pub(crate) fn signal_ready(&self) {
        self.ready.store(true, Ordering::Release);
        let _ = self.sender.send(NativeExecutionStart::Ready);
    }

    pub(crate) fn is_ready(&self) -> bool {
        self.ready.load(Ordering::Acquire)
    }

    pub(crate) fn fail_after_cleanup(&self, error: FragmentExecutionError) {
        if !self.is_ready() {
            let _ = self.sender.send(NativeExecutionStart::Failed(error));
        }
    }
}

pub(crate) fn native_execution_readiness_channel() -> (
    NativeExecutionReadiness,
    ReadinessReceiver<NativeExecutionStart>,
) {
    let (sender, receiver) = std::sync::mpsc::sync_channel(1);
    (
        NativeExecutionReadiness {
            sender,
            ready: Arc::new(AtomicBool::new(false)),
        },
        receiver,
    )
}

pub(crate) fn execute_native_submission(
    submission: FragmentSubmission,
    context: NativeExecutionContext,
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
    let query_options =
        apply_query_option_overrides(Some(instance.runtime_options().query_options().clone()));
    let runtime_state = build_runtime_state(
        RuntimeStateInputs {
            query_options,
            query_id: Some(query_id),
            runtime_filter_params: Some(instance.runtime_filter_params().clone()),
            fragment_instance_id: Some(fragment_instance_id),
            backend_num: Some(backend_num),
            mem_tracker: context.mem_tracker.clone(),
            native_runtime_filter_context: context.runtime_filter.clone(),
        },
        context.profiler.as_ref(),
    )
    .map_err(|error| FragmentExecutionError::new(FragmentExecutionErrorKind::Pipeline, error))?;

    if program.sink().kind() == FragmentSinkKind::Result {
        #[cfg(test)]
        wait_at_test_result_buffer_creation_gate(fragment_instance_id);
        #[cfg(test)]
        maybe_panic_before_ready(fragment_instance_id);
        prepare_result_buffer(
            fragment_instance_id,
            instance.runtime_options().typed_result_sink(),
            context.mem_tracker.as_ref(),
        );
        context.readiness.signal_ready();
    }
    if let Some(profiler) = context.profiler.as_ref() {
        record_runtime_filter_dormancy(profiler, program.runtime_filters().dormancy_facts());
    }
    let sink = materialize_fragment_sink(program, instance).map_err(|error| {
        FragmentExecutionError::new(FragmentExecutionErrorKind::Sink, error.to_string())
    })?;
    if program.sink().kind() != FragmentSinkKind::Result {
        context.readiness.signal_ready();
    }

    // PBF-2 launches each validated submission once. PBF-4 will materialize
    // instance-owned scan and exchange state instead of cloning bound nodes.
    let exec_plan = program.plan().clone();
    let _exec_timer = context
        .profiler
        .as_ref()
        .map(|profiler| profiler.scoped_timer("PipelineExecuteTime"));
    execute_native_plan_with_pipeline(
        exec_plan,
        debug_exec_node_output(),
        Duration::from_millis(50),
        sink,
        Some((fragment_instance_id.hi, fragment_instance_id.lo)),
        context.profiler,
        pipeline_dop,
        runtime_state,
        Some(query_id),
        None,
        Some(backend_num),
    )
    .map_err(|error| FragmentExecutionError::new(FragmentExecutionErrorKind::Pipeline, error))?;

    Ok(FragmentOutput { profile_json: None })
}

fn prepare_result_buffer(
    finst_id: crate::common::types::UniqueId,
    typed_result_sink: bool,
    mem_tracker: Option<&Arc<MemTracker>>,
) {
    if typed_result_sink {
        result_buffer::create_typed_sender(finst_id);
    } else {
        result_buffer::create_sender(finst_id);
    }
    if let Some(root) = mem_tracker {
        let tracker = MemTracker::new_child(format!("ResultBuffer: finst={finst_id}"), root);
        result_buffer::set_mem_tracker(finst_id, tracker);
    }
}

fn record_runtime_filter_dormancy(profiler: &Profiler, facts: &[RuntimeFilterDormancyFact]) {
    let dormancy = profiler.child(NATIVE_RUNTIME_FILTER_DORMANCY_PROFILE);
    dormancy.counter_set_unit(
        NATIVE_RUNTIME_FILTER_BINDING_COUNT,
        i64::try_from(facts.len()).expect("runtime-filter binding count fits i64"),
    );
    for fact in facts {
        let binding = dormancy.child(format!("Binding{}", fact.binding_id));
        binding.add_info_string("BindingId", fact.binding_id.to_string());
        binding.add_info_string("ChannelId", fact.channel_id.to_string());
        binding.add_info_string("NodeId", fact.node_id.to_string());
        binding.add_info_string(
            "ApplyPoint",
            match fact.apply_point {
                RuntimeFilterApplyPoint::NodeInput => "NodeInput",
                RuntimeFilterApplyPoint::NodeOutput => "NodeOutput",
            },
        );
        binding.add_info_string(
            "Role",
            match fact.role {
                RuntimeFilterDormancyRole::Producer => "Producer",
                RuntimeFilterDormancyRole::Consumer => "Consumer",
            },
        );
        binding.counter_set_unit(NATIVE_RUNTIME_FILTER_VALIDATED_LOOKUP, 1);
        binding.counter_set_unit(NATIVE_RUNTIME_FILTER_DEPLOYMENT_NOT_INSTALLED, 1);
        for counter in NATIVE_RUNTIME_FILTER_ZERO_SIDE_EFFECT_COUNTERS {
            binding.counter_set_unit(counter, 0);
        }
    }
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
        RuntimeFilterApplyPoint, RuntimeFilterContract, RuntimeFilterDormancyFact,
        RuntimeFilterDormancyRole,
    };
    use crate::exec::fragment::sink::FragmentSinkProgram;
    use crate::exec::node::values::ValuesNode;
    use crate::exec::node::{ExecNode, ExecNodeKind, ExecPlan};
    use crate::runtime::fragment::instance::{
        BackendNum, ExchangeInputAssignments, FragmentInstanceId, FragmentInstanceSpec,
        FragmentRuntimeOptions, FragmentSinkAssignment, ScanAssignments,
    };
    use crate::runtime::fragment::submission::FragmentSubmission;
    use crate::runtime::profile::{
        NATIVE_RUNTIME_FILTER_DORMANCY_PROFILE, NATIVE_RUNTIME_FILTER_ZERO_SIDE_EFFECT_COUNTERS,
        Profiler,
    };
    use crate::runtime::query_context::QueryId;
    use crate::runtime::query_options::QueryOptions;
    use crate::runtime::runtime_filter_params::RuntimeFilterParams;

    use super::{
        NativeExecutionContext, execute_native_submission, native_execution_readiness_channel,
        record_runtime_filter_dormancy,
    };

    fn noop_values_submission() -> FragmentSubmission {
        let plan = ExecPlan {
            arena: ExprArena::default(),
            root: ExecNode {
                kind: ExecNodeKind::Values(ValuesNode {
                    chunk: Chunk::default(),
                    node_id: 10,
                }),
            },
        };
        let program = Arc::new(FragmentProgram::new(
            plan,
            FragmentSinkSpec::try_new(FragmentSinkProgram::Noop).expect("noop sink"),
            FragmentProgramOptions::new(FragmentContractVersion::CURRENT),
            BTreeMap::new(),
            BTreeMap::new(),
            RuntimeFilterContract::new(BTreeSet::new(), BTreeSet::new()),
        ));
        let instance = FragmentInstanceSpec::new(
            FragmentContractVersion::CURRENT,
            QueryId { hi: 81, lo: 82 },
            FragmentInstanceId::new(UniqueId { hi: 83, lo: 84 }),
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
    fn executes_noop_values_submission_without_wire_inputs() {
        let profiler = Profiler::new("native submission");
        let (readiness, _receiver) = native_execution_readiness_channel();
        let output = execute_native_submission(
            noop_values_submission(),
            NativeExecutionContext {
                profiler: Some(profiler.clone()),
                mem_tracker: None,
                readiness,
                runtime_filter: None,
            },
        )
        .expect("noop submission executes");

        assert!(output.profile_json.is_none());
        let dormancy = profiler
            .get_child(NATIVE_RUNTIME_FILTER_DORMANCY_PROFILE)
            .expect("dormancy profile");
        assert_eq!(dormancy.counter_value("BindingCount"), Some(0));
    }

    #[test]
    fn records_validated_dormancy_facts_without_installing_runtime_filters() {
        let profiler = Profiler::new("native submission");
        record_runtime_filter_dormancy(
            &profiler,
            &[RuntimeFilterDormancyFact {
                binding_id: 7,
                channel_id: 9,
                node_id: 11,
                apply_point: RuntimeFilterApplyPoint::NodeInput,
                role: RuntimeFilterDormancyRole::Consumer,
            }],
        );

        let dormancy = profiler
            .get_child(NATIVE_RUNTIME_FILTER_DORMANCY_PROFILE)
            .expect("dormancy profile");
        assert_eq!(dormancy.counter_value("BindingCount"), Some(1));
        let binding = dormancy.get_child("Binding7").expect("binding profile");
        assert_eq!(binding.get_info_string("ChannelId").as_deref(), Some("9"));
        assert_eq!(binding.get_info_string("NodeId").as_deref(), Some("11"));
        assert_eq!(
            binding.get_info_string("ApplyPoint").as_deref(),
            Some("NodeInput")
        );
        assert_eq!(binding.get_info_string("Role").as_deref(), Some("Consumer"));
        assert_eq!(binding.counter_value("ValidatedLookup"), Some(1));
        assert_eq!(binding.counter_value("DeploymentNotInstalled"), Some(1));
        for counter in NATIVE_RUNTIME_FILTER_ZERO_SIDE_EFFECT_COUNTERS {
            assert_eq!(binding.counter_value(counter), Some(0), "counter={counter}");
        }
    }
}
