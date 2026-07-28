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

use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::mpsc::{Receiver as ReadinessReceiver, SyncSender as ReadinessSender};
use std::time::Duration;

use crate::common::config::debug_exec_node_output;
use crate::exec::fragment::program::FragmentSinkKind;
use crate::exec::pipeline::executor::execute_native_plan_with_pipeline;
use crate::runtime::fragment::error::{FragmentExecutionError, FragmentExecutionErrorKind};
use crate::runtime::fragment::runtime_state::{
    RuntimeStateInputs, apply_query_option_overrides, build_runtime_state,
};
use crate::runtime::fragment::sink::materialize_fragment_sink;
use crate::runtime::fragment::submission::FragmentSubmission;
use crate::runtime::fragment_output::FragmentOutput;
use crate::runtime::mem_tracker::MemTracker;
use crate::runtime::profile::Profiler;
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

#[derive(Debug, Eq, PartialEq)]
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
    let failure_trigger = configured_fragment_failure_trigger();
    execute_native_submission_with_failure_trigger(submission, context, failure_trigger.as_deref())
}

fn configured_fragment_failure_trigger() -> Option<PathBuf> {
    std::env::var_os("NOVAROCKS_SQL_TEST_FRAGMENT_FAILURE_TRIGGER_FILE").map(PathBuf::from)
}

pub(crate) fn execute_native_submission_with_failure_trigger(
    submission: FragmentSubmission,
    context: NativeExecutionContext,
    failure_trigger: Option<&Path>,
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
    let sink = materialize_fragment_sink(program, instance).map_err(|error| {
        FragmentExecutionError::new(FragmentExecutionErrorKind::Sink, error.to_string())
    })?;
    if program.sink().kind() != FragmentSinkKind::Result {
        context.readiness.signal_ready();
    }
    if let Some(token) = consume_fragment_failure_trigger(failure_trigger)? {
        eprintln!(
            "NOVAROCKS_FRAGMENT_EXECUTOR_FAILURE_INJECTED token={} query_hi={} query_lo={} finst_hi={} finst_lo={}",
            token, query_id.hi, query_id.lo, fragment_instance_id.hi, fragment_instance_id.lo
        );
        return Err(FragmentExecutionError::new(
            FragmentExecutionErrorKind::Pipeline,
            "fragment executor failure injected after start",
        ));
    }

    // PBF-2 launches each validated submission once. Instance-owned scan and
    // exchange state is materialized per-instance from the shared static
    // program (no bound ops baked into the plan nodes).
    let exec_plan = program.plan().clone();
    let exchange_bindings =
        crate::runtime::fragment::exchange::materialize_exchange_bindings(program, instance);
    let scan_bindings = crate::runtime::fragment::scan::materialize_scan_bindings(
        program, instance,
    )
    .map_err(|error| {
        FragmentExecutionError::new(FragmentExecutionErrorKind::Pipeline, error.to_string())
    })?;
    let _exec_timer = context
        .profiler
        .as_ref()
        .map(|profiler| profiler.scoped_timer("PipelineExecuteTime"));
    execute_native_plan_with_pipeline(
        exec_plan,
        debug_exec_node_output(),
        Duration::from_millis(50),
        sink,
        exchange_bindings,
        scan_bindings,
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

fn consume_fragment_failure_trigger(
    failure_trigger: Option<&Path>,
) -> Result<Option<String>, FragmentExecutionError> {
    let Some(path) = failure_trigger else {
        return Ok(None);
    };
    static NEXT_CLAIM: AtomicU64 = AtomicU64::new(1);
    let claim_sequence = NEXT_CLAIM.fetch_add(1, Ordering::Relaxed);
    let claim_path =
        path.with_extension(format!("claimed-{}-{claim_sequence}", std::process::id()));
    match std::fs::rename(path, &claim_path) {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => {
            return Err(FragmentExecutionError::new(
                FragmentExecutionErrorKind::Pipeline,
                format!(
                    "claim fragment executor failure trigger {} failed: {error}",
                    path.display()
                ),
            ));
        }
    }
    let token_result = std::fs::read_to_string(&claim_path);
    let cleanup_result = std::fs::remove_file(&claim_path);
    let token = token_result.map_err(|error| {
        FragmentExecutionError::new(
            FragmentExecutionErrorKind::Pipeline,
            format!(
                "read claimed fragment executor failure trigger {} failed: {error}",
                claim_path.display()
            ),
        )
    })?;
    cleanup_result.map_err(|error| {
        FragmentExecutionError::new(
            FragmentExecutionErrorKind::Pipeline,
            format!(
                "remove claimed fragment executor failure trigger {} failed: {error}",
                claim_path.display()
            ),
        )
    })?;
    let token = token.trim();
    if token.is_empty() || token.split_ascii_whitespace().count() != 1 {
        return Err(FragmentExecutionError::new(
            FragmentExecutionErrorKind::Pipeline,
            format!(
                "fragment executor failure trigger {} contains an invalid evidence token",
                path.display()
            ),
        ));
    }
    Ok(Some(token.to_string()))
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
    use crate::runtime::fragment::submission::FragmentSubmission;
    use crate::runtime::profile::Profiler;
    use crate::runtime::query_context::QueryId;
    use crate::runtime::query_options::QueryOptions;

    use super::{
        NativeExecutionContext, NativeExecutionStart, consume_fragment_failure_trigger,
        execute_native_submission, execute_native_submission_with_failure_trigger,
        native_execution_readiness_channel,
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
        let instance = FragmentInstanceSpec::new_native(
            FragmentContractVersion::CURRENT,
            QueryId { hi: 81, lo: 82 },
            FragmentInstanceId::new(UniqueId { hi: 83, lo: 84 }),
            ScanAssignments::default(),
            ExchangeInputAssignments::default(),
            FragmentSinkAssignment::None,
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
    }

    #[test]
    fn fragment_executor_failure_trigger_fires_after_readiness_and_only_once() {
        let temp = tempfile::tempdir().expect("temp trigger directory");
        let trigger = temp.path().join("fail-next-fragment");
        std::fs::write(&trigger, b"armed").expect("arm fragment failure");
        let (readiness, receiver) = native_execution_readiness_channel();

        let error = execute_native_submission_with_failure_trigger(
            noop_values_submission(),
            NativeExecutionContext {
                profiler: None,
                mem_tracker: None,
                readiness,
                runtime_filter: None,
            },
            Some(trigger.as_path()),
        )
        .expect_err("armed fragment must fail");

        assert_eq!(
            receiver.recv().expect("executor readiness"),
            NativeExecutionStart::Ready,
            "the injected failure must happen after the fragment executor publishes readiness"
        );
        assert!(
            error
                .to_string()
                .contains("fragment executor failure injected after start"),
            "{error}"
        );
        assert!(!trigger.exists(), "the trigger must be consumed once");

        let (readiness, _receiver) = native_execution_readiness_channel();
        execute_native_submission_with_failure_trigger(
            noop_values_submission(),
            NativeExecutionContext {
                profiler: None,
                mem_tracker: None,
                readiness,
                runtime_filter: None,
            },
            Some(trigger.as_path()),
        )
        .expect("consumed trigger must not poison later fragments");
    }

    #[test]
    fn fragment_failure_trigger_atomically_carries_step_token() {
        let temp = tempfile::tempdir().expect("temp trigger directory");
        let trigger = temp.path().join("fail-next-fragment");
        std::fs::write(&trigger, b"step-token-17").expect("arm fragment failure");

        assert_eq!(
            consume_fragment_failure_trigger(Some(trigger.as_path()))
                .expect("consume trigger token"),
            Some("step-token-17".to_string())
        );
        assert_eq!(
            consume_fragment_failure_trigger(Some(trigger.as_path())).expect("trigger is one-shot"),
            None
        );
    }
}
