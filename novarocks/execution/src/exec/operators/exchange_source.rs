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
//! Exchange source for receiving distributed upstream data.
//!
//! Responsibilities:
//! - Fetches remote stream pages from exchange service and reconstructs chunks for local pipeline processing.
//! - Handles end-of-stream coordination, sender completion tracking, and error propagation.
//!
//! Key exported interfaces:
//! - Types: `ExchangeSourceFactory`.
//!
//! Current limitations:
//! - Implements only the execution semantics currently wired by novarocks plan lowering and pipeline builder.
//! - Unsupported states should be surfaced as explicit runtime errors instead of fallback behavior.

use crate::runtime::fragment::ExecutionResult;

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use crate::exec::chunk::Chunk;
use crate::exec::pipeline::binding::ExchangeBinding;
use crate::exec::pipeline::operator::{DriverBlockDeadline, Operator, ProcessorOperator};
use crate::exec::pipeline::operator_factory::OperatorFactory;
use crate::exec::pipeline::schedule::observer::Observable;
use crate::runtime::exchange;
use crate::runtime::fragment::io::ExchangeReceiverKey;
use crate::runtime::runtime_state::RuntimeState;
use novarocks_types::UniqueId;
use tracing::debug;

static EXCHANGE_SOURCE_READY_LOG_COUNT: AtomicU64 = AtomicU64::new(0);

fn should_log_exchange_source_ready() -> bool {
    let count = EXCHANGE_SOURCE_READY_LOG_COUNT.fetch_add(1, Ordering::Relaxed);
    count.is_multiple_of(1024)
}

struct ExchangeIdleDeadlineState {
    current: Option<DriverBlockDeadline>,
    next_token: u64,
    canceled: bool,
}

struct ExchangeIdleDeadline {
    state: Mutex<ExchangeIdleDeadlineState>,
}

struct ExchangeIdleProgressListener {
    deadline: std::sync::Weak<ExchangeIdleDeadline>,
    observable: std::sync::Weak<Observable>,
}

struct ExchangeIdleProgressState {
    receiver_observable: Option<std::sync::Weak<Observable>>,
    listeners: Vec<ExchangeIdleProgressListener>,
}

struct ExchangeIdleProgress {
    state: Mutex<ExchangeIdleProgressState>,
}

impl ExchangeIdleProgress {
    fn new() -> Self {
        Self {
            state: Mutex::new(ExchangeIdleProgressState {
                receiver_observable: None,
                listeners: Vec::new(),
            }),
        }
    }

    fn register(&self, deadline: &Arc<ExchangeIdleDeadline>, observable: &Arc<Observable>) {
        self.state
            .lock()
            .expect("exchange idle progress lock")
            .listeners
            .push(ExchangeIdleProgressListener {
                deadline: Arc::downgrade(deadline),
                observable: Arc::downgrade(observable),
            });
    }

    fn attach_receiver(self: &Arc<Self>, receiver_observable: &Arc<Observable>) {
        let should_attach = {
            let mut state = self.state.lock().expect("exchange idle progress lock");
            match state.receiver_observable.as_ref() {
                Some(current) if current.ptr_eq(&Arc::downgrade(receiver_observable)) => false,
                Some(_) => {
                    debug_assert!(false, "exchange source factory changed receiver observable");
                    false
                }
                None => {
                    state.receiver_observable = Some(Arc::downgrade(receiver_observable));
                    true
                }
            }
        };
        if !should_attach {
            return;
        }

        let progress = Arc::downgrade(self);
        receiver_observable.add_observer(Arc::new(move || {
            if let Some(progress) = progress.upgrade() {
                progress.publish();
            }
        }));
    }

    fn publish(&self) {
        let listeners = {
            let mut state = self.state.lock().expect("exchange idle progress lock");
            let mut listeners = Vec::with_capacity(state.listeners.len());
            state.listeners.retain(|listener| {
                let Some(deadline) = listener.deadline.upgrade() else {
                    return false;
                };
                let Some(observable) = listener.observable.upgrade() else {
                    return false;
                };
                listeners.push((deadline, observable));
                true
            });
            listeners
        };

        // A receiver packet belongs to the shared exchange key, not to the
        // driver that happens to win the queue pop. Reset every live driver's
        // idle period before publishing any wake-up, so a fast sibling cannot
        // consume the packet before another driver observes the progress.
        for (deadline, _) in &listeners {
            deadline.clear();
        }
        for (_, observable) in listeners {
            observable.notify_observers();
        }
    }
}

impl ExchangeIdleDeadline {
    fn new() -> Self {
        Self {
            state: Mutex::new(ExchangeIdleDeadlineState {
                current: None,
                next_token: 0,
                canceled: false,
            }),
        }
    }

    fn arm(&self, timeout: std::time::Duration) -> Option<DriverBlockDeadline> {
        let mut state = self.state.lock().expect("exchange deadline state lock");
        if state.canceled {
            return None;
        }
        if let Some(current) = state.current {
            return Some(current);
        }
        state.next_token = state.next_token.wrapping_add(1).max(1);
        let deadline = DriverBlockDeadline::new(Instant::now() + timeout, state.next_token);
        state.current = Some(deadline);
        Some(deadline)
    }

    fn is_expired(&self) -> bool {
        self.state
            .lock()
            .expect("exchange deadline state lock")
            .current
            .is_some_and(|deadline| deadline.at() <= Instant::now())
    }

    fn clear(&self) {
        self.state
            .lock()
            .expect("exchange deadline state lock")
            .current = None;
    }

    fn cancel(&self) {
        let mut state = self.state.lock().expect("exchange deadline state lock");
        state.canceled = true;
        state.current = None;
    }
}

/// Factory for exchange source operators that fetch and decode remote stream pages.
pub struct ExchangeSourceFactory {
    name: String,
    node: ExchangeSourceIdentity,
    binding: ExchangeBinding,
    idle_progress: Arc<ExchangeIdleProgress>,
}

/// The receiver registry owns the exact schema and decoded chunks. A source
/// driver needs only routing and timeout facts, never another schema owner.
#[derive(Clone, Copy)]
struct ExchangeSourceIdentity {
    node_id: i32,
    timeout: std::time::Duration,
}

impl ExchangeSourceFactory {
    /// Build the source for a compiled ExchangeSource (local-compiler
    /// output). Its builder validates the frozen local node's supported shape.
    /// The receiver registry supplies exact typed chunks; no expression arena
    /// or duplicate expected schema is constructed for this driver.
    pub(crate) fn new_compiled(
        node_id: i32,
        timeout: std::time::Duration,
        binding: ExchangeBinding,
    ) -> Self {
        Self {
            name: format!("EXCHANGE_SOURCE (id={node_id})"),
            node: ExchangeSourceIdentity { node_id, timeout },
            binding,
            idle_progress: Arc::new(ExchangeIdleProgress::new()),
        }
    }
}

impl OperatorFactory for ExchangeSourceFactory {
    fn name(&self) -> &str {
        &self.name
    }

    fn create(&self, _dop: i32, driver_id: i32) -> Box<dyn Operator> {
        let source_observable = Arc::new(Observable::new());
        let idle_deadline = Arc::new(ExchangeIdleDeadline::new());
        self.idle_progress
            .register(&idle_deadline, &source_observable);
        Box::new(ExchangeSourceOperator {
            name: self.name.clone(),
            node: self.node.clone(),
            binding: self.binding.clone(),
            driver_id,
            receiver: None,
            idle_deadline,
            idle_progress: Arc::clone(&self.idle_progress),
            source_observable,
            finished: false,
            logged_first_pull: false,
            logged_first_none: false,
            #[cfg(novarocks_dev_controls)]
            last_partial_eos_marker_count: 0,
            receiver_mem_tracker_ready: false,
        })
    }

    fn is_source(&self) -> bool {
        true
    }
}

struct ExchangeSourceOperator {
    name: String,
    node: ExchangeSourceIdentity,
    binding: ExchangeBinding,
    driver_id: i32,
    receiver: Option<exchange::ExchangeReceiverHandle>,
    source_observable: Arc<Observable>,
    idle_deadline: Arc<ExchangeIdleDeadline>,
    idle_progress: Arc<ExchangeIdleProgress>,
    finished: bool,
    logged_first_pull: bool,
    logged_first_none: bool,
    #[cfg(novarocks_dev_controls)]
    last_partial_eos_marker_count: usize,
    receiver_mem_tracker_ready: bool,
}

impl Operator for ExchangeSourceOperator {
    fn name(&self) -> &str {
        &self.name
    }

    fn prepare(&mut self) -> ExecutionResult<()> {
        if self.receiver.is_some() {
            return Ok(());
        }
        let receiver = self.binding.receiver_port.receiver_handle(
            receiver_key(self.binding.key),
            self.binding.expected_senders,
        )?;
        self.idle_progress.attach_receiver(&receiver.observable());
        self.receiver = Some(receiver);
        debug!(
            "ExchangeSource prepared: finst={} node_id={} expected_senders={} timeout={:?}",
            self.binding.key.finst_uuid(),
            self.node.node_id,
            self.binding.expected_senders,
            self.node.timeout
        );
        Ok(())
    }

    fn close(&mut self) -> ExecutionResult<()> {
        self.idle_deadline.cancel();
        Ok(())
    }

    fn cancel(&mut self) {
        self.idle_deadline.cancel();
    }

    fn as_processor_mut(&mut self) -> Option<&mut dyn ProcessorOperator> {
        Some(self)
    }

    fn as_processor_ref(&self) -> Option<&dyn ProcessorOperator> {
        Some(self)
    }

    fn is_finished(&self) -> bool {
        self.finished
    }
}

impl ProcessorOperator for ExchangeSourceOperator {
    fn need_input(&self) -> bool {
        false
    }

    fn has_output(&self) -> bool {
        if self.finished {
            return false;
        }
        let Some(receiver) = self.receiver.as_ref() else {
            return false;
        };
        let ready = receiver.has_output_or_finished(self.binding.expected_senders);
        if ready {
            self.idle_deadline.clear();
        } else if self.idle_deadline.is_expired() {
            if should_log_exchange_source_ready() {
                debug!(
                    "ExchangeSource has_output due to idle timeout: finst={} node_id={} timeout={:?}",
                    self.binding.key.finst_uuid(),
                    self.node.node_id,
                    self.node.timeout
                );
            }
            return true;
        }
        if ready && should_log_exchange_source_ready() {
            debug!(
                "ExchangeSource has_output due to receiver: finst={} node_id={} expected_senders={}",
                self.binding.key.finst_uuid(),
                self.node.node_id,
                self.binding.expected_senders
            );
        }
        ready
    }

    fn push_chunk(&mut self, _state: &RuntimeState, _chunk: Chunk) -> ExecutionResult<()> {
        Err("exchange source operator does not accept input"
            .to_string()
            .into())
    }

    fn pull_chunk(&mut self, state: &RuntimeState) -> ExecutionResult<Option<Chunk>> {
        if self.finished {
            return Ok(None);
        }

        if self.receiver.is_none() {
            return Err("exchange source operator not prepared".to_string().into());
        }

        if !self.receiver_mem_tracker_ready {
            self.receiver_mem_tracker_ready = true;
            if let Some(root) = state.mem_tracker() {
                let _ = self
                    .binding
                    .receiver_port
                    .ensure_mem_tracker(receiver_key(self.binding.key), &root)?;
            }
        }

        if !self.logged_first_pull {
            self.logged_first_pull = true;
            debug!(
                "ExchangeSource first pull: node_id={} driver_id={}",
                self.node.node_id, self.driver_id
            );
        }

        loop {
            let receiver = self.receiver.as_ref().expect("receiver");
            let out = receiver
                .try_pop_next_with_stats(self.binding.expected_senders)
                .map_err(|e| e.to_string())?;

            match out {
                Some(exchange::ExchangePopResult::Chunk(chunk)) => {
                    self.idle_deadline.clear();
                    let input_rows = chunk.len();
                    if chunk.is_empty() {
                        debug!(
                            "ExchangeSource filtered empty chunk: node_id={} driver_id={} input_rows={}",
                            self.node.node_id, self.driver_id, input_rows
                        );
                        continue;
                    }
                    debug!(
                        "ExchangeSource output chunk: node_id={} driver_id={} input_rows={} output_rows={}",
                        self.node.node_id,
                        self.driver_id,
                        input_rows,
                        chunk.len()
                    );
                    return Ok(Some(chunk));
                }
                Some(exchange::ExchangePopResult::Finished(stats)) => {
                    self.idle_deadline.cancel();
                    #[cfg(novarocks_dev_controls)]
                    if let Some(observer) = state
                        .exchange_test_observer()
                        .filter(|observer| observer.is_active())
                    {
                        let snapshot = self
                            .binding
                            .receiver_port
                            .snapshot(receiver_key(self.binding.key));
                        exchange::emit_exchange_snapshot_marker(Some(observer.as_ref()), || {
                            format!(
                                "event=source_finished finst={} node_id={} driver_id={} expected_senders={} snapshot={:?} source_generation={}",
                                self.binding.key.finst_uuid(),
                                self.node.node_id,
                                self.driver_id,
                                self.binding.expected_senders,
                                snapshot,
                                self.source_observable.generation(),
                            )
                        });
                    }
                    debug!(
                        "ExchangeSource finished: finst={} node_id={} driver_id={} request_received={} bytes_received={} deserialize_ns={} chunks_received={} rows_received={}",
                        self.binding.key.finst_uuid(),
                        self.node.node_id,
                        self.driver_id,
                        stats.request_received,
                        stats.bytes_received,
                        stats.deserialize_ns,
                        stats.chunks_received,
                        stats.rows_received
                    );
                    self.finished = true;
                    return Ok(None);
                }
                None => {
                    if self.idle_deadline.is_expired() {
                        let snapshot = self
                            .binding
                            .receiver_port
                            .snapshot(receiver_key(self.binding.key));
                        debug!(
                            "ExchangeSource timeout waiting for senders: finst_id={} node_id={} timeout={:?} expected_senders={} snapshot={:?}",
                            self.binding.key.finst_uuid(),
                            self.node.node_id,
                            self.node.timeout,
                            self.binding.expected_senders,
                            snapshot,
                        );
                        return Err(format!(
                            "exchange timeout waiting for senders: finst_id={} node_id={} expected_senders={} snapshot={snapshot:?}",
                            self.binding.key.finst_uuid(),
                            self.node.node_id,
                            self.binding.expected_senders,
                        ).into());
                    }
                    let first_none = !self.logged_first_none;
                    if first_none {
                        self.logged_first_none = true;
                    }
                    #[cfg(novarocks_dev_controls)]
                    if let Some(observer) = state
                        .exchange_test_observer()
                        .filter(|observer| observer.is_active())
                    {
                        let snapshot = self
                            .binding
                            .receiver_port
                            .snapshot(receiver_key(self.binding.key));
                        if first_none {
                            let first_snapshot = snapshot.clone();
                            exchange::emit_exchange_snapshot_marker(
                                Some(observer.as_ref()),
                                || {
                                    format!(
                                        "event=source_first_block finst={} node_id={} driver_id={} expected_senders={} snapshot={:?} source_generation={}",
                                        self.binding.key.finst_uuid(),
                                        self.node.node_id,
                                        self.driver_id,
                                        self.binding.expected_senders,
                                        first_snapshot,
                                        self.source_observable.generation(),
                                    )
                                },
                            );
                        }
                        if let Some(snapshot) = snapshot
                            && snapshot.finished_senders > self.last_partial_eos_marker_count
                            && snapshot.finished_senders < self.binding.expected_senders
                        {
                            self.last_partial_eos_marker_count = snapshot.finished_senders;
                            exchange::emit_exchange_snapshot_marker(
                                Some(observer.as_ref()),
                                || {
                                    format!(
                                        "event=source_partial_eos_block finst={} node_id={} driver_id={} expected_senders={} snapshot={:?} source_generation={}",
                                        self.binding.key.finst_uuid(),
                                        self.node.node_id,
                                        self.driver_id,
                                        self.binding.expected_senders,
                                        snapshot,
                                        self.source_observable.generation(),
                                    )
                                },
                            );
                        }
                    }
                    if first_none {
                        debug!(
                            "ExchangeSource no output yet: node_id={} driver_id={}",
                            self.node.node_id, self.driver_id
                        );
                    }
                    return Ok(None);
                }
            }
        }
    }

    fn set_finishing(&mut self, _state: &RuntimeState) -> ExecutionResult<()> {
        Ok(())
    }

    fn precondition_dependency(
        &self,
    ) -> Option<crate::exec::pipeline::dependency::DependencyHandle> {
        None
    }

    fn source_observable(&self) -> Option<Arc<Observable>> {
        Some(Arc::clone(&self.source_observable))
    }

    fn source_block_deadline(&self) -> Option<DriverBlockDeadline> {
        self.idle_deadline.arm(self.node.timeout)
    }
}

impl Drop for ExchangeSourceOperator {
    fn drop(&mut self) {
        self.idle_deadline.cancel();
    }
}

fn receiver_key(key: exchange::ExchangeKey) -> ExchangeReceiverKey {
    ExchangeReceiverKey {
        fragment_instance_id: UniqueId::new(key.finst_id_hi, key.finst_id_lo),
        node_id: key.node_id,
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    use arrow::array::{Array, Int32Array};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;

    use super::*;
    use crate::exec::pipeline::binding::ExchangeBinding;
    use crate::exec::pipeline::driver::PipelineDriver;
    use crate::exec::pipeline::fragment_context::FragmentContext;
    use crate::exec::pipeline::global_driver_executor::{
        DriverTask, FragmentCompletion, GlobalDriverExecutor,
    };
    use crate::exec::pipeline::operator::Operator;
    use crate::runtime::fragment::io::ExchangeReceiverPort;
    use crate::runtime::fragment::io::exchange::in_process_test_exchange_receiver_port;
    use crate::runtime::runtime_state::RuntimeState;
    use novarocks_types::SlotId;

    fn int32_chunk(values: Vec<i32>) -> Chunk {
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int32, false)]));
        let array = Arc::new(Int32Array::from(values)) as arrow::array::ArrayRef;
        let batch = RecordBatch::try_new(schema, vec![array]).expect("test batch");
        crate::exec::chunk::typed_fixture::chunk(
            batch,
            &[SlotId::new(1)],
            &[novarocks_type_contract::FunctionValueType::new(
                DataType::Int32,
                false,
            )],
        )
    }

    fn register_int32_receiver(binding: &ExchangeBinding) {
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int32, false)]));
        let layout = crate::exec::chunk::typed_fixture::layout(
            schema,
            &[SlotId::new(1)],
            &[novarocks_type_contract::FunctionValueType::new(
                DataType::Int32,
                false,
            )],
        );
        let creation_factory = crate::exec::pipeline::operator::input_construction::try_runtime_input_construction_factory(
            crate::exec::pipeline::pipeline::representation_construction_profile().unwrap(),
            crate::runtime::mem_tracker::MemTracker::new_root("exchange source receiver fixture"),
            Arc::new(crate::runtime::runtime_state::RuntimeErrorState::default()),
        ).unwrap();
        let expected_chunk_schema = creation_factory
            .create_compiled_chunk_schema(&layout)
            .unwrap();
        binding
            .receiver_port
            .register(crate::runtime::fragment::io::ExchangeReceiverRegistration {
                key: receiver_key(binding.key),
                expected_senders: binding.expected_senders,
                expected_chunk_schema,
                column_binding: exchange::ExchangeColumnBinding::Positional,
                creation_factory,
            })
            .expect("register typed receiver");
    }

    fn int32_values(chunk: &Chunk) -> Vec<i32> {
        let array = chunk
            .column_by_slot_id(SlotId::new(1))
            .expect("slot column");
        let ints = array
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("int32 array");
        (0..ints.len()).map(|row| ints.value(row)).collect()
    }

    fn runtime_state() -> RuntimeState {
        RuntimeState::default()
    }

    struct CountingSink {
        rows: Arc<AtomicUsize>,
        finishing: bool,
    }

    impl Operator for CountingSink {
        fn name(&self) -> &str {
            "COUNTING_SINK"
        }

        fn is_finished(&self) -> bool {
            self.finishing
        }

        fn as_processor_mut(&mut self) -> Option<&mut dyn ProcessorOperator> {
            Some(self)
        }

        fn as_processor_ref(&self) -> Option<&dyn ProcessorOperator> {
            Some(self)
        }
    }

    impl ProcessorOperator for CountingSink {
        fn need_input(&self) -> bool {
            !self.finishing
        }

        fn has_output(&self) -> bool {
            false
        }

        fn push_chunk(&mut self, _state: &RuntimeState, chunk: Chunk) -> ExecutionResult<()> {
            self.rows.fetch_add(chunk.len(), Ordering::AcqRel);
            Ok(())
        }

        fn pull_chunk(&mut self, _state: &RuntimeState) -> ExecutionResult<Option<Chunk>> {
            Ok(None)
        }

        fn set_finishing(&mut self, _state: &RuntimeState) -> ExecutionResult<()> {
            self.finishing = true;
            Ok(())
        }
    }

    struct TimeoutPipeline {
        state: Arc<RuntimeState>,
        observable: Arc<Observable>,
        rows: Arc<AtomicUsize>,
        completion: Arc<FragmentCompletion>,
        fragment: Arc<FragmentContext>,
        source: Box<dyn Operator>,
        binding: ExchangeBinding,
    }

    fn timeout_pipeline(key: exchange::ExchangeKey, timeout: Duration) -> TimeoutPipeline {
        let binding = ExchangeBinding {
            key,
            expected_senders: 1,
            receiver_port: in_process_test_exchange_receiver_port(),
        };
        register_int32_receiver(&binding);
        let factory = ExchangeSourceFactory::new_compiled(key.node_id, timeout, binding.clone());
        let state = Arc::new(runtime_state());
        let mut source = factory.create(1, 0);
        source.prepare().expect("prepare exchange source");
        source
            .bind_runtime_state(&state)
            .expect("bind exchange source runtime");
        let observable = source
            .as_processor_ref()
            .and_then(ProcessorOperator::source_observable)
            .expect("exchange source observable");
        let rows = Arc::new(AtomicUsize::new(0));
        let completion = FragmentCompletion::new(1);
        let fragment = Arc::new(FragmentContext::new(
            None,
            Arc::clone(&state),
            Some((key.finst_id_hi, key.finst_id_lo)),
            None,
            None,
            None,
        ));
        TimeoutPipeline {
            state,
            observable,
            rows,
            completion,
            fragment,
            source,
            binding,
        }
    }

    fn submit_timeout_pipeline(
        executor: &GlobalDriverExecutor,
        state: Arc<RuntimeState>,
        source: Box<dyn Operator>,
        rows: Arc<AtomicUsize>,
        completion: Arc<FragmentCompletion>,
        fragment: Arc<FragmentContext>,
    ) {
        let driver = PipelineDriver::new(
            0,
            vec![
                source,
                Box::new(CountingSink {
                    rows,
                    finishing: false,
                }),
            ],
            None,
            Vec::new(),
            state,
            fragment.fragment_instance_id(),
        );
        let task = DriverTask::new(driver, completion, fragment, Duration::from_millis(5));
        executor.submit(vec![task]);
    }

    fn wait_for_exchange_timeout(
        completion: &Arc<FragmentCompletion>,
        fragment: &Arc<FragmentContext>,
    ) -> crate::runtime::fragment::ExecutionFailure {
        let deadline = Instant::now() + Duration::from_secs(2);
        while !completion.should_abort() && Instant::now() < deadline {
            std::thread::yield_now();
        }
        if !completion.should_abort() {
            let error = "exchange timeout test did not complete".to_string();
            completion.fail(error.clone());
            fragment.set_final_status(error);
        }
        completion
            .wait()
            .expect_err("exchange timeout must fail the pipeline")
    }

    #[test]
    fn no_first_packet_timeout_wakes_a_parked_pipeline() {
        let key = exchange::ExchangeKey {
            finst_id_hi: 92_001,
            finst_id_lo: 92_002,
            node_id: 92_003,
        };
        let TimeoutPipeline {
            state,
            observable,
            rows,
            completion,
            fragment,
            source,
            binding: _,
        } = timeout_pipeline(key, Duration::from_millis(30));
        let before = observable.generation();
        let executor = GlobalDriverExecutor::new(1);
        submit_timeout_pipeline(
            &executor,
            state,
            source,
            rows,
            Arc::clone(&completion),
            Arc::clone(&fragment),
        );

        let error = wait_for_exchange_timeout(&completion, &fragment);

        assert!(
            error
                .detail()
                .contains("exchange timeout waiting for senders")
        );
        assert_eq!(
            observable.generation(),
            before,
            "deadline scheduling must not synthesize a receiver event"
        );
    }

    #[test]
    fn partial_data_stall_timeout_rewakes_the_same_parked_pipeline() {
        let key = exchange::ExchangeKey {
            finst_id_hi: 92_011,
            finst_id_lo: 92_012,
            node_id: 92_013,
        };
        let TimeoutPipeline {
            state,
            observable,
            rows,
            completion,
            fragment,
            source,
            binding,
        } = timeout_pipeline(key, Duration::from_millis(100));
        let executor = GlobalDriverExecutor::new(1);
        submit_timeout_pipeline(
            &executor,
            state,
            source,
            Arc::clone(&rows),
            Arc::clone(&completion),
            Arc::clone(&fragment),
        );

        let registration_deadline = Instant::now() + Duration::from_secs(1);
        while observable.num_observers() == 0 && Instant::now() < registration_deadline {
            std::thread::yield_now();
        }
        assert!(observable.num_observers() > 0, "source did not park");
        let after_park = observable.generation();
        binding.receiver_port.push_local(
            receiver_key(key),
            0,
            0,
            vec![int32_chunk(vec![7])],
            false,
        );

        let consume_deadline = Instant::now() + Duration::from_secs(1);
        while rows.load(Ordering::Acquire) == 0 && Instant::now() < consume_deadline {
            std::thread::yield_now();
        }
        assert_eq!(rows.load(Ordering::Acquire), 1);
        let after_receiver = observable.generation();
        assert!(
            after_receiver > after_park,
            "receiver event was not forwarded"
        );

        let error = wait_for_exchange_timeout(&completion, &fragment);
        assert!(
            error
                .detail()
                .contains("exchange timeout waiting for senders")
        );
        assert_eq!(
            observable.generation(),
            after_receiver,
            "deadline scheduling must remain separate from receiver event generations"
        );
    }

    #[test]
    fn sibling_chunk_consumption_resets_every_driver_idle_deadline() {
        let key = exchange::ExchangeKey {
            finst_id_hi: 92_021,
            finst_id_lo: 92_022,
            node_id: 92_023,
        };
        let timeout = Duration::from_secs(1);
        let binding = ExchangeBinding {
            key,
            expected_senders: 1,
            receiver_port: in_process_test_exchange_receiver_port(),
        };
        register_int32_receiver(&binding);
        let factory = ExchangeSourceFactory::new_compiled(key.node_id, timeout, binding.clone());
        let mut first = factory.create(2, 0);
        let mut second = factory.create(2, 1);
        first.prepare().expect("prepare first exchange source");
        second.prepare().expect("prepare second exchange source");

        let first_before = first
            .as_processor_ref()
            .and_then(ProcessorOperator::source_block_deadline)
            .expect("first source deadline");
        let second_before = second
            .as_processor_ref()
            .and_then(ProcessorOperator::source_block_deadline)
            .expect("second source deadline");
        assert_eq!(first_before.token(), 1);
        assert_eq!(second_before.token(), 1);

        binding.receiver_port.push_local(
            receiver_key(key),
            0,
            0,
            vec![int32_chunk(vec![11])],
            false,
        );
        let output = first
            .as_processor_mut()
            .expect("first processor")
            .pull_chunk(&runtime_state())
            .expect("first source pull")
            .expect("first source chunk");
        assert_eq!(int32_values(&output), vec![11]);
        assert!(
            !second
                .as_processor_ref()
                .expect("second processor")
                .has_output(),
            "the sibling consumed the only queued chunk"
        );

        let second_after = second
            .as_processor_ref()
            .and_then(ProcessorOperator::source_block_deadline)
            .expect("reset second source deadline");
        assert_ne!(
            second_after.token(),
            second_before.token(),
            "receiver progress must rotate an idle deadline even when a sibling consumes the chunk"
        );
        assert!(second_after.at() >= second_before.at());
        binding
            .receiver_port
            .cancel_fragment(UniqueId::new(key.finst_id_hi, key.finst_id_lo));
    }

    #[test]
    fn every_parked_driver_observes_the_shared_three_sender_eos() {
        let key = exchange::ExchangeKey {
            finst_id_hi: 92_031,
            finst_id_lo: 92_032,
            node_id: 92_033,
        };
        let timeout = Duration::from_secs(1);
        let binding = ExchangeBinding {
            key,
            expected_senders: 3,
            receiver_port: in_process_test_exchange_receiver_port(),
        };
        register_int32_receiver(&binding);
        let factory = ExchangeSourceFactory::new_compiled(key.node_id, timeout, binding.clone());
        let state = Arc::new(runtime_state());
        let fragment = Arc::new(FragmentContext::new(
            None,
            Arc::clone(&state),
            Some((key.finst_id_hi, key.finst_id_lo)),
            None,
            None,
            None,
        ));
        let completion = FragmentCompletion::new(5);
        let rows = Arc::new(AtomicUsize::new(0));
        let mut observables = Vec::new();
        let mut tasks = Vec::new();
        for driver_id in 0..5 {
            let mut source = factory.create(5, driver_id);
            source.prepare().expect("prepare exchange source");
            source
                .bind_runtime_state(&state)
                .expect("bind exchange source runtime");
            observables.push(
                source
                    .as_processor_ref()
                    .and_then(ProcessorOperator::source_observable)
                    .expect("source observable"),
            );
            let driver = PipelineDriver::new(
                driver_id,
                vec![
                    source,
                    Box::new(CountingSink {
                        rows: Arc::clone(&rows),
                        finishing: false,
                    }),
                ],
                None,
                Vec::new(),
                Arc::clone(&state),
                fragment.fragment_instance_id(),
            );
            tasks.push(DriverTask::new(
                driver,
                Arc::clone(&completion),
                Arc::clone(&fragment),
                Duration::from_millis(5),
            ));
        }

        let executor = GlobalDriverExecutor::new(2);
        executor.submit(tasks);
        let park_deadline = Instant::now() + Duration::from_secs(1);
        while observables
            .iter()
            .any(|observable| observable.num_observers() == 0)
            && Instant::now() < park_deadline
        {
            std::thread::yield_now();
        }
        assert!(
            observables
                .iter()
                .all(|observable| observable.num_observers() > 0),
            "every high-DOP exchange driver must be parked before EOS"
        );

        for sender in 0..3 {
            binding.receiver_port.push_local(
                receiver_key(key),
                100 + sender,
                sender,
                Vec::new(),
                true,
            );
        }

        completion
            .wait_timeout(
                Duration::from_secs(1),
                "shared exchange EOS did not finish every parked driver".to_string(),
            )
            .expect("all exchange drivers finish after the third sender EOS");
        assert_eq!(rows.load(Ordering::Acquire), 0);
        binding
            .receiver_port
            .cancel_fragment(UniqueId::new(key.finst_id_hi, key.finst_id_lo));
    }

    #[test]
    fn compiled_factory_leaves_receiver_registration_to_the_fragment_resource_lease() {
        // Receiver registration is acquired exactly once by FragmentResources during
        // prepare. The factory only consumes that registration through its binding.
        let key = exchange::ExchangeKey {
            finst_id_hi: 77_001,
            finst_id_lo: 88_002,
            node_id: 5,
        };
        let binding = ExchangeBinding {
            key,
            expected_senders: 4,
            receiver_port: in_process_test_exchange_receiver_port(),
        };
        let factory = ExchangeSourceFactory::new_compiled(5, Duration::from_secs(60), binding);
        assert!(
            binding_receiver_port(&factory)
                .snapshot(receiver_key(key))
                .is_none(),
            "factory must not create a second exchange receiver registration"
        );
    }

    fn binding_receiver_port(factory: &ExchangeSourceFactory) -> &Arc<dyn ExchangeReceiverPort> {
        &factory.binding.receiver_port
    }
}
