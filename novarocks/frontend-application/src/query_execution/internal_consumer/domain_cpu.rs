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

//! The fixed domain consumer's stable custody of its one serial CPU job.

use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};

use novarocks_execution_contract::{
    SafeDetail, failure::FailureProjection, root_result::RootResultEnd,
};
use novarocks_query_application::{
    admitted_query_context::QueryResultCapacityBinding,
    api::{QueryExecutionError, RetainedRootReply},
    cancellation::QueryCancellationView,
    execution_error::{ExecutionFailureInput, ExecutionFailureSite, QueryFailureReason},
};

use super::domain_state::{InternalDomainFailure, InternalDomainSlot, InternalDomainState};
use crate::native::data_runtime::FrontendDataRuntime;
use crate::query_execution::internal_result_cpu::{
    InternalResultCpu, InternalResultCpuJob, InternalResultOperation as Operation,
    InternalResultValue,
};
use crate::task_execution::status_intake::NotifyWake;

type Job = InternalResultCpuJob<Result<InternalDomainSlot, InternalDomainFailure>>;

struct State {
    value: Option<InternalResultValue<InternalDomainSlot>>,
    computation: Option<novarocks_workload_control::joint_admission::InternalComputationGrant>,
    job: Option<Job>,
    busy: bool,
    closing: bool,
    receipt: Option<Result<(), QueryExecutionError>>,
    domain_failure: Option<InternalDomainFailure>,
    cpu_admission_failure: Option<novarocks_workload_control::WorkError>,
}

/// This object belongs to the registered consumer/statement holder, outside
/// every destructible receipt waiter. Dropping a waiter cannot lose its join,
/// decoded payload, original input allowance, or an early-cancelled job.
pub(crate) struct DomainCpuCustody {
    state: Mutex<State>,
    binding: QueryResultCapacityBinding,
    cpu: InternalResultCpu,
    runtime: FrontendDataRuntime,
    cancellation: QueryCancellationView,
    registration: futures::task::AtomicWaker,
    receipt_wake: futures::task::AtomicWaker,
}

impl DomainCpuCustody {
    pub(crate) fn try_new(
        domain: InternalDomainState,
        binding: QueryResultCapacityBinding,
        cpu: InternalResultCpu,
        runtime: FrontendDataRuntime,
        cancellation: QueryCancellationView,
    ) -> Result<Arc<Self>, QueryExecutionError> {
        Self::try_new_stage(domain, binding, cpu, runtime, cancellation, None)
    }

    pub(crate) fn try_new_with_computation(
        domain: InternalDomainState,
        binding: QueryResultCapacityBinding,
        cpu: InternalResultCpu,
        runtime: FrontendDataRuntime,
        cancellation: QueryCancellationView,
        computation: novarocks_workload_control::joint_admission::InternalComputationGrant,
    ) -> Result<Arc<Self>, QueryExecutionError> {
        Self::try_new_stage(
            domain,
            binding,
            cpu,
            runtime,
            cancellation,
            Some(computation),
        )
    }

    fn try_new_stage(
        domain: InternalDomainState,
        binding: QueryResultCapacityBinding,
        cpu: InternalResultCpu,
        runtime: FrontendDataRuntime,
        cancellation: QueryCancellationView,
        computation: Option<novarocks_workload_control::joint_admission::InternalComputationGrant>,
    ) -> Result<Arc<Self>, QueryExecutionError> {
        if binding.is_root_backed_internal() != computation.is_some() {
            return Err(domain_error(
                "Root payload requires its exact independent stage computation",
            ));
        }
        if let Some(grant) = &computation {
            let alias = grant
                .retain_alias()
                .map_err(|_| domain_error("stage computation pin is unavailable"))?;
            if !alias.is_for_scope(binding.scope()) {
                return Err(domain_error(
                    "stage computation belongs to a foreign execution",
                ));
            }
        }
        let window = binding
            .try_window_alias()
            .map_err(|_| domain_error("original payload pin is unavailable"))?;
        let value = InternalResultValue::try_produce(binding.scope(), window, |_| {
            Ok(InternalDomainSlot::Collecting {
                state: Box::new(domain),
                receipt: None,
            })
        })
        .map_err(|_| domain_error("internal decoder has no complete admitted capacity"))?;
        Ok(Arc::new(Self {
            state: Mutex::new(State {
                value: Some(value),
                computation,
                job: None,
                busy: false,
                closing: false,
                receipt: None,
                domain_failure: None,
                cpu_admission_failure: None,
            }),
            binding,
            cpu,
            runtime,
            cancellation,
            registration: futures::task::AtomicWaker::new(),
            receipt_wake: futures::task::AtomicWaker::new(),
        }))
    }

    pub(crate) async fn apply_body(
        &self,
        reply: Arc<RetainedRootReply>,
    ) -> Result<(), QueryExecutionError> {
        self.start(Operation::Body(reply))?;
        self.receipt().await
    }

    pub(crate) async fn finish_eof(&self, end: RootResultEnd) -> Result<(), QueryExecutionError> {
        self.start(Operation::End(end))?;
        self.receipt().await
    }

    pub(crate) async fn finish_statistics(&self) -> Result<(), QueryExecutionError> {
        self.start(Operation::FinishStatistics)?;
        self.receipt().await
    }

    pub(crate) async fn accept_write(
        &self,
        receiving: crate::query_execution::write_session::WriteReceivingPort,
    ) -> Result<(), QueryExecutionError> {
        self.start(Operation::AcceptWrite(receiving))?;
        self.receipt().await
    }

    fn start(&self, operation: Operation) -> Result<(), QueryExecutionError> {
        let (input, computation, source) = {
            let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
            if state.closing || state.busy || state.job.is_some() {
                return Err(domain_error(
                    "internal decoder already has an active or closed CPU job",
                ));
            }
            // Checked retain is a callback-free original State transaction.
            // The stable grant remains here until the complete receiving cut.
            let source = match state
                .computation
                .as_ref()
                .map(|grant| grant.cpu_action_source(self.binding.scope()))
                .transpose()
            {
                Ok(source) => source,
                Err(error) => {
                    let projected =
                        QueryExecutionError::from_execution(ExecutionFailureInput::new(
                            error.failure_fact(),
                            SafeDetail::truncating("stage CPU action source is unavailable"),
                            ExecutionFailureSite::Coordinator,
                        ));
                    state.cpu_admission_failure = Some(error);
                    return Err(projected);
                }
            };
            let computation = state
                .computation
                .as_ref()
                .map(|grant| grant.retain_alias())
                .transpose()
                .map_err(|_| domain_error("stage computation pin count overflow"))?;
            let input = state
                .value
                .take()
                .ok_or_else(|| domain_error("internal decoder has no serial input"))?;
            state.busy = true;
            state.receipt = None;
            (input, computation, source)
        };
        let tracked = match computation {
            Some(computation) => input.track_computation_exit(&self.binding, computation),
            None => input.track_actual_exit(&self.binding),
        };
        let input = match tracked {
            Ok(input) => input,
            Err(_) => {
                let error =
                    domain_error("internal decoder could not register its actual CPU input");
                let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
                state.busy = false;
                state.receipt = Some(Err(error.clone()));
                drop(state);
                self.registration.wake();
                return Err(error);
            }
        };
        let wake = Arc::new(NotifyWake::new(Arc::new(tokio::sync::Notify::new())));
        let job = match source {
            Some(source) => self.cpu.submit_root_operation(
                input,
                source,
                self.cancellation.clone(),
                &self.runtime,
                wake,
                operation,
            ),
            // Explicit old non-Root path. It grants no joint/finite-source
            // qualification; all Root-backed domains above require the source.
            None => self.cpu.submit_operation(
                input,
                self.cancellation.clone(),
                &self.runtime,
                wake,
                operation,
            ),
        };
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        state.job = Some(job);
        // A concurrent close sees busy and retains this exact job. Receipt
        // polling itself uses the original oneshot waker, never a timed poll.
        state.busy = false;
        drop(state);
        self.registration.wake();
        Ok(())
    }

    pub(crate) async fn receipt(&self) -> Result<(), QueryExecutionError> {
        std::future::poll_fn(|cx| self.poll_receipt(cx)).await
    }

    fn poll_receipt(&self, cx: &mut Context<'_>) -> Poll<Result<(), QueryExecutionError>> {
        // Receiver::close need not wake a previous oneshot receiver poll.
        // Closing this custody explicitly wakes its original receipt waiter.
        self.receipt_wake.register(cx.waker());
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        if state.closing {
            return Poll::Ready(Err(domain_error(
                "internal decoder receipt owner is closing",
            )));
        }
        if state.receipt.is_none() {
            let Some(job) = state.job.as_mut() else {
                return Poll::Ready(Err(domain_error(
                    "internal decoder has no dispatched CPU job",
                )));
            };
            let result = std::task::ready!(job.poll_take(cx));
            match result {
                Ok(output) => match output.try_transform(|slot, _| slot) {
                    Ok(output) => {
                        state.value = Some(output);
                        state.receipt = Some(Ok(()));
                    }
                    Err(error) => {
                        state.receipt = Some(Err(project_domain_failure(&error)));
                        state.domain_failure = Some(error);
                    }
                },
                Err(error) => state.receipt = Some(Err(QueryExecutionError::from_execution(error))),
            }
        }
        if let Some(Err(error)) = &state.receipt {
            // Early cancellation is an outcome, not actual exit. The original
            // job stays here until the independent close observer joins it.
            return Poll::Ready(Err(error.clone()));
        }
        let Some(job) = state.job.as_mut() else {
            return Poll::Ready(Ok(()));
        };
        if std::task::ready!(job.poll_forwarder_closed(cx)).is_err() {
            let error = domain_error("internal decoder CPU forwarder did not exit normally");
            state.receipt = Some(Err(error.clone()));
            return Poll::Ready(Err(error));
        }
        let retired = state
            .job
            .as_mut()
            .and_then(|job| job.take_retired_outcome());
        let original_job = state.job.take();
        drop(state);
        drop(retired);
        drop(original_job);
        Poll::Ready(Ok(()))
    }

    /// Only a checked CPU result can leave this holder. The closed factory
    /// then embeds it in its concrete receiving holder before proposing Ready.
    pub(crate) fn take(
        &self,
    ) -> Result<InternalResultValue<InternalDomainSlot>, QueryExecutionError> {
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        if state.closing
            || state.busy
            || state.job.is_some()
            || !matches!(state.receipt, Some(Ok(())))
        {
            return Err(domain_error(
                "internal decoder output is not completely received",
            ));
        }
        state
            .value
            .take()
            .ok_or_else(|| domain_error("internal decoder output was already transferred"))
    }

    pub(crate) fn finish_computation_after_handoff(&self) -> Result<(), QueryExecutionError> {
        let computation = {
            let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
            if state.closing
                || state.busy
                || state.job.is_some()
                || state.value.is_some()
                || !matches!(state.receipt, Some(Ok(())))
            {
                return Err(domain_error(
                    "stage computation has not reached its actual receiving cut",
                ));
            }
            state.computation.take()
        };
        drop(computation);
        Ok(())
    }

    pub(crate) fn begin_close(&self) {
        let retired = {
            let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
            state.closing = true;
            state.value.take()
        };
        self.receipt_wake.wake();
        drop(retired);
    }

    pub(crate) fn health_failure(&self) -> Option<Arc<dyn std::error::Error + Send + Sync>> {
        let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        state.job.as_ref().and_then(|job| job.health_failure())
    }

    pub(crate) async fn join_closed(&self) -> Result<(), Arc<dyn std::error::Error + Send + Sync>> {
        self.begin_close();
        std::future::poll_fn(|cx| {
            self.registration.register(cx.waker());
            let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
            if state.busy {
                // start() is synchronous and cannot suspend. Its caller must
                // finish registration before handing off this stable holder.
                return Poll::Pending;
            }
            let Some(job) = state.job.as_mut() else {
                let computation = state.computation.take();
                drop(state);
                drop(computation);
                return Poll::Ready(Ok(()));
            };
            let retired = job.close_delivery();
            let result = match job.poll_forwarder_closed(cx) {
                Poll::Ready(Ok(())) => Poll::Ready(Ok(())),
                Poll::Ready(Err(original)) => Poll::Ready(Err(original)),
                Poll::Pending => Poll::Pending,
            };
            let (original_job, late) = if matches!(result, Poll::Ready(Ok(()))) {
                let late = state
                    .job
                    .as_mut()
                    .and_then(|job| job.take_retired_outcome());
                (state.job.take(), late)
            } else {
                (None, None)
            };
            let computation = if matches!(result, Poll::Ready(Ok(()))) {
                state.computation.take()
            } else {
                None
            };
            drop(state);
            // Raw failures stay in the same failed job. Ordinary late outcomes
            // retire only after its real join, outside serial custody locks.
            drop(retired);
            drop(late);
            drop(original_job);
            drop(computation);
            result
        })
        .await
    }
}

pub(crate) fn domain_error(detail: &'static str) -> QueryExecutionError {
    QueryExecutionError::from_execution(ExecutionFailureInput::new(
        QueryFailureReason::PreparationProtocol.failure_fact(),
        SafeDetail::truncating(detail),
        ExecutionFailureSite::Coordinator,
    ))
}

fn project_domain_failure(error: &InternalDomainFailure) -> QueryExecutionError {
    match error.execution_input() {
        Some(input) => QueryExecutionError::from_execution(input),
        None => domain_error("internal domain decoder rejected its input"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query_execution::{
        internal_result_cpu::{InternalResultCpuOwner, admitted_internal_fixture},
        statistics::StatisticsRootResultDecoder,
    };
    use novarocks_query_application::cancellation::QueryCancellationSource;
    use std::{
        future::Future,
        num::NonZeroU64,
        time::{Duration, Instant},
    };

    fn end() -> RootResultEnd {
        RootResultEnd {
            sequence: NonZeroU64::new(1).unwrap(),
            output_rows: 0,
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn statistics_retains_distinct_eof_and_success_receipts_in_stable_custody() {
        let (_control, root, binding, capacity) = admitted_internal_fixture();
        let mut owner = InternalResultCpuOwner::try_new_for_test().unwrap();
        let cancellation = QueryCancellationSource::new();
        let custody = DomainCpuCustody::try_new(
            InternalDomainState::Statistics(StatisticsRootResultDecoder::for_test_with_capacity(
                [],
                &binding,
            )),
            binding.clone(),
            owner.runtime(),
            FrontendDataRuntime::new(tokio::runtime::Handle::current()),
            cancellation.view(),
        )
        .unwrap();
        custody.finish_eof(end()).await.unwrap();
        {
            let state = custody.state.lock().unwrap();
            let value = state.value.as_ref().unwrap().value();
            assert_eq!(value.end(), Some(end()));
            assert!(!value.statistics_finished());
        }
        custody.finish_statistics().await.unwrap();
        let output = custody.take().unwrap();
        assert!(custody.take().is_err());
        assert!(
            output
                .hand_off(|slot, _| slot.into_statistics())
                .unwrap()
                .is_empty()
        );
        custody.join_closed().await.unwrap();
        drop(custody);
        drop(binding);
        root.owner.complete();
        root.business.release();
        assert_eq!(capacity.snapshot().held_positions, [0; 4]);
        owner
            .shutdown_until(Instant::now() + Duration::from_secs(5))
            .await
            .unwrap();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn dropping_receipt_waiter_keeps_original_job_until_real_cpu_exit() {
        let (_control, root, binding, capacity) = admitted_internal_fixture();
        let mut owner = InternalResultCpuOwner::try_new_for_test().unwrap();
        let runtime = FrontendDataRuntime::new(tokio::runtime::Handle::current());
        let cancellation = QueryCancellationSource::new();
        let mut blockers = Vec::new();
        let mut releases = Vec::new();
        let mut entered = Vec::new();
        // Occupy the actual finite worker set so EOF is genuinely queued.
        let workers = novarocks_workload_control::ResultCapacityConfig::V1.positions[2];
        for _ in 0..workers {
            let (release, gate) = std::sync::mpsc::channel();
            let (started, start) = tokio::sync::oneshot::channel();
            let input =
                InternalResultValue::try_produce(binding.scope(), binding.window_alias(), |_| {
                    Ok(())
                })
                .unwrap()
                .track_actual_exit(&binding)
                .unwrap();
            blockers.push(owner.runtime().submit(
                input,
                cancellation.view(),
                &runtime,
                Arc::new(NotifyWake::new(Arc::new(tokio::sync::Notify::new()))),
                move |(), _| {
                    let _ = started.send(());
                    gate.recv_timeout(Duration::from_secs(10))
                        .expect("release CPU gate watchdog");
                },
            ));
            releases.push(release);
            entered.push(start);
        }
        for entered in entered {
            tokio::time::timeout(Duration::from_secs(5), entered)
                .await
                .unwrap()
                .unwrap();
        }
        let custody = DomainCpuCustody::try_new(
            InternalDomainState::Statistics(StatisticsRootResultDecoder::for_test_with_capacity(
                [],
                &binding,
            )),
            binding.clone(),
            owner.runtime(),
            runtime,
            cancellation.view(),
        )
        .unwrap();
        custody.start(Operation::End(end())).unwrap();
        {
            let receipt = custody.receipt();
            tokio::pin!(receipt);
            std::future::poll_fn(|cx| {
                assert!(receipt.as_mut().poll(cx).is_pending());
                Poll::Ready(())
            })
            .await;
        }
        {
            struct ReceiptWake(std::sync::atomic::AtomicBool);
            impl std::task::Wake for ReceiptWake {
                fn wake(self: Arc<Self>) {
                    self.wake_by_ref();
                }
                fn wake_by_ref(self: &Arc<Self>) {
                    self.0.store(true, std::sync::atomic::Ordering::Release);
                }
            }
            let receipt_wake = Arc::new(ReceiptWake(std::sync::atomic::AtomicBool::new(false)));
            let waker = std::task::Waker::from(Arc::clone(&receipt_wake));
            let mut receipt_context = Context::from_waker(&waker);
            let receipt = custody.receipt();
            tokio::pin!(receipt);
            assert!(receipt.as_mut().poll(&mut receipt_context).is_pending());
            let drain = custody.join_closed();
            tokio::pin!(drain);
            std::future::poll_fn(|cx| {
                assert!(drain.as_mut().poll(cx).is_pending());
                Poll::Ready(())
            })
            .await;
            assert!(receipt_wake.0.load(std::sync::atomic::Ordering::Acquire));
            assert!(matches!(
                receipt.as_mut().poll(&mut receipt_context),
                Poll::Ready(Err(_))
            ));
            assert!(custody.state.lock().unwrap().job.is_some());
            assert_eq!(capacity.snapshot().held_positions, [0, 0, 1, 0]);
            for release in releases {
                release.send(()).unwrap();
            }
            tokio::time::timeout(Duration::from_secs(5), drain)
                .await
                .unwrap()
                .unwrap();
        }
        for mut job in blockers {
            job.join_closed().await.unwrap();
        }
        binding.result_activities_exited().await;
        drop(custody);
        drop(binding);
        root.owner.complete();
        root.business.release();
        assert_eq!(capacity.snapshot().held_positions, [0; 4]);
        owner
            .shutdown_until(Instant::now() + Duration::from_secs(5))
            .await
            .unwrap();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn close_destroys_unclaimed_original_payload_outside_the_custody_lock() {
        use novarocks_spi::connector::{ConnectorPayloadRetentionGuard, StatisticsArtifactDraft};
        use std::sync::atomic::{AtomicBool, Ordering};
        struct DropProbe {
            custody: std::sync::Weak<DomainCpuCustody>,
            dropped: Arc<AtomicBool>,
        }
        impl Drop for DropProbe {
            fn drop(&mut self) {
                let custody = self.custody.upgrade().expect("stable receiving owner");
                assert!(
                    custody.state.try_lock().is_ok(),
                    "payload destroyed under custody lock"
                );
                self.dropped.store(true, Ordering::Release);
            }
        }
        let (_control, root, binding, capacity) = admitted_internal_fixture();
        let mut owner = InternalResultCpuOwner::try_new_for_test().unwrap();
        let runtime = FrontendDataRuntime::new(tokio::runtime::Handle::current());
        let cancellation = QueryCancellationSource::new();
        let custody = DomainCpuCustody::try_new(
            InternalDomainState::Statistics(StatisticsRootResultDecoder::for_test_with_capacity(
                [],
                &binding,
            )),
            binding.clone(),
            owner.runtime(),
            runtime.clone(),
            cancellation.view(),
        )
        .unwrap();
        let input = custody
            .state
            .lock()
            .unwrap()
            .value
            .take()
            .unwrap()
            .track_actual_exit(&binding)
            .unwrap();
        let dropped = Arc::new(AtomicBool::new(false));
        let probe = DropProbe {
            custody: Arc::downgrade(&custody),
            dropped: Arc::clone(&dropped),
        };
        let completed = Arc::new(tokio::sync::Notify::new());
        let job = owner.runtime().submit(
            input,
            cancellation.view(),
            &runtime,
            Arc::new(NotifyWake::new(Arc::clone(&completed))),
            move |_original, window| {
                let artifact = StatisticsArtifactDraft::try_new_with_guard(
                    vec![1],
                    "apache-datasketches-theta-v1",
                    bytes::Bytes::from_static(b"original artifact"),
                    Default::default(),
                    ConnectorPayloadRetentionGuard::new((window.clone(), probe)),
                )
                .unwrap();
                Ok(InternalDomainSlot::StatisticsFinished(vec![artifact]))
            },
        );
        custody.state.lock().unwrap().job = Some(job);
        tokio::time::timeout(Duration::from_secs(5), completed.notified())
            .await
            .unwrap();
        assert!(!dropped.load(Ordering::Acquire));
        custody.join_closed().await.unwrap();
        assert!(dropped.load(Ordering::Acquire));
        drop(custody);
        drop(binding);
        root.owner.complete();
        root.business.release();
        assert_eq!(capacity.snapshot().held_positions, [0; 4]);
        owner
            .shutdown_until(Instant::now() + Duration::from_secs(5))
            .await
            .unwrap();
    }
}

#[cfg(test)]
mod root_action_tests {
    use super::*;
    use crate::query_execution::internal_result_cpu::{
        INTERNAL_PEAK_BYTES, InternalResultCpuOwner,
    };
    use crate::query_execution::statistics::StatisticsRootResultDecoder;
    use novarocks_query_application::cancellation::{
        QueryCancellationReason, QueryCancellationSource,
    };
    use novarocks_resource_contract::SharedDependency;
    use novarocks_workload_control::joint_admission::*;
    use novarocks_workload_control::{
        ControlStorageConfig, WorkClass, WorkOwner, WorkRequest, WorkScope, WorkloadConfig,
        WorkloadControl,
    };
    use std::{
        future::Future,
        num::NonZeroU64,
        pin::Pin,
        task::Waker,
        time::{Duration, Instant},
    };
    fn root_control(
        cpu: &InternalResultCpu,
    ) -> (WorkloadControl, JointAdmissionHandle, JointProgressDriver) {
        let lane = JointLane::Foreground;
        let cpu_jobs = cpu.configured_capacity().worker_threads().get() as u64;
        assert!(cpu.configured_capacity().queue_capacity().get() >= cpu_jobs as usize);
        let quote = cpu.operation_action_quote().unwrap();
        let count = 8;
        let storage = ControlStorageConfig::new([count, 0, 0, 0, 0], 1, 2, &[]);
        let mut config = JointCapacityConfig {
            global: JointResourceLimits::default(),
            classes: [JointResourceLimits::default(); 5],
            lanes: [JointResourceLimits::default(); 3],
            class_lanes: [[JointResourceLimits::default(); 3]; 5],
            max_dependencies_per_claim: 2,
            max_registered_dependencies: 266,
            max_claims: 16,
            max_requests: 16,
            max_changes_per_request: 2,
            max_snapshot_bytes: 16 * 1024 * 1024,
            max_scratch_bytes: 16 * 1024 * 1024,
            max_residual_arcs: 10000,
            max_total_proof_work: 10000000,
            work_per_turn: std::num::NonZeroUsize::new(7).unwrap(),
        };
        let base = storage.base_charge(&config).unwrap();
        let m = base.get(JointResource::MetadataBytes);
        let custody = JointResourceLimits {
            global: JointResources::ZERO
                .with(JointResource::ScopeRecords, 8)
                .with(JointResource::ClaimRecords, 12)
                .with(JointResource::MetadataBytes, m * 16)
                .with(
                    JointResource::PayloadBytes,
                    crate::query_execution::internal_result_cpu::INTERNAL_PEAK_BYTES * 2,
                )
                .with(JointResource::UnreceivedProductions, 1),
            per_dependency: JointResources::ZERO
                .with(JointResource::ScopeRecords, 7)
                .with(JointResource::ClaimRecords, 12)
                .with(JointResource::MetadataBytes, m * 8)
                .with(
                    JointResource::PayloadBytes,
                    crate::query_execution::internal_result_cpu::INTERNAL_PEAK_BYTES * 2,
                )
                .with(JointResource::UnreceivedProductions, 1),
            protected_progress: base,
        };
        let foreground = JointResourceLimits {
            global: JointResources::ZERO
                .with(JointResource::ClaimRecords, 4)
                .with(JointResource::MetadataBytes, m * 16)
                .with(JointResource::PayloadBytes, 32)
                .with(JointResource::Computations, 1)
                .with(JointResource::InternalCpuJobs, cpu_jobs)
                .with(JointResource::PhysicalQueueRecords, cpu_jobs)
                .with(
                    JointResource::PhysicalQueueBytes,
                    cpu_jobs * quote.physical_queue_bytes.get(),
                ),
            per_dependency: JointResources::ZERO
                .with(JointResource::ClaimRecords, 4)
                .with(JointResource::MetadataBytes, m * 8)
                .with(JointResource::PayloadBytes, 32)
                .with(JointResource::Computations, 1)
                .with(JointResource::InternalCpuJobs, cpu_jobs)
                .with(JointResource::PhysicalQueueRecords, cpu_jobs)
                .with(
                    JointResource::PhysicalQueueBytes,
                    cpu_jobs * quote.physical_queue_bytes.get(),
                ),
            protected_progress: JointResources::ZERO,
        };
        let global = JointResourceLimits {
            // Explicit sums preserve these two real fixture lane vectors;
            // JointResources arithmetic is intentionally private to WC.
            global: JointResources::ZERO
                .with(JointResource::ScopeRecords, 8)
                .with(JointResource::ClaimRecords, 16)
                .with(JointResource::MetadataBytes, m.checked_mul(32).unwrap())
                .with(
                    JointResource::PayloadBytes,
                    INTERNAL_PEAK_BYTES
                        .checked_mul(2)
                        .unwrap()
                        .checked_add(32)
                        .unwrap(),
                )
                .with(JointResource::Computations, 1)
                .with(JointResource::UnreceivedProductions, 1)
                .with(JointResource::InternalCpuJobs, cpu_jobs)
                .with(JointResource::PhysicalQueueRecords, cpu_jobs)
                .with(
                    JointResource::PhysicalQueueBytes,
                    cpu_jobs
                        .checked_mul(quote.physical_queue_bytes.get())
                        .unwrap(),
                ),
            per_dependency: JointResources::ZERO
                .with(JointResource::ScopeRecords, 7)
                .with(JointResource::ClaimRecords, 16)
                .with(JointResource::MetadataBytes, m.checked_mul(16).unwrap())
                .with(
                    JointResource::PayloadBytes,
                    INTERNAL_PEAK_BYTES
                        .checked_mul(2)
                        .unwrap()
                        .checked_add(32)
                        .unwrap(),
                )
                .with(JointResource::Computations, 1)
                .with(JointResource::UnreceivedProductions, 1)
                .with(JointResource::InternalCpuJobs, cpu_jobs)
                .with(JointResource::PhysicalQueueRecords, cpu_jobs)
                .with(
                    JointResource::PhysicalQueueBytes,
                    cpu_jobs
                        .checked_mul(quote.physical_queue_bytes.get())
                        .unwrap(),
                ),
            protected_progress: base,
        };
        config.global = global;
        config.classes[0] = global;
        config.lanes[0] = custody;
        let lane_index = match lane {
            JointLane::Foreground => 1,
            JointLane::Settlement => 2,
            JointLane::Custody => panic!("CPU fixture requires an execution lane"),
        };
        config.lanes[lane_index] = foreground;
        config.class_lanes[0][0] = custody;
        config.class_lanes[0][lane_index] = foreground;
        // Only the original fixed substrate requires progress in this fixture.
        // CPU/complete Native action protection is deliberately not asserted.
        let mut required = RequiredJointProgress {
            global: base,
            classes: [JointResources::ZERO; 5],
            lanes: [JointResources::ZERO; 3],
            class_lanes: [[JointResources::ZERO; 3]; 5],
        };
        required.classes[0] = base;
        required.lanes[0] = base;
        required.class_lanes[0][0] = base;
        let control = WorkloadControl::try_new_counted(WorkloadConfig {
            root_limit: count,
            business_limit: count,
            query_concurrency_limit: count,
            scope_records_limit: count,
            ..WorkloadConfig::default()
        })
        .unwrap()
        .owner;
        let (handle, driver) = control
            .configure_joint_capacity(config.validate(&required).unwrap())
            .unwrap();
        control.configure_control_storage(storage).unwrap();
        control.mark_ready().unwrap();
        (control, handle, driver)
    }
    fn drive(driver: &mut JointProgressDriver) {
        for _ in 0..10000 {
            if matches!(driver.step(), JointProgress::Idle(_)) {
                return;
            }
        }
        panic!("original proof driver failed to reach idle");
    }
    fn receive(request: &mut JointAdmission) -> JointGrantedBatch {
        match Pin::new(request).poll(&mut Context::from_waker(Waker::noop())) {
            Poll::Ready(Ok(reply)) => reply,
            _ => panic!("expected the original paid reply"),
        }
    }
    fn bind(scope: &WorkScope, driver: &mut JointProgressDriver) {
        let mut proposal = scope.begin_control_binding().unwrap();
        proposal
            .push_dependency(&SharedDependency::LocalHost)
            .unwrap();
        proposal.end_change().unwrap();
        let mut request = proposal.submit().unwrap();
        drive(driver);
        let mut reply = receive(&mut request);
        reply
            .adjustment
            .as_mut()
            .unwrap()
            .install_control_scope_in(scope)
            .unwrap()
            .notify();
        drop(reply);
        drive(driver);
    }
    fn paid_claim(
        handle: &JointAdmissionHandle,
        owner: &WorkOwner,
        lane: JointLane,
        resource: JointResource,
        amount: u64,
        driver: &mut JointProgressDriver,
    ) -> JointClaim {
        let mut proposal = handle.begin_paid_proposal(owner).unwrap();
        proposal
            .begin_new(
                lane,
                JointResources::ZERO
                    .with(JointResource::ClaimRecords, 1)
                    .with(resource, amount),
            )
            .unwrap();
        proposal
            .push_dependency(&SharedDependency::LocalHost)
            .unwrap();
        proposal.end_change().unwrap();
        let mut request = proposal.submit().unwrap();
        drive(driver);
        let mut reply = receive(&mut request);
        reply.new_claims.pop().unwrap()
    }
    fn child(scope: &WorkScope, driver: &mut JointProgressDriver) -> WorkOwner {
        let mut request = scope.request_joint_child().unwrap();
        drive(driver);
        let mut reply = receive(&mut request);
        let mut target = None;
        reply
            .adjustment
            .as_mut()
            .unwrap()
            .install_child_in(scope, &mut target)
            .unwrap()
            .notify();
        drop(reply);
        drive(driver);
        target.unwrap()
    }
    async fn with_driver<F: Future>(
        control: &WorkloadControl,
        driver: &mut JointProgressDriver,
        future: F,
    ) -> F::Output {
        tokio::pin!(future);
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                match driver.step() {
                    JointProgress::More => tokio::task::yield_now().await,
                    JointProgress::Idle(revision) => tokio::select! {
                        biased;
                        result = &mut future => return result,
                        _ = control.wait_progress(revision) => {},
                    },
                }
            }
        })
        .await
        .unwrap()
    }
    #[tokio::test(flavor = "current_thread")]
    async fn root_eof_and_statistics_reuse_one_compute_through_the_real_joint_cpu_source() {
        root_case(false).await;
    }
    #[tokio::test(flavor = "current_thread")]
    async fn cancelled_root_wait_keeps_its_original_input_until_the_same_forwarder_joins() {
        root_case(true).await;
    }
    async fn root_case(cancel_before_proof: bool) {
        let mut cpu_owner = InternalResultCpuOwner::try_new_for_test().unwrap();
        let cpu = cpu_owner.runtime();
        let quote = cpu.operation_action_quote().unwrap();
        assert!(quote.metadata_bytes.get() > 0 && quote.physical_queue_bytes.get() > 0);
        let (control, handle, mut driver) = root_control(&cpu);
        let work = control
            .try_begin_root(WorkRequest::new(WorkClass::Query))
            .unwrap();
        let root_scope = work.owner.scope();
        bind(&root_scope, &mut driver);
        let mut raw = Some(paid_claim(
            &handle,
            &work.owner,
            JointLane::Custody,
            JointResource::PayloadBytes,
            INTERNAL_PEAK_BYTES,
            &mut driver,
        ));
        let mut payload = None;
        RetainedPayloadGrant::adopt_in(&root_scope, &mut raw, &mut payload)
            .unwrap()
            .notify();
        let stage = child(&root_scope, &mut driver);
        let stage_scope = stage.scope();
        let mut raw = Some(paid_claim(
            &handle,
            &stage,
            JointLane::Foreground,
            JointResource::Computations,
            1,
            &mut driver,
        ));
        let mut computation = None;
        InternalComputationGrant::adopt_in(&stage_scope, &mut raw, &mut computation)
            .unwrap()
            .notify();
        let binding = QueryResultCapacityBinding::from_internal_payload(
            &root_scope,
            &stage_scope,
            payload.as_ref().unwrap(),
        )
        .unwrap();
        let cancellation = QueryCancellationSource::new();
        let custody = DomainCpuCustody::try_new_with_computation(
            InternalDomainState::Statistics(StatisticsRootResultDecoder::for_test_with_capacity(
                [],
                &binding,
            )),
            binding.clone(),
            cpu,
            FrontendDataRuntime::new(tokio::runtime::Handle::current()),
            cancellation.view(),
            computation.unwrap(),
        )
        .unwrap();
        let end = RootResultEnd {
            sequence: NonZeroU64::new(1).unwrap(),
            output_rows: 0,
        };
        if cancel_before_proof {
            custody.start(Operation::End(end)).unwrap();
            let mut input_exit = Box::pin(binding.result_activities_exited());
            assert!(
                input_exit
                    .as_mut()
                    .poll(&mut Context::from_waker(Waker::noop()))
                    .is_pending()
            );
            cancellation.request(QueryCancellationReason::ExecutionCancellationRequested);
            with_driver(&control, &mut driver, custody.join_closed())
                .await
                .unwrap();
            tokio::time::timeout(Duration::from_secs(5), input_exit)
                .await
                .unwrap();
            assert!(custody.take().is_err());
        } else {
            with_driver(&control, &mut driver, custody.finish_eof(end))
                .await
                .unwrap();
            assert!(
                custody.state.lock().unwrap().job.is_none(),
                "the original forwarder joins before serial reuse"
            );
            with_driver(&control, &mut driver, custody.finish_statistics())
                .await
                .unwrap();
            assert!(custody.state.lock().unwrap().job.is_none());
            let output = custody.take().unwrap();
            assert!(
                output
                    .hand_off(|slot, _| slot.into_statistics())
                    .unwrap()
                    .is_empty()
            );
            custody.finish_computation_after_handoff().unwrap();
            with_driver(&control, &mut driver, custody.join_closed())
                .await
                .unwrap();
        }
        drop(custody);
        drop(binding);
        drop(payload);
        stage.complete();
        work.owner.complete();
        work.business.release();
        drive(&mut driver);
        cpu_owner
            .shutdown_until(Instant::now() + Duration::from_secs(5))
            .await
            .unwrap();
    }
}
