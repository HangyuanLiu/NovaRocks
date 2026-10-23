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
    InternalResultCpu, InternalResultCpuJob, InternalResultValue,
};
use crate::task_execution::status_intake::NotifyWake;

type Job = InternalResultCpuJob<Result<InternalDomainSlot, InternalDomainFailure>>;

struct State {
    value: Option<InternalResultValue<InternalDomainSlot>>,
    job: Option<Job>,
    busy: bool,
    closing: bool,
    receipt: Option<Result<(), QueryExecutionError>>,
    domain_failure: Option<InternalDomainFailure>,
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

enum Operation {
    Body(Arc<RetainedRootReply>),
    End(RootResultEnd),
    FinishStatistics,
    AcceptWrite(crate::query_execution::write_session::WriteReceivingPort),
}

impl Operation {
    fn apply(self, slot: InternalDomainSlot) -> Result<InternalDomainSlot, InternalDomainFailure> {
        match self {
            Self::Body(reply) => slot.apply_body(reply),
            Self::End(end) => slot.finish_eof(end),
            Self::FinishStatistics => slot.finish_statistics(),
            Self::AcceptWrite(receiving) => slot.accept_write(receiving),
        }
    }
}

impl DomainCpuCustody {
    pub(crate) fn try_new(
        domain: InternalDomainState,
        binding: QueryResultCapacityBinding,
        cpu: InternalResultCpu,
        runtime: FrontendDataRuntime,
        cancellation: QueryCancellationView,
    ) -> Result<Arc<Self>, QueryExecutionError> {
        let value =
            InternalResultValue::try_produce(binding.scope(), binding.window_alias(), |_| {
                Ok(InternalDomainSlot::Collecting {
                    state: domain,
                    receipt: None,
                })
            })
            .map_err(|_| domain_error("internal decoder has no complete admitted capacity"))?;
        Ok(Arc::new(Self {
            state: Mutex::new(State {
                value: Some(value),
                job: None,
                busy: false,
                closing: false,
                receipt: None,
                domain_failure: None,
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
        let input = {
            let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
            if state.closing || state.busy || state.job.is_some() {
                return Err(domain_error(
                    "internal decoder already has an active or closed CPU job",
                ));
            }
            let input = state
                .value
                .take()
                .ok_or_else(|| domain_error("internal decoder has no serial input"))?;
            state.busy = true;
            state.receipt = None;
            input
        };
        let input = match input.track_actual_exit(&self.binding) {
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
        let job = self.cpu.submit(
            input,
            self.cancellation.clone(),
            &self.runtime,
            Arc::new(NotifyWake::new(Arc::new(tokio::sync::Notify::new()))),
            move |slot, _| operation.apply(slot),
        );
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
        state.job.take();
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

    pub(crate) fn begin_close(&self) {
        let retired = {
            let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
            state.closing = true;
            state.value.take()
        };
        self.receipt_wake.wake();
        drop(retired);
    }

    pub(crate) async fn join_closed(&self) -> Result<(), Arc<tokio::task::JoinError>> {
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
                return Poll::Ready(Ok(()));
            };
            let retired = job.close_delivery();
            let result = match job.poll_forwarder_closed(cx) {
                Poll::Ready(Ok(())) => {
                    state.job.take();
                    Poll::Ready(Ok(()))
                }
                Poll::Ready(Err(_)) => {
                    Poll::Ready(Err(job.join_failure().expect("original joined failure")))
                }
                Poll::Pending => Poll::Pending,
            };
            drop(state);
            // Complete physical destruction is part of this close turn, but
            // never runs while the serial domain custody lock is held.
            drop(retired);
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
