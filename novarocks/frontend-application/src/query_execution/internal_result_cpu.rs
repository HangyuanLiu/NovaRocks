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

//! The FE's finite execution lane for internal result codecs and collectors.
//! This is a separate process worker set from command execution: a command
//! may wait for domain progress while holding its own worker.

use std::num::NonZeroUsize;
use std::time::Instant;

use novarocks_execution_contract::{SafeDetail, failure::FailureProjection};
use novarocks_query_application::cpu::{
    QueryBlockingExecutor, QueryBlockingExecutorConfig, QueryBlockingExecutorOwner,
};
use novarocks_query_application::{
    cancellation::{QueryCancellationReason, QueryCancellationView},
    execution_error::{ExecutionFailureInput, ExecutionFailureSite, QueryFailureReason},
};
use novarocks_workload_control::{
    ResultCapacityConfig, ResultWindowAlias, ResultWindowClass, WorkScope,
};
use tokio::sync::oneshot;

use crate::native::data_runtime::FrontendDataRuntime;
use crate::task_execution::status_intake::StatusIntakeWake;
use std::sync::Arc;

// The whole maximum coexistence envelope is authorized before any domain
// decoder/collector construction. Nested stages reuse it, never acquire again.
pub(super) const INTERNAL_PEAK_BYTES: u64 = (2 * 336 + 256 + 32 + 32 + 8 + 8) * 1024 * 1024;

/// Coverage check for closed first-party factories which embed the supplied
/// alias into their actual payload backing before handing it off.
pub(crate) fn require_internal_result_capacity(
    scope: &WorkScope,
    window: &ResultWindowAlias,
) -> Result<(), String> {
    if !window.is_for_scope(scope) {
        return Err("internal result CPU window belongs to a foreign scope".into());
    }
    scope.check().map_err(|error| error.to_string())?;
    if window.class() != ResultWindowClass::Internal {
        return Err("internal result CPU requires an admitted Internal window".into());
    }
    window
        .check_backing_total(INTERNAL_PEAK_BYTES)
        .map_err(|error| error.to_string())?;
    Ok(())
}

/// Private payload retention for a closed Internal handoff. A write union
/// may reuse only this exact allowance and attribution, keeping one guard.
#[derive(Clone)]
pub(crate) struct InternalResultRetention {
    binding: novarocks_query_application::admitted_query_context::QueryResultCapacityBinding,
}
impl InternalResultRetention {
    pub(crate) fn try_new(
        binding: &novarocks_query_application::admitted_query_context::QueryResultCapacityBinding,
    ) -> Result<Self, String> {
        require_internal_result_capacity(binding.scope(), &binding.window_alias())?;
        Ok(Self {
            binding: binding.clone(),
        })
    }
    pub(crate) fn spi_guard(&self) -> novarocks_spi::connector::ConnectorPayloadRetentionGuard {
        novarocks_spi::connector::ConnectorPayloadRetentionGuard::new(self.binding.window_alias())
    }
    pub(crate) fn is_same_admission(&self, other: &Self) -> bool {
        let window = self.binding.window_alias();
        window.is_for_scope(other.binding.scope())
            && window.shares_capacity_with(&other.binding.window_alias())
    }
}
impl std::fmt::Debug for InternalResultRetention {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("InternalResultRetention")
    }
}

/// Frozen FE result capacity shared by the original window and CPU owners.
/// This validates declarations only; it grants no position or retained bytes.
#[derive(Clone, Copy, Debug)]
pub struct FrontendResultCapacityProfile {
    capacity: ResultCapacityConfig,
    internal_positions: NonZeroUsize,
}
impl FrontendResultCapacityProfile {
    pub fn try_new(
        capacity: ResultCapacityConfig,
    ) -> Result<Self, novarocks_workload_control::WorkError> {
        let capacity = capacity.validate()?;
        if capacity.all_objects_bytes[2] < INTERNAL_PEAK_BYTES {
            return Err(novarocks_workload_control::WorkError::InvalidConfig(
                "internal result capacity does not cover its supported peak",
            ));
        }
        let internal_positions = NonZeroUsize::new(capacity.positions[2]).ok_or(
            novarocks_workload_control::WorkError::InvalidConfig("internal result positions"),
        )?;
        Ok(Self {
            capacity,
            internal_positions,
        })
    }

    pub const fn declared(self) -> ResultCapacityConfig {
        self.capacity
    }

    #[cfg(test)]
    pub(crate) fn for_test() -> Self {
        Self::try_new(ResultCapacityConfig::V1).expect("test result capacity is supported")
    }
}

pub(crate) struct InternalResultCpuOwner {
    workers: QueryBlockingExecutorOwner,
    runtime: InternalResultCpu,
}

#[derive(Clone)]
pub(crate) struct InternalResultCpu {
    executor: QueryBlockingExecutor,
}

impl InternalResultCpuOwner {
    pub(crate) fn try_new(profile: FrontendResultCapacityProfile) -> Result<Self, String> {
        let positions = profile.internal_positions;
        let workers = QueryBlockingExecutorOwner::try_new(QueryBlockingExecutorConfig::new(
            positions, positions,
        ))?;
        let runtime = InternalResultCpu {
            executor: workers.executor(),
        };
        Ok(Self { workers, runtime })
    }

    #[cfg(test)]
    pub(crate) fn try_new_for_test() -> Result<Self, String> {
        Self::try_new(FrontendResultCapacityProfile::for_test())
    }

    pub(crate) fn runtime(&self) -> InternalResultCpu {
        self.runtime.clone()
    }

    pub(crate) async fn shutdown_until(&mut self, deadline: Instant) -> Result<(), String> {
        self.workers.shutdown_until(deadline).await
    }

    pub(crate) fn request_shutdown_for_process_exit(&self) {
        self.workers.request_shutdown_for_process_exit();
    }
}

/// Private first-party domain state, followed by its complete physical guard.
/// No bare graph extraction is provided. Actual domain output transfer must
/// explicitly carry the guard into its final payload owners.
pub(crate) struct InternalResultValue<T> {
    value: T,
    window: ResultWindowAlias,
    activity:
        Option<novarocks_query_application::admitted_query_context::ResultCapacityActivityLease>,
}
impl<T> InternalResultValue<T> {
    /// Check coverage before invoking a constructor that may grow the graph.
    pub(crate) fn try_produce(
        scope: &WorkScope,
        window: ResultWindowAlias,
        produce: impl FnOnce(&ResultWindowAlias) -> Result<T, String>,
    ) -> Result<Self, String> {
        require_internal_result_capacity(scope, &window)?;
        let value = produce(&window)?;
        Ok(Self {
            value,
            window,
            activity: None,
        })
    }

    pub(crate) fn track_actual_exit(
        mut self,
        binding: &novarocks_query_application::admitted_query_context::QueryResultCapacityBinding,
    ) -> Result<Self, String> {
        if self.activity.is_some()
            || !self.window.is_for_scope(binding.scope())
            || !self.window.shares_capacity_with(&binding.window_alias())
        {
            return Err("internal CPU activity differs from its input admission".into());
        }
        self.activity = Some(
            binding
                .begin_result_activity()
                .map_err(|error| error.to_string())?,
        );
        Ok(self)
    }

    pub(crate) fn value(&self) -> &T {
        &self.value
    }
    pub(crate) fn try_transform<R, E>(
        self,
        transform: impl FnOnce(T, &ResultWindowAlias) -> Result<R, E>,
    ) -> Result<InternalResultValue<R>, E> {
        let Self {
            value,
            window,
            activity,
        } = self;
        let value = transform(value, &window)?;
        Ok(InternalResultValue {
            value,
            window,
            activity,
        })
    }

    /// Final closed handoff only: the callback must return payloads whose
    /// actual backing was already guarded by the first-party domain factory.
    /// The full allowance remains held throughout the callback itself.
    pub(crate) fn hand_off<R>(self, handoff: impl FnOnce(T, &ResultWindowAlias) -> R) -> R {
        let Self {
            value,
            window,
            activity,
        } = self;
        let output = handoff(value, &window);
        drop(window);
        drop(activity);
        output
    }

    /// A closed application transformation keeps the same whole allowance.
    /// The closure must retain the supplied alias in any payload it hands to
    /// a provider or separately clonable backing before returning that graph.
    pub(crate) fn transform<R>(
        self,
        transform: impl FnOnce(T, &ResultWindowAlias) -> R,
    ) -> InternalResultValue<R> {
        let Self {
            value,
            window,
            activity,
        } = self;
        let value = transform(value, &window);
        InternalResultValue {
            value,
            window,
            activity,
        }
    }
}

fn cpu_execution_failure(detail: &'static str) -> ExecutionFailureInput {
    ExecutionFailureInput::new(
        QueryFailureReason::LogicalFailureUnspecified.failure_fact(),
        SafeDetail::truncating(detail),
        ExecutionFailureSite::Coordinator,
    )
}

fn cpu_cancellation_failure(reason: QueryCancellationReason) -> ExecutionFailureInput {
    let reason = match reason {
        QueryCancellationReason::DeadlineExceeded { .. }
        | QueryCancellationReason::FrontendDrainDeadlineExceeded { .. } => {
            QueryFailureReason::DeadlineExceeded
        }
        _ => QueryFailureReason::Cancelled,
    };
    ExecutionFailureInput::new(
        reason.failure_fact(),
        SafeDetail::truncating("internal result CPU was stopped by its statement control owner"),
        ExecutionFailureSite::Coordinator,
    )
}

/// At most one result-codec job per serial root consumer. Its forwarder owns
/// the bounded receipt. Closing its waiter never cancels a running CPU closure:
/// the same forwarder still awaits its receipt and retires unclaimed output.
/// An unclaimed output contains the same window until its actual destruction.
pub(crate) struct InternalResultCpuJob<T> {
    completion: oneshot::Receiver<Result<InternalResultValue<T>, ExecutionFailureInput>>,
    forwarder: Option<tokio::task::JoinHandle<()>>,
    closing: bool,
    claimed: bool,
    closed: Option<Result<(), Arc<tokio::task::JoinError>>>,
}
impl<T> InternalResultCpuJob<T> {
    /// Refuse further result delivery. Sender::closed wakes the same original
    /// forwarder, which continues awaiting the already-owned CPU receipt.
    pub(crate) fn begin_close(&mut self) {
        drop(self.close_delivery());
    }

    /// Move unclaimed output to the stable caller so its potentially large
    /// graph is destroyed outside the caller's custody lock. Returning this
    /// value is not actual exit; the original forwarder must still be joined.
    pub(crate) fn close_delivery(
        &mut self,
    ) -> Option<Result<InternalResultValue<T>, ExecutionFailureInput>> {
        self.closing = true;
        self.completion.close();
        self.completion.try_recv().ok()
    }

    pub(crate) fn poll_closed(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), &tokio::task::JoinError>> {
        self.begin_close();
        self.poll_forwarder_closed(cx)
    }

    /// Observe the original join without destroying delivery output. A stable
    /// caller closes delivery separately and retires its result outside locks.
    pub(crate) fn poll_forwarder_closed(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), &tokio::task::JoinError>> {
        use std::{future::Future, pin::Pin, task::Poll};
        if self.closed.is_none() {
            let result = std::task::ready!(
                Pin::new(
                    self.forwarder
                        .as_mut()
                        .expect("original CPU forwarder join")
                )
                .poll(cx)
            );
            self.closed = Some(result.map_err(Arc::new));
            self.forwarder.take();
        }
        Poll::Ready(
            self.closed
                .as_ref()
                .expect("CPU forwarder joined")
                .as_ref()
                .map(|_| ())
                .map_err(|error| error.as_ref()),
        )
    }

    /// Shares the same original join error with health custody. The task's
    /// result was moved into this Arc once; no failure is reconstructed.
    pub(crate) fn join_failure(&self) -> Option<Arc<tokio::task::JoinError>> {
        self.closed
            .as_ref()
            .and_then(|result| result.as_ref().err())
            .map(Arc::clone)
    }

    #[cfg(test)]
    pub(crate) async fn join_closed(&mut self) -> Result<(), &tokio::task::JoinError> {
        std::future::poll_fn(|cx| self.poll_closed(cx).map(|_| ())).await;
        self.closed
            .as_ref()
            .expect("CPU forwarder joined")
            .as_ref()
            .map(|_| ())
            .map_err(|error| error.as_ref())
    }

    pub(crate) fn try_take(
        &mut self,
    ) -> Option<Result<InternalResultValue<T>, ExecutionFailureInput>> {
        if self.closing || self.claimed {
            return None;
        }
        match self.completion.try_recv() {
            Ok(mut value) => {
                self.claimed = true;
                // A claimed receipt proves CPU execution has exited. The
                // serial consumer keeps the payload/window, not a CPU lease.
                if let Ok(value) = &mut value {
                    value.activity.take();
                }
                Some(value)
            }
            Err(oneshot::error::TryRecvError::Empty) => None,
            Err(oneshot::error::TryRecvError::Closed) => {
                self.claimed = true;
                Some(Err(cpu_execution_failure(
                    "internal result CPU forwarder exited without its outcome",
                )))
            }
        }
    }

    /// Poll the original bounded receipt directly. A cancelled waiter leaves
    /// both the receipt and the original join in this job's stable owner.
    pub(crate) fn poll_take(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<InternalResultValue<T>, ExecutionFailureInput>> {
        use std::{future::Future, pin::Pin, task::Poll};
        if self.closing || self.claimed {
            return Poll::Ready(Err(cpu_execution_failure(
                "internal result CPU receipt is no longer available",
            )));
        }
        let result = std::task::ready!(Pin::new(&mut self.completion).poll(cx));
        self.claimed = true;
        Poll::Ready(match result {
            Ok(mut result) => {
                if let Ok(value) = &mut result {
                    value.activity.take();
                }
                result
            }
            Err(_) => Err(cpu_execution_failure(
                "internal result CPU forwarder exited without its outcome",
            )),
        })
    }
}
impl<T> Drop for InternalResultCpuJob<T> {
    fn drop(&mut self) {
        // Detached fallback is not exit proof. A Native aggregate retaining
        // this job uses the original join and result activity independently.
        self.begin_close();
    }
}

impl InternalResultCpu {
    pub(crate) fn submit<I, O, F>(
        &self,
        input: InternalResultValue<I>,
        cancellation: QueryCancellationView,
        data_runtime: &FrontendDataRuntime,
        wake: Arc<dyn StatusIntakeWake>,
        apply: F,
    ) -> InternalResultCpuJob<O>
    where
        I: Send + 'static,
        O: Send + 'static,
        F: FnOnce(I, &ResultWindowAlias) -> O + Send + 'static,
    {
        let executor = self.executor.clone();
        let (mut completed, completion) = oneshot::channel();
        let forwarder = data_runtime.spawn(async move {
            let worker_cancellation = cancellation.clone();
            let work = executor.execute(move || {
                if let Some(reason) = worker_cancellation.reason() {
                    // input and its physical alias exit here, on the worker.
                    return Err(cpu_cancellation_failure(reason));
                }
                Ok(input.transform(apply))
            });
            tokio::pin!(work);
            let early_error = tokio::select! {
                biased;
                reason = cancellation.cancelled() => cpu_cancellation_failure(reason),
                _ = completed.closed() => cpu_execution_failure("internal result CPU consumer closed"),
                result = &mut work => {
                    // An unclaimed success still owns its input/output and
                    // original allowance until it is actually destroyed.
                    if let Err(unclaimed) = completed.send(result.map_err(|error| {
                        tracing::debug!(%error, "Internal result CPU executor failed");
                        cpu_execution_failure("internal result CPU executor failed")
                    }).and_then(|result| result)) {
                        drop(unclaimed);
                    }
                    wake.wake();
                    return;
                }
            };
            if let Err(unclaimed) = completed.send(Err(early_error)) {
                drop(unclaimed);
            }
            wake.wake();
            // Cancellation above is only an early result. Never drop this
            // executor future before its original receipt resolves.
            drop(work.await);
            wake.wake();
        });
        InternalResultCpuJob {
            completion,
            forwarder: Some(forwarder),
            closing: false,
            claimed: false,
            closed: None,
        }
    }
}

#[cfg(test)]
pub(crate) fn admitted_internal_fixture() -> (
    novarocks_workload_control::WorkloadControl,
    novarocks_workload_control::RootWork,
    novarocks_query_application::admitted_query_context::QueryResultCapacityBinding,
    novarocks_workload_control::ResultCapacityHandle,
) {
    use novarocks_workload_control::{
        ResourceConfig, WorkClass, WorkRequest, WorkloadConfig, WorkloadControl,
    };
    let control = WorkloadControl::try_new(
        WorkloadConfig::default(),
        ResourceConfig {
            total_bytes: 1024 * 1024,
            control_bytes: 1024,
            per_scope_bytes: 1024 * 1024 - 1024,
        },
    )
    .unwrap();
    let capacity = control
        .configure_result_capacity(ResultCapacityConfig::V1)
        .unwrap();
    control.mark_ready().unwrap();
    let (root, window) = control
        .root_admission()
        .try_begin_root_with_result(
            WorkRequest::new(WorkClass::Management),
            ResultWindowClass::Internal,
        )
        .unwrap();
    let binding =
        novarocks_query_application::admitted_query_context::QueryResultCapacityBinding::try_new(
            &root.owner.scope(),
            window.retain_alias(),
        )
        .unwrap();
    drop(window);
    (control, root, binding, capacity)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::task_execution::status_intake::CondvarWake;
    use novarocks_query_application::cancellation::{
        QueryCancellationReason, QueryCancellationSource,
    };
    use novarocks_workload_control::{
        ResourceConfig, WorkClass, WorkRequest, WorkloadConfig, WorkloadControl,
    };
    use std::sync::mpsc;
    use std::time::Duration;

    fn control() -> (
        WorkloadControl,
        novarocks_workload_control::ResultCapacityHandle,
    ) {
        let control = WorkloadControl::try_new(
            WorkloadConfig::default(),
            ResourceConfig {
                total_bytes: 1024 * 1024,
                control_bytes: 1024,
                per_scope_bytes: 1024 * 1024 - 1024,
            },
        )
        .unwrap();
        let capacity = control
            .configure_result_capacity(ResultCapacityConfig::V1)
            .unwrap();
        control.mark_ready().unwrap();
        (control, capacity)
    }

    async fn take<T>(
        job: &mut InternalResultCpuJob<T>,
    ) -> Result<InternalResultValue<T>, ExecutionFailureInput> {
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if let Some(value) = job.try_take() {
                    break value;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("CPU outcome watchdog")
    }

    #[tokio::test]
    async fn one_validated_profile_governs_actual_windows_and_physical_cpu_geometry() {
        let profile = FrontendResultCapacityProfile::try_new(ResultCapacityConfig {
            positions: [320, 16, 1, 64],
            all_objects_bytes: [
                8 * 1024 * 1024,
                96 * 1024 * 1024,
                INTERNAL_PEAK_BYTES,
                8 * 1024 * 1024,
            ],
            ..ResultCapacityConfig::V1
        })
        .unwrap();
        let control = WorkloadControl::try_new_counted(WorkloadConfig::default())
            .unwrap()
            .owner;
        let capacity = control
            .configure_result_capacity(profile.declared())
            .unwrap();
        control.mark_ready().unwrap();
        let root = control
            .try_begin_root(WorkRequest::new(WorkClass::Management))
            .unwrap();
        let window = capacity
            .try_acquire(&root.owner.scope(), ResultWindowClass::Internal)
            .unwrap();
        assert!(matches!(
            capacity.try_acquire(&root.owner.scope(), ResultWindowClass::Internal),
            Err(novarocks_workload_control::WorkError::Capacity(_))
        ));
        assert_eq!(capacity.snapshot().held_positions, [0, 0, 1, 0]);
        assert!(
            window
                .retain_alias()
                .check_backing_total(INTERNAL_PEAK_BYTES)
                .is_ok()
        );
        assert!(
            window
                .retain_alias()
                .check_backing_total(INTERNAL_PEAK_BYTES + 1)
                .is_err()
        );
        let mut owner = InternalResultCpuOwner::try_new(profile).unwrap();
        let installed = owner.runtime().executor.configured_capacity();
        assert_eq!(installed.worker_threads().get(), 1);
        assert_eq!(installed.queue_capacity().get(), 1);
        drop(window);
        root.owner.complete();
        root.business.release();
        owner
            .shutdown_until(Instant::now() + Duration::from_secs(5))
            .await
            .unwrap();
    }

    #[test]
    fn unsupported_internal_declarations_fail_before_opening_workers() {
        for (positions, bytes) in [(0, INTERNAL_PEAK_BYTES), (1, INTERNAL_PEAK_BYTES - 1)] {
            let mut capacity = ResultCapacityConfig::V1;
            capacity.positions[2] = positions;
            capacity.all_objects_bytes[2] = bytes;
            assert!(FrontendResultCapacityProfile::try_new(capacity).is_err());
        }
    }

    #[test]
    fn foreign_class_or_incomplete_envelope_refuses_before_constructor() {
        let (control, _) = control();
        let (root, window) = control
            .root_admission()
            .try_begin_root_with_result(
                WorkRequest::new(WorkClass::Management),
                ResultWindowClass::Local,
            )
            .unwrap();
        let result = InternalResultValue::<()>::try_produce(
            &root.owner.scope(),
            window.retain_alias(),
            |_| {
                panic!("a refused class must not construct a graph");
            },
        );
        assert!(result.is_err());
        let (foreign_control, _) = self::control();
        let (foreign_root, foreign_window) = foreign_control
            .root_admission()
            .try_begin_root_with_result(
                WorkRequest::new(WorkClass::Management),
                ResultWindowClass::Internal,
            )
            .unwrap();
        assert!(
            InternalResultValue::<()>::try_produce(
                &root.owner.scope(),
                foreign_window.retain_alias(),
                |_| {
                    panic!("foreign host capacity must refuse before growth");
                }
            )
            .is_err()
        );
        drop(foreign_window);
        foreign_root.owner.complete();
        foreign_root.business.release();
        drop(window);
        root.owner.complete();
        root.business.release();

        let control = WorkloadControl::try_new(
            WorkloadConfig::default(),
            ResourceConfig {
                total_bytes: 1024,
                control_bytes: 64,
                per_scope_bytes: 960,
            },
        )
        .unwrap();
        control
            .configure_result_capacity(ResultCapacityConfig {
                all_objects_bytes: [64; 4],
                ..ResultCapacityConfig::V1
            })
            .unwrap();
        control.mark_ready().unwrap();
        let (root, window) = control
            .root_admission()
            .try_begin_root_with_result(
                WorkRequest::new(WorkClass::Management),
                ResultWindowClass::Internal,
            )
            .unwrap();
        assert!(
            InternalResultValue::<()>::try_produce(
                &root.owner.scope(),
                window.retain_alias(),
                |_| panic!("peak refusal precedes growth")
            )
            .is_err()
        );
        drop(window);
        root.owner.complete();
        root.business.release();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn domain_cpu_does_not_block_control_and_result_retains_the_original_window() {
        let (control, capacity) = control();
        let (root, window) = control
            .root_admission()
            .try_begin_root_with_result(
                WorkRequest::new(WorkClass::Management),
                ResultWindowClass::Internal,
            )
            .unwrap();
        let mut owner = InternalResultCpuOwner::try_new_for_test().unwrap();
        let runtime = FrontendDataRuntime::new(tokio::runtime::Handle::current());
        let (entered, started) = mpsc::channel();
        let (release, blocked) = mpsc::channel();
        let input =
            InternalResultValue::try_produce(&root.owner.scope(), window.retain_alias(), |_| {
                Ok(vec![7])
            })
            .unwrap();
        let cancellation = QueryCancellationSource::new();
        let mut job = owner.runtime().submit(
            input,
            cancellation.view(),
            &runtime,
            Arc::new(CondvarWake::default()),
            move |input, _| {
                entered.send(()).unwrap();
                blocked.recv().unwrap();
                input
            },
        );
        drop(window);
        root.owner.complete();
        root.business.release();
        tokio::time::timeout(Duration::from_secs(5), async {
            while started.try_recv().is_err() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert_eq!(tokio::spawn(async { 11 }).await.unwrap(), 11);
        assert_eq!(capacity.snapshot().held_positions, [0, 0, 1, 0]);
        release.send(()).unwrap();
        let result = take(&mut job).await.unwrap();
        assert_eq!(result.value(), &vec![7]);
        assert_eq!(capacity.snapshot().held_positions, [0, 0, 1, 0]);
        drop(result);
        drop(job);
        assert_eq!(capacity.snapshot().held_positions, [0; 4]);
        owner
            .shutdown_until(Instant::now() + Duration::from_secs(5))
            .await
            .unwrap();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn unclaimed_success_keeps_capacity_and_drops_payload_before_its_window() {
        use std::sync::atomic::{AtomicBool, Ordering};
        struct Notice {
            capacity: novarocks_workload_control::ResultCapacityHandle,
            dropped: Arc<AtomicBool>,
        }
        impl Drop for Notice {
            fn drop(&mut self) {
                assert_eq!(self.capacity.snapshot().held_positions, [0, 0, 1, 0]);
                self.dropped.store(true, Ordering::Release);
            }
        }
        struct Wake(Arc<AtomicBool>);
        impl std::fmt::Debug for Wake {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str("Wake")
            }
        }
        impl StatusIntakeWake for Wake {
            fn wake(&self) {
                self.0.store(true, Ordering::Release);
            }
        }
        let (control, capacity) = control();
        let (root, window) = control
            .root_admission()
            .try_begin_root_with_result(
                WorkRequest::new(WorkClass::Management),
                ResultWindowClass::Internal,
            )
            .unwrap();
        let mut owner = InternalResultCpuOwner::try_new_for_test().unwrap();
        let runtime = FrontendDataRuntime::new(tokio::runtime::Handle::current());
        let dropped = Arc::new(AtomicBool::new(false));
        let input =
            InternalResultValue::try_produce(&root.owner.scope(), window.retain_alias(), |_| {
                Ok(Notice {
                    capacity: capacity.clone(),
                    dropped: Arc::clone(&dropped),
                })
            })
            .unwrap();
        let published = Arc::new(AtomicBool::new(false));
        let cancellation = QueryCancellationSource::new();
        let job = owner.runtime().submit(
            input,
            cancellation.view(),
            &runtime,
            Arc::new(Wake(Arc::clone(&published))),
            |input, _| input,
        );
        drop(window);
        root.owner.complete();
        root.business.release();
        tokio::time::timeout(Duration::from_secs(5), async {
            while !published.load(Ordering::Acquire) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert!(!dropped.load(Ordering::Acquire));
        assert_eq!(capacity.snapshot().held_positions, [0, 0, 1, 0]);
        drop(job);
        assert!(dropped.load(Ordering::Acquire));
        assert_eq!(capacity.snapshot().held_positions, [0; 4]);
        owner
            .shutdown_until(Instant::now() + Duration::from_secs(5))
            .await
            .unwrap();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn cancelled_waiter_keeps_running_input_until_actual_worker_exit() {
        let (control, capacity) = control();
        let (root, window) = control
            .root_admission()
            .try_begin_root_with_result(
                WorkRequest::new(WorkClass::Management),
                ResultWindowClass::Internal,
            )
            .unwrap();
        let binding = novarocks_query_application::admitted_query_context::QueryResultCapacityBinding::try_new(&root.owner.scope(), window.retain_alias()).unwrap();
        let mut owner = InternalResultCpuOwner::try_new_for_test().unwrap();
        let runtime = FrontendDataRuntime::new(tokio::runtime::Handle::current());
        let (entered, started) = mpsc::channel();
        let (release, blocked) = mpsc::channel();
        let input =
            InternalResultValue::try_produce(&root.owner.scope(), window.retain_alias(), |_| {
                Ok(vec![1])
            })
            .unwrap()
            .track_actual_exit(&binding)
            .unwrap();
        let cancellation = QueryCancellationSource::new();
        let mut job = owner.runtime().submit(
            input,
            cancellation.view(),
            &runtime,
            Arc::new(CondvarWake::default()),
            move |input, _| {
                entered.send(()).unwrap();
                blocked.recv().unwrap();
                input
            },
        );
        drop(window);
        root.owner.complete();
        root.business.release();
        tokio::time::timeout(Duration::from_secs(5), async {
            while started.try_recv().is_err() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        cancellation.request(QueryCancellationReason::ExplicitKill {
            requester_connection_id: 1,
        });
        assert!(take(&mut job).await.is_err());
        drop(job);
        let (fence_done, exited) = mpsc::channel();
        let fence = std::thread::spawn(move || {
            binding.wait_result_activities_exited();
            drop(binding);
            fence_done.send(()).unwrap();
        });
        assert!(matches!(exited.try_recv(), Err(mpsc::TryRecvError::Empty)));
        assert_eq!(capacity.snapshot().held_positions, [0, 0, 1, 0]);
        release.send(()).unwrap();
        owner
            .shutdown_until(Instant::now() + Duration::from_secs(5))
            .await
            .unwrap();
        // The original forwarder retires the worker receipt on this runtime.
        // Waiting for its activity fence must leave that runtime able to poll it.
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                match exited.try_recv() {
                    Ok(()) => break,
                    Err(mpsc::TryRecvError::Empty) => tokio::task::yield_now().await,
                    Err(mpsc::TryRecvError::Disconnected) => {
                        panic!("result activity fence exited without its observation")
                    }
                }
            }
        })
        .await
        .unwrap();
        fence.join().unwrap();
        assert_eq!(capacity.snapshot().held_positions, [0; 4]);
    }
}

#[cfg(test)]
mod actual_close_tests {
    use super::*;
    use novarocks_query_application::cancellation::{
        QueryCancellationReason, QueryCancellationSource,
    };
    use std::{
        sync::{
            atomic::{AtomicBool, Ordering},
            mpsc,
        },
        time::Duration,
    };

    #[tokio::test(flavor = "current_thread")]
    async fn early_cancel_and_explicit_close_keep_original_cpu_join_until_payload_exit() {
        close_case(None).await;
        for reason in [
            QueryCancellationReason::ExplicitKill {
                requester_connection_id: 1,
            },
            QueryCancellationReason::DeadlineExceeded { timeout_ms: 91 },
            QueryCancellationReason::FrontendDrainDeadlineExceeded { timeout_ms: 92 },
        ] {
            close_case(Some(reason)).await;
        }
    }

    async fn close_case(cancel: Option<QueryCancellationReason>) {
        let explicit_close = cancel.is_none();
        let expected_reason = match &cancel {
            Some(
                QueryCancellationReason::DeadlineExceeded { .. }
                | QueryCancellationReason::FrontendDrainDeadlineExceeded { .. },
            ) => QueryFailureReason::DeadlineExceeded,
            _ => QueryFailureReason::Cancelled,
        };
        struct Payload(Arc<AtomicBool>);
        impl Drop for Payload {
            fn drop(&mut self) {
                self.0.store(true, Ordering::Release);
            }
        }
        let (_control, root, binding, capacity) = admitted_internal_fixture();
        let dropped = Arc::new(AtomicBool::new(false));
        let input =
            InternalResultValue::try_produce(binding.scope(), binding.window_alias(), |_| {
                Ok(Payload(Arc::clone(&dropped)))
            })
            .unwrap()
            .track_actual_exit(&binding)
            .unwrap();
        let mut owner = InternalResultCpuOwner::try_new_for_test().unwrap();
        let runtime = FrontendDataRuntime::new(tokio::runtime::Handle::current());
        let (entered, started) = mpsc::channel();
        let (release, blocked) = mpsc::channel();
        let cancellation = QueryCancellationSource::new();
        let mut job = owner.runtime().submit(
            input,
            cancellation.view(),
            &runtime,
            Arc::new(crate::task_execution::status_intake::CondvarWake::default()),
            move |input, _| {
                entered.send(()).unwrap();
                let _ = blocked.recv();
                input
            },
        );
        root.owner.complete();
        root.business.release();
        tokio::time::timeout(Duration::from_secs(5), async {
            while started.try_recv().is_err() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        // This is the real loop race: its control observation was still clear,
        // then cancellation arrives before the owner claims the CPU outcome.
        let control_was_clear = !cancellation.view().is_cancelled();
        let early = if explicit_close {
            job.begin_close();
            Ok(None)
        } else {
            cancellation.request(cancel.expect("explicit control reason"));
            tokio::time::timeout(Duration::from_secs(5), async {
                loop {
                    if let Some(result) = job.try_take() {
                        break result.err();
                    }
                    tokio::task::yield_now().await;
                }
            })
            .await
        };
        job.begin_close();
        let still_joining =
            std::future::poll_fn(|cx| std::task::Poll::Ready(job.poll_closed(cx).is_pending()))
                .await;
        let mut first_waiter = Box::pin(binding.result_activities_exited());
        let mut surviving_waiter = Box::pin(binding.result_activities_exited());
        let waiting = std::future::poll_fn(|cx| {
            use std::future::Future;
            std::task::Poll::Ready(
                first_waiter.as_mut().poll(cx).is_pending()
                    && surviving_waiter.as_mut().poll(cx).is_pending(),
            )
        })
        .await;
        drop(first_waiter);
        let held =
            capacity.snapshot().held_positions == [0, 0, 1, 0] && !dropped.load(Ordering::Acquire);
        // Release and join all real workers before asserting the observations.
        release.send(()).unwrap();
        let joined = tokio::time::timeout(Duration::from_secs(5), job.join_closed())
            .await
            .unwrap()
            .is_ok();
        tokio::time::timeout(Duration::from_secs(5), surviving_waiter)
            .await
            .unwrap();
        binding.result_activities_exited().await;
        let dropped_at_join = dropped.load(Ordering::Acquire);
        let no_join_failure = job.join_failure().is_none();
        drop(job);
        drop(binding);
        owner
            .shutdown_until(Instant::now() + Duration::from_secs(5))
            .await
            .unwrap();
        let early = early.unwrap();
        if explicit_close {
            assert!(early.is_none());
        } else {
            let input = early.expect("the original statement control authors the CPU result");
            assert_eq!(input.fact(), expected_reason.failure_fact());
            assert_eq!(input.site(), ExecutionFailureSite::Coordinator);
            assert_eq!(
                input.detail().as_str(),
                "internal result CPU was stopped by its statement control owner"
            );
            assert_eq!(
                (input.descriptor().errno, input.descriptor().sqlstate),
                (1317, "70100")
            );
            assert_eq!(
                input.user_error().phase(),
                novarocks_user_error::ErrorPhase::Execution
            );
        }
        assert!(control_was_clear);
        assert!(still_joining && waiting && held);
        assert!(joined && dropped_at_join && no_join_failure);
        // Successful joins have no failure to hand to process health.
        // `job` was already disposed after the actual close above.
        assert_eq!(capacity.snapshot().held_positions, [0; 4]);
    }
}
