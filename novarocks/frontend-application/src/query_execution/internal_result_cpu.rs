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

use novarocks_query_application::cancellation::QueryCancellationView;
use novarocks_query_application::cpu::{
    QueryBlockingExecutor, QueryBlockingExecutorConfig, QueryBlockingExecutorOwner,
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

pub(crate) struct InternalResultCpuOwner {
    workers: QueryBlockingExecutorOwner,
    runtime: InternalResultCpu,
}

#[derive(Clone)]
pub(crate) struct InternalResultCpu {
    executor: QueryBlockingExecutor,
}

impl InternalResultCpuOwner {
    pub(crate) fn try_new() -> Result<Self, String> {
        let positions = NonZeroUsize::new(ResultCapacityConfig::V1.positions[2])
            .expect("the frozen Internal profile has positions");
        let workers = QueryBlockingExecutorOwner::try_new(QueryBlockingExecutorConfig::new(
            positions, positions,
        ))?;
        let runtime = InternalResultCpu {
            executor: workers.executor(),
        };
        Ok(Self { workers, runtime })
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
    pub(crate) fn window(&self) -> &ResultWindowAlias {
        &self.window
    }

    pub(crate) fn try_transform<R>(
        self,
        transform: impl FnOnce(T, &ResultWindowAlias) -> Result<R, String>,
    ) -> Result<InternalResultValue<R>, String> {
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

/// At most one result-codec job per serial root consumer. Its forwarder owns
/// the bounded receipt; Drop cancels that waiter, never a running CPU closure.
/// An unclaimed output contains the same window until its actual destruction.
pub(crate) struct InternalResultCpuJob<T> {
    completion: oneshot::Receiver<Result<InternalResultValue<T>, String>>,
    forwarder: tokio::task::JoinHandle<()>,
}
impl<T> InternalResultCpuJob<T> {
    pub(crate) fn try_take(&mut self) -> Option<Result<InternalResultValue<T>, String>> {
        match self.completion.try_recv() {
            Ok(mut value) => {
                // A claimed receipt proves CPU execution has exited. The
                // serial consumer keeps the payload/window, not a CPU lease.
                if let Ok(value) = &mut value {
                    value.activity.take();
                }
                Some(value)
            }
            Err(oneshot::error::TryRecvError::Empty) => None,
            Err(oneshot::error::TryRecvError::Closed) => Some(Err(
                "internal result CPU forwarder exited without its outcome".into(),
            )),
        }
    }
}
impl<T> Drop for InternalResultCpuJob<T> {
    fn drop(&mut self) {
        self.forwarder.abort();
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
        let (completed, completion) = oneshot::channel();
        let forwarder = data_runtime.spawn(async move {
            let worker_cancellation = cancellation.clone();
            let work = executor.execute(move || {
                if worker_cancellation.is_cancelled() {
                    // input and its physical alias exit here, on the worker.
                    return Err("internal result CPU cancelled before execution".to_owned());
                }
                Ok(input.transform(apply))
            });
            tokio::pin!(work);
            let outcome = tokio::select! {
                biased;
                _ = cancellation.cancelled() => Err("internal result CPU waiter cancelled".into()),
                result = &mut work => result.and_then(|result| result),
            };
            // If the serial consumer has exited, send returns the still-owned
            // result. Drop it before publishing the forwarder's actual exit.
            if let Err(unclaimed) = completed.send(outcome) {
                drop(unclaimed);
            }
            wake.wake();
        });
        InternalResultCpuJob {
            completion,
            forwarder,
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

    async fn take<T>(job: &mut InternalResultCpuJob<T>) -> Result<InternalResultValue<T>, String> {
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
        let mut owner = InternalResultCpuOwner::try_new().unwrap();
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
        let mut owner = InternalResultCpuOwner::try_new().unwrap();
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
        let mut owner = InternalResultCpuOwner::try_new().unwrap();
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
        exited.recv_timeout(Duration::from_secs(5)).unwrap();
        fence.join().unwrap();
        assert_eq!(capacity.snapshot().held_positions, [0; 4]);
    }
}
