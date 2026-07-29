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

#[cfg(test)]
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, mpsc};

use novarocks::common::app_config;
use novarocks::novarocks_logging::{error, info, warn};
use novarocks::runtime::fragment::io::{
    ExchangeFrameTransmitter, FragmentEventSink, FragmentLookupClient, FragmentResultWriter,
};
use novarocks::runtime::fragment::{
    FragmentCancelReason, FragmentOutcome, RunningFragmentHandle, prepare_fragment,
};
use novarocks::runtime::native_fragment_query::NativeFragmentQueryRuntime;
use novarocks::runtime::profile::Profiler;
use novarocks::service::fe_report;
use novarocks::service::native_fragment_ingress::{
    NativeFragmentAccepted, NativeFragmentCancelRequest, NativeFragmentIngress,
    NativeFragmentIngressError, NativeFragmentRequest,
};

use super::control::{FragmentControlHandle, FragmentControlRegistry};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum NativeFragmentLifecycleEvent {
    Prepared,
    Registered,
    Accepted,
    Started,
}

type LifecycleObserver = Arc<dyn Fn(NativeFragmentLifecycleEvent) + Send + Sync>;

pub struct NativeFragmentService {
    pub(super) controls: Arc<FragmentControlRegistry>,
    queries: NativeFragmentQueryRuntime,
    exchange_transmitter: Arc<dyn ExchangeFrameTransmitter>,
    lookup_client: Arc<dyn FragmentLookupClient>,
    result_writer: Arc<dyn FragmentResultWriter>,
    event_sink: Arc<dyn FragmentEventSink>,
    lifecycle_observer: Option<LifecycleObserver>,
    #[cfg(test)]
    fail_worker_spawn_on_submission: Option<usize>,
    #[cfg(test)]
    submission_count: AtomicUsize,
}

impl std::fmt::Debug for NativeFragmentService {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("NativeFragmentService")
            .finish_non_exhaustive()
    }
}

impl NativeFragmentService {
    pub fn new(
        exchange_transmitter: Arc<dyn ExchangeFrameTransmitter>,
        lookup_client: Arc<dyn FragmentLookupClient>,
        result_writer: Arc<dyn FragmentResultWriter>,
        event_sink: Arc<dyn FragmentEventSink>,
    ) -> Self {
        Self {
            controls: Arc::new(FragmentControlRegistry::default()),
            queries: NativeFragmentQueryRuntime::global(),
            exchange_transmitter,
            lookup_client,
            result_writer,
            event_sink,
            lifecycle_observer: None,
            #[cfg(test)]
            fail_worker_spawn_on_submission: None,
            #[cfg(test)]
            submission_count: AtomicUsize::new(0),
        }
    }

    #[cfg(test)]
    fn with_lifecycle_observer(
        observer: impl Fn(NativeFragmentLifecycleEvent) + Send + Sync + 'static,
    ) -> Self {
        Self {
            lifecycle_observer: Some(Arc::new(observer)),
            ..Self::new(
                crate::fragment::grpc_exchange_transmitter(),
                crate::fragment::grpc_fragment_lookup_client(),
                crate::fragment::native_result_writer(),
                crate::fragment::native_fragment_event_sink(),
            )
        }
    }

    #[cfg(test)]
    fn with_lifecycle_observer_and_worker_spawn_failure(
        observer: impl Fn(NativeFragmentLifecycleEvent) + Send + Sync + 'static,
        fail_worker_spawn_on_submission: usize,
    ) -> Self {
        Self {
            lifecycle_observer: Some(Arc::new(observer)),
            fail_worker_spawn_on_submission: Some(fail_worker_spawn_on_submission),
            ..Self::new(
                crate::fragment::grpc_exchange_transmitter(),
                crate::fragment::grpc_fragment_lookup_client(),
                crate::fragment::native_result_writer(),
                crate::fragment::native_fragment_event_sink(),
            )
        }
    }

    fn observe(&self, event: NativeFragmentLifecycleEvent) {
        if let Some(observer) = self.lifecycle_observer.as_ref() {
            observer(event);
        }
    }
}

impl NativeFragmentIngress for NativeFragmentService {
    fn submit(
        &self,
        request: NativeFragmentRequest,
    ) -> Result<NativeFragmentAccepted, NativeFragmentIngressError> {
        let query_id = request.query_id();
        let fragment_instance_id = request.fragment_instance_id();
        let backend_num = request.backend_num();
        let report_endpoint = request.report_endpoint().cloned();
        let enable_profile = request.enable_profile();
        let report_interval_ns = profile_report_interval_ns(
            enable_profile,
            request.runtime_profile_report_interval_seconds(),
        );
        let (delivery_expire, query_expire) = request.query_expire_durations();
        let cache_options = request.cache_options()?;
        let profiler =
            enable_profile.then(|| profiler_for_native_fragment(request.root_plan_node_id()));
        let admission = self
            .queries
            .prepare_admission(
                query_id,
                fragment_instance_id,
                delivery_expire,
                query_expire,
                cache_options,
                request.has_runtime_filter_bindings(),
            )
            .map_err(NativeFragmentIngressError::new)?;
        let query_mem_tracker = admission.query_mem_tracker();
        let fragment_mem_tracker = admission.fragment_mem_tracker();
        let dormant = prepare_fragment(
            request.into_submission(),
            admission.into_prepare_context(
                profiler.clone(),
                Arc::clone(&self.exchange_transmitter),
                Arc::clone(&self.lookup_client),
                Arc::clone(&self.result_writer),
                Arc::clone(&self.event_sink),
            ),
        )
        .map_err(NativeFragmentIngressError::new)?;
        self.observe(NativeFragmentLifecycleEvent::Prepared);

        let reservation = self
            .controls
            .reserve(fragment_instance_id)
            .map_err(NativeFragmentIngressError::new)?;
        let registration = self
            .queries
            .register_fragment(
                query_id,
                fragment_instance_id,
                delivery_expire,
                query_expire,
            )
            .map_err(NativeFragmentIngressError::new)?;

        let (start_tx, start_rx) = mpsc::sync_channel::<()>(0);
        let controls = Arc::clone(&self.controls);
        let queries = self.queries.clone();
        let observer = self.lifecycle_observer.clone();
        #[cfg(test)]
        if self.fail_worker_spawn_on_submission.is_some_and(|target| {
            self.submission_count.fetch_add(1, Ordering::SeqCst) + 1 == target
        }) {
            return Err(NativeFragmentIngressError::new(
                "injected native fragment adapter worker spawn failure",
            ));
        }
        std::thread::Builder::new()
            .name(format!(
                "native-fragment-{:x}-{:x}",
                fragment_instance_id.hi, fragment_instance_id.lo
            ))
            .spawn(move || {
                if start_rx.recv().is_err() {
                    let error = "native fragment start signal was dropped".to_string();
                    fe_report::report_fragment_done(fragment_instance_id, Some(error), false);
                    return;
                }
                let running = dormant.start();
                let control = Arc::new(RunningFragmentControl {
                    handle: running.clone(),
                });
                let token = reservation.publish(control);
                registration.into_running();
                if let Some(observer) = observer.as_ref() {
                    observer(NativeFragmentLifecycleEvent::Started);
                }
                consume_terminal_fact(running, token, controls, queries);
            })
            .map_err(|error| {
                NativeFragmentIngressError::new(format!(
                    "spawn native fragment adapter worker failed: {error}"
                ))
            })?;

        if let Some(report_endpoint) = report_endpoint {
            fe_report::register_novarocks_instance(
                fragment_instance_id,
                query_id,
                report_endpoint,
                backend_num,
                enable_profile,
                profiler,
                Some(fragment_mem_tracker),
                Some(query_mem_tracker),
                report_interval_ns,
            );
        } else {
            warn!(
                target: "novarocks::report",
                finst_id = %fragment_instance_id,
                "missing native report_endpoint for reportExecStatus"
            );
        }
        self.observe(NativeFragmentLifecycleEvent::Registered);
        self.observe(NativeFragmentLifecycleEvent::Accepted);
        start_tx.send(()).map_err(|_| {
            NativeFragmentIngressError::new(
                "native fragment adapter worker terminated before start",
            )
        })?;
        Ok(NativeFragmentAccepted::new(query_id, fragment_instance_id))
    }

    fn cancel(
        &self,
        request: NativeFragmentCancelRequest,
    ) -> Result<(), NativeFragmentIngressError> {
        self.controls
            .cancel_many(request.fragment_instance_ids(), request.reason());
        Ok(())
    }
}

struct RunningFragmentControl {
    handle: RunningFragmentHandle,
}

impl FragmentControlHandle for RunningFragmentControl {
    fn cancel(&self, reason: &str) {
        self.handle.cancel(FragmentCancelReason::new(reason));
    }
}

fn consume_terminal_fact(
    running: RunningFragmentHandle,
    token: super::control::FragmentControlToken,
    controls: Arc<FragmentControlRegistry>,
    queries: NativeFragmentQueryRuntime,
) {
    let fact = running.join();
    let query_id = fact.query_id();
    let fragment_instance_id = fact.fragment_instance_id();
    let report_error = match fact.outcome() {
        FragmentOutcome::Succeeded => {
            if let Some(profile) = fact.profile() {
                info!(
                    target: "novarocks::profile",
                    finst_id = %fragment_instance_id,
                    profile = ?profile,
                    "native_fragment_profile"
                );
            }
            None
        }
        FragmentOutcome::Failed(execution_error) => {
            let report_error = execution_error.to_string();
            error!(
                target: "novarocks::exec",
                finst_id = %fragment_instance_id,
                error = %execution_error,
                "native fragment execution failed"
            );
            let finsts = queries.cancel_query(query_id, report_error.clone());
            controls.cancel_many(&finsts, &report_error);
            Some(report_error)
        }
        FragmentOutcome::Cancelled { reason } => Some(reason.detail().to_string()),
    };
    let report_decision = queries.finish_fragment_for_report(query_id);
    fe_report::report_fragment_done(
        fragment_instance_id,
        report_error,
        report_decision.include_runtime_filter_profile(),
    );
    queries.unregister_fragment(fragment_instance_id);
    queries.cleanup_after_fragment_report(query_id, report_decision);
    token.complete();
}

fn profiler_for_native_fragment(root_plan_node_id: i32) -> Profiler {
    let profiler = Profiler::new(format!(
        "execute_fragment_native (plan_node_id={root_plan_node_id})"
    ));
    profiler.set_metadata(i64::from(root_plan_node_id));
    profiler
}

fn profile_report_interval_ns(
    enable_profile: bool,
    query_interval_seconds: Option<i64>,
) -> Option<i64> {
    if !enable_profile {
        return None;
    }
    query_interval_seconds
        .filter(|value| *value > 0)
        .and_then(|value| value.checked_mul(1_000_000_000))
        .or_else(|| {
            app_config::config()
                .ok()
                .map(|config| config.runtime.profile_report_interval.max(1) * 1_000_000_000)
        })
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex, mpsc};
    use std::time::{Duration, Instant};

    use novarocks::UniqueId;
    use novarocks::proto;
    use novarocks::runtime::query_context::QueryId;
    use novarocks::service::native_fragment_ingress::{
        NativeFragmentIngress, NativeFragmentRequest,
    };

    use super::{NativeFragmentLifecycleEvent, NativeFragmentService};

    fn values_result_request(query_base: i64, fragment_base: i64) -> NativeFragmentRequest {
        let fragment_id = 7;
        NativeFragmentRequest::try_decode(
            proto::plan::PlanFragment {
                fragment_id,
                root: Some(proto::plan::DistributedNode {
                    node_id: 41,
                    fragment_id,
                    limit: -1,
                    payload: Some(proto::plan::distributed_node::Payload::Physical(
                        proto::plan::PlanNode {
                            output_columns: Vec::new(),
                            kind: Some(proto::plan::plan_node::Kind::Values(
                                proto::plan::ValuesNode {
                                    rows: Vec::new(),
                                    columns: Vec::new(),
                                },
                            )),
                        },
                    )),
                    ..Default::default()
                }),
                sink: Some(proto::plan::DataSink {
                    kind: Some(proto::plan::data_sink::Kind::Result(true)),
                }),
                output_columns: Vec::new(),
                runtime_filter_bindings: Some(proto::plan::RuntimeFilterBindingTable {
                    fragment_id,
                    bindings: Vec::new(),
                }),
                ..Default::default()
            },
            proto::novarocks::InstanceParams {
                query_id: Some(proto::common::UniqueId {
                    hi: query_base,
                    lo: query_base + 1,
                }),
                fragment_instance_id: Some(proto::common::UniqueId {
                    hi: fragment_base,
                    lo: fragment_base + 1,
                }),
                backend_num: 3,
                query_options: Some(proto::novarocks::QueryOptions {
                    batch_size: 1024,
                    pipeline_dop: 1,
                    ..Default::default()
                }),
                ..Default::default()
            },
        )
        .expect("valid native fragment request")
    }

    #[test]
    fn submit_acceptance_point_follows_prepare_and_registration_before_start() {
        let events = Arc::new(Mutex::new(Vec::new()));
        let captured = Arc::clone(&events);
        let service = NativeFragmentService::with_lifecycle_observer(move |event| {
            captured.lock().expect("lifecycle events").push(event);
        });

        service
            .submit(values_result_request(81_000, 81_002))
            .expect("native fragment submit");

        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
        while events.lock().expect("lifecycle events").len() < 4
            && std::time::Instant::now() < deadline
        {
            std::thread::yield_now();
        }
        assert_eq!(
            *events.lock().expect("lifecycle events"),
            vec![
                NativeFragmentLifecycleEvent::Prepared,
                NativeFragmentLifecycleEvent::Registered,
                NativeFragmentLifecycleEvent::Accepted,
                NativeFragmentLifecycleEvent::Started,
            ]
        );
    }

    #[test]
    fn registration_failure_drops_dormant_resources_before_retry() {
        let service = NativeFragmentService::new(
            crate::fragment::grpc_exchange_transmitter(),
            crate::fragment::grpc_fragment_lookup_client(),
            crate::fragment::native_result_writer(),
            crate::fragment::native_fragment_event_sink(),
        );
        let first = values_result_request(82_000, 82_002);
        let finst_id = first.fragment_instance_id();
        let reservation = service
            .controls
            .reserve(finst_id)
            .expect("reserve conflicting service route");

        let error = service
            .submit(first)
            .expect_err("duplicate service registration must fail");
        assert!(error.to_string().contains("already registered"), "{error}");

        drop(reservation);
        service
            .submit(values_result_request(82_000, 82_002))
            .expect("retry must observe rolled-back dormant resources");
    }

    #[test]
    fn second_worker_spawn_failure_rolls_back_only_its_pre_start_registration() {
        let (started_tx, started_rx) = mpsc::sync_channel(1);
        let (release_tx, release_rx) = mpsc::sync_channel(0);
        let release_rx = Arc::new(Mutex::new(release_rx));
        let worker_release = Arc::clone(&release_rx);
        let service = NativeFragmentService::with_lifecycle_observer_and_worker_spawn_failure(
            move |event| {
                if event == NativeFragmentLifecycleEvent::Started {
                    started_tx.send(()).expect("publish first worker start");
                    worker_release
                        .lock()
                        .expect("first worker release")
                        .recv()
                        .expect("release first worker");
                }
            },
            2,
        );
        let query_id = QueryId::new(83_000, 83_001);
        let first = UniqueId {
            hi: 83_002,
            lo: 83_003,
        };
        let second = UniqueId {
            hi: 83_004,
            lo: 83_005,
        };

        service
            .submit(values_result_request(83_000, 83_002))
            .expect("first fragment reaches running");
        started_rx
            .recv_timeout(Duration::from_secs(2))
            .expect("first worker remains registered");

        let error = service
            .submit(values_result_request(83_000, 83_004))
            .expect_err("second worker spawn is injected to fail");
        assert!(error.to_string().contains("spawn failure"), "{error}");
        assert!(
            service.controls.reserve(first).is_err(),
            "first running route must remain registered"
        );
        drop(
            service
                .controls
                .reserve(second)
                .expect("failed second registration must release its route"),
        );

        release_tx.send(()).expect("release first worker");
        let deadline = Instant::now() + Duration::from_secs(2);
        loop {
            match service.controls.reserve(first) {
                Ok(reservation) => {
                    drop(reservation);
                    break;
                }
                Err(_) if Instant::now() < deadline => std::thread::yield_now(),
                Err(error) => panic!("first fragment did not terminate: {error}"),
            }
        }
        assert!(
            service
                .queries
                .cancel_query(query_id, "post-terminal probe".to_string())
                .is_empty(),
            "terminated query must not retain either fragment mapping"
        );
    }
}
