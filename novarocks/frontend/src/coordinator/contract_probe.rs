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

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};

use novarocks::UniqueId;
use novarocks::query_execution::artifact::{NativeSubmissionContext, PreparedNativeExecutionParts};
use novarocks::query_execution::contract::{
    DistributedQueryError, DistributedQueryErrorKind, DistributedQueryIntent,
    DistributedQueryOutcome, DistributedQueryRequest, ProfileReportBuilder, QueryId,
};
use novarocks::query_execution::fragment_transport::{FetchOutcome, FragmentDispatcher};
use novarocks::query_execution::write::{NativeExecutionReport, WriteReportBuilder};

use super::scheduler::FrontendFragmentScheduler;

pub(crate) struct FrontendContractProbe {
    query_id: QueryId,
    report_endpoint: SocketAddr,
    scheduler: FrontendFragmentScheduler,
    dispatcher: Arc<dyn FragmentDispatcher>,
    native_reports: Mutex<Vec<NativeExecutionReport>>,
    failure_state: FrontendFailureState,
}

#[derive(Default)]
struct FrontendFailureState {
    first_failure: Mutex<Option<String>>,
}

impl FrontendFailureState {
    fn latch(&self, message: impl Into<String>) -> String {
        let mut first_failure = self.first_failure.lock().unwrap();
        first_failure.get_or_insert_with(|| message.into()).clone()
    }

    fn message(&self) -> Option<String> {
        self.first_failure.lock().unwrap().clone()
    }
}

impl FrontendContractProbe {
    pub(crate) fn new(
        query_id: QueryId,
        report_endpoint: SocketAddr,
        scheduler: FrontendFragmentScheduler,
        dispatcher: Arc<dyn FragmentDispatcher>,
    ) -> Self {
        Self {
            query_id,
            report_endpoint,
            scheduler,
            dispatcher,
            native_reports: Mutex::new(Vec::new()),
            failure_state: FrontendFailureState::default(),
        }
    }

    pub(crate) fn with_native_reports(mut self, reports: Vec<NativeExecutionReport>) -> Self {
        self.native_reports = Mutex::new(reports);
        self
    }

    pub(crate) fn failure_message(&self) -> Option<String> {
        self.failure_state.message()
    }

    pub(crate) fn execute(
        &self,
        request: DistributedQueryRequest,
    ) -> Result<DistributedQueryOutcome, DistributedQueryError> {
        let parts = request.into_parts();
        let intent = parts.completion.intent();
        let schedule = self
            .scheduler
            .schedule(parts.artifacts.scheduling_view(), self.query_id)?;
        let context = NativeSubmissionContext::new(
            self.query_id,
            &parts.options,
            self.report_endpoint,
            self.dispatcher.needs_fragment_status_report()
                || intent == DistributedQueryIntent::Profile,
        );
        let execution = parts.artifacts.assemble(schedule, context)?;
        let PreparedNativeExecutionParts {
            submissions,
            root_fetch,
            writer_registrations,
            expected_output,
            runtime_filter_deployment: _,
        } = execution.into_parts();

        let mut attempted = BTreeMap::<usize, Vec<UniqueId>>::new();
        for submission in submissions {
            let backend_idx = submission.backend_idx();
            let finst_id = submission.fragment_instance_id();
            if parts.cancellation.is_cancelled() {
                cancel_attempted(self.dispatcher.as_ref(), &attempted);
                return Err(failed("query cancelled before fragment submission"));
            }
            attempted.entry(backend_idx).or_default().push(finst_id);
            if let Err(error) = self
                .dispatcher
                .submit_fragment(backend_idx, submission.into_envelope())
            {
                cancel_attempted(self.dispatcher.as_ref(), &attempted);
                return Err(failed(error));
            }
        }

        if parts.cancellation.is_cancelled() {
            cancel_attempted(self.dispatcher.as_ref(), &attempted);
            return Err(failed("query cancelled after fragment submission"));
        }

        let reports = std::mem::take(&mut *self.native_reports.lock().unwrap());
        if let Some(message) = reports
            .iter()
            .find_map(|report| report.failure_message().map(str::to_owned))
        {
            let message = self.failure_state.latch(message);
            cancel_attempted(self.dispatcher.as_ref(), &attempted);
            if intent != DistributedQueryIntent::Write {
                return Err(failed(message));
            }
        }

        let mut batches = Vec::new();
        if root_fetch.uses_result_buffer() {
            let timeout_ms = parts.options.timeout_ms().max(0);
            let deadline =
                std::time::Instant::now() + std::time::Duration::from_millis(timeout_ms as u64);
            loop {
                if parts.cancellation.is_cancelled() {
                    cancel_attempted(self.dispatcher.as_ref(), &attempted);
                    return Err(failed("query cancelled while fetching result"));
                }
                let now = std::time::Instant::now();
                if now >= deadline {
                    cancel_attempted(self.dispatcher.as_ref(), &attempted);
                    return Err(failed(format!("query timed out after {timeout_ms} ms")));
                }
                let fetch_wait_ms = deadline
                    .saturating_duration_since(now)
                    .as_millis()
                    .min(300)
                    .max(1) as i64;
                let fetch = match self.dispatcher.fetch_result(
                    root_fetch.backend_idx(),
                    root_fetch.fragment_instance_id(),
                    fetch_wait_ms,
                    Some(expected_output.fetch_view()),
                ) {
                    Ok(fetch) => fetch,
                    Err(error) => {
                        cancel_attempted(self.dispatcher.as_ref(), &attempted);
                        return Err(failed(error));
                    }
                };
                match fetch {
                    FetchOutcome::Ready(batch) => batches.push(batch),
                    FetchOutcome::NotReady => continue,
                    FetchOutcome::Eof => break,
                    FetchOutcome::Err(error) => {
                        cancel_attempted(self.dispatcher.as_ref(), &attempted);
                        return Err(failed(error));
                    }
                }
            }
        }
        let outcome = (|| {
            let result = expected_output.into_query_result(batches)?;
            match intent {
                DistributedQueryIntent::Result => parts.completion.result(result),
                DistributedQueryIntent::Write => {
                    let mut builder = WriteReportBuilder::new(writer_registrations)?;
                    for report in reports {
                        builder.apply(report)?;
                    }
                    let (commit, abort) = builder.finish()?.into_payloads();
                    parts.completion.write(result, commit, abort)
                }
                DistributedQueryIntent::Profile => {
                    let mut builder = ProfileReportBuilder::new();
                    for report in reports {
                        builder.apply(report)?;
                    }
                    parts.completion.profile(result, builder.finish())
                }
            }
        })();
        if outcome.is_err() {
            cancel_attempted(self.dispatcher.as_ref(), &attempted);
        }
        outcome
    }
}

fn cancel_attempted(
    dispatcher: &dyn FragmentDispatcher,
    attempted: &BTreeMap<usize, Vec<UniqueId>>,
) {
    for (backend_idx, finst_ids) in attempted {
        dispatcher.cancel_fragments(*backend_idx, finst_ids);
    }
}

fn failed(message: impl Into<String>) -> DistributedQueryError {
    DistributedQueryError::new(DistributedQueryErrorKind::Failed, message)
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};

    use novarocks::UniqueId;
    use novarocks::query_execution::contract::QueryId;
    use novarocks::query_execution::contract_test_support::{
        assert_profile_outcome_preserved, assert_result_outcome_preserved,
        assert_write_outcome_preserved, non_empty_profile_contract_fixture,
        non_empty_result_contract_fixture, non_empty_write_contract_fixture,
    };
    use novarocks::query_execution::fragment_transport::{
        FetchOutcome, FetchedQueryBatch, FragmentDispatcher, NativeFragmentEnvelope,
    };

    use super::FrontendContractProbe;
    use crate::coordinator::scheduler::{FrontendBackendSnapshot, FrontendFragmentScheduler};

    fn report_endpoint() -> SocketAddr {
        SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 19040)
    }

    #[derive(Default)]
    struct RecordingDispatcher {
        submissions: Mutex<Vec<(usize, UniqueId)>>,
        submission_reporting: Mutex<Vec<(bool, bool)>>,
        fetches: Mutex<Vec<(usize, UniqueId)>>,
        cancellations: Mutex<Vec<(usize, Vec<UniqueId>)>>,
        outcomes: Mutex<VecDeque<FetchOutcome>>,
        cancel_on_submit: Mutex<Option<(usize, Arc<AtomicBool>)>>,
    }

    impl RecordingDispatcher {
        fn with_result(batch: FetchedQueryBatch) -> Self {
            Self {
                outcomes: Mutex::new(VecDeque::from([
                    FetchOutcome::Ready(batch),
                    FetchOutcome::Eof,
                ])),
                ..Self::default()
            }
        }

        fn with_result_and_cancellation(
            batch: FetchedQueryBatch,
            cancellation: Arc<AtomicBool>,
            submit_count: usize,
        ) -> Self {
            Self {
                outcomes: Mutex::new(VecDeque::from([
                    FetchOutcome::Ready(batch),
                    FetchOutcome::Eof,
                ])),
                cancel_on_submit: Mutex::new(Some((submit_count, cancellation))),
                ..Self::default()
            }
        }
    }

    impl FragmentDispatcher for RecordingDispatcher {
        fn submit_fragment(
            &self,
            backend_idx: usize,
            submission: NativeFragmentEnvelope,
        ) -> Result<(), String> {
            let mut submissions = self.submissions.lock().unwrap();
            submissions.push((backend_idx, submission.fragment_instance_id()?));
            self.submission_reporting.lock().unwrap().push((
                submission.has_report_endpoint(),
                submission.uses_typed_result_sink(),
            ));
            let should_cancel = self
                .cancel_on_submit
                .lock()
                .unwrap()
                .as_ref()
                .is_some_and(|(submit_count, _)| submissions.len() == *submit_count);
            if should_cancel {
                if let Some((_, cancellation)) = self.cancel_on_submit.lock().unwrap().take() {
                    cancellation.store(true, Ordering::SeqCst);
                }
            }
            Ok(())
        }

        fn fetch_result(
            &self,
            backend_idx: usize,
            finst_id: UniqueId,
            _max_wait_ms: i64,
            _expected_output_schema: Option<
                novarocks::query_execution::fragment_transport::ExpectedOutputSchemaView<'_>,
            >,
        ) -> Result<FetchOutcome, String> {
            self.fetches.lock().unwrap().push((backend_idx, finst_id));
            Ok(self
                .outcomes
                .lock()
                .unwrap()
                .pop_front()
                .unwrap_or(FetchOutcome::Eof))
        }

        fn cancel_fragments(&self, backend_idx: usize, finst_ids: &[UniqueId]) {
            self.cancellations
                .lock()
                .unwrap()
                .push((backend_idx, finst_ids.to_vec()));
        }

        fn backend_count(&self) -> usize {
            2
        }

        fn needs_fragment_status_report(&self) -> bool {
            true
        }
    }

    #[test]
    fn frontend_consumes_non_empty_result_contract() {
        let fixture = non_empty_result_contract_fixture();
        let backends = fixture.backends().to_vec();
        let batch = fixture.result_batch();
        let request = fixture.into_request();
        let dispatcher = Arc::new(RecordingDispatcher::with_result(batch));
        let scheduler =
            FrontendFragmentScheduler::new(FrontendBackendSnapshot::new(backends).unwrap());
        let probe = FrontendContractProbe::new(
            QueryId::new(41, 73),
            report_endpoint(),
            scheduler,
            dispatcher.clone(),
        );

        let outcome = probe.execute(request).expect("frontend executes fixture");

        assert_result_outcome_preserved(outcome, 1).expect("engine consumes Result payload");
        assert!(!dispatcher.submissions.lock().unwrap().is_empty());
        let reporting = dispatcher.submission_reporting.lock().unwrap();
        assert!(reporting.iter().all(|(has_endpoint, _)| *has_endpoint));
        assert_eq!(
            reporting
                .iter()
                .filter(|(_, typed_result_sink)| *typed_result_sink)
                .count(),
            1
        );
        assert!(!dispatcher.fetches.lock().unwrap().is_empty());
    }

    #[test]
    fn frontend_returns_non_empty_write_contract() {
        let fixture = non_empty_write_contract_fixture();
        let backends = fixture.backends().to_vec();
        let report = fixture.successful_writer_report();
        let request = fixture.into_request();
        let dispatcher = Arc::new(RecordingDispatcher::default());
        let scheduler =
            FrontendFragmentScheduler::new(FrontendBackendSnapshot::new(backends).unwrap());
        let probe = FrontendContractProbe::new(
            QueryId::new(51, 91),
            report_endpoint(),
            scheduler,
            dispatcher.clone(),
        )
        .with_native_reports(vec![report]);

        let outcome = probe
            .execute(request)
            .expect("frontend executes write fixture");

        assert_write_outcome_preserved(outcome).expect("engine consumes Write payload");
        assert!(!dispatcher.submissions.lock().unwrap().is_empty());
        assert!(dispatcher.fetches.lock().unwrap().is_empty());
    }

    #[test]
    fn frontend_returns_non_empty_profile_contract() {
        let fixture = non_empty_profile_contract_fixture();
        let backends = fixture.backends().to_vec();
        let batch = fixture.result_batch();
        let report = fixture.fragment_profile_report();
        let request = fixture.into_request();
        let dispatcher = Arc::new(RecordingDispatcher::with_result(batch));
        let scheduler =
            FrontendFragmentScheduler::new(FrontendBackendSnapshot::new(backends).unwrap());
        let probe = FrontendContractProbe::new(
            QueryId::new(61, 101),
            report_endpoint(),
            scheduler,
            dispatcher.clone(),
        )
        .with_native_reports(vec![report]);

        let outcome = probe
            .execute(request)
            .expect("frontend executes profile fixture");

        assert_profile_outcome_preserved(outcome, 1)
            .expect("engine consumes non-empty Profile payload");
        assert!(!dispatcher.submissions.lock().unwrap().is_empty());
        assert!(!dispatcher.fetches.lock().unwrap().is_empty());
    }

    #[test]
    fn frontend_contract_cancels_only_through_dispatcher() {
        let fixture = non_empty_result_contract_fixture();
        let backends = fixture.backends().to_vec();
        let cancellation = fixture.cancellation_flag();
        let batch = fixture.result_batch();
        let request = fixture.into_request();
        let dispatcher = Arc::new(RecordingDispatcher::with_result_and_cancellation(
            batch,
            cancellation,
            2,
        ));
        let local_cleanup_calls = AtomicUsize::new(0);
        let scheduler =
            FrontendFragmentScheduler::new(FrontendBackendSnapshot::new(backends).unwrap());
        let probe = FrontendContractProbe::new(
            QueryId::new(41, 73),
            report_endpoint(),
            scheduler,
            dispatcher.clone(),
        );

        let error = match probe.execute(request) {
            Ok(_) => panic!("cancellation must stop execution"),
            Err(error) => error,
        };

        assert_eq!(
            error.kind(),
            novarocks::query_execution::contract::DistributedQueryErrorKind::Failed
        );
        assert_eq!(dispatcher.submissions.lock().unwrap().len(), 2);
        assert!(!dispatcher.cancellations.lock().unwrap().is_empty());
        assert!(dispatcher.fetches.lock().unwrap().is_empty());
        assert_eq!(local_cleanup_calls.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn failed_report_cancels_via_dispatcher_without_local_cleanup() {
        let fixture = non_empty_result_contract_fixture();
        let backends = fixture.backends().to_vec();
        let batch = fixture.result_batch();
        let report = fixture.failed_fragment_report();
        let request = fixture.into_request();
        let dispatcher = Arc::new(RecordingDispatcher::with_result(batch));
        let local_cleanup_calls = AtomicUsize::new(0);
        let scheduler =
            FrontendFragmentScheduler::new(FrontendBackendSnapshot::new(backends).unwrap());
        let probe = FrontendContractProbe::new(
            QueryId::new(41, 73),
            report_endpoint(),
            scheduler,
            dispatcher.clone(),
        )
        .with_native_reports(vec![report]);

        let error = match probe.execute(request) {
            Ok(_) => panic!("failed native report must fail execution"),
            Err(error) => error,
        };

        assert_eq!(
            error.kind(),
            novarocks::query_execution::contract::DistributedQueryErrorKind::Failed
        );
        assert_eq!(
            probe.failure_message().as_deref(),
            Some("contract native failure")
        );
        assert!(!dispatcher.submissions.lock().unwrap().is_empty());
        assert!(!dispatcher.cancellations.lock().unwrap().is_empty());
        assert!(dispatcher.fetches.lock().unwrap().is_empty());
        assert_eq!(local_cleanup_calls.load(Ordering::SeqCst), 0);

        let write_fixture = non_empty_write_contract_fixture();
        let backends = write_fixture.backends().to_vec();
        let report = write_fixture.failed_writer_report();
        let request = write_fixture.into_request();
        let dispatcher = Arc::new(RecordingDispatcher::default());
        let scheduler =
            FrontendFragmentScheduler::new(FrontendBackendSnapshot::new(backends).unwrap());
        let probe = FrontendContractProbe::new(
            QueryId::new(51, 91),
            report_endpoint(),
            scheduler,
            dispatcher.clone(),
        )
        .with_native_reports(vec![report]);

        let outcome = probe
            .execute(request)
            .expect("failed writer report returns abort payload");

        assert_write_outcome_preserved(outcome).expect("engine preserves Write abort payload");
        assert_eq!(
            probe.failure_message().as_deref(),
            Some("contract writer failure")
        );
        assert!(!dispatcher.cancellations.lock().unwrap().is_empty());
        assert!(dispatcher.fetches.lock().unwrap().is_empty());
    }
}
