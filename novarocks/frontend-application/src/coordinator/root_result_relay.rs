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

//! The synchronous domain coordinator consumes the same bounded root frontier
//! as the client actor. It owns one outstanding delivery, not an Arrow stream.

use crate::{
    native::data_runtime::FrontendDataRuntime, task_execution::status_intake::StatusIntakeWake,
};
use novarocks_execution_contract::{
    TaskIdentity,
    root_result::{RootResultEnd, RootResultRead},
};
use novarocks_query_application::{
    api::{BoundedRootReadPort, RetainedRootReply, RootReplyView},
    coordination::{RootRelayFrontier, RootRelayStep, RootResultFetchFailure},
};
use novarocks_result_contract::{RootOutputKind, RootProfileId};
use novarocks_workload_control::ResultWindowAlias;
use std::{
    num::NonZeroU64,
    sync::Arc,
    time::{Duration, Instant},
};

pub(super) enum RelayedRootAnswer {
    Data(RetainedRootReply),
    End(RootResultEnd),
    NotReady,
    AwaitTerminalControl,
    FetchFailure(RootResultFetchFailure),
    Refused(String),
}

pub(super) struct RelayedRootPolls {
    answers: tokio::sync::mpsc::Receiver<RelayedRootAnswer>,
    receipts: tokio::sync::mpsc::Sender<NonZeroU64>,
    task: tokio::task::JoinHandle<()>,
}
impl RelayedRootPolls {
    pub(super) fn start(
        port: Arc<dyn BoundedRootReadPort>,
        root: TaskIdentity,
        kind: RootOutputKind,
        window: ResultWindowAlias,
        deadline: Instant,
        wake: Arc<dyn StatusIntakeWake>,
        runtime: FrontendDataRuntime,
    ) -> Result<Self, String> {
        if window.class() != novarocks_workload_control::ResultWindowClass::Internal {
            return Err("internal root reader requires its admitted Internal window".into());
        }
        let frontier = RootRelayFrontier::new(root, RootProfileId::V1, kind, None)
            .map_err(|error| error.to_string())?;
        let (sender, answers) = tokio::sync::mpsc::channel(1);
        let (receipts, completed) = tokio::sync::mpsc::channel(1);
        let task = runtime.spawn(drive(
            port, frontier, window, deadline, wake, sender, completed,
        ));
        Ok(Self {
            answers,
            receipts,
            task,
        })
    }
    pub(super) fn take(&mut self) -> Option<RelayedRootAnswer> {
        self.answers.try_recv().ok()
    }
    /// Caller has validated the domain facts and dropped the retained body.
    pub(super) fn acknowledge(&self, sequence: NonZeroU64) -> Result<(), String> {
        self.receipts
            .try_send(sequence)
            .map_err(|error| format!("root receipt refused: {error}"))
    }
}
impl Drop for RelayedRootPolls {
    fn drop(&mut self) {
        self.task.abort();
    }
}

#[expect(
    clippy::too_many_arguments,
    reason = "Each owner of this bounded poll is explicit."
)]
async fn drive(
    port: Arc<dyn BoundedRootReadPort>,
    mut frontier: RootRelayFrontier,
    window: ResultWindowAlias,
    deadline: Instant,
    wake: Arc<dyn StatusIntakeWake>,
    sender: tokio::sync::mpsc::Sender<RelayedRootAnswer>,
    mut receipts: tokio::sync::mpsc::Receiver<NonZeroU64>,
) {
    let send = |answer| async {
        let result = sender.send(answer).await;
        wake.wake();
        result
    };
    loop {
        if let Some(end) = frontier.end_consumed() {
            let _ = send(RelayedRootAnswer::End(end)).await;
            return;
        }
        let now = Instant::now();
        if now >= deadline {
            let _ = send(RelayedRootAnswer::Refused(
                "root read deadline expired".into(),
            ))
            .await;
            return;
        }
        // The wire wait is expressed in whole milliseconds. Round down so the
        // request cannot extend the admitted absolute deadline. If no whole
        // millisecond remains, wait for that exact deadline without issuing RPC.
        let wait = Duration::from_millis(
            deadline
                .saturating_duration_since(now)
                .min(super::execution::MAX_ROOT_RESULT_WAIT)
                .as_millis() as u64,
        );
        if wait.is_zero() {
            tokio::time::sleep_until(deadline.into()).await;
            let _ = send(RelayedRootAnswer::Refused(
                "root read deadline expired".into(),
            ))
            .await;
            return;
        }
        let Some(read) = frontier.next_read() else {
            let _ = send(RelayedRootAnswer::Refused(
                "root reader has no next frontier".into(),
            ))
            .await;
            return;
        };
        let request = match RootResultRead::try_new(
            frontier.root(),
            frontier.profile(),
            frontier.kind(),
            read.wanted,
            read.consumed,
            wait,
        ) {
            Ok(request) => request,
            Err(error) => {
                let _ = send(RelayedRootAnswer::Refused(error.to_string())).await;
                return;
            }
        };
        frontier.sent(read);
        let reply = match port.read(request, window.clone()).await {
            Ok(reply) => reply,
            Err(error) => {
                let _ = send(RelayedRootAnswer::FetchFailure(error)).await;
                return;
            }
        };
        let body = match reply.outcome() {
            RootReplyView::Data { body, .. } => Some(body),
            _ => None,
        };
        let step = match frontier.accept(reply.reply(), body) {
            Ok(step) => step,
            Err(error) => {
                let _ = send(RelayedRootAnswer::Refused(error.to_string())).await;
                return;
            }
        };
        match step {
            RootRelayStep::Deliver { sequence, .. } => {
                if send(RelayedRootAnswer::Data(reply)).await.is_err() {
                    return;
                }
                match receipts.recv().await {
                    Some(actual) if actual == sequence => {
                        if let Err(error) = frontier.receipt(actual) {
                            let _ = send(RelayedRootAnswer::Refused(error.to_string())).await;
                            return;
                        }
                    }
                    Some(_) => {
                        let _ = send(RelayedRootAnswer::Refused(
                            "root receipt sequence mismatch".into(),
                        ))
                        .await;
                        return;
                    }
                    None => return,
                }
            }
            RootRelayStep::EndKnown(_) | RootRelayStep::Acknowledged => {
                drop(reply);
            }
            RootRelayStep::NotReady => {
                drop(reply);
                if send(RelayedRootAnswer::NotReady).await.is_err() {
                    return;
                }
                tokio::task::yield_now().await;
            }
            RootRelayStep::AwaitTerminalControl => {
                drop(reply);
                let _ = send(RelayedRootAnswer::AwaitTerminalControl).await;
                return;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use novarocks_execution_contract::{
        root_lifetime::RootReadSealed,
        root_result::{RootReadOutcome, RootResultData, RootResultReply},
    };
    use novarocks_query_application::{
        api::{QueryExecutionError, QueryExecutionErrorKind},
        coordination::AttemptFailureClass,
    };
    use novarocks_result_contract::InternalResultDomain;
    use novarocks_types::{
        AttemptId, BackendProcessId, QueryExecutionId, QueryId, StageId, TaskId,
    };
    use novarocks_workload_control::{
        ResourceConfig, ResultCapacityConfig, ResultCapacityHandle, ResultWindowClass,
        ResultWindowGrant, RootWork, WorkClass, WorkRequest, WorkloadConfig, WorkloadControl,
    };
    use std::{
        collections::VecDeque,
        future::Future,
        pin::Pin,
        sync::{
            Mutex,
            atomic::{AtomicBool, Ordering},
        },
        time::Duration,
    };

    #[derive(Debug)]
    struct Wake;
    impl StatusIntakeWake for Wake {
        fn wake(&self) {}
    }
    struct Port {
        outcomes: Mutex<VecDeque<RootReadOutcome>>,
        reads: Mutex<Vec<(Option<u64>, u64)>>,
        waits: Mutex<Vec<Duration>>,
        sealed: AtomicBool,
    }
    impl Port {
        fn new(outcomes: Vec<RootReadOutcome>) -> Arc<Self> {
            Arc::new(Self {
                outcomes: Mutex::new(outcomes.into()),
                reads: Mutex::new(Vec::new()),
                waits: Mutex::new(Vec::new()),
                sealed: AtomicBool::new(false),
            })
        }
    }
    impl BoundedRootReadPort for Port {
        fn read(
            &self,
            request: RootResultRead,
            guard: ResultWindowAlias,
        ) -> Pin<Box<dyn Future<Output = Result<RetainedRootReply, RootResultFetchFailure>> + Send>>
        {
            assert!(
                !self.sealed.load(Ordering::Acquire),
                "no read may start after seal"
            );
            self.reads
                .lock()
                .unwrap()
                .push((request.wanted().map(|n| n.get()), request.consumed()));
            self.waits.lock().unwrap().push(request.max_wait());
            let outcome = self
                .outcomes
                .lock()
                .unwrap()
                .pop_front()
                .unwrap_or(RootReadOutcome::NotReady);
            let reply = RootResultReply {
                root_task: request.root_task(),
                profile: request.profile(),
                kind: request.kind(),
                accepted_consumed: request.consumed(),
                outcome,
            };
            Box::pin(async move {
                RetainedRootReply::try_new(reply, guard, 1024).map_err(|error| {
                    RootResultFetchFailure::new(
                        AttemptFailureClass::ContractViolation,
                        QueryExecutionError::new(
                            QueryExecutionErrorKind::InvalidRequest,
                            error.to_string(),
                        ),
                    )
                })
            })
        }
        fn seal(&self, _proof: RootReadSealed) -> Result<(), QueryExecutionError> {
            self.sealed.store(true, Ordering::Release);
            Ok(())
        }
    }
    fn window() -> (
        WorkloadControl,
        ResultCapacityHandle,
        RootWork,
        ResultWindowGrant,
    ) {
        let control = WorkloadControl::try_new(
            WorkloadConfig::default(),
            ResourceConfig {
                total_bytes: 1 << 20,
                control_bytes: 1 << 12,
                per_scope_bytes: 1 << 18,
            },
        )
        .unwrap();
        let capacity = control
            .configure_result_capacity(ResultCapacityConfig::V1)
            .unwrap();
        control.mark_ready().unwrap();
        let work = control
            .try_begin_root(WorkRequest::new(WorkClass::Query))
            .unwrap();
        let window = capacity
            .try_acquire(&work.owner.scope(), ResultWindowClass::Internal)
            .unwrap();
        (control, capacity, work, window)
    }
    fn root() -> TaskIdentity {
        TaskIdentity::new(
            QueryExecutionId::new(QueryId::new(1, 2), AttemptId::new(1).unwrap()).unwrap(),
            StageId::new(1).unwrap(),
            TaskId::new(1).unwrap(),
            BackendProcessId::new_v7(),
        )
    }
    async fn next(polls: &mut RelayedRootPolls) -> RelayedRootAnswer {
        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                if let Some(answer) = polls.take() {
                    return answer;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap()
    }
    fn data(sequence: u64, end: Option<(u64, u64)>) -> RootReadOutcome {
        RootReadOutcome::Data(
            RootResultData::try_new(
                RootOutputKind::InternalFacts(InternalResultDomain::StatisticsArtifactV1),
                NonZeroU64::new(sequence).unwrap(),
                bytes::Bytes::from_static(b"record"),
                end.map(|(n, rows)| RootResultEnd {
                    sequence: NonZeroU64::new(n).unwrap(),
                    output_rows: rows,
                }),
            )
            .unwrap(),
        )
    }
    fn start(
        port: Arc<Port>,
        root: TaskIdentity,
        kind: RootOutputKind,
        window: &ResultWindowGrant,
    ) -> RelayedRootPolls {
        RelayedRootPolls::start(
            port,
            root,
            kind,
            window.retain_alias(),
            Instant::now() + Duration::from_secs(2),
            Arc::new(Wake),
            FrontendDataRuntime::new(tokio::runtime::Handle::current()),
        )
        .unwrap()
    }
    #[tokio::test]
    async fn receipt_precedes_next_read_and_end_needs_no_final_backend_ack() {
        let (_control, _capacity, _work, window) = window();
        let port = Port::new(vec![data(1, None), data(2, Some((3, 2)))]);
        let mut polls = start(
            port.clone(),
            root(),
            RootOutputKind::InternalFacts(InternalResultDomain::StatisticsArtifactV1),
            &window,
        );
        let RelayedRootAnswer::Data(first) = next(&mut polls).await else {
            panic!("data");
        };
        assert!(
            tokio::time::timeout(Duration::from_millis(20), next(&mut polls))
                .await
                .is_err()
        );
        assert_eq!(*port.reads.lock().unwrap(), vec![(Some(1), 0)]);
        drop(first);
        polls.acknowledge(NonZeroU64::MIN).unwrap();
        let RelayedRootAnswer::Data(second) = next(&mut polls).await else {
            panic!("data");
        };
        assert_eq!(
            *port.reads.lock().unwrap(),
            vec![(Some(1), 0), (Some(2), 1)]
        );
        drop(second);
        polls.acknowledge(NonZeroU64::new(2).unwrap()).unwrap();
        let RelayedRootAnswer::End(end) = next(&mut polls).await else {
            panic!("end");
        };
        assert_eq!(end.output_rows, 2);
        assert_eq!(port.reads.lock().unwrap().len(), 2);
        assert!(
            !port.sealed.load(Ordering::Acquire),
            "the control owner must still join exact Finished"
        );
    }
    #[tokio::test]
    async fn count_only_has_no_data_and_preserves_exact_nonzero_count() {
        let (_control, _capacity, _work, window) = window();
        let port = Port::new(vec![RootReadOutcome::End(RootResultEnd {
            sequence: NonZeroU64::MIN,
            output_rows: 42,
        })]);
        let mut polls = start(port.clone(), root(), RootOutputKind::CountOnly, &window);
        let RelayedRootAnswer::End(end) = next(&mut polls).await else {
            panic!("only End");
        };
        assert_eq!(end.output_rows, 42);
        assert_eq!(*port.reads.lock().unwrap(), vec![(Some(1), 0)]);
    }
    #[tokio::test]
    async fn fractional_deadline_tail_uses_a_valid_bounded_wire_wait() {
        let (_control, _capacity, _work, window) = window();
        let port = Port::new(vec![RootReadOutcome::End(RootResultEnd {
            sequence: NonZeroU64::MIN,
            output_rows: 42,
        })]);
        let mut polls = RelayedRootPolls::start(
            port.clone(),
            root(),
            RootOutputKind::CountOnly,
            window.retain_alias(),
            Instant::now() + Duration::from_micros(100_900),
            Arc::new(Wake),
            FrontendDataRuntime::new(tokio::runtime::Handle::current()),
        )
        .unwrap();
        let RelayedRootAnswer::End(end) = next(&mut polls).await else {
            panic!("fractional deadline tail must not violate the wire contract");
        };
        assert_eq!(end.output_rows, 42);
        let waits = port.waits.lock().unwrap();
        assert_eq!(waits.len(), 1);
        assert!(waits[0] <= Duration::from_millis(100));
        assert!(!waits[0].is_zero());
        assert!(waits[0].subsec_nanos().is_multiple_of(1_000_000));
    }

    #[tokio::test]
    async fn submillisecond_deadline_tail_expires_without_issuing_a_read() {
        let (_control, _capacity, _work, window) = window();
        let port = Port::new(Vec::new());
        let deadline = Instant::now() + Duration::from_micros(900);
        let mut polls = RelayedRootPolls::start(
            port.clone(),
            root(),
            RootOutputKind::CountOnly,
            window.retain_alias(),
            deadline,
            Arc::new(Wake),
            FrontendDataRuntime::new(tokio::runtime::Handle::current()),
        )
        .unwrap();
        let RelayedRootAnswer::Refused(message) = next(&mut polls).await else {
            panic!("deadline tail must expire");
        };
        assert_eq!(message, "root read deadline expired");
        assert!(Instant::now() >= deadline);
        assert!(port.reads.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn wrong_receipt_refuses_and_last_delivery_alias_keeps_window() {
        let (_control, capacity, work, window) = window();
        let port = Port::new(vec![data(1, None)]);
        let mut polls = start(
            port.clone(),
            root(),
            RootOutputKind::InternalFacts(InternalResultDomain::StatisticsArtifactV1),
            &window,
        );
        let RelayedRootAnswer::Data(delivery) = next(&mut polls).await else {
            panic!("data");
        };
        polls.acknowledge(NonZeroU64::new(2).unwrap()).unwrap();
        let RelayedRootAnswer::Refused(message) = next(&mut polls).await else {
            panic!("wrong receipt must refuse");
        };
        assert!(message.contains("sequence"));
        drop((polls, window, work));
        tokio::task::yield_now().await;
        assert_eq!(capacity.snapshot().held_positions[2], 1);
        drop(delivery);
        assert_eq!(capacity.snapshot().held_positions[2], 0);
        assert_eq!(port.reads.lock().unwrap().len(), 1);
    }
    #[tokio::test]
    async fn terminal_cut_seals_before_revoking_poller_and_keeps_late_body_owner() {
        use super::super::execution::{RootResultPolls, close_failed_root_reads};
        let (_control, capacity, work, window) = window();
        let port = Port::new(vec![data(1, None)]);
        let root = root();
        let mut relay = start(
            port.clone(),
            root,
            RootOutputKind::InternalFacts(InternalResultDomain::StatisticsArtifactV1),
            &window,
        );
        let RelayedRootAnswer::Data(delivery) = next(&mut relay).await else {
            panic!("data");
        };
        let mut polls = Some(RootResultPolls(relay));
        let mut reader = Some(port.clone() as Arc<dyn BoundedRootReadPort>);
        close_failed_root_reads(&mut reader, &mut polls, root);
        assert!(port.sealed.load(Ordering::Acquire));
        assert!(reader.is_none());
        assert!(polls.is_none());
        drop((window, work));
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert_eq!(port.reads.lock().unwrap().len(), 1);
        assert_eq!(capacity.snapshot().held_positions[2], 1);
        drop(delivery);
        assert_eq!(capacity.snapshot().held_positions[2], 0);
    }

    #[test]
    fn fetch_verdict_keeps_kind_class_and_topology_without_retry_authority() {
        use crate::query_execution::contract::{DistributedQueryError, DistributedQueryErrorKind};
        use novarocks_query_application::api::NativeAttemptTopologyRequirement;
        for (class, kind, result_kind) in [
            (
                AttemptFailureClass::ResourceGovernance,
                QueryExecutionErrorKind::Rejected,
                DistributedQueryErrorKind::Rejected,
            ),
            (
                AttemptFailureClass::ContractViolation,
                QueryExecutionErrorKind::InvalidRequest,
                DistributedQueryErrorKind::ContractViolation,
            ),
            (
                AttemptFailureClass::RecoverableInfrastructure,
                QueryExecutionErrorKind::Failed,
                DistributedQueryErrorKind::Failed,
            ),
        ] {
            let failure =
                RootResultFetchFailure::new(class, QueryExecutionError::new(kind, "typed verdict"))
                    .with_topology_requirement(NativeAttemptTopologyRequirement::ExcludeProcess(
                        root().backend_process_id(),
                    ));
            let error =
                DistributedQueryError::new(DistributedQueryErrorKind::Failed, "attempt failed")
                    .with_root_fetch_failure(failure.clone());
            assert_eq!(error.kind(), result_kind);
            assert_eq!(error.root_fetch_failure(), Some(&failure));
            assert!(error.pre_ready_topology_outcome().is_none());
        }
    }
}
