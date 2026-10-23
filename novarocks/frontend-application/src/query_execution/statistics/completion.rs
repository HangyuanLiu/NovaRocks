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

//! Statistics publication requires the original complete success-compatible Task set.

use crate::query_execution::contract::{DistributedQueryError, DistributedQueryErrorKind};
use crate::task_execution::{round::TaskRound, termination::render_termination_detail};
use novarocks_execution_contract::{TaskIdentity, TaskState, TerminationDetail};

/// Incremental observation of the frozen statistics Task set. The existing
/// success-compatible rule below remains the sole publication predicate.
#[derive(Debug)]
pub(crate) struct StatisticsProtocolTracker {
    provenance: crate::task_execution::protocol_facts::AttemptProtocolProvenance,
    revision: u64,
    facts: Box<[StatisticsTaskCompletionFact]>,
    nonterminal: usize,
    incompatible: usize,
    selected_failure: Option<novarocks_coordination_contract::SelectedTaskTermination>,
}
impl StatisticsProtocolTracker {
    pub(crate) fn from_initial(
        changes: &crate::task_execution::protocol_facts::AcceptedAttemptProtocolChanges,
    ) -> Result<Self, crate::task_execution::error::TaskExecutionError> {
        let provenance =
            crate::task_execution::protocol_facts::AttemptProtocolProvenance::from_initial(
                changes,
            )?;
        let facts: Box<[_]> = provenance
            .tasks()
            .iter()
            .map(|&identity| StatisticsTaskCompletionFact {
                identity,
                terminal: false,
                success_compatible_terminal: false,
                state: None,
                failure_cause: None,
            })
            .collect();
        let mut tracker = Self {
            nonterminal: facts.len(),
            incompatible: facts.len(),
            facts,
            provenance,
            revision: 0,
            selected_failure: None,
        };
        tracker.observe(changes)?;
        Ok(tracker)
    }
    pub(crate) fn observe(
        &mut self,
        changes: &crate::task_execution::protocol_facts::AcceptedAttemptProtocolChanges,
    ) -> Result<(), crate::task_execution::error::TaskExecutionError> {
        use crate::task_execution::error::TaskExecutionError;
        self.provenance.verify(changes)?;
        if changes.revision() < self.revision {
            return Err(TaskExecutionError::Schedule(
                "statistics protocol revision moved backwards".into(),
            ));
        }
        if changes.revision() == self.revision {
            return Ok(());
        }
        for change in changes.tasks() {
            let fact = self
                .facts
                .get_mut(change.index())
                .filter(|fact| fact.identity == change.identity())
                .ok_or_else(|| {
                    TaskExecutionError::Schedule(
                        "statistics protocol delta names a foreign frozen slot".into(),
                    )
                })?;
            self.nonterminal -= usize::from(!fact.terminal);
            self.incompatible -= usize::from(!statistics_task_is_success_compatible(fact));
            fact.terminal = change
                .status()
                .is_some_and(|status| status.state().is_terminal());
            fact.success_compatible_terminal = change
                .status()
                .is_some_and(|status| status.is_success_compatible_terminal());
            fact.state = change.status().map(|status| status.state());
            fact.failure_cause = change
                .status()
                .and_then(|status| status.termination())
                .cloned();
            self.nonterminal += usize::from(!fact.terminal);
            self.incompatible += usize::from(!statistics_task_is_success_compatible(fact));
        }
        self.selected_failure = changes.selected_failure().cloned();
        self.revision = changes.revision();
        Ok(())
    }
    pub(crate) const fn all_terminal(&self) -> bool {
        self.nonterminal == 0
    }
    pub(crate) fn all_success_compatible(&self) -> bool {
        self.incompatible == 0 && self.selected_failure.is_none()
    }
    pub(crate) fn selected_failure(
        &self,
    ) -> Option<&novarocks_coordination_contract::SelectedTaskTermination> {
        self.selected_failure.as_ref()
    }
    pub(crate) fn failure_message(&self) -> Option<String> {
        statistics_all_success_failure_message(
            &self.facts,
            self.selected_failure
                .as_ref()
                .map(|selected| selected.detail()),
        )
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct StatisticsTaskCompletionFact {
    identity: TaskIdentity,
    terminal: bool,
    success_compatible_terminal: bool,
    state: Option<TaskState>,
    failure_cause: Option<TerminationDetail>,
}

pub(crate) fn statistics_tasks_are_terminal(round: &TaskRound) -> bool {
    round.execution().graph().tasks().all(|task| {
        round
            .execution()
            .task(task.task_id())
            .is_some_and(|remote| remote.is_terminal())
    })
}

pub(crate) fn statistics_all_success_error(round: &TaskRound) -> Option<DistributedQueryError> {
    let facts = round
        .execution()
        .graph()
        .tasks()
        .map(|task| {
            let remote = round.execution().task(task.task_id());
            StatisticsTaskCompletionFact {
                identity: task.identity(),
                terminal: remote.is_some_and(|remote| remote.is_terminal()),
                success_compatible_terminal: remote
                    .and_then(|remote| remote.status())
                    .is_some_and(|status| status.is_success_compatible_terminal()),
                state: remote.map(|remote| remote.task_state()),
                failure_cause: remote
                    .and_then(|remote| remote.status())
                    .and_then(|status| status.termination())
                    .cloned(),
            }
        })
        .collect::<Vec<_>>();
    statistics_all_success_failure_message(&facts, round.failure_cause()).map(|message| {
        DistributedQueryError::new(DistributedQueryErrorKind::ContractViolation, message)
    })
}

fn statistics_all_success_failure_message(
    facts: &[StatisticsTaskCompletionFact],
    round_failure_cause: Option<&TerminationDetail>,
) -> Option<String> {
    let incompatible = facts
        .iter()
        .filter(|fact| !statistics_task_is_success_compatible(fact))
        .map(format_statistics_task_completion_fact)
        .collect::<Vec<_>>();
    if incompatible.is_empty() && round_failure_cause.is_none() {
        return None;
    }
    Some(format!(
        "statistics execution did not reach a success-compatible terminal set; \
         incompatible_tasks=[{}]; round_failure_cause={}",
        incompatible.join(", "),
        render_termination_detail(round_failure_cause),
    ))
}

/// Whether this task's terminal is compatible with the already-complete Root
/// result remaining successful.
///
/// The Root reached `FINISHED` before this predicate can run. A non-root task
/// may then be stood down with `UPSTREAM_NO_LONGER_NEEDED`: the Root aggregate
/// has already consumed every sender through exchange EOS, so that terminal
/// closes work the result no longer depends on. An abort, a failure, a missing
/// status, or a task that has not reached a terminal still refuses statistics
/// publication.
fn statistics_task_is_success_compatible(fact: &StatisticsTaskCompletionFact) -> bool {
    fact.terminal && fact.success_compatible_terminal
}

fn format_statistics_task_completion_fact(fact: &StatisticsTaskCompletionFact) -> String {
    let execution_id = fact.identity.query_execution_id();
    format!(
        "{{identity={{query_id={}, attempt_id={}, stage_id={}, task_id={}, \
         backend_process_id={}}}, terminal={}, state={}, failure_cause={}}}",
        execution_id.query_id(),
        execution_id.attempt_id().get(),
        fact.identity.stage_id(),
        fact.identity.task_id(),
        fact.identity.backend_process_id(),
        fact.terminal,
        fact.state.map_or("MISSING", TaskState::as_str),
        render_termination_detail(fact.failure_cause.as_ref()),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use novarocks_execution_contract::{AbortCause, CancelReason, SafeDetail, TaskFailure};
    use novarocks_types::{
        AttemptId, BackendProcessId, QueryExecutionId, QueryId, StageId, TaskId,
    };

    #[test]
    fn statistics_failure_preserves_every_incompatible_task_and_round_cause() {
        let execution_id = QueryExecutionId::new(
            QueryId::new(7, 11),
            AttemptId::new(2).expect("nonzero attempt"),
        )
        .expect("nonzero execution identity");
        let stage_id = StageId::new(3).expect("nonzero stage");
        let finished_identity = TaskIdentity::new(
            execution_id,
            stage_id,
            TaskId::new(1).expect("nonzero task"),
            BackendProcessId::new_v7(),
        );
        let failed_identity = TaskIdentity::new(
            execution_id,
            stage_id,
            TaskId::new(2).expect("nonzero task"),
            BackendProcessId::new_v7(),
        );
        let aborted_identity = TaskIdentity::new(
            execution_id,
            stage_id,
            TaskId::new(3).expect("nonzero task"),
            BackendProcessId::new_v7(),
        );
        let task_failure = TerminationDetail::Failed(TaskFailure::new(
            novarocks_execution_contract::failure::FailureFact::new(
                novarocks_execution_contract::failure::FailureClass::Resource,
                novarocks_execution_contract::failure::FailureReason::new(
                    std::num::NonZeroU16::new(u16::MAX).unwrap(),
                    std::num::NonZeroU16::new(5).unwrap(),
                ),
            ),
            SafeDetail::new("memory pool exhausted").expect("bounded safe detail"),
        ));
        let facts = vec![
            StatisticsTaskCompletionFact {
                identity: finished_identity,
                terminal: true,
                success_compatible_terminal: true,
                state: Some(TaskState::Finished),
                failure_cause: None,
            },
            StatisticsTaskCompletionFact {
                identity: failed_identity,
                terminal: true,
                success_compatible_terminal: false,
                state: Some(TaskState::Failed),
                failure_cause: Some(task_failure.clone()),
            },
            StatisticsTaskCompletionFact {
                identity: aborted_identity,
                terminal: true,
                success_compatible_terminal: false,
                state: Some(TaskState::Aborted),
                failure_cause: Some(TerminationDetail::Aborted(AbortCause::PeerTaskFailed)),
            },
        ];

        let message = statistics_all_success_failure_message(&facts, Some(&task_failure))
            .expect("non-finished tasks reject statistics completion");
        assert!(!message.contains("task_id=1,"));
        assert!(message.contains(&format!(
            "identity={{query_id={}, attempt_id=2, stage_id=3, task_id=2, backend_process_id={}}}, terminal=true, state=FAILED, failure_cause=FAILED(category=RESOURCE_EXHAUSTED, phase=EXECUTION, detail=memory pool exhausted)",
            execution_id.query_id(),
            failed_identity.backend_process_id(),
        )));
        assert!(message.contains(&format!(
            "identity={{query_id={}, attempt_id=2, stage_id=3, task_id=3, backend_process_id={}}}, terminal=true, state=ABORTED, failure_cause=ABORTED(cause=PEER_TASK_FAILED)",
            execution_id.query_id(),
            aborted_identity.backend_process_id(),
        )));
        assert!(message.ends_with(
            "round_failure_cause=FAILED(category=RESOURCE_EXHAUSTED, phase=EXECUTION, detail=memory pool exhausted)"
        ));
        assert!(
            statistics_all_success_failure_message(&facts[..1], None).is_none(),
            "a FINISHED task set with no round failure is successful"
        );
    }

    #[test]
    fn statistics_accepts_only_the_closed_success_compatible_cancel_terminal() {
        let execution_id = QueryExecutionId::new(
            QueryId::new(13, 17),
            AttemptId::new(1).expect("nonzero attempt"),
        )
        .expect("nonzero execution identity");
        let stage_id = StageId::new(2).expect("nonzero stage");
        let finished_root = StatisticsTaskCompletionFact {
            identity: TaskIdentity::new(
                execution_id,
                stage_id,
                TaskId::new(1).expect("nonzero task"),
                BackendProcessId::new_v7(),
            ),
            terminal: true,
            success_compatible_terminal: true,
            state: Some(TaskState::Finished),
            failure_cause: None,
        };
        let canceled_producer = StatisticsTaskCompletionFact {
            identity: TaskIdentity::new(
                execution_id,
                stage_id,
                TaskId::new(2).expect("nonzero task"),
                BackendProcessId::new_v7(),
            ),
            terminal: true,
            success_compatible_terminal: true,
            state: Some(TaskState::Canceled),
            failure_cause: Some(TerminationDetail::Canceled(
                CancelReason::UpstreamNoLongerNeeded,
            )),
        };
        assert!(
            statistics_all_success_failure_message(
                &[finished_root.clone(), canceled_producer],
                None,
            )
            .is_none(),
            "a producer stood down after Root EOF is success-compatible"
        );

        let aborted_producer = StatisticsTaskCompletionFact {
            identity: TaskIdentity::new(
                execution_id,
                stage_id,
                TaskId::new(3).expect("nonzero task"),
                BackendProcessId::new_v7(),
            ),
            terminal: true,
            success_compatible_terminal: false,
            state: Some(TaskState::Aborted),
            failure_cause: Some(TerminationDetail::Aborted(AbortCause::QueryFailed)),
        };
        let message =
            statistics_all_success_failure_message(&[finished_root, aborted_producer], None)
                .expect("query cancellation is not a success-compatible stand-down");
        assert!(message.contains("ABORTED(cause=QUERY_FAILED)"));
    }

    #[test]
    fn statistics_does_not_classify_a_cancel_until_it_reaches_terminal() {
        let execution_id = QueryExecutionId::new(
            QueryId::new(19, 23),
            AttemptId::new(1).expect("nonzero attempt"),
        )
        .expect("nonzero execution identity");
        let canceling = StatisticsTaskCompletionFact {
            identity: TaskIdentity::new(
                execution_id,
                StageId::new(2).expect("nonzero stage"),
                TaskId::new(1).expect("nonzero task"),
                BackendProcessId::new_v7(),
            ),
            terminal: false,
            success_compatible_terminal: false,
            state: Some(TaskState::Canceling),
            failure_cause: Some(TerminationDetail::Canceled(
                CancelReason::UpstreamNoLongerNeeded,
            )),
        };
        let message = statistics_all_success_failure_message(&[canceling], None)
            .expect("a canceling task is not yet classifiable as success");
        assert!(message.contains("terminal=false"));
        assert!(message.contains("state=CANCELING"));
    }
}
