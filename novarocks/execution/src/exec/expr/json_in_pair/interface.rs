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
use crate::exec::expr::agg::AggregateAllocator;
use crate::runtime::local_offset::LocalOffsetOwner;
use crate::runtime::mem_tracker::MemTracker;
use crate::runtime::runtime_state::{RuntimeErrorState, RuntimeState};
use chrono::FixedOffset;
use novarocks_execution_contract::{SafeDetail, TaskFailure, TaskFailureCategory, TaskIdentity};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum JsonPairTruth {
    True,
    False,
    Unknown,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum JsonPairPoll {
    Yield,
    Ready(JsonPairTruth),
}
#[derive(Debug)]
pub(crate) enum JsonPairError {
    Failed(TaskFailure),
    Stopped,
    Contract(&'static str),
}

/// Minted by the owner of immutable pending probe and build chunks.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct JsonPairInputId {
    pub probe_generation: u64,
    pub probe_row: usize,
    pub build_batch: usize,
    pub build_row: usize,
}
pub(crate) struct JsonPairInput<'a> {
    pub id: JsonPairInputId,
    /// None is SQL NULL; the text "null" is a non-null JSON value.
    pub lhs: Option<&'a str>,
    pub rhs: Option<&'a str>,
}

/// One allowance shared by every pair poll in the same driver turn.
pub(crate) struct JsonPairWork {
    remaining: usize,
}
impl JsonPairWork {
    pub(crate) const fn new(units: usize) -> Self {
        Self { remaining: units }
    }
    pub(crate) fn spend_one(&mut self) -> bool {
        if self.remaining == 0 {
            false
        } else {
            self.remaining -= 1;
            true
        }
    }
    #[cfg(test)]
    pub(crate) const fn exhausted(&self) -> bool {
        self.remaining == 0
    }
}

/// Reuses the native task's existing allocations and tracker. There is no
/// tracker-label inference, process fallback, or per-cursor child tracker.
///
/// It also binds the process-level local offset owner of the task's execution
/// runtime: governed Variant conversion reads the offset from that owner's
/// snapshot and never calls chrono `Local` on the driver thread.
#[derive(Clone)]
pub(crate) struct JsonPairTask {
    identity: TaskIdentity,
    task: Arc<MemTracker>,
    allocator: AggregateAllocator,
    errors: Arc<RuntimeErrorState>,
    local_offset: Arc<LocalOffsetOwner>,
}
impl JsonPairTask {
    fn local_offset_owner(state: &RuntimeState) -> Option<Arc<LocalOffsetOwner>> {
        state
            .execution_runtime()
            .map(|runtime| Arc::clone(runtime.local_offset_owner()))
    }
    pub(crate) fn try_bind(state: &RuntimeState) -> Result<Self, JsonPairError> {
        let (identity, task) = state.exact_task_tracker().ok_or(JsonPairError::Contract(
            "JSON membership requires an exact native task memory owner",
        ))?;
        let local_offset = Self::local_offset_owner(state).ok_or(JsonPairError::Contract(
            "JSON membership requires the execution runtime local offset owner",
        ))?;
        Ok(Self {
            identity,
            allocator: AggregateAllocator::new(Arc::clone(&task)),
            task,
            errors: state.error_state(),
            local_offset,
        })
    }
    pub(crate) fn validate_task(&self, state: &RuntimeState) -> Result<(), JsonPairError> {
        let (identity, tracker) = state.exact_task_tracker().ok_or(JsonPairError::Contract(
            "JSON membership task memory owner is missing",
        ))?;
        let local_offset = Self::local_offset_owner(state).ok_or(JsonPairError::Contract(
            "JSON membership local offset owner is missing",
        ))?;
        if self.identity == identity
            && Arc::ptr_eq(&self.task, &tracker)
            && Arc::ptr_eq(&self.errors, &state.error_state())
            && Arc::ptr_eq(&self.local_offset, &local_offset)
        {
            Ok(())
        } else {
            Err(JsonPairError::Contract(
                "JSON membership crossed task memory owners",
            ))
        }
    }
    pub(crate) fn same_owner(&self, other: &Self) -> bool {
        self.identity == other.identity
            && Arc::ptr_eq(&self.task, &other.task)
            && Arc::ptr_eq(&self.errors, &other.errors)
            && Arc::ptr_eq(&self.local_offset, &other.local_offset)
    }
    pub(crate) fn allocator(&self) -> &AggregateAllocator {
        &self.allocator
    }
    /// The exact native Task tracker every owner bound to this task charges.
    pub(crate) fn tracker(&self) -> &Arc<MemTracker> {
        &self.task
    }
    /// The task's own failure, if one has been published.
    pub(crate) fn task_failure(&self) -> Option<TaskFailure> {
        self.errors.task_failure()
    }
    /// The local offset in effect now, read once when a Variant conversion
    /// starts. Allocation-free on success.
    ///
    /// An expired snapshot is the task's own typed failure. There is no
    /// fallback to chrono `Local`, UTC or a session time zone, and the
    /// failure never selects the raw-text comparison.
    pub(crate) fn conversion_offset(&self) -> Result<FixedOffset, JsonPairError> {
        self.local_offset
            .offset_now()
            .map_err(|expired| JsonPairError::Failed(expired.into_task_failure()))
    }
    pub(crate) fn allocation_error(&self) -> JsonPairError {
        if let Some(failure) = self.errors.task_failure() {
            JsonPairError::Failed(failure)
        } else if self.errors.is_stopped() {
            JsonPairError::Stopped
        } else {
            JsonPairError::Failed(TaskFailure::new(
                TaskFailureCategory::ResourceExhausted,
                SafeDetail::truncating(
                    "JSON membership allocation was refused by the system allocator",
                ),
            ))
        }
    }
}

pub(crate) struct JsonPairContext<'a> {
    pub task: &'a JsonPairTask,
    pub stopped: &'a AtomicBool,
}
impl JsonPairContext<'_> {
    pub(crate) fn check_running(&self) -> Result<(), JsonPairError> {
        if self.stopped.load(Ordering::Acquire) || self.task.errors.is_stopped() {
            Err(JsonPairError::Stopped)
        } else {
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::super::test_support::{runtime_with_offset_owner, shared_runtime};
    use super::*;
    use crate::exec::expr::agg::AggregateVec;
    use crate::runtime::local_offset::{LocalOffsetStepRules, unix_seconds_floor};
    use crate::runtime::verification::TaskVerificationHolder;
    use novarocks_types::identity::{AttemptId, QueryExecutionId, QueryId};
    use novarocks_types::{BackendProcessId, StageId, TaskId};

    fn identity() -> TaskIdentity {
        TaskIdentity::new(
            QueryExecutionId::new(QueryId::new(7, 9), AttemptId::new(1).unwrap()).unwrap(),
            StageId::new(1).unwrap(),
            TaskId::new(1).unwrap(),
            BackendProcessId::new_v7(),
        )
    }
    fn task_state(id: TaskIdentity, tracker: Arc<MemTracker>) -> RuntimeState {
        super::super::test_support::task_state(id, tracker, Some(shared_runtime()))
    }
    #[test]
    fn json_in_pair_binding_requires_runtime_local_offset_owner() {
        let id = identity();
        let tracker = MemTracker::new_root("native-task");
        let without_runtime =
            super::super::test_support::task_state(id, Arc::clone(&tracker), None);
        assert!(matches!(
            JsonPairTask::try_bind(&without_runtime),
            Err(JsonPairError::Contract(message)) if message.contains("local offset owner")
        ));

        let state = task_state(id, Arc::clone(&tracker));
        let bound = JsonPairTask::try_bind(&state).unwrap();
        assert!(matches!(
            bound.validate_task(&without_runtime),
            Err(JsonPairError::Contract(message)) if message.contains("local offset owner")
        ));

        // Another owner is another process fact; a task never mixes them.
        let now = unix_seconds_floor(std::time::SystemTime::now());
        let east = |secs| FixedOffset::east_opt(secs).unwrap();
        let other_owner = LocalOffsetOwner::start(LocalOffsetStepRules {
            transition_at_utc: now + 3600,
            before: east(3600),
            after: east(3600),
        })
        .unwrap();
        let other_state = super::super::test_support::task_state(
            id,
            Arc::clone(&tracker),
            Some(runtime_with_offset_owner(Arc::clone(&other_owner))),
        );
        let other = JsonPairTask::try_bind(&other_state).unwrap();
        assert!(Arc::ptr_eq(&other.local_offset, &other_owner));
        assert_eq!(other.conversion_offset().unwrap(), east(3600));
        let mut swapped = bound.clone();
        swapped.local_offset = other_owner;
        assert!(bound.same_owner(&bound.clone()));
        assert!(!bound.same_owner(&swapped));
    }
    #[test]
    fn json_in_pair_binding_requires_identity_and_exact_tracker() {
        assert!(JsonPairTask::try_bind(&RuntimeState::default()).is_err());
        assert!(
            JsonPairTask::try_bind(
                &RuntimeState::default()
                    .with_verification(Arc::new(TaskVerificationHolder::new(identity())))
            )
            .is_err()
        );
        let tracker = MemTracker::new_root("native-task");
        let no_identity =
            RuntimeState::new(None, None, None, None, None, Some(tracker.clone()), None);
        assert!(JsonPairTask::try_bind(&no_identity).is_err());
        let id = identity();
        let state = task_state(id, tracker.clone());
        let bound = JsonPairTask::try_bind(&state).unwrap();
        bound.validate_task(&state.clone()).unwrap();
        assert!(bound.same_owner(&bound.clone()));
        assert!(
            bound
                .validate_task(&task_state(id, tracker.clone()))
                .is_err()
        );
        assert!(
            bound
                .validate_task(&task_state(identity(), tracker))
                .is_err()
        );
        assert!(
            bound
                .validate_task(&task_state(id, MemTracker::new_root("other-task")))
                .is_err()
        );
    }
    #[test]
    fn json_in_pair_allocation_preserves_typed_refusal_and_rolls_back_charge() {
        let tracker = MemTracker::new_root("native-task");
        tracker.install_limit_once(1).unwrap();
        let state = task_state(identity(), tracker.clone());
        let task = JsonPairTask::try_bind(&state).unwrap();
        let mut bytes: AggregateVec<u8> = AggregateVec::new_in(task.allocator().clone());
        assert!(bytes.try_reserve_exact(2).is_err());
        assert!(
            matches!(task.allocation_error(), JsonPairError::Failed(failure) if matches!(failure.category(), TaskFailureCategory::CapacityRefused { .. }))
        );
        assert_eq!(tracker.current(), 0);
    }
    #[test]
    fn json_in_pair_work_and_stop_are_separate_from_capacity() {
        let state = task_state(identity(), MemTracker::new_root("native-task"));
        let task = JsonPairTask::try_bind(&state).unwrap();
        let stopped = AtomicBool::new(false);
        let context = JsonPairContext {
            task: &task,
            stopped: &stopped,
        };
        let mut work = JsonPairWork::new(1);
        assert!(work.spend_one());
        assert!(!work.spend_one());
        assert!(work.exhausted());
        context.check_running().unwrap();
        stopped.store(true, Ordering::Release);
        assert!(matches!(
            context.check_running(),
            Err(JsonPairError::Stopped)
        ));
    }
}
