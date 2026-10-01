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

//! Bounded operator-owned verification facts, independent of task status and profiling.
use crate::{QueryContextRef, TaskIdentity};

pub const VERIFICATION_MAX_INSTANCES_PER_TASK: usize = 256;
pub const VERIFICATION_MAX_TASKS_PER_CONTEXT: usize = 4096;
pub const VERIFICATION_MAX_INSTANCES_PER_CONTEXT: usize = 4096;
pub const VERIFICATION_MAX_CONTEXT_BYTES: usize = 512 * 1024;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub struct VerificationInstance {
    pub plan_node_id: i32,
    pub local_instance_id: u32,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum VerificationState {
    NotStarted,
    Started,
    Completed { requested: u64, matched: u64 },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct VerificationRecord {
    pub instance: VerificationInstance,
    pub state: VerificationState,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum TaskVerificationObservation {
    Available(Vec<VerificationRecord>),
    Truncated,
    Unavailable,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TaskVerificationFacts {
    pub identity: TaskIdentity,
    pub observation: TaskVerificationObservation,
}

impl TaskVerificationFacts {
    pub fn unavailable(identity: TaskIdentity) -> Self {
        Self {
            identity,
            observation: TaskVerificationObservation::Unavailable,
        }
    }

    pub fn validate(&self) -> Result<(), &'static str> {
        if let TaskVerificationObservation::Available(records) = &self.observation {
            if records.len() > VERIFICATION_MAX_INSTANCES_PER_TASK {
                return Err("task verification instance budget exceeded");
            }
            let mut previous = None;
            for record in records {
                if record.instance.plan_node_id < 0 {
                    return Err("verification plan node id must be nonnegative");
                }
                if previous.is_some_and(|value| value >= record.instance) {
                    return Err("verification instances must be unique and ordered");
                }
                if matches!(record.state, VerificationState::Completed { requested, matched } if matched > requested)
                {
                    return Err(
                        "completed verification cannot match more deletions than requested",
                    );
                }
                previous = Some(record.instance);
            }
        }
        Ok(())
    }

    pub fn retained_bytes(&self) -> usize {
        std::mem::size_of::<Self>()
            + match &self.observation {
                TaskVerificationObservation::Available(records) => {
                    records.len() * std::mem::size_of::<VerificationRecord>()
                }
                _ => 0,
            }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ContextVerificationFacts {
    pub context: QueryContextRef,
    pub tasks: Vec<TaskVerificationFacts>,
    pub truncated: bool,
}

impl ContextVerificationFacts {
    pub fn validate(&self) -> Result<(), &'static str> {
        if self.tasks.len() > VERIFICATION_MAX_TASKS_PER_CONTEXT {
            return Err("context verification task budget exceeded");
        }
        let mut previous = None;
        let mut instances = 0;
        let mut bytes = 0;
        for task in &self.tasks {
            if task.identity.verify_query_context(self.context).is_err() {
                return Err("verification task does not belong to the exact released context");
            }
            if previous.is_some_and(|value| value >= task.identity) {
                return Err("verification tasks must be unique and ordered");
            }
            task.validate()?;
            if let TaskVerificationObservation::Available(records) = &task.observation {
                instances += records.len();
            }
            bytes += task.retained_bytes();
            previous = Some(task.identity);
        }
        if instances > VERIFICATION_MAX_INSTANCES_PER_CONTEXT
            || bytes > VERIFICATION_MAX_CONTEXT_BYTES
        {
            return Err("context verification fact budget exceeded");
        }
        Ok(())
    }

    /// A missing, unavailable or partial record cannot authorize a rollback.
    pub fn permits_rollback(&self, expected: &[(TaskIdentity, VerificationInstance)]) -> bool {
        if self.truncated || expected.is_empty() || self.validate().is_err() {
            return false;
        }
        let mut all_not_started = true;
        let mut all_completed = true;
        let mut unique = std::collections::BTreeSet::new();
        for &(identity, instance) in expected {
            if !unique.insert((identity, instance)) {
                return false;
            }
            let Some(task) = self.tasks.iter().find(|task| task.identity == identity) else {
                return false;
            };
            let TaskVerificationObservation::Available(records) = &task.observation else {
                return false;
            };
            let Some(record) = records.iter().find(|record| record.instance == instance) else {
                return false;
            };
            all_not_started &= record.state == VerificationState::NotStarted;
            all_completed &= matches!(record.state, VerificationState::Completed { requested, matched } if requested == matched);
        }
        all_not_started || all_completed
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fixture() -> (TaskIdentity, QueryContextRef) {
        use novarocks_types::identity::*;
        let execution =
            QueryExecutionId::new(QueryId::new(7, 9), AttemptId::new(1).unwrap()).unwrap();
        let backend = BackendProcessId::new_v7();
        (
            TaskIdentity::new(
                execution,
                StageId::new(1).unwrap(),
                TaskId::new(1).unwrap(),
                backend,
            ),
            QueryContextRef::new(execution, FrontendProcessId::new_v7(), backend),
        )
    }

    #[test]
    fn verification_requires_exact_complete_expected_instances() {
        let (identity, context) = fixture();
        let instance = VerificationInstance {
            plan_node_id: 8,
            local_instance_id: 0,
        };
        let expected = [(identity, instance)];
        let mut facts = ContextVerificationFacts {
            context,
            truncated: false,
            tasks: vec![TaskVerificationFacts {
                identity,
                observation: TaskVerificationObservation::Available(vec![VerificationRecord {
                    instance,
                    state: VerificationState::NotStarted,
                }]),
            }],
        };
        assert!(facts.permits_rollback(&expected));
        assert!(!facts.permits_rollback(&[]));
        facts.tasks[0].observation =
            TaskVerificationObservation::Available(vec![VerificationRecord {
                instance,
                state: VerificationState::Started,
            }]);
        assert!(!facts.permits_rollback(&expected));
        facts.tasks[0].observation =
            TaskVerificationObservation::Available(vec![VerificationRecord {
                instance,
                state: VerificationState::Completed {
                    requested: 5,
                    matched: 5,
                },
            }]);
        assert!(facts.permits_rollback(&expected));
        facts.truncated = true;
        assert!(!facts.permits_rollback(&expected));
        facts.truncated = false;
        facts.tasks[0].observation = TaskVerificationObservation::Unavailable;
        assert!(!facts.permits_rollback(&expected));
        facts.tasks.clear();
        assert!(!facts.permits_rollback(&expected));
    }

    #[test]
    fn verification_rejects_foreign_task_duplicate_instance_and_excess_matches() {
        let (identity, context) = fixture();
        let record = VerificationRecord {
            instance: VerificationInstance {
                plan_node_id: 8,
                local_instance_id: 0,
            },
            state: VerificationState::NotStarted,
        };
        let mut facts = TaskVerificationFacts {
            identity,
            observation: TaskVerificationObservation::Available(vec![record, record]),
        };
        assert!(facts.validate().is_err());
        facts.observation = TaskVerificationObservation::Available(vec![VerificationRecord {
            state: VerificationState::Completed {
                requested: 5,
                matched: 4,
            },
            ..record
        }]);
        assert!(facts.validate().is_ok());
        facts.observation = TaskVerificationObservation::Available(vec![VerificationRecord {
            state: VerificationState::Completed {
                requested: 5,
                matched: 6,
            },
            ..record
        }]);
        assert!(facts.validate().is_err());
        let (foreign, _) = fixture();
        let context_facts = ContextVerificationFacts {
            context,
            truncated: false,
            tasks: vec![TaskVerificationFacts::unavailable(foreign)],
        };
        assert!(context_facts.validate().is_err());
    }
}
