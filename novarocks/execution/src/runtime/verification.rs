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
//! Always-on facts owned by the exact task runtime, sealed after physical convergence.
use novarocks_execution_contract::{
    TaskIdentity, TaskVerificationFacts, TaskVerificationObservation,
    VERIFICATION_MAX_INSTANCES_PER_TASK, VerificationInstance, VerificationRecord,
    VerificationState,
};
use std::collections::BTreeMap;
use std::sync::Mutex;

#[derive(Debug)]
struct Facts {
    records: BTreeMap<VerificationInstance, VerificationState>,
    truncated: bool,
    sealed: bool,
}

#[derive(Debug)]
pub struct TaskVerificationHolder {
    identity: TaskIdentity,
    facts: Mutex<Facts>,
}

impl TaskVerificationHolder {
    pub fn new(identity: TaskIdentity) -> Self {
        Self {
            identity,
            facts: Mutex::new(Facts {
                records: BTreeMap::new(),
                truncated: false,
                sealed: false,
            }),
        }
    }

    pub fn register(&self, instance: VerificationInstance) -> Result<(), String> {
        let mut facts = self.facts.lock().expect("task verification lock");
        if facts.sealed || instance.plan_node_id < 0 {
            return Err("verification registration outside its task lifetime".into());
        }
        if facts.records.contains_key(&instance) {
            return Err("duplicate verification instance".into());
        }
        if facts.records.len() == VERIFICATION_MAX_INSTANCES_PER_TASK {
            facts.truncated = true;
            return Err("task verification instance budget exceeded".into());
        }
        facts
            .records
            .insert(instance, VerificationState::NotStarted);
        Ok(())
    }

    pub fn start(&self, instance: VerificationInstance) -> Result<(), String> {
        self.advance(instance, VerificationState::Started)
    }

    pub fn complete(
        &self,
        instance: VerificationInstance,
        requested: u64,
        matched: u64,
    ) -> Result<(), String> {
        if matched > requested {
            return Err("verification completion has more matches than requested".into());
        }
        self.advance(
            instance,
            VerificationState::Completed { requested, matched },
        )
    }

    fn advance(
        &self,
        instance: VerificationInstance,
        next: VerificationState,
    ) -> Result<(), String> {
        let mut facts = self.facts.lock().expect("task verification lock");
        if facts.sealed {
            return Err("sealed verification facts cannot advance".into());
        }
        let Some(current) = facts.records.get_mut(&instance) else {
            return Err("verification instance was not registered".into());
        };
        if *current == next {
            return Ok(());
        }
        match (*current, next) {
            (VerificationState::NotStarted, VerificationState::Started)
            | (VerificationState::Started, VerificationState::Completed { .. }) => {
                *current = next;
                Ok(())
            }
            _ => Err("verification facts cannot regress or skip their start".into()),
        }
    }

    /// Called only after the Worker has observed physical task convergence.
    pub fn seal(&self) -> TaskVerificationFacts {
        let mut facts = self.facts.lock().expect("task verification lock");
        facts.sealed = true;
        TaskVerificationFacts {
            identity: self.identity,
            observation: if facts.truncated {
                TaskVerificationObservation::Truncated
            } else {
                TaskVerificationObservation::Available(
                    facts
                        .records
                        .iter()
                        .map(|(&instance, &state)| VerificationRecord { instance, state })
                        .collect(),
                )
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use novarocks_execution_contract::QueryContextRef;

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
    fn verification_is_always_on_monotonic_and_seals_idempotently() {
        let (identity, _) = fixture();
        let holder = TaskVerificationHolder::new(identity);
        let instance = VerificationInstance {
            plan_node_id: 8,
            local_instance_id: 0,
        };
        holder.register(instance).unwrap();
        assert!(holder.complete(instance, 0, 0).is_err());
        holder.start(instance).unwrap();
        assert!(holder.complete(instance, 5, 6).is_err());
        holder.complete(instance, 5, 5).unwrap();
        assert!(holder.start(instance).is_err());
        let first = holder.seal();
        assert_eq!(holder.seal(), first);
        assert!(holder.complete(instance, 5, 5).is_err());
        assert!(
            matches!(first.observation, TaskVerificationObservation::Available(records) if records[0].state == VerificationState::Completed { requested: 5, matched: 5 })
        );
    }

    #[test]
    fn verification_cancellation_preserves_started_and_overflow_is_explicit() {
        let (identity, _) = fixture();
        let holder = TaskVerificationHolder::new(identity);
        for local_instance_id in 0..VERIFICATION_MAX_INSTANCES_PER_TASK as u32 {
            holder
                .register(VerificationInstance {
                    plan_node_id: 8,
                    local_instance_id,
                })
                .unwrap();
        }
        assert!(
            holder
                .register(VerificationInstance {
                    plan_node_id: 8,
                    local_instance_id: 256
                })
                .is_err()
        );
        assert_eq!(
            holder.seal().observation,
            TaskVerificationObservation::Truncated
        );
        let holder = TaskVerificationHolder::new(identity);
        let instance = VerificationInstance {
            plan_node_id: 8,
            local_instance_id: 0,
        };
        holder.register(instance).unwrap();
        holder.start(instance).unwrap();
        assert!(
            matches!(holder.seal().observation, TaskVerificationObservation::Available(records) if records[0].state == VerificationState::Started)
        );
    }

    #[test]
    fn verification_complete_deficit_is_sealed_and_replayed_exactly() {
        let (identity, _) = fixture();
        let holder = TaskVerificationHolder::new(identity);
        let instance = VerificationInstance {
            plan_node_id: 8,
            local_instance_id: 0,
        };
        holder.register(instance).unwrap();
        holder.start(instance).unwrap();
        holder.complete(instance, 5, 4).unwrap();
        holder.complete(instance, 5, 4).unwrap();
        assert!(holder.complete(instance, 5, 5).is_err());
        let first = holder.seal();
        assert!(first.validate().is_ok());
        assert_eq!(first, holder.seal());
        assert!(
            matches!(first.observation, TaskVerificationObservation::Available(records) if records[0].state == VerificationState::Completed { requested: 5, matched: 4 })
        );
    }
}
