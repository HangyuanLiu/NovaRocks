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

//! Worker-owned task-domain execution policy.

use std::fmt;

use novarocks_execution_contract::{
    DomainProgression, OperationOutcome, TaskDescriptor, TaskDomainReceipt, TaskDomainUpdate,
};

use crate::{
    DomainPolicyRejection, HostRejection, TaskDomains, TaskExecutionHost,
    commit_task_domain_updates, plan_task_domain_updates, task_domain_reaches_execution,
    validate_task_domain_membership,
};

/// The typed operation outcome for a task-domain policy refusal.
#[derive(Clone, Debug)]
pub struct DomainExecutionRejection {
    outcome: OperationOutcome,
    detail: String,
}

impl DomainExecutionRejection {
    fn new(outcome: OperationOutcome, detail: impl Into<String>) -> Self {
        Self {
            outcome,
            detail: detail.into(),
        }
    }

    pub const fn outcome(&self) -> OperationOutcome {
        self.outcome
    }

    pub fn detail(&self) -> &str {
        &self.detail
    }
}

impl fmt::Display for DomainExecutionRejection {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.detail)
    }
}

impl From<DomainPolicyRejection> for DomainExecutionRejection {
    fn from(rejection: DomainPolicyRejection) -> Self {
        Self::new(OperationOutcome::DomainConflict, rejection.detail())
    }
}

pub fn validate_task_domain_execution_membership(
    descriptor: &TaskDescriptor,
    updates: &[TaskDomainUpdate],
) -> Result<(), DomainExecutionRejection> {
    validate_task_domain_membership(descriptor, updates).map_err(Into::into)
}

pub fn plan_task_domain_execution_updates(
    descriptor: &TaskDescriptor,
    domains: &TaskDomains,
    updates: &[TaskDomainUpdate],
) -> Result<Vec<DomainProgression>, DomainExecutionRejection> {
    plan_task_domain_updates(descriptor, domains, updates).map_err(Into::into)
}

pub fn commit_task_domain_execution_updates(
    domains: &mut TaskDomains,
    updates: &[TaskDomainUpdate],
    queued: &[Option<u64>],
) -> Result<(Vec<TaskDomainReceipt>, bool), DomainExecutionRejection> {
    commit_task_domain_updates(domains, updates, queued).map_err(Into::into)
}

pub fn apply_task_domain_updates(
    host: &dyn TaskExecutionHost,
    descriptor: &TaskDescriptor,
    domains: &mut TaskDomains,
    updates: &[TaskDomainUpdate],
) -> Result<(Vec<TaskDomainReceipt>, bool), DomainExecutionRejection> {
    let plan = plan_task_domain_execution_updates(descriptor, domains, updates)?;
    let queued = apply_planned_task_domain_updates(host, descriptor, updates, &plan)?;
    commit_task_domain_execution_updates(domains, updates, &queued)
}

pub fn apply_planned_task_domain_updates(
    host: &dyn TaskExecutionHost,
    descriptor: &TaskDescriptor,
    updates: &[TaskDomainUpdate],
    plan: &[DomainProgression],
) -> Result<Vec<Option<u64>>, DomainExecutionRejection> {
    let mut queued = Vec::with_capacity(updates.len());
    for (update, progression) in updates.iter().zip(plan) {
        if task_domain_reaches_execution(update, *progression) {
            match host.apply_task_domain(descriptor, update) {
                Ok(depth) => queued.push(depth),
                Err(rejection) => {
                    tracing::warn!(
                        task = %descriptor.identity(),
                        kind = ?update.kind(),
                        progression = ?progression,
                        category = ?rejection.category(),
                        detail = %rejection.detail(),
                        "task domain update refused by the execution host"
                    );
                    return Err(rejection_from_host(rejection));
                }
            }
        } else {
            queued.push(None);
        }
    }
    Ok(queued)
}

fn rejection_from_host(rejection: HostRejection) -> DomainExecutionRejection {
    use novarocks_execution_contract::TaskFailureCategory;

    let outcome = match rejection.category() {
        TaskFailureCategory::ResourceExhausted => OperationOutcome::ResourceExhausted,
        TaskFailureCategory::Protocol
        | TaskFailureCategory::Exchange
        | TaskFailureCategory::Execution
        | TaskFailureCategory::Internal => OperationOutcome::InvalidStateOrRequest,
    };
    DomainExecutionRejection::new(outcome, rejection.detail().as_str())
}
