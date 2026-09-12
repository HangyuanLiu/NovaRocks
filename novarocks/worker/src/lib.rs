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

//! Worker-owned policy for native task execution.
//!
//! The execution contract supplies immutable cross-process facts. This crate
//! decides how one worker admits those facts, advances local lifecycle state,
//! fences exchange input, and accounts for its process-local time. It owns no
//! transport model and has no dependency on a query application or frontend.

use std::num::NonZeroUsize;

mod admission;
mod clock;
mod convergence;
mod domain;
mod drain;
mod host;
mod ingress;
mod lease;
mod lifecycle;
mod observation;
mod operation;
mod receipt;
mod reliable_transport;
mod status;

pub use admission::{
    AdmissionTicketAcquisitionRejection, AdmissionTicketAuthority, AdmissionTicketConfig,
    AdmissionTicketGrant, AdmissionTicketProgression, AdmissionTicketRedemption,
    AdmissionTicketRedemptionRejection, AdmissionTicketState, MAX_ADMISSION_RESERVATIONS,
    MAX_ADMISSION_TICKET_VALID_FOR,
};
pub use clock::{ManualClock, ProcessMonotonicClock, WorkerMonotonicClock};
pub use convergence::{
    TaskConvergence, TaskConvergenceAdvance, TaskConvergenceRejection, TaskConvergenceSnapshot,
};
pub use domain::{
    DomainPolicyRejection, InitialDomainKey, QueryContextDomains, TaskDomains,
    commit_task_domain_updates, initial_domain_keys, plan_task_domain_updates,
    task_domain_reaches_execution, validate_task_domain_membership,
};
pub use drain::WorkerDrainState;
pub use host::{HostRejection, RunnableTask, TaskExecutionHost};
pub use ingress::{IngressRejection, authorize_inbound_frame};
pub use lease::{InstalledLease, LeaseBounds, LeaseProgression, MonotonicInstant, RequestHorizon};
pub use lifecycle::{
    ContextOperationKind, ContextTransition, LatchOutcome, OperationAdmission, QueryContextEvent,
    RootDrainAction, TaskTransition, TerminationLatch, classify_context_transition,
    classify_operation_admission, classify_root_drain, classify_task_transition,
};
pub use observation::{
    ContextConvergenceCursorError, CursorObservation, TaskStatusEvent, TaskStatusSource,
    TaskStatusSubscriptionPosition,
};
pub use operation::OperationWaitCaps;
pub use receipt::{
    AdmissionTicketOutcome, CancelTaskOutcome, CreateTaskOutcome, DynamicFilterReadOutcome,
    FinalTaskInfoOutcome, OperationReceipt, QueryContextOutcome, ReleaseAcknowledgement,
    ReleaseQueryContextOutcome, TaskDynamicFilterRead, UpdateTaskOutcome,
};
pub use reliable_transport::{
    ReliableTransportAckOutcome, ReliableTransportFailOpenReason, ReliableTransportFailureOutcome,
    ReliableTransportPolicy, ReliableTransportResourceLimit, ReliableTransportSendOutcome,
    ReliableTransportState, ReliableTransportStateError, ReliableTransportTick,
};
pub use status::{
    METRIC_PUBLISH_MIN_INTERVAL, RootResultBinding, RootResultRoute, StatusAdvance,
    TaskMetricsSink, TaskStatusOwner, TaskStatusReporter,
};

/// Positive, ordered joint retained-result limits owned by one worker process.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct WorkerResultRetainedLimits {
    per_root: NonZeroUsize,
    per_process: NonZeroUsize,
}

impl WorkerResultRetainedLimits {
    pub fn try_new(per_root: usize, per_process: usize) -> Result<Self, String> {
        let per_root = NonZeroUsize::new(per_root).ok_or_else(|| {
            "per-root joint result retained-byte cap must be greater than 0".to_string()
        })?;
        let per_process = NonZeroUsize::new(per_process).ok_or_else(|| {
            "per-process joint result retained-byte cap must be greater than 0".to_string()
        })?;
        if per_root > per_process {
            return Err(format!(
                "per-root joint result retained-byte cap {} must not exceed per-process cap {}",
                per_root.get(),
                per_process.get()
            ));
        }
        Ok(Self {
            per_root,
            per_process,
        })
    }
    pub fn per_root(self) -> NonZeroUsize {
        self.per_root
    }
    pub fn per_process(self) -> NonZeroUsize {
        self.per_process
    }
}
