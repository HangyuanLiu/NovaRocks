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
use std::error::Error;
use std::fmt;
use std::sync::{Arc, Weak};

use crate::common::types::UniqueId;
use crate::runtime_filter::model::contract::BindingId;

use super::identity::{PartitionId, ProducerSequence};
use super::ordered_bound::OrderedBoundUpdate;
use super::value_domain::ValueDomainDelta;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum InstallOutcome {
    IgnoredEmpty,
    Installed,
    AlreadyInstalled,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum InstallContractErrorKind {
    InvalidEpoch,
    DuplicateIdentity,
    UnsupportedChannelContract,
    UnsupportedMembershipType,
    InvalidCoverage,
    UnknownCoverageWitness,
    DuplicateCoverageWitness,
    EmptyExpectedInstances,
    InvalidConsumerActivation,
    MissingMembershipCapability,
    InvalidPolicy,
    InvalidBudget,
    ConflictingDeployment,
    EpochMismatch,
    ServiceClosed,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct InstallContractError {
    kind: InstallContractErrorKind,
    detail: String,
}

impl InstallContractError {
    pub(crate) fn new(kind: InstallContractErrorKind, detail: impl Into<String>) -> Self {
        Self {
            kind,
            detail: detail.into(),
        }
    }

    pub(crate) const fn kind(&self) -> InstallContractErrorKind {
        self.kind
    }

    pub(crate) fn detail(&self) -> &str {
        &self.detail
    }
}

impl fmt::Display for InstallContractError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "runtime filter install {:?}: {}",
            self.kind, self.detail
        )
    }
}

impl Error for InstallContractError {}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RuntimeContractViolationKind {
    UnauthorizedBinding,
    UnauthorizedFragmentInstance,
    InvalidPartitionCount,
    PartitionCountConflict,
    InvalidPartition,
    InvalidContributionLease,
    TypeMismatch,
    ConflictingReplay,
    ConflictingTerminalSequence,
    ConflictingArtifactPublish,
    SequenceOutsideTerminalRange,
    OrderedContractMismatch,
    OrderedBoundLoosened,
    LogicalVersionOverflow,
    ProducerPortMismatch,
    ConsumerPortMismatch,
    SubscriptionActivationMismatch,
    ServiceUnavailable,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct RuntimeContractViolation {
    kind: RuntimeContractViolationKind,
    detail: String,
}

impl RuntimeContractViolation {
    pub(crate) fn new(kind: RuntimeContractViolationKind, detail: impl Into<String>) -> Self {
        Self {
            kind,
            detail: detail.into(),
        }
    }

    pub(crate) const fn kind(&self) -> RuntimeContractViolationKind {
        self.kind
    }

    pub(crate) fn detail(&self) -> &str {
        &self.detail
    }
}

impl fmt::Display for RuntimeContractViolation {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "runtime filter contract violation {:?}: {}",
            self.kind, self.detail
        )
    }
}

impl Error for RuntimeContractViolation {}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ProducerFailureReason {
    Cancelled,
    ExecutionFailed,
    UpstreamUnavailable,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum SubmitOutcome {
    Applied,
    Duplicate,
    Stale,
    SequenceAdvancedEqual,
    StreamAcceptedNoGlobalChange,
    Published,
    PendingGap,
    PendingFinalSnapshot,
    CoverageStillPossible,
    TerminalNoop,
    Completed,
    CompletedWithoutArtifact,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct ProducerOpenRequest {
    binding_id: BindingId,
    fragment_instance_id: UniqueId,
    local_partition_count: u32,
}

impl ProducerOpenRequest {
    pub(crate) const fn new(
        binding_id: BindingId,
        fragment_instance_id: UniqueId,
        local_partition_count: u32,
    ) -> Self {
        Self {
            binding_id,
            fragment_instance_id,
            local_partition_count,
        }
    }

    pub(crate) const fn binding_id(self) -> BindingId {
        self.binding_id
    }

    pub(crate) const fn fragment_instance_id(self) -> UniqueId {
        self.fragment_instance_id
    }

    pub(crate) const fn local_partition_count(self) -> u32 {
        self.local_partition_count
    }
}

pub(crate) trait ProducerAdapter: Send + Sync {
    fn submit(
        &self,
        partition_id: PartitionId,
        sequence: ProducerSequence,
        delta: ValueDomainDelta,
    ) -> Result<SubmitOutcome, RuntimeContractViolation>;

    fn close_partition(
        &self,
        partition_id: PartitionId,
        terminal_sequence: ProducerSequence,
    ) -> Result<SubmitOutcome, RuntimeContractViolation>;

    fn fail(
        &self,
        reason: ProducerFailureReason,
    ) -> Result<SubmitOutcome, RuntimeContractViolation>;
}

pub(crate) trait OrderedBoundProducerAdapter: Send + Sync {
    fn submit_bound(
        &self,
        partition_id: PartitionId,
        sequence: ProducerSequence,
        update: OrderedBoundUpdate,
    ) -> Result<SubmitOutcome, RuntimeContractViolation>;

    fn close_partition(
        &self,
        partition_id: PartitionId,
        terminal_sequence: ProducerSequence,
    ) -> Result<SubmitOutcome, RuntimeContractViolation>;

    fn fail(
        &self,
        reason: ProducerFailureReason,
    ) -> Result<SubmitOutcome, RuntimeContractViolation>;
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ProducerPortKind {
    Membership,
    OrderedBound,
}

pub(crate) enum ProducerHandle {
    Membership(Arc<dyn ProducerAdapter>),
    OrderedBound(Arc<dyn OrderedBoundProducerAdapter>),
}

impl fmt::Debug for ProducerHandle {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_tuple("ProducerHandle")
            .field(&self.kind())
            .finish()
    }
}

impl ProducerHandle {
    pub(crate) const fn kind(&self) -> ProducerPortKind {
        match self {
            Self::Membership(_) => ProducerPortKind::Membership,
            Self::OrderedBound(_) => ProducerPortKind::OrderedBound,
        }
    }

    pub(crate) fn downgrade(&self) -> ProducerHandleWeak {
        match self {
            Self::Membership(handle) => ProducerHandleWeak::Membership(Arc::downgrade(handle)),
            Self::OrderedBound(handle) => ProducerHandleWeak::OrderedBound(Arc::downgrade(handle)),
        }
    }

    pub(crate) fn into_membership(
        self,
    ) -> Result<Arc<dyn ProducerAdapter>, RuntimeContractViolation> {
        match self {
            Self::Membership(handle) => Ok(handle),
            Self::OrderedBound(_) => Err(RuntimeContractViolation::new(
                RuntimeContractViolationKind::ProducerPortMismatch,
                "ordered producer handle cannot be used as a membership producer",
            )),
        }
    }
}

pub(crate) enum ProducerHandleWeak {
    Membership(Weak<dyn ProducerAdapter>),
    OrderedBound(Weak<dyn OrderedBoundProducerAdapter>),
}

impl ProducerHandleWeak {
    pub(crate) const fn kind(&self) -> ProducerPortKind {
        match self {
            Self::Membership(_) => ProducerPortKind::Membership,
            Self::OrderedBound(_) => ProducerPortKind::OrderedBound,
        }
    }

    pub(crate) fn upgrade(&self) -> Option<ProducerHandle> {
        match self {
            Self::Membership(handle) => handle.upgrade().map(ProducerHandle::Membership),
            Self::OrderedBound(handle) => handle.upgrade().map(ProducerHandle::OrderedBound),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::RuntimeContractViolationKind;

    fn contract_violation_name(kind: RuntimeContractViolationKind) -> &'static str {
        match kind {
            RuntimeContractViolationKind::UnauthorizedBinding => "unauthorized-binding",
            RuntimeContractViolationKind::UnauthorizedFragmentInstance => {
                "unauthorized-fragment-instance"
            }
            RuntimeContractViolationKind::InvalidPartitionCount => "invalid-partition-count",
            RuntimeContractViolationKind::PartitionCountConflict => "partition-count-conflict",
            RuntimeContractViolationKind::InvalidPartition => "invalid-partition",
            RuntimeContractViolationKind::InvalidContributionLease => "invalid-contribution-lease",
            RuntimeContractViolationKind::TypeMismatch => "type-mismatch",
            RuntimeContractViolationKind::ConflictingReplay => "conflicting-replay",
            RuntimeContractViolationKind::ConflictingArtifactPublish => {
                "conflicting-artifact-publish"
            }
            RuntimeContractViolationKind::ConflictingTerminalSequence => {
                "conflicting-terminal-sequence"
            }
            RuntimeContractViolationKind::SequenceOutsideTerminalRange => {
                "sequence-outside-terminal-range"
            }
            RuntimeContractViolationKind::OrderedContractMismatch => "ordered-contract-mismatch",
            RuntimeContractViolationKind::OrderedBoundLoosened => "ordered-bound-loosened",
            RuntimeContractViolationKind::LogicalVersionOverflow => "logical-version-overflow",
            RuntimeContractViolationKind::ProducerPortMismatch => "producer-port-mismatch",
            RuntimeContractViolationKind::ConsumerPortMismatch => "consumer-port-mismatch",
            RuntimeContractViolationKind::SubscriptionActivationMismatch => {
                "subscription-activation-mismatch"
            }
            RuntimeContractViolationKind::ServiceUnavailable => "service-unavailable",
        }
    }

    #[test]
    fn runtime_contract_violations_exclude_resource_limits() {
        assert_eq!(
            contract_violation_name(RuntimeContractViolationKind::TypeMismatch),
            "type-mismatch"
        );
    }
}
