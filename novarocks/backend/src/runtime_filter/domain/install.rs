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

use std::fmt;

use novarocks_execution::runtime_filter::{
    RuntimeFilterContribution, RuntimeFilterContributionKind, RuntimeFilterExecutionContract,
    RuntimeFilterProducerContract, RuntimeFilterProducerKind, contribution,
};

use super::{BackendChannelIdentity, BackendCoverage, BackendParticipantIdentity};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum BackendInstallPolicyError {
    ZeroContributionBudget,
    ContributionTooLarge,
    ContributionKindMismatch,
    ContributionDigestMismatch,
    ContributionDecodeFailed(contribution::ContributionCodecError),
    InvalidTopKContract,
}

impl fmt::Display for BackendInstallPolicyError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "invalid Backend runtime-filter install policy: {self:?}"
        )
    }
}

impl std::error::Error for BackendInstallPolicyError {}

/// Backend-private install policy for a single producer channel. Fragment
/// roles, contribution format, and reduction legality remain Execution-owned
/// values; this type adds the participant authority, coverage, and resource
/// ceiling that only Backend owns.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct BackendInstallPolicy {
    channel: BackendChannelIdentity,
    producer: RuntimeFilterProducerContract,
    coverage: BackendCoverage,
    max_contribution_bytes: usize,
}

impl BackendInstallPolicy {
    pub(crate) fn new(
        participant: BackendParticipantIdentity,
        producer: RuntimeFilterProducerContract,
        coverage: BackendCoverage,
        max_contribution_bytes: usize,
    ) -> Result<Self, BackendInstallPolicyError> {
        if max_contribution_bytes == 0 {
            return Err(BackendInstallPolicyError::ZeroContributionBudget);
        }
        Ok(Self {
            channel: BackendChannelIdentity::new(
                participant,
                producer.binding_id(),
                producer.channel_id(),
            ),
            producer,
            coverage,
            max_contribution_bytes,
        })
    }

    pub(crate) const fn channel(&self) -> BackendChannelIdentity {
        self.channel
    }

    pub(crate) const fn producer(&self) -> &RuntimeFilterProducerContract {
        &self.producer
    }

    #[allow(
        dead_code,
        reason = "Retained for staged backend runtime-filter domain and materialization integration."
    )]
    pub(crate) const fn coverage(&self) -> &BackendCoverage {
        &self.coverage
    }

    pub(crate) const fn max_contribution_bytes(&self) -> usize {
        self.max_contribution_bytes
    }

    pub(crate) fn contract_digest(&self) -> [u8; 32] {
        match self.producer.contract() {
            RuntimeFilterExecutionContract::Membership(schema) => schema.digest(),
            RuntimeFilterExecutionContract::Ordered(contract) => contract.digest(),
        }
    }

    pub(crate) fn decode_contribution(
        &self,
        contribution: &RuntimeFilterContribution,
    ) -> Result<contribution::RuntimeFilterContribution, BackendInstallPolicyError> {
        if contribution.canonical_bytes().len() > self.max_contribution_bytes {
            return Err(BackendInstallPolicyError::ContributionTooLarge);
        }
        if contribution.kind() != expected_contribution_kind(self.producer.kind()) {
            return Err(BackendInstallPolicyError::ContributionKindMismatch);
        }
        if contribution.contract_digest() != self.contract_digest() {
            return Err(BackendInstallPolicyError::ContributionDigestMismatch);
        }
        let decoded = match (
            self.producer.kind(),
            self.producer.contract(),
            self.producer.reduction(),
        ) {
            (
                RuntimeFilterProducerKind::Membership,
                RuntimeFilterExecutionContract::Membership(schema),
                _,
            ) => contribution::decode_contribution(
                contribution.canonical_bytes(),
                &self.contract_digest(),
                contribution::ContributionCodecExpectation::membership(
                    schema.data_type(),
                    schema.digest(),
                ),
                self.max_contribution_bytes,
            ),
            (
                RuntimeFilterProducerKind::FinalDomain,
                RuntimeFilterExecutionContract::Membership(schema),
                _,
            ) => contribution::decode_contribution(
                contribution.canonical_bytes(),
                &self.contract_digest(),
                contribution::ContributionCodecExpectation::final_domain(
                    schema.data_type(),
                    schema.digest(),
                ),
                self.max_contribution_bytes,
            ),
            (
                RuntimeFilterProducerKind::OrderedBound,
                RuntimeFilterExecutionContract::Ordered(contract),
                _,
            ) => contribution::decode_contribution(
                contribution.canonical_bytes(),
                &self.contract_digest(),
                contribution::ContributionCodecExpectation::OrderedBound(contract),
                self.max_contribution_bytes,
            ),
            (
                RuntimeFilterProducerKind::TopKSummary,
                RuntimeFilterExecutionContract::Ordered(contract),
                novarocks_execution::runtime_filter::RuntimeFilterReduction::MergeTopKSummary {
                    k,
                    contract_digest,
                },
            ) => {
                let topk = contribution::RuntimeTopKSummaryContract::new(
                    contract.as_ref().clone(),
                    k,
                    contract_digest,
                );
                contribution::decode_contribution(
                    contribution.canonical_bytes(),
                    &self.contract_digest(),
                    contribution::ContributionCodecExpectation::TopKSummary(&topk),
                    self.max_contribution_bytes,
                )
            }
            _ => return Err(BackendInstallPolicyError::InvalidTopKContract),
        };
        decoded.map_err(BackendInstallPolicyError::ContributionDecodeFailed)
    }
}

fn expected_contribution_kind(kind: RuntimeFilterProducerKind) -> RuntimeFilterContributionKind {
    match kind {
        RuntimeFilterProducerKind::Membership => RuntimeFilterContributionKind::Membership,
        RuntimeFilterProducerKind::OrderedBound => RuntimeFilterContributionKind::OrderedBound,
        RuntimeFilterProducerKind::TopKSummary => RuntimeFilterContributionKind::TopKSummary,
        RuntimeFilterProducerKind::FinalDomain => RuntimeFilterContributionKind::FinalDomain,
    }
}
