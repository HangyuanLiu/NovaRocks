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
use std::sync::Arc;

use novarocks_execution::runtime_filter::{
    LogicalVersion, RuntimeFilterChannelId, RuntimeFilterContribution, contribution,
};

use super::{BackendInstallPolicy, BackendInstallPolicyError};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum BackendLogicalSnapshotError {
    InvalidContribution(BackendInstallPolicyError),
    VersionRegression,
}

impl fmt::Display for BackendLogicalSnapshotError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "invalid Backend runtime-filter logical snapshot: {self:?}"
        )
    }
}

impl std::error::Error for BackendLogicalSnapshotError {}

/// Immutable Backend publication candidate. It owns no evaluator-facing
/// artifact: later materialization turns this strict decoded contribution into
/// a separate retained artifact for an Execution snapshot.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct BackendLogicalSnapshot {
    logical_version: LogicalVersion,
    contribution: Arc<contribution::RuntimeFilterContribution>,
}

impl BackendLogicalSnapshot {
    pub(crate) fn new(
        policy: &BackendInstallPolicy,
        logical_version: LogicalVersion,
        contribution: RuntimeFilterContribution,
    ) -> Result<Self, BackendLogicalSnapshotError> {
        let contribution = policy
            .decode_contribution(&contribution)
            .map_err(BackendLogicalSnapshotError::InvalidContribution)?;
        Ok(Self {
            logical_version,
            contribution: Arc::new(contribution),
        })
    }

    pub(crate) fn next_after(
        previous: &Self,
        policy: &BackendInstallPolicy,
        contribution: RuntimeFilterContribution,
    ) -> Result<Self, BackendLogicalSnapshotError> {
        let logical_version = previous
            .logical_version
            .checked_next()
            .ok_or(BackendLogicalSnapshotError::VersionRegression)?;
        Self::new(policy, logical_version, contribution)
    }

    pub(crate) const fn logical_version(&self) -> LogicalVersion {
        self.logical_version
    }

    pub(crate) const fn contribution(&self) -> &Arc<contribution::RuntimeFilterContribution> {
        &self.contribution
    }
}

/// Immutable participant-local reduction result.  Unlike
/// [`BackendLogicalSnapshot`], which validates a single ingress contribution,
/// this value is what a Backend channel publishes after it has combined its
/// accepted streams.  The semantic payload remains an Execution value; only
/// the publication identity and reduction lifetime are Backend-owned.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct BackendReducedLogicalSnapshot {
    channel_id: RuntimeFilterChannelId,
    logical_version: LogicalVersion,
    domain: BackendReducedLogicalDomain,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum BackendReducedLogicalDomain {
    Membership(contribution::ValueDomainDelta),
    OrderedBound(contribution::OrderedTuple),
}

impl BackendReducedLogicalSnapshot {
    pub(crate) const fn membership(
        channel_id: RuntimeFilterChannelId,
        logical_version: LogicalVersion,
        domain: contribution::ValueDomainDelta,
    ) -> Self {
        Self {
            channel_id,
            logical_version,
            domain: BackendReducedLogicalDomain::Membership(domain),
        }
    }

    pub(crate) const fn ordered_bound(
        channel_id: RuntimeFilterChannelId,
        logical_version: LogicalVersion,
        bound: contribution::OrderedTuple,
    ) -> Self {
        Self {
            channel_id,
            logical_version,
            domain: BackendReducedLogicalDomain::OrderedBound(bound),
        }
    }

    pub(crate) const fn channel_id(&self) -> RuntimeFilterChannelId {
        self.channel_id
    }

    pub(crate) const fn logical_version(&self) -> LogicalVersion {
        self.logical_version
    }

    pub(crate) const fn domain(&self) -> &BackendReducedLogicalDomain {
        &self.domain
    }
}
