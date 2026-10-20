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

use crate::runtime::fragment::error::FragmentExecutionError;
use crate::runtime::profile::RuntimeProfileTree;
use novarocks_types::QueryId;
use novarocks_types::UniqueId;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FragmentCancelReason {
    detail: novarocks_failure_contract::BoundedDetail,
}

impl FragmentCancelReason {
    pub fn new(detail: impl std::fmt::Display) -> Self {
        Self {
            detail: novarocks_failure_contract::BoundedDetail::truncating_display(detail),
        }
    }

    pub fn detail(&self) -> &str {
        self.detail.as_str()
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum FragmentOutcome {
    Succeeded,
    Failed(FragmentExecutionError),
    Cancelled { reason: FragmentCancelReason },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FragmentTerminalFact {
    query_id: QueryId,
    fragment_instance_id: UniqueId,
    outcome: FragmentOutcome,
    profile: Option<RuntimeProfileTree>,
}

impl FragmentTerminalFact {
    pub fn new(
        query_id: QueryId,
        fragment_instance_id: UniqueId,
        outcome: FragmentOutcome,
        profile: Option<RuntimeProfileTree>,
    ) -> Self {
        Self {
            query_id,
            fragment_instance_id,
            outcome,
            profile,
        }
    }

    pub const fn query_id(&self) -> QueryId {
        self.query_id
    }

    pub const fn fragment_instance_id(&self) -> UniqueId {
        self.fragment_instance_id
    }

    pub const fn outcome(&self) -> &FragmentOutcome {
        &self.outcome
    }

    pub const fn profile(&self) -> Option<&RuntimeProfileTree> {
        self.profile.as_ref()
    }
}

impl std::fmt::Display for FragmentCancelReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.detail.fmt(f)
    }
}
impl std::error::Error for FragmentCancelReason {}
impl novarocks_failure_contract::FailureProjection for FragmentCancelReason {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        novarocks_failure_contract::FailureFact::stop_observation(
            crate::runtime::fragment::ExecutionReason::FragmentStopRequested.reason(),
        )
    }
}
impl From<FragmentCancelReason> for crate::runtime::fragment::ExecutionFailure {
    fn from(source: FragmentCancelReason) -> Self {
        Self::from_domain(source)
    }
}
