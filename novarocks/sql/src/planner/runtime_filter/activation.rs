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

//! SQL planner activation constraints and sealed decision provenance.

use std::collections::BTreeMap;

use super::contract::{BindingId, ChannelId, ConsumerActivation, LateApplyGranularity};
use super::wait_graph::CycleStep;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ActivationConstraint {
    LiveOnly {
        late_apply: LateApplyGranularity,
        reason: RequiredLiveReason,
    },
    BlockingOrBatchLive {
        fallback: ActivationFallback,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ActivationFallback {
    BlockingSnapshot,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RequiredLiveReason {
    OrderedBoundContract,
    FencedFinalDomainContract,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ActivationDecision {
    pub(crate) channel: ChannelId,
    pub(crate) consumer_binding: BindingId,
    pub(crate) consumer_fragment: u32,
    pub(crate) activation: ConsumerActivation,
    pub(crate) reason: ActivationDecisionReason,
}

pub(crate) type ActivationDecisionCatalog = BTreeMap<BindingId, ActivationDecision>;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum ActivationDecisionReason {
    RequiredByContract {
        reason: RequiredLiveReason,
    },
    CycleForced {
        producer_bindings: Vec<BindingId>,
        witness: Vec<CycleStep>,
    },
    ConservativeFallback,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn required_live_constraint_preserves_its_reason_and_granularity() {
        assert_eq!(
            ActivationConstraint::LiveOnly {
                late_apply: LateApplyGranularity::Batch,
                reason: RequiredLiveReason::OrderedBoundContract,
            },
            ActivationConstraint::LiveOnly {
                late_apply: LateApplyGranularity::Batch,
                reason: RequiredLiveReason::OrderedBoundContract,
            }
        );
    }
}
