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

pub(crate) mod compiler;
pub(crate) mod extension;
pub(crate) mod role_graph;
pub(crate) mod shard;
pub(crate) mod wait_for;

use std::fmt;

use crate::runtime_filter::model::contract::{BindingId, ChannelId, PlanFragmentId};
use crate::runtime_filter::model::validation::GraphValidationError;
use crate::runtime_filter::port::install::RuntimeFilterCoreBudget;

/// Deployment-time resource / routing policy. Read-only input to the compiler.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct RuntimeFilterDeploymentPolicy {
    /// Per-channel core budget (`RuntimeFilterCoreBudget`) stamped into every shard.
    pub core_budget: RuntimeFilterCoreBudget,
    /// How many redundant replica producers an `AnyOf` channel may use.
    /// Never hardcode a fixed fanout; the compiler clamps this to the live topology.
    pub replica_redundancy: u32,
}

/// Static contract failures. Every variant is caught before fragment submission.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum DeploymentError {
    /// The graph failed RFD-1 validation (called first).
    GraphInvalid(GraphValidationError),
    /// A binding references a fragment with no placement in the scheduling plan.
    MissingPlacement { fragment: PlanFragmentId },
    /// A placement referenced a backend id absent from the live snapshot.
    UnknownBackend { backend_idx: usize },
    /// A `BlockingSnapshot` consumer would wait on a producer that (transitively)
    /// depends on the consumer's own fragment — an execution cycle.
    BlockingFeedbackCycle {
        channel: ChannelId,
        binding: BindingId,
    },
    /// A channel's coverage carries no witnesses / producers.
    EmptyCoverage { channel: ChannelId },
    /// M1 install only supports Membership logical domains.
    UnsupportedLogicalDomain { channel: ChannelId },
}

impl fmt::Display for DeploymentError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::GraphInvalid(e) => write!(f, "runtime filter graph invalid: {e}"),
            Self::MissingPlacement { fragment } => {
                write!(f, "no placement for fragment {}", fragment.get())
            }
            Self::UnknownBackend { backend_idx } => {
                write!(f, "unknown backend index {backend_idx}")
            }
            Self::BlockingFeedbackCycle { channel, binding } => write!(
                f,
                "blocking-snapshot consumer binding {} on channel {} forms an execution cycle",
                binding.get(),
                channel.get()
            ),
            Self::EmptyCoverage { channel } => {
                write!(f, "channel {} has empty coverage", channel.get())
            }
            Self::UnsupportedLogicalDomain { channel } => {
                write!(
                    f,
                    "channel {} uses an unsupported logical domain",
                    channel.get()
                )
            }
        }
    }
}

impl std::error::Error for DeploymentError {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime_filter::model::contract::{BindingId, ChannelId};
    use crate::runtime_filter::port::install::RuntimeFilterCoreBudget;

    #[test]
    fn policy_and_error_are_constructible() {
        let policy = RuntimeFilterDeploymentPolicy {
            core_budget: RuntimeFilterCoreBudget::new(4096),
            replica_redundancy: 2,
        };
        assert_eq!(policy.core_budget.max_reducer_bytes(), 4096);
        assert_eq!(policy.replica_redundancy, 2);

        let err = DeploymentError::BlockingFeedbackCycle {
            channel: ChannelId::new(1),
            binding: BindingId::new(2),
        };
        let rendered = format!("{err}");
        assert!(rendered.contains("blocking"));
        assert!(rendered.contains("binding 2"));
        assert!(rendered.contains("channel 1"));
    }
}
