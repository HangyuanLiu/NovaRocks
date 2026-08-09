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

use novarocks_execution::runtime_filter::{
    PartitionId, RuntimeFilterBindingId, RuntimeFilterChannelId,
};
use novarocks_types::UniqueId;

/// Backend authority for exactly one query attempt. The epoch is deliberately
/// local to the participant domain; Execution owns fragment semantics, not
/// participant lifetime.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(crate) struct BackendParticipantIdentity {
    query_id: UniqueId,
    deployment_epoch: u64,
}

impl BackendParticipantIdentity {
    pub(crate) const fn new(query_id: UniqueId, deployment_epoch: u64) -> Self {
        Self {
            query_id,
            deployment_epoch,
        }
    }

    pub(crate) const fn query_id(self) -> UniqueId {
        self.query_id
    }

    pub(crate) const fn deployment_epoch(self) -> u64 {
        self.deployment_epoch
    }
}

/// One installed channel below a Backend participant. Binding and channel IDs
/// stay in Execution's vocabulary so the participant never mirrors fragment
/// contract identity.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(crate) struct BackendChannelIdentity {
    participant: BackendParticipantIdentity,
    binding_id: RuntimeFilterBindingId,
    channel_id: RuntimeFilterChannelId,
}

impl BackendChannelIdentity {
    pub(crate) const fn new(
        participant: BackendParticipantIdentity,
        binding_id: RuntimeFilterBindingId,
        channel_id: RuntimeFilterChannelId,
    ) -> Self {
        Self {
            participant,
            binding_id,
            channel_id,
        }
    }

    pub(crate) const fn participant(self) -> BackendParticipantIdentity {
        self.participant
    }

    pub(crate) const fn binding_id(self) -> RuntimeFilterBindingId {
        self.binding_id
    }

    pub(crate) const fn channel_id(self) -> RuntimeFilterChannelId {
        self.channel_id
    }
}

/// One producer partition under a sealed Backend channel. This is a routing
/// coordinate only; it contains no contribution or reducer state.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(crate) struct BackendProducerStreamIdentity {
    channel: BackendChannelIdentity,
    fragment_instance_id: UniqueId,
    partition_id: PartitionId,
}

/// Backend-private physical route edge.  It is intentionally not an Execution
/// identifier: route topology is a participant delivery concern.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(crate) struct BackendRouteEdgeId(u64);

impl BackendRouteEdgeId {
    pub(crate) const fn new(raw: u64) -> Self {
        Self(raw)
    }

    pub(crate) const fn get(self) -> u64 {
        self.0
    }
}

/// Monotonic transport sequence scoped by its Backend route identity.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(crate) struct BackendTransportSequence(u64);

impl BackendTransportSequence {
    pub(crate) const fn new(raw: u64) -> Self {
        Self(raw)
    }

    pub(crate) const fn get(self) -> u64 {
        self.0
    }
}

/// One Backend-local consumer instance subscribed to a sealed channel.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(crate) struct BackendConsumerSubscriptionIdentity {
    channel: BackendChannelIdentity,
    consumer_binding_id: RuntimeFilterBindingId,
    fragment_instance_id: UniqueId,
}

impl BackendConsumerSubscriptionIdentity {
    pub(crate) const fn new(
        channel: BackendChannelIdentity,
        consumer_binding_id: RuntimeFilterBindingId,
        fragment_instance_id: UniqueId,
    ) -> Self {
        Self {
            channel,
            consumer_binding_id,
            fragment_instance_id,
        }
    }

    pub(crate) const fn channel(self) -> BackendChannelIdentity {
        self.channel
    }

    pub(crate) const fn consumer_binding_id(self) -> RuntimeFilterBindingId {
        self.consumer_binding_id
    }

    pub(crate) const fn fragment_instance_id(self) -> UniqueId {
        self.fragment_instance_id
    }
}

impl BackendProducerStreamIdentity {
    pub(crate) const fn new(
        channel: BackendChannelIdentity,
        fragment_instance_id: UniqueId,
        partition_id: PartitionId,
    ) -> Self {
        Self {
            channel,
            fragment_instance_id,
            partition_id,
        }
    }

    pub(crate) const fn channel(self) -> BackendChannelIdentity {
        self.channel
    }

    pub(crate) const fn fragment_instance_id(self) -> UniqueId {
        self.fragment_instance_id
    }

    pub(crate) const fn partition_id(self) -> PartitionId {
        self.partition_id
    }
}
