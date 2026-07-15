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

use std::sync::Arc;

use crate::common::types::UniqueId;
use crate::runtime_filter::core::channel::RuntimeFilterChannel;
use crate::runtime_filter::model::contract::{BindingId, ChannelId};
use crate::runtime_filter::port::identity::{PartitionId, ProducerSequence};
use crate::runtime_filter::port::ordered_bound::OrderedBoundUpdate;
use crate::runtime_filter::port::producer::{
    OrderedBoundProducerAdapter, ProducerAdapter, ProducerFailureReason, RuntimeContractViolation,
    SubmitOutcome,
};
use crate::runtime_filter::port::support::{
    RuntimeFilterMemoryAccount, TemporaryContributionLease,
};
use crate::runtime_filter::port::value_domain::ValueDomainDelta;

use super::ActionDispatcher;

pub(super) struct ServiceProducerAdapter {
    channel_id: ChannelId,
    channel: Arc<RuntimeFilterChannel>,
    binding_id: BindingId,
    fragment_instance_id: UniqueId,
    memory_account: Arc<dyn RuntimeFilterMemoryAccount>,
    dispatcher: Arc<ActionDispatcher>,
    #[cfg(test)]
    before_dispatch: std::sync::Mutex<Option<Arc<dyn Fn() + Send + Sync>>>,
}

impl ServiceProducerAdapter {
    pub(super) fn new(
        channel_id: ChannelId,
        channel: Arc<RuntimeFilterChannel>,
        binding_id: BindingId,
        fragment_instance_id: UniqueId,
        memory_account: Arc<dyn RuntimeFilterMemoryAccount>,
        dispatcher: Arc<ActionDispatcher>,
    ) -> Self {
        Self {
            channel_id,
            channel,
            binding_id,
            fragment_instance_id,
            memory_account,
            dispatcher,
            #[cfg(test)]
            before_dispatch: std::sync::Mutex::new(None),
        }
    }

    #[cfg(test)]
    pub(super) fn set_before_dispatch(&self, hook: Arc<dyn Fn() + Send + Sync>) {
        *self.before_dispatch.lock().unwrap() = Some(hook);
    }

    fn finish(
        &self,
        action: crate::runtime_filter::core::channel::ChannelAction,
    ) -> Result<SubmitOutcome, RuntimeContractViolation> {
        let outcome = action.outcome();
        #[cfg(test)]
        self.dispatcher
            .reserve_core_before_hook(self.channel_id, &action);
        #[cfg(test)]
        let hook = self.before_dispatch.lock().unwrap().take();
        #[cfg(test)]
        if let Some(hook) = hook {
            hook();
        }
        self.dispatcher.dispatch(self.channel_id, action)?;
        Ok(outcome)
    }
}

impl ProducerAdapter for ServiceProducerAdapter {
    fn submit(
        &self,
        partition_id: PartitionId,
        sequence: ProducerSequence,
        delta: ValueDomainDelta,
    ) -> Result<SubmitOutcome, RuntimeContractViolation> {
        self.channel
            .authorize_submit(self.binding_id, self.fragment_instance_id, partition_id)?;
        let Ok(bytes) = delta.estimated_contribution_bytes() else {
            return self
                .channel
                .reject_submit_resource_exhausted(
                    self.binding_id,
                    self.fragment_instance_id,
                    partition_id,
                    sequence,
                    &delta,
                )
                .and_then(|action| self.finish(action));
        };
        let Ok(lease) = TemporaryContributionLease::try_new(self.memory_account.clone(), bytes)
        else {
            return self
                .channel
                .reject_submit_resource_exhausted(
                    self.binding_id,
                    self.fragment_instance_id,
                    partition_id,
                    sequence,
                    &delta,
                )
                .and_then(|action| self.finish(action));
        };
        self.channel
            .submit(
                self.binding_id,
                self.fragment_instance_id,
                partition_id,
                sequence,
                delta,
                lease,
            )
            .and_then(|action| self.finish(action))
    }

    fn close_partition(
        &self,
        partition_id: PartitionId,
        terminal_sequence: ProducerSequence,
    ) -> Result<SubmitOutcome, RuntimeContractViolation> {
        self.channel
            .close_partition(
                self.binding_id,
                self.fragment_instance_id,
                partition_id,
                terminal_sequence,
            )
            .and_then(|action| self.finish(action))
    }

    fn fail(
        &self,
        reason: ProducerFailureReason,
    ) -> Result<SubmitOutcome, RuntimeContractViolation> {
        self.channel
            .fail_instance(self.binding_id, self.fragment_instance_id, reason)
            .and_then(|action| self.finish(action))
    }
}

impl OrderedBoundProducerAdapter for ServiceProducerAdapter {
    fn submit_bound(
        &self,
        partition_id: PartitionId,
        sequence: ProducerSequence,
        update: OrderedBoundUpdate,
    ) -> Result<SubmitOutcome, RuntimeContractViolation> {
        self.channel
            .authorize_submit(self.binding_id, self.fragment_instance_id, partition_id)?;
        let Some(bytes) = update.canonical_contribution_bytes() else {
            return self
                .channel
                .reject_ordered_submit_resource_exhausted(
                    self.binding_id,
                    self.fragment_instance_id,
                    partition_id,
                    sequence,
                    &update,
                )
                .and_then(|action| self.finish(action));
        };
        let Ok(lease) = TemporaryContributionLease::try_new(self.memory_account.clone(), bytes)
        else {
            return self
                .channel
                .reject_ordered_submit_resource_exhausted(
                    self.binding_id,
                    self.fragment_instance_id,
                    partition_id,
                    sequence,
                    &update,
                )
                .and_then(|action| self.finish(action));
        };
        self.channel
            .submit_ordered(
                self.binding_id,
                self.fragment_instance_id,
                partition_id,
                sequence,
                update,
                lease,
            )
            .and_then(|action| self.finish(action))
    }

    fn close_partition(
        &self,
        partition_id: PartitionId,
        terminal_sequence: ProducerSequence,
    ) -> Result<SubmitOutcome, RuntimeContractViolation> {
        self.channel
            .close_ordered_partition(
                self.binding_id,
                self.fragment_instance_id,
                partition_id,
                terminal_sequence,
            )
            .and_then(|action| self.finish(action))
    }

    fn fail(
        &self,
        reason: ProducerFailureReason,
    ) -> Result<SubmitOutcome, RuntimeContractViolation> {
        self.channel
            .fail_instance(self.binding_id, self.fragment_instance_id, reason)
            .and_then(|action| self.finish(action))
    }
}
