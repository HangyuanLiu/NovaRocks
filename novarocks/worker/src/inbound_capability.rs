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

//! Worker-owned admission authority for task exchange destinations.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};

use novarocks_execution_contract::{
    ExchangeSource, FragmentNodeId, QueryContextRef, TaskDescriptor, TaskIdentity,
};
use novarocks_types::{QueryExecutionId, UniqueId};

use crate::{HostRejection, IngressRejection, authorize_inbound_frame};

const CAPABILITY_LOCK: &str = "task inbound capability lock";

/// Which task, if any, may receive one inbound exchange frame.
///
/// A descriptor already freezes its complete inbound topology, so the answer
/// is a single lookup on the kernel key the frame carries, followed by the
/// descriptor's own authorization. Installation is exclusive on that key:
/// two live tasks sharing one key would make a frame ambiguous.
#[derive(Debug, Default)]
struct TaskInboundCapabilityState {
    installed: HashMap<UniqueId, Arc<TaskDescriptor>>,
    /// A descriptor carries no frontend process identity. The worker context
    /// owner admits at most one context per execution, so the execution is
    /// the complete key this data-plane owner must fence.
    closed_executions: HashSet<QueryExecutionId>,
}

#[derive(Debug, Default)]
pub struct TaskInboundCapabilities {
    state: Mutex<TaskInboundCapabilityState>,
}

/// The task and frozen source an admitted frame belongs to.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct InboundFrameAdmission {
    destination: TaskIdentity,
    source: ExchangeSource,
}

impl InboundFrameAdmission {
    pub const fn destination(self) -> TaskIdentity {
        self.destination
    }

    pub const fn source(self) -> ExchangeSource {
        self.source
    }
}

/// The worker's ownership verdict for an inbound exchange destination.
///
/// `NotHeld` is intentionally distinct from `Refused`: another independent
/// destination owner may hold the same kernel key in a composed data plane.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum InboundFrameClaim {
    NotHeld,
    Authorized,
    Refused(String),
}

impl TaskInboundCapabilities {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// Authorizes one frame from only frozen task facts.
    pub fn authorize_frame(
        &self,
        destination_kernel_key: UniqueId,
        destination_node_id: FragmentNodeId,
        source_kernel_key: UniqueId,
        sender_ordinal: u32,
        sender_count: u32,
    ) -> Result<InboundFrameAdmission, IngressRejection> {
        let descriptor = {
            let state = self.state.lock().expect(CAPABILITY_LOCK);
            let descriptor = state
                .installed
                .get(&destination_kernel_key)
                .map(Arc::clone)
                .ok_or(IngressRejection::UnknownDestinationTask)?;
            if state
                .closed_executions
                .contains(&descriptor.identity().query_execution_id())
            {
                return Err(IngressRejection::UnknownDestinationTask);
            }
            descriptor
        };
        let source = authorize_inbound_frame(
            &descriptor,
            destination_kernel_key,
            destination_node_id,
            source_kernel_key,
            sender_ordinal,
            sender_count,
        )?;
        Ok(InboundFrameAdmission {
            destination: descriptor.identity(),
            source,
        })
    }

    /// States whether this worker owns a destination and, if it does, whether
    /// the frozen route is legal. The wire adapter composes this verdict with
    /// any other destination owners.
    pub fn claim_frame(
        &self,
        destination_kernel_key: UniqueId,
        destination_node_id: FragmentNodeId,
        source_kernel_key: UniqueId,
        sender_ordinal: u32,
        sender_count: u32,
    ) -> InboundFrameClaim {
        let descriptor = {
            let state = self.state.lock().expect(CAPABILITY_LOCK);
            state
                .installed
                .get(&destination_kernel_key)
                .map(|descriptor| {
                    (
                        Arc::clone(descriptor),
                        state
                            .closed_executions
                            .contains(&descriptor.identity().query_execution_id()),
                    )
                })
        };
        let Some((descriptor, context_closed)) = descriptor else {
            return InboundFrameClaim::NotHeld;
        };
        if context_closed {
            return InboundFrameClaim::Refused(format!(
                "task {} belongs to a query context whose data-plane admission is closed",
                descriptor.identity()
            ));
        }
        match authorize_inbound_frame(
            &descriptor,
            destination_kernel_key,
            destination_node_id,
            source_kernel_key,
            sender_ordinal,
            sender_count,
        ) {
            Ok(_) => InboundFrameClaim::Authorized,
            Err(rejection) => InboundFrameClaim::Refused(format!(
                "task {} froze this destination but {rejection}",
                descriptor.identity()
            )),
        }
    }

    pub fn len(&self) -> usize {
        self.state.lock().expect(CAPABILITY_LOCK).installed.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn install(&self, descriptor: Arc<TaskDescriptor>) -> Result<(), HostRejection> {
        let key = descriptor.fragment_instance_id();
        let mut state = self.state.lock().expect(CAPABILITY_LOCK);
        if state
            .closed_executions
            .contains(&descriptor.identity().query_execution_id())
        {
            return Err(HostRejection::new(
                novarocks_execution_contract::TaskFailureCategory::Protocol,
                format!(
                    "task {} cannot install inbound capability after its query context closed",
                    descriptor.identity()
                ),
            ));
        }
        if let Some(existing) = state.installed.get(&key) {
            return Err(HostRejection::new(
                novarocks_execution_contract::TaskFailureCategory::Protocol,
                format!(
                    "task {} cannot claim kernel key {key}, which task {} already holds",
                    descriptor.identity(),
                    existing.identity()
                ),
            ));
        }
        state.installed.insert(key, descriptor);
        Ok(())
    }

    pub fn close_context(&self, context: QueryContextRef) {
        self.state
            .lock()
            .expect(CAPABILITY_LOCK)
            .closed_executions
            .insert(context.query_execution_id());
    }

    pub fn forget_context(&self, context: QueryContextRef) {
        let mut state = self.state.lock().expect(CAPABILITY_LOCK);
        debug_assert!(state.installed.values().all(|descriptor| {
            descriptor.identity().query_execution_id() != context.query_execution_id()
        }));
        state
            .closed_executions
            .remove(&context.query_execution_id());
    }

    /// Withdraws exactly this task's capability. Identity is re-checked so a
    /// stale rollback cannot evict a newer owner of the same kernel key.
    pub fn remove(&self, descriptor: &TaskDescriptor) {
        let mut state = self.state.lock().expect(CAPABILITY_LOCK);
        let key = descriptor.fragment_instance_id();
        if state
            .installed
            .get(&key)
            .is_some_and(|held| held.identity() == descriptor.identity())
        {
            state.installed.remove(&key);
        }
    }
}
