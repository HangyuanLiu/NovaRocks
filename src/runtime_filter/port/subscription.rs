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
use std::time::Duration;

use crate::common::types::UniqueId;
use crate::runtime_filter::model::contract::BindingId;

use super::identity::RouteEdgeId;
use super::value_domain::LogicalSnapshot;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct SubscriptionRequest {
    binding_id: BindingId,
    fragment_instance_id: UniqueId,
}

impl SubscriptionRequest {
    pub(crate) const fn new(binding_id: BindingId, fragment_instance_id: UniqueId) -> Self {
        Self {
            binding_id,
            fragment_instance_id,
        }
    }

    pub(crate) const fn binding_id(self) -> BindingId {
        self.binding_id
    }

    pub(crate) const fn fragment_instance_id(self) -> UniqueId {
        self.fragment_instance_id
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum UnavailableReason {
    ResourceLimit,
    IncompleteCoverage,
    ProducerFailed,
    RouteUnavailable,
}

#[derive(Debug)]
pub(crate) enum AcquireOutcome {
    Completed(Arc<LogicalSnapshot>),
    Unavailable(UnavailableReason),
    Cancelled,
    TimedOut,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum DeliveryTerminal {
    Unavailable(UnavailableReason),
    Cancelled,
}

pub(crate) trait SnapshotDelivery: Send + Sync {
    fn deliver(&self, route_edge_id: RouteEdgeId, snapshot: Arc<LogicalSnapshot>);
    fn terminal(&self, route_edge_id: RouteEdgeId, outcome: DeliveryTerminal);
}

pub(crate) trait BlockingSnapshotSubscription: Send + Sync {
    fn acquire(&self, timeout: Duration) -> AcquireOutcome;
}

#[cfg(test)]
mod tests {
    use crate::common::types::UniqueId;
    use crate::runtime_filter::model::contract::BindingId;

    use super::{DeliveryTerminal, SubscriptionRequest, UnavailableReason};

    fn router_terminal_kind(terminal: DeliveryTerminal) -> &'static str {
        match terminal {
            DeliveryTerminal::Unavailable(_) => "unavailable",
            DeliveryTerminal::Cancelled => "cancelled",
        }
    }

    #[test]
    fn router_terminal_boundary_excludes_completed_and_caller_local_timeout() {
        assert_eq!(
            router_terminal_kind(DeliveryTerminal::Unavailable(
                UnavailableReason::RouteUnavailable
            )),
            "unavailable"
        );
        assert_eq!(
            router_terminal_kind(DeliveryTerminal::Cancelled),
            "cancelled"
        );
    }

    #[test]
    fn subscription_request_keeps_binding_and_fragment_instance_identity() {
        let request = SubscriptionRequest::new(BindingId::new(3), UniqueId { hi: 4, lo: 5 });

        assert_eq!(request.binding_id().get(), 3);
        assert_eq!(request.fragment_instance_id(), UniqueId { hi: 4, lo: 5 });
    }
}
