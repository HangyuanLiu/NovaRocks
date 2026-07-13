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

use std::collections::BTreeMap;
use std::sync::Arc;

use crate::runtime_filter::core::channel::ChannelAction;
use crate::runtime_filter::port::identity::RouteEdgeId;
use crate::runtime_filter::port::subscription::{DeliveryTerminal, SnapshotDelivery};

pub(crate) struct LoopbackRouter {
    routes: BTreeMap<RouteEdgeId, Arc<dyn SnapshotDelivery>>,
}

impl LoopbackRouter {
    pub(crate) fn new(routes: BTreeMap<RouteEdgeId, Arc<dyn SnapshotDelivery>>) -> Self {
        Self { routes }
    }

    pub(crate) fn contains_route(&self, route_edge_id: RouteEdgeId) -> bool {
        self.routes.contains_key(&route_edge_id)
    }

    pub(crate) fn route(
        &self,
        route_edge_ids: &[RouteEdgeId],
        action: &ChannelAction,
    ) -> Vec<RouteEdgeId> {
        let deliveries = route_edge_ids
            .iter()
            .filter_map(|route_edge_id| {
                self.routes
                    .get(route_edge_id)
                    .cloned()
                    .map(|delivery| (*route_edge_id, delivery))
            })
            .collect::<Vec<_>>();
        match action {
            ChannelAction::Completed { snapshot, .. } => {
                for (route_edge_id, delivery) in &deliveries {
                    delivery.deliver(*route_edge_id, snapshot.clone());
                }
            }
            ChannelAction::Unavailable { reason, .. } => {
                for (route_edge_id, delivery) in &deliveries {
                    delivery.terminal(*route_edge_id, DeliveryTerminal::Unavailable(*reason));
                }
            }
            ChannelAction::Cancelled { .. } => {
                for (route_edge_id, delivery) in &deliveries {
                    delivery.terminal(*route_edge_id, DeliveryTerminal::Cancelled);
                }
            }
            ChannelAction::None | ChannelAction::Progress { .. } => return Vec::new(),
        }
        deliveries
            .into_iter()
            .map(|(route_edge_id, _)| route_edge_id)
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex, Weak};

    use crate::runtime_filter::core::channel::ChannelAction;
    use crate::runtime_filter::port::identity::RouteEdgeId;
    use crate::runtime_filter::port::producer::SubmitOutcome;
    use crate::runtime_filter::port::subscription::{DeliveryTerminal, SnapshotDelivery};
    use crate::runtime_filter::port::value_domain::LogicalSnapshot;

    use super::LoopbackRouter;

    struct ReentrantDelivery {
        router: Mutex<Weak<LoopbackRouter>>,
        terminal_calls: AtomicUsize,
        reentered: AtomicBool,
    }

    impl SnapshotDelivery for ReentrantDelivery {
        fn deliver(&self, _route_edge_id: RouteEdgeId, _snapshot: Arc<LogicalSnapshot>) {
            if let Some(router) = self.router.lock().unwrap().upgrade() {
                assert!(router.contains_route(RouteEdgeId::new(1)));
            }
        }

        fn terminal(&self, _route_edge_id: RouteEdgeId, _outcome: DeliveryTerminal) {
            self.terminal_calls.fetch_add(1, Ordering::SeqCst);
            if let Some(router) = self.router.lock().unwrap().upgrade() {
                assert!(router.contains_route(RouteEdgeId::new(1)));
                self.reentered.store(true, Ordering::SeqCst);
            }
        }
    }

    #[test]
    fn reentrant_delivery_runs_without_router_lock_and_progress_is_not_broadcast() {
        let delivery = Arc::new(ReentrantDelivery {
            router: Mutex::new(Weak::new()),
            terminal_calls: AtomicUsize::new(0),
            reentered: AtomicBool::new(false),
        });
        let router = Arc::new(LoopbackRouter::new(BTreeMap::from([(
            RouteEdgeId::new(1),
            delivery.clone() as Arc<dyn SnapshotDelivery>,
        )])));
        *delivery.router.lock().unwrap() = Arc::downgrade(&router);
        assert_eq!(
            router.route(
                &[RouteEdgeId::new(1)],
                &ChannelAction::Cancelled {
                    order: 0,
                    events: Vec::new(),
                },
            ),
            vec![RouteEdgeId::new(1)]
        );
        assert!(router.contains_route(RouteEdgeId::new(1)));
        assert_eq!(delivery.terminal_calls.load(Ordering::SeqCst), 1);
        assert!(delivery.reentered.load(Ordering::SeqCst));
        assert!(
            router
                .route(
                    &[RouteEdgeId::new(1)],
                    &ChannelAction::Progress {
                        order: None,
                        outcome: SubmitOutcome::Applied,
                        events: Vec::new(),
                    },
                )
                .is_empty()
        );
        assert_eq!(delivery.terminal_calls.load(Ordering::SeqCst), 1);
    }
}
