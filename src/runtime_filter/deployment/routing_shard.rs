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

use std::collections::{BTreeMap, BTreeSet};

use crate::common::types::UniqueId;
use crate::coordinator::scheduler::LiveBackendSnapshot;
use crate::runtime::endpoint::RuntimeEndpoint;
use crate::runtime_filter::deployment::role_graph::{RoleGraph, RouteEdge, RouteKind};
use crate::runtime_filter::deployment::{BindingInstanceIndex, DeploymentError};
use crate::runtime_filter::model::contract::{BindingId, ChannelId};
use crate::runtime_filter::port::identity::{DeploymentEpoch, RuntimeFilterParticipantId};
use crate::runtime_filter::port::routing::{
    RuntimeFilterChannelRoutingView, RuntimeFilterRouteEndpointView, RuntimeFilterRoutePeer,
    RuntimeFilterRouteRole, RuntimeFilterRoutingEdgeView, RuntimeFilterRoutingShard,
};
use crate::runtime_filter::port::transport::RuntimeFilterEnvelopeKind;

#[derive(Default)]
struct ChannelShardBuilder {
    local_roles: BTreeSet<RuntimeFilterRouteRole>,
    producer_instances: BTreeMap<(BindingId, UniqueId), RuntimeFilterParticipantId>,
    inbound_edges: Vec<RuntimeFilterRoutingEdgeView>,
    outbound_edges: Vec<RuntimeFilterRoutingEdgeView>,
}

fn route_roles_and_kinds(
    edge: &RouteEdge,
) -> (
    RuntimeFilterRouteRole,
    RuntimeFilterRouteRole,
    BTreeSet<RuntimeFilterEnvelopeKind>,
) {
    match edge.kind {
        RouteKind::Loopback | RouteKind::ReplicaDirect => (
            RuntimeFilterRouteRole::Producer(edge.from.binding),
            RuntimeFilterRouteRole::Consumer(edge.to.binding),
            BTreeSet::from([
                RuntimeFilterEnvelopeKind::Artifact,
                RuntimeFilterEnvelopeKind::Unavailable,
            ]),
        ),
        RouteKind::ToAggregator => (
            RuntimeFilterRouteRole::Producer(edge.from.binding),
            RuntimeFilterRouteRole::Aggregator,
            BTreeSet::from([
                RuntimeFilterEnvelopeKind::Contribution,
                RuntimeFilterEnvelopeKind::ProducerClosed,
                RuntimeFilterEnvelopeKind::Unavailable,
            ]),
        ),
        RouteKind::FromAggregator => (
            RuntimeFilterRouteRole::Aggregator,
            RuntimeFilterRouteRole::Consumer(edge.to.binding),
            BTreeSet::from([
                RuntimeFilterEnvelopeKind::Artifact,
                RuntimeFilterEnvelopeKind::Unavailable,
            ]),
        ),
    }
}

fn endpoint_for(
    endpoints: &BTreeMap<RuntimeFilterParticipantId, RuntimeEndpoint>,
    participant: RuntimeFilterParticipantId,
) -> Result<RuntimeEndpoint, DeploymentError> {
    endpoints
        .get(&participant)
        .cloned()
        .ok_or(DeploymentError::UnknownRouteParticipant { participant })
}

fn invalid_routing_shard(detail: impl Into<String>) -> DeploymentError {
    DeploymentError::InvalidRoutingShard {
        detail: detail.into(),
    }
}

pub(crate) fn project_routing_shards(
    epoch: DeploymentEpoch,
    role_graph: &RoleGraph,
    instances: &BindingInstanceIndex,
    backends: &LiveBackendSnapshot,
) -> Result<BTreeMap<RuntimeFilterParticipantId, RuntimeFilterRoutingShard>, DeploymentError> {
    let mut endpoints = BTreeMap::new();
    for (backend_idx, socket_addr) in backends.entries() {
        let participant = RuntimeFilterParticipantId::new(*backend_idx as u32);
        if endpoints
            .insert(participant, RuntimeEndpoint::from_socket_addr(*socket_addr))
            .is_some()
        {
            return Err(DeploymentError::DuplicateBackend {
                backend_idx: *backend_idx,
            });
        }
    }

    let mut edge_ids = BTreeSet::new();
    for (channel_id, channel) in &role_graph.channels {
        if *channel_id != channel.channel_id {
            return Err(invalid_routing_shard(format!(
                "role graph channel key {} does not match channel {}",
                channel_id.get(),
                channel.channel_id.get()
            )));
        }
        for edge in &channel.routes {
            if edge.edge_id.get() == 0 {
                return Err(invalid_routing_shard(format!(
                    "route edge on channel {} has zero id",
                    channel_id.get()
                )));
            }
            if !edge_ids.insert(edge.edge_id) {
                return Err(DeploymentError::DuplicateRouteEdge {
                    edge_id: edge.edge_id,
                });
            }
            if edge.channel != *channel_id {
                return Err(invalid_routing_shard(format!(
                    "route edge {} carries channel {} but belongs to channel {}",
                    edge.edge_id.get(),
                    edge.channel.get(),
                    channel_id.get()
                )));
            }
        }
    }

    let mut per_participant: BTreeMap<
        RuntimeFilterParticipantId,
        BTreeMap<ChannelId, ChannelShardBuilder>,
    > = BTreeMap::new();

    for (channel_id, channel) in &role_graph.channels {
        for (participant, bindings) in &channel.producers {
            endpoint_for(&endpoints, *participant)?;
            let builder = per_participant
                .entry(*participant)
                .or_default()
                .entry(*channel_id)
                .or_default();
            builder.local_roles.extend(
                bindings
                    .iter()
                    .copied()
                    .map(RuntimeFilterRouteRole::Producer),
            );
        }
        for (participant, bindings) in &channel.consumers {
            endpoint_for(&endpoints, *participant)?;
            let builder = per_participant
                .entry(*participant)
                .or_default()
                .entry(*channel_id)
                .or_default();
            builder.local_roles.extend(
                bindings
                    .iter()
                    .copied()
                    .map(RuntimeFilterRouteRole::Consumer),
            );
        }
        if let Some(participant) = channel.aggregator {
            endpoint_for(&endpoints, participant)?;
            per_participant
                .entry(participant)
                .or_default()
                .entry(*channel_id)
                .or_default()
                .local_roles
                .insert(RuntimeFilterRouteRole::Aggregator);
        }

        for edge in &channel.routes {
            let source_endpoint = endpoint_for(&endpoints, edge.from.participant)?;
            let target_endpoint = endpoint_for(&endpoints, edge.to.participant)?;
            let (source_role, target_role, allowed_kinds) = route_roles_and_kinds(edge);
            let source = RuntimeFilterRouteEndpointView::new(edge.from.participant, source_role);
            let target = RuntimeFilterRouteEndpointView::new(edge.to.participant, target_role);
            let (outbound_peer, inbound_peer) = if edge.from.participant == edge.to.participant {
                (
                    RuntimeFilterRoutePeer::Loopback,
                    RuntimeFilterRoutePeer::Loopback,
                )
            } else {
                (
                    RuntimeFilterRoutePeer::Remote {
                        participant_id: edge.to.participant,
                        endpoint: target_endpoint,
                    },
                    RuntimeFilterRoutePeer::Remote {
                        participant_id: edge.from.participant,
                        endpoint: source_endpoint,
                    },
                )
            };
            let outbound = RuntimeFilterRoutingEdgeView::new(
                *channel_id,
                edge.edge_id,
                source.clone(),
                target.clone(),
                outbound_peer,
                allowed_kinds.clone(),
            )
            .map_err(|error| invalid_routing_shard(error.to_string()))?;
            let inbound = RuntimeFilterRoutingEdgeView::new(
                *channel_id,
                edge.edge_id,
                source,
                target,
                inbound_peer,
                allowed_kinds,
            )
            .map_err(|error| invalid_routing_shard(error.to_string()))?;
            per_participant
                .entry(edge.from.participant)
                .or_default()
                .entry(*channel_id)
                .or_default()
                .outbound_edges
                .push(outbound);
            per_participant
                .entry(edge.to.participant)
                .or_default()
                .entry(*channel_id)
                .or_default()
                .inbound_edges
                .push(inbound);
        }

        let mut producer_instances = BTreeMap::new();
        for (participant, bindings) in &channel.producers {
            for binding in bindings {
                let Some(fragment_instances) =
                    instances.get(&(*channel_id, *binding, *participant))
                else {
                    continue;
                };
                for fragment_instance_id in fragment_instances {
                    if let Some(previous_participant) =
                        producer_instances.insert((*binding, *fragment_instance_id), *participant)
                        && previous_participant != *participant
                    {
                        return Err(DeploymentError::AmbiguousProducerInstance {
                            channel: *channel_id,
                            binding: *binding,
                            fragment_instance_id: *fragment_instance_id,
                        });
                    }
                }
            }
        }
        for builder in per_participant
            .values_mut()
            .filter_map(|channels| channels.get_mut(channel_id))
        {
            builder.producer_instances = producer_instances.clone();
        }
    }

    let mut shards = BTreeMap::new();
    for (participant, channels) in per_participant {
        let mut channel_views = BTreeMap::new();
        for (channel_id, builder) in channels {
            let view = RuntimeFilterChannelRoutingView::new(
                channel_id,
                builder.local_roles,
                builder.producer_instances,
                builder.inbound_edges,
                builder.outbound_edges,
            )
            .map_err(|error| invalid_routing_shard(error.to_string()))?;
            channel_views.insert(channel_id, view);
        }
        let shard = RuntimeFilterRoutingShard::new(epoch, participant, channel_views)
            .map_err(|error| invalid_routing_shard(error.to_string()))?;
        shards.insert(participant, shard);
    }
    Ok(shards)
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};

    use crate::common::types::UniqueId;
    use crate::coordinator::scheduler::LiveBackendSnapshot;
    use crate::runtime_filter::deployment::role_graph::{
        ChannelRoleGraph, RoleGraph, RouteEdge, RouteEndpoint, RouteKind,
    };
    use crate::runtime_filter::deployment::{BindingInstanceIndex, DeploymentError};
    use crate::runtime_filter::model::contract::{BindingId, ChannelId};
    use crate::runtime_filter::port::identity::{
        DeploymentEpoch, RouteEdgeId, RuntimeFilterParticipantId,
    };
    use crate::runtime_filter::port::routing::{
        RuntimeFilterRoutePeer, RuntimeFilterRouteRole, RuntimeFilterRoutingEdgeView,
    };
    use crate::runtime_filter::port::transport::RuntimeFilterEnvelopeKind;

    use super::project_routing_shards;

    fn pid(raw: u32) -> RuntimeFilterParticipantId {
        RuntimeFilterParticipantId::new(raw)
    }

    fn finst(raw: i64) -> UniqueId {
        UniqueId {
            hi: raw,
            lo: raw + 100,
        }
    }

    fn backends() -> LiveBackendSnapshot {
        LiveBackendSnapshot::new(vec![
            (2, "10.0.0.2:9060".parse().unwrap()),
            (7, "10.0.0.7:9060".parse().unwrap()),
            (11, "10.0.0.11:9060".parse().unwrap()),
        ])
    }

    fn all_of_fixture() -> (RoleGraph, BindingInstanceIndex) {
        let channel_id = ChannelId::new(1);
        let producer_binding = BindingId::new(10);
        let consumer_binding = BindingId::new(20);
        let mut channel = ChannelRoleGraph::empty(channel_id);
        channel
            .producers
            .insert(pid(2), BTreeSet::from([producer_binding]));
        channel
            .producers
            .insert(pid(7), BTreeSet::from([producer_binding]));
        channel
            .consumers
            .insert(pid(2), BTreeSet::from([consumer_binding]));
        channel
            .consumers
            .insert(pid(11), BTreeSet::from([consumer_binding]));
        channel.aggregator = Some(pid(2));
        channel.routes = vec![
            RouteEdge {
                channel: channel_id,
                edge_id: RouteEdgeId::new(1),
                kind: RouteKind::ToAggregator,
                from: RouteEndpoint {
                    participant: pid(2),
                    binding: producer_binding,
                },
                to: RouteEndpoint {
                    participant: pid(2),
                    binding: producer_binding,
                },
            },
            RouteEdge {
                channel: channel_id,
                edge_id: RouteEdgeId::new(2),
                kind: RouteKind::ToAggregator,
                from: RouteEndpoint {
                    participant: pid(7),
                    binding: producer_binding,
                },
                to: RouteEndpoint {
                    participant: pid(2),
                    binding: producer_binding,
                },
            },
            RouteEdge {
                channel: channel_id,
                edge_id: RouteEdgeId::new(3),
                kind: RouteKind::FromAggregator,
                from: RouteEndpoint {
                    participant: pid(2),
                    binding: consumer_binding,
                },
                to: RouteEndpoint {
                    participant: pid(2),
                    binding: consumer_binding,
                },
            },
            RouteEdge {
                channel: channel_id,
                edge_id: RouteEdgeId::new(4),
                kind: RouteKind::FromAggregator,
                from: RouteEndpoint {
                    participant: pid(2),
                    binding: consumer_binding,
                },
                to: RouteEndpoint {
                    participant: pid(11),
                    binding: consumer_binding,
                },
            },
        ];
        let graph = RoleGraph {
            channels: BTreeMap::from([(channel_id, channel)]),
        };
        let instances = BTreeMap::from([
            (
                (channel_id, producer_binding, pid(2)),
                BTreeSet::from([finst(200)]),
            ),
            (
                (channel_id, producer_binding, pid(7)),
                BTreeSet::from([finst(100)]),
            ),
            (
                (channel_id, consumer_binding, pid(2)),
                BTreeSet::from([finst(300)]),
            ),
            (
                (channel_id, consumer_binding, pid(11)),
                BTreeSet::from([finst(400)]),
            ),
        ]);
        (graph, instances)
    }

    fn edge_by_id(
        edges: &[RuntimeFilterRoutingEdgeView],
        edge_id: u32,
    ) -> &RuntimeFilterRoutingEdgeView {
        edges
            .iter()
            .find(|edge| edge.route_edge_id() == RouteEdgeId::new(edge_id))
            .expect("route edge")
    }

    #[test]
    fn all_of_projects_self_aggregator_edges_as_loopback_by_participant_equality() {
        let (graph, instances) = all_of_fixture();
        let shards =
            project_routing_shards(DeploymentEpoch::new(9), &graph, &instances, &backends())
                .expect("projection succeeds");

        let aggregator = shards.get(&pid(2)).expect("aggregator shard");
        let channel = aggregator.channel(ChannelId::new(1)).expect("channel");
        assert_eq!(
            channel.local_roles(),
            &BTreeSet::from([
                RuntimeFilterRouteRole::Producer(BindingId::new(10)),
                RuntimeFilterRouteRole::Aggregator,
                RuntimeFilterRouteRole::Consumer(BindingId::new(20)),
            ])
        );
        assert!(matches!(
            edge_by_id(channel.outbound_edges(), 1).peer(),
            RuntimeFilterRoutePeer::Loopback
        ));
        assert!(matches!(
            edge_by_id(channel.inbound_edges(), 1).peer(),
            RuntimeFilterRoutePeer::Loopback
        ));
        assert!(matches!(
            edge_by_id(channel.outbound_edges(), 3).peer(),
            RuntimeFilterRoutePeer::Loopback
        ));
        assert!(matches!(
            edge_by_id(channel.inbound_edges(), 3).peer(),
            RuntimeFilterRoutePeer::Loopback
        ));

        let producer = shards.get(&pid(7)).expect("producer shard");
        let remote_to_aggregator = edge_by_id(
            producer
                .channel(ChannelId::new(1))
                .unwrap()
                .outbound_edges(),
            2,
        );
        match remote_to_aggregator.peer() {
            RuntimeFilterRoutePeer::Remote {
                participant_id,
                endpoint,
            } => {
                assert_eq!(*participant_id, pid(2));
                assert_eq!(endpoint.as_host_port(), "10.0.0.2:9060");
            }
            RuntimeFilterRoutePeer::Loopback => panic!("expected remote aggregator"),
        }

        let consumer = shards.get(&pid(11)).expect("consumer shard");
        let remote_from_aggregator = edge_by_id(
            consumer.channel(ChannelId::new(1)).unwrap().inbound_edges(),
            4,
        );
        match remote_from_aggregator.peer() {
            RuntimeFilterRoutePeer::Remote {
                participant_id,
                endpoint,
            } => {
                assert_eq!(*participant_id, pid(2));
                assert_eq!(endpoint.as_host_port(), "10.0.0.2:9060");
            }
            RuntimeFilterRoutePeer::Loopback => panic!("expected remote aggregator"),
        }
    }

    #[test]
    fn sparse_backend_ids_use_snapshot_ids_not_vector_positions() {
        let (graph, instances) = all_of_fixture();
        let shards =
            project_routing_shards(DeploymentEpoch::new(9), &graph, &instances, &backends())
                .expect("projection succeeds");

        let aggregator = shards.get(&pid(2)).expect("aggregator shard");
        let edge = edge_by_id(
            aggregator
                .channel(ChannelId::new(1))
                .unwrap()
                .outbound_edges(),
            4,
        );
        match edge.peer() {
            RuntimeFilterRoutePeer::Remote {
                participant_id,
                endpoint,
            } => {
                assert_eq!(*participant_id, pid(11));
                assert_eq!(endpoint.as_host_port(), "10.0.0.11:9060");
            }
            RuntimeFilterRoutePeer::Loopback => panic!("expected sparse remote backend"),
        }
    }

    #[test]
    fn every_global_edge_has_one_mirrored_outbound_and_inbound_projection() {
        let (graph, instances) = all_of_fixture();
        let shards =
            project_routing_shards(DeploymentEpoch::new(9), &graph, &instances, &backends())
                .expect("projection succeeds");

        let projected = |outbound: bool| {
            let mut edges = Vec::new();
            for shard in shards.values() {
                for channel in shard.channels().values() {
                    let incident = if outbound {
                        channel.outbound_edges()
                    } else {
                        channel.inbound_edges()
                    };
                    edges.extend(incident.iter().map(|edge| {
                        (
                            edge.route_edge_id(),
                            edge.source().clone(),
                            edge.target().clone(),
                            edge.allowed_kinds().clone(),
                        )
                    }));
                }
            }
            edges.sort_unstable_by_key(|entry| entry.0);
            edges
        };

        assert_eq!(projected(true), projected(false));
    }

    #[test]
    fn producer_finst_index_is_available_on_aggregator_target_shard() {
        let (graph, instances) = all_of_fixture();
        let shards =
            project_routing_shards(DeploymentEpoch::new(9), &graph, &instances, &backends())
                .expect("projection succeeds");

        let channel = shards[&pid(2)]
            .channel(ChannelId::new(1))
            .expect("aggregator channel");
        assert_eq!(
            channel.producer_participant(BindingId::new(10), finst(100)),
            Some(pid(7))
        );
    }

    #[test]
    fn participant_zero_is_a_valid_routing_participant() {
        let channel_id = ChannelId::new(1);
        let producer_binding = BindingId::new(10);
        let consumer_binding = BindingId::new(20);
        let mut channel = ChannelRoleGraph::empty(channel_id);
        channel
            .producers
            .insert(pid(0), BTreeSet::from([producer_binding]));
        channel
            .consumers
            .insert(pid(2), BTreeSet::from([consumer_binding]));
        channel.routes.push(RouteEdge {
            channel: channel_id,
            edge_id: RouteEdgeId::new(1),
            kind: RouteKind::ReplicaDirect,
            from: RouteEndpoint {
                participant: pid(0),
                binding: producer_binding,
            },
            to: RouteEndpoint {
                participant: pid(2),
                binding: consumer_binding,
            },
        });
        let graph = RoleGraph {
            channels: BTreeMap::from([(channel_id, channel)]),
        };
        let instances = BTreeMap::from([(
            (channel_id, producer_binding, pid(0)),
            BTreeSet::from([finst(0)]),
        )]);
        let backends = LiveBackendSnapshot::new(vec![
            (0, "10.0.0.1:9060".parse().unwrap()),
            (2, "10.0.0.2:9060".parse().unwrap()),
        ]);

        let shards = project_routing_shards(DeploymentEpoch::new(9), &graph, &instances, &backends)
            .expect("participant zero must remain valid");

        assert!(shards.contains_key(&pid(0)));
        assert_eq!(
            shards[&pid(2)]
                .channel(channel_id)
                .unwrap()
                .producer_participant(producer_binding, finst(0)),
            Some(pid(0))
        );
    }

    #[test]
    fn unavailable_is_allowed_on_every_data_route_but_ack_is_not() {
        let (graph, instances) = all_of_fixture();
        let shards =
            project_routing_shards(DeploymentEpoch::new(9), &graph, &instances, &backends())
                .expect("projection succeeds");
        let aggregator = shards[&pid(2)].channel(ChannelId::new(1)).unwrap();

        let to_aggregator = edge_by_id(aggregator.inbound_edges(), 2);
        assert_eq!(
            to_aggregator.allowed_kinds(),
            &BTreeSet::from([
                RuntimeFilterEnvelopeKind::Contribution,
                RuntimeFilterEnvelopeKind::ProducerClosed,
                RuntimeFilterEnvelopeKind::Unavailable,
            ])
        );
        let from_aggregator = edge_by_id(aggregator.outbound_edges(), 4);
        assert_eq!(
            from_aggregator.allowed_kinds(),
            &BTreeSet::from([
                RuntimeFilterEnvelopeKind::Artifact,
                RuntimeFilterEnvelopeKind::Unavailable,
            ])
        );
        assert!(
            shards
                .values()
                .all(|shard| shard.channels().values().all(|channel| channel
                    .inbound_edges()
                    .iter()
                    .chain(channel.outbound_edges())
                    .all(|edge| !edge
                        .allowed_kinds()
                        .contains(&RuntimeFilterEnvelopeKind::Ack))))
        );
    }

    #[test]
    fn projector_rejects_duplicate_backend_id_unknown_endpoint_and_duplicate_edge_id() {
        let (graph, instances) = all_of_fixture();
        let duplicate_backends = LiveBackendSnapshot::new(vec![
            (2, "10.0.0.2:9060".parse().unwrap()),
            (2, "10.0.0.22:9060".parse().unwrap()),
            (7, "10.0.0.7:9060".parse().unwrap()),
            (11, "10.0.0.11:9060".parse().unwrap()),
        ]);
        assert_eq!(
            project_routing_shards(
                DeploymentEpoch::new(9),
                &graph,
                &instances,
                &duplicate_backends,
            ),
            Err(DeploymentError::DuplicateBackend { backend_idx: 2 })
        );

        let missing_backend = LiveBackendSnapshot::new(vec![
            (2, "10.0.0.2:9060".parse().unwrap()),
            (7, "10.0.0.7:9060".parse().unwrap()),
        ]);
        assert_eq!(
            project_routing_shards(
                DeploymentEpoch::new(9),
                &graph,
                &instances,
                &missing_backend,
            ),
            Err(DeploymentError::UnknownRouteParticipant {
                participant: pid(11),
            })
        );

        let mut duplicate_edge_graph = graph.clone();
        duplicate_edge_graph
            .channels
            .get_mut(&ChannelId::new(1))
            .unwrap()
            .routes[3]
            .edge_id = RouteEdgeId::new(2);
        assert_eq!(
            project_routing_shards(
                DeploymentEpoch::new(9),
                &duplicate_edge_graph,
                &instances,
                &backends(),
            ),
            Err(DeploymentError::DuplicateRouteEdge {
                edge_id: RouteEdgeId::new(2),
            })
        );
    }

    #[test]
    fn projector_rejects_one_producer_instance_on_multiple_participants() {
        let (graph, mut instances) = all_of_fixture();
        instances
            .entry((ChannelId::new(1), BindingId::new(10), pid(2)))
            .or_default()
            .insert(finst(100));

        assert_eq!(
            project_routing_shards(DeploymentEpoch::new(9), &graph, &instances, &backends(),),
            Err(DeploymentError::AmbiguousProducerInstance {
                channel: ChannelId::new(1),
                binding: BindingId::new(10),
                fragment_instance_id: finst(100),
            })
        );
    }
}
