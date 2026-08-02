//! Frontend-owned projection of a deterministic role graph into routing DTOs.

use std::collections::{BTreeMap, BTreeSet};

use novarocks_protocol::{common, filter};

use super::role_graph::{
    RoutingChannelInput, RoutingChannelRoleGraph, RoutingEdgeAllocator, RoutingRole,
    RoutingRouteEdge, RoutingRouteKind, build_channel_role_graph, participant_id_for_backend,
    producer_instance_routes,
};

#[derive(Default)]
struct ChannelRoutingBuilder {
    local_roles: BTreeSet<RoutingRole>,
    producer_instances: BTreeMap<(u32, novarocks_types::UniqueId), u32>,
    inbound_edges: Vec<filter::RuntimeFilterRoutingEdgeView>,
    outbound_edges: Vec<filter::RuntimeFilterRoutingEdgeView>,
}

fn route_roles_and_kinds(route: &RoutingRouteEdge) -> (RoutingRole, RoutingRole, Vec<i32>) {
    let (source, target, allowed_kinds) = match route.kind {
        RoutingRouteKind::Loopback | RoutingRouteKind::ReplicaDirect => (
            RoutingRole::Producer(route.from.binding_id),
            RoutingRole::Consumer(route.to.binding_id),
            vec![
                filter::RuntimeFilterEnvelopeKind::Artifact as i32,
                filter::RuntimeFilterEnvelopeKind::FinalArtifact as i32,
                filter::RuntimeFilterEnvelopeKind::Unavailable as i32,
                filter::RuntimeFilterEnvelopeKind::CompletedWithoutArtifact as i32,
                filter::RuntimeFilterEnvelopeKind::DegradedLogical as i32,
            ],
        ),
        RoutingRouteKind::ToAggregator => (
            RoutingRole::Producer(route.from.binding_id),
            RoutingRole::Aggregator,
            vec![
                filter::RuntimeFilterEnvelopeKind::Contribution as i32,
                filter::RuntimeFilterEnvelopeKind::ProducerClosed as i32,
                filter::RuntimeFilterEnvelopeKind::ProducerUnavailable as i32,
            ],
        ),
        RoutingRouteKind::FromAggregator => (
            RoutingRole::Aggregator,
            RoutingRole::Consumer(route.to.binding_id),
            vec![
                filter::RuntimeFilterEnvelopeKind::Artifact as i32,
                filter::RuntimeFilterEnvelopeKind::FinalArtifact as i32,
                filter::RuntimeFilterEnvelopeKind::Unavailable as i32,
                filter::RuntimeFilterEnvelopeKind::CompletedWithoutArtifact as i32,
                filter::RuntimeFilterEnvelopeKind::DegradedLogical as i32,
            ],
        ),
    };
    (source, target, allowed_kinds)
}

fn role_to_wire(role: RoutingRole) -> filter::RuntimeFilterRouteRole {
    use filter::runtime_filter_route_role::Role;

    let role = match role {
        RoutingRole::Producer(binding_id) => Role::ProducerBindingId(binding_id),
        RoutingRole::Aggregator => Role::Aggregator(true),
        RoutingRole::Consumer(binding_id) => Role::ConsumerBindingId(binding_id),
    };
    filter::RuntimeFilterRouteRole { role: Some(role) }
}

fn endpoint_to_wire(
    participant_id: u32,
    role: RoutingRole,
) -> filter::RuntimeFilterRouteEndpointView {
    filter::RuntimeFilterRouteEndpointView {
        participant_id,
        role: Some(role_to_wire(role)),
    }
}

fn edge_to_wire(
    route: &RoutingRouteEdge,
    source_role: RoutingRole,
    target_role: RoutingRole,
    peer: filter::RuntimeFilterRoutePeer,
    allowed_kinds: Vec<i32>,
) -> filter::RuntimeFilterRoutingEdgeView {
    filter::RuntimeFilterRoutingEdgeView {
        route_edge_id: route.edge_id,
        source: Some(endpoint_to_wire(route.from.participant_id, source_role)),
        target: Some(endpoint_to_wire(route.to.participant_id, target_role)),
        peer: Some(peer),
        allowed_kinds,
    }
}

fn loopback_peer() -> filter::RuntimeFilterRoutePeer {
    filter::RuntimeFilterRoutePeer {
        peer: Some(filter::runtime_filter_route_peer::Peer::Loopback(true)),
    }
}

fn remote_peer(participant_id: u32, endpoint: &str) -> filter::RuntimeFilterRoutePeer {
    filter::RuntimeFilterRoutePeer {
        peer: Some(filter::runtime_filter_route_peer::Peer::Remote(
            filter::RuntimeFilterRemotePeer {
                participant_id,
                endpoint: endpoint.to_string(),
            },
        )),
    }
}

fn endpoint_map(backends: &BTreeMap<usize, String>) -> Result<BTreeMap<u32, &str>, String> {
    let mut endpoints = BTreeMap::new();
    for (backend_idx, endpoint) in backends {
        if endpoint.is_empty() {
            return Err(format!(
                "runtime filter backend {backend_idx} has an empty routing endpoint"
            ));
        }
        let participant_id = participant_id_for_backend(*backend_idx)?;
        if endpoints
            .insert(participant_id, endpoint.as_str())
            .is_some()
        {
            return Err(format!(
                "runtime filter duplicate routing participant {participant_id}"
            ));
        }
    }
    Ok(endpoints)
}

fn require_endpoint<'a>(
    endpoints: &'a BTreeMap<u32, &'a str>,
    participant_id: u32,
) -> Result<&'a str, String> {
    endpoints.get(&participant_id).copied().ok_or_else(|| {
        format!("runtime filter route references unknown participant {participant_id}")
    })
}

fn validate_channel_graph(channel: &RoutingChannelRoleGraph) -> Result<(), String> {
    let mut has_direct = false;
    let mut has_to_aggregator = false;
    let mut has_from_aggregator = false;
    for route in &channel.routes {
        match route.kind {
            RoutingRouteKind::Loopback | RoutingRouteKind::ReplicaDirect => {
                has_direct = true;
                if !channel
                    .producers
                    .get(&route.from.participant_id)
                    .is_some_and(|bindings| bindings.contains(&route.from.binding_id))
                    || !channel
                        .consumers
                        .get(&route.to.participant_id)
                        .is_some_and(|bindings| bindings.contains(&route.to.binding_id))
                {
                    return Err(format!(
                        "runtime filter direct route {} has a role not present on channel {}",
                        route.edge_id, channel.channel_id
                    ));
                }
            }
            RoutingRouteKind::ToAggregator => {
                has_to_aggregator = true;
                let aggregator = channel.aggregator.ok_or_else(|| {
                    format!(
                        "runtime filter channel {} has an aggregator route without an aggregator",
                        channel.channel_id
                    )
                })?;
                if route.to.participant_id != aggregator
                    || route.from.binding_id != route.to.binding_id
                    || !channel
                        .producers
                        .get(&route.from.participant_id)
                        .is_some_and(|bindings| bindings.contains(&route.from.binding_id))
                {
                    return Err(format!(
                        "runtime filter ToAggregator route {} is inconsistent on channel {}",
                        route.edge_id, channel.channel_id
                    ));
                }
            }
            RoutingRouteKind::FromAggregator => {
                has_from_aggregator = true;
                let aggregator = channel.aggregator.ok_or_else(|| {
                    format!(
                        "runtime filter channel {} has an aggregator route without an aggregator",
                        channel.channel_id
                    )
                })?;
                if route.from.participant_id != aggregator
                    || route.from.binding_id != route.to.binding_id
                    || !channel
                        .consumers
                        .get(&route.to.participant_id)
                        .is_some_and(|bindings| bindings.contains(&route.to.binding_id))
                {
                    return Err(format!(
                        "runtime filter FromAggregator route {} is inconsistent on channel {}",
                        route.edge_id, channel.channel_id
                    ));
                }
            }
        }
    }
    match channel.aggregator {
        Some(aggregator) if has_direct => Err(format!(
            "runtime filter channel {} aggregator {} mixes direct and aggregator routes",
            channel.channel_id, aggregator
        )),
        Some(aggregator) if !has_to_aggregator || !has_from_aggregator => Err(format!(
            "runtime filter channel {} aggregator {} requires both route directions",
            channel.channel_id, aggregator
        )),
        None if has_to_aggregator || has_from_aggregator => Err(format!(
            "runtime filter channel {} has an aggregator route without an aggregator",
            channel.channel_id
        )),
        _ => Ok(()),
    }
}

/// Project Frontend-local placement and route facts into the native protocol
/// DTOs. Keys are backend indices, while all wire participant identities are
/// exactly `backend_idx + 1`.
pub(crate) fn compile_routing(
    inputs: &[RoutingChannelInput],
    backends: &BTreeMap<usize, String>,
) -> Result<BTreeMap<usize, Vec<filter::RuntimeFilterChannelRoutingView>>, String> {
    let endpoints = endpoint_map(backends)?;
    let mut sorted_inputs = inputs.to_vec();
    sorted_inputs.sort_by_key(|input| input.channel_id);
    let mut allocator = RoutingEdgeAllocator::new();
    let mut per_participant: BTreeMap<u32, BTreeMap<u32, ChannelRoutingBuilder>> = BTreeMap::new();
    let mut seen_channels = BTreeSet::new();

    for input in &sorted_inputs {
        if !seen_channels.insert(input.channel_id) {
            return Err(format!(
                "runtime filter routing repeats channel {}",
                input.channel_id
            ));
        }
        let channel = build_channel_role_graph(input, &mut allocator)?;
        validate_channel_graph(&channel)?;

        for (participant_id, bindings) in &channel.producers {
            require_endpoint(&endpoints, *participant_id)?;
            per_participant
                .entry(*participant_id)
                .or_default()
                .entry(channel.channel_id)
                .or_default()
                .local_roles
                .extend(bindings.iter().copied().map(RoutingRole::Producer));
        }
        for (participant_id, bindings) in &channel.consumers {
            require_endpoint(&endpoints, *participant_id)?;
            per_participant
                .entry(*participant_id)
                .or_default()
                .entry(channel.channel_id)
                .or_default()
                .local_roles
                .extend(bindings.iter().copied().map(RoutingRole::Consumer));
        }
        if let Some(participant_id) = channel.aggregator {
            require_endpoint(&endpoints, participant_id)?;
            per_participant
                .entry(participant_id)
                .or_default()
                .entry(channel.channel_id)
                .or_default()
                .local_roles
                .insert(RoutingRole::Aggregator);
        }

        for route in &channel.routes {
            let source_endpoint = require_endpoint(&endpoints, route.from.participant_id)?;
            let target_endpoint = require_endpoint(&endpoints, route.to.participant_id)?;
            let (source_role, target_role, allowed_kinds) = route_roles_and_kinds(route);
            let (outbound_peer, inbound_peer) =
                if route.from.participant_id == route.to.participant_id {
                    (loopback_peer(), loopback_peer())
                } else {
                    (
                        remote_peer(route.to.participant_id, target_endpoint),
                        remote_peer(route.from.participant_id, source_endpoint),
                    )
                };
            let outbound = edge_to_wire(
                route,
                source_role,
                target_role,
                outbound_peer,
                allowed_kinds.clone(),
            );
            let inbound =
                edge_to_wire(route, source_role, target_role, inbound_peer, allowed_kinds);
            per_participant
                .entry(route.from.participant_id)
                .or_default()
                .entry(channel.channel_id)
                .or_default()
                .outbound_edges
                .push(outbound);
            per_participant
                .entry(route.to.participant_id)
                .or_default()
                .entry(channel.channel_id)
                .or_default()
                .inbound_edges
                .push(inbound);
        }

        let producer_instances = producer_instance_routes(input, &channel)?;
        for channels in per_participant.values_mut() {
            if let Some(builder) = channels.get_mut(&channel.channel_id) {
                builder.producer_instances = producer_instances.clone();
            }
        }
    }

    let mut by_backend = BTreeMap::new();
    for (participant_id, channels) in per_participant {
        let backend_idx = usize::try_from(participant_id)
            .map_err(|_| {
                "runtime filter participant identity does not fit backend index".to_string()
            })?
            .checked_sub(1)
            .ok_or_else(|| "runtime filter participant identity must be nonzero".to_string())?;
        let mut wire_channels = Vec::new();
        for (channel_id, builder) in channels {
            if builder.local_roles.is_empty()
                && builder.inbound_edges.is_empty()
                && builder.outbound_edges.is_empty()
            {
                continue;
            }
            let producer_instances = builder
                .producer_instances
                .into_iter()
                .map(|((binding_id, instance_id), participant_id)| {
                    filter::RuntimeFilterProducerInstanceRoute {
                        binding_id,
                        fragment_instance_id: Some(common::UniqueId {
                            hi: instance_id.high(),
                            lo: instance_id.low(),
                        }),
                        participant_id,
                    }
                })
                .collect();
            wire_channels.push(filter::RuntimeFilterChannelRoutingView {
                channel_id,
                local_roles: builder.local_roles.into_iter().map(role_to_wire).collect(),
                producer_instances,
                inbound_edges: builder.inbound_edges,
                outbound_edges: builder.outbound_edges,
            });
        }
        if !wire_channels.is_empty() {
            by_backend.insert(backend_idx, wire_channels);
        }
    }
    Ok(by_backend)
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};

    use novarocks_protocol::filter;
    use novarocks_types::UniqueId;

    use super::compile_routing;
    use crate::runtime_filter::deployment::role_graph::{
        RoutingAvailability, RoutingBindingPlacement, RoutingChannelInput,
        RoutingProducerInstancePlacement,
    };

    fn placement(binding_id: u32, backends: &[usize]) -> RoutingBindingPlacement {
        RoutingBindingPlacement {
            binding_id,
            backend_indices: backends.iter().copied().collect::<BTreeSet<_>>(),
        }
    }

    fn backends() -> BTreeMap<usize, String> {
        BTreeMap::from([
            (0, "be-0:9010".to_string()),
            (1, "be-1:9010".to_string()),
            (2, "be-2:9010".to_string()),
        ])
    }

    #[test]
    fn projection_preserves_remote_endpoints_routes_and_instances() {
        let projected = compile_routing(
            &[RoutingChannelInput {
                channel_id: 7,
                availability: RoutingAvailability::AnyOf,
                replica_redundancy: 1,
                producers: vec![placement(10, &[0, 1])],
                consumers: vec![placement(11, &[2])],
                producer_instances: vec![RoutingProducerInstancePlacement {
                    binding_id: 10,
                    backend_idx: 0,
                    fragment_instance_ids: BTreeSet::from([UniqueId::new(3, 4)]),
                }],
            }],
            &backends(),
        )
        .expect("routing projection succeeds");

        let producer = &projected[&0][0];
        assert_eq!(producer.local_roles.len(), 1);
        assert_eq!(producer.producer_instances.len(), 1);
        let edge = &producer.outbound_edges[0];
        assert_eq!(edge.route_edge_id, 1);
        assert_eq!(
            edge.allowed_kinds[0],
            filter::RuntimeFilterEnvelopeKind::Artifact as i32
        );
        let peer = edge.peer.as_ref().expect("peer is present");
        assert!(matches!(
            peer.peer,
            Some(filter::runtime_filter_route_peer::Peer::Remote(ref remote))
                if remote.participant_id == 3 && remote.endpoint == "be-2:9010"
        ));
        let consumer = &projected[&2][0];
        assert_eq!(consumer.inbound_edges.len(), 1);
        assert_eq!(consumer.producer_instances.len(), 1);
    }

    #[test]
    fn aggregator_projection_keeps_aggregator_role_and_both_route_families() {
        let projected = compile_routing(
            &[RoutingChannelInput {
                channel_id: 8,
                availability: RoutingAvailability::Aggregated,
                replica_redundancy: 3,
                producers: vec![placement(20, &[0, 1])],
                consumers: vec![placement(21, &[2])],
                producer_instances: Vec::new(),
            }],
            &backends(),
        )
        .expect("aggregator routing projection succeeds");
        let aggregator = &projected[&0][0];
        assert!(aggregator.local_roles.iter().any(|role| {
            matches!(
                role.role,
                Some(filter::runtime_filter_route_role::Role::Aggregator(true))
            )
        }));
        assert!(aggregator.outbound_edges.iter().any(|edge| {
            edge.allowed_kinds
                .contains(&(filter::RuntimeFilterEnvelopeKind::Contribution as i32))
        }));
        assert!(aggregator.outbound_edges.iter().any(|edge| {
            edge.allowed_kinds
                .contains(&(filter::RuntimeFilterEnvelopeKind::Artifact as i32))
        }));
    }

    #[test]
    fn loopback_uses_loopback_peer() {
        let projected = compile_routing(
            &[RoutingChannelInput {
                channel_id: 9,
                availability: RoutingAvailability::Aggregated,
                replica_redundancy: 1,
                producers: vec![placement(30, &[1])],
                consumers: vec![placement(31, &[1])],
                producer_instances: Vec::new(),
            }],
            &backends(),
        )
        .expect("loopback projection succeeds");
        let channel = &projected[&1][0];
        assert!(matches!(
            channel.outbound_edges[0]
                .peer
                .as_ref()
                .and_then(|peer| peer.peer.as_ref()),
            Some(filter::runtime_filter_route_peer::Peer::Loopback(true))
        ));
    }
}
