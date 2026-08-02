//! Frontend-local, deterministic runtime-filter role graph.
//!
//! This deliberately models only scalar placement facts.  It is the policy
//! projection between the sealed Core schedule view and the protocol routing
//! DTO; it neither receives nor reconstructs the SQL runtime-filter graph.

use std::collections::{BTreeMap, BTreeSet};

use novarocks_types::UniqueId;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RoutingAvailability {
    /// A bounded replica set may publish directly to every consumer.
    AnyOf,
    /// Contributions must be collected by a deterministic aggregator first.
    Aggregated,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct RoutingBindingPlacement {
    pub(crate) binding_id: u32,
    pub(crate) backend_indices: BTreeSet<usize>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct RoutingProducerInstancePlacement {
    pub(crate) binding_id: u32,
    pub(crate) backend_idx: usize,
    pub(crate) fragment_instance_ids: BTreeSet<UniqueId>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct RoutingChannelInput {
    pub(crate) channel_id: u32,
    pub(crate) availability: RoutingAvailability,
    /// The number of distinct producer participants used for an AnyOf route.
    /// Zero is normalized to one, matching the frozen Core policy semantics.
    pub(crate) replica_redundancy: u32,
    pub(crate) producers: Vec<RoutingBindingPlacement>,
    pub(crate) consumers: Vec<RoutingBindingPlacement>,
    pub(crate) producer_instances: Vec<RoutingProducerInstancePlacement>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub(crate) enum RoutingRole {
    Producer(u32),
    Aggregator,
    Consumer(u32),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RoutingRouteKind {
    Loopback,
    ReplicaDirect,
    ToAggregator,
    FromAggregator,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub(crate) struct RoutingEndpoint {
    pub(crate) participant_id: u32,
    pub(crate) binding_id: u32,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct RoutingRouteEdge {
    pub(crate) edge_id: u32,
    pub(crate) kind: RoutingRouteKind,
    pub(crate) from: RoutingEndpoint,
    pub(crate) to: RoutingEndpoint,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct RoutingChannelRoleGraph {
    pub(crate) channel_id: u32,
    pub(crate) producers: BTreeMap<u32, BTreeSet<u32>>,
    pub(crate) consumers: BTreeMap<u32, BTreeSet<u32>>,
    pub(crate) aggregator: Option<u32>,
    pub(crate) routes: Vec<RoutingRouteEdge>,
}

#[derive(Default)]
pub(crate) struct RoutingEdgeAllocator {
    next: u32,
}

impl RoutingEdgeAllocator {
    pub(crate) fn new() -> Self {
        Self { next: 1 }
    }

    fn allocate(&mut self) -> Result<u32, String> {
        let edge_id = self.next;
        self.next = self
            .next
            .checked_add(1)
            .ok_or_else(|| "runtime filter route edge id overflow".to_string())?;
        Ok(edge_id)
    }
}

pub(crate) fn participant_id_for_backend(backend_idx: usize) -> Result<u32, String> {
    let ordinal = backend_idx
        .checked_add(1)
        .ok_or_else(|| "runtime filter backend index overflows participant identity".to_string())?;
    u32::try_from(ordinal)
        .map_err(|_| "runtime filter backend index exceeds participant identity width".to_string())
}

fn normalized_placements(
    placements: &[RoutingBindingPlacement],
    label: &'static str,
) -> Result<Vec<RoutingBindingPlacement>, String> {
    let mut normalized = placements.to_vec();
    normalized.sort_by(|left, right| {
        left.binding_id
            .cmp(&right.binding_id)
            .then_with(|| left.backend_indices.cmp(&right.backend_indices))
    });
    for placement in &normalized {
        if placement.binding_id == 0 {
            return Err(format!("runtime filter {label} binding id must be nonzero"));
        }
        for backend_idx in &placement.backend_indices {
            participant_id_for_backend(*backend_idx)?;
        }
    }
    Ok(normalized)
}

fn participants(placements: &[RoutingBindingPlacement]) -> BTreeSet<u32> {
    placements
        .iter()
        .flat_map(|placement| placement.backend_indices.iter().copied())
        .map(|backend_idx| {
            participant_id_for_backend(backend_idx)
                .expect("validated runtime-filter placement participant identity")
        })
        .collect()
}

/// Build one channel's role graph using the exact old deterministic choices:
/// a single colocated pair is loopback; AnyOf has direct routes from the
/// first sorted replica participants; all other coverage uses the first sorted
/// producer participant as aggregator.
pub(crate) fn build_channel_role_graph(
    input: &RoutingChannelInput,
    allocator: &mut RoutingEdgeAllocator,
) -> Result<RoutingChannelRoleGraph, String> {
    if input.channel_id == 0 {
        return Err("runtime filter routing channel id must be nonzero".to_string());
    }
    let producers = normalized_placements(&input.producers, "producer")?;
    let consumers = normalized_placements(&input.consumers, "consumer")?;
    let mut graph = RoutingChannelRoleGraph {
        channel_id: input.channel_id,
        producers: BTreeMap::new(),
        consumers: BTreeMap::new(),
        aggregator: None,
        routes: Vec::new(),
    };
    for placement in &producers {
        for backend_idx in &placement.backend_indices {
            graph
                .producers
                .entry(participant_id_for_backend(*backend_idx)?)
                .or_default()
                .insert(placement.binding_id);
        }
    }
    for placement in &consumers {
        for backend_idx in &placement.backend_indices {
            graph
                .consumers
                .entry(participant_id_for_backend(*backend_idx)?)
                .or_default()
                .insert(placement.binding_id);
        }
    }

    let producer_participants = participants(&producers);
    let consumer_participants = participants(&consumers);
    if producer_participants == consumer_participants && producer_participants.len() == 1 {
        let participant_id = *producer_participants
            .first()
            .expect("a single colocated participant exists");
        for producer in &producers {
            for consumer in &consumers {
                graph.routes.push(RoutingRouteEdge {
                    edge_id: allocator.allocate()?,
                    kind: RoutingRouteKind::Loopback,
                    from: RoutingEndpoint {
                        participant_id,
                        binding_id: producer.binding_id,
                    },
                    to: RoutingEndpoint {
                        participant_id,
                        binding_id: consumer.binding_id,
                    },
                });
            }
        }
        return Ok(graph);
    }

    match input.availability {
        RoutingAvailability::AnyOf => {
            let mut senders = producer_participants.iter().copied().collect::<Vec<_>>();
            let cap = usize::try_from(input.replica_redundancy)
                .unwrap_or(usize::MAX)
                .max(1)
                .min(senders.len());
            senders.truncate(cap);
            for producer in &producers {
                for participant_id in &senders {
                    if !producer.backend_indices.iter().copied().any(|backend_idx| {
                        participant_id_for_backend(backend_idx).ok() == Some(*participant_id)
                    }) {
                        continue;
                    }
                    for consumer in &consumers {
                        for backend_idx in &consumer.backend_indices {
                            graph.routes.push(RoutingRouteEdge {
                                edge_id: allocator.allocate()?,
                                kind: RoutingRouteKind::ReplicaDirect,
                                from: RoutingEndpoint {
                                    participant_id: *participant_id,
                                    binding_id: producer.binding_id,
                                },
                                to: RoutingEndpoint {
                                    participant_id: participant_id_for_backend(*backend_idx)?,
                                    binding_id: consumer.binding_id,
                                },
                            });
                        }
                    }
                }
            }
        }
        RoutingAvailability::Aggregated => {
            let aggregator = producer_participants.first().copied();
            graph.aggregator = aggregator;
            if let Some(aggregator) = aggregator {
                for producer in &producers {
                    for backend_idx in &producer.backend_indices {
                        graph.routes.push(RoutingRouteEdge {
                            edge_id: allocator.allocate()?,
                            kind: RoutingRouteKind::ToAggregator,
                            from: RoutingEndpoint {
                                participant_id: participant_id_for_backend(*backend_idx)?,
                                binding_id: producer.binding_id,
                            },
                            to: RoutingEndpoint {
                                participant_id: aggregator,
                                binding_id: producer.binding_id,
                            },
                        });
                    }
                }
                for consumer in &consumers {
                    for backend_idx in &consumer.backend_indices {
                        graph.routes.push(RoutingRouteEdge {
                            edge_id: allocator.allocate()?,
                            kind: RoutingRouteKind::FromAggregator,
                            from: RoutingEndpoint {
                                participant_id: aggregator,
                                binding_id: consumer.binding_id,
                            },
                            to: RoutingEndpoint {
                                participant_id: participant_id_for_backend(*backend_idx)?,
                                binding_id: consumer.binding_id,
                            },
                        });
                    }
                }
            }
        }
    }
    Ok(graph)
}

pub(crate) fn producer_instance_routes(
    input: &RoutingChannelInput,
    graph: &RoutingChannelRoleGraph,
) -> Result<BTreeMap<(u32, UniqueId), u32>, String> {
    let mut routes = BTreeMap::new();
    for placement in &input.producer_instances {
        if placement.binding_id == 0 {
            return Err("runtime filter producer instance binding id must be nonzero".to_string());
        }
        let participant_id = participant_id_for_backend(placement.backend_idx)?;
        if !graph
            .producers
            .get(&participant_id)
            .is_some_and(|bindings| bindings.contains(&placement.binding_id))
        {
            // The Core projection ignores an index entry which is not a
            // producer role for this channel.
            continue;
        }
        for instance_id in &placement.fragment_instance_ids {
            if instance_id.high() == 0 && instance_id.low() == 0 {
                return Err(
                    "runtime filter producer fragment instance id must be nonzero".to_string(),
                );
            }
            if let Some(previous) =
                routes.insert((placement.binding_id, *instance_id), participant_id)
                && previous != participant_id
            {
                return Err(format!(
                    "runtime filter producer instance binding {} is assigned to participants {} and {}",
                    placement.binding_id, previous, participant_id
                ));
            }
        }
    }
    Ok(routes)
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use super::{
        RoutingAvailability, RoutingBindingPlacement, RoutingChannelInput, RoutingEdgeAllocator,
        RoutingRouteKind, build_channel_role_graph,
    };

    fn placement(binding_id: u32, backend_indices: &[usize]) -> RoutingBindingPlacement {
        RoutingBindingPlacement {
            binding_id,
            backend_indices: backend_indices.iter().copied().collect::<BTreeSet<_>>(),
        }
    }

    #[test]
    fn role_graph_preserves_loopback_direct_and_aggregator_routes() {
        let mut allocator = RoutingEdgeAllocator::new();
        let loopback = build_channel_role_graph(
            &RoutingChannelInput {
                channel_id: 1,
                availability: RoutingAvailability::Aggregated,
                replica_redundancy: 2,
                producers: vec![placement(10, &[2])],
                consumers: vec![placement(11, &[2])],
                producer_instances: Vec::new(),
            },
            &mut allocator,
        )
        .expect("loopback graph is valid");
        assert!(
            loopback
                .routes
                .iter()
                .all(|route| route.kind == RoutingRouteKind::Loopback)
        );

        let direct = build_channel_role_graph(
            &RoutingChannelInput {
                channel_id: 2,
                availability: RoutingAvailability::AnyOf,
                replica_redundancy: 1,
                producers: vec![placement(20, &[0, 1])],
                consumers: vec![placement(21, &[2])],
                producer_instances: Vec::new(),
            },
            &mut allocator,
        )
        .expect("direct graph is valid");
        assert_eq!(direct.routes.len(), 1);
        assert_eq!(direct.routes[0].kind, RoutingRouteKind::ReplicaDirect);
        assert_eq!(direct.routes[0].from.participant_id, 1);

        let aggregated = build_channel_role_graph(
            &RoutingChannelInput {
                channel_id: 3,
                availability: RoutingAvailability::Aggregated,
                replica_redundancy: 2,
                producers: vec![placement(30, &[1, 3])],
                consumers: vec![placement(31, &[2])],
                producer_instances: Vec::new(),
            },
            &mut allocator,
        )
        .expect("aggregated graph is valid");
        assert_eq!(aggregated.aggregator, Some(2));
        assert!(
            aggregated
                .routes
                .iter()
                .any(|route| route.kind == RoutingRouteKind::ToAggregator)
        );
        assert!(
            aggregated
                .routes
                .iter()
                .any(|route| route.kind == RoutingRouteKind::FromAggregator)
        );
    }
}
