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

use super::*;

use std::collections::{BTreeMap, BTreeSet};

use crate::{
    ArtifactInputField, CoverageSet, Distribution, Edge, EdgeId, Fragment, FragmentId,
    FragmentSink, NodeId, NodeKind, PhysicalPlan, RowMultiplicity, SealedArtifactRef,
    SealedArtifactSinkSpec, ValueId, ValueOrigin,
};

pub(crate) fn validate_fragment_graph(plan: &PhysicalPlan, errors: &mut ValidationContext) {
    let mut indegree = plan
        .fragments()
        .keys()
        .copied()
        .map(|fragment| (fragment, 0_usize))
        .collect::<BTreeMap<_, _>>();
    let mut successors: BTreeMap<FragmentId, Vec<FragmentId>> = BTreeMap::new();
    for edge in plan.edges().values() {
        if indegree.contains_key(&edge.source.fragment)
            && let Some(degree) = indegree.get_mut(&edge.destination.fragment)
        {
            *degree += 1;
            successors
                .entry(edge.source.fragment)
                .or_default()
                .push(edge.destination.fragment);
        }
    }
    let mut ready = indegree
        .iter()
        .filter_map(|(fragment, degree)| (*degree == 0).then_some(*fragment))
        .collect::<Vec<_>>();
    let mut visited = 0_usize;
    while let Some(fragment) = ready.pop() {
        visited += 1;
        if let Some(next) = successors.get(&fragment) {
            for successor in next {
                if let Some(degree) = indegree.get_mut(successor) {
                    *degree -= 1;
                    if *degree == 0 {
                        ready.push(*successor);
                    }
                }
            }
        }
    }
    if visited != plan.fragments().len() {
        errors.push(ValidationError::new(
            "edges",
            "fragment exchange graph contains a cycle",
        ));
    }
}

pub(crate) fn validate_fragment_sink(fragment: &Fragment, errors: &mut ValidationContext) {
    let path = format!("fragments[{}].sink", fragment.id().get());
    let root_output = fragment
        .nodes()
        .get(&fragment.root())
        .map(|root| root.output.columns.as_ref())
        .unwrap_or_default();
    let root_values = root_output.iter().copied().collect::<BTreeSet<_>>();
    let root_multiplicity = fragment
        .nodes()
        .get(&fragment.root())
        .map(|root| root.output_properties.row_multiplicity);
    let mut edge_ids = BTreeSet::new();
    match fragment.sink() {
        FragmentSink::Multicast { edges } => {
            if edges.is_empty() {
                errors.push(ValidationError::new(
                    &path,
                    "multi-destination sink has no edges",
                ));
            }
            for edge in edges {
                if !edge_ids.insert(*edge) {
                    errors.push(ValidationError::new(
                        &path,
                        "sink contains a duplicate edge destination",
                    ));
                }
            }
        }
        FragmentSink::Router { effect, routes } => {
            if root_multiplicity != Some(RowMultiplicity::SingleCopy) {
                errors.push(ValidationError::new(
                    &path,
                    "router requires single-copy row ownership",
                ));
            }
            require_value(fragment, *effect, &path, errors);
            if !root_values.contains(effect) {
                errors.push(ValidationError::new(
                    &path,
                    "router effect is absent from the fragment root output",
                ));
            }
            let change_events = change_event_source(fragment, *effect);
            if change_events.is_none() {
                errors.push(ValidationError::new(
                    &path,
                    "router effect is not the exact output of a change-event expansion",
                ));
            }
            if routes.is_empty() {
                errors.push(ValidationError::new(
                    &path,
                    "multi-destination sink has no edges",
                ));
            }
            let mut route_ids = BTreeSet::new();
            for (ordinal, route) in routes.iter().enumerate() {
                if route.route_id == crate::ConnectorWriteRouteId::from_bytes([0; 32])
                    || !route_ids.insert(route.route_id)
                    || usize::try_from(route.write_target_ordinal.get()).ok() != Some(ordinal)
                {
                    errors.push(ValidationError::new(
                        &path,
                        "router routes require unique identities and dense target ordinals in route order",
                    ));
                }
                let mut effects = BTreeSet::new();
                if route.accepted_effects.is_empty()
                    || route
                        .accepted_effects
                        .iter()
                        .any(|effect| !effects.insert(*effect))
                {
                    errors.push(ValidationError::new(
                        &path,
                        "router route requires unique accepted effects",
                    ));
                }
                let mut tokens = BTreeSet::new();
                let mut input_values = BTreeSet::new();
                if route.input_mapping.is_empty()
                    || route.input_mapping.iter().any(|(token, value)| {
                        !tokens.insert(*token) || !root_values.contains(value)
                    })
                {
                    errors.push(ValidationError::new(
                        &path,
                        "router route requires unique input tokens and root-output values",
                    ));
                }
                input_values.extend(route.input_mapping.iter().map(|(_, value)| *value));
                if route.input_mapping.iter().any(|(_, value)| value == effect) {
                    errors.push(ValidationError::new(
                        &path,
                        "router data input cannot contain its generated effect value",
                    ));
                }
                let mut partition_values = BTreeSet::new();
                if route
                    .partition_by
                    .iter()
                    .any(|value| !partition_values.insert(*value) || !input_values.contains(value))
                {
                    errors.push(ValidationError::new(
                        &path,
                        "router partition values must be unique route inputs",
                    ));
                }
                if !edge_ids.insert(route.edge) {
                    errors.push(ValidationError::new(
                        &path,
                        "sink contains a duplicate edge destination",
                    ));
                }
            }
            let covered_effects = routes
                .iter()
                .flat_map(|route| route.accepted_effects.iter().copied())
                .collect::<BTreeSet<_>>();
            if change_events.is_some_and(|events| {
                events
                    .iter()
                    .any(|event| !covered_effects.contains(&event.effect))
            }) {
                errors.push(ValidationError::new(
                    &path,
                    "router routes do not cover every emitted change-event effect",
                ));
            }
        }
        FragmentSink::SealedArtifact(spec) => validate_artifact_sink(fragment, spec, errors),
        FragmentSink::Result => {
            if root_multiplicity != Some(RowMultiplicity::SingleCopy) {
                errors.push(ValidationError::new(
                    &path,
                    "result sink requires single-copy row ownership",
                ));
            }
        }
        FragmentSink::Stream { .. } | FragmentSink::Noop => {}
    }
}

pub(crate) fn import_origin_matches(
    origin: &ValueOrigin,
    edge: EdgeId,
    kind: crate::EdgeKind,
    source_fragment: FragmentId,
    source_value: ValueId,
) -> bool {
    match (kind, origin) {
        (
            crate::EdgeKind::CteMulticast,
            ValueOrigin::CteImport {
                edge: value_edge,
                producer_fragment,
                producer_value,
            },
        ) => {
            *value_edge == edge
                && *producer_fragment == source_fragment
                && *producer_value == source_value
        }
        (
            crate::EdgeKind::Stream | crate::EdgeKind::ChangeStreamRouter,
            ValueOrigin::ExchangeImport {
                edge: value_edge,
                source_value: value_source,
            },
        ) => *value_edge == edge && *value_source == source_value,
        _ => false,
    }
}

pub(crate) fn validate_node_graph(fragment: &Fragment, errors: &mut ValidationContext) {
    let path = format!("fragments[{}].nodes", fragment.id().get());
    let mut remaining_inputs = BTreeMap::new();
    let mut dependents: BTreeMap<NodeId, Vec<NodeId>> = BTreeMap::new();
    let mut ready = Vec::new();
    for (id, node) in fragment.nodes() {
        let inputs = node
            .inputs
            .iter()
            .copied()
            .filter(|input| fragment.nodes().contains_key(input))
            .collect::<BTreeSet<_>>();
        remaining_inputs.insert(*id, inputs.len());
        if inputs.is_empty() {
            ready.push(*id);
        }
        for input in inputs {
            dependents.entry(input).or_default().push(*id);
        }
    }
    let mut processed = 0_usize;
    while let Some(id) = ready.pop() {
        processed += 1;
        if let Some(users) = dependents.get(&id) {
            for user in users {
                if let Some(remaining) = remaining_inputs.get_mut(user) {
                    *remaining -= 1;
                    if *remaining == 0 {
                        ready.push(*user);
                    }
                }
            }
        }
    }
    if processed != fragment.nodes().len() {
        errors.push(ValidationError::new(&path, "node graph contains a cycle"));
    }

    let mut visited = BTreeSet::new();
    let mut pending = vec![fragment.root()];
    while let Some(id) = pending.pop() {
        if !visited.insert(id) {
            continue;
        }
        if let Some(node) = fragment.nodes().get(&id) {
            pending.extend(node.inputs.iter().copied());
        }
    }
    if visited.len() != fragment.nodes().len() {
        errors.push(ValidationError::new(
            &path,
            "fragment contains nodes unreachable from its root",
        ));
    }
}

pub(crate) fn validate_edge(
    plan: &PhysicalPlan,
    edge: &Edge,
    root_port_indexes: &mut BTreeMap<FragmentId, ValuePortIndex>,
    errors: &mut ValidationContext,
) {
    let path = format!("edges[{}]", edge.id.get());
    let Some(source) = plan.fragments().get(&edge.source.fragment) else {
        errors.push(ValidationError::new(
            &path,
            "source fragment is not defined",
        ));
        return;
    };
    let Some(destination) = plan.fragments().get(&edge.destination.fragment) else {
        errors.push(ValidationError::new(
            &path,
            "destination fragment is not defined",
        ));
        return;
    };
    if edge.source.fragment == edge.destination.fragment {
        errors.push(ValidationError::new(
            &path,
            "edge cannot connect a fragment to itself",
        ));
    }
    if edge.source.projection.len() != edge.destination.receive_mapping.len() {
        errors.push(ValidationError::new(
            &path,
            "source projection and receive mapping have different widths",
        ));
    }
    if let Some(root) = source.nodes().get(&source.root()) {
        let root_values = root_port_indexes
            .entry(source.id())
            .or_insert_with(|| ValuePortIndex::new(&root.output.columns));
        if edge
            .source
            .projection
            .iter()
            .any(|value| !root_values.contains(value))
        {
            errors.push(ValidationError::new(
                &path,
                "edge projects a value absent from the source fragment root output",
            ));
        }
        if root.output_properties.row_multiplicity != edge.partitioning.source_multiplicity {
            errors.push(ValidationError::new(
                &path,
                "edge source row multiplicity differs from the source fragment root",
            ));
        }
    }
    validate_distribution(
        source,
        &edge.partitioning.source,
        "edge.source_partitioning",
        errors,
    );
    validate_distribution(
        destination,
        &edge.partitioning.destination,
        "edge.destination_partitioning",
        errors,
    );
    for (ordinal, (projected, (mapped_source, imported))) in edge
        .source
        .projection
        .iter()
        .zip(edge.destination.receive_mapping.iter())
        .enumerate()
    {
        if projected != mapped_source {
            errors.push(ValidationError::new(
                &path,
                format!("receive mapping source differs at ordinal {ordinal}"),
            ));
        }
        match (
            source.values().get(projected),
            destination.values().get(imported),
        ) {
            (Some(source_value), Some(destination_value)) => {
                if source_value.ty != destination_value.ty {
                    errors.push(ValidationError::new(
                        &path,
                        format!("source and destination types differ at ordinal {ordinal}"),
                    ));
                }
                if !import_origin_matches(
                    &destination_value.origin,
                    edge.id,
                    edge.kind,
                    edge.source.fragment,
                    *projected,
                ) {
                    errors.push(ValidationError::new(
                        &path,
                        format!("destination value origin differs at ordinal {ordinal}"),
                    ));
                }
            }
            _ => errors.push(ValidationError::new(
                &path,
                format!("mapping references an undefined value at ordinal {ordinal}"),
            )),
        }
    }
    validate_edge_partitioning(edge, &path, errors);
    match destination.nodes().get(&edge.destination.node) {
        Some(node)
            if matches!(
                &node.kind,
                NodeKind::ExchangeSource { edge: node_edge, imports }
                    if *node_edge == edge.id
                        && imports.as_ref() == edge.destination.receive_mapping.as_ref()
            ) =>
        {
            if node.output_properties.distribution != edge.partitioning.destination
                || node.output_properties.row_multiplicity
                    != edge.partitioning.destination_multiplicity
                || !node.output_properties.ordering.is_empty()
            {
                errors.push(ValidationError::new(
                    &path,
                    "exchange receiver properties differ from the edge destination contract",
                ));
            }
        }
        Some(_) => errors.push(ValidationError::new(
            &path,
            "destination node is not the exact exchange receiver",
        )),
        None => errors.push(ValidationError::new(
            &path,
            "destination node is not defined",
        )),
    }
}

pub(crate) fn validate_edge_partitioning(edge: &Edge, path: &str, errors: &mut ValidationContext) {
    validate_mapped_partitioning(
        &edge.partitioning,
        &edge.destination.receive_mapping,
        path,
        errors,
    );
}

pub(crate) fn validate_mapped_partitioning(
    partitioning: &crate::EdgePartitioning,
    mapping: &[(ValueId, ValueId)],
    path: &str,
    errors: &mut ValidationContext,
) {
    let layout_valid = match (&partitioning.source, &partitioning.destination) {
        (Distribution::Unconstrained, Distribution::Unconstrained)
        | (Distribution::Singleton, Distribution::Singleton)
        | (Distribution::RoundRobin, Distribution::RoundRobin)
        | (Distribution::Broadcast, Distribution::Broadcast) => true,
        (
            Distribution::Hash {
                keys: source_keys,
                scheme: source_scheme,
            },
            Distribution::Hash {
                keys: destination_keys,
                scheme: destination_scheme,
            },
        ) => {
            source_scheme == destination_scheme
                && mapped_partition_keys_match(source_keys, destination_keys, mapping)
        }
        (
            Distribution::BucketShuffle {
                keys: source_keys,
                scheme: source_scheme,
            },
            Distribution::BucketShuffle {
                keys: destination_keys,
                scheme: destination_scheme,
            },
        ) => {
            source_scheme == destination_scheme
                && mapped_partition_keys_match(source_keys, destination_keys, mapping)
        }
        _ => false,
    };
    let multiplicity_valid = if partitioning.destination == Distribution::Broadcast {
        partitioning.source_multiplicity == RowMultiplicity::SingleCopy
            && partitioning.destination_multiplicity == RowMultiplicity::Replicated
    } else {
        partitioning.source_multiplicity == partitioning.destination_multiplicity
    };
    if !layout_valid || !multiplicity_valid {
        errors.push(ValidationError::new(
            path,
            "edge source and destination partitioning or row multiplicity are inconsistent",
        ));
    }
}

pub(crate) fn mapped_partition_keys_match(
    source_keys: &[ValueId],
    destination_keys: &[ValueId],
    mapping: &[(ValueId, ValueId)],
) -> bool {
    let mapping = ValueMappingIndex::from_pairs(mapping);
    source_keys.len() == destination_keys.len()
        && source_keys
            .iter()
            .zip(destination_keys)
            .all(|(source, destination)| mapping.contains(*source, *destination))
}

pub(crate) fn distribution_values(distribution: &Distribution) -> &[ValueId] {
    match distribution {
        Distribution::Hash { keys, .. } | Distribution::BucketShuffle { keys, .. } => keys,
        Distribution::Unconstrained
        | Distribution::Singleton
        | Distribution::RoundRobin
        | Distribution::Broadcast => &[],
    }
}

pub(crate) fn validate_sinks(plan: &PhysicalPlan, errors: &mut ValidationContext) {
    let mut referenced = BTreeSet::new();
    for fragment in plan.fragments().values() {
        let path = format!("fragments[{}].sink", fragment.id().get());
        let edges: &[EdgeId] = match fragment.sink() {
            FragmentSink::Stream { edge } => std::slice::from_ref(edge),
            FragmentSink::Multicast { edges } => edges,
            FragmentSink::Router { routes, .. } => {
                for route in routes {
                    if !referenced.insert(route.edge) {
                        errors.push(ValidationError::new(
                            &path,
                            "edge is referenced by more than one sink",
                        ));
                    }
                    match plan.edges().get(&route.edge) {
                        Some(edge)
                            if edge.source.fragment == fragment.id()
                                && edge.kind == crate::EdgeKind::ChangeStreamRouter =>
                        {
                            if !edge
                                .source
                                .projection
                                .iter()
                                .eq(route.input_mapping.iter().map(|(_, value)| value))
                            {
                                errors.push(ValidationError::new(
                                    &path,
                                    "router edge projection differs from its exact route input sequence",
                                ));
                            }
                            validate_router_writer_contract(plan, route, edge, &path, errors);
                            validate_router_partitioning(
                                route,
                                &edge.partitioning.source,
                                &path,
                                errors,
                            );
                        }
                        Some(_) => errors.push(ValidationError::new(
                            &path,
                            "sink edge belongs to another source fragment",
                        )),
                        None => {
                            errors.push(ValidationError::new(&path, "sink edge is not defined"))
                        }
                    }
                }
                &[]
            }
            FragmentSink::Result | FragmentSink::SealedArtifact(_) | FragmentSink::Noop => &[],
        };
        for edge_id in edges {
            if !referenced.insert(*edge_id) {
                errors.push(ValidationError::new(
                    &path,
                    "edge is referenced by more than one sink",
                ));
            }
            match plan.edges().get(edge_id) {
                Some(edge)
                    if edge.source.fragment == fragment.id()
                        && edge_kind_matches_sink(fragment.sink(), edge.kind) => {}
                Some(_) => errors.push(ValidationError::new(
                    &path,
                    "sink edge belongs to another source fragment",
                )),
                None => errors.push(ValidationError::new(&path, "sink edge is not defined")),
            }
        }
    }
    for edge in plan.edges().values() {
        if !referenced.contains(&edge.id) {
            errors.push(ValidationError::new(
                format!("edges[{}]", edge.id.get()),
                "edge is not owned by its source fragment sink",
            ));
        }
    }
}

pub(crate) fn validate_router_writer_contract(
    plan: &PhysicalPlan,
    route: &crate::ChangeStreamRoute,
    edge: &Edge,
    path: &str,
    errors: &mut ValidationContext,
) {
    let Some(destination) = plan.fragments().get(&edge.destination.fragment) else {
        return;
    };
    let Some(writer) = destination.nodes().get(&destination.root()) else {
        return;
    };
    let NodeKind::TableWriter { target } = &writer.kind else {
        errors.push(ValidationError::new(
            path,
            "router edge destination fragment root is not its exact table writer",
        ));
        return;
    };
    if writer.inputs.as_ref() != [edge.destination.node] {
        errors.push(ValidationError::new(
            path,
            "router edge receiver is not the direct table writer input",
        ));
    }
    if route.write_target_ordinal != target.write_target_ordinal {
        errors.push(ValidationError::new(
            path,
            "router write target ordinal differs from its destination table writer",
        ));
    }
    if target.required_distribution != edge.partitioning.destination {
        errors.push(ValidationError::new(
            path,
            "router edge destination distribution differs from its table writer requirement",
        ));
    }
    let fields_match = route.input_mapping.len() == edge.destination.receive_mapping.len()
        && route.input_mapping.len() == target.target_fields.len()
        && route
            .input_mapping
            .iter()
            .zip(edge.destination.receive_mapping.iter())
            .zip(target.target_fields.iter())
            .all(
                |(((route_token, route_value), (mapped_source, imported)), target_field)| {
                    route_value == mapped_source
                        && route_token == &target_field.token
                        && imported == &target_field.input
                },
            );
    if !fields_match {
        errors.push(ValidationError::new(
            path,
            "router field mapping differs from its destination table writer contract",
        ));
    }
}

pub(crate) fn validate_router_partitioning(
    route: &crate::ChangeStreamRoute,
    source_distribution: &Distribution,
    path: &str,
    errors: &mut ValidationContext,
) {
    let matches = match source_distribution {
        Distribution::Singleton => route.partition_by.is_empty(),
        Distribution::Hash { keys, .. } | Distribution::BucketShuffle { keys, .. } => {
            !route.partition_by.is_empty() && keys.as_ref() == route.partition_by.as_ref()
        }
        Distribution::Unconstrained | Distribution::RoundRobin | Distribution::Broadcast => false,
    };
    if !matches {
        errors.push(ValidationError::new(
            path,
            "router partition values differ from its exact edge distribution",
        ));
    }
}

pub(crate) fn edge_kind_matches_sink(sink: &FragmentSink, kind: crate::EdgeKind) -> bool {
    match sink {
        FragmentSink::Stream { .. } => kind == crate::EdgeKind::Stream,
        FragmentSink::Multicast { .. } => kind == crate::EdgeKind::CteMulticast,
        FragmentSink::Router { .. } => kind == crate::EdgeKind::ChangeStreamRouter,
        FragmentSink::Result | FragmentSink::SealedArtifact(_) | FragmentSink::Noop => false,
    }
}

pub(crate) fn validate_writer_flows(plan: &PhysicalPlan, errors: &mut ValidationContext) {
    let writers = plan
        .fragments()
        .values()
        .flat_map(|fragment| {
            fragment.nodes().values().filter_map(|node| {
                matches!(node.kind, NodeKind::TableWriter { .. })
                    .then_some((fragment.id(), node.id))
            })
        })
        .collect::<BTreeSet<_>>();
    let mut writer_finish_counts = BTreeMap::<(FragmentId, NodeId), usize>::new();

    for fragment in plan.fragments().values() {
        for finish_node in fragment
            .nodes()
            .values()
            .filter(|node| matches!(node.kind, NodeKind::TableFinish(_)))
        {
            let path = format!(
                "fragments[{}].nodes[{}].writer_flow",
                fragment.id().get(),
                finish_node.id.get()
            );
            if finish_node.id != fragment.root() {
                errors.push(ValidationError::new(
                    &path,
                    "table finish must be the root of its fragment",
                ));
            }
            let NodeKind::TableFinish(finish) = &finish_node.kind else {
                unreachable!();
            };
            let finish_values = finish
                .input_schema
                .fields
                .iter()
                .map(|field| field.value)
                .collect::<Box<[_]>>();
            let mut pending = finish_node
                .inputs
                .iter()
                .copied()
                .map(|node| (fragment.id(), node, finish_values.clone()))
                .collect::<Vec<_>>();
            let mut visited = BTreeSet::new();
            let mut finish_writers = Vec::new();
            while let Some((fragment_id, node_id, expected_values)) = pending.pop() {
                if !visited.insert((fragment_id, node_id)) {
                    errors.push(ValidationError::new(
                        &path,
                        "writer relation reaches table finish through more than one path",
                    ));
                    continue;
                }
                let Some(flow_fragment) = plan.fragments().get(&fragment_id) else {
                    continue;
                };
                let Some(node) = flow_fragment.nodes().get(&node_id) else {
                    continue;
                };
                match &node.kind {
                    NodeKind::TableWriter { target } => {
                        finish_writers.push((fragment_id, node_id, target));
                        *writer_finish_counts
                            .entry((fragment_id, node_id))
                            .or_default() += 1;
                        if !writer_schema_matches_finish_values(
                            &target.output_schema,
                            &finish.input_schema,
                            &expected_values,
                        ) {
                            errors.push(ValidationError::new(
                                &path,
                                "table writer fields do not map exactly to its table finish input roles",
                            ));
                        }
                    }
                    NodeKind::ExchangeSource { edge, .. } => match plan.edges().get(edge) {
                        Some(edge_contract) if edge_contract.kind == crate::EdgeKind::Stream => {
                            let source_values = edge_contract
                                .destination
                                .receive_mapping
                                .iter()
                                .zip(&expected_values)
                                .map(|((source, destination), expected)| {
                                    (*destination == *expected).then_some(*source)
                                })
                                .collect::<Option<Box<[_]>>>();
                            if let Some(source) =
                                plan.fragments().get(&edge_contract.source.fragment)
                            {
                                let Some(source_root) = source.nodes().get(&source.root()) else {
                                    continue;
                                };
                                match (&source_root.kind, source_values) {
                                    (NodeKind::TableWriter { target }, Some(source_values)) => {
                                        finish_writers.push((source.id(), source_root.id, target));
                                        *writer_finish_counts
                                            .entry((source.id(), source_root.id))
                                            .or_default() += 1;
                                        if !writer_schema_matches_finish_values(
                                            &target.output_schema,
                                            &finish.input_schema,
                                            &source_values,
                                        ) {
                                            errors.push(ValidationError::new(
                                                &path,
                                                "streamed table writer fields do not map exactly to its table finish input roles",
                                            ));
                                        }
                                    }
                                    _ => errors.push(ValidationError::new(
                                        &path,
                                        "table finish stream source fields do not map exactly to its table finish input roles",
                                    )),
                                }
                            }
                        }
                        _ => errors.push(ValidationError::new(
                            &path,
                            "table finish writer relation uses a non-stream exchange",
                        )),
                    },
                    NodeKind::SetOp {
                        kind: crate::SetOperationKind::UnionAll,
                        input_mappings,
                    } => {
                        if node.output.columns.as_ref() != expected_values.as_ref()
                            || input_mappings.len() != node.inputs.len()
                            || input_mappings
                                .iter()
                                .any(|mapping| mapping.len() != expected_values.len())
                        {
                            errors.push(ValidationError::new(
                                &path,
                                "writer UnionAll does not preserve the exact finish field occurrences",
                            ));
                            continue;
                        }
                        pending.extend(
                            node.inputs
                                .iter()
                                .copied()
                                .zip(input_mappings.iter().cloned())
                                .map(|(input, mapping)| (fragment_id, input, mapping)),
                        );
                    }
                    _ => errors.push(ValidationError::new(
                        &path,
                        "table finish input contains a non-preserving writer relation node",
                    )),
                }
            }
            let actual_ordinals = finish_writers
                .iter()
                .map(|(_, _, target)| target.write_target_ordinal)
                .collect::<BTreeSet<_>>();
            if actual_ordinals.len() != finish_writers.len()
                || !actual_ordinals
                    .iter()
                    .copied()
                    .eq(finish.expected_target_ordinals.iter().copied())
            {
                errors.push(ValidationError::new(
                    &path,
                    "table finish expected targets differ from its exact upstream writers",
                ));
            }
            for (_, _, target) in finish_writers {
                if !writer_schema_shapes_match(&target.output_schema, &finish.input_schema) {
                    errors.push(ValidationError::new(
                        &path,
                        "table writer output schema differs from its table finish input schema",
                    ));
                }
            }
        }
    }

    for writer in writers {
        if writer_finish_counts.get(&writer).copied() != Some(1) {
            errors.push(ValidationError::new(
                format!("fragments[{}].nodes[{}]", writer.0.get(), writer.1.get()),
                "table writer must feed exactly one table finish",
            ));
        }
    }
}

pub(crate) fn validate_result(plan: &PhysicalPlan, errors: &mut ValidationContext) {
    let result_sinks = plan
        .fragments()
        .values()
        .filter(|fragment| matches!(fragment.sink(), FragmentSink::Result))
        .collect::<Vec<_>>();
    match (plan.result_port(), result_sinks.as_slice()) {
        (None, []) => {}
        (None, _) => errors.push(ValidationError::new(
            "result_port",
            "result sink has no result port",
        )),
        (Some(_), []) => errors.push(ValidationError::new(
            "result_port",
            "result port has no result sink",
        )),
        (Some(_), [_, _, ..]) => errors.push(ValidationError::new(
            "result_port",
            "plan has more than one result sink",
        )),
        (Some(result), [fragment]) => {
            if result.fragment != fragment.id() {
                errors.push(ValidationError::new(
                    "result_port.fragment",
                    "result port belongs to another fragment",
                ));
            }
            if result.output.node != fragment.root() {
                errors.push(ValidationError::new(
                    "result_port.output",
                    "result output is not the result fragment root",
                ));
            }
            match fragment.nodes().get(&result.output.node) {
                Some(node) if node.output == result.output => {}
                Some(_) => errors.push(ValidationError::new(
                    "result_port.output",
                    "result output differs from the node output port",
                )),
                None => errors.push(ValidationError::new(
                    "result_port.output",
                    "result node is not defined",
                )),
            }
            if result.fields.len() != result.output.columns.len() {
                errors.push(ValidationError::new(
                    "result_port.fields",
                    "result schema width differs from output width",
                ));
            }
            for (ordinal, (field, value)) in
                result.fields.iter().zip(&result.output.columns).enumerate()
            {
                if field.value != *value {
                    errors.push(ValidationError::new(
                        "result_port.fields",
                        format!("result value differs at ordinal {ordinal}"),
                    ));
                }
                if field.name.is_empty() {
                    errors.push(ValidationError::new(
                        "result_port.fields",
                        format!("result name is empty at ordinal {ordinal}"),
                    ));
                }
                if let Some(definition) = fragment.values().get(value)
                    && definition.ty != field.ty
                {
                    errors.push(ValidationError::new(
                        "result_port.fields",
                        format!("result type differs at ordinal {ordinal}"),
                    ));
                }
            }
        }
    }
}

pub(crate) fn validate_artifact_sink(
    fragment: &Fragment,
    spec: &SealedArtifactSinkSpec,
    errors: &mut ValidationContext,
) {
    let path = format!("fragments[{}].sink.sealed_artifact", fragment.id().get());
    if spec.format.revision == 0
        || spec.input.is_empty()
        || spec.max_reference_bytes == 0
        || spec.max_reference_bytes > MAX_ARTIFACT_REFERENCE_BYTES
        || spec.source.selection_digest == [0; 32]
    {
        errors.push(ValidationError::new(
            &path,
            "sealed artifact sink requires a format revision, input and reference budget",
        ));
    }
    validate_read_reference(&spec.source.source, &path, errors);
    validate_coverage(&spec.required_coverage, &path, errors);
    if spec.required_coverage.selection_digest != spec.source.selection_digest {
        errors.push(ValidationError::new(
            &path,
            "artifact coverage is not bound to the exact source selection",
        ));
    }
    for ArtifactInputField { value, ty } in &spec.input {
        match fragment.values().get(value) {
            Some(definition) if definition.ty != *ty => errors.push(ValidationError::new(
                &path,
                "artifact input type differs from its value definition",
            )),
            Some(_) => {}
            None => require_value(fragment, *value, &path, errors),
        }
    }
    for value in &spec.partition_by {
        require_value(fragment, *value, &path, errors);
    }
    for key in &spec.order_by {
        require_value(fragment, key.value, &path, errors);
    }
    for value in &spec.group_boundaries {
        require_value(fragment, *value, &path, errors);
    }
    let Some(root) = fragment.nodes().get(&fragment.root()) else {
        return;
    };
    let input_values = spec
        .input
        .iter()
        .map(|field| field.value)
        .collect::<Vec<_>>();
    let input_value_index = ValuePortIndex::new(&input_values);
    if input_values.as_slice() != root.output.columns.as_ref() {
        errors.push(ValidationError::new(
            &path,
            "artifact input schema differs from the fragment root output",
        ));
    }
    if !spec.partition_by.is_empty() {
        match &root.output_properties.distribution {
            Distribution::Hash { keys, .. } | Distribution::BucketShuffle { keys, .. }
                if keys.as_ref() == spec.partition_by.as_ref() => {}
            _ => errors.push(ValidationError::new(
                &path,
                "artifact partition requirement is not guaranteed by the fragment output",
            )),
        }
    } else if root.output_properties.distribution != Distribution::Singleton {
        errors.push(ValidationError::new(
            &path,
            "unpartitioned sealed artifact requires singleton placement",
        ));
    }
    if root.output_properties.row_multiplicity != RowMultiplicity::SingleCopy {
        errors.push(ValidationError::new(
            &path,
            "sealed artifact requires single-copy row ownership",
        ));
    }
    let required_order = spec
        .order_by
        .iter()
        .map(|key| crate::OrderingKey {
            value: key.value,
            direction: key.direction,
            null_ordering: key.null_ordering,
        })
        .collect::<Vec<_>>();
    if root.output_properties.ordering.len() < required_order.len()
        || root.output_properties.ordering[..required_order.len()] != required_order
    {
        errors.push(ValidationError::new(
            &path,
            "artifact ordering requirement is not guaranteed by the fragment root",
        ));
    }
    if spec
        .group_boundaries
        .iter()
        .any(|value| !input_value_index.contains(value))
    {
        errors.push(ValidationError::new(
            &path,
            "artifact group boundary is absent from the exact input schema",
        ));
    }
}

pub(crate) fn validate_artifact_refs(plan: &PhysicalPlan, errors: &mut ValidationContext) {
    for artifact in plan.artifact_refs().values() {
        validate_artifact_ref(artifact, errors);
    }
}

pub(crate) fn validate_artifact_ref(artifact: &SealedArtifactRef, errors: &mut ValidationContext) {
    let path = format!("artifact_refs[{}]", artifact.id.get());
    if artifact.format.revision == 0 || artifact.schema.is_empty() {
        errors.push(ValidationError::new(
            &path,
            "artifact reference requires a format revision and schema",
        ));
    }
    if artifact.location.is_empty() || artifact.location.len() > 4096 {
        errors.push(ValidationError::new(
            &path,
            "artifact location must be bounded and non-empty",
        ));
    }
    if artifact.content_digest == [0; 32]
        || artifact.schema_digest == [0; 32]
        || artifact.source.selection_digest == [0; 32]
    {
        errors.push(ValidationError::new(
            &path,
            "artifact evidence digests must be non-zero",
        ));
    }
    validate_read_reference(&artifact.source.source, &path, errors);
    validate_coverage(&artifact.coverage, &path, errors);
    if artifact.coverage.selection_digest != artifact.source.selection_digest {
        errors.push(ValidationError::new(
            &path,
            "artifact coverage is not bound to the exact source selection",
        ));
    }
}

pub(crate) fn validate_coverage(
    coverage: &CoverageSet,
    path: &str,
    errors: &mut ValidationContext,
) {
    if coverage.domain.is_empty()
        || coverage.domain.len() > 1024
        || coverage.selection_digest == [0; 32]
        || coverage.ranges.is_empty()
    {
        errors.push(ValidationError::new(
            path,
            "coverage requires a bounded domain and at least one range",
        ));
        return;
    }
    for (index, range) in coverage.ranges.iter().enumerate() {
        if let (Some(start), Some(end)) = (&range.start, &range.end)
            && start.as_ref() >= end.as_ref()
        {
            errors.push(ValidationError::new(
                path,
                format!("coverage range {index} is empty or reversed"),
            ));
        }
        if index > 0 {
            let previous = &coverage.ranges[index - 1];
            let ordered = match (&previous.end, &range.start) {
                (Some(previous_end), Some(current_start)) => {
                    previous_end.as_ref() <= current_start.as_ref()
                }
                (Some(_), None) | (None, _) => false,
            };
            if !ordered {
                errors.push(ValidationError::new(
                    path,
                    format!("coverage ranges overlap or are out of order at {index}"),
                ));
            }
        }
    }
    if coverage.complete_input {
        let spans_complete_domain = coverage
            .ranges
            .first()
            .is_some_and(|range| range.start.is_none())
            && coverage
                .ranges
                .last()
                .is_some_and(|range| range.end.is_none())
            && coverage.ranges.windows(2).all(|pair| {
                matches!(
                    (&pair[0].end, &pair[1].start),
                    (Some(previous_end), Some(next_start))
                        if previous_end.as_ref() == next_start.as_ref()
                )
            });
        if !spans_complete_domain {
            errors.push(ValidationError::new(
                path,
                "complete coverage must form one gap-free unbounded domain",
            ));
        }
    }
}

pub(crate) fn validate_artifact_inputs(plan: &PhysicalPlan, errors: &mut ValidationContext) {
    for fragment in plan.fragments().values() {
        for node in fragment.nodes().values() {
            if let NodeKind::Scan { relation, .. } = &node.kind {
                let path = format!(
                    "fragments[{}].nodes[{}].relation.artifact_inputs",
                    fragment.id().get(),
                    node.id.get()
                );
                for requirement in relation.artifact_inputs() {
                    match plan.artifact_refs().get(&requirement.artifact) {
                        Some(artifact)
                            if artifact.kind == requirement.kind
                                && artifact.format == requirement.format
                                && artifact.schema == requirement.schema
                                && artifact.source == requirement.source
                                && artifact.coverage == requirement.required_coverage => {}
                        Some(_) => errors.push(ValidationError::new(
                            &path,
                            format!(
                                "artifact {} differs from the relation's exact input requirement",
                                requirement.artifact.get()
                            ),
                        )),
                        None => errors.push(ValidationError::new(
                            &path,
                            format!(
                                "artifact reference {} is not defined",
                                requirement.artifact.get()
                            ),
                        )),
                    }
                }
            }
        }
    }
}

pub(crate) fn validate_cross_fragment_value_origins(
    plan: &PhysicalPlan,
    errors: &mut ValidationContext,
) {
    let receive_mappings = plan
        .edges()
        .iter()
        .map(|(id, edge)| {
            (
                *id,
                edge.destination
                    .receive_mapping
                    .iter()
                    .copied()
                    .collect::<BTreeSet<_>>(),
            )
        })
        .collect::<BTreeMap<_, _>>();
    for fragment in plan.fragments().values() {
        for value in fragment.values().values() {
            let path = format!(
                "fragments[{}].values[{}]",
                fragment.id().get(),
                value.id.get()
            );
            match value.origin {
                ValueOrigin::ExchangeImport { edge, source_value } => match plan.edges().get(&edge)
                {
                    Some(edge_contract)
                        if edge_contract.destination.fragment == fragment.id()
                            && receive_mappings.get(&edge).is_some_and(|mapping| {
                                mapping.contains(&(source_value, value.id))
                            }) => {}
                    Some(_) => errors.push(ValidationError::new(
                        &path,
                        "exchange import is not present in the edge receive mapping",
                    )),
                    None => {
                        errors.push(ValidationError::new(&path, "exchange edge is not defined"))
                    }
                },
                ValueOrigin::CteImport {
                    edge,
                    producer_fragment,
                    producer_value,
                } => match plan.edges().get(&edge) {
                    Some(edge_contract)
                        if edge_contract.kind == crate::EdgeKind::CteMulticast
                            && edge_contract.source.fragment == producer_fragment
                            && edge_contract.destination.fragment == fragment.id()
                            && receive_mappings.get(&edge).is_some_and(|mapping| {
                                mapping.contains(&(producer_value, value.id))
                            }) => {}
                    Some(_) => errors.push(ValidationError::new(
                        &path,
                        "CTE import is not present in the exact CTE edge mapping",
                    )),
                    None => errors.push(ValidationError::new(&path, "CTE edge is not defined")),
                },
                _ => {}
            }
        }
    }
}
