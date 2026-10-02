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

fn broadcast() -> PhysicalProperties {
    PhysicalProperties {
        distribution: Distribution::Broadcast,
        row_multiplicity: RowMultiplicity::Replicated,
        ordering: Box::default(),
    }
}

fn single_copy() -> PhysicalProperties {
    PhysicalProperties {
        distribution: Distribution::Singleton,
        row_multiplicity: RowMultiplicity::SingleCopy,
        ordering: Box::default(),
    }
}

fn any_of(witnesses: &[RuntimeFilterWitnessId]) -> RuntimeFilterCoverage {
    let mut nodes = witnesses
        .iter()
        .copied()
        .map(RuntimeFilterCoverageNode::Witness)
        .collect::<Vec<_>>();
    nodes.push(RuntimeFilterCoverageNode::AnyOf {
        children: (0..u32::try_from(witnesses.len()).unwrap())
            .collect::<Vec<_>>()
            .into_boxed_slice(),
    });
    RuntimeFilterCoverage {
        root: u32::try_from(nodes.len() - 1).unwrap(),
        nodes: nodes.into_boxed_slice(),
    }
}

fn append_single_copy_literal(builder: &mut FragmentBuilder) -> (NodeId, ValueId) {
    let node = builder.reserve_node_id().unwrap();
    let value_type = ty(DataType::Int64, false);
    let expression = builder
        .add_expression(
            node,
            value_type.clone(),
            ExprKind::Literal(LiteralValue::Int64(11)),
        )
        .unwrap();
    let value = builder
        .add_value(
            value_type,
            ValueOrigin::NodeOutput {
                node,
                output_ordinal: 0,
            },
        )
        .unwrap();
    builder
        .insert_node_unchecked(PhysicalNode {
            id: node,
            inputs: Box::default(),
            required_inputs: Box::default(),
            output_properties: single_copy(),
            output: OutputPort {
                node,
                columns: Box::from([value]),
            },
            kind: NodeKind::Values {
                rows: Box::from([Box::from([expression])]),
            },
        })
        .unwrap();
    (node, value)
}

fn append_exchange(
    builder: &mut FragmentBuilder,
    edge: EdgeId,
    source_value: ValueId,
    properties: PhysicalProperties,
) -> (NodeId, ValueId) {
    let node = builder.reserve_node_id().unwrap();
    let value = builder
        .add_value(
            ty(DataType::Int64, false),
            ValueOrigin::ExchangeImport { edge, source_value },
        )
        .unwrap();
    builder
        .insert_node_unchecked(PhysicalNode {
            id: node,
            inputs: Box::default(),
            required_inputs: Box::default(),
            output_properties: properties,
            output: OutputPort {
                node,
                columns: Box::from([value]),
            },
            kind: NodeKind::ExchangeSource {
                edge,
                imports: Box::from([(source_value, value)]),
            },
        })
        .unwrap();
    (node, value)
}

fn append_cte_exchange(
    builder: &mut FragmentBuilder,
    edge: EdgeId,
    producer_fragment: FragmentId,
    source_value: ValueId,
) -> (NodeId, ValueId) {
    let node = builder.reserve_node_id().unwrap();
    let value = builder
        .add_value(
            ty(DataType::Int64, false),
            ValueOrigin::CteImport {
                edge,
                producer_fragment,
                producer_value: source_value,
            },
        )
        .unwrap();
    builder
        .insert_node_unchecked(PhysicalNode {
            id: node,
            inputs: Box::default(),
            required_inputs: Box::default(),
            output_properties: single_copy(),
            output: OutputPort {
                node,
                columns: Box::from([value]),
            },
            kind: NodeKind::ExchangeSource {
                edge,
                imports: Box::from([(source_value, value)]),
            },
        })
        .unwrap();
    (node, value)
}

fn append_broadcast_join(
    builder: &mut FragmentBuilder,
    left: (NodeId, ValueId),
    right: (NodeId, ValueId),
) -> NodeId {
    let node = builder.reserve_node_id().unwrap();
    let left_key = builder
        .add_expression(node, ty(DataType::Int64, false), ExprKind::Value(left.1))
        .unwrap();
    let right_key = builder
        .add_expression(node, ty(DataType::Int64, false), ExprKind::Value(right.1))
        .unwrap();
    builder
        .insert_node_unchecked(PhysicalNode {
            id: node,
            inputs: Box::from([left.0, right.0]),
            required_inputs: Box::from([single_copy(), broadcast()]),
            output_properties: single_copy(),
            output: OutputPort {
                node,
                columns: Box::from([left.1, right.1]),
            },
            kind: NodeKind::HashJoin {
                kind: JoinKind::Inner,
                build_side: JoinSide::Right,
                keys: Box::from([JoinKey {
                    left: left_key,
                    right: right_key,
                    null_safe: false,
                }]),
                distribution: JoinDistribution::BroadcastBuild,
                residual: None,
                null_extended: Box::default(),
            },
        })
        .unwrap();
    node
}

fn equality(id: u32, fragment: FragmentId, join: NodeId) -> RuntimeFilterEqualityWitness {
    RuntimeFilterEqualityWitness {
        id: RuntimeFilterEqualityWitnessId::new(id),
        fragment,
        join,
        key_ordinal: 0,
        domain_side: JoinSide::Right,
    }
}

fn producer(
    witness: u32,
    equality: u32,
    fragment: FragmentId,
    join: NodeId,
    build_value: ValueId,
    build_edges: Box<[EdgeId]>,
) -> RuntimeFilterProducer {
    RuntimeFilterProducer {
        witness: RuntimeFilterWitnessId::new(witness),
        endpoint: RuntimeFilterEndpoint {
            fragment,
            node: join,
            values: Box::from([build_value]),
        },
        apply_point: RuntimeFilterApplyPoint::NodeInput { input_ordinal: 1 },
        contribution_kinds: Box::from([
            RuntimeFilterContributionKind::ValueDomainDelta,
            RuntimeFilterContributionKind::ProducerClosed,
        ]),
        completion: RuntimeFilterCompletion::ProducerClosed,
        progress: RuntimeFilterProducerProgress {
            build_edges,
            non_build_edges: Box::default(),
        },
        target: RuntimeFilterProducerTarget::JoinBuildKey {
            equality: RuntimeFilterEqualityWitnessId::new(equality),
        },
    }
}

fn blocking_consumer(
    equality: u32,
    fragment: FragmentId,
    join: NodeId,
    probe_value: ValueId,
) -> RuntimeFilterConsumer {
    RuntimeFilterConsumer {
        endpoint: RuntimeFilterEndpoint {
            fragment,
            node: join,
            values: Box::from([probe_value]),
        },
        apply_point: RuntimeFilterApplyPoint::NodeInput { input_ordinal: 0 },
        capabilities: Box::from([
            RuntimeFilterArtifactCapability::Membership,
            RuntimeFilterArtifactCapability::EmptyDomain,
        ]),
        activation: RuntimeFilterConsumerActivation::BlockingSnapshot,
        target: RuntimeFilterConsumerTarget::JoinProbeKey {
            equality: RuntimeFilterEqualityWitnessId::new(equality),
        },
    }
}

fn filter(
    id: RuntimeFilterId,
    equalities: Box<[RuntimeFilterEqualityWitness]>,
    producers: Box<[RuntimeFilterProducer]>,
    consumers: Box<[RuntimeFilterConsumer]>,
) -> RuntimeFilter {
    let witnesses = producers
        .iter()
        .map(|producer| producer.witness)
        .collect::<Vec<_>>();
    let coverage = any_of(&witnesses);
    RuntimeFilter {
        id,
        kind: RuntimeFilterKind::InList,
        domain: RuntimeFilterDomain::Membership {
            ty: ty(DataType::Int64, false),
            null_semantics: RuntimeFilterNullSemantics::NeverMatches,
        },
        lifecycle: RuntimeFilterLifecycle::CompleteOnce,
        reduction: RuntimeFilterReduction::SetUnion,
        availability_coverage: coverage.clone(),
        terminal_coverage: coverage,
        equality_witnesses: equalities,
        producers,
        consumers,
        policy: RuntimeFilterPolicy {
            max_contribution_bytes: 1024,
            max_artifact_bytes: 1024,
            deadline_ms: 100,
            max_retries: 1,
        },
    }
}

struct EdgeEndpoints {
    source_fragment: FragmentId,
    source_value: ValueId,
    destination_fragment: FragmentId,
    destination_node: NodeId,
    destination_value: ValueId,
}

fn add_edge(plan: &mut PlanBuilder, id: EdgeId, endpoints: EdgeEndpoints, broadcast_edge: bool) {
    add_edge_with_kind(plan, id, endpoints, broadcast_edge, EdgeKind::Stream);
}

fn add_edge_with_kind(
    plan: &mut PlanBuilder,
    id: EdgeId,
    endpoints: EdgeEndpoints,
    broadcast_edge: bool,
    kind: EdgeKind,
) {
    plan.add_edge(Edge {
        id,
        kind,
        source: EdgeSource {
            fragment: endpoints.source_fragment,
            projection: Box::from([endpoints.source_value]),
        },
        destination: EdgeDestination {
            fragment: endpoints.destination_fragment,
            node: endpoints.destination_node,
            receive_mapping: Box::from([(endpoints.source_value, endpoints.destination_value)]),
        },
        partitioning: EdgePartitioning {
            source: if broadcast_edge {
                Distribution::Broadcast
            } else {
                Distribution::Singleton
            },
            source_multiplicity: RowMultiplicity::SingleCopy,
            destination: if broadcast_edge {
                Distribution::Broadcast
            } else {
                Distribution::Singleton
            },
            destination_multiplicity: if broadcast_edge {
                RowMultiplicity::Replicated
            } else {
                RowMultiplicity::SingleCopy
            },
        },
    })
    .unwrap();
}

#[test]
fn blocking_runtime_filter_rejects_multicast_backpressure_cycle() {
    let source_fragment_id = FragmentId::new(420);
    let build_fragment_id = FragmentId::new(421);
    let probe_fragment_id = FragmentId::new(422);
    let build_branch = EdgeId::new(420);
    let probe_branch = EdgeId::new(421);
    let join_build_edge = EdgeId::new(422);
    let filter_id = RuntimeFilterId::new(420);

    let mut source_builder = FragmentBuilder::new(source_fragment_id);
    let (source_root, source_value) = append_single_copy_literal(&mut source_builder);
    let source_fragment = source_builder
        .finish_definition(
            source_root,
            FragmentSink::Multicast {
                edges: Box::from([build_branch, probe_branch]),
            },
            dop(),
        )
        .unwrap();

    let mut build_builder = FragmentBuilder::new(build_fragment_id);
    let (build_root, build_value) = append_cte_exchange(
        &mut build_builder,
        build_branch,
        source_fragment_id,
        source_value,
    );
    let build_fragment = build_builder
        .finish_definition(
            build_root,
            FragmentSink::Stream {
                edge: join_build_edge,
            },
            dop(),
        )
        .unwrap();

    let mut probe_builder = FragmentBuilder::new(probe_fragment_id);
    let probe = append_cte_exchange(
        &mut probe_builder,
        probe_branch,
        source_fragment_id,
        source_value,
    );
    let build = append_exchange(
        &mut probe_builder,
        join_build_edge,
        build_value,
        broadcast(),
    );
    let join = append_broadcast_join(&mut probe_builder, probe, build);
    probe_builder.attach_runtime_filter(filter_id).unwrap();
    let probe_fragment = probe_builder
        .finish_definition(join, FragmentSink::Noop, dop())
        .unwrap();

    let mut filter = filter(
        filter_id,
        Box::from([equality(420, probe_fragment_id, join)]),
        Box::from([producer(
            420,
            420,
            probe_fragment_id,
            join,
            build.1,
            Box::from([join_build_edge]),
        )]),
        Box::from([blocking_consumer(420, probe_fragment_id, join, probe.1)]),
    );
    filter.producers[0].progress.non_build_edges = Box::from([probe_branch]);

    let finish = |filter: RuntimeFilter| {
        let mut plan = PlanBuilder::new(version());
        for fragment in [
            source_fragment.clone(),
            build_fragment.clone(),
            probe_fragment.clone(),
        ] {
            plan.add_fragment(fragment).unwrap();
        }
        for (id, destination_fragment, destination_node, destination_value) in [
            (build_branch, build_fragment_id, build_root, build_value),
            (probe_branch, probe_fragment_id, probe.0, probe.1),
        ] {
            add_edge_with_kind(
                &mut plan,
                id,
                EdgeEndpoints {
                    source_fragment: source_fragment_id,
                    source_value,
                    destination_fragment,
                    destination_node,
                    destination_value,
                },
                false,
                EdgeKind::CteMulticast,
            );
        }
        add_edge(
            &mut plan,
            join_build_edge,
            EdgeEndpoints {
                source_fragment: build_fragment_id,
                source_value: build_value,
                destination_fragment: probe_fragment_id,
                destination_node: build.0,
                destination_value: build.1,
            },
            true,
        );
        plan.add_runtime_filter(filter).unwrap();
        plan.finish()
    };

    let mut non_blocking = filter.clone();
    non_blocking.consumers[0].activation =
        RuntimeFilterConsumerActivation::StartUnfilteredThenApplyComplete {
            late_apply: LateApplyGranularity::Batch,
        };
    let valid = finish(non_blocking).expect("late apply breaks the multicast wait cycle");
    let cuts = fragment_cuts(&valid, probe_fragment_id).unwrap();
    validate_fragment(&valid.fragments()[&probe_fragment_id], &cuts).unwrap();

    let error = finish(filter).unwrap_err().to_string();
    assert!(error.contains(
        "blocking runtime-filter waits form a cycle with physical execution dependencies"
    ));
}

#[test]
fn blocking_runtime_filter_rejects_a_cycle_through_a_nested_producer_build() {
    let build_fragment_id = FragmentId::new(400);
    let inner_fragment_id = FragmentId::new(401);
    let outer_fragment_id = FragmentId::new(402);
    let build_edge = EdgeId::new(400);
    let nested_build_edge = EdgeId::new(401);
    let filter_id = RuntimeFilterId::new(400);

    let mut build_builder = FragmentBuilder::new(build_fragment_id);
    let (build_root, build_value) = append_single_copy_literal(&mut build_builder);
    let build_fragment = build_builder
        .finish_definition(build_root, FragmentSink::Stream { edge: build_edge }, dop())
        .unwrap();

    let mut inner_builder = FragmentBuilder::new(inner_fragment_id);
    let inner_probe = append_single_copy_literal(&mut inner_builder);
    let inner_build = append_exchange(&mut inner_builder, build_edge, build_value, broadcast());
    let inner_join = append_broadcast_join(&mut inner_builder, inner_probe, inner_build);
    inner_builder.attach_runtime_filter(filter_id).unwrap();
    let inner_fragment = inner_builder
        .finish_definition(
            inner_join,
            FragmentSink::Stream {
                edge: nested_build_edge,
            },
            dop(),
        )
        .unwrap();

    let mut outer_builder = FragmentBuilder::new(outer_fragment_id);
    let outer_probe = append_single_copy_literal(&mut outer_builder);
    let outer_build = append_exchange(
        &mut outer_builder,
        nested_build_edge,
        inner_build.1,
        broadcast(),
    );
    let outer_join = append_broadcast_join(&mut outer_builder, outer_probe, outer_build);
    outer_builder.attach_runtime_filter(filter_id).unwrap();
    let outer_fragment = outer_builder
        .finish_definition(outer_join, FragmentSink::Noop, dop())
        .unwrap();

    let filter = filter(
        filter_id,
        Box::from([
            equality(400, inner_fragment_id, inner_join),
            equality(401, outer_fragment_id, outer_join),
        ]),
        Box::from([
            producer(
                400,
                400,
                inner_fragment_id,
                inner_join,
                inner_build.1,
                Box::from([build_edge]),
            ),
            producer(
                401,
                401,
                outer_fragment_id,
                outer_join,
                outer_build.1,
                Box::from([nested_build_edge]),
            ),
        ]),
        Box::from([blocking_consumer(
            400,
            inner_fragment_id,
            inner_join,
            inner_probe.1,
        )]),
    );

    let mut plan = PlanBuilder::new(version());
    plan.add_fragment(build_fragment).unwrap();
    plan.add_fragment(inner_fragment).unwrap();
    plan.add_fragment(outer_fragment).unwrap();
    add_edge(
        &mut plan,
        build_edge,
        EdgeEndpoints {
            source_fragment: build_fragment_id,
            source_value: build_value,
            destination_fragment: inner_fragment_id,
            destination_node: inner_build.0,
            destination_value: inner_build.1,
        },
        true,
    );
    add_edge(
        &mut plan,
        nested_build_edge,
        EdgeEndpoints {
            source_fragment: inner_fragment_id,
            source_value: inner_build.1,
            destination_fragment: outer_fragment_id,
            destination_node: outer_build.0,
            destination_value: outer_build.1,
        },
        true,
    );
    plan.add_runtime_filter(filter).unwrap();
    let error = plan.finish().unwrap_err().to_string();

    assert!(error.contains(
        "blocking runtime-filter waits form a cycle with physical execution dependencies"
    ));
}

struct AcyclicFixture {
    plan: PhysicalPlan,
    target_fragment: FragmentId,
    recursive_build_edge: EdgeId,
    recursive_source_fragment: FragmentId,
}

fn acyclic_cross_fragment_fixture() -> AcyclicFixture {
    let source_fragment = FragmentId::new(410);
    let relay_fragment = FragmentId::new(411);
    let target_fragment = FragmentId::new(412);
    let recursive_build_edge = EdgeId::new(410);
    let direct_build_edge = EdgeId::new(411);
    let filter_id = RuntimeFilterId::new(410);

    let mut source_builder = FragmentBuilder::new(source_fragment);
    let (source_root, source_value) = append_single_copy_literal(&mut source_builder);
    let source = source_builder
        .finish_definition(
            source_root,
            FragmentSink::Stream {
                edge: recursive_build_edge,
            },
            dop(),
        )
        .unwrap();

    let mut relay_builder = FragmentBuilder::new(relay_fragment);
    let (relay_root, relay_value) = append_exchange(
        &mut relay_builder,
        recursive_build_edge,
        source_value,
        single_copy(),
    );
    let relay = relay_builder
        .finish_definition(
            relay_root,
            FragmentSink::Stream {
                edge: direct_build_edge,
            },
            dop(),
        )
        .unwrap();

    let mut target_builder = FragmentBuilder::new(target_fragment);
    let probe = append_single_copy_literal(&mut target_builder);
    let build = append_exchange(
        &mut target_builder,
        direct_build_edge,
        relay_value,
        broadcast(),
    );
    let join = append_broadcast_join(&mut target_builder, probe, build);
    target_builder.attach_runtime_filter(filter_id).unwrap();
    let target = target_builder
        .finish_definition(join, FragmentSink::Noop, dop())
        .unwrap();

    let filter = filter(
        filter_id,
        Box::from([equality(410, target_fragment, join)]),
        Box::from([producer(
            410,
            410,
            target_fragment,
            join,
            build.1,
            Box::from([direct_build_edge]),
        )]),
        Box::from([blocking_consumer(410, target_fragment, join, probe.1)]),
    );

    let mut plan = PlanBuilder::new(version());
    for fragment in [source, relay, target] {
        plan.add_fragment(fragment).unwrap();
    }
    add_edge(
        &mut plan,
        recursive_build_edge,
        EdgeEndpoints {
            source_fragment,
            source_value,
            destination_fragment: relay_fragment,
            destination_node: relay_root,
            destination_value: relay_value,
        },
        false,
    );
    add_edge(
        &mut plan,
        direct_build_edge,
        EdgeEndpoints {
            source_fragment: relay_fragment,
            source_value: relay_value,
            destination_fragment: target_fragment,
            destination_node: build.0,
            destination_value: build.1,
        },
        true,
    );
    plan.add_runtime_filter(filter).unwrap();

    AcyclicFixture {
        plan: plan.finish().unwrap(),
        target_fragment,
        recursive_build_edge,
        recursive_source_fragment: source_fragment,
    }
}

fn replace_source_fragment(
    plan: &PhysicalPlan,
    source_id: FragmentId,
    change: impl FnOnce(&mut crate::plan::FragmentParts),
) -> PhysicalPlan {
    let source = &plan.fragments()[&source_id];
    let mut parts = crate::plan::FragmentParts {
        id: source.id(),
        root: source.root(),
        values: source.values().clone(),
        expressions: source.expressions().clone(),
        nodes: source.nodes().clone(),
        sink: source.sink().clone(),
        dop_domain: source.dop_domain(),
        runtime_filters: source.runtime_filters().to_vec().into_boxed_slice(),
    };
    change(&mut parts);
    let mut fragments = plan.fragments().clone();
    fragments.insert(source_id, Fragment::from(parts));
    PhysicalPlan::from(crate::PhysicalPlanParts {
        parameters: novarocks_type_contract::SemanticParameters::default(),
        version: plan.version(),
        fragments,
        edges: plan.edges().clone(),
        runtime_filters: plan.runtime_filters().clone(),
        result_port: plan.result_port().cloned(),
        required: plan.required(),
        annotations: plan.annotations().to_vec().into_boxed_slice(),
    })
}

#[test]
fn blocking_runtime_filter_accepts_an_acyclic_recursive_build_dependency() {
    let fixture = acyclic_cross_fragment_fixture();
    let target = &fixture.plan.fragments()[&fixture.target_fragment];
    let cuts = fragment_cuts(&fixture.plan, fixture.target_fragment).unwrap();
    validate_fragment(target, &cuts).unwrap();
}

#[test]
fn remote_execution_growth_does_not_expand_local_boundary_facts() {
    let fixture = acyclic_cross_fragment_fixture();
    let cuts = fragment_cuts(&fixture.plan, fixture.target_fragment).unwrap();
    for count in [2, 64, 1024] {
        let enlarged =
            replace_source_fragment(&fixture.plan, fixture.recursive_source_fragment, |parts| {
                let node = parts.nodes.get_mut(&parts.root).unwrap();
                let NodeKind::Values { rows } = &mut node.kind else {
                    panic!("fixture source must contain literal rows");
                };
                *rows = vec![rows[0].clone(); count].into_boxed_slice();
            });
        validate_plan(&enlarged).unwrap();
        let local = fragment_cuts(&enlarged, fixture.target_fragment).unwrap();
        assert_eq!(cuts, local);
        validate_fragment(&enlarged.fragments()[&fixture.target_fragment], &local).unwrap();
    }
}

#[test]
fn global_validation_rejects_remote_sink_ownership_that_local_facts_cannot_prove() {
    let fixture = acyclic_cross_fragment_fixture();
    let cuts = fragment_cuts(&fixture.plan, fixture.target_fragment).unwrap();
    let direct_edge = cuts.inbound[0].edge;
    for sink in [
        FragmentSink::Noop,
        FragmentSink::Stream { edge: direct_edge },
        FragmentSink::Multicast {
            edges: Box::from([fixture.recursive_build_edge]),
        },
    ] {
        let malformed =
            replace_source_fragment(&fixture.plan, fixture.recursive_source_fragment, |parts| {
                parts.sink = sink
            });
        assert!(validate_plan(&malformed).is_err());
        // The receiver cannot reconstruct remote executable dependencies. Its
        // own self-consistent input boundary remains independently valid.
        validate_fragment(&malformed.fragments()[&fixture.target_fragment], &cuts).unwrap();
    }
}

#[test]
fn local_validation_rejects_an_unattached_runtime_filter() {
    let fixture = acyclic_cross_fragment_fixture();
    let mut cuts = fragment_cuts(&fixture.plan, fixture.target_fragment).unwrap();
    cuts.runtime_filters = Box::default();
    let error = validate_fragment(&fixture.plan.fragments()[&fixture.target_fragment], &cuts)
        .unwrap_err()
        .to_string();
    assert!(error.contains("differ from fragment attachments"));
}
