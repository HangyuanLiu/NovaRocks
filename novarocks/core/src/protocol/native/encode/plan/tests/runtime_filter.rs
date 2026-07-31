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

use super::super::runtime_filter::{
    encode_runtime_filter_activation, encode_runtime_filter_apply_point,
    encode_runtime_filter_binding_table, encode_runtime_filter_capability,
    encode_runtime_filter_completion, encode_runtime_filter_contribution_kind,
    encode_runtime_filter_membership_contract, encode_runtime_filter_ordered_contract,
    encode_runtime_filter_producer_target, encode_runtime_filter_topk_reduction,
};
use super::*;
use crate::runtime_filter::model::contract::{
    NullOrder, OrderContract, OrderKeyContract, SortDirection, TopKSummaryRequirement,
};
use crate::runtime_filter::model::graph::ProducerBindingTarget;
use crate::runtime_filter::port::ordered_bound::RuntimeOrderContract;
use crate::runtime_filter::port::topk_summary::RuntimeTopKSummaryContract;
use crate::sql::analysis::{ExprKind, TypedExpr};
use crate::sql::column_id::ColumnId;
use crate::sql::planner::physical::runtime_filter::{
    RuntimeFilterBuildIntent, RuntimeFilterProbeIntent,
};
use crate::sql::planner::physical::{JoinExecutionMode, PhysicalPlanKind};

#[test]
fn full_plan_encoding_requires_prepared_runtime_filter_binding_tables() {
    let plan = two_fragment_stream_plan_for_test();
    let error = encode_distributed_plan_with_context(
        &plan,
        NativePlanEncodeContext {
            scan_bindings: None,
            node_outputs: None,
            fragment_edge_outputs: None,
            write_contracts: None,
            runtime_filter_bindings: None,
        },
    )
    .expect_err("full-plan encoding without prepared RF binding tables must fail");

    assert_eq!(
        error,
        "native distributed plan encoding requires prepared runtime filter binding tables"
    );
}

#[test]
fn producer_binding_target_encoder_rejects_ordinal_overflow() {
    let join_error = encode_runtime_filter_producer_target(
        17,
        ProducerBindingTarget::JoinBuildKey {
            ordinal: usize::MAX,
        },
    )
    .expect_err("join ordinal overflow must fail");
    assert!(
        join_error.contains("ordinal does not fit u32"),
        "{join_error}"
    );

    let aggregate_error = encode_runtime_filter_producer_target(
        18,
        ProducerBindingTarget::AggregateTopNKey {
            group_key_ordinal: usize::MAX,
            limit: std::num::NonZeroU32::new(7).unwrap(),
        },
    )
    .expect_err("aggregate ordinal overflow must fail");
    assert!(
        aggregate_error.contains("group key ordinal does not fit u32"),
        "{aggregate_error}"
    );
}

#[test]
fn native_encoder_round_trips_all_binding_roles_contracts_and_locations() {
    let probe_expr = TypedExpr {
        kind: ExprKind::ColumnRef {
            column_id: ColumnId::new_for_test(1),
            qualifier: Some("probe".to_string()),
            column: "k".to_string(),
        },
        data_type: DataType::Int64,
        nullable: false,
    };
    let build_expr = TypedExpr {
        kind: ExprKind::ColumnRef {
            column_id: ColumnId::new_for_test(2),
            qualifier: Some("build".to_string()),
            column: "k".to_string(),
        },
        data_type: DataType::Int64,
        nullable: false,
    };
    let probe_output = vec![output_column(1, "probe", DataType::Int64)];
    let build_output = vec![output_column(2, "build", DataType::Int64)];
    let probe = crate::sql::planner::physical::PhysicalPlanNode {
        kind: PhysicalPlanKind::Values(crate::sql::planner::payload::PlanValuesNode {
            rows: Vec::new(),
            columns: probe_output.clone(),
        }),
        children: Vec::new(),
        output_columns: probe_output,
        stats: stats(),
        probe_runtime_filters: vec![RuntimeFilterProbeIntent {
            filter_id: 41,
            probe_expr: probe_expr.clone(),
        }],
    };
    let build = crate::sql::planner::physical::PhysicalPlanNode {
        kind: PhysicalPlanKind::Values(crate::sql::planner::payload::PlanValuesNode {
            rows: Vec::new(),
            columns: build_output.clone(),
        }),
        children: Vec::new(),
        output_columns: build_output,
        stats: stats(),
        probe_runtime_filters: Vec::new(),
    };
    let physical = crate::sql::planner::physical::PhysicalPlanNode {
        kind: PhysicalPlanKind::HashJoin(Box::new(
            crate::sql::planner::physical::PhysicalHashJoinNode {
                join_type: JoinKind::Inner,
                eq_conditions: vec![crate::sql::planner::physical::PhysicalHashJoinEqCondition {
                    left: probe_expr.clone(),
                    right: build_expr.clone(),
                    null_safe: false,
                }],
                other_condition: None,
                distribution: JoinDistribution::Broadcast,
                execution_mode: Some(JoinExecutionMode::Broadcast),
                build_runtime_filters: vec![RuntimeFilterBuildIntent {
                    filter_id: 41,
                    build_expr,
                    probe_expr,
                    expr_order: 0,
                    execution_mode: JoinExecutionMode::Broadcast,
                }],
                output_columns: vec![
                    output_column(1, "probe", DataType::Int64),
                    output_column(2, "build", DataType::Int64),
                ],
            },
        )),
        children: vec![probe, build],
        output_columns: vec![
            output_column(1, "probe", DataType::Int64),
            output_column(2, "build", DataType::Int64),
        ],
        stats: stats(),
        probe_runtime_filters: Vec::new(),
    };

    let distributed = crate::sql::planner::distributed::build::build_distributed_plan(&physical)
        .expect("build Graph-owned RF plan");
    assert_eq!(distributed.runtime_filter_graph().channel_count(), 1);
    let registry = crate::connector::ConnectorRegistry::new();
    let controls = crate::connector::FixtureControlResolver::new(registry.clone());
    let prepared = crate::query_execution::preparation::prepare_fragments(
        &distributed,
        &registry,
        &controls,
        &crate::connector::test_request_context(),
        None,
        crate::query_execution::preparation::ScanPreparationOptions::default(),
    )
    .expect("prepare Graph-owned RF projection");
    let encoded = encode_distributed_plan_with_context(
        &distributed,
        NativePlanEncodeContext {
            scan_bindings: Some(prepared.scan_bindings()),
            node_outputs: None,
            fragment_edge_outputs: None,
            write_contracts: None,
            runtime_filter_bindings: Some(&prepared),
        },
    )
    .expect("encode Graph-owned RF plan");
    let root = encoded.fragments[0].root.as_ref().expect("encoded root");
    assert!(!root.runtime_filter_binding_ids.is_empty());
    assert!(!root.children[0].runtime_filter_binding_ids.is_empty());
    let Some(plan::distributed_node::Payload::Physical(physical)) = root.payload.as_ref() else {
        panic!("expected physical HashJoin root");
    };
    let Some(plan::plan_node::Kind::HashJoin(_join)) = physical.kind.as_ref() else {
        panic!("expected HashJoin payload");
    };

    let bundle = crate::protocol::native::encode::bundle::encode_native_fragment_bundle(
        &distributed,
        &prepared,
    )
    .expect("encode prepared fragment binding tables");
    let encoded_binding_count = bundle
        .fragments_in_id_order()
        .map(|(_, fragment)| {
            fragment
                .runtime_filter_bindings
                .as_ref()
                .expect("every fragment owns an explicit binding table")
                .bindings
                .len()
        })
        .sum::<usize>();
    assert_eq!(
        encoded_binding_count,
        distributed.runtime_filter_graph().binding_count(),
        "the encoder must materialize every prepared binding exactly once"
    );

    for (_, fragment) in bundle.fragments_in_id_order() {
        let bindings = &fragment
            .runtime_filter_bindings
            .as_ref()
            .expect("every fragment owns an explicit binding table")
            .bindings;
        assert!(
            bindings
                .windows(2)
                .all(|pair| pair[0].binding_id < pair[1].binding_id),
            "each fragment-local table must use deterministic binding-id order"
        );
    }
    let encoded_bindings = bundle
        .fragments_in_id_order()
        .flat_map(|(_, fragment)| {
            fragment
                .runtime_filter_bindings
                .as_ref()
                .expect("every fragment owns an explicit binding table")
                .bindings
                .iter()
        })
        .collect::<Vec<_>>();
    let mut producer_count = 0;
    let mut consumer_count = 0;
    for binding in &encoded_bindings {
        let source = distributed
            .runtime_filter_graph()
            .binding(crate::runtime_filter::model::contract::BindingId::new(
                binding.binding_id,
            ))
            .expect("encoded binding originates in the sealed graph");
        assert_eq!(binding.channel_id, source.channel_id.get());
        assert_eq!(binding.node_id, source.location.node_id.get());
        assert_eq!(
            binding.apply_point,
            encode_runtime_filter_apply_point(source.apply_point)
        );
        assert!(binding.expression.is_some());
        let Some(plan::runtime_filter_contract::Kind::Membership(contract)) = binding
            .contract
            .as_ref()
            .and_then(|contract| contract.kind.as_ref())
        else {
            panic!("broadcast join fixture must encode membership contracts");
        };
        assert!(!contract.canonical_schema.is_empty());
        assert_eq!(contract.schema_digest.len(), 32);
        assert!(matches!(
            binding
                .reduction
                .as_ref()
                .and_then(|reduction| reduction.kind.as_ref()),
            Some(plan::runtime_filter_reduction_contract::Kind::SetUnion(
                true
            ))
        ));
        match (binding.role.as_ref().expect("binding role"), &source.role) {
            (
                plan::runtime_filter_binding::Role::Producer(role),
                crate::runtime_filter::model::graph::RuntimeFilterBindingRole::Producer(
                    source_role,
                ),
            ) => {
                producer_count += 1;
                assert_eq!(
                    role.contribution_kinds,
                    source_role
                        .contribution_kinds
                        .iter()
                        .copied()
                        .map(encode_runtime_filter_contribution_kind)
                        .collect::<Vec<_>>()
                );
                assert_eq!(
                    role.completion_requirement,
                    encode_runtime_filter_completion(source_role.completion_requirement)
                );
            }
            (
                plan::runtime_filter_binding::Role::Consumer(role),
                crate::runtime_filter::model::graph::RuntimeFilterBindingRole::Consumer(
                    source_role,
                ),
            ) => {
                consumer_count += 1;
                assert_eq!(
                    role.capabilities,
                    source_role
                        .capabilities
                        .iter()
                        .copied()
                        .map(encode_runtime_filter_capability)
                        .collect::<Vec<_>>()
                );
                assert_eq!(
                    role.activation,
                    Some(encode_runtime_filter_activation(source_role.activation))
                );
            }
            _ => panic!("encoded binding role must match the sealed graph role"),
        }
    }
    assert_eq!((producer_count, consumer_count), (1, 1));

    fn collect_binding_ids(node: &plan::DistributedNode, ids: &mut Vec<u32>) {
        ids.extend_from_slice(&node.runtime_filter_binding_ids);
        for child in &node.children {
            collect_binding_ids(child, ids);
        }
    }
    let mut attached_ids = Vec::new();
    for (_, fragment) in bundle.fragments_in_id_order() {
        collect_binding_ids(
            fragment.root.as_ref().expect("fragment root"),
            &mut attached_ids,
        );
    }
    attached_ids.sort_unstable();
    assert_eq!(
        attached_ids,
        encoded_bindings
            .iter()
            .map(|binding| binding.binding_id)
            .collect::<Vec<_>>(),
        "sealed node binding attachments must round-trip with the table"
    );

    let second = crate::protocol::native::encode::bundle::encode_native_fragment_bundle(
        &distributed,
        &prepared,
    )
    .expect("deterministic second encoding");
    for (fragment_id, first_fragment) in bundle.fragments_in_id_order() {
        assert_eq!(
            first_fragment.runtime_filter_bindings,
            second
                .get(fragment_id)
                .expect("same prepared fragment set")
                .runtime_filter_bindings
        );
    }

    let (&fragment_id, prepared_fragment) = prepared
        .fragment_ids()
        .iter()
        .find_map(|fragment_id| {
            prepared
                .fragment(*fragment_id)
                .filter(|fragment| !fragment.runtime_filter_bindings().is_empty())
                .map(|fragment| (fragment_id, fragment))
        })
        .expect("fixture has a nonempty binding table");
    let mismatch = encode_runtime_filter_binding_table(
        fragment_id
            .checked_add(100)
            .expect("small fixture fragment id"),
        prepared_fragment.runtime_filter_bindings(),
    )
    .expect_err("enclosing fragment mismatch must fail");
    assert!(mismatch.contains("fragment mismatch"), "{mismatch}");
}

#[test]
fn native_encoder_rejects_noncanonical_membership_digest() {
    let schema = crate::runtime_filter::port::artifact::ArtifactMembershipSchema::new(
        &DataType::Int64,
        crate::runtime_filter::model::contract::NullSemantics::NeverMatches,
    )
    .expect("canonical membership schema");
    let error = encode_runtime_filter_membership_contract(7, schema.canonical_bytes(), [0xAB; 32])
        .expect_err("digest drift must fail before encoding");
    assert_eq!(
        error,
        "native runtime filter binding id=7 membership schema digest does not match canonical bytes"
    );
}

fn canonical_order_contract_for_encoder_test() -> OrderContract {
    let keys = vec![
        OrderKeyContract {
            data_type: DataType::Int64,
            direction: SortDirection::Descending,
            null_order: NullOrder::First,
        },
        OrderKeyContract {
            data_type: DataType::Utf8,
            direction: SortDirection::Ascending,
            null_order: NullOrder::Last,
        },
    ];
    OrderContract {
        comparator_digest: crate::runtime_filter::port::ordered_bound::comparator_digest_for_test(
            &keys,
            crate::runtime_filter::port::ordered_bound::COMPARATOR_ALGORITHM_VERSION,
        ),
        keys,
        inclusive: true,
    }
}

#[test]
fn native_encoder_preserves_ordered_and_topk_contracts() {
    let order = canonical_order_contract_for_encoder_test();
    let runtime_order = RuntimeOrderContract::try_from_plan(&order).expect("canonical order");
    let encoded_order = encode_runtime_filter_ordered_contract(
        19,
        runtime_order.keys(),
        runtime_order.plan_comparator_digest(),
        runtime_order.digest(),
    )
    .expect("encode canonical ordered contract");
    assert_eq!(encoded_order.keys.len(), 2);
    assert_eq!(
        encoded_order.keys[0].r#type,
        Some(encode_type(&DataType::Int64).expect("encode Int64"))
    );
    assert_eq!(
        encoded_order.keys[0].direction,
        i32::from(plan::RuntimeFilterSortDirection::Descending)
    );
    assert_eq!(
        encoded_order.keys[0].null_order,
        i32::from(plan::RuntimeFilterNullOrder::First)
    );
    assert_eq!(
        encoded_order.keys[1].r#type,
        Some(encode_type(&DataType::Utf8).expect("encode Utf8"))
    );
    assert_eq!(
        encoded_order.keys[1].direction,
        i32::from(plan::RuntimeFilterSortDirection::Ascending)
    );
    assert_eq!(
        encoded_order.keys[1].null_order,
        i32::from(plan::RuntimeFilterNullOrder::Last)
    );
    assert_eq!(
        encoded_order.comparator_digest,
        runtime_order.plan_comparator_digest().get()
    );
    assert_eq!(
        encoded_order.order_contract_digest,
        runtime_order.digest().bytes()
    );

    let requirement = TopKSummaryRequirement::try_new(13).expect("nonzero K");
    let runtime_topk = RuntimeTopKSummaryContract::try_from_plan(&order, requirement)
        .expect("canonical TopK contract");
    let encoded_topk = encode_runtime_filter_topk_reduction(
        19,
        runtime_order.keys(),
        runtime_order.plan_comparator_digest(),
        runtime_topk.k(),
        runtime_topk.digest(),
    )
    .expect("encode canonical TopK reduction");
    assert_eq!(encoded_topk.k, 13);
    assert_eq!(encoded_topk.contract_digest, runtime_topk.digest().bytes());
}

#[test]
fn native_encoder_rejects_corrupt_ordered_and_topk_digests() {
    let order = canonical_order_contract_for_encoder_test();
    let runtime_order = RuntimeOrderContract::try_from_plan(&order).expect("canonical order");
    let ordered_error = encode_runtime_filter_ordered_contract(
        23,
        runtime_order.keys(),
        runtime_order.plan_comparator_digest(),
        crate::runtime_filter::port::ordered_bound::OrderContractDigest::from_bytes_for_codec(
            [0xA5; 32],
        ),
    )
    .expect_err("corrupt order digest must fail");
    assert_eq!(
        ordered_error,
        "native runtime filter binding id=23 order contract digest does not match typed keys"
    );

    let canonical_topk = RuntimeTopKSummaryContract::try_from_plan(
        &order,
        TopKSummaryRequirement::try_new(13).expect("nonzero K"),
    )
    .expect("canonical TopK contract");
    let topk_error = encode_runtime_filter_topk_reduction(
        23,
        runtime_order.keys(),
        runtime_order.plan_comparator_digest(),
        TopKSummaryRequirement::try_new(14)
            .expect("different nonzero K")
            .k(),
        canonical_topk.digest(),
    )
    .expect_err("digest for a different K must fail");
    assert_eq!(
        topk_error,
        "native runtime filter binding id=23 TopK digest does not match typed order keys and K"
    );
}

#[test]
fn native_encoder_emits_explicit_empty_fragment_table() {
    let distributed = two_fragment_stream_plan_for_test();
    assert!(distributed.runtime_filter_graph().is_empty());
    let registry = crate::connector::ConnectorRegistry::new();
    let controls = crate::connector::FixtureControlResolver::new(registry.clone());
    let prepared = crate::query_execution::preparation::prepare_fragments(
        &distributed,
        &registry,
        &controls,
        &crate::connector::test_request_context(),
        None,
        crate::query_execution::preparation::ScanPreparationOptions::default(),
    )
    .expect("prepare no-runtime-filter plan");
    let bundle = crate::protocol::native::encode::bundle::encode_native_fragment_bundle(
        &distributed,
        &prepared,
    )
    .expect("encode explicit empty tables");
    for (fragment_id, fragment) in bundle.fragments_in_id_order() {
        let table = fragment
            .runtime_filter_bindings
            .as_ref()
            .expect("empty table is explicit, never absent");
        assert_eq!(table.fragment_id, fragment_id);
        assert!(table.bindings.is_empty());
    }
}
