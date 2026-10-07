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

fn json_rows(
    builder: &mut FragmentBuilder,
    kind: Option<ValueLogicalKind>,
    distribution: Distribution,
) -> (NodeId, ValueId) {
    let node = builder.reserve_node_id().unwrap();
    let value_type = ty(DataType::Utf8, true);
    let expr = builder
        .add_expression(
            node,
            value_type.clone(),
            ExprKind::Literal(LiteralValue::Utf8("null".into())),
        )
        .unwrap();
    let value = builder
        .add_value_with_logical_kind(
            value_type,
            ValueOrigin::NodeOutput {
                node,
                output_ordinal: 0,
            },
            kind,
        )
        .unwrap();
    let multiplicity = if distribution == Distribution::Broadcast {
        RowMultiplicity::Replicated
    } else {
        RowMultiplicity::SingleCopy
    };
    builder
        .insert_node_unchecked(PhysicalNode {
            id: node,
            inputs: Box::default(),
            required_inputs: Box::default(),
            output_properties: PhysicalProperties {
                distribution,
                row_multiplicity: multiplicity,
                ordering: Box::default(),
            },
            output: OutputPort {
                node,
                columns: Box::from([value]),
            },
            kind: NodeKind::Values {
                rows: Box::from([Box::from([expr])]),
            },
        })
        .unwrap();
    (node, value)
}

fn membership_fragment(
    mode: MembershipDistribution,
    kind: Option<ValueLogicalKind>,
    mutate: impl FnOnce(&mut PhysicalNode),
) -> Result<Fragment, ValidationErrors> {
    let mut builder = FragmentBuilder::new(FragmentId::new(620));
    let (probe, lhs) = json_rows(&mut builder, kind, Distribution::Singleton);
    let distribution = if mode == MembershipDistribution::Singleton {
        Distribution::Singleton
    } else {
        Distribution::Broadcast
    };
    let (build, rhs) = json_rows(&mut builder, Some(ValueLogicalKind::Json), distribution);
    let node = builder.reserve_node_id().unwrap();
    let result = builder
        .add_value(
            ty(DataType::Boolean, true),
            ValueOrigin::NodeOutput {
                node,
                output_ordinal: 1,
            },
        )
        .unwrap();
    let mut physical = PhysicalNode {
        id: node,
        inputs: Box::from([probe, build]),
        required_inputs: Box::from([
            singleton(),
            PhysicalProperties {
                distribution: if mode == MembershipDistribution::Singleton {
                    Distribution::Singleton
                } else {
                    Distribution::Broadcast
                },
                row_multiplicity: if mode == MembershipDistribution::Singleton {
                    RowMultiplicity::SingleCopy
                } else {
                    RowMultiplicity::Replicated
                },
                ordering: Box::default(),
            },
        ]),
        output_properties: singleton(),
        output: OutputPort {
            node,
            columns: Box::from([lhs, result]),
        },
        kind: NodeKind::Membership {
            spec: MembershipSpec {
                probe: lhs,
                build: rhs,
                result,
                negated: false,
                comparison: MembershipComparison::JsonInListV1,
                distribution: mode,
            },
        },
    };
    if mode == MembershipDistribution::BroadcastBuild {
        physical.required_inputs[0] = unconstrained();
    }
    mutate(&mut physical);
    builder.insert_node_unchecked(physical).unwrap();
    builder.finish_definition(node, FragmentSink::Noop, dop())
}

#[test]
fn membership_accepts_both_closed_placements_and_negation() {
    for mode in [
        MembershipDistribution::Singleton,
        MembershipDistribution::BroadcastBuild,
    ] {
        for negated in [false, true] {
            let fragment = membership_fragment(mode, Some(ValueLogicalKind::Json), |node| {
                let NodeKind::Membership { spec } = &mut node.kind else {
                    unreachable!()
                };
                spec.negated = negated;
            })
            .unwrap();
            let mut plan = PlanBuilder::new(version());
            plan.add_fragment(fragment).unwrap();
            plan.finish().unwrap();
        }
    }
}

#[test]
fn membership_plain_utf8_cannot_supply_json_evidence() {
    let error = membership_fragment(MembershipDistribution::Singleton, None, |_| {})
        .unwrap_err()
        .to_string();
    assert!(error.contains("declared JSON Utf8 operands"), "{error}");
}

#[test]
fn membership_slots_belong_to_their_exact_children() {
    let error = membership_fragment(
        MembershipDistribution::Singleton,
        Some(ValueLogicalKind::Json),
        |node| {
            let NodeKind::Membership { spec } = &mut node.kind else {
                unreachable!()
            };
            std::mem::swap(&mut spec.probe, &mut spec.build);
        },
    )
    .unwrap_err()
    .to_string();
    assert!(error.contains("exact child output occurrence"), "{error}");
}

#[test]
fn membership_output_cannot_drop_probe_or_leak_build() {
    for leak_build in [false, true] {
        let error = membership_fragment(
            MembershipDistribution::Singleton,
            Some(ValueLogicalKind::Json),
            |node| {
                let NodeKind::Membership { spec } = &node.kind else {
                    unreachable!()
                };
                node.output.columns = if leak_build {
                    Box::from([spec.build, spec.result])
                } else {
                    Box::from([spec.result])
                };
            },
        )
        .unwrap_err()
        .to_string();
        assert!(error.contains("output"), "{error}");
    }
}

#[test]
fn membership_result_cannot_alias_any_input() {
    let error = membership_fragment(
        MembershipDistribution::Singleton,
        Some(ValueLogicalKind::Json),
        |node| {
            let NodeKind::Membership { spec } = &mut node.kind else {
                unreachable!()
            };
            spec.result = spec.probe;
            node.output.columns[1] = spec.probe;
        },
    )
    .unwrap_err()
    .to_string();
    assert!(
        error.contains("fresh node-owned nullable Boolean"),
        "{error}"
    );
}

#[test]
fn membership_rejects_replica_and_requirement_mismatches() {
    for invalid_output in [false, true] {
        let error = membership_fragment(
            MembershipDistribution::BroadcastBuild,
            Some(ValueLogicalKind::Json),
            |node| {
                if invalid_output {
                    node.output_properties.row_multiplicity = RowMultiplicity::Replicated;
                } else {
                    node.required_inputs[1] = singleton();
                }
            },
        )
        .unwrap_err()
        .to_string();
        assert!(
            error.contains("properties") || error.contains("placement"),
            "{error}"
        );
    }
}

#[test]
fn membership_invalid_json_carrier_does_not_consume_value_identity() {
    let mut builder = FragmentBuilder::new(FragmentId::new(621));
    let origin = ValueOrigin::NodeOutput {
        node: NodeId::new(0),
        output_ordinal: 0,
    };
    assert!(matches!(
        builder.add_value_with_logical_kind(
            ty(DataType::Int64, true),
            origin.clone(),
            Some(ValueLogicalKind::Json)
        ),
        Err(BuildError::InvalidLogicalKind(_))
    ));
    assert_eq!(
        builder
            .add_value(ty(DataType::Int64, true), origin)
            .unwrap(),
        ValueId::new(0)
    );
}

fn rewrite(fragment: &Fragment, mutate: impl FnOnce(&mut crate::plan::FragmentParts)) -> Fragment {
    let mut parts = crate::plan::FragmentParts {
        id: fragment.id(),
        root: fragment.root(),
        values: fragment.values().clone(),
        expressions: fragment.expressions().clone(),
        nodes: fragment.nodes().clone(),
        sink: fragment.sink().clone(),
        dop_domain: fragment.dop_domain(),
        runtime_filters: fragment.runtime_filters().into(),
    };
    mutate(&mut parts);
    Fragment::from(parts)
}

#[test]
fn membership_nullable_boolean_result_is_checked_independently_of_output_port() {
    let fragment = membership_fragment(
        MembershipDistribution::Singleton,
        Some(ValueLogicalKind::Json),
        |_| {},
    )
    .unwrap();
    for wrong_type in [false, true] {
        let broken = rewrite(&fragment, |parts| {
            let NodeKind::Membership { spec } = &parts.nodes[&parts.root].kind else {
                unreachable!()
            };
            let result = parts.values.get_mut(&spec.result).unwrap();
            if wrong_type {
                result.ty = ty(DataType::Utf8, true);
            } else {
                result.ty.nullable = false;
            }
        });
        let error = crate::validation::validate_fragment_definition(&broken)
            .unwrap_err()
            .to_string();
        assert!(
            error.contains("fresh node-owned nullable Boolean"),
            "{error}"
        );
    }
}

#[test]
fn membership_detached_cut_preserves_exact_json_evidence() {
    for (destination_kind, source_kind, accepted) in [
        (
            Some(ValueLogicalKind::Json),
            Some(ValueLogicalKind::Json),
            true,
        ),
        (None, Some(ValueLogicalKind::Json), false),
        (Some(ValueLogicalKind::Json), None, false),
    ] {
        let mut builder = FragmentBuilder::new(FragmentId::new(625));
        let node = builder.reserve_node_id().unwrap();
        let edge = EdgeId::new(625);
        let source = ValueId::new(42);
        let dest = builder
            .add_value_with_logical_kind(
                ty(DataType::Utf8, true),
                ValueOrigin::ExchangeImport {
                    edge,
                    source_value: source,
                },
                destination_kind,
            )
            .unwrap();
        builder
            .add_exchange_source(
                node,
                edge,
                Box::from([(source, dest)]),
                Box::from([dest]),
                Distribution::Singleton,
                RowMultiplicity::SingleCopy,
            )
            .unwrap();
        let fragment = builder
            .finish_definition(node, FragmentSink::Noop, dop())
            .unwrap();
        let cuts = FragmentCuts {
            inbound: Box::from([InboundFragmentCut {
                edge,
                kind: EdgeKind::Stream,
                source_fragment: FragmentId::new(624),
                destination_node: node,
                imports: Box::from([CutImport {
                    source: CutValue {
                        value: source,
                        ty: ty(DataType::Utf8, true),
                        logical_kind: source_kind,
                    },
                    destination: dest,
                }]),
                partitioning: EdgePartitioning {
                    source: Distribution::Singleton,
                    source_multiplicity: RowMultiplicity::SingleCopy,
                    destination: Distribution::Singleton,
                    destination_multiplicity: RowMultiplicity::SingleCopy,
                },
                source_bindings: Box::default(),
                has_source_free_rows: true,
                change_stream_writer: None,
                writer_result: None,
            }]),
            ..FragmentCuts::default()
        };
        let result = validate_fragment(&fragment, &cuts);
        if accepted {
            result.unwrap();
        } else {
            assert!(
                result
                    .unwrap_err()
                    .to_string()
                    .contains("inbound cut type or destination origin")
            );
        }
    }
}

#[test]
fn membership_identity_project_cannot_change_json_evidence() {
    for (source_kind, output_kind, accepted) in [
        (
            Some(ValueLogicalKind::Json),
            Some(ValueLogicalKind::Json),
            true,
        ),
        (None, Some(ValueLogicalKind::Json), false),
        (Some(ValueLogicalKind::Json), None, false),
    ] {
        let mut builder = FragmentBuilder::new(FragmentId::new(626));
        let (input, value) = json_rows(&mut builder, source_kind, Distribution::Singleton);
        let node = builder.reserve_node_id().unwrap();
        let expr = builder
            .add_expression(node, ty(DataType::Utf8, true), ExprKind::Value(value))
            .unwrap();
        let output = builder
            .add_value_with_logical_kind(
                ty(DataType::Utf8, true),
                ValueOrigin::Expr { node, expr },
                output_kind,
            )
            .unwrap();
        builder
            .add_project_with_retention(
                node,
                input,
                Box::from([(expr, output)]),
                Box::from([output]),
                ProjectRetentionAdmission::CheckedTask,
            )
            .unwrap();
        let result = builder.finish_definition(node, FragmentSink::Noop, dop());
        if accepted {
            result.unwrap();
        } else {
            assert!(result.unwrap_err().to_string().contains("logical"));
        }
    }
}

#[test]
fn membership_builder_preserves_probe_ordering_and_complete_output() {
    let mut builder = FragmentBuilder::new(FragmentId::new(627));
    let probe = builder.reserve_node_id().unwrap();
    let json_type = ty(DataType::Utf8, true);
    let json_expr = builder
        .add_expression(
            probe,
            json_type.clone(),
            ExprKind::Literal(LiteralValue::Utf8("{}".into())),
        )
        .unwrap();
    let json = builder
        .add_value_with_logical_kind(
            json_type,
            ValueOrigin::NodeOutput {
                node: probe,
                output_ordinal: 0,
            },
            Some(ValueLogicalKind::Json),
        )
        .unwrap();
    let order_expr = builder
        .add_expression(
            probe,
            ty(DataType::Int64, false),
            ExprKind::Literal(LiteralValue::Int64(1)),
        )
        .unwrap();
    let order = builder
        .add_value(
            ty(DataType::Int64, false),
            ValueOrigin::NodeOutput {
                node: probe,
                output_ordinal: 1,
            },
        )
        .unwrap();
    builder
        .add_values(
            probe,
            Box::from([Box::from([json_expr, order_expr])]),
            Box::from([json, order]),
        )
        .unwrap();
    let sorted = builder.reserve_node_id().unwrap();
    let sort_expr = builder
        .add_expression(sorted, ty(DataType::Int64, false), ExprKind::Value(order))
        .unwrap();
    builder
        .add_sort(
            sorted,
            probe,
            Box::from([SortExpr {
                expr: sort_expr,
                direction: SortDirection::Ascending,
                null_ordering: NullOrdering::Last,
            }]),
            SortMode::Global,
        )
        .unwrap();
    let (build, rhs) = json_rows(
        &mut builder,
        Some(ValueLogicalKind::Json),
        Distribution::Singleton,
    );
    let node = builder.reserve_node_id().unwrap();
    let result = builder
        .add_value(
            ty(DataType::Boolean, true),
            ValueOrigin::NodeOutput {
                node,
                output_ordinal: 2,
            },
        )
        .unwrap();
    builder
        .add_membership(
            node,
            sorted,
            build,
            MembershipSpec {
                probe: json,
                build: rhs,
                result,
                negated: false,
                comparison: MembershipComparison::JsonInListV1,
                distribution: MembershipDistribution::Singleton,
            },
        )
        .unwrap();
    let fragment = builder
        .finish_definition(node, FragmentSink::Noop, dop())
        .unwrap();
    assert_eq!(
        fragment.nodes()[&node].output.columns.as_ref(),
        [json, order, result]
    );
    assert_eq!(
        fragment.nodes()[&node].output_properties,
        fragment.nodes()[&sorted].output_properties
    );
    assert!(
        !fragment.nodes()[&node]
            .output_properties
            .ordering
            .is_empty()
    );
    let broken = rewrite(&fragment, |parts| {
        parts
            .nodes
            .get_mut(&node)
            .unwrap()
            .output_properties
            .ordering = Box::default()
    });
    assert!(crate::validation::validate_fragment_definition(&broken).is_err());
}

#[test]
fn membership_unpivot_mapping_cannot_change_json_evidence() {
    for (input_kind, output_kind, accepted) in [
        (
            Some(ValueLogicalKind::Json),
            Some(ValueLogicalKind::Json),
            true,
        ),
        (None, Some(ValueLogicalKind::Json), false),
        (Some(ValueLogicalKind::Json), None, false),
    ] {
        let mut builder = FragmentBuilder::new(FragmentId::new(628));
        let (input_node, input) = json_rows(&mut builder, input_kind, Distribution::Singleton);
        let node = builder.reserve_node_id().unwrap();
        let output = builder
            .add_value_with_logical_kind(
                ty(DataType::Utf8, true),
                ValueOrigin::NodeOutput {
                    node,
                    output_ordinal: 0,
                },
                output_kind,
            )
            .unwrap();
        builder
            .insert_node_unchecked(PhysicalNode {
                id: node,
                inputs: Box::from([input_node]),
                required_inputs: Box::from([unconstrained()]),
                output_properties: singleton(),
                output: OutputPort {
                    node,
                    columns: Box::from([output]),
                },
                kind: NodeKind::Unpivot {
                    spec: UnpivotSpec {
                        passthrough: Box::default(),
                        value_output: output,
                        literal_outputs: Box::default(),
                        mappings: Box::from([UnpivotValueMapping {
                            input,
                            constants: Box::default(),
                        }]),
                        max_output_rows: 1024,
                        max_output_bytes: 1024 * 1024,
                    },
                },
            })
            .unwrap();
        let result = builder.finish_definition(node, FragmentSink::Noop, dop());
        if accepted {
            result.unwrap();
        } else {
            assert!(
                result
                    .unwrap_err()
                    .to_string()
                    .contains("mapping input type differs")
            );
        }
    }
}
