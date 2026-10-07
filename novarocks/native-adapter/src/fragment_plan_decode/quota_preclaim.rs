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

//! Native typed quota preselection projection.
use crate::fragment_error::NativeFragmentDecodeError;
use crate::fragment_layout::decode_output_layout;
use crate::fragment_plan_node::NativeLoweredPlanNode;
use novarocks_execution::exec::chunk::{ChunkSchemaRef, SlotLayout};
use novarocks_execution::exec::node::quota_preclaim::{
    QuotaNeed, QuotaPreclaimNode, QuotaPreclaimSpec,
};
use novarocks_execution::exec::node::{ExecNode, ExecNodeKind};
use novarocks_proto_codec::FieldPath;
use novarocks_proto_models::plan;
use novarocks_types::SlotId;

pub(super) fn column(
    schema: &ChunkSchemaRef,
    id: u32,
    path: FieldPath,
) -> Result<usize, NativeFragmentDecodeError> {
    schema.index_of(SlotId::new(id)).ok_or_else(|| {
        NativeFragmentDecodeError::inconsistent(path, "quota column is absent from its exact input")
    })
}
pub(super) fn need(
    wire: Option<&plan::QuotaNeed>,
    schema: &ChunkSchemaRef,
    path: FieldPath,
) -> Result<QuotaNeed, NativeFragmentDecodeError> {
    let kind = wire.and_then(|v| v.kind.as_ref()).ok_or_else(|| {
        NativeFragmentDecodeError::missing(
            path.clone(),
            "quota need requires an explicit interpretation",
        )
    })?;
    Ok(match kind {
        plan::quota_need::Kind::CountColumnId(id) => QuotaNeed::Count {
            column: column(schema, *id, path.field("count_column_id"))?,
        },
        plan::quota_need::Kind::NegativeWeightColumnId(id) => QuotaNeed::NegativeWeight {
            column: column(schema, *id, path.field("negative_weight_column_id"))?,
        },
    })
}
pub(super) fn limits(
    content: i32,
    domains: i32,
    bytes: u64,
    path: FieldPath,
) -> Result<usize, NativeFragmentDecodeError> {
    if plan::ResultContentEquivalence::try_from(content)
        != Ok(plan::ResultContentEquivalence::NativeResultContentV1)
    {
        return Err(NativeFragmentDecodeError::invalid_value(
            path.field("content_equivalence"),
            "quota requires NativeResultContentV1",
        ));
    }
    if domains < 0 {
        return Err(NativeFragmentDecodeError::invalid_value(
            path.field("preselection_domain_node_id"),
            "quota domain reference must be nonnegative",
        ));
    }
    if bytes == 0 || bytes > i64::MAX as u64 {
        return Err(NativeFragmentDecodeError::invalid_value(
            path.field("max_state_bytes"),
            "quota state budget must be positive and representable",
        ));
    }
    usize::try_from(bytes).map_err(|_| {
        NativeFragmentDecodeError::invalid_value(
            path.field("max_state_bytes"),
            "quota state budget exceeds this platform",
        )
    })
}
pub(super) fn output(
    physical: &plan::PlanNode,
    path: FieldPath,
) -> Result<(SlotLayout, ChunkSchemaRef), NativeFragmentDecodeError> {
    let out = decode_output_layout(&physical.output_columns, path)
        .map_err(NativeFragmentDecodeError::from)?;
    Ok((
        SlotLayout::for_slots(out.slot_ids().iter().copied()),
        out.chunk_schema(),
    ))
}
pub(super) fn lower(
    node: &plan::DistributedNode,
    physical: &plan::PlanNode,
    wire: &plan::QuotaPreclaimNode,
    path: FieldPath,
    output_path: FieldPath,
    mut children: Vec<NativeLoweredPlanNode>,
) -> Result<NativeLoweredPlanNode, NativeFragmentDecodeError> {
    if children.len() != 2 {
        return Err(NativeFragmentDecodeError::inconsistent(
            path,
            "QuotaPreclaim requires demand and target inputs",
        ));
    }
    if wire.preselection_domain_node_id != node.node_id {
        return Err(NativeFragmentDecodeError::inconsistent(
            path.clone().field("preselection_domain_node_id"),
            "QuotaPreclaim must define its own exact domain",
        ));
    }
    let mut pending = vec![&node.children[1]];
    while let Some(target) = pending.pop() {
        if target.fragment_id != node.fragment_id
            || matches!(
                target.payload,
                Some(plan::distributed_node::Payload::Exchange(_))
            )
        {
            return Err(NativeFragmentDecodeError::inconsistent(
                path.clone(),
                "QuotaPreclaim target scan must remain in its exact owning fragment",
            ));
        }
        pending.extend(&target.children);
    }
    let target = children.pop().expect("two validated inputs");
    let demand = children.pop().expect("two validated inputs");
    let max_state_bytes = limits(
        wire.content_equivalence,
        wire.preselection_domain_node_id,
        wire.max_state_bytes,
        path.clone(),
    )?;
    if wire.target_value_column_ids.len() > target.output_schema.slots().len() {
        return Err(NativeFragmentDecodeError::invalid_value(
            path.clone().field("target_value_column_ids"),
            "quota content field count exceeds its input",
        ));
    }
    let spec = QuotaPreclaimSpec {
        demand_entry_id_column: column(
            &demand.output_schema,
            wire.demand_entry_id_column_id,
            path.clone().field("demand_entry_id_column_id"),
        )?,
        demand_key_column: column(
            &demand.output_schema,
            wire.demand_key_column_id,
            path.clone().field("demand_key_column_id"),
        )?,
        demand_need: need(
            wire.demand_need.as_ref(),
            &demand.output_schema,
            path.clone().field("demand_need"),
        )?,
        target_value_columns: wire
            .target_value_column_ids
            .iter()
            .enumerate()
            .map(|(i, id)| {
                column(
                    &target.output_schema,
                    *id,
                    path.clone().field("target_value_column_ids").index(i),
                )
            })
            .collect::<Result<Vec<_>, _>>()?,
        target_file_column: column(
            &target.output_schema,
            wire.target_file_column_id,
            path.clone().field("target_file_column_id"),
        )?,
        target_position_column: column(
            &target.output_schema,
            wire.target_position_column_id,
            path.clone().field("target_position_column_id"),
        )?,
        preselection_domain: novarocks_local_program::QuotaDomainId::try_new(
            wire.preselection_domain_node_id,
        )
        .map_err(|_| {
            NativeFragmentDecodeError::invalid_value(
                path.clone().field("preselection_domain_node_id"),
                "quota requires a nonnegative exact domain reference",
            )
        })?,
        max_state_bytes,
    };
    let (layout, output_schema) = output(physical, output_path)?;
    if !spec.validate(
        &demand.output_schema.arrow_schema_ref(),
        &target.output_schema.arrow_schema_ref(),
        &output_schema.arrow_schema_ref(),
    ) || output_schema
        .slots()
        .iter()
        .any(|slot| slot.field().is_nullable())
    {
        return Err(NativeFragmentDecodeError::inconsistent(
            path.clone(),
            "QuotaPreclaim has an invalid exact input/output schema",
        ));
    }
    let mut unique = std::collections::BTreeSet::new();
    if spec.target_value_columns.iter().any(|i| {
        !unique.insert(*i) || *i == spec.target_file_column || *i == spec.target_position_column
    }) || spec.demand_entry_id_column == spec.demand_key_column
        || spec.demand_entry_id_column == spec.demand_need.column()
        || spec.demand_key_column == spec.demand_need.column()
    {
        return Err(NativeFragmentDecodeError::inconsistent(
            path,
            "QuotaPreclaim requires distinct content and locator fields",
        ));
    }
    Ok(NativeLoweredPlanNode {
        node: ExecNode {
            kind: ExecNodeKind::QuotaPreclaim(QuotaPreclaimNode {
                node_id: node.node_id,
                demand: Box::new(demand.node),
                target: Box::new(target.node),
                spec,
                output_chunk_schema: output_schema.clone(),
                runtime_filters: Vec::new(),
            }),
        },
        layout,
        output_schema,
    })
}

#[cfg(test)]
mod tests {
    use super::super::decode_node;
    use super::*;
    use crate::fragment_decode_context::NativePlanDecodeContext;
    use arrow::datatypes::DataType;
    use novarocks_execution::exec::expr::ExprArena;
    use novarocks_proto_models::common;

    fn columns(ids: &[u32], types: &[DataType]) -> Vec<common::OutputColumn> {
        ids.iter()
            .zip(types)
            .map(|(id, ty)| common::OutputColumn {
                column_id: *id,
                name: format!("c{id}"),
                r#type: Some(novarocks_plan_codec::encode_native_type(ty).unwrap()),
                nullable: false,
                is_internal: false,
            })
            .collect()
    }
    fn physical(
        id: i32,
        kind: plan::plan_node::Kind,
        columns: Vec<common::OutputColumn>,
        children: Vec<plan::DistributedNode>,
    ) -> plan::DistributedNode {
        plan::DistributedNode {
            node_id: id,
            fragment_id: 1,
            tuple_ids: Vec::new(),
            nullable_tuple_ids: Vec::new(),
            limit: -1,
            runtime_filter_binding_ids: Vec::new(),
            children,
            payload: Some(plan::distributed_node::Payload::Physical(plan::PlanNode {
                output_columns: columns,
                kind: Some(kind),
            })),
        }
    }
    fn values(id: i32, ids: &[u32], types: &[DataType]) -> plan::DistributedNode {
        let cols = columns(ids, types);
        physical(
            id,
            plan::plan_node::Kind::Values(plan::ValuesNode {
                rows: Vec::new(),
                columns: cols.clone(),
            }),
            cols,
            Vec::new(),
        )
    }
    fn preclaim() -> plan::DistributedNode {
        physical(
            20,
            plan::plan_node::Kind::QuotaPreclaim(plan::QuotaPreclaimNode {
                demand_entry_id_column_id: 1,
                demand_key_column_id: 2,
                demand_need: Some(plan::QuotaNeed {
                    kind: Some(plan::quota_need::Kind::NegativeWeightColumnId(3)),
                }),
                demand_value_column_ids: Vec::new(),
                target_value_column_ids: vec![4],
                target_file_column_id: 5,
                target_position_column_id: 6,
                content_equivalence: plan::ResultContentEquivalence::NativeResultContentV1 as i32,
                preselection_domain_node_id: 20,
                max_state_bytes: 4096,
            }),
            columns(
                &[7, 8, 9],
                &[DataType::Binary, DataType::Utf8, DataType::Int64],
            ),
            vec![
                values(
                    10,
                    &[1, 2, 3],
                    &[DataType::Binary, DataType::Binary, DataType::Int64],
                ),
                values(
                    11,
                    &[4, 5, 6],
                    &[DataType::Int64, DataType::Utf8, DataType::Int64],
                ),
            ],
        )
    }
    fn trim() -> plan::DistributedNode {
        physical(
            30,
            plan::plan_node::Kind::QuotaTrim(plan::QuotaTrimNode {
                seed_entry_id_column_id: 1,
                seed_need: Some(plan::QuotaNeed {
                    kind: Some(plan::quota_need::Kind::CountColumnId(2)),
                }),
                candidate_entry_id_column_id: 3,
                candidate_file_column_id: 4,
                candidate_position_column_id: 5,
                content_equivalence: plan::ResultContentEquivalence::NativeResultContentV1 as i32,
                preselection_domain_node_id: 20,
                max_state_bytes: 4096,
            }),
            columns(&[6, 7], &[DataType::Utf8, DataType::Int64]),
            vec![
                values(10, &[1, 2], &[DataType::Binary, DataType::Int64]),
                values(
                    11,
                    &[3, 4, 5],
                    &[DataType::Binary, DataType::Utf8, DataType::Int64],
                ),
            ],
        )
    }
    fn decode(
        node: &plan::DistributedNode,
    ) -> Result<super::super::DecodedNode, NativeFragmentDecodeError> {
        decode_node(
            node,
            &mut ExprArena::default(),
            &NativePlanDecodeContext::default(),
        )
    }
    fn preclaim_wire(node: &mut plan::DistributedNode) -> &mut plan::QuotaPreclaimNode {
        let Some(plan::distributed_node::Payload::Physical(physical)) = &mut node.payload else {
            panic!("physical")
        };
        let Some(plan::plan_node::Kind::QuotaPreclaim(wire)) = &mut physical.kind else {
            panic!("preclaim")
        };
        wire
    }
    #[test]
    fn quota_nodes_decode_exact_typed_schemas_and_domain_reference() {
        let decoded = decode(&preclaim()).unwrap();
        let ExecNodeKind::QuotaPreclaim(node) = decoded.node.kind else {
            panic!("preclaim")
        };
        assert_eq!(node.spec.preselection_domain.get(), 20);
        assert_eq!(node.spec.target_value_columns, [0]);
        assert!(node.runtime_filters.is_empty());
        let decoded = decode(&trim()).unwrap();
        let ExecNodeKind::QuotaTrim(node) = decoded.node.kind else {
            panic!("trim")
        };
        assert_eq!(node.spec.preselection_domain.get(), 20);
    }
    #[test]
    fn quota_decode_rejects_missing_need_wrong_input_and_contract() {
        let mut node = preclaim();
        preclaim_wire(&mut node).demand_need = None;
        assert!(decode(&node).is_err());
        let mut node = preclaim();
        preclaim_wire(&mut node).target_file_column_id = 2;
        assert!(decode(&node).is_err());
        let mut node = preclaim();
        preclaim_wire(&mut node).content_equivalence = 0;
        assert!(decode(&node).is_err());
        let mut node = preclaim();
        preclaim_wire(&mut node).preselection_domain_node_id = 21;
        assert!(decode(&node).is_err());
    }
    #[test]
    fn quota_decode_rejects_zero_budget_duplicate_content_and_wrong_arity() {
        let mut node = preclaim();
        preclaim_wire(&mut node).max_state_bytes = 0;
        assert!(decode(&node).is_err());
        let mut node = preclaim();
        preclaim_wire(&mut node).target_value_column_ids = vec![4, 4];
        assert!(decode(&node).is_err());
        let mut node = preclaim();
        node.children.pop();
        assert!(decode(&node).is_err());
    }
    #[test]
    fn quota_content_producer_attachment_requires_the_exact_demand_field() {
        use super::super::decode_node_with_runtime_filters;
        use crate::fragment_runtime_filter_binding::NativeRuntimeFilterDecodeLedger;
        use novarocks_proto_models::expr;
        for (field, accepted) in [(3, true), (4, false)] {
            let mut node = preclaim();
            preclaim_wire(&mut node).demand_value_column_ids = vec![3];
            node.runtime_filter_binding_ids = vec![1];
            let binding = plan::RuntimeFilterBinding {
                binding_id: 1,
                channel_id: 2,
                node_id: 20,
                apply_point: plan::RuntimeFilterApplyPoint::NodeOutput as i32,
                expression: Some(expr::Expr {
                    r#type: Some(
                        novarocks_plan_codec::encode_native_type(&DataType::Int64).unwrap(),
                    ),
                    nullable: false,
                    kind: Some(expr::expr::Kind::ColumnRef(expr::ColumnRef {
                        column_id: field,
                        ..Default::default()
                    })),
                }),
                contract: Some(plan::RuntimeFilterContract {
                    kind: Some(plan::runtime_filter_contract::Kind::Membership(
                        plan::RuntimeFilterMembershipContract {
                            null_semantics:
                                plan::RuntimeFilterMembershipNullSemantics::NullSafeEqual as i32,
                        },
                    )),
                }),
                reduction: Some(plan::RuntimeFilterReductionContract {
                    kind: Some(plan::runtime_filter_reduction_contract::Kind::SetUnion(
                        true,
                    )),
                }),
                role: Some(plan::runtime_filter_binding::Role::Producer(
                    plan::RuntimeFilterProducerRole {
                        contribution_kinds: vec![
                            plan::RuntimeFilterContributionKind::ValueDomainDelta as i32,
                            plan::RuntimeFilterContributionKind::ProducerClosed as i32,
                        ],
                        completion_requirement:
                            plan::RuntimeFilterCompletionRequirement::ProducerClosed as i32,
                        target: Some(
                            plan::runtime_filter_producer_role::Target::QuotaContentField(
                                plan::RuntimeFilterQuotaContentField {
                                    field_ordinal: 0,
                                    content_equivalence:
                                        plan::ResultContentEquivalence::NativeResultContentV1 as i32,
                                    witness_id: 1,
                                },
                            ),
                        ),
                    },
                )),
            };
            let mut ledger = NativeRuntimeFilterDecodeLedger::decode(
                1,
                Some(&plan::RuntimeFilterBindingTable {
                    fragment_id: 1,
                    bindings: vec![binding],
                }),
            )
            .unwrap();
            let decoded = decode_node_with_runtime_filters(
                &node,
                &mut ExprArena::default(),
                &NativePlanDecodeContext::default(),
                &mut ledger,
            );
            assert_eq!(decoded.is_ok(), accepted);
            if accepted {
                let ExecNodeKind::QuotaPreclaim(quota) = decoded.unwrap().node.kind else {
                    panic!("quota")
                };
                assert_eq!(quota.runtime_filters.len(), 1);
            }
        }
    }
}
