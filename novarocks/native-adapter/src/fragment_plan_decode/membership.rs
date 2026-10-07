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

//! Exact native JSON value-membership construction.

use crate::fragment_error::NativeFragmentDecodeError;
use crate::fragment_layout::decode_fragment_output_layout;
use crate::fragment_plan_node::NativeLoweredPlanNode;
use arrow::datatypes::DataType;
use novarocks_execution::exec::chunk::{ChunkSchema, ChunkSchemaRef};
use novarocks_execution::exec::node::membership::{
    MembershipComparison, MembershipDistribution, MembershipNode, MembershipSpec,
};
use novarocks_execution::exec::node::{ExecNode, ExecNodeKind};
use novarocks_proto_codec::FieldPath;
use novarocks_proto_models::plan;
use novarocks_types::{SlotId, logical::LogicalType};
use std::sync::Arc;

pub(super) fn lower(
    node: &plan::DistributedNode,
    physical: &plan::PlanNode,
    wire: &plan::MembershipNode,
    path: FieldPath,
    output_path: FieldPath,
    mut children: Vec<NativeLoweredPlanNode>,
) -> Result<NativeLoweredPlanNode, NativeFragmentDecodeError> {
    if children.len() != 2 || node.children.len() != 2 {
        return Err(NativeFragmentDecodeError::inconsistent(
            path,
            "Membership requires ordered probe and build inputs",
        ));
    }
    if node
        .children
        .iter()
        .any(|child| child.fragment_id != node.fragment_id)
    {
        return Err(NativeFragmentDecodeError::inconsistent(
            path,
            "Membership children must belong to its fragment",
        ));
    }
    for (id, field) in [
        (wire.probe_column_id, "probe_column_id"),
        (wire.build_column_id, "build_column_id"),
        (wire.result_column_id, "result_column_id"),
    ] {
        if id == 0 {
            return Err(NativeFragmentDecodeError::missing(
                path.clone().field(field),
                "Membership requires a nonzero exact slot ID",
            ));
        }
    }
    let comparison = match plan::MembershipComparison::try_from(wire.comparison) {
        Ok(plan::MembershipComparison::JsonInListV1) => MembershipComparison::JsonInListV1,
        _ => {
            return Err(NativeFragmentDecodeError::invalid_enum(
                path.clone().field("comparison"),
                "Membership requires a known nonzero comparison",
            ));
        }
    };
    let distribution = match plan::MembershipDistribution::try_from(wire.distribution) {
        Ok(plan::MembershipDistribution::Singleton) => MembershipDistribution::Singleton,
        Ok(plan::MembershipDistribution::BroadcastBuild) => MembershipDistribution::BroadcastBuild,
        _ => {
            return Err(NativeFragmentDecodeError::invalid_enum(
                path.clone().field("distribution"),
                "Membership requires a known nonzero distribution",
            ));
        }
    };
    validate_placement(node, distribution, path.clone())?;
    let build = children.pop().expect("validated build child");
    let probe = children.pop().expect("validated probe child");
    let probe_slot = json_slot(
        &probe.output_schema,
        wire.probe_column_id,
        path.clone().field("probe_column_id"),
    )?;
    let build_slot = json_slot(
        &build.output_schema,
        wire.build_column_id,
        path.clone().field("build_column_id"),
    )?;
    let result = SlotId::new(wire.result_column_id);
    if probe.output_schema.slot(result).is_some() || build.output_schema.slot(result).is_some() {
        return Err(NativeFragmentDecodeError::inconsistent(
            path.clone().field("result_column_id"),
            "Membership result aliases an input slot",
        ));
    }
    let declared = decode_fragment_output_layout(&physical.output_columns, output_path.clone())?;
    let declared_schema = declared.chunk_schema();
    let prefix = probe.output_schema.slots();
    if declared_schema.slots().len() != prefix.len() + 1 {
        return Err(NativeFragmentDecodeError::inconsistent(
            output_path,
            "Membership must publish the complete probe plus one result",
        ));
    }
    for (index, source) in prefix.iter().enumerate() {
        let output = &declared_schema.slots()[index];
        if output.slot_id() != source.slot_id()
            || output.field().name() != source.field().name()
            || output.field().data_type() != source.field().data_type()
            || output.field().is_nullable() != source.field().is_nullable()
            || output.field_schema() != source.field_schema()
        {
            return Err(NativeFragmentDecodeError::inconsistent(
                output_path.clone().index(index),
                "Membership output changes a probe slot, field, or logical semantics",
            ));
        }
    }
    let result_field = &declared_schema.slots()[prefix.len()];
    if result_field.slot_id() != result
        || result_field.field().data_type() != &DataType::Boolean
        || !result_field.field().is_nullable()
        || result_field.field_schema().logical_type().is_some()
        || !result_field.field_schema().children().is_empty()
    {
        return Err(NativeFragmentDecodeError::inconsistent(
            output_path.clone().index(prefix.len()),
            "Membership result must be a fresh nullable Boolean",
        ));
    }
    // Preserve the input fields and semantic sidecars, including metadata and
    // unique IDs not representable by the SQL compatibility descriptor.
    let mut slots = prefix.to_vec();
    slots.push(result_field.clone());
    let output_schema = Arc::new(
        ChunkSchema::try_new(slots)
            .map_err(|detail| NativeFragmentDecodeError::inconsistent(output_path, detail))?,
    );
    Ok(NativeLoweredPlanNode {
        layout: novarocks_execution::exec::chunk::SlotLayout::for_slots(
            declared.slot_ids().iter().copied(),
        ),
        output_schema: Arc::clone(&output_schema),
        node: ExecNode {
            kind: ExecNodeKind::Membership(MembershipNode {
                probe: Box::new(probe.node),
                build: Box::new(build.node),
                node_id: node.node_id,
                spec: MembershipSpec {
                    probe: probe_slot,
                    build: build_slot,
                    result,
                    negated: wire.negated,
                    comparison,
                    distribution,
                },
                output_chunk_schema: output_schema,
            }),
        },
    })
}

fn json_slot(
    schema: &ChunkSchemaRef,
    column: u32,
    path: FieldPath,
) -> Result<SlotId, NativeFragmentDecodeError> {
    let slot = SlotId::new(column);
    let field = schema.slot(slot).ok_or_else(|| {
        NativeFragmentDecodeError::invalid_value(
            path.clone(),
            "Membership slot is not in its exact input",
        )
    })?;
    if field.field().data_type() != &DataType::Utf8
        || field.field_schema().logical_type() != Some(LogicalType::Json)
    {
        return Err(NativeFragmentDecodeError::inconsistent(
            path,
            "Membership requires proved Json over the exact Utf8 carrier",
        ));
    }
    Ok(slot)
}

fn first_exchange(node: &plan::DistributedNode) -> Option<&plan::ExchangeReceiver> {
    if let Some(plan::distributed_node::Payload::Exchange(exchange)) = node.payload.as_ref() {
        return Some(exchange);
    }
    // Traverse only a unary branch. A join's internal exchange is not proof
    // that the complete build relation has been broadcast.
    if node.children.len() == 1 {
        first_exchange(&node.children[0])
    } else {
        None
    }
}

fn validate_placement(
    node: &plan::DistributedNode,
    distribution: MembershipDistribution,
    path: FieldPath,
) -> Result<(), NativeFragmentDecodeError> {
    let build = first_exchange(&node.children[1]);
    let valid_exchange = |exchange: &plan::ExchangeReceiver| {
        exchange.partition_type == plan::PartitionType::Unpartitioned as i32
            && exchange.partition_exprs.is_empty()
            && exchange.source_fragment_id != node.fragment_id
            && matches!(
                exchange
                    .flavor
                    .as_ref()
                    .and_then(|flavor| flavor.kind.as_ref()),
                Some(plan::exchange_flavor::Kind::Distribution(true))
            )
    };
    if distribution == MembershipDistribution::BroadcastBuild && !build.is_some_and(valid_exchange)
        || distribution == MembershipDistribution::Singleton
            && build.is_some_and(|exchange| !valid_exchange(exchange))
        || distribution == MembershipDistribution::Singleton
            && first_exchange(&node.children[0]).is_some_and(|exchange| !valid_exchange(exchange))
    {
        return Err(NativeFragmentDecodeError::inconsistent(
            path.field("distribution"),
            "Membership distribution conflicts with its exact exchange boundary",
        ));
    }
    // Global SingleCopy/broadcast multiplicity is proven by PhysicalPlan cuts.
    // Native checks the supplied boundary, not a guessed backend count.
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::super::decode_node;
    use super::*;
    use crate::fragment_decode_context::NativePlanDecodeContext;
    use novarocks_execution::exec::expr::ExprArena;
    use novarocks_execution::exec::node::ExecPlanBuilder;
    use novarocks_proto_models::{common, expr};

    fn column(
        id: u32,
        name: &str,
        primitive: common::PrimitiveType,
        nullable: bool,
    ) -> common::OutputColumn {
        common::OutputColumn {
            column_id: id,
            name: name.into(),
            nullable,
            is_internal: false,
            r#type: Some(common::TypeDesc {
                nodes: vec![common::TypeNode {
                    kind: Some(common::type_node::Kind::Scalar(common::ScalarType {
                        r#type: primitive as i32,
                        ..Default::default()
                    })),
                }],
            }),
        }
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
            limit: -1,
            children,
            payload: Some(plan::distributed_node::Payload::Physical(plan::PlanNode {
                output_columns: columns,
                kind: Some(kind),
            })),
            ..Default::default()
        }
    }
    fn input(id: i32, slot: u32) -> plan::DistributedNode {
        physical(
            id,
            plan::plan_node::Kind::Values(plan::ValuesNode::default()),
            vec![column(slot, "json", common::PrimitiveType::Json, true)],
            vec![],
        )
    }
    fn fixture() -> plan::DistributedNode {
        physical(
            3,
            plan::plan_node::Kind::Membership(plan::MembershipNode {
                probe_column_id: 7,
                build_column_id: 8,
                result_column_id: 9,
                negated: true,
                comparison: plan::MembershipComparison::JsonInListV1 as i32,
                distribution: plan::MembershipDistribution::Singleton as i32,
            }),
            vec![
                column(7, "json", common::PrimitiveType::Json, true),
                column(9, "member", common::PrimitiveType::Boolean, true),
            ],
            vec![input(1, 7), input(2, 8)],
        )
    }
    fn payload(node: &mut plan::DistributedNode) -> &mut plan::PlanNode {
        let Some(plan::distributed_node::Payload::Physical(value)) = node.payload.as_mut() else {
            panic!("physical fixture")
        };
        value
    }
    fn wire(node: &mut plan::DistributedNode) -> &mut plan::MembershipNode {
        let Some(plan::plan_node::Kind::Membership(value)) = payload(node).kind.as_mut() else {
            panic!("membership fixture")
        };
        value
    }
    fn rejected(mutator: impl FnOnce(&mut plan::DistributedNode)) {
        let mut node = fixture();
        mutator(&mut node);
        decode_node(
            &node,
            &mut ExprArena::default(),
            &NativePlanDecodeContext::default(),
        )
        .expect_err("invalid membership must fail closed");
    }

    #[test]
    fn membership_exact_json_layout_survives_construction_to_local_program() {
        let mut arena = ExprArena::default();
        let decoded =
            decode_node(&fixture(), &mut arena, &NativePlanDecodeContext::default()).unwrap();
        assert_eq!(
            decoded
                .output_schema
                .slot(SlotId::new(7))
                .unwrap()
                .field_schema()
                .logical_type(),
            Some(LogicalType::Json)
        );
        let ExecNodeKind::Membership(owner) = &decoded.node.kind else {
            panic!("membership owner")
        };
        assert!(owner.spec.negated);
        let plan = ExecPlanBuilder::new(arena, decoded.node).finish().unwrap();
        let profile = plan
            .local_compile_profile(std::num::NonZeroUsize::new(1).unwrap(), None)
            .unwrap();
        let (program, bindings) = plan
            .into_local_program_and_bindings(
                profile,
                std::collections::BTreeMap::new(),
                vec![],
                novarocks_local_program::StaticSinkProgram::Noop,
            )
            .unwrap();
        assert_eq!(bindings.scan_count(), 0);
        let layout = program.nodes()[program.root().index()].output_layout();
        assert_eq!(layout.slots(), &[SlotId::new(7), SlotId::new(9)]);
        assert_eq!(
            layout.slot_metadata_at(0).unwrap().0.logical_type(),
            Some(LogicalType::Json)
        );
        assert!(layout.schema().field(1).is_nullable());
    }
    #[test]
    fn membership_preserves_original_probe_metadata_and_unique_id() {
        use novarocks_execution::exec::chunk::{Chunk, ChunkSlotSchema};
        let node = fixture();
        let mut arena = ExprArena::default();
        let mut probe = decode_node(
            &node.children[0],
            &mut arena,
            &NativePlanDecodeContext::default(),
        )
        .unwrap();
        let source = &probe.output_schema.slots()[0];
        let mut metadata = source.field().metadata().clone();
        metadata.insert("provider.field.id".into(), "42".into());
        let source = ChunkSlotSchema::try_new_with_field(
            source.slot_id(),
            source.field().clone().with_metadata(metadata),
            Some(source.field_schema().clone()),
            Some(42),
        )
        .unwrap();
        let schema = Arc::new(ChunkSchema::try_new(vec![source.clone()]).unwrap());
        let ExecNodeKind::Values(values) = &mut probe.node.kind else {
            unreachable!()
        };
        values.chunk = Chunk::try_new_with_chunk_schema(
            arrow::record_batch::RecordBatch::new_empty(schema.arrow_schema_ref()),
            Arc::clone(&schema),
        )
        .unwrap();
        probe.output_schema = schema;
        let build = decode_node(
            &node.children[1],
            &mut arena,
            &NativePlanDecodeContext::default(),
        )
        .unwrap();
        let Some(plan::distributed_node::Payload::Physical(physical)) = node.payload.as_ref()
        else {
            unreachable!()
        };
        let Some(plan::plan_node::Kind::Membership(wire)) = physical.kind.as_ref() else {
            unreachable!()
        };
        let decoded = lower(
            &node,
            physical,
            wire,
            FieldPath::root("membership"),
            FieldPath::root("output_columns"),
            vec![probe, build],
        )
        .unwrap();
        assert_eq!(decoded.output_schema.slots()[0], source);
    }
    #[test]
    fn membership_unknown_and_unspecified_modes_are_rejected() {
        for number in [0, 999] {
            rejected(|node| wire(node).comparison = number);
            rejected(|node| wire(node).distribution = number);
        }
    }
    #[test]
    fn membership_rejects_mixed_scope_alias_result_and_wrong_children() {
        rejected(|node| wire(node).probe_column_id = 0);
        rejected(|node| wire(node).build_column_id = 0);
        rejected(|node| wire(node).result_column_id = 0);
        rejected(|node| wire(node).probe_column_id = 8);
        rejected(|node| wire(node).build_column_id = 7);
        rejected(|node| wire(node).result_column_id = 7);
        rejected(|node| wire(node).result_column_id = 8);
        rejected(|node| {
            payload(node).output_columns[0].r#type = None;
        });
        rejected(|node| node.children.push(input(4, 10)));
        rejected(|node| {
            node.children.pop();
        });
        rejected(|node| node.children[1].fragment_id = 2);
    }
    #[test]
    fn membership_plain_string_and_wrong_output_contract_are_rejected() {
        rejected(|node| {
            payload(&mut node.children[0]).output_columns[0] =
                column(7, "json", common::PrimitiveType::Varchar, true)
        });
        rejected(|node| {
            payload(&mut node.children[1]).output_columns[0] =
                column(8, "json", common::PrimitiveType::Varchar, true)
        });
        rejected(|node| {
            payload(node).output_columns[0] =
                column(7, "json", common::PrimitiveType::Varchar, true)
        });
        rejected(|node| payload(node).output_columns[1].nullable = false);
        rejected(|node| {
            payload(node).output_columns[1] = column(9, "member", common::PrimitiveType::Json, true)
        });
        rejected(|node| payload(node).output_columns.swap(0, 1));
        rejected(|node| {
            payload(node).output_columns.pop();
        });
        rejected(|node| {
            payload(node).output_columns.push(column(
                10,
                "extra",
                common::PrimitiveType::Boolean,
                true,
            ));
        });
    }

    fn projected_input() -> plan::DistributedNode {
        let field = column(7, "json", common::PrimitiveType::Json, true);
        let expression = expr::Expr {
            // The expression carrier is not another root logical authority.
            r#type: column(7, "json", common::PrimitiveType::Varchar, true).r#type,
            nullable: true,
            kind: Some(expr::expr::Kind::ColumnRef(expr::ColumnRef {
                column_id: 7,
                qualifier: None,
                column: None,
            })),
        };
        physical(
            4,
            plan::plan_node::Kind::Project(plan::ProjectNode {
                items: vec![plan::ProjectItem {
                    expr: Some(expression),
                    output_name: "json".into(),
                    output_column_id: 7,
                }],
                output_qualifier: None,
                retention_admission: plan::ProjectRetentionAdmission::CheckedTask as i32,
            }),
            vec![field],
            vec![input(1, 7)],
        )
    }
    #[test]
    fn membership_identity_project_preserves_json_and_checked_retention() {
        let mut node = fixture();
        node.children[0] = projected_input();
        let decoded = decode_node(
            &node,
            &mut ExprArena::default(),
            &NativePlanDecodeContext::default(),
        )
        .unwrap();
        let ExecNodeKind::Membership(owner) = decoded.node.kind else {
            panic!("membership")
        };
        let ExecNodeKind::Project(project) = owner.probe.kind else {
            panic!("project")
        };
        assert_eq!(
            project.retention_admission,
            novarocks_execution::exec::node::project::ProjectRetentionAdmission::CheckedTask
        );
        assert_eq!(
            project
                .output_chunk_schema
                .slot(SlotId::new(7))
                .unwrap()
                .field_schema()
                .logical_type(),
            Some(LogicalType::Json)
        );
    }
    #[test]
    fn membership_computed_project_uses_unique_json_output_declaration() {
        let mut node = fixture();
        let expression = expr::Expr {
            r#type: column(7, "json", common::PrimitiveType::Varchar, true).r#type,
            nullable: true,
            kind: Some(expr::expr::Kind::Literal(expr::LiteralExpr {
                value: Some(common::LiteralValue {
                    value: Some(common::literal_value::Value::StringValue(
                        "{\"a\":1}".into(),
                    )),
                }),
            })),
        };
        node.children[0] = physical(
            4,
            plan::plan_node::Kind::Project(plan::ProjectNode {
                items: vec![plan::ProjectItem {
                    expr: Some(expression),
                    output_name: "json".into(),
                    output_column_id: 7,
                }],
                output_qualifier: None,
                retention_admission: plan::ProjectRetentionAdmission::CheckedTask as i32,
            }),
            vec![column(7, "json", common::PrimitiveType::Json, true)],
            vec![input(1, 10)],
        );
        let decoded = decode_node(
            &node,
            &mut ExprArena::default(),
            &NativePlanDecodeContext::default(),
        )
        .unwrap();
        assert_eq!(
            decoded
                .output_schema
                .slot(SlotId::new(7))
                .unwrap()
                .field_schema()
                .logical_type(),
            Some(LogicalType::Json)
        );
        payload(&mut node.children[0]).output_columns[0] =
            column(7, "json", common::PrimitiveType::Varchar, true);
        assert!(
            decode_node(
                &node,
                &mut ExprArena::default(),
                &NativePlanDecodeContext::default()
            )
            .is_err()
        );
    }
    #[test]
    fn membership_identity_project_cannot_invent_or_drop_json() {
        rejected(|node| {
            node.children[0] = projected_input();
            payload(&mut node.children[0].children[0]).output_columns[0] =
                column(7, "json", common::PrimitiveType::Varchar, true);
        });
        rejected(|node| {
            node.children[0] = projected_input();
            payload(&mut node.children[0]).output_columns[0] =
                column(7, "json", common::PrimitiveType::Varchar, true);
        });
    }
    #[test]
    fn membership_project_unknown_retention_is_rejected() {
        rejected(|node| {
            node.children[0] = projected_input();
            let Some(plan::plan_node::Kind::Project(project)) =
                payload(&mut node.children[0]).kind.as_mut()
            else {
                unreachable!()
            };
            project.retention_admission = 999;
        });
    }
    fn exchange() -> plan::DistributedNode {
        plan::DistributedNode {
            node_id: 2,
            fragment_id: 1,
            limit: -1,
            payload: Some(plan::distributed_node::Payload::Exchange(
                plan::ExchangeReceiver {
                    partition_type: plan::PartitionType::Unpartitioned as i32,
                    source_fragment_id: 2,
                    output_columns: vec![column(8, "json", common::PrimitiveType::Json, true)],
                    flavor: Some(plan::ExchangeFlavor {
                        kind: Some(plan::exchange_flavor::Kind::Distribution(true)),
                    }),
                    ..Default::default()
                },
            )),
            ..Default::default()
        }
    }
    #[test]
    fn membership_broadcast_receiver_preserves_exact_json_field() {
        let mut node = fixture();
        node.children[1] = exchange();
        wire(&mut node).distribution = plan::MembershipDistribution::BroadcastBuild as i32;
        let ctx = NativePlanDecodeContext::default().with_exchange_sender_count(
            novarocks_execution::runtime::exchange::ExchangeKey {
                finst_id_hi: 1,
                finst_id_lo: 2,
                node_id: 2,
            },
            3,
        );
        let decoded = decode_node(&node, &mut ExprArena::default(), &ctx).unwrap();
        let ExecNodeKind::Membership(owner) = decoded.node.kind else {
            panic!("membership")
        };
        let ExecNodeKind::ExchangeSource(source) = owner.build.kind else {
            panic!("exchange")
        };
        assert_eq!(
            source
                .expected_chunk_schema
                .slot(SlotId::new(8))
                .unwrap()
                .field_schema()
                .logical_type(),
            Some(LogicalType::Json)
        );
    }
    #[test]
    fn membership_broadcast_rejects_local_or_partitioned_build() {
        rejected(|node| {
            wire(node).distribution = plan::MembershipDistribution::BroadcastBuild as i32
        });
        let mut node = fixture();
        node.children[1] = exchange();
        let Some(plan::distributed_node::Payload::Exchange(exchange)) =
            node.children[1].payload.as_mut()
        else {
            unreachable!()
        };
        exchange.partition_type = plan::PartitionType::Random as i32;
        assert!(
            validate_placement(
                &node,
                MembershipDistribution::BroadcastBuild,
                FieldPath::root("membership")
            )
            .is_err()
        );
    }
}
