use std::collections::{HashMap, HashSet};

use crate::common::ids::SlotId;
use crate::exec::expr::{ExprArena, ExprId};
use crate::exec::node::change_event_expand::{
    ChangeEventExecOutputExpr, ChangeEventExecSpec, ChangeEventExpandNode,
};
use crate::exec::node::{ExecNode, ExecNodeKind};
use crate::lower::expr::lower_t_expr;
use crate::lower::layout::Layout;
use crate::lower::node::Lowered;
use crate::sql::common::ChangeStreamBranchKind;
use crate::thrift::{plan_nodes, types};

pub(crate) fn lower_change_event_expand_node(
    children: Vec<Lowered>,
    node: &plan_nodes::TPlanNode,
    out_layout: Layout,
    arena: &mut ExprArena,
    last_query_id: Option<&str>,
    fe_addr: Option<&types::TNetworkAddress>,
) -> Result<Lowered, String> {
    if children.len() != 1 {
        return Err(format!(
            "CHANGE_EVENT_EXPAND_NODE expected 1 child, got {}",
            children.len()
        ));
    }
    let child = children.into_iter().next().expect("child");
    let payload = node.change_event_expand_node.as_ref().ok_or_else(|| {
        format!(
            "CHANGE_EVENT_EXPAND_NODE node_id={} missing change_event_expand_node payload",
            node.node_id
        )
    })?;
    if payload.output_slot_ids.is_empty() {
        return Err(format!(
            "CHANGE_EVENT_EXPAND_NODE node_id={} output_slot_ids is empty",
            node.node_id
        ));
    }

    let output_set: HashSet<types::TSlotId> = payload.output_slot_ids.iter().copied().collect();
    if output_set.len() != payload.output_slot_ids.len() {
        return Err(format!(
            "CHANGE_EVENT_EXPAND_NODE node_id={} output_slot_ids contains duplicates",
            node.node_id
        ));
    }
    require_route_slot_in_outputs(
        "change_op_slot_id",
        payload.change_op_slot_id,
        &output_set,
        node.node_id,
    )?;
    if let Some(data_route_slot_id) = payload.data_route_slot_id {
        require_route_slot_in_outputs(
            "data_route_slot_id",
            data_route_slot_id,
            &output_set,
            node.node_id,
        )?;
    }

    let mut events = Vec::with_capacity(payload.events.len());
    for (event_idx, event) in payload.events.iter().enumerate() {
        let branch_kind = change_event_branch_kind_from_thrift(event.branch_kind)?;
        if matches!(
            branch_kind,
            ChangeStreamBranchKind::ReuseData | ChangeStreamBranchKind::FreshData
        ) && payload.data_route_slot_id.is_none()
        {
            return Err(format!(
                "CHANGE_EVENT_EXPAND_NODE node_id={} data branch {:?} requires data_route_slot_id",
                node.node_id, branch_kind
            ));
        }
        let predicate = event
            .predicate
            .as_ref()
            .map(|expr| lower_t_expr(expr, arena, &child.layout, last_query_id, fe_addr))
            .transpose()
            .map_err(|err| {
                format!(
                    "CHANGE_EVENT_EXPAND_NODE node_id={} failed to lower predicate for event {}: {}",
                    node.node_id, event_idx, err
                )
            })?;
        let mut assignments = Vec::with_capacity(event.assignments.len());
        for assignment in &event.assignments {
            if !output_set.contains(&assignment.output_slot_id) {
                return Err(format!(
                    "CHANGE_EVENT_EXPAND_NODE node_id={} assignment output slot {} is not in output_slot_ids",
                    node.node_id, assignment.output_slot_id
                ));
            }
            let expr = event_assignment_expr(
                assignment,
                arena,
                &child.layout,
                last_query_id,
                fe_addr,
                node.node_id,
            )?;
            assignments.push(ChangeEventExecOutputExpr {
                output_slot_id: SlotId::try_from(assignment.output_slot_id)?,
                expr,
            });
        }
        events.push(ChangeEventExecSpec {
            predicate,
            branch_kind,
            assignments,
        });
    }

    let output_slot_ids = payload
        .output_slot_ids
        .iter()
        .copied()
        .map(SlotId::try_from)
        .collect::<Result<Vec<_>, _>>()?;
    let layout = output_layout_for_slots(&out_layout, &payload.output_slot_ids)?;

    Ok(Lowered {
        node: ExecNode {
            kind: ExecNodeKind::ChangeEventExpand(ChangeEventExpandNode {
                input: Box::new(child.node),
                node_id: node.node_id,
                events,
                output_slot_ids,
                change_op_slot_id: SlotId::try_from(payload.change_op_slot_id)?,
                data_route_slot_id: payload
                    .data_route_slot_id
                    .map(SlotId::try_from)
                    .transpose()?,
            }),
        },
        layout,
    })
}

fn require_route_slot_in_outputs(
    name: &str,
    slot_id: types::TSlotId,
    output_set: &HashSet<types::TSlotId>,
    node_id: i32,
) -> Result<(), String> {
    if !output_set.contains(&slot_id) {
        return Err(format!(
            "CHANGE_EVENT_EXPAND_NODE node_id={} {} {} is not in output_slot_ids",
            node_id, name, slot_id
        ));
    }
    Ok(())
}

fn event_assignment_expr(
    assignment: &plan_nodes::TChangeEventOutputExpr,
    arena: &mut ExprArena,
    input_layout: &Layout,
    last_query_id: Option<&str>,
    fe_addr: Option<&types::TNetworkAddress>,
    node_id: i32,
) -> Result<Option<ExprId>, String> {
    assignment
        .expr
        .as_ref()
        .map(|expr| lower_t_expr(expr, arena, input_layout, last_query_id, fe_addr))
        .transpose()
        .map_err(|err| {
            format!(
                "CHANGE_EVENT_EXPAND_NODE node_id={} failed to lower assignment for output slot {}: {}",
                node_id, assignment.output_slot_id, err
            )
        })
}

fn change_event_branch_kind_from_thrift(
    kind: plan_nodes::TChangeEventBranchKind,
) -> Result<ChangeStreamBranchKind, String> {
    match kind {
        plan_nodes::TChangeEventBranchKind::DELETE_DV => Ok(ChangeStreamBranchKind::DeleteDv),
        plan_nodes::TChangeEventBranchKind::REUSE_DATA => Ok(ChangeStreamBranchKind::ReuseData),
        plan_nodes::TChangeEventBranchKind::FRESH_DATA => Ok(ChangeStreamBranchKind::FreshData),
        other => Err(format!("unknown change event branch kind: {other:?}")),
    }
}

fn output_layout_for_slots(
    out_layout: &Layout,
    output_slot_ids: &[types::TSlotId],
) -> Result<Layout, String> {
    let requested: HashSet<types::TSlotId> = output_slot_ids.iter().copied().collect();
    let mut tuple_by_slot = HashMap::with_capacity(output_slot_ids.len());
    for (tuple_id, slot_id) in &out_layout.order {
        if !requested.contains(slot_id) {
            continue;
        }
        if let Some(previous_tuple_id) = tuple_by_slot.insert(*slot_id, *tuple_id)
            && previous_tuple_id != *tuple_id
        {
            return Err(format!(
                "CHANGE_EVENT_EXPAND_NODE output slot {} appears in multiple output layout tuples: {} and {}",
                slot_id, previous_tuple_id, tuple_id
            ));
        }
    }

    let mut order = Vec::with_capacity(output_slot_ids.len());
    for slot_id in output_slot_ids {
        let tuple_id = tuple_by_slot.get(slot_id).copied().ok_or_else(|| {
            format!(
                "CHANGE_EVENT_EXPAND_NODE output slot {} is missing from output layout",
                slot_id
            )
        })?;
        order.push((tuple_id, *slot_id));
    }
    let index = order
        .iter()
        .enumerate()
        .map(|(idx, key)| (*key, idx))
        .collect();
    Ok(Layout { order, index })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::exec::expr::ExprArena;
    use crate::exec::node::{ExecNode, ExecNodeKind};
    use crate::lower::node::Lowered;
    use crate::sql::common::ChangeStreamBranchKind;
    use crate::thrift::exprs::{TExpr, TExprNode, TExprNodeType, TSlotRef};
    use crate::thrift::plan_nodes::{
        TChangeEventBranchKind, TChangeEventExpandNode, TChangeEventOutputExpr, TChangeEventSpec,
        TPlanNodeType,
    };
    use crate::thrift::types::{TTypeDesc, TTypeNode, TTypeNodeType};

    fn dummy_type() -> TTypeDesc {
        TTypeDesc {
            types: Some(vec![TTypeNode {
                type_: TTypeNodeType::SCALAR,
                scalar_type: None,
                struct_fields: None,
                is_named: None,
            }]),
        }
    }

    fn slot_expr(slot_id: i32, tuple_id: i32) -> TExpr {
        TExpr {
            nodes: vec![TExprNode {
                node_type: TExprNodeType::SLOT_REF,
                type_: dummy_type(),
                opcode: None,
                num_children: 0,
                agg_expr: None,
                bool_literal: None,
                case_expr: None,
                date_literal: None,
                float_literal: None,
                int_literal: None,
                in_predicate: None,
                is_null_pred: None,
                like_pred: None,
                literal_pred: None,
                slot_ref: Some(TSlotRef { slot_id, tuple_id }),
                string_literal: None,
                tuple_is_null_pred: None,
                info_func: None,
                decimal_literal: None,
                output_scale: 0,
                fn_call_expr: None,
                large_int_literal: None,
                output_column: None,
                output_type: None,
                vector_opcode: None,
                fn_: None,
                vararg_start_idx: None,
                child_type: None,
                vslot_ref: None,
                used_subfield_names: None,
                binary_literal: None,
                copy_flag: None,
                check_is_out_of_bounds: None,
                use_vectorized: None,
                has_nullable_child: None,
                is_nullable: None,
                child_type_desc: None,
                is_monotonic: None,
                dict_query_expr: None,
                dictionary_get_expr: None,
                is_index_only_filter: None,
                is_nondeterministic: None,
            }],
        }
    }

    fn child_lowered() -> Lowered {
        let layout = crate::lower::layout::layout_from_slot_ids(7, [10, 11, 12]);
        Lowered {
            node: ExecNode {
                kind: ExecNodeKind::Values(crate::exec::node::values::ValuesNode {
                    chunk: crate::exec::chunk::Chunk::default(),
                    node_id: 0,
                }),
            },
            layout,
        }
    }

    #[test]
    fn lower_change_event_expand_carries_route_slots_and_events() {
        let mut node =
            crate::lower::node::test_plan_node(9, TPlanNodeType::CHANGE_EVENT_EXPAND_NODE, 1);
        node.change_event_expand_node = Some(TChangeEventExpandNode {
            events: vec![
                TChangeEventSpec {
                    predicate: Some(slot_expr(10, 7)),
                    branch_kind: TChangeEventBranchKind::DELETE_DV,
                    assignments: vec![TChangeEventOutputExpr {
                        output_slot_id: 20,
                        expr: Some(slot_expr(10, 7)),
                    }],
                },
                TChangeEventSpec {
                    predicate: None,
                    branch_kind: TChangeEventBranchKind::REUSE_DATA,
                    assignments: vec![
                        TChangeEventOutputExpr {
                            output_slot_id: 20,
                            expr: Some(slot_expr(11, 7)),
                        },
                        TChangeEventOutputExpr {
                            output_slot_id: 21,
                            expr: None,
                        },
                    ],
                },
            ],
            output_slot_ids: vec![20, 21, 22, 30, 31],
            change_op_slot_id: 30,
            data_route_slot_id: Some(31),
        });

        let out_layout = crate::lower::layout::layout_from_slot_ids(8, [20, 21, 22, 30, 31]);
        let mut arena = ExprArena::default();
        let lowered = lower_change_event_expand_node(
            vec![child_lowered()],
            &node,
            out_layout,
            &mut arena,
            None,
            None,
        )
        .expect("lower change event expand");

        assert_eq!(
            lowered.layout.order,
            vec![(8, 20), (8, 21), (8, 22), (8, 30), (8, 31)]
        );
        match lowered.node.kind {
            ExecNodeKind::ChangeEventExpand(expand) => {
                assert_eq!(expand.node_id, 9);
                assert_eq!(
                    expand.output_slot_ids,
                    vec![
                        crate::common::ids::SlotId::new(20),
                        crate::common::ids::SlotId::new(21),
                        crate::common::ids::SlotId::new(22),
                        crate::common::ids::SlotId::new(30),
                        crate::common::ids::SlotId::new(31),
                    ]
                );
                assert_eq!(
                    expand.change_op_slot_id,
                    crate::common::ids::SlotId::new(30)
                );
                assert_eq!(
                    expand.data_route_slot_id,
                    Some(crate::common::ids::SlotId::new(31))
                );
                assert_eq!(expand.events.len(), 2);
                assert_eq!(
                    expand.events[0].branch_kind,
                    ChangeStreamBranchKind::DeleteDv
                );
                assert_eq!(
                    expand.events[1].branch_kind,
                    ChangeStreamBranchKind::ReuseData
                );
                assert!(expand.events[0].predicate.is_some());
                assert!(expand.events[1].predicate.is_none());
                assert_eq!(expand.events[0].assignments.len(), 1);
                assert_eq!(expand.events[1].assignments.len(), 2);
                assert!(expand.events[1].assignments[1].expr.is_none());
            }
            other => panic!("expected ChangeEventExpand exec node, got {other:?}"),
        }
    }

    #[test]
    fn lower_change_event_expand_rejects_missing_output_layout_slot() {
        let mut node =
            crate::lower::node::test_plan_node(10, TPlanNodeType::CHANGE_EVENT_EXPAND_NODE, 1);
        node.change_event_expand_node = Some(TChangeEventExpandNode {
            events: vec![TChangeEventSpec {
                predicate: None,
                branch_kind: TChangeEventBranchKind::FRESH_DATA,
                assignments: vec![TChangeEventOutputExpr {
                    output_slot_id: 20,
                    expr: Some(slot_expr(10, 7)),
                }],
            }],
            output_slot_ids: vec![20, 21, 30, 31],
            change_op_slot_id: 30,
            data_route_slot_id: Some(31),
        });

        let out_layout = crate::lower::layout::layout_from_slot_ids(8, [20, 30, 31]);
        let mut arena = ExprArena::default();
        let err = lower_change_event_expand_node(
            vec![child_lowered()],
            &node,
            out_layout,
            &mut arena,
            None,
            None,
        )
        .expect_err("missing output layout slot must fail");

        assert!(
            err.contains("output slot 21") && err.contains("output layout"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn lower_change_event_expand_rejects_change_op_slot_missing_from_outputs() {
        let mut node =
            crate::lower::node::test_plan_node(11, TPlanNodeType::CHANGE_EVENT_EXPAND_NODE, 1);
        node.change_event_expand_node = Some(TChangeEventExpandNode {
            events: vec![TChangeEventSpec {
                predicate: None,
                branch_kind: TChangeEventBranchKind::DELETE_DV,
                assignments: vec![TChangeEventOutputExpr {
                    output_slot_id: 20,
                    expr: Some(slot_expr(10, 7)),
                }],
            }],
            output_slot_ids: vec![20],
            change_op_slot_id: 30,
            data_route_slot_id: None,
        });

        let out_layout = crate::lower::layout::layout_from_slot_ids(8, [20]);
        let mut arena = ExprArena::default();
        let err = lower_change_event_expand_node(
            vec![child_lowered()],
            &node,
            out_layout,
            &mut arena,
            None,
            None,
        )
        .expect_err("missing change op output slot must fail");

        assert!(
            err.contains("change_op_slot_id") && err.contains("output_slot_ids"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn lower_change_event_expand_rejects_data_route_slot_missing_from_outputs() {
        let mut node =
            crate::lower::node::test_plan_node(12, TPlanNodeType::CHANGE_EVENT_EXPAND_NODE, 1);
        node.change_event_expand_node = Some(TChangeEventExpandNode {
            events: vec![TChangeEventSpec {
                predicate: None,
                branch_kind: TChangeEventBranchKind::REUSE_DATA,
                assignments: vec![TChangeEventOutputExpr {
                    output_slot_id: 20,
                    expr: Some(slot_expr(10, 7)),
                }],
            }],
            output_slot_ids: vec![20, 30],
            change_op_slot_id: 30,
            data_route_slot_id: Some(31),
        });

        let out_layout = crate::lower::layout::layout_from_slot_ids(8, [20, 30]);
        let mut arena = ExprArena::default();
        let err = lower_change_event_expand_node(
            vec![child_lowered()],
            &node,
            out_layout,
            &mut arena,
            None,
            None,
        )
        .expect_err("missing data route output slot must fail");

        assert!(
            err.contains("data_route_slot_id") && err.contains("output_slot_ids"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn lower_change_event_expand_rejects_change_op_slot_missing_from_output_layout() {
        let mut node =
            crate::lower::node::test_plan_node(13, TPlanNodeType::CHANGE_EVENT_EXPAND_NODE, 1);
        node.change_event_expand_node = Some(TChangeEventExpandNode {
            events: vec![TChangeEventSpec {
                predicate: None,
                branch_kind: TChangeEventBranchKind::DELETE_DV,
                assignments: vec![TChangeEventOutputExpr {
                    output_slot_id: 20,
                    expr: Some(slot_expr(10, 7)),
                }],
            }],
            output_slot_ids: vec![20, 30],
            change_op_slot_id: 30,
            data_route_slot_id: None,
        });

        let out_layout = crate::lower::layout::layout_from_slot_ids(8, [20]);
        let mut arena = ExprArena::default();
        let err = lower_change_event_expand_node(
            vec![child_lowered()],
            &node,
            out_layout,
            &mut arena,
            None,
            None,
        )
        .expect_err("missing change op output layout slot must fail");

        assert!(
            err.contains("output slot 30") && err.contains("output layout"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn lower_change_event_expand_rejects_data_route_slot_missing_from_output_layout() {
        let mut node =
            crate::lower::node::test_plan_node(14, TPlanNodeType::CHANGE_EVENT_EXPAND_NODE, 1);
        node.change_event_expand_node = Some(TChangeEventExpandNode {
            events: vec![TChangeEventSpec {
                predicate: None,
                branch_kind: TChangeEventBranchKind::REUSE_DATA,
                assignments: vec![TChangeEventOutputExpr {
                    output_slot_id: 20,
                    expr: Some(slot_expr(10, 7)),
                }],
            }],
            output_slot_ids: vec![20, 30, 31],
            change_op_slot_id: 30,
            data_route_slot_id: Some(31),
        });

        let out_layout = crate::lower::layout::layout_from_slot_ids(8, [20, 30]);
        let mut arena = ExprArena::default();
        let err = lower_change_event_expand_node(
            vec![child_lowered()],
            &node,
            out_layout,
            &mut arena,
            None,
            None,
        )
        .expect_err("missing data route output layout slot must fail");

        assert!(
            err.contains("output slot 31") && err.contains("output layout"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn lower_change_event_expand_rejects_data_branch_without_route_slot() {
        let mut node =
            crate::lower::node::test_plan_node(15, TPlanNodeType::CHANGE_EVENT_EXPAND_NODE, 1);
        node.change_event_expand_node = Some(TChangeEventExpandNode {
            events: vec![TChangeEventSpec {
                predicate: None,
                branch_kind: TChangeEventBranchKind::FRESH_DATA,
                assignments: vec![TChangeEventOutputExpr {
                    output_slot_id: 20,
                    expr: Some(slot_expr(10, 7)),
                }],
            }],
            output_slot_ids: vec![20, 30],
            change_op_slot_id: 30,
            data_route_slot_id: None,
        });

        let out_layout = crate::lower::layout::layout_from_slot_ids(8, [20, 30]);
        let mut arena = ExprArena::default();
        let err = lower_change_event_expand_node(
            vec![child_lowered()],
            &node,
            out_layout,
            &mut arena,
            None,
            None,
        )
        .expect_err("data branch without route slot must fail");

        assert!(
            err.contains("data_route_slot_id") && err.contains("data branch"),
            "unexpected error: {err}"
        );
    }
}
