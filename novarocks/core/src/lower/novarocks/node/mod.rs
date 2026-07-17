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

//! Proto node lowering placeholder.

mod aggregate;
mod assert;
mod change_event_expand;
mod common;
mod exchange;
mod filter;
mod generate_series;
mod hash_join;
mod limit;
mod nestloop_join;
mod project;
mod redistribute;
mod repeat;
mod set_op;
mod sort;
mod table_function;
mod topn;
mod values;
mod window;

use std::collections::HashMap;
use std::sync::Arc;

use self::common::*;

use super::layout::Layout;
use crate::common::types::UniqueId;
use crate::exec::chunk::ChunkSchemaRef;
use crate::exec::expr::ExprArena;
use crate::exec::node::limit::LimitNode;
use crate::exec::node::{ExecNode, ExecNodeKind};
use crate::proto::{novarocks, plan};
use crate::runtime::exchange::ExchangeKey;
use crate::runtime::query_options::QueryOptions;

#[derive(Clone, Debug)]
pub(crate) struct LoweredNode {
    pub node: ExecNode,
    pub layout: Layout,
    pub output_schema: ChunkSchemaRef,
}

#[derive(Clone, Debug, Default)]
pub(crate) struct NodeLoweringContext {
    exchange_sender_counts: HashMap<ExchangeKey, usize>,
    scan_ranges: HashMap<i32, Vec<novarocks::ScanRangeParams>>,
    query_options: Option<QueryOptions>,
    connectors: Option<Arc<crate::connector::ConnectorRegistry>>,
    query_id: Option<UniqueId>,
    fragment_instance_hi: i64,
    fragment_instance_lo: i64,
}

impl NodeLoweringContext {
    #[allow(dead_code)]
    pub(crate) fn with_fragment_instance_id(mut self, hi: i64, lo: i64) -> Self {
        self.fragment_instance_hi = hi;
        self.fragment_instance_lo = lo;
        self
    }

    #[allow(dead_code)]
    pub(crate) fn with_exchange_sender_count(mut self, key: ExchangeKey, count: usize) -> Self {
        self.exchange_sender_counts.insert(key, count);
        self
    }

    #[allow(dead_code)]
    pub(crate) fn with_scan_ranges(
        mut self,
        node_id: i32,
        ranges: Vec<novarocks::ScanRangeParams>,
    ) -> Self {
        self.scan_ranges.insert(node_id, ranges);
        self
    }

    #[allow(dead_code)]
    pub(crate) fn with_query_options(mut self, query_options: Option<QueryOptions>) -> Self {
        self.query_options = query_options;
        self
    }

    #[allow(dead_code)]
    pub(crate) fn with_connector_registry(
        mut self,
        connectors: Arc<crate::connector::ConnectorRegistry>,
    ) -> Self {
        self.connectors = Some(connectors);
        self
    }

    #[allow(dead_code)]
    pub(crate) fn with_query_id(mut self, query_id: UniqueId) -> Self {
        self.query_id = Some(query_id);
        self
    }

    pub(crate) fn scan_ranges(
        &self,
        node_id: i32,
    ) -> Result<&[novarocks::ScanRangeParams], String> {
        self.scan_ranges
            .get(&node_id)
            .map(Vec::as_slice)
            .ok_or_else(|| format!("native ScanNode node_id={node_id} missing scan ranges"))
    }

    pub(crate) fn query_options(&self) -> Option<&QueryOptions> {
        self.query_options.as_ref()
    }

    #[cfg(feature = "compat")]
    pub(crate) fn query_id(&self) -> Option<UniqueId> {
        self.query_id
    }

    pub(crate) fn connectors(&self) -> Result<&crate::connector::ConnectorRegistry, String> {
        self.connectors.as_deref().ok_or_else(|| {
            "native ScanNode requires ConnectorRegistry in NodeLoweringContext".to_string()
        })
    }

    fn exchange_key(&self, node_id: i32) -> ExchangeKey {
        ExchangeKey {
            finst_id_hi: self.fragment_instance_hi,
            finst_id_lo: self.fragment_instance_lo,
            node_id,
        }
    }
}

#[allow(dead_code)]
pub(crate) fn lower_proto_node(
    node: &plan::DistributedNode,
    arena: &mut ExprArena,
    ctx: &NodeLoweringContext,
) -> Result<LoweredNode, String> {
    let children = node
        .children
        .iter()
        .map(|child| lower_proto_node(child, arena, ctx))
        .collect::<Result<Vec<_>, _>>()?;

    let payload = node
        .payload
        .as_ref()
        .ok_or_else(|| format!("DistributedNode node_id={} payload missing", node.node_id))?;
    let lowered = match payload {
        plan::distributed_node::Payload::Physical(physical) => {
            lower_physical_node(node, physical, children, arena, ctx)
        }
        plan::distributed_node::Payload::Exchange(exchange) => {
            exchange::lower_exchange_receiver(node, exchange, children, arena, ctx)
        }
    }?;
    apply_distributed_limit_if_needed(node, lowered)
}

fn apply_distributed_limit_if_needed(
    node: &plan::DistributedNode,
    mut lowered: LoweredNode,
) -> Result<LoweredNode, String> {
    let Some(limit) = parse_distributed_limit(node.limit, "DistributedNode.limit")? else {
        return Ok(lowered);
    };
    if matches!(
        lowered.node.kind,
        ExecNodeKind::Limit(_) | ExecNodeKind::Sort(_)
    ) {
        return Ok(lowered);
    }
    lowered.node = ExecNode {
        kind: ExecNodeKind::Limit(LimitNode {
            input: Box::new(lowered.node),
            node_id: node.node_id,
            limit: Some(limit),
            offset: 0,
        }),
    };
    Ok(lowered)
}

fn lower_physical_node(
    node: &plan::DistributedNode,
    physical: &plan::PlanNode,
    children: Vec<LoweredNode>,
    arena: &mut ExprArena,
    ctx: &NodeLoweringContext,
) -> Result<LoweredNode, String> {
    let kind = physical
        .kind
        .as_ref()
        .ok_or_else(|| format!("PlanNode node_id={} kind missing", node.node_id))?;
    match kind {
        plan::plan_node::Kind::Values(values) => {
            values::lower_values_node(node, physical, values, children, arena)
        }
        plan::plan_node::Kind::Project(project) => {
            project::lower_project_node(node, project, children, arena)
        }
        plan::plan_node::Kind::Filter(filter) => {
            filter::lower_filter_node(node, filter, children, arena)
        }
        plan::plan_node::Kind::Limit(limit) => limit::lower_limit_node(node, limit, children),
        plan::plan_node::Kind::Sort(sort) => {
            sort::lower_sort_node(node, physical, sort, children, arena)
        }
        plan::plan_node::Kind::Topn(topn) => topn::lower_topn_node(node, topn, children, arena),
        plan::plan_node::Kind::SetOp(set_op) => {
            set_op::lower_set_op_node(node, physical, set_op, children, arena)
        }
        plan::plan_node::Kind::AssertOneRow(assert) => {
            assert::lower_assert_one_row_node(node, assert, children)
        }
        plan::plan_node::Kind::Scan(scan) => {
            super::scan::lower_scan_node(node, physical, scan, ctx, arena)
        }
        plan::plan_node::Kind::HashAggregate(aggregate) => {
            aggregate::lower_hash_aggregate_node(node, physical, aggregate, children, arena)
        }
        plan::plan_node::Kind::HashJoin(join) => {
            hash_join::lower_hash_join_node(node, physical, join, children, arena)
        }
        plan::plan_node::Kind::NestLoopJoin(join) => {
            nestloop_join::lower_nest_loop_join_node(node, physical, join, children, arena)
        }
        plan::plan_node::Kind::Window(window) => {
            window::lower_window_node(node, physical, window, children, arena)
        }
        plan::plan_node::Kind::Repeat(repeat) => repeat::lower_repeat_node(node, repeat, children),
        plan::plan_node::Kind::GenerateSeries(generate_series) => {
            generate_series::lower_generate_series_node(node, generate_series, children, arena)
        }
        plan::plan_node::Kind::TableFunction(table_function) => {
            table_function::lower_table_function_node(node, table_function, children, arena)
        }
        plan::plan_node::Kind::Decode(_) => unsupported("Decode"),
        plan::plan_node::Kind::ChangeEventExpand(expand) => {
            change_event_expand::lower_change_event_expand_node(
                node, physical, expand, children, arena,
            )
        }
        plan::plan_node::Kind::CteAnchor(_) => unsupported("CTEAnchor"),
        plan::plan_node::Kind::CteProduce(_) => unsupported("CTEProduce"),
        plan::plan_node::Kind::CteConsume(_) => unsupported("CTEConsume"),
        plan::plan_node::Kind::Redistribute(redistribute) => {
            redistribute::lower_redistribute_node(physical, redistribute, children, arena)
        }
    }
}

#[cfg(test)]
mod tests {
    use arrow::datatypes::DataType;

    use super::{NodeLoweringContext, lower_proto_node};
    use crate::common::ids::SlotId;
    use crate::exec::expr::ExprArena;
    use crate::exec::node::ExecNodeKind;
    use crate::exec::node::assert::{AssertNumRowsMode, Assertion};
    use crate::exec::node::set_op::SetOpKind;
    use crate::proto::{common, expr, plan};
    use crate::types::native_proto::encode_type;

    pub(super) fn type_desc(data_type: &DataType) -> common::TypeDesc {
        encode_type(data_type).expect("encode type")
    }

    pub(super) fn output_column_with_nullable(
        column_id: u32,
        name: &str,
        data_type: DataType,
        nullable: bool,
    ) -> common::OutputColumn {
        common::OutputColumn {
            column_id,
            name: name.to_string(),
            r#type: Some(type_desc(&data_type)),
            nullable,
            is_internal: false,
        }
    }

    pub(super) fn output_column(
        column_id: u32,
        name: &str,
        data_type: DataType,
    ) -> common::OutputColumn {
        output_column_with_nullable(column_id, name, data_type, true)
    }

    fn int_literal(value: i64) -> expr::Expr {
        expr::Expr {
            r#type: Some(type_desc(&DataType::Int64)),
            nullable: false,
            kind: Some(expr::expr::Kind::Literal(expr::LiteralExpr {
                value: Some(common::LiteralValue {
                    value: Some(common::literal_value::Value::IntValue(value)),
                }),
            })),
        }
    }

    pub(super) fn string_literal(value: &str) -> expr::Expr {
        expr::Expr {
            r#type: Some(type_desc(&DataType::Utf8)),
            nullable: false,
            kind: Some(expr::expr::Kind::Literal(expr::LiteralExpr {
                value: Some(common::LiteralValue {
                    value: Some(common::literal_value::Value::StringValue(value.to_string())),
                }),
            })),
        }
    }

    pub(super) fn bool_literal(value: bool) -> expr::Expr {
        expr::Expr {
            r#type: Some(type_desc(&DataType::Boolean)),
            nullable: false,
            kind: Some(expr::expr::Kind::Literal(expr::LiteralExpr {
                value: Some(common::LiteralValue {
                    value: Some(common::literal_value::Value::BoolValue(value)),
                }),
            })),
        }
    }

    fn null_literal(data_type: DataType) -> expr::Expr {
        expr::Expr {
            r#type: Some(type_desc(&data_type)),
            nullable: true,
            kind: Some(expr::expr::Kind::Literal(expr::LiteralExpr {
                value: Some(common::LiteralValue {
                    value: Some(common::literal_value::Value::NullValue(true)),
                }),
            })),
        }
    }

    pub(super) fn column_ref(column_id: u32, data_type: DataType) -> expr::Expr {
        expr::Expr {
            r#type: Some(type_desc(&data_type)),
            nullable: true,
            kind: Some(expr::expr::Kind::ColumnRef(expr::ColumnRef {
                column_id,
                qualifier: None,
                column: None,
            })),
        }
    }

    pub(super) fn sort_item(column_id: u32) -> expr::SortItem {
        expr::SortItem {
            expr: Some(column_ref(column_id, DataType::Int64)),
            asc: true,
            nulls_first: false,
        }
    }

    pub(super) fn physical_node(
        node_id: i32,
        kind: plan::plan_node::Kind,
        output_columns: Vec<common::OutputColumn>,
        children: Vec<plan::DistributedNode>,
    ) -> plan::DistributedNode {
        plan::DistributedNode {
            node_id,
            fragment_id: 1,
            tuple_ids: Vec::new(),
            nullable_tuple_ids: Vec::new(),
            limit: -1,
            build_runtime_filters: Vec::new(),
            probe_runtime_filters: Vec::new(),
            children,
            payload: Some(plan::distributed_node::Payload::Physical(plan::PlanNode {
                output_columns,
                kind: Some(kind),
            })),
        }
    }

    pub(super) fn values_node(node_id: i32) -> plan::DistributedNode {
        let columns = vec![
            output_column(1, "id", DataType::Int64),
            output_column(2, "name", DataType::Utf8),
        ];
        physical_node(
            node_id,
            plan::plan_node::Kind::Values(plan::ValuesNode {
                rows: vec![
                    plan::ExprList {
                        values: vec![int_literal(10), string_literal("alice")],
                    },
                    plan::ExprList {
                        values: vec![int_literal(20), string_literal("bob")],
                    },
                ],
                columns: columns.clone(),
            }),
            columns,
            Vec::new(),
        )
    }

    pub(super) fn one_col_values_node(node_id: i32) -> plan::DistributedNode {
        one_col_values_node_with(node_id, 1, "id", 10)
    }

    pub(super) fn one_col_values_node_with(
        node_id: i32,
        column_id: u32,
        name: &str,
        value: i64,
    ) -> plan::DistributedNode {
        one_col_values_node_with_nullable(node_id, column_id, name, value, true)
    }

    pub(super) fn one_col_values_node_with_nullable(
        node_id: i32,
        column_id: u32,
        name: &str,
        value: i64,
        nullable: bool,
    ) -> plan::DistributedNode {
        let columns = vec![output_column_with_nullable(
            column_id,
            name,
            DataType::Int64,
            nullable,
        )];
        physical_node(
            node_id,
            plan::plan_node::Kind::Values(plan::ValuesNode {
                rows: vec![plan::ExprList {
                    values: vec![int_literal(value)],
                }],
                columns: columns.clone(),
            }),
            columns,
            Vec::new(),
        )
    }

    pub(super) fn two_col_values_node(node_id: i32) -> plan::DistributedNode {
        let columns = vec![
            output_column(1, "a", DataType::Int64),
            output_column(2, "b", DataType::Int64),
        ];
        physical_node(
            node_id,
            plan::plan_node::Kind::Values(plan::ValuesNode {
                rows: vec![plan::ExprList {
                    values: vec![int_literal(10), int_literal(20)],
                }],
                columns: columns.clone(),
            }),
            columns,
            Vec::new(),
        )
    }

    pub(super) fn three_col_values_node(node_id: i32) -> plan::DistributedNode {
        let columns = vec![
            output_column(1, "a", DataType::Int64),
            output_column(2, "b", DataType::Int64),
            output_column(3, "c", DataType::Int64),
        ];
        physical_node(
            node_id,
            plan::plan_node::Kind::Values(plan::ValuesNode {
                rows: vec![plan::ExprList {
                    values: vec![int_literal(10), int_literal(20), int_literal(30)],
                }],
                columns: columns.clone(),
            }),
            columns,
            Vec::new(),
        )
    }

    pub(super) fn lower(node: &plan::DistributedNode) -> super::LoweredNode {
        let mut arena = ExprArena::default();
        lower_proto_node(node, &mut arena, &NodeLoweringContext::default()).expect("lower node")
    }

    #[test]
    fn rejects_scan_without_context_and_union_distinct() {
        let scan = physical_node(
            50,
            plan::plan_node::Kind::Scan(plan::ScanNode::default()),
            Vec::new(),
            Vec::new(),
        );
        let mut arena = ExprArena::default();
        let err = lower_proto_node(&scan, &mut arena, &NodeLoweringContext::default()).unwrap_err();
        assert!(err.contains("Scan"));
        assert!(err.contains("table missing"));

        let union_distinct = physical_node(
            60,
            plan::plan_node::Kind::SetOp(plan::SetOpNode {
                kind: plan::PlanSetOpKind::UnionDistinct as i32,
                output_columns: vec![output_column(1, "id", DataType::Int64)],
                child_output_columns: Vec::new(),
            }),
            Vec::new(),
            vec![one_col_values_node(10), one_col_values_node(11)],
        );
        let err = lower_proto_node(&union_distinct, &mut arena, &NodeLoweringContext::default())
            .unwrap_err();
        assert!(err.contains("UnionDistinct"));
        assert!(err.contains("not implemented"));
    }

    #[test]
    fn lowers_union_all_intersect_except_and_assert_one_row() {
        let output_columns = vec![output_column(1, "id", DataType::Int64)];
        let union_all = physical_node(
            60,
            plan::plan_node::Kind::SetOp(plan::SetOpNode {
                kind: plan::PlanSetOpKind::UnionAll as i32,
                output_columns: output_columns.clone(),
                child_output_columns: Vec::new(),
            }),
            output_columns.clone(),
            vec![one_col_values_node(10), one_col_values_node(11)],
        );
        let lowered = lower(&union_all);
        assert!(matches!(lowered.node.kind, ExecNodeKind::UnionAll(_)));

        for (kind, expected) in [
            (plan::PlanSetOpKind::Intersect, SetOpKind::Intersect),
            (plan::PlanSetOpKind::Except, SetOpKind::Except),
        ] {
            let set_op = physical_node(
                61,
                plan::plan_node::Kind::SetOp(plan::SetOpNode {
                    kind: kind as i32,
                    output_columns: output_columns.clone(),
                    child_output_columns: Vec::new(),
                }),
                output_columns.clone(),
                vec![one_col_values_node(10), one_col_values_node(11)],
            );
            let lowered = lower(&set_op);
            let ExecNodeKind::SetOp(set_op) = lowered.node.kind else {
                panic!("expected SetOp");
            };
            assert_eq!(
                std::mem::discriminant(&set_op.kind),
                std::mem::discriminant(&expected)
            );
            assert_eq!(set_op.output_chunk_schema.slot_ids(), &[SlotId::new(1)]);
        }

        let assert_one_row = physical_node(
            70,
            plan::plan_node::Kind::AssertOneRow(plan::AssertOneRowNode {
                subquery_text: "select id from t".to_string(),
                desired_num_rows: Some(1),
                assertion: plan::RowCountAssertion::Le as i32,
                group_key_column_ids: Vec::new(),
                group_key_labels: Vec::new(),
                keyed_message_prefix: None,
            }),
            Vec::new(),
            vec![one_col_values_node(10)],
        );
        let lowered = lower(&assert_one_row);
        let ExecNodeKind::AssertNumRows(assert) = lowered.node.kind else {
            panic!("expected AssertNumRows");
        };
        match assert.mode {
            AssertNumRowsMode::Global {
                desired_num_rows,
                assertion,
                subquery_string,
            } => {
                assert_eq!(desired_num_rows, Some(1));
                assert!(matches!(assertion, Assertion::Le));
                assert_eq!(subquery_string.as_deref(), Some("select id from t"));
            }
            AssertNumRowsMode::PerKeyAtMostOne { .. } => panic!("expected global assert"),
        }
    }

    #[test]
    fn lowers_hash_aggregate_and_join_shapes() {
        let output_columns = vec![
            output_column(1, "id", DataType::Int64),
            output_column(2, "cnt", DataType::Int64),
        ];
        let aggregate = physical_node(
            20,
            plan::plan_node::Kind::HashAggregate(plan::HashAggregateNode {
                mode: plan::AggMode::Single as i32,
                group_by: vec![column_ref(1, DataType::Int64)],
                aggregates: vec![plan::PlanAggregateCall {
                    name: "count".to_string(),
                    args: Vec::new(),
                    distinct: false,
                    result_type: Some(type_desc(&DataType::Int64)),
                    order_by: Vec::new(),
                    output_column_id: 2,
                }],
                is_merge: vec![false],
                output_layout: Some(plan::AggregateOutputLayout {
                    group_key_columns: vec![output_columns[0].clone()],
                    aggregate_columns: vec![output_columns[1].clone()],
                }),
                output_columns: output_columns.clone(),
            }),
            output_columns,
            vec![one_col_values_node(10)],
        );
        let lowered = lower(&aggregate);
        let ExecNodeKind::Aggregate(aggregate) = lowered.node.kind else {
            panic!("expected Aggregate");
        };
        assert_eq!(aggregate.node_id, 20);
        assert_eq!(aggregate.group_by.len(), 1);
        assert_eq!(aggregate.functions.len(), 1);
        assert!(aggregate.need_finalize);
        assert_eq!(
            aggregate.output_chunk_schema.slot_ids(),
            &[SlotId::new(1), SlotId::new(2)]
        );

        let join = physical_node(
            30,
            plan::plan_node::Kind::HashJoin(plan::HashJoinNode {
                join_type: plan::JoinKind::Inner as i32,
                eq_conditions: vec![plan::HashJoinEqCondition {
                    left: Some(column_ref(1, DataType::Int64)),
                    right: Some(column_ref(2, DataType::Int64)),
                    null_safe: false,
                }],
                other_condition: None,
                distribution: plan::JoinDistribution::Broadcast as i32,
                execution_mode: None,
                build_runtime_filters: Vec::new(),
            }),
            Vec::new(),
            vec![
                one_col_values_node_with(10, 1, "lhs", 10),
                one_col_values_node_with(11, 2, "rhs", 10),
            ],
        );
        let lowered = lower(&join);
        let ExecNodeKind::Join(join) = lowered.node.kind else {
            panic!("expected Join");
        };
        assert_eq!(join.probe_keys.len(), 1);
        assert_eq!(join.build_keys.len(), 1);
        assert_eq!(join.eq_null_safe, vec![false]);
        assert_eq!(
            join.join_scope_chunk_schema.slot_ids(),
            &[SlotId::new(1), SlotId::new(2)]
        );
        assert!(matches!(
            join.join_type,
            crate::exec::node::join::JoinType::Inner
        ));
    }

    #[test]
    fn lowers_repeat_change_event_and_redistribute_shapes() {
        let repeat = physical_node(
            20,
            plan::plan_node::Kind::Repeat(plan::RepeatNode {
                repeat_column_ref_list: Vec::new(),
                repeat_column_ref_ids: vec![
                    plan::UInt32List { values: vec![1] },
                    plan::UInt32List { values: Vec::new() },
                ],
                grouping_ids: vec![0, 1],
                all_rollup_columns: vec!["id".to_string()],
                all_rollup_column_ids: vec![1],
                grouping_key_aliases: Vec::new(),
                grouping_fn_args: Vec::new(),
                grouping_fn_arg_ids: vec![plan::UInt32List { values: vec![1] }],
                grouping_fn_ids: vec![plan::NamedUInt32 {
                    name: "__grouping_fn_0".to_string(),
                    value: 9,
                }],
                virtual_tuple_id: Some(7),
            }),
            Vec::new(),
            vec![one_col_values_node(10)],
        );
        let lowered = lower(&repeat);
        let ExecNodeKind::Repeat(repeat) = lowered.node.kind else {
            panic!("expected Repeat");
        };
        assert_eq!(repeat.repeat_times, 2);
        assert_eq!(repeat.null_slot_ids, vec![vec![], vec![SlotId::new(1)]]);
        assert_eq!(repeat.grouping_slot_ids, vec![SlotId::new(9)]);
        assert_eq!(repeat.grouping_list, vec![vec![0, 1]]);
        assert_eq!(lowered.layout.order(), &[SlotId::new(1), SlotId::new(9)]);
        assert_eq!(
            lowered.output_schema.slot_ids(),
            &[SlotId::new(1), SlotId::new(9)]
        );

        let change_event = physical_node(
            30,
            plan::plan_node::Kind::ChangeEventExpand(plan::ChangeEventExpandNode {
                events: vec![plan::DistributedChangeEventSpec {
                    predicate: None,
                    branch_kind: plan::ChangeStreamBranchKind::DeleteDv as i32,
                    assignments: vec![plan::DistributedChangeEventOutputExpr {
                        output_column_id: 2,
                        expr: None,
                    }],
                }],
                output_columns: vec![
                    output_column(1, "id", DataType::Int64),
                    output_column(2, "op", DataType::Int8),
                ],
                change_op_column_id: 2,
                data_route_column_id: None,
            }),
            Vec::new(),
            vec![one_col_values_node(10)],
        );
        let lowered = lower(&change_event);
        let ExecNodeKind::ChangeEventExpand(change_event) = lowered.node.kind else {
            panic!("expected ChangeEventExpand");
        };
        assert_eq!(
            change_event.output_slot_ids,
            vec![SlotId::new(1), SlotId::new(2)]
        );
        assert_eq!(change_event.change_op_slot_id, SlotId::new(2));
        assert_eq!(change_event.events.len(), 1);

        let redistribute = physical_node(
            40,
            plan::plan_node::Kind::Redistribute(plan::RedistributeNode {
                mode: Some(plan::RedistributeMode {
                    mode: Some(plan::redistribute_mode::Mode::Gather(true)),
                }),
                partition_exprs: Vec::new(),
                output_columns: vec![output_column(1, "id", DataType::Int64)],
            }),
            Vec::new(),
            vec![one_col_values_node(10)],
        );
        let lowered = lower(&redistribute);
        assert!(matches!(lowered.node.kind, ExecNodeKind::Values(_)));
        assert_eq!(lowered.layout.order(), &[SlotId::new(1)]);
    }
}
