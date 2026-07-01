#![allow(dead_code)]

use crate::sql::codegen::scalar_materialize::{
    materialize, materialize_aggregate_calls, materialize_exprs, materialize_project_items,
    materialize_sort_keys, materialize_window_exprs,
};
use crate::sql::optimizer::operator::{Operator, PhysicalDistributionOp};
use crate::sql::optimizer::physical_tree::{JoinExecutionDistribution, OptimizerPhysicalNode};
use crate::sql::optimizer::property::DistributionSpec;
use crate::sql::optimizer::scalar::ScalarArena;
use crate::sql::optimizer::statistics::{ColumnStatistic, Confidence, DistinctValueCount};
use crate::sql::planner::plan::*;
use crate::sql::planner::{
    JoinExecutionMode, PhysicalPlanKind, PhysicalPlanNode, PhysicalPlanStats,
    PlannerBroadcastDecision, PlannerColumnStatistic, PlannerConfidence, PlannerCostEstimate,
    RedistributeMode, RedistributeNode, RuntimeFilterBuildIntent, RuntimeFilterProbeIntent,
};

struct BridgeCtx<'a> {
    scalars: &'a ScalarArena,
}

impl BridgeCtx<'_> {
    fn convert_node(&self, node: &OptimizerPhysicalNode) -> Result<PhysicalPlanNode, String> {
        if node.op.is_logical() {
            return Err(format!(
                "Bridge 2a expected a physical operator, got logical operator {:?}",
                node.op
            ));
        }

        let children = node
            .children
            .iter()
            .map(|child| self.convert_node(child))
            .collect::<Result<Vec<_>, _>>()?;
        let kind = self.convert_kind(node)?;

        Ok(PhysicalPlanNode {
            kind,
            children,
            output_columns: node.output_columns.clone(),
            stats: planner_stats(node),
            probe_runtime_filters: convert_probe_runtime_filters(self.scalars, node),
        })
    }

    fn convert_kind(&self, node: &OptimizerPhysicalNode) -> Result<PhysicalPlanKind, String> {
        match &node.op {
            Operator::PhysicalScan(op) => Ok(PhysicalPlanKind::Scan(PlanScanNode {
                database: op.database.clone(),
                table: op.table.clone(),
                alias: op.alias.clone(),
                columns: op.columns.clone(),
                predicates: materialize_exprs(self.scalars, &op.predicates),
                required_columns: op.required_columns.clone(),
                dict_columns: op.dict_columns.clone(),
                variant_columns: op.variant_columns.clone(),
                mv_rewritten_from: op.mv_rewritten_from.clone(),
            })),
            Operator::PhysicalFilter(op) => Ok(PhysicalPlanKind::Filter(PlanFilterNode {
                predicate: materialize(self.scalars, op.predicate),
            })),
            Operator::PhysicalProject(op) => Ok(PhysicalPlanKind::Project(PlanProjectNode {
                items: materialize_project_items(self.scalars, &op.items),
                output_qualifier: op.output_qualifier.clone(),
            })),
            Operator::PhysicalSort(op) => Ok(PhysicalPlanKind::Sort(PlanSortNode {
                items: materialize_sort_keys(self.scalars, &op.items),
                analytic_partition_by: materialize_exprs(
                    self.scalars,
                    &op.analytic_partition_exprs,
                ),
                output_columns: node.output_columns.clone(),
                offset: None,
                partition_limit: op.partition_limit,
                topn_type: op.topn_type,
            })),
            Operator::PhysicalLimit(op) => Ok(PhysicalPlanKind::Limit(PlanLimitNode {
                limit: op.limit,
                offset: op.offset,
            })),
            Operator::PhysicalTopN(op) => Ok(PhysicalPlanKind::TopN(PhysicalTopNNode {
                items: materialize_sort_keys(self.scalars, &op.items),
                limit: op.limit,
                offset: op.offset,
                phase: op.phase,
                is_split: op.is_split,
            })),
            Operator::PhysicalHashAggregate(op) => {
                let group_by = materialize_exprs(self.scalars, &op.group_by);
                Ok(PhysicalPlanKind::HashAggregate(Box::new(
                    PhysicalHashAggregateNode {
                        mode: op.mode,
                        group_by,
                        aggregates: materialize_aggregate_calls(
                            self.scalars,
                            &op.aggregates,
                            op.group_by.len(),
                            &op.output_columns,
                        ),
                        is_merge: op.is_merge.clone(),
                        output_columns: op.output_columns.clone(),
                    },
                )))
            }
            Operator::PhysicalHashJoin(op) => {
                let execution_mode =
                    join_execution_mode(node.execution_props.join_distribution);
                let rf_execution_mode = runtime_filter_execution_mode(node, &op.distribution);
                Ok(PhysicalPlanKind::HashJoin(Box::new(PhysicalHashJoinNode {
                    join_type: op.join_type,
                    eq_conditions: op
                        .eq_conditions
                        .iter()
                        .map(|cond| PhysicalHashJoinEqCondition {
                            left: materialize(self.scalars, cond.left),
                            right: materialize(self.scalars, cond.right),
                            null_safe: cond.null_safe,
                        })
                        .collect(),
                    other_condition: op
                        .other_condition
                        .map(|expr| materialize(self.scalars, expr)),
                    distribution: op.distribution.clone(),
                    execution_mode,
                    build_runtime_filters: node
                        .build_runtime_filters
                        .iter()
                        .map(|rf| RuntimeFilterBuildIntent {
                            filter_id: rf.filter_id,
                            build_expr: materialize(self.scalars, rf.build_expr),
                            probe_expr: materialize(self.scalars, rf.probe_expr),
                            expr_order: rf.expr_order,
                            execution_mode: rf_execution_mode,
                        })
                        .collect(),
                })))
            }
            Operator::PhysicalNestLoopJoin(op) => {
                Ok(PhysicalPlanKind::NestLoopJoin(PhysicalNestLoopJoinNode {
                    join_type: op.join_type,
                    condition: op.condition.map(|expr| materialize(self.scalars, expr)),
                }))
            }
            Operator::PhysicalValues(op) => Ok(PhysicalPlanKind::Values(PlanValuesNode {
                rows: op
                    .rows
                    .iter()
                    .map(|row| materialize_exprs(self.scalars, row))
                    .collect(),
                columns: op.columns.clone(),
            })),
            Operator::PhysicalAssertOneRow(op) => {
                Ok(PhysicalPlanKind::AssertOneRow(PlanAssertOneRowNode {
                    subquery_text: op.subquery_text.clone(),
                }))
            }
            Operator::PhysicalDecode(op) => Ok(PhysicalPlanKind::Decode(PlanDecodeNode {
                mappings: op.mappings.clone(),
                output_columns: op.output_columns.clone(),
            })),
            Operator::PhysicalRepeat(op) => Ok(PhysicalPlanKind::Repeat(PlanRepeatNode {
                repeat_column_ref_list: op.repeat_column_ref_list.clone(),
                repeat_column_ref_ids: op.repeat_column_ref_ids.clone(),
                grouping_ids: op.grouping_ids.clone(),
                all_rollup_columns: op.all_rollup_columns.clone(),
                all_rollup_column_ids: op.all_rollup_column_ids.clone(),
                grouping_key_aliases: op.grouping_key_aliases.clone(),
                grouping_fn_args: op.grouping_fn_args.clone(),
                grouping_fn_arg_ids: op.grouping_fn_arg_ids.clone(),
                grouping_fn_ids: op.grouping_fn_ids.clone(),
                virtual_tuple_id: None,
            })),
            Operator::PhysicalWindow(op) => Ok(PhysicalPlanKind::Window(PlanWindowNode {
                window_exprs: materialize_window_exprs(
                    self.scalars,
                    &op.window_exprs,
                    &op.output_columns,
                ),
                output_columns: op.output_columns.clone(),
            })),
            Operator::PhysicalUnion(op) => {
                if !op.all {
                    return Err(
                        "Bridge 2a cannot model UNION DISTINCT without fragment rewrites"
                            .to_string(),
                    );
                }
                Ok(PhysicalPlanKind::SetOp(PhysicalSetOpNode {
                    kind: PlanSetOpKind::UnionAll,
                    output_columns: op.output_columns.clone(),
                    child_output_columns: op.child_output_columns.clone(),
                }))
            }
            Operator::PhysicalIntersect(op) => Ok(PhysicalPlanKind::SetOp(PhysicalSetOpNode {
                kind: PlanSetOpKind::Intersect,
                output_columns: op.output_columns.clone(),
                child_output_columns: op.child_output_columns.clone(),
            })),
            Operator::PhysicalExcept(op) => Ok(PhysicalPlanKind::SetOp(PhysicalSetOpNode {
                kind: PlanSetOpKind::Except,
                output_columns: op.output_columns.clone(),
                child_output_columns: op.child_output_columns.clone(),
            })),
            Operator::PhysicalGenerateSeries(op) => Ok(PhysicalPlanKind::GenerateSeries(
                PlanGenerateSeriesNode {
                    start: op.start,
                    end: op.end,
                    step: op.step,
                    column_name: op.column_name.clone(),
                    alias: op.alias.clone(),
                    output_column_id: op.output_column_id,
                },
            )),
            Operator::PhysicalTableFunction(op) => {
                Ok(PhysicalPlanKind::TableFunction(PlanTableFunctionNode {
                    function_name: op.function_name.clone(),
                    args: materialize_exprs(self.scalars, &op.args),
                    output_columns: op.output_columns.clone(),
                    alias: op.alias.clone(),
                    is_left_join: op.is_left_join,
                }))
            }
            Operator::PhysicalCTEAnchor(op) => Ok(PhysicalPlanKind::CTEAnchor(
                LogicalCTEAnchorNode { cte_id: op.cte_id },
            )),
            Operator::PhysicalCTEProduce(op) => Ok(PhysicalPlanKind::CTEProduce(
                LogicalCTEProduceNode {
                    cte_id: op.cte_id,
                    output_columns: op.output_columns.clone(),
                },
            )),
            Operator::PhysicalCTEConsume(op) => Ok(PhysicalPlanKind::CTEConsume(
                LogicalCTEConsumeNode {
                    cte_id: op.cte_id,
                    alias: op.alias.clone(),
                    output_columns: op.output_columns.clone(),
                },
            )),
            Operator::PhysicalDistribution(op) => {
                Ok(PhysicalPlanKind::Redistribute(RedistributeNode {
                    mode: redistribute_mode(op)?,
                    output_columns: node.output_columns.clone(),
                }))
            }
            Operator::PhysicalAggregateStateMerge(_) => Err(
                "Bridge 2a does not model PhysicalAggregateStateMerge; PIR-7 must either retire it through IMV relationization or define explicit planner IR"
                    .to_string(),
            ),
            op if op.is_logical() => Err(format!(
                "Bridge 2a expected a physical operator, got logical operator {op:?}"
            )),
            op => Err(format!("Bridge 2a cannot convert physical operator {op:?}")),
        }
    }
}

fn join_execution_mode(
    distribution: Option<JoinExecutionDistribution>,
) -> Option<JoinExecutionMode> {
    distribution.map(|distribution| match distribution {
        JoinExecutionDistribution::Broadcast => JoinExecutionMode::Broadcast,
        JoinExecutionDistribution::Partitioned => JoinExecutionMode::Partitioned,
        JoinExecutionDistribution::Colocate => JoinExecutionMode::Colocate,
    })
}

fn runtime_filter_execution_mode(
    node: &OptimizerPhysicalNode,
    fallback_join_distribution: &crate::sql::optimizer::operator::JoinDistribution,
) -> JoinExecutionMode {
    join_execution_mode(node.execution_props.join_distribution).unwrap_or(
        match fallback_join_distribution {
            crate::sql::optimizer::operator::JoinDistribution::Shuffle => {
                JoinExecutionMode::Partitioned
            }
            crate::sql::optimizer::operator::JoinDistribution::Colocate => {
                JoinExecutionMode::Colocate
            }
            crate::sql::optimizer::operator::JoinDistribution::Broadcast
            | crate::sql::optimizer::operator::JoinDistribution::Unknown => {
                JoinExecutionMode::Broadcast
            }
        },
    )
}

fn convert_probe_runtime_filters(
    scalars: &ScalarArena,
    node: &OptimizerPhysicalNode,
) -> Vec<RuntimeFilterProbeIntent> {
    node.probe_runtime_filters
        .iter()
        .map(|rf| RuntimeFilterProbeIntent {
            filter_id: rf.filter_id,
            probe_expr: materialize(scalars, rf.probe_expr),
        })
        .collect()
}

fn redistribute_mode(op: &PhysicalDistributionOp) -> Result<RedistributeMode, String> {
    match &op.spec {
        DistributionSpec::Gather => Ok(RedistributeMode::Gather),
        DistributionSpec::Broadcast => Ok(RedistributeMode::Broadcast),
        DistributionSpec::HashPartitioned { cols, source } => Ok(RedistributeMode::Hash {
            cols: cols.clone(),
            source: *source,
        }),
        DistributionSpec::Any => Err(
            "Bridge 2a cannot convert PhysicalDistribution with DistributionSpec::Any".to_string(),
        ),
    }
}

fn planner_confidence(confidence: Confidence) -> PlannerConfidence {
    match confidence {
        Confidence::Fallback => PlannerConfidence::Fallback,
        Confidence::Estimated => PlannerConfidence::Estimated,
        Confidence::Exact => PlannerConfidence::Exact,
        Confidence::Measured => PlannerConfidence::Measured,
    }
}

fn planner_column_statistic(stat: &ColumnStatistic) -> PlannerColumnStatistic {
    PlannerColumnStatistic {
        min_value: stat.min_value,
        max_value: stat.max_value,
        nulls_fraction: stat.nulls_fraction,
        average_row_size: stat.average_row_size,
        ndv: match &stat.ndv {
            DistinctValueCount::Known { value, .. } => Some(*value),
            DistinctValueCount::Unknown { .. } => None,
        },
        confidence: planner_confidence(stat.confidence),
    }
}

fn planner_stats(node: &OptimizerPhysicalNode) -> PhysicalPlanStats {
    PhysicalPlanStats {
        output_row_count: node.stats.output_row_count,
        row_count_confidence: planner_confidence(node.stats.row_count_confidence),
        column_statistics: node
            .stats
            .column_statistics
            .iter()
            .map(|(column, stat)| (*column, planner_column_statistic(stat)))
            .collect(),
        cost_estimate: node
            .explain_stats
            .cost_estimate
            .as_ref()
            .map(|cost| PlannerCostEstimate {
                cpu_cost: cost.cpu_cost,
                memory_cost: cost.memory_cost,
                network_cost: cost.network_cost,
            }),
        broadcast_decision: node
            .explain_stats
            .broadcast_decision
            .as_ref()
            .map(|decision| PlannerBroadcastDecision {
                feasible: decision.feasible,
                forced: decision.forced,
                build_bytes: decision.build_bytes,
                hash_table_bytes: decision.hash_table_bytes,
                effective_backend_count: decision.effective_backend_count,
                risk_adj_fanout_bytes: decision.risk_adj_fanout_bytes,
                per_node_budget_bytes: decision.per_node_budget_bytes,
                cluster_network_budget_bytes: decision.cluster_network_budget_bytes,
                risk_multiplier: decision.risk_multiplier,
                reject_reason: decision
                    .reject_reason
                    .as_ref()
                    .map(|reason| format!("{reason:?}")),
            }),
    }
}

pub(crate) fn optimizer_physical_to_plan(
    root: &OptimizerPhysicalNode,
) -> Result<PhysicalPlanNode, String> {
    let scalars = root
        .execution_props
        .scalar_arena
        .as_deref()
        .ok_or_else(|| "Bridge 2a requires OptimizerPhysicalNode.scalar_arena".to_string())?;
    BridgeCtx { scalars }.convert_node(root)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::analysis::{ExprKind, LiteralValue, TypedExpr};
    use crate::sql::column_id::ColumnId;
    use crate::sql::common::OutputColumn;
    use crate::sql::optimizer::operator::{
        AggregateStateMergeOp, Operator, PhysicalDistributionOp, ValuesOp,
    };
    use crate::sql::optimizer::physical_tree::{OptimizerPhysicalNode, PlanExecutionProps};
    use crate::sql::optimizer::property::{DistributionSpec, HashSource, PhysicalPropertySet};
    use crate::sql::optimizer::scalar::ScalarArena;
    use crate::sql::optimizer::statistics::{Confidence, Statistics};
    use crate::sql::planner::PhysicalPlanKind;
    use std::sync::Arc;

    fn int_expr(v: i64) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::Literal(LiteralValue::Int(v)),
            data_type: arrow::datatypes::DataType::Int64,
            nullable: false,
        }
    }

    fn attach_arena(
        mut node: OptimizerPhysicalNode,
        arena: Arc<ScalarArena>,
    ) -> OptimizerPhysicalNode {
        node.execution_props.scalar_arena = Some(arena);
        node
    }

    fn base_node(op: Operator) -> OptimizerPhysicalNode {
        OptimizerPhysicalNode {
            op,
            children: vec![],
            stats: Statistics {
                output_row_count: 1.0,
                row_count_confidence: Confidence::Exact,
                ..Default::default()
            },
            explain_stats: Default::default(),
            output_columns: vec![],
            execution_props: PlanExecutionProps {
                output_property: PhysicalPropertySet::gather(),
                child_output_properties: vec![],
                join_distribution: None,
                scalar_arena: None,
            },
            build_runtime_filters: vec![],
            probe_runtime_filters: vec![],
        }
    }

    fn values_node() -> OptimizerPhysicalNode {
        attach_arena(
            base_node(Operator::PhysicalValues(ValuesOp {
                rows: vec![],
                columns: vec![OutputColumn {
                    column_id: ColumnId::new_for_test(1),
                    name: "v".to_string(),
                    data_type: arrow::datatypes::DataType::Int32,
                    nullable: false,
                    is_internal: false,
                }],
            })),
            Arc::new(ScalarArena::new()),
        )
    }

    #[test]
    fn bridge_converts_values_without_optimizer_types() {
        let physical = optimizer_physical_to_plan(&values_node()).expect("bridge should convert");
        assert!(matches!(physical.kind, PhysicalPlanKind::Values(_)));
        assert!(physical.probe_runtime_filters.is_empty());
        assert_eq!(physical.stats.output_row_count, 1.0);
    }

    #[test]
    fn bridge_materializes_values_rows() {
        let mut arena = ScalarArena::new();
        let one =
            crate::sql::planner::optimizer_bridge::scalar::intern_typed(&mut arena, &int_expr(1));
        let node = attach_arena(
            base_node(Operator::PhysicalValues(ValuesOp {
                rows: vec![vec![one]],
                columns: vec![],
            })),
            Arc::new(arena),
        );

        let physical = optimizer_physical_to_plan(&node).expect("bridge should convert");
        let PhysicalPlanKind::Values(values) = physical.kind else {
            panic!("expected Values");
        };
        assert_eq!(values.rows.len(), 1);
        assert_eq!(values.rows[0].len(), 1);
        assert!(matches!(
            values.rows[0][0].kind,
            ExprKind::Literal(LiteralValue::Int(1))
        ));
        assert_eq!(
            values.rows[0][0].data_type,
            arrow::datatypes::DataType::Int64
        );
        assert!(!values.rows[0][0].nullable);
    }

    #[test]
    fn physical_distribution_becomes_redistribute_hash() {
        let node = attach_arena(
            base_node(Operator::PhysicalDistribution(PhysicalDistributionOp {
                spec: DistributionSpec::HashPartitioned {
                    cols: vec![ColumnId::new_for_test(7)],
                    source: HashSource::ShuffleJoin,
                },
            })),
            Arc::new(ScalarArena::new()),
        );

        let physical = optimizer_physical_to_plan(&node).expect("bridge should convert");
        let PhysicalPlanKind::Redistribute(redistribute) = physical.kind else {
            panic!("expected Redistribute");
        };
        assert_eq!(
            redistribute.mode,
            RedistributeMode::Hash {
                cols: vec![ColumnId::new_for_test(7)],
                source: HashSource::ShuffleJoin,
            }
        );
    }

    #[test]
    fn physical_distribution_any_is_rejected() {
        let node = attach_arena(
            base_node(Operator::PhysicalDistribution(PhysicalDistributionOp {
                spec: DistributionSpec::Any,
            })),
            Arc::new(ScalarArena::new()),
        );

        let err = optimizer_physical_to_plan(&node).expect_err("Any should be rejected");
        assert!(err.contains("DistributionSpec::Any"));
    }

    #[test]
    fn bridge_rejects_logical_operator() {
        let node = attach_arena(
            base_node(Operator::LogicalValues(ValuesOp {
                rows: vec![],
                columns: vec![],
            })),
            Arc::new(ScalarArena::new()),
        );

        let err = optimizer_physical_to_plan(&node).expect_err("logical op should be rejected");
        assert!(err.contains("Bridge 2a expected a physical operator"));
    }

    #[test]
    fn bridge_rejects_aggregate_state_merge_with_pir7_message() {
        let node = attach_arena(
            base_node(Operator::PhysicalAggregateStateMerge(
                AggregateStateMergeOp {
                    group_key_names: vec![],
                    aggregate_state_names: vec![],
                    change_op_column: "op".to_string(),
                    output_columns: vec![],
                },
            )),
            Arc::new(ScalarArena::new()),
        );

        let err = optimizer_physical_to_plan(&node).expect_err("IMV direct-exec op is PIR-7");
        assert!(err.contains("PIR-7"));
    }
}
