use crate::sql::analysis::cte::CteId;
use crate::sql::planner::plan::*;
use std::collections::{HashMap, HashSet};

#[derive(Clone, Debug, Default)]
pub(crate) struct CTEContext {
    pub produces: HashSet<CteId>,
    pub consume_count: HashMap<CteId, usize>,
}

pub(crate) fn collect_cte_counts(plan: &LogicalPlanNode) -> CTEContext {
    fn visit(plan: &LogicalPlanNode, ctx: &mut CTEContext) {
        match &plan.kind {
            LogicalPlanNodeKind::CTEAnchor(node) => {
                ctx.produces.insert(node.cte_id);
                for child in &plan.children {
                    visit(child, ctx);
                }
            }
            LogicalPlanNodeKind::CTEConsume(node) => {
                *ctx.consume_count.entry(node.cte_id).or_insert(0) += 1;
            }
            LogicalPlanNodeKind::ImvDelta(_) | LogicalPlanNodeKind::ImvVersion(_) => {
                panic!("imv marker leaked into non-IMV plan");
            }
            _ => {
                for child in &plan.children {
                    visit(child, ctx);
                }
            }
        }
    }

    let mut ctx = CTEContext::default();
    visit(plan, &mut ctx);
    ctx
}

pub(crate) fn inline_single_use_ctes(
    mut plan: LogicalPlanNode,
    ctx: &CTEContext,
) -> Result<LogicalPlanNode, String> {
    match &plan.kind {
        LogicalPlanNodeKind::CTEAnchor(node) => {
            let cte_id = node.cte_id;
            let produce = inline_single_use_ctes(plan.take_child(0), ctx)?;
            let consumer = inline_single_use_ctes(plan.take_child(0), ctx)?;
            let consume_count = ctx.consume_count.get(&cte_id).copied().unwrap_or(0);

            // Inline single-use CTEs. Multi-consume CTEs use the CTE
            // Produce/Consume path with MultiCast exchange.
            if ctx.produces.contains(&cte_id) && consume_count <= 1 {
                let produce_input = if matches!(
                    &produce.kind,
                    LogicalPlanNodeKind::CTEProduce(produce_node) if produce_node.cte_id == cte_id
                ) {
                    produce.into_single_child()
                } else {
                    produce
                };
                replace_cte_consume(consumer, cte_id, &produce_input)
            } else {
                plan.children = vec![produce, consumer];
                Ok(plan)
            }
        }
        LogicalPlanNodeKind::ImvDelta(_) | LogicalPlanNodeKind::ImvVersion(_) => {
            panic!("imv marker leaked into non-IMV plan");
        }
        _ => {
            plan.children = plan
                .children
                .into_iter()
                .map(|child| inline_single_use_ctes(child, ctx))
                .collect::<Result<Vec<_>, _>>()?;
            Ok(plan)
        }
    }
}

fn replace_cte_consume(
    mut plan: LogicalPlanNode,
    cte_id: CteId,
    replacement: &LogicalPlanNode,
) -> Result<LogicalPlanNode, String> {
    match &plan.kind {
        LogicalPlanNodeKind::CTEConsume(node) if node.cte_id == cte_id => {
            crate::sql::planner::adapt_plan_output_with_qualifier(
                replacement.clone(),
                &node.output_columns,
                Some(&node.alias),
            )
        }
        LogicalPlanNodeKind::CTEConsume(_) => Ok(plan),
        LogicalPlanNodeKind::ImvDelta(_) | LogicalPlanNodeKind::ImvVersion(_) => {
            panic!("imv marker leaked into non-IMV plan");
        }
        _ => {
            plan.children = plan
                .children
                .into_iter()
                .map(|child| replace_cte_consume(child, cte_id, replacement))
                .collect::<Result<Vec<_>, _>>()?;
            Ok(plan)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::analysis::OutputColumn;
    use crate::sql::catalog::{ColumnDef, ScanSource, TableDef};
    use crate::sql::column_id::ColumnId;
    use arrow::datatypes::DataType;

    fn scan_plan() -> LogicalPlanNode {
        LogicalPlanNode::new(
            LogicalPlanNodeKind::Scan(LogicalScanNode {
                database: "db".to_string(),
                table: TableDef {
                    name: "t1".to_string(),
                    columns: vec![ColumnDef {
                        name: "id".to_string(),
                        data_type: DataType::Int32,
                        nullable: false,
                        write_default: None,
                        logical_type: None,
                    }],
                    iceberg_row_lineage_metadata_columns: vec![],
                    source: ScanSource::StarRocks {
                        db_id: 0,
                        table_id: 0,
                    },
                },
                alias: None,
                columns: vec![OutputColumn {
                    column_id: ColumnId::UNSET,
                    name: "id".to_string(),
                    data_type: DataType::Int32,
                    nullable: false,
                    is_internal: false,
                }],
                predicates: vec![],
                required_columns: None,
                dict_columns: vec![],
                variant_columns: vec![],
            }),
            vec![],
            None,
        )
    }

    fn output_columns() -> Vec<OutputColumn> {
        vec![OutputColumn {
            column_id: ColumnId::UNSET,
            name: "id".to_string(),
            data_type: DataType::Int32,
            nullable: false,
            is_internal: false,
        }]
    }

    fn output_columns_with_id_and_name(column_id: ColumnId, name: &str) -> Vec<OutputColumn> {
        vec![OutputColumn {
            column_id,
            name: name.to_string(),
            data_type: DataType::Int32,
            nullable: false,
            is_internal: false,
        }]
    }

    fn consume_plan(cte_id: CteId, alias: &str) -> LogicalPlanNode {
        LogicalPlanNode::new(
            LogicalPlanNodeKind::CTEConsume(LogicalCTEConsumeNode {
                cte_id: cte_id,
                alias: alias.to_string(),
                output_columns: output_columns(),
            }),
            vec![],
            None,
        )
    }

    fn consume_plan_with_output_columns(
        cte_id: CteId,
        alias: &str,
        output_columns: Vec<OutputColumn>,
    ) -> LogicalPlanNode {
        LogicalPlanNode::new(
            LogicalPlanNodeKind::CTEConsume(LogicalCTEConsumeNode {
                cte_id: cte_id,
                alias: alias.to_string(),
                output_columns: output_columns,
            }),
            vec![],
            None,
        )
    }

    #[test]
    fn test_collect_cte_counts_counts_consumes() {
        let plan = LogicalPlanNode::new(
            LogicalPlanNodeKind::CTEAnchor(LogicalCTEAnchorNode { cte_id: 1 }),
            vec![
                LogicalPlanNode::new(
                    LogicalPlanNodeKind::CTEProduce(LogicalCTEProduceNode {
                        cte_id: 1,
                        output_columns: output_columns(),
                    }),
                    vec![scan_plan()],
                    None,
                ),
                LogicalPlanNode::new(
                    LogicalPlanNodeKind::CTEConsume(LogicalCTEConsumeNode {
                        cte_id: 1,
                        alias: "t".to_string(),
                        output_columns: output_columns(),
                    }),
                    vec![],
                    None,
                ),
            ],
            None,
        );

        let ctx = collect_cte_counts(&plan);
        assert!(ctx.produces.contains(&1));
        assert_eq!(ctx.consume_count.get(&1), Some(&1));
    }

    #[test]
    fn test_inline_single_use_cte_removes_anchor_without_alias_node() {
        let plan = LogicalPlanNode::new(
            LogicalPlanNodeKind::CTEAnchor(LogicalCTEAnchorNode { cte_id: 1 }),
            vec![
                LogicalPlanNode::new(
                    LogicalPlanNodeKind::CTEProduce(LogicalCTEProduceNode {
                        cte_id: 1,
                        output_columns: output_columns(),
                    }),
                    vec![scan_plan()],
                    None,
                ),
                consume_plan(1, "t"),
            ],
            None,
        );

        let ctx = collect_cte_counts(&plan);
        let rewritten = inline_single_use_ctes(plan, &ctx).expect("inline should succeed");
        assert!(matches!(
            &rewritten.kind,
            LogicalPlanNodeKind::Scan(_) | LogicalPlanNodeKind::Project(_)
        ));
    }

    #[test]
    fn test_inline_single_use_cte_preserves_consumer_output_columns_with_project() {
        let consume_output_id = ColumnId::new_for_test(42);
        let consume_output_columns = output_columns_with_id_and_name(consume_output_id, "x_id");
        let plan = LogicalPlanNode::new(
            LogicalPlanNodeKind::CTEAnchor(LogicalCTEAnchorNode { cte_id: 1 }),
            vec![
                LogicalPlanNode::new(
                    LogicalPlanNodeKind::CTEProduce(LogicalCTEProduceNode {
                        cte_id: 1,
                        output_columns: output_columns(),
                    }),
                    vec![scan_plan()],
                    None,
                ),
                consume_plan_with_output_columns(1, "x", consume_output_columns.clone()),
            ],
            None,
        );

        let ctx = collect_cte_counts(&plan);
        let rewritten = inline_single_use_ctes(plan, &ctx).expect("inline should succeed");

        let output = crate::sql::planner::plan_output_columns(&rewritten)
            .expect("rewritten output columns should be derivable");
        assert_eq!(output.len(), consume_output_columns.len());
        assert_eq!(output[0].column_id, consume_output_columns[0].column_id);
        assert_eq!(output[0].name, consume_output_columns[0].name);
        assert_eq!(output[0].data_type, consume_output_columns[0].data_type);
        assert_eq!(output[0].nullable, consume_output_columns[0].nullable);
        let LogicalPlanNodeKind::Project(project) = &rewritten.kind else {
            panic!("expected Project adapter");
        };
        assert_eq!(project.items[0].output_name, "x_id");
        assert_eq!(project.items[0].output_column_id, consume_output_id);
    }

    #[test]
    fn test_inline_single_use_cte_keeps_multi_use_anchor() {
        let plan = LogicalPlanNode::new(
            LogicalPlanNodeKind::CTEAnchor(LogicalCTEAnchorNode { cte_id: 1 }),
            vec![
                LogicalPlanNode::new(
                    LogicalPlanNodeKind::CTEProduce(LogicalCTEProduceNode {
                        cte_id: 1,
                        output_columns: output_columns(),
                    }),
                    vec![scan_plan()],
                    None,
                ),
                LogicalPlanNode::new(
                    LogicalPlanNodeKind::Union(LogicalUnionNode {
                        all: true,
                        output_columns: vec![],
                    }),
                    vec![consume_plan(1, "t1"), consume_plan(1, "t2")],
                    None,
                ),
            ],
            None,
        );

        let ctx = collect_cte_counts(&plan);
        assert_eq!(ctx.consume_count.get(&1), Some(&2));

        let rewritten = inline_single_use_ctes(plan, &ctx).expect("inline should succeed");
        assert!(matches!(&rewritten.kind, LogicalPlanNodeKind::CTEAnchor(_)));
    }

    #[test]
    fn test_inline_single_use_cte_inlines_nested_cte_inside_later_produce() {
        let plan = LogicalPlanNode::new(
            LogicalPlanNodeKind::CTEAnchor(LogicalCTEAnchorNode { cte_id: 1 }),
            vec![
                LogicalPlanNode::new(
                    LogicalPlanNodeKind::CTEProduce(LogicalCTEProduceNode {
                        cte_id: 1,
                        output_columns: output_columns(),
                    }),
                    vec![scan_plan()],
                    None,
                ),
                LogicalPlanNode::new(
                    LogicalPlanNodeKind::CTEAnchor(LogicalCTEAnchorNode { cte_id: 2 }),
                    vec![
                        LogicalPlanNode::new(
                            LogicalPlanNodeKind::CTEProduce(LogicalCTEProduceNode {
                                cte_id: 2,
                                output_columns: output_columns(),
                            }),
                            vec![LogicalPlanNode::new(
                                LogicalPlanNodeKind::CTEAnchor(LogicalCTEAnchorNode { cte_id: 1 }),
                                vec![
                                    LogicalPlanNode::new(
                                        LogicalPlanNodeKind::CTEProduce(LogicalCTEProduceNode {
                                            cte_id: 1,
                                            output_columns: output_columns(),
                                        }),
                                        vec![scan_plan()],
                                        None,
                                    ),
                                    consume_plan(1, "a"),
                                ],
                                None,
                            )],
                            None,
                        ),
                        LogicalPlanNode::new(
                            LogicalPlanNodeKind::Union(LogicalUnionNode {
                                all: true,
                                output_columns: vec![],
                            }),
                            vec![consume_plan(2, "b1"), consume_plan(2, "b2")],
                            None,
                        ),
                    ],
                    None,
                ),
            ],
            None,
        );

        let ctx = collect_cte_counts(&plan);
        assert_eq!(ctx.consume_count.get(&1), Some(&1));
        assert_eq!(ctx.consume_count.get(&2), Some(&2));

        let rewritten = inline_single_use_ctes(plan, &ctx).expect("inline should succeed");

        match &rewritten.kind {
            LogicalPlanNodeKind::CTEAnchor(anchor) => {
                assert_eq!(anchor.cte_id, 2);
                let produce_plan = rewritten.child(0);
                match &produce_plan.kind {
                    LogicalPlanNodeKind::CTEProduce(_) => match &produce_plan.unary_input().kind {
                        LogicalPlanNodeKind::Scan(_) | LogicalPlanNodeKind::Project(_) => {}
                        other => panic!("expected nested inline replacement, got {other:?}"),
                    },
                    other => panic!("expected CTEProduce for b, got {other:?}"),
                }
                assert!(matches!(
                    &rewritten.child(1).kind,
                    LogicalPlanNodeKind::Union(_)
                ));
            }
            other => panic!("expected surviving anchor for b, got {other:?}"),
        }
    }

    #[test]
    fn test_replace_cte_consume_only_rewrites_targeted_cte_id() {
        let plan = LogicalPlanNode::new(
            LogicalPlanNodeKind::CTEAnchor(LogicalCTEAnchorNode { cte_id: 2 }),
            vec![
                LogicalPlanNode::new(
                    LogicalPlanNodeKind::CTEProduce(LogicalCTEProduceNode {
                        cte_id: 2,
                        output_columns: output_columns(),
                    }),
                    vec![scan_plan()],
                    None,
                ),
                LogicalPlanNode::new(
                    LogicalPlanNodeKind::Union(LogicalUnionNode {
                        all: true,
                        output_columns: vec![],
                    }),
                    vec![consume_plan(1, "target"), consume_plan(2, "shadow")],
                    None,
                ),
            ],
            None,
        );

        let rewritten = replace_cte_consume(plan, 1, &scan_plan()).expect("replace should succeed");

        match &rewritten.kind {
            LogicalPlanNodeKind::CTEAnchor(_) => match &rewritten.child(1).kind {
                LogicalPlanNodeKind::Union(_) => {
                    let union_plan = rewritten.child(1);
                    match &union_plan.child(0).kind {
                        LogicalPlanNodeKind::Scan(_) | LogicalPlanNodeKind::Project(_) => {}
                        other => panic!("expected targeted consume to be rewritten, got {other:?}"),
                    }
                    assert!(matches!(
                        &union_plan.child(1).kind,
                        LogicalPlanNodeKind::CTEConsume(_)
                    ));
                }
                other => panic!("expected union consumer, got {other:?}"),
            },
            other => panic!("expected outer anchor, got {other:?}"),
        }
    }
}
