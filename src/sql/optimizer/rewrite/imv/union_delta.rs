use crate::sql::optimizer::rewrite::context::RewriteContext;
use crate::sql::optimizer::rewrite::imv::action_column::ImvActionColumn;
use crate::sql::optimizer::rewrite::imv::annotation::ImvExtension;
use crate::sql::optimizer::rewrite::imv::join_delta::{
    mark_delta_scan, normalize_branch_output, plan_output_columns,
};
use crate::sql::optimizer::rewrite::imv::marker::{ImvDeltaNode, plan_contains_imv_marker};
use crate::sql::optimizer::rewrite::phase::RewritePhase;
use crate::sql::optimizer::rewrite::result::RewriteResult;
use crate::sql::optimizer::rewrite::rule::{LogicalRewriteRule, RewriteTraversal};
use crate::sql::planner::plan::{LogicalPlan, UnionNode};

pub(crate) struct RewriteUnionAggregateDeltaRule;

impl LogicalRewriteRule for RewriteUnionAggregateDeltaRule {
    fn name(&self) -> &'static str {
        "RewriteUnionAggregateDelta"
    }

    fn phase(&self) -> RewritePhase {
        RewritePhase::StructuralRewrite
    }

    fn traversal(&self) -> RewriteTraversal {
        RewriteTraversal::TopDown
    }

    fn matches(&self, plan: &LogicalPlan, _ctx: &RewriteContext) -> bool {
        matches!(
            plan,
            LogicalPlan::ImvDelta(delta)
                if delta.is_root
                    && matches!(
                        delta.input.as_ref(),
                        LogicalPlan::Aggregate(aggregate)
                            if matches!(
                                aggregate.input.as_ref(),
                                LogicalPlan::Union(union)
                                    if union.all
                                        && !plan_contains_imv_marker(aggregate.input.as_ref())
                            )
                    )
        )
    }

    fn apply(&self, plan: LogicalPlan, ctx: &mut RewriteContext) -> Result<RewriteResult, String> {
        let LogicalPlan::ImvDelta(delta) = plan else {
            return Ok(RewriteResult::Unchanged);
        };
        if !delta.is_root {
            return Ok(RewriteResult::Unchanged);
        }
        let LogicalPlan::Aggregate(mut aggregate) = *delta.input else {
            return Ok(RewriteResult::Unchanged);
        };
        let LogicalPlan::Union(union) = *aggregate.input else {
            return Ok(RewriteResult::Unchanged);
        };
        if plan_contains_imv_marker(&LogicalPlan::Union(union.clone())) {
            return Ok(RewriteResult::Unchanged);
        }
        if !union.all {
            return Err(
                "Iceberg IMV UNION aggregate delta rewrite supports UNION ALL only".to_string(),
            );
        }

        let action_column = match delta.action_column {
            Some(action_column) => action_column,
            None => ctx
                .extension::<ImvExtension>()
                .ok_or_else(|| {
                    "RewriteUnionAggregateDelta requires ImvExtension in RewriteContext".to_string()
                })?
                .allocate_column_id(),
        };

        let UnionNode {
            inputs,
            all,
            output_columns,
            required_output_columns,
        } = union;
        let action_output = ImvActionColumn::output_column(action_column);
        let mut rewritten_inputs = Vec::with_capacity(inputs.len());
        for branch in inputs {
            let mut branch_output = plan_output_columns(&branch)?;
            branch_output.push(action_output.clone());
            let marked = mark_delta_scan(branch, action_column)?;
            rewritten_inputs.push(normalize_branch_output(marked, &branch_output));
        }

        let mut union_output_columns = output_columns;
        union_output_columns.push(action_output);
        aggregate.input = Box::new(LogicalPlan::Union(UnionNode {
            inputs: rewritten_inputs,
            all,
            output_columns: union_output_columns,
            required_output_columns,
        }));

        Ok(RewriteResult::Changed(LogicalPlan::ImvDelta(
            ImvDeltaNode {
                input: Box::new(LogicalPlan::Aggregate(aggregate)),
                is_root: true,
                action_column: Some(action_column),
            },
        )))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::Arc;
    use std::sync::atomic::AtomicU32;

    use arrow::datatypes::DataType;

    use super::*;
    use crate::engine::mv::refresh_context::tests_support::dummy_rewrite_context;
    use crate::sql::analysis::{ExprKind, OutputColumn, TypedExpr};
    use crate::sql::catalog::{
        ColumnDef, IcebergSchemaDef, IcebergTableInfo, ScanSource, TableDef,
    };
    use crate::sql::column_id::ColumnId;
    use crate::sql::optimizer::rewrite::context::RewriteContext;
    use crate::sql::optimizer::rewrite::imv::action_column::ImvActionColumn;
    use crate::sql::optimizer::rewrite::imv::annotation::{ImvExtension, ImvPlanAnnotation};
    use crate::sql::optimizer::rewrite::imv::marker::ImvDeltaNode;
    use crate::sql::planner::plan::{AggregateNode, ScanNode, UnionNode};

    #[test]
    fn matches_root_delta_over_aggregate_over_source_union() {
        let rule = RewriteUnionAggregateDeltaRule;
        let ctx = build_ctx();
        let plan = delta(aggregate_over(source_union(true)));

        assert!(rule.matches(&plan, &ctx));
    }

    #[test]
    fn does_not_match_union_already_marked() {
        let rule = RewriteUnionAggregateDeltaRule;
        let ctx = build_ctx();
        let plan = delta(aggregate_over(marked_source_union()));

        assert!(!rule.matches(&plan, &ctx));
    }

    #[test]
    fn rewrite_marks_each_branch_with_shared_action_column() {
        let rule = RewriteUnionAggregateDeltaRule;
        let mut ctx = build_ctx();
        let plan = delta(aggregate_over(source_union(true)));

        assert!(rule.matches(&plan, &ctx));
        let RewriteResult::Changed(LogicalPlan::ImvDelta(root_delta)) = rule
            .apply(plan, &mut ctx)
            .expect("UNION ALL aggregate delta must rewrite")
        else {
            panic!("expected Changed(ImvDelta)");
        };
        assert!(root_delta.is_root);
        let action_column = root_delta
            .action_column
            .expect("root delta must carry shared action column");
        assert_eq!(action_column, ColumnId(100));

        let LogicalPlan::Aggregate(aggregate) = root_delta.input.as_ref() else {
            panic!("expected root ImvDelta(Aggregate)");
        };
        let LogicalPlan::Union(union) = aggregate.input.as_ref() else {
            panic!("expected Aggregate(Union)");
        };
        assert!(union.all);
        assert_eq!(union.inputs.len(), 2);
        assert_eq!(
            union
                .output_columns
                .iter()
                .map(|column| column.column_id)
                .collect::<Vec<_>>(),
            vec![ColumnId(1), ColumnId(2), action_column]
        );
        assert_eq!(union.required_output_columns, required_output_columns());

        assert_normalized_delta_branch(
            &union.inputs[0],
            action_column,
            0,
            &[ColumnId(1), ColumnId(2), action_column],
        );
        assert_normalized_delta_branch(
            &union.inputs[1],
            action_column,
            1,
            &[ColumnId(10), ColumnId(11), action_column],
        );
    }

    fn build_ctx() -> RewriteContext {
        let mut ctx = RewriteContext::for_mv_refresh(Vec::<String>::new());
        ctx.set_extension::<ImvExtension>(ImvExtension {
            mv_ctx: dummy_rewrite_context(),
            annotation: ImvPlanAnnotation::default(),
            next_column_id: Arc::new(AtomicU32::new(100)),
        });
        ctx
    }

    fn delta(input: LogicalPlan) -> LogicalPlan {
        LogicalPlan::ImvDelta(ImvDeltaNode {
            input: Box::new(input),
            is_root: true,
            action_column: None,
        })
    }

    fn aggregate_over(input: LogicalPlan) -> LogicalPlan {
        LogicalPlan::Aggregate(AggregateNode {
            input: Box::new(input),
            group_by: vec![col_expr(1, "k")],
            aggregates: Vec::new(),
            output_columns: vec![output_column(1, "k")],
            already_pushed: false,
            required_output_columns: None,
        })
    }

    fn source_union(all: bool) -> LogicalPlan {
        LogicalPlan::Union(UnionNode {
            inputs: vec![scan("t1", 1), scan("t2", 10)],
            all,
            output_columns: vec![output_column(1, "k"), output_column(2, "v")],
            required_output_columns: required_output_columns(),
        })
    }

    fn marked_source_union() -> LogicalPlan {
        LogicalPlan::Union(UnionNode {
            inputs: vec![
                LogicalPlan::ImvDelta(ImvDeltaNode {
                    input: Box::new(scan("t1", 1)),
                    is_root: false,
                    action_column: Some(ColumnId(99)),
                }),
                scan("t2", 1),
            ],
            all: true,
            output_columns: vec![output_column(1, "k"), output_column(2, "v")],
            required_output_columns: None,
        })
    }

    fn required_output_columns() -> Option<std::collections::HashSet<ColumnId>> {
        Some([ColumnId(1), ColumnId(2)].into_iter().collect())
    }

    fn scan(name: &str, first_id: u32) -> LogicalPlan {
        let columns = vec![column_def("k"), column_def("v")];
        LogicalPlan::Scan(ScanNode {
            database: "db".to_string(),
            table: TableDef {
                name: name.to_string(),
                columns,
                iceberg_row_lineage_metadata_columns: Vec::new(),
                source: ScanSource::IcebergDataFiles {
                    table: IcebergTableInfo {
                        catalog: "ice".to_string(),
                        namespace: "db".to_string(),
                        table: name.to_string(),
                        table_uuid: Some(format!("uuid-{name}")),
                        current_snapshot_id: Some(22),
                        schema_id: 7,
                        location: format!("file:///tmp/ice/db/{name}"),
                        schema: IcebergSchemaDef { fields: Vec::new() },
                        serialized_metadata: None,
                    },
                    files: Vec::new(),
                    cloud_properties: BTreeMap::new(),
                },
            },
            alias: None,
            columns: vec![
                output_column(first_id, "k"),
                output_column(first_id + 1, "v"),
            ],
            predicates: Vec::new(),
            required_columns: None,
            dict_columns: Vec::new(),
            required_output_columns: None,
        })
    }

    fn column_def(name: &str) -> ColumnDef {
        ColumnDef {
            name: name.to_string(),
            data_type: DataType::Int64,
            nullable: false,
            write_default: None,
            logical_type: None,
        }
    }

    fn output_column(id: u32, name: &str) -> OutputColumn {
        OutputColumn {
            column_id: ColumnId(id),
            name: name.to_string(),
            data_type: DataType::Int64,
            nullable: false,
            is_internal: false,
        }
    }

    fn col_expr(id: u32, name: &str) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::ColumnRef {
                column_id: ColumnId(id),
                qualifier: None,
                column: name.to_string(),
            },
            data_type: DataType::Int64,
            nullable: false,
        }
    }

    fn assert_normalized_delta_branch(
        plan: &LogicalPlan,
        action_column: ColumnId,
        idx: usize,
        expected_item_ids: &[ColumnId],
    ) {
        let LogicalPlan::Project(project) = plan else {
            panic!("branch {idx} must be normalized through Project");
        };
        assert_eq!(
            project
                .items
                .iter()
                .map(|item| item.output_column_id)
                .collect::<Vec<_>>(),
            expected_item_ids
        );
        assert_eq!(
            project
                .items
                .iter()
                .map(|item| match &item.expr.kind {
                    ExprKind::ColumnRef { column_id, .. } => *column_id,
                    other => panic!("branch {idx} must project ColumnRef, got {other:?}"),
                })
                .collect::<Vec<_>>(),
            expected_item_ids
        );
        assert!(
            project.items.iter().any(|item| item
                .output_name
                .eq_ignore_ascii_case(ImvActionColumn::NAME)
                && item.output_column_id == action_column),
            "branch {idx} must expose shared action column"
        );
        let LogicalPlan::ImvDelta(delta) = project.input.as_ref() else {
            panic!("branch {idx} must wrap source in ImvDelta");
        };
        assert!(!delta.is_root);
        assert_eq!(delta.action_column, Some(action_column));
        assert!(matches!(delta.input.as_ref(), LogicalPlan::Scan(_)));
    }
}
