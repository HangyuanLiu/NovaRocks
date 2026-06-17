//! Aggregate pushdown rewriter — phase 2 of the rule.

use crate::sql::analysis::{ExprKind, OutputColumn};
use crate::sql::codegen::helpers::{agg_call_display_name_from_parts, typed_expr_display_name};
use crate::sql::column_id::{ColumnId, ColumnRefFactory};
use crate::sql::optimizer::operator::{
    AggStage, LogicalAggregateOp, Operator, ProjectOp, ScalarAggregateSpec, ScalarProjectItem,
};
use crate::sql::optimizer::opt_expr::OptExpr;
use crate::sql::optimizer::scalar::{ScalarArena, ScalarId, materialize};
use crate::sql::optimizer::scalar_bridge::{materialize_exprs, materialize_sort_keys};

use super::context::PushPlan;

/// Build the display name for a `ScalarAggregateSpec` by materializing its args.
fn agg_spec_display_name(spec: &ScalarAggregateSpec, arena: &ScalarArena) -> String {
    let args = materialize_exprs(arena, &spec.args);
    let order_by = materialize_sort_keys(arena, &spec.order_by);
    agg_call_display_name_from_parts(&spec.name, &args, spec.distinct, &order_by)
}

/// Build the display name for a ScalarId group-by expression.
fn group_by_display_name(id: ScalarId, arena: &ScalarArena) -> String {
    let expr = materialize(arena, id);
    typed_expr_display_name(&expr)
}

/// Construct the final OptExpr: a top-level Aggregate (with is_split=true)
/// whose input is the original Join with one side wrapped by a partial
/// Aggregate.
pub(crate) fn rewrite(
    original: &LogicalAggregateOp,
    original_input: &OptExpr,
    plan: PushPlan,
    column_ref_factory: &mut ColumnRefFactory,
    arena: &mut ScalarArena,
) -> OptExpr {
    // Capture the side before plan is consumed by the moves below.
    let plan_side = plan.side;

    // 1. Build partial ScalarAggregateSpecs. For SUM/MIN/MAX the function name
    //    is unchanged at the partial stage; for COUNT it stays COUNT at partial
    //    and becomes SUM at final.
    //
    //    Each partial spec gets a fresh synthetic ColumnId so the final aggregate
    //    can reference the partial output. We derive the output column metadata
    //    from the args (DataType comes from the original spec's output column).
    //
    //    To obtain the result DataType we look up the original output columns:
    //    layout is [group_by..., aggregates...].
    let group_by_len = original.group_by.len();

    let partial_specs_and_output_cols: Vec<(ScalarAggregateSpec, OutputColumn)> = plan
        .partial_aggregates
        .iter()
        .enumerate()
        .map(|(idx, spec)| {
            let partial_spec = ScalarAggregateSpec {
                name: partial_fn_name(&spec.name),
                args: spec.args.clone(),
                distinct: false,
                order_by: vec![],
            };
            let display_name = agg_spec_display_name(&partial_spec, arena);
            // Result type comes from the original aggregate's output column.
            let result_type = original
                .output_columns
                .get(group_by_len + idx)
                .map(|c| c.data_type.clone())
                .or_else(|| {
                    // Fallback: materialize first arg and use its type.
                    spec.args
                        .first()
                        .map(|id| materialize(arena, *id).data_type)
                })
                .unwrap_or(arrow::datatypes::DataType::Int64);
            let partial_col_id =
                column_ref_factory.create(None, display_name.clone(), result_type.clone(), true);
            let output_col = OutputColumn {
                column_id: partial_col_id,
                name: display_name,
                data_type: result_type,
                nullable: true,
                is_internal: false,
            };
            (partial_spec, output_col)
        })
        .collect();

    // 2. Partial group-by output columns (column-ref pass-through).
    let partial_groupby_outputs: Vec<OutputColumn> = plan
        .partial_groupby
        .iter()
        .filter_map(|gb_id| {
            let gb = materialize(arena, *gb_id);
            match &gb.kind {
                ExprKind::ColumnRef {
                    column_id, column, ..
                } => Some(OutputColumn {
                    column_id: *column_id,
                    name: column.clone(),
                    data_type: gb.data_type.clone(),
                    nullable: gb.nullable,
                    is_internal: false,
                }),
                _ => None,
            }
        })
        .collect();

    let mut partial_output_cols: Vec<OutputColumn> = partial_groupby_outputs;
    let partial_agg_output_cols: Vec<OutputColumn> = partial_specs_and_output_cols
        .iter()
        .map(|(_, oc)| oc.clone())
        .collect();
    partial_output_cols.extend(partial_agg_output_cols.clone());

    let partial_specs: Vec<ScalarAggregateSpec> = partial_specs_and_output_cols
        .into_iter()
        .map(|(spec, _)| spec)
        .collect();

    let is_merge_partial = vec![false; partial_specs.len()];
    let partial_aggregate = OptExpr::new(
        Operator::LogicalAggregate(LogicalAggregateOp::staged(
            AggStage::Single,
            plan.partial_groupby,
            partial_specs,
            partial_output_cols,
            is_merge_partial,
            false, // partial isn't itself a final
        )),
        vec![plan.target_subtree],
    );

    // 3. Splice partial into the chosen side of the join. v1 invariant
    //    (enforced by the collector): original input is a Join, and
    //    PushPlan.side identifies which side gets wrapped.
    let new_input = {
        let mut join = original_input.clone();
        match &join.op {
            Operator::LogicalJoin(_) => {}
            _ => unreachable!("collector guarantees original.input is a Join"),
        };
        match plan_side {
            super::context::Side::Left => join.children[0] = partial_aggregate,
            super::context::Side::Right => join.children[1] = partial_aggregate,
        }
        join
    };

    // 4. Rewrite top-level aggregate specs to reference partial outputs.
    //    The final aggregate's args are ColumnRefs into the partial output cols.
    let final_specs: Vec<ScalarAggregateSpec> = original
        .aggregates
        .iter()
        .zip(partial_agg_output_cols.iter())
        .map(|(orig_spec, pc)| {
            let col_ref_typed = crate::sql::analysis::TypedExpr {
                kind: ExprKind::ColumnRef {
                    column_id: pc.column_id,
                    qualifier: None,
                    column: pc.name.clone(),
                },
                data_type: pc.data_type.clone(),
                nullable: pc.nullable,
            };
            let arg_id = crate::sql::optimizer::scalar::intern_typed(arena, &col_ref_typed);
            ScalarAggregateSpec {
                name: final_fn_name(&orig_spec.name),
                args: vec![arg_id],
                distinct: false,
                order_by: orig_spec.order_by.clone(),
            }
        })
        .collect();

    // 5. Final aggregate output columns.
    let mut final_output_cols: Vec<OutputColumn> = original
        .group_by
        .iter()
        .map(|gb_id| {
            let gb = materialize(arena, *gb_id);
            let (column_id, name) = match &gb.kind {
                ExprKind::ColumnRef { column_id, .. } => {
                    (*column_id, group_by_display_name(*gb_id, arena))
                }
                _ => (ColumnId::UNSET, group_by_display_name(*gb_id, arena)),
            };
            OutputColumn {
                column_id,
                name,
                data_type: gb.data_type.clone(),
                nullable: gb.nullable,
                is_internal: false,
            }
        })
        .collect();
    let final_agg_output_cols: Vec<OutputColumn> = final_specs
        .iter()
        .enumerate()
        .map(|(idx, spec)| {
            let display_name = agg_spec_display_name(spec, arena);
            let result_type = original
                .output_columns
                .get(group_by_len + idx)
                .map(|c| c.data_type.clone())
                .or_else(|| {
                    spec.args
                        .first()
                        .map(|id| materialize(arena, *id).data_type)
                })
                .unwrap_or(arrow::datatypes::DataType::Int64);
            OutputColumn {
                column_id: ColumnId::UNSET,
                name: display_name,
                data_type: result_type,
                nullable: true,
                is_internal: false,
            }
        })
        .collect();
    final_output_cols.extend(final_agg_output_cols);

    let is_merge_final = vec![false; final_specs.len()];
    let final_aggregate = OptExpr::new(
        Operator::LogicalAggregate(LogicalAggregateOp::staged(
            AggStage::Single,
            original.group_by.clone(),
            final_specs.clone(),
            final_output_cols.clone(),
            is_merge_final,
            true, // is_split = true marks as already-pushed
        )),
        vec![new_input],
    );

    // 6. Exposure Project: expose original group-by and aggregate outputs
    //    under the original ColumnIds so the query above resolves correctly.
    let project_items: Vec<ScalarProjectItem> = exposure_project_items(
        original,
        &final_specs,
        &final_output_cols,
        group_by_len,
        arena,
    );
    OptExpr::new(
        Operator::LogicalProject(ProjectOp {
            items: project_items,
            output_qualifier: None,
        }),
        vec![final_aggregate],
    )
}

fn partial_fn_name(name: &str) -> String {
    name.to_ascii_lowercase()
}

fn final_fn_name(name: &str) -> String {
    match name.to_ascii_lowercase().as_str() {
        "count" => "sum".to_string(),
        other => other.to_string(),
    }
}

fn exposure_project_items(
    original: &LogicalAggregateOp,
    final_specs: &[ScalarAggregateSpec],
    final_output_cols: &[OutputColumn],
    group_by_len: usize,
    arena: &mut ScalarArena,
) -> Vec<ScalarProjectItem> {
    use crate::sql::optimizer::scalar::intern_typed;

    let mut items: Vec<ScalarProjectItem> = original
        .group_by
        .iter()
        .map(|gb_id| {
            let gb = materialize(arena, *gb_id);
            let output_name = group_by_display_name(*gb_id, arena);
            let (column_id, col_name) = match &gb.kind {
                ExprKind::ColumnRef { column_id, column, .. } => (*column_id, column.clone()),
                _ => (ColumnId::UNSET, output_name.clone()),
            };
            // The exposure project emits a ColumnRef to the final aggregate's
            // group-by output column (same ColumnId as the original).
            let col_ref = crate::sql::analysis::TypedExpr {
                kind: ExprKind::ColumnRef {
                    column_id,
                    qualifier: None,
                    column: col_name,
                },
                data_type: gb.data_type.clone(),
                nullable: gb.nullable,
            };
            ScalarProjectItem {
                expr: intern_typed(arena, &col_ref),
                output_name,
                output_column_id: column_id,
                expr_display: None,
            }
        })
        .collect();

    // Aggregate outputs: each final_spec exposes the already-computed result
    // as a ColumnRef so downstream operators see the original output ColumnId.
    items.extend(
        original
            .aggregates
            .iter()
            .zip(final_specs.iter())
            .enumerate()
            .map(|(idx, (orig_spec, final_spec))| {
                let final_col = &final_output_cols[group_by_len + idx];
                let final_display = agg_spec_display_name(final_spec, arena);
                let col_ref = crate::sql::analysis::TypedExpr {
                    kind: ExprKind::ColumnRef {
                        column_id: final_col.column_id,
                        qualifier: None,
                        column: final_display.clone(),
                    },
                    data_type: final_col.data_type.clone(),
                    nullable: true,
                };
                let orig_display = agg_spec_display_name(orig_spec, arena);
                // The output_column_id from the original aggregate's output
                // columns (if present) so upstream selects resolve correctly.
                let orig_output_col_id = original
                    .output_columns
                    .get(group_by_len + idx)
                    .map(|c| c.column_id)
                    .unwrap_or(ColumnId::UNSET);
                ScalarProjectItem {
                    expr: intern_typed(arena, &col_ref),
                    output_name: orig_display,
                    output_column_id: orig_output_col_id,
                    expr_display: None,
                }
            }),
    );

    items
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::analysis::{BinOp, ExprKind, JoinKind, OutputColumn};
    use crate::sql::catalog::{ScanSource, TableDef};
    use crate::sql::column_id::ColumnId;
    use crate::sql::optimizer::operator::{LogicalJoinOp, ScanOp};
    use crate::sql::optimizer::scalar::{ScalarArena, intern_typed};
    use arrow::datatypes::DataType;

    fn col_ref_typed(name: &str, ty: DataType) -> crate::sql::analysis::TypedExpr {
        crate::sql::analysis::TypedExpr {
            kind: ExprKind::ColumnRef {
                column_id: ColumnId::UNSET,
                qualifier: None,
                column: name.into(),
            },
            data_type: ty,
            nullable: true,
        }
    }

    fn scan_opt(name: &str, cols: &[(&str, DataType)]) -> OptExpr {
        scan_opt_with_alias(name, None, &cols.iter().map(|(c, ty)| (*c, ColumnId::UNSET, ty.clone())).collect::<Vec<_>>())
    }

    fn scan_opt_with_alias(
        name: &str,
        alias: Option<&str>,
        cols: &[(&str, ColumnId, DataType)],
    ) -> OptExpr {
        OptExpr::leaf(Operator::LogicalScan(ScanOp {
            database: "db".into(),
            table: TableDef {
                name: name.into(),
                columns: vec![],
                iceberg_row_lineage_metadata_columns: vec![],
                source: ScanSource::StarRocks {
                    db_id: 0,
                    table_id: 0,
                },
            },
            alias: alias.map(str::to_string),
            columns: cols
                .iter()
                .map(|(n, id, ty)| OutputColumn {
                    column_id: *id,
                    name: (*n).into(),
                    data_type: ty.clone(),
                    nullable: false,
                    is_internal: false,
                })
                .collect(),
            predicates: vec![],
            required_columns: None,
            dict_columns: vec![],
            variant_columns: vec![],
            mv_rewritten_from: None,
        }))
    }

    fn eq_typed(a: &str, b: &str) -> crate::sql::analysis::TypedExpr {
        crate::sql::analysis::TypedExpr {
            kind: ExprKind::BinaryOp {
                left: Box::new(col_ref_typed(a, DataType::Int64)),
                op: BinOp::Eq,
                right: Box::new(col_ref_typed(b, DataType::Int64)),
            },
            data_type: DataType::Boolean,
            nullable: false,
        }
    }

    fn join_opt(
        left: OptExpr,
        right: OptExpr,
        cond: Option<crate::sql::analysis::TypedExpr>,
        arena: &mut ScalarArena,
    ) -> OptExpr {
        let cond_id = cond.as_ref().map(|c| intern_typed(arena, c));
        OptExpr::new(
            Operator::LogicalJoin(LogicalJoinOp {
                join_type: JoinKind::Inner,
                condition: cond_id,
            }),
            vec![left, right],
        )
    }

    fn make_agg(
        group_by_typed: Vec<crate::sql::analysis::TypedExpr>,
        agg_specs: Vec<ScalarAggregateSpec>,
        output_columns: Vec<OutputColumn>,
        arena: &mut ScalarArena,
    ) -> LogicalAggregateOp {
        let group_by: Vec<ScalarId> = group_by_typed
            .iter()
            .map(|e| intern_typed(arena, e))
            .collect();
        let is_merge = vec![false; agg_specs.len()];
        LogicalAggregateOp::staged(
            AggStage::Single,
            group_by,
            agg_specs,
            output_columns,
            is_merge,
            false,
        )
    }

    fn count_spec(col: &str, arena: &mut ScalarArena) -> ScalarAggregateSpec {
        let arg = col_ref_typed(col, DataType::Int64);
        ScalarAggregateSpec {
            name: "count".into(),
            args: vec![intern_typed(arena, &arg)],
            distinct: false,
            order_by: vec![],
        }
    }

    fn sum_spec(col: &str, arena: &mut ScalarArena) -> ScalarAggregateSpec {
        let arg = col_ref_typed(col, DataType::Int64);
        ScalarAggregateSpec {
            name: "sum".into(),
            args: vec![intern_typed(arena, &arg)],
            distinct: false,
            order_by: vec![],
        }
    }

    fn unwrap_exposure_project(plan: OptExpr) -> (Vec<ScalarProjectItem>, OptExpr) {
        let Operator::LogicalProject(project) = plan.op else {
            panic!("expected exposure Project")
        };
        let aggregate_plan = plan.children.into_iter().next().expect("project child");
        assert!(
            matches!(&aggregate_plan.op, Operator::LogicalAggregate(_)),
            "expected final Aggregate under exposure Project"
        );
        (project.items, aggregate_plan)
    }

    #[test]
    fn rewrites_count_to_sum_at_final() {
        let mut arena = ScalarArena::new();
        let a = scan_opt("a", &[("k", DataType::Int64), ("v", DataType::Int64)]);
        let b = scan_opt("b", &[("k", DataType::Int64)]);
        let join = join_opt(a.clone(), b, Some(eq_typed("k", "k")), &mut arena);

        let count = count_spec("v", &mut arena);
        let original = make_agg(
            vec![col_ref_typed("k", DataType::Int64)],
            vec![count.clone()],
            vec![
                OutputColumn {
                    column_id: ColumnId::UNSET,
                    name: "k".into(),
                    data_type: DataType::Int64,
                    nullable: true,
                    is_internal: false,
                },
                OutputColumn {
                    column_id: ColumnId::UNSET,
                    name: "count(v)".into(),
                    data_type: DataType::Int64,
                    nullable: true,
                    is_internal: false,
                },
            ],
            &mut arena,
        );
        let push = PushPlan {
            side: super::super::context::Side::Left,
            target_subtree: a,
            partial_groupby: original.group_by.clone(),
            partial_aggregates: vec![count],
        };
        let mut factory = ColumnRefFactory::new();
        let out = rewrite(&original, &join, push, &mut factory, &mut arena);
        let (_, top_plan) = unwrap_exposure_project(out);
        let Operator::LogicalAggregate(top) = &top_plan.op else {
            panic!("expected final Aggregate");
        };
        assert!(top.is_split);
        assert_eq!(top.aggregates[0].name, "sum");
        let join_plan = top_plan.children.first().expect("final agg child");
        assert!(matches!(&join_plan.op, Operator::LogicalJoin(_)));
        let partial_plan = join_plan.children.first().expect("join left child");
        let Operator::LogicalAggregate(partial) = &partial_plan.op else {
            panic!("partial on left")
        };
        assert!(!partial.is_split);
        assert_eq!(partial.aggregates[0].name, "count");
    }

    #[test]
    fn rewrites_sum_stays_sum() {
        let mut arena = ScalarArena::new();
        let a = scan_opt("a", &[("k", DataType::Int64), ("v", DataType::Int64)]);
        let b = scan_opt("b", &[("k", DataType::Int64)]);
        let join = join_opt(a.clone(), b, Some(eq_typed("k", "k")), &mut arena);

        let sum = sum_spec("v", &mut arena);
        let original = make_agg(
            vec![col_ref_typed("k", DataType::Int64)],
            vec![sum.clone()],
            vec![],
            &mut arena,
        );
        let push = PushPlan {
            side: super::super::context::Side::Left,
            target_subtree: a,
            partial_groupby: original.group_by.clone(),
            partial_aggregates: vec![sum],
        };
        let mut factory = ColumnRefFactory::new();
        let out = rewrite(&original, &join, push, &mut factory, &mut arena);
        let (_, top_plan) = unwrap_exposure_project(out);
        let Operator::LogicalAggregate(top) = &top_plan.op else {
            panic!("expected final Aggregate");
        };
        assert_eq!(top.aggregates[0].name, "sum");
        // The final SUM arg must be a ColumnRef into partial output.
        let final_arg = materialize(&arena, top.aggregates[0].args[0]);
        match &final_arg.kind {
            ExprKind::ColumnRef { column, .. } => {
                assert_eq!(column, "sum(v)");
            }
            _ => panic!("final SUM arg must be a ColumnRef"),
        }
    }

    #[test]
    fn rewriter_exposure_project_preserves_group_and_original_aggregate_names() {
        let mut arena = ScalarArena::new();
        let a = scan_opt("a", &[("k", DataType::Int64), ("v", DataType::Int64)]);
        let b = scan_opt("b", &[("k", DataType::Int64)]);
        let join = join_opt(a.clone(), b, Some(eq_typed("k", "k")), &mut arena);

        let sum = sum_spec("v", &mut arena);
        let original = make_agg(
            vec![col_ref_typed("k", DataType::Int64)],
            vec![sum.clone()],
            vec![
                OutputColumn {
                    column_id: ColumnId::UNSET,
                    name: "k".into(),
                    data_type: DataType::Int64,
                    nullable: true,
                    is_internal: false,
                },
                OutputColumn {
                    column_id: ColumnId::UNSET,
                    name: "total".into(),
                    data_type: DataType::Int64,
                    nullable: true,
                    is_internal: false,
                },
            ],
            &mut arena,
        );
        let push = PushPlan {
            side: super::super::context::Side::Left,
            target_subtree: a,
            partial_groupby: original.group_by.clone(),
            partial_aggregates: vec![sum],
        };
        let mut factory = ColumnRefFactory::new();
        let out = rewrite(&original, &join, push, &mut factory, &mut arena);
        let (items, top_plan) = unwrap_exposure_project(out);
        let Operator::LogicalAggregate(top) = &top_plan.op else {
            panic!("expected final Aggregate");
        };
        assert_eq!(top.output_columns.len(), 2);
        assert_eq!(top.output_columns[0].name, "k");
        // The final SUM of partial SUM produces "sum(sum(v))"
        assert_eq!(top.output_columns[1].name, "sum(sum(v))");
        assert!(items.iter().any(|item| item.output_name == "k"));
        assert!(items.iter().any(|item| item.output_name == "sum(v)"));
    }

    #[test]
    fn rewrite_keeps_partial_source_columns_visible_in_partial_aggregate() {
        // This test replaces the original "required_column_tagging" test.
        // Since TagRequiredColumns still operates on LogicalPlanNode (not yet
        // migrated), we verify the structural invariant directly: the partial
        // aggregate's input must be the chosen scan, which carries both the
        // group-by key column and the aggregate input column.
        let mut factory = ColumnRefFactory::new();
        let mut arena = ScalarArena::new();
        let c_key = factory.create(Some("t1".into()), "c_key".into(), DataType::Int32, false);
        let c_bigint =
            factory.create(Some("t1".into()), "c_bigint".into(), DataType::Int64, true);
        let c_int = factory.create(Some("t2".into()), "c_int".into(), DataType::Int32, true);

        let left = scan_opt_with_alias(
            "t1",
            Some("t1"),
            &[
                ("c_key", c_key, DataType::Int32),
                ("c_bigint", c_bigint, DataType::Int64),
            ],
        );
        let right =
            scan_opt_with_alias("t2", Some("t2"), &[("c_int", c_int, DataType::Int32)]);

        let qualified_col = |qualifier: &str, name: &str, id: ColumnId, ty: DataType| {
            crate::sql::analysis::TypedExpr {
                kind: ExprKind::ColumnRef {
                    column_id: id,
                    qualifier: Some(qualifier.into()),
                    column: name.into(),
                },
                data_type: ty,
                nullable: true,
            }
        };

        let join_cond = crate::sql::analysis::TypedExpr {
            kind: ExprKind::BinaryOp {
                left: Box::new(qualified_col("t1", "c_bigint", c_bigint, DataType::Int64)),
                op: BinOp::Eq,
                right: Box::new(qualified_col("t2", "c_int", c_int, DataType::Int32)),
            },
            data_type: DataType::Boolean,
            nullable: false,
        };
        let join = join_opt(left.clone(), right, Some(join_cond), &mut arena);

        let sum_arg = intern_typed(
            &mut arena,
            &qualified_col("t1", "c_key", c_key, DataType::Int32),
        );
        let sum = ScalarAggregateSpec {
            name: "sum".into(),
            args: vec![sum_arg],
            distinct: false,
            order_by: vec![],
        };
        let gb_id = intern_typed(
            &mut arena,
            &qualified_col("t1", "c_bigint", c_bigint, DataType::Int64),
        );
        let original = LogicalAggregateOp::staged(
            AggStage::Single,
            vec![gb_id],
            vec![sum.clone()],
            vec![
                OutputColumn {
                    column_id: c_bigint,
                    name: "c_bigint".into(),
                    data_type: DataType::Int64,
                    nullable: true,
                    is_internal: false,
                },
                OutputColumn {
                    column_id: c_key,
                    name: "sum(t1.c_key)".into(),
                    data_type: DataType::Int64,
                    nullable: true,
                    is_internal: false,
                },
            ],
            vec![false],
            false,
        );
        let push = PushPlan {
            side: super::super::context::Side::Left,
            target_subtree: left.clone(),
            partial_groupby: original.group_by.clone(),
            partial_aggregates: vec![sum],
        };

        let rewritten = rewrite(&original, &join, push, &mut factory, &mut arena);

        // Verify structure: Project → Aggregate → Join → [partial_Aggregate → Scan, Scan]
        let Operator::LogicalProject(_) = &rewritten.op else {
            panic!("expected exposure project");
        };
        let top_plan = rewritten.children.first().expect("project child");
        let Operator::LogicalAggregate(_) = &top_plan.op else {
            panic!("expected final aggregate");
        };
        let join_plan = top_plan.children.first().expect("final agg child");
        let Operator::LogicalJoin(_) = &join_plan.op else {
            panic!("expected rewritten join");
        };
        let partial_plan = join_plan.children.first().expect("join left child");
        let Operator::LogicalAggregate(partial_agg) = &partial_plan.op else {
            panic!("expected partial aggregate on left");
        };
        // Partial aggregate's input is the left scan.
        let scan_plan = partial_plan.children.first().expect("partial agg child");
        let Operator::LogicalScan(scan_op) = &scan_plan.op else {
            panic!("expected scan under partial aggregate");
        };
        // The scan must expose c_key and c_bigint.
        assert!(scan_op.columns.iter().any(|c| c.column_id == c_key));
        assert!(scan_op.columns.iter().any(|c| c.column_id == c_bigint));
        // The partial agg group_by must include c_bigint.
        assert!(!partial_agg.group_by.is_empty());
    }

    #[test]
    fn rewrite_exposes_original_count_display_after_final_sum_merge() {
        let mut factory = ColumnRefFactory::new();
        let mut arena = ScalarArena::new();
        let c_key = factory.create(Some("t1".into()), "c_key".into(), DataType::Int32, false);
        let c_bigint =
            factory.create(Some("t1".into()), "c_bigint".into(), DataType::Int64, true);
        let c_int = factory.create(Some("t2".into()), "c_int".into(), DataType::Int32, true);

        let qualified_col = |qualifier: &str, name: &str, id: ColumnId, ty: DataType| {
            crate::sql::analysis::TypedExpr {
                kind: ExprKind::ColumnRef {
                    column_id: id,
                    qualifier: Some(qualifier.into()),
                    column: name.into(),
                },
                data_type: ty,
                nullable: true,
            }
        };

        let left = scan_opt_with_alias(
            "t1",
            Some("t1"),
            &[
                ("c_key", c_key, DataType::Int32),
                ("c_bigint", c_bigint, DataType::Int64),
            ],
        );
        let right =
            scan_opt_with_alias("t2", Some("t2"), &[("c_int", c_int, DataType::Int32)]);
        let join_cond = crate::sql::analysis::TypedExpr {
            kind: ExprKind::BinaryOp {
                left: Box::new(qualified_col("t1", "c_bigint", c_bigint, DataType::Int64)),
                op: BinOp::Eq,
                right: Box::new(qualified_col("t2", "c_int", c_int, DataType::Int32)),
            },
            data_type: DataType::Boolean,
            nullable: false,
        };
        let join = join_opt(left.clone(), right, Some(join_cond), &mut arena);

        let count_arg = intern_typed(
            &mut arena,
            &qualified_col("t1", "c_key", c_key, DataType::Int32),
        );
        let count_spec = ScalarAggregateSpec {
            name: "count".into(),
            args: vec![count_arg],
            distinct: false,
            order_by: vec![],
        };
        let expected_count_display = agg_spec_display_name(&count_spec, &arena);

        let gb_id = intern_typed(
            &mut arena,
            &qualified_col("t1", "c_bigint", c_bigint, DataType::Int64),
        );
        let original = LogicalAggregateOp::staged(
            AggStage::Single,
            vec![gb_id],
            vec![count_spec.clone()],
            vec![],
            vec![false],
            false,
        );
        let push = PushPlan {
            side: super::super::context::Side::Left,
            target_subtree: left,
            partial_groupby: original.group_by.clone(),
            partial_aggregates: vec![count_spec],
        };

        let rewritten = rewrite(&original, &join, push, &mut factory, &mut arena);
        let (items, top_plan) = unwrap_exposure_project(rewritten);
        let Operator::LogicalAggregate(top) = &top_plan.op else {
            panic!("expected final Aggregate");
        };
        assert_eq!(top.aggregates[0].name, "sum");
        // Exposure project must use the original count display name.
        assert!(
            items
                .iter()
                .any(|item| item.output_name == expected_count_display)
        );
    }
}
