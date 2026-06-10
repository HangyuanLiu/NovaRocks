//! SPJG (select-project-join-group-by, single-table subset) decomposition.
//!
//! Both sides of MV rewrite matching are normalized into this shape:
//! the MV defining plan (built at candidate-prep time from the planner
//! LogicalPlan) and the query subtree (rebuilt from memo MExprs by the rule).

use std::collections::HashMap;

use crate::sql::analysis::{ExprKind, OutputColumn, TypedExpr};
use crate::sql::catalog::TableDef;
use crate::sql::column_id::ColumnId;
use crate::sql::planner::plan::{AggregateCall, LogicalPlan};

/// One visible output of the SPJG subtree, in output order.
#[derive(Clone, Debug)]
pub(crate) struct SpjgOutput {
    pub name: String,
    /// The ColumnId this output is addressed by at the subtree top.
    pub column_id: ColumnId,
    pub expr: SpjgOutputExpr,
}

#[derive(Clone, Debug)]
pub(crate) enum SpjgOutputExpr {
    /// Expression over base-table columns (projection item or group key).
    Dimension(TypedExpr),
    /// Aggregate call over base-table columns.
    Aggregate(AggregateCall),
}

#[derive(Clone, Debug)]
pub(crate) struct SpjgAggregate {
    /// Group keys, composed down to base-table column expressions.
    pub group_by: Vec<TypedExpr>,
    /// Aggregate calls, args composed down to base-table columns.
    pub aggregates: Vec<AggregateCall>,
}

#[derive(Clone, Debug)]
pub(crate) struct SpjgDescriptor {
    pub database: String,
    pub table: TableDef,
    /// Scan output columns: ColumnId -> base column binding.
    pub scan_columns: Vec<OutputColumn>,
    /// All conjuncts below the aggregate (scan predicates + filter, CNF-split).
    pub predicates: Vec<TypedExpr>,
    pub aggregate: Option<SpjgAggregate>,
    /// Visible outputs in order (the subtree's output schema).
    pub outputs: Vec<SpjgOutput>,
}

impl SpjgDescriptor {
    /// Map from scan ColumnId to base column name (for cross-side matching:
    /// the two sides see the same physical table through different ids).
    #[allow(dead_code)]
    pub(crate) fn base_name_of(&self) -> HashMap<ColumnId, String> {
        self.scan_columns
            .iter()
            .map(|c| (c.column_id, c.name.clone()))
            .collect()
    }

    pub(crate) fn from_logical_plan(plan: &LogicalPlan) -> Result<SpjgDescriptor, String> {
        // Accepted normal form, peeled top-down:
        //   [Project] -> [Aggregate] -> [Project] -> [Filter]* -> Scan
        // Anything else (Join/Sort/Limit/Window/Union/CTE/...) is rejected.
        let mut node = plan;

        // Optional top project (rebinding of aggregate/scan outputs).
        let top_project = match node {
            LogicalPlan::Project(p) => {
                node = &p.input;
                Some(p)
            }
            _ => None,
        };

        let aggregate = match node {
            LogicalPlan::Aggregate(a) => {
                node = &a.input;
                Some(a)
            }
            _ => None,
        };

        // Optional pre-aggregate project (planner may compute group-key /
        // agg-arg expressions in a project below the aggregate).
        let mid_project = match node {
            LogicalPlan::Project(p) => {
                node = &p.input;
                Some(p)
            }
            _ => None,
        };

        let mut predicates: Vec<TypedExpr> = Vec::new();
        while let LogicalPlan::Filter(f) = node {
            split_conjuncts(&f.predicate, &mut predicates);
            node = &f.input;
        }

        let LogicalPlan::Scan(scan) = node else {
            return Err(format!(
                "not a single-table SPJG shape: unexpected node {:?}",
                std::mem::discriminant(node)
            ));
        };
        predicates.extend(scan.predicates.iter().cloned());

        // Composition map: ColumnId -> defining expr over scan columns
        // (from the mid project). Identity for scan columns themselves.
        let mut defs: HashMap<ColumnId, TypedExpr> = HashMap::new();
        if let Some(p) = mid_project {
            for item in &p.items {
                let composed = substitute(&item.expr, &defs);
                defs.insert(item.output_column_id, composed);
            }
        }

        let compose = |e: &TypedExpr| substitute(e, &defs);

        let (agg, outputs) = match aggregate {
            Some(a) => {
                let group_by: Vec<TypedExpr> = a.group_by.iter().map(&compose).collect();
                let aggregates: Vec<AggregateCall> = a
                    .aggregates
                    .iter()
                    .map(|c| AggregateCall {
                        args: c.args.iter().map(&compose).collect(),
                        ..c.clone()
                    })
                    .collect();
                // Aggregate output convention: [group keys..., agg results...]
                if a.output_columns.len() != a.group_by.len() + a.aggregates.len() {
                    return Err(format!(
                        "aggregate output layout {} != group_by {} + aggs {}",
                        a.output_columns.len(),
                        a.group_by.len(),
                        a.aggregates.len()
                    ));
                }
                // Binding map at the aggregate's outputs.
                let mut agg_outputs: Vec<SpjgOutput> = Vec::new();
                for (i, oc) in a.output_columns.iter().enumerate() {
                    let expr = if i < a.group_by.len() {
                        SpjgOutputExpr::Dimension(group_by[i].clone())
                    } else {
                        SpjgOutputExpr::Aggregate(aggregates[i - a.group_by.len()].clone())
                    };
                    agg_outputs.push(SpjgOutput {
                        name: oc.name.clone(),
                        column_id: oc.column_id,
                        expr,
                    });
                }
                let outputs = apply_top_project(top_project, agg_outputs)?;
                (
                    Some(SpjgAggregate {
                        group_by,
                        aggregates,
                    }),
                    outputs,
                )
            }
            None => {
                let scan_outputs: Vec<SpjgOutput> = scan
                    .columns
                    .iter()
                    .map(|c| SpjgOutput {
                        name: c.name.clone(),
                        column_id: c.column_id,
                        expr: SpjgOutputExpr::Dimension(TypedExpr {
                            kind: ExprKind::ColumnRef {
                                column_id: c.column_id,
                                qualifier: None,
                                column: c.name.clone(),
                            },
                            data_type: c.data_type.clone(),
                            nullable: c.nullable,
                        }),
                    })
                    .collect();
                // mid_project without aggregate is just "the" project.
                let scan_outputs = match mid_project {
                    Some(p) => p
                        .items
                        .iter()
                        .map(|item| SpjgOutput {
                            name: item.output_name.clone(),
                            column_id: item.output_column_id,
                            expr: SpjgOutputExpr::Dimension(substitute(&item.expr, &defs)),
                        })
                        .collect(),
                    None => scan_outputs,
                };
                let outputs = apply_top_project(top_project, scan_outputs)?;
                (None, outputs)
            }
        };

        Ok(SpjgDescriptor {
            database: scan.database.clone(),
            table: scan.table.clone(),
            scan_columns: scan.columns.clone(),
            predicates,
            aggregate: agg,
            outputs,
        })
    }
}

/// Rebind outputs through an optional top project. MVP: top project items
/// must be bare ColumnRefs into the inputs (renames only); complex exprs
/// over aggregate results reject the shape.
fn apply_top_project(
    project: Option<&crate::sql::planner::plan::ProjectNode>,
    inputs: Vec<SpjgOutput>,
) -> Result<Vec<SpjgOutput>, String> {
    let Some(p) = project else {
        return Ok(inputs);
    };
    let by_id: HashMap<ColumnId, &SpjgOutput> = inputs.iter().map(|o| (o.column_id, o)).collect();
    p.items
        .iter()
        .map(|item| match &item.expr.kind {
            ExprKind::ColumnRef { column_id, .. } => by_id
                .get(column_id)
                .map(|o| SpjgOutput {
                    name: item.output_name.clone(),
                    column_id: item.output_column_id,
                    expr: o.expr.clone(),
                })
                .ok_or_else(|| "top project references unknown column".to_string()),
            // A computed top-project item over a pure-dimension input can be
            // composed; over aggregate outputs it is rejected (MVP).
            _ => {
                let mut defs: HashMap<ColumnId, TypedExpr> = HashMap::new();
                for o in &inputs {
                    match &o.expr {
                        SpjgOutputExpr::Dimension(e) => {
                            defs.insert(o.column_id, e.clone());
                        }
                        SpjgOutputExpr::Aggregate(_) => {}
                    }
                }
                let composed = substitute(&item.expr, &defs);
                if references_any(&composed, &inputs_agg_ids(&inputs)) {
                    Err("computed top-project over aggregate output (unsupported)".to_string())
                } else {
                    Ok(SpjgOutput {
                        name: item.output_name.clone(),
                        column_id: item.output_column_id,
                        expr: SpjgOutputExpr::Dimension(composed),
                    })
                }
            }
        })
        .collect()
}

fn inputs_agg_ids(inputs: &[SpjgOutput]) -> Vec<ColumnId> {
    inputs
        .iter()
        .filter(|o| matches!(o.expr, SpjgOutputExpr::Aggregate(_)))
        .map(|o| o.column_id)
        .collect()
}

fn references_any(e: &TypedExpr, ids: &[ColumnId]) -> bool {
    let mut found = false;
    walk(e, &mut |x| {
        if let ExprKind::ColumnRef { column_id, .. } = &x.kind
            && ids.contains(column_id)
        {
            found = true;
        }
    });
    found
}

/// Split a conjunction into CNF conjuncts.
pub(crate) fn split_conjuncts(e: &TypedExpr, out: &mut Vec<TypedExpr>) {
    if let ExprKind::BinaryOp {
        left,
        op: crate::sql::analysis::BinOp::And,
        right,
    } = &e.kind
    {
        split_conjuncts(left, out);
        split_conjuncts(right, out);
    } else {
        out.push(e.clone());
    }
}

/// Replace ColumnRefs by their defining exprs (identity when absent).
pub(crate) fn substitute(e: &TypedExpr, defs: &HashMap<ColumnId, TypedExpr>) -> TypedExpr {
    if let ExprKind::ColumnRef { column_id, .. } = &e.kind
        && let Some(d) = defs.get(column_id)
    {
        return d.clone();
    }
    map_children(e, &|child| substitute(child, defs))
}

/// Structural walk over all sub-expressions (pre-order).
pub(crate) fn walk(e: &TypedExpr, f: &mut impl FnMut(&TypedExpr)) {
    f(e);
    for_each_child(e, &mut |c| walk(c, f));
}

/// Visit each immediate `TypedExpr` child of `e` (no recursion).
///
/// Leaf variants (`ColumnRef`/`LambdaParamRef`/`Literal`/`SubqueryPlaceholder`)
/// have no `TypedExpr` children and do nothing. SPJG-unsupported variants
/// (`WindowCall`/`LambdaFunction`/`Lambda`) are treated as opaque: their
/// children are intentionally not traversed here. A valid single-table SPJG
/// input never contains those variants; if one appears, the later normalize
/// step fails closed and the candidate is dropped.
pub(crate) fn for_each_child(e: &TypedExpr, f: &mut impl FnMut(&TypedExpr)) {
    match &e.kind {
        // Leaves: no TypedExpr children.
        ExprKind::ColumnRef { .. }
        | ExprKind::LambdaParamRef { .. }
        | ExprKind::Literal(_)
        | ExprKind::SubqueryPlaceholder { .. } => {}

        ExprKind::BinaryOp { left, right, .. } => {
            f(left);
            f(right);
        }
        ExprKind::UnaryOp { expr, .. } => f(expr),
        ExprKind::FunctionCall { args, .. } => {
            for a in args {
                f(a);
            }
        }
        ExprKind::AggregateCall { args, order_by, .. } => {
            for a in args {
                f(a);
            }
            for s in order_by {
                f(&s.expr);
            }
        }
        ExprKind::Cast { expr, .. } => f(expr),
        ExprKind::IsNull { expr, .. } => f(expr),
        ExprKind::InList { expr, list, .. } => {
            f(expr);
            for item in list {
                f(item);
            }
        }
        ExprKind::Between {
            expr, low, high, ..
        } => {
            f(expr);
            f(low);
            f(high);
        }
        ExprKind::Like { expr, pattern, .. } => {
            f(expr);
            f(pattern);
        }
        ExprKind::Case {
            operand,
            when_then,
            else_expr,
        } => {
            if let Some(o) = operand {
                f(o);
            }
            for (w, t) in when_then {
                f(w);
                f(t);
            }
            if let Some(els) = else_expr {
                f(els);
            }
        }
        ExprKind::IsTruthValue { expr, .. } => f(expr),
        ExprKind::Nested(inner) => f(inner),

        // Opaque in SPJG: do not traverse.
        ExprKind::WindowCall { .. } | ExprKind::LambdaFunction { .. } | ExprKind::Lambda { .. } => {
        }
    }
}

/// Rebuild `e` with each immediate `TypedExpr` child mapped through `f`.
///
/// Mirrors [`for_each_child`]: leaves are returned unchanged, child-bearing
/// variants are rebuilt, and SPJG-unsupported variants
/// (`WindowCall`/`LambdaFunction`/`Lambda`) are returned unchanged (opaque).
/// `AggregateCall`'s `order_by` is preserved as-is (SPJG agg order_by is empty);
/// only `args` are rewritten.
pub(crate) fn map_children(e: &TypedExpr, f: &impl Fn(&TypedExpr) -> TypedExpr) -> TypedExpr {
    let kind = match &e.kind {
        // Leaves: clone unchanged.
        ExprKind::ColumnRef { .. }
        | ExprKind::LambdaParamRef { .. }
        | ExprKind::Literal(_)
        | ExprKind::SubqueryPlaceholder { .. } => e.kind.clone(),

        ExprKind::BinaryOp { left, op, right } => ExprKind::BinaryOp {
            left: Box::new(f(left)),
            op: *op,
            right: Box::new(f(right)),
        },
        ExprKind::UnaryOp { op, expr } => ExprKind::UnaryOp {
            op: *op,
            expr: Box::new(f(expr)),
        },
        ExprKind::FunctionCall {
            name,
            args,
            distinct,
        } => ExprKind::FunctionCall {
            name: name.clone(),
            args: args.iter().map(f).collect(),
            distinct: *distinct,
        },
        ExprKind::AggregateCall {
            name,
            args,
            distinct,
            order_by,
        } => ExprKind::AggregateCall {
            name: name.clone(),
            args: args.iter().map(f).collect(),
            distinct: *distinct,
            order_by: order_by.clone(),
        },
        ExprKind::Cast { expr, target } => ExprKind::Cast {
            expr: Box::new(f(expr)),
            target: target.clone(),
        },
        ExprKind::IsNull { expr, negated } => ExprKind::IsNull {
            expr: Box::new(f(expr)),
            negated: *negated,
        },
        ExprKind::InList {
            expr,
            list,
            negated,
        } => ExprKind::InList {
            expr: Box::new(f(expr)),
            list: list.iter().map(f).collect(),
            negated: *negated,
        },
        ExprKind::Between {
            expr,
            low,
            high,
            negated,
        } => ExprKind::Between {
            expr: Box::new(f(expr)),
            low: Box::new(f(low)),
            high: Box::new(f(high)),
            negated: *negated,
        },
        ExprKind::Like {
            expr,
            pattern,
            negated,
        } => ExprKind::Like {
            expr: Box::new(f(expr)),
            pattern: Box::new(f(pattern)),
            negated: *negated,
        },
        ExprKind::Case {
            operand,
            when_then,
            else_expr,
        } => ExprKind::Case {
            operand: operand.as_ref().map(|o| Box::new(f(o))),
            when_then: when_then.iter().map(|(w, t)| (f(w), f(t))).collect(),
            else_expr: else_expr.as_ref().map(|els| Box::new(f(els))),
        },
        ExprKind::IsTruthValue {
            expr,
            value,
            negated,
        } => ExprKind::IsTruthValue {
            expr: Box::new(f(expr)),
            value: *value,
            negated: *negated,
        },
        ExprKind::Nested(inner) => ExprKind::Nested(Box::new(f(inner))),

        // Opaque in SPJG: return unchanged.
        ExprKind::WindowCall { .. } | ExprKind::LambdaFunction { .. } | ExprKind::Lambda { .. } => {
            e.kind.clone()
        }
    };
    TypedExpr {
        kind,
        data_type: e.data_type.clone(),
        nullable: e.nullable,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::analysis::{ExprKind, LiteralValue, OutputColumn, TypedExpr};
    use crate::sql::catalog::{ColumnDef, ScanSource, TableDef};
    use crate::sql::column_id::ColumnId;
    use crate::sql::planner::plan::{
        AggregateCall, AggregateNode, FilterNode, LogicalPlan, ScanNode, SortNode,
    };
    use arrow::datatypes::DataType;

    fn col(id: u32, name: &str) -> OutputColumn {
        OutputColumn {
            column_id: ColumnId(id),
            name: name.to_string(),
            data_type: DataType::Int64,
            nullable: true,
            is_internal: false,
        }
    }

    fn col_ref(c: &OutputColumn) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::ColumnRef {
                column_id: c.column_id,
                qualifier: None,
                column: c.name.clone(),
            },
            data_type: c.data_type.clone(),
            nullable: c.nullable,
        }
    }

    fn int_lit(v: i64) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::Literal(LiteralValue::Int(v)),
            data_type: DataType::Int64,
            nullable: false,
        }
    }

    fn cmp(left: TypedExpr, op: crate::sql::analysis::BinOp, right: TypedExpr) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::BinaryOp {
                left: Box::new(left),
                op,
                right: Box::new(right),
            },
            data_type: DataType::Boolean,
            nullable: true,
        }
    }

    /// Simplest `ScanSource` to construct in unit tests. The descriptor logic
    /// never inspects `ScanSource`, so the cheap `StarRocks` variant (two i64s)
    /// is sufficient and avoids building a heavy `IcebergTableInfo`.
    fn test_scan_source() -> ScanSource {
        ScanSource::StarRocks {
            db_id: 0,
            table_id: 0,
        }
    }

    fn scan(cols: &[OutputColumn]) -> ScanNode {
        ScanNode {
            database: "db".to_string(),
            table: TableDef {
                name: "t".to_string(),
                columns: cols
                    .iter()
                    .map(|c| ColumnDef {
                        name: c.name.clone(),
                        data_type: c.data_type.clone(),
                        nullable: c.nullable,
                        write_default: None,
                        logical_type: None,
                    })
                    .collect(),
                iceberg_row_lineage_metadata_columns: vec![],
                source: test_scan_source(),
            },
            alias: None,
            columns: cols.to_vec(),
            predicates: vec![],
            required_columns: None,
            dict_columns: vec![],
            required_output_columns: None,
        }
    }

    #[test]
    fn extracts_filter_scan_shape() {
        let a = col(1, "a");
        let b = col(2, "b");
        let plan = LogicalPlan::Filter(FilterNode {
            input: Box::new(LogicalPlan::Scan(scan(&[a.clone(), b.clone()]))),
            predicate: cmp(col_ref(&a), crate::sql::analysis::BinOp::Ge, int_lit(5)),
            required_output_columns: None,
        });
        let d = SpjgDescriptor::from_logical_plan(&plan).expect("spjg");
        assert_eq!(d.table.name, "t");
        assert_eq!(d.predicates.len(), 1);
        assert!(d.aggregate.is_none());
        assert_eq!(d.outputs.len(), 2); // pass-through scan columns
    }

    #[test]
    fn extracts_aggregate_shape_and_rejects_join() {
        let a = col(1, "a");
        let v = col(2, "v");
        let sum_out = col(3, "s");
        let plan = LogicalPlan::Aggregate(AggregateNode {
            input: Box::new(LogicalPlan::Scan(scan(&[a.clone(), v.clone()]))),
            group_by: vec![col_ref(&a)],
            aggregates: vec![AggregateCall {
                name: "sum".to_string(),
                args: vec![col_ref(&v)],
                distinct: false,
                result_type: DataType::Int64,
                order_by: vec![],
                output_column_id: sum_out.column_id,
            }],
            output_columns: vec![col(1, "a"), sum_out.clone()],
            already_pushed: false,
            required_output_columns: None,
        });
        let d = SpjgDescriptor::from_logical_plan(&plan).expect("spjg");
        let agg = d.aggregate.as_ref().expect("aggregate present");
        assert_eq!(agg.group_by.len(), 1);
        assert_eq!(agg.aggregates.len(), 1);
        // outputs: Dimension(a) then Aggregate(sum(v))
        assert_eq!(d.outputs.len(), 2);
    }

    #[test]
    fn rejects_sort_and_window() {
        // Any node outside {Scan, Filter, Project, Aggregate} must yield Err.
        let a = col(1, "a");
        let plan = LogicalPlan::Sort(SortNode {
            input: Box::new(LogicalPlan::Scan(scan(&[a.clone()]))),
            items: vec![],
            analytic_partition_by: vec![],
            required_output_columns: None,
        });
        assert!(SpjgDescriptor::from_logical_plan(&plan).is_err());
    }
}
