//! IMV apply-key projection rule.
//!
//! Wraps the rewrite plan root in a `Project` that appends the apply-key column
//! `__nova_base_row_id` derived from the internal `_row_id` column. The merge
//! sink reads this column by name to locate target rows for DELETE. Fires once
//! at the root; idempotent.

use std::sync::atomic::{AtomicBool, Ordering};

use crate::engine::mv::iceberg_target_apply::ICEBERG_MV_APPLY_KEY_COLUMN;
use crate::sql::analysis::{ExprKind, ProjectItem, TypedExpr};
use crate::sql::column_id::ColumnId;
use crate::sql::optimizer::rewrite::context::RewriteContext;
use crate::sql::optimizer::rewrite::imv::action_propagation::descendant_internal_columns;
use crate::sql::optimizer::rewrite::imv::row_id_column::ImvRowIdColumn;
use crate::sql::optimizer::rewrite::phase::RewritePhase;
use crate::sql::optimizer::rewrite::result::RewriteResult;
use crate::sql::optimizer::rewrite::rule::{LogicalRewriteRule, RewriteTraversal};
use crate::sql::planner::plan::{LogicalPlan, ProjectNode};

pub(crate) struct InjectApplyKeyProjectRule {
    fired: AtomicBool,
}

impl InjectApplyKeyProjectRule {
    pub(crate) fn new() -> Self {
        Self {
            fired: AtomicBool::new(false),
        }
    }
}

/// Find the propagated `_row_id` column id from the plan's effective output,
/// walking Project items first then descendant scans.
fn root_row_id_ref(plan: &LogicalPlan) -> Option<(ColumnId, String)> {
    if let LogicalPlan::Project(p) = plan {
        if let Some(item) = p
            .items
            .iter()
            .find(|i| i.output_name.eq_ignore_ascii_case(ImvRowIdColumn::NAME))
        {
            if let ExprKind::ColumnRef { column_id, column, .. } = &item.expr.kind {
                return Some((*column_id, column.clone()));
            }
        }
    }
    descendant_internal_columns(plan)
        .into_iter()
        .find(|c| ImvRowIdColumn::matches(c))
        .map(|c| (c.column_id, c.name))
}

fn output_has_apply_key(plan: &LogicalPlan) -> bool {
    match plan {
        LogicalPlan::Project(p) => p
            .items
            .iter()
            .any(|i| i.output_name.eq_ignore_ascii_case(ICEBERG_MV_APPLY_KEY_COLUMN)),
        _ => false,
    }
}

impl LogicalRewriteRule for InjectApplyKeyProjectRule {
    fn name(&self) -> &'static str {
        "InjectApplyKeyProject"
    }

    fn phase(&self) -> RewritePhase {
        RewritePhase::SemanticRewrite
    }

    fn traversal(&self) -> RewriteTraversal {
        RewriteTraversal::TopDown
    }

    fn matches(&self, plan: &LogicalPlan, _ctx: &RewriteContext) -> bool {
        if self.fired.load(Ordering::SeqCst) {
            return false;
        }
        root_row_id_ref(plan).is_some() && !output_has_apply_key(plan)
    }

    fn apply(&self, plan: LogicalPlan, _ctx: &mut RewriteContext) -> Result<RewriteResult, String> {
        self.fired.store(true, Ordering::SeqCst);
        let Some((row_id_col, row_id_name)) = root_row_id_ref(&plan) else {
            return Ok(RewriteResult::Unchanged);
        };
        let apply_item = ProjectItem {
            expr: TypedExpr {
                kind: ExprKind::ColumnRef {
                    column_id: row_id_col,
                    qualifier: None,
                    column: row_id_name,
                },
                data_type: arrow::datatypes::DataType::Int64,
                nullable: false,
            },
            output_name: ICEBERG_MV_APPLY_KEY_COLUMN.to_string(),
        };
        match plan {
            LogicalPlan::Project(mut p) => {
                p.items.push(apply_item);
                Ok(RewriteResult::Changed(LogicalPlan::Project(p)))
            }
            // Defensive wrap for non-Project roots. Projection/filter MVs always
            // plan a root Project; if a future shape reaches this arm it will lose
            // visible output and V6/V4 validation will reject it with a clear error.
            other => Ok(RewriteResult::Changed(LogicalPlan::Project(ProjectNode {
                input: Box::new(other),
                items: vec![apply_item],
            }))),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::AtomicU32;

    use arrow::datatypes::DataType;

    use super::*;
    use crate::engine::mv::iceberg_target_apply::ICEBERG_MV_APPLY_KEY_COLUMN;
    use crate::engine::mv::refresh_context::tests_support::dummy_rewrite_context;
    use crate::sql::analysis::{ExprKind, OutputColumn, ProjectItem, TypedExpr};
    use crate::sql::catalog::{ColumnDef, IcebergSchemaDef, IcebergTableInfo, ScanSource, TableDef};
    use crate::sql::column_id::ColumnId;
    use crate::sql::optimizer::rewrite::context::RewriteContext;
    use crate::sql::optimizer::rewrite::imv::annotation::{ImvExtension, ImvPlanAnnotation};
    use crate::sql::optimizer::rewrite::imv::row_id_column::ImvRowIdColumn;
    use crate::sql::optimizer::rewrite::result::RewriteResult;
    use crate::sql::optimizer::rewrite::rule::LogicalRewriteRule;
    use crate::sql::planner::plan::{LogicalPlan, ProjectNode, ScanNode};

    fn build_ctx() -> RewriteContext {
        let mut ctx = RewriteContext::for_mv_refresh(Vec::new());
        ctx.set_extension::<ImvExtension>(ImvExtension {
            mv_ctx: dummy_rewrite_context(),
            annotation: ImvPlanAnnotation::default(),
            next_column_id: Arc::new(AtomicU32::new(200)),
        });
        ctx
    }

    fn delta_scan_with_row_id(row_id: ColumnId) -> ScanNode {
        ScanNode {
            database: "db".to_string(),
            table: TableDef {
                name: "b".to_string(),
                columns: vec![ColumnDef {
                    name: "k".to_string(),
                    data_type: DataType::Int64,
                    nullable: false,
                    write_default: None,
                    logical_type: None,
                }],
                iceberg_row_lineage_metadata_columns: Vec::new(),
                source: ScanSource::IcebergDeltaTable {
                    table: IcebergTableInfo {
                        catalog: "ice".to_string(),
                        namespace: "db".to_string(),
                        table: "b".to_string(),
                        table_uuid: Some("uuid-b".to_string()),
                        current_snapshot_id: Some(22),
                        schema_id: 7,
                        location: "file:///tmp/ice/db/b".to_string(),
                        schema: IcebergSchemaDef { fields: Vec::new() },
                        serialized_metadata: None,
                    },
                    from_snapshot_id: 11,
                    to_snapshot_id: 22,
                },
            },
            alias: None,
            columns: vec![
                OutputColumn {
                    column_id: ColumnId(1),
                    name: "k".to_string(),
                    data_type: DataType::Int64,
                    nullable: false,
                    is_internal: false,
                },
                ImvRowIdColumn::output_column(row_id),
            ],
            predicates: Vec::new(),
            required_columns: None,
            dict_columns: Vec::new(),
        }
    }

    fn project_root(scan: ScanNode, row_id: ColumnId) -> LogicalPlan {
        // Project carrying user col k + propagated _row_id (as Task 3 would leave it).
        LogicalPlan::Project(ProjectNode {
            input: Box::new(LogicalPlan::Scan(scan)),
            items: vec![
                ProjectItem {
                    expr: TypedExpr {
                        kind: ExprKind::ColumnRef {
                            column_id: ColumnId(1),
                            qualifier: None,
                            column: "k".to_string(),
                        },
                        data_type: DataType::Int64,
                        nullable: false,
                    },
                    output_name: "k".to_string(),
                },
                ProjectItem {
                    expr: TypedExpr {
                        kind: ExprKind::ColumnRef {
                            column_id: row_id,
                            qualifier: None,
                            column: "_row_id".to_string(),
                        },
                        data_type: DataType::Int64,
                        nullable: false,
                    },
                    output_name: "_row_id".to_string(),
                },
            ],
        })
    }

    #[test]
    fn wraps_root_with_apply_key_project() {
        let rule = InjectApplyKeyProjectRule::new();
        let mut ctx = build_ctx();
        let plan = project_root(delta_scan_with_row_id(ColumnId(101)), ColumnId(101));
        assert!(rule.matches(&plan, &ctx));
        let RewriteResult::Changed(LogicalPlan::Project(root)) =
            rule.apply(plan, &mut ctx).expect("apply")
        else {
            panic!("expected Changed(Project)");
        };
        assert!(root
            .items
            .iter()
            .any(|i| i.output_name.eq_ignore_ascii_case(ICEBERG_MV_APPLY_KEY_COLUMN)));
    }

    #[test]
    fn idempotent_when_apply_key_present() {
        let rule = InjectApplyKeyProjectRule::new();
        let ctx = build_ctx();
        let mut plan = project_root(delta_scan_with_row_id(ColumnId(101)), ColumnId(101));
        if let LogicalPlan::Project(p) = &mut plan {
            p.items.push(ProjectItem {
                expr: TypedExpr {
                    kind: ExprKind::ColumnRef {
                        column_id: ColumnId(101),
                        qualifier: None,
                        column: "_row_id".to_string(),
                    },
                    data_type: DataType::Int64,
                    nullable: false,
                },
                output_name: ICEBERG_MV_APPLY_KEY_COLUMN.to_string(),
            });
        }
        assert!(!rule.matches(&plan, &ctx));
    }
}
