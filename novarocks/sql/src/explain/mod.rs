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

//! EXPLAIN plan formatter for logical plans and shared expression formatting.

pub(crate) mod completed;
pub(crate) mod completed_tree;

use crate::compiler::SqlCompileError;
use novarocks_type_contract::{CompileCheckpoints, CompilePhase, PureCompileControl};

use crate::analysis::{
    BinOp, ExprKind, JoinKind, LiteralValue, ProjectItem, SortItem, TypedExpr, UnOp,
};
use crate::common::ApplyKind;
use crate::planner::logical::{LogicalPlanKind, LogicalPlanNode};
use crate::planner::payload::{PlanAssertOneRowNode, PlanRowCountAssertion};
use crate::planner::table::ScanSource;

/// Detail level for EXPLAIN output.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ExplainLevel {
    Normal,
    Verbose,
    Costs,
    /// Produce node-level output identical to Verbose; the
    /// Planning/Execution/Rows header is added by
    /// the frontend query-admission adapter.
    Analyze,
    /// The plan contract rather than the operators it describes.
    ///
    /// Every other level answers what the statement will do. This one answers
    /// what the plan states to the backend, which is a different question and
    /// has its own reader.
    Contract,
}

/// Format one logical plan under the original compile control.
pub(crate) fn explain_plan_checked(
    plan: &LogicalPlanNode,
    level: ExplainLevel,
    control: &dyn PureCompileControl,
) -> Result<Vec<String>, SqlCompileError> {
    format_checked(control, |work| {
        let mut out = Vec::new();
        format_node_with_work(plan, level, 0, &mut out, work)?;
        Ok(out)
    })
}

#[cfg(test)]
pub(crate) fn explain_plan(plan: &LogicalPlanNode, level: ExplainLevel) -> Vec<String> {
    explain_plan_checked(
        plan,
        level,
        &crate::compiler::SqlCompileControl::unbounded(),
    )
    .expect("invalid logical plan stage")
}

fn format_checked<T>(
    control: &dyn PureCompileControl,
    format: impl FnOnce(&mut CompileCheckpoints<'_>) -> Result<T, SqlCompileError>,
) -> Result<T, SqlCompileError> {
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::LowerProgram)?;
    let result = format(&mut work);
    if matches!(
        &result,
        Err(SqlCompileError::Cancelled
            | SqlCompileError::DeadlineExceeded
            | SqlCompileError::ResourceExhausted)
    ) {
        return result;
    }
    work.finish()?;
    result
}

// These helpers observe the work performed by this formatter. Standard library
// formatting and external type diagnostics remain opaque library operations;
// this diagnostic API is not an allocation grant or a semantic identity author.
fn append_text(
    out: &mut String,
    text: &str,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), SqlCompileError> {
    let mut start = 0;
    while start < text.len() {
        let mut end = start.saturating_add(1024).min(text.len());
        while !text.is_char_boundary(end) {
            end -= 1;
        }
        out.push_str(&text[start..end]);
        work.step()?;
        start = end;
    }
    Ok(())
}

fn copy_text(text: &str, work: &mut CompileCheckpoints<'_>) -> Result<String, SqlCompileError> {
    let mut out = String::new();
    append_text(&mut out, text, work)?;
    Ok(out)
}

fn join_text<T: AsRef<str>>(
    items: &[T],
    separator: &str,
    work: &mut CompileCheckpoints<'_>,
) -> Result<String, SqlCompileError> {
    let mut out = String::new();
    for (index, item) in items.iter().enumerate() {
        if index != 0 {
            append_text(&mut out, separator, work)?;
        }
        append_text(&mut out, item.as_ref(), work)?;
        work.step()?;
    }
    Ok(out)
}

fn uppercase_text(
    text: &str,
    work: &mut CompileCheckpoints<'_>,
) -> Result<String, SqlCompileError> {
    let mut out = String::new();
    for character in text.chars() {
        for upper in character.to_uppercase() {
            out.push(upper);
        }
        work.step()?;
    }
    Ok(out)
}

fn format_binary(
    bytes: &[u8],
    work: &mut CompileCheckpoints<'_>,
) -> Result<String, SqlCompileError> {
    const HEX: &[u8; 16] = b"0123456789ABCDEF";
    let mut out = String::from("X'");
    for byte in bytes {
        out.push(HEX[(byte >> 4) as usize] as char);
        out.push(HEX[(byte & 15) as usize] as char);
        work.step()?;
    }
    out.push('\'');
    Ok(out)
}

#[allow(dead_code)]
fn format_node_with_work(
    plan: &LogicalPlanNode,
    level: ExplainLevel,
    indent: usize,
    out: &mut Vec<String>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), SqlCompileError> {
    let pad = "  ".repeat(indent);
    match &plan.kind {
        LogicalPlanKind::Scan(node) => {
            let header = format_shared_plan_node_header_with_work(
                &plan.kind,
                PlanNodeExplainStage::Logical,
                work,
            )?
            .expect("Scan is a shared explain node");
            out.push(format!("{pad}0:{header}",));
            if let Some(ref cols) = node.required_columns
                && matches!(
                    level,
                    ExplainLevel::Verbose | ExplainLevel::Costs | ExplainLevel::Analyze
                )
            {
                let mut names = Vec::new();
                for required in cols {
                    let mut found = None;
                    for column in &node.columns {
                        let matches = column.column_id == *required;
                        work.step()?;
                        if matches {
                            found = Some(copy_text(&column.name, work)?);
                            break;
                        }
                    }
                    names.push(found.unwrap_or_else(|| format!("ColumnId({})", required.0)));
                    work.step()?;
                }
                out.push(format!(
                    "{pad}     columns: {}",
                    join_text(&names, ", ", work)?
                ));
            }
            if matches!(
                level,
                ExplainLevel::Verbose | ExplainLevel::Costs | ExplainLevel::Analyze
            ) && let Some(source) =
                logical_scan_source_label_with_work(&node.table.source, work)?
            {
                out.push(format!("{pad}     source: {source}"));
            }
            if !node.predicates.is_empty() {
                let preds: Vec<String> = node
                    .predicates
                    .iter()
                    .map(|expr| format_expr_with_work(expr, work))
                    .collect::<Result<_, SqlCompileError>>()?;
                out.push(format!(
                    "{pad}     predicates: {}",
                    join_text(&preds, " AND ", work)?
                ));
            }
        }
        LogicalPlanKind::Filter(_) => {
            let header = format_shared_plan_node_header_with_work(
                &plan.kind,
                PlanNodeExplainStage::Logical,
                work,
            )?
            .expect("Filter is a shared explain node");
            out.push(format!("{pad}{header}"));
            for line in format_shared_plan_node_detail_lines_with_work(
                &plan.kind,
                PlanNodeExplainStage::Logical,
                work,
            )? {
                out.push(format!("{pad}  {line}"));
            }
            format_node_with_work(plan.unary_input(), level, indent + 1, out, work)?;
        }
        LogicalPlanKind::Project(_) => {
            let header = format_shared_plan_node_header_with_work(
                &plan.kind,
                PlanNodeExplainStage::Logical,
                work,
            )?
            .expect("Project is a shared explain node");
            out.push(format!("{pad}{header}"));
            format_node_with_work(plan.unary_input(), level, indent + 1, out, work)?;
        }
        LogicalPlanKind::Aggregate(node) => {
            let groups: Vec<String> = node
                .group_by
                .iter()
                .map(|expr| format_expr_with_work(expr, work))
                .collect::<Result<_, SqlCompileError>>()?;
            let aggs: Vec<String> = node
                .aggregates
                .iter()
                .map(|a| {
                    let args: Vec<String> = a
                        .source
                        .arguments()
                        .iter()
                        .map(|expr| format_expr_with_work(expr, work))
                        .collect::<Result<_, SqlCompileError>>()?;
                    let distinct = if a.distinct { "DISTINCT " } else { "" };
                    let value =
                        format!("{}({}{})", a.name, distinct, join_text(&args, ", ", work)?);
                    work.step()?;
                    Ok(value)
                })
                .collect::<Result<_, SqlCompileError>>()?;
            out.push(format!("{pad}AGGREGATE"));
            if !groups.is_empty() {
                out.push(format!(
                    "{pad}  group by: {}",
                    join_text(&groups, ", ", work)?
                ));
            }
            if !aggs.is_empty() {
                out.push(format!(
                    "{pad}  aggregations: {}",
                    join_text(&aggs, ", ", work)?
                ));
            }
            format_node_with_work(plan.unary_input(), level, indent + 1, out, work)?;
        }
        LogicalPlanKind::Join(node) => {
            let join_str = match node.join_type {
                JoinKind::Inner => "INNER JOIN",
                JoinKind::LeftOuter => "LEFT OUTER JOIN",
                JoinKind::RightOuter => "RIGHT OUTER JOIN",
                JoinKind::FullOuter => "FULL OUTER JOIN",
                JoinKind::Cross => "CROSS JOIN",
                JoinKind::LeftSemi => "LEFT SEMI JOIN",
                JoinKind::RightSemi => "RIGHT SEMI JOIN",
                JoinKind::LeftAnti => "LEFT ANTI JOIN",
                JoinKind::RightAnti => "RIGHT ANTI JOIN",
                JoinKind::NullAwareLeftAnti => "NULL AWARE LEFT ANTI JOIN",
            };
            out.push(format!("{pad}{join_str}"));
            if let Some(ref cond) = node.condition {
                out.push(format!("{pad}  on: {}", format_expr_with_work(cond, work)?));
            }
            format_node_with_work(plan.left(), level, indent + 1, out, work)?;
            format_node_with_work(plan.right(), level, indent + 1, out, work)?;
        }
        LogicalPlanKind::Sort(_) => {
            let body = format_shared_plan_node_header_with_work(
                &plan.kind,
                PlanNodeExplainStage::Logical,
                work,
            )?
            .expect("Sort is a shared explain node");
            out.push(format!("{pad}{body}"));
            format_node_with_work(plan.unary_input(), level, indent + 1, out, work)?;
        }
        LogicalPlanKind::Limit(node) => {
            let mut parts = Vec::new();
            if let Some(limit) = node.limit {
                parts.push(format!("limit={limit}"));
            }
            if let Some(offset) = node.offset {
                parts.push(format!("offset={offset}"));
            }
            out.push(format!("{pad}LIMIT [{}]", join_text(&parts, ", ", work)?));
            format_node_with_work(plan.unary_input(), level, indent + 1, out, work)?;
        }
        LogicalPlanKind::Union(node) => {
            let kind = if node.all { "UNION ALL" } else { "UNION" };
            out.push(format!("{pad}{kind}"));
            for input in &plan.children {
                format_node_with_work(input, level, indent + 1, out, work)?;
            }
        }
        LogicalPlanKind::Intersect(_) => {
            out.push(format!("{pad}INTERSECT"));
            for input in &plan.children {
                format_node_with_work(input, level, indent + 1, out, work)?;
            }
        }
        LogicalPlanKind::Except(_) => {
            out.push(format!("{pad}EXCEPT"));
            for input in &plan.children {
                format_node_with_work(input, level, indent + 1, out, work)?;
            }
        }
        LogicalPlanKind::Window(_) => {
            let header = format_shared_plan_node_header_with_work(
                &plan.kind,
                PlanNodeExplainStage::Logical,
                work,
            )?
            .expect("Window is a shared explain node");
            out.push(format!("{pad}{header}"));
            format_node_with_work(plan.unary_input(), level, indent + 1, out, work)?;
        }
        LogicalPlanKind::Values(_) => {
            let body = format_shared_plan_node_header_with_work(
                &plan.kind,
                PlanNodeExplainStage::Logical,
                work,
            )?
            .expect("Values is a shared explain node");
            out.push(format!("{pad}{body}"));
        }
        LogicalPlanKind::GenerateSeries(_) => {
            let body = format_shared_plan_node_header_with_work(
                &plan.kind,
                PlanNodeExplainStage::Logical,
                work,
            )?
            .expect("GenerateSeries is a shared explain node");
            out.push(format!("{pad}{body}"));
        }
        LogicalPlanKind::TableFunction(_) => {
            let body = format_shared_plan_node_header_with_work(
                &plan.kind,
                PlanNodeExplainStage::Logical,
                work,
            )?
            .expect("TableFunction is a shared explain node");
            out.push(format!("{pad}{body}"));
            format_node_with_work(plan.unary_input(), level, indent + 1, out, work)?;
        }
        LogicalPlanKind::Repeat(_) => {
            let body = format_shared_plan_node_header_with_work(
                &plan.kind,
                PlanNodeExplainStage::Logical,
                work,
            )?
            .expect("Repeat is a shared explain node");
            out.push(format!("{pad}{body}"));
            format_node_with_work(plan.unary_input(), level, indent + 1, out, work)?;
        }
        LogicalPlanKind::CTEAnchor(node) => {
            out.push(format!("{pad}CTE_ANCHOR(cte_id={})", node.cte_id));
            format_node_with_work(plan.child(0), level, indent + 1, out, work)?;
            format_node_with_work(plan.child(1), level, indent + 1, out, work)?;
        }
        LogicalPlanKind::CTEProduce(node) => {
            out.push(format!("{pad}CTE_PRODUCE(cte_id={})", node.cte_id));
            format_node_with_work(plan.unary_input(), level, indent + 1, out, work)?;
        }
        LogicalPlanKind::CTEConsume(node) => {
            out.push(format!("{pad}CTE_CONSUME(cte_id={})", node.cte_id));
        }
        LogicalPlanKind::Apply(node) => {
            let kind = match node.kind {
                ApplyKind::Scalar => "SCALAR",
                ApplyKind::Exists { negated: false } => "EXISTS",
                ApplyKind::Exists { negated: true } => "NOT EXISTS",
                ApplyKind::In { negated: false } => "IN",
                ApplyKind::In { negated: true } => "NOT IN",
            };
            out.push(format!(
                "{pad}APPLY ({kind}, correlated={}, use_semi_anti={})",
                !node.correlation_column_ids.is_empty(),
                node.use_semi_anti
            ));
            format_node_with_work(plan.left(), level, indent + 1, out, work)?;
            format_node_with_work(plan.right(), level, indent + 1, out, work)?;
        }
        LogicalPlanKind::AssertOneRow(_) => {
            let body = format_shared_plan_node_header_with_work(
                &plan.kind,
                PlanNodeExplainStage::Logical,
                work,
            )?
            .expect("AssertOneRow is a shared explain node");
            out.push(format!("{pad}{body}"));
            format_node_with_work(plan.unary_input(), level, indent + 1, out, work)?;
        }
        LogicalPlanKind::ImvDelta(_) | LogicalPlanKind::ImvVersion(_) => {
            panic!("imv marker leaked into non-IMV plan");
        }
    }

    work.step()?;
    Ok(())
}

fn logical_scan_source_label_with_work(
    source: &ScanSource,
    work: &mut CompileCheckpoints<'_>,
) -> Result<Option<String>, SqlCompileError> {
    let value = match source {
        ScanSource::Sql(source) => match &source.kind {
            crate::planner::table::SqlScanKind::Delta {
                from_snapshot_id,
                to_snapshot_id,
            } => Some(format!(
                "IcebergDeltaTable from_snapshot_id={from_snapshot_id} to_snapshot_id={to_snapshot_id}"
            )),
            crate::planner::table::SqlScanKind::FrozenInputSet {
                version: crate::planner::table::SqlTableVersionSelector::Snapshot(snapshot_id),
            } => Some(format!("IcebergVersionTable snapshot_id={snapshot_id}")),
            crate::planner::table::SqlScanKind::FrozenInputSet {
                version:
                    crate::planner::table::SqlTableVersionSelector::TimestampMillis(timestamp_millis),
            } => Some(format!(
                "IcebergVersionTable timestamp_millis={timestamp_millis}"
            )),
            crate::planner::table::SqlScanKind::MvTargetState { facts } => Some(format!(
                "IcebergMvTargetState target={}.{}.{} keys=[{}] states=[{}] {}",
                source.table.catalog,
                source.table.namespace,
                source.table.table,
                join_text(&facts.group_key_names, ",", work)?,
                join_text(&facts.aggregate_state_names, ",", work)?,
                facts.constraint_summary()
            )),
            crate::planner::table::SqlScanKind::MvTargetLocator { facts } => Some(format!(
                "IcebergMvTargetLocator target={}.{}.{} apply_key={}{}",
                source.table.catalog,
                source.table.namespace,
                source.table.table,
                facts.apply_key_column,
                facts
                    .branch_id_column
                    .as_deref()
                    .map(|column| format!(" branch_id={column}"))
                    .unwrap_or_default()
            )),
            _ => None,
        },
    };
    work.step()?;
    Ok(value)
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum PlanNodeExplainStage {
    Logical,
    Distributed,
}

fn format_shared_plan_node_header_with_work(
    kind: &LogicalPlanKind,
    stage: PlanNodeExplainStage,
    work: &mut CompileCheckpoints<'_>,
) -> Result<Option<String>, SqlCompileError> {
    let value = match kind {
        LogicalPlanKind::Scan(node) => {
            let alias = node
                .alias
                .as_deref()
                .map(|a| format!(" (alias={a})"))
                .unwrap_or_default();
            Some(format!(
                "SCAN {}.{}{}",
                node.database, node.table.name, alias
            ))
        }
        LogicalPlanKind::Filter(_) => Some("FILTER".to_string()),
        LogicalPlanKind::Project(node) => {
            let items = node
                .items
                .iter()
                .map(|item| format_project_item_with_work(item, work))
                .collect::<Result<Vec<_>, SqlCompileError>>()?;
            Some(format!("PROJECT [{}]", join_text(&items, ", ", work)?))
        }
        LogicalPlanKind::Sort(node) => {
            let items = format_sort_items_with_work(&node.items, work)?;
            Some(format!("SORT BY [{}]", join_text(&items, ", ", work)?))
        }
        LogicalPlanKind::Window(node) => {
            let fns = format_window_exprs_with_work(&node.window_exprs, stage, work)?;
            Some(format!("WINDOW [{}]", join_text(&fns, "; ", work)?))
        }
        LogicalPlanKind::Values(node) => Some(format!("VALUES ({} rows)", node.rows.len())),
        LogicalPlanKind::Repeat(node) => Some(format!(
            "REPEAT ({} grouping sets)",
            node.grouping_ids.len()
        )),
        LogicalPlanKind::GenerateSeries(node) => Some(format!(
            "GENERATE_SERIES({}, {}, {})",
            node.start, node.end, node.step
        )),
        LogicalPlanKind::TableFunction(node) => {
            let join_type = if node.is_left_join { "LEFT" } else { "CROSS" };
            Some(format!(
                "TABLE_FUNCTION [{} {}]",
                join_type,
                uppercase_text(&node.function_name, work)?
            ))
        }
        LogicalPlanKind::AssertOneRow(node) => {
            Some(format_assert_one_row_header_with_work(node, stage, work)?)
        }
        _ => None,
    };
    work.step()?;
    Ok(value)
}

fn format_assert_one_row_header_with_work(
    node: &PlanAssertOneRowNode,
    stage: PlanNodeExplainStage,
    work: &mut CompileCheckpoints<'_>,
) -> Result<String, SqlCompileError> {
    if matches!(stage, PlanNodeExplainStage::Logical) {
        work.step()?;
        return Ok("ASSERT ONE ROW".to_string());
    }
    let relation = format_row_count_assertion(node.assertion);
    let desired = node.desired_num_rows.unwrap_or(1);
    if node.group_key_column_ids.is_empty() {
        work.step()?;
        return Ok(format!("ASSERT NUM ROWS ({relation} {desired})"));
    }
    let labels = if node.group_key_labels.is_empty() {
        node.group_key_column_ids
            .iter()
            .map(|column_id| {
                let label = format!("column_{}", column_id.0);
                work.step()?;
                Ok(label)
            })
            .collect::<Result<Vec<_>, SqlCompileError>>()?
    } else {
        node.group_key_labels
            .iter()
            .map(|label| copy_text(label, work))
            .collect::<Result<Vec<_>, SqlCompileError>>()?
    };
    let value = format!(
        "ASSERT NUM ROWS (PER KEY {relation} {desired} BY [{}])",
        join_text(&labels, ", ", work)?
    );
    work.step()?;
    Ok(value)
}

fn format_row_count_assertion(assertion: PlanRowCountAssertion) -> &'static str {
    match assertion {
        PlanRowCountAssertion::Eq => "=",
        PlanRowCountAssertion::Ne => "!=",
        PlanRowCountAssertion::Lt => "<",
        PlanRowCountAssertion::Le => "<=",
        PlanRowCountAssertion::Gt => ">",
        PlanRowCountAssertion::Ge => ">=",
    }
}

fn format_shared_plan_node_detail_lines_with_work(
    kind: &LogicalPlanKind,
    _stage: PlanNodeExplainStage,
    work: &mut CompileCheckpoints<'_>,
) -> Result<Vec<String>, SqlCompileError> {
    let value = match kind {
        LogicalPlanKind::Filter(node) => {
            vec![format!(
                "predicate: {}",
                format_expr_with_work(&node.predicate, work)?
            )]
        }
        _ => vec![],
    };
    work.step()?;
    Ok(value)
}

fn format_sort_items_with_work(
    items: &[SortItem],
    work: &mut CompileCheckpoints<'_>,
) -> Result<Vec<String>, SqlCompileError> {
    items
        .iter()
        .map(|s| {
            let dir = if s.asc { "ASC" } else { "DESC" };
            let nulls = if s.nulls_first {
                " NULLS FIRST"
            } else {
                " NULLS LAST"
            };
            let value = format!("{} {dir}{nulls}", format_expr_with_work(&s.expr, work)?);
            work.step()?;
            Ok(value)
        })
        .collect::<Result<Vec<_>, SqlCompileError>>()
}

fn format_window_exprs_with_work(
    exprs: &[crate::planner::payload::WindowExpr],
    stage: PlanNodeExplainStage,
    work: &mut CompileCheckpoints<'_>,
) -> Result<Vec<String>, SqlCompileError> {
    exprs
        .iter()
        .map(|w| {
            let args = w
                .args
                .iter()
                .map(|expr| format_expr_with_work(expr, work))
                .collect::<Result<Vec<_>, SqlCompileError>>()?;
            let value = match stage {
                PlanNodeExplainStage::Logical => {
                    let partition = w
                        .partition_by
                        .iter()
                        .map(|expr| format_expr_with_work(expr, work))
                        .collect::<Result<Vec<_>, SqlCompileError>>()?;
                    let order = w
                        .order_by
                        .iter()
                        .map(|s| {
                            let dir = if s.asc { "ASC" } else { "DESC" };
                            let value = format!("{} {dir}", format_expr_with_work(&s.expr, work)?);
                            work.step()?;
                            Ok(value)
                        })
                        .collect::<Result<Vec<_>, SqlCompileError>>()?;
                    let mut over_parts = Vec::new();
                    if !partition.is_empty() {
                        over_parts.push(format!(
                            "PARTITION BY {}",
                            join_text(&partition, ", ", work)?
                        ));
                    }
                    if !order.is_empty() {
                        over_parts.push(format!("ORDER BY {}", join_text(&order, ", ", work)?));
                    }
                    format!(
                        "{}({}) OVER ({})",
                        w.name,
                        join_text(&args, ", ", work)?,
                        join_text(&over_parts, " ", work)?
                    )
                }
                PlanNodeExplainStage::Distributed => {
                    format!("{}({})", w.name, join_text(&args, ", ", work)?)
                }
            };
            work.step()?;
            Ok(value)
        })
        .collect::<Result<Vec<_>, SqlCompileError>>()
}

fn format_expr_with_work(
    expr: &TypedExpr,
    work: &mut CompileCheckpoints<'_>,
) -> Result<String, SqlCompileError> {
    format_expr_kind_with_work(&expr.kind, work)
}

fn format_project_item_with_work(
    item: &ProjectItem,
    work: &mut CompileCheckpoints<'_>,
) -> Result<String, SqlCompileError> {
    let expr_str = format_expr_with_work(&item.expr, work)?;
    let value = if item.output_name == expr_str {
        expr_str
    } else {
        format!("{expr_str} AS {}", item.output_name)
    };
    work.step()?;
    Ok(value)
}

fn format_expr_kind_with_work(
    kind: &ExprKind,
    work: &mut CompileCheckpoints<'_>,
) -> Result<String, SqlCompileError> {
    let value = match kind {
        ExprKind::ColumnRef {
            qualifier, column, ..
        } => match qualifier {
            Some(q) => {
                let mut text = copy_text(q, work)?;
                text.push('.');
                append_text(&mut text, column, work)?;
                text
            }
            None => copy_text(column, work)?,
        },
        ExprKind::LambdaParamRef { name, .. } => copy_text(name, work)?,
        ExprKind::Constant(value) => {
            work.flush()?;
            crate::constant::format_constant_observed(value, work.control())?
        }
        ExprKind::Literal(lit) => match lit {
            LiteralValue::Null => "NULL".to_string(),
            LiteralValue::Bool(b) => b.to_string(),
            LiteralValue::Int(n) => n.to_string(),
            LiteralValue::LargeInt(n) => n.to_string(),
            LiteralValue::Float(f) => f.to_string(),
            LiteralValue::Decimal(d) => copy_text(d, work)?,
            LiteralValue::String(s) => {
                let mut out = String::from("'");
                append_text(&mut out, s, work)?;
                out.push('\'');
                out
            }
            LiteralValue::Binary(bytes) => format_binary(bytes, work)?,
        },
        ExprKind::BinaryOp {
            left, op, right, ..
        } => {
            let op_str = match op {
                BinOp::Add => "+",
                BinOp::Sub => "-",
                BinOp::Mul => "*",
                BinOp::Div => "/",
                BinOp::Mod => "%",
                BinOp::Eq => "=",
                BinOp::Ne => "!=",
                BinOp::Lt => "<",
                BinOp::Le => "<=",
                BinOp::Gt => ">",
                BinOp::Ge => ">=",
                BinOp::EqForNull => "<=>",
                BinOp::And => "AND",
                BinOp::Or => "OR",
            };
            let (display_left, display_right) = if matches!(op, BinOp::Eq | BinOp::EqForNull)
                && matches!(left.kind, ExprKind::Literal(_) | ExprKind::Constant(_))
                && matches!(right.kind, ExprKind::ColumnRef { .. })
            {
                (right.as_ref(), left.as_ref())
            } else {
                (left.as_ref(), right.as_ref())
            };
            format!(
                "{} {op_str} {}",
                format_expr_with_work(display_left, work)?,
                format_expr_with_work(display_right, work)?
            )
        }
        ExprKind::UnaryOp { op, expr } => {
            let op_str = match op {
                UnOp::Not => "NOT",
                UnOp::Negate => "-",
                UnOp::BitwiseNot => "~",
            };
            format!("{op_str} {}", format_expr_with_work(expr, work)?)
        }
        ExprKind::FunctionCall {
            name,
            args,
            distinct,
            ..
        } => {
            let args_str: Vec<String> = args
                .iter()
                .map(|expr| format_expr_with_work(expr, work))
                .collect::<Result<_, SqlCompileError>>()?;
            let distinct_str = if *distinct { "DISTINCT " } else { "" };
            format!(
                "{name}({distinct_str}{})",
                join_text(&args_str, ", ", work)?
            )
        }
        ExprKind::LambdaFunction { params, body } => {
            let mut names = Vec::new();
            for param in params {
                names.push(param.name.as_str());
                work.step()?;
            }
            let params = join_text(&names, ", ", work)?;
            format!("({params}) -> {}", format_expr_with_work(body, work)?)
        }
        ExprKind::AggregateCall {
            name,
            args,
            distinct,
            ..
        } => {
            let args_str: Vec<String> = args
                .iter()
                .map(|expr| format_expr_with_work(expr, work))
                .collect::<Result<_, SqlCompileError>>()?;
            let distinct_str = if *distinct { "DISTINCT " } else { "" };
            format!(
                "{name}({distinct_str}{})",
                join_text(&args_str, ", ", work)?
            )
        }
        ExprKind::Cast { expr, target, .. } => {
            format!("CAST({} AS {target:?})", format_expr_with_work(expr, work)?)
        }
        ExprKind::IsNull { expr, negated } => {
            let not = if *negated { " NOT" } else { "" };
            format!("{} IS{not} NULL", format_expr_with_work(expr, work)?)
        }
        ExprKind::InList {
            expr,
            list,
            negated,
        } => {
            let not = if *negated { " NOT" } else { "" };
            let items: Vec<String> = list
                .iter()
                .map(|expr| format_expr_with_work(expr, work))
                .collect::<Result<_, SqlCompileError>>()?;
            format!(
                "{}{not} IN ({})",
                format_expr_with_work(expr, work)?,
                join_text(&items, ", ", work)?
            )
        }
        ExprKind::Between {
            expr,
            low,
            high,
            negated,
        } => {
            let not = if *negated { " NOT" } else { "" };
            format!(
                "{}{not} BETWEEN {} AND {}",
                format_expr_with_work(expr, work)?,
                format_expr_with_work(low, work)?,
                format_expr_with_work(high, work)?
            )
        }
        ExprKind::Like {
            expr,
            pattern,
            negated,
        } => {
            let not = if *negated { " NOT" } else { "" };
            format!(
                "{}{not} LIKE {}",
                format_expr_with_work(expr, work)?,
                format_expr_with_work(pattern, work)?
            )
        }
        ExprKind::Case {
            operand,
            when_then,
            else_expr,
        } => {
            let mut s = String::from("CASE");
            if let Some(op) = operand {
                append_text(&mut s, " ", work)?;
                append_text(&mut s, &format_expr_with_work(op, work)?, work)?;
            }
            for (when, then) in when_then {
                append_text(&mut s, " WHEN ", work)?;
                append_text(&mut s, &format_expr_with_work(when, work)?, work)?;
                append_text(&mut s, " THEN ", work)?;
                append_text(&mut s, &format_expr_with_work(then, work)?, work)?;
                work.step()?;
            }
            if let Some(otherwise) = else_expr {
                append_text(&mut s, " ELSE ", work)?;
                append_text(&mut s, &format_expr_with_work(otherwise, work)?, work)?;
            }
            append_text(&mut s, " END", work)?;
            s
        }
        ExprKind::IsTruthValue {
            expr,
            value,
            negated,
        } => {
            let not = if *negated { " NOT" } else { "" };
            let val = if *value { "TRUE" } else { "FALSE" };
            format!("{} IS{not} {val}", format_expr_with_work(expr, work)?)
        }
        ExprKind::Nested(inner) => format_expr_with_work(inner, work)?,
        ExprKind::WindowCall { name, args, .. } => {
            let args_str: Vec<String> = args
                .iter()
                .map(|expr| format_expr_with_work(expr, work))
                .collect::<Result<_, SqlCompileError>>()?;
            format!("{name}({})", join_text(&args_str, ", ", work)?)
        }
        ExprKind::SubqueryPlaceholder { id, .. } => format!("<subquery_{id}>"),
        ExprKind::Lambda { params, body } => match params.as_slice() {
            [single] => format!("{} -> {}", single, format_expr_with_work(body, work)?),
            many => format!(
                "({}) -> {}",
                join_text(many, ", ", work)?,
                format_expr_with_work(body, work)?
            ),
        },
    };
    work.step()?;
    Ok(value)
}

pub(crate) fn format_expr(
    expr: &TypedExpr,
    control: &dyn PureCompileControl,
) -> Result<String, SqlCompileError> {
    format_checked(control, |work| format_expr_with_work(expr, work))
}

pub(crate) fn format_project_item(
    item: &ProjectItem,
    control: &dyn PureCompileControl,
) -> Result<String, SqlCompileError> {
    format_checked(control, |work| format_project_item_with_work(item, work))
}

pub(crate) fn format_shared_plan_node_header(
    kind: &LogicalPlanKind,
    stage: PlanNodeExplainStage,
    control: &dyn PureCompileControl,
) -> Result<Option<String>, SqlCompileError> {
    format_checked(control, |work| {
        format_shared_plan_node_header_with_work(kind, stage, work)
    })
}

pub(crate) fn format_shared_plan_node_detail_lines(
    kind: &LogicalPlanKind,
    stage: PlanNodeExplainStage,
    control: &dyn PureCompileControl,
) -> Result<Vec<String>, SqlCompileError> {
    format_checked(control, |work| {
        format_shared_plan_node_detail_lines_with_work(kind, stage, work)
    })
}

pub(crate) fn format_sort_items(
    items: &[SortItem],
    control: &dyn PureCompileControl,
) -> Result<Vec<String>, SqlCompileError> {
    format_checked(control, |work| format_sort_items_with_work(items, work))
}

pub(crate) fn format_window_exprs(
    exprs: &[crate::planner::payload::WindowExpr],
    stage: PlanNodeExplainStage,
    control: &dyn PureCompileControl,
) -> Result<Vec<String>, SqlCompileError> {
    format_checked(control, |work| {
        format_window_exprs_with_work(exprs, stage, work)
    })
}

pub(crate) fn format_assert_one_row_header(
    node: &PlanAssertOneRowNode,
    stage: PlanNodeExplainStage,
    control: &dyn PureCompileControl,
) -> Result<String, SqlCompileError> {
    format_checked(control, |work| {
        format_assert_one_row_header_with_work(node, stage, work)
    })
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::num::{NonZeroU32, NonZeroU64};

    use arrow::datatypes::DataType;

    use super::{
        ExplainLevel, PlanNodeExplainStage, explain_plan, format_expr, format_project_item,
        format_shared_plan_node_header,
    };
    use crate::analysis::{
        BinOp, ExprKind, LiteralValue, OutputColumn, ProjectItem, SortItem, TypedExpr,
    };
    use crate::binding::{SqlTableBindingId, SqlTableBindingScopeId};
    use crate::column_id::ColumnId;
    use crate::common::ApplyKind;
    use crate::planner::logical::{LogicalApplyNode, LogicalPlanKind, LogicalPlanNode};
    use crate::planner::payload::{
        PlanAssertOneRowNode, PlanFilterNode, PlanProjectNode, PlanScanNode, PlanValuesNode,
        PlanWindowNode, WindowExpr,
    };
    use crate::planner::table::{
        ScanSource, SqlMvTargetLocatorScan, SqlScanKind, SqlScanSource, SqlTableIdentity, TableDef,
    };
    use novarocks_types::schema::ColumnDef;

    fn empty_values_for_test() -> LogicalPlanNode {
        LogicalPlanNode::new(
            LogicalPlanKind::Values(PlanValuesNode {
                rows: vec![],
                columns: vec![],
            }),
            vec![],
            None,
        )
    }

    fn output_column(id: u32, name: &str, data_type: DataType, nullable: bool) -> OutputColumn {
        OutputColumn {
            column_id: ColumnId::new_for_test(id),
            name: name.to_string(),
            value_type: novarocks_type_contract::FunctionValueType::new(data_type, nullable),

            is_internal: false,
        }
    }

    fn column_def(name: &str, data_type: DataType, nullable: bool) -> ColumnDef {
        ColumnDef {
            name: name.to_string(),
            data_type,
            nullable,
            write_default: None,
            logical_type: None,
        }
    }

    fn test_table_def() -> TableDef {
        TableDef {
            name: "t".to_string(),
            columns: vec![column_def("k", DataType::Int64, false)],
            iceberg_row_lineage_metadata_columns: vec![],
            source: crate::compiler::mv_rewrite::test_scan_source(
                crate::planner::table::SqlScanKind::ConnectorRead,
            ),
        }
    }

    fn sql_delta_source(from_snapshot_id: i64, to_snapshot_id: i64) -> ScanSource {
        let binding = SqlTableBindingId::new(
            SqlTableBindingScopeId::new(NonZeroU64::new(1).expect("scope")),
            NonZeroU32::new(1).expect("ordinal"),
        );
        ScanSource::Sql(SqlScanSource::new(
            binding,
            SqlTableIdentity {
                catalog: "ice".to_string(),
                namespace: "db".to_string(),
                table: "orders".to_string(),
            },
            SqlScanKind::Delta {
                from_snapshot_id,
                to_snapshot_id,
            },
        ))
    }

    fn sql_snapshot_source(snapshot_id: i64) -> ScanSource {
        let binding = SqlTableBindingId::new(
            SqlTableBindingScopeId::new(NonZeroU64::new(1).expect("scope")),
            NonZeroU32::new(1).expect("ordinal"),
        );
        ScanSource::Sql(SqlScanSource::new(
            binding,
            SqlTableIdentity {
                catalog: "ice".to_string(),
                namespace: "db".to_string(),
                table: "orders".to_string(),
            },
            SqlScanKind::FrozenInputSet {
                version: crate::planner::table::SqlTableVersionSelector::Snapshot(snapshot_id),
            },
        ))
    }

    fn sql_target_locator_source() -> ScanSource {
        let binding = SqlTableBindingId::new(
            SqlTableBindingScopeId::new(NonZeroU64::new(1).expect("scope")),
            NonZeroU32::new(1).expect("ordinal"),
        );
        ScanSource::Sql(SqlScanSource::new(
            binding,
            SqlTableIdentity {
                catalog: "ice".to_string(),
                namespace: "db".to_string(),
                table: "pf_mv".to_string(),
            },
            SqlScanKind::MvTargetLocator {
                facts: SqlMvTargetLocatorScan {
                    target_table_uuid: "uuid-pf-mv".to_string(),
                    target_snapshot_id: Some(99),
                    apply_key_column: "__nova_base_row_id".to_string(),
                    branch_id_column: Some("__branch_id".to_string()),
                },
            },
        ))
    }

    fn scan_plan_with_source(table_name: &str, source: ScanSource) -> LogicalPlanNode {
        LogicalPlanNode::new(
            LogicalPlanKind::Scan(PlanScanNode {
                database: "db".to_string(),
                table: TableDef {
                    name: table_name.to_string(),
                    columns: vec![column_def("k", DataType::Int64, false)],
                    iceberg_row_lineage_metadata_columns: vec![],
                    source,
                },
                alias: None,
                columns: vec![output_column(1, "k", DataType::Int64, false)],
                predicates: vec![],
                required_columns: None,
                variant_columns: vec![],
                mv_rewritten_from: None,
            }),
            vec![],
            None,
        )
    }

    fn column_expr(id: u32, qualifier: Option<&str>, name: &str) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::ColumnRef {
                column_id: ColumnId::new_for_test(id),
                qualifier: qualifier.map(str::to_string),
                column: name.to_string(),
            },
            value_type: novarocks_type_contract::FunctionValueType::new(DataType::Int64, false),
        }
    }

    fn int_literal(value: i64) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::Literal(LiteralValue::Int(value)),
            value_type: novarocks_type_contract::FunctionValueType::new(DataType::Int64, false),
        }
    }

    #[test]
    fn logical_explain_verbose_prints_refresh_scan_sources() {
        let delta_plan = scan_plan_with_source("orders", sql_delta_source(101, 200));

        let delta_normal = explain_plan(&delta_plan, ExplainLevel::Normal).join("\n");
        assert!(!delta_normal.contains("source:"), "{delta_normal}");
        let delta_verbose = explain_plan(&delta_plan, ExplainLevel::Verbose).join("\n");
        assert!(
            delta_verbose
                .contains("source: IcebergDeltaTable from_snapshot_id=101 to_snapshot_id=200"),
            "{delta_verbose}"
        );

        let version_plan = scan_plan_with_source("orders", sql_snapshot_source(200));
        let version_verbose = explain_plan(&version_plan, ExplainLevel::Verbose).join("\n");
        assert!(
            version_verbose.contains("source: IcebergVersionTable snapshot_id=200"),
            "{version_verbose}"
        );

        let locator_plan = scan_plan_with_source("pf_mv", sql_target_locator_source());

        let locator_normal = explain_plan(&locator_plan, ExplainLevel::Normal).join("\n");
        assert!(!locator_normal.contains("source:"), "{locator_normal}");
        let locator_verbose = explain_plan(&locator_plan, ExplainLevel::Verbose).join("\n");
        assert!(
            locator_verbose.contains(
                "source: IcebergMvTargetLocator target=ice.db.pf_mv apply_key=__nova_base_row_id branch_id=__branch_id"
            ),
            "{locator_verbose}"
        );
    }

    #[test]
    fn logical_explain_formats_apply_and_assert_one_row() {
        let plan = LogicalPlanNode::new(
            LogicalPlanKind::Apply(LogicalApplyNode {
                kind: ApplyKind::Exists { negated: true },
                subquery_expr: TypedExpr {
                    kind: ExprKind::ColumnRef {
                        column_id: ColumnId(5),
                        qualifier: None,
                        column: "sq".to_string(),
                    },
                    value_type: novarocks_type_contract::FunctionValueType::new(
                        DataType::Boolean,
                        false,
                    ),
                },
                output_column: OutputColumn {
                    column_id: ColumnId(5),
                    name: "sq".to_string(),
                    value_type: novarocks_type_contract::FunctionValueType::new(
                        DataType::Boolean,
                        false,
                    ),

                    is_internal: true,
                },
                inner_output_column_id: ColumnId(5),
                correlation_column_ids: vec![ColumnId(1)],
                correlation_conjuncts: vec![],
                residual_predicate: None,
                need_check_max_rows: false,
                use_semi_anti: true,
                uncorrelated_outer_predicate_columns: HashSet::new(),
            }),
            vec![
                empty_values_for_test(),
                LogicalPlanNode::new(
                    LogicalPlanKind::AssertOneRow(PlanAssertOneRowNode::global_at_most_one(
                        "select 1",
                    )),
                    vec![empty_values_for_test()],
                    None,
                ),
            ],
            None,
        );

        let out = explain_plan(&plan, ExplainLevel::Normal).join("\n");

        assert!(
            out.contains("APPLY (NOT EXISTS, correlated=true, use_semi_anti=true)"),
            "missing APPLY line: {out}"
        );
        assert!(
            out.contains("ASSERT ONE ROW"),
            "missing ASSERT ONE ROW line: {out}"
        );
    }

    #[test]
    fn shared_plan_node_header_formats_unified_pass_through_nodes() {
        let values = LogicalPlanKind::Values(PlanValuesNode {
            rows: vec![vec![], vec![]],
            columns: vec![],
        });
        let assert =
            LogicalPlanKind::AssertOneRow(PlanAssertOneRowNode::global_at_most_one("select 1"));

        assert_eq!(
            format_shared_plan_node_header(
                &values,
                PlanNodeExplainStage::Logical,
                &crate::compiler::SqlCompileControl::unbounded()
            )
            .unwrap(),
            Some("VALUES (2 rows)".to_string())
        );
        assert_eq!(
            format_shared_plan_node_header(
                &values,
                PlanNodeExplainStage::Distributed,
                &crate::compiler::SqlCompileControl::unbounded()
            )
            .unwrap(),
            Some("VALUES (2 rows)".to_string())
        );
        assert_eq!(
            format_shared_plan_node_header(
                &assert,
                PlanNodeExplainStage::Logical,
                &crate::compiler::SqlCompileControl::unbounded()
            )
            .unwrap(),
            Some("ASSERT ONE ROW".to_string())
        );
        assert_eq!(
            format_shared_plan_node_header(
                &assert,
                PlanNodeExplainStage::Distributed,
                &crate::compiler::SqlCompileControl::unbounded()
            )
            .unwrap(),
            Some("ASSERT NUM ROWS (<= 1)".to_string())
        );

        let keyed = LogicalPlanKind::AssertOneRow(PlanAssertOneRowNode::per_key_at_most_one(
            "DML change-stream matched row uniqueness",
            vec![crate::column_id::ColumnId::new_for_test(7)],
            vec!["_row_id".to_string()],
            "MOR UPDATE matched target row",
        ));
        assert_eq!(
            format_shared_plan_node_header(
                &keyed,
                PlanNodeExplainStage::Distributed,
                &crate::compiler::SqlCompileControl::unbounded()
            )
            .unwrap(),
            Some("ASSERT NUM ROWS (PER KEY <= 1 BY [_row_id])".to_string())
        );
    }

    #[test]
    fn shared_logical_formatter_path_covers_scan_filter_project_and_window() {
        let scan_columns = vec![output_column(1, "k", DataType::Int64, false)];
        let scan = LogicalPlanNode::new(
            LogicalPlanKind::Scan(PlanScanNode {
                database: "test_db".to_string(),
                table: test_table_def(),
                alias: Some("t".to_string()),
                columns: scan_columns,
                predicates: vec![],
                required_columns: None,
                variant_columns: vec![],
                mv_rewritten_from: None,
            }),
            vec![],
            None,
        );
        let predicate = TypedExpr {
            kind: ExprKind::BinaryOp {
                left: Box::new(column_expr(1, Some("t"), "k")),
                op: BinOp::Gt,
                right: Box::new(int_literal(10)),
                decimal_overflow_policy: novarocks_type_contract::DecimalOverflowPolicy::OutputNull,
            },
            value_type: novarocks_type_contract::FunctionValueType::new(DataType::Boolean, false),
        };
        let filter = LogicalPlanNode::new(
            LogicalPlanKind::Filter(PlanFilterNode { predicate }),
            vec![scan],
            None,
        );
        let project = LogicalPlanNode::new(
            LogicalPlanKind::Project(PlanProjectNode {
                items: vec![ProjectItem {
                    expr: column_expr(1, Some("t"), "k"),
                    output_name: "k".to_string(),
                    output_column_id: ColumnId::new_for_test(1),
                }],
                output_qualifier: None,
            }),
            vec![filter],
            None,
        );
        let window = LogicalPlanNode::new(
            LogicalPlanKind::Window(PlanWindowNode {
                window_exprs: vec![WindowExpr {
                    name: "row_number".to_string(),
                    args: vec![],
                    distinct: false,
                    binding: crate::analysis::test_window_binding(
                        "row_number",
                        &[],
                        DataType::Int64,
                        false,
                    ),
                    function_order_by: vec![],
                    aggregate_binding: None,
                    partition_by: vec![column_expr(1, None, "k")],
                    order_by: vec![SortItem {
                        expr: column_expr(1, None, "k"),
                        asc: true,
                        nulls_first: false,
                    }],
                    window_frame: None,
                    result_type: DataType::Int64,
                    output_name: "rn".to_string(),
                    output_column_id: ColumnId::new_for_test(2),
                    ignore_nulls: false,
                }],
                output_columns: vec![
                    output_column(1, "k", DataType::Int64, false),
                    output_column(2, "rn", DataType::Int64, false),
                ],
            }),
            vec![project],
            None,
        );

        assert_eq!(
            format_shared_plan_node_header(
                &window.kind,
                PlanNodeExplainStage::Logical,
                &crate::compiler::SqlCompileControl::unbounded()
            )
            .unwrap(),
            Some("WINDOW [row_number() OVER (PARTITION BY k ORDER BY k ASC)]".to_string())
        );
        assert_eq!(
            explain_plan(&window, ExplainLevel::Normal),
            vec![
                "WINDOW [row_number() OVER (PARTITION BY k ORDER BY k ASC)]".to_string(),
                "  PROJECT [t.k AS k]".to_string(),
                "    FILTER".to_string(),
                "      predicate: t.k > 10".to_string(),
                "      0:SCAN test_db.t (alias=t)".to_string(),
            ]
        );
    }

    #[test]
    fn format_expr_prints_column_before_literal_for_equality() {
        let expr = TypedExpr {
            kind: ExprKind::BinaryOp {
                left: Box::new(TypedExpr {
                    kind: ExprKind::Literal(LiteralValue::Int(10)),
                    value_type: novarocks_type_contract::FunctionValueType::new(
                        DataType::Int64,
                        false,
                    ),
                }),
                op: BinOp::Eq,
                right: Box::new(TypedExpr {
                    kind: ExprKind::ColumnRef {
                        column_id: ColumnId(42),
                        qualifier: Some("r".to_string()),
                        column: "rk".to_string(),
                    },
                    value_type: novarocks_type_contract::FunctionValueType::new(
                        DataType::Int64,
                        false,
                    ),
                }),
                decimal_overflow_policy: novarocks_type_contract::DecimalOverflowPolicy::OutputNull,
            },
            value_type: novarocks_type_contract::FunctionValueType::new(DataType::Boolean, false),
        };

        assert_eq!(
            format_expr(&expr, &crate::compiler::SqlCompileControl::unbounded()).unwrap(),
            "r.rk = 10"
        );
    }

    #[test]
    fn format_project_item_keeps_qualified_column_alias() {
        let item = ProjectItem {
            expr: TypedExpr {
                kind: ExprKind::ColumnRef {
                    column_id: ColumnId(1),
                    qualifier: Some("a".to_string()),
                    column: "k".to_string(),
                },
                value_type: novarocks_type_contract::FunctionValueType::new(DataType::Int64, false),
            },
            output_name: "k".to_string(),
            output_column_id: ColumnId(1),
        };

        assert_eq!(
            format_project_item(&item, &crate::compiler::SqlCompileControl::unbounded()).unwrap(),
            "a.k AS k"
        );
    }

    #[test]
    fn format_project_item_keeps_real_column_alias() {
        let item = ProjectItem {
            expr: TypedExpr {
                kind: ExprKind::ColumnRef {
                    column_id: ColumnId(1),
                    qualifier: None,
                    column: "id".to_string(),
                },
                value_type: novarocks_type_contract::FunctionValueType::new(DataType::Int64, false),
            },
            output_name: "alias_id".to_string(),
            output_column_id: ColumnId(1),
        };

        assert_eq!(
            format_project_item(&item, &crate::compiler::SqlCompileControl::unbounded()).unwrap(),
            "id AS alias_id"
        );
    }
}

#[cfg(test)]
#[path = "cv_tests.rs"]
mod cv_tests;
