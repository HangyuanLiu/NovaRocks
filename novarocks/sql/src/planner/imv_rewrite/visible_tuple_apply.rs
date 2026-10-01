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

//! Visible-tuple integer-weight apply. Quota nodes and fanout are closed
//! semantic barriers; no row identity is persisted in the result relation.

use crate::analysis::{BinOp, ExprKind, LiteralValue, OutputColumn, ProjectItem, TypedExpr};
use crate::column_id::ColumnId;
use crate::compiler::mv_rewrite::{SqlImvRewriteSnapshot, SqlImvVisibleApplyKind};
use crate::optimizer::opt_expr::OptExpr;
use crate::optimizer::rewrite::{
    context::RewriteContext,
    phase::RewritePhase,
    result::RewriteResult,
    rule::{LogicalRewriteRule, RewriteTraversal},
};
use crate::planner::imv_rewrite::{
    PlanRewriteResult, annotation::ImvExtension, bridge_apply_result_mut,
    column_alloc::allocate_imv_output_column, join_delta::plan_output_columns,
};
use crate::planner::logical::{
    LogicalAggregateNode, LogicalPlanKind, LogicalPlanNode, LogicalUnionNode,
};
use crate::planner::payload::{
    AggregateCall, PlanFilterNode, PlanProjectNode, PlanScanNode, PlanTableFunctionNode,
};
use crate::planner::quota::*;
use crate::planner::table::{
    ScanSource, SqlMvTargetBagScan, SqlScanKind, SqlScanSource, SqlTableIdentity, TableDef,
};
use arrow::datatypes::DataType;
use novarocks_types::schema::ColumnDef;
use std::sync::atomic::{AtomicBool, Ordering};

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct VisibleBagApplyDescriptor {
    pub kind: SqlImvVisibleApplyKind,
    pub action: ColumnId,
    pub file: Option<ColumnId>,
    pub position: Option<ColumnId>,
}

pub(crate) struct RewriteVisibleBagApplyRule {
    fired: AtomicBool,
}
impl RewriteVisibleBagApplyRule {
    pub fn new() -> Self {
        Self {
            fired: AtomicBool::new(false),
        }
    }
}
impl LogicalRewriteRule for RewriteVisibleBagApplyRule {
    fn name(&self) -> &'static str {
        "RewriteVisibleBagApply"
    }
    fn phase(&self) -> RewritePhase {
        RewritePhase::SemanticRewrite
    }
    fn traversal(&self) -> RewriteTraversal {
        RewriteTraversal::TopDown
    }
    fn matches(&self, _: &OptExpr, ctx: &RewriteContext) -> bool {
        !self.fired.load(Ordering::SeqCst)
            && ctx
                .extension::<ImvExtension>()
                .is_some_and(|e| e.snapshot.schema_contract.aggregate.is_none())
    }
    fn apply(&self, expr: OptExpr, ctx: &mut RewriteContext) -> Result<RewriteResult, String> {
        self.fired.store(true, Ordering::SeqCst);
        bridge_apply_result_mut(expr, ctx, |plan, ctx| {
            let ext = ctx
                .extension::<ImvExtension>()
                .ok_or("visible apply requires IMV snapshot")?
                .clone();
            let (plan, descriptor) = build_visible_apply(plan, &ext.snapshot, ctx)?;
            let mut annotation = ext.annotation.clone();
            annotation.change_stream.visible_bag = Some(descriptor);
            ctx.set_extension(ImvExtension { annotation, ..ext });
            Ok(PlanRewriteResult::Changed(plan))
        })
    }
}

fn node(kind: LogicalPlanKind, children: Vec<LogicalPlanNode>) -> LogicalPlanNode {
    LogicalPlanNode::new(kind, children, None)
}
fn col(
    ctx: &RewriteContext,
    name: &str,
    data_type: DataType,
    nullable: bool,
    internal: bool,
) -> Result<OutputColumn, String> {
    allocate_imv_output_column(ctx, name, data_type, nullable, internal)
}
fn reference(c: &OutputColumn) -> TypedExpr {
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
fn literal(value: i64) -> TypedExpr {
    TypedExpr {
        kind: ExprKind::Literal(LiteralValue::Int(value)),
        data_type: DataType::Int64,
        nullable: false,
    }
}
fn cast(expr: TypedExpr, target: DataType) -> TypedExpr {
    let nullable = expr.nullable;
    TypedExpr {
        kind: ExprKind::Cast {
            expr: Box::new(expr),
            target: target.clone(),
            decimal_overflow_policy: novarocks_type_contract::DecimalOverflowPolicy::OutputNull,
        },
        data_type: target,
        nullable,
    }
}
fn comparison(c: &OutputColumn, op: BinOp) -> TypedExpr {
    TypedExpr {
        kind: ExprKind::BinaryOp {
            left: Box::new(reference(c)),
            op,
            right: Box::new(literal(0)),
            decimal_overflow_policy: novarocks_type_contract::DecimalOverflowPolicy::OutputNull,
        },
        data_type: DataType::Boolean,
        nullable: false,
    }
}
fn item(output: &OutputColumn, expr: TypedExpr) -> ProjectItem {
    ProjectItem {
        expr,
        output_name: output.name.clone(),
        output_column_id: output.column_id,
    }
}
fn project(input: LogicalPlanNode, items: Vec<ProjectItem>) -> LogicalPlanNode {
    node(
        LogicalPlanKind::Project(PlanProjectNode {
            items,
            output_qualifier: None,
        }),
        vec![input],
    )
}
fn filter(input: LogicalPlanNode, predicate: TypedExpr) -> LogicalPlanNode {
    node(
        LogicalPlanKind::Filter(PlanFilterNode { predicate }),
        vec![input],
    )
}
fn scalar(ctx: &RewriteContext, name: &str, args: Vec<TypedExpr>) -> Result<TypedExpr, String> {
    let arguments = args
        .iter()
        .map(crate::analysis::function_argument)
        .collect::<Vec<_>>();
    let exact = ctx
        .function_catalog()
        .resolve_scalar_binding_trusted(name, &arguments)
        .map_err(|e| e.to_string())?;
    let novarocks_functions::FunctionResultType::Scalar(result) = &exact.selected.result_type
    else {
        return Err("visible apply scalar returned a relation".into());
    };
    Ok(TypedExpr {
        data_type: result.data_type.clone(),
        nullable: result.nullable,
        kind: ExprKind::FunctionCall {
            name: name.into(),
            args,
            distinct: false,
            volatility: exact.semantics.volatility,
            binding: exact.into(),
        },
    })
}
fn aggregate(
    ctx: &RewriteContext,
    name: &str,
    args: Vec<TypedExpr>,
    output: &OutputColumn,
) -> Result<AggregateCall, String> {
    let exact = crate::functions::resolve_sql_aggregate_binding(
        ctx.function_catalog(),
        name,
        &args,
        &[],
        true,
    )?;
    Ok(AggregateCall {
        name: name.into(),
        args,
        distinct: false,
        result_type: output.data_type.clone(),
        order_by: vec![],
        resolved: exact.into(),
        output_column_id: output.column_id,
    })
}

fn build_visible_apply(
    input: LogicalPlanNode,
    snapshot: &SqlImvRewriteSnapshot,
    ctx: &RewriteContext,
) -> Result<(LogicalPlanNode, VisibleBagApplyDescriptor), String> {
    let facts = snapshot
        .visible_apply
        .as_ref()
        .ok_or("non-aggregate apply requires frozen delete and capacity facts")?;
    let output = plan_output_columns(&input)?;
    let visible = output
        .iter()
        .filter(|c| !c.is_internal)
        .cloned()
        .collect::<Vec<_>>();
    if visible.is_empty() || visible.len() != snapshot.schema_contract.target.visible_columns.len()
    {
        return Err("visible apply target arity differs from admitted contract".into());
    }
    let action = output
        .iter()
        .filter(|c| c.is_internal && c.name == super::action_column::ImvActionColumn::NAME)
        .collect::<Vec<_>>();
    if action.len() != 1 || action[0].data_type != DataType::Int8 || action[0].nullable {
        return Err("visible apply requires one non-null signed action".into());
    }
    if facts.kind == SqlImvVisibleApplyKind::AppendOnly {
        let final_action = col(
            ctx,
            super::action_column::ImvActionColumn::NAME,
            DataType::Int8,
            false,
            true,
        )?;
        let mut items = visible
            .iter()
            .map(|c| item(c, reference(c)))
            .collect::<Vec<_>>();
        items.push(item(&final_action, cast(literal(1), DataType::Int8)));
        return Ok((
            project(input, items),
            VisibleBagApplyDescriptor {
                kind: facts.kind,
                action: final_action.column_id,
                file: None,
                position: None,
            },
        ));
    }
    let key = col(ctx, "__mv_content_key", DataType::Binary, false, true)?;
    let weight = col(ctx, "__mv_weight", DataType::Int64, false, true)?;
    let mut items = visible
        .iter()
        .map(|c| item(c, reference(c)))
        .collect::<Vec<_>>();
    items.push(item(
        &key,
        scalar(
            ctx,
            "mv_content_key",
            visible.iter().map(reference).collect(),
        )?,
    ));
    items.push(item(&weight, cast(reference(action[0]), DataType::Int64)));
    let input = project(input, items);
    let final_visible = visible
        .iter()
        .map(|c| col(ctx, &c.name, c.data_type.clone(), c.nullable, false))
        .collect::<Result<Vec<_>, _>>()?;
    let net_state = col(ctx, "__mv_weight", DataType::Int64, true, true)?;
    let representatives = visible
        .iter()
        .map(|c| col(ctx, &c.name, c.data_type.clone(), true, false))
        .collect::<Result<Vec<_>, _>>()?;
    let mut aggregates = visible
        .iter()
        .zip(&representatives)
        .map(|(src, dst)| aggregate(ctx, "any_value", vec![reference(src)], dst))
        .collect::<Result<Vec<_>, _>>()?;
    aggregates.push(aggregate(
        ctx,
        "mv_weight_sum",
        vec![reference(&weight)],
        &net_state,
    )?);
    let mut outputs = vec![key.clone()];
    outputs.extend(representatives.clone());
    outputs.push(net_state.clone());
    let input = node(
        LogicalPlanKind::Aggregate(LogicalAggregateNode {
            group_by: vec![reference(&key)],
            aggregates,
            output_columns: outputs,
            already_pushed: false,
        }),
        vec![input],
    );
    let net = col(ctx, "__mv_weight", DataType::Int64, false, true)?;
    let mut restored = final_visible
        .iter()
        .zip(&representatives)
        .map(|(dst, src)| {
            let value = if dst.nullable {
                reference(src)
            } else {
                scalar(ctx, "mv_require_non_null", vec![reference(src)])?
            };
            Ok(item(dst, value))
        })
        .collect::<Result<Vec<_>, String>>()?;
    restored.push(item(
        &net,
        scalar(ctx, "mv_require_non_null", vec![reference(&net_state)])?,
    ));
    let input = project(input, restored);
    let input = filter(input, comparison(&net, BinOp::Ne));
    // Encode after representative materialization, so the final key has a
    // complete visible-value homology proof in the authoritative plan.
    let key = col(ctx, "__mv_content_key", DataType::Binary, false, true)?;
    let mut items = final_visible
        .iter()
        .map(|c| item(c, reference(c)))
        .collect::<Vec<_>>();
    items.push(item(&net, reference(&net)));
    items.push(item(
        &key,
        scalar(
            ctx,
            "mv_content_key",
            final_visible.iter().map(reference).collect(),
        )?,
    ));
    let input = project(input, items);
    let action = col(
        ctx,
        super::action_column::ImvActionColumn::NAME,
        DataType::Int8,
        false,
        true,
    )?;
    let entry = col(ctx, "__mv_entry", DataType::Binary, false, true)?;
    let mut producer_columns = final_visible.clone();
    producer_columns.extend([net.clone(), key.clone(), entry.clone()]);
    let mut items = producer_columns
        .iter()
        .filter(|c| c.column_id != entry.column_id)
        .map(|c| item(c, reference(c)))
        .collect::<Vec<_>>();
    items.push(item(&entry, scalar(ctx, "mv_entry_id", vec![])?));
    let producer = project(input, items);
    let anchor = col(ctx, "__mv_fanout", DataType::Binary, false, true)?.column_id;
    let branches = vec![
        PlanFanoutBranch {
            predicate: comparison(&net, BinOp::Gt),
            distribution: PlanFanoutDistribution::RoundRobin,
        },
        PlanFanoutBranch {
            predicate: comparison(&net, BinOp::Lt),
            distribution: PlanFanoutDistribution::Broadcast,
        },
        PlanFanoutBranch {
            predicate: comparison(&net, BinOp::Lt),
            distribution: PlanFanoutDistribution::Hash(vec![entry.column_id]),
        },
    ];
    let (positive, pos_columns) = consume(
        ctx,
        anchor,
        0,
        &producer_columns,
        branches[0].distribution.clone(),
    )?;
    let positive = expand_positive(positive, &pos_columns[final_visible.len()], ctx)?;
    let (demand, demand_columns) = consume(
        ctx,
        anchor,
        1,
        &producer_columns,
        branches[1].distribution.clone(),
    )?;
    let (seed, seed_columns) = consume(
        ctx,
        anchor,
        2,
        &producer_columns,
        branches[2].distribution.clone(),
    )?;
    let (target, target_values, target_file, target_pos) = target_scan(snapshot, ctx)?;
    let candidate_entry = col(ctx, "__mv_entry", DataType::Binary, false, true)?;
    let candidate_file = col(
        ctx,
        crate::common::ICEBERG_FILE_PATH_COL,
        DataType::Utf8,
        false,
        true,
    )?;
    let candidate_pos = col(
        ctx,
        crate::common::ICEBERG_ROW_POS_COL,
        DataType::Int64,
        false,
        true,
    )?;
    let domain = col(ctx, "__mv_quota_domain", DataType::Binary, false, true)?.column_id;
    let candidate = node(
        LogicalPlanKind::QuotaPreclaim(PlanQuotaPreclaimNode {
            domain,
            demand_entry_id: demand_columns[final_visible.len() + 2].column_id,
            demand_key: demand_columns[final_visible.len() + 1].column_id,
            demand_need: PlanQuotaNeed::NegativeWeight(
                demand_columns[final_visible.len()].column_id,
            ),
            demand_values: demand_columns[..final_visible.len()]
                .iter()
                .map(|c| c.column_id)
                .collect(),
            target_values: target_values.iter().map(|c| c.column_id).collect(),
            target_file: target_file.column_id,
            target_position: target_pos.column_id,
            output_columns: vec![
                candidate_entry.clone(),
                candidate_file.clone(),
                candidate_pos.clone(),
            ],
            max_state_bytes: facts.max_state_bytes,
        }),
        vec![demand, target],
    );
    let deleted_file = col(
        ctx,
        crate::common::ICEBERG_FILE_PATH_COL,
        DataType::Utf8,
        false,
        true,
    )?;
    let deleted_pos = col(
        ctx,
        crate::common::ICEBERG_ROW_POS_COL,
        DataType::Int64,
        false,
        true,
    )?;
    let deleted = node(
        LogicalPlanKind::QuotaTrim(PlanQuotaTrimNode {
            domain,
            seed_entry_id: seed_columns[final_visible.len() + 2].column_id,
            seed_need: PlanQuotaNeed::NegativeWeight(seed_columns[final_visible.len()].column_id),
            candidate_entry_id: candidate_entry.column_id,
            candidate_file: candidate_file.column_id,
            candidate_position: candidate_pos.column_id,
            output_columns: vec![deleted_file.clone(), deleted_pos.clone()],
            max_state_bytes: facts.max_state_bytes,
        }),
        vec![seed, candidate],
    );
    let final_visible = final_visible
        .iter()
        .map(|c| col(ctx, &c.name, c.data_type.clone(), true, false))
        .collect::<Result<Vec<_>, _>>()?;
    let file = col(
        ctx,
        crate::common::ICEBERG_FILE_PATH_COL,
        DataType::Utf8,
        true,
        true,
    )?;
    let position = col(
        ctx,
        crate::common::ICEBERG_ROW_POS_COL,
        DataType::Int64,
        true,
        true,
    )?;
    let mut positive_items = final_visible
        .iter()
        .zip(&pos_columns)
        .map(|(dst, src)| item(dst, reference(src)))
        .collect::<Vec<_>>();
    positive_items.extend([
        item(&file, null(&file.data_type)),
        item(&position, null(&position.data_type)),
        item(&action, cast(literal(1), DataType::Int8)),
    ]);
    let mut delete_items = final_visible
        .iter()
        .map(|c| item(c, null(&c.data_type)))
        .collect::<Vec<_>>();
    delete_items.extend([
        item(&file, reference(&deleted_file)),
        item(&position, reference(&deleted_pos)),
        item(&action, cast(literal(-1), DataType::Int8)),
    ]);
    let mut outputs = final_visible;
    outputs.extend([file.clone(), position.clone(), action.clone()]);
    let body = node(
        LogicalPlanKind::Union(LogicalUnionNode {
            all: true,
            output_columns: outputs,
        }),
        vec![
            project(positive, positive_items),
            project(deleted, delete_items),
        ],
    );
    let plan = node(
        LogicalPlanKind::FanoutAnchor(PlanFanoutAnchorNode {
            id: anchor,
            branches,
        }),
        vec![producer, body],
    );
    Ok((
        plan,
        VisibleBagApplyDescriptor {
            kind: facts.kind,
            action: action.column_id,
            file: Some(file.column_id),
            position: Some(position.column_id),
        },
    ))
}
fn null(data_type: &DataType) -> TypedExpr {
    TypedExpr {
        kind: ExprKind::Literal(LiteralValue::Null),
        data_type: data_type.clone(),
        nullable: true,
    }
}
fn consume(
    ctx: &RewriteContext,
    anchor: ColumnId,
    branch: usize,
    producer: &[OutputColumn],
    distribution: PlanFanoutDistribution,
) -> Result<(LogicalPlanNode, Vec<OutputColumn>), String> {
    let outputs = producer
        .iter()
        .map(|c| col(ctx, &c.name, c.data_type.clone(), c.nullable, c.is_internal))
        .collect::<Result<Vec<_>, _>>()?;
    let plan = node(
        LogicalPlanKind::FanoutConsume(PlanFanoutConsumeNode {
            anchor,
            branch,
            output_columns: outputs.clone(),
            producer_column_ids: producer.iter().map(|c| c.column_id).collect(),
            distribution,
        }),
        vec![],
    );
    Ok((plan, outputs))
}
fn expand_positive(
    input: LogicalPlanNode,
    weight: &OutputColumn,
    ctx: &RewriteContext,
) -> Result<LogicalPlanNode, String> {
    let args = vec![literal(1), reference(weight), literal(1)];
    let arguments = args
        .iter()
        .map(crate::analysis::function_argument)
        .collect::<Vec<_>>();
    let binding = ctx
        .function_catalog()
        .resolve_table_binding_trusted("generate_series", &arguments)
        .map_err(|e| e.to_string())?;
    let repeated = col(ctx, "__mv_copy", DataType::Int64, false, true)?;
    Ok(node(
        LogicalPlanKind::TableFunction(PlanTableFunctionNode {
            function_name: "generate_series".into(),
            args,
            binding: binding.into(),
            output_columns: vec![repeated],
            alias: None,
            is_left_join: false,
        }),
        vec![input],
    ))
}
fn target_scan(
    snapshot: &SqlImvRewriteSnapshot,
    ctx: &RewriteContext,
) -> Result<
    (
        LogicalPlanNode,
        Vec<OutputColumn>,
        OutputColumn,
        OutputColumn,
    ),
    String,
> {
    let visible = snapshot
        .schema_contract
        .target
        .visible_columns
        .iter()
        .map(|contract| {
            let field = snapshot
                .target_columns
                .iter()
                .find(|c| c.name.eq_ignore_ascii_case(&contract.output_name))
                .ok_or_else(|| {
                    format!("visible target field {} is absent", contract.output_name)
                })?;
            col(
                ctx,
                &field.name,
                field.data_type.clone(),
                field.nullable,
                false,
            )
        })
        .collect::<Result<Vec<_>, String>>()?;
    let file = col(
        ctx,
        crate::common::ICEBERG_FILE_PATH_COL,
        DataType::Utf8,
        false,
        true,
    )?;
    let position = col(
        ctx,
        crate::common::ICEBERG_ROW_POS_COL,
        DataType::Int64,
        false,
        true,
    )?;
    let definitions = |columns: &[OutputColumn]| {
        columns
            .iter()
            .map(|c| ColumnDef {
                name: c.name.clone(),
                data_type: c.data_type.clone(),
                nullable: c.nullable,
                write_default: None,
                logical_type: None,
            })
            .collect::<Vec<_>>()
    };
    let mut columns = visible.clone();
    columns.extend([file.clone(), position.clone()]);
    let scan = PlanScanNode {
        database: snapshot.target.namespace.clone(),
        table: TableDef {
            name: snapshot.target.table.clone(),
            columns: definitions(&visible),
            iceberg_row_lineage_metadata_columns: definitions(&[file.clone(), position.clone()]),
            source: ScanSource::Sql(SqlScanSource::new(
                snapshot.target_binding,
                SqlTableIdentity {
                    catalog: snapshot.target.catalog.clone(),
                    namespace: snapshot.target.namespace.clone(),
                    table: snapshot.target.table.clone(),
                },
                SqlScanKind::MvTargetBag {
                    facts: SqlMvTargetBagScan {
                        target_table_uuid: snapshot.target_table_uuid.clone(),
                        target_snapshot_id: snapshot.target_snapshot_id,
                        visible_columns: visible.iter().map(|c| c.name.clone()).collect(),
                    },
                },
            )),
        },
        alias: None,
        columns,
        predicates: vec![],
        required_columns: None,
        variant_columns: vec![],
        mv_rewritten_from: None,
    };
    Ok((
        node(LogicalPlanKind::Scan(scan), vec![]),
        visible,
        file,
        position,
    ))
}

pub(crate) fn validate_descriptor(
    plan: &LogicalPlanNode,
    descriptor: &VisibleBagApplyDescriptor,
) -> Result<(), String> {
    let columns = plan_output_columns(plan)?;
    if columns.iter().filter(|c| !c.is_internal).count() == 0
        || !columns.iter().any(|c| {
            c.column_id == descriptor.action
                && c.is_internal
                && c.data_type == DataType::Int8
                && !c.nullable
        })
    {
        return Err("visible bag apply root has invalid visible/action outputs".into());
    }
    match descriptor.kind {
        SqlImvVisibleApplyKind::AppendOnly => {
            if descriptor.file.is_some()
                || descriptor.position.is_some()
                || !matches!(plan.kind, LogicalPlanKind::Project(_))
            {
                return Err("append apply has an invalid root contract".into());
            }
        }
        SqlImvVisibleApplyKind::PotentialDeletes => {
            if !matches!(plan.kind, LogicalPlanKind::FanoutAnchor(_))
                || descriptor.file.is_none()
                || descriptor.position.is_none()
            {
                return Err("delete apply requires a materialized fanout root and locators".into());
            }
            for (id, data_type) in [
                (descriptor.file.unwrap(), DataType::Utf8),
                (descriptor.position.unwrap(), DataType::Int64),
            ] {
                if !columns.iter().any(|c| {
                    c.column_id == id && c.is_internal && c.data_type == data_type && c.nullable
                }) {
                    return Err("delete apply locator contract differs from the root".into());
                }
            }
        }
    }
    let mut todo = vec![plan];
    let (mut preclaims, mut trims, mut targets, mut anchors, mut series) = (0, 0, 0, 0, 0);
    while let Some(node) = todo.pop() {
        match &node.kind {
            LogicalPlanKind::QuotaPreclaim(_) => preclaims += 1,
            LogicalPlanKind::QuotaTrim(_) => trims += 1,
            LogicalPlanKind::FanoutAnchor(anchor) => {
                anchors += 1;
                if anchor.branches.len() != 3 {
                    return Err("visible apply requires three exact fanout branches".into());
                }
            }
            LogicalPlanKind::Scan(scan) => match &scan.table.source {
                ScanSource::Sql(source) => match source.kind {
                    SqlScanKind::MvTargetBag { .. } => targets += 1,
                    SqlScanKind::MvTargetLocator { .. } => {
                        return Err("visible apply cannot use a row-identity locator".into());
                    }
                    _ => {}
                },
                _ => {}
            },
            LogicalPlanKind::TableFunction(table) if table.function_name == "generate_series" => {
                series += 1
            }
            _ => {}
        }
        todo.extend(node.children.iter());
    }
    let expected = match descriptor.kind {
        SqlImvVisibleApplyKind::AppendOnly => (0, 0, 0, 0, 0),
        SqlImvVisibleApplyKind::PotentialDeletes => (1, 1, 1, 1, 1),
    };
    if (preclaims, trims, targets, anchors, series) != expected {
        return Err("visible apply contains an incomplete quota/expansion graph".into());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{cell::RefCell, rc::Rc};
    fn fixture(
        kind: SqlImvVisibleApplyKind,
    ) -> (RewriteContext, SqlImvRewriteSnapshot, LogicalPlanNode) {
        let factory = Rc::new(RefCell::new(crate::column_id::ColumnRefFactory::new()));
        let mut ctx = RewriteContext::for_mv_refresh(Vec::<String>::new());
        ctx.set_column_ref_factory(factory);
        ctx.set_function_catalog(crate::functions::test_function_catalog_snapshot());
        let value = col(&ctx, "k", DataType::Int64, false, false).unwrap();
        let op = col(
            &ctx,
            super::super::action_column::ImvActionColumn::NAME,
            DataType::Int8,
            false,
            true,
        )
        .unwrap();
        let input = node(
            LogicalPlanKind::Values(crate::planner::payload::PlanValuesNode {
                rows: vec![vec![literal(7), cast(literal(-1), DataType::Int8)]],
                columns: vec![value, op],
            }),
            vec![],
        );
        let mut snapshot = (*crate::compiler::mv_rewrite::test_incremental_snapshot()).clone();
        let mut schema = (*snapshot.schema_contract).clone();
        schema.target.hidden_apply_key = None;
        schema.target.visible_columns =
            vec![crate::compiler::mv_rewrite::SqlImvTargetVisibleColumn {
                output_name: "k".into(),
                target_field_id: bytes::Bytes::from_static(b"k"),
            }];
        snapshot.schema_contract = std::sync::Arc::new(schema);
        snapshot.visible_apply = Some(
            crate::compiler::mv_rewrite::SqlImvVisibleApplyFacts::try_new(kind, 1024 * 1024)
                .unwrap(),
        );
        (ctx, snapshot, input)
    }
    #[test]
    fn visible_apply_append_contains_no_target_or_consolidation() {
        let (ctx, snapshot, input) = fixture(SqlImvVisibleApplyKind::AppendOnly);
        let (plan, descriptor) = build_visible_apply(input, &snapshot, &ctx).unwrap();
        validate_descriptor(&plan, &descriptor).unwrap();
        assert!(matches!(plan.kind, LogicalPlanKind::Project(_)));
        assert!(matches!(plan.children[0].kind, LogicalPlanKind::Values(_)));
        let columns = plan_output_columns(&plan).unwrap();
        assert_eq!(columns.len(), 2);
        assert_eq!(columns[0].name, "k");
    }
    #[test]
    fn visible_apply_delete_has_one_materialization_and_two_quota_inputs() {
        let (ctx, snapshot, input) = fixture(SqlImvVisibleApplyKind::PotentialDeletes);
        let (plan, descriptor) = build_visible_apply(input, &snapshot, &ctx).unwrap();
        validate_descriptor(&plan, &descriptor).unwrap();
        let mut arena = crate::optimizer::scalar::ScalarArena::new();
        let opt =
            crate::planner::optimizer_bridge::logical::try_to_optimizer_expr(&plan, &mut arena)
                .unwrap();
        let roundtrip = crate::planner::optimizer_bridge::logical::to_logical_plan(opt, &arena);
        validate_descriptor(&roundtrip, &descriptor).unwrap();
        let LogicalPlanKind::FanoutAnchor(anchor) = &plan.kind else {
            panic!()
        };
        assert_eq!(anchor.branches.len(), 3);
        let LogicalPlanKind::Project(producer) = &plan.children[0].kind else {
            panic!()
        };
        assert_eq!(producer.items.iter().filter(|item|matches!(&item.expr.kind,ExprKind::FunctionCall {name,..} if name=="mv_entry_id")).count(),1);
        let mut malformed = plan.clone();
        malformed.children[1].children.pop();
        assert!(validate_descriptor(&malformed, &descriptor).is_err());
    }
    #[test]
    fn visible_apply_delete_survives_cascades_and_binding_validation() {
        let (ctx, snapshot, input) = fixture(SqlImvVisibleApplyKind::PotentialDeletes);
        let (plan, _) = build_visible_apply(input, &snapshot, &ctx).unwrap();
        let mut arena = crate::optimizer::scalar::ScalarArena::new();
        let opt =
            crate::planner::optimizer_bridge::logical::try_to_optimizer_expr(&plan, &mut arena)
                .unwrap();
        let factory = ctx.column_ref_factory().unwrap().borrow().clone();
        let optimized = crate::optimizer::optimize_with_test_table_statistics(
            opt,
            arena,
            &std::collections::HashMap::new(),
            factory,
            Vec::new(),
            &crate::optimizer::options::SessionOptimizerSettings::default(),
        )
        .unwrap();
        let physical = crate::planner::optimizer_bridge::to_physical_plan(&optimized).unwrap();
        assert!(matches!(
            physical.kind,
            crate::planner::physical::PhysicalPlanKind::FanoutAnchor(_)
        ));
    }
    #[test]
    fn visible_apply_delete_lowers_to_authoritative_provider_backed_contract() {
        use crate::compiler::*;
        use novarocks_physical_plan::{
            ExactInputVersion, PipelineDopDomain, PlanVersionId, ProviderColumnReference,
            ProviderReadOccurrenceId, ProviderReadReference, ValueType,
        };
        use novarocks_spi::connector::read_stack::*;
        use novarocks_spi::connector::*;
        let (ctx, snapshot, input) = fixture(SqlImvVisibleApplyKind::PotentialDeletes);
        let (plan, _) = build_visible_apply(input, &snapshot, &ctx).unwrap();
        let mut arena = crate::optimizer::scalar::ScalarArena::new();
        let opt =
            crate::planner::optimizer_bridge::logical::try_to_optimizer_expr(&plan, &mut arena)
                .unwrap();
        let factory = ctx.column_ref_factory().unwrap().borrow().clone();
        let optimized = crate::optimizer::optimize_with_test_table_statistics(
            opt,
            arena,
            &std::collections::HashMap::new(),
            factory,
            Vec::new(),
            &crate::optimizer::options::SessionOptimizerSettings::default(),
        )
        .unwrap();
        let mut physical = crate::planner::optimizer_bridge::to_physical_plan(&optimized).unwrap();
        fn bind_scan(
            plan: &mut crate::planner::physical::PhysicalPlanNode,
        ) -> Option<Vec<OutputColumn>> {
            if let crate::planner::physical::PhysicalPlanKind::Scan(scan) = &mut plan.kind {
                *scan = scan
                    .clone()
                    .finalize_provider_read_occurrence(ProviderReadOccurrenceId::new(0))
                    .unwrap();
                return Some(plan.output_columns.clone());
            }
            plan.children.iter_mut().find_map(bind_scan)
        }
        let columns = bind_scan(&mut physical).unwrap();
        let needs: Vec<_> = columns
            .iter()
            .enumerate()
            .map(|(ordinal, c)| {
                ProviderReadColumnNeed::for_test(
                    ordinal as u32,
                    c.name.clone(),
                    ValueType::new(c.data_type.clone(), c.nullable),
                    if c.data_type == DataType::Utf8 {
                        ConnectorValueType::Varchar
                    } else {
                        ConnectorValueType::BigInt
                    },
                )
            })
            .collect();
        let need = ProviderReadNeed::exact_projection_for_test(
            snapshot.target_binding,
            ProviderReadRelationNeed::MvTarget {
                relation: snapshot.target.clone(),
                target_table_uuid: snapshot.target_table_uuid.clone(),
                target_snapshot_id: snapshot.target_snapshot_id,
                use_affected_partitions: true,
            },
            needs,
        );
        let instance = ConnectorInstanceId::parse("quota-fixture").unwrap();
        let binding = ConnectorReadBinding::new(
            ConnectorInstanceDescriptor {
                provider_id: ConnectorProviderId::parse("iceberg").unwrap(),
                instance_id: instance.clone(),
            },
            CatalogHandle::new(instance, CatalogVersion::from_bytes([3; 32])),
        );
        let payload = |category| {
            ConnectorEncodedPayload::new(
                ConnectorEnvelopeHeader::new(
                    binding.descriptor().provider_id.clone(),
                    binding.catalog_handle().clone(),
                    category,
                    ConnectorCodecRevision::try_new(1).unwrap(),
                ),
                vec![7].into(),
            )
        };
        let read = ProviderReadReference {
            binding: binding.clone(),
            input_version: ExactInputVersion::try_new([9]).unwrap(),
            relation: ConnectorReadRelationPayload::new(
                ConnectorReadRelationKind::Table,
                payload(ConnectorCodecCategory::ReadTable),
                payload(ConnectorCodecCategory::ReadView),
            ),
        };
        let contract = ProviderReadStaticContract {
            sql_binding: snapshot.target_binding,
            request: ProviderReadRequestBinding::from_need(&need),
            read,
            work_source: ConnectorReadWorkSource::RuntimeSplits,
            selection_digest: [8; 32],
            schema: columns
                .iter()
                .enumerate()
                .map(|(ordinal, c)| {
                    ProviderReadColumnFact::new(
                        ordinal as u32,
                        ProviderColumnReference {
                            column_payload: payload(ConnectorCodecCategory::ReadColumn),
                        },
                        ValueType::new(c.data_type.clone(), c.nullable),
                    )
                })
                .collect(),
            predicates: Box::default(),
            limit: ProviderReadLimitFact::NotRequested,
            provided_properties: ProviderReadProperties {
                distribution: ProviderReadDistribution::Unconstrained,
                ordering: Box::default(),
            },
            artifact_inputs: Box::default(),
            artifact_refs: Box::default(),
            coverage_evidence: Box::default(),
        };
        let reads = FinalizedProviderReadSet::single_for_test(
            snapshot.target_binding,
            ProviderReadOccurrenceId::new(0),
            contract,
            novarocks_physical_plan::ScanReadBudget {
                max_batch_rows: 1024,
                max_batch_bytes: 1024 * 1024,
            },
        );
        let finished =
            crate::planner::distributed::build::lower_final_physical_plan_with_provider_reads(
                &physical,
                PlanVersionId::try_new([41; 16]).unwrap(),
                PipelineDopDomain {
                    min: 1,
                    max: 8,
                    requires_power_of_two: true,
                },
                reads,
            )
            .unwrap()
            .finish()
            .unwrap();
        assert_eq!(finished.runtime_filters().len(), 1);
        assert!(
            finished
                .runtime_filters()
                .values()
                .flat_map(|filter| filter.producers.iter())
                .all(|producer| producer.witness.get() > 0)
        );
        assert!(finished.runtime_filters().values().all(|filter|matches!(filter.consumers[0].activation,novarocks_physical_plan::RuntimeFilterConsumerActivation::StartUnfilteredThenApplyComplete{..})));
        let nodes = finished
            .fragments()
            .values()
            .flat_map(|f| f.nodes().values())
            .collect::<Vec<_>>();
        assert_eq!(
            nodes
                .iter()
                .filter(|n| matches!(
                    n.kind,
                    novarocks_physical_plan::NodeKind::QuotaPreclaim { .. }
                ))
                .count(),
            1
        );
        assert_eq!(
            nodes
                .iter()
                .filter(|n| matches!(n.kind, novarocks_physical_plan::NodeKind::QuotaTrim { .. }))
                .count(),
            1
        );
        assert_eq!(
            finished
                .fragments()
                .values()
                .filter(|f| matches!(
                    f.sink(),
                    novarocks_physical_plan::FragmentSink::PredicateFanout { .. }
                ))
                .count(),
            1
        );
    }
    #[test]
    fn visible_apply_refuses_unfrozen_policy() {
        let (ctx, mut snapshot, input) = fixture(SqlImvVisibleApplyKind::PotentialDeletes);
        snapshot.visible_apply = None;
        assert!(
            build_visible_apply(input, &snapshot, &ctx)
                .unwrap_err()
                .contains("frozen")
        );
    }
}
