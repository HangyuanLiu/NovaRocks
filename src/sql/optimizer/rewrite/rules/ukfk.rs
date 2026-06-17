//! UK/FK-based logical rewrites for standalone Iceberg table properties.

use std::collections::{HashMap, HashSet};

use arrow::datatypes::DataType;

use crate::sql::analysis::{BinOp, ExprKind, JoinKind, LiteralValue, TypedExpr};
use crate::sql::column_id::ColumnId;
use crate::sql::optimizer::operator::{FilterOp, LogicalJoinOp, Operator, ProjectOp, ScanOp, ScalarAggregateSpec, ScalarProjectItem};
use crate::sql::optimizer::opt_expr::OptExpr;
use crate::sql::optimizer::options::current_session_optimizer_settings;
use crate::sql::optimizer::rewrite::context::RewriteContext;
use crate::sql::optimizer::rewrite::phase::RewritePhase;
use crate::sql::optimizer::rewrite::result::RewriteResult;
use crate::sql::optimizer::rewrite::rule::LogicalRewriteRule;
use crate::sql::optimizer::rewrite::rules::utils::{
    collect_column_id_refs_strict, collect_output_ids_opt, combine_and,
};
use crate::sql::optimizer::scalar::{self, ScalarArena, ScalarId};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Side {
    Left,
    Right,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct ForeignKeyConstraint {
    local_columns: Vec<String>,
    referenced_table: String,
    referenced_columns: Vec<String>,
}

pub(crate) struct PruneUkFkJoin;

impl LogicalRewriteRule for PruneUkFkJoin {
    fn name(&self) -> &'static str {
        "PruneUkFkJoin"
    }

    fn phase(&self) -> RewritePhase {
        RewritePhase::StructuralRewrite
    }

    fn matches(&self, expr: &OptExpr, _ctx: &RewriteContext) -> bool {
        matches!(&expr.op, Operator::LogicalProject(_))
            && expr.children.first().map(|c| matches!(&c.op, Operator::LogicalJoin(_))).unwrap_or(false)
    }

    fn apply(
        &self,
        expr: OptExpr,
        ctx: &mut RewriteContext,
    ) -> Result<RewriteResult, String> {
        let settings = current_session_optimizer_settings();
        let table_prune_enabled = settings.enable_query_rewrite_table_prune
            || settings.enable_cbo_table_prune
            || settings.enable_table_prune_on_update;
        if !table_prune_enabled && !settings.enable_ukfk_opt {
            return Ok(RewriteResult::Unchanged);
        }

        let OptExpr {
            op,
            mut children,
            required_output_columns,
        } = expr;
        let Operator::LogicalProject(project) = op else {
            return Ok(RewriteResult::Unchanged);
        };
        if children.len() != 1 {
            return Ok(RewriteResult::Unchanged);
        }
        let join_expr = children.remove(0);
        let OptExpr {
            op: join_op,
            children: mut join_children,
            required_output_columns: _,
        } = join_expr;
        let Operator::LogicalJoin(join) = join_op else {
            return Ok(RewriteResult::Unchanged);
        };
        if join_children.len() != 2 {
            return Ok(RewriteResult::Unchanged);
        }
        let right = join_children.remove(1);
        let left = join_children.remove(0);

        let arena_rc = ctx.scalar_arena();

        let retained_side = match project_referenced_side(&project.items, &left, &right, &arena_rc.borrow())? {
            Some(s) => s,
            None => return Ok(RewriteResult::Unchanged),
        };
        let eq_pairs = join_equality_pairs(&join, &left, &right, &arena_rc.borrow())?;
        if eq_pairs.is_empty() {
            return Ok(RewriteResult::Unchanged);
        }
        let left_cols: Vec<String> = eq_pairs.iter().map(|(left, _)| left.clone()).collect();
        let right_cols: Vec<String> = eq_pairs.iter().map(|(_, right)| right.clone()).collect();
        let left_scan = root_scan(&left);
        let right_scan = root_scan(&right);
        let (Some(left_scan), Some(right_scan)) = (left_scan, right_scan) else {
            return Ok(RewriteResult::Unchanged);
        };

        let retained = match (join.join_type, retained_side) {
            (JoinKind::LeftOuter, Side::Left)
                if table_prune_enabled && table_has_unique_key(right_scan, &right_cols) =>
            {
                Some(left.clone())
            }
            (JoinKind::RightOuter, Side::Right)
                if table_prune_enabled && table_has_unique_key(left_scan, &left_cols) =>
            {
                Some(right.clone())
            }
            (JoinKind::Inner, Side::Left)
                if settings.enable_ukfk_opt
                    && foreign_key_matches(left_scan, right_scan, &left_cols, &right_cols) =>
            {
                Some(add_not_null_filter(left.clone(), left_scan, &left_cols, &mut arena_rc.borrow_mut()))
            }
            (JoinKind::Inner, Side::Right)
                if settings.enable_ukfk_opt
                    && foreign_key_matches(right_scan, left_scan, &right_cols, &left_cols) =>
            {
                Some(add_not_null_filter(right.clone(), right_scan, &right_cols, &mut arena_rc.borrow_mut()))
            }
            _ => None,
        };
        let Some(retained) = retained else {
            return Ok(RewriteResult::Unchanged);
        };

        Ok(RewriteResult::Changed(OptExpr {
            op: Operator::LogicalProject(project),
            children: vec![retained],
            required_output_columns,
        }))
    }
}

pub(crate) struct EliminateUniqueAggregate;

impl LogicalRewriteRule for EliminateUniqueAggregate {
    fn name(&self) -> &'static str {
        "EliminateUniqueAggregate"
    }

    fn phase(&self) -> RewritePhase {
        RewritePhase::StructuralRewrite
    }

    fn matches(&self, expr: &OptExpr, _ctx: &RewriteContext) -> bool {
        matches!(&expr.op, Operator::LogicalProject(_))
            && expr.children.first().map(|c| matches!(&c.op, Operator::LogicalAggregate(_))).unwrap_or(false)
    }

    fn apply(
        &self,
        expr: OptExpr,
        ctx: &mut RewriteContext,
    ) -> Result<RewriteResult, String> {
        let settings = current_session_optimizer_settings();
        if !settings.enable_eliminate_agg {
            return Ok(RewriteResult::Unchanged);
        }

        let OptExpr {
            op,
            mut children,
            required_output_columns,
        } = expr;
        let Operator::LogicalProject(project) = op else {
            return Ok(RewriteResult::Unchanged);
        };
        if children.len() != 1 {
            return Ok(RewriteResult::Unchanged);
        }
        let aggregate_expr = children.remove(0);
        let OptExpr {
            op: agg_op,
            children: mut agg_children,
            required_output_columns: _,
        } = aggregate_expr;
        let Operator::LogicalAggregate(aggregate) = agg_op else {
            return Ok(RewriteResult::Unchanged);
        };
        if agg_children.len() != 1 {
            return Ok(RewriteResult::Unchanged);
        }
        let aggregate_input = agg_children.remove(0);
        let scan = match root_scan(&aggregate_input) {
            Some(s) => s,
            None => return Ok(RewriteResult::Unchanged),
        };

        let arena_rc = ctx.scalar_arena();
        let group_columns = match group_by_columns(&aggregate.group_by, &arena_rc.borrow()) {
            Some(cols) => cols,
            None => return Ok(RewriteResult::Unchanged),
        };
        if group_columns.is_empty() || !table_has_unique_key(scan, &group_columns) {
            return Ok(RewriteResult::Unchanged);
        }
        if aggregate.aggregates.is_empty()
            || !aggregate
                .aggregates
                .iter()
                .all(|a| is_eliminable_count(a, &arena_rc.borrow()))
        {
            return Ok(RewriteResult::Unchanged);
        }
        let items = project
            .items
            .into_iter()
            .map(|item| rewrite_eliminated_aggregate_project_item(item, &mut arena_rc.borrow_mut()))
            .collect::<Option<Vec<_>>>();
        let Some(items) = items else {
            return Ok(RewriteResult::Unchanged);
        };

        Ok(RewriteResult::Changed(OptExpr {
            op: Operator::LogicalProject(ProjectOp {
                items,
                output_qualifier: project.output_qualifier,
            }),
            children: vec![aggregate_input],
            required_output_columns,
        }))
    }
}

fn root_scan(expr: &OptExpr) -> Option<&ScanOp> {
    match &expr.op {
        Operator::LogicalScan(scan) => Some(scan),
        Operator::LogicalFilter(_) => root_scan(expr.unary_input()),
        _ => None,
    }
}

fn project_referenced_side(
    items: &[ScalarProjectItem],
    left: &OptExpr,
    right: &OptExpr,
    arena: &ScalarArena,
) -> Result<Option<Side>, String> {
    let mut left_ids = collect_output_ids_opt(left);
    let mut right_ids = collect_output_ids_opt(right);
    left_ids.remove(&ColumnId::UNSET);
    right_ids.remove(&ColumnId::UNSET);
    let mut side = None;
    for item in items {
        let item_expr = scalar::materialize(arena, item.expr);
        let ids = match collect_column_id_refs_strict(&item_expr) {
            Some(ids) => ids,
            None => return Ok(None),
        };
        if ids.is_empty() {
            continue;
        }
        let reference_side = match referenced_side(&ids, &left_ids, &right_ids) {
            Some(s) => s,
            None => return Ok(None),
        };
        if let Some(existing) = side {
            if existing != reference_side {
                return Ok(None);
            }
        } else {
            side = Some(reference_side);
        }
    }
    Ok(side)
}

fn join_equality_pairs(
    join: &LogicalJoinOp,
    left: &OptExpr,
    right: &OptExpr,
    arena: &ScalarArena,
) -> Result<Vec<(String, String)>, String> {
    let Some(cond_id) = join.condition else {
        return Ok(vec![]);
    };
    let condition = scalar::materialize(arena, cond_id);
    let mut left_ids = collect_output_ids_opt(left);
    let mut right_ids = collect_output_ids_opt(right);
    left_ids.remove(&ColumnId::UNSET);
    right_ids.remove(&ColumnId::UNSET);
    let mut pairs = Vec::new();
    let ok = collect_join_equality_pairs(&condition, &left_ids, &right_ids, &mut pairs);
    if ok.is_some() && !pairs.is_empty() {
        Ok(pairs)
    } else {
        Ok(vec![])
    }
}

fn collect_join_equality_pairs(
    expr: &TypedExpr,
    left_ids: &HashSet<ColumnId>,
    right_ids: &HashSet<ColumnId>,
    pairs: &mut Vec<(String, String)>,
) -> Option<()> {
    match &expr.kind {
        ExprKind::BinaryOp {
            left,
            op: BinOp::And,
            right,
        } => {
            collect_join_equality_pairs(left, left_ids, right_ids, pairs)?;
            collect_join_equality_pairs(right, left_ids, right_ids, pairs)
        }
        ExprKind::BinaryOp {
            left,
            op: BinOp::Eq,
            right,
        } => {
            let left_ref = classify_column_ref(left, left_ids, right_ids)?;
            let right_ref = classify_column_ref(right, left_ids, right_ids)?;
            match (left_ref, right_ref) {
                ((Side::Left, left_col), (Side::Right, right_col)) => {
                    pairs.push((left_col, right_col));
                    Some(())
                }
                ((Side::Right, right_col), (Side::Left, left_col)) => {
                    pairs.push((left_col, right_col));
                    Some(())
                }
                _ => None,
            }
        }
        _ => None,
    }
}

fn referenced_side(
    id_refs: &HashSet<ColumnId>,
    left_ids: &HashSet<ColumnId>,
    right_ids: &HashSet<ColumnId>,
) -> Option<Side> {
    let mut side = None;
    for id in id_refs {
        let reference_side = match (left_ids.contains(id), right_ids.contains(id)) {
            (true, false) => Side::Left,
            (false, true) => Side::Right,
            _ => return None,
        };
        if let Some(existing) = side {
            if existing != reference_side {
                return None;
            }
        } else {
            side = Some(reference_side);
        }
    }
    side
}

fn classify_column_ref(
    expr: &TypedExpr,
    left_ids: &HashSet<ColumnId>,
    right_ids: &HashSet<ColumnId>,
) -> Option<(Side, String)> {
    match &expr.kind {
        ExprKind::ColumnRef {
            column_id, column, ..
        } => {
            if *column_id == ColumnId::UNSET {
                return None;
            }
            match (left_ids.contains(column_id), right_ids.contains(column_id)) {
                (true, false) => Some((Side::Left, normalize_identifier(column))),
                (false, true) => Some((Side::Right, normalize_identifier(column))),
                _ => None,
            }
        }
        ExprKind::Cast { expr, .. } | ExprKind::Nested(expr) => {
            classify_column_ref(expr, left_ids, right_ids)
        }
        _ => None,
    }
}

fn group_by_columns(group_by: &[ScalarId], arena: &ScalarArena) -> Option<Vec<String>> {
    group_by
        .iter()
        .map(|id| {
            let expr = scalar::materialize(arena, *id);
            match &expr.kind {
                ExprKind::ColumnRef { column, .. } => Some(normalize_identifier(column)),
                _ => None,
            }
        })
        .collect()
}

fn table_has_unique_key(scan: &ScanOp, columns: &[String]) -> bool {
    unique_constraints(scan)
        .into_iter()
        .any(|constraint| same_columns(&constraint, columns))
}

fn foreign_key_matches(
    local_scan: &ScanOp,
    referenced_scan: &ScanOp,
    local_columns: &[String],
    referenced_columns: &[String],
) -> bool {
    if !table_has_unique_key(referenced_scan, referenced_columns) {
        return false;
    }
    foreign_key_constraints(local_scan).into_iter().any(|fk| {
        same_columns(&fk.local_columns, local_columns)
            && table_name_matches(referenced_scan, &fk.referenced_table)
            && same_columns(&fk.referenced_columns, referenced_columns)
    })
}

fn unique_constraints(scan: &ScanOp) -> Vec<Vec<String>> {
    let Some(value) = table_properties(scan).remove("unique_constraints") else {
        return Vec::new();
    };
    value.split(';').filter_map(parse_column_list).collect()
}

fn foreign_key_constraints(scan: &ScanOp) -> Vec<ForeignKeyConstraint> {
    let Some(value) = table_properties(scan).remove("foreign_key_constraints") else {
        return Vec::new();
    };
    value
        .split(';')
        .filter_map(parse_foreign_key_constraint)
        .collect()
}

fn table_properties(scan: &ScanOp) -> HashMap<String, String> {
    let Some(serialized_metadata) =
        iceberg_table_info(&scan.table.source).and_then(|table| table.serialized_metadata.as_ref())
    else {
        return HashMap::new();
    };
    let Ok(metadata) = serde_json::from_str::<iceberg::spec::TableMetadata>(serialized_metadata)
    else {
        return HashMap::new();
    };
    metadata
        .properties()
        .iter()
        .map(|(key, value)| (key.to_ascii_lowercase(), value.clone()))
        .collect()
}

fn iceberg_table_info(
    source: &crate::sql::catalog::ScanSource,
) -> Option<&crate::sql::catalog::IcebergTableInfo> {
    match source {
        crate::sql::catalog::ScanSource::IcebergDataFiles { table, .. }
        | crate::sql::catalog::ScanSource::IcebergMetadataTable { table, .. }
        | crate::sql::catalog::ScanSource::IcebergDeltaTable { table, .. }
        | crate::sql::catalog::ScanSource::IcebergVersionTable { table, .. } => Some(table),
        crate::sql::catalog::ScanSource::StarRocks { .. }
        | crate::sql::catalog::ScanSource::IcebergMvTargetState { .. } => None,
    }
}

fn parse_foreign_key_constraint(raw: &str) -> Option<ForeignKeyConstraint> {
    let raw = raw.trim().trim_end_matches(';').trim();
    if raw.is_empty() {
        return None;
    }
    let references_idx = raw.to_ascii_lowercase().find("references")?;
    let left = raw[..references_idx].trim();
    let right = raw[references_idx + "references".len()..].trim();
    let local_columns = parse_column_list(left)?;
    let open = right.find('(')?;
    let referenced_table = normalize_table_name(&right[..open]);
    let referenced_columns = parse_column_list(right)?;
    if referenced_table.is_empty() || local_columns.is_empty() || referenced_columns.is_empty() {
        return None;
    }
    Some(ForeignKeyConstraint {
        local_columns,
        referenced_table,
        referenced_columns,
    })
}

fn parse_column_list(raw: &str) -> Option<Vec<String>> {
    let segment = if let Some(open) = raw.find('(') {
        let close = raw[open + 1..].find(')')? + open + 1;
        &raw[open + 1..close]
    } else {
        raw
    };
    let columns = segment
        .split(',')
        .map(normalize_identifier)
        .filter(|column| !column.is_empty())
        .collect::<Vec<_>>();
    (!columns.is_empty()).then_some(columns)
}

fn same_columns(left: &[String], right: &[String]) -> bool {
    if left.len() != right.len() {
        return false;
    }
    let left: HashSet<&str> = left.iter().map(String::as_str).collect();
    right.iter().all(|column| left.contains(column.as_str()))
}

fn table_name_matches(scan: &ScanOp, raw_table: &str) -> bool {
    let table = normalize_table_name(raw_table);
    if table.eq_ignore_ascii_case(&scan.table.name) {
        return true;
    }
    scan.alias
        .as_ref()
        .is_some_and(|alias| table.eq_ignore_ascii_case(alias))
}

fn normalize_identifier(raw: &str) -> String {
    let trimmed = raw
        .trim()
        .trim_matches('`')
        .trim_matches('"')
        .trim_matches('\'');
    let leaf = trimmed.rsplit('.').next().unwrap_or(trimmed);
    leaf.trim()
        .trim_matches('`')
        .trim_matches('"')
        .trim_matches('\'')
        .to_ascii_lowercase()
}

fn normalize_table_name(raw: &str) -> String {
    normalize_identifier(raw)
}

fn add_not_null_filter(
    plan: OptExpr,
    scan: &ScanOp,
    columns: &[String],
    arena: &mut ScalarArena,
) -> OptExpr {
    let qualifier = scan
        .alias
        .clone()
        .unwrap_or_else(|| scan.table.name.clone());
    let predicates = columns
        .iter()
        .filter_map(|column| {
            scan.columns
                .iter()
                .find(|candidate| candidate.name.eq_ignore_ascii_case(column))
                .filter(|output| output.column_id != ColumnId::UNSET)
                .map(|output| TypedExpr {
                    data_type: DataType::Boolean,
                    nullable: false,
                    kind: ExprKind::IsNull {
                        expr: Box::new(TypedExpr {
                            data_type: output.data_type.clone(),
                            nullable: output.nullable,
                            kind: ExprKind::ColumnRef {
                                column_id: output.column_id,
                                qualifier: Some(qualifier.clone()),
                                column: output.name.clone(),
                            },
                        }),
                        negated: true,
                    },
                })
        })
        .collect::<Vec<_>>();
    if predicates.is_empty() {
        return plan;
    }
    let predicate = scalar::intern_typed(arena, &combine_and(predicates));
    OptExpr::new(
        Operator::LogicalFilter(FilterOp { predicate }),
        vec![plan],
    )
}

fn is_eliminable_count(aggregate: &ScalarAggregateSpec, arena: &ScalarArena) -> bool {
    if !aggregate.name.eq_ignore_ascii_case("count") {
        return false;
    }
    if aggregate.distinct {
        return false;
    }
    if !aggregate.order_by.is_empty() {
        return false;
    }
    aggregate.args.iter().all(|id| {
        let expr = scalar::materialize(arena, *id);
        matches!(
            expr.kind,
            ExprKind::Literal(LiteralValue::Int(_)) | ExprKind::Literal(LiteralValue::Null)
        )
    })
}

fn rewrite_eliminated_aggregate_project_item(
    item: ScalarProjectItem,
    arena: &mut ScalarArena,
) -> Option<ScalarProjectItem> {
    let item_expr = scalar::materialize(arena, item.expr);
    let rewritten = rewrite_eliminated_aggregate_expr(item_expr)?;
    let new_expr_id = scalar::intern_typed(arena, &rewritten);
    Some(ScalarProjectItem {
        expr: new_expr_id,
        output_name: item.output_name,
        output_column_id: item.output_column_id,
        expr_display: item.expr_display,
    })
}

fn rewrite_eliminated_aggregate_expr(expr: TypedExpr) -> Option<TypedExpr> {
    match expr.kind {
        ExprKind::AggregateCall {
            name,
            distinct,
            order_by,
            ..
        } if name.eq_ignore_ascii_case("count") && !distinct && order_by.is_empty() => {
            Some(TypedExpr {
                data_type: expr.data_type,
                nullable: false,
                kind: ExprKind::Literal(LiteralValue::Int(1)),
            })
        }
        _ if !contains_aggregate(&expr) => Some(expr),
        _ => None,
    }
}

fn contains_aggregate(expr: &TypedExpr) -> bool {
    match &expr.kind {
        ExprKind::AggregateCall { .. } => true,
        ExprKind::BinaryOp { left, right, .. } => {
            contains_aggregate(left) || contains_aggregate(right)
        }
        ExprKind::UnaryOp { expr, .. }
        | ExprKind::Cast { expr, .. }
        | ExprKind::IsNull { expr, .. }
        | ExprKind::Nested(expr) => contains_aggregate(expr),
        ExprKind::FunctionCall { args, .. } => args.iter().any(contains_aggregate),
        ExprKind::LambdaFunction { body, .. } => contains_aggregate(body),
        ExprKind::InList { expr, list, .. } => {
            contains_aggregate(expr) || list.iter().any(contains_aggregate)
        }
        ExprKind::Between {
            expr, low, high, ..
        } => contains_aggregate(expr) || contains_aggregate(low) || contains_aggregate(high),
        ExprKind::Like { expr, pattern, .. } => {
            contains_aggregate(expr) || contains_aggregate(pattern)
        }
        ExprKind::Case {
            operand,
            when_then,
            else_expr,
        } => {
            operand
                .as_ref()
                .is_some_and(|expr| contains_aggregate(expr))
                || when_then
                    .iter()
                    .any(|(when, then)| contains_aggregate(when) || contains_aggregate(then))
                || else_expr
                    .as_ref()
                    .is_some_and(|expr| contains_aggregate(expr))
        }
        ExprKind::IsTruthValue { expr, .. } => contains_aggregate(expr),
        ExprKind::WindowCall {
            args,
            partition_by,
            order_by,
            ..
        } => {
            args.iter().any(contains_aggregate)
                || partition_by.iter().any(contains_aggregate)
                || order_by.iter().any(|item| contains_aggregate(&item.expr))
        }
        ExprKind::Lambda { body, .. } => contains_aggregate(body),
        ExprKind::ColumnRef { .. }
        | ExprKind::LambdaParamRef { .. }
        | ExprKind::Literal(_)
        | ExprKind::SubqueryPlaceholder { .. } => false,
    }
}
