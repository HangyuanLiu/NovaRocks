use arrow::datatypes::DataType;

use crate::exec::expr::function::variant::variant_get_target_type;
use crate::exec::variant::{VariantPathSegment, parse_variant_path};
use crate::sql::analysis::{ExprKind, LiteralValue, OutputColumn, SortItem, TypedExpr};
use crate::sql::catalog::ScanSource;
use crate::sql::column_id::{ColumnId, ColumnRefFactory};
use crate::sql::optimizer::operator::{FilterOp, Operator, ProjectOp, ScanOp, ScanVariantColumn};
use crate::sql::optimizer::opt_expr::OptExpr;
use crate::sql::optimizer::rewrite::context::RewriteContext;
use crate::sql::optimizer::rewrite::phase::RewritePhase;
use crate::sql::optimizer::rewrite::result::RewriteResult;
use crate::sql::optimizer::rewrite::rule::{LogicalRewriteRule, RewriteTraversal};
use crate::sql::optimizer::scalar::{self, ScalarArena};

#[derive(Default)]
pub(crate) struct VariantPathPushdownRule;

#[derive(Clone, Debug, PartialEq, Eq)]
struct VariantRequest {
    source_column_id: ColumnId,
    canonical_path: String,
    requested_type: DataType,
    strict: bool,
}

impl LogicalRewriteRule for VariantPathPushdownRule {
    fn name(&self) -> &'static str {
        "VariantPathPushdown"
    }

    fn phase(&self) -> RewritePhase {
        RewritePhase::StructuralRewrite
    }

    fn traversal(&self) -> RewriteTraversal {
        RewriteTraversal::TopDown
    }

    fn matches(&self, expr: &OptExpr, ctx: &RewriteContext) -> bool {
        let arena_rc = ctx.scalar_arena();
        let arena = arena_rc.borrow();
        match &expr.op {
            Operator::LogicalFilter(filter) => {
                let pred = scalar::materialize(&arena, filter.predicate);
                contains_variant_get_candidate(&pred)
            }
            Operator::LogicalProject(project) => project.items.iter().any(|item| {
                let expr = scalar::materialize(&arena, item.expr);
                contains_variant_get_candidate(&expr)
            }),
            Operator::LogicalScan(scan) => scan.predicates.iter().any(|id| {
                let pred = scalar::materialize(&arena, *id);
                contains_variant_get_candidate(&pred)
            }),
            _ => false,
        }
    }

    fn apply(
        &self,
        mut expr: OptExpr,
        ctx: &mut RewriteContext,
    ) -> Result<RewriteResult, String> {
        let Some(factory) = ctx.column_ref_factory().cloned() else {
            return Ok(RewriteResult::Unchanged);
        };
        let mut factory = factory.borrow_mut();
        let arena_rc = ctx.scalar_arena();

        let changed = match &expr.op {
            Operator::LogicalFilter(_) => {
                let Some(input) = expr.children.get_mut(0) else {
                    return Ok(RewriteResult::Unchanged);
                };
                // Materialize the filter predicate, rewrite it, re-intern.
                let filter_op = match &expr.op {
                    Operator::LogicalFilter(f) => f.clone(),
                    _ => unreachable!(),
                };
                let pred_typed = scalar::materialize(&arena_rc.borrow(), filter_op.predicate);
                let mut pred_typed = pred_typed;
                let changed = {
                    let arena = arena_rc.borrow();
                    rewrite_expr(&mut pred_typed, input, &mut factory, &arena)?
                };
                if changed {
                    let new_pred_id = scalar::intern_typed(&mut arena_rc.borrow_mut(), &pred_typed);
                    expr.op = Operator::LogicalFilter(FilterOp { predicate: new_pred_id });
                }
                changed
            }
            Operator::LogicalProject(_) => {
                let Some(input) = expr.children.get_mut(0) else {
                    return Ok(RewriteResult::Unchanged);
                };
                let project_op = match &expr.op {
                    Operator::LogicalProject(p) => p.clone(),
                    _ => unreachable!(),
                };
                let mut items = project_op.items;
                let mut changed = false;
                for item in &mut items {
                    let mut item_expr = scalar::materialize(&arena_rc.borrow(), item.expr);
                    let c = {
                        let arena = arena_rc.borrow();
                        rewrite_expr(&mut item_expr, input, &mut factory, &arena)?
                    };
                    if c {
                        item.expr = scalar::intern_typed(&mut arena_rc.borrow_mut(), &item_expr);
                    }
                    changed |= c;
                }
                if changed {
                    expr.op = Operator::LogicalProject(ProjectOp {
                        items,
                        output_qualifier: project_op.output_qualifier,
                    });
                }
                changed
            }
            Operator::LogicalScan(_) => {
                let scan_op = match &expr.op {
                    Operator::LogicalScan(s) => s.clone(),
                    _ => unreachable!(),
                };
                // We need &mut ScanOp to call rewrite_scan_predicates.
                // Temporarily take it out, mutate, put back.
                let mut scan = scan_op;
                let changed = rewrite_scan_predicates(&mut scan, &mut factory, &mut arena_rc.borrow_mut())?;
                if changed {
                    expr.op = Operator::LogicalScan(scan);
                }
                changed
            }
            _ => false,
        };

        if changed {
            Ok(RewriteResult::Changed(expr))
        } else {
            Ok(RewriteResult::Unchanged)
        }
    }
}

fn rewrite_scan_predicates(
    scan: &mut ScanOp,
    factory: &mut ColumnRefFactory,
    arena: &mut ScalarArena,
) -> Result<bool, String> {
    let pred_ids = std::mem::take(&mut scan.predicates);
    let mut new_pred_ids = Vec::with_capacity(pred_ids.len());
    let mut changed = false;
    for pred_id in pred_ids {
        let mut pred = scalar::materialize(arena, pred_id);
        let c = rewrite_expr_against_scan(&mut pred, scan, factory)?;
        changed |= c;
        let new_id = if c {
            scalar::intern_typed(arena, &pred)
        } else {
            pred_id
        };
        new_pred_ids.push(new_id);
    }
    scan.predicates = new_pred_ids;
    Ok(changed)
}

fn rewrite_expr(
    expr: &mut TypedExpr,
    scan_root: &mut OptExpr,
    factory: &mut ColumnRefFactory,
    arena: &ScalarArena,
) -> Result<bool, String> {
    if let Some(replacement) = replacement_for_variant_get(expr, scan_root, factory, arena)? {
        *expr = replacement;
        return Ok(true);
    }

    let mut changed = false;
    match &mut expr.kind {
        ExprKind::BinaryOp { left, right, .. } => {
            changed |= rewrite_expr(left, scan_root, factory, arena)?;
            changed |= rewrite_expr(right, scan_root, factory, arena)?;
        }
        ExprKind::UnaryOp { expr, .. }
        | ExprKind::Cast { expr, .. }
        | ExprKind::IsNull { expr, .. }
        | ExprKind::IsTruthValue { expr, .. }
        | ExprKind::Nested(expr) => {
            changed |= rewrite_expr(expr, scan_root, factory, arena)?;
        }
        ExprKind::FunctionCall { args, .. } | ExprKind::AggregateCall { args, .. } => {
            changed |= rewrite_expr_list(args, scan_root, factory, arena)?;
            if let ExprKind::AggregateCall { order_by, .. } = &mut expr.kind {
                changed |= rewrite_sort_items(order_by, scan_root, factory, arena)?;
            }
        }
        ExprKind::LambdaFunction { body, .. } | ExprKind::Lambda { body, .. } => {
            changed |= rewrite_expr(body, scan_root, factory, arena)?;
        }
        ExprKind::InList {
            expr: input, list, ..
        } => {
            changed |= rewrite_expr(input, scan_root, factory, arena)?;
            changed |= rewrite_expr_list(list, scan_root, factory, arena)?;
        }
        ExprKind::Between {
            expr: input,
            low,
            high,
            ..
        } => {
            changed |= rewrite_expr(input, scan_root, factory, arena)?;
            changed |= rewrite_expr(low, scan_root, factory, arena)?;
            changed |= rewrite_expr(high, scan_root, factory, arena)?;
        }
        ExprKind::Like {
            expr: input,
            pattern,
            ..
        } => {
            changed |= rewrite_expr(input, scan_root, factory, arena)?;
            changed |= rewrite_expr(pattern, scan_root, factory, arena)?;
        }
        ExprKind::Case {
            operand,
            when_then,
            else_expr,
        } => {
            if let Some(operand) = operand {
                changed |= rewrite_expr(operand, scan_root, factory, arena)?;
            }
            for (when, then) in when_then {
                changed |= rewrite_expr(when, scan_root, factory, arena)?;
                changed |= rewrite_expr(then, scan_root, factory, arena)?;
            }
            if let Some(else_expr) = else_expr {
                changed |= rewrite_expr(else_expr, scan_root, factory, arena)?;
            }
        }
        ExprKind::WindowCall {
            args,
            partition_by,
            order_by,
            ..
        } => {
            changed |= rewrite_expr_list(args, scan_root, factory, arena)?;
            changed |= rewrite_expr_list(partition_by, scan_root, factory, arena)?;
            changed |= rewrite_sort_items(order_by, scan_root, factory, arena)?;
        }
        ExprKind::ColumnRef { .. }
        | ExprKind::LambdaParamRef { .. }
        | ExprKind::Literal(_)
        | ExprKind::SubqueryPlaceholder { .. } => {}
    }
    Ok(changed)
}

fn rewrite_expr_against_scan(
    expr: &mut TypedExpr,
    scan: &mut ScanOp,
    factory: &mut ColumnRefFactory,
) -> Result<bool, String> {
    if let Some(replacement) = replacement_for_variant_get_against_scan(expr, scan, factory)? {
        *expr = replacement;
        return Ok(true);
    }

    let mut changed = false;
    match &mut expr.kind {
        ExprKind::BinaryOp { left, right, .. } => {
            changed |= rewrite_expr_against_scan(left, scan, factory)?;
            changed |= rewrite_expr_against_scan(right, scan, factory)?;
        }
        ExprKind::UnaryOp { expr, .. }
        | ExprKind::Cast { expr, .. }
        | ExprKind::IsNull { expr, .. }
        | ExprKind::IsTruthValue { expr, .. }
        | ExprKind::Nested(expr) => {
            changed |= rewrite_expr_against_scan(expr, scan, factory)?;
        }
        ExprKind::FunctionCall { args, .. } | ExprKind::AggregateCall { args, .. } => {
            changed |= rewrite_expr_list_against_scan(args, scan, factory)?;
            if let ExprKind::AggregateCall { order_by, .. } = &mut expr.kind {
                changed |= rewrite_sort_items_against_scan(order_by, scan, factory)?;
            }
        }
        ExprKind::LambdaFunction { body, .. } | ExprKind::Lambda { body, .. } => {
            changed |= rewrite_expr_against_scan(body, scan, factory)?;
        }
        ExprKind::InList {
            expr: input, list, ..
        } => {
            changed |= rewrite_expr_against_scan(input, scan, factory)?;
            changed |= rewrite_expr_list_against_scan(list, scan, factory)?;
        }
        ExprKind::Between {
            expr: input,
            low,
            high,
            ..
        } => {
            changed |= rewrite_expr_against_scan(input, scan, factory)?;
            changed |= rewrite_expr_against_scan(low, scan, factory)?;
            changed |= rewrite_expr_against_scan(high, scan, factory)?;
        }
        ExprKind::Like {
            expr: input,
            pattern,
            ..
        } => {
            changed |= rewrite_expr_against_scan(input, scan, factory)?;
            changed |= rewrite_expr_against_scan(pattern, scan, factory)?;
        }
        ExprKind::Case {
            operand,
            when_then,
            else_expr,
        } => {
            if let Some(operand) = operand {
                changed |= rewrite_expr_against_scan(operand, scan, factory)?;
            }
            for (when, then) in when_then {
                changed |= rewrite_expr_against_scan(when, scan, factory)?;
                changed |= rewrite_expr_against_scan(then, scan, factory)?;
            }
            if let Some(else_expr) = else_expr {
                changed |= rewrite_expr_against_scan(else_expr, scan, factory)?;
            }
        }
        ExprKind::WindowCall {
            args,
            partition_by,
            order_by,
            ..
        } => {
            changed |= rewrite_expr_list_against_scan(args, scan, factory)?;
            changed |= rewrite_expr_list_against_scan(partition_by, scan, factory)?;
            changed |= rewrite_sort_items_against_scan(order_by, scan, factory)?;
        }
        ExprKind::ColumnRef { .. }
        | ExprKind::LambdaParamRef { .. }
        | ExprKind::Literal(_)
        | ExprKind::SubqueryPlaceholder { .. } => {}
    }
    Ok(changed)
}

fn rewrite_expr_list(
    exprs: &mut [TypedExpr],
    scan_root: &mut OptExpr,
    factory: &mut ColumnRefFactory,
    arena: &ScalarArena,
) -> Result<bool, String> {
    let mut changed = false;
    for expr in exprs {
        changed |= rewrite_expr(expr, scan_root, factory, arena)?;
    }
    Ok(changed)
}

fn rewrite_expr_list_against_scan(
    exprs: &mut [TypedExpr],
    scan: &mut ScanOp,
    factory: &mut ColumnRefFactory,
) -> Result<bool, String> {
    let mut changed = false;
    for expr in exprs {
        changed |= rewrite_expr_against_scan(expr, scan, factory)?;
    }
    Ok(changed)
}

fn rewrite_sort_items(
    items: &mut [SortItem],
    scan_root: &mut OptExpr,
    factory: &mut ColumnRefFactory,
    arena: &ScalarArena,
) -> Result<bool, String> {
    let mut changed = false;
    for item in items {
        changed |= rewrite_expr(&mut item.expr, scan_root, factory, arena)?;
    }
    Ok(changed)
}

fn rewrite_sort_items_against_scan(
    items: &mut [SortItem],
    scan: &mut ScanOp,
    factory: &mut ColumnRefFactory,
) -> Result<bool, String> {
    let mut changed = false;
    for item in items {
        changed |= rewrite_expr_against_scan(&mut item.expr, scan, factory)?;
    }
    Ok(changed)
}

fn replacement_for_variant_get(
    expr: &TypedExpr,
    scan_root: &mut OptExpr,
    factory: &mut ColumnRefFactory,
    arena: &ScalarArena,
) -> Result<Option<TypedExpr>, String> {
    let Some(request) = variant_request(expr) else {
        return Ok(None);
    };
    Ok(find_or_create_slot(scan_root, &request, factory, arena))
}

fn replacement_for_variant_get_against_scan(
    expr: &TypedExpr,
    scan: &mut ScanOp,
    factory: &mut ColumnRefFactory,
) -> Result<Option<TypedExpr>, String> {
    let Some(request) = variant_request(expr) else {
        return Ok(None);
    };
    Ok(find_or_create_slot_on_scan(scan, &request, factory))
}

fn variant_request(expr: &TypedExpr) -> Option<VariantRequest> {
    let ExprKind::FunctionCall {
        name,
        args,
        distinct,
    } = &expr.kind
    else {
        return None;
    };
    if *distinct || args.len() != 3 {
        return None;
    }

    let strict = if name.eq_ignore_ascii_case("variant_get") {
        true
    } else if name.eq_ignore_ascii_case("try_variant_get") {
        false
    } else {
        return None;
    };

    let ExprKind::ColumnRef { column_id, .. } = &args[0].kind else {
        return None;
    };
    if *column_id == ColumnId::UNSET {
        return None;
    }
    let path = string_literal_value(&args[1])?;
    let requested_type = requested_type_value(&args[2])?;
    let canonical_path = canonical_object_path(path)?;

    Some(VariantRequest {
        source_column_id: *column_id,
        canonical_path,
        requested_type,
        strict,
    })
}

fn string_literal_value(expr: &TypedExpr) -> Option<&str> {
    match &expr.kind {
        ExprKind::Literal(LiteralValue::String(value)) => Some(value),
        _ => None,
    }
}

fn requested_type_value(expr: &TypedExpr) -> Option<DataType> {
    let value = string_literal_value(expr)?;
    let data_type = variant_get_target_type(value).ok()?;
    match data_type {
        DataType::Boolean
        | DataType::Int64
        | DataType::Float64
        | DataType::Utf8
        | DataType::Date32 => Some(data_type),
        _ => None,
    }
}

fn canonical_object_path(path: &str) -> Option<String> {
    let parsed = parse_variant_path(path).ok()?;
    if parsed.segments.is_empty() {
        return None;
    }
    let mut out = String::from("$");
    for segment in parsed.segments {
        let VariantPathSegment::ObjectKey(key) = segment else {
            return None;
        };
        append_canonical_key(&mut out, &key);
    }
    Some(out)
}

fn append_canonical_key(out: &mut String, key: &str) {
    if is_plain_path_key(key) {
        out.push('.');
        out.push_str(key);
        return;
    }
    out.push_str("['");
    for ch in key.chars() {
        match ch {
            '\\' => out.push_str("\\\\"),
            '\'' => out.push_str("\\'"),
            _ => out.push(ch),
        }
    }
    out.push_str("']");
}

fn is_plain_path_key(key: &str) -> bool {
    !key.is_empty() && key.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_')
}

/// Walk the OptExpr tree looking for a scan to add a variant column slot to.
/// Mirrors the old `find_or_create_slot` which traversed `LogicalPlanNode`.
fn find_or_create_slot(
    expr: &mut OptExpr,
    request: &VariantRequest,
    factory: &mut ColumnRefFactory,
    arena: &ScalarArena,
) -> Option<TypedExpr> {
    match &expr.op {
        Operator::LogicalScan(scan) => {
            // For strict requests, only push when the scan has no predicates of
            // its own (same condition as the pre-OptExpr implementation).
            let scan_clone = scan.clone();
            let can_push = !request.strict
                || scan_clone.predicates.is_empty()
                || scan_clone.predicates.iter().any(|pred_id| {
                    let pred = scalar::materialize(arena, *pred_id);
                    expr_contains_variant_request(&pred, request)
                });
            if can_push {
                let Operator::LogicalScan(scan_mut) = &mut expr.op else { return None; };
                find_or_create_slot_on_scan(scan_mut, request, factory)
            } else {
                None
            }
        }
        Operator::LogicalFilter(filter_op) => {
            // For strict requests, only descend through a Filter whose predicate
            // contains the same variant_request — this preserves the pre-OptExpr
            // semantics that prevented spurious pushdown of unrelated projections.
            if !request.strict {
                let Some(input) = expr.children.get_mut(0) else {
                    return None;
                };
                find_or_create_slot(input, request, factory, arena)
            } else {
                let pred = scalar::materialize(arena, filter_op.predicate);
                if expr_contains_variant_request(&pred, request) {
                    let Some(input) = expr.children.get_mut(0) else {
                        return None;
                    };
                    find_or_create_slot(input, request, factory, arena)
                } else {
                    None
                }
            }
        }
        _ => None,
    }
}

fn expr_contains_variant_request(expr: &TypedExpr, request: &VariantRequest) -> bool {
    if variant_request(expr).is_some_and(|candidate| candidate == *request) {
        return true;
    }

    match &expr.kind {
        ExprKind::BinaryOp { left, right, .. } => {
            expr_contains_variant_request(left, request)
                || expr_contains_variant_request(right, request)
        }
        ExprKind::UnaryOp { expr, .. }
        | ExprKind::Cast { expr, .. }
        | ExprKind::IsNull { expr, .. }
        | ExprKind::IsTruthValue { expr, .. }
        | ExprKind::Nested(expr) => expr_contains_variant_request(expr, request),
        ExprKind::FunctionCall { args, .. } => args
            .iter()
            .any(|arg| expr_contains_variant_request(arg, request)),
        ExprKind::AggregateCall { args, order_by, .. } => {
            args.iter()
                .any(|arg| expr_contains_variant_request(arg, request))
                || order_by
                    .iter()
                    .any(|item| expr_contains_variant_request(&item.expr, request))
        }
        ExprKind::WindowCall {
            args,
            partition_by,
            order_by,
            ..
        } => {
            args.iter()
                .any(|arg| expr_contains_variant_request(arg, request))
                || partition_by
                    .iter()
                    .any(|expr| expr_contains_variant_request(expr, request))
                || order_by
                    .iter()
                    .any(|item| expr_contains_variant_request(&item.expr, request))
        }
        ExprKind::LambdaFunction { body, .. } | ExprKind::Lambda { body, .. } => {
            expr_contains_variant_request(body, request)
        }
        ExprKind::InList { expr, list, .. } => {
            expr_contains_variant_request(expr, request)
                || list
                    .iter()
                    .any(|item| expr_contains_variant_request(item, request))
        }
        ExprKind::Between {
            expr, low, high, ..
        } => {
            expr_contains_variant_request(expr, request)
                || expr_contains_variant_request(low, request)
                || expr_contains_variant_request(high, request)
        }
        ExprKind::Like { expr, pattern, .. } => {
            expr_contains_variant_request(expr, request)
                || expr_contains_variant_request(pattern, request)
        }
        ExprKind::Case {
            operand,
            when_then,
            else_expr,
        } => {
            operand
                .as_deref()
                .is_some_and(|expr| expr_contains_variant_request(expr, request))
                || when_then.iter().any(|(when, then)| {
                    expr_contains_variant_request(when, request)
                        || expr_contains_variant_request(then, request)
                })
                || else_expr
                    .as_deref()
                    .is_some_and(|expr| expr_contains_variant_request(expr, request))
        }
        ExprKind::ColumnRef { .. }
        | ExprKind::LambdaParamRef { .. }
        | ExprKind::Literal(_)
        | ExprKind::SubqueryPlaceholder { .. } => false,
    }
}

fn find_or_create_slot_on_scan(
    scan: &mut ScanOp,
    request: &VariantRequest,
    factory: &mut ColumnRefFactory,
) -> Option<TypedExpr> {
    if !matches!(scan.table.source, ScanSource::IcebergDataFiles { .. }) {
        return None;
    }

    if let Some(existing) = scan.variant_columns.iter().find(|column| {
        column.source_column_id == request.source_column_id
            && column.canonical_path == request.canonical_path
            && column.requested_type == request.requested_type
            && column.strict == request.strict
    }) {
        return Some(column_ref_for_variant_slot(existing));
    }

    let source_column = scan
        .columns
        .iter()
        .find(|column| column.column_id == request.source_column_id)?;
    if source_column.data_type != DataType::LargeBinary {
        return None;
    }

    let source_name = source_column.name.clone();
    let synthetic_name = next_synthetic_column_name(scan, &source_name);
    let synthetic_column_id = factory.create(
        None,
        synthetic_name.clone(),
        request.requested_type.clone(),
        true,
    );
    let descriptor = ScanVariantColumn {
        source_column_id: request.source_column_id,
        source_column: source_name,
        synthetic_column_id,
        synthetic_column: synthetic_name.clone(),
        canonical_path: request.canonical_path.clone(),
        requested_type: request.requested_type.clone(),
        strict: request.strict,
    };
    scan.columns.push(OutputColumn {
        column_id: synthetic_column_id,
        name: synthetic_name,
        data_type: request.requested_type.clone(),
        nullable: true,
        // Optimizer-managed scan output must survive pruning until the
        // lowering/codegen path consumes `variant_columns`.
        is_internal: true,
    });
    scan.variant_columns.push(descriptor);
    let descriptor = scan.variant_columns.last().expect("variant descriptor");
    Some(column_ref_for_variant_slot(descriptor))
}

fn column_ref_for_variant_slot(descriptor: &ScanVariantColumn) -> TypedExpr {
    TypedExpr {
        kind: ExprKind::ColumnRef {
            column_id: descriptor.synthetic_column_id,
            qualifier: None,
            column: descriptor.synthetic_column.clone(),
        },
        data_type: descriptor.requested_type.clone(),
        nullable: true,
    }
}

fn next_synthetic_column_name(scan: &ScanOp, source_column: &str) -> String {
    let source = sanitize_column_name(source_column);
    let mut ordinal = scan.variant_columns.len();
    loop {
        let candidate = format!("__nr_var_{source}_{ordinal}");
        if !scan.columns.iter().any(|column| column.name == candidate) {
            return candidate;
        }
        ordinal += 1;
    }
}

fn sanitize_column_name(name: &str) -> String {
    let mut out = String::with_capacity(name.len());
    for ch in name.chars() {
        if ch.is_ascii_alphanumeric() || ch == '_' {
            out.push(ch);
        } else {
            out.push('_');
        }
    }
    if out.is_empty() {
        "col".to_string()
    } else {
        out
    }
}

fn contains_variant_get_candidate(expr: &TypedExpr) -> bool {
    if let ExprKind::FunctionCall { name, args, .. } = &expr.kind
        && args.len() == 3
        && (name.eq_ignore_ascii_case("variant_get")
            || name.eq_ignore_ascii_case("try_variant_get"))
    {
        return true;
    }

    match &expr.kind {
        ExprKind::BinaryOp { left, right, .. } => {
            contains_variant_get_candidate(left) || contains_variant_get_candidate(right)
        }
        ExprKind::UnaryOp { expr, .. }
        | ExprKind::Cast { expr, .. }
        | ExprKind::IsNull { expr, .. }
        | ExprKind::IsTruthValue { expr, .. }
        | ExprKind::Nested(expr) => contains_variant_get_candidate(expr),
        ExprKind::FunctionCall { args, .. } => args.iter().any(contains_variant_get_candidate),
        ExprKind::AggregateCall { args, order_by, .. } => {
            args.iter().any(contains_variant_get_candidate)
                || order_by
                    .iter()
                    .any(|item| contains_variant_get_candidate(&item.expr))
        }
        ExprKind::WindowCall {
            args,
            partition_by,
            order_by,
            ..
        } => {
            args.iter().any(contains_variant_get_candidate)
                || partition_by.iter().any(contains_variant_get_candidate)
                || order_by
                    .iter()
                    .any(|item| contains_variant_get_candidate(&item.expr))
        }
        ExprKind::LambdaFunction { body, .. } | ExprKind::Lambda { body, .. } => {
            contains_variant_get_candidate(body)
        }
        ExprKind::InList { expr, list, .. } => {
            contains_variant_get_candidate(expr) || list.iter().any(contains_variant_get_candidate)
        }
        ExprKind::Between {
            expr, low, high, ..
        } => {
            contains_variant_get_candidate(expr)
                || contains_variant_get_candidate(low)
                || contains_variant_get_candidate(high)
        }
        ExprKind::Like { expr, pattern, .. } => {
            contains_variant_get_candidate(expr) || contains_variant_get_candidate(pattern)
        }
        ExprKind::Case {
            operand,
            when_then,
            else_expr,
        } => {
            operand
                .as_deref()
                .is_some_and(contains_variant_get_candidate)
                || when_then.iter().any(|(when, then)| {
                    contains_variant_get_candidate(when) || contains_variant_get_candidate(then)
                })
                || else_expr
                    .as_deref()
                    .is_some_and(contains_variant_get_candidate)
        }
        ExprKind::ColumnRef { .. }
        | ExprKind::LambdaParamRef { .. }
        | ExprKind::Literal(_)
        | ExprKind::SubqueryPlaceholder { .. } => false,
    }
}
