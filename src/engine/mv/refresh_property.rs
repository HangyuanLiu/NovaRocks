//! Capability property algebra for Iceberg IMV refresh.
//!
//! This module synthesizes a `RefreshFragmentProperty` (a `TargetIdentity` +
//! `StateContract` + base refs + branch count) recursively over an analyzed MV
//! query. It is the structural successor to the flat classifier in
//! [`crate::engine::mv::refresh_contract`]: it MIRRORS the exact same
//! acceptance/rejection of query shapes (non-inner / non-equi joins,
//! non-UNION-ALL set ops, metadata / delta / generate-series / unnest / CTE
//! relations, DISTINCT, HAVING, ROLLUP/CUBE/GROUPING SETS, ORDER BY / LIMIT /
//! OFFSET, WITH, unsupported / non-deterministic expressions, etc.) but instead
//! of emitting a closed enum of named strategies it emits a compositional
//! property.
//!
//! The single semantic divergence from the flat classifier is UNION ALL
//! homogeneity: the classifier rejects any UNION ALL whose branches are not all
//! literal simple aggregates or all literal projection/filters, while this
//! algebra accepts a UNION ALL as long as every branch synthesizes the SAME
//! `(TargetIdentity kind, StateContract kind)`. That admits previously-rejected
//! composed branches such as `Aggregate(Join(..))` as long as every branch
//! agrees on the synthesized property kind.
//!
//! This module is intentionally self-contained and behavior-neutral: it is NOT
//! yet wired into `derive_imv_refresh_contract`. Wiring is a follow-up task.

use crate::connector::starrocks::table::model::IcebergTableRef;
use crate::sql::analysis::{
    BinOp, ExprKind, JoinKind, QueryBody, Relation, ResolvedQuery, ResolvedSelect, ResolvedSetOp,
    SetOpKind, SortItem, TypedExpr,
};
use crate::sql::catalog::ScanSource;

/// The row-identity contract synthesized for a refresh fragment. This describes
/// *what a single output row is identified by* so the apply path can compute a
/// stable apply key.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum TargetIdentity {
    /// A single base-table row (a direct scan).
    BaseRowId,
    /// A joined row, identified by the composition of its two input
    /// identities.
    JoinRowKey(Box<TargetIdentity>, Box<TargetIdentity>),
    /// An aggregated group row, identified by the listed group-key output
    /// names.
    GroupRowId(Vec<String>),
    /// A branch-scoped identity (UNION ALL): the underlying per-branch identity
    /// tagged with a branch discriminant. Construction flattens nested
    /// `BranchScoped` so that `BranchScoped(BranchScoped(x)) == BranchScoped(x)`.
    BranchScoped(Box<TargetIdentity>),
}

impl TargetIdentity {
    /// Wrap an identity in `BranchScoped`, flattening an already branch-scoped
    /// inner identity so wrapping is idempotent.
    fn branch_scoped(inner: TargetIdentity) -> TargetIdentity {
        match inner {
            TargetIdentity::BranchScoped(_) => inner,
            other => TargetIdentity::BranchScoped(Box::new(other)),
        }
    }

    /// A stable kind label used for UNION ALL homogeneity comparison. Two
    /// identities are "same kind" iff their labels match. For `BranchScoped`
    /// and `JoinRowKey` only the top-level constructor participates; nested
    /// shape is intentionally ignored to match the property-kind contract.
    fn kind_label(&self) -> &'static str {
        match self {
            TargetIdentity::BaseRowId => "BaseRowId",
            TargetIdentity::JoinRowKey(_, _) => "JoinRowKey",
            TargetIdentity::GroupRowId(_) => "GroupRowId",
            TargetIdentity::BranchScoped(_) => "BranchScoped",
        }
    }
}

/// The aggregation-state contract synthesized for a refresh fragment.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum StateContract {
    /// No incremental aggregate state — projection / filter / join only.
    Stateless,
    /// Aggregate state with the given number of group keys and aggregate
    /// outputs.
    AggregateState {
        group_key_count: usize,
        aggregate_count: usize,
    },
}

impl StateContract {
    /// A stable kind label used for UNION ALL homogeneity comparison. The
    /// aggregate arities are intentionally NOT part of the kind label — branch
    /// arity compatibility, when required, is enforced separately.
    fn kind_label(&self) -> &'static str {
        match self {
            StateContract::Stateless => "Stateless",
            StateContract::AggregateState { .. } => "AggregateState",
        }
    }
}

/// The synthesized capability property of a refresh fragment.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct RefreshFragmentProperty {
    pub(crate) identity: TargetIdentity,
    pub(crate) state: StateContract,
    pub(crate) base_refs: Vec<IcebergTableRef>,
    /// `Some(n)` iff the identity top is `BranchScoped`, where `n` is the
    /// number of UNION ALL branches; `None` otherwise.
    pub(crate) branch_count: Option<usize>,
}

/// Synthesize the refresh-fragment property for an analyzed MV query.
///
/// Recursively walks the query mirroring the structural validation of the flat
/// classifier (`derive_from_query` and friends) while emitting a compositional
/// property instead of a named strategy enum. Returns a precise `Err(String)`
/// for every shape the classifier rejects.
pub(crate) fn derive_fragment_property(
    query: &ResolvedQuery,
) -> Result<RefreshFragmentProperty, String> {
    validate_query_wrapper(query)?;
    derive_from_query_body(&query.body)
}

fn validate_query_wrapper(query: &ResolvedQuery) -> Result<(), String> {
    if !query.local_cte_ids.is_empty() {
        return Err("Iceberg IMV refresh contract does not support WITH queries".to_string());
    }
    if !query.order_by.is_empty() || query.limit.is_some() || query.offset.is_some() {
        return Err(
            "Iceberg IMV refresh contract does not support ORDER BY, LIMIT, or OFFSET".to_string(),
        );
    }
    Ok(())
}

fn derive_from_query_body(body: &QueryBody) -> Result<RefreshFragmentProperty, String> {
    match body {
        QueryBody::Select(select) => derive_from_select(select),
        QueryBody::SetOperation(set_op) => derive_from_set_operation(set_op),
        QueryBody::Values(_) => {
            Err("Iceberg IMV refresh contract does not support VALUES queries".to_string())
        }
    }
}

fn derive_from_select(select: &ResolvedSelect) -> Result<RefreshFragmentProperty, String> {
    if select.distinct {
        return Err("Iceberg IMV refresh contract does not support SELECT DISTINCT".to_string());
    }
    if select.having.is_some() || select.repeat.is_some() {
        return Err(
            "Iceberg IMV refresh contract does not support HAVING, ROLLUP, CUBE, or GROUPING SETS"
                .to_string(),
        );
    }

    let has_aggregate = select.has_aggregation || !select.group_by.is_empty();
    if has_aggregate {
        let group_key_count = select.group_by.len();
        if group_key_count == 0 {
            return Err(
                "Iceberg IMV refresh contract requires aggregate queries to use a non-empty GROUP BY"
                    .to_string(),
            );
        }
        if let Some(filter) = &select.filter {
            validate_projection_filter_expr(filter)?;
        }
        for group_key in &select.group_by {
            validate_projection_filter_expr(group_key)?;
        }
        let aggregate_count = count_aggregate_projection_outputs(select)?;
        if aggregate_count == 0 {
            return Err(
                "Iceberg IMV refresh contract requires at least one aggregate output".to_string(),
            );
        }
        let child = derive_from_optional_relation(select.from.as_ref())?;
        let group_key_output_names = group_key_output_names(select);
        Ok(RefreshFragmentProperty {
            identity: TargetIdentity::GroupRowId(group_key_output_names),
            state: StateContract::AggregateState {
                group_key_count,
                aggregate_count,
            },
            base_refs: child.base_refs,
            branch_count: child.branch_count,
        })
    } else {
        validate_projection_filter_exprs(select)?;
        let child = derive_from_optional_relation(select.from.as_ref())?;
        // Mirror refresh_contract.rs:382-392: projection/filter over an
        // aggregate subquery is rejected. In the property world every aggregate
        // subquery synthesizes AggregateState, so key on that.
        if matches!(child.state, StateContract::AggregateState { .. }) {
            return Err(
                "Iceberg IMV refresh contract does not support projection/filter over aggregate subqueries"
                    .to_string(),
            );
        }
        // Projection / filter passthrough: identity, state, refs, and branch
        // count are inherited unchanged from the child relation.
        Ok(child)
    }
}

fn derive_from_optional_relation(
    relation: Option<&Relation>,
) -> Result<RefreshFragmentProperty, String> {
    let Some(relation) = relation else {
        return Err(
            "Iceberg IMV refresh contract requires a SELECT with at least one base relation"
                .to_string(),
        );
    };
    derive_from_relation(relation)
}

fn derive_from_relation(relation: &Relation) -> Result<RefreshFragmentProperty, String> {
    match relation {
        Relation::Scan(scan) => {
            let base_ref = iceberg_ref_from_scan(scan)?;
            Ok(RefreshFragmentProperty {
                identity: TargetIdentity::BaseRowId,
                state: StateContract::Stateless,
                base_refs: vec![base_ref],
                branch_count: None,
            })
        }
        Relation::Subquery { query, .. } => derive_fragment_property(query),
        Relation::Join(join) => {
            if join.join_type != JoinKind::Inner {
                return Err(
                    "Iceberg IMV refresh contract supports only two-table inner equi-join shapes"
                        .to_string(),
                );
            }
            let condition = join.condition.as_ref().ok_or_else(|| {
                "Iceberg IMV refresh contract requires JOIN ... ON equi-join predicates".to_string()
            })?;
            let left_qualifiers = relation_qualifiers(&join.left)?;
            let right_qualifiers = relation_qualifiers(&join.right)?;
            let join_key_count =
                count_equality_join_keys(condition, &left_qualifiers, &right_qualifiers)?;
            if join_key_count == 0 {
                return Err(
                    "Iceberg IMV refresh contract requires at least one equi-join predicate"
                        .to_string(),
                );
            }
            let left = derive_from_relation(&join.left)?;
            let right = derive_from_relation(&join.right)?;
            let mut base_refs = left.base_refs;
            base_refs.extend(right.base_refs);
            Ok(RefreshFragmentProperty {
                identity: TargetIdentity::JoinRowKey(
                    Box::new(left.identity),
                    Box::new(right.identity),
                ),
                // Compose: both join inputs are stateless today, so the join is
                // stateless.
                state: StateContract::Stateless,
                base_refs,
                branch_count: None,
            })
        }
        Relation::IcebergMetadataScan(_)
        | Relation::IcebergDeltaScan(_)
        | Relation::GenerateSeries(_)
        | Relation::Unnest(_)
        | Relation::CTEConsume { .. } => Err(format!(
            "Iceberg IMV refresh contract does not support relation {relation:?}"
        )),
    }
}

fn derive_from_set_operation(set_op: &ResolvedSetOp) -> Result<RefreshFragmentProperty, String> {
    let mut branches = Vec::new();
    collect_union_all_branches(set_op, &mut branches)?;
    if branches.len() < 2 {
        return Err(
            "Iceberg IMV refresh contract requires UNION ALL with at least two branches"
                .to_string(),
        );
    }
    let derived = branches
        .iter()
        .map(|query| derive_fragment_property(query))
        .collect::<Result<Vec<_>, _>>()?;
    let branch_count = derived.len();

    // Homogeneity is checked on the synthesized property: every branch must
    // produce the same (identity kind, state kind). Unlike the old shape
    // classifier this admits composed branches (e.g. Aggregate(Join(..))) as
    // long as every branch agrees on the synthesized property kind.
    let first = derived
        .first()
        .expect("UNION ALL branch list was checked as non-empty");
    let first_identity_kind = first.identity.kind_label();
    let first_state_kind = first.state.kind_label();
    for (index, branch) in derived.iter().enumerate().skip(1) {
        let branch_identity_kind = branch.identity.kind_label();
        let branch_state_kind = branch.state.kind_label();
        if branch_identity_kind != first_identity_kind || branch_state_kind != first_state_kind {
            return Err(format!(
                "Iceberg IMV refresh contract requires homogeneous UNION ALL branches: branch {index} \
                 synthesizes ({branch_identity_kind}, {branch_state_kind}) but branch 0 synthesizes \
                 ({first_identity_kind}, {first_state_kind})"
            ));
        }
    }

    let mut base_refs = Vec::new();
    for branch in &derived {
        base_refs.extend(branch.base_refs.iter().cloned());
    }

    let identity = TargetIdentity::branch_scoped(first.identity.clone());
    let state = first.state.clone();
    Ok(RefreshFragmentProperty {
        identity,
        state,
        base_refs,
        branch_count: Some(branch_count),
    })
}

fn collect_union_all_branches<'a>(
    set_op: &'a ResolvedSetOp,
    out: &mut Vec<&'a ResolvedQuery>,
) -> Result<(), String> {
    if set_op.kind != SetOpKind::Union || !set_op.all {
        return Err(
            "Iceberg IMV refresh contract only supports UNION ALL set operations".to_string(),
        );
    }
    collect_union_all_query(&set_op.left, out)?;
    collect_union_all_query(&set_op.right, out)
}

fn collect_union_all_query<'a>(
    query: &'a ResolvedQuery,
    out: &mut Vec<&'a ResolvedQuery>,
) -> Result<(), String> {
    validate_query_wrapper(query)?;
    match &query.body {
        QueryBody::SetOperation(set_op) => collect_union_all_branches(set_op, out),
        _ => {
            out.push(query);
            Ok(())
        }
    }
}

/// Derive the Iceberg base-table ref for a direct scan. Mirrors
/// `iceberg_ref_from_resolved` in the flat classifier, but reads the identity
/// off the scan's `ScanSource` (the relation tree, not the MV-declared refs).
fn iceberg_ref_from_scan(
    scan: &crate::sql::analysis::ScanRelation,
) -> Result<IcebergTableRef, String> {
    match &scan.table.source {
        ScanSource::IcebergDataFiles { table, .. } => Ok(IcebergTableRef {
            catalog: table.catalog.clone(),
            namespace: table.namespace.clone(),
            table: table.table.clone(),
        }),
        _ => Err(format!(
            "Iceberg IMV refresh contract requires Iceberg base tables, got non-Iceberg scan of `{}`",
            scan.table.name
        )),
    }
}

/// Group-key output names for an aggregate select: the SELECT-list output names
/// of the projection items that are themselves GROUP BY keys, in projection
/// order. `count_aggregate_projection_outputs` separately guarantees every
/// GROUP BY key is projected, so this captures the full group-key output set.
fn group_key_output_names(select: &ResolvedSelect) -> Vec<String> {
    select
        .projection
        .iter()
        .filter(|item| {
            select
                .group_by
                .iter()
                .any(|group_key| typed_expr_eq(group_key, &item.expr))
        })
        .map(|item| item.output_name.clone())
        .collect()
}

// ---------------------------------------------------------------------------
// Expression / shape validators replicated from the flat classifier.
//
// These mirror `refresh_contract.rs` exactly so the property algebra reproduces
// the same rejections. `refresh_contract.rs` is read-only for this task and its
// helpers are private, so the relevant ones are replicated here verbatim.
// ---------------------------------------------------------------------------

fn count_aggregate_projection_outputs(select: &ResolvedSelect) -> Result<usize, String> {
    let mut aggregate_count = 0;
    let mut projected_group_keys = vec![false; select.group_by.len()];
    for item in &select.projection {
        if let Some(index) = select
            .group_by
            .iter()
            .position(|group_key| typed_expr_eq(group_key, &item.expr))
        {
            projected_group_keys[index] = true;
            continue;
        }

        match &item.expr.kind {
            ExprKind::AggregateCall {
                name,
                args,
                distinct,
                order_by,
                ..
            } => {
                validate_supported_aggregate_call(name, args.len(), *distinct, order_by)?;
                validate_aggregate_argument_exprs(args)?;
                aggregate_count += 1;
                continue;
            }
            ExprKind::FunctionCall {
                name,
                args,
                distinct,
            } if is_legacy_unresolved_aggregate_function_name(name) => {
                validate_supported_aggregate_call(name, args.len(), *distinct, &[])?;
                validate_aggregate_argument_exprs(args)?;
                aggregate_count += 1;
                continue;
            }
            _ => {}
        }

        validate_non_contract_aggregate_projection_expr(&item.expr)?;
        return Err(
            "Iceberg IMV refresh contract aggregate projections must be GROUP BY keys or direct aggregate calls"
                .to_string(),
        );
    }
    if projected_group_keys.iter().any(|projected| !projected) {
        return Err(
            "Iceberg IMV refresh contract aggregate projection must include every GROUP BY key"
                .to_string(),
        );
    }
    Ok(aggregate_count)
}

fn validate_non_contract_aggregate_projection_expr(expr: &TypedExpr) -> Result<(), String> {
    match &expr.kind {
        ExprKind::AggregateCall {
            name,
            args,
            distinct,
            order_by,
            ..
        } => {
            validate_supported_aggregate_call(name, args.len(), *distinct, order_by)?;
            validate_aggregate_argument_exprs(args)
        }
        ExprKind::WindowCall { .. } => Err(
            "Iceberg IMV refresh contract does not support aggregate or window expressions outside direct aggregate outputs"
                .to_string(),
        ),
        ExprKind::BinaryOp { left, right, .. } => {
            validate_non_contract_aggregate_projection_expr(left)?;
            validate_non_contract_aggregate_projection_expr(right)
        }
        ExprKind::UnaryOp { expr, .. }
        | ExprKind::Cast { expr, .. }
        | ExprKind::IsNull { expr, .. }
        | ExprKind::IsTruthValue { expr, .. } => {
            validate_non_contract_aggregate_projection_expr(expr)
        }
        ExprKind::Nested(expr) => validate_non_contract_aggregate_projection_expr(expr),
        ExprKind::FunctionCall {
            name,
            args,
            distinct,
        } => {
            if is_legacy_unresolved_aggregate_function_name(name) {
                return Err(format!(
                    "Iceberg IMV refresh contract does not support aggregate function `{name}` outside direct aggregate outputs"
                ));
            }
            if *distinct {
                return Err(format!(
                    "Iceberg IMV refresh contract does not support DISTINCT scalar function `{name}`"
                ));
            }
            if is_unsupported_contract_scalar_function(name, args.len()) {
                return Err(format!(
                    "Iceberg IMV refresh contract does not support non-deterministic or unsafe scalar function `{name}`"
                ));
            }
            args.iter()
                .try_for_each(validate_non_contract_aggregate_projection_expr)
        }
        ExprKind::LambdaFunction { body, .. } => {
            validate_non_contract_aggregate_projection_expr(body)
        }
        ExprKind::InList { expr, list, .. } => {
            validate_non_contract_aggregate_projection_expr(expr)?;
            list.iter()
                .try_for_each(validate_non_contract_aggregate_projection_expr)
        }
        ExprKind::Between {
            expr, low, high, ..
        } => {
            validate_non_contract_aggregate_projection_expr(expr)?;
            validate_non_contract_aggregate_projection_expr(low)?;
            validate_non_contract_aggregate_projection_expr(high)
        }
        ExprKind::Like { expr, pattern, .. } => {
            validate_non_contract_aggregate_projection_expr(expr)?;
            validate_non_contract_aggregate_projection_expr(pattern)
        }
        ExprKind::Case {
            operand,
            when_then,
            else_expr,
        } => {
            if let Some(operand) = operand {
                validate_non_contract_aggregate_projection_expr(operand)?;
            }
            for (when, then) in when_then {
                validate_non_contract_aggregate_projection_expr(when)?;
                validate_non_contract_aggregate_projection_expr(then)?;
            }
            if let Some(else_expr) = else_expr {
                validate_non_contract_aggregate_projection_expr(else_expr)?;
            }
            Ok(())
        }
        ExprKind::Lambda { body, .. } => validate_non_contract_aggregate_projection_expr(body),
        ExprKind::SubqueryPlaceholder { .. } => Err(
            "Iceberg IMV refresh contract does not support subquery expressions in aggregate projections"
                .to_string(),
        ),
        ExprKind::ColumnRef { .. } | ExprKind::LambdaParamRef { .. } | ExprKind::Literal(_) => {
            Ok(())
        }
    }
}

fn validate_supported_aggregate_call(
    name: &str,
    arg_count: usize,
    distinct: bool,
    order_by: &[SortItem],
) -> Result<(), String> {
    if !order_by.is_empty() {
        return Err("Iceberg IMV refresh contract does not support aggregate ORDER BY".to_string());
    }
    let normalized = name.to_ascii_lowercase();
    let supported = matches!(
        normalized.as_str(),
        "count"
            | "count_distinct"
            | "multi_distinct_count"
            | "approx_count_distinct"
            | "ndv"
            | "hll_ndv"
            | "sum"
            | "avg"
            | "min"
            | "max"
            | "bool_or"
            | "boolor_agg"
            | "bool_and"
            | "booland_agg"
    );
    if !supported {
        return Err(format!(
            "Iceberg IMV refresh contract does not support aggregate function `{name}`"
        ));
    }
    if distinct && normalized != "count" {
        return Err(format!(
            "Iceberg IMV refresh contract does not support DISTINCT aggregate `{name}`"
        ));
    }
    if normalized == "count" {
        if (distinct && arg_count != 1) || (!distinct && arg_count > 1) {
            return Err(format!(
                "Iceberg IMV refresh contract supports only zero or one argument for aggregate function `{name}`"
            ));
        }
    } else if arg_count != 1 {
        return Err(format!(
            "Iceberg IMV refresh contract requires exactly one argument for aggregate function `{name}`"
        ));
    }
    Ok(())
}

fn validate_aggregate_argument_exprs(args: &[TypedExpr]) -> Result<(), String> {
    args.iter().try_for_each(validate_projection_filter_expr)
}

fn is_legacy_unresolved_aggregate_function_name(name: &str) -> bool {
    matches!(
        name.to_ascii_lowercase().as_str(),
        "count_distinct" | "hll_ndv"
    )
}

fn typed_expr_eq(left: &TypedExpr, right: &TypedExpr) -> bool {
    left.data_type == right.data_type
        && left.nullable == right.nullable
        && expr_kind_eq(&left.kind, &right.kind)
}

fn typed_exprs_eq(left: &[TypedExpr], right: &[TypedExpr]) -> bool {
    left.len() == right.len()
        && left
            .iter()
            .zip(right.iter())
            .all(|(left, right)| typed_expr_eq(left, right))
}

fn expr_kind_eq(left: &ExprKind, right: &ExprKind) -> bool {
    match (left, right) {
        (
            ExprKind::ColumnRef {
                column_id: left_id,
                qualifier: left_qualifier,
                column: left_column,
            },
            ExprKind::ColumnRef {
                column_id: right_id,
                qualifier: right_qualifier,
                column: right_column,
            },
        ) => {
            left_id == right_id
                && left_qualifier == right_qualifier
                && left_column.eq_ignore_ascii_case(right_column)
        }
        (
            ExprKind::LambdaParamRef {
                name: left_name,
                slot_id: left_slot,
            },
            ExprKind::LambdaParamRef {
                name: right_name,
                slot_id: right_slot,
            },
        ) => left_name == right_name && left_slot == right_slot,
        (ExprKind::Literal(left), ExprKind::Literal(right)) => left == right,
        (
            ExprKind::BinaryOp {
                left: left_left,
                op: left_op,
                right: left_right,
            },
            ExprKind::BinaryOp {
                left: right_left,
                op: right_op,
                right: right_right,
            },
        ) => {
            left_op == right_op
                && typed_expr_eq(left_left, right_left)
                && typed_expr_eq(left_right, right_right)
        }
        (
            ExprKind::UnaryOp {
                op: left_op,
                expr: left_expr,
            },
            ExprKind::UnaryOp {
                op: right_op,
                expr: right_expr,
            },
        ) => left_op == right_op && typed_expr_eq(left_expr, right_expr),
        (
            ExprKind::FunctionCall {
                name: left_name,
                args: left_args,
                distinct: left_distinct,
            },
            ExprKind::FunctionCall {
                name: right_name,
                args: right_args,
                distinct: right_distinct,
            },
        ) => {
            left_name.eq_ignore_ascii_case(right_name)
                && left_distinct == right_distinct
                && typed_exprs_eq(left_args, right_args)
        }
        (
            ExprKind::Cast {
                expr: left_expr,
                target: left_target,
            },
            ExprKind::Cast {
                expr: right_expr,
                target: right_target,
            },
        ) => left_target == right_target && typed_expr_eq(left_expr, right_expr),
        (
            ExprKind::IsNull {
                expr: left_expr,
                negated: left_negated,
            },
            ExprKind::IsNull {
                expr: right_expr,
                negated: right_negated,
            },
        ) => left_negated == right_negated && typed_expr_eq(left_expr, right_expr),
        (
            ExprKind::InList {
                expr: left_expr,
                list: left_list,
                negated: left_negated,
            },
            ExprKind::InList {
                expr: right_expr,
                list: right_list,
                negated: right_negated,
            },
        ) => {
            left_negated == right_negated
                && typed_expr_eq(left_expr, right_expr)
                && typed_exprs_eq(left_list, right_list)
        }
        (
            ExprKind::Between {
                expr: left_expr,
                low: left_low,
                high: left_high,
                negated: left_negated,
            },
            ExprKind::Between {
                expr: right_expr,
                low: right_low,
                high: right_high,
                negated: right_negated,
            },
        ) => {
            left_negated == right_negated
                && typed_expr_eq(left_expr, right_expr)
                && typed_expr_eq(left_low, right_low)
                && typed_expr_eq(left_high, right_high)
        }
        (
            ExprKind::Like {
                expr: left_expr,
                pattern: left_pattern,
                negated: left_negated,
            },
            ExprKind::Like {
                expr: right_expr,
                pattern: right_pattern,
                negated: right_negated,
            },
        ) => {
            left_negated == right_negated
                && typed_expr_eq(left_expr, right_expr)
                && typed_expr_eq(left_pattern, right_pattern)
        }
        (
            ExprKind::Case {
                operand: left_operand,
                when_then: left_when_then,
                else_expr: left_else,
            },
            ExprKind::Case {
                operand: right_operand,
                when_then: right_when_then,
                else_expr: right_else,
            },
        ) => {
            option_typed_expr_eq(left_operand.as_deref(), right_operand.as_deref())
                && left_when_then.len() == right_when_then.len()
                && left_when_then.iter().zip(right_when_then.iter()).all(
                    |((left_when, left_then), (right_when, right_then))| {
                        typed_expr_eq(left_when, right_when) && typed_expr_eq(left_then, right_then)
                    },
                )
                && option_typed_expr_eq(left_else.as_deref(), right_else.as_deref())
        }
        (
            ExprKind::IsTruthValue {
                expr: left_expr,
                value: left_value,
                negated: left_negated,
            },
            ExprKind::IsTruthValue {
                expr: right_expr,
                value: right_value,
                negated: right_negated,
            },
        ) => {
            left_value == right_value
                && left_negated == right_negated
                && typed_expr_eq(left_expr, right_expr)
        }
        (ExprKind::Nested(left), ExprKind::Nested(right)) => typed_expr_eq(left, right),
        _ => false,
    }
}

fn option_typed_expr_eq(left: Option<&TypedExpr>, right: Option<&TypedExpr>) -> bool {
    match (left, right) {
        (Some(left), Some(right)) => typed_expr_eq(left, right),
        (None, None) => true,
        _ => false,
    }
}

fn validate_projection_filter_exprs(select: &ResolvedSelect) -> Result<(), String> {
    for item in &select.projection {
        validate_projection_filter_expr(&item.expr)?;
    }
    if let Some(filter) = &select.filter {
        validate_projection_filter_expr(filter)?;
    }
    Ok(())
}

fn validate_projection_filter_expr(expr: &TypedExpr) -> Result<(), String> {
    match &expr.kind {
        ExprKind::AggregateCall { .. } | ExprKind::WindowCall { .. } => {
            Err("Iceberg IMV refresh contract does not support aggregate or window expressions in projection/filter shapes".to_string())
        }
        ExprKind::SubqueryPlaceholder { .. } => Err(
            "Iceberg IMV refresh contract does not support subquery expressions in projection/filter shapes"
                .to_string(),
        ),
        ExprKind::BinaryOp { left, right, .. } => {
            validate_projection_filter_expr(left)?;
            validate_projection_filter_expr(right)
        }
        ExprKind::UnaryOp { expr, .. }
        | ExprKind::Cast { expr, .. }
        | ExprKind::IsNull { expr, .. }
        | ExprKind::IsTruthValue { expr, .. }
        | ExprKind::Nested(expr)
        | ExprKind::LambdaFunction { body: expr, .. }
        | ExprKind::Lambda { body: expr, .. } => validate_projection_filter_expr(expr),
        ExprKind::FunctionCall {
            name,
            args,
            distinct,
        } => {
            if is_legacy_unresolved_aggregate_function_name(name) {
                return Err(format!(
                    "Iceberg IMV refresh contract does not support aggregate function `{name}` in projection/filter shapes"
                ));
            }
            if *distinct {
                return Err(format!(
                    "Iceberg IMV refresh contract does not support DISTINCT scalar function `{name}`"
                ));
            }
            if is_unsupported_contract_scalar_function(name, args.len()) {
                return Err(format!(
                    "Iceberg IMV refresh contract does not support non-deterministic or unsafe scalar function `{name}`"
                ));
            }
            for arg in args {
                validate_projection_filter_expr(arg)?;
            }
            Ok(())
        }
        ExprKind::InList { expr, list, .. } => {
            validate_projection_filter_expr(expr)?;
            for item in list {
                validate_projection_filter_expr(item)?;
            }
            Ok(())
        }
        ExprKind::Between {
            expr, low, high, ..
        } => {
            validate_projection_filter_expr(expr)?;
            validate_projection_filter_expr(low)?;
            validate_projection_filter_expr(high)
        }
        ExprKind::Like { expr, pattern, .. } => {
            validate_projection_filter_expr(expr)?;
            validate_projection_filter_expr(pattern)
        }
        ExprKind::Case {
            operand,
            when_then,
            else_expr,
        } => {
            if let Some(operand) = operand {
                validate_projection_filter_expr(operand)?;
            }
            for (when, then) in when_then {
                validate_projection_filter_expr(when)?;
                validate_projection_filter_expr(then)?;
            }
            if let Some(else_expr) = else_expr {
                validate_projection_filter_expr(else_expr)?;
            }
            Ok(())
        }
        ExprKind::ColumnRef { .. } | ExprKind::LambdaParamRef { .. } | ExprKind::Literal(_) => {
            Ok(())
        }
    }
}

fn is_unsupported_contract_scalar_function(name: &str, arg_count: usize) -> bool {
    matches!(
        name.to_ascii_lowercase().as_str(),
        "now"
            | "current_timestamp"
            | "localtime"
            | "localtimestamp"
            | "utc_timestamp"
            | "current_date"
            | "curdate"
            | "current_time"
            | "curtime"
            | "utc_time"
            | "random"
            | "rand"
            | "uuid"
            | "sleep"
            | "version"
            | "database"
            | "current_user"
            | "user"
            | "grouping"
            | "grouping_id"
    ) || (name.eq_ignore_ascii_case("unix_timestamp") && arg_count == 0)
}

fn relation_qualifiers(relation: &Relation) -> Result<Vec<String>, String> {
    match relation {
        Relation::Scan(scan) => Ok(vec![
            scan.alias
                .clone()
                .unwrap_or_else(|| scan.table.name.clone())
                .to_ascii_lowercase(),
        ]),
        _ => Err(
            "Iceberg IMV refresh contract supports join keys only over direct scan inputs"
                .to_string(),
        ),
    }
}

fn count_equality_join_keys(
    expr: &TypedExpr,
    left_qualifiers: &[String],
    right_qualifiers: &[String],
) -> Result<usize, String> {
    match &expr.kind {
        ExprKind::BinaryOp {
            left,
            op: BinOp::And,
            right,
        } => Ok(
            count_equality_join_keys(left, left_qualifiers, right_qualifiers)?
                + count_equality_join_keys(right, left_qualifiers, right_qualifiers)?,
        ),
        ExprKind::BinaryOp {
            left,
            op: BinOp::Eq,
            right,
        } => {
            let left_side = join_key_side(left, left_qualifiers, right_qualifiers)?;
            let right_side = join_key_side(right, left_qualifiers, right_qualifiers)?;
            if left_side == right_side {
                return Err(
                    "Iceberg IMV refresh contract equi-join predicates must compare left and right join inputs"
                        .to_string(),
                );
            }
            Ok(1)
        }
        ExprKind::Nested(expr) => count_equality_join_keys(expr, left_qualifiers, right_qualifiers),
        _ => Err(
            "Iceberg IMV refresh contract supports only AND-combined equi-join predicates"
                .to_string(),
        ),
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum JoinKeySide {
    Left,
    Right,
}

fn join_key_side(
    expr: &TypedExpr,
    left_qualifiers: &[String],
    right_qualifiers: &[String],
) -> Result<JoinKeySide, String> {
    match &expr.kind {
        ExprKind::ColumnRef {
            qualifier: Some(qualifier),
            ..
        } => {
            let qualifier = qualifier.to_ascii_lowercase();
            if left_qualifiers.iter().any(|left| left == &qualifier) {
                Ok(JoinKeySide::Left)
            } else if right_qualifiers.iter().any(|right| right == &qualifier) {
                Ok(JoinKeySide::Right)
            } else {
                Err(format!(
                    "Iceberg IMV refresh contract join key qualifier `{qualifier}` does not match either join input"
                ))
            }
        }
        ExprKind::Nested(expr) => join_key_side(expr, left_qualifiers, right_qualifiers),
        _ => Err(
            "Iceberg IMV refresh contract join keys must be qualified column references"
                .to_string(),
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::analysis::QueryBody;
    use crate::sql::catalog::{
        CatalogProvider, ColumnDef, IcebergDataFileBinding, IcebergSchemaDef, IcebergTableInfo,
        ScanSource, TableDef,
    };
    use arrow::datatypes::DataType;

    struct TestIcebergCatalog;

    impl CatalogProvider for TestIcebergCatalog {
        fn get_table(&self, database: &str, table: &str) -> Result<TableDef, String> {
            Ok(TableDef {
                name: table.to_string(),
                columns: vec![
                    column("id", DataType::Int64, false),
                    column("region", DataType::Utf8, true),
                    column("amount", DataType::Int64, true),
                    column("flag", DataType::Boolean, true),
                ],
                iceberg_row_lineage_metadata_columns: Vec::new(),
                source: ScanSource::IcebergDataFiles {
                    table: iceberg_table_info(database, table),
                    files: Vec::new(),
                    cloud_properties: Default::default(),
                    binding: IcebergDataFileBinding::CurrentSnapshot,
                },
            })
        }
    }

    fn column(name: &str, data_type: DataType, nullable: bool) -> ColumnDef {
        ColumnDef {
            name: name.to_string(),
            data_type,
            nullable,
            write_default: None,
            logical_type: None,
        }
    }

    fn iceberg_table_info(database: &str, table: &str) -> IcebergTableInfo {
        IcebergTableInfo {
            catalog: "ice".to_string(),
            namespace: database.to_string(),
            table: table.to_string(),
            table_uuid: Some(format!("uuid-{table}")),
            current_snapshot_id: Some(7),
            schema_id: 1,
            location: format!("file:///tmp/{database}/{table}"),
            schema: IcebergSchemaDef { fields: Vec::new() },
            serialized_metadata: None,
            serialized_metadata_rows: None,
        }
    }

    fn analyze_query(sql: &str) -> ResolvedQuery {
        let stmt = crate::sql::parser::parse_sql_raw(sql).expect("parse query");
        let sqlparser::ast::Statement::Query(query) = stmt else {
            panic!("expected query");
        };
        let (resolved_query, _, _) =
            crate::sql::analyzer::analyze(&query, &TestIcebergCatalog, "sales")
                .expect("analyze query");
        resolved_query
    }

    fn property(sql: &str) -> RefreshFragmentProperty {
        derive_fragment_property(&analyze_query(sql)).expect("derive fragment property")
    }

    fn error(sql: &str) -> String {
        derive_fragment_property(&analyze_query(sql)).expect_err("expected rejection")
    }

    fn base_ref_fqns(property: &RefreshFragmentProperty) -> Vec<String> {
        property
            .base_refs
            .iter()
            .map(IcebergTableRef::fqn)
            .collect()
    }

    // --- Acceptance: per-operator synthesis -------------------------------

    #[test]
    fn scan_synthesizes_base_row_identity_stateless() {
        let prop = property("SELECT region, amount FROM fact_east WHERE amount > 0");

        assert_eq!(prop.identity, TargetIdentity::BaseRowId);
        assert_eq!(prop.state, StateContract::Stateless);
        assert_eq!(base_ref_fqns(&prop), vec!["ice.sales.fact_east"]);
        assert_eq!(prop.branch_count, None);
    }

    #[test]
    fn aggregate_over_scan_synthesizes_group_row_and_aggregate_state() {
        let prop = property(
            "SELECT region, count(*) AS c, sum(amount) AS s FROM fact_east GROUP BY region",
        );

        assert_eq!(
            prop.identity,
            TargetIdentity::GroupRowId(vec!["region".to_string()])
        );
        assert_eq!(
            prop.state,
            StateContract::AggregateState {
                group_key_count: 1,
                aggregate_count: 2,
            }
        );
        assert_eq!(base_ref_fqns(&prop), vec!["ice.sales.fact_east"]);
        assert_eq!(prop.branch_count, None);
    }

    #[test]
    fn inner_equi_join_synthesizes_join_row_key_stateless() {
        let prop = property(
            "SELECT l.region, r.amount
             FROM fact_east l JOIN fact_west r ON l.id = r.id",
        );

        assert_eq!(
            prop.identity,
            TargetIdentity::JoinRowKey(
                Box::new(TargetIdentity::BaseRowId),
                Box::new(TargetIdentity::BaseRowId),
            )
        );
        assert_eq!(prop.state, StateContract::Stateless);
        assert_eq!(
            base_ref_fqns(&prop),
            vec!["ice.sales.fact_east", "ice.sales.fact_west"]
        );
        assert_eq!(prop.branch_count, None);
    }

    #[test]
    fn union_all_of_aggregates_synthesizes_branch_scoped_group_row() {
        let prop = property(
            "SELECT region, count(*) AS c, sum(amount) AS s
             FROM fact_east
             GROUP BY region
             UNION ALL
             SELECT region, count(*) AS c, sum(amount) AS s
             FROM fact_west
             GROUP BY region",
        );

        assert_eq!(
            prop.identity,
            TargetIdentity::BranchScoped(Box::new(TargetIdentity::GroupRowId(vec![
                "region".to_string()
            ])))
        );
        assert_eq!(
            prop.state,
            StateContract::AggregateState {
                group_key_count: 1,
                aggregate_count: 2,
            }
        );
        assert_eq!(
            base_ref_fqns(&prop),
            vec!["ice.sales.fact_east", "ice.sales.fact_west"]
        );
        assert_eq!(prop.branch_count, Some(2));
    }

    #[test]
    fn union_all_of_aggregate_joins_synthesizes_branch_scoped_group_row() {
        // The composed case the flat classifier REJECTED (it required each
        // branch to be a literal simple aggregate over a non-join input). The
        // property algebra ACCEPTS it because every branch synthesizes the same
        // (GroupRowId, AggregateState) kind.
        let prop = property(
            "SELECT l.region, count(*) AS c, sum(r.amount) AS s
             FROM fact_a l JOIN fact_b r ON l.id = r.id
             GROUP BY l.region
             UNION ALL
             SELECT l.region, count(*) AS c, sum(r.amount) AS s
             FROM fact_c l JOIN fact_d r ON l.id = r.id
             GROUP BY l.region",
        );

        assert_eq!(
            prop.identity,
            TargetIdentity::BranchScoped(Box::new(TargetIdentity::GroupRowId(vec![
                "region".to_string()
            ])))
        );
        assert_eq!(
            prop.state,
            StateContract::AggregateState {
                group_key_count: 1,
                aggregate_count: 2,
            }
        );
        assert_eq!(prop.base_refs.len(), 4);
        assert_eq!(
            base_ref_fqns(&prop),
            vec![
                "ice.sales.fact_a",
                "ice.sales.fact_b",
                "ice.sales.fact_c",
                "ice.sales.fact_d",
            ]
        );
        assert_eq!(prop.branch_count, Some(2));
    }

    #[test]
    fn nested_union_all_flattens_branch_scoped_identity() {
        // Three-branch UNION ALL parses as a nested set op; branch scoping must
        // flatten so the identity top is a single BranchScoped, and the branch
        // count counts every leaf branch.
        let prop = property(
            "SELECT region, amount FROM fact_a
             UNION ALL
             SELECT region, amount FROM fact_b
             UNION ALL
             SELECT region, amount FROM fact_c",
        );

        assert_eq!(
            prop.identity,
            TargetIdentity::BranchScoped(Box::new(TargetIdentity::BaseRowId))
        );
        assert_eq!(prop.state, StateContract::Stateless);
        assert_eq!(prop.branch_count, Some(3));
        assert_eq!(
            base_ref_fqns(&prop),
            vec!["ice.sales.fact_a", "ice.sales.fact_b", "ice.sales.fact_c"]
        );
    }

    #[test]
    fn projection_filter_passthrough_preserves_child_property() {
        // Plain projection/filter over a scan must inherit the child's identity
        // and state verbatim.
        let prop =
            property("SELECT region, amount + 1 AS adjusted FROM fact_east WHERE amount > 0");

        assert_eq!(prop.identity, TargetIdentity::BaseRowId);
        assert_eq!(prop.state, StateContract::Stateless);
        assert_eq!(base_ref_fqns(&prop), vec!["ice.sales.fact_east"]);
        assert_eq!(prop.branch_count, None);
    }

    #[test]
    fn projection_filter_passthrough_over_join_preserves_join_identity() {
        let prop = property(
            "SELECT joined.region, joined.amount
             FROM (
                 SELECT l.region AS region, r.amount AS amount
                 FROM fact_east l JOIN fact_west r ON l.id = r.id
             ) joined
             WHERE joined.amount > 0",
        );

        assert_eq!(
            prop.identity,
            TargetIdentity::JoinRowKey(
                Box::new(TargetIdentity::BaseRowId),
                Box::new(TargetIdentity::BaseRowId),
            )
        );
        assert_eq!(prop.state, StateContract::Stateless);
        assert_eq!(
            base_ref_fqns(&prop),
            vec!["ice.sales.fact_east", "ice.sales.fact_west"]
        );
    }

    // --- Rejections mirroring the flat classifier ------------------------

    #[test]
    fn rejects_non_union_all_set_op() {
        let err = error(
            "SELECT region FROM fact_east
             UNION
             SELECT region FROM fact_west",
        );
        assert!(
            err.contains("only supports UNION ALL"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn rejects_intersect_set_op() {
        let err = error(
            "SELECT region FROM fact_east
             INTERSECT
             SELECT region FROM fact_west",
        );
        assert!(
            err.contains("only supports UNION ALL"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn rejects_outer_join() {
        let err = error(
            "SELECT l.region, r.amount
             FROM fact_east l LEFT JOIN fact_west r ON l.id = r.id",
        );
        assert!(err.contains("inner equi-join"), "unexpected error: {err}");
    }

    #[test]
    fn rejects_cross_join() {
        let err = error(
            "SELECT l.region, r.amount
             FROM fact_east l CROSS JOIN fact_west r",
        );
        assert!(err.contains("inner equi-join"), "unexpected error: {err}");
    }

    #[test]
    fn rejects_non_equi_join() {
        let err = error(
            "SELECT l.region, r.amount
             FROM fact_east l JOIN fact_west r ON l.id > r.id",
        );
        assert!(err.contains("equi-join"), "unexpected error: {err}");
    }

    #[test]
    fn rejects_heterogeneous_union_all_branches() {
        // One aggregate branch + one projection branch: same arity is
        // impossible to compare, but the branches diverge on property kind
        // (GroupRowId/AggregateState vs BaseRowId/Stateless).
        let err = error(
            "SELECT region, count(*) AS c FROM fact_east GROUP BY region
             UNION ALL
             SELECT region, amount AS c FROM fact_west",
        );
        assert!(
            err.contains("homogeneous UNION ALL branches") && err.contains("branch 1"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn rejects_select_distinct() {
        let err = error("SELECT DISTINCT region FROM fact_east");
        assert!(err.contains("SELECT DISTINCT"), "unexpected error: {err}");
    }

    #[test]
    fn rejects_aggregate_without_group_keys() {
        let err = error("SELECT count(*) AS c FROM fact_east");
        assert!(
            err.contains("non-empty GROUP BY"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn rejects_with_query() {
        let err = error(
            "WITH unused AS (SELECT id FROM fact_extra)
             SELECT region, amount FROM fact_east",
        );
        assert!(err.contains("WITH"), "unexpected error: {err}");
    }

    #[test]
    fn rejects_order_by_limit_offset() {
        for sql in [
            "SELECT region FROM fact_east ORDER BY region",
            "SELECT region FROM fact_east LIMIT 10",
            "SELECT region FROM fact_east OFFSET 1",
        ] {
            let err = error(sql);
            assert!(
                err.contains("ORDER BY, LIMIT, or OFFSET"),
                "unexpected error for {sql}: {err}"
            );
        }
    }

    #[test]
    fn rejects_join_subquery_side() {
        // Join keys must be over direct scan inputs; a subquery side is
        // rejected by relation_qualifiers, same as the flat classifier.
        let err = error(
            "SELECT l.region, r.amount
             FROM (SELECT id, region FROM fact_east WHERE amount > 0) l
             JOIN fact_west r ON l.id = r.id",
        );
        assert!(
            err.contains("direct scan inputs"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn rejects_nondeterministic_projection() {
        let err = error("SELECT region, rand() AS r FROM fact_east");
        assert!(
            err.contains("non-deterministic or unsafe"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn rejects_projection_filter_over_aggregate_subquery() {
        // A plain SELECT over an aggregate subquery must be rejected by the
        // property algebra, mirroring refresh_contract.rs:382-392.
        let err = error(
            "SELECT region, adjusted
             FROM (
                 SELECT region, count(*) AS adjusted
                 FROM fact_east
                 GROUP BY region
             ) s
             WHERE adjusted > 0",
        );
        assert!(
            err.contains("projection/filter over aggregate subqueries"),
            "unexpected error: {err}"
        );
    }
}
