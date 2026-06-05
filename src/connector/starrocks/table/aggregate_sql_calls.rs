/// Focused aggregate-call extractor for the Iceberg IMV path.
///
/// Extracts only the aggregate calls, GROUP BY keys, and visible-output ordering
/// from a parsed SELECT query. The FROM clause (scan, join, or union) is
/// intentionally ignored — this extractor does not classify or reject based on
/// the table structure.
use super::mv_shape::{
    AggregateCallShape, AggregateMvShape, GroupKeyShape, VisibleAggregateOutput,
    classify_aggregate_select_outputs,
};

/// The focused aggregate-call surface extracted from a stored MV SELECT.
///
/// This is the non-base subset of `AggregateMvShape`: it carries the aggregate
/// calls, GROUP BY keys, and visible-output ordering, but knows nothing about
/// the FROM clause (scan / join / union). The extractor works uniformly over
/// any FROM structure.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct AggregateSqlCalls {
    pub(crate) group_keys: Vec<GroupKeyShape>,
    pub(crate) aggregates: Vec<AggregateCallShape>,
    /// Visible output ordering, interleaved in SELECT projection order.
    /// Each entry is either `GroupKey(i)` (index into `group_keys`) or
    /// `Aggregate(i)` (index into `aggregates`), preserving the projection
    /// order of the stored SELECT so that downstream layout / codec / merge
    /// operators can derive column positions deterministically.
    pub(crate) visible_outputs: Vec<VisibleAggregateOutput>,
}

/// TEMPORARY bridge: project the aggregate-call subset out of an
/// `AggregateMvShape`.
///
/// This exists only while the Iceberg path still classifies a stored SELECT
/// into a full `IncrementalMvShape` before consuming it. It lets the narrowed
/// layout builder / SQL rewrites take `AggregateSqlCalls` without disturbing the
/// shape acquisition. It will be deleted in P4.5 once the Iceberg path no longer
/// produces an `AggregateMvShape` at all.
impl From<&AggregateMvShape> for AggregateSqlCalls {
    fn from(shape: &AggregateMvShape) -> Self {
        AggregateSqlCalls {
            group_keys: shape.group_keys.clone(),
            aggregates: shape.aggregates.clone(),
            visible_outputs: shape.visible_outputs.clone(),
        }
    }
}

/// Extract aggregate calls + GROUP BY keys from a parsed aggregate SELECT.
///
/// Accepts any `Query` whose body is a plain `SELECT` with a `GROUP BY` clause
/// and aggregate projections. The FROM clause is not examined — a scan, a JOIN,
/// or a subquery UNION are all treated identically.
///
/// Returns `Err` with an English message if:
/// - The query body is not a plain SELECT.
/// - The GROUP BY is absent, empty, or uses unsupported modifiers.
/// - A projection item is neither a resolvable GROUP BY key nor a supported
///   aggregate call.
/// - Not every GROUP BY key appears in the projection.
///
/// The FROM clause is never examined and never causes a rejection.
pub(crate) fn extract_aggregate_sql_calls(
    query: &sqlparser::ast::Query,
) -> Result<AggregateSqlCalls, String> {
    let sqlparser::ast::SetExpr::Select(select) = query.body.as_ref() else {
        return Err("extract_aggregate_sql_calls: expected a plain SELECT body".to_string());
    };

    let (group_keys, aggregates, visible_outputs) = classify_aggregate_select_outputs(select)?;

    Ok(AggregateSqlCalls {
        group_keys,
        aggregates,
        visible_outputs,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::connector::starrocks::table::mv_shape::{
        AggregateFunctionKind, AggregateInput, VisibleAggregateOutput,
    };

    fn parse_query(sql: &str) -> sqlparser::ast::Query {
        let normalized =
            crate::sql::parser::dialect::normalize_for_raw_parse(sql).expect("normalize");
        let stmt = crate::sql::parser::parse_normalized_sql_raw(&normalized).expect("parse");
        let sqlparser::ast::Statement::Query(query) = stmt else {
            panic!("not a query: {stmt:?}");
        };
        *query
    }

    fn extract(sql: &str) -> Result<AggregateSqlCalls, String> {
        let query = parse_query(sql);
        extract_aggregate_sql_calls(&query)
    }

    // (a) Basic single-table aggregate: k, sum(v) FROM t GROUP BY k
    // Verifies group_keys=[k], aggregates=[sum(v)], visible_outputs=[GroupKey(0), Aggregate(0)]
    #[test]
    fn simple_aggregate_over_plain_scan() {
        let calls = extract("SELECT k, sum(v) FROM t GROUP BY k")
            .expect("plain scan aggregate should succeed");

        assert_eq!(calls.group_keys.len(), 1, "one group key");
        assert_eq!(calls.group_keys[0].output_name, "k");

        assert_eq!(calls.aggregates.len(), 1, "one aggregate");
        assert_eq!(calls.aggregates[0].function, AggregateFunctionKind::Sum);
        assert_eq!(calls.aggregates[0].output_name, "sum(v)");
        assert!(
            matches!(calls.aggregates[0].input, AggregateInput::Expr(_)),
            "sum input is an expr"
        );

        assert_eq!(
            calls.visible_outputs,
            vec![
                VisibleAggregateOutput::GroupKey(0),
                VisibleAggregateOutput::Aggregate(0),
            ],
            "visible outputs: GroupKey first, then Aggregate, in projection order"
        );
    }

    // (b) Aggregate over a JOIN: should produce the same aggregate-call output
    // as the plain-scan case and must NOT return an error about the join.
    // This is the crucial test — proves the extractor ignores the FROM join.
    #[test]
    fn aggregate_over_join_ignores_from_clause() {
        let calls =
            extract("SELECT a.k, sum(a.v) FROM t_a a JOIN t_b b ON a.id = b.id GROUP BY a.k")
                .expect("aggregate over join must not be rejected");

        assert_eq!(calls.group_keys.len(), 1, "one group key");
        // The group key expression is a qualified column (a.k).
        let key_expr_str = calls.group_keys[0].expr.to_string();
        assert!(
            key_expr_str.contains('k') || key_expr_str.contains("a.k"),
            "group key references k: {key_expr_str}"
        );

        assert_eq!(calls.aggregates.len(), 1, "one aggregate");
        assert_eq!(calls.aggregates[0].function, AggregateFunctionKind::Sum);
        assert!(
            matches!(calls.aggregates[0].input, AggregateInput::Expr(_)),
            "sum input is an expr"
        );

        assert_eq!(
            calls.visible_outputs,
            vec![
                VisibleAggregateOutput::GroupKey(0),
                VisibleAggregateOutput::Aggregate(0),
            ],
            "visible outputs in projection order"
        );
    }

    // (c) Multiple aggregate functions including count(*): k, count(*), max(x), min(y)
    // Verifies correct functions and that count(*) is recognized as AggregateInput::Star.
    #[test]
    fn multiple_aggregates_including_count_star() {
        let calls =
            extract("SELECT k, count(*) as c, max(x) as mx, min(y) as mn FROM t GROUP BY k")
                .expect("multiple aggregates should succeed");

        assert_eq!(calls.group_keys.len(), 1);
        assert_eq!(calls.group_keys[0].output_name, "k");

        assert_eq!(calls.aggregates.len(), 3, "three aggregates");

        assert_eq!(calls.aggregates[0].output_name, "c");
        assert_eq!(calls.aggregates[0].function, AggregateFunctionKind::Count);
        assert_eq!(
            calls.aggregates[0].input,
            AggregateInput::Star,
            "count(*) recognized as Star"
        );

        assert_eq!(calls.aggregates[1].output_name, "mx");
        assert_eq!(calls.aggregates[1].function, AggregateFunctionKind::Max);

        assert_eq!(calls.aggregates[2].output_name, "mn");
        assert_eq!(calls.aggregates[2].function, AggregateFunctionKind::Min);

        assert_eq!(
            calls.visible_outputs,
            vec![
                VisibleAggregateOutput::GroupKey(0),
                VisibleAggregateOutput::Aggregate(0),
                VisibleAggregateOutput::Aggregate(1),
                VisibleAggregateOutput::Aggregate(2),
            ],
            "visible outputs in projection order"
        );
    }

    // Aggregate over a subquery UNION: the subquery FROM is also ignored.
    #[test]
    fn aggregate_over_union_subquery_ignores_from() {
        let calls = extract(
            "SELECT k, sum(v) as s FROM (SELECT k, v FROM t1 UNION ALL SELECT k, v FROM t2) sub GROUP BY k",
        )
        .expect("aggregate over union subquery must not be rejected");

        assert_eq!(calls.group_keys.len(), 1);
        assert_eq!(calls.aggregates.len(), 1);
        assert_eq!(calls.aggregates[0].function, AggregateFunctionKind::Sum);
    }

    // A non-aggregate SELECT (no GROUP BY) must be rejected.
    #[test]
    fn rejects_non_aggregate_query() {
        let err = extract("SELECT k, v FROM t").expect_err("non-aggregate query must be rejected");
        assert!(
            err.contains("GROUP BY") || err.contains("group"),
            "expected GROUP BY error, got: {err}"
        );
    }

    // A projection item that is neither a group key nor a supported aggregate must be rejected.
    #[test]
    fn rejects_non_aggregate_scalar_projection() {
        // k+1 is not a group key (the GROUP BY is k) and not an aggregate call.
        let err = extract("SELECT k+1, sum(v) FROM t GROUP BY k")
            .expect_err("unsupported scalar projection must be rejected");
        assert!(
            err.contains("GROUP BY key") || err.contains("aggregate call"),
            "expected GROUP BY key or aggregate call error, got: {err}"
        );
    }
}
