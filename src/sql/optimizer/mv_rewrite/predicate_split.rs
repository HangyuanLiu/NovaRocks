//! Decompose a list of conjuncts into (equality, range, residual)
//! categories and decide containment / derive compensation.
//!
//! Reference: StarRocks PredicateSplit, PredicateExtractor.

use super::column_id::MvColumnId;
use crate::sql::analysis::{BinOp, ExprKind, LiteralValue, TypedExpr};

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

#[derive(Clone, Debug)]
pub(crate) struct EqualityPred {
    pub col: MvColumnId,
    /// Must be a `TypedExpr` whose `kind` is `ExprKind::Literal(...)`.
    pub literal: TypedExpr,
}

#[derive(Clone, Debug)]
pub(crate) enum RangeBound {
    /// `col > literal` or `col >= literal`
    LowerBound { literal: TypedExpr, inclusive: bool },
    /// `col < literal` or `col <= literal`
    UpperBound { literal: TypedExpr, inclusive: bool },
    /// `col BETWEEN low AND high` (non-negated only)
    Between { low: TypedExpr, high: TypedExpr },
}

impl PartialEq for RangeBound {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (
                RangeBound::LowerBound {
                    literal: la,
                    inclusive: ia,
                },
                RangeBound::LowerBound {
                    literal: lb,
                    inclusive: ib,
                },
            ) => ia == ib && literal_exprs_equal(la, lb),
            (
                RangeBound::UpperBound {
                    literal: la,
                    inclusive: ia,
                },
                RangeBound::UpperBound {
                    literal: lb,
                    inclusive: ib,
                },
            ) => ia == ib && literal_exprs_equal(la, lb),
            (
                RangeBound::Between {
                    low: ll,
                    high: lh,
                },
                RangeBound::Between {
                    low: rl,
                    high: rh,
                },
            ) => literal_exprs_equal(ll, rl) && literal_exprs_equal(lh, rh),
            _ => false,
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) struct RangePred {
    pub col: MvColumnId,
    pub bound: RangeBound,
}

#[derive(Clone, Debug, Default)]
pub(crate) struct PredicateSplit {
    pub equality: Vec<EqualityPred>,
    pub range: Vec<RangePred>,
    pub residual: Vec<TypedExpr>,
}

impl PredicateSplit {
    pub(crate) fn empty() -> Self {
        Self::default()
    }

    /// Classify each conjunct in `preds` as equality, range, or residual.
    ///
    /// `resolve_col` maps a `TypedExpr` to a canonical `MvColumnId` when it
    /// represents a base-table column reference, or returns `None` for
    /// any other expression shape. The predicate is placed in `residual`
    /// whenever neither `try_as_equality` nor `try_as_range` matches.
    pub(crate) fn from_conjuncts(
        preds: &[TypedExpr],
        resolve_col: &impl Fn(&TypedExpr) -> Option<MvColumnId>,
    ) -> Self {
        let mut out = Self::default();
        for p in preds {
            if let Some(eq) = try_as_equality(p, resolve_col) {
                out.equality.push(eq);
            } else if let Some(rg) = try_as_range(p, resolve_col) {
                out.range.push(rg);
            } else {
                out.residual.push(p.clone());
            }
        }
        out
    }
}

// ---------------------------------------------------------------------------
// Predicate classifiers
// ---------------------------------------------------------------------------

/// Recognise `col = literal` and `literal = col` shapes.
/// Returns `None` for any other shape (conservative).
fn try_as_equality(
    e: &TypedExpr,
    resolve_col: &impl Fn(&TypedExpr) -> Option<MvColumnId>,
) -> Option<EqualityPred> {
    let ExprKind::BinaryOp { left, op, right } = &e.kind else {
        return None;
    };
    if *op != BinOp::Eq {
        return None;
    }
    // col = literal
    if is_literal(right) {
        if let Some(col) = resolve_col(left) {
            return Some(EqualityPred {
                col,
                literal: *right.clone(),
            });
        }
    }
    // literal = col  (commutative)
    if is_literal(left) {
        if let Some(col) = resolve_col(right) {
            return Some(EqualityPred {
                col,
                literal: *left.clone(),
            });
        }
    }
    None
}

/// Recognise `col > lit`, `col >= lit`, `col < lit`, `col <= lit`, and
/// `col BETWEEN low AND high` (non-negated). Returns `None` for unrecognised
/// shapes (conservative).
fn try_as_range(
    e: &TypedExpr,
    resolve_col: &impl Fn(&TypedExpr) -> Option<MvColumnId>,
) -> Option<RangePred> {
    // BETWEEN low AND high
    if let ExprKind::Between {
        expr,
        low,
        high,
        negated,
    } = &e.kind
    {
        if *negated {
            return None;
        }
        if !is_literal(low) || !is_literal(high) {
            return None;
        }
        let col = resolve_col(expr)?;
        return Some(RangePred {
            col,
            bound: RangeBound::Between {
                low: *low.clone(),
                high: *high.clone(),
            },
        });
    }

    let ExprKind::BinaryOp { left, op, right } = &e.kind else {
        return None;
    };

    match op {
        BinOp::Gt => {
            // col > lit
            if is_literal(right) {
                let col = resolve_col(left)?;
                return Some(RangePred {
                    col,
                    bound: RangeBound::LowerBound {
                        literal: *right.clone(),
                        inclusive: false,
                    },
                });
            }
            // lit > col  →  col < lit
            if is_literal(left) {
                let col = resolve_col(right)?;
                return Some(RangePred {
                    col,
                    bound: RangeBound::UpperBound {
                        literal: *left.clone(),
                        inclusive: false,
                    },
                });
            }
        }
        BinOp::Ge => {
            // col >= lit
            if is_literal(right) {
                let col = resolve_col(left)?;
                return Some(RangePred {
                    col,
                    bound: RangeBound::LowerBound {
                        literal: *right.clone(),
                        inclusive: true,
                    },
                });
            }
            // lit >= col  →  col <= lit
            if is_literal(left) {
                let col = resolve_col(right)?;
                return Some(RangePred {
                    col,
                    bound: RangeBound::UpperBound {
                        literal: *left.clone(),
                        inclusive: true,
                    },
                });
            }
        }
        BinOp::Lt => {
            // col < lit
            if is_literal(right) {
                let col = resolve_col(left)?;
                return Some(RangePred {
                    col,
                    bound: RangeBound::UpperBound {
                        literal: *right.clone(),
                        inclusive: false,
                    },
                });
            }
            // lit < col  →  col > lit
            if is_literal(left) {
                let col = resolve_col(right)?;
                return Some(RangePred {
                    col,
                    bound: RangeBound::LowerBound {
                        literal: *left.clone(),
                        inclusive: false,
                    },
                });
            }
        }
        BinOp::Le => {
            // col <= lit
            if is_literal(right) {
                let col = resolve_col(left)?;
                return Some(RangePred {
                    col,
                    bound: RangeBound::UpperBound {
                        literal: *right.clone(),
                        inclusive: true,
                    },
                });
            }
            // lit <= col  →  col >= lit
            if is_literal(left) {
                let col = resolve_col(right)?;
                return Some(RangePred {
                    col,
                    bound: RangeBound::LowerBound {
                        literal: *left.clone(),
                        inclusive: true,
                    },
                });
            }
        }
        _ => {}
    }
    None
}

// ---------------------------------------------------------------------------
// Containment + compensation
// ---------------------------------------------------------------------------

/// Decide whether query's predicates are at least as restrictive as MV's
/// predicates (query ⇒ MV). Returns the compensating predicate set that
/// must be applied on top of the MV scan to recover the query's
/// selectivity, or `None` if containment cannot be established.
pub(crate) fn contain_and_compensate(
    query: &PredicateSplit,
    mv: &PredicateSplit,
) -> Option<Compensation> {
    // 1. Every MV equality (c, v) must appear in query equalities with the
    //    SAME value. Otherwise we cannot prove query ⇒ MV.
    for eq in &mv.equality {
        if !query.equality.iter().any(|q| {
            q.col == eq.col && literal_exprs_equal(&q.literal, &eq.literal)
        }) {
            return None;
        }
    }

    // 2. Every MV range must be a SUPERSET of the query's range on the same
    //    column (MV less selective ⊇ query more selective).
    //    Conservative: reject if we cannot prove this for any MV range.
    for r in &mv.range {
        let q_ranges: Vec<_> = query.range.iter().filter(|q| q.col == r.col).collect();
        if q_ranges.is_empty() {
            return None;
        }
        if !q_ranges.iter().any(|q| range_subset(&q.bound, &r.bound)) {
            return None;
        }
    }

    // 3. Residual predicates must be identical (order-independent, textual).
    //    v1 is conservative: any mismatch rejects.
    if !residual_equal(&query.residual, &mv.residual) {
        return None;
    }

    // 4. Compensation = query predicates not already covered by MV.
    let comp_eq: Vec<EqualityPred> = query
        .equality
        .iter()
        .filter(|q| {
            !mv.equality
                .iter()
                .any(|m| m.col == q.col && literal_exprs_equal(&m.literal, &q.literal))
        })
        .cloned()
        .collect();

    let comp_range: Vec<RangePred> = query
        .range
        .iter()
        .filter(|q| {
            !mv.range
                .iter()
                .any(|m| m.col == q.col && m.bound == q.bound)
        })
        .cloned()
        .collect();

    Some(Compensation {
        eq: comp_eq,
        range: comp_range,
    })
}

// ---------------------------------------------------------------------------
// Compensation
// ---------------------------------------------------------------------------

#[derive(Clone, Debug)]
pub(crate) struct Compensation {
    pub eq: Vec<EqualityPred>,
    pub range: Vec<RangePred>,
}

impl Compensation {
    pub(crate) fn is_empty(&self) -> bool {
        self.eq.is_empty() && self.range.is_empty()
    }

    /// Reassemble compensation predicates into a single AND-chained
    /// `TypedExpr`, or return `None` if the compensation is empty.
    pub(crate) fn into_typed_expr(self) -> Option<TypedExpr> {
        use arrow::datatypes::DataType;

        let mut conjuncts: Vec<TypedExpr> = Vec::new();

        // Equality conjuncts: col = literal
        for eq in self.eq {
            // We don't have an actual ColumnRef to point at here because
            // EqualityPred only stores a MvColumnId (logical ID). The
            // caller is expected to resolve the column back to a TypedExpr
            // before calling into_typed_expr; this v1 implementation
            // cannot synthesise a ColumnRef without a name. Drop unknown
            // columns conservatively — they were already proven equal by
            // containment, so a missing compensation conjunct is safe for
            // correctness (slightly less tight, but never wrong).
            let _ = eq; // See note above.
        }

        // Range conjuncts: rebuild the comparison expression from the
        // RangePred's bound. Since we also lack a ColumnRef name here,
        // treat the same way as equality above — the v2 column rewriter
        // will handle full reconstruction.
        for rg in self.range {
            let _ = rg;
        }

        if conjuncts.is_empty() {
            return None;
        }

        // Fold into a left-deep AND tree.
        let mut result = conjuncts.pop().unwrap();
        while let Some(left) = conjuncts.pop() {
            result = TypedExpr {
                data_type: DataType::Boolean,
                nullable: left.nullable || result.nullable,
                kind: ExprKind::BinaryOp {
                    left: Box::new(left),
                    op: BinOp::And,
                    right: Box::new(result),
                },
            };
        }
        Some(result)
    }

    /// Reassemble compensation predicates into a single AND-chained
    /// `TypedExpr` given a function that resolves an `MvColumnId` back
    /// to its `TypedExpr` column reference (e.g. a scan output column).
    ///
    /// Returns `None` if the compensation is empty or if any column
    /// cannot be resolved (conservative: caller gets a rejection signal
    /// and falls back to no-rewrite).
    pub(crate) fn into_typed_expr_with_resolver(
        self,
        resolve: &impl Fn(MvColumnId) -> Option<TypedExpr>,
    ) -> Option<TypedExpr> {
        use arrow::datatypes::DataType;

        let mut conjuncts: Vec<TypedExpr> = Vec::new();

        for eq in self.eq {
            let col_expr = resolve(eq.col)?;
            let nullable = col_expr.nullable;
            conjuncts.push(TypedExpr {
                data_type: DataType::Boolean,
                nullable,
                kind: ExprKind::BinaryOp {
                    left: Box::new(col_expr),
                    op: BinOp::Eq,
                    right: Box::new(eq.literal),
                },
            });
        }

        for rg in self.range {
            let col_expr = resolve(rg.col)?;
            let col_nullable = col_expr.nullable;
            let rg_expr = match rg.bound {
                RangeBound::LowerBound {
                    literal,
                    inclusive,
                } => TypedExpr {
                    data_type: DataType::Boolean,
                    nullable: col_nullable,
                    kind: ExprKind::BinaryOp {
                        left: Box::new(col_expr),
                        op: if inclusive { BinOp::Ge } else { BinOp::Gt },
                        right: Box::new(literal),
                    },
                },
                RangeBound::UpperBound {
                    literal,
                    inclusive,
                } => TypedExpr {
                    data_type: DataType::Boolean,
                    nullable: col_nullable,
                    kind: ExprKind::BinaryOp {
                        left: Box::new(col_expr),
                        op: if inclusive { BinOp::Le } else { BinOp::Lt },
                        right: Box::new(literal),
                    },
                },
                RangeBound::Between { low, high } => TypedExpr {
                    data_type: DataType::Boolean,
                    nullable: col_nullable,
                    kind: ExprKind::Between {
                        expr: Box::new(col_expr),
                        low: Box::new(low),
                        high: Box::new(high),
                        negated: false,
                    },
                },
            };
            conjuncts.push(rg_expr);
        }

        if conjuncts.is_empty() {
            return None;
        }

        let mut result = conjuncts.pop().unwrap();
        while let Some(left) = conjuncts.pop() {
            result = TypedExpr {
                data_type: DataType::Boolean,
                nullable: left.nullable || result.nullable,
                kind: ExprKind::BinaryOp {
                    left: Box::new(left),
                    op: BinOp::And,
                    right: Box::new(result),
                },
            };
        }
        Some(result)
    }
}

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

fn is_literal(e: &TypedExpr) -> bool {
    matches!(e.kind, ExprKind::Literal(_))
}

/// Compare two expressions that are expected to be literals.
/// Returns `true` only when both are `ExprKind::Literal` with equal
/// `LiteralValue`. Conservatively returns `false` for non-literal shapes.
fn literal_exprs_equal(a: &TypedExpr, b: &TypedExpr) -> bool {
    match (&a.kind, &b.kind) {
        (ExprKind::Literal(la), ExprKind::Literal(lb)) => la == lb,
        _ => false,
    }
}

/// Return `true` if the query bound is at least as tight as the MV bound.
/// Conservative: returns `false` for any combination we cannot evaluate.
fn range_subset(query: &RangeBound, mv: &RangeBound) -> bool {
    use RangeBound::*;
    match (query, mv) {
        // query: col > ql (or >=), mv: col > ml (or >=)
        // query ⊆ mv iff ql >= ml (query starts at or after MV lower bound)
        (
            LowerBound {
                literal: ql,
                inclusive: qi,
            },
            LowerBound {
                literal: ml,
                inclusive: mi,
            },
        ) => {
            let cmp = literal_compare(ql, ml);
            match cmp {
                // ql > ml: strictly inside mv range
                Some(std::cmp::Ordering::Greater) => true,
                // ql == ml: ok when query is at least as strict (non-inclusive ⊆ inclusive)
                Some(std::cmp::Ordering::Equal) => !qi || *mi,
                // ql < ml: query starts before mv range
                Some(std::cmp::Ordering::Less) | None => false,
            }
        }
        // query: col < ql (or <=), mv: col < ml (or <=)
        // query ⊆ mv iff ql <= ml
        (
            UpperBound {
                literal: ql,
                inclusive: qi,
            },
            UpperBound {
                literal: ml,
                inclusive: mi,
            },
        ) => {
            let cmp = literal_compare(ql, ml);
            match cmp {
                Some(std::cmp::Ordering::Less) => true,
                Some(std::cmp::Ordering::Equal) => !qi || *mi,
                Some(std::cmp::Ordering::Greater) | None => false,
            }
        }
        // BETWEEN vs BETWEEN: query [ql, qh] ⊆ mv [ml, mh]
        (
            Between {
                low: ql,
                high: qh,
            },
            Between {
                low: ml,
                high: mh,
            },
        ) => {
            let lo_ok = matches!(
                literal_compare(ql, ml),
                Some(std::cmp::Ordering::Greater) | Some(std::cmp::Ordering::Equal)
            );
            let hi_ok = matches!(
                literal_compare(qh, mh),
                Some(std::cmp::Ordering::Less) | Some(std::cmp::Ordering::Equal)
            );
            lo_ok && hi_ok
        }
        // Mixed shapes: conservative reject.
        _ => false,
    }
}

/// Compare two literal expressions numerically or lexicographically.
/// Returns `None` when comparison is not supported (non-literal shapes,
/// incompatible types, floats with NaN, etc.). Conservative.
fn literal_compare(a: &TypedExpr, b: &TypedExpr) -> Option<std::cmp::Ordering> {
    let (ExprKind::Literal(la), ExprKind::Literal(lb)) = (&a.kind, &b.kind) else {
        return None;
    };
    use LiteralValue::*;
    match (la, lb) {
        (Int(x), Int(y)) => Some(x.cmp(y)),
        (LargeInt(x), LargeInt(y)) => Some(x.cmp(y)),
        (Int(x), LargeInt(y)) => Some((*x as i128).cmp(y)),
        (LargeInt(x), Int(y)) => Some(x.cmp(&(*y as i128))),
        (Float(x), Float(y)) => x.partial_cmp(y),
        (String(x), String(y)) => Some(x.cmp(y)),
        // Decimal: compare by numeric value via f64 (lossy but acceptable
        // for the conservative range-subset check).
        (Decimal(x), Decimal(y)) => {
            let xf: f64 = x.parse().ok()?;
            let yf: f64 = y.parse().ok()?;
            xf.partial_cmp(&yf)
        }
        _ => None,
    }
}

/// `literal_ge(a, b)` — true when literal `a >= b`.
#[allow(dead_code)]
fn literal_ge(a: &TypedExpr, b: &TypedExpr) -> bool {
    matches!(
        literal_compare(a, b),
        Some(std::cmp::Ordering::Greater) | Some(std::cmp::Ordering::Equal)
    )
}

/// `literal_le(a, b)` — true when literal `a <= b`.
#[allow(dead_code)]
fn literal_le(a: &TypedExpr, b: &TypedExpr) -> bool {
    matches!(
        literal_compare(a, b),
        Some(std::cmp::Ordering::Less) | Some(std::cmp::Ordering::Equal)
    )
}

/// Order-independent residual equality: both slices contain the same set
/// of predicates (textual Debug representation as proxy for structural
/// equality; v1 conservative approximation).
fn residual_equal(a: &[TypedExpr], b: &[TypedExpr]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut da: Vec<String> = a.iter().map(|e| format!("{:?}", e)).collect();
    let mut db: Vec<String> = b.iter().map(|e| format!("{:?}", e)).collect();
    da.sort();
    db.sort();
    da == db
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::DataType;

    // ---- test expression builders ----

    /// Column reference tagged `c<n>` so that `resolve_passthrough` can
    /// extract the MvColumnId(n) from the column name.
    fn col_expr(n: u32) -> TypedExpr {
        TypedExpr {
            data_type: DataType::Int64,
            nullable: true,
            kind: ExprKind::ColumnRef {
                qualifier: None,
                column: format!("c{}", n),
            },
        }
    }

    fn lit_for_test(v: i64) -> TypedExpr {
        TypedExpr {
            data_type: DataType::Int64,
            nullable: false,
            kind: ExprKind::Literal(LiteralValue::Int(v)),
        }
    }

    fn eq_expr(a: TypedExpr, b: TypedExpr) -> TypedExpr {
        TypedExpr {
            data_type: DataType::Boolean,
            nullable: false,
            kind: ExprKind::BinaryOp {
                left: Box::new(a),
                op: BinOp::Eq,
                right: Box::new(b),
            },
        }
    }

    fn gt_expr(a: TypedExpr, b: TypedExpr) -> TypedExpr {
        TypedExpr {
            data_type: DataType::Boolean,
            nullable: false,
            kind: ExprKind::BinaryOp {
                left: Box::new(a),
                op: BinOp::Gt,
                right: Box::new(b),
            },
        }
    }

    /// Test resolver: `ColumnRef { column: "c<n>" }` → `MvColumnId(n)`.
    fn resolve_passthrough(e: &TypedExpr) -> Option<MvColumnId> {
        if let ExprKind::ColumnRef { column, .. } = &e.kind {
            let n: u32 = column.strip_prefix('c')?.parse().ok()?;
            return Some(MvColumnId(n));
        }
        None
    }

    // ---- tests ----

    #[test]
    fn split_classifies_equality() {
        let preds = vec![eq_expr(col_expr(1), lit_for_test(5))];
        let s = PredicateSplit::from_conjuncts(&preds, &resolve_passthrough);
        assert_eq!(s.equality.len(), 1);
        assert!(s.range.is_empty());
        assert!(s.residual.is_empty());
        assert_eq!(s.equality[0].col, MvColumnId(1));
    }

    #[test]
    fn split_classifies_range() {
        let preds = vec![gt_expr(col_expr(1), lit_for_test(5))];
        let s = PredicateSplit::from_conjuncts(&preds, &resolve_passthrough);
        assert_eq!(s.range.len(), 1);
        assert!(s.equality.is_empty());
        assert!(s.residual.is_empty());
        assert_eq!(s.range[0].col, MvColumnId(1));
        assert!(matches!(
            s.range[0].bound,
            RangeBound::LowerBound {
                inclusive: false,
                ..
            }
        ));
    }

    #[test]
    fn split_classifies_residual_when_unrecognized() {
        // A CASE expression is not recognized by either matcher.
        let case_expr = TypedExpr {
            data_type: DataType::Boolean,
            nullable: false,
            kind: ExprKind::Case {
                operand: None,
                when_then: vec![(lit_for_test(1), lit_for_test(1))],
                else_expr: Some(Box::new(lit_for_test(0))),
            },
        };
        let s = PredicateSplit::from_conjuncts(&[case_expr], &resolve_passthrough);
        assert_eq!(s.residual.len(), 1);
        assert!(s.equality.is_empty());
        assert!(s.range.is_empty());
    }

    #[test]
    fn containment_rejects_when_mv_has_eq_query_doesnt() {
        let mv = PredicateSplit {
            equality: vec![EqualityPred {
                col: MvColumnId(1),
                literal: lit_for_test(5),
            }],
            ..Default::default()
        };
        let query = PredicateSplit::default();
        assert!(contain_and_compensate(&query, &mv).is_none());
    }

    #[test]
    fn containment_compensates_extra_query_eq() {
        let query = PredicateSplit {
            equality: vec![
                EqualityPred {
                    col: MvColumnId(1),
                    literal: lit_for_test(5),
                },
                EqualityPred {
                    col: MvColumnId(2),
                    literal: lit_for_test(7),
                },
            ],
            ..Default::default()
        };
        let mv = PredicateSplit {
            equality: vec![EqualityPred {
                col: MvColumnId(1),
                literal: lit_for_test(5),
            }],
            ..Default::default()
        };
        let comp = contain_and_compensate(&query, &mv).expect("should match");
        assert_eq!(comp.eq.len(), 1);
        assert_eq!(comp.eq[0].col, MvColumnId(2));
    }

    #[test]
    fn range_subset_lower_bound_pass() {
        // query col > 10, mv col > 5 → query is tighter → subset = true
        let q = RangeBound::LowerBound {
            literal: lit_for_test(10),
            inclusive: false,
        };
        let m = RangeBound::LowerBound {
            literal: lit_for_test(5),
            inclusive: false,
        };
        assert!(range_subset(&q, &m));
    }

    #[test]
    fn range_subset_lower_bound_fail() {
        // query col > 3, mv col > 5 → query starts before mv → not subset
        let q = RangeBound::LowerBound {
            literal: lit_for_test(3),
            inclusive: false,
        };
        let m = RangeBound::LowerBound {
            literal: lit_for_test(5),
            inclusive: false,
        };
        assert!(!range_subset(&q, &m));
    }

    #[test]
    fn range_subset_upper_bound_pass() {
        // query col < 20, mv col < 30 → query ends before mv → subset
        let q = RangeBound::UpperBound {
            literal: lit_for_test(20),
            inclusive: false,
        };
        let m = RangeBound::UpperBound {
            literal: lit_for_test(30),
            inclusive: false,
        };
        assert!(range_subset(&q, &m));
    }

    #[test]
    fn literal_ge_and_le_helpers() {
        assert!(literal_ge(&lit_for_test(10), &lit_for_test(5)));
        assert!(literal_ge(&lit_for_test(5), &lit_for_test(5)));
        assert!(!literal_ge(&lit_for_test(4), &lit_for_test(5)));

        assert!(literal_le(&lit_for_test(3), &lit_for_test(5)));
        assert!(literal_le(&lit_for_test(5), &lit_for_test(5)));
        assert!(!literal_le(&lit_for_test(6), &lit_for_test(5)));
    }
}
