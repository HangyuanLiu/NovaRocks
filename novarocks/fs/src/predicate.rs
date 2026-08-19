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

use std::cmp::Ordering;

#[derive(Clone, Debug, PartialEq)]
pub enum MinMaxPredicateValue {
    Boolean(bool),
    Int32(i32),
    Int64(i64),
    Float(f32),
    Double(f64),
    ByteArray(Vec<u8>),
    FixedLenByteArray(Vec<u8>),
    Date32(i32),
    DateTimeMicros(i64),
    DateTimeNanos(i64),
    LargeInt(i128),
    Decimal128 {
        value: i128,
        precision: u8,
        scale: i8,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MinMaxPredicateOp {
    Le,
    Ge,
    Lt,
    Gt,
    Eq,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ScanPredicateSource {
    Static,
    RuntimeIn,
    RuntimeMembership,
    RuntimeMinMax,
}

#[derive(Clone, Debug, PartialEq)]
pub enum ScanPredicateDomain {
    Range {
        op: MinMaxPredicateOp,
        value: MinMaxPredicateValue,
    },
    DiscreteSet {
        values: Vec<MinMaxPredicateValue>,
        min: MinMaxPredicateValue,
        max: MinMaxPredicateValue,
    },
    Membership {
        values: Vec<MinMaxPredicateValue>,
    },
}

impl ScanPredicateDomain {
    /// Evaluate this domain against one `[min, max]` bound pair.
    ///
    /// The bounds may come from any statistics carrier -- a Parquet footer, an
    /// Iceberg manifest, or another physical source. The evaluation is
    /// deliberately source-agnostic: callers own the decoding that produces the
    /// bounds, this function owns only the comparison. Keeping it here is what
    /// lets file-level and row-group-level pruning share one judgement instead
    /// of drifting apart.
    ///
    /// Returns `true` when a row satisfying the domain may exist inside the
    /// bounds, i.e. the caller must keep the unit.
    ///
    /// Pruning must be *sound*: a unit may only be skipped when it provably
    /// cannot produce a TRUE row. Skipping too much is a correctness bug;
    /// keeping too much only costs I/O. Every case where the bounds cannot be
    /// judged therefore resolves to "keep":
    ///
    /// - values that are not comparable (different variants -- e.g. an `Int64`
    ///   predicate against `Int32` statistics written before an Iceberg
    ///   `int -> long` promotion) yield `None` from `compare` and keep the unit;
    /// - a missing bound pair is handled by the caller, which also keeps.
    ///
    /// An empty `DiscreteSet` / `Membership` is a different matter and still
    /// prunes: `x IN ()` is unsatisfiable, so skipping is provably correct.
    pub fn may_match_bounds(&self, min: &MinMaxPredicateValue, max: &MinMaxPredicateValue) -> bool {
        match self {
            Self::Range { op, value } => match op {
                MinMaxPredicateOp::Le => {
                    compare(min, value).is_none_or(|order| order != Ordering::Greater)
                }
                MinMaxPredicateOp::Lt => {
                    compare(min, value).is_none_or(|order| order == Ordering::Less)
                }
                MinMaxPredicateOp::Ge => {
                    compare(max, value).is_none_or(|order| order != Ordering::Less)
                }
                MinMaxPredicateOp::Gt => {
                    compare(max, value).is_none_or(|order| order == Ordering::Greater)
                }
                MinMaxPredicateOp::Eq => {
                    compare(min, value).is_none_or(|order| order != Ordering::Greater)
                        && compare(max, value).is_none_or(|order| order != Ordering::Less)
                }
            },
            Self::DiscreteSet { values, .. } | Self::Membership { values } => {
                values.iter().any(|value| {
                    compare(min, value).is_none_or(|order| order != Ordering::Greater)
                        && compare(max, value).is_none_or(|order| order != Ordering::Less)
                })
            }
        }
    }

    /// Returns whether every literal in this domain can be compared to the
    /// supplied physical bounds. Readers use this to distinguish a definite
    /// page-index judgement from the conservative "keep" result returned by
    /// [`Self::may_match_bounds`] for incomparable logical representations.
    pub fn can_compare_bounds(
        &self,
        min: &MinMaxPredicateValue,
        max: &MinMaxPredicateValue,
    ) -> bool {
        let comparable = |value: &MinMaxPredicateValue| {
            compare(min, value).is_some() && compare(max, value).is_some()
        };
        match self {
            Self::Range { value, .. } => comparable(value),
            Self::DiscreteSet { values, .. } | Self::Membership { values } => {
                values.iter().all(comparable)
            }
        }
    }
}

fn compare(left: &MinMaxPredicateValue, right: &MinMaxPredicateValue) -> Option<Ordering> {
    match (left, right) {
        (MinMaxPredicateValue::Boolean(a), MinMaxPredicateValue::Boolean(b)) => a.partial_cmp(b),
        (MinMaxPredicateValue::Int32(a), MinMaxPredicateValue::Int32(b)) => a.partial_cmp(b),
        (MinMaxPredicateValue::Int64(a), MinMaxPredicateValue::Int64(b)) => a.partial_cmp(b),
        (MinMaxPredicateValue::Float(a), MinMaxPredicateValue::Float(b)) => a.partial_cmp(b),
        (MinMaxPredicateValue::Double(a), MinMaxPredicateValue::Double(b)) => a.partial_cmp(b),
        (MinMaxPredicateValue::ByteArray(a), MinMaxPredicateValue::ByteArray(b))
        | (
            MinMaxPredicateValue::FixedLenByteArray(a),
            MinMaxPredicateValue::FixedLenByteArray(b),
        ) => a.partial_cmp(b),
        _ => None,
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct ScanPredicate {
    column: String,
    /// Optional physical field identifier. When present, readers must bind the
    /// predicate by this identifier and must not fall back to a same-named
    /// column. This keeps Iceberg rename/reorder reads conservative.
    physical_field_id: Option<i32>,
    domain: ScanPredicateDomain,
    source: ScanPredicateSource,
}

impl ScanPredicate {
    pub fn new(
        column: impl Into<String>,
        domain: ScanPredicateDomain,
        source: ScanPredicateSource,
    ) -> Self {
        Self {
            column: column.into(),
            physical_field_id: None,
            domain,
            source,
        }
    }

    pub fn with_physical_field_id(mut self, physical_field_id: i32) -> Self {
        self.physical_field_id = Some(physical_field_id);
        self
    }

    pub fn column(&self) -> &str {
        &self.column
    }

    pub fn physical_field_id(&self) -> Option<i32> {
        self.physical_field_id
    }

    pub fn domain(&self) -> &ScanPredicateDomain {
        &self.domain
    }

    pub fn source(&self) -> ScanPredicateSource {
        self.source
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct PhysicalPruning {
    pub row_groups: Option<Vec<usize>>,
    pub pages: Vec<PhysicalPageSelection>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PhysicalPageSelection {
    pub row_group: usize,
    pub page_indices: Vec<usize>,
}

#[cfg(test)]
mod tests {
    use super::*;

    const BOUNDS: (MinMaxPredicateValue, MinMaxPredicateValue) = (
        MinMaxPredicateValue::Int32(10),
        MinMaxPredicateValue::Int32(20),
    );

    const ALL_OPS: [MinMaxPredicateOp; 5] = [
        MinMaxPredicateOp::Eq,
        MinMaxPredicateOp::Lt,
        MinMaxPredicateOp::Le,
        MinMaxPredicateOp::Gt,
        MinMaxPredicateOp::Ge,
    ];

    fn range(op: MinMaxPredicateOp, value: MinMaxPredicateValue) -> ScanPredicateDomain {
        ScanPredicateDomain::Range { op, value }
    }

    fn discrete(values: Vec<MinMaxPredicateValue>) -> ScanPredicateDomain {
        let min = values
            .first()
            .cloned()
            .unwrap_or(MinMaxPredicateValue::Int32(0));
        let max = values
            .last()
            .cloned()
            .unwrap_or(MinMaxPredicateValue::Int32(0));
        ScanPredicateDomain::DiscreteSet { values, min, max }
    }

    #[test]
    fn range_ops_evaluate_against_the_correct_bound() {
        let (min, max) = BOUNDS;
        for (op, literal, expected) in [
            (MinMaxPredicateOp::Eq, 12, true),
            (MinMaxPredicateOp::Eq, 21, false),
            (MinMaxPredicateOp::Eq, 9, false),
            (MinMaxPredicateOp::Lt, 11, true),
            (MinMaxPredicateOp::Lt, 10, false),
            (MinMaxPredicateOp::Le, 10, true),
            (MinMaxPredicateOp::Le, 9, false),
            (MinMaxPredicateOp::Gt, 19, true),
            (MinMaxPredicateOp::Gt, 20, false),
            (MinMaxPredicateOp::Ge, 20, true),
            (MinMaxPredicateOp::Ge, 21, false),
        ] {
            let domain = range(op, MinMaxPredicateValue::Int32(literal));
            assert_eq!(
                domain.may_match_bounds(&min, &max),
                expected,
                "bounds [10, 20] with {op:?} {literal}"
            );
        }
    }

    /// Iceberg `int -> long` promotion: the table schema became `long`, so the
    /// predicate literal arrives as `Int64`, while data files written before the
    /// promotion still publish `Int32` statistics. The pair cannot be compared,
    /// and pruning stays sound only by keeping the unit.
    #[test]
    fn incomparable_variants_keep_the_unit_for_every_op() {
        let (min, max) = BOUNDS;
        for op in ALL_OPS {
            let domain = range(op, MinMaxPredicateValue::Int64(12));
            assert!(
                domain.may_match_bounds(&min, &max),
                "{op:?} must keep the unit when the literal cannot be compared to the bounds"
            );
        }
    }

    #[test]
    fn incomparable_discrete_set_keeps_the_unit() {
        let (min, max) = BOUNDS;
        // Every value is outside [10, 20] numerically, but none is comparable,
        // so the set must not be treated as disjoint from the bounds.
        let domain = discrete(vec![
            MinMaxPredicateValue::Int64(1),
            MinMaxPredicateValue::Int64(30),
        ]);
        assert!(domain.may_match_bounds(&min, &max));
    }

    #[test]
    fn empty_value_set_still_prunes() {
        let (min, max) = BOUNDS;
        // `x IN ()` is unsatisfiable, so skipping the unit is provably correct.
        assert!(!discrete(Vec::new()).may_match_bounds(&min, &max));
        assert!(
            !ScanPredicateDomain::Membership { values: Vec::new() }.may_match_bounds(&min, &max)
        );
    }

    #[test]
    fn discrete_set_prunes_only_when_every_value_is_outside() {
        let (min, max) = BOUNDS;
        let disjoint = discrete(vec![
            MinMaxPredicateValue::Int32(1),
            MinMaxPredicateValue::Int32(30),
        ]);
        assert!(!disjoint.may_match_bounds(&min, &max));

        let overlapping = discrete(vec![
            MinMaxPredicateValue::Int32(1),
            MinMaxPredicateValue::Int32(12),
        ]);
        assert!(overlapping.may_match_bounds(&min, &max));
    }

    #[test]
    fn membership_follows_the_same_rule_as_discrete_set() {
        let (min, max) = BOUNDS;
        let disjoint = ScanPredicateDomain::Membership {
            values: vec![MinMaxPredicateValue::Int32(30)],
        };
        assert!(!disjoint.may_match_bounds(&min, &max));

        let overlapping = ScanPredicateDomain::Membership {
            values: vec![MinMaxPredicateValue::Int32(12)],
        };
        assert!(overlapping.may_match_bounds(&min, &max));
    }
}
