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

//! Provider-neutral row-group pruning against a live dynamic filter.
//!
//! The decision is deliberately one-sided: a row group is dropped only when the
//! statistics *prove* it cannot satisfy the predicate. Missing, incomplete, or
//! untyped statistics keep the row group, because a wrong prune silently
//! returns fewer rows while a wrong keep only costs work.
//!
//! Identity is `(scheduled split sequence, row-group ordinal)`. Both come from
//! facts the task already owns, so no membership digest is needed to name a
//! row group.

use std::cmp::Ordering;

use novarocks_spi::connector::read_stack::{Bound, ConnectorValue, Domain, Range};

/// Statistics one row group reports for one column.
///
/// Every field is optional on purpose: a Parquet writer may omit any of them,
/// and the evaluator must stay correct when it does.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct RowGroupColumnStatistics {
    pub min: Option<ConnectorValue>,
    pub max: Option<ConnectorValue>,
    pub null_count: Option<u64>,
    pub value_count: Option<u64>,
    /// Whether the writer's min/max are known to be exact and comparable.
    /// A deprecated or unknown-sort-order statistic must set this to false.
    pub bounds_are_exact: bool,
}

/// Why a row group could not be judged.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RowGroupNotEvaluatedReason {
    /// No statistics at all for the filtered column.
    MissingStatistics,
    /// Statistics exist but are not usable as exact bounds.
    InexactStatistics,
    /// The value types do not line up, so no comparison is defined.
    IncomparableTypes,
}

/// The outcome of judging one row group against one column domain.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RowGroupOutcome {
    /// Proven to contain no matching row.
    Pruned,
    /// Might contain a matching row.
    Kept,
    /// No decision was possible; the caller keeps the row group.
    NotEvaluated(RowGroupNotEvaluatedReason),
}

impl RowGroupOutcome {
    /// Whether the caller must still read this row group.
    pub const fn must_read(self) -> bool {
        !matches!(self, Self::Pruned)
    }
}

/// Names one judged row group without inventing an identity for it.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct RuntimeFilterRowGroupId {
    pub scheduled_split_sequence_id: u64,
    pub row_group_ordinal: u32,
}

impl RuntimeFilterRowGroupId {
    pub const fn new(scheduled_split_sequence_id: u64, row_group_ordinal: u32) -> Self {
        Self {
            scheduled_split_sequence_id,
            row_group_ordinal,
        }
    }
}

/// Judge one row group against one column's domain.
pub fn evaluate_row_group(
    domain: &Domain,
    statistics: &RowGroupColumnStatistics,
) -> RowGroupOutcome {
    // An unconstrained domain never prunes, and an unsatisfiable one always
    // does; neither needs a statistic.
    if domain.is_all() {
        return RowGroupOutcome::Kept;
    }
    if domain.is_none() {
        return RowGroupOutcome::Pruned;
    }

    let all_null = matches!(
        (statistics.null_count, statistics.value_count),
        (Some(nulls), Some(values)) if values > 0 && nulls == values
    );
    if all_null {
        return if domain.null_allowed() {
            RowGroupOutcome::Kept
        } else {
            RowGroupOutcome::Pruned
        };
    }

    let has_non_null = match (statistics.null_count, statistics.value_count) {
        (Some(nulls), Some(values)) => values > nulls,
        _ => true,
    };
    if !has_non_null {
        // Row count unknown or zero non-null rows we cannot confirm: keep.
        return RowGroupOutcome::Kept;
    }

    let values = domain.values();
    if values.is_none() {
        // Only NULL satisfies this domain, and there is at least one non-null
        // row; a row group with no nulls at all cannot match.
        return match statistics.null_count {
            Some(0) => RowGroupOutcome::Pruned,
            _ => RowGroupOutcome::Kept,
        };
    }

    if !statistics.bounds_are_exact {
        return RowGroupOutcome::NotEvaluated(RowGroupNotEvaluatedReason::InexactStatistics);
    }
    let (Some(min), Some(max)) = (statistics.min.as_ref(), statistics.max.as_ref()) else {
        return RowGroupOutcome::NotEvaluated(RowGroupNotEvaluatedReason::MissingStatistics);
    };
    if min.value_type() != values.value_type() || max.value_type() != values.value_type() {
        return RowGroupOutcome::NotEvaluated(RowGroupNotEvaluatedReason::IncomparableTypes);
    }

    for range in values.ranges() {
        match range_overlaps_bounds(range, min, max) {
            Ok(true) => return RowGroupOutcome::Kept,
            Ok(false) => {}
            Err(reason) => return RowGroupOutcome::NotEvaluated(reason),
        }
    }
    // No range can hold a value inside the row group's bounds. A null-allowing
    // domain still matches if the row group has any null.
    if domain.null_allowed() && statistics.null_count.is_none_or(|nulls| nulls > 0) {
        return RowGroupOutcome::Kept;
    }
    RowGroupOutcome::Pruned
}

/// Whether one range can contain a value within `[min, max]`.
fn range_overlaps_bounds(
    range: &Range,
    min: &ConnectorValue,
    max: &ConnectorValue,
) -> Result<bool, RowGroupNotEvaluatedReason> {
    // The row group's low end must not sit above the range's high end.
    match range.high() {
        Bound::Unbounded => {}
        Bound::Inclusive(high) => {
            if compare(min, high)? == Ordering::Greater {
                return Ok(false);
            }
        }
        Bound::Exclusive(high) => {
            if compare(min, high)? != Ordering::Less {
                return Ok(false);
            }
        }
    }
    // ...and its high end must not sit below the range's low end.
    match range.low() {
        Bound::Unbounded => {}
        Bound::Inclusive(low) => {
            if compare(max, low)? == Ordering::Less {
                return Ok(false);
            }
        }
        Bound::Exclusive(low) => {
            if compare(max, low)? != Ordering::Greater {
                return Ok(false);
            }
        }
    }
    Ok(true)
}

fn compare(
    left: &ConnectorValue,
    right: &ConnectorValue,
) -> Result<Ordering, RowGroupNotEvaluatedReason> {
    left.try_compare_same_type(right)
        .ok_or(RowGroupNotEvaluatedReason::IncomparableTypes)
}

#[cfg(test)]
mod tests {
    use novarocks_spi::connector::read_stack::{ConnectorValueType, ValueSet};

    use super::*;

    fn big_int(value: i64) -> ConnectorValue {
        ConnectorValue::BigInt(value)
    }

    fn exact(min: i64, max: i64) -> RowGroupColumnStatistics {
        RowGroupColumnStatistics {
            min: Some(big_int(min)),
            max: Some(big_int(max)),
            null_count: Some(0),
            value_count: Some(100),
            bounds_are_exact: true,
        }
    }

    fn equals(value: i64) -> Domain {
        Domain::single_value(big_int(value)).expect("single value")
    }

    fn at_least(value: i64) -> Domain {
        let range = Range::try_new(
            ConnectorValueType::BigInt,
            Bound::Inclusive(big_int(value)),
            Bound::Unbounded,
        )
        .expect("range");
        Domain::new(
            ValueSet::of_ranges(ConnectorValueType::BigInt, vec![range]).expect("set"),
            false,
        )
    }

    #[test]
    fn a_disjoint_row_group_is_pruned_and_an_overlapping_one_is_kept() {
        assert_eq!(
            evaluate_row_group(&equals(500), &exact(0, 100)),
            RowGroupOutcome::Pruned
        );
        assert_eq!(
            evaluate_row_group(&equals(50), &exact(0, 100)),
            RowGroupOutcome::Kept
        );
        assert_eq!(
            evaluate_row_group(&at_least(101), &exact(0, 100)),
            RowGroupOutcome::Pruned
        );
        assert_eq!(
            evaluate_row_group(&at_least(100), &exact(0, 100)),
            RowGroupOutcome::Kept
        );
    }

    #[test]
    fn exclusive_bounds_are_respected_at_the_edge() {
        let range = Range::try_new(
            ConnectorValueType::BigInt,
            Bound::Exclusive(big_int(100)),
            Bound::Unbounded,
        )
        .expect("range");
        let domain = Domain::new(
            ValueSet::of_ranges(ConnectorValueType::BigInt, vec![range]).expect("set"),
            false,
        );
        assert_eq!(
            evaluate_row_group(&domain, &exact(0, 100)),
            RowGroupOutcome::Pruned
        );
        assert_eq!(
            evaluate_row_group(&domain, &exact(0, 101)),
            RowGroupOutcome::Kept
        );
    }

    #[test]
    fn missing_or_inexact_statistics_never_prune() {
        let mut missing = exact(0, 100);
        missing.min = None;
        assert_eq!(
            evaluate_row_group(&equals(500), &missing),
            RowGroupOutcome::NotEvaluated(RowGroupNotEvaluatedReason::MissingStatistics)
        );
        assert!(evaluate_row_group(&equals(500), &missing).must_read());

        let mut inexact = exact(0, 100);
        inexact.bounds_are_exact = false;
        assert_eq!(
            evaluate_row_group(&equals(500), &inexact),
            RowGroupOutcome::NotEvaluated(RowGroupNotEvaluatedReason::InexactStatistics)
        );
    }

    #[test]
    fn a_type_mismatch_is_never_a_prune() {
        let mut mismatched = exact(0, 100);
        mismatched.min = Some(ConnectorValue::Integer(0));
        mismatched.max = Some(ConnectorValue::Integer(100));
        assert_eq!(
            evaluate_row_group(&equals(500), &mismatched),
            RowGroupOutcome::NotEvaluated(RowGroupNotEvaluatedReason::IncomparableTypes)
        );
    }

    #[test]
    fn nullability_is_judged_without_bounds() {
        let all_null = RowGroupColumnStatistics {
            min: None,
            max: None,
            null_count: Some(100),
            value_count: Some(100),
            bounds_are_exact: false,
        };
        assert_eq!(
            evaluate_row_group(&Domain::only_null(ConnectorValueType::BigInt), &all_null),
            RowGroupOutcome::Kept
        );
        assert_eq!(
            evaluate_row_group(&equals(1), &all_null),
            RowGroupOutcome::Pruned
        );

        let no_nulls = exact(0, 100);
        assert_eq!(
            evaluate_row_group(&Domain::only_null(ConnectorValueType::BigInt), &no_nulls),
            RowGroupOutcome::Pruned
        );
    }

    #[test]
    fn an_unconstrained_domain_keeps_and_an_unsatisfiable_one_prunes() {
        assert_eq!(
            evaluate_row_group(&Domain::all(ConnectorValueType::BigInt), &exact(0, 100)),
            RowGroupOutcome::Kept
        );
        assert_eq!(
            evaluate_row_group(&Domain::none(ConnectorValueType::BigInt), &exact(0, 100)),
            RowGroupOutcome::Pruned
        );
    }

    #[test]
    fn a_null_allowing_domain_keeps_a_row_group_that_has_nulls() {
        let mut with_nulls = exact(0, 100);
        with_nulls.null_count = Some(5);
        let domain = Domain::new(
            ValueSet::of_values(ConnectorValueType::BigInt, vec![big_int(500)]).expect("set"),
            true,
        );
        assert_eq!(
            evaluate_row_group(&domain, &with_nulls),
            RowGroupOutcome::Kept
        );

        let mut without_nulls = with_nulls.clone();
        without_nulls.null_count = Some(0);
        assert_eq!(
            evaluate_row_group(&domain, &without_nulls),
            RowGroupOutcome::Pruned
        );
    }

    #[test]
    fn a_row_group_identity_needs_no_digest() {
        let first = RuntimeFilterRowGroupId::new(7, 2);
        assert_eq!(first, RuntimeFilterRowGroupId::new(7, 2));
        assert_ne!(first, RuntimeFilterRowGroupId::new(7, 3));
    }
}
