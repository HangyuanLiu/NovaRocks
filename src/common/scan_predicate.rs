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

use crate::common::min_max_predicate::{MinMaxPredicate, MinMaxPredicateOp, MinMaxPredicateValue};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[allow(dead_code)]
pub(crate) enum ScanPredicateSource {
    Static,
    RuntimeIn,
    RuntimeMembership,
    RuntimeMinMax,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum ScanPredicateDomain {
    Range {
        op: MinMaxPredicateOp,
        value: MinMaxPredicateValue,
    },
    DiscreteSet {
        values: Vec<MinMaxPredicateValue>,
        min: MinMaxPredicateValue,
        max: MinMaxPredicateValue,
    },
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct ScanPredicate {
    column: String,
    domain: ScanPredicateDomain,
    source: ScanPredicateSource,
}

impl ScanPredicate {
    pub(crate) fn new(
        column: String,
        domain: ScanPredicateDomain,
        source: ScanPredicateSource,
    ) -> Self {
        Self {
            column,
            domain,
            source,
        }
    }

    pub(crate) fn from_min_max_predicate(
        predicate: MinMaxPredicate,
        source: ScanPredicateSource,
    ) -> Self {
        let column = predicate.column().to_string();
        let domain = ScanPredicateDomain::Range {
            op: predicate.op(),
            value: predicate.value().clone(),
        };
        Self::new(column, domain, source)
    }

    pub(crate) fn discrete_set(
        column: String,
        mut values: Vec<MinMaxPredicateValue>,
        source: ScanPredicateSource,
    ) -> Result<Self, String> {
        let Some(first) = values.first() else {
            return Err("scan predicate discrete set cannot be empty".to_string());
        };
        let Some(family) = ScanPredicateValueFamily::from_value(first) else {
            return Err("mixed scan predicate value families are unsupported".to_string());
        };
        if values
            .iter()
            .any(|value| ScanPredicateValueFamily::from_value(value) != Some(family))
        {
            return Err("mixed scan predicate value families are unsupported".to_string());
        }

        values.sort_by(compare_scan_predicate_values);
        values.dedup_by(|left, right| compare_scan_predicate_values(left, right).is_eq());

        let min = values
            .first()
            .expect("discrete set should retain at least one value")
            .clone();
        let max = values
            .last()
            .expect("discrete set should retain at least one value")
            .clone();

        Ok(Self::new(
            column,
            ScanPredicateDomain::DiscreteSet { values, min, max },
            source,
        ))
    }

    pub(crate) fn column(&self) -> &str {
        &self.column
    }

    pub(crate) fn source(&self) -> ScanPredicateSource {
        self.source
    }

    pub(crate) fn domain(&self) -> &ScanPredicateDomain {
        &self.domain
    }

    pub(crate) fn range_op(&self) -> Option<MinMaxPredicateOp> {
        match &self.domain {
            ScanPredicateDomain::Range { op, .. } => Some(*op),
            ScanPredicateDomain::DiscreteSet { .. } => None,
        }
    }

    pub(crate) fn to_min_max_predicates(&self) -> Vec<MinMaxPredicate> {
        match &self.domain {
            ScanPredicateDomain::Range { op, value } => {
                vec![min_max_predicate_from_parts(
                    self.column.clone(),
                    *op,
                    value.clone(),
                )]
            }
            ScanPredicateDomain::DiscreteSet { min, max, .. } => {
                vec![
                    MinMaxPredicate::Ge {
                        column: self.column.clone(),
                        value: min.clone(),
                    },
                    MinMaxPredicate::Le {
                        column: self.column.clone(),
                        value: max.clone(),
                    },
                ]
            }
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ScanPredicateValueFamily {
    Boolean,
    Int32,
    Int64,
    ByteArray,
    FixedLenByteArray,
    Date32,
    DateTimeMicros,
    DateTimeNanos,
    LargeInt,
    Decimal128 { precision: u8, scale: i8 },
}

impl ScanPredicateValueFamily {
    fn from_value(value: &MinMaxPredicateValue) -> Option<Self> {
        match value {
            MinMaxPredicateValue::Boolean(_) => Some(Self::Boolean),
            MinMaxPredicateValue::Int32(_) => Some(Self::Int32),
            MinMaxPredicateValue::Int64(_) => Some(Self::Int64),
            MinMaxPredicateValue::Float(_) | MinMaxPredicateValue::Double(_) => None,
            MinMaxPredicateValue::ByteArray(_) => Some(Self::ByteArray),
            MinMaxPredicateValue::FixedLenByteArray(_) => Some(Self::FixedLenByteArray),
            MinMaxPredicateValue::Date32(_) => Some(Self::Date32),
            MinMaxPredicateValue::DateTimeMicros(_) => Some(Self::DateTimeMicros),
            MinMaxPredicateValue::DateTimeNanos(_) => Some(Self::DateTimeNanos),
            MinMaxPredicateValue::LargeInt(_) => Some(Self::LargeInt),
            MinMaxPredicateValue::Decimal128 {
                precision, scale, ..
            } => Some(Self::Decimal128 {
                precision: *precision,
                scale: *scale,
            }),
        }
    }
}

fn compare_scan_predicate_values(
    left: &MinMaxPredicateValue,
    right: &MinMaxPredicateValue,
) -> Ordering {
    debug_assert_eq!(
        ScanPredicateValueFamily::from_value(left),
        ScanPredicateValueFamily::from_value(right)
    );

    match (left, right) {
        (MinMaxPredicateValue::Boolean(left), MinMaxPredicateValue::Boolean(right)) => {
            left.cmp(right)
        }
        (MinMaxPredicateValue::Int32(left), MinMaxPredicateValue::Int32(right)) => left.cmp(right),
        (MinMaxPredicateValue::Int64(left), MinMaxPredicateValue::Int64(right)) => left.cmp(right),
        (MinMaxPredicateValue::ByteArray(left), MinMaxPredicateValue::ByteArray(right)) => {
            left.cmp(right)
        }
        (
            MinMaxPredicateValue::FixedLenByteArray(left),
            MinMaxPredicateValue::FixedLenByteArray(right),
        ) => left.cmp(right),
        (MinMaxPredicateValue::Date32(left), MinMaxPredicateValue::Date32(right)) => {
            left.cmp(right)
        }
        (
            MinMaxPredicateValue::DateTimeMicros(left),
            MinMaxPredicateValue::DateTimeMicros(right),
        ) => left.cmp(right),
        (MinMaxPredicateValue::DateTimeNanos(left), MinMaxPredicateValue::DateTimeNanos(right)) => {
            left.cmp(right)
        }
        (MinMaxPredicateValue::LargeInt(left), MinMaxPredicateValue::LargeInt(right)) => {
            left.cmp(right)
        }
        (
            MinMaxPredicateValue::Decimal128 {
                value: left,
                precision: left_precision,
                scale: left_scale,
            },
            MinMaxPredicateValue::Decimal128 {
                value: right,
                precision: right_precision,
                scale: right_scale,
            },
        ) if left_precision == right_precision && left_scale == right_scale => left.cmp(right),
        _ => Ordering::Equal,
    }
}

fn min_max_predicate_from_parts(
    column: String,
    op: MinMaxPredicateOp,
    value: MinMaxPredicateValue,
) -> MinMaxPredicate {
    match op {
        MinMaxPredicateOp::Le => MinMaxPredicate::Le { column, value },
        MinMaxPredicateOp::Ge => MinMaxPredicate::Ge { column, value },
        MinMaxPredicateOp::Lt => MinMaxPredicate::Lt { column, value },
        MinMaxPredicateOp::Gt => MinMaxPredicate::Gt { column, value },
        MinMaxPredicateOp::Eq => MinMaxPredicate::Eq { column, value },
    }
}

#[cfg(test)]
mod tests {
    use crate::common::min_max_predicate::{
        MinMaxPredicate, MinMaxPredicateOp, MinMaxPredicateValue,
    };
    use crate::common::scan_predicate::{ScanPredicate, ScanPredicateDomain, ScanPredicateSource};

    #[test]
    fn range_predicate_round_trips_to_min_max_predicate() {
        let predicate = ScanPredicate::from_min_max_predicate(
            MinMaxPredicate::Ge {
                column: "0".to_string(),
                value: MinMaxPredicateValue::Int32(10),
            },
            ScanPredicateSource::Static,
        );

        assert_eq!(predicate.column(), "0");
        assert_eq!(predicate.source(), ScanPredicateSource::Static);
        assert_eq!(
            predicate.to_min_max_predicates(),
            vec![MinMaxPredicate::Ge {
                column: "0".to_string(),
                value: MinMaxPredicateValue::Int32(10),
            }]
        );
    }

    #[test]
    fn discrete_set_builds_stable_envelope() {
        let predicate = ScanPredicate::discrete_set(
            "0".to_string(),
            vec![
                MinMaxPredicateValue::Int32(100),
                MinMaxPredicateValue::Int32(1),
                MinMaxPredicateValue::Int32(50),
            ],
            ScanPredicateSource::RuntimeIn,
        )
        .expect("discrete predicate");

        assert_eq!(
            predicate.domain(),
            &ScanPredicateDomain::DiscreteSet {
                values: vec![
                    MinMaxPredicateValue::Int32(1),
                    MinMaxPredicateValue::Int32(50),
                    MinMaxPredicateValue::Int32(100),
                ],
                min: MinMaxPredicateValue::Int32(1),
                max: MinMaxPredicateValue::Int32(100),
            }
        );
        assert_eq!(
            predicate.to_min_max_predicates(),
            vec![
                MinMaxPredicate::Ge {
                    column: "0".to_string(),
                    value: MinMaxPredicateValue::Int32(1),
                },
                MinMaxPredicate::Le {
                    column: "0".to_string(),
                    value: MinMaxPredicateValue::Int32(100),
                },
            ]
        );
    }

    #[test]
    fn discrete_set_rejects_empty_values() {
        let err = ScanPredicate::discrete_set(
            "0".to_string(),
            vec![],
            ScanPredicateSource::RuntimeMembership,
        )
        .expect_err("empty discrete sets are unsupported");

        assert!(err.contains("discrete set cannot be empty"));
    }

    #[test]
    fn discrete_set_sorts_and_deduplicates_values() {
        let predicate = ScanPredicate::discrete_set(
            "0".to_string(),
            vec![
                MinMaxPredicateValue::Int64(9),
                MinMaxPredicateValue::Int64(3),
                MinMaxPredicateValue::Int64(9),
                MinMaxPredicateValue::Int64(1),
                MinMaxPredicateValue::Int64(3),
            ],
            ScanPredicateSource::RuntimeIn,
        )
        .expect("discrete predicate");

        assert_eq!(
            predicate.domain(),
            &ScanPredicateDomain::DiscreteSet {
                values: vec![
                    MinMaxPredicateValue::Int64(1),
                    MinMaxPredicateValue::Int64(3),
                    MinMaxPredicateValue::Int64(9),
                ],
                min: MinMaxPredicateValue::Int64(1),
                max: MinMaxPredicateValue::Int64(9),
            }
        );
    }

    #[test]
    fn discrete_set_rejects_float_and_double_values() {
        for value in [
            MinMaxPredicateValue::Float(1.0),
            MinMaxPredicateValue::Double(1.0),
        ] {
            let err = ScanPredicate::discrete_set(
                "0".to_string(),
                vec![value],
                ScanPredicateSource::RuntimeIn,
            )
            .expect_err("floating point values are unsupported");

            assert!(err.contains("mixed scan predicate value families"));
        }
    }

    #[test]
    fn discrete_set_sorts_decimal128_with_same_precision_and_scale() {
        let predicate = ScanPredicate::discrete_set(
            "0".to_string(),
            vec![
                MinMaxPredicateValue::Decimal128 {
                    value: 300,
                    precision: 12,
                    scale: 2,
                },
                MinMaxPredicateValue::Decimal128 {
                    value: 100,
                    precision: 12,
                    scale: 2,
                },
                MinMaxPredicateValue::Decimal128 {
                    value: 200,
                    precision: 12,
                    scale: 2,
                },
            ],
            ScanPredicateSource::RuntimeIn,
        )
        .expect("decimal discrete predicate");

        assert_eq!(
            predicate.domain(),
            &ScanPredicateDomain::DiscreteSet {
                values: vec![
                    MinMaxPredicateValue::Decimal128 {
                        value: 100,
                        precision: 12,
                        scale: 2,
                    },
                    MinMaxPredicateValue::Decimal128 {
                        value: 200,
                        precision: 12,
                        scale: 2,
                    },
                    MinMaxPredicateValue::Decimal128 {
                        value: 300,
                        precision: 12,
                        scale: 2,
                    },
                ],
                min: MinMaxPredicateValue::Decimal128 {
                    value: 100,
                    precision: 12,
                    scale: 2,
                },
                max: MinMaxPredicateValue::Decimal128 {
                    value: 300,
                    precision: 12,
                    scale: 2,
                },
            }
        );
    }

    #[test]
    fn discrete_set_rejects_decimal128_precision_or_scale_mismatch() {
        let err = ScanPredicate::discrete_set(
            "0".to_string(),
            vec![
                MinMaxPredicateValue::Decimal128 {
                    value: 100,
                    precision: 12,
                    scale: 2,
                },
                MinMaxPredicateValue::Decimal128 {
                    value: 100,
                    precision: 12,
                    scale: 3,
                },
            ],
            ScanPredicateSource::RuntimeIn,
        )
        .expect_err("decimal scale mismatch is unsupported");

        assert!(err.contains("mixed scan predicate value families"));
    }

    #[test]
    fn discrete_set_rejects_mixed_value_families() {
        let err = ScanPredicate::discrete_set(
            "0".to_string(),
            vec![
                MinMaxPredicateValue::Int32(1),
                MinMaxPredicateValue::ByteArray(b"a".to_vec()),
            ],
            ScanPredicateSource::RuntimeIn,
        )
        .expect_err("mixed families are unsupported");

        assert!(err.contains("mixed scan predicate value families"));
    }

    #[test]
    fn range_domain_exposes_operator() {
        let predicate = ScanPredicate::from_min_max_predicate(
            MinMaxPredicate::Lt {
                column: "2".to_string(),
                value: MinMaxPredicateValue::Int64(9),
            },
            ScanPredicateSource::RuntimeMinMax,
        );

        assert_eq!(predicate.range_op(), Some(MinMaxPredicateOp::Lt));
    }
}
