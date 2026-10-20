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
//! Array generation owns its control values, range arithmetic and List32 limit.
//! These facts do not admit memory or decide retry/SQL policy.
use arrow_schema::ArrowError;
use chrono::{Datelike, Duration, NaiveDate, NaiveDateTime};
use novarocks_failure_contract::{
    BoundedDetail, FailureClass, FailureFact, FailureProjection, FailureReason,
    RepresentationBoundary, RepresentationFacts, RepresentationLayout, domain,
};
use std::{error::Error, fmt, num::NonZeroU16};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ArrayGenerateUnit {
    Year,
    Quarter,
    Month,
    Week,
    Day,
    Hour,
    Minute,
    Second,
    Millisecond,
    Microsecond,
}
impl ArrayGenerateUnit {
    pub fn parse(raw: &str) -> Result<Self, ArrayGenerateFailure> {
        let raw = raw.trim();
        for unit in [
            Self::Year,
            Self::Quarter,
            Self::Month,
            Self::Week,
            Self::Day,
            Self::Hour,
            Self::Minute,
            Self::Second,
            Self::Millisecond,
            Self::Microsecond,
        ] {
            if raw.eq_ignore_ascii_case(unit.name()) {
                return Ok(unit);
            }
        }
        let mut bounded = BoundedDetail::truncating(raw).as_str().to_owned();
        bounded.make_ascii_lowercase();
        Err(ArrayGenerateFailure::UnsupportedUnit(
            BoundedDetail::truncating(&bounded),
        ))
    }
    pub const fn name(self) -> &'static str {
        match self {
            Self::Year => "year",
            Self::Quarter => "quarter",
            Self::Month => "month",
            Self::Week => "week",
            Self::Day => "day",
            Self::Hour => "hour",
            Self::Minute => "minute",
            Self::Second => "second",
            Self::Millisecond => "millisecond",
            Self::Microsecond => "microsecond",
        }
    }
    pub const fn is_subday(self) -> bool {
        matches!(
            self,
            Self::Hour | Self::Minute | Self::Second | Self::Millisecond | Self::Microsecond
        )
    }
    pub fn advance(
        self,
        dt: NaiveDateTime,
        step: i64,
    ) -> Result<NaiveDateTime, ArrayGenerateFailure> {
        let next = match self {
            Self::Year | Self::Quarter | Self::Month => i32::try_from(step)
                .ok()
                .and_then(|step| {
                    step.checked_mul(match self {
                        Self::Year => 12,
                        Self::Quarter => 3,
                        _ => 1,
                    })
                })
                .and_then(|months| add_months(dt, months)),
            Self::Week => Duration::try_weeks(step).and_then(|d| dt.checked_add_signed(d)),
            Self::Day => Duration::try_days(step).and_then(|d| dt.checked_add_signed(d)),
            Self::Hour => Duration::try_hours(step).and_then(|d| dt.checked_add_signed(d)),
            Self::Minute => Duration::try_minutes(step).and_then(|d| dt.checked_add_signed(d)),
            Self::Second => Duration::try_seconds(step).and_then(|d| dt.checked_add_signed(d)),
            Self::Millisecond => {
                Duration::try_milliseconds(step).and_then(|d| dt.checked_add_signed(d))
            }
            Self::Microsecond => dt.checked_add_signed(Duration::microseconds(step)),
        };
        next.ok_or(ArrayGenerateFailure::TemporalStep(self))
    }
}
fn add_months(dt: NaiveDateTime, months: i32) -> Option<NaiveDateTime> {
    let offset = (dt.month() as i32 - 1).checked_add(months)?;
    let year = dt.year().checked_add(offset.div_euclid(12))?;
    let month = (offset.rem_euclid(12) + 1) as u32;
    let last_day = last_day_of_month(year, month)?;
    NaiveDate::from_ymd_opt(year, month, dt.day().min(last_day))
        .map(|date| date.and_time(dt.time()))
}
fn last_day_of_month(year: i32, month: u32) -> Option<u32> {
    (28..=31)
        .rev()
        .find(|day| NaiveDate::from_ymd_opt(year, month, *day).is_some())
}
/// Inclusive cardinality in a wide value domain, before producing any elements.
/// Zero/direction mismatch preserve the scalar array function's empty-row policy.
pub fn integer_range_count(start: i64, stop: i64, step: i64) -> u128 {
    if step == 0 || (step > 0 && start > stop) || (step < 0 && start < stop) {
        return 0;
    }
    (i128::from(stop) - i128::from(start)).unsigned_abs() / u128::from(step.unsigned_abs()) + 1
}
pub fn check_list_extent(
    required: u128,
    row_values: u128,
    prefix: usize,
) -> Result<(), ArrayGenerateFailure> {
    if required > i32::MAX as u128 {
        Err(ArrayGenerateFailure::ListExtent {
            required,
            row_values,
            prefix,
        })
    } else {
        Ok(())
    }
}
#[derive(Debug)]
pub enum ArrayGenerateFailure {
    Contract(BoundedDetail),
    NullStep,
    NegativeStep,
    UnsupportedUnit(BoundedDetail),
    TemporalStep(ArrayGenerateUnit),
    ListExtent {
        required: u128,
        row_values: u128,
        prefix: usize,
    },
    Arrow(ArrowError),
}
impl ArrayGenerateFailure {
    pub fn contract(detail: impl fmt::Display) -> Self {
        Self::Contract(BoundedDetail::truncating_display(detail))
    }
}
impl fmt::Display for ArrayGenerateFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Contract(detail) => f.write_str(detail.as_str()),
            Self::NullStep => {
                f.write_str("array_generate requires step parameter must be a constant integer")
            }
            Self::NegativeStep => {
                f.write_str("array_generate requires step parameter must be non-negative.")
            }
            Self::UnsupportedUnit(unit) => {
                write!(f, "array_generate unsupported time unit: {}", unit.as_str())
            }
            Self::TemporalStep(unit) => write!(
                f,
                "array_generate temporal step is out of range for {}",
                unit.name()
            ),
            Self::ListExtent { .. } => f.write_str("array_generate offset overflow"),
            Self::Arrow(source) => write!(f, "array_generate array conversion failed: {source}"),
        }
    }
}
impl Error for ArrayGenerateFailure {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Arrow(source) => Some(source),
            _ => None,
        }
    }
}
fn fact(class: FailureClass, code: u16) -> FailureFact {
    FailureFact::new(
        class,
        FailureReason::new(domain::KERNEL, NonZeroU16::new(code).unwrap()),
    )
}
impl FailureProjection for ArrayGenerateFailure {
    fn failure_fact(&self) -> FailureFact {
        match self {
            Self::NullStep | Self::NegativeStep | Self::UnsupportedUnit(_) => {
                crate::DataFailureReason::InvalidValue.failure_fact()
            }
            Self::TemporalStep(_) | Self::Arrow(ArrowError::ArithmeticOverflow(_)) => {
                crate::DataFailureReason::NumericOutOfRange.failure_fact()
            }
            Self::Contract(_) => fact(FailureClass::Invariant, 286),
            Self::ListExtent {
                required,
                row_values,
                prefix,
            } => {
                let minimum = *row_values > i32::MAX as u128;
                fact(FailureClass::Representation, 287).with_representation(RepresentationFacts {
                    boundary: if minimum {
                        RepresentationBoundary::MinimumUnit
                    } else {
                        RepresentationBoundary::SingleOutput
                    },
                    layout: RepresentationLayout {
                        domain: domain::KERNEL,
                        code: NonZeroU16::new(58).unwrap(),
                    },
                    required: u64::try_from(if minimum { *row_values } else { *required }).ok(),
                    extent_limit: Some(i32::MAX as u64),
                    representable_prefix: u64::try_from(*prefix).ok(),
                })
            }
            Self::Arrow(ArrowError::MemoryError(_)) => fact(FailureClass::Resource, 288),
            Self::Arrow(ArrowError::OffsetOverflowError(_)) => {
                fact(FailureClass::Representation, 289).with_representation(RepresentationFacts {
                    boundary: RepresentationBoundary::SingleOutput,
                    layout: RepresentationLayout {
                        domain: domain::KERNEL,
                        code: NonZeroU16::new(58).unwrap(),
                    },
                    required: None,
                    extent_limit: None,
                    representable_prefix: None,
                })
            }
            Self::Arrow(ArrowError::IoError(..)) => fact(FailureClass::Unexpected, 290),
            // Casts at this boundary are the admitted numeric -> Int64, UTF8
            // unit identity and timestamp-unit output conversions. Safe casts
            // keep bad values NULL; unsupported carriers are a contract error.
            Self::Arrow(_) => fact(FailureClass::Invariant, 291),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn array_generate_inclusive_counts_cover_i64_domain_without_expansion() {
        for (start, stop, step, count) in [
            (1, 5, 2, 3),
            (5, 1, -2, 3),
            (5, 1, 2, 0),
            (1, 5, -2, 0),
            (1, 1, 0, 0),
            (i64::MIN, i64::MAX, 1, 1u128 << 64),
            (i64::MAX, i64::MIN, i64::MIN, 2),
        ] {
            assert_eq!(integer_range_count(start, stop, step), count);
        }
        let required = integer_range_count(0, i32::MAX as i64, 1);
        let error = check_list_extent(required, required, 2).unwrap_err();
        let facts = error.failure_fact().representation().unwrap();
        assert_eq!(facts.boundary, RepresentationBoundary::MinimumUnit);
        assert_eq!(facts.required, Some(i32::MAX as u64 + 1));
        assert_eq!(facts.extent_limit, Some(i32::MAX as u64));
        assert_eq!(facts.representable_prefix, Some(2));
        let error = check_list_extent(i32::MAX as u128 + 1, 1, 4).unwrap_err();
        let facts = error.failure_fact().representation().unwrap();
        assert_eq!(facts.boundary, RepresentationBoundary::SingleOutput);
        assert_eq!(facts.representable_prefix, Some(4));
    }
    #[test]
    fn array_generate_real_temporal_limits_and_bounded_unit_cause() {
        let max = NaiveDateTime::MAX;
        let error = ArrayGenerateUnit::Second.advance(max, 1).unwrap_err();
        assert_eq!(
            error.failure_fact(),
            crate::DataFailureReason::NumericOutOfRange.failure_fact()
        );
        let jan = NaiveDate::from_ymd_opt(2024, 1, 31)
            .unwrap()
            .and_hms_opt(12, 34, 56)
            .unwrap();
        assert_eq!(
            ArrayGenerateUnit::Month.advance(jan, 1).unwrap(),
            NaiveDate::from_ymd_opt(2024, 2, 29)
                .unwrap()
                .and_hms_opt(12, 34, 56)
                .unwrap()
        );
        assert_eq!(
            ArrayGenerateUnit::parse("  MiCrOsEcOnD ").unwrap(),
            ArrayGenerateUnit::Microsecond
        );
        let error = ArrayGenerateUnit::parse(&"大UNIT".repeat(100_000)).unwrap_err();
        assert_eq!(error.failure_fact().class(), FailureClass::Data);
        let ArrayGenerateFailure::UnsupportedUnit(detail) = error else {
            panic!("actual unit author")
        };
        assert!(detail.as_str().len() <= 512);
    }
    #[test]
    fn array_generate_checked_helpers_cover_extreme_units_and_calendar_limits() {
        let start = NaiveDate::from_ymd_opt(2025, 10, 1)
            .unwrap()
            .and_hms_opt(14, 28, 31)
            .unwrap();
        for unit in [
            "year",
            "quarter",
            "month",
            "week",
            "day",
            "hour",
            "minute",
            "second",
            "millisecond",
            "microsecond",
        ] {
            for step in [i64::MAX, i64::MIN] {
                let error = ArrayGenerateUnit::parse(unit)
                    .unwrap()
                    .advance(start, step)
                    .unwrap_err();
                assert!(
                    error.to_string().contains("temporal step is out of range"),
                    "{unit}: {error}"
                );
            }
        }
        assert!(add_months(start, i32::MAX).is_none());
        assert!(add_months(start, i32::MIN).is_none());
        assert_eq!(last_day_of_month(NaiveDate::MAX.year(), 12), Some(31));
        assert_eq!(last_day_of_month(NaiveDate::MIN.year(), 1), Some(31));
        assert_eq!(last_day_of_month(i32::MAX, 12), None);
        assert_eq!(last_day_of_month(2025, 13), None);
        let max_november = NaiveDate::from_ymd_opt(NaiveDate::MAX.year(), 11, 30)
            .unwrap()
            .and_hms_opt(0, 0, 0)
            .unwrap();
        let max_december = add_months(max_november, 1).unwrap();
        assert_eq!(max_december.date().month(), 12);
        assert_eq!(max_december.date().day(), 30);
        assert!(
            ArrayGenerateUnit::Month
                .advance(max_december, 1)
                .unwrap_err()
                .to_string()
                .contains("out of range")
        );
        assert!(
            ArrayGenerateUnit::parse("unknown")
                .unwrap_err()
                .to_string()
                .contains("unsupported time unit")
        );
    }
}
