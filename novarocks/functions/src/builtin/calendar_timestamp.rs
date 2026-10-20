// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

//! Calendar value conversion has only numeric range failures. Output declarations
//! are admitted separately, so NULL-on-range callers cannot hide contract errors.
use arrow_schema::TimeUnit;
use chrono::{Datelike, NaiveDateTime};
use novarocks_failure_contract::{FailureFact, FailureProjection};
use std::{error::Error, fmt};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TimestampValueError {
    YearRange,
    PackedEncodingRange,
    NanosecondRange,
}
impl fmt::Display for TimestampValueError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::YearRange => "timestamp out of StarRocks DATETIME year range",
            Self::PackedEncodingRange => "timestamp out of StarRocks DATETIME encoding range",
            Self::NanosecondRange => "timestamp out of range",
        })
    }
}
impl Error for TimestampValueError {}
impl FailureProjection for TimestampValueError {
    fn failure_fact(&self) -> FailureFact {
        crate::DataFailureReason::NumericOutOfRange.failure_fact()
    }
}
impl TimestampValueError {
    pub fn row_error(self, ordinal: usize) -> crate::RowDataError {
        let detail = novarocks_failure_contract::BoundedDetail::truncating_display(self);
        crate::RowDataError::with_reason(
            ordinal,
            crate::DataFailureReason::NumericOutOfRange,
            detail.as_str(),
        )
    }
}
pub fn timestamp_value(dt: NaiveDateTime, unit: TimeUnit) -> Result<i64, TimestampValueError> {
    if !(0..=9999).contains(&dt.year()) {
        return Err(TimestampValueError::YearRange);
    }
    let micros = dt.and_utc().timestamp_micros();
    let julian = 2_440_588_i64
        .checked_add(micros.div_euclid(86_400_000_000))
        .ok_or(TimestampValueError::PackedEncodingRange)?;
    if julian < 0 {
        return Err(TimestampValueError::PackedEncodingRange);
    }
    let packed = ((julian as u64) << 40) | micros.rem_euclid(86_400_000_000) as u64;
    i64::try_from(packed).map_err(|_| TimestampValueError::PackedEncodingRange)?;
    match unit {
        TimeUnit::Microsecond => Ok(micros),
        TimeUnit::Millisecond => Ok(dt.and_utc().timestamp_millis()),
        TimeUnit::Second => Ok(dt.and_utc().timestamp()),
        TimeUnit::Nanosecond => dt
            .and_utc()
            .timestamp_nanos_opt()
            .ok_or(TimestampValueError::NanosecondRange),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::NaiveDate;
    use novarocks_failure_contract::FailureClass;

    // Functions owns this cheap, fixture-free range boundary check. Independent
    // epoch and Chrono/Arrow nanosecond endpoints guard unit and NULL semantics.
    #[test]
    fn calendar_timestamp_real_units_and_range_failures_are_separate_from_declarations() {
        let pre_epoch = NaiveDate::from_ymd_opt(1969, 12, 31)
            .unwrap()
            .and_hms_micro_opt(23, 59, 59, 999_999)
            .unwrap();
        for (unit, expected) in [
            (TimeUnit::Second, -1),
            (TimeUnit::Millisecond, -1),
            (TimeUnit::Microsecond, -1),
            (TimeUnit::Nanosecond, -1000),
        ] {
            assert_eq!(timestamp_value(pre_epoch, unit).unwrap(), expected);
        }
        let upper = NaiveDate::from_ymd_opt(2262, 4, 11)
            .unwrap()
            .and_hms_nano_opt(23, 47, 16, 854_775_807)
            .unwrap();
        assert_eq!(timestamp_value(upper, TimeUnit::Nanosecond), Ok(i64::MAX));
        assert_eq!(
            timestamp_value(
                upper + chrono::Duration::nanoseconds(1),
                TimeUnit::Nanosecond
            ),
            Err(TimestampValueError::NanosecondRange)
        );
        for year in [-1, 10000] {
            let dt = NaiveDate::from_ymd_opt(year, 1, 1)
                .unwrap()
                .and_hms_opt(0, 0, 0)
                .unwrap();
            let error = timestamp_value(dt, TimeUnit::Microsecond).unwrap_err();
            assert_eq!(error, TimestampValueError::YearRange);
            assert_eq!(error.failure_fact().class(), FailureClass::Data);
            assert_eq!(error.row_error(7).selected_ordinal(), 7);
            assert_eq!(
                error.row_error(7).reason(),
                crate::DataFailureReason::NumericOutOfRange
            );
        }
        for year in [0, 9999] {
            let dt = NaiveDate::from_ymd_opt(year, 1, 1)
                .unwrap()
                .and_hms_opt(0, 0, 0)
                .unwrap();
            assert!(timestamp_value(dt, TimeUnit::Microsecond).is_ok());
        }
    }
}
