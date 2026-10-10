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

//! The original months_diff tail and years_diff anniversary formulas.
//! This finite numeric author does not introduce end-of-month normalization.

use chrono::{Datelike, NaiveDate, NaiveDateTime, Timelike};

/// Original DATEDIFF/DAYS_DIFF row computation after both date extractions.
pub fn date_difference_days(left: Option<NaiveDate>, right: Option<NaiveDate>) -> Option<i64> {
    match (left, right) {
        (Some(left), Some(right)) => Some((left - right).num_days()),
        _ => None,
    }
}

fn datetime_tail_value(dt: NaiveDateTime) -> i64 {
    dt.day() as i64 * 1_000_000_000_000
        + dt.hour() as i64 * 10_000_000_000
        + dt.minute() as i64 * 100_000_000
        + dt.second() as i64 * 1_000_000
        + (dt.nanosecond() / 1_000) as i64
}

pub fn months_diff_starrocks(lhs: NaiveDateTime, rhs: NaiveDateTime) -> i64 {
    let mut month =
        (lhs.year() - rhs.year()) as i64 * 12 + (lhs.month() as i64 - rhs.month() as i64);
    let lhs_tail = datetime_tail_value(lhs);
    let rhs_tail = datetime_tail_value(rhs);

    if month >= 0 {
        if lhs_tail < rhs_tail {
            month -= 1;
        }
    } else if lhs_tail > rhs_tail {
        month += 1;
    }

    month
}

/// Original chronological anniversary calculation, including nanosecond time.
pub fn years_diff_starrocks(lhs: NaiveDateTime, rhs: NaiveDateTime) -> i64 {
    let (sign, start, end) = if lhs >= rhs {
        (1_i64, rhs, lhs)
    } else {
        (-1_i64, lhs, rhs)
    };
    let mut years = (end.year() - start.year()) as i64;
    let end_tuple = (end.month(), end.day(), end.time());
    let start_tuple = (start.month(), start.day(), start.time());
    if end_tuple < start_tuple {
        years -= 1;
    }
    years * sign
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::NaiveDate;

    fn datetime(y: i32, m: u32, d: u32, nanos: u32) -> NaiveDateTime {
        NaiveDate::from_ymd_opt(y, m, d)
            .unwrap()
            .and_hms_nano_opt(0, 0, 0, nanos)
            .unwrap()
    }

    #[test]
    fn calendar_period_numeric_original_zero_month_asymmetry_and_precision_are_not_normalized() {
        let a = datetime(2024, 1, 15, 0);
        let b = datetime(2024, 1, 16, 0);
        assert_eq!(months_diff_starrocks(a, b), -1);
        assert_eq!(months_diff_starrocks(b, a), 0);
        assert_eq!(
            months_diff_starrocks(datetime(2024, 2, 29, 0), datetime(2024, 1, 31, 0)),
            0
        );
        assert_eq!(
            months_diff_starrocks(datetime(2024, 1, 31, 0), datetime(2024, 2, 29, 0)),
            0
        );
        assert_eq!(
            months_diff_starrocks(datetime(2024, 2, 15, 1), datetime(2024, 1, 15, 2)),
            1
        );
        assert_eq!(
            years_diff_starrocks(datetime(2025, 1, 1, 1), datetime(2024, 1, 1, 2)),
            0
        );
        assert_eq!(
            years_diff_starrocks(datetime(2025, 1, 1, 2), datetime(2024, 1, 1, 1)),
            1
        );
        assert_eq!(
            years_diff_starrocks(datetime(2025, 2, 28, 0), datetime(2024, 2, 29, 0)),
            0
        );
        assert_eq!(
            years_diff_starrocks(datetime(2025, 3, 1, 0), datetime(2024, 2, 29, 0)),
            1
        );
    }

    #[test]
    fn calendar_period_numeric_full_chrono_bounds_and_negative_years_stay_in_i64() {
        let min = NaiveDate::MIN.and_hms_opt(0, 0, 0).unwrap();
        let max = NaiveDate::MAX.and_hms_opt(0, 0, 0).unwrap();
        assert_eq!(months_diff_starrocks(max, min), 6291431);
        assert_eq!(months_diff_starrocks(min, max), -6291431);
        assert_eq!(years_diff_starrocks(max, min), 524285);
        assert_eq!(years_diff_starrocks(min, max), -524285);
        assert_eq!(
            years_diff_starrocks(datetime(0, 1, 1, 0), datetime(-1, 1, 1, 0)),
            1
        );
        assert_eq!(
            months_diff_starrocks(datetime(0, 1, 1, 0), datetime(-1, 1, 1, 0)),
            12
        );
        assert_eq!(months_diff_starrocks(min, min), 0);
        assert_eq!(years_diff_starrocks(max, max), 0);
    }
}
