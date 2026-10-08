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
//! Original numeric calendar algorithms, shared by pure and legacy owners.
//! Compact calendar literals and year/day ordinals are not epoch conversions.

use chrono::{Days, NaiveDate, NaiveDateTime, NaiveTime};

/// Original MAKEDATE range checks: ordinal overflow never rolls into another year.
pub fn makedate_from_year_day(year: i64, day: i64) -> Option<NaiveDate> {
    let year = i32::try_from(year).ok()?;
    let day = i32::try_from(day).ok()?;
    if day <= 0 || !(0..=9999).contains(&year) {
        return None;
    }
    let base = NaiveDate::from_ymd_opt(year, 1, 1);
    let leap = NaiveDate::from_ymd_opt(year, 2, 29).is_some();
    let max_day = if leap { 366 } else { 365 };
    if day > max_day {
        return None;
    }
    base.and_then(|date| date.checked_add_days(Days::new((day - 1) as u64)))
}

pub fn standardize_numeric_datetime_literal(value: i64) -> Option<i64> {
    const YY_PART_YEAR: i64 = 70;
    if value <= 0 {
        return None;
    }
    if value >= 10000101000000 {
        if value > 99999999999999 {
            return None;
        }
        return Some(value);
    }
    if value < 101 {
        return None;
    }
    if value <= (YY_PART_YEAR - 1) * 10000 + 1231 {
        return Some((value + 20000000) * 1000000);
    }
    if value < YY_PART_YEAR * 10000 + 101 {
        return None;
    }
    if value <= 991231 {
        return Some((value + 19000000) * 1000000);
    }
    if value < 10000101 {
        return None;
    }
    if value <= 99991231 {
        return Some(value * 1000000);
    }
    if value < 101000000 {
        return None;
    }
    if value <= (YY_PART_YEAR - 1) * 10000000000 + 1231235959 {
        return Some(value + 20000000000000);
    }
    if value < YY_PART_YEAR * 10000000000 + 101000000 {
        return None;
    }
    if value <= 991231235959 {
        return Some(value + 19000000000000);
    }
    Some(value)
}

pub fn numeric_datetime_literal_to_naive(value: i64) -> Option<NaiveDateTime> {
    let standardized = standardize_numeric_datetime_literal(value)?;
    let date_part = standardized / 1_000_000;
    let time_part = standardized % 1_000_000;

    let year = (date_part / 10_000) as i32;
    let month = ((date_part / 100) % 100) as u32;
    let day = (date_part % 100) as u32;
    let hour = (time_part / 10_000) as u32;
    let minute = ((time_part / 100) % 100) as u32;
    let second = (time_part % 100) as u32;

    let date = NaiveDate::from_ymd_opt(year, month, day)?;
    let time = NaiveTime::from_hms_opt(hour, minute, second)?;
    Some(date.and_time(time))
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{Datelike, Timelike};

    #[test]
    fn numeric_calendar_literal_preserves_pivot_ranges_and_calendar_components() {
        for (source, year, month, day, hour, minute, second) in [
            (101, 2000, 1, 1, 0, 0, 0),
            (690101, 2069, 1, 1, 0, 0, 0),
            (691231, 2069, 12, 31, 0, 0, 0),
            (700101, 1970, 1, 1, 0, 0, 0),
            (991231, 1999, 12, 31, 0, 0, 0),
            (10000101, 1000, 1, 1, 0, 0, 0),
            (99991231, 9999, 12, 31, 0, 0, 0),
            (101000000, 2000, 1, 1, 0, 0, 0),
            (690101123456, 2069, 1, 1, 12, 34, 56),
            (700101123456, 1970, 1, 1, 12, 34, 56),
            (20240229123456, 2024, 2, 29, 12, 34, 56),
            (99991231235959, 9999, 12, 31, 23, 59, 59),
        ] {
            let date = numeric_datetime_literal_to_naive(source).unwrap();
            assert_eq!(
                (
                    date.year(),
                    date.month(),
                    date.day(),
                    date.hour(),
                    date.minute(),
                    date.second()
                ),
                (year, month, day, hour, minute, second)
            );
        }
        assert_eq!(
            standardize_numeric_datetime_literal(690101),
            Some(20690101000000)
        );
        assert_eq!(
            standardize_numeric_datetime_literal(700101),
            Some(19700101000000)
        );
    }

    #[test]
    fn numeric_calendar_literal_keeps_invalid_none_and_never_uses_epoch_interpretation() {
        for source in [
            i64::MIN,
            -1,
            0,
            1,
            100,
            691232,
            700100,
            991232,
            10000100,
            20230229,
            20241301,
            20240229240000,
            20240229126000,
            20240229123460,
            99999999999999,
            100000000000000,
            i64::MAX,
        ] {
            assert_eq!(numeric_datetime_literal_to_naive(source), None, "{source}");
        }
        assert_eq!(
            numeric_datetime_literal_to_naive(19700101).unwrap().year(),
            1970
        );
    }
}
