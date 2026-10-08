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
use arrow::array::ArrayRef;
use arrow::datatypes::DataType;
use chrono::{Datelike, NaiveDate, NaiveDateTime, NaiveTime, TimeZone, Timelike, Utc};

pub use novarocks_functions::calendar_julian::{BC_EPOCH_JULIAN, julian_from_date};
pub use novarocks_functions::datetime_value::{
    UNIX_EPOCH_DAY_OFFSET, date32_to_naive, naive_to_timestamp_micros, parse_date, parse_datetime,
    timestamp_to_naive,
};

pub fn naive_to_date32(date: NaiveDate) -> i32 {
    date.num_days_from_ce() - UNIX_EPOCH_DAY_OFFSET
}

pub use novarocks_functions::builtin::calendar_time_text_shared::parse_time;

pub fn extract_i64_array(array: &ArrayRef, func_name: &str) -> Result<Vec<Option<i64>>, String> {
    novarocks_functions::builtin::calendar_add_interval::legacy_extract_calendar_intervals(
        array, func_name,
    )
}

pub fn extract_datetime_array(array: &ArrayRef) -> Result<Vec<Option<NaiveDateTime>>, String> {
    novarocks_functions::builtin::calendar_extended_shared::legacy_extract_datetimes(array)
}

pub fn extract_date_array(array: &ArrayRef) -> Result<Vec<Option<NaiveDate>>, String> {
    novarocks_functions::builtin::calendar_extended_shared::legacy_extract_dates(array)
}

pub fn datetime_from_local_now() -> NaiveDateTime {
    chrono::Local::now().naive_local()
}

pub fn datetime_from_utc_now() -> NaiveDateTime {
    chrono::Utc::now().naive_utc()
}

pub fn time_from_local_now() -> NaiveTime {
    chrono::Local::now().naive_local().time()
}

pub fn time_from_utc_now() -> NaiveTime {
    chrono::Utc::now().naive_utc().time()
}

pub fn date_from_julian(julian: i32) -> Option<NaiveDate> {
    novarocks_functions::builtin::calendar_extended_shared::calendar_date_from_julian(julian)
}

pub use novarocks_functions::builtin::calendar_time_text_shared::time_to_seconds;

pub fn seconds_to_time(seconds: i64) -> NaiveTime {
    let mut secs = seconds % 86400;
    if secs < 0 {
        secs += 86400;
    }
    let h = (secs / 3600) as u32;
    let m = ((secs % 3600) / 60) as u32;
    let s = (secs % 60) as u32;
    NaiveTime::from_hms_opt(h, m, s).unwrap_or_else(|| NaiveTime::from_hms_opt(0, 0, 0).unwrap())
}

pub fn to_timestamp_value(dt: NaiveDateTime, output_type: &DataType) -> Result<i64, String> {
    novarocks_functions::builtin::calendar_extended_shared::legacy_to_timestamp_value(
        dt,
        output_type,
    )
}

pub fn format_datetime_with_pattern(dt: NaiveDateTime, pattern: &str) -> String {
    dt.format(pattern).to_string()
}

pub fn parse_datetime_with_pattern(s: &str, pattern: &str) -> Option<NaiveDateTime> {
    NaiveDateTime::parse_from_str(s, pattern).ok()
}

pub fn mysql_format_to_chrono(fmt: &str) -> String {
    novarocks_functions::builtin::calendar_extended_shared::legacy_mysql_format_to_chrono(fmt)
}

pub fn convert_tz_fixed(
    dt: NaiveDateTime,
    from: chrono::FixedOffset,
    to: chrono::FixedOffset,
) -> NaiveDateTime {
    let dt_from = from.from_local_datetime(&dt).unwrap();
    let utc = dt_from.with_timezone(&Utc);
    utc.with_timezone(&to).naive_local()
}

#[cfg(test)]
mod date32_checked_tests {
    use super::*;
    use std::sync::Arc;

    #[test]
    fn date32_rejects_unrepresentable_dates_without_overflowing_epoch_translation() {
        for days in [i32::MIN, i32::MAX, i32::MAX - UNIX_EPOCH_DAY_OFFSET + 1] {
            assert_eq!(date32_to_naive(days), None);
        }
        assert_eq!(date32_to_naive(0), NaiveDate::from_ymd_opt(1970, 1, 1));
        assert_eq!(date32_to_naive(-1), NaiveDate::from_ymd_opt(1969, 12, 31));
        assert_eq!(date32_to_naive(1), NaiveDate::from_ymd_opt(1970, 1, 2));
        for date in [NaiveDate::MIN, NaiveDate::MAX] {
            let days = naive_to_date32(date);
            assert_eq!(date32_to_naive(days), Some(date));
            let outside = if date == NaiveDate::MIN {
                days - 1
            } else {
                days + 1
            };
            assert_eq!(date32_to_naive(outside), None);
        }
    }

    #[test]
    fn datetime_extraction_keeps_nulls_for_invalid_date32_and_valid_epoch_rows() {
        use arrow::array::Date32Array;
        let input: ArrayRef = Arc::new(Date32Array::from(vec![
            Some(i32::MAX),
            Some(0),
            None,
            Some(i32::MIN),
            Some(-1),
        ]));
        let epoch = NaiveDate::from_ymd_opt(1970, 1, 1)
            .unwrap()
            .and_hms_opt(0, 0, 0)
            .unwrap();
        let before = NaiveDate::from_ymd_opt(1969, 12, 31)
            .unwrap()
            .and_hms_opt(0, 0, 0)
            .unwrap();
        assert_eq!(
            extract_datetime_array(&input).unwrap(),
            [None, Some(epoch), None, None, Some(before)]
        );
        assert_eq!(
            extract_datetime_array(&input.slice(1, 3)).unwrap(),
            [Some(epoch), None, None]
        );
    }
}
