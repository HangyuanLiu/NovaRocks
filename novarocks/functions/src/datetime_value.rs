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

//! Pure date and timestamp value conversions shared by CPU function owners.
//! The accepted formats and invalid-value `None` behavior are retained from
//! the existing execution date helpers. Parsing library work is not a resource grant.

use arrow_schema::TimeUnit;
use chrono::{DateTime, NaiveDate, NaiveDateTime, Timelike, Utc};

pub const UNIX_EPOCH_DAY_OFFSET: i32 = 719163; // 1970-01-01 in Julian days

pub fn date32_to_naive(days: i32) -> Option<NaiveDate> {
    NaiveDate::from_num_days_from_ce_opt(UNIX_EPOCH_DAY_OFFSET.checked_add(days)?)
}

pub fn timestamp_to_naive(unit: &TimeUnit, value: i64) -> Option<NaiveDateTime> {
    match unit {
        TimeUnit::Second => DateTime::<Utc>::from_timestamp(value, 0).map(|dt| dt.naive_utc()),
        TimeUnit::Millisecond => {
            let secs = value.div_euclid(1_000);
            let nanos = (value.rem_euclid(1_000) as u32) * 1_000_000;
            DateTime::<Utc>::from_timestamp(secs, nanos).map(|dt| dt.naive_utc())
        }
        TimeUnit::Microsecond => {
            let secs = value.div_euclid(1_000_000);
            let nanos = (value.rem_euclid(1_000_000) as u32) * 1_000;
            DateTime::<Utc>::from_timestamp(secs, nanos).map(|dt| dt.naive_utc())
        }
        TimeUnit::Nanosecond => {
            let secs = value.div_euclid(1_000_000_000);
            let nanos = value.rem_euclid(1_000_000_000) as u32;
            DateTime::<Utc>::from_timestamp(secs, nanos).map(|dt| dt.naive_utc())
        }
    }
}

pub fn naive_to_timestamp_micros(dt: NaiveDateTime) -> i64 {
    dt.and_utc().timestamp_micros()
}

pub fn parse_date(s: &str) -> Option<NaiveDate> {
    NaiveDate::parse_from_str(s, "%Y-%m-%d")
        .or_else(|_| NaiveDate::parse_from_str(s, "%Y%m%d"))
        .ok()
}

fn parse_datetime_flexible(raw: &str) -> Option<NaiveDateTime> {
    let text = raw.trim();
    let bytes = text.as_bytes();
    if bytes.is_empty() || !bytes[0].is_ascii_digit() {
        return None;
    }

    let mut pos = 0usize;
    while pos < bytes.len() && (bytes[pos].is_ascii_digit() || bytes[pos] == b'T') {
        pos += 1;
    }
    let compact_digits = bytes[..pos].iter().filter(|b| b.is_ascii_digit()).count();
    let is_compact = pos == bytes.len() || bytes.get(pos) == Some(&b'.');
    let mut field_len = if is_compact {
        if compact_digits == 4 || compact_digits == 8 || compact_digits >= 14 {
            4usize
        } else {
            2usize
        }
    } else {
        4usize
    };

    let mut values = [0u32; 7];
    let mut lengths = [0usize; 7];
    let mut field_idx = 0usize;
    let mut ptr = 0usize;
    while ptr < bytes.len() && bytes[ptr].is_ascii_digit() && field_idx < 7 {
        let start = ptr;
        let mut value = 0u32;
        let scan_to_delim = !is_compact && field_idx != 6;
        while ptr < bytes.len() && bytes[ptr].is_ascii_digit() && (scan_to_delim || field_len > 0) {
            value = value
                .checked_mul(10)?
                .checked_add((bytes[ptr] - b'0') as u32)?;
            ptr += 1;
            if !scan_to_delim {
                field_len -= 1;
            }
        }
        values[field_idx] = value;
        lengths[field_idx] = ptr - start;
        field_len = 2;

        if ptr == bytes.len() {
            field_idx += 1;
            break;
        }
        if field_idx == 2 && bytes[ptr] == b'T' {
            ptr += 1;
            field_idx += 1;
            continue;
        }
        if field_idx == 5 {
            if bytes[ptr] == b'.' {
                ptr += 1;
                field_len = 6;
            } else if bytes[ptr].is_ascii_digit() {
                field_idx += 1;
                break;
            }
            field_idx += 1;
            continue;
        }
        while ptr < bytes.len()
            && (bytes[ptr].is_ascii_punctuation() || bytes[ptr].is_ascii_whitespace())
        {
            ptr += 1;
        }
        field_idx += 1;
    }

    let parsed_fields = field_idx;
    if parsed_fields < 3 {
        return None;
    }

    let mut year = values[0] as i32;
    let month = values[1];
    let day = values[2];
    let hour = values[3];
    let minute = values[4];
    let second = values[5];
    let mut microsecond = values[6];

    if lengths[6] > 0 && lengths[6] < 6 {
        microsecond = microsecond.checked_mul(10u32.pow((6 - lengths[6]) as u32))?;
    }

    if lengths[0] == 2 {
        year = if year < 70 { year + 2000 } else { year + 1900 };
    }

    if !(1..=12).contains(&month)
        || day == 0
        || hour > 23
        || minute > 59
        || second > 59
        || microsecond >= 1_000_000
    {
        return None;
    }

    let date = NaiveDate::from_ymd_opt(year, month, day)?;
    date.and_hms_micro_opt(hour, minute, second, microsecond)
}

pub fn parse_datetime(s: &str) -> Option<NaiveDateTime> {
    // chrono's %S accepts 60 (leap second) and normalizes it to the next minute;
    // reject that to match StarRocks behavior (second=60 is invalid).
    let from_chrono = NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S")
        .or_else(|_| NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S%.f"))
        .or_else(|_| NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S"))
        .or_else(|_| NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S%.f"))
        .ok()
        .filter(|dt| dt.nanosecond() < 1_000_000_000);
    from_chrono.or_else(|| parse_datetime_flexible(s))
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Datelike;

    fn date(year: i32, month: u32, day: u32) -> NaiveDate {
        NaiveDate::from_ymd_opt(year, month, day).unwrap()
    }

    #[test]
    fn date32_epoch_neighbors_and_extremes_keep_checked_none() {
        assert_eq!(date32_to_naive(0), Some(date(1970, 1, 1)));
        assert_eq!(date32_to_naive(-1), Some(date(1969, 12, 31)));
        assert_eq!(date32_to_naive(1), Some(date(1970, 1, 2)));
        for days in [i32::MIN, i32::MAX, i32::MAX - UNIX_EPOCH_DAY_OFFSET + 1] {
            assert_eq!(date32_to_naive(days), None);
        }
        for boundary in [NaiveDate::MIN, NaiveDate::MAX] {
            let days = boundary.num_days_from_ce() - UNIX_EPOCH_DAY_OFFSET;
            assert_eq!(date32_to_naive(days), Some(boundary));
        }
    }

    #[test]
    fn timestamp_negative_units_use_floor_seconds_and_positive_remainders() {
        let previous = date(1969, 12, 31);
        for (unit, value, nanos) in [
            (TimeUnit::Second, -1, 0),
            (TimeUnit::Millisecond, -1, 999_000_000),
            (TimeUnit::Microsecond, -1, 999_999_000),
            (TimeUnit::Nanosecond, -1, 999_999_999),
        ] {
            assert_eq!(
                timestamp_to_naive(&unit, value),
                previous.and_hms_nano_opt(23, 59, 59, nanos)
            );
            assert_eq!(
                timestamp_to_naive(&unit, 0),
                date(1970, 1, 1).and_hms_opt(0, 0, 0)
            );
        }
        assert_eq!(timestamp_to_naive(&TimeUnit::Second, i64::MAX), None);
        assert_eq!(timestamp_to_naive(&TimeUnit::Second, i64::MIN), None);
        assert_eq!(timestamp_to_naive(&TimeUnit::Millisecond, i64::MAX), None);
        assert_eq!(timestamp_to_naive(&TimeUnit::Millisecond, i64::MIN), None);
    }

    #[test]
    fn timestamp_microsecond_projection_preserves_negative_subseconds() {
        let before = date(1969, 12, 31)
            .and_hms_micro_opt(23, 59, 59, 999_999)
            .unwrap();
        let after = date(1970, 1, 1).and_hms_micro_opt(0, 0, 0, 1).unwrap();
        assert_eq!(naive_to_timestamp_micros(before), -1);
        assert_eq!(naive_to_timestamp_micros(after), 1);
        for dt in [before, after] {
            assert_eq!(
                timestamp_to_naive(&TimeUnit::Microsecond, naive_to_timestamp_micros(dt)),
                Some(dt)
            );
        }
        assert_eq!(
            naive_to_timestamp_micros(
                date(1969, 12, 31)
                    .and_hms_nano_opt(23, 59, 59, 999_999_999)
                    .unwrap()
            ),
            -1
        );
    }

    #[test]
    fn date_parser_keeps_dashed_compact_and_calendar_rejection() {
        assert_eq!(parse_date("2024-02-29"), Some(date(2024, 2, 29)));
        assert_eq!(parse_date("20240229"), Some(date(2024, 2, 29)));
        for invalid in ["", "2023-02-29", "20240230", "2024-13-01", "x2024-01-01"] {
            assert_eq!(parse_date(invalid), None, "{invalid}");
        }
    }

    #[test]
    fn datetime_parser_keeps_flexible_fields_year_pivot_and_fraction_precision() {
        let expected = date(2024, 2, 29).and_hms_opt(12, 34, 56).unwrap();
        for text in [
            "2024-02-29 12:34:56",
            "2024-02-29T12:34:56",
            "20240229123456",
            " 2024/02/29 12:34:56 ",
        ] {
            assert_eq!(parse_datetime(text), Some(expected), "{text}");
        }
        assert_eq!(
            parse_datetime("691231235959"),
            date(2069, 12, 31).and_hms_opt(23, 59, 59)
        );
        assert_eq!(
            parse_datetime("700101000000"),
            date(1970, 1, 1).and_hms_opt(0, 0, 0)
        );
        assert_eq!(
            parse_datetime("2024-02-29T12:34:56.123456789"),
            date(2024, 2, 29).and_hms_nano_opt(12, 34, 56, 123_456_789)
        );
        assert_eq!(
            parse_datetime("20240229123456.12"),
            date(2024, 2, 29).and_hms_micro_opt(12, 34, 56, 120_000)
        );
    }

    #[test]
    fn datetime_parser_rejects_leap_seconds_and_invalid_components() {
        for invalid in [
            "",
            "not a datetime",
            "2023-02-29 12:34:56",
            "2024-01-01 24:00:00",
            "2024-01-01 12:60:00",
            "2016-12-31 23:59:60",
            "2016-12-31T23:59:60.1",
            "20161231235960",
        ] {
            assert_eq!(parse_datetime(invalid), None, "{invalid}");
        }
    }
}
