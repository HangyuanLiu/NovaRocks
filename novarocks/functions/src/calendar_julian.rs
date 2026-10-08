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

//! Original forward Julian calendar arithmetic shared by pure and legacy owners.
//! Signed division intentionally retains the original negative-year behavior.

use chrono::{Datelike, NaiveDate};

pub const BC_EPOCH_JULIAN: i32 = 1721060; // from StarRocks time_types.h

pub fn julian_from_date(date: NaiveDate) -> i32 {
    // Julian day number for proleptic Gregorian calendar
    let y = date.year();
    let m = date.month() as i32;
    let d = date.day() as i32;
    let a = (14 - m) / 12;
    let y = y + 4800 - a;
    let m = m + 12 * a - 3;
    d + ((153 * m + 2) / 5) + 365 * y + y / 4 - y / 100 + y / 400 - 32045
}

/// Original TO_DAYS projection, including signed Julian arithmetic for negative years.
pub fn day_number_from_date(date: NaiveDate) -> i64 {
    (julian_from_date(date) - BC_EPOCH_JULIAN) as i64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn forward_julian_preserves_epoch_leap_and_negative_truncation_oracles() {
        for (year, month, day, julian) in [
            (1970, 1, 1, 2440588),
            (1970, 1, 2, 2440589),
            (1969, 12, 31, 2440587),
            (2024, 2, 29, 2460370),
            (0, 1, 1, 1721060),
            (-1, 11, 30, 1721028),
            (-4800, 1, 1, -32103),
            (-4801, 1, 1, -32468),
        ] {
            let date = NaiveDate::from_ymd_opt(year, month, day).unwrap();
            assert_eq!(julian_from_date(date), julian);
        }
        assert_eq!(BC_EPOCH_JULIAN, 1721060);
    }

    #[test]
    fn forward_julian_keeps_full_chrono_domain_without_output_year_gate() {
        assert_eq!(julian_from_date(NaiveDate::MIN), -94024704);
        assert_eq!(julian_from_date(NaiveDate::MAX), 97466824);
        for year in [-262143, -4801, -4800, -1, 0, 1, 9999, 262142] {
            // Calendar operations are finite for every already-checked date.
            let date = NaiveDate::from_ymd_opt(year, 12, 31).unwrap();
            let number = julian_from_date(date) - BC_EPOCH_JULIAN;
            assert!((-100_000_000..=100_000_000).contains(&number));
        }
    }
}
