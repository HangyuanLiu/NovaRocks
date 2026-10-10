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

//! Original strict float-to-DATE computation. NULL/address/error projection belongs to callers.
use crate::calendar_numeric::standardize_numeric_datetime_literal as standardize_date_literal;
use chrono::{Datelike, NaiveDate};
const UNIX_EPOCH_DAY_OFFSET: i32 = 719163;

pub fn date_literal_to_date32(value: i64) -> Result<i32, String> {
    let standardized =
        standardize_date_literal(value).ok_or_else(|| format!("invalid date literal {value}"))?;
    let date_part = standardized / 1_000_000;
    let time_part = standardized % 1_000_000;
    let year = (date_part / 10000) as i32;
    let month = ((date_part / 100) % 100) as u32;
    let day = (date_part % 100) as u32;
    let hour = (time_part / 10000) as i32;
    let minute = ((time_part / 100) % 100) as i32;
    let second = (time_part % 100) as i32;
    if hour > 23 || minute > 59 || second > 59 {
        return Err(format!("invalid date literal {value}"));
    }
    let date = NaiveDate::from_ymd_opt(year, month, day)
        .ok_or_else(|| format!("invalid date literal {value}"))?;
    Ok(date.num_days_from_ce() - UNIX_EPOCH_DAY_OFFSET)
}

pub fn value_f64(value: f64) -> Result<i32, String> {
    if !value.is_finite() {
        return Err(format!("invalid date literal {value}"));
    }
    let literal = value as i64;
    date_literal_to_date32(literal)
}
pub fn value_f32(value: f32) -> Result<i32, String> {
    value_f64(value as f64)
}
