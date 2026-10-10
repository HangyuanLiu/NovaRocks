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

//! Original DATE-to-float scalar computation, with the v1 raw arithmetic intact.
//! Callers own NULL/address validation and original full/bounded error projection.
use chrono::{Datelike, NaiveDate};
const UNIX_EPOCH_DAY_OFFSET: i32 = 719163;

pub fn date32_to_date_literal(days: i32) -> Result<i32, String> {
    let date = NaiveDate::from_num_days_from_ce_opt(UNIX_EPOCH_DAY_OFFSET + days)
        .ok_or_else(|| format!("invalid Date32 value {days}"))?;
    Ok(date.year() * 10000 + date.month() as i32 * 100 + date.day() as i32)
}

pub fn value_f32(days: i32) -> Result<f32, String> {
    Ok(date32_to_date_literal(days)? as f32)
}
pub fn value_f64(days: i32) -> Result<f64, String> {
    Ok(date32_to_date_literal(days)? as f64)
}
