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

//! DATE-to-float scalar computation with checked calendar-range conversion.
//! Callers own NULL/address validation and original full/bounded error projection.
use chrono::{Datelike, NaiveDate};
const UNIX_EPOCH_DAY_OFFSET: i32 = 719163;

pub fn date32_to_date_literal(days: i32) -> Result<i64, DateFloatFailure> {
    let date = UNIX_EPOCH_DAY_OFFSET
        .checked_add(days)
        .and_then(NaiveDate::from_num_days_from_ce_opt)
        .ok_or(DateFloatFailure { days })?;
    Ok(i64::from(date.year()) * 10000 + i64::from(date.month()) * 100 + i64::from(date.day()))
}

pub fn value_f32(days: i32) -> Result<f32, DateFloatFailure> {
    Ok(date32_to_date_literal(days)? as f32)
}
pub fn value_f64(days: i32) -> Result<f64, DateFloatFailure> {
    Ok(date32_to_date_literal(days)? as f64)
}

/// A genuine value failure must use a row or invocation Data channel.
/// ```compile_fail
/// let value = novarocks_functions::date_float_cast::value_f64(i32::MAX).unwrap_err();
/// let _ = novarocks_functions::KernelFailure::from_domain(value);
/// ```
#[derive(Debug)]
pub struct DateFloatFailure {
    days: i32,
}
impl std::fmt::Display for DateFloatFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "invalid Date32 value {}", self.days)
    }
}
impl std::error::Error for DateFloatFailure {}
impl novarocks_failure_contract::FailureProjection for DateFloatFailure {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        crate::DataFailureReason::NumericOutOfRange.failure_fact()
    }
}
// This value-only failure has no outer-kernel marker.
#[cfg(test)]
mod tests {
    use super::*;
    use novarocks_failure_contract::FailureProjection;
    #[test]
    fn date_float_calendar_range_is_checked_without_signed_add_panic() {
        assert_eq!(date32_to_date_literal(0).unwrap(), 19700101);
        let max_date = NaiveDate::MAX;
        let max_literal =
            date32_to_date_literal(max_date.num_days_from_ce() - UNIX_EPOCH_DAY_OFFSET).unwrap();
        assert_eq!(max_literal, 2_621_421_231);
        assert!(max_literal > i64::from(i32::MAX));
        for days in [i32::MIN, i32::MAX] {
            let error = date32_to_date_literal(days).unwrap_err();
            assert_eq!(error.days, days);
            assert_eq!(
                error.failure_fact(),
                crate::DataFailureReason::NumericOutOfRange.failure_fact()
            );
        }
    }
}
