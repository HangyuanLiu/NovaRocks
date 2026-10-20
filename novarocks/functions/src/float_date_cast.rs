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

pub fn date_literal_to_date32(value: i64) -> Result<i32, FloatDateFailure> {
    let standardized = standardize_date_literal(value)
        .ok_or_else(|| FloatDateFailure(RejectedDateValue::Literal(value)))?;
    let date_part = standardized / 1_000_000;
    let time_part = standardized % 1_000_000;
    let year = (date_part / 10000) as i32;
    let month = ((date_part / 100) % 100) as u32;
    let day = (date_part % 100) as u32;
    let hour = (time_part / 10000) as i32;
    let minute = ((time_part / 100) % 100) as i32;
    let second = (time_part % 100) as i32;
    if hour > 23 || minute > 59 || second > 59 {
        return Err(FloatDateFailure(RejectedDateValue::Literal(value)));
    }
    let date = NaiveDate::from_ymd_opt(year, month, day)
        .ok_or_else(|| FloatDateFailure(RejectedDateValue::Literal(value)))?;
    Ok(date.num_days_from_ce() - UNIX_EPOCH_DAY_OFFSET)
}

pub fn value_f64(value: f64) -> Result<i32, FloatDateFailure> {
    if !value.is_finite() {
        return Err(FloatDateFailure(RejectedDateValue::NonFinite(value)));
    }
    let literal = value as i64;
    date_literal_to_date32(literal)
}
pub fn value_f32(value: f32) -> Result<i32, FloatDateFailure> {
    value_f64(value as f64)
}

/// A genuine value failure must use a row or invocation Data channel.
/// ```compile_fail
/// let value = novarocks_functions::float_date_cast::value_f64(f64::NAN).unwrap_err();
/// let _ = novarocks_functions::KernelFailure::from_domain(value);
/// ```
#[derive(Debug)]
pub struct FloatDateFailure(RejectedDateValue);
#[derive(Debug)]
enum RejectedDateValue {
    NonFinite(f64),
    Literal(i64),
}
impl std::fmt::Display for FloatDateFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.0 {
            RejectedDateValue::NonFinite(value) => write!(f, "invalid date literal {value}"),
            RejectedDateValue::Literal(value) => write!(f, "invalid date literal {value}"),
        }
    }
}
impl std::error::Error for FloatDateFailure {}
impl novarocks_failure_contract::FailureProjection for FloatDateFailure {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        crate::DataFailureReason::InvalidValue.failure_fact()
    }
}
// This value-only failure has no outer-kernel marker.

#[cfg(test)]
mod rejected_value_tests {
    use super::*;
    use novarocks_failure_contract::FailureProjection;
    #[test]
    fn rejected_integral_literal_retains_all_bits_and_nonfinite_stays_floating() {
        for value in [i64::MIN, i64::MAX] {
            let error = date_literal_to_date32(value).unwrap_err();
            assert!(matches!(error.0, RejectedDateValue::Literal(actual) if actual == value));
            assert_eq!(error.to_string(), format!("invalid date literal {value}"));
            assert_eq!(
                error.failure_fact(),
                crate::DataFailureReason::InvalidValue.failure_fact()
            );
        }
        for value in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
            let error = value_f64(value).unwrap_err();
            assert!(
                matches!(error.0, RejectedDateValue::NonFinite(actual) if actual.to_bits() == value.to_bits())
            );
            assert_eq!(error.to_string(), format!("invalid date literal {value}"));
        }
        assert_eq!(value_f64(19700101.0).unwrap(), 0);
    }
}
