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

//! Original actual Types Float64/Float32 -> Decimal128 value and array author.
//! Preserve relaxed precision, signed-abs and i8-negation bugs; no cast policy is inferred here.
use arrow_array::{ArrayRef, Decimal128Array};
use std::fmt;
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FactorFailure {
    Scale(i8),
    Precision(u8),
}
impl fmt::Display for FactorFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Scale(scale) => write!(
                f,
                "decimal scale overflow while casting float to DECIMAL: scale={scale}"
            ),
            Self::Precision(precision) => write!(
                f,
                "decimal precision overflow while casting float to DECIMAL: precision={precision}"
            ),
        }
    }
}
pub enum ExprValue {
    Value(i128),
    Null,
    Overflow,
}
pub struct Factors {
    scale_factor_f64: f64,
    abs_limit: i128,
}
impl Factors {
    /// Original static factor check precedes every NULL/empty row traversal.
    pub fn try_new(precision: u8, scale: i8) -> Result<Self, FactorFailure> {
        let scale_factor_f64 = if scale >= 0 {
            let factor = crate::legacy_decimal::checked_pow10_i128((scale as u32) as usize)
                .ok_or(FactorFailure::Scale(scale))?;
            factor as f64
        } else {
            // Original unchecked i8 negation remains a panic in checked builds.
            let factor = crate::legacy_decimal::checked_pow10_i128(((-scale) as u32) as usize)
                .ok_or(FactorFailure::Scale(scale))?;
            1.0 / (factor as f64)
        };
        let effective_precision = if precision <= 18 { 18 } else { precision };
        let abs_limit =
            crate::legacy_decimal::checked_pow10_i128((effective_precision as u32) as usize)
                .ok_or(FactorFailure::Precision(effective_precision))?;
        Ok(Self {
            scale_factor_f64,
            abs_limit,
        })
    }
    /// Original Expr checked-numeric projection after the relaxed Types value.
    /// Project and Types do not use this original outer precision/policy stage.
    pub fn expr_value(
        &self,
        value: f64,
        precision: u8,
        policy: novarocks_type_contract::DecimalOverflowPolicy,
    ) -> ExprValue {
        let relaxed = self.value(value);
        let declared = crate::decimal128_rescale::DeclaredPrecision::try_new(precision)
            .expect("already validated exact Decimal128 precision");
        let checked = relaxed.filter(|value| declared.contains(*value));
        if let Some(value) = checked {
            return ExprValue::Value(value);
        }
        if value.is_finite()
            && policy == novarocks_type_contract::DecimalOverflowPolicy::ReportError
        {
            ExprValue::Overflow
        } else {
            ExprValue::Null
        }
    }
    /// Sole original per-value arithmetic, including the original MIN abs panic.
    pub fn value(&self, v: f64) -> Option<i128> {
        if !v.is_finite() {
            return None;
        }
        // Match StarRocks DecimalV3Cast::from_float: nearest integer with half-up behavior.
        let delta = if v >= 0.0 { 0.5 } else { -0.5 };
        let scaled = v * self.scale_factor_f64 + delta;
        if !scaled.is_finite() {
            return None;
        }
        let unscaled_f = scaled.trunc();
        if unscaled_f > (i128::MAX as f64) || unscaled_f < (i128::MIN as f64) {
            return None;
        }
        let unscaled = unscaled_f as i128;
        if unscaled.abs() >= self.abs_limit {
            return None;
        }
        Some(unscaled)
    }
}
pub fn evaluate_legacy(
    len: usize,
    mut value_at: impl FnMut(usize) -> Option<f64>,
    precision: u8,
    scale: i8,
) -> Result<ArrayRef, String> {
    let factors = Factors::try_new(precision, scale).map_err(|e| e.to_string())?;
    let mut values: Vec<Option<i128>> = Vec::with_capacity(len);
    for row in 0..len {
        let Some(v) = value_at(row) else {
            values.push(None);
            continue;
        };
        values.push(factors.value(v));
    }
    let wide = Decimal128Array::from(values)
        .with_precision_and_scale(38, scale)
        .map_err(|e| e.to_string())?;
    crate::decimal128_rescale::retag_legacy(&wide, precision, scale)
}
