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

//! Original Execution Decimal128 rescale, retag, precision and overflow authors.
//! The typed observer supplies host observation only; it grants no allocation.
use arrow_array::{Array, ArrayRef, Decimal128Array, Float32Array, Float64Array, make_array};
use arrow_schema::DataType;
use novarocks_type_contract::DecimalOverflowPolicy;
use std::{convert::Infallible, sync::Arc};
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DecimalRescaleObservation {
    Step,
    OpaqueBoundary,
}
#[derive(Debug)]
pub enum DecimalRescaleError<E> {
    Data(String),
    Host(E),
}
impl<E> From<String> for DecimalRescaleError<E> {
    fn from(value: String) -> Self {
        Self::Data(value)
    }
}
fn observe<E>(
    observer: &mut dyn FnMut(DecimalRescaleObservation) -> Result<(), E>,
    event: DecimalRescaleObservation,
) -> Result<(), DecimalRescaleError<E>> {
    observer(event).map_err(DecimalRescaleError::Host)
}
fn raw<T>(
    f: impl FnOnce(
        &mut dyn FnMut(DecimalRescaleObservation) -> Result<(), Infallible>,
    ) -> Result<T, DecimalRescaleError<Infallible>>,
) -> Result<T, String> {
    match f(&mut |_| Ok(())) {
        Ok(value) => Ok(value),
        Err(DecimalRescaleError::Data(message)) => Err(message),
        Err(DecimalRescaleError::Host(never)) => match never {},
    }
}
pub fn retag_legacy(array: &Decimal128Array, precision: u8, scale: i8) -> Result<ArrayRef, String> {
    raw(|observe| retag_observed(array, precision, scale, observe))
}
pub(crate) fn retag_observed<E>(
    array: &Decimal128Array,
    precision: u8,
    scale: i8,
    observer: &mut dyn FnMut(DecimalRescaleObservation) -> Result<(), E>,
) -> Result<ArrayRef, DecimalRescaleError<E>> {
    observe(observer, DecimalRescaleObservation::OpaqueBoundary)?;
    let data = array
        .to_data()
        .into_builder()
        .data_type(DataType::Decimal128(precision, scale))
        .build()
        .map_err(|e| e.to_string())?;
    let out = make_array(data);
    observe(observer, DecimalRescaleObservation::OpaqueBoundary)?;
    Ok(out)
}
pub fn relaxed_legacy(
    child_array: &ArrayRef,
    source_scale: i8,
    target_precision: u8,
    target_scale: i8,
) -> Result<ArrayRef, String> {
    raw(|observe| {
        relaxed_observed(
            child_array,
            source_scale,
            target_precision,
            target_scale,
            observe,
        )
    })
}
fn relaxed_observed<E>(
    child_array: &ArrayRef,
    source_scale: i8,
    target_precision: u8,
    target_scale: i8,
    observer: &mut dyn FnMut(DecimalRescaleObservation) -> Result<(), E>,
) -> Result<ArrayRef, DecimalRescaleError<E>> {
    let arr = child_array
        .as_any()
        .downcast_ref::<Decimal128Array>()
        .ok_or_else(|| "failed to downcast to Decimal128Array".to_string())?;
    let precision_limit = 10_u128
        .checked_pow(u32::from(target_precision))
        .filter(|_| (1..=38).contains(&target_precision))
        .ok_or_else(|| "invalid frozen Decimal128 CAST precision".to_string())?;
    observe(observer, DecimalRescaleObservation::OpaqueBoundary)?;
    let mut values = Vec::with_capacity(arr.len());
    for row in 0..arr.len() {
        observe(observer, DecimalRescaleObservation::Step)?;
        if arr.is_null(row) {
            values.push(None);
            continue;
        }
        let mut value = arr.value(row);
        if source_scale < target_scale {
            // Preserve original unchecked i8 subtraction and original checked pow.
            let factor = crate::legacy_decimal::checked_pow10_i128(
                ((target_scale - source_scale) as u32) as usize,
            )
            .ok_or_else(|| "decimal scale overflow while casting DECIMAL".to_string())?;
            let Some(scaled) = value.checked_mul(factor) else {
                values.push(None);
                continue;
            };
            value = scaled;
        } else if source_scale > target_scale {
            let factor = crate::legacy_decimal::checked_pow10_i128(
                ((source_scale - target_scale) as u32) as usize,
            )
            .ok_or_else(|| "decimal scale overflow while casting DECIMAL".to_string())?;
            let quotient = value / factor;
            let remainder = value % factor;
            let needs_round = remainder.abs().saturating_mul(2) >= factor;
            value = if needs_round {
                let carry = if value < 0 { -1 } else { 1 };
                let Some(rounded) = quotient.checked_add(carry) else {
                    values.push(None);
                    continue;
                };
                rounded
            } else {
                quotient
            };
        }
        if value.unsigned_abs() >= precision_limit {
            values.push(None);
            continue;
        }
        values.push(Some(value));
    }
    observe(observer, DecimalRescaleObservation::OpaqueBoundary)?;
    let wide = Decimal128Array::from(values)
        .with_precision_and_scale(38, target_scale)
        .map_err(|e| e.to_string())?;
    observe(observer, DecimalRescaleObservation::OpaqueBoundary)?;
    retag_observed(&wide, target_precision, target_scale, observer)
}
pub fn enforce_precision_legacy(array: ArrayRef) -> Result<ArrayRef, String> {
    raw(|observe| enforce_precision_observed(array, observe))
}
/// Sole original declared-precision admission and active-value predicate.
pub(crate) struct DeclaredPrecision {
    limit: u128,
}
impl DeclaredPrecision {
    pub(crate) fn try_new(precision: u8) -> Option<Self> {
        10_u128
            .checked_pow(u32::from(precision))
            .filter(|_| (1..=38).contains(&precision))
            .map(|limit| Self { limit })
    }
    pub(crate) fn contains(&self, value: i128) -> bool {
        value.unsigned_abs() < self.limit
    }
}
pub(crate) fn enforce_precision_observed<E>(
    array: ArrayRef,
    observer: &mut dyn FnMut(DecimalRescaleObservation) -> Result<(), E>,
) -> Result<ArrayRef, DecimalRescaleError<E>> {
    let DataType::Decimal128(precision, scale) = *array.data_type() else {
        return Ok(array);
    };
    let declared_precision = DeclaredPrecision::try_new(precision)
        .ok_or_else(|| "invalid frozen Decimal128 CAST precision".to_string())?;
    let source = array
        .as_any()
        .downcast_ref::<Decimal128Array>()
        .ok_or_else(|| "checked CAST Decimal128 downcast failed".to_string())?;
    // Same original short-circuit all() scan. A rejected host callback executes
    // no following row or original builder operation.
    let mut all_fit = true;
    for row in 0..source.len() {
        observe(observer, DecimalRescaleObservation::Step)?;
        if !source.is_null(row) && !declared_precision.contains(source.value(row)) {
            all_fit = false;
            break;
        }
    }
    if all_fit {
        return Ok(array);
    }
    observe(observer, DecimalRescaleObservation::OpaqueBoundary)?;
    let values = (0..source.len())
        .map(|row| {
            observe(observer, DecimalRescaleObservation::Step)?;
            Ok::<_, DecimalRescaleError<E>>(
                if source.is_null(row) || !declared_precision.contains(source.value(row)) {
                    None
                } else {
                    Some(source.value(row))
                },
            )
        })
        .collect::<Result<Vec<_>, DecimalRescaleError<E>>>()?;
    observe(observer, DecimalRescaleObservation::OpaqueBoundary)?;
    let out = Arc::new(
        Decimal128Array::from(values)
            .with_precision_and_scale(precision, scale)
            .map_err(|error| error.to_string())?,
    ) as ArrayRef;
    observe(observer, DecimalRescaleObservation::OpaqueBoundary)?;
    Ok(out)
}
pub fn checked_numeric_cast_has_overflow_legacy(
    source: &ArrayRef,
    casted: &ArrayRef,
) -> Result<bool, String> {
    raw(|observe| checked_numeric_cast_has_overflow_observed(source, casted, observe))
}
pub(crate) fn checked_numeric_cast_has_overflow_observed<E>(
    source: &ArrayRef,
    casted: &ArrayRef,
    observer: &mut dyn FnMut(DecimalRescaleObservation) -> Result<(), E>,
) -> Result<bool, DecimalRescaleError<E>> {
    if source.len() != casted.len() {
        return Err("checked decimal CAST length mismatch".to_string().into());
    }
    for row in 0..source.len() {
        observe(observer, DecimalRescaleObservation::Step)?;
        if source.is_null(row) || !casted.is_null(row) {
            continue;
        }
        let finite = match source.data_type() {
            DataType::Float32 => source
                .as_any()
                .downcast_ref::<Float32Array>()
                .ok_or_else(|| "checked CAST Float32 downcast failed".to_string())?
                .value(row)
                .is_finite(),
            DataType::Float64 => source
                .as_any()
                .downcast_ref::<Float64Array>()
                .ok_or_else(|| "checked CAST Float64 downcast failed".to_string())?
                .value(row)
                .is_finite(),
            _ => true,
        };
        if finite {
            return Ok(true);
        }
    }
    Ok(false)
}
pub fn evaluate_legacy(
    source: &ArrayRef,
    target_precision: u8,
    target_scale: i8,
    policy: DecimalOverflowPolicy,
    allow: bool,
) -> Result<ArrayRef, String> {
    raw(|observe| {
        evaluate_observed(
            source,
            target_precision,
            target_scale,
            policy,
            allow,
            observe,
        )
    })
}
pub fn evaluate_observed<E>(
    source: &ArrayRef,
    target_precision: u8,
    target_scale: i8,
    policy: DecimalOverflowPolicy,
    allow: bool,
    observer: &mut dyn FnMut(DecimalRescaleObservation) -> Result<(), E>,
) -> Result<ArrayRef, DecimalRescaleError<E>> {
    let target = DataType::Decimal128(target_precision, target_scale);
    let casted = if source.data_type() == &target {
        source.clone()
    } else {
        let DataType::Decimal128(_, source_scale) = *source.data_type() else {
            return Err("failed to downcast to Decimal128Array".to_string().into());
        };
        match relaxed_observed(
            source,
            source_scale,
            target_precision,
            target_scale,
            observer,
        ) {
            Ok(out) => out,
            Err(DecimalRescaleError::Data(message)) => {
                return Err(DecimalRescaleError::Data(format!(
                    "CAST failed: from {:?} to {:?}: {message}",
                    source.data_type(),
                    target
                )));
            }
            Err(DecimalRescaleError::Host(cause)) => return Err(DecimalRescaleError::Host(cause)),
        }
    };
    let casted = enforce_precision_observed(casted, observer)?;
    let overflow = checked_numeric_cast_has_overflow_observed(source, &casted, observer)?;
    if overflow && (policy == DecimalOverflowPolicy::ReportError || allow) {
        return Err(
            "Expr evaluate meet error: The numeric type cast involving decimal overflows"
                .to_string()
                .into(),
        );
    }
    // The original final non-finite sanitizer returns Decimal128 unchanged.
    Ok(casted)
}
/// Successful NULLs from finite/full raw Decimal128 values are possible only
/// under original OutputNull and false ALLOW. Input NULL is a separate fact.
pub fn can_produce_null(policy: DecimalOverflowPolicy, allow: bool) -> bool {
    policy == DecimalOverflowPolicy::OutputNull && !allow
}
/// The original factor fails before per-value overflow for large deltas;
/// unchecked i8 overflow remains an unmaskable host panic, not a Data value.
pub fn may_raise(
    source_scale: i8,
    target_scale: i8,
    policy: DecimalOverflowPolicy,
    allow: bool,
) -> bool {
    (i16::from(source_scale) - i16::from(target_scale)).unsigned_abs() > 38
        || policy == DecimalOverflowPolicy::ReportError
        || allow
}
