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
pub enum DecimalCastFailure {
    Contract(novarocks_failure_contract::BoundedDetail),
    NumericOverflow,
    Arrow(arrow_schema::ArrowError),
    FloatFactor(crate::float_decimal128::FactorFailure),
}
impl DecimalCastFailure {
    pub fn is_data(&self) -> bool {
        matches!(self, Self::NumericOverflow)
    }
}
impl std::fmt::Display for DecimalCastFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Contract(detail) => f.write_str(detail.as_str()),
            Self::NumericOverflow => f.write_str(
                "Expr evaluate meet error: The numeric type cast involving decimal overflows",
            ),
            Self::Arrow(cause) => cause.fmt(f),
            Self::FloatFactor(cause) => cause.fmt(f),
        }
    }
}
impl std::error::Error for DecimalCastFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Arrow(cause) => Some(cause),
            Self::FloatFactor(cause) => Some(cause),
            _ => None,
        }
    }
}
impl novarocks_failure_contract::FailureProjection for DecimalCastFailure {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        use novarocks_failure_contract::{FailureClass, FailureFact, FailureReason, domain};
        if matches!(self, Self::NumericOverflow) {
            return crate::DataFailureReason::NumericOutOfRange.failure_fact();
        }
        let (class, code) = match self {
            Self::Arrow(arrow_schema::ArrowError::MemoryError(_)) => (FailureClass::Resource, 218),
            Self::Arrow(_) => (FailureClass::Invariant, 219),
            Self::FloatFactor(_) => (FailureClass::Invariant, 220),
            _ => (FailureClass::Invariant, 221),
        };

        FailureFact::new(
            class,
            FailureReason::new(domain::KERNEL, std::num::NonZeroU16::new(code).unwrap()),
        )
    }
}
/// Value and outer witnesses can only be constructed by this domain's classifier.
#[derive(Debug)]
pub enum ClassifiedDecimalCastFailure {
    Value(DecimalCastValueFailure),
    Outer(DecimalCastOuterFailure),
}
#[derive(Debug)]
/// A value failure cannot be promoted through the outer-only kernel constructor.
/// ```compile_fail
/// fn invalid(cause: novarocks_functions::decimal128_rescale::DecimalCastValueFailure) {
///     let _ = novarocks_functions::KernelFailure::from_domain(cause);
/// }
/// ```
pub struct DecimalCastValueFailure {
    source: DecimalCastFailure,
    reason: crate::DataFailureReason,
}
#[derive(Debug)]
pub struct DecimalCastOuterFailure(DecimalCastFailure);
impl DecimalCastFailure {
    pub fn classify(self) -> ClassifiedDecimalCastFailure {
        match if self.is_data() {
            Some(crate::DataFailureReason::NumericOutOfRange)
        } else {
            None
        } {
            Some(reason) => ClassifiedDecimalCastFailure::Value(DecimalCastValueFailure {
                source: self,
                reason,
            }),
            None => ClassifiedDecimalCastFailure::Outer(DecimalCastOuterFailure(self)),
        }
    }
}
impl DecimalCastValueFailure {
    pub fn reason(&self) -> crate::DataFailureReason {
        self.reason
    }
}
impl std::fmt::Display for DecimalCastValueFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.source.fmt(f)
    }
}
impl std::error::Error for DecimalCastValueFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.source)
    }
}
impl novarocks_failure_contract::FailureProjection for DecimalCastValueFailure {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        novarocks_failure_contract::FailureProjection::failure_fact(&self.reason)
    }
}
impl std::fmt::Display for DecimalCastOuterFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}
impl std::error::Error for DecimalCastOuterFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.0)
    }
}
impl novarocks_failure_contract::FailureProjection for DecimalCastOuterFailure {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        novarocks_failure_contract::FailureProjection::failure_fact(&self.0)
    }
}
impl novarocks_failure_contract::OuterFailureProjection for DecimalCastOuterFailure {}

pub(crate) fn decimal_contract(detail: impl std::fmt::Display) -> DecimalCastFailure {
    DecimalCastFailure::Contract(
        novarocks_failure_contract::BoundedDetail::truncating_display(detail),
    )
}
#[derive(Debug)]
pub enum DecimalRescaleError<E> {
    Domain(DecimalCastFailure),
    Host(E),
}
impl<E> From<DecimalCastFailure> for DecimalRescaleError<E> {
    fn from(cause: DecimalCastFailure) -> Self {
        Self::Domain(cause)
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
) -> Result<T, DecimalCastFailure> {
    match f(&mut |_| Ok(())) {
        Ok(value) => Ok(value),
        Err(DecimalRescaleError::Domain(message)) => Err(message),
        Err(DecimalRescaleError::Host(never)) => match never {},
    }
}
pub fn retag_legacy(
    array: &Decimal128Array,
    precision: u8,
    scale: i8,
) -> Result<ArrayRef, DecimalCastFailure> {
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
        .map_err(DecimalCastFailure::Arrow)?;
    let out = make_array(data);
    observe(observer, DecimalRescaleObservation::OpaqueBoundary)?;
    Ok(out)
}
pub fn relaxed_legacy(
    child_array: &ArrayRef,
    source_scale: i8,
    target_precision: u8,
    target_scale: i8,
) -> Result<ArrayRef, DecimalCastFailure> {
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
        .ok_or_else(|| decimal_contract("failed to downcast to Decimal128Array"))?;
    let precision_limit = 10_u128
        .checked_pow(u32::from(target_precision))
        .filter(|_| (1..=38).contains(&target_precision))
        .ok_or_else(|| decimal_contract("invalid frozen Decimal128 CAST precision"))?;
    observe(observer, DecimalRescaleObservation::OpaqueBoundary)?;
    let mut values = Vec::with_capacity(arr.len());
    for row in 0..arr.len() {
        observe(observer, DecimalRescaleObservation::Step)?;
        if arr.is_null(row) {
            values.push(None);
            continue;
        }
        let mut value = arr.value(row);
        let delta = i16::from(target_scale) - i16::from(source_scale);
        if delta > 38 {
            // 10^39 exceeds every i128 magnitude. Only zero can scale up.
            if value != 0 {
                values.push(None);
                continue;
            }
        } else if delta < -38 {
            // |i128::MIN| < 10^39 / 2: even half-up rounding is exactly zero.
            value = 0;
        } else if delta > 0 {
            let factor = crate::legacy_decimal::checked_pow10_i128(delta as usize)
                .expect("positive delta at most 38 has an i128 power of ten");
            let Some(scaled) = value.checked_mul(factor) else {
                values.push(None);
                continue;
            };
            value = scaled;
        } else if delta < 0 {
            let factor = crate::legacy_decimal::checked_pow10_i128((-delta) as usize)
                .expect("negative delta magnitude at most 38 has an i128 power of ten");
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
        .map_err(DecimalCastFailure::Arrow)?;
    observe(observer, DecimalRescaleObservation::OpaqueBoundary)?;
    retag_observed(&wide, target_precision, target_scale, observer)
}
pub fn enforce_precision_legacy(array: ArrayRef) -> Result<ArrayRef, DecimalCastFailure> {
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
        .ok_or_else(|| decimal_contract("invalid frozen Decimal128 CAST precision"))?;
    let source = array
        .as_any()
        .downcast_ref::<Decimal128Array>()
        .ok_or_else(|| decimal_contract("checked CAST Decimal128 downcast failed"))?;
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
            .map_err(DecimalCastFailure::Arrow)?,
    ) as ArrayRef;
    observe(observer, DecimalRescaleObservation::OpaqueBoundary)?;
    Ok(out)
}
pub fn checked_numeric_cast_has_overflow_legacy(
    source: &ArrayRef,
    casted: &ArrayRef,
) -> Result<bool, DecimalCastFailure> {
    raw(|observe| checked_numeric_cast_has_overflow_observed(source, casted, observe))
}
pub(crate) fn checked_numeric_cast_has_overflow_observed<E>(
    source: &ArrayRef,
    casted: &ArrayRef,
    observer: &mut dyn FnMut(DecimalRescaleObservation) -> Result<(), E>,
) -> Result<bool, DecimalRescaleError<E>> {
    if source.len() != casted.len() {
        return Err(decimal_contract("checked decimal CAST length mismatch").into());
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
                .ok_or_else(|| decimal_contract("checked CAST Float32 downcast failed"))?
                .value(row)
                .is_finite(),
            DataType::Float64 => source
                .as_any()
                .downcast_ref::<Float64Array>()
                .ok_or_else(|| decimal_contract("checked CAST Float64 downcast failed"))?
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
) -> Result<ArrayRef, DecimalCastFailure> {
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
            return Err(decimal_contract("failed to downcast to Decimal128Array").into());
        };
        match relaxed_observed(
            source,
            source_scale,
            target_precision,
            target_scale,
            observer,
        ) {
            Ok(out) => out,
            Err(DecimalRescaleError::Domain(cause)) => {
                return Err(DecimalRescaleError::Domain(cause));
            }
            Err(DecimalRescaleError::Host(cause)) => return Err(DecimalRescaleError::Host(cause)),
        }
    };
    let casted = enforce_precision_observed(casted, observer)?;
    let overflow = checked_numeric_cast_has_overflow_observed(source, &casted, observer)?;
    if overflow && (policy == DecimalOverflowPolicy::ReportError || allow) {
        return Err(DecimalCastFailure::NumericOverflow.into());
    }
    // The original final non-finite sanitizer returns Decimal128 unchanged.
    Ok(casted)
}
/// Successful NULLs from finite/full raw Decimal128 values are possible only
/// under original OutputNull and false ALLOW. Input NULL is a separate fact.
pub fn can_produce_null(policy: DecimalOverflowPolicy, allow: bool) -> bool {
    policy == DecimalOverflowPolicy::OutputNull && !allow
}
/// Scale differences are computed in i16 and resolved without an unrepresentable factor.
pub fn may_raise(
    _source_scale: i8,
    _target_scale: i8,
    policy: DecimalOverflowPolicy,
    allow: bool,
) -> bool {
    policy == DecimalOverflowPolicy::ReportError || allow
}

#[cfg(test)]
mod source_properties {
    use super::*;
    use novarocks_failure_contract::{FailureClass, FailureProjection};
    #[test]
    fn wide_scale_uses_value_proofs_instead_of_a_factor_capacity_limit() {
        let source: ArrayRef = Arc::new(
            Decimal128Array::from(vec![Some(0), Some(1), Some(-1), None])
                .with_precision_and_scale(38, -128)
                .unwrap(),
        );
        let output =
            evaluate_legacy(&source, 38, 38, DecimalOverflowPolicy::OutputNull, false).unwrap();
        let output = output.as_any().downcast_ref::<Decimal128Array>().unwrap();
        assert_eq!(output.value(0), 0);
        assert!(!output.is_null(0));
        assert!(output.is_null(1));
        assert!(output.is_null(2));
        assert!(output.is_null(3));
        let failure = evaluate_legacy(&source, 38, 38, DecimalOverflowPolicy::ReportError, false)
            .unwrap_err();
        let ClassifiedDecimalCastFailure::Value(failure) = failure.classify() else {
            panic!("actual overflow is a value failure")
        };
        assert_eq!(
            failure.failure_fact(),
            crate::DataFailureReason::NumericOutOfRange.failure_fact()
        );
        let source: ArrayRef = Arc::new(
            Decimal128Array::from(vec![Some(i128::MIN), Some(i128::MAX), Some(1), None])
                .with_precision_and_scale(38, 38)
                .unwrap(),
        );
        let output =
            evaluate_legacy(&source, 38, -128, DecimalOverflowPolicy::ReportError, true).unwrap();
        let output = output.as_any().downcast_ref::<Decimal128Array>().unwrap();
        for row in 0..3 {
            assert_eq!(output.value(row), 0);
            assert!(!output.is_null(row));
        }
        assert!(output.is_null(3));
    }
    #[test]
    fn decimal_construction_retains_actual_arrow_source() {
        use std::error::Error;
        let source = Decimal128Array::from(vec![1])
            .with_precision_and_scale(0, 0)
            .unwrap_err();
        let cause = DecimalCastFailure::Arrow(source);
        let original = cause.failure_fact();
        let ClassifiedDecimalCastFailure::Outer(cause) = cause.classify() else {
            panic!("constructor error is outer")
        };
        assert!(
            cause
                .source()
                .unwrap()
                .source()
                .unwrap()
                .downcast_ref::<arrow_schema::ArrowError>()
                .is_some()
        );
        assert_eq!(cause.failure_fact(), original);
        assert_eq!(
            crate::KernelFailure::from_domain(cause)
                .failure_fact()
                .class(),
            FailureClass::Invariant
        );
    }
}
