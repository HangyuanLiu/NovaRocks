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

//! Frozen v1 Arrow temporal casts shared by full-array and selected calls.
use arrow_array::{Array, ArrayRef};
use arrow_schema::{DataType, TimeUnit};

/// Admit only these exact carrier conversions; timezone policy stays explicit.
pub fn supports(source: &DataType, target: &DataType) -> bool {
    matches!(
        (source, target),
        (
            DataType::Date32,
            DataType::Utf8 | DataType::Timestamp(_, None)
        ) | (DataType::Timestamp(_, None), DataType::Date32)
    )
}

/// Date32 multiplication is checked on active values only. Other conversions
/// retain the Arrow author, including Timestamp -> Date32 calendar flooring.
pub fn cast(array: &dyn Array, target: &DataType) -> Result<ArrayRef, TemporalCastFailure> {
    if !supports(array.data_type(), target) {
        return Err(TemporalCastFailure::Contract(
            novarocks_failure_contract::BoundedDetail::truncating_display(format_args!(
                "unsupported exact temporal cast from {:?} to {target:?}",
                array.data_type()
            )),
        ));
    }
    if let (DataType::Date32, DataType::Timestamp(unit, None)) = (array.data_type(), target) {
        let dates = array
            .as_any()
            .downcast_ref::<arrow_array::Date32Array>()
            .ok_or_else(|| {
                TemporalCastFailure::Contract(
                    novarocks_failure_contract::BoundedDetail::truncating_display(
                        "Date32 temporal cast has a foreign carrier",
                    ),
                )
            })?;
        return match unit {
            TimeUnit::Second => {
                dates_to_timestamp::<arrow_array::types::TimestampSecondType>(dates, *unit, 86_400)
            }
            TimeUnit::Millisecond => dates_to_timestamp::<
                arrow_array::types::TimestampMillisecondType,
            >(dates, *unit, 86_400_000),
            TimeUnit::Microsecond => dates_to_timestamp::<
                arrow_array::types::TimestampMicrosecondType,
            >(dates, *unit, 86_400_000_000),
            TimeUnit::Nanosecond => {
                dates_to_timestamp::<arrow_array::types::TimestampNanosecondType>(
                    dates,
                    *unit,
                    86_400_000_000_000,
                )
            }
        };
    }
    arrow_cast::cast(array, target).map_err(TemporalCastFailure::Arrow)
}

#[derive(Debug)]
pub enum TemporalCastFailure {
    Contract(novarocks_failure_contract::BoundedDetail),
    Arrow(arrow_schema::ArrowError),
    Numeric {
        days: i32,
        unit: TimeUnit,
        ticks: i128,
        source: std::num::TryFromIntError,
    },
}
impl TemporalCastFailure {
    pub fn data_reason(&self) -> Option<crate::DataFailureReason> {
        match self {
            Self::Numeric { .. } | Self::Arrow(arrow_schema::ArrowError::ArithmeticOverflow(_)) => {
                Some(crate::DataFailureReason::NumericOutOfRange)
            }
            Self::Arrow(arrow_schema::ArrowError::CastError(_)) => {
                Some(crate::DataFailureReason::InvalidValue)
            }
            _ => None,
        }
    }
    pub fn is_data(&self) -> bool {
        self.data_reason().is_some()
    }
}
fn dates_to_timestamp<T: arrow_array::types::ArrowPrimitiveType<Native = i64>>(
    dates: &arrow_array::Date32Array,
    unit: TimeUnit,
    units_per_day: i64,
) -> Result<ArrayRef, TemporalCastFailure> {
    let mut builder = arrow_array::builder::PrimitiveBuilder::<T>::with_capacity(dates.len());
    for value in dates.iter() {
        match value {
            None => builder.append_null(),
            Some(days) => {
                let ticks = days as i128 * units_per_day as i128;
                let value =
                    i64::try_from(ticks).map_err(|source| TemporalCastFailure::Numeric {
                        days,
                        unit,
                        ticks,
                        source,
                    })?;
                builder.append_value(value);
            }
        }
    }
    Ok(std::sync::Arc::new(builder.finish()))
}

impl std::fmt::Display for TemporalCastFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Contract(detail) => f.write_str(detail.as_str()),
            Self::Arrow(source) => source.fmt(f),
            Self::Numeric {
                days, unit, ticks, ..
            } => write!(
                f,
                "Date32 value {days} requires {ticks} {unit:?} timestamp units, outside i64"
            ),
        }
    }
}
impl std::error::Error for TemporalCastFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Arrow(source) => Some(source),
            Self::Numeric { source, .. } => Some(source),
            _ => None,
        }
    }
}
impl novarocks_failure_contract::FailureProjection for TemporalCastFailure {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        use arrow_schema::ArrowError;
        use novarocks_failure_contract::{
            FailureClass, FailureFact, FailureReason, RepresentationBoundary, RepresentationFacts,
            RepresentationLayout, domain,
        };
        if let Some(reason) = self.data_reason() {
            return reason.failure_fact();
        }
        let (class, code) = match self {
            Self::Contract(_) => (FailureClass::Invariant, 207),
            Self::Arrow(ArrowError::CastError(_)) => unreachable!("value facts returned above"),
            Self::Arrow(ArrowError::MemoryError(_)) => (FailureClass::Resource, 209),
            Self::Arrow(ArrowError::OffsetOverflowError(_)) => (FailureClass::Representation, 210),
            Self::Arrow(ArrowError::IoError(..)) => (FailureClass::Unexpected, 211),
            Self::Arrow(_) => (FailureClass::Invariant, 212),
            Self::Numeric { .. } => unreachable!("numeric temporal facts returned above"),
        };
        let fact = FailureFact::new(
            class,
            FailureReason::new(domain::KERNEL, std::num::NonZeroU16::new(code).unwrap()),
        );
        if matches!(self, Self::Arrow(ArrowError::OffsetOverflowError(_))) {
            fact.with_representation(RepresentationFacts {
                boundary: RepresentationBoundary::SingleOutput,
                layout: RepresentationLayout {
                    domain: domain::KERNEL,
                    code: std::num::NonZeroU16::new(34).unwrap(),
                },
                required: None,
                extent_limit: None,
                representable_prefix: None,
            })
        } else {
            fact
        }
    }
}
/// Value and outer witnesses can only be constructed by this domain's classifier.
#[derive(Debug)]
pub enum ClassifiedTemporalCastFailure {
    Value(TemporalCastValueFailure),
    Outer(TemporalCastOuterFailure),
}
#[derive(Debug)]
/// A value failure cannot be promoted through the outer-only kernel constructor.
/// ```compile_fail
/// fn invalid(cause: novarocks_functions::temporal_carrier::TemporalCastValueFailure) {
///     let _ = novarocks_functions::KernelFailure::from_domain(cause);
/// }
/// ```
pub struct TemporalCastValueFailure {
    source: TemporalCastFailure,
    reason: crate::DataFailureReason,
}
#[derive(Debug)]
pub struct TemporalCastOuterFailure(TemporalCastFailure);
impl TemporalCastFailure {
    pub fn classify(self) -> ClassifiedTemporalCastFailure {
        match self.data_reason() {
            Some(reason) => ClassifiedTemporalCastFailure::Value(TemporalCastValueFailure {
                source: self,
                reason,
            }),
            None => ClassifiedTemporalCastFailure::Outer(TemporalCastOuterFailure(self)),
        }
    }
}
impl TemporalCastValueFailure {
    pub fn reason(&self) -> crate::DataFailureReason {
        self.reason
    }
}
impl std::fmt::Display for TemporalCastValueFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.source.fmt(f)
    }
}
impl std::error::Error for TemporalCastValueFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.source)
    }
}
impl novarocks_failure_contract::FailureProjection for TemporalCastValueFailure {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        novarocks_failure_contract::FailureProjection::failure_fact(&self.reason)
    }
}
impl std::fmt::Display for TemporalCastOuterFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}
impl std::error::Error for TemporalCastOuterFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.0)
    }
}
impl novarocks_failure_contract::FailureProjection for TemporalCastOuterFailure {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        novarocks_failure_contract::FailureProjection::failure_fact(&self.0)
    }
}
impl novarocks_failure_contract::OuterFailureProjection for TemporalCastOuterFailure {}

#[cfg(test)]
mod domain_tests {
    use super::*;
    use novarocks_failure_contract::{FailureClass, FailureProjection};
    #[test]
    fn temporal_date32_checks_both_exact_timestamp_boundaries() {
        for (unit, per_day) in [
            (TimeUnit::Microsecond, 86_400_000_000_i64),
            (TimeUnit::Nanosecond, 86_400_000_000_000_i64),
        ] {
            let low = (i64::MIN / per_day) as i32;
            let high = (i64::MAX / per_day) as i32;
            let active = arrow_array::Date32Array::from(vec![low, 0, high]);
            let output = cast(&active, &DataType::Timestamp(unit, None)).unwrap();
            assert_eq!(output.null_count(), 0);
            assert_eq!(output.len(), 3);
            for days in [low - 1, high + 1] {
                let error = cast(
                    &arrow_array::Date32Array::from(vec![days]),
                    &DataType::Timestamp(unit, None),
                )
                .unwrap_err();
                assert_eq!(
                    error.failure_fact(),
                    crate::DataFailureReason::NumericOutOfRange.failure_fact()
                );
            }
        }
    }

    #[test]
    fn temporal_date32_overflow_is_typed_and_hidden_null_payload_is_ignored() {
        use std::error::Error;
        for unit in [TimeUnit::Microsecond, TimeUnit::Nanosecond] {
            let active = arrow_array::Date32Array::from(vec![i32::MAX]);
            let error = cast(&active, &DataType::Timestamp(unit, None)).unwrap_err();
            assert_eq!(
                error.failure_fact(),
                crate::DataFailureReason::NumericOutOfRange.failure_fact()
            );
            assert!(
                error
                    .source()
                    .unwrap()
                    .downcast_ref::<std::num::TryFromIntError>()
                    .is_some()
            );
            let hidden = arrow_array::Date32Array::new(
                vec![i32::MAX, 0].into(),
                Some(arrow_buffer::NullBuffer::from(vec![false, true])),
            );
            let output = cast(&hidden, &DataType::Timestamp(unit, None)).unwrap();
            assert!(output.is_null(0));
            assert!(!output.is_null(1));
            let data = output.to_data();
            assert_eq!(&data.buffers()[0].as_slice()[8..16], &0_i64.to_ne_bytes());
        }
    }

    #[test]
    fn temporal_actual_arrow_failure_preserves_source_and_not_payload_owner() {
        use std::error::Error;
        let array = std::sync::Arc::new(arrow_array::Date32Array::from(vec![i32::MAX]));
        let weak = std::sync::Arc::downgrade(&array);
        let error = cast(array.as_ref(), &DataType::Utf8).unwrap_err();
        assert!(error.is_data());
        assert_eq!(error.failure_fact().class(), FailureClass::Data);
        assert!(
            error
                .source()
                .unwrap()
                .downcast_ref::<arrow_schema::ArrowError>()
                .is_some()
        );
        drop(array);
        assert!(weak.upgrade().is_none());
    }
    #[test]
    fn temporal_failure_variant_preserves_outer_resource_and_numeric_facts() {
        let error = TemporalCastFailure::Arrow(arrow_schema::ArrowError::MemoryError(
            "invalid value; cancelled".into(),
        ));
        assert!(!error.is_data());
        assert_eq!(error.failure_fact().class(), FailureClass::Resource);
        let numeric = TemporalCastFailure::Arrow(arrow_schema::ArrowError::ArithmeticOverflow(
            "overflow".into(),
        ));
        assert!(numeric.is_data());
        assert_eq!(
            numeric.failure_fact(),
            crate::DataFailureReason::NumericOutOfRange.failure_fact()
        );
    }
}

#[cfg(test)]
mod outer_witness_tests {
    use super::*;
    use novarocks_failure_contract::{FailureClass, FailureProjection};
    #[test]
    fn temporal_cast_outer_witness_preserves_actual_value_and_contract_sources() {
        let dates = arrow_array::Date32Array::from(vec![i32::MAX]);
        let cause = cast(&dates, &DataType::Timestamp(TimeUnit::Nanosecond, None)).unwrap_err();
        let original = cause.failure_fact();
        let ClassifiedTemporalCastFailure::Value(value) = cause.classify() else {
            panic!("expected actual numeric value failure")
        };
        assert_eq!(value.failure_fact(), original);
        assert_eq!(value.reason(), crate::DataFailureReason::NumericOutOfRange);
        use std::error::Error;
        assert!(
            value
                .source()
                .unwrap()
                .source()
                .unwrap()
                .is::<std::num::TryFromIntError>()
        );
        let cause = cast(&dates, &DataType::Boolean).unwrap_err();
        let original = cause.failure_fact();
        let ClassifiedTemporalCastFailure::Outer(outer) = cause.classify() else {
            panic!("expected frozen contract failure")
        };
        let kernel = crate::KernelFailure::from_domain(outer);
        assert_eq!(kernel.failure_fact(), original);
        assert_eq!(original.class(), FailureClass::Invariant);
    }
}
