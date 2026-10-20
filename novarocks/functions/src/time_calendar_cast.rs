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
//! Original Date32 and timestamp-to-TIME math; timezone metadata remains ignored.
use crate::time_text_cast::TimeTextObservation;
use arrow_array::{
    Array, ArrayRef, Date32Array, TimestampMicrosecondArray, TimestampMillisecondArray,
    TimestampNanosecondArray, TimestampSecondArray,
};
use arrow_schema::{DataType, TimeUnit};
use chrono::{DateTime, Timelike};
pub fn seconds_from_timestamp(unit: &TimeUnit, value: i64) -> Option<i64> {
    let micros = match unit {
        TimeUnit::Second => value.checked_mul(1_000_000)?,
        TimeUnit::Millisecond => value.checked_mul(1_000)?,
        TimeUnit::Microsecond => value,
        TimeUnit::Nanosecond => value / 1_000,
    };
    let seconds = micros.div_euclid(1_000_000);
    let sub_micros = micros.rem_euclid(1_000_000) as u32;
    let dt = DateTime::from_timestamp(seconds, sub_micros * 1000)?;
    let t = dt.naive_utc().time();
    Some((t.hour() as i64) * 3600 + (t.minute() as i64) * 60 + t.second() as i64)
}

/// Only the original timestamp ranges can generate a successful NULL.
/// Every i64 nanosecond value lies within Chrono's supported UTC date range.
pub fn can_produce_null(source: &DataType) -> bool {
    matches!(
        source,
        DataType::Timestamp(
            TimeUnit::Second | TimeUnit::Millisecond | TimeUnit::Microsecond,
            _
        )
    )
}
fn contract_failure(
    detail: impl std::fmt::Display,
) -> crate::temporal_carrier::TemporalCastFailure {
    crate::temporal_carrier::TemporalCastFailure::Contract(
        novarocks_failure_contract::BoundedDetail::truncating_display(detail),
    )
}
fn observe<E>(
    observer: &mut Option<&mut dyn FnMut(TimeTextObservation) -> Result<(), E>>,
    event: TimeTextObservation,
) -> Result<(), E> {
    if let Some(observer) = observer.as_deref_mut() {
        observer(event)?;
    }
    Ok(())
}
pub fn evaluate_arrays_observed<E>(
    array: &ArrayRef,
    target: &DataType,
    mut observer: Option<&mut dyn FnMut(TimeTextObservation) -> Result<(), E>>,
) -> Result<Result<ArrayRef, crate::temporal_carrier::TemporalCastFailure>, E> {
    if target != &DataType::Time64(TimeUnit::Microsecond) {
        return Ok(Err(contract_failure(format_args!(
            "CAST failed: TIME target must be Time64(Microsecond), got {target:?}"
        ))));
    }
    let outcome =
        (|| -> Result<Result<ArrayRef, crate::temporal_carrier::TemporalCastFailure>, E> {
            observe(&mut observer, TimeTextObservation::OpaqueBoundary)?;
            let mut seconds = Vec::with_capacity(array.len());
            observe(&mut observer, TimeTextObservation::OpaqueBoundary)?;
            let child_array = array;
            match child_array.data_type() {
                DataType::Date32 => {
                    let arr = child_array
                        .as_any()
                        .downcast_ref::<Date32Array>()
                        .ok_or_else(|| contract_failure("failed to downcast to Date32Array"));
                    let arr = match arr {
                        Ok(arr) => arr,
                        Err(e) => return Ok(Err(e)),
                    };
                    for i in 0..arr.len() {
                        observe(&mut observer, TimeTextObservation::Step)?;
                        if arr.is_null(i) {
                            seconds.push(None);
                        } else {
                            seconds.push(Some(0));
                        }
                    }
                }
                DataType::Timestamp(unit, _) => match unit {
                    TimeUnit::Second => {
                        let arr = child_array
                            .as_any()
                            .downcast_ref::<TimestampSecondArray>()
                            .ok_or_else(|| {
                                contract_failure("failed to downcast to TimestampSecondArray")
                            });
                        let arr = match arr {
                            Ok(arr) => arr,
                            Err(e) => return Ok(Err(e)),
                        };
                        for i in 0..arr.len() {
                            observe(&mut observer, TimeTextObservation::Step)?;
                            if arr.is_null(i) {
                                seconds.push(None);
                            } else {
                                observe(&mut observer, TimeTextObservation::OpaqueBoundary)?;
                                seconds.push(seconds_from_timestamp(unit, arr.value(i)));
                                observe(&mut observer, TimeTextObservation::OpaqueBoundary)?;
                            }
                        }
                    }
                    TimeUnit::Millisecond => {
                        let arr = child_array
                            .as_any()
                            .downcast_ref::<TimestampMillisecondArray>()
                            .ok_or_else(|| {
                                contract_failure("failed to downcast to TimestampMillisecondArray")
                            });
                        let arr = match arr {
                            Ok(arr) => arr,
                            Err(e) => return Ok(Err(e)),
                        };
                        for i in 0..arr.len() {
                            observe(&mut observer, TimeTextObservation::Step)?;
                            if arr.is_null(i) {
                                seconds.push(None);
                            } else {
                                observe(&mut observer, TimeTextObservation::OpaqueBoundary)?;
                                seconds.push(seconds_from_timestamp(unit, arr.value(i)));
                                observe(&mut observer, TimeTextObservation::OpaqueBoundary)?;
                            }
                        }
                    }
                    TimeUnit::Microsecond => {
                        let arr = child_array
                            .as_any()
                            .downcast_ref::<TimestampMicrosecondArray>()
                            .ok_or_else(|| {
                                contract_failure("failed to downcast to TimestampMicrosecondArray")
                            });
                        let arr = match arr {
                            Ok(arr) => arr,
                            Err(e) => return Ok(Err(e)),
                        };
                        for i in 0..arr.len() {
                            observe(&mut observer, TimeTextObservation::Step)?;
                            if arr.is_null(i) {
                                seconds.push(None);
                            } else {
                                observe(&mut observer, TimeTextObservation::OpaqueBoundary)?;
                                seconds.push(seconds_from_timestamp(unit, arr.value(i)));
                                observe(&mut observer, TimeTextObservation::OpaqueBoundary)?;
                            }
                        }
                    }
                    TimeUnit::Nanosecond => {
                        let arr = child_array
                            .as_any()
                            .downcast_ref::<TimestampNanosecondArray>()
                            .ok_or_else(|| {
                                contract_failure("failed to downcast to TimestampNanosecondArray")
                            });
                        let arr = match arr {
                            Ok(arr) => arr,
                            Err(e) => return Ok(Err(e)),
                        };
                        for i in 0..arr.len() {
                            observe(&mut observer, TimeTextObservation::Step)?;
                            if arr.is_null(i) {
                                seconds.push(None);
                            } else {
                                observe(&mut observer, TimeTextObservation::OpaqueBoundary)?;
                                seconds.push(seconds_from_timestamp(unit, arr.value(i)));
                                observe(&mut observer, TimeTextObservation::OpaqueBoundary)?;
                            }
                        }
                    }
                },
                other => {
                    return Ok(Err(contract_failure(format_args!(
                        "TIME calendar cast requires Date32 or Timestamp, got {other:?}"
                    ))));
                }
            }
            if observer.is_some() {
                for _ in &seconds {
                    observe(&mut observer, TimeTextObservation::Step)?;
                }
            }
            observe(&mut observer, TimeTextObservation::OpaqueBoundary)?;
            let output = crate::time_text_cast::seconds_to_time64_micro_array(seconds);
            observe(&mut observer, TimeTextObservation::OpaqueBoundary)?;
            Ok(Ok(output))
        })();
    outcome
}
pub fn evaluate_arrays(
    array: &ArrayRef,
    target: &DataType,
) -> Result<ArrayRef, crate::temporal_carrier::TemporalCastFailure> {
    match evaluate_arrays_observed::<std::convert::Infallible>(array, target, None) {
        Ok(r) => r,
        Err(never) => match never {},
    }
}
