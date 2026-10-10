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
//! The original text-to-TIME parser and array projection, shared by both callers.
//! Arrow retains its own distinct LargeUtf8/Utf8View parser; no normalization occurs.
use arrow_array::{Array, ArrayRef, StringArray, Time64MicrosecondArray};
use arrow_schema::{DataType, TimeUnit};
use chrono::Timelike;
use std::sync::Arc;
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TimeTextParseMode {
    Duration,
    Datetime,
}
#[derive(Clone, Copy, Debug)]
pub enum TimeTextObservation {
    Step,
    OpaqueBoundary,
}
pub fn parse_time_string_to_seconds(raw: &str) -> Option<i64> {
    let raw = raw.trim();
    if raw.is_empty() || raw.contains('+') || raw.starts_with('-') {
        return None;
    }
    let mut parts = raw.split(':');
    let hour = parts.next()?.trim().parse::<i64>().ok()?;
    let minute = parts.next()?.trim().parse::<i64>().ok()?;
    let second = parts.next()?.trim().parse::<i64>().ok()?;
    if parts.next().is_some()
        || hour < 0
        || minute < 0
        || second < 0
        || minute >= 60
        || second >= 60
    {
        return None;
    }
    hour.checked_mul(3600)?
        .checked_add(minute.checked_mul(60)?)?
        .checked_add(second)
}

pub fn parse_datetime_string_to_seconds(raw: &str) -> Option<i64> {
    let raw = raw.trim();
    if raw.is_empty() {
        return None;
    }
    let dt = chrono::NaiveDateTime::parse_from_str(raw, "%Y-%m-%d %H:%M:%S").ok()?;
    let t = dt.time();
    Some((t.hour() as i64) * 3600 + (t.minute() as i64) * 60 + t.second() as i64)
}

pub fn seconds_to_time64_micro_array(seconds: Vec<Option<i64>>) -> Result<ArrayRef, String> {
    let micros: Vec<Option<i64>> = seconds
        .into_iter()
        .map(|v| v.and_then(|s| s.checked_mul(1_000_000)))
        .collect();
    Ok(Arc::new(Time64MicrosecondArray::from(micros)) as ArrayRef)
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
/// The raw parser body above is the sole semantic author. Observation visits only
/// already demanded bytes, before the original opaque standard-library parsing.
pub fn parse_observed<E>(
    raw: &str,
    mode: TimeTextParseMode,
    mut observer: Option<&mut dyn FnMut(TimeTextObservation) -> Result<(), E>>,
) -> Result<Option<i64>, E> {
    if observer.is_some() {
        for _ in raw.bytes() {
            observe(&mut observer, TimeTextObservation::Step)?;
        }
    }
    observe(&mut observer, TimeTextObservation::OpaqueBoundary)?;
    let result = match mode {
        TimeTextParseMode::Duration => parse_time_string_to_seconds(raw),
        TimeTextParseMode::Datetime => parse_datetime_string_to_seconds(raw),
    };
    observe(&mut observer, TimeTextObservation::OpaqueBoundary)?;
    Ok(result)
}
/// Complete original text invocation. A None observer preserves the original
/// compatibility path; it is not an execution-control or memory authority.
pub fn evaluate_arrays_observed<E>(
    array: &ArrayRef,
    target: &DataType,
    mode: TimeTextParseMode,
    mut observer: Option<&mut dyn FnMut(TimeTextObservation) -> Result<(), E>>,
) -> Result<Result<ArrayRef, String>, E> {
    if !matches!(target, DataType::Time64(TimeUnit::Microsecond)) {
        return Ok(Err(format!(
            "CAST failed: TIME target must be Time64(Microsecond), got {:?}",
            target
        )));
    }
    observe(&mut observer, TimeTextObservation::OpaqueBoundary)?;
    // This allocation originally preceded both StringArray downcast and the
    // Arrow fallback. Preserve it; a layout/observation is not a host grant.
    let mut seconds = Vec::with_capacity(array.len());
    observe(&mut observer, TimeTextObservation::OpaqueBoundary)?;
    if array.data_type() == &DataType::Utf8 {
        let Some(source) = array.as_any().downcast_ref::<StringArray>() else {
            return Ok(Err("failed to downcast to StringArray".to_string()));
        };
        for row in 0..source.len() {
            observe(&mut observer, TimeTextObservation::Step)?;
            if source.is_null(row) {
                seconds.push(None);
            } else {
                let parsed = if observer.is_some() {
                    let mut relay = |event| observe(&mut observer, event);
                    parse_observed(source.value(row), mode, Some(&mut relay))?
                } else {
                    parse_observed::<E>(source.value(row), mode, None)?
                };
                seconds.push(parsed);
            }
        }
        // The checked multiplication and original Arrow builder are shared.
        if observer.is_some() {
            for _ in &seconds {
                observe(&mut observer, TimeTextObservation::Step)?;
            }
        }
        observe(&mut observer, TimeTextObservation::OpaqueBoundary)?;
        let output = seconds_to_time64_micro_array(seconds);
        observe(&mut observer, TimeTextObservation::OpaqueBoundary)?;
        Ok(output)
    } else {
        // Exact native text profiles use the original Arrow safe parser.
        // The caller has proved the concrete carrier and selected row domain.
        if observer.is_some() {
            for row in 0..array.len() {
                observe(&mut observer, TimeTextObservation::Step)?;
                if array.is_null(row) {
                    continue;
                }
                let text = match array.data_type() {
                    DataType::LargeUtf8 => array
                        .as_any()
                        .downcast_ref::<arrow_array::LargeStringArray>()
                        .map(|a| a.value(row)),
                    DataType::Utf8View => array
                        .as_any()
                        .downcast_ref::<arrow_array::StringViewArray>()
                        .map(|a| a.value(row)),
                    _ => None,
                };
                if let Some(text) = text {
                    for _ in text.bytes() {
                        observe(&mut observer, TimeTextObservation::Step)?;
                    }
                }
            }
        }
        observe(&mut observer, TimeTextObservation::OpaqueBoundary)?;
        let output = arrow_cast::cast(array.as_ref(), target).map_err(|error| {
            format!(
                "CAST failed: from {:?} to {:?}: {error}",
                array.data_type(),
                target
            )
        });
        if output.is_ok() {
            observe(&mut observer, TimeTextObservation::OpaqueBoundary)?;
        }
        // Original sanitize_non_finite_cast_result is identity for TIME. A
        // data failure returns immediately, without a fallible observer footer.
        Ok(output)
    }
}
pub fn evaluate_arrays(
    array: &ArrayRef,
    target: &DataType,
    mode: TimeTextParseMode,
) -> Result<ArrayRef, String> {
    match evaluate_arrays_observed::<std::convert::Infallible>(array, target, mode, None) {
        Ok(result) => result,
        Err(never) => match never {},
    }
}
