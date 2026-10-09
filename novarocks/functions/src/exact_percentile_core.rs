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

//! Original exact-percentile JSON codec, rate admission and final math.
//! Storage and parser scratch authority are supplied explicitly by the host.
use crate::aggregate_scalar::{
    AggScalarValue, ScalarStateAllocator, ScalarWork, TrackedAggScalarValue,
};
use crate::exact_percentile_failure::{
    LegacyPercentileFailure, PercentileDataRecipe, PercentileFailureSink,
};
use crate::percentile_input::{PercentileInputDiagnostic, numeric_value_at, payload_bytes_at};
use allocator_api2::vec::Vec as ScalarVec;
use arrow_array::ArrayRef;
use arrow_schema::DataType;
use serde::de::{DeserializeSeed, MapAccess, SeqAccess, Visitor};
use serde::ser::{SerializeSeq, SerializeStruct};
use serde::{Deserialize, Serialize, Serializer};
use std::borrow::Cow;

pub trait ExactPercentileAllocator: ScalarStateAllocator {
    type ParserReservation;
    fn reserve_percentile_transient(
        &self,
        bytes: usize,
        operation: &str,
    ) -> Result<Self::ParserReservation, String>;
    fn reserve_percentile_transient_lossless(
        &self,
        bytes: usize,
        operation: &str,
    ) -> Result<Self::ParserReservation, crate::aggregate_scalar::ScalarStateError> {
        self.reserve_percentile_transient(bytes, operation)
            .map_err(crate::aggregate_scalar::ScalarStateError::Legacy)
    }
    fn percentile_allocation_failure(
        &self,
        operation: &str,
    ) -> crate::aggregate_scalar::ScalarStateError {
        crate::aggregate_scalar::ScalarStateError::Legacy(
            self.percentile_allocation_error(operation),
        )
    }
    fn percentile_allocation_error(&self, operation: &str) -> String {
        self.scalar_allocation_error(operation).to_string()
    }
}
fn scalar_bytes_legacy<A: ExactPercentileAllocator>(
    allocator: A,
    bytes: &[u8],
) -> Result<ScalarVec<u8, A>, String> {
    crate::aggregate_scalar::scalar_bytes(allocator, bytes, &mut ScalarWork::new(None))
        .map_err(|error| error.to_string())
}
fn compare_scalar_values(
    left: &AggScalarValue,
    right: &AggScalarValue,
) -> Result<std::cmp::Ordering, String> {
    crate::aggregate_scalar_fingerprint::compare_scalar_values(
        left,
        right,
        &mut ScalarWork::new(None),
    )
    .map_err(|error| error.to_string())
}
pub(crate) const EXACT_PERCENTILE_MAGIC: u8 = 0xC3;
pub(crate) const EXACT_PERCENTILE_VERSION: u8 = 1;

#[derive(Debug, Deserialize)]
enum BorrowedSerializableScalar<'a> {
    Int64(i64),
    Float64(f64),
    Utf8(#[serde(borrow)] Cow<'a, str>),
    Date32(i32),
    Timestamp(i64),
    Decimal128(i128),
}

pub struct ExactPercentileState<A: ExactPercentileAllocator> {
    pub allocator: A,
    pub rate: Option<f64>,
    pub values: ScalarVec<TrackedAggScalarValue<A>, A>,
}

impl<A: ExactPercentileAllocator> ExactPercentileState<A> {
    pub fn new(allocator: A) -> Self {
        Self {
            values: ScalarVec::new_in(allocator.clone()),
            allocator,
            rate: None,
        }
    }

    pub fn push(&mut self, value: TrackedAggScalarValue<A>) -> Result<(), String> {
        self.push_with_sink(value, &mut LegacyPercentileFailure)
    }
    pub fn push_with_sink<S: PercentileFailureSink<A>>(
        &mut self,
        value: TrackedAggScalarValue<A>,
        sink: &mut S,
    ) -> Result<(), S::Error> {
        self.values.try_reserve(1).map_err(|_| {
            sink.scalar(
                self.allocator
                    .percentile_allocation_failure("reserve exact percentile value"),
            )
        })?;
        self.values.push(value);
        Ok(())
    }
}

struct TrackedScalarWire<'a, A: ExactPercentileAllocator>(&'a TrackedAggScalarValue<A>);

impl<A: ExactPercentileAllocator> Serialize for TrackedScalarWire<'_, A> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self.0 {
            TrackedAggScalarValue::Int64(value) => {
                serializer.serialize_newtype_variant("SerializableScalar", 0, "Int64", value)
            }
            TrackedAggScalarValue::Float64(value) => {
                serializer.serialize_newtype_variant("SerializableScalar", 1, "Float64", value)
            }
            TrackedAggScalarValue::Utf8(value) => serializer.serialize_newtype_variant(
                "SerializableScalar",
                2,
                "Utf8",
                std::str::from_utf8(value).map_err(serde::ser::Error::custom)?,
            ),
            TrackedAggScalarValue::Date32(value) => {
                serializer.serialize_newtype_variant("SerializableScalar", 3, "Date32", value)
            }
            TrackedAggScalarValue::Timestamp(value) => {
                serializer.serialize_newtype_variant("SerializableScalar", 4, "Timestamp", value)
            }
            TrackedAggScalarValue::Decimal128(value) => {
                serializer.serialize_newtype_variant("SerializableScalar", 5, "Decimal128", value)
            }
            other => Err(serde::ser::Error::custom(format!(
                "unsupported exact percentile scalar {other:?}"
            ))),
        }
    }
}

struct TrackedValuesWire<'a, A: ExactPercentileAllocator>(&'a [TrackedAggScalarValue<A>]);

impl<A: ExactPercentileAllocator> Serialize for TrackedValuesWire<'_, A> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut sequence = serializer.serialize_seq(Some(self.0.len()))?;
        for value in self.0 {
            sequence.serialize_element(&TrackedScalarWire(value))?;
        }
        sequence.end()
    }
}

struct ExactStateWire<'a, A: ExactPercentileAllocator>(&'a ExactPercentileState<A>);

impl<A: ExactPercentileAllocator> Serialize for ExactStateWire<'_, A> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("ExactPercentileState", 2)?;
        state.serialize_field("rate", &self.0.rate)?;
        state.serialize_field("values", &TrackedValuesWire(&self.0.values))?;
        state.end()
    }
}

pub fn encode_state<A: ExactPercentileAllocator>(state: &ExactPercentileState<A>) -> Vec<u8> {
    let payload =
        serde_json::to_vec(&ExactStateWire(state)).expect("serialize exact percentile state");
    let mut out = Vec::with_capacity(2 + payload.len());
    out.push(EXACT_PERCENTILE_MAGIC);
    out.push(EXACT_PERCENTILE_VERSION);
    out.extend_from_slice(&payload);
    out
}

struct ValuesSeed<'a, A: ExactPercentileAllocator> {
    allocator: &'a A,
}

impl<'de, A: ExactPercentileAllocator> DeserializeSeed<'de> for ValuesSeed<'_, A> {
    type Value = ScalarVec<TrackedAggScalarValue<A>, A>;

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct ValuesVisitor<'a, A: ExactPercentileAllocator> {
            allocator: &'a A,
        }

        impl<'de, A: ExactPercentileAllocator> Visitor<'de> for ValuesVisitor<'_, A> {
            type Value = ScalarVec<TrackedAggScalarValue<A>, A>;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("an exact percentile value array")
            }

            fn visit_seq<S>(self, mut sequence: S) -> Result<Self::Value, S::Error>
            where
                S: SeqAccess<'de>,
            {
                let mut values = ScalarVec::new_in(self.allocator.clone());
                while let Some(value) =
                    sequence.next_element::<BorrowedSerializableScalar<'de>>()?
                {
                    values.try_reserve(1).map_err(|_| {
                        serde::de::Error::custom(
                            self.allocator.percentile_allocation_error(
                                "reserve decoded exact percentile value",
                            ),
                        )
                    })?;
                    let value = match value {
                        BorrowedSerializableScalar::Int64(value) => {
                            TrackedAggScalarValue::Int64(value)
                        }
                        BorrowedSerializableScalar::Float64(value) => {
                            TrackedAggScalarValue::Float64(value)
                        }
                        BorrowedSerializableScalar::Utf8(value) => TrackedAggScalarValue::Utf8(
                            scalar_bytes_legacy(self.allocator.clone(), value.as_bytes())
                                .map_err(serde::de::Error::custom)?,
                        ),
                        BorrowedSerializableScalar::Date32(value) => {
                            TrackedAggScalarValue::Date32(value)
                        }
                        BorrowedSerializableScalar::Timestamp(value) => {
                            TrackedAggScalarValue::Timestamp(value)
                        }
                        BorrowedSerializableScalar::Decimal128(value) => {
                            TrackedAggScalarValue::Decimal128(value)
                        }
                    };
                    values.push(value);
                }
                Ok(values)
            }
        }

        deserializer.deserialize_seq(ValuesVisitor {
            allocator: self.allocator,
        })
    }
}

struct StateSeed<'a, A: ExactPercentileAllocator> {
    allocator: &'a A,
}

impl<'de, A: ExactPercentileAllocator> DeserializeSeed<'de> for StateSeed<'_, A> {
    type Value = (Option<f64>, ScalarVec<TrackedAggScalarValue<A>, A>);

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct StateVisitor<'a, A: ExactPercentileAllocator> {
            allocator: &'a A,
        }

        impl<'de, A: ExactPercentileAllocator> Visitor<'de> for StateVisitor<'_, A> {
            type Value = (Option<f64>, ScalarVec<TrackedAggScalarValue<A>, A>);

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("an exact percentile state object")
            }

            fn visit_map<M>(self, mut map: M) -> Result<Self::Value, M::Error>
            where
                M: MapAccess<'de>,
            {
                let mut rate = None;
                let mut values = None;
                while let Some(field) = map.next_key::<Cow<'de, str>>()? {
                    match field.as_ref() {
                        "rate" => rate = map.next_value()?,
                        "values" => {
                            values = Some(map.next_value_seed(ValuesSeed {
                                allocator: self.allocator,
                            })?)
                        }
                        _ => {
                            let _: serde::de::IgnoredAny = map.next_value()?;
                        }
                    }
                }
                Ok((
                    rate,
                    values.unwrap_or_else(|| ScalarVec::new_in(self.allocator.clone())),
                ))
            }
        }

        deserializer.deserialize_struct(
            "ExactPercentileState",
            &["rate", "values"],
            StateVisitor {
                allocator: self.allocator,
            },
        )
    }
}

pub fn decode_state_into<A: ExactPercentileAllocator>(
    payload: &[u8],
    allocator: &A,
) -> Result<(Option<f64>, ScalarVec<TrackedAggScalarValue<A>, A>), String> {
    decode_state_into_with_sink(payload, allocator, &mut LegacyPercentileFailure)
}
pub fn decode_state_into_with_sink<A: ExactPercentileAllocator, S: PercentileFailureSink<A>>(
    payload: &[u8],
    allocator: &A,
    sink: &mut S,
) -> Result<(Option<f64>, ScalarVec<TrackedAggScalarValue<A>, A>), S::Error> {
    if payload.is_empty() {
        return Ok((None, ScalarVec::new_in(allocator.clone())));
    }
    if payload.len() < 2 {
        return Err(sink.data(PercentileDataRecipe::ShortPayload));
    }
    if payload[0] != EXACT_PERCENTILE_MAGIC {
        return Err(sink.data(PercentileDataRecipe::Magic(payload[0])));
    }
    if payload[1] != EXACT_PERCENTILE_VERSION {
        return Err(sink.data(PercentileDataRecipe::Version(payload[1])));
    }
    // serde_json only needs an owned scratch buffer when a string contains
    // escapes. At most one token is decoded at a time; its decoded and encoded
    // forms are each bounded by the complete payload. State-owned values are
    // separately charged by A, so 2 * payload is a complete
    // bound for parser-private live scratch.
    let scratch_bound = payload
        .len()
        .checked_mul(2)
        .ok_or_else(|| sink.data(PercentileDataRecipe::ScratchOverflow))?;
    let _scratch = allocator
        .reserve_percentile_transient_lossless(
            scratch_bound,
            "reserve exact percentile JSON parser scratch",
        )
        .map_err(|error| sink.scalar(error))?;
    let mut deserializer = serde_json::Deserializer::from_slice(&payload[2..]);
    StateSeed { allocator }
        .deserialize(&mut deserializer)
        .map_err(|error| sink.json(&error, allocator))
}

pub fn apply_rate<A: ExactPercentileAllocator>(
    state: &mut ExactPercentileState<A>,
    rate: f64,
) -> Result<(), String> {
    apply_rate_with_sink(state, rate, &mut LegacyPercentileFailure)
}
pub fn apply_rate_with_sink<A: ExactPercentileAllocator, S: PercentileFailureSink<A>>(
    state: &mut ExactPercentileState<A>,
    rate: f64,
    sink: &mut S,
) -> Result<(), S::Error> {
    if !(0.0..=1.0).contains(&rate) {
        return Err(sink.data(PercentileDataRecipe::RateOutOfRange));
    }
    match state.rate {
        Some(existing) if (existing - rate).abs() > f64::EPSILON => {
            Err(sink.data(PercentileDataRecipe::RateMismatch {
                existing,
                incoming: rate,
            }))
        }
        Some(_) => Ok(()),
        None => {
            state.rate = Some(rate);
            Ok(())
        }
    }
}

pub fn validate_exact_scalar<A: ExactPercentileAllocator>(
    value: TrackedAggScalarValue<A>,
) -> Result<TrackedAggScalarValue<A>, String> {
    validate_exact_scalar_with_sink(value, &mut LegacyPercentileFailure)
}
pub fn validate_exact_scalar_with_sink<A: ExactPercentileAllocator, S: PercentileFailureSink<A>>(
    value: TrackedAggScalarValue<A>,
    sink: &mut S,
) -> Result<TrackedAggScalarValue<A>, S::Error> {
    match value {
        value @ (TrackedAggScalarValue::Int64(_)
        | TrackedAggScalarValue::Float64(_)
        | TrackedAggScalarValue::Utf8(_)
        | TrackedAggScalarValue::Date32(_)
        | TrackedAggScalarValue::Timestamp(_)
        | TrackedAggScalarValue::Decimal128(_)) => Ok(value),
        other => Err(sink.data(PercentileDataRecipe::InvalidScalar(&other))),
    }
}

fn numeric_from_scalar_with_sink<A: ExactPercentileAllocator, S: PercentileFailureSink<A>>(
    value: &AggScalarValue,
    sink: &mut S,
) -> Result<f64, S::Error> {
    match value {
        AggScalarValue::Int64(v) => Ok(*v as f64),
        AggScalarValue::Float64(v) => Ok(*v),
        AggScalarValue::Date32(v) => Ok(*v as f64),
        AggScalarValue::Timestamp(v) => Ok(*v as f64),
        AggScalarValue::Decimal128(v) => Ok(*v as f64),
        other => Err(sink.data(PercentileDataRecipe::InterpolationInput(other))),
    }
}

fn scalar_from_numeric_with_sink<A: ExactPercentileAllocator, S: PercentileFailureSink<A>>(
    output_type: &DataType,
    value: f64,
    sink: &mut S,
) -> Result<AggScalarValue, S::Error> {
    match output_type {
        DataType::Float64 => Ok(AggScalarValue::Float64(value)),
        DataType::Date32 => Ok(AggScalarValue::Date32(value as i32)),
        DataType::Timestamp(_, _) => Ok(AggScalarValue::Timestamp(value as i64)),
        other => Err(sink.data(PercentileDataRecipe::OutputType(other))),
    }
}

/// Original row update; the host has already mapped actual selected addresses.
pub fn update_from_arrays<A: ExactPercentileAllocator>(
    state: &mut ExactPercentileState<A>,
    values: &ArrayRef,
    value_row: usize,
    rates: &ArrayRef,
    rate_row: usize,
) -> Result<(), String> {
    update_from_arrays_with_sink(
        state,
        values,
        value_row,
        rates,
        rate_row,
        &mut ScalarWork::new(None),
        &mut LegacyPercentileFailure,
    )
}
pub fn update_from_arrays_with_sink<A: ExactPercentileAllocator, S: PercentileFailureSink<A>>(
    state: &mut ExactPercentileState<A>,
    values: &ArrayRef,
    value_row: usize,
    rates: &ArrayRef,
    rate_row: usize,
    work: &mut ScalarWork<'_, '_>,
    sink: &mut S,
) -> Result<(), S::Error> {
    if let Some(rate) = numeric_value_at(rates, rate_row, PercentileInputDiagnostic::ExactUpdate)
        .map_err(|error| sink.reader(error))?
    {
        apply_rate_with_sink(state, rate, sink)?;
    }
    let Some(value) = crate::aggregate_scalar::tracked_scalar_from_array(
        values,
        value_row,
        &state.allocator,
        work,
    )
    .map_err(|error| sink.scalar(error))?
    else {
        return Ok(());
    };
    state.push_with_sink(validate_exact_scalar_with_sink(value, sink)?, sink)?;
    Ok(())
}
/// Original merge; update can also consume the original binary-like state.
pub fn merge_from_array<A: ExactPercentileAllocator>(
    state: &mut ExactPercentileState<A>,
    array: &ArrayRef,
    row: usize,
    diagnostic: ExactMergeDiagnostic,
) -> Result<(), String> {
    merge_from_array_with_sink(
        state,
        array,
        row,
        diagnostic,
        &mut ScalarWork::new(None),
        &mut LegacyPercentileFailure,
    )
}
pub fn merge_from_array_with_sink<A: ExactPercentileAllocator, S: PercentileFailureSink<A>>(
    state: &mut ExactPercentileState<A>,
    array: &ArrayRef,
    row: usize,
    diagnostic: ExactMergeDiagnostic,
    work: &mut ScalarWork<'_, '_>,
    sink: &mut S,
) -> Result<(), S::Error> {
    let context = match diagnostic {
        ExactMergeDiagnostic::Update => PercentileInputDiagnostic::ExactUpdate,
        ExactMergeDiagnostic::Merge => PercentileInputDiagnostic::ExactMerge,
    };
    let Some(payload) =
        payload_bytes_at(array, row, context).map_err(|error| sink.reader(error))?
    else {
        return Ok(());
    };
    work.flush().map_err(|error| sink.scalar(error))?;
    let (rate, incoming) = decode_state_into_with_sink(payload, &state.allocator, sink)?;
    if let Some(rate) = rate {
        apply_rate_with_sink(state, rate, sink)?;
    }
    work.flush().map_err(|error| sink.scalar(error))?;
    state.values.try_reserve(incoming.len()).map_err(|_| {
        sink.scalar(
            state
                .allocator
                .percentile_allocation_failure("reserve merged exact percentile values"),
        )
    })?;
    // Extend remains the original owned-vector operation, observed as opaque work.
    state.values.extend(incoming);
    work.flush().map_err(|error| sink.scalar(error))?;
    Ok(())
}
#[derive(Clone, Copy, Debug)]
pub enum ExactMergeDiagnostic {
    Update,
    Merge,
}

pub fn finalize_cont<A: ExactPercentileAllocator>(
    state: &ExactPercentileState<A>,
    output_type: &DataType,
) -> Result<Option<AggScalarValue>, String> {
    finalize_cont_with_sink(
        state,
        output_type,
        &mut ScalarWork::new(None),
        &mut LegacyPercentileFailure,
    )
}
pub fn finalize_cont_with_sink<A: ExactPercentileAllocator, S: PercentileFailureSink<A>>(
    state: &ExactPercentileState<A>,
    output_type: &DataType,
    work: &mut ScalarWork<'_, '_>,
    sink: &mut S,
) -> Result<Option<AggScalarValue>, S::Error> {
    if state.values.is_empty() {
        return Ok(None);
    }
    let rate = state.rate.unwrap_or(0.0);
    let mut values: Vec<AggScalarValue> = state
        .values
        .iter()
        .map(|value| crate::aggregate_scalar::tracked_scalar_to_output(value, work))
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| sink.scalar(error))?;
    work.flush().map_err(|error| sink.scalar(error))?;
    values.sort_by(|left, right| {
        compare_scalar_values(left, right).unwrap_or(std::cmp::Ordering::Equal)
    });
    work.flush().map_err(|error| sink.scalar(error))?;

    if values.len() == 1 || rate == 1.0 {
        return match output_type {
            DataType::Float64 => Ok(Some(AggScalarValue::Float64(
                numeric_from_scalar_with_sink::<A, S>(values.last().expect("last"), sink)?,
            ))),
            _ => Ok(Some(values.last().expect("last").clone())),
        };
    }

    if rate == 0.0 {
        return match output_type {
            DataType::Float64 => Ok(Some(AggScalarValue::Float64(
                numeric_from_scalar_with_sink::<A, S>(values.first().expect("first"), sink)?,
            ))),
            _ => Ok(Some(values.first().expect("first").clone())),
        };
    }

    let u = ((values.len() - 1) as f64) * rate;
    let index = u.floor() as usize;
    let fraction = u - index as f64;
    if fraction == 0.0 {
        return match output_type {
            DataType::Float64 => Ok(Some(AggScalarValue::Float64(
                numeric_from_scalar_with_sink::<A, S>(&values[index], sink)?,
            ))),
            _ => Ok(Some(values[index].clone())),
        };
    }

    let lower = numeric_from_scalar_with_sink::<A, S>(&values[index], sink)?;
    let upper = numeric_from_scalar_with_sink::<A, S>(&values[index + 1], sink)?;
    let interpolated = lower + fraction * (upper - lower);
    scalar_from_numeric_with_sink::<A, S>(output_type, interpolated, sink).map(Some)
}

pub fn finalize_disc<A: ExactPercentileAllocator>(
    state: &ExactPercentileState<A>,
) -> Result<Option<AggScalarValue>, String> {
    finalize_disc_with_sink(
        state,
        &mut ScalarWork::new(None),
        &mut LegacyPercentileFailure,
    )
}
pub fn finalize_disc_with_sink<A: ExactPercentileAllocator, S: PercentileFailureSink<A>>(
    state: &ExactPercentileState<A>,
    work: &mut ScalarWork<'_, '_>,
    sink: &mut S,
) -> Result<Option<AggScalarValue>, S::Error> {
    if state.values.is_empty() {
        return Ok(None);
    }
    let rate = state.rate.unwrap_or(0.0);
    let mut values: Vec<AggScalarValue> = state
        .values
        .iter()
        .map(|value| crate::aggregate_scalar::tracked_scalar_to_output(value, work))
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| sink.scalar(error))?;
    work.flush().map_err(|error| sink.scalar(error))?;
    values.sort_by(|left, right| {
        compare_scalar_values(left, right).unwrap_or(std::cmp::Ordering::Equal)
    });
    work.flush().map_err(|error| sink.scalar(error))?;
    if values.len() == 1 || rate == 1.0 {
        return Ok(values.last().cloned());
    }
    let index = (((values.len() - 1) as f64) * rate).ceil() as usize;
    Ok(values.get(index).cloned())
}
