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
fn tracked_scalar_from_array<A: ExactPercentileAllocator>(
    array: &ArrayRef,
    row: usize,
    allocator: &A,
) -> Result<Option<TrackedAggScalarValue<A>>, String> {
    crate::aggregate_scalar::tracked_scalar_from_array(
        array,
        row,
        allocator,
        &mut ScalarWork::new(None),
    )
    .map_err(|error| error.to_string())
}
fn tracked_scalar_to_output<A: ExactPercentileAllocator>(
    value: &TrackedAggScalarValue<A>,
) -> Result<AggScalarValue, String> {
    crate::aggregate_scalar::tracked_scalar_to_output(value, &mut ScalarWork::new(None))
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
const EXACT_PERCENTILE_MAGIC: u8 = 0xC3;
const EXACT_PERCENTILE_VERSION: u8 = 1;

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
        self.values.try_reserve(1).map_err(|_| {
            self.allocator
                .percentile_allocation_error("reserve exact percentile value")
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
    if payload.is_empty() {
        return Ok((None, ScalarVec::new_in(allocator.clone())));
    }
    if payload.len() < 2 {
        return Err("exact percentile payload too short".to_string());
    }
    if payload[0] != EXACT_PERCENTILE_MAGIC {
        return Err(format!(
            "unsupported exact percentile payload magic: expected=0x{:02x} actual=0x{:02x}",
            EXACT_PERCENTILE_MAGIC, payload[0]
        ));
    }
    if payload[1] != EXACT_PERCENTILE_VERSION {
        return Err(format!(
            "unsupported exact percentile payload version: expected={} actual={}",
            EXACT_PERCENTILE_VERSION, payload[1]
        ));
    }
    // serde_json only needs an owned scratch buffer when a string contains
    // escapes. At most one token is decoded at a time; its decoded and encoded
    // forms are each bounded by the complete payload. State-owned values are
    // separately charged by A, so 2 * payload is a complete
    // bound for parser-private live scratch.
    let scratch_bound = payload
        .len()
        .checked_mul(2)
        .ok_or_else(|| "exact percentile parser scratch bound overflow".to_string())?;
    let _scratch = allocator.reserve_percentile_transient(
        scratch_bound,
        "reserve exact percentile JSON parser scratch",
    )?;
    let mut deserializer = serde_json::Deserializer::from_slice(&payload[2..]);
    StateSeed { allocator }
        .deserialize(&mut deserializer)
        .map_err(|error| error.to_string())
}

pub fn apply_rate<A: ExactPercentileAllocator>(
    state: &mut ExactPercentileState<A>,
    rate: f64,
) -> Result<(), String> {
    if !(0.0..=1.0).contains(&rate) {
        return Err("Percentile rate must be between 0 and 1".to_string());
    }
    match state.rate {
        Some(existing) if (existing - rate).abs() > f64::EPSILON => Err(format!(
            "percentile rate mismatch while merging states: existing={} incoming={}",
            existing, rate
        )),
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
    match value {
        value @ (TrackedAggScalarValue::Int64(_)
        | TrackedAggScalarValue::Float64(_)
        | TrackedAggScalarValue::Utf8(_)
        | TrackedAggScalarValue::Date32(_)
        | TrackedAggScalarValue::Timestamp(_)
        | TrackedAggScalarValue::Decimal128(_)) => Ok(value),
        other => Err(format!(
            "unsupported percentile_disc/cont input scalar {:?}",
            other
        )),
    }
}

fn numeric_from_scalar(value: &AggScalarValue, context: &str) -> Result<f64, String> {
    match value {
        AggScalarValue::Int64(v) => Ok(*v as f64),
        AggScalarValue::Float64(v) => Ok(*v),
        AggScalarValue::Date32(v) => Ok(*v as f64),
        AggScalarValue::Timestamp(v) => Ok(*v as f64),
        AggScalarValue::Decimal128(v) => Ok(*v as f64),
        other => Err(format!(
            "{context}: unsupported percentile_cont interpolation input {:?}",
            other
        )),
    }
}

fn scalar_from_numeric(output_type: &DataType, value: f64) -> Result<AggScalarValue, String> {
    match output_type {
        DataType::Float64 => Ok(AggScalarValue::Float64(value)),
        DataType::Date32 => Ok(AggScalarValue::Date32(value as i32)),
        DataType::Timestamp(_, _) => Ok(AggScalarValue::Timestamp(value as i64)),
        other => Err(format!(
            "unsupported percentile_cont output type {:?}",
            other
        )),
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
    if let Some(rate) = numeric_value_at(rates, rate_row, PercentileInputDiagnostic::ExactUpdate)? {
        apply_rate(state, rate)?;
    }
    let Some(value) = tracked_scalar_from_array(values, value_row, &state.allocator)? else {
        return Ok(());
    };
    state.push(validate_exact_scalar(value)?)?;
    Ok(())
}
/// Original merge; update can also consume the original binary-like state.
pub fn merge_from_array<A: ExactPercentileAllocator>(
    state: &mut ExactPercentileState<A>,
    array: &ArrayRef,
    row: usize,
    diagnostic: ExactMergeDiagnostic,
) -> Result<(), String> {
    let context = match diagnostic {
        ExactMergeDiagnostic::Update => PercentileInputDiagnostic::ExactUpdate,
        ExactMergeDiagnostic::Merge => PercentileInputDiagnostic::ExactMerge,
    };
    let Some(payload) = payload_bytes_at(array, row, context)? else {
        return Ok(());
    };
    let (rate, incoming) = decode_state_into(payload, &state.allocator)?;
    if let Some(rate) = rate {
        apply_rate(state, rate)?;
    }
    state.values.try_reserve(incoming.len()).map_err(|_| {
        state
            .allocator
            .percentile_allocation_error("reserve merged exact percentile values")
    })?;
    state.values.extend(incoming);
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
    if state.values.is_empty() {
        return Ok(None);
    }
    let rate = state.rate.unwrap_or(0.0);
    let mut values: Vec<AggScalarValue> = state
        .values
        .iter()
        .map(tracked_scalar_to_output)
        .collect::<Result<Vec<_>, _>>()?;
    values.sort_by(|left, right| {
        compare_scalar_values(left, right).unwrap_or(std::cmp::Ordering::Equal)
    });

    if values.len() == 1 || rate == 1.0 {
        return match output_type {
            DataType::Float64 => Ok(Some(AggScalarValue::Float64(numeric_from_scalar(
                values.last().expect("last"),
                "percentile_cont",
            )?))),
            _ => Ok(Some(values.last().expect("last").clone())),
        };
    }

    if rate == 0.0 {
        return match output_type {
            DataType::Float64 => Ok(Some(AggScalarValue::Float64(numeric_from_scalar(
                values.first().expect("first"),
                "percentile_cont",
            )?))),
            _ => Ok(Some(values.first().expect("first").clone())),
        };
    }

    let u = ((values.len() - 1) as f64) * rate;
    let index = u.floor() as usize;
    let fraction = u - index as f64;
    if fraction == 0.0 {
        return match output_type {
            DataType::Float64 => Ok(Some(AggScalarValue::Float64(numeric_from_scalar(
                &values[index],
                "percentile_cont",
            )?))),
            _ => Ok(Some(values[index].clone())),
        };
    }

    let lower = numeric_from_scalar(&values[index], "percentile_cont")?;
    let upper = numeric_from_scalar(&values[index + 1], "percentile_cont")?;
    let interpolated = lower + fraction * (upper - lower);
    scalar_from_numeric(output_type, interpolated).map(Some)
}

pub fn finalize_disc<A: ExactPercentileAllocator>(
    state: &ExactPercentileState<A>,
) -> Result<Option<AggScalarValue>, String> {
    if state.values.is_empty() {
        return Ok(None);
    }
    let rate = state.rate.unwrap_or(0.0);
    let mut values: Vec<AggScalarValue> = state
        .values
        .iter()
        .map(tracked_scalar_to_output)
        .collect::<Result<Vec<_>, _>>()?;
    values.sort_by(|left, right| {
        compare_scalar_values(left, right).unwrap_or(std::cmp::Ordering::Equal)
    });
    if values.len() == 1 || rate == 1.0 {
        return Ok(values.last().cloned());
    }
    let index = (((values.len() - 1) as f64) * rate).ceil() as usize;
    Ok(values.get(index).cloned())
}
