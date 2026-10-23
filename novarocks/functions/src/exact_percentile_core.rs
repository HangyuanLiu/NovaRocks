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
    PercentileDataRecipe, PercentileFailureSink, RequiredPercentileFailure,
};
use crate::percentile_input::{
    PercentileInputDiagnostic, numeric_value_at_with_failure, payload_bytes_at_with_failure,
};
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
    ) -> Result<Self::ParserReservation, crate::KernelFailure>;
}

/// A decoded exact-percentile state with values owned by its supplied allocator.
#[expect(
    type_alias_bounds,
    reason = "The allocator bound is required to form the decoded allocator-backed values"
)]
pub type DecodedExactPercentileState<A: ExactPercentileAllocator> =
    (Option<f64>, ScalarVec<TrackedAggScalarValue<A>, A>);

fn scalar_bytes<A: ExactPercentileAllocator>(
    allocator: A,
    bytes: &[u8],
) -> Result<ScalarVec<u8, A>, crate::KernelFailure> {
    crate::aggregate_scalar::scalar_bytes(allocator, bytes, &mut ScalarWork::new(None))
}
/// serde has its own fixed error type. The synchronous decode owns this one
/// stack slot, so an allocation/control refusal never travels through text.
#[derive(Default)]
struct DecodeRefusal(std::cell::RefCell<Option<crate::KernelFailure>>);
impl DecodeRefusal {
    fn serde_error<E: serde::de::Error>(&self, source: crate::KernelFailure) -> E {
        let mut first = self.0.borrow_mut();
        if first.is_none() {
            *first = Some(source);
        }
        E::custom("exact percentile state allocation refused")
    }
    fn take(&self) -> Option<crate::KernelFailure> {
        self.0.borrow_mut().take()
    }
}
fn compare_scalar_values(
    left: &AggScalarValue,
    right: &AggScalarValue,
) -> Result<std::cmp::Ordering, crate::KernelFailure> {
    use crate::aggregate_scalar::ScalarOrdering;
    // Preserve the original percentile stable ordering for NaN and mixed
    // kinds. An actual callback failure is not an ordering policy.
    Ok(
        match crate::aggregate_scalar_fingerprint::compare_scalar_values_partial(
            left,
            right,
            &mut ScalarWork::new(None),
        )? {
            ScalarOrdering::Ordered(order) => order,
            ScalarOrdering::UnorderedFloat | ScalarOrdering::MismatchedKinds => {
                std::cmp::Ordering::Equal
            }
        },
    )
}
fn sort_scalar_values(values: &mut [AggScalarValue]) -> Result<(), crate::KernelFailure> {
    let mut primary = None;
    values.sort_by(|left, right| {
        if primary.is_some() {
            return std::cmp::Ordering::Equal;
        }
        match compare_scalar_values(left, right) {
            Ok(order) => order,
            Err(cause) => {
                primary = Some(cause);
                std::cmp::Ordering::Equal
            }
        }
    });
    match primary {
        Some(cause) => Err(cause),
        None => Ok(()),
    }
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

    pub fn push(&mut self, value: TrackedAggScalarValue<A>) -> Result<(), crate::KernelFailure> {
        self.push_with_sink(value, &mut RequiredPercentileFailure)
    }
    pub fn push_with_sink<S: PercentileFailureSink<A>>(
        &mut self,
        value: TrackedAggScalarValue<A>,
        sink: &mut S,
    ) -> Result<(), S::Error> {
        self.values.try_reserve(1).map_err(|source| {
            sink.scalar(
                self.allocator
                    .scalar_allocation_error("reserve exact percentile value", source),
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

pub fn encode_state<A: ExactPercentileAllocator>(
    state: &ExactPercentileState<A>,
) -> Result<Vec<u8>, crate::KernelFailure> {
    let payload = serde_json::to_vec(&ExactStateWire(state))
        .map_err(crate::exact_percentile_failure::json_encode_failure)?;
    let size = crate::exact_percentile_failure::byte_extent(2 + payload.len() as u128)?;
    let mut out = Vec::new();
    out.try_reserve_exact(size).map_err(|source| {
        crate::KernelFailure::from_domain(crate::allocation_failure::AllocationRefusal::new(
            "reserve encoded exact percentile state",
            source,
        ))
    })?;
    out.push(EXACT_PERCENTILE_MAGIC);
    out.push(EXACT_PERCENTILE_VERSION);
    out.extend_from_slice(&payload);
    Ok(out)
}

struct ValuesSeed<'a, A: ExactPercentileAllocator> {
    allocator: &'a A,
    refusal: &'a DecodeRefusal,
}

impl<'de, A: ExactPercentileAllocator> DeserializeSeed<'de> for ValuesSeed<'_, A> {
    type Value = ScalarVec<TrackedAggScalarValue<A>, A>;

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct ValuesVisitor<'a, A: ExactPercentileAllocator> {
            allocator: &'a A,
            refusal: &'a DecodeRefusal,
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
                    values.try_reserve(1).map_err(|source| {
                        self.refusal
                            .serde_error(self.allocator.scalar_allocation_error(
                                "reserve decoded exact percentile value",
                                source,
                            ))
                    })?;
                    let value = match value {
                        BorrowedSerializableScalar::Int64(value) => {
                            TrackedAggScalarValue::Int64(value)
                        }
                        BorrowedSerializableScalar::Float64(value) => {
                            TrackedAggScalarValue::Float64(value)
                        }
                        BorrowedSerializableScalar::Utf8(value) => TrackedAggScalarValue::Utf8(
                            scalar_bytes(self.allocator.clone(), value.as_bytes())
                                .map_err(|source| self.refusal.serde_error(source))?,
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
            refusal: self.refusal,
        })
    }
}

struct StateSeed<'a, A: ExactPercentileAllocator> {
    allocator: &'a A,
    refusal: &'a DecodeRefusal,
}

impl<'de, A: ExactPercentileAllocator> DeserializeSeed<'de> for StateSeed<'_, A> {
    type Value = (Option<f64>, ScalarVec<TrackedAggScalarValue<A>, A>);

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct StateVisitor<'a, A: ExactPercentileAllocator> {
            allocator: &'a A,
            refusal: &'a DecodeRefusal,
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
                                refusal: self.refusal,
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
                refusal: self.refusal,
            },
        )
    }
}

pub fn decode_state_into<A: ExactPercentileAllocator>(
    payload: &[u8],
    allocator: &A,
) -> Result<DecodedExactPercentileState<A>, crate::KernelFailure> {
    decode_state_into_with_sink(payload, allocator, &mut RequiredPercentileFailure)
}
pub fn decode_state_into_with_sink<A: ExactPercentileAllocator, S: PercentileFailureSink<A>>(
    payload: &[u8],
    allocator: &A,
    sink: &mut S,
) -> Result<DecodedExactPercentileState<A>, S::Error> {
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
    let scratch_bound = crate::exact_percentile_failure::byte_extent(2 * payload.len() as u128)
        .map_err(|source| sink.scalar(source))?;
    let _scratch = allocator
        .reserve_percentile_transient(
            scratch_bound,
            "reserve exact percentile JSON parser scratch",
        )
        .map_err(|error| sink.scalar(error))?;
    let mut deserializer = serde_json::Deserializer::from_slice(&payload[2..]);
    let refusal = DecodeRefusal::default();
    let result = StateSeed {
        allocator,
        refusal: &refusal,
    }
    .deserialize(&mut deserializer);
    match refusal.take() {
        Some(source) => Err(sink.scalar(source)),
        None => result.map_err(|error| sink.json(error, allocator)),
    }
}

pub fn apply_rate<A: ExactPercentileAllocator>(
    state: &mut ExactPercentileState<A>,
    rate: f64,
) -> Result<(), crate::KernelFailure> {
    apply_rate_with_sink(state, rate, &mut RequiredPercentileFailure)
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
) -> Result<TrackedAggScalarValue<A>, crate::KernelFailure> {
    validate_exact_scalar_with_sink(value, &mut RequiredPercentileFailure)
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
) -> Result<(), crate::KernelFailure> {
    update_from_arrays_with_sink(
        state,
        values,
        value_row,
        rates,
        rate_row,
        &mut ScalarWork::new(None),
        &mut RequiredPercentileFailure,
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
    if let Some(rate) = numeric_value_at_with_failure(rates, rate_row, &mut |source| {
        sink.reader(source.into_failure(PercentileInputDiagnostic::ExactUpdate))
    })? {
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
) -> Result<(), crate::KernelFailure> {
    merge_from_array_with_sink(
        state,
        array,
        row,
        diagnostic,
        &mut ScalarWork::new(None),
        &mut RequiredPercentileFailure,
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
    let Some(payload) = payload_bytes_at_with_failure(array, row, &mut |source| {
        sink.reader(source.into_failure(context))
    })?
    else {
        return Ok(());
    };
    work.flush().map_err(|error| sink.scalar(error))?;
    let (rate, incoming) = decode_state_into_with_sink(payload, &state.allocator, sink)?;
    if let Some(rate) = rate {
        apply_rate_with_sink(state, rate, sink)?;
    }
    work.flush().map_err(|error| sink.scalar(error))?;
    state.values.try_reserve(incoming.len()).map_err(|source| {
        sink.scalar(
            state
                .allocator
                .scalar_allocation_error("reserve merged exact percentile values", source),
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
) -> Result<Option<AggScalarValue>, crate::KernelFailure> {
    finalize_cont_with_sink(
        state,
        output_type,
        &mut ScalarWork::new(None),
        &mut RequiredPercentileFailure,
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
    sort_scalar_values(&mut values).map_err(|cause| sink.scalar(cause))?;
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
) -> Result<Option<AggScalarValue>, crate::KernelFailure> {
    finalize_disc_with_sink(
        state,
        &mut ScalarWork::new(None),
        &mut RequiredPercentileFailure,
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
    sort_scalar_values(&mut values).map_err(|cause| sink.scalar(cause))?;
    work.flush().map_err(|error| sink.scalar(error))?;
    if values.len() == 1 || rate == 1.0 {
        return Ok(values.last().cloned());
    }
    let index = (((values.len() - 1) as f64) * rate).ceil() as usize;
    Ok(values.get(index).cloned())
}

#[cfg(test)]
mod refusal_journal_tests {
    use super::*;
    use crate::{KernelDiagnostic, KernelFailure};
    use allocator_api2::alloc::{AllocError, Allocator, Global};
    use std::{
        alloc::Layout,
        ptr::NonNull,
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
    };
    /// This causal probe deliberately has no host error journal. Only the
    /// real failing allocation's synchronous continuation knows its cause.
    #[derive(Clone, Debug)]
    struct AllocatorProbe {
        stop_at: usize,
        source: KernelFailure,
        attempts: Arc<AtomicUsize>,
        releases: Arc<AtomicUsize>,
    }
    unsafe impl Allocator for AllocatorProbe {
        fn allocate(&self, layout: Layout) -> Result<NonNull<[u8]>, AllocError> {
            let at = self.attempts.fetch_add(1, Ordering::SeqCst);
            assert!(
                at <= self.stop_at,
                "allocation continued after its first refusal"
            );
            if at == self.stop_at {
                return Err(AllocError);
            }
            Global.allocate(layout)
        }
        unsafe fn deallocate(&self, pointer: NonNull<u8>, layout: Layout) {
            self.releases.fetch_add(1, Ordering::SeqCst);
            unsafe { Global.deallocate(pointer, layout) };
        }
    }
    impl ScalarStateAllocator for AllocatorProbe {
        fn scalar_allocation_error<E: crate::allocation_failure::AllocationRefusalSource>(
            &self,
            _: &str,
            _source: E,
        ) -> crate::KernelFailure {
            self.source.clone()
        }
    }
    impl ExactPercentileAllocator for AllocatorProbe {
        type ParserReservation = ();
        fn reserve_percentile_transient(&self, _: usize, _: &str) -> Result<(), KernelFailure> {
            Ok(())
        }
    }
    struct SourceSink;
    impl PercentileFailureSink<AllocatorProbe> for SourceSink {
        type Error = KernelFailure;
        fn scalar(&mut self, source: KernelFailure) -> KernelFailure {
            source
        }
        fn data(&mut self, recipe: PercentileDataRecipe<'_, AllocatorProbe>) -> KernelFailure {
            panic!("allocation refusal was converted to data: {recipe}")
        }
        fn reader(&mut self, message: KernelFailure) -> KernelFailure {
            panic!("allocation refusal was converted to reader text: {message}")
        }
        fn json(&mut self, error: serde_json::Error, _: &AllocatorProbe) -> KernelFailure {
            panic!("allocation refusal escaped through serde text: {error}")
        }
    }
    #[test]
    fn serde_refusals_keep_each_original_cause_without_an_allocator_side_channel() {
        use novarocks_failure_contract::{FailureProjection, FailureReason, domain};
        let sources = [
            KernelFailure::Cancelled,
            KernelFailure::DeadlineExceeded,
            KernelFailure::ResourceExhausted,
            KernelFailure::InvalidProgram(KernelDiagnostic::new("original plan refusal")),
            KernelFailure::Internal(KernelDiagnostic::new("original invariant refusal")),
            KernelFailure::Operational(KernelDiagnostic::new("original unexpected refusal")),
            KernelFailure::InstanceFailed,
            KernelFailure::Capacity(crate::KernelCapacityRefusal::new(
                FailureReason::new(domain::MEMORY, std::num::NonZeroU16::new(8).unwrap()),
                128,
                64,
                128,
            )),
        ];
        let mut payload = vec![EXACT_PERCENTILE_MAGIC, EXACT_PERCENTILE_VERSION];
        payload.extend_from_slice(br#"{"rate":0.5,"values":[{"Utf8":"hello"}]}"#);
        // First the values vector, then its owned UTF8 value. Both actual
        // allocation sites must unwind through the same typed decode slot.
        for stop_at in 0..2 {
            for source in &sources {
                let allocator = AllocatorProbe {
                    stop_at,
                    source: source.clone(),
                    attempts: Arc::new(AtomicUsize::new(0)),
                    releases: Arc::new(AtomicUsize::new(0)),
                };
                let error =
                    decode_state_into_with_sink(&payload, &allocator, &mut SourceSink).unwrap_err();
                assert_eq!(&error, source);
                assert_eq!(error.failure_fact(), source.failure_fact());
                assert_eq!(allocator.attempts.load(Ordering::SeqCst), stop_at + 1);
                assert_eq!(allocator.releases.load(Ordering::SeqCst), stop_at);
            }
        }
    }
    fn source_chain_has<T: std::error::Error + 'static>(cause: &KernelFailure) -> bool {
        let mut source: &(dyn std::error::Error + 'static) = cause;
        loop {
            if source.is::<T>() {
                return true;
            }
            let Some(next) = source.source() else {
                return false;
            };
            source = next;
        }
    }
    #[test]
    fn exact_percentile_codec_keeps_real_serde_sources_and_distinct_failure_facts() {
        use novarocks_failure_contract::{FailureClass, FailureProjection};
        let allocator = AllocatorProbe {
            stop_at: usize::MAX,
            source: KernelFailure::Cancelled,
            attempts: Arc::new(AtomicUsize::new(0)),
            releases: Arc::new(AtomicUsize::new(0)),
        };
        let payload = [EXACT_PERCENTILE_MAGIC, EXACT_PERCENTILE_VERSION, b'{'];
        let cause = decode_state_into(&payload, &allocator).unwrap_err();
        assert_eq!(
            cause.failure_fact(),
            crate::DataFailureReason::InvalidEncoding.failure_fact()
        );
        assert!(source_chain_has::<serde_json::Error>(&cause));
        let mut state = ExactPercentileState::new(allocator.clone());
        state.values.push(TrackedAggScalarValue::Bool(true));
        let cause = encode_state(&state).unwrap_err();
        assert_eq!(cause.failure_fact().class(), FailureClass::Invariant);
        assert!(source_chain_has::<serde_json::Error>(&cause));
        drop(state);
        assert_eq!(
            allocator.attempts.load(Ordering::SeqCst),
            allocator.releases.load(Ordering::SeqCst)
        );
        let cause =
            crate::exact_percentile_failure::byte_extent(isize::MAX as u128 + 1).unwrap_err();
        assert_eq!(cause.failure_fact().class(), FailureClass::Representation);
        let facts = cause.failure_fact().representation().unwrap();
        assert_eq!(facts.required, Some(isize::MAX as u64 + 1));
        assert_eq!(facts.extent_limit, Some(isize::MAX as u64));
        assert_eq!(facts.representable_prefix, None);
        assert!(source_chain_has::<std::alloc::LayoutError>(&cause));
    }
}
