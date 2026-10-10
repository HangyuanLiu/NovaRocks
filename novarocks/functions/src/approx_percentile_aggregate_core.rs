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

//! Original already-evaluated approximate-percentile aggregate row consumers.
//! The diagnostic enum selects original text only; no function-name dispatch.
use crate::aggregate_scalar::AggScalarValue;
use crate::approx_percentile_core as percentile;
use crate::approx_percentile_failure::{
    ApproxPercentileDataRecipe as DataRecipe, ApproxPercentileFailureSink,
    ApproxPercentileObservation as Observation, LegacyApproxPercentileFailure,
};
use crate::percentile_input::{self, PercentileInputDiagnostic};
use allocator_api2::alloc::Allocator;
/// Storage policy only: the row calculation retains its single original body.
pub trait QuantileBuffer {
    fn push(&mut self, value: f64);
    fn values(&self) -> &[f64];
}
impl QuantileBuffer for Vec<f64> {
    fn push(&mut self, value: f64) {
        Vec::push(self, value);
    }
    fn values(&self) -> &[f64] {
        self.as_slice()
    }
}
impl<A: Allocator> QuantileBuffer for allocator_api2::vec::Vec<f64, A> {
    fn push(&mut self, value: f64) {
        Self::push(self, value);
    }
    fn values(&self) -> &[f64] {
        self.as_slice()
    }
}
pub trait QuantileScratch {
    type Buffer: QuantileBuffer;
    fn prepare(&self, count: usize) -> Result<Self::Buffer, ()>;
}
pub struct LegacyQuantileScratch;
impl QuantileScratch for LegacyQuantileScratch {
    type Buffer = Vec<f64>;
    fn prepare(&self, count: usize) -> Result<Self::Buffer, ()> {
        let mut out = Vec::new();
        out.try_reserve_exact(count).map_err(|_| ())?;
        Ok(out)
    }
}
pub struct AllocatedQuantileScratch<A: Allocator + Clone>(pub A);
impl<A: Allocator + Clone> QuantileScratch for AllocatedQuantileScratch<A> {
    type Buffer = allocator_api2::vec::Vec<f64, A>;
    fn prepare(&self, count: usize) -> Result<Self::Buffer, ()> {
        let mut out = allocator_api2::vec::Vec::new_in(self.0.clone());
        out.try_reserve_exact(count).map_err(|_| ())?;
        Ok(out)
    }
}
fn legacy_data<T, S: ApproxPercentileFailureSink>(
    result: Result<T, String>,
    sink: &mut S,
) -> Result<T, S::Error> {
    result.map_err(|text| sink.data(DataRecipe::Existing(&text)))
}
use arrow_array::{Array, ArrayRef, ListArray, StructArray};
use arrow_schema::DataType;

#[derive(Clone, Copy, Debug)]
pub enum ApproxPercentileDiagnostic {
    UnweightedUpdate,
    WeightedUpdate,
    UnweightedMerge,
    WeightedMerge,
}
impl ApproxPercentileDiagnostic {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::UnweightedUpdate => "percentile_approx",
            Self::WeightedUpdate => "percentile_approx_weighted",
            Self::UnweightedMerge => "percentile_approx_merge",
            Self::WeightedMerge => "percentile_approx_weighted_merge",
        }
    }
}
impl std::fmt::Display for ApproxPercentileDiagnostic {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}
fn numeric_value_at(
    array: &ArrayRef,
    row: usize,
    context: ApproxPercentileDiagnostic,
) -> Result<Option<f64>, String> {
    percentile_input::numeric_value_at(
        array,
        row,
        PercentileInputDiagnostic::LegacyLabel(context.as_str()),
    )
}
fn payload_bytes_at<'a>(
    array: &'a ArrayRef,
    row: usize,
    context: ApproxPercentileDiagnostic,
) -> Result<Option<&'a [u8]>, String> {
    percentile_input::payload_bytes_at(
        array,
        row,
        PercentileInputDiagnostic::LegacyLabel(context.as_str()),
    )
}
fn integer_value_at(
    array: &ArrayRef,
    row: usize,
    context: ApproxPercentileDiagnostic,
) -> Result<Option<i64>, String> {
    Ok(numeric_value_at(array, row, context)?.map(|value| value as i64))
}
fn validate_quantile(context: ApproxPercentileDiagnostic, quantile: f64) -> Result<(), String> {
    if !(0.0..=1.0).contains(&quantile) {
        return Err(format!(
            "{context}: percentile parameter must be between 0 and 1, got {}",
            quantile
        ));
    }
    Ok(())
}

fn apply_quantiles<A: Allocator + Clone>(
    state: &mut percentile::PercentileState<A>,
    array: &ArrayRef,
    row: usize,
    context: ApproxPercentileDiagnostic,
) -> Result<(), String> {
    apply_quantiles_with_sink(
        state,
        array,
        row,
        context,
        &LegacyQuantileScratch,
        &mut LegacyApproxPercentileFailure,
    )
}
fn apply_quantiles_with_sink<
    A: Allocator + Clone,
    S: ApproxPercentileFailureSink,
    Q: QuantileScratch,
>(
    state: &mut percentile::PercentileState<A>,
    array: &ArrayRef,
    row: usize,
    context: ApproxPercentileDiagnostic,
    scratch: &Q,
    sink: &mut S,
) -> Result<(), S::Error> {
    if matches!(array.data_type(), DataType::List(_)) {
        let list = array.as_any().downcast_ref::<ListArray>().ok_or_else(|| {
            sink.data(DataRecipe::Arguments(format_args!(
                "{context}: failed to downcast percentile array input"
            )))
        })?;
        if list.is_null(row) {
            return Ok(());
        }
        let offsets = list.value_offsets();
        let start = offsets[row] as usize;
        let end = offsets[row + 1] as usize;
        let values = list.values();
        let count = end.saturating_sub(start);
        if count > percentile::MAX_QUANTILE_COUNT {
            return Err(sink.data(DataRecipe::Arguments(format_args!(
                "{context}: percentile quantile count {count} exceeds {}",
                percentile::MAX_QUANTILE_COUNT
            ))));
        }
        let mut quantiles = scratch.prepare(count).map_err(|_| {
            sink.allocation(DataRecipe::Arguments(format_args!(
                "ResourceExhausted: {context} quantile array"
            )))
        })?;
        for (idx, value_row) in (start..end).enumerate() {
            sink.observe(Observation::Step)?;
            let Some(q) = legacy_data(numeric_value_at(values, value_row, context), sink)? else {
                return Err(sink.data(DataRecipe::Arguments(format_args!(
                    "{context}: percentile array element[{idx}] cannot be null"
                ))));
            };
            legacy_data(validate_quantile(context, q), sink)?;
            quantiles.push(q);
        }
        return percentile::set_quantiles_with_sink(state, quantiles.values(), sink);
    }
    match legacy_data(numeric_value_at(array, row, context), sink)? {
        Some(q) => {
            legacy_data(validate_quantile(context, q), sink)?;
            percentile::set_quantile_with_sink(state, q, sink)
        }
        None => Ok(()),
    }
}

fn apply_compression<A: Allocator + Clone>(
    state: &mut percentile::PercentileState<A>,
    array: &ArrayRef,
    row: usize,
    context: ApproxPercentileDiagnostic,
) -> Result<(), String> {
    apply_compression_with_sink(
        state,
        array,
        row,
        context,
        &mut LegacyApproxPercentileFailure,
    )
}
fn apply_compression_with_sink<A: Allocator + Clone, S: ApproxPercentileFailureSink>(
    state: &mut percentile::PercentileState<A>,
    array: &ArrayRef,
    row: usize,
    context: ApproxPercentileDiagnostic,
    sink: &mut S,
) -> Result<(), S::Error> {
    if let Some(value) = legacy_data(numeric_value_at(array, row, context), sink)? {
        percentile::set_compression_with_sink(state, value, sink)?;
    }
    Ok(())
}

pub struct UnweightedInput {
    values: ArrayRef,
    quantiles: ArrayRef,
    compression: Option<ArrayRef>,
}
impl UnweightedInput {
    pub fn try_new(
        array: &StructArray,
        context: ApproxPercentileDiagnostic,
    ) -> Result<Self, String> {
        let fields = array.columns();
        if fields.len() < 2 {
            return Err(format!(
                "{context}: percentile_approx expects STRUCT(value, quantile[, compression]) input"
            ));
        }
        let values = fields[0].clone();
        let quantiles = fields[1].clone();
        let compression = if fields.len() >= 3 {
            Some(fields[2].clone())
        } else {
            None
        };
        Ok(Self {
            values,
            quantiles,
            compression,
        })
    }
    pub fn update_row<A: Allocator + Clone>(
        &self,
        state: &mut percentile::PercentileState<A>,
        row: usize,
        context: ApproxPercentileDiagnostic,
    ) -> Result<(), String> {
        self.update_addresses(state, row, row, row, context)
    }
    /// Borrow the same original already-evaluated channels, without building
    /// a second packed Struct or changing their constant/selected addresses.
    pub fn from_arguments(
        values: ArrayRef,
        quantiles: ArrayRef,
        compression: Option<ArrayRef>,
    ) -> Self {
        Self {
            values,
            quantiles,
            compression,
        }
    }
    pub fn update_addresses<A: Allocator + Clone>(
        &self,
        state: &mut percentile::PercentileState<A>,
        value_row: usize,
        quantile_row: usize,
        compression_row: usize,
        context: ApproxPercentileDiagnostic,
    ) -> Result<(), String> {
        self.update_addresses_with_sink(
            state,
            value_row,
            quantile_row,
            compression_row,
            context,
            &LegacyQuantileScratch,
            &mut LegacyApproxPercentileFailure,
        )
    }
    pub fn update_addresses_with_sink<
        A: Allocator + Clone,
        S: ApproxPercentileFailureSink,
        Q: QuantileScratch,
    >(
        &self,
        state: &mut percentile::PercentileState<A>,
        value_row: usize,
        quantile_row: usize,
        compression_row: usize,
        context: ApproxPercentileDiagnostic,
        scratch: &Q,
        sink: &mut S,
    ) -> Result<(), S::Error> {
        let values = &self.values;
        let quantiles = &self.quantiles;
        let compression = &self.compression;
        apply_quantiles_with_sink(state, quantiles, quantile_row, context, scratch, sink)?;
        if let Some(compression) = compression {
            apply_compression_with_sink(state, compression, compression_row, context, sink)?;
        }
        match values.data_type() {
            DataType::Binary | DataType::Utf8 | DataType::LargeBinary | DataType::LargeUtf8 => {
                if let Some(payload) =
                    legacy_data(payload_bytes_at(values, value_row, context), sink)?
                {
                    percentile::merge_bounded_serialized_state_into_with_sink(
                        state, payload, sink,
                    )?;
                }
            }
            _ => {
                if let Some(value) =
                    legacy_data(numeric_value_at(values, value_row, context), sink)?
                {
                    percentile::add_value_with_sink(state, value, sink)?;
                    percentile::validate_state_with_sink(state, sink)?;
                }
            }
        }
        Ok(())
    }
}

pub struct WeightedInput {
    values: ArrayRef,
    weights: ArrayRef,
    quantiles: ArrayRef,
    compression: Option<ArrayRef>,
}
impl WeightedInput {
    pub fn try_new(
        array: &StructArray,
        context: ApproxPercentileDiagnostic,
    ) -> Result<Self, String> {
        let fields = array.columns();
        if fields.len() < 3 {
            return Err(format!(
                "{context}: percentile_approx_weighted expects STRUCT(value, weight, quantile[, compression]) input"
            ));
        }
        let values = fields[0].clone();
        let weights = fields[1].clone();
        let quantiles = fields[2].clone();
        let compression = if fields.len() >= 4 {
            Some(fields[3].clone())
        } else {
            None
        };
        Ok(Self {
            values,
            weights,
            quantiles,
            compression,
        })
    }
    pub fn update_row<A: Allocator + Clone>(
        &self,
        state: &mut percentile::PercentileState<A>,
        row: usize,
        context: ApproxPercentileDiagnostic,
    ) -> Result<(), String> {
        self.update_addresses(state, row, row, row, row, context)
    }
    /// Borrow the same original already-evaluated channels, without building
    /// a second packed Struct or changing their constant/selected addresses.
    pub fn from_arguments(
        values: ArrayRef,
        weights: ArrayRef,
        quantiles: ArrayRef,
        compression: Option<ArrayRef>,
    ) -> Self {
        Self {
            values,
            weights,
            quantiles,
            compression,
        }
    }
    pub fn update_addresses<A: Allocator + Clone>(
        &self,
        state: &mut percentile::PercentileState<A>,
        value_row: usize,
        weight_row: usize,
        quantile_row: usize,
        compression_row: usize,
        context: ApproxPercentileDiagnostic,
    ) -> Result<(), String> {
        self.update_addresses_with_sink(
            state,
            value_row,
            weight_row,
            quantile_row,
            compression_row,
            context,
            &LegacyQuantileScratch,
            &mut LegacyApproxPercentileFailure,
        )
    }
    pub fn update_addresses_with_sink<
        A: Allocator + Clone,
        S: ApproxPercentileFailureSink,
        Q: QuantileScratch,
    >(
        &self,
        state: &mut percentile::PercentileState<A>,
        value_row: usize,
        weight_row: usize,
        quantile_row: usize,
        compression_row: usize,
        context: ApproxPercentileDiagnostic,
        scratch: &Q,
        sink: &mut S,
    ) -> Result<(), S::Error> {
        let values = &self.values;
        let weights = &self.weights;
        let quantiles = &self.quantiles;
        let compression = &self.compression;
        apply_quantiles_with_sink(state, quantiles, quantile_row, context, scratch, sink)?;
        if let Some(compression) = compression {
            apply_compression_with_sink(state, compression, compression_row, context, sink)?;
        }
        let Some(value) = legacy_data(numeric_value_at(values, value_row, context), sink)? else {
            return Ok(());
        };
        let weight =
            legacy_data(integer_value_at(weights, weight_row, context), sink)?.unwrap_or_default();
        if weight < 0 {
            return Err(sink.data(DataRecipe::Arguments(format_args!(
                "{context}: percentile weight must be non-negative, got {}",
                weight
            ))));
        }
        percentile::add_weighted_value_with_sink(state, value, weight, sink)?;
        percentile::validate_state_with_sink(state, sink)?;
        Ok(())
    }
}

pub fn payload_for_merge<'a>(
    array: &'a ArrayRef,
    row: usize,
    context: ApproxPercentileDiagnostic,
) -> Result<Option<&'a [u8]>, String> {
    payload_bytes_at(array, row, context)
}
pub fn merge_payload<A: Allocator + Clone>(
    state: &mut percentile::PercentileState<A>,
    payload: &[u8],
) -> Result<(), String> {
    percentile::merge_bounded_serialized_state_into(state, payload)
}
pub fn merge_row<A: Allocator + Clone>(
    state: &mut percentile::PercentileState<A>,
    array: &ArrayRef,
    row: usize,
    context: ApproxPercentileDiagnostic,
) -> Result<(), String> {
    merge_row_with_sink(
        state,
        array,
        row,
        context,
        &mut LegacyApproxPercentileFailure,
    )
}
pub fn merge_row_with_sink<A: Allocator + Clone, S: ApproxPercentileFailureSink>(
    state: &mut percentile::PercentileState<A>,
    array: &ArrayRef,
    row: usize,
    context: ApproxPercentileDiagnostic,
    sink: &mut S,
) -> Result<(), S::Error> {
    let Some(payload) = legacy_data(payload_for_merge(array, row, context), sink)? else {
        return Ok(());
    };
    percentile::merge_bounded_serialized_state_into_with_sink(state, payload, sink)
}

#[derive(Clone, Copy, Debug)]
pub enum ScalarOutput {
    Float64,
    List,
}
/// Original projection into the shared aggregate scalar output builder.
pub fn scalar_output<A: Allocator + Clone>(
    state: &percentile::PercentileState<A>,
    output: ScalarOutput,
) -> Result<Option<AggScalarValue>, String> {
    scalar_output_with_policy(state, output, &mut percentile::OriginalDerivedClone)
}
/// Allocation policy is fixed by the actual v1/pure consumer, not inferred
/// from the value, function name or returned scalar type.
pub fn scalar_output_with_policy<A: Allocator + Clone, P: percentile::TDigestClonePolicy<A>>(
    state: &percentile::PercentileState<A>,
    output: ScalarOutput,
    policy: &mut P,
) -> Result<Option<AggScalarValue>, String> {
    match output {
        ScalarOutput::Float64 => Ok(percentile::quantile_from_state_with_policy(
            state, None, policy,
        )?
        .map(AggScalarValue::Float64)),
        ScalarOutput::List => Ok(
            percentile::quantiles_from_state_with_policy(state, policy)?.map(|items| {
                AggScalarValue::List(
                    items
                        .into_iter()
                        .map(|item| Some(AggScalarValue::Float64(item)))
                        .collect(),
                )
            }),
        ),
    }
}

#[cfg(test)]
#[path = "approx_percentile_aggregate_core_tests.rs"]
mod tests;
