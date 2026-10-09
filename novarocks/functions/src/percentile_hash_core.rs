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
//! Original scalar percentile_hash row policy, with shared numeric reader/TDigest codec.
//! No expression, argument demand, function-name dispatch or ambient authority lives here.
use crate::percentile_input::{PercentileInputDiagnostic, numeric_value_at};
use arrow_array::ArrayRef;
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Observation {
    ReadBoundary,
    EncodeBoundary,
    Step,
}
/// Original SOME -> singleton and numeric NULL -> empty-state encoding.
/// Existing TDigest/codec authors retain f32 rounding, NaN/signedzero/inf and
/// infallible raw allocation behavior. These opaque allocations are not grants.
pub trait ValueEncoder {
    type Error;
    fn single(&mut self, value: f64) -> Result<Vec<u8>, Self::Error>;
    fn empty(&mut self) -> Result<Vec<u8>, Self::Error>;
}
/// The ONE original Some/None encoding choice, with a narrow allocation port.
pub fn encode_numeric_with<E: ValueEncoder>(
    value: Option<f64>,
    encoder: &mut E,
) -> Result<Vec<u8>, E::Error> {
    match value {
        Some(value) => encoder.single(value),
        None => encoder.empty(),
    }
}
struct OriginalEncoder<E>(std::marker::PhantomData<E>);
impl<E> ValueEncoder for OriginalEncoder<E> {
    type Error = E;
    fn single(&mut self, value: f64) -> Result<Vec<u8>, E> {
        Ok(crate::approx_percentile_core::encode_single_value(value))
    }
    fn empty(&mut self) -> Result<Vec<u8>, E> {
        Ok(crate::approx_percentile_core::encode_empty_state())
    }
}
pub fn encode_numeric(value: Option<f64>) -> Vec<u8> {
    match encode_numeric_with(
        value,
        &mut OriginalEncoder::<std::convert::Infallible>(std::marker::PhantomData),
    ) {
        Ok(value) => value,
        Err(impossible) => match impossible {},
    }
}
pub fn row_with_encoder_observed<E, Encoder: ValueEncoder<Error = E>>(
    array: &ArrayRef,
    row: usize,
    mut observe: impl FnMut(Observation) -> Result<(), E>,
    encoder: &mut Encoder,
) -> Result<Result<Vec<u8>, String>, E> {
    observe(Observation::ReadBoundary)?;
    let value = match numeric_value_at(array, row, PercentileInputDiagnostic::Hash) {
        Ok(value) => value,
        Err(error) => return Ok(Err(error)),
    };
    observe(Observation::ReadBoundary)?;
    observe(Observation::Step)?;
    observe(Observation::EncodeBoundary)?;
    let encoded = encode_numeric_with(value, encoder)?;
    observe(Observation::EncodeBoundary)?;
    Ok(Ok(encoded))
}
pub fn row_observed<E>(
    array: &ArrayRef,
    row: usize,
    observe: impl FnMut(Observation) -> Result<(), E>,
) -> Result<Result<Vec<u8>, String>, E> {
    row_with_encoder_observed(
        array,
        row,
        observe,
        &mut OriginalEncoder::<E>(std::marker::PhantomData),
    )
}
pub fn row(array: &ArrayRef, row: usize) -> Result<Vec<u8>, String> {
    match row_observed(array, row, |_| Ok::<_, std::convert::Infallible>(())) {
        Ok(value) => value,
        Err(impossible) => match impossible {},
    }
}
#[cfg(test)]
#[path = "percentile_hash_core_tests.rs"]
mod tests;
