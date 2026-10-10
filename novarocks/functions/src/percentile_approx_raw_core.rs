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
//! Original PERCENTILE_APPROX_RAW per-row combination of the ONE readers and TDigest.
//! No expression, argument demand, name dispatch or ambient host lives here.
use crate::percentile_input::{
    PercentileInputDiagnostic, PercentileNumericFailure, PercentilePayloadFailure,
    numeric_value_at_with_failure, payload_bytes_at_with_failure,
};
use arrow_array::{ArrayRef, builder::Float64Builder};
use std::sync::Arc;
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Observation {
    ReadBoundary,
    DecodeBoundary,
    Step,
}
/// A narrow execution port for the shared original decode/clone/quantile author.
/// The outer Result is host/control; the inner Result is original semantic Data.
pub trait QuantileEvaluator {
    type Error;
    fn payload_failure(
        &mut self,
        failure: PercentilePayloadFailure<'_>,
    ) -> Result<String, Self::Error> {
        Ok(failure
            .message(PercentileInputDiagnostic::ApproxRaw)
            .to_string())
    }
    fn numeric_failure(
        &mut self,
        failure: PercentileNumericFailure<'_>,
    ) -> Result<String, Self::Error> {
        Ok(failure
            .message(PercentileInputDiagnostic::ApproxRaw)
            .to_string())
    }

    fn quantile(
        &mut self,
        payload: &[u8],
        quantile: f64,
    ) -> Result<Result<Option<f64>, String>, Self::Error>;
}
pub struct OriginalQuantileEvaluator;
impl QuantileEvaluator for OriginalQuantileEvaluator {
    type Error = std::convert::Infallible;
    fn quantile(
        &mut self,
        payload: &[u8],
        quantile: f64,
    ) -> Result<Result<Option<f64>, String>, Self::Error> {
        Ok((|| {
            let state = crate::approx_percentile_core::decode_state(payload)?;
            crate::approx_percentile_core::quantile_from_state(&state, Some(quantile))
        })())
    }
}
/// Distinct actual addresses support the original Column and selected constant carriers.
/// Payload admission precedes quantile admission, even when payload is SQL NULL.
pub fn row_with_evaluator_observed<E: QuantileEvaluator>(
    payloads: &ArrayRef,
    payload_row: usize,
    quantiles: &ArrayRef,
    quantile_row: usize,
    mut observe: impl FnMut(Observation) -> Result<(), E::Error>,
    evaluator: &mut E,
) -> Result<Result<Option<f64>, String>, E::Error> {
    observe(Observation::ReadBoundary)?;
    let payload = match payload_bytes_at_with_failure(payloads, payload_row, &mut |failure| {
        evaluator.payload_failure(failure)
    }) {
        Ok(value) => value,
        Err(result) => return result.map(Err),
    };
    let quantile = match numeric_value_at_with_failure(quantiles, quantile_row, &mut |failure| {
        evaluator.numeric_failure(failure)
    }) {
        Ok(value) => value,
        Err(result) => return result.map(Err),
    };
    observe(Observation::ReadBoundary)?;
    observe(Observation::Step)?;
    match (payload, quantile) {
        (Some(payload), Some(quantile)) => {
            observe(Observation::DecodeBoundary)?;
            let result = evaluator.quantile(payload, quantile)?;
            // Original first Data has no subsequent optional observation.
            if result.is_ok() {
                observe(Observation::DecodeBoundary)?;
            }
            Ok(result)
        }
        _ => Ok(Ok(None)),
    }
}
pub fn row(payloads: &ArrayRef, row: usize, quantiles: &ArrayRef) -> Result<Option<f64>, String> {
    match row_with_evaluator_observed(
        payloads,
        row,
        quantiles,
        row,
        |_| Ok(()),
        &mut OriginalQuantileEvaluator,
    ) {
        Ok(value) => value,
        Err(impossible) => match impossible {},
    }
}
/// The original batch builder allocation, traversal order and NULL policy.
pub fn evaluate_legacy(payloads: &ArrayRef, quantiles: &ArrayRef) -> Result<ArrayRef, String> {
    let mut builder = Float64Builder::with_capacity(payloads.len());
    for index in 0..payloads.len() {
        match row(payloads, index, quantiles)? {
            Some(value) => builder.append_value(value),
            None => builder.append_null(),
        }
    }
    Ok(Arc::new(builder.finish()) as ArrayRef)
}
