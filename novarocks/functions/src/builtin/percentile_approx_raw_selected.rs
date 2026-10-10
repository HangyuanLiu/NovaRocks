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

//! Full ANY2 selected consumer of the original shared percentile computation.
use crate::aggregate_host_allocator::HostAggregateAllocator;
use crate::approx_percentile_core::{self as core, PercentileDecodeStorage};
use crate::approx_percentile_failure::{
    ApproxPercentileDataRecipe, ApproxPercentileFailureSink, ApproxPercentileObservation,
};
use crate::kernel_control::{internal, invalid};
use crate::kernel_input::EvaluationCheckpoints;
use crate::opaque_memory::{OpaqueReservation, OpaqueRetainedCharge};
use crate::percentile_input::{
    PercentileInputDiagnostic, PercentileNumericFailure, PercentilePayloadFailure,
};
use crate::{
    AggregateStateAllocator, FunctionArgumentType, KernelEvaluationControl, KernelFailure,
    RowDataError, ScalarCallContract, ScalarCallInput, SelectedValues,
};
use allocator_api2::vec::Vec as HostVec;
use arrow_array::{Array, ArrayRef, builder::Float64Builder};
use arrow_schema::DataType;
use std::{alloc::Layout, fmt, sync::Arc};

enum RowFailure {
    Kernel(KernelFailure),
    Data(RowDataError),
}
struct Sink<'a, 'control> {
    ordinal: usize,
    allocator: &'a HostAggregateAllocator,
    control: &'control dyn KernelEvaluationControl,
    work: EvaluationCheckpoints<'control>,
    charge: OpaqueRetainedCharge,
    json: [Option<OpaqueReservation>; 2],
    json_count: usize,
    sort: Option<OpaqueReservation>,
}
impl Sink<'_, '_> {
    fn diagnostic(&mut self, value: impl fmt::Display) -> RowFailure {
        match RowDataError::prepare_host(self.ordinal, self.allocator, &mut self.work, |writer| {
            write!(writer, "{value}")
        }) {
            Ok(data) => RowFailure::Data(data),
            Err(cause) => RowFailure::Kernel(cause),
        }
    }
}
impl ApproxPercentileFailureSink for Sink<'_, '_> {
    type Error = RowFailure;
    fn data(&mut self, recipe: ApproxPercentileDataRecipe<'_>) -> RowFailure {
        self.diagnostic(recipe)
    }
    fn allocation(&mut self, _: ApproxPercentileDataRecipe<'_>) -> RowFailure {
        RowFailure::Kernel(
            self.allocator
                .take_recorded_failure()
                .unwrap_or(KernelFailure::ResourceExhausted),
        )
    }
    fn observe(&mut self, event: ApproxPercentileObservation) -> Result<(), RowFailure> {
        match event {
            ApproxPercentileObservation::Step => self.work.step().map_err(RowFailure::Kernel),
            ApproxPercentileObservation::SortBegin(len) => {
                self.work.flush().map_err(RowFailure::Kernel)?;
                let extent = Layout::array::<[f32; 2]>(len)
                    .map_err(|_| RowFailure::Kernel(KernelFailure::ResourceExhausted))?
                    .size();
                self.sort = Some(
                    self.charge
                        .reserve_operation(extent)
                        .map_err(RowFailure::Kernel)?,
                );
                Ok(())
            }
            ApproxPercentileObservation::SortEnd => {
                self.sort = None;
                self.control.checkpoint(0).map_err(RowFailure::Kernel)
            }
            ApproxPercentileObservation::JsonBegin(bytes) => {
                self.work.flush().map_err(RowFailure::Kernel)?;
                let extent = json_parse_extent(bytes)
                    .ok_or(RowFailure::Kernel(KernelFailure::ResourceExhausted))?;
                let slot = self.json.get_mut(self.json_count).ok_or_else(|| {
                    RowFailure::Kernel(internal(
                        "original percentile v3 performs more than two JSON decodes",
                    ))
                })?;
                *slot = Some(
                    self.charge
                        .reserve_operation(extent)
                        .map_err(RowFailure::Kernel)?,
                );
                self.json_count += 1;
                Ok(())
            }
            // The reservation pins parsed Global vectors until the same row's
            // actual conversion/drop. An exit observation never releases it early.
            ApproxPercentileObservation::JsonEnd => {
                self.control.checkpoint(0).map_err(RowFailure::Kernel)
            }
        }
    }
}
// Pinned serde_json 1.0.150 / serde_core 1.0.228 slice SeqAccess has no size_hint.
// The bound accounts geometric Vec growth, escape scratch, one original serde
// diagnostic including Debug escape growth, and the actual ErrorImpl box layout.
// It is an operation admission recipe, not a data-domain cap or a MEM grant.
fn json_parse_extent(bytes: usize) -> Option<usize> {
    let vector_elements = bytes
        .checked_mul(2)?
        .checked_mul(std::mem::size_of::<[f32; 2]>())?;
    let vector_initial = 3usize
        .checked_mul(4)?
        .checked_mul(std::mem::size_of::<[f32; 2]>())?;
    let scratch = bytes.checked_mul(3)?.checked_add(24)?;
    let message = bytes.checked_mul(6)?.checked_add(256)?;
    let message_overlap = message.checked_mul(3)?;
    let error_boxes = 2usize
        .checked_mul(6)?
        .checked_mul(std::mem::size_of::<usize>())?;
    vector_elements
        .checked_add(vector_initial)?
        .checked_add(scratch)?
        .checked_add(message_overlap)?
        .checked_add(error_boxes)
}
impl PercentileDecodeStorage for HostAggregateAllocator {
    fn collect_decoded<T: Copy, S: ApproxPercentileFailureSink>(
        &self,
        values: Vec<T>,
        sink: &mut S,
    ) -> Result<HostVec<T, Self>, S::Error> {
        let mut out = HostVec::new_in(self.clone());
        out.try_reserve_exact(values.len()).map_err(|_| {
            sink.allocation(ApproxPercentileDataRecipe::Static(
                "ResourceExhausted: reserve original decoded percentile values",
            ))
        })?;
        for value in values {
            sink.observe(ApproxPercentileObservation::Step)?;
            out.push(value);
        }
        Ok(out)
    }
}
impl crate::percentile_approx_raw_core::QuantileEvaluator for Sink<'_, '_> {
    type Error = RowFailure;
    fn payload_failure(
        &mut self,
        failure: PercentilePayloadFailure<'_>,
    ) -> Result<String, RowFailure> {
        Err(self.diagnostic(failure.message(PercentileInputDiagnostic::ApproxRaw)))
    }
    fn numeric_failure(
        &mut self,
        failure: PercentileNumericFailure<'_>,
    ) -> Result<String, RowFailure> {
        Err(self.diagnostic(failure.message(PercentileInputDiagnostic::ApproxRaw)))
    }
    fn quantile(
        &mut self,
        payload: &[u8],
        quantile: f64,
    ) -> Result<Result<Option<f64>, String>, RowFailure> {
        let state = core::decode_state_with_sink(payload, self.allocator.clone(), self)?;
        Ok(Ok(core::quantile_from_state_with_sink(
            &state,
            Some(quantile),
            &mut core::TrackedTDigestClone,
            self,
        )?))
    }
}
pub(super) fn validate_profile(
    contract: &ScalarCallContract,
    mut step: impl FnMut() -> Result<(), KernelFailure>,
) -> Result<(), KernelFailure> {
    if contract.selected().argument_types.len() != 2
        || contract.call().logical_argument_count() != 2
    {
        return Err(invalid(
            "percentile_approx_raw requires its actual two logical arguments",
        ));
    }
    for arg in &contract.selected().argument_types {
        step()?;
        if !matches!(arg, FunctionArgumentType::Value(_)) {
            return Err(invalid(
                "percentile_approx_raw requires exact value arguments",
            ));
        }
    }
    let result = contract.result_type();
    if result.data_type != DataType::Float64
        || !result.nullable
        || result.logical_type != novarocks_type_contract::ValueLogicalType::Physical
    {
        return Err(invalid(
            "percentile_approx_raw requires its exact nullable Physical Float64 result",
        ));
    }
    step()
}
pub(super) fn evaluate<'a>(
    input: ScalarCallInput<'_, 'a>,
    control: &dyn KernelEvaluationControl,
    allocator: &HostAggregateAllocator,
    host: &Arc<dyn AggregateStateAllocator>,
) -> Result<SelectedValues<'a>, KernelFailure> {
    control.checkpoint(0)?;
    let mut rows = EvaluationCheckpoints::new(control);
    let result = (|| {
        validate_profile(input.contract(), || rows.step())?;
        let [payloads, quantiles] = input.arguments() else {
            return Err(invalid(
                "percentile_approx_raw evaluated channel count differs from its checked call",
            ));
        };
        let selection = input.selection();
        let mut values = Float64Builder::with_capacity(selection.len());
        let mut errors = Vec::new();
        for (ordinal, batch_row) in selection.iter().enumerate() {
            let payload_row = payloads.value_row(ordinal, batch_row);
            let quantile_row = quantiles.value_row(ordinal, batch_row);
            for (arg, row) in [(payloads, payload_row), (quantiles, quantile_row)] {
                if row >= arg.array().len() {
                    return Err(internal(
                        "percentile_approx_raw demanded address exceeds its original carrier",
                    ));
                }
            }
            let mut sink = Sink {
                ordinal,
                allocator,
                control,
                work: EvaluationCheckpoints::new(control),
                charge: OpaqueRetainedCharge::try_new(Arc::clone(host))?,
                json: [None, None],
                json_count: 0,
                sort: None,
            };
            let output = crate::percentile_approx_raw_core::row_with_evaluator_observed(
                payloads.array(),
                payload_row,
                quantiles.array(),
                quantile_row,
                |event| match event {
                    crate::percentile_approx_raw_core::Observation::Step => {
                        rows.step().map_err(RowFailure::Kernel)
                    }
                    _ => control.checkpoint(0).map_err(RowFailure::Kernel),
                },
                &mut sink,
            );
            match output {
                Ok(Ok(value)) => {
                    match value {
                        Some(value) => values.append_value(value),
                        None => values.append_null(),
                    };
                    sink.work.finish()?;
                }
                Err(RowFailure::Data(data)) => {
                    values.append_null();
                    errors.push(data);
                }
                Err(RowFailure::Kernel(cause)) => return Err(cause),
                Ok(Err(_)) => {
                    return Err(internal(
                        "percentile_approx_raw host evaluator returned an unhosted diagnostic",
                    ));
                }
            }
        }
        rows.flush()?;
        let array = Arc::new(values.finish()) as ArrayRef;
        SelectedValues::try_new_observed::<KernelFailure>(
            selection,
            &input.contract().result_type().data_type,
            array,
            errors.into_boxed_slice(),
            || rows.step(),
        )
    })();
    if result.is_ok() {
        rows.finish()?;
    }
    result
}
