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

//! Exact selected adapter over the original DS scalar-state core and actual host.
use super::ds_hll_state_core::{self as core, ScalarSketchPort, ScalarStateSink, TuningFailure};
use crate::aggregate_host_allocator::HostAggregateAllocator;
use crate::aggregate_scalar::{ScalarReadFailure, ScalarReadFailureSink, ScalarReadObservation};
use crate::datasketches_hll_failure::{
    HllDataRecipe, HllFailureSink, HllInvariantRecipe, HllObservation,
};
use crate::kernel_control::{internal, invalid};
use crate::kernel_input::EvaluationCheckpoints;
use crate::opaque_memory::{OpaqueReservation, OpaqueRetainedCharge};
use crate::sketch_hash::{SketchHashFailure, SketchHashFailureSink};
use crate::{
    AggregateStateAllocator, FunctionArgumentType, KernelEvaluationControl, KernelFailure,
    RowDataError, ScalarCallContract, ScalarCallInput, SelectedValues,
};
use arrow_array::{ArrayRef, builder::BinaryBuilder};
use arrow_schema::DataType;
use std::{alloc::Layout, fmt, sync::Arc};
#[derive(Debug)]
enum RowFailure {
    Data(RowDataError),
    Kernel(KernelFailure),
}
struct Sink<'work, 'control, 'host> {
    ordinal: usize,
    allocator: &'host HostAggregateAllocator,
    work: &'work mut EvaluationCheckpoints<'control>,
    // Every charge comes from the actual original host before its std copy.
    // The temporary owned scalar is destroyed inside core before this sink.
    copies: OpaqueRetainedCharge,
}
impl Sink<'_, '_, '_> {
    fn data(&mut self, message: &dyn fmt::Display) -> RowFailure {
        match RowDataError::prepare_host(self.ordinal, self.allocator, self.work, |f| {
            write!(f, "{message}")
        }) {
            Ok(data) => RowFailure::Data(data),
            Err(cause) => RowFailure::Kernel(cause),
        }
    }
    fn observed(&mut self, event: HllObservation) -> Result<(), RowFailure> {
        match event {
            HllObservation::Step => self.work.step(),
            HllObservation::OpaqueBoundary => self.work.flush(),
        }
        .map_err(RowFailure::Kernel)
    }
}
impl HllFailureSink for Sink<'_, '_, '_> {
    type Error = RowFailure;
    fn data(&mut self, r: HllDataRecipe<'_>) -> RowFailure {
        Sink::data(self, &r)
    }
    fn invariant(&mut self, r: HllInvariantRecipe) -> RowFailure {
        RowFailure::Kernel(internal(r.message()))
    }
    fn observe(&mut self, event: HllObservation) -> Result<(), RowFailure> {
        self.observed(event)
    }
}
impl SketchHashFailureSink for Sink<'_, '_, '_> {
    type Error = RowFailure;
    fn hash_data(&mut self, r: SketchHashFailure<'_>) -> RowFailure {
        self.data(&r.message("ds_hll_count_distinct_state"))
    }
    fn observe_hash(&mut self, event: HllObservation) -> Result<(), RowFailure> {
        self.observed(event)
    }
}
impl ScalarReadFailureSink for Sink<'_, '_, '_> {
    type Error = RowFailure;
    fn read_data(&mut self, r: ScalarReadFailure<'_>) -> RowFailure {
        self.data(&r)
    }
    fn observe(&mut self, event: ScalarReadObservation) -> Result<(), RowFailure> {
        self.observed(match event {
            ScalarReadObservation::Step => HllObservation::Step,
            ScalarReadObservation::OpaqueBoundary => HllObservation::OpaqueBoundary,
        })
    }
    fn reserve_scalar_copy(
        &mut self,
        n: usize,
        width: usize,
        alignment: usize,
    ) -> Result<(), RowFailure> {
        let bytes = n
            .checked_mul(width)
            .ok_or(RowFailure::Kernel(KernelFailure::ResourceExhausted))?;
        Layout::from_size_align(bytes, alignment)
            .map_err(|_| RowFailure::Kernel(KernelFailure::ResourceExhausted))?;
        if bytes == 0 {
            return Ok(());
        }
        let retained = self
            .copies
            .bytes()
            .checked_add(bytes)
            .ok_or(RowFailure::Kernel(KernelFailure::ResourceExhausted))?;
        let mut reservation = self
            .copies
            .reserve_operation(bytes)
            .map_err(RowFailure::Kernel)?;
        self.copies
            .reconcile_under_reservation(retained, &mut reservation)
            .map_err(RowFailure::Kernel)
    }
}
impl ScalarStateSink for Sink<'_, '_, '_> {
    fn tuning(&mut self, r: TuningFailure<'_>) -> RowFailure {
        self.data(&r)
    }
}
struct Port(OpaqueRetainedCharge);
impl<'w, 'c, 'h> ScalarSketchPort<Sink<'w, 'c, 'h>> for Port {
    type Reservation = OpaqueReservation;
    fn reserve(
        &self,
        bytes: usize,
        _: &mut Sink<'w, 'c, 'h>,
    ) -> Result<OpaqueReservation, RowFailure> {
        self.0.reserve_operation(bytes).map_err(RowFailure::Kernel)
    }
    fn reconcile(
        &mut self,
        bytes: usize,
        lease: &mut OpaqueReservation,
        _: &mut Sink<'w, 'c, 'h>,
    ) -> Result<(), RowFailure> {
        self.0
            .reconcile_under_reservation(bytes, lease)
            .map_err(RowFailure::Kernel)
    }
}
pub(super) fn validate_profile(
    contract: &ScalarCallContract,
    mut step: impl FnMut() -> Result<(), KernelFailure>,
) -> Result<(), KernelFailure> {
    let args = &contract.selected().argument_types;
    if !(1..=3).contains(&args.len())
        || args
            .iter()
            .any(|a| !matches!(a, FunctionArgumentType::Value(_)))
    {
        return Err(invalid(
            "ds_hll_count_distinct_state requires one, two or three exact value arguments",
        ));
    }
    // Generic ANY is not a hash or tuning whitelist. Unsupported actual values
    // remain lazy original row errors; NULL keys skip tuning altogether.
    for _ in args.iter() {
        step()?;
    }
    step()?;
    if contract.result_type().data_type != DataType::Binary
        || !contract.result_type().nullable
        || contract.result_type().logical_type
            != novarocks_type_contract::ValueLogicalType::Physical
    {
        return Err(invalid(
            "ds_hll_count_distinct_state requires its exact nullable Physical Binary result",
        ));
    }
    Ok(())
}
pub(super) fn evaluate<'a>(
    input: ScalarCallInput<'_, 'a>,
    control: &dyn KernelEvaluationControl,
    allocator: &HostAggregateAllocator,
    host: &Arc<dyn AggregateStateAllocator>,
) -> Result<SelectedValues<'a>, KernelFailure> {
    control.checkpoint(0)?;
    let mut work = EvaluationCheckpoints::new(control);
    let result = (|| {
        validate_profile(input.contract(), || work.step())?;
        let args = input.arguments();
        if args.len() != input.contract().selected().argument_types.len() {
            return Err(invalid(
                "ds_hll_count_distinct_state evaluated arity differs from its exact call",
            ));
        }
        let selection = input.selection();
        let mut builder = BinaryBuilder::new();
        let mut errors = Vec::new();
        for (ordinal, row) in selection.iter().enumerate() {
            work.step()?;
            let port = Port(OpaqueRetainedCharge::try_new(Arc::clone(host))?);
            let mut sink = Sink {
                ordinal,
                allocator,
                work: &mut work,
                copies: OpaqueRetainedCharge::try_new(Arc::clone(host))?,
            };
            let value = (args[0].array(), args[0].value_row(ordinal, row));
            let log = args.get(1).map(|a| (a.array(), a.value_row(ordinal, row)));
            let target = args.get(2).map(|a| (a.array(), a.value_row(ordinal, row)));
            match core::produce_row(value, log, target, port, &mut sink) {
                Ok(Some(produced)) => {
                    // Original append happens while the original serialized Vec
                    // and original sketch are both still alive.
                    sink.work.flush()?;
                    for _ in produced.payload() {
                        sink.work.step()?;
                    }
                    sink.work.flush()?;
                    builder.append_value(produced.payload());
                    sink.work.flush()?;
                }
                Ok(None) => builder.append_null(),
                Err(RowFailure::Data(data)) => {
                    builder.append_null();
                    errors.push(data);
                }
                Err(RowFailure::Kernel(cause)) => return Err(cause),
            }
            // No diagnostic production occurs here; a published row Data stays
            // lossless while other necessary selected rows are evaluated.
        }
        work.flush()?;
        let values: ArrayRef = Arc::new(builder.finish());
        work.flush()?;
        SelectedValues::try_new_observed::<KernelFailure>(
            selection,
            &input.contract().result_type().data_type,
            values,
            errors.into_boxed_slice(),
            || work.step(),
        )
    })();
    if result.is_ok() {
        work.finish()?;
    }
    result
}
