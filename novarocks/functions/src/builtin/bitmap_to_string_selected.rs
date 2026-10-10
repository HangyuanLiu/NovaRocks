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
//! Selected adapter over ONE original historical decoder and owned renderer.
//! Library internals remain opaque; these are actual host reservations, not a
//! formal MEM claim or a claim of cooperative 256-work inside roaring/std.
use crate::aggregate_host_allocator::HostAggregateAllocator;
use crate::bitmap_value::BitmapDecodePort;
use crate::kernel_control::{internal, invalid};
use crate::kernel_input::EvaluationCheckpoints;
use crate::bitmap_decode_resources::BitmapDecodeResources;
use crate::{
    AggregateStateAllocator, FunctionArgumentType, KernelEvaluationControl, KernelFailure,
    RowDataError, ScalarCallContract, ScalarCallInput, SelectedValues,
};
use arrow_array::{Array, ArrayRef, BinaryArray, StringArray};
use arrow_buffer::{BooleanBuffer, Buffer, NullBuffer, OffsetBuffer};
use arrow_schema::DataType;
use novarocks_type_contract::{ArgumentControl, ValueLogicalType};
use std::{alloc::Layout, fmt, sync::Arc};

pub(super) fn original_native_profile(
    selected: &crate::FunctionBindingSelection,
    count: usize,
) -> bool {
    let [FunctionArgumentType::Value(source)] = selected.argument_types.as_ref() else {
        return false;
    };
    let crate::FunctionResultType::Scalar(target) = &selected.result_type else {
        return false;
    };
    count == 1
        && selected.aggregate.is_none()
        && source.logical_type == ValueLogicalType::Physical
        && source.data_type == DataType::Binary
        && target.logical_type == ValueLogicalType::Physical
        && target.data_type == DataType::Utf8
        && target.nullable
}
pub(super) fn validate_profile(
    contract: &ScalarCallContract,
    mut observe: impl FnMut() -> Result<(), KernelFailure>,
) -> Result<(), KernelFailure> {
    observe()?;
    if contract.effects().argument_control != ArgumentControl::Eager
        || contract.value_argument_types().len() != 1
        || !original_native_profile(
            contract.selected(),
            contract.call().logical_argument_count(),
        )
    {
        return Err(invalid(
            "bitmap_to_string requires its original single Physical Binary profile",
        ));
    }
    observe()
}
#[derive(Debug)]
enum Failure {
    Data(RowDataError),
    Kernel(KernelFailure),
}
impl fmt::Display for Failure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Data(d) => f.write_str(d.message()),
            Self::Kernel(k) => write!(f, "{k:?}"),
        }
    }
}
struct Port<'w, 'c, 'h> {
    ordinal: usize,
    allocator: &'h HostAggregateAllocator,
    work: &'w mut EvaluationCheckpoints<'c>,
    resources: BitmapDecodeResources,
    refused: bool,
}
impl Port<'_, '_, '_> {
    fn observed(&mut self, result: Result<(), KernelFailure>) -> Result<(), Failure> {
        match result {
            Ok(()) => Ok(()),
            Err(cause) => {
                self.refused = true;
                Err(Failure::Kernel(cause))
            }
        }
    }
}
impl BitmapDecodePort for Port<'_, '_, '_> {
    type Error = Failure;
    fn data(&mut self, message: fmt::Arguments<'_>) -> Failure {
        match RowDataError::prepare_host(self.ordinal, self.allocator, self.work, |f| {
            f.write_fmt(message)
        }) {
            Ok(data) => Failure::Data(data),
            Err(cause) => {
                self.refused = true;
                Failure::Kernel(cause)
            }
        }
    }
    fn is_data(error: &Failure) -> bool {
        matches!(error, Failure::Data(_))
    }
    fn step(&mut self) -> Result<(), Failure> {
        let r = self.work.step();
        self.observed(r)
    }
    fn boundary(&mut self) -> Result<(), Failure> {
        let r = self.work.flush();
        self.observed(r)
    }
    fn before_tree_insert(&mut self, existing: usize) -> Result<(), Failure> {
        let result = self.resources.before_tree_insert(existing, self.work);
        self.refused |= self.resources.refused();
        result.map_err(Failure::Kernel)
    }
    fn before_tree_collection(&mut self, entries: usize) -> Result<(), Failure> {
        let result = self.resources.before_tree_collection(entries, self.work);
        self.refused |= self.resources.refused();
        result.map_err(Failure::Kernel)
    }
    fn before_roaring(&mut self, bytes: usize) -> Result<(), Failure> {
        let result = self.resources.before_roaring(bytes, self.work);
        self.refused |= self.resources.refused();
        result.map_err(Failure::Kernel)
    }
    fn before_u32_collection(&mut self, entries: u64) -> Result<(), Failure> {
        let result = self.resources.before_u32_collection(entries, self.work);
        self.refused |= self.resources.refused();
        result.map_err(Failure::Kernel)
    }
    fn before_render(&mut self, entries: usize) -> Result<(), Failure> {
        let result = self.resources.before_render(entries, self.work);
        self.refused |= self.resources.refused();
        result.map_err(Failure::Kernel)
    }
}
pub(super) fn evaluate<'a>(
    input: ScalarCallInput<'_, 'a>,
    control: &dyn KernelEvaluationControl,
    allocator: &HostAggregateAllocator,
    host: &Arc<dyn AggregateStateAllocator>,
) -> Result<SelectedValues<'a>, KernelFailure> {
    control.checkpoint(0)?;
    let mut rows = EvaluationCheckpoints::new(control);
    let mut work = EvaluationCheckpoints::new(control);
    let mut observed_failure = false;
    macro_rules! observed {
        ($operation:expr) => {
            match $operation {
                Ok(value) => Ok(value),
                Err(cause) => {
                    observed_failure = true;
                    Err(cause)
                }
            }
        };
    }
    let result = (|| {
        validate_profile(input.contract(), || observed!(rows.step()))?;
        let [argument] = input.arguments() else {
            return Err(invalid("bitmap_to_string requires one evaluated argument"));
        };
        let array = argument
            .array()
            .as_any()
            .downcast_ref::<BinaryArray>()
            .ok_or_else(|| internal("bitmap_to_string canonical Binary carrier differs"))?;
        let selection = input.selection();
        let count = selection
            .len()
            .checked_add(1)
            .ok_or(KernelFailure::ResourceExhausted)?;
        Layout::array::<i32>(count).map_err(|_| KernelFailure::ResourceExhausted)?;
        let mut offsets = Vec::new();
        observed!(control.checkpoint(0))?;
        offsets
            .try_reserve_exact(count)
            .map_err(|_| KernelFailure::ResourceExhausted)?;
        observed!(control.checkpoint(0))?;
        offsets.push(0i32);
        let validity_bytes = selection
            .len()
            .checked_add(7)
            .ok_or(KernelFailure::ResourceExhausted)?
            / 8;
        let mut validity = Vec::new();
        validity
            .try_reserve_exact(validity_bytes)
            .map_err(|_| KernelFailure::ResourceExhausted)?;
        validity.resize(validity_bytes, 0u8);
        let mut payload = Vec::new();
        let mut errors = Vec::new();
        for (ordinal, batch_row) in selection.iter().enumerate() {
            observed!(rows.step())?;
            let row = argument.value_row(ordinal, batch_row);
            if row >= array.len() {
                return Err(internal("bitmap_to_string selected address exceeds Binary"));
            }
            if array.is_null(row) {
                offsets.push(*offsets.last().unwrap());
                continue;
            }
            let mut port = Port {
                ordinal,
                allocator,
                work: &mut work,
                resources: BitmapDecodeResources::try_new(Arc::clone(host))?,
                refused: false,
            };
            let rendered =
                crate::bitmap_to_string_core::render_with_port(array.value(row), &mut port);
            observed_failure |= port.refused;
            match rendered {
                Err(Failure::Kernel(cause)) => return Err(cause),
                Err(Failure::Data(data)) => {
                    errors
                        .try_reserve(1)
                        .map_err(|_| KernelFailure::ResourceExhausted)?;
                    errors.push(data);
                    offsets.push(*offsets.last().unwrap());
                }
                Ok(text) => {
                    let extent = payload
                        .len()
                        .checked_add(text.len())
                        .ok_or(KernelFailure::ResourceExhausted)?;
                    let offset =
                        i32::try_from(extent).map_err(|_| KernelFailure::ResourceExhausted)?;
                    Layout::array::<u8>(extent).map_err(|_| KernelFailure::ResourceExhausted)?;
                    port.boundary().map_err(|f| {
                        observed_failure = true;
                        match f {
                            Failure::Kernel(k) => k,
                            Failure::Data(_) => unreachable!(),
                        }
                    })?;
                    payload
                        .try_reserve_exact(text.len())
                        .map_err(|_| KernelFailure::ResourceExhausted)?;
                    observed!(control.checkpoint(0))?;
                    for byte in text.bytes() {
                        payload.push(byte);
                        port.step().map_err(|f| {
                            observed_failure = true;
                            match f {
                                Failure::Kernel(k) => k,
                                Failure::Data(_) => unreachable!(),
                            }
                        })?;
                    }
                    offsets.push(offset);
                    validity[ordinal / 8] |= 1 << (ordinal % 8);
                    // Drop original owned String before its operation charge.
                    drop(text);
                }
            }
            drop(port);
        }
        observed!(work.flush())?;
        let nulls = Some(NullBuffer::new(BooleanBuffer::new(
            Buffer::from(validity),
            0,
            selection.len(),
        )));
        let values: ArrayRef = Arc::new(StringArray::new(
            OffsetBuffer::new(offsets.into()),
            Buffer::from(payload),
            nulls,
        ));
        observed!(control.checkpoint(0))?;
        SelectedValues::try_new_observed::<KernelFailure>(
            selection,
            &input.contract().result_type().data_type,
            values,
            errors.into_boxed_slice(),
            || observed!(rows.step()),
        )
    })();
    // An observed refusal or Resource first cause has no optional footer.
    if observed_failure
        || matches!(
            &result,
            Err(KernelFailure::Cancelled
                | KernelFailure::DeadlineExceeded
                | KernelFailure::ResourceExhausted)
        )
    {
        return result;
    }
    work.finish()?;
    rows.finish()?;
    result
}
