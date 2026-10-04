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

//! FIRST/LAST value selection from the original complete frame table.
//! Input/geometry and immutable contract backing remain under their host owners.

use crate::kernel_control::{internal, invalid};
use crate::kernel_input::EvaluationCheckpoints;
use crate::{
    KernelEvaluationControl, KernelFailure, PreparedWindowKernel, SelectedValues, Selection,
    WindowCallContract, WindowKernelPartition, WindowPartitionInput,
};
use arrow_array::{ArrayRef, UInt64Array};
use std::{alloc::Layout, ops::Range, sync::Arc};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum ValueOperation {
    First,
    Last,
}

#[derive(Debug)]
pub(super) struct PreparedValue {
    pub(super) contract: Arc<WindowCallContract>,
    pub(super) operation: ValueOperation,
}

struct ValuePartition<'a> {
    prepared: Arc<PreparedValue>,
    input: WindowPartitionInput<'a>,
    addresses: Box<[Option<u64>]>,
    closed: bool,
}

fn reserve<T>(rows: usize, work: &mut EvaluationCheckpoints<'_>) -> Result<Vec<T>, KernelFailure> {
    Layout::array::<T>(rows).map_err(|_| KernelFailure::ResourceExhausted)?;
    work.flush()?;
    let mut values = Vec::new();
    values
        .try_reserve_exact(rows)
        .map_err(|_| KernelFailure::ResourceExhausted)?;
    work.flush()?;
    Ok(values)
}

fn copy_plan(
    input: crate::EvaluatedArgument<'_>,
    indices: &[Option<u64>],
    work: &mut EvaluationCheckpoints<'_>,
) -> Result<(), KernelFailure> {
    // These are actual delegated take-plan scratch and UInt64 values/bitmap
    // representations. They do not authorize the Arrow copy's host MEM scope.
    Layout::array::<Range<usize>>(indices.len()).map_err(|_| KernelFailure::ResourceExhausted)?;
    Layout::array::<u64>(indices.len()).map_err(|_| KernelFailure::ResourceExhausted)?;
    let bitmap = indices
        .len()
        .checked_add(7)
        .ok_or(KernelFailure::ResourceExhausted)?
        / 8;
    Layout::array::<u8>(bitmap).map_err(|_| KernelFailure::ResourceExhausted)?;
    work.flush()?;
    match crate::selected_copy::preflight_take(input.array().as_ref(), indices, |opaque| {
        if opaque { work.flush() } else { work.step() }
    }) {
        Ok(()) => work.flush(),
        Err(crate::selected_copy::CopyError::Control(cause)) => Err(cause),
        Err(crate::selected_copy::CopyError::Extent) => Err(KernelFailure::ResourceExhausted),
        Err(crate::selected_copy::CopyError::Invalid(message)) => {
            work.flush()?;
            Err(invalid(message))
        }
        Err(crate::selected_copy::CopyError::Unsupported(_)) => {
            work.flush()?;
            Err(invalid("window value take carrier is unsupported"))
        }
    }
}

impl PreparedWindowKernel for PreparedValue {
    fn contract(&self) -> &Arc<WindowCallContract> {
        &self.contract
    }

    fn partition_retained_upper_bound(&self, rows: usize) -> Result<usize, KernelFailure> {
        let heap = Layout::array::<Option<u64>>(rows)
            .map_err(|_| KernelFailure::ResourceExhausted)?
            .size();
        size_of::<ValuePartition<'_>>()
            .checked_add(heap)
            .ok_or(KernelFailure::ResourceExhausted)
    }

    fn begin_partition<'a>(
        self: Arc<Self>,
        input: WindowPartitionInput<'a>,
        control: &dyn KernelEvaluationControl,
    ) -> Result<Box<dyn WindowKernelPartition + 'a>, KernelFailure> {
        control.checkpoint(0)?;
        let mut work = EvaluationCheckpoints::new(control);
        let result = (|| {
            let same = std::ptr::eq(input.full_input().contract(), self.contract.as_ref());
            work.step()?;
            if !same {
                return Err(invalid(
                    "window value partition differs from its original prepared contract",
                ));
            }
            let rows = input.full_input().partition_rows();
            self.partition_retained_upper_bound(rows)?;
            let source = input.full_input().logical_arguments()[0];
            let mut addresses = reserve::<Option<u64>>(rows, &mut work)?;
            // Only the needed direction is retained temporarily. IGNORE NULLS
            // intentionally uses the original physical Array::is_null rule,
            // not the logical encoded-NULL traversal of scalar kernels.
            let nearest = if self.contract.options().ignore_nulls() {
                let count = rows
                    .checked_add(1)
                    .ok_or(KernelFailure::ResourceExhausted)?;
                let mut nearest = reserve::<Option<usize>>(count, &mut work)?;
                for _ in 0..count {
                    nearest.push(None);
                    work.step()?;
                }
                let mut found = None;
                match self.operation {
                    ValueOperation::First => {
                        for row in (0..rows).rev() {
                            let address = source.value_row(row, row);
                            if !source.array().is_null(address) {
                                found = Some(row);
                            }
                            nearest[row] = found;
                            work.step()?;
                        }
                    }
                    ValueOperation::Last => {
                        for (row, slot) in nearest.iter_mut().take(rows).enumerate() {
                            let address = source.value_row(row, row);
                            if !source.array().is_null(address) {
                                found = Some(row);
                            }
                            *slot = found;
                            work.step()?;
                        }
                    }
                }
                nearest
            } else {
                Vec::new()
            };
            for frame in input.frames() {
                let target = if frame.start == frame.end {
                    None
                } else {
                    match (self.operation, self.contract.options().ignore_nulls()) {
                        (ValueOperation::First, false) => Some(frame.start),
                        (ValueOperation::Last, false) => Some(frame.end - 1),
                        (ValueOperation::First, true) => {
                            nearest[frame.start].filter(|row| *row < frame.end)
                        }
                        (ValueOperation::Last, true) => {
                            nearest[frame.end - 1].filter(|row| *row >= frame.start)
                        }
                    }
                };
                let address = target
                    .map(|row| u64::try_from(source.value_row(row, row)))
                    .transpose()
                    .map_err(|_| KernelFailure::ResourceExhausted)?;
                addresses.push(address);
                work.step()?;
            }
            // Complete setup, including format/extent refusal, precedes any
            // output demand. This creates no hidden full-partition value array.
            copy_plan(source, &addresses, &mut work)?;
            drop(nearest);
            work.flush()?;
            let addresses = addresses.into_boxed_slice();
            work.flush()?;
            let partition = Box::new(ValuePartition {
                prepared: self,
                input,
                addresses,
                closed: false,
            }) as Box<dyn WindowKernelPartition>;
            work.flush()?;
            Ok(partition)
        })();
        if matches!(
            &result,
            Err(KernelFailure::Cancelled
                | KernelFailure::DeadlineExceeded
                | KernelFailure::ResourceExhausted)
        ) {
            return result;
        }
        work.finish()?;
        result
    }
}

impl WindowKernelPartition for ValuePartition<'_> {
    fn evaluate<'s>(
        &mut self,
        selection: Selection<'s>,
        row_capacity: usize,
        control: &dyn KernelEvaluationControl,
    ) -> Result<SelectedValues<'s>, KernelFailure> {
        if self.closed {
            return Err(KernelFailure::InstanceFailed);
        }
        let result = (|| {
            control.checkpoint(0)?;
            let mut work = EvaluationCheckpoints::new(control);
            let result = (|| {
                let shape = selection.batch_rows() == self.addresses.len()
                    && selection.len() <= row_capacity;
                work.step()?;
                if !shape {
                    return Err(invalid(
                        "window value output differs from its partition or host row grant",
                    ));
                }
                let mut indices = reserve::<Option<u64>>(selection.len(), &mut work)?;
                for row in selection.iter() {
                    indices.push(self.addresses[row]);
                    work.step()?;
                }
                let source = self.input.full_input().logical_arguments()[0];
                copy_plan(source, &indices, &mut work)?;
                work.flush()?;
                let indices = UInt64Array::from(indices);
                work.flush()?;
                let copied = arrow_select::take::take(source.array().as_ref(), &indices, None);
                work.flush()?;
                let copied: ArrayRef =
                    copied.map_err(|_| internal("window value Arrow take failed"))?;
                SelectedValues::try_new_observed::<KernelFailure>(
                    selection,
                    &self.prepared.contract.result_type().data_type,
                    copied,
                    Box::default(),
                    || work.step(),
                )
            })();
            if matches!(
                &result,
                Err(KernelFailure::Cancelled
                    | KernelFailure::DeadlineExceeded
                    | KernelFailure::ResourceExhausted)
            ) {
                return result;
            }
            work.finish()?;
            result
        })();
        if result.is_err() {
            self.closed = true;
        }
        result
    }

    fn finish(&mut self, control: &dyn KernelEvaluationControl) -> Result<(), KernelFailure> {
        if self.closed {
            return Err(KernelFailure::InstanceFailed);
        }
        self.closed = true;
        control.checkpoint(0)?;
        EvaluationCheckpoints::new(control).finish()
    }

    fn retained_bytes(&self) -> usize {
        size_of::<Self>() + self.addresses.len() * size_of::<Option<u64>>()
    }
}
