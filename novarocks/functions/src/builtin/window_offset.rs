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

//! LEAD/LAG selected values from one complete original partition.
//! Offset is a checked immutable preparation constant; frame geometry remains host-owned.

use crate::kernel_control::{internal, invalid};
use crate::kernel_input::EvaluationCheckpoints;
use crate::{
    KernelEvaluationControl, KernelFailure, PreparedWindowKernel, SelectedValues, Selection,
    WindowCallContract, WindowKernelPartition, WindowPartitionInput,
};
use arrow_array::{ArrayRef, UInt64Array};
use std::{alloc::Layout, ops::Range, sync::Arc};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum OffsetOperation {
    Lead,
    Lag,
}

#[derive(Debug)]
pub(super) struct PreparedOffset {
    pub(super) contract: Arc<WindowCallContract>,
    pub(super) operation: OffsetOperation,
    pub(super) offset: i64,
}

struct OffsetPartition<'a> {
    prepared: Arc<PreparedOffset>,
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
            Err(invalid("window offset take carrier is unsupported"))
        }
    }
}

impl PreparedWindowKernel for PreparedOffset {
    fn contract(&self) -> &Arc<WindowCallContract> {
        &self.contract
    }
    fn partition_retained_upper_bound(&self, rows: usize) -> Result<usize, KernelFailure> {
        let heap = Layout::array::<Option<u64>>(rows)
            .map_err(|_| KernelFailure::ResourceExhausted)?
            .size();
        size_of::<OffsetPartition<'_>>()
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
                    "window offset partition differs from its original prepared contract",
                ));
            }
            let rows = input.full_input().partition_rows();
            self.partition_retained_upper_bound(rows)?;
            let source = input.full_input().logical_arguments()[0];
            let mut addresses = reserve::<Option<u64>>(rows, &mut work)?;
            let ignore = self.contract.options().ignore_nulls();
            let mut positions = if ignore {
                reserve::<usize>(rows, &mut work)?
            } else {
                Vec::new()
            };
            let mut prefix = if ignore {
                let count = rows
                    .checked_add(1)
                    .ok_or(KernelFailure::ResourceExhausted)?;
                reserve::<usize>(count, &mut work)?
            } else {
                Vec::new()
            };
            if ignore {
                prefix.push(0);
                work.step()?;
                for row in 0..rows {
                    let address = source.value_row(row, row);
                    // Original IGNORE NULLS tests physical Array::is_null. No
                    // encoded logical-NULL rule is substituted here.
                    if !source.array().is_null(address) {
                        positions.push(row);
                    }
                    prefix.push(positions.len());
                    work.step()?;
                }
            }
            let offset = usize::try_from(self.offset).ok();
            for row in 0..rows {
                let target = if ignore {
                    if self.offset == 0 {
                        Some(row)
                    } else {
                        offset.and_then(|offset| match self.operation {
                            OffsetOperation::Lag => prefix[row]
                                .checked_sub(offset)
                                .and_then(|rank| positions.get(rank).copied()),
                            OffsetOperation::Lead => prefix[row + 1]
                                .checked_add(offset - 1)
                                .and_then(|rank| positions.get(rank).copied()),
                        })
                    }
                } else {
                    offset.and_then(|offset| match self.operation {
                        OffsetOperation::Lag => row.checked_sub(offset),
                        OffsetOperation::Lead => {
                            row.checked_add(offset).filter(|target| *target < rows)
                        }
                    })
                };
                let address = target
                    .map(|row| u64::try_from(source.value_row(row, row)))
                    .transpose()
                    .map_err(|_| KernelFailure::ResourceExhausted)?;
                addresses.push(address);
                work.step()?;
            }
            // Full required setup and format refusal precede output demand.
            // No full-partition result is materialized or hidden in the cursor.
            copy_plan(source, &addresses, &mut work)?;
            drop(positions);
            drop(prefix);
            work.flush()?;
            let addresses = addresses.into_boxed_slice();
            work.flush()?;
            let partition = Box::new(OffsetPartition {
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

impl WindowKernelPartition for OffsetPartition<'_> {
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
                        "window offset output differs from its partition or host row grant",
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
                    copied.map_err(|_| internal("window offset Arrow take failed"))?;
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

#[cfg(test)]
#[path = "window_offset_tests.rs"]
mod tests;
