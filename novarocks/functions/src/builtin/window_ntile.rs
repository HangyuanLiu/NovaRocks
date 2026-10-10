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

//! Selected NTILE emission from the original complete partition length.
//! Geometry and input backing stay borrowed under their original host owners.

use crate::kernel_control::{internal, invalid};
use crate::kernel_input::EvaluationCheckpoints;
use crate::{
    KernelEvaluationControl, KernelFailure, PreparedWindowKernel, SelectedValues, Selection,
    WindowCallContract, WindowKernelPartition, WindowPartitionInput,
};
use arrow_array::{ArrayRef, Int64Array};
use std::{alloc::Layout, sync::Arc};

#[derive(Debug)]
pub(super) struct PreparedNtile {
    pub(super) contract: Arc<WindowCallContract>,
    pub(super) buckets: i64,
}

struct NtilePartition<'a> {
    prepared: Arc<PreparedNtile>,
    input: WindowPartitionInput<'a>,
    small: i64,
    large: i64,
    large_buckets: i64,
    large_rows: i64,
    closed: bool,
}

fn output_capacity(rows: usize) -> Result<(), KernelFailure> {
    Layout::array::<i64>(rows).map_err(|_| KernelFailure::ResourceExhausted)?;
    Ok(())
}

/// Preserve compute_ntile's quotient/remainder algorithm, not floor(pos*b/n).
/// Checked setup refuses an unrepresentable complete partition operationally;
/// it never invents a wrapping bucket ID or a scalar row error.
fn partition_sizes(rows: usize, buckets: i64) -> Result<(i64, i64, i64, i64), KernelFailure> {
    if buckets <= 0 {
        return Err(invalid("ntile buckets must be positive"));
    }
    let rows = i64::try_from(rows).map_err(|_| KernelFailure::ResourceExhausted)?;
    let small = rows / buckets;
    let large = small
        .checked_add(1)
        .ok_or(KernelFailure::ResourceExhausted)?;
    let large_buckets = rows % buckets;
    let large_rows = large_buckets
        .checked_mul(large)
        .ok_or(KernelFailure::ResourceExhausted)?;
    Ok((small, large, large_buckets, large_rows))
}

impl PreparedWindowKernel for PreparedNtile {
    fn contract(&self) -> &Arc<WindowCallContract> {
        &self.contract
    }
    fn partition_retained_upper_bound(&self, _: usize) -> Result<usize, KernelFailure> {
        Ok(size_of::<NtilePartition<'_>>())
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
                    "ntile partition differs from its original prepared contract",
                ));
            }
            // Original geometry/input checking is already complete. Setup performs
            // only this actual required length/arithmetic check even for no output.
            let (small, large, large_buckets, large_rows) =
                partition_sizes(input.full_input().partition_rows(), self.buckets)?;
            work.step()?;
            work.flush()?;
            let partition = Box::new(NtilePartition {
                prepared: self,
                input,
                small,
                large,
                large_buckets,
                large_rows,
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

impl WindowKernelPartition for NtilePartition<'_> {
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
                let shape = selection.batch_rows() == self.input.full_input().partition_rows()
                    && selection.len() <= row_capacity;
                work.step()?;
                if !shape {
                    return Err(invalid(
                        "ntile output differs from its partition or host row grant",
                    ));
                }
                output_capacity(selection.len())?;
                work.flush()?;
                let mut values = Vec::new();
                values
                    .try_reserve_exact(selection.len())
                    .map_err(|_| KernelFailure::ResourceExhausted)?;
                work.flush()?;
                for row in selection.iter() {
                    let position =
                        i64::try_from(row).map_err(|_| KernelFailure::ResourceExhausted)?;
                    let id = if position < self.large_rows {
                        position / self.large + 1
                    } else {
                        // When buckets exceed rows, all real rows belong to the
                        // large region. No zero-sized small bucket is evaluated.
                        if self.small == 0 {
                            return Err(internal(
                                "ntile selected row escaped its complete partition",
                            ));
                        }
                        (position - self.large_rows) / self.small + self.large_buckets + 1
                    };
                    values.push(id);
                    work.step()?;
                }
                work.flush()?;
                let array = Arc::new(Int64Array::new(values.into(), None)) as ArrayRef;
                work.flush()?;
                SelectedValues::try_new_observed::<KernelFailure>(
                    selection,
                    &self.prepared.contract.result_type().data_type,
                    array,
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
        size_of::<Self>()
    }
}

#[cfg(test)]
#[path = "window_ntile_tests.rs"]
mod tests;
