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

//! COUNT OVER reuses the installed aggregate's state contribution and emitter.
//! Frames belong to the original partition owner; host memory grants are external.

use super::aggregate_count::CountKernel;
use crate::kernel_control::{KernelControlObservation, compile_failure, internal, invalid};
use crate::kernel_input::EvaluationCheckpoints;
use crate::{
    AggregateKernelPhase, KernelEvaluationControl, KernelFailure, PreparedAggregateKernel,
    PreparedWindowKernel, SelectedAggregateUpdateInput, SelectedValues, Selection,
    WindowCallContract, WindowKernelPartition, WindowPartitionInput,
};
use arrow_schema::DataType;
use novarocks_type_contract::{
    CompileCheckpoints, CompilePhase, PureCompileControl, WindowFrameExclusion,
};
use std::{alloc::Layout, sync::Arc};

#[derive(Debug)]
pub(super) struct PreparedCountWindow {
    aggregate: Arc<CountKernel>,
    contract: Arc<WindowCallContract>,
}

/// The same installed COUNT owner passes its already-prepared aggregate kernel.
pub(super) fn prepare(
    aggregate: Arc<CountKernel>,
    contract: Arc<WindowCallContract>,
    control: &dyn PureCompileControl,
) -> Result<Arc<dyn PreparedWindowKernel>, KernelFailure> {
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::FunctionSpecialization)
        .map_err(compile_failure)?;
    let result = (|| {
        let exact = contract
            .aggregate()
            .is_some_and(|source| Arc::ptr_eq(source, aggregate.contract()))
            && aggregate.contract.phase() == AggregateKernelPhase::Single
            && !aggregate.contract.distinct()
            && aggregate.contract.order_keys().is_empty()
            && aggregate.contract.call().logical_argument_count() <= 1;
        work.step().map_err(compile_failure)?;
        if !exact {
            return Err(invalid(
                "COUNT OVER requires its exact Single non-DISTINCT aggregate without function ORDER",
            ));
        }
        for source in aggregate.contract.logical_argument_types() {
            // The original analytic COUNT uses physical Array::is_null. These
            // roots differ from the aggregate's logical NULL contribution rule.
            // Children of ordinary List/Struct do not change their root NULL rule.
            let unsupported = matches!(
                source.data_type,
                DataType::Null
                    | DataType::Dictionary(_, _)
                    | DataType::Union(_, _)
                    | DataType::RunEndEncoded(_, _)
            );
            work.step().map_err(compile_failure)?;
            if unsupported {
                return Err(invalid(
                    "COUNT OVER encoded or bare Null physical NULL profile is unsupported",
                ));
            }
        }
        let unsupported = contract
            .options()
            .frame()
            .is_some_and(|frame| frame.exclusion != WindowFrameExclusion::NoOthers);
        work.step().map_err(compile_failure)?;
        if unsupported {
            return Err(invalid("COUNT OVER frame exclusion is unsupported"));
        }
        // IGNORE NULLS is deliberately not a separate COUNT rule: the original
        // analytic COUNT ignores that option and the aggregate skips NULL input.
        work.flush().map_err(compile_failure)?;
        let prepared: Arc<dyn PreparedWindowKernel> = Arc::new(PreparedCountWindow {
            aggregate,
            contract,
        });
        work.flush().map_err(compile_failure)?;
        Ok(prepared)
    })();
    if matches!(
        &result,
        Err(KernelFailure::Cancelled
            | KernelFailure::DeadlineExceeded
            | KernelFailure::ResourceExhausted)
    ) {
        return result;
    }
    work.finish().map_err(compile_failure)?;
    result
}

struct CountPartition {
    prepared: Arc<PreparedCountWindow>,
    counts: Box<[i64]>,
    closed: bool,
}

fn observed<T>(
    original: &dyn KernelEvaluationControl,
    operation: impl FnOnce(
        &mut EvaluationCheckpoints<'_>,
        &dyn KernelEvaluationControl,
    ) -> Result<T, KernelFailure>,
) -> Result<T, KernelFailure> {
    // Nested COUNT scopes must preserve all seven original control failures,
    // including ordinary diagnostic failures, before this scope's final tail.
    let control = KernelControlObservation::new(original);
    let result = (|| {
        control.checkpoint(0)?;
        let mut work = EvaluationCheckpoints::new(&control);
        let result = operation(&mut work, &control);
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
    control.finish(result)
}

fn reserve(rows: usize, work: &mut EvaluationCheckpoints<'_>) -> Result<Vec<i64>, KernelFailure> {
    Layout::array::<i64>(rows).map_err(|_| KernelFailure::ResourceExhausted)?;
    work.flush()?;
    let mut values = Vec::new();
    values
        .try_reserve_exact(rows)
        .map_err(|_| KernelFailure::ResourceExhausted)?;
    work.flush()?;
    Ok(values)
}

impl PreparedWindowKernel for PreparedCountWindow {
    fn contract(&self) -> &Arc<WindowCallContract> {
        &self.contract
    }

    fn partition_retained_upper_bound(&self, rows: usize) -> Result<usize, KernelFailure> {
        i64::try_from(rows).map_err(|_| KernelFailure::ResourceExhausted)?;
        let heap = Layout::array::<i64>(rows)
            .map_err(|_| KernelFailure::ResourceExhausted)?
            .size();
        size_of::<CountPartition>()
            .checked_add(heap)
            .ok_or(KernelFailure::ResourceExhausted)
    }

    fn begin_partition<'a>(
        self: Arc<Self>,
        input: WindowPartitionInput<'a>,
        control: &dyn KernelEvaluationControl,
    ) -> Result<Box<dyn WindowKernelPartition + 'a>, KernelFailure> {
        observed(control, |work, control| {
            let exact = std::ptr::eq(input.full_input().contract(), self.contract.as_ref());
            work.step()?;
            if !exact {
                return Err(invalid(
                    "COUNT OVER partition differs from its exact prepared contract",
                ));
            }
            let full = input.full_input();
            let rows = full.partition_rows();
            self.partition_retained_upper_bound(rows)?;
            let prefix_rows = rows
                .checked_add(1)
                .ok_or(KernelFailure::ResourceExhausted)?;
            // Both actual temporary and retained requests are checked before the
            // first reserve. Box conversion remains an observed opaque operation.
            Layout::array::<i64>(prefix_rows).map_err(|_| KernelFailure::ResourceExhausted)?;
            let mut prefix = reserve(prefix_rows, work)?;
            let mut counts = reserve(rows, work)?;
            work.flush()?;
            let update = SelectedAggregateUpdateInput::try_new(
                self.aggregate.contract(),
                Selection::all(rows),
                full.logical_arguments(),
                &[],
                control,
            )?;
            let prepared = self.aggregate.prepare_update(update, control)?;
            let mut state = self.aggregate.create_state(control)?;
            prefix.push(state);
            work.step()?;
            for row in 0..rows {
                work.flush()?;
                self.aggregate
                    .update_row(&mut state, &prepared, row, control)?;
                prefix.push(state);
                work.step()?;
            }
            for frame in input.frames() {
                let value = prefix[frame.end].checked_sub(prefix[frame.start]);
                work.step()?;
                let value = value
                    .filter(|value| *value >= 0)
                    .ok_or_else(|| internal("COUNT OVER prefix differs from its admitted frame"))?;
                counts.push(value);
                work.step()?;
            }
            drop(prefix);
            work.flush()?;
            let counts = counts.into_boxed_slice();
            work.flush()?;
            let partition = Box::new(CountPartition {
                prepared: self,
                counts,
                closed: false,
            }) as Box<dyn WindowKernelPartition>;
            work.flush()?;
            Ok(partition)
        })
    }
}

impl WindowKernelPartition for CountPartition {
    fn evaluate<'a>(
        &mut self,
        selection: Selection<'a>,
        row_capacity: usize,
        control: &dyn KernelEvaluationControl,
    ) -> Result<SelectedValues<'a>, KernelFailure> {
        if self.closed {
            return Err(KernelFailure::InstanceFailed);
        }
        let result = observed(control, |work, control| {
            let valid =
                selection.batch_rows() == self.counts.len() && selection.len() <= row_capacity;
            work.step()?;
            if !valid {
                return Err(invalid(
                    "COUNT OVER output differs from its partition or host row grant",
                ));
            }
            let mut states = reserve(selection.len(), work)?;
            for row in selection.iter() {
                states.push(self.counts[row]);
                work.step()?;
            }
            work.flush()?;
            let output = self
                .prepared
                .aggregate
                .build_final(states.iter(), control)?;
            SelectedValues::try_new_observed::<KernelFailure>(
                selection,
                &self.prepared.contract.result_type().data_type,
                output,
                Box::default(),
                || work.step(),
            )
        });
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
        observed(control, |work, _| {
            self.counts = Box::default();
            work.step()
        })
    }

    fn retained_bytes(&self) -> usize {
        size_of::<Self>() + self.counts.len() * size_of::<i64>()
    }
}

#[cfg(test)]
#[path = "aggregate_count_window_tests.rs"]
mod tests;
