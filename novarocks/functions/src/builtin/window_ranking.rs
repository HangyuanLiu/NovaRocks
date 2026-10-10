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

//! Selected ranking emission from the original complete partition geometry.
//! Borrowed peer/frame storage remains host-owned; these Layout facts are not grants.

use crate::kernel_control::{internal, invalid};
use crate::kernel_input::EvaluationCheckpoints;
use crate::{
    KernelEvaluationControl, KernelFailure, PreparedWindowKernel, SelectedValues, Selection,
    WindowCallContract, WindowKernelPartition, WindowPartitionInput,
};
use arrow_array::{
    ArrayRef, PrimitiveArray,
    types::{ArrowPrimitiveType, Float64Type, Int64Type},
};
use std::{alloc::Layout, sync::Arc};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum RankingOperation {
    RowNumber,
    Rank,
    DenseRank,
    CumeDist,
    PercentRank,
}

#[derive(Debug)]
pub(super) struct PreparedRanking {
    pub(super) contract: Arc<WindowCallContract>,
    pub(super) operation: RankingOperation,
}

struct RankingPartition<'a> {
    prepared: Arc<PreparedRanking>,
    input: WindowPartitionInput<'a>,
    closed: bool,
}

fn output_capacity(rows: usize) -> Result<(), KernelFailure> {
    Layout::array::<i64>(rows).map_err(|_| KernelFailure::ResourceExhausted)?;
    Ok(())
}

impl PreparedWindowKernel for PreparedRanking {
    fn contract(&self) -> &Arc<WindowCallContract> {
        &self.contract
    }
    fn partition_retained_upper_bound(&self, _: usize) -> Result<usize, KernelFailure> {
        Ok(size_of::<RankingPartition<'_>>())
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
                    "ranking partition differs from its original prepared contract",
                ));
            }
            // The original geometry author already checks the complete peer
            // and frame tables. Ranking setup needs no independent traversal.
            // Integer representability is required even for empty output.
            let integer_rows = matches!(
                self.operation,
                RankingOperation::CumeDist | RankingOperation::PercentRank
            ) || i64::try_from(input.full_input().partition_rows()).is_ok();
            work.step()?;
            if !integer_rows {
                return Err(KernelFailure::ResourceExhausted);
            }
            work.flush()?;
            let partition = Box::new(RankingPartition {
                prepared: self,
                input,
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

/// Forward peer traversal is reset for each selected batch: repeated and split
/// output demand is legal and never mutates the input's peer/frame facts.
struct PeerCursor<'a> {
    peers: &'a [crate::WindowRowRange],
    group: usize,
    float_rank: f64,
}
impl PeerCursor<'_> {
    fn at(
        &mut self,
        row: usize,
        work: &mut EvaluationCheckpoints<'_>,
    ) -> Result<crate::WindowRowRange, KernelFailure> {
        loop {
            let peer = self.peers.get(self.group).copied();
            work.step()?;
            let peer =
                peer.ok_or_else(|| internal("ranking selected row has no original peer group"))?;
            if row < peer.end {
                return Ok(peer);
            }
            // Preserve the legacy PERCENT_RANK f64 accumulation, including
            // its rounding, rather than casting the cumulative integer start.
            self.float_rank += (peer.end - peer.start) as f64;
            self.group += 1;
            work.step()?;
        }
    }
}

fn emit<'s, T: ArrowPrimitiveType>(
    selection: Selection<'s>,
    target: &arrow_schema::DataType,
    work: &mut EvaluationCheckpoints<'_>,
    mut row_value: impl FnMut(usize, &mut EvaluationCheckpoints<'_>) -> Result<T::Native, KernelFailure>,
) -> Result<SelectedValues<'s>, KernelFailure> {
    output_capacity(selection.len())?;
    work.flush()?;
    let mut values = Vec::new();
    values
        .try_reserve_exact(selection.len())
        .map_err(|_| KernelFailure::ResourceExhausted)?;
    work.flush()?;
    for row in selection.iter() {
        let value = row_value(row, work)?;
        values.push(value);
        work.step()?;
    }
    work.flush()?;
    let array = Arc::new(PrimitiveArray::<T>::new(values.into(), None)) as ArrayRef;
    work.flush()?;
    SelectedValues::try_new_observed::<KernelFailure>(
        selection,
        target,
        array,
        Box::default(),
        || work.step(),
    )
}

impl WindowKernelPartition for RankingPartition<'_> {
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
                        "ranking output differs from its partition or host row grant",
                    ));
                }
                let rows = self.input.full_input().partition_rows();
                let target = &self.prepared.contract.result_type().data_type;
                let mut cursor = PeerCursor {
                    peers: self.input.peers(),
                    group: 0,
                    float_rank: 1.0,
                };
                match self.prepared.operation {
                    RankingOperation::RowNumber
                    | RankingOperation::Rank
                    | RankingOperation::DenseRank => {
                        emit::<Int64Type>(selection, target, &mut work, |row, work| {
                            let value = match self.prepared.operation {
                                RankingOperation::RowNumber => row.checked_add(1),
                                RankingOperation::Rank => {
                                    cursor.at(row, work)?.start.checked_add(1)
                                }
                                RankingOperation::DenseRank => {
                                    cursor.at(row, work)?;
                                    cursor.group.checked_add(1)
                                }
                                _ => unreachable!("integer ranking operation selected above"),
                            };
                            work.step()?;
                            value
                                .and_then(|value| i64::try_from(value).ok())
                                .ok_or(KernelFailure::ResourceExhausted)
                        })
                    }
                    RankingOperation::CumeDist | RankingOperation::PercentRank => {
                        emit::<Float64Type>(selection, target, &mut work, |row, work| {
                            let peer = cursor.at(row, work)?;
                            let size = rows as f64;
                            let value = match self.prepared.operation {
                                RankingOperation::CumeDist => peer.end as f64 / size,
                                RankingOperation::PercentRank if size > 1.0 => {
                                    (cursor.float_rank - 1.0) / (size - 1.0)
                                }
                                RankingOperation::PercentRank => 0.0,
                                _ => unreachable!("floating ranking operation selected above"),
                            };
                            work.step()?;
                            Ok(value)
                        })
                    }
                }
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
#[path = "window_ranking_tests.rs"]
mod tests;
