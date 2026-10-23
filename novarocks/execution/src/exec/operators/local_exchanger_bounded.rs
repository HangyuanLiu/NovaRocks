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
//! Strict local windows for explicitly hierarchical B1b routes. Ordinary
//! passthrough/hash/handoff queues keep their existing behavior.
use super::*;
use crate::exec::{
    chunk::OperatorStorageLimits,
    pipeline::{
        input::RawInput,
        operator::{ConstructionFailure, InputConstructionFactory},
        shape::{SinkStep, SourceRead},
        step::{NextStep, WaitFact, WaitReason},
        step_context::StepContext,
    },
};
use crate::runtime::{
    fragment::{ExecutionFailure, ExecutionResult},
    mem_tracker::TrackedBytes,
    operator_byte_credit::{
        CreditAcquire, HolderAcquire, HolderPermit, OperatorByteCredit, PortCreditSpec,
        QueuePermit, credit_allocation, credit_contract, credit_layout, credit_observer_backing,
    },
};
use novarocks_type_contract::{
    ValueTypeStorageLifetime,
    owned_resources::layout::{LayoutResourceError, arc_layout},
};
use std::alloc::Layout;
struct Queued {
    chunk: Chunk,
    permit: QueuePermit,
}
pub(super) struct BoundedLocalExchange {
    credit: OperatorByteCredit,
    queues: Mutex<Vec<VecDeque<Queued>>>,
    producer_closed: Mutex<Vec<bool>>,
    producers: usize,
    partitions: usize,
    limits: OperatorStorageLimits,
    workspace_bytes: usize,
    progress: Arc<Observable>,
    _metadata: Arc<TrackedBytes>,
}
pub(crate) struct LocalInputPermit {
    holder: HolderPermit,
}
pub(crate) enum LocalAdmission {
    Granted(LocalInputPermit),
    Pending(WaitFact),
    Closed,
}
// Actual payload/scalar storage drops before its lifetime-only responsibility.
pub(crate) struct LocalContinuation {
    chunk: Chunk,
    routes: Vec<Vec<u32>>,
    cursor: usize,
    rows: u64,
    _scratch: ValueTypeStorageLifetime,
}
fn plus(a: usize, b: usize) -> ExecutionResult<usize> {
    a.checked_add(b)
        .ok_or_else(|| credit_layout(LayoutResourceError::Arithmetic))
}
fn times(a: usize, b: usize) -> ExecutionResult<usize> {
    a.checked_mul(b)
        .ok_or_else(|| credit_layout(LayoutResourceError::Arithmetic))
}
fn array<T>(n: usize) -> ExecutionResult<usize> {
    Layout::array::<T>(n)
        .map(|v| v.size())
        .map_err(|_| credit_layout(LayoutResourceError::Arithmetic))
}
fn arc<T>() -> ExecutionResult<usize> {
    arc_layout(Layout::new::<T>())
        .map(|v| v.size())
        .map_err(credit_layout)
}
fn observable_bytes(waiters: usize) -> ExecutionResult<usize> {
    plus(arc::<Observable>()?, credit_observer_backing(waiters)?)
}
impl LocalExchanger {
    pub(crate) fn new_bound(
        partition_count: usize,
        producer_count: usize,
        partition_spec: LocalExchangePartitionSpec,
        per_contributor_bytes: usize,
        parent: Arc<MemTracker>,
        limits: OperatorStorageLimits,
        workspace_bytes_per_producer: usize,
        minimum_frame_bytes: usize,
    ) -> ExecutionResult<Arc<Self>> {
        if partition_count == 0
            || producer_count == 0
            || workspace_bytes_per_producer == 0
            || !matches!(
                &partition_spec,
                LocalExchangePartitionSpec::HierarchicalInputSlotIds(_)
                    | LocalExchangePartitionSpec::ConditionalKey { .. }
            )
        {
            return Err(credit_contract(
                "strict local exchange requires exact hierarchical endpoints and workspace",
            ));
        }
        let slots = producer_count
            .checked_next_power_of_two()
            .ok_or_else(|| credit_layout(LayoutResourceError::Arithmetic))?;
        let mut metadata = plus(arc::<Self>()?, arc::<Mutex<LocalExchangerState>>()?)?;
        metadata = plus(metadata, arc::<ChunkBufferMemoryManager>()?)?;
        metadata = plus(metadata, arc::<TrackedBytes>()?)?;
        metadata = plus(metadata, array::<VecDeque<Chunk>>(partition_count)?)?;
        metadata = plus(metadata, array::<bool>(partition_count)?)?;
        metadata = plus(metadata, array::<usize>(partition_count)?)?;
        metadata = plus(metadata, array::<AtomicBool>(partition_count)?)?;
        metadata = plus(metadata, times(array::<AtomicU64>(partition_count)?, 4)?)?;
        metadata = plus(metadata, array::<VecDeque<Queued>>(partition_count)?)?;
        metadata = plus(metadata, times(array::<Queued>(slots)?, partition_count)?)?;
        metadata = plus(metadata, array::<bool>(producer_count)?)?;
        metadata = plus(metadata, observable_bytes(0)?)?;
        metadata = plus(metadata, times(observable_bytes(producer_count)?, 2)?)?;
        metadata = plus(
            metadata,
            array::<PortCreditSpec>(plus(partition_count, producer_count)?)?,
        )?;
        #[cfg(target_vendor = "apple")]
        {
            metadata = plus(
                metadata,
                times(std::mem::size_of::<(isize, [u8; 56])>(), 3)?,
            )?;
        }
        let workspace = times(workspace_bytes_per_producer, producer_count)?;
        OperatorByteCredit::check_fixed_reservation(
            per_contributor_bytes,
            producer_count,
            plus(workspace, metadata)?,
        )?;
        let loan = Arc::new(TrackedBytes::try_new(metadata, parent.clone())?);
        let mut specs = Vec::new();
        specs
            .try_reserve_exact(partition_count + producer_count)
            .map_err(credit_allocation)?;
        for _ in 0..partition_count {
            specs.push(PortCreditSpec {
                queue_positions: producer_count,
                holder_positions: producer_count,
                minimum_frame_bytes,
                waiters: producer_count + 1,
            });
        }
        for _ in 0..producer_count {
            specs.push(PortCreditSpec {
                queue_positions: 1,
                holder_positions: 1,
                minimum_frame_bytes,
                waiters: 1,
            });
        }
        OperatorByteCredit::preflight(
            per_contributor_bytes,
            producer_count,
            plus(workspace, metadata)?,
            &specs,
        )?;
        let progress = Arc::new(
            Observable::try_bounded_retained(producer_count, loan.clone())
                .map_err(ExecutionFailure::from_domain)?,
        );
        let closed = Arc::new(
            Observable::try_bounded_retained(producer_count, loan.clone())
                .map_err(ExecutionFailure::from_domain)?,
        );
        let source = Arc::new(
            Observable::try_bounded_retained(0, loan.clone())
                .map_err(ExecutionFailure::from_domain)?,
        );
        let credit = OperatorByteCredit::bind(
            per_contributor_bytes,
            producer_count,
            plus(workspace, metadata)?,
            &specs,
            parent,
            Some(progress.clone()),
        )?;
        let mut queues = Vec::new();
        queues
            .try_reserve_exact(partition_count)
            .map_err(credit_allocation)?;
        for _ in 0..partition_count {
            let mut queue = VecDeque::new();
            queue.try_reserve_exact(slots).map_err(credit_allocation)?;
            if queue.capacity() != slots {
                return Err(credit_contract(
                    "strict local queue differs from pre-admitted Layout",
                ));
            }
            queues.push(queue);
        }
        let mut producer_closed = Vec::new();
        producer_closed
            .try_reserve_exact(producer_count)
            .map_err(credit_allocation)?;
        producer_closed.resize(producer_count, false);
        if queues.capacity() != partition_count
            || producer_closed.capacity() != producer_count
            || specs.capacity() != partition_count + producer_count
        {
            return Err(credit_contract(
                "strict local metadata differs from pre-admitted Layout",
            ));
        }
        let mut exchange = Self::build(
            partition_count,
            partition_count,
            producer_count,
            partition_spec,
            LocalExchangeCapacity::Buffered,
            Arc::new(ChunkBufferMemoryManager::new(
                producer_count,
                i64::try_from(per_contributor_bytes)
                    .map_err(|_| credit_layout(LayoutResourceError::Arithmetic))?,
                i64::MAX,
            )),
        );
        let mutable = Arc::get_mut(&mut exchange).expect("unpublished local exchange");
        mutable.source_observable = source;
        mutable.sink_observable = progress.clone();
        mutable.closed_observable = closed;
        mutable.bounded = Some(BoundedLocalExchange {
            credit,
            queues: Mutex::new(queues),
            producer_closed: Mutex::new(producer_closed),
            producers: producer_count,
            partitions: partition_count,
            limits,
            workspace_bytes: workspace_bytes_per_producer,
            progress,
            _metadata: loan,
        });
        drop(mutable.inner.lock().expect("local exchanger lock"));
        let bounded = mutable.bounded.as_ref().unwrap();
        drop(bounded.queues.lock().expect("strict local queues"));
        drop(
            bounded
                .producer_closed
                .lock()
                .expect("strict local endpoints"),
        );
        Ok(exchange)
    }
    pub(crate) fn is_bounded(&self) -> bool {
        self.bounded.is_some()
    }
    pub(crate) fn source_observable_for_partition(
        &self,
        partition: usize,
    ) -> ExecutionResult<Arc<Observable>> {
        match &self.bounded {
            Some(bounded) => Ok(bounded.credit.port(partition)?.observable()),
            None => Ok(self.source_observable()),
        }
    }
    pub(crate) fn admit_bounded(
        &self,
        producer: i32,
        dop: i32,
        input: &RawInput,
    ) -> ExecutionResult<LocalAdmission> {
        let bounded = self
            .bounded
            .as_ref()
            .ok_or_else(|| credit_contract("strict local input has no bound windows"))?;
        let producer = usize::try_from(producer)
            .map_err(|_| credit_contract("negative strict local producer"))?;
        if usize::try_from(dop).ok() != Some(bounded.producers) || producer >= bounded.producers {
            return Err(credit_contract(
                "strict local producer differs from frozen endpoints",
            ));
        }
        if self.all_consumers_closed() {
            return Ok(LocalAdmission::Closed);
        }
        let parent = bounded.credit.port(bounded.partitions + producer)?;
        let limit = parent
            .max_frame_bytes()
            .min(bounded.credit.port(0)?.max_frame_bytes())
            .min(bounded.limits.bytes);
        let bytes = input.retained_geometry(OperatorStorageLimits {
            bytes: limit,
            ..bounded.limits
        })?;
        match parent.try_queue(bytes)? {
            CreditAcquire::Granted(queue) => Ok(match queue.try_into_holder() {
                HolderAcquire::Granted(holder) => {
                    LocalAdmission::Granted(LocalInputPermit { holder })
                }
                HolderAcquire::Pending { queue, wait } => {
                    drop(queue);
                    LocalAdmission::Pending(wait)
                }
                HolderAcquire::Closed(queue) => {
                    drop(queue);
                    LocalAdmission::Closed
                }
            }),
            CreditAcquire::Pending(wait) => Ok(LocalAdmission::Pending(wait)),
            CreditAcquire::Closed => Ok(LocalAdmission::Closed),
        }
    }
    pub(crate) fn prepare_bounded(
        &self,
        mut chunk: Chunk,
        permit: LocalInputPermit,
        cursor: &mut usize,
        factory: &dyn InputConstructionFactory,
        ctx: &StepContext<'_>,
    ) -> ExecutionResult<LocalContinuation> {
        let bounded = self
            .bounded
            .as_ref()
            .ok_or_else(|| credit_contract("strict local continuation has no windows"))?;
        chunk.retain_operator_holder::<ConstructionFailure>(
            permit.holder.into_storage_lifetime(),
            |_| Ok(()),
        )?;
        let owner = factory
            .begin_typed_construction_bounded(chunk.chunk_schema(), bounded.workspace_bytes)?;
        let rows = self.driver_partition_rows_observed(
            &chunk,
            cursor,
            |keys, rows| {
                crate::exec::hash_table::tuple_partition::native_tuple_hashes_created(
                    keys,
                    rows,
                    owner.as_ref(),
                    &|| ctx.checkpoint(),
                )
            },
            |layout| owner.request(layout).map_err(ExecutionFailure::from),
        )?;
        let count = u64::try_from(chunk.len())
            .map_err(|_| credit_layout(LayoutResourceError::Arithmetic))?;
        Ok(LocalContinuation {
            chunk,
            routes: rows,
            cursor: 0,
            rows: count,
            _scratch: owner.storage_lifetime(),
        })
    }
    pub(crate) fn poll_bounded(
        &self,
        pending: &mut Option<LocalContinuation>,
        factory: &dyn InputConstructionFactory,
        ctx: &StepContext<'_>,
    ) -> ExecutionResult<SinkStep> {
        let bounded = self
            .bounded
            .as_ref()
            .ok_or_else(|| credit_contract("strict local progress has no windows"))?;
        let Some(input) = pending.as_mut() else {
            return Ok(SinkStep::new(NextStep::NeedInput, 0));
        };
        let generation = bounded.progress.generation();
        let count = bounded.partitions;
        for offset in 0..count {
            let partition = (input.cursor + offset) % count;
            if input.routes[partition].is_empty() {
                continue;
            }
            if self.partition_closed(partition) {
                input.routes[partition].clear();
                continue;
            }
            ctx.checkpoint()?;
            let port = bounded.credit.port(partition)?;
            let mut permit = match port.try_queue(port.max_frame_bytes())? {
                CreditAcquire::Granted(permit) => permit,
                CreditAcquire::Pending(_) => continue,
                CreditAcquire::Closed => {
                    input.routes[partition].clear();
                    continue;
                }
            };
            let chunk =
                crate::exec::hash_table::tuple_partition::take_partition_rows_created_bounded(
                    &input.chunk,
                    &input.routes[partition],
                    factory,
                    &|| ctx.checkpoint(),
                    port.max_frame_bytes(),
                )?;
            let bytes = chunk.retained_operator_geometry(OperatorStorageLimits {
                bytes: port.max_frame_bytes().min(bounded.limits.bytes),
                ..bounded.limits
            })?;
            if bytes > permit.bytes() {
                return Err(credit_contract(
                    "strict local output exceeds its reserved queue window",
                ));
            }
            permit.retain_actual(bytes)?;
            let rows = chunk.len() as u64;
            let mut queues = bounded.queues.lock().expect("strict local queues");
            if self.partition_closed(partition) {
                drop(queues);
                drop(chunk);
                drop(permit);
                input.routes[partition].clear();
                continue;
            }
            if queues[partition].len() == queues[partition].capacity() {
                return Err(credit_contract(
                    "strict local output metadata position is missing",
                ));
            }
            queues[partition].push_back(Queued { chunk, permit });
            self.queued_chunks.fetch_add(1, Ordering::AcqRel);
            self.pushed_rows[partition].fetch_add(rows, Ordering::Relaxed);
            self.pushed_chunks[partition].fetch_add(1, Ordering::Relaxed);
            drop(queues);
            input.routes[partition].clear();
            port.observable().notify_observers();
        }
        input.cursor = if input.cursor == count - 1 {
            0
        } else {
            input.cursor + 1
        };
        if input.routes.iter().all(Vec::is_empty) {
            let rows = input.rows;
            pending.take();
            Ok(SinkStep::new(NextStep::NeedInput, rows))
        } else {
            Ok(SinkStep::new(
                NextStep::Wait(WaitFact {
                    reason: WaitReason::Output,
                    observable: bounded.progress.clone(),
                    observed_generation: generation,
                    deadline: None,
                }),
                0,
            ))
        }
    }
    pub(crate) fn finish_producer_at(&self, producer: i32) -> ExecutionResult<bool> {
        if let Some(bounded) = &self.bounded {
            let producer = usize::try_from(producer)
                .map_err(|_| credit_contract("negative strict local producer close"))?;
            let mut closed = bounded
                .producer_closed
                .lock()
                .expect("strict local endpoints");
            let closed = closed
                .get_mut(producer)
                .ok_or_else(|| credit_contract("unknown strict local producer close"))?;
            if *closed {
                return Ok(self.remaining_producers() == 0);
            }
            *closed = true;
        }
        let done = self.finish_producer();
        if let Some(bounded) = &self.bounded {
            if done {
                for partition in 0..bounded.partitions {
                    bounded
                        .credit
                        .port(partition)?
                        .observable()
                        .notify_observers();
                }
            }
        }
        Ok(done)
    }
}
impl BoundedLocalExchange {
    pub(super) fn next_chunk(
        &self,
        partition: usize,
        finished: bool,
    ) -> ExecutionResult<SourceRead> {
        let mut queues = self.queues.lock().expect("strict local queues");
        let queue = queues
            .get_mut(partition)
            .ok_or_else(|| credit_contract("unknown strict local source partition"))?;
        let Some(Queued { mut chunk, permit }) = queue.pop_front() else {
            return Ok(if finished {
                SourceRead::Finished
            } else {
                SourceRead::Pending
            });
        };
        match permit.try_into_holder() {
            HolderAcquire::Granted(holder) => {
                chunk.retain_operator_holder::<ConstructionFailure>(
                    holder.into_storage_lifetime(),
                    |_| Ok(()),
                )?;
                Ok(SourceRead::Chunk(chunk))
            }
            HolderAcquire::Pending { queue: permit, .. } => {
                queue.push_front(Queued { chunk, permit });
                Ok(SourceRead::Pending)
            }
            HolderAcquire::Closed(permit) => {
                drop(queues);
                drop(chunk);
                drop(permit);
                Ok(SourceRead::Finished)
            }
        }
    }
    pub(super) fn close_partition(&self, partition: usize) -> usize {
        if let Ok(port) = self.credit.port(partition) {
            port.close();
        }
        let queue = {
            let mut queues = self.queues.lock().expect("strict local queues");
            std::mem::take(&mut queues[partition])
        };
        let count = queue.len();
        drop(queue);
        self.progress.notify_observers();
        count
    }
    pub(super) fn buffered(&self, partition: usize) -> Option<usize> {
        self.queues
            .lock()
            .expect("strict local queues")
            .get(partition)
            .map(VecDeque::len)
    }
}

#[cfg(test)]
#[path = "local_exchanger_bounded_tests.rs"]
mod tests;
