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
//! Bounded, fair UNION ALL fan-in. Each input occurrence owns independent
//! queue/consumer windows and each producer endpoint closes exactly once.

use crate::exec::{
    chunk::{Chunk, OperatorStorageLimits},
    pipeline::shape::SourceRead,
};
use crate::runtime::{
    fragment::{ExecutionFailure, ExecutionResult},
    mem_tracker::{MemTracker, TrackedBytes},
    observable::Observable,
    operator_byte_credit::{
        CreditAcquire, HolderAcquire, OperatorByteCredit, PortCreditSpec, QueuePermit,
        credit_allocation, credit_contract, credit_layout, credit_observer_backing,
    },
};
use novarocks_type_contract::owned_resources::layout::{LayoutResourceError, arc_layout};
use std::{
    alloc::Layout,
    collections::VecDeque,
    sync::{Arc, Mutex},
};

struct QueuedChunk {
    chunk: Chunk,
    permit: QueuePermit,
}
struct InputState {
    queue: VecDeque<QueuedChunk>,
    closed: Vec<bool>,
}
struct State {
    inputs: Vec<InputState>,
    cursor: usize,
    closed: bool,
}
struct Shared {
    state: Mutex<State>,
    credit: OperatorByteCredit,
    observable: Arc<Observable>,
    limits: OperatorStorageLimits,
    _metadata: Arc<TrackedBytes>,
}
#[derive(Clone)]
pub(crate) struct UnionAllSharedState(Arc<Shared>);
#[derive(Clone, Copy, Debug)]
pub(crate) struct UnionProducer {
    input: usize,
    driver: usize,
}

fn plus(a: usize, b: usize) -> ExecutionResult<usize> {
    a.checked_add(b)
        .ok_or_else(|| credit_layout(LayoutResourceError::Arithmetic))
}
fn array<T>(n: usize) -> ExecutionResult<usize> {
    Layout::array::<T>(n)
        .map(|v| v.size())
        .map_err(|_| credit_layout(LayoutResourceError::Arithmetic))
}

impl UnionAllSharedState {
    pub(crate) fn new_bound(
        input_producers: &[usize],
        _node_id: i32,
        per_contributor_bytes: usize,
        parent: Arc<MemTracker>,
        limits: OperatorStorageLimits,
        minimum_frame_bytes: usize,
    ) -> ExecutionResult<Self> {
        if input_producers.is_empty() || input_producers.contains(&0) {
            return Err(credit_contract(
                "UnionAll requires exact nonempty producer inputs",
            ));
        }
        let contributors = input_producers
            .iter()
            .try_fold(0usize, |n, v| n.checked_add(*v))
            .ok_or_else(|| credit_layout(LayoutResourceError::Arithmetic))?;
        let mut metadata = plus(
            arc_layout(Layout::new::<Shared>())
                .map_err(credit_layout)?
                .size(),
            array::<InputState>(input_producers.len())?,
        )?;
        metadata = plus(
            metadata,
            arc_layout(Layout::new::<TrackedBytes>())
                .map_err(credit_layout)?
                .size(),
        )?;
        metadata = plus(
            metadata,
            arc_layout(Layout::new::<Observable>())
                .map_err(credit_layout)?
                .size(),
        )?;
        metadata = plus(metadata, credit_observer_backing(1)?)?;
        #[cfg(target_vendor = "apple")]
        {
            metadata = plus(metadata, std::mem::size_of::<(isize, [u8; 56])>())?;
        }
        // One queue and holder metadata position per actual producer permits
        // all endpoints to publish once, including zero-byte/zero-row frames.
        // Deque backing is fixed before publication and never grows.
        for &count in input_producers {
            let slots = count
                .checked_next_power_of_two()
                .ok_or_else(|| credit_layout(LayoutResourceError::Arithmetic))?;
            metadata = plus(metadata, array::<QueuedChunk>(slots)?)?;
            metadata = plus(metadata, array::<bool>(count)?)?;
        }
        // Temporary spec backing is part of preparation's peak, still under E.
        metadata = plus(metadata, array::<PortCreditSpec>(input_producers.len())?)?;
        OperatorByteCredit::check_fixed_reservation(per_contributor_bytes, contributors, metadata)?;
        let loan = Arc::new(TrackedBytes::try_new(metadata, parent.clone())?);
        let mut specs = Vec::new();
        specs
            .try_reserve_exact(input_producers.len())
            .map_err(credit_allocation)?;
        for &count in input_producers {
            specs.push(PortCreditSpec {
                queue_positions: count,
                holder_positions: count,
                minimum_frame_bytes,
                waiters: count,
            });
        }
        OperatorByteCredit::preflight(per_contributor_bytes, contributors, metadata, &specs)?;
        let observable = Arc::new(
            Observable::try_bounded_retained(1, loan.clone())
                .map_err(ExecutionFailure::from_domain)?,
        );
        let credit = OperatorByteCredit::bind(
            per_contributor_bytes,
            contributors,
            metadata,
            &specs,
            parent,
            Some(observable.clone()),
        )?;
        let mut inputs = Vec::new();
        inputs
            .try_reserve_exact(input_producers.len())
            .map_err(credit_allocation)?;
        if inputs.capacity() != input_producers.len() {
            return Err(credit_contract(
                "UnionAll input backing differs from admitted Layout",
            ));
        }
        for &count in input_producers {
            let mut queue = VecDeque::new();
            let slots = count
                .checked_next_power_of_two()
                .ok_or_else(|| credit_layout(LayoutResourceError::Arithmetic))?;
            queue.try_reserve_exact(slots).map_err(credit_allocation)?;
            if queue.capacity() != slots {
                return Err(credit_contract(
                    "UnionAll queue backing differs from admitted Layout",
                ));
            }
            let mut closed = Vec::new();
            closed.try_reserve_exact(count).map_err(credit_allocation)?;
            if closed.capacity() != count {
                return Err(credit_contract(
                    "UnionAll endpoint backing differs from admitted Layout",
                ));
            }
            closed.resize(count, false);
            inputs.push(InputState { queue, closed });
        }
        let shared = Self(Arc::new(Shared {
            state: Mutex::new(State {
                inputs,
                cursor: 0,
                closed: false,
            }),
            credit,
            observable,
            limits,
            _metadata: loan,
        }));
        drop(shared.0.state.lock().expect("UnionAll state lock"));
        Ok(shared)
    }
    pub(crate) fn producer(
        &self,
        input: usize,
        driver: usize,
        dop: usize,
    ) -> ExecutionResult<UnionProducer> {
        let state = self.0.state.lock().expect("UnionAll state lock");
        let branch = state
            .inputs
            .get(input)
            .ok_or_else(|| credit_contract("unknown UnionAll input occurrence"))?;
        if dop != branch.closed.len() || driver >= dop {
            return Err(credit_contract(
                "UnionAll producer differs from frozen endpoint",
            ));
        }
        Ok(UnionProducer { input, driver })
    }
    pub(crate) fn input_limits(
        &self,
        producer: UnionProducer,
    ) -> ExecutionResult<OperatorStorageLimits> {
        let port = self.0.credit.port(producer.input)?;
        Ok(OperatorStorageLimits {
            bytes: self.0.limits.bytes.min(port.max_frame_bytes()),
            ..self.0.limits
        })
    }
    pub(crate) fn acquire(
        &self,
        producer: UnionProducer,
        bytes: usize,
    ) -> ExecutionResult<CreditAcquire> {
        {
            let state = self.0.state.lock().expect("UnionAll state lock");
            if state.closed {
                return Ok(CreditAcquire::Closed);
            }
            let input = state
                .inputs
                .get(producer.input)
                .ok_or_else(|| credit_contract("unknown UnionAll input"))?;
            if input.closed.get(producer.driver).copied() != Some(false) {
                return Err(credit_contract("UnionAll input follows endpoint close"));
            }
        }
        self.0.credit.port(producer.input)?.try_queue(bytes)
    }
    pub(crate) fn push_chunk(
        &self,
        producer: UnionProducer,
        chunk: Chunk,
        permit: QueuePermit,
    ) -> ExecutionResult<()> {
        // No normalization/copy is permitted between original admission and
        // enqueue. The passthrough encoding policy retains every carrier.
        let bytes = chunk.retained_operator_geometry(self.input_limits(producer)?)?;
        if bytes > permit.bytes() {
            return Err(credit_contract(
                "UnionAll prepared input exceeds its original queue reservation",
            ));
        }
        let mut state = self.0.state.lock().expect("UnionAll state lock");
        if state.closed {
            return Ok(());
        }
        let input = &mut state.inputs[producer.input];
        if input.closed[producer.driver] {
            return Err(credit_contract(
                "UnionAll publication follows endpoint close",
            ));
        }
        if input.queue.len() == input.queue.capacity() {
            return Err(credit_contract(
                "UnionAll queue metadata position is missing",
            ));
        }
        input.queue.push_back(QueuedChunk { chunk, permit });
        drop(state);
        self.0.observable.notify_observers();
        Ok(())
    }
    pub(crate) fn next_chunk(&self) -> ExecutionResult<SourceRead> {
        let mut state = self.0.state.lock().expect("UnionAll state lock");
        if state.closed {
            return Ok(SourceRead::Finished);
        }
        let count = state.inputs.len();
        for offset in 0..count {
            let index = (state.cursor + offset) % count;
            let Some(QueuedChunk { mut chunk, permit }) = state.inputs[index].queue.pop_front()
            else {
                continue;
            };
            match permit.try_into_holder() {
                HolderAcquire::Granted(holder) => {
                    // Both this shell and possible composite hold were paid
                    // for by the port's fixed holder metadata positions.
                    chunk.retain_operator_holder::<crate::exec::pipeline::operator::ConstructionFailure>(holder.into_storage_lifetime(), |_|Ok(()))?;
                    state.cursor = (index + 1) % count;
                    return Ok(SourceRead::Chunk(chunk));
                }
                HolderAcquire::Pending { queue, .. } => {
                    state.inputs[index].queue.push_front(QueuedChunk {
                        chunk,
                        permit: queue,
                    })
                }
                HolderAcquire::Closed(permit) => {
                    drop(chunk);
                    drop(permit);
                }
            }
        }
        if state
            .inputs
            .iter()
            .all(|v| v.queue.is_empty() && v.closed.iter().all(|v| *v))
        {
            Ok(SourceRead::Finished)
        } else {
            Ok(SourceRead::Pending)
        }
    }
    pub(crate) fn producer_finished(&self, producer: UnionProducer) -> ExecutionResult<()> {
        let mut state = self.0.state.lock().expect("UnionAll state lock");
        if state.closed {
            return Ok(());
        }
        let input = state
            .inputs
            .get_mut(producer.input)
            .ok_or_else(|| credit_contract("unknown UnionAll endpoint close"))?;
        let endpoint = input
            .closed
            .get_mut(producer.driver)
            .ok_or_else(|| credit_contract("unknown UnionAll driver close"))?;
        *endpoint = true;
        drop(state);
        self.0.observable.notify_observers();
        Ok(())
    }
    pub(crate) fn close(&self) {
        // Close admission first; then detach payloads under the state lock and
        // destroy them outside it. Holder releases may notify this same source.
        self.0.credit.close();
        let queues = {
            let mut state = self.0.state.lock().expect("UnionAll state lock");
            if state.closed {
                return;
            }
            state.closed = true;
            std::mem::take(&mut state.inputs)
        };
        drop(queues);
        self.0.observable.notify_observers();
    }
    pub(crate) fn is_closed(&self) -> bool {
        self.0.state.lock().expect("UnionAll state lock").closed
    }
    pub(crate) fn observable(&self) -> Arc<Observable> {
        self.0.observable.clone()
    }
    pub(crate) fn producer_observable(
        &self,
        producer: UnionProducer,
    ) -> ExecutionResult<Arc<Observable>> {
        Ok(self.0.credit.port(producer.input)?.observable())
    }
}

#[cfg(test)]
#[path = "union_all_bounded_tests.rs"]
mod tests;
