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

//! The original hash-set storage shape with explicit host allocation.
use super::aggregate_distinct_numeric::{
    DistinctComputationError, NumericDistinctBuffer, NumericDistinctSet,
};
use crate::aggregate_host_allocator::HostAggregateAllocator;
use crate::kernel_input::EvaluationCheckpoints;
use crate::{AggregateStateAllocator, KernelFailure};
use allocator_api2::vec::Vec as HostVec;
use hashbrown::{Equivalent, HashSet, hash_map::DefaultHashBuilder};
use std::sync::Arc;

struct Lookup<'a>(&'a [u8]);
impl std::hash::Hash for Lookup<'_> {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        std::hash::Hash::hash(self.0, state);
    }
}
impl Equivalent<HostVec<u8, HostAggregateAllocator>> for Lookup<'_> {
    fn equivalent(&self, value: &HostVec<u8, HostAggregateAllocator>) -> bool {
        self.0 == value.as_slice()
    }
}

#[derive(Debug)]
pub(super) struct NumericDistinctState {
    pub(super) allocator: HostAggregateAllocator,
    values:
        HashSet<HostVec<u8, HostAggregateAllocator>, DefaultHashBuilder, HostAggregateAllocator>,
    key_backing_bytes: usize,
    pub(super) failed: bool,
}
impl NumericDistinctState {
    pub(super) fn new(host: Arc<dyn AggregateStateAllocator>) -> Result<Self, KernelFailure> {
        let allocator = HostAggregateAllocator::try_new(host)?;
        Ok(Self {
            values: HashSet::with_hasher_in(DefaultHashBuilder::default(), allocator.clone()),
            allocator,
            key_backing_bytes: 0,
            failed: false,
        })
    }
    pub(super) fn retained_bytes(&self) -> usize {
        self.allocator.metadata_bytes()
            + self.values.raw_table().allocation_info().1.size()
            + self.key_backing_bytes
    }
    pub(super) fn insert(
        &mut self,
        bytes: &[u8],
        work: &mut EvaluationCheckpoints<'_>,
    ) -> Result<(), DistinctComputationError> {
        if self.failed {
            return Err(KernelFailure::InstanceFailed.into());
        }
        work.flush()?;
        let found = self.values.contains(&Lookup(bytes));
        work.flush()?;
        if found {
            return Ok(());
        }
        self.values
            .try_reserve(1)
            .map_err(|_| self.values.allocator().take_failure())?;
        work.flush()?;
        let mut key = HostVec::new_in(self.allocator.clone());
        key.try_reserve_exact(bytes.len())
            .map_err(|_| key.allocator().take_failure())?;
        work.flush()?;
        for byte in bytes {
            key.push(*byte);
            work.step()?;
        }
        let next = self
            .key_backing_bytes
            .checked_add(key.capacity())
            .ok_or(KernelFailure::ResourceExhausted)?;
        work.flush()?;
        // No allocation follows: the original reservation holds this entry.
        if self.values.insert(key) {
            self.key_backing_bytes = next;
        }
        work.flush()?;
        Ok(())
    }
    pub(super) fn buffer(&self) -> NumericBuffer {
        NumericBuffer {
            bytes: HostVec::new_in(self.allocator.clone()),
        }
    }
}
impl NumericDistinctSet for NumericDistinctState {
    fn len(&self) -> usize {
        self.values.len()
    }
    fn keys(&self) -> impl Iterator<Item = &[u8]> {
        self.values.iter().map(|value| value.as_slice())
    }
}
pub(super) struct NumericBuffer {
    pub(super) bytes: HostVec<u8, HostAggregateAllocator>,
}
impl NumericDistinctBuffer for NumericBuffer {
    fn reserve_exact(&mut self, size: usize) -> Result<(), String> {
        self.reserve_exact_typed(size)
            .map_err(DistinctComputationError::into_legacy_message)
    }
    fn reserve_exact_typed(&mut self, size: usize) -> Result<(), DistinctComputationError> {
        self.bytes
            .try_reserve_exact(size)
            .map_err(|_| self.bytes.allocator().take_failure().into())
    }
    fn append(&mut self, bytes: &[u8]) {
        self.bytes.extend_from_slice(bytes);
    }
}

#[cfg(test)]
#[path = "aggregate_distinct_dynamic_tests.rs"]
mod tests;
