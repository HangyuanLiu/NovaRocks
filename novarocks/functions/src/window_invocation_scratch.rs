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

//! Narrow owned structural scratch for the tracked window host. Every backing
//! block uses the existing ONE allocator's exact actual Layout and first typed
//! refusal journal. This is neither a budget nor an opaque Arrow-copy grant.
use crate::aggregate_host_allocator::HostAggregateAllocator;
use crate::{AggregateStateAllocator, KernelEvaluationControl, KernelFailure};
use allocator_api2::vec::Vec as HostVec;
use std::{ops::Deref, sync::Arc};

pub struct WindowInvocationScratch<T> {
    values: HostVec<T, HostAggregateAllocator>,
}
impl<T> WindowInvocationScratch<T> {
    pub fn try_new(
        host: Arc<dyn AggregateStateAllocator>,
        control: &dyn KernelEvaluationControl,
    ) -> Result<Self, KernelFailure> {
        control.checkpoint(0)?;
        let allocator = HostAggregateAllocator::try_new(host)?;
        control.checkpoint(0)?;
        Ok(Self {
            values: HostVec::new_in(allocator),
        })
    }
    pub fn try_with_capacity(
        capacity: usize,
        host: Arc<dyn AggregateStateAllocator>,
        control: &dyn KernelEvaluationControl,
    ) -> Result<Self, KernelFailure> {
        let mut result = Self::try_new(host, control)?;
        result.try_reserve(capacity, control)?;
        Ok(result)
    }
    pub fn try_reserve(
        &mut self,
        additional: usize,
        control: &dyn KernelEvaluationControl,
    ) -> Result<(), KernelFailure> {
        if additional <= self.values.capacity() - self.values.len() {
            return Ok(());
        }
        control.checkpoint(0)?;
        self.values
            .try_reserve_exact(additional)
            .map_err(|_| self.values.allocator().take_failure())?;
        control.checkpoint(0)?;
        Ok(())
    }
    pub fn try_push(
        &mut self,
        value: T,
        control: &dyn KernelEvaluationControl,
    ) -> Result<(), KernelFailure> {
        self.try_reserve(1, control)?;
        self.values.push(value);
        control.checkpoint(1)?;
        Ok(())
    }
    pub fn as_slice(&self) -> &[T] {
        self.values.as_slice()
    }
    pub fn as_mut_slice(&mut self) -> &mut [T] {
        self.values.as_mut_slice()
    }
    pub fn retained_bytes(&self) -> usize {
        self.values.capacity() * size_of::<T>() + self.values.allocator().metadata_bytes()
    }
}
impl<T> Deref for WindowInvocationScratch<T> {
    type Target = [T];
    fn deref(&self) -> &Self::Target {
        self.values.as_slice()
    }
}

impl<'a, T> IntoIterator for &'a WindowInvocationScratch<T> {
    type Item = &'a T;
    type IntoIter = std::slice::Iter<'a, T>;
    fn into_iter(self) -> Self::IntoIter {
        self.values.iter()
    }
}
