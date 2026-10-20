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

//! Real allocation for the original child-reference table constructors.
//! The allocator receives each actual allocate/grow/deallocate Layout. No
//! source stock, row estimate or separate memory wallet authorizes these tables.
use super::CopyError;
use crate::aggregate_host_allocator::HostAggregateAllocator;
use allocator_api2::vec::Vec as HostVec;
use std::ops::{Deref, DerefMut};

pub(super) enum ChildScratchVec<T> {
    Original(Vec<T>),
    Hosted(HostVec<T, HostAggregateAllocator>),
}
impl<T> From<Vec<T>> for ChildScratchVec<T> {
    fn from(values: Vec<T>) -> Self {
        Self::Original(values)
    }
}
impl<T> FromIterator<T> for ChildScratchVec<T> {
    fn from_iter<I: IntoIterator<Item = T>>(values: I) -> Self {
        Self::Original(values.into_iter().collect())
    }
}
impl<T> ChildScratchVec<T> {
    pub(super) fn try_with_capacity_observed<E: From<CopyError>>(
        capacity: usize,
        allocator: Option<&HostAggregateAllocator>,
        request: &mut Option<&mut dyn FnMut(std::alloc::Layout) -> Result<(), E>>,
    ) -> Result<Self, E> {
        if allocator.is_none() && capacity != 0 && size_of::<T>() != 0 {
            if let Some(request) = request {
                request(
                    std::alloc::Layout::array::<T>(capacity)
                        .map_err(|source| CopyError::layout::<T>(capacity, source))?,
                )?;
            }
        }
        Self::try_with_capacity(capacity, allocator).map_err(Into::into)
    }
    pub(super) fn try_push_observed<E: From<CopyError>>(
        &mut self,
        value: T,
        request: &mut Option<&mut dyn FnMut(std::alloc::Layout) -> Result<(), E>>,
    ) -> Result<(), E> {
        if let Self::Original(values) = self {
            if values.len() == values.capacity() && size_of::<T>() != 0 {
                if let Some(request) = request {
                    // The original std Vec::push growth uses the pinned RawVec
                    // max(double capacity, required, min_non_zero_cap) rule.
                    let minimum = if size_of::<T>() == 1 {
                        8
                    } else if size_of::<T>() <= 1024 {
                        4
                    } else {
                        1
                    };
                    let required = values
                        .len()
                        .checked_add(1)
                        .ok_or_else(|| CopyError::arithmetic(Some(values.len() as u128 + 1)))?;
                    let count = values
                        .capacity()
                        .checked_mul(2)
                        .ok_or_else(|| CopyError::arithmetic(Some(values.capacity() as u128 * 2)))?
                        .max(required)
                        .max(minimum);
                    request(
                        std::alloc::Layout::array::<T>(count)
                            .map_err(|source| CopyError::layout::<T>(count, source))?,
                    )?;
                }
            }
        }
        self.try_push(value).map_err(Into::into)
    }
    pub(super) fn try_with_capacity(
        capacity: usize,
        allocator: Option<&HostAggregateAllocator>,
    ) -> Result<Self, CopyError> {
        match allocator {
            None => {
                std::alloc::Layout::array::<T>(capacity)
                    .map_err(|source| CopyError::layout::<T>(capacity, source))?;
                let mut values = Vec::new();
                values.try_reserve_exact(capacity).map_err(|source| {
                    CopyError::Control(crate::KernelFailure::from_domain(
                        crate::allocation_failure::AllocationRefusal::new(
                            "copy child scratch allocation",
                            source,
                        ),
                    ))
                })?;
                Ok(Self::Original(values))
            }
            Some(allocator) => {
                let mut values = HostVec::new_in(allocator.clone());
                if let Err(error) = values.try_reserve_exact(capacity) {
                    return Err(match values.allocator().recorded_failure() {
                        Some(cause) => CopyError::Control(cause),
                        None => CopyError::Control(crate::KernelFailure::from_domain(
                            crate::allocation_failure::AllocationRefusal::new(
                                "copy hosted child scratch allocation",
                                error,
                            ),
                        )),
                    });
                }
                Ok(Self::Hosted(values))
            }
        }
    }
    pub(super) fn try_collect<I: Iterator<Item = Result<T, CopyError>>>(
        values: I,
        allocator: Option<&HostAggregateAllocator>,
    ) -> Result<Self, CopyError> {
        match allocator {
            None => values
                .collect::<Result<Vec<_>, CopyError>>()
                .map(Self::Original),
            Some(allocator) => {
                let mut result = Self::new(Some(allocator));
                for value in values {
                    result.try_push(value?)?;
                }
                Ok(result)
            }
        }
    }
    pub(super) fn remove(&mut self, index: usize) -> T {
        match self {
            Self::Original(values) => values.remove(index),
            Self::Hosted(values) => values.remove(index),
        }
    }

    pub(super) fn new(allocator: Option<&HostAggregateAllocator>) -> Self {
        match allocator {
            None => Self::Original(Vec::new()),
            Some(allocator) => Self::Hosted(HostVec::new_in(allocator.clone())),
        }
    }
    pub(super) fn try_push(&mut self, value: T) -> Result<(), CopyError> {
        match self {
            Self::Original(values) => {
                values.try_reserve(1).map_err(|source| {
                    CopyError::Control(crate::KernelFailure::from_domain(
                        crate::allocation_failure::AllocationRefusal::new(
                            "copy child scratch growth",
                            source,
                        ),
                    ))
                })?;
                values.push(value);
            }
            Self::Hosted(values) => {
                // Child validation and its first Data have already completed,
                // exactly at the original push site. Admission happens now.
                if let Err(error) = values.try_reserve(1) {
                    return Err(match values.allocator().recorded_failure() {
                        Some(cause) => CopyError::Control(cause),
                        // The real container error remains the source when its
                        // allocator did not publish a nominal host refusal.
                        None => CopyError::Control(crate::KernelFailure::from_domain(
                            crate::allocation_failure::AllocationRefusal::new(
                                "copy hosted child scratch growth",
                                error,
                            ),
                        )),
                    });
                }
                values.push(value);
            }
        }
        Ok(())
    }
}
impl<T> Deref for ChildScratchVec<T> {
    type Target = [T];
    fn deref(&self) -> &[T] {
        match self {
            Self::Original(values) => values.as_slice(),
            Self::Hosted(values) => values.as_slice(),
        }
    }
}
impl<T> DerefMut for ChildScratchVec<T> {
    fn deref_mut(&mut self) -> &mut [T] {
        match self {
            Self::Original(values) => values.as_mut_slice(),
            Self::Hosted(values) => values.as_mut_slice(),
        }
    }
}
impl<'a, T> IntoIterator for &'a ChildScratchVec<T> {
    type Item = &'a T;
    type IntoIter = std::slice::Iter<'a, T>;
    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

#[cfg(test)]
#[path = "child_scratch_tests.rs"]
mod tests;
