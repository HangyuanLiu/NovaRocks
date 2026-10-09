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
    pub(super) fn try_with_capacity(
        capacity: usize,
        allocator: Option<&HostAggregateAllocator>,
    ) -> Result<Self, CopyError> {
        match allocator {
            None => Ok(Self::Original(Vec::with_capacity(capacity))),
            Some(allocator) => {
                let mut values = HostVec::new_in(allocator.clone());
                if values.try_reserve_exact(capacity).is_err() {
                    return Err(match values.allocator().recorded_failure() {
                        Some(cause) => CopyError::Control(cause),
                        None => CopyError::Extent,
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
            Self::Original(values) => values.push(value),
            Self::Hosted(values) => {
                // Child validation and its first Data have already completed,
                // exactly at the original push site. Admission happens now.
                if let Err(_error) = values.try_reserve(1) {
                    return Err(match values.allocator().recorded_failure() {
                        Some(cause) => CopyError::Control(cause),
                        // No host refusal means the container layout overflowed.
                        // Representability is not retyped as a host MEM cause.
                        None => CopyError::Extent,
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
