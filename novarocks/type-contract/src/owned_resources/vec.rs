// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

//! One locked default-Global Vec growth projection from actual length/capacity.
//! Layouts describe library requests, not allocator CPU, retained stock or a
//! MEM grant. Element destruction remains with the actual element owner.

use super::{copy::reserve_exit, profile::LOCKED_TOOLCHAIN};
use crate::{CompileCheckpoints, CompileControlError, ControlResourceError};
use std::{alloc::Layout, mem::size_of};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct VecPushGrowthFacts {
    pub next_len: usize,
    pub requested_capacity: usize,
    pub old_backing: Option<Layout>,
    pub requested_backing: Option<Layout>,
}

fn resource() -> ControlResourceError {
    CompileControlError::ResourceExhausted.into()
}

fn push_geometry<T>(
    len: usize,
    capacity: usize,
) -> Result<VecPushGrowthFacts, ControlResourceError> {
    if !LOCKED_TOOLCHAIN {
        return Err(ControlResourceError::SourceModel(
            "Vec growth source model drift",
        ));
    }
    let next_len = len.checked_add(1).ok_or_else(resource)?;
    if len > capacity {
        return Err(ControlResourceError::SourceModel(
            "Vec length exceeds actual capacity",
        ));
    }
    let size = size_of::<T>();
    if size == 0 {
        if capacity != usize::MAX {
            return Err(ControlResourceError::SourceModel(
                "zero-sized Vec capacity source model drift",
            ));
        }
        return Ok(VecPushGrowthFacts {
            next_len,
            requested_capacity: capacity,
            old_backing: None,
            requested_backing: None,
        });
    }
    let old_backing = if capacity == 0 {
        None
    } else {
        Some(Layout::array::<T>(capacity).map_err(|_| resource())?)
    };
    if next_len <= capacity {
        return Ok(VecPushGrowthFacts {
            next_len,
            requested_capacity: capacity,
            old_backing,
            requested_backing: None,
        });
    }
    // Rust 1.92 RawVec::grow_amortized: both push and try_reserve(1)
    // use this minimum and doubling. No private RawVec fields are borrowed.
    let minimum = if size == 1 {
        8
    } else if size <= 1024 {
        4
    } else {
        1
    };
    let requested_capacity = capacity
        .checked_mul(2)
        .ok_or_else(resource)?
        .max(next_len)
        .max(minimum);
    let requested_backing = Some(Layout::array::<T>(requested_capacity).map_err(|_| resource())?);
    Ok(VecPushGrowthFacts {
        next_len,
        requested_capacity,
        old_backing,
        requested_backing,
    })
}

/// Capture the next original push request from this actual default-Global Vec.
/// A full new layout is a cumulative request contribution; old/new coexistence
/// and cleanup are separate caller facts. Spare capacity requests no backing.
pub fn push_growth<T>(source: &Vec<T>) -> Result<VecPushGrowthFacts, ControlResourceError> {
    push_geometry::<T>(source.len(), source.capacity())
}

/// Admit the original amortized reserve before observing or requesting it.
/// This leaf neither pushes an element nor owns an entry/footer. The caller
/// retains the original push and its completed work. A reserve refusal is
/// primary and has no later completed step or opaque-exit callback.
pub fn reserve_for_push_in<T, E: From<ControlResourceError> + From<CompileControlError>>(
    source: &mut Vec<T>,
    admit: &mut impl FnMut(&VecPushGrowthFacts) -> Result<(), E>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), E> {
    let facts = push_growth(source).map_err(E::from)?;
    admit(&facts)?;
    if facts.requested_backing.is_some() {
        work.flush().map_err(E::from)?;
        let reserved = source.try_reserve(1);
        if reserved.is_ok() {
            work.step().map_err(E::from)?;
        }
        reserve_exit::<E>(reserved, work)?;
    }
    Ok(())
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct VecBoxFacts {
    pub old_backing: Option<Layout>,
    pub requested_backing: Option<Layout>,
    pub result_backing: Option<Layout>,
}

fn box_geometry<T>(len: usize, capacity: usize) -> Result<VecBoxFacts, ControlResourceError> {
    if !LOCKED_TOOLCHAIN {
        return Err(ControlResourceError::SourceModel(
            "Vec boxing source model drift",
        ));
    }
    if len > capacity {
        return Err(ControlResourceError::SourceModel(
            "Vec length exceeds actual capacity",
        ));
    }
    if size_of::<T>() == 0 {
        if capacity != usize::MAX {
            return Err(ControlResourceError::SourceModel(
                "zero-sized Vec capacity source model drift",
            ));
        }
        // RawVec::current_memory is None for ZSTs, including len==MAX.
        // Boxing does not append and therefore has no len+1 arithmetic.
        return Ok(VecBoxFacts {
            old_backing: None,
            requested_backing: None,
            result_backing: None,
        });
    }
    let old_backing = if capacity == 0 {
        None
    } else {
        Some(Layout::array::<T>(capacity).map_err(|_| resource())?)
    };
    let result_backing = if len == 0 {
        None
    } else {
        Some(Layout::array::<T>(len).map_err(|_| resource())?)
    };
    // Rust 1.92 Vec::shrink_to_fit calls RawVec only when cap>len.
    // A zero target frees old backing; it requests no zero-byte allocation.
    let requested_backing = if capacity > len { result_backing } else { None };
    Ok(VecBoxFacts {
        old_backing,
        requested_backing,
        result_backing,
    })
}

/// Capture the original default-Global Vec-to-Box trim request. Empty spare
/// backing is freed; equal capacity is reused; a nonempty trim requests the
/// full new layout. The result fact distinguishes reuse from empty freeing.
pub fn box_facts<T>(source: &Vec<T>) -> Result<VecBoxFacts, ControlResourceError> {
    box_geometry::<T>(source.len(), source.capacity())
}

/// Admit and perform the original owned Vec-to-Box operation on caller work.
/// This Result covers parent/control refusal, not recoverable allocator OOM:
/// the original standard-library shrink allocation handler does not return.
/// Caller-owned cleanup admission must cover T destruction on errors both
/// before conversion (Vec) and after conversion (Box). Layouts cannot bound
/// arbitrary T::drop or allocator CPU. This leaf owns no entry or footer.
pub fn boxed_slice_in<T, E: From<ControlResourceError> + From<CompileControlError>>(
    source: Vec<T>,
    admit: &mut impl FnMut(&VecBoxFacts) -> Result<(), E>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<Box<[T]>, E> {
    let facts = box_facts(&source).map_err(E::from)?;
    admit(&facts)?;
    work.flush().map_err(E::from)?;
    let output = source.into_boxed_slice();
    work.step().map_err(E::from)?;
    work.flush().map_err(E::from)?;
    Ok(output)
}

#[cfg(test)]
mod tests;
