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

//! Locked Rust Arc and bytes Shared request geometry, without allocation.
//! Shared is a conservative private-layout upper bound. The actual caller
//! owns conditional creation and each later promotion or clone occurrence.

use super::profile::{LOCKED_TOOLCHAIN, require_locked_bytes_request_model};
use std::{
    alloc::Layout,
    mem::{align_of, size_of},
    sync::atomic::AtomicUsize,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LayoutResourceError {
    SourceModel,
    ArcHeader,
    ArcBacking,
    Arithmetic,
    BytesShared,
}

pub fn arc_layout(payload: Layout) -> Result<Layout, LayoutResourceError> {
    if !LOCKED_TOOLCHAIN {
        return Err(LayoutResourceError::SourceModel);
    }
    // Rust 1.98.1 alloc/sync.rs ArcInner is repr(C, align(2)): strong/weak
    // counters followed by the actual payload, including an empty slice.
    let counters = Layout::new::<[AtomicUsize; 2]>();
    let header = counters
        .align_to(counters.align().max(2))
        .map_err(|_| LayoutResourceError::ArcHeader)?
        .pad_to_align();
    header
        .extend(payload)
        .map(|(layout, _)| layout.pad_to_align())
        .map_err(|_| LayoutResourceError::ArcBacking)
}

pub fn bytes_shared_upper() -> Result<usize, LayoutResourceError> {
    require_locked_bytes_request_model();
    if !LOCKED_TOOLCHAIN {
        return Err(LayoutResourceError::SourceModel);
    }
    // bytes 1.11 From<Vec> reuses its payload and conditionally boxes Shared.
    // Private repr(Rust) fields plus per-field padding are an upper bound,
    // never an exact private mirror. An empty Vec with spare capacity can
    // also take this branch; no condition is inferred from payload length.
    let alignment = align_of::<*mut u8>()
        .max(align_of::<usize>())
        .max(align_of::<AtomicUsize>());
    let fields = size_of::<*mut u8>()
        .checked_add(size_of::<usize>())
        .and_then(|n| n.checked_add(size_of::<AtomicUsize>()))
        .ok_or(LayoutResourceError::Arithmetic)?;
    let size = (alignment - 1)
        .checked_mul(3)
        .and_then(|n| fields.checked_add(n))
        .ok_or(LayoutResourceError::Arithmetic)?;
    Layout::from_size_align(size, alignment)
        .map(|layout| layout.pad_to_align().size())
        .map_err(|_| LayoutResourceError::BytesShared)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn arc_actual_payload_alignment_empty_slice_and_counter_goldens() {
        assert_eq!(size_of::<usize>(), 8);
        for (size, alignment, expected_size, expected_alignment) in [
            (0, 1, 16, 8),
            (1, 1, 24, 8),
            (8, 8, 24, 8),
            (3, 32, 64, 32),
            (32, 32, 64, 32),
        ] {
            let actual = arc_layout(Layout::from_size_align(size, alignment).unwrap()).unwrap();
            assert_eq!(
                (actual.size(), actual.align()),
                (expected_size, expected_alignment)
            );
        }
    }

    #[test]
    fn arc_backing_extent_overflow_remains_checked_without_allocation() {
        let payload = Layout::from_size_align(isize::MAX as usize, 1).unwrap();
        assert_eq!(arc_layout(payload), Err(LayoutResourceError::ArcBacking));
    }

    #[test]
    fn shared_private_padding_upper_is_not_an_exact_or_conditional_request() {
        assert_eq!(size_of::<usize>(), 8);
        // Three eight-byte fields, each allowing seven padding bytes: 45,
        // rounded to eight-byte alignment = 48. No payload-length parameter.
        assert_eq!(bytes_shared_upper(), Ok(48));
    }
}
