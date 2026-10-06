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

//! `Bytes` whose last alias returns a NovaRocks-owned grant.
//!
//! Built only on the upstream `Bytes::from_owner` API. The owner and its
//! guard drop together when the final clone or slice of the returned `Bytes`
//! drops, including aliases still held by Tonic/Hyper/H2 while a frame is
//! being written. The owner (the actual backing) drops before the guard, so a
//! credit carried by the guard is never returned while its backing is live.
//!
//! The only allocation this does not cover with the guard is the fixed-size
//! wrapper that `from_owner` boxes; it is freed right after the guard drops.
//! Callers charge [`owner_wrapper_bytes`] for it once per concurrently
//! retiring position, never against a per-delivery credit.
//!
//! Design: ADR-0168 (docs/adr/ADR-0168-third-party-crates-are-bounded-by-public-configuration-not-forked.md)

use std::alloc::Layout;
use std::sync::atomic::AtomicUsize;

use bytes::Bytes;

/// Field order is the drop order: the backing exits before its guard.
struct GuardedOwner<T, G> {
    owner: T,
    _guard: G,
}

impl<T: AsRef<[u8]>, G> AsRef<[u8]> for GuardedOwner<T, G> {
    fn as_ref(&self) -> &[u8] {
        self.owner.as_ref()
    }
}

/// Wrap `owner` so that `guard` drops right after the backing it covers,
/// when the last alias of the returned `Bytes` drops.
pub fn bytes_with_exit_guard<T, G>(owner: T, guard: G) -> Bytes
where
    T: AsRef<[u8]> + Send + 'static,
    G: Send + 'static,
{
    Bytes::from_owner(GuardedOwner {
        owner,
        _guard: guard,
    })
}

/// Mirrors the private `#[repr(C)] Owned<T> { ref_cnt, owner }` box that
/// `Bytes::from_owner` allocates. Pinned by a counting-allocator test.
#[repr(C)]
struct OwnedWrapperLayout<T> {
    _ref_cnt: AtomicUsize,
    _owner: T,
}

/// Bytes of the single wrapper allocation behind [`bytes_with_exit_guard`].
pub fn owner_wrapper_bytes<T, G>() -> usize {
    Layout::new::<OwnedWrapperLayout<GuardedOwner<T, G>>>()
        .pad_to_align()
        .size()
}

/// Bytes of the shared header `BytesMut::freeze` allocates once a buffer is
/// split or frozen: `{ Vec<u8>, original_capacity_repr, ref_count }`.
/// Pinned by a counting-allocator test.
pub const BYTES_MUT_SHARED_HEADER_BYTES: usize =
    std::mem::size_of::<Vec<u8>>() + 2 * std::mem::size_of::<usize>();
