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

//! Provider-neutral row-mutation match collection and validation.
//!
//! This module deliberately knows only the signed SPI contract.  It neither
//! interprets provider identity values nor derives a physical write strategy.

//! Finite canonical uniqueness storage for one signed COW match consumer.
//! This is a physical container ceiling under the admitted Internal window.

use std::alloc::Layout;
use std::collections::hash_map::RandomState;
use std::ptr::NonNull;
use std::sync::{Arc, Mutex};

use allocator_api2::alloc::{AllocError, Allocator, Global};
use hashbrown::HashSet;
use novarocks_spi::connector::{ConnectorError, ConnectorErrorKind};

const KEY_BYTES: usize = 64 * 1024 * 1024;
// Leave one MiB for the collector's bounded old/new batch-header vectors.
const BOOKKEEPING_BYTES: usize = 7 * 1024 * 1024;
const _: () = assert!(2 * 4096 * size_of::<arrow::record_batch::RecordBatch>() <= 1024 * 1024);
const TRANSFORM_BYTES: usize = 256 * 1024 * 1024;
const KEY_HEADER_BYTES: usize = 64;
// Includes allocator state and bounded uniqueness ordinal/ArrayRef vectors.
const FIXED_BYTES: usize = 256 * 1024;

#[derive(Default)]
struct Usage {
    table: usize,
    keys: usize,
    headers: usize,
    external: usize,
}

struct Limits {
    keys: usize,
    bookkeeping: usize,
    transform: usize,
}
struct State {
    usage: Usage,
    limits: Limits,
}

impl State {
    fn fits(&self, table_add: usize, key_copy: usize) -> bool {
        let u = &self.usage;
        let Some(bookkeeping) = u
            .table
            .checked_add(table_add)
            .and_then(|v| v.checked_add(u.headers))
            .and_then(|v| v.checked_add(FIXED_BYTES))
        else {
            return false;
        };
        bookkeeping <= self.limits.bookkeeping
            && u.keys <= self.limits.keys
            && bookkeeping
                .checked_add(u.keys)
                .and_then(|v| v.checked_add(u.external))
                .and_then(|v| v.checked_add(key_copy))
                .is_some_and(|v| v <= self.limits.transform)
    }
}

#[derive(Clone)]
struct TableAllocator(Arc<Mutex<State>>);

// SAFETY: Every block is allocated and freed by Global with its exact Layout.
// Default Allocator grow/shrink allocate the replacement through allocate,
// so both generations are counted before any replacement allocation occurs.
unsafe impl Allocator for TableAllocator {
    fn allocate(&self, layout: Layout) -> Result<NonNull<[u8]>, AllocError> {
        let mut state = self.0.lock().unwrap();
        if !state.fits(layout.size(), 0) {
            return Err(AllocError);
        }
        let block = Global.allocate(layout)?;
        state.usage.table += layout.size();
        Ok(block)
    }

    unsafe fn deallocate(&self, pointer: NonNull<u8>, layout: Layout) {
        // SAFETY: hashbrown passes the unchanged allocation pointer/layout.
        unsafe { Global.deallocate(pointer, layout) };
        self.0.lock().unwrap().usage.table -= layout.size();
    }
}

pub(super) struct BoundedMutationKeys {
    keys: HashSet<Box<[u8]>, RandomState, TableAllocator>,
}

fn exhausted() -> ConnectorError {
    ConnectorError::new(
        ConnectorErrorKind::ResourceExhausted,
        "COW uniqueness storage exceeds its fixed key, bookkeeping or transform workspace",
    )
}

impl BoundedMutationKeys {
    pub(super) fn new() -> Self {
        Self::with_limits(Limits {
            keys: KEY_BYTES,
            bookkeeping: BOOKKEEPING_BYTES,
            transform: TRANSFORM_BYTES,
        })
    }

    fn with_limits(limits: Limits) -> Self {
        let allocator = TableAllocator(Arc::new(Mutex::new(State {
            usage: Usage::default(),
            limits,
        })));
        Self {
            keys: HashSet::with_hasher_in(RandomState::new(), allocator),
        }
    }

    pub(super) fn retained_bytes(&self) -> usize {
        let state = self.keys.allocator().0.lock().unwrap();
        state.usage.table + state.usage.keys + state.usage.headers + FIXED_BYTES
    }

    /// The caller preflights Arrow conversion with this retained count, then
    /// installs the live Rows/converter/input bound for subsequent key copies.
    pub(super) fn set_external_bytes(&mut self, bytes: usize) -> Result<(), ConnectorError> {
        let mut state = self.keys.allocator().0.lock().unwrap();
        let old = state.usage.external;
        state.usage.external = bytes;
        if !state.fits(0, 0) {
            state.usage.external = old;
            return Err(exhausted());
        }
        Ok(())
    }

    pub(super) fn insert(&mut self, bytes: &[u8]) -> Result<bool, ConnectorError> {
        // Same converter for every batch: canonical byte identity is precisely
        // OwnedRow's equality, without retaining another RowConfig alias.
        if self.keys.contains(bytes) {
            return Ok(false);
        }
        {
            let mut state = self.keys.allocator().0.lock().unwrap();
            let next_keys = state
                .usage
                .keys
                .checked_add(bytes.len())
                .ok_or_else(exhausted)?;
            let next_headers = state
                .usage
                .headers
                .checked_add(KEY_HEADER_BYTES)
                .ok_or_else(exhausted)?;
            let old = (state.usage.keys, state.usage.headers);
            state.usage.keys = next_keys;
            state.usage.headers = next_headers;
            // An exact Vec and its boxed replacement may coexist. The key
            // payload counted above plus this extra copy covers both.
            if !state.fits(0, bytes.len()) {
                (state.usage.keys, state.usage.headers) = old;
                return Err(exhausted());
            }
        }
        let result = (|| {
            self.keys.try_reserve(1).map_err(|_| exhausted())?;
            if !self.keys.allocator().0.lock().unwrap().fits(0, bytes.len()) {
                return Err(exhausted());
            }
            let mut key = Vec::new();
            key.try_reserve_exact(bytes.len())
                .map_err(|_| exhausted())?;
            if key.capacity() != bytes.len() {
                return Err(exhausted());
            }
            key.extend_from_slice(bytes);
            let inserted = self.keys.insert(key.into_boxed_slice());
            debug_assert!(
                inserted,
                "borrowed canonical key was checked before allocation"
            );
            Ok(inserted)
        })();
        if result.is_err() {
            let mut state = self.keys.allocator().0.lock().unwrap();
            state.usage.keys -= bytes.len();
            state.usage.headers -= KEY_HEADER_BYTES;
        }
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn duplicate_keys_do_not_allocate_or_lose_existing_keys_on_refusal() {
        let mut keys = BoundedMutationKeys::with_limits(Limits {
            keys: 5,
            bookkeeping: FIXED_BYTES + 32768,
            transform: FIXED_BYTES + 65536,
        });
        assert!(keys.insert(b"abc").unwrap());
        let retained = keys.retained_bytes();
        assert!(!keys.insert(b"abc").unwrap());
        assert_eq!(keys.retained_bytes(), retained);
        assert!(keys.insert(b"def").is_err());
        assert_eq!(keys.retained_bytes(), retained);
        assert!(!keys.insert(b"abc").unwrap());
    }

    #[test]
    fn hash_table_replacement_counts_old_and_new_before_global_allocation() {
        let allocator = TableAllocator(Arc::new(Mutex::new(State {
            usage: Usage::default(),
            limits: Limits {
                keys: 100,
                bookkeeping: FIXED_BYTES + 48,
                transform: FIXED_BYTES + 65536,
            },
        })));
        let layout = Layout::from_size_align(32, 8).unwrap();
        let block = allocator.allocate(layout).unwrap();
        let pointer = NonNull::new(block.as_ptr() as *mut u8).unwrap();
        // A 32 -> 40 replacement would fit alone, but its 72-byte peak cannot.
        let bigger = Layout::from_size_align(40, 8).unwrap();
        assert!(unsafe { allocator.grow(pointer, layout, bigger) }.is_err());
        assert_eq!(allocator.0.lock().unwrap().usage.table, 32);
        unsafe { allocator.deallocate(pointer, layout) };
        assert_eq!(allocator.0.lock().unwrap().usage.table, 0);
    }

    #[test]
    fn external_rows_and_key_copy_are_checked_together_before_key_growth() {
        let mut keys = BoundedMutationKeys::with_limits(Limits {
            keys: 100,
            bookkeeping: FIXED_BYTES + 32768,
            transform: FIXED_BYTES + 65536,
        });
        assert!(keys.insert(b"first").unwrap());
        let retained = keys.retained_bytes();
        keys.set_external_bytes(FIXED_BYTES + 65536 - retained)
            .unwrap();
        assert!(keys.insert(b"second").is_err());
        assert_eq!(keys.retained_bytes(), retained);
        assert!(!keys.insert(b"first").unwrap());
    }
}
