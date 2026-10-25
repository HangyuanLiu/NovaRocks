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

//! Selection-aligned byte hashes. Local hash algorithms can supply their own
//! state machine; the built-in adapters retain the existing versioned values.
use super::{BYTE_BLOCK, PreparedBytes, RowWork, prepared::Mapping, with_mapping};
use crate::{ValueError, ValueFailure, ValueViolation, decode::RowMapping, hash_eq::NullHash};
use std::alloc::Layout;

/// A concrete hash implementation selected once per batch. The running state
/// is deliberately distinct from the finalized output and the input seed.
pub trait ByteHashKernel {
    type Seed: Copy + Into<u64>;
    type State: Clone;
    fn init(&self, seed: Self::Seed) -> Self::State;
    fn update(&self, state: &mut Self::State, bytes: &[u8]);
    fn finish(&self, state: Self::State) -> u64;
}

/// FNV-1a's seed is its unmodified accumulator.
#[derive(Clone, Copy, Debug, Default)]
pub struct Fnv1a64;
impl ByteHashKernel for Fnv1a64 {
    type Seed = u64;
    type State = u64;
    fn init(&self, seed: u64) -> u64 {
        seed
    }
    fn update(&self, state: &mut u64, bytes: &[u8]) {
        for &byte in bytes {
            *state = (*state ^ u64::from(byte)).wrapping_mul(1099511628211);
        }
    }
    fn finish(&self, state: u64) -> u64 {
        state
    }
}

/// IEEE reflected CRC32. The public seed is a finalized previous CRC; this is
/// not CRC32C and does not use that instruction's different polynomial.
#[derive(Clone, Copy, Debug, Default)]
pub struct Crc32;
impl ByteHashKernel for Crc32 {
    type Seed = u32;
    type State = u32;
    fn init(&self, seed: u32) -> u32 {
        !seed
    }
    fn update(&self, state: &mut u32, bytes: &[u8]) {
        for &byte in bytes {
            *state ^= u32::from(byte);
            for _ in 0..8 {
                *state = (*state >> 1) ^ (0xedb88320u32.wrapping_mul(*state & 1));
            }
        }
    }
    fn finish(&self, state: u32) -> u64 {
        u64::from(!state)
    }
}

/// An existing running state and the value to retain when the logical input
/// is NULL. No conversion from a finalized hash to an opaque state is guessed.
#[derive(Clone, Debug)]
pub struct HashState<S> {
    pub state: S,
    pub preserved_seed: u64,
}

/// Per-row states are in selection-ordinal order, never bottom-row order.
pub enum HashSeed<'a, S, Seed = u64> {
    Uniform(Seed),
    PerRow(&'a [HashState<S>]),
}

pub struct HashOptions<'a, S, Seed = u64> {
    pub seeds: HashSeed<'a, S, Seed>,
    pub nulls: NullHash,
}

#[derive(Clone, Copy, Default)]
struct CachedHash {
    bottom: usize,
    hash: u64,
    occupied: bool,
}

/// Caller-owned scratch. Every call invalidates previous entries, including
/// when the algorithm, seed or source changes. Its allocation is bounded by
/// selected logical rows, not by the original dictionary's bottom length.
/// Reuse follows the decoded mapping: a flattened dictionary has independent
/// bottom rows and no longer exposes its original dictionary identities.
#[derive(Default)]
pub struct HashScratch {
    slots: Vec<CachedHash>,
}
impl HashScratch {
    pub fn new() -> Self {
        Self::default()
    }
    fn prepare<E, G, W>(
        &mut self,
        count: usize,
        growth: &mut G,
        work: &mut W,
    ) -> Result<(), ValueFailure<E>>
    where
        G: FnMut(Layout) -> Result<(), E>,
        W: FnMut(usize) -> Result<(), E>,
    {
        self.slots.clear();
        if count == 0 {
            return Ok(());
        }
        let size = count
            .checked_mul(2)
            .and_then(usize::checked_next_power_of_two)
            .ok_or(ValueError::Invariant(ValueViolation::Bounds))?;
        let layout = Layout::array::<CachedHash>(size)
            .map_err(|_| ValueError::Invariant(ValueViolation::Bounds))?;
        if self.slots.capacity() < size {
            growth(layout).map_err(ValueFailure::Caller)?;
            self.slots
                .try_reserve_exact(size)
                .map_err(|source| ValueError::Allocation {
                    requested: layout.size(),
                    source,
                })?;
        }
        let rows_per_block = (BYTE_BLOCK / std::mem::size_of::<CachedHash>()).max(1);
        while self.slots.len() < size {
            let end = (self.slots.len() + rows_per_block).min(size);
            work((end - self.slots.len()) * std::mem::size_of::<CachedHash>())
                .map_err(ValueFailure::Caller)?;
            self.slots.resize(end, CachedHash::default());
        }
        Ok(())
    }
    fn slot<E, W>(
        &self,
        bottom: usize,
        control: &mut RowWork<'_, W>,
    ) -> Result<usize, ValueFailure<E>>
    where
        W: FnMut(usize) -> Result<(), E>,
    {
        // Mix all original-coordinate bits. Merely multiplying by an odd
        // number preserves low-bit collisions for sparse dictionary strides.
        let mut hash = bottom as u64;
        hash ^= hash >> 30;
        hash = hash.wrapping_mul(0xbf58476d1ce4e5b9);
        hash ^= hash >> 27;
        hash = hash.wrapping_mul(0x94d049bb133111eb);
        hash ^= hash >> 31;
        let mask = self.slots.len() - 1;
        let mut slot = hash as usize & mask;
        loop {
            // Even a long collision chain remains synchronously cancellable.
            control.row()?;
            let entry = self.slots[slot];
            if !entry.occupied || entry.bottom == bottom {
                return Ok(slot);
            }
            slot = (slot + 1) & mask;
        }
    }
}

/// Hashes exactly the prepared selection into caller storage. On failure the
/// output is not committed and the caller must not consume its partial writes.
/// Uniform dictionary/constant values reuse a hash only within this call.
pub fn hash_into<H, E, G, W>(
    input: &PreparedBytes<'_>,
    kernel: &H,
    options: HashOptions<'_, H::State, H::Seed>,
    output: &mut [u64],
    scratch: &mut HashScratch,
    growth: &mut G,
    work: &mut W,
) -> Result<(), ValueFailure<E>>
where
    H: ByteHashKernel,
    G: FnMut(Layout) -> Result<(), E>,
    W: FnMut(usize) -> Result<(), E>,
{
    if output.len() != input.len() {
        return Err(ValueError::Invariant(ValueViolation::Bounds).into());
    }
    if let HashSeed::PerRow(states) = &options.seeds
        && states.len() != input.len()
    {
        return Err(ValueError::Invariant(ValueViolation::Bounds).into());
    }
    work(0).map_err(ValueFailure::Caller)?;
    let cached = matches!(
        input.mapping(),
        RowMapping::Dictionary(_) | RowMapping::Constant { .. }
    ) && matches!(options.seeds, HashSeed::Uniform(_))
        && !input.is_empty();
    // Clear even uncached calls so later reuse cannot observe stale entries.
    let cache_entries = if !cached {
        0
    } else if matches!(input.mapping(), RowMapping::Constant { .. }) {
        1
    } else {
        input.len()
    };
    scratch.prepare(cache_entries, growth, work)?;
    let run = HashRun {
        input,
        kernel,
        nulls: options.nulls,
        output,
        control: RowWork::new(work),
    };
    with_mapping!(input, |mapping| {
        match options.seeds {
            HashSeed::Uniform(seed) => run.uniform(mapping, seed, cached.then_some(scratch)),
            HashSeed::PerRow(states) => run.per_row(mapping, states),
        }
    })
}

struct HashRun<'a, 'd, H: ByteHashKernel, W> {
    input: &'a PreparedBytes<'d>,
    kernel: &'a H,
    nulls: NullHash,
    output: &'a mut [u64],
    control: RowWork<'a, W>,
}
impl<H: ByteHashKernel, W> HashRun<'_, '_, H, W> {
    fn null_hash(&self, preserved: u64) -> u64 {
        match self.nulls {
            NullHash::PreserveSeed => preserved,
            NullHash::Value(value) => value,
        }
    }
    fn hash_bytes<E>(&mut self, mut state: H::State, bytes: &[u8]) -> Result<u64, ValueFailure<E>>
    where
        W: FnMut(usize) -> Result<(), E>,
    {
        for chunk in bytes.chunks(BYTE_BLOCK) {
            self.control.bytes(chunk.len())?;
            self.kernel.update(&mut state, chunk);
        }
        Ok(self.kernel.finish(state))
    }
    fn uniform<M: Mapping, E>(
        mut self,
        mapping: M,
        seed: H::Seed,
        mut cache: Option<&mut HashScratch>,
    ) -> Result<(), ValueFailure<E>>
    where
        W: FnMut(usize) -> Result<(), E>,
    {
        let mut cursor = self.input.selection().cursor();
        let mut ordinal = 0;
        while let Some(row) = cursor.next_observed(&mut |n| self.control.observe_metadata(n))? {
            self.control.row()?;
            let hash = if let Some(bottom) = mapping.index(row) {
                let cache_slot = match cache.as_deref() {
                    Some(cache) => Some(cache.slot(bottom, &mut self.control)?),
                    None => None,
                };
                let cached_hash = cache_slot.and_then(|slot| {
                    cache.as_deref().and_then(|cache| {
                        let entry = cache.slots[slot];
                        entry.occupied.then_some(entry.hash)
                    })
                });
                if let Some(hash) = cached_hash {
                    hash
                } else {
                    let hash = match self.input.view().value(bottom)? {
                        Some(bytes) => self.hash_bytes(self.kernel.init(seed), bytes)?,
                        None => self.null_hash(seed.into()),
                    };
                    if let (Some(cache), Some(slot)) = (cache.as_deref_mut(), cache_slot) {
                        cache.slots[slot] = CachedHash {
                            bottom,
                            hash,
                            occupied: true,
                        };
                    }
                    hash
                }
            } else {
                self.null_hash(seed.into())
            };
            self.output[ordinal] = hash;
            ordinal += 1;
        }
        self.control.flush()
    }
    fn per_row<M: Mapping, E>(
        mut self,
        mapping: M,
        states: &[HashState<H::State>],
    ) -> Result<(), ValueFailure<E>>
    where
        W: FnMut(usize) -> Result<(), E>,
    {
        let mut cursor = self.input.selection().cursor();
        let mut ordinal = 0;
        while let Some(row) = cursor.next_observed(&mut |n| self.control.observe_metadata(n))? {
            self.control.row()?;
            let initial = &states[ordinal];
            self.output[ordinal] = match mapping.index(row) {
                Some(bottom) => match self.input.view().value(bottom)? {
                    Some(bytes) => self.hash_bytes(initial.state.clone(), bytes)?,
                    None => self.null_hash(initial.preserved_seed),
                },
                None => self.null_hash(initial.preserved_seed),
            };
            ordinal += 1;
        }
        self.control.flush()
    }
}

#[cfg(test)]
#[path = "hash_tests.rs"]
mod tests;
