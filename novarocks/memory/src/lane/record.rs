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

//! Fixed records and the atomic lifetime word shared by hooks and reclamation.
use crate::sync::{AtomicI64, AtomicU32, AtomicU64, Ordering};

pub(crate) const COUNT_BITS: u32 = 40;
pub(crate) const COUNT_MASK: u64 = (1 << COUNT_BITS) - 1;
pub(crate) const COUNT_BIAS: u64 = 1 << (COUNT_BITS - 1);
pub(crate) const SLOT_PIN: u64 = 1 << COUNT_BITS;
pub(crate) const CLAIMED: u64 = 1 << 63;
pub(crate) const ZERO_STATE: u64 = COUNT_BIAS;
pub(crate) const PRODUCTION_MASK: u32 = 3;
pub(crate) const CLASS_SHIFT: u32 = 2;
pub(crate) const CLASS_MASK: u32 = 3 << CLASS_SHIFT;
pub(crate) const DRAINING: u32 = 1 << 4;
pub(crate) const IMMORTAL: u32 = 1 << 5;
pub(crate) const ACTIVE_SCOPE: u32 = 1 << 6;

#[repr(u32)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ProductionState {
    Producing = 0,
    Sealed = 1,
    Stopped = 2,
}
#[repr(u32)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ResponsibilityClass {
    Query = 0,
    Residual = 1,
    Service = 2,
    Unattributed = 3,
}

/// Signed outstanding count, independent slot pins, and one reclaim claim.
/// The bias permits a remote free to precede a buffered positive publication.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct LifetimeState(pub u64);
impl LifetimeState {
    pub const fn outstanding(self) -> i64 {
        (self.0 & COUNT_MASK) as i64 - COUNT_BIAS as i64
    }
    pub const fn pins(self) -> u32 {
        ((self.0 & !CLAIMED) >> COUNT_BITS) as u32
    }
    pub const fn claimed(self) -> bool {
        self.0 & CLAIMED != 0
    }
    pub(crate) const fn delta(count: i64, pins: i64) -> u64 {
        // Do not mask a negative count: its borrow cancels the pin arithmetic.
        (count as u64).wrapping_add((pins as u64).wrapping_mul(SLOT_PIN))
    }
}

#[repr(C, align(64))]
#[derive(Debug)]
pub struct LaneRecord {
    pub(crate) state: AtomicU64,
    pub(crate) tagged: AtomicI64,
    pub(crate) r1_small: AtomicI64,
    pub(crate) sequence: AtomicU64,
    pub(crate) origin: AtomicU64,
    pub(crate) generation: AtomicU32,
    pub(crate) flags: AtomicU32,
    pub(crate) next: AtomicU32,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RecordSnapshot {
    pub tagged_bytes: i64,
    pub r1_small_bytes: i64,
    pub outstanding: i64,
    pub slot_pins: u32,
    pub sequence: u64,
    pub origin: u64,
    pub generation: u32,
    pub flags: u32,
}
macro_rules! record_initializer {
    ($state:expr, $flags:expr, $generation:expr) => {
        Self {
            state: AtomicU64::new($state),
            tagged: AtomicI64::new(0),
            r1_small: AtomicI64::new(0),
            sequence: AtomicU64::new(0),
            origin: AtomicU64::new(0),
            generation: AtomicU32::new($generation),
            flags: AtomicU32::new($flags),
            next: AtomicU32::new(u32::MAX),
        }
    };
}
impl LaneRecord {
    #[cfg(not(loom))]
    pub(crate) const fn vacant() -> Self {
        record_initializer!(ZERO_STATE | CLAIMED, 0, 1)
    }
    #[cfg(loom)]
    pub(crate) fn vacant() -> Self {
        record_initializer!(ZERO_STATE | CLAIMED, 0, 1)
    }
    #[cfg(not(loom))]
    pub(crate) const fn unattributed() -> Self {
        record_initializer!(
            ZERO_STATE,
            IMMORTAL | ((ResponsibilityClass::Unattributed as u32) << CLASS_SHIFT),
            0
        )
    }
    #[cfg(loom)]
    pub(crate) fn unattributed() -> Self {
        record_initializer!(
            ZERO_STATE,
            IMMORTAL | ((ResponsibilityClass::Unattributed as u32) << CLASS_SHIFT),
            0
        )
    }
    pub fn lifetime(&self) -> LifetimeState {
        LifetimeState(self.state.load(Ordering::Acquire))
    }
    /// Independently sampled fields; use RecordStore::snapshot_ref to exclude
    /// observations spanning a generation change.
    pub fn snapshot(&self) -> RecordSnapshot {
        let life = self.lifetime();
        RecordSnapshot {
            tagged_bytes: self.tagged.load(Ordering::Acquire),
            r1_small_bytes: self.r1_small.load(Ordering::Acquire),
            outstanding: life.outstanding(),
            slot_pins: life.pins(),
            sequence: self.sequence.load(Ordering::Acquire),
            origin: self.origin.load(Ordering::Acquire),
            generation: self.generation.load(Ordering::Acquire),
            flags: self.flags.load(Ordering::Acquire),
        }
    }
    pub fn production_state(&self) -> ProductionState {
        match self.flags.load(Ordering::Acquire) & PRODUCTION_MASK {
            0 => ProductionState::Producing,
            1 => ProductionState::Sealed,
            _ => ProductionState::Stopped,
        }
    }
    pub fn responsibility_class(&self) -> ResponsibilityClass {
        match (self.flags.load(Ordering::Acquire) & CLASS_MASK) >> CLASS_SHIFT {
            0 => ResponsibilityClass::Query,
            1 => ResponsibilityClass::Residual,
            2 => ResponsibilityClass::Service,
            _ => ResponsibilityClass::Unattributed,
        }
    }
    pub(crate) fn reset_for_owner(&self, origin: u64, class: ResponsibilityClass) {
        self.tagged.store(0, Ordering::Release);
        self.r1_small.store(0, Ordering::Release);
        self.sequence.store(0, Ordering::Release);
        self.origin.store(origin, Ordering::Release);
        self.flags
            .store((class as u32) << CLASS_SHIFT, Ordering::Release);
        self.state.store(ZERO_STATE, Ordering::Release);
    }
}
#[cfg(not(loom))]
const _: () = assert!(std::mem::size_of::<LaneRecord>() == 64);

#[cfg(all(test, not(loom)))]
mod tests {
    use super::*;
    #[test]
    fn biased_count_and_pin_arithmetic_do_not_borrow_between_fields() {
        for count in [
            -((COUNT_BIAS - 1) as i64),
            -1,
            0,
            1,
            (COUNT_BIAS - 2) as i64,
        ] {
            for pins in [0, 1, (1 << 23) - 2] {
                let raw = (COUNT_BIAS as i64 + count) as u64 + pins as u64 * SLOT_PIN;
                let pinned = LifetimeState(raw.wrapping_add(LifetimeState::delta(0, 1)));
                assert_eq!(pinned.outstanding(), count);
                assert_eq!(pinned.pins(), pins + 1);
                let flushed = LifetimeState(pinned.0.wrapping_add(LifetimeState::delta(1, -1)));
                assert_eq!(flushed.outstanding(), count + 1);
                assert_eq!(flushed.pins(), pins);
                assert!(!flushed.claimed());
            }
        }
        let pinned = ZERO_STATE + SLOT_PIN;
        let negative = LifetimeState(pinned.wrapping_add(LifetimeState::delta(-1, 0)));
        assert_eq!((negative.outstanding(), negative.pins()), (-1, 1));
        let drained = LifetimeState(negative.0.wrapping_add(LifetimeState::delta(1, -1)));
        assert_eq!(drained.0, ZERO_STATE);
        let zero_count = LifetimeState(0);
        assert_eq!(zero_count.outstanding(), -(COUNT_BIAS as i64));
        let maximum = LifetimeState(COUNT_MASK + (((1u64 << 23) - 1) * SLOT_PIN));
        assert_eq!(maximum.outstanding(), COUNT_BIAS as i64 - 1);
        assert_eq!(maximum.pins(), (1 << 23) - 1);
        let reduced = LifetimeState(maximum.0.wrapping_add(LifetimeState::delta(-1, -1)));
        assert_eq!(reduced.outstanding(), COUNT_BIAS as i64 - 2);
        assert_eq!(reduced.pins(), (1 << 23) - 2);
        assert!(!reduced.claimed());
        assert_eq!(LifetimeState(ZERO_STATE).outstanding(), 0);
    }
}
