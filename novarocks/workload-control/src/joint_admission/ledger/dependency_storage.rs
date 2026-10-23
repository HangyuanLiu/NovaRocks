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

//! Canonical dependency identities in the original ledger's paid inventory.
//! The zero queue contains only indices, never facts or admission authority.
//! Lock order is State -> zero queue; no queue operation calls back into State.

use super::*;
use std::{ops::Deref, sync::Mutex};

struct DependencyCell {
    record: Option<DependencyRecord>,
}
struct DependencySlot {
    cell: Arc<DependencyCell>,
    next_free: Option<usize>,
    published: bool,
}
#[derive(Default)]
struct RowLinks {
    generation: u64,
    aliases: usize,
    previous: Option<usize>,
    next: Option<usize>,
    linked: bool,
}
struct ZeroRows {
    rows: Vec<RowLinks>,
    head: Option<usize>,
    tail: Option<usize>,
}
impl ZeroRows {
    fn unlink(&mut self, index: usize) {
        if !self.rows[index].linked {
            return;
        }
        let previous = self.rows[index].previous.take();
        let next = self.rows[index].next.take();
        match previous {
            Some(previous) => self.rows[previous].next = next,
            None => self.head = next,
        }
        match next {
            Some(next) => self.rows[next].previous = previous,
            None => self.tail = previous,
        }
        self.rows[index].linked = false;
    }
    fn push(&mut self, index: usize) {
        assert!(!self.rows[index].linked);
        assert_eq!(self.rows[index].aliases, 0);
        self.rows[index].previous = self.tail;
        self.rows[index].linked = true;
        match self.tail {
            Some(tail) => self.rows[tail].next = Some(index),
            None => self.head = Some(index),
        }
        self.tail = Some(index);
    }
}

/// Private: every data alias obeys the same actual-exit protocol. No raw Arc
/// or Weak of a cell may escape this module. Sharing a ProfileFacts Arc does
/// not lock the zero queue; only construction and actual graph exit do so.
pub(in crate::joint_admission) struct DependencyRef {
    cell: Option<Arc<DependencyCell>>,
    zero: Arc<Mutex<ZeroRows>>,
    index: usize,
    generation: u64,
}
impl Deref for DependencyRef {
    type Target = DependencyRecord;
    fn deref(&self) -> &Self::Target {
        self.cell.as_ref().unwrap().record.as_ref().unwrap()
    }
}
impl Clone for DependencyRef {
    fn clone(&self) -> Self {
        let mut zero = self.zero.lock().unwrap();
        let row = &mut zero.rows[self.index];
        assert_eq!(row.generation, self.generation);
        assert!(row.aliases > 0 && !row.linked);
        let aliases = row
            .aliases
            .checked_add(1)
            .expect("bounded private dependency aliases");
        let cell = Arc::clone(self.cell.as_ref().unwrap());
        row.aliases = aliases;
        Self {
            cell: Some(cell),
            zero: Arc::clone(&self.zero),
            index: self.index,
            generation: self.generation,
        }
    }
}
impl Drop for DependencyRef {
    fn drop(&mut self) {
        let mut zero = self.zero.lock().unwrap();
        let row = &mut zero.rows[self.index];
        assert_eq!(row.generation, self.generation);
        assert!(row.aliases > 0 && !row.linked);
        // This exits the actual data Arc before publishing zero. A producer
        // cannot pause between zero and queue publication: both use this lock.
        drop(self.cell.take());
        row.aliases -= 1;
        if row.aliases == 0 {
            zero.push(self.index);
        }
        // No arbitrary Drop, waker, allocation or State acquisition follows
        // inside this lock. The owning builder/turn/State mutation wakes WC.
    }
}

pub(super) struct DependencyRegistry {
    canonical: BTreeMap<SharedDependency, usize>,
    slots: Vec<DependencySlot>,
    zero: Arc<Mutex<ZeroRows>>,
    free: Option<usize>,
}
impl DependencyRegistry {
    pub(super) fn new(count: usize) -> Result<Self, WorkError> {
        let mut slots = Vec::new();
        let mut rows = Vec::new();
        slots
            .try_reserve_exact(count)
            .map_err(|_| WorkError::Capacity("joint dependency inventory"))?;
        rows.try_reserve_exact(count)
            .map_err(|_| WorkError::Capacity("joint dependency queue"))?;
        for index in 0..count {
            slots.push(DependencySlot {
                cell: Arc::new(DependencyCell { record: None }),
                next_free: (index + 1 < count).then_some(index + 1),
                published: false,
            });
            rows.push(RowLinks::default());
        }
        let zero = Arc::new(Mutex::new(ZeroRows {
            rows,
            head: None,
            tail: None,
        }));
        // Materialize the source-pinned platform mutex backing at composition.
        drop(zero.lock().unwrap());
        Ok(Self {
            canonical: BTreeMap::new(),
            slots,
            zero,
            free: (count > 0).then_some(0),
        })
    }
    pub(super) fn is_empty(&self) -> bool {
        self.canonical.is_empty()
    }
    pub(super) fn has_zero(&self) -> bool {
        self.zero.lock().unwrap().head.is_some()
    }
    pub(super) fn lookup(&mut self, fact: &SharedDependency) -> Option<DependencyRef> {
        let index = *self.canonical.get(fact)?;
        let mut zero = self.zero.lock().unwrap();
        if zero.rows[index].aliases == 0 {
            zero.unlink(index);
        }
        let row = &mut zero.rows[index];
        let aliases = row
            .aliases
            .checked_add(1)
            .expect("bounded private dependency aliases");
        let cell = Arc::clone(&self.slots[index].cell);
        row.aliases = aliases;
        Some(DependencyRef {
            cell: Some(cell),
            zero: Arc::clone(&self.zero),
            index,
            generation: row.generation,
        })
    }
    pub(super) fn insert(
        &mut self,
        fact: SharedDependency,
        ordinal: u64,
    ) -> Result<DependencyRef, WorkError> {
        assert!(!self.canonical.contains_key(&fact));
        if self.free.is_none() {
            self.reap_one();
        }
        let index = self
            .free
            .ok_or(WorkError::Capacity("joint dependency registry"))?;
        let mut zero = self.zero.lock().unwrap();
        let row = &mut zero.rows[index];
        let generation = row
            .generation
            .checked_add(1)
            .ok_or(WorkError::ArithmeticOverflow)?;
        assert_eq!(row.aliases, 0);
        assert!(!row.linked);
        let slot = &mut self.slots[index];
        let cell = Arc::get_mut(&mut slot.cell).expect("zero aliases exited before reuse");
        assert!(cell.record.is_none());
        self.free = slot.next_free.take();
        cell.record = Some(DependencyRecord {
            ordinal,
            fact: fact.clone(),
        });
        slot.published = false;
        row.generation = generation;
        row.aliases = 1;
        self.canonical.insert(fact, index);
        Ok(DependencyRef {
            cell: Some(Arc::clone(&slot.cell)),
            zero: Arc::clone(&self.zero),
            index,
            generation,
        })
    }
    pub(super) fn mark_published(&mut self, record: &DependencyRef) {
        assert!(Arc::ptr_eq(&self.zero, &record.zero));
        let slot = &mut self.slots[record.index];
        assert!(Arc::ptr_eq(&slot.cell, record.cell.as_ref().unwrap()));
        slot.published = true;
    }
    /// O(1) queue removal plus the original canonical BTree removal. Data
    /// destruction is closed SharedDependency/Arc<str>, with bounded width;
    /// there is no user callback or next-stage resource acquisition here.
    pub(super) fn reap_one(&mut self) -> bool {
        let mut zero = self.zero.lock().unwrap();
        let Some(index) = zero.head else { return false };
        assert_eq!(zero.rows[index].aliases, 0);
        let slot = &mut self.slots[index];
        let cell = Arc::get_mut(&mut slot.cell).expect("all actual data aliases exited");
        let record = cell.record.take().expect("occupied zero row");
        let removed = self.canonical.remove(&record.fact);
        assert_eq!(removed, Some(index));
        // Both key and value payloads actually exit before the slot is free.
        drop(record);
        zero.unlink(index);
        slot.published = false;
        slot.next_free = self.free;
        self.free = Some(index);
        true
    }
}

pub(super) fn backing_bytes(count: usize) -> Result<usize, WorkError> {
    use crate::control_storage::{
        arc_bytes, arc_slice_bytes, array_bytes, btree_peak_bytes, sum_bytes,
    };
    let origin = novarocks_resource_contract::NetworkServiceOrigin::MAX_BYTES;
    sum_bytes([
        btree_peak_bytes::<SharedDependency, usize>(count)?,
        array_bytes::<DependencySlot>(count)?,
        array_bytes::<RowLinks>(count)?,
        arc_bytes::<Mutex<ZeroRows>>()?,
        count
            .checked_mul(sum_bytes([
                arc_bytes::<DependencyCell>()?,
                arc_slice_bytes::<u8>(origin)?,
            ])?)
            .ok_or(WorkError::ArithmeticOverflow)?,
        if cfg!(target_os = "macos") { 64 } else { 0 },
    ])
}

#[cfg(test)]
mod tests {
    use super::*;
    use novarocks_resource_contract::NetworkServiceOrigin;

    fn network(name: usize) -> SharedDependency {
        SharedDependency::Network(
            NetworkServiceOrigin::from_selected_url(
                &url::Url::parse(&format!("https://dep-{name}.example")).unwrap(),
            )
            .unwrap(),
        )
    }

    #[test]
    fn canonical_aliases_revive_and_only_actual_exit_allows_reuse() {
        let mut registry = DependencyRegistry::new(1).unwrap();
        let a = registry.insert(SharedDependency::LocalHost, 1).unwrap();
        let alias = a.clone();
        drop(a);
        assert!(!registry.reap_one());
        assert!(matches!(
            registry.insert(network(1), 2),
            Err(WorkError::Capacity(_))
        ));
        drop(alias);
        let revived = registry.lookup(&SharedDependency::LocalHost).unwrap();
        assert_eq!(revived.ordinal, 1);
        assert!(!registry.reap_one());
        drop(revived);
        // A new builder reclaims at most one indexed zero row itself. It does
        // not depend on an unbounded retain/scan or a background reaper race.
        let b = registry.insert(network(2), 3).unwrap();
        assert!(registry.lookup(&SharedDependency::LocalHost).is_none());
        assert_eq!(b.ordinal, 3);
        assert_eq!(b.generation, 2);
        drop(b);
        assert!(registry.reap_one());
        assert!(registry.is_empty());
    }

    #[test]
    fn zero_fifo_unlinks_head_middle_and_tail_on_canonical_revival() {
        let mut registry = DependencyRegistry::new(3).unwrap();
        let a = registry.insert(network(0), 1).unwrap();
        let b = registry.insert(network(1), 2).unwrap();
        let c = registry.insert(network(2), 3).unwrap();
        drop(a);
        drop(b);
        drop(c);
        let middle = registry.lookup(&network(1)).unwrap();
        let head = registry.lookup(&network(0)).unwrap();
        let tail = registry.lookup(&network(2)).unwrap();
        assert!(!registry.reap_one());
        drop(middle);
        drop(head);
        drop(tail);
        for fact in [network(1), network(0), network(2)] {
            assert!(registry.reap_one());
            assert!(!registry.canonical.contains_key(&fact));
        }
        assert!(!registry.reap_one());
    }

    #[test]
    fn only_successful_original_grant_promotes_a_provisional_row() {
        let mut registry = DependencyRegistry::new(1).unwrap();
        let a = registry.insert(SharedDependency::LocalHost, 1).unwrap();
        let b = registry.lookup(&SharedDependency::LocalHost).unwrap();
        assert!(!registry.slots[0].published);
        drop(a);
        assert!(!registry.slots[0].published);
        registry.mark_published(&b);
        assert!(registry.slots[0].published);
        drop(b);
        assert!(registry.reap_one());
        let c = registry.insert(network(1), 2).unwrap();
        assert!(!registry.slots[0].published);
        drop(c);
    }

    #[test]
    fn concurrent_graph_exit_and_canonical_revival_preserve_one_identity() {
        let mut registry = DependencyRegistry::new(1).unwrap();
        let original = registry.insert(SharedDependency::LocalHost, 1).unwrap();
        std::thread::scope(|scope| {
            let exit = scope.spawn(move || {
                // Many private aliases share one immutable actual record.
                let aliases: Vec<_> = (0..128).map(|_| original.clone()).collect();
                drop(original);
                drop(aliases);
            });
            for _ in 0..128 {
                let current = registry.lookup(&SharedDependency::LocalHost).unwrap();
                assert_eq!(current.ordinal, 1);
                assert_eq!(current.generation, 1);
                drop(current);
            }
            exit.join().unwrap();
        });
        assert!(registry.reap_one());
        let replacement = registry.insert(network(1), 2).unwrap();
        assert_eq!(replacement.generation, 2);
        assert_eq!(replacement.ordinal, 2);
        drop(replacement);
        assert!(registry.reap_one());
    }
}
