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

//! Fixed storage geometry and original control-claim transitions.

use super::*;
use crate::control_storage::{ControlSlot, ControlStorageConfig};

/// The actual storage envelope used by configuration. This deliberately covers
/// this substrate only, not actor mailboxes, Provider parsing or complete roles.
pub(crate) fn control_storage_base(
    config: ControlStorageConfig,
) -> Result<JointResources, WorkError> {
    use crate::control_storage::{
        arc_bytes, btree_peak_bytes, resources_for_bytes, storage_cell_bytes,
    };
    let count = config.count()?;
    if count == 0 || config.children_per_slot > count.saturating_sub(1) {
        return Err(WorkError::InvalidConfig("fixed control storage geometry"));
    }
    if !cfg!(any(target_os = "macos", target_os = "linux")) {
        return Err(WorkError::InvalidConfig("fixed control storage platform"));
    }
    let cell = storage_cell_bytes(config.children_per_slot)?;
    let facts = arc_bytes::<ProfileFacts>()?
        .checked_add(arc_bytes::<ProfileKey>()?)
        .and_then(|n| n.checked_add(2 * std::mem::size_of::<std::sync::atomic::AtomicUsize>()))
        .ok_or(WorkError::ArithmeticOverflow)?;
    let mut bytes = cell
        .checked_add(facts)
        .and_then(|n| n.checked_mul(count))
        .and_then(|n| n.checked_add(std::mem::size_of::<CancellationStorage>()))
        .ok_or(WorkError::ArithmeticOverflow)?;
    // Fixed inventory reserves the original maps' retained roots and one
    // insertion path peak. These are whole-map amounts, counted exactly once.
    for map in [
        btree_peak_bytes::<WorkId, crate::scope::Node>(count)?,
        btree_peak_bytes::<u64, ClaimRecord>(count)?,
        btree_peak_bytes::<Arc<ProfileKey>, ProfileRecord>(count)?,
    ] {
        bytes = bytes
            .checked_add(map)
            .ok_or(WorkError::ArithmeticOverflow)?;
    }
    // Evenly reserve this one physical envelope across its original members.
    // Rounding reserves at most count-1 extra bytes; it is not an RSS multiplier.
    let per_slot = bytes
        .checked_add(count - 1)
        .ok_or(WorkError::ArithmeticOverflow)?
        / count;
    resources_for_bytes(per_slot)
}

impl WorkloadControl {
    /// Internal storage-only composition. There is deliberately no Server call
    /// or public safe-profile API until FirstBind/roles/Provider closure exists.
    pub(crate) fn configure_control_storage(
        &self,
        config: ControlStorageConfig,
    ) -> Result<(), WorkError> {
        let count = config.count()?;
        let base = control_storage_base(config)?;
        if count != self.inner.config.scope_records_limit {
            return Err(WorkError::InvalidConfig("fixed control scope inventory"));
        }
        self.inner.update(|state| {
            if state.ready
                || state.closed
                || !state.nodes.is_empty()
                || !state.cancellations.slots.is_empty()
            {
                return Err(WorkError::Conflict);
            }
            let ledger = state.joint.as_ref().ok_or(WorkError::NotReady)?;
            if ledger.driver_closed
                || ledger.active.is_some()
                || !ledger.claims.is_empty()
                || !ledger.requests.is_empty()
            {
                return Err(WorkError::Conflict);
            }
            let declared = &ledger.config.config;
            if declared.max_claims < count {
                return Err(WorkError::InvalidConfig("fixed control claim inventory"));
            }
            let total_scopes = u64::try_from(count).map_err(|_| WorkError::ArithmeticOverflow)?;
            let totals = |n: usize| -> Result<JointResources, WorkError> {
                let mut result = JointResources::ZERO;
                for index in 0..RESOURCE_COUNT {
                    result.0[index] = base.0[index]
                        .checked_mul(u64::try_from(n).map_err(|_| WorkError::ArithmeticOverflow)?)
                        .ok_or(WorkError::ArithmeticOverflow)?;
                }
                Ok(result)
            };
            if declared.global.global.get(JointResource::ScopeRecords) != total_scopes
                || declared.lanes[JointLane::Custody as usize]
                    .global
                    .get(JointResource::ScopeRecords)
                    != total_scopes
                || !declared.global.global.covers(totals(count)?)
                || !declared.lanes[JointLane::Custody as usize]
                    .global
                    .covers(totals(count)?)
            {
                return Err(WorkError::InvalidConfig("fixed global custody inventory"));
            }
            for (class, n) in config.slots_per_class.into_iter().enumerate() {
                let n64 = u64::try_from(n).map_err(|_| WorkError::ArithmeticOverflow)?;
                if declared.classes[class]
                    .global
                    .get(JointResource::ScopeRecords)
                    != n64
                    || declared.class_lanes[class][JointLane::Custody as usize]
                        .global
                        .get(JointResource::ScopeRecords)
                        != n64
                    || !declared.classes[class].global.covers(totals(n)?)
                    || !declared.class_lanes[class][JointLane::Custody as usize]
                        .global
                        .covers(totals(n)?)
                {
                    return Err(WorkError::InvalidConfig("fixed class custody inventory"));
                }
            }
            state
                .next_id
                .checked_add(total_scopes)
                .ok_or(WorkError::ArithmeticOverflow)?;
            let mut slots = Vec::new();
            slots
                .try_reserve_exact(count)
                .map_err(|_| WorkError::Capacity("fixed control inventory"))?;
            let mut claims = BTreeMap::new();
            let mut profiles: BTreeMap<Arc<ProfileKey>, ProfileRecord> = BTreeMap::new();
            let mut empty_facts = std::array::from_fn(|_| None);
            let mut free_heads = [None; 5];
            let classes = [
                WorkClass::Query,
                WorkClass::MaterializedView,
                WorkClass::Statistics,
                WorkClass::TableMaintenance,
                WorkClass::Management,
            ];
            for (class, n) in config.slots_per_class.into_iter().enumerate() {
                if n == 0 {
                    continue;
                }
                let facts = Arc::new(ProfileFacts {
                    key: Arc::new(ProfileKey {
                        class,
                        lane: JointLane::Custody as usize,
                        dependencies: Vec::new(),
                    }),
                    class: classes[class],
                    lane: JointLane::Custody,
                    dependencies: Arc::from(Vec::<Arc<DependencyRecord>>::new()),
                });
                empty_facts[class] = Some(Arc::clone(&facts));
                free_heads[class] = Some(SlotId(slots.len()));
                for offset in 0..n {
                    let cell =
                        crate::cancellation::Cancellation::preallocated(config.children_per_slot)?;
                    let claim = state.next_id()?;
                    let id = SlotId(slots.len());
                    slots.push(ControlSlot {
                        cell: Some(cell),
                        class: classes[class],
                        phase: SlotPhase::Free,
                        generation: 0,
                        claim: Some(claim),
                        base,
                        parent: None,
                        next_free: (offset + 1 < n).then_some(SlotId(id.0 + 1)),
                    });
                    claims.insert(
                        claim,
                        ClaimRecord {
                            owner: ClaimOwner::ControlSlot(id),
                            facts: Arc::clone(&facts),
                            amounts: base,
                            active_facts: Arc::clone(&facts),
                            active_amounts: base,
                            pending: None,
                            structure_revision: 0,
                            release_revision: 0,
                        },
                    );
                    let profile = profiles
                        .entry(Arc::clone(&facts.key))
                        .or_insert(ProfileRecord {
                            facts: Arc::clone(&facts),
                            amounts: JointResources::ZERO,
                            claims: 0,
                        });
                    profile.amounts = profile.amounts.checked_add(base)?;
                    profile.claims += 1;
                }
            }
            let ledger = state.joint.as_mut().unwrap();
            ledger.claims = claims;
            ledger.profiles = profiles;
            ledger.control_empty_facts = empty_facts;
            state.cancellations = CancellationStorage {
                slots,
                fixed: true,
                retired: 0,
                free_heads,
                legacy_vacant: None,
            };
            Ok(())
        })
    }
}

impl JointLedger {
    pub(crate) fn control_is_empty(&self, claim: u64) -> bool {
        self.claims.get(&claim).is_some_and(|record| {
            record.facts.dependencies.is_empty()
                && record.active_facts.dependencies.is_empty()
                && record.pending.is_none()
        })
    }

    pub(crate) fn activate_control_slot(
        &mut self,
        slot: SlotId,
        claim: u64,
    ) -> Result<(), WorkError> {
        let record = self.claims.get_mut(&claim).ok_or(WorkError::Released)?;
        if self.driver_closed
            || record.owner != ClaimOwner::ControlSlot(slot)
            || record.pending.is_some()
        {
            return Err(WorkError::Conflict);
        }
        record.structure_revision = record
            .structure_revision
            .checked_add(1)
            .ok_or(WorkError::ArithmeticOverflow)?;
        Ok(())
    }

    pub(crate) fn reset_control_slot(&mut self, slot: SlotId, claim: u64, base: JointResources) {
        let record = self.claims.get_mut(&claim).expect("original fixed claim");
        assert_eq!(record.owner, ClaimOwner::ControlSlot(slot));
        assert!(
            record.pending.is_none(),
            "collected control slot has no pending reply"
        );
        let from = Arc::clone(&record.facts);
        let amount = record.amounts;
        let to = Arc::clone(
            self.control_empty_facts[class_index(record.facts.class)]
                .as_ref()
                .unwrap(),
        );
        record.facts = Arc::clone(&to);
        record.active_facts = Arc::clone(&to);
        record.amounts = base;
        record.active_amounts = base;
        match record.structure_revision.checked_add(1) {
            Some(revision) => record.structure_revision = revision,
            None => self.driver_closed = true,
        }
        subtract_profile(self, &from, amount);
        add_profile(self, Arc::clone(&to), base);
        if from.key != to.key {
            self.layout_changed();
        }
        self.released();
    }
}

#[cfg(test)]
impl JointLedger {
    pub(crate) fn control_snapshot_for_test(&self) -> (usize, usize, JointResources, usize) {
        let mut amount = JointResources::ZERO;
        let mut records = 0;
        let mut bound = 0;
        for record in self.claims.values() {
            if matches!(record.owner, ClaimOwner::ControlSlot(_)) {
                amount = amount.checked_add(record.amounts).unwrap();
                records += 1;
                bound += usize::from(!record.facts.dependencies.is_empty());
            }
        }
        (
            records,
            self.profiles.values().map(|profile| profile.claims).sum(),
            amount,
            bound,
        )
    }

    pub(crate) fn control_has_domains_for_test(
        &self,
        claim: u64,
        expected: &[SharedDependency],
    ) -> bool {
        let record = &self.claims[&claim];
        [&record.facts, &record.active_facts]
            .into_iter()
            .all(|facts| {
                facts.dependencies.len() == expected.len()
                    && expected.iter().all(|fact| {
                        facts
                            .dependencies
                            .iter()
                            .any(|dependency| &dependency.fact == fact)
                    })
            })
    }

    pub(crate) fn control_revision_for_test(&self, claim: u64) -> u64 {
        self.claims[&claim].structure_revision
    }

    /// Establish an existing domain-bound fixture without pretending that this
    /// test setup has driven the separate FirstBind protocol.
    pub(crate) fn bind_control_fixture(&mut self, slot: SlotId, claim: u64, ordinal: u64) {
        let record = &self.claims[&claim];
        assert_eq!(record.owner, ClaimOwner::ControlSlot(slot));
        assert!(record.facts.dependencies.is_empty());
        let from = Arc::clone(&record.facts);
        let amount = record.amounts;
        let dependency = Arc::new(DependencyRecord {
            ordinal,
            fact: SharedDependency::LocalHost,
        });
        let to = Arc::new(ProfileFacts {
            key: Arc::new(ProfileKey {
                class: class_index(from.class),
                lane: from.lane as usize,
                dependencies: vec![ordinal],
            }),
            class: from.class,
            lane: from.lane,
            dependencies: Arc::from(vec![dependency]),
        });
        subtract_profile(self, &from, amount);
        add_profile(self, Arc::clone(&to), amount);
        let record = self.claims.get_mut(&claim).unwrap();
        record.facts = Arc::clone(&to);
        record.active_facts = to;
        record.structure_revision += 1;
        self.layout_changed();
    }
}
