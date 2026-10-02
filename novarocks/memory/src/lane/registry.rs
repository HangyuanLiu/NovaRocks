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

//! Control-plane ownership for unfunded observation lanes. Tokens remain
//! pointer-free, while this bounded registry retains lifecycle handoff access.
use super::{LaneHandle, handle::LaneShared};
use crate::{
    account::Path,
    authority::Shared,
    sync::{Arc, Ordering},
};
#[derive(Debug)]
pub(crate) struct ObservationRegistry {
    pub records: Vec<Option<LaneHandle>>,
    pub upper: usize,
    pub cursor: usize,
}
impl ObservationRegistry {
    pub fn new(capacity: usize) -> Self {
        Self {
            records: (0..capacity).map(|_| None).collect(),
            upper: 0,
            cursor: 0,
        }
    }
}
/// This metadata is reported, not charged to authorization in observation-only S1.
pub const OBSERVATION_LANE_METADATA_BYTES: u64 = (std::mem::size_of::<LaneShared>()
    + std::mem::size_of::<crate::membership::MemberNode>()
    + 4 * std::mem::size_of::<usize>()
    + 64) as u64;
impl Shared {
    pub(crate) fn retain_observation(&self, lane: &LaneHandle) {
        let index = lane.reference().index as usize;
        let mut registry = self.observation_lanes.lock().unwrap();
        assert!(registry.records[index].is_none());
        registry.records[index] = Some(lane.clone());
        registry.upper = registry.upper.max(index + 1);
    }
    pub(crate) fn prune_observations(&self, budget: usize) {
        let visits = budget.min(self.observation_lanes.lock().unwrap().upper);
        for _ in 0..visits {
            let (index, lane) = {
                let mut registry = self.observation_lanes.lock().unwrap();
                if registry.cursor >= registry.upper {
                    registry.cursor = 0;
                }
                let index = registry.cursor;
                registry.cursor += 1;
                (index, registry.records[index].clone())
            };
            let Some(lane) = lane else {
                continue;
            };
            if !lane.reclaimable_with_handles(2) {
                continue;
            }
            let account = lane.affiliation();
            let path = Path::new(&account);
            let _gates = path.exclusive_gates();
            if lane.affiliation().id() != account.id() {
                continue;
            }
            let member = lane.detach_member();
            let removed = {
                let mut registry = self.observation_lanes.lock().unwrap();
                if !registry.records[index]
                    .as_ref()
                    .is_some_and(|published| Arc::ptr_eq(&published.0, &lane.0))
                    || !lane.reclaimable_with_handles(2)
                {
                    lane.restore_member(&member);
                    continue;
                }
                registry.records[index].take()
            };
            let reference = lane.reference();
            drop(removed);
            drop(lane);
            drop(member);
            assert!(
                self.record_store.store().reclaim_exact(reference),
                "final observation owner must complete exact record reclamation"
            );
            self.membership_revision.fetch_add(1, Ordering::Release);
        }
    }
}
