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

//! One independently redeemable authorization domain.
use crate::sync::{Arc, Mutex, Ordering};
use crate::{
    account::{AccountHandle, Path, grow_locked, qualify},
    error::{CapacityError, MetadataRegistryLabel},
    ids::AccountKind,
    owner::{AllocationOrigin, OwnerRecord},
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DomainSnapshot {
    pub authorized: u64,
    pub live: u64,
    pub external: u64,
    pub committed: u64,
    pub debt: u64,
    pub free: u64,
    pub active: bool,
    pub sealed: bool,
    pub residual: bool,
    pub metadata_bytes: u64,
}
#[derive(Debug)]
pub(crate) struct DomainState {
    pub account: AccountHandle,
    pub authorized: u64,
    pub settled_live: u64,
    pub external: u64,
    pub external_handles: u64,
    pub committed: u64,
    pub active: bool,
    pub sealed: bool,
    pub residual: bool,
    pub query_origin: bool,
    pub generation: u64,
    pub blocked: Option<CapacityError>,
    pub drain_requested: bool,
}
#[derive(Debug)]
pub(crate) struct Domain {
    pub id: u64,
    pub owner: OwnerRecord,
    pub state: Mutex<DomainState>,
    pub metadata: u64,
}
/// A reusable local lane; cloning it cannot create a second concurrent scope.
#[derive(Debug, Clone)]
pub struct FundingDomain(pub(crate) Arc<Domain>);
/// Storage fee includes stable state and both Arc counters. Registry backing
/// is charged separately at assembly, so its slot is not counted twice here.
pub const OWNER_METADATA_BYTES: u64 =
    (std::mem::size_of::<Domain>() + 2 * std::mem::size_of::<usize>()) as u64;

impl AccountHandle {
    pub fn create_domain(&self, authorized: u64) -> Result<FundingDomain, CapacityError> {
        self.create_domain_from(authorized, None)
            .map_err(|e| e.with_requested(authorized))
    }
    fn create_domain_from(
        &self,
        authorized: u64,
        source: Option<&FundingDomain>,
    ) -> Result<FundingDomain, CapacityError> {
        let shared = &self.0.shared;
        let record = Arc::new(Domain {
            id: shared.next_identity()?,
            owner: OwnerRecord::new(self.id()),
            metadata: OWNER_METADATA_BYTES,
            state: Mutex::new(DomainState {
                account: self.clone(),
                authorized,
                settled_live: 0,
                external: 0,
                external_handles: 0,
                committed: authorized,
                active: false,
                sealed: false,
                residual: false,
                generation: 0,
                blocked: None,
                drain_requested: false,
                query_origin: Path::new(self)
                    .nodes
                    .iter()
                    .flatten()
                    .any(|n| n.kind() == AccountKind::Work),
            }),
        });
        let path = Path::new(self);
        let _gates = path.shared_gates();
        let mut source_state = source.map(|d| d.0.state.lock().unwrap());
        if let Some(s) = &source_state {
            if s.account.id() != self.id() || s.active || s.sealed || s.residual {
                return Err(CapacityError::Invalid {
                    detail: "split requires an inactive open source in this account",
                });
            }
            let free = s
                .authorized
                .saturating_sub(source.unwrap().0.owner.live().saturating_add(s.external));
            if authorized > free {
                return Err(CapacityError::Invalid {
                    detail: "split exceeds source free authorization",
                });
            }
        }
        let need = if source.is_some() {
            OWNER_METADATA_BYTES
        } else {
            authorized
                .checked_add(OWNER_METADATA_BYTES)
                .ok_or(CapacityError::Invalid {
                    detail: "domain size overflow",
                })?
        };
        let protected = {
            let s = self.0.ledger.lock().unwrap();
            self.0.control.load(Ordering::Acquire) != 0 && s.floor >= need && s.slack >= need
        };
        qualify(self, &path, authorized, !protected)?;
        let mut states = path.requirement_locks(need);
        let mut registry = shared.domains.lock().unwrap();
        if registry.active >= shared.max_active_owners || registry.free_slots.is_empty() {
            return Err(CapacityError::MetadataExhausted {
                registry: MetadataRegistryLabel::Owners,
                limit: shared.max_active_owners as u64,
            });
        }
        if registry
            .metadata
            .checked_add(OWNER_METADATA_BYTES)
            .is_none_or(|m| m > shared.metadata_budget)
        {
            return Err(CapacityError::MetadataExhausted {
                registry: MetadataRegistryLabel::Owners,
                limit: shared.metadata_budget,
            });
        }
        let growth = need.saturating_sub(states[0].as_ref().unwrap().slack);
        // A concurrent sibling cannot change this account's slack while the
        // ledger is held; prefix locks are sufficient for the exact demand.
        if growth != 0 {
            grow_locked(self, &path, &mut states, growth, true)?;
        }
        states[0].as_mut().unwrap().slack -= need;
        if let Some(s) = &mut source_state {
            // Publication below and source debit are one transaction. No live
            // allocation or external bound is moved by this free-only operation.
            s.authorized -= authorized;
            s.committed -= authorized;
            source
                .unwrap()
                .0
                .owner
                .sequence
                .fetch_add(1, Ordering::Release);
        }
        registry.metadata += OWNER_METADATA_BYTES;
        registry.active += 1;
        let slot = registry
            .free_slots
            .pop()
            .expect("available byte-backed index slot");
        registry.records[slot] = Some(record.clone());
        registry.upper = registry.upper.max(slot + 1);
        shared.membership_revision.fetch_add(1, Ordering::Release);
        Ok(FundingDomain(record))
    }
}
impl FundingDomain {
    /// Split unredeemed rights into a distinct owner in the same account.
    /// Metadata is separately funded; denial leaves the original rights intact.
    /// Live allocations and external bounds retain their exact origin.
    pub fn split_free(&self, bytes: u64) -> Result<Self, CapacityError> {
        self.affiliation()
            .create_domain_from(bytes, Some(self))
            .map_err(|e| e.with_requested(bytes))
    }
    pub fn id(&self) -> u64 {
        self.0.id
    }
    pub fn snapshot(&self) -> DomainSnapshot {
        let s = self.0.state.lock().unwrap();
        let live = self.0.owner.live();
        let obligation = live.checked_add(s.external).expect("valid domain facts");
        DomainSnapshot {
            authorized: s.authorized,
            live,
            external: s.external,
            committed: s.committed,
            debt: obligation.saturating_sub(s.authorized),
            free: s.authorized.saturating_sub(obligation),
            active: s.active,
            sealed: s.sealed,
            residual: s.residual,
            metadata_bytes: self.0.metadata,
        }
    }
    pub fn seal(&self) {
        let mut s = self.0.state.lock().unwrap();
        s.sealed = true;
        self.0.owner.sequence.fetch_add(1, Ordering::Release);
    }
    pub(crate) fn affiliation(&self) -> AccountHandle {
        self.0.state.lock().unwrap().account.clone()
    }
    pub(crate) fn origin(&self) -> AllocationOrigin {
        AllocationOrigin::new(&self.0.owner)
    }
}
#[derive(Debug)]
pub(crate) struct DomainRegistry {
    pub records: Vec<Option<Arc<Domain>>>,
    pub free_slots: Vec<usize>,
    pub active: u32,
    pub metadata: u64,
    pub upper: usize,
}
