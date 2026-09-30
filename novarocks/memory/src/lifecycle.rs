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

//! Teardown is evidence of actual executor exit, never a timeout or task label.
use crate::{
    account::{AccountHandle, Path, node_publish},
    domain::FundingDomain,
};
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IoExitEvidence {
    pub owner: u64,
    pub cancel_started_ns: u64,
    pub exit_bound_ns: u64,
    pub max_inflight: u32,
    pub actually_exited: bool,
}
#[derive(Debug)]
pub struct TeardownEvidence<'a> {
    pub tasks_exited: bool,
    pub operators_destroyed: bool,
    pub io: &'a [IoExitEvidence],
    pub now_ns: u64,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TeardownError {
    ExecutionPending,
    IoPending { owner: u64 },
    TeardownDeadlineExceeded { owner: u64 },
    InvalidIoBound { owner: u64 },
    ActiveScope,
    ExternalResponsibility,
    CannotRetireRoot,
    ProtectedControlOwner,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TransferReceipt {
    pub transferred_payload: u64,
    pub transferred_metadata: u64,
    pub returned_idle: u64,
    pub retired_records: u64,
}
impl TeardownEvidence<'_> {
    fn validate(&self) -> Result<(), TeardownError> {
        if !self.tasks_exited || !self.operators_destroyed {
            return Err(TeardownError::ExecutionPending);
        }
        for io in self.io {
            if io.exit_bound_ns == 0 || io.cancel_started_ns.checked_add(io.exit_bound_ns).is_none()
            {
                return Err(TeardownError::InvalidIoBound { owner: io.owner });
            }
            if !io.actually_exited {
                return Err(if self.now_ns >= io.cancel_started_ns + io.exit_bound_ns {
                    TeardownError::TeardownDeadlineExceeded { owner: io.owner }
                } else {
                    TeardownError::IoPending { owner: io.owner }
                });
            }
        }
        Ok(())
    }
}
impl AccountHandle {
    // Design: ADR-0166 (docs/adr/ADR-0166-hierarchical-funding-and-stable-allocation-origins.md)
    pub fn retire(
        &self,
        evidence: &TeardownEvidence<'_>,
    ) -> Result<TransferReceipt, TeardownError> {
        if self.0.parent.is_none() {
            return Err(TeardownError::CannotRetireRoot);
        }
        if self.0.control.load(crate::sync::Ordering::Acquire) != 0 {
            return Err(TeardownError::ProtectedControlOwner);
        }
        self.close_to_growth();
        evidence.validate()?;
        let upper = self.0.shared.domains.lock().unwrap().upper;
        // Preflight before any handoff. Closing has sealed all issuing lanes;
        // remote free can continue and cannot create a new producer.
        for index in 0..upper {
            let record = self.0.shared.domains.lock().unwrap().records[index].clone();
            let Some(record) = record else { continue };
            let s = record.state.lock().unwrap();
            if s.account.is_descendant_of(self) {
                if s.active {
                    return Err(TeardownError::ActiveScope);
                }
                if s.external != 0 {
                    return Err(TeardownError::ExternalResponsibility);
                }
            }
        }
        let mut receipt = TransferReceipt {
            transferred_payload: 0,
            transferred_metadata: 0,
            returned_idle: 0,
            retired_records: 0,
        };
        for index in 0..upper {
            let record = self.0.shared.domains.lock().unwrap().records[index].clone();
            let Some(record) = record else { continue };
            let domain = FundingDomain(record);
            if domain.affiliation().is_descendant_of(self) {
                let (payload, metadata, idle) = domain.transfer_residual(self);
                receipt.transferred_payload += payload;
                receipt.transferred_metadata += metadata;
                receipt.returned_idle += idle;
                receipt.retired_records += 1;
            }
        }
        // Revoke the entire retired subtree's undistributed slack, bottom up.
        let account_upper = self.0.shared.accounts.lock().unwrap().upper;
        for index in 0..account_upper {
            let account = self.0.shared.accounts.lock().unwrap().records[index]
                .as_ref()
                .and_then(|a| a.upgrade())
                .map(AccountHandle);
            let Some(account) = account else { continue };
            if account.is_descendant_of(self) {
                receipt.returned_idle += account.return_slack(u64::MAX);
                let path = Path::new(&account);
                let _gates = path.exclusive_gates();
                account.0.ledger.lock().unwrap().retired = true;
                // A retired but externally held handle is closed, not an
                // active registry slot. It cannot reenter work admission.
                let slot = account.0.slot.swap(u64::MAX, crate::sync::Ordering::AcqRel);
                if slot != u64::MAX {
                    let mut registry = account.0.shared.accounts.lock().unwrap();
                    registry.records[slot as usize] = None;
                    account
                        .0
                        .shared
                        .membership_revision
                        .fetch_add(1, crate::sync::Ordering::Release);
                    registry.free_slots.push(slot as usize);
                }
            }
        }
        Ok(receipt)
    }
    pub fn is_retired(&self) -> bool {
        self.0.ledger.lock().unwrap().retired
    }
    pub(crate) fn is_descendant_of(&self, ancestor: &Self) -> bool {
        let mut next = Some(self.clone());
        while let Some(node) = next {
            if node.id() == ancestor.id() {
                return true;
            }
            next = node.0.parent.clone();
        }
        false
    }
}
impl FundingDomain {
    fn transfer_residual(&self, retiring: &AccountHandle) -> (u64, u64, u64) {
        loop {
            let source = self.affiliation();
            let path = Path::new(&source);
            let _gates = path.exclusive_gates();
            let mut s = self.0.state.lock().unwrap();
            if s.account.id() != source.id() {
                continue;
            }
            // A concurrent ancestor retirement may already have transferred it.
            if !source.is_descendant_of(retiring) {
                return (0, 0, 0);
            }
            let mut states = path.locks();
            let live = self.0.owner.live();
            crate::settlement::adjust_commitment(&path, &mut states, s.committed, live);
            let idle = s.authorized.saturating_sub(live);
            s.authorized = s.authorized.min(live);
            s.committed = live;
            s.settled_live = live;
            let receiver = (1..path.len)
                .find(|&i| {
                    !path.node(i).is_descendant_of(retiring) && !states[i].as_ref().unwrap().closed
                })
                .unwrap_or(path.len - 1);
            let obligation = live + self.0.metadata;
            // All source/target gates are held. Ancestor close and reception
            // share these gates, so no receiver can retire between acceptance
            // and releasing source liability. Common ancestors stay unchanged.
            for (i, state) in states.iter_mut().enumerate().take(receiver) {
                let state = state.as_mut().unwrap();
                state.committed -= obligation;
                state.revision += 1;
                node_publish(path.node(i), state.committed);
            }
            s.account = path.node(receiver).clone();
            let was_residual = s.residual;
            s.residual = true;
            s.sealed = true;
            if !was_residual {
                source.0.shared.domains.lock().unwrap().active -= 1;
            }
            return (live, self.0.metadata, idle);
        }
    }
}

impl FundingDomain {
    /// Retire a leaf lane after its own producers and external obligations
    /// have exited. Its execution account continues; the residual stays in
    /// that exact branch until account teardown or the real final release.
    pub fn retire_lane(&self) -> Result<(), TeardownError> {
        self.seal();
        self.settle();
        loop {
            let account = self.affiliation();
            let path = Path::new(&account);
            let _gates = path.exclusive_gates();
            let mut s = self.0.state.lock().unwrap();
            if s.account.id() != account.id() {
                continue;
            }
            if s.active {
                return Err(TeardownError::ActiveScope);
            }
            if s.external != 0 {
                return Err(TeardownError::ExternalResponsibility);
            }
            let mut states = path.locks();
            let live = self.0.owner.live();
            let retained_backing = s.authorized.min(live);
            let idle = s.authorized - retained_backing;
            if account.0.control.load(crate::sync::Ordering::Acquire) != 0 {
                states[0].as_mut().unwrap().slack += idle;
                crate::settlement::adjust_commitment(
                    &path,
                    &mut states,
                    s.committed,
                    live.checked_add(idle).expect("valid protected exposure"),
                );
            } else {
                crate::settlement::adjust_commitment(&path, &mut states, s.committed, live);
            }
            s.committed = live;
            s.authorized = retained_backing;
            s.settled_live = live;
            if !s.residual {
                account.0.shared.domains.lock().unwrap().active -= 1;
            }
            s.residual = true;
            return Ok(());
        }
    }
}
