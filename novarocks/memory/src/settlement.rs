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

//! Local fact acceptance and the separate decision to continue.
use crate::sync::Ordering;
use crate::{
    account::{MAX_DEPTH, Path, node_publish},
    domain::{DomainState, FundingDomain},
    error::CapacityError,
};
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StepReceipt {
    pub generation: u64,
    pub accepted_live: u64,
    pub debt: u64,
    pub committed: u64,
    pub next_step: Result<(), CapacityError>,
}
impl FundingDomain {
    /// Accepts facts even if the next step is denied. A funded conversion is
    /// local; debt and a real commitment reduction are hierarchical events.
    pub fn settle(&self) -> StepReceipt {
        {
            let mut s = self.0.state.lock().unwrap();
            let live = self.0.owner.live();
            let desired = s
                .authorized
                .max(live.checked_add(s.external).expect("valid live+external"));
            if desired == s.committed && (s.blocked.is_none() || s.sealed) {
                s.settled_live = live;
                return receipt(
                    &s,
                    live,
                    if s.sealed {
                        Err(CapacityError::Closed {
                            account: s.account.id(),
                        })
                    } else {
                        s.blocked.clone().map_or(Ok(()), Err)
                    },
                );
            }
        }
        loop {
            // Never acquire a qualification gate from under the domain lock.
            let account = self.affiliation();
            let path = Path::new(&account);
            let _gates = path.exclusive_gates();
            let mut s = self.0.state.lock().unwrap();
            if s.account.id() != account.id() {
                continue;
            }
            let mut states = path.locks();
            let live = self.0.owner.live();
            let desired = s
                .authorized
                .max(live.checked_add(s.external).expect("valid live+external"));
            adjust_commitment(&path, &mut states, s.committed, desired);
            s.committed = desired;
            s.settled_live = live;
            let next = if s.sealed {
                Err(CapacityError::Closed {
                    account: account.id(),
                })
            } else {
                path.nodes
                    .iter()
                    .flatten()
                    .find_map(|node| {
                        let index = path
                            .nodes
                            .iter()
                            .position(|n| n.as_ref().is_some_and(|n| n.id() == node.id()))
                            .unwrap();
                        let state = states[index].as_ref().unwrap();
                        let limit = if node.0.parent.is_none() {
                            node.0.shared.target.load(Ordering::Acquire)
                        } else {
                            node.0.limit.load(Ordering::Acquire)
                        };
                        (state.committed > limit).then(|| account.refusal(node, state, 0, true))
                    })
                    .map_or(Ok(()), Err)
            };
            s.blocked = next.clone().err();
            return receipt(&s, live, next);
        }
    }
    /// Reclassify existing uncovered facts using exclusive account slack.
    /// It creates no spare redemption right and remains legal while frozen.
    pub fn cover_debt(&self) -> u64 {
        loop {
            let account = self.affiliation();
            let path = Path::new(&account);
            let _gates = path.exclusive_gates();
            let mut s = self.0.state.lock().unwrap();
            if s.account.id() != account.id() {
                continue;
            }
            let mut states = path.locks();
            let live = self.0.owner.live();
            let desired = s
                .authorized
                .max(live.checked_add(s.external).expect("valid live+external"));
            adjust_commitment(&path, &mut states, s.committed, desired);
            s.committed = desired;
            s.settled_live = live;
            let debt = live.saturating_add(s.external).saturating_sub(s.authorized);
            let amount = debt.min(states[0].as_ref().unwrap().slack);
            s.authorized += amount;
            if amount != 0 {
                self.0.owner.sequence.fetch_add(1, Ordering::Release);
            }
            states[0].as_mut().unwrap().slack -= amount;
            // The domain's existing commitment is unchanged; revoking account
            // slack removes a distinct obligation, rather than minting F.
            adjust_commitment(&path, &mut states, amount, 0);
            return amount;
        }
    }
    /// Issue new local rights under all ancestor qualifications. The amount
    /// transferred from account slack has exactly one redeeming domain.
    pub fn refill(&self, bytes: u64) -> Result<(), CapacityError> {
        loop {
            let account = self.affiliation();
            let path = Path::new(&account);
            let _gates = path.shared_gates();
            let protected = account.0.control.load(Ordering::Acquire) != 0
                && account.local_free_bytes() >= bytes;
            crate::account::qualify(&account, &path, bytes, !protected)?;
            let mut s = self.0.state.lock().unwrap();
            if s.account.id() != account.id() {
                continue;
            }
            if s.sealed || s.residual {
                return Err(CapacityError::Closed {
                    account: account.id(),
                });
            }
            let debt = s.committed.saturating_sub(s.authorized);
            if debt != 0 {
                return Err(CapacityError::Invalid {
                    detail: "settle and cover existing debt before requesting spare rights",
                });
            }
            let mut states = path.requirement_locks(bytes);
            let growth = bytes.saturating_sub(states[0].as_ref().unwrap().slack);
            crate::account::grow_locked(&account, &path, &mut states, growth, true)
                .map_err(|e| e.with_requested(bytes))?;
            states[0].as_mut().unwrap().slack -= bytes;
            s.authorized = s
                .authorized
                .checked_add(bytes)
                .expect("validated authorized growth");
            s.committed += bytes;
            if bytes != 0 {
                self.0.owner.sequence.fetch_add(1, Ordering::Release);
            }
            return Ok(());
        }
    }
}
pub(crate) fn adjust_commitment(
    path: &Path,
    states: &mut [Option<crate::account::LedgerGuard<'_>>; MAX_DEPTH],
    old: u64,
    new: u64,
) {
    if old == new {
        return;
    }
    for (i, state) in states.iter_mut().enumerate().take(path.len) {
        let state = state.as_mut().unwrap();
        state.committed = state
            .committed
            .checked_sub(old)
            .and_then(|n| n.checked_add(new))
            .expect("commitment invariant");
        state.peak = state.peak.max(state.committed);
        state.revision += 1;
        node_publish(path.node(i), state.committed);
        path.node(i).0.interactions.fetch_add(1, Ordering::Relaxed);
    }
}
fn receipt(s: &DomainState, live: u64, next_step: Result<(), CapacityError>) -> StepReceipt {
    StepReceipt {
        generation: s.generation,
        accepted_live: live,
        debt: live.saturating_add(s.external).saturating_sub(s.authorized),
        committed: s.committed,
        next_step,
    }
}

impl FundingDomain {
    /// Prepare an inactive lane's next bounded workset. Existing local rights
    /// are reused without ancestor contact; shortage refills in fixed quanta.
    /// Normal detach retains up to the declared workset, rather than returning
    /// and reacquiring a quantum at every boundary oscillation.
    pub fn ensure_workset(&self, required_free: u64) -> Result<(), CapacityError> {
        let amount = {
            let s = self.0.state.lock().unwrap();
            if s.active {
                return Err(CapacityError::Invalid {
                    detail: "workset preparation requires an inactive lane",
                });
            }
            let free = s
                .authorized
                .saturating_sub(self.0.owner.live().saturating_add(s.external));
            if free >= required_free {
                return Ok(());
            }
            s.account
                .0
                .shared
                .top_up
                .amount_for(required_free - free, s.committed)
        };
        self.refill(amount)
    }
}
