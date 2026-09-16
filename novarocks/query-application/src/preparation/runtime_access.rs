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

//! What a completed plan's scans were frozen with, kept beside the plan rather
//! than inside it.
//!
//! Freezing a read yields two things of different kinds: static facts, which
//! belong in the plan and travel to every worker, and the capability to
//! actually perform that read, which belongs to this attempt alone and travels
//! nowhere. Putting the second inside the first is how a credential ends up
//! encoded on a wire, so they are separate here - and separate in a way that
//! keeps them accounted for together.
//!
//! A plan with a scan whose capability is missing cannot execute, and a
//! capability with no scan to serve is one nobody will release. Neither is a
//! state worth carrying, so the two are paired only when each scan has exactly
//! one capability and every capability has a scan.
//!
//! Capabilities reach this module the moment they are taken and leave it only
//! by being handed to an owner - paired with a plan on the way to execution, or
//! returned unpaired on every path where no plan appears. No path drops one.

use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{Arc, Mutex, PoisonError},
};

use novarocks_physical_plan::{NodeKind, PhysicalPlan, ProviderReadOccurrenceId};
use novarocks_sql::binding::SqlTableBindingId;

use super::final_plan::CompletedPhysicalPlanCandidate;

/// The capability one scan occurrence was frozen with.
#[derive(Clone, Debug)]
pub struct FrozenReadAccess<A> {
    /// The binding this read was admitted under. The plan does not carry it -
    /// it is SQL-local - but the owner releasing the capability needs it.
    pub binding: SqlTableBindingId,
    pub access: A,
}

/// Where a capability goes the moment it is taken.
///
/// A freeze yields its capability before it yields the facts that describe it,
/// and everything after that point can still fail: the provider can refuse the
/// next read, the compiler can reject the answer, the statement can be
/// cancelled. Depositing on the spot means no capability is ever in flight when
/// that happens, so a half-finished freeze leaves nothing behind that its owner
/// cannot find and release.
///
/// It accepts deposits through a shared reference because the depositor is the
/// adapter doing the freezing and the owner is the driver that outlives it;
/// the adapter never reads back what it put in.
pub struct ReadAccessSink<A> {
    taken: Taken<A>,
}

type Taken<A> = Arc<Mutex<Vec<(ProviderReadOccurrenceId, FrozenReadAccess<A>)>>>;

impl<A> Default for ReadAccessSink<A> {
    fn default() -> Self {
        Self {
            taken: Arc::new(Mutex::new(Vec::new())),
        }
    }
}

impl<A> ReadAccessSink<A> {
    pub fn new() -> Self {
        Self::default()
    }

    /// Account for one capability at the instant it is taken.
    ///
    /// A duplicate occurrence is not refused here. Refusing would mean handing
    /// the capability back to a caller that is mid-freeze and has nowhere to
    /// put it; the occurrence is checked once, later, with every capability
    /// already in hand.
    pub fn deposit(&self, occurrence: ProviderReadOccurrenceId, access: FrozenReadAccess<A>) {
        self.entries().push((occurrence, access));
    }

    /// A deposit slip for wherever the freezing actually happens.
    ///
    /// Freezing may run somewhere the sink's borrow cannot reach - a blocking
    /// lane, another thread - and "deposited the moment it is taken" has to
    /// hold there too, or that thread becomes a place capabilities can be lost.
    /// The slip reaches the same account and can do nothing else with it.
    pub fn deposits(&self) -> ReadAccessDeposit<A> {
        ReadAccessDeposit {
            taken: Arc::clone(&self.taken),
        }
    }

    /// Everything taken so far, for an owner that will release rather than use
    /// it.
    pub fn into_taken(self) -> Vec<FrozenReadAccess<A>> {
        self.into_entries()
            .into_iter()
            .map(|(_, access)| access)
            .collect()
    }

    /// Turn the deposits into a sidecar, or hand every capability back.
    ///
    /// One occurrence frozen twice means two freezes happened where the plan
    /// expects one. The sidecar has room for one of them, so rather than keep
    /// one and drop the other, neither is kept.
    pub fn try_into_access(
        self,
    ) -> Result<FinalPlanRuntimeAccess<A>, (FinalPlanAccessError, Vec<FrozenReadAccess<A>>)> {
        let taken = self.into_entries();
        let mut seen = BTreeSet::new();
        for (occurrence, _) in &taken {
            if !seen.insert(*occurrence) {
                let error = FinalPlanAccessError::FrozenTwice {
                    occurrence: *occurrence,
                };
                return Err((error, taken.into_iter().map(|(_, a)| a).collect()));
            }
        }
        Ok(FinalPlanRuntimeAccess {
            by_occurrence: taken.into_iter().collect(),
        })
    }

    fn entries(
        &self,
    ) -> std::sync::MutexGuard<'_, Vec<(ProviderReadOccurrenceId, FrozenReadAccess<A>)>> {
        lock(&self.taken)
    }

    fn into_entries(self) -> Vec<(ProviderReadOccurrenceId, FrozenReadAccess<A>)> {
        std::mem::take(&mut *self.entries())
    }
}

/// A deposit-only view of one sink, for the thread where a read is frozen.
#[derive(Clone)]
pub struct ReadAccessDeposit<A> {
    taken: Taken<A>,
}

impl<A> ReadAccessDeposit<A> {
    pub fn deposit(&self, occurrence: ProviderReadOccurrenceId, access: FrozenReadAccess<A>) {
        lock(&self.taken).push((occurrence, access));
    }
}

/// A panic while holding this lock leaves a consistent vector, and the
/// capabilities in it still have to reach their owner. Refusing to look at them
/// would turn one panic into a leak.
fn lock<A>(
    taken: &Taken<A>,
) -> std::sync::MutexGuard<'_, Vec<(ProviderReadOccurrenceId, FrozenReadAccess<A>)>> {
    taken.lock().unwrap_or_else(PoisonError::into_inner)
}

/// Capabilities for every scan of one completed plan.
#[derive(Debug)]
pub struct FinalPlanRuntimeAccess<A> {
    by_occurrence: BTreeMap<ProviderReadOccurrenceId, FrozenReadAccess<A>>,
}

impl<A> Default for FinalPlanRuntimeAccess<A> {
    fn default() -> Self {
        Self {
            by_occurrence: BTreeMap::new(),
        }
    }
}

impl<A> FinalPlanRuntimeAccess<A> {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn get(&self, occurrence: ProviderReadOccurrenceId) -> Option<&FrozenReadAccess<A>> {
        self.by_occurrence.get(&occurrence)
    }

    pub fn len(&self) -> usize {
        self.by_occurrence.len()
    }

    pub fn is_empty(&self) -> bool {
        self.by_occurrence.is_empty()
    }

    /// Give up every capability, for an owner that will release them.
    pub fn into_taken(self) -> Vec<FrozenReadAccess<A>> {
        self.by_occurrence.into_values().collect()
    }

    /// Give up every capability, still keyed by the scan it was frozen for.
    ///
    /// A capability cannot be copied, so an owner that needs to place each one
    /// somewhere - a per-attempt access plan, say - has to take them rather
    /// than read them. The occurrence comes along because that is how the plan
    /// addresses the scan each one belongs to.
    pub fn into_occurrences(self) -> BTreeMap<ProviderReadOccurrenceId, FrozenReadAccess<A>> {
        self.by_occurrence
    }
}

/// A completed plan and the capabilities its scans were frozen with.
///
/// The two exist together or not at all: this is the only way to hold either,
/// and it cannot be built unless they account for each other exactly.
#[derive(Debug)]
pub struct CompletedPlanWithAccess<A> {
    candidate: CompletedPhysicalPlanCandidate,
    access: FinalPlanRuntimeAccess<A>,
}

impl<A> CompletedPlanWithAccess<A> {
    /// Pair them, or hand the capabilities back unpaired.
    ///
    /// A refusal returns the capabilities because the plan they were taken for
    /// will not execute and nothing else will ever ask for them.
    pub fn try_pair(
        candidate: CompletedPhysicalPlanCandidate,
        access: FinalPlanRuntimeAccess<A>,
    ) -> Result<Self, (FinalPlanAccessError, FinalPlanRuntimeAccess<A>)> {
        let scans = provider_read_occurrences(candidate.plan());
        for occurrence in &scans {
            if !access.by_occurrence.contains_key(occurrence) {
                let error = FinalPlanAccessError::ScanWithoutAccess {
                    occurrence: *occurrence,
                };
                return Err((error, access));
            }
        }
        for occurrence in access.by_occurrence.keys() {
            if !scans.contains(occurrence) {
                let error = FinalPlanAccessError::AccessWithoutScan {
                    occurrence: *occurrence,
                };
                return Err((error, access));
            }
        }
        Ok(Self { candidate, access })
    }

    pub const fn candidate(&self) -> &CompletedPhysicalPlanCandidate {
        &self.candidate
    }

    pub const fn access(&self) -> &FinalPlanRuntimeAccess<A> {
        &self.access
    }

    /// Give up the capabilities so their owner can release them.
    pub fn into_access(self) -> FinalPlanRuntimeAccess<A> {
        self.access
    }

    /// Split the pair, for an owner that consumes both halves.
    ///
    /// They separate only here, after having been proved to account for each
    /// other; nothing can obtain one half without the other having existed.
    pub fn into_parts(self) -> (CompletedPhysicalPlanCandidate, FinalPlanRuntimeAccess<A>) {
        (self.candidate, self.access)
    }
}

/// Every provider-read occurrence the plan scans.
fn provider_read_occurrences(plan: &PhysicalPlan) -> Vec<ProviderReadOccurrenceId> {
    let mut occurrences = plan
        .fragments()
        .values()
        .flat_map(|fragment| fragment.nodes().values())
        .filter_map(|node| match &node.kind {
            NodeKind::Scan { occurrence, .. } => Some(*occurrence),
            _ => None,
        })
        .collect::<Vec<_>>();
    occurrences.sort_unstable();
    occurrences.dedup();
    occurrences
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FinalPlanAccessError {
    /// The plan scans a read nothing was frozen for, so it cannot execute.
    ScanWithoutAccess {
        occurrence: ProviderReadOccurrenceId,
    },
    /// A capability was frozen for a read the plan does not scan, so nobody
    /// would ever use or release it.
    AccessWithoutScan {
        occurrence: ProviderReadOccurrenceId,
    },
    /// One occurrence was frozen twice, where the plan expects one freeze.
    FrozenTwice {
        occurrence: ProviderReadOccurrenceId,
    },
}

impl std::fmt::Display for FinalPlanAccessError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ScanWithoutAccess { occurrence } => write!(
                formatter,
                "plan scans provider read occurrence {} with no frozen access",
                occurrence.get()
            ),
            Self::AccessWithoutScan { occurrence } => write!(
                formatter,
                "frozen access for provider read occurrence {} that the plan does not scan",
                occurrence.get()
            ),
            Self::FrozenTwice { occurrence } => write!(
                formatter,
                "provider read occurrence {} was frozen more than once",
                occurrence.get()
            ),
        }
    }
}

impl std::error::Error for FinalPlanAccessError {}

#[cfg(test)]
mod tests {
    use novarocks_sql::binding::SqlTableBindingAllocator;

    use crate::completed_plan_fixture::completed_values_plan;

    use super::*;

    fn a_binding() -> SqlTableBindingId {
        SqlTableBindingAllocator::try_new_for_test(std::num::NonZeroU64::new(1).unwrap())
            .expect("allocator")
            .allocate()
            .expect("binding")
    }

    fn an_access() -> FrozenReadAccess<()> {
        FrozenReadAccess {
            binding: a_binding(),
            access: (),
        }
    }

    /// A plan over literal rows scans no provider, so it needs no capability.
    async fn values_candidate() -> CompletedPhysicalPlanCandidate {
        completed_values_plan([9; 16]).await.candidate().clone()
    }

    #[tokio::test]
    async fn a_capability_nothing_scans_is_refused() {
        let candidate = values_candidate().await;
        let sink = ReadAccessSink::new();
        sink.deposit(ProviderReadOccurrenceId::new(7), an_access());
        let access = sink
            .try_into_access()
            .expect("one deposit is not a duplicate");
        // Nobody would ever use or release it, so the pair is refused - and the
        // capability comes back rather than disappearing with the refusal.
        let (error, returned) = CompletedPlanWithAccess::try_pair(candidate, access)
            .expect_err("a capability nothing scans cannot be paired");
        assert!(matches!(
            error,
            FinalPlanAccessError::AccessWithoutScan { .. }
        ));
        assert_eq!(returned.into_taken().len(), 1);
    }

    #[tokio::test]
    async fn a_plan_that_scans_nothing_pairs_with_no_capability() {
        let candidate = values_candidate().await;
        let paired =
            CompletedPlanWithAccess::<()>::try_pair(candidate, FinalPlanRuntimeAccess::new())
                .unwrap_or_else(|(error, _)| panic!("literal rows need no capability: {error}"));
        assert!(paired.access().is_empty());
    }

    /// Two freezes where the plan expects one: the sidecar has room for a
    /// single capability per occurrence, so neither is kept and both are
    /// returned for release.
    #[test]
    fn one_occurrence_frozen_twice_returns_both_capabilities() {
        let sink = ReadAccessSink::new();
        sink.deposit(ProviderReadOccurrenceId::new(1), an_access());
        sink.deposit(ProviderReadOccurrenceId::new(1), an_access());
        let (error, returned) = sink
            .try_into_access()
            .expect_err("one occurrence cannot be frozen twice");
        assert!(matches!(error, FinalPlanAccessError::FrozenTwice { .. }));
        assert_eq!(returned.len(), 2);
    }

    /// A capability deposited from another thread reaches the same account, so
    /// a freeze that runs on the blocking lane cannot lose one.
    #[test]
    fn a_deposit_slip_reaches_the_same_account() {
        let sink = ReadAccessSink::new();
        let slip = sink.deposits();
        std::thread::spawn(move || slip.deposit(ProviderReadOccurrenceId::new(5), an_access()))
            .join()
            .expect("deposit thread");
        assert_eq!(sink.into_taken().len(), 1);
    }

    /// Distinct occurrences are the ordinary case, and the sidecar is keyed by
    /// them however they arrived.
    #[test]
    fn each_occurrence_keeps_its_own_capability() {
        let sink = ReadAccessSink::new();
        sink.deposit(ProviderReadOccurrenceId::new(4), an_access());
        sink.deposit(ProviderReadOccurrenceId::new(2), an_access());
        let access = sink.try_into_access().expect("distinct occurrences");
        assert_eq!(access.len(), 2);
        assert!(access.get(ProviderReadOccurrenceId::new(2)).is_some());
        assert!(access.get(ProviderReadOccurrenceId::new(4)).is_some());
        assert!(access.get(ProviderReadOccurrenceId::new(3)).is_none());
    }
}
