// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Actual owner, activation, retirement and bounded maintenance models.
//!
//! These models compose with ledger_loom's independent-domain conservation:
//! sealing uses the production local activation handshake, handoff moves the
//! exact payload/metadata obligation without creating capacity, and real last
//! free permits record reclamation only after publication/access responsibility
//! is gone. Raw origins are used under their actual one-publication/one-free
//! unsafe contract. No copied origin is accessed after its final decrement.
//!
//! Search boundary: two or three spawned participants, one scope or lifecycle
//! operation per participant, a one-member maintenance batch during races,
//! and two preemptions. max_branches is an error limit, not truncation accepted
//! as a pass; neither wall-clock nor permutation limits are set. The final
//! complete quiescent sweep is deterministic and verifies true convergence:
//! first finish any race-time epoch, then cover one entire successor epoch.
//! A late free after a cursor passed is not included in the old epoch's
//! remaining account slots; completion of that remainder is not a fresh sweep.
//! The production sync seam models locks, atomic facts and strong/weak
//! admission. Stable std backing does not model allocator address reuse; this
//! is a logical last-access proof boundary and does not replace Miri/ASan.

use crate::{
    AccountKind, AuthorityConfig, ExternalRef, FundingDomain, MemoryAuthority,
    OWNER_METADATA_BYTES, TeardownEvidence,
};
use loom::{model::Builder, sync::Arc, thread};

fn model(f: impl Fn() + Send + Sync + 'static) {
    let mut builder = Builder::new();
    builder.max_threads = 4;
    builder.preemption_bound = Some(2);
    builder.max_branches = 20_000;
    builder.max_permutations = None;
    builder.max_duration = None;
    builder.checkpoint_file = None;
    builder.check(f);
}

fn authority(accounts: u32) -> MemoryAuthority {
    let mut config = AuthorityConfig::new(16_384, 16_384, 0);
    config.max_accounts = accounts;
    config.max_active_owners = 1;
    config.metadata_budget_bytes = OWNER_METADATA_BYTES + 2 * std::mem::size_of::<usize>() as u64;
    MemoryAuthority::new(config).unwrap()
}

fn exited() -> TeardownEvidence<'static> {
    TeardownEvidence {
        tasks_exited: true,
        operators_destroyed: true,
        io: &[],
        now_ns: 1,
    }
}

fn retain_one(domain: &FundingDomain) -> crate::FactToken {
    let mut scope = domain.activate(1, 0).unwrap();
    let origin = scope.record_allocation(1);
    scope.finish();
    origin
}

fn assert_empty_registry(authority: &MemoryAuthority) {
    let registry = authority.shared.domains.lock().unwrap();
    assert_eq!(registry.active, 0);
    assert_eq!(registry.metadata, 0);
    assert!(registry.records.iter().all(Option::is_none));
}

fn complete_quiescent_successor(authority: &MemoryAuthority) {
    // All race participants have joined. At most four members exist, so a
    // batch of eight finishes an outstanding epoch without retries. Start one
    // successor epoch to cover late frees or dropped pins behind its cursor.
    let previous = authority.maintain(8);
    assert!(previous.complete);
    let successor = authority.maintain(8);
    assert!(successor.complete);
    assert!(successor.epoch > previous.epoch);
}

#[test]
fn detach_reactivation_and_collector_keep_drain_responsibility() {
    model(|| {
        let authority = Arc::new(authority(2));
        let account = authority
            .create_account(AccountKind::Work, ExternalRef::NONE)
            .unwrap();
        let baseline = authority.root().committed_bytes();
        let domain = account.create_domain(1).unwrap();
        let (entered, await_entry) = loom::sync::mpsc::channel();
        let (finish, await_finish) = loom::sync::mpsc::channel();
        let (reactivate, await_reactivation) = loom::sync::mpsc::channel();

        let original = domain.clone();
        let original_scope = thread::spawn(move || {
            let scope = original.activate(1, 0).unwrap();
            entered.send(()).unwrap();
            await_finish.recv().unwrap();
            scope.finish();
        });
        let collector = authority.clone();
        let maintenance = thread::spawn(move || {
            await_entry.recv().unwrap();
            // The original scope stays active until this real cursor visit has
            // requested a drain. Both subsequent actors race the detach gap.
            let coverage = collector.maintain(1);
            assert_eq!(coverage.deferred_active, 1);
            finish.send(()).unwrap();
            reactivate.send(()).unwrap();
        });
        let next = domain.clone();
        let next_scope = thread::spawn(move || {
            await_reactivation.recv().unwrap();
            match next.activate(0, 0) {
                Ok(scope) => {
                    scope.finish();
                }
                Err(crate::CapacityError::Invalid { .. }) => {
                    // Active or pending-drain refusal is legitimate. No retry
                    // hides a lost flag or makes this actor acquire new rights.
                }
                Err(error) => panic!("unexpected reactivation refusal: {error}"),
            }
        });
        original_scope.join().unwrap();
        maintenance.join().unwrap();
        next_scope.join().unwrap();

        // Assert before a successor sweep: detach itself must retain and
        // complete this responsibility, rather than relying on a later scan.
        let state = domain.0.state.lock().unwrap();
        assert!(!state.active);
        assert!(!state.drain_requested);
        assert_eq!(state.authorized, 0);
        assert_eq!(state.committed, 0);
        drop(state);
        assert_eq!(
            authority.root().committed_bytes(),
            baseline + OWNER_METADATA_BYTES
        );
        complete_quiescent_successor(&authority);
        assert_eq!(domain.snapshot().committed, 0);
    });
}

#[test]
fn seal_and_activation_share_the_production_local_handshake() {
    model(|| {
        let authority = authority(2);
        let account = authority
            .create_account(AccountKind::Work, ExternalRef::NONE)
            .unwrap();
        let domain = account.create_domain(1).unwrap();
        let before = authority.root().committed_bytes();

        let activating = domain.clone();
        let activation = thread::spawn(move || match activating.activate(1, 0) {
            Ok(mut scope) => {
                let origin = scope.record_allocation(1);
                // SAFETY: the scope's successful allocation has one free.
                unsafe { origin.record_deallocation(1) };
                scope.finish();
                true
            }
            Err(crate::CapacityError::Closed { .. }) => false,
            Err(error) => panic!("unexpected activation refusal: {error}"),
        });
        let sealing = domain.clone();
        let seal = thread::spawn(move || sealing.seal());
        activation.join().unwrap();
        seal.join().unwrap();

        let snapshot = domain.snapshot();
        assert!(snapshot.sealed);
        assert!(!snapshot.active);
        assert_eq!(snapshot.live, 0);
        assert_eq!(domain.0.lane.record().lifetime().outstanding(), 0);
        assert!(domain.activate(0, 0).is_err());
        assert!(domain.settle().next_step.is_err());
        assert_eq!(authority.root().committed_bytes(), before);
    });
}

#[test]
fn remote_free_query_retirement_and_cursor_keep_exact_liability() {
    model(|| {
        let authority = Arc::new(authority(2));
        let storage = authority.root().committed_bytes();
        let account = authority
            .create_account(AccountKind::Work, ExternalRef::NONE)
            .unwrap();
        let domain = account.create_domain(1).unwrap();
        let origin = retain_one(&domain);

        let retiring = account.clone();
        let retirement = thread::spawn(move || retiring.retire(&exited()).unwrap());
        let free = thread::spawn(move || {
            // SAFETY: the original allocation stays outstanding until here.
            unsafe { origin.record_deallocation(1) };
        });
        let collector = authority.clone();
        let maintenance = thread::spawn(move || collector.maintain(1));
        retirement.join().unwrap();
        free.join().unwrap();
        maintenance.join().unwrap();

        assert!(account.is_retired());
        assert_eq!(account.committed_bytes(), 0);
        assert!(domain.snapshot().residual);
        assert_eq!(domain.affiliation().id(), authority.root().id());
        // A handle still pins metadata, but the real free must remove payload.
        complete_quiescent_successor(&authority);
        assert_eq!(domain.snapshot().committed, 0);
        assert_eq!(
            authority.root().committed_bytes(),
            storage + crate::ACCOUNT_METADATA_BYTES + OWNER_METADATA_BYTES
        );
        drop(domain);
        complete_quiescent_successor(&authority);
        assert_empty_registry(&authority);
        assert_eq!(
            authority.root().committed_bytes(),
            storage + crate::ACCOUNT_METADATA_BYTES
        );
        // Slot retirement does not free an externally held Account Arc box.
        drop(account);
        assert_eq!(authority.root().committed_bytes(), storage);
    });
}

#[test]
fn last_free_and_metadata_reclamation_do_not_require_the_origin_thread() {
    model(|| {
        let authority = Arc::new(authority(2));
        let storage = authority.root().committed_bytes();
        let account = authority
            .create_account(AccountKind::Work, ExternalRef::NONE)
            .unwrap();
        let domain = account.create_domain(1).unwrap();
        let origin = retain_one(&domain);
        domain.retire_lane().unwrap();
        drop(domain);
        // Only registry publication and the outstanding allocation now keep
        // the origin address alive. Reclamation races its real final access.
        let free = thread::spawn(move || {
            // SAFETY: exactly one free, with no subsequent origin access.
            unsafe { origin.record_deallocation(1) };
        });
        let collector = authority.clone();
        let maintenance = thread::spawn(move || collector.maintain(1));
        free.join().unwrap();
        maintenance.join().unwrap();

        complete_quiescent_successor(&authority);
        assert_empty_registry(&authority);
        assert_eq!(account.committed_bytes(), 0);
        assert_eq!(
            authority.root().committed_bytes(),
            storage + crate::ACCOUNT_METADATA_BYTES
        );
        drop(account);
        assert_eq!(authority.root().committed_bytes(), storage);
    });
}

#[test]
fn protected_last_free_returns_backing_but_never_residual_debt_to_floor() {
    model(|| {
        let authority = Arc::new(authority(2));
        let floor = OWNER_METADATA_BYTES + 2;
        authority.install_control_branch(floor).unwrap();
        let baseline = authority.root().committed_bytes();
        let control = authority.control_branch().unwrap();
        let domain = control.create_domain(1).unwrap();
        let mut scope = domain.activate(1, 0).unwrap();
        let origin = scope.record_allocation(2);
        assert_eq!(scope.finish().debt, 1);
        domain.retire_lane().unwrap();
        let residual = domain.snapshot();
        assert_eq!(
            (residual.authorized, residual.committed, residual.debt),
            (1, 2, 1)
        );
        assert_eq!(authority.root().committed_bytes(), baseline + 1);
        drop(domain);

        let free = thread::spawn(move || {
            // SAFETY: this allocation is released once, before final access.
            unsafe { origin.record_deallocation(2) };
        });
        let collector = authority.clone();
        let maintenance = thread::spawn(move || collector.maintain(1));
        free.join().unwrap();
        maintenance.join().unwrap();

        complete_quiescent_successor(&authority);
        assert_empty_registry(&authority);
        assert_eq!(control.local_free_bytes(), floor);
        assert_eq!(control.committed_bytes(), floor);
        assert_eq!(authority.root().committed_bytes(), baseline);
    });
}

#[test]
fn ancestor_retirement_and_reclamation_revalidate_affiliation() {
    model(|| {
        let authority = Arc::new(authority(3));
        let storage = authority.root().committed_bytes();
        let parent = authority
            .create_account(AccountKind::ResourceGroup, ExternalRef::NONE)
            .unwrap();
        let account = parent
            .create_child(AccountKind::Work, ExternalRef::NONE)
            .unwrap();
        let domain = account.create_domain(1).unwrap();
        let origin = retain_one(&domain);
        account.retire(&exited()).unwrap();
        assert_eq!(domain.affiliation().id(), parent.id());
        // SAFETY: this is the sole allocation, released before the race so
        // the collector can genuinely remove this record's metadata.
        unsafe { origin.record_deallocation(1) };
        drop(domain);

        let retiring = parent.clone();
        let retirement = thread::spawn(move || retiring.retire(&exited()).unwrap());
        let collector = authority.clone();
        let maintenance = thread::spawn(move || collector.maintain(1));
        retirement.join().unwrap();
        maintenance.join().unwrap();

        complete_quiescent_successor(&authority);
        assert!(parent.is_retired());
        assert_eq!(parent.committed_bytes(), 0);
        assert_eq!(account.committed_bytes(), 0);
        assert_empty_registry(&authority);
        assert_eq!(
            authority.root().committed_bytes(),
            storage + 2 * crate::ACCOUNT_METADATA_BYTES
        );
        drop(account);
        drop(parent);
        assert_eq!(authority.root().committed_bytes(), storage);
    });
}
