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

//! Composable, bounded models of the production hierarchical ledger.
//!
//! Every model executes actual account/domain/growth-gate transitions. The
//! first two use three participants and two independently redeemable domains:
//! transferring parent slack must preserve root C, while shrink prohibits new
//! elastic rights and preserves the separately backed control pool. The debt
//! models combine an uncovered owner with a second funded domain; that domain's
//! F never pays the first owner's E. Their aggregate conservation is the same
//! invariant used by the ownership/cursor models in owner_loom.
//!
//! Search boundary: at most three spawned participants, one operation per
//! participant (a funded scope includes publication, free and finish), and two
//! preemptions. max_branches is a failure limit, not a successful cutoff. There
//! is no permutation limit or wall-clock timeout. Exhaustion proves only this
//! operation/preemption boundary, not unbounded fairness or allocator address
//! validity. sync.rs substitutes production locks/atomics and models strong /
//! weak reference admission over stable std backing; Miri remains separate.

use crate::{
    AccountKind, AuthorityConfig, ExternalRef, FundingDomain, MemoryAuthority, OWNER_METADATA_BYTES,
};
use loom::{model::Builder, thread};

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

fn authority(records: u32) -> MemoryAuthority {
    let mut config = AuthorityConfig::new(16_384, 16_384, 0);
    config.max_accounts = 3;
    config.max_active_owners = records;
    config.metadata_budget_bytes =
        u64::from(records) * (OWNER_METADATA_BYTES + 2 * std::mem::size_of::<usize>() as u64);
    MemoryAuthority::new(config).unwrap()
}

fn funded_step(domain: &FundingDomain) {
    let mut scope = domain.activate(1, 0).unwrap();
    let origin = scope.record_allocation(1);
    // SAFETY: this successful modeled allocation is released exactly once.
    unsafe { origin.record_deallocation(1) };
    scope.finish();
}

#[test]
fn two_domains_refill_and_shrink_are_ordered_without_root_growth() {
    model(|| {
        let authority = authority(2);
        let account = authority
            .create_account(AccountKind::Work, ExternalRef::NONE)
            .unwrap();
        account.prefund(2 * OWNER_METADATA_BYTES + 6).unwrap();
        let first = account.create_domain(1).unwrap();
        let second = account.create_domain(1).unwrap();
        let commitment = authority.root().committed_bytes();
        let mut writer = authority.take_capacity_writer().unwrap();

        let a = first.clone();
        let first_refill = thread::spawn(move || a.refill(1).is_ok());
        let b = second.clone();
        let second_refill = thread::spawn(move || b.refill(1).is_ok());
        let shrink = thread::spawn(move || writer.set_capacity(commitment - 1).unwrap());
        let first_grew = first_refill.join().unwrap();
        let second_grew = second_refill.join().unwrap();
        shrink.join().unwrap();

        let issued = u64::from(first_grew) + u64::from(second_grew);
        assert_eq!(first.snapshot().authorized, 1 + u64::from(first_grew));
        assert_eq!(second.snapshot().authorized, 1 + u64::from(second_grew));
        assert_eq!(account.local_free_bytes(), 4 - issued);
        assert_eq!(authority.root().committed_bytes(), commitment);
        assert!(first.refill(1).is_err());
        assert!(second.refill(1).is_err());
        // Existing independent rights remain redeemable after the freeze.
        funded_step(&first);
        funded_step(&second);
        assert_eq!(authority.root().committed_bytes(), commitment);
    });
}

#[test]
fn zero_target_preserves_control_redemption_without_elastic_exemption() {
    model(|| {
        let authority = authority(2);
        let control = authority
            .install_control_branch(OWNER_METADATA_BYTES + 4)
            .unwrap();
        let control_domain = control.create_domain(1).unwrap();
        let account = authority
            .create_account(AccountKind::Work, ExternalRef::NONE)
            .unwrap();
        account.prefund(OWNER_METADATA_BYTES + 3).unwrap();
        let work_domain = account.create_domain(1).unwrap();
        let commitment = authority.root().committed_bytes();
        let floor = authority.root().snapshot().floor_bytes;
        let mut writer = authority.take_capacity_writer().unwrap();

        let work = work_domain.clone();
        let work_refill = thread::spawn(move || work.refill(1).is_ok());
        let control = control_domain.clone();
        let control_refill = thread::spawn(move || control.refill(1).unwrap());
        let shrink = thread::spawn(move || writer.set_capacity(0).unwrap());
        let work_grew = work_refill.join().unwrap();
        control_refill.join().unwrap();
        shrink.join().unwrap();

        assert_eq!(control_domain.snapshot().authorized, 2);
        assert_eq!(work_domain.snapshot().authorized, 1 + u64::from(work_grew));
        assert_eq!(authority.root().committed_bytes(), commitment);
        assert_eq!(authority.root().snapshot().floor_bytes, floor);
        assert!(work_domain.refill(1).is_err());
        funded_step(&control_domain);
        assert_eq!(authority.root().committed_bytes(), commitment);
    });
}

#[test]
fn concurrent_debt_cover_and_free_preserve_two_domain_exposure() {
    model(|| {
        let authority = authority(2);
        let account = authority
            .create_account(AccountKind::Work, ExternalRef::NONE)
            .unwrap();
        let debt_domain = account.create_domain(0).unwrap();
        let other = account.create_domain(2).unwrap();
        account.prefund(2).unwrap();
        let mut scope = debt_domain.activate(0, 0).unwrap();
        let origin = scope.record_allocation(2);
        scope.finish();
        let before = authority.root().committed_bytes();
        authority
            .take_capacity_writer()
            .unwrap()
            .set_capacity(0)
            .unwrap();

        let cover_domain = debt_domain.clone();
        let cover = thread::spawn(move || cover_domain.cover_debt());
        let free = thread::spawn(move || {
            // SAFETY: origin describes the one still-outstanding allocation.
            unsafe { origin.record_deallocation(2) };
        });
        let other_domain = other.clone();
        let funded = thread::spawn(move || funded_step(&other_domain));
        let covered = cover.join().unwrap();
        free.join().unwrap();
        funded.join().unwrap();
        debt_domain.settle();

        assert!(covered == 0 || covered == 2);
        assert_eq!(debt_domain.snapshot().authorized, covered);
        assert_eq!(debt_domain.snapshot().committed, covered);
        assert_eq!(account.local_free_bytes(), 2 - covered);
        assert_eq!(other.snapshot().authorized, 2);
        assert_eq!(other.snapshot().committed, 2);
        assert_eq!(authority.root().committed_bytes(), before - 2);
    });
}

#[test]
fn published_free_before_cover_cannot_mint_spare_under_freeze() {
    model(|| {
        let authority = authority(2);
        let account = authority
            .create_account(AccountKind::Work, ExternalRef::NONE)
            .unwrap();
        let debt_domain = account.create_domain(0).unwrap();
        let other = account.create_domain(2).unwrap();
        account.prefund(2).unwrap();
        let mut scope = debt_domain.activate(0, 0).unwrap();
        let origin = scope.record_allocation(2);
        scope.finish();
        let before = authority.root().committed_bytes();
        authority
            .take_capacity_writer()
            .unwrap()
            .set_capacity(0)
            .unwrap();
        let (released, await_release) = loom::sync::mpsc::channel();

        let free = thread::spawn(move || {
            // SAFETY: origin is consumed once before publishing completion.
            unsafe { origin.record_deallocation(2) };
            released.send(()).unwrap();
        });
        let cover_domain = debt_domain.clone();
        let cover = thread::spawn(move || {
            await_release.recv().unwrap();
            cover_domain.cover_debt()
        });
        let other_domain = other.clone();
        let funded = thread::spawn(move || funded_step(&other_domain));
        free.join().unwrap();
        assert_eq!(cover.join().unwrap(), 0);
        funded.join().unwrap();

        let snapshot = debt_domain.snapshot();
        assert_eq!(snapshot.authorized, 0);
        assert_eq!(snapshot.committed, 0);
        assert_eq!(snapshot.free, 0);
        assert_eq!(account.local_free_bytes(), 2);
        assert_eq!(other.snapshot().authorized, 2);
        assert_eq!(authority.root().committed_bytes(), before - 2);
    });
}
