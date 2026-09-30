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

mod common;
use common::*;
use novarocks_memory::*;
#[test]
fn accepting_the_same_debt_does_not_clear_a_prior_denial() {
    let a = authority(32_768);
    let q = work(&a);
    q.install_policy(2_048 + OWNER_METADATA_BYTES, LimitDimension::Work);
    let d = q.create_domain(2_048).unwrap();
    let mut l = d.activate(2_048, 0).unwrap();
    let p = l.record_allocation(3_072);
    let r = l.finish();
    assert!(r.next_step.is_err());
    let r = d.settle();
    assert!(r.next_step.is_err());
    free(p, 3_072);
    assert!(d.settle().next_step.is_ok());
    d.seal();
    assert!(d.settle().next_step.is_err());
}
#[test]
fn a_free_cannot_turn_stale_debt_into_new_rights_while_frozen() {
    let a = authority(32_768);
    let q = work(&a);
    q.prefund(4_096).unwrap();
    let d = q.create_domain(0).unwrap();
    let mut l = d.activate(0, 0).unwrap();
    let p = l.record_allocation(1_024);
    l.finish();
    let mut w = a.take_capacity_writer().unwrap();
    w.set_capacity(0).unwrap();
    free(p, 1_024);
    assert_eq!(d.cover_debt(), 0);
    assert_eq!(d.snapshot().free, 0);
}

#[test]
fn protected_trim_accepts_real_debt_release_without_minting_free_rights() {
    let a = authority(32_768);
    let control = a.install_control_branch(4_096).unwrap();
    let baseline = a.root().committed_bytes();
    let d = control.create_domain(5).unwrap();
    let mut scope = d.activate(5, 0).unwrap();
    let released = scope.record_allocation(9);
    let retained = scope.record_allocation(1);
    assert_eq!(scope.finish().debt, 5);
    assert_eq!(a.root().committed_bytes(), baseline + 5);
    free(released, 9);
    assert_eq!(d.trim_idle_to(1), 3);
    let state = d.snapshot();
    assert_eq!((state.authorized, state.live, state.committed), (2, 1, 2));
    assert_eq!((state.free, state.debt), (1, 0));
    assert_eq!(a.root().committed_bytes(), baseline);
    assert_eq!(control.committed_bytes(), 4_096);
    assert!(a.snapshot().root.is_internally_consistent());
    free(retained, 1);
    d.retire_lane().unwrap();
    drop(d);
    assert!(a.maintain(64).complete);
    assert_eq!(control.local_free_bytes(), 4_096);
    assert_eq!(a.root().committed_bytes(), baseline);
}

#[test]
fn protected_retirement_keeps_unbacked_residual_until_real_free() {
    let a = authority(32_768);
    let control = a.install_control_branch(4_096).unwrap();
    let baseline = a.root().committed_bytes();
    let d = control.create_domain(1).unwrap();
    let mut scope = d.activate(1, 0).unwrap();
    let origin = scope.record_allocation(2);
    assert_eq!(scope.finish().debt, 1);
    d.retire_lane().unwrap();
    let residual = d.snapshot();
    assert_eq!(
        (residual.authorized, residual.committed, residual.debt),
        (1, 2, 1)
    );
    assert_eq!(residual.free, 0);
    assert_eq!(a.root().committed_bytes(), baseline + 1);
    assert!(a.snapshot().root.is_internally_consistent());
    drop(d);
    free(origin, 2);
    assert!(a.maintain(64).complete);
    assert_eq!(control.local_free_bytes(), 4_096);
    assert_eq!(a.root().committed_bytes(), baseline);
    assert!(a.snapshot().root.is_internally_consistent());
}

#[test]
fn unbacked_control_exposure_cannot_make_protected_treasury_refundable() {
    let a = authority(32_768);
    let floor = OWNER_METADATA_BYTES + 2;
    let control = a.install_control_branch(floor).unwrap();
    let baseline = a.root().committed_bytes();
    let d = control.create_domain(1).unwrap();
    let mut scope = d.activate(1, 0).unwrap();
    let origin = scope.record_allocation(2);
    assert_eq!(scope.finish().debt, 1);
    d.retire_lane().unwrap();
    assert_eq!(control.local_free_bytes(), 1);
    assert_eq!(control.return_slack(u64::MAX), 0);
    assert_eq!(control.committed_bytes(), floor + 1);
    assert_eq!(a.root().committed_bytes(), baseline + 1);
    assert!(a.maintain(64).complete);
    assert_eq!(control.local_free_bytes(), 1);
    drop(d);
    free(origin, 2);
    assert!(a.maintain(64).complete);
    assert_eq!(control.local_free_bytes(), floor);
    assert_eq!(control.committed_bytes(), floor);
    assert_eq!(a.root().committed_bytes(), baseline);
}

#[test]
fn account_handoff_preserves_residual_debt_and_cannot_authorize_it() {
    let a = authority(32_768);
    let q = work(&a);
    let d = q.create_domain(1).unwrap();
    let mut scope = d.activate(1, 0).unwrap();
    let origin = scope.record_allocation(2);
    scope.finish();
    // Remove unrelated quantum slack before measuring the pure handoff.
    q.return_slack(u64::MAX);
    let before = a.root().committed_bytes();
    q.retire(&exited()).unwrap();
    let residual = d.snapshot();
    assert_eq!(
        (residual.authorized, residual.committed, residual.debt),
        (1, 2, 1)
    );
    assert_eq!(residual.free, 0);
    assert_eq!(a.root().committed_bytes(), before);
    assert_eq!(a.snapshot().root.settled_debt_bytes, 1);
    assert!(a.snapshot().root.is_internally_consistent());
    free(origin, 2);
    drop(d);
    drop(q);
    assert!(a.maintain(64).complete);
    assert_eq!(a.pressure_projection().residual_committed, 0);
}
