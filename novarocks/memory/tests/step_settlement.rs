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
fn external_to_live_and_multiple_holders_never_charge_twice() {
    let a = authority(32_768);
    let q = work(&a);
    let d = q.create_domain(4_096).unwrap();
    let before = a.root().committed_bytes();
    let mut b = d.external_bound(2_048).unwrap();
    assert_eq!(d.snapshot().external, 2_048);
    let p = b.convert_to_live(1_024).unwrap();
    assert_eq!(d.snapshot().external, 1_024);
    assert_eq!(d.snapshot().live, 1_024);
    let pin1 = d.pin();
    let pin2 = pin1.clone();
    assert_eq!(pin1.sampled_live_bytes(), 1_024);
    assert_eq!(pin2.sampled_live_bytes(), 1_024);
    d.settle();
    assert_eq!(a.root().committed_bytes(), before);
    assert!(a.snapshot().root.is_internally_consistent());
    drop(b);
    free(p, 1_024);
}
#[test]
fn explicit_grant_remainder_and_escape_keep_one_charge() {
    let a = authority(32_768);
    let q = work(&a);
    let mut grant = q.request_explicit(2_048).unwrap();
    let p = grant.record_success(1_024).unwrap();
    assert_eq!(grant.remaining_bytes(), 1_024);
    assert!(grant.record_success(1_025).is_err());
    drop(grant);
    assert_eq!(
        a.pressure_projection().query_committed,
        1_024 + OWNER_METADATA_BYTES
    );
    free(p, 1_024);
    while !a.maintain(64).complete {}
    assert_eq!(a.pressure_projection().query_committed, 0);
    assert_eq!(a.pressure_projection().residual_committed, 0);
}
#[test]
fn boundary_oscillation_within_retained_workset_does_not_refill_each_step() {
    let a = authority(32_768);
    let q = work(&a);
    let d = q.create_domain(0).unwrap();
    d.ensure_workset(1_000).unwrap();
    let before = a.root().interactions();
    for required in [900, 1_000, 950, 1_024, 900, 1_000] {
        d.ensure_workset(required).unwrap();
        let l = d.activate(required, 0).unwrap();
        l.finish();
    }
    assert_eq!(a.root().interactions(), before);
}

#[test]
fn external_authorization_cannot_duplicate_active_stock() {
    let a = authority(32768);
    let q = work(&a);
    let d = q.create_domain(1).unwrap();
    let scope = d.activate(1, 0).unwrap();
    assert!(d.external_bound(1).is_err());
    assert_eq!(d.snapshot().external, 0);
    scope.finish();
    let bound = d.external_bound(1).unwrap();
    assert!(d.activate(1, 0).is_err());
    drop(bound);
}
#[test]
fn conversion_accepts_only_its_bound_facts_until_scope_debt_settles() {
    let a = authority(32768);
    let q = work(&a);
    let d = q.create_domain(1024).unwrap();
    let mut bound = d.external_bound(512).unwrap();
    let mut scope = d.activate(512, 0).unwrap();
    let first = scope.record_allocation(1000);
    let second = bound.convert_to_live(512).unwrap();
    assert_eq!(q.snapshot().live_bytes, 512 + OWNER_METADATA_BYTES);
    let receipt = scope.finish();
    assert_eq!(receipt.debt, 488);
    assert_eq!(d.snapshot().committed, 1512);
    free(first, 1000);
    free(second, 512);
}

#[test]
fn restored_capacity_rechecks_cached_debt_refusal_without_new_allocation() {
    let a = authority(32768);
    let q = work(&a);
    let d = q.create_domain(1024).unwrap();
    let mut writer = a.take_capacity_writer().unwrap();
    writer.set_capacity(a.root().committed_bytes()).unwrap();
    let mut scope = d.activate(1024, 0).unwrap();
    let p = scope.record_allocation(2048);
    assert!(scope.finish().next_step.is_err());
    writer.set_capacity(32768).unwrap();
    assert!(d.settle().next_step.is_ok());
    assert_eq!(d.snapshot().debt, 1024);
    free(p, 2048);
}
