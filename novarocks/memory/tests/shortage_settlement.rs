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
fn shortage_consumes_previously_published_free_before_rechecking() {
    let a = authority(16_384);
    let q = work(&a);
    let d = q.create_domain(10_240).unwrap();
    let mut l = d.activate(10_240, 0).unwrap();
    let p = l.record_allocation(10_240);
    l.finish();
    free(p, 10_240);
    let other = work(&a);
    assert!(matches!(
        a.request_domain(&other, 8_192, 64),
        RequestOutcome::Granted(_)
    ));
}
#[test]
fn incomplete_coverage_only_returns_pending_and_foreign_receipts_are_refused() {
    let a = authority(16_384);
    let q = work(&a);
    let d = q.create_domain(10_240).unwrap();
    let mut l = d.activate(10_240, 0).unwrap();
    let p = l.record_allocation(10_240);
    l.finish();
    let other = work(&a);
    match a.request_domain(&other, 8_192, 0) {
        RequestOutcome::SettlementPending(r) => assert!(!r.complete),
        o => panic!("{o:?}"),
    }
    match a.request_domain(&other, 8_192, 64) {
        RequestOutcome::SharedShortage(r) => {
            assert!(r.coverage.complete);
            assert_eq!(r.refusal.capacity_revision, r.coverage.capacity_revision);
        }
        o => panic!("{o:?}"),
    }
    let foreign = authority(16_384);
    assert!(matches!(
        foreign.request_domain(&other, 8_192, 64),
        RequestOutcome::Refused(CapacityError::Invalid { .. })
    ));
    free(p, 10_240);
}
#[test]
fn account_slack_is_reclaimed_before_shared_shortage_is_actionable() {
    let a = authority(16_384);
    let q = work(&a);
    q.prefund(10_240).unwrap();
    let other = work(&a);
    assert!(matches!(
        a.request_domain(&other, 8_192, 64),
        RequestOutcome::Granted(_)
    ));
    assert_eq!(q.local_free_bytes(), 0);
}

#[test]
fn actionable_receipt_expires_after_late_free_capacity_policy_or_membership_change() {
    let a = authority(16384);
    let q = work(&a);
    let d = q.create_domain(10240).unwrap();
    let mut scope = d.activate(10240, 0).unwrap();
    let origin = scope.record_allocation(10240);
    scope.finish();
    let other = work(&a);
    let RequestOutcome::SharedShortage(receipt) = a.request_domain(&other, 8192, 64) else {
        panic!("completed shortage expected");
    };
    assert_eq!(receipt.refusal.requested, 8192);
    assert!(receipt.refusal.required_growth >= 8192);
    assert!(a.shortage_is_fresh(&receipt, std::time::Duration::from_secs(1)));
    assert!(!a.shortage_is_fresh(&receipt, std::time::Duration::ZERO));
    free(origin, 10240);
    assert!(!a.shortage_is_fresh(&receipt, std::time::Duration::from_secs(1)));
    assert!(matches!(
        a.request_domain(&other, 8192, 64),
        RequestOutcome::Granted(_)
    ));
}
