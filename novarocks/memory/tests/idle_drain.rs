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
fn an_independent_maintenance_entry_consumes_free_after_a_completed_sweep() {
    let a = authority(32_768);
    let q = work(&a);
    let d = q.create_domain(1_024).unwrap();
    let mut l = d.activate(1_024, 0).unwrap();
    let p = l.record_allocation(1_024);
    l.finish();
    q.retire(&exited()).unwrap();
    drop(q);
    drop(d);
    while !a.maintain(64).complete {}
    assert!(a.pressure_projection().residual_committed > 0);
    free(p, 1_024);
    while !a.maintain(64).complete {}
    assert_eq!(a.pressure_projection().residual_committed, 0);
}
#[test]
fn three_drains_preserve_control_and_account_slack_is_reclaimed() {
    for reason in [
        MaintenanceReason::CapacityReduced,
        MaintenanceReason::ShortageCandidate,
        MaintenanceReason::ExplicitLocalReclaim,
    ] {
        let a = authority(32_768);
        let control = a.install_control_branch(4_096).unwrap();
        let q = work(&a);
        q.prefund(8_192).unwrap();
        let c = control.create_domain(1_024).unwrap();
        a.request_maintenance(reason);
        while !a.maintain(64).complete {}
        assert_eq!(q.local_free_bytes(), 0);
        assert_eq!(control.committed_bytes(), 4_096);
        let mut w = a.take_capacity_writer().unwrap();
        w.set_capacity(0).unwrap();
        c.refill(1_024).unwrap();
        assert!(a.root().create_domain(1).is_err());
    }
}
#[test]
fn active_drain_is_deferred_to_the_scopes_safe_boundary() {
    let a = authority(32_768);
    let q = work(&a);
    let d = q.create_domain(4_096).unwrap();
    assert_eq!(a.pressure_projection().settled_idle_authorization, 4_096);
    let l = d.activate(1_024, 0).unwrap();
    while !a.maintain(64).complete {}
    assert_eq!(d.snapshot().authorized, 4_096);
    let pending = a.pressure_projection();
    assert_eq!(pending.pending_drain_domains, 1);
    assert_eq!(pending.settled_idle_authorization, 0);
    l.finish();
    assert_eq!(d.snapshot().authorized, 0);
    assert_eq!(a.pressure_projection().pending_drain_domains, 0);
}
