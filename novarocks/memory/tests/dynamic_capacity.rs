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
fn capacity_writer_is_unique_and_target_does_not_follow_debt() {
    let a = authority(16_384);
    let q = work(&a);
    // Account backing is already charged before the domain acquires rights.
    let base = a.root().committed_bytes();
    let d = q.create_domain(4_096).unwrap();
    let mut lease = d.activate(4_096, 0).unwrap();
    let origin = lease.record_allocation(6_144);
    let r = lease.finish();
    assert_eq!(r.debt, 2_048);
    free(origin, 6_144);
    d.settle();
    let mut w = a.take_capacity_writer().unwrap();
    assert!(a.take_capacity_writer().is_err());
    w.set_capacity(base + OWNER_METADATA_BYTES + 4_096).unwrap();
    assert!(q.prefund(6_144).is_err());
    assert_eq!(a.capacity_bytes(), base + OWNER_METADATA_BYTES + 4_096);
    assert!(w.set_capacity(16_385).is_err());
    w.set_capacity(0).unwrap();
    assert!(a.snapshot().root.growth_frozen);
}
#[test]
fn zero_target_preserves_control_floor_and_existing_domain_rights() {
    let a = authority(32_768);
    let control = a.install_control_branch(4_096).unwrap();
    let q = work(&a);
    let d = q.create_domain(2_048).unwrap();
    let before = a.root().committed_bytes();
    let mut w = a.take_capacity_writer().unwrap();
    w.set_capacity(0).unwrap();
    let c = control.create_domain(1_024).unwrap();
    let mut scope = c.activate(1_024, 0).unwrap();
    let o = scope.record_allocation(1_024);
    scope.finish();
    assert_eq!(a.root().committed_bytes(), before);
    let old = d.activate(2_048, 0).unwrap();
    old.finish();
    assert!(q.create_domain(1).is_err());
    free(o, 1_024);
}
