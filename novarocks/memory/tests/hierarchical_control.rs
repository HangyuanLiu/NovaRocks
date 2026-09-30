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
#[test]
fn funded_steps_and_parent_slack_do_not_touch_root_ledger() {
    let a = authority(65_536);
    let q = work(&a);
    q.prefund(16_384).unwrap();
    let before = a.root().interactions();
    let d = q.create_domain(4_096).unwrap();
    assert_eq!(a.root().interactions(), before);
    for _ in 0..32 {
        let mut l = d.activate(1_024, 32).unwrap();
        let o = l.record_allocation(1_024);
        free(o, 1_024);
        l.finish();
    }
    assert_eq!(a.root().interactions(), before);
    d.refill(1_024).unwrap();
    assert_eq!(a.root().interactions(), before);
}
#[test]
fn frozen_ancestor_prevents_new_domain_from_exclusive_slack() {
    let a = authority(65_536);
    let q = work(&a);
    q.prefund(20_480).unwrap();
    let d = q.create_domain(10_240).unwrap();
    let base = a.root().committed_bytes();
    let mut w = a.take_capacity_writer().unwrap();
    w.set_capacity(base - 5_120).unwrap();
    let before = a.root().committed_bytes();
    assert!(q.create_domain(1_024).is_err());
    assert!(d.refill(1_024).is_err());
    assert_eq!(a.root().committed_bytes(), before);
    assert!(q.return_slack(5_120) > 0);
}
