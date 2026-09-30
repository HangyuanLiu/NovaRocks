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
fn stock_miss_is_covered_by_the_same_workset_and_signal_stays_sticky() {
    let a = authority(32_768);
    let q = work(&a);
    let d = q.create_domain(4_096).unwrap();
    let before = a.root().interactions();
    let mut l = d.activate(1_024, 100).unwrap();
    let o = l.record_allocation(1_536);
    assert!(l.threshold_triggered());
    free(o, 1_536);
    assert!(l.threshold_triggered());
    let r = l.finish();
    assert_eq!(r.debt, 0);
    assert!(r.next_step.is_ok());
    assert_eq!(a.root().interactions(), before);
}
