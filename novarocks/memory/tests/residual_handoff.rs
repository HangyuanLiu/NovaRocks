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
fn retire_preserves_payload_metadata_but_ends_query_pressure() {
    let a = authority(32_768);
    let q = work(&a);
    let d = q.create_domain(1_024).unwrap();
    let mut l = d.activate(1_024, 0).unwrap();
    let o = l.record_allocation(1_024);
    l.finish();
    let before = a.pressure_projection();
    let r = q.retire(&exited()).unwrap();
    assert_eq!(r.transferred_payload, 1_024);
    assert_eq!(
        r.transferred_metadata,
        novarocks_memory::OWNER_METADATA_BYTES
    );
    let after = a.pressure_projection();
    assert_eq!(after.root_committed, before.root_committed);
    assert_eq!(after.query_pressure(), 0);
    assert!(q.is_retired());
    assert!(d.activate(0, 0).is_err());
    drop(q);
    drop(d);
    free(o, 1_024);
    a.request_maintenance(novarocks_memory::MaintenanceReason::ExplicitLocalReclaim);
    while !a.maintain(64).complete {}
    assert_eq!(a.pressure_projection().residual_committed, 0);
}
