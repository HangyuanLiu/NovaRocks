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
fn independent_free_rights_do_not_offset_another_domains_debt() {
    let a = authority(32_768);
    let q = work(&a);
    let base = a.root().committed_bytes();
    let d1 = q.create_domain(1_024).unwrap();
    let d2 = q.create_domain(1_024).unwrap();
    let mut l = d1.activate(1_024, 0).unwrap();
    let p = l.record_allocation(2_048);
    l.finish();
    assert_eq!(d1.snapshot().debt, 1_024);
    assert_eq!(d2.snapshot().free, 1_024);
    assert_eq!(
        a.root().committed_bytes(),
        base + 3_072 + 2 * OWNER_METADATA_BYTES
    );
    assert!(a.snapshot().root.is_internally_consistent());
    free(p, 2_048);
}
#[test]
fn denial_does_not_leave_partial_commitment() {
    let a = authority(16_384);
    let group = a
        .create_account(AccountKind::ResourceGroup, ExternalRef::NONE)
        .unwrap();
    let q = group
        .create_child(AccountKind::Work, ExternalRef::NONE)
        .unwrap();
    group.install_policy(2_048, LimitDimension::ResourceGroup);
    let before = a.root().committed_bytes();
    assert!(q.create_domain(4_096).is_err());
    assert_eq!(a.root().committed_bytes(), before);
    assert_eq!(q.committed_bytes(), 0);
    assert_eq!(group.committed_bytes(), 0);
}
