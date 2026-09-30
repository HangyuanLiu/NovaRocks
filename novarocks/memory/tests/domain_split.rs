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
fn split_establishes_disjoint_rights_atomically_and_leaves_live_origin() {
    let a = authority(32_768);
    let q = work(&a);
    let original = q.create_domain(4096).unwrap();
    let mut scope = original.activate(1024, 0).unwrap();
    let old = scope.record_allocation(1024);
    assert!(original.split_free(1024).is_err());
    scope.finish();
    let before = q.committed_bytes();
    let child = original.split_free(2048).unwrap();
    assert_eq!(q.committed_bytes(), before + OWNER_METADATA_BYTES);
    assert_eq!(original.snapshot().authorized, 2048);
    assert_eq!(child.snapshot().authorized, 2048);
    let before_failed = original.snapshot();
    assert!(original.split_free(1025).is_err());
    assert_eq!(original.snapshot(), before_failed);
    let mut left = original.activate(1024, 0).unwrap();
    let mut right = child.activate(2048, 0).unwrap();
    let lp = left.record_allocation(2048);
    let rp = right.record_allocation(2048);
    left.finish();
    right.finish();
    assert_eq!(original.snapshot().debt, 1024);
    assert_eq!(child.snapshot().debt, 0);
    free(rp, 2048);
    child.settle();
    assert_eq!(original.snapshot().debt, 1024);
    free(lp, 2048);
    free(old, 1024);
}
#[test]
fn split_denial_for_metadata_or_frozen_ancestor_never_debits_source() {
    let a = authority(32_768);
    let q = work(&a);
    let domain = q.create_domain(4096).unwrap();
    let before = domain.snapshot();
    q.install_policy(q.committed_bytes(), LimitDimension::Work);
    assert!(domain.split_free(1024).is_err());
    assert_eq!(domain.snapshot(), before);
    let mut writer = a.take_capacity_writer().unwrap();
    writer.set_capacity(0).unwrap();
    assert!(domain.split_free(1).is_err());
    assert_eq!(domain.snapshot(), before);
}
