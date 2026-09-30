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
fn query_handoff_reclassifies_payload_and_metadata_without_lowering_u_or_n() {
    let a = authority(65_536);
    let group = a
        .create_account(AccountKind::ResourceGroup, ExternalRef::NONE)
        .unwrap();
    let baseline = a.pressure_projection();
    let q = group
        .create_child(AccountKind::Work, ExternalRef::NONE)
        .unwrap();
    let domain = q.create_domain(1_024).unwrap();
    let mut scope = domain.activate(1_024, 0).unwrap();
    let origin = scope.record_allocation(1_024);
    scope.finish();
    let before = a.pressure_projection();
    let obligation = 1_024 + OWNER_METADATA_BYTES;
    assert_eq!(before.query_committed, obligation);
    assert_eq!(before.residual_committed, 0);
    assert_eq!(
        before.root_committed,
        baseline.root_committed + ACCOUNT_METADATA_BYTES + obligation
    );

    q.retire(&exited()).unwrap();
    let after = a.pressure_projection();
    assert_eq!(after.query_committed, 0);
    assert_eq!(after.residual_query_committed, obligation);
    assert_eq!(after.residual_committed, obligation);
    assert_eq!(after.residual_metadata, OWNER_METADATA_BYTES);
    assert_eq!(after.storage_metadata, before.storage_metadata);
    assert_eq!(after.root_committed, before.root_committed);
    assert_eq!(after.query_pressure(), before.query_pressure());
    assert_eq!(after.non_evictable(0), before.non_evictable(0));
    assert_eq!(
        after.root_committed,
        after.storage_metadata + after.query_committed + after.residual_committed
    );
    assert!(after.root_revision >= before.root_revision);
    assert_eq!(q.committed_bytes(), 0);
    assert_eq!(group.committed_bytes(), obligation);
    assert!(a.snapshot().root.is_internally_consistent());

    free(origin, 1_024);
    drop(domain);
    drop(q);
    a.request_maintenance(MaintenanceReason::ExplicitLocalReclaim);
    while !a.maintain(64).complete {}
    let released = a.pressure_projection();
    assert_eq!(released.query_pressure(), 0);
    assert_eq!(released.non_evictable(0), Some(baseline.root_committed));
}

#[test]
fn service_residual_is_disjoint_from_query_origin_subset() {
    let a = authority(65_536);
    let service = a
        .create_account(AccountKind::Service, ExternalRef::NONE)
        .unwrap();
    let q = work(&a);
    let service_domain = service.create_domain(256).unwrap();
    let query_domain = q.create_domain(512).unwrap();
    let mut service_scope = service_domain.activate(256, 0).unwrap();
    let service_origin = service_scope.record_allocation(256);
    service_scope.finish();
    let mut query_scope = query_domain.activate(512, 0).unwrap();
    let query_origin = query_scope.record_allocation(512);
    query_scope.finish();
    let before = a.pressure_projection();
    service.retire(&exited()).unwrap();
    q.retire(&exited()).unwrap();
    let after = a.pressure_projection();
    assert_eq!(after.query_committed, 0);
    assert_eq!(after.residual_query_committed, 512 + OWNER_METADATA_BYTES);
    assert_eq!(after.residual_committed, 768 + 2 * OWNER_METADATA_BYTES);
    assert_eq!(after.residual_metadata, 2 * OWNER_METADATA_BYTES);
    assert_eq!(after.query_pressure(), before.query_pressure());
    assert_eq!(after.root_committed, before.root_committed);
    assert_eq!(after.non_evictable(after.root_committed + 1), None);
    free(service_origin, 256);
    free(query_origin, 512);
}
