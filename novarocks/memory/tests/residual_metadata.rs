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
fn many_historical_residuals_do_not_exhaust_concurrent_owner_admission() {
    let mut config = AuthorityConfig::new(262_144, 131_072, 131_072);
    config.max_accounts = 4;
    config.max_active_owners = 1;
    config.metadata_budget_bytes = 32_768;
    let a = MemoryAuthority::new(config).unwrap();
    let baseline = a.root().committed_bytes();
    let mut origins = Vec::new();
    for generation in 0..32 {
        let q = work(&a);
        let domain = q
            .create_domain(128)
            .expect("residuals must not consume an active owner position");
        let mut scope = domain.activate(128, 0).unwrap();
        origins.push(scope.record_allocation(128));
        scope.finish();
        let before = a.root().committed_bytes();
        let receipt = q.retire(&exited()).unwrap();
        assert_eq!(receipt.transferred_payload, 128);
        assert_eq!(receipt.transferred_metadata, OWNER_METADATA_BYTES);
        assert_eq!(a.root().committed_bytes(), before);
        let pressure = a.pressure_projection();
        assert_eq!(
            pressure.residual_metadata,
            (generation + 1) * OWNER_METADATA_BYTES
        );
        assert_eq!(
            pressure.residual_committed,
            (generation + 1) * (128 + OWNER_METADATA_BYTES)
        );
        assert_eq!(pressure.query_pressure(), 0);
    }
    assert_eq!(a.live_accounts(), 1);
    for origin in origins.into_iter().rev() {
        free(origin, 128);
    }
    a.request_maintenance(MaintenanceReason::ExplicitLocalReclaim);
    while !a.maintain(3).complete {}
    let pressure = a.pressure_projection();
    assert_eq!(pressure.residual_committed, 0);
    assert_eq!(pressure.residual_metadata, 0);
    assert_eq!(pressure.storage_metadata, baseline);
    assert_eq!(
        a.root().committed_bytes(),
        baseline,
        "retained index backing remains charged to storage"
    );
}

#[test]
fn multiple_ancestor_retirements_keep_metadata_and_payload_on_one_live_branch() {
    let a = authority(65_536);
    let group = a
        .create_account(AccountKind::ResourceGroup, ExternalRef::NONE)
        .unwrap();
    let q = group
        .create_child(AccountKind::Work, ExternalRef::NONE)
        .unwrap();
    let task = q
        .create_child(AccountKind::Task, ExternalRef::NONE)
        .unwrap();
    let domain = task.create_domain(512).unwrap();
    let mut scope = domain.activate(512, 0).unwrap();
    let origin = scope.record_allocation(512);
    scope.finish();
    let before = a.pressure_projection();
    for account in [&task, &q, &group] {
        account.retire(&exited()).unwrap();
        let pressure = a.pressure_projection();
        assert_eq!(pressure.root_committed, before.root_committed);
        if account.id() == task.id() {
            assert_eq!(pressure.query_pressure(), before.query_pressure());
            assert_eq!(pressure.residual_metadata, 0);
            assert_eq!(pressure.residual_committed, 0);
        } else {
            assert_eq!(pressure.query_pressure(), 0);
            assert_eq!(pressure.residual_metadata, OWNER_METADATA_BYTES);
            assert_eq!(pressure.residual_committed, 512 + OWNER_METADATA_BYTES);
        }
        assert_eq!(account.committed_bytes(), 0);
    }
    assert_eq!(a.live_accounts(), 1);
    free(origin, 512);
    drop(domain);
    drop(task);
    drop(q);
    drop(group);
    a.request_maintenance(MaintenanceReason::ExplicitLocalReclaim);
    while !a.maintain(64).complete {}
    assert_eq!(a.pressure_projection().residual_committed, 0);
}
