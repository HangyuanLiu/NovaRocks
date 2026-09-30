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
fn active_owner_limit_refuses_new_lane_without_displacing_existing_obligations() {
    let mut config = AuthorityConfig::new(131_072, 65_536, 65_536);
    config.max_accounts = 8;
    config.max_active_owners = 2;
    config.metadata_budget_bytes = 16_384;
    let a = MemoryAuthority::new(config).unwrap();
    let q = work(&a);
    let first = q.create_domain(512).unwrap();
    let second = q.create_domain(1_024).unwrap();
    let before = a.root().committed_bytes();
    assert_eq!(
        q.create_domain(1_024).unwrap_err(),
        CapacityError::MetadataExhausted {
            registry: MetadataRegistryLabel::Owners,
            limit: 2,
        }
    );
    assert_eq!(a.root().committed_bytes(), before);
    assert_eq!(first.snapshot().authorized, 512);
    assert_eq!(second.snapshot().authorized, 1_024);
    assert!(a.snapshot().root.is_internally_consistent());
}

#[test]
fn byte_budget_exhaustion_preserves_residual_and_a_real_reclaim_restores_admission() {
    let mut config = AuthorityConfig::new(131_072, 65_536, 65_536);
    config.max_accounts = 4;
    config.max_active_owners = 1;
    config.metadata_budget_bytes = 4 * (OWNER_METADATA_BYTES + 2 * size_of::<usize>() as u64);
    let a = MemoryAuthority::new(config).unwrap();
    let mut origins = Vec::new();
    loop {
        let q = work(&a);
        match q.create_domain(64) {
            Ok(domain) => {
                let mut scope = domain.activate(64, 0).unwrap();
                let origin = scope.record_allocation(64);
                scope.finish();
                q.retire(&exited()).unwrap();
                origins.push(origin);
            }
            Err(error) => {
                assert!(matches!(
                    error,
                    CapacityError::MetadataExhausted {
                        registry: MetadataRegistryLabel::Owners,
                        ..
                    }
                ));
                break;
            }
        }
        assert!(
            origins.len() <= 8,
            "finite metadata budget must bound retained storage"
        );
    }
    assert_eq!(origins.len(), 4);
    let pressure = a.pressure_projection();
    assert_eq!(pressure.residual_committed, 4 * (64 + OWNER_METADATA_BYTES));
    assert_eq!(pressure.residual_metadata, 4 * OWNER_METADATA_BYTES);
    // Transfer occurred at the final available slot, without new metadata.
    assert_eq!(a.live_accounts(), 1);
    let candidate = work(&a);
    let before_refusal = a.pressure_projection();
    assert!(
        matches!(
            a.request_domain(&candidate, 64, 64),
            RequestOutcome::Refused(CapacityError::MetadataExhausted {
                registry: MetadataRegistryLabel::Owners,
                ..
            })
        ),
        "metadata exhaustion cannot become FE-actionable shared shortage"
    );
    assert_eq!(a.pressure_projection(), before_refusal);
    drop(candidate);
    let first = origins.remove(0);
    free(first, 64);
    a.request_maintenance(MaintenanceReason::ExplicitLocalReclaim);
    while !a.maintain(64).complete {}
    assert_eq!(
        a.pressure_projection().residual_metadata,
        3 * OWNER_METADATA_BYTES
    );
    let q = work(&a);
    let replacement = q
        .create_domain(64)
        .expect("real record reclamation restores byte-backed storage");
    assert_eq!(replacement.snapshot().authorized, 64);
    for origin in origins {
        free(origin, 64);
    }
    q.retire(&exited()).unwrap();
    drop(replacement);
    drop(q);
    a.request_maintenance(MaintenanceReason::ExplicitLocalReclaim);
    while !a.maintain(64).complete {}
    assert_eq!(a.pressure_projection().residual_committed, 0);
}

#[test]
fn invalid_owner_limits_fail_at_configuration_time() {
    for (active, bytes) in [(0, 1_024), (1, 0)] {
        let mut config = AuthorityConfig::new(32_768, 16_384, 16_384);
        config.max_active_owners = active;
        config.metadata_budget_bytes = bytes;
        assert_eq!(
            MemoryAuthority::new(config).unwrap_err(),
            ConfigError::MetadataLimitIsZero {
                registry: MetadataRegistryLabel::Owners,
            }
        );
    }
}

#[test]
fn retired_execution_account_no_longer_occupies_active_account_registry() {
    let mut config = AuthorityConfig::new(65_536, 32_768, 32_768);
    config.max_accounts = 2;
    config.max_active_owners = 1;
    config.metadata_budget_bytes = 4_096;
    let a = MemoryAuthority::new(config).unwrap();
    let retired = work(&a);
    retired.retire(&exited()).unwrap();
    assert!(retired.is_retired());
    assert_eq!(a.live_accounts(), 1);
    let replacement = a
        .create_account(AccountKind::Work, ExternalRef::NONE)
        .expect(
            "a retired account releases registry admission even if a diagnostic handle remains",
        );
    assert_ne!(retired.id(), replacement.id());
    assert!(retired.create_domain(0).is_err());
}

#[test]
fn retired_handle_generations_retain_real_storage_charge_until_final_drop() {
    let mut config = AuthorityConfig::new(131_072, 65_536, 65_536);
    config.max_accounts = 2;
    config.max_active_owners = 1;
    config.metadata_budget_bytes = 4_096;
    let a = MemoryAuthority::new(config).unwrap();
    let baseline = a.root().committed_bytes();
    let mut writer = a.take_capacity_writer().unwrap();
    writer
        .set_capacity(baseline + 3 * ACCOUNT_METADATA_BYTES)
        .unwrap();
    let mut retired = Vec::new();
    for generation in 1..=3 {
        let account = work(&a);
        account.retire(&exited()).unwrap();
        retired.push(account);
        assert_eq!(a.live_accounts(), 1);
        assert_eq!(
            a.root().committed_bytes(),
            baseline + generation * ACCOUNT_METADATA_BYTES
        );
        assert_eq!(
            a.snapshot().root.storage_metadata_bytes,
            baseline + generation * ACCOUNT_METADATA_BYTES
        );
        assert!(a.snapshot().root.is_internally_consistent());
    }
    let before_refusal = a.root().committed_bytes();
    assert!(matches!(
        a.create_account(AccountKind::Work, ExternalRef::NONE),
        Err(CapacityError::ShortageCandidate(_))
    ));
    assert_eq!(a.root().committed_bytes(), before_refusal);
    drop(retired.pop());
    assert_eq!(
        a.root().committed_bytes(),
        baseline + 2 * ACCOUNT_METADATA_BYTES
    );
    let replacement = work(&a);
    assert_eq!(a.root().committed_bytes(), before_refusal);
    drop(replacement);
    drop(retired);
    assert_eq!(a.root().committed_bytes(), baseline);
    assert_eq!(a.snapshot().root.storage_metadata_bytes, baseline);
    assert!(a.snapshot().root.is_internally_consistent());
}
