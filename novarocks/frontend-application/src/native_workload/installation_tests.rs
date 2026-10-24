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

use super::*;

fn fixture() -> FrontendWorkloadCapacityProfile {
    original_owner_test_profile(QueryBlockingExecutorConfig::new(
        NonZeroUsize::new(1).unwrap(),
        NonZeroUsize::new(1).unwrap(),
    ))
}

#[test]
fn received_production_rows_require_real_batch_capacity() {
    let profile = fixture();
    let mut declared = profile.capacity.clone();
    // Three canonical records can contain any supported occurrence count.
    // They remain Publication responsibilities after complete receiving.
    declared.global.global = declared.global.global.with(JointResource::Publications, 3);
    assert_eq!(production_batch_records(&declared).unwrap(), 7);
    let error = FrontendWorkloadCapacityProfile::from_complete_actions(
        profile.inputs.clone(),
        &profile.workload,
        declared,
        profile.required.clone(),
        profile.children_per_slot,
        profile.proposals_per_slot,
    )
    .unwrap_err();
    assert_eq!(
        error,
        WorkError::InvalidConfig("frontend production growth records")
    );
}

#[test]
fn additional_production_changes_increase_the_original_paid_metadata_envelope() {
    let profile = fixture();
    let mut next = profile.capacity.clone();
    let storage = ControlStorageConfig::new(
        profile.policy().slots_per_class(),
        profile.children_per_slot,
        profile.proposals_per_slot.get(),
        &profile.poll_roles,
    );
    let old_m = storage.minimum_dependency_records(&next).unwrap();
    let old_base = storage.base_charge(&next).unwrap();
    next.global.global = next.global.global.with(JointResource::Publications, 3);
    next.max_changes_per_request = production_batch_records(&next).unwrap();
    let next_m = storage.minimum_dependency_records(&next).unwrap();
    // One paid request retains its complete distinct union once, irrespective
    // of the number of changes; additional C still enlarges the paid body.
    assert_eq!(next_m, old_m);
    next.max_registered_dependencies = next_m;
    let next_base = storage.base_charge(&next).unwrap();
    assert!(
        next_base.get(JointResource::MetadataBytes) > old_base.get(JointResource::MetadataBytes)
    );
    // C/M are actual retained backing, so merely increasing the configured C
    // cannot reuse the previous cheaper global/class inventory declaration.
    assert!(matches!(
        FrontendWorkloadCapacityProfile::from_complete_actions(
            profile.inputs.clone(),
            &profile.workload,
            next,
            profile.required.clone(),
            profile.children_per_slot,
            profile.proposals_per_slot,
        ),
        Err(WorkError::InvalidConfig("frontend fixed global inventory"))
    ));
}

#[test]
fn original_dependency_author_refuses_an_underfunded_membership_inventory() {
    let profile = fixture();
    let mut next = profile.capacity.clone();
    next.max_registered_dependencies -= 1;
    let error = FrontendWorkloadCapacityProfile::from_complete_actions(
        profile.inputs.clone(),
        &profile.workload,
        next,
        profile.required.clone(),
        profile.children_per_slot,
        profile.proposals_per_slot,
    )
    .unwrap_err();
    assert_eq!(
        error,
        WorkError::InvalidConfig("frontend fixed dependency inventory")
    );
}
