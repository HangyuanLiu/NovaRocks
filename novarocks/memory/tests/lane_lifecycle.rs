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
use novarocks_memory::attribution::{AttributingAllocator, binding};
use novarocks_memory::lane::{ProductionState, ResponsibilityClass, global_store};
use novarocks_memory::*;
use std::alloc::{GlobalAlloc, Layout, System};

#[test]
fn stopped_build_liability_stays_query_until_work_teardown() {
    let a = authority(65_536);
    let query = work(&a);
    let domain = query.create_domain(600).unwrap();
    let mut scope = domain.activate(600, 0).unwrap();
    let token = scope.record_allocation(600);
    scope.finish();
    let before = a.pressure_projection();
    domain.stop_producing().unwrap();
    assert_eq!(domain.lane().production_state(), ProductionState::Stopped);
    assert_eq!(
        domain.lane().responsibility_class(),
        ResponsibilityClass::Query
    );
    assert_eq!(
        a.pressure_projection().query_pressure(),
        before.query_pressure()
    );
    assert!(!domain.snapshot().residual);
    query.retire(&exited()).unwrap();
    let after = a.pressure_projection();
    assert_eq!(
        domain.lane().responsibility_class(),
        ResponsibilityClass::Residual
    );
    assert_eq!(after.root_committed, before.root_committed);
    assert_eq!(after.non_evictable(0), before.non_evictable(0));
    assert_eq!(after.query_pressure(), 0);
    assert_eq!(domain.lane().origin(), query.id());
    free(token, 600);
}

#[test]
fn child_teardown_accepts_liability_on_its_executing_work_ancestor() {
    let a = authority(65_536);
    let query = work(&a);
    let task = query
        .create_child(AccountKind::Task, ExternalRef::NONE)
        .unwrap();
    let domain = task.create_domain(600).unwrap();
    let mut scope = domain.activate(600, 0).unwrap();
    let token = scope.record_allocation(600);
    scope.finish();
    let before = a.pressure_projection();
    task.retire(&exited()).unwrap();
    assert_eq!(domain.lane().affiliation().id(), query.id());
    assert_eq!(
        domain.lane().responsibility_class(),
        ResponsibilityClass::Query
    );
    assert_eq!(domain.lane().origin(), task.id());
    assert_eq!(
        a.pressure_projection().query_pressure(),
        before.query_pressure()
    );
    query.retire(&exited()).unwrap();
    assert_eq!(
        domain.lane().responsibility_class(),
        ResponsibilityClass::Residual
    );
    free(token, 600);
}

#[test]
fn observation_creation_has_no_funding_or_stock_authority() {
    let a = authority(65_536);
    let query = work(&a);
    query.install_policy(0, LimitDimension::Work);
    let before = a.pressure_projection();
    let lane = query.create_lane().unwrap();
    assert_eq!(a.pressure_projection(), before);
    assert_eq!(lane.responsibility_class(), ResponsibilityClass::Query);
    query.close_to_growth();
    assert_eq!(
        query.create_lane().unwrap_err(),
        lane::CoverageError::AccountClosed
    );
}

#[test]
fn token_only_observation_retains_teardown_classification_and_real_last_free() {
    let a = authority(65_536);
    let query = work(&a);
    let lane = query.create_lane().unwrap();
    let reference = lane.reference();
    let wrapper = AttributingAllocator::new(System);
    let layout = Layout::from_size_align(600, 8).unwrap();
    // SAFETY: held process-store owner, one matching allocation/release and
    // exact restoration before its external production handle drops.
    let pointer = unsafe {
        let previous = binding::install_ambient(reference);
        let pointer = wrapper.alloc(layout);
        binding::restore_ambient(previous);
        pointer
    };
    assert!(!pointer.is_null());
    drop(lane);
    a.maintain(usize::MAX);
    assert_eq!(
        global_store()
            .resolve(reference)
            .unwrap()
            .responsibility_class(),
        ResponsibilityClass::Query
    );
    query.retire(&exited()).unwrap();
    assert_eq!(
        global_store()
            .resolve(reference)
            .unwrap()
            .responsibility_class(),
        ResponsibilityClass::Residual
    );
    // SAFETY: same live block and original requested layout, released once.
    unsafe {
        wrapper.dealloc(pointer, layout);
    }
    a.maintain(usize::MAX);
    assert!(global_store().snapshot_ref(reference).is_none());
}
