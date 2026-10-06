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
fn retirement_visits_only_its_subtree_amid_ten_thousand_unrelated_lanes() {
    let a = authority(65_536);
    let service = a
        .create_account(AccountKind::Service, ExternalRef::NONE)
        .unwrap();
    let unrelated: Vec<_> = (0..10_000)
        .map(|_| service.create_lane().unwrap())
        .collect();
    let query = work(&a);
    let child = query
        .create_child(AccountKind::Task, ExternalRef::NONE)
        .unwrap();
    let lanes = [query.create_lane().unwrap(), child.create_lane().unwrap()];
    let receipt = query.retire(&exited()).unwrap();
    assert_eq!(
        receipt.visited_members, 3,
        "one child account and two own lanes"
    );
    assert!(child.is_retired());
    assert!(!service.is_closed_to_growth());
    assert!(
        unrelated
            .iter()
            .all(|lane| lane.production_state() == lane::ProductionState::Producing)
    );
    assert!(
        lanes
            .iter()
            .all(|lane| lane.responsibility_class() == lane::ResponsibilityClass::Residual)
    );
}

#[test]
fn removed_middle_sibling_never_hides_live_members() {
    let a = authority(65_536);
    let query = work(&a);
    let first = query
        .create_child(AccountKind::Task, ExternalRef::NONE)
        .unwrap();
    let middle = query
        .create_child(AccountKind::Task, ExternalRef::NONE)
        .unwrap();
    let last = query
        .create_child(AccountKind::Task, ExternalRef::NONE)
        .unwrap();
    let lane = first.create_lane().unwrap();
    drop(middle);
    let receipt = query.retire(&exited()).unwrap();
    assert_eq!(receipt.visited_members, 3);
    assert!(first.is_retired() && last.is_retired());
    assert_eq!(
        lane.responsibility_class(),
        lane::ResponsibilityClass::Residual
    );
}
