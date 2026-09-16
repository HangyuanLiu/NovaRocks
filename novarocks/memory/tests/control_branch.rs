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

//! The control branch as a separate hard partition (MEM-1 wave-2 W2A).
//!
//! A cancellation, a status update or a result header must still be able to
//! allocate when ordinary query work has filled the process. These tests state
//! that as a property of the account tree rather than of any caller's
//! discipline: control gets its own branch off the root, so the things that
//! stop work capacity cannot reach it.

use novarocks_memory::{
    AccountKind, AuthorityConfig, ExternalRef, LimitDimension, MemoryAuthority,
};

const P: u64 = 64 * 1024 * 1024;
const CONTROL_BYTES: u64 = 4 * 1024 * 1024;

fn authority() -> MemoryAuthority {
    MemoryAuthority::new(AuthorityConfig::new(P, P - P / 4, P / 4))
        .expect("the partition under test must be valid")
}

#[test]
fn a_process_has_no_control_branch_until_one_is_installed() {
    let authority = authority();
    assert!(authority.control_branch().is_none());

    let installed = authority
        .install_control_branch(CONTROL_BYTES)
        .expect("the first installation must succeed");
    assert_eq!(installed.id(), authority.control_branch().unwrap().id());
}

#[test]
fn a_second_installation_is_refused_rather_than_silently_ignored() {
    let authority = authority();
    authority
        .install_control_branch(CONTROL_BYTES)
        .expect("the first installation must succeed");

    // Two owners each sizing the control partition is a configuration error,
    // not something to absorb: the second caller would otherwise believe its
    // own number took effect.
    authority
        .install_control_branch(CONTROL_BYTES * 2)
        .expect_err("installing the control branch twice must be refused");
}

#[test]
fn closing_the_work_branch_to_growth_leaves_control_able_to_grant() {
    let authority = authority();
    let control = authority
        .install_control_branch(CONTROL_BYTES)
        .expect("the control branch must install");
    let work = authority
        .create_account(AccountKind::Work, ExternalRef::from_u128(1))
        .expect("a work account must be creatable");

    work.close_to_growth();

    work.request_grant(1024)
        .expect_err("a closed work account must refuse further capacity");
    control
        .request_grant(1024)
        .expect("cancelling work must not reach the control branch");
}

#[test]
fn a_work_side_limit_does_not_constrain_the_control_branch() {
    let authority = authority();
    let control = authority
        .install_control_branch(CONTROL_BYTES)
        .expect("the control branch must install");
    let work = authority
        .create_account(AccountKind::Work, ExternalRef::from_u128(2))
        .expect("a work account must be creatable");

    // A per-scope limit small enough that any real request exceeds it. In a
    // strict tree an account has exactly one parent chain, and the control
    // branch is not on this one, so the policy cannot reach it.
    work.install_policy(1024, LimitDimension::Work);

    work.request_grant(64 * 1024)
        .expect_err("the work limit must bind the branch it was installed on");
    control
        .request_grant(64 * 1024)
        .expect("a work-side limit must not constrain control");
}

#[test]
fn the_control_branch_is_bounded_by_its_own_limit() {
    let authority = authority();
    let control = authority
        .install_control_branch(CONTROL_BYTES)
        .expect("the control branch must install");

    // Separate does not mean unbounded: the branch is a hard partition with a
    // size, otherwise control-plane growth would be the new blind spot.
    control
        .request_grant(CONTROL_BYTES + 1)
        .expect_err("the control branch must honour its own limit");
    control
        .request_grant(CONTROL_BYTES)
        .expect("a request within the control limit must be granted");
}
