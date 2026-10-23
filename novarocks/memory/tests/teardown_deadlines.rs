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
use novarocks_memory::attribution::scope::{
    AmbientEntryObservation, AmbientExitObservation, AmbientStepObservation,
};
use novarocks_memory::*;

fn assert_evidence_rejection_preserves_production(
    evidence: &TeardownEvidence<'_>,
    expected: TeardownError,
) {
    let a = authority(32_768);
    let q = work(&a);
    let d = q.create_domain(1_024).unwrap();
    let mut l = d.activate(1_024, 0).unwrap();
    let o = l.record_allocation(512);
    l.finish();
    let before = d.snapshot();

    assert_eq!(q.retire(evidence), Err(expected));
    assert!(!q.is_closed_to_growth());
    assert!(!q.is_retired());
    assert_eq!(d.snapshot(), before);
    assert!(!d.snapshot().residual);
    assert_eq!(
        d.lane().production_state(),
        lane::ProductionState::Producing
    );
    let mut observation = AmbientStepObservation::default();
    d.lane().run_observed(&mut observation, || {});
    assert_eq!(observation.entry(), AmbientEntryObservation::Bound);
    assert_eq!(observation.exit(), AmbientExitObservation::Restored);

    let receipt = q.retire(&exited()).unwrap();
    assert_eq!(receipt.transferred_payload, 512);
    assert!(q.is_retired());
    assert!(d.snapshot().residual);
    free(o, 512);
}

#[test]
fn deadline_and_terminal_do_not_fake_actual_io_exit() {
    let io = [IoExitEvidence {
        owner: 7,
        cancel_started_ns: 10,
        exit_bound_ns: 10,
        max_inflight: 2,
        actually_exited: false,
    }];
    let evidence = TeardownEvidence {
        tasks_exited: true,
        operators_destroyed: true,
        io: &io,
        now_ns: 20,
    };
    assert_evidence_rejection_preserves_production(
        &evidence,
        TeardownError::TeardownDeadlineExceeded { owner: 7 },
    );
}

#[test]
fn pending_io_preserves_production_until_actual_exit() {
    let io = [IoExitEvidence {
        owner: 7,
        cancel_started_ns: 10,
        exit_bound_ns: 10,
        max_inflight: 2,
        actually_exited: false,
    }];
    let evidence = TeardownEvidence {
        tasks_exited: true,
        operators_destroyed: true,
        io: &io,
        now_ns: 19,
    };
    assert_evidence_rejection_preserves_production(
        &evidence,
        TeardownError::IoPending { owner: 7 },
    );
}

#[test]
fn pending_execution_preserves_production_until_actual_exit() {
    for (tasks_exited, operators_destroyed) in [(false, true), (true, false)] {
        let evidence = TeardownEvidence {
            tasks_exited,
            operators_destroyed,
            ..exited()
        };
        assert_evidence_rejection_preserves_production(&evidence, TeardownError::ExecutionPending);
    }
}

#[test]
fn invalid_io_bounds_preserve_production_for_corrected_evidence() {
    for (cancel_started_ns, exit_bound_ns) in [(10, 0), (u64::MAX, 1)] {
        let io = [IoExitEvidence {
            owner: 7,
            cancel_started_ns,
            exit_bound_ns,
            max_inflight: 2,
            actually_exited: true,
        }];
        let evidence = TeardownEvidence {
            io: &io,
            ..exited()
        };
        assert_evidence_rejection_preserves_production(
            &evidence,
            TeardownError::InvalidIoBound { owner: 7 },
        );
    }
}

#[test]
fn root_and_control_owner_checks_precede_evidence_validation() {
    let a = authority(32_768);
    let control = a.install_control_branch(4_096).unwrap();
    let evidence = TeardownEvidence {
        tasks_exited: false,
        ..exited()
    };
    assert_eq!(
        a.root().retire(&evidence),
        Err(TeardownError::CannotRetireRoot)
    );
    assert_eq!(
        control.retire(&evidence),
        Err(TeardownError::ProtectedControlOwner)
    );
    assert!(!a.root().is_closed_to_growth());
    assert!(!control.is_closed_to_growth());
}
