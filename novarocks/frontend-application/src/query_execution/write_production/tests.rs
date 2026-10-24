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

//! Real original metadata/decoder/session boundaries, without starting BE writers.
use super::*;
use crate::query_execution::write_result::{RootWriteDecodeContract, RootWriteResultDecoder};
use crate::query_execution::write_session::tests::{
    commit_fragment_bytes, fixture_with_outcome, known_committed, request_context,
};
use novarocks_workload_control::joint_admission::*;
use novarocks_workload_control::{
    ControlStorageConfig, WorkClass, WorkRequest, WorkloadConfig, WorkloadControl,
};
use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll, Waker},
};

fn drive(driver: &mut JointProgressDriver) {
    for _ in 0..200_000 {
        if matches!(driver.step(), JointProgress::Idle(_)) {
            return;
        }
    }
    panic!("original metadata proof did not finish");
}
fn receive<A: Future + Unpin>(admission: &mut A) -> A::Output {
    match Pin::new(admission).poll(&mut Context::from_waker(Waker::noop())) {
        Poll::Ready(value) => value,
        Poll::Pending => panic!("expected original finite metadata reply"),
    }
}
fn original_runtime() -> (WorkloadControl, JointProgressDriver) {
    let storage = ControlStorageConfig::new([8, 0, 0, 0, 0], 1, 2, &[]);
    let mut config = JointCapacityConfig {
        global: JointResourceLimits::default(),
        classes: [JointResourceLimits::default(); 5],
        lanes: [JointResourceLimits::default(); 3],
        class_lanes: [[JointResourceLimits::default(); 3]; 5],
        max_dependencies_per_claim: 2,
        max_registered_dependencies: 266,
        max_claims: 16,
        max_requests: 16,
        max_changes_per_request: 2,
        max_snapshot_bytes: 16 * 1024 * 1024,
        max_scratch_bytes: 16 * 1024 * 1024,
        max_residual_arcs: 10000,
        max_total_proof_work: 10_000_000,
        work_per_turn: NonZeroUsize::new(7).unwrap(),
    };
    let base = storage.base_charge(&config).unwrap();
    let m = base.get(JointResource::MetadataBytes);
    let limits = JointResourceLimits {
        global: JointResources::ZERO
            .with(JointResource::ScopeRecords, 8)
            .with(JointResource::ClaimRecords, 12)
            .with(JointResource::MetadataBytes, m * 16)
            .with(JointResource::UnreceivedProductions, 4)
            .with(JointResource::Publications, 4),
        per_dependency: JointResources::ZERO
            .with(JointResource::ScopeRecords, 7)
            .with(JointResource::ClaimRecords, 12)
            .with(JointResource::MetadataBytes, m * 8)
            .with(JointResource::UnreceivedProductions, 4)
            .with(JointResource::Publications, 4),
        protected_progress: base,
    };
    config.global = limits;
    config.classes[0] = limits;
    config.lanes[0] = limits;
    config.class_lanes[0][0] = limits;
    let mut required = RequiredJointProgress {
        global: base,
        classes: [JointResources::ZERO; 5],
        lanes: [JointResources::ZERO; 3],
        class_lanes: [[JointResources::ZERO; 3]; 5],
    };
    required.classes[0] = base;
    required.lanes[0] = base;
    required.class_lanes[0][0] = base;
    let control = WorkloadControl::try_new_counted(WorkloadConfig {
        root_limit: 8,
        business_limit: 8,
        query_concurrency_limit: 8,
        scope_records_limit: 8,
        ..WorkloadConfig::default()
    })
    .unwrap()
    .owner;
    let (_joint, driver) = control
        .configure_joint_capacity(config.validate(&required).unwrap())
        .unwrap();
    control.configure_control_storage(storage).unwrap();
    control.mark_ready().unwrap();
    (control, driver)
}
fn bind(scope: &WorkScope, driver: &mut JointProgressDriver) {
    let mut proposal = scope.begin_control_binding().unwrap();
    proposal
        .push_dependency(&novarocks_resource_contract::SharedDependency::LocalHost)
        .unwrap();
    proposal.end_change().unwrap();
    let mut admission = proposal.submit().unwrap();
    drive(driver);
    let mut reply = receive(&mut admission).unwrap();
    reply
        .adjustment
        .as_mut()
        .unwrap()
        .install_control_scope_in(scope)
        .unwrap()
        .notify();
    drop(reply);
    drive(driver);
}
fn register_original(
    session: &ConnectorWriteSession,
    scope: &WorkScope,
    driver: &mut JointProgressDriver,
) -> (
    QueryExecutionId,
    PlanSeal,
    UnreceivedProductionOwner,
    Vec<u8>,
) {
    let cohorts = TaskManifestBinding::original_writer_occurrences_for_test();
    assert_eq!(
        cohorts.len(),
        2,
        "two actual placements of the checked writer"
    );
    let execution = cohorts[0].task().query_execution_id();
    let plan = cohorts[0].plan();
    let quote = WriteProductionQuote::for_occurrences(cohorts.len())
        .unwrap()
        .unwrap();
    assert_eq!(quote.occurrences(), 2);
    assert_eq!(quote.metadata_bytes(), 2 * COHORT_BYTES);
    let origin: Arc<dyn Send + Sync> = session.decode_origin.clone();
    let mut admission = scope
        .begin_unreceived_productions(Arc::downgrade(&origin), quote.quote)
        .unwrap();
    drive(driver);
    let mut ready = receive(&mut admission).unwrap();
    // Actual checked cohort bytes are allocated only after the original grant.
    let mut facts = Some(encode_occurrences(cohorts.into_iter(), quote).unwrap());
    let expected = facts.as_ref().unwrap().to_vec();
    let notification = session
        .register_production_ready_in(execution, plan, &mut ready, &mut facts)
        .unwrap();
    drop((ready, admission, facts));
    notification.notify();
    let owner = session.productions.lock().unwrap().owner.clone().unwrap();
    (execution, plan, owner, expected)
}
fn decoder(
    session: &ConnectorWriteSession,
    execution: QueryExecutionId,
    plan: PlanSeal,
) -> RootWriteResultDecoder {
    let targets = [WriteTargetOrdinal::try_new(0).unwrap()];
    let mut decoder = RootWriteResultDecoder::new(
        RootWriteDecodeContract::try_new(&targets, session.targets()).unwrap(),
    );
    decoder.bind_production_execution(execution, plan);
    decoder
}
#[test]
fn checked_writer_prefix_without_end_keeps_actual_record_and_refuses_cleanup_complete() {
    let (control, mut driver) = original_runtime();
    let work = control
        .try_begin_root(WorkRequest::new(WorkClass::Query))
        .unwrap();
    let scope = work.owner.scope();
    bind(&scope, &mut driver);
    let fixture = fixture_with_outcome(1, 1, known_committed());
    let session = fixture.session;
    let (execution, plan, owner, facts) = register_original(&session, &scope, &mut driver);
    let mut decoder = decoder(&session, execution, plan);
    decoder
        .apply_relay_body(
            &crate::query_execution::write_result::tests::component_write_body(
                commit_fragment_bytes(),
            ),
        )
        .unwrap();
    assert!(
        decoder.finish().is_err(),
        "a real prefix cannot manufacture complete End"
    );
    assert_eq!(session.accumulated_row_count().unwrap(), 0);
    assert_eq!(fixture.recorded.lock().unwrap().finish, 0);
    let outcome = session.abort(request_context()).unwrap();
    assert!(matches!(
        outcome,
        ConnectorWriteAbortOutcome::KnownUncommitted {
            cleanup: novarocks_spi::connector::ExternalMutationFinalization::Failed(_)
        }
    ));
    assert_eq!(owner.pending_records().unwrap(), 2);
    assert!(owner.retains_facts(&facts).unwrap());
    let retired = work.owner.retirement_observer().unwrap();
    drop(session);
    work.business.release();
    work.owner.complete();
    drive(&mut driver);
    assert_eq!(owner.pending_records().unwrap(), 2);
    assert!(owner.retains_facts(&facts).unwrap());
    assert!(!retired.is_reclaimed().unwrap());
}
#[test]
fn complete_actual_end_moves_same_set_then_requires_explicit_publication_disposition() {
    let (control, mut driver) = original_runtime();
    let work = control
        .try_begin_root(WorkRequest::new(WorkClass::Query))
        .unwrap();
    let scope = work.owner.scope();
    bind(&scope, &mut driver);
    let fixture = fixture_with_outcome(1, 1, known_committed());
    let session = fixture.session;
    let (execution, plan, owner, facts) = register_original(&session, &scope, &mut driver);
    let mut decoder = decoder(&session, execution, plan);
    decoder
        .apply_relay_body(
            &crate::query_execution::write_result::tests::component_write_body(
                commit_fragment_bytes(),
            ),
        )
        .unwrap();
    decoder.check_relay_end(2).unwrap();
    decoder.observe_root_eof().unwrap();
    let prepared = decoder.finish().unwrap();
    let decode = session
        .computation_decode(&[WriteTargetOrdinal::try_new(0).unwrap()])
        .unwrap();
    let interpreted = decode.interpret(prepared).unwrap();
    let accepted = session.receiving_port().accept(interpreted).unwrap();
    assert!(accepted.belongs_to(&session));
    assert_eq!(accepted.row_count(), 3);
    assert_eq!(owner.pending_records().unwrap(), 0);
    assert!(owner.retains_facts(&facts).unwrap());
    let retired = work.owner.retirement_observer().unwrap();
    work.business.release();
    work.owner.complete();
    drive(&mut driver);
    assert!(
        !retired.is_reclaimed().unwrap(),
        "the known publication record still owns its charge"
    );
    // Real original scripted Provider invocation; its verdict is stable here
    // before the borrowed disposition/notification seam is consumed.
    let outcome = session.finish_accumulated(request_context()).unwrap();
    assert!(matches!(
        outcome,
        ExternalMutationOutcome::KnownCommitted { .. }
    ));
    let notification = session.settle_received_productions_in().unwrap().unwrap();
    notification.notify();
    drive(&mut driver);
    assert!(!owner.retains_facts(&facts).unwrap());
    assert!(retired.is_reclaimed().unwrap());
}

#[test]
fn terminal_decision_during_paid_registration_keeps_both_original_sources() {
    let (control, mut driver) = original_runtime();
    let work = control
        .try_begin_root(WorkRequest::new(WorkClass::Query))
        .unwrap();
    let scope = work.owner.scope();
    bind(&scope, &mut driver);
    let fixture = fixture_with_outcome(1, 1, known_committed());
    let session = fixture.session;
    let cohorts = TaskManifestBinding::original_writer_occurrences_for_test();
    let execution = cohorts[0].task().query_execution_id();
    let plan = cohorts[0].plan();
    let quote = WriteProductionQuote::for_occurrences(cohorts.len())
        .unwrap()
        .unwrap();
    let origin: Arc<dyn Send + Sync> = session.decode_origin.clone();
    let mut admission = scope
        .begin_unreceived_productions(Arc::downgrade(&origin), quote.quote)
        .unwrap();
    drive(&mut driver);
    let mut ready = receive(&mut admission).unwrap();
    let mut facts = Some(encode_occurrences(cohorts.into_iter(), quote).unwrap());
    let expected = facts.as_ref().unwrap().to_vec();
    let _actual_abort = session.abort(request_context()).unwrap();
    assert!(
        session
            .register_production_ready_in(execution, plan, &mut ready, &mut facts)
            .is_err()
    );
    assert_eq!(facts.as_ref().unwrap().as_ref(), expected);
    assert!(session.productions.lock().unwrap().current.is_none());
    // No writer could have been dispatched: this is a rejected, uninstalled
    // grant, so its original ordinary rollback is still the correct author.
    drop((ready, admission, facts));
    work.business.release();
    work.owner.complete();
    drive(&mut driver);
    assert!(control.snapshot().root_responsibilities == 0);
}
