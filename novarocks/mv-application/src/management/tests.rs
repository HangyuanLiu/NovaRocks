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

use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use novarocks_spi::connector::{
    CatalogHandle, CatalogVersion, ConnectorCancellation, ConnectorCommittedVersion,
    ConnectorControlRuntimeId, ConnectorDocumentManagementObservation,
    ConnectorDocumentManagementOperation, ConnectorDocumentObservationRequest,
    ConnectorDocumentStorageBudget, ConnectorDocumentStorageLimits, ConnectorInstanceId,
    ConnectorManagedObjectMarker, ConnectorProviderBindingKey, ConnectorRequestContext,
    ConnectorTableIdentity, ConnectorTableObjectId, MAX_CONNECTOR_HANDLE_PAYLOAD_BYTES,
    MAX_CONNECTOR_TOTAL_PAYLOAD_BYTES, ProviderBindingEpoch,
};

use super::*;

struct NeverCancelled;

impl ConnectorCancellation for NeverCancelled {
    fn is_cancelled(&self) -> bool {
        false
    }
}

fn owner(value: &str) -> DeploymentOwner {
    DeploymentOwner::parse(value).unwrap()
}

fn incarnation(value: &str) -> ProcessIncarnation {
    ProcessIncarnation::parse(value).unwrap()
}

fn runtime_id(value: u8) -> ConnectorControlRuntimeId {
    ConnectorControlRuntimeId::from_bytes([value; 16])
}

fn table(name: &str) -> ConnectorTableIdentity {
    ConnectorTableIdentity {
        instance_id: ConnectorInstanceId::parse("iceberg").unwrap(),
        namespace: Arc::from("db"),
        table: Arc::from(name),
    }
}

fn catalog(version: u8) -> CatalogHandle {
    CatalogHandle::new(
        ConnectorInstanceId::parse("iceberg").unwrap(),
        CatalogVersion::from_bytes([version; 32]),
    )
}

fn object(value: &'static [u8]) -> ConnectorTableObjectId {
    ConnectorTableObjectId::try_new(Bytes::from_static(value)).unwrap()
}

fn target(name: &str, object_value: &'static [u8]) -> ManagedMvTarget {
    ManagedMvTarget::try_new(catalog(1), table(name), object(object_value)).unwrap()
}

fn request_context() -> ConnectorRequestContext {
    ConnectorRequestContext::try_new(
        Instant::now() + Duration::from_secs(30),
        Arc::new(NeverCancelled),
        MAX_CONNECTOR_HANDLE_PAYLOAD_BYTES,
        MAX_CONNECTOR_TOTAL_PAYLOAD_BYTES,
    )
    .unwrap()
}

fn connector_observation(
    target: &ManagedMvTarget,
    marker_owner: &str,
    marker_incarnation: &str,
    metadata: u8,
) -> ConnectorDocumentManagementObservation {
    let request = ConnectorDocumentObservationRequest::try_new(
        ConnectorProviderBindingKey {
            instance_id: target.table().instance_id.clone(),
            incarnation: ProviderBindingEpoch::from_bytes([7; 16]),
        },
        target.catalog().clone(),
        target.table().clone(),
        target.object_id().clone(),
        ConnectorDocumentStorageBudget::new(ConnectorDocumentStorageLimits::default()),
        request_context(),
    )
    .unwrap();
    ConnectorDocumentManagementObservation::try_new(
        &request,
        ConnectorCommittedVersion::try_new(Bytes::from(vec![metadata]), Some(metadata as i64 + 1))
            .unwrap(),
        ConnectorManagedObjectMarker::try_new(
            "materialized-view",
            marker_owner,
            marker_incarnation,
        )
        .unwrap(),
        vec![],
    )
    .unwrap()
}

fn fresh_observation(
    request_id: [u8; 16],
    target: &ManagedMvTarget,
    marker_owner: &str,
    marker_incarnation: &str,
    metadata: u8,
) -> FreshManagementObservation {
    FreshManagementObservation::for_test(
        ManagementObservationRequestId::from_bytes(request_id),
        target.clone(),
        owner(marker_owner),
        incarnation(marker_incarnation),
        ConnectorCommittedVersion::try_new(Bytes::from(vec![metadata]), Some(metadata as i64 + 1))
            .unwrap(),
    )
}

fn unknown_effect(
    target: &ManagedMvTarget,
    effect_byte: u8,
    old_incarnation: &str,
    scope: EffectScope,
    dispatched_at: u64,
) -> UnsettledEffect {
    match EffectResponsibility::new(
        EffectIdentity::from_bytes([effect_byte; 16]),
        target.clone(),
        incarnation(old_incarnation),
        scope,
        ManagementTimestamp::from_unix_millis(dispatched_at),
    )
    .record_terminal(EffectDisposition::CommitUnknown)
    {
        EffectTerminalFact::CommitUnknown(effect) => effect,
        _ => unreachable!(),
    }
}

fn isolation(target: &ManagedMvTarget, old_incarnation: &str, at: u64) -> IsolationEvidence {
    IsolationEvidence::try_new(
        target.clone(),
        incarnation(old_incarnation),
        ManagementTimestamp::from_unix_millis(at),
        "deployment controller isolated the old writer",
    )
    .unwrap()
}

fn catalog_terminal(
    target: &ManagedMvTarget,
    effect_byte: u8,
    dispatching_incarnation: &str,
    disposition: EffectDisposition,
) -> EffectTerminalFact {
    EffectResponsibility::new(
        EffectIdentity::from_bytes([effect_byte; 16]),
        target.clone(),
        incarnation(dispatching_incarnation),
        EffectScope::CATALOG_COMMIT,
        ManagementTimestamp::from_unix_millis(1_300),
    )
    .record_terminal(disposition)
}

fn ready_observation_state(
    target: ManagedMvTarget,
    local_owner: &str,
    local_incarnation: &str,
) -> ManagementObservationState {
    let mut state = ManagementObservationState::try_new(
        target.clone(),
        owner(local_owner),
        incarnation(local_incarnation),
        ManagementContinuation::SameOwner {
            previous_incarnation: incarnation(local_incarnation),
        },
        vec![],
        None,
        None,
    )
    .unwrap();
    let _pending = state
        .begin_current_observation(ManagementObservationRequestId::from_bytes([1; 16]))
        .unwrap();
    assert_eq!(
        state
            .accept_current_observation(fresh_observation(
                [1; 16],
                &target,
                local_owner,
                local_incarnation,
                1,
            ))
            .unwrap(),
        ManagementObservationPhase::Ready
    );
    state
}

#[test]
fn policy_window_opens_exactly_at_the_deadline_without_reclassifying_unknown() {
    let target = target("mv", b"object-a");
    let effect = unknown_effect(&target, 3, "old", EffectScope::CATALOG_COMMIT, 1_000);
    let isolation = isolation(&target, "old", 1_100);
    let guarantee = RemoteEffectLifetimeGuarantee::try_new(
        EffectScope::CATALOG_COMMIT,
        Duration::from_millis(500),
        Duration::from_millis(50),
        RemoteEffectGuaranteeBasis::ProviderServiceContract,
        "catalog service contract",
    )
    .unwrap();
    let clock = VirtualManagementClock::new(ManagementTimestamp::from_unix_millis(1_649));
    let mut evaluator = ReadmissionEvaluator::default();

    assert!(matches!(
        evaluator.from_policy_window(
            &effect,
            &isolation,
            &guarantee,
            ReadmissionMode::AutomaticWhenGuaranteed,
            &clock,
        ),
        Err(ReadmissionError::WindowNotElapsed { .. })
    ));
    clock.set(ManagementTimestamp::from_unix_millis(1_650));
    let permit = evaluator
        .from_policy_window(
            &effect,
            &isolation,
            &guarantee,
            ReadmissionMode::AutomaticWhenGuaranteed,
            &clock,
        )
        .unwrap();
    assert!(permit.preserves_unknown_disposition());
    assert_eq!(
        effect.original_disposition(),
        EffectDisposition::CommitUnknown
    );
}

#[test]
fn local_timeout_and_idempotency_retention_cannot_mint_a_remote_guarantee() {
    for basis in [
        RemoteEffectGuaranteeBasis::ClientRequestTimeout,
        RemoteEffectGuaranteeBasis::GatewayTimeout,
        RemoteEffectGuaranteeBasis::CredentialExpiry,
        RemoteEffectGuaranteeBasis::IdempotencyRetention,
        RemoteEffectGuaranteeBasis::EmpiricalPercentile,
    ] {
        assert_eq!(
            RemoteEffectLifetimeGuarantee::try_new(
                EffectScope::CATALOG_COMMIT,
                Duration::from_secs(30),
                Duration::ZERO,
                basis,
                "non-authoritative timeout",
            )
            .unwrap_err(),
            ReadmissionError::InvalidGuarantee
        );
    }
}

#[test]
fn deadline_rounds_sub_millisecond_guarantees_up_and_rejects_overflow() {
    let target = target("mv", b"object-a");
    let effect = unknown_effect(&target, 36, "old", EffectScope::CATALOG_COMMIT, 1_000);
    let isolation_evidence = isolation(&target, "old", 1_000);
    let guarantee = RemoteEffectLifetimeGuarantee::try_new(
        EffectScope::CATALOG_COMMIT,
        Duration::from_nanos(1),
        Duration::ZERO,
        RemoteEffectGuaranteeBasis::DeploymentEnforcedBound,
        "deployment network and service bound",
    )
    .unwrap();
    let clock = VirtualManagementClock::new(ManagementTimestamp::from_unix_millis(1_000));
    assert!(matches!(
        ReadmissionEvaluator::default().from_policy_window(
            &effect,
            &isolation_evidence,
            &guarantee,
            ReadmissionMode::AutomaticWhenGuaranteed,
            &clock,
        ),
        Err(ReadmissionError::WindowNotElapsed { .. })
    ));

    let overflow_effect = unknown_effect(&target, 37, "old", EffectScope::CATALOG_COMMIT, u64::MAX);
    let overflow_isolation = isolation(&target, "old", u64::MAX);
    let clock = VirtualManagementClock::new(ManagementTimestamp::from_unix_millis(u64::MAX));
    assert_eq!(
        ReadmissionEvaluator::default()
            .from_policy_window(
                &overflow_effect,
                &overflow_isolation,
                &guarantee,
                ReadmissionMode::AutomaticWhenGuaranteed,
                &clock,
            )
            .unwrap_err(),
        ReadmissionError::Overflow
    );
}

#[test]
fn unsealed_management_observation_is_rejected_before_owner_policy() {
    let target = target("mv", b"object-a");
    let mut state = ManagementObservationState::try_new(
        target.clone(),
        owner("deployment-a"),
        incarnation("new"),
        ManagementContinuation::SameOwner {
            previous_incarnation: incarnation("old"),
        },
        vec![],
        None,
        None,
    )
    .unwrap();
    let pending = state
        .begin_current_observation(ManagementObservationRequestId::from_bytes([38; 16]))
        .unwrap();
    assert_eq!(
        state
            .complete_current_observation(
                pending,
                &connector_observation(&target, "deployment-a", "old", 1),
            )
            .unwrap_err(),
        ManagementObservationError::UnsealedObservation
    );
    assert_eq!(
        state.phase(),
        ManagementObservationPhase::AwaitingFreshObservation
    );
    assert_eq!(
        state
            .begin_current_observation(ManagementObservationRequestId::from_bytes([38; 16]))
            .err()
            .unwrap(),
        ManagementObservationError::ReusedObservationRequest
    );
}

#[test]
fn stale_and_replayed_observations_preserve_the_active_request() {
    let target = target("mv", b"object-a");
    let mut state = ready_observation_state(target.clone(), "deployment-a", "inc-a");
    state
        .begin_current_observation(ManagementObservationRequestId::from_bytes([39; 16]))
        .unwrap();
    assert_eq!(
        state
            .accept_current_observation(fresh_observation(
                [40; 16],
                &target,
                "deployment-a",
                "inc-a",
                2,
            ))
            .unwrap_err(),
        ManagementObservationError::StaleObservation
    );
    assert_eq!(
        state
            .begin_current_observation(ManagementObservationRequestId::from_bytes([41; 16]))
            .err()
            .unwrap(),
        ManagementObservationError::ObservationAlreadyPending
    );
    state
        .accept_current_observation(fresh_observation(
            [39; 16],
            &target,
            "deployment-a",
            "inc-a",
            2,
        ))
        .unwrap();
    assert_eq!(
        state
            .begin_current_observation(ManagementObservationRequestId::from_bytes([39; 16]))
            .err()
            .unwrap(),
        ManagementObservationError::ReusedObservationRequest
    );

    state
        .begin_current_observation(ManagementObservationRequestId::from_bytes([41; 16]))
        .unwrap();
    assert_eq!(
        state
            .begin_current_observation(ManagementObservationRequestId::from_bytes([39; 16]))
            .err()
            .unwrap(),
        ManagementObservationError::ReusedObservationRequest
    );
    assert_eq!(
        state
            .accept_current_observation(fresh_observation(
                [39; 16],
                &target,
                "deployment-a",
                "inc-a",
                2,
            ))
            .unwrap_err(),
        ManagementObservationError::ReusedObservationRequest
    );
    state
        .accept_current_observation(fresh_observation(
            [41; 16],
            &target,
            "deployment-a",
            "inc-a",
            3,
        ))
        .unwrap();
}

#[test]
fn isolation_cutoff_restarts_the_full_policy_window() {
    let target = target("mv", b"object-a");
    let effect = unknown_effect(&target, 4, "old", EffectScope::CATALOG_COMMIT, 1_000);
    let isolation = isolation(&target, "old", 2_000);
    let guarantee = RemoteEffectLifetimeGuarantee::try_new(
        EffectScope::CATALOG_COMMIT,
        Duration::from_millis(500),
        Duration::ZERO,
        RemoteEffectGuaranteeBasis::ProviderServiceContract,
        "catalog service contract",
    )
    .unwrap();
    let clock = VirtualManagementClock::new(ManagementTimestamp::from_unix_millis(2_499));
    let mut evaluator = ReadmissionEvaluator::default();
    assert!(matches!(
        evaluator.from_policy_window(
            &effect,
            &isolation,
            &guarantee,
            ReadmissionMode::AutomaticWhenGuaranteed,
            &clock,
        ),
        Err(ReadmissionError::WindowNotElapsed { .. })
    ));
}

#[test]
fn manual_mode_requires_one_current_exact_challenge_and_real_evidence_text() {
    let target = target("mv", b"object-a");
    let effect = unknown_effect(&target, 5, "old", EffectScope::CATALOG_COMMIT, 1_000);
    let isolation = isolation(&target, "old", 1_100);
    let clock = VirtualManagementClock::new(ManagementTimestamp::from_unix_millis(99_000));
    let guarantee = RemoteEffectLifetimeGuarantee::try_new(
        EffectScope::CATALOG_COMMIT,
        Duration::from_millis(1),
        Duration::ZERO,
        RemoteEffectGuaranteeBasis::ProviderServiceContract,
        "catalog service contract",
    )
    .unwrap();
    let challenge = ReadmissionChallenge::from_bytes([9; 16]);
    let mut evaluator = ReadmissionEvaluator::default();
    assert_eq!(
        evaluator
            .from_policy_window(
                &effect,
                &isolation,
                &guarantee,
                ReadmissionMode::OperatorDeclarationOnly,
                &clock,
            )
            .unwrap_err(),
        ReadmissionError::ManualMode
    );
    evaluator.issue_challenge(challenge).unwrap();
    let declaration = ManualReadmissionDeclaration::try_new(
        challenge,
        effect.responsibility().identity(),
        target,
        incarnation("old"),
        EffectScope::CATALOG_COMMIT,
        "operator@example.com",
        "provider audit confirms the request reached a terminal state",
        ManagementTimestamp::from_unix_millis(1_200),
    )
    .unwrap();
    assert!(
        evaluator
            .from_manual_declaration(&effect, &isolation, &declaration)
            .unwrap()
            .preserves_unknown_disposition()
    );
    assert_eq!(
        evaluator
            .from_manual_declaration(&effect, &isolation, &declaration)
            .unwrap_err(),
        ReadmissionError::MissingChallenge
    );
    assert_eq!(
        evaluator.issue_challenge(challenge).unwrap_err(),
        ReadmissionError::ReusedChallenge
    );
}

#[test]
fn catalog_guarantee_cannot_cover_deletion_or_enable_automatic_gc() {
    let target = target("mv", b"object-a");
    let effect = unknown_effect(&target, 6, "old", EffectScope::OBJECT_DELETION, 1_000);
    let isolation = isolation(&target, "old", 1_100);
    let catalog_only = RemoteEffectLifetimeGuarantee::try_new(
        EffectScope::CATALOG_COMMIT,
        Duration::from_secs(10),
        Duration::from_secs(1),
        RemoteEffectGuaranteeBasis::ProviderServiceContract,
        "catalog service contract",
    )
    .unwrap();
    let clock = VirtualManagementClock::new(ManagementTimestamp::from_unix_millis(99_000));
    assert_eq!(
        ReadmissionEvaluator::default()
            .from_policy_window(
                &effect,
                &isolation,
                &catalog_only,
                ReadmissionMode::AutomaticWhenGuaranteed,
                &clock,
            )
            .unwrap_err(),
        ReadmissionError::GuaranteeScopeMismatch
    );
    let gc = GarbageCollectionSafetyPolicy::new(
        Duration::from_secs(30),
        Some(catalog_only.clone()),
        Some(catalog_only),
        Duration::from_secs(1),
        Duration::from_secs(2),
        Duration::from_secs(3),
    );
    assert_eq!(
        gc.minimum_safe_age().unwrap_err(),
        ReadmissionError::MissingDeleteGuarantee
    );
}

#[test]
fn gc_safe_age_uses_catalog_reference_tail_and_validates_a_separate_delete_tail() {
    let catalog = RemoteEffectLifetimeGuarantee::try_new(
        EffectScope::CATALOG_COMMIT,
        Duration::from_secs(13),
        Duration::from_secs(4),
        RemoteEffectGuaranteeBasis::ProviderServiceContract,
        "catalog commit service contract",
    )
    .unwrap();
    let delete = RemoteEffectLifetimeGuarantee::try_new(
        EffectScope::OBJECT_DELETION,
        Duration::from_secs(11),
        Duration::from_secs(2),
        RemoteEffectGuaranteeBasis::ProviderServiceContract,
        "object store service contract",
    )
    .unwrap();
    let gc = GarbageCollectionSafetyPolicy::new(
        Duration::from_secs(30),
        Some(catalog),
        Some(delete),
        Duration::from_secs(3),
        Duration::from_secs(5),
        Duration::from_secs(7),
    );
    assert_eq!(gc.minimum_safe_age().unwrap(), Duration::from_secs(62));
    assert_eq!(
        gc.deletion_effect_window().unwrap(),
        Duration::from_secs(13)
    );
}

#[test]
fn object_deletion_guarantee_cannot_replace_the_catalog_reference_tail() {
    let delete = RemoteEffectLifetimeGuarantee::try_new(
        EffectScope::OBJECT_DELETION,
        Duration::from_secs(11),
        Duration::from_secs(2),
        RemoteEffectGuaranteeBasis::ProviderServiceContract,
        "object store service contract",
    )
    .unwrap();
    let gc = GarbageCollectionSafetyPolicy::new(
        Duration::from_secs(30),
        Some(delete.clone()),
        Some(delete),
        Duration::from_secs(3),
        Duration::from_secs(5),
        Duration::from_secs(7),
    );
    assert_eq!(
        gc.minimum_safe_age().unwrap_err(),
        ReadmissionError::MissingReferenceGuarantee
    );
}

#[test]
fn clock_regression_permanently_closes_automatic_readmission_for_the_evaluator() {
    let target = target("mv", b"object-a");
    let effect = unknown_effect(&target, 7, "old", EffectScope::CATALOG_COMMIT, 1_000);
    let isolation = isolation(&target, "old", 1_100);
    let guarantee = RemoteEffectLifetimeGuarantee::try_new(
        EffectScope::CATALOG_COMMIT,
        Duration::from_millis(500),
        Duration::ZERO,
        RemoteEffectGuaranteeBasis::ProviderServiceContract,
        "catalog service contract",
    )
    .unwrap();
    let clock = VirtualManagementClock::new(ManagementTimestamp::from_unix_millis(1_200));
    let mut evaluator = ReadmissionEvaluator::default();
    let _ = evaluator.from_policy_window(
        &effect,
        &isolation,
        &guarantee,
        ReadmissionMode::AutomaticWhenGuaranteed,
        &clock,
    );
    clock.set(ManagementTimestamp::from_unix_millis(1_100));
    assert_eq!(
        evaluator
            .from_policy_window(
                &effect,
                &isolation,
                &guarantee,
                ReadmissionMode::AutomaticWhenGuaranteed,
                &clock,
            )
            .unwrap_err(),
        ReadmissionError::ClockRegressed
    );
    clock.set(ManagementTimestamp::from_unix_millis(99_000));
    assert_eq!(
        evaluator
            .from_policy_window(
                &effect,
                &isolation,
                &guarantee,
                ReadmissionMode::AutomaticWhenGuaranteed,
                &clock,
            )
            .unwrap_err(),
        ReadmissionError::ClockRegressed
    );
}

#[test]
fn same_owner_restart_requires_effect_closure_reobservation_and_registration() {
    let target = target("mv", b"object-a");
    let effect = unknown_effect(&target, 8, "old", EffectScope::CATALOG_COMMIT, 1_000);
    let isolation = isolation(&target, "old", 1_100);
    let completion = ActualCompletionEvidence::try_new(
        effect.responsibility().identity(),
        target.clone(),
        EffectScope::CATALOG_COMMIT,
        ManagementTimestamp::from_unix_millis(1_200),
        "catalog audit terminal receipt",
    )
    .unwrap();
    let permit = ReadmissionEvaluator::default()
        .from_actual_completion(&effect, &isolation, &completion)
        .unwrap();
    let mut state = ManagementObservationState::try_new(
        target.clone(),
        owner("deployment-a"),
        incarnation("new"),
        ManagementContinuation::SameOwner {
            previous_incarnation: incarnation("old"),
        },
        vec![effect],
        None,
        None,
    )
    .unwrap();
    assert_eq!(
        state.phase(),
        ManagementObservationPhase::AwaitingEffectClosure
    );
    state.accept_readmission_permit(permit).unwrap();
    let _pending = state
        .begin_current_observation(ManagementObservationRequestId::from_bytes([10; 16]))
        .unwrap();
    assert_eq!(
        state
            .accept_current_observation(fresh_observation(
                [10; 16],
                &target,
                "deployment-a",
                "old",
                2,
            ))
            .unwrap(),
        ManagementObservationPhase::RegistrationRequired(RegistrationRequirement::Incarnation)
    );
    state
        .record_registration_terminal(catalog_terminal(
            &target,
            30,
            "new",
            EffectDisposition::KnownCommitted,
        ))
        .unwrap();
    let _pending = state
        .begin_current_observation(ManagementObservationRequestId::from_bytes([11; 16]))
        .unwrap();
    assert_eq!(
        state
            .accept_current_observation(fresh_observation(
                [11; 16],
                &target,
                "deployment-a",
                "new",
                3,
            ))
            .unwrap(),
        ManagementObservationPhase::Ready
    );
}

#[test]
fn owner_handover_never_opens_before_owner_and_incarnation_are_reobserved() {
    let target = target("mv", b"object-a");
    let mut state = ManagementObservationState::try_new(
        target.clone(),
        owner("deployment-b"),
        incarnation("new-b"),
        ManagementContinuation::OwnerHandover {
            previous_owner: owner("deployment-a"),
            previous_incarnation: incarnation("old-a"),
        },
        vec![],
        None,
        None,
    )
    .unwrap();
    let _pending = state
        .begin_current_observation(ManagementObservationRequestId::from_bytes([12; 16]))
        .unwrap();
    assert_eq!(
        state
            .accept_current_observation(fresh_observation(
                [12; 16],
                &target,
                "deployment-a",
                "old-a",
                1,
            ))
            .unwrap(),
        ManagementObservationPhase::RegistrationRequired(
            RegistrationRequirement::OwnerAndIncarnation
        )
    );
    state
        .record_registration_terminal(catalog_terminal(
            &target,
            31,
            "new-b",
            EffectDisposition::KnownCommitted,
        ))
        .unwrap();
    let _pending = state
        .begin_current_observation(ManagementObservationRequestId::from_bytes([13; 16]))
        .unwrap();
    assert_eq!(
        state
            .accept_current_observation(fresh_observation(
                [13; 16],
                &target,
                "deployment-a",
                "old-a",
                2,
            ))
            .unwrap_err(),
        ManagementObservationError::ForeignOwner
    );
    assert_eq!(
        state.phase(),
        ManagementObservationPhase::ClosedForeignOwner
    );
}

#[test]
fn registration_unknown_returns_to_effect_closure_without_changing_its_result() {
    let target = target("mv", b"object-a");
    let mut state = ManagementObservationState::try_new(
        target.clone(),
        owner("deployment-a"),
        incarnation("new"),
        ManagementContinuation::SameOwner {
            previous_incarnation: incarnation("old"),
        },
        vec![],
        None,
        None,
    )
    .unwrap();
    let _pending = state
        .begin_current_observation(ManagementObservationRequestId::from_bytes([32; 16]))
        .unwrap();
    state
        .accept_current_observation(fresh_observation(
            [32; 16],
            &target,
            "deployment-a",
            "old",
            1,
        ))
        .unwrap();
    let terminal = catalog_terminal(&target, 33, "new", EffectDisposition::CommitUnknown);
    state
        .record_registration_terminal(terminal.clone())
        .unwrap();
    assert_eq!(
        state.phase(),
        ManagementObservationPhase::AwaitingEffectClosure
    );
    assert!(matches!(
        terminal,
        EffectTerminalFact::CommitUnknown(ref effect)
            if effect.original_disposition() == EffectDisposition::CommitUnknown
    ));
    assert!(
        state
            .begin_current_observation(ManagementObservationRequestId::from_bytes([34; 16]))
            .is_err()
    );
}

#[test]
fn process_rebuild_installs_a_barrier_instead_of_defaulting_to_ready() {
    let target = target("mv", b"object-a");
    let barrier = unknown_effect(
        &target,
        35,
        "old",
        EffectScope::CATALOG_AND_OBJECT_DELETION,
        1_000,
    );
    let entrance = ManagementEntrance::new(owner("deployment-a"), incarnation("new"));
    let readmission = entrance
        .begin_recovered_target(
            target.clone(),
            ManagementContinuation::SameOwner {
                previous_incarnation: incarnation("old"),
            },
            barrier,
        )
        .unwrap();
    assert_eq!(
        readmission.phase(),
        ManagementObservationPhase::AwaitingEffectClosure
    );
    let request = ManagementRequest::try_new(
        target.catalog().clone(),
        target.table().clone(),
        Some(target.object_id().clone()),
        ConnectorDocumentManagementOperation::SingleTargetUpdate,
        Some(ManagementDependencySet::new(
            [1; 32],
            [2; 32],
            None,
            runtime_id(1),
        )),
        EffectScope::CATALOG_COMMIT,
    )
    .unwrap();
    assert_eq!(
        entrance.acquire(request, || false).err().unwrap(),
        ManagementAdmissionError::EffectUnsettled
    );
}

#[test]
fn recovered_target_rejects_a_narrow_or_foreign_incarnation_barrier() {
    let target = target("mv", b"object-a");
    let entrance = ManagementEntrance::new(owner("deployment-a"), incarnation("new"));
    let continuation = || ManagementContinuation::SameOwner {
        previous_incarnation: incarnation("old"),
    };

    assert_eq!(
        entrance
            .begin_recovered_target(
                target.clone(),
                continuation(),
                unknown_effect(
                    &target,
                    36,
                    "foreign-old",
                    EffectScope::CATALOG_AND_OBJECT_DELETION,
                    1_000,
                ),
            )
            .err()
            .unwrap(),
        ManagementObservationError::InvalidRecoveryBarrier
    );
    assert_eq!(
        entrance
            .begin_recovered_target(
                target.clone(),
                ManagementContinuation::SameOwner {
                    previous_incarnation: incarnation("new"),
                },
                unknown_effect(
                    &target,
                    37,
                    "new",
                    EffectScope::CATALOG_AND_OBJECT_DELETION,
                    1_000,
                ),
            )
            .err()
            .unwrap(),
        ManagementObservationError::InvalidRecoveryBarrier
    );
    assert_eq!(
        entrance
            .begin_recovered_target(
                target.clone(),
                continuation(),
                unknown_effect(&target, 38, "old", EffectScope::CATALOG_COMMIT, 1_000,),
            )
            .err()
            .unwrap(),
        ManagementObservationError::InvalidRecoveryBarrier
    );

    let recovered = entrance
        .begin_recovered_target(
            target.clone(),
            continuation(),
            unknown_effect(
                &target,
                40,
                "old",
                EffectScope::CATALOG_AND_OBJECT_DELETION,
                1_000,
            ),
        )
        .unwrap();
    assert_eq!(
        recovered.phase(),
        ManagementObservationPhase::AwaitingEffectClosure
    );
}

#[test]
fn abandoned_recovery_observation_can_be_reissued_without_opening_admission() {
    let target = target("mv", b"object-a");
    let entrance = ManagementEntrance::new(owner("deployment-a"), incarnation("new"));
    let continuation = || ManagementContinuation::SameOwner {
        previous_incarnation: incarnation("old"),
    };
    let recovery = entrance
        .begin_recovered_target(
            target.clone(),
            continuation(),
            unknown_effect(
                &target,
                39,
                "old",
                EffectScope::CATALOG_AND_OBJECT_DELETION,
                1_000,
            ),
        )
        .unwrap();
    entrance.abandon_observation(recovery).unwrap();

    match entrance.begin_readmission(
        target.table(),
        ManagementContinuation::SameOwner {
            previous_incarnation: incarnation("different-old"),
        },
    ) {
        Err(error) => assert_eq!(error, ManagementObservationError::InvalidRecoveryBarrier),
        Ok(_) => panic!("a different continuation must not replace the recovery barrier"),
    }

    let replacement = entrance
        .begin_readmission(target.table(), continuation())
        .unwrap();
    assert_eq!(
        replacement.phase(),
        ManagementObservationPhase::AwaitingEffectClosure
    );
    let request = ManagementRequest::try_new(
        target.catalog().clone(),
        target.table().clone(),
        Some(target.object_id().clone()),
        ConnectorDocumentManagementOperation::SingleTargetUpdate,
        Some(ManagementDependencySet::new(
            [1; 32],
            [2; 32],
            None,
            runtime_id(1),
        )),
        EffectScope::CATALOG_COMMIT,
    )
    .unwrap();
    assert_eq!(
        entrance.acquire(request, || false).err().unwrap(),
        ManagementAdmissionError::EffectUnsettled
    );
}

#[test]
fn fresh_incarnation_mismatch_closes_the_installed_entrance_target() {
    let target = target("mv", b"object-a");
    let dependencies = ManagementDependencySet::new([1; 32], [2; 32], None, runtime_id(1));
    let entrance = ManagementEntrance::new(owner("deployment-a"), incarnation("new"));
    let mut state = ready_observation_state(target.clone(), "deployment-a", "new");
    entrance
        .install_observed_target(&state, dependencies.clone())
        .unwrap();
    let request = ManagementRequest::try_new(
        target.catalog().clone(),
        target.table().clone(),
        Some(target.object_id().clone()),
        ConnectorDocumentManagementOperation::SingleTargetUpdate,
        Some(dependencies.clone()),
        EffectScope::CATALOG_COMMIT,
    )
    .unwrap();
    let mut acquired_before_mismatch = entrance.acquire(request.clone(), || false).unwrap();
    let _pending = state
        .begin_current_observation(ManagementObservationRequestId::from_bytes([14; 16]))
        .unwrap();
    assert_eq!(
        state
            .accept_current_observation(fresh_observation(
                [14; 16],
                &target,
                "deployment-a",
                "other",
                2,
            ))
            .unwrap_err(),
        ManagementObservationError::IncarnationMismatch
    );
    assert_eq!(
        state.phase(),
        ManagementObservationPhase::ClosedIncarnationMismatch
    );
    assert_eq!(
        acquired_before_mismatch
            .mark_dispatched(EffectResponsibility::new(
                EffectIdentity::from_bytes([49; 16]),
                target.clone(),
                incarnation("new"),
                EffectScope::CATALOG_COMMIT,
                ManagementTimestamp::from_unix_millis(1_000),
            ))
            .unwrap_err(),
        ManagementAdmissionError::ReadmissionIncomplete
    );
    drop(acquired_before_mismatch);
    assert!(entrance.unsettled_effects(target.table()).is_empty());
    assert_eq!(
        entrance.acquire(request, || false).err().unwrap(),
        ManagementAdmissionError::ReadmissionIncomplete
    );
}

#[test]
fn object_replacement_closes_readmission_instead_of_becoming_a_miss() {
    let original = target("mv", b"object-a");
    let replacement = target("mv", b"object-b");
    let mut state = ManagementObservationState::try_new(
        original,
        owner("deployment-a"),
        incarnation("new"),
        ManagementContinuation::SameOwner {
            previous_incarnation: incarnation("old"),
        },
        vec![],
        None,
        None,
    )
    .unwrap();
    let _pending = state
        .begin_current_observation(ManagementObservationRequestId::from_bytes([15; 16]))
        .unwrap();
    assert!(matches!(
        state.accept_current_observation(fresh_observation(
            [15; 16],
            &replacement,
            "deployment-a",
            "old",
            1,
        )),
        Err(ManagementObservationError::Ownership(
            ManagementOwnershipError::TargetReplaced
        ))
    ));
    assert_eq!(
        state.phase(),
        ManagementObservationPhase::ClosedObjectReplacement
    );
}

#[test]
fn entrance_rechecks_long_computation_dependencies_and_isolates_targets() {
    let target_a = target("mv_a", b"object-a");
    let target_b = target("mv_b", b"object-b");
    let observation_a = ready_observation_state(target_a.clone(), "deployment-a", "inc-a");
    let observation_b = ready_observation_state(target_b.clone(), "deployment-a", "inc-a");
    let dependencies = ManagementDependencySet::new([1; 32], [2; 32], Some([3; 32]), runtime_id(4));
    let entrance = ManagementEntrance::new(owner("deployment-a"), incarnation("inc-a"));
    entrance
        .install_observed_target(&observation_a, dependencies.clone())
        .unwrap();
    entrance
        .install_observed_target(&observation_b, dependencies.clone())
        .unwrap();

    let stale = ManagementRequest::try_new(
        target_a.catalog().clone(),
        target_a.table().clone(),
        Some(target_a.object_id().clone()),
        ConnectorDocumentManagementOperation::Publication,
        Some(ManagementDependencySet::new(
            [9; 32],
            [2; 32],
            Some([3; 32]),
            runtime_id(4),
        )),
        EffectScope::CATALOG_COMMIT,
    )
    .unwrap();
    assert_eq!(
        entrance.acquire(stale, || false).err().unwrap(),
        ManagementAdmissionError::DependencyChanged
    );

    let mut different_runtime_bytes = runtime_id(4).to_bytes();
    different_runtime_bytes[15] = 9;
    let stale_runtime = ManagementRequest::try_new(
        target_a.catalog().clone(),
        target_a.table().clone(),
        Some(target_a.object_id().clone()),
        ConnectorDocumentManagementOperation::Publication,
        Some(ManagementDependencySet::new(
            [1; 32],
            [2; 32],
            Some([3; 32]),
            ConnectorControlRuntimeId::from_bytes(different_runtime_bytes),
        )),
        EffectScope::CATALOG_COMMIT,
    )
    .unwrap();
    assert_eq!(
        entrance.acquire(stale_runtime, || false).err().unwrap(),
        ManagementAdmissionError::DependencyChanged
    );

    let lease_a = entrance
        .acquire(
            ManagementRequest::try_new(
                target_a.catalog().clone(),
                target_a.table().clone(),
                Some(target_a.object_id().clone()),
                ConnectorDocumentManagementOperation::Publication,
                Some(dependencies.clone()),
                EffectScope::CATALOG_COMMIT,
            )
            .unwrap(),
            || false,
        )
        .unwrap();
    let lease_b = entrance
        .acquire(
            ManagementRequest::try_new(
                target_b.catalog().clone(),
                target_b.table().clone(),
                Some(target_b.object_id().clone()),
                ConnectorDocumentManagementOperation::SingleTargetUpdate,
                Some(dependencies),
                EffectScope::CATALOG_COMMIT,
            )
            .unwrap(),
            || false,
        )
        .unwrap();
    drop((lease_a, lease_b));
}

#[test]
fn unknown_or_dropped_after_dispatch_keeps_conflicting_management_closed() {
    let target = target("mv", b"object-a");
    let observation = ready_observation_state(target.clone(), "deployment-a", "inc-a");
    let dependencies = ManagementDependencySet::new([1; 32], [2; 32], Some([3; 32]), runtime_id(4));
    let entrance = ManagementEntrance::new(owner("deployment-a"), incarnation("inc-a"));
    entrance
        .install_observed_target(&observation, dependencies.clone())
        .unwrap();
    let request = || {
        ManagementRequest::try_new(
            target.catalog().clone(),
            target.table().clone(),
            Some(target.object_id().clone()),
            ConnectorDocumentManagementOperation::Publication,
            Some(dependencies.clone()),
            EffectScope::CATALOG_COMMIT,
        )
        .unwrap()
    };
    let mut lease = entrance.acquire(request(), || false).unwrap();
    lease
        .mark_dispatched(EffectResponsibility::new(
            EffectIdentity::from_bytes([20; 16]),
            target.clone(),
            incarnation("inc-a"),
            EffectScope::CATALOG_COMMIT,
            ManagementTimestamp::from_unix_millis(1_000),
        ))
        .unwrap();
    drop(lease);

    let unsettled = entrance.unsettled_effects(target.table());
    assert_eq!(unsettled.len(), 1);
    assert_eq!(
        entrance.acquire(request(), || false).err().unwrap(),
        ManagementAdmissionError::EffectUnsettled
    );

    let effect = &unsettled[0];
    let completion = ActualCompletionEvidence::try_new(
        effect.responsibility().identity(),
        target.clone(),
        EffectScope::CATALOG_COMMIT,
        ManagementTimestamp::from_unix_millis(1_200),
        "provider audit terminal receipt",
    )
    .unwrap();
    let permit = ReadmissionEvaluator::default()
        .from_actual_completion(effect, &isolation(&target, "inc-a", 1_100), &completion)
        .unwrap();
    let mut readmission = entrance
        .begin_readmission(
            target.table(),
            ManagementContinuation::SameOwner {
                previous_incarnation: incarnation("inc-a"),
            },
        )
        .unwrap();
    readmission.accept_readmission_permit(permit).unwrap();
    let _pending = readmission
        .begin_current_observation(ManagementObservationRequestId::from_bytes([21; 16]))
        .unwrap();
    readmission
        .accept_current_observation(fresh_observation(
            [21; 16],
            &target,
            "deployment-a",
            "inc-a",
            5,
        ))
        .unwrap();
    entrance
        .install_observed_target(&readmission, dependencies.clone())
        .unwrap();
    assert!(entrance.acquire(request(), || false).is_ok());
    assert_eq!(
        effect.original_disposition(),
        EffectDisposition::CommitUnknown
    );
}

#[test]
fn fabricated_empty_readmission_cannot_clear_an_unknown_effect() {
    let target = target("mv", b"object-a");
    let observation = ready_observation_state(target.clone(), "deployment-a", "inc-a");
    let dependencies = ManagementDependencySet::new([1; 32], [2; 32], None, runtime_id(4));
    let entrance = ManagementEntrance::new(owner("deployment-a"), incarnation("inc-a"));
    entrance
        .install_observed_target(&observation, dependencies.clone())
        .unwrap();
    let request = ManagementRequest::try_new(
        target.catalog().clone(),
        target.table().clone(),
        Some(target.object_id().clone()),
        ConnectorDocumentManagementOperation::Publication,
        Some(dependencies.clone()),
        EffectScope::CATALOG_COMMIT,
    )
    .unwrap();
    let mut lease = entrance.acquire(request, || false).unwrap();
    lease
        .mark_dispatched(EffectResponsibility::new(
            EffectIdentity::from_bytes([22; 16]),
            target.clone(),
            incarnation("inc-a"),
            EffectScope::CATALOG_COMMIT,
            ManagementTimestamp::from_unix_millis(1_000),
        ))
        .unwrap();
    drop(lease);

    let fabricated = ready_observation_state(target, "deployment-a", "inc-a");
    assert_eq!(
        entrance
            .install_observed_target(&fabricated, dependencies)
            .unwrap_err(),
        ManagementAdmissionError::ReadmissionIncomplete
    );
}

#[test]
fn dispatched_effect_requires_exact_scope_and_the_current_incarnation() {
    let target = target("mv", b"object-a");
    let observation = ready_observation_state(target.clone(), "deployment-a", "inc-a");
    let dependencies = ManagementDependencySet::new([1; 32], [2; 32], None, runtime_id(4));
    let entrance = ManagementEntrance::new(owner("deployment-a"), incarnation("inc-a"));
    entrance
        .install_observed_target(&observation, dependencies.clone())
        .unwrap();
    let request = ManagementRequest::try_new(
        target.catalog().clone(),
        target.table().clone(),
        Some(target.object_id().clone()),
        ConnectorDocumentManagementOperation::Publication,
        Some(dependencies),
        EffectScope::CATALOG_AND_OBJECT_DELETION,
    )
    .unwrap();
    let mut lease = entrance.acquire(request, || false).unwrap();
    assert_eq!(
        lease
            .mark_dispatched(EffectResponsibility::new(
                EffectIdentity::from_bytes([42; 16]),
                target.clone(),
                incarnation("foreign-incarnation"),
                EffectScope::CATALOG_AND_OBJECT_DELETION,
                ManagementTimestamp::from_unix_millis(1_000),
            ))
            .unwrap_err(),
        ManagementAdmissionError::InvalidEffect
    );
    assert_eq!(
        lease
            .mark_dispatched(EffectResponsibility::new(
                EffectIdentity::from_bytes([43; 16]),
                target.clone(),
                incarnation("inc-a"),
                EffectScope::CATALOG_COMMIT,
                ManagementTimestamp::from_unix_millis(1_000),
            ))
            .unwrap_err(),
        ManagementAdmissionError::InvalidEffect
    );
    lease
        .mark_dispatched(EffectResponsibility::new(
            EffectIdentity::from_bytes([44; 16]),
            target,
            incarnation("inc-a"),
            EffectScope::CATALOG_AND_OBJECT_DELETION,
            ManagementTimestamp::from_unix_millis(1_000),
        ))
        .unwrap();
    lease
        .record_terminal(EffectDisposition::KnownUncommitted)
        .unwrap();
}

#[test]
fn committed_update_stays_closed_until_exact_observation_installs_new_dependencies() {
    let target = target("mv", b"object-a");
    let observation = ready_observation_state(target.clone(), "deployment-a", "inc-a");
    let old_dependencies = ManagementDependencySet::new([1; 32], [2; 32], None, runtime_id(4));
    let new_dependencies = ManagementDependencySet::new([5; 32], [6; 32], None, runtime_id(7));
    let entrance = ManagementEntrance::new(owner("deployment-a"), incarnation("inc-a"));
    entrance
        .install_observed_target(&observation, old_dependencies.clone())
        .unwrap();
    let request = |dependencies| {
        ManagementRequest::try_new(
            target.catalog().clone(),
            target.table().clone(),
            Some(target.object_id().clone()),
            ConnectorDocumentManagementOperation::SingleTargetUpdate,
            Some(dependencies),
            EffectScope::CATALOG_COMMIT,
        )
        .unwrap()
    };
    let mut lease = entrance
        .acquire(request(old_dependencies.clone()), || false)
        .unwrap();
    lease
        .mark_dispatched(EffectResponsibility::new(
            EffectIdentity::from_bytes([45; 16]),
            target.clone(),
            incarnation("inc-a"),
            EffectScope::CATALOG_COMMIT,
            ManagementTimestamp::from_unix_millis(1_000),
        ))
        .unwrap();
    lease
        .record_terminal(EffectDisposition::KnownCommitted)
        .unwrap();
    assert_eq!(
        entrance
            .acquire(request(old_dependencies.clone()), || false)
            .err()
            .unwrap(),
        ManagementAdmissionError::ReadmissionIncomplete
    );

    let fabricated = ready_observation_state(target.clone(), "deployment-a", "inc-a");
    assert_eq!(
        entrance
            .install_observed_target(&fabricated, new_dependencies.clone())
            .unwrap_err(),
        ManagementAdmissionError::ReadmissionIncomplete
    );
    let mut convergence = entrance
        .begin_committed_convergence(
            target.table(),
            ManagementContinuation::SameOwner {
                previous_incarnation: incarnation("inc-a"),
            },
        )
        .unwrap();
    assert_eq!(
        entrance
            .begin_committed_convergence(
                target.table(),
                ManagementContinuation::SameOwner {
                    previous_incarnation: incarnation("inc-a"),
                },
            )
            .err()
            .unwrap(),
        ManagementObservationError::ObservationAlreadyPending
    );
    convergence
        .begin_current_observation(ManagementObservationRequestId::from_bytes([46; 16]))
        .unwrap();
    convergence
        .accept_current_observation(fresh_observation(
            [46; 16],
            &target,
            "deployment-a",
            "inc-a",
            2,
        ))
        .unwrap();
    entrance
        .install_observed_target(&convergence, new_dependencies.clone())
        .unwrap();
    assert_eq!(
        entrance
            .install_observed_target(&convergence, new_dependencies.clone())
            .unwrap_err(),
        ManagementAdmissionError::ReadmissionIncomplete
    );
    assert_eq!(
        entrance
            .acquire(request(old_dependencies), || false)
            .err()
            .unwrap(),
        ManagementAdmissionError::DependencyChanged
    );
    assert!(
        entrance
            .acquire(request(new_dependencies), || false)
            .is_ok()
    );
}

#[test]
fn abandoned_committed_convergence_preserves_exact_reissue_requirement() {
    let target = target("mv", b"object-a");
    let dependencies = ManagementDependencySet::new([1; 32], [2; 32], None, runtime_id(4));
    let observation = ready_observation_state(target.clone(), "deployment-a", "inc-a");
    let entrance = ManagementEntrance::new(owner("deployment-a"), incarnation("inc-a"));
    entrance
        .install_observed_target(&observation, dependencies.clone())
        .unwrap();
    let request = || {
        ManagementRequest::try_new(
            target.catalog().clone(),
            target.table().clone(),
            Some(target.object_id().clone()),
            ConnectorDocumentManagementOperation::SingleTargetUpdate,
            Some(dependencies.clone()),
            EffectScope::CATALOG_COMMIT,
        )
        .unwrap()
    };
    let mut lease = entrance.acquire(request(), || false).unwrap();
    lease
        .mark_dispatched(EffectResponsibility::new(
            EffectIdentity::from_bytes([50; 16]),
            target.clone(),
            incarnation("inc-a"),
            EffectScope::CATALOG_COMMIT,
            ManagementTimestamp::from_unix_millis(1_000),
        ))
        .unwrap();
    lease
        .record_terminal(EffectDisposition::KnownCommitted)
        .unwrap();

    let convergence = entrance
        .begin_committed_convergence(
            target.table(),
            ManagementContinuation::SameOwner {
                previous_incarnation: incarnation("inc-a"),
            },
        )
        .unwrap();
    entrance.abandon_observation(convergence).unwrap();
    match entrance.begin_committed_convergence(
        target.table(),
        ManagementContinuation::SameOwner {
            previous_incarnation: incarnation("different-incarnation"),
        },
    ) {
        Err(error) => assert_eq!(error, ManagementObservationError::InvalidRecoveryBarrier),
        Ok(_) => panic!("a different continuation must not replace committed convergence"),
    }
    match entrance.acquire(request(), || false) {
        Err(error) => assert_eq!(error, ManagementAdmissionError::ReadmissionIncomplete),
        Ok(_) => panic!("abandoning convergence must retain the committed effect barrier"),
    }

    let mut replacement = entrance
        .begin_committed_convergence(
            target.table(),
            ManagementContinuation::SameOwner {
                previous_incarnation: incarnation("inc-a"),
            },
        )
        .unwrap();
    replacement
        .begin_current_observation(ManagementObservationRequestId::from_bytes([51; 16]))
        .unwrap();
    replacement
        .accept_current_observation(fresh_observation(
            [51; 16],
            &target,
            "deployment-a",
            "inc-a",
            3,
        ))
        .unwrap();
    entrance
        .install_observed_target(&replacement, dependencies.clone())
        .unwrap();
    assert!(entrance.acquire(request(), || false).is_ok());
}

#[test]
fn committed_create_reserves_the_target_until_observation_converges() {
    let target = target("created_mv", b"created-object");
    let dependencies = ManagementDependencySet::new([1; 32], [2; 32], None, runtime_id(4));
    let entrance = ManagementEntrance::new(owner("deployment-a"), incarnation("inc-a"));
    let create = |identity| {
        ManagementRequest::for_create_intent(
            CreateIntent::try_new(target.catalog().clone(), target.table().clone(), identity)
                .unwrap(),
            EffectScope::CATALOG_COMMIT,
        )
    };
    let mut lease = entrance
        .acquire(create(EffectIdentity::from_bytes([47; 16])), || false)
        .unwrap();
    lease
        .mark_create_intent_dispatched(ManagementTimestamp::from_unix_millis(1_000))
        .unwrap();
    lease.late_bind_create_target(target.clone()).unwrap();
    lease
        .record_terminal(EffectDisposition::KnownCommitted)
        .unwrap();
    assert_eq!(
        entrance
            .acquire(create(EffectIdentity::from_bytes([48; 16])), || false)
            .err()
            .unwrap(),
        ManagementAdmissionError::TargetAlreadyExists
    );

    let mut convergence = entrance
        .begin_committed_convergence(
            target.table(),
            ManagementContinuation::SameOwner {
                previous_incarnation: incarnation("inc-a"),
            },
        )
        .unwrap();
    convergence
        .begin_current_observation(ManagementObservationRequestId::from_bytes([48; 16]))
        .unwrap();
    convergence
        .accept_current_observation(fresh_observation(
            [48; 16],
            &target,
            "deployment-a",
            "inc-a",
            1,
        ))
        .unwrap();
    entrance
        .install_observed_target(&convergence, dependencies.clone())
        .unwrap();
    assert!(
        entrance
            .acquire(
                ManagementRequest::try_new(
                    target.catalog().clone(),
                    target.table().clone(),
                    Some(target.object_id().clone()),
                    ConnectorDocumentManagementOperation::SingleTargetUpdate,
                    Some(dependencies),
                    EffectScope::CATALOG_COMMIT,
                )
                .unwrap(),
                || false,
            )
            .is_ok()
    );
}

#[test]
fn create_known_uncommitted_can_retry_but_create_unknown_cannot() {
    let exact_target = target("new_mv", b"created-object");
    let entrance = ManagementEntrance::new(owner("deployment-a"), incarnation("inc-a"));
    let create = |identity| {
        ManagementRequest::for_create_intent(
            CreateIntent::try_new(
                exact_target.catalog().clone(),
                exact_target.table().clone(),
                identity,
            )
            .unwrap(),
            EffectScope::CATALOG_COMMIT,
        )
    };
    let mut first = entrance
        .acquire(create(EffectIdentity::from_bytes([23; 16])), || false)
        .unwrap();
    first
        .mark_create_intent_dispatched(ManagementTimestamp::from_unix_millis(1_000))
        .unwrap();
    first.late_bind_create_target(exact_target.clone()).unwrap();
    first
        .record_terminal(EffectDisposition::KnownUncommitted)
        .unwrap();
    let mut second = entrance
        .acquire(create(EffectIdentity::from_bytes([24; 16])), || false)
        .unwrap();
    second
        .mark_create_intent_dispatched(ManagementTimestamp::from_unix_millis(2_000))
        .unwrap();
    second
        .late_bind_create_target(exact_target.clone())
        .unwrap();
    second
        .record_terminal(EffectDisposition::CommitUnknown)
        .unwrap();
    assert_eq!(
        entrance
            .acquire(create(EffectIdentity::from_bytes([25; 16])), || false)
            .err()
            .unwrap(),
        ManagementAdmissionError::EffectUnsettled
    );
    assert_eq!(
        entrance
            .begin_readmission(
                exact_target.table(),
                ManagementContinuation::SameOwner {
                    previous_incarnation: incarnation("inc-a"),
                },
            )
            .unwrap()
            .phase(),
        ManagementObservationPhase::AwaitingEffectClosure
    );
}

#[test]
fn create_intent_marks_before_stage_binds_once_and_blocks_a_lost_response() {
    let logical = table("created_mv");
    let exact = target("created_mv", b"created-object");
    let mismatch = target("other_mv", b"created-object");
    let entrance = ManagementEntrance::new(owner("deployment-a"), incarnation("inc-a"));
    let lost_intent = CreateIntent::try_new(
        catalog(1),
        logical.clone(),
        EffectIdentity::from_bytes([71; 16]),
    )
    .unwrap();
    let request = |identity| {
        ManagementRequest::for_create_intent(
            CreateIntent::try_new(catalog(1), logical.clone(), identity).unwrap(),
            EffectScope::CATALOG_COMMIT,
        )
    };

    let mut lease = entrance
        .acquire(
            ManagementRequest::for_create_intent(lost_intent.clone(), EffectScope::CATALOG_COMMIT),
            || false,
        )
        .unwrap();
    assert_eq!(
        lease
            .mark_dispatched(EffectResponsibility::new(
                EffectIdentity::from_bytes([71; 16]),
                exact.clone(),
                incarnation("inc-a"),
                EffectScope::CATALOG_COMMIT,
                ManagementTimestamp::from_unix_millis(1_000),
            ))
            .unwrap_err(),
        ManagementAdmissionError::InvalidEffect
    );
    lease
        .mark_create_intent_dispatched(ManagementTimestamp::from_unix_millis(1_000))
        .unwrap();
    assert_eq!(
        lease.late_bind_create_target(mismatch).unwrap_err(),
        ManagementAdmissionError::TargetReplaced
    );
    drop(lease);

    // The staged response was lost before an exact target could be bound. The
    // old intent remains an Unknown barrier rather than being treated as an
    // absent table or retried with another caller-generated UUID.
    assert_eq!(
        entrance
            .acquire(request(EffectIdentity::from_bytes([72; 16])), || false)
            .err()
            .unwrap(),
        ManagementAdmissionError::EffectUnsettled
    );

    assert_eq!(
        entrance
            .begin_unbound_create_observation(&lost_intent)
            .unwrap()
            .complete(&connector_observation(&exact, "deployment-a", "inc-a", 1))
            .err()
            .unwrap(),
        ManagementObservationError::UnsealedObservation
    );
    let fresh = FreshCreateIntentObservation::for_test(lost_intent.clone(), exact.clone());
    assert_eq!(fresh.target(), &exact);
    assert_eq!(
        entrance.resolve_unbound_create_as_unknown(fresh).unwrap(),
        exact
    );
    assert_eq!(entrance.unsettled_effects(exact.table()).len(), 1);
    assert_eq!(
        entrance
            .begin_readmission(
                exact.table(),
                ManagementContinuation::SameOwner {
                    previous_incarnation: incarnation("inc-a"),
                },
            )
            .unwrap()
            .phase(),
        ManagementObservationPhase::AwaitingEffectClosure
    );
}

#[test]
fn automatic_actions_are_single_frozen_effects_and_require_convergence() {
    let target = target("mv", b"object-a");
    let dependencies = ManagementDependencySet::new([1; 32], [2; 32], None, runtime_id(4));
    let entrance = ManagementEntrance::new(owner("deployment-a"), incarnation("inc-a"));
    let observation = ready_observation_state(target.clone(), "deployment-a", "inc-a");
    entrance
        .install_observed_target(&observation, dependencies.clone())
        .unwrap();
    let action = |identity| {
        AutomaticMaintenanceEffect::new(
            target.clone(),
            dependencies.clone(),
            identity,
            EffectScope::CATALOG_COMMIT,
        )
    };

    let mut first = entrance
        .acquire_automatic_maintenance(action(EffectIdentity::from_bytes([81; 16])), || false)
        .unwrap();
    assert_eq!(
        first
            .mark_dispatched(EffectResponsibility::new(
                EffectIdentity::from_bytes([82; 16]),
                target.clone(),
                incarnation("inc-a"),
                EffectScope::CATALOG_COMMIT,
                ManagementTimestamp::from_unix_millis(1_000),
            ))
            .unwrap_err(),
        ManagementAdmissionError::InvalidEffect
    );
    first
        .mark_dispatched(EffectResponsibility::new(
            EffectIdentity::from_bytes([81; 16]),
            target.clone(),
            incarnation("inc-a"),
            EffectScope::CATALOG_COMMIT,
            ManagementTimestamp::from_unix_millis(1_000),
        ))
        .unwrap();
    first
        .record_terminal(EffectDisposition::KnownCommitted)
        .unwrap();

    // A next action needs a fresh installation; it cannot share the completed
    // action's lease or stale management observation.
    assert_eq!(
        entrance
            .acquire_automatic_maintenance(action(EffectIdentity::from_bytes([83; 16])), || false)
            .err()
            .unwrap(),
        ManagementAdmissionError::ReadmissionIncomplete
    );
    let mut convergence = entrance
        .begin_committed_convergence(
            target.table(),
            ManagementContinuation::SameOwner {
                previous_incarnation: incarnation("inc-a"),
            },
        )
        .unwrap();
    convergence
        .begin_current_observation(ManagementObservationRequestId::from_bytes([84; 16]))
        .unwrap();
    convergence
        .accept_current_observation(fresh_observation(
            [84; 16],
            &target,
            "deployment-a",
            "inc-a",
            2,
        ))
        .unwrap();
    entrance
        .install_observed_target(&convergence, dependencies.clone())
        .unwrap();
    assert!(
        entrance
            .acquire_automatic_maintenance(action(EffectIdentity::from_bytes([83; 16])), || false)
            .is_ok()
    );
}

#[test]
fn same_target_wait_honors_work_scope_cancellation() {
    let target = target("mv", b"object-a");
    let observation = ready_observation_state(target.clone(), "deployment-a", "inc-a");
    let dependencies = ManagementDependencySet::new([1; 32], [2; 32], None, runtime_id(4));
    let entrance = ManagementEntrance::new(owner("deployment-a"), incarnation("inc-a"));
    entrance
        .install_observed_target(&observation, dependencies.clone())
        .unwrap();
    let request = || {
        ManagementRequest::try_new(
            target.catalog().clone(),
            target.table().clone(),
            Some(target.object_id().clone()),
            ConnectorDocumentManagementOperation::SingleTargetUpdate,
            Some(dependencies.clone()),
            EffectScope::CATALOG_COMMIT,
        )
        .unwrap()
    };
    let first = entrance.acquire(request(), || false).unwrap();
    assert_eq!(
        entrance.acquire(request(), || true).err().unwrap(),
        ManagementAdmissionError::Cancelled
    );
    drop(first);
}

#[test]
fn management_ticket_preserves_worker_owner_and_nonblocking_fifo() {
    let target = target("mv", b"object-a");
    let observation = ready_observation_state(target.clone(), "deployment-a", "inc-a");
    let dependencies = ManagementDependencySet::new([1; 32], [2; 32], None, runtime_id(4));
    let entrance = ManagementEntrance::new(owner("deployment-a"), incarnation("inc-a"));
    entrance
        .install_observed_target(&observation, dependencies.clone())
        .unwrap();
    let request = || {
        ManagementRequest::try_new(
            target.catalog().clone(),
            target.table().clone(),
            Some(target.object_id().clone()),
            ConnectorDocumentManagementOperation::Publication,
            Some(dependencies.clone()),
            EffectScope::CATALOG_COMMIT,
        )
        .unwrap()
    };

    let mut scheduled = entrance
        .request(
            request(),
            crate::activity::MvActivityOwner::ScheduledRefresh,
        )
        .unwrap();
    let scheduled = scheduled
        .try_acquire()
        .unwrap()
        .expect("first worker ticket acquires without blocking");
    assert!(scheduled.worker_cancellation().is_some());

    let mut manual = entrance
        .request(request(), crate::activity::MvActivityOwner::ManualRefresh)
        .unwrap();
    assert!(manual.try_acquire().unwrap().is_none());
    drop(scheduled);

    let manual = manual
        .try_acquire()
        .unwrap()
        .expect("next ticket acquires after the worker releases the target");
    assert!(manual.worker_cancellation().is_none());
}

#[test]
fn stopping_cancels_worker_acquired_through_management_entrance() {
    let target = target("mv", b"object-a");
    let observation = ready_observation_state(target.clone(), "deployment-a", "inc-a");
    let dependencies = ManagementDependencySet::new([1; 32], [2; 32], None, runtime_id(4));
    let entrance = ManagementEntrance::new(owner("deployment-a"), incarnation("inc-a"));
    entrance
        .install_observed_target(&observation, dependencies.clone())
        .unwrap();
    let request = ManagementRequest::try_new(
        target.catalog().clone(),
        target.table().clone(),
        Some(target.object_id().clone()),
        ConnectorDocumentManagementOperation::SingleTargetUpdate,
        Some(dependencies),
        EffectScope::CATALOG_COMMIT,
    )
    .unwrap();
    let mut ticket = entrance
        .request(
            request,
            crate::activity::MvActivityOwner::AutomaticMaintenance,
        )
        .unwrap();
    let lease = ticket.try_acquire().unwrap().expect("worker lease");
    let cancellation = lease.worker_cancellation().expect("worker cancellation");

    entrance.begin_stopping();

    assert!(cancellation.is_cancelled());
}

fn installed_entrance(
    target: &ManagedMvTarget,
    dependencies: &ManagementDependencySet,
) -> ManagementEntrance {
    let observation = ready_observation_state(target.clone(), "deployment-a", "inc-a");
    let entrance = ManagementEntrance::new(owner("deployment-a"), incarnation("inc-a"));
    entrance
        .install_observed_target(&observation, dependencies.clone())
        .unwrap();
    entrance
}

fn unsettle(
    entrance: &ManagementEntrance,
    target: &ManagedMvTarget,
    dependencies: &ManagementDependencySet,
) {
    let mut lease = entrance
        .acquire(
            ManagementRequest::try_new(
                target.catalog().clone(),
                target.table().clone(),
                Some(target.object_id().clone()),
                ConnectorDocumentManagementOperation::Publication,
                Some(dependencies.clone()),
                EffectScope::CATALOG_COMMIT,
            )
            .unwrap(),
            || false,
        )
        .unwrap();
    lease
        .mark_dispatched(EffectResponsibility::new(
            EffectIdentity::from_bytes([20; 16]),
            target.clone(),
            incarnation("inc-a"),
            EffectScope::CATALOG_COMMIT,
            ManagementTimestamp::from_unix_millis(1_000),
        ))
        .unwrap();
    drop(lease);
}

fn catalog_guarantee() -> RemoteEffectLifetimeGuarantee {
    RemoteEffectLifetimeGuarantee::try_new(
        EffectScope::CATALOG_COMMIT,
        Duration::from_secs(60),
        Duration::from_secs(5),
        RemoteEffectGuaranteeBasis::ProviderServiceContract,
        "iceberg rest service contract",
    )
    .unwrap()
}

#[test]
fn a_manageable_target_offers_a_handover_and_asks_for_no_evidence() {
    let target = target("mv", b"object-a");
    let dependencies = ManagementDependencySet::new([1; 32], [2; 32], Some([3; 32]), runtime_id(4));
    let service = ManagementContinuationService::new(
        installed_entrance(&target, &dependencies),
        RemoteEffectPolicy::default(),
    );

    let status = service
        .status(
            target.table(),
            Some(target.object_id().clone()),
            ReadmissionChallenge::from_bytes([7; 16]),
        )
        .unwrap();

    assert_eq!(status.phase, MvManagementPhase::Manageable);
    assert!(status.unsettled.is_empty());
    assert!(
        status.handover_available,
        "a settled target is exactly the one whose owner may be handed over"
    );
    assert_eq!(
        status.challenge,
        Some(ReadmissionChallenge::from_bytes([7; 16])),
        "the handover statement has to quote a challenge, so one is issued"
    );
    assert_eq!(
        status.required_evidence, None,
        "a handover asserts nothing about a writer elsewhere"
    );
    assert_eq!(status.local_owner.as_str(), "deployment-a");
}

#[test]
fn a_target_with_a_write_in_flight_offers_no_handover_and_no_challenge() {
    let target = target("mv", b"object-a");
    let dependencies = ManagementDependencySet::new([1; 32], [2; 32], Some([3; 32]), runtime_id(4));
    let entrance = installed_entrance(&target, &dependencies);
    let _lease = entrance
        .acquire(
            ManagementRequest::try_new(
                target.catalog().clone(),
                target.table().clone(),
                Some(target.object_id().clone()),
                ConnectorDocumentManagementOperation::SingleTargetUpdate,
                Some(dependencies.clone()),
                EffectScope::CATALOG_COMMIT,
            )
            .unwrap(),
            || false,
        )
        .expect("an installed target admits one write");
    let service = ManagementContinuationService::new(entrance, RemoteEffectPolicy::default());

    let status = service
        .status(
            target.table(),
            Some(target.object_id().clone()),
            ReadmissionChallenge::from_bytes([8; 16]),
        )
        .unwrap();

    assert_eq!(status.phase, MvManagementPhase::Managing);
    assert!(
        !status.handover_available,
        "a target another statement is writing cannot be handed away underneath it"
    );
    assert_eq!(
        status.challenge, None,
        "a challenge never suggests an action that does not exist"
    );
}

#[test]
fn an_unsettled_target_asks_for_the_evidence_its_policy_actually_needs() {
    let target = target("mv", b"object-a");
    let dependencies = ManagementDependencySet::new([1; 32], [2; 32], Some([3; 32]), runtime_id(4));
    let entrance = installed_entrance(&target, &dependencies);
    unsettle(&entrance, &target, &dependencies);
    let service = ManagementContinuationService::new(entrance, RemoteEffectPolicy::default());

    let status = service
        .status(
            target.table(),
            Some(target.object_id().clone()),
            ReadmissionChallenge::from_bytes([7; 16]),
        )
        .unwrap();

    assert_eq!(
        status.phase,
        MvManagementPhase::AwaitingEffectSettlement { unsettled: 1 }
    );
    assert_eq!(status.unsettled.len(), 1);
    assert_eq!(
        status.unsettled[0].mode,
        ReadmissionMode::OperatorDeclarationOnly,
        "no configured guarantee means only an operator can continue this"
    );
    assert_eq!(
        status.challenge,
        Some(ReadmissionChallenge::from_bytes([7; 16]))
    );
    let required = status.required_evidence.expect("operator evidence named");
    assert!(required.contains("isolated"), "{required}");
}

#[test]
fn a_guaranteed_path_waits_for_its_window_instead_of_an_operator() {
    let target = target("mv", b"object-a");
    let dependencies = ManagementDependencySet::new([1; 32], [2; 32], Some([3; 32]), runtime_id(4));
    let entrance = installed_entrance(&target, &dependencies);
    unsettle(&entrance, &target, &dependencies);
    let service = ManagementContinuationService::new(
        entrance,
        RemoteEffectPolicy::try_new(Some(catalog_guarantee()), None).unwrap(),
    );

    let status = service
        .status(
            target.table(),
            Some(target.object_id().clone()),
            ReadmissionChallenge::from_bytes([8; 16]),
        )
        .unwrap();

    assert_eq!(
        status.unsettled[0].mode,
        ReadmissionMode::AutomaticWhenGuaranteed
    );
    let required = status.required_evidence.expect("window evidence named");
    assert!(required.contains("elapsed"), "{required}");
}

#[test]
fn a_catalog_guarantee_cannot_be_declared_for_object_deletion() {
    assert_eq!(
        RemoteEffectPolicy::try_new(None, Some(catalog_guarantee())).unwrap_err(),
        ReadmissionError::GuaranteeScopeMismatch
    );
}

#[test]
fn an_effect_spanning_both_paths_needs_both_guarantees() {
    let both = RemoteEffectPolicy::try_new(Some(catalog_guarantee()), None).unwrap();
    assert!(
        both.guarantee_for(EffectScope::CATALOG_AND_OBJECT_DELETION)
            .is_none()
    );
    assert_eq!(
        both.mode_for(EffectScope::CATALOG_AND_OBJECT_DELETION),
        ReadmissionMode::OperatorDeclarationOnly
    );
    assert!(both.guarantee_for(EffectScope::CATALOG_COMMIT).is_some());
}

#[test]
fn a_challenge_is_issued_once_and_never_reissued() {
    let target = target("mv", b"object-a");
    let dependencies = ManagementDependencySet::new([1; 32], [2; 32], Some([3; 32]), runtime_id(4));
    let entrance = installed_entrance(&target, &dependencies);
    unsettle(&entrance, &target, &dependencies);
    let service = ManagementContinuationService::new(entrance, RemoteEffectPolicy::default());
    let challenge = ReadmissionChallenge::from_bytes([9; 16]);

    service
        .status(target.table(), None, challenge)
        .expect("first status issues the challenge");

    assert_eq!(
        service.status(target.table(), None, challenge).unwrap_err(),
        ReadmissionError::ReusedChallenge
    );
}

#[test]
fn an_unguaranteed_effect_refuses_a_window_resume_rather_than_timing_out() {
    let target = target("mv", b"object-a");
    let effect = unknown_effect(&target, 21, "inc-a", EffectScope::CATALOG_COMMIT, 1_000);
    let dependencies = ManagementDependencySet::new([1; 32], [2; 32], Some([3; 32]), runtime_id(4));
    let service = ManagementContinuationService::new(
        installed_entrance(&target, &dependencies),
        RemoteEffectPolicy::default(),
    );

    assert_eq!(
        service
            .resume_on_policy_window(
                &effect,
                &isolation(&target, "inc-a", 1_100),
                &VirtualManagementClock::new(ManagementTimestamp::from_unix_millis(9_999_999)),
            )
            .unwrap_err(),
        ReadmissionError::ManualMode
    );
}

#[test]
fn an_operator_declaration_permits_reobservation_without_deciding_the_outcome() {
    let target = target("mv", b"object-a");
    let dependencies = ManagementDependencySet::new([1; 32], [2; 32], Some([3; 32]), runtime_id(4));
    let entrance = installed_entrance(&target, &dependencies);
    unsettle(&entrance, &target, &dependencies);
    let service = ManagementContinuationService::new(entrance, RemoteEffectPolicy::default());
    let challenge = ReadmissionChallenge::from_bytes([11; 16]);
    let status = service.status(target.table(), None, challenge).unwrap();
    let effect = unknown_effect(&target, 20, "inc-a", EffectScope::CATALOG_COMMIT, 1_000);

    let permit = service
        .resume_on_declaration(
            &effect,
            &isolation(&target, "inc-a", 1_100),
            &ManualReadmissionDeclaration::try_new(
                status.challenge.expect("challenge issued"),
                effect.responsibility().identity(),
                target.clone(),
                incarnation("inc-a"),
                EffectScope::CATALOG_COMMIT,
                "operator@example",
                "deployment controller confirmed the old FE exited",
                ManagementTimestamp::from_unix_millis(1_200),
            )
            .unwrap(),
        )
        .expect("declaration permits re-observation");

    assert!(
        permit.preserves_unknown_disposition(),
        "a declaration must not decide what the effect did"
    );
}

#[test]
fn one_statement_settles_every_effect_the_declared_writer_left() {
    let target = target("mv", b"object-a");
    let dependencies = ManagementDependencySet::new([1; 32], [2; 32], Some([3; 32]), runtime_id(4));
    let entrance = installed_entrance(&target, &dependencies);
    unsettle(&entrance, &target, &dependencies);
    let service = ManagementContinuationService::new(entrance, RemoteEffectPolicy::default());
    let challenge = ReadmissionChallenge::from_bytes([31; 16]);
    service
        .status(target.table(), None, challenge)
        .expect("status issues the challenge");

    let permits = service
        .resume_target_on_declaration(
            target.table(),
            &MvResumeDeclaration {
                challenge,
                old_incarnation: incarnation("inc-a"),
                operator: "operator@example".to_string(),
                evidence: "deployment controller confirmed the old FE exited".to_string(),
                declared_at: ManagementTimestamp::from_unix_millis(2_000),
            },
        )
        .expect("the declaration covers this target");

    assert_eq!(permits.len(), 1);
    assert!(
        permits[0].preserves_unknown_disposition(),
        "a declaration permits re-observation; it does not decide what happened"
    );
}

#[test]
fn a_statement_about_another_writer_settles_nothing() {
    let target = target("mv", b"object-a");
    let dependencies = ManagementDependencySet::new([1; 32], [2; 32], Some([3; 32]), runtime_id(4));
    let entrance = installed_entrance(&target, &dependencies);
    unsettle(&entrance, &target, &dependencies);
    let service = ManagementContinuationService::new(entrance, RemoteEffectPolicy::default());
    let challenge = ReadmissionChallenge::from_bytes([32; 16]);
    service
        .status(target.table(), None, challenge)
        .expect("status issues the challenge");

    assert_eq!(
        service
            .resume_target_on_declaration(
                target.table(),
                &MvResumeDeclaration {
                    challenge,
                    old_incarnation: incarnation("someone-else"),
                    operator: "operator@example".to_string(),
                    evidence: "an unrelated writer was isolated".to_string(),
                    declared_at: ManagementTimestamp::from_unix_millis(2_000),
                },
            )
            .unwrap_err(),
        ReadmissionError::IncarnationMismatch,
    );
}

#[test]
fn a_spent_challenge_cannot_resume_a_second_time() {
    let target = target("mv", b"object-a");
    let dependencies = ManagementDependencySet::new([1; 32], [2; 32], Some([3; 32]), runtime_id(4));
    let entrance = installed_entrance(&target, &dependencies);
    unsettle(&entrance, &target, &dependencies);
    let service = ManagementContinuationService::new(entrance, RemoteEffectPolicy::default());
    let challenge = ReadmissionChallenge::from_bytes([33; 16]);
    service
        .status(target.table(), None, challenge)
        .expect("status issues the challenge");
    let declaration = MvResumeDeclaration {
        challenge,
        old_incarnation: incarnation("inc-a"),
        operator: "operator@example".to_string(),
        evidence: "deployment controller confirmed the old FE exited".to_string(),
        declared_at: ManagementTimestamp::from_unix_millis(2_000),
    };
    service
        .resume_target_on_declaration(target.table(), &declaration)
        .expect("the first statement is admitted");

    assert_eq!(
        service
            .resume_target_on_declaration(target.table(), &declaration)
            .unwrap_err(),
        ReadmissionError::MissingChallenge,
    );
}

#[test]
fn a_target_with_nothing_unresolved_has_no_declaration_to_admit() {
    let target = target("mv", b"object-a");
    let dependencies = ManagementDependencySet::new([1; 32], [2; 32], Some([3; 32]), runtime_id(4));
    let service = ManagementContinuationService::new(
        installed_entrance(&target, &dependencies),
        RemoteEffectPolicy::default(),
    );

    assert_eq!(
        service
            .resume_target_on_declaration(
                target.table(),
                &MvResumeDeclaration {
                    challenge: ReadmissionChallenge::from_bytes([34; 16]),
                    old_incarnation: incarnation("inc-a"),
                    operator: "operator@example".to_string(),
                    evidence: "nothing to settle".to_string(),
                    declared_at: ManagementTimestamp::from_unix_millis(2_000),
                },
            )
            .unwrap_err(),
        ReadmissionError::ManualMode,
    );
}
