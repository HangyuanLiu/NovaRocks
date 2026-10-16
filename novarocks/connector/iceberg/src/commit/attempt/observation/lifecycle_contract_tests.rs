// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

//! The former transaction's nine lifecycle properties on the sole journal.
//! Typed fault mechanisms isolate lifecycle rules; real catalog publication,
//! artifact cleanup and actual-exit behavior have independent owner tests.
use super::*;
use crate::catalog::publication_dispatch::{
    DispatchCompletion, PreparedCatalogDispatch, PreparedPublication,
};
use std::sync::atomic::{AtomicUsize, Ordering};

struct Fault {
    evidence: DispatchEvidence,
    calls: Arc<AtomicUsize>,
}
#[async_trait]
impl PreparedCatalogDispatch for Fault {
    async fn dispatch_once(
        self: Box<Self>,
        _: crate::catalog::publication::EvidenceContext,
    ) -> DispatchCompletion {
        self.calls.fetch_add(1, Ordering::SeqCst);
        DispatchCompletion::evidence(self.evidence)
    }
}
fn applied() -> DispatchEvidence {
    DispatchEvidence::Applied {
        proof: CommitProof::applied(Some(99)),
    }
}
fn rejected(kind: ConnectorMutationFailureKind) -> DispatchEvidence {
    DispatchEvidence::Rejected {
        failure: ConnectorMutationFailure::new(kind, "explicit adapter rejection"),
        observation: ProtocolObservation {
            adapter: "test-journal",
            stage: "rejected",
            ..Default::default()
        },
    }
}
fn unknown(kind: ConnectorMutationFailureKind) -> DispatchEvidence {
    DispatchEvidence::Unknown {
        failure: ConnectorMutationFailure::new(kind, "explicit inconclusive observation"),
        observation: ProtocolObservation {
            adapter: "test-journal",
            stage: "unknown",
            ..Default::default()
        },
    }
}
async fn publish(
    journal: &PublicationJournal<String>,
    context: &ConnectorRequestContext,
    evidence: DispatchEvidence,
    calls: Arc<AtomicUsize>,
) -> Result<JournalEntryRef> {
    journal
        .publish_once(
            context.clone(),
            Default::default(),
            move |_, _, _| async move {
                Ok(PreparedPublication::Mutation {
                    binding: None,
                    dispatch: Box::new(Fault { evidence, calls }),
                })
            },
        )
        .await
}
fn property_journal() -> (PublicationJournal<String>, ConnectorRequestContext) {
    super::tests::catalog_journal_with_attribution(AttributionPlan::Property {
        table_uuid: Uuid::from_u128(7),
        key: "operation".into(),
        value: "exact-request".into(),
    })
}
fn positive(uuid: Uuid) -> PositiveAttributionObservation {
    PositiveAttributionObservation::PropertyMarker {
        table_uuid: uuid,
        key: "operation".into(),
        value: "exact-request".into(),
    }
}

#[tokio::test]
async fn journal_advances_only_on_typed_adapter_evidence() {
    let kind = ConnectorMutationFailureKind::NotFound;
    for (evidence, expected) in [
        (applied(), "applied"),
        (rejected(kind), "rejected"),
        (unknown(kind), "unknown"),
    ] {
        let (journal, context) = super::tests::catalog_journal();
        let calls = Arc::new(AtomicUsize::new(0));
        let entry = publish(&journal, &context, evidence, Arc::clone(&calls))
            .await
            .unwrap();
        match (expected, journal.catalog_entry_outcome(entry)) {
            ("applied", ExternalMutationOutcome::KnownCommitted { .. }) => {}
            ("rejected", ExternalMutationOutcome::KnownUncommitted { failure, .. }) => {
                assert_eq!(failure.kind(), kind)
            }
            ("unknown", ExternalMutationOutcome::CommitUnknown { failure, .. }) => {
                assert_eq!(failure.kind(), kind)
            }
            (expected, outcome) => panic!("expected {expected}, got {outcome:?}"),
        }
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }
}
#[tokio::test]
async fn success_commits_with_exactly_one_dispatch() {
    let (journal, context) = super::tests::catalog_journal();
    let calls = Arc::new(AtomicUsize::new(0));
    let entry = publish(&journal, &context, applied(), Arc::clone(&calls))
        .await
        .unwrap();
    assert!(matches!(
        journal.catalog_entry_outcome(entry),
        ExternalMutationOutcome::KnownCommitted { .. }
    ));
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}
#[tokio::test]
async fn definite_rejection_remains_uncommitted_and_never_grants_a_second_dispatch() {
    let (journal, context) = super::tests::catalog_journal();
    let calls = Arc::new(AtomicUsize::new(0));
    let entry = publish(
        &journal,
        &context,
        rejected(ConnectorMutationFailureKind::Conflict),
        Arc::clone(&calls),
    )
    .await
    .unwrap();
    assert!(
        matches!(journal.catalog_entry_outcome(entry),ExternalMutationOutcome::KnownUncommitted{failure,cleanup:ExternalMutationFinalization::Complete}
        if failure.kind()==ConnectorMutationFailureKind::Conflict)
    );
    assert!(journal.claim_dispatch().is_err());
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}
#[tokio::test]
async fn definite_conflict_retry_requires_a_fresh_one_dispatch_entry() {
    let (journal, context) = super::tests::catalog_journal();
    let calls = Arc::new(AtomicUsize::new(0));
    let first = publish(
        &journal,
        &context,
        rejected(ConnectorMutationFailureKind::Conflict),
        Arc::clone(&calls),
    )
    .await
    .unwrap();
    let checked = journal.current_checked().unwrap();
    let fresh = journal.next_frontier().unwrap();
    fresh
        .prepare_catalog(
            checked.evidence,
            "fresh receipt".into(),
            vec!["namespace:db".into()],
            b"fresh request".to_vec(),
            AttributionPlan::ObserveOnly,
            context.clone(),
        )
        .unwrap();
    assert_ne!(
        journal.descriptor().unwrap().key,
        fresh.descriptor().unwrap().key
    );
    let second = publish(&fresh, &context, applied(), Arc::clone(&calls))
        .await
        .unwrap();
    assert!(matches!(
        journal.catalog_entry_outcome(first),
        ExternalMutationOutcome::KnownUncommitted { .. }
    ));
    assert!(matches!(
        fresh.catalog_entry_outcome(second),
        ExternalMutationOutcome::KnownCommitted { .. }
    ));
    assert_eq!(calls.load(Ordering::SeqCst), 2);
}
#[tokio::test]
async fn lost_response_closes_mutation_authority() {
    let (journal, context) = super::tests::catalog_journal();
    let calls = Arc::new(AtomicUsize::new(0));
    let entry = publish(
        &journal,
        &context,
        unknown(ConnectorMutationFailureKind::Unavailable),
        Arc::clone(&calls),
    )
    .await
    .unwrap();
    assert!(
        publish(&journal, &context, applied(), Arc::clone(&calls))
            .await
            .is_err()
    );
    assert!(journal.next_frontier().is_err());
    assert!(journal.claim_dispatch().is_err());
    assert!(matches!(
        journal.catalog_entry_outcome(entry),
        ExternalMutationOutcome::CommitUnknown { .. }
    ));
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}
#[tokio::test]
async fn adjudication_upgrades_only_on_exact_positive_evidence() {
    let (journal, context) = property_journal();
    let calls = Arc::new(AtomicUsize::new(0));
    let entry = publish(
        &journal,
        &context,
        unknown(ConnectorMutationFailureKind::Unavailable),
        Arc::clone(&calls),
    )
    .await
    .unwrap();
    let key = journal.descriptor().unwrap().key;
    assert!(matches!(
        journal.catalog_entry_outcome(entry),
        ExternalMutationOutcome::CommitUnknown { .. }
    ));
    assert!(
        journal
            .append_positive_proof_with_finalization(
                key,
                positive(Uuid::from_u128(8)),
                ExternalMutationFinalization::Complete
            )
            .is_err()
    );
    assert!(matches!(
        journal.catalog_entry_outcome(entry),
        ExternalMutationOutcome::CommitUnknown { .. }
    ));
    journal
        .append_positive_proof_with_finalization(
            key,
            positive(Uuid::from_u128(7)),
            ExternalMutationFinalization::Complete,
        )
        .unwrap();
    assert!(matches!(
        journal.catalog_entry_outcome(entry),
        ExternalMutationOutcome::KnownCommitted { .. }
    ));
    let state = journal.state.lock().unwrap();
    assert!(
        matches!(
            &state.entries[&entry.0].phase,
            EntryPhase::Concluded(DispatchEvidence::Unknown { .. })
        ),
        "positive proof must preserve the original observation"
    );
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}
#[tokio::test]
async fn adjudication_is_refused_outside_the_unknown_state() {
    let (journal, context) = property_journal();
    let key = journal.descriptor().unwrap().key;
    assert!(
        journal
            .append_positive_proof_with_finalization(
                key,
                positive(Uuid::from_u128(7)),
                ExternalMutationFinalization::Complete
            )
            .is_err()
    );
    let calls = Arc::new(AtomicUsize::new(0));
    publish(&journal, &context, applied(), Arc::clone(&calls))
        .await
        .unwrap();
    assert!(
        journal
            .append_positive_proof_with_finalization(
                key,
                positive(Uuid::from_u128(7)),
                ExternalMutationFinalization::Complete
            )
            .is_err()
    );
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}
#[tokio::test]
async fn abandoning_the_pre_dispatch_claim_releases_authority_without_dispatch() {
    let (journal, _) = super::tests::catalog_journal();
    let claim = journal.claim_dispatch().unwrap();
    drop(claim);
    assert!(matches!(
        journal.catalog_outcome(),
        ExternalMutationOutcome::KnownUncommitted {
            cleanup: ExternalMutationFinalization::Complete,
            ..
        }
    ));
    assert!(journal.claim_dispatch().is_err());
    let entry = journal.current_entry().unwrap();
    assert!(matches!(
        &journal.state.lock().unwrap().entries[&entry.0].phase,
        EntryPhase::Concluded(DispatchEvidence::NotDispatched { .. })
    ));
}
#[tokio::test]
async fn a_proven_commit_cannot_be_aborted_or_replaced_by_a_fresh_stage() {
    let (journal, context) = super::tests::catalog_journal();
    let calls = Arc::new(AtomicUsize::new(0));
    let entry = publish(&journal, &context, applied(), Arc::clone(&calls))
        .await
        .unwrap();
    assert!(journal.claim_dispatch().is_err());
    let fresh = journal.next_frontier().unwrap();
    assert!(fresh.current_entry().is_none());
    assert!(matches!(
        journal.catalog_entry_outcome(entry),
        ExternalMutationOutcome::KnownCommitted { .. }
    ));
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}
