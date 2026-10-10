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

//! Retain checked recovery and receipt projections across the runtime bridge.

use super::{Publisher, Report};
use crate::catalog::error::CatalogOutcome;
use crate::catalog::transaction::CommitProof;
use crate::commit::model::{CleanupScope, FrozenRequest, OperationToken, PublicationOutcome};
use crate::commit::operation::{IcebergCommitAttempt, IcebergCommitOperation};
use crate::commit::staging::StagingBase;
use crate::iceberg::{Error, ErrorKind, Result};
use async_trait::async_trait;
use novarocks_spi::connector::{
    ConnectorMutationFailure, ConnectorMutationFailureKind, ExternalMutationEvidence,
    ExternalMutationFinalization, ExternalMutationOutcome,
};
use std::sync::{Arc, Mutex};

#[derive(Clone)]
struct Checked<R> {
    evidence: ExternalMutationEvidence,
    receipt: R,
}
#[derive(Clone)]
enum Phase<R> {
    Undispatched,
    Ready(Checked<R>),
    Issued(Checked<R>),
    Committed(CommitProof, Checked<R>, ExternalMutationFinalization),
    Rejected(ConnectorMutationFailure),
}

pub(crate) struct PublicationJournal<R> {
    operation: OperationToken,
    phase: Arc<Mutex<Phase<R>>>,
}
impl<R> Clone for PublicationJournal<R> {
    fn clone(&self) -> Self {
        Self {
            operation: self.operation,
            phase: Arc::clone(&self.phase),
        }
    }
}
impl<R: Clone + Send + Sync> PublicationJournal<R> {
    pub(crate) fn new(operation: OperationToken) -> Self {
        Self {
            operation,
            phase: Arc::new(Mutex::new(Phase::Undispatched)),
        }
    }
    /// Called only by actual FrozenRequest preflight. Capacity and receipt
    /// projection are checked before this transition can authorize dispatch.
    pub(crate) fn record(&self, evidence: ExternalMutationEvidence, receipt: R) -> Result<()> {
        if evidence.operation_id().to_bytes() != self.operation.to_bytes() {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                "Checked recovery carrier names another operation",
            ));
        }
        let mut phase = self
            .phase
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if matches!(*phase, Phase::Issued(_) | Phase::Committed(..)) {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                "Publication cannot prepare again after a possible dispatch",
            ));
        }
        *phase = Phase::Ready(Checked { evidence, receipt });
        Ok(())
    }
    pub(crate) fn observe(&self, inner: Arc<dyn Publisher>) -> impl Publisher + use<R> {
        ObservedPublisher {
            inner,
            journal: self.clone(),
        }
    }
    pub(crate) fn project(&self, report: Report) -> ExternalMutationOutcome<R> {
        match report.publication {
            PublicationOutcome::KnownUncommitted(failure) => {
                ExternalMutationOutcome::KnownUncommitted {
                    failure,
                    cleanup: report.cleanup.finalization(),
                }
            }
            PublicationOutcome::Committed(proof) => {
                let phase = self
                    .phase
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                let Phase::Committed(_, checked, owner_finalization) = &*phase else {
                    panic!("Committed publication follows the observed checked dispatch")
                };
                ExternalMutationOutcome::KnownCommitted {
                    effect: proof.effect,
                    receipt: checked.receipt.clone(),
                    finalization: merge_finalization(
                        owner_finalization.clone(),
                        report.cleanup.finalization(),
                    ),
                }
            }
            PublicationOutcome::Unknown { failure, .. } => {
                let phase = self
                    .phase
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                let Phase::Issued(checked) = &*phase else {
                    panic!("Unknown publication follows the observed checked dispatch")
                };
                ExternalMutationOutcome::CommitUnknown {
                    failure,
                    evidence: checked.evidence.clone(),
                }
            }
        }
    }
    pub(crate) async fn recover_bridge(
        &self,
        operation: &IcebergCommitOperation,
        message: String,
    ) -> ExternalMutationOutcome<R> {
        let phase = self
            .phase
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        let failure =
            ConnectorMutationFailure::new(ConnectorMutationFailureKind::Unavailable, message);
        match phase {
            Phase::Issued(checked) => ExternalMutationOutcome::CommitUnknown {
                failure,
                evidence: checked.evidence,
            },
            Phase::Committed(proof, checked, owner_finalization) => {
                ExternalMutationOutcome::KnownCommitted {
                    effect: proof.effect,
                    receipt: checked.receipt,
                    finalization: merge_finalization(
                        owner_finalization,
                        ExternalMutationFinalization::Failed(failure),
                    ),
                }
            }
            Phase::Rejected(original) => ExternalMutationOutcome::KnownUncommitted {
                failure: original,
                cleanup: operation
                    .cleanup(CleanupScope::EntireOperation)
                    .await
                    .finalization(),
            },
            Phase::Undispatched | Phase::Ready(_) => ExternalMutationOutcome::KnownUncommitted {
                failure,
                cleanup: operation
                    .cleanup(CleanupScope::EntireOperation)
                    .await
                    .finalization(),
            },
        }
    }
    /// The recovery bridge itself failed. No physical cleanup completion can
    /// be claimed, while an issued or proven publication keeps its exact fact.
    pub(crate) fn bridge_failure(&self, message: String) -> ExternalMutationOutcome<R> {
        let phase = self
            .phase
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        let failure =
            ConnectorMutationFailure::new(ConnectorMutationFailureKind::Unavailable, message);
        match phase {
            Phase::Issued(checked) => ExternalMutationOutcome::CommitUnknown {
                failure,
                evidence: checked.evidence,
            },
            Phase::Committed(proof, checked, owner_finalization) => {
                ExternalMutationOutcome::KnownCommitted {
                    effect: proof.effect,
                    receipt: checked.receipt,
                    finalization: merge_finalization(
                        owner_finalization,
                        ExternalMutationFinalization::Failed(failure),
                    ),
                }
            }
            Phase::Rejected(original) => ExternalMutationOutcome::KnownUncommitted {
                failure: original,
                cleanup: ExternalMutationFinalization::Failed(failure),
            },
            Phase::Undispatched | Phase::Ready(_) => ExternalMutationOutcome::KnownUncommitted {
                failure: failure.clone(),
                cleanup: ExternalMutationFinalization::Failed(failure),
            },
        }
    }
}

fn merge_finalization(
    first: ExternalMutationFinalization,
    second: ExternalMutationFinalization,
) -> ExternalMutationFinalization {
    match (first, second) {
        (
            ExternalMutationFinalization::Failed(first),
            ExternalMutationFinalization::Failed(second),
        ) => ExternalMutationFinalization::Failed(ConnectorMutationFailure::new(
            first.kind(),
            format!(
                "{}; additional finalization failure: {}",
                first.message(),
                second.message()
            ),
        )),
        (failed @ ExternalMutationFinalization::Failed(_), _)
        | (_, failed @ ExternalMutationFinalization::Failed(_)) => failed,
        _ => ExternalMutationFinalization::Complete,
    }
}

struct ObservedPublisher<R> {
    inner: Arc<dyn Publisher>,
    journal: PublicationJournal<R>,
}
#[async_trait]
impl<R: Clone + Send + Sync> Publisher for ObservedPublisher<R> {
    async fn load_target(&self, attempt: &IcebergCommitAttempt) -> Result<StagingBase> {
        self.inner.load_target(attempt).await
    }
    fn preflight_recovery(
        &self,
        request: &FrozenRequest,
        operation: &IcebergCommitOperation,
    ) -> Result<()> {
        self.inner.preflight_recovery(request, operation)?;
        if !matches!(
            *self
                .journal
                .phase
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner),
            Phase::Ready(_)
        ) {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                "Publication preflight did not retain a complete checked carrier",
            ));
        }
        Ok(())
    }
    async fn dispatch_once(&self, request: FrozenRequest) -> CatalogOutcome<CommitProof> {
        let checked = {
            let mut phase = self
                .journal
                .phase
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let Phase::Ready(checked) = &*phase else {
                return CatalogOutcome::uncommitted(
                    ConnectorMutationFailureKind::Internal,
                    "Publication has no checked frozen request before dispatch",
                );
            };
            let checked = checked.clone();
            *phase = Phase::Issued(checked.clone());
            checked
        };
        let outcome = self.inner.dispatch_once(request).await;
        let mut phase = self
            .journal
            .phase
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        match &outcome {
            CatalogOutcome::KnownCommitted {
                receipt,
                finalization,
                ..
            } => *phase = Phase::Committed(receipt.clone(), checked, finalization.clone()),
            CatalogOutcome::KnownUncommitted { failure } => {
                *phase = Phase::Rejected(failure.clone())
            }
            CatalogOutcome::Unsupported(error) => {
                *phase = Phase::Rejected(ConnectorMutationFailure::new(
                    ConnectorMutationFailureKind::Unsupported,
                    error.message(),
                ))
            }
            CatalogOutcome::CommitUnknown { .. } => {}
        }
        outcome
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::error::CatalogCommitEvidence;
    use crate::commit::attempt::{RetryPolicy, run};
    use crate::commit::fast_append::FastAppendPreparer;
    use crate::commit::model::ObjectIdentity;
    use crate::commit::overwrite::preparer_tests::Fixture;
    use crate::commit::recovery::FrozenPublicationFacts;
    use crate::commit::staging::PreparedChange;
    use crate::iceberg::spec::{FormatVersion, TableMetadata};
    use bytes::Bytes;
    use novarocks_spi::connector::{
        ConnectorInstanceDescriptor, ConnectorInstanceId, ConnectorMutationOperationId,
        ConnectorProviderId, ExternalMutationEffect, ProviderBindingEpoch,
    };
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[derive(Clone, Copy)]
    enum Behavior {
        Commit,
        CommitFinalizationFailure,
        Unknown,
        PanicBeforeLoad,
        PanicAfterIssue,
    }
    struct Fake {
        operation: IcebergCommitOperation,
        metadata: TableMetadata,
        metadata_location: String,
        journal: PublicationJournal<String>,
        dispatches: Arc<AtomicUsize>,
        behavior: Behavior,
    }
    #[async_trait]
    impl Publisher for Fake {
        async fn load_target(&self, _: &IcebergCommitAttempt) -> Result<StagingBase> {
            if matches!(self.behavior, Behavior::PanicBeforeLoad) {
                panic!("injected before-load bridge panic")
            }
            Ok(StagingBase::Existing {
                metadata: self.metadata.clone(),
                metadata_location: self.metadata_location.clone(),
            })
        }
        fn preflight_recovery(
            &self,
            request: &FrozenRequest,
            operation: &IcebergCommitOperation,
        ) -> Result<()> {
            let facts = FrozenPublicationFacts::from_request(request, operation)?;
            let encoded = serde_json::to_vec(&facts).unwrap();
            let decoded: FrozenPublicationFacts = serde_json::from_slice(&encoded).unwrap();
            assert_eq!(facts, decoded);
            decoded.validate(operation.token())?;
            let evidence = ExternalMutationEvidence::try_new(
                2,
                ConnectorInstanceDescriptor {
                    provider_id: ConnectorProviderId::parse("iceberg").unwrap(),
                    instance_id: ConnectorInstanceId::parse("observation-test").unwrap(),
                },
                ProviderBindingEpoch::from_bytes([1; 16]),
                ConnectorMutationOperationId::from_bytes(self.operation.token().to_bytes()),
                "observation-test",
                Bytes::from(encoded),
            )
            .unwrap();
            self.journal.record(evidence, "preflight receipt".into())
        }
        async fn dispatch_once(&self, request: FrozenRequest) -> CatalogOutcome<CommitProof> {
            self.dispatches.fetch_add(1, Ordering::SeqCst);
            match self.behavior {
                Behavior::PanicAfterIssue => panic!("injected issued bridge panic"),
                Behavior::Unknown => CatalogOutcome::unknown(
                    "lost response",
                    CatalogCommitEvidence::for_target("db.t"),
                ),
                Behavior::Commit => CatalogOutcome::committed(
                    CommitProof::applied(request.ref_snapshot_after(request.target_ref())),
                    ExternalMutationEffect::Applied,
                ),
                Behavior::CommitFinalizationFailure => CatalogOutcome::KnownCommitted {
                    receipt: CommitProof::applied(request.ref_snapshot_after(request.target_ref())),
                    effect: ExternalMutationEffect::Applied,
                    finalization: ExternalMutationFinalization::Failed(
                        ConnectorMutationFailure::new(
                            ConnectorMutationFailureKind::CorruptData,
                            "injected owner finalization failure",
                        ),
                    ),
                },
                Behavior::PanicBeforeLoad => unreachable!(),
            }
        }
    }

    #[test]
    fn bridge_frontier_preserves_unknown_and_prebuilt_committed_receipts() {
        for behavior in [
            Behavior::Unknown,
            Behavior::PanicAfterIssue,
            Behavior::Commit,
            Behavior::CommitFinalizationFailure,
        ] {
            let executor = tokio::runtime::Runtime::new().unwrap();
            let fixture = executor.block_on(async { Fixture::new() });
            let metadata = fixture.metadata(FormatVersion::V3);
            let intent = fixture.intent(&metadata, "main", vec![]);
            let operation = fixture.operation.clone();
            let provenance = fixture.directory.path().join("publication-provenance.json");
            std::fs::write(&provenance, b"provider-owned").unwrap();
            operation
                .adopt_operation_reference(
                    ObjectIdentity::new(format!("file://{}", provenance.display())).unwrap(),
                )
                .unwrap();
            let journal = PublicationJournal::new(operation.token());
            let dispatches = Arc::new(AtomicUsize::new(0));
            let publisher = journal.observe(Arc::new(Fake {
                operation: operation.clone(),
                metadata: metadata.clone(),
                metadata_location: format!(
                    "file://{}/base.metadata.json",
                    fixture.directory.path().display()
                ),
                journal: journal.clone(),
                dispatches: Arc::clone(&dispatches),
                behavior,
            }));
            let run_operation = operation.clone();
            let result = operation.runtime().block_on(async move {
                run(
                    &run_operation,
                    &intent,
                    &PreparedChange::default(),
                    &[&FastAppendPreparer],
                    &publisher,
                    RetryPolicy::from_properties(metadata.properties()).unwrap(),
                )
                .await
            });
            let outcome = match result {
                Ok(report) => journal.project(report),
                Err(error) => executor.block_on(journal.recover_bridge(&operation, error)),
            };
            assert_eq!(dispatches.load(Ordering::SeqCst), 1);
            assert!(!operation.artifacts().unwrap().is_empty());
            match behavior {
                Behavior::Unknown | Behavior::PanicAfterIssue => {
                    let ExternalMutationOutcome::CommitUnknown { evidence, .. } = outcome else {
                        panic!("issued frontier lost")
                    };
                    let Phase::Issued(checked) = &*journal.phase.lock().unwrap() else {
                        panic!("phase lost")
                    };
                    assert_eq!(evidence, checked.evidence);
                    let facts: FrozenPublicationFacts =
                        serde_json::from_slice(evidence.provider_payload()).unwrap();
                    facts.validate(operation.token()).unwrap();
                }
                Behavior::Commit | Behavior::CommitFinalizationFailure => {
                    let ExternalMutationOutcome::KnownCommitted {
                        receipt,
                        finalization,
                        ..
                    } = outcome
                    else {
                        panic!("commit proof lost")
                    };
                    assert_eq!(receipt, "preflight receipt");
                    assert!(provenance.exists());
                    assert!(operation.artifacts().unwrap().iter().any(
                        |record| record.class == crate::commit::model::ArtifactClass::Operation
                    ));
                    if matches!(behavior, Behavior::CommitFinalizationFailure) {
                        let ExternalMutationFinalization::Failed(failure) = finalization else {
                            panic!("owner failure lost")
                        };
                        assert_eq!(failure.kind(), ConnectorMutationFailureKind::CorruptData);
                        assert_eq!(failure.message(), "injected owner finalization failure");
                    }
                    let before = operation.artifacts().unwrap();
                    let recovered = executor.block_on(journal.recover_bridge(
                        &operation,
                        "post-proof finalization bridge failed".into(),
                    ));
                    assert!(matches!(
                        recovered,
                        ExternalMutationOutcome::KnownCommitted {
                            finalization: ExternalMutationFinalization::Failed(_),
                            ..
                        }
                    ));
                    assert_eq!(operation.artifacts().unwrap(), before);
                }
                Behavior::PanicBeforeLoad => unreachable!(),
            }
        }
    }
    #[test]
    fn bridge_before_dispatch_cleans_owned_objects_and_keeps_definite_verdict() {
        let executor = tokio::runtime::Runtime::new().unwrap();
        let fixture = executor.block_on(async { Fixture::new() });
        let metadata = fixture.metadata(FormatVersion::V3);
        let intent = fixture.intent(&metadata, "main", vec![]);
        let path = fixture.directory.path().join("owned.parquet");
        std::fs::write(&path, b"owned").unwrap();
        fixture
            .operation
            .adopt_operation_reference(
                ObjectIdentity::new(format!("file://{}", path.display())).unwrap(),
            )
            .unwrap();
        let operation = fixture.operation.clone();
        let journal = PublicationJournal::new(operation.token());
        let dispatches = Arc::new(AtomicUsize::new(0));
        let publisher = journal.observe(Arc::new(Fake {
            operation: operation.clone(),
            metadata: metadata.clone(),
            metadata_location: "file:///base.metadata.json".into(),
            journal: journal.clone(),
            dispatches: Arc::clone(&dispatches),
            behavior: Behavior::PanicBeforeLoad,
        }));
        let run_operation = operation.clone();
        let error = operation
            .runtime()
            .block_on(async move {
                run(
                    &run_operation,
                    &intent,
                    &PreparedChange::default(),
                    &[&FastAppendPreparer],
                    &publisher,
                    RetryPolicy::from_properties(metadata.properties()).unwrap(),
                )
                .await
            })
            .unwrap_err();
        let outcome = executor.block_on(journal.recover_bridge(&operation, error));
        assert!(matches!(
            outcome,
            ExternalMutationOutcome::KnownUncommitted {
                cleanup: ExternalMutationFinalization::Complete,
                ..
            }
        ));
        assert_eq!(dispatches.load(Ordering::SeqCst), 0);
        assert!(!path.exists());
        assert!(operation.artifacts().unwrap().is_empty());
    }
}
