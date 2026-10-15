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

//! The sole publication executor. Prepared mechanisms report evidence; only
//! this journal owns the dispatch fact and its independently recorded cleanup.

use super::*;
use crate::catalog::publication::CatalogEvidencePolicy;
use crate::catalog::publication_dispatch::{PreparationResult, PreparedPublication};
use crate::commit::publication_descriptor::RecoveryBinding;
use std::future::Future;

impl<R: Clone + Send + Sync> PublicationJournal<R> {
    pub(crate) async fn publish_once<F, Fut>(
        &self,
        preparation: ConnectorRequestContext,
        policy: CatalogEvidencePolicy,
        prepare: F,
    ) -> Result<JournalEntryRef>
    where
        F: FnOnce(
                PublicationDescriptor,
                novarocks_spi::connector::ProviderBindingEpoch,
                ConnectorRequestContext,
            ) -> Fut
            + Send,
        Fut: Future<Output = PreparationResult<PreparedPublication>> + Send,
    {
        // The claim is acquired before polling any preparation or barrier.
        // A duplicate invocation is an independent call error. It cannot
        // manufacture a rejection or cleanup authority for the live entry.
        let claim = self.claim_dispatch()?;
        let entry_ref = JournalEntryRef(claim.key);
        let (descriptor, generation) = {
            let state = self
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let entry = state
                .entries
                .get(&entry_ref.0)
                .expect("claimed entry exists");
            (
                entry.checked.descriptor.clone(),
                entry.checked.evidence.incarnation(),
            )
        };
        if let Err(error) = preparation.check_active() {
            claim.refuse(super::super::before_dispatch_failure(
                &Error::new(ErrorKind::Unexpected, "Publication preparation stopped")
                    .with_source(error),
            ));
            return Ok(entry_ref);
        }
        // Every adapter receives the original preparation control and owns
        // any started, non-abortable I/O to actual exit. The generic executor
        // must not drop that work in an outer timeout/select.
        let prepared = prepare(descriptor, generation, preparation.clone()).await;
        let prepared = match prepared {
            Ok(prepared) => prepared,
            Err(error) => {
                claim.refuse(error.0);
                return Ok(entry_ref);
            }
        };
        let (binding, dispatch) = match prepared {
            PreparedPublication::Mutation { binding, dispatch } => (binding, dispatch),
            PreparedPublication::NoOp => {
                if claim
                    .begin_dispatch(&preparation, &MemoryDispatchBarrier)
                    .await
                    .is_ok()
                {
                    self.conclude_entry(
                        entry_ref,
                        DispatchEvidence::Applied {
                            proof: CommitProof::no_op(),
                        },
                    );
                }
                return Ok(entry_ref);
            }
        };
        if let Err(error) = claim.bind(binding) {
            claim.refuse(super::super::before_dispatch_failure(&error));
            return Ok(entry_ref);
        }
        if claim
            .begin_dispatch(&preparation, &MemoryDispatchBarrier)
            .await
            .is_err()
        {
            return Ok(entry_ref);
        }
        let evidence_context = match policy.begin_evidence(&preparation) {
            Ok(context) => context,
            Err(error) => {
                self.conclude_entry(
                    entry_ref,
                    DispatchEvidence::NotDispatched {
                        failure: super::super::before_dispatch_failure(
                            &Error::new(
                                ErrorKind::Unexpected,
                                "Publication evidence grant refused",
                            )
                            .with_source(error),
                        ),
                    },
                );
                return Ok(entry_ref);
            }
        };
        self.mark_dispatch_start(entry_ref);
        // No statement cancellation branch may drop a dispatched mutation.
        let completion = dispatch.dispatch_once(evidence_context).await;
        // This is deliberately the first statement after adapter return.
        self.conclude_entry(entry_ref, completion.evidence);
        if let Some(work) = completion.finalization {
            let finalization = match policy.begin_finalization() {
                Ok(context) => work.run(context).await,
                Err(error) => ExternalMutationFinalization::Failed(ConnectorMutationFailure::new(
                    ConnectorMutationFailureKind::Internal,
                    error.message(),
                )),
            };
            self.record_entry_finalization(entry_ref, finalization);
        }
        Ok(entry_ref)
    }

    pub(super) fn mark_dispatch_start(&self, entry_ref: JournalEntryRef) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let entry = state
            .entries
            .get_mut(&entry_ref.0)
            .expect("dispatch entry exists");
        assert!(matches!(entry.phase, EntryPhase::Dispatching));
        assert!(entry.checked.descriptor.first_dispatch_ms.is_none());
        entry.checked.descriptor.first_dispatch_ms = Some(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("clock is after Unix epoch")
                .as_millis()
                .try_into()
                .expect("timestamp fits i64"),
        );
    }

    pub(crate) fn commit_outcome(&self, entry_ref: JournalEntryRef) -> CatalogOutcome<CommitProof> {
        match self.entry_phase(entry_ref) {
            Phase::Committed(proof, _, finalization) => CatalogOutcome::KnownCommitted {
                effect: proof.effect,
                receipt: proof,
                finalization,
            },
            Phase::Rejected(failure, _) => CatalogOutcome::KnownUncommitted { failure },
            Phase::Unknown(checked, _) | Phase::Issued(checked) => {
                // CatalogOutcome is a projection, not another stored verdict.
                // The external recovery envelope remains owned by the journal.
                let failure = match self.entry_phase(entry_ref) {
                    Phase::Unknown(_, failure) => failure,
                    _ => ConnectorMutationFailure::new(
                        ConnectorMutationFailureKind::Unavailable,
                        "Publication has no dispatch conclusion",
                    ),
                };
                CatalogOutcome::CommitUnknown {
                    failure,
                    evidence: crate::catalog::error::CatalogCommitEvidence::for_target(
                        checked.descriptor.targets.join(","),
                    ),
                }
            }
            _ => CatalogOutcome::uncommitted(
                ConnectorMutationFailureKind::Internal,
                "Publication entry has no conclusion",
            ),
        }
    }
}

impl<R: Clone + Send + Sync> PublicationJournal<R> {
    pub(crate) fn observe_prepared(
        &self,
        inner: Arc<dyn super::super::prepared::PublicationPreparer>,
    ) -> impl Publisher + use<R> {
        PreparedObservedPublisher {
            inner,
            journal: self.clone(),
        }
    }
    fn invocation_error(&self, message: &str) -> CatalogOutcome<CommitProof> {
        match self.phase() {
            Phase::Committed(_, _, _)
            | Phase::Rejected(_, _)
            | Phase::Unknown(_, _)
            | Phase::Issued(_) => {
                self.commit_outcome(self.current_entry().expect("active publication entry"))
            }
            Phase::Ready | Phase::Waiting => CatalogOutcome::unknown(
                message,
                crate::catalog::error::CatalogCommitEvidence::for_target(
                    self.descriptor()
                        .expect("active publication descriptor")
                        .targets
                        .join(","),
                ),
            ),
            Phase::Undispatched => {
                CatalogOutcome::uncommitted(ConnectorMutationFailureKind::Internal, message)
            }
        }
    }
}
struct PreparedObservedPublisher<R> {
    inner: Arc<dyn super::super::prepared::PublicationPreparer>,
    journal: PublicationJournal<R>,
}
#[async_trait]
impl<R: Clone + Send + Sync> Publisher for PreparedObservedPublisher<R> {
    async fn load_target(&self, attempt: &IcebergCommitAttempt) -> Result<StagingBase> {
        self.inner.load_target(attempt).await
    }
    fn preflight_recovery(
        &self,
        request: &FrozenRequest,
        operation: &IcebergCommitOperation,
    ) -> Result<()> {
        self.inner.preflight_recovery(request, operation)?;
        self.journal
            .bind_preparation(operation.request_context().clone())
    }
    async fn dispatch_once(&self, request: FrozenRequest) -> CatalogOutcome<CommitProof> {
        let Some(preparation) = self
            .journal
            .current_checked()
            .and_then(|checked| checked.preparation)
        else {
            return self
                .journal
                .invocation_error("Publication has no admitted preparation context");
        };
        let inner = Arc::clone(&self.inner);
        let result = self
            .journal
            .publish_once(
                preparation,
                inner.evidence_policy(),
                move |descriptor, generation, preparation| {
                    inner.prepare_dispatch(request, descriptor, generation, preparation)
                },
            )
            .await;
        match result {
            Ok(entry) => self.journal.commit_outcome(entry),
            Err(_) => self.journal.invocation_error("Duplicate publication invocation refused; the original journal remains authoritative"),
        }
    }
}

impl<R> DispatchClaim<R> {
    fn bind(&self, binding: Option<RecoveryBinding>) -> Result<()> {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let entry = state
            .entries
            .get_mut(&self.key)
            .expect("claimed entry exists");
        if !matches!(entry.phase, EntryPhase::Claimed) || entry.checked.descriptor.binding.is_some()
        {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                "Publication binding requires its unconsumed claim",
            ));
        }
        if let Some(binding) = &binding {
            binding.validate_generation(entry.checked.evidence.incarnation().to_bytes())?;
        }
        let mut candidate = entry.checked.descriptor.clone();
        candidate.binding = binding;
        candidate.validate()?;
        entry.checked.descriptor = candidate;
        Ok(())
    }

    pub(super) fn refuse(mut self, failure: ConnectorMutationFailure) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let entry = state
            .entries
            .get_mut(&self.key)
            .expect("claimed entry exists");
        assert!(
            matches!(entry.phase, EntryPhase::Claimed),
            "Only preparation can refuse a claim"
        );
        entry.phase = EntryPhase::Concluded(DispatchEvidence::NotDispatched { failure });
        self.completed = true;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::publication::EvidenceContext;
    use crate::catalog::publication_dispatch::{
        DispatchCompletion, FinalizationWork, PreparationFailure, PreparedCatalogDispatch,
    };
    use crate::commit::publication_descriptor::RecoveryPrincipalScope;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::sync::Notify;

    struct DelayedDispatch {
        entered: Arc<Notify>,
        exit: Arc<Notify>,
        dispatches: Arc<AtomicUsize>,
        journal: PublicationJournal<String>,
    }
    #[async_trait]
    impl PreparedCatalogDispatch for DelayedDispatch {
        async fn dispatch_once(self: Box<Self>, _: EvidenceContext) -> DispatchCompletion {
            self.dispatches.fetch_add(1, Ordering::SeqCst);
            self.entered.notify_one();
            self.exit.notified().await;
            DispatchCompletion {
                evidence: DispatchEvidence::Applied {
                    proof: CommitProof::applied(None),
                },
                finalization: Some(Box::new(CheckFinalization(self.journal.clone()))),
            }
        }
    }
    struct CheckFinalization(PublicationJournal<String>);
    #[async_trait]
    impl FinalizationWork for CheckFinalization {
        async fn run(self: Box<Self>, _: EvidenceContext) -> ExternalMutationFinalization {
            assert!(
                matches!(self.0.phase(), Phase::Committed(_, _, _)),
                "Applied must precede finalization's first poll"
            );
            ExternalMutationFinalization::Failed(ConnectorMutationFailure::new(
                ConnectorMutationFailureKind::Internal,
                "injected independent finalization failure",
            ))
        }
    }
    fn binding(generation: [u8; 16]) -> RecoveryBinding {
        RecoveryBinding {
            generation,
            method: "POST".into(),
            endpoint: "https://catalog.example/v1/namespaces".into(),
            warehouse: None,
            query: vec![],
            principal_scope: RecoveryPrincipalScope::CatalogGeneration { generation },
            deduplication_domain: "exact-test-domain".into(),
            lifetime: None,
        }
    }

    #[tokio::test]
    async fn duplicate_call_and_statement_stop_do_not_replace_the_live_requests_evidence() {
        let (journal, preparation) = super::super::tests::catalog_journal();
        let stop = novarocks_spi::connector::ConnectorStopOwner::new();
        let preparation = preparation.with_additional_stop(stop.view());
        let entered = Arc::new(Notify::new());
        let exit = Arc::new(Notify::new());
        let dispatches = Arc::new(AtomicUsize::new(0));
        let task_journal = journal.clone();
        let task_entered = entered.clone();
        let task_exit = exit.clone();
        let task_dispatches = dispatches.clone();
        let duplicate_context = preparation.clone();
        let task = tokio::spawn(async move {
            let finalization_journal = task_journal.clone();
            task_journal
                .publish_once(
                    preparation,
                    CatalogEvidencePolicy::default(),
                    move |_, generation, _| async move {
                        Ok(PreparedPublication::Mutation {
                            binding: Some(binding(generation.to_bytes())),
                            dispatch: Box::new(DelayedDispatch {
                                entered: task_entered,
                                exit: task_exit,
                                dispatches: task_dispatches,
                                journal: finalization_journal,
                            }),
                        })
                    },
                )
                .await
        });
        entered.notified().await;
        let duplicate = journal
            .publish_once(
                duplicate_context,
                CatalogEvidencePolicy::default(),
                |_, _, _| async {
                    panic!("duplicate must fail before polling preparation");
                    #[allow(unreachable_code)]
                    Ok::<PreparedPublication, PreparationFailure>(unreachable!())
                },
            )
            .await;
        assert!(duplicate.is_err());
        assert!(matches!(journal.phase(), Phase::Issued(_)));
        assert!(journal.descriptor().unwrap().first_dispatch_ms.is_some());
        stop.request_stop();
        tokio::task::yield_now().await;
        assert!(
            !task.is_finished(),
            "the publication owner must await actual adapter exit"
        );
        exit.notify_one();
        let entry = task.await.unwrap().unwrap();
        assert_eq!(dispatches.load(Ordering::SeqCst), 1);
        assert!(matches!(
            journal.commit_outcome(entry),
            CatalogOutcome::KnownCommitted {
                finalization: ExternalMutationFinalization::Failed(_),
                ..
            }
        ));
        assert!(matches!(
            journal.catalog_entry_outcome(entry),
            ExternalMutationOutcome::KnownCommitted { .. }
        ));
    }

    #[tokio::test]
    async fn foreign_generation_is_refused_without_partial_binding_or_dispatch() {
        let (journal, preparation) = super::super::tests::catalog_journal();
        let dispatches = Arc::new(AtomicUsize::new(0));
        let counter = dispatches.clone();
        let observed = journal.clone();
        let entry = journal
            .publish_once(
                preparation,
                CatalogEvidencePolicy::default(),
                move |_, _, _| async move {
                    Ok(PreparedPublication::Mutation {
                        binding: Some(binding([9; 16])),
                        dispatch: Box::new(DelayedDispatch {
                            entered: Arc::new(Notify::new()),
                            exit: Arc::new(Notify::new()),
                            dispatches: counter,
                            journal: observed,
                        }),
                    })
                },
            )
            .await
            .unwrap();
        assert_eq!(dispatches.load(Ordering::SeqCst), 0);
        let descriptor = journal.entry_descriptor(entry).unwrap();
        assert!(descriptor.binding.is_none());
        assert!(descriptor.first_dispatch_ms.is_none());
        assert!(matches!(
            journal.commit_outcome(entry),
            CatalogOutcome::KnownUncommitted { .. }
        ));
        assert!(matches!(
            journal.state.lock().unwrap().entries[&entry.0].phase,
            EntryPhase::Concluded(DispatchEvidence::NotDispatched { .. })
        ));
    }

    #[tokio::test]
    async fn preparation_waits_for_started_work_to_exit_and_preserves_cancellation_kind() {
        let (journal, preparation) = super::super::tests::catalog_journal();
        let stop = novarocks_spi::connector::ConnectorStopOwner::new();
        let preparation = preparation.with_additional_stop(stop.view());
        let entered = Arc::new(Notify::new());
        let exit = Arc::new(Notify::new());
        let observed = journal.clone();
        let work_entered = entered.clone();
        let work_exit = exit.clone();
        let task = tokio::spawn(async move {
            observed
                .publish_once(
                    preparation,
                    CatalogEvidencePolicy::default(),
                    move |_, _, preparation| async move {
                        work_entered.notify_one();
                        preparation.stop().stopped().await;
                        work_exit.notified().await;
                        Err(PreparationFailure(ConnectorMutationFailure::new(
                            ConnectorMutationFailureKind::Cancelled,
                            "cancelled after actual preparation work exit",
                        )))
                    },
                )
                .await
        });
        entered.notified().await;
        stop.request_stop();
        tokio::task::yield_now().await;
        assert!(!task.is_finished());
        exit.notify_one();
        let entry = task.await.unwrap().unwrap();
        let CatalogOutcome::KnownUncommitted { failure } = journal.commit_outcome(entry) else {
            panic!("preparation cannot dispatch")
        };
        assert_eq!(failure.kind(), ConnectorMutationFailureKind::Cancelled);
        assert!(
            journal
                .entry_descriptor(entry)
                .unwrap()
                .first_dispatch_ms
                .is_none()
        );
    }
    #[tokio::test]
    async fn local_noop_concludes_without_a_dispatch_timestamp_or_binding() {
        let (journal, preparation) = super::super::tests::catalog_journal();
        let entry = journal
            .publish_once(
                preparation,
                CatalogEvidencePolicy::default(),
                |_, _, _| async { Ok(PreparedPublication::NoOp) },
            )
            .await
            .unwrap();
        assert!(matches!(
            journal.commit_outcome(entry),
            CatalogOutcome::KnownCommitted {
                effect: novarocks_spi::connector::ExternalMutationEffect::NoOp,
                ..
            }
        ));
        let descriptor = journal.entry_descriptor(entry).unwrap();
        assert!(descriptor.first_dispatch_ms.is_none());
        assert!(descriptor.binding.is_none());
    }
}
