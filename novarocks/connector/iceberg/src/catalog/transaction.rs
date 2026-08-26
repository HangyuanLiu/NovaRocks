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

//! The unified provider-private transaction.
//!
//! Design: ADR-0110 (docs/adr/ADR-0110-iceberg-provider-private-catalog-owner.md)
//!
//! All three constructors on [`super::NovaRocksCatalog`] return this one type.
//! It exists because the vendored `iceberg::Transaction::commit` cannot express
//! what the publication contract requires: it returns a plain error and retries
//! on `Error::retryable()`, so a caller cannot tell a rejected request from a
//! lost response, and a lost response gets resent.
//!
//! This type answers that question instead, and enforces three rules the
//! previous per-family state machines each restated in their own way:
//!
//! 1. **One dispatch.** [`Transaction::commit`] performs exactly one external
//!    request. It never loops, never reloads-and-resends, and never delegates
//!    to a layer that would.
//! 2. **Unknown closes mutation authority.** Once a request may have landed,
//!    `commit` and `abort` both refuse. The only remaining move is
//!    [`Transaction::adjudicate`], which is read-only.
//! 3. **Abort is pre-dispatch only.** Cleanup after a possible dispatch is how
//!    committed data gets deleted, so the type refuses rather than trusting
//!    each caller to remember.

use std::sync::Arc;

use async_trait::async_trait;
use novarocks_spi::connector::{
    ConnectorError, ConnectorErrorKind, ConnectorMutationFailure, ConnectorMutationFailureKind,
    ExternalMutationEffect,
};

use super::error::{
    CatalogCommitEvidence, CatalogOutcome, classify_dispatched_error, proves_uncommitted,
    uncommitted_failure_kind,
};
use super::{CatalogCreateIntent, CatalogTableName};

/// The exact identity a publication attempt is bound to.
///
/// # On the identity type
///
/// The write and CTAS families carry a `LakePublicationId` end to end. The
/// catalog-mutation family does not: the frontend mints a fresh
/// `ConnectorMutationOperationId` per request and the neutral request carries
/// no publication id. Synthesising a `LakePublicationId` from those bytes here
/// would manufacture an identity the frontend never saw, which is precisely the
/// "second family UUID" the publication contract forbids.
///
/// So this holds whatever exact identity the caller actually owns, as opaque
/// bytes plus the label naming which authority minted it. One identity per
/// frontier is preserved; inventing one is not.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct TransactionIdentity {
    pub(crate) authority: &'static str,
    pub(crate) bytes: [u8; 16],
}

impl TransactionIdentity {
    pub(crate) fn new(authority: &'static str, bytes: [u8; 16]) -> Self {
        Self { authority, bytes }
    }

    pub(crate) fn hex(&self) -> String {
        self.bytes.iter().map(|b| format!("{b:02x}")).collect()
    }
}

/// Proof that a specific publication is present in the catalog.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct CommitProof {
    /// Snapshot the publication landed on, when the operation produces one.
    pub(crate) snapshot_id: Option<i64>,
    /// Table UUID observed at proof time.
    pub(crate) table_uuid: Option<Arc<str>>,
    /// Whether the catalog state changed or the request was already satisfied.
    pub(crate) effect: ExternalMutationEffect,
}

impl CommitProof {
    pub(crate) fn applied(snapshot_id: Option<i64>) -> Self {
        Self {
            snapshot_id,
            table_uuid: None,
            effect: ExternalMutationEffect::Applied,
        }
    }

    pub(crate) fn no_op() -> Self {
        Self {
            snapshot_id: None,
            table_uuid: None,
            effect: ExternalMutationEffect::NoOp,
        }
    }
}

/// Performs the single external request behind one transaction, and can later
/// re-read the catalog to prove whether that request landed.
///
/// Each concrete catalog supplies its own implementation: a REST staged-create
/// commit, a Hadoop conditional metadata publication, or a plain
/// `update_table`. The transaction owns the contract; this owns the mechanism.
#[async_trait]
pub(crate) trait CatalogCommitDispatch: std::fmt::Debug + Send + Sync {
    /// Issue **exactly one** external request.
    ///
    /// Implementations must not retry, must not fall back to a second request,
    /// and must not consult `Error::retryable()`. Returning `Err` hands the
    /// dispatch question to [`super::error::proves_uncommitted`].
    async fn dispatch_once(&self) -> Result<CommitProof, crate::iceberg::Error>;

    /// Re-read the catalog and report whether this exact publication is
    /// present.
    ///
    /// Read-only by contract. `Ok(None)` means "not observed", which is **not**
    /// proof of absence — a caller may never escalate it to "safe to delete".
    async fn adjudicate(&self) -> Result<Option<CommitProof>, ConnectorError>;

    /// Release caller-owned temporary state. Only ever invoked before dispatch.
    async fn abort_before_dispatch(&self) -> Result<(), ConnectorError> {
        Ok(())
    }
}

/// What the transaction is publishing. Kept private to the catalog module so
/// callers above see one lifecycle rather than four.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum TransactionShape {
    ExistingTable,
    CreateTable(CatalogCreateIntent),
    CreateOrReplaceTable,
}

/// Where the single frontier currently stands.
#[derive(Debug)]
enum TransactionState {
    /// Admitted. Nothing external has been attempted yet.
    Admitted,
    /// Terminal: the publication is proven present.
    Committed,
    /// Terminal: proven that nothing was published.
    Uncommitted,
    /// The request may have landed. Mutation authority is closed for good.
    Unknown(CatalogCommitEvidence),
}

/// Inputs for a transaction against an existing table.
#[derive(Clone, Debug)]
pub(crate) struct TransactionRequest {
    pub(crate) identity: TransactionIdentity,
    pub(crate) target: CatalogTableName,
    pub(crate) target_ref: Arc<str>,
    pub(crate) base_snapshot_id: Option<i64>,
    pub(crate) expected_table_uuid: Option<Arc<str>>,
}

/// Inputs for a transaction that creates a table.
#[derive(Clone, Debug)]
pub(crate) struct CreateTableTransactionRequest {
    pub(crate) identity: TransactionIdentity,
    pub(crate) target: CatalogTableName,
    pub(crate) intent: CatalogCreateIntent,
    /// Explicit warehouse root. CTAS admission requires one; empty-table
    /// creation may fall back to the catalog's own table location.
    pub(crate) warehouse: Option<Arc<str>>,
}

/// One publication frontier, from admission to a terminal outcome.
#[derive(Debug)]
pub(crate) struct Transaction {
    identity: TransactionIdentity,
    target: CatalogTableName,
    shape: TransactionShape,
    evidence: CatalogCommitEvidence,
    dispatch: Arc<dyn CatalogCommitDispatch>,
    state: TransactionState,
}

impl Transaction {
    pub(crate) fn new(
        identity: TransactionIdentity,
        target: CatalogTableName,
        shape: TransactionShape,
        evidence: CatalogCommitEvidence,
        dispatch: Arc<dyn CatalogCommitDispatch>,
    ) -> Self {
        Self {
            identity,
            target,
            shape,
            evidence,
            dispatch,
            state: TransactionState::Admitted,
        }
    }

    pub(crate) fn identity(&self) -> &TransactionIdentity {
        &self.identity
    }

    pub(crate) fn target(&self) -> &CatalogTableName {
        &self.target
    }

    pub(crate) fn shape(&self) -> TransactionShape {
        self.shape
    }

    /// True once no further mutation may be issued for this frontier.
    pub(crate) fn mutation_authority_closed(&self) -> bool {
        matches!(
            self.state,
            TransactionState::Unknown(_) | TransactionState::Committed
        )
    }

    /// Publish, with exactly one external request.
    ///
    /// A second call is refused rather than re-dispatched: repeating a request
    /// whose first outcome is unknown is the specific mistake this owner
    /// exists to prevent.
    pub(crate) async fn commit(&mut self) -> CatalogOutcome<CommitProof> {
        match &self.state {
            TransactionState::Admitted => {}
            TransactionState::Committed => {
                return CatalogOutcome::uncommitted(
                    ConnectorMutationFailureKind::InvalidRequest,
                    format!(
                        "Iceberg publication {} already committed; commit is not repeatable",
                        self.identity.hex()
                    ),
                );
            }
            TransactionState::Uncommitted => {
                return CatalogOutcome::uncommitted(
                    ConnectorMutationFailureKind::InvalidRequest,
                    format!(
                        "Iceberg publication {} already failed; start a new attempt instead of \
                         reusing this transaction",
                        self.identity.hex()
                    ),
                );
            }
            TransactionState::Unknown(evidence) => {
                // Re-dispatching here is exactly how a lost response becomes a
                // duplicate publication. Report the standing unknown instead.
                return CatalogOutcome::CommitUnknown {
                    failure: ConnectorMutationFailure::new(
                        ConnectorMutationFailureKind::Unavailable,
                        format!(
                            "Iceberg publication {} outcome is unknown; mutation authority is \
                             closed for this attempt",
                            self.identity.hex()
                        ),
                    ),
                    evidence: evidence.clone(),
                };
            }
        }

        match self.dispatch.dispatch_once().await {
            Ok(proof) => {
                self.state = TransactionState::Committed;
                let effect = proof.effect;
                CatalogOutcome::committed(proof, effect)
            }
            Err(error) => {
                if proves_uncommitted(&error) {
                    self.state = TransactionState::Uncommitted;
                    return CatalogOutcome::KnownUncommitted {
                        failure: ConnectorMutationFailure::new(
                            uncommitted_failure_kind(&error),
                            error.to_string(),
                        ),
                    };
                }
                let evidence = self.evidence.clone();
                self.state = TransactionState::Unknown(evidence.clone());
                classify_dispatched_error(&error, || evidence)
            }
        }
    }

    /// Release caller-owned temporary state, before anything was dispatched.
    ///
    /// Refused once the outcome is unknown. Cleaning up after a possible
    /// dispatch is how a committed table loses its files.
    pub(crate) async fn abort(&mut self) -> Result<(), ConnectorError> {
        match &self.state {
            TransactionState::Admitted => {
                self.dispatch.abort_before_dispatch().await?;
                self.state = TransactionState::Uncommitted;
                Ok(())
            }
            TransactionState::Uncommitted => Ok(()),
            TransactionState::Committed => Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                format!(
                    "Iceberg publication {} is committed; abort would delete published state",
                    self.identity.hex()
                ),
            )),
            TransactionState::Unknown(_) => Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                format!(
                    "Iceberg publication {} outcome is unknown; abort is refused because the \
                     request may have been applied",
                    self.identity.hex()
                ),
            )),
        }
    }

    /// Read-only adjudication after an unknown outcome.
    ///
    /// Only exact positive evidence upgrades the verdict. Absence keeps the
    /// state unknown: a catalog that does not show the publication may simply
    /// not show it yet, and treating that as proof of absence is how orphaned
    /// data gets deleted while it is still live.
    pub(crate) async fn adjudicate(&mut self) -> Result<CatalogOutcome<CommitProof>, ConnectorError> {
        let TransactionState::Unknown(evidence) = &self.state else {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                format!(
                    "Iceberg publication {} is not in an unknown state; adjudication applies only \
                     after a possibly-dispatched request",
                    self.identity.hex()
                ),
            ));
        };
        let evidence = evidence.clone();
        match self.dispatch.adjudicate().await? {
            Some(proof) => {
                self.state = TransactionState::Committed;
                let effect = proof.effect;
                Ok(CatalogOutcome::committed(proof, effect))
            }
            None => Ok(CatalogOutcome::CommitUnknown {
                failure: ConnectorMutationFailure::new(
                    ConnectorMutationFailureKind::Unavailable,
                    format!(
                        "Iceberg publication {} was not observed in the catalog; absence is not \
                         proof that it did not commit",
                        self.identity.hex()
                    ),
                ),
                evidence,
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use crate::iceberg::{Error as IcebergError, ErrorKind};

    #[derive(Debug)]
    enum Behavior {
        Succeed,
        RejectDefinitely,
        LoseResponse,
    }

    #[derive(Debug)]
    struct FakeDispatch {
        behavior: Behavior,
        dispatches: AtomicUsize,
        adjudications: AtomicUsize,
        aborts: AtomicUsize,
        /// What a later read-only adjudication would observe.
        observable: Option<i64>,
    }

    impl FakeDispatch {
        fn new(behavior: Behavior) -> Arc<Self> {
            Arc::new(Self {
                behavior,
                dispatches: AtomicUsize::new(0),
                adjudications: AtomicUsize::new(0),
                aborts: AtomicUsize::new(0),
                observable: None,
            })
        }

        fn with_observable(behavior: Behavior, snapshot_id: i64) -> Arc<Self> {
            Arc::new(Self {
                behavior,
                dispatches: AtomicUsize::new(0),
                adjudications: AtomicUsize::new(0),
                aborts: AtomicUsize::new(0),
                observable: Some(snapshot_id),
            })
        }
    }

    #[async_trait]
    impl CatalogCommitDispatch for FakeDispatch {
        async fn dispatch_once(&self) -> Result<CommitProof, IcebergError> {
            self.dispatches.fetch_add(1, Ordering::SeqCst);
            match self.behavior {
                Behavior::Succeed => Ok(CommitProof::applied(Some(41))),
                Behavior::RejectDefinitely => Err(IcebergError::new(
                    ErrorKind::CatalogCommitConflicts,
                    "requirement failed",
                )),
                Behavior::LoseResponse => {
                    Err(IcebergError::new(ErrorKind::Unexpected, "connection reset"))
                }
            }
        }

        async fn adjudicate(&self) -> Result<Option<CommitProof>, ConnectorError> {
            self.adjudications.fetch_add(1, Ordering::SeqCst);
            Ok(self.observable.map(|id| CommitProof::applied(Some(id))))
        }

        async fn abort_before_dispatch(&self) -> Result<(), ConnectorError> {
            self.aborts.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    fn transaction(dispatch: Arc<FakeDispatch>) -> Transaction {
        Transaction::new(
            TransactionIdentity::new("test", [7u8; 16]),
            CatalogTableName::new("db", "t"),
            TransactionShape::ExistingTable,
            CatalogCommitEvidence::for_target("db.t").with_commit_uuid("commit-uuid"),
            dispatch,
        )
    }

    #[tokio::test]
    async fn success_commits_with_exactly_one_dispatch() {
        let dispatch = FakeDispatch::new(Behavior::Succeed);
        let mut tx = transaction(Arc::clone(&dispatch));
        let outcome = tx.commit().await;
        assert!(matches!(outcome, CatalogOutcome::KnownCommitted { .. }));
        assert_eq!(dispatch.dispatches.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn definite_rejection_is_uncommitted_and_still_allows_cleanup() {
        let dispatch = FakeDispatch::new(Behavior::RejectDefinitely);
        let mut tx = transaction(Arc::clone(&dispatch));
        let outcome = tx.commit().await;
        assert!(outcome.permits_cleanup());
        assert!(matches!(outcome, CatalogOutcome::KnownUncommitted { .. }));
        assert_eq!(dispatch.dispatches.load(Ordering::SeqCst), 1);
        // Cleanup is legitimate here, so abort must succeed.
        tx.abort().await.expect("abort after definite rejection");
    }

    #[tokio::test]
    async fn lost_response_closes_mutation_authority() {
        let dispatch = FakeDispatch::new(Behavior::LoseResponse);
        let mut tx = transaction(Arc::clone(&dispatch));
        let outcome = tx.commit().await;
        assert!(matches!(outcome, CatalogOutcome::CommitUnknown { .. }));
        assert!(tx.mutation_authority_closed());

        // The three things that must not happen after an unknown outcome.
        let second = tx.commit().await;
        assert!(matches!(second, CatalogOutcome::CommitUnknown { .. }));
        assert_eq!(
            dispatch.dispatches.load(Ordering::SeqCst),
            1,
            "a second commit must not re-dispatch"
        );
        tx.abort().await.expect_err("abort must be refused");
        assert_eq!(
            dispatch.aborts.load(Ordering::SeqCst),
            0,
            "abort must not reach the dispatcher after an unknown outcome"
        );
    }

    #[tokio::test]
    async fn adjudication_upgrades_only_on_exact_positive_evidence() {
        // Absence keeps it unknown.
        let dispatch = FakeDispatch::new(Behavior::LoseResponse);
        let mut tx = transaction(Arc::clone(&dispatch));
        let _ = tx.commit().await;
        let verdict = tx.adjudicate().await.expect("adjudicate");
        assert!(matches!(verdict, CatalogOutcome::CommitUnknown { .. }));
        assert!(tx.mutation_authority_closed());

        // Presence upgrades it.
        let dispatch = FakeDispatch::with_observable(Behavior::LoseResponse, 99);
        let mut tx = transaction(Arc::clone(&dispatch));
        let _ = tx.commit().await;
        let verdict = tx.adjudicate().await.expect("adjudicate");
        let CatalogOutcome::KnownCommitted { receipt, .. } = verdict else {
            panic!("expected KnownCommitted");
        };
        assert_eq!(receipt.snapshot_id, Some(99));
        assert_eq!(
            dispatch.dispatches.load(Ordering::SeqCst),
            1,
            "adjudication must be read-only"
        );
    }

    #[tokio::test]
    async fn adjudication_is_refused_outside_the_unknown_state() {
        let dispatch = FakeDispatch::new(Behavior::Succeed);
        let mut tx = transaction(Arc::clone(&dispatch));
        tx.adjudicate()
            .await
            .expect_err("adjudication before any dispatch is a contract error");
        let _ = tx.commit().await;
        tx.adjudicate()
            .await
            .expect_err("adjudication after a proven commit is a contract error");
        assert_eq!(dispatch.adjudications.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn abort_before_dispatch_releases_caller_state_without_dispatching() {
        let dispatch = FakeDispatch::new(Behavior::Succeed);
        let mut tx = transaction(Arc::clone(&dispatch));
        tx.abort().await.expect("abort before dispatch");
        assert_eq!(dispatch.aborts.load(Ordering::SeqCst), 1);
        assert_eq!(dispatch.dispatches.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn abort_after_commit_is_refused() {
        let dispatch = FakeDispatch::new(Behavior::Succeed);
        let mut tx = transaction(Arc::clone(&dispatch));
        let _ = tx.commit().await;
        tx.abort()
            .await
            .expect_err("abort must not delete published state");
        assert_eq!(dispatch.aborts.load(Ordering::SeqCst), 0);
    }
}
