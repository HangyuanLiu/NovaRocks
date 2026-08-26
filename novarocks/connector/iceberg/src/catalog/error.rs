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

//! Typed results for the provider-private Iceberg catalog owner.
//!
//! Design: ADR-0110 (docs/adr/ADR-0110-iceberg-provider-private-catalog-owner.md)
//!
//! Two rules shape everything in this module.
//!
//! First, `Unsupported` is an admission result, not an error class. It is only
//! constructible through [`CatalogUnsupported`], which no post-dispatch code
//! path can reach: once a request may have left this process the classifier
//! can only produce `KnownUncommitted` or `CommitUnknown`. The zero-side-effect
//! promise is therefore carried by the type, not by a comment.
//!
//! Second, a mutation error is classified by what it proves about *dispatch*,
//! never by whether it looks transient. The vendored catalog reports a lost or
//! ambiguous response as `ErrorKind::Unexpected` with `retryable == true`; for
//! a mutation that is precisely the state in which the request may already have
//! been applied. It maps to `CommitUnknown`, and `retryable` never authorizes a
//! resend here.

use std::sync::Arc;

use novarocks_spi::connector::{
    ConnectorError, ConnectorErrorKind, ConnectorMutationFailure, ConnectorMutationFailureKind,
    ExternalMutationEffect, ExternalMutationFinalization,
};

/// A refusal proven to have produced no external side effect.
///
/// Construction is deliberately restricted to admission paths. See the module
/// comment: reaching this type after a request may have been dispatched would
/// break the contract it exists to encode.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct CatalogUnsupported {
    message: Arc<str>,
}

impl CatalogUnsupported {
    pub(crate) fn new(message: impl Into<Arc<str>>) -> Self {
        Self {
            message: message.into(),
        }
    }

    pub(crate) fn message(&self) -> &str {
        &self.message
    }
}

impl std::fmt::Display for CatalogUnsupported {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message)
    }
}

/// Bounded, provider-owned facts retained when a catalog request may have been
/// dispatched but its outcome is not known.
///
/// This deliberately holds only lake-side identity. Connector identity
/// (descriptor, incarnation, neutral operation id) belongs to the metadata
/// adapter, which wraps this into the neutral evidence carrier at the SPI
/// boundary.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(crate) struct CatalogCommitEvidence {
    /// Canonical `namespace.table` the request targeted.
    pub(crate) target: Option<Arc<str>>,
    /// Table UUID the request asserted against, when the target existed.
    pub(crate) target_uuid: Option<Arc<str>>,
    /// Ref the publication targeted (`main` unless a branch was named).
    pub(crate) target_ref: Option<Arc<str>>,
    /// Snapshot id the request required as its base, when applicable.
    pub(crate) base_snapshot_id: Option<i64>,
    /// Commit uuid stamped into the attempted snapshot, when applicable. This
    /// is what makes later read-only adjudication exact rather than heuristic.
    pub(crate) commit_uuid: Option<Arc<str>>,
    /// Metadata location the attempt tried to publish, when applicable.
    pub(crate) metadata_location: Option<Arc<str>>,
}

impl CatalogCommitEvidence {
    pub(crate) fn for_target(target: impl Into<Arc<str>>) -> Self {
        Self {
            target: Some(target.into()),
            ..Self::default()
        }
    }

    pub(crate) fn with_commit_uuid(mut self, commit_uuid: impl Into<Arc<str>>) -> Self {
        self.commit_uuid = Some(commit_uuid.into());
        self
    }

    pub(crate) fn with_target_uuid(mut self, uuid: impl Into<Arc<str>>) -> Self {
        self.target_uuid = Some(uuid.into());
        self
    }

    pub(crate) fn with_target_ref(mut self, target_ref: impl Into<Arc<str>>) -> Self {
        self.target_ref = Some(target_ref.into());
        self
    }

    pub(crate) fn with_base_snapshot_id(mut self, base: Option<i64>) -> Self {
        self.base_snapshot_id = base;
        self
    }

    pub(crate) fn with_metadata_location(mut self, location: impl Into<Arc<str>>) -> Self {
        self.metadata_location = Some(location.into());
        self
    }
}

/// The result of one catalog operation that owns a single external frontier.
///
/// The three committed/uncommitted/unknown arms mirror the neutral publication
/// contract. The fourth arm exists because admission and failure are different
/// facts: `Unsupported` says the operation was refused with nothing attempted,
/// which a caller may safely treat as "nothing happened anywhere".
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum CatalogOutcome<T> {
    KnownCommitted {
        effect: ExternalMutationEffect,
        receipt: T,
        finalization: ExternalMutationFinalization,
    },
    /// Refused before any external side effect.
    Unsupported(CatalogUnsupported),
    /// Failed with proof that no external mutation was applied.
    KnownUncommitted { failure: ConnectorMutationFailure },
    /// The request may have been applied. Callers must not retry, abort,
    /// clean up, or delete on this arm.
    CommitUnknown {
        failure: ConnectorMutationFailure,
        evidence: CatalogCommitEvidence,
    },
}

/// Proof that a catalog mutation is known committed.
///
/// This exists so "delete objects only after the catalog drop committed" is a
/// compile-time rule rather than a review comment. A witness cannot be
/// constructed directly; the only source is
/// [`CatalogOutcome::into_known_committed`], so an unknown or uncommitted
/// outcome simply cannot produce the value that object deletion requires.
#[derive(Debug)]
pub(crate) struct KnownCommitted(());

impl<T> CatalogOutcome<T> {
    pub(crate) fn committed(receipt: T, effect: ExternalMutationEffect) -> Self {
        Self::KnownCommitted {
            effect,
            receipt,
            finalization: ExternalMutationFinalization::Complete,
        }
    }

    pub(crate) fn unsupported(message: impl Into<Arc<str>>) -> Self {
        Self::Unsupported(CatalogUnsupported::new(message))
    }

    pub(crate) fn uncommitted(
        kind: ConnectorMutationFailureKind,
        message: impl Into<Arc<str>>,
    ) -> Self {
        Self::KnownUncommitted {
            failure: ConnectorMutationFailure::new(kind, message),
        }
    }

    pub(crate) fn unknown(message: impl Into<Arc<str>>, evidence: CatalogCommitEvidence) -> Self {
        Self::CommitUnknown {
            failure: ConnectorMutationFailure::new(
                ConnectorMutationFailureKind::Unavailable,
                message,
            ),
            evidence,
        }
    }

    /// Consume a committed outcome, yielding its receipt and the witness that
    /// authorizes post-commit object cleanup.
    ///
    /// Returns `None` for every other arm, which is the point: `CommitUnknown`
    /// must leak rather than delete, and `Unsupported` / `KnownUncommitted`
    /// have nothing published to clean up after.
    pub(crate) fn into_known_committed(
        self,
    ) -> Option<(T, ExternalMutationEffect, KnownCommitted)> {
        match self {
            Self::KnownCommitted {
                receipt, effect, ..
            } => Some((receipt, effect, KnownCommitted(()))),
            _ => None,
        }
    }

    /// True when the caller still owns every resource the request touched, so
    /// cleanup of caller-owned temporary state is safe.
    pub(crate) fn permits_cleanup(&self) -> bool {
        matches!(self, Self::Unsupported(_) | Self::KnownUncommitted { .. })
    }

    /// True when no further mutation may be issued for this frontier.
    pub(crate) fn closes_mutation_authority(&self) -> bool {
        matches!(self, Self::CommitUnknown { .. })
    }
}

/// Classify an error raised **before** the request could reach the catalog.
///
/// Callers must only use this where the surrounding code proves nothing was
/// dispatched — argument validation, local metadata construction, or an
/// admission check that runs ahead of any network or storage write.
pub(crate) fn predispatch_failure(
    kind: ConnectorMutationFailureKind,
    message: impl Into<Arc<str>>,
) -> ConnectorMutationFailure {
    ConnectorMutationFailure::new(kind, message)
}

/// Map a vendored catalog error raised by a **read** into the neutral error
/// vocabulary.
///
/// Reads have no publication frontier, so the only job here is to keep
/// `NotFound`, `Unsupported`, and `Unavailable` distinguishable. In particular
/// `FeatureUnsupported` must not be flattened into a generic internal error:
/// the vendored catalog trait already answers "this catalog cannot do views"
/// that way, and callers depend on telling that apart from an authoritative
/// absence.
pub(crate) fn map_read_error(error: &crate::iceberg::Error) -> ConnectorError {
    use crate::iceberg::ErrorKind;
    let kind = match error.kind() {
        ErrorKind::NamespaceNotFound | ErrorKind::TableNotFound => ConnectorErrorKind::NotFound,
        ErrorKind::FeatureUnsupported => ConnectorErrorKind::Unsupported,
        ErrorKind::NamespaceAlreadyExists | ErrorKind::TableAlreadyExists => {
            ConnectorErrorKind::InvalidRequest
        }
        ErrorKind::PreconditionFailed | ErrorKind::CatalogCommitConflicts => {
            ConnectorErrorKind::InvalidRequest
        }
        ErrorKind::DataInvalid => ConnectorErrorKind::CorruptData,
        ErrorKind::Unexpected => ConnectorErrorKind::Unavailable,
        _ => ConnectorErrorKind::Internal,
    };
    ConnectorError::new(kind, error.to_string())
}

/// Whether a vendored catalog error proves the mutation was **not** applied.
///
/// This is the single place the dispatch question is answered, and it answers
/// conservatively: only errors that a catalog raises as a definite rejection
/// count as proof. Everything else — most importantly `Unexpected`, which is
/// what a lost or truncated response surfaces as — leaves the outcome unknown.
///
/// `Error::retryable()` is deliberately not consulted. It describes whether the
/// vendored client would resend, which is a different question from whether the
/// request already took effect, and acting on it here is exactly the behavior
/// this owner exists to prevent.
pub(crate) fn proves_uncommitted(error: &crate::iceberg::Error) -> bool {
    use crate::iceberg::ErrorKind;
    matches!(
        error.kind(),
        ErrorKind::NamespaceNotFound
            | ErrorKind::TableNotFound
            | ErrorKind::NamespaceAlreadyExists
            | ErrorKind::TableAlreadyExists
            | ErrorKind::PreconditionFailed
            | ErrorKind::CatalogCommitConflicts
            | ErrorKind::DataInvalid
            | ErrorKind::FeatureUnsupported
    )
}

/// Map a definite-rejection catalog error to its neutral failure kind.
///
/// Only meaningful for errors [`proves_uncommitted`] accepted.
pub(crate) fn uncommitted_failure_kind(
    error: &crate::iceberg::Error,
) -> ConnectorMutationFailureKind {
    use crate::iceberg::ErrorKind;
    match error.kind() {
        ErrorKind::NamespaceNotFound | ErrorKind::TableNotFound => {
            ConnectorMutationFailureKind::NotFound
        }
        ErrorKind::NamespaceAlreadyExists | ErrorKind::TableAlreadyExists => {
            ConnectorMutationFailureKind::AlreadyExists
        }
        ErrorKind::PreconditionFailed | ErrorKind::CatalogCommitConflicts => {
            ConnectorMutationFailureKind::Conflict
        }
        ErrorKind::FeatureUnsupported => ConnectorMutationFailureKind::Unsupported,
        ErrorKind::DataInvalid => ConnectorMutationFailureKind::InvalidRequest,
        _ => ConnectorMutationFailureKind::Unavailable,
    }
}

/// Classify a vendored catalog error raised by a request that **was already
/// dispatched**, or whose dispatch state the caller cannot rule out.
///
/// The `Unsupported` arm is unreachable from here by construction: once a
/// request may have left the process, refusing it is no longer an option.
pub(crate) fn classify_dispatched_error<T>(
    error: &crate::iceberg::Error,
    evidence: impl FnOnce() -> CatalogCommitEvidence,
) -> CatalogOutcome<T> {
    if proves_uncommitted(error) {
        return CatalogOutcome::KnownUncommitted {
            failure: ConnectorMutationFailure::new(
                uncommitted_failure_kind(error),
                error.to_string(),
            ),
        };
    }
    CatalogOutcome::unknown(error.to_string(), evidence())
}

/// A catalog-runtime bridge failure (thread spawn or panic while polling).
///
/// The bridge fails around the future, so it cannot say whether the wrapped
/// request was dispatched. Treat it exactly like a lost response.
pub(crate) fn classify_bridge_failure<T>(
    message: &str,
    evidence: CatalogCommitEvidence,
) -> CatalogOutcome<T> {
    CatalogOutcome::unknown(
        format!("Iceberg catalog runtime bridge: {message}"),
        evidence,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::iceberg::{Error as IcebergError, ErrorKind};

    fn err(kind: ErrorKind) -> IcebergError {
        IcebergError::new(kind, "test")
    }

    #[test]
    fn feature_unsupported_reads_stay_distinguishable_from_absence() {
        // The vendored catalog trait answers "this catalog has no views" with
        // FeatureUnsupported. Collapsing it into NotFound would recreate the
        // exact fiction this owner removes.
        assert_eq!(
            map_read_error(&err(ErrorKind::FeatureUnsupported)).kind(),
            ConnectorErrorKind::Unsupported
        );
        assert_eq!(
            map_read_error(&err(ErrorKind::TableNotFound)).kind(),
            ConnectorErrorKind::NotFound
        );
        assert_eq!(
            map_read_error(&err(ErrorKind::Unexpected)).kind(),
            ConnectorErrorKind::Unavailable
        );
    }

    #[test]
    fn only_definite_rejections_prove_a_mutation_did_not_land() {
        for kind in [
            ErrorKind::NamespaceNotFound,
            ErrorKind::TableNotFound,
            ErrorKind::NamespaceAlreadyExists,
            ErrorKind::TableAlreadyExists,
            ErrorKind::PreconditionFailed,
            ErrorKind::CatalogCommitConflicts,
            ErrorKind::DataInvalid,
            ErrorKind::FeatureUnsupported,
        ] {
            assert!(proves_uncommitted(&err(kind)), "{kind:?} should be proof");
        }
        assert!(
            !proves_uncommitted(&err(ErrorKind::Unexpected)),
            "a lost or ambiguous response proves nothing"
        );
    }

    #[test]
    fn lost_response_becomes_unknown_and_closes_mutation_authority() {
        let outcome: CatalogOutcome<()> =
            classify_dispatched_error(&err(ErrorKind::Unexpected), || {
                CatalogCommitEvidence::for_target("db.t").with_commit_uuid("commit-uuid")
            });
        let CatalogOutcome::CommitUnknown { evidence, .. } = &outcome else {
            panic!("expected CommitUnknown, got {outcome:?}");
        };
        assert_eq!(evidence.target.as_deref(), Some("db.t"));
        assert_eq!(evidence.commit_uuid.as_deref(), Some("commit-uuid"));
        assert!(outcome.closes_mutation_authority());
        assert!(
            !outcome.permits_cleanup(),
            "unknown must not authorize cleanup"
        );
    }

    #[test]
    fn retryable_flag_does_not_change_the_dispatch_verdict() {
        // `Unexpected` is what the vendored client raises for a lost response
        // and it is the kind it marks retryable. Retryability describes the
        // client's resend policy, not whether the request took effect.
        let retryable = err(ErrorKind::Unexpected).with_retryable(true);
        let not_retryable = err(ErrorKind::Unexpected).with_retryable(false);
        assert!(!proves_uncommitted(&retryable));
        assert!(!proves_uncommitted(&not_retryable));
    }

    #[test]
    fn conflict_is_uncommitted_not_unknown() {
        let outcome: CatalogOutcome<()> =
            classify_dispatched_error(&err(ErrorKind::CatalogCommitConflicts), || {
                panic!("evidence must not be built for a definite rejection")
            });
        let CatalogOutcome::KnownUncommitted { failure } = &outcome else {
            panic!("expected KnownUncommitted, got {outcome:?}");
        };
        assert_eq!(failure.kind(), ConnectorMutationFailureKind::Conflict);
        assert!(outcome.permits_cleanup());
        assert!(!outcome.closes_mutation_authority());
    }

    #[test]
    fn bridge_failure_is_unknown_because_it_wraps_the_request() {
        let outcome: CatalogOutcome<()> =
            classify_bridge_failure("panicked", CatalogCommitEvidence::for_target("db.t"));
        assert!(outcome.closes_mutation_authority());
    }

    #[test]
    fn only_a_committed_outcome_yields_the_cleanup_witness() {
        // Object deletion needs the witness, and only this arm hands one out.
        // That is what stops a lost response from authorizing a delete.
        let committed: CatalogOutcome<&str> =
            CatalogOutcome::committed("receipt", ExternalMutationEffect::Applied);
        let (receipt, _effect, _witness) = committed
            .into_known_committed()
            .expect("a committed outcome authorizes cleanup");
        assert_eq!(receipt, "receipt");

        let unknown: CatalogOutcome<&str> = CatalogOutcome::unknown(
            "connection reset",
            CatalogCommitEvidence::for_target("db.t"),
        );
        assert!(
            unknown.into_known_committed().is_none(),
            "an unknown outcome must leak, never delete"
        );

        let uncommitted: CatalogOutcome<&str> =
            CatalogOutcome::uncommitted(ConnectorMutationFailureKind::Conflict, "rejected");
        assert!(uncommitted.into_known_committed().is_none());

        let unsupported: CatalogOutcome<&str> = CatalogOutcome::unsupported("no");
        assert!(unsupported.into_known_committed().is_none());
    }

    #[test]
    fn unsupported_permits_cleanup_and_leaves_authority_open() {
        let outcome: CatalogOutcome<()> = CatalogOutcome::unsupported("no staged create here");
        assert!(outcome.permits_cleanup());
        assert!(!outcome.closes_mutation_authority());
    }
}
