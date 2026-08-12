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

//! Classifying a discovered MV refresh attempt from lake evidence alone.
//!
//! This is the decision that says whether a staging artifact may be cleaned up,
//! so it is deliberately a pure function with an explicit input record: the whole
//! point is that nothing ambient can influence it.
//!
//! What may **not** decide the outcome, and why each is excluded:
//!
//! * **StateStore timestamps** — after a ledger loss they may not exist, and when
//!   they do they describe when a frontend *observed* something, not when the lake
//!   committed it.
//! * **Local queue order** — it is per-frontend, and two frontends disagree.
//! * **Numeric `mv_id`** — a rebuild reassigns it, so it cannot order attempts
//!   across the very event this classification exists to survive.
//! * **"Highest refresh ID wins"** — refresh IDs are allocated per frontend
//!   ledger, so the highest is not the winner, merely the loudest.
//!
//! Only lake facts decide: where `main` points, what the target's ancestry
//! contains, which generation owns the fence, and whether the provider could
//! resolve the attempt's own operation. Anything short of proof is
//! [`MvAttemptDisposition::Ambiguous`], which retains evidence and stops
//! automatic refresh for that target rather than guessing.

use novarocks_spi::connector::{
    ConnectorMvPublicationFenceGeneration, ConnectorMvPublicationFenceOrder,
};

/// Terminal classification of one discovered attempt.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum MvAttemptDisposition {
    /// Provably never committed. Its artifacts may be reclaimed.
    KnownUncommitted,
    /// Staged and still publishable by its own generation.
    Staged,
    /// Its result is the target's published state, proven by lake evidence.
    Published,
    /// A higher fence or a later legitimate publication made it unpublishable.
    Superseded,
    /// The business outcome is settled; only a retryable cleanup remains.
    CleanupPending,
    /// Evidence is missing, unreadable, or self-contradictory.
    Ambiguous,
}

impl MvAttemptDisposition {
    /// Whether this disposition permits reclaiming the attempt's staging
    /// artifacts.
    ///
    /// `Published` is deliberately excluded: a published attempt's staging ref
    /// may still be the evidence a concurrent recovery is reading, so releasing
    /// it is a separate decision made under current ownership.
    pub(crate) const fn permits_artifact_reclaim(self) -> bool {
        matches!(self, Self::KnownUncommitted | Self::Superseded)
    }

    /// Whether discovering this disposition must stop automatic refresh for the
    /// target until a human or a later inspection resolves it.
    pub(crate) const fn halts_automatic_refresh(self) -> bool {
        matches!(self, Self::Ambiguous)
    }
}

/// How the provider resolved the attempt's own external operation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum AttemptOperationEvidence {
    /// The provider proved the operation committed.
    KnownCommitted,
    /// The provider proved it did not.
    KnownUncommitted,
    /// The provider could not tell.
    Unresolved,
    /// No evidence was retained for this attempt at all.
    Absent,
}

/// The lake facts one attempt is classified against.
#[derive(Clone, Debug)]
pub(crate) struct AttemptEvidence {
    /// The generation that produced this attempt.
    pub generation: ConnectorMvPublicationFenceGeneration,
    /// The generation currently owning the target's fence, if any.
    pub established_generation: Option<ConnectorMvPublicationFenceGeneration>,
    /// `true` when the target's visible state is this attempt's staged result.
    pub is_target_current: bool,
    /// `true` when the staged snapshot is an ancestor of the target's visible
    /// state, meaning it committed and a later refresh built on it.
    pub in_target_ancestry: bool,
    /// `true` when the staging artifact still exists.
    pub staging_present: bool,
    /// Whether the staged snapshot's identity matches this attempt's permit.
    pub staged_identity_matches: bool,
    pub operation: AttemptOperationEvidence,
    /// `true` when cleanup is the only step left for an already-settled outcome.
    pub cleanup_outstanding: bool,
}

/// Classifies one attempt from lake evidence.
///
/// Ordering between generations is delegated to the fencing contract, so
/// "superseded" means the same thing here as it does at the external commit
/// point. A comparison the contract refuses — cross-cluster, or one epoch with
/// two tokens — becomes `Ambiguous` rather than a guess.
pub(crate) fn classify_attempt(evidence: &AttemptEvidence) -> MvAttemptDisposition {
    // Published is the strongest claim and needs a positive lake witness: the
    // target *is* this result, or this result is in its ancestry. Either way the
    // staged identity must match, or we are looking at someone else's snapshot.
    if (evidence.is_target_current || evidence.in_target_ancestry)
        && evidence.staged_identity_matches
    {
        return if evidence.cleanup_outstanding {
            MvAttemptDisposition::CleanupPending
        } else {
            MvAttemptDisposition::Published
        };
    }
    // A target that claims to be this attempt's result while carrying a
    // different identity is a contradiction, not a near-miss.
    if evidence.is_target_current && !evidence.staged_identity_matches {
        return MvAttemptDisposition::Ambiguous;
    }

    // Not published. Whether it still *could* be depends on who owns the fence.
    let superseded = match &evidence.established_generation {
        Some(established) => match evidence.generation.try_order(established) {
            Ok(ConnectorMvPublicationFenceOrder::Superseded) => true,
            Ok(_) => false,
            // The contract refused to order these. That is exactly the case we
            // must not resolve by preference.
            Err(_) => return MvAttemptDisposition::Ambiguous,
        },
        // No fence at all: nobody currently owns publication, so this attempt is
        // not superseded by anyone -- but it also cannot publish until a
        // generation establishes one.
        None => false,
    };

    match evidence.operation {
        // A provably uncommitted attempt is reclaimable regardless of fences.
        AttemptOperationEvidence::KnownUncommitted => MvAttemptDisposition::KnownUncommitted,
        // The provider says it committed, yet the target neither is nor descends
        // from it. Something else published over it, or the evidence disagrees
        // with the lake. Not a cleanup decision.
        AttemptOperationEvidence::KnownCommitted => MvAttemptDisposition::Ambiguous,
        AttemptOperationEvidence::Unresolved => {
            if superseded {
                // It can never publish now, so an unresolved operation no longer
                // changes what happens to it.
                MvAttemptDisposition::Superseded
            } else {
                MvAttemptDisposition::Ambiguous
            }
        }
        AttemptOperationEvidence::Absent => {
            if superseded {
                MvAttemptDisposition::Superseded
            } else if evidence.staging_present && evidence.staged_identity_matches {
                // Intact staging under a generation that has not been superseded
                // is a live attempt, not an orphan.
                MvAttemptDisposition::Staged
            } else {
                // Staging is gone or belongs to someone else, and there is no
                // evidence either way. Absence of an artifact is not proof it was
                // never committed.
                MvAttemptDisposition::Ambiguous
            }
        }
    }
}

/// What a cleanup must prove before it may delete an attempt's artifacts.
///
/// Cleanup is the only irreversible step in recovery, so its authorization is a
/// separate, explicit record rather than an implication of the classification.
/// A disposition says *what an attempt is*; this says *whether this frontend, right
/// now, is entitled to act on it*.
#[derive(Clone, Debug)]
pub(crate) struct CleanupAuthorization {
    /// The disposition reached from lake evidence.
    pub disposition: MvAttemptDisposition,
    /// `true` when this frontend currently holds the target's refresh lease.
    pub holds_current_ownership: bool,
    /// `true` when the generation acting is the one that owns the fence.
    pub acts_under_established_fence: bool,
    /// `true` when the observation being acted on is byte-identical to the one
    /// that produced the disposition.
    pub observation_digest_matches: bool,
    /// `true` when the artifact is still referenced by the target's visible state
    /// or its ancestry.
    pub still_referenced_by_target: bool,
    /// `true` when the artifact is the target's current publication fence.
    pub is_current_fence_artifact: bool,
}

/// Why a cleanup was refused.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum CleanupRefusal {
    /// The disposition does not settle the attempt's fate.
    DispositionNotTerminal,
    /// This frontend is not the current owner.
    NotOwner,
    /// The acting generation does not own the fence.
    NotFenced,
    /// The observation changed since it was classified.
    StaleObservation,
    /// Deleting it would remove data the target still depends on.
    StillReferenced,
    /// It is the fence that arbitrates publication.
    IsCurrentFence,
}

/// Decides whether a cleanup may proceed.
///
/// Every condition is independent, and all must hold. In particular a correct
/// disposition is not sufficient: it was computed at some earlier moment, and by
/// the time cleanup runs this frontend may have lost the lease or the lake may
/// have moved. Requiring live ownership and a matching observation is what keeps a
/// stale classification from authorizing a delete.
pub(crate) fn authorize_cleanup(
    authorization: &CleanupAuthorization,
) -> Result<(), CleanupRefusal> {
    if !authorization.disposition.permits_artifact_reclaim() {
        return Err(CleanupRefusal::DispositionNotTerminal);
    }
    if !authorization.holds_current_ownership {
        return Err(CleanupRefusal::NotOwner);
    }
    if !authorization.acts_under_established_fence {
        return Err(CleanupRefusal::NotFenced);
    }
    if !authorization.observation_digest_matches {
        return Err(CleanupRefusal::StaleObservation);
    }
    // These two are checked last so their refusals are never masked by a
    // permission problem: they are facts about the artifact, and a reviewer
    // reading a log wants to see them distinctly.
    if authorization.still_referenced_by_target {
        return Err(CleanupRefusal::StillReferenced);
    }
    if authorization.is_current_fence_artifact {
        return Err(CleanupRefusal::IsCurrentFence);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn generation(incarnation: u64, epoch: u64) -> ConnectorMvPublicationFenceGeneration {
        ConnectorMvPublicationFenceGeneration::try_new("cluster-a", incarnation, epoch, [7u8; 32])
            .unwrap()
    }

    fn base() -> AttemptEvidence {
        AttemptEvidence {
            generation: generation(1, 1),
            established_generation: Some(generation(1, 1)),
            is_target_current: false,
            in_target_ancestry: false,
            staging_present: true,
            staged_identity_matches: true,
            operation: AttemptOperationEvidence::Absent,
            cleanup_outstanding: false,
        }
    }

    #[test]
    fn a_live_attempt_under_its_own_fence_is_staged() {
        assert_eq!(classify_attempt(&base()), MvAttemptDisposition::Staged);
        assert!(!MvAttemptDisposition::Staged.permits_artifact_reclaim());
    }

    #[test]
    fn publication_requires_a_positive_lake_witness() {
        let current = AttemptEvidence {
            is_target_current: true,
            ..base()
        };
        assert_eq!(classify_attempt(&current), MvAttemptDisposition::Published);

        // Committed and then built upon by a later refresh.
        let ancestor = AttemptEvidence {
            in_target_ancestry: true,
            ..base()
        };
        assert_eq!(classify_attempt(&ancestor), MvAttemptDisposition::Published);

        // A published attempt's staging ref may still be evidence a concurrent
        // recovery is reading, so publication alone does not authorize reclaim.
        assert!(!MvAttemptDisposition::Published.permits_artifact_reclaim());
    }

    #[test]
    fn a_settled_outcome_awaiting_cleanup_is_not_reported_as_published() {
        let pending = AttemptEvidence {
            is_target_current: true,
            cleanup_outstanding: true,
            ..base()
        };
        assert_eq!(
            classify_attempt(&pending),
            MvAttemptDisposition::CleanupPending
        );
    }

    #[test]
    fn a_target_claiming_our_result_with_another_identity_is_ambiguous() {
        // This is a contradiction, not a near-miss: acting on it would attribute
        // someone else's snapshot to this attempt.
        let mismatched = AttemptEvidence {
            is_target_current: true,
            staged_identity_matches: false,
            ..base()
        };
        assert_eq!(
            classify_attempt(&mismatched),
            MvAttemptDisposition::Ambiguous
        );
    }

    #[test]
    fn a_higher_fence_supersedes_and_permits_reclaim() {
        let superseded = AttemptEvidence {
            established_generation: Some(generation(2, 1)),
            ..base()
        };
        assert_eq!(
            classify_attempt(&superseded),
            MvAttemptDisposition::Superseded
        );
        assert!(MvAttemptDisposition::Superseded.permits_artifact_reclaim());

        // Even an unresolved operation is settled once it can never publish.
        let unresolved_but_superseded = AttemptEvidence {
            established_generation: Some(generation(2, 1)),
            operation: AttemptOperationEvidence::Unresolved,
            ..base()
        };
        assert_eq!(
            classify_attempt(&unresolved_but_superseded),
            MvAttemptDisposition::Superseded
        );
    }

    #[test]
    fn provably_uncommitted_is_reclaimable_regardless_of_fences() {
        for established in [None, Some(generation(1, 1)), Some(generation(9, 9))] {
            let evidence = AttemptEvidence {
                established_generation: established,
                operation: AttemptOperationEvidence::KnownUncommitted,
                ..base()
            };
            assert_eq!(
                classify_attempt(&evidence),
                MvAttemptDisposition::KnownUncommitted
            );
        }
        assert!(MvAttemptDisposition::KnownUncommitted.permits_artifact_reclaim());
    }

    #[test]
    fn an_unorderable_generation_pair_is_ambiguous_not_a_preference() {
        // Cross-cluster: the fencing contract refuses to order these, and so must
        // this classifier.
        let other_cluster =
            ConnectorMvPublicationFenceGeneration::try_new("cluster-b", 5, 5, [7u8; 32]).unwrap();
        let evidence = AttemptEvidence {
            established_generation: Some(other_cluster),
            ..base()
        };
        assert_eq!(classify_attempt(&evidence), MvAttemptDisposition::Ambiguous);

        // One epoch, two tokens: two owners claiming one generation.
        let conflicting =
            ConnectorMvPublicationFenceGeneration::try_new("cluster-a", 1, 1, [8u8; 32]).unwrap();
        let evidence = AttemptEvidence {
            established_generation: Some(conflicting),
            ..base()
        };
        assert_eq!(classify_attempt(&evidence), MvAttemptDisposition::Ambiguous);
    }

    #[test]
    fn evidence_contradicting_the_lake_is_ambiguous() {
        // The provider says committed, but the target neither is nor descends
        // from this result. Never a cleanup decision.
        let evidence = AttemptEvidence {
            operation: AttemptOperationEvidence::KnownCommitted,
            ..base()
        };
        assert_eq!(classify_attempt(&evidence), MvAttemptDisposition::Ambiguous);
        assert!(!MvAttemptDisposition::Ambiguous.permits_artifact_reclaim());
    }

    #[test]
    fn a_missing_artifact_is_not_proof_it_never_committed() {
        // No fence has superseded us, staging is gone, and nothing resolved the
        // operation. The tempting conclusion is "abandoned"; it is unfounded.
        let evidence = AttemptEvidence {
            staging_present: false,
            ..base()
        };
        assert_eq!(classify_attempt(&evidence), MvAttemptDisposition::Ambiguous);
    }

    fn authorized() -> CleanupAuthorization {
        CleanupAuthorization {
            disposition: MvAttemptDisposition::Superseded,
            holds_current_ownership: true,
            acts_under_established_fence: true,
            observation_digest_matches: true,
            still_referenced_by_target: false,
            is_current_fence_artifact: false,
        }
    }

    #[test]
    fn cleanup_requires_every_condition_independently() {
        authorize_cleanup(&authorized()).unwrap();

        // A stale classification must not authorize a delete: by now this
        // frontend may have lost the lease, or the lake may have moved.
        assert_eq!(
            authorize_cleanup(&CleanupAuthorization {
                holds_current_ownership: false,
                ..authorized()
            }),
            Err(CleanupRefusal::NotOwner)
        );
        assert_eq!(
            authorize_cleanup(&CleanupAuthorization {
                acts_under_established_fence: false,
                ..authorized()
            }),
            Err(CleanupRefusal::NotFenced)
        );
        assert_eq!(
            authorize_cleanup(&CleanupAuthorization {
                observation_digest_matches: false,
                ..authorized()
            }),
            Err(CleanupRefusal::StaleObservation)
        );
    }

    #[test]
    fn no_disposition_short_of_settled_authorizes_a_delete() {
        for disposition in [
            MvAttemptDisposition::Staged,
            MvAttemptDisposition::Published,
            MvAttemptDisposition::CleanupPending,
            MvAttemptDisposition::Ambiguous,
        ] {
            assert_eq!(
                authorize_cleanup(&CleanupAuthorization {
                    disposition,
                    ..authorized()
                }),
                Err(CleanupRefusal::DispositionNotTerminal),
                "{disposition:?} must not authorize deleting artifacts"
            );
        }
    }

    #[test]
    fn referenced_data_and_the_current_fence_are_never_deletable() {
        // Even a fully authorized owner acting on a settled attempt may not
        // remove data the target still depends on.
        assert_eq!(
            authorize_cleanup(&CleanupAuthorization {
                still_referenced_by_target: true,
                ..authorized()
            }),
            Err(CleanupRefusal::StillReferenced)
        );
        // Deleting the fence would remove the ref that arbitrates publication.
        assert_eq!(
            authorize_cleanup(&CleanupAuthorization {
                is_current_fence_artifact: true,
                ..authorized()
            }),
            Err(CleanupRefusal::IsCurrentFence)
        );
    }

    #[test]
    fn ambiguity_halts_automatic_refresh_and_nothing_else_does() {
        assert!(MvAttemptDisposition::Ambiguous.halts_automatic_refresh());
        for disposition in [
            MvAttemptDisposition::KnownUncommitted,
            MvAttemptDisposition::Staged,
            MvAttemptDisposition::Published,
            MvAttemptDisposition::Superseded,
            MvAttemptDisposition::CleanupPending,
        ] {
            assert!(
                !disposition.halts_automatic_refresh(),
                "{disposition:?} must not stall a target"
            );
        }
    }
}
