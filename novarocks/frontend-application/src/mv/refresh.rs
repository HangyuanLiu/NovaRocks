// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information regarding
// copyright ownership.  The ASF licenses this file to you under the
// Apache License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License.  You may obtain a copy of the
// License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Frontend execution of a lake-authoritative MV publication.

use std::sync::Arc;

use crate::mv::domain::application::{MvApplicationError, MvApplicationErrorKind};
use crate::query_execution::mv_native_write::MvRefreshProviderActivation;
use crate::query_execution::service::QueryExecutionService;
use novarocks_mv_application::ports::{MvProviderFailure, MvProviderFailureKind};
use novarocks_mv_application::product::MvProductError;
use novarocks_mv_application::publication::{
    MvRefreshPublicationFinalizationFacts, MvRefreshPublicationIntent,
};
use novarocks_spi::connector::{
    ConnectorControlRegistry, ConnectorRequestContext, ConnectorWriteReceipt,
    ExternalMutationEffect, ExternalMutationFinalization, ExternalMutationOutcome,
};

#[derive(Clone)]
pub(super) struct FrontendMvRefreshDependencies {
    pub(super) query_execution: QueryExecutionService,
    pub(super) connector_control: Arc<dyn ConnectorControlRegistry>,
    pub(super) provider_activation: Arc<dyn MvRefreshProviderActivation>,
    #[cfg(novarocks_dev_controls)]
    pub(super) test_controls: Option<Arc<crate::mv::test_controls::MvTestControls>>,
}

/// Bind the exact publication document this write promised when it opened.
///
/// The session opened with a declaration and no payload because P states the
/// post-write logical cardinality, which requires the completed write effect
/// and the admitted prior publication.
/// Building P here and binding it before finish is what makes the document and
/// the rows it describes one commit rather than two.
pub(crate) fn bind_publication_documents(
    intent: &MvRefreshPublicationIntent,
    admitted: &crate::mv::domain::staged_create::AdmittedMvDataPublication,
    session: &crate::query_execution::write_session::ConnectorWriteSession,
    logical_result_rows: Option<u64>,
) -> Result<(), MvApplicationError> {
    let declaration = session.pending_publication_declaration().map_err(|error| {
        MvApplicationError::from_connector(MvApplicationErrorKind::InvalidRequest, error)
    })?;
    let documents = admitted
        .publication_document_set(intent.publication_id(), logical_result_rows)
        .map_err(invalid)?;
    let publication = novarocks_spi::connector::KeyedPublicationIntent {
        declaration,
        documents,
    };
    session
        .bind_keyed_document_publication(publication)
        .map_err(|error| {
            MvApplicationError::from_connector(MvApplicationErrorKind::InvalidRequest, error)
        })
}

/// The one commit authority of one MV data write.
///
/// Every MV data write -- first refresh and incremental alike -- commits through
/// the write session that admitted it. It is still established explicitly
/// rather than assumed: a write whose data plane never closed carries no
/// session completion, and it is refused here instead of reaching the provider.
fn write_commit_authority(
    session: Option<crate::query_execution::outcome::ConnectorWriteSessionCompletion>,
) -> Result<crate::query_execution::outcome::ConnectorWriteSessionCompletion, MvApplicationError> {
    session.ok_or_else(|| invalid("MV refresh write completed without a write session"))
}

fn product_error(error: MvProductError) -> MvApplicationError {
    MvApplicationError::from_refresh_product(&error)
}

fn commit_known(
    authority: crate::query_execution::outcome::ConnectorWriteSessionCompletion,
    context: ConnectorRequestContext,
) -> Result<
    (
        novarocks_spi::connector::ExternalMutationEffect,
        ConnectorWriteReceipt,
    ),
    MvApplicationError,
> {
    // The commit is always performed, never gated on the write having produced
    // fragments. What an empty publication means -- terminate without an
    // external effect, or commit the empty write -- is a disposition the
    // publication carries and the provider applies at finish. Skipping the call
    // here would silently drop the commit of a full-overwrite refresh that
    // legitimately truncates its target.
    interpret_committed_write(
        crate::query_execution::write_session::finish_write_session(authority, context)
            .map(crate::query_execution::write_session::CommittedWriteSession::into_outcome),
    )
}

/// One reading of what a finished write session reported.
fn interpret_committed_write(
    outcome: Result<
        ExternalMutationOutcome<ConnectorWriteReceipt>,
        novarocks_spi::connector::ConnectorError,
    >,
) -> Result<
    (
        novarocks_spi::connector::ExternalMutationEffect,
        ConnectorWriteReceipt,
    ),
    MvApplicationError,
> {
    match outcome.map_err(|error| {
        MvApplicationError::from_connector(MvApplicationErrorKind::Engine, error)
    })? {
        ExternalMutationOutcome::KnownCommitted {
            cleanup,
            effect,
            receipt,
            finalization,
        } => {
            if let ExternalMutationFinalization::Failed(error) = &cleanup {
                tracing::warn!(%error, "committed MV write retains incomplete cleanup");
            }
            match finalization {
                ExternalMutationFinalization::Complete => Ok((effect, receipt)),
                ExternalMutationFinalization::Failed(error) => Err(MvApplicationError::new(
                    match effect {
                        ExternalMutationEffect::Applied => {
                            MvApplicationErrorKind::KnownCommittedFinalizeFailed
                        }
                        ExternalMutationEffect::NoOp => MvApplicationErrorKind::Engine,
                    },
                    error.to_string(),
                )
                .with_original_failure(mutation_original_failure(error))),
            }
        }
        ExternalMutationOutcome::KnownUncommitted { failure, cleanup } => {
            Err(MvApplicationError::new(
                MvApplicationErrorKind::Engine,
                crate::connector::mutation::known_uncommitted_message(&failure, &cleanup),
            )
            .with_original_failure(mutation_original_failure(failure)))
        }
        ExternalMutationOutcome::CommitUnknown { failure, .. } => Err(MvApplicationError::new(
            MvApplicationErrorKind::CommitUnknown,
            format!("MV refresh commit outcome is unknown: {failure}"),
        )
        .with_original_failure(mutation_original_failure(failure))),
    }
}

fn mutation_original_failure(
    failure: novarocks_spi::connector::ConnectorMutationFailure,
) -> novarocks_failure_contract::SharedFailureCause {
    if let Some(cause) = failure
        .connector_source()
        .and_then(|error| std::error::Error::source(error))
        .and_then(|source| source.downcast_ref::<novarocks_failure_contract::SharedFailureCause>())
    {
        return cause.clone();
    }
    novarocks_failure_contract::SharedFailureCause::new(failure)
}

/// Only the provider's actual Applied fact enters the committed MV tail.
/// Managed publications require a real commit even when no rows were written.
fn applied_publication_receipt(
    completed: (ExternalMutationEffect, ConnectorWriteReceipt),
) -> Result<ConnectorWriteReceipt, MvApplicationError> {
    match completed {
        (ExternalMutationEffect::Applied, receipt) => Ok(receipt),
        (ExternalMutationEffect::NoOp, _) => Err(invalid(
            "MV publication completed without its required external commit",
        )),
    }
}

fn committed_snapshot_id(
    published: &MvRefreshPublicationFinalizationFacts,
) -> Result<i64, MvApplicationError> {
    published
        .publication_version()
        .snapshot_id()
        .ok_or_else(|| committed_finalization("MV publication completed without a snapshot ID"))
}

fn committed_finalization(message: impl Into<String>) -> MvApplicationError {
    MvApplicationError::new(
        MvApplicationErrorKind::KnownCommittedFinalizeFailed,
        message,
    )
}

/// Preserve the original typed control cause while keeping the already
/// observed publication fact. This helper is used only after Applied.
#[cfg(any(test, novarocks_dev_controls))]
fn preserve_committed_finalization(error: MvApplicationError) -> MvApplicationError {
    let mut failure =
        committed_finalization(error.message()).with_compile_control(error.compile_control_error());
    if let Some(cause) = error.original_failure() {
        failure = failure.with_original_failure(cause.clone());
    }
    failure
}

fn invalid(message: impl Into<String>) -> MvApplicationError {
    MvApplicationError::new(MvApplicationErrorKind::InvalidRequest, message)
}

fn provider_failure(error: MvApplicationError) -> MvProviderFailure {
    let kind = match error.kind() {
        MvApplicationErrorKind::InvalidRequest => MvProviderFailureKind::InvalidRequest,
        MvApplicationErrorKind::Unavailable
        | MvApplicationErrorKind::AlreadyActive
        | MvApplicationErrorKind::ShutdownCancelled => MvProviderFailureKind::Unavailable,
        MvApplicationErrorKind::BindingInvalidated => MvProviderFailureKind::TargetReplaced,
        MvApplicationErrorKind::CommitUnknown => MvProviderFailureKind::CommitUnknown,
        MvApplicationErrorKind::KnownCommittedFinalizeFailed => {
            MvProviderFailureKind::KnownCommittedFinalizeFailed
        }
        MvApplicationErrorKind::Engine => MvProviderFailureKind::KnownUncommitted,
    };
    let mut failure = MvProviderFailure::new(kind, error.to_string())
        .with_compile_control(error.compile_control_error())
        .with_unavailable_implementation(error.unavailable_implementation().cloned());
    if let Some(cause) = error.original_failure() {
        failure = failure.with_original_failure(cause.clone());
    }
    failure
}
#[cfg(test)]
mod tests {
    use novarocks_mv_application::product::MvProductErrorKind;
    use std::sync::Arc;

    use novarocks_mv_application::publication::MvRefreshCommittedFacts;
    use novarocks_spi::connector::{
        ConnectorCommittedVersion, ConnectorTableObjectId, ExternalMutationEffect,
        ExternalMutationOutcome, LakePublicationId,
    };

    use super::*;
    use crate::query_execution::outcome::ConnectorWriteSessionCompletion;
    use crate::query_execution::write_barrier::WriteCommitBarrier;
    use crate::query_execution::write_result::DecodedPreparedWriteSet;
    use crate::query_execution::write_session::tests as write_session_tests;
    use novarocks_mv_application::publication::{
        MvRefreshPublicationBase, MvRefreshPublicationIntent, MvRefreshPublicationTechnique,
    };

    fn committed_version(snapshot_id: i64) -> ConnectorCommittedVersion {
        ConnectorCommittedVersion::try_new(
            bytes::Bytes::from_static(b"mv-publication-receipt"),
            Some(snapshot_id),
        )
        .expect("committed version")
    }

    /// What a publication that actually committed reports back.
    fn published_outcome(row_count: u64) -> ExternalMutationOutcome<ConnectorWriteReceipt> {
        ExternalMutationOutcome::KnownCommitted {
            cleanup: novarocks_spi::connector::ExternalMutationFinalization::Complete,
            effect: ExternalMutationEffect::Applied,
            receipt: ConnectorWriteReceipt::try_new_with_committed_facts(
                bytes::Bytes::from_static(b"mv-publication-receipt"),
                committed_version(42),
                Some(row_count),
            )
            .expect("receipt"),
            finalization: ExternalMutationFinalization::Complete,
        }
    }

    /// What a publication that declared `AbortWithoutExternalCommit` reports
    /// when its write produced nothing: the unchanged snapshot, and no
    /// committed facts at all. This mirrors the provider's
    /// `settle_empty_write_without_commit`.
    fn skipped_publication_outcome() -> ExternalMutationOutcome<ConnectorWriteReceipt> {
        ExternalMutationOutcome::KnownCommitted {
            cleanup: novarocks_spi::connector::ExternalMutationFinalization::Complete,
            effect: ExternalMutationEffect::NoOp,
            receipt: ConnectorWriteReceipt::try_new_with_committed_facts(
                bytes::Bytes::from_static(b"mv-publication-receipt"),
                committed_version(7),
                None,
            )
            .expect("receipt"),
            finalization: ExternalMutationFinalization::Complete,
        }
    }

    fn publication_intent() -> MvRefreshPublicationIntent {
        let publication_id = LakePublicationId::new_v7();
        let target_object_id =
            ConnectorTableObjectId::try_new(bytes::Bytes::from_static(b"mv-target-object"))
                .expect("target object id");
        MvRefreshPublicationIntent::try_new(
            publication_id,
            target_object_id.clone(),
            Some(7),
            crate::mv::domain::test_admission::publication_admission(
                "ice",
                "db",
                "mv",
                publication_id,
                &target_object_id,
            ),
            MvRefreshPublicationTechnique::Full,
            vec![
                MvRefreshPublicationBase::try_new(
                    0,
                    "ice.db.base".to_string(),
                    ConnectorTableObjectId::try_new(bytes::Bytes::from_static(b"base-object"))
                        .expect("base object id"),
                    None,
                    7,
                )
                .expect("base"),
            ],
            "fingerprint".to_string(),
            "ice".to_string(),
            "db".to_string(),
            "mv".to_string(),
        )
        .expect("publication intent")
    }

    fn session_completion(
        session: &Arc<crate::query_execution::write_session::ConnectorWriteSession>,
        row_count: u64,
        fragments: Vec<(
            novarocks_spi::connector::write_stack::WriteTargetOrdinal,
            Vec<u8>,
        )>,
    ) -> ConnectorWriteSessionCompletion {
        ConnectorWriteSessionCompletion::for_test(
            Arc::clone(session),
            DecodedPreparedWriteSet::for_test(row_count, fragments),
        )
    }

    fn ordinal(value: u32) -> novarocks_spi::connector::write_stack::WriteTargetOrdinal {
        novarocks_spi::connector::write_stack::WriteTargetOrdinal::try_new(value).expect("ordinal")
    }

    fn sole_target() -> novarocks_spi::connector::write_stack::WriteTargetOrdinal {
        ordinal(0)
    }

    /// The headline invariant: one write, one external commit, performed by the
    /// session that admitted it rather than by an operation session beside it.
    #[test]
    fn a_first_refresh_commits_exactly_once_through_its_write_session() {
        let fixture = write_session_tests::fixture_with_outcome(1, 16, published_outcome(7));
        let completion = session_completion(
            &fixture.session,
            7,
            vec![(sole_target(), write_session_tests::commit_fragment_bytes())],
        );

        let (effect, receipt) = commit_known(completion, write_session_tests::request_context())
            .expect("the write session performs the publication commit");

        assert_eq!(effect, ExternalMutationEffect::Applied);
        assert_eq!(receipt.resulting_row_count(), Some(7));
        assert_eq!(fixture.session.finish_invocations(), 1);
        assert_eq!(fixture.recorded.lock().expect("recorded").finish, 1);
    }

    /// A full-overwrite refresh that produced no rows is a truncate, and it must
    /// still commit. Whether an empty publication commits or terminates without
    /// an external effect is a disposition the publication carries and the
    /// provider applies at finish, so the terminal always asks.
    #[test]
    fn an_empty_first_refresh_still_reaches_its_publication_commit() {
        let fixture = write_session_tests::fixture_with_outcome(1, 16, published_outcome(0));
        let completion = session_completion(&fixture.session, 0, Vec::new());

        let (effect, _) = commit_known(completion, write_session_tests::request_context())
            .expect("an empty overwrite still commits");

        assert_eq!(effect, ExternalMutationEffect::Applied);
        assert_eq!(fixture.session.finish_invocations(), 1);
    }

    /// The same invariant for the last flow to move onto the session: an
    /// incremental refresh applies a change stream over several sealed branches
    /// -- a data branch and the delete branch retiring what it supersedes -- and
    /// commits all of them exactly once, through the session that sealed them.
    ///
    /// Before the migration this flow ran its writers under the session's
    /// recipes but committed through a staged-report operation beside it, so the
    /// write had a second commit authority the session never saw.
    #[test]
    fn an_incremental_refresh_commits_every_branch_once_through_its_write_session() {
        let fixture = write_session_tests::fixture_with_outcome(2, 16, published_outcome(7));
        let completion = session_completion(
            &fixture.session,
            7,
            vec![
                (ordinal(0), write_session_tests::commit_fragment_bytes()),
                (ordinal(1), write_session_tests::commit_fragment_bytes()),
            ],
        );

        let (effect, receipt) = commit_known(completion, write_session_tests::request_context())
            .expect("the write session performs the publication commit");

        assert_eq!(effect, ExternalMutationEffect::Applied);
        assert_eq!(receipt.resulting_row_count(), Some(7));
        // One commit for the whole refresh, not one per branch.
        assert_eq!(fixture.session.finish_invocations(), 1);
        assert_eq!(fixture.recorded.lock().expect("recorded").finish, 1);
    }

    /// An append-mode refresh whose window produced nothing is the common
    /// "nothing changed" case. Its session terminates without an external
    /// commit and reports the unchanged snapshot, so its receipt carries no
    /// committed facts -- and the terminal must decide on the effect before it
    /// tries to interpret them.
    #[test]
    fn an_empty_append_refresh_takes_the_waterline_path_without_interpreting_its_receipt() {
        let fixture =
            write_session_tests::fixture_with_outcome(1, 16, skipped_publication_outcome());
        let completion = session_completion(&fixture.session, 0, Vec::new());

        let (effect, receipt) = commit_known(completion, write_session_tests::request_context())
            .expect("a skipped publication is a successful no-op");

        assert_eq!(effect, ExternalMutationEffect::NoOp);
        assert!(receipt.resulting_row_count().is_none());
        // Interpreting this receipt -- which is what the terminal did before the
        // effect check was hoisted above it -- fails on exactly the fact a
        // skipped publication cannot have. The waterline path has to be taken
        // first.
        let error = MvRefreshCommittedFacts::from_write_receipt(publication_intent(), &receipt)
            .expect_err("a skipped publication has no committed facts to interpret");
        assert!(
            error.contains("without resulting row-count fact"),
            "unexpected interpretation failure: {error}"
        );
    }

    /// The dual barrier is what keeps a half-closed write away from the
    /// provider: no prepared write set means no completion, and no completion
    /// means the terminal cannot even name a commit authority.
    #[test]
    fn an_mv_write_whose_data_plane_never_closed_reaches_the_provider_zero_times() {
        let fixture = write_session_tests::fixture_with_outcome(1, 16, published_outcome(7));

        // Every participant succeeded, but no writer's prepared set ever
        // arrived, so the coordinator produces no completion for this write.
        let mut barrier = WriteCommitBarrier::new();
        barrier.observe_execution_terminals(true);
        assert!(barrier.into_committable().is_err());

        let Err(error) = write_commit_authority(None) else {
            panic!("a write with no completion has no commit authority");
        };

        assert_eq!(error.kind(), MvApplicationErrorKind::InvalidRequest);
        assert_eq!(fixture.session.finish_invocations(), 0);
        assert_eq!(fixture.recorded.lock().expect("recorded").finish, 0);
    }

    /// The same barrier for an incremental refresh, whose change stream fans out
    /// across several sealed branches: a write whose data plane never closed has
    /// no completion, so it can name no commit authority and the provider is
    /// never asked.
    #[test]
    fn an_incremental_write_whose_data_plane_never_closed_reaches_the_provider_zero_times() {
        let fixture = write_session_tests::fixture_with_outcome(2, 16, published_outcome(7));

        let mut barrier = WriteCommitBarrier::new();
        barrier.observe_execution_terminals(true);
        assert!(barrier.into_committable().is_err());

        let Err(error) = write_commit_authority(None) else {
            panic!("a write with no completion has no commit authority");
        };

        assert_eq!(error.kind(), MvApplicationErrorKind::InvalidRequest);
        assert_eq!(fixture.session.finish_invocations(), 0);
        assert_eq!(fixture.recorded.lock().expect("recorded").finish, 0);
    }

    /// The commit authority is the session completion itself, so the terminal
    /// never has to guess which data plane a write came from.
    #[test]
    fn a_session_completion_is_the_commit_authority() {
        let fixture = write_session_tests::fixture_with_outcome(1, 16, published_outcome(0));
        let completion = session_completion(&fixture.session, 0, Vec::new());

        assert!(write_commit_authority(Some(completion)).is_ok());
        assert_eq!(fixture.session.finish_invocations(), 0);
    }

    #[test]
    fn known_committed_publication_keeps_its_fact_when_projection_fails() {
        let error = product_error(MvProductError::new(
            MvProductErrorKind::KnownCommittedFinalizeFailed,
            "projector store unavailable",
        ));

        assert_eq!(
            error.kind(),
            MvApplicationErrorKind::KnownCommittedFinalizeFailed
        );
        assert!(error.message().contains("projector store unavailable"));
    }

    fn provider_resource_failure(
        message: &'static str,
    ) -> (
        novarocks_spi::connector::ConnectorMutationFailure,
        novarocks_failure_contract::SharedFailureCause,
    ) {
        let error = novarocks_spi::connector::ConnectorError::from_domain(
            novarocks_spi::connector::ConnectorErrorKind::ResourceExhausted,
            novarocks_workload_control::WorkError::Capacity(message),
        );
        let original = std::error::Error::source(&error)
            .and_then(|source| {
                source.downcast_ref::<novarocks_failure_contract::SharedFailureCause>()
            })
            .expect("actual Connector domain source")
            .clone();
        (
            novarocks_spi::connector::ConnectorMutationFailure::from_connector(error),
            original,
        )
    }

    fn assert_original_source_through_command(
        application: MvApplicationError,
        original: &novarocks_failure_contract::SharedFailureCause,
    ) {
        use novarocks_failure_contract::{FailureProjection, SharedFailureCause};
        assert_eq!(application.original_failure(), Some(original));
        let provider = provider_failure(application);
        assert_eq!(provider.original_failure(), Some(original));
        let product = provider.into_product_error();
        assert_eq!(product.original_failure(), Some(original));
        assert_eq!(
            std::error::Error::source(&product)
                .and_then(|source| source.downcast_ref::<SharedFailureCause>()),
            Some(original),
        );
        let application = product_error(product);
        assert_eq!(application.original_failure(), Some(original));
        let user = crate::mv::command::project_product_failure_for_test(application);
        assert_eq!(user.failure_cause(), Some(original));
        assert_eq!(
            user.failure_cause().unwrap().failure_fact(),
            original.failure_fact()
        );
        assert!(
            user.failure_cause()
                .unwrap()
                .downcast_ref::<novarocks_workload_control::WorkError>()
                .is_some()
        );
        assert_eq!(
            user.compile_control_error(),
            None,
            "a Provider cause is not compiler authority"
        );
    }

    #[test]
    fn actual_uncommitted_session_keeps_primary_source_when_cleanup_also_fails() {
        let (failure, original) = provider_resource_failure("provider publication refused");
        let (cleanup, cleanup_source) = provider_resource_failure("artifact cleanup refused");
        let outcome = ExternalMutationOutcome::KnownUncommitted {
            failure,
            cleanup: ExternalMutationFinalization::Failed(cleanup),
        };
        let fixture = write_session_tests::fixture_with_outcome(1, 16, outcome);
        let completion = session_completion(&fixture.session, 0, Vec::new());
        let failure = commit_known(completion, write_session_tests::request_context())
            .expect_err("actual session proves no commit");
        assert_eq!(failure.kind(), MvApplicationErrorKind::Engine);
        assert!(failure.message().contains("artifact cleanup refused"));
        assert_ne!(failure.original_failure(), Some(&cleanup_source));
        assert_original_source_through_command(failure, &original);
        assert_eq!(fixture.session.finish_invocations(), 1);
        assert_eq!(fixture.recorded.lock().unwrap().finish, 1);
    }

    #[test]
    fn terminal_connector_refusal_preserves_source_and_plain_mutation_keeps_author_fact() {
        use novarocks_failure_contract::FailureProjection;
        let connector = novarocks_spi::connector::ConnectorError::from_domain(
            novarocks_spi::connector::ConnectorErrorKind::ResourceExhausted,
            novarocks_workload_control::WorkError::Capacity("original terminal refusal"),
        );
        let original = std::error::Error::source(&connector)
            .and_then(|source| {
                source.downcast_ref::<novarocks_failure_contract::SharedFailureCause>()
            })
            .unwrap()
            .clone();
        let application = interpret_committed_write(Err(connector)).unwrap_err();
        assert_eq!(application.kind(), MvApplicationErrorKind::Engine);
        assert_original_source_through_command(application, &original);

        let failure = novarocks_spi::connector::ConnectorMutationFailure::new(
            novarocks_spi::connector::ConnectorMutationFailureKind::Unavailable,
            "ordinary provider failure without a domain wrapper",
        );
        let fact = failure.failure_fact();
        let application =
            interpret_committed_write(Ok(ExternalMutationOutcome::KnownUncommitted {
                failure,
                cleanup: ExternalMutationFinalization::Complete,
            }))
            .unwrap_err();
        let source = application
            .original_failure()
            .expect("terminal captured once");
        assert_eq!(source.failure_fact(), fact);
        assert!(
            source
                .downcast_ref::<novarocks_spi::connector::ConnectorMutationFailure>()
                .is_some()
        );
        assert_eq!(application.clone().original_failure(), Some(source));
        let user = crate::mv::command::project_product_failure_for_test(application.clone());
        assert_eq!(user.failure_cause(), Some(source));
        assert_eq!(user.compile_control_error(), None);
    }

    #[test]
    fn actual_write_finalization_failure_keeps_its_effect_through_product() {
        for (effect, expected) in [
            (
                ExternalMutationEffect::Applied,
                MvApplicationErrorKind::KnownCommittedFinalizeFailed,
            ),
            (ExternalMutationEffect::NoOp, MvApplicationErrorKind::Engine),
        ] {
            let mut outcome = published_outcome(0);
            let ExternalMutationOutcome::KnownCommitted {
                effect: actual_effect,
                finalization,
                ..
            } = &mut outcome
            else {
                unreachable!()
            };
            *actual_effect = effect;
            let (failure, original) =
                provider_resource_failure("necessary provider finalization failed");
            *finalization = ExternalMutationFinalization::Failed(failure);
            let fixture = write_session_tests::fixture_with_outcome(1, 16, outcome);
            let completion = session_completion(&fixture.session, 0, Vec::new());
            let failure = commit_known(completion, write_session_tests::request_context())
                .expect_err("actual session reports necessary finalization failure");
            let terminal = product_error(provider_failure(failure).into_product_error());
            assert_eq!(terminal.kind(), expected);
            assert!(
                terminal
                    .message()
                    .contains("necessary provider finalization failed")
            );
            assert_original_source_through_command(terminal, &original);
            assert_eq!(fixture.session.finish_invocations(), 1);
            assert_eq!(fixture.recorded.lock().unwrap().finish, 1);
        }
    }

    #[test]
    fn actual_applied_receipt_validation_failure_keeps_committed_fact() {
        // Both are valid generic Provider receipts. Only the actual MV proof
        // author can reject their missing MV-specific facts after finish.
        for (snapshot, rows) in [(Some(42), None), (None, Some(0))] {
            let receipt = ConnectorWriteReceipt::try_new_with_committed_facts(
                bytes::Bytes::from_static(b"mv-committed-receipt"),
                novarocks_spi::connector::ConnectorCommittedVersion::try_new(
                    bytes::Bytes::from_static(b"mv-committed-version"),
                    snapshot,
                )
                .unwrap(),
                rows,
            )
            .unwrap();
            let outcome = ExternalMutationOutcome::KnownCommitted {
                effect: ExternalMutationEffect::Applied,
                receipt,
                finalization: ExternalMutationFinalization::Complete,
                cleanup: ExternalMutationFinalization::Complete,
            };
            let fixture = write_session_tests::fixture_with_outcome(1, 16, outcome);
            let completion = session_completion(&fixture.session, 0, Vec::new());
            let receipt = applied_publication_receipt(
                commit_known(completion, write_session_tests::request_context()).unwrap(),
            )
            .unwrap();
            let finalization =
                MvRefreshCommittedFacts::from_write_receipt(publication_intent(), &receipt)
                    .map_err(committed_finalization)
                    .and_then(|committed| {
                        MvRefreshPublicationFinalizationFacts::try_new(
                            committed.intent().clone(),
                            committed.committed_version().clone(),
                        )
                        .map_err(committed_finalization)
                    })
                    .and_then(|published| committed_snapshot_id(&published));
            let failure = finalization.expect_err("MV-only postcommit proof is incomplete");
            let terminal = product_error(provider_failure(failure).into_product_error());
            assert_eq!(
                terminal.kind(),
                MvApplicationErrorKind::KnownCommittedFinalizeFailed
            );
            assert_eq!(fixture.recorded.lock().unwrap().finish, 1);
        }
    }

    #[test]
    fn actual_noop_and_unknown_do_not_gain_a_committed_mv_fact() {
        let fixture =
            write_session_tests::fixture_with_outcome(1, 16, skipped_publication_outcome());
        let completion = session_completion(&fixture.session, 0, Vec::new());
        let failure = applied_publication_receipt(
            commit_known(completion, write_session_tests::request_context()).unwrap(),
        )
        .expect_err("a managed MV publication requires a real Applied receipt");
        assert_eq!(failure.kind(), MvApplicationErrorKind::InvalidRequest);
        let terminal = product_error(provider_failure(failure).into_product_error());
        assert_eq!(terminal.kind(), MvApplicationErrorKind::InvalidRequest);
        assert_eq!(fixture.recorded.lock().unwrap().finish, 1);

        let (failure, original) = provider_resource_failure("provider outcome is unknown");
        let mut outcome = write_session_tests::commit_unknown();
        let ExternalMutationOutcome::CommitUnknown {
            failure: actual, ..
        } = &mut outcome
        else {
            unreachable!()
        };
        *actual = failure;
        let fixture = write_session_tests::fixture_with_outcome(1, 16, outcome);
        let completion = session_completion(&fixture.session, 0, Vec::new());
        let failure = commit_known(completion, write_session_tests::request_context())
            .expect_err("original Unknown is still unsettled");
        let terminal = product_error(provider_failure(failure).into_product_error());
        assert_eq!(terminal.kind(), MvApplicationErrorKind::CommitUnknown);
        assert_original_source_through_command(terminal, &original);
        assert_eq!(fixture.recorded.lock().unwrap().finish, 1);
    }

    #[test]
    fn product_refresh_publication_conflict_is_already_active() {
        let error = product_error(MvProductError::new(
            MvProductErrorKind::Conflict,
            "an MV publication is already active for this target",
        ));

        assert_eq!(error.kind(), MvApplicationErrorKind::AlreadyActive);
    }

    #[test]
    fn refresh_activation_source_survives_provider_product_and_command_bridges() {
        use novarocks_failure_contract::{FailureProjection, SharedFailureCause};
        use novarocks_sql::compiler::SqlCompileError;
        use novarocks_type_contract::CompileControlError;
        for original in [
            novarocks_plan_codec::PhysicalEncodeError::UnsupportedCapability("router".into()),
            novarocks_plan_codec::PhysicalEncodeError::Invalid("unsupported diagnostic".into()),
        ] {
            let source = SharedFailureCause::new(original);
            let fact = source.failure_fact();
            let application = MvApplicationError::from_compile(SqlCompileError::CompilationSource(
                source.clone(),
            ));
            let terminal = product_error(provider_failure(application).into_product_error());
            assert_eq!(terminal.kind(), MvApplicationErrorKind::InvalidRequest);
            assert_eq!(terminal.original_failure(), Some(&source));
            assert_eq!(terminal.unavailable_implementation(), None);
            let Some(CompileControlError::Source(carried)) = terminal.compile_control_error()
            else {
                panic!("the provider/product bridge must retain the compile source");
            };
            assert_eq!(carried, source);
            assert_eq!(carried.failure_fact(), fact);
            let committed = preserve_committed_finalization(terminal.clone());
            let committed = product_error(provider_failure(committed).into_product_error());
            assert_eq!(
                committed.kind(),
                MvApplicationErrorKind::KnownCommittedFinalizeFailed
            );
            let Some(CompileControlError::Source(carried)) = committed.compile_control_error()
            else {
                panic!("postcommit classification must preserve the original typed source");
            };
            assert_eq!(carried, source);
            assert_eq!(carried.failure_fact(), fact);
            assert_eq!(committed.original_failure(), Some(&source));
            let user = crate::mv::command::project_product_failure_for_test(terminal);
            assert_eq!(user.failure_cause(), Some(&source));
            let Some(CompileControlError::Source(carried)) = user.compile_control_error() else {
                panic!("the final command must retain the compile source");
            };
            assert_eq!(carried, source);
            assert_eq!(carried.failure_fact(), fact);
        }
    }

    #[test]
    fn refresh_activation_refusal_survives_provider_product_and_frontend_bridges() {
        use novarocks_functions::{FunctionBindingError, FunctionOverloadId};
        use novarocks_sql::{analyze_error::AnalyzeError, compiler::SqlCompileError};
        let compile = SqlCompileError::from(FunctionBindingError::UnavailableImplementation(
            FunctionOverloadId::try_new("fixture/mv/activation/missing-pure-v1").unwrap(),
        ));
        let SqlCompileError::Analyze(expected) = &compile else {
            panic!("missing pure implementation must be typed analysis");
        };
        let application = MvApplicationError::from_compile(compile.clone());
        let terminal = product_error(provider_failure(application).into_product_error());
        assert_eq!(terminal.kind(), MvApplicationErrorKind::InvalidRequest);
        assert_eq!(terminal.unavailable_implementation(), Some(expected));
        assert_eq!(terminal.compile_control_error(), None);

        let ordinary = MvApplicationError::from_compile(SqlCompileError::Analyze(
            AnalyzeError::internal("unsupported diagnostic: UnavailableImplementation"),
        ));
        let ordinary = product_error(provider_failure(ordinary).into_product_error());
        assert_eq!(ordinary.kind(), MvApplicationErrorKind::InvalidRequest);
        assert_eq!(ordinary.unavailable_implementation(), None);
        assert_eq!(
            ordinary.message(),
            "unsupported diagnostic: UnavailableImplementation"
        );
    }
}
