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

//! Conditional v1 publication retains the original protocol and separates its
//! decisive storage fact from hint, cache and table materialization.
use super::publication::{not_dispatched, unknown};
use super::*;
use crate::catalog::publication::EvidenceContext;
use crate::catalog::publication_dispatch::{
    DispatchCompletion, FinalizationWork, PreparationResult, PreparedCatalogDispatch,
    PreparedPublication,
};
use crate::catalog::transaction::CommitProof;
use crate::commit::attempt::observation::{DispatchEvidence, ProtocolObservation};
use novarocks_spi::connector::{
    ConnectorMutationFailure, ConnectorMutationFailureKind, ConnectorOperationControl,
    ConnectorRequestContext, ExternalMutationFinalization,
};

impl HadoopFileSystemCatalog {
    pub(crate) fn prepare_conditional_create(
        self: Arc<Self>,
        attempt: HadoopCreateAttempt,
        no_op_if_exists: bool,
        context: &ConnectorRequestContext,
    ) -> PreparationResult<PreparedPublication> {
        context.check_active()?;
        let binding = self.binding.as_ref().ok_or_else(|| {
            Error::new(
                ErrorKind::DataInvalid,
                "Hadoop catalog has no admitted filesystem capability",
            )
        })?;
        let access =
            crate::fs_io::resolve_access_for_location(&attempt.facts.metadata_location, binding)
                .map_err(|message| Error::new(ErrorKind::DataInvalid, message))?;
        if !access.supports_conditional_create() {
            return Err(Error::new(
                ErrorKind::FeatureUnsupported,
                "Hadoop catalog storage does not support native conditional create",
            )
            .into());
        }
        context.check_active()?;
        Ok(PreparedPublication::Mutation {
            binding: None,
            dispatch: Box::new(HadoopPreparedCreate {
                client: self,
                attempt,
                no_op_if_exists,
                access,
            }),
        })
    }
}
struct HadoopPreparedCreate {
    client: Arc<HadoopFileSystemCatalog>,
    attempt: HadoopCreateAttempt,
    no_op_if_exists: bool,
    access: crate::fs_io::IcebergFsAccess,
}
fn refused(kind: ConnectorMutationFailureKind, message: impl Into<String>) -> DispatchCompletion {
    DispatchCompletion::evidence(DispatchEvidence::Rejected {
        failure: ConnectorMutationFailure::new(kind, message.into()),
        observation: ProtocolObservation {
            adapter: "hadoop",
            stage: "conditional-create-exists",
            ..Default::default()
        },
    })
}
impl HadoopPreparedCreate {
    async fn read_exact(&self, context: EvidenceContext) -> Result<(TableMetadata, String)> {
        #[cfg(test)]
        if self
            .client
            .take_test_fault(HadoopCatalogTestFault::AuthoritativeV1Read)
        {
            return Err(Error::new(
                ErrorKind::Unexpected,
                "injected authoritative Hadoop v1 reread failure",
            ));
        }
        for index in 0..AUTHORITATIVE_V1_READ_ATTEMPTS {
            HadoopFileSystemCatalog::check_publication_budget(Some(context))?;
            let result = async {
                let input = self
                    .client
                    .file_io
                    .new_input(&self.attempt.facts.metadata_location)?;
                let bytes = input.read().await?;
                let metadata =
                    serde_json::from_slice::<TableMetadata>(&bytes).map_err(|error| {
                        Error::new(
                            ErrorKind::DataInvalid,
                            "Existing Hadoop canonical metadata is invalid",
                        )
                        .with_source(
                            novarocks_spi::connector::ConnectorError::new(
                                novarocks_spi::connector::ConnectorErrorKind::CorruptData,
                                error.to_string(),
                            ),
                        )
                    })?;
                Ok((metadata, hex_digest(&bytes)))
            }
            .await;
            match result {
                Ok(value) => return Ok(value),
                Err(error) if index + 1 == AUTHORITATIVE_V1_READ_ATTEMPTS => return Err(error),
                Err(_) => {
                    HadoopFileSystemCatalog::check_publication_budget(Some(context))?;
                    tokio::time::sleep_until(std::cmp::min(
                        tokio::time::Instant::now() + AUTHORITATIVE_V1_READ_RETRY_DELAY,
                        context.deadline().into(),
                    ))
                    .await;
                }
            }
        }
        unreachable!("bounded read returns its final result")
    }
    fn applied(self: Box<Self>) -> DispatchCompletion {
        let proof = CommitProof::applied(None)
            .with_table_uuid(self.attempt.facts.table_uuid.clone())
            .with_metadata(
                self.attempt.facts.metadata_location.clone(),
                self.attempt.facts.metadata_digest.clone(),
            );
        DispatchCompletion {
            evidence: DispatchEvidence::Applied { proof },
            finalization: Some(Box::new(HadoopCreateFinalization {
                client: self.client,
                attempt: self.attempt,
            })),
        }
    }
}
#[async_trait]
impl PreparedCatalogDispatch for HadoopPreparedCreate {
    async fn dispatch_once(self: Box<Self>, context: EvidenceContext) -> DispatchCompletion {
        if let Err(error) = HadoopFileSystemCatalog::check_publication_budget(Some(context)) {
            return not_dispatched(error);
        }
        if let Err(message) = self.access.ensure_parent_directory().await {
            return not_dispatched(Error::new(ErrorKind::Unexpected, message));
        }
        if let Err(error) = HadoopFileSystemCatalog::check_publication_budget(Some(context)) {
            return not_dispatched(error);
        }
        #[cfg(test)]
        if self
            .client
            .take_test_fault(HadoopCatalogTestFault::BeforeConditionalRequest)
        {
            return not_dispatched(Error::new(
                ErrorKind::Unexpected,
                "injected failure before Hadoop conditional create request",
            ));
        }
        // A started local operation is awaited to actual exit. Statement stop
        // has no authority to discard its acknowledged result.
        let conditional = self
            .access
            .handle()
            .create_if_absent(
                0,
                self.attempt.metadata_bytes.clone(),
                &FileCancellation::new(),
            )
            .await;
        #[cfg(test)]
        let conditional = match conditional {
            Ok(ConditionalCreateOutcome::Created)
                if self
                    .client
                    .take_test_fault(HadoopCatalogTestFault::AfterConditionalResponseLoss) =>
            {
                Err(FileError::new(
                    FileErrorKind::Transient,
                    "injected conditional create response loss",
                ))
            }
            Ok(ConditionalCreateOutcome::AlreadyExists)
                if self.client.take_test_fault(
                    HadoopCatalogTestFault::AfterExistingConditionalResponseLoss,
                ) =>
            {
                Err(FileError::new(
                    FileErrorKind::Transient,
                    "injected conditional existing response loss",
                ))
            }
            Ok(_)
                if self
                    .client
                    .take_test_fault(HadoopCatalogTestFault::AfterConditionalUnsupported) =>
            {
                Err(FileError::new(
                    FileErrorKind::Unsupported,
                    "injected unsupported response after conditional storage operation",
                ))
            }
            value => value,
        };
        let definitely_exists = match conditional {
            Ok(ConditionalCreateOutcome::Created) => return self.applied(),
            Ok(ConditionalCreateOutcome::AlreadyExists) => true,
            Err(_) => false,
        };
        #[cfg(test)]
        if self
            .client
            .take_test_fault(HadoopCatalogTestFault::BeforeConditionalReadEvidenceExpiry)
        {
            tokio::time::sleep_until(context.deadline().into()).await;
        }
        match self.read_exact(context).await {
            Ok((metadata, digest))
                if metadata.uuid().to_string() == self.attempt.facts.table_uuid
                    && digest == self.attempt.facts.metadata_digest =>
            {
                self.applied()
            }
            Ok(_) if definitely_exists && self.no_op_if_exists => {
                DispatchCompletion::evidence(DispatchEvidence::Applied {
                    proof: CommitProof::no_op(),
                })
            }
            Ok(_) if definitely_exists => refused(
                ConnectorMutationFailureKind::AlreadyExists,
                "Iceberg table already exists",
            ),
            Err(error) if definitely_exists => {
                let failure = crate::commit::attempt::before_dispatch_failure(&error);
                refused(failure.kind(), failure.to_string())
            }
            Err(error) => super::publication::unknown_error("conditional-create-readback", error),
            _ => unknown(
                "conditional-create-readback",
                "Hadoop conditional create has no exact positive acknowledgement or readback",
            ),
        }
    }
}
struct HadoopCreateFinalization {
    client: Arc<HadoopFileSystemCatalog>,
    attempt: HadoopCreateAttempt,
}
#[async_trait]
impl FinalizationWork for HadoopCreateFinalization {
    async fn run(self: Box<Self>, context: EvidenceContext) -> ExternalMutationFinalization {
        let failed = |error: Error| {
            ExternalMutationFinalization::Failed(crate::commit::attempt::before_dispatch_failure(
                &error,
            ))
        };
        if let Err(error) = HadoopFileSystemCatalog::check_publication_budget(Some(context)) {
            return failed(error);
        }
        if let Err(error) = self
            .client
            .write_version_hint(&self.attempt.table_location, 1)
            .await
        {
            return failed(error);
        }
        if let Err(error) = HadoopFileSystemCatalog::check_publication_budget(Some(context)) {
            return failed(error);
        }
        let mut tables =
            match tokio::time::timeout_at(context.deadline().into(), self.client.tables.lock())
                .await
            {
                Ok(tables) => tables,
                Err(_) => {
                    return ExternalMutationFinalization::Failed(ConnectorMutationFailure::new(
                        ConnectorMutationFailureKind::DeadlineExceeded,
                        "Hadoop create cache finalization deadline elapsed",
                    ));
                }
            };
        if let Err(error) = HadoopFileSystemCatalog::check_publication_budget(Some(context)) {
            return failed(error);
        }
        tables.insert(
            HadoopFileSystemCatalog::table_key(&self.attempt.ident),
            self.attempt.facts.metadata_location.clone(),
        );
        drop(tables);
        if let Err(error) = HadoopFileSystemCatalog::check_publication_budget(Some(context)) {
            return failed(error);
        }
        match self.client.build_table(
            self.attempt.ident,
            self.attempt.metadata,
            self.attempt.facts.metadata_location,
        ) {
            Ok(_) => ExternalMutationFinalization::Complete,
            Err(error) => failed(error),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::publication::CatalogEvidencePolicy;
    use crate::commit::attempt::observation::{PublicationJournal, PublicationReceipts};
    use crate::commit::model::OperationToken;
    use crate::commit::publication_descriptor::AttributionPlan;
    use novarocks_spi::connector::{
        ConnectorInstanceDescriptor, ConnectorInstanceId, ConnectorMutationOperationId,
        ConnectorProviderId, ConnectorStopOwner, ExternalMutationEffect, ExternalMutationEvidence,
        ExternalMutationOutcome, ProviderBindingEpoch,
    };

    async fn fixture(
        location: &str,
        conditional: bool,
        existing: bool,
    ) -> (
        Arc<HadoopFileSystemCatalog>,
        HadoopCreateAttempt,
        ConnectorRequestContext,
        PublicationJournal<String>,
    ) {
        let catalog = Arc::new(super::super::tests::test_catalog(location));
        let namespace = NamespaceIdent::new("db".into());
        if existing {
            catalog
                .create_table(&namespace, super::super::tests::test_creation("t"))
                .await
                .unwrap();
        }
        let operation = ConnectorMutationOperationId::from_bytes([31; 16]);
        let attempt = catalog
            .prepare_create_attempt(
                &namespace,
                super::super::tests::test_creation("t"),
                uuid::Uuid::from_bytes(operation.to_bytes())
                    .simple()
                    .to_string(),
            )
            .unwrap();
        let context = ConnectorRequestContext::try_new(
            std::time::Instant::now() + Duration::from_secs(5),
            ConnectorStopOwner::new().view(),
            65536,
            131072,
        )
        .unwrap();
        let journal = PublicationJournal::new(OperationToken::from_mutation(operation));
        journal
            .prepare_catalog_with_receipts(
                ExternalMutationEvidence::try_new(
                    2,
                    ConnectorInstanceDescriptor {
                        provider_id: ConnectorProviderId::parse("iceberg").unwrap(),
                        instance_id: ConnectorInstanceId::parse("hadoop-create-test").unwrap(),
                    },
                    ProviderBindingEpoch::from_bytes([3; 16]),
                    operation,
                    "create_table",
                    Bytes::from_static(b"{}"),
                )
                .unwrap(),
                PublicationReceipts::for_effects(
                    attempt.facts.table_uuid.clone(),
                    "no-op-without-attempt-identity".into(),
                ),
                vec![format!("conditional={conditional}")],
                attempt.metadata_bytes.to_vec(),
                AttributionPlan::ExactMetadata {
                    table_uuid: attempt.metadata.uuid(),
                    location: attempt.facts.metadata_location.clone(),
                    digest: attempt.facts.metadata_digest.clone(),
                },
                context.clone(),
            )
            .unwrap();
        (catalog, attempt, context, journal)
    }
    async fn publish(
        catalog: Arc<HadoopFileSystemCatalog>,
        attempt: HadoopCreateAttempt,
        conditional: bool,
        context: ConnectorRequestContext,
        journal: &PublicationJournal<String>,
    ) -> ExternalMutationOutcome<String> {
        let entry = journal
            .publish_once(
                context,
                CatalogEvidencePolicy::default(),
                move |_, _, context| async move {
                    catalog.prepare_conditional_create(attempt, conditional, &context)
                },
            )
            .await
            .unwrap();
        journal.catalog_entry_outcome(entry)
    }
    #[tokio::test]
    async fn created_ack_and_exact_response_loss_keep_the_frozen_identity() {
        for loss in [false, true] {
            let directory = tempfile::tempdir().unwrap();
            let (catalog, attempt, context, journal) =
                fixture(directory.path().to_str().unwrap(), false, false).await;
            let facts = attempt.facts.clone();
            let bytes = attempt.metadata_bytes.clone();
            if loss {
                catalog.inject_test_fault(HadoopCatalogTestFault::AfterConditionalResponseLoss);
            }
            assert!(
                matches!(publish(catalog,attempt,false,context,&journal).await,
                ExternalMutationOutcome::KnownCommitted {effect:ExternalMutationEffect::Applied,receipt,finalization:ExternalMutationFinalization::Complete} if receipt==facts.table_uuid)
            );
            assert_eq!(std::fs::read(facts.metadata_location).unwrap(), bytes);
            assert_eq!(
                std::fs::read_to_string(directory.path().join("db/t/metadata/version-hint.text"))
                    .unwrap(),
                "1\n"
            );
        }
    }
    #[tokio::test]
    async fn explicit_existing_is_policy_bound_but_ambiguous_foreign_is_always_unknown() {
        for conditional in [false, true] {
            for ambiguous in [false, true] {
                let directory = tempfile::tempdir().unwrap();
                let (catalog, attempt, context, journal) =
                    fixture(directory.path().to_str().unwrap(), conditional, true).await;
                let before = std::fs::read(&attempt.facts.metadata_location).unwrap();
                let location = attempt.facts.metadata_location.clone();
                if ambiguous {
                    catalog.inject_test_fault(
                        HadoopCatalogTestFault::AfterExistingConditionalResponseLoss,
                    );
                }
                let outcome = publish(catalog, attempt, conditional, context, &journal).await;
                match (ambiguous, conditional, outcome) {
                    (true, _, ExternalMutationOutcome::CommitUnknown { .. }) => (),
                    (
                        false,
                        true,
                        ExternalMutationOutcome::KnownCommitted {
                            effect: ExternalMutationEffect::NoOp,
                            receipt,
                            ..
                        },
                    ) => assert_eq!(receipt, "no-op-without-attempt-identity"),
                    (false, false, ExternalMutationOutcome::KnownUncommitted { failure, .. }) => {
                        assert_eq!(failure.kind(), ConnectorMutationFailureKind::AlreadyExists)
                    }
                    (_, _, other) => panic!("unexpected conditional outcome: {other:?}"),
                }
                assert_eq!(std::fs::read(location).unwrap(), before);
            }
        }
    }
    #[tokio::test]
    async fn failed_readback_cannot_erase_explicit_non_creation_or_invent_commit() {
        for existing in [false, true] {
            let directory = tempfile::tempdir().unwrap();
            let (catalog, attempt, context, journal) =
                fixture(directory.path().to_str().unwrap(), true, existing).await;
            catalog.inject_test_fault(HadoopCatalogTestFault::AuthoritativeV1Read);
            if !existing {
                catalog.inject_test_fault(HadoopCatalogTestFault::AfterConditionalResponseLoss);
            }
            let outcome = publish(catalog, attempt, true, context, &journal).await;
            if existing {
                assert!(matches!(
                    outcome,
                    ExternalMutationOutcome::KnownUncommitted { .. }
                ));
            } else {
                assert!(matches!(
                    outcome,
                    ExternalMutationOutcome::CommitUnknown { .. }
                ));
            }
        }
    }
    #[tokio::test]
    async fn hint_failure_is_finalization_after_a_decisive_create() {
        for fault in [
            HadoopCatalogTestFault::BeforeHintWrite,
            HadoopCatalogTestFault::AfterHintWriteResponseLoss,
        ] {
            let directory = tempfile::tempdir().unwrap();
            let (catalog, attempt, context, journal) =
                fixture(directory.path().to_str().unwrap(), false, false).await;
            let uuid = attempt.facts.table_uuid.clone();
            catalog.inject_test_fault(fault);
            assert!(
                matches!(publish(catalog,attempt,false,context,&journal).await,ExternalMutationOutcome::KnownCommitted{effect:ExternalMutationEffect::Applied, receipt, finalization:ExternalMutationFinalization::Failed(_)} if receipt==uuid)
            );
            assert!(
                directory
                    .path()
                    .join("db/t/metadata/v1.metadata.json")
                    .exists()
            );
        }
    }
    #[tokio::test]
    async fn definite_existing_preserves_the_readback_deadline_kind() {
        let directory = tempfile::tempdir().unwrap();
        let (catalog, attempt, context, journal) =
            fixture(directory.path().to_str().unwrap(), true, true).await;
        catalog.inject_test_fault(HadoopCatalogTestFault::BeforeConditionalReadEvidenceExpiry);
        let policy = CatalogEvidencePolicy {
            evidence: Duration::from_millis(10),
            ..Default::default()
        };
        let entry = journal
            .publish_once(context, policy, move |_, _, context| async move {
                catalog.prepare_conditional_create(attempt, true, &context)
            })
            .await
            .unwrap();
        assert!(
            matches!(journal.catalog_entry_outcome(entry), ExternalMutationOutcome::KnownUncommitted { failure, .. } if failure.kind() == ConnectorMutationFailureKind::DeadlineExceeded)
        );
    }
    #[tokio::test]
    async fn create_cache_wait_cannot_restart_finalization_or_erase_applied() {
        let directory = tempfile::tempdir().unwrap();
        let (catalog, attempt, context, journal) =
            fixture(directory.path().to_str().unwrap(), false, false).await;
        let uuid = attempt.facts.table_uuid.clone();
        let held = catalog.tables.lock().await;
        let client = Arc::clone(&catalog);
        let entry = journal
            .publish_once(
                context,
                CatalogEvidencePolicy {
                    finalization: Duration::from_millis(10),
                    ..Default::default()
                },
                move |_, _, context| async move {
                    client.prepare_conditional_create(attempt, false, &context)
                },
            )
            .await
            .unwrap();
        assert!(
            matches!(journal.catalog_entry_outcome(entry), ExternalMutationOutcome::KnownCommitted { effect: ExternalMutationEffect::Applied, receipt, finalization: ExternalMutationFinalization::Failed(failure) } if receipt == uuid && failure.kind() == ConnectorMutationFailureKind::DeadlineExceeded)
        );
        drop(held);
        assert!(
            directory
                .path()
                .join("db/t/metadata/v1.metadata.json")
                .exists()
        );
    }
    #[tokio::test]
    async fn unsupported_after_conditional_storage_is_not_a_negative_proof() {
        for existing in [false, true] {
            for readable in [false, true] {
                let directory = tempfile::tempdir().unwrap();
                let (catalog, attempt, context, journal) =
                    fixture(directory.path().to_str().unwrap(), true, existing).await;
                let bytes = attempt.metadata_bytes.clone();
                let facts = attempt.facts.clone();
                let before = std::fs::read(&facts.metadata_location).ok();
                catalog.inject_test_fault(HadoopCatalogTestFault::AfterConditionalUnsupported);
                if !readable {
                    catalog.inject_test_fault(HadoopCatalogTestFault::AuthoritativeV1Read);
                }
                let outcome = publish(catalog, attempt, true, context, &journal).await;
                if !existing && readable {
                    assert!(
                        matches!(outcome, ExternalMutationOutcome::KnownCommitted { effect: ExternalMutationEffect::Applied, receipt, .. } if receipt == facts.table_uuid)
                    );
                } else {
                    assert!(
                        matches!(outcome, ExternalMutationOutcome::CommitUnknown { .. }),
                        "a returned storage error has no not-dispatched stage: {outcome:?}"
                    );
                }
                assert_eq!(
                    std::fs::read(&facts.metadata_location).unwrap(),
                    before.unwrap_or(bytes.to_vec())
                );
            }
        }
    }

    #[tokio::test]
    async fn malformed_existing_v1_is_corrupt_after_definite_exists_but_unknown_after_response_loss()
     {
        for ambiguous in [false, true] {
            let directory = tempfile::tempdir().unwrap();
            let (catalog, attempt, context, journal) =
                fixture(directory.path().to_str().unwrap(), true, true).await;
            std::fs::write(&attempt.facts.metadata_location, b"{").unwrap();
            if ambiguous {
                catalog.inject_test_fault(
                    HadoopCatalogTestFault::AfterExistingConditionalResponseLoss,
                );
            }
            let outcome = publish(catalog, attempt, true, context, &journal).await;
            if ambiguous {
                assert!(matches!(
                    outcome,
                    ExternalMutationOutcome::CommitUnknown { .. }
                ));
            } else {
                assert!(
                    matches!(outcome, ExternalMutationOutcome::KnownUncommitted { failure, .. } if failure.kind() == ConnectorMutationFailureKind::CorruptData)
                );
            }
            assert_eq!(
                std::fs::read(directory.path().join("db/t/metadata/v1.metadata.json")).unwrap(),
                b"{"
            );
        }
    }
}
