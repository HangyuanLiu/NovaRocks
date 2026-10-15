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

//! The retained Hadoop update protocol, separated at its existing decisive
//! boundary. IRU-6c owns changing numbering, overwrite and hint semantics.

use super::*;
use crate::catalog::publication::EvidenceContext;
use crate::catalog::publication_dispatch::{
    DispatchCompletion, FinalizationWork, PreparationResult, PreparedCatalogDispatch,
    PreparedPublication,
};
use crate::catalog::transaction::{CommitProof, MetadataPredecessorObservation};
use crate::commit::attempt::observation::{DispatchEvidence, ProtocolObservation};
use crate::commit::model::FrozenRequest;
use novarocks_spi::connector::{
    ConnectorMutationFailure, ConnectorMutationFailureKind, ConnectorOperationControl,
    ConnectorRequestContext, ExternalMutationFinalization,
};

impl HadoopFileSystemCatalog {
    pub(crate) async fn prepare_frozen_update(
        self: Arc<Self>,
        request: FrozenRequest,
        preparation: ConnectorRequestContext,
    ) -> PreparationResult<PreparedPublication> {
        preparation.check_active()?;
        if !matches!(
            request.base(),
            crate::commit::model::BaseIdentity::Existing { .. }
        ) {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                "Hadoop existing-table publication requires an existing frozen base",
            )
            .into());
        }
        // No catalog I/O occurs before the journal's dispatch grant. The old
        // loader can repair a hint and must therefore run in that interval.
        Ok(PreparedPublication::Mutation {
            binding: None,
            dispatch: Box::new(HadoopPreparedUpdate {
                client: self,
                request,
            }),
        })
    }
}

struct HadoopPreparedUpdate {
    client: Arc<HadoopFileSystemCatalog>,
    request: FrozenRequest,
}
fn not_dispatched(error: Error) -> DispatchCompletion {
    DispatchCompletion::evidence(DispatchEvidence::NotDispatched {
        failure: crate::commit::attempt::before_dispatch_failure(&error),
    })
}
fn rejected(stage: &'static str, message: &'static str) -> DispatchCompletion {
    DispatchCompletion::evidence(DispatchEvidence::Rejected {
        failure: ConnectorMutationFailure::new(ConnectorMutationFailureKind::Conflict, message),
        observation: ProtocolObservation {
            adapter: "hadoop",
            stage,
            ..Default::default()
        },
    })
}
fn unknown(stage: &'static str, message: &'static str) -> DispatchCompletion {
    DispatchCompletion::evidence(DispatchEvidence::Unknown {
        failure: ConnectorMutationFailure::new(ConnectorMutationFailureKind::Unavailable, message),
        observation: ProtocolObservation {
            adapter: "hadoop",
            stage,
            ..Default::default()
        },
    })
}
#[async_trait]
impl PreparedCatalogDispatch for HadoopPreparedUpdate {
    async fn dispatch_once(self: Box<Self>, evidence: EvidenceContext) -> DispatchCompletion {
        // The catalog's existing FileIO and hint-repair loader are retained.
        // An already started local I/O is awaited to actual exit; deadline
        // checks prohibit starting the next step after the grant expires.
        let current = match self
            .client
            .load_existing_table_with_budget(self.request.identifier(), Some(evidence))
            .await
        {
            Ok(current) => current,
            Err(error) => return not_dispatched(error),
        };
        let crate::commit::model::BaseIdentity::Existing {
            uuid,
            metadata_location: base_location,
            ..
        } = self.request.base()
        else {
            unreachable!("prepared Hadoop update has an existing base")
        };
        if current.metadata().uuid() != *uuid
            || current.metadata_location() != Some(base_location.as_str())
        {
            return rejected(
                "base-identity",
                "Frozen Hadoop metadata location or table UUID no longer matches the current version",
            );
        }
        for requirement in self.request.requirements() {
            if requirement.check(Some(current.metadata())).is_err() {
                return rejected(
                    "requirements",
                    "Frozen Hadoop publication requirements no longer hold",
                );
            }
        }
        let predecessor = MetadataPredecessorObservation::Candidate {
            location: base_location.clone(),
            timestamp_ms: current.metadata().last_updated_ms(),
        };
        let mut builder = current
            .metadata()
            .clone()
            .into_builder(Some(base_location.clone()));
        for update in self.request.updates() {
            builder = match update.clone().apply(builder) {
                Ok(builder) => builder,
                Err(error) => return not_dispatched(error),
            };
        }
        let metadata = match builder.build() {
            Ok(result) => result.metadata,
            Err(error) => return not_dispatched(error),
        };
        let snapshot_id = match crate::catalog::dispatch::committed_snapshot_id(
            &metadata,
            self.request.target_ref(),
            self.request.ref_snapshot_after(self.request.target_ref()),
        ) {
            Ok(snapshot) => snapshot,
            Err(error) => return not_dispatched(error),
        };
        if let Err(error) = HadoopFileSystemCatalog::check_publication_budget(Some(evidence)) {
            return not_dispatched(error);
        }
        let table_location = current.metadata().location();
        // Preserve the old, second hint read and hint-based numbering. In the
        // absent/unparseable case the retained loader first repairs v1's hint.
        let version = self.client.read_version_hint(table_location).await;
        if let Err(error) = HadoopFileSystemCatalog::check_publication_budget(Some(evidence)) {
            return not_dispatched(error);
        }
        let next_version = match version.checked_add(1) {
            Some(version) => version,
            None => {
                return not_dispatched(Error::new(
                    ErrorKind::DataInvalid,
                    "Hadoop metadata version has exhausted its representable range",
                ));
            }
        };
        let metadata_location =
            HadoopFileSystemCatalog::metadata_path(table_location, next_version);
        let bytes = match serde_json::to_vec(&metadata) {
            Ok(bytes) => bytes,
            Err(error) => return not_dispatched(Error::from(error)),
        };
        let output = match self.client.file_io.new_output(&metadata_location) {
            Ok(output) => output,
            Err(error) => return not_dispatched(error),
        };
        if let Err(error) = HadoopFileSystemCatalog::check_publication_budget(Some(evidence)) {
            return not_dispatched(error);
        }
        if output.write(bytes.into()).await.is_err() {
            return unknown("metadata-write", "Hadoop metadata write outcome is unknown");
        }
        #[cfg(test)]
        if self
            .client
            .take_test_fault(HadoopCatalogTestFault::AfterMetadataWriteEvidenceExpiry)
        {
            tokio::time::sleep_until(evidence.deadline().into()).await;
        }
        if evidence.expired() {
            return unknown(
                "before-hint-write",
                "Hadoop metadata was written but the evidence deadline forbids starting its version hint",
            );
        }
        if self
            .client
            .write_version_hint(table_location, next_version)
            .await
            .is_err()
        {
            return unknown(
                "hint-write",
                "Hadoop metadata was written but version hint outcome is unknown",
            );
        }
        // Both old-protocol writes acknowledged success. No cancellation or
        // expiration check may discard this decisive evidence.
        let mut proof =
            CommitProof::applied(snapshot_id).with_table_uuid(metadata.uuid().to_string());
        proof.metadata_location = Some(Arc::from(metadata_location.as_str()));
        proof.metadata_predecessor = predecessor;
        DispatchCompletion {
            evidence: DispatchEvidence::Applied { proof },
            finalization: Some(Box::new(HadoopCacheFinalization {
                client: self.client,
                ident: self.request.identifier().clone(),
                location: metadata_location,
            })),
        }
    }
}
struct HadoopCacheFinalization {
    client: Arc<HadoopFileSystemCatalog>,
    ident: TableIdent,
    location: String,
}
#[async_trait]
impl FinalizationWork for HadoopCacheFinalization {
    async fn run(self: Box<Self>, context: EvidenceContext) -> ExternalMutationFinalization {
        if context.expired() {
            return ExternalMutationFinalization::Failed(ConnectorMutationFailure::new(
                ConnectorMutationFailureKind::DeadlineExceeded,
                "Hadoop publication cache finalization deadline elapsed",
            ));
        }
        match tokio::time::timeout_at(context.deadline().into(), self.client.tables.lock()).await {
            Ok(mut tables) => {
                if context.expired() {
                    return ExternalMutationFinalization::Failed(ConnectorMutationFailure::new(
                        ConnectorMutationFailureKind::DeadlineExceeded,
                        "Hadoop publication cache finalization deadline elapsed",
                    ));
                }
                tables.insert(
                    HadoopFileSystemCatalog::table_key(&self.ident),
                    self.location,
                );
                ExternalMutationFinalization::Complete
            }
            Err(_) => ExternalMutationFinalization::Failed(ConnectorMutationFailure::new(
                ConnectorMutationFailureKind::DeadlineExceeded,
                "Hadoop publication cache finalization deadline elapsed",
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::publication::CatalogEvidencePolicy;
    use crate::commit::attempt::observation::PublicationJournal;
    use crate::commit::model::{
        AttemptArtifacts, AttemptToken, BaseIdentity, FrozenRequestParts, OperationToken,
        RequestShape,
    };
    use novarocks_spi::connector::{
        ConnectorInstanceDescriptor, ConnectorInstanceId, ConnectorMutationOperationId,
        ConnectorProviderId, ConnectorStopOwner, ExternalMutationEvidence, ExternalMutationOutcome,
        ProviderBindingEpoch,
    };

    async fn fixture(
        location: &str,
    ) -> (
        Arc<HadoopFileSystemCatalog>,
        FrozenRequest,
        ConnectorRequestContext,
        PublicationJournal<()>,
    ) {
        let catalog = Arc::new(super::super::tests::test_catalog(location));
        let namespace = NamespaceIdent::new("db".into());
        let table = catalog
            .create_table(&namespace, super::super::tests::test_creation("t"))
            .await
            .unwrap();
        let operation =
            OperationToken::from_mutation(ConnectorMutationOperationId::from_bytes([6; 16]));
        let request = FrozenRequest::new(FrozenRequestParts {
            shape: RequestShape::MetadataOnly,
            target: table.identifier().clone(),
            target_ref: "main".into(),
            base: BaseIdentity::Existing {
                uuid: table.metadata().uuid(),
                parent: None,
                metadata_location: table.metadata_location().unwrap().into(),
            },
            requirements: vec![crate::iceberg::TableRequirement::UuidMatch {
                uuid: table.metadata().uuid(),
            }],
            updates: vec![crate::iceberg::TableUpdate::SetProperties {
                updates: HashMap::from([("test.publication".into(), "value".into())]),
            }],
            artifacts: AttemptArtifacts::empty(AttemptToken::new(operation, 0)),
        })
        .unwrap()
        .with_base_facts(table.metadata())
        .unwrap();
        let preparation = ConnectorRequestContext::try_new(
            std::time::Instant::now() + Duration::from_secs(5),
            ConnectorStopOwner::new().view(),
            64,
            128,
        )
        .unwrap();
        let journal = PublicationJournal::new(operation);
        journal
            .record(
                ExternalMutationEvidence::try_new(
                    2,
                    ConnectorInstanceDescriptor {
                        provider_id: ConnectorProviderId::parse("iceberg").unwrap(),
                        instance_id: ConnectorInstanceId::parse("hadoop-test").unwrap(),
                    },
                    ProviderBindingEpoch::from_bytes([2; 16]),
                    ConnectorMutationOperationId::from_bytes(operation.to_bytes()),
                    "alter_properties",
                    Bytes::from_static(b"{}"),
                )
                .unwrap(),
                (),
                &request,
            )
            .unwrap();
        (catalog, request, preparation, journal)
    }
    struct HoldCacheDispatch {
        inner: Box<dyn PreparedCatalogDispatch>,
        catalog: Arc<HadoopFileSystemCatalog>,
    }
    #[async_trait]
    impl PreparedCatalogDispatch for HoldCacheDispatch {
        async fn dispatch_once(self: Box<Self>, context: EvidenceContext) -> DispatchCompletion {
            let mut completion = self.inner.dispatch_once(context).await;
            if let Some(inner) = completion.finalization.take() {
                completion.finalization = Some(Box::new(HoldCacheFinalization {
                    inner,
                    catalog: self.catalog,
                }));
            }
            completion
        }
    }
    struct HoldCacheFinalization {
        inner: Box<dyn FinalizationWork>,
        catalog: Arc<HadoopFileSystemCatalog>,
    }
    #[async_trait]
    impl FinalizationWork for HoldCacheFinalization {
        async fn run(self: Box<Self>, context: EvidenceContext) -> ExternalMutationFinalization {
            let held = self.catalog.tables.lock().await;
            let result = self.inner.run(context).await;
            drop(held);
            result
        }
    }

    #[tokio::test]
    async fn update_hint_failure_stays_unknown_after_metadata_write_under_the_old_protocol() {
        let directory = tempfile::tempdir().unwrap();
        let (catalog, request, preparation, journal) =
            fixture(directory.path().to_str().unwrap()).await;
        catalog.inject_test_fault(HadoopCatalogTestFault::BeforeHintWrite);
        let entry = journal
            .publish_once(
                preparation,
                CatalogEvidencePolicy::default(),
                move |_, _, context| catalog.prepare_frozen_update(request, context),
            )
            .await
            .unwrap();
        assert!(matches!(
            journal.catalog_entry_outcome(entry),
            ExternalMutationOutcome::CommitUnknown { .. }
        ));
        let location = directory.path().join("db/t/metadata");
        assert!(location.join("v2.metadata.json").exists());
        assert_eq!(
            std::fs::read_to_string(location.join("version-hint.text"))
                .unwrap()
                .trim(),
            "1"
        );
        let metadata: serde_json::Value =
            serde_json::from_slice(&std::fs::read(location.join("v2.metadata.json")).unwrap())
                .unwrap();
        assert_eq!(metadata["properties"]["test.publication"], "value");
    }
    #[tokio::test]
    async fn update_both_writes_prove_applied_before_bounded_cache_finalization() {
        let directory = tempfile::tempdir().unwrap();
        let (catalog, request, preparation, journal) =
            fixture(directory.path().to_str().unwrap()).await;
        let owner = catalog.clone();
        let observed = journal.clone();
        let task = tokio::spawn(async move {
            observed
                .publish_once(
                    preparation,
                    CatalogEvidencePolicy {
                        finalization: Duration::from_millis(20),
                        ..Default::default()
                    },
                    move |_, _, context| async move {
                        let mut prepared = owner
                            .clone()
                            .prepare_frozen_update(request, context)
                            .await?;
                        prepared = match prepared {
                            PreparedPublication::Mutation { binding, dispatch } => {
                                PreparedPublication::Mutation {
                                    binding,
                                    dispatch: Box::new(HoldCacheDispatch {
                                        inner: dispatch,
                                        catalog: owner,
                                    }),
                                }
                            }
                            PreparedPublication::NoOp => panic!("existing update must dispatch"),
                        };
                        Ok(prepared)
                    },
                )
                .await
        });
        let entry = task.await.unwrap().unwrap();
        let ExternalMutationOutcome::KnownCommitted {
            finalization: ExternalMutationFinalization::Failed(_),
            ..
        } = journal.catalog_entry_outcome(entry)
        else {
            panic!("cache wait cannot downgrade acknowledged metadata and hint writes")
        };
        assert_eq!(
            std::fs::read_to_string(directory.path().join("db/t/metadata/version-hint.text"))
                .unwrap()
                .trim(),
            "2"
        );
        let proof = journal.commit_outcome(entry);
        assert!(matches!(
            proof,
            crate::catalog::error::CatalogOutcome::KnownCommitted {
                receipt: CommitProof {
                    metadata_predecessor: MetadataPredecessorObservation::Candidate { .. },
                    ..
                },
                ..
            }
        ));
    }
    #[tokio::test]
    async fn missing_or_unparseable_hint_keeps_v1_and_advances_to_v2() {
        for missing in [true, false] {
            let directory = tempfile::tempdir().unwrap();
            let (catalog, request, preparation, journal) =
                fixture(directory.path().to_str().unwrap()).await;
            let metadata = directory.path().join("db/t/metadata");
            let original = std::fs::read(metadata.join("v1.metadata.json")).unwrap();
            if missing {
                std::fs::remove_file(metadata.join("version-hint.text")).unwrap();
            } else {
                std::fs::write(metadata.join("version-hint.text"), "unparseable").unwrap();
            }
            let entry = journal
                .publish_once(
                    preparation,
                    CatalogEvidencePolicy::default(),
                    move |_, _, context| catalog.prepare_frozen_update(request, context),
                )
                .await
                .unwrap();
            assert!(matches!(
                journal.catalog_entry_outcome(entry),
                ExternalMutationOutcome::KnownCommitted { .. }
            ));
            assert_eq!(
                std::fs::read(metadata.join("v1.metadata.json")).unwrap(),
                original
            );
            assert!(metadata.join("v2.metadata.json").exists());
            assert_eq!(
                std::fs::read_to_string(metadata.join("version-hint.text"))
                    .unwrap()
                    .trim(),
                "2"
            );
        }
    }
    #[tokio::test]
    async fn frozen_location_or_uuid_change_is_a_definite_conflict_with_no_update_write() {
        for uuid_changed in [false, true] {
            let directory = tempfile::tempdir().unwrap();
            let (catalog, request, preparation, journal) =
                fixture(directory.path().to_str().unwrap()).await;
            let metadata = directory.path().join("db/t/metadata");
            let mut external: serde_json::Value =
                serde_json::from_slice(&std::fs::read(metadata.join("v1.metadata.json")).unwrap())
                    .unwrap();
            let version = if uuid_changed {
                external["table-uuid"] = serde_json::json!(uuid::Uuid::now_v7());
                1
            } else {
                2
            };
            let changed = serde_json::to_vec(&external).unwrap();
            std::fs::write(metadata.join(format!("v{version}.metadata.json")), &changed).unwrap();
            std::fs::write(metadata.join("version-hint.text"), format!("{version}\n")).unwrap();
            let entry = journal
                .publish_once(
                    preparation,
                    CatalogEvidencePolicy::default(),
                    move |_, _, context| catalog.prepare_frozen_update(request, context),
                )
                .await
                .unwrap();
            let ExternalMutationOutcome::KnownUncommitted { failure, .. } =
                journal.catalog_entry_outcome(entry)
            else {
                panic!("stale frozen base must be rejected")
            };
            assert_eq!(failure.kind(), ConnectorMutationFailureKind::Conflict);
            assert_eq!(
                std::fs::read(metadata.join(format!("v{version}.metadata.json"))).unwrap(),
                changed
            );
            assert!(
                !metadata
                    .join(format!("v{}.metadata.json", version + 1))
                    .exists()
            );
            assert_eq!(
                std::fs::read_to_string(metadata.join("version-hint.text"))
                    .unwrap()
                    .trim(),
                version.to_string()
            );
        }
    }
    #[tokio::test]
    async fn evidence_expiry_after_metadata_exit_forbids_starting_hint_write() {
        let directory = tempfile::tempdir().unwrap();
        let (catalog, request, preparation, journal) =
            fixture(directory.path().to_str().unwrap()).await;
        catalog.inject_test_fault(HadoopCatalogTestFault::AfterMetadataWriteEvidenceExpiry);
        let entry = journal
            .publish_once(
                preparation,
                CatalogEvidencePolicy {
                    evidence: Duration::from_secs(1),
                    ..Default::default()
                },
                move |_, _, context| catalog.prepare_frozen_update(request, context),
            )
            .await
            .unwrap();
        assert!(matches!(
            journal.catalog_entry_outcome(entry),
            ExternalMutationOutcome::CommitUnknown { .. }
        ));
        let metadata = directory.path().join("db/t/metadata");
        assert!(metadata.join("v2.metadata.json").exists());
        assert_eq!(
            std::fs::read_to_string(metadata.join("version-hint.text"))
                .unwrap()
                .trim(),
            "1"
        );
    }
}
