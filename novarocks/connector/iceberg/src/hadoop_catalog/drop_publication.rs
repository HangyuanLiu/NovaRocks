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

//! DROP owns the complete frozen version set. The last maximum-version
//! anchor is removed last; it never deletes data or grants object reclamation.
use super::publication::{
    not_dispatched, original_binding, phase_binding, rejected, unknown_error,
};
use super::*;
use crate::access_binding::IcebergReadBinding;
use crate::catalog::publication::EvidenceContext;
use crate::catalog::publication_dispatch::{
    DispatchCompletion, PreparationFailure, PreparationResult, PreparedCatalogDispatch,
    PreparedPublication,
};
use crate::catalog::transaction::CommitProof;
use crate::commit::attempt::observation::{DispatchEvidence, ProtocolObservation};
use novarocks_spi::connector::{
    ConnectorMutationFailure, ConnectorMutationFailureKind, ConnectorOperationControl,
    ConnectorRequestContext,
};
impl HadoopFileSystemCatalog {
    pub(crate) async fn prepare_table_drop(
        self: Arc<Self>,
        ident: TableIdent,
        no_op_if_missing: bool,
        preparation: ConnectorRequestContext,
    ) -> PreparationResult<PreparedPublication> {
        preparation.check_active()?;
        let original = original_binding(&self, &preparation)?;
        let inventory = self.version_inventory(&ident, original.clone()).await?;
        if inventory.files.is_empty() {
            return if no_op_if_missing {
                Ok(PreparedPublication::NoOp)
            } else {
                Err(PreparationFailure(ConnectorMutationFailure::new(
                    ConnectorMutationFailureKind::NotFound,
                    "Hadoop table does not exist",
                )))
            };
        }
        preparation.check_active()?;
        Ok(PreparedPublication::Mutation {
            binding: None,
            dispatch: Box::new(HadoopPreparedDrop {
                client: self,
                ident,
                original,
                inventory,
            }),
        })
    }
}
struct HadoopPreparedDrop {
    client: Arc<HadoopFileSystemCatalog>,
    ident: TableIdent,
    original: IcebergReadBinding,
    inventory: versions::VersionInventory,
}
fn incomplete_drop(error: Error) -> DispatchCompletion {
    tracing::warn!(stage="drop-before-last-anchor", error=%error, "Hadoop DROP did not finish; lower metadata versions may already have been deleted");
    DispatchCompletion::evidence(DispatchEvidence::Rejected {
        failure: crate::commit::attempt::before_dispatch_failure(&error),
        observation: ProtocolObservation {
            adapter: "hadoop",
            stage: "drop-before-last-anchor",
            ..Default::default()
        },
    })
}
#[async_trait]
impl PreparedCatalogDispatch for HadoopPreparedDrop {
    async fn dispatch_once(self: Box<Self>, context: EvidenceContext) -> DispatchCompletion {
        let binding = match phase_binding(&self.original, context) {
            Ok(binding) => binding,
            Err(error) => return not_dispatched(error),
        };
        match self
            .client
            .version_inventory(&self.ident, binding.clone())
            .await
        {
            Ok(current) if current.files == self.inventory.files => (),
            Ok(_) => {
                return rejected(
                    ConnectorMutationFailureKind::Conflict,
                    "drop-inventory",
                    "Hadoop version inventory changed before DROP dispatch",
                );
            }
            Err(error) => return not_dispatched(error),
        }
        let location = self.client.table_location(&self.ident);
        if let Err(error) = HadoopFileSystemCatalog::check_publication_budget(Some(context)) {
            return not_dispatched(error);
        }
        #[cfg(test)]
        if self
            .client
            .take_test_fault(HadoopCatalogTestFault::BeforeDropHintDelete)
        {
            return not_dispatched(Error::new(
                ErrorKind::Unexpected,
                "injected failure before Hadoop hint deletion",
            ));
        }
        let removed = super::publication::delete_authorized(
            &binding,
            &HadoopFileSystemCatalog::version_hint_path(&location),
        )
        .await;
        #[cfg(test)]
        let removed = if removed.is_ok()
            && self
                .client
                .take_test_fault(HadoopCatalogTestFault::AfterDropHintResponseLoss)
        {
            Err(Error::new(
                ErrorKind::Unexpected,
                "injected hint delete response loss",
            ))
        } else {
            removed
        };
        if let Err(error) = removed {
            return incomplete_drop(error);
        }
        #[cfg(test)]
        if self
            .client
            .take_test_fault(HadoopCatalogTestFault::AfterDropHintEvidenceExpiry)
        {
            tokio::time::sleep_until(context.deadline().into()).await;
        }
        for (index, file) in self.inventory.files.iter().enumerate() {
            if let Err(error) = HadoopFileSystemCatalog::check_publication_budget(Some(context)) {
                return incomplete_drop(error);
            }
            let last = index + 1 == self.inventory.files.len();
            #[cfg(test)]
            if last
                && self
                    .client
                    .take_test_fault(HadoopCatalogTestFault::BeforeCanonicalDrop)
            {
                return incomplete_drop(Error::new(
                    ErrorKind::Unexpected,
                    "injected failure before last Hadoop anchor deletion",
                ));
            }
            let removed = super::publication::delete_authorized(&binding, &file.path).await;
            #[cfg(test)]
            let removed = if last
                && removed.is_ok()
                && self
                    .client
                    .take_test_fault(HadoopCatalogTestFault::AfterCanonicalDropResponseLoss)
            {
                Err(Error::new(
                    ErrorKind::Unexpected,
                    "injected last anchor response loss",
                ))
            } else {
                removed
            };
            match removed {
                Ok(()) if last => {
                    return DispatchCompletion::evidence(DispatchEvidence::Applied {
                        proof: CommitProof::applied(None),
                    });
                }
                Ok(()) => (),
                Err(error) if !last => return incomplete_drop(error),
                Err(error) => {
                    match self
                        .client
                        .version_inventory(&self.ident, binding.clone())
                        .await
                    {
                        Ok(current) if current.files.is_empty() => {
                            return DispatchCompletion::evidence(DispatchEvidence::Applied {
                                proof: CommitProof::applied(None),
                            });
                        }
                        Err(readback) => {
                            tracing::warn!(stage="drop-last-readback", error=%readback, "Hadoop DROP last anchor readback failed")
                        }
                        _ => (),
                    }
                    return unknown_error("drop-last-anchor", error);
                }
            }
        }
        unreachable!("prepared DROP has at least one anchor")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::CatalogTableName;
    use crate::catalog::catalog_request::CatalogPublicationRequest;
    use crate::catalog::publication::CatalogEvidencePolicy;
    use crate::commit::attempt::observation::PublicationJournal;
    use crate::commit::model::OperationToken;
    use novarocks_spi::connector::{
        ConnectorInstanceDescriptor, ConnectorInstanceId, ConnectorMutationOperationId,
        ConnectorProviderId, ConnectorRequestContext, ConnectorStopOwner, ExternalMutationEffect,
        ExternalMutationEvidence, ExternalMutationOutcome, ProviderBindingEpoch,
    };

    #[test]
    fn last_anchor_drop_witness_preserves_purge_capture_and_data() {
        crate::hadoop_catalog::tests::on_bridge(async move {
            for scenario in [
                "ack",
                "before",
                "response-loss",
                "absent",
                "deadline",
                "before-hint",
                "no-op",
                "hint-only-loss",
                "hint-only-deadline",
            ] {
                let directory = tempfile::tempdir().unwrap();
                let catalog = Arc::new(super::super::tests::test_catalog(
                    directory.path().to_str().unwrap(),
                ));
                let namespace = NamespaceIdent::new("db".into());
                let table = catalog
                    .create_fixture_table(&namespace, super::super::tests::test_creation("t"))
                    .await
                    .unwrap();
                let ident = table.identifier().clone();
                let canonical = directory.path().join("db/t/metadata/v1.metadata.json");
                let retained = directory.path().join("db/t/metadata/v2.metadata.json");
                std::fs::write(&retained, b"unrelated retained object").unwrap();
                match scenario {
                    "before" => {
                        catalog.inject_test_fault(HadoopCatalogTestFault::BeforeCanonicalDrop)
                    }
                    "response-loss" => catalog
                        .inject_test_fault(HadoopCatalogTestFault::AfterCanonicalDropResponseLoss),
                    "deadline" => catalog
                        .inject_test_fault(HadoopCatalogTestFault::AfterDropHintEvidenceExpiry),
                    "before-hint" => {
                        catalog.inject_test_fault(HadoopCatalogTestFault::BeforeDropHintDelete)
                    }
                    "hint-only-loss" | "hint-only-deadline" => {
                        std::fs::rename(
                            &canonical,
                            directory.path().join("db/t/metadata/only-v2.metadata.json"),
                        )
                        .unwrap();
                        std::fs::write(
                            directory.path().join("db/t/metadata/version-hint.text"),
                            b"2\n",
                        )
                        .unwrap();
                        std::fs::copy(
                            directory.path().join("db/t/metadata/only-v2.metadata.json"),
                            directory.path().join("db/t/metadata/v2.metadata.json"),
                        )
                        .unwrap();
                        assert!(
                            catalog.load_table(table.identifier()).await.is_ok(),
                            "hint-only v2 is readable before drop"
                        );
                        catalog.inject_test_fault(if scenario == "hint-only-loss" {
                            HadoopCatalogTestFault::AfterDropHintResponseLoss
                        } else {
                            HadoopCatalogTestFault::AfterDropHintEvidenceExpiry
                        });
                    }
                    "absent" => std::fs::remove_file(&canonical).unwrap(),
                    _ => (),
                }
                let retained_bytes = std::fs::read(&retained).unwrap();
                let capture = crate::catalog::CatalogDropTableReceipt {
                    table_uuid: Some(Arc::from(table.metadata().uuid().to_string())),
                    table_location: Some(Arc::from(table.metadata().location())),
                    metadata_location: table.metadata_location().map(Arc::from),
                    last_updated_ms: table.metadata().last_updated_ms(),
                };
                let operation = ConnectorMutationOperationId::from_bytes([41; 16]);
                let frozen = CatalogPublicationRequest::DropTable {
                    table: CatalogTableName::new("db", "t"),
                    no_op_if_missing: false,
                    purge: true,
                    capture: Some(capture.clone()),
                }
                .freeze(operation.to_bytes())
                .unwrap();
                let scope = frozen.drop_scope().unwrap();
                let mismatched_scope = CatalogPublicationRequest::DropTable {
                    table: CatalogTableName::new("db", "t"),
                    no_op_if_missing: false,
                    purge: false,
                    capture: Some(capture.clone()),
                }
                .freeze(operation.to_bytes())
                .unwrap()
                .drop_scope();
                assert!(
                    mismatched_scope.is_none(),
                    "Retain cannot produce a cleanup scope"
                );
                let context = ConnectorRequestContext::try_new(
                    std::time::Instant::now() + Duration::from_secs(5),
                    ConnectorStopOwner::new().view(),
                    65536,
                    131072,
                )
                .unwrap();
                let journal = PublicationJournal::new(OperationToken::from_mutation(operation));
                journal
                    .prepare_catalog(
                        ExternalMutationEvidence::try_new(
                            2,
                            ConnectorInstanceDescriptor {
                                provider_id: ConnectorProviderId::parse("iceberg").unwrap(),
                                instance_id: ConnectorInstanceId::parse("hadoop-drop-test")
                                    .unwrap(),
                            },
                            ProviderBindingEpoch::from_bytes([4; 16]),
                            operation,
                            "drop_table",
                            Bytes::from_static(b"{}"),
                        )
                        .unwrap(),
                        (),
                        frozen.targets(),
                        frozen.request_bytes().to_vec(),
                        frozen.attribution().clone(),
                        context.clone(),
                    )
                    .unwrap();
                let prepared = if scenario == "no-op" {
                    crate::catalog::publication_dispatch::PreparedPublication::NoOp
                } else {
                    Arc::clone(&catalog)
                        .prepare_table_drop(ident, false, context.clone())
                        .await
                        .unwrap()
                };
                let entry = journal
                    .publish_once(
                        context,
                        CatalogEvidencePolicy {
                            evidence: if scenario.ends_with("deadline") {
                                Duration::from_millis(20)
                            } else {
                                Duration::from_secs(300)
                            },
                            ..Default::default()
                        },
                        move |_, _, _| async move { Ok(prepared) },
                    )
                    .await
                    .unwrap();
                let outcome = journal.catalog_entry_outcome(entry);
                match (scenario, outcome) {
                    (
                        "ack" | "response-loss" | "absent",
                        ExternalMutationOutcome::KnownCommitted {
                            effect: ExternalMutationEffect::Applied,
                            ..
                        },
                    ) => assert!(journal.applied_drop_witness(entry, &scope).is_some()),
                    ("before-hint", ExternalMutationOutcome::KnownUncommitted { .. }) => {
                        assert!(journal.applied_drop_witness(entry, &scope).is_none())
                    }
                    (
                        "before" | "deadline" | "hint-only-loss" | "hint-only-deadline",
                        ExternalMutationOutcome::KnownUncommitted { .. },
                    ) => {
                        assert!(journal.applied_drop_witness(entry, &scope).is_none())
                    }
                    (
                        "no-op",
                        ExternalMutationOutcome::KnownCommitted {
                            effect: ExternalMutationEffect::NoOp,
                            ..
                        },
                    ) => assert!(journal.applied_drop_witness(entry, &scope).is_none()),
                    (_, other) => panic!("unexpected {scenario} drop: {other:?}"),
                }
                if let Some(witness) = journal.applied_drop_witness(entry, &scope) {
                    assert!(crate::catalog_control::drop_cleanup::PostCommitCleanupRequest::from_journal_drop(Arc::from("db.foreign"), &capture, &witness).is_none());
                    let mut foreign = capture.clone();
                    foreign.table_uuid = Some(Arc::from(uuid::Uuid::new_v4().to_string()));
                    assert!(crate::catalog_control::drop_cleanup::PostCommitCleanupRequest::from_journal_drop(Arc::from("db.t"), &foreign, &witness).is_none());
                    let foreign_scope = CatalogPublicationRequest::DropTable {
                        table: CatalogTableName::new("db", "t"),
                        no_op_if_missing: false,
                        purge: true,
                        capture: Some(foreign),
                    }
                    .freeze(operation.to_bytes())
                    .unwrap()
                    .drop_scope()
                    .unwrap();
                    assert!(
                        journal
                            .applied_drop_witness(entry, &foreign_scope)
                            .is_none(),
                        "capture is part of the exact descriptor"
                    );
                    assert!(crate::catalog_control::drop_cleanup::PostCommitCleanupRequest::from_journal_drop(Arc::from("db.t"), &capture, &witness).is_some());
                }
                assert_eq!(
                    canonical.exists(),
                    matches!(scenario, "deadline" | "before-hint" | "no-op")
                );
                if matches!(scenario, "ack" | "response-loss" | "absent") {
                    assert!(!retained.exists());
                } else {
                    assert_eq!(std::fs::read(&retained).unwrap(), retained_bytes);
                }
            }
        });
    }
}
