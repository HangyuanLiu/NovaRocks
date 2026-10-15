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

//! Retains hint-first, canonical-v1-last table drop; only canonical delete
//! acknowledgement is decisive. Object reclamation belongs to its caller.
use super::publication::{not_dispatched, unknown};
use super::*;
use crate::catalog::publication::EvidenceContext;
use crate::catalog::publication_dispatch::{
    DispatchCompletion, PreparedCatalogDispatch, PreparedPublication,
};
use crate::catalog::transaction::CommitProof;
use crate::commit::attempt::observation::DispatchEvidence;

impl HadoopFileSystemCatalog {
    pub(crate) fn prepare_table_drop(self: Arc<Self>, ident: TableIdent) -> PreparedPublication {
        PreparedPublication::Mutation {
            binding: None,
            dispatch: Box::new(HadoopPreparedDrop {
                client: self,
                ident,
            }),
        }
    }
}
struct HadoopPreparedDrop {
    client: Arc<HadoopFileSystemCatalog>,
    ident: TableIdent,
}
fn before_canonical(error: Error, hint_may_have_changed: bool) -> DispatchCompletion {
    if !hint_may_have_changed {
        return not_dispatched(error);
    }
    // A valid hint may be the table's only resolvable pointer. Without proof
    // that v1 remains usable, a possible hint removal forbids a negative fact.
    DispatchCompletion::evidence(DispatchEvidence::Unknown {
        failure: crate::commit::attempt::before_dispatch_failure(&error),
        observation: crate::commit::attempt::observation::ProtocolObservation {
            adapter: "hadoop",
            stage: "drop-after-hint",
            ..Default::default()
        },
    })
}
#[async_trait]
impl PreparedCatalogDispatch for HadoopPreparedDrop {
    async fn dispatch_once(self: Box<Self>, context: EvidenceContext) -> DispatchCompletion {
        let mut tables = match self.client.table_cache_with_budget(Some(context)).await {
            Ok(tables) => tables,
            Err(error) => return not_dispatched(error),
        };
        tables.remove(&HadoopFileSystemCatalog::table_key(&self.ident));
        drop(tables);
        let location = self.client.table_location(&self.ident);
        let hint = HadoopFileSystemCatalog::version_hint_path(&location);
        if let Err(error) = HadoopFileSystemCatalog::check_publication_budget(Some(context)) {
            return not_dispatched(error);
        }
        let exists = match self.client.file_io.exists(&hint).await {
            Ok(value) => value,
            Err(error) => return not_dispatched(error),
        };
        if exists {
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
                    "injected failure before version hint deletion",
                ));
            }
            let removed = self.client.file_io.delete(&hint).await;
            #[cfg(test)]
            let removed = if removed.is_ok()
                && self
                    .client
                    .take_test_fault(HadoopCatalogTestFault::AfterDropHintResponseLoss)
            {
                Err(Error::new(
                    ErrorKind::Unexpected,
                    "injected response loss after version hint deletion",
                ))
            } else {
                removed
            };
            if let Err(error) = removed {
                return before_canonical(error, true);
            }
        }
        #[cfg(test)]
        if self
            .client
            .take_test_fault(HadoopCatalogTestFault::AfterDropHintEvidenceExpiry)
        {
            tokio::time::sleep_until(context.deadline().into()).await;
        }
        if let Err(error) = HadoopFileSystemCatalog::check_publication_budget(Some(context)) {
            return before_canonical(error, exists);
        }
        let canonical = HadoopFileSystemCatalog::metadata_path(&location, 1);
        match self.client.file_io.exists(&canonical).await {
            Err(error) => return before_canonical(error, exists),
            Ok(false) => {
                return unknown(
                    "drop-canonical-absent",
                    "Canonical metadata absence cannot prove this table drop committed",
                );
            }
            Ok(true) => (),
        }
        if let Err(error) = HadoopFileSystemCatalog::check_publication_budget(Some(context)) {
            return before_canonical(error, exists);
        }
        #[cfg(test)]
        if self
            .client
            .take_test_fault(HadoopCatalogTestFault::BeforeCanonicalDrop)
        {
            return before_canonical(
                Error::new(
                    ErrorKind::Unexpected,
                    "injected failure before canonical metadata deletion",
                ),
                exists,
            );
        }
        let deleted = self.client.file_io.delete(&canonical).await;
        #[cfg(test)]
        let deleted = if deleted.is_ok()
            && self
                .client
                .take_test_fault(HadoopCatalogTestFault::AfterCanonicalDropResponseLoss)
        {
            Err(Error::new(
                ErrorKind::Unexpected,
                "injected response loss after canonical metadata deletion",
            ))
        } else {
            deleted
        };
        match deleted {
            Ok(()) => DispatchCompletion::evidence(DispatchEvidence::Applied {
                proof: CommitProof::applied(None),
            }),
            Err(_) => unknown(
                "drop-canonical-delete",
                "Canonical metadata deletion has no decisive acknowledgement",
            ),
        }
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

    #[tokio::test]
    async fn canonical_delete_ack_is_the_only_drop_witness_and_other_objects_remain() {
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
                .create_table(&namespace, super::super::tests::test_creation("t"))
                .await
                .unwrap();
            let ident = table.identifier().clone();
            let canonical = directory.path().join("db/t/metadata/v1.metadata.json");
            let retained = directory.path().join("db/t/metadata/v2.metadata.json");
            std::fs::write(&retained, b"unrelated retained object").unwrap();
            match scenario {
                "before" => catalog.inject_test_fault(HadoopCatalogTestFault::BeforeCanonicalDrop),
                "response-loss" => catalog
                    .inject_test_fault(HadoopCatalogTestFault::AfterCanonicalDropResponseLoss),
                "deadline" => {
                    catalog.inject_test_fault(HadoopCatalogTestFault::AfterDropHintEvidenceExpiry)
                }
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
                            instance_id: ConnectorInstanceId::parse("hadoop-drop-test").unwrap(),
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
                Arc::clone(&catalog).prepare_table_drop(ident)
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
                    "ack",
                    ExternalMutationOutcome::KnownCommitted {
                        effect: ExternalMutationEffect::Applied,
                        ..
                    },
                ) => assert!(journal.applied_drop_witness(entry, &scope).is_some()),
                ("before-hint", ExternalMutationOutcome::KnownUncommitted { .. }) => {
                    assert!(journal.applied_drop_witness(entry, &scope).is_none())
                }
                (
                    "before" | "deadline" | "response-loss" | "absent" | "hint-only-loss"
                    | "hint-only-deadline",
                    ExternalMutationOutcome::CommitUnknown { .. },
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
                matches!(scenario, "before" | "deadline" | "before-hint" | "no-op")
            );
            assert_eq!(std::fs::read(&retained).unwrap(), retained_bytes);
        }
    }
}
