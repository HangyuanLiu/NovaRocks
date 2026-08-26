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

//! The Hadoop filesystem catalog implementation.
//!
//! Design: ADR-0110 (docs/adr/ADR-0110-iceberg-provider-private-catalog-owner.md)

use std::sync::Arc;

use async_trait::async_trait;
use novarocks_spi::connector::ConnectorError;

use super::delegate::CatalogDelegate;
use super::error::{CatalogOutcome, CatalogUnsupported};
use super::transaction::{CreateTableTransactionRequest, TransactionRequest};
use super::{
    CatalogCreateIntent, CatalogDropTableReceipt, CatalogNamespaceName, CatalogTableName,
    CatalogTransactionStart, ConditionalCreateAttempt, ConditionalCreateEvidence,
    ConditionalCreateFacts, ConditionalCreateReceipt, ConditionalCreateRequest,
    ConditionalCreateVerdict, NovaRocksCatalog,
};

/// A Hadoop filesystem Iceberg catalog.
///
/// Its create is not a catalog call: the linearization point is a conditional
/// write of the canonical `v1.metadata.json` in storage, followed by an
/// authoritative reread (ADR-0077). That gives atomic empty-table creation, and
/// it is why this catalog accepts one create intent while refusing the other.
///
/// CTAS is refused before any side effect. There is no staged-create protocol
/// here, and the alternatives are worse than refusing: creating a visible empty
/// table and filling it afterwards exposes a half-built table to readers, and a
/// process-local lock does not fence a second writer.
///
/// Views are not special-cased. The Hadoop client does not implement the view
/// methods, so delegation already yields a typed `Unsupported` — this catalog
/// format cannot store a view, and saying "no views here" instead would be
/// answering a question it cannot answer.
#[derive(Debug)]
pub(super) struct NovaRocksHadoopCatalog {
    delegate: CatalogDelegate,
    client: Arc<crate::hadoop_catalog::HadoopFileSystemCatalog>,
}

impl NovaRocksHadoopCatalog {
    pub(super) fn new(client: Arc<crate::hadoop_catalog::HadoopFileSystemCatalog>) -> Self {
        Self {
            delegate: CatalogDelegate::new(client.clone()),
            client,
        }
    }

    /// The concrete client, for the conditional-create path that has no
    /// equivalent on the generic catalog trait.
    pub(super) fn conditional_client(
        &self,
    ) -> &Arc<crate::hadoop_catalog::HadoopFileSystemCatalog> {
        &self.client
    }
}

#[async_trait]
impl NovaRocksCatalog for NovaRocksHadoopCatalog {
    fn implementation_name(&self) -> &'static str {
        "hadoop"
    }

    fn vendored_client(&self) -> Arc<dyn crate::iceberg::Catalog> {
        Arc::clone(self.delegate.client())
    }

    fn admit_create(&self, intent: CatalogCreateIntent) -> Result<(), CatalogUnsupported> {
        match intent {
            CatalogCreateIntent::EmptyTable => Ok(()),
            CatalogCreateIntent::CreateTableAsSelect => Err(CatalogUnsupported::new(
                "Hadoop Iceberg catalog has no staged-create protocol, so CREATE TABLE AS \
                     SELECT cannot publish its target atomically",
            )),
        }
    }

    async fn list_namespaces(&self) -> Result<Vec<String>, ConnectorError> {
        self.delegate.list_namespaces().await
    }

    async fn namespace_exists(
        &self,
        namespace: CatalogNamespaceName,
    ) -> Result<bool, ConnectorError> {
        self.delegate.namespace_exists(&namespace).await
    }

    async fn list_tables(
        &self,
        namespace: CatalogNamespaceName,
    ) -> Result<Vec<String>, ConnectorError> {
        self.delegate.list_tables(&namespace).await
    }

    async fn table_exists(&self, table: CatalogTableName) -> Result<bool, ConnectorError> {
        self.delegate.table_exists(&table).await
    }

    async fn load_table(
        &self,
        table: CatalogTableName,
    ) -> Result<crate::iceberg::table::Table, ConnectorError> {
        self.delegate.load_table(&table).await
    }

    async fn view_exists(&self, view: CatalogTableName) -> Result<bool, ConnectorError> {
        self.delegate.view_exists(&view).await
    }

    async fn list_views(
        &self,
        namespace: CatalogNamespaceName,
    ) -> Result<Vec<String>, ConnectorError> {
        self.delegate.list_views(&namespace).await
    }

    async fn load_view(
        &self,
        view: CatalogTableName,
    ) -> Result<crate::iceberg::spec::ViewMetadata, ConnectorError> {
        self.delegate.load_view(&view).await
    }

    async fn create_namespace(
        &self,
        namespace: CatalogNamespaceName,
    ) -> CatalogOutcome<CatalogNamespaceName> {
        self.delegate.create_namespace(namespace).await
    }

    async fn drop_namespace(
        &self,
        namespace: CatalogNamespaceName,
    ) -> CatalogOutcome<CatalogNamespaceName> {
        self.delegate.drop_namespace(namespace).await
    }

    async fn drop_table(&self, table: CatalogTableName) -> CatalogOutcome<CatalogDropTableReceipt> {
        self.delegate.drop_table(table).await
    }

    async fn register_table(
        &self,
        table: CatalogTableName,
        metadata_location: Arc<str>,
    ) -> CatalogOutcome<CatalogTableName> {
        self.delegate.register_table(table, metadata_location).await
    }

    async fn anchor_written_metadata(
        &self,
        table: CatalogTableName,
        metadata_location: Arc<str>,
    ) -> CatalogOutcome<CatalogTableName> {
        // The namespace has to exist before the table can be anchored under it.
        // The previous helper created it with `let _ =`, so a namespace that
        // failed to appear surfaced later as a confusing registration failure
        // instead of the thing that actually went wrong.
        let namespace = CatalogNamespaceName::new(Arc::clone(&table.namespace));
        match self.delegate.namespace_exists(&namespace).await {
            Ok(true) => {}
            Ok(false) => {
                let created = self.delegate.create_namespace(namespace).await;
                if !matches!(created, CatalogOutcome::KnownCommitted { .. }) {
                    return match created {
                        CatalogOutcome::KnownCommitted { .. } => unreachable!(),
                        CatalogOutcome::Unsupported(reason) => CatalogOutcome::Unsupported(reason),
                        CatalogOutcome::KnownUncommitted { failure } => {
                            CatalogOutcome::KnownUncommitted { failure }
                        }
                        CatalogOutcome::CommitUnknown { failure, evidence } => {
                            CatalogOutcome::CommitUnknown { failure, evidence }
                        }
                    };
                }
            }
            Err(error) => {
                return CatalogOutcome::uncommitted(
                    novarocks_spi::connector::ConnectorMutationFailureKind::Unavailable,
                    error.to_string(),
                );
            }
        }
        self.delegate.register_table(table, metadata_location).await
    }

    /// Prepare the conditional metadata write that makes this table exist.
    ///
    /// Local only: it builds metadata and sends nothing, which is what lets the
    /// caller freeze publication evidence before the attempt is dispatched.
    async fn stage_create_table(
        &self,
        _namespace: CatalogNamespaceName,
        _creation: crate::iceberg::TableCreation,
    ) -> super::StagedCreateStart {
        super::StagedCreateStart::Unsupported(CatalogUnsupported::new(
            "Hadoop Iceberg catalog has no staged-create protocol",
        ))
    }

    async fn commit_staged_table(
        &self,
        _commit: crate::iceberg::TableCommit,
    ) -> super::StagedCommitResult {
        super::StagedCommitResult::Unsupported(CatalogUnsupported::new(
            "Hadoop Iceberg catalog has no staged-create protocol",
        ))
    }

    async fn prepare_conditional_create(
        &self,
        request: ConditionalCreateRequest,
    ) -> CatalogOutcome<ConditionalCreateAttempt> {
        let namespace = match super::delegate::namespace_ident(&request.namespace) {
            Ok(ident) => ident,
            Err(error) => {
                return CatalogOutcome::uncommitted(
                    novarocks_spi::connector::ConnectorMutationFailureKind::InvalidRequest,
                    error.to_string(),
                );
            }
        };
        match self.client.prepare_create_attempt(
            &namespace,
            request.creation,
            request.operation_id.to_string(),
        ) {
            Ok(attempt) => {
                let facts = facts_from_hadoop(attempt.facts());
                CatalogOutcome::committed(
                    ConditionalCreateAttempt::hadoop(attempt, facts),
                    novarocks_spi::connector::ExternalMutationEffect::NoOp,
                )
            }
            // Preparing never dispatches, so every failure here is proven
            // uncommitted -- including an unsupported storage binding, which is
            // checked before any directory is created.
            Err(failure) => map_prepare_failure(&failure),
        }
    }

    async fn publish_conditional_create(
        &self,
        attempt: ConditionalCreateAttempt,
    ) -> CatalogOutcome<ConditionalCreateReceipt> {
        let facts = attempt.facts.clone();
        let Some(attempt) = attempt.into_hadoop() else {
            return CatalogOutcome::uncommitted(
                novarocks_spi::connector::ConnectorMutationFailureKind::InvalidRequest,
                "conditional create attempt was not prepared by this catalog",
            );
        };
        match self.client.publish_create_attempt(attempt).await {
            Ok(result) => CatalogOutcome::committed(
                ConditionalCreateReceipt {
                    facts,
                    already_existed: matches!(
                        result.disposition,
                        crate::hadoop_catalog::HadoopCreateDisposition::Existing
                    ),
                    authoritative_table_uuid: Arc::from(result.authoritative_table_uuid),
                    authoritative_metadata_digest: Arc::from(result.authoritative_metadata_digest),
                    published_metadata_location: result.table.metadata_location().map(Arc::from),
                    finalization_failure: result.finalization_failure.map(Arc::from),
                },
                novarocks_spi::connector::ExternalMutationEffect::Applied,
            ),
            Err(failure) => map_publish_failure(&failure, &facts),
        }
    }

    async fn adjudicate_conditional_create(
        &self,
        evidence: ConditionalCreateEvidence,
    ) -> Result<ConditionalCreateVerdict, ConnectorError> {
        let outcome = self
            .client
            .reconcile_create_attempt(
                &evidence.namespace,
                &evidence.table,
                &evidence.expected_table_uuid,
                &evidence.metadata_location,
                &evidence.metadata_digest,
            )
            .await
            .map_err(|error| {
                ConnectorError::new(
                    novarocks_spi::connector::ConnectorErrorKind::Unavailable,
                    error,
                )
            })?;
        Ok(match outcome {
            crate::hadoop_catalog::HadoopCreateReconciliation::Committed {
                finalization_failure,
            } => ConditionalCreateVerdict::Committed {
                finalization_failure: finalization_failure.map(Arc::from),
            },
            crate::hadoop_catalog::HadoopCreateReconciliation::Absent => {
                ConditionalCreateVerdict::Absent
            }
            crate::hadoop_catalog::HadoopCreateReconciliation::Foreign => {
                ConditionalCreateVerdict::Foreign
            }
        })
    }

    async fn new_transaction(&self, request: TransactionRequest) -> CatalogTransactionStart {
        super::start_update_table_transaction(&self.delegate, request)
    }

    async fn new_create_table_transaction(
        &self,
        request: CreateTableTransactionRequest,
    ) -> CatalogTransactionStart {
        match request.intent {
            CatalogCreateIntent::EmptyTable => {
                // This catalog's create is a conditional metadata write, not a
                // catalog call, so the transaction is built around that
                // primitive rather than around `create_table`.
                let target = request.target.clone();
                let namespace = CatalogNamespaceName::new(Arc::clone(&target.namespace));
                let prepared = self
                    .prepare_conditional_create(super::ConditionalCreateRequest {
                        namespace,
                        creation: request.creation,
                        operation_id: Arc::from(request.identity.hex()),
                    })
                    .await;
                let Some((attempt, _effect, _witness)) = prepared.into_known_committed() else {
                    // Preparation sends nothing, so a failure here is proven
                    // uncommitted; report it without inventing a transaction.
                    return CatalogTransactionStart::KnownUncommitted {
                        failure: novarocks_spi::connector::ConnectorMutationFailure::new(
                            novarocks_spi::connector::ConnectorMutationFailureKind::InvalidRequest,
                            "conditional create could not be prepared",
                        ),
                    };
                };
                let evidence = super::ConditionalCreateEvidence {
                    namespace: Arc::clone(&target.namespace),
                    table: Arc::clone(&target.name),
                    expected_table_uuid: Arc::clone(&attempt.facts.table_uuid),
                    metadata_location: Arc::clone(&attempt.facts.metadata_location),
                    metadata_digest: Arc::clone(&attempt.facts.metadata_digest),
                };
                let commit_evidence =
                    super::error::CatalogCommitEvidence::for_target(target.canonical())
                        .with_target_uuid(Arc::clone(&attempt.facts.table_uuid))
                        .with_metadata_location(Arc::clone(&attempt.facts.metadata_location));
                CatalogTransactionStart::Ready(Box::new(super::transaction::Transaction::new(
                    request.identity,
                    target,
                    super::transaction::TransactionShape::Create(request.intent),
                    commit_evidence,
                    Arc::new(super::dispatch::ConditionalCreateDispatch::new(
                        Arc::clone(&self.client),
                        attempt
                            .into_hadoop()
                            .expect("this catalog prepared the attempt"),
                        evidence,
                    )),
                )))
            }
            CatalogCreateIntent::CreateTableAsSelect => match self.admit_create(request.intent) {
                Ok(()) => super::start_create_table_transaction(&self.delegate, request),
                Err(reason) => CatalogTransactionStart::Unsupported(reason),
            },
        }
    }

    async fn new_create_or_replace_table_transaction(
        &self,
        _request: CreateTableTransactionRequest,
    ) -> CatalogTransactionStart {
        CatalogTransactionStart::Unsupported(CatalogUnsupported::new(
            "Hadoop Iceberg catalog cannot replace a table atomically",
        ))
    }
}

fn facts_from_hadoop(
    facts: &crate::hadoop_catalog::HadoopCreateAttemptFacts,
) -> ConditionalCreateFacts {
    ConditionalCreateFacts {
        operation_id: Arc::from(facts.operation_id.as_str()),
        table_uuid: Arc::from(facts.table_uuid.as_str()),
        metadata_location: Arc::from(facts.metadata_location.as_str()),
        metadata_digest: Arc::from(facts.metadata_digest.as_str()),
    }
}

/// Preparing sends nothing, so its failures are all proven uncommitted.
fn map_prepare_failure<T>(
    failure: &crate::hadoop_catalog::HadoopCreateFailure,
) -> CatalogOutcome<T> {
    use crate::hadoop_catalog::HadoopCreateFailureKind as Kind;
    use novarocks_spi::connector::ConnectorMutationFailureKind as Neutral;
    match failure.kind {
        Kind::Unsupported => CatalogOutcome::unsupported(failure.message.clone()),
        Kind::Invalid => CatalogOutcome::uncommitted(Neutral::InvalidRequest, message(failure)),
        // Preparation cannot reach these, but classifying them as unknown keeps
        // the conservative answer if it ever does.
        Kind::Uncommitted => CatalogOutcome::uncommitted(Neutral::Unavailable, message(failure)),
        Kind::Unknown => CatalogOutcome::unknown(
            message(failure),
            super::error::CatalogCommitEvidence::default(),
        ),
    }
}

fn map_publish_failure<T>(
    failure: &crate::hadoop_catalog::HadoopCreateFailure,
    facts: &ConditionalCreateFacts,
) -> CatalogOutcome<T> {
    use crate::hadoop_catalog::HadoopCreateFailureKind as Kind;
    use novarocks_spi::connector::ConnectorMutationFailureKind as Neutral;
    match failure.kind {
        Kind::Unsupported => CatalogOutcome::unsupported(failure.message.clone()),
        Kind::Invalid => CatalogOutcome::uncommitted(Neutral::InvalidRequest, message(failure)),
        Kind::Uncommitted => CatalogOutcome::uncommitted(Neutral::Unavailable, message(failure)),
        // The conditional write may have landed. Carry the exact identity a
        // read-only adjudication needs; nothing here may retry or delete.
        Kind::Unknown => CatalogOutcome::unknown(
            message(failure),
            super::error::CatalogCommitEvidence::default()
                .with_target_uuid(Arc::clone(&facts.table_uuid))
                .with_metadata_location(Arc::clone(&facts.metadata_location)),
        ),
    }
}

fn message(failure: &crate::hadoop_catalog::HadoopCreateFailure) -> String {
    match failure.facts.as_ref() {
        Some(facts) => format!("{} [operation_id={}]", failure.message, facts.operation_id),
        None => failure.message.clone(),
    }
}
