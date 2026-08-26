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

//! The REST catalog implementation.
//!
//! Design: ADR-0110 (docs/adr/ADR-0110-iceberg-provider-private-catalog-owner.md)

use std::sync::Arc;

use async_trait::async_trait;
use novarocks_spi::connector::ConnectorError;

use super::delegate::CatalogDelegate;
use super::error::CatalogOutcome;
use super::transaction::{CreateTableTransactionRequest, TransactionRequest};
use super::{
    CatalogCreateIntent, CatalogDropTableReceipt, CatalogNamespaceName, CatalogTableName,
    CatalogTransactionStart, ConditionalCreateAttempt, ConditionalCreateEvidence,
    ConditionalCreateReceipt, ConditionalCreateRequest, ConditionalCreateVerdict, NovaRocksCatalog,
};

/// A REST Iceberg catalog.
///
/// This is the only implementation that can satisfy a standard staged CTAS,
/// and even here that is a per-request question: staged creation needs an
/// explicit warehouse so the unanchored staging namespace is enumerable and
/// therefore collectable. A REST catalog configured without one can still
/// serve reads, DDL, and ordinary writes; it just cannot run CTAS safely, and
/// says so before the source executes.
#[derive(Debug)]
pub(super) struct NovaRocksRestCatalog {
    delegate: CatalogDelegate,
    /// The REST client, for the staged-create protocol that has no equivalent
    /// on the generic catalog trait. It never leaves this module.
    client: Arc<crate::iceberg_catalog_rest::RestCatalog>,
    warehouse: Option<Arc<str>>,
}

impl NovaRocksRestCatalog {
    pub(super) fn new(
        client: Arc<crate::iceberg_catalog_rest::RestCatalog>,
        warehouse: Option<Arc<str>>,
    ) -> Self {
        Self {
            delegate: CatalogDelegate::new(client.clone()),
            client,
            warehouse,
        }
    }

    /// Admission for the staged-create path.
    ///
    /// Returns the proven warehouse root, or the refusal to report before any
    /// side effect.
    fn admit_staged_create(&self, intent: CatalogCreateIntent) -> Result<Arc<str>, String> {
        match &self.warehouse {
            Some(warehouse) if !warehouse.is_empty() => Ok(Arc::clone(warehouse)),
            _ => Err(format!(
                "REST Iceberg catalog has no explicit warehouse, so {} cannot stage its target \
                 where the unanchored staging namespace stays enumerable and collectable",
                intent.as_str()
            )),
        }
    }
}

#[async_trait]
impl NovaRocksCatalog for NovaRocksRestCatalog {
    fn implementation_name(&self) -> &'static str {
        "rest"
    }

    fn vendored_client(&self) -> Arc<dyn crate::iceberg::Catalog> {
        Arc::clone(self.delegate.client())
    }

    fn admit_create(
        &self,
        intent: CatalogCreateIntent,
    ) -> Result<(), super::error::CatalogUnsupported> {
        match intent {
            CatalogCreateIntent::EmptyTable => Ok(()),
            CatalogCreateIntent::CreateTableAsSelect => self
                .admit_staged_create(intent)
                .map(|_| ())
                .map_err(super::error::CatalogUnsupported::new),
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
        _metadata_location: Arc<str>,
    ) -> CatalogOutcome<CatalogTableName> {
        // This catalog owns its own metadata pointer, so a committed write is
        // already reachable through it.
        CatalogOutcome::committed(
            table,
            novarocks_spi::connector::ExternalMutationEffect::NoOp,
        )
    }

    async fn stage_create_table(
        &self,
        namespace: CatalogNamespaceName,
        creation: crate::iceberg::TableCreation,
    ) -> super::StagedCreateStart {
        let creation = with_explicit_format_version(creation);
        let ident = match super::delegate::namespace_ident(&namespace) {
            Ok(ident) => ident,
            Err(error) => return super::StagedCreateStart::KnownUncommitted(error.to_string()),
        };
        match self.client.stage_create_table_typed(&ident, creation).await {
            Ok(staged) => {
                let (table, initialization_updates) = staged.into_parts();
                super::StagedCreateStart::Staged {
                    table,
                    initialization_updates,
                }
            }
            Err(crate::iceberg_catalog_rest::StagedCreateError::Conflict(error)) => {
                super::StagedCreateStart::Conflict(error.to_string())
            }
            Err(crate::iceberg_catalog_rest::StagedCreateError::KnownNotDispatched(error)) => {
                super::StagedCreateStart::KnownUncommitted(error.to_string())
            }
            Err(crate::iceberg_catalog_rest::StagedCreateError::PossiblyDispatched(error)) => {
                super::StagedCreateStart::CommitUnknown(error.to_string())
            }
        }
    }

    async fn commit_staged_table(
        &self,
        commit: crate::iceberg::TableCommit,
    ) -> super::StagedCommitResult {
        match self.client.commit_staged_table_typed(commit).await {
            Ok(table) => super::StagedCommitResult::Committed(table),
            Err(crate::iceberg_catalog_rest::StagedCommitError::Conflict(error)) => {
                super::StagedCommitResult::Conflict(error.to_string())
            }
            Err(crate::iceberg_catalog_rest::StagedCommitError::KnownNotDispatched(error)) => {
                super::StagedCommitResult::KnownUncommitted(error.to_string())
            }
            Err(crate::iceberg_catalog_rest::StagedCommitError::PossiblyDispatched(error)) => {
                super::StagedCommitResult::CommitUnknown(error.to_string())
            }
            Err(crate::iceberg_catalog_rest::StagedCommitError::CommittedResponseInvalid(
                error,
            )) => super::StagedCommitResult::CommittedResponseInvalid(error.to_string()),
        }
    }

    async fn prepare_conditional_create(
        &self,
        _request: ConditionalCreateRequest,
    ) -> CatalogOutcome<ConditionalCreateAttempt> {
        CatalogOutcome::unsupported(
            "REST Iceberg catalog publishes a create through the catalog, not through a conditional metadata write",
        )
    }

    async fn publish_conditional_create(
        &self,
        _attempt: ConditionalCreateAttempt,
    ) -> CatalogOutcome<ConditionalCreateReceipt> {
        CatalogOutcome::unsupported(
            "REST Iceberg catalog publishes a create through the catalog, not through a conditional metadata write",
        )
    }

    async fn adjudicate_conditional_create(
        &self,
        _evidence: ConditionalCreateEvidence,
    ) -> Result<ConditionalCreateVerdict, ConnectorError> {
        Err(novarocks_spi::connector::ConnectorError::new(
            novarocks_spi::connector::ConnectorErrorKind::Unsupported,
            "REST Iceberg catalog publishes a create through the catalog, not through a conditional metadata write",
        ))
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
                let request = crate::catalog::transaction::CreateTableTransactionRequest {
                    creation: with_explicit_format_version(request.creation),
                    ..request
                };
                super::start_create_table_transaction(&self.delegate, request)
            }
            CatalogCreateIntent::CreateTableAsSelect => {
                // Same decision as `admit_create`, so a caller that asked first
                // and a caller that went straight to the constructor get the
                // same answer.
                match self.admit_create(request.intent) {
                    Ok(()) => super::start_create_table_transaction(&self.delegate, request),
                    Err(reason) => CatalogTransactionStart::Unsupported(reason),
                }
            }
        }
    }

    async fn new_create_or_replace_table_transaction(
        &self,
        request: CreateTableTransactionRequest,
    ) -> CatalogTransactionStart {
        match self.admit_create(CatalogCreateIntent::CreateTableAsSelect) {
            Ok(()) => super::start_create_table_transaction(&self.delegate, request),
            Err(reason) => CatalogTransactionStart::Unsupported(reason),
        }
    }
}

/// Spell the format version into the table properties.
///
/// A REST catalog needs it stated explicitly; a filesystem catalog reads it off
/// the creation itself. That difference is this implementation's to know, which
/// is why it used to be a catalog-kind comparison at the call site and is not
/// any more.
fn with_explicit_format_version(
    creation: crate::iceberg::TableCreation,
) -> crate::iceberg::TableCreation {
    if creation.properties.contains_key("format-version") {
        return creation;
    }
    let mut properties = creation.properties.clone();
    properties.insert(
        "format-version".to_string(),
        (creation.format_version as u8).to_string(),
    );
    crate::iceberg::TableCreation {
        properties,
        ..creation
    }
}
