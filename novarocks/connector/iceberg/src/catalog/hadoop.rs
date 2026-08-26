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
    CatalogTransactionStart, NovaRocksCatalog,
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

    async fn new_transaction(&self, request: TransactionRequest) -> CatalogTransactionStart {
        super::start_update_table_transaction(&self.delegate, request)
    }

    async fn new_create_table_transaction(
        &self,
        request: CreateTableTransactionRequest,
    ) -> CatalogTransactionStart {
        match request.intent {
            CatalogCreateIntent::EmptyTable => {
                super::start_create_table_transaction(&self.delegate, request)
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
