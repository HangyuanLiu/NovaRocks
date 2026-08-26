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
    CatalogTransactionStart, NovaRocksCatalog,
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
    warehouse: Option<Arc<str>>,
}

impl NovaRocksRestCatalog {
    pub(super) fn new(
        client: Arc<crate::iceberg_catalog_rest::RestCatalog>,
        warehouse: Option<Arc<str>>,
    ) -> Self {
        Self {
            delegate: CatalogDelegate::new(client),
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
            CatalogCreateIntent::CreateTableAsSelect => {
                // Admission runs before anything is staged or executed. The
                // staged-create dispatch itself is wired by the CTAS family,
                // which owns the staged-write aggregate; what belongs here is
                // the catalog's own answer to "can this request work at all".
                match self.admit_staged_create(request.intent) {
                    Ok(_warehouse) => {
                        super::start_create_table_transaction(&self.delegate, request)
                    }
                    Err(reason) => CatalogTransactionStart::Unsupported(
                        super::error::CatalogUnsupported::new(reason),
                    ),
                }
            }
        }
    }

    async fn new_create_or_replace_table_transaction(
        &self,
        request: CreateTableTransactionRequest,
    ) -> CatalogTransactionStart {
        match self.admit_staged_create(request.intent) {
            Ok(_warehouse) => super::start_create_table_transaction(&self.delegate, request),
            Err(reason) => {
                CatalogTransactionStart::Unsupported(super::error::CatalogUnsupported::new(reason))
            }
        }
    }
}
