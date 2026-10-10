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

//! Explicitly permissive owner for publication protocol tests only.
use std::sync::Arc;

use async_trait::async_trait;
use novarocks_spi::connector::{ConnectorError, ConnectorListingBound};

use super::admission::{CatalogAdmissionTarget, CatalogOperation};
use super::error::{CatalogOutcome, CatalogUnsupported};
use super::transaction::{CreateTableTransactionRequest, TransactionRequest};
use super::{
    CatalogDropTableReceipt, CatalogNamespaceName, CatalogTableName, CatalogTransactionStart,
    ConditionalCreateAttempt, ConditionalCreateEvidence, ConditionalCreateReceipt,
    ConditionalCreateRequest, ConditionalCreateVerdict, NovaRocksCatalog,
};

#[derive(Debug)]
struct AdmittedCatalog {
    inner: Arc<dyn NovaRocksCatalog>,
}

pub(crate) fn all_admitted(inner: Arc<dyn NovaRocksCatalog>) -> Arc<dyn NovaRocksCatalog> {
    Arc::new(AdmittedCatalog { inner })
}

#[async_trait]
impl NovaRocksCatalog for AdmittedCatalog {
    fn implementation_name(&self) -> &'static str {
        "admitted-test-owner"
    }
    fn vendored_client(&self) -> Arc<dyn crate::iceberg::Catalog> {
        self.inner.vendored_client()
    }
    fn listing_admission(&self) -> Arc<super::listing_admission::ListingAdmission> {
        self.inner.listing_admission()
    }
    fn admit_operation(
        &self,
        operation: &CatalogOperation,
        target: &CatalogAdmissionTarget,
    ) -> Result<(), CatalogUnsupported> {
        operation.validate_target(target)
    }
    async fn list_namespaces(
        &self,
        bound: ConnectorListingBound,
    ) -> Result<Vec<String>, ConnectorError> {
        self.inner.list_namespaces(bound).await
    }

    async fn namespace_exists(
        &self,
        namespace: CatalogNamespaceName,
    ) -> Result<bool, ConnectorError> {
        self.inner.namespace_exists(namespace).await
    }

    async fn list_tables(
        &self,
        namespace: CatalogNamespaceName,
        bound: ConnectorListingBound,
    ) -> Result<Vec<String>, ConnectorError> {
        self.inner.list_tables(namespace, bound).await
    }

    async fn table_exists(&self, table: CatalogTableName) -> Result<bool, ConnectorError> {
        self.inner.table_exists(table).await
    }

    async fn load_table(
        &self,
        table: CatalogTableName,
    ) -> Result<crate::loaded_table::IcebergLoadedTable, ConnectorError> {
        self.inner.load_table(table).await
    }

    async fn view_exists(&self, view: CatalogTableName) -> Result<bool, ConnectorError> {
        self.inner.view_exists(view).await
    }

    async fn list_views(
        &self,
        namespace: CatalogNamespaceName,
        bound: ConnectorListingBound,
    ) -> Result<Vec<String>, ConnectorError> {
        self.inner.list_views(namespace, bound).await
    }

    async fn load_view(
        &self,
        view: CatalogTableName,
    ) -> Result<crate::iceberg::spec::ViewMetadata, ConnectorError> {
        self.inner.load_view(view).await
    }

    async fn create_namespace(
        &self,
        _namespace: CatalogNamespaceName,
    ) -> CatalogOutcome<CatalogNamespaceName> {
        self.inner.create_namespace(_namespace).await
    }

    async fn drop_namespace(
        &self,
        _namespace: CatalogNamespaceName,
    ) -> CatalogOutcome<CatalogNamespaceName> {
        self.inner.drop_namespace(_namespace).await
    }

    async fn drop_table(
        &self,
        _table: CatalogTableName,
    ) -> CatalogOutcome<CatalogDropTableReceipt> {
        self.inner.drop_table(_table).await
    }

    async fn anchor_written_metadata(
        &self,
        _table: CatalogTableName,
        _metadata_location: Arc<str>,
    ) -> CatalogOutcome<CatalogTableName> {
        self.inner
            .anchor_written_metadata(_table, _metadata_location)
            .await
    }

    async fn stage_create_table(
        &self,
        _namespace: CatalogNamespaceName,
        _creation: crate::iceberg::TableCreation,
    ) -> super::StagedCreateStart {
        self.inner.stage_create_table(_namespace, _creation).await
    }

    async fn commit_staged_table(
        &self,
        _commit: crate::iceberg::TableCommit,
        _request_file_io: crate::iceberg::io::FileIO,
    ) -> super::StagedCommitResult {
        self.inner
            .commit_staged_table(_commit, _request_file_io)
            .await
    }

    async fn prepare_conditional_create(
        &self,
        _request: ConditionalCreateRequest,
    ) -> CatalogOutcome<ConditionalCreateAttempt> {
        self.inner.prepare_conditional_create(_request).await
    }

    async fn publish_conditional_create(
        &self,
        _attempt: ConditionalCreateAttempt,
    ) -> CatalogOutcome<ConditionalCreateReceipt> {
        self.inner.publish_conditional_create(_attempt).await
    }

    async fn adjudicate_conditional_create(
        &self,
        _evidence: ConditionalCreateEvidence,
    ) -> Result<ConditionalCreateVerdict, ConnectorError> {
        self.inner.adjudicate_conditional_create(_evidence).await
    }

    async fn new_transaction(&self, _request: TransactionRequest) -> CatalogTransactionStart {
        self.inner.new_transaction(_request).await
    }

    async fn new_create_table_transaction(
        &self,
        request: CreateTableTransactionRequest,
    ) -> CatalogTransactionStart {
        self.inner.new_create_table_transaction(request).await
    }

    async fn new_create_or_replace_table_transaction(
        &self,
        _request: CreateTableTransactionRequest,
    ) -> CatalogTransactionStart {
        self.inner
            .new_create_or_replace_table_transaction(_request)
            .await
    }
}
