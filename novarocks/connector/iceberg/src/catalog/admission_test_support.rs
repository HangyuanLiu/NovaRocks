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

type FrozenRecorder =
    Arc<dyn Fn(&[crate::iceberg::TableRequirement], &[crate::iceberg::TableUpdate]) + Send + Sync>;
struct AdmittedCatalog {
    inner: Arc<dyn NovaRocksCatalog>,
    recorder: Option<FrozenRecorder>,
}

pub(crate) fn all_admitted(inner: Arc<dyn NovaRocksCatalog>) -> Arc<dyn NovaRocksCatalog> {
    Arc::new(AdmittedCatalog {
        inner,
        recorder: None,
    })
}

pub(crate) fn all_admitted_recording(
    inner: Arc<dyn NovaRocksCatalog>,
    recorder: FrozenRecorder,
) -> Arc<dyn NovaRocksCatalog> {
    Arc::new(AdmittedCatalog {
        inner,
        recorder: Some(recorder),
    })
}
impl std::fmt::Debug for AdmittedCatalog {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AdmittedCatalog")
            .field("inner", &self.inner)
            .field("recording", &self.recorder.is_some())
            .finish()
    }
}
struct RecordedDispatch {
    inner: Box<dyn super::publication_dispatch::PreparedCatalogDispatch>,
    recorder: FrozenRecorder,
    requirements: Vec<crate::iceberg::TableRequirement>,
    updates: Vec<crate::iceberg::TableUpdate>,
}
#[async_trait]
impl super::publication_dispatch::PreparedCatalogDispatch for RecordedDispatch {
    async fn dispatch_once(
        self: Box<Self>,
        context: super::publication::EvidenceContext,
    ) -> super::publication_dispatch::DispatchCompletion {
        (self.recorder)(&self.requirements, &self.updates);
        self.inner.dispatch_once(context).await
    }
}

#[async_trait]
impl NovaRocksCatalog for AdmittedCatalog {
    fn bind_publication_generation(
        &self,
        generation: novarocks_spi::connector::ProviderBindingEpoch,
    ) -> Result<(), ConnectorError> {
        self.inner.bind_publication_generation(generation)
    }
    fn evidence_policy(&self) -> crate::catalog::publication::CatalogEvidencePolicy {
        self.inner.evidence_policy()
    }
    async fn prepare_catalog_publication(
        &self,
        request: crate::catalog::catalog_request::FrozenCatalogPublication,
        descriptor: crate::commit::publication_descriptor::PublicationDescriptor,
        generation: novarocks_spi::connector::ProviderBindingEpoch,
        preparation: novarocks_spi::connector::ConnectorRequestContext,
        document: crate::catalog::catalog_request::CatalogResponseDocument,
    ) -> crate::catalog::publication_dispatch::PreparationResult<
        crate::catalog::publication_dispatch::PreparedPublication,
    > {
        self.inner
            .prepare_catalog_publication(request, descriptor, generation, preparation, document)
            .await
    }
    fn materialize_staged_create_response(
        &self,
        table: crate::catalog::CatalogTableName,
        document: Vec<u8>,
    ) -> Result<crate::catalog::StagedCreateMaterial, novarocks_spi::connector::ConnectorError>
    {
        self.inner
            .materialize_staged_create_response(table, document)
    }
    async fn prepare_frozen_publication(
        &self,
        request: crate::commit::model::FrozenRequest,
        operation: crate::catalog::admission::CatalogOperation,
        staged_expectation: Option<crate::catalog::StagedPublicationExpectation>,
        descriptor: crate::commit::publication_descriptor::PublicationDescriptor,
        generation: novarocks_spi::connector::ProviderBindingEpoch,
        preparation: novarocks_spi::connector::ConnectorRequestContext,
    ) -> crate::catalog::publication_dispatch::PreparationResult<
        crate::catalog::publication_dispatch::PreparedPublication,
    > {
        let requirements = request.requirements().to_vec();
        let updates = request.updates().to_vec();
        let prepared = self
            .inner
            .prepare_frozen_for_admission_test(
                request,
                operation,
                staged_expectation,
                descriptor,
                generation,
                preparation,
            )
            .await?;
        Ok(match (prepared, &self.recorder) {
            (
                super::publication_dispatch::PreparedPublication::Mutation { binding, dispatch },
                Some(recorder),
            ) => super::publication_dispatch::PreparedPublication::Mutation {
                binding,
                dispatch: Box::new(RecordedDispatch {
                    inner: dispatch,
                    recorder: recorder.clone(),
                    requirements,
                    updates,
                }),
            },
            (prepared, _) => prepared,
        })
    }

    fn commit_dispatch(
        &self,
        ident: crate::iceberg::TableIdent,
        target_ref: Arc<str>,
        marker: Option<(Arc<str>, Arc<str>)>,
    ) -> Arc<dyn super::transaction::CatalogCommitDispatch> {
        self.inner.commit_dispatch(ident, target_ref, marker)
    }

    async fn create_view(
        &self,
        namespace: super::CatalogNamespaceName,
        creation: super::view_request::ViewCreate,
    ) -> CatalogOutcome<super::CatalogTableName> {
        self.inner.create_view(namespace, creation).await
    }

    async fn replace_view(
        &self,
        replace: super::view_request::ValidatedViewReplace,
    ) -> CatalogOutcome<super::CatalogTableName> {
        self.inner.replace_view(replace).await
    }

    async fn drop_view(
        &self,
        view: super::CatalogTableName,
    ) -> CatalogOutcome<super::CatalogTableName> {
        self.inner.drop_view(view).await
    }

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
        expectation: super::StagedPublicationExpectation,
    ) -> super::StagedCommitResult {
        self.inner.commit_staged_table(_commit, expectation).await
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
