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

//! How this process answers what the compiler asks.
//!
//! Query application decides what to ask and in what order, and knows nothing
//! about where an answer comes from. These adapters are the other half: each
//! one knows a single owner in this process and nothing about compilation.
//! Neither half can be made to do the other's job, which is the point - it is
//! how the compiler stays pure while its answers come from a catalog, a
//! statistics resolver and a materialized-view inventory that are anything but.
//!
//! All three owners answer synchronously and may reach a remote catalog to do
//! it. They therefore run on the frontend's bounded connector lane rather than
//! on the async runtime: a slow REST catalog must not be able to stall the
//! scheduler, and an unbounded number of statements must not be able to queue
//! against it.

use std::sync::Arc;

use async_trait::async_trait;
use novarocks_catalog_application::{CatalogApplicationPort, ConnectorControlHost};
use novarocks_query_application::preparation::{
    CatalogFactPort, MaterializedViewFactPort, ProviderReadFactPort, QueryCompletionFactSource,
    StatisticsFactPort,
};
use novarocks_spi::connector::{
    ConnectorReadAttemptAccess, ConnectorRequestContext, MvStorageObservationPort,
    read_stack::ConnectorSession,
};
use novarocks_sql::compiler::{
    CatalogLookupTarget, CatalogRelationFact, CatalogRelationNeed, MaterializedViewFact,
    MaterializedViewNeed, StatisticsFact, StatisticsNeed,
};

use crate::catalog_application::query_bindings::QueryTableBindingStore;
use crate::catalog_application::query_catalog::{CatalogResolutionError, QueryCatalogService};
use crate::catalog_application::query_materializer::{
    CatalogServiceMaterializer, iceberg_table_binding_loader,
};
use crate::connector::UnifiedStatisticsResolver;
use crate::mv::domain::readiness::MvCandidateReader;
use crate::mv::domain::rewrite_prep::freeze_materialized_view_fact_with_ports;
use crate::query_execution::planning::statistics::resolve_statistics_need;
use crate::query_execution::provider_read_facts::FrontendProviderReadFacts;
use crate::task_execution::blocking_io::ConnectorBlockingIoSupervisor;

/// The owners in this process that can answer a compilation question, and the
/// lane their synchronous calls are admitted through.
///
/// One value per process: nothing here is statement-scoped.
#[derive(Clone)]
pub(crate) struct CompletionFactOwners {
    catalog_service: Arc<QueryCatalogService>,
    catalog_application: Option<Arc<dyn CatalogApplicationPort>>,
    connector_control: Arc<ConnectorControlHost>,
    statistics: Arc<UnifiedStatisticsResolver>,
    materialized_views: MvCandidateReader,
    mv_storage_observation: Arc<dyn MvStorageObservationPort>,
    blocking: ConnectorBlockingIoSupervisor,
}

impl CompletionFactOwners {
    pub(crate) const fn new(
        catalog_service: Arc<QueryCatalogService>,
        catalog_application: Option<Arc<dyn CatalogApplicationPort>>,
        connector_control: Arc<ConnectorControlHost>,
        statistics: Arc<UnifiedStatisticsResolver>,
        materialized_views: MvCandidateReader,
        mv_storage_observation: Arc<dyn MvStorageObservationPort>,
        blocking: ConnectorBlockingIoSupervisor,
    ) -> Self {
        Self {
            catalog_service,
            catalog_application,
            connector_control,
            statistics,
            materialized_views,
            mv_storage_observation,
            blocking,
        }
    }
}

/// What one statement adds to those owners.
///
/// The binding store is the statement's own: every fact about a relation is
/// answered against the bindings that statement was admitted under, never
/// against whatever the catalog holds at the moment the question is asked.
#[derive(Clone)]
pub(crate) struct StatementFactScope {
    bindings: Arc<QueryTableBindingStore>,
    connector_context: ConnectorRequestContext,
    current_catalog: Option<Arc<str>>,
}

impl StatementFactScope {
    pub(crate) fn new(
        bindings: Arc<QueryTableBindingStore>,
        connector_context: ConnectorRequestContext,
        current_catalog: Option<&str>,
    ) -> Self {
        Self {
            bindings,
            connector_context,
            current_catalog: current_catalog.map(Arc::from),
        }
    }
}

/// Answers relation lookups from the frontend catalog.
pub(crate) struct FrontendCatalogFacts {
    owners: CompletionFactOwners,
    scope: StatementFactScope,
}

impl FrontendCatalogFacts {
    pub(crate) const fn new(owners: CompletionFactOwners, scope: StatementFactScope) -> Self {
        Self { owners, scope }
    }
}

#[async_trait]
impl CatalogFactPort for FrontendCatalogFacts {
    async fn resolve_relations(
        &self,
        needs: &[CatalogRelationNeed],
    ) -> Result<Vec<CatalogRelationFact>, String> {
        let needs = needs.to_vec();
        let owners = self.owners.clone();
        let scope = self.scope.clone();
        admitted(&self.owners, move || {
            let loader = iceberg_table_binding_loader(
                owners.connector_control.as_ref(),
                scope.connector_context.clone(),
            );
            let materializer = CatalogServiceMaterializer::new(
                scope.current_catalog.as_deref(),
                owners.catalog_service.as_ref(),
                Arc::clone(&scope.bindings),
                loader,
            )
            .with_catalog_application(owners.catalog_application.as_deref());
            needs
                .iter()
                .map(|need| catalog_fact(&materializer, need))
                .collect()
        })
        .await
    }
}

/// One relation, resolved and turned back into the answer to its own question.
///
/// Absence and failure are different answers, and only absence is the
/// compiler's to interpret: a relation that is not there is a fact about the
/// query, while a catalog that could not say is a fact about this process. The
/// second must not be flattened into the first, or a query over a healthy table
/// would report that the table does not exist whenever the catalog is down.
fn catalog_fact(
    materializer: &CatalogServiceMaterializer<'_>,
    need: &CatalogRelationNeed,
) -> Result<CatalogRelationFact, String> {
    let relation = need.relation();
    let resolved = match need.target() {
        CatalogLookupTarget::Table { .. } => materializer.resolve_table_for_analysis_typed(
            Some(&relation.catalog),
            &relation.namespace,
            &relation.table,
        ),
        CatalogLookupTarget::IcebergMetadata { kind } => materializer.resolve_metadata_table_typed(
            Some(&relation.catalog),
            &relation.namespace,
            &relation.table,
            kind,
        ),
    };
    match resolved {
        Ok(table) => CatalogRelationFact::resolved(need, table),
        Err(CatalogResolutionError::Missing { reason }) => {
            CatalogRelationFact::missing(need, reason)
        }
        Err(error @ CatalogResolutionError::Failed { .. }) => {
            return Err(error.into_message());
        }
    }
    .map_err(|error| format!("build catalog completion fact: {error}"))
}

/// Answers statistics questions from the one unified resolver.
pub(crate) struct FrontendStatisticsFacts {
    owners: CompletionFactOwners,
    scope: StatementFactScope,
}

impl FrontendStatisticsFacts {
    pub(crate) const fn new(owners: CompletionFactOwners, scope: StatementFactScope) -> Self {
        Self { owners, scope }
    }
}

#[async_trait]
impl StatisticsFactPort for FrontendStatisticsFacts {
    async fn resolve_statistics(
        &self,
        needs: &[StatisticsNeed],
    ) -> Result<Vec<StatisticsFact>, String> {
        let needs = needs.to_vec();
        let owners = self.owners.clone();
        let scope = self.scope.clone();
        admitted(&self.owners, move || {
            needs
                .iter()
                .map(|need| {
                    resolve_statistics_need(
                        owners.statistics.as_ref(),
                        scope.bindings.as_ref(),
                        need,
                        &scope.connector_context,
                    )
                })
                .collect()
        })
        .await
    }
}

/// Answers materialized-view discovery from the request-local inventory.
pub(crate) struct FrontendMaterializedViewFacts {
    owners: CompletionFactOwners,
}

impl FrontendMaterializedViewFacts {
    pub(crate) const fn new(owners: CompletionFactOwners) -> Self {
        Self { owners }
    }
}

#[async_trait]
impl MaterializedViewFactPort for FrontendMaterializedViewFacts {
    /// Rewrite is an accelerator, so an inventory that cannot answer produces a
    /// fact saying the view is unavailable. There is deliberately no error
    /// path here that a working query could take: a query that would have run
    /// without the rewrite must not fail because of it.
    async fn resolve_materialized_views(
        &self,
        needs: &[MaterializedViewNeed],
    ) -> Result<Vec<MaterializedViewFact>, String> {
        let needs = needs.to_vec();
        let owners = self.owners.clone();
        admitted(&self.owners, move || {
            Ok(needs
                .iter()
                .map(|need| {
                    freeze_materialized_view_fact_with_ports(
                        need,
                        &owners.materialized_views,
                        owners.connector_control.as_ref(),
                        owners.mv_storage_observation.as_ref(),
                    )
                })
                .collect())
        })
        .await
    }
}

/// Run one owner's synchronous answer on the bounded connector lane.
///
/// Losing the lane is not the owner's answer and must not be reported as one:
/// a query that was refused admission never asked its question.
async fn admitted<T, F>(owners: &CompletionFactOwners, call: F) -> Result<T, String>
where
    T: Send + 'static,
    F: FnOnce() -> Result<T, String> + Send + 'static,
{
    owners
        .blocking
        .spawn_ordinary(call)
        .finish()
        .await
        .map_err(|error| format!("frontend completion fact lane: {error}"))?
}

/// The fact source one statement's completion is driven with, with all four
/// kinds of question answered by this process.
pub(crate) fn frontend_fact_source(
    owners: CompletionFactOwners,
    scope: StatementFactScope,
    session: ConnectorSession,
) -> QueryCompletionFactSource<ConnectorReadAttemptAccess> {
    let provider_reads = Arc::new(FrontendProviderReadFacts::new(
        Arc::clone(&owners.connector_control),
        Arc::clone(&scope.bindings),
        session,
        scope.connector_context.clone(),
        owners.blocking.clone(),
    ));
    statement_fact_source(owners, scope, provider_reads)
}

/// The one fact source one statement's completion is driven with.
///
/// Assembling it in a single place is what makes "each kind of question reaches
/// exactly one owner" true of a whole statement rather than of one call site at
/// a time. Provider reads are supplied separately because, unlike the other
/// three, answering them takes a capability that belongs to the attempt.
pub(crate) fn statement_fact_source<A>(
    owners: CompletionFactOwners,
    scope: StatementFactScope,
    provider_reads: Arc<dyn ProviderReadFactPort<Access = A>>,
) -> QueryCompletionFactSource<A> {
    QueryCompletionFactSource::new(
        Arc::new(FrontendCatalogFacts::new(owners.clone(), scope.clone())),
        Arc::new(FrontendStatisticsFacts::new(owners.clone(), scope)),
        Arc::new(FrontendMaterializedViewFacts::new(owners)),
        provider_reads,
    )
}
