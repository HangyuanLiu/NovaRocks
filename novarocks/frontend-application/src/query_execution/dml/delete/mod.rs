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

//! Transitional reverse port for frontend-owned DELETE application routing.

pub(crate) mod equality;
pub(crate) mod standard;

use std::any::Any;
use std::sync::Arc;

use crate::query_execution::kernels::DmlExecutionKernel;
use novarocks_proto_codec::lifecycle::QueryOptions;
use novarocks_query_application::admitted_query_context::QueryExecutionContext;

/// One parser-owned DELETE variant admitted by the typed statement router.
///
/// The source is retained separately in [`PrepareDeleteRequest`] and may only
/// be sliced through the selected AST node's [`novarocks_parser::Span`].
#[derive(Clone, Copy, Debug)]
pub enum DeleteStatement<'a> {
    Predicate(&'a novarocks_parser::ast::Delete),
    Equality(&'a novarocks_parser::ast::AddEqualityDelete),
}

/// The exact parser-owned statement retained by the supervised write action.
/// Source text is never reparsed to rediscover the statement or its target.
#[derive(Clone, Debug)]
pub(crate) enum OwnedDeleteStatement {
    Predicate(novarocks_parser::ast::Delete),
    Equality(novarocks_parser::ast::AddEqualityDelete),
}

/// Resolve only local catalog admission and parser-owned target facts. The
/// action calls this before creating its metadata-I/O phase.
pub(crate) fn resolve_target(
    state: &DmlExecutionKernel,
    statement: &OwnedDeleteStatement,
    current_catalog: Option<&str>,
    current_database: &str,
) -> Result<(crate::catalog_application::resolver::TargetBackend, String), String> {
    match statement {
        OwnedDeleteStatement::Predicate(statement) => {
            standard::resolve_target(state, statement, current_catalog, current_database)
        }
        OwnedDeleteStatement::Equality(statement) => {
            equality::resolve_target(state, statement, current_catalog, current_database)
        }
    }
}

/// Frozen preparation facts retained before SQL compilation starts. The
/// session and original request context remain the statement's publication
/// authority even when a later phase fails or is cancelled.
pub(crate) struct PreparedDeleteWrite {
    state: DmlExecutionKernel,
    target: crate::catalog_application::resolver::TargetBackend,
    target_ref: String,
    base_snapshot_id: Option<i64>,
    delete_query: novarocks_parser::ast::Query,
    sql_write_input: novarocks_sql::planning::dml::DmlWritePlanInput,
    table_bindings: Arc<crate::catalog_application::query_bindings::QueryTableBindingStore>,
    execution: QueryExecutionContext,
    connector_context: novarocks_spi::connector::ConnectorRequestContext,
    write_session: Arc<crate::query_execution::write_session::ConnectorWriteSession>,
    distribution: novarocks_sql::compiler::RootDistributionRequirement,
}

/// The original Prepared allocation has one Arc header. Variable AST, SQL and
/// Provider backing keep their original invoices rather than graph estimates.
pub(crate) fn prepared_delete_source_layouts() -> Result<
    [std::alloc::Layout; 1],
    novarocks_type_contract::owned_resources::layout::LayoutResourceError,
> {
    Ok([
        novarocks_type_contract::owned_resources::layout::arc_layout(std::alloc::Layout::new::<
            PreparedDeleteWrite,
        >())?,
    ])
}

impl PreparedDeleteWrite {
    pub(crate) fn target(&self) -> &crate::catalog_application::resolver::TargetBackend {
        &self.target
    }

    pub(crate) fn target_ref(&self) -> &str {
        &self.target_ref
    }

    pub(crate) fn base_snapshot_id(&self) -> Option<i64> {
        self.base_snapshot_id
    }

    pub(crate) fn write_session(
        &self,
    ) -> Arc<crate::query_execution::write_session::ConnectorWriteSession> {
        Arc::clone(&self.write_session)
    }

    /// Called only after the action installed this exact Prepared in its stable
    /// source. This future borrows the already retained session and custody.
    pub(crate) async fn prepare_native_assembly_for_execution_async(
        &self,
        provider_source: &crate::query_execution::completion_facts::FrontendCompletionCustody,
        query_options: Option<QueryOptions>,
    ) -> Result<
        crate::query_execution::compiler::PreparedDmlWriteAssembly,
        crate::dml::error::DmlExecutionError,
    > {
        crate::query_execution::compiler::prepare_query_as_iceberg_write_with_write_session_and_constants_async(
            &self.state,
            Some(&self.target.catalog),
            &self.target.namespace,
            &self.delete_query,
            None,
            self.sql_write_input.clone(),
            Arc::clone(&self.table_bindings),
            query_options,
            self.distribution.clone(),
            &self.execution,
            &self.connector_context,
            Arc::clone(&self.write_session),
            provider_source,
        ).await
    }

    pub(crate) fn finalize(&self) -> Result<(), String> {
        self.state.catalog_service().invalidate_table(
            &self.target.catalog,
            &self.target.namespace,
            &self.target.table,
        )
    }
}

#[expect(
    clippy::too_many_arguments,
    reason = "The action passes its original statement, target, generation, context, identity and receiving authority independently."
)]
pub(crate) async fn prepare_delete_write_async(
    state: &DmlExecutionKernel,
    statement: &OwnedDeleteStatement,
    target: &crate::catalog_application::resolver::TargetBackend,
    target_ref: &str,
    planning_lease: novarocks_spi::connector::ConnectorControlPlanningLease,
    execution: QueryExecutionContext,
    connector_context: &novarocks_spi::connector::ConnectorRequestContext,
    operation_id: novarocks_spi::connector::ConnectorWriteOperationId,
    begin_receiving: Arc<crate::query_execution::write_session::WriteBeginReceiving>,
) -> Result<PreparedDeleteWrite, crate::dml::error::DmlExecutionError> {
    match statement {
        OwnedDeleteStatement::Predicate(statement) => {
            standard::prepare_delete_statement_async(
                state,
                statement,
                target,
                target_ref,
                planning_lease,
                execution,
                connector_context,
                operation_id,
                begin_receiving,
            )
            .await
        }
        OwnedDeleteStatement::Equality(statement) => {
            equality::prepare_equality_delete_statement_async(
                state,
                statement,
                target,
                target_ref,
                planning_lease,
                execution,
                connector_context,
                begin_receiving,
            )
            .await
        }
    }
}

/// Load through the already admitted generation, without acquiring a new lease
/// or using a synchronous joining metadata wrapper.
async fn load_delete_target_async(
    target: &crate::catalog_application::resolver::TargetBackend,
    planning_lease: novarocks_spi::connector::ConnectorControlPlanningLease,
    connector_context: &novarocks_spi::connector::ConnectorRequestContext,
) -> Result<crate::connector::write_target::ConnectorWriteTargetBinding, String> {
    let context =
        crate::connector::context_for_planning_lease(&planning_lease, connector_context.clone())?;
    let metadata = crate::connector::schema_creation::load_frontend_table_async(
        planning_lease.binding().metadata().as_ref(),
        novarocks_spi::connector::ConnectorTableRequest {
            table: novarocks_spi::connector::ConnectorTableIdentity {
                instance_id: planning_lease.binding().descriptor().instance_id.clone(),
                namespace: Arc::from(target.namespace.as_str()),
                table: Arc::from(target.table.as_str()),
            },
            resolution: novarocks_spi::connector::ConnectorTableResolution::StrictBaseTable,
            context,
        },
    )
    .await
    .map_err(|error| error.to_string())?;
    Ok(crate::connector::write_target::ConnectorWriteTargetBinding::new(metadata, planning_lease))
}

async fn begin_delete_write_session_async(
    state: &DmlExecutionKernel,
    planning_lease: &novarocks_spi::connector::ConnectorControlPlanningLease,
    write_lease: &novarocks_spi::connector::ConnectorWriteLease,
    request: novarocks_spi::connector::write_stack::ConnectorWriteBeginRequest,
    begin_receiving: &crate::query_execution::write_session::WriteBeginReceiving,
) -> Result<Arc<crate::query_execution::write_session::ConnectorWriteSession>, String> {
    let catalog_properties = write_lease.catalog_properties().cloned().ok_or_else(|| {
        "connector write lease has no immutable catalog runtime identity".to_string()
    })?;
    crate::query_execution::write_session::ConnectorWriteSession::begin_async_in(
        crate::connector::write_target::derive_write_stack_lease(
            state.typed_connector_control(),
            planning_lease,
        )?,
        catalog_properties,
        request,
        begin_receiving,
    )
    .await
    .map_err(|error| format!("begin connector write session: {error}"))
}

/// DELETE statements recognized by the frontend command router.
#[cfg(test)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DeleteStatementKind {
    Predicate,
    Equality,
}

impl DeleteStatement<'_> {
    #[cfg(test)]
    pub const fn kind(self) -> DeleteStatementKind {
        match self {
            Self::Predicate(_) => DeleteStatementKind::Predicate,
            Self::Equality(_) => DeleteStatementKind::Equality,
        }
    }
}

/// One admitted frontend DELETE request. `source` is retained only for exact
/// slices selected by a parser-owned AST span; it must not be reparsed or used
/// to rediscover the statement family.
pub struct PrepareDeleteRequest<'a> {
    pub publication_id: novarocks_spi::connector::LakePublicationId,
    pub statement: DeleteStatement<'a>,
    pub source: &'a str,
    pub current_catalog: Option<String>,
    pub current_database: String,
    pub query_options: Option<QueryOptions>,
    pub execution: QueryExecutionContext,
}

pub trait DeletePrepared: Send + Sync {
    fn as_any(&self) -> &dyn Any;
}

pub trait DeleteCommit: Send + Sync {
    fn as_any(&self) -> &dyn Any;
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DeleteOperation {
    pub publication_id: novarocks_spi::connector::LakePublicationId,
    pub catalog: String,
    pub namespace: String,
    pub table: String,
    pub target_ref: String,
    pub attempt_id: String,
    pub base_snapshot_id: Option<i64>,
}

pub struct PreparedDelete {
    pub operation: DeleteOperation,
    pub handle: Arc<dyn DeletePrepared>,
    pub sql_source: String,
}

pub enum DeleteWriteReport {
    NoOp,
    CommitRequired(Arc<dyn DeleteCommit>),
}

/// Borrowed native encoder input for a Core-sealed DELETE request. Holding the
/// guard keeps the exact request carrier unavailable for replacement or
/// execution until Frontend has produced the corresponding bundle.
pub(crate) trait PreparedDeleteExecution: Send + Sync {
    /// Submit this statement's write.
    ///
    /// The plan was encoded when it was prepared, so there is nothing for the
    /// caller to assemble first: a prepared write is already on the wire's
    /// terms.
    fn run(
        &self,
    ) -> Result<
        crate::query_execution::outcome::QueryExecutionResult,
        crate::dml::error::DmlExecutionError,
    >;
    /// The admitted request context this statement commits and reconciles
    /// under. It is the statement's own context, never a fresh one: a commit
    /// issued under different credentials is a different request.
    fn terminal_request_context(&self) -> novarocks_spi::connector::ConnectorRequestContext;
    fn finalize(&self) -> Result<(), String>;
}

/// Complete original DELETE input retained by its governed statement source.
pub struct DeleteStatementInput {
    pub(crate) statement: OwnedDeleteStatement,
    pub(crate) sql: String,
    pub(crate) context: novarocks_query_application::admitted_query_context::RequestContext,
    pub(crate) query_options: QueryOptions,
    pub(crate) launcher:
        Arc<dyn crate::query_execution::logical_execution::LogicalExecutionLauncher>,
    #[cfg(novarocks_dev_controls)]
    pub(crate) observation: Option<crate::preparation_diagnostics::StatementObservationHandle>,
}

/// One-to-one core capability used only by the frontend DML application owner.
// Design: ADR-0020 (docs/adr/ADR-0020-frontend-delete-application-owner.md)
pub trait DeleteEngine: Send + Sync {
    fn prepare_statement_action(
        &self,
        input: DeleteStatementInput,
    ) -> Result<
        Box<dyn novarocks_query_application::session_control::GovernedStatementAction>,
        String,
    >;

    fn prepare_delete(&self, request: PrepareDeleteRequest<'_>) -> Result<PreparedDelete, String>;

    fn run_delete(
        &self,
        prepared: &dyn DeletePrepared,
    ) -> Result<DeleteWriteReport, crate::dml::error::DmlExecutionError>;
    fn commit_delete_terminal(
        &self,
        _prepared: &dyn DeletePrepared,
        _commit: &dyn DeleteCommit,
    ) -> Result<
        novarocks_spi::connector::ExternalMutationOutcome<
            novarocks_spi::connector::ConnectorWriteReceipt,
        >,
        String,
    > {
        Err("DELETE engine does not expose a connector terminal outcome".to_string())
    }
    fn adjudicate_delete_publication(
        &self,
        _prepared: &dyn DeletePrepared,
        _commit: &dyn DeleteCommit,
        _evidence: novarocks_spi::connector::ExternalMutationEvidence,
    ) -> Result<
        novarocks_spi::connector::ExternalMutationOutcome<
            novarocks_spi::connector::ConnectorWriteReceipt,
        >,
        String,
    > {
        Err("DELETE engine does not expose same-session publication adjudication".to_string())
    }
    fn finalize_delete(&self, prepared: &dyn DeletePrepared) -> Result<(), String>;
}

impl DeleteEngine for DmlExecutionKernel {
    fn prepare_statement_action(
        &self,
        input: DeleteStatementInput,
    ) -> Result<
        Box<dyn novarocks_query_application::session_control::GovernedStatementAction>,
        String,
    > {
        crate::query::delete_action::prepare(self.clone(), input)
    }

    fn prepare_delete(&self, request: PrepareDeleteRequest<'_>) -> Result<PreparedDelete, String> {
        let connector_context = self.connector_request_context_for_execution(
            request.query_options.as_ref(),
            &request.execution,
        )?;
        let mut prepared = match request.statement {
            DeleteStatement::Predicate(statement) => standard::prepare_delete_statement(
                self,
                statement,
                request.source,
                request.current_catalog.as_deref(),
                &request.current_database,
                &request.execution,
                &connector_context,
                request.publication_id,
            ),
            DeleteStatement::Equality(statement) => equality::prepare_equality_delete_statement(
                self,
                statement,
                request.current_catalog.as_deref(),
                &request.current_database,
                &request.execution,
                &connector_context,
                request.publication_id,
            ),
        }?;
        prepared.sql_source = request.source.to_string();
        Ok(prepared)
    }

    fn run_delete(
        &self,
        prepared: &dyn DeletePrepared,
    ) -> Result<DeleteWriteReport, crate::dml::error::DmlExecutionError> {
        let prepared = downcast_prepared(prepared)?;
        let result = prepared.execution.run()?;
        delete_write_report_from_result(result, prepared.execution.terminal_request_context())
    }

    fn commit_delete_terminal(
        &self,
        prepared: &dyn DeletePrepared,
        commit: &dyn DeleteCommit,
    ) -> Result<
        novarocks_spi::connector::ExternalMutationOutcome<
            novarocks_spi::connector::ConnectorWriteReceipt,
        >,
        String,
    > {
        let prepared = downcast_prepared(prepared)?;
        let commit = commit
            .as_any()
            .downcast_ref::<CoreDeleteCommit>()
            .ok_or_else(|| "foreign DELETE commit handle".to_string())?;
        commit.commit(prepared.execution.terminal_request_context())
    }

    fn adjudicate_delete_publication(
        &self,
        prepared: &dyn DeletePrepared,
        commit: &dyn DeleteCommit,
        evidence: novarocks_spi::connector::ExternalMutationEvidence,
    ) -> Result<
        novarocks_spi::connector::ExternalMutationOutcome<
            novarocks_spi::connector::ConnectorWriteReceipt,
        >,
        String,
    > {
        let prepared = downcast_prepared(prepared)?;
        let commit = commit
            .as_any()
            .downcast_ref::<CoreDeleteCommit>()
            .ok_or_else(|| "foreign DELETE commit handle".to_string())?;
        commit
            .session
            .reconcile(evidence, prepared.execution.terminal_request_context())
            .map_err(|error| error.to_string())
    }

    fn finalize_delete(&self, prepared: &dyn DeletePrepared) -> Result<(), String> {
        downcast_prepared(prepared)?.execution.finalize()
    }
}

fn delete_write_report_from_result(
    result: crate::query_execution::outcome::QueryExecutionResult,
    context: novarocks_spi::connector::ConnectorRequestContext,
) -> Result<DeleteWriteReport, crate::dml::error::DmlExecutionError> {
    let Some(completion) = result.write_session else {
        return Ok(DeleteWriteReport::NoOp);
    };
    // A DELETE whose predicate matched nothing produced no commit fragment.
    // Committing that would publish an empty snapshot, so the session is
    // released instead and the statement reports a no-op.
    if completion.is_empty() {
        completion
            .session()
            .abort(context)
            .map_err(|error| error.to_string())?;
        return Ok(DeleteWriteReport::NoOp);
    }
    Ok(DeleteWriteReport::CommitRequired(Arc::new(
        CoreDeleteCommit::new(completion),
    )))
}

struct CorePreparedDelete {
    #[allow(
        dead_code,
        reason = "Retained for staged query-execution DML recovery and connector wiring."
    )]
    operation: DeleteOperation,
    execution: Arc<dyn PreparedDeleteExecution>,
}

impl DeletePrepared for CorePreparedDelete {
    fn as_any(&self) -> &dyn Any {
        self
    }
}

/// One DELETE's commit authority.
///
/// The completion is taken by the single commit, so a second attempt finds
/// nothing rather than asking the connector twice. The session stays beside it
/// because a `CommitUnknown` is resolved through the session that issued the
/// commit, never through a replacement.
struct CoreDeleteCommit {
    session: Arc<crate::query_execution::write_session::ConnectorWriteSession>,
    completion:
        std::sync::Mutex<Option<crate::query_execution::outcome::ConnectorWriteSessionCompletion>>,
    /// Set only after a commit that is known to have succeeded.
    affected_rows: std::sync::Mutex<Option<u64>>,
}

impl CoreDeleteCommit {
    fn new(completion: crate::query_execution::outcome::ConnectorWriteSessionCompletion) -> Self {
        Self {
            session: Arc::clone(completion.session()),
            completion: std::sync::Mutex::new(Some(completion)),
            affected_rows: std::sync::Mutex::new(None),
        }
    }

    fn commit(
        &self,
        context: novarocks_spi::connector::ConnectorRequestContext,
    ) -> Result<
        novarocks_spi::connector::ExternalMutationOutcome<
            novarocks_spi::connector::ConnectorWriteReceipt,
        >,
        String,
    > {
        let completion = self
            .completion
            .lock()
            .map_err(|_| "DELETE commit handle is poisoned".to_string())?
            .take()
            .ok_or_else(|| "DELETE write session was already committed".to_string())?;
        let committed =
            crate::query_execution::write_session::finish_write_session(completion, context)
                .map_err(|error| error.to_string())?;
        // Rows become reportable exactly here: after the external commit said
        // it succeeded, and never on a commit whose outcome is unknown.
        *self
            .affected_rows
            .lock()
            .map_err(|_| "DELETE commit handle is poisoned".to_string())? =
            committed.affected_rows();
        Ok(committed.into_outcome())
    }

    /// The rows a client may be told about. `None` until a known-successful
    /// commit has happened.
    #[allow(
        dead_code,
        reason = "The gated affected-row count is surfaced to the MySQL result by a later task."
    )]
    fn affected_rows(&self) -> Option<u64> {
        self.affected_rows.lock().ok().and_then(|rows| *rows)
    }
}

impl DeleteCommit for CoreDeleteCommit {
    fn as_any(&self) -> &dyn Any {
        self
    }
}

pub(crate) fn prepared_delete(
    operation: DeleteOperation,
    execution: Arc<dyn PreparedDeleteExecution>,
) -> PreparedDelete {
    PreparedDelete {
        operation: operation.clone(),
        handle: Arc::new(CorePreparedDelete {
            operation,
            execution,
        }),
        sql_source: String::new(),
    }
}

fn downcast_prepared(prepared: &dyn DeletePrepared) -> Result<&CorePreparedDelete, String> {
    prepared
        .as_any()
        .downcast_ref::<CorePreparedDelete>()
        .ok_or_else(|| "foreign DELETE prepared handle".to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use novarocks_parser::ast::{DmlStatement, Statement};

    #[test]
    fn typed_variant_determines_delete_kind() {
        let statements =
            novarocks_parser::parse("DELETE FROM t WHERE id = 1").expect("parse DELETE");
        let Statement::Dml(DmlStatement::Delete(delete)) = &statements[0] else {
            panic!("expected DELETE");
        };

        assert_eq!(
            DeleteStatement::Predicate(delete).kind(),
            DeleteStatementKind::Predicate
        );
    }

    fn session_result(
        session: &Arc<crate::query_execution::write_session::ConnectorWriteSession>,
        row_count: u64,
        fragments: Vec<(
            novarocks_spi::connector::write_stack::WriteTargetOrdinal,
            Vec<u8>,
        )>,
    ) -> crate::query_execution::outcome::QueryExecutionResult {
        crate::query_execution::outcome::QueryExecutionResult {
            query_result: novarocks_query_application::api::QueryResult::empty(),
            write_session: Some(
                crate::query_execution::outcome::ConnectorWriteSessionCompletion::for_test(
                    Arc::clone(session),
                    crate::query_execution::write_result::DecodedPreparedWriteSet::for_test(
                        row_count, fragments,
                    ),
                ),
            ),
            fragment_profiles: Vec::new(),
        }
    }

    /// A DELETE that matched rows commits exactly once, and its rows become
    /// reportable only after that commit succeeded.
    #[test]
    fn a_matching_delete_commits_its_session_once() {
        use crate::query_execution::write_session::tests as write_session_fixture;

        let fixture = write_session_fixture::fixture_with_outcome(
            1,
            16,
            write_session_fixture::known_committed(),
        );
        let ordinal =
            novarocks_spi::connector::write_stack::WriteTargetOrdinal::try_new(0).expect("ordinal");
        let report = delete_write_report_from_result(
            session_result(
                &fixture.session,
                4,
                vec![(ordinal, write_session_fixture::commit_fragment_bytes())],
            ),
            write_session_fixture::request_context(),
        )
        .expect("report");

        let DeleteWriteReport::CommitRequired(handle) = report else {
            panic!("a delete that staged a fragment must require a commit");
        };
        let commit = handle
            .as_any()
            .downcast_ref::<CoreDeleteCommit>()
            .expect("core delete commit handle");
        assert!(commit.affected_rows().is_none());

        commit
            .commit(write_session_fixture::request_context())
            .expect("commit");
        assert_eq!(fixture.session.finish_invocations(), 1);
        assert_eq!(fixture.recorded.lock().expect("recorded").finish, 1);
        assert_eq!(commit.affected_rows(), Some(4));
    }

    /// A DELETE whose predicate matched nothing produced no commit fragment.
    /// Committing that would publish a snapshot describing nothing, so the
    /// session is released and the connector is never asked to commit.
    #[test]
    fn a_delete_that_staged_nothing_releases_its_session_without_committing() {
        use crate::query_execution::write_session::tests as write_session_fixture;

        let fixture = write_session_fixture::fixture_with_outcome(
            1,
            16,
            write_session_fixture::known_committed(),
        );
        let report = delete_write_report_from_result(
            session_result(&fixture.session, 0, Vec::new()),
            write_session_fixture::request_context(),
        )
        .expect("report");

        assert!(matches!(report, DeleteWriteReport::NoOp));
        assert_eq!(fixture.session.finish_invocations(), 0);
        let recorded = fixture.recorded.lock().expect("recorded");
        assert_eq!(recorded.finish, 0);
        assert_eq!(recorded.abort, 1);
    }
}
