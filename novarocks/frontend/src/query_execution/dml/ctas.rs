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

//! Statement-specific reverse port for frontend-local CTAS orchestration.
//!
//! The frontend owns one non-durable statement attempt. Core owns pure SQL
//! preparation, the exact admitted execution context, source execution, and connector calls.
//! Opaque handles ensure the frontend cannot inspect or reconstruct compiler,
//! writer, or provider-private staged-table state.

use std::any::Any;
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use novarocks_spi::connector::{
    ConnectorColumnDefinition, ConnectorMutationFailure, ConnectorPartitionTransform,
    ConnectorStagedCreateLease, ConnectorStagedCreatePrepareOutcome,
    ConnectorStagedCreatePrepareRequest, ConnectorStagedCreatePublicationAdjudicationOutcome,
    ConnectorStagedCreatePublicationAdjudicationRequest, ConnectorStagedCreatePublishOutcome,
    ConnectorStagedCreatePublishRequest, ConnectorStagedTableHandle,
    ConnectorStagedWritePlanningRequest, ConnectorUnanchoredCtasCleanupLease,
    ConnectorWriteAdmissionPurpose, ConnectorWriteFieldRequest, ConnectorWriteInputRequest,
    ConnectorWriteIntent, ConnectorWriteLease, ConnectorWriteOperationCompletion,
    ConnectorWriteOperationId, ConnectorWritePreparationOutcome, ConnectorWritePreparationRequest,
    CreatePolicy, ExternalMutationEvidence, ExternalMutationFinalization, LakePublicationId,
};
use novarocks_user_error::UserError;

use crate::common::admitted_query_context::QueryExecutionContext;
use crate::query_execution::kernels::DmlExecutionKernel;
use novarocks_parser::ast::{
    CreateTableAsSelect, Literal, LiteralKind, PartitionTransform, Query, TablePartition,
    TableStatement,
};
use novarocks_parser::printer;
use novarocks_proto_codec::lifecycle::QueryOptions;
use novarocks_sql::semantic::ObjectName;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CtasCommand {
    pub target_parts: Vec<String>,
    pub if_not_exists: bool,
    pub source: Query,
    pub partitioning: Vec<ConnectorPartitionTransform>,
    pub properties: BTreeMap<Arc<str>, Arc<str>>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CtasAdmissionFailure {
    pub span: novarocks_parser::Span,
    pub message: String,
}

impl CtasCommand {
    /// Lower one already-admitted CTAS node to the narrow execution request.
    ///
    /// The embedded query is already a parser-owned typed AST. This method
    /// never reparses or canonicalizes the surrounding DDL text.
    pub fn from_typed(
        statement: &CreateTableAsSelect,
        _source: &str,
    ) -> Result<Self, CtasAdmissionFailure> {
        let TableStatement::Create(table) = &statement.table;
        if table.temporary || table.external {
            return Err(unsupported(
                table.span,
                "CTAS does not support TEMPORARY or EXTERNAL tables",
            ));
        }
        if table.like.is_some() {
            return Err(unsupported(
                table.span,
                "CTAS does not support CREATE TABLE LIKE",
            ));
        }
        if !table.columns.is_empty() {
            return Err(unsupported(
                table.span,
                "CTAS with explicit column definitions is not supported; use CREATE TABLE then INSERT instead",
            ));
        }
        if let Some(engine) = &table.engine
            && !engine.value.eq_ignore_ascii_case("iceberg")
        {
            return Err(unsupported(
                engine.span,
                format!("CTAS does not support ENGINE = {}", engine.value),
            ));
        }
        if let Some(key) = &table.key {
            return Err(unsupported(key.span, "CTAS does not support table keys"));
        }
        if let Some(distribution) = &table.distribution {
            return Err(unsupported(
                distribution.span,
                "CTAS does not support DISTRIBUTED BY",
            ));
        }
        if !table.order_by.is_empty() {
            return Err(unsupported(table.span, "CTAS does not support ORDER BY"));
        }

        let partitioning = match &table.partition {
            None => Vec::new(),
            Some(TablePartition::Transform(partition)) => partition
                .expressions
                .iter()
                .map(lower_partition_transform)
                .collect::<Result<Vec<_>, _>>()?,
            Some(TablePartition::LegacyRange(partition)) => {
                return Err(unsupported(
                    partition.span,
                    "CTAS does not support legacy RANGE partition definitions",
                ));
            }
        };
        let mut properties: BTreeMap<Arc<str>, Arc<str>> = BTreeMap::new();
        for property in &table.properties {
            let key = literal_text(&property.key)?;
            let value = literal_text(&property.value)?;
            if key.eq_ignore_ascii_case("format-version") && value != "3" {
                return Err(unsupported(
                    property.span,
                    format!("CTAS only supports format-version=3, got '{value}'"),
                ));
            }
            if (key.eq_ignore_ascii_case("row-lineage")
                || key.eq_ignore_ascii_case("write.row-lineage"))
                && !value.eq_ignore_ascii_case("true")
            {
                return Err(unsupported(
                    property.span,
                    format!("CTAS requires row-lineage=true, got '{value}'"),
                ));
            }
            properties.insert(Arc::from(key), Arc::from(value));
        }
        if let Some(comment) = &table.comment {
            properties.insert(Arc::from("comment"), Arc::from(literal_text(comment)?));
        }
        properties.retain(|key, _| {
            !key.eq_ignore_ascii_case("format-version")
                && !key.eq_ignore_ascii_case("write.row-lineage")
        });
        properties.insert(Arc::from("format-version"), Arc::from("3"));
        properties.insert(Arc::from("write.row-lineage"), Arc::from("true"));

        Ok(Self {
            target_parts: table
                .name
                .parts
                .iter()
                .map(|part| part.value.clone())
                .collect(),
            if_not_exists: table.if_not_exists,
            source: statement.query.clone(),
            partitioning,
            properties,
        })
    }
}

fn unsupported(span: novarocks_parser::Span, message: impl Into<String>) -> CtasAdmissionFailure {
    CtasAdmissionFailure {
        span,
        message: message.into(),
    }
}

fn literal_text(literal: &Literal) -> Result<String, CtasAdmissionFailure> {
    match &literal.kind {
        LiteralKind::Null => Err(unsupported(
            literal.span,
            "CTAS properties do not support NULL literals",
        )),
        LiteralKind::Boolean(value) => Ok(value.to_string()),
        LiteralKind::Number(value) | LiteralKind::String(value) | LiteralKind::HexString(value) => {
            Ok(value.clone())
        }
    }
}

fn lower_partition_transform(
    transform: &PartitionTransform,
) -> Result<ConnectorPartitionTransform, CtasAdmissionFailure> {
    let as_arc = |column: &novarocks_parser::ast::Ident| Arc::from(column.value.as_str());
    match transform {
        PartitionTransform::Identity { column, .. } => Ok(ConnectorPartitionTransform::Identity {
            column: as_arc(column),
        }),
        PartitionTransform::Year { column, .. } => Ok(ConnectorPartitionTransform::Year {
            column: as_arc(column),
        }),
        PartitionTransform::Month { column, .. } => Ok(ConnectorPartitionTransform::Month {
            column: as_arc(column),
        }),
        PartitionTransform::Day { column, .. } => Ok(ConnectorPartitionTransform::Day {
            column: as_arc(column),
        }),
        PartitionTransform::Hour { column, .. } => Ok(ConnectorPartitionTransform::Hour {
            column: as_arc(column),
        }),
        PartitionTransform::Bucket {
            buckets,
            column: column_name,
            span,
        } => Ok(ConnectorPartitionTransform::Bucket {
            column: as_arc(column_name),
            num_buckets: u32::try_from(*buckets)
                .map_err(|_| unsupported(*span, "CTAS bucket count exceeds u32"))?,
        }),
        PartitionTransform::Truncate {
            width,
            column: column_name,
            span,
        } => Ok(ConnectorPartitionTransform::Truncate {
            column: as_arc(column_name),
            width: u32::try_from(*width)
                .map_err(|_| unsupported(*span, "CTAS truncate width exceeds u32"))?,
        }),
        PartitionTransform::Void { column, .. } => Ok(ConnectorPartitionTransform::Void {
            column: as_arc(column),
        }),
    }
}

pub enum CtasTargetPreflightOutcome {
    Ready(PreparedCtasTargetPreflight),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CtasTargetPreflightFacts {
    pub provider_id: String,
    pub instance_id: String,
    pub control_runtime_id: [u8; 16],
    pub capability_version: u32,
    pub target_namespace: String,
    pub target_table: String,
}

pub trait CtasPreparedTargetPreflight: Send + Sync {
    fn as_any(&self) -> &dyn Any;
}

pub struct PreparedCtasTargetPreflight {
    pub facts: CtasTargetPreflightFacts,
    pub handle: Arc<dyn CtasPreparedTargetPreflight>,
}

pub trait CtasPreparedCatalogAction: Send + Sync {
    fn as_any(&self) -> &dyn Any;
}

pub struct PreparedCtasCatalogAction {
    pub input_digest: [u8; 32],
    pub handle: Arc<dyn CtasPreparedCatalogAction>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CtasFailureKind {
    InvalidRequest,
    NotFound,
    AlreadyExists,
    Conflict,
    Unsupported,
    Cancelled,
    DeadlineExceeded,
    Unavailable,
    Internal,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CtasFailure {
    pub kind: CtasFailureKind,
    pub message: String,
    pub(crate) user_error: Option<CtasUserError>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum CtasUserError {
    Analyze(novarocks_sql::analyze_error::AnalyzeError),
}

impl CtasFailure {
    /// Retains the typed failure until the CTAS statement boundary can render
    /// its parser-owned span against the original SQL source.
    fn analyze(error: novarocks_sql::analyze_error::AnalyzeError) -> Self {
        Self {
            kind: CtasFailureKind::InvalidRequest,
            message: error.message().to_string(),
            user_error: Some(CtasUserError::Analyze(error)),
        }
    }

    pub(crate) fn user_error(&self, source: Option<&str>) -> Option<UserError> {
        match self.user_error.as_ref()? {
            CtasUserError::Analyze(error) => Some(error.to_user_error(source)),
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct CtasPreparedSourceFacts {
    pub target_catalog: String,
    pub target_namespace: String,
    pub target_table: String,
    pub source_catalog: Option<String>,
    pub source_database: String,
    pub plan_digest: [u8; 32],
    pub schema_digest: [u8; 32],
    pub execution_identity: [u8; 32],
    pub output_columns: Vec<ConnectorColumnDefinition>,
}

pub struct PrepareCtasSourceRequest {
    pub command: CtasCommand,
    pub current_catalog: Option<String>,
    pub current_database: String,
    pub query_options: Option<QueryOptions>,
    pub execution: QueryExecutionContext,
}

pub trait CtasPreparedSource: Send + Sync {
    fn as_any(&self) -> &dyn Any;
    fn execution_identity(&self) -> [u8; 32];
}

pub struct PreparedCtasSource {
    pub facts: CtasPreparedSourceFacts,
    pub handle: Arc<dyn CtasPreparedSource>,
}

/// Opaque target session. A concrete core implementation retains the same
/// exact fenced-publication lease, fence, ordinary writer handle, opaque
/// locator and proof for foreground stage, publish and abort.
pub trait CtasPreparedTarget: Send + Sync {
    fn as_any(&self) -> &dyn Any;
}

pub trait CtasPreparedWrite: Send + Sync {
    fn as_any(&self) -> &dyn Any;
    fn execution_identity(&self) -> [u8; 32];
    fn native_encoding(&self) -> Result<CtasNativeEncoding<'_>, CtasFailure>;
}

/// Borrowed access to the exact Core-retained encoding input. Frontend may
/// inspect it only for the native encoder call; Core consumes the same input
/// when the resulting bundle is bound for dispatch.
pub struct CtasNativeEncoding<'a> {
    encoding: std::sync::MutexGuard<
        'a,
        Option<crate::query_execution::compiler::NativeFragmentEncodingInput>,
    >,
}

impl CtasNativeEncoding<'_> {
    pub fn input(
        &self,
    ) -> Result<&crate::query_execution::compiler::NativeFragmentEncodingInput, CtasFailure> {
        self.encoding
            .as_ref()
            .ok_or_else(|| internal_failure("CTAS native encoding input was already consumed"))
    }
}

/// Crash-only CTAS target facts.  Unlike the legacy fenced facts, this is
/// deliberately only the exact staged-create identity that can reach the
/// single publish frontier.  It contains no branch, fence, locator or cleanup
/// authority.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StandardCtasTargetFacts {
    pub provider_id: String,
    pub instance_id: String,
    pub control_runtime_id: [u8; 16],
    pub publication_id: LakePublicationId,
    pub target_handle_digest: [u8; 32],
}

pub struct PreparedStandardCtasTarget {
    pub facts: StandardCtasTargetFacts,
    pub handle: Arc<dyn CtasPreparedTarget>,
}

pub struct PreparedStandardCtasCatalogAction {
    pub input_digest: [u8; 32],
    pub handle: Arc<dyn CtasPreparedCatalogAction>,
}

pub enum StandardCtasStageOutcome {
    Prepared {
        target: PreparedStandardCtasTarget,
        receipt: novarocks_spi::connector::ConnectorStagedCreateReceipt,
    },
    KnownUncommitted {
        failure: CtasFailure,
    },
    CommitUnknown {
        failure: CtasFailure,
        evidence: ExternalMutationEvidence,
    },
}

pub struct PreparedStandardCtasWrite {
    pub target_facts: StandardCtasTargetFacts,
    pub write_operation_id: ConnectorWriteOperationId,
    pub cohort_set_digest: [u8; 32],
    pub execution_identity: [u8; 32],
    pub handle: Arc<dyn CtasPreparedWrite>,
}

pub enum StandardCtasPublishOutcome {
    Applied {
        receipt: novarocks_spi::connector::ConnectorStagedCreateReceipt,
        finalization: ExternalMutationFinalization,
    },
    NoOp {
        receipt: novarocks_spi::connector::ConnectorStagedCreateReceipt,
        finalization: ExternalMutationFinalization,
    },
    KnownUncommitted {
        failure: CtasFailure,
    },
    CommitUnknown {
        failure: CtasFailure,
        evidence: ExternalMutationEvidence,
    },
}

/// The only same-statement observation permitted after the standard CTAS
/// publication call returned an exact `CommitUnknown` evidence carrier.
/// It cannot represent a negative result as cleanup or another publication.
pub enum StandardCtasPublicationAdjudicationOutcome {
    Published {
        receipt: novarocks_spi::connector::ConnectorStagedCreateReceipt,
        finalization: ExternalMutationFinalization,
    },
    CommitUnknown {
        failure: CtasFailure,
    },
}

/// The standard staged-create path never carries a takeover fence. Keeping a
/// separate outcome type prevents the retired authority carrier from leaking
/// back into the crash-only CTAS frontier while legacy code is being removed.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum StandardCtasWriteOutcome {
    Completed {
        completion: ConnectorWriteOperationCompletion,
        execution_identity: [u8; 32],
    },
    KnownUncommitted {
        failure: CtasFailure,
    },
    CommitUnknown {
        failure: CtasFailure,
        evidence: ExternalMutationEvidence,
    },
}

/// One-to-one core capability consumed by the frontend CTAS application
/// owner. It is intentionally not a generic connector DML facade.
pub trait CtasEngine: Send + Sync {
    /// Crash-only standard staged-create preflight.  The standard path is
    /// intentionally separate from the legacy fenced surface while T50 still
    /// compiles the historical recovery implementation.
    fn preflight_standard_ctas_target(
        &self,
        _statement: &CreateTableAsSelect,
        _source: &str,
        _command: &CtasCommand,
        _current_catalog: Option<&str>,
        _current_database: &str,
    ) -> Result<CtasTargetPreflightOutcome, CtasFailure> {
        Err(standard_ctas_unsupported())
    }

    fn prepare_standard_ctas_source(
        &self,
        _preflight: &dyn CtasPreparedTargetPreflight,
        _request: PrepareCtasSourceRequest,
    ) -> Result<PreparedCtasSource, CtasFailure> {
        Err(standard_ctas_unsupported())
    }

    fn prepare_standard_ctas_target(
        &self,
        _source: &dyn CtasPreparedSource,
        _publication_id: LakePublicationId,
        _policy: CreatePolicy,
    ) -> Result<PreparedStandardCtasCatalogAction, CtasFailure> {
        Err(standard_ctas_unsupported())
    }

    fn stage_standard_ctas_target(
        &self,
        _action: &dyn CtasPreparedCatalogAction,
    ) -> Result<StandardCtasStageOutcome, CtasFailure> {
        Err(standard_ctas_unsupported())
    }

    fn prepare_standard_ctas_write(
        &self,
        _source: &dyn CtasPreparedSource,
        _target: &dyn CtasPreparedTarget,
        _write_operation_id: ConnectorWriteOperationId,
    ) -> Result<PreparedStandardCtasWrite, CtasFailure> {
        Err(standard_ctas_unsupported())
    }

    fn bind_standard_ctas_write_native_bundle(
        &self,
        _prepared: &dyn CtasPreparedWrite,
        _native_bundle: crate::query_execution::native_fragment::NativeFragmentAttachment,
    ) -> Result<(), CtasFailure> {
        Err(standard_ctas_unsupported())
    }

    fn execute_standard_ctas_write(
        &self,
        _prepared: &dyn CtasPreparedWrite,
    ) -> StandardCtasWriteOutcome {
        StandardCtasWriteOutcome::KnownUncommitted {
            failure: standard_ctas_unsupported(),
        }
    }

    fn prepare_standard_publish_ctas(
        &self,
        _target: &dyn CtasPreparedTarget,
        _publication_id: LakePublicationId,
        _completion: ConnectorWriteOperationCompletion,
    ) -> Result<PreparedStandardCtasCatalogAction, CtasFailure> {
        Err(standard_ctas_unsupported())
    }

    fn publish_standard_ctas(
        &self,
        _action: &dyn CtasPreparedCatalogAction,
    ) -> Result<StandardCtasPublishOutcome, CtasFailure> {
        Err(standard_ctas_unsupported())
    }

    /// Read-only, exact-evidence publication adjudication. Callers receive
    /// this permission only after one publish outcome reported `CommitUnknown`.
    fn adjudicate_standard_ctas_publication(
        &self,
        _target: &dyn CtasPreparedTarget,
        _evidence: ExternalMutationEvidence,
    ) -> Result<StandardCtasPublicationAdjudicationOutcome, CtasFailure> {
        Err(standard_ctas_unsupported())
    }
}

/// Core-private guard embedded in concrete prepared source/write handles.
/// It proves preparation is inert, preserves one execution identity, and
/// rejects any second execution before reaching the coordinator.
pub(crate) struct CtasSourceExecutionGate {
    execution_identity: [u8; 32],
    executed: AtomicBool,
    source_artifact: Arc<dyn Any + Send + Sync>,
    retained_execution: Mutex<Option<QueryExecutionContext>>,
}

/// Pure CTAS source artifact. It retains the one optimized tree and the exact
/// scan bindings produced by analysis so target preparation cannot trigger a
/// second SQL compilation or a current-generation metadata lookup.
pub(crate) struct PlannedCtasSourceQuery {
    source: novarocks_sql::planning::dml::DmlCtasSourcePlan,
    table_bindings: Arc<crate::catalog_application::query_bindings::QueryTableBindingStore>,
    optimizer_settings: novarocks_sql::compiler::SessionOptimizerSettings,
    connector_target_parallelism: std::num::NonZeroUsize,
}

#[allow(clippy::too_many_arguments)]
fn plan_query_for_ctas_source(
    state: &DmlExecutionKernel,
    current_catalog: Option<&str>,
    current_database: &str,
    query: &Query,
    execution: &QueryExecutionContext,
    connector_context: &novarocks_spi::connector::ConnectorRequestContext,
) -> Result<PlannedCtasSourceQuery, CtasFailure> {
    let mut query = query.clone();
    if crate::query_execution::planning::time_travel::has_time_travel_refs(&query) {
        crate::query_execution::planning::time_travel::rewrite_time_travel_refs(
            state,
            current_catalog,
            current_database,
            &mut query,
            connector_context,
        )
        .map_err(|error| match error {
            crate::query_execution::planning::time_travel::TimeTravelRewriteError::Engine(
                error,
            ) => internal_failure(error),
            crate::query_execution::planning::time_travel::TimeTravelRewriteError::Analyze(
                error,
            ) => CtasFailure::analyze(error),
        })?;
    }
    let catalog_service_snapshot =
        crate::catalog_application::query_catalog::catalog_service_snapshot(state);
    let analyzer_provider =
        crate::catalog_application::query_materializer::build_catalog_service_provider(
            current_catalog,
            &catalog_service_snapshot,
            state.connector_control().as_ref(),
            connector_context.clone(),
            novarocks_sql::planning::catalog::TableLookupMode::SchemaOnly,
            state.catalog_application().map(Arc::as_ref),
        );
    let table_bindings = analyzer_provider.query_table_bindings();
    let catalog_snapshot =
        novarocks_sql::compiler::SqlPlannerTableSnapshot::new(&analyzer_provider);
    let backend_count = std::num::NonZeroUsize::new(execution.topology().targets().len())
        .ok_or_else(|| internal_failure("CTAS requires a frozen non-empty backend topology"))?;
    let request = novarocks_sql::compiler::SqlAnalyzeRequest::new(
        novarocks_sql::compiler::SqlStatementInput::parsed_query(Box::new(query)),
        novarocks_sql::compiler::SqlCompileIntent::IcebergWrite {
            root_distribution: novarocks_sql::compiler::RootDistributionRequirement::Any,
        },
        novarocks_sql::compiler::SqlSessionContext {
            current_catalog: current_catalog.map(str::to_string),
            current_database: current_database.to_string(),
            optimizer_settings: execution.optimizer_settings().clone(),
        },
        novarocks_sql::compiler::SqlPlanningEnvironment::Distributed { backend_count },
        &catalog_snapshot,
        novarocks_sql::compiler::builtin_sql_function_catalog(),
        crate::query_execution::constant_eval::constant_evaluator(),
        None,
        novarocks_sql::compiler::SqlCompileControl::new(
            execution.deadline(),
            crate::query_execution::planning::sql_cancellation_observation(
                execution.cancellation().clone(),
            ),
        ),
    );
    let analyzed = novarocks_sql::compiler::SqlCompiler::analyze(request)
        .map_err(|error| match error {
            novarocks_sql::compiler::SqlCompileError::Analyze(error) => CtasFailure::analyze(error),
            error => internal_failure(error.to_string()),
        })?
        .into_pending()
        .map_err(|error| internal_failure(error.to_string()))?;
    let statistics =
        crate::query_execution::planning::statistics::QueryStatisticsContext::from_statistics_resolver_with_bindings(
            state,
            Arc::clone(&table_bindings),
            connector_context,
        )
        .map_err(internal_failure)?;
    let source = novarocks_sql::planning::dml::compile_ctas_source(
        novarocks_sql::compiler::SqlOptimizeRequest::new(analyzed, &statistics),
    )
    .map_err(internal_failure)?;
    Ok(PlannedCtasSourceQuery {
        source,
        table_bindings,
        optimizer_settings: execution.optimizer_settings().clone(),
        connector_target_parallelism: backend_count,
    })
}

fn prepare_planned_ctas_connector_write(
    state: &DmlExecutionKernel,
    planned: &PlannedCtasSourceQuery,
    input_schema: arrow::datatypes::SchemaRef,
    query_options: Option<QueryOptions>,
    connector_context: &novarocks_spi::connector::ConnectorRequestContext,
    template: crate::query_execution::contract::ConnectorWritePlanningTemplate,
) -> Result<
    (
        crate::query_execution::compiler::NativeFragmentEncodingInput,
        PendingCtasDistributedWrite,
    ),
    String,
> {
    let distributed = novarocks_sql::planning::dml::build_ctas_connector_write_distributed_plan(
        &planned.source,
        input_schema,
        &planned.optimizer_settings,
    )?;
    let prepared = crate::query_execution::preparation::prepare_fragments(
        &distributed,
        state.connector_control().as_ref(),
        connector_context,
        Some(planned.table_bindings.as_ref()),
        None,
        crate::query_execution::preparation::ScanPreparationOptions::new(
            planned
                .optimizer_settings
                .connector_static_predicate_pushdown_enabled(),
            planned.connector_target_parallelism,
            None,
        )
        .with_typed_connector_control(
            Arc::clone(state.typed_connector_control()),
            crate::query_execution::compiler::typed_connector_session()?,
        ),
    )?;
    let cohort_id = template.cohort_id();
    let exact_lease = template.lease();
    Ok((
        crate::query_execution::compiler::NativeFragmentEncodingInput::new(distributed, prepared),
        PendingCtasDistributedWrite {
            query_options,
            registration:
                crate::query_execution::contract::ConnectorWriteOperationRegistration::single(
                    template,
                ),
            cohort_id,
            lease: exact_lease,
        },
    ))
}

enum CoreCtasCatalogActionKind {
    StandardStage {
        preflight: CoreStandardCtasTargetPreflight,
        request: ConnectorStagedCreatePrepareRequest,
        target_slot: Arc<Mutex<Option<Arc<CoreStandardCtasTargetSession>>>>,
    },
    StandardPublish {
        target: Arc<CoreStandardCtasTargetSession>,
        request: ConnectorStagedCreatePublishRequest,
    },
}

struct CoreCtasCatalogAction {
    kind: CoreCtasCatalogActionKind,
    dispatched: AtomicBool,
}

impl CtasPreparedCatalogAction for CoreCtasCatalogAction {
    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl CoreCtasCatalogAction {
    fn begin_dispatch(&self) -> Result<(), CtasFailure> {
        self.dispatched
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .map(|_| ())
            .map_err(|_| CtasFailure {
                kind: CtasFailureKind::InvalidRequest,
                message: "prepared CTAS catalog action has already been dispatched".into(),
                user_error: None,
            })
    }
}

#[derive(Clone)]
struct CoreStandardCtasTargetPreflight {
    target: crate::catalog_application::resolver::TargetBackend,
    lease: ConnectorStagedCreateLease,
    _unanchored_ctas_cleanup_lease: novarocks_spi::connector::ConnectorUnanchoredCtasCleanupLease,
    write_lease: ConnectorWriteLease,
    context: novarocks_spi::connector::ConnectorRequestContext,
}

impl CtasPreparedTargetPreflight for CoreStandardCtasTargetPreflight {
    fn as_any(&self) -> &dyn Any {
        self
    }
}

struct CorePreparedStandardCtasSource {
    gate: Arc<CtasSourceExecutionGate>,
    preflight: CoreStandardCtasTargetPreflight,
    command: CtasCommand,
    target: crate::catalog_application::resolver::TargetBackend,
    query_options: Option<QueryOptions>,
    connector_context: novarocks_spi::connector::ConnectorRequestContext,
    output_schema: arrow::datatypes::SchemaRef,
    output_columns: Vec<ConnectorColumnDefinition>,
    target_session: Arc<Mutex<Option<Arc<CoreStandardCtasTargetSession>>>>,
    target_prepare_started: AtomicBool,
}

impl CtasPreparedSource for CorePreparedStandardCtasSource {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn execution_identity(&self) -> [u8; 32] {
        self.gate.execution_identity()
    }
}

struct CoreStandardCtasTargetSession {
    lease: ConnectorStagedCreateLease,
    write_lease: ConnectorWriteLease,
    handle: ConnectorStagedTableHandle,
    publication_id: LakePublicationId,
    context: novarocks_spi::connector::ConnectorRequestContext,
    write_plan_started: AtomicBool,
    write_unknown_latched: AtomicBool,
    publish_started: AtomicBool,
}

impl CoreStandardCtasTargetSession {
    fn facts(&self) -> StandardCtasTargetFacts {
        StandardCtasTargetFacts {
            provider_id: "iceberg".to_string(),
            instance_id: self.lease.instance_id().as_str().to_string(),
            control_runtime_id: self.lease.control_runtime_id().to_bytes(),
            publication_id: self.publication_id,
            target_handle_digest: self.handle.digest(),
        }
    }

    fn prepare_publish(
        &self,
        publication_id: LakePublicationId,
        completion: ConnectorWriteOperationCompletion,
    ) -> Result<ConnectorStagedCreatePublishRequest, CtasFailure> {
        if publication_id != self.publication_id {
            return Err(internal_failure(
                "standard CTAS publish changed its statement publication ID",
            ));
        }
        if self.write_unknown_latched.load(Ordering::Acquire) {
            return Err(CtasFailure {
                kind: CtasFailureKind::Unavailable,
                message: "standard CTAS writer outcome is unresolved; publication is forbidden"
                    .to_string(),
                user_error: None,
            });
        }
        if self
            .publish_started
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return Err(internal_failure(
                "standard CTAS publish has already been prepared",
            ));
        }
        Ok(ConnectorStagedCreatePublishRequest {
            operation_id: novarocks_spi::connector::ConnectorMutationOperationId::from_bytes(
                publication_id.to_bytes(),
            ),
            handle: self.handle.clone(),
            completion,
            context: self.context.clone(),
        })
    }
}

impl CtasPreparedTarget for CoreStandardCtasTargetSession {
    fn as_any(&self) -> &dyn Any {
        self
    }
}

trait CtasWriteTarget: Send + Sync {
    fn plan_write(
        &self,
        operation_id: ConnectorWriteOperationId,
        input_schema: arrow::datatypes::SchemaRef,
        context: novarocks_spi::connector::ConnectorRequestContext,
    ) -> Result<
        crate::query_execution::contract::ConnectorWritePlanningTemplate,
        novarocks_spi::connector::ConnectorError,
    >;

    fn bind_write(
        &self,
        completion: ConnectorWriteOperationCompletion,
    ) -> Result<(), novarocks_spi::connector::ConnectorError>;

    fn mark_write_unknown(&self) -> Result<(), novarocks_spi::connector::ConnectorError>;

    fn reconcile_write_completion(
        &self,
        completion: ConnectorWriteOperationCompletion,
    ) -> Result<(), novarocks_spi::connector::ConnectorError>;
}

impl CtasWriteTarget for CoreStandardCtasTargetSession {
    fn plan_write(
        &self,
        operation_id: ConnectorWriteOperationId,
        input_schema: arrow::datatypes::SchemaRef,
        context: novarocks_spi::connector::ConnectorRequestContext,
    ) -> Result<
        crate::query_execution::contract::ConnectorWritePlanningTemplate,
        novarocks_spi::connector::ConnectorError,
    > {
        if self.write_unknown_latched.load(Ordering::Acquire) {
            return Err(novarocks_spi::connector::ConnectorError::new(
                novarocks_spi::connector::ConnectorErrorKind::Unavailable,
                "standard CTAS writer outcome is unresolved",
            ));
        }
        if self
            .write_plan_started
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return Err(novarocks_spi::connector::ConnectorError::new(
                novarocks_spi::connector::ConnectorErrorKind::InvalidRequest,
                "standard CTAS staged target write has already been prepared",
            ));
        }
        let binding = self.lease.plan_write(ConnectorStagedWritePlanningRequest {
            handle: self.handle.clone(),
            operation_id,
            intent: ConnectorWriteIntent::Append,
            input_schema: Arc::clone(&input_schema),
            context,
        })?;
        let outcome = self
            .write_lease
            .prepare_write(ConnectorWritePreparationRequest {
                table: binding.table().clone(),
                target_ref: novarocks_spi::connector::ConnectorWriteTargetRef::main(),
                intent: binding.intent(),
                purpose: ConnectorWriteAdmissionPurpose::OrdinaryDml,
                input: ConnectorWriteInputRequest::Data {
                    fields: input_schema
                        .fields()
                        .iter()
                        .map(|field| ConnectorWriteFieldRequest::new(field.as_ref().clone()))
                        .collect(),
                },
                context: binding.context().clone(),
            })?;
        let preparation = match outcome {
            ConnectorWritePreparationOutcome::Prepared(preparation) => preparation,
            ConnectorWritePreparationOutcome::Denied(error) => return Err(error),
        };
        crate::query_execution::contract::ConnectorWritePlanningTemplate::activate_prepared(
            binding.operation_id(),
            preparation,
            binding.context().clone(),
            self.write_lease.clone(),
        )
    }

    fn bind_write(
        &self,
        completion: ConnectorWriteOperationCompletion,
    ) -> Result<(), novarocks_spi::connector::ConnectorError> {
        self.lease.bind_write(self.handle.clone(), completion)
    }

    fn mark_write_unknown(&self) -> Result<(), novarocks_spi::connector::ConnectorError> {
        self.write_unknown_latched.store(true, Ordering::Release);
        self.lease.mark_write_unknown(&self.handle)
    }

    fn reconcile_write_completion(
        &self,
        _completion: ConnectorWriteOperationCompletion,
    ) -> Result<(), novarocks_spi::connector::ConnectorError> {
        Err(novarocks_spi::connector::ConnectorError::new(
            novarocks_spi::connector::ConnectorErrorKind::Unsupported,
            "crash-only standard CTAS does not reconcile writer uncertainty in process",
        ))
    }
}

impl CtasSourceExecutionGate {
    pub(crate) fn new(
        execution_identity: [u8; 32],
        source_artifact: Arc<dyn Any + Send + Sync>,
        execution: QueryExecutionContext,
    ) -> Self {
        Self {
            execution_identity,
            executed: AtomicBool::new(false),
            source_artifact,
            retained_execution: Mutex::new(Some(execution)),
        }
    }

    pub(crate) const fn execution_identity(&self) -> [u8; 32] {
        self.execution_identity
    }

    pub(crate) fn source_artifact(&self) -> &(dyn Any + Send + Sync) {
        self.source_artifact.as_ref()
    }

    pub(crate) fn execute_once<T>(
        &self,
        expected_identity: [u8; 32],
        execute: impl FnOnce(&(dyn Any + Send + Sync), QueryExecutionContext) -> Result<T, String>,
    ) -> Result<T, String> {
        if expected_identity != self.execution_identity {
            return Err("CTAS prepared write execution identity mismatch".to_string());
        }
        if self
            .executed
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return Err("CTAS prepared source has already been executed".to_string());
        }
        let execution = self
            .retained_execution
            .lock()
            .map_err(|error| format!("CTAS execution context lock: {error}"))?
            .take()
            .ok_or_else(|| "CTAS admitted execution context was already consumed".to_string())?;
        execute(self.source_artifact(), execution)
    }
}

#[allow(
    dead_code,
    reason = "Retained for staged query-execution DML recovery and connector wiring."
)]
pub(crate) fn mutation_failure(failure: ConnectorMutationFailure) -> CtasFailure {
    let kind = match failure.kind() {
        novarocks_spi::connector::ConnectorMutationFailureKind::InvalidRequest => {
            CtasFailureKind::InvalidRequest
        }
        novarocks_spi::connector::ConnectorMutationFailureKind::NotFound => {
            CtasFailureKind::NotFound
        }
        novarocks_spi::connector::ConnectorMutationFailureKind::AlreadyExists => {
            CtasFailureKind::AlreadyExists
        }
        novarocks_spi::connector::ConnectorMutationFailureKind::Conflict => {
            CtasFailureKind::Conflict
        }
        novarocks_spi::connector::ConnectorMutationFailureKind::Unsupported => {
            CtasFailureKind::Unsupported
        }
        novarocks_spi::connector::ConnectorMutationFailureKind::Cancelled => {
            CtasFailureKind::Cancelled
        }
        novarocks_spi::connector::ConnectorMutationFailureKind::DeadlineExceeded => {
            CtasFailureKind::DeadlineExceeded
        }
        novarocks_spi::connector::ConnectorMutationFailureKind::Unavailable => {
            CtasFailureKind::Unavailable
        }
        _ => CtasFailureKind::Internal,
    };
    CtasFailure {
        kind,
        message: failure.message().to_string(),
        user_error: None,
    }
}

fn connector_failure(error: novarocks_spi::connector::ConnectorError) -> CtasFailure {
    use novarocks_spi::connector::ConnectorErrorKind;
    let kind = match error.kind() {
        ConnectorErrorKind::InvalidRequest => CtasFailureKind::InvalidRequest,
        ConnectorErrorKind::NotFound => CtasFailureKind::NotFound,
        ConnectorErrorKind::Unsupported => CtasFailureKind::Unsupported,
        ConnectorErrorKind::Cancelled => CtasFailureKind::Cancelled,
        ConnectorErrorKind::DeadlineExceeded => CtasFailureKind::DeadlineExceeded,
        ConnectorErrorKind::Unavailable => CtasFailureKind::Unavailable,
        ConnectorErrorKind::PermissionDenied
        | ConnectorErrorKind::ResourceExhausted
        | ConnectorErrorKind::CorruptData
        | ConnectorErrorKind::Internal => CtasFailureKind::Internal,
    };
    CtasFailure {
        kind,
        message: error.to_string(),
        user_error: None,
    }
}

fn internal_failure(message: impl Into<String>) -> CtasFailure {
    CtasFailure {
        kind: CtasFailureKind::Internal,
        message: message.into(),
        user_error: None,
    }
}

struct CorePreparedCtasWrite {
    state: DmlExecutionKernel,
    gate: Arc<CtasSourceExecutionGate>,
    target: Arc<dyn CtasWriteTarget>,
    native_encoding: Mutex<Option<crate::query_execution::compiler::NativeFragmentEncodingInput>>,
    pending: Mutex<Option<PendingCtasDistributedWrite>>,
    prepared:
        Mutex<Option<crate::query_execution::prepared_write::PreparedDistributedWriteRequest>>,
    completion: Mutex<Option<crate::query_execution::ConnectorWriteCompletion>>,
    write_session:
        Mutex<Option<crate::query_execution::write_operation::ConnectorWriteOperationSession>>,
    write_unknown: Mutex<Option<ExternalMutationEvidence>>,
    execution_identity: [u8; 32],
}

/// Core-retained write facts that are not part of the Frontend-owned native
/// encoding step. They are consumed exactly once when Frontend returns the
/// native bundle for the sealed plan/preparation pair.
struct PendingCtasDistributedWrite {
    query_options: Option<QueryOptions>,
    registration: crate::query_execution::contract::ConnectorWriteOperationRegistration,
    cohort_id: novarocks_spi::connector::ConnectorWriteCohortId,
    lease: ConnectorWriteLease,
}

impl CtasPreparedWrite for CorePreparedCtasWrite {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn execution_identity(&self) -> [u8; 32] {
        self.execution_identity
    }

    fn native_encoding(&self) -> Result<CtasNativeEncoding<'_>, CtasFailure> {
        let encoding = self
            .native_encoding
            .lock()
            .map_err(|error| internal_failure(format!("CTAS native encoding lock: {error}")))?;
        if encoding.is_none() {
            return Err(internal_failure(
                "CTAS native encoding input was already consumed",
            ));
        }
        Ok(CtasNativeEncoding { encoding })
    }
}

fn downcast_standard_source(
    source: &dyn CtasPreparedSource,
) -> Result<&CorePreparedStandardCtasSource, CtasFailure> {
    source
        .as_any()
        .downcast_ref::<CorePreparedStandardCtasSource>()
        .ok_or_else(|| {
            internal_failure("CTAS source handle does not belong to the standard core engine")
        })
}

fn downcast_standard_preflight(
    preflight: &dyn CtasPreparedTargetPreflight,
) -> Result<&CoreStandardCtasTargetPreflight, CtasFailure> {
    preflight
        .as_any()
        .downcast_ref::<CoreStandardCtasTargetPreflight>()
        .ok_or_else(|| {
            internal_failure("CTAS target preflight does not belong to the standard core engine")
        })
}

fn sweep_unanchored_ctas_roots(
    kernel: &DmlExecutionKernel,
    lease: &ConnectorUnanchoredCtasCleanupLease,
    context: novarocks_spi::connector::ConnectorRequestContext,
) -> Result<(), CtasFailure> {
    let policy = kernel.lake_publication_runtime_policy().ok_or_else(|| CtasFailure {
        kind: CtasFailureKind::Unsupported,
        message: "standard CTAS is unsupported until the shared lake publication GC policy is installed"
            .to_string(),
        user_error: None,
    })?;
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| internal_failure("system clock is before Unix epoch"))?;
    let now_ms = i64::try_from(now.as_millis())
        .map_err(|_| internal_failure("system clock exceeds i64 milliseconds"))?;
    let safe_age_ms = i64::try_from(policy.safe_gc_age().as_millis())
        .map_err(|_| internal_failure("lake publication safe GC age exceeds i64 milliseconds"))?;
    let cutoff_ms = now_ms
        .checked_sub(safe_age_ms)
        .ok_or_else(|| internal_failure("lake publication safe GC cutoff underflows"))?;
    let warehouse_root = lease.warehouse_root().map_err(connector_failure)?;
    let candidates = lease
        .discover_unanchored_ctas(warehouse_root.clone(), cutoff_ms, context.clone())
        .map_err(connector_failure)?;
    for provenance in candidates {
        // An exact delete whose acknowledgement is uncertain stays an aged
        // residue for a later GC pass. It never grants this new CTAS attempt
        // authority to reconcile, retry, or infer the old publication.
        let _ = lease
            .inspect_then_delete_unanchored_ctas(
                warehouse_root.clone(),
                cutoff_ms,
                provenance,
                context.clone(),
            )
            .map_err(connector_failure)?;
    }
    Ok(())
}

fn downcast_catalog_action(
    action: &dyn CtasPreparedCatalogAction,
) -> Result<&CoreCtasCatalogAction, CtasFailure> {
    action
        .as_any()
        .downcast_ref::<CoreCtasCatalogAction>()
        .ok_or_else(|| internal_failure("CTAS catalog action does not belong to the core engine"))
}

fn prepared_catalog_action(
    input_digest: [u8; 32],
    kind: CoreCtasCatalogActionKind,
) -> PreparedCtasCatalogAction {
    PreparedCtasCatalogAction {
        input_digest,
        handle: Arc::new(CoreCtasCatalogAction {
            kind,
            dispatched: AtomicBool::new(false),
        }),
    }
}

fn downcast_standard_target(
    target: &dyn CtasPreparedTarget,
) -> Result<&CoreStandardCtasTargetSession, CtasFailure> {
    target
        .as_any()
        .downcast_ref::<CoreStandardCtasTargetSession>()
        .ok_or_else(|| {
            internal_failure("CTAS target handle does not belong to the standard core engine")
        })
}

fn downcast_write(write: &dyn CtasPreparedWrite) -> Result<&CorePreparedCtasWrite, CtasFailure> {
    write
        .as_any()
        .downcast_ref::<CorePreparedCtasWrite>()
        .ok_or_else(|| internal_failure("CTAS write handle does not belong to the core engine"))
}

fn sha256(parts: &[&[u8]]) -> [u8; 32] {
    use sha2::{Digest, Sha256};
    let mut digest = Sha256::new();
    for part in parts {
        digest.update((part.len() as u64).to_be_bytes());
        digest.update(part);
    }
    digest.finalize().into()
}

fn standard_stage_input_digest(request: &ConnectorStagedCreatePrepareRequest) -> [u8; 32] {
    let policy = match request.policy {
        CreatePolicy::FailIfExists => b"fail".as_slice(),
        CreatePolicy::NoOpIfExists => b"noop".as_slice(),
    };
    let columns = request
        .columns
        .iter()
        .map(|column| format!("{column:?}"))
        .collect::<Vec<_>>()
        .join("\n");
    let partitioning = request
        .partitioning
        .iter()
        .map(|transform| format!("{transform:?}"))
        .collect::<Vec<_>>()
        .join("\n");
    let properties = request
        .properties
        .iter()
        .map(|(key, value)| format!("{key}={value}"))
        .collect::<Vec<_>>()
        .join("\n");
    sha256(&[
        b"novarocks.standard-ctas-stage.v1",
        &request.publication_id.to_bytes(),
        request.table.instance_id.as_str().as_bytes(),
        request.table.namespace.as_bytes(),
        request.table.table.as_bytes(),
        policy,
        columns.as_bytes(),
        partitioning.as_bytes(),
        properties.as_bytes(),
    ])
}

fn standard_publish_input_digest(request: &ConnectorStagedCreatePublishRequest) -> [u8; 32] {
    sha256(&[
        b"novarocks.standard-ctas-publish.v1",
        &request.operation_id.to_bytes(),
        &request.handle.digest(),
        &request.completion.aggregate_digest(),
    ])
}

fn standard_ctas_unsupported() -> CtasFailure {
    CtasFailure {
        kind: CtasFailureKind::Unsupported,
        message: "connector does not support crash-only standard CTAS staged-create".to_string(),
        user_error: None,
    }
}

fn write_staging_evidence(
    session: &crate::query_execution::write_operation::ConnectorWriteOperationSession,
) -> ExternalMutationEvidence {
    let mut payload = Vec::with_capacity(8 + 16 + 16 + 32);
    payload.extend_from_slice(b"CTASWS1\0");
    payload.extend_from_slice(&session.operation_id().to_bytes());
    payload.extend_from_slice(&session.owner().incarnation.to_bytes());
    payload.extend_from_slice(&session.sealed().digest());
    ExternalMutationEvidence::try_new(
        1,
        novarocks_spi::connector::ConnectorInstanceDescriptor {
            provider_id: novarocks_spi::connector::ConnectorProviderId::parse("iceberg")
                .expect("static Iceberg provider ID is valid"),
            instance_id: session.owner().instance_id.clone(),
        },
        session.owner().incarnation,
        novarocks_spi::connector::ConnectorMutationOperationId::from_bytes(
            session.operation_id().to_bytes(),
        ),
        "ctas-write-staging",
        Bytes::from(payload),
    )
    .expect("bounded CTAS writer evidence is valid")
}

fn standard_write_commit_unknown(
    prepared: &CorePreparedCtasWrite,
    session: &crate::query_execution::write_operation::ConnectorWriteOperationSession,
    message: impl Into<String>,
) -> StandardCtasWriteOutcome {
    let mut failure_message = message.into();
    let evidence = {
        let mut stored = prepared
            .write_unknown
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if stored.is_none() {
            let evidence = write_staging_evidence(session);
            if let Err(error) = prepared.target.mark_write_unknown() {
                failure_message.push_str(&format!(
                    "; standard CTAS target rejected write-unknown transition: {error}"
                ));
            }
            *stored = Some(evidence);
        }
        stored
            .as_ref()
            .expect("CTAS evidence was installed")
            .clone()
    };
    StandardCtasWriteOutcome::CommitUnknown {
        failure: CtasFailure {
            kind: CtasFailureKind::Unavailable,
            message: failure_message,
            user_error: None,
        },
        evidence,
    }
}

impl CtasEngine for DmlExecutionKernel {
    fn preflight_standard_ctas_target(
        &self,
        _statement: &CreateTableAsSelect,
        _source: &str,
        command: &CtasCommand,
        current_catalog: Option<&str>,
        current_database: &str,
    ) -> Result<CtasTargetPreflightOutcome, CtasFailure> {
        let target = crate::catalog_application::resolver::resolve_table_target(
            self,
            &ObjectName {
                parts: command.target_parts.clone(),
            },
            current_catalog,
            current_database,
        )
        .map_err(internal_failure)?;
        let context =
            crate::connector::connector_request_context(None, Arc::new(AtomicBool::new(false)))
                .map_err(internal_failure)?;
        let instance_id = novarocks_spi::connector::ConnectorInstanceId::parse(&target.catalog)
            .map_err(connector_failure)?;
        let planning = self
            .connector_control()
            .acquire_current(&instance_id)
            .map_err(connector_failure)?;
        // The standard staged-create capability is the only mutation
        // authority.  Acquiring it before source planning preserves the
        // unsupported-at-zero-side-effect boundary.
        let lease = planning
            .derive_staged_create_lease()
            .map_err(connector_failure)?;
        let unanchored_ctas_cleanup_lease = planning
            .derive_unanchored_ctas_cleanup_lease()
            .map_err(connector_failure)?;
        let write_lease = planning.derive_write_lease().map_err(connector_failure)?;
        if !lease.matches_write_lease(&write_lease)
            || lease.control_runtime_id() != unanchored_ctas_cleanup_lease.control_runtime_id()
        {
            return Err(internal_failure(
                "standard CTAS staged-create, cleanup, and writer leases do not share one exact generation",
            ));
        }
        sweep_unanchored_ctas_roots(self, &unanchored_ctas_cleanup_lease, context.clone())?;
        // The provider's staged-create publication contract is the only
        // authority allowed to decide whether IF NOT EXISTS is a no-op. A
        // frontend metadata read is not a substitute for its exact commit
        // result and would race the publication frontier.
        let binding = planning.binding();
        Ok(CtasTargetPreflightOutcome::Ready(
            PreparedCtasTargetPreflight {
                facts: CtasTargetPreflightFacts {
                    provider_id: binding.descriptor().provider_id.as_str().to_string(),
                    instance_id: binding.descriptor().instance_id.as_str().to_string(),
                    control_runtime_id: lease.control_runtime_id().to_bytes(),
                    capability_version:
                        novarocks_spi::connector::CONNECTOR_STAGED_CREATE_CONTRACT_VERSION,
                    target_namespace: target.namespace.clone(),
                    target_table: target.table.clone(),
                },
                handle: Arc::new(CoreStandardCtasTargetPreflight {
                    target,
                    lease,
                    _unanchored_ctas_cleanup_lease: unanchored_ctas_cleanup_lease,
                    write_lease,
                    context,
                }),
            },
        ))
    }

    fn prepare_standard_ctas_source(
        &self,
        preflight: &dyn CtasPreparedTargetPreflight,
        request: PrepareCtasSourceRequest,
    ) -> Result<PreparedCtasSource, CtasFailure> {
        let preflight = downcast_standard_preflight(preflight)?;
        let target = crate::catalog_application::resolver::resolve_table_target(
            self,
            &ObjectName {
                parts: request.command.target_parts.clone(),
            },
            request.current_catalog.as_deref(),
            &request.current_database,
        )
        .map_err(internal_failure)?;
        if target != preflight.target {
            return Err(CtasFailure {
                kind: CtasFailureKind::InvalidRequest,
                message: "standard CTAS source target does not match its exact preflight"
                    .to_string(),
                user_error: None,
            });
        }
        let connector_context = crate::connector::connector_request_context_for_execution(
            request.query_options.as_ref(),
            &request.execution,
        )
        .map_err(internal_failure)?;
        let planned = plan_query_for_ctas_source(
            self,
            request.current_catalog.as_deref(),
            &request.current_database,
            &request.command.source,
            &request.execution,
            &connector_context,
        )?;
        let source_columns = planned.source.output_columns();
        if source_columns.is_empty() {
            return Err(CtasFailure {
                kind: CtasFailureKind::InvalidRequest,
                message: "CTAS source has no output columns".to_string(),
                user_error: None,
            });
        }
        let output_schema = Arc::new(arrow::datatypes::Schema::new(
            source_columns
                .iter()
                .map(|column| {
                    arrow::datatypes::Field::new(
                        &column.name,
                        column.data_type.clone(),
                        column.nullable,
                    )
                })
                .collect::<Vec<_>>(),
        ));
        let table_columns =
            crate::query_execution::dml::iceberg_ctas::arrow_schema_to_table_column_defs(
                output_schema.as_ref(),
            )
            .map_err(internal_failure)?;
        let output_columns = table_columns
            .iter()
            .map(crate::catalog_application::statement::connector_column)
            .collect::<Result<Vec<_>, _>>()
            .map_err(internal_failure)?;
        let schema_text = format!("{output_schema:?}");
        let optimized_fingerprint = planned.source.capture_fingerprint();
        let settings_material =
            novarocks_sql::planning::dml::optimizer_settings_stable_digest_material(
                &planned.optimizer_settings,
            );
        let binding_material = planned.table_bindings.stable_digest_material();
        let execution_nonce = uuid::Uuid::now_v7();
        let execution_identity =
            sha256(&[b"novarocks.ctas-execution.v1", execution_nonce.as_bytes()]);
        let plan_digest = sha256(&[
            b"novarocks.ctas-plan.v1",
            printer::print_query(&request.command.source).as_bytes(),
            optimized_fingerprint.as_slice(),
            settings_material.as_slice(),
            binding_material.as_slice(),
        ]);
        let schema_digest = sha256(&[schema_text.as_bytes()]);
        let gate = Arc::new(CtasSourceExecutionGate::new(
            execution_identity,
            Arc::new(planned),
            request.execution,
        ));
        Ok(PreparedCtasSource {
            facts: CtasPreparedSourceFacts {
                target_catalog: target.catalog.clone(),
                target_namespace: target.namespace.clone(),
                target_table: target.table.clone(),
                source_catalog: request.current_catalog.clone(),
                source_database: request.current_database.clone(),
                plan_digest,
                schema_digest,
                execution_identity,
                output_columns: output_columns.clone(),
            },
            handle: Arc::new(CorePreparedStandardCtasSource {
                gate,
                preflight: preflight.clone(),
                command: request.command,
                target,
                query_options: request.query_options,
                connector_context,
                output_schema,
                output_columns,
                target_session: Arc::new(Mutex::new(None)),
                target_prepare_started: AtomicBool::new(false),
            }),
        })
    }

    fn prepare_standard_ctas_target(
        &self,
        source: &dyn CtasPreparedSource,
        publication_id: LakePublicationId,
        policy: CreatePolicy,
    ) -> Result<PreparedStandardCtasCatalogAction, CtasFailure> {
        let source = downcast_standard_source(source)?;
        if source
            .target_prepare_started
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return Err(CtasFailure {
                kind: CtasFailureKind::InvalidRequest,
                message: "standard CTAS target preparation has already been attempted".to_string(),
                user_error: None,
            });
        }
        let request = source.preflight.lease.prepare_request(
            publication_id,
            novarocks_spi::connector::ConnectorMutationOperationId::from_bytes(
                publication_id.to_bytes(),
            ),
            novarocks_spi::connector::ConnectorTableIdentity {
                instance_id: source.preflight.lease.instance_id().clone(),
                namespace: Arc::from(source.target.namespace.as_str()),
                table: Arc::from(source.target.table.as_str()),
            },
            source.output_columns.clone(),
            source.command.partitioning.clone(),
            source.command.properties.clone(),
            policy,
            source.connector_context.clone(),
        );
        Ok(PreparedStandardCtasCatalogAction {
            input_digest: standard_stage_input_digest(&request),
            handle: Arc::new(CoreCtasCatalogAction {
                kind: CoreCtasCatalogActionKind::StandardStage {
                    preflight: source.preflight.clone(),
                    request,
                    target_slot: Arc::clone(&source.target_session),
                },
                dispatched: AtomicBool::new(false),
            }),
        })
    }

    fn stage_standard_ctas_target(
        &self,
        action: &dyn CtasPreparedCatalogAction,
    ) -> Result<StandardCtasStageOutcome, CtasFailure> {
        let action = downcast_catalog_action(action)?;
        let CoreCtasCatalogActionKind::StandardStage {
            preflight,
            request,
            target_slot,
        } = &action.kind
        else {
            return Err(internal_failure(
                "CTAS catalog action is not a standard stage action",
            ));
        };
        action.begin_dispatch()?;
        match preflight
            .lease
            .prepare(request.clone())
            .map_err(connector_failure)?
        {
            ConnectorStagedCreatePrepareOutcome::Prepared {
                handle, receipt, ..
            } => {
                let target = Arc::new(CoreStandardCtasTargetSession {
                    lease: preflight.lease.clone(),
                    write_lease: preflight.write_lease.clone(),
                    handle,
                    publication_id: request.publication_id,
                    context: request.context.clone(),
                    write_plan_started: AtomicBool::new(false),
                    write_unknown_latched: AtomicBool::new(false),
                    publish_started: AtomicBool::new(false),
                });
                *target_slot
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(Arc::clone(&target));
                Ok(StandardCtasStageOutcome::Prepared {
                    target: PreparedStandardCtasTarget {
                        facts: target.facts(),
                        handle: target,
                    },
                    receipt,
                })
            }
            ConnectorStagedCreatePrepareOutcome::Conflict { failure }
            | ConnectorStagedCreatePrepareOutcome::KnownUncommitted { failure } => {
                Ok(StandardCtasStageOutcome::KnownUncommitted {
                    failure: mutation_failure(failure),
                })
            }
            ConnectorStagedCreatePrepareOutcome::CommitUnknown { failure, evidence } => {
                Ok(StandardCtasStageOutcome::CommitUnknown {
                    failure: mutation_failure(failure),
                    evidence,
                })
            }
        }
    }

    fn prepare_standard_ctas_write(
        &self,
        source: &dyn CtasPreparedSource,
        target: &dyn CtasPreparedTarget,
        write_operation_id: ConnectorWriteOperationId,
    ) -> Result<PreparedStandardCtasWrite, CtasFailure> {
        let source = downcast_standard_source(source)?;
        let target = downcast_standard_target(target)?;
        if source.gate.execution_identity() != source.execution_identity() {
            return Err(internal_failure(
                "standard CTAS source execution identity drift",
            ));
        }
        let target_arc = source
            .target_session
            .lock()
            .map_err(|error| {
                internal_failure(format!("standard CTAS target session lock: {error}"))
            })?
            .clone()
            .ok_or_else(|| {
                internal_failure("standard CTAS source has no retained target session")
            })?;
        if !std::ptr::eq(target_arc.as_ref(), target) {
            return Err(internal_failure(
                "standard CTAS target does not match the source-retained exact session",
            ));
        }
        let template = target
            .plan_write(
                write_operation_id,
                Arc::clone(&source.output_schema),
                source.connector_context.clone(),
            )
            .map_err(connector_failure)?;
        let planned = source
            .gate
            .source_artifact()
            .downcast_ref::<PlannedCtasSourceQuery>()
            .ok_or_else(|| {
                internal_failure("standard CTAS retained source artifact type mismatch")
            })?;
        let (native_encoding, pending) = prepare_planned_ctas_connector_write(
            self,
            planned,
            Arc::clone(&source.output_schema),
            source.query_options.clone(),
            &source.connector_context,
            template,
        )
        .map_err(internal_failure)?;
        let cohort_set_digest = pending
            .registration
            .clone()
            .sealed_cohorts()
            .map_err(connector_failure)?
            .digest();
        let identity = source.gate.execution_identity();
        let target_facts = target_arc.facts();
        let target_for_write: Arc<dyn CtasWriteTarget> = target_arc;
        Ok(PreparedStandardCtasWrite {
            target_facts,
            write_operation_id,
            cohort_set_digest,
            execution_identity: identity,
            handle: Arc::new(CorePreparedCtasWrite {
                state: self.clone(),
                gate: Arc::clone(&source.gate),
                target: target_for_write,
                native_encoding: Mutex::new(Some(native_encoding)),
                pending: Mutex::new(Some(pending)),
                prepared: Mutex::new(None),
                completion: Mutex::new(None),
                write_session: Mutex::new(None),
                write_unknown: Mutex::new(None),
                execution_identity: identity,
            }),
        })
    }

    fn bind_standard_ctas_write_native_bundle(
        &self,
        prepared: &dyn CtasPreparedWrite,
        native_bundle: crate::query_execution::native_fragment::NativeFragmentAttachment,
    ) -> Result<(), CtasFailure> {
        let prepared = downcast_write(prepared)?;
        let pending = prepared
            .pending
            .lock()
            .map_err(|error| internal_failure(format!("CTAS pending write lock: {error}")))?
            .take()
            .ok_or_else(|| internal_failure("CTAS native bundle was already bound"))?;
        let encoding = prepared
            .native_encoding
            .lock()
            .map_err(|error| internal_failure(format!("CTAS native encoding lock: {error}")))?
            .take()
            .ok_or_else(|| internal_failure("CTAS native encoding input was already consumed"))?;
        if !encoding.matches_native_attachment(&native_bundle) {
            return Err(internal_failure(
                "native fragment bundle does not match the sealed CTAS encoding input",
            ));
        }
        let (_, prepared_fragments) = encoding.into_parts();
        let request = crate::query_execution::prepared_write::PreparedDistributedWriteRequest::new(
            prepared_fragments,
            native_bundle,
            pending.query_options,
            pending.registration,
            pending.cohort_id,
            pending.lease,
        )
        .map_err(|error| internal_failure(error.to_string()))?;
        *prepared
            .prepared
            .lock()
            .map_err(|error| internal_failure(format!("CTAS prepared write lock: {error}")))? =
            Some(request);
        Ok(())
    }

    fn execute_standard_ctas_write(
        &self,
        prepared: &dyn CtasPreparedWrite,
    ) -> StandardCtasWriteOutcome {
        let prepared = match downcast_write(prepared) {
            Ok(prepared) => prepared,
            Err(failure) => return StandardCtasWriteOutcome::KnownUncommitted { failure },
        };
        let result = prepared
            .gate
            .execute_once(prepared.execution_identity, |_, execution| {
                let request = prepared
                    .prepared
                    .lock()
                    .map_err(|error| format!("CTAS prepared write lock: {error}"))?
                    .take()
                    .ok_or_else(|| "CTAS prepared write was already consumed".to_string())?;
                let cohort_id = request.write_cohort_id();
                let session = prepared
                    .state
                    .query_execution()
                    .begin_write_operation(request.registration(), request.lease())
                    .map_err(|error| error.to_string())?;
                *prepared
                    .write_session
                    .lock()
                    .map_err(|error| format!("CTAS write session lock: {error}"))? =
                    Some(session.clone());
                let registration =
                    crate::query_execution::contract::ConnectorWriteExecutionRegistration::try_new(
                        session.clone(),
                        cohort_id,
                    )
                    .map_err(|error| error.to_string())?;
                let request = request
                    .into_request(&execution, registration)
                    .map_err(|error| error.to_string())?;
                let outcome = match prepared.state.query_execution().execute(request) {
                    Ok(outcome) => outcome,
                    Err(error) => {
                        return Ok(standard_write_commit_unknown(
                            prepared,
                            &session,
                            format!("CTAS writer dispatch outcome is unknown: {error}"),
                        ));
                    }
                };
                let write = match outcome.into_write() {
                    Ok(write) => write,
                    Err(error) => {
                        return Ok(standard_write_commit_unknown(
                            prepared,
                            &session,
                            format!("CTAS writer terminal outcome is unknown: {error}"),
                        ));
                    }
                };
                let (_, _, _, completion) = write.into_parts_with_connector();
                let Some(completion) = completion else {
                    return Ok(standard_write_commit_unknown(
                        prepared,
                        &session,
                        "CTAS writer returned no complete generic completion",
                    ));
                };
                let sealed = match completion.sealed_operation_completion() {
                    Ok(sealed) => sealed,
                    Err(error) => {
                        return Ok(standard_write_commit_unknown(
                            prepared,
                            &session,
                            format!("CTAS writer aggregate is incomplete: {error}"),
                        ));
                    }
                };
                if let Err(error) = prepared.target.bind_write(sealed.clone()) {
                    return Ok(standard_write_commit_unknown(
                        prepared,
                        &session,
                        format!("CTAS target write binding outcome is unknown: {error}"),
                    ));
                }
                *prepared
                    .completion
                    .lock()
                    .map_err(|error| format!("CTAS completion lock: {error}"))? = Some(completion);
                Ok(StandardCtasWriteOutcome::Completed {
                    completion: sealed,
                    execution_identity: prepared.execution_identity,
                })
            });
        match result {
            Ok(outcome) => outcome,
            Err(message) => StandardCtasWriteOutcome::KnownUncommitted {
                failure: internal_failure(message),
            },
        }
    }

    fn prepare_standard_publish_ctas(
        &self,
        target: &dyn CtasPreparedTarget,
        publication_id: LakePublicationId,
        completion: ConnectorWriteOperationCompletion,
    ) -> Result<PreparedStandardCtasCatalogAction, CtasFailure> {
        let target = downcast_standard_target(target)?;
        let request = target.prepare_publish(publication_id, completion)?;
        Ok(PreparedStandardCtasCatalogAction {
            input_digest: standard_publish_input_digest(&request),
            handle: Arc::new(CoreCtasCatalogAction {
                kind: CoreCtasCatalogActionKind::StandardPublish {
                    target: Arc::new(CoreStandardCtasTargetSession {
                        lease: target.lease.clone(),
                        write_lease: target.write_lease.clone(),
                        handle: target.handle.clone(),
                        publication_id: target.publication_id,
                        context: target.context.clone(),
                        write_plan_started: AtomicBool::new(true),
                        write_unknown_latched: AtomicBool::new(
                            target.write_unknown_latched.load(Ordering::Acquire),
                        ),
                        publish_started: AtomicBool::new(true),
                    }),
                    request,
                },
                dispatched: AtomicBool::new(false),
            }),
        })
    }

    fn publish_standard_ctas(
        &self,
        action: &dyn CtasPreparedCatalogAction,
    ) -> Result<StandardCtasPublishOutcome, CtasFailure> {
        let action = downcast_catalog_action(action)?;
        let CoreCtasCatalogActionKind::StandardPublish { target, request } = &action.kind else {
            return Err(internal_failure(
                "CTAS catalog action is not a standard publish action",
            ));
        };
        action.begin_dispatch()?;
        match target
            .lease
            .publish(request.clone())
            .map_err(connector_failure)?
        {
            ConnectorStagedCreatePublishOutcome::Applied {
                receipt,
                finalization,
            } => Ok(StandardCtasPublishOutcome::Applied {
                receipt,
                finalization,
            }),
            ConnectorStagedCreatePublishOutcome::NoOp {
                receipt,
                finalization,
            } => Ok(StandardCtasPublishOutcome::NoOp {
                receipt,
                finalization,
            }),
            ConnectorStagedCreatePublishOutcome::Conflict { failure }
            | ConnectorStagedCreatePublishOutcome::KnownUncommitted { failure } => {
                Ok(StandardCtasPublishOutcome::KnownUncommitted {
                    failure: mutation_failure(failure),
                })
            }
            ConnectorStagedCreatePublishOutcome::CommitUnknown { failure, evidence } => {
                Ok(StandardCtasPublishOutcome::CommitUnknown {
                    failure: mutation_failure(failure),
                    evidence,
                })
            }
        }
    }

    fn adjudicate_standard_ctas_publication(
        &self,
        target: &dyn CtasPreparedTarget,
        evidence: ExternalMutationEvidence,
    ) -> Result<StandardCtasPublicationAdjudicationOutcome, CtasFailure> {
        let target = downcast_standard_target(target)?;
        let outcome = target
            .lease
            .adjudicate_publication(ConnectorStagedCreatePublicationAdjudicationRequest {
                target_operation_id:
                    novarocks_spi::connector::ConnectorStagedCreateOperationId::from_bytes(
                        target.publication_id.to_bytes(),
                    ),
                evidence,
                context: target.context.clone(),
            })
            .map_err(connector_failure)?;
        match outcome {
            ConnectorStagedCreatePublicationAdjudicationOutcome::Published {
                receipt,
                finalization,
            } => Ok(StandardCtasPublicationAdjudicationOutcome::Published {
                receipt,
                finalization,
            }),
            ConnectorStagedCreatePublicationAdjudicationOutcome::CommitUnknown {
                failure, ..
            } => Ok(StandardCtasPublicationAdjudicationOutcome::CommitUnknown {
                failure: mutation_failure(failure),
            }),
        }
    }
}
