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

//! Frontend-owned native execution adapter for current-process ANALYZE attempts.
//!
//! Core owns the provider-neutral statistics program and its one-shot
//! prepare/finish boundary.  This adapter owns the only native mapping step:
//! it encodes the sealed prepared view before returning the resulting request
//! to the carrier-neutral query-execution service.

use std::any::Any;
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::common::backend_topology::BackendTopologyService;
use crate::query_execution::service::QueryExecutionService;
use crate::statistics_jobs::application::{
    StatisticsApplicationError, StatisticsAttemptExecutor, StatisticsAttemptRequest,
    StatisticsCollectedAttempt, StatisticsColumnIntent, rebind_table_object,
};
use novarocks_spi::connector::{
    ConnectorControlRegistry, ConnectorMutationOperationId, ConnectorRequestContext,
    ConnectorStatisticsLease, ConnectorTableHandle, ExternalMutationEvidence,
    ExternalMutationFinalization, ExternalMutationOutcome, MAX_CONNECTOR_HANDLE_PAYLOAD_BYTES,
    MAX_CONNECTOR_STATISTICS_METRICS, MAX_CONNECTOR_TOTAL_PAYLOAD_BYTES,
    StatisticsCollectionRequest, StatisticsMetric, StatisticsMetricRequest,
    StatisticsPublishPreparationRequest, StatisticsPublishRequest,
};

/// Exact Frontend composition leaves retained by the process-owned ANALYZE worker.
/// Each collection takes a fresh live topology snapshot. The worker persists
/// only a logical target and physical object ID; a current binding, its data
/// version, and schema columns are attempt-local facts.
#[derive(Clone)]
pub(crate) struct StatisticsAttemptExecutionPorts {
    execution_role: novarocks_types::ClusterRole,
    connector_control: Arc<dyn ConnectorControlRegistry>,
    backend_topology: BackendTopologyService,
    query_execution: QueryExecutionService,
    attempt_timeout: Duration,
}

impl StatisticsAttemptExecutionPorts {
    pub(crate) fn new(
        execution_role: novarocks_types::ClusterRole,
        connector_control: Arc<dyn ConnectorControlRegistry>,
        backend_topology: BackendTopologyService,
        query_execution: QueryExecutionService,
        attempt_timeout: Duration,
    ) -> Self {
        Self {
            execution_role,
            connector_control,
            backend_topology,
            query_execution,
            attempt_timeout,
        }
    }
}

/// Implements Core's process-worker port while retaining the native encoder
/// exclusively in Frontend.
pub(crate) struct FrontendStatisticsAttemptExecutor {
    ports: StatisticsAttemptExecutionPorts,
}

impl FrontendStatisticsAttemptExecutor {
    pub(crate) fn new(ports: StatisticsAttemptExecutionPorts) -> Self {
        Self { ports }
    }

    fn collection_context(&self) -> Result<ConnectorRequestContext, StatisticsApplicationError> {
        let deadline = Instant::now()
            .checked_add(self.ports.attempt_timeout)
            .ok_or_else(|| {
                StatisticsApplicationError::new("statistics attempt deadline overflow")
            })?;
        ConnectorRequestContext::try_new(
            deadline,
            Arc::new(NeverCancelled),
            MAX_CONNECTOR_HANDLE_PAYLOAD_BYTES,
            MAX_CONNECTOR_TOTAL_PAYLOAD_BYTES,
        )
        .map_err(|error| StatisticsApplicationError::new(error.to_string()))
    }

    fn resolve_columns(
        request: &StatisticsAttemptRequest,
        bound_columns: &[String],
    ) -> Result<Vec<String>, StatisticsApplicationError> {
        match &request.columns {
            StatisticsColumnIntent::AllColumns => Ok(bound_columns.to_vec()),
            StatisticsColumnIntent::Explicit(requested_columns) => {
                let mut resolved = Vec::with_capacity(requested_columns.len());
                for requested in requested_columns {
                    let mut matches = bound_columns
                        .iter()
                        .filter(|bound| bound.eq_ignore_ascii_case(requested));
                    let Some(column) = matches.next() else {
                        return Err(StatisticsApplicationError::new(format!(
                            "ANALYZE requested column '{requested}' does not exist on the rebound table"
                        )));
                    };
                    if matches.next().is_some() {
                        return Err(StatisticsApplicationError::new(format!(
                            "ANALYZE requested column '{requested}' matches multiple rebound table columns"
                        )));
                    }
                    if resolved
                        .iter()
                        .any(|existing: &String| existing.eq_ignore_ascii_case(column))
                    {
                        return Err(StatisticsApplicationError::new(format!(
                            "ANALYZE requested column '{requested}' is duplicated"
                        )));
                    }
                    resolved.push(column.clone());
                }
                Ok(resolved)
            }
        }
    }

    fn metrics(columns: &[String]) -> Result<StatisticsMetricRequest, StatisticsApplicationError> {
        let requested_metric_count = columns
            .len()
            .checked_mul(5)
            .and_then(|count| count.checked_add(1))
            .ok_or_else(|| {
                StatisticsApplicationError::new(
                    "ANALYZE requested too many connector statistics metrics",
                )
            })?;
        if requested_metric_count > MAX_CONNECTOR_STATISTICS_METRICS {
            return Err(StatisticsApplicationError::new(format!(
                "ANALYZE requires {requested_metric_count} metrics, exceeding the connector statistics limit of {MAX_CONNECTOR_STATISTICS_METRICS}",
            )));
        }
        let mut metrics = vec![StatisticsMetric::RowCount];
        for column in columns {
            let column: Arc<str> = Arc::from(column.as_str());
            metrics.extend([
                StatisticsMetric::NullCount {
                    column: Arc::clone(&column),
                },
                StatisticsMetric::Minimum {
                    column: Arc::clone(&column),
                },
                StatisticsMetric::Maximum {
                    column: Arc::clone(&column),
                },
                StatisticsMetric::AverageSize {
                    column: Arc::clone(&column),
                },
                StatisticsMetric::ThetaNdv { column },
            ]);
        }
        StatisticsMetricRequest::try_new(metrics)
            .map_err(|error| StatisticsApplicationError::new(error.to_string()))
    }

    fn operation_id(request: &StatisticsAttemptRequest) -> ConnectorMutationOperationId {
        ConnectorMutationOperationId::from_bytes(request.operation_id.to_bytes())
    }

    fn collected(
        collected: &dyn StatisticsCollectedAttempt,
    ) -> Result<&FrontendStatisticsCollectedAttempt, StatisticsApplicationError> {
        collected
            .as_any()
            .downcast_ref::<FrontendStatisticsCollectedAttempt>()
            .ok_or_else(|| {
                StatisticsApplicationError::new(
                    "statistics publication received a collection artifact from another executor",
                )
            })
    }

    fn outcome(
        outcome: ExternalMutationOutcome<novarocks_spi::connector::StatisticsReceipt>,
    ) -> Result<(), StatisticsApplicationError> {
        match outcome {
            ExternalMutationOutcome::KnownCommitted {
                finalization: ExternalMutationFinalization::Complete,
                ..
            } => Ok(()),
            ExternalMutationOutcome::KnownCommitted {
                finalization: ExternalMutationFinalization::Failed(failure),
                ..
            } => Err(StatisticsApplicationError::publication(
                crate::statistics_jobs::application::StatisticsPublicationTerminal::KnownCommittedFinalization,
                failure.to_string(),
            )),
            ExternalMutationOutcome::KnownUncommitted { failure } => Err(
                StatisticsApplicationError::publication(
                    crate::statistics_jobs::application::StatisticsPublicationTerminal::KnownUncommitted,
                    failure.to_string(),
                ),
            ),
            ExternalMutationOutcome::CommitUnknown { failure, .. } => {
                Err(StatisticsApplicationError::publication(
                    crate::statistics_jobs::application::StatisticsPublicationTerminal::CommitUnknown,
                    failure.to_string(),
                ))
            }
        }
    }
}

struct FrontendStatisticsCollectedAttempt {
    lease: ConnectorStatisticsLease,
    table: ConnectorTableHandle,
    data_version: novarocks_spi::connector::StatisticsDataVersion,
    result: novarocks_spi::connector::StatisticsCollectionResult,
    context: ConnectorRequestContext,
}

impl StatisticsCollectedAttempt for FrontendStatisticsCollectedAttempt {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn basis_data_version(&self) -> &novarocks_spi::connector::StatisticsDataVersion {
        &self.data_version
    }
}

impl StatisticsAttemptExecutor for FrontendStatisticsAttemptExecutor {
    fn collect(
        &self,
        request: &StatisticsAttemptRequest,
    ) -> Result<Box<dyn StatisticsCollectedAttempt>, StatisticsApplicationError> {
        let context = self.collection_context()?;
        let instance_id =
            novarocks_spi::connector::ConnectorInstanceId::parse(&request.connector_instance_id)
                .map_err(|error| StatisticsApplicationError::new(error.to_string()))?;
        let planning_lease = self
            .ports
            .connector_control
            .acquire_current(&instance_id)
            .map_err(|error| StatisticsApplicationError::new(error.to_string()))?;
        // Rebinding and collection preparation intentionally share one current
        // connector generation. Do not translate this error: its typed physical
        // object binding classification must reach the process worker unchanged.
        let binding = rebind_table_object(&planning_lease, context.clone(), request)?;
        let columns = Self::resolve_columns(request, &binding.sql_columns)?;
        let metrics = Self::metrics(&columns)?;
        let lease = planning_lease
            .derive_statistics_lease()
            .map_err(|error| StatisticsApplicationError::new(error.to_string()))?;
        let data_version = binding.data_version.clone();
        let plan = lease
            .prepare_collection(StatisticsCollectionRequest {
                operation_id: Self::operation_id(request),
                table: binding.table.clone(),
                data_version,
                metrics,
                context: context.clone(),
            })
            .map_err(|error| StatisticsApplicationError::new(error.to_string()))?;
        let program = crate::query_execution::statistics::StatisticsCollectionProgram::try_new(
            plan,
            crate::query_execution::statistics::StatisticsExecutionPolicy::try_new(
                crate::query_execution::statistics::StatisticsExecutionMode::ProcessJobAttempt,
                self.ports.attempt_timeout,
            )
            .map_err(|error| StatisticsApplicationError::new(error.to_string()))?,
        )
        .map_err(|error| StatisticsApplicationError::new(error.to_string()))?;
        let topology = self
            .ports
            .backend_topology
            .snapshot()
            .map_err(|error| StatisticsApplicationError::new(error.to_string()))?;
        let cancellation = crate::common::query_cancellation::QueryCancellationSource::new();
        let execution = crate::common::admitted_query_context::QueryExecutionContext::new(
            self.ports.execution_role,
            topology,
            Some(
                Instant::now()
                    .checked_add(program.policy().attempt_timeout())
                    .ok_or_else(|| {
                        StatisticsApplicationError::new("statistics attempt deadline overflow")
                    })?,
            ),
            cancellation.view(),
            novarocks_sql::compiler::SessionOptimizerSettings::default(),
        );

        // The sequence is intentional: Core prepares immutable provider facts;
        // Frontend maps the sealed view; Core consumes the exact attachment.
        let prepared = crate::query_execution::statistics::prepare_statistics_collection_request(
            self.ports.connector_control.as_ref(),
            &execution,
            context.clone(),
            program,
            planning_lease,
        )
        .map_err(|error| StatisticsApplicationError::new(error.to_string()))?;
        let native_attachment = crate::native::fragment_encoder::encode_native_fragment_bundle(
            prepared.encoding_view(),
        )
        .map_err(StatisticsApplicationError::new)?;
        let distributed = prepared
            .finish(native_attachment)
            .map_err(|error| StatisticsApplicationError::new(error.to_string()))?;
        let result = self
            .ports
            .query_execution
            .execute(distributed)
            .and_then(crate::query_execution::contract::DistributedQueryOutcome::into_statistics)
            .map(|outcome| outcome.into_collection_result())
            .map_err(|error| StatisticsApplicationError::new(error.to_string()))?;
        Ok(Box::new(FrontendStatisticsCollectedAttempt {
            lease,
            table: binding.table,
            data_version: binding.data_version,
            result,
            context,
        }))
    }

    fn prepare_publish(
        &self,
        request: &StatisticsAttemptRequest,
        collected: &dyn StatisticsCollectedAttempt,
    ) -> Result<ExternalMutationEvidence, StatisticsApplicationError> {
        let collected = Self::collected(collected)?;
        collected
            .lease
            .prepare_publish(StatisticsPublishPreparationRequest {
                operation_id: Self::operation_id(request),
                table: collected.table.clone(),
                result: collected.result.clone(),
                context: collected.context.clone(),
            })
            .map_err(|error| StatisticsApplicationError::new(error.to_string()))
    }

    fn publish(
        &self,
        request: &StatisticsAttemptRequest,
        collected: &dyn StatisticsCollectedAttempt,
        evidence: &ExternalMutationEvidence,
    ) -> Result<(), StatisticsApplicationError> {
        let collected = Self::collected(collected)?;
        Self::outcome(
            collected
                .lease
                .publish(StatisticsPublishRequest {
                    operation_id: Self::operation_id(request),
                    table: collected.table.clone(),
                    result: collected.result.clone(),
                    context: collected.context.clone(),
                    evidence: evidence.clone(),
                })
                .map_err(|error| StatisticsApplicationError::new(error.to_string()))?,
        )
    }
}

struct NeverCancelled;

impl novarocks_spi::connector::ConnectorCancellation for NeverCancelled {
    fn is_cancelled(&self) -> bool {
        false
    }
}
