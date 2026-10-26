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

//! Native write assembly for the Frontend-owned MV refresh lifecycle.
//!
//! The MV application module owns refresh domain facts; this module owns the
//! assembly vocabulary those facts are dispatched through.  Keeping the two
//! apart lets the MV application port stay with the MV domain while the
//! sealed encoding carrier and its provider activation port travel with the
//! rest of query assembly.

use novarocks_proto_codec::lifecycle::QueryOptions;
use novarocks_spi::connector::{
    ConnectorControlPlanningLease, ConnectorRequestContext, ConnectorTableIdentity,
    ConnectorWriteLease, ConnectorWriteReceipt,
};

use crate::catalog_application::query_bindings::QueryTableBindingStore;
use crate::query_execution::kernels::QueryPreparationKernel;
use crate::query_execution::mv_assembly::refresh_handoff::PreparedMvRefreshWrite;
use novarocks_mv_application::publication::{MvRefreshCommittedFacts, MvRefreshPublicationIntent};
use novarocks_query_application::admitted_query_context::QueryExecutionContext;

/// Exact completed-plan inputs for one Frontend-owned MV native assembly.
///
/// The plan and its frozen read access are paired before encoding. Finishing
/// consumes their attempt template, so another binding cannot reach dispatch.
///
/// Every MV data write -- first refresh and incremental alike -- commits through
/// the write session that admitted it. The session sealed the recipes this
/// plan's writer nodes carry, so the two travel together and no operation,
/// cohort, or attempt identity reaches the writer data plane.
pub struct PreparedMvNativeWriteAssembly {
    description: novarocks_query_application::preparation::FrozenExecutionDescription,
    template: crate::query_execution::artifact::PreparedDistributedAttemptTemplate,
    query_options: Option<QueryOptions>,
    session: std::sync::Arc<crate::query_execution::write_session::ConnectorWriteSession>,
}

impl PreparedMvNativeWriteAssembly {
    pub(crate) fn session(
        encoded: crate::query_execution::physical_encoding::EncodedCompletedPlan,
        version: novarocks_physical_plan::PlanVersionId,
        query_options: Option<QueryOptions>,
        write_session: std::sync::Arc<crate::query_execution::write_session::ConnectorWriteSession>,
    ) -> Result<Self, String> {
        let (template, candidate) = encoded.into_attempt_template_with_candidate(version);
        let description = mv_write_description(
            candidate,
            template
                .attempt_scheduling_facts()?
                .fragments
                .iter()
                .flat_map(|fragment| fragment.scans.iter().map(|scan| scan.scan))
                .collect(),
        )?;
        Ok(Self {
            description,
            template,
            query_options,
            session: write_session,
        })
    }

    /// The commit authority of this write, so a caller that fails between
    /// assembly and dispatch can release it rather than leaving the provider
    /// holding a session for a plan that will never run.
    pub(crate) fn write_session(
        &self,
    ) -> &std::sync::Arc<crate::query_execution::write_session::ConnectorWriteSession> {
        &self.session
    }
}

/// MV uses the same complete-write consumer as the other write intents. The
/// original session and reservation are consumed once, without constructing a
/// synchronous execution request.
impl crate::query::statement_computation::StatementComputationAssembly
    for PreparedMvNativeWriteAssembly
{
    type Output = crate::query_execution::internal_consumer::write::WriteComputationOutput;

    fn prepare(
        self,
        initial: crate::query_execution::logical_execution::InitialComputationReservation,
        runtime: crate::query_execution::internal_consumer::decoded::DecodedConsumerRuntime,
    ) -> Result<
        (
            crate::query_execution::logical_execution::PreparedLogicalComputation,
            novarocks_query_application::api::ReservedExecutionIdentity,
            Self::Output,
        ),
        novarocks_query_application::api::QueryExecutionError,
    > {
        crate::query_execution::internal_consumer::write::prepare_write_computation(
            self.description,
            self.template,
            std::sync::Arc::new(
                crate::query_execution::contract::ResolvedQueryOptions::from_upstream(
                    self.query_options,
                ),
            ),
            self.session,
            initial,
            runtime,
        )
    }
}

/// Validate every static-plan check while the actual read capability set is
/// still held by the original activation Source. The final bind only moves it.
pub(crate) struct PreparedMvNativeWriteParts {
    encoded: crate::query_execution::physical_encoding::PreparedStaticPlan,
    description: novarocks_query_application::preparation::FrozenExecutionDescription,
    version: novarocks_physical_plan::PlanVersionId,
    query_options: Option<QueryOptions>,
}
impl PreparedMvNativeWriteParts {
    pub(crate) fn try_new(
        encoded: crate::query_execution::physical_encoding::PreparedStaticPlan,
        version: novarocks_physical_plan::PlanVersionId,
        query_options: Option<QueryOptions>,
    ) -> Result<Self, String> {
        let scheduling = encoded.scheduling_facts(version)?;
        let description = mv_write_description(
            encoded.candidate().clone(),
            scheduling
                .fragments
                .iter()
                .flat_map(|fragment| fragment.scans.iter().map(|scan| scan.scan))
                .collect(),
        )?;
        Ok(Self {
            encoded,
            description,
            version,
            query_options,
        })
    }
    pub(crate) fn bind_access(
        self,
        access: novarocks_query_application::preparation::FinalPlanRuntimeAccess<
            crate::query_execution::provider_read_facts::FrozenProviderRead,
        >,
        session: std::sync::Arc<crate::query_execution::write_session::ConnectorWriteSession>,
    ) -> PreparedMvNativeWriteAssembly {
        let template = self
            .encoded
            .bind_access(access)
            .into_attempt_template(self.version);
        PreparedMvNativeWriteAssembly {
            description: self.description,
            template,
            query_options: self.query_options,
            session,
        }
    }
}
fn mv_write_description(
    candidate: novarocks_query_application::preparation::CompletedPhysicalPlanCandidate,
    scan_identities: Vec<novarocks_query_application::api::PlanScanIdentity>,
) -> Result<novarocks_query_application::preparation::FrozenExecutionDescription, String> {
    novarocks_query_application::preparation::FrozenExecutionDescription::for_completed_plan(
        novarocks_query_application::api::QueryExecutionKind::Write,
        candidate,
        scan_identities,
        novarocks_query_application::preparation::OutputContract::CompletionOnly,
        novarocks_coordination_contract::ExecutionEffect::External,
        novarocks_coordination_contract::RecoveryMode::NoRecovery,
        Vec::new(),
        novarocks_query_application::preparation::FrozenCostEstimate::unknown(
            novarocks_query_application::preparation::FrozenEstimateUnknownReason::NotProjected,
        ),
        novarocks_query_application::preparation::ExecutionResourceRequirements::unknown(
            novarocks_query_application::preparation::FrozenEstimateUnknownReason::NotProjected,
        ),
    )
}

/// Complete one MV write after its SQL producer has stated every provider read.
/// The sink contracts supply field names; the session supplies the exact
/// handles it sealed for the writer nodes.
pub(crate) fn prepare_completed_mv_write(
    kernel: &QueryPreparationKernel,
    execution: &QueryExecutionContext,
    bindings: &QueryTableBindingStore,
    connector_context: &ConnectorRequestContext,
    write_session: std::sync::Arc<crate::query_execution::write_session::ConnectorWriteSession>,
    needs: Box<[novarocks_sql::compiler::ProviderReadNeed]>,
    finish: impl FnOnce(
        novarocks_physical_plan::PlanVersionId,
        novarocks_physical_plan::PipelineDopDomain,
        novarocks_sql::planning::dml::DmlFinalizedProviderReadSet,
        novarocks_sql::planning::dml::DmlFinalizedWriteTargetSet,
    ) -> Result<
        novarocks_sql::compiler::SqlAuthoredPhysicalPlan,
        novarocks_sql::compiler::SqlCompileError,
    >,
    control: &novarocks_sql::compiler::SqlCompileControl,
) -> Result<PreparedMvNativeWriteAssembly, novarocks_sql::compiler::SqlCompileError> {
    let session = crate::query_execution::compiler::typed_connector_session()?;
    let access_sink = novarocks_query_application::preparation::ReadAccessSink::new();
    let mut facts = Vec::with_capacity(needs.len());
    for need in &needs {
        facts.push(
            crate::query_execution::provider_read_facts::freeze_one_read(
                need,
                kernel.typed_connector_control().as_ref(),
                bindings,
                &session,
                connector_context,
                &access_sink.deposits(),
            )?,
        );
    }
    let access = access_sink
        .try_into_access()
        .map_err(|(error, _returned)| error.to_string())?;
    let targets = novarocks_sql::planning::dml::DmlFinalizedWriteTargetSet::try_new(
        write_session
            .targets()
            .iter()
            .map(|target| {
                Ok(novarocks_sql::planning::dml::DmlFinalizedWriteTarget {
                    ordinal: target.ordinal(),
                    handle: write_session
                        .encode_writer_handle_payload(target.handle())
                        .map_err(|error| error.to_string())?,
                })
            })
            .collect::<Result<Vec<_>, String>>()?,
    )?;
    let version = crate::query_execution::physical_encoding::mint_plan_version();
    let dop_domain = crate::query_execution::contract::completed_plan_dop_domain(None)?;
    let reads =
        novarocks_sql::planning::dml::DmlFinalizedProviderReadSet::try_new(facts.into_iter().map(
            |fact| novarocks_sql::planning::dml::DmlFinalizedProviderRead {
                fact,
                read_budget: novarocks_physical_plan::ScanReadBudget {
                    max_batch_rows: novarocks_physical_plan::MAX_SCAN_BATCH_ROWS,
                    max_batch_bytes: novarocks_physical_plan::MAX_SCAN_BATCH_BYTES,
                },
            },
        ))?;
    let plan = finish(version, dop_domain, reads, targets)?;
    let candidate =
        novarocks_query_application::preparation::CompletedPhysicalPlanCandidate::for_sql_program(
            plan, control,
        )
        .and_then(|candidate| {
            candidate.freeze_root_output(
                novarocks_result_contract::FrozenRootOutput::InternalFacts(
                    novarocks_result_contract::InternalResultDomain::PreparedWriteCommitV1,
                ),
            )
        })
        .map_err(|error| error.to_string())?;
    let paired = novarocks_query_application::preparation::CompletedPlanWithAccess::try_pair(
        candidate, access,
    )
    .map_err(|(error, _returned)| error.to_string())?;
    let encoded = crate::query_execution::physical_encoding::encode_completed_plan(
        paired,
        kernel.static_plan_carrier(),
        kernel.constant_policy(),
        Some(
            &crate::query_execution::physical_encoding::WriteTargetFacts {
                session: write_session.as_ref(),
            },
        ),
        control,
    )
    .map_err(encode_compile_error)?;
    let options = crate::query_execution::contract::synthetic_statement_query_options(execution);
    PreparedMvNativeWriteAssembly::session(encoded, version, Some(options), write_session)
        .map_err(novarocks_sql::compiler::SqlCompileError::Compilation)
}

pub(crate) fn encode_compile_error(
    error: novarocks_plan_codec::PhysicalEncodeError,
) -> novarocks_sql::compiler::SqlCompileError {
    match error {
        novarocks_plan_codec::PhysicalEncodeError::Control(error) => error.into(),
        other => novarocks_sql::compiler::SqlCompileError::CompilationSource(
            novarocks_failure_contract::SharedFailureCause::new(other),
        ),
    }
}

/// Provider activation and native fragment preparation for a SQL-shaped
/// refresh artifact. The frontend owns intent persistence, write-session
/// admission, native assembly, execution, commit, publication, and cleanup;
/// the port returns only an exact sealed encoding carrier after the lease is
/// retained.
pub trait MvRefreshProviderActivation: Send + Sync {
    /// Install one original write-preparation source from already received
    /// facts. Construction performs no Provider I/O, polling or execution.
    fn prepare_write_source(
        &self,
        request: crate::query_execution::mv_assembly::OriginalMvWritePreparationRequest,
    ) -> crate::query_execution::mv_assembly::MvWritePreparationCustody;

    fn activate_write(
        &self,
        prepared: PreparedMvRefreshWrite,
        planning_lease: &ConnectorControlPlanningLease,
        exact_lease: &ConnectorWriteLease,
        execution: &QueryExecutionContext,
        connector_context: ConnectorRequestContext,
    ) -> Result<PreparedMvNativeWriteAssembly, novarocks_sql::compiler::SqlCompileError>;

    /// Open the session one metadata-only publication commits through.
    ///
    /// It returns a session rather than a write assembly because there is no
    /// plan to assemble: a refresh whose inputs did not move reads nothing, so
    /// no fragment, writer or cohort ever exists. Everything after this point
    /// -- bind P, finish, interpret the receipt -- is the data path's own tail.
    fn activate_metadata_only_publication(
        &self,
        prepared: &crate::query_execution::mv_assembly::refresh_artifact::PreparedMvMetadataOnlyWrite,
        planning_lease: &ConnectorControlPlanningLease,
        exact_lease: &ConnectorWriteLease,
        connector_context: ConnectorRequestContext,
    ) -> Result<std::sync::Arc<crate::query_execution::write_session::ConnectorWriteSession>, String>;

    fn interpret_write_commit(
        &self,
        intent: MvRefreshPublicationIntent,
        receipt: &ConnectorWriteReceipt,
    ) -> Result<MvRefreshCommittedFacts, String>;

    /// Install the Current projection of a target this refresh just published.
    ///
    /// The publication committed P in the same snapshot as its rows, so the
    /// projection is read back from that one committed document set rather
    /// than assembled from the facts the frontend happened to hold. The caller
    /// supplies the committed carrier snapshot it already proved.
    fn install_published_projection(
        &self,
        planning_lease: &ConnectorControlPlanningLease,
        table: &ConnectorTableIdentity,
        expected_snapshot_id: i64,
        operation_id: uuid::Uuid,
        connector_context: &ConnectorRequestContext,
    ) -> Result<(), String>;
}

#[cfg(test)]
mod capability_identity_tests {
    #[test]
    fn mv_encode_capability_refusal_keeps_the_client_identity() {
        use novarocks_failure_contract::{FailureClass, FailureProjection};
        let original = novarocks_plan_codec::PhysicalEncodeError::UnsupportedCapability(
            "change-stream router".into(),
        );
        let expected = original.failure_fact();
        let novarocks_sql::compiler::SqlCompileError::CompilationSource(source) =
            super::encode_compile_error(original)
        else {
            panic!("retain the actual encode author")
        };
        assert_eq!(source.failure_fact(), expected);
        assert_eq!(expected.class(), FailureClass::Unsupported);
        assert!(
            matches!(source.downcast_ref::<novarocks_plan_codec::PhysicalEncodeError>(), Some(novarocks_plan_codec::PhysicalEncodeError::UnsupportedCapability(detail)) if detail == "change-stream router")
        );
    }
}
