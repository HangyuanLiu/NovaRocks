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
        let template = encoded.into_attempt_template(version);
        let description =
            novarocks_query_application::preparation::FrozenExecutionDescription::for_completed_plan(
                novarocks_query_application::api::QueryExecutionKind::Write,
                version,
                template.attempt_scheduling_facts()?.fragments.iter()
                    .flat_map(|fragment| fragment.scans.iter().map(|scan| scan.scan)).collect(),
                novarocks_query_application::preparation::OutputContract::CompletionOnly,
                novarocks_query_application::coordination::ExecutionEffect::External,
                novarocks_query_application::coordination::RecoveryMode::NoRecovery,
                Vec::new(),
                novarocks_query_application::preparation::FrozenCostEstimate::unknown(
                    novarocks_query_application::preparation::FrozenEstimateUnknownReason::NotProjected,
                ),
                novarocks_query_application::preparation::ExecutionResourceRequirements::unknown(
                    novarocks_query_application::preparation::FrozenEstimateUnknownReason::NotProjected,
                ),
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

    pub fn finish(self) -> PreparedMvSessionWrite {
        PreparedMvSessionWrite {
            description: self.description,
            template: self.template,
            query_options: self.query_options,
            session: self.session,
        }
    }
}

/// A session-driven MV write, one step away from dispatch.
///
/// The session rides along as the request's single commit authority, so no
/// operation, cohort, or attempt identity reaches the writer data plane.
pub struct PreparedMvSessionWrite {
    description: novarocks_query_application::preparation::FrozenExecutionDescription,
    template: crate::query_execution::artifact::PreparedDistributedAttemptTemplate,
    query_options: Option<QueryOptions>,
    session: std::sync::Arc<crate::query_execution::write_session::ConnectorWriteSession>,
}

impl PreparedMvSessionWrite {
    pub(crate) fn into_request(
        self,
        execution: &QueryExecutionContext,
    ) -> Result<crate::query_execution::contract::DistributedQueryRequest, String> {
        let request = crate::query_execution::contract::build_request_from_finalized_execution(
            crate::query_execution::post_compile::FinalizedDistributedExecution::for_completed_plan(
                self.description,
                self.template,
            ),
            self.query_options,
            crate::query_execution::contract::DistributedQueryIntent::Write,
            execution,
            None,
        )
        .map_err(|error| error.to_string())?;
        crate::query_execution::contract::with_connector_write_session(request, self.session)
            .map_err(|error| error.to_string())
    }
}

/// Complete one MV write after its SQL producer has stated every provider read.
/// The sink contracts supply field names; the session supplies the exact
/// handles it sealed for the writer nodes.
#[allow(clippy::too_many_arguments)]
pub(crate) fn prepare_completed_mv_write(
    kernel: &QueryPreparationKernel,
    execution: &QueryExecutionContext,
    bindings: &QueryTableBindingStore,
    connector_context: &ConnectorRequestContext,
    write_session: std::sync::Arc<crate::query_execution::write_session::ConnectorWriteSession>,
    sealed: novarocks_plan_codec::SealedWriteTargets,
    needs: Box<[novarocks_sql::compiler::ProviderReadNeed]>,
    field_names: std::collections::BTreeMap<
        novarocks_spi::connector::write_stack::WriteTargetOrdinal,
        std::collections::BTreeMap<[u8; 32], Box<str>>,
    >,
    finish: impl FnOnce(
        novarocks_physical_plan::PlanVersionId,
        novarocks_physical_plan::PipelineDopDomain,
        novarocks_sql::planning::dml::DmlFinalizedProviderReadSet,
        novarocks_sql::planning::dml::DmlFinalizedWriteTargetSet,
    ) -> Result<novarocks_physical_plan::PhysicalPlan, String>,
) -> Result<PreparedMvNativeWriteAssembly, String> {
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
    let live = u32::try_from(execution.topology().targets().len()).unwrap_or(u32::MAX);
    let dop_domain = novarocks_physical_plan::PipelineDopDomain {
        min: 1,
        max: live.max(1),
        requires_power_of_two: false,
    };
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
        novarocks_query_application::preparation::CompletedPhysicalPlanCandidate::for_program(plan)
            .map_err(|error| error.to_string())?;
    let paired = novarocks_query_application::preparation::CompletedPlanWithAccess::try_pair(
        candidate, access,
    )
    .map_err(|(error, _returned)| error.to_string())?;
    let encoded = crate::query_execution::physical_encoding::encode_completed_plan(
        paired,
        kernel.function_catalog().as_ref(),
        Some(
            &crate::query_execution::physical_encoding::WriteTargetFacts {
                sealed: &sealed,
                field_names,
            },
        ),
    )?;
    PreparedMvNativeWriteAssembly::session(encoded, version, None, write_session)
}

/// Provider activation and native fragment preparation for a SQL-shaped
/// refresh artifact. The frontend owns intent persistence, write-session
/// admission, native assembly, execution, commit, publication, and cleanup;
/// the port returns only an exact sealed encoding carrier after the lease is
/// retained.
pub trait MvRefreshProviderActivation: Send + Sync {
    fn activate_write(
        &self,
        prepared: PreparedMvRefreshWrite,
        planning_lease: &ConnectorControlPlanningLease,
        exact_lease: &ConnectorWriteLease,
        execution: &QueryExecutionContext,
        connector_context: ConnectorRequestContext,
    ) -> Result<PreparedMvNativeWriteAssembly, String>;

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
    /// supplies the committed snapshot it already proved and the provider's own
    /// storage row count, which must belong to that exact output.
    fn install_published_projection(
        &self,
        planning_lease: &ConnectorControlPlanningLease,
        table: &ConnectorTableIdentity,
        expected_snapshot_id: i64,
        storage_rows: u64,
        operation_id: uuid::Uuid,
        connector_context: &ConnectorRequestContext,
    ) -> Result<(), String>;
}
