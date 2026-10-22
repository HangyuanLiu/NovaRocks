// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

//! Fixed Frontend launch boundary for client and internal logical execution.

use super::internal_consumer::attempt_handoff::AttemptHandoffOfferPort;
use crate::query_execution::{
    artifact::PreparedDistributedAttemptTemplate, contract::ResolvedQueryOptions,
};
use novarocks_query_application::api::{
    InternalComputationConsumer, QueryExecutionError, QueryExecutionFuture,
    ReservedExecutionIdentity,
};
use novarocks_query_application::preparation::{
    ExecutionFinishBoundary, FrozenExecutionDescription,
};
use novarocks_workload_control::WorkOwner;
use std::sync::Arc;

use crate::query_execution::completion::PreparedLogicalRead;

/// Production composition injects one implementation backed by the process
/// Query Application runtime. The SQL session transfers the complete prepared
/// carrier and the unique governed owner without learning Native adapter parts.
pub(crate) trait LogicalExecutionLauncher: Send + Sync + 'static {
    fn reserve_identity(&self) -> Result<ReservedExecutionIdentity, QueryExecutionError>;

    fn start_computation(
        &self,
        computation: PreparedLogicalComputation,
        owner: WorkOwner,
        result_window: novarocks_workload_control::ResultWindowAlias,
        reservation: Option<ReservedExecutionIdentity>,
        #[cfg(novarocks_dev_controls)] observation: Option<
            crate::preparation_diagnostics::StatementObservationHandle,
        >,
    ) -> QueryExecutionFuture;

    fn start(
        &self,
        read: PreparedLogicalRead,
        owner: WorkOwner,
        result_window: Option<novarocks_workload_control::ResultWindowAlias>,
        #[cfg(novarocks_dev_controls)] observation: Option<
            crate::preparation_diagnostics::StatementObservationHandle,
        >,
    ) -> QueryExecutionFuture;
}

/// The diagnostic contract comes from the closed FA consumer constructor,
/// never from Native's interpretation of a wire kind or public output mode.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ComputationDiagnostics {
    None,
    Profile,
}

/// One frozen computation and its registered application's exact handoff pair.
/// Domain payload/proof construction stays in the fixed FA consumer.
pub(crate) struct PreparedLogicalComputation {
    pub(super) description: FrozenExecutionDescription,
    pub(super) template: PreparedDistributedAttemptTemplate,
    pub(super) options: Arc<ResolvedQueryOptions>,
    pub(super) consumer: Arc<dyn InternalComputationConsumer>,
    pub(super) handoffs: Arc<AttemptHandoffOfferPort>,
    pub(super) diagnostics: ComputationDiagnostics,
}
impl PreparedLogicalComputation {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        description: FrozenExecutionDescription,
        template: PreparedDistributedAttemptTemplate,
        options: Arc<ResolvedQueryOptions>,
        consumer: Arc<dyn InternalComputationConsumer>,
        handoffs: AttemptHandoffOfferPort,
        diagnostics: ComputationDiagnostics,
    ) -> Result<Self, String> {
        if description.finish_boundary() != ExecutionFinishBoundary::ComputationReady
            || !description.matches_plan_seal(template.native_manifest_template().plan())
        {
            return Err(
                "internal computation does not match its frozen finish boundary or Native plan"
                    .into(),
            );
        }
        Ok(Self {
            description,
            template,
            options,
            consumer,
            handoffs: Arc::new(handoffs),
            diagnostics,
        })
    }
}
