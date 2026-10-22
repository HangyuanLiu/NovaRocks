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
    artifact::PreparedDistributedAttemptTemplate,
    attempt_initialization::RoundCredentialLeaseSource,
    completion::QueryAttemptReservation,
    contract::{DistributedQueryError, DistributedQueryErrorKind, ResolvedQueryOptions},
    lifecycle_plan::{AttemptCredentialStorage, AttemptTerminalCredentialResolver},
};
use novarocks_coordination_contract::RecoveryMode;
use novarocks_query_application::api::{
    InternalComputationConsumer, QueryExecutionError, QueryExecutionFuture,
    ReservedExecutionIdentity,
};
use novarocks_query_application::preparation::{
    ExecutionFinishBoundary, FrozenExecutionDescription,
};
use novarocks_spi::connector::ConnectorRequestContext;
use novarocks_types::QueryExecutionId;
use novarocks_workload_control::WorkOwner;
use std::sync::{Arc, Mutex};

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

/// Original first-attempt ownership established before effectful preparation.
/// Credentials remain FA-owned; the QA token is moved unchanged into start.
pub(crate) struct InitialComputationReservation {
    reserved: Option<ReservedExecutionIdentity>,
    credentials: Arc<InitialComputationCredentials>,
}

impl InitialComputationReservation {
    pub(crate) fn new(reserved: ReservedExecutionIdentity) -> Result<Self, DistributedQueryError> {
        let reservation = QueryAttemptReservation::from_reserved(&reserved)?;
        let terminal = reservation.terminal_storage_resolver();
        Ok(Self {
            credentials: Arc::new(InitialComputationCredentials {
                execution: reserved.execution(),
                reservation: Mutex::new(Some(reservation)),
                terminal,
            }),
            reserved: Some(reserved),
        })
    }

    pub(crate) fn credentials(&self) -> Arc<InitialComputationCredentials> {
        Arc::clone(&self.credentials)
    }

    fn into_start_parts(
        mut self,
    ) -> (
        ReservedExecutionIdentity,
        Arc<InitialComputationCredentials>,
    ) {
        let reserved = self.reserved.take().expect("one initial QA reservation");
        (reserved, Arc::clone(&self.credentials))
    }

    pub(crate) fn connector_request_context(
        &self,
        context: ConnectorRequestContext,
        #[cfg(novarocks_dev_controls)] nomination: Option<
            &Arc<novarocks_failpoint::target_binding::StatementWorkNomination>,
        >,
    ) -> Result<ConnectorRequestContext, DistributedQueryError> {
        let reservation = self
            .credentials
            .reservation
            .lock()
            .expect("initial credential reservation");
        reservation
            .as_ref()
            .ok_or_else(|| {
                initial_credential_error(
                    "initial computation credentials no longer authorize preparation",
                )
            })?
            .connector_request_context(
                context,
                #[cfg(novarocks_dev_controls)]
                nomination,
            )
            .map_err(initial_credential_error)
    }
}

impl Drop for InitialComputationReservation {
    fn drop(&mut self) {
        if self.reserved.is_some() {
            // An attachment takes the token first and disarms this fallback.
            // The statement's stable owner still retains terminal material.
            let _ = self.credentials.seal_for_terminal();
        }
    }
}

/// One exact reservation plus the same strong terminal resolver. Holding this
/// Arc grants neither a second start nor the right to create another collector.
pub(crate) struct InitialComputationCredentials {
    execution: QueryExecutionId,
    reservation: Mutex<Option<QueryAttemptReservation>>,
    terminal: Arc<AttemptTerminalCredentialResolver>,
}

impl InitialComputationCredentials {
    pub(crate) const fn execution(&self) -> QueryExecutionId {
        self.execution
    }

    pub(crate) fn terminal_storage_resolver(&self) -> Arc<AttemptTerminalCredentialResolver> {
        Arc::clone(&self.terminal)
    }

    pub(crate) fn verify_native_storage(
        &self,
        storage: &Arc<AttemptCredentialStorage>,
    ) -> Result<(), DistributedQueryError> {
        self.terminal.verify_native_storage(self.execution, storage)
    }

    /// The stable statement owner calls this on failure/cancellation, including
    /// a start rejected before Native consumes the original reservation.
    pub(crate) fn seal_for_terminal(&self) -> Result<(), DistributedQueryError> {
        let abandoned = self
            .reservation
            .lock()
            .expect("initial credential reservation")
            .take();
        // Close the collection authority even when initialization already took
        // its reservation. Never drop that reservation under the slot mutex.
        let result = self.terminal.seal_for_terminal().map(|_| ());
        drop(abandoned);
        result
    }

    pub(super) fn take_source(
        &self,
        execution: QueryExecutionId,
    ) -> Result<RoundCredentialLeaseSource, DistributedQueryError> {
        if execution != self.execution {
            return Err(initial_credential_error(
                "initial computation credentials belong to another execution",
            ));
        }
        let reservation = self
            .reservation
            .lock()
            .expect("initial credential reservation")
            .take()
            .ok_or_else(|| {
                initial_credential_error(
                    "initial computation credential reservation was consumed or retired",
                )
            })?;
        Ok(RoundCredentialLeaseSource::Reservation {
            reservation: Some(reservation),
            observed_collected: None,
        })
    }
}

fn initial_credential_error(detail: impl Into<String>) -> DistributedQueryError {
    DistributedQueryError::new(DistributedQueryErrorKind::ContractViolation, detail)
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
    pub(super) initial_credentials: Option<Arc<InitialComputationCredentials>>,
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
            initial_credentials: None,
        })
    }

    pub(crate) fn with_initial_reservation(
        mut self,
        initial: InitialComputationReservation,
    ) -> Result<(Self, ReservedExecutionIdentity), DistributedQueryError> {
        if self.description.recovery() != RecoveryMode::NoRecovery
            || self.initial_credentials.is_some()
        {
            return Err(initial_credential_error(
                "initial computation credentials require one unbound NoRecovery computation",
            ));
        }
        let (reserved, credentials) = initial.into_start_parts();
        self.initial_credentials = Some(credentials);
        Ok((self, reserved))
    }
}

#[cfg(test)]
mod initial_reservation_tests {
    use super::*;
    use novarocks_query_application::{
        api::{LogicalExecutionNativePort, LogicalNativeOpenFuture, LogicalNativeOpenRequest},
        coordination::{
            LogicalExecutionRowsConfig, LogicalExecutionSupervisor,
            LogicalExecutionSupervisorConfig,
        },
    };
    use novarocks_spi::connector::{
        ConnectorStopOwner, MAX_CONNECTOR_HANDLE_PAYLOAD_BYTES, MAX_CONNECTOR_TOTAL_PAYLOAD_BYTES,
    };
    use novarocks_types::identity::{AttemptId, FrontendProcessId, QueryProcessNamespace};
    use std::{
        num::{NonZeroU32, NonZeroUsize},
        time::{Duration, Instant},
    };

    #[derive(Debug)]
    struct NoNativeStart;
    impl LogicalExecutionNativePort for NoNativeStart {
        fn open(&self, _: LogicalNativeOpenRequest) -> LogicalNativeOpenFuture {
            panic!("reserving an identity cannot start Native work")
        }
    }

    fn process() -> (
        LogicalExecutionSupervisor,
        novarocks_query_application::api::QueryExecutionClient,
    ) {
        LogicalExecutionSupervisor::new(
            tokio::runtime::Handle::current(),
            Arc::new(NoNativeStart),
            QueryProcessNamespace::new(0x9b1),
            FrontendProcessId::new_v7(),
            LogicalExecutionSupervisorConfig::new(
                NonZeroUsize::new(8).unwrap(),
                NonZeroUsize::MIN,
                NonZeroUsize::MIN,
                LogicalExecutionRowsConfig::new(
                    NonZeroUsize::MIN,
                    NonZeroU32::MIN,
                    Duration::from_secs(1),
                ),
            ),
        )
    }

    fn context() -> ConnectorRequestContext {
        ConnectorRequestContext::try_new(
            Instant::now() + Duration::from_secs(5),
            ConnectorStopOwner::new().view(),
            MAX_CONNECTOR_HANDLE_PAYLOAD_BYTES,
            MAX_CONNECTOR_TOTAL_PAYLOAD_BYTES,
        )
        .unwrap()
    }

    #[tokio::test]
    async fn original_qa_reservation_transfers_its_same_request_scope_once() {
        let (mut owner, client) = process();
        let original = client.reserve_identity().unwrap();
        let execution = original.execution();
        let initial = InitialComputationReservation::new(original).unwrap();
        let stable = initial.credentials();
        let prepared_context = initial
            .connector_request_context(
                context(),
                #[cfg(novarocks_dev_controls)]
                None,
            )
            .unwrap();
        let marker =
            prepared_context.request_scope_extension_or_insert_with(|| String::from("original"));
        let (original, transferred) = initial.into_start_parts();
        assert_eq!(original.execution(), execution);
        assert!(Arc::ptr_eq(&stable, &transferred));
        let foreign =
            QueryExecutionId::new(execution.query_id(), AttemptId::new(2).unwrap()).unwrap();
        assert!(stable.take_source(foreign).is_err());
        let source = stable.take_source(execution).unwrap();
        assert!(stable.take_source(execution).is_err());
        let native_context = source.connector_request_context(context()).unwrap();
        let observed =
            native_context.request_scope_extension_or_insert_with(|| String::from("foreign"));
        assert!(Arc::ptr_eq(&marker, &observed));
        assert!(Arc::ptr_eq(
            prepared_context.vended_credential_lease_sink().unwrap(),
            native_context.vended_credential_lease_sink().unwrap(),
        ));
        // Dropping the actual unstarted source closes production rights even
        // though the stable responsibility owner still retains the material.
        drop(source);
        let terminal = stable.terminal_storage_resolver();
        let original_table = terminal.seal_for_terminal().unwrap();
        assert!(Arc::ptr_eq(
            &original_table,
            &terminal.seal_for_terminal().unwrap()
        ));
        assert!(stable.verify_native_storage(&original_table).is_err());
        assert!(stable.take_source(execution).is_err());
        drop((original, transferred, stable, terminal, original_table));
        owner
            .shutdown_until(Instant::now() + Duration::from_secs(5))
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn abandoned_original_qa_reservation_retires_before_terminal_custody_drops() {
        let (mut owner, client) = process();
        let initial =
            InitialComputationReservation::new(client.reserve_identity().unwrap()).unwrap();
        let stable = initial.credentials();
        let execution = stable.execution();
        drop(initial);
        assert!(stable.take_source(execution).is_err());
        let terminal = stable.terminal_storage_resolver();
        let original_table = terminal.seal_for_terminal().unwrap();
        stable.seal_for_terminal().unwrap();
        assert!(Arc::ptr_eq(
            &original_table,
            &terminal.seal_for_terminal().unwrap()
        ));
        assert!(stable.verify_native_storage(&original_table).is_err());
        drop((stable, terminal, original_table));
        owner
            .shutdown_until(Instant::now() + Duration::from_secs(5))
            .await
            .unwrap();
    }
}
