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

use novarocks_workload_control::{
    QueryConcurrencyPermit, RootAdmissionHandle, WorkClass, WorkOwner, WorkRequest,
};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use super::background::MvBackgroundBindings;
use crate::mv::domain::application::MvApplicationError;
use crate::mv::domain::readiness::MvReadinessPort;
use crate::query_execution::maintenance::{TableMaintenanceEngine, TableMaintenanceService};
use novarocks_mv_application::{
    activity::{MvActivityAdmissionError, MvActivityLease, MvActivityOwner},
    maintenance::{
        MaintenanceCoordinatorConfig, MvBackgroundEngineError, MvBackgroundEngineErrorKind,
    },
    process_runtime::{MvBackgroundRuntime, MvBackgroundStop, MvBackgroundTasks},
    service::MvKeyedProductService,
};
use novarocks_query_application::admitted_query_context::{RequestAdmission, RequestContext};
use novarocks_query_application::api::BackendTopologyService;
use novarocks_spi::connector::{ConnectorControlRegistry, ConnectorRequestContext};
use novarocks_sql::compiler::SessionOptimizerSettings;

use super::{
    activity::canonical_mv_target,
    refresh,
    scheduler::{self, ScheduledRefreshDisposition, ScheduledRefreshRequest},
};

#[path = "service/background_original.rs"]
mod background_original;

/// Frontend composition and statement adapter for materialized-view products.
///
/// MVX-1 owns only Iceberg CREATE sequencing. Other MV statement classes
/// deliberately return `None` so their existing core routes remain active.
pub struct FrontendMvProductAdapter {
    readiness: Arc<MvReadinessPort>,
    refresh: refresh::FrontendMvRefreshDependencies,
    product_service: Arc<MvKeyedProductService>,
    maintenance_config: MaintenanceCoordinatorConfig,
    table_maintenance_service: Arc<dyn TableMaintenanceService>,
    execution_role: novarocks_types::ClusterRole,
    topology: BackendTopologyService,
    /// Cost budget frozen from `[runtime]`; the MV worker has no session, so it
    /// carries the value that statement admission would otherwise have to guess.
    optimizer_query_mem_limit_bytes: u64,
    attempt_timeout: Duration,
    root_admission: RootAdmissionHandle,
    /// The deployment's statement that a previous incarnation is isolated,
    /// when it configured one. Absent means every barrier a restart left
    /// waits for an operator.
    startup_isolation: Option<crate::mv::startup_isolation_file::StartupIsolationSource>,
}

impl FrontendMvProductAdapter {
    #[expect(
        clippy::too_many_arguments,
        reason = "Frontend MV composition keeps independently owned ports explicit at the application boundary."
    )]
    pub(crate) fn with_refresh_dependencies(
        readiness: Arc<MvReadinessPort>,
        connector_control: Arc<dyn ConnectorControlRegistry>,
        provider_activation: Arc<
            dyn crate::query_execution::mv_native_write::MvRefreshProviderActivation,
        >,
        execution_role: novarocks_types::ClusterRole,
        topology: BackendTopologyService,
        product_service: Arc<MvKeyedProductService>,
        maintenance_config: MaintenanceCoordinatorConfig,
        table_maintenance_service: Arc<dyn TableMaintenanceService>,
        optimizer_query_mem_limit_bytes: u64,
        attempt_timeout: Duration,
        root_admission: RootAdmissionHandle,
        startup_isolation: Option<crate::mv::startup_isolation_file::StartupIsolationSource>,
    ) -> Self {
        Self {
            refresh: refresh::FrontendMvRefreshDependencies {
                connector_control: Arc::clone(&connector_control),
                provider_activation: Arc::clone(&provider_activation),
                #[cfg(novarocks_dev_controls)]
                test_controls: readiness.test_controls_owner(),
            },
            readiness: Arc::clone(&readiness),
            product_service,
            maintenance_config,
            table_maintenance_service,
            execution_role,
            topology,
            optimizer_query_mem_limit_bytes,
            attempt_timeout,
            root_admission,
            startup_isolation,
        }
    }

    pub(crate) fn readiness_port(&self) -> Arc<MvReadinessPort> {
        Arc::clone(&self.readiness)
    }

    pub(crate) fn original_refresh_ports(
        &self,
    ) -> (
        Arc<MvKeyedProductService>,
        Arc<dyn crate::query_execution::mv_native_write::MvRefreshProviderActivation>,
    ) {
        (
            self.product_service.clone(),
            self.refresh.provider_activation.clone(),
        )
    }

    pub(crate) fn product_service(&self) -> &MvKeyedProductService {
        self.product_service.as_ref()
    }

    pub(crate) async fn shutdown_background_workers_until(
        &self,
        deadline: Instant,
    ) -> Result<(), String> {
        self.product_service
            .shutdown_background_workers_until(deadline)
            .await
    }

    pub(crate) fn request_background_stop_for_process_exit(&self) {
        self.product_service
            .request_background_stop_for_process_exit();
    }

    pub(crate) fn start_background_workers(
        &self,
        bindings: MvBackgroundBindings,
    ) -> Result<(), MvBackgroundEngineError> {
        let reservation = self
            .product_service
            .begin_background_start()
            .map_err(lifecycle_error)?;
        let dependencies = self.refresh.clone();
        let topology = self.topology.clone();
        let table_maintenance_service = Arc::clone(&self.table_maintenance_service);
        let runtime = start_background_workers(RefreshWorkerDependencies {
            readiness: Arc::clone(&self.readiness),
            refresh: dependencies,
            ports: bindings.ports,
            execution: bindings.execution,
            topology,
            role: self.execution_role,
            maintenance_config: self.maintenance_config.clone(),
            table_maintenance_engine: bindings.table_maintenance_engine,
            table_maintenance_service,
            product_service: Arc::clone(&self.product_service),
            root_admission: self.root_admission.clone(),
            optimizer_query_mem_limit_bytes: self.optimizer_query_mem_limit_bytes,
            attempt_timeout: self.attempt_timeout,
            runtime: tokio::runtime::Handle::current(),
            startup_isolation: self.startup_isolation.clone(),
        })?;
        reservation.install(runtime).map_err(lifecycle_error)
    }
}

impl FrontendMvProductAdapter {
    /// Run one foreground DDL path under the same per-target FIFO gate as
    /// refresh and background maintenance. The SQL session cancellation scope
    /// remains the authority while the statement waits for its turn.
    pub(crate) fn execute_serialized<T, E: From<String>>(
        &self,
        target: &novarocks_sql::planning::mv::SqlMvTarget,
        owner: MvActivityOwner,
        execution: &novarocks_query_application::admitted_query_context::QueryExecutionContext,
        action: impl FnOnce() -> Result<T, E>,
    ) -> Result<T, E> {
        let _gate_lease = self
            .acquire_activity_lease(target, owner, execution)
            .map_err(|error| E::from(error.to_string()))?;
        action()
    }

    fn acquire_activity_lease(
        &self,
        target: &novarocks_sql::planning::mv::SqlMvTarget,
        owner: MvActivityOwner,
        execution: &novarocks_query_application::admitted_query_context::QueryExecutionContext,
    ) -> Result<MvActivityLease, MvApplicationError> {
        self.product_service
            .acquire_foreground(canonical_mv_target(target), owner, || {
                execution.cancellation().is_cancelled()
            })
            .map_err(|error| match error {
                MvActivityAdmissionError::Cancelled => MvApplicationError::new(
                    crate::mv::domain::application::MvApplicationErrorKind::ShutdownCancelled,
                    "MV statement was cancelled while waiting for activity gate",
                ),
                MvActivityAdmissionError::Stopping => MvApplicationError::new(
                    crate::mv::domain::application::MvApplicationErrorKind::ShutdownCancelled,
                    "frontend MV activity admission is closed",
                ),
            })
    }
}

#[derive(Clone)]
struct RefreshWorkerDependencies {
    readiness: Arc<MvReadinessPort>,
    refresh: refresh::FrontendMvRefreshDependencies,
    ports: crate::mv::domain::iceberg_refresh::IcebergMvCorePorts,
    execution: crate::query::mv_action::MvExecutionServices,
    topology: BackendTopologyService,
    role: novarocks_types::ClusterRole,
    maintenance_config: MaintenanceCoordinatorConfig,
    table_maintenance_engine: Arc<dyn TableMaintenanceEngine>,
    table_maintenance_service: Arc<dyn TableMaintenanceService>,
    product_service: Arc<MvKeyedProductService>,
    root_admission: RootAdmissionHandle,
    optimizer_query_mem_limit_bytes: u64,
    attempt_timeout: Duration,
    /// The existing FE runtime hosts the two original serial asynchronous
    /// tasks and the shared warehouse queue; no worker or local queue is added.
    runtime: tokio::runtime::Handle,
    startup_isolation: Option<crate::mv::startup_isolation_file::StartupIsolationSource>,
}

struct MvBackgroundHealth(novarocks_query_application::api::QueryExecutionClient);
impl novarocks_mv_application::process_runtime::MvBackgroundHealthPort for MvBackgroundHealth {
    fn record_failure(&self, cause: Arc<dyn std::error::Error + Send + Sync>) {
        self.0.report_process_failure(
            novarocks_query_application::api::StatementNativeHealthFailure::actual_exit_unproven(
                "MV background original source failed before actual closure",
                cause,
            ),
        );
    }
}

fn start_background_workers(
    dependencies: RefreshWorkerDependencies,
) -> Result<MvBackgroundRuntime, MvBackgroundEngineError> {
    let interval = Duration::from_millis(
        dependencies
            .product_service
            .scheduler_tick_interval_ms()
            .max(1),
    );
    let maintenance_interval =
        Duration::from_millis(dependencies.maintenance_config.tick_interval_ms.max(1));
    let scheduler = background_original::SchedulerOriginal::new(
        background_original::OriginalWorkerDependencies::from(&dependencies),
    );
    let maintenance = background_original::MaintenanceOriginal::new(&dependencies);
    MvBackgroundRuntime::start(
        &dependencies.runtime,
        Arc::new(MvBackgroundHealth(dependencies.execution.client.clone())),
        interval,
        maintenance_interval,
        MvBackgroundTasks::new(
            Box::new(move |stop, maintenance_wakeup_tx| {
                Box::pin(scheduler.clone().run(stop, maintenance_wakeup_tx))
            }),
            Box::new(move |stop| Box::pin(maintenance.clone().run(stop))),
        ),
    )
    .map_err(|error| {
        MvBackgroundEngineError::new(MvBackgroundEngineErrorKind::TransientUnavailable, error)
    })
}

/// One startup-continuation pass, if this process still owns the management
/// authority the pass needs. A product without it has nothing to reopen.
fn run_startup_continuation(
    source: &crate::mv::startup_isolation_file::StartupIsolationSource,
    readiness: &MvReadinessPort,
    connector_control: &dyn novarocks_spi::connector::ConnectorControlResolver,
    product_service: &MvKeyedProductService,
) {
    let (Some(entrance), Some(continuation)) = (
        product_service.management_entrance(),
        product_service.management_continuation(),
    ) else {
        return;
    };
    let report = crate::mv::domain::startup_continuation::run_startup_continuation_pass(
        &crate::mv::domain::startup_continuation::StartupContinuationPass {
            entrance: entrance.as_ref(),
            continuation: continuation.as_ref(),
            readiness,
            connector_control,
            source,
        },
    );
    if !report.is_silent() {
        tracing::info!(
            readmitted = report.readmitted,
            waiting = report.waiting,
            operator_only = report.operator_only,
            not_covered = report.not_covered,
            failed = report.failed,
            "MV startup continuation pass"
        );
    }
}

fn lifecycle_error(error: impl std::fmt::Display) -> MvBackgroundEngineError {
    MvBackgroundEngineError::new(
        MvBackgroundEngineErrorKind::InvariantViolation,
        error.to_string(),
    )
}

fn scheduler_outcome_log_fields(
    disposition: &ScheduledRefreshDisposition,
) -> Option<(&'static str, &str)> {
    match disposition {
        ScheduledRefreshDisposition::TransientUnavailable(reason) => {
            Some(("transient_unavailable", reason.message()))
        }
        ScheduledRefreshDisposition::InvalidDefinition(reason) => {
            Some(("invalid_definition", reason.message()))
        }
        ScheduledRefreshDisposition::TerminalFailure(reason) => {
            Some(("terminal_failure", reason.message()))
        }
        ScheduledRefreshDisposition::Corruption(reason) => Some(("corruption", reason.message())),
        ScheduledRefreshDisposition::InvariantViolation(reason) => {
            Some(("invariant_violation", reason.message()))
        }
        ScheduledRefreshDisposition::TargetGone(_) => {
            Some(("target_gone", "MV target no longer exists"))
        }
        ScheduledRefreshDisposition::Completed
        | ScheduledRefreshDisposition::NoOp
        | ScheduledRefreshDisposition::AlreadyActive(_)
        | ScheduledRefreshDisposition::ShutdownCancelled(_) => None,
    }
}

fn application_disposition(error: MvApplicationError) -> ScheduledRefreshDisposition {
    use crate::mv::domain::application::MvApplicationErrorKind;
    let failure = novarocks_mv_application::scheduler_runtime::MvRefreshFailure::new(
        error.message(),
        error.compile_control_error(),
    );
    match error.kind() {
        MvApplicationErrorKind::AlreadyActive => {
            ScheduledRefreshDisposition::AlreadyActive(error.compile_control_error())
        }
        MvApplicationErrorKind::Unavailable | MvApplicationErrorKind::BindingInvalidated => {
            ScheduledRefreshDisposition::TransientUnavailable(failure)
        }
        MvApplicationErrorKind::InvalidRequest => {
            ScheduledRefreshDisposition::InvalidDefinition(failure)
        }
        MvApplicationErrorKind::CommitUnknown
        | MvApplicationErrorKind::KnownCommittedFinalizeFailed => {
            ScheduledRefreshDisposition::TerminalFailure(failure)
        }
        MvApplicationErrorKind::ShutdownCancelled => {
            ScheduledRefreshDisposition::ShutdownCancelled(error.compile_control_error())
        }
        MvApplicationErrorKind::Engine => ScheduledRefreshDisposition::InvariantViolation(failure),
    }
}

fn now_unix_millis() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .try_into()
        .unwrap_or(i64::MAX)
}

#[cfg(test)]
mod shutdown_tests {
    use std::time::{Duration, Instant};

    use super::{MvBackgroundRuntime, application_disposition};
    use crate::mv::domain::application::{MvApplicationError, MvApplicationErrorKind};
    use crate::mv::scheduler::ScheduledRefreshDisposition;

    #[test]
    fn pre_dispatch_binding_invalidation_is_reprepared_not_recorded_as_unknown() {
        let disposition = application_disposition(MvApplicationError::new(
            MvApplicationErrorKind::BindingInvalidated,
            "test target generation changed",
        ));

        assert!(matches!(
            disposition,
            ScheduledRefreshDisposition::TransientUnavailable(_)
        ));
    }

    #[tokio::test]
    async fn shared_deadline_retains_the_same_mv_worker_join_for_retry() {
        let (maintenance_wakeup_tx, _maintenance_wakeup_rx) = tokio::sync::mpsc::channel(1);
        let (release_tx, release_rx) = tokio::sync::oneshot::channel();
        let refresh_worker = tokio::spawn(async move {
            let _ = release_rx.await;
        });
        let maintenance_worker = tokio::spawn(async {});
        let mut runtime = MvBackgroundRuntime::new(
            novarocks_mv_application::process_runtime::MvBackgroundStop::default(),
            refresh_worker,
            maintenance_wakeup_tx,
            maintenance_worker,
        );
        let error = runtime
            .stop_and_join_until(Instant::now() + Duration::from_millis(10))
            .await
            .expect_err("blocked MV worker must respect the shared deadline");
        assert!(error.contains("shared shutdown deadline"));
        release_tx.send(()).unwrap();
        runtime
            .stop_and_join_until(Instant::now() + Duration::from_secs(1))
            .await
            .expect("the retained MV worker join remains retryable");
    }
    #[tokio::test]
    async fn retry_still_joins_mv_maintenance_after_refresh_already_stopped() {
        let (maintenance_wakeup_tx, _maintenance_wakeup_rx) = tokio::sync::mpsc::channel(1);
        let (release_tx, release_rx) = tokio::sync::oneshot::channel();
        let maintenance_worker = tokio::spawn(async move {
            let _ = release_rx.await;
        });
        let refresh_worker = tokio::spawn(async {});
        let mut runtime = MvBackgroundRuntime::new(
            novarocks_mv_application::process_runtime::MvBackgroundStop::default(),
            refresh_worker,
            maintenance_wakeup_tx,
            maintenance_worker,
        );
        let error = runtime
            .stop_and_join_until(Instant::now() + Duration::from_millis(10))
            .await
            .expect_err("blocked MV maintenance must respect the shared deadline");
        assert!(error.contains("shared shutdown deadline"));
        release_tx.send(()).unwrap();
        runtime
            .stop_and_join_until(Instant::now() + Duration::from_secs(1))
            .await
            .expect("retry must join the retained MV maintenance worker");
    }
}

#[cfg(test)]
mod compile_control_tests {
    use crate::mv::domain::application::MvApplicationErrorKind;
    use crate::query_execution::mv_assembly::refresh_preparation::async_facts::PreparationError;
    use novarocks_type_contract::CompileControlError;

    #[test]
    fn foreground_preparation_terminal_retains_control_without_changing_kind() {
        for control in [
            CompileControlError::Cancelled,
            CompileControlError::DeadlineExceeded,
            CompileControlError::ResourceExhausted,
        ] {
            let error = PreparationError::from(novarocks_sql::compiler::SqlCompileError::from(
                control.clone(),
            ))
            .0;
            assert_eq!(error.compile_control_error(), Some(control.clone()));
            assert_eq!(error.kind(), MvApplicationErrorKind::InvalidRequest);
            let command = crate::mv::command::mv_application_command_error(error);
            assert_eq!(command.compile_control_error(), Some(control.clone()));
            assert_eq!(command.to_string(), control.to_string());
            assert_eq!(
                command.kind(),
                if control == CompileControlError::Cancelled {
                    novarocks_query_application::api::CommandErrorKind::Cancelled
                } else {
                    novarocks_query_application::api::CommandErrorKind::Failed
                }
            );
        }
    }
    #[test]
    fn application_compile_control_cause_reaches_actual_scheduler_terminal() {
        use super::{application_disposition, scheduler_outcome_log_fields};
        use novarocks_mv_application::scheduler::MvSchedulerConfig;
        use novarocks_mv_application::scheduler_runtime::{
            MvRefreshRuntimeDecision, MvRefreshSchedulerRuntime,
        };
        for control in [
            CompileControlError::Cancelled,
            CompileControlError::DeadlineExceeded,
            CompileControlError::ResourceExhausted,
            CompileControlError::Source(novarocks_failure_contract::SharedFailureCause::new(
                novarocks_plan_codec::PhysicalEncodeError::UnsupportedCapability("router".into()),
            )),
            CompileControlError::Source(novarocks_failure_contract::SharedFailureCause::new(
                novarocks_plan_codec::PhysicalEncodeError::Invalid("unsupported diagnostic".into()),
            )),
        ] {
            let error = PreparationError::from(novarocks_sql::compiler::SqlCompileError::from(
                control.clone(),
            ))
            .0;
            let message = error.message().to_owned();
            let disposition = application_disposition(error);
            assert_eq!(disposition.compile_control_error(), Some(control.clone()));
            assert_eq!(
                scheduler_outcome_log_fields(&disposition),
                Some(("invalid_definition", message.as_str()))
            );
            let mut runtime = MvRefreshSchedulerRuntime::<i64, ()>::new(MvSchedulerConfig::new(
                true, 1, 1, 10, 40,
            ));
            let decision = runtime.record(&7, disposition, 100);
            assert!(matches!(decision, MvRefreshRuntimeDecision::Blocked { .. }));
            assert_eq!(decision.compile_control_error(), Some(control));
            assert!(runtime.is_suppressed(&7, 100));
        }
    }

    #[test]
    fn retryable_preparation_error_preserves_its_typed_disposition() {
        use super::{application_disposition, scheduler_outcome_log_fields};
        use novarocks_failure_contract::FailureProjection;
        use novarocks_mv_application::scheduler::MvSchedulerConfig;
        use novarocks_mv_application::scheduler_runtime::{
            MvRefreshRuntimeDecision, MvRefreshSchedulerRuntime,
        };
        use novarocks_spi::connector::{ConnectorError, ConnectorErrorKind};

        let error = PreparationError::from(ConnectorError::new(
            ConnectorErrorKind::Unavailable,
            "connector is starting",
        ))
        .0;
        let cause = error
            .original_failure()
            .expect("provider cause is retained")
            .clone();
        let fact = cause.failure_fact();
        let command = crate::mv::command::mv_application_command_error(error.clone());
        assert_eq!(command.failure_cause(), Some(&cause));
        assert_eq!(command.failure_cause().unwrap().failure_fact(), fact);
        let message = error.message().to_owned();
        let disposition = application_disposition(error);
        assert_eq!(
            scheduler_outcome_log_fields(&disposition),
            Some(("transient_unavailable", message.as_str()))
        );
        let mut runtime =
            MvRefreshSchedulerRuntime::<i64, ()>::new(MvSchedulerConfig::new(true, 1, 1, 10, 40));
        let MvRefreshRuntimeDecision::TransientBackoff { error, retry_at_ms } =
            runtime.record(&7, disposition, 100)
        else {
            panic!("provider unavailability must remain retryable");
        };
        assert_eq!(error.message(), message);
        assert!(retry_at_ms > 100);
        assert!(runtime.is_suppressed(&7, 100));
        assert!(!runtime.is_suppressed(&7, retry_at_ms));
    }

    #[test]
    fn definition_preparation_error_remains_blocked() {
        use super::{application_disposition, scheduler_outcome_log_fields};
        use novarocks_mv_application::scheduler::MvSchedulerConfig;
        use novarocks_mv_application::scheduler_runtime::{
            MvRefreshRuntimeDecision, MvRefreshSchedulerRuntime,
        };

        let error = PreparationError::user("stored MV contract is incompatible").0;
        let message = error.message().to_owned();
        let disposition = application_disposition(error);
        assert_eq!(
            scheduler_outcome_log_fields(&disposition),
            Some(("invalid_definition", message.as_str()))
        );
        let mut runtime =
            MvRefreshSchedulerRuntime::<i64, ()>::new(MvSchedulerConfig::new(true, 1, 1, 10, 40));
        let MvRefreshRuntimeDecision::Blocked { error } = runtime.record(&7, disposition, 100)
        else {
            panic!("invalid stored definitions must remain blocked");
        };
        assert_eq!(error.message(), message);
        assert!(runtime.is_suppressed(&7, 100));
        assert!(runtime.is_suppressed(&7, i64::MAX));
    }

    #[test]
    fn background_preparation_terminal_retains_control_without_changing_policy_kind() {
        use super::{application_disposition, scheduler_outcome_log_fields};
        use novarocks_mv_application::scheduler::MvSchedulerConfig;
        use novarocks_mv_application::scheduler_runtime::{
            MvRefreshRuntimeDecision, MvRefreshSchedulerRuntime,
        };
        for control in [
            CompileControlError::Cancelled,
            CompileControlError::DeadlineExceeded,
            CompileControlError::ResourceExhausted,
        ] {
            let error = PreparationError::from(novarocks_sql::compiler::SqlCompileError::from(
                control.clone(),
            ))
            .0;
            let message = error.message().to_owned();
            let disposition = application_disposition(error);
            assert_eq!(disposition.compile_control_error(), Some(control.clone()));
            assert_eq!(
                scheduler_outcome_log_fields(&disposition),
                Some(("invalid_definition", message.as_str()))
            );
            let mut runtime = MvRefreshSchedulerRuntime::<i64, ()>::new(MvSchedulerConfig::new(
                true, 1, 1, 10, 40,
            ));
            let decision = runtime.record(&7, disposition, 100);
            assert!(matches!(decision, MvRefreshRuntimeDecision::Blocked { .. }));
            assert_eq!(decision.compile_control_error(), Some(control));
            assert!(runtime.is_suppressed(&7, i64::MAX));
        }
    }
}
