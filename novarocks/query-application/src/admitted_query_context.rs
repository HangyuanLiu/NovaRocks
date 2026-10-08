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

//! Immutable request state captured exactly once at statement admission.
// Design: ADR-0011 (docs/adr/ADR-0011-immutable-request-execution-context.md)

use std::time::Instant;

use crate::api::BackendTopologySnapshot;
use crate::cancellation::QueryCancellationView;
use crate::request_session::RequestSessionContext;
use novarocks_sql::compiler::SessionOptimizerSettings;
use novarocks_types::ClusterRole;
use novarocks_workload_control::{ResultWindowAlias, ResultWindowClass, WorkError, WorkScope};

/// All inputs accepted at the frontend statement-admission boundary.
///
/// The contained values are moved into an immutable [`RequestContext`]; the
/// individual projection constructors intentionally remain private.
pub struct RequestAdmission {
    current_catalog: Option<String>,
    current_database: String,
    role: ClusterRole,
    topology: BackendTopologySnapshot,
    deadline: Option<Instant>,
    cancellation: QueryCancellationView,
    optimizer_settings: SessionOptimizerSettings,
    sql_semantics: novarocks_sql::sql_mode::SqlSemanticSettings,
}

impl RequestAdmission {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        current_catalog: Option<String>,
        current_database: String,
        role: ClusterRole,
        topology: BackendTopologySnapshot,
        deadline: Option<Instant>,
        cancellation: QueryCancellationView,
        optimizer_settings: SessionOptimizerSettings,
        sql_semantics: novarocks_sql::sql_mode::SqlSemanticSettings,
    ) -> Self {
        Self {
            current_catalog,
            current_database,
            role,
            topology,
            deadline,
            cancellation,
            optimizer_settings,
            sql_semantics,
        }
    }
}

/// Statement-stable inputs retained while a distributed statement may need a
/// new topology round.  It deliberately owns no backend snapshot: every
/// round derives a fresh [`QueryExecutionContext`] from this immutable
/// admission boundary.
///
/// The existing [`RequestContext`] remains the narrow projection consumed by
/// compiler and coordinator code.  It is therefore impossible for a caller
/// to manufacture a later round by modifying an already prepared request.
#[derive(Clone)]
pub struct StatementAdmissionContext {
    session: RequestSessionContext,
    role: ClusterRole,
    deadline: Option<Instant>,
    cancellation: QueryCancellationView,
}

impl StatementAdmissionContext {
    pub fn new(
        current_catalog: Option<String>,
        current_database: String,
        role: ClusterRole,
        deadline: Option<Instant>,
        cancellation: QueryCancellationView,
        optimizer_settings: SessionOptimizerSettings,
        sql_semantics: novarocks_sql::sql_mode::SqlSemanticSettings,
    ) -> Self {
        Self {
            session: RequestSessionContext::new(
                current_catalog,
                current_database,
                optimizer_settings,
                sql_semantics,
            ),
            role,
            deadline,
            cancellation,
        }
    }

    /// Derive the compiler/coordinator projection for exactly one frozen
    /// topology round.  Semantic session state, deadline, and cancellation
    /// identity remain those admitted for the original statement.
    pub fn for_topology(&self, topology: BackendTopologySnapshot) -> RequestContext {
        RequestContext::new(
            self.session.clone(),
            QueryExecutionContext::new(
                self.role,
                topology,
                self.deadline,
                self.cancellation.clone(),
                self.session.optimizer_settings().clone(),
                self.session.sql_semantics().clone(),
            ),
        )
    }

    pub fn session(&self) -> &RequestSessionContext {
        &self.session
    }

    pub const fn role(&self) -> ClusterRole {
        self.role
    }

    pub const fn deadline(&self) -> Option<Instant> {
        self.deadline
    }

    pub fn cancellation(&self) -> &QueryCancellationView {
        &self.cancellation
    }
}

/// Exact runtime capacity supplied by admission. It is never projected into
/// a completed plan, frozen semantic description, or native plan DTO.
#[derive(Clone)]
pub struct QueryResultCapacityBinding {
    scope: WorkScope,
    window: ResultWindowAlias,
}
impl QueryResultCapacityBinding {
    pub fn try_new(scope: &WorkScope, window: ResultWindowAlias) -> Result<Self, WorkError> {
        if !window.is_for_scope(scope) || window.class() == ResultWindowClass::Closing {
            return Err(WorkError::Conflict);
        }
        scope.check()?;
        Ok(Self {
            scope: scope.clone(),
            window,
        })
    }
    pub fn scope(&self) -> &WorkScope {
        &self.scope
    }
    pub fn window_alias(&self) -> ResultWindowAlias {
        self.window.clone()
    }
    pub fn class(&self) -> ResultWindowClass {
        self.window.class()
    }

    /// A nested stage keeps its parent's whole envelope, with exact child
    /// attribution. No result-capacity admission is performed here.
    pub fn for_child(&self, child: &WorkScope) -> Result<Self, WorkError> {
        Self::try_new(child, self.window.for_child(child)?)
    }
}

/// Execution inputs which must remain identical from planning through native
/// coordinator submission.
///
/// CLS-R2 boundary: request admission — deciding these inputs once per
/// statement — is frontend authority and moves there. The immutable value
/// itself stays with the aggregate package because `mv` and `connector` still
/// receive it as a parameter. Those consumers leave with CLS-R3 and CLS-R5.
#[derive(Clone)]
pub struct QueryExecutionContext {
    result_capacity: Option<QueryResultCapacityBinding>,
    role: ClusterRole,
    topology: BackendTopologySnapshot,
    deadline: Option<Instant>,
    cancellation: QueryCancellationView,
    optimizer_settings: SessionOptimizerSettings,
    sql_semantics: novarocks_sql::sql_mode::SqlSemanticSettings,
}

impl QueryExecutionContext {
    pub fn new(
        role: ClusterRole,
        topology: BackendTopologySnapshot,
        deadline: Option<Instant>,
        cancellation: QueryCancellationView,
        mut optimizer_settings: SessionOptimizerSettings,
        sql_semantics: novarocks_sql::sql_mode::SqlSemanticSettings,
    ) -> Self {
        if optimizer_settings.cbo_broadcast_backend_count.is_none()
            && optimizer_settings.effective_backend_count.is_none()
            && !topology.targets().is_empty()
        {
            optimizer_settings.effective_backend_count = Some(topology.targets().len() as f64);
        }
        Self {
            result_capacity: None,
            role,
            topology,
            deadline,
            cancellation,
            optimizer_settings,
            sql_semantics,
        }
    }

    pub fn with_result_capacity(
        mut self,
        binding: QueryResultCapacityBinding,
    ) -> Result<Self, WorkError> {
        if self.result_capacity.is_some() {
            return Err(WorkError::Conflict);
        }
        binding.scope().check()?;
        self.result_capacity = Some(binding);
        Ok(self)
    }

    pub fn result_capacity(&self) -> Option<&QueryResultCapacityBinding> {
        self.result_capacity.as_ref()
    }

    pub const fn role(&self) -> ClusterRole {
        self.role
    }

    pub fn topology(&self) -> &BackendTopologySnapshot {
        &self.topology
    }

    pub const fn deadline(&self) -> Option<Instant> {
        self.deadline
    }

    pub fn cancellation(&self) -> &QueryCancellationView {
        &self.cancellation
    }

    /// Settings frozen with the request so DML and coordinator-adjacent plan
    /// construction never consult process- or thread-local session state.
    pub fn sql_semantics(&self) -> &novarocks_sql::sql_mode::SqlSemanticSettings {
        &self.sql_semantics
    }

    pub fn optimizer_settings(&self) -> &SessionOptimizerSettings {
        &self.optimizer_settings
    }
}

/// Complete immutable statement context.  The frontend application admits a
/// statement once, then consumers receive only narrow projections.
#[derive(Clone)]
pub struct RequestContext {
    session: RequestSessionContext,
    execution: QueryExecutionContext,
}

#[allow(
    dead_code,
    reason = "Retained for target-specific frontend integration and regression coverage."
)]
impl RequestContext {
    pub fn new(session: RequestSessionContext, execution: QueryExecutionContext) -> Self {
        Self { session, execution }
    }

    /// Freeze one statement's context.
    ///
    /// The cost budget arrives inside `optimizer_settings`: the frontend query
    /// service and the MV worker each carry the value resolved from `[runtime]`
    /// at composition, so admission never reads a process-global configuration.
    /// A settings value of `None` means "no budget", which leaves optimizer
    /// costing on its profile default.
    pub fn admit(admission: RequestAdmission) -> Self {
        let statement = StatementAdmissionContext::new(
            admission.current_catalog,
            admission.current_database,
            admission.role,
            admission.deadline,
            admission.cancellation,
            admission.optimizer_settings,
            admission.sql_semantics,
        );
        statement.for_topology(admission.topology)
    }

    /// Bind admission's runtime sidecar before source preparation begins.
    pub fn with_result_capacity(
        mut self,
        binding: QueryResultCapacityBinding,
    ) -> Result<Self, WorkError> {
        self.execution = self.execution.with_result_capacity(binding)?;
        Ok(self)
    }

    pub fn session(&self) -> &RequestSessionContext {
        &self.session
    }

    pub fn execution(&self) -> &QueryExecutionContext {
        &self.execution
    }

    pub fn preparation(&self) -> QueryPreparationContext<'_> {
        QueryPreparationContext {
            session: &self.session,
            execution: &self.execution,
        }
    }
}

/// Borrowed projection used by SQL preparation and optimizer layers.
pub struct QueryPreparationContext<'a> {
    session: &'a RequestSessionContext,
    execution: &'a QueryExecutionContext,
}

impl<'a> QueryPreparationContext<'a> {
    pub fn session(&self) -> &'a RequestSessionContext {
        self.session
    }

    pub fn execution(&self) -> &'a QueryExecutionContext {
        self.execution
    }
}

#[cfg(test)]
mod tests {
    use novarocks_execution_contract::{
        AdmissionEpochCapability, BackendProcessDescriptor, RuntimeEndpoint,
    };
    use novarocks_types::BackendProcessId;

    use super::*;
    use crate::api::LiveBackendTarget;
    use crate::cancellation::QueryCancellationSource;

    fn capacity_control() -> (
        novarocks_workload_control::WorkloadControl,
        novarocks_workload_control::ResultCapacityHandle,
    ) {
        use novarocks_workload_control::{
            ResourceConfig, ResultCapacityConfig, WorkloadConfig, WorkloadControl,
        };
        let control = WorkloadControl::try_new(
            WorkloadConfig::default(),
            ResourceConfig {
                total_bytes: 1024 * 1024,
                control_bytes: 1024,
                per_scope_bytes: 1024 * 1024 - 1024,
            },
        )
        .unwrap();
        let capacity = control
            .configure_result_capacity(ResultCapacityConfig::V1)
            .unwrap();
        control.mark_ready().unwrap();
        (control, capacity)
    }

    #[test]
    fn runtime_capacity_is_explicit_and_survives_execution_clones() {
        use novarocks_workload_control::{WorkClass, WorkRequest};
        let (control, capacity) = capacity_control();
        let (root, window) = control
            .root_admission()
            .try_begin_root_with_result(
                WorkRequest::new(WorkClass::Management),
                ResultWindowClass::Internal,
            )
            .unwrap();
        let context = RequestContext::admit(RequestAdmission::new(
            None,
            "db1".to_string(),
            ClusterRole::Fe,
            topology(7, 3),
            None,
            QueryCancellationSource::new().view(),
            SessionOptimizerSettings::default(),
            novarocks_sql::sql_mode::SqlSemanticSettings::default(),
        ))
        .with_result_capacity(
            QueryResultCapacityBinding::try_new(&root.owner.scope(), window.retain_alias())
                .unwrap(),
        )
        .unwrap();
        let execution = context.execution().clone();
        assert!(
            execution
                .clone()
                .with_result_capacity(execution.result_capacity().unwrap().clone(),)
                .is_err()
        );
        let replacement = StatementAdmissionContext::new(
            None,
            "db1".to_string(),
            execution.role(),
            execution.deadline(),
            execution.cancellation().clone(),
            execution.optimizer_settings().clone(),
            execution.sql_semantics().clone(),
        )
        .for_topology(topology(8, 1));
        assert!(replacement.execution().result_capacity().is_none());
        assert_eq!(
            execution.result_capacity().unwrap().class(),
            ResultWindowClass::Internal
        );
        drop(context);
        drop(window);
        root.owner.complete();
        root.business.release();
        assert_eq!(capacity.snapshot().held_positions, [0, 0, 1, 0]);
        drop(execution);
        assert_eq!(capacity.snapshot().held_positions, [0; 4]);
    }

    #[test]
    fn runtime_capacity_rejects_foreign_scope_and_delegates_without_new_position() {
        use novarocks_workload_control::{WorkClass, WorkRequest};
        let (control, capacity) = capacity_control();
        let (root, window) = control
            .root_admission()
            .try_begin_root_with_result(
                WorkRequest::new(WorkClass::Management),
                ResultWindowClass::Internal,
            )
            .unwrap();
        let binding =
            QueryResultCapacityBinding::try_new(&root.owner.scope(), window.retain_alias())
                .unwrap();
        let (foreign, _) = capacity_control();
        let other = foreign
            .try_begin_root(WorkRequest::new(WorkClass::Management))
            .unwrap();
        assert!(
            QueryResultCapacityBinding::try_new(&other.owner.scope(), window.retain_alias())
                .is_err()
        );
        let child = root
            .owner
            .scope()
            .child(WorkRequest::new(WorkClass::Management))
            .unwrap();
        let delegated = binding.for_child(&child.scope()).unwrap();
        assert!(delegated.window_alias().is_for_scope(&child.scope()));
        assert_eq!(capacity.snapshot().held_positions, [0, 0, 1, 0]);
        drop(binding);
        drop(window);
        child.complete();
        root.owner.complete();
        root.business.release();
        assert_eq!(capacity.snapshot().held_positions, [0, 0, 1, 0]);
        assert!(
            QueryResultCapacityBinding::try_new(&other.owner.scope(), delegated.window_alias())
                .is_err()
        );
        drop(delegated);
        assert_eq!(capacity.snapshot().held_positions, [0; 4]);
        other.owner.complete();
        other.business.release();
    }

    fn topology(revision: u64, backend_count: usize) -> BackendTopologySnapshot {
        let targets = (0..backend_count)
            .map(|backend_idx| {
                LiveBackendTarget::new(
                    backend_idx,
                    BackendProcessDescriptor::try_new(
                        BackendProcessId::new_v7(),
                        RuntimeEndpoint::new(
                            "127.0.0.1",
                            9030 + i32::try_from(backend_idx).expect("fixture backend index"),
                        )
                        .expect("valid loopback endpoint"),
                        RuntimeEndpoint::new(format!("control-{backend_idx}.test.invalid"), 19061)
                            .expect("valid control endpoint"),
                        "test-deployment",
                        "test-build",
                        novarocks_types::NativeCompatibilityId::new([0x71; 32]),
                        4096,
                    )
                    .expect("valid test descriptor"),
                    AdmissionEpochCapability::try_from_bytes(
                        [u8::try_from(backend_idx + 1).expect("fixture backend index"); 16],
                    )
                    .expect("nonzero epoch"),
                )
            })
            .collect();
        BackendTopologySnapshot::try_new(revision, targets).expect("valid topology")
    }

    #[test]
    fn projections_share_one_cancellation_and_topology_identity() {
        let cancellation = QueryCancellationSource::new();
        let snapshot = BackendTopologySnapshot::try_new(
            9,
            vec![LiveBackendTarget::new(
                7,
                BackendProcessDescriptor::try_new(
                    BackendProcessId::new_v7(),
                    RuntimeEndpoint::new("127.0.0.1", 9030).expect("valid loopback endpoint"),
                    RuntimeEndpoint::new("control.test.invalid", 19061)
                        .expect("valid control endpoint"),
                    "test-deployment",
                    "test-build",
                    novarocks_types::NativeCompatibilityId::new([0x71; 32]),
                    4096,
                )
                .expect("valid test descriptor"),
                AdmissionEpochCapability::try_from_bytes([0x61; 16]).expect("nonzero epoch"),
            )],
        )
        .expect("valid snapshot");
        let context = RequestContext::new(
            RequestSessionContext::new(
                Some("iceberg".to_string()),
                "db1".to_string(),
                SessionOptimizerSettings::default(),
                novarocks_sql::sql_mode::SqlSemanticSettings::default(),
            ),
            QueryExecutionContext::new(
                ClusterRole::Fe,
                snapshot,
                None,
                cancellation.view(),
                SessionOptimizerSettings::default(),
                novarocks_sql::sql_mode::SqlSemanticSettings::default(),
            ),
        );

        assert_eq!(context.execution().topology().revision(), 9);
        assert_eq!(context.session().current_catalog(), Some("iceberg"));
        assert!(!context.execution().cancellation().is_cancelled());
        cancellation.request(crate::cancellation::QueryCancellationReason::ClientDisconnected);
        assert!(context.execution().cancellation().is_cancelled());
    }

    #[test]
    fn admission_copies_session_settings_and_preserves_deadline() {
        let deadline = Instant::now();
        let mut admitted_settings = SessionOptimizerSettings {
            enable_eliminate_agg: true,
            cbo_broadcast_backend_count: Some(3.0),
            ..SessionOptimizerSettings::default()
        };
        let context = RequestContext::new(
            RequestSessionContext::new(
                None,
                "db1".to_string(),
                admitted_settings.clone(),
                novarocks_sql::sql_mode::SqlSemanticSettings::default(),
            ),
            QueryExecutionContext::new(
                ClusterRole::Fe,
                BackendTopologySnapshot::empty(4),
                Some(deadline),
                QueryCancellationSource::new().view(),
                admitted_settings.clone(),
                novarocks_sql::sql_mode::SqlSemanticSettings::default(),
            ),
        );

        admitted_settings.enable_eliminate_agg = false;
        admitted_settings.cbo_broadcast_backend_count = Some(99.0);

        assert!(context.session().optimizer_settings().enable_eliminate_agg);
        assert_eq!(
            context
                .execution()
                .optimizer_settings()
                .cbo_broadcast_backend_count,
            Some(3.0)
        );
        assert_eq!(context.execution().deadline(), Some(deadline));
        assert!(context.execution().topology().targets().is_empty());
    }

    /// Load-bearing for the sealed-description rebind: a replacement round
    /// reuses the first round's plan, so the count that shaped that plan must
    /// not be re-derived from the new topology. `effective_backend_count` is
    /// not a mere cost hint -- it gates BroadcastJoin feasibility, so a
    /// re-derived value can flip a join to shuffle and invalidate the very
    /// description the retry is rebinding.
    #[test]
    fn statement_admission_freezes_first_topology_count_across_later_rounds() {
        let first = RequestContext::admit(RequestAdmission::new(
            None,
            "db1".to_string(),
            ClusterRole::Fe,
            topology(7, 3),
            None,
            QueryCancellationSource::new().view(),
            SessionOptimizerSettings::default(),
            novarocks_sql::sql_mode::SqlSemanticSettings::default(),
        ));
        let statement = StatementAdmissionContext::new(
            None,
            "db1".to_string(),
            first.execution().role(),
            first.execution().deadline(),
            first.execution().cancellation().clone(),
            first.execution().optimizer_settings().clone(),
            first.execution().sql_semantics().clone(),
        );
        let replacement = statement.for_topology(topology(8, 1));

        assert_eq!(
            first
                .execution()
                .optimizer_settings()
                .effective_backend_count,
            Some(3.0)
        );
        assert_eq!(
            replacement
                .execution()
                .optimizer_settings()
                .effective_backend_count,
            Some(3.0)
        );
        assert_eq!(
            first.execution().optimizer_settings(),
            replacement.execution().optimizer_settings()
        );
    }

    #[test]
    fn explicit_session_backend_count_wins_over_admitted_topology() {
        let first = RequestContext::admit(RequestAdmission::new(
            None,
            "db1".to_string(),
            ClusterRole::Fe,
            topology(7, 3),
            None,
            QueryCancellationSource::new().view(),
            SessionOptimizerSettings {
                cbo_broadcast_backend_count: Some(11.0),
                ..SessionOptimizerSettings::default()
            },
            novarocks_sql::sql_mode::SqlSemanticSettings::default(),
        ));
        let statement = StatementAdmissionContext::new(
            None,
            "db1".to_string(),
            first.execution().role(),
            first.execution().deadline(),
            first.execution().cancellation().clone(),
            first.execution().optimizer_settings().clone(),
            first.execution().sql_semantics().clone(),
        );
        let replacement = statement.for_topology(topology(8, 1));

        for context in [&first, &replacement] {
            assert_eq!(
                context
                    .execution()
                    .optimizer_settings()
                    .cbo_broadcast_backend_count,
                Some(11.0)
            );
            assert_eq!(
                context
                    .execution()
                    .optimizer_settings()
                    .effective_backend_count,
                None
            );
        }
    }

    #[test]
    fn existing_effective_backend_count_is_not_replaced_at_admission() {
        let context = RequestContext::admit(RequestAdmission::new(
            None,
            "db1".to_string(),
            ClusterRole::Fe,
            topology(7, 3),
            None,
            QueryCancellationSource::new().view(),
            SessionOptimizerSettings {
                effective_backend_count: Some(5.0),
                ..SessionOptimizerSettings::default()
            },
            novarocks_sql::sql_mode::SqlSemanticSettings::default(),
        ));

        assert_eq!(
            context
                .execution()
                .optimizer_settings()
                .effective_backend_count,
            Some(5.0)
        );
    }

    #[test]
    fn admission_carries_the_optimizer_query_memory_budget_verbatim() {
        let context = RequestContext::admit(RequestAdmission::new(
            None,
            "db1".to_string(),
            ClusterRole::Fe,
            BackendTopologySnapshot::empty(7),
            None,
            QueryCancellationSource::new().view(),
            SessionOptimizerSettings {
                optimizer_query_mem_limit_bytes: Some(512.0 * 1024.0 * 1024.0),
                ..SessionOptimizerSettings::default()
            },
            novarocks_sql::sql_mode::SqlSemanticSettings::default(),
        ));

        assert_eq!(
            context
                .execution()
                .optimizer_settings()
                .optimizer_query_mem_limit_bytes,
            Some(512.0 * 1024.0 * 1024.0)
        );
    }

    #[test]
    fn admission_without_a_budget_leaves_costing_on_its_profile_default() {
        let context = RequestContext::admit(RequestAdmission::new(
            None,
            "db1".to_string(),
            ClusterRole::Fe,
            BackendTopologySnapshot::empty(7),
            None,
            QueryCancellationSource::new().view(),
            SessionOptimizerSettings::default(),
            novarocks_sql::sql_mode::SqlSemanticSettings::default(),
        ));

        assert_eq!(
            context
                .execution()
                .optimizer_settings()
                .optimizer_query_mem_limit_bytes,
            None
        );
    }

    #[test]
    fn sql_mode_is_frozen_in_both_request_projections_and_topology_retries() {
        let mode = novarocks_sql::sql_mode::SqlSemanticSettings::default().with_sql_mode(
            novarocks_sql::sql_mode::SqlMode::from_assignment(
                "GROUP_CONCAT_LEGACY,ALLOW_THROW_EXCEPTION,ERROR_IF_OVERFLOW",
            ),
        );
        let first = RequestContext::admit(RequestAdmission::new(
            None,
            "db1".to_owned(),
            ClusterRole::Fe,
            topology(7, 3),
            None,
            QueryCancellationSource::new().view(),
            SessionOptimizerSettings::default(),
            mode.clone(),
        ));
        assert_eq!(first.session().sql_semantics(), &mode);
        assert_eq!(first.execution().sql_semantics(), &mode);
        let statement = StatementAdmissionContext::new(
            None,
            "db1".to_owned(),
            first.execution().role(),
            first.execution().deadline(),
            first.execution().cancellation().clone(),
            first.execution().optimizer_settings().clone(),
            first.session().sql_semantics().clone(),
        );
        let replacement = statement.for_topology(topology(8, 1));
        assert_eq!(replacement.session().sql_semantics(), &mode);
        assert_eq!(replacement.execution().sql_semantics(), &mode);
    }
}
