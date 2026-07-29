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

use crate::common::app_config::ClusterRole;
use crate::query_execution::backend::BackendTopologySnapshot;
use crate::query_execution::cancellation::QueryCancellationView;
use crate::sql::optimizer::options::SessionOptimizerSettings;

/// Session-derived inputs frozen at the request boundary.
#[derive(Clone, Debug)]
pub struct RequestSessionContext {
    current_catalog: Option<String>,
    current_database: String,
    optimizer_settings: SessionOptimizerSettings,
}

impl RequestSessionContext {
    pub(crate) fn new(
        current_catalog: Option<String>,
        current_database: String,
        optimizer_settings: SessionOptimizerSettings,
    ) -> Self {
        Self {
            current_catalog,
            current_database,
            optimizer_settings,
        }
    }

    pub fn current_catalog(&self) -> Option<&str> {
        self.current_catalog.as_deref()
    }

    pub fn current_database(&self) -> &str {
        &self.current_database
    }

    pub(crate) fn optimizer_settings(&self) -> &SessionOptimizerSettings {
        &self.optimizer_settings
    }
}

/// Execution inputs which must remain identical from planning through native
/// coordinator submission.
#[derive(Clone)]
pub struct QueryExecutionContext {
    role: ClusterRole,
    topology: BackendTopologySnapshot,
    deadline: Option<Instant>,
    cancellation: QueryCancellationView,
    optimizer_settings: SessionOptimizerSettings,
}

impl QueryExecutionContext {
    pub(crate) fn new(
        role: ClusterRole,
        topology: BackendTopologySnapshot,
        deadline: Option<Instant>,
        cancellation: QueryCancellationView,
        optimizer_settings: SessionOptimizerSettings,
    ) -> Self {
        Self {
            role,
            topology,
            deadline,
            cancellation,
            optimizer_settings,
        }
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
    pub(crate) fn optimizer_settings(&self) -> &SessionOptimizerSettings {
        &self.optimizer_settings
    }
}

/// Complete immutable statement context. Only the core server admission
/// adapter constructs it; consumers get a narrow projection.
#[derive(Clone)]
pub struct RequestContext {
    session: RequestSessionContext,
    execution: QueryExecutionContext,
}

impl RequestContext {
    pub(crate) fn new(session: RequestSessionContext, execution: QueryExecutionContext) -> Self {
        Self { session, execution }
    }

    pub fn session(&self) -> &RequestSessionContext {
        &self.session
    }

    pub fn execution(&self) -> &QueryExecutionContext {
        &self.execution
    }

    pub(crate) fn preparation(&self) -> QueryPreparationContext<'_> {
        QueryPreparationContext {
            session: &self.session,
            execution: &self.execution,
        }
    }
}

/// Borrowed projection used by SQL preparation and optimizer layers.
pub(crate) struct QueryPreparationContext<'a> {
    session: &'a RequestSessionContext,
    execution: &'a QueryExecutionContext,
}

impl<'a> QueryPreparationContext<'a> {
    pub(crate) fn session(&self) -> &'a RequestSessionContext {
        self.session
    }

    pub(crate) fn execution(&self) -> &'a QueryExecutionContext {
        self.execution
    }
}

#[cfg(test)]
mod tests {
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};

    use super::*;
    use crate::query_execution::backend::LiveBackendTarget;
    use crate::query_execution::cancellation::QueryCancellationSource;

    #[test]
    fn projections_share_one_cancellation_and_topology_identity() {
        let cancellation = QueryCancellationSource::new();
        let snapshot = BackendTopologySnapshot::try_new(
            9,
            vec![LiveBackendTarget::new(
                7,
                SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 9030),
                2,
            )],
        )
        .expect("valid snapshot");
        let context = RequestContext::new(
            RequestSessionContext::new(
                Some("iceberg".to_string()),
                "db1".to_string(),
                SessionOptimizerSettings::default(),
            ),
            QueryExecutionContext::new(
                ClusterRole::Fe,
                snapshot,
                None,
                cancellation.view(),
                SessionOptimizerSettings::default(),
            ),
        );

        assert_eq!(context.execution().topology().revision(), 9);
        assert_eq!(context.session().current_catalog(), Some("iceberg"));
        assert!(!context.execution().cancellation().is_cancelled());
        cancellation.request(
            crate::query_execution::cancellation::QueryCancellationReason::ClientDisconnected,
        );
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
            RequestSessionContext::new(None, "db1".to_string(), admitted_settings.clone()),
            QueryExecutionContext::new(
                ClusterRole::AllInOne,
                BackendTopologySnapshot::empty(4),
                Some(deadline),
                QueryCancellationSource::new().view(),
                admitted_settings.clone(),
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
}
