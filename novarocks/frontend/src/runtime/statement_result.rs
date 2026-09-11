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

use super::query_result::QueryResult;
use novarocks_query_application::protocol_delivery::GovernedProtocolOwner;
use novarocks_query_application::protocol_delivery::StreamingStatementResult;
use novarocks_query_application::session_control::GovernedQueryStatementOwner;
use novarocks_query_application::session_error::QueryServiceError;
use novarocks_workload_control::LocalResourceAuthority;

/// Neutral statement result carrier shared by Core domain handlers and the
/// Frontend query-assembly owner.
pub enum StatementResult {
    Query(QueryResult),
    /// Immediate query output whose business and cancellation owner remains
    /// live through the final MySQL protocol outcome.
    GovernedQuery(GovernedImmediateStatementResult),
    /// Move-only Query Application output. The protocol adapter owns this
    /// value until schema, every batch, and success EOF have reached the
    /// client or the connection has failed.
    StreamingQuery(StreamingStatementResult),
    /// Completion-only output whose statement owner remains live through the
    /// terminal OK packet.
    GovernedCompletion(GovernedCompletionStatementResult),
    /// Error output whose statement owner remains live through the terminal
    /// MySQL error packet.
    GovernedError(GovernedErrorStatementResult),
    Ok,
}

impl std::fmt::Debug for StatementResult {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Query(result) => formatter.debug_tuple("Query").field(result).finish(),
            Self::GovernedQuery(_) => formatter.write_str("GovernedQuery(..)"),
            Self::StreamingQuery(_) => formatter.write_str("StreamingQuery(..)"),
            Self::GovernedCompletion(_) => formatter.write_str("GovernedCompletion(..)"),
            Self::GovernedError(_) => formatter.write_str("GovernedError(..)"),
            Self::Ok => formatter.write_str("Ok"),
        }
    }
}

#[must_use = "the governed query result must be settled by its protocol owner"]
pub struct GovernedImmediateStatementResult {
    result: QueryResult,
    protocol: GovernedProtocolOwner,
}

impl GovernedImmediateStatementResult {
    pub(crate) fn new(
        result: QueryResult,
        resources: LocalResourceAuthority,
        statement: GovernedQueryStatementOwner,
    ) -> Self {
        Self {
            result,
            protocol: GovernedProtocolOwner::new(statement, resources),
        }
    }

    pub(crate) fn into_parts(self) -> (QueryResult, GovernedProtocolOwner) {
        (self.result, self.protocol)
    }
}

/// Completion-only output that retains its statement generation and business
/// permit until the MySQL adapter has written the terminal OK packet.
#[must_use = "the governed completion must be settled by its protocol owner"]
pub struct GovernedCompletionStatementResult {
    protocol: GovernedProtocolOwner,
}

impl GovernedCompletionStatementResult {
    pub(crate) fn new(
        resources: LocalResourceAuthority,
        statement: GovernedQueryStatementOwner,
    ) -> Self {
        Self {
            protocol: GovernedProtocolOwner::new(statement, resources),
        }
    }

    pub(crate) fn into_protocol(self) -> GovernedProtocolOwner {
        self.protocol
    }
}

/// Error output that retains its statement generation and business permit
/// until the MySQL adapter has written the terminal error packet.
#[must_use = "the governed error must be settled by its protocol owner"]
pub struct GovernedErrorStatementResult {
    error: QueryServiceError,
    protocol: GovernedProtocolOwner,
}

impl GovernedErrorStatementResult {
    pub(crate) fn new(
        error: QueryServiceError,
        resources: LocalResourceAuthority,
        statement: GovernedQueryStatementOwner,
    ) -> Self {
        Self {
            error,
            protocol: GovernedProtocolOwner::new(statement, resources),
        }
    }

    pub(crate) fn into_parts(self) -> (QueryServiceError, GovernedProtocolOwner) {
        (self.error, self.protocol)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use novarocks_workload_control::{ResourceConfig, WorkloadConfig, WorkloadControl};

    use super::*;
    use novarocks_query_application::client_connection::ClientConnectionToken;
    use novarocks_query_application::query_control::QueryApplicationControl;
    use novarocks_query_application::session_control::{
        GovernedStatementFinishOutcome, GovernedStatementVisibilitySealOutcome, QueryControlPort,
        QueryControlService, QuerySessionLease, SessionIdentity,
    };

    fn immediate_fixture() -> (
        GovernedImmediateStatementResult,
        WorkloadControl,
        QuerySessionLease,
    ) {
        let control = QueryControlService::new(
            Arc::new(QueryApplicationControl::default()) as Arc<dyn QueryControlPort>
        );
        let session = control
            .register_session(SessionIdentity::new(
                ClientConnectionToken::new(301, 1).expect("connection token"),
                "root",
            ))
            .expect("register session");
        let workload = WorkloadControl::try_new(
            WorkloadConfig::default(),
            ResourceConfig {
                total_bytes: 1024,
                control_bytes: 128,
                per_scope_bytes: 896,
            },
        )
        .expect("workload control");
        workload.mark_ready().expect("workload ready");
        let mut statement = control
            .begin_governed_query_statement(session.token(), &workload.root_admission(), None, None)
            .expect("governed statement");
        statement
            .take_execution_owner()
            .expect("execution owner")
            .complete();
        let result = GovernedImmediateStatementResult::new(
            QueryResult::empty(),
            workload.resources(),
            statement,
        );
        (result, workload, session)
    }

    #[test]
    fn governed_immediate_retains_business_through_success_visibility_seal() {
        let (result, workload, _session) = immediate_fixture();
        assert_eq!(workload.snapshot().businesses, 1);

        let (_, mut protocol) = result.into_parts();
        assert_eq!(
            protocol.seal_success_visibility(),
            GovernedStatementVisibilitySealOutcome::Sealed
        );
        assert_eq!(workload.snapshot().businesses, 1);
        assert_eq!(
            protocol.complete(),
            GovernedStatementFinishOutcome::Completed
        );
        assert_eq!(workload.snapshot().businesses, 0);
    }

    #[test]
    fn governed_immediate_eof_failure_after_seal_is_protocol_failed() {
        let (result, workload, _session) = immediate_fixture();
        let (_, mut protocol) = result.into_parts();
        assert_eq!(
            protocol.seal_success_visibility(),
            GovernedStatementVisibilitySealOutcome::Sealed
        );
        assert_eq!(
            protocol.fail(),
            GovernedStatementFinishOutcome::ProtocolFailed
        );
        assert_eq!(workload.snapshot().businesses, 0);
    }

    #[test]
    fn governed_completion_retains_business_until_terminal_ok() {
        let control = QueryControlService::new(
            Arc::new(QueryApplicationControl::default()) as Arc<dyn QueryControlPort>
        );
        let session = control
            .register_session(SessionIdentity::new(
                ClientConnectionToken::new(303, 1).expect("connection token"),
                "root",
            ))
            .expect("register session");
        let workload = WorkloadControl::try_new(
            WorkloadConfig::default(),
            ResourceConfig {
                total_bytes: 1024,
                control_bytes: 128,
                per_scope_bytes: 896,
            },
        )
        .expect("workload control");
        workload.mark_ready().expect("workload ready");
        let mut statement = control
            .begin_governed_query_statement(session.token(), &workload.root_admission(), None, None)
            .expect("governed statement");
        statement.complete_execution();
        let result = GovernedCompletionStatementResult::new(workload.resources(), statement);

        assert_eq!(workload.snapshot().businesses, 1);
        let mut protocol = result.into_protocol();
        assert_eq!(
            protocol.seal_success_visibility(),
            GovernedStatementVisibilitySealOutcome::Sealed
        );
        assert_eq!(
            protocol.complete(),
            GovernedStatementFinishOutcome::Completed
        );
        assert_eq!(workload.snapshot().businesses, 0);
    }

    #[test]
    fn governed_error_retains_business_until_terminal_error() {
        let control = QueryControlService::new(
            Arc::new(QueryApplicationControl::default()) as Arc<dyn QueryControlPort>
        );
        let session = control
            .register_session(SessionIdentity::new(
                ClientConnectionToken::new(304, 1).expect("connection token"),
                "root",
            ))
            .expect("register session");
        let workload = WorkloadControl::try_new(
            WorkloadConfig::default(),
            ResourceConfig {
                total_bytes: 1024,
                control_bytes: 128,
                per_scope_bytes: 896,
            },
        )
        .expect("workload control");
        workload.mark_ready().expect("workload ready");
        let mut statement = control
            .begin_governed_query_statement(session.token(), &workload.root_admission(), None, None)
            .expect("governed statement");
        statement.complete_execution();
        let result = GovernedErrorStatementResult::new(
            QueryServiceError::new(
                novarocks_query_application::session_error::QueryServiceErrorKind::Internal,
                "failed command",
            ),
            workload.resources(),
            statement,
        );

        assert_eq!(workload.snapshot().businesses, 1);
        let (_, mut protocol) = result.into_parts();
        assert_eq!(
            protocol.fail(),
            GovernedStatementFinishOutcome::ProtocolFailed
        );
        assert_eq!(workload.snapshot().businesses, 0);
    }

    #[tokio::test]
    async fn governed_protocol_cancellation_preserves_configured_timeout() {
        let control = QueryControlService::new(
            Arc::new(QueryApplicationControl::default()) as Arc<dyn QueryControlPort>
        );
        let session = control
            .register_session(SessionIdentity::new(
                ClientConnectionToken::new(302, 1).expect("connection token"),
                "root",
            ))
            .expect("register session");
        let workload = WorkloadControl::try_new(
            WorkloadConfig::default(),
            ResourceConfig {
                total_bytes: 1024,
                control_bytes: 128,
                per_scope_bytes: 896,
            },
        )
        .expect("workload control");
        workload.mark_ready().expect("workload ready");
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_millis(1);
        let statement = control
            .begin_governed_query_statement(
                session.token(),
                &workload.root_admission(),
                Some(deadline),
                Some(73),
            )
            .expect("governed statement");
        let protocol = GovernedProtocolOwner::new(statement, workload.resources());

        assert_eq!(
            protocol.cancellation().cancelled().await,
            novarocks_query_application::cancellation::QueryCancellationReason::DeadlineExceeded {
                timeout_ms: 73,
            }
        );
    }
}
