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

//! Session-local deadline and terminal-error interpretation.

use std::time::{Duration, Instant};

use crate::api::{QueryExecutionError, QueryExecutionErrorKind};
use crate::cancellation::QueryCancellationReason;
use crate::session_control::GovernedStatementFinishOutcome;
use crate::session_error::{QueryServiceError, QueryServiceErrorKind};
use crate::sql::session::SessionSqlState;
use novarocks_workload_control::CancellationReason as WorkCancellationReason;

pub fn governed_query_deadline(
    state: &SessionSqlState,
) -> Result<(Option<Instant>, Option<u64>), QueryServiceError> {
    let timeout_secs = state.execution_settings().query_timeout_secs();
    let deadline = match timeout_secs {
        Some(seconds) => Some(
            Instant::now()
                .checked_add(Duration::from_secs(seconds))
                .ok_or_else(|| internal_error("query deadline exceeds monotonic clock range"))?,
        ),
        None => None,
    };
    Ok((
        deadline,
        timeout_secs.map(|seconds| seconds.saturating_mul(1_000)),
    ))
}

pub fn cancellation_error(reason: QueryCancellationReason) -> QueryServiceError {
    let (kind, message) = match reason {
        QueryCancellationReason::ExecutionCancellationRequested => (
            QueryServiceErrorKind::Interrupted,
            "Query execution cancellation was requested".to_string(),
        ),
        QueryCancellationReason::ExecutionOwnerDropped => (
            QueryServiceErrorKind::Interrupted,
            "Query execution protocol owner was dropped".to_string(),
        ),
        QueryCancellationReason::DeadlineExceeded { timeout_ms } => (
            QueryServiceErrorKind::Timeout,
            format!("query timed out after {timeout_ms} ms"),
        ),
        QueryCancellationReason::FrontendDrainDeadlineExceeded { timeout_ms } => (
            QueryServiceErrorKind::Interrupted,
            format!(
                "FRONTEND_DRAIN_DEADLINE_EXCEEDED: frontend drain deadline exceeded after {timeout_ms} ms"
            ),
        ),
        QueryCancellationReason::ExplicitKill { .. } => (
            QueryServiceErrorKind::Interrupted,
            "Query execution was interrupted".to_string(),
        ),
        QueryCancellationReason::ExplicitKillConnection { .. } => (
            QueryServiceErrorKind::Interrupted,
            "Query execution was interrupted because the connection was killed".to_string(),
        ),
        QueryCancellationReason::ClientDisconnected => (
            QueryServiceErrorKind::Interrupted,
            "Query execution was interrupted because the client disconnected".to_string(),
        ),
        QueryCancellationReason::ServerShutdown => (
            QueryServiceErrorKind::Interrupted,
            "Query execution was interrupted because the server is shutting down".to_string(),
        ),
    };
    QueryServiceError::new(kind, message)
}

pub fn governed_execution_error(
    error: QueryExecutionError,
    completion: GovernedStatementFinishOutcome,
) -> QueryServiceError {
    match completion {
        GovernedStatementFinishOutcome::Cancelled(reason) => governed_cancellation_error(reason),
        GovernedStatementFinishOutcome::Completed | GovernedStatementFinishOutcome::Stale => {
            governed_query_execution_error(error)
        }
        GovernedStatementFinishOutcome::ProtocolFailed => internal_error(format!(
            "query protocol ownership failed while handling execution error: {error}"
        )),
    }
}

pub fn governed_query_execution_error(error: QueryExecutionError) -> QueryServiceError {
    let kind = match error.kind() {
        QueryExecutionErrorKind::Cancelled => QueryServiceErrorKind::Interrupted,
        QueryExecutionErrorKind::DeadlineExceeded => QueryServiceErrorKind::Timeout,
        QueryExecutionErrorKind::Rejected => QueryServiceErrorKind::Unavailable,
        QueryExecutionErrorKind::InvalidRequest | QueryExecutionErrorKind::Failed => {
            QueryServiceErrorKind::Internal
        }
    };
    QueryServiceError::new(kind, error.to_string())
}

pub fn governed_statement_begin_error(
    error: crate::session_control::GovernedQueryStatementBeginError,
) -> QueryServiceError {
    QueryServiceError::new(
        QueryServiceErrorKind::Unavailable,
        format!("begin governed query statement failed: {error}"),
    )
}

pub fn scalar_query_error(message: impl Into<String>) -> QueryServiceError {
    QueryServiceError::new(QueryServiceErrorKind::InvalidValue, message.into())
}

pub fn governed_cancellation_error(reason: WorkCancellationReason) -> QueryServiceError {
    let (kind, message) = match reason {
        WorkCancellationReason::DeadlineExceeded => (
            QueryServiceErrorKind::Timeout,
            "query deadline exceeded".to_string(),
        ),
        WorkCancellationReason::FrontendDrainDeadlineExceeded => (
            QueryServiceErrorKind::Interrupted,
            "FRONTEND_DRAIN_DEADLINE_EXCEEDED: frontend drain deadline exceeded".to_string(),
        ),
        WorkCancellationReason::ExplicitKill { .. } => (
            QueryServiceErrorKind::Interrupted,
            "Query execution was interrupted".to_string(),
        ),
        WorkCancellationReason::ExplicitKillConnection { .. } => (
            QueryServiceErrorKind::Interrupted,
            "Query execution was interrupted because the connection was killed".to_string(),
        ),
        WorkCancellationReason::ClientDisconnected => (
            QueryServiceErrorKind::Interrupted,
            "Query execution was interrupted because the client disconnected".to_string(),
        ),
        WorkCancellationReason::ServerShutdown => (
            QueryServiceErrorKind::Interrupted,
            "Query execution was interrupted because the server is shutting down".to_string(),
        ),
        WorkCancellationReason::Requested => (
            QueryServiceErrorKind::Interrupted,
            "Query execution cancellation was requested".to_string(),
        ),
        WorkCancellationReason::OwnerDropped => (
            QueryServiceErrorKind::Interrupted,
            "Query execution protocol owner was dropped".to_string(),
        ),
    };
    QueryServiceError::new(kind, message)
}

pub fn cancellation_requires_statement_fence(reason: &QueryCancellationReason) -> bool {
    matches!(reason, QueryCancellationReason::ExplicitKill { .. })
}

fn internal_error(message: impl Into<String>) -> QueryServiceError {
    QueryServiceError::new(QueryServiceErrorKind::Internal, message)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cancellation_errors_keep_timeout_distinct_from_interrupts() {
        assert_eq!(
            cancellation_error(QueryCancellationReason::DeadlineExceeded { timeout_ms: 7 }).kind(),
            QueryServiceErrorKind::Timeout
        );
        assert_eq!(
            cancellation_error(QueryCancellationReason::ClientDisconnected).kind(),
            QueryServiceErrorKind::Interrupted
        );
    }

    #[test]
    fn only_kill_query_fences_the_successor_statement() {
        assert!(cancellation_requires_statement_fence(
            &QueryCancellationReason::ExplicitKill {
                requester_connection_id: 8
            }
        ));
        assert!(!cancellation_requires_statement_fence(
            &QueryCancellationReason::ClientDisconnected
        ));
    }
}
