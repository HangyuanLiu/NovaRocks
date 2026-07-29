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

//! Frontend-owned SQL session boundary consumed by the MySQL wire adapter.
//! Design: ADR-0012 (docs/adr/ADR-0012-frontend-query-session-router.md)
//!
//! The core server owns protocol framing only.  Authentication success opens a
//! frontend session through this port; all request admission, routing and
//! cancellation identity remain with that session.

use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;

use crate::engine::StatementResult;
use crate::query_execution::cancellation::QueryCancellationReason;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct QuerySessionOpenRequest {
    connection_id: u32,
    principal: Arc<str>,
}

impl QuerySessionOpenRequest {
    pub fn new(connection_id: u32, principal: impl Into<Arc<str>>) -> Self {
        Self {
            connection_id,
            principal: principal.into(),
        }
    }

    pub const fn connection_id(&self) -> u32 {
        self.connection_id
    }

    pub fn principal(&self) -> &str {
        &self.principal
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum QueryServiceErrorKind {
    Parse,
    BadDatabase,
    Unsupported,
    PermissionDenied,
    NoSuchSession,
    Interrupted,
    Timeout,
    InvalidValue,
    Internal,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct QueryServiceError {
    kind: QueryServiceErrorKind,
    message: String,
}

impl QueryServiceError {
    pub fn new(kind: QueryServiceErrorKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
        }
    }

    pub const fn kind(&self) -> QueryServiceErrorKind {
        self.kind
    }

    pub fn message(&self) -> &str {
        &self.message
    }
}

impl fmt::Display for QueryServiceError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl std::error::Error for QueryServiceError {}

#[async_trait]
pub trait QuerySession: Send + Sync + 'static {
    async fn init_database(&self, schema: &str) -> Result<(), QueryServiceError>;

    async fn execute_batch(&self, sql: &str) -> Result<StatementResult, QueryServiceError>;

    fn cancel_current(&self, reason: QueryCancellationReason);

    fn close(&self);
}

pub trait QuerySessionFactory: Send + Sync + 'static {
    fn open_session(
        &self,
        request: QuerySessionOpenRequest,
    ) -> Result<Arc<dyn QuerySession>, QueryServiceError>;

    fn cancel_all(&self, reason: QueryCancellationReason);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn open_request_keeps_connection_identity_private_but_readable() {
        let request = QuerySessionOpenRequest::new(42, "alice");
        assert_eq!(request.connection_id(), 42);
        assert_eq!(request.principal(), "alice");
    }

    #[test]
    fn typed_error_preserves_kind_and_message() {
        let error = QueryServiceError::new(QueryServiceErrorKind::Timeout, "deadline elapsed");
        assert_eq!(error.kind(), QueryServiceErrorKind::Timeout);
        assert_eq!(error.message(), "deadline elapsed");
    }
}
