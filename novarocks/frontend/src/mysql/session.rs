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
//! The Frontend MySQL server owns protocol framing. Authentication success opens a
//! frontend session through this port; all request admission, routing and
//! cancellation identity remain with that session.

use std::sync::Arc;

use async_trait::async_trait;

use crate::runtime::statement_result::StatementResult;
use novarocks_query_application::cancellation::QueryCancellationReason;
use novarocks_query_application::client_connection::ClientConnectionToken;
use novarocks_query_application::session_error::QueryServiceError;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct QuerySessionOpenRequest {
    connection: ClientConnectionToken,
    principal: Arc<str>,
}

impl QuerySessionOpenRequest {
    pub fn new(connection: ClientConnectionToken, principal: impl Into<Arc<str>>) -> Self {
        Self {
            connection,
            principal: principal.into(),
        }
    }

    pub const fn connection_id(&self) -> u32 {
        self.connection.connection_id()
    }

    pub const fn connection_token(&self) -> ClientConnectionToken {
        self.connection
    }

    pub fn principal(&self) -> &str {
        &self.principal
    }
}

#[async_trait]
pub trait QuerySession: Send + Sync + 'static {
    async fn init_database(&self, schema: &str) -> Result<(), QueryServiceError>;

    async fn execute_batch(&self, sql: &str) -> Result<StatementResult, QueryServiceError>;

    /// Settles the protocol-owned statement terminal after its final wire
    /// outcome. Every adapter implementation must make this ownership
    /// explicit; there is no safe default settlement.
    fn complete_statement(&self);

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
        let request = QuerySessionOpenRequest::new(
            ClientConnectionToken::new(42, 7).expect("valid connection token"),
            "alice",
        );
        assert_eq!(request.connection_id(), 42);
        assert_eq!(request.connection_token().generation(), 7);
        assert_eq!(request.principal(), "alice");
    }
}
