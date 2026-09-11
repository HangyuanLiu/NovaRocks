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

use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;

use crate::common::query_cancellation::QueryCancellationReason;
use crate::runtime::statement_result::StatementResult;
use novarocks_query_application::client_connection::ClientConnectionToken;
use novarocks_spi::connector::LakePublicationTerminal;
use novarocks_user_error::UserError;

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
    Unavailable,
    /// The FE-local serving lifecycle has irreversibly closed workload admission.
    FrontendDraining,
    Internal,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct QueryServiceError {
    kind: QueryServiceErrorKind,
    message: String,
    user_error: Option<UserError>,
    publication_terminal: Option<LakePublicationTerminal>,
}

impl QueryServiceError {
    pub const FRONTEND_DRAINING_MESSAGE: &'static str =
        "FRONTEND_DRAINING: frontend is draining; retry on another frontend";

    pub fn frontend_draining() -> Self {
        Self::new(
            QueryServiceErrorKind::FrontendDraining,
            Self::FRONTEND_DRAINING_MESSAGE,
        )
    }

    pub fn new(kind: QueryServiceErrorKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
            user_error: None,
            publication_terminal: None,
        }
    }

    /// Retains parser-owned facts until MySQL encodes the broad error class.
    pub fn from_user_error(error: UserError) -> Self {
        Self {
            kind: QueryServiceErrorKind::Parse,
            message: error.to_string(),
            user_error: Some(error),
            publication_terminal: None,
        }
    }

    pub fn with_publication_terminal(
        message: impl Into<String>,
        terminal: LakePublicationTerminal,
    ) -> Self {
        Self {
            kind: QueryServiceErrorKind::Internal,
            message: message.into(),
            user_error: None,
            publication_terminal: Some(terminal),
        }
    }

    pub const fn kind(&self) -> QueryServiceErrorKind {
        self.kind
    }

    pub fn message(&self) -> &str {
        &self.message
    }

    pub fn user_error(&self) -> Option<&UserError> {
        self.user_error.as_ref()
    }

    pub fn publication_terminal(&self) -> Option<&LakePublicationTerminal> {
        self.publication_terminal.as_ref()
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

    #[test]
    fn typed_error_preserves_kind_and_message() {
        let error = QueryServiceError::new(QueryServiceErrorKind::Timeout, "deadline elapsed");
        assert_eq!(error.kind(), QueryServiceErrorKind::Timeout);
        assert_eq!(error.message(), "deadline elapsed");
        assert!(error.user_error().is_none());
    }

    #[test]
    fn frontend_draining_error_uses_the_fixed_retry_message() {
        let error = QueryServiceError::frontend_draining();
        assert_eq!(error.kind(), QueryServiceErrorKind::FrontendDraining);
        assert_eq!(
            error.message(),
            "FRONTEND_DRAINING: frontend is draining; retry on another frontend"
        );
    }

    #[test]
    fn parser_user_error_is_preserved_without_message_classification() {
        let parser_error = novarocks_parser::parse("SHOW")
            .expect_err("incomplete SHOW command must be a parser error")
            .to_user_error("SHOW");
        let error = QueryServiceError::from_user_error(parser_error.clone());

        assert_eq!(error.kind(), QueryServiceErrorKind::Parse);
        assert_eq!(error.user_error(), Some(&parser_error));
        assert_eq!(error.message(), parser_error.to_string());
    }
}
