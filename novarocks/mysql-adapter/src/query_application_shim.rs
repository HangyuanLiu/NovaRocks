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

//! MySQL protocol dispatch over Query Application session contracts.

use std::io;
use std::net::SocketAddr;
use std::sync::{Arc, OnceLock};

use async_trait::async_trait;
use opensrv_mysql::{
    AsyncMysqlIntermediary, AsyncMysqlShim, ErrorKind, InitWriter, ParamParser, QueryResultWriter,
    StatementMetaWriter,
};
use tokio::io::AsyncWrite;
use tokio::net::TcpStream;
use tracing::{info, warn};

use novarocks_query_application::cancellation::QueryCancellationReason;
use novarocks_query_application::client_connection::{
    ClientConnectionTerminationReason, ClientConnectionToken,
};
use novarocks_query_application::protocol_delivery::QuerySessionOutput as StatementResult;
use novarocks_query_application::session::{
    QuerySession, QuerySessionFactory, QuerySessionOpenRequest,
};
use novarocks_query_application::session_error::{QueryServiceError, QueryServiceErrorKind};

use crate::{ClientDisconnectWatcher, MysqlClientConnectionRegistry, spawn_disconnect_watcher};

pub async fn serve_query_application_mysql_connection(
    user: String,
    server_version: String,
    session_factory: Arc<dyn QuerySessionFactory>,
    connections: Arc<MysqlClientConnectionRegistry>,
    stream: TcpStream,
    peer_addr: SocketAddr,
) {
    let mut registration = match connections.register() {
        Ok(registration) => registration,
        Err(error) => {
            warn!(
                "reject standalone mysql connection because the connection registry is exhausted: peer={}, error={:?}",
                peer_addr, error
            );
            return;
        }
    };
    let connection = registration.token();
    let session: Arc<OnceLock<Arc<dyn QuerySession>>> = Arc::new(OnceLock::new());
    let session_for_disconnect = Arc::clone(&session);
    let disconnect_watcher = spawn_disconnect_watcher(&stream, move || {
        if let Some(session) = session_for_disconnect.get() {
            session.cancel_current(QueryCancellationReason::ClientDisconnected);
        }
    });
    let shim = QueryApplicationMysqlShim::new(
        user,
        connection,
        session_factory,
        Arc::clone(&session),
        disconnect_watcher,
        server_version,
    );
    let (reader, writer) = stream.into_split();
    let result = {
        let intermediary = AsyncMysqlIntermediary::run_with_options(
            shim,
            reader,
            writer,
            &crate::MYSQL_INTERMEDIARY_OPTIONS,
        );
        tokio::pin!(intermediary);
        tokio::select! {
            termination = registration.termination_receiver() => {
                match termination {
                    Ok(reason) => {
                        if let Some(session) = session.get() {
                            session.cancel_current(query_cancellation_reason_for_connection_termination(&reason));
                        }
                        info!(
                            "terminate standalone mysql connection: peer={}, connection_id={}, reason={:?}",
                            peer_addr,
                            connection.connection_id(),
                            reason
                        );
                    }
                    Err(error) => {
                        warn!(
                            "standalone mysql connection termination signal closed unexpectedly: peer={}, connection_id={}, error={}",
                            peer_addr,
                            connection.connection_id(),
                            error
                        );
                    }
                }
                None
            }
            result = &mut intermediary => Some(result),
        }
    };
    if let Some(Err(err)) = result {
        warn!(
            "standalone mysql connection failed: peer={}, connection_id={}, err={}",
            peer_addr,
            connection.connection_id(),
            err
        );
    }
}

fn query_cancellation_reason_for_connection_termination(
    reason: &ClientConnectionTerminationReason,
) -> QueryCancellationReason {
    match reason {
        ClientConnectionTerminationReason::ExplicitKillConnection {
            requester_connection_id,
        } => QueryCancellationReason::ExplicitKillConnection {
            requester_connection_id: *requester_connection_id,
        },
        ClientConnectionTerminationReason::ServerShutdown => {
            QueryCancellationReason::ServerShutdown
        }
    }
}

pub struct QueryApplicationMysqlShim {
    user: String,
    connection: ClientConnectionToken,
    session_factory: Arc<dyn QuerySessionFactory>,
    session: Arc<OnceLock<Arc<dyn QuerySession>>>,
    _disconnect_watcher: ClientDisconnectWatcher,
    server_version: String,
}

impl QueryApplicationMysqlShim {
    pub fn new(
        user: String,
        connection: ClientConnectionToken,
        session_factory: Arc<dyn QuerySessionFactory>,
        session: Arc<OnceLock<Arc<dyn QuerySession>>>,
        disconnect_watcher: ClientDisconnectWatcher,
        server_version: String,
    ) -> Self {
        Self {
            user,
            connection,
            session_factory,
            session,
            _disconnect_watcher: disconnect_watcher,
            server_version,
        }
    }

    fn session(&self) -> Result<&Arc<dyn QuerySession>, QueryServiceError> {
        self.session.get().ok_or_else(|| {
            QueryServiceError::new(
                QueryServiceErrorKind::PermissionDenied,
                "session is not authenticated",
            )
        })
    }
}

impl Drop for QueryApplicationMysqlShim {
    fn drop(&mut self) {
        if let Some(session) = self.session.get() {
            session.close();
        }
    }
}

#[async_trait]
impl<W: AsyncWrite + Send + Unpin> AsyncMysqlShim<W> for QueryApplicationMysqlShim {
    type Error = io::Error;

    fn version(&self) -> String {
        format!("{}-standalone-mysql", self.server_version)
    }

    fn connect_id(&self) -> u32 {
        self.connection.connection_id()
    }

    async fn authenticate(
        &self,
        auth_plugin: &str,
        username: &[u8],
        salt: &[u8],
        auth_data: &[u8],
    ) -> bool {
        if !crate::authenticate_empty_password(&self.user, auth_plugin, username, salt, auth_data) {
            return false;
        }
        let session = match self
            .session_factory
            .open_session(QuerySessionOpenRequest::new(
                self.connection,
                self.user.clone(),
            )) {
            Ok(session) => session,
            Err(error) => {
                warn!(
                    "failed to open frontend query session for connection_id={}: {}",
                    self.connection.connection_id(),
                    error
                );
                return false;
            }
        };
        self.session.set(session).is_ok()
    }

    async fn on_prepare<'a>(
        &'a mut self,
        _query: &'a str,
        info: StatementMetaWriter<'a, W>,
    ) -> io::Result<()> {
        info.error(
            ErrorKind::ER_NOT_SUPPORTED_YET,
            b"prepared statements are not supported in standalone server v1",
        )
        .await
    }

    async fn on_execute<'a>(
        &'a mut self,
        _id: u32,
        _params: ParamParser<'a>,
        results: QueryResultWriter<'a, W>,
    ) -> io::Result<()> {
        results
            .error(
                ErrorKind::ER_NOT_SUPPORTED_YET,
                b"prepared statements are not supported in standalone server v1",
            )
            .await
    }

    async fn on_close<'a>(&'a mut self, _stmt: u32) {}

    async fn on_init<'a>(
        &'a mut self,
        schema: &'a str,
        writer: InitWriter<'a, W>,
    ) -> io::Result<()> {
        let session = match self.session() {
            Ok(session) => session,
            Err(error) => {
                return writer
                    .error(crate::mysql_error_kind(&error), error.message().as_bytes())
                    .await;
            }
        };
        match session
            .init_database(&crate::normalize_init_database_schema(schema))
            .await
        {
            Ok(()) => writer.ok().await,
            Err(error) => {
                writer
                    .error(crate::mysql_error_kind(&error), error.message().as_bytes())
                    .await
            }
        }
    }

    async fn on_query<'a>(
        &'a mut self,
        query: &'a str,
        results: QueryResultWriter<'a, W>,
    ) -> io::Result<()> {
        let session = match self.session() {
            Ok(session) => session,
            Err(error) => {
                return results
                    .error(crate::mysql_error_kind(&error), error.message().as_bytes())
                    .await;
            }
        };
        let (statement, terminal) = match session.execute_batch(query).await {
            Ok(statement) => statement.into_parts(),
            Err(error) => {
                return results
                    .error(crate::mysql_error_kind(&error), error.message().as_bytes())
                    .await;
            }
        };
        let outcome = match statement {
            StatementResult::Query(result) => crate::write_query_result(result, results).await,
            StatementResult::GovernedQuery(result) => {
                crate::write_governed_query_result(result, results).await
            }
            StatementResult::StreamingQuery(result) => {
                crate::write_streaming_query_result(result, results).await
            }
            StatementResult::GovernedCompletion(result) => {
                crate::write_governed_terminal_ok(result.into_protocol(), results).await
            }
            StatementResult::GovernedError(result) => {
                let (error, protocol) = result.into_parts();
                crate::write_governed_terminal_error(error, protocol, results).await
            }
            StatementResult::Ok => crate::write_terminal_ok(results).await,
        };
        terminal.complete();
        outcome
    }
}
