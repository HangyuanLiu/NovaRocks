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

use std::future::Future;
use std::io;
use std::net::SocketAddr;
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use async_trait::async_trait;
use opensrv_mysql::{
    AsyncMysqlIntermediary, AsyncMysqlShim, CapabilityFlags, ErrorKind, InitWriter, ParamParser,
    QueryResultWriter, StatementMetaWriter,
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
use novarocks_query_application::sql::admission::negotiated_query_statements;

use crate::connection_registry::{MysqlClientConnectionRegistration, MysqlConnectionClass};
use crate::{ClientDisconnectWatcher, MysqlClientConnectionRegistry, spawn_disconnect_watcher};

async fn write_negotiated_statement<'writer, W: AsyncWrite + Unpin>(
    statement: StatementResult,
    results: QueryResultWriter<'writer, W>,
    more_results: bool,
) -> io::Result<crate::MysqlStatementWriteOutcome<'writer, W>> {
    match statement {
        StatementResult::Query(_) => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "MySQL query result has no original governed owner",
        )),
        StatementResult::GovernedQuery(result) => {
            crate::local_result_writer::write_local_result_one(result, results, more_results).await
        }
        StatementResult::StreamingQuery(result) => {
            crate::governed_result_writer::write_streaming_query_result_with_more(
                result,
                results,
                more_results,
            )
            .await
        }
        StatementResult::GovernedCompletion(result) => {
            crate::terminal::write_governed_terminal_ok_with_more(
                result.into_protocol(),
                results,
                more_results,
            )
            .await
        }
        StatementResult::GovernedError(result) => {
            let (error, protocol) = result.into_parts();
            crate::write_governed_terminal_error(error, protocol, results)
                .await
                .map(|_| crate::MysqlStatementWriteOutcome::Terminated)
        }
        StatementResult::Ok => crate::write_terminal_ok_one(results)
            .await
            .map(crate::MysqlStatementWriteOutcome::Continue),
    }
}

/// Default upper bound for draining protocol tasks during an immediate
/// application shutdown.
pub const QUERY_APPLICATION_MYSQL_SESSION_DRAIN_TIMEOUT: Duration = Duration::from_secs(5);

/// Runs a ready Query Application session factory until shutdown.
///
/// The adapter owns protocol-task draining; the caller supplies the already
/// composed session factory and the role-specific readiness action.
pub async fn serve_query_application_mysql_until_shutdown<F, R>(
    settings: crate::ResolvedMysqlListenerSettings,
    server_version: String,
    session_factory: Arc<dyn QuerySessionFactory>,
    connections: Arc<MysqlClientConnectionRegistry>,
    shutdown: F,
    on_ready: R,
) -> Result<(), String>
where
    F: Future<Output = ()> + Send,
    R: FnOnce(SocketAddr),
{
    let shutdown_factory = Arc::clone(&session_factory);
    let shutdown_connections = Arc::clone(&connections);
    serve_query_application_mysql_until_drain_then_shutdown(
        settings,
        server_version,
        session_factory,
        connections,
        async move {
            shutdown.await;
        },
        async move {
            shutdown_factory.cancel_all(QueryCancellationReason::ServerShutdown);
            shutdown_connections.terminate_all(ClientConnectionTerminationReason::ServerShutdown);
        },
        QUERY_APPLICATION_MYSQL_SESSION_DRAIN_TIMEOUT,
        on_ready,
    )
    .await
}

/// Stops accepting new sockets at `drain`, runs role-owned finalization, then
/// drains established Query Application protocol tasks.
pub async fn serve_query_application_mysql_until_drain_then_shutdown<F, G, R>(
    settings: crate::ResolvedMysqlListenerSettings,
    server_version: String,
    session_factory: Arc<dyn QuerySessionFactory>,
    connections: Arc<MysqlClientConnectionRegistry>,
    drain: F,
    finalize: G,
    cleanup_timeout: Duration,
    on_ready: R,
) -> Result<(), String>
where
    F: Future<Output = ()> + Send,
    G: Future<Output = ()> + Send,
    R: FnOnce(SocketAddr),
{
    serve_query_application_mysql_kernel(
        settings,
        server_version,
        session_factory,
        connections,
        drain,
        finalize,
        cleanup_timeout,
        on_ready,
        #[cfg(feature = "mem-1-m07-exact-mysql-write")]
        None,
    )
    .await
}

/// Opt-in binding stays inside the adapter; it carries no public Hub capability.
#[cfg(feature = "mem-1-m07-exact-mysql-write")]
pub async fn serve_query_application_mysql_until_drain_then_shutdown_fixture<F, G, R>(
    settings: crate::ResolvedMysqlListenerSettings,
    server_version: String,
    session_factory: Arc<dyn QuerySessionFactory>,
    connections: Arc<MysqlClientConnectionRegistry>,
    drain: F,
    finalize: G,
    cleanup_timeout: Duration,
    on_ready: R,
    binding: crate::exact_mysql_write_fixture::MysqlWriteFixtureListenerBinding,
) -> Result<(), String>
where
    F: Future<Output = ()> + Send,
    G: Future<Output = ()> + Send,
    R: FnOnce(SocketAddr),
{
    serve_query_application_mysql_kernel(
        settings,
        server_version,
        session_factory,
        connections,
        drain,
        finalize,
        cleanup_timeout,
        on_ready,
        Some(binding),
    )
    .await
}

async fn serve_query_application_mysql_kernel<F, G, R>(
    settings: crate::ResolvedMysqlListenerSettings,
    server_version: String,
    session_factory: Arc<dyn QuerySessionFactory>,
    connections: Arc<MysqlClientConnectionRegistry>,
    drain: F,
    finalize: G,
    cleanup_timeout: Duration,
    on_ready: R,
    #[cfg(feature = "mem-1-m07-exact-mysql-write")] binding: Option<
        crate::exact_mysql_write_fixture::MysqlWriteFixtureListenerBinding,
    >,
) -> Result<(), String>
where
    F: Future<Output = ()> + Send,
    G: Future<Output = ()> + Send,
    R: FnOnce(SocketAddr),
{
    let (bind_addr, session_user) = settings.into_parts();
    let drain_registry = Arc::clone(&connections);
    #[cfg(feature = "mem-1-m07-exact-mysql-write")]
    let (fixture_hub, fixture_joins) = match binding {
        Some(binding) => (Some(binding.hub), Some(binding.joins)),
        None => (None, None),
    };
    #[cfg(feature = "mem-1-m07-exact-mysql-write")]
    let fixture_watchers = fixture_joins.clone();
    let handler = move |stream, peer_addr| {
        // Acquire a finite position before creating the task, watcher or
        // intermediary and its protocol buffers. Full admission closes IO.
        let registration = connections.register().ok()?;
        #[cfg(feature = "mem-1-m07-exact-mysql-write")]
        let watcher_permit = match &fixture_watchers {
            Some(joins) => Some(joins.reserve_watcher(registration.retain_owner()).ok()?),
            None => None,
        };
        Some(serve_registered_mysql_connection(
            session_user.clone(),
            server_version.clone(),
            Arc::clone(&session_factory),
            registration,
            stream,
            peer_addr,
            #[cfg(feature = "mem-1-m07-exact-mysql-write")]
            fixture_hub.clone(),
            #[cfg(feature = "mem-1-m07-exact-mysql-write")]
            watcher_permit,
            #[cfg(feature = "mem-1-m07-exact-mysql-write")]
            fixture_watchers.clone(),
        ))
    };
    #[cfg(feature = "mem-1-m07-exact-mysql-write")]
    let serve_result = match fixture_joins {
        Some(joins) => {
            crate::listener::serve_tcp_until_drain_then_shutdown_admitted_observed(
                bind_addr,
                drain,
                finalize,
                handler,
                on_ready,
                cleanup_timeout,
                joins,
            )
            .await
        }
        None => {
            crate::listener::serve_tcp_until_drain_then_shutdown_admitted(
                bind_addr,
                drain,
                finalize,
                handler,
                on_ready,
                cleanup_timeout,
            )
            .await
        }
    };
    #[cfg(not(feature = "mem-1-m07-exact-mysql-write"))]
    let serve_result = crate::listener::serve_tcp_until_drain_then_shutdown_admitted(
        bind_addr,
        drain,
        finalize,
        handler,
        on_ready,
        cleanup_timeout,
    )
    .await;
    tokio::time::timeout(cleanup_timeout, drain_registry.wait_drained())
        .await
        .map_err(|_| {
            "MySQL connection owners did not drain before the cleanup deadline".to_string()
        })?;
    serve_result
}

pub async fn serve_query_application_mysql_connection(
    user: String,
    server_version: String,
    session_factory: Arc<dyn QuerySessionFactory>,
    connections: Arc<MysqlClientConnectionRegistry>,
    stream: TcpStream,
    peer_addr: SocketAddr,
) {
    let registration = match connections.register() {
        Ok(registration) => registration,
        Err(error) => {
            warn!(
                "reject standalone mysql connection because the connection registry is exhausted: peer={}, error={:?}",
                peer_addr, error
            );
            return;
        }
    };
    serve_registered_mysql_connection(
        user,
        server_version,
        session_factory,
        registration,
        stream,
        peer_addr,
        #[cfg(feature = "mem-1-m07-exact-mysql-write")]
        None,
        #[cfg(feature = "mem-1-m07-exact-mysql-write")]
        None,
        #[cfg(feature = "mem-1-m07-exact-mysql-write")]
        None,
    )
    .await;
}

#[cfg(feature = "mem-1-m07-exact-mysql-write")]
enum FixtureMysqlWriter {
    Raw(tokio::net::tcp::OwnedWriteHalf),
    Gated(
        crate::mysql_write_gate::late_binding::InitiallyRawMysqlWriter<
            tokio::net::tcp::OwnedWriteHalf,
        >,
    ),
}
#[cfg(feature = "mem-1-m07-exact-mysql-write")]
impl AsyncWrite for FixtureMysqlWriter {
    fn poll_write(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        bytes: &[u8],
    ) -> std::task::Poll<io::Result<usize>> {
        match self.get_mut() {
            Self::Raw(io) => std::pin::Pin::new(io).poll_write(cx, bytes),
            Self::Gated(io) => std::pin::Pin::new(io).poll_write(cx, bytes),
        }
    }
    fn poll_write_vectored(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        bytes: &[io::IoSlice<'_>],
    ) -> std::task::Poll<io::Result<usize>> {
        match self.get_mut() {
            Self::Raw(io) => std::pin::Pin::new(io).poll_write_vectored(cx, bytes),
            Self::Gated(io) => std::pin::Pin::new(io).poll_write_vectored(cx, bytes),
        }
    }
    fn is_write_vectored(&self) -> bool {
        match self {
            Self::Raw(io) => io.is_write_vectored(),
            Self::Gated(io) => io.is_write_vectored(),
        }
    }
    fn poll_flush(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<io::Result<()>> {
        match self.get_mut() {
            Self::Raw(io) => std::pin::Pin::new(io).poll_flush(cx),
            Self::Gated(io) => std::pin::Pin::new(io).poll_flush(cx),
        }
    }
    fn poll_shutdown(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<io::Result<()>> {
        match self.get_mut() {
            Self::Raw(io) => std::pin::Pin::new(io).poll_shutdown(cx),
            Self::Gated(io) => std::pin::Pin::new(io).poll_shutdown(cx),
        }
    }
}

async fn serve_registered_mysql_connection(
    user: String,
    server_version: String,
    session_factory: Arc<dyn QuerySessionFactory>,
    mut registration: MysqlClientConnectionRegistration,
    stream: TcpStream,
    peer_addr: SocketAddr,
    #[cfg(feature = "mem-1-m07-exact-mysql-write")] fixture_hub: Option<
        Arc<crate::mysql_write_gate::late_binding::MysqlWriteGateHub>,
    >,
    #[cfg(feature = "mem-1-m07-exact-mysql-write")] watcher_permit: Option<
        crate::listener::WatcherPermit,
    >,
    #[cfg(feature = "mem-1-m07-exact-mysql-write")] fixture_protocol: Option<
        Arc<crate::listener::MysqlFixtureSessionJoins>,
    >,
) {
    let connection = registration.token();
    let session: Arc<OnceLock<Arc<dyn QuerySession>>> = Arc::new(OnceLock::new());
    let session_for_disconnect = Arc::clone(&session);
    let watcher_owner = registration.retain_owner();
    let disconnect_watcher = spawn_disconnect_watcher(&stream, move || {
        let _owner = &watcher_owner;
        if let Some(session) = session_for_disconnect.get() {
            session.cancel_current(QueryCancellationReason::ClientDisconnected);
        }
    });
    #[cfg(feature = "mem-1-m07-exact-mysql-write")]
    let disconnect_watcher = match watcher_permit {
        Some(permit) => permit.attach(disconnect_watcher),
        None => disconnect_watcher,
    };
    let shim = QueryApplicationMysqlShim::new(
        user,
        connection,
        session_factory,
        Arc::clone(&session),
        disconnect_watcher,
        server_version,
    )
    .with_connection_class(registration.class());
    #[cfg(feature = "mem-1-m07-exact-mysql-write")]
    let shim = shim.with_fixture_hub(if registration.class() == MysqlConnectionClass::Ordinary {
        fixture_hub.clone()
    } else {
        None
    });
    let (reader, writer) = stream.into_split();
    #[cfg(feature = "mem-1-m07-exact-mysql-write")]
    let writer = match (registration.class(), fixture_hub) {
        (MysqlConnectionClass::Ordinary, Some(hub)) => FixtureMysqlWriter::Gated(
            crate::mysql_write_gate::late_binding::InitiallyRawMysqlWriter::new(
                writer, connection, hub,
            ),
        ),
        _ => FixtureMysqlWriter::Raw(writer),
    };
    let result = {
        let mut limits = opensrv_mysql::ProtocolLimits::default();
        if registration.class() == MysqlConnectionClass::Control {
            limits.command_bytes = limits.diagnostic_bytes;
        }
        let intermediary = AsyncMysqlIntermediary::run_with_input_deadlines(
            shim,
            reader,
            writer,
            &crate::MYSQL_INTERMEDIARY_OPTIONS,
            limits,
            tokio::time::Instant::from_std(registration.admitted_at()) + Duration::from_secs(10),
            Duration::from_secs(10),
            Duration::from_secs(30),
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
        #[cfg(feature = "mem-1-m07-exact-mysql-write")]
        if let Some(observation) = fixture_protocol {
            observation.observe_protocol_failure(connection, registration.class(), err);
        }
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
    connection_class: MysqlConnectionClass,
    #[cfg(feature = "mem-1-m07-exact-mysql-write")]
    fixture_hub: Option<Arc<crate::mysql_write_gate::late_binding::MysqlWriteGateHub>>,
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
            connection_class: MysqlConnectionClass::Ordinary,
            #[cfg(feature = "mem-1-m07-exact-mysql-write")]
            fixture_hub: None,
        }
    }

    fn with_connection_class(mut self, class: MysqlConnectionClass) -> Self {
        self.connection_class = class;
        self
    }

    #[cfg(feature = "mem-1-m07-exact-mysql-write")]
    fn with_fixture_hub(
        mut self,
        hub: Option<Arc<crate::mysql_write_gate::late_binding::MysqlWriteGateHub>>,
    ) -> Self {
        self.fixture_hub = hub;
        self
    }

    #[cfg(feature = "mem-1-m07-exact-mysql-write")]
    fn reject_fixture_non_streaming(&self, sql_sha256: [u8; 32]) -> io::Result<()> {
        if let Some(hub) = &self.fixture_hub {
            hub.reject_non_streaming_target(self.connection, sql_sha256)?;
        }
        Ok(())
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

    fn permits_query_shortcuts(&self) -> bool {
        #[cfg(feature = "mem-1-m07-exact-mysql-write")]
        if self
            .fixture_hub
            .as_ref()
            .is_some_and(|hub| hub.is_selected_connection(self.connection))
        {
            return false;
        }
        self.connection_class == MysqlConnectionClass::Ordinary
    }

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
        if self.connection_class == MysqlConnectionClass::Control {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "control connection only accepts KILL",
            ));
        }
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
        if self.connection_class == MysqlConnectionClass::Control {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "control connection only accepts KILL",
            ));
        }
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
        if self.connection_class == MysqlConnectionClass::Control {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "control connection only accepts KILL",
            ));
        }
        let session = match self.session() {
            Ok(session) => session,
            Err(error) => {
                return writer
                    .error(crate::mysql_error_kind(&error), error.message().as_bytes())
                    .await;
            }
        };
        let (statement, terminal) = match session
            .init_database(&crate::normalize_init_database_schema(schema))
            .await
        {
            Ok(statement) => statement.into_parts(),
            Err(error) => {
                writer
                    .error(crate::mysql_error_kind(&error), error.message().as_bytes())
                    .await?;
                return Ok(());
            }
        };
        let outcome = match statement {
            StatementResult::Ok => writer.ok().await,
            StatementResult::GovernedCompletion(result) => {
                crate::write_governed_init_ok(result.into_protocol(), writer).await
            }
            StatementResult::GovernedError(result) => {
                let (error, protocol) = result.into_parts();
                crate::write_governed_init_error(error, protocol, writer).await
            }
            StatementResult::Query(_)
            | StatementResult::GovernedQuery(_)
            | StatementResult::StreamingQuery(_) => {
                writer
                    .error(
                        ErrorKind::ER_UNKNOWN_ERROR,
                        b"COM_INIT_DB returned a non-terminal query result",
                    )
                    .await
            }
        };
        terminal.complete();
        outcome
    }

    async fn on_query<'a>(
        &'a mut self,
        query: &'a str,
        results: QueryResultWriter<'a, W>,
    ) -> io::Result<()> {
        #[cfg(feature = "mem-1-m07-exact-mysql-write")]
        let fixture_sql_sha256: [u8; 32] = {
            use sha2::{Digest, Sha256};
            Sha256::digest(query.as_bytes()).into()
        };
        #[cfg(feature = "mem-1-m07-exact-mysql-write")]
        if let Some(hub) = &self.fixture_hub {
            if hub.is_selected_connection(self.connection)
                && negotiated_query_statements(query).is_ok_and(|statements| statements.len() > 1)
            {
                hub.reject_unsupported_batch(self.connection)?;
            }
        }
        if self.connection_class == MysqlConnectionClass::Control {
            if let Err(error) =
                novarocks_query_application::sql::admission::admit_control_connection_batch(query)
            {
                return results
                    .reject_connection(crate::mysql_error_kind(&error), error.message().as_bytes())
                    .await;
            }
        }
        let session = match self.session() {
            Ok(session) => session,
            Err(error) => {
                #[cfg(feature = "mem-1-m07-exact-mysql-write")]
                self.reject_fixture_non_streaming(fixture_sql_sha256)?;
                return results
                    .error(crate::mysql_error_kind(&error), error.message().as_bytes())
                    .await;
            }
        };
        let capabilities = results.client_capabilities();
        let multi_results_negotiated = capabilities
            .contains(CapabilityFlags::CLIENT_MULTI_STATEMENTS)
            && capabilities.contains(CapabilityFlags::CLIENT_MULTI_RESULTS);
        let statements = if multi_results_negotiated {
            match negotiated_query_statements(query) {
                Ok(statements) => statements,
                Err(error) => {
                    #[cfg(feature = "mem-1-m07-exact-mysql-write")]
                    self.reject_fixture_non_streaming(fixture_sql_sha256)?;
                    return results
                        .error(crate::mysql_error_kind(&error), error.message().as_bytes())
                        .await;
                }
            }
        } else {
            Vec::new()
        };
        if statements.len() > 1 {
            #[cfg(feature = "mem-1-m07-exact-mysql-write")]
            if let Some(hub) = &self.fixture_hub {
                hub.reject_unsupported_batch(self.connection)?;
            }
            let mut results = results;
            let count = statements.len();
            for (index, statement_sql) in statements.into_iter().enumerate() {
                let statement = match session.execute_statement(statement_sql).await {
                    Ok(statement) => statement,
                    Err(error) => {
                        return results
                            .error(crate::mysql_error_kind(&error), error.message().as_bytes())
                            .await;
                    }
                };
                let (statement, terminal) = statement.into_parts();
                let outcome =
                    write_negotiated_statement(statement, results, index + 1 < count).await;
                terminal.complete();
                match outcome? {
                    crate::MysqlStatementWriteOutcome::Continue(next) => results = next,
                    crate::MysqlStatementWriteOutcome::Terminated => return Ok(()),
                }
            }
            return results.no_more_results().await;
        }
        let (statement, terminal) = match session.execute_batch(query).await {
            Ok(statement) => statement.into_parts(),
            Err(error) => {
                #[cfg(feature = "mem-1-m07-exact-mysql-write")]
                self.reject_fixture_non_streaming(fixture_sql_sha256)?;
                return results
                    .error(crate::mysql_error_kind(&error), error.message().as_bytes())
                    .await;
            }
        };
        #[cfg(feature = "mem-1-m07-exact-mysql-write")]
        if !matches!(&statement, StatementResult::StreamingQuery(_)) {
            if let Err(error) = self.reject_fixture_non_streaming(fixture_sql_sha256) {
                drop(statement);
                terminal.complete();
                return Err(error);
            }
        }
        let outcome = match statement {
            StatementResult::Query(_) => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "MySQL query result has no original governed owner",
            )),
            StatementResult::GovernedQuery(result) => {
                crate::write_governed_query_result(result, results).await
            }
            StatementResult::StreamingQuery(result) => {
                #[cfg(feature = "mem-1-m07-exact-mysql-write")]
                {
                    // Keep the original terminal completion below on every fixture error.
                    async {
                        if let Some(hub) = &self.fixture_hub {
                            let token = result.statement_token().ok_or_else(|| {
                                hub.fail_selected(self.connection, crate::mysql_write_gate::GateFailure::Identity);
                                io::Error::new(io::ErrorKind::InvalidData, "fixture streaming result has no original statement token")
                            })?;
                            let hook = hub.bind_statement(self.connection, token, fixture_sql_sha256)?;
                            let outcome = crate::governed_result_writer::write_streaming_query_result_with_gate(
                                result, results, hook,
                            ).await;
                            if outcome.as_ref().is_err_and(|error| {
                                !crate::mysql_write_gate::late_binding::PrescribedRelayEof::from_error(error)
                                    .is_some_and(|eof| eof.matches(self.connection, Some(token)))
                            }) {
                                // A bounded fixture summary keeps original scope first cause;
                                // the original typed IO error is still returned unchanged.
                                hub.fail_selected(self.connection, crate::mysql_write_gate::GateFailure::Transition);
                            }
                            outcome
                        } else {
                            crate::write_streaming_query_result(result, results).await
                        }
                    }.await
                }
                #[cfg(not(feature = "mem-1-m07-exact-mysql-write"))]
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

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicBool, Ordering};

    use super::*;

    struct RawResultFactory;
    struct RawResultSession;
    impl QuerySessionFactory for RawResultFactory {
        fn open_session(
            &self,
            _: QuerySessionOpenRequest,
        ) -> Result<Arc<dyn QuerySession>, QueryServiceError> {
            Ok(Arc::new(RawResultSession))
        }
        fn cancel_all(&self, _: QueryCancellationReason) {}
    }
    #[async_trait::async_trait]
    impl QuerySession for RawResultSession {
        async fn init_database(
            &self,
            _: &str,
        ) -> Result<novarocks_query_application::session::QuerySessionStatement, QueryServiceError>
        {
            Ok(
                novarocks_query_application::session::QuerySessionStatement::output_owned(
                    StatementResult::Ok,
                ),
            )
        }
        async fn execute_statement(
            &self,
            _: &str,
        ) -> Result<novarocks_query_application::session::QuerySessionStatement, QueryServiceError>
        {
            Ok(
                novarocks_query_application::session::QuerySessionStatement::output_owned(
                    StatementResult::Query(
                        novarocks_query_application::api::build_string_query_result(
                            "raw",
                            vec!["must not be published".to_string()],
                        )
                        .unwrap(),
                    ),
                ),
            )
        }
        async fn execute_batch(
            &self,
            sql: &str,
        ) -> Result<novarocks_query_application::session::QuerySessionStatement, QueryServiceError>
        {
            self.execute_statement(sql).await
        }
        fn cancel_current(&self, _: QueryCancellationReason) {}
        fn close(&self) {}
    }

    #[tokio::test]
    async fn raw_result_without_original_owner_is_refused_before_metadata_in_both_protocol_modes() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::net::{TcpListener, TcpStream};
        async fn send(stream: &mut TcpStream, sequence: u8, body: &[u8]) {
            let mut header = (body.len() as u32).to_le_bytes();
            header[3] = sequence;
            stream.write_all(&header).await.unwrap();
            stream.write_all(body).await.unwrap();
        }
        async fn read(stream: &mut TcpStream) -> Vec<u8> {
            let mut header = [0; 4];
            stream.read_exact(&mut header).await.unwrap();
            header[3] = 0;
            let length = u32::from_le_bytes(header) as usize;
            assert!(length < 4096);
            let mut body = vec![0; length];
            stream.read_exact(&mut body).await.unwrap();
            body
        }
        tokio::time::timeout(Duration::from_secs(5), async {
            for negotiated in [false, true] {
                let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
                let address = listener.local_addr().unwrap();
                let server = tokio::spawn(async move {
                    let (stream, _) = listener.accept().await.unwrap();
                    let (read, write) = stream.into_split();
                    opensrv_mysql::AsyncMysqlIntermediary::run_with_options(
                        QueryApplicationMysqlShim::new(
                            "root".into(),
                            ClientConnectionToken::new(91, 1).unwrap(),
                            Arc::new(RawResultFactory),
                            Arc::new(OnceLock::new()),
                            ClientDisconnectWatcher::inactive(),
                            "test".into(),
                        ),
                        read,
                        write,
                        &crate::MYSQL_INTERMEDIARY_OPTIONS,
                    )
                    .await
                });
                let mut client = TcpStream::connect(address).await.unwrap();
                assert_eq!(read(&mut client).await[0], 10);
                let mut flags = CapabilityFlags::CLIENT_PROTOCOL_41
                    | CapabilityFlags::CLIENT_SECURE_CONNECTION
                    | CapabilityFlags::CLIENT_PLUGIN_AUTH;
                if negotiated {
                    flags |= CapabilityFlags::CLIENT_MULTI_STATEMENTS
                        | CapabilityFlags::CLIENT_MULTI_RESULTS;
                }
                let mut auth = Vec::new();
                auth.extend_from_slice(&flags.bits().to_le_bytes());
                auth.extend_from_slice(&(64_u32 * 1024 * 1024).to_le_bytes());
                auth.push(33);
                auth.extend_from_slice(&[0; 23]);
                auth.extend_from_slice(b"root\0");
                auth.push(0);
                auth.extend_from_slice(b"mysql_native_password\0");
                send(&mut client, 1, &auth).await;
                assert_eq!(read(&mut client).await[0], 0);
                send(&mut client, 0, b"\x03SELECT raw_result").await;
                let mut first = [0; 1];
                assert_eq!(
                    client.read(&mut first).await.unwrap(),
                    0,
                    "raw schema/data must never reach the socket"
                );
                let error = server.await.unwrap().unwrap_err();
                assert_eq!(error.kind(), io::ErrorKind::InvalidData);
                assert!(error.to_string().contains("original governed owner"));
            }
        })
        .await
        .unwrap();
    }

    struct CancellationProbeFactory {
        cancelled: Arc<AtomicBool>,
    }

    impl QuerySessionFactory for CancellationProbeFactory {
        fn open_session(
            &self,
            _request: QuerySessionOpenRequest,
        ) -> Result<Arc<dyn QuerySession>, QueryServiceError> {
            Err(QueryServiceError::new(
                QueryServiceErrorKind::Internal,
                "test session factory must not open a session",
            ))
        }

        fn cancel_all(&self, _reason: QueryCancellationReason) {
            self.cancelled.store(true, Ordering::SeqCst);
        }
    }

    fn rejecting_shim() -> QueryApplicationMysqlShim {
        QueryApplicationMysqlShim::new(
            "root".to_string(),
            ClientConnectionToken::new(1, 1).expect("valid connection token"),
            Arc::new(CancellationProbeFactory {
                cancelled: Arc::new(AtomicBool::new(false)),
            }),
            Arc::new(OnceLock::new()),
            ClientDisconnectWatcher::inactive(),
            "test".to_string(),
        )
    }

    #[cfg(feature = "mem-1-m07-exact-mysql-write")]
    #[test]
    fn armed_fixture_target_reaches_query_dispatch_instead_of_shortcut() {
        type Shim = QueryApplicationMysqlShim;
        for target in [1, 2] {
            let (hub, mut controller) =
                crate::mysql_write_gate::late_binding::MysqlWriteGateHub::new(
                    novarocks_types::FrontendProcessId::new_v7(),
                    [1; 16],
                    std::time::Instant::now() + Duration::from_secs(3),
                )
                .unwrap();
            let shim = rejecting_shim().with_fixture_hub(Some(hub));
            assert!(<Shim as AsyncMysqlShim<Vec<u8>>>::permits_query_shortcuts(
                &shim
            ));
            controller
                .arm(controller.snapshot().frontend, [1; 16], target, [2; 32], 1)
                .unwrap();
            assert_eq!(
                <Shim as AsyncMysqlShim<Vec<u8>>>::permits_query_shortcuts(&shim),
                target != 1
            );
            let control = shim.with_connection_class(MysqlConnectionClass::Control);
            assert!(!<Shim as AsyncMysqlShim<Vec<u8>>>::permits_query_shortcuts(
                &control
            ));
            controller.stop();
        }
    }

    #[cfg(feature = "mem-1-m07-exact-mysql-write")]
    #[tokio::test]
    async fn fixture_original_registered_intermediary_retains_actual_io_after_socket_exit() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let connections = MysqlClientConnectionRegistry::new();
        let registration = connections.register().unwrap();
        let actual_token = registration.token();
        let observation = Arc::new(crate::listener::MysqlFixtureSessionJoins::default());
        let permit = observation
            .reserve_watcher(registration.retain_owner())
            .unwrap();
        let server = async {
            let (stream, peer) = listener.accept().await.unwrap();
            serve_registered_mysql_connection(
                "root".into(),
                "test".into(),
                Arc::new(RawResultFactory),
                registration,
                stream,
                peer,
                None,
                Some(permit),
                Some(observation.clone()),
            )
            .await;
            observation.abort_remaining_watchers();
            while observation.next_watcher().await.is_some() {}
            connections.wait_drained().await;
        };
        let client = async {
            let mut stream = tokio::net::TcpStream::connect(address).await.unwrap();
            async fn read_packet(stream: &mut TcpStream) -> Vec<u8> {
                let mut header = [0; 4];
                stream.read_exact(&mut header).await.unwrap();
                header[3] = 0;
                let len = u32::from_le_bytes(header) as usize;
                assert!(len < 4096);
                let mut body = vec![0; len];
                stream.read_exact(&mut body).await.unwrap();
                body
            }
            assert_eq!(read_packet(&mut stream).await[0], 10);
            let flags = CapabilityFlags::CLIENT_PROTOCOL_41
                | CapabilityFlags::CLIENT_SECURE_CONNECTION
                | CapabilityFlags::CLIENT_PLUGIN_AUTH;
            let mut auth = Vec::new();
            auth.extend_from_slice(&flags.bits().to_le_bytes());
            auth.extend_from_slice(&(64_u32 * 1024 * 1024).to_le_bytes());
            auth.push(33);
            auth.extend_from_slice(&[0; 23]);
            auth.extend_from_slice(b"root\0");
            auth.push(0);
            auth.extend_from_slice(b"mysql_native_password\0");
            let mut header = (auth.len() as u32).to_le_bytes();
            header[3] = 1;
            stream.write_all(&header).await.unwrap();
            stream.write_all(&auth).await.unwrap();
            assert_eq!(read_packet(&mut stream).await[0], 0);
            let query = b"\x03SELECT raw_result";
            let mut header = (query.len() as u32).to_le_bytes();
            header[3] = 0;
            stream.write_all(&header).await.unwrap();
            stream.write_all(query).await.unwrap();
            assert_eq!(stream.read(&mut [0; 1]).await.unwrap(), 0);
        };
        tokio::time::timeout(Duration::from_secs(2), async {
            tokio::join!(server, client);
        })
        .await
        .unwrap();
        let actual = observation.take_protocol_failure_after_join().unwrap();
        assert_eq!(actual.connection, actual_token);
        assert_eq!(actual.class, MysqlConnectionClass::Ordinary);
        assert_eq!(actual.cause.kind(), io::ErrorKind::InvalidData);
        assert!(actual.cause.to_string().contains("original governed owner"));
        assert_eq!(observation.snapshot().protocol_io_failures, 1);
        assert_eq!(observation.watcher_snapshot().joined, 1);
        assert!(observation.watchers_empty());
    }

    async fn authenticate(
        shim: &QueryApplicationMysqlShim,
        auth_plugin: &str,
        user: &[u8],
        auth: &[u8],
    ) -> bool {
        AsyncMysqlShim::<tokio::io::Sink>::authenticate(
            shim,
            auth_plugin,
            user,
            b"0123456789abcdefghij",
            auth,
        )
        .await
    }

    #[test]
    fn reserved_control_connections_disable_protocol_query_shortcuts() {
        let ordinary = rejecting_shim();
        assert!(AsyncMysqlShim::<tokio::io::Sink>::permits_query_shortcuts(
            &ordinary
        ));
        let control = ordinary.with_connection_class(MysqlConnectionClass::Control);
        assert!(!AsyncMysqlShim::<tokio::io::Sink>::permits_query_shortcuts(
            &control
        ));
    }

    #[tokio::test]
    async fn adapter_rejects_unauthorized_handshakes_before_session_open() {
        let shim = rejecting_shim();

        assert!(!authenticate(&shim, "mysql_native_password", b"other", b"").await);
        assert!(!authenticate(&shim, "mysql_native_password", b"ROOT", b"").await);
        assert!(!authenticate(&shim, "mysql_native_password", b"root", b"secret").await);
        assert!(!authenticate(&shim, "caching_sha2_password", b"root", b"").await);
    }

    #[tokio::test]
    async fn immediate_protocol_shutdown_cancels_the_ready_session_factory() {
        let cancelled = Arc::new(AtomicBool::new(false));
        let factory: Arc<dyn QuerySessionFactory> = Arc::new(CancellationProbeFactory {
            cancelled: Arc::clone(&cancelled),
        });
        let settings = crate::ResolvedMysqlListenerSettings::new(
            SocketAddr::from(([127, 0, 0, 1], 0)),
            "root",
        );

        serve_query_application_mysql_until_shutdown(
            settings,
            "test".to_string(),
            factory,
            Arc::new(MysqlClientConnectionRegistry::new()),
            async {},
            |_| {},
        )
        .await
        .expect("ready protocol server should shut down cleanly");

        assert!(cancelled.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn immediate_protocol_shutdown_notifies_registered_connections() {
        let cancelled = Arc::new(AtomicBool::new(false));
        let factory: Arc<dyn QuerySessionFactory> = Arc::new(CancellationProbeFactory {
            cancelled: Arc::clone(&cancelled),
        });
        let connections = Arc::new(MysqlClientConnectionRegistry::new());
        let mut registration = connections
            .register()
            .expect("register protocol connection");
        let settings = crate::ResolvedMysqlListenerSettings::new(
            SocketAddr::from(([127, 0, 0, 1], 0)),
            "root",
        );

        let server = serve_query_application_mysql_until_shutdown(
            settings,
            "test".to_string(),
            factory,
            Arc::clone(&connections),
            async {},
            |_| {},
        );
        let (result, reason) = tokio::join!(server, async move {
            let reason = registration
                .termination_receiver()
                .await
                .expect("shutdown signal");
            // The protocol task actually exits before the registry is drained.
            drop(registration);
            reason
        });
        result.expect("ready protocol server should shut down cleanly");
        assert!(cancelled.load(Ordering::SeqCst));
        assert_eq!(reason, ClientConnectionTerminationReason::ServerShutdown);
    }
}

#[cfg(all(test, feature = "mem-1-m07-exact-mysql-write"))]
#[path = "query_application_shim/exact_eof_tests.rs"]
mod exact_eof_tests;
