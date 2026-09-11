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

pub use novarocks_mysql_adapter::{
    MysqlClientConnectionRegistry, ResolvedMysqlListenerSettings, resolve_mysql_listener_settings,
};

use std::future::Future;
use std::net::SocketAddr;
use std::sync::Arc;
#[cfg(test)]
use std::sync::OnceLock;
#[cfg(test)]
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

#[cfg(test)]
use opensrv_mysql::{AsyncMysqlShim, ErrorKind};
use tracing::info;

use novarocks_version as version;

use novarocks_query_application::cancellation::QueryCancellationReason;
use novarocks_query_application::client_connection::ClientConnectionTerminationReason;
#[cfg(test)]
use novarocks_query_application::client_connection::ClientConnectionToken;
use novarocks_query_application::session::QuerySessionFactory;
#[cfg(test)]
use novarocks_query_application::session::{QuerySession, QuerySessionOpenRequest};
#[cfg(test)]
use novarocks_query_application::session_error::{QueryServiceError, QueryServiceErrorKind};
use novarocks_types::naming::DEFAULT_DATABASE;

#[cfg(test)]
const ROOT_USER: &str = novarocks_mysql_adapter::DEFAULT_MYSQL_USER;
const SESSION_DRAIN_TIMEOUT: Duration = Duration::from_secs(5);

/// Runs the MySQL protocol listener with a ready frontend-owned session
/// factory.
///
/// The listener preserves the public ready marker and the shutdown drain
/// contract.  On shutdown it first asks the session factory to cancel all
/// sessions, stops accepting new connections, then waits for active protocol
/// tasks to drain (or aborts them after the bounded drain timeout).
// Design: ADR-0102 (docs/adr/ADR-0102-mysql-kill-connection-lifecycle-ownership.md)
// Design: ADR-0121 (docs/adr/ADR-0121-frontend-serving-lifecycle-and-admission-drain.md)
pub async fn run_mysql_server_until_shutdown<F>(
    settings: ResolvedMysqlListenerSettings,
    session_factory: Arc<dyn QuerySessionFactory>,
    connections: Arc<MysqlClientConnectionRegistry>,
    shutdown: F,
) -> Result<(), String>
where
    F: Future<Output = ()> + Send,
{
    let shutdown_factory = Arc::clone(&session_factory);
    let shutdown_connections = Arc::clone(&connections);
    run_mysql_server_until_drain_then_shutdown(
        settings,
        session_factory,
        connections,
        async move {
            shutdown.await;
        },
        async move {
            shutdown_factory.cancel_all(QueryCancellationReason::ServerShutdown);
            shutdown_connections.terminate_all(ClientConnectionTerminationReason::ServerShutdown);
        },
        SESSION_DRAIN_TIMEOUT,
    )
    .await
}

/// Stops accepting new sockets at `drain`, but retains already accepted
/// protocol tasks until the FE lifecycle owner performs final teardown.
///
/// This is deliberately separate from `run_mysql_server_until_shutdown`: an
/// idle MySQL session is not an admitted workload and must not decide the FE
/// drain deadline, while an admitted statement needs its socket and result
/// path until it completes or the deadline cancellation wins.
pub async fn run_mysql_server_until_drain_then_shutdown<F, G>(
    settings: ResolvedMysqlListenerSettings,
    session_factory: Arc<dyn QuerySessionFactory>,
    connections: Arc<MysqlClientConnectionRegistry>,
    drain: F,
    finalize: G,
    cleanup_timeout: Duration,
) -> Result<(), String>
where
    F: Future<Output = ()> + Send,
    G: Future<Output = ()> + Send,
{
    let (bind_addr, session_user) = settings.into_parts();
    let ready_user = session_user.clone();
    novarocks_mysql_adapter::serve_tcp_until_drain_then_shutdown(
        bind_addr,
        drain,
        finalize,
        move |stream, peer_addr| {
            novarocks_mysql_adapter::serve_query_application_mysql_connection(
                session_user.clone(),
                version::short_version().to_string(),
                Arc::clone(&session_factory),
                Arc::clone(&connections),
                stream,
                peer_addr,
            )
        },
        move |bound_addr| emit_standalone_ready(bound_addr, &ready_user),
        cleanup_timeout,
    )
    .await
}

fn emit_standalone_ready(bind_addr: SocketAddr, user: &str) {
    info!(
        "standalone mysql server listening on {} (user={}, db={})",
        bind_addr, user, DEFAULT_DATABASE
    );
    // Emit a parser-friendly readiness marker on stdout. Orchestration
    // scripts must wait for this exact line before connecting; probing the
    // mysql port alone cannot distinguish a freshly-bound server from a
    // pre-existing process that already owned the port. The keyword
    // `NOVAROCKS_READY` is the wait-for-ready contract — do not change it
    // without updating callers (CLAUDE.md, SQL test harness, etc.).
    println!(
        "NOVAROCKS_READY mysql_port={} pid={}",
        bind_addr.port(),
        std::process::id()
    );
}

#[cfg(test)]
#[test]
fn query_service_error_mapping_is_owned_by_the_mysql_adapter() {
    assert_eq!(
        novarocks_mysql_adapter::error_kind_for_query_service_error(
            QueryServiceErrorKind::BadDatabase
        ),
        ErrorKind::ER_BAD_DB_ERROR
    );
    assert_eq!(
        novarocks_mysql_adapter::error_kind_for_query_service_error(
            QueryServiceErrorKind::Interrupted
        ),
        ErrorKind::ER_QUERY_INTERRUPTED
    );
    assert_eq!(
        novarocks_mysql_adapter::error_kind_for_query_service_error(
            QueryServiceErrorKind::Unavailable
        ),
        ErrorKind::ER_UNKNOWN_ERROR
    );
    assert_eq!(
        novarocks_mysql_adapter::error_kind_for_query_service_error(
            QueryServiceErrorKind::FrontendDraining
        ),
        ErrorKind::ER_SERVER_SHUTDOWN
    );
    assert_eq!(ErrorKind::ER_SERVER_SHUTDOWN.sqlstate(), b"08S01");
}

#[cfg(test)]
#[test]
fn user_error_code_overrides_the_legacy_session_error_kind() {
    use novarocks_user_error::{ErrorCodeDescriptor, ErrorCodeStatus, ErrorPhase, RetryClass};

    let user_error = novarocks_user_error::UserError::from_descriptor(
        ErrorCodeDescriptor {
            code: novarocks_user_error::ErrorCodeId::new("sql.analyze.unknown_table"),
            phase: ErrorPhase::Analyze,
            status: ErrorCodeStatus::Active,
        },
        "unknown table",
        None,
        RetryClass::Never,
    );
    let error = QueryServiceError::from_user_error(user_error);
    assert_eq!(
        novarocks_mysql_adapter::mysql_error_kind(&error),
        ErrorKind::ER_NO_SUCH_TABLE
    );
}

#[cfg(test)]
#[test]
fn every_active_manifest_descriptor_has_exactly_one_adapter_wire_mapping() {
    use std::collections::BTreeSet;

    use novarocks_parser::ERROR_CODE_DESCRIPTORS as PARSER_ERROR_CODE_DESCRIPTORS;
    use novarocks_sql::analyze_error::ERROR_CODE_DESCRIPTORS as ANALYZE_ERROR_CODE_DESCRIPTORS;
    use novarocks_user_error::ErrorCodeStatus;

    let descriptor_codes = PARSER_ERROR_CODE_DESCRIPTORS
        .iter()
        .chain(ANALYZE_ERROR_CODE_DESCRIPTORS)
        .chain(crate::DML_ERROR_CODE_DESCRIPTORS)
        .chain(crate::SESSION_ERROR_CODE_DESCRIPTORS)
        .filter(|descriptor| descriptor.status == ErrorCodeStatus::Active)
        .map(|descriptor| descriptor.code.as_str())
        .collect::<BTreeSet<_>>();
    assert_eq!(descriptor_codes.len(), 29);
    for code in descriptor_codes {
        assert!(
            novarocks_mysql_adapter::error_kind_for_domain_code(code).is_some(),
            "active descriptor `{code}` must have one MySQL wire mapping"
        );
    }
}

#[cfg(test)]
mod protocol_api_tests {
    use super::*;

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

    #[tokio::test]
    async fn ready_session_factory_api_cancels_sessions_before_listener_drain() {
        let cancelled = Arc::new(AtomicBool::new(false));
        let factory: Arc<dyn QuerySessionFactory> = Arc::new(CancellationProbeFactory {
            cancelled: Arc::clone(&cancelled),
        });
        let settings =
            ResolvedMysqlListenerSettings::new(SocketAddr::from(([127, 0, 0, 1], 0)), ROOT_USER);

        run_mysql_server_until_shutdown(
            settings,
            factory,
            Arc::new(MysqlClientConnectionRegistry::new()),
            async {},
        )
        .await
        .expect("ready protocol server should shut down cleanly");

        assert!(cancelled.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn shutdown_broadcasts_the_same_protocol_connection_registry() {
        let cancelled = Arc::new(AtomicBool::new(false));
        let factory: Arc<dyn QuerySessionFactory> = Arc::new(CancellationProbeFactory {
            cancelled: Arc::clone(&cancelled),
        });
        let connections = Arc::new(MysqlClientConnectionRegistry::new());
        let mut registration = connections
            .register()
            .expect("register protocol connection");
        let settings =
            ResolvedMysqlListenerSettings::new(SocketAddr::from(([127, 0, 0, 1], 0)), ROOT_USER);

        run_mysql_server_until_shutdown(settings, factory, Arc::clone(&connections), async {})
            .await
            .expect("ready protocol server should shut down cleanly");

        assert!(cancelled.load(Ordering::SeqCst));
        assert_eq!(
            registration
                .termination_receiver()
                .try_recv()
                .expect("shutdown must reach the registered connection"),
            ClientConnectionTerminationReason::ServerShutdown
        );
    }

    /// A shim whose session factory must never be reached. Every assertion below
    /// is a rejection, and rejection happens strictly before a session is opened.
    fn rejecting_shim() -> novarocks_mysql_adapter::QueryApplicationMysqlShim {
        novarocks_mysql_adapter::QueryApplicationMysqlShim::new(
            ROOT_USER.to_string(),
            ClientConnectionToken::new(1, 1).expect("valid connection token"),
            Arc::new(CancellationProbeFactory {
                cancelled: Arc::new(AtomicBool::new(false)),
            }),
            Arc::new(OnceLock::new()),
            novarocks_mysql_adapter::ClientDisconnectWatcher::inactive(),
            "test".to_string(),
        )
    }

    async fn authenticate(
        shim: &novarocks_mysql_adapter::QueryApplicationMysqlShim,
        auth_plugin: &str,
        username: &[u8],
        auth_data: &[u8],
    ) -> bool {
        AsyncMysqlShim::<tokio::io::Sink>::authenticate(
            shim,
            auth_plugin,
            username,
            b"0123456789abcdefghij",
            auth_data,
        )
        .await
    }

    #[tokio::test]
    async fn authenticate_rejects_other_users() {
        let shim = rejecting_shim();

        assert!(
            !authenticate(&shim, "mysql_native_password", b"other", b"").await,
            "only the configured user may authenticate"
        );
        assert!(
            !authenticate(&shim, "mysql_native_password", b"", b"").await,
            "an empty user name must not authenticate"
        );
        assert!(
            !authenticate(&shim, "mysql_native_password", b"ROOT", b"").await,
            "the user name comparison is exact, not case-insensitive"
        );
    }

    #[tokio::test]
    async fn authenticate_rejects_non_empty_credentials() {
        let shim = rejecting_shim();

        assert!(
            !authenticate(
                &shim,
                "mysql_native_password",
                ROOT_USER.as_bytes(),
                b"secret"
            )
            .await,
            "a non-empty scramble must not authenticate against the empty password"
        );
    }

    #[tokio::test]
    async fn authenticate_rejects_other_auth_plugins() {
        let shim = rejecting_shim();

        assert!(
            !authenticate(&shim, "caching_sha2_password", ROOT_USER.as_bytes(), b"").await,
            "only mysql_native_password is supported"
        );
        assert!(
            !authenticate(&shim, "", ROOT_USER.as_bytes(), b"").await,
            "an absent auth plugin must not authenticate"
        );
    }
}

#[cfg(test)]
mod tests {
    use std::future::pending;
    use std::sync::Mutex;

    use tokio::net::{TcpListener, TcpStream};
    use tokio::sync::oneshot;

    use super::*;

    #[test]
    fn com_init_db_normalizes_quoted_qualified_schema() {
        assert_eq!(
            novarocks_mysql_adapter::normalize_init_database_schema("`iceberg_cat`.`ssb`"),
            "iceberg_cat.ssb"
        );
        assert_eq!(
            novarocks_mysql_adapter::normalize_init_database_schema("iceberg_cat.ssb"),
            "iceberg_cat.ssb"
        );
    }

    mod shutdown_lifecycle {
        use super::*;
        use std::net::Ipv4Addr;

        const TEST_TIMEOUT: Duration = Duration::from_secs(1);

        #[derive(Clone)]
        struct DropProbe(Arc<AtomicBool>);

        impl Drop for DropProbe {
            fn drop(&mut self) {
                self.0.store(true, Ordering::SeqCst);
            }
        }

        async fn wait_until_connect_refused(addr: SocketAddr) {
            tokio::time::timeout(TEST_TIMEOUT, async {
                loop {
                    match TcpStream::connect(addr).await {
                        Ok(stream) => drop(stream),
                        Err(_) => break,
                    }
                    tokio::task::yield_now().await;
                }
            })
            .await
            .expect("listener should stop accepting within the test timeout");
        }

        #[tokio::test]
        async fn shutdown_before_first_connection_stops_accepting() {
            let (ready_tx, ready_rx) = oneshot::channel();
            let (shutdown_tx, shutdown_rx) = oneshot::channel();
            let server = tokio::spawn(novarocks_mysql_adapter::serve_tcp_until_shutdown(
                SocketAddr::from((Ipv4Addr::LOCALHOST, 0)),
                async move {
                    let _ = shutdown_rx.await;
                },
                |_stream, _peer_addr| async move {
                    panic!("no connection should be accepted before shutdown")
                },
                move |addr| {
                    let _ = ready_tx.send(addr);
                },
            ));
            let addr = tokio::time::timeout(TEST_TIMEOUT, ready_rx)
                .await
                .expect("server should bind within the test timeout")
                .expect("ready sender should stay alive");

            shutdown_tx.send(()).expect("send shutdown");
            tokio::time::timeout(TEST_TIMEOUT, server)
                .await
                .expect("server should stop within the test timeout")
                .expect("server task should not panic")
                .expect("server shutdown should succeed");

            assert!(TcpStream::connect(addr).await.is_err());
        }

        #[tokio::test]
        async fn shutdown_stops_new_accepts_and_waits_for_active_session() {
            let (ready_tx, ready_rx) = oneshot::channel();
            let (shutdown_tx, shutdown_rx) = oneshot::channel();
            let (started_tx, started_rx) = oneshot::channel();
            let started_tx = Arc::new(Mutex::new(Some(started_tx)));
            let (release_tx, release_rx) = oneshot::channel();
            let release_rx = Arc::new(Mutex::new(Some(release_rx)));
            let server = tokio::spawn(novarocks_mysql_adapter::serve_tcp_until_shutdown(
                SocketAddr::from((Ipv4Addr::LOCALHOST, 0)),
                async move {
                    let _ = shutdown_rx.await;
                },
                move |_stream, _peer_addr| {
                    let started_tx = Arc::clone(&started_tx);
                    let release_rx = Arc::clone(&release_rx);
                    async move {
                        if let Some(started_tx) = started_tx.lock().expect("started lock").take() {
                            let _ = started_tx.send(());
                        }
                        let release_rx = release_rx.lock().expect("release lock").take();
                        if let Some(release_rx) = release_rx {
                            let _ = release_rx.await;
                        }
                    }
                },
                move |addr| {
                    let _ = ready_tx.send(addr);
                },
            ));
            let addr = tokio::time::timeout(TEST_TIMEOUT, ready_rx)
                .await
                .expect("server should bind within the test timeout")
                .expect("ready sender should stay alive");
            let _client = TcpStream::connect(addr)
                .await
                .expect("connect active session");
            tokio::time::timeout(TEST_TIMEOUT, started_rx)
                .await
                .expect("session should start within the test timeout")
                .expect("session start sender should stay alive");

            shutdown_tx.send(()).expect("send shutdown");
            wait_until_connect_refused(addr).await;
            assert!(
                !server.is_finished(),
                "server must wait for the accepted session to finish"
            );

            release_tx.send(()).expect("release active session");
            tokio::time::timeout(TEST_TIMEOUT, server)
                .await
                .expect("server should stop after session release")
                .expect("server task should not panic")
                .expect("server shutdown should succeed");
        }

        #[tokio::test]
        async fn drain_stops_accepts_before_final_connection_teardown() {
            let (ready_tx, ready_rx) = oneshot::channel();
            let (drain_tx, drain_rx) = oneshot::channel();
            let (finalize_tx, finalize_rx) = oneshot::channel();
            let (started_tx, started_rx) = oneshot::channel();
            let started_tx = Arc::new(Mutex::new(Some(started_tx)));
            let (release_tx, release_rx) = oneshot::channel();
            let release_rx = Arc::new(Mutex::new(Some(release_rx)));
            let server = tokio::spawn(
                novarocks_mysql_adapter::serve_tcp_until_drain_then_shutdown(
                    SocketAddr::from((Ipv4Addr::LOCALHOST, 0)),
                    async move {
                        let _ = drain_rx.await;
                    },
                    async move {
                        let _ = finalize_rx.await;
                    },
                    move |_stream, _peer_addr| {
                        let started_tx = Arc::clone(&started_tx);
                        let release_rx = Arc::clone(&release_rx);
                        async move {
                            if let Some(started_tx) =
                                started_tx.lock().expect("started lock").take()
                            {
                                let _ = started_tx.send(());
                            }
                            let release_rx = { release_rx.lock().expect("release lock").take() };
                            if let Some(release_rx) = release_rx {
                                let _ = release_rx.await;
                            }
                        }
                    },
                    move |addr| {
                        let _ = ready_tx.send(addr);
                    },
                    TEST_TIMEOUT,
                ),
            );
            let addr = tokio::time::timeout(TEST_TIMEOUT, ready_rx)
                .await
                .expect("server should bind within the test timeout")
                .expect("ready sender should stay alive");
            let _client = TcpStream::connect(addr)
                .await
                .expect("connect active session");
            tokio::time::timeout(TEST_TIMEOUT, started_rx)
                .await
                .expect("session should start within the test timeout")
                .expect("session start sender should stay alive");

            drain_tx.send(()).expect("send drain");
            wait_until_connect_refused(addr).await;
            assert!(
                !server.is_finished(),
                "drain must not finalize existing protocol tasks"
            );
            finalize_tx.send(()).expect("send final teardown");
            tokio::task::yield_now().await;
            assert!(
                !server.is_finished(),
                "final teardown still waits for the protocol task to finish"
            );

            release_tx.send(()).expect("release active session");
            tokio::time::timeout(TEST_TIMEOUT, server)
                .await
                .expect("server should stop after final teardown and session release")
                .expect("server task should not panic")
                .expect("server shutdown should succeed");
        }

        #[tokio::test]
        async fn drain_timeout_aborts_stuck_session() {
            assert_eq!(SESSION_DRAIN_TIMEOUT, Duration::from_secs(5));

            let (ready_tx, ready_rx) = oneshot::channel();
            let (shutdown_tx, shutdown_rx) = oneshot::channel();
            let (started_tx, started_rx) = oneshot::channel();
            let started_tx = Arc::new(Mutex::new(Some(started_tx)));
            let session_dropped = Arc::new(AtomicBool::new(false));
            let session_dropped_in_task = Arc::clone(&session_dropped);
            let server = tokio::spawn(
                novarocks_mysql_adapter::serve_tcp_until_shutdown_with_drain_timeout(
                    SocketAddr::from((Ipv4Addr::LOCALHOST, 0)),
                    async move {
                        let _ = shutdown_rx.await;
                    },
                    move |_stream, _peer_addr| {
                        let started_tx = Arc::clone(&started_tx);
                        let session_dropped = Arc::clone(&session_dropped_in_task);
                        async move {
                            let _probe = DropProbe(session_dropped);
                            if let Some(started_tx) =
                                started_tx.lock().expect("started lock").take()
                            {
                                let _ = started_tx.send(());
                            }
                            pending::<()>().await;
                        }
                    },
                    move |addr| {
                        let _ = ready_tx.send(addr);
                    },
                    Duration::from_millis(20),
                ),
            );
            let addr = tokio::time::timeout(TEST_TIMEOUT, ready_rx)
                .await
                .expect("server should bind within the test timeout")
                .expect("ready sender should stay alive");
            let _client = TcpStream::connect(addr)
                .await
                .expect("connect stuck session");
            tokio::time::timeout(TEST_TIMEOUT, started_rx)
                .await
                .expect("session should start within the test timeout")
                .expect("session start sender should stay alive");

            shutdown_tx.send(()).expect("send shutdown");
            tokio::time::timeout(TEST_TIMEOUT, server)
                .await
                .expect("server should abort the stuck session")
                .expect("server task should not panic")
                .expect("server shutdown should succeed");

            assert!(session_dropped.load(Ordering::SeqCst));
            assert!(TcpStream::connect(addr).await.is_err());
        }

        #[tokio::test]
        async fn bind_failure_returns_without_ready_marker() {
            let occupied = TcpListener::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))
                .await
                .expect("reserve test address");
            let addr = occupied.local_addr().expect("reserved address");
            let ready_emitted = Arc::new(AtomicBool::new(false));
            let ready_emitted_in_callback = Arc::clone(&ready_emitted);

            let err = novarocks_mysql_adapter::serve_tcp_until_shutdown(
                addr,
                pending::<()>(),
                |_stream, _peer_addr| async move {},
                move |_addr| ready_emitted_in_callback.store(true, Ordering::SeqCst),
            )
            .await
            .expect_err("occupied address should fail to bind");

            assert!(err.contains("bind MySQL listener"), "{err}");
            assert!(!ready_emitted.load(Ordering::SeqCst));
        }
    }
}
