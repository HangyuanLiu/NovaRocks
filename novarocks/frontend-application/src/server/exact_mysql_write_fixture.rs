// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

//! Default-off test seam in the original FE service owner. No additional task owner.

use super::*;
use novarocks_mysql_adapter::exact_mysql_write_fixture::MysqlWriteFixture;
use novarocks_native_trust::NativeProcessIdentity;
use std::path::PathBuf;

const PATH_ENV: &str = "NOVAROCKS_MEM_1_M07_MYSQL_WRITE_SOCKET";
const NONCE_ENV: &str = "NOVAROCKS_MEM_1_M07_MYSQL_WRITE_NONCE_HEX";
const CONTROL_DEADLINE: Duration = Duration::from_secs(20);

pub(super) fn bind_from_environment(
    trust: &NativeTrust,
) -> Result<Option<MysqlWriteFixture>, FrontendApplicationError> {
    let path = std::env::var_os(PATH_ENV);
    let nonce = std::env::var_os(NONCE_ENV);
    let (path, nonce) = match (path, nonce) {
        (None, None) => return Ok(None),
        (Some(path), Some(nonce)) => (PathBuf::from(path), nonce),
        _ => {
            return Err(FrontendApplicationError::server(
                "exact MySQL fixture requires both startup environment fields",
            ));
        }
    };
    let nonce = nonce.to_str().ok_or_else(|| {
        FrontendApplicationError::server("exact MySQL fixture nonce is not canonical ASCII hex")
    })?;
    let nonce = decode_nonce(nonce).map_err(FrontendApplicationError::server)?;
    let Some(NativeProcessIdentity::Frontend(frontend)) = trust.local_process_identity() else {
        return Err(FrontendApplicationError::server(
            "exact MySQL fixture requires the original bound FE process identity",
        ));
    };
    let mut fixture =
        MysqlWriteFixture::bind(path, frontend, nonce, Instant::now() + CONTROL_DEADLINE)
            .map_err(FrontendApplicationError::server_fixture)?;
    if let Err(cause) = write_frontend_identity_marker(&mut std::io::stdout().lock(), frontend) {
        return Err(FrontendApplicationError::server_fixture(
            fixture.fail_startup_projection(cause),
        ));
    }
    Ok(Some(fixture))
}
// Only this feature's successful original fixture bind emits this fixed projection.
// No marker is emitted by the default server or feature builds with no explicit inputs.
fn write_frontend_identity_marker(
    output: &mut impl std::io::Write,
    frontend: novarocks_types::FrontendProcessId,
) -> std::io::Result<()> {
    use std::io::Write;
    let mut bytes = [0u8; 128];
    let length = {
        let mut target = std::io::Cursor::new(&mut bytes[..]);
        writeln!(
            target,
            "NOVAROCKS_MEM_1_M07_EXACT_MYSQL_FE frontend_process_id={frontend}"
        )?;
        target.position() as usize
    };
    output.write_all(&bytes[..length])
}
fn decode_nonce(text: &str) -> Result<[u8; 16], &'static str> {
    let bytes = text.as_bytes();
    if bytes.len() != 32
        || !bytes
            .iter()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(b))
    {
        return Err("exact MySQL fixture nonce is not canonical 32-byte lowercase hex");
    }
    let digit = |byte: u8| {
        if byte.is_ascii_digit() {
            byte - b'0'
        } else {
            byte - b'a' + 10
        }
    };
    let mut nonce = [0; 16];
    for (value, pair) in nonce.iter_mut().zip(bytes.chunks_exact(2)) {
        *value = digit(pair[0]) * 16 + digit(pair[1]);
    }
    if nonce == [0; 16] {
        return Err("exact MySQL fixture nonce must be nonzero");
    }
    Ok(nonce)
}

enum Exit {
    Mysql(Result<(), String>),
    Shutdown,
    Listener(String),
    Control(novarocks_mysql_adapter::exact_mysql_write_fixture::MysqlWriteFixtureError),
    OriginalTask,
}

pub(super) async fn serve<F>(
    mut fixture: MysqlWriteFixture,
    mysql_listener: ResolvedMysqlListenerSettings,
    session_factory: Arc<dyn QuerySessionFactory>,
    client_connections: Arc<MysqlClientConnectionRegistry>,
    shutdown: F,
    report_server: &mut crate::native::report_server::FrontendReportServerHandle,
    management_server: &mut FrontendManagementServer,
    host: &FrontendApplicationHost,
    drain_timeout: Duration,
    cleanup_timeout: Duration,
) -> Result<(), FrontendApplicationError>
where
    F: Future<Output = ()> + Send,
{
    let binding = fixture
        .listener_binding()
        .map_err(FrontendApplicationError::server_fixture)?;
    let original_failures = fixture.failure_observation();
    let (drain_tx, drain_rx) = tokio::sync::watch::channel(false);
    let (finalize_tx, finalize_rx) = tokio::sync::watch::channel(false);
    let wait_for_signal = |mut receiver: tokio::sync::watch::Receiver<bool>| async move {
        while !*receiver.borrow() {
            if receiver.changed().await.is_err() {
                break;
            }
        }
    };
    let ready_user = mysql_listener.user().to_string();
    let mysql_server = novarocks_mysql_adapter::query_application_fixture_listener(
        mysql_listener,
        version::short_version().to_string(),
        Arc::clone(&session_factory),
        Arc::clone(&client_connections),
        wait_for_signal(drain_rx),
        wait_for_signal(finalize_rx),
        cleanup_timeout,
        move |addr| emit_frontend_mysql_ready(addr, &ready_user),
        binding,
    );
    tokio::pin!(mysql_server);
    tokio::pin!(shutdown);
    let mut control_completed = false;
    let exit = loop {
        // Only this borrow is cancelled at a sibling exit. The controller and original
        // MySQL future remain in this owner. Successful Stop disables this branch forever.
        let event = {
            let control = fixture.run_control();
            tokio::pin!(control);
            tokio::select! {
                result = &mut mysql_server => Some(Exit::Mysql(result)),
                _ = &mut shutdown => Some(Exit::Shutdown),
                error = wait_for_frontend_listener_failure(report_server, management_server)
                    => Some(Exit::Listener(error)),
                _ = original_failures.wait_for_failure() => Some(Exit::OriginalTask),
                result = &mut control, if !control_completed => match result {
                    Ok(()) => None,
                    Err(error) => Some(Exit::Control(error)),
                },
            }
        };
        if let Some(exit) = event {
            break exit;
        }
        control_completed = true;
    };
    if !matches!(&exit, Exit::Shutdown) || !control_completed {
        fixture.fail_and_stop();
    }
    let close = fixture
        .close_control()
        .map_err(FrontendApplicationError::server_fixture);
    let result = match exit {
        // The original future is already completed; never poll it again.
        Exit::Mysql(result) => {
            session_factory.cancel_all(QueryCancellationReason::ServerShutdown);
            client_connections.terminate_all(ClientConnectionTerminationReason::ServerShutdown);
            host.serving_lifecycle().mark_stopping();
            result.map_err(FrontendApplicationError::server)
        }
        exit => {
            host.begin_serving_drain(drain_timeout);
            let _ = drain_tx.send(true);
            if matches!(&exit, Exit::Shutdown) {
                let graceful = tokio::time::timeout(
                    drain_timeout,
                    host.workload_observation()
                        .wait_until_no_root_responsibilities(),
                )
                .await;
                if graceful.is_err() {
                    host.cancel_governed_work_at_drain_deadline();
                    let _ = tokio::time::timeout(
                        cleanup_timeout,
                        host.workload_observation()
                            .wait_until_no_root_responsibilities(),
                    )
                    .await;
                }
            } else {
                host.cancel_governed_work_at_drain_deadline();
            }
            session_factory.cancel_all(QueryCancellationReason::ServerShutdown);
            client_connections.terminate_all(ClientConnectionTerminationReason::ServerShutdown);
            host.serving_lifecycle().mark_stopping();
            let _ = finalize_tx.send(true);
            let mysql = mysql_server.await.map_err(FrontendApplicationError::server);
            match exit {
                Exit::Shutdown => mysql,
                Exit::Listener(error) => {
                    combine_server_and_shutdown(Err(FrontendApplicationError::server(error)), mysql)
                }
                Exit::Control(error) => combine_server_and_shutdown(
                    Err(FrontendApplicationError::server_fixture(error)),
                    mysql,
                ),
                Exit::OriginalTask => combine_server_and_shutdown(
                    Err(FrontendApplicationError::server(
                        "original MySQL task failed; retained join evidence follows",
                    )),
                    mysql,
                ),
                Exit::Mysql(_) => unreachable!("completed MySQL handled above"),
            }
        }
    };
    // Registry drain retains the original watcher/connection lifetime, including
    // an accept failure where the listener skipped its finalize hook.
    let drained = fixture
        .verify_original_connection_drain(&client_connections)
        .await
        .map_err(FrontendApplicationError::server_fixture);
    let owners = fixture
        .finish_after_original_listener_join()
        .map_err(FrontendApplicationError::server_fixture);
    combine_server_and_shutdown(
        combine_server_and_shutdown(combine_server_and_shutdown(result, close), drained),
        owners,
    )
}

#[cfg(test)]
mod tests {
    use super::decode_nonce;
    #[test]
    fn startup_nonce_rejects_noncanonical_and_zero_without_echoing_input() {
        assert_eq!(
            decode_nonce("0102030405060708090a0b0c0d0e0f10").unwrap(),
            [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16]
        );
        for text in [
            "",
            "00000000000000000000000000000000",
            "0102030405060708090A0B0C0D0E0F10",
            "0102030405060708090a0b0c0d0e0f1x",
        ] {
            assert!(decode_nonce(text).is_err());
        }
    }
    #[test]
    fn original_control_io_source_survives_role_cleanup_aggregation() {
        use super::*;
        use std::error::Error;
        // No directory is created; this negative probes an actual local filesystem error.
        let path = PathBuf::from(format!(
            "/tmp/nr-m07-missing-{}-{}/gate.sock",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let control = match MysqlWriteFixture::bind(
            path,
            novarocks_types::FrontendProcessId::new_v7(),
            [1; 16],
            Instant::now() + Duration::from_secs(3),
        ) {
            Ok(_) => panic!("nonexistent private directory must fail"),
            Err(error) => error,
        };
        let role = FrontendApplicationError::server("original role failure")
            .with_role_cleanup(FrontendApplicationError::server_fixture(control));
        let mut source = role.source().expect("actual fixture source retained");
        loop {
            if let Some(io) = source.downcast_ref::<std::io::Error>() {
                assert_eq!(io.kind(), std::io::ErrorKind::NotFound);
                break;
            }
            source = source.source().expect("original local IO cause retained");
        }
        assert!(
            role.to_string()
                .starts_with("Server: original role failure; cleanup failed:")
        );
    }
}

#[cfg(test)]
mod identity_marker_tests {
    #[test]
    fn one_fixed_line_projects_only_supplied_actual_identity() {
        let frontend = novarocks_types::FrontendProcessId::try_from_bytes([
            1, 137, 15, 110, 122, 0, 113, 35, 129, 35, 69, 103, 137, 171, 205, 239,
        ])
        .unwrap();
        let mut output = Vec::new();
        super::write_frontend_identity_marker(&mut output, frontend).unwrap();
        assert_eq!(output,b"NOVAROCKS_MEM_1_M07_EXACT_MYSQL_FE frontend_process_id=01890f6e-7a00-7123-8123-456789abcdef\n");
    }
}

#[cfg(test)]
mod identity_marker_io_tests {
    #[test]
    fn original_output_error_is_returned_without_text_reconstruction() {
        struct Refusal;
        impl std::io::Write for Refusal {
            fn write(&mut self, _: &[u8]) -> std::io::Result<usize> {
                Err(std::io::Error::from_raw_os_error(32))
            }
            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }
        let error = super::write_frontend_identity_marker(
            &mut Refusal,
            novarocks_types::FrontendProcessId::new_v7(),
        )
        .unwrap_err();
        assert_eq!(error.raw_os_error(), Some(32));
    }
}
