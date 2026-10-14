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

use crate::liveness::{PreparedRoleSession, RunnerSession};
use crate::scope::{CloseCause, ProcessBinding, SessionScope};
use crate::session::{
    DECLARATION_VERSION, DomainGrant, MAX_DECLARATION_BYTES, SessionDeclaration, SessionRole,
};
use std::sync::{Arc, Barrier};
use std::time::{Duration, Instant};

struct PrivateRoot(std::path::PathBuf);
impl PrivateRoot {
    fn new() -> Self {
        use std::os::unix::fs::DirBuilderExt;
        static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);
        let n = NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!("g2-{}-{n}", std::process::id()));
        std::fs::DirBuilder::new()
            .mode(0o700)
            .create(&path)
            .unwrap();
        Self(path)
    }
    fn declaration(&self, millis: u64) -> SessionDeclaration {
        SessionDeclaration {
            schema_version: DECLARATION_VERSION,
            session_id: "test-session".into(),
            launch_id: "launch-one".into(),
            role: SessionRole::Be,
            lifetime_millis: millis,
            liveness_socket: self.0.join("live.sock"),
            handshake_secret: "abcdefghijklmnopqrstuvwxyz0123456789".into(),
            domain_grants: vec![DomainGrant {
                domain: "test-domain".into(),
                grant_id: "only-once".into(),
                parameters: serde_json::json!({"request":"exact"}),
            }],
        }
    }
}
impl Drop for PrivateRoot {
    fn drop(&mut self) {
        std::fs::remove_dir_all(&self.0).unwrap();
    }
}

#[test]
fn declaration_is_bounded_private_and_debug_redacted() {
    use std::os::unix::fs::PermissionsExt;
    let root = PrivateRoot::new();
    let declaration = root.declaration(1000);
    let path = root.0.join("declaration.json");
    declaration.write_private(&path).unwrap();
    assert_eq!(
        std::fs::metadata(&path).unwrap().permissions().mode() & 0o777,
        0o600
    );
    assert!(declaration.write_private(&path).is_err());
    assert_eq!(
        SessionDeclaration::read_bounded(&path).unwrap().session_id,
        declaration.session_id
    );
    let debug = format!("{declaration:?} {:?}", declaration.domain_grants[0]);
    assert!(!debug.contains(&declaration.handshake_secret));
    assert!(!debug.contains("exact"));
    let mut duplicate = declaration.clone();
    duplicate
        .domain_grants
        .push(duplicate.domain_grants[0].clone());
    assert!(duplicate.validate_envelope().is_err());
    let large = root.0.join("large.json");
    std::fs::write(&large, vec![b' '; MAX_DECLARATION_BYTES + 1]).unwrap();
    std::fs::set_permissions(&large, std::fs::Permissions::from_mode(0o600)).unwrap();
    assert!(
        SessionDeclaration::read_bounded(&large)
            .unwrap_err()
            .to_string()
            .contains("bounded regular")
    );
    let link = root.0.join("link.json");
    std::os::unix::fs::symlink(&path, &link).unwrap();
    assert!(SessionDeclaration::read_bounded(&link).is_err());
}

#[test]
fn exact_process_once_claim_and_close_are_linearized() {
    let root = PrivateRoot::new();
    let process = ProcessBinding::Backend(novarocks_types::BackendProcessId::new_v7());
    let scope = SessionScope::bind(
        root.declaration(1000),
        process,
        Instant::now() + Duration::from_secs(1),
    )
    .unwrap();
    assert!(scope.grants_for("other-domain").is_empty());
    let grant = scope.grants_for("test-domain").pop().unwrap();
    assert!(
        grant
            .claim_once(ProcessBinding::Backend(
                novarocks_types::BackendProcessId::new_v7()
            ))
            .is_err()
    );
    let barrier = Arc::new(Barrier::new(12));
    let count = std::thread::scope(|threads| {
        let workers = (0..12)
            .map(|_| {
                let grant = grant.clone();
                let barrier = barrier.clone();
                threads.spawn(move || {
                    barrier.wait();
                    grant.claim_once(process).is_ok()
                })
            })
            .collect::<Vec<_>>();
        // Join every helper, irrespective of which claimant wins.
        let mut wins = 0;
        for worker in workers {
            wins += usize::from(worker.join().unwrap());
        }
        wins
    });
    assert_eq!(count, 1);
    assert_eq!(scope.observations().len(), 2);
    scope.close(CloseCause::ExplicitEnd);
    assert!(grant.claim_once(process).is_err());
    assert!(!grant.cancellation().is_active());
}

#[test]
fn controlled_clock_expiration_does_not_renew_or_reopen() {
    let root = PrivateRoot::new();
    let process = ProcessBinding::Backend(novarocks_types::BackendProcessId::new_v7());
    let deadline = Instant::now() + Duration::from_secs(1);
    let scope = SessionScope::bind(root.declaration(1000), process, deadline).unwrap();
    assert!(scope.is_active_at(deadline - Duration::from_nanos(1)));
    assert!(!scope.is_active_at(deadline));
    assert!(!scope.is_active_at(deadline - Duration::from_secs(1)));
    assert_eq!(scope.close_cause(), Some(CloseCause::Deadline));
    assert!(
        scope.grants_for("test-domain")[0]
            .claim_once(process)
            .is_err()
    );
}

#[tokio::test(flavor = "current_thread")]
async fn scope_cancellation_wakes_waiters_and_hit_is_only_an_observation() {
    let root = PrivateRoot::new();
    let process = ProcessBinding::Backend(novarocks_types::BackendProcessId::new_v7());
    let scope = SessionScope::bind(
        root.declaration(1000),
        process,
        Instant::now() + Duration::from_secs(1),
    )
    .unwrap();
    let mut claim = scope.grants_for("test-domain")[0]
        .claim_once(process)
        .unwrap();
    claim.record_hit().unwrap();
    assert!(claim.record_hit().is_err());
    let cancellation = claim.cancellation();
    scope.close(CloseCause::RoleStopped);
    tokio::time::timeout(Duration::from_millis(100), cancellation.cancelled())
        .await
        .unwrap();
    drop(claim);
    assert_eq!(
        scope
            .observations()
            .iter()
            .map(|o| o.event)
            .collect::<Vec<_>>(),
        ["claimed", "hit", "claim-released"]
    );
}

#[test]
fn one_launch_binds_existing_native_identity_and_end_joins_both_helpers() {
    let root = PrivateRoot::new();
    let declaration = root.declaration(2000);
    let path = root.0.join("declaration.json");
    declaration.write_private(&path).unwrap();
    assert!(PreparedRoleSession::read_and_claim(&path, SessionRole::Fe).is_err());
    let prepared = PreparedRoleSession::read_and_claim(&path, SessionRole::Be).unwrap();
    assert!(PreparedRoleSession::read_and_claim(&path, SessionRole::Be).is_err());
    let mut runner = RunnerSession::start(declaration).unwrap();
    let process = ProcessBinding::Backend(novarocks_types::BackendProcessId::new_v7());
    let mut role = prepared.bind(process).unwrap();
    assert_eq!(role.scope().process(), process);
    assert!(prepared.clone().bind(process).is_err());
    let until = Instant::now() + Duration::from_secs(1);
    while runner.bound_process().unwrap().is_none() {
        assert!(Instant::now() < until);
        std::thread::yield_now();
    }
    assert_eq!(
        runner.bound_process().unwrap().unwrap(),
        process.process_id()
    );
    let receipt =
        crate::target_binding::RoleProcessReceipt::read_private(&runner.process_receipt_path())
            .unwrap();
    assert_eq!(
        receipt.backend_process().unwrap().to_string(),
        process.process_id()
    );
    assert_eq!(receipt.session_id, "test-session");
    assert_eq!(receipt.launch_id, "launch-one");
    let scope = role.scope().clone();
    runner.end_and_join().unwrap();
    scope.wait_closed();
    assert!(!scope.is_active());
    role.close_and_join().unwrap();
    role.close_and_join().unwrap();
    runner.end_and_join().unwrap();
    assert!(!root.0.join("live.sock").exists());
}

#[test]
fn total_lifetime_closes_without_a_new_action_or_explicit_end() {
    let root = PrivateRoot::new();
    let declaration = root.declaration(200);
    let path = root.0.join("declaration.json");
    declaration.write_private(&path).unwrap();
    let prepared = PreparedRoleSession::read_and_claim(&path, SessionRole::Be).unwrap();
    let mut runner = RunnerSession::start(declaration).unwrap();
    let process = ProcessBinding::Backend(novarocks_types::BackendProcessId::new_v7());
    let mut role = prepared.bind(process).unwrap();
    let scope = role.scope().clone();
    scope.wait_closed();
    assert!(!scope.is_active());
    assert!(
        scope.grants_for("test-domain")[0]
            .claim_once(process)
            .is_err()
    );
    role.close_and_join().unwrap();
    runner.end_and_join().unwrap();
}

#[test]
fn original_deadline_charges_late_start_and_both_helpers_actually_join() {
    let root = PrivateRoot::new();
    let declaration = root.declaration(5000);
    let path = root.0.join("declaration.json");
    declaration.write_private(&path).unwrap();
    let prepared = PreparedRoleSession::read_and_claim(&path, SessionRole::Be).unwrap();
    let original_deadline = Instant::now() + Duration::from_millis(1500);
    std::thread::sleep(Duration::from_millis(50));
    let mut runner = RunnerSession::start_until(declaration, original_deadline).unwrap();
    assert_eq!(runner.deadline(), original_deadline);
    let process = ProcessBinding::Backend(novarocks_types::BackendProcessId::new_v7());
    let mut role = prepared.bind(process).unwrap();
    let scope = role.scope().clone();
    assert!(scope.deadline() <= original_deadline);
    scope.wait_closed();
    assert!(!scope.is_active());
    role.close_and_join().unwrap();
    runner.end_and_join().unwrap();
    role.close_and_join().unwrap();
    runner.end_and_join().unwrap();
    assert!(!root.0.join("live.sock").exists());
}

#[test]
fn expired_original_deadline_never_binds_a_socket_or_starts_a_helper() {
    let root = PrivateRoot::new();
    let declaration = root.declaration(2000);
    let socket = declaration.liveness_socket.clone();
    let result = RunnerSession::start_until(declaration, Instant::now());
    let Err(error) = result else {
        panic!("an expired original deadline must refuse startup");
    };
    assert_eq!(error.kind(), std::io::ErrorKind::TimedOut);
    assert!(!socket.exists());
}

#[test]
fn a_later_original_deadline_does_not_extend_declared_lifetime() {
    let root = PrivateRoot::new();
    let declaration = root.declaration(200);
    let before = Instant::now();
    let mut runner =
        RunnerSession::start_until(declaration, before + Duration::from_secs(2)).unwrap();
    let after = Instant::now();
    assert!(runner.deadline() >= before + Duration::from_millis(200));
    assert!(runner.deadline() <= after + Duration::from_millis(200));
    runner.end_and_join().unwrap();
    assert!(!root.0.join("live.sock").exists());
}

#[test]
fn socket_cleanup_refuses_a_replacement_and_start_never_overwrites() {
    let root = PrivateRoot::new();
    let declaration = root.declaration(1000);
    let mut runner = RunnerSession::start(declaration.clone()).unwrap();
    assert!(RunnerSession::start(declaration.clone()).is_err());
    std::fs::remove_file(&declaration.liveness_socket).unwrap();
    std::fs::write(&declaration.liveness_socket, "replacement").unwrap();
    assert!(runner.end_and_join().is_err());
    assert_eq!(
        std::fs::read_to_string(&declaration.liveness_socket).unwrap(),
        "replacement"
    );
}

#[test]
fn eof_closes_role_scope_without_a_marker_file_or_product_exit_claim() {
    use crate::session::{LivenessFrame, read_frame_with, write_frame};
    use std::io::Read;
    use std::os::unix::net::UnixListener;
    let root = PrivateRoot::new();
    let declaration = root.declaration(2000);
    let path = root.0.join("declaration.json");
    declaration.write_private(&path).unwrap();
    let listener = UnixListener::bind(&declaration.liveness_socket).unwrap();
    let (disconnect, after_bind) = std::sync::mpsc::sync_channel::<()>(1);
    let peer = std::thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        stream
            .set_read_timeout(Some(Duration::from_secs(1)))
            .unwrap();
        assert!(matches!(
            read_frame_with(&mut |bytes| stream.read_exact(bytes)).unwrap(),
            LivenessFrame::Hello { .. }
        ));
        write_frame(
            &mut stream,
            &LivenessFrame::Bound {
                remaining_millis: 1000,
            },
        )
        .unwrap();
        after_bind.recv_timeout(Duration::from_secs(1)).unwrap();
        // EOF after successful binding is control cancellation, not product cancellation.
    });
    let prepared = PreparedRoleSession::read_and_claim(&path, SessionRole::Be).unwrap();
    let mut role = prepared
        .bind(ProcessBinding::Backend(
            novarocks_types::BackendProcessId::new_v7(),
        ))
        .unwrap();
    disconnect.send(()).unwrap();
    role.scope().wait_closed();
    assert_eq!(role.scope().close_cause(), Some(CloseCause::Disconnected));
    role.close_and_join().unwrap();
    peer.join().unwrap();
}

#[test]
fn invalid_handshake_cannot_bind_or_take_over_a_launch() {
    use crate::session::{LivenessFrame, write_frame};
    use std::os::unix::net::UnixStream;
    let root = PrivateRoot::new();
    let declaration = root.declaration(2000);
    let mut runner = RunnerSession::start(declaration.clone()).unwrap();
    let mut stream = UnixStream::connect(&declaration.liveness_socket).unwrap();
    write_frame(
        &mut stream,
        &LivenessFrame::Hello {
            session_id: declaration.session_id,
            launch_id: declaration.launch_id,
            role: SessionRole::Be,
            process_id: novarocks_types::BackendProcessId::new_v7().to_string(),
            handshake_secret: "wrong-secret".into(),
        },
    )
    .unwrap();
    let until = Instant::now() + Duration::from_secs(1);
    while runner.bound_process().is_ok() {
        assert!(Instant::now() < until);
        std::thread::yield_now();
    }
    assert!(runner.end_and_join().is_err());
    assert!(!root.0.join("live.sock").exists());
}

/// Own every queued client so dropping a probe cannot make the listener ready
/// again. No peer accepts during the assertion window.
#[cfg(any(target_os = "linux", target_os = "macos"))]
struct BackloggedControlSocket {
    listener: socket2::Socket,
    _queued_clients: Vec<socket2::Socket>,
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
#[derive(Debug, PartialEq, Eq)]
enum BacklogConnectOutcome {
    Pending,
    Refused,
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
impl BackloggedControlSocket {
    fn saturate(path: &std::path::Path) -> std::io::Result<(Self, BacklogConnectOutcome)> {
        use socket2::{Domain, SockAddr, Socket, Type};
        let address = SockAddr::unix(path)?;
        let listener = Socket::new(Domain::UNIX, Type::STREAM, None)?;
        listener.bind(&address)?;
        listener.listen(1)?;
        listener.set_nonblocking(true)?;
        let mut queued_clients = Vec::new();
        for _ in 0..32 {
            let probe = Socket::new(Domain::UNIX, Type::STREAM, None)?;
            probe.set_nonblocking(true)?;
            match probe.connect(&address) {
                Ok(()) => queued_clients.push(probe),
                Err(error)
                    if error.kind() == std::io::ErrorKind::WouldBlock
                        || matches!(
                            error.raw_os_error(),
                            Some(libc::EINPROGRESS) | Some(libc::EALREADY)
                        ) =>
                {
                    // BSD may initially report EINPROGRESS even for a free
                    // slot. Require the probe to remain unconnected before
                    // claiming that the kernel backlog is actually full.
                    let probe_deadline = Instant::now() + Duration::from_millis(20);
                    while probe.peer_addr().is_err() && Instant::now() < probe_deadline {
                        if let Some(error) = probe.take_error()? {
                            if error.kind() == std::io::ErrorKind::ConnectionRefused {
                                return Ok((
                                    Self {
                                        listener,
                                        _queued_clients: queued_clients,
                                    },
                                    BacklogConnectOutcome::Refused,
                                ));
                            }
                            return Err(error);
                        }
                        std::thread::sleep(Duration::from_millis(1));
                    }
                    let still_pending = probe.peer_addr().is_err();
                    queued_clients.push(probe);
                    if still_pending {
                        return Ok((
                            Self {
                                listener,
                                _queued_clients: queued_clients,
                            },
                            BacklogConnectOutcome::Pending,
                        ));
                    }
                }
                Err(error) if error.kind() == std::io::ErrorKind::ConnectionRefused => {
                    return Ok((
                        Self {
                            listener,
                            _queued_clients: queued_clients,
                        },
                        BacklogConnectOutcome::Refused,
                    ));
                }
                Err(error) => return Err(error),
            }
        }
        Err(std::io::Error::other(
            "test could not saturate the private Unix listener backlog",
        ))
    }

    /// Release kernel queue pressure even when a regressed blocking connect
    /// missed the assertion deadline. This is cleanup, not a retry of the test.
    fn drain(&self) -> std::io::Result<()> {
        loop {
            match self.listener.accept() {
                Ok((peer, _)) => drop(peer),
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => return Ok(()),
                Err(error) if error.kind() == std::io::ErrorKind::Interrupted => continue,
                Err(error) => return Err(error),
            }
        }
    }
}

/// Join the actual helper before reporting the primary assertion failure. The
/// cancellation plus backlog drain also releases a blocking-connect regression.
#[cfg(any(target_os = "linux", target_os = "macos"))]
fn finish_backlogged_connect(
    backlog: &BackloggedControlSocket,
    stop: &std::sync::atomic::AtomicBool,
    worker: std::thread::JoinHandle<()>,
) {
    stop.store(true, std::sync::atomic::Ordering::Release);
    let cleanup_deadline = Instant::now() + Duration::from_secs(1);
    let mut cleanup_error = None;
    while !worker.is_finished() && Instant::now() < cleanup_deadline {
        if let Err(error) = backlog.drain() {
            cleanup_error = Some(error);
            break;
        }
        std::thread::sleep(Duration::from_millis(1));
    }
    // A queue slot is now free even if the helper has not yet been scheduled.
    // The helper is joined rather than detached on the failure path.
    let joined = worker.join();
    assert!(joined.is_ok(), "control-connect helper panicked");
    assert!(
        cleanup_error.is_none(),
        "backlog cleanup failed: {cleanup_error:?}"
    );
}

#[test]
fn control_connect_expired_absolute_deadline_never_reaches_a_ready_listener() {
    use std::os::unix::net::UnixListener;
    let root = PrivateRoot::new();
    let path = root.0.join("expired-connect.sock");
    let listener = UnixListener::bind(&path).unwrap();
    listener.set_nonblocking(true).unwrap();
    let original_deadline = Instant::now() - Duration::from_millis(1);
    let error = crate::liveness::connect_until(&path, original_deadline, || false).unwrap_err();
    assert_eq!(error.kind(), std::io::ErrorKind::TimedOut);
    assert_eq!(
        listener.accept().unwrap_err().kind(),
        std::io::ErrorKind::WouldBlock
    );
}

#[test]
fn control_connect_cancellation_is_checked_before_and_after_connection_readiness() {
    use std::os::unix::net::UnixListener;
    use std::sync::atomic::{AtomicUsize, Ordering};
    let root = PrivateRoot::new();
    let path = root.0.join("cancel-connect.sock");
    let listener = UnixListener::bind(&path).unwrap();
    listener.set_nonblocking(true).unwrap();
    let deadline = Instant::now() + Duration::from_secs(1);
    assert_eq!(
        crate::liveness::connect_until(&path, deadline, || true)
            .unwrap_err()
            .kind(),
        std::io::ErrorKind::Interrupted
    );
    assert_eq!(
        listener.accept().unwrap_err().kind(),
        std::io::ErrorKind::WouldBlock
    );
    let calls = AtomicUsize::new(0);
    let actual_peer = std::sync::Mutex::new(None);
    let error =
        crate::liveness::connect_until(&path, Instant::now() + Duration::from_secs(1), || {
            calls.fetch_add(1, Ordering::AcqRel);
            match listener.accept() {
                Ok((peer, _)) => {
                    *actual_peer.lock().unwrap() = Some(peer);
                    true
                }
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => false,
                Err(error) => panic!("readiness probe failed: {error}"),
            }
        })
        .unwrap_err();
    assert_eq!(error.kind(), std::io::ErrorKind::Interrupted);
    assert!(calls.load(Ordering::Acquire) >= 2);
    assert!(
        actual_peer.lock().unwrap().is_some(),
        "cancellation must follow an actual connection"
    );
}

// Linux AF_UNIX queue saturation provides a real WouldBlock retry. macOS
// refuses a full queue; that behavior is tested separately below and cannot
// substitute for this pending-connect deadline/cancellation evidence.
#[cfg(target_os = "linux")]
#[test]
fn control_connect_backlog_retries_keep_the_original_deadline() {
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    let root = PrivateRoot::new();
    let path = root.0.join("deadline-backlog.sock");
    let (backlog, outcome) = BackloggedControlSocket::saturate(&path).unwrap();
    assert_eq!(
        outcome,
        BacklogConnectOutcome::Pending,
        "this platform must provide an actual pending backlog for retry coverage"
    );
    let stop = Arc::new(AtomicBool::new(false));
    let polls = Arc::new(AtomicUsize::new(0));
    let original_deadline = Instant::now() + Duration::from_millis(80);
    let (send, receive) = std::sync::mpsc::channel();
    let worker_stop = stop.clone();
    let worker_polls = polls.clone();
    let worker = std::thread::spawn(move || {
        let result = crate::liveness::connect_until(&path, original_deadline, || {
            worker_polls.fetch_add(1, Ordering::AcqRel);
            worker_stop.load(Ordering::Acquire)
        });
        let _ = send.send((Instant::now(), result));
    });
    // This is a failure watchdog, never a replacement operation deadline.
    let received = receive.recv_timeout(Duration::from_millis(500));
    finish_backlogged_connect(&backlog, &stop, worker);
    let (finished_at, result) = received.expect("backlog connect exceeded its bounded watchdog");
    assert_eq!(result.unwrap_err().kind(), std::io::ErrorKind::TimedOut);
    assert!(
        finished_at >= original_deadline,
        "original deadline was shortened"
    );
    assert!(
        polls.load(Ordering::Acquire) >= 2,
        "kernel backlog did not exercise connect retries"
    );
}

// Linux AF_UNIX queue saturation provides a real WouldBlock retry. macOS
// refuses a full queue; that behavior is tested separately below and cannot
// substitute for this pending-connect deadline/cancellation evidence.
#[cfg(target_os = "linux")]
#[test]
fn control_connect_cancellation_unblocks_a_saturated_unix_backlog() {
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    let root = PrivateRoot::new();
    let path = root.0.join("cancel-backlog.sock");
    let (backlog, outcome) = BackloggedControlSocket::saturate(&path).unwrap();
    assert_eq!(
        outcome,
        BacklogConnectOutcome::Pending,
        "this platform must provide an actual pending backlog for retry coverage"
    );
    let stop = Arc::new(AtomicBool::new(false));
    let polls = Arc::new(AtomicUsize::new(0));
    let original_deadline = Instant::now() + Duration::from_secs(2);
    let (entered_send, entered_receive) = std::sync::mpsc::sync_channel(1);
    let (send, receive) = std::sync::mpsc::channel();
    let worker_stop = stop.clone();
    let worker = std::thread::spawn(move || {
        let result = crate::liveness::connect_until(&path, original_deadline, || {
            if polls.fetch_add(1, Ordering::AcqRel) == 1 {
                let _ = entered_send.try_send(());
            }
            worker_stop.load(Ordering::Acquire)
        });
        let _ = send.send((Instant::now(), result));
    });
    // Cancellation is requested only after a real blocked-connect retry.
    let entered = entered_receive.recv_timeout(Duration::from_millis(300));
    stop.store(true, Ordering::Release);
    let received = receive.recv_timeout(Duration::from_millis(300));
    finish_backlogged_connect(&backlog, &stop, worker);
    entered.expect("kernel backlog never reached a connect retry");
    let (finished_at, result) = received.expect("cancellation did not release backlog connect");
    assert_eq!(result.unwrap_err().kind(), std::io::ErrorKind::Interrupted);
    assert!(
        finished_at < original_deadline,
        "connect waited for deadline rather than cancellation"
    );
}

// Linux AF_UNIX queue saturation provides a real WouldBlock retry. macOS
// refuses a full queue; that behavior is tested separately below and cannot
// substitute for this pending-connect deadline/cancellation evidence.
#[cfg(target_os = "linux")]
#[test]
fn control_connect_backlog_readiness_after_expiry_cannot_renew_the_attempt() {
    use std::sync::atomic::{AtomicBool, Ordering};
    let root = PrivateRoot::new();
    let path = root.0.join("late-ready-backlog.sock");
    let (backlog, outcome) = BackloggedControlSocket::saturate(&path).unwrap();
    assert_eq!(
        outcome,
        BacklogConnectOutcome::Pending,
        "this platform must provide an actual pending backlog for retry coverage"
    );
    let stop = Arc::new(AtomicBool::new(false));
    let original_deadline = Instant::now() + Duration::from_millis(60);
    let (send, receive) = std::sync::mpsc::channel();
    let worker_stop = stop.clone();
    let worker = std::thread::spawn(move || {
        let result = crate::liveness::connect_until(&path, original_deadline, || {
            worker_stop.load(Ordering::Acquire)
        });
        let _ = send.send(result);
    });
    // Make the socket ready only after the caller's original absolute expiry.
    // A retry that renewed its budget would now return a successful stream.
    std::thread::sleep(
        original_deadline.saturating_duration_since(Instant::now()) + Duration::from_millis(10),
    );
    let release = backlog.drain();
    let received = receive.recv_timeout(Duration::from_millis(300));
    finish_backlogged_connect(&backlog, &stop, worker);
    release.expect("release the expired attempt's backlog pressure");
    assert_eq!(
        received
            .expect("late-readiness probe exceeded its watchdog")
            .unwrap_err()
            .kind(),
        std::io::ErrorKind::TimedOut
    );
}

// macOS returns ECONNREFUSED for a full AF_UNIX queue. This covers fail-fast
// refusal, not retry-loop expiry or cancellation; those require the Linux
// pending fixture above. Unexpected kernel behavior fails this assertion.
#[cfg(target_os = "macos")]
#[test]
fn control_connect_macos_full_backlog_refuses_without_waiting_for_deadline() {
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    let root = PrivateRoot::new();
    let path = root.0.join("refused-backlog.sock");
    let (backlog, outcome) = BackloggedControlSocket::saturate(&path).unwrap();
    assert_eq!(outcome, BacklogConnectOutcome::Refused);
    assert!(
        !backlog._queued_clients.is_empty(),
        "refusal must follow real queued clients on a live listener"
    );
    let stop = Arc::new(AtomicBool::new(false));
    let polls = Arc::new(AtomicUsize::new(0));
    let original_deadline = Instant::now() + Duration::from_secs(2);
    let (send, receive) = std::sync::mpsc::channel();
    let worker_stop = stop.clone();
    let worker_polls = polls.clone();
    let worker_path = path.clone();
    let worker = std::thread::spawn(move || {
        let result = crate::liveness::connect_until(&worker_path, original_deadline, || {
            worker_polls.fetch_add(1, Ordering::AcqRel);
            worker_stop.load(Ordering::Acquire)
        });
        let _ = send.send((Instant::now(), result));
    });
    let received = receive.recv_timeout(Duration::from_millis(300));
    finish_backlogged_connect(&backlog, &stop, worker);
    let (finished_at, result) = received.expect("full backlog refusal did not fail fast");
    let error = result.unwrap_err();
    assert_eq!(error.kind(), std::io::ErrorKind::ConnectionRefused);
    assert_eq!(error.raw_os_error(), Some(libc::ECONNREFUSED));
    assert_eq!(polls.load(Ordering::Acquire), 1, "refusal was retried");
    assert!(finished_at < original_deadline);

    // Prove the refusal came from queue pressure, not an absent or closed
    // endpoint. Once accepted clients leave the queue, the same listener works.
    backlog.drain().unwrap();
    let connected =
        crate::liveness::connect_until(&path, Instant::now() + Duration::from_secs(1), || false)
            .unwrap();
    let (accepted, _) = backlog.listener.accept().unwrap();
    drop((connected, accepted));
}
