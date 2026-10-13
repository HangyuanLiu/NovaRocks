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
