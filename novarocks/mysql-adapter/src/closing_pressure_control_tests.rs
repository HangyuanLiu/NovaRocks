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

//! Control-component tests only. No original MySQL writer, Work or native role is created.
//! Both Unix futures run directly on one current-thread caller; no task is spawned.

use super::*;
use crate::closing_pressure_gate::PressureOwner;
use std::os::unix::fs::{DirBuilderExt, symlink};
use std::sync::{
    Arc,
    atomic::{AtomicU64, AtomicUsize, Ordering},
};
use std::time::Duration;

const TEST_NONCE: [u8; 16] = [0x6a; 16];
const TEST_BUDGET: Duration = Duration::from_secs(3);
// Independent literal of the already-frozen SQL hash, not a fabricated SQL/query owner.
const FROZEN_HASH: [u8; 32] = [
    0x3c, 0xf4, 0x35, 0x3c, 0xe6, 0xc2, 0x89, 0x49, 0xb0, 0x9b, 0x6c, 0xb3, 0xed, 0x0d, 0xc2, 0x24,
    0x47, 0xc5, 0x00, 0xb0, 0xcb, 0x18, 0x7f, 0x58, 0xe9, 0xe4, 0x91, 0x8d, 0xaa, 0xd7, 0x8f, 0xd2,
];
static DIRECTORY_SEQUENCE: AtomicU64 = AtomicU64::new(1);

fn frontend() -> FrontendProcessId {
    FrontendProcessId::try_from_bytes([
        0x01, 0x89, 0x0f, 0x6e, 0x7a, 0x00, 0x71, 0x23, 0x81, 0x23, 0x45, 0x67, 0x89, 0xab, 0xcd,
        0xef,
    ])
    .unwrap()
}
fn run(work: impl Future<Output = ()>) {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(work);
}
fn digest(bytes: &[u8]) -> [u8; 32] {
    Sha256::digest(bytes).into()
}
async fn bounded_peer<F: Future>(original_deadline: Instant, work: F) -> F::Output {
    // One fixed fixture watchdog allows error observation/cleanup after the protocol clock.
    // It never refreshes the actual server/controller deadline or spawns a task.
    let watchdog = original_deadline + Duration::from_millis(250);
    match tokio::time::timeout_at(tokio::time::Instant::from_std(watchdog), work).await {
        Ok(value) => value,
        Err(_) => panic!("pressure component peer exceeded fixed fixture watchdog"),
    }
}
fn body(opcode: u8, sequence: u16, payload: &[u8]) -> Vec<u8> {
    let mut body = Vec::with_capacity(43 + payload.len());
    body.extend_from_slice(b"NRCL65\0\x01");
    body.push(opcode);
    body.extend_from_slice(&TEST_NONCE);
    body.extend_from_slice(&frontend().to_bytes());
    body.extend_from_slice(&sequence.to_le_bytes());
    body.extend_from_slice(payload);
    body
}
fn frame(opcode: u8, sequence: u16, payload: &[u8]) -> Vec<u8> {
    let body = body(opcode, sequence, payload);
    let mut wire = Vec::with_capacity(4 + body.len());
    wire.extend_from_slice(&(body.len() as u32).to_le_bytes());
    wire.extend_from_slice(&body);
    wire
}
fn arm_payload() -> Vec<u8> {
    let mut payload = Vec::with_capacity(65 * 36);
    // Fixture-only IDs arm scalar selection facts. No real session/token is manufactured.
    for id in 1..=65u32 {
        payload.extend_from_slice(&id.to_le_bytes());
        payload.extend_from_slice(&FROZEN_HASH);
    }
    payload
}
async fn reply(peer: &mut UnixStream) -> io::Result<Vec<u8>> {
    let mut header = [0; 4];
    peer.read_exact(&mut header).await?;
    let length = u32::from_le_bytes(header) as usize;
    if !(45..=4092).contains(&length) {
        return Err(io::Error::from(io::ErrorKind::InvalidData));
    }
    let mut wire = vec![0; length + 4];
    wire[..4].copy_from_slice(&header);
    peer.read_exact(&mut wire[4..]).await?;
    Ok(wire)
}
fn reply_header(
    wire: &[u8],
    opcode: u8,
    sequence: u16,
    request_bytes: u64,
    earlier_reply_bytes: u64,
) {
    assert_eq!(&wire[4..12], b"NRCL65\0\x01");
    assert_eq!(&wire[12..14], &[opcode, 0]);
    assert_eq!(&wire[14..30], &frontend().to_bytes());
    assert_eq!(
        u16::from_le_bytes(wire[30..32].try_into().unwrap()),
        sequence
    );
    assert_eq!(wire[32], 1);
    assert_eq!(
        u64::from_le_bytes(wire[33..41].try_into().unwrap()),
        request_bytes
    );
    assert_eq!(
        u64::from_le_bytes(wire[41..49].try_into().unwrap()),
        earlier_reply_bytes
    );
}

// At most three owned leaf artifacts plus one original private directory. Never recursive.
struct PrivateDirectory {
    path: PathBuf,
    identity: FileIdentity,
    leaves: [Option<(PathBuf, FileIdentity)>; 3],
}
impl PrivateDirectory {
    fn new() -> Self {
        let sequence = DIRECTORY_SEQUENCE.fetch_add(1, Ordering::Relaxed);
        let path = PathBuf::from(format!(
            "/tmp/nr-cp6a-{:x}-{sequence:x}",
            std::process::id()
        ));
        std::fs::DirBuilder::new()
            .mode(0o700)
            .create(&path)
            .unwrap();
        let identity = FileIdentity::of(&std::fs::symlink_metadata(&path).unwrap());
        Self {
            path,
            identity,
            leaves: std::array::from_fn(|_| None),
        }
    }
    fn socket_path(&self) -> PathBuf {
        self.path.join("c.sock")
    }
    fn track(&mut self, path: PathBuf) {
        let identity = FileIdentity::of(&std::fs::symlink_metadata(&path).unwrap());
        *self
            .leaves
            .iter_mut()
            .find(|slot| slot.is_none())
            .expect("fixed artifact capacity") = Some((path, identity));
    }
    fn cleanup(&mut self) -> io::Result<()> {
        // Only this exact directory owner may remove the fixture's own leaves.
        let parent = std::fs::symlink_metadata(&self.path)?;
        if !self.identity.matches(&parent) || !parent.is_dir() || parent.file_type().is_symlink() {
            return Err(io::Error::from(io::ErrorKind::InvalidData));
        }
        for leaf in &mut self.leaves {
            if let Some((path, identity)) = leaf {
                match std::fs::symlink_metadata(&*path) {
                    Ok(meta) if identity.matches(&meta) => {
                        std::fs::remove_file(&*path)?;
                    }
                    Ok(_) => continue,
                    Err(error) if error.kind() == io::ErrorKind::NotFound => {}
                    Err(error) => return Err(error),
                }
                *leaf = None;
            }
        }
        std::fs::remove_dir(&self.path)
    }
}
impl Drop for PrivateDirectory {
    fn drop(&mut self) {
        let _cleanup = self.cleanup();
    }
}
struct Fixture {
    // Field drop order closes original IO before the directory fallback.
    control: UnixPressureControl,
    controller: PressureController,
    _owner: PressureOwner,
    directory: PrivateDirectory,
}
impl Fixture {
    fn new(budget: Duration) -> Self {
        let deadline = Instant::now() + budget;
        let mut directory = PrivateDirectory::new();
        let (owner, controller) = PressureOwner::new(frontend(), deadline).unwrap();
        let control = match UnixPressureControl::bind(
            directory.socket_path(),
            frontend(),
            TEST_NONCE,
            deadline,
        ) {
            Ok(control) => control,
            Err(_) => panic!("fixture private socket bind failed"),
        };
        directory.track(directory.socket_path());
        Self {
            control,
            controller,
            _owner: owner,
            directory,
        }
    }
    fn close(&mut self) {
        self.control.close(&mut self.controller).unwrap();
        assert!(self.control.peer.is_none() && self.control.listener.is_none());
        assert!(!self.directory.socket_path().exists());
        self.directory.cleanup().unwrap();
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        let _cleanup = self.control.close(&mut self.controller);
    }
}

#[test]
fn fixed_request_shape_accepts_only_exact_opcode_payloads() {
    assert_eq!(
        (FRAME_CAP, BODY_CAP, REQUEST_HEADER, COMMAND_CAP),
        (4096, 4092, 43, 512)
    );
    assert_eq!(MAGIC, *b"NRCL65\0\x01");
    let payload = arm_payload();
    let wire = frame(1, 1, &payload);
    assert_eq!(wire.len(), 2387);
    assert_eq!(&wire[..4], &2383u32.to_le_bytes());
    match decode(&wire[4..], frontend(), &TEST_NONCE, 1) {
        Ok(Operation::Arm(targets)) => {
            for (slot, target) in targets.iter().enumerate() {
                assert_eq!(target.handshake_connection_id, slot as u32 + 1);
                assert_eq!(target.original_sql_sha256, FROZEN_HASH);
            }
        }
        _ => panic!("exact independent ARM frame refused"),
    }
    assert!(matches!(
        decode(&body(2, 1, &[64]), frontend(), &TEST_NONCE, 1),
        Ok(Operation::Snapshot(64))
    ));
    assert!(matches!(
        decode(&body(3, 1, &[0, 65]), frontend(), &TEST_NONCE, 1),
        Ok(Operation::Rows(0, 65))
    ));
    assert!(matches!(
        decode(&body(3, 1, &[64, 1]), frontend(), &TEST_NONCE, 1),
        Ok(Operation::Rows(64, 1))
    ));
    assert!(matches!(
        decode(&body(4, 1, &[0]), frontend(), &TEST_NONCE, 1),
        Ok(Operation::Joint(false))
    ));
    assert!(matches!(
        decode(&body(4, 1, &[1]), frontend(), &TEST_NONCE, 1),
        Ok(Operation::Joint(true))
    ));
    assert!(matches!(
        decode(&body(5, 1, &[63]), frontend(), &TEST_NONCE, 1),
        Ok(Operation::Release(63))
    ));
    assert!(matches!(
        decode(&body(6, 1, &[]), frontend(), &TEST_NONCE, 1),
        Ok(Operation::Exit)
    ));
    assert!(matches!(
        decode(&body(7, 1, &[]), frontend(), &TEST_NONCE, 1),
        Ok(Operation::Stop)
    ));
    let invalid = [
        (0, vec![]),
        (8, vec![]),
        (255, vec![]),
        (1, payload[..payload.len() - 1].to_vec()),
        (2, vec![]),
        (2, vec![65]),
        (2, vec![0, 0]),
        (3, vec![0, 0]),
        (3, vec![64, 2]),
        (3, vec![0, 65, 0]),
        (4, vec![]),
        (4, vec![2]),
        (4, vec![255]),
        (5, vec![64]),
        (5, vec![0, 0]),
        (6, vec![0]),
        (7, vec![0]),
    ];
    for (opcode, payload) in invalid {
        assert!(matches!(
            decode(&body(opcode, 1, &payload), frontend(), &TEST_NONCE, 1),
            Err(Class::Length)
        ));
    }
    let mut extra_arm = payload;
    extra_arm.push(0);
    assert!(matches!(
        decode(&body(1, 1, &extra_arm), frontend(), &TEST_NONCE, 1),
        Err(Class::Length)
    ));
}

#[test]
fn complete_header_identity_sequence_and_every_short_header_are_strict() {
    let original = body(2, 1, &[0]);
    for cut in 0..43 {
        assert!(matches!(
            decode(&original[..cut], frontend(), &TEST_NONCE, 1),
            Err(Class::Length)
        ));
    }
    for offset in (0..8).chain(9..43) {
        let mut changed = original.clone();
        changed[offset] ^= 1;
        assert!(
            matches!(
                decode(&changed, frontend(), &TEST_NONCE, 1),
                Err(Class::Identity)
            ),
            "offset={offset}"
        );
    }
    assert!(matches!(
        decode(&original, frontend(), &TEST_NONCE, 2),
        Err(Class::Identity)
    ));
    for sequence in [0, 2, u16::MAX] {
        assert!(matches!(
            decode(&body(2, sequence, &[0]), frontend(), &TEST_NONCE, 1),
            Err(Class::Identity)
        ));
    }
    let mut nil = original.clone();
    nil[25..41].fill(0);
    assert!(matches!(
        decode(&nil, frontend(), &TEST_NONCE, 1),
        Err(Class::Identity)
    ));
    let mut uuid_v4 = original;
    uuid_v4[31] = 0x41;
    assert!(matches!(
        decode(&uuid_v4, frontend(), &TEST_NONCE, 1),
        Err(Class::Identity)
    ));
}

#[test]
fn bind_uses_same_euid_private_directory_and_socket_and_close_is_exact() {
    run(async {
        let mut fixture = Fixture::new(TEST_BUDGET);
        let directory = std::fs::symlink_metadata(&fixture.directory.path).unwrap();
        let socket = std::fs::symlink_metadata(fixture.directory.socket_path()).unwrap();
        assert_eq!(directory.permissions().mode() & 0o7777, 0o700);
        assert_eq!(socket.permissions().mode() & 0o7777, 0o600);
        assert_eq!(socket.uid(), directory.uid());
        assert_eq!(socket.uid(), fixture.control.uid);
        assert!(socket.file_type().is_socket());
        let peer = UnixStream::connect(fixture.directory.socket_path())
            .await
            .unwrap();
        assert_eq!(peer.peer_cred().unwrap().uid(), fixture.control.uid);
        fixture.close();
        drop(peer);
        assert!(!fixture.control.completed_and_closed());
    });
}

#[test]
fn bind_refuses_existing_file_socket_and_symlink_without_unlinking() {
    run(async {
        for kind in 0..3 {
            let mut directory = PrivateDirectory::new();
            let path = directory.socket_path();
            let original_listener = match kind {
                0 => {
                    std::fs::write(&path, b"fixture-owned canary").unwrap();
                    None
                }
                1 => Some(UnixListener::bind(&path).unwrap()),
                _ => {
                    symlink("absent-target", &path).unwrap();
                    None
                }
            };
            directory.track(path.clone());
            let original = std::fs::symlink_metadata(&path).unwrap();
            let error = match UnixPressureControl::bind(
                path.clone(),
                frontend(),
                TEST_NONCE,
                Instant::now() + TEST_BUDGET,
            ) {
                Ok(_) => panic!("existing artifact unexpectedly replaced"),
                Err((error, cleanup)) => {
                    assert!(cleanup.is_none());
                    error
                }
            };
            assert!(matches!(error.class, Class::Path));
            assert_eq!(
                error.cause.as_ref().unwrap().kind(),
                io::ErrorKind::AlreadyExists
            );
            assert!(
                FileIdentity::of(&original).matches(&std::fs::symlink_metadata(&path).unwrap())
            );
            if kind == 0 {
                assert_eq!(std::fs::read(&path).unwrap(), b"fixture-owned canary");
            }
            drop(original_listener);
            directory.cleanup().unwrap();
        }
    });
}

#[test]
fn bind_refuses_nonprivate_parent_symlink_parent_zero_nonce_and_expired_clock() {
    run(async {
        let mut directory = PrivateDirectory::new();
        let path = directory.socket_path();
        std::fs::set_permissions(&directory.path, std::fs::Permissions::from_mode(0o755)).unwrap();
        assert!(matches!(
            UnixPressureControl::bind(
                path.clone(),
                frontend(),
                TEST_NONCE,
                Instant::now() + TEST_BUDGET
            ),
            Err((
                ControlError {
                    class: Class::Path,
                    ..
                },
                None
            ))
        ));
        std::fs::set_permissions(&directory.path, std::fs::Permissions::from_mode(0o700)).unwrap();
        assert!(matches!(
            UnixPressureControl::bind(
                path.clone(),
                frontend(),
                [0; 16],
                Instant::now() + TEST_BUDGET
            ),
            Err((
                ControlError {
                    class: Class::Identity,
                    ..
                },
                None
            ))
        ));
        assert!(matches!(
            UnixPressureControl::bind(path.clone(), frontend(), TEST_NONCE, Instant::now()),
            Err((
                ControlError {
                    class: Class::Deadline,
                    ..
                },
                None
            ))
        ));
        let alias = directory.path.join("alias");
        symlink(&directory.path, &alias).unwrap();
        directory.track(alias.clone());
        assert!(matches!(
            UnixPressureControl::bind(
                alias.join("c.sock"),
                frontend(),
                TEST_NONCE,
                Instant::now() + TEST_BUDGET
            ),
            Err((
                ControlError {
                    class: Class::Path,
                    ..
                },
                None
            ))
        ));
        assert!(std::fs::symlink_metadata(&path).is_err());
        directory.cleanup().unwrap();
    });
}

#[test]
fn exact_cleanup_preserves_a_replacement_and_retains_cleanup_error() {
    run(async {
        let mut fixture = Fixture::new(TEST_BUDGET);
        let path = fixture.directory.socket_path();
        let held = fixture.directory.path.join("held.sock");
        std::fs::rename(&path, &held).unwrap();
        fixture.directory.track(held.clone());
        std::fs::write(&path, b"test replacement retained").unwrap();
        fixture.directory.track(path.clone());
        let replacement = FileIdentity::of(&std::fs::symlink_metadata(&path).unwrap());
        let error = fixture.control.close(&mut fixture.controller).unwrap_err();
        assert!(matches!(error.class, Class::Cleanup));
        assert!(matches!(error.stage, Stage::Cleanup));
        assert!(error.cause.is_some());
        assert!(!fixture.control.closed);
        assert!(replacement.matches(&std::fs::symlink_metadata(&path).unwrap()));
        assert_eq!(std::fs::read(&path).unwrap(), b"test replacement retained");
        assert!(held.exists());
        assert!(fixture.control.peer.is_none() && fixture.control.listener.is_none());
        // The independent fixture owns both replacement and moved original; remove only these exact inodes.
        fixture.directory.cleanup().unwrap();
    });
}

#[test]
fn exact_cleanup_rejects_parent_mode_change_and_can_retry_after_restoration() {
    run(async {
        let mut fixture = Fixture::new(TEST_BUDGET);
        let path = fixture.directory.socket_path();
        let original = FileIdentity::of(&std::fs::symlink_metadata(&path).unwrap());
        std::fs::set_permissions(
            &fixture.directory.path,
            std::fs::Permissions::from_mode(0o755),
        )
        .unwrap();
        let error = fixture.control.close(&mut fixture.controller).unwrap_err();
        assert!(matches!(error.class, Class::Cleanup));
        assert!(original.matches(&std::fs::symlink_metadata(&path).unwrap()));
        std::fs::set_permissions(
            &fixture.directory.path,
            std::fs::Permissions::from_mode(0o700),
        )
        .unwrap();
        fixture.close();
    });
}

#[test]
fn oversize_frame_is_rejected_after_actual_header_before_any_body_is_read() {
    run(async {
        for declared in [4093u32, u32::MAX] {
            let mut fixture = Fixture::new(TEST_BUDGET);
            let path = fixture.directory.socket_path();
            let mut sent = declared.to_le_bytes().to_vec();
            sent.extend_from_slice(&[0xde, 0xad, 0xbe, 0xef]);
            let peer_work = async {
                let mut peer = UnixStream::connect(path).await.unwrap();
                peer.write_all(&sent).await.unwrap();
                peer
            };
            let peer_work = bounded_peer(fixture.control.deadline, peer_work);
            let (result, peer) =
                tokio::join!(fixture.control.run(&mut fixture.controller), peer_work);
            let error = result.unwrap_err();
            assert!(matches!(error.class, Class::Length));
            assert!(matches!(error.stage, Stage::Header));
            assert_eq!(error.request.declared, Some(u64::from(declared) + 4));
            assert_eq!(error.request.observed, 4);
            assert_eq!(error.request.sha256, digest(&declared.to_le_bytes()));
            assert_eq!(fixture.control.request_bytes, 4);
            assert_eq!(error.response.observed, 0);
            assert_eq!(fixture.control.commands, 0);
            fixture.close();
            drop(peer);
        }
    });
}

#[test]
fn full_4096_wire_is_read_then_trailing_payload_is_refused_at_decode() {
    run(async {
        let mut fixture = Fixture::new(TEST_BUDGET);
        let path = fixture.directory.socket_path();
        let mut payload = body(2, 1, &[0]);
        payload.resize(4092, 0);
        let mut sent = 4092u32.to_le_bytes().to_vec();
        sent.extend_from_slice(&payload);
        let peer_work = async {
            let mut peer = UnixStream::connect(path).await.unwrap();
            peer.write_all(&sent).await.unwrap();
            peer
        };
        let peer_work = bounded_peer(fixture.control.deadline, peer_work);
        let (result, peer) = tokio::join!(fixture.control.run(&mut fixture.controller), peer_work);
        let error = result.unwrap_err();
        assert!(matches!(error.class, Class::Length));
        assert!(matches!(error.stage, Stage::Decode));
        assert_eq!(error.request.declared, Some(4096));
        assert_eq!(error.request.observed, 4096);
        assert_eq!(error.request.sha256, digest(&sent));
        assert_eq!(fixture.control.commands, 0);
        fixture.close();
        drop(peer);
    });
}

#[test]
fn partial_eof_preserves_actual_header_and_body_prefixes_and_finite_diagnostics() {
    run(async {
        let complete = frame(2, 1, &[0]);
        for cut in [2, 4 + 17] {
            let mut fixture = Fixture::new(TEST_BUDGET);
            let path = fixture.directory.socket_path();
            let sent = &complete[..cut];
            let peer_work = async {
                let mut peer = UnixStream::connect(path).await.unwrap();
                peer.write_all(sent).await.unwrap();
                // Keep the actual peer alive for the mandatory UID check; only the
                // write half ends, so the original read observes this exact EOF.
                peer.shutdown().await.unwrap();
                peer
            };
            let peer_work = bounded_peer(fixture.control.deadline, peer_work);
            let (result, peer) =
                tokio::join!(fixture.control.run(&mut fixture.controller), peer_work);
            let error = result.unwrap_err();
            assert!(matches!(error.class, Class::Eof));
            assert_eq!(error.request.observed, cut as u64);
            assert_eq!(error.request.sha256, digest(sent));
            assert_eq!(
                error.request.declared,
                if cut < 4 { None } else { Some(48) }
            );
            assert!(if cut < 4 {
                matches!(error.stage, Stage::Header)
            } else {
                matches!(error.stage, Stage::Body)
            });
            let display = format!("{error}");
            assert!(display.len() < 1024);
            assert_eq!(display, format!("{error:?}"));
            assert!(!display.contains("fixture-owned canary"));
            assert!(
                fixture
                    .control
                    .request
                    .iter()
                    .chain(fixture.control.reply.iter())
                    .all(|byte| *byte == 0)
            );
            fixture.close();
            drop(peer);
        }
    });
}

#[test]
fn arm_and_more_than_sixteen_snapshots_do_not_create_writer_exit_or_stop_success() {
    run(async {
        let mut fixture = Fixture::new(TEST_BUDGET);
        let path = fixture.directory.socket_path();
        let peer_work = async {
            let mut peer = UnixStream::connect(path).await.unwrap();
            let arm = frame(1, 1, &arm_payload());
            peer.write_all(&arm).await.unwrap();
            let armed = reply(&mut peer).await.unwrap();
            assert_eq!(armed.len(), 49);
            reply_header(&armed, 1, 1, 2387, 0);
            let mut request_bytes = 2387;
            let mut response_bytes = 49;
            for sequence in 2..=21u16 {
                let snapshot = frame(2, sequence, &[64]);
                peer.write_all(&snapshot).await.unwrap();
                request_bytes += 48;
                let facts = reply(&mut peer).await.unwrap();
                reply_header(&facts, 2, sequence, request_bytes, response_bytes);
                assert_eq!(facts.len(), 222);
                assert_eq!(&facts[49..53], &[64, 1, 0, 1]);
                assert_eq!(u32::from_le_bytes(facts[53..57].try_into().unwrap()), 65);
                assert_eq!(&facts[218..222], &[0; 4]);
                response_bytes += facts.len() as u64;
            }
            peer.write_all(&frame(7, 22, &[])).await.unwrap();
            peer
        };
        let peer_work = bounded_peer(fixture.control.deadline, peer_work);
        let (result, mut peer) =
            tokio::join!(fixture.control.run(&mut fixture.controller), peer_work);
        let error = result.unwrap_err();
        assert!(matches!(error.class, Class::State));
        assert!(matches!(error.stage, Stage::Apply));
        assert_eq!(fixture.control.commands, 22);
        assert!(!fixture.control.completed_and_closed());
        for slot in 0..65 {
            let facts = fixture.controller.snapshot(slot).unwrap();
            assert_eq!(facts.phase, Phase::Armed);
            assert_eq!(facts.failure, Some(Failure::Transition));
            assert!(
                !facts.writer_attached
                    && !facts.writer_destructor_returned
                    && !facts.physical_shutdown_completed
            );
            assert!(
                facts.connection.is_none()
                    && facts.statement.is_none()
                    && facts.cancel_receipt.is_none()
            );
        }
        fixture.close();
        let mut missing_reply = [0; 4];
        assert_eq!(peer.read(&mut missing_reply).await.unwrap(), 0);
    });
}

#[test]
fn real_peer_replay_is_refused_and_run_is_once_only() {
    run(async {
        let mut fixture = Fixture::new(TEST_BUDGET);
        let path = fixture.directory.socket_path();
        let request = frame(2, 1, &[0]);
        let peer_work = async {
            let mut peer = UnixStream::connect(path).await.unwrap();
            peer.write_all(&request).await.unwrap();
            let first = reply(&mut peer).await.unwrap();
            reply_header(&first, 2, 1, 48, 0);
            peer.write_all(&request).await.unwrap();
            peer
        };
        let peer_work = bounded_peer(fixture.control.deadline, peer_work);
        let (result, peer) = tokio::join!(fixture.control.run(&mut fixture.controller), peer_work);
        let error = result.unwrap_err();
        assert!(matches!(error.class, Class::Identity));
        assert!(matches!(error.stage, Stage::Decode));
        assert_eq!(fixture.control.commands, 1);
        assert_eq!(error.request.observed, 48);
        assert_eq!(error.request.sha256, digest(&request));
        let again = fixture
            .control
            .run(&mut fixture.controller)
            .await
            .unwrap_err();
        assert!(matches!(again.class, Class::State));
        assert_eq!(fixture.control.commands, 1);
        fixture.close();
        drop(peer);
    });
}

#[test]
fn original_absolute_deadline_covers_accept_and_after_an_earlier_reply() {
    run(async {
        let mut fixture = Fixture::new(Duration::from_millis(80));
        let original = fixture.control.deadline;
        let result = fixture
            .control
            .run(&mut fixture.controller)
            .await
            .unwrap_err();
        assert!(matches!(result.class, Class::Deadline));
        assert!(matches!(result.stage, Stage::Accept));
        assert_eq!(result.request.observed, 0);
        assert_eq!(fixture.control.deadline, original);
        fixture.close();
        let mut fixture = Fixture::new(Duration::from_millis(180));
        let original = fixture.control.deadline;
        let path = fixture.directory.socket_path();
        let late = frame(2, 2, &[0]);
        let peer_work = async {
            let mut peer = UnixStream::connect(path).await.unwrap();
            peer.write_all(&frame(2, 1, &[0])).await.unwrap();
            reply(&mut peer).await.unwrap();
            tokio::time::sleep(Duration::from_millis(60)).await;
            peer.write_all(&late[..2]).await.unwrap();
            // This coherent request would finish under a refreshed clock, but misses the original one.
            tokio::time::sleep_until(tokio::time::Instant::from_std(
                original + Duration::from_millis(20),
            ))
            .await;
            peer.write_all(&late[2..]).await.unwrap();
            peer
        };
        let peer_work = bounded_peer(fixture.control.deadline, peer_work);
        let (result, peer) = tokio::join!(fixture.control.run(&mut fixture.controller), peer_work);
        let error = result.unwrap_err();
        assert!(matches!(error.class, Class::Deadline));
        assert!(matches!(error.stage, Stage::Header));
        assert_eq!(fixture.control.commands, 1);
        assert_eq!(fixture.control.deadline, original);
        assert_eq!(error.request.observed, 2);
        assert_eq!(error.request.declared, None);
        assert_eq!(error.request.sha256, digest(&late[..2]));
        assert_eq!(fixture.control.request_bytes, 50);
        fixture.close();
        drop(peer);
    });
}

struct FormatterCanary {
    identity: Arc<()>,
    formatted: Arc<AtomicUsize>,
    dropped: Arc<AtomicUsize>,
}
impl fmt::Debug for FormatterCanary {
    fn fmt(&self, _: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.formatted.fetch_add(1, Ordering::SeqCst);
        panic!("arbitrary IO formatter must not run")
    }
}
impl fmt::Display for FormatterCanary {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self, f)
    }
}
impl std::error::Error for FormatterCanary {}
impl Drop for FormatterCanary {
    fn drop(&mut self) {
        self.dropped.fetch_add(1, Ordering::SeqCst);
    }
}

#[test]
fn finite_control_error_retains_actual_io_source_without_calling_its_formatter() {
    let identity = Arc::new(());
    let formatted = Arc::new(AtomicUsize::new(0));
    let dropped = Arc::new(AtomicUsize::new(0));
    let original = FormatterCanary {
        identity: Arc::clone(&identity),
        formatted: Arc::clone(&formatted),
        dropped: Arc::clone(&dropped),
    };
    let error = ControlError::startup(Class::Io, Some(io::Error::other(original)));
    let display = format!("{error}");
    assert_eq!(display, format!("{error:?}"));
    assert!(display.len() < 1024);
    let source = std::error::Error::source(&error)
        .unwrap()
        .downcast_ref::<io::Error>()
        .unwrap();
    let retained = source
        .get_ref()
        .unwrap()
        .downcast_ref::<FormatterCanary>()
        .unwrap();
    assert!(Arc::ptr_eq(&retained.identity, &identity));
    assert_eq!(formatted.load(Ordering::SeqCst), 0);
    assert_eq!(dropped.load(Ordering::SeqCst), 0);
    drop(error);
    assert_eq!(dropped.load(Ordering::SeqCst), 1);
}

#[test]
fn actual_poll_panic_payload_is_retained_by_identity_and_never_formatted() {
    run(async {
        let identity = Arc::new(());
        let formatted = Arc::new(AtomicUsize::new(0));
        let dropped = Arc::new(AtomicUsize::new(0));
        let mut original = Some(FormatterCanary {
            identity: Arc::clone(&identity),
            formatted: Arc::clone(&formatted),
            dropped: Arc::clone(&dropped),
        });
        let work = std::future::poll_fn::<(), _>(move |_| {
            std::panic::panic_any(original.take().expect("one actual panic poll"))
        });
        let payload = match catch_panic(work).await {
            Err(payload) => payload,
            Ok(()) => panic!("actual panic unexpectedly escaped capture"),
        };
        let error = ControlError::startup(
            Class::Panic,
            Some(io::Error::other(OriginalPanic {
                _payload: Mutex::new(payload),
            })),
        );
        assert!(format!("{error}").len() < 1024);
        assert!(format!("{error:?}").len() < 1024);
        let retained = error
            .cause
            .as_ref()
            .unwrap()
            .get_ref()
            .unwrap()
            .downcast_ref::<OriginalPanic>()
            .unwrap();
        let payload = retained._payload.lock().unwrap();
        let original = payload.downcast_ref::<FormatterCanary>().unwrap();
        assert!(Arc::ptr_eq(&original.identity, &identity));
        assert_eq!(formatted.load(Ordering::SeqCst), 0);
        assert_eq!(dropped.load(Ordering::SeqCst), 0);
        drop(payload);
        drop(error);
        assert_eq!(dropped.load(Ordering::SeqCst), 1);
    });
}

#[test]
fn actual_peer_command_limit_stops_before_reading_command_513() {
    run(async {
        let mut fixture = Fixture::new(TEST_BUDGET);
        let path = fixture.directory.socket_path();
        let peer_work = async {
            let mut peer = UnixStream::connect(path).await.unwrap();
            let mut response_bytes = 0;
            for sequence in 1..=512u16 {
                peer.write_all(&frame(2, sequence, &[0])).await.unwrap();
                let facts = reply(&mut peer).await.unwrap();
                reply_header(
                    &facts,
                    2,
                    sequence,
                    u64::from(sequence) * 48,
                    response_bytes,
                );
                response_bytes += facts.len() as u64;
            }
            // The control owner still owns the actual peer until its explicit close.
            peer.write_all(&frame(2, 513, &[0])).await.unwrap();
            peer
        };
        let peer_work = bounded_peer(fixture.control.deadline, peer_work);
        let (result, peer) = tokio::join!(fixture.control.run(&mut fixture.controller), peer_work);
        let error = result.unwrap_err();
        assert!(matches!(error.class, Class::CommandLimit));
        assert_eq!(fixture.control.commands, 512);
        assert_eq!(fixture.control.request_bytes, 512 * 48);
        assert_eq!(fixture.control.response_bytes, 512 * 222);
        assert_eq!(error.request.sha256, digest(&frame(2, 512, &[0])));
        assert!(!fixture.control.completed_and_closed());
        fixture.close();
        drop(peer);
    });
}
