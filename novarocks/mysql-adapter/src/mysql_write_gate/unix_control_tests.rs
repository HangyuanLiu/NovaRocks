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

//! Component-only child module of unix_control; no original MySQL owner is present.
//! The parent keeps ControlOwner while a borrowed run future is selected or timed out.
//! Only actual Unix clients are spawned, and all original handles are joined.

use super::*;
use std::os::unix::fs::DirBuilderExt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use tokio::sync::oneshot;
use tokio::task::JoinSet;

const NONCE: [u8; 16] = [9; 16];
const SQL_HASH: [u8; 32] = [7; 32];
const COMPONENT_BUDGET: Duration = Duration::from_secs(3);
static NEXT_DIRECTORY: AtomicU64 = AtomicU64::new(1);

fn invalid(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}
fn require(condition: bool, message: &'static str) -> io::Result<()> {
    if condition {
        Ok(())
    } else {
        Err(invalid(message))
    }
}
fn digest(bytes: &[u8]) -> [u8; 32] {
    Sha256::digest(bytes).into()
}

// Test failures retain a bounded streamed digest; no input/nonce or panic text is formatted.
#[derive(Debug)]
struct TestCause {
    class: &'static str,
    bytes: usize,
    hash: [u8; 32],
}
impl std::fmt::Display for TestCause {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "component failure: class={} bytes={} sha256={:?}",
            self.class, self.bytes, self.hash
        )
    }
}
impl std::error::Error for TestCause {}
struct HashWriter {
    bytes: usize,
    hash: Sha256,
}
impl std::fmt::Write for HashWriter {
    fn write_str(&mut self, value: &str) -> std::fmt::Result {
        self.bytes = self.bytes.saturating_add(value.len());
        self.hash.update(value.as_bytes());
        Ok(())
    }
}
fn finite_error(class: &'static str, error: &dyn std::fmt::Display) -> io::Error {
    use std::fmt::Write;
    let mut writer = HashWriter {
        bytes: 0,
        hash: Sha256::new(),
    };
    let _ = write!(writer, "{error}");
    io::Error::other(TestCause {
        class,
        bytes: writer.bytes,
        hash: writer.hash.finalize().into(),
    })
}
async fn catch_fixture<F: Future<Output = io::Result<()>>>(future: F) -> io::Result<()> {
    let mut future = Box::pin(future);
    std::future::poll_fn(|cx| {
        match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| future.as_mut().poll(cx))) {
            Ok(value) => value,
            Err(payload) => {
                let text = payload
                    .downcast_ref::<&str>()
                    .copied()
                    .or_else(|| payload.downcast_ref::<String>().map(String::as_str));
                let error = io::Error::other(TestCause {
                    class: "parent-panic",
                    bytes: text.map_or(0, str::len),
                    hash: text.map_or_else(|| digest(&[]), |value| digest(value.as_bytes())),
                });
                Poll::Ready(Err(error))
            }
        }
    })
    .await
}

// This fixture owns at most two private directories and two socket inodes.
// Explicit cleanup refuses replacements; no remove_dir_all or detached task is used.
struct Artifact {
    path: PathBuf,
    identity: FileIdentity,
    directory: bool,
}
struct Fixture {
    owner: UnixMysqlWriteControl,
    clients: JoinSet<io::Result<()>>,
    spawned: usize,
    joined: usize,
    artifacts: [Option<Artifact>; 4],
    replacement: Option<UnixListener>,
    expected_close_refusal: Option<io::Error>,
}
impl Fixture {
    fn new() -> io::Result<Self> {
        Self::with_budget(COMPONENT_BUDGET)
    }
    fn with_budget(budget: Duration) -> io::Result<Self> {
        let absolute = Instant::now() + budget;
        let serial = NEXT_DIRECTORY.fetch_add(1, Ordering::Relaxed);
        let directory = PathBuf::from(format!("/tmp/nr-uc-{:x}-{serial:x}", std::process::id()));
        std::fs::DirBuilder::new().mode(0o700).create(&directory)?;
        let mut artifact = Artifact {
            path: directory.clone(),
            identity: FileIdentity::of(&std::fs::symlink_metadata(&directory)?),
            directory: true,
        };
        if let Err(primary) =
            std::fs::set_permissions(&directory, std::fs::Permissions::from_mode(0o700))
        {
            let cleanup = remove_artifact(&mut artifact).err();
            return Err(io::Error::other(FixtureExit {
                primary: Some(primary),
                cleanup,
            }));
        }
        let path = directory.join("gate.sock");
        let deadline = absolute;
        let (mut owner, hub) = match UnixMysqlWriteControl::bind(
            path.clone(),
            FrontendProcessId::new_v7(),
            NONCE,
            deadline,
        ) {
            Ok(value) => value,
            Err(primary) => {
                let cleanup = remove_artifact(&mut artifact).err();
                return Err(io::Error::other(FixtureExit {
                    primary: Some(finite_error("bind", &primary)),
                    cleanup,
                }));
            }
        };
        drop(hub); // No extra Hub authority/alias is needed by the client fixture.
        let socket_identity = match owner.path.socket {
            Some(value) => value,
            None => {
                let cleanup = owner
                    .close()
                    .err()
                    .map(|error| finite_error("bind-close", &error));
                let parent_cleanup = remove_artifact(&mut artifact).err();
                return Err(io::Error::other(FixtureExit {
                    primary: Some(invalid("missing original socket identity")),
                    cleanup: cleanup.or(parent_cleanup),
                }));
            }
        };
        Ok(Self {
            owner,
            clients: JoinSet::new(),
            spawned: 0,
            joined: 0,
            artifacts: [
                Some(artifact),
                Some(Artifact {
                    path,
                    identity: socket_identity,
                    directory: false,
                }),
                None,
                None,
            ],
            replacement: None,
            expected_close_refusal: None,
        })
    }
    fn spawn<F: Future<Output = io::Result<()>> + Send + 'static>(&mut self, future: F) {
        self.clients.spawn(future);
        self.spawned += 1;
    }
    fn path(&self) -> PathBuf {
        self.owner.path.path.clone()
    }
    fn frontend(&self) -> FrontendProcessId {
        self.owner.frontend
    }
    fn remember(&mut self, slot: usize, path: PathBuf, directory: bool) -> io::Result<()> {
        require(
            self.artifacts[slot].is_none(),
            "artifact slot already owned",
        )?;
        let identity = FileIdentity::of(&std::fs::symlink_metadata(&path)?);
        self.artifacts[slot] = Some(Artifact {
            path,
            identity,
            directory,
        });
        Ok(())
    }
    async fn settle(&mut self, primary: io::Result<()>) -> io::Result<()> {
        // The borrowed run future is already dropped before this call.
        if primary.is_err() {
            self.owner.fail(GateFailure::Transition);
        }
        self.owner.stop();
        let mut cleanup = match self.owner.close() {
            Ok(()) if self.expected_close_refusal.is_some() => {
                Some(invalid("expected exact-inode refusal disappeared"))
            }
            Ok(()) => None,
            Err(error) => {
                let cause = finite_error("control-close", &error);
                match &self.expected_close_refusal {
                    Some(expected) => {
                        let expected = expected
                            .get_ref()
                            .and_then(|value| value.downcast_ref::<TestCause>());
                        let actual = cause
                            .get_ref()
                            .and_then(|value| value.downcast_ref::<TestCause>());
                        if matches!((expected,actual),(Some(left),Some(right)) if left.bytes==right.bytes && left.hash==right.hash)
                            && matches!(error.class, ControlClass::Cleanup)
                        {
                            None
                        } else {
                            Some(cause)
                        }
                    }
                    None => Some(cause),
                }
            }
        };
        self.clients.abort_all();
        while let Some(result) = self.clients.join_next().await {
            self.joined += 1;
            let error = match result {
                Ok(Ok(())) => None,
                Ok(Err(error)) => Some(finite_error("client-io", &error)),
                Err(error) if error.is_cancelled() => None, // Requested abort, still actually joined.
                Err(error) => Some(finite_error("client-join", &error)),
            };
            if cleanup.is_none() {
                cleanup = error;
            }
        }
        if self.joined != self.spawned && cleanup.is_none() {
            cleanup = Some(invalid("not every original client handle joined"));
        }
        drop(self.replacement.take());
        // Remove sockets before their exact private parent directories.
        for directory in [false, true] {
            for artifact in self
                .artifacts
                .iter_mut()
                .flatten()
                .filter(|entry| entry.directory == directory)
            {
                if let Err(error) = remove_artifact(artifact) {
                    if cleanup.is_none() {
                        cleanup = Some(finite_error("artifact-cleanup", &error));
                    }
                }
            }
        }
        match (primary.err(), cleanup) {
            (None, None) => Ok(()),
            (primary, cleanup) => Err(io::Error::other(FixtureExit { primary, cleanup })),
        }
    }
}
#[derive(Debug)]
struct FixtureExit {
    primary: Option<io::Error>,
    cleanup: Option<io::Error>,
}
impl std::fmt::Display for FixtureExit {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if let Some(primary) = &self.primary {
            write!(f, "primary: {primary}")?;
        }
        if let Some(cleanup) = &self.cleanup {
            write!(f, "; cleanup: {cleanup}")?;
        }
        Ok(())
    }
}
impl std::error::Error for FixtureExit {}
fn remove_artifact(artifact: &mut Artifact) -> io::Result<()> {
    let metadata = match std::fs::symlink_metadata(&artifact.path) {
        Ok(value) => value,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(error),
    };
    require(
        artifact.identity.matches(&metadata),
        "fixture refuses changed inode",
    )?;
    require(
        if artifact.directory {
            metadata.is_dir()
        } else {
            metadata.file_type().is_socket()
        },
        "fixture refuses changed file type",
    )?;
    if artifact.directory {
        std::fs::remove_dir(&artifact.path)
    } else {
        std::fs::remove_file(&artifact.path)
    }
}
macro_rules! fixture {
    ($fixture:ident, $body:block) => {{
        let mut $fixture = Fixture::new()?;
        let watchdog = tokio::time::Instant::from_std($fixture.owner.deadline + Duration::from_secs(1));
        let primary = match tokio::time::timeout_at(watchdog, catch_fixture(async $body)).await {
            Ok(result) => result,
            Err(_) => {
                $fixture.owner.fail(GateFailure::Deadline);
                Err(invalid("parent fixture absolute watchdog"))
            }
        };
        $fixture.settle(primary).await
    }};
}

macro_rules! fixture_budget {
    ($fixture:ident, $budget:expr, $body:block) => {{
        let mut $fixture = Fixture::with_budget($budget)?;
        let watchdog = tokio::time::Instant::from_std($fixture.owner.deadline + Duration::from_secs(1));
        let primary = match tokio::time::timeout_at(watchdog, catch_fixture(async $body)).await {
            Ok(result) => result,
            Err(_) => { $fixture.owner.fail(GateFailure::Deadline); Err(invalid("parent fixture absolute watchdog")) }
        };
        $fixture.settle(primary).await
    }};
}

fn tlv(body: &mut Vec<u8>, tag: u8, value: &[u8]) {
    assert!(body.len() + 3 + value.len() <= BODY_CAP);
    body.push(tag);
    body.extend_from_slice(&(value.len() as u16).to_le_bytes());
    body.extend_from_slice(value);
}
fn body(opcode: u8, frontend: FrontendProcessId, nonce: [u8; 16]) -> Vec<u8> {
    let mut body = Vec::with_capacity(93);
    body.extend_from_slice(&[1, opcode]);
    tlv(&mut body, 1, &frontend.to_bytes());
    tlv(&mut body, 2, &nonce);
    if opcode == ARM {
        tlv(&mut body, 3, &71u32.to_le_bytes());
        tlv(&mut body, 4, &SQL_HASH);
        tlv(&mut body, 5, &2u64.to_le_bytes());
    }
    body
}
fn wire(body: &[u8]) -> Vec<u8> {
    assert!(body.len() <= BODY_CAP);
    let mut wire = Vec::with_capacity(body.len() + 4);
    wire.extend_from_slice(&(body.len() as u32).to_le_bytes());
    wire.extend_from_slice(body);
    wire
}
#[derive(Clone, Copy)]
struct Reply {
    opcode: u8,
    commands: u8,
    request_bytes: u64,
    earlier_response_bytes: u64,
    explicit_stop: bool,
    used_arm: bool,
    stopped: bool,
    hash: [u8; 32],
    wire_bytes: u64,
}
async fn read_reply(peer: &mut UnixStream, frontend: FrontendProcessId) -> io::Result<Reply> {
    let mut header = [0; 4];
    peer.read_exact(&mut header).await?;
    let length = u32::from_le_bytes(header) as usize;
    require(length <= BODY_CAP, "reply cap before body read")?;
    // This fixture has no original MySQL writer: the exact no-gate reply body is 43 bytes.
    require(length == 43, "unexpected scoped/no-gate reply layout")?;
    let mut bytes = [0; 43];
    peer.read_exact(&mut bytes).await?;
    require(bytes[0] == 1 && bytes[2] == 0, "reply version/status")?;
    require(
        bytes[3..19] == frontend.to_bytes() && bytes[19] == 1,
        "reply actual identity/one peer",
    )?;
    require(
        bytes[37..40].iter().all(|value| *value <= 1),
        "reply canonical booleans",
    )?;
    require(
        bytes[40..43] == [0, 0, 0],
        "reply cannot claim failure-free original writer exit or gate",
    )?;
    let mut hash = Sha256::new();
    hash.update(header);
    hash.update(bytes);
    Ok(Reply {
        opcode: bytes[1],
        commands: bytes[20],
        request_bytes: u64::from_le_bytes(bytes[21..29].try_into().unwrap()),
        earlier_response_bytes: u64::from_le_bytes(bytes[29..37].try_into().unwrap()),
        explicit_stop: bytes[37] == 1,
        used_arm: bytes[38] == 1,
        stopped: bytes[39] == 1,
        hash: hash.finalize().into(),
        wire_bytes: (length + 4) as u64,
    })
}
async fn send_command(
    peer: &mut UnixStream,
    frontend: FrontendProcessId,
    opcode: u8,
) -> io::Result<Reply> {
    peer.write_all(&wire(&body(opcode, frontend, NONCE)))
        .await?;
    let reply = read_reply(peer, frontend).await?;
    require(reply.opcode == opcode, "reply opcode echo")?;
    Ok(reply)
}
fn prefix(summary: PrefixSummary, observed: &[u8], declared: Option<u64>) -> io::Result<()> {
    require(
        summary.observed_bytes == observed.len() as u64,
        "actual prefix count",
    )?;
    require(
        summary.declared_wire_bytes == declared,
        "actual declared prefix length",
    )?;
    require(
        summary.sha256 == digest(observed),
        "actual streamed prefix digest",
    )
}
async fn receive<T>(receiver: oneshot::Receiver<T>) -> io::Result<T> {
    receiver
        .await
        .map_err(|_| invalid("client receipt missing"))
}

#[tokio::test]
async fn actual_one_peer_arm_snapshot_stop_is_only_protocol_completion() -> io::Result<()> {
    fixture!(fixture, {
        let path = fixture.path();
        let frontend = fixture.frontend();
        let (tx, rx) = oneshot::channel();
        fixture.spawn(async move {
            let mut peer = UnixStream::connect(&path).await?;
            let arm = send_command(&mut peer, frontend, ARM).await?;
            require(
                arm.used_arm && !arm.stopped && !arm.explicit_stop && arm.commands == 1,
                "arm facts",
            )?;
            require(
                arm.earlier_response_bytes == 0,
                "reply before actual write accounting",
            )?;
            // The listening FD is closed after the first accept; no second peer is admitted.
            require(
                UnixStream::connect(&path).await.is_err(),
                "second peer connected after first reply",
            )?;
            let snapshot = send_command(&mut peer, frontend, SNAPSHOT).await?;
            require(
                snapshot.commands == 2 && snapshot.earlier_response_bytes == arm.wire_bytes,
                "snapshot accounting",
            )?;
            let stop = send_command(&mut peer, frontend, STOP).await?;
            require(
                stop.explicit_stop && stop.stopped && stop.used_arm && stop.commands == 3,
                "stop facts",
            )?;
            require(
                stop.earlier_response_bytes == arm.wire_bytes + snapshot.wire_bytes,
                "stop write accounting",
            )?;
            tx.send(stop)
                .map_err(|_| invalid("parent dropped reply receipt"))?;
            Ok(())
        });
        let completed = fixture
            .owner
            .run()
            .await
            .map_err(|error| finite_error("run", &error))?;
        let reply = receive(rx).await?;
        require(
            completed.facts.commands == 3 && completed.facts.accepted_peers == 1,
            "complete actual control facts",
        )?;
        require(
            completed.facts.response_wire_bytes == 3 * reply.wire_bytes,
            "actual Ready(n) response count",
        )?;
        require(
            completed.facts.last_response.sha256 == reply.hash,
            "actual reply streamed digest",
        )?;
        require(
            completed.hub.used_arm
                && completed.hub.stopped
                && completed.hub.gate.is_none()
                && !completed.hub.original_writer_exited,
            "unbound Stop cannot be native joined/cut proof",
        )?;
        require(
            !fixture.path().exists()
                && fixture.owner.peer.is_none()
                && fixture.owner.listener.is_none(),
            "physical Unix close",
        )?;
        Ok(())
    })
}

async fn rejected_wire(
    bytes: Vec<u8>,
    expected: fn(ControlClass) -> bool,
    stage: fn(ControlStage) -> bool,
    declared: Option<u64>,
    eof: bool,
) -> io::Result<()> {
    fixture!(fixture, {
        let path = fixture.path();
        let sent = bytes.clone();
        let (tx, rx) = oneshot::channel();
        fixture.spawn(async move {
            let mut peer = UnixStream::connect(path).await?;
            peer.write_all(&sent).await?;
            if eof {
                peer.shutdown().await?;
            }
            tx.send(())
                .map_err(|_| invalid("parent dropped sent receipt"))?;
            // The parent aborts and joins this actual original handle after observing failure.
            std::future::pending::<()>().await;
            Ok(())
        });
        let error = fixture
            .owner
            .run()
            .await
            .err()
            .ok_or_else(|| invalid("invalid frame passed"))?;
        receive(rx).await?;
        require(expected(error.primary.class), "wrong refusal class")?;
        require(stage(error.primary.stage), "wrong refusal stage")?;
        prefix(error.primary.frame, &bytes, declared)?;
        require(
            error.cleanup.is_none(),
            "unexpected exact-path cleanup failure",
        )?;
        require(
            fixture.owner.hub_snapshot().stopped && fixture.owner.hub_snapshot().failure.is_some(),
            "failure sticky plus Stop",
        )?;
        require(
            fixture.owner.request.iter().all(|byte| *byte == 0)
                && fixture.owner.reply.iter().all(|byte| *byte == 0),
            "private buffers erased",
        )?;
        Ok(())
    })
}
fn fields(class: ControlClass) -> bool {
    matches!(class, ControlClass::Fields)
}
fn identity(class: ControlClass) -> bool {
    matches!(class, ControlClass::Identity)
}
fn decode_stage(stage: ControlStage) -> bool {
    matches!(stage, ControlStage::Decode)
}

#[tokio::test]
async fn actual_strict_tlv_unknown_duplicate_missing_and_extra() -> io::Result<()> {
    // Every case opens a fresh actual Unix fixture, rather than calling decode alone.
    for case in 0..9 {
        fixture!(fixture, {
            let frontend = fixture.frontend();
            let mut malformed = body(SNAPSHOT, frontend, NONCE);
            match case {
                0 => malformed[0] = 2,
                1 => malformed[1] = 4, // External Resume is not a command.
                2 => tlv(&mut malformed, 6, &[]),
                3 => tlv(&mut malformed, 1, &frontend.to_bytes()),
                4 => tlv(&mut malformed, 2, &NONCE),
                5 => malformed.truncate(21), // Missing nonce.
                6 => tlv(&mut malformed, 3, &71u32.to_le_bytes()),
                7 => malformed.push(1), // Partial trailing TLV header.
                8 => malformed[3..5].copy_from_slice(&17u16.to_le_bytes()),
                _ => unreachable!(),
            }
            let bytes = wire(&malformed);
            let sent = bytes.clone();
            let path = fixture.path();
            fixture.spawn(async move {
                let mut peer = UnixStream::connect(path).await?;
                peer.write_all(&sent).await?;
                std::future::pending::<()>().await;
                Ok(())
            });
            let error = fixture
                .owner
                .run()
                .await
                .err()
                .ok_or_else(|| invalid("malformed TLV passed"))?;
            require(
                fields(error.primary.class) && decode_stage(error.primary.stage),
                "strict TLV refusal",
            )?;
            prefix(error.primary.frame, &bytes, Some(bytes.len() as u64))?;
            require(
                fixture.owner.facts().commands == 1 && !fixture.owner.hub_snapshot().used_arm,
                "malformed input never arms",
            )?;
            Ok(())
        })?;
    }
    Ok(())
}

#[tokio::test]
async fn actual_identity_nonce_and_zero_arm_parameters_refused() -> io::Result<()> {
    for case in 0..6 {
        fixture!(fixture, {
            let frontend = fixture.frontend();
            let mut malformed = body(if case >= 2 { ARM } else { SNAPSHOT }, frontend, NONCE);
            match case {
                0 => malformed[5..21].copy_from_slice(&FrontendProcessId::new_v7().to_bytes()),
                1 => malformed[24..40].fill(8),
                2 => malformed[43..47].fill(0), // connection id zero
                3 => malformed[85..93].fill(0), // cut zero
                4 => malformed[85..93].copy_from_slice(&(64 * 1024 * 1024u64 + 1).to_le_bytes()),
                5 => malformed[5..21].fill(0), // Nil is not a canonical UUIDv7 FE identity.
                _ => unreachable!(),
            }
            let bytes = wire(&malformed);
            let sent = bytes.clone();
            let path = fixture.path();
            fixture.spawn(async move {
                let mut peer = UnixStream::connect(path).await?;
                peer.write_all(&sent).await?;
                std::future::pending::<()>().await;
                Ok(())
            });
            let error = fixture
                .owner
                .run()
                .await
                .err()
                .ok_or_else(|| invalid("invalid identity/arm passed"))?;
            require(
                if case < 2 || case == 5 {
                    identity(error.primary.class)
                } else {
                    matches!(error.primary.class, ControlClass::Hub)
                },
                "identity or original Hub parameter refusal",
            )?;
            prefix(error.primary.frame, &bytes, Some(bytes.len() as u64))?;
            require(
                fixture.owner.hub_snapshot().failure
                    == Some(if case <= 2 || case == 5 {
                        GateFailure::Identity
                    } else {
                        GateFailure::Length
                    }),
                "exact first Hub cause",
            )?;
            Ok(())
        })?;
    }
    Ok(())
}

#[tokio::test]
async fn announced_over_cap_refused_before_any_payload_read() -> io::Result<()> {
    for declared in [4093u32, u32::MAX, 0, 1] {
        rejected_wire(
            declared.to_le_bytes().to_vec(),
            |class| matches!(class, ControlClass::Length),
            |stage| matches!(stage, ControlStage::ReadHeader),
            Some(declared as u64 + 4),
            false,
        )
        .await?;
    }
    Ok(())
}
#[tokio::test]
async fn full_4096_wire_boundary_is_read_then_strictly_decoded() -> io::Result<()> {
    let mut bytes = vec![0u8; 4096];
    bytes[..4].copy_from_slice(&4092u32.to_le_bytes());
    bytes[4] = 1;
    bytes[5] = 2;
    rejected_wire(bytes, fields, decode_stage, Some(4096), false).await
}
#[tokio::test]
async fn actual_partial_eof_retains_header_and_body_prefix() -> io::Result<()> {
    for bytes in [vec![], vec![40, 0], vec![40, 0, 0, 0, 1, 2, 1]] {
        let header_complete = bytes.len() >= 4;
        rejected_wire(
            bytes,
            |class| matches!(class, ControlClass::Eof),
            if header_complete {
                |stage| matches!(stage, ControlStage::ReadBody)
            } else {
                |stage| matches!(stage, ControlStage::ReadHeader)
            },
            if header_complete { Some(44) } else { None },
            true,
        )
        .await?;
    }
    Ok(())
}

#[tokio::test]
async fn duplicate_arm_is_rejected_by_original_one_slot_controller() -> io::Result<()> {
    fixture!(fixture, {
        let path = fixture.path();
        let frontend = fixture.frontend();
        let bytes = wire(&body(ARM, frontend, NONCE));
        let expected = bytes.clone();
        fixture.spawn(async move {
            let mut peer = UnixStream::connect(path).await?;
            send_command(&mut peer, frontend, ARM).await?;
            peer.write_all(&bytes).await?;
            std::future::pending::<()>().await;
            Ok(())
        });
        let error = fixture
            .owner
            .run()
            .await
            .err()
            .ok_or_else(|| invalid("duplicate Arm passed"))?;
        require(
            matches!(error.primary.class, ControlClass::Hub)
                && matches!(error.primary.stage, ControlStage::Apply),
            "duplicate original slot refusal",
        )?;
        require(
            fixture.owner.hub_snapshot().failure == Some(GateFailure::Transition)
                && fixture.owner.facts().commands == 2,
            "duplicate sticky first cause",
        )?;
        prefix(error.primary.frame, &expected, Some(expected.len() as u64))?;
        Ok(())
    })
}

#[tokio::test]
async fn sixteenth_stop_completes_but_sixteenth_snapshot_is_limit_failure() -> io::Result<()> {
    for terminal in [STOP, SNAPSHOT] {
        fixture!(fixture, {
            let path = fixture.path();
            let frontend = fixture.frontend();
            let (tx, rx) = oneshot::channel();
            fixture.spawn(async move {
                let mut peer = UnixStream::connect(path).await?;
                let mut last = None;
                for index in 0..16 {
                    let reply = if index == 15 && terminal == SNAPSHOT {
                        // Queue a real 17th frame without giving it another admission/read.
                        let command = wire(&body(SNAPSHOT, frontend, NONCE));
                        let mut queued = Vec::with_capacity(88);
                        queued.extend_from_slice(&command);
                        queued.extend_from_slice(&command);
                        peer.write_all(&queued).await?;
                        read_reply(&mut peer, frontend).await?
                    } else {
                        send_command(
                            &mut peer,
                            frontend,
                            if index == 15 { terminal } else { SNAPSHOT },
                        )
                        .await?
                    };
                    require(
                        reply.commands == index + 1
                            && reply.earlier_response_bytes == index as u64 * 47,
                        "command counter without clock refresh",
                    )?;
                    last = Some(reply);
                }
                if terminal == STOP {
                    let mut byte = [0];
                    require(
                        peer.read(&mut byte).await? == 0,
                        "Stop peer physically closed",
                    )?;
                }
                tx.send(last.ok_or_else(|| invalid("missing bounded final reply"))?)
                    .map_err(|_| invalid("parent dropped final reply"))?;
                Ok(())
            });
            let outcome = fixture.owner.run().await;
            let reply = receive(rx).await?;
            require(
                fixture.owner.facts().commands == 16
                    && fixture.owner.facts().request_wire_bytes == 16 * 44
                    && fixture.owner.facts().response_wire_bytes == 16 * 47,
                "exact 16 responses",
            )?;
            require(
                fixture.owner.facts().last_response.sha256 == reply.hash,
                "last response actual digest",
            )?;
            if terminal == STOP {
                require(
                    outcome.is_ok() && fixture.owner.facts().explicit_stop,
                    "16th Stop completes protocol only",
                )?;
            } else {
                let error = outcome
                    .err()
                    .ok_or_else(|| invalid("16 snapshots falsely complete"))?;
                require(
                    matches!(error.primary.class, ControlClass::CommandLimit)
                        && !fixture.owner.facts().explicit_stop,
                    "no read/admission of 17th command",
                )?;
            }
            Ok(())
        })?;
    }
    Ok(())
}

#[tokio::test]
async fn no_peer_and_partial_header_body_use_one_original_absolute_clock() -> io::Result<()> {
    for bytes in [None, Some(vec![40, 0]), Some(vec![40, 0, 0, 0, 1, 2, 1])] {
        fixture_budget!(fixture, Duration::from_millis(150), {
            // Component-only shorter clock passed once to both owner and Hub at bind.
            let absolute = fixture.owner.deadline;
            let expected = bytes.clone();
            if let Some(sent) = bytes {
                let path = fixture.path();
                fixture.spawn(async move {
                    let mut peer = UnixStream::connect(path).await?;
                    peer.write_all(&sent).await?;
                    std::future::pending::<()>().await;
                    Ok(())
                });
            }
            let error = fixture
                .owner
                .run()
                .await
                .err()
                .ok_or_else(|| invalid("pending frame passed"))?;
            require(
                matches!(error.primary.class, ControlClass::Deadline),
                "original absolute deadline failure",
            )?;
            require(
                fixture.owner.deadline == absolute && Instant::now() >= absolute,
                "clock neither reset nor extended",
            )?;
            let expected = expected.unwrap_or_default();
            prefix(
                error.primary.frame,
                &expected,
                if expected.len() >= 4 { Some(44) } else { None },
            )?;
            require(
                fixture.owner.hub_snapshot().failure == Some(GateFailure::Deadline),
                "Deadline sticky before close Transition",
            )?;
            fixture.owner.fail(GateFailure::Identity);
            require(
                fixture.owner.hub_snapshot().failure == Some(GateFailure::Deadline),
                "later failure cannot overwrite first cause",
            )?;
            Ok(())
        })?;
    }
    Ok(())
}

#[tokio::test]
async fn dropping_selected_run_borrow_retains_parent_controller_and_unix_cleanup() -> io::Result<()>
{
    fixture!(fixture, {
        let path = fixture.path();
        let frontend = fixture.frontend();
        let (tx, rx) = oneshot::channel();
        fixture.spawn(async move {
            let mut peer = UnixStream::connect(path).await?;
            send_command(&mut peer, frontend, SNAPSHOT).await?;
            tx.send(())
                .map_err(|_| invalid("parent sibling receipt missing"))?;
            std::future::pending::<()>().await;
            Ok(())
        });
        {
            let run = fixture.owner.run();
            tokio::pin!(run);
            tokio::select! {
                outcome=&mut run => {let _=outcome;return Err(invalid("run completed before selected sibling"));}
                sibling=rx => {sibling.map_err(|_|invalid("actual sibling receipt missing"))?;}
            }
        } // Only the borrow future drops, never ControlOwner or unique controller.
        require(
            fixture.owner.peer.is_some() && fixture.owner.listener.is_none(),
            "parent retains actual accepted IO",
        )?;
        require(
            !fixture.owner.hub_snapshot().stopped && fixture.owner.facts().commands == 1,
            "parent retains live controller",
        )?;
        fixture.owner.fail(GateFailure::Deadline);
        fixture
            .owner
            .close()
            .map_err(|error| finite_error("close", &error))?;
        require(
            fixture.owner.hub_snapshot().stopped
                && fixture.owner.hub_snapshot().failure == Some(GateFailure::Deadline),
            "selected timeout cause retained across close",
        )?;
        require(
            !fixture.path().exists(),
            "actual original socket inode removed by parent",
        )?;
        Ok(())
    })
}

#[tokio::test]
async fn outer_timeout_drops_only_run_borrow_and_first_cause_survives() -> io::Result<()> {
    fixture!(fixture, {
        let path = fixture.path();
        let frontend = fixture.frontend();
        let (tx, rx) = oneshot::channel();
        fixture.spawn(async move {
            let mut peer = UnixStream::connect(path).await?;
            send_command(&mut peer, frontend, SNAPSHOT).await?;
            tx.send(()).map_err(|_| invalid("client receipt missing"))?;
            std::future::pending::<()>().await;
            Ok(())
        });
        let parent_cut = tokio::time::Instant::now() + Duration::from_millis(150);
        let result = tokio::time::timeout_at(parent_cut, fixture.owner.run()).await;
        require(
            result.is_err(),
            "parent sibling timeout expected before control original clock",
        )?;
        receive(rx).await?;
        require(
            fixture.owner.peer.is_some() && fixture.owner.facts().commands == 1,
            "borrow timeout preserves actual owner state",
        )?;
        fixture.owner.fail(GateFailure::Receipt);
        fixture.owner.fail(GateFailure::Deadline);
        fixture
            .owner
            .close()
            .map_err(|error| finite_error("close", &error))?;
        require(
            fixture.owner.hub_snapshot().failure == Some(GateFailure::Receipt),
            "close never replaces prior primary",
        )?;
        Ok(())
    })
}

#[tokio::test]
async fn actual_socket_inode_replacement_is_not_unlinked_by_control_cleanup() -> io::Result<()> {
    fixture!(fixture, {
        let path = fixture.path();
        std::fs::remove_file(&path)?;
        fixture.artifacts[1] = None; // The test deliberately removed its own saved original inode.
        fixture.replacement = Some(UnixListener::bind(&path)?);
        fixture.remember(1, path.clone(), false)?;
        let replacement = fixture.artifacts[1].as_ref().unwrap().identity;
        let error = fixture
            .owner
            .close()
            .err()
            .ok_or_else(|| invalid("replaced socket wrongly removed"))?;
        require(
            matches!(error.class, ControlClass::Cleanup),
            "exact socket replacement cleanup refusal",
        )?;
        require(
            replacement.matches(&std::fs::symlink_metadata(&path)?),
            "replacement socket remains untouched",
        )?;
        require(
            fixture.owner.peer.is_none()
                && fixture.owner.listener.is_none()
                && fixture.owner.hub_snapshot().stopped,
            "original IO closed despite cleanup refusal",
        )?;
        // Retain the expected failure; do not modify the original control owner's identity.
        fixture.expected_close_refusal = Some(finite_error("control-close", &error));
        Ok(())
    })
}

#[tokio::test]
async fn actual_parent_inode_replacement_refuses_even_when_leaf_is_valid_socket() -> io::Result<()>
{
    fixture!(fixture, {
        let path = fixture.path();
        let parent = path.parent().unwrap().to_owned();
        let backup = parent.with_extension("old");
        std::fs::rename(&parent, &backup)?;
        fixture.artifacts[0].as_mut().unwrap().path = backup.clone();
        fixture.artifacts[1].as_mut().unwrap().path = backup.join("gate.sock");
        std::fs::DirBuilder::new().mode(0o700).create(&parent)?;
        fixture.remember(2, parent.clone(), true)?;
        std::fs::set_permissions(&parent, std::fs::Permissions::from_mode(0o700))?;
        fixture.replacement = Some(UnixListener::bind(&path)?);
        fixture.remember(3, path.clone(), false)?;
        let error = fixture
            .owner
            .close()
            .err()
            .ok_or_else(|| invalid("changed parent wrongly accepted"))?;
        require(
            matches!(error.class, ControlClass::Cleanup),
            "actual parent inode refusal",
        )?;
        require(
            fixture.artifacts[3]
                .as_ref()
                .unwrap()
                .identity
                .matches(&std::fs::symlink_metadata(&path)?),
            "replacement in changed parent untouched",
        )?;
        require(
            fixture.artifacts[1]
                .as_ref()
                .unwrap()
                .identity
                .matches(&std::fs::symlink_metadata(backup.join("gate.sock"))?),
            "original moved socket not silently removed",
        )?;
        // The fixture removes only its recorded original/replacement artifacts after IO joins.
        // The production control locator and saved identities remain unchanged.
        fixture.expected_close_refusal = Some(finite_error("control-close", &error));
        Ok(())
    })
}

#[tokio::test]
async fn client_panic_and_parent_error_still_join_every_original_handle() -> io::Result<()> {
    let mut fixture = Fixture::new()?;
    fixture.spawn(async {
        panic!("fixed component client panic");
        #[allow(unreachable_code)]
        Ok(())
    });
    let parent = catch_fixture(async {
        panic!("fixed component parent panic");
        #[allow(unreachable_code)]
        Ok(())
    })
    .await;
    // Give the actual child one poll; its panic remains a typed JoinError, not an exit boolean.
    tokio::task::yield_now().await;
    let error = fixture
        .settle(parent)
        .await
        .err()
        .ok_or_else(|| invalid("panic cleanup falsely passed"))?;
    let exit = error
        .get_ref()
        .and_then(|value| value.downcast_ref::<FixtureExit>())
        .ok_or_else(|| invalid("missing bounded primary plus cleanup"))?;
    require(
        exit.primary.is_some() && exit.cleanup.is_some(),
        "parent and actual JoinError both retained",
    )?;
    require(
        fixture.joined == fixture.spawned && fixture.clients.is_empty() && !fixture.path().exists(),
        "actual clients joined and socket removed after panic",
    )
}

#[tokio::test]
async fn valid_snapshot_does_not_refresh_next_partial_request_deadline() -> io::Result<()> {
    fixture_budget!(fixture, Duration::from_millis(350), {
        let absolute = fixture.owner.deadline;
        let path = fixture.path();
        let frontend = fixture.frontend();
        let (tx, rx) = oneshot::channel();
        fixture.spawn(async move {
            let mut peer = UnixStream::connect(path).await?;
            let reply = send_command(&mut peer, frontend, SNAPSHOT).await?;
            require(
                reply.commands == 1,
                "snapshot completed before partial request",
            )?;
            tokio::time::sleep_until(tokio::time::Instant::from_std(
                absolute - Duration::from_millis(100),
            ))
            .await;
            peer.write_all(&[40, 0]).await?;
            tx.send(reply)
                .map_err(|_| invalid("completed snapshot receipt missing"))?;
            std::future::pending::<()>().await;
            Ok(())
        });
        let error = fixture
            .owner
            .run()
            .await
            .err()
            .ok_or_else(|| invalid("partial request survived original deadline"))?;
        let reply = receive(rx).await?;
        require(
            matches!(error.primary.class, ControlClass::Deadline)
                && matches!(error.primary.stage, ControlStage::ReadHeader),
            "shared original deadline applies after snapshot",
        )?;
        require(
            fixture.owner.deadline == absolute && fixture.owner.facts().commands == 1,
            "successful command did not refresh clock/count",
        )?;
        require(
            fixture.owner.facts().request_wire_bytes == 44 + 2
                && fixture.owner.facts().response_wire_bytes == reply.wire_bytes,
            "actual complete plus partial wire accounting",
        )?;
        prefix(error.primary.frame, &[40, 0], None)?;
        require(
            fixture.owner.hub_snapshot().failure == Some(GateFailure::Deadline),
            "original deadline remains first cause",
        )?;
        Ok(())
    })
}

#[tokio::test]
async fn reordered_known_tlvs_are_legal_and_do_not_add_authority() -> io::Result<()> {
    fixture!(fixture, {
        let path = fixture.path();
        let frontend = fixture.frontend();
        let (tx, rx) = oneshot::channel();
        let mut reversed = vec![1, ARM];
        tlv(&mut reversed, 5, &2u64.to_le_bytes());
        tlv(&mut reversed, 4, &SQL_HASH);
        tlv(&mut reversed, 3, &71u32.to_le_bytes());
        tlv(&mut reversed, 2, &NONCE);
        tlv(&mut reversed, 1, &frontend.to_bytes());
        fixture.spawn(async move {
            let mut peer = UnixStream::connect(path).await?;
            peer.write_all(&wire(&reversed)).await?;
            let arm = read_reply(&mut peer, frontend).await?;
            require(
                arm.opcode == ARM && arm.used_arm,
                "reordered known fields legal",
            )?;
            let stop = send_command(&mut peer, frontend, STOP).await?;
            tx.send(stop).map_err(|_| invalid("stop receipt missing"))?;
            Ok(())
        });
        let completed = fixture
            .owner
            .run()
            .await
            .map_err(|error| finite_error("run", &error))?;
        receive(rx).await?;
        require(
            completed.facts.commands == 2
                && completed.hub.gate.is_none()
                && !completed.hub.original_writer_exited,
            "control only, no fake writer/native proof",
        )?;
        // Re-running the same one-peer controller is strict even after protocol completion.
        let error = fixture
            .owner
            .run()
            .await
            .err()
            .ok_or_else(|| invalid("second run accepted"))?;
        require(
            matches!(error.primary.class, ControlClass::State),
            "original owner rejects second run",
        )?;
        Ok(())
    })
}
