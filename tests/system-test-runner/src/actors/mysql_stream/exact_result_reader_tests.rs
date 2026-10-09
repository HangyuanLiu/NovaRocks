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

// Actual loopback observer components only; no FE/BE, marker, Root, poll_write or owner-finish proof.
use super::*;
use std::time::Duration;
use tokio::net::TcpListener;
use tokio::sync::oneshot;

fn tiny(cut: u64) -> RowInput {
    RowInput {
        columns: 1,
        value_bytes: 3,
        repeated_byte: b'a',
        cut,
        expect_complete_tail: true,
    }
}
fn wide() -> RowInput {
    RowInput {
        columns: 17,
        value_bytes: 1_048_576,
        repeated_byte: b'q',
        cut: 1_048_577,
        expect_complete_tail: false,
    }
}
fn metadata_column(name: &[u8], mysql_type: u8) -> Vec<u8> {
    let mut bytes = b"\x03def\x00\x00\x00".to_vec();
    bytes.push(name.len() as u8);
    bytes.extend_from_slice(name);
    bytes.push(0);
    bytes.extend_from_slice(&[12, 33, 0, 0, 4, 0, 0, mysql_type, 0, 0, 0, 0, 0]);
    bytes
}
async fn frame(peer: &mut TcpStream, sequence: u8, payload: &[u8]) -> Result<()> {
    ensure!(
        payload.len() <= SMALL,
        "test frame exceeds fixed local component bound"
    );
    let n = payload.len();
    peer.write_all(&[n as u8, (n >> 8) as u8, (n >> 16) as u8, sequence])
        .await?;
    peer.write_all(payload).await?;
    Ok(())
}
async fn metadata(peer: &mut TcpStream, input: RowInput) -> Result<()> {
    frame(peer, 1, &[input.columns]).await?;
    for index in 0..input.columns {
        let name = if input.columns == 1 {
            "payload".to_owned()
        } else {
            format!("c{index}")
        };
        frame(peer, index + 2, &metadata_column(name.as_bytes(), 253)).await?;
    }
    frame(peer, input.columns + 2, &[0xfe, 0, 0, 0, 0]).await
}
async fn client_command(peer: &mut TcpStream, expected: &str, deadline: Instant) -> Result<()> {
    let mut recorder = Recorder::new();
    let packet = small_packet(peer, 0, SMALL, deadline, &mut recorder, ReadKind::Handshake).await?;
    ensure!(
        packet.payload().first() == Some(&3) && &packet.payload()[1..] == expected.as_bytes(),
        "test peer received changed original command"
    );
    Ok(())
}
#[derive(Clone, Copy)]
enum Mode {
    TinyHealth,
    WideEof,
    OversizeMetadata,
    PartialMetadata,
    WrongTerminal,
    OversizeHandshake,
    TinyEarlyEof,
    SourceCanary,
    XFullErr,
    XPartialPayload,
}
struct Peer {
    port: u16,
    release: Option<oneshot::Sender<()>>,
    tasks: tokio::task::JoinSet<Result<()>>,
}
struct ComponentSettleFailure {
    primary: Option<anyhow::Error>,
    cleanup: Vec<anyhow::Error>,
    joined_tasks: usize,
    task_set_empty: bool,
}
impl std::fmt::Display for ComponentSettleFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "host component failure primary={} cleanup={} joined={} empty={}",
            self.primary.is_some(),
            self.cleanup.len(),
            self.joined_tasks,
            self.task_set_empty
        )
    }
}
impl std::fmt::Debug for ComponentSettleFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(self, f)
    }
}
impl std::error::Error for ComponentSettleFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.primary
            .as_ref()
            .or_else(|| self.cleanup.first())
            .map(|error| error.as_ref())
    }
}
impl Peer {
    async fn start(mode: Mode, input: RowInput, original_deadline: Instant) -> Result<Self> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let port = listener.local_addr()?.port();
        let (release, wait_release) = oneshot::channel();
        let mut tasks = tokio::task::JoinSet::new();
        tasks.spawn(async move {
            // Independent host-fixture watchdog settles the peer. It does not renew scene time.
            tokio::time::timeout(Duration::from_secs(3), async move {
                let (mut peer, _) = listener.accept().await?;
                if matches!(mode, Mode::OversizeHandshake) {
                    peer.write_all(&[1, 16, 0, 0]).await?; // 4097 announced, zero payload.
                    let _ = wait_release.await;
                    return Ok(());
                }
                if matches!(mode, Mode::PartialMetadata | Mode::XPartialPayload) {
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
                let mut handshake = b"\x0a8.0\0".to_vec();
                handshake.extend_from_slice(&71u32.to_le_bytes());
                frame(&mut peer, 0, &handshake).await?;
                let mut recorder = Recorder::new();
                let auth = small_packet(
                    &mut peer,
                    1,
                    SMALL,
                    original_deadline,
                    &mut recorder,
                    ReadKind::Handshake,
                )
                .await?;
                ensure!(
                    auth.payload().len() >= 32 && auth.payload()[4..8] == MAX_PACKET.to_le_bytes(),
                    "actual client did not advertise original max_allowed_packet"
                );
                ensure!(
                    &auth.payload()[32..] == b"root\0\0mysql_native_password\0",
                    "actual original auth response changed"
                );
                frame(&mut peer, 2, &[0]).await?;
                if matches!(mode, Mode::SourceCanary) {
                    let mut byte = [0u8; 1];
                    ensure!(
                        peer.read(&mut byte).await? == 0,
                        "actual original reader owner did not close"
                    );
                    return Ok(());
                }
                client_command(&mut peer, original_sql(input)?, original_deadline).await?;
                match mode {
                    Mode::OversizeMetadata => {
                        peer.write_all(&[1, 16, 0, 1]).await?;
                        let _ = wait_release.await;
                    }
                    Mode::PartialMetadata => {
                        peer.write_all(&[1, 0]).await?;
                        let _ = wait_release.await;
                    }
                    Mode::WideEof => {
                        metadata(&mut peer, input).await?;
                        peer.write_all(&[255, 255, 255, 20]).await?;
                        peer.write_all(&[0xfd, 0, 0, 0x10]).await?;
                        let mut left = input.cut + 2048 - 8;
                        let mut scratch = [0u8; SMALL];
                        let mut payload_offset = 4u64;
                        while left != 0 {
                            let n = left.min(SMALL as u64) as usize;
                            // Every original wide column has its own lenenc prefix.
                            // Crossing column one must not replace column two's prefix with q.
                            for (index, byte) in scratch[..n].iter_mut().enumerate() {
                                *byte = match (payload_offset + index as u64) % 1_048_580 {
                                    0 => 0xfd,
                                    1 | 2 => 0,
                                    3 => 0x10,
                                    _ => b'q',
                                };
                            }
                            peer.write_all(&scratch[..n]).await?;
                            left -= n as u64;
                            payload_offset += n as u64;
                        }
                        // Actual write-half FIN permits testing the same socket followup write,
                        // while this peer still reads the exact original health command.
                        peer.shutdown().await?;
                        client_command(&mut peer, ORIGINAL_HEALTH_SQL, original_deadline).await?;
                    }
                    Mode::TinyEarlyEof => {
                        metadata(&mut peer, input).await?;
                        peer.write_all(&[4, 0, 0, 4, 3, b'a']).await?;
                        peer.shutdown().await?;
                        let _ = wait_release.await;
                    }
                    Mode::TinyHealth | Mode::WrongTerminal => {
                        metadata(&mut peer, input).await?;
                        frame(&mut peer, 4, &[3, b'a', b'b', b'c']).await?;
                        let code = if matches!(mode, Mode::WrongTerminal) {
                            1105u16
                        } else {
                            1317u16
                        };
                        let mut error = vec![0xff];
                        error.extend_from_slice(&code.to_le_bytes());
                        error.extend_from_slice(b"#70100");
                        frame(&mut peer, 5, &error).await?;
                        if matches!(mode, Mode::WrongTerminal) {
                            let _ = wait_release.await;
                        } else {
                            client_command(&mut peer, ORIGINAL_HEALTH_SQL, original_deadline)
                                .await?;
                            frame(&mut peer, 1, &[1]).await?;
                            frame(&mut peer, 2, &metadata_column(b"total", 8)).await?;
                            frame(&mut peer, 3, &[0xfe, 0, 0, 0, 0]).await?;
                            frame(&mut peer, 4, &[4, b'5', b'0', b'5', b'0']).await?;
                            frame(&mut peer, 5, &[0xfe, 0, 0, 0, 0]).await?;
                        }
                    }
                    Mode::XFullErr | Mode::XPartialPayload => {
                        ensure!(
                            input.columns == 1
                                && input.value_bytes == 1_048_576
                                && input.repeated_byte == b'x'
                                && input.expect_complete_tail,
                            "x peer received changed original input"
                        );
                        metadata(&mut peer, input).await?;
                        let wire_bytes = if matches!(mode, Mode::XFullErr) {
                            X_ROW_WIRE_BYTES
                        } else {
                            input.cut + 3
                        };
                        original_x_row_prefix(&mut peer, wire_bytes).await?;
                        if matches!(mode, Mode::XFullErr) {
                            frame(
                                &mut peer,
                                5,
                                &[0xff, 0x25, 0x05, b'#', b'7', b'0', b'1', b'0', b'0'],
                            )
                            .await?;
                        }
                        // No EOF/late tail: keep the real socket open until the original
                        // observer has completed or returned its original deadline error.
                        let _ = wait_release.await;
                    }
                    Mode::OversizeHandshake | Mode::SourceCanary => unreachable!(),
                }
                Ok::<_, anyhow::Error>(())
            })
            .await
            .context("host-only test peer watchdog")?
        });
        Ok(Self {
            port,
            release: Some(release),
            tasks,
        })
    }
    async fn run_observer<F>(mut self, observer: F) -> Result<()>
    where
        F: std::future::Future<Output = Result<()>> + Send + 'static,
    {
        let observer_id = self.tasks.spawn(observer).id();
        let mut cleanup = Vec::with_capacity(2);
        let mut joined_tasks = 0usize;
        let primary = loop {
            let joined = self.tasks.join_next_with_id().await;
            if joined.is_some() {
                joined_tasks += 1;
            }
            match joined {
                Some(Ok((id, result))) if id == observer_id => break result,
                Some(Err(error)) if error.id() == observer_id => break Err(error.into()),
                Some(Ok((_, Err(error)))) => cleanup.push(error),
                Some(Err(error)) => cleanup.push(error.into()),
                Some(Ok((_, Ok(())))) => {}
                None => {
                    break Err(anyhow::anyhow!(
                        "original observer task disappeared before join"
                    ));
                }
            }
        };
        if let Some(release) = self.release.take() {
            let _ = release.send(());
        }
        if primary.is_err() {
            self.tasks.abort_all();
        }
        // Never detach the original peer after observer panic/error/timeout.
        // Watchdog only bounds its work; this parent actually joins every original task.
        while let Some(result) = self.tasks.join_next_with_id().await {
            joined_tasks += 1;
            match result {
                Ok((_, Ok(()))) => {}
                Ok((_, Err(error))) => cleanup.push(error),
                Err(error) => cleanup.push(error.into()),
            }
        }
        ensure!(
            self.tasks.is_empty(),
            "original host component task set did not converge"
        );
        if primary.is_ok() && cleanup.is_empty() {
            return Ok(());
        }
        Err(ComponentSettleFailure {
            primary: primary.err(),
            cleanup,
            joined_tasks,
            task_set_empty: self.tasks.is_empty(),
        }
        .into())
    }
}
fn tiny_hash() -> [u8; 32] {
    let mut hash = Sha256::new();
    hash.update([3, b'a', b'b', b'c']);
    hash.update(4u64.to_le_bytes());
    hash.finalize().into()
}

#[test]
fn literal_schema_refuses_name_type_flags_suffix_and_truncation() {
    for (input, index, name) in [
        (tiny(1), 0, "payload"),
        (wide(), 0, "c0"),
        (wide(), 16, "c16"),
    ] {
        let bytes = metadata_column(name.as_bytes(), 253);
        column(&bytes, input, index).unwrap();
        for cut in 0..bytes.len() {
            assert!(column(&bytes[..cut], input, index).is_err());
        }
        let mut bad = bytes.clone();
        bad.push(0);
        assert!(column(&bad, input, index).is_err());
        let mut bad = bytes.clone();
        let at = bad.len() - 6;
        bad[at] = 8;
        assert!(column(&bad, input, index).is_err());
        let mut bad = bytes.clone();
        let at = bad.len() - 5;
        bad[at] = 2;
        assert!(column(&bad, input, index).is_err());
    }
    assert!(column(&metadata_column(b"c15", 253), wide(), 16).is_err());
    assert!(column(&metadata_column(b"v", 253), tiny(1), 0).is_err());
}

#[tokio::test]
async fn all_tiny_cuts_exact_tcp_prefix_full_row_err_and_same_socket_health_join() -> Result<()> {
    for cut in 1..=6 {
        let input = tiny(cut);
        let deadline = Instant::now() + Duration::from_secs(2);
        let peer = Peer::start(Mode::TinyHealth, input, deadline).await?;
        let port = peer.port;
        peer.run_observer(async move {
            let mut reader = ExactResultReader::connect("root", port, input, deadline).await?;
            let result = async {
                ensure!(
                    reader.connection_id() == 71
                        && reader
                            .receive_buffer_bytes()
                            .is_some_and(|n| n <= MAX_APPLIED_RECV),
                    "actual handshake/receive buffer facts differ"
                );
                reader.send_original_query(NEW_TINY_SQL).await?;
                let metadata = reader.read_metadata().await?;
                ensure!(
                    metadata.columns == 1 && metadata.actual_next_sequence == 4,
                    "actual metadata sequence differs"
                );
                let prefix = reader.read_cut_prefix().await?;
                ensure!(
                    prefix.row_wire_bytes == cut
                        && prefix.row_wire_sha256 == input.prefix_hash()?,
                    "actual cut/hash differs"
                );
                ensure!(
                    prefix.pending_header_bytes == cut.min(4) as u8
                        && prefix.pending_payload_received == cut.saturating_sub(4) as u32,
                    "actual partial row header/payload receipt differs"
                );
                let terminal = reader.complete_row_then_interrupted(tiny_hash()).await?;
                ensure!(
                    terminal.row_wire_bytes == 8
                        && terminal.row_complete
                        && terminal.interrupted_complete,
                    "actual complete tiny row plus ERR differs"
                );
                reader
                    .begin_original_health_query(ORIGINAL_HEALTH_SQL)
                    .await?;
                for (index, expected) in [
                    vec![1],
                    metadata_column(b"total", 8),
                    vec![0xfe, 0, 0, 0, 0],
                    vec![4, b'5', b'0', b'5', b'0'],
                    vec![0xfe, 0, 0, 0, 0],
                ]
                .iter()
                .enumerate()
                {
                    let packet = reader.read_original_health_packet().await?;
                    ensure!(
                        packet.sequence == index as u8 + 1
                            && packet.payload() == expected.as_slice(),
                        "original same-socket health differs"
                    );
                }
                Ok::<_, anyhow::Error>(())
            }
            .await;
            drop(reader);
            result
        })
        .await?;
    }
    Ok(())
}

#[tokio::test]
async fn wide_valid_partial_eof_and_whole_followup_write_actual_zero_read_join() -> Result<()> {
    let input = wide();
    let deadline = Instant::now() + Duration::from_secs(2);
    let peer = Peer::start(Mode::WideEof, input, deadline).await?;
    let port = peer.port;
    peer.run_observer(async move {
        let mut reader = ExactResultReader::connect("root", port, input, deadline).await?;
        let result = async {
            reader.send_original_query(ORIGINAL_WIDE_SQL).await?;
            let metadata = reader.read_metadata().await?;
            ensure!(
                metadata.columns == 17 && metadata.actual_next_sequence == 20,
                "wide metadata differs"
            );
            reader.read_cut_prefix().await?;
            let missing = reader.read_missing_tail_to_eof().await?;
            ensure!(
                missing.row_wire_bytes == input.cut + 2048
                    && missing.missing_tail_read_eof
                    && !missing.row_complete,
                "actual incomplete validated row/EOF differs"
            );
            let refused = reader
                .probe_zero_response_followup(ORIGINAL_HEALTH_SQL)
                .await?;
            ensure!(
                refused.followup_write_complete
                    && refused.followup_read_eof
                    && refused.followup_response_bytes == 0,
                "same-socket zero-response receipt differs"
            );
            ensure!(
                refused.followup_written_bytes == ORIGINAL_HEALTH_SQL.len() as u64 + 5,
                "followup command not whole"
            );
            Ok::<_, anyhow::Error>(())
        }
        .await;
        drop(reader);
        result
    })
    .await
}

#[tokio::test]
async fn metadata_overcap_and_partial_header_clock_keep_original_bounded_facts_and_join()
-> Result<()> {
    for mode in [Mode::OversizeMetadata, Mode::PartialMetadata] {
        let input = tiny(2);
        let origin = Instant::now();
        let deadline = origin + Duration::from_millis(150);
        let peer = Peer::start(mode, input, deadline).await?;
        let port = peer.port;
        peer.run_observer(async move {
            let mut reader = ExactResultReader::connect("root", port, input, deadline).await?;
            let result = async {
                reader.send_original_query(NEW_TINY_SQL).await?;
                let failure = reader
                    .read_metadata()
                    .await
                    .expect_err("negative metadata must refuse");
                let state = reader.snapshot();
                ensure!(state.failed, "failed read is not sealed");
                ensure!(
                    state.metadata_wire_bytes == failure.observation.metadata_wire_bytes,
                    "partial failure facts lost"
                );
                match mode {
                    Mode::OversizeMetadata => ensure!(
                        state.metadata_wire_bytes == 4
                            && state.pending_payload_length == Some(4097)
                            && state.pending_payload_received == 0,
                        "overcap allocated/read payload"
                    ),
                    _ => {
                        ensure!(
                            Instant::now() >= deadline,
                            "clock was refreshed or returned before frozen deadline"
                        );
                        ensure!(
                            state.metadata_wire_bytes == 2
                                && state.pending_header_bytes == 2
                                && state.pending_header[..2] == [1, 0],
                            "actual partial metadata header lost"
                        );
                        ensure!(
                            failure
                                .cause
                                .downcast_ref::<tokio::time::error::Elapsed>()
                                .is_some(),
                            "original timeout cause lost"
                        );
                    }
                }
                Ok::<_, anyhow::Error>(())
            }
            .await;
            drop(reader);
            result
        })
        .await?;
    }
    Ok(())
}

#[tokio::test]
async fn handshake_announces_4097_refuses_before_payload_and_actual_peer_join() -> Result<()> {
    let deadline = Instant::now() + Duration::from_secs(1);
    let peer = Peer::start(Mode::OversizeHandshake, tiny(1), deadline).await?;
    let port = peer.port;
    peer.run_observer(async move {
        let failure = match ExactResultReader::connect("root", port, tiny(1), deadline).await {
            Ok(reader) => {
                drop(reader);
                None
            }
            Err(failure) => Some(failure),
        };
        let failure = failure.context("announced overcap handshake must refuse")?;
        ensure!(
            failure.observation.handshake_received_bytes == 4
                && failure.observation.pending_payload_length == Some(4097)
                && failure.observation.pending_payload_received == 0,
            "handshake failed after unbounded payload read"
        );
        Ok(())
    })
    .await
}

#[tokio::test]
async fn wrong_terminal_keeps_actual_full_row_receipt_but_never_claims_interrupted() -> Result<()> {
    let deadline = Instant::now() + Duration::from_secs(1);
    let input = tiny(6);
    let peer = Peer::start(Mode::WrongTerminal, input, deadline).await?;
    let port = peer.port;
    peer.run_observer(async move {
        let mut reader = ExactResultReader::connect("root", port, input, deadline).await?;
        let result = async {
            reader.send_original_query(NEW_TINY_SQL).await?;
            reader.read_metadata().await?;
            reader.read_cut_prefix().await?;
            let failure = reader
                .complete_row_then_interrupted(tiny_hash())
                .await
                .expect_err("wrong ERR must refuse");
            ensure!(
                failure.observation.row_complete
                    && !failure.observation.interrupted_complete
                    && failure.observation.failed,
                "wrong terminal became successful Interrupted"
            );
            ensure!(
                failure.observation.complete_row_sha256 == Some(tiny_hash()),
                "actual full row digest lost"
            );
            Ok::<_, anyhow::Error>(())
        }
        .await;
        drop(reader);
        result
    })
    .await
}

#[tokio::test]
async fn preexpired_connect_refuses_without_tcp_or_renewed_clock() {
    let result = ExactResultReader::connect("root", 1, tiny(1), Instant::now()).await;
    assert!(result.is_err());
    let failure = match result {
        Err(failure) => failure,
        Ok(_) => unreachable!(),
    };
    assert_eq!(failure.observation.received_bytes, 0);
    assert_eq!(failure.observation.sent_bytes, 0);
    assert_eq!(failure.observation.connection_id, 0);
}

#[tokio::test]
async fn resident_actual_row_eof_retains_received_prefix_and_never_claims_full_row() -> Result<()> {
    let deadline = Instant::now() + Duration::from_secs(1);
    let input = tiny(1);
    let peer = Peer::start(Mode::TinyEarlyEof, input, deadline).await?;
    let port = peer.port;
    peer.run_observer(async move {
        let mut reader = ExactResultReader::connect("root", port, input, deadline).await?;
        let result = async {
            reader.send_original_query(NEW_TINY_SQL).await?;
            reader.read_metadata().await?;
            reader.read_cut_prefix().await?;
            let failure = reader
                .complete_row_then_interrupted(tiny_hash())
                .await
                .expect_err("early actual row EOF must refuse");
            let actual = [4, 0, 0, 4, 3, b'a'];
            ensure!(
                failure.observation.last_read_eof
                    && failure.observation.row_wire_bytes == 6
                    && !failure.observation.row_complete,
                "early EOF lost actual row prefix or claimed full row"
            );
            ensure!(
                failure.observation.row_wire_sha256 == <[u8; 32]>::from(Sha256::digest(actual))
                    && failure.observation.pending_header == [4, 0, 0, 4]
                    && failure.observation.pending_payload_received == 2,
                "actual row prefix digest/header/body receipt differs"
            );
            Ok::<_, anyhow::Error>(())
        }
        .await;
        drop(reader);
        result
    })
    .await
}

#[derive(Debug)]
struct SourceCanary {
    identity: std::sync::Arc<()>,
    message: &'static str,
}
impl std::fmt::Display for SourceCanary {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.message)
    }
}
impl std::error::Error for SourceCanary {}
fn move_owned_failure(value: ExactReadFailure) -> ExactReadFailure {
    value
}

#[tokio::test]
async fn failure_presentation_is_finite_and_original_source_identity_survives_ownerclose_move()
-> Result<()> {
    // The cause is synthetic. TCP drop/EOF/join are actual loopback observations only.
    const CANARY: &str = "SOURCE-CANARY-DO-NOT-RENDER-AUTHORIZATION";
    let deadline = Instant::now() + Duration::from_secs(1);
    let peer = Peer::start(Mode::SourceCanary, tiny(1), deadline).await?;
    let port = peer.port;
    peer.run_observer(async move {
        let mut reader = ExactResultReader::connect("root", port, tiny(1), deadline).await?;
        let identity = std::sync::Arc::new(());
        let original = io::Error::new(
            io::ErrorKind::BrokenPipe,
            SourceCanary {
                identity: identity.clone(),
                message: CANARY,
            },
        );
        let failure = reader
            .settle(Err::<(), anyhow::Error>(anyhow::Error::new(original)))
            .expect_err("synthetic original IO must fail");
        let before = failure.observation;
        drop(reader); // Actual owner close, before source is moved or inspected below.
        let failure = move_owned_failure(failure);
        for presentation in [
            format!("{failure}"),
            format!("{failure:?}"),
            format!("{failure:#?}"),
        ] {
            ensure!(
                presentation.len() <= 512 && !presentation.contains(CANARY),
                "failure rendered source or exceeded finite presentation"
            );
        }
        let original = failure
            .cause
            .downcast_ref::<io::Error>()
            .context("original IO source type lost")?;
        ensure!(
            original.kind() == io::ErrorKind::BrokenPipe,
            "original IO kind rebuilt"
        );
        let canary = original
            .get_ref()
            .and_then(|source| source.downcast_ref::<SourceCanary>())
            .context("original canary source type lost")?;
        ensure!(
            std::sync::Arc::ptr_eq(&identity, &canary.identity) && canary.message == CANARY,
            "original source identity was cloned or rebuilt"
        );
        ensure!(
            before.received_bytes == failure.observation.received_bytes
                && before.received_sha256 == failure.observation.received_sha256,
            "moving failure or closing reader lost bounded partial receipt"
        );
        ensure!(
            std::error::Error::source(&failure).is_some(),
            "owned original source chain lost"
        );
        Ok::<_, anyhow::Error>(())
    })
    .await
}

#[test]
fn completed_actual_header_keeps_announced_length_before_original_clock_refusal() {
    let mut recorder = Recorder::new();
    recorder.state.phase = ExactPhase::Metadata;
    recorder.observe_read(ReadKind::Metadata, &[1, 16, 0, 1]);
    recorder.append_packet_header(&[1, 16, 0, 1]).unwrap();
    let cause = check(Instant::now()).unwrap_err();
    let failure = recorder.failure(cause);
    assert_eq!(failure.observation.metadata_wire_bytes, 4);
    assert_eq!(failure.observation.pending_header_bytes, 4);
    assert_eq!(failure.observation.pending_payload_length, Some(4097));
    assert_eq!(failure.observation.pending_payload_received, 0);
}

#[tokio::test]
async fn observer_panic_is_captured_before_all_original_task_handles_are_joined() -> Result<()> {
    let deadline = Instant::now() + Duration::from_secs(1);
    let peer = Peer::start(Mode::OversizeHandshake, tiny(1), deadline).await?;
    let failure = peer
        .run_observer(async move {
            panic!("synthetic observer panic inside owned task");
        })
        .await
        .expect_err("observer panic must whole-refuse after actual joins");
    let failure = failure
        .downcast_ref::<ComponentSettleFailure>()
        .context("original aggregate missing")?;
    ensure!(
        failure.task_set_empty && failure.joined_tasks == 2,
        "panic detached original child task"
    );
    ensure!(
        failure
            .primary
            .as_ref()
            .and_then(|cause| cause.downcast_ref::<tokio::task::JoinError>())
            .is_some_and(|cause| cause.is_panic()),
        "original observer panic cause lost"
    );
    Ok(())
}

// Additional actual loopback components for the ORIGINAL 1MiB x input.
// No FE/BE, poll_write, gate, marker, Root or Native acceptance is established here.
const X_VALUE_BYTES: u64 = 1_048_576;
const X_ROW_PAYLOAD_BYTES: u64 = X_VALUE_BYTES + 4;
const X_ROW_WIRE_BYTES: u64 = X_VALUE_BYTES + 8;
const X_HEADER: [u8; 4] = [4, 0, 0x10, 4];
const X_CELL_PREFIX: [u8; 4] = [0xfd, 0, 0, 0x10];

fn original_x(cut: u64) -> RowInput {
    RowInput {
        columns: 1,
        value_bytes: X_VALUE_BYTES as u32,
        repeated_byte: b'x',
        cut,
        expect_complete_tail: true,
    }
}
fn original_x_frozen_row_hash() -> [u8; 32] {
    [
        0xe5, 0xa0, 0x5e, 0x54, 0xf4, 0x63, 0x6f, 0xe6, 0xe8, 0x7e, 0xb8, 0x09, 0x4f, 0xce, 0xda,
        0x8e, 0x00, 0x2b, 0xff, 0x79, 0x9f, 0x3d, 0xe7, 0x64, 0x13, 0xa2, 0xa7, 0x7c, 0x52, 0xfd,
        0x50, 0xb8,
    ]
}

/// Host external producer only. Each packet/row prefix is generated from independent literals.
/// Large bodies are streamed with the original fixed4096 scratch, never a 1MiB Vec.
async fn original_x_row_prefix(peer: &mut TcpStream, wire_bytes: u64) -> Result<()> {
    ensure!(
        (8..=X_ROW_WIRE_BYTES).contains(&wire_bytes),
        "x fixture prefix exceeds original row geometry"
    );
    peer.write_all(&X_HEADER).await?;
    peer.write_all(&X_CELL_PREFIX).await?;
    let scratch = [b'x'; SMALL];
    let mut left = wire_bytes - 8;
    while left != 0 {
        let count = left.min(SMALL as u64) as usize;
        peer.write_all(&scratch[..count]).await?;
        left -= count as u64;
    }
    Ok(())
}

/// Expected hash is independent of reader/RowWireOracle and uses frozen wire literals.
fn original_x_literal_wire_hash(wire_bytes: u64) -> Result<[u8; 32]> {
    ensure!(
        (8..=X_ROW_WIRE_BYTES).contains(&wire_bytes),
        "x literal hash prefix exceeds original geometry"
    );
    let mut hash = Sha256::new();
    hash.update(X_HEADER);
    hash.update(X_CELL_PREFIX);
    let scratch = [b'x'; SMALL];
    let mut left = wire_bytes - 8;
    while left != 0 {
        let count = left.min(SMALL as u64) as usize;
        hash.update(&scratch[..count]);
        left -= count as u64;
    }
    Ok(hash.finalize().into())
}
fn original_x_literal_payload_prefix_hash(wire_bytes: u64) -> Result<[u8; 32]> {
    ensure!(
        (8..=X_ROW_WIRE_BYTES).contains(&wire_bytes),
        "x payload hash prefix exceeds original geometry"
    );
    let mut hash = Sha256::new();
    hash.update(X_CELL_PREFIX);
    let scratch = [b'x'; SMALL];
    let mut left = wire_bytes - 8;
    while left != 0 {
        let count = left.min(SMALL as u64) as usize;
        hash.update(&scratch[..count]);
        left -= count as u64;
    }
    Ok(hash.finalize().into())
}

#[tokio::test]
async fn original_x_all_frozen_cuts_actual_full_one_mib_row_plus_err_and_original_handles_join()
-> Result<()> {
    for cut in [X_VALUE_BYTES - 1, X_VALUE_BYTES, X_VALUE_BYTES + 1] {
        let input = original_x(cut);
        // Same original component deadline origin as existing actual loopback tests.
        // It is captured before Peer/reader; no connect/metadata/row clock renewal.
        let original_deadline = Instant::now() + Duration::from_secs(2);
        let peer = Peer::start(Mode::XFullErr, input, original_deadline).await?;
        let port = peer.port;
        peer.run_observer(async move {
            let mut reader =
                ExactResultReader::connect("root", port, input, original_deadline).await?;
            let result = async {
                ensure!(
                    reader.connection_id() == 71
                        && reader
                            .receive_buffer_bytes()
                            .is_some_and(|n| n > 0 && n <= MAX_APPLIED_RECV),
                    "actual x handshake/receive buffer differs"
                );
                reader.send_original_query(ORIGINAL_X_SQL).await?;
                let metadata = reader.read_metadata().await?;
                ensure!(
                    metadata.columns == 1 && metadata.actual_next_sequence == 4,
                    "actual original x metadata differs"
                );
                let prefix = reader.read_cut_prefix().await?;
                ensure!(
                    prefix.row_wire_bytes == cut
                        && prefix.row_wire_sha256 == original_x_literal_wire_hash(cut)?,
                    "actual original x cut count/literal hash differs"
                );
                let complete = reader
                    .complete_row_then_interrupted(original_x_frozen_row_hash())
                    .await?;
                ensure!(
                    complete.row_wire_bytes == X_ROW_WIRE_BYTES
                        && complete.row_complete
                        && complete.interrupted_complete,
                    "actual 1MiB x row or complete ERR1317 differs"
                );
                ensure!(
                    complete.complete_row_sha256 == Some(original_x_frozen_row_hash())
                        && complete.row_wire_sha256
                            == original_x_literal_wire_hash(X_ROW_WIRE_BYTES)?,
                    "actual original x full-row golden/literal hashes differ"
                );
                ensure!(
                    complete.pending_header == [9, 0, 0, 5]
                        && complete.pending_payload_received == 9
                        && complete.terminal_wire_bytes == 13
                        && !complete.last_read_eof,
                    "actual x terminal is not complete bounded ERR at original sequence"
                );
                ensure!(
                    !complete.failed && Instant::now() < original_deadline,
                    "x scene succeeded outside original clock"
                );
                Ok::<_, anyhow::Error>(())
            }
            .await;
            drop(reader); // Same original TCP owner always exits before parent joins the peer.
            result
        })
        .await?;
    }
    Ok(())
}

#[tokio::test]
async fn original_x_partial_row_payload_actual_deadline_retains_prefix_and_original_source_after_ownerclose()
-> Result<()> {
    for cut in [X_VALUE_BYTES - 1, X_VALUE_BYTES, X_VALUE_BYTES + 1] {
        let input = original_x(cut);
        let original_origin = Instant::now();
        let original_deadline = original_origin + Duration::from_secs(2);
        let peer = Peer::start(Mode::XPartialPayload, input, original_deadline).await?;
        let port = peer.port;
        peer.run_observer(async move {
            let mut reader =
                ExactResultReader::connect("root", port, input, original_deadline).await?;
            let result = async {
                reader.send_original_query(ORIGINAL_X_SQL).await?;
                let metadata = reader.read_metadata().await?;
                ensure!(
                    metadata.columns == 1 && metadata.actual_next_sequence == 4,
                    "original x metadata did not finish"
                );
                let prefix = reader.read_cut_prefix().await?;
                ensure!(
                    prefix.row_wire_bytes == cut
                        && prefix.row_wire_sha256 == original_x_literal_wire_hash(cut)?,
                    "actual x cut was not received before the deadline tail read"
                );
                let failure = reader
                    .complete_row_then_interrupted(original_x_frozen_row_hash())
                    .await
                    .expect_err("open peer with incomplete x row must fail original deadline");
                let before = failure.observation;
                let source_address = failure
                    .cause
                    .downcast_ref::<tokio::time::error::Elapsed>()
                    .context("actual x payload deadline lost original Elapsed source")?
                    as *const _ as usize;
                ensure!(
                    Instant::now() >= original_deadline && before.phase == ExactPhase::RowTail,
                    "payload read used a renewed clock or failed in a different phase"
                );
                let received = cut + 3; // Original full rowwire is only S+8: do not accidentally complete it.
                ensure!(
                    received < X_ROW_WIRE_BYTES && before.row_wire_bytes == received,
                    "actual x partial read count differs from the external producer prefix"
                );
                ensure!(
                    before.pending_header == X_HEADER
                        && before.pending_header_bytes == 4
                        && before.pending_payload_length == Some(X_ROW_PAYLOAD_BYTES as u32)
                        && u64::from(before.pending_payload_received) == received - 4,
                    "actual x incomplete payload/header receipt was lost"
                );
                ensure!(
                    before.row_wire_sha256 == original_x_literal_wire_hash(received)?
                        && before.row_payload_prefix_sha256
                            == original_x_literal_payload_prefix_hash(received)?,
                    "actual x partial wire/payload hashes differ from independent literal bytes"
                );
                ensure!(
                    before.failed
                        && !before.last_read_eof
                        && !before.row_complete
                        && !before.interrupted_complete
                        && before.complete_row_sha256.is_none()
                        && before.terminal_wire_bytes == 0,
                    "payload deadline became EOF/fullrow/terminal success"
                );
                let current = reader.snapshot();
                ensure!(
                    current.row_wire_bytes == before.row_wire_bytes
                        && current.row_wire_sha256 == before.row_wire_sha256,
                    "actual owner lost its partial receipt on read failure"
                );
                Ok::<_, anyhow::Error>((failure, before, source_address))
            }
            .await;
            drop(reader);
            let (failure, before, source_address) = result?;
            let failure = move_owned_failure(failure);
            let after = failure
                .cause
                .downcast_ref::<tokio::time::error::Elapsed>()
                .context("original Elapsed source rebuilt after reader close/move")?
                as *const _ as usize;
            ensure!(
                after == source_address && std::error::Error::source(&failure).is_some(),
                "original actual deadline source object did not survive close/move"
            );
            ensure!(
                failure.observation.row_wire_bytes == before.row_wire_bytes
                    && failure.observation.row_payload_prefix_sha256
                        == before.row_payload_prefix_sha256,
                "moving original failure lost actual bounded partial observations"
            );
            Ok(())
        })
        .await?;
    }
    Ok(())
}
