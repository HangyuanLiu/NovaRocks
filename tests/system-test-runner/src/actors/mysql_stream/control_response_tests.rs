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

//! Actual loopback COM_QUERY components, not a Native FE/provider acceptance.
use super::*;
use std::{io, time::Duration};
use tokio::{
    io::AsyncWriteExt,
    net::{TcpListener, TcpStream},
    task::JoinHandle,
};

const OK: &[u8] = &[0, 0, 0, 2, 0, 0, 0];
const SQL: &str = "USE original_catalog.original_namespace";
const WATCHDOG: Duration = Duration::from_secs(3);

async fn pair() -> Result<(AsyncMysqlStream, TcpStream)> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let port = listener.local_addr()?.port();
    let (client, peer) = tokio::time::timeout(WATCHDOG, async {
        tokio::join!(TcpStream::connect(("127.0.0.1", port)), listener.accept())
    })
    .await?;
    let client = client?;
    let (peer, _) = peer?;
    Ok((
        AsyncMysqlStream {
            stream: client,
            timeout: Duration::from_secs(2),
            receive_buffer_bytes: None,
            connection_id: 1,
        },
        peer,
    ))
}
fn frame(sequence: u8, payload: &[u8]) -> Vec<u8> {
    let length = payload.len();
    let mut bytes = vec![
        length as u8,
        (length >> 8) as u8,
        (length >> 16) as u8,
        sequence,
    ];
    bytes.extend_from_slice(payload);
    bytes
}
async fn query(peer: &mut TcpStream) -> Result<Vec<u8>> {
    let mut header = [0; 4];
    peer.read_exact(&mut header).await?;
    let length =
        usize::from(header[0]) | (usize::from(header[1]) << 8) | (usize::from(header[2]) << 16);
    ensure!(
        header[3] == 0 && (1..=4096).contains(&length),
        "component query header differs"
    );
    let mut payload = vec![0; length];
    peer.read_exact(&mut payload).await?;
    Ok(payload)
}
struct PeerFacts {
    query: Vec<u8>,
    original_socket_end: io::Result<usize>,
    drip_write_failure: Option<io::Error>,
}
struct SettledPeer {
    watchdog: Option<tokio::time::error::Elapsed>,
    original_join: std::result::Result<Result<PeerFacts>, tokio::task::JoinError>,
}
async fn settle(mut peer: JoinHandle<Result<PeerFacts>>) -> SettledPeer {
    match tokio::time::timeout(WATCHDOG, &mut peer).await {
        Ok(original_join) => SettledPeer {
            watchdog: None,
            original_join,
        },
        Err(watchdog) => {
            peer.abort();
            let original_join = (&mut peer).await;
            SettledPeer {
                watchdog: Some(watchdog),
                original_join,
            }
        }
    }
}
fn facts(joined: SettledPeer) -> PeerFacts {
    // Assert only after the original handle actually completed, even if the
    // peer watchdog failed or the server panicked during client cleanup.
    assert!(
        joined.watchdog.is_none(),
        "original component peer needed watchdog abort"
    );
    joined
        .original_join
        .expect("original peer did not panic/cancel")
        .expect("original peer IO succeeded")
}
fn assert_closed(result: &io::Result<usize>) {
    match result {
        Ok(count) => assert_eq!(
            *count, 0,
            "peer received a new command instead of socket exit"
        ),
        Err(error) => assert_eq!(
            error.kind(),
            io::ErrorKind::ConnectionReset,
            "unexpected original peer read failure"
        ),
    }
}
async fn exchange(bytes: Vec<u8>) -> (Result<BoundedCommandResponse>, PeerFacts) {
    let (mut client, mut peer) = pair().await.unwrap();
    let peer = tokio::spawn(async move {
        let query = query(&mut peer).await?;
        peer.write_all(&bytes).await?;
        let mut byte = [0; 1];
        let original_socket_end = peer.read(&mut byte).await;
        Ok(PeerFacts {
            query,
            original_socket_end,
            drip_write_failure: None,
        })
    });
    let outcome = client
        .command_response_until(SQL, Instant::now() + Duration::from_secs(1))
        .await;
    drop(client);
    let joined = settle(peer).await;
    (outcome, facts(joined))
}
fn error_payload() -> Vec<u8> {
    let mut payload = vec![0xff];
    payload.extend_from_slice(&1105u16.to_le_bytes());
    payload.extend_from_slice(b"#HY000Unsupported: list_views is not supported by this catalog");
    payload
}
#[tokio::test]
async fn original_loopback_complete_ok_has_typed_fields_and_original_payload() {
    let (outcome, peer) = exchange(frame(1, OK)).await;
    assert_eq!(peer.query, [vec![3], SQL.as_bytes().to_vec()].concat());
    assert_closed(&peer.original_socket_end);
    let BoundedCommandResponse::Ok(value) = outcome.unwrap() else {
        panic!("ERR cannot be an OK")
    };
    assert_eq!(
        (
            value.affected_rows,
            value.last_insert_id,
            value.status_flags,
            value.warnings
        ),
        (0, 0, 2, 0)
    );
    assert!(value.info.is_empty());
    assert_eq!(value.original_payload, OK);
}
#[tokio::test]
async fn original_loopback_err_is_typed_error_with_bounded_message_and_raw_payload() {
    let payload = error_payload();
    let (outcome, peer) = exchange(frame(1, &payload)).await;
    assert_closed(&peer.original_socket_end);
    let BoundedCommandResponse::Error(value) = outcome.unwrap() else {
        panic!("ERR cannot be string success")
    };
    assert_eq!(value.error.code, 1105);
    assert_eq!(value.error.sqlstate, "HY000");
    assert_eq!(
        value.error.message,
        "Unsupported: list_views is not supported by this catalog"
    );
    assert_eq!(value.error.payload_bytes, payload.len());
    assert_eq!(value.original_payload, payload);
    assert_eq!(
        value.error.payload_hex,
        bounded_packet_hex(&value.original_payload)
    );
}
#[tokio::test]
async fn oversize_and_continuation_headers_fail_before_response_body_allocation() {
    for header in [[1, 16, 0, 1], [255, 255, 255, 1]] {
        let (outcome, peer) = exchange(header.to_vec()).await;
        assert_closed(&peer.original_socket_end);
        let error = outcome.unwrap_err();
        let failed = error.downcast_ref::<CommandResponseFailure>().unwrap();
        assert_eq!(failed.header, header);
        assert_eq!(failed.header_received, 4);
        assert!(failed.payload_prefix.is_empty());
        assert_eq!(failed.payload_prefix.capacity(), 0);
        assert!(failed.expected_payload_bytes.unwrap() > 4096);
    }
}
#[tokio::test]
async fn wrong_sequence_and_empty_headers_fail_before_body() {
    for header in [[7, 0, 0, 2], [0, 0, 0, 1]] {
        let (outcome, peer) = exchange(header.to_vec()).await;
        assert_closed(&peer.original_socket_end);
        let error = outcome.unwrap_err();
        let failed = error.downcast_ref::<CommandResponseFailure>().unwrap();
        assert_eq!(failed.header_received, 4);
        assert!(failed.payload_prefix.is_empty());
        assert_eq!(failed.payload_prefix.capacity(), 0);
    }
}
#[tokio::test]
async fn malformed_resultset_and_nonterminal_ok_are_not_command_success() {
    for payload in [
        vec![1],
        vec![0],
        vec![0, 251, 0, 2, 0, 0, 0],
        vec![0, 0, 0, 8, 0, 0, 0],
        vec![0xfe, 0, 0, 2, 0],
        vec![0xff, 0, 0, b'#', b'H', b'Y', b'0', b'0', b'0'],
        vec![0xff, 0x51, 0x04, b'x', b'H', b'Y', b'0', b'0', b'0'],
    ] {
        let (outcome, peer) = exchange(frame(1, &payload)).await;
        assert_closed(&peer.original_socket_end);
        let error = outcome.unwrap_err();
        let failed = error.downcast_ref::<CommandResponseFailure>().unwrap();
        assert_eq!(failed.payload_prefix, payload);
        assert_eq!(failed.header_received, 4);
    }
}
#[tokio::test]
async fn eof_retains_actual_partial_header_and_payload_io_cause() {
    for bytes in [vec![7, 0], vec![7, 0, 0, 1, 0, 0]] {
        let (mut client, mut peer) = pair().await.unwrap();
        let sent = bytes.clone();
        let peer = tokio::spawn(async move {
            let query = query(&mut peer).await?;
            peer.write_all(&sent).await?;
            peer.shutdown().await?;
            let mut byte = [0; 1];
            let original_socket_end = peer.read(&mut byte).await;
            Ok(PeerFacts {
                query,
                original_socket_end,
                drip_write_failure: None,
            })
        });
        let outcome = client
            .command_response_until(SQL, Instant::now() + Duration::from_secs(1))
            .await;
        drop(client);
        let peer = facts(settle(peer).await);
        assert_closed(&peer.original_socket_end);
        let error = outcome.unwrap_err();
        let failed = error.downcast_ref::<CommandResponseFailure>().unwrap();
        assert_eq!(failed.header_received, bytes.len().min(4));
        assert_eq!(failed.payload_prefix, &bytes[bytes.len().min(4)..]);
        // The zero-byte TCP read was actually observed; no made-up IO error.
        assert!(failed.actual_cause.to_string().contains("truncated"));
    }
}
#[tokio::test]
async fn same_absolute_clock_covers_partial_header_and_held_payload() {
    for bytes in [vec![7], vec![7, 0, 0, 1, 0]] {
        let (mut client, mut peer) = pair().await.unwrap();
        let sent = bytes.clone();
        let peer = tokio::spawn(async move {
            let query = query(&mut peer).await?;
            peer.write_all(&sent).await?;
            let mut byte = [0; 1];
            let original_socket_end = peer.read(&mut byte).await;
            Ok(PeerFacts {
                query,
                original_socket_end,
                drip_write_failure: None,
            })
        });
        let deadline = Instant::now() + Duration::from_millis(80);
        let outcome = client.command_response_until(SQL, deadline).await;
        let completed = Instant::now();
        drop(client);
        let peer = facts(settle(peer).await);
        assert_closed(&peer.original_socket_end);
        assert!(completed >= deadline);
        let error = outcome.unwrap_err();
        let failed = error.downcast_ref::<CommandResponseFailure>().unwrap();
        assert_eq!(failed.header_received, bytes.len().min(4));
        assert_eq!(failed.payload_prefix, &bytes[bytes.len().min(4)..]);
        assert!(failed.original_deadline_expired());
    }
}
#[tokio::test]
async fn drip_does_not_refresh_deadline_between_header_or_payload_reads() {
    let (mut client, mut peer) = pair().await.unwrap();
    let peer = tokio::spawn(async move {
        let query = query(&mut peer).await?;
        let mut drip_write_failure = None;
        for byte in frame(1, OK) {
            if let Err(error) = peer.write_all(&[byte]).await {
                drip_write_failure = Some(error);
                break;
            }
            tokio::time::sleep(Duration::from_millis(15)).await;
        }
        let mut byte = [0; 1];
        let original_socket_end = peer.read(&mut byte).await;
        Ok(PeerFacts {
            query,
            original_socket_end,
            drip_write_failure,
        })
    });
    let started = Instant::now();
    let deadline = started + Duration::from_millis(80);
    let outcome = client.command_response_until(SQL, deadline).await;
    let completed = Instant::now();
    drop(client);
    let peer = facts(settle(peer).await);
    assert_closed(&peer.original_socket_end);
    if let Some(error) = peer.drip_write_failure {
        assert!(
            matches!(
                error.kind(),
                io::ErrorKind::BrokenPipe | io::ErrorKind::ConnectionReset
            ),
            "unexpected actual drip writer failure"
        );
    }
    assert!(completed >= deadline && completed.duration_since(started) < Duration::from_secs(1));
    let error = outcome.unwrap_err();
    let failed = error.downcast_ref::<CommandResponseFailure>().unwrap();
    assert!(failed.original_deadline_expired());
    assert!(failed.header_received <= 4 && failed.payload_prefix.len() < OK.len());
}
#[tokio::test]
async fn expired_original_clock_rejects_before_sending_even_ready_socket() {
    let (mut client, mut peer) = pair().await.unwrap();
    let peer = tokio::spawn(async move {
        let mut byte = [0; 1];
        let original_socket_end = peer.read(&mut byte).await;
        Ok(PeerFacts {
            query: Vec::new(),
            original_socket_end,
            drip_write_failure: None,
        })
    });
    let outcome = client
        .command_response_until(SQL, Instant::now() - Duration::from_secs(1))
        .await;
    drop(client);
    let peer = facts(settle(peer).await);
    assert_closed(&peer.original_socket_end);
    let error = outcome.unwrap_err();
    let failed = error.downcast_ref::<CommandResponseFailure>().unwrap();
    assert_eq!(failed.header_received, 0);
    assert_eq!(failed.expected_payload_bytes, None);
    assert!(failed.payload_prefix.is_empty());
}
#[tokio::test]
async fn borrowed_command_cancel_preserves_original_socket_until_owner_drop() {
    let (mut client, mut peer) = pair().await.unwrap();
    let (query_seen, seen) = tokio::sync::oneshot::channel();
    let peer = tokio::spawn(async move {
        let query = query(&mut peer).await?;
        let _ = query_seen.send(());
        let mut byte = [0; 1];
        let original_socket_end = peer.read(&mut byte).await;
        Ok(PeerFacts {
            query,
            original_socket_end,
            drip_write_failure: None,
        })
    });
    let mut command =
        Box::pin(client.command_response_until(SQL, Instant::now() + Duration::from_secs(2)));
    let cancel=tokio::time::timeout(WATCHDOG,async {
        tokio::select! {biased;
            response=&mut command=>Err(anyhow::anyhow!("command completed before deliberate cancel: {}",response.is_ok())),
            received=seen=>received.map_err(Into::into),
        }
    }).await;
    drop(command);
    tokio::task::yield_now().await;
    let original_peer_still_pending = !peer.is_finished();
    drop(client);
    let peer = facts(settle(peer).await);
    assert!(
        original_peer_still_pending,
        "borrow cancellation must not close original socket"
    );
    assert_closed(&peer.original_socket_end);
    assert_eq!(peer.query, [vec![3], SQL.as_bytes().to_vec()].concat());
    cancel
        .expect("component cancel watchdog")
        .expect("original peer reached response wait");
}
#[test]
fn parser_rejects_bad_sqlstate_noncanonical_integer_and_truncated_ok_fields() {
    for payload in [
        vec![0, 252, 1, 0, 0, 2, 0, 0, 0],
        vec![0, 0, 0, 2],
        vec![0xff, 0x51, 0x04, b'#', b'h', b'y', b'0', b'0', b'0'],
        vec![0xff, 0x51, 0x04, b'#', b'0', b'0', b'0', b'0', b'0'],
    ] {
        assert!(decode(&payload).is_err());
    }
    assert!(decode(&[vec![0, 0, 0, 2, 0, 0, 0], b"bounded info".to_vec()].concat()).is_ok());
}

#[tokio::test]
#[allow(deprecated)]
async fn actual_tcp_reset_cause_is_retained_in_original_failure() {
    let (mut client, mut socket) = pair().await.unwrap();
    let mut peer = tokio::spawn(async move {
        query(&mut socket).await?;
        socket.set_linger(Some(Duration::ZERO))?;
        drop(socket);
        Ok::<_, anyhow::Error>(())
    });
    let outcome = client
        .command_response_until(SQL, Instant::now() + Duration::from_secs(1))
        .await;
    drop(client);
    let (watchdog, joined) = match tokio::time::timeout(WATCHDOG, &mut peer).await {
        Ok(joined) => (None, joined),
        Err(watchdog) => {
            peer.abort();
            (Some(watchdog), (&mut peer).await)
        }
    };
    assert!(watchdog.is_none());
    joined
        .expect("original reset peer joined")
        .expect("original reset peer completed");
    let error = outcome.unwrap_err();
    let failed = error.downcast_ref::<CommandResponseFailure>().unwrap();
    assert_eq!(
        failed
            .actual_cause
            .downcast_ref::<io::Error>()
            .unwrap()
            .kind(),
        io::ErrorKind::ConnectionReset
    );
    assert_eq!(failed.header_received, 0);
}
#[test]
fn retained_original_failure_formatter_is_not_invoked() {
    struct Actual;
    impl fmt::Debug for Actual {
        fn fmt(&self, _: &mut fmt::Formatter<'_>) -> fmt::Result {
            panic!("must not debug actual source")
        }
    }
    impl fmt::Display for Actual {
        fn fmt(&self, _: &mut fmt::Formatter<'_>) -> fmt::Result {
            panic!("must not format actual source")
        }
    }
    impl std::error::Error for Actual {}
    let error = Partial::default().failure(anyhow::Error::new(Actual));
    let failed = error.downcast_ref::<CommandResponseFailure>().unwrap();
    assert!(failed.actual_cause.downcast_ref::<Actual>().is_some());
    assert!(!failed.original_deadline_expired());
    assert_eq!(
        format!("{error:#}"),
        "bounded original MySQL command response failed"
    );
    assert!(format!("{failed:?}").contains("actual_cause_retained: true"));
}
