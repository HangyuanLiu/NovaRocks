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

//! Original loopback text sockets; no Native or HMS acceptance is inferred.
use super::*;
use std::{io, time::Instant};
use tokio::task::JoinHandle;

const SQL: &str = "SELECT schema_name, table_name, table_type FROM original_listing";
const WATCHDOG: Duration = Duration::from_secs(3);
const ROW: &[u8] = b"\x02ns\x03tab\x0aBASE TABLE";
const EOF: &[u8] = &[0xfe, 0, 0, 2, 0];

fn frame(sequence: u8, payload: &[u8]) -> Vec<u8> {
    let length = payload.len();
    let mut result = vec![
        length as u8,
        (length >> 8) as u8,
        (length >> 16) as u8,
        sequence,
    ];
    result.extend_from_slice(payload);
    result
}
fn column(name: &[u8]) -> Vec<u8> {
    let mut result = b"\x03def\x00\x00\x00".to_vec();
    result.push(name.len() as u8);
    result.extend_from_slice(name);
    result.push(0);
    result.extend_from_slice(&[0x0c, 33, 0, 0xff, 0xff, 0xff, 0xff, 253, 0, 0, 0, 0, 0]);
    result
}
fn metadata() -> Vec<u8> {
    let mut wire = frame(1, &[3]);
    for (sequence, name) in [
        (2, &b"schema_name"[..]),
        (3, &b"table_name"[..]),
        (4, &b"table_type"[..]),
    ] {
        wire.extend(frame(sequence, &column(name)));
    }
    wire.extend(frame(5, EOF));
    wire
}
fn complete() -> Vec<u8> {
    let mut wire = metadata();
    wire.extend(frame(6, ROW));
    wire.extend(frame(7, EOF));
    wire
}
async fn pair() -> Result<(AsyncMysqlStream, tokio::net::TcpStream)> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let (client, peer) = tokio::time::timeout(WATCHDOG, async {
        tokio::join!(tokio::net::TcpStream::connect(address), listener.accept())
    })
    .await?;
    let (peer, _) = peer?;
    Ok((
        AsyncMysqlStream {
            stream: client?,
            timeout: Duration::from_secs(2),
            receive_buffer_bytes: None,
            connection_id: 29,
        },
        peer,
    ))
}
struct OriginalPeerJoin {
    watchdog: Option<tokio::time::error::Elapsed>,
    result: std::result::Result<Result<io::Result<usize>>, tokio::task::JoinError>,
}
async fn settle(mut peer: JoinHandle<Result<io::Result<usize>>>) -> OriginalPeerJoin {
    match tokio::time::timeout(WATCHDOG, &mut peer).await {
        Ok(result) => OriginalPeerJoin {
            watchdog: None,
            result,
        },
        Err(watchdog) => {
            peer.abort();
            let result = (&mut peer).await;
            OriginalPeerJoin {
                watchdog: Some(watchdog),
                result,
            }
        }
    }
}
fn assert_joined(join: OriginalPeerJoin) {
    // The original task has actually joined before any assertion, including
    // on watchdog, panic or original IO failure.
    assert!(
        join.watchdog.is_none(),
        "original loopback peer required cleanup abort"
    );
    let actual_end = join
        .result
        .expect("original peer joined without panic/cancel")
        .expect("original peer query/response succeeded");
    match actual_end {
        Ok(count) => assert_eq!(count, 0),
        // Our original client Drop with rejected unread bytes may reset this
        // same peer. Preserve and check its actual IO; do not fabricate EOF.
        Err(error) => assert_eq!(error.kind(), io::ErrorKind::ConnectionReset),
    }
}
async fn exchange(
    wire: Vec<u8>,
    single_column: bool,
    budget: Duration,
) -> OwnedTextResultObservation {
    let (mut client, mut stream) = pair().await.unwrap();
    let peer = tokio::spawn(async move {
        let (sequence, query) = read_wire_packet_async(&mut stream, WATCHDOG).await?;
        ensure!(
            sequence == 0 && query == [vec![3], SQL.as_bytes().to_vec()].concat(),
            "original query differs"
        );
        stream.write_all(&wire).await?;
        let mut byte = [0; 1];
        Ok(stream.read(&mut byte).await)
    });
    let deadline = Instant::now() + budget;
    let result = if single_column {
        client
            .observe_text_query_owned_until(SQL, Duration::ZERO, None, deadline)
            .await
    } else {
        client
            .observe_three_column_text_query_owned_until(SQL, deadline)
            .await
    };
    drop(client);
    let joined = settle(peer).await;
    assert_joined(joined);
    result
}

#[tokio::test]
async fn three_columns_complete_actual_row_matches_literal_schema_and_digest() {
    let wire = complete();
    let expected_wire_bytes = wire.len() as u64;
    let result = exchange(wire, false, Duration::from_secs(1)).await;
    assert!(result.actual_failure.is_none());
    assert_eq!(result.server_result_error_code, None);
    assert_eq!(
        (
            result.observation.columns,
            result.observation.rows,
            result.observation.packets
        ),
        (3, 1, 7)
    );
    assert_eq!(result.observation.wire_bytes, expected_wire_bytes);
    assert_eq!(
        result.observation.schema,
        [
            TextColumnObservation {
                name: "schema_name".into(),
                mysql_type: 253
            },
            TextColumnObservation {
                name: "table_name".into(),
                mysql_type: 253
            },
            TextColumnObservation {
                name: "table_type".into(),
                mysql_type: 253
            },
        ]
    );
    assert_eq!(result.observation.row_payload_bytes, 18);
    assert_eq!(
        result.observation.row_sha256,
        "69a0d627a03bf5d7745b878c4c2d6d30fcf9f34e22b3a743b962a50ff80e6986"
    );
}

#[tokio::test]
async fn three_columns_rejects_wrong_count_and_unknown_count_encoding() {
    for count in [vec![1], vec![2], vec![4], vec![0xfb], vec![3, 0]] {
        let result = exchange(frame(1, &count), false, Duration::from_secs(1)).await;
        assert!(result.actual_failure.is_some());
        assert_eq!(result.server_result_error_code, None);
        assert_eq!(result.observation.rows, 0);
        assert_eq!(result.observation.packets, 1);
        assert!(result.observation.schema.is_empty());
    }
}

#[tokio::test]
async fn original_one_column_owned_observer_still_rejects_three_columns() {
    let result = exchange(complete(), true, Duration::from_secs(1)).await;
    assert!(result.actual_failure.is_some());
    assert_eq!(
        (result.observation.rows, result.observation.packets),
        (0, 1)
    );
    assert!(result.observation.schema.is_empty());
}

#[tokio::test]
async fn three_columns_rejects_unknown_or_nonterminal_eof_after_actual_row() {
    for terminal in [vec![0], vec![0xfe, 0, 0, 8, 0]] {
        let mut wire = metadata();
        wire.extend(frame(6, ROW));
        wire.extend(frame(7, &terminal));
        let result = exchange(wire, false, Duration::from_secs(1)).await;
        assert!(result.actual_failure.is_some());
        assert_eq!(result.server_result_error_code, None);
        assert_eq!(result.observation.rows, 1);
        assert_eq!(result.observation.packets, 7);
    }
}

#[tokio::test]
async fn three_columns_partial_row_returns_actual_prefix_under_original_deadline() {
    let mut wire = metadata();
    wire.extend_from_slice(&[18, 0, 0, 6, 2, b'n']);
    let expected_wire_bytes = wire.len() as u64;
    let result = exchange(wire, false, Duration::from_millis(80)).await;
    assert!(
        result
            .actual_failure
            .as_ref()
            .unwrap()
            .is::<tokio::time::error::Elapsed>()
    );
    assert_eq!(result.observation.columns, 3);
    assert_eq!(result.observation.rows, 0);
    assert_eq!(result.observation.wire_bytes, expected_wire_bytes);
    assert_eq!(result.server_result_error_code, None);
    assert_eq!(
        result.observation.error.as_deref(),
        Some("absolute query deadline exceeded")
    );
    assert!(result.observation.elapsed_micros < 1_000_000);
}

#[tokio::test]
async fn three_columns_expired_clock_sends_no_original_query() {
    let (mut client, mut stream) = pair().await.unwrap();
    let peer = tokio::spawn(async move {
        let mut byte = [0; 1];
        Ok::<_, anyhow::Error>(stream.read(&mut byte).await)
    });
    let result = client
        .observe_three_column_text_query_owned_until(SQL, Instant::now() - Duration::from_secs(1))
        .await;
    drop(client);
    let joined = settle(peer).await;
    assert_joined(joined);
    assert!(result.actual_failure.is_some());
    assert_eq!(
        (result.observation.wire_bytes, result.observation.rows),
        (0, 0)
    );
}
