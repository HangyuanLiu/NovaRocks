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

// Real loopback original sockets; not authenticated Native role acceptance.
use super::*;
fn column() -> Vec<u8> {
    let mut payload = b"\x03def\x00\x00\x00\x01v\x00".to_vec();
    payload.extend([0x0c, 33, 0, 0xff, 0xff, 0xff, 0xff, 253, 0, 0, 0, 0, 0]);
    payload
}
async fn exchange(terminal: Option<Vec<u8>>) -> OwnedTextResultObservation {
    exchange_row(terminal, vec![1, b'7']).await
}
async fn exchange_row(terminal: Option<Vec<u8>>, row: Vec<u8>) -> OwnedTextResultObservation {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let client = tokio::net::TcpStream::connect(listener.local_addr().unwrap())
        .await
        .unwrap();
    let peer = tokio::spawn(async move {
        let (mut peer, _) = listener.accept().await?;
        let budget = Duration::from_secs(2);
        read_wire_packet_async(&mut peer, budget).await?;
        for (sequence, payload) in [
            (1, vec![1]),
            (2, column()),
            (3, vec![0xfe, 0, 0, 0, 0]),
            (4, row),
        ] {
            write_packet_async(&mut peer, sequence, &payload, budget).await?;
        }
        if let Some(terminal) = terminal {
            write_packet_async(&mut peer, 5, &terminal, budget).await?;
        }
        Ok::<(), anyhow::Error>(())
    });
    let mut original = AsyncMysqlStream {
        stream: client,
        timeout: Duration::from_secs(2),
        receive_buffer_bytes: None,
        connection_id: 23,
    };
    let result = original
        .observe_text_query_owned_until(
            "SELECT 7",
            Duration::ZERO,
            None,
            std::time::Instant::now() + Duration::from_secs(2),
        )
        .await;
    let peer_result = peer.await;
    drop(original);
    peer_result.unwrap().unwrap();
    result
}
#[tokio::test]
async fn complete_actual_err_retains_source_and_frozen_code_after_real_row() {
    let result = exchange(Some(b"\xff\x25\x05#70100interrupted".to_vec())).await;
    assert_eq!(result.server_result_error_code, Some(1317));
    assert!(result.actual_failure.is_some());
    assert_eq!(
        (result.observation.rows, result.observation.packets),
        (1, 5)
    );
    assert_eq!(
        result.observation.error.as_deref(),
        Some("original result failure; actual source retained")
    );
}
#[tokio::test]
async fn truncated_code_only_err_is_not_an_approved_server_terminal() {
    let result = exchange(Some(vec![0xff, 0x25, 0x05])).await;
    assert_eq!(result.server_result_error_code, None);
    assert!(result.actual_failure.is_some());
    assert_eq!(
        (result.observation.rows, result.observation.packets),
        (1, 5)
    );
}
#[tokio::test]
async fn oversized_actual_err_never_exposes_waivable_code() {
    let mut payload = b"\xff\x25\x05#70100".to_vec();
    payload.resize(4097, b'x');
    let result = exchange(Some(payload)).await;
    assert_eq!(result.server_result_error_code, None);
    assert!(result.actual_failure.is_some());
}
#[tokio::test]
async fn actual_eof_is_unknown_io_with_partial_wire_not_expected_1317() {
    let result = exchange(None).await;
    assert_eq!(result.server_result_error_code, None);
    assert!(result.actual_failure.is_some());
    assert_eq!(result.observation.rows, 1);
    assert!(result.observation.wire_bytes > 0);
}
#[tokio::test]
async fn expired_original_clock_sends_no_query_bytes() {
    use tokio::io::AsyncReadExt;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let client = tokio::net::TcpStream::connect(listener.local_addr().unwrap())
        .await
        .unwrap();
    let (mut peer, _) = listener.accept().await.unwrap();
    let mut original = AsyncMysqlStream {
        stream: client,
        timeout: Duration::from_secs(2),
        receive_buffer_bytes: None,
        connection_id: 23,
    };
    let result = original
        .observe_text_query_owned_until("SELECT 7", Duration::ZERO, None, std::time::Instant::now())
        .await;
    drop(original);
    let mut byte = [0; 1];
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(1), peer.read(&mut byte))
            .await
            .unwrap()
            .unwrap(),
        0
    );
    assert_eq!(result.observation.wire_bytes, 0);
    assert!(result.actual_failure.is_some());
}

#[tokio::test]
async fn metadata_pause_timeout_returns_partial_and_original_job_actually_joins() {
    use tokio::io::AsyncReadExt;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let client = tokio::net::TcpStream::connect(listener.local_addr().unwrap())
        .await
        .unwrap();
    let peer = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let budget = Duration::from_secs(2);
        read_wire_packet_async(&mut stream, budget).await.unwrap();
        for (sequence, payload) in [(1, vec![1]), (2, column()), (3, vec![0xfe, 0, 0, 0, 0])] {
            write_packet_async(&mut stream, sequence, &payload, budget)
                .await
                .unwrap();
        }
        let mut byte = [0; 1];
        tokio::time::timeout(budget, stream.read(&mut byte))
            .await
            .unwrap()
            .unwrap()
    });
    let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
    let (_resume, resume_rx) = tokio::sync::oneshot::channel();
    let mut original_job = tokio::spawn(async move {
        let mut original = AsyncMysqlStream {
            stream: client,
            timeout: Duration::from_secs(2),
            receive_buffer_bytes: None,
            connection_id: 23,
        };
        let deadline = std::time::Instant::now() + Duration::from_millis(200);
        let result = original
            .observe_text_query_owned_until(
                "SELECT 7",
                Duration::ZERO,
                Some((ready_tx, resume_rx)),
                deadline,
            )
            .await;
        (original, result)
    });
    tokio::time::timeout(Duration::from_secs(1), ready_rx)
        .await
        .unwrap()
        .unwrap();
    assert!(
        !original_job.is_finished(),
        "original job must still own paused TCP"
    );
    // The timed observer returns its own actual partial result. Borrow this same
    // JoinHandle until exit; no outer timeout discards the original worker.
    let (original, result) = (&mut original_job).await.unwrap();
    assert_eq!(
        (
            result.observation.columns,
            result.observation.packets,
            result.observation.rows
        ),
        (1, 3, 0)
    );
    assert!(result.observation.wire_bytes > 0);
    assert_eq!(result.server_result_error_code, None);
    assert!(
        result
            .actual_failure
            .as_ref()
            .unwrap()
            .downcast_ref::<tokio::time::error::Elapsed>()
            .is_some()
    );
    drop(original);
    assert_eq!(peer.await.unwrap(), 0);
}

#[tokio::test]
async fn full_segment_row_commits_hash_before_actual_bounded_err() {
    const SEGMENT: usize = 1_048_576;
    let mut row = vec![0xfd, 0, 0, 0x10];
    row.resize(SEGMENT + 4, b'x');
    let result = exchange_row(Some(b"\xff\x25\x05#70100interrupted".to_vec()), row).await;
    assert_eq!(result.server_result_error_code, Some(1317));
    assert!(result.actual_failure.is_some());
    assert_eq!(
        (result.observation.rows, result.observation.packets),
        (1, 5)
    );
    assert_eq!(result.observation.row_payload_bytes, (SEGMENT + 4) as u64);
    assert_eq!(
        result.observation.row_sha256,
        "e5a05e54f4636fe6e87eb8094fceda8e002bff799f3de76413a2a77c52fd50b8"
    );
}
