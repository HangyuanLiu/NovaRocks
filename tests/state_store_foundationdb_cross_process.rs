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

#![cfg(all(feature = "foundationdb-provider", feature = "state-store-test-hooks"))]

use std::io::{BufRead, BufReader, Write};
use std::process::{Child, ChildStdin, Command as ProcessCommand, Stdio};
use std::sync::mpsc::{self, Receiver};
use std::thread::JoinHandle;
use std::time::Duration;

use serde_json::{Value as JsonValue, json};
use uuid::Uuid;

#[allow(dead_code)]
#[path = "support/state_store_foundationdb_helper.rs"]
mod helper;

#[test]
fn helper_protocol_accepts_only_the_frozen_command_set_and_hex_payloads() {
    let open = helper::parse_command(
        r#"{"command":"Open","cluster_id":"cross-process","keyspace_id":"00000000-0000-4000-8000-000000000001"}"#,
    )
    .expect("parse Open");
    assert!(matches!(open, helper::Command::Open { .. }));

    let put = helper::parse_command(
        r#"{"command":"Put","transaction_id":"00000000-0000-4000-8000-000000000002","key":"00ff","value":"ff00","precondition":"Any"}"#,
    )
    .expect("parse binary Put");
    assert!(matches!(
        put,
        helper::Command::Put {
            key,
            value,
            ..
        } if key == vec![0x00, 0xff] && value == vec![0xff, 0x00]
    ));

    for forbidden in ["Pause", "Barrier", "Sleep", "Inject", "Crash"] {
        let input = format!(r#"{{"command":"{forbidden}"}}"#);
        assert!(
            helper::parse_command(&input).is_err(),
            "helper protocol must reject {forbidden}"
        );
    }
    assert!(
        helper::parse_command(
            r#"{"command":"Put","transaction_id":"00000000-0000-4000-8000-000000000002","key":"not-hex","value":"00","precondition":"Any"}"#,
        )
        .is_err(),
        "helper protocol must reject non-hex payloads"
    );
    assert!(
        helper::parse_command(
            r#"{"command":"Get","transaction_id":"00000000-0000-4000-8000-000000000002","key":"00","unexpected":true}"#,
        )
        .is_err(),
        "helper protocol must reject unknown fields"
    );
    assert!(
        helper::parse_command(
            r#"{"command":"Put","transaction_id":"00000000-0000-4000-8000-000000000002","key":"00","value":"01","precondition":{"version":"00","unexpected":true}}"#,
        )
        .is_err(),
        "helper protocol must reject unknown nested precondition fields"
    );
}

struct HelperProcess {
    child: Child,
    stdin: ChildStdin,
    responses: Receiver<String>,
    reader: Option<JoinHandle<()>>,
}

impl HelperProcess {
    fn spawn() -> Self {
        let mut child = ProcessCommand::new(env!("CARGO_BIN_EXE_state-store-foundationdb-helper"))
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()
            .expect("spawn FoundationDB helper process");
        let stdin = child.stdin.take().expect("take helper stdin");
        let stdout = child.stdout.take().expect("take helper stdout");
        let (responses_tx, responses) = mpsc::channel();
        let reader = std::thread::spawn(move || {
            for line in BufReader::new(stdout).lines() {
                let Ok(line) = line else {
                    break;
                };
                if responses_tx.send(line).is_err() {
                    break;
                }
            }
        });
        Self {
            child,
            stdin,
            responses,
            reader: Some(reader),
        }
    }

    fn id(&self) -> u32 {
        self.child.id()
    }

    fn send(&mut self, command: JsonValue) {
        serde_json::to_writer(&mut self.stdin, &command).expect("encode helper command");
        self.stdin
            .write_all(b"\n")
            .expect("write helper command delimiter");
        self.stdin.flush().expect("flush helper command");
    }

    fn receive(&mut self) -> helper::Response {
        let line = self
            .responses
            .recv_timeout(Duration::from_secs(10))
            .expect("helper must respond within ten seconds");
        let response: helper::Response =
            serde_json::from_str(&line).expect("decode helper response");
        assert!(
            response.ok,
            "helper command failed: event={}, error={:?}",
            response.event, response.error
        );
        response
    }

    fn request(&mut self, command: JsonValue) -> helper::Response {
        self.send(command);
        self.receive()
    }

    fn shutdown(mut self) {
        let response = self.request(json!({"command": "Shutdown"}));
        assert_eq!(response.event, "Shutdown");
        let status = self.child.wait().expect("wait for helper shutdown");
        assert!(status.success(), "helper shutdown status: {status}");
        if let Some(reader) = self.reader.take() {
            reader.join().expect("join helper response reader");
        }
    }
}

impl Drop for HelperProcess {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        if let Some(reader) = self.reader.take() {
            let _ = reader.join();
        }
    }
}

fn transaction() -> Uuid {
    Uuid::now_v7()
}

fn begin(helper: &mut HelperProcess, transaction_id: Uuid, description: &str) {
    let response = helper.request(json!({
        "command": "Begin",
        "transaction_id": transaction_id,
        "description": description,
    }));
    assert_eq!(response.event, "Begun");
}

fn put(
    helper: &mut HelperProcess,
    transaction_id: Uuid,
    key: &[u8],
    value: &[u8],
    precondition: &str,
) {
    let response = helper.request(json!({
        "command": "Put",
        "transaction_id": transaction_id,
        "key": hex::encode(key),
        "value": hex::encode(value),
        "precondition": precondition,
    }));
    assert_eq!(response.event, "Staged");
}

fn commit(helper: &mut HelperProcess, transaction_id: Uuid) -> helper::Response {
    helper.request(json!({
        "command": "Commit",
        "transaction_id": transaction_id,
    }))
}

fn assert_outcome(response: &helper::Response, expected: &str) {
    assert_eq!(response.event, "Commit");
    assert_eq!(response.outcome.as_deref(), Some(expected));
}

fn resolve(helper: &mut HelperProcess, transaction_id: Uuid) -> helper::Response {
    helper.request(json!({
        "command": "Resolve",
        "transaction_id": transaction_id,
    }))
}

#[test]
fn foundationdb_cross_process_suite() {
    let keyspace_id = Uuid::new_v4();
    let mut left = HelperProcess::spawn();
    let mut right = HelperProcess::spawn();

    let left_open = left.request(json!({
        "command": "Open",
        "cluster_id": "cross-process-cluster",
        "keyspace_id": keyspace_id,
    }));
    let right_open = right.request(json!({
        "command": "Open",
        "cluster_id": "cross-process-cluster",
        "keyspace_id": keyspace_id,
    }));
    assert_eq!(left_open.event, "Opened");
    assert_eq!(right_open.event, "Opened");
    assert_eq!(left_open.pid, left.id());
    assert_eq!(right_open.pid, right.id());
    assert_ne!(
        left_open.pid, right_open.pid,
        "helpers must be separate execs"
    );

    let seed_id = transaction();
    begin(&mut left, seed_id, "seed same-key conflict");
    put(&mut left, seed_id, b"same-key", b"seed", "Any");
    assert_outcome(&commit(&mut left, seed_id), "Committed");

    let same_left = transaction();
    let same_right = transaction();
    begin(&mut left, same_left, "same-key left");
    begin(&mut right, same_right, "same-key right");
    for (helper, transaction_id) in [(&mut left, same_left), (&mut right, same_right)] {
        let response = helper.request(json!({
            "command": "Get",
            "transaction_id": transaction_id,
            "key": hex::encode(b"same-key"),
        }));
        assert_eq!(
            response.record.expect("same-key seed").value,
            hex::encode(b"seed")
        );
        put(
            helper,
            transaction_id,
            b"same-key",
            transaction_id.as_bytes(),
            "Present",
        );
    }
    left.send(json!({"command": "Commit", "transaction_id": same_left}));
    right.send(json!({"command": "Commit", "transaction_id": same_right}));
    let same_outcomes = [
        left.receive().outcome.expect("left same-key outcome"),
        right.receive().outcome.expect("right same-key outcome"),
    ];
    assert_eq!(
        same_outcomes
            .iter()
            .filter(|outcome| outcome.as_str() == "Committed")
            .count(),
        1,
        "same-key commits must have exactly one winner: {same_outcomes:?}"
    );
    assert_eq!(
        same_outcomes
            .iter()
            .filter(|outcome| outcome.as_str() == "Conflict")
            .count(),
        1,
        "same-key commits must have exactly one conflict: {same_outcomes:?}"
    );

    let disjoint_left = transaction();
    let disjoint_right = transaction();
    begin(&mut left, disjoint_left, "disjoint left");
    begin(&mut right, disjoint_right, "disjoint right");
    put(&mut left, disjoint_left, b"disjoint-left", b"left", "Any");
    put(
        &mut right,
        disjoint_right,
        b"disjoint-right",
        b"right",
        "Any",
    );
    left.send(json!({"command": "Commit", "transaction_id": disjoint_left}));
    right.send(json!({"command": "Commit", "transaction_id": disjoint_right}));
    assert_outcome(&left.receive(), "Committed");
    assert_outcome(&right.receive(), "Committed");

    let phantom_reader = transaction();
    begin(&mut left, phantom_reader, "range phantom reader");
    let page = left.request(json!({
        "command": "Range",
        "transaction_id": phantom_reader,
        "start": hex::encode(b"phantom/"),
        "end": hex::encode(b"phantom0"),
        "direction": "Forward",
        "page_size": 10,
    }));
    assert!(page.records.is_empty(), "phantom range must start empty");
    let phantom_writer = transaction();
    begin(&mut right, phantom_writer, "range phantom writer");
    put(
        &mut right,
        phantom_writer,
        b"phantom/key",
        b"inserted",
        "Any",
    );
    assert_outcome(&commit(&mut right, phantom_writer), "Committed");
    put(
        &mut left,
        phantom_reader,
        b"phantom-result",
        b"must-conflict",
        "Any",
    );
    assert_outcome(&commit(&mut left, phantom_reader), "Conflict");

    let committed_resolution = resolve(&mut right, disjoint_left);
    assert_eq!(
        committed_resolution.resolution.as_deref(),
        Some("Committed")
    );

    let pending_id = transaction();
    begin(&mut left, pending_id, "held cross-process resolution");
    put(
        &mut left,
        pending_id,
        b"pending-key",
        b"pending-value",
        "Any",
    );
    let held = left.request(json!({
        "command": "Commit",
        "transaction_id": pending_id,
        "hold_pre_native": true,
    }));
    assert_eq!(held.event, "CommitHeld");
    let pending_resolution = resolve(&mut right, pending_id);
    assert_eq!(pending_resolution.resolution.as_deref(), Some("Pending"));
    let released = left.request(json!({
        "command": "Release",
        "transaction_id": pending_id,
    }));
    assert_outcome(&released, "Committed");
    assert_eq!(
        resolve(&mut right, pending_id).resolution.as_deref(),
        Some("Committed")
    );

    let absent_id = transaction();
    assert_eq!(
        resolve(&mut right, absent_id).resolution.as_deref(),
        Some("NotCommitted")
    );

    left.shutdown();
    right.shutdown();
}
