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

//! P09 real FE semantic refusal after a bounded authenticated native reply fault.
//! Requires the separately reviewed actor v2 and runner launch opt-in integration.

use super::result_delivery::root_census;
use super::result_delivery_baseline::await_idle;
use super::result_delivery_root_protocol::parse_created_task_candidates;
use crate::actors::mysql_stream::{
    AsyncMysqlStream, BoundedNegativeTextObservation, TextColumnObservation, TextResultObservation,
};
use crate::scenario::{Scenario, ScenarioContext, ScenarioLaunchConfig};
use anyhow::{Context, Result, ensure};
use novarocks_cluster_harness::native_root_reply_fault::{
    RootReplyFaultBounds, RootReplyFaultObservation, RootReplyMutation,
};
use novarocks_cluster_harness::{CrossProcessRootReplyFaultConfig, LaunchProfile, ServerHandle};
use novarocks_execution_contract::identity::TaskIdentity;
use novarocks_proto_codec::FieldPath;
use novarocks_task_codec::budget::TransportBudget;
use novarocks_task_codec::identity::decode_task_identity;
use novarocks_types::{BackendProcessId, FrontendProcessId};
use serde::Deserialize;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::io::Read;
use std::path::Path;
use std::time::{Duration, Instant};

const CAPTURE: Duration = Duration::from_secs(5);
const CLIENT: Duration = Duration::from_secs(20);
const LOG_CAP: usize = 2 * 1024 * 1024;
const METRIC_CAP: usize = 1024 * 1024;
const CREATE: &str = "NOVAROCKS_TASK_CREATE_APPLIED";
const ESTABLISH: &str = "NOVAROCKS_TASK_CONTEXT_ESTABLISH_APPLIED";
const SQL: &str = "SELECT REPEAT('x',64) AS payload";
const HEALTH: &str = "SELECT SUM(generate_series) AS total FROM generate_series(1, 100)";

#[derive(Clone, Copy)]
enum Case {
    Profile,
    Kind,
    Prefix,
}
impl Case {
    fn mutation(self) -> RootReplyMutation {
        match self {
            Self::Profile => RootReplyMutation::ProfileTwo,
            Self::Kind => RootReplyMutation::ClientRowsFalse,
            Self::Prefix => RootReplyMutation::FourBytePrefixOnly,
        }
    }
    fn freeze(self) -> &'static str {
        match self {
            Self::Profile => include_str!(
                "../../../../docs/testing/mem-1-m07/inputs/native-root-reply-profile-freeze-v1.json"
            ),
            Self::Kind => include_str!(
                "../../../../docs/testing/mem-1-m07/inputs/native-root-reply-kind-freeze-v1.json"
            ),
            Self::Prefix => include_str!(
                "../../../../docs/testing/mem-1-m07/inputs/native-root-reply-prefix-only-freeze-v1.json"
            ),
        }
    }
}
struct NativeRootReplyRefusal(Case);
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Freeze {
    schema_version: u32,
    status: String,
    scenario: String,
    topology: String,
    sql: String,
    mutation: String,
    contract_reason: String,
    capture_millis: u64,
    maximum_target_attempts: u64,
    forward_millis: u64,
    mysql_packet_cap: usize,
    mysql_error_code: u16,
    mysql_sqlstate: String,
    metadata_oracle: String,
    root_meta_proof: String,
    expected_native_body_sha256: String,
    expected_row_sha256: String,
    health_row_sha256: String,
}
pub(super) fn scenarios() -> Vec<Box<dyn Scenario>> {
    [Case::Profile, Case::Kind, Case::Prefix]
        .into_iter()
        .map(|case| Box::new(NativeRootReplyRefusal(case)) as Box<dyn Scenario>)
        .collect()
}
#[derive(serde::Serialize)]
struct NegativeClientAttempt {
    connection_stage: &'static str,
    connection_wire: &'static str,
    connection_failure: Value,
    mysql: Option<BoundedNegativeTextObservation>,
}
async fn negative_client(
    user: String,
    port: u16,
    ready: tokio::sync::oneshot::Sender<()>,
    deadline: Instant,
) -> NegativeClientAttempt {
    let remaining = deadline.saturating_duration_since(Instant::now());
    if remaining.is_zero() {
        let cause = anyhow::anyhow!("absolute client deadline elapsed before connect attempt");
        return NegativeClientAttempt {
            connection_stage: "absolute-deadline-before-connect",
            connection_wire: "not-attempted",
            connection_failure: failure_summary("mysql-connect-deadline", Some(&cause)),
            mysql: None,
        };
    }
    let connect = tokio::time::timeout_at(
        tokio::time::Instant::from_std(deadline),
        AsyncMysqlStream::connect(&user, port, remaining),
    )
    .await;
    match connect {
        Ok(Ok(mut stream)) => NegativeClientAttempt {
            connection_stage: "authenticated",
            connection_wire: "handshake-bytes-unobserved",
            connection_failure: Value::Null,
            mysql: Some(
                stream
                    .observe_bounded_contract_error(SQL, ready, deadline)
                    .await,
            ),
        },
        Ok(Err(error)) => NegativeClientAttempt {
            connection_stage: "failed-before-observer",
            connection_wire: "unknown-partial-handshake",
            connection_failure: failure_summary("mysql-connect", Some(&error)),
            mysql: None,
        },
        Err(error) => {
            let cause = anyhow::Error::new(error);
            NegativeClientAttempt {
                connection_stage: "absolute-deadline-before-observer",
                connection_wire: "unknown-partial-handshake",
                connection_failure: failure_summary("mysql-connect-deadline", Some(&cause)),
                mysql: None,
            }
        }
    }
}

impl Scenario for NativeRootReplyRefusal {
    fn name(&self) -> &'static str {
        match self.0 {
            Case::Profile => "result-delivery/native-root-reply-profile-refusal",
            Case::Kind => "result-delivery/native-root-reply-kind-refusal",
            Case::Prefix => "result-delivery/native-root-reply-prefix-only-refusal",
        }
    }
    fn is_explicit_stage(&self) -> bool {
        true
    }
    fn validate_runner_inputs(&self, profile: LaunchProfile, _: Option<&Path>) -> Result<()> {
        ensure!(
            profile == LaunchProfile::FaultScenario,
            "native reply refusal requires task observation FaultScenario profile"
        );
        Ok(())
    }
    fn launch_config(&self, _: &Path) -> Result<ScenarioLaunchConfig> {
        let mut config = ScenarioLaunchConfig::default();
        // Select the bounded RootReply actor explicitly for these scenarios.
        // Ordinary scenarios keep the existing TCP forwarding path.
        config.native_root_reply_fault = Some(CrossProcessRootReplyFaultConfig {
            backend_indices: (0..3).collect(),
            bounds: RootReplyFaultBounds::default(),
        });
        // Existing Control TCP path remains in Forward state with its usual cap.
        config.native_fault_proxies.backend_retained_byte_limits =
            (0..3).map(|i| (i, 1024 * 1024)).collect();
        Ok(config)
    }
    fn run(&self, context: &mut ScenarioContext) -> Result<()> {
        ensure!(
            context.handle().be_count() == 3,
            "native root reply refusal requires 1FE+3BE"
        );
        let freeze: Freeze = serde_json::from_str(self.0.freeze())?;
        ensure!(
            freeze.schema_version == 1
                && freeze.status == "draft-unexecuted"
                && freeze.scenario == self.name()
                && freeze.topology == "1FE+3BE"
                && freeze.sql == SQL
                && freeze.mutation == format!("{:?}", self.0.mutation())
                && freeze.capture_millis == 5000
                && freeze.maximum_target_attempts == 32
                && freeze.forward_millis == 20000
                && freeze.mysql_packet_cap == 4096
                && freeze.mysql_error_code == 1105
                && freeze.mysql_sqlstate == "HY000"
                && freeze.metadata_oracle
                    == "complete-same-SQL-successful-native-baseline-plus-fixed-encoder-fields"
                && freeze.root_meta_proof
                    == "known-semantic-profile-purpose-schema-not-exact-protobuf-bytes",
            "unsupported frozen negative contract"
        );
        let control = context.handle().native_root_reply_fault(0)?;
        let epoch = Instant::now();
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()?;
        let mut job = None;
        let mut evidence = json!({"schema_version":1,"scenario":self.name(),"freeze":serde_json::from_str::<Value>(self.0.freeze())?});
        let outcome = (|| -> Result<()> {
            check_effective(context, &mut evidence)?;
            context.recheck_live_process_launch_identities()?;
            await_idle(context, "root-reply", "before-baseline", epoch)?;
            zero_roots(context, "before-baseline", &mut evidence)?;
            let baseline = positive_query(context, &runtime, SQL)?;
            ensure!(
                baseline.error.is_none()
                    && baseline.rows == 1
                    && baseline.row_payload_bytes == 65
                    && baseline.packets == 5
                    && baseline.columns == 1
                    && baseline.schema
                        == [TextColumnObservation {
                            name: "payload".to_owned(),
                            mysql_type: 253
                        }]
                    && baseline.row_sha256 == freeze.expected_row_sha256,
                "same-SQL native metadata baseline differs from frozen scalar row"
            );
            let metadata_hash = baseline.metadata_sha256.clone();
            evidence["metadata_baseline"] = serde_json::to_value(baseline)?;
            await_idle(context, "root-reply", "before-capture", epoch)?;
            zero_roots(context, "before-capture", &mut evidence)?;
            let before = logs(context)?;
            let processes = (0..3)
                .map(|i| context.handle().backend_process_id(i))
                .collect::<Result<Vec<_>>>()?;
            let user = context.mysql_user().to_owned();
            let port = context.mysql_port();
            let (ready_tx, mut ready_rx) = tokio::sync::oneshot::channel();
            ensure!(
                context.remaining("absolute capture and client join")? >= CLIENT,
                "insufficient remaining scenario budget for bounded query join"
            );
            let client_started = Instant::now();
            let client_deadline = client_started + CLIENT;
            let deadline = client_started + CAPTURE;
            control.begin_capture(deadline)?;
            job = Some(runtime.spawn(negative_client(user, port, ready_tx, client_deadline)));
            let mut snapshots = Vec::with_capacity(51);
            let mut metadata_ready = false;
            loop {
                ensure!(
                    Instant::now() < deadline && snapshots.len() < 51,
                    "exact root proof exceeded original capture clock"
                );
                ensure!(
                    !job.as_ref()
                        .context("missing negative client")?
                        .is_finished(),
                    "negative client exited before exact arm"
                );
                if !metadata_ready {
                    match ready_rx.try_recv() {
                        Ok(()) => metadata_ready = true,
                        Err(tokio::sync::oneshot::error::TryRecvError::Empty) => {}
                        Err(_) => anyhow::bail!("negative metadata observer closed before arm"),
                    }
                }
                let roots = census(context, deadline)?;
                snapshots.push(json!({"epoch_micros":epoch.elapsed().as_micros(),"roots":roots}));
                evidence["capture_samples"] = json!(snapshots);
                if roots.iter().all(Option::is_some) {
                    let occupied: Vec<_> = roots
                        .iter()
                        .enumerate()
                        .filter_map(|(i, r)| {
                            (r.as_ref().expect("checked availability")["channels"] != 0)
                                .then_some(i)
                        })
                        .collect();
                    ensure!(
                        occupied.len() <= 1,
                        "multiple fresh root owners cannot establish exact target"
                    );
                    if let [be] = occupied.as_slice() {
                        let root = roots[*be].as_ref().expect("available root");
                        if root["channels"] == 1
                            && root["terminal_task_records"] == 1
                            && root["producers_running"] == 0
                            && root["producers_exited"] == 1
                            && root["ends_published"] == 1
                            && root["ends_acknowledged"] == 0
                            && root["sealed"] == 0
                            && root["data_positions"] == 1
                            && root["segments"] == 1
                            && root["payload_bytes"] == 69
                            && roots.iter().enumerate().all(|(i, r)| {
                                i == *be || r.as_ref().expect("available").values().all(|n| *n == 0)
                            })
                            && metadata_ready
                        {
                            let after = logs(context)?;
                            let (task, frontend) =
                                independent_target(&before, &after, *be, processes[*be])?;
                            context.recheck_live_process_launch_identities()?;
                            if let Some(candidate) = control.candidate() {
                                ensure!(
                                    candidate.backend_index == *be
                                        && candidate.caller == frontend
                                        && candidate.read.root_task() == task
                                        && candidate.read.profile().get() == 1
                                        && candidate.read.kind()
                                            == novarocks_result_contract::RootOutputKind::ClientRows
                                        && candidate.read.consumed() == 0
                                        && candidate.read.wanted().map(|n| n.get()) == Some(1),
                                    "candidate differs from independently unique native task/frontend/root"
                                );
                                evidence["exact_target"] = json!({"backend_index":be,"backend_process":processes[*be].to_string(),
                                    "task":task.to_string(),"frontend_process":frontend.to_string(),
                                    "independent_markers":appended_markers(&before,&after)?,
                                    "root_meta_known_facts":{"profile":1,"kind":"ClientRows","schema":[{"name":"payload","mysql_type":253}]},
                                    "metadata_sha256":metadata_hash,"candidate_request_sha256":candidate.request_sha256});
                                control.arm_exact(task, frontend, *be, self.0.mutation())?;
                                break;
                            }
                        }
                    }
                }
                std::thread::sleep(
                    Duration::from_millis(100)
                        .min(deadline.saturating_duration_since(Instant::now())),
                );
            }
            let observed = runtime
                .block_on(job.take().context("negative client missing at join")?)
                .context("negative client task failed while joining")?;
            evidence["negative_client"] = serde_json::to_value(&observed)?;
            let mysql = observed.mysql.as_ref().context(
                "connection failed before MySQL observer; partial connection wire unknown",
            )?;
            check_mysql(mysql, &freeze, &metadata_hash)?;
            check_fault(&control.observation(), &freeze, self.0)?;
            evidence["fault_complete"] = serde_json::to_value(control.observation())?;
            control.disarm();
            await_idle(context, "root-reply", "after-refusal", epoch)?;
            zero_roots(context, "after-refusal", &mut evidence)?;
            let health = positive_query(context, &runtime, HEALTH)?;
            ensure!(
                health.error.is_none()
                    && health.rows == 1
                    && health.row_payload_bytes == 5
                    && health.packets == 5
                    && health.row_sha256 == freeze.health_row_sha256
                    && health.schema
                        == [TextColumnObservation {
                            name: "total".to_owned(),
                            mysql_type: 8
                        }],
                "normal native query failed after fault phase closed"
            );
            evidence["healthy_mysql"] = serde_json::to_value(health)?;
            await_idle(context, "root-reply", "after-health", epoch)?;
            zero_roots(context, "after-health", &mut evidence)?;
            check_fault(&control.observation(), &freeze, self.0)?;
            context.recheck_live_process_launch_identities()?;
            Ok(())
        })();
        // Failure paths release the held slot and actually join the client;
        // dropping a runtime or issuing KILL QUERY is not a successful refusal.
        control.disarm();
        let cleanup_join = if let Some(job) = job.take() {
            match runtime.block_on(job) {
                Ok(observed) => {
                    evidence["cleanup_negative_client"] = serde_json::to_value(&observed)?;
                    if observed.mysql.is_some() {
                        Ok(())
                    } else {
                        Err(anyhow::anyhow!(
                            "connection failed before negative observer; actual cause retained in client evidence"
                        ))
                    }
                }
                Err(error) => {
                    Err(anyhow::Error::new(error).context("negative client cleanup join failed"))
                }
            }
        } else {
            Ok(())
        };
        let cleanup_owners = if outcome.is_err() {
            await_idle(context, "root-reply", "failure-cleanup", epoch)
                .and_then(|_| zero_roots(context, "failure-cleanup", &mut evidence))
        } else {
            Ok(())
        };
        evidence["failure_summaries"] = json!({
            "primary": failure_summary("scene-contract", outcome.as_ref().err()),
            "client_join": failure_summary("client-join", cleanup_join.as_ref().err()),
            "owner_cleanup": failure_summary("owner-cleanup", cleanup_owners.as_ref().err()),
        });
        evidence["actor_before_shutdown"] = serde_json::to_value(control.observation())?;
        if outcome.is_err()
            || cleanup_join.is_err()
            || cleanup_owners.is_err()
            || !control.observation().failures.is_empty()
        {
            context.retain_artifacts();
        }
        let shutdown = context.shutdown();
        let post = control.observation();
        let joined = check_shutdown(&post);
        evidence["actor_after_shutdown"] = serde_json::to_value(post)?;
        evidence["outcome"] = json!({"contract_and_recovery_ok":outcome.is_ok(),"client_join_ok":cleanup_join.is_ok(),
            "cluster_shutdown_ok":shutdown.is_ok(),"actor_join_ok":joined.is_ok(),"failure_owner_cleanup_ok":cleanup_owners.is_ok()});
        std::fs::write(
            context
                .scenario_root()
                .join("native-root-reply-observations.json"),
            serde_json::to_vec_pretty(&evidence)?,
        )?;
        outcome?;
        cleanup_owners?;
        cleanup_join?;
        shutdown?;
        joined?;
        context.action("verified one exact native RootReply semantic refusal, no target ACK, owner convergence and normal native recovery");
        Ok(())
    }
}

fn positive_query(
    context: &mut ScenarioContext,
    runtime: &tokio::runtime::Runtime,
    sql: &str,
) -> Result<TextResultObservation> {
    let before = (0..3)
        .map(|i| context.handle().backend_task_execution_tasks_created(i))
        .collect::<Result<Vec<_>>>()?;
    let timeout = context.remaining("normal native query")?.min(CLIENT);
    let observed = runtime.block_on(async {
        tokio::time::timeout(timeout, async {
            let mut stream =
                AsyncMysqlStream::connect(context.mysql_user(), context.mysql_port(), timeout)
                    .await?;
            Ok::<_, anyhow::Error>(stream.observe_text_query(sql, Duration::ZERO).await)
        })
        .await
        .context("absolute positive client phase deadline exceeded")?
    })?;
    let after = (0..3)
        .map(|i| context.handle().backend_task_execution_tasks_created(i))
        .collect::<Result<Vec<_>>>()?;
    ensure!(
        before
            .iter()
            .zip(&after)
            .all(|(a, b)| a.is_finite() && b.is_finite() && *a >= 0.0 && b >= a)
            && before.iter().zip(&after).any(|(a, b)| b > a),
        "normal query never created a native task"
    );
    Ok(observed)
}

fn check_mysql(
    observed: &BoundedNegativeTextObservation,
    freeze: &Freeze,
    metadata: &str,
) -> Result<()> {
    let err = observed
        .terminal
        .as_ref()
        .context("no complete protocol-41 contract ERR")?;
    ensure!(
        observed.text.error.is_none()
            && observed.completed_metadata
            && observed.metadata_payload_hex.len() == 3
            && observed.text.columns == 1
            && observed.text.packets == 4
            && observed.text.rows == 0
            && observed.text.row_payload_bytes == 0
            && observed.text.first_row_micros.is_none()
            && observed.text.first_payload_chunk_micros.is_none()
            && observed.text.metadata_sha256 == metadata
            && err.sequence == 4
            && err.code == freeze.mysql_error_code
            && err.sqlstate == freeze.mysql_sqlstate
            && err.message.contains(&freeze.contract_reason)
            && err.payload_hex.starts_with("ff5104234859303030"),
        "real FE did not report the exact semantic contract refusal with complete metadata and zero rows"
    );
    Ok(())
}
fn check_fault(obs: &RootReplyFaultObservation, freeze: &Freeze, case: Case) -> Result<()> {
    ensure!(
        obs.claimed == 1
            && obs.emitted_messages == 1
            && obs.target_consumed_positive_requests == 0
            && (1..=32).contains(&obs.target_attempts)
            && obs.not_ready_observed == obs.not_ready_forwarded
            && obs.failures.is_empty()
            && !obs.failure_overflow
            && obs.first_failure.is_none()
            && obs.overflow_failures == 0,
        "native fault incomplete, overflowed, failed, or target result acknowledged"
    );
    let old = obs
        .original
        .as_ref()
        .context("no original real reply evidence")?;
    let new = obs
        .mutated
        .as_ref()
        .context("no emitted mutation evidence")?;
    ensure!(
        old["profile"] == 1
            && old["kind"] == "ClientRows"
            && old["sequence"] == 1
            && old["accepted_consumed"] == 0
            && old["body_bytes"] == 69
            && old["body_sha256"] == freeze.expected_native_body_sha256
            && new["mutation"] == format!("{:?}", case.mutation())
            && new["task"] == old["task"]
            && new["sequence"] == old["sequence"]
            && new["accepted_consumed"] == old["accepted_consumed"]
            && new["end_after_data"] == old["end_after_data"],
        "native mutation lost frozen immutable reply facts"
    );
    if !old["end_after_data"].is_null() {
        ensure!(
            old["end_after_data"]["sequence"] == 2 && old["end_after_data"]["output_rows"] == 1,
            "real optional Data1 piggyback End differs from scalar result contract"
        );
    }
    match case {
        Case::Profile => ensure!(
            new["profile"] == 2
                && new["output_kind"]
                    .as_str()
                    .is_some_and(|s| s.contains("ClientRows(true)"))
                && new["body_sha256"] == old["body_sha256"],
            "profile mutation differs"
        ),
        Case::Kind => ensure!(
            new["profile"] == 1
                && new["output_kind"]
                    .as_str()
                    .is_some_and(|s| s.contains("ClientRows(false)"))
                && new["body_sha256"] == old["body_sha256"],
            "kind mutation differs"
        ),
        Case::Prefix => ensure!(
            new["profile"] == 1
                && new["output_kind"]
                    .as_str()
                    .is_some_and(|s| s.contains("ClientRows(true)"))
                && new["body_bytes"] == 4
                && new["body_hex"] == "01000000",
            "prefix-only mutation differs"
        ),
    }
    Ok(())
}
fn check_shutdown(obs: &RootReplyFaultObservation) -> Result<()> {
    ensure!(
        obs.shutdown_joined
            && obs.active_listeners == 0
            && obs.joined_listeners == 3
            && obs.active_connections == 0
            && obs.active_streams == 0
            && obs.connection_positions == 0
            && obs.stream_positions == 0
            && obs.target_slots == 0
            && obs.owned_buffer_bytes == 0
            && obs.failures.is_empty()
            && !obs.failure_overflow
            && obs.first_failure.is_none()
            && obs.target_consumed_positive_requests == 0
            && obs.claimed == 1
            && obs.emitted_messages == 1
            && obs.joined_children > 0
            && obs.joined_connections > 0,
        "actor descendants or owned-buffer aliases survived shutdown, or wholefailure occurred"
    );
    Ok(())
}

fn logs(context: &ScenarioContext) -> Result<Vec<String>> {
    // CrossProcessServerHandle current log layout, with live launch identities
    // checked before/after capture; no BE restart or log-history concatenation.
    (0..3)
        .map(|i| {
            let mut bytes = Vec::with_capacity(LOG_CAP + 1);
            std::fs::File::open(context.runtime_dir().join(format!("be_{i}.log")))?
                .take((LOG_CAP + 1) as u64)
                .read_to_end(&mut bytes)?;
            ensure!(
                bytes.len() <= LOG_CAP,
                "BE task log exceeds 2 MiB observation cap"
            );
            Ok(String::from_utf8(bytes)?)
        })
        .collect()
}
fn appended_markers(before: &[String], after: &[String]) -> Result<Vec<Vec<String>>> {
    ensure!(
        before.len() == 3 && after.len() == 3,
        "task log inventory is not three BEs"
    );
    before
        .iter()
        .zip(after)
        .map(|(before, after)| {
            ensure!(
                after.starts_with(before),
                "task log was replaced or truncated"
            );
            let delta = &after[before.len()..];
            ensure!(
                delta.is_empty() || delta.ends_with('\n'),
                "partial appended task observation"
            );
            let mut markers = Vec::with_capacity(2);
            for line in delta
                .lines()
                .filter(|l| l.starts_with(CREATE) || l.starts_with(ESTABLISH))
            {
                ensure!(
                    markers.len() < 2 && line.len() <= 384,
                    "task marker inventory overflow"
                );
                markers.push(line.to_owned());
            }
            Ok(markers)
        })
        .collect()
}
fn independent_target(
    before: &[String],
    after: &[String],
    occupied: usize,
    process: BackendProcessId,
) -> Result<(TaskIdentity, FrontendProcessId)> {
    let markers = appended_markers(before, after)?;
    let created: Vec<_> = markers
        .iter()
        .enumerate()
        .flat_map(|(be, lines)| {
            lines
                .iter()
                .filter(|s| s.starts_with(CREATE))
                .map(move |s| (be, s))
        })
        .collect();
    ensure!(
        created.len() == 1 && created[0].0 == occupied,
        "whole three-BE set is not exactly one fresh task on occupied root backend"
    );
    let candidates = parse_created_task_candidates(before, after, occupied)?;
    ensure!(
        candidates.len() == 1,
        "unique native task identity was not decoded"
    );
    let task = decode_task_identity(&candidates[0], FieldPath::root("independent_root_task"))?;
    ensure!(
        task.backend_process_id() == process,
        "sole task backend UUID differs from actual process/descriptor"
    );
    let contexts: Vec<_> = markers
        .iter()
        .enumerate()
        .flat_map(|(be, lines)| {
            lines
                .iter()
                .filter(|s| s.starts_with(ESTABLISH))
                .map(move |s| (be, s))
        })
        .collect();
    ensure!(
        contexts.len() == 1 && contexts[0].0 == occupied,
        "unique task has no unique independent fresh context establish"
    );
    let fields: Vec<_> = contexts[0].1.split(' ').collect();
    let execution = task.query_execution_id();
    let expected = format!(
        "execution_id={}:{}:{}",
        execution.query_id().high(),
        execution.query_id().low(),
        execution.attempt_id().get()
    );
    ensure!(
        fields.len() == 4 && fields[0] == ESTABLISH && fields[1] == expected,
        "context establish execution differs from sole task"
    );
    let frontend_text = fields[2]
        .strip_prefix("frontend=")
        .context("missing canonical frontend identity")?;
    let frontend: FrontendProcessId = frontend_text.parse()?;
    ensure!(
        frontend.to_string() == frontend_text && fields[3] == format!("backend={process}"),
        "context establish has a noncanonical frontend or different backend UUID"
    );
    Ok((task, frontend))
}
fn census(
    context: &mut ScenarioContext,
    deadline: Instant,
) -> Result<Vec<Option<BTreeMap<String, u64>>>> {
    let ports: Vec<_> = context
        .handle()
        .runtime()
        .be
        .iter()
        .map(|be| be.http)
        .collect();
    ports
        .into_iter()
        .map(|port| {
            let remaining = deadline.saturating_duration_since(Instant::now());
            ensure!(
                !remaining.is_zero(),
                "root census absolute deadline elapsed"
            );
            let client = reqwest::blocking::Client::builder()
                .timeout(remaining.min(Duration::from_millis(500)))
                .build()?;
            let mut bytes = Vec::with_capacity(METRIC_CAP + 1);
            client
                .get(format!("http://127.0.0.1:{port}/metrics?type=json"))
                .send()?
                .error_for_status()?
                .take((METRIC_CAP + 1) as u64)
                .read_to_end(&mut bytes)?;
            ensure!(
                bytes.len() <= METRIC_CAP,
                "metric response exceeds 1 MiB cap"
            );
            let rows: Value = serde_json::from_slice(&bytes)?;
            root_census(rows.as_array().context("invalid root metric array")?)
        })
        .collect()
}
fn zero_roots(context: &mut ScenarioContext, phase: &str, evidence: &mut Value) -> Result<()> {
    let deadline = context.deadline().min(Instant::now() + CAPTURE);
    let mut snapshots = Vec::with_capacity(51);
    let mut consecutive = 0;
    loop {
        ensure!(
            Instant::now() < deadline && snapshots.len() < 51,
            "root owners failed bounded zero barrier"
        );
        let roots = census(context, deadline)?;
        let zero = roots
            .iter()
            .all(|r| r.as_ref().is_some_and(|r| r.values().all(|n| *n == 0)));
        consecutive = if zero { consecutive + 1 } else { 0 };
        snapshots.push(json!(roots));
        evidence[format!("root_zero_{phase}")] = json!(snapshots);
        if consecutive == 2 {
            return Ok(());
        }
        std::thread::sleep(
            Duration::from_millis(100).min(deadline.saturating_duration_since(Instant::now())),
        );
    }
}
fn check_effective(context: &ScenarioContext, evidence: &mut Value) -> Result<()> {
    let actual: Value =
        serde_json::from_slice(context.effective_launch_config_evidence().artifact_bytes())?;
    let actor = &actual["semantics"]["native_proxy"]["root_reply_message_fault"];
    ensure!(
        actor["backend_indices"] == json!([0, 1, 2])
            && actor["actor"]["target_slots"] == 1
            && actor["actor"]["not_ready_transparency"] == true
            && actor["actor"]["capture_deadline"] == "single-absolute-never-reset"
            && actor["actor"]["bounds"]["maximum_connections"] == 32
            && actor["actor"]["bounds"]["maximum_active_streams"] == 256
            && actor["actor"]["bounds"]["maximum_owned_buffer_bytes"] == 16 * 1024 * 1024
            && actor["actor"]["bounds"]["handshake_timeout_millis"] == 2000
            && actor["actor"]["maximum_target_attempts"] == 32
            && actor["actor"]["bounds"]["maximum_capture_millis"] == 5000
            && actor["actor"]["bounds"]["forward_timeout_millis"] == 20000,
        "effective launch did not enable exact actor v2 bounds on all three Data listeners"
    );
    let roles = actual["semantics"]["roles"]
        .as_array()
        .context("missing effective role configs")?;
    let fe: Vec<_> = roles.iter().filter(|r| r["role"] == "fe").collect();
    ensure!(fe.len() == 1, "effective FE role config is not unique");
    let config = &fe[0]["effective_config"];
    ensure!(
        config["type"] == "table",
        "effective FE config is not canonical table"
    );
    let runtime = &config["value"]["runtime"];
    ensure!(
        runtime.is_null() || runtime["type"] == "table",
        "effective FE runtime is not canonical table"
    );
    let queue = &runtime["value"]["task_operation_queue_residence_ms"];
    let (millis, source) = if queue.is_null() {
        // Server serde default is derived from this public budget constant.
        (
            u64::try_from(
                TransportBudget::DEFAULT
                    .frontend_queue_residence()
                    .as_millis(),
            )?,
            "server-serde-default",
        )
    } else {
        ensure!(
            queue["type"] == "integer",
            "effective queue residence has wrong type"
        );
        (
            queue["value"]
                .as_u64()
                .context("effective queue residence is not positive millis")?,
            "effective-explicit-config",
        )
    };
    ensure!(
        millis >= 5000,
        "effective FE RPC queue grace cannot cover original five-second capture"
    );
    evidence["effective_launch"] = actual;
    evidence["effective_capture_grace"] =
        json!({"queue_residence_millis":millis,"source":source,"capture_millis":5000});
    Ok(())
}

// Hash arbitrary cause formatting incrementally; store no unbounded full text.
struct FailureDigestWriter {
    hash: Sha256,
    bytes: usize,
    length_overflow: bool,
}
impl std::fmt::Write for FailureDigestWriter {
    fn write_str(&mut self, text: &str) -> std::fmt::Result {
        self.hash.update(text.as_bytes());
        if let Some(length) = self.bytes.checked_add(text.len()) {
            self.bytes = length;
        } else {
            self.length_overflow = true;
            self.bytes = usize::MAX;
        }
        Ok(())
    }
}
fn failure_summary(class: &str, error: Option<&anyhow::Error>) -> Value {
    use std::fmt::Write as _;
    error.map_or(Value::Null, |error| {
        let mut sink = FailureDigestWriter {
            hash: Sha256::new(),
            bytes: 0,
            length_overflow: false,
        };
        let format_failed = write!(&mut sink, "{error:#}").is_err();
        json!({"class":class,"bytes":sink.bytes,"sha256":format!("{:x}",sink.hash.finalize()),
            "length_overflow":sink.length_overflow,"format_failed":format_failed})
    })
}

#[cfg(test)]
mod root_reply_predicate_tests {
    use super::*;
    use crate::actors::mysql_stream::root_reply_test_peer::{Mode, script};

    #[tokio::test]
    async fn scripted_sequence_interrupted_eof_and_ok_never_satisfy_real_contract_err_oracle() {
        let freeze: Freeze = serde_json::from_str(Case::Profile.freeze()).expect("freeze");
        for mode in [Mode::WrongSequence, Mode::Interrupted, Mode::Eof, Mode::Ok] {
            let result = script(mode, Duration::ZERO)
                .await
                .expect("joined scripted peer");
            assert!(
                check_mysql(&result.observed, &freeze, &result.metadata_sha256).is_err(),
                "accepted {mode:?}"
            );
            assert!(
                result.observed.completed_metadata,
                "failure must occur after complete valid metadata"
            );
            if matches!(mode, Mode::Interrupted) {
                assert_eq!(
                    result.observed.text.error, None,
                    "1317 is a full structurally valid ERR, not a parser failure"
                );
                assert_eq!(
                    result
                        .observed
                        .terminal
                        .as_ref()
                        .expect("complete ERR")
                        .code,
                    1317
                );
            }
            if matches!(mode, Mode::WrongSequence) {
                assert!(
                    result
                        .observed
                        .text
                        .error
                        .as_deref()
                        .is_some_and(|e| e.contains("sequence mismatch"))
                );
                let consumed = result.metadata_wire_bytes + 4;
                assert_eq!(result.observed.text.wire_bytes as usize, consumed);
                assert_eq!(
                    result.observed.text.wire_prefix_sha256,
                    format!("{:x}", Sha256::digest(&result.response_wire[..consumed]))
                );
            }
        }
        // Positive control: the same parser/predicate must accept a complete
        // 1105/HY000 contract ERR, so the negative checks are not vacuous.
        let valid = script(Mode::Valid, Duration::ZERO)
            .await
            .expect("joined positive control");
        check_mysql(&valid.observed, &freeze, &valid.metadata_sha256)
            .expect("valid contract ERR oracle");
        assert!(valid.observed.pending_packet.is_none());
        assert_eq!(
            valid.observed.text.wire_bytes as usize,
            valid.response_wire.len()
        );
        assert_eq!(
            valid.observed.text.wire_prefix_sha256,
            format!("{:x}", Sha256::digest(&valid.response_wire))
        );
    }

    // These immutable observations test the runner predicate only. They do
    // not assert that a real FE consumed native bytes or establish identity.
    fn data_fixture(end: Value, freeze: &Freeze) -> RootReplyFaultObservation {
        let original = json!({"task":"test-only-task","profile":1,"kind":"ClientRows","sequence":1,
            "accepted_consumed":0,"body_bytes":69,"body_sha256":freeze.expected_native_body_sha256,"end_after_data":end});
        let mutated = json!({"task":"test-only-task","profile":2,"output_kind":"Some(ClientRows(true))","sequence":1,
            "accepted_consumed":0,"mutation":"ProfileTwo","body_sha256":freeze.expected_native_body_sha256,
            "end_after_data":original["end_after_data"]});
        RootReplyFaultObservation {
            active_listeners: 3,
            joined_listeners: 0,
            active_connections: 1,
            active_streams: 0,
            connection_positions: 1,
            stream_positions: 0,
            target_slots: 0,
            owned_buffer_bytes: 0,
            peak_owned_buffer_bytes: 69,
            claimed: 1,
            emitted_messages: 1,
            frozen_root_task: Some("test-only-task".to_owned()),
            frozen_request: None,
            target_requests: 1,
            target_consumed_positive_requests: 0,
            joined_connections: 0,
            joined_children: 0,
            shutdown_joined: false,
            failures: vec![],
            failure_overflow: false,
            first_failure: None,
            first_overflow: None,
            last_overflow: None,
            overflow_failures: 0,
            target_attempts: 1,
            not_ready_observed: 0,
            not_ready_forwarded: 0,
            non_target_peer_cancels: 0,
            not_ready_replies: vec![],
            target_attempt_requests: vec![],
            original: Some(original),
            mutated: Some(mutated),
        }
    }
    #[test]
    fn data_without_piggyback_end_is_legal_but_some_end_is_exact_and_immutable() {
        let freeze: Freeze = serde_json::from_str(Case::Profile.freeze()).expect("freeze");
        for end in [Value::Null, json!({"sequence":2,"output_rows":1})] {
            check_fault(&data_fixture(end, &freeze), &freeze, Case::Profile)
                .expect("legal real Data1 form");
        }
        for end in [
            json!({"sequence":3,"output_rows":1}),
            json!({"sequence":2,"output_rows":2}),
            json!({"sequence":2,"output_rows":0}),
        ] {
            assert!(
                check_fault(&data_fixture(end, &freeze), &freeze, Case::Profile).is_err(),
                "invalid piggyback End accepted"
            );
        }
        let mut changed = data_fixture(Value::Null, &freeze);
        changed.mutated.as_mut().expect("mutated")["end_after_data"] =
            json!({"sequence":2,"output_rows":1});
        assert!(
            check_fault(&changed, &freeze, Case::Profile).is_err(),
            "mutation fabricated End"
        );
        let mut removed = data_fixture(json!({"sequence":2,"output_rows":1}), &freeze);
        removed.mutated.as_mut().expect("mutated")["end_after_data"] = Value::Null;
        assert!(
            check_fault(&removed, &freeze, Case::Profile).is_err(),
            "mutation removed real End"
        );
    }
}
