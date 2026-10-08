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

use crate::actors::mysql_stream::{AsyncMysqlStream, TextResultObservation};
use crate::scenario::{Scenario, ScenarioContext};
use anyhow::{Context, Result, ensure};
use novarocks_cluster_harness::LaunchProfile;
use novarocks_cluster_harness::process_resources::{
    ProcessResourceMonitor, ProcessResourceSampler,
};
use reqwest::blocking::Client;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Barrier;

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Workload {
    schema_version: u32,
    concurrency: Vec<usize>,
    repetitions: usize,
    query_deadline_ms: u64,
    queries: Vec<Query>,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(deny_unknown_fields)]
struct Query {
    name: String,
    sql: String,
    expected_rows: Option<u64>,
    deterministic_rows: bool,
    slow_read_delay_ms: u64,
}

impl Workload {
    fn load(path: &Path) -> Result<Self> {
        let bytes = std::fs::read(path)?;
        ensure!(
            bytes.len() <= 1024 * 1024,
            "result workload exceeds manifest bound"
        );
        let workload: Self = serde_json::from_slice(&bytes)?;
        ensure!(
            workload.schema_version == 1,
            "unsupported result workload version"
        );
        ensure!(
            (1..=8).contains(&workload.concurrency.len()),
            "invalid concurrency window count"
        );
        ensure!(
            workload.concurrency.iter().all(|n| (1..=512).contains(n)),
            "invalid client count"
        );
        ensure!(
            (1..=10).contains(&workload.repetitions),
            "invalid repetition count"
        );
        ensure!(
            (1..=32).contains(&workload.queries.len()),
            "invalid query count"
        );
        ensure!(
            (100..=120000).contains(&workload.query_deadline_ms),
            "invalid absolute query deadline"
        );
        let mut names = std::collections::BTreeSet::new();
        for query in &workload.queries {
            ensure!(names.insert(&query.name), "duplicate workload query name");
            ensure!(
                !query.name.is_empty() && query.name.len() <= 64,
                "invalid query name"
            );
            ensure!(
                !query.sql.is_empty() && query.sql.len() <= 65536,
                "invalid query body"
            );
            ensure!(query.slow_read_delay_ms <= 10, "invalid slow-reader delay");
        }
        Ok(workload)
    }
}

#[derive(Serialize)]
struct Sample {
    query: String,
    concurrency: usize,
    repetition: usize,
    client: usize,
    started_millis: u128,
    ended_millis: u128,
    started_micros: u128,
    ended_micros: u128,
    connect_micros: u128,
    observation: TextResultObservation,
}

#[derive(Serialize)]
struct Evidence<'a> {
    schema_version: u32,
    workload: &'a Workload,
    samples: &'a [Sample],
    failures: usize,
    deterministic_mismatch_queries: Vec<String>,
    run_error: Option<String>,
    producer_placement: &'static str,
    cpu_evidence: &'static str,
}

pub struct ResultDeliveryBaseline;

impl Scenario for ResultDeliveryBaseline {
    fn name(&self) -> &'static str {
        "performance/mem-1-m07-wire-baseline"
    }

    fn is_explicit_stage(&self) -> bool {
        true
    }

    fn validate_runner_inputs(
        &self,
        profile: LaunchProfile,
        manifest: Option<&Path>,
    ) -> Result<()> {
        ensure!(
            profile == LaunchProfile::Performance,
            "result baseline requires performance launch profile"
        );
        Workload::load(manifest.context("result baseline requires --uea1-workload-manifest")?)?;
        Ok(())
    }

    fn run(&self, context: &mut ScenarioContext) -> Result<()> {
        ensure!(
            context.process_ids().backends.len() == 3,
            "result baseline requires native 1FE+3BE"
        );
        let workload = Workload::load(
            context
                .uea1_workload_manifest()
                .context("missing result workload")?,
        )?;
        let monitor = ProcessResourceMonitor::start_with_identities(
            context.process_resource_identities()?,
            "mem-1-m07-wire-baseline",
            Duration::from_millis(100),
        )?;
        let user = context.mysql_user().to_string();
        let port = context.mysql_port();
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(4)
            .enable_all()
            .build()?;
        let epoch = std::time::Instant::now();
        let mut samples = Vec::new();
        let execution = (|| -> Result<()> {
            for &concurrency in &workload.concurrency {
                for repetition in 0..workload.repetitions {
                    for query in &workload.queries {
                        context.remaining("result measurement window")?;
                        let window_name = format!("{}-{concurrency}-{repetition}", query.name);
                        await_idle(context, &window_name, "before", epoch)?;
                        let mut boundary = ProcessResourceSampler::from_identities(
                            context.process_resource_identities()?,
                            &window_name,
                        )?;
                        boundary.sample_cluster()?;
                        let window = runtime.block_on(async {
                            let barrier = Arc::new(Barrier::new(concurrency));
                            let mut jobs = tokio::task::JoinSet::new();
                            for client in 0..concurrency {
                                let barrier = barrier.clone();
                                let query = query.clone();
                                let user = user.clone();
                                let deadline = Duration::from_millis(workload.query_deadline_ms);
                                jobs.spawn(async move {
                                    let connected = std::time::Instant::now();
                                    let stream = tokio::time::timeout(
                                        deadline,
                                        AsyncMysqlStream::connect(&user, port, deadline),
                                    )
                                    .await
                                    .context("absolute connect deadline exceeded")
                                    .and_then(|result| result);
                                    let connect_micros = connected.elapsed().as_micros();
                                    barrier.wait().await;
                                    let started = epoch.elapsed();
                                    let started_millis = started.as_millis();
                                    let started_micros = started.as_micros();
                                    let observation = match stream {
                                        Ok(mut stream) => {
                                            stream
                                                .observe_text_query(
                                                    &query.sql,
                                                    Duration::from_millis(query.slow_read_delay_ms),
                                                )
                                                .await
                                        }
                                        Err(error) => TextResultObservation {
                                            error: Some(
                                                format!("connect: {error}")
                                                    .chars()
                                                    .take(512)
                                                    .collect(),
                                            ),
                                            ..Default::default()
                                        },
                                    };
                                    let ended = epoch.elapsed();
                                    Sample {
                                        query: query.name,
                                        concurrency,
                                        repetition,
                                        client,
                                        started_millis,
                                        ended_millis: ended.as_millis(),
                                        started_micros,
                                        ended_micros: ended.as_micros(),
                                        connect_micros,
                                        observation,
                                    }
                                });
                            }
                            let mut window = Vec::with_capacity(concurrency);
                            while let Some(sample) = jobs.join_next().await {
                                window.push(sample.context("result observer task failed")?);
                            }
                            Ok::<_, anyhow::Error>(window)
                        })?;
                        samples.extend(window);
                        std::fs::write(
                            context
                                .scenario_root()
                                .join("result-samples-checkpoint.json"),
                            serde_json::to_vec(&samples)?,
                        )?;
                        let convergence = await_idle(context, &window_name, "after", epoch);
                        boundary.sample_cluster()?;
                        boundary.write_json(
                            &context
                                .scenario_root()
                                .join(format!("window-cpu-{window_name}.json")),
                        )?;
                        convergence?;
                    }
                }
            }
            Ok(())
        })();
        let root = context.scenario_root();
        let monitor_result = monitor.finish(&root.join("result-process-resources.json"));
        let run_error = execution
            .err()
            .or_else(|| monitor_result.err())
            .map(|error| error.to_string().chars().take(512).collect());
        let failures = samples
            .iter()
            .filter(|sample| {
                let query = workload
                    .queries
                    .iter()
                    .find(|query| query.name == sample.query)
                    .expect("manifest query");
                sample.observation.error.is_some()
                    || query
                        .expected_rows
                        .is_some_and(|rows| rows != sample.observation.rows)
            })
            .count();
        let mut mismatch_queries = Vec::new();
        for query in workload
            .queries
            .iter()
            .filter(|query| query.deterministic_rows)
        {
            let mut hashes = samples
                .iter()
                .filter(|s| s.query == query.name && s.observation.error.is_none())
                .map(|s| &s.observation.row_sha256);
            if let Some(first) = hashes.next() {
                if hashes.any(|hash| hash != first) {
                    mismatch_queries.push(query.name.clone());
                }
            }
        }
        std::fs::write(
            root.join("result-wire-samples.json"),
            serde_json::to_vec_pretty(&Evidence {
                schema_version: 2,
                workload: &workload,
                samples: &samples,
                failures,
                deterministic_mismatch_queries: mismatch_queries.clone(),
                run_error: run_error.clone(),
                producer_placement: "one root task per query; placement requires separate task evidence",
                cpu_evidence: "window-cpu-*.json: explicit before-connect and after-owner-convergence cumulative CPU boundaries; result-process-resources.json: continuous RSS and diagnostic whole-run CPU only",
            })?,
        )?;
        let mismatch = !mismatch_queries.is_empty();
        context.action(format!(
            "result baseline retained {} samples, {} failures, deterministic mismatch={mismatch}",
            samples.len(),
            failures
        ));
        ensure!(
            failures == 0 && !mismatch && run_error.is_none(),
            "result baseline failed; all observations retained"
        );
        Ok(())
    }
}

pub fn scenarios() -> Vec<Box<dyn Scenario>> {
    vec![Box::new(ResultDeliveryBaseline)]
}

// Measurement barriers observe actual owners. They do not change admission,
// evict replay history, or treat an absent counter as a successful zero.
fn required_count(value: &Value, key: &str) -> Result<u64> {
    value[key]
        .as_u64()
        .with_context(|| format!("missing or invalid owner counter {key}"))
}

fn metric(rows: &[Value], name: &str, labels: &[(&str, &str)]) -> Result<u64> {
    let mut matched = rows.iter().filter(|row| {
        row["tags"]["metric"] == name
            && labels
                .iter()
                .all(|(key, value)| row["tags"][*key] == *value)
    });
    let row = matched
        .next()
        .with_context(|| format!("missing metric {name} {labels:?}"))?;
    ensure!(
        matched.next().is_none(),
        "duplicate metric {name} {labels:?}"
    );
    let value = row["value"].as_f64().context("invalid metric number")?;
    ensure!(
        value.is_finite() && value >= 0.0 && value.fract() == 0.0 && value < u64::MAX as f64,
        "invalid owner metric {name}"
    );
    Ok(value as u64)
}

fn idle_snapshot(context: &mut ScenarioContext, client: &Client) -> Result<(Value, bool)> {
    let response = context
        .handle()
        .frontend_management_get("/v1/frontend/state", Duration::from_secs(1))?;
    ensure!(
        response.status == 200,
        "owner observation returned HTTP {}",
        response.status
    );
    let frontend: Value = serde_json::from_str(&response.body)?;
    let workload = &frontend["workload"];
    let governance = &workload["governance"];
    let mut idle = required_count(&workload["active"], "statement")? == 0
        && required_count(&workload["active"], "background")? == 0;
    for key in [
        "root_responsibilities",
        "admitted_queries",
        "preparation",
        "execution",
        "old_attempts",
        "unknown_creates",
        "obligations",
        "waiting_records",
        "control_ready",
        "control_inflight",
    ] {
        idle &= required_count(governance, key)? == 0;
    }
    let new_windows = governance.get("result_window_positions");
    let old_credits = governance.get("result_credit_held_bytes");
    ensure!(
        new_windows.is_some() != old_credits.is_some(),
        "ambiguous or absent result owner observation"
    );
    let root_class = if let Some(positions) = new_windows {
        let positions = positions.as_array().context("invalid result positions")?;
        ensure!(positions.len() == 4, "invalid result position dimensions");
        for position in positions {
            idle &= position.as_u64().context("invalid result position count")? == 0;
        }
        Some("root_result")
    } else {
        idle &= required_count(governance, "held_bytes")? == 0
            && required_count(governance, "result_credit_held_bytes")? == 0;
        None
    };
    let ports: Vec<_> = context
        .handle()
        .runtime()
        .be
        .iter()
        .map(|be| be.http)
        .collect();
    let mut backends = Vec::new();
    for (index, port) in ports.into_iter().enumerate() {
        let rows: Value = client
            .get(format!("http://127.0.0.1:{port}/metrics?type=json"))
            .send()?
            .error_for_status()?
            .json()?;
        let rows = rows.as_array().context("invalid backend metric array")?;
        let reserved = metric(
            rows,
            "novarocks_backend_worker_context_reservations",
            &[("dimension", "used")],
        )?;
        let published = metric(
            rows,
            "novarocks_backend_worker_reservation_last_published_unixtime_seconds",
            &[],
        )?;
        ensure!(
            published > 0,
            "Worker reservation observation has no publication"
        );
        idle &= reserved == 0;
        let mut ingress = Vec::new();
        for class in [Some("ordinary"), Some("control"), root_class]
            .into_iter()
            .flatten()
        {
            for phase in ["running", "waiting"] {
                let used = metric(
                    rows,
                    "novarocks_backend_native_ingress_slots",
                    &[("class", class), ("phase", phase), ("dimension", "used")],
                )?;
                idle &= used == 0;
                ingress.push(serde_json::json!({"class":class,"phase":phase,"used":used}));
            }
        }
        backends.push(
            serde_json::json!({"index":index,"http_port":port,"worker_reservations":reserved,
            "reservation_last_published_unix_seconds":published,"ingress":ingress}),
        );
    }
    Ok((
        serde_json::json!({"frontend_active":workload["active"],"frontend_governance":governance,
        "backends":backends,"idle":idle}),
        idle,
    ))
}

fn await_idle(
    context: &mut ScenarioContext,
    window: &str,
    phase: &str,
    epoch: Instant,
) -> Result<()> {
    let deadline = context
        .deadline()
        .min(Instant::now() + Duration::from_secs(30));
    let client = Client::builder().timeout(Duration::from_secs(1)).build()?;
    let mut snapshots = Vec::new();
    let outcome = (|| -> Result<()> {
        let mut consecutive = 0;
        loop {
            context.remaining("result owner convergence barrier")?;
            let (mut snapshot, idle) = idle_snapshot(context, &client)?;
            snapshot["query_epoch_micros"] = serde_json::json!(epoch.elapsed().as_micros());
            snapshots.push(snapshot);
            consecutive = if idle { consecutive + 1 } else { 0 };
            if consecutive == 2 {
                return Ok(());
            }
            ensure!(
                Instant::now() < deadline && snapshots.len() < 301,
                "result owners did not converge before {window} {phase} barrier deadline"
            );
            std::thread::sleep(Duration::from_millis(100));
        }
    })();
    std::fs::write(
        context
            .scenario_root()
            .join(format!("owner-barrier-{window}-{phase}.json")),
        serde_json::to_vec_pretty(
            &serde_json::json!({"schema_version":1,"window":window,"phase":phase,
            "snapshots":snapshots,"error":outcome.as_ref().err().map(|e| format!("{e:#}"))}),
        )?,
    )?;
    outcome
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn owner_observation_never_turns_missing_duplicate_or_invalid_into_idle() {
        assert!(required_count(&json!({}), "held_bytes").is_err());
        assert!(required_count(&json!({"held_bytes": -1}), "held_bytes").is_err());
        let row = json!({"tags":{"metric":"reservation","dimension":"used"},"value":0});
        assert_eq!(
            metric(
                std::slice::from_ref(&row),
                "reservation",
                &[("dimension", "used")]
            )
            .unwrap(),
            0
        );
        assert!(metric(&[], "reservation", &[("dimension", "used")]).is_err());
        assert!(metric(&[row.clone(), row], "reservation", &[("dimension", "used")]).is_err());
        for value in [json!(-1), json!(0.5), json!("0"), Value::Null] {
            assert!(
                metric(
                    &[json!({"tags":{"metric":"reservation"},"value":value})],
                    "reservation",
                    &[]
                )
                .is_err()
            );
        }
    }
}
