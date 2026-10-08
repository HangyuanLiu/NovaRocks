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
use novarocks_cluster_harness::process_resources::ProcessResourceMonitor;
use serde::{Deserialize, Serialize};
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;
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
                schema_version: 1,
                workload: &workload,
                samples: &samples,
                failures,
                deterministic_mismatch_queries: mismatch_queries.clone(),
                run_error: run_error.clone(),
                producer_placement: "one root task per query; placement requires separate task evidence",
                cpu_evidence: "result-process-resources.json; samples and process monitor use separate monotonic origins",
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
