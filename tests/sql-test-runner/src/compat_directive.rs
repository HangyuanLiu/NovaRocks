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

use crate::{Mode, RecordFrom};
use crate::cluster::{ClusterMode, ServerHandle};
use crate::types::{QueryMeta, SqlStep};
use anyhow::{Context, Result, bail};
use std::collections::{HashMap, HashSet};
use std::fmt::Write;
use std::time::{Duration, Instant};

#[cfg(not(test))]
const LOG_EVIDENCE_TIMEOUT: Duration = Duration::from_secs(3);
#[cfg(test)]
const LOG_EVIDENCE_TIMEOUT: Duration = Duration::from_millis(200);
const LOG_EVIDENCE_POLL_INTERVAL: Duration = Duration::from_millis(25);

const COMPAT_PROBES: &[&str] = &[
    "malformed-plan",
    "malformed-batch-plan",
    "malformed-chunk",
    "malformed-runtime-filter",
    "malformed-lookup",
    "terminal-fetch",
];

pub(crate) fn has_directives(meta: &QueryMeta) -> bool {
    meta.has_compat_directives()
}

pub(crate) fn validate_mode(meta: &QueryMeta, mode: ClusterMode) -> Result<()> {
    if has_directives(meta) && mode != ClusterMode::StarRocksCompat {
        bail!("compatibility log and probe directives require starrocks-compat mode");
    }
    for probe in &meta.compat_probes {
        if !COMPAT_PROBES.contains(&probe.as_str()) {
            bail!("unknown compatibility probe: {probe}");
        }
    }
    Ok(())
}

pub(crate) fn validate_execution_mode(meta: &QueryMeta, mode: Mode) -> Result<()> {
    if has_directives(meta) && !matches!(mode, Mode::Verify | Mode::Record) {
        bail!("compatibility directives require verify or record mode (got {mode:?})");
    }
    Ok(())
}

pub(crate) fn validate_record_source(
    meta: &QueryMeta,
    mode: Mode,
    record_from: RecordFrom,
) -> Result<()> {
    if has_directives(meta) && mode == Mode::Record && record_from == RecordFrom::Reference {
        bail!("compatibility directives cannot run with record-from=reference");
    }
    Ok(())
}

#[derive(Debug, Default)]
pub(crate) struct BeLogSnapshot {
    counts: HashMap<(usize, String), usize>,
}

pub(crate) fn snapshot(
    meta: &QueryMeta,
    server_handle: &dyn ServerHandle,
) -> Result<BeLogSnapshot> {
    if !has_directives(meta) {
        return Ok(BeLogSnapshot::default());
    }
    let endpoints = server_handle.be_endpoints();
    if endpoints.is_empty() {
        bail!("compatibility directives require at least one real BE endpoint");
    }
    let patterns = meta
        .be_log_contains
        .iter()
        .chain(meta.be_log_count_at_least.iter().map(|(pattern, _)| pattern))
        .chain(
            meta.be_log_be_count_at_least
                .iter()
                .map(|(pattern, _)| pattern),
        )
        .collect::<HashSet<_>>();
    let mut counts = HashMap::new();
    for pattern in patterns {
        for index in 0..endpoints.len() {
            counts.insert(
                (index, pattern.clone()),
                server_handle.be_log_count(index, pattern)?,
            );
        }
    }
    Ok(BeLogSnapshot { counts })
}

fn log_delta(
    snapshot: &BeLogSnapshot,
    server_handle: &dyn ServerHandle,
    index: usize,
    pattern: &str,
) -> Result<usize> {
    let before = snapshot
        .counts
        .get(&(index, pattern.to_string()))
        .copied()
        .unwrap_or(0);
    let after = server_handle.be_log_count(index, pattern)?;
    after.checked_sub(before).ok_or_else(|| {
        anyhow::anyhow!(
            "BE log {index} count for pattern {pattern:?} decreased from {before} to {after}"
        )
    })
}

enum LogEvidenceCheck {
    Satisfied(Vec<String>),
    Pending(String),
}

fn evaluate_log_evidence(
    step: &SqlStep,
    server_handle: &dyn ServerHandle,
    snapshot: &BeLogSnapshot,
    endpoint_count: usize,
) -> Result<LogEvidenceCheck> {
    let mut successes = Vec::new();
    let mut pending = Vec::new();

    for pattern in &step.meta.be_log_contains {
        let mut total = 0usize;
        for index in 0..endpoint_count {
            total = total
                .checked_add(log_delta(snapshot, server_handle, index, pattern)?)
                .context("BE log occurrence count overflow")?;
        }
        if total == 0 {
            pending.push(format!("no BE log contains pattern {pattern:?}"));
        } else {
            successes.push(format!(
                "    @be_log_contains PASS pattern={pattern:?}"
            ));
        }
    }

    for (pattern, required) in &step.meta.be_log_count_at_least {
        let mut total = 0usize;
        for index in 0..endpoint_count {
            total = total
                .checked_add(log_delta(snapshot, server_handle, index, pattern)?)
                .context("BE log occurrence count overflow")?;
        }
        if total < *required {
            pending.push(format!(
                "BE log pattern {pattern:?} occurred {total} times across all BE logs; required at least {required}"
            ));
        } else {
            successes.push(format!(
                "    @be_log_count_at_least PASS pattern={pattern:?} actual={total} required={required}"
            ));
        }
    }

    for (pattern, required) in &step.meta.be_log_be_count_at_least {
        let mut actual = 0usize;
        for index in 0..endpoint_count {
            if log_delta(snapshot, server_handle, index, pattern)? > 0 {
                actual += 1;
            }
        }
        if actual < *required {
            pending.push(format!(
                "BE log pattern {pattern:?} appeared in {actual} distinct BE logs after the step; required at least {required}"
            ));
        } else {
            successes.push(format!(
                "    @be_log_be_count_at_least PASS pattern={pattern:?} actual={actual} required={required}"
            ));
        }
    }

    if pending.is_empty() {
        Ok(LogEvidenceCheck::Satisfied(successes))
    } else {
        Ok(LogEvidenceCheck::Pending(pending.join("; ")))
    }
}

pub(crate) fn run(
    step: &SqlStep,
    server_handle: &dyn ServerHandle,
    snapshot: &BeLogSnapshot,
    log: &mut String,
) -> Result<()> {
    if !has_directives(&step.meta) {
        return Ok(());
    }
    let endpoints = server_handle.be_endpoints();
    if endpoints.is_empty() {
        bail!("compatibility directives require at least one real BE endpoint");
    }

    let started = Instant::now();
    loop {
        match evaluate_log_evidence(step, server_handle, snapshot, endpoints.len())? {
            LogEvidenceCheck::Satisfied(successes) => {
                for success in successes {
                    let _ = writeln!(log, "{success}");
                }
                break;
            }
            LogEvidenceCheck::Pending(reason) => {
                if started.elapsed() >= LOG_EVIDENCE_TIMEOUT {
                    bail!(
                        "compatibility log evidence timed out after {}ms (poll interval {}ms): {reason}",
                        LOG_EVIDENCE_TIMEOUT.as_millis(),
                        LOG_EVIDENCE_POLL_INTERVAL.as_millis()
                    );
                }
                std::thread::sleep(LOG_EVIDENCE_POLL_INTERVAL);
            }
        }
    }

    let endpoint = endpoints
        .first()
        .context("compatibility probe requires a real BE BRPC endpoint")?;
    for probe in &step.meta.compat_probes {
        server_handle.run_compat_probe(probe, endpoint)?;
        let _ = writeln!(log, "    @compat_probe PASS probe={probe}");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{CompatBeEndpoint, QueryMeta, SqlStep};
    use anyhow::{Result, bail};
    use std::sync::{Arc, Mutex};

    struct FakeCompatHandle {
        endpoints: Vec<CompatBeEndpoint>,
        logs: Mutex<Vec<String>>,
        probes: Mutex<Vec<(String, String, u16)>>,
    }

    impl FakeCompatHandle {
        fn new(logs: Vec<&str>) -> Self {
            let endpoints = logs
                .iter()
                .enumerate()
                .map(|(index, _)| CompatBeEndpoint {
                    host: "127.0.0.1".to_string(),
                    heartbeat_port: 19050 + index as u16,
                    be_port: 19060 + index as u16,
                    brpc_port: 18060 + index as u16,
                    http_port: 18040 + index as u16,
                    grpc_port: 18070 + index as u16,
                    starlet_port: 19070 + index as u16,
                })
                .collect();
            Self {
                endpoints,
                logs: Mutex::new(logs.into_iter().map(ToString::to_string).collect()),
                probes: Mutex::new(Vec::new()),
            }
        }

        fn append_log(&self, index: usize, text: &str) {
            self.logs.lock().expect("logs lock")[index].push_str(text);
        }
    }

    impl ServerHandle for FakeCompatHandle {
        fn target_host(&self) -> Option<&str> {
            Some("127.0.0.1")
        }

        fn target_port(&self) -> Option<u16> {
            Some(9030)
        }

        fn be_endpoints(&self) -> &[CompatBeEndpoint] {
            &self.endpoints
        }

        fn be_log_count(&self, index: usize, needle: &str) -> Result<usize> {
            let logs = self.logs.lock().expect("logs lock");
            let log = logs
                .get(index)
                .ok_or_else(|| anyhow::anyhow!("missing fake BE log {index}"))?;
            Ok(log.match_indices(needle).count())
        }

        fn run_compat_probe(&self, probe: &str, endpoint: &CompatBeEndpoint) -> Result<()> {
            if endpoint.brpc_port == 0 {
                bail!("fake BRPC endpoint is invalid");
            }
            self.probes.lock().expect("probe lock").push((
                probe.to_string(),
                endpoint.host.clone(),
                endpoint.brpc_port,
            ));
            Ok(())
        }
    }

    fn step(meta: QueryMeta) -> SqlStep {
        SqlStep {
            query_number: 1,
            sql: "SELECT 1".to_string(),
            meta,
        }
    }

    #[test]
    fn compat_directive_mode_rejects_native_and_all_in_one() {
        let meta = QueryMeta {
            compat_probes: vec!["malformed-runtime-filter".to_string()],
            ..QueryMeta::default()
        };

        for mode in [ClusterMode::CrossProcess, ClusterMode::AllInOne] {
            let error = validate_mode(&meta, mode).expect_err("native modes must reject probes");
            assert!(
                error.to_string().contains("require starrocks-compat mode"),
                "unexpected error for {mode:?}: {error:#}"
            );
        }
        validate_mode(&meta, ClusterMode::StarRocksCompat)
            .expect("starrocks-compat mode must allow probes");
    }

    #[test]
    fn compat_directive_allows_record_but_rejects_diff_mode() {
        let meta = QueryMeta {
            be_log_contains: vec!["compat_ingress".to_string()],
            ..QueryMeta::default()
        };

        validate_execution_mode(&meta, Mode::Record)
            .expect("record mode writes goldens before verify executes compatibility directives");
        let error = validate_execution_mode(&meta, Mode::Diff)
            .expect_err("diff mode must not silently skip compatibility directives");
        assert!(
            error
                .to_string()
                .contains("compatibility directives require verify or record mode"),
            "unexpected error: {error:#}"
        );
        validate_execution_mode(&meta, Mode::Verify)
            .expect("verify mode must execute compatibility directives");
    }

    #[test]
    fn compat_directive_rejects_reference_recording() {
        let meta = QueryMeta {
            be_log_contains: vec!["compat_ingress".to_string()],
            ..QueryMeta::default()
        };

        validate_record_source(&meta, Mode::Record, RecordFrom::Target)
            .expect("target recording can collect compatibility evidence");
        let error = validate_record_source(&meta, Mode::Record, RecordFrom::Reference)
            .expect_err("reference recording cannot collect target BE evidence");

        assert!(error.to_string().contains("record-from=reference"));
    }

    #[test]
    fn compat_directive_inspects_all_be_logs_and_sums_occurrences() {
        let handle = FakeCompatHandle::new(vec!["old compat_ingress\n", "", "unrelated\n"]);
        let step = step(QueryMeta {
            be_log_contains: vec!["compat_ingress".to_string()],
            be_log_count_at_least: vec![("runtime_filter_receive".to_string(), 3)],
            be_log_be_count_at_least: vec![("runtime_filter_receive".to_string(), 2)],
            ..QueryMeta::default()
        });
        let mut log = String::new();
        let before = snapshot(&step.meta, &handle).expect("pre-step snapshot");
        handle.append_log(0, "compat_ingress\nruntime_filter_receive\n");
        handle.append_log(1, "runtime_filter_receive\nruntime_filter_receive\n");

        run(&step, &handle, &before, &mut log)
            .expect("directives should inspect post-step deltas across every BE log");

        assert!(log.contains("@be_log_contains PASS pattern=\"compat_ingress\""));
        assert!(log.contains(
            "@be_log_count_at_least PASS pattern=\"runtime_filter_receive\" actual=3 required=3"
        ));
        assert!(log.contains(
            "@be_log_be_count_at_least PASS pattern=\"runtime_filter_receive\" actual=2 required=2"
        ));
    }

    #[test]
    fn compat_directive_runs_each_probe_once_against_a_real_be_endpoint() {
        let handle = FakeCompatHandle::new(vec!["ready", "ready", "ready"]);
        let step = step(QueryMeta {
            compat_probes: vec![
                "malformed-runtime-filter".to_string(),
                "terminal-fetch".to_string(),
            ],
            ..QueryMeta::default()
        });
        let mut log = String::new();
        let before = snapshot(&step.meta, &handle).expect("pre-step snapshot");

        run(&step, &handle, &before, &mut log).expect("probes should pass");

        assert_eq!(
            *handle.probes.lock().expect("probe lock"),
            vec![
                (
                    "malformed-runtime-filter".to_string(),
                    "127.0.0.1".to_string(),
                    18060,
                ),
                ("terminal-fetch".to_string(), "127.0.0.1".to_string(), 18060,),
            ]
        );
        assert!(log.contains("@compat_probe PASS probe=malformed-runtime-filter"));
        assert!(log.contains("@compat_probe PASS probe=terminal-fetch"));
    }

    #[test]
    fn compat_directive_polls_bounded_post_step_deltas_for_async_evidence() {
        let handle = Arc::new(FakeCompatHandle::new(vec!["old close\n", "", ""]));
        let step = step(QueryMeta {
            be_log_be_count_at_least: vec![(
                "lookup_close direction=receive status=ok".to_string(),
                2,
            )],
            ..QueryMeta::default()
        });
        let before = snapshot(&step.meta, handle.as_ref()).expect("pre-step snapshot");
        let delayed_handle = Arc::clone(&handle);
        let writer = std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(50));
            delayed_handle.append_log(0, "lookup_close direction=receive status=ok\n");
            delayed_handle.append_log(1, "lookup_close direction=receive status=ok\n");
        });
        let mut log = String::new();

        run(&step, handle.as_ref(), &before, &mut log)
            .expect("bounded polling should observe delayed post-step evidence");
        writer.join().expect("delayed log writer");

        assert!(log.contains("actual=2 required=2"), "{log}");
    }

    #[test]
    fn stale_pre_step_log_marker_does_not_satisfy_directive() {
        let handle = FakeCompatHandle::new(vec!["compat_ingress\n", "", ""]);
        let step = step(QueryMeta {
            be_log_contains: vec!["compat_ingress".to_string()],
            ..QueryMeta::default()
        });
        let before = snapshot(&step.meta, &handle).expect("pre-step snapshot");

        let error = run(&step, &handle, &before, &mut String::new())
            .expect_err("stale marker must not satisfy post-step evidence");

        assert!(error.to_string().contains("no BE log contains pattern"));
    }
}
