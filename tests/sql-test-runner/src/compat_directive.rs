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

use crate::cluster::{ClusterMode, ServerHandle};
use crate::types::{QueryMeta, SqlStep};
use anyhow::{Context, Result, bail};
use std::fmt::Write;

const COMPAT_PROBES: &[&str] = &[
    "malformed-plan",
    "malformed-batch-plan",
    "malformed-chunk",
    "malformed-runtime-filter",
    "malformed-lookup",
    "terminal-fetch",
];

pub(crate) fn has_directives(meta: &QueryMeta) -> bool {
    !meta.be_log_contains.is_empty()
        || !meta.be_log_count_at_least.is_empty()
        || !meta.compat_probes.is_empty()
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

pub(crate) fn run(step: &SqlStep, server_handle: &dyn ServerHandle, log: &mut String) -> Result<()> {
    if !has_directives(&step.meta) {
        return Ok(());
    }
    let endpoints = server_handle.be_endpoints();
    if endpoints.is_empty() {
        bail!("compatibility directives require at least one real BE endpoint");
    }

    for pattern in &step.meta.be_log_contains {
        let mut total = 0usize;
        for index in 0..endpoints.len() {
            total = total
                .checked_add(server_handle.be_log_count(index, pattern)?)
                .context("BE log occurrence count overflow")?;
        }
        if total == 0 {
            bail!("no BE log contains pattern {pattern:?}");
        }
        let _ = writeln!(log, "    @be_log_contains PASS pattern={pattern:?}");
    }

    for (pattern, required) in &step.meta.be_log_count_at_least {
        let mut total = 0usize;
        for index in 0..endpoints.len() {
            total = total
                .checked_add(server_handle.be_log_count(index, pattern)?)
                .context("BE log occurrence count overflow")?;
        }
        if total < *required {
            bail!(
                "BE log pattern {pattern:?} occurred {total} times across all BE logs; required at least {required}"
            );
        }
        let _ = writeln!(
            log,
            "    @be_log_count_at_least PASS pattern={pattern:?} actual={total} required={required}"
        );
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
    use std::sync::Mutex;

    struct FakeCompatHandle {
        endpoints: Vec<CompatBeEndpoint>,
        logs: Vec<String>,
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
                logs: logs.into_iter().map(ToString::to_string).collect(),
                probes: Mutex::new(Vec::new()),
            }
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
            let log = self
                .logs
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
    fn compat_directive_inspects_all_be_logs_and_sums_occurrences() {
        let handle = FakeCompatHandle::new(vec![
            "compat_ingress\nruntime_filter_receive\n",
            "runtime_filter_receive\nruntime_filter_receive\n",
            "unrelated\n",
        ]);
        let step = step(QueryMeta {
            be_log_contains: vec!["compat_ingress".to_string()],
            be_log_count_at_least: vec![("runtime_filter_receive".to_string(), 3)],
            ..QueryMeta::default()
        });
        let mut log = String::new();

        run(&step, &handle, &mut log).expect("directives should inspect every BE log");

        assert!(log.contains("@be_log_contains PASS pattern=\"compat_ingress\""));
        assert!(log.contains(
            "@be_log_count_at_least PASS pattern=\"runtime_filter_receive\" actual=3 required=3"
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

        run(&step, &handle, &mut log).expect("probes should pass");

        assert_eq!(
            *handle.probes.lock().expect("probe lock"),
            vec![
                (
                    "malformed-runtime-filter".to_string(),
                    "127.0.0.1".to_string(),
                    18060,
                ),
                (
                    "terminal-fetch".to_string(),
                    "127.0.0.1".to_string(),
                    18060,
                ),
            ]
        );
        assert!(log.contains("@compat_probe PASS probe=malformed-runtime-filter"));
        assert!(log.contains("@compat_probe PASS probe=terminal-fetch"));
    }
}
