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
use crate::{Mode, RecordFrom};
use anyhow::{Context, Result, bail};
use std::collections::{BTreeMap, HashMap, HashSet};
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
    "stream-load",
    "transaction-load",
];

pub(crate) fn has_directives(meta: &QueryMeta) -> bool {
    meta.has_compat_directives()
}

pub(crate) fn validate_mode(meta: &QueryMeta, mode: ClusterMode) -> Result<()> {
    if meta.has_be_log_directives() && mode == ClusterMode::AllInOne {
        bail!("BE log evidence directives require a runner-owned cross-process cluster");
    }
    if !meta.compat_probes.is_empty() && mode != ClusterMode::StarRocksCompat {
        bail!("compatibility probes require starrocks-compat mode");
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
    fragment_failure_token: Option<String>,
}

pub(crate) fn snapshot(
    meta: &QueryMeta,
    server_handle: &dyn ServerHandle,
) -> Result<BeLogSnapshot> {
    if !has_directives(meta) {
        return Ok(BeLogSnapshot::default());
    }
    let be_count = server_handle.be_count();
    if be_count == 0 {
        bail!("BE log evidence directives require at least one runner-owned BE");
    }
    let patterns = meta
        .be_log_contains
        .iter()
        .chain(
            meta.be_log_count_at_least
                .iter()
                .map(|(pattern, _)| pattern),
        )
        .chain(
            meta.be_log_be_count_at_least
                .iter()
                .map(|(pattern, _)| pattern),
        )
        .collect::<HashSet<_>>();
    let mut counts = HashMap::new();
    for pattern in patterns {
        for index in 0..be_count {
            counts.insert(
                (index, pattern.clone()),
                server_handle.be_log_count(index, pattern)?,
            );
        }
    }
    let fragment_failure_token = if meta.be_log_exact_fragment_cancellation.is_some() {
        let index = meta.fail_fragment_after_start_be_index.context(
            "@be_log_exact_fragment_cancellation requires @fail_fragment_after_start_be_index",
        )?;
        Some(
            server_handle
                .armed_fragment_failure_token(index)?
                .with_context(|| {
                    format!(
                        "BE[{index}] has no armed fragment failure token for exact cancellation evidence"
                    )
                })?,
        )
    } else {
        None
    };
    Ok(BeLogSnapshot {
        counts,
        fragment_failure_token,
    })
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct QueryIdentity {
    hi: i64,
    lo: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct FragmentIdentity {
    query: QueryIdentity,
    finst_hi: i64,
    finst_lo: i64,
}

type FragmentMultiset = BTreeMap<FragmentIdentity, usize>;

fn marker_payload<'a>(line: &'a str, marker: &str) -> Option<&'a str> {
    line.find(marker)
        .map(|position| &line[position + marker.len()..])
}

fn marker_fields<'a>(payload: &'a str, marker: &str) -> Result<HashMap<&'a str, &'a str>> {
    let mut fields = HashMap::new();
    for field in payload.split_whitespace() {
        let (key, value) = field
            .split_once('=')
            .with_context(|| format!("malformed {marker} field {field:?}"))?;
        if key.is_empty() || value.is_empty() {
            bail!("malformed {marker} field {field:?}");
        }
        if fields.insert(key, value).is_some() {
            bail!("duplicate {marker} field {key:?}");
        }
    }
    Ok(fields)
}

fn parse_i64_field(fields: &HashMap<&str, &str>, marker: &str, field: &str) -> Result<i64> {
    fields
        .get(field)
        .with_context(|| format!("{marker} is missing {field}"))?
        .parse::<i64>()
        .with_context(|| format!("{marker} has invalid {field}"))
}

fn parse_fragment_identity(fields: &HashMap<&str, &str>, marker: &str) -> Result<FragmentIdentity> {
    Ok(FragmentIdentity {
        query: QueryIdentity {
            hi: parse_i64_field(fields, marker, "query_hi")?,
            lo: parse_i64_field(fields, marker, "query_lo")?,
        },
        finst_hi: parse_i64_field(fields, marker, "finst_hi")?,
        finst_lo: parse_i64_field(fields, marker, "finst_lo")?,
    })
}

fn parse_identity_markers(log: &str, marker: &str) -> Result<Vec<FragmentIdentity>> {
    log.lines()
        .filter_map(|line| marker_payload(line, marker))
        .map(|payload| {
            let fields = marker_fields(payload, marker)?;
            parse_fragment_identity(&fields, marker)
        })
        .collect()
}

fn parse_failure_markers(log: &str) -> Result<Vec<(String, FragmentIdentity)>> {
    const MARKER: &str = "NOVAROCKS_FRAGMENT_EXECUTOR_FAILURE_INJECTED";
    log.lines()
        .filter_map(|line| marker_payload(line, MARKER))
        .map(|payload| {
            let fields = marker_fields(payload, MARKER)?;
            let token = fields
                .get("token")
                .with_context(|| format!("{MARKER} is missing token"))?;
            Ok((
                (*token).to_string(),
                parse_fragment_identity(&fields, MARKER)?,
            ))
        })
        .collect()
}

fn identity_multiset(
    identities: impl IntoIterator<Item = FragmentIdentity>,
    query: QueryIdentity,
) -> FragmentMultiset {
    let mut result = FragmentMultiset::new();
    for identity in identities {
        if identity.query == query {
            *result.entry(identity).or_insert(0) += 1;
        }
    }
    result
}

fn exact_fragment_cancellation_evidence(
    server_handle: &dyn ServerHandle,
    snapshot: &BeLogSnapshot,
    endpoint_count: usize,
    required_be_count: usize,
) -> Result<LogEvidenceCheck> {
    if endpoint_count != required_be_count {
        bail!(
            "@be_log_exact_fragment_cancellation requires exactly {required_be_count} runner-owned BEs; found {endpoint_count}"
        );
    }
    let token = snapshot
        .fragment_failure_token
        .as_deref()
        .context("@be_log_exact_fragment_cancellation snapshot has no fragment failure token")?;
    let logs = (0..endpoint_count)
        .map(|index| server_handle.be_log_contents(index))
        .collect::<Result<Vec<_>>>()?;

    let failure_markers = logs
        .iter()
        .map(|log| parse_failure_markers(log))
        .collect::<Result<Vec<_>>>();
    let failure_markers = match failure_markers {
        Ok(markers) => markers,
        Err(error) => {
            return Ok(LogEvidenceCheck::Pending(format!(
                "malformed fragment failure marker: {error:#}"
            )));
        }
    };
    let anchors = failure_markers
        .into_iter()
        .enumerate()
        .flat_map(|(be_index, markers)| {
            markers
                .into_iter()
                .map(move |marker| (be_index, marker))
        })
        .filter_map(|(be_index, (marker_token, identity))| {
            (marker_token == token).then_some((be_index, identity))
        })
        .collect::<Vec<_>>();
    let (anchor_be_index, anchor) = match anchors.as_slice() {
        [] => {
            return Ok(LogEvidenceCheck::Pending(format!(
                "no fragment failure marker has current step token {token:?}"
            )));
        }
        [(be_index, identity)] => (*be_index, *identity),
        _ => {
            bail!(
                "fragment failure token {token:?} anchored {} failure markers; expected exactly one",
                anchors.len()
            );
        }
    };

    let acknowledgements = logs
        .iter()
        .map(|log| parse_identity_markers(log, "NOVAROCKS_FAILED_FRAGMENT_REPORT_ACK"))
        .collect::<Result<Vec<_>>>();
    let acknowledgements = match acknowledgements {
        Ok(markers) => markers,
        Err(error) => {
            return Ok(LogEvidenceCheck::Pending(format!(
                "malformed failed-report ACK marker: {error:#}"
            )));
        }
    };
    let acknowledgements_total = acknowledgements
        .iter()
        .flatten()
        .filter(|identity| **identity == anchor)
        .count();
    match acknowledgements_total {
        0 => {
            return Ok(LogEvidenceCheck::Pending(format!(
                "no explicit frontend ACK matches injected fragment {anchor:?}"
            )));
        }
        1 => {}
        count => {
            bail!(
                "injected fragment {anchor:?} has {count} explicit frontend ACK markers; expected exactly one"
            );
        }
    }
    let acknowledgements_on_failure_be = acknowledgements[anchor_be_index]
        .iter()
        .filter(|identity| **identity == anchor)
        .count();
    if acknowledgements_on_failure_be != 1 {
        bail!(
            "injected fragment {anchor:?} ACK is not on failure BE[{anchor_be_index}]"
        );
    }

    let mut total = 0usize;
    let mut mismatches = Vec::new();
    for (index, log) in logs.iter().enumerate() {
        let accepted = match parse_identity_markers(log, "NOVAROCKS_GRPC_SUBMIT_ACCEPTED") {
            Ok(identities) => identity_multiset(identities, anchor.query),
            Err(error) => {
                return Ok(LogEvidenceCheck::Pending(format!(
                    "BE[{index}] has malformed accepted-fragment marker: {error:#}"
                )));
            }
        };
        let cancelled = match parse_identity_markers(log, "NOVAROCKS_CANCEL_FINST") {
            Ok(identities) => identity_multiset(identities, anchor.query),
            Err(error) => {
                return Ok(LogEvidenceCheck::Pending(format!(
                    "BE[{index}] has malformed cancelled-fragment marker: {error:#}"
                )));
            }
        };
        total = total
            .checked_add(accepted.len())
            .context("accepted fragment identity count overflow")?;

        let accepted_duplicates = accepted
            .iter()
            .filter(|(_, count)| **count != 1)
            .collect::<Vec<_>>();
        let cancelled_duplicates = cancelled
            .iter()
            .filter(|(_, count)| **count != 1)
            .collect::<Vec<_>>();
        if accepted.is_empty() {
            mismatches.push(format!(
                "BE[{index}] accepted no fragment for the injected query"
            ));
        } else if accepted != cancelled {
            mismatches.push(format!(
                "BE[{index}] identity mismatch accepted={accepted:?} cancelled={cancelled:?}"
            ));
        } else if !accepted_duplicates.is_empty() {
            mismatches.push(format!(
                "BE[{index}] accepted duplicate fragment identities: {accepted_duplicates:?}"
            ));
        } else if !cancelled_duplicates.is_empty() {
            mismatches.push(format!(
                "BE[{index}] cancelled duplicate fragment identities: {cancelled_duplicates:?}"
            ));
        }
        if index == anchor_be_index && accepted.get(&anchor) != Some(&1) {
            mismatches.push(format!(
                "injected fragment {anchor:?} was not accepted exactly once on failure BE[{index}]"
            ));
        }
        if index == anchor_be_index && cancelled.get(&anchor) != Some(&1) {
            mismatches.push(format!(
                "injected fragment {anchor:?} was not cancelled exactly once on failure BE[{index}]"
            ));
        }
    }
    if !mismatches.is_empty() {
        return Ok(LogEvidenceCheck::Pending(mismatches.join("; ")));
    }

    Ok(LogEvidenceCheck::Satisfied(vec![format!(
        "    @be_log_exact_fragment_cancellation PASS query_hi={} query_lo={} be_count={} total={total}",
        anchor.query.hi, anchor.query.lo, endpoint_count
    )]))
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
            successes.push(format!("    @be_log_contains PASS pattern={pattern:?}"));
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

    if let Some(required_be_count) = step.meta.be_log_exact_fragment_cancellation {
        match exact_fragment_cancellation_evidence(
            server_handle,
            snapshot,
            endpoint_count,
            required_be_count,
        )? {
            LogEvidenceCheck::Satisfied(exact_successes) => successes.extend(exact_successes),
            LogEvidenceCheck::Pending(reason) => pending.push(reason),
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
    if step.meta.has_be_log_directives() {
        let be_count = server_handle.be_count();
        if be_count == 0 {
            bail!("BE log evidence directives require at least one runner-owned BE");
        }
        let started = Instant::now();
        loop {
            match evaluate_log_evidence(step, server_handle, snapshot, be_count)? {
                LogEvidenceCheck::Satisfied(successes) => {
                    for success in successes {
                        let _ = writeln!(log, "{success}");
                    }
                    break;
                }
                LogEvidenceCheck::Pending(reason) => {
                    if started.elapsed() >= LOG_EVIDENCE_TIMEOUT {
                        bail!(
                            "BE log evidence timed out after {}ms (poll interval {}ms): {reason}",
                            LOG_EVIDENCE_TIMEOUT.as_millis(),
                            LOG_EVIDENCE_POLL_INTERVAL.as_millis()
                        );
                    }
                    std::thread::sleep(LOG_EVIDENCE_POLL_INTERVAL);
                }
            }
        }
    }

    if !step.meta.compat_probes.is_empty() {
        let endpoint = server_handle
            .be_endpoints()
            .first()
            .context("compatibility probe requires a real BE BRPC endpoint")?;
        for probe in &step.meta.compat_probes {
            server_handle.run_compat_probe(probe, endpoint)?;
            let _ = writeln!(log, "    @compat_probe PASS probe={probe}");
        }
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
        fragment_failure_token: Option<String>,
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
                fragment_failure_token: None,
            }
        }

        fn with_fragment_failure_token(mut self, token: &str) -> Self {
            self.fragment_failure_token = Some(token.to_string());
            self
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

        fn be_log_contents(&self, index: usize) -> Result<String> {
            self.logs
                .lock()
                .expect("logs lock")
                .get(index)
                .cloned()
                .ok_or_else(|| anyhow::anyhow!("missing fake BE log {index}"))
        }

        fn armed_fragment_failure_token(&self, index: usize) -> Result<Option<String>> {
            if index >= self.endpoints.len() {
                bail!("missing fake BE {index}");
            }
            Ok(self.fragment_failure_token.clone())
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
    fn native_cross_process_mode_allows_be_log_evidence_without_compat_probes() {
        let meta = QueryMeta {
            be_log_contains: vec!["NOVAROCKS_FAILED_FRAGMENT_REPORT_ACK".to_string()],
            ..QueryMeta::default()
        };

        validate_mode(&meta, ClusterMode::CrossProcess)
            .expect("runner-owned native BE logs must support evidence directives");
        validate_mode(&meta, ClusterMode::StarRocksCompat)
            .expect("compat BE logs must remain supported");
        validate_mode(&meta, ClusterMode::AllInOne)
            .expect_err("all-in-one has no runner-owned BE logs");
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
    fn exact_injected_query_cancellation_compares_per_be_identity_multisets() {
        let handle = FakeCompatHandle::new(vec![
            "NOVAROCKS_GRPC_SUBMIT_ACCEPTED query_hi=1 query_lo=2 finst_hi=3 finst_lo=4\n",
            "",
            "",
        ])
        .with_fragment_failure_token("step-token");
        let step = step(QueryMeta {
            fail_fragment_after_start_be_index: Some(1),
            be_log_exact_fragment_cancellation: Some(3),
            ..QueryMeta::default()
        });
        let before = snapshot(&step.meta, &handle).expect("capture armed trigger token");
        handle.append_log(
            0,
            "NOVAROCKS_GRPC_SUBMIT_ACCEPTED query_hi=7 query_lo=8 finst_hi=9 finst_lo=10\n",
        );
        handle.append_log(
            0,
            "NOVAROCKS_GRPC_SUBMIT_ACCEPTED query_hi=10 query_lo=20 finst_hi=101 finst_lo=201\nNOVAROCKS_CANCEL_FINST query_hi=10 query_lo=20 finst_hi=101 finst_lo=201\n",
        );
        handle.append_log(
            1,
            "NOVAROCKS_GRPC_SUBMIT_ACCEPTED query_hi=10 query_lo=20 finst_hi=102 finst_lo=202\nNOVAROCKS_FRAGMENT_EXECUTOR_FAILURE_INJECTED token=step-token query_hi=10 query_lo=20 finst_hi=102 finst_lo=202\nNOVAROCKS_FAILED_FRAGMENT_REPORT_ACK query_hi=10 query_lo=20 finst_hi=102 finst_lo=202\nNOVAROCKS_CANCEL_FINST query_hi=10 query_lo=20 finst_hi=102 finst_lo=202\n",
        );
        handle.append_log(
            2,
            "NOVAROCKS_GRPC_SUBMIT_ACCEPTED query_hi=10 query_lo=20 finst_hi=103 finst_lo=203\nNOVAROCKS_GRPC_SUBMIT_ACCEPTED query_hi=10 query_lo=20 finst_hi=104 finst_lo=204\nNOVAROCKS_CANCEL_FINST query_hi=10 query_lo=20 finst_hi=103 finst_lo=203\nNOVAROCKS_CANCEL_FINST query_hi=10 query_lo=20 finst_hi=104 finst_lo=204\n",
        );
        let mut log = String::new();

        run(&step, &handle, &before, &mut log)
            .expect("every accepted current-query identity is cancelled exactly once");

        assert!(
            log.contains(
                "@be_log_exact_fragment_cancellation PASS query_hi=10 query_lo=20 be_count=3 total=4"
            ),
            "{log}"
        );
    }

    #[test]
    fn exact_injected_query_cancellation_rejects_equal_counts_with_wrong_identity() {
        let handle =
            FakeCompatHandle::new(vec!["", "", ""]).with_fragment_failure_token("step-token");
        let step = step(QueryMeta {
            fail_fragment_after_start_be_index: Some(1),
            be_log_exact_fragment_cancellation: Some(3),
            ..QueryMeta::default()
        });
        let before = snapshot(&step.meta, &handle).expect("capture armed trigger token");
        handle.append_log(
            0,
            "NOVAROCKS_GRPC_SUBMIT_ACCEPTED query_hi=10 query_lo=20 finst_hi=101 finst_lo=201\nNOVAROCKS_GRPC_SUBMIT_ACCEPTED query_hi=10 query_lo=20 finst_hi=105 finst_lo=205\nNOVAROCKS_CANCEL_FINST query_hi=10 query_lo=20 finst_hi=101 finst_lo=201\nNOVAROCKS_CANCEL_FINST query_hi=10 query_lo=20 finst_hi=101 finst_lo=201\n",
        );
        handle.append_log(
            1,
            "NOVAROCKS_GRPC_SUBMIT_ACCEPTED query_hi=10 query_lo=20 finst_hi=102 finst_lo=202\nNOVAROCKS_FRAGMENT_EXECUTOR_FAILURE_INJECTED token=step-token query_hi=10 query_lo=20 finst_hi=102 finst_lo=202\nNOVAROCKS_FAILED_FRAGMENT_REPORT_ACK query_hi=10 query_lo=20 finst_hi=102 finst_lo=202\nNOVAROCKS_CANCEL_FINST query_hi=10 query_lo=20 finst_hi=102 finst_lo=202\n",
        );
        handle.append_log(
            2,
            "NOVAROCKS_GRPC_SUBMIT_ACCEPTED query_hi=10 query_lo=20 finst_hi=103 finst_lo=203\nNOVAROCKS_CANCEL_FINST query_hi=10 query_lo=20 finst_hi=103 finst_lo=203\n",
        );
        let mut log = String::new();

        let error = run(&step, &handle, &before, &mut log)
            .expect_err("A/B accepted but A/A cancelled must fail exact identity evidence");

        assert!(
            error.to_string().contains("BE[0] identity mismatch"),
            "{error:#}"
        );
    }

    #[test]
    fn exact_injected_query_cancellation_compares_each_be_not_only_global_identity() {
        let handle =
            FakeCompatHandle::new(vec!["", "", ""]).with_fragment_failure_token("step-token");
        let step = step(QueryMeta {
            fail_fragment_after_start_be_index: Some(1),
            be_log_exact_fragment_cancellation: Some(3),
            ..QueryMeta::default()
        });
        let before = snapshot(&step.meta, &handle).expect("capture armed trigger token");
        handle.append_log(
            0,
            "NOVAROCKS_GRPC_SUBMIT_ACCEPTED query_hi=10 query_lo=20 finst_hi=101 finst_lo=201\nNOVAROCKS_CANCEL_FINST query_hi=10 query_lo=20 finst_hi=102 finst_lo=202\n",
        );
        handle.append_log(
            1,
            "NOVAROCKS_GRPC_SUBMIT_ACCEPTED query_hi=10 query_lo=20 finst_hi=102 finst_lo=202\nNOVAROCKS_FRAGMENT_EXECUTOR_FAILURE_INJECTED token=step-token query_hi=10 query_lo=20 finst_hi=102 finst_lo=202\nNOVAROCKS_FAILED_FRAGMENT_REPORT_ACK query_hi=10 query_lo=20 finst_hi=102 finst_lo=202\nNOVAROCKS_CANCEL_FINST query_hi=10 query_lo=20 finst_hi=101 finst_lo=201\n",
        );
        handle.append_log(
            2,
            "NOVAROCKS_GRPC_SUBMIT_ACCEPTED query_hi=10 query_lo=20 finst_hi=103 finst_lo=203\nNOVAROCKS_CANCEL_FINST query_hi=10 query_lo=20 finst_hi=103 finst_lo=203\n",
        );

        let error = run(&step, &handle, &before, &mut String::new())
            .expect_err("globally equal identities assigned to the wrong BEs must fail");

        assert!(
            error.to_string().contains("BE[0] identity mismatch"),
            "{error:#}"
        );
    }

    #[test]
    fn exact_injected_query_cancellation_binds_failure_and_ack_to_the_same_be() {
        let handle =
            FakeCompatHandle::new(vec!["", "", ""]).with_fragment_failure_token("step-token");
        let step = step(QueryMeta {
            fail_fragment_after_start_be_index: Some(1),
            be_log_exact_fragment_cancellation: Some(3),
            ..QueryMeta::default()
        });
        let before = snapshot(&step.meta, &handle).expect("capture armed trigger token");
        handle.append_log(
            0,
            "NOVAROCKS_GRPC_SUBMIT_ACCEPTED query_hi=10 query_lo=20 finst_hi=101 finst_lo=201\nNOVAROCKS_FAILED_FRAGMENT_REPORT_ACK query_hi=10 query_lo=20 finst_hi=102 finst_lo=202\nNOVAROCKS_CANCEL_FINST query_hi=10 query_lo=20 finst_hi=101 finst_lo=201\n",
        );
        handle.append_log(
            1,
            "NOVAROCKS_GRPC_SUBMIT_ACCEPTED query_hi=10 query_lo=20 finst_hi=105 finst_lo=205\nNOVAROCKS_FRAGMENT_EXECUTOR_FAILURE_INJECTED token=step-token query_hi=10 query_lo=20 finst_hi=102 finst_lo=202\nNOVAROCKS_CANCEL_FINST query_hi=10 query_lo=20 finst_hi=105 finst_lo=205\n",
        );
        handle.append_log(
            2,
            "NOVAROCKS_GRPC_SUBMIT_ACCEPTED query_hi=10 query_lo=20 finst_hi=103 finst_lo=203\nNOVAROCKS_CANCEL_FINST query_hi=10 query_lo=20 finst_hi=103 finst_lo=203\n",
        );

        let error = run(&step, &handle, &before, &mut String::new()).expect_err(
            "the injected identity and its ACK must be proven on the BE that consumed the token",
        );

        assert!(
            error.to_string().contains("injected fragment"),
            "{error:#}"
        );
    }

    #[test]
    fn exact_injected_query_cancellation_rejects_duplicate_identity_evidence() {
        let handle =
            FakeCompatHandle::new(vec!["", "", ""]).with_fragment_failure_token("step-token");
        let step = step(QueryMeta {
            fail_fragment_after_start_be_index: Some(1),
            be_log_exact_fragment_cancellation: Some(3),
            ..QueryMeta::default()
        });
        let before = snapshot(&step.meta, &handle).expect("capture armed trigger token");
        handle.append_log(
            0,
            "NOVAROCKS_GRPC_SUBMIT_ACCEPTED query_hi=10 query_lo=20 finst_hi=101 finst_lo=201\nNOVAROCKS_GRPC_SUBMIT_ACCEPTED query_hi=10 query_lo=20 finst_hi=101 finst_lo=201\nNOVAROCKS_CANCEL_FINST query_hi=10 query_lo=20 finst_hi=101 finst_lo=201\nNOVAROCKS_CANCEL_FINST query_hi=10 query_lo=20 finst_hi=101 finst_lo=201\n",
        );
        handle.append_log(
            1,
            "NOVAROCKS_GRPC_SUBMIT_ACCEPTED query_hi=10 query_lo=20 finst_hi=102 finst_lo=202\nNOVAROCKS_FRAGMENT_EXECUTOR_FAILURE_INJECTED token=step-token query_hi=10 query_lo=20 finst_hi=102 finst_lo=202\nNOVAROCKS_FAILED_FRAGMENT_REPORT_ACK query_hi=10 query_lo=20 finst_hi=102 finst_lo=202\nNOVAROCKS_CANCEL_FINST query_hi=10 query_lo=20 finst_hi=102 finst_lo=202\n",
        );
        handle.append_log(
            2,
            "NOVAROCKS_GRPC_SUBMIT_ACCEPTED query_hi=10 query_lo=20 finst_hi=103 finst_lo=203\nNOVAROCKS_CANCEL_FINST query_hi=10 query_lo=20 finst_hi=103 finst_lo=203\n",
        );

        let error = run(&step, &handle, &before, &mut String::new())
            .expect_err("matching duplicate identity evidence must fail closed");

        assert!(
            error
                .to_string()
                .contains("accepted duplicate fragment identities"),
            "{error:#}"
        );
    }

    #[test]
    fn exact_injected_query_cancellation_requires_declared_be_coverage() {
        let handle = FakeCompatHandle::new(vec!["", ""]).with_fragment_failure_token("step-token");
        let step = step(QueryMeta {
            fail_fragment_after_start_be_index: Some(1),
            be_log_exact_fragment_cancellation: Some(3),
            ..QueryMeta::default()
        });
        let before = snapshot(&step.meta, &handle).expect("capture armed trigger token");

        let error = run(&step, &handle, &before, &mut String::new())
            .expect_err("a two-BE cluster cannot satisfy a three-BE proof");

        assert!(error.to_string().contains("found 2"), "{error:#}");
    }

    #[test]
    fn exact_injected_query_cancellation_rejects_malformed_markers() {
        let handle =
            FakeCompatHandle::new(vec!["", "", ""]).with_fragment_failure_token("step-token");
        let step = step(QueryMeta {
            fail_fragment_after_start_be_index: Some(1),
            be_log_exact_fragment_cancellation: Some(3),
            ..QueryMeta::default()
        });
        let before = snapshot(&step.meta, &handle).expect("capture armed trigger token");
        handle.append_log(
            0,
            "NOVAROCKS_GRPC_SUBMIT_ACCEPTED query_hi=10 query_lo=20 finst_hi=101\n",
        );
        handle.append_log(
            1,
            "NOVAROCKS_FRAGMENT_EXECUTOR_FAILURE_INJECTED token=step-token query_hi=10 query_lo=20 finst_hi=102 finst_lo=202\nNOVAROCKS_FAILED_FRAGMENT_REPORT_ACK query_hi=10 query_lo=20 finst_hi=102 finst_lo=202\n",
        );

        let error = run(&step, &handle, &before, &mut String::new())
            .expect_err("malformed identity marker must fail closed");

        assert!(
            error
                .to_string()
                .contains("malformed accepted-fragment marker"),
            "{error:#}"
        );
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
