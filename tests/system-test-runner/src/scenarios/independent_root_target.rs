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

//! Independent original-BE marker observation for the exact MySQL fixture.
//! Prepared markers are source observations, never Installed or body/ACK authority.
use super::result_delivery_root_protocol::parse_created_task_candidates;
use crate::scenario::ScenarioContext;
use anyhow::{Context, Result, ensure};
use novarocks_cluster_harness::process_resources::ProcessLaunchIdentity;
use novarocks_execution_contract::identity::{QueryContextRef, TaskIdentity};
use novarocks_proto_codec::FieldPath;
use novarocks_task_codec::identity::decode_task_identity;
use novarocks_types::{BackendProcessId, FrontendProcessId};
use sha2::{Digest, Sha256};
use std::io::Read;
use std::time::Instant;

const BACKENDS: usize = 3;
const LOG_BYTES: u64 = 2 * 1024 * 1024;
const MARKERS: usize = 8;
const LINE_BYTES: usize = 384;
const CREATE: &str = "NOVAROCKS_TASK_CREATE_APPLIED";
const CONTEXT: &str = "NOVAROCKS_TASK_CONTEXT_ESTABLISH_APPLIED";
const ROOT: &str = "NOVAROCKS_TASK_PREPARED_CLIENT_ROOT";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct LogAnchor {
    bytes: u64,
    sha256: [u8; 32],
}
struct LogDelta {
    anchor: LogAnchor,
    markers: Vec<String>,
}

#[derive(Clone, Copy)]
pub(crate) enum FrontendIdentitySource {
    ExactMysqlFixture,
    RootObservation,
}
pub(crate) struct IndependentRootObserver {
    source: FrontendIdentitySource,
    original_deadline: Instant,
    expected_frontend: FrontendProcessId,
    actual_backends: [BackendProcessId; BACKENDS],
    original_roles: Vec<ProcessLaunchIdentity>,
    before: [LogAnchor; BACKENDS],
}
pub(crate) struct IndependentRootTarget {
    pub root: TaskIdentity,
    pub root_backend_index: usize,
    pub backend_process_from_descriptor: BackendProcessId,
    pub frontend: FrontendProcessId,
    pub contexts: [Option<QueryContextRef>; BACKENDS],
    pub fresh_tasks: Vec<(usize, TaskIdentity)>,
    pub marker_counts: [usize; BACKENDS],
    pub baseline_sha256: [[u8; 32]; BACKENDS],
    pub after_sha256: [[u8; 32]; BACKENDS],
    pub after_log_bytes: [u64; BACKENDS],
}

fn before_deadline(deadline: Instant) -> Result<()> {
    ensure!(
        Instant::now() < deadline,
        "original exact MySQL root observation deadline expired"
    );
    Ok(())
}
impl IndependentRootObserver {
    /// Original snapshot sizes only; no new read, payload alias or target authority.
    pub(crate) fn original_baseline_log_bytes(&self) -> [u64; BACKENDS] {
        self.before.map(|anchor| anchor.bytes)
    }

    /// Call only after original idle/root-zero baseline and before the sole target SQL.
    /// expected_frontend must come from the independent actual-FE stdout marker.
    pub(crate) fn capture_baseline(
        context: &mut ScenarioContext,
        expected_frontend: FrontendProcessId,
    ) -> Result<Self> {
        let original_deadline = context.exact_mysql_deadline()?;
        Self::capture_baseline_until(
            context,
            expected_frontend,
            original_deadline,
            FrontendIdentitySource::ExactMysqlFixture,
        )
    }
    pub(crate) fn capture_baseline_until(
        context: &mut ScenarioContext,
        expected_frontend: FrontendProcessId,
        original_deadline: Instant,
        source: FrontendIdentitySource,
    ) -> Result<Self> {
        before_deadline(original_deadline)?;
        let original_roles = context.recheck_live_process_launch_identities()?;
        ensure!(
            original_roles.len() == BACKENDS + 1,
            "original role inventory is not 1FE+3BE"
        );
        let actual_backends = context
            .handle()
            .original_exact_mysql_backend_process_ids(original_deadline)?;
        let snapshots = read_logs(context, [None; BACKENDS], original_deadline)?;
        ensure!(
            context.recheck_live_process_launch_identities()? == original_roles,
            "original role instance changed during root baseline"
        );
        before_deadline(original_deadline)?;
        Ok(Self {
            source,
            original_deadline,
            expected_frontend,
            actual_backends,
            original_roles,
            before: snapshots.map(|snapshot| snapshot.anchor),
        })
    }
    /// Scalar source observations only; no reply/body/guard or Root authority.
    /// The caller must use the freshly observed target, never reconstruct it from census.
    pub(crate) fn source_evidence(
        &self,
        target: &IndependentRootTarget,
    ) -> Result<serde_json::Value> {
        ensure!(
            target.root_backend_index < BACKENDS
                && target.frontend == self.expected_frontend
                && target.backend_process_from_descriptor
                    == self.actual_backends[target.root_backend_index]
                && target.root.backend_process_id() == target.backend_process_from_descriptor
                && target.fresh_tasks.len() <= BACKENDS * MARKERS
                && target.marker_counts.iter().all(|n| *n <= MARKERS)
                && target.baseline_sha256 == self.before.map(|a| a.sha256)
                && target.after_log_bytes.iter().all(|n| *n <= LOG_BYTES),
            "target source evidence differs from original bounded observer"
        );
        let identity = |task: TaskIdentity| {
            let e = task.query_execution_id();
            serde_json::json!({"query_hi":e.query_id().high(),"query_lo":e.query_id().low(),
                "attempt":e.attempt_id().get(),"stage":task.stage_id().get(),"task":task.task_id().get(),
                "backend":task.backend_process_id().to_string()})
        };
        let contexts = target.contexts.map(|ctx| {
            ctx.map(|ctx| {
                let e = ctx.query_execution_id();
                serde_json::json!({"query_hi":e.query_id().high(),"query_lo":e.query_id().low(),
                "attempt":e.attempt_id().get(),"frontend":ctx.frontend_process_id().to_string(),
                "backend":ctx.backend_process_id().to_string()})
            })
        });
        let tasks:Vec<_>=target.fresh_tasks.iter().map(|(backend,task)|
            serde_json::json!({"backend_index":backend,"identity":identity(*task)})).collect();
        Ok(
            serde_json::json!({"schema_version":1,"source":match self.source {
            FrontendIdentitySource::ExactMysqlFixture=>"ExactMysqlFixture",
            FrontendIdentitySource::RootObservation=>"RootObservation"},
            "frontend":target.frontend.to_string(),"root":identity(target.root),
            "root_backend_index":target.root_backend_index,
            "backend_process_from_descriptor":target.backend_process_from_descriptor.to_string(),
            "actual_backend_descriptors":self.actual_backends.map(|id| id.to_string()),
            "original_roles":self.original_roles,"contexts":contexts,"fresh_tasks":tasks,
            "marker_counts":target.marker_counts,
            "baseline_log_bytes":self.before.map(|a| a.bytes),
            "baseline_sha256":target.baseline_sha256,"after_log_bytes":target.after_log_bytes,
            "after_sha256":target.after_sha256}),
        )
    }
    /// Call once the original gate is actually held, before cancellation/health SQL.
    /// This emits no SQL target, Root RPC, proxy, registry entry, ACK or authority.
    pub(crate) fn observe(&self, context: &mut ScenarioContext) -> Result<IndependentRootTarget> {
        self.observe_until(context, self.original_deadline)
    }
    pub(crate) fn observe_until(
        &self,
        context: &mut ScenarioContext,
        step_deadline: Instant,
    ) -> Result<IndependentRootTarget> {
        ensure!(
            step_deadline <= self.original_deadline,
            "root observer step renews original clock"
        );
        before_deadline(step_deadline)?;
        if matches!(self.source, FrontendIdentitySource::ExactMysqlFixture) {
            ensure!(
                context.exact_mysql_deadline()? == self.original_deadline,
                "root observer was given a different original exact clock"
            );
        }
        ensure!(
            context.recheck_live_process_launch_identities()? == self.original_roles,
            "original role instance changed before root observation"
        );
        ensure!(
            context
                .handle()
                .original_exact_mysql_backend_process_ids(step_deadline)?
                == self.actual_backends,
            "actual live backend descriptor UUID changed"
        );
        let snapshots = read_logs(context, self.before.map(Some), step_deadline)?;
        let target = resolve(
            &snapshots,
            self.expected_frontend,
            &self.actual_backends,
            self.before.map(|anchor| anchor.sha256),
        )?;
        ensure!(
            context.recheck_live_process_launch_identities()? == self.original_roles,
            "original role instance changed during root observation"
        );
        before_deadline(step_deadline)?;
        Ok(target)
    }
}
fn read_logs(
    context: &mut ScenarioContext,
    before: [Option<LogAnchor>; BACKENDS],
    deadline: Instant,
) -> Result<[LogDelta; BACKENDS]> {
    let mut snapshots = Vec::with_capacity(BACKENDS);
    for (backend, anchor) in before.into_iter().enumerate() {
        before_deadline(deadline)?;
        snapshots.push(context.handle().with_original_backend_log_snapshot(
            backend,
            LOG_BYTES,
            deadline,
            |reader, length| scan(reader, length, anchor, deadline),
        )?);
        before_deadline(deadline)?;
    }
    snapshots
        .try_into()
        .map_err(|_| anyhow::anyhow!("original backend log inventory changed"))
}

// Strict UTF-8 validation across fixed read chunks; no full-log String/copy.
#[derive(Default)]
struct Utf8Check {
    trailing: [u8; 3],
    used: usize,
}
impl Utf8Check {
    fn push(&mut self, bytes: &[u8]) -> Result<()> {
        let mut joined = [0; 515];
        joined[..self.used].copy_from_slice(&self.trailing[..self.used]);
        joined[self.used..self.used + bytes.len()].copy_from_slice(bytes);
        let length = self.used + bytes.len();
        match std::str::from_utf8(&joined[..length]) {
            Ok(_) => self.used = 0,
            Err(error) => {
                ensure!(
                    error.error_len().is_none(),
                    "original BE log is not valid UTF-8"
                );
                let tail = &joined[error.valid_up_to()..length];
                ensure!(tail.len() <= 3, "invalid original BE log UTF-8 tail");
                self.trailing[..tail.len()].copy_from_slice(tail);
                self.used = tail.len();
            }
        }
        Ok(())
    }
}
fn scan(
    reader: &mut dyn Read,
    length: u64,
    before: Option<LogAnchor>,
    deadline: Instant,
) -> Result<LogDelta> {
    ensure!(
        length <= LOG_BYTES,
        "original BE log exceeds 2 MiB observation cap"
    );
    let baseline = before.map_or(length, |anchor| anchor.bytes);
    ensure!(
        baseline <= length,
        "original BE log was truncated below baseline"
    );
    let mut hash = Sha256::new();
    let mut prefix = Sha256::new();
    let mut utf8 = Utf8Check::default();
    let mut scratch = [0; 512];
    let mut offset = 0u64;
    let mut last = None;
    let mut line = [0; LINE_BYTES];
    let mut used = 0usize;
    let mut overflow = false;
    let mut rolling = [0; 64];
    let mut rolling_used = 0usize;
    let mut marker_seen = false;
    let mut markers = Vec::with_capacity(MARKERS);
    while offset < length {
        before_deadline(deadline)?;
        let take = ((length - offset) as usize).min(scratch.len());
        let read = reader.read(&mut scratch[..take])?;
        ensure!(
            read > 0,
            "original BE log snapshot ended before declared length"
        );
        let bytes = &scratch[..read];
        hash.update(bytes);
        utf8.push(bytes)?;
        let prefix_bytes = baseline.saturating_sub(offset).min(read as u64) as usize;
        prefix.update(&bytes[..prefix_bytes]);
        for byte in bytes {
            last = Some(*byte);
            if offset >= baseline && before.is_some() {
                if *byte == b'\n' {
                    if marker_seen {
                        ensure!(
                            !overflow && markers.len() < MARKERS,
                            "original BE marker inventory exceeds 8 markers/384 bytes"
                        );
                        markers.push(std::str::from_utf8(&line[..used])?.to_owned());
                    }
                    used = 0;
                    overflow = false;
                    rolling_used = 0;
                    marker_seen = false;
                } else {
                    if used < line.len() {
                        line[used] = *byte;
                        used += 1;
                    } else {
                        overflow = true;
                    }
                    if rolling_used == rolling.len() {
                        rolling.copy_within(1.., 0);
                        rolling_used -= 1;
                    }
                    rolling[rolling_used] = *byte;
                    rolling_used += 1;
                    marker_seen |= [CREATE, CONTEXT, ROOT]
                        .iter()
                        .any(|name| rolling[..rolling_used].ends_with(name.as_bytes()));
                }
            }
            offset += 1;
        }
    }
    ensure!(
        utf8.used == 0,
        "original BE log ends inside UTF-8 codepoint"
    );
    ensure!(
        length == 0 || last == Some(b'\n'),
        "original BE log ends inside a line"
    );
    if let Some(before) = before {
        ensure!(
            <[u8; 32]>::from(prefix.finalize()) == before.sha256,
            "original BE baseline bytes were replaced instead of appended"
        );
    }
    before_deadline(deadline)?;
    Ok(LogDelta {
        anchor: LogAnchor {
            bytes: length,
            sha256: hash.finalize().into(),
        },
        markers,
    })
}

// Fixed mapping of the closed name reuses the already strict TaskCreate parser.
// Each bounded single marker still reaches production full TaskIdentity decode.
fn decode_task_marker(line: &str, prefix: &str) -> Result<TaskIdentity> {
    let suffix = line
        .strip_prefix(prefix)
        .filter(|suffix| suffix.starts_with(' '))
        .context("original task marker has unexpected prefix")?;
    let normalized = format!("{CREATE}{suffix}\n");
    let candidates = parse_created_task_candidates(&[String::new()], &[normalized], 0)?;
    ensure!(
        candidates.len() == 1,
        "original task marker is not one complete identity"
    );
    Ok(decode_task_identity(
        &candidates[0],
        FieldPath::root("independent_original_root_task"),
    )?)
}
fn resolve(
    snapshots: &[LogDelta; BACKENDS],
    expected_frontend: FrontendProcessId,
    processes: &[BackendProcessId; BACKENDS],
    baseline_sha256: [[u8; 32]; BACKENDS],
) -> Result<IndependentRootTarget> {
    ensure!(
        processes[0] != processes[1]
            && processes[0] != processes[2]
            && processes[1] != processes[2],
        "actual three-BE descriptor UUIDs are not unique"
    );
    // The total bound derives only from original 3BE x 8 marker positions.
    let mut tasks = Vec::with_capacity(BACKENDS * MARKERS);
    let mut root = None;
    for (backend, snapshot) in snapshots.iter().enumerate() {
        ensure!(
            snapshot.markers.len() <= MARKERS,
            "marker inventory exceeds original bound"
        );
        for line in &snapshot.markers {
            ensure!(
                line.len() <= LINE_BYTES,
                "marker exceeds original line bound"
            );
            if line.contains(CREATE) {
                let task = decode_task_marker(line, CREATE)?;
                ensure!(
                    task.backend_process_id() == processes[backend],
                    "fresh created task differs from actual original backend UUID"
                );
                ensure!(
                    !tasks.iter().any(|(_, prior)| *prior == task),
                    "duplicate fresh created task identity"
                );
                tasks.push((backend, task));
            } else if line.contains(ROOT) {
                let task = decode_task_marker(line, ROOT)?;
                ensure!(
                    root.replace((backend, task)).is_none(),
                    "multiple prepared ClientRows roots"
                );
            } else {
                ensure!(line.contains(CONTEXT), "unknown bounded marker kind");
            }
        }
    }
    ensure!(!tasks.is_empty(), "no fresh target SQL task creation");
    let execution = tasks[0].1.query_execution_id();
    ensure!(
        tasks
            .iter()
            .all(|(_, task)| task.query_execution_id() == execution),
        "fresh SQL task set spans multiple executions"
    );
    let (root_backend_index, root) = root.context("missing fresh prepared ClientRows root")?;
    ensure!(
        root.backend_process_id() == processes[root_backend_index]
            && tasks.contains(&(root_backend_index, root)),
        "prepared root is not an exact fresh created task on its actual original backend"
    );
    let mut contexts = [None; BACKENDS];
    let execution_text = format!(
        "execution_id={}:{}:{}",
        execution.query_id().high(),
        execution.query_id().low(),
        execution.attempt_id().get()
    );
    for (backend, snapshot) in snapshots.iter().enumerate() {
        for line in snapshot
            .markers
            .iter()
            .filter(|line| line.contains(CONTEXT))
        {
            let fields: Vec<_> = line.split(' ').collect();
            ensure!(
                fields.len() == 4 && fields[0] == CONTEXT && fields[1] == execution_text,
                "fresh context has unknown fields or different execution"
            );
            let frontend_text = fields[2]
                .strip_prefix("frontend=")
                .context("missing context frontend")?;
            let frontend: FrontendProcessId = frontend_text.parse()?;
            let backend_text = fields[3]
                .strip_prefix("backend=")
                .context("missing context backend")?;
            let observed_backend: BackendProcessId = backend_text.parse()?;
            ensure!(
                frontend.to_string() == frontend_text
                    && frontend == expected_frontend
                    && observed_backend.to_string() == backend_text
                    && observed_backend == processes[backend],
                "context differs from independently actual original FE/BE processes"
            );
            ensure!(
                contexts[backend].is_none(),
                "duplicate fresh context on one backend"
            );
            contexts[backend] = Some(QueryContextRef::new(execution, frontend, observed_backend));
        }
        ensure!(
            contexts[backend].is_some() == tasks.iter().any(|(slot, _)| *slot == backend),
            "fresh context membership differs from actual task placement"
        );
    }
    Ok(IndependentRootTarget {
        root,
        root_backend_index,
        backend_process_from_descriptor: processes[root_backend_index],
        frontend: expected_frontend,
        contexts,
        fresh_tasks: tasks,
        marker_counts: std::array::from_fn(|i| snapshots[i].markers.len()),
        baseline_sha256,
        after_sha256: std::array::from_fn(|i| snapshots[i].anchor.sha256),
        after_log_bytes: std::array::from_fn(|i| snapshots[i].anchor.bytes),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;
    use std::time::Duration;
    #[test]
    fn source_evidence_retains_all_descriptors_contexts_and_original_log_anchors() {
        let (logs, ids) = fixtures(&[1, 2]);
        let mut target = resolve(&logs, frontend(), &ids, [empty_sha(); BACKENDS]).unwrap();
        let observer = IndependentRootObserver {
            source: FrontendIdentitySource::RootObservation,
            original_deadline: Instant::now() + Duration::from_secs(1),
            expected_frontend: frontend(),
            actual_backends: ids,
            original_roles: Vec::new(),
            before: [LogAnchor {
                bytes: 0,
                sha256: empty_sha(),
            }; BACKENDS],
        };
        let evidence = observer.source_evidence(&target).unwrap();
        assert_eq!(evidence["source"], "RootObservation");
        assert_eq!(
            evidence["actual_backend_descriptors"]
                .as_array()
                .unwrap()
                .len(),
            3
        );
        assert_eq!(
            evidence["contexts"]
                .as_array()
                .unwrap()
                .iter()
                .filter(|v| !v.is_null())
                .count(),
            2
        );
        assert_eq!(evidence["fresh_tasks"].as_array().unwrap().len(), 2);
        assert_eq!(
            evidence["baseline_sha256"],
            serde_json::json!(([empty_sha(); BACKENDS]))
        );
        assert_eq!(
            evidence["after_log_bytes"],
            serde_json::json!(logs.each_ref().map(|log| log.anchor.bytes))
        );
        target.baseline_sha256[0] = [1; 32];
        assert!(observer.source_evidence(&target).is_err());
        target.baseline_sha256[0] = empty_sha();
        target.marker_counts[0] = MARKERS + 1;
        assert!(observer.source_evidence(&target).is_err());
        target.marker_counts[0] = 0;
        target.backend_process_from_descriptor = ids[0];
        assert!(observer.source_evidence(&target).is_err());
    }

    fn processes() -> [BackendProcessId; BACKENDS] {
        [
            "01900000-0000-7000-8000-000000000001",
            "01900000-0000-7000-8000-000000000002",
            "01900000-0000-7000-8000-000000000003",
        ]
        .map(|s| s.parse().unwrap())
    }
    fn frontend() -> FrontendProcessId {
        "01900000-0000-7000-8000-000000000004".parse().unwrap()
    }
    fn empty_sha() -> [u8; 32] {
        Sha256::digest(b"").into()
    }
    fn decode_log(bytes: &[u8], before: Option<LogAnchor>) -> Result<LogDelta> {
        scan(
            &mut Cursor::new(bytes),
            bytes.len() as u64,
            before,
            Instant::now() + Duration::from_secs(1),
        )
    }
    pub(super) fn fixtures(
        placements: &[usize],
    ) -> ([LogDelta; BACKENDS], [BackendProcessId; BACKENDS]) {
        let processes = processes();
        let mut logs: [String; BACKENDS] = std::array::from_fn(|_| String::new());
        for (i, backend) in placements.iter().copied().enumerate() {
            // The unique root is intentionally lowest stage, not greatest stage/task.
            logs[backend].push_str(&format!(
                "{CREATE} execution_id=0:-7:2 stage={} task={} backend={}\n",
                i + 1,
                i + 1,
                processes[backend]
            ));
            if i == 0 {
                logs[backend].push_str(&format!(
                    "{ROOT} execution_id=0:-7:2 stage=1 task=1 backend={}\n",
                    processes[backend]
                ));
            }
        }
        for backend in 0..BACKENDS {
            if placements.contains(&backend) {
                logs[backend].push_str(&format!(
                    "{CONTEXT} execution_id=0:-7:2 frontend={} backend={}\n",
                    frontend(),
                    processes[backend]
                ));
            }
        }
        (
            logs.map(|log| {
                decode_log(
                    log.as_bytes(),
                    Some(LogAnchor {
                        bytes: 0,
                        sha256: empty_sha(),
                    }),
                )
                .unwrap()
            }),
            processes,
        )
    }
    pub(super) fn resolve_fixture(
        snapshots: &[LogDelta; BACKENDS],
        processes: &[BackendProcessId; BACKENDS],
    ) -> Result<IndependentRootTarget> {
        resolve(snapshots, frontend(), processes, [empty_sha(); BACKENDS])
    }
    #[test]
    fn actual_one_two_and_many_tasks_accept_without_stage_or_participant_guess() {
        for placements in [vec![1], vec![2, 1], vec![0, 1, 2, 2, 1]] {
            let (snapshots, processes) = fixtures(&placements);
            let target = resolve_fixture(&snapshots, &processes).unwrap();
            assert_eq!(target.fresh_tasks.len(), placements.len());
            assert_eq!(target.root_backend_index, placements[0]);
            assert_eq!(target.root.stage_id().get(), 1);
            for slot in 0..BACKENDS {
                assert_eq!(target.contexts[slot].is_some(), placements.contains(&slot));
            }
        }
    }
    #[test]
    fn missing_multiple_noncreated_or_wrongbackend_roots_refuse() {
        for alteration in 0..4 {
            let (mut snapshots, processes) = fixtures(&[1, 2]);
            let index = snapshots[1]
                .markers
                .iter()
                .position(|line| line.starts_with(ROOT))
                .unwrap();
            match alteration {
                0 => {
                    snapshots[1].markers.remove(index);
                }
                1 => {
                    let duplicate = snapshots[1].markers[index].clone();
                    snapshots[1].markers.push(duplicate);
                }
                2 => {
                    snapshots[1].markers[index] =
                        snapshots[1].markers[index].replace("task=1", "task=99");
                }
                _ => {
                    snapshots[1].markers[index] = snapshots[1].markers[index]
                        .replace(&processes[1].to_string(), &processes[0].to_string());
                }
            }
            assert!(
                resolve_fixture(&snapshots, &processes).is_err(),
                "alteration={alteration}"
            );
        }
    }
    #[test]
    fn duplicate_otherexecution_zero_and_noncanonical_tasks_refuse() {
        for alteration in 0..5 {
            let (mut snapshots, processes) = fixtures(&[1, 2]);
            let index = snapshots[2]
                .markers
                .iter()
                .position(|line| line.starts_with(CREATE))
                .unwrap();
            match alteration {
                0 => {
                    let duplicate = snapshots[2].markers[index].clone();
                    snapshots[2].markers.push(duplicate);
                }
                1 => {
                    snapshots[2].markers[index] =
                        snapshots[2].markers[index].replace("0:-7:2", "0:-8:2");
                }
                2 => {
                    snapshots[2].markers[index] =
                        snapshots[2].markers[index].replace("task=2", "task=0");
                }
                3 => {
                    snapshots[2].markers[index] =
                        snapshots[2].markers[index].replace("stage=2", "stage=02");
                }
                _ => {
                    snapshots[2].markers[index].push_str(" extra=1");
                }
            }
            assert!(
                resolve_fixture(&snapshots, &processes).is_err(),
                "alteration={alteration}"
            );
        }
    }
    #[test]
    fn missing_duplicate_nonparticipant_wrongfe_and_wrongexecution_contexts_refuse() {
        for alteration in 0..5 {
            let (mut snapshots, processes) = fixtures(&[1, 2]);
            let index = snapshots[1]
                .markers
                .iter()
                .position(|line| line.starts_with(CONTEXT))
                .unwrap();
            match alteration {
                0 => {
                    snapshots[1].markers.remove(index);
                }
                1 => {
                    let duplicate = snapshots[1].markers[index].clone();
                    snapshots[1].markers.push(duplicate);
                }
                2 => {
                    snapshots[0].markers.push(format!(
                        "{CONTEXT} execution_id=0:-7:2 frontend={} backend={}",
                        frontend(),
                        processes[0]
                    ));
                }
                3 => {
                    snapshots[1].markers[index] = snapshots[1].markers[index].replace(
                        &frontend().to_string(),
                        "01900000-0000-7000-8000-000000000005",
                    );
                }
                _ => {
                    snapshots[1].markers[index] =
                        snapshots[1].markers[index].replace("0:-7:2", "0:-7:3");
                }
            }
            assert!(
                resolve_fixture(&snapshots, &processes).is_err(),
                "alteration={alteration}"
            );
        }
    }
    #[test]
    fn baseline_hash_proves_exact_old_prefix_and_partial_or_short_snapshot_refuses() {
        let baseline = decode_log(b"old log\n", None).unwrap().anchor;
        let mut appended = b"old log\n".to_vec();
        appended.extend_from_slice(b"new log\n");
        assert!(decode_log(&appended, Some(baseline)).is_ok());
        assert!(decode_log(b"bad log\nnew log\n", Some(baseline)).is_err());
        assert!(decode_log(b"old", Some(baseline)).is_err());
        assert!(decode_log(b"old log\nnew log", Some(baseline)).is_err());
        assert!(
            scan(
                &mut Cursor::new(b"short"),
                20,
                None,
                Instant::now() + Duration::from_secs(1)
            )
            .is_err()
        );
    }
    #[test]
    fn marker_overflow_long_late_embedded_marker_and_invalid_utf8_refuse() {
        let line = format!(
            "{CREATE} execution_id=0:-7:2 stage=1 task=1 backend={}\n",
            processes()[0]
        );
        let baseline = Some(LogAnchor {
            bytes: 0,
            sha256: empty_sha(),
        });
        assert!(decode_log(line.repeat(MARKERS + 1).as_bytes(), baseline).is_err());
        let long = format!("{}{}", "x".repeat(LINE_BYTES + 20), line);
        assert!(decode_log(long.as_bytes(), baseline).is_err());
        assert!(decode_log(b"unrelated \xff\n", baseline).is_err());
        assert!(decode_log(b"unterminated utf8 \xf0", baseline).is_err());
    }
    #[test]
    fn long_unrelated_line_and_chunk_split_unicode_keep_exact_hash_without_log_copy() {
        let mut bytes = vec![b'x'; 511];
        bytes.extend_from_slice("文\n".as_bytes());
        let snapshot = decode_log(&bytes, None).unwrap();
        assert_eq!(
            snapshot.anchor.sha256,
            <[u8; 32]>::from(Sha256::digest(&bytes))
        );
        assert!(snapshot.markers.is_empty());
    }
    #[test]
    fn original_log_cap_and_expired_clock_refuse_before_read() {
        struct PanicRead;
        impl Read for PanicRead {
            fn read(&mut self, _: &mut [u8]) -> std::io::Result<usize> {
                panic!("must not read");
            }
        }
        assert!(
            scan(
                &mut PanicRead,
                LOG_BYTES + 1,
                None,
                Instant::now() + Duration::from_secs(1)
            )
            .is_err()
        );
        assert!(scan(&mut PanicRead, 1, None, Instant::now()).is_err());
    }
}

#[cfg(test)]
mod binding_projection_tests {
    use super::super::exact_mysql_native_oracle::ActualBinding;
    use super::*;
    use crate::exact_mysql_target_binding::OriginalTargetBinding;
    #[test]
    fn independent_source_binding_preserves_every_raw_domain_and_descriptor() {
        let (snapshots, processes) = super::tests::fixtures(&[2, 1]);
        let mut target = super::tests::resolve_fixture(&snapshots, &processes).unwrap();
        let mut raw = OriginalTargetBinding {
            frontend_process_id: target.frontend,
            connection_id: 71,
            connection_generation: 19,
            session_connection_id: 71,
            session_epoch: 23,
            statement_generation: 29,
            sql_sha256: [31; 32],
        };
        let binding = ActualBinding::from_original_sources(raw, &target).unwrap();
        assert_eq!(binding.connection.generation, 19);
        assert_eq!(
            (
                binding.statement.session_epoch,
                binding.statement.generation
            ),
            (23, 29)
        );
        assert_eq!(
            (
                binding.root.query_high,
                binding.root.query_low,
                binding.root.attempt
            ),
            (0, -7, 2)
        );
        assert_eq!(binding.backend_uuid_from_role, processes[2].to_bytes());
        raw.session_connection_id = 72;
        assert!(ActualBinding::from_original_sources(raw, &target).is_err());
        raw.session_connection_id = 71;
        raw.frontend_process_id = FrontendProcessId::new_v7();
        assert!(ActualBinding::from_original_sources(raw, &target).is_err());
        raw.frontend_process_id = target.frontend;
        target.backend_process_from_descriptor = processes[0];
        assert!(ActualBinding::from_original_sources(raw, &target).is_err());
    }
}
