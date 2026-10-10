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

//! An explicit readonly consumer of an externally prepared stock HMS bulk input.
//! This module owns no fixture, producer, catalog mutation or role runtime.
//! Observations come from the original generation on the existing FE HTTP listener.
use super::connector::require_three_backends;
use crate::actors::mysql_stream::{
    AsyncMysqlStream, BoundedCommandError, BoundedCommandResponse, BoundedMysqlError,
    CommandResponseFailure, TextResultObservation,
};
use crate::scenario::{Scenario, ScenarioContext, ScenarioLaunchConfig};
use anyhow::{Result, bail, ensure};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{
    fmt, fs,
    io::{Read, Write},
    path::{Path, PathBuf},
    sync::{
        Condvar, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant},
};

const FILE_CAP: usize = 1_048_576;
const METRICS_CAP: usize = 1_048_576;
const PHASE_MILLIS: u64 = 120_000; // Existing listing operation ceiling, not a new product bound.
const CLIENTS: [usize; 3] = [1, 8, 16];
const ROWS: u64 = 16_384;
const CATALOG: &str = "m07_hms_readonly_cl";
const PROBE_ENV: &str = "NOVAROCKS_HMS_LISTING_OBSERVATION_CATALOG";
const VIEW_REFUSAL: &str = "list_views is not supported by this catalog";
const INFO_SQL: &str = "SELECT TABLE_SCHEMA AS table_schema, TABLE_NAME AS table_name, TABLE_TYPE AS table_type FROM m07_hms_readonly_cl.information_schema.tables WHERE TABLE_CATALOG = 'm07_hms_readonly_cl' AND TABLE_SCHEMA LIKE 'cl_ns_%' ORDER BY TABLE_SCHEMA, TABLE_NAME";

#[derive(Deserialize, Serialize, Debug, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(crate) struct Normal {
    pub(crate) namespaces: u64,
    pub(crate) tables_per_namespace: u64,
    pub(crate) views_per_namespace: u64,
    pub(crate) page_size: u64,
    pub(crate) namespace_pattern: String,
    pub(crate) table_pattern: String,
    pub(crate) view_pattern: String,
    pub(crate) concurrency: [usize; 3],
}
impl Normal {
    fn validate(&self) -> Result<()> {
        ensure!(
            self.namespaces == 32
                && self.tables_per_namespace == 512
                && self.views_per_namespace == 512
                && self.page_size == 256
                && self.namespace_pattern == "cl_ns_%04d"
                && self.table_pattern == "cl_table_%06d"
                && self.view_pattern == "cl_view_%06d"
                && self.concurrency == CLIENTS,
            "HMS bulk normal geometry differs"
        );
        Ok(())
    }
}
#[derive(Deserialize, Serialize, Debug, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(crate) struct Locator {
    pub(crate) control_root: String,
    pub(crate) daemon_id: String,
}
#[derive(Deserialize, Serialize, Debug, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(crate) struct FixtureBinding {
    pub(crate) publication: String,
    pub(crate) manifest_sha256: String,
    pub(crate) owner_locator: Locator,
    pub(crate) env_id: String,
    pub(crate) catalog_id: String,
    pub(crate) object_store_id: String,
    pub(crate) hms_uri: String,
    pub(crate) hms_project: String,
    pub(crate) hms_container_id: String,
    pub(crate) hms_image_id: String,
    pub(crate) hms_manifest_sha256: String,
    pub(crate) warehouse: String,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Ready {
    schema_version: u32,
    scope: String,
    normal: Normal,
    bulk_freeze_sha256: String,
    binding: FixtureBinding,
    oracle_manifest_sha256: String,
    tables: u64,
    views: u64,
    native_acceptance: bool,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Binding {
    pub(crate) schema_version: u32,
    pub(crate) frozen_before_execution: bool,
    pub(crate) runnable: bool,
    pub(crate) clean_source_revision: String,
    pub(crate) source_tree_sha256: String,
    pub(crate) cargo_lock_sha256: String,
    pub(crate) server_sha256: String,
    pub(crate) runner_sha256: String,
    pub(crate) base_config_sha256: String,
    pub(crate) original_cl_freeze_sha256: String,
    pub(crate) owner_freeze_raw_sha256: String,
    pub(crate) bulk_freeze_raw_sha256: String,
    pub(crate) bulk_freeze_canonical_sha256: String,
    pub(crate) clock_freeze_raw_sha256: String,
    pub(crate) input_ready_path: PathBuf,
    pub(crate) input_ready_sha256: String,
    pub(crate) before_oracle_manifest_path: PathBuf,
    pub(crate) before_oracle_manifest_sha256: String,
    pub(crate) fixture: FixtureBinding,
    pub(crate) object_store_endpoint: String,
    pub(crate) normal: Normal,
    pub(crate) phase_budget_millis: u64,
    pub(crate) native_budget_millis: u64,
    pub(crate) exporter_contract: String,
    pub(crate) native_deadline_monotonic_nanos: u64,
    pub(crate) native_clock_domain: String,
    pub(crate) original_cl_freeze_path: PathBuf,
    pub(crate) owner_freeze_path: PathBuf,
    pub(crate) bulk_freeze_path: PathBuf,
    pub(crate) clock_freeze_path: PathBuf,
    pub(crate) hms_manifest_path: PathBuf,
    pub(crate) external_clock_receipt_path: PathBuf,
    pub(crate) external_clock_receipt_sha256: String,
}

// The caller must supply an independently admitted clean source/build/binary
// binding BEFORE calling the scenario constructor and BEFORE any role launch.
// Source tree/git/binary admission is not manufactured by this runner module.
pub(crate) struct HmsBulkReadonlyNative {
    input: Binding,
    deadline: Instant,
    namespace_targets: Vec<String>,
    admission: crate::exact_native_admission::hms_native_admission::Admission,
}
fn sha(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}
fn hash(value: &str, length: usize) -> bool {
    value.len() == length
        && value
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}
fn remaining(deadline: Instant) -> Result<Duration> {
    let duration = deadline.saturating_duration_since(Instant::now());
    ensure!(
        !duration.is_zero(),
        "HMS original absolute Native deadline expired"
    );
    Ok(duration)
}
fn validate_ready(input: &Binding, raw: &[u8], manifest: &[u8]) -> Result<()> {
    let ready: Ready = serde_json::from_slice(raw)?;
    ready.normal.validate()?;
    ensure!(
        ready.schema_version == 1
            && ready.scope == "external-stock-input-only"
            && ready.tables == ROWS
            && ready.views == ROWS
            && !ready.native_acceptance
            && ready.normal == input.normal
            && ready.binding == input.fixture
            && ready.bulk_freeze_sha256 == input.bulk_freeze_canonical_sha256
            && sha(raw) == input.input_ready_sha256
            && sha(manifest) == input.before_oracle_manifest_sha256
            && ready.oracle_manifest_sha256 == input.before_oracle_manifest_sha256,
        "HMS INPUT_READY independent binding differs"
    );
    validate_index(manifest)
}
fn validate_index(manifest: &[u8]) -> Result<()> {
    // The external owner supplies the full metadata oracle. Validate its exact
    // index here; no Java, metadata load, provider retry or preparation occurs.
    #[derive(Deserialize)]
    #[serde(deny_unknown_fields)]
    struct Index {
        epoch: String,
        shards: Vec<Shard>,
        tables: u64,
        views: u64,
        actual_metadata_loads: u64,
    }
    #[derive(Deserialize)]
    #[serde(deny_unknown_fields)]
    struct Shard {
        path: String,
        sha256: String,
        namespace: u64,
        shard: u64,
    }
    let index: Index = serde_json::from_slice(manifest)?;
    ensure!(
        index.epoch == "before"
            && index.shards.len() == 128
            && index.tables == ROWS
            && index.views == ROWS
            && index.actual_metadata_loads == 32_768,
        "HMS independent metadata index differs"
    );
    let mut paths = std::collections::BTreeSet::new();
    for (ordinal, shard) in index.shards.iter().enumerate() {
        ensure!(
            shard.namespace == ordinal as u64 / 4
                && shard.shard == ordinal as u64 % 4
                && hash(&shard.sha256, 64)
                && !shard.path.is_empty()
                && shard.path.len() <= 4096
                && paths.insert(&shard.path),
            "HMS oracle shard identity differs"
        );
    }
    Ok(())
}
impl HmsBulkReadonlyNative {
    pub(crate) fn admit(path: &Path, server: &Path, base: &Path) -> Result<Self> {
        let admitted =
            crate::exact_native_admission::hms_native_admission::admit(path, server, base)?;
        let (input, ready, manifest, root_oracle, admission) = admitted;
        let deadline = admission.deadline();
        input.normal.validate()?;
        ensure!(
            !input.fixture.catalog_id.is_empty()
                && input.fixture.catalog_id.len() <= 128
                && input
                    .fixture
                    .catalog_id
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
                && hash(&input.fixture.manifest_sha256, 64)
                && hash(&input.fixture.hms_manifest_sha256, 64)
                && input.fixture.hms_container_id.len() == 64
                && hash(&input.fixture.hms_container_id, 64)
                && input
                    .fixture
                    .hms_image_id
                    .strip_prefix("sha256:")
                    .is_some_and(|v| hash(v, 64))
                && input.fixture.publication.len() <= 4096
                && Path::new(&input.fixture.publication).is_absolute()
                && input.fixture.owner_locator.control_root.len() <= 4096
                && Path::new(&input.fixture.owner_locator.control_root).is_absolute()
                && !input.fixture.owner_locator.daemon_id.is_empty(),
            "HMS original private fixture binding is malformed"
        );
        ensure!(
            input.schema_version == 1 && input.runnable && input.frozen_before_execution,
            "HMS Native execution binding is not admitted"
        );
        ensure!(
            hash(&input.clean_source_revision, 40)
                && input.clean_source_revision == novarocks_version::build_git_commit()
                && input.clean_source_revision == novarocks_version::native_build_identity(),
            "HMS actual runner build differs from admitted clean source"
        );
        for value in [
            &input.source_tree_sha256,
            &input.cargo_lock_sha256,
            &input.server_sha256,
            &input.runner_sha256,
            &input.base_config_sha256,
            &input.original_cl_freeze_sha256,
            &input.owner_freeze_raw_sha256,
            &input.bulk_freeze_raw_sha256,
            &input.bulk_freeze_canonical_sha256,
            &input.clock_freeze_raw_sha256,
            &input.input_ready_sha256,
            &input.before_oracle_manifest_sha256,
        ] {
            ensure!(hash(value, 64), "HMS Native provenance hash is malformed");
        }
        ensure!(
            input.original_cl_freeze_sha256
                == "e279724dc4ab2ce34dfdef5f3a939ad3f0f05ed076c36c5c60a7b7d7c6c1a3d3",
            "HMS immutable approved execution input differs"
        );
        ensure!(
            input.phase_budget_millis == PHASE_MILLIS
                && input.native_budget_millis > 0
                && input.native_budget_millis <= 3_600_000
                && input.exporter_contract == "same-generation-hms-listing-v1",
            "HMS Native clock/exporter contract differs"
        );
        let hms_port = input
            .fixture
            .hms_uri
            .strip_prefix("thrift://127.0.0.1:")
            .and_then(|p| p.parse::<u16>().ok());
        let object_port = input
            .object_store_endpoint
            .strip_prefix("http://127.0.0.1:")
            .and_then(|p| p.parse::<u16>().ok());
        ensure!(
            hms_port.is_some_and(|p| p != 0)
                && object_port.is_some_and(|p| p != 0)
                && input.fixture.warehouse
                    == format!("s3://warehouse/{}/rest/hms", input.fixture.catalog_id),
            "HMS exact private fixture endpoint/warehouse differs"
        );
        validate_ready(&input, &ready, &manifest)?;
        remaining(deadline)?;
        let namespace_targets = namespace_targets(&root_oracle)?;
        Ok(Self {
            input,
            deadline,
            namespace_targets,
            admission,
        })
    }
    pub(crate) fn recheck_admission(&self) -> Result<()> {
        self.admission.recheck(self.deadline)
    }

    fn phase(&self) -> Result<Instant> {
        remaining(self.deadline)?;
        Ok(self
            .deadline
            .min(Instant::now() + Duration::from_millis(PHASE_MILLIS)))
    }
}

#[derive(Deserialize, Serialize, Debug)]
#[serde(deny_unknown_fields)]
struct Journal {
    schema_version: u32,
    process_id: u32,
    catalog_name: String,
    catalog_version: String,
    incarnation: String,
    domain: String,
    phase: u64,
    sequence: u64,
    invocations_in_flight: u64,
    admitted_wrappers_live: u64,
    peak_admitted_wrappers_live: u64,
    available_positions_sample: Option<u64>,
    sdk_objects_live: u64,
    peak_sdk_objects_live: u64,
    used: usize,
    records: Vec<Record>,
}
#[derive(Deserialize, Serialize, Debug)]
#[serde(deny_unknown_fields)]
struct Record {
    ordinal: u64,
    operation: Operation,
    target_sha256: Option<String>,
    original_deadline_remaining_nanos: u64,
    original_deadline_elapsed: bool,
    started: u64,
    acquired: u64,
    sdk_created: u64,
    sdk_first_poll: u64,
    sdk_ready: u64,
    sdk_dropped: u64,
    wrapper_dropped: u64,
    permit_returned: u64,
    settled: u64,
    selection: Selection,
    stop_at_selection: bool,
    deadline_at_selection: bool,
}
#[derive(Deserialize, Serialize, Debug, PartialEq, Eq)]
enum Operation {
    Namespaces,
    Tables,
    Views,
}
#[derive(Deserialize, Serialize, Debug, PartialEq, Eq)]
enum Selection {
    OwnerDropped,
    InitialCheck,
    StopWaiting,
    DeadlineWaiting,
    AdmissionClosed,
    StopAdmitted,
    DeadlineAdmitted,
    ReadyOk,
    ReadyErr,
}
fn journal_idle(value: &Journal, frontend: u32) -> Result<()> {
    ensure!(
        value.schema_version == 1
            && value.process_id == frontend
            && value.catalog_name == CATALOG
            && hash(&value.catalog_version, 64)
            && uuid(&value.domain)
            && uuid(&value.incarnation)
            && value.used == value.records.len()
            && value.used <= 1024
            && value.invocations_in_flight == 0
            && value.admitted_wrappers_live == 0
            && value.sdk_objects_live == 0
            && value.available_positions_sample == Some(8)
            && value.peak_admitted_wrappers_live <= 8
            && value.peak_sdk_objects_live <= 8,
        "HMS same-generation idle journal differs"
    );
    let mut last = 0;
    for record in &value.records {
        ensure!(
            record.ordinal > last
                && record.started > 0
                && record.settled > record.started
                && record.target_sha256.as_ref().is_none_or(|v| hash(v, 64)),
            "HMS journal record identity differs"
        );
        if record.sdk_created != 0 {
            ensure!(
                record.acquired > record.started
                    && record.sdk_created > record.acquired
                    && record.sdk_first_poll > record.sdk_created
                    && record.sdk_dropped > record.sdk_first_poll
                    && record.wrapper_dropped > record.sdk_dropped
                    && record.permit_returned > record.wrapper_dropped
                    && record.settled > record.permit_returned
                    && record.settled <= value.sequence,
                "HMS original SDK destructor did not precede original position return"
            );
        }
        last = record.ordinal;
    }
    Ok(())
}
#[path = "hms_admission_completion.rs"]
mod admission_completion;
#[cfg(test)]
use admission_completion::record_and_validate_catalog_admission;
use admission_completion::wait_original_admission;

fn uuid(value: &str) -> bool {
    value.len() == 36
        && value.bytes().enumerate().all(|(i, b)| {
            if [8, 13, 18, 23].contains(&i) {
                b == b'-'
            } else {
                b.is_ascii_digit() || (b'a'..=b'f').contains(&b)
            }
        })
        && value != "00000000-0000-0000-0000-000000000000"
}
fn same_domain(before: &Journal, after: &Journal) -> Result<()> {
    ensure!(
        before.process_id == after.process_id
            && before.catalog_name == after.catalog_name
            && before.catalog_version == after.catalog_version
            && before.incarnation == after.incarnation
            && before.domain == after.domain
            && before.phase == after.phase
            && after.sequence >= before.sequence,
        "HMS journal generation/domain changed"
    );
    Ok(())
}
fn namespace_targets(raw: &[u8]) -> Result<Vec<String>> {
    let receipt: Value = serde_json::from_slice(raw)?;
    let record = &receipt["records"][0];
    ensure!(
        record["kind"].as_str() == Some("namespaces"),
        "HMS independent root oracle has no namespace record"
    );
    let names = record["value"]
        .as_array()
        .ok_or_else(|| anyhow::anyhow!("HMS independent root namespaces are not an array"))?;
    ensure!(
        names.len() <= 128,
        "HMS independent root namespace bound differs"
    );
    let mut visible = std::collections::BTreeSet::new();
    for value in names {
        let name = value
            .as_str()
            .ok_or_else(|| anyhow::anyhow!("HMS root namespace type differs"))?;
        ensure!(name.len() <= 4096, "HMS root namespace byte bound differs");
        if name.starts_with('.') {
            continue;
        }
        let normalized = novarocks_types::naming::normalize_identifier(name)
            .map_err(|_| anyhow::anyhow!("HMS independent root namespace is unsupported"))?;
        ensure!(
            visible.insert(normalized),
            "HMS independent root namespaces duplicate normalized identity"
        );
    }
    for ordinal in 0..32 {
        ensure!(
            visible.contains(&format!("cl_ns_{ordinal:04}")),
            "HMS independent root oracle omits a prepared namespace"
        );
    }
    ensure!(
        16 * (1 + visible.len()) <= 1024,
        "HMS original journal cannot cover the full normal matrix phase"
    );
    Ok(visible.iter().map(|name| sha(name.as_bytes())).collect())
}
fn normal_coverage(journal: &Journal, targets: &[String], clients: usize) -> Result<()> {
    ensure!(
        CLIENTS.contains(&clients),
        "HMS normal coverage client count differs"
    );
    let mut namespaces = 0;
    let mut tables = std::collections::BTreeMap::<&str, usize>::new();
    for record in &journal.records {
        ensure!(
            record.selection == Selection::ReadyOk
                && record.sdk_created > 0
                && record.sdk_ready > record.sdk_first_poll
                && record.sdk_dropped > record.sdk_ready
                && !record.stop_at_selection
                && !record.deadline_at_selection,
            "HMS normal phase has incomplete original SDK calls"
        );
        match (&record.operation, record.target_sha256.as_deref()) {
            (Operation::Namespaces, None) => namespaces += 1,
            (Operation::Tables, Some(target)) => *tables.entry(target).or_default() += 1,
            _ => bail!("HMS normal phase has a foreign operation/target"),
        }
    }
    ensure!(
        namespaces == clients
            && tables.len() == targets.len()
            && targets
                .iter()
                .all(|target| tables.get(target.as_str()) == Some(&clients)),
        "HMS original namespace/table operation multiset is incomplete or duplicated"
    );
    Ok(())
}
fn view_coverage(journal: &Journal) -> Result<()> {
    let mut targets = std::collections::BTreeSet::new();
    ensure!(
        journal.records.len() == 32,
        "HMS view refusal operation count differs"
    );
    for record in &journal.records {
        ensure!(
            record.operation == Operation::Views
                && record.selection == Selection::ReadyErr
                && record.sdk_created > 0
                && record.sdk_ready > record.sdk_first_poll
                && record.sdk_dropped > record.sdk_ready
                && !record.stop_at_selection
                && !record.deadline_at_selection,
            "HMS view refusal lacks original unsupported SDK-future exit facts"
        );
        ensure!(
            targets.insert(
                record
                    .target_sha256
                    .as_deref()
                    .ok_or_else(|| anyhow::anyhow!("HMS view refusal target is absent"))?
            ),
            "HMS view refusal duplicated a namespace target"
        );
    }
    ensure!(
        (0..32)
            .all(|namespace| targets
                .contains(sha(format!("cl_ns_{namespace:04}").as_bytes()).as_str())),
        "HMS view refusal omitted or replaced a namespace target"
    );
    Ok(())
}
fn http_bytes(port: u16, route: &str, body: Option<Value>, deadline: Instant) -> Result<Vec<u8>> {
    let client = reqwest::blocking::Client::builder()
        .no_proxy()
        .timeout(remaining(deadline)?.min(Duration::from_secs(1)))
        .build()?;
    let url = format!("http://127.0.0.1:{port}{route}");
    let response = if let Some(body) = body {
        client.post(url).json(&body).send()?
    } else {
        client.get(url).send()?
    };
    let mut response = response.error_for_status()?;
    let mut raw = Vec::new();
    response
        .by_ref()
        .take(METRICS_CAP as u64 + 1)
        .read_to_end(&mut raw)?;
    ensure!(
        raw.len() <= METRICS_CAP,
        "HMS observation body exceeds fixed bound"
    );
    remaining(deadline)?;
    Ok(raw)
}
fn snapshot(port: u16, deadline: Instant) -> Result<Journal> {
    serde_json::from_slice(&http_bytes(
        port,
        "/debug/hms-listing-observation",
        Some(json!({"operation":"snapshot"})),
        deadline,
    )?)
    .map_err(Into::into)
}
fn reset(port: u16, old: &Journal, deadline: Instant) -> Result<()> {
    #[derive(Deserialize)]
    #[serde(deny_unknown_fields)]
    struct Ack {
        schema_version: u32,
        domain: String,
        phase: u64,
        process_id: u32,
    }
    let raw = http_bytes(
        port,
        "/debug/hms-listing-observation",
        Some(json!({
        "operation":"reset", "domain":old.domain,"phase":old.phase,"sequence":old.sequence})),
        deadline,
    )?;
    let ack: Ack = serde_json::from_slice(&raw)?;
    ensure!(
        ack.schema_version == 1
            && ack.domain == old.domain
            && ack.process_id == old.process_id
            && Some(ack.phase) == old.phase.checked_add(1),
        "HMS exact idle reset response differs"
    );
    Ok(())
}
#[derive(Clone, Copy, Debug, Serialize)]
struct Allocator {
    allocated: u64,
    active: u64,
    resident: u64,
}
fn allocator(port: u16, deadline: Instant) -> Result<Allocator> {
    let raw = http_bytes(port, "/metrics", None, deadline)?;
    let text = std::str::from_utf8(&raw)?;
    let get = |statistic: &str| -> Result<u64> {
        let prefix = format!(
            "novarocks_frontend_process_allocator_memory_bytes{{statistic=\"{statistic}\"}} "
        );
        let mut values = text.lines().filter_map(|line| line.strip_prefix(&prefix));
        let value = values
            .next()
            .ok_or_else(|| anyhow::anyhow!("FE allocator statistic missing"))?
            .parse()?;
        ensure!(values.next().is_none(), "FE allocator statistic duplicated");
        Ok(value)
    };
    Ok(Allocator {
        allocated: get("allocated")?,
        active: get("active")?,
        resident: get("resident")?,
    })
}
// All actual error/panic objects remain owned. No arbitrary Display/Debug is
// invoked for receipt/log presentation; the outer runner owns role cleanup.
struct Failure {
    first: anyhow::Error,
    rest: Vec<anyhow::Error>,
}
impl fmt::Debug for Failure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("HmsBulkOriginalFailures")
    }
}
impl fmt::Display for Failure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("HMS bulk original failure sources retained")
    }
}
// Raw causes are retained in fixed, bounded slots and deliberately not exposed
// to generic anyhow chain formatting. In-crate tests can inspect the objects.
impl std::error::Error for Failure {}
struct Panic(std::sync::Mutex<Box<dyn std::any::Any + Send>>);
impl fmt::Debug for Panic {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("OriginalHmsClientPanic")
    }
}
impl fmt::Display for Panic {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("original HMS Native component panicked")
    }
}
impl std::error::Error for Panic {}
fn errors(mut failures: Vec<anyhow::Error>) -> Result<()> {
    ensure!(
        failures.len() <= 20,
        "HMS fixed failure slot count exceeded"
    );
    if failures.is_empty() {
        return Ok(());
    }
    let first = failures.remove(0);
    Err(anyhow::Error::new(Failure {
        first,
        rest: failures,
    }))
}
// Stages are private diagnostic facts, never new execution rights or clocks.
#[derive(Clone, Copy)]
enum InitialStage {
    PhaseClock,
    AllocatorBefore,
    SamplerSpawn,
    SamplerMetrics,
    MeasuredOperation,
    ControlRuntime,
    ControlConnect,
    CatalogCreate,
    CatalogUse,
    RegistrationComplete,
    SamplerJoin,
    AllocatorAfter,
    CatalogSnapshot,
    CatalogJournalValidation,
    CatalogAdmissionCompletion,
}
impl InitialStage {
    fn label(self) -> &'static str {
        match self {
            Self::PhaseClock => "phase-clock",
            Self::AllocatorBefore => "allocator-before",
            Self::SamplerSpawn => "sampler-spawn",
            Self::SamplerMetrics => "sampler-metrics",
            Self::MeasuredOperation => "measured-operation",
            Self::ControlRuntime => "control-runtime",
            Self::ControlConnect => "control-connect",
            Self::CatalogCreate => "catalog-create",
            Self::CatalogUse => "catalog-use",
            Self::RegistrationComplete => "registration-complete-clock",
            Self::SamplerJoin => "sampler-join",
            Self::AllocatorAfter => "allocator-after",
            Self::CatalogSnapshot => "catalog-snapshot",
            Self::CatalogJournalValidation => "catalog-journal-validation",
            Self::CatalogAdmissionCompletion => "catalog-admission-completion",
        }
    }
}
struct StageFailure {
    stage: InitialStage,
    cause: anyhow::Error,
}
impl fmt::Debug for StageFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("HmsStageFailure")
            .field("stage", &self.stage.label())
            .field("actual_cause_retained", &true)
            .finish()
    }
}
impl fmt::Display for StageFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "HMS original stage failed: {}", self.stage.label())
    }
}
// Preserve opaque sources without invoking their arbitrary chain formatters.
impl std::error::Error for StageFailure {}
fn staged_error(stage: InitialStage, cause: anyhow::Error) -> anyhow::Error {
    anyhow::Error::new(StageFailure { stage, cause })
}
fn at_stage<T>(stage: InitialStage, operation: impl FnOnce() -> Result<T>) -> Result<T> {
    operation().map_err(|cause| staged_error(stage, cause))
}

#[derive(Serialize)]
struct MysqlErrDiagnostic {
    sequence: u8,
    payload_bytes: usize,
    code: u16,
    sqlstate: String,
    message: String,
    message_sha256: String,
    raw_payload_hex: String,
    raw_payload_sha256: String,
}
#[derive(Serialize)]
struct MysqlPartialDiagnostic {
    header: [u8; 4],
    header_received: usize,
    expected_payload_bytes: Option<usize>,
    payload_prefix_hex: String,
    payload_prefix_sha256: String,
    original_deadline_expired: bool,
}
#[derive(Serialize)]
struct HttpFailureDiagnostic {
    is_timeout: bool,
    is_connect: bool,
    status: Option<u16>,
}
#[derive(Serialize)]
struct FailureDiagnostic {
    schema_version: u8,
    scope: &'static str,
    stage: Option<&'static str>,
    class: &'static str,
    secondary_sources_retained: usize,
    mysql_err: Option<MysqlErrDiagnostic>,
    mysql_partial: Option<MysqlPartialDiagnostic>,
    http_failure: Option<HttpFailureDiagnostic>,
}
fn raw_hex(bytes: &[u8]) -> String {
    use std::fmt::Write as _;
    let mut result = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        write!(&mut result, "{byte:02x}").expect("write bounded String");
    }
    result
}
fn failure_diagnostic(error: &anyhow::Error) -> Result<FailureDiagnostic> {
    let mut current = error;
    let mut stage = None;
    let mut secondary = 0;
    // Only inspect our own finite wrappers; never traverse arbitrary sources.
    for _ in 0..20 {
        if let Some(failure) = current.downcast_ref::<Failure>() {
            ensure!(
                failure.rest.len() <= 19,
                "HMS diagnostic source slots differ"
            );
            secondary += failure.rest.len();
            current = &failure.first;
        } else if let Some(failure) = current.downcast_ref::<StageFailure>() {
            stage = Some(failure.stage.label());
            current = &failure.cause;
        } else {
            let mut result = FailureDiagnostic {
                schema_version: 1,
                scope: "private-original-HMS-failure-not-exit-proof",
                stage,
                class: "opaque-original-source-retained",
                secondary_sources_retained: secondary,
                mysql_err: None,
                mysql_partial: None,
                http_failure: None,
            };
            if let Some(reply) = current.downcast_ref::<UnexpectedControlReply>() {
                let raw = &reply.0.original_payload;
                let error = &reply.0.error;
                // Mirror the existing actor's 4096-byte response bound, not a new cap.
                ensure!(
                    raw.len() >= 9 && raw.len() <= 4096,
                    "HMS ERR diagnostic raw bound differs"
                );
                ensure!(
                    error.sequence == 1
                        && error.payload_bytes == raw.len()
                        && raw[0] == 0xff
                        && raw[3] == b'#'
                        && error.code == u16::from_le_bytes([raw[1], raw[2]])
                        && error.sqlstate.as_bytes() == &raw[4..9]
                        && error.message.as_bytes() == &raw[9..]
                        && error.payload_hex == raw_hex(raw),
                    "HMS ERR diagnostic facts differ from original bytes"
                );
                result.class = "complete-bounded-mysql-ERR";
                result.mysql_err = Some(MysqlErrDiagnostic {
                    sequence: error.sequence,
                    payload_bytes: raw.len(),
                    code: error.code,
                    sqlstate: error.sqlstate.clone(),
                    message: error.message.clone(),
                    message_sha256: sha(error.message.as_bytes()),
                    raw_payload_hex: raw_hex(raw),
                    raw_payload_sha256: sha(raw),
                });
            } else if let Some(partial) = current.downcast_ref::<CommandResponseFailure>() {
                ensure!(
                    partial.header_received <= 4 && partial.payload_prefix.len() <= 4096,
                    "HMS partial diagnostic facts exceed original bounds"
                );
                result.class = "partial-original-mysql-response";
                result.mysql_partial = Some(MysqlPartialDiagnostic {
                    header: partial.header,
                    header_received: partial.header_received,
                    expected_payload_bytes: partial.expected_payload_bytes,
                    payload_prefix_hex: raw_hex(&partial.payload_prefix),
                    payload_prefix_sha256: sha(&partial.payload_prefix),
                    original_deadline_expired: partial.original_deadline_expired(),
                });
            } else if current.is::<Panic>() {
                result.class = "original-panic-payload-retained";
            } else if let Some(http) = current.downcast_ref::<reqwest::Error>() {
                result.http_failure = Some(HttpFailureDiagnostic {
                    is_timeout: http.is_timeout(),
                    is_connect: http.is_connect(),
                    status: http.status().map(|status| status.as_u16()),
                });
                result.class = if http.is_timeout() {
                    "http-timeout"
                } else if http.is_connect() {
                    "http-connect"
                } else {
                    "http-original-error-retained"
                };
            }
            return Ok(result);
        }
    }
    bail!("HMS private diagnostic wrapper depth exceeded")
}
pub(super) fn save_failure_diagnostic(root: &Path, error: &anyhow::Error) -> Result<Value> {
    let diagnostic = failure_diagnostic(error)?;
    let raw = serde_json::to_vec(&diagnostic)?;
    ensure!(
        raw.len() <= FILE_CAP,
        "HMS private diagnostic exceeds existing file bound"
    );
    let path = root.join("hms-bulk-original-failure.json");
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        let mut file = fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(path)?;
        file.write_all(&raw)?;
        file.sync_all()?;
    }
    #[cfg(not(unix))]
    bail!("HMS private failure diagnostic requires Unix file permissions");
    Ok(
        json!({"file":"hms-bulk-original-failure.json","bytes":raw.len(),
        "sha256":sha(&raw),"stage":diagnostic.stage,"class":diagnostic.class,
        "raw_message_export":"private0600-only"}),
    )
}
pub(super) fn finish_evidence_errors(
    primary: Option<anyhow::Error>,
    diagnostic_saved: Option<Result<Value>>,
    saved: Result<()>,
) -> Result<()> {
    let mut failures = Vec::with_capacity(3);
    if let Some(error) = primary {
        failures.push(error);
    }
    if let Some(Err(error)) = diagnostic_saved {
        failures.push(error);
    }
    if let Err(error) = saved {
        failures.push(error);
    }
    errors(failures)
}
fn credential_overlay(purpose: &str) -> Result<String> {
    ensure!(
        matches!(purpose, "object-store-metadata" | "object-store-data"),
        "HMS role credential purpose differs"
    );
    Ok(format!(
        r#"[[connector.credentials]]
purpose = "{purpose}"
name = "iceberg-test-data"
generation = "v1"
kind = "s3"
access_key_id = "${{ENV:AWS_S3_ACCESS_KEY_ID}}"
access_key_secret = "${{ENV:AWS_S3_SECRET_ACCESS_KEY}}"
"#
    ))
}
pub(super) fn install_credential_overlays(launch: &mut ScenarioLaunchConfig) -> Result<()> {
    ensure!(
        launch.config_overlay.fe.is_none()
            && launch.config_overlay.be.is_none()
            && launch.config_overlay.be_by_index.is_empty(),
        "HMS credential overlay already present"
    );
    launch.config_overlay.fe = Some(credential_overlay("object-store-metadata")?);
    launch.config_overlay.be = Some(credential_overlay("object-store-data")?);
    Ok(())
}
fn row_oracle() -> (String, u64) {
    let mut digest = Sha256::new();
    let mut total = 0;
    for namespace in 0..32 {
        for table in 0..512 {
            let cells = [
                format!("cl_ns_{namespace:04}"),
                format!("cl_table_{table:06}"),
                "BASE TABLE".into(),
            ];
            let length = cells.iter().map(|cell| cell.len() + 1).sum::<usize>();
            for cell in cells {
                digest.update([cell.len() as u8]);
                digest.update(cell.as_bytes());
            }
            digest.update((length as u64).to_le_bytes());
            total += length as u64;
        }
    }
    (format!("{:x}", digest.finalize()), total)
}
fn validate_rows(value: &TextResultObservation) -> Result<()> {
    let (digest, bytes) = row_oracle();
    ensure!(
        value.error.is_none()
            && value.rows == ROWS
            && value.row_sha256 == digest
            && value.row_payload_bytes == bytes
            && value.columns == 3
            && value.packets == ROWS + 6
            && value
                .schema
                .iter()
                .map(|c| (c.name.as_str(), c.mysql_type))
                .collect::<Vec<_>>()
                == vec![
                    ("table_schema", 253),
                    ("table_name", 253),
                    ("table_type", 253)
                ],
        "HMS base table names/schema/type oracle differs"
    );
    Ok(())
}
fn clients(user: &str, port: u16, count: usize, deadline: Instant) -> Result<Value> {
    ensure!(CLIENTS.contains(&count), "HMS client count differs");
    let gate = (Mutex::new(None::<bool>), Condvar::new());
    std::thread::scope(|scope| {
        let mut handles = Vec::with_capacity(count);
        let mut spawn_failure = None;
        for _ in 0..count {
            let gate = &gate;
            let spawned =
                std::thread::Builder::new().spawn_scoped(scope, move || -> Result<Value> {
                    let mut decision = gate
                        .0
                        .lock()
                        .map_err(|_| anyhow::anyhow!("HMS client start gate poisoned"))?;
                    while decision.is_none() {
                        decision = gate
                            .1
                            .wait(decision)
                            .map_err(|_| anyhow::anyhow!("HMS client start gate poisoned"))?;
                    }
                    let execute = decision.unwrap();
                    drop(decision);
                    ensure!(execute, "HMS original client spawn cohort incomplete");
                    let runtime = tokio::runtime::Builder::new_current_thread()
                        .enable_all()
                        .build()?;
                    runtime.block_on(async {
                        let mut connection = tokio::time::timeout_at(
                            deadline.into(),
                            AsyncMysqlStream::connect(user, port, remaining(deadline)?),
                        )
                        .await??;
                        let owned = connection
                            .observe_three_column_text_query_owned_until(INFO_SQL, deadline)
                            .await;
                        drop(connection); // Borrowed timeout completes; no detached client/driver.
                        if let Some(error) = owned.actual_failure {
                            return Err(error);
                        }
                        validate_rows(&owned.observation)?;
                        remaining(deadline)?;
                        Ok(serde_json::to_value(owned.observation)?)
                    })
                });
            match spawned {
                Ok(handle) => handles.push(handle),
                Err(error) => {
                    spawn_failure = Some(error);
                    break;
                }
            }
        }
        {
            let mut decision = gate.0.lock().unwrap_or_else(|poison| poison.into_inner());
            *decision = Some(spawn_failure.is_none());
            gate.1.notify_all();
        }
        let mut failures = Vec::with_capacity(count + 1);
        if let Some(error) = spawn_failure {
            failures.push(error.into());
        }
        let mut observed = Vec::with_capacity(count);
        for handle in handles {
            match handle.join() {
                Ok(Ok(value)) => observed.push(value),
                Ok(Err(error)) => failures.push(error),
                Err(payload) => {
                    failures.push(anyhow::Error::new(Panic(std::sync::Mutex::new(payload))))
                }
            }
        }
        errors(failures)?;
        ensure!(
            observed.len() == count,
            "HMS original client join count differs"
        );
        Ok(
            json!({"clients":count,"actual_original_threads_joined":count,"observations":observed,
            "verified_output_entries_per_client":ROWS,"verified_output_schema_and_name_bytes_per_client":409600,
            "provider_page_count":"not-applicable-to-stock-HMS-get_all; not-observed-as-REST-pages"}),
        )
    })
}
struct Stop<'a>(&'a AtomicBool);
impl Drop for Stop<'_> {
    fn drop(&mut self) {
        self.0.store(true, Ordering::Release);
    }
}
fn measure(
    port: u16,
    deadline: Instant,
    operation: impl FnOnce() -> Result<Value>,
) -> Result<Value> {
    // Same before/scoped sampler/operation/actual join/after organization as
    // listing::measure, with a bounded body and retained original sources.
    let before = at_stage(InitialStage::AllocatorBefore, || allocator(port, deadline))?;
    let stopped = AtomicBool::new(false);
    std::thread::scope(|scope| {
        let sampler = std::thread::Builder::new()
            .spawn_scoped(scope, || -> Result<(Allocator, u64)> {
                let mut peak = before;
                let mut samples = 0;
                while !stopped.load(Ordering::Acquire) {
                    let reading =
                        at_stage(InitialStage::SamplerMetrics, || allocator(port, deadline))?;
                    peak.allocated = peak.allocated.max(reading.allocated);
                    peak.active = peak.active.max(reading.active);
                    peak.resident = peak.resident.max(reading.resident);
                    samples += 1;
                    std::thread::sleep(Duration::from_millis(100).min(remaining(deadline)?));
                }
                Ok((peak, samples))
            })
            .map_err(|cause| staged_error(InitialStage::SamplerSpawn, cause.into()))?;
        let guard = Stop(&stopped);
        let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(operation));
        drop(guard);
        let observed = sampler.join(); // Always the original sampler; never Drop as join.
        let mut failure = Vec::with_capacity(3);
        let mut result = None;
        let mut readings = None;
        match outcome {
            Ok(Ok(value)) => result = Some(value),
            Ok(Err(error)) => failure.push(error),
            Err(payload) => failure.push(staged_error(
                InitialStage::MeasuredOperation,
                anyhow::Error::new(Panic(std::sync::Mutex::new(payload))),
            )),
        }
        match observed {
            Ok(Ok(value)) => readings = Some(value),
            Ok(Err(error)) => failure.push(staged_error(InitialStage::SamplerJoin, error)),
            Err(payload) => failure.push(staged_error(
                InitialStage::SamplerJoin,
                anyhow::Error::new(Panic(std::sync::Mutex::new(payload))),
            )),
        }
        let after = match at_stage(InitialStage::AllocatorAfter, || allocator(port, deadline)) {
            Ok(value) => Some(value),
            Err(error) => {
                failure.push(error);
                None
            }
        };
        errors(failure)?;
        let (peak, samples) = readings.expect("joined success retained readings");
        Ok(
            json!({"before":before,"sampled_peak":peak,"after":after,"samples":samples,
            "result":result,"sampler_actual_joined":true,"byte_pass_gate":false,
            "measurement_mode":"hms-listing-feature-witness-including-diagnostic-overhead","default_product_cost_gate_passed":false}),
        )
    })
}
fn quote(value: &str) -> String {
    format!("'{}'", value.replace('\\', "\\\\").replace('\'', "''"))
}
/// Borrowed, already validated public owner facts; this creates no provider authority.
pub(super) struct HmsRegistration<'a> {
    pub(super) catalog: &'a str,
    pub(super) namespace: &'a str,
    pub(super) hms_uri: &'a str,
    pub(super) warehouse: &'a str,
    pub(super) object_store_endpoint: &'a str,
}
fn bulk_registration(input: &Binding) -> HmsRegistration<'_> {
    HmsRegistration {
        catalog: CATALOG,
        namespace: "cl_ns_0000",
        hms_uri: &input.fixture.hms_uri,
        warehouse: &input.fixture.warehouse,
        object_store_endpoint: &input.object_store_endpoint,
    }
}
fn catalog_sql(input: &HmsRegistration<'_>) -> String {
    let catalog = input.catalog;
    format!(
        "CREATE EXTERNAL CATALOG {catalog} PROPERTIES (\"type\"=\"iceberg\",\"iceberg.catalog.type\"=\"hive\",\"iceberg.catalog.hive.metastore.uris\"={},\"iceberg.catalog.warehouse\"={},\"aws.s3.endpoint\"={},\"aws.s3.enable_path_style_access\"=\"true\",\"credential.object-store-metadata.consumer-role\"=\"frontend\",\"credential.object-store-metadata.mode\"=\"static\",\"credential.object-store-metadata.name\"=\"iceberg-test-data\",\"credential.object-store-metadata.generation\"=\"v1\",\"credential.object-store-data.consumer-role\"=\"backend\",\"credential.object-store-data.mode\"=\"static\",\"credential.object-store-data.name\"=\"iceberg-test-data\",\"credential.object-store-data.generation\"=\"v1\")",
        quote(input.hms_uri),
        quote(input.warehouse),
        quote(input.object_store_endpoint)
    )
}
fn unsupported(error: BoundedMysqlError) -> Result<Value> {
    ensure!(
        error.code == 1105
            && error.sqlstate == "HY000"
            && error.message.contains("Unsupported:")
            && error.message.contains(VIEW_REFUSAL),
        "HMS SHOW VIEWS returned an unexpected bounded error"
    );
    Ok(
        json!({"code":error.code,"state":error.sqlstate,"reason_sha256":sha(error.message.as_bytes())}),
    )
}
async fn connect_control(user: &str, port: u16, deadline: Instant) -> Result<AsyncMysqlStream> {
    tokio::time::timeout_at(
        deadline.into(),
        AsyncMysqlStream::connect(user, port, remaining(deadline)?),
    )
    .await?
}
async fn require_ok(connection: &mut AsyncMysqlStream, sql: &str, deadline: Instant) -> Result<()> {
    require_ok_reply(connection.command_response_until(sql, deadline).await?)
}
fn require_ok_reply(reply: BoundedCommandResponse) -> Result<()> {
    match reply {
        BoundedCommandResponse::Ok(_) => Ok(()),
        BoundedCommandResponse::Error(error) => {
            Err(anyhow::Error::new(UnexpectedControlReply(error)))
        }
    }
}
struct UnexpectedControlReply(BoundedCommandError);
impl fmt::Debug for UnexpectedControlReply {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("UnexpectedBoundedHmsControlReply")
    }
}
impl fmt::Display for UnexpectedControlReply {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("HMS control returned an unexpected bounded error")
    }
}
impl std::error::Error for UnexpectedControlReply {}
// Keep the original typed ERR object. Successful facts come only from the same
// command_response_until parser, never from an inferred credential diagnosis.
async fn registration_ok(
    connection: &mut AsyncMysqlStream,
    sql: &str,
    deadline: Instant,
) -> Result<Value> {
    registration_ok_reply(connection.command_response_until(sql, deadline).await?)
}
fn registration_ok_reply(reply: BoundedCommandResponse) -> Result<Value> {
    match reply {
        BoundedCommandResponse::Ok(ok) => Ok(json!({
            "verdict":"complete-bounded-mysql-OK","sequence":1,
            "payload_bytes":ok.original_payload.len(),
            "payload_sha256":sha(&ok.original_payload),
            "affected_rows":ok.affected_rows,"last_insert_id":ok.last_insert_id,
            "status_flags":ok.status_flags,"warnings":ok.warnings,
            "info_bytes":ok.info.len(),"info_sha256":sha(ok.info.as_bytes())
        })),
        BoundedCommandResponse::Error(error) => {
            Err(anyhow::Error::new(UnexpectedControlReply(error)))
        }
    }
}
pub(super) fn register(
    input: &HmsRegistration<'_>,
    user: &str,
    port: u16,
    deadline: Instant,
) -> Result<Value> {
    let runtime = at_stage(InitialStage::ControlRuntime, || {
        Ok(tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?)
    })?;
    runtime.block_on(async {
        let stage = InitialStage::ControlConnect;
        let mut connection = connect_control(user, port, deadline)
            .await
            .map_err(|cause| staged_error(stage, cause))?;
        let stage = InitialStage::CatalogCreate;
        let created = registration_ok(&mut connection, &catalog_sql(input), deadline)
            .await
            .map_err(|cause| staged_error(stage, cause))?;
        let stage = InitialStage::CatalogUse;
        let selected = registration_ok(
            &mut connection,
            &format!("USE {}.{}", input.catalog, input.namespace),
            deadline,
        )
        .await
        .map_err(|cause| staged_error(stage, cause))?;
        drop(connection);
        at_stage(InitialStage::RegistrationComplete, || remaining(deadline))?;
        Ok(json!({"schema_version":1,"catalog":input.catalog,
            "namespace":input.namespace,"external_mutation":false,
            "scope":"same-bounded-register-not-provider-or-role-exit-proof",
            "control_connect":"actual-handshake-completed",
            "catalog_create":created,"catalog_use":selected,
            "registration_complete_clock":"original-deadline-on-time"}))
    })
}
fn views(user: &str, port: u16, deadline: Instant) -> Result<Vec<Value>> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    runtime.block_on(async {
        let mut connection = connect_control(user, port, deadline).await?;
        let mut refusals = Vec::with_capacity(32);
        for namespace in 0..32 {
            require_ok(
                &mut connection,
                &format!("USE {CATALOG}.cl_ns_{namespace:04}"),
                deadline,
            )
            .await?;
            match connection
                .command_response_until("SHOW VIEWS", deadline)
                .await?
            {
                BoundedCommandResponse::Error(error) => refusals.push(unsupported(error.error)?),
                BoundedCommandResponse::Ok(_) => bail!("HMS SHOW VIEWS unexpectedly succeeded"),
            }
        }
        drop(connection);
        remaining(deadline)?;
        Ok(refusals)
    })
}
impl Scenario for HmsBulkReadonlyNative {
    fn name(&self) -> &'static str {
        "catalog/mem-1-m07-real-hms-readonly-bulk"
    }
    fn is_explicit_stage(&self) -> bool {
        true
    }
    fn root_observation_deadline(&self) -> Result<Option<Instant>> {
        remaining(self.deadline)?;
        Ok(Some(self.deadline))
    }
    fn freeze_prepared_exact_config(
        &self,
        artifact: &novarocks_cluster_harness::EffectiveLaunchConfigEvidence,
        root: &Path,
        deadline: Instant,
    ) -> Result<()> {
        ensure!(
            deadline == self.deadline,
            "HMS original prelaunch clock changed"
        );
        self.recheck_admission()?;
        fs::write(
            root.join("hms-bulk-original-prepared-config.json"),
            artifact.artifact_bytes(),
        )?;
        fs::write(
            root.join("hms-bulk-original-prepared-config-sha256.txt"),
            artifact.artifact_sha256(),
        )?;
        remaining(deadline)?;
        Ok(())
    }
    fn launch_config(&self, _root: &Path) -> Result<ScenarioLaunchConfig> {
        remaining(self.deadline)?;
        let mut launch = ScenarioLaunchConfig::default();
        install_credential_overlays(&mut launch)?;
        launch
            .child_environment
            .fe
            .insert(PROBE_ENV.into(), CATALOG.into());
        for name in ["AWS_S3_ACCESS_KEY_ID", "AWS_S3_SECRET_ACCESS_KEY"] {
            let value = std::env::var(name)?;
            ensure!(!value.is_empty(), "HMS explicit role credential is empty");
            launch
                .child_environment
                .fe
                .insert(name.into(), value.clone());
            launch.child_environment.be.insert(name.into(), value);
        }
        Ok(launch)
    }
    fn run(&self, context: &mut ScenarioContext) -> Result<()> {
        require_three_backends(context)?;
        let original = context.recheck_live_process_launch_identities()?;
        let roles = serde_json::to_value(&original)?;
        let role_array = roles
            .as_array()
            .ok_or_else(|| anyhow::anyhow!("HMS actual role identities are not flat"))?;
        ensure!(
            role_array.len() == 4
                && role_array
                    .iter()
                    .map(|r| r.get("role").and_then(Value::as_str))
                    .collect::<Vec<_>>()
                    == vec![Some("fe"), Some("be-0"), Some("be-1"), Some("be-2")],
            "HMS original 1FE+3BE role order differs"
        );
        let frontend = original[0].pid;
        ensure!(
            frontend != 0
                && original
                    .iter()
                    .map(|role| role.pid)
                    .collect::<std::collections::BTreeSet<_>>()
                    .len()
                    == 4,
            "HMS original role PID inventory differs"
        );
        let port = context.fe_http_port();
        let user = context.mysql_user().to_owned();
        let mysql_port = context.mysql_port();
        let mut receipts = Vec::with_capacity(6);
        let mut primary = None;
        let execution = (|| -> Result<()> {
            let phase = at_stage(InitialStage::PhaseClock, || self.phase())?;
            // Catalog registration is FE runtime only. It creates no HMS object.
            let registration = measure(port, phase, || {
                register(&bulk_registration(&self.input), &user, mysql_port, phase)
            })?;
            receipts.push(json!({"phase":"catalog-admission","allocator":registration}));
            let (mut journal, admission) =
                wait_original_admission(&mut receipts, port, frontend, phase)?;
            let generation = (
                journal.process_id,
                journal.catalog_name.clone(),
                journal.catalog_version.clone(),
                journal.incarnation.clone(),
                journal.domain.clone(),
            );
            admission_completion::persist_before_reset(
                &receipts,
                &journal,
                &admission,
                frontend,
                phase,
                || {
                    fs::write(
                        context
                            .scenario_root()
                            .join("hms-bulk-catalog-admission.json"),
                        serde_json::to_vec_pretty(&receipts)?,
                    )
                    .map_err(Into::into)
                },
                || reset(port, &journal, phase),
            )?;
            for count in CLIENTS {
                let phase = self.phase()?;
                let before = snapshot(port, phase)?;
                journal_idle(&before, frontend)?;
                ensure!(
                    (
                        before.process_id,
                        &before.catalog_name,
                        &before.catalog_version,
                        &before.incarnation,
                        &before.domain
                    ) == (
                        generation.0,
                        &generation.1,
                        &generation.2,
                        &generation.3,
                        &generation.4
                    ),
                    "HMS original generation changed after exact reset"
                );
                ensure!(
                    before.used == 0,
                    "HMS phase history not reset after publication"
                );
                let result = measure(port, phase, || clients(&user, mysql_port, count, phase));
                // Capture/save final journal even on a client/allocator failure.
                let after = snapshot(port, phase);
                let mut failures = Vec::with_capacity(2);
                let reading = match result {
                    Ok(value) => Some(value),
                    Err(error) => {
                        failures.push(error);
                        None
                    }
                };
                let current = match after {
                    Ok(value) => Some(value),
                    Err(error) => {
                        failures.push(error);
                        None
                    }
                };
                receipts.push(json!({"phase":"information-schema","clients":count,"allocator":reading,"journal":current}));
                errors(failures)?;
                journal = current.expect("snapshot success retained journal");
                journal_idle(&journal, frontend)?;
                same_domain(&before, &journal)?;
                normal_coverage(&journal, &self.namespace_targets, count)?;
                ensure!(
                    context.recheck_live_process_launch_identities()? == original,
                    "HMS original role changed during matrix"
                );
                // Save this exact original history before resetting it.
                fs::write(
                    context
                        .scenario_root()
                        .join(format!("hms-bulk-clients-{count:02}.json")),
                    serde_json::to_vec_pretty(receipts.last().unwrap())?,
                )?;
                remaining(phase)?;
                reset(port, &journal, phase)?;
            }
            let phase = self.phase()?;
            let before = snapshot(port, phase)?;
            journal_idle(&before, frontend)?;
            ensure!(
                (
                    before.process_id,
                    &before.catalog_name,
                    &before.catalog_version,
                    &before.incarnation,
                    &before.domain
                ) == (
                    generation.0,
                    &generation.1,
                    &generation.2,
                    &generation.3,
                    &generation.4
                ),
                "HMS original view-refusal generation changed"
            );
            ensure!(before.used == 0, "HMS view-refusal phase not reset");
            let views = views(&user, mysql_port, phase);
            let captured = snapshot(port, phase);
            let mut failures = Vec::with_capacity(2);
            let refusals = match views {
                Ok(value) => Some(value),
                Err(error) => {
                    failures.push(error);
                    None
                }
            };
            let current = match captured {
                Ok(value) => Some(value),
                Err(error) => {
                    failures.push(error);
                    None
                }
            };
            receipts.push(
                json!({"phase":"show-views-unsupported","refusals":refusals,"journal":current}),
            );
            errors(failures)?;
            journal = current.expect("actual journal captured");
            journal_idle(&journal, frontend)?;
            same_domain(&before, &journal)?;
            view_coverage(&journal)?;
            remaining(self.deadline)?;
            ensure!(
                context.recheck_live_process_launch_identities()? == original,
                "HMS original roles changed before final assertions"
            );
            Ok(())
        })();
        if let Err(error) = execution {
            primary = Some(error);
        }
        // The original primary remains owned until run_one performs role cleanup.
        // A diagnostic save failure is secondary, never a replacement verdict.
        let diagnostic_saved = primary
            .as_ref()
            .map(|error| save_failure_diagnostic(context.scenario_root(), error));
        let diagnostic_receipt = diagnostic_saved
            .as_ref()
            .and_then(|result| result.as_ref().ok());
        let evidence = json!({"schema_version":1,"scope":"stock-hms-readonly-bulk-native-assertions-only",
            "normal":self.input.normal,"input_ready_sha256":self.input.input_ready_sha256,
            "before_oracle_manifest_sha256":self.input.before_oracle_manifest_sha256,
            "original_process_launch_identities":roles,"actual_frontend_pid":frontend,
            "phases":receipts,"failure_diagnostic":diagnostic_receipt,"assertions_passed":primary.is_none(),"role_shutdown":"owned-by-original-runner; not-proven-by-this-receipt",
            "native_mutations_excluded":true,"physical_last_alias_proven":false,"source_binary_admission":"actual-clean-source-binaries-config-feature-before-original-roles"});
        let saved = serde_json::to_vec_pretty(&evidence)
            .map_err(anyhow::Error::from)
            .and_then(|raw| {
                fs::write(
                    context
                        .scenario_root()
                        .join("hms-bulk-readonly-native-assertions.json"),
                    raw,
                )
                .map_err(Into::into)
            });
        finish_evidence_errors(primary, diagnostic_saved, saved)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn error(code: u16, sqlstate: &str, message: &str) -> BoundedMysqlError {
        BoundedMysqlError {
            sequence: 1,
            payload_bytes: 0,
            code,
            sqlstate: sqlstate.into(),
            message: message.into(),
            payload_hex: String::new(),
        }
    }
    fn normal() -> Normal {
        serde_json::from_value(json!({"namespaces":32,"tables_per_namespace":512,"views_per_namespace":512,"page_size":256,"namespace_pattern":"cl_ns_%04d","table_pattern":"cl_table_%06d","view_pattern":"cl_view_%06d","concurrency":[1,8,16]})).unwrap()
    }
    #[test]
    fn exact_geometry_cannot_shrink_or_replace_true_views() {
        let mut value = normal();
        assert!(value.validate().is_ok());
        value.views_per_namespace = 0;
        assert!(value.validate().is_err());
        value = normal();
        value.concurrency = [1, 8, 8];
        assert!(value.validate().is_err());
        value = normal();
        value.page_size = 512;
        assert!(value.validate().is_err());
    }
    #[test]
    fn strict_geometry_rejects_unknown_and_bool_integer() {
        let mut value = serde_json::to_value(normal()).unwrap();
        value["extra"] = json!(0);
        assert!(serde_json::from_value::<Normal>(value).is_err());
        let mut value = serde_json::to_value(normal()).unwrap();
        value["namespaces"] = json!(true);
        assert!(serde_json::from_value::<Normal>(value).is_err());
    }
    #[test]
    fn independent_names_hash_rejects_views_duplicates_and_missing_rows() {
        let (digest, bytes) = row_oracle();
        assert_eq!(
            digest,
            "59f40e575db0ff0d1d60a6355e1e0594db0705af1a298a79b0772c7f99915b35"
        );
        assert_eq!(bytes, 622592);
        let mut observed = TextResultObservation::default();
        observed.rows = ROWS;
        observed.columns = 3;
        observed.row_sha256 = digest;
        observed.row_payload_bytes = bytes;
        observed.packets = ROWS + 6;
        observed.schema = vec![
            ("table_schema", 253),
            ("table_name", 253),
            ("table_type", 253),
        ]
        .into_iter()
        .map(
            |(name, mysql_type)| crate::actors::mysql_stream::TextColumnObservation {
                name: name.into(),
                mysql_type,
            },
        )
        .collect();
        assert!(validate_rows(&observed).is_ok());
        observed.rows -= 1;
        assert!(validate_rows(&observed).is_err());
        observed.rows += 1;
        observed.row_sha256 = sha(b"cl_view_000000");
        assert!(validate_rows(&observed).is_err());
    }
    #[test]
    fn schema_type_and_server_err_cannot_pass_row_hash() {
        let (digest, bytes) = row_oracle();
        let mut value = TextResultObservation::default();
        value.rows = ROWS;
        value.columns = 3;
        value.row_sha256 = digest;
        value.row_payload_bytes = bytes;
        value.packets = ROWS + 6;
        assert!(validate_rows(&value).is_err());
        value.error = Some("actual failure retained".into());
        assert!(validate_rows(&value).is_err());
    }
    #[test]
    fn refused_success_or_unknown_error_is_not_unsupported() {
        assert!(unsupported(error(0, "", "")).is_err());
        assert!(unsupported(error(1105, "HY000", "network unavailable")).is_err());
        assert!(
            unsupported(error(
                1105,
                "HY000",
                &format!("Unsupported: {VIEW_REFUSAL}")
            ))
            .is_ok()
        );
    }
    #[test]
    fn no_fresh_clock_on_expired_entry() {
        assert!(remaining(Instant::now() - Duration::from_secs(1)).is_err());
    }
    #[test]
    fn missing_exporter_body_never_mints_zero_journal() {
        for value in [json!({}), json!({"schema_version":1}), json!(null)] {
            assert!(serde_json::from_value::<Journal>(value).is_err());
        }
    }
    #[test]
    fn identities_are_full_canonical_not_pid_defaults() {
        assert!(uuid("12345678-1234-1234-1234-123456789abc"));
        for value in [
            "0",
            "1234",
            "00000000-0000-0000-0000-000000000000",
            "12345678-1234-1234-1234-123456789ABC",
        ] {
            assert!(!uuid(value));
        }
    }
    fn journal() -> Journal {
        serde_json::from_value(json!({"schema_version":1,
            "process_id":123,"catalog_name":CATALOG,
            "catalog_version":"a".repeat(64),"incarnation":"01890f6e-7a00-7123-8123-456789abcdee",
            "domain":"01890f6e-7a00-7123-8123-456789abcded","phase":1,"sequence":9,
            "invocations_in_flight":0,"admitted_wrappers_live":0,"peak_admitted_wrappers_live":1,
            "available_positions_sample":8,"sdk_objects_live":0,"peak_sdk_objects_live":1,
            "used":1,"records":[{"ordinal":1,"operation":"Tables","target_sha256":"b".repeat(64),
            "original_deadline_remaining_nanos":1,"original_deadline_elapsed":false,
            "started":1,"acquired":2,"sdk_created":3,"sdk_first_poll":4,"sdk_ready":5,
            "sdk_dropped":6,"wrapper_dropped":7,"permit_returned":8,"settled":9,
            "selection":"ReadyOk","stop_at_selection":false,"deadline_at_selection":false}]}))
        .unwrap()
    }
    #[test]
    fn real_sdk_exit_order_not_available_position_alone() {
        let mut value = journal();
        let fe = value.process_id;
        assert!(journal_idle(&value, fe).is_ok());
        value.records[0].sdk_dropped = 8;
        assert!(journal_idle(&value, fe).is_err());
        value = journal();
        value.records[0].settled = 0;
        assert!(journal_idle(&value, fe).is_err());
        value = journal();
        value.sdk_objects_live = 1;
        assert!(journal_idle(&value, fe).is_err());
    }
    #[test]
    fn journal_overflow_foreign_generation_or_incomplete_sample_fails() {
        let mut value = journal();
        let fe = value.process_id;
        value.used = 1025;
        assert!(journal_idle(&value, fe).is_err());
        value = journal();
        value.available_positions_sample = None;
        assert!(journal_idle(&value, fe).is_err());
        value = journal();
        assert!(journal_idle(&value, 456).is_err());
        let before = journal();
        value = journal();
        value.incarnation = "01890f6e-7a00-7123-8123-456789abcdff".into();
        assert!(same_domain(&before, &value).is_err());
    }
    #[test]
    fn original_error_object_retained_without_invoking_its_formatter() {
        struct Actual;
        impl fmt::Debug for Actual {
            fn fmt(&self, _: &mut fmt::Formatter<'_>) -> fmt::Result {
                panic!("must not format source")
            }
        }
        impl fmt::Display for Actual {
            fn fmt(&self, _: &mut fmt::Formatter<'_>) -> fmt::Result {
                panic!("must not format source")
            }
        }
        impl std::error::Error for Actual {}
        let error = errors(vec![
            anyhow::Error::new(Actual),
            anyhow::anyhow!("original cleanup"),
        ])
        .unwrap_err();
        assert_eq!(
            error.to_string(),
            "HMS bulk original failure sources retained"
        );
        let aggregate = error.downcast_ref::<Failure>().unwrap();
        assert!(aggregate.first.downcast_ref::<Actual>().is_some());
        assert_eq!(aggregate.rest.len(), 1);
    }

    fn index() -> Value {
        json!({"epoch":"before","tables":16384,"views":16384,"actual_metadata_loads":32768,
            "shards":(0..128).map(|i|json!({"path":format!("/original-private/before-{i}.json"),
                "sha256":"a".repeat(64),"namespace":i/4,"shard":i%4})).collect::<Vec<_>>()})
    }
    #[test]
    fn exact_independent_oracle_index_rejects_missing_duplicate_and_foreign_scope() {
        let valid = index();
        assert!(validate_index(&serde_json::to_vec(&valid).unwrap()).is_ok());
        let mut changed = index();
        changed["shards"].as_array_mut().unwrap().pop();
        assert!(validate_index(&serde_json::to_vec(&changed).unwrap()).is_err());
        let mut changed = index();
        changed["shards"][1]["path"] = changed["shards"][0]["path"].clone();
        assert!(validate_index(&serde_json::to_vec(&changed).unwrap()).is_err());
        let mut changed = index();
        changed["shards"][1]["namespace"] = json!(1);
        assert!(validate_index(&serde_json::to_vec(&changed).unwrap()).is_err());
        let mut changed = index();
        changed["views"] = json!(0);
        assert!(validate_index(&serde_json::to_vec(&changed).unwrap()).is_err());
    }

    #[test]
    fn independent_root_oracle_includes_baseline_and_rejects_missing_normal_namespace() {
        let mut names = (0..32)
            .map(|n| json!(format!("cl_ns_{n:04}")))
            .collect::<Vec<_>>();
        names.push(json!("default"));
        names.push(json!(".internal"));
        let encode = |names: &[Value]| {
            serde_json::to_vec(&json!({"records":[{"kind":"namespaces","value":names}]})).unwrap()
        };
        let targets = namespace_targets(&encode(&names)).unwrap();
        assert_eq!(targets.len(), 33);
        assert!(targets.contains(&sha(b"default")));
        names.remove(0);
        assert!(namespace_targets(&encode(&names)).is_err());
    }

    #[test]
    fn exact_operation_multiset_rejects_missing_duplicate_and_foreign_target() {
        let valid = journal();
        let target = valid.records[0].target_sha256.clone().unwrap();
        let mut value: Journal =
            serde_json::from_value(serde_json::to_value(&valid).unwrap()).unwrap();
        let mut namespaces: Record =
            serde_json::from_value(serde_json::to_value(&valid.records[0]).unwrap()).unwrap();
        namespaces.operation = Operation::Namespaces;
        namespaces.target_sha256 = None;
        value.records.push(namespaces);
        assert!(normal_coverage(&value, std::slice::from_ref(&target), 1).is_ok());
        value.records[0].target_sha256 = Some("c".repeat(64));
        assert!(normal_coverage(&value, std::slice::from_ref(&target), 1).is_err());
        value.records[0].target_sha256 = Some(target.clone());
        value.records.pop();
        assert!(normal_coverage(&value, std::slice::from_ref(&target), 1).is_err());
        value.records.push(
            serde_json::from_value(serde_json::to_value(&valid.records[0]).unwrap()).unwrap(),
        );
        assert!(normal_coverage(&value, std::slice::from_ref(&target), 1).is_err());
    }

    #[test]
    fn view_refusal_count_cannot_replace_exact_namespace_coverage() {
        let mut value = journal();
        let prototype = serde_json::to_value(&value.records[0]).unwrap();
        value.records = (0..32)
            .map(|namespace| {
                let mut record: Record = serde_json::from_value(prototype.clone()).unwrap();
                record.operation = Operation::Views;
                record.selection = Selection::ReadyErr;
                record.target_sha256 = Some(sha(format!("cl_ns_{namespace:04}").as_bytes()));
                record
            })
            .collect();
        assert!(view_coverage(&value).is_ok());
        value.records[1].target_sha256 = value.records[0].target_sha256.clone();
        assert!(view_coverage(&value).is_err());
    }
}

#[cfg(test)]
#[path = "hms_bulk_readonly_native_diagnostic_tests.rs"]
mod diagnostic_tests;
