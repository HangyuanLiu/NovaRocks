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

//! Protocol-only cost runner. No measured iteration allocates payload storage.
//! Queue and timing costs are identical for baseline and stock variants.
#![recursion_limit = "256"]
use novarocks_memory::{
    ACCOUNT_METADATA_BYTES, AccountHandle, AccountKind, AllocationOrigin, AuthorityConfig,
    ExternalRef, FundingDomain, InteractionSnapshot, MaintenanceReason, MemoryAuthority,
    OWNER_METADATA_BYTES, ScopeLease, TeardownEvidence, TopUpPolicy,
};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{
    hint::black_box,
    path::PathBuf,
    sync::{
        Arc, Barrier,
        atomic::{AtomicBool, Ordering},
        mpsc,
    },
    thread,
    time::{Duration, Instant},
};

const FROZEN_HASH: &str = "cd3f06768bd6b757860b8c17c724a31469eb880ab2674cd608d7b13693a0d676";
const HISTORICAL_SHA: &str = "af9d0591676c75ede5fb53c2d64949e2d80667c5";
type BenchResult<T> = Result<T, String>;

#[derive(Clone, Debug)]
struct Options {
    manifest: PathBuf,
    threads: usize,
    pairs: usize,
    rounds: usize,
    bytes: u64,
    work_us: u64,
    work_iterations: Option<u64>,
    calibrate: bool,
    interval: usize,
    mode: String,
    variant: String,
    quantum: u64,
    accounts: usize,
    depth: usize,
    domains_per_thread: usize,
    case: String,
    service: bool,
    dynamic: bool,
    duration_ms: u64,
    residual_count: usize,
    late_phase: String,
}
impl Options {
    fn parse() -> BenchResult<Self> {
        let mut o = Self {
            manifest: PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                .join("benches/stock_cost_manifest.json"),
            threads: 1,
            pairs: 10000,
            rounds: 1,
            bytes: 4096,
            work_us: 0,
            work_iterations: None,
            calibrate: false,
            interval: 16,
            mode: "same-thread".into(),
            variant: "stock".into(),
            quantum: 262144,
            accounts: 1,
            depth: 2,
            domains_per_thread: 1,
            case: "funded".into(),
            service: false,
            dynamic: false,
            duration_ms: 30000,
            residual_count: 0,
            late_phase: "before".into(),
        };
        let mut args = std::env::args().skip(1);
        while let Some(flag) = args.next() {
            if flag == "--calibrate-supply" {
                o.calibrate = true;
                continue;
            }
            if flag == "--bench" {
                continue;
            }
            if flag == "--help" || flag == "-h" {
                println!(
                    "stock_cost --manifest PATH --threads T --pairs N --rounds N --bytes B --work-us U [--work-iterations N] [--calibrate-supply] --scope-interval N --mode same-thread|round-robin|fan-in --variant baseline|stock --quantum B --accounts N --depth N --domains-per-thread N --case funded|refill|debt|refusal|oscillation|alternating-sponsor [--service dynamic-drain-residual|dynamic] [--duration-ms N --historical-residuals N --late-free-phase before|during|after]"
                );
                std::process::exit(0);
            }
            let value = args
                .next()
                .ok_or_else(|| format!("missing value after {flag}"))?;
            let number = || {
                value
                    .parse::<u64>()
                    .map_err(|_| format!("invalid number for {flag}: {value}"))
            };
            match flag.as_str() {
                "--manifest" => o.manifest = value.into(),
                "--threads" => o.threads = usize::try_from(number()?).map_err(|e| e.to_string())?,
                "--pairs" => o.pairs = usize::try_from(number()?).map_err(|e| e.to_string())?,
                "--rounds" => o.rounds = usize::try_from(number()?).map_err(|e| e.to_string())?,
                "--bytes" => o.bytes = number()?,
                "--work-us" => o.work_us = number()?,
                "--work-iterations" => o.work_iterations = Some(number()?),
                "--scope-interval" => {
                    o.interval = usize::try_from(number()?).map_err(|e| e.to_string())?
                }
                "--mode" => o.mode = value,
                "--variant" => o.variant = value,
                "--quantum" => o.quantum = number()?,
                "--case" => o.case = value,
                "--accounts" => {
                    o.accounts = usize::try_from(number()?).map_err(|e| e.to_string())?
                }
                "--depth" => o.depth = usize::try_from(number()?).map_err(|e| e.to_string())?,
                "--domains-per-thread" => {
                    o.domains_per_thread = usize::try_from(number()?).map_err(|e| e.to_string())?
                }
                "--service" if value == "dynamic-drain-residual" => o.service = true,
                "--service" if value == "dynamic" => o.dynamic = true,
                "--duration-ms" => o.duration_ms = number()?,
                "--historical-residuals" => {
                    o.residual_count = usize::try_from(number()?).map_err(|e| e.to_string())?
                }
                "--late-free-phase" => o.late_phase = value,
                _ => return Err(format!("unsupported argument {flag} {value}")),
            }
        }
        if [
            o.threads,
            o.pairs,
            o.rounds,
            o.interval,
            o.accounts,
            o.depth,
            o.domains_per_thread,
        ]
        .contains(&0)
            || o.bytes == 0
            || o.quantum == 0
        {
            return Err("counts, bytes and quantum must be nonzero".into());
        }
        if !["same-thread", "round-robin", "fan-in"].contains(&o.mode.as_str())
            || !["baseline", "stock"].contains(&o.variant.as_str())
        {
            return Err(
                "unsupported mode or variant; historical Reservation runs externally".into(),
            );
        }
        if o.depth >= 16 {
            return Err("depth including the root must fit maximum tree depth 16".into());
        }
        if ![
            "funded",
            "refill",
            "debt",
            "refusal",
            "oscillation",
            "alternating-sponsor",
        ]
        .contains(&o.case.as_str())
        {
            return Err("unsupported authorization case".into());
        }
        if o.case == "alternating-sponsor" && (o.accounts < 2 || o.domains_per_thread < 2) {
            return Err(
                "alternating-sponsor needs at least two accounts and domains per thread".into(),
            );
        }
        if !["before", "during", "after"].contains(&o.late_phase.as_str())
            || (o.dynamic && o.duration_ms < 20)
        {
            return Err(
                "late phase must be before/during/after; dynamic duration must be at least 20ms"
                    .into(),
            );
        }
        Ok(o)
    }
}
fn validate_manifest(o: &Options) -> BenchResult<Value> {
    let bytes = std::fs::read(&o.manifest).map_err(|e| e.to_string())?;
    let actual = format!("{:x}", Sha256::digest(&bytes));
    if actual != FROZEN_HASH {
        return Err(format!(
            "frozen manifest SHA256 mismatch: expected {FROZEN_HASH}, found {actual}; review inputs before measurement"
        ));
    }
    let m: Value = serde_json::from_slice(&bytes).map_err(|e| e.to_string())?;
    if m["historical_reservation_baseline_sha"] != HISTORICAL_SHA
        || m["protocol"]["max_tree_depth"] != 16
    {
        return Err("manifest contract differs from the reviewed runner".into());
    }
    if !m["protocol"]["quantum_sensitivity_bytes"]
        .as_array()
        .unwrap()
        .contains(&json!(o.quantum))
    {
        return Err("quantum is outside the frozen sensitivity matrix".into());
    }
    Ok(m)
}
fn mul(a: u64, b: u64) -> BenchResult<u64> {
    a.checked_mul(b)
        .ok_or_else(|| "benchmark input overflow".into())
}
fn add(a: u64, b: u64) -> BenchResult<u64> {
    a.checked_add(b)
        .ok_or_else(|| "benchmark input overflow".into())
}
struct Assembly {
    authority: Arc<MemoryAuthority>,
    accounts: Vec<AccountHandle>,
    leaves: Vec<AccountHandle>,
    domains: Vec<Vec<FundingDomain>>,
    storage: u64,
    floor: u64,
    workset: u64,
    target: u64,
    historical_origins: Vec<AllocationOrigin>,
    historical_backing: u64,
}
fn assemble(o: &Options, m: &Value) -> BenchResult<Assembly> {
    let workset = mul(o.bytes, 4)?.max(mul(o.quantum, 3)?);
    let domain_count = mul(o.threads as u64, o.domains_per_thread as u64)?;
    if domain_count > m["protocol"]["max_active_owners"].as_u64().unwrap() {
        return Err("cell exceeds frozen active owner limit".into());
    }
    let floor = mul(o.quantum, 4)?;
    // Only max_accounts is sized to the known topology. Record bytes retain
    // the exact frozen 64 MiB budget; no cost result selects this sizing.
    let accounts_needed = o
        .accounts
        .checked_mul(o.depth)
        .and_then(|n| n.checked_add(2))
        .ok_or("account count overflow")?;
    let max_accounts = u32::try_from(accounts_needed).map_err(|e| e.to_string())?;
    let config_for = |capacity| {
        let mut c = AuthorityConfig::new(capacity, capacity, 0);
        c.max_accounts = max_accounts;
        c.max_active_owners = m["protocol"]["max_active_owners"].as_u64().unwrap() as u32;
        c.metadata_budget_bytes = m["protocol"]["metadata_budget_bytes"].as_u64().unwrap();
        c.top_up = TopUpPolicy::uniform(o.quantum);
        c
    };
    // This reads configuration-derived prepaid storage, not a timing or a
    // candidate performance value. Drop the sizing authority before assembly.
    let sizing = MemoryAuthority::new(config_for(u64::MAX)).map_err(|e| e.to_string())?;
    let root_storage = sizing.root().committed_bytes();
    let storage = add(
        root_storage,
        mul((accounts_needed - 1) as u64, ACCOUNT_METADATA_BYTES)?,
    )?;
    drop(sizing);
    let historical_backing = mul(o.residual_count as u64, add(o.bytes, OWNER_METADATA_BYTES)?)?;
    let target = add(
        add(add(storage, floor)?, historical_backing)?,
        mul(mul(domain_count, workset)?, 2)?,
    )?;
    let authority = Arc::new(MemoryAuthority::new(config_for(target)).map_err(|e| e.to_string())?);
    if authority.root().committed_bytes() != root_storage {
        return Err("assembly storage changed between identical configurations".into());
    }
    authority
        .install_control_branch(floor)
        .map_err(|e| e.to_string())?;
    let mut historical_origins = Vec::with_capacity(o.residual_count);
    if o.residual_count != 0 {
        let history = authority
            .create_account(AccountKind::Work, ExternalRef::from_u128(u128::MAX))
            .map_err(|e| e.to_string())?;
        // One activity slot is reused while old records retain real bytes.
        // Batch account retirement prevents quadratic repeated full-tree scans.
        for _ in 0..o.residual_count {
            let lane = history.create_domain(o.bytes).map_err(|e| e.to_string())?;
            let mut scope = lane
                .activate(o.bytes, o.quantum)
                .map_err(|e| e.to_string())?;
            historical_origins.push(scope.record_allocation(o.bytes));
            scope.finish().next_step.map_err(|e| e.to_string())?;
            lane.retire_lane()
                .map_err(|e| format!("historical lane retirement: {e:?}"))?;
        }
        history
            .retire(&TeardownEvidence {
                tasks_exited: true,
                operators_destroyed: true,
                io: &[],
                now_ns: 0,
            })
            .map_err(|e| format!("historical account retirement: {e:?}"))?;
    }
    let mut accounts = Vec::new();
    let mut leaves = Vec::new();
    for index in 0..o.accounts {
        let mut leaf = authority
            .create_account(AccountKind::Work, ExternalRef::from_u128(index as u128 + 1))
            .map_err(|e| e.to_string())?;
        accounts.push(leaf.clone());
        for level in 1..o.depth {
            leaf = leaf
                .create_child(
                    AccountKind::Service,
                    ExternalRef::from_u128((index * o.depth + level + 1) as u128),
                )
                .map_err(|e| e.to_string())?;
            accounts.push(leaf.clone());
        }
        leaves.push(leaf);
    }
    if authority.pressure_projection().storage_metadata != storage {
        return Err(
            "configured account-tree metadata does not match actual prepaid storage".into(),
        );
    }
    let mut domains = Vec::with_capacity(o.threads);
    for index in 0..o.threads {
        let mut local = Vec::with_capacity(o.domains_per_thread);
        for lane in 0..o.domains_per_thread {
            let backing = match o.case.as_str() {
                "debt" => o.bytes / 2,
                "refill" => workset - o.quantum,
                "oscillation" => workset - 2 * o.quantum,
                _ => workset,
            };
            let sponsor = if o.case == "alternating-sponsor" {
                (index + lane) % leaves.len()
            } else {
                index % leaves.len()
            };
            local.push(
                leaves[sponsor]
                    .create_domain(backing)
                    .map_err(|e| e.to_string())?,
            );
        }
        domains.push(local);
    }
    Ok(Assembly {
        authority,
        accounts,
        leaves,
        domains,
        storage,
        floor,
        workset,
        target,
        historical_origins,
        historical_backing,
    })
}
fn supply_work(us: u64, tag: u64, iterations: Option<u64>) -> u64 {
    if let Some(iterations) = iterations {
        let mut value = tag.wrapping_mul(0x9e3779b97f4a7c15);
        for _ in 0..iterations {
            value ^= value << 13;
            value ^= value >> 7;
            value ^= value << 17;
            black_box(value);
        }
        black_box(value);
        return tag.wrapping_mul(0xd6e8feb86659fd93);
    }
    let start = Instant::now();
    let duration = Duration::from_micros(us);
    let mut value = tag.wrapping_mul(0x9e3779b97f4a7c15);
    loop {
        value ^= value << 13;
        value ^= value >> 7;
        value ^= value << 17;
        black_box(value);
        if us == 0 || start.elapsed() >= duration {
            break;
        }
    }
    // Scheduling-dependent burn iteration counts must not change the checksum.
    black_box(value);
    tag.wrapping_mul(0xd6e8feb86659fd93)
}
struct Packet {
    origin: Option<AllocationOrigin>,
    bytes: u64,
    checksum: u64,
    ack: mpsc::Sender<(u64, u64)>,
}
fn release(packet: Packet) -> BenchResult<()> {
    if let Some(origin) = packet.origin {
        // SAFETY: exactly one packet owns this successful publication. No
        // payload allocator runs in protocol-only mode; the simulated storage
        // lifetime ends here before its one and only origin release.
        unsafe {
            origin.record_deallocation(packet.bytes);
        }
    }
    packet
        .ack
        .send((packet.bytes, packet.checksum))
        .map_err(|e| e.to_string())
}
struct WorkerResult {
    samples: Vec<u64>,
    checksum: u64,
    freed: u64,
    scope_count: u64,
    misses: u64,
    refusals: u64,
    refills: u64,
    accepted_debt: u64,
}
fn worker(
    o: &Options,
    index: usize,
    domains: &[FundingDomain],
    workset: u64,
    barrier: &Barrier,
    outbound: Option<mpsc::Sender<Packet>>,
    inbound: Option<mpsc::Receiver<Packet>>,
) -> BenchResult<WorkerResult> {
    let (ack, acknowledgements) = mpsc::channel();
    let mut result = WorkerResult {
        samples: Vec::with_capacity(o.pairs),
        checksum: 0,
        freed: 0,
        scope_count: 0,
        misses: 0,
        refusals: 0,
        refills: 0,
        accepted_debt: 0,
    };
    let mut scope: Option<ScopeLease> = None;
    barrier.wait();
    for pair in 0..o.pairs {
        let began = Instant::now();
        let tag = (index as u64)
            .wrapping_mul(o.pairs as u64)
            .wrapping_add(pair as u64)
            .wrapping_add(1);
        let checksum = supply_work(o.work_us, tag, o.work_iterations);
        let domain = &domains[(pair / o.interval) % domains.len()];
        if o.variant == "stock" && scope.is_none() {
            let refill = match o.case.as_str() {
                "refill" => o.quantum,

                _ => 0,
            };
            if refill != 0 {
                domain.refill(refill).map_err(|e| e.to_string())?;
                result.refills += 1;
            }
            if o.case == "oscillation" {
                let before = domain.snapshot().authorized;
                let wiggle = o.quantum / 16;
                let required = if (pair / o.interval).is_multiple_of(2) {
                    workset - wiggle
                } else {
                    add(workset, wiggle)?
                };
                domain.ensure_workset(required).map_err(|e| e.to_string())?;
                if domain.snapshot().authorized > before {
                    result.refills += 1;
                }
            }
            let backing = if o.case == "debt" {
                o.bytes / 2
            } else {
                workset
            };
            let stock = mul(o.bytes, o.interval as u64)?.min(backing);
            scope = Some(
                domain
                    .activate(stock, o.quantum)
                    .map_err(|e| e.to_string())?,
            );
            result.scope_count += 1;
        }
        if o.case == "refusal" && o.variant == "stock" {
            // This request exceeds the configured process bound independently
            // of current live facts. It must not create partial new rights.
            let before = domain.snapshot().authorized;
            if domain.refill(u64::MAX).is_ok() {
                return Err("impossible refill unexpectedly succeeded".into());
            }
            if domain.snapshot().authorized != before {
                return Err("refusal changed domain authorization".into());
            }
            result.refusals += 1;
        }
        let origin = scope.as_mut().map(|scope| scope.record_allocation(o.bytes));
        if o.case == "debt" && o.variant == "stock" {
            let receipt = domain.settle();
            result.accepted_debt = add(result.accepted_debt, receipt.debt)?;
        }
        let end_scope = (pair + 1) % o.interval == 0 || pair + 1 == o.pairs;
        // Boundary objects escape their scope before true local or remote
        // release. The same ordering applies to the ungoverned comparator.
        if end_scope && let Some(scope) = scope.take() {
            if scope.threshold_triggered() {
                result.misses += 1;
            }
            scope.finish().next_step.map_err(|e| e.to_string())?;
        }
        let packet = Packet {
            origin,
            bytes: o.bytes,
            checksum,
            ack: ack.clone(),
        };
        match o.mode.as_str() {
            "same-thread" => release(packet)?,
            "round-robin" => {
                outbound
                    .as_ref()
                    .unwrap()
                    .send(packet)
                    .map_err(|e| e.to_string())?;
                release(
                    inbound
                        .as_ref()
                        .unwrap()
                        .recv_timeout(Duration::from_secs(30))
                        .map_err(|e| e.to_string())?,
                )?;
            }
            "fan-in" => outbound
                .as_ref()
                .unwrap()
                .send(packet)
                .map_err(|e| e.to_string())?,
            _ => unreachable!(),
        }
        let (freed, received_checksum) = acknowledgements
            .recv_timeout(Duration::from_secs(30))
            .map_err(|e| e.to_string())?;
        if freed != o.bytes || received_checksum != checksum {
            return Err("free acknowledgement does not match exact allocation".into());
        }
        result.freed = add(result.freed, freed)?;
        result.checksum = result.checksum.wrapping_add(received_checksum);
        if o.variant == "stock" && o.case == "debt" {
            domain.settle().next_step.map_err(|e| e.to_string())?;
        }
        if o.variant == "stock" && end_scope {
            let amount = match o.case.as_str() {
                "refill" => o.quantum,

                _ => 0,
            };
            if amount != 0
                && domain.trim_idle_to(domain.snapshot().free.saturating_sub(amount)) != amount
            {
                return Err(
                    "domain did not return the exact idle rights after acknowledgement".into(),
                );
            }
        }
        result
            .samples
            .push(u64::try_from(began.elapsed().as_nanos()).map_err(|e| e.to_string())?);
    }
    Ok(result)
}
fn quantile(sorted: &[u64], numerator: usize, denominator: usize) -> u64 {
    let rank = sorted
        .len()
        .saturating_mul(numerator)
        .div_ceil(denominator)
        .max(1);
    sorted[rank.min(sorted.len()) - 1]
}
fn provenance() -> Value {
    let output = |args: &[&str]| {
        std::process::Command::new("git")
            .args(args)
            .current_dir(env!("CARGO_MANIFEST_DIR"))
            .output()
            .ok()
            .filter(|r| r.status.success())
            .map(|r| String::from_utf8_lossy(&r.stdout).trim().to_string())
    };
    json!({"source_sha":output(&["rev-parse","HEAD"]),"dirty":output(&["status","--porcelain"]).map(|s| !s.is_empty()),"os":std::env::consts::OS,"arch":std::env::consts::ARCH,"profile":"release_required_for_formal_collection","manifest_sha256":FROZEN_HASH,"historical_baseline_sha":HISTORICAL_SHA})
}
fn interaction_sum(accounts: &[AccountHandle]) -> InteractionSnapshot {
    let mut sum = InteractionSnapshot::default();
    for account in accounts {
        let reading = account.interaction_snapshot();
        sum.gate_acquisitions += reading.gate_acquisitions;
        sum.gate_wait_ns += reading.gate_wait_ns;
        sum.gate_hold_ns += reading.gate_hold_ns;
        sum.ledger_acquisitions += reading.ledger_acquisitions;
        sum.ledger_wait_ns += reading.ledger_wait_ns;
        sum.ledger_hold_ns += reading.ledger_hold_ns;
        for index in 0..32 {
            sum.wait_histogram[index] += reading.wait_histogram[index];
            sum.hold_histogram[index] += reading.hold_histogram[index];
        }
    }
    sum
}
fn histogram_upper(histogram: &[u64; 32], numerator: u64, denominator: u64) -> Option<u64> {
    let total: u64 = histogram.iter().sum();
    if total == 0 {
        return None;
    }
    let rank = (u128::from(total) * u128::from(numerator)).div_ceil(u128::from(denominator)) as u64;
    let mut covered = 0;
    for (index, count) in histogram.iter().enumerate() {
        covered += count;
        if covered >= rank {
            return if index == 31 {
                None
            } else {
                Some(1u64 << index)
            };
        }
    }
    None
}
fn interaction_delta(before: InteractionSnapshot, after: InteractionSnapshot) -> Value {
    let waits = std::array::from_fn(|i| after.wait_histogram[i] - before.wait_histogram[i]);
    let holds = std::array::from_fn(|i| after.hold_histogram[i] - before.hold_histogram[i]);
    let wait_count: u64 = waits.iter().sum();
    let hold_count: u64 = holds.iter().sum();
    // For a matched acquisition population, union bounding the two marginal
    // 99.5% quantiles gives a conservative upper bound for wait+hold p99.
    // The corresponding 99.95% bounds cover wait+hold p99.9. This does not
    // pretend that two separate histograms are an exact joint raw sample.
    let combined = |num, den| {
        if wait_count == hold_count {
            histogram_upper(&waits, num, den)
                .zip(histogram_upper(&holds, num, den))
                .and_then(|(w, h)| w.checked_add(h))
        } else {
            None
        }
    };
    json!({"aggregation":"actual_counter_delta_sum","gate_acquisitions":after.gate_acquisitions-before.gate_acquisitions,
        "gate_wait_ns":after.gate_wait_ns-before.gate_wait_ns,"gate_hold_ns":after.gate_hold_ns-before.gate_hold_ns,
        "ledger_acquisitions":after.ledger_acquisitions-before.ledger_acquisitions,
        "ledger_wait_ns":after.ledger_wait_ns-before.ledger_wait_ns,"ledger_hold_ns":after.ledger_hold_ns-before.ledger_hold_ns,
        "wait_histogram_raw":waits,"hold_histogram_raw":holds,"wait_samples":wait_count,"hold_samples":hold_count,
        "histogram_format":"bin i<31 upper2^i nanoseconds; bin31 unbounded overflow",
        "wait_p99_upper_ns":histogram_upper(&waits,99,100),"wait_p999_upper_ns":histogram_upper(&waits,999,1000),
        "hold_p99_upper_ns":histogram_upper(&holds,99,100),"hold_p999_upper_ns":histogram_upper(&holds,999,1000),
        "queue_plus_hold_p99_upper_ns":combined(199,200),"queue_plus_hold_p999_upper_ns":combined(1999,2000),
        "queue_plus_hold_max_upper_ns":combined(1,1),"combined_method":"conservative marginal union bound; not exact paired tail",
        "population_counts_aligned":wait_count==hold_count})
}

fn resource_snapshot(a: &Assembly) -> Value {
    let p = a.authority.pressure_projection();
    let domains = a
        .domains
        .iter()
        .flatten()
        .map(FundingDomain::snapshot)
        .collect::<Vec<_>>();
    json!({"sample_point":"outside timed hook path after producers have joined", "root_committed_bytes":p.root_committed,
        "query_committed_bytes":p.query_committed,"residual_committed_bytes":p.residual_committed,
        "residual_payload_bytes":p.residual_committed.saturating_sub(p.residual_metadata),
        "residual_metadata_bytes":p.residual_metadata,"active_metadata_bytes":p.active_metadata,
        "shared_storage_backing_bytes":p.storage_metadata,"active_scope_count":p.active_scopes,
        "dirty_domain_count":p.dirty_domains,"sampled_payload_live_bytes":p.sampled_payload_live,
        "settled_payload_live_bytes":p.settled_payload_live,"sampled_debt_bytes":p.sampled_debt,
        "settled_debt_bytes":p.settled_debt,"classification_complete":p.classification_complete,
        "capacity_target_bytes":p.capacity_target,"elastic_excess_bytes":p.elastic_excess,
        "control_floor_bytes":p.control_floor,"root_revision":p.root_revision,
        "active_domain_count":domains.iter().filter(|d|!d.sealed).count(),
        "lane_free_bytes":domains.iter().map(|d|d.free).sum::<u64>(),
        "lane_authorized_bytes":domains.iter().map(|d|d.authorized).sum::<u64>(),
        "lane_committed_bytes":domains.iter().map(|d|d.committed).sum::<u64>(),
        "idle_stock_bytes":if p.active_scopes==0{Some(0u64)}else{None},
        "stock_sample_limit":"idle stock is zero only with all actual scope leases finished; active stock sampling unavailable"})
}
fn run_round(o: &Options, m: &Value, round: usize) -> BenchResult<Value> {
    let mut a = assemble(o, m)?;
    let parent_before: u64 = a.accounts.iter().map(AccountHandle::interactions).sum();
    let root_before = a.authority.root().interactions();
    let parent_metrics_before = interaction_sum(&a.accounts);
    let root_metrics_before = a.authority.root().interaction_snapshot();
    let (samples, elapsed, checksum, freed, scopes, misses, refusals, refills, debt) =
        thread::scope(|s| -> BenchResult<_> {
            let consumer_count = usize::from(o.mode == "fan-in");
            let barrier = Arc::new(Barrier::new(o.threads + consumer_count + 1));
            let mut senders = Vec::new();
            let mut receivers = Vec::new();
            let queues = if o.mode == "round-robin" {
                o.threads
            } else {
                consumer_count
            };
            for _ in 0..queues {
                let (tx, rx) = mpsc::channel();
                senders.push(tx);
                receivers.push(Some(rx));
            }
            let consumer = if consumer_count == 1 {
                let inbound = receivers[0].take().unwrap();
                let ready = barrier.clone();
                Some(s.spawn(move || -> BenchResult<()> {
                    ready.wait();
                    for _ in 0..o
                        .threads
                        .checked_mul(o.pairs)
                        .ok_or("pair count overflow")?
                    {
                        release(
                            inbound
                                .recv_timeout(Duration::from_secs(30))
                                .map_err(|e| e.to_string())?,
                        )?;
                    }
                    Ok(())
                }))
            } else {
                None
            };
            let mut handles = Vec::new();
            for index in 0..o.threads {
                let tx = match o.mode.as_str() {
                    "round-robin" => Some(senders[(index + 1) % o.threads].clone()),
                    "fan-in" => Some(senders[0].clone()),
                    _ => None,
                };
                let rx = if o.mode == "round-robin" {
                    receivers[index].take()
                } else {
                    None
                };
                let ready = barrier.clone();
                let domains = &a.domains[index];
                let workset = a.workset;
                handles.push(s.spawn(move || worker(o, index, domains, workset, &ready, tx, rx)));
            }
            drop(senders);
            let started = Instant::now();
            barrier.wait();
            let mut samples = Vec::new();
            let mut checksum = 0u64;
            let mut freed = 0;
            let mut scopes = 0;
            let mut misses = 0;
            let mut refusals = 0;
            let mut refills = 0;
            let mut debt = 0;
            for handle in handles {
                let r = handle.join().map_err(|_| "worker panicked")??;
                samples.extend(r.samples);
                checksum = checksum.wrapping_add(r.checksum);
                freed = add(freed, r.freed)?;
                scopes += r.scope_count;
                misses += r.misses;
                refusals += r.refusals;
                refills += r.refills;
                debt = add(debt, r.accepted_debt)?;
            }
            if let Some(handle) = consumer {
                handle.join().map_err(|_| "free consumer panicked")??;
            }
            Ok((
                samples,
                started.elapsed(),
                checksum,
                freed,
                scopes,
                misses,
                refusals,
                refills,
                debt,
            ))
        })?;
    let expected_pairs = o
        .threads
        .checked_mul(o.pairs)
        .ok_or("pair count overflow")?;
    if samples.len() != expected_pairs || freed != mul(expected_pairs as u64, o.bytes)? {
        return Err("pair or exact free-byte conservation mismatch".into());
    }
    let parent_after: u64 = a.accounts.iter().map(AccountHandle::interactions).sum();
    let root_after = a.authority.root().interactions();
    let mut sorted = samples;
    sorted.sort_unstable();
    let idle: u64 = a.domains.iter().flatten().map(|d| d.snapshot().free).sum();
    let raw_samples = std::env::var_os("STOCK_COST_SAVE_SAMPLES")
        .map(|directory| -> BenchResult<String> {
            let directory = PathBuf::from(directory);
            std::fs::create_dir_all(&directory).map_err(|e| e.to_string())?;
            let path = directory.join(format!(
                "{}-{}-{}-t{}-b{}-u{}-i{}-q{}-a{}-d{}-m{}-r{}.json",
                o.case,
                o.variant,
                o.mode,
                o.threads,
                o.bytes,
                o.work_us,
                o.interval,
                o.quantum,
                o.accounts,
                o.depth,
                o.domains_per_thread,
                round
            ));
            std::fs::write(
                &path,
                serde_json::to_vec(&sorted).map_err(|e| e.to_string())?,
            )
            .map_err(|e| e.to_string())?;
            Ok(path.display().to_string())
        })
        .transpose()?;
    let mut record = json!({"kind":"static_authorization_round","case":o.case,"round":round,"variant":o.variant,"mode":o.mode,"threads":o.threads,"pairs_per_thread":o.pairs,"successful_pairs":expected_pairs,"refusals":refusals,"successful_refills":refills,"accepted_debt_bytes_cumulative":debt,"operation_bytes":o.bytes,"work_us":o.work_us,"supply_work_iterations":o.work_iterations,"supply_kind":if o.work_iterations.is_some(){"fixed_calibrated_iterations"}else{"wall_clock_smoke_probe"},"scope_interval":o.interval,"quantum_bytes":o.quantum,"accounts":o.accounts,"tree_depth":o.depth,"domains_per_thread":o.domains_per_thread,"workset_per_domain_bytes":a.workset,"assembly_storage_bytes":a.storage,"control_floor_bytes":a.floor,"high_total_target_bytes":a.target,"elapsed_ns":elapsed.as_nanos(),"pairs_per_second":expected_pairs as f64/elapsed.as_secs_f64(),"iteration_p99_ns":quantile(&sorted,99,100),"iteration_p999_ns":quantile(&sorted,999,1000),"iteration_max_ns":sorted.last(),"iteration_min_ns":sorted.first(),"checksum":checksum,"free_acknowledged_bytes":freed,"actual_protocol_released_bytes":if o.variant=="stock"{freed}else{0},"parent_interactions":parent_after-parent_before,"root_interactions":root_after-root_before,"scope_count":scopes,"threshold_scopes":misses,"idle_workset_bytes":idle,"raw_sorted_samples":raw_samples,"p999_sample_count":expected_pairs,"matrix_complete":false,"formal_acceptance":false,"growth_gate_atomic_cost":null,"control_queue_wait":null,"control_lock_hold":null,"formal_blockers":["historical_reservation_external_results_missing","cpu_affinity_and_once_only_supply_calibration_unverified","hardware_growth_gate_cacheline_atomic_cost_unmeasured","full_frozen_cell_coverage_and_seven_independent_processes_unverified"]});
    record["provenance"] = provenance();
    record["parent_control_metrics"] =
        interaction_delta(parent_metrics_before, interaction_sum(&a.accounts));
    record["root_control_metrics"] = interaction_delta(
        root_metrics_before,
        a.authority.root().interaction_snapshot(),
    );
    record["control_queue_wait"] = json!({"aggregation":"sum","parent":record["parent_control_metrics"]["ledger_wait_ns"],"root":record["root_control_metrics"]["ledger_wait_ns"]});
    record["control_lock_hold"] = json!({"aggregation":"sum","parent":record["parent_control_metrics"]["ledger_hold_ns"],"root":record["root_control_metrics"]["ledger_hold_ns"]});
    record["growth_gate_acquisitions"] = json!({"parent":record["parent_control_metrics"]["gate_acquisitions"],"root":record["root_control_metrics"]["gate_acquisitions"]});
    record["growth_gate_wait"] = json!({"aggregation":"actual sum", "parent_ns":record["parent_control_metrics"]["gate_wait_ns"],"root_ns":record["root_control_metrics"]["gate_wait_ns"]});
    record["post_load_resource_snapshot"] = resource_snapshot(&a);
    record["local_hook_calls"] = json!({"actual_allocation_publications":if o.variant=="stock"{expected_pairs}else{0},"actual_origin_releases":if o.variant=="stock"{expected_pairs}else{0}});
    record["sample_dispersion_ns"] = json!({"p50":quantile(&sorted,50,100),"p90":quantile(&sorted,90,100),"p99":quantile(&sorted,99,100),"p999":quantile(&sorted,999,1000),"max":sorted.last()});
    // Cleanup is outside the throughput/tail interval. All true free acks
    // arrived before retiring each lane and driving independent maintenance.
    for domains in &a.domains {
        for domain in domains {
            domain
                .retire_lane()
                .map_err(|e| format!("lane retirement: {e:?}"))?;
        }
    }
    release_history(std::mem::take(&mut a.historical_origins), o.bytes)?;
    drop(std::mem::take(&mut a.domains));
    let mut batches = 0;
    loop {
        batches += 1;
        if a.authority.maintain(64).complete {
            break;
        }
        if batches > 1_000_000 {
            return Err("maintenance did not complete bounded coverage".into());
        }
    }
    record["cleanup_maintenance_batches"] = json!(batches);
    record["cleanup_root_committed_bytes"] = json!(a.authority.root().committed_bytes());
    Ok(record)
}
fn service(o: &Options, m: &Value) -> BenchResult<Value> {
    if o.variant != "stock" {
        return Err("service probe requires stock variant".into());
    }
    let mut a = assemble(o, m)?;
    let mut writer = a
        .authority
        .take_capacity_writer()
        .map_err(|e| e.to_string())?;
    let origins = thread::scope(|s| -> BenchResult<Vec<AllocationOrigin>> {
        let handles: Vec<_> = a
            .domains
            .iter()
            .map(|domains| {
                s.spawn(move || -> BenchResult<Vec<AllocationOrigin>> {
                    let mut origins = Vec::with_capacity(domains.len());
                    for domain in domains {
                        let mut scope = domain
                            .activate(o.bytes, o.quantum)
                            .map_err(|e| e.to_string())?;
                        origins.push(scope.record_allocation(o.bytes));
                        scope.finish().next_step.map_err(|e| e.to_string())?;
                    }
                    Ok(origins)
                })
            })
            .collect();
        let mut origins = Vec::new();
        for handle in handles {
            origins.extend(handle.join().map_err(|_| "origin thread panicked")??);
        }
        Ok(origins)
    })?;
    let before = a.authority.pressure_projection();
    let evidence = TeardownEvidence {
        tasks_exited: true,
        operators_destroyed: true,
        io: &[],
        now_ns: 0,
    };
    let began = Instant::now();
    let mut payload = 0;
    let mut metadata = 0;
    let mut returned_idle = 0;
    for leaf in &a.leaves {
        let transfer = leaf
            .retire(&evidence)
            .map_err(|e| format!("retire: {e:?}"))?;
        payload += transfer.transferred_payload;
        metadata += transfer.transferred_metadata;
        returned_idle += transfer.returned_idle;
    }
    let handoff_ns = began.elapsed().as_nanos();
    let after = a.authority.pressure_projection();
    let mut transitions = Vec::new();
    for target in [
        a.storage
            + a.floor
            + mul((o.threads * o.domains_per_thread) as u64, a.workset)?.saturating_sub(o.quantum),
        0,
        a.target,
    ] {
        let start = Instant::now();
        let revision = writer.set_capacity(target).map_err(|e| e.to_string())?;
        transitions.push(
            json!({"target":target,"revision":revision,"elapsed_ns":start.elapsed().as_nanos()}),
        );
    }
    // All originating threads/scopes have stopped. Allocation obligations
    // keep records alive through handoff until these exact remote releases.
    let free_start = Instant::now();
    thread::scope(|s| {
        s.spawn(move || {
            for origin in origins {
                // SAFETY: this vector uniquely owns one publication per domain; no
                // origin is used again after its exact release.
                unsafe {
                    origin.record_deallocation(o.bytes);
                }
            }
        })
        .join()
        .map_err(|_| "late free worker panicked")
    })
    .map_err(str::to_string)?;
    let free_ns = free_start.elapsed().as_nanos();
    release_history(std::mem::take(&mut a.historical_origins), o.bytes)?;
    drop(std::mem::take(&mut a.domains));
    let start = Instant::now();
    let mut batches = 0;
    let mut epochs = Vec::new();
    for reason in [
        MaintenanceReason::CapacityReduced,
        MaintenanceReason::ShortageCandidate,
        MaintenanceReason::ExplicitLocalReclaim,
    ] {
        a.authority.request_maintenance(reason);
        loop {
            batches += 1;
            let r = a.authority.maintain(64);
            if r.complete {
                epochs.push(json!({"epoch":r.epoch,"scanned":r.scanned,"upper":r.upper,"deferred_active":r.deferred_active}));
                break;
            }
            if batches > 1_000_000 {
                return Err("service coverage did not complete".into());
            }
        }
    }
    let final_projection = a.authority.pressure_projection();
    Ok(
        json!({"kind":"deterministic_dynamic_drain_residual_service_probe","provenance":provenance(),"matrix_complete":false,"formal_acceptance":false,"real_io_exit_bound_proved":false,"assembly_storage_bytes":a.storage,"control_floor_bytes":a.floor,"transferred_payload_bytes":payload,"transferred_metadata_bytes":metadata,"returned_idle_bytes":returned_idle,"handoff_ns":handoff_ns,"root_before":before.root_committed,"root_after_handoff":after.root_committed,"U_before":before.query_pressure(),"U_after_handoff":after.query_pressure(),"late_free_ns":free_ns,"actual_protocol_released_bytes":mul((o.threads*o.domains_per_thread)as u64,o.bytes)?,"capacity_transitions":transitions,"coverage_epochs":epochs,"maintenance_batches":batches,"maintenance_elapsed_ns":start.elapsed().as_nanos(),"final_root_committed_bytes":final_projection.root_committed,"final_residual_metadata_bytes":final_projection.residual_metadata,"limitations":["service_probe_has_no_30_second_mixed_load_or_10ms_transition_cadence","empty_io_evidence_is_protocol_only","deterministic_service_does_not_collect_per_lock_telemetry","not_a_formal_dynamic_gate_receipt"]}),
    )
}
fn release_history(origins: Vec<AllocationOrigin>, bytes: u64) -> BenchResult<u64> {
    let freed = mul(origins.len() as u64, bytes)?;
    for origin in origins {
        // SAFETY: each historical record has one outstanding publication;
        // this vector is consumed and its origins are never reused.
        unsafe {
            origin.record_deallocation(bytes);
        }
    }
    Ok(freed)
}
struct DynamicWorker {
    samples: Vec<u64>,
    attempts: u64,
    successful: u64,
    refused: u64,
    activation_race_refusals: u64,
    checksum: u64,
    freed: u64,
    max_ns: u64,
}
fn sample_dynamic(worker: &mut DynamicWorker, duration_ns: u64) {
    worker.max_ns = worker.max_ns.max(duration_ns);
    reservoir(&mut worker.samples, worker.attempts, duration_ns);
}
fn reservoir(samples: &mut Vec<u64>, seen: u64, value: u64) {
    const CAPACITY: usize = 100000;
    if samples.len() < CAPACITY {
        samples.push(value);
        return;
    }
    // A deterministic uniform reservoir bounds observer memory independently
    // of workload throughput. Report represented population and sample count.
    let mut random = seen.wrapping_add(0x9e3779b97f4a7c15);
    random = (random ^ (random >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
    random = (random ^ (random >> 27)).wrapping_mul(0x94d049bb133111eb);
    random ^= random >> 31;
    let slot = random % seen;
    if slot < CAPACITY as u64 {
        samples[slot as usize] = value;
    }
}
fn dynamic_round(o: &Options, m: &Value, round: usize) -> BenchResult<Value> {
    if o.variant != "stock" || o.case != "funded" {
        return Err("timed dynamic service uses stock funded protocol; historical dynamic comparison is not equivalent".into());
    }
    let mut a = assemble(o, m)?;
    let high = a.target;
    let low = add(
        add(add(a.storage, a.floor)?, a.historical_backing)?,
        mul((o.threads * o.domains_per_thread) as u64, a.workset)?.saturating_sub(o.quantum),
    )?;
    let mut writer = a
        .authority
        .take_capacity_writer()
        .map_err(|e| e.to_string())?;
    let root_before = a.authority.root().interactions();
    let root_metrics_before = a.authority.root().interaction_snapshot();
    let parent_metrics_before = interaction_sum(&a.accounts);
    let parent_before: u64 = a.accounts.iter().map(AccountHandle::interactions).sum();
    let historical_origins = std::mem::take(&mut a.historical_origins);
    let stop = Arc::new(AtomicBool::new(false));
    let (workers, transitions, maintenance, elapsed) = thread::scope(|s| -> BenchResult<_> {
        let consumers = match o.mode.as_str() {
            "fan-in" => 1,
            "round-robin" => o.threads,
            _ => 0,
        };
        let barrier = Arc::new(Barrier::new(o.threads + consumers + 2));
        let mut senders = Vec::new();
        let mut receiver_handles = Vec::new();
        for _ in 0..consumers {
            let (tx, rx) = mpsc::channel();
            senders.push(tx);
            let ready = barrier.clone();
            receiver_handles.push(s.spawn(move || -> BenchResult<()> {
                ready.wait();
                while let Ok(packet) = rx.recv() {
                    release(packet)?;
                }
                Ok(())
            }));
        }
        let (first_low, begin_maintenance) = mpsc::channel();
        let progress = a.authority.clone();
        let done = stop.clone();
        let ready = barrier.clone();
        let maintenance=s.spawn(move || -> BenchResult<Value> {
            let mut origins=Some(historical_origins);let mut freed=0;
            let mut injection_epoch=None;let mut injection_scanned=None;
            let mut latency=Vec::new();let mut batches=0u64;let mut complete=0u64;let mut deferred=0u64;
            ready.wait();
            // Preserve the initial workset until a real low-target revision
            // has frozen an already committed root. No correctness sleep.
            begin_maintenance.recv().map_err(|e|e.to_string())?;
            if o.late_phase=="before" {freed=release_history(origins.take().unwrap(),o.bytes)?;}
            progress.request_maintenance(MaintenanceReason::CapacityReduced);
            let mut first_batch=true;let mut next=Instant::now();
            while !done.load(Ordering::Acquire) {
                let began=Instant::now();let receipt=progress.maintain(64);
                latency.push(began.elapsed().as_nanos() as u64);batches+=1;deferred+=receipt.deferred_active;
                if receipt.complete {complete+=1;}
                let inject=(o.late_phase=="during" && first_batch) || (o.late_phase=="after" && receipt.complete);
                if inject && origins.is_some() {
                    if o.late_phase=="during" && receipt.complete && o.residual_count>0 {return Err("during-coverage injection needs a coverage set larger than one batch".into());}
                    injection_epoch=Some(receipt.epoch);injection_scanned=Some(receipt.scanned);
                    freed=release_history(origins.take().unwrap(),o.bytes)?;
                }
                first_batch=false;next+=Duration::from_millis(1);
                if let Some(wait)=next.checked_duration_since(Instant::now()){thread::sleep(wait);}
            }
            let scheduled_injection_completed=origins.is_none();
            // Tiny smoke durations may end before an after-coverage event.
            // Complete the actual covered epoch, then inject; disclose this
            // as cleanup rather than claiming it happened during timed load.
            if let Some(origins)=origins {
                loop {let r=progress.maintain(64);if r.complete{injection_epoch=Some(r.epoch);injection_scanned=Some(r.scanned);break;}}
                freed=release_history(origins,o.bytes)?;
            }
            latency.sort_unstable();
            Ok(json!({"batches":batches,"completed_coverage_epochs":complete,"deferred_active_observations":deferred,"historical_actual_free_bytes":freed,"injection_epoch":injection_epoch,"injection_scanned":injection_scanned,"scheduled_injection_completed_during_load":scheduled_injection_completed,"batch_service_p99_ns":if latency.is_empty(){None}else{Some(quantile(&latency,99,100))},"batch_service_p999_ns":if latency.is_empty(){None}else{Some(quantile(&latency,999,1000))},"batch_service_max_ns":latency.last()}))
        });
        let mut worker_handles = Vec::new();
        for index in 0..o.threads {
            let ready = barrier.clone();
            let done = stop.clone();
            let domains = &a.domains[index];
            let outbound = match o.mode.as_str() {
                "fan-in" => Some(senders[0].clone()),
                "round-robin" => Some(senders[(index + 1) % consumers].clone()),
                _ => None,
            };
            let workset = a.workset;
            worker_handles.push(s.spawn(move || -> BenchResult<DynamicWorker> {
                let (ack, acknowledgements) = mpsc::channel();
                let mut scope: Option<ScopeLease> = None;
                let mut result = DynamicWorker {
                    samples: Vec::with_capacity(100000),
                    attempts: 0,
                    successful: 0,
                    refused: 0,
                    activation_race_refusals: 0,
                    checksum: 0,
                    freed: 0,
                    max_ns: 0,
                };
                ready.wait();
                while !done.load(Ordering::Acquire) {
                    let start = Instant::now();
                    result.attempts += 1;
                    let checksum = supply_work(
                        o.work_us,
                        (index as u64)
                            .wrapping_mul(0x100000000)
                            .wrapping_add(result.attempts),
                        o.work_iterations,
                    );
                    let domain =
                        &domains[(result.successful as usize / o.interval) % domains.len()];
                    if scope.is_none() {
                        let stock = mul(o.bytes, o.interval as u64)?.min(workset);
                        if domain.ensure_workset(stock).is_err() {
                            result.refused += 1;
                            sample_dynamic(&mut result, start.elapsed().as_nanos() as u64);
                            continue;
                        }
                        match domain.activate(stock, o.quantum) {
                            Ok(lease) => scope = Some(lease),
                            Err(_) => {
                                result.activation_race_refusals += 1;
                                result.refused += 1;
                                sample_dynamic(&mut result, start.elapsed().as_nanos() as u64);
                                continue;
                            }
                        }
                    }
                    let origin = scope.as_mut().unwrap().record_allocation(o.bytes);
                    let boundary = (result.successful + 1).is_multiple_of(o.interval as u64);
                    if boundary {
                        let receipt = scope.take().unwrap().finish();
                        let _ = black_box(receipt.next_step);
                    }
                    let packet = Packet {
                        origin: Some(origin),
                        bytes: o.bytes,
                        checksum,
                        ack: ack.clone(),
                    };
                    if let Some(sender) = &outbound {
                        sender.send(packet).map_err(|e| e.to_string())?;
                    } else {
                        release(packet)?;
                    }
                    let (freed, received) = acknowledgements
                        .recv_timeout(Duration::from_secs(30))
                        .map_err(|e| e.to_string())?;
                    if freed != o.bytes || received != checksum {
                        return Err("dynamic exact-free acknowledgement mismatch".into());
                    }
                    result.successful += 1;
                    result.freed = add(result.freed, freed)?;
                    result.checksum = result.checksum.wrapping_add(received);
                    sample_dynamic(&mut result, start.elapsed().as_nanos() as u64);
                }
                if let Some(scope) = scope {
                    black_box(scope.finish());
                }
                Ok(result)
            }));
        }
        drop(senders);
        let began = Instant::now();
        barrier.wait();
        let mut transitions = Vec::new();
        let mut revision_index = 0;
        let mut next = Instant::now() + Duration::from_millis(10);
        while began.elapsed() < Duration::from_millis(o.duration_ms) {
            if Instant::now() >= next {
                let target = [low, 0, high][revision_index % 3];
                let committed_before = a.authority.root().committed_bytes();
                let start = Instant::now();
                let revision = writer.set_capacity(target).map_err(|e| e.to_string())?;
                let elapsed_ns = start.elapsed().as_nanos();
                transitions.push(json!({"target":target,"revision":revision,"committed_before":committed_before,"preexisting_excess":committed_before.saturating_sub(target),"service_ns":elapsed_ns}));
                if revision_index == 0 {
                    if committed_before <= target {
                        return Err(
                            "first frozen low target failed to create preexisting excess".into(),
                        );
                    }
                    first_low.send(()).map_err(|e| e.to_string())?;
                }
                revision_index += 1;
                next += Duration::from_millis(10);
            }
            thread::sleep(Duration::from_micros(100));
        }
        stop.store(true, Ordering::Release);
        let mut workers = Vec::new();
        for handle in worker_handles {
            workers.push(handle.join().map_err(|_| "dynamic producer panicked")??);
        }
        for handle in receiver_handles {
            handle
                .join()
                .map_err(|_| "dynamic free consumer panicked")??;
        }
        let progress = maintenance
            .join()
            .map_err(|_| "maintenance worker panicked")??;
        Ok((workers, transitions, progress, began.elapsed()))
    })?;
    let mut samples = Vec::new();
    let mut attempts = 0;
    let mut successful = 0;
    let mut refused = 0;
    let mut checksum = 0u64;
    let mut freed = 0;
    let mut activation_races = 0;
    let mut max_ns = 0;
    for worker in workers {
        samples.extend(worker.samples);
        attempts += worker.attempts;
        successful += worker.successful;
        refused += worker.refused;
        checksum = checksum.wrapping_add(worker.checksum);
        freed = add(freed, worker.freed)?;
        activation_races += worker.activation_race_refusals;
        max_ns = max_ns.max(worker.max_ns);
    }
    samples.sort_unstable();
    if attempts != successful + refused || freed != mul(successful, o.bytes)? {
        return Err("dynamic publication/free conservation failed".into());
    }
    let root_after = a.authority.root().interactions();
    let parent_after: u64 = a.accounts.iter().map(AccountHandle::interactions).sum();
    let root_metrics_after = a.authority.root().interaction_snapshot();
    let post_load_resources = resource_snapshot(&a);
    let parent_metrics_after = interaction_sum(&a.accounts);
    writer.set_capacity(high).map_err(|e| e.to_string())?;
    for domain in a.domains.iter().flatten() {
        domain
            .retire_lane()
            .map_err(|e| format!("dynamic lane retirement: {e:?}"))?;
    }
    release_history(std::mem::take(&mut a.historical_origins), o.bytes)?;
    drop(std::mem::take(&mut a.domains));
    let mut cleanup_batches = 0;
    loop {
        cleanup_batches += 1;
        let r = a.authority.maintain(64);
        if r.complete {
            break;
        }
        if cleanup_batches > 1_000_000 {
            return Err("dynamic final coverage incomplete".into());
        }
    }
    let projection = a.authority.pressure_projection();
    let mut record = json!({"kind":"timed_dynamic_protocol_round","round":round,"provenance":provenance(),"threads":o.threads,"free_consumer_threads":match o.mode.as_str(){"fan-in"=>1,"round-robin"=>o.threads,_=>0},"mode":o.mode,"work_us":o.work_us,"supply_work_iterations":o.work_iterations,"supply_kind":if o.work_iterations.is_some(){"fixed_calibrated_iterations"}else{"wall_clock_smoke_probe"},"scope_interval":o.interval,"operation_bytes":o.bytes,"quantum_bytes":o.quantum,"accounts":o.accounts,"depth":o.depth,"domains_per_thread":o.domains_per_thread,"historical_residual_count":o.residual_count,"historical_backing_bytes":a.historical_backing,"historical_owner_metadata_bytes":OWNER_METADATA_BYTES,"late_free_phase":o.late_phase,"elapsed_ns":elapsed.as_nanos(),"frozen_minimum_duration_met":o.duration_ms>=30000,"capacity_transition_period_us":10000,"maintenance_period_us":1000,"maintenance_budget":64,"attempts":attempts,"successful_pairs":successful,"refusals":refused,"activation_race_refusals":activation_races,"free_acknowledged_bytes":freed,"checksum":checksum,"reservoir_samples":samples.len(),"represented_iterations":attempts,"iteration_p99_ns":quantile(&samples,99,100),"iteration_p999_ns":quantile(&samples,999,1000),"iteration_max_sample_ns":samples.last(),"iteration_max_ns":max_ns,"root_interactions":root_after-root_before,"parent_interactions":parent_after-parent_before,"transitions":transitions,"maintenance":maintenance,"cleanup_coverage_batches":cleanup_batches,"final_root_committed_bytes":projection.root_committed,"final_residual_metadata_bytes":projection.residual_metadata,"control_floor_bytes":a.floor,"assembly_storage_bytes":a.storage,"matrix_complete":false,"formal_acceptance":false,"growth_gate_atomic_cost":null,"control_queue_wait":null,"control_lock_hold":null,"formal_blockers":["historical_static_comparator_requires_external_results","full_frozen_matrix_and_independent_rounds_unverified","hardware_growth_gate_cacheline_atomic_cost_unmeasured","once_only_machine_supply_calibration_receipt_unverified"]});
    record["post_load_resource_snapshot"] = post_load_resources;
    record["final_resource_snapshot"] = resource_snapshot(&a);
    record["local_hook_calls"] =
        json!({"actual_allocation_publications":successful,"actual_origin_releases":successful});
    record["sample_dispersion_ns"] = json!({"p50":quantile(&samples,50,100),"p90":quantile(&samples,90,100),"p99":quantile(&samples,99,100),"p999":quantile(&samples,999,1000),"max_sample":samples.last(),"actual_max":max_ns,"population":"bounded reservoir"});
    record["root_control_metrics"] = interaction_delta(root_metrics_before, root_metrics_after);
    record["parent_control_metrics"] =
        interaction_delta(parent_metrics_before, parent_metrics_after);
    record["control_queue_wait"] = json!({"aggregation":"sum","parent":record["parent_control_metrics"]["ledger_wait_ns"],"root":record["root_control_metrics"]["ledger_wait_ns"]});
    record["control_lock_hold"] = json!({"aggregation":"sum","parent":record["parent_control_metrics"]["ledger_hold_ns"],"root":record["root_control_metrics"]["ledger_hold_ns"]});
    record["growth_gate_acquisitions"] = json!({"parent":record["parent_control_metrics"]["gate_acquisitions"],"root":record["root_control_metrics"]["gate_acquisitions"]});
    record["growth_gate_wait"] = json!({"aggregation":"actual sum", "parent_ns":record["parent_control_metrics"]["gate_wait_ns"],"root_ns":record["root_control_metrics"]["gate_wait_ns"]});
    Ok(record)
}
fn calibrate_supply() -> Value {
    const ITERATIONS: u64 = 100000;
    let mut samples = Vec::with_capacity(9);
    for sample in 0..9 {
        let began = Instant::now();
        black_box(supply_work(0, sample + 1, Some(ITERATIONS)));
        samples.push(began.elapsed().as_nanos() as u64);
    }
    samples.sort_unstable();
    let median = samples[4].max(1);
    let iterations = |us: u64| (us * 1000 * ITERATIONS).div_ceil(median).max(1);
    json!({"schema_version":1,"kind":"once_only_supply_calibration","provenance":provenance(),"calibration_loop_iterations":ITERATIONS,"calibration_samples_ns":samples,"calibration_median_ns":median,"iterations":{"0":0,"1":iterations(1),"5":iterations(5),"20":iterations(20)},"matrix_complete":false,"formal_acceptance":false,"instruction_recipe":"xorshift13_7_17_u64_then_black_box_each_iteration","repeat_for_candidate_tuning_forbidden":true})
}
fn main() {
    let result = (|| -> BenchResult<()> {
        let o = Options::parse()?;
        let manifest = validate_manifest(&o)?;
        if o.calibrate {
            println!("{}", calibrate_supply());
        } else if o.dynamic {
            for round in 0..o.rounds {
                println!("{}", dynamic_round(&o, &manifest, round)?);
            }
        } else if o.service {
            println!("{}", service(&o, &manifest)?);
        } else {
            for round in 0..o.rounds {
                println!("{}", run_round(&o, &manifest, round)?);
            }
        }
        Ok(())
    })();
    if let Err(error) = result {
        eprintln!("stock_cost failed: {error}");
        std::process::exit(1);
    }
}
