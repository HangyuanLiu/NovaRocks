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

//! The shipping process's allocation observer (MEM-1 wave-1 T07).
//!
//! Installs [`CountingAllocator`] as this process's `#[global_allocator]` and
//! states, once at startup, what that observation covers and what it
//! structurally cannot see.
//!
//! # Why the shipping binary installs the wrapper
//!
//! The observation tier is worth nothing in a test that opts into it. Hard
//! governance knows only what an owner declared, so a real deployment can be
//! honest about every account it keeps and still be over its bound; the only
//! number that closes that gap is the bytes the process actually asked the
//! allocator for, and that number exists only if the wrapper is the process
//! allocator of the binary operators run. Declaring it here — in the server
//! *library*, not in `main` — is also what lets it observe allocations made
//! before `main` is entered, because a `#[global_allocator]` static is
//! resolved at link time rather than installed by code.
//!
//! # What a reading here is, and is not
//!
//! It measures requested bytes routed through the Rust global allocator and
//! attributes nothing to any query, task, operator or session: this module
//! keeps no per-work state and asks no owner who a byte belongs to. Only the
//! accounts in the governance tier own attribution.
//!
//! The difference between this reading and the governance tier's known `L` is
//! therefore a diagnostic — "this much of the process is unaccounted" — never
//! an attribution and never an accusation against a particular account. It is
//! also a lower bound: the counters are request sizes rather than physical
//! memory, and the sources they cannot reach at all are enumerated as typed
//! data by [`CoverageDescriptor::blind_spots`] and logged beside the bytes
//! rather than folded into them.
//!
//! # Startup line, and wave 2
//!
//! [`log_installed`] emits exactly one line, whose field names are frozen:
//! `live_bytes`, `allocations`, `covers`, `blind_spots`. It is a statement of
//! coverage at a moment when nothing interesting has been allocated yet, not a
//! measurement — its value is that an operator reading the log knows which
//! sources the process can account for before any query runs.
//!
//! Wave-2 pressure sampling reuses [`snapshot`]: it is allocation-free and
//! lock-free, so a sampler owns its own cadence and its own observed maximum.
//! No peak is kept here, because a peak would need one contended cache line
//! for every allocation in the process.
//!
//! # Which allocator serves the memory
//!
//! jemalloc, behind the `jemalloc` Cargo feature that is on by default. It is
//! the only supported production allocator: its internal statistics are the
//! process's second physical reading, and later governance builds on its size
//! classes. A Rust global allocator is fixed at link time, so the choice is a
//! build choice and never a runtime switch. Building without the feature keeps
//! the system allocator for same-revision baselines and for tools that need
//! it; such a build is not a supported deployment, and it says so through
//! [`process_allocator`] and [`log_allocator_configuration`].
//!
//! jemalloc's defaults are compiled in, background purge threads only on
//! Linux: jemalloc is built without them on macOS, where asking for them makes
//! its initialisation fail. jemalloc also reads `_RJEM_MALLOC_CONF` at start-up
//! and lets it override the compiled defaults, so instead of claiming the
//! environment is ignored, the process logs the configuration that took
//! effect.

use novarocks_memory::observe::{
    AllocatorInternalsReading, AllocatorSnapshot, CountingAllocator, CoverageDescriptor,
    PhysicalMemoryReading,
};

use crate::cgroup_memory;
use crate::memory_limit::{self, CgroupFinding};

/// This process's allocator: jemalloc, counted.
///
/// jemalloc serves the memory; the wrapper only adds relaxed atomic arithmetic
/// on a sharded counter array around each call, so the counters keep their
/// meaning whichever allocator sits inside.
// Design: ADR-0148 (docs/adr/ADR-0148-process-memory-capacity-authority.md)
#[cfg(feature = "jemalloc")]
#[global_allocator]
static GLOBAL: CountingAllocator<tikv_jemallocator::Jemalloc> =
    CountingAllocator::new(tikv_jemallocator::Jemalloc);

/// This process's allocator in a build without the `jemalloc` feature: the
/// system allocator, counted.
#[cfg(not(feature = "jemalloc"))]
#[global_allocator]
static GLOBAL: CountingAllocator<std::alloc::System> = CountingAllocator::new(std::alloc::System);

/// The allocator that actually serves this process's memory.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProcessAllocator {
    /// jemalloc: the only supported production allocator.
    Jemalloc,
    /// The platform allocator of a build without the `jemalloc` feature, kept
    /// for same-revision baselines and for tools that need it.
    System,
}

impl ProcessAllocator {
    /// Returns the stable label used in logs and metrics.
    pub const fn label(self) -> &'static str {
        match self {
            Self::Jemalloc => "jemalloc",
            Self::System => "system",
        }
    }
}

/// Returns the allocator this build installed.
pub const fn process_allocator() -> ProcessAllocator {
    if cfg!(feature = "jemalloc") {
        ProcessAllocator::Jemalloc
    } else {
        ProcessAllocator::System
    }
}

/// The jemalloc configuration that took effect in this process.
///
/// Read back through `mallctl` rather than restated from the build, because
/// `_RJEM_MALLOC_CONF` overrides the compiled defaults at start-up.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JemallocConfiguration {
    /// `opt.background_thread`. The process never changes it at run time, so
    /// the option is also the running state.
    pub background_thread: bool,
    /// `opt.dirty_decay_ms`; `-1` disables decay-based purging.
    pub dirty_decay_ms: i64,
    /// `opt.muzzy_decay_ms`; `-1` disables decay-based purging.
    pub muzzy_decay_ms: i64,
    /// `config.malloc_conf`: the defaults compiled into this binary.
    pub compiled_malloc_conf: &'static str,
}

/// Reads the jemalloc configuration that took effect.
///
/// `Ok(None)` means this build has no jemalloc. An error names the option
/// that could not be read; it is reported as such and never replaced by a
/// guessed default.
pub fn jemalloc_configuration() -> Result<Option<JemallocConfiguration>, String> {
    #[cfg(feature = "jemalloc")]
    {
        jemalloc::configuration().map(Some)
    }
    #[cfg(not(feature = "jemalloc"))]
    {
        Ok(None)
    }
}

#[cfg(feature = "jemalloc")]
mod jemalloc {
    use novarocks_memory::observe::AllocatorInternalsReading;
    use tikv_jemalloc_ctl::{config, epoch, opt, raw, stats};

    use super::JemallocConfiguration;

    /// Reads jemalloc's statistics after refreshing them: jemalloc caches its
    /// statistics until the epoch advances.
    pub(super) fn internals() -> Option<AllocatorInternalsReading> {
        fn bytes<E>(value: Result<usize, E>) -> Option<u64> {
            value.ok().and_then(|bytes| u64::try_from(bytes).ok())
        }
        epoch::advance().ok()?;
        Some(AllocatorInternalsReading {
            allocated_bytes: bytes(stats::allocated::read())?,
            active_bytes: bytes(stats::active::read())?,
            resident_bytes: bytes(stats::resident::read())?,
        })
    }

    pub(super) fn configuration() -> Result<JemallocConfiguration, String> {
        let background_thread = opt::background_thread::read()
            .map_err(|error| format!("read opt.background_thread: {error}"))?;
        let dirty_decay_ms = read_decay(b"opt.dirty_decay_ms\0")?;
        let muzzy_decay_ms = read_decay(b"opt.muzzy_decay_ms\0")?;
        let compiled_malloc_conf = config::malloc_conf::read()
            .map_err(|error| format!("read config.malloc_conf: {error}"))?;
        Ok(JemallocConfiguration {
            background_thread,
            dirty_decay_ms,
            muzzy_decay_ms,
            compiled_malloc_conf,
        })
    }

    fn read_decay(name: &'static [u8]) -> Result<i64, String> {
        // SAFETY: jemalloc declares both decay options as `ssize_t`, and
        // `name` is one of those two NUL-terminated option names.
        let printable = || String::from_utf8_lossy(&name[..name.len() - 1]).into_owned();
        let value = unsafe { raw::read::<libc::ssize_t>(name) }
            .map_err(|error| format!("read {}: {error}", printable()))?;
        i64::try_from(value).map_err(|_| format!("read {}: {value} exceeds i64", printable()))
    }
}

/// Reads this process's current allocation counters.
///
/// Exposed as a function rather than by exposing `GLOBAL` so a caller cannot
/// reach the allocator itself and cannot mistake a shard for a thread. The
/// read is allocation-free and lock-free and never panics, so any diagnostic
/// or sampling path may call it at any time; the returned reading is
/// eventually consistent and is a lower bound under concurrency.
pub fn snapshot() -> AllocatorSnapshot {
    GLOBAL.snapshot()
}

/// Returns what a [`snapshot`] of this process covers, and what it cannot see.
///
/// Constant data, not a measurement: the blind spots are a property of
/// wrapping the Rust global allocator, so every consumer states the same
/// coverage instead of paraphrasing it at each call site.
pub fn coverage() -> CoverageDescriptor {
    CoverageDescriptor::RUST_GLOBAL_ALLOCATOR
}

/// Samples what the operating system and the allocator report about this
/// process.
///
/// When a cgroup limit bounds the process the cgroup's anonymous memory is
/// read, otherwise the resident set. The allocator's statistics are read in a
/// jemalloc build and are unknown in any other. A source that cannot be read
/// stays unknown rather than zero. This reads files and allocator statistics,
/// so it belongs to scrape and sampling paths, never to an allocator callback.
pub fn sample_physical() -> PhysicalMemoryReading {
    let limited_cgroup = memory_limit::visible_memory().and_then(|visible| match &visible.cgroup {
        CgroupFinding::Limited { layout, .. } => Some(layout),
        _ => None,
    });
    let (cgroup_anonymous_bytes, process_resident_bytes) = match limited_cgroup {
        Some(layout) => (
            cgroup_memory::anonymous_bytes(layout, |path| std::fs::read_to_string(path)).ok(),
            None,
        ),
        None => (None, process_resident_bytes()),
    };
    PhysicalMemoryReading {
        cgroup_anonymous_bytes,
        process_resident_bytes,
        allocator_internals: allocator_internals(),
    }
}

#[cfg(feature = "jemalloc")]
fn allocator_internals() -> Option<AllocatorInternalsReading> {
    jemalloc::internals()
}

#[cfg(not(feature = "jemalloc"))]
fn allocator_internals() -> Option<AllocatorInternalsReading> {
    None
}

#[cfg(target_os = "linux")]
fn process_resident_bytes() -> Option<u64> {
    let statm = std::fs::read_to_string("/proc/self/statm").ok()?;
    // SAFETY: `sysconf` has no preconditions.
    let page_size = u64::try_from(unsafe { libc::sysconf(libc::_SC_PAGESIZE) }).ok()?;
    resident_bytes_from_statm(&statm, page_size)
}

/// `/proc/self/statm` lists sizes in pages; its second field is the resident
/// set.
#[cfg(any(target_os = "linux", test))]
fn resident_bytes_from_statm(statm: &str, page_size: u64) -> Option<u64> {
    statm
        .split_whitespace()
        .nth(1)?
        .parse::<u64>()
        .ok()?
        .checked_mul(page_size)
}

#[cfg(target_os = "macos")]
fn process_resident_bytes() -> Option<u64> {
    let mut info = std::mem::MaybeUninit::<libc::proc_taskinfo>::zeroed();
    let size = libc::c_int::try_from(std::mem::size_of::<libc::proc_taskinfo>()).ok()?;
    // SAFETY: the buffer is one `proc_taskinfo` of exactly `size` bytes, which
    // is what `PROC_PIDTASKINFO` writes for the calling process.
    let written = unsafe {
        libc::proc_pidinfo(
            libc::getpid(),
            libc::PROC_PIDTASKINFO,
            0,
            info.as_mut_ptr().cast(),
            size,
        )
    };
    if written != size {
        return None;
    }
    // SAFETY: `proc_pidinfo` filled the whole struct.
    Some(unsafe { info.assume_init() }.pti_resident_size)
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
fn process_resident_bytes() -> Option<u64> {
    None
}

/// Emits the one startup line that records the observer is installed.
///
/// Called once from process initialisation, after logging is ready and before
/// any runtime is built, so the line is the earliest evidence in the log that
/// this process can account for its own allocations at all. The field names
/// are a contract with wave-2 consumers and must not be renamed.
pub fn log_installed() {
    let snapshot = snapshot();
    let coverage = coverage();
    let blind_spots = blind_spot_labels(coverage);
    tracing::info!(
        target: "novarocks::memory_observation",
        live_bytes = snapshot.live_bytes,
        allocations = snapshot.allocations,
        covers = coverage.covered_description(),
        blind_spots = %blind_spots,
        "process allocator observation installed"
    );
}

/// Emits the startup line that says which allocator serves this process and,
/// for jemalloc, the configuration that took effect.
///
/// A line of its own rather than extra fields on [`log_installed`], whose
/// field set is frozen. An unreadable jemalloc option is logged as a warning
/// with its reason, not filled in.
pub fn log_allocator_configuration() {
    let allocator = process_allocator().label();
    match jemalloc_configuration() {
        Ok(Some(configuration)) => tracing::info!(
            target: "novarocks::memory_observation",
            allocator,
            background_thread = configuration.background_thread,
            dirty_decay_ms = configuration.dirty_decay_ms,
            muzzy_decay_ms = configuration.muzzy_decay_ms,
            compiled_malloc_conf = configuration.compiled_malloc_conf,
            "process allocator configured"
        ),
        Ok(None) => tracing::info!(
            target: "novarocks::memory_observation",
            allocator,
            allocator_internals = "unknown",
            "process allocator configured"
        ),
        Err(error) => tracing::warn!(
            target: "novarocks::memory_observation",
            allocator,
            %error,
            "process allocator configuration unreadable"
        ),
    }
}

/// Joins a descriptor's blind-spot labels into one comma-separated field value.
///
/// The stable labels are used rather than the prose descriptions so the field
/// stays a token list a later consumer can split, and the whole set is
/// rendered rather than a count so the log names *which* sources are missing.
/// Building a `String` is fine here: this runs on the startup path, never
/// inside an allocator callback, where allocating would recurse.
fn blind_spot_labels(coverage: CoverageDescriptor) -> String {
    coverage
        .blind_spots()
        .iter()
        .map(|spot| spot.label())
        .collect::<Vec<_>>()
        .join(",")
}

#[cfg(test)]
mod tests {
    use novarocks_memory::observe::BlindSpot;

    use super::*;

    #[test]
    fn the_installed_allocator_counts_this_process() {
        let before = snapshot();
        // A heap allocation the optimiser cannot remove, large enough that no
        // small-allocation shortcut can serve it without the allocator.
        let held = std::hint::black_box(vec![0u8; 1 << 20]);
        let after = snapshot();
        assert!(
            after.allocations > before.allocations,
            "installed wrapper counted no allocation: {before:?} then {after:?}"
        );
        assert!(
            after.allocated_total_bytes >= before.allocated_total_bytes + (1 << 20),
            "installed wrapper missed a 1 MiB request: {before:?} then {after:?}"
        );
        drop(held);
    }

    #[test]
    fn the_logged_blind_spots_name_every_source_the_reading_misses() {
        let rendered = blind_spot_labels(coverage());
        let labels: Vec<&str> = rendered.split(',').collect();
        assert_eq!(labels.len(), BlindSpot::ALL.len());
        for spot in BlindSpot::ALL {
            assert!(labels.contains(&spot.label()), "{rendered}");
        }
    }

    #[test]
    fn the_reported_allocator_is_the_one_this_build_installed() {
        let expected = if cfg!(feature = "jemalloc") {
            ProcessAllocator::Jemalloc
        } else {
            ProcessAllocator::System
        };
        assert_eq!(process_allocator(), expected);
    }

    #[cfg(feature = "jemalloc")]
    #[test]
    fn jemalloc_runs_background_threads_on_linux_only() {
        let configuration = jemalloc_configuration()
            .expect("jemalloc configuration is readable")
            .expect("a jemalloc build reports its configuration");
        assert_eq!(
            configuration.background_thread,
            cfg!(target_os = "linux"),
            "{configuration:?}"
        );
        assert_eq!(
            configuration
                .compiled_malloc_conf
                .contains("background_thread:true"),
            cfg!(target_os = "linux"),
            "{configuration:?}"
        );
    }

    #[cfg(not(feature = "jemalloc"))]
    #[test]
    fn a_build_without_jemalloc_reports_no_jemalloc_configuration() {
        assert_eq!(jemalloc_configuration(), Ok(None));
    }

    #[test]
    fn a_physical_sample_reads_the_cgroup_or_the_resident_set() {
        let reading = sample_physical();
        assert!(
            reading.cgroup_anonymous_bytes.is_some() || reading.process_resident_bytes.is_some(),
            "{reading:?}"
        );
    }

    #[cfg(feature = "jemalloc")]
    #[test]
    fn a_jemalloc_build_samples_allocator_statistics() {
        let held = std::hint::black_box(vec![1u8; 8 << 20]);
        let internals = sample_physical()
            .allocator_internals
            .expect("jemalloc publishes statistics");
        assert!(internals.allocated_bytes >= 8 << 20, "{internals:?}");
        assert!(
            internals.active_bytes >= internals.allocated_bytes,
            "{internals:?}"
        );
        assert!(internals.resident_bytes > 0, "{internals:?}");
        drop(held);
    }

    #[cfg(not(feature = "jemalloc"))]
    #[test]
    fn a_build_without_jemalloc_reports_allocator_statistics_as_unknown() {
        let reading = sample_physical();
        assert_eq!(reading.allocator_internals, None);
        assert_eq!(reading.allocator_internals_line().measured_bytes(), None);
    }

    #[test]
    fn statm_resident_pages_become_bytes() {
        assert_eq!(
            resident_bytes_from_statm("2500 1200 300 10 0 800 0\n", 4096),
            Some(1200 * 4096)
        );
        assert_eq!(resident_bytes_from_statm("", 4096), None);
    }

    #[test]
    fn coverage_describes_the_rust_global_allocator() {
        assert_eq!(coverage(), CoverageDescriptor::RUST_GLOBAL_ALLOCATOR);
        assert!(
            coverage()
                .covered_description()
                .contains("global allocator"),
            "{}",
            coverage().covered_description()
        );
    }
}
