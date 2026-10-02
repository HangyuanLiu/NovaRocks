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

//! Measurements are explicit about live, retained, sampled and unavailable facts.
use crate::Variant;
use serde::Serialize;
use sha2::{Digest, Sha256};
use std::{collections::BTreeMap, ffi::CStr, fs, process::Command};

#[derive(Clone, Copy, Debug, Default, Serialize)]
pub struct CpuSample {
    pub user_ns: u64,
    pub system_ns: u64,
    pub available: bool,
}
impl CpuSample {
    pub fn delta(self, before: Self) -> Self {
        Self {
            user_ns: self.user_ns.saturating_sub(before.user_ns),
            system_ns: self.system_ns.saturating_sub(before.system_ns),
            available: self.available && before.available,
        }
    }
}
pub fn cpu_sample() -> CpuSample {
    // SAFETY: getrusage initializes the valid writable output on success.
    unsafe {
        let mut value: libc::rusage = std::mem::zeroed();
        if libc::getrusage(libc::RUSAGE_SELF, &mut value) != 0 {
            return CpuSample::default();
        }
        fn nanos(v: libc::timeval) -> u64 {
            v.tv_sec as u64 * 1_000_000_000 + v.tv_usec as u64 * 1000
        }
        CpuSample {
            user_ns: nanos(value.ru_utime),
            system_ns: nanos(value.ru_stime),
            available: true,
        }
    }
}
#[derive(Clone, Copy, Debug, Serialize)]
pub struct JemallocSample {
    pub allocated_bytes: u64,
    pub active_bytes: u64,
    pub resident_bytes: u64,
}
#[derive(Clone, Copy, Debug, Serialize)]
pub struct SpaceSample {
    pub jemalloc: Option<JemallocSample>,
    pub current_rss_bytes: Option<u64>,
    pub peak_rss_bytes: Option<u64>,
}
#[derive(Clone, Copy, Debug, Serialize)]
pub struct SpaceDelta {
    pub allocated_bytes: Option<i128>,
    pub active_bytes: Option<i128>,
    pub resident_bytes: Option<i128>,
    pub current_rss_bytes: Option<i128>,
}
impl SpaceSample {
    pub fn delta(self, before: Self) -> SpaceDelta {
        let (allocated_bytes, active_bytes, resident_bytes) = match (self.jemalloc, before.jemalloc)
        {
            (Some(a), Some(b)) => (
                Some(i128::from(a.allocated_bytes) - i128::from(b.allocated_bytes)),
                Some(i128::from(a.active_bytes) - i128::from(b.active_bytes)),
                Some(i128::from(a.resident_bytes) - i128::from(b.resident_bytes)),
            ),
            _ => (None, None, None),
        };
        SpaceDelta {
            allocated_bytes,
            active_bytes,
            resident_bytes,
            current_rss_bytes: self
                .current_rss_bytes
                .zip(before.current_rss_bytes)
                .map(|(a, b)| i128::from(a) - i128::from(b)),
        }
    }
}
pub fn space_sample(variant: Variant) -> Result<SpaceSample, String> {
    let jemalloc = if variant.has_jemalloc() {
        tikv_jemalloc_ctl::epoch::advance().map_err(|e| e.to_string())?;
        Some(JemallocSample {
            allocated_bytes: tikv_jemalloc_ctl::stats::allocated::read()
                .map_err(|e| e.to_string())? as u64,
            active_bytes: tikv_jemalloc_ctl::stats::active::read().map_err(|e| e.to_string())?
                as u64,
            resident_bytes: tikv_jemalloc_ctl::stats::resident::read().map_err(|e| e.to_string())?
                as u64,
        })
    } else {
        None
    };
    #[cfg(target_os = "linux")]
    let current_rss_bytes = {
        // SAFETY: sysconf has no pointer inputs; the returned page size is checked.
        let page = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };
        fs::read_to_string("/proc/self/statm")
            .ok()
            .and_then(|s| {
                s.split_whitespace()
                    .nth(1)
                    .and_then(|n| n.parse::<u64>().ok())
            })
            .and_then(|n| (page > 0).then(|| n.saturating_mul(page as u64)))
    };
    #[cfg(not(target_os = "linux"))]
    let current_rss_bytes = None;
    // SAFETY: getrusage initializes writable output, maxrss has OS-specific units.
    let peak_rss_bytes = unsafe {
        let mut value: libc::rusage = std::mem::zeroed();
        if libc::getrusage(libc::RUSAGE_SELF, &mut value) == 0 {
            #[cfg(target_os = "macos")]
            let bytes = value.ru_maxrss as u64;
            #[cfg(not(target_os = "macos"))]
            let bytes = (value.ru_maxrss as u64).saturating_mul(1024);
            Some(bytes)
        } else {
            None
        }
    };
    Ok(SpaceSample {
        jemalloc,
        current_rss_bytes,
        peak_rss_bytes,
    })
}
#[derive(Debug, Serialize)]
pub struct HostMetadata {
    pub host: String,
    pub kernel: String,
    pub rustc: String,
    pub target_os: &'static str,
    pub target_arch: &'static str,
    pub build_debug_assertions: bool,
    pub git_sha: String,
    pub git_dirty: bool,
    pub cargo_lock_sha256: String,
    pub jemallocator_crate_version: &'static str,
    pub jemalloc_ctl_crate_version: &'static str,
    pub jemalloc_runtime_version: Option<String>,
    pub jemalloc_build_malloc_conf: Option<String>,
    pub malloc_conf_environment: Option<String>,
    pub prefixed_malloc_conf_environment: Option<String>,
    pub effective_jemalloc_options: BTreeMap<String, serde_json::Value>,
    pub cpu_affinity: Option<String>,
    pub jemalloc_unavailable_reason: Option<&'static str>,
}
fn command(program: &str, args: &[&str]) -> Result<String, String> {
    let out = Command::new(program)
        .args(args)
        .output()
        .map_err(|e| e.to_string())?;
    if !out.status.success() {
        return Err(format!("metadata command {program} failed"));
    }
    Ok(String::from_utf8_lossy(&out.stdout).trim().into())
}
fn ctl_string(name: &[u8]) -> Result<String, String> {
    // SAFETY: these named mallctl keys return a process-lived const char pointer.
    unsafe {
        let value: *const libc::c_char =
            tikv_jemalloc_ctl::raw::read(name).map_err(|e| e.to_string())?;
        if value.is_null() {
            return Err("jemalloc returned null metadata string".into());
        }
        Ok(CStr::from_ptr(value).to_string_lossy().into_owned())
    }
}
pub fn host_metadata(variant: Variant) -> Result<HostMetadata, String> {
    let mut options = BTreeMap::new();
    let (version, conf) = if variant.has_jemalloc() {
        // SAFETY: opt.narenas is a documented unsigned mallctl value.
        let narenas = unsafe { tikv_jemalloc_ctl::raw::read::<libc::c_uint>(b"opt.narenas\0") }
            .map_err(|e| e.to_string())?;
        options.insert("opt.narenas".into(), serde_json::json!(narenas));
        options.insert(
            "opt.tcache_max".into(),
            serde_json::json!(
                tikv_jemalloc_ctl::opt::tcache_max::read().map_err(|e| e.to_string())?
            ),
        );
        for key in ["opt.dirty_decay_ms", "opt.muzzy_decay_ms"] {
            let name = format!("{key}\0");
            // SAFETY: decay options are ssize_t values.
            let value = unsafe { tikv_jemalloc_ctl::raw::read::<libc::ssize_t>(name.as_bytes()) }
                .map_err(|e| e.to_string())?;
            options.insert(key.into(), serde_json::json!(value));
        }
        for key in ["opt.tcache", "opt.background_thread"] {
            let name = format!("{key}\0");
            // SAFETY: these mallctl keys are documented bool values.
            let value = unsafe { tikv_jemalloc_ctl::raw::read::<bool>(name.as_bytes()) }
                .map_err(|e| e.to_string())?;
            options.insert(key.into(), serde_json::json!(value));
        }
        (
            Some(ctl_string(b"version\0")?),
            Some(ctl_string(b"config.malloc_conf\0")?),
        )
    } else {
        (None, None)
    };
    let affinity = fs::read_to_string("/proc/self/status").ok().and_then(|s| {
        s.lines().find_map(|line| {
            line.strip_prefix("Cpus_allowed_list:")
                .map(|v| v.trim().to_owned())
        })
    });
    let repository = command("git", &["rev-parse", "--show-toplevel"])?;
    let lock_path = std::path::Path::new(&repository).join("Cargo.lock");
    let lock = fs::read(lock_path).map_err(|e| e.to_string())?;
    for (name, expected) in [
        ("tikv-jemallocator", "0.7.0"),
        ("tikv-jemalloc-ctl", "0.7.0"),
        (
            "tikv-jemalloc-sys",
            "0.7.1+5.3.1-0-g81034ce1f1373e37dc865038e1bc8eeecf559ce8",
        ),
    ] {
        let pattern = format!("name = \"{name}\"\nversion = \"{expected}\"");
        if !String::from_utf8_lossy(&lock).contains(&pattern) {
            return Err(format!(
                "frozen allocator version missing from Cargo.lock: {name} {expected}"
            ));
        }
    }
    Ok(HostMetadata {
        target_os: std::env::consts::OS,
        target_arch: std::env::consts::ARCH,
        build_debug_assertions: cfg!(debug_assertions),
        host: command("hostname", &[])?,
        kernel: command("uname", &["-a"])?,
        rustc: command("rustc", &["--version", "--verbose"])?,
        git_sha: command("git", &["rev-parse", "HEAD"])?,
        git_dirty: !command("git", &["status", "--porcelain"])?.is_empty(),
        cargo_lock_sha256: format!("{:x}", Sha256::digest(&lock)),
        jemallocator_crate_version: "0.7.0",
        jemalloc_ctl_crate_version: "0.7.0",
        jemalloc_runtime_version: version,
        jemalloc_build_malloc_conf: conf,
        malloc_conf_environment: std::env::var("MALLOC_CONF").ok(),
        prefixed_malloc_conf_environment: std::env::var("_RJEM_MALLOC_CONF").ok(),
        effective_jemalloc_options: options,
        cpu_affinity: affinity,
        jemalloc_unavailable_reason: (!variant.has_jemalloc()).then_some(
            "System allocator variant; jemalloc statistics and usable sizes do not describe its blocks",
        ),
    })
}
