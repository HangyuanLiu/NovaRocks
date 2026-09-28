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
use std::sync::OnceLock;

use anyhow::{Context, Result, bail};
#[cfg(target_os = "linux")]
use std::fs;

use crate::cgroup_memory::{self, CgroupLayout, CgroupMemory, ProbeError};

pub const DEFAULT_MEM_LIMIT_SPEC: &str = "90%";
pub const FALLBACK_VISIBLE_MEMORY_BYTES: u64 = 64 * 1024 * 1024 * 1024;

const BE_SOFT_LIMIT_RATIO: f64 = 0.9;

pub fn resolve_starrocks_process_mem_limit_bytes(mem_limit: &str) -> Result<u64> {
    let visible_memory_bytes =
        visible_memory().map_or(FALLBACK_VISIBLE_MEMORY_BYTES, |visible| visible.bytes);
    resolve_starrocks_process_mem_limit_bytes_for_visible_memory(mem_limit, visible_memory_bytes)
}

pub fn resolve_starrocks_process_mem_limit_bytes_for_visible_memory(
    mem_limit: &str,
    visible_memory_bytes: u64,
) -> Result<u64> {
    let parsed_bytes = parse_starrocks_mem_spec(mem_limit, visible_memory_bytes)?;
    let soft_limit = ((parsed_bytes as f64) * BE_SOFT_LIMIT_RATIO) as i128;
    if soft_limit <= 0 {
        bail!("failed to parse mem limit from '{mem_limit}'");
    }

    let clamped_limit = soft_limit.min(visible_memory_bytes as i128);
    if clamped_limit <= 0 {
        bail!("invalid mem limit: {clamped_limit}");
    }

    Ok(clamped_limit as u64)
}

fn parse_starrocks_mem_spec(mem_spec: &str, memory_limit: u64) -> Result<i128> {
    if mem_spec.is_empty() {
        return Ok(0);
    }

    let last = mem_spec.chars().next_back().expect("mem_spec is non-empty");
    let number_part_without_suffix = &mem_spec[..mem_spec.len() - last.len_utf8()];
    match last {
        't' | 'T' => parse_float_bytes(mem_spec, number_part_without_suffix, 1024_f64.powi(4)),
        'g' | 'G' => parse_float_bytes(mem_spec, number_part_without_suffix, 1024_f64.powi(3)),
        'm' | 'M' => parse_float_bytes(mem_spec, number_part_without_suffix, 1024_f64.powi(2)),
        'k' | 'K' => parse_float_bytes(mem_spec, number_part_without_suffix, 1024_f64),
        'b' | 'B' => parse_integer_bytes(mem_spec, number_part_without_suffix),
        '%' => {
            let percent = parse_integer_bytes(mem_spec, number_part_without_suffix)?;
            Ok(((percent as f64 / 100.0) * memory_limit as f64) as i128)
        }
        _ => parse_integer_bytes(mem_spec, mem_spec),
    }
}

fn parse_integer_bytes(mem_spec: &str, number_part: &str) -> Result<i128> {
    number_part
        .parse::<i128>()
        .with_context(|| format!("parse mem string: {mem_spec}"))
}

fn parse_float_bytes(mem_spec: &str, number_part: &str, multiplier: f64) -> Result<i128> {
    let value = number_part
        .parse::<f64>()
        .with_context(|| format!("parse mem string: {mem_spec}"))?;
    if !value.is_finite() {
        bail!("parse mem string: {mem_spec}");
    }
    Ok((value * multiplier) as i128)
}

/// What bounds this process's memory: the smaller of its cgroup limit and
/// physical memory, with where each number came from.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VisibleMemory {
    /// The smaller of the cgroup limit and physical memory.
    pub bytes: u64,
    /// Physical memory, when the platform reports it.
    pub physical_bytes: Option<u64>,
    /// What probing this process's memory cgroup found.
    pub cgroup: CgroupFinding,
}

/// What probing this process's memory cgroup found.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CgroupFinding {
    /// A memory cgroup with a limit somewhere on its path.
    Limited {
        /// Where the cgroup is.
        layout: CgroupLayout,
        /// The smallest limit on its path.
        limit_bytes: u64,
    },
    /// A memory cgroup with no limit anywhere on its path.
    Unlimited {
        /// Where the cgroup is.
        layout: CgroupLayout,
    },
    /// No memory cgroup is visible: the platform has none, or no hierarchy
    /// carrying the memory controller is mounted.
    Absent,
    /// Probing failed, so physical memory bounds the process instead. The
    /// reason is kept: a failed probe is never read as "no limit" silently.
    Failed {
        /// Why the probe failed.
        reason: String,
    },
}

impl CgroupFinding {
    /// This process's cgroup, when one was located.
    pub fn layout(&self) -> Option<&CgroupLayout> {
        match self {
            Self::Limited { layout, .. } | Self::Unlimited { layout } => Some(layout),
            Self::Absent | Self::Failed { .. } => None,
        }
    }

    /// The cgroup limit, when one was found.
    pub fn limit_bytes(&self) -> Option<u64> {
        match self {
            Self::Limited { limit_bytes, .. } => Some(*limit_bytes),
            Self::Unlimited { .. } | Self::Absent | Self::Failed { .. } => None,
        }
    }
}

impl VisibleMemory {
    /// Combines a cgroup probe with physical memory; `None` when neither
    /// bounds the process.
    pub fn compose(
        probe: Result<Option<CgroupMemory>, ProbeError>,
        physical_bytes: Option<u64>,
    ) -> Option<Self> {
        let cgroup = match probe {
            Ok(Some(CgroupMemory {
                layout,
                limit_bytes: Some(limit_bytes),
            })) => CgroupFinding::Limited {
                layout,
                limit_bytes,
            },
            Ok(Some(CgroupMemory {
                layout,
                limit_bytes: None,
            })) => CgroupFinding::Unlimited { layout },
            Ok(None) => CgroupFinding::Absent,
            Err(error) => CgroupFinding::Failed {
                reason: error.to_string(),
            },
        };
        let bytes = match (cgroup.limit_bytes(), physical_bytes) {
            (Some(limit), Some(physical)) => limit.min(physical),
            (Some(limit), None) => limit,
            (None, Some(physical)) => physical,
            (None, None) => return None,
        };
        Some(Self {
            bytes,
            physical_bytes,
            cgroup,
        })
    }

    /// Reports whether the cgroup limit, rather than physical memory, binds.
    pub fn bound_by_cgroup(&self) -> bool {
        self.cgroup
            .limit_bytes()
            .is_some_and(|limit| self.physical_bytes.is_none_or(|physical| limit <= physical))
    }
}

/// Returns this process's visible memory, probed once on first use.
///
/// `None` when neither a cgroup limit nor physical memory could be read. The
/// probe runs once so every derivation of P in this process sees the same
/// input, and so the start-up line describes the value actually used.
pub fn visible_memory() -> Option<&'static VisibleMemory> {
    static VISIBLE: OnceLock<Option<VisibleMemory>> = OnceLock::new();
    VISIBLE
        .get_or_init(|| {
            VisibleMemory::compose(cgroup_memory::probe(), detect_physical_memory_bytes())
        })
        .as_ref()
}

/// Emits the start-up line that says what bounds this process's memory.
///
/// Called once after logging is ready. Configuration may resolve P before
/// that, which is why the probe itself does not log.
pub fn log_visible_memory() {
    const TARGET: &str = "novarocks::memory_limit";
    let Some(visible) = visible_memory() else {
        tracing::warn!(
            target: TARGET,
            fallback_bytes = FALLBACK_VISIBLE_MEMORY_BYTES,
            "visible memory unknown: no cgroup limit and no physical memory reading"
        );
        return;
    };
    let source = if visible.bound_by_cgroup() {
        "cgroup"
    } else {
        "physical"
    };
    let physical_bytes = visible
        .physical_bytes
        .map_or_else(|| "unknown".to_string(), |bytes| bytes.to_string());
    match &visible.cgroup {
        CgroupFinding::Limited {
            layout,
            limit_bytes,
        } => tracing::info!(
            target: TARGET,
            visible_bytes = visible.bytes,
            source,
            cgroup_version = layout.version.label(),
            cgroup_dir = %layout.dir.display(),
            cgroup_limit_bytes = limit_bytes,
            physical_bytes = %physical_bytes,
            "process visible memory detected"
        ),
        CgroupFinding::Unlimited { layout } => tracing::info!(
            target: TARGET,
            visible_bytes = visible.bytes,
            source,
            cgroup_version = layout.version.label(),
            cgroup_dir = %layout.dir.display(),
            cgroup_limit_bytes = "none",
            physical_bytes = %physical_bytes,
            "process visible memory detected"
        ),
        CgroupFinding::Absent => tracing::info!(
            target: TARGET,
            visible_bytes = visible.bytes,
            source,
            cgroup = "absent",
            physical_bytes = %physical_bytes,
            "process visible memory detected"
        ),
        CgroupFinding::Failed { reason } => tracing::warn!(
            target: TARGET,
            visible_bytes = visible.bytes,
            source,
            cgroup_error = %reason,
            physical_bytes = %physical_bytes,
            "process visible memory detected without its cgroup"
        ),
    }
}

#[cfg(target_os = "linux")]
fn detect_physical_memory_bytes() -> Option<u64> {
    let content = fs::read_to_string("/proc/meminfo").ok()?;
    for line in content.lines() {
        let Some(rest) = line.strip_prefix("MemTotal:") else {
            continue;
        };
        let kb = rest.split_whitespace().next()?.parse::<u64>().ok()?;
        return kb.checked_mul(1024);
    }
    None
}

#[cfg(target_os = "macos")]
fn detect_physical_memory_bytes() -> Option<u64> {
    let name = std::ffi::CString::new("hw.memsize").ok()?;
    let mut mem_size: u64 = 0;
    let mut len = std::mem::size_of::<u64>();
    let rc = unsafe {
        libc::sysctlbyname(
            name.as_ptr(),
            &mut mem_size as *mut u64 as *mut libc::c_void,
            &mut len,
            std::ptr::null_mut(),
            0,
        )
    };
    if rc == 0 { Some(mem_size) } else { None }
}

#[cfg(all(unix, not(any(target_os = "linux", target_os = "macos"))))]
fn detect_physical_memory_bytes() -> Option<u64> {
    let pages = unsafe { libc::sysconf(libc::_SC_PHYS_PAGES) };
    let page_size = unsafe { libc::sysconf(libc::_SC_PAGE_SIZE) };
    if pages <= 0 || page_size <= 0 {
        return None;
    }
    (pages as u64).checked_mul(page_size as u64)
}

#[cfg(not(unix))]
fn detect_physical_memory_bytes() -> Option<u64> {
    None
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use super::{
        CgroupFinding, VisibleMemory, resolve_starrocks_process_mem_limit_bytes_for_visible_memory,
    };
    use crate::cgroup_memory::{CgroupLayout, CgroupMemory, CgroupVersion, ProbeError};

    const GIB: u64 = 1024 * 1024 * 1024;

    fn pod_cgroup(limit_bytes: Option<u64>) -> CgroupMemory {
        CgroupMemory {
            layout: CgroupLayout {
                version: CgroupVersion::V2,
                dir: PathBuf::from("/sys/fs/cgroup"),
                mount_point: PathBuf::from("/sys/fs/cgroup"),
            },
            limit_bytes,
        }
    }

    #[test]
    fn a_cgroup_limit_below_physical_memory_binds() {
        let visible =
            VisibleMemory::compose(Ok(Some(pod_cgroup(Some(16 * GIB)))), Some(128 * GIB)).unwrap();
        assert_eq!(visible.bytes, 16 * GIB);
        assert!(visible.bound_by_cgroup());
    }

    #[test]
    fn physical_memory_binds_when_the_cgroup_limit_is_larger_or_absent() {
        let larger =
            VisibleMemory::compose(Ok(Some(pod_cgroup(Some(256 * GIB)))), Some(128 * GIB)).unwrap();
        assert_eq!(larger.bytes, 128 * GIB);
        assert!(!larger.bound_by_cgroup());

        let unlimited =
            VisibleMemory::compose(Ok(Some(pod_cgroup(None))), Some(128 * GIB)).unwrap();
        assert_eq!(unlimited.bytes, 128 * GIB);
        assert!(matches!(unlimited.cgroup, CgroupFinding::Unlimited { .. }));

        let absent = VisibleMemory::compose(Ok(None), Some(128 * GIB)).unwrap();
        assert_eq!(absent.cgroup, CgroupFinding::Absent);
    }

    #[test]
    fn a_failed_probe_keeps_its_reason_and_falls_back_to_physical_memory() {
        let failed = VisibleMemory::compose(
            Err(ProbeError::Read {
                path: PathBuf::from("/proc/self/mountinfo"),
                error: "permission denied".to_string(),
            }),
            Some(128 * GIB),
        )
        .unwrap();
        assert_eq!(failed.bytes, 128 * GIB);
        match failed.cgroup {
            CgroupFinding::Failed { reason } => assert!(reason.contains("mountinfo"), "{reason}"),
            other => panic!("expected a failed probe, got {other:?}"),
        }
    }

    #[test]
    fn nothing_known_means_no_visible_memory() {
        assert_eq!(VisibleMemory::compose(Ok(None), None), None);
        let limit_only = VisibleMemory::compose(Ok(Some(pod_cgroup(Some(8 * GIB)))), None).unwrap();
        assert_eq!(limit_only.bytes, 8 * GIB);
        assert!(limit_only.bound_by_cgroup());
    }

    #[test]
    fn parses_starrocks_mem_spec_units() {
        assert_eq!(
            resolve_starrocks_process_mem_limit_bytes_for_visible_memory(
                "10G",
                100 * 1024 * 1024 * 1024
            )
            .unwrap(),
            9 * 1024 * 1024 * 1024
        );
        assert_eq!(
            resolve_starrocks_process_mem_limit_bytes_for_visible_memory(
                "10M",
                100 * 1024 * 1024 * 1024
            )
            .unwrap(),
            9 * 1024 * 1024
        );
    }

    #[test]
    fn rejects_non_positive_starrocks_mem_spec() {
        assert!(
            resolve_starrocks_process_mem_limit_bytes_for_visible_memory(
                "-1",
                100 * 1024 * 1024 * 1024
            )
            .is_err()
        );
        assert!(
            resolve_starrocks_process_mem_limit_bytes_for_visible_memory(
                "",
                100 * 1024 * 1024 * 1024
            )
            .is_err()
        );
        assert!(resolve_starrocks_process_mem_limit_bytes_for_visible_memory("1G", 0).is_err());
    }
}
