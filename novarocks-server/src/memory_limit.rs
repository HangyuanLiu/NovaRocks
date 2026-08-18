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
use anyhow::{Context, Result, bail};
#[cfg(target_os = "linux")]
use std::fs;
#[cfg(target_os = "linux")]
use std::path::Path;

pub const DEFAULT_MEM_LIMIT_SPEC: &str = "90%";
pub const FALLBACK_VISIBLE_MEMORY_BYTES: u64 = 64 * 1024 * 1024 * 1024;

const BE_SOFT_LIMIT_RATIO: f64 = 0.9;

pub fn resolve_starrocks_process_mem_limit_bytes(mem_limit: &str) -> Result<u64> {
    let visible_memory_bytes =
        detect_visible_memory_bytes().unwrap_or(FALLBACK_VISIBLE_MEMORY_BYTES);
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

pub fn detect_visible_memory_bytes() -> Option<u64> {
    match (
        detect_container_memory_limit_bytes(),
        detect_physical_memory_bytes(),
    ) {
        (Some(container), Some(physical)) => Some(container.min(physical)),
        (Some(container), None) => Some(container),
        (None, Some(physical)) => Some(physical),
        (None, None) => None,
    }
}

#[cfg(target_os = "linux")]
fn detect_container_memory_limit_bytes() -> Option<u64> {
    if !Path::new("/.dockerenv").exists() {
        return None;
    }

    let cgroup_path = std::ffi::CString::new("/sys/fs/cgroup").ok()?;
    let mut stat: libc::statfs = unsafe { std::mem::zeroed() };
    if unsafe { libc::statfs(cgroup_path.as_ptr(), &mut stat) } != 0 {
        return None;
    }

    const TMPFS_MAGIC: libc::c_long = 0x0102_1994;
    const CGROUP2_SUPER_MAGIC: libc::c_long = 0x6367_7270;

    if stat.f_type == TMPFS_MAGIC {
        return read_cgroup_memory_limit("/sys/fs/cgroup/memory/memory.limit_in_bytes");
    }
    if stat.f_type == CGROUP2_SUPER_MAGIC {
        return read_cgroup_memory_limit("/sys/fs/cgroup/memory.max")
            .or_else(|| read_cgroup_memory_limit("/sys/fs/cgroup/kubepods/memory.max"));
    }

    None
}

#[cfg(not(target_os = "linux"))]
fn detect_container_memory_limit_bytes() -> Option<u64> {
    None
}

#[cfg(target_os = "linux")]
fn read_cgroup_memory_limit(path: &str) -> Option<u64> {
    let content = fs::read_to_string(path).ok()?;
    match content.trim().parse::<u64>() {
        Ok(value) => Some(value),
        Err(_) => Some(u64::MAX),
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
    use super::resolve_starrocks_process_mem_limit_bytes_for_visible_memory;

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
