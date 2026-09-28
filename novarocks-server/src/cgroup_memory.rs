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

//! This process's memory cgroup: where it is, and what limits it.
//!
//! The limit a BE may plan against is the smallest `memory.max` (v2) or
//! `memory.limit_in_bytes` (v1) on the path from its own cgroup up to the
//! hierarchy's mount point: a parent's limit binds the process as much as its
//! own. The cgroup is located from `/proc/self/cgroup` and
//! `/proc/self/mountinfo`, never from a marker file of one container runtime
//! or a fixed path — a containerd or CRI-O pod has no `/.dockerenv`, and a
//! fixed path misses both nested cgroups and a smaller parent limit.
//!
//! Everything except [`probe`] is pure over file contents, so each layout is
//! testable from fixtures without the environment that produces it.

use std::fmt;
use std::io;
use std::path::{Component, Path, PathBuf};

/// Values at or above this are the v1 "no limit" default rather than a limit.
///
/// v1 reports an unlimited cgroup as `LONG_MAX` rounded down to the page size,
/// which differs by page size; no machine has 4 EiB of memory, so anything
/// this large means unlimited.
const V1_UNLIMITED_FLOOR: u64 = 1 << 62;

/// Which cgroup hierarchy carries the memory controller for this process.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CgroupVersion {
    /// A v1 hierarchy with the `memory` controller.
    V1,
    /// The unified v2 hierarchy.
    V2,
}

impl CgroupVersion {
    /// Returns the stable label used in logs.
    pub const fn label(self) -> &'static str {
        match self {
            Self::V1 => "v1",
            Self::V2 => "v2",
        }
    }

    const fn limit_file(self) -> &'static str {
        match self {
            Self::V1 => "memory.limit_in_bytes",
            Self::V2 => "memory.max",
        }
    }
}

/// Where this process's memory cgroup is, as seen from this process.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CgroupLayout {
    /// The hierarchy that carries the memory controller.
    pub version: CgroupVersion,
    /// This process's cgroup directory.
    pub dir: PathBuf,
    /// Where the hierarchy is mounted; the search for limits stops here.
    pub mount_point: PathBuf,
}

/// This process's memory cgroup and the smallest limit on its path.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CgroupMemory {
    /// Where the cgroup is.
    pub layout: CgroupLayout,
    /// The smallest limit from the cgroup up to the mount point; `None` when
    /// no level sets one.
    pub limit_bytes: Option<u64>,
}

/// Why this process's memory cgroup could not be probed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProbeError {
    /// The process's cgroup lies outside what the mount exposes, so its
    /// directory cannot be reached from this process.
    NotVisible {
        /// The path `/proc/self/cgroup` reports.
        cgroup_path: String,
        /// The root of the mount that should contain it.
        mount_root: String,
    },
    /// A cgroup or proc file exists but could not be read.
    Read {
        /// The file.
        path: PathBuf,
        /// The I/O error.
        error: String,
    },
    /// A limit file held something that is neither a byte count nor `max`.
    Parse {
        /// The file.
        path: PathBuf,
        /// What it held.
        content: String,
    },
}

impl fmt::Display for ProbeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NotVisible {
                cgroup_path,
                mount_root,
            } => write!(
                f,
                "cgroup path {cgroup_path} is outside the mount rooted at {mount_root}"
            ),
            Self::Read { path, error } => write!(f, "read {}: {error}", path.display()),
            Self::Parse { path, content } => {
                write!(
                    f,
                    "parse {}: unexpected content {content:?}",
                    path.display()
                )
            }
        }
    }
}

impl std::error::Error for ProbeError {}

/// Probes this process's memory cgroup.
///
/// `Ok(None)` means no memory cgroup is visible: the platform has none, or no
/// hierarchy carrying the memory controller is mounted.
#[cfg(target_os = "linux")]
pub fn probe() -> Result<Option<CgroupMemory>, ProbeError> {
    let read = |path: &Path| std::fs::read_to_string(path);
    let proc_self_cgroup = read_required(&read, Path::new("/proc/self/cgroup"))?;
    let mountinfo = read_required(&read, Path::new("/proc/self/mountinfo"))?;
    let Some(layout) = locate(&proc_self_cgroup, &mountinfo)? else {
        return Ok(None);
    };
    let limit_bytes = effective_limit(&layout, read)?;
    Ok(Some(CgroupMemory {
        layout,
        limit_bytes,
    }))
}

/// Probes this process's memory cgroup. There is none on this platform.
#[cfg(not(target_os = "linux"))]
pub fn probe() -> Result<Option<CgroupMemory>, ProbeError> {
    Ok(None)
}

#[cfg(target_os = "linux")]
fn read_required(
    read: &impl Fn(&Path) -> io::Result<String>,
    path: &Path,
) -> Result<String, ProbeError> {
    read(path).map_err(|error| ProbeError::Read {
        path: path.to_path_buf(),
        error: error.to_string(),
    })
}

/// Locates this process's memory cgroup from `/proc/self/cgroup` and
/// `/proc/self/mountinfo` contents.
///
/// A v1 hierarchy with the `memory` controller wins over the unified
/// hierarchy: a controller is bound to one hierarchy at a time, so in hybrid
/// mode the unified hierarchy carries no memory limit at all.
pub fn locate(proc_self_cgroup: &str, mountinfo: &str) -> Result<Option<CgroupLayout>, ProbeError> {
    let mounts = parse_mountinfo(mountinfo);
    let (version, cgroup_path) = if let Some(path) = v1_memory_path(proc_self_cgroup) {
        (CgroupVersion::V1, path)
    } else if let Some(path) = v2_path(proc_self_cgroup) {
        (CgroupVersion::V2, path)
    } else {
        return Ok(None);
    };
    let candidates: Vec<&Mount> = mounts
        .iter()
        .filter(|mount| match version {
            CgroupVersion::V1 => {
                mount.fs_type == "cgroup"
                    && mount
                        .super_options
                        .split(',')
                        .any(|option| option == "memory")
            }
            CgroupVersion::V2 => mount.fs_type == "cgroup2",
        })
        .collect();
    if candidates.is_empty() {
        return Ok(None);
    }
    resolve_dir(version, cgroup_path, &candidates).map(Some)
}

/// Returns the smallest limit from `layout.dir` up to `layout.mount_point`.
///
/// A level without a limit file is skipped — the v2 root cgroup has none —
/// while any other read failure is an error, because silently skipping a
/// level could miss the limit that binds.
pub fn effective_limit(
    layout: &CgroupLayout,
    read: impl Fn(&Path) -> io::Result<String>,
) -> Result<Option<u64>, ProbeError> {
    let mut smallest: Option<u64> = None;
    let mut dir = layout.dir.as_path();
    loop {
        let file = dir.join(layout.version.limit_file());
        match read(&file) {
            Ok(content) => {
                if let Some(bytes) = parse_limit(layout.version, &file, &content)? {
                    smallest = Some(smallest.map_or(bytes, |current| current.min(bytes)));
                }
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => {
                return Err(ProbeError::Read {
                    path: file,
                    error: error.to_string(),
                });
            }
        }
        if dir == layout.mount_point {
            break;
        }
        match dir.parent() {
            Some(parent) if parent.starts_with(&layout.mount_point) => dir = parent,
            _ => break,
        }
    }
    Ok(smallest)
}

fn parse_limit(
    version: CgroupVersion,
    path: &Path,
    content: &str,
) -> Result<Option<u64>, ProbeError> {
    let value = content.trim();
    if version == CgroupVersion::V2 && value == "max" {
        return Ok(None);
    }
    let bytes = value.parse::<u64>().map_err(|_| ProbeError::Parse {
        path: path.to_path_buf(),
        content: value.to_string(),
    })?;
    if version == CgroupVersion::V1 && bytes >= V1_UNLIMITED_FLOOR {
        return Ok(None);
    }
    Ok(Some(bytes))
}

/// The path of the v1 hierarchy whose controller list includes `memory`.
fn v1_memory_path(proc_self_cgroup: &str) -> Option<&str> {
    proc_self_cgroup.lines().find_map(|line| {
        let mut fields = line.splitn(3, ':');
        let (_hierarchy, controllers, path) = (fields.next()?, fields.next()?, fields.next()?);
        controllers
            .split(',')
            .any(|controller| controller == "memory")
            .then_some(path)
    })
}

/// The path in the unified hierarchy (`0::/path`).
fn v2_path(proc_self_cgroup: &str) -> Option<&str> {
    proc_self_cgroup.lines().find_map(|line| {
        let mut fields = line.splitn(3, ':');
        let (hierarchy, controllers, path) = (fields.next()?, fields.next()?, fields.next()?);
        (hierarchy == "0" && controllers.is_empty()).then_some(path)
    })
}

/// Picks the mount whose root contains the cgroup path — the deepest one when
/// several do — and maps the path into it.
fn resolve_dir(
    version: CgroupVersion,
    cgroup_path: &str,
    candidates: &[&Mount],
) -> Result<CgroupLayout, ProbeError> {
    let path = Path::new(cgroup_path);
    let best = candidates
        .iter()
        .filter_map(|mount| {
            let relative = path.strip_prefix(&mount.root).ok()?;
            let inside = !relative
                .components()
                .any(|component| component == Component::ParentDir);
            inside.then_some((*mount, relative))
        })
        .max_by_key(|(mount, _)| mount.root.components().count());
    let Some((mount, relative)) = best else {
        return Err(ProbeError::NotVisible {
            cgroup_path: cgroup_path.to_string(),
            mount_root: candidates[0].root.display().to_string(),
        });
    };
    let dir = if relative.as_os_str().is_empty() {
        mount.mount_point.clone()
    } else {
        mount.mount_point.join(relative)
    };
    Ok(CgroupLayout {
        version,
        dir,
        mount_point: mount.mount_point.clone(),
    })
}

/// The fields of one `/proc/self/mountinfo` line this module needs.
#[derive(Debug)]
struct Mount {
    root: PathBuf,
    mount_point: PathBuf,
    fs_type: String,
    super_options: String,
}

/// Parses `/proc/self/mountinfo`. Lines that do not have the documented
/// shape are skipped; they cannot describe a cgroup mount this module uses.
fn parse_mountinfo(mountinfo: &str) -> Vec<Mount> {
    mountinfo
        .lines()
        .filter_map(|line| {
            // `ID PARENT MAJ:MIN ROOT MOUNT_POINT OPTIONS [OPTIONAL...] - FSTYPE SOURCE SUPER_OPTIONS`
            let (before, after) = line.split_once(" - ")?;
            let mut before = before.split(' ');
            let root = before.nth(3)?;
            let mount_point = before.next()?;
            let mut after = after.split(' ');
            let fs_type = after.next()?;
            let _source = after.next()?;
            let super_options = after.next().unwrap_or("");
            Some(Mount {
                root: PathBuf::from(unescape(root)),
                mount_point: PathBuf::from(unescape(mount_point)),
                fs_type: fs_type.to_string(),
                super_options: super_options.to_string(),
            })
        })
        .collect()
}

/// Decodes the octal escapes mountinfo uses for space, tab, newline and
/// backslash.
fn unescape(field: &str) -> String {
    let bytes = field.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut index = 0;
    while index < bytes.len() {
        let octal = bytes
            .get(index + 1..index + 4)
            .filter(|digits| {
                bytes[index] == b'\\' && digits.iter().all(|d| (b'0'..=b'7').contains(d))
            })
            .map(|digits| {
                digits
                    .iter()
                    .fold(0u32, |value, d| value * 8 + u32::from(d - b'0'))
            })
            .and_then(|value| u8::try_from(value).ok());
        if let Some(value) = octal {
            out.push(value);
            index += 4;
        } else {
            out.push(bytes[index]);
            index += 1;
        }
    }
    String::from_utf8_lossy(&out).into_owned()
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;

    const GIB: u64 = 1024 * 1024 * 1024;

    /// A reader over an in-memory directory tree: listed files exist, the
    /// rest are `NotFound`.
    fn files(entries: &[(&str, &str)]) -> impl Fn(&Path) -> io::Result<String> {
        let map: HashMap<PathBuf, String> = entries
            .iter()
            .map(|(path, content)| (PathBuf::from(path), content.to_string()))
            .collect();
        move |path: &Path| {
            map.get(path)
                .cloned()
                .ok_or_else(|| io::Error::from(io::ErrorKind::NotFound))
        }
    }

    const V2_MOUNT: &str = "35 25 0:30 / /sys/fs/cgroup rw,nosuid,nodev,noexec,relatime shared:9 - cgroup2 cgroup2 rw,nsdelegate\n";

    fn layout(version: CgroupVersion, dir: &str, mount_point: &str) -> CgroupLayout {
        CgroupLayout {
            version,
            dir: PathBuf::from(dir),
            mount_point: PathBuf::from(mount_point),
        }
    }

    #[test]
    fn v2_with_a_cgroup_namespace_uses_the_mount_point() {
        let found = locate("0::/\n", V2_MOUNT).unwrap().unwrap();
        assert_eq!(
            found,
            layout(CgroupVersion::V2, "/sys/fs/cgroup", "/sys/fs/cgroup")
        );
        let limit = effective_limit(
            &found,
            files(&[("/sys/fs/cgroup/memory.max", "17179869184\n")]),
        );
        assert_eq!(limit, Ok(Some(16 * GIB)));
    }

    #[test]
    fn v2_nested_pod_cgroup_takes_the_smallest_limit_on_its_path() {
        let path = "/kubepods.slice/kubepods-burstable.slice/kubepods-burstable-pod1.slice/cri-containerd-abc.scope";
        let found = locate(&format!("0::{path}\n"), V2_MOUNT).unwrap().unwrap();
        assert_eq!(found.dir, PathBuf::from(format!("/sys/fs/cgroup{path}")));
        let limit = effective_limit(
            &found,
            files(&[
                (&format!("/sys/fs/cgroup{path}/memory.max"), "max\n"),
                (
                    "/sys/fs/cgroup/kubepods.slice/kubepods-burstable.slice/kubepods-burstable-pod1.slice/memory.max",
                    "8589934592\n",
                ),
                (
                    "/sys/fs/cgroup/kubepods.slice/kubepods-burstable.slice/memory.max",
                    "max\n",
                ),
                ("/sys/fs/cgroup/kubepods.slice/memory.max", "68719476736\n"),
            ]),
        );
        assert_eq!(limit, Ok(Some(8 * GIB)));
    }

    #[test]
    fn a_parent_limit_binds_when_the_leaf_sets_none() {
        let found = locate("0::/system.slice/novarocks.service\n", V2_MOUNT)
            .unwrap()
            .unwrap();
        let limit = effective_limit(
            &found,
            files(&[
                (
                    "/sys/fs/cgroup/system.slice/novarocks.service/memory.max",
                    "max\n",
                ),
                ("/sys/fs/cgroup/system.slice/memory.max", "4294967296\n"),
            ]),
        );
        assert_eq!(limit, Ok(Some(4 * GIB)));
    }

    #[test]
    fn v2_max_everywhere_means_no_limit() {
        let found = locate("0::/a/b\n", V2_MOUNT).unwrap().unwrap();
        let limit = effective_limit(
            &found,
            files(&[
                ("/sys/fs/cgroup/a/b/memory.max", "max\n"),
                ("/sys/fs/cgroup/a/memory.max", "max\n"),
            ]),
        );
        assert_eq!(limit, Ok(None));
    }

    #[test]
    fn v1_memory_controller_co_mounted_with_another_controller() {
        let proc_self_cgroup = "12:cpuset,memory:/docker/abc\n11:cpu,cpuacct:/docker/abc\n1:name=systemd:/docker/abc\n";
        let mountinfo = "40 30 0:36 /docker/abc /sys/fs/cgroup/memory rw,nosuid shared:20 - cgroup cgroup rw,cpuset,memory\n\
                         41 30 0:37 /docker/abc /sys/fs/cgroup/cpu,cpuacct rw,nosuid shared:21 - cgroup cgroup rw,cpu,cpuacct\n";
        let found = locate(proc_self_cgroup, mountinfo).unwrap().unwrap();
        assert_eq!(
            found,
            layout(
                CgroupVersion::V1,
                "/sys/fs/cgroup/memory",
                "/sys/fs/cgroup/memory"
            )
        );
        let limit = effective_limit(
            &found,
            files(&[(
                "/sys/fs/cgroup/memory/memory.limit_in_bytes",
                "2147483648\n",
            )]),
        );
        assert_eq!(limit, Ok(Some(2 * GIB)));
    }

    #[test]
    fn v1_default_huge_value_means_no_limit() {
        let found = layout(
            CgroupVersion::V1,
            "/sys/fs/cgroup/memory",
            "/sys/fs/cgroup/memory",
        );
        for unlimited in ["9223372036854771712\n", "9223372036854710272\n"] {
            let limit = effective_limit(
                &found,
                files(&[("/sys/fs/cgroup/memory/memory.limit_in_bytes", unlimited)]),
            );
            assert_eq!(limit, Ok(None), "{unlimited}");
        }
    }

    #[test]
    fn hybrid_mode_uses_the_v1_memory_controller() {
        let proc_self_cgroup = "10:memory:/user.slice\n1:name=systemd:/user.slice/session-2.scope\n0::/user.slice/session-2.scope\n";
        let mountinfo = "30 25 0:26 / /sys/fs/cgroup/unified rw,nosuid shared:5 - cgroup2 cgroup2 rw\n\
                         36 25 0:32 / /sys/fs/cgroup/memory rw,nosuid shared:15 - cgroup cgroup rw,memory\n";
        let found = locate(proc_self_cgroup, mountinfo).unwrap().unwrap();
        assert_eq!(
            found,
            layout(
                CgroupVersion::V1,
                "/sys/fs/cgroup/memory/user.slice",
                "/sys/fs/cgroup/memory"
            )
        );
    }

    #[test]
    fn no_mounted_memory_hierarchy_means_no_cgroup() {
        let mountinfo = "22 1 259:1 / / rw,relatime shared:1 - ext4 /dev/root rw\n";
        assert_eq!(locate("0::/\n", mountinfo), Ok(None));
        assert_eq!(locate("", V2_MOUNT), Ok(None));
    }

    #[test]
    fn a_cgroup_outside_the_mount_is_not_visible() {
        let outside = locate("0::/../../system.slice/x.service\n", V2_MOUNT);
        assert!(
            matches!(outside, Err(ProbeError::NotVisible { .. })),
            "{outside:?}"
        );
        let mountinfo =
            "35 25 0:30 /kubepods/pod1 /sys/fs/cgroup rw shared:9 - cgroup2 cgroup2 rw\n";
        let elsewhere = locate("0::/kubepods/pod10/c1\n", mountinfo);
        assert!(
            matches!(elsewhere, Err(ProbeError::NotVisible { .. })),
            "{elsewhere:?}"
        );
    }

    #[test]
    fn the_deepest_mount_containing_the_cgroup_wins() {
        let mountinfo = "35 25 0:30 / /sys/fs/cgroup rw shared:9 - cgroup2 cgroup2 rw\n\
                         36 25 0:30 /kubepods/pod1 /host/cgroup rw shared:10 - cgroup2 cgroup2 rw\n";
        let found = locate("0::/kubepods/pod1/c1\n", mountinfo)
            .unwrap()
            .unwrap();
        assert_eq!(
            found,
            layout(CgroupVersion::V2, "/host/cgroup/c1", "/host/cgroup")
        );
    }

    #[test]
    fn escaped_mount_points_are_decoded() {
        let mountinfo = "35 25 0:30 / /mnt/cgroup\\040root rw shared:9 - cgroup2 cgroup2 rw\n";
        let found = locate("0::/a\n", mountinfo).unwrap().unwrap();
        assert_eq!(found.dir, PathBuf::from("/mnt/cgroup root/a"));
    }

    #[test]
    fn unreadable_or_malformed_limits_are_errors() {
        let found = layout(CgroupVersion::V2, "/sys/fs/cgroup/a", "/sys/fs/cgroup");
        let denied = effective_limit(&found, |_: &Path| {
            Err(io::Error::from(io::ErrorKind::PermissionDenied))
        });
        assert!(matches!(denied, Err(ProbeError::Read { .. })), "{denied:?}");
        let malformed =
            effective_limit(&found, files(&[("/sys/fs/cgroup/a/memory.max", "lots\n")]));
        assert!(
            matches!(malformed, Err(ProbeError::Parse { .. })),
            "{malformed:?}"
        );
    }
}
