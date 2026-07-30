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

//! Compat-owned disk-report facts and worker.

use std::collections::BTreeMap;
use std::ffi::CString;
use std::hash::{Hash, Hasher};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;

use novarocks::novarocks_logging::{debug, warn};
use novarocks::thrift::{master_service, types};

use crate::control::FrontendControlState;

pub(crate) trait DiskReportSender: Send + Sync + 'static {
    fn send_disk_report(
        &self,
        fe_addr: &types::TNetworkAddress,
        request: &master_service::TReportRequest,
    ) -> Result<(), String>;
}

/// Host-owned report worker.  A changed FE endpoint can start a fresh report
/// without waiting for a blocked request to the old FE; all workers are joined
/// by `shutdown` before the host releases its control state.
pub(crate) struct DiskReportWorker {
    control: Arc<FrontendControlState>,
    sender: Arc<dyn DiskReportSender>,
    state: Mutex<WorkerState>,
}

#[derive(Default)]
struct WorkerState {
    stopping: bool,
    workers: Vec<JoinHandle<()>>,
    failure: Option<String>,
}

impl DiskReportWorker {
    pub(crate) fn new(
        control: Arc<FrontendControlState>,
        sender: Arc<dyn DiskReportSender>,
    ) -> Self {
        Self {
            control,
            sender,
            state: Mutex::new(WorkerState::default()),
        }
    }

    pub(crate) fn request(&self, be_port: u16, http_port: u16) {
        let Ok(mut state) = self.state.lock() else {
            return;
        };
        if state.stopping {
            return;
        }
        Self::reap_finished(&mut state);
        let Some(reservation) = self.control.begin_disk_report() else {
            return;
        };
        let control = Arc::clone(&self.control);
        let sender = Arc::clone(&self.sender);
        let worker = std::thread::spawn(move || {
            let result = send_report(
                sender.as_ref(),
                &reservation.fe_addr,
                reservation.backend_host.clone(),
                be_port,
                http_port,
            );
            control.finish_disk_report(&reservation, result.is_ok());
            match result {
                Ok(()) => debug!("reported disk info to FE"),
                Err(error) => warn!("failed to report disks to FE: {}", error),
            }
        });
        state.workers.push(worker);
    }

    pub(crate) fn shutdown(&self) -> Result<(), String> {
        let workers = self
            .state
            .lock()
            .map_err(|_| "disk report worker state lock is poisoned".to_string())
            .map(|mut state| {
                state.stopping = true;
                state.workers.drain(..).collect::<Vec<_>>()
            })?;
        let mut failures = Vec::new();
        for worker in workers {
            if worker.join().is_err() {
                failures.push("disk report worker panicked".to_string());
            }
        }
        if failures.is_empty() {
            Ok(())
        } else {
            Err(failures.join("; "))
        }
    }

    /// Returns an unexpected worker failure for host supervision.  A failed
    /// FE report is still a retryable protocol outcome and remains a warning;
    /// only a worker panic is reported as a component failure.
    pub(crate) fn poll_failure(&self) -> Option<String> {
        let Ok(mut state) = self.state.lock() else {
            return Some("disk report worker state lock is poisoned".to_string());
        };
        Self::reap_finished(&mut state);
        state.failure.clone()
    }

    fn reap_finished(state: &mut WorkerState) {
        let mut pending = Vec::with_capacity(state.workers.len());
        for worker in state.workers.drain(..) {
            if worker.is_finished() {
                if worker.join().is_err() {
                    warn!("disk report worker panicked");
                    state
                        .failure
                        .get_or_insert_with(|| "disk report worker panicked".to_string());
                }
            } else {
                pending.push(worker);
            }
        }
        state.workers = pending;
    }
}

fn default_storage_path() -> String {
    if let Ok(path) = std::env::var("novarocks_STORAGE_PATH")
        && !path.trim().is_empty()
        && std::path::Path::new(&path).exists()
    {
        return path;
    }
    if let Ok(cwd) = std::env::current_dir() {
        let path = cwd.to_string_lossy().to_string();
        if std::path::Path::new(&path).exists() {
            return path;
        }
    }
    "/".to_string()
}

fn stat_capacity_bytes(path: &str) -> Option<(u64, u64)> {
    let c_path = CString::new(path).ok()?;
    let mut stat: libc::statvfs = unsafe { std::mem::zeroed() };
    let rc = unsafe { libc::statvfs(c_path.as_ptr(), &mut stat) };
    if rc != 0 {
        return None;
    }
    let block_size = stat.f_frsize as u64;
    let total = stat.f_blocks as u64 * block_size;
    let available = stat.f_bavail as u64 * block_size;
    Some((total, available))
}

fn hash_path(path: &str) -> i64 {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    path.hash(&mut hasher);
    hasher.finish() as i64
}

fn send_report(
    sender: &dyn DiskReportSender,
    fe_addr: &types::TNetworkAddress,
    backend_host: String,
    be_port: u16,
    http_port: u16,
) -> Result<(), String> {
    let backend = types::TBackend::new(backend_host, be_port as i32, http_port as i32);
    let root_path = default_storage_path();
    let (total, available) = stat_capacity_bytes(&root_path).unwrap_or((1_u64 << 40, 1_u64 << 40));
    let used = total.saturating_sub(available);
    let disk = master_service::TDisk::new(
        root_path.clone(),
        total as i64,
        used as i64,
        true,
        Some(available as i64),
        Some(hash_path(&root_path)),
        Some(types::TStorageMedium::HDD),
    );
    let mut disks = BTreeMap::new();
    disks.insert(root_path, disk);
    let request = master_service::TReportRequest::new(
        backend,
        Some(0),
        None,
        None,
        Some(disks),
        None,
        None,
        None,
        None,
        None,
        None,
    );
    sender.send_disk_report(fe_addr, &request)
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;

    struct CountingSender(AtomicUsize);

    impl DiskReportSender for CountingSender {
        fn send_disk_report(
            &self,
            _: &types::TNetworkAddress,
            _: &master_service::TReportRequest,
        ) -> Result<(), String> {
            self.0.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    #[test]
    fn worker_reports_once_per_fe() {
        let control = Arc::new(FrontendControlState::new());
        control.observe_heartbeat(
            &types::TNetworkAddress::new("fe".to_string(), 9020),
            None,
            "be".to_string(),
        );
        let sender = Arc::new(CountingSender(AtomicUsize::new(0)));
        let worker = DiskReportWorker::new(Arc::clone(&control), sender.clone());

        worker.request(9060, 8040);
        worker.shutdown().expect("join worker");
        worker.request(9060, 8040);
        worker.shutdown().expect("no duplicate worker");

        assert_eq!(sender.0.load(Ordering::SeqCst), 1);
    }
}
