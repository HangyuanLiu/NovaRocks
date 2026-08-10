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

//! Bounded CP-3A recovery scheduling.
//!
//! Statement-family historical reconciliation lands in CP-3B/C/D. Until
//! those profiles are installed, this controller only claims due operations
//! under their exact operation lease and defers them without changing any
//! business lifecycle or external evidence.

use std::sync::Arc;
use std::time::Duration;

use tokio::sync::watch;
use tokio::task::JoinHandle;

use crate::dml::model::DML_RECOVERY_SHARD_COUNT;
use crate::dml::now_unix_millis;
use crate::dml::service::DmlService;

pub(crate) const DML_RECOVERY_POLL_INTERVAL: Duration = Duration::from_millis(500);
pub(crate) const DML_RECOVERY_UNSUPPORTED_PROFILE_DELAY_MS: i64 = 30_000;
pub(crate) const DML_RECOVERY_MAX_CLAIMS_PER_POLL: usize = 4;

pub(crate) struct DmlRecoveryController {
    stop: watch::Sender<bool>,
    task: Option<JoinHandle<()>>,
}

impl DmlRecoveryController {
    pub(crate) fn start(service: Arc<DmlService>) -> Self {
        let (stop, mut stop_rx) = watch::channel(false);
        let task = tokio::spawn(async move {
            let shard_count = usize::from(DML_RECOVERY_SHARD_COUNT);
            let mut next_shard = 0usize;
            let mut shard_offsets = [0usize; DML_RECOVERY_SHARD_COUNT as usize];
            loop {
                tokio::select! {
                    changed = stop_rx.changed() => {
                        if changed.is_err() || *stop_rx.borrow() {
                            return;
                        }
                    }
                    () = tokio::time::sleep(DML_RECOVERY_POLL_INTERVAL) => {}
                }
                let cutoff = now_unix_millis();
                let mut remaining = DML_RECOVERY_MAX_CLAIMS_PER_POLL;
                for shard_offset in 0..shard_count {
                    if remaining == 0 {
                        break;
                    }
                    let shard_index = (next_shard + shard_offset) % shard_count;
                    let shard = shard_index as u8;
                    let service_for_scan = Arc::clone(&service);
                    let candidates = match tokio::task::spawn_blocking(move || {
                        service_for_scan.recovery_candidates(shard, cutoff)
                    })
                    .await
                    {
                        Ok(Ok(candidates)) => candidates,
                        Ok(Err(error)) => {
                            tracing::warn!(shard, error = %error, "scan DML recovery candidates failed");
                            continue;
                        }
                        Err(error) => {
                            tracing::warn!(shard, error = %error, "DML recovery scan task failed");
                            continue;
                        }
                    };
                    if candidates.is_empty() {
                        shard_offsets[shard_index] = 0;
                        continue;
                    }
                    let candidate_count = candidates.len();
                    let start = shard_offsets[shard_index] % candidate_count;
                    let selected = candidates
                        .iter()
                        .cycle()
                        .skip(start)
                        .take(remaining.min(candidate_count))
                        .cloned()
                        .collect::<Vec<_>>();
                    shard_offsets[shard_index] = (start + selected.len()) % candidate_count;
                    for candidate in selected {
                        remaining -= 1;
                        let service_for_claim = Arc::clone(&service);
                        let operation_id = candidate.operation_id;
                        let next_due =
                            cutoff.saturating_add(DML_RECOVERY_UNSUPPORTED_PROFILE_DELAY_MS);
                        match tokio::task::spawn_blocking(move || {
                            service_for_claim.defer_recovery_candidate(candidate, next_due)
                        })
                        .await
                        {
                            Ok(Ok(())) => {}
                            Ok(Err(error)) => tracing::debug!(
                                operation_id = %operation_id,
                                error = %error,
                                "DML recovery candidate was not claimed"
                            ),
                            Err(error) => tracing::warn!(
                                operation_id = %operation_id,
                                error = %error,
                                "DML recovery claim task failed"
                            ),
                        }
                    }
                }
                next_shard = (next_shard + 1) % shard_count;
            }
        });
        Self {
            stop,
            task: Some(task),
        }
    }

    pub(crate) async fn shutdown(&mut self) {
        self.stop.send_replace(true);
        if let Some(mut task) = self.task.take() {
            if tokio::time::timeout(Duration::from_secs(5), &mut task)
                .await
                .is_err()
            {
                tracing::warn!(
                    "DML recovery controller exceeded the 5s shutdown target; draining its in-flight StateStore operation before teardown"
                );
                let _ = task.await;
            }
        }
    }
}
