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

use std::sync::{Arc, Weak};
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crate::connector::starrocks::fs_access::resolve_tablet_root;
use crate::connector::starrocks::table::config::StarRocksTableConfig;
use crate::engine::StandaloneState;
use crate::fs::object_store::oss_block_on;
use crate::novarocks_logging::warn;

const ERASE_RETRY_DELAY_MS: i64 = 5_000;
const ERASE_WORKER_POLL_INTERVAL: Duration = Duration::from_secs(2);

pub(crate) fn run_erase_jobs_once(state: &StandaloneState) -> Result<(), String> {
    let config = state
        .starrocks_table_config
        .as_ref()
        .ok_or_else(|| "StarRocks table erase worker requires config".to_string())?;
    run_erase_jobs_once_with(state, |root_path| erase_root(root_path, config))
}

fn run_erase_jobs_once_with<F>(state: &StandaloneState, mut erase_root_fn: F) -> Result<(), String>
where
    F: FnMut(&str) -> Result<(), String>,
{
    let provider = state
        .metadata_provider
        .as_ref()
        .ok_or_else(|| "StarRocks table erase worker requires metadata provider".to_string())?;
    let now_ms = current_time_ms();
    let read = provider
        .begin_read()
        .map_err(|e| format!("open erase job read transaction failed: {e}"))?;
    let jobs = state
        .job_repo
        .list_runnable_erase_jobs(read.as_ref(), now_ms)
        .map_err(|e| format!("list erase jobs failed: {e}"))?;
    drop(read);

    for job in jobs {
        let claimed = {
            let mut txn = provider
                .begin_write("claim StarRocks table erase job")
                .map_err(|e| format!("open erase job claim transaction failed: {e}"))?;
            let claimed = state
                .job_repo
                .claim_erase_job(txn.as_mut(), job.job_id, current_time_ms())
                .map_err(|e| format!("claim erase job {} failed: {e}", job.job_id))?;
            txn.commit()
                .map_err(|e| format!("commit erase job claim failed: {e}"))?;
            claimed
        };
        if !claimed {
            continue;
        }

        let result: Result<(), String> = (|| {
            erase_root_fn(&job.root_path)?;
            let mut txn = provider
                .begin_write("finish StarRocks table erase job")
                .map_err(|e| format!("open erase job finish transaction failed: {e}"))?;
            match job.partition_id {
                None => {
                    state
                        .starrocks_txn_repo
                        .delete_for_table(txn.as_mut(), job.table_id)
                        .map_err(|e| format!("delete erased table txns failed: {e}"))?;
                    state
                        .starrocks_table_repo
                        .purge_retired_table_metadata(txn.as_mut(), job.table_id)
                        .map_err(|e| format!("purge erased table metadata failed: {e}"))?;
                }
                Some(partition_id) => {
                    state
                        .starrocks_txn_repo
                        .delete_for_partition(txn.as_mut(), partition_id)
                        .map_err(|e| format!("delete erased partition txns failed: {e}"))?;
                    state
                        .starrocks_table_repo
                        .purge_retired_partition_metadata(txn.as_mut(), partition_id)
                        .map_err(|e| format!("purge erased partition metadata failed: {e}"))?;
                }
            }
            state
                .job_repo
                .finish_erase_job(txn.as_mut(), job.job_id, current_time_ms())
                .map_err(|e| format!("finish erase job {} failed: {e}", job.job_id))?;
            txn.commit()
                .map_err(|e| format!("commit erase job finish failed: {e}"))?;
            Ok(())
        })();

        if let Err(err) = result {
            let retry_at_ms = current_time_ms() + ERASE_RETRY_DELAY_MS;
            let mut txn = provider
                .begin_write("fail StarRocks table erase job")
                .map_err(|e| format!("open erase job failure transaction failed: {e}"))?;
            state
                .job_repo
                .fail_erase_job(
                    txn.as_mut(),
                    job.job_id,
                    err.clone(),
                    Some(retry_at_ms),
                    current_time_ms(),
                )
                .map_err(|persist_err| {
                    format!(
                        "record erase failure for job {} failed after `{err}`: {persist_err}",
                        job.job_id
                    )
                })?;
            txn.commit()
                .map_err(|e| format!("commit erase job failure failed: {e}"))?;
        }
    }
    Ok(())
}

pub(crate) fn spawn_erase_worker(state: Arc<StandaloneState>) {
    let weak = Arc::downgrade(&state);
    thread::spawn(move || erase_worker_loop(weak));
}

fn erase_worker_loop(state: Weak<StandaloneState>) {
    loop {
        let Some(strong) = state.upgrade() else {
            return;
        };
        if strong.metadata_provider.is_none() {
            return;
        }
        if strong.starrocks_table_config.is_none() {
            return;
        }

        if let Err(err) = run_erase_jobs_once(&strong) {
            warn!("StarRocks table erase worker iteration failed: {err}");
        }
        drop(strong);
        thread::sleep(ERASE_WORKER_POLL_INTERVAL);
    }
}

fn erase_root(root_path: &str, config: &StarRocksTableConfig) -> Result<(), String> {
    let root_access = resolve_tablet_root(root_path, Some(&config.s3))
        .map_err(|e| format!("resolve erase root `{root_path}` failed: {e}"))?;
    let rel_path = root_access
        .single_relative_path()
        .map_err(|e| format!("resolve erase root `{root_path}` failed: {e}"))?;
    let warehouse_access =
        resolve_tablet_root(&config.warehouse_uri, Some(&config.s3)).map_err(|e| {
            format!(
                "resolve StarRocks table warehouse `{}` failed: {e}",
                config.warehouse_uri
            )
        })?;
    let warehouse_rel = warehouse_access.single_relative_path().map_err(|e| {
        format!(
            "resolve StarRocks table warehouse `{}` failed: {e}",
            config.warehouse_uri
        )
    })?;
    let erase_prefix = erase_prefix_path(&rel_path, &warehouse_rel)
        .map_err(|e| format!("refuse to erase StarRocks table root `{root_path}`: {e}"))?;
    let operator = root_access.operator();
    let remove_result = oss_block_on(operator.remove_all(&erase_prefix))
        .map_err(|e| format!("run erase root `{root_path}` failed: {e}"))?;
    remove_result.map_err(|e| format!("erase root `{root_path}` failed: {e}"))?;
    Ok(())
}

fn erase_prefix_path(rel_path: &str, warehouse_rel: &str) -> Result<String, String> {
    let trimmed = rel_path.trim_matches('/');
    let warehouse_trimmed = warehouse_rel.trim_matches('/');
    // Refuse erasing the bucket root or the entire StarRocks warehouse —
    // these would otherwise wipe data belonging to other StarRocks tables
    // or even the entire bucket.
    if trimmed.is_empty() || trimmed == warehouse_trimmed {
        return Err("empty StarRocks table root".to_string());
    }
    if !warehouse_trimmed.is_empty() && !trimmed.starts_with(&format!("{warehouse_trimmed}/")) {
        return Err(format!(
            "StarRocks table root `{trimmed}` is outside warehouse `{warehouse_trimmed}`"
        ));
    }
    Ok(format!("{trimmed}/"))
}

fn current_time_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}
