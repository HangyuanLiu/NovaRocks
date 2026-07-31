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

//! Lake transaction orchestration over protocol-neutral storage facts.
//!
//! StarRocks protobuf is decoded by compat before these operations enter the
//! core kernel.  Every persisted metadata/log boundary is explicit through
//! `StorageMetadataProvider`; native callers without that capability fail only
//! when they request a lake storage operation.

use futures::TryStreamExt;
use std::collections::{HashMap, HashSet};
use std::fs;
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::connector::schema;
use crate::connector::starrocks::fs_access::resolve_tablet_root;
use crate::connector::starrocks::lake::abort_executor::abort_one_tablet;
use crate::connector::starrocks::lake::abort_policy::should_skip_abort_cleanup;
use crate::connector::starrocks::lake::applier::apply_storage_txn_log_to_metadata;
use crate::connector::starrocks::lake::context::{
    TabletRuntimeEntry, cache_tablet_runtime, get_tablet_runtime, remove_tablet_runtime,
    with_txn_log_append_lock,
};
use crate::connector::starrocks::lake::service_domain::{
    AbortTransactionCommand, DeleteDataCommand, DeleteTabletsCommand, DropLakeTableCommand,
    FailedTabletsResult, LakeOkResult, LakeTransactionInfo, LakeTransactionType,
    PublishLogVersionBatchCommand, PublishLogVersionCommand, PublishVersionCommand,
    PublishVersionResult, TabletStat, TabletStatsCommand, TabletStatsResult, VacuumCommand,
    VacuumResult,
};
use crate::connector::starrocks::lake::storage_domain::{
    StorageRowset, StorageTabletMetadata, StorageTransactionLog, StorageWriteOperation,
};
use crate::connector::starrocks::lake::txn_loader::load_txn_logs_for_publish;
use crate::connector::starrocks::ports::{LakeStorageDependencies, StorageMetadataProvider};
use crate::formats::starrocks::writer::bundle_meta::{
    load_bundle_file_with_provider, load_tablet_metadata_at_version_with_provider,
    parse_bundle_version_from_meta_file_name, write_bundle_meta_file_with_provider,
};
use crate::formats::starrocks::writer::io::{
    delete_path_if_exists, read_bytes_if_exists, read_transaction_log_if_exists_with_provider,
    write_bytes, write_transaction_log_with_provider,
};
use crate::formats::starrocks::writer::layout::{
    DATA_DIR, LOG_DIR, META_DIR, bundle_meta_file_path, join_tablet_path, txn_log_file_path,
    txn_vlog_file_path,
};
use crate::novarocks_logging::warn;
use crate::runtime::global_async_runtime::data_block_on;
use crate::runtime::starlet_shard_registry::{self, S3StoreConfig};
use novarocks_fs::FsScheme;

const EMPTY_TXNLOG_TXN_ID: i64 = -1;
const DEFAULT_GET_TABLET_STATS_TIMEOUT_MS: i64 = 5 * 60 * 1000;

pub fn execute_publish_version(
    dependencies: &LakeStorageDependencies,
    command: &PublishVersionCommand,
) -> Result<PublishVersionResult, String> {
    warmup_tablet_locations_for_dependencies(dependencies, "publish_version", &command.tablet_ids);
    let provider = dependencies.storage_metadata()?;
    let transactions = normalized_publish_transactions(command)?;
    let new_version = command
        .new_version
        .ok_or_else(|| "publish_version missing new_version".to_string())?;
    if new_version <= 0 {
        return Err(format!(
            "publish_version has invalid new_version={new_version}"
        ));
    }
    let base_version = command
        .base_version
        .unwrap_or_else(|| new_version.saturating_sub(transactions.len() as i64));
    if base_version < 0 {
        return Err(format!(
            "publish_version has invalid base_version={base_version}"
        ));
    }
    let expected = base_version.saturating_add(transactions.len() as i64);
    if expected != new_version
        && !(transactions.len() == 1 && base_version == 1 && new_version > expected)
    {
        return Err(format!(
            "publish_version version mismatch: base_version={} txn_count={} expected_new_version={} actual_new_version={}",
            base_version,
            transactions.len(),
            expected,
            new_version
        ));
    }

    let mut failed_tablets = Vec::new();
    let mut tablet_row_nums = HashMap::new();
    for tablet_id in &command.tablet_ids {
        match publish_one_tablet(
            *tablet_id,
            base_version,
            new_version,
            &transactions,
            provider.clone(),
        ) {
            Ok(row_count) => {
                tablet_row_nums.insert(*tablet_id, row_count);
            }
            Err(error) => {
                warn!(
                    "publish_version failed: tablet_id={}, error={}",
                    tablet_id, error
                );
                failed_tablets.push(*tablet_id);
            }
        }
    }
    failed_tablets.sort_unstable();
    failed_tablets.dedup();
    Ok(PublishVersionResult {
        failed_tablets,
        compaction_scores: HashMap::new(),
        tablet_row_nums,
    })
}

fn normalized_publish_transactions(
    command: &PublishVersionCommand,
) -> Result<Vec<LakeTransactionInfo>, String> {
    let transactions = if command.transactions.is_empty() {
        command
            .transaction_ids
            .iter()
            .map(|txn_id| LakeTransactionInfo {
                txn_id: *txn_id,
                commit_time: command.commit_time,
                combined_txn_log: false,
                transaction_type: LakeTransactionType::Normal,
                force_publish: false,
                rebuild_pindex: false,
                gtid: 0,
                load_ids: Vec::new(),
            })
            .collect()
    } else {
        command.transactions.clone()
    };
    if transactions.is_empty() {
        return Err("publish_version requires txn_infos or txn_ids".to_string());
    }
    let empty_allowed = transactions.len() == 1;
    let mut seen = HashSet::new();
    for transaction in &transactions {
        if transaction.txn_id == EMPTY_TXNLOG_TXN_ID && empty_allowed {
            continue;
        }
        if transaction.txn_id <= 0 {
            return Err(format!(
                "publish_version txn_infos has non-positive txn_id={}",
                transaction.txn_id
            ));
        }
        if !seen.insert(transaction.txn_id) {
            return Err(format!(
                "publish_version txn_infos has duplicate txn_id={}",
                transaction.txn_id
            ));
        }
    }
    Ok(transactions)
}

fn publish_one_tablet(
    tablet_id: i64,
    base_version: i64,
    new_version: i64,
    transactions: &[LakeTransactionInfo],
    provider: Arc<dyn StorageMetadataProvider>,
) -> Result<i64, String> {
    if tablet_id <= 0 {
        return Err(format!(
            "publish_version has non-positive tablet_id={tablet_id}"
        ));
    }
    let runtime = get_runtime_for_publish(tablet_id, base_version, provider.clone())?;
    let mut metadata = if base_version == 0 {
        StorageTabletMetadata {
            id: Some(tablet_id),
            version: Some(0),
            schema: Some(runtime.schema.clone()),
            ..StorageTabletMetadata::default()
        }
    } else {
        load_tablet_metadata_at_version_with_provider(
            &runtime.root_path,
            tablet_id,
            base_version,
            provider.as_ref(),
        )?
        .ok_or_else(|| {
            format!(
                "base tablet metadata not found: tablet_id={} base_version={}",
                tablet_id, base_version
            )
        })?
    };
    let schema_id = runtime
        .schema
        .id
        .filter(|id| *id > 0)
        .ok_or_else(|| format!("tablet schema id is missing for tablet_id={tablet_id}"))?;
    let mut apply_version = base_version;
    for transaction in transactions {
        apply_version = apply_version.saturating_add(1);
        if transaction.txn_id == EMPTY_TXNLOG_TXN_ID {
            continue;
        }
        let logs = load_txn_logs_for_publish(
            &runtime.root_path,
            tablet_id,
            transaction,
            provider.as_ref(),
        )?;
        if logs.is_empty() {
            if transaction.force_publish {
                continue;
            }
            return Err(format!(
                "txn log not found for publish_version: tablet_id={} txn_id={} (single publish)",
                tablet_id, transaction.txn_id
            ));
        }
        for log in logs {
            apply_storage_txn_log_to_metadata(
                &mut metadata,
                &log.log,
                schema_id,
                &runtime.schema,
                &runtime.root_path,
                runtime.s3_config.as_ref(),
                apply_version,
            )?;
        }
    }
    metadata.id = Some(tablet_id);
    metadata.version = Some(new_version);
    if let Some(last) = transactions.last() {
        metadata.commit_time = last.commit_time;
        metadata.gtid = Some(last.gtid);
    }
    if metadata.next_rowset_id.is_none() {
        metadata.next_rowset_id = Some(next_rowset_id(&metadata.rowsets));
    }
    write_bundle_meta_file_with_provider(
        &runtime.root_path,
        tablet_id,
        new_version,
        &runtime.schema,
        &metadata,
        provider.as_ref(),
    )?;
    if transactions.len() == 1 && !transactions[0].combined_txn_log {
        let path = txn_log_file_path(&runtime.root_path, tablet_id, transactions[0].txn_id)?;
        let _ = delete_path_if_exists(&path);
    }
    schema::mark_be_txn_published(
        transactions
            .last()
            .map(|transaction| transaction.txn_id)
            .unwrap_or(0),
        tablet_id,
        unix_seconds_now(),
        new_version,
    );
    Ok(tablet_row_count(&metadata))
}

fn get_runtime_for_publish(
    tablet_id: i64,
    base_version: i64,
    provider: Arc<dyn StorageMetadataProvider>,
) -> Result<TabletRuntimeEntry, String> {
    if let Ok(runtime) = get_tablet_runtime(tablet_id) {
        return Ok(runtime);
    }

    let (root_path, s3_config) = resolve_tablet_location("publish_version", tablet_id)?;
    let recovery_version = if base_version > 0 { base_version } else { 1 };
    let metadata = load_tablet_metadata_at_version_with_provider(
        &root_path,
        tablet_id,
        recovery_version,
        provider.as_ref(),
    )?
    .ok_or_else(|| {
        format!(
            "publish_version could not recover tablet runtime from metadata: tablet_id={} base_version={}",
            tablet_id, base_version
        )
    })?;
    let schema = metadata.schema.ok_or_else(|| {
        format!(
            "publish_version recovered metadata without schema: tablet_id={} base_version={}",
            tablet_id, base_version
        )
    })?;
    cache_tablet_runtime(
        tablet_id,
        TabletRuntimeEntry {
            root_path,
            schema,
            s3_config,
            storage_metadata_provider: Some(provider),
        },
    )
}

pub fn execute_publish_log_version(
    dependencies: &LakeStorageDependencies,
    command: &PublishLogVersionCommand,
) -> Result<FailedTabletsResult, String> {
    let transaction = command
        .transaction
        .clone()
        .unwrap_or_else(|| LakeTransactionInfo {
            txn_id: command.transaction_id.unwrap_or(0),
            commit_time: None,
            combined_txn_log: false,
            transaction_type: LakeTransactionType::Normal,
            force_publish: false,
            rebuild_pindex: false,
            gtid: 0,
            load_ids: Vec::new(),
        });
    execute_publish_log_steps(
        dependencies,
        &command.tablet_ids,
        &[(
            transaction,
            command
                .version
                .ok_or_else(|| "publish_log_version missing version".to_string())?,
        )],
    )
}

pub fn execute_publish_log_version_batch(
    dependencies: &LakeStorageDependencies,
    command: &PublishLogVersionBatchCommand,
) -> Result<FailedTabletsResult, String> {
    if command.versions.is_empty() {
        return Err("publish_log_version_batch requires versions".to_string());
    }
    let transactions = if command.transactions.is_empty() {
        command
            .transaction_ids
            .iter()
            .map(|id| LakeTransactionInfo {
                txn_id: *id,
                commit_time: None,
                combined_txn_log: false,
                transaction_type: LakeTransactionType::Normal,
                force_publish: false,
                rebuild_pindex: false,
                gtid: 0,
                load_ids: Vec::new(),
            })
            .collect::<Vec<_>>()
    } else {
        command.transactions.clone()
    };
    if transactions.len() != command.versions.len() {
        return Err(format!(
            "publish_log_version_batch txn_infos/versions size mismatch: txn_infos={} versions={}",
            transactions.len(),
            command.versions.len()
        ));
    }
    let steps = transactions
        .into_iter()
        .zip(command.versions.iter().copied())
        .collect::<Vec<_>>();
    execute_publish_log_steps(dependencies, &command.tablet_ids, &steps)
}

fn execute_publish_log_steps(
    dependencies: &LakeStorageDependencies,
    tablet_ids: &[i64],
    steps: &[(LakeTransactionInfo, i64)],
) -> Result<FailedTabletsResult, String> {
    warmup_tablet_locations_for_dependencies(dependencies, "publish_log_version", tablet_ids);
    let provider = dependencies.storage_metadata()?;
    let mut failed_tablets = Vec::new();
    for tablet_id in tablet_ids {
        let result = (|| {
            if *tablet_id <= 0 {
                return Err(format!(
                    "publish_log_version has non-positive tablet_id={tablet_id}"
                ));
            }
            let runtime = get_tablet_runtime(*tablet_id)?;
            for (transaction, version) in steps {
                if *version <= 0 {
                    return Err(format!(
                        "publish_log_version requires positive version, got {version}"
                    ));
                }
                if transaction.txn_id <= 0 || !transaction.load_ids.is_empty() {
                    return Err(format!(
                        "publish_log_version does not support load_ids: tablet_id={} txn_id={} version={}",
                        tablet_id, transaction.txn_id, version
                    ));
                }
                let logs = load_txn_logs_for_publish(
                    &runtime.root_path,
                    *tablet_id,
                    transaction,
                    provider.as_ref(),
                )?;
                if logs.len() != 1 {
                    return Err(format!(
                        "publish_log_version expects exactly one txn log for combined txn: tablet_id={} txn_id={} version={} actual_logs={}",
                        tablet_id,
                        transaction.txn_id,
                        version,
                        logs.len()
                    ));
                }
                let path = txn_vlog_file_path(&runtime.root_path, *tablet_id, *version)?;
                if read_bytes_if_exists(&path)?.is_none() {
                    let bytes = provider.encode_transaction_log(&logs[0].log)?;
                    write_bytes(&path, bytes)?;
                }
            }
            Ok::<_, String>(())
        })();
        if let Err(error) = result {
            warn!(
                "publish_log_version failed: tablet_id={} error={}",
                tablet_id, error
            );
            failed_tablets.push(*tablet_id);
        }
    }
    failed_tablets.sort_unstable();
    Ok(FailedTabletsResult { failed_tablets })
}

pub fn execute_abort_txn(
    dependencies: &LakeStorageDependencies,
    command: &AbortTransactionCommand,
) -> Result<FailedTabletsResult, String> {
    warmup_tablet_locations_for_dependencies(dependencies, "abort_txn", &command.tablet_ids);
    if should_skip_abort_cleanup(command.skip_cleanup) || command.tablet_ids.is_empty() {
        return Ok(FailedTabletsResult::default());
    }
    let transactions = normalized_abort_transactions(command)?;
    let provider = dependencies.storage_metadata()?;
    let mut failed_tablets = Vec::new();
    let mut combined_logs = HashSet::new();
    for tablet_id in &command.tablet_ids {
        if abort_one_tablet(
            *tablet_id,
            &transactions,
            provider.as_ref(),
            &mut combined_logs,
        )
        .is_err()
        {
            failed_tablets.push(*tablet_id);
        } else {
            for transaction in &transactions {
                schema::abort_be_txn_active(transaction.txn_id, *tablet_id);
            }
        }
    }
    for path in combined_logs {
        let _ = delete_path_if_exists(&path);
    }
    Ok(FailedTabletsResult { failed_tablets })
}

fn normalized_abort_transactions(
    command: &AbortTransactionCommand,
) -> Result<Vec<LakeTransactionInfo>, String> {
    let transactions = if command.transactions.is_empty() {
        command
            .transaction_ids
            .iter()
            .enumerate()
            .map(|(index, id)| LakeTransactionInfo {
                txn_id: *id,
                commit_time: None,
                combined_txn_log: false,
                transaction_type: command
                    .transaction_types
                    .get(index)
                    .copied()
                    .unwrap_or(LakeTransactionType::Normal),
                force_publish: false,
                rebuild_pindex: false,
                gtid: 0,
                load_ids: Vec::new(),
            })
            .collect()
    } else {
        command.transactions.clone()
    };
    let mut seen = HashSet::new();
    for transaction in &transactions {
        if transaction.txn_id <= 0 {
            return Err(format!(
                "abort_txn txn_infos has non-positive txn_id={}",
                transaction.txn_id
            ));
        }
        if !seen.insert(transaction.txn_id) {
            return Err(format!(
                "abort_txn txn_infos has duplicate txn_id={}",
                transaction.txn_id
            ));
        }
    }
    Ok(transactions)
}

pub fn execute_drop_table(
    dependencies: &LakeStorageDependencies,
    command: &DropLakeTableCommand,
) -> Result<LakeOkResult, String> {
    let tablet_id = command
        .tablet_id
        .filter(|id| *id > 0)
        .ok_or_else(|| "drop_table missing tablet_id".to_string())?;
    warmup_tablet_locations_for_dependencies(dependencies, "drop_table", &[tablet_id]);
    let (path, s3) = match resolve_tablet_location("drop_table", tablet_id) {
        Ok((path, s3)) => (command.path.as_deref().unwrap_or(&path).to_string(), s3),
        Err(error) => (command.path.clone().ok_or(error)?, None),
    };
    remove_all(&path, s3.as_ref())?;
    let _ = remove_tablet_runtime(tablet_id);
    Ok(LakeOkResult)
}

pub fn execute_delete_data(
    dependencies: &LakeStorageDependencies,
    command: &DeleteDataCommand,
) -> Result<FailedTabletsResult, String> {
    warmup_tablet_locations_for_dependencies(dependencies, "delete_data", &command.tablet_ids);
    let txn_id = command
        .txn_id
        .ok_or_else(|| "delete_data missing txn_id".to_string())?;
    if txn_id <= 0 {
        return Err(format!("delete_data has non-positive txn_id={txn_id}"));
    }
    let predicate = command
        .delete_predicate
        .clone()
        .ok_or_else(|| "delete_data missing delete_predicate".to_string())?;
    let provider = dependencies.storage_metadata()?;
    let tablet_ids = normalized_tablet_ids("delete_data", &command.tablet_ids)?;
    let mut failed_tablets = Vec::new();
    for tablet_id in tablet_ids {
        let result = (|| {
            let (root, _) = resolve_tablet_location("delete_data", tablet_id)?;
            let path = txn_log_file_path(&root, tablet_id, txn_id)?;
            with_txn_log_append_lock(tablet_id, txn_id, || {
                let mut log =
                    read_transaction_log_if_exists_with_provider(&path, provider.as_ref())?
                        .unwrap_or_else(|| StorageTransactionLog {
                            tablet_id: Some(tablet_id),
                            txn_id: Some(txn_id),
                            ..StorageTransactionLog::default()
                        });
                if log.tablet_id != Some(tablet_id) || log.txn_id != Some(txn_id) {
                    return Err(format!(
                        "delete_data txn log tablet_id mismatch: expected={} actual={:?}",
                        tablet_id, log.tablet_id
                    ));
                }
                if log.compaction.is_some()
                    || log.schema_change.is_some()
                    || log.alter_metadata.is_some()
                    || log.replication.is_some()
                {
                    return Err(format!(
                        "delete_data does not support mixed txn log operation: tablet_id={} txn_id={}",
                        tablet_id, txn_id
                    ));
                }
                let write = log.write.get_or_insert_with(StorageWriteOperation::default);
                if write.schema_key.is_none() {
                    write.schema_key = command.schema_key.clone();
                }
                let rowset = write.rowset.get_or_insert_with(StorageRowset::default);
                if !rowset.segments.is_empty() || rowset.num_rows.unwrap_or(0) > 0 {
                    return Err(format!(
                        "delete_data found non-empty write rowset in same txn: tablet_id={} txn_id={} segments={} num_rows={}",
                        tablet_id,
                        txn_id,
                        rowset.segments.len(),
                        rowset.num_rows.unwrap_or(0)
                    ));
                }
                if let Some(existing) = rowset.delete_predicate.as_ref()
                    && existing != &predicate
                {
                    return Err(format!(
                        "delete_data conflicting delete_predicate in same txn: tablet_id={} txn_id={}",
                        tablet_id, txn_id
                    ));
                }
                rowset.delete_predicate = Some(predicate.clone());
                write_transaction_log_with_provider(&path, &log, provider.as_ref())
            })
        })();
        if let Err(error) = result {
            warn!(
                "delete_data failed to append txn log: tablet_id={}, txn_id={}, error={}",
                tablet_id, txn_id, error
            );
            failed_tablets.push(tablet_id);
        }
    }
    Ok(FailedTabletsResult { failed_tablets })
}

pub fn execute_delete_tablet(
    dependencies: &LakeStorageDependencies,
    command: &DeleteTabletsCommand,
) -> Result<FailedTabletsResult, String> {
    warmup_tablet_locations_for_dependencies(dependencies, "delete_tablet", &command.tablet_ids);
    let tablet_ids = normalized_tablet_ids("delete_tablet", &command.tablet_ids)?;
    let provider = dependencies.storage_metadata()?;
    let mut roots = Vec::<(String, Option<S3StoreConfig>, Vec<i64>)>::new();
    let mut failed_tablets = Vec::new();
    for tablet_id in tablet_ids {
        match resolve_tablet_location("delete_tablet", tablet_id) {
            Ok((root, s3)) => {
                if let Some((_, _, ids)) =
                    roots.iter_mut().find(|(existing_root, existing_s3, _)| {
                        existing_root == &root && existing_s3 == &s3
                    })
                {
                    ids.push(tablet_id);
                } else {
                    roots.push((root, s3, vec![tablet_id]));
                }
            }
            Err(error) => {
                warn!(
                    "delete_tablet failed to resolve tablet: tablet_id={} error={}",
                    tablet_id, error
                );
                failed_tablets.push(tablet_id);
            }
        }
    }
    for (root, s3, ids) in roots {
        if let Err(error) = delete_tablets_in_root(&root, &ids, s3.as_ref(), provider.as_ref()) {
            warn!(
                "delete_tablet cleanup failed: root_path={} tablet_ids={:?} error={}",
                root, ids, error
            );
            failed_tablets.extend(ids);
        } else {
            for tablet_id in ids {
                let _ = remove_tablet_runtime(tablet_id);
            }
        }
    }
    failed_tablets.sort_unstable();
    failed_tablets.dedup();
    Ok(FailedTabletsResult { failed_tablets })
}

pub fn execute_vacuum(
    dependencies: &LakeStorageDependencies,
    command: &VacuumCommand,
) -> Result<VacuumResult, String> {
    let tablet_min_versions = if command.tablet_min_versions.is_empty() {
        command
            .tablet_ids
            .iter()
            .map(|id| (*id, command.min_retain_version))
            .collect::<Vec<_>>()
    } else {
        command.tablet_min_versions.clone()
    };
    let tablet_ids = tablet_min_versions
        .iter()
        .map(|(id, _)| *id)
        .collect::<Vec<_>>();
    warmup_tablet_locations_for_dependencies(dependencies, "vacuum", &tablet_ids);
    let minimum = command
        .min_retain_version
        .or_else(|| {
            tablet_min_versions
                .iter()
                .filter_map(|(_, version)| *version)
                .min()
        })
        .unwrap_or(1);
    if minimum <= 0 {
        return Err(format!(
            "vacuum has non-positive min_retain_version={minimum}"
        ));
    }
    let mut vacuumed_files = 0_i64;
    let provider = dependencies.storage_metadata()?;
    let target = tablet_ids.iter().copied().collect::<HashSet<_>>();
    let mut roots = Vec::<(String, Option<S3StoreConfig>)>::new();
    for tablet_id in &tablet_ids {
        let (root, s3) = resolve_tablet_location("vacuum", *tablet_id)?;
        if !roots
            .iter()
            .any(|(existing_root, existing_s3)| existing_root == &root && existing_s3 == &s3)
        {
            roots.push((root, s3));
        }
    }
    for (root, s3) in roots {
        for version in list_bundle_versions(&root, s3.as_ref())? {
            if version >= minimum || command.retain_versions.contains(&version) {
                continue;
            }
            let Some(bundle) = load_bundle_file_with_provider(&root, version, provider.as_ref())?
            else {
                continue;
            };
            if bundle_is_fully_targeted(&bundle, &target) {
                let path = bundle_meta_file_path(&root, version)?;
                if read_bytes_if_exists(&path)?.is_some() {
                    delete_path_if_exists(&path)?;
                    vacuumed_files = vacuumed_files.saturating_add(1);
                }
            }
        }
    }
    if command.delete_txn_log.unwrap_or(false) {
        let active = command.min_active_txn_id.unwrap_or(0);
        if active > 0 {
            for tablet_id in &tablet_ids {
                let (root, _) = resolve_tablet_location("vacuum", *tablet_id)?;
                let path = txn_log_file_path(&root, *tablet_id, active.saturating_sub(1))?;
                if delete_path_if_exists(&path).is_ok() {
                    vacuumed_files += 1;
                }
            }
        }
    }
    Ok(VacuumResult {
        vacuumed_files,
        vacuumed_file_size: 0,
        vacuumed_version: minimum,
        tablet_min_versions: tablet_min_versions
            .into_iter()
            .map(|(id, version)| (id, version.unwrap_or(minimum).max(1)))
            .collect(),
        extra_file_size: 0,
    })
}

pub fn execute_get_tablet_stats(
    dependencies: &LakeStorageDependencies,
    command: &TabletStatsCommand,
) -> Result<TabletStatsResult, String> {
    if command.tablet_versions.is_empty() {
        return Err("get_tablet_stats missing tablet_infos".to_string());
    }
    let tablet_ids = command
        .tablet_versions
        .iter()
        .map(|entry| entry.tablet_id)
        .collect::<Vec<_>>();
    warmup_tablet_locations_for_dependencies(dependencies, "get_tablet_stats", &tablet_ids);
    let provider = dependencies.storage_metadata()?;
    let deadline = Instant::now()
        .checked_add(Duration::from_millis(
            command
                .timeout_ms
                .unwrap_or(DEFAULT_GET_TABLET_STATS_TIMEOUT_MS)
                .max(0) as u64,
        ))
        .unwrap_or_else(Instant::now);
    let mut tablet_stats = Vec::new();
    for entry in &command.tablet_versions {
        if Instant::now() >= deadline {
            break;
        }
        if entry.tablet_id <= 0 || entry.version <= 0 {
            continue;
        }
        let (root, _) = resolve_tablet_location("get_tablet_stats", entry.tablet_id)?;
        if let Some(metadata) = load_tablet_metadata_at_version_with_provider(
            &root,
            entry.tablet_id,
            entry.version,
            provider.as_ref(),
        )? {
            let num_rows = metadata
                .rowsets
                .iter()
                .map(|rowset| {
                    rowset
                        .num_rows
                        .unwrap_or(0)
                        .saturating_sub(rowset.num_dels.unwrap_or(0))
                        .max(0)
                })
                .sum();
            let data_size = metadata
                .rowsets
                .iter()
                .map(|rowset| rowset.data_size.unwrap_or(0).max(0))
                .sum();
            tablet_stats.push(TabletStat {
                tablet_id: entry.tablet_id,
                num_rows,
                data_size,
            });
        }
    }
    Ok(TabletStatsResult { tablet_stats })
}

fn normalized_tablet_ids(operation: &str, ids: &[i64]) -> Result<Vec<i64>, String> {
    if ids.is_empty() {
        return Err(format!("{operation} missing tablet_ids"));
    }
    let mut result = ids.to_vec();
    if let Some(id) = result.iter().find(|id| **id <= 0) {
        return Err(format!("{operation} has non-positive tablet_id={id}"));
    }
    result.sort_unstable();
    result.dedup();
    Ok(result)
}

fn next_rowset_id(rowsets: &[StorageRowset]) -> u32 {
    rowsets
        .iter()
        .filter_map(|rowset| rowset.id)
        .max()
        .map(|id| id.saturating_add(1))
        .unwrap_or(0)
}

fn tablet_row_count(metadata: &StorageTabletMetadata) -> i64 {
    metadata
        .rowsets
        .iter()
        .map(|rowset| rowset.num_rows.unwrap_or(0))
        .sum()
}

fn unix_seconds_now() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .ok()
        .and_then(|duration| i64::try_from(duration.as_secs()).ok())
        .unwrap_or(0)
}

fn remove_all(path: &str, s3_config: Option<&S3StoreConfig>) -> Result<(), String> {
    let access = resolve_tablet_root(path, s3_config)?;
    if matches!(access.scheme(), FsScheme::Hdfs) {
        return Err(format!(
            "drop_table does not support hdfs path yet: {}",
            path
        ));
    }
    let relative = access.single_relative_path()?.to_string();
    data_block_on(access.operator().remove_all(&relative))
        .map_err(|error| format!("drop_table runtime execution failed: {error}"))?
        .map_err(|error| format!("drop_table remove path failed: path={path}, error={error}"))
}

fn delete_tablets_in_root(
    root_path: &str,
    tablet_ids: &[i64],
    s3_config: Option<&S3StoreConfig>,
    provider: &dyn StorageMetadataProvider,
) -> Result<(), String> {
    let target = tablet_ids.iter().copied().collect::<HashSet<_>>();
    let mut delete_paths = HashSet::new();
    let mut unshared_data_files = HashSet::new();
    let mut all_bundle_tablets_are_targets = true;
    let mut saw_target_bundle = false;
    for version in list_bundle_versions(root_path, s3_config)? {
        let Some(bundle) = load_bundle_file_with_provider(root_path, version, provider)? else {
            continue;
        };
        let bundle_ids = bundle
            .tablet_metadata_pages
            .keys()
            .copied()
            .collect::<HashSet<_>>();
        if !bundle_ids.iter().any(|id| target.contains(id)) {
            continue;
        }
        saw_target_bundle = true;
        if !bundle_is_fully_targeted(&bundle, &target) {
            all_bundle_tablets_are_targets = false;
            continue;
        }
        let path = bundle_meta_file_path(root_path, version)?;
        delete_paths.insert(path);
        for tablet_id in &bundle_ids {
            let page = bundle
                .tablet_metadata_pages
                .get(tablet_id)
                .expect("bundle id has page");
            let metadata = provider.decode_tablet_metadata(page)?;
            for rowset in metadata.rowsets {
                for (index, segment) in rowset.segments.iter().enumerate() {
                    if !rowset.shared_segments.get(index).copied().unwrap_or(false) {
                        unshared_data_files.insert(segment.clone());
                    }
                }
            }
        }
    }
    if saw_target_bundle && all_bundle_tablets_are_targets {
        return remove_all(root_path, s3_config);
    }
    for name in unshared_data_files {
        delete_paths.insert(join_tablet_path(root_path, &format!("{DATA_DIR}/{name}"))?);
    }
    for path in delete_paths {
        delete_path_if_exists(&path)?;
    }
    Ok(())
}

fn bundle_is_fully_targeted(
    bundle: &crate::connector::starrocks::lake::storage_domain::StorageBundleFile,
    target: &HashSet<i64>,
) -> bool {
    !bundle.tablet_metadata_pages.is_empty()
        && bundle
            .tablet_metadata_pages
            .keys()
            .all(|tablet_id| target.contains(tablet_id))
}

fn list_bundle_versions(
    root_path: &str,
    s3_config: Option<&S3StoreConfig>,
) -> Result<Vec<i64>, String> {
    let meta_dir = join_tablet_path(root_path, META_DIR)?;
    let mut versions = list_directory_file_names(&meta_dir, s3_config)?
        .into_iter()
        .filter_map(|name| parse_bundle_version_from_meta_file_name(&name))
        .collect::<Vec<_>>();
    versions.sort_unstable();
    versions.dedup();
    Ok(versions)
}

fn list_directory_file_names(
    dir_path: &str,
    s3_config: Option<&S3StoreConfig>,
) -> Result<Vec<String>, String> {
    let access = resolve_tablet_root(dir_path, s3_config)?;
    match access.scheme() {
        FsScheme::Local => {
            let directory = std::path::PathBuf::from(dir_path);
            if !directory.exists() {
                return Ok(Vec::new());
            }
            let names = fs::read_dir(&directory)
                .map_err(|error| {
                    format!("read directory failed: path={}, error={}", dir_path, error)
                })?
                .filter_map(|entry| entry.ok())
                .filter_map(|entry| {
                    entry
                        .file_type()
                        .ok()
                        .filter(|kind| kind.is_file())
                        .map(|_| entry)
                })
                .filter_map(|entry| entry.file_name().to_str().map(str::to_owned))
                .collect::<Vec<_>>();
            Ok(names)
        }
        FsScheme::ObjectStore => {
            let operator = access.operator();
            let relative = access
                .single_relative_path()?
                .trim_end_matches('/')
                .to_string();
            let prefix = (!relative.is_empty())
                .then(|| format!("{relative}/"))
                .unwrap_or_default();
            data_block_on(async move {
                let mut lister = operator
                    .lister_with(&prefix)
                    .recursive(false)
                    .await
                    .map_err(|error| {
                        format!(
                            "list object-store directory failed: path={}, error={}",
                            dir_path, error
                        )
                    })?;
                let mut names = Vec::new();
                while let Some(entry) = lister.try_next().await.map_err(|error| {
                    format!(
                        "iterate object-store directory failed: path={}, error={}",
                        dir_path, error
                    )
                })? {
                    let path = entry.path().trim_end_matches('/');
                    if let Some(name) = path.rsplit('/').next().filter(|name| !name.is_empty()) {
                        names.push(name.to_string());
                    }
                }
                Ok(names)
            })
            .map_err(|error| {
                format!("list object-store directory runtime execution failed: {error}")
            })?
        }
        FsScheme::Hdfs => Err(format!(
            "txn log directory listing does not support hdfs path yet: {dir_path}"
        )),
    }
}

pub(crate) fn warmup_tablet_locations_for_dependencies(
    dependencies: &LakeStorageDependencies,
    operation: &str,
    tablet_ids: &[i64],
) {
    let mut missing = tablet_ids
        .iter()
        .copied()
        .filter(|id| *id > 0 && get_tablet_runtime(*id).is_err())
        .collect::<Vec<_>>();
    missing.sort_unstable();
    missing.dedup();
    if missing.is_empty() {
        return;
    }
    let Ok(provider) = dependencies.starlet_metadata() else {
        return;
    };
    match provider.retrieve_shard_infos(&missing) {
        Ok(infos) => {
            starlet_shard_registry::upsert_many_infos(infos);
        }
        Err(error) => warn!(
            "{} tablet location warmup from Starlet metadata failed: missing={:?}, error={}",
            operation, missing, error
        ),
    }
}

pub(crate) fn resolve_tablet_location(
    operation: &str,
    tablet_id: i64,
) -> Result<(String, Option<S3StoreConfig>), String> {
    match get_tablet_runtime(tablet_id) {
        Ok(runtime) => Ok((runtime.root_path, runtime.s3_config)),
        Err(runtime_error) => {
            let mut infos = starlet_shard_registry::select_infos(&[tablet_id]);
            infos.remove(&tablet_id).map(|info| (info.full_path, info.s3)).ok_or_else(|| format!("{operation} missing tablet runtime for tablet_id={tablet_id}: {runtime_error}"))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::connector::starrocks::lake::storage_domain::{
        StorageBundleFile, StorageBundleMetadata, StorageCombinedTransactionLog,
    };
    use crate::connector::starrocks::schema::StarRocksTabletSchema;
    use std::collections::HashMap;
    use std::sync::atomic::{AtomicU64, Ordering};

    struct BundleProvider {
        includes_alive_tablet: bool,
    }

    impl StorageMetadataProvider for BundleProvider {
        fn encode_tablet_schema(&self, _: &StarRocksTabletSchema) -> Result<Vec<u8>, String> {
            Err("not used".to_string())
        }

        fn decode_tablet_schema(&self, _: &[u8]) -> Result<StarRocksTabletSchema, String> {
            Err("not used".to_string())
        }

        fn decode_tablet_metadata(&self, bytes: &[u8]) -> Result<StorageTabletMetadata, String> {
            let segment = std::str::from_utf8(bytes).map_err(|error| error.to_string())?;
            Ok(StorageTabletMetadata {
                rowsets: vec![StorageRowset {
                    segments: vec![segment.to_string()],
                    shared_segments: vec![false],
                    ..StorageRowset::default()
                }],
                ..StorageTabletMetadata::default()
            })
        }

        fn encode_tablet_metadata(&self, _: &StorageTabletMetadata) -> Result<Vec<u8>, String> {
            Err("not used".to_string())
        }

        fn decode_bundle_metadata(&self, _: &[u8]) -> Result<StorageBundleMetadata, String> {
            Err("not used".to_string())
        }

        fn decode_bundle_file(&self, _: &[u8]) -> Result<StorageBundleFile, String> {
            let mut pages = HashMap::from([(1, b"target.segment".to_vec())]);
            if self.includes_alive_tablet {
                pages.insert(2, b"alive.segment".to_vec());
            }
            Ok(StorageBundleFile {
                tablet_metadata_pages: pages,
                ..StorageBundleFile::default()
            })
        }

        fn encode_bundle_file(&self, _: &StorageBundleFile) -> Result<Vec<u8>, String> {
            Err("not used".to_string())
        }

        fn rewrite_tablet_metadata_version(&self, _: &[u8], _: i64) -> Result<Vec<u8>, String> {
            Err("not used".to_string())
        }

        fn decode_transaction_log(&self, _: &[u8]) -> Result<StorageTransactionLog, String> {
            Err("not used".to_string())
        }

        fn encode_transaction_log(&self, _: &StorageTransactionLog) -> Result<Vec<u8>, String> {
            Err("not used".to_string())
        }

        fn decode_combined_transaction_log(
            &self,
            _: &[u8],
        ) -> Result<StorageCombinedTransactionLog, String> {
            Err("not used".to_string())
        }

        fn encode_combined_transaction_log(
            &self,
            _: &StorageCombinedTransactionLog,
        ) -> Result<Vec<u8>, String> {
            Err("not used".to_string())
        }
    }

    fn test_root() -> String {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let unique = NEXT.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "novarocks-rci-6a-transactions-{}-{unique}",
            std::process::id()
        ));
        std::fs::create_dir_all(&path).expect("create test root");
        path.to_string_lossy().into_owned()
    }

    fn write_bundle(root: &str, version: i64) -> String {
        let path = bundle_meta_file_path(root, version).expect("bundle path");
        write_bytes(&path, b"bundle".to_vec()).expect("write bundle");
        path
    }

    #[test]
    fn partial_target_bundle_keeps_shared_bundle_page_and_segments() {
        let root = test_root();
        let bundle_path = write_bundle(&root, 1);
        let segment = join_tablet_path(&root, "data/target.segment").expect("segment path");
        write_bytes(&segment, b"segment".to_vec()).expect("write segment");

        delete_tablets_in_root(
            &root,
            &[1],
            None,
            &BundleProvider {
                includes_alive_tablet: true,
            },
        )
        .expect("partial delete");

        assert!(std::path::Path::new(&bundle_path).exists());
        assert!(std::path::Path::new(&segment).exists());
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn fully_targeted_bundle_removes_tablet_root() {
        let root = test_root();
        let _ = write_bundle(&root, 1);
        let segment = join_tablet_path(&root, "data/target.segment").expect("segment path");
        write_bytes(&segment, b"segment".to_vec()).expect("write segment");

        let provider = BundleProvider {
            includes_alive_tablet: false,
        };
        let bundle = load_bundle_file_with_provider(&root, 1, &provider)
            .expect("load bundle")
            .expect("bundle exists");
        assert!(bundle_is_fully_targeted(&bundle, &HashSet::from([1])));
        delete_tablets_in_root(&root, &[1], None, &provider).expect("full delete");

        assert!(!std::path::Path::new(&root).exists());
    }

    #[test]
    fn vacuum_keeps_partial_bundle_ownership() {
        let root = test_root();
        let bundle_path = write_bundle(&root, 1);
        let provider = BundleProvider {
            includes_alive_tablet: true,
        };
        let bundle = load_bundle_file_with_provider(&root, 1, &provider)
            .expect("load bundle")
            .expect("bundle exists");
        assert!(bundle.tablet_metadata_pages.contains_key(&2));
        assert!(!bundle_is_fully_targeted(&bundle, &HashSet::from([1])));
        assert!(std::path::Path::new(&bundle_path).exists());
        let _ = std::fs::remove_dir_all(root);
    }
}
