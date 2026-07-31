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

use std::collections::HashSet;

use crate::connector::starrocks::lake::service_domain::LakeTransactionInfo;
use crate::connector::starrocks::lake::storage_domain::StorageTransactionLog;
use crate::connector::starrocks::ports::StorageMetadataProvider;
use crate::formats::starrocks::writer::io::{
    read_combined_transaction_log_if_exists_with_provider,
    read_transaction_log_if_exists_with_provider,
};
use crate::formats::starrocks::writer::layout::{
    combined_txn_log_file_path, txn_log_file_path, txn_log_file_path_with_load_id,
    txn_vlog_file_path,
};

#[derive(Clone)]
pub(crate) struct LoadedTxnLog {
    pub(crate) log: StorageTransactionLog,
}

/// Loads persisted transaction facts only through the explicit storage codec.
/// The combined-log fallback is intentionally retained: some FE plans declare
/// a combined log while the sink has written per-tablet logs.
pub(crate) fn load_txn_logs_for_publish(
    tablet_root_path: &str,
    tablet_id: i64,
    txn_info: &LakeTransactionInfo,
    storage_metadata: &dyn StorageMetadataProvider,
) -> Result<Vec<LoadedTxnLog>, String> {
    let txn_id = txn_info.txn_id;
    if !txn_info.load_ids.is_empty() {
        let mut logs = Vec::with_capacity(txn_info.load_ids.len());
        let mut seen_paths = HashSet::with_capacity(txn_info.load_ids.len());
        for load_id in &txn_info.load_ids {
            let path =
                txn_log_file_path_with_load_id(tablet_root_path, tablet_id, txn_id, load_id)?;
            if !seen_paths.insert(path.clone()) {
                continue;
            }
            if let Some(txn_log) =
                read_transaction_log_if_exists_with_provider(&path, storage_metadata)?
            {
                logs.push(LoadedTxnLog { log: txn_log });
            }
        }
        if !logs.is_empty() {
            return Ok(logs);
        }
        let fallback_path = txn_log_file_path(tablet_root_path, tablet_id, txn_id)?;
        if let Some(txn_log) =
            read_transaction_log_if_exists_with_provider(&fallback_path, storage_metadata)?
        {
            return Ok(vec![LoadedTxnLog { log: txn_log }]);
        }
        return Ok(Vec::new());
    }

    if txn_info.combined_txn_log {
        let combined_path = combined_txn_log_file_path(tablet_root_path, txn_id)?;
        if let Some(combined_log) =
            read_combined_transaction_log_if_exists_with_provider(&combined_path, storage_metadata)?
        {
            let logs = combined_log
                .transaction_logs
                .into_iter()
                .filter(|log| log.tablet_id == Some(tablet_id))
                .map(|log| LoadedTxnLog { log })
                .collect::<Vec<_>>();
            if !logs.is_empty() {
                return Ok(logs);
            }
        }

        let tablet_path = txn_log_file_path(tablet_root_path, tablet_id, txn_id)?;
        if let Some(txn_log) =
            read_transaction_log_if_exists_with_provider(&tablet_path, storage_metadata)?
        {
            return Ok(vec![LoadedTxnLog { log: txn_log }]);
        }
        return Ok(Vec::new());
    }

    let path = txn_log_file_path(tablet_root_path, tablet_id, txn_id)?;
    Ok(
        read_transaction_log_if_exists_with_provider(&path, storage_metadata)?
            .map(|log| vec![LoadedTxnLog { log }])
            .unwrap_or_default(),
    )
}

pub(crate) fn load_txn_vlog_for_publish(
    tablet_root_path: &str,
    tablet_id: i64,
    version: i64,
    storage_metadata: &dyn StorageMetadataProvider,
) -> Result<Option<LoadedTxnLog>, String> {
    if version <= 0 {
        return Err(format!(
            "publish_version requires positive vlog version, got {version}"
        ));
    }
    let path = txn_vlog_file_path(tablet_root_path, tablet_id, version)?;
    Ok(
        read_transaction_log_if_exists_with_provider(&path, storage_metadata)?
            .map(|log| LoadedTxnLog { log }),
    )
}
