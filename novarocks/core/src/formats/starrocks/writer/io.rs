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

use std::fs;
use std::path::PathBuf;

use crate::connector::starrocks::lake::storage_domain::{
    StorageCombinedTransactionLog, StorageTransactionLog,
};
use crate::connector::starrocks::ports::StorageMetadataProvider;
use crate::formats::starrocks::fs_access::resolve_format_path;
use novarocks_fs::FsScheme;
use opendal::ErrorKind;

pub fn write_bytes(path: &str, bytes: Vec<u8>) -> Result<(), String> {
    reject_hdfs_path(path, "write_bytes")?;
    let access = resolve_format_path(path)?;
    match access.scheme() {
        FsScheme::Local => {
            let path_buf = PathBuf::from(path);
            if let Some(parent) = path_buf.parent() {
                fs::create_dir_all(parent)
                    .map_err(|e| format!("create parent dir failed: {}", e))?;
            }
            fs::write(path_buf, bytes).map_err(|e| format!("write file failed: {}", e))
        }
        FsScheme::ObjectStore => {
            let rel = access.single_relative_path()?.to_string();
            let write_result =
                crate::fs::object_store::oss_block_on(access.operator().write(&rel, bytes))?;
            write_result.map_err(|e| format!("write object failed: {}", e))?;
            Ok(())
        }
        FsScheme::Hdfs => Err(format!(
            "write_bytes does not support hdfs path yet: {}",
            path
        )),
    }
}

#[allow(dead_code)]
pub fn read_bytes(path: &str) -> Result<Vec<u8>, String> {
    reject_hdfs_path(path, "read_bytes")?;
    let access = resolve_format_path(path)?;
    match access.scheme() {
        FsScheme::Local => fs::read(path).map_err(|e| format!("read file failed: {}", e)),
        FsScheme::ObjectStore => {
            let rel = access.single_relative_path()?.to_string();
            let read_result = crate::fs::object_store::oss_block_on(access.operator().read(&rel))?;
            let bytes = read_result.map_err(|e| format!("read object failed: {}", e))?;
            Ok(bytes.to_vec())
        }
        FsScheme::Hdfs => Err(format!(
            "read_bytes does not support hdfs path yet: {}",
            path
        )),
    }
}

pub fn read_bytes_if_exists(path: &str) -> Result<Option<Vec<u8>>, String> {
    reject_hdfs_path(path, "read_bytes_if_exists")?;
    let access = resolve_format_path(path)?;
    match access.scheme() {
        FsScheme::Local => {
            let path_buf = PathBuf::from(path);
            if !path_buf.exists() {
                return Ok(None);
            }
            fs::read(path_buf)
                .map(Some)
                .map_err(|e| format!("read file failed: {}", e))
        }
        FsScheme::ObjectStore => {
            let rel = access.single_relative_path()?.to_string();
            match crate::fs::object_store::oss_block_on(access.operator().read(&rel))? {
                Ok(bytes) => Ok(Some(bytes.to_vec())),
                Err(e) if e.kind() == ErrorKind::NotFound => Ok(None),
                Err(e) => Err(format!("read object failed: {}", e)),
            }
        }
        FsScheme::Hdfs => Err(format!(
            "read_bytes_if_exists does not support hdfs path yet: {}",
            path
        )),
    }
}

/// Writes a transaction log through the explicitly installed storage wire
/// provider. The lake kernel only passes protocol-neutral facts here; compat
/// owns protobuf encoding at the file boundary.
pub fn write_transaction_log_with_provider(
    path: &str,
    log: &StorageTransactionLog,
    provider: &dyn StorageMetadataProvider,
) -> Result<(), String> {
    let bytes = provider
        .encode_transaction_log(log)
        .map_err(|error| format!("encode StarRocks transaction log failed: {error}"))?;
    write_bytes(path, bytes)
}

/// Reads a transaction log through the explicitly installed storage wire
/// provider. A missing file remains distinguishable from an invalid payload.
pub fn read_transaction_log_if_exists_with_provider(
    path: &str,
    provider: &dyn StorageMetadataProvider,
) -> Result<Option<StorageTransactionLog>, String> {
    let Some(bytes) = read_bytes_if_exists(path)? else {
        return Ok(None);
    };
    provider
        .decode_transaction_log(&bytes)
        .map(Some)
        .map_err(|error| format!("decode StarRocks transaction log failed: {error}"))
}

/// Writes a combined transaction log through the explicitly installed storage
/// wire provider.
pub fn write_combined_transaction_log_with_provider(
    path: &str,
    log: &StorageCombinedTransactionLog,
    provider: &dyn StorageMetadataProvider,
) -> Result<(), String> {
    let bytes = provider
        .encode_combined_transaction_log(log)
        .map_err(|error| format!("encode StarRocks combined transaction log failed: {error}"))?;
    write_bytes(path, bytes)
}

/// Reads a combined transaction log through the explicitly installed storage
/// wire provider. A missing file remains distinguishable from an invalid
/// payload.
pub fn read_combined_transaction_log_if_exists_with_provider(
    path: &str,
    provider: &dyn StorageMetadataProvider,
) -> Result<Option<StorageCombinedTransactionLog>, String> {
    let Some(bytes) = read_bytes_if_exists(path)? else {
        return Ok(None);
    };
    provider
        .decode_combined_transaction_log(&bytes)
        .map(Some)
        .map_err(|error| format!("decode StarRocks combined transaction log failed: {error}"))
}

pub fn delete_path_if_exists(path: &str) -> Result<(), String> {
    reject_hdfs_path(path, "delete_path_if_exists")?;
    let access = resolve_format_path(path)?;
    match access.scheme() {
        FsScheme::Local => {
            let path_buf = PathBuf::from(path);
            if !path_buf.exists() {
                return Ok(());
            }
            fs::remove_file(path_buf).map_err(|e| format!("delete file failed: {}", e))
        }
        FsScheme::ObjectStore => {
            let rel = access.single_relative_path()?.to_string();
            match crate::fs::object_store::oss_block_on(access.operator().delete(&rel))? {
                Ok(_) => Ok(()),
                Err(e) if e.kind() == ErrorKind::NotFound => Ok(()),
                Err(e) => Err(format!("delete object failed: {}", e)),
            }
        }
        FsScheme::Hdfs => Err(format!(
            "delete_path_if_exists does not support hdfs path yet: {}",
            path
        )),
    }
}

fn reject_hdfs_path(path: &str, function_name: &str) -> Result<(), String> {
    let trimmed = path.trim();
    if trimmed
        .split_once("://")
        .is_some_and(|(scheme, _)| scheme.eq_ignore_ascii_case("hdfs"))
    {
        return Err(format!(
            "{function_name} does not support hdfs path yet: {path}"
        ));
    }
    Ok(())
}
