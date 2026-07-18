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
use std::collections::HashMap;
use std::sync::{Mutex, OnceLock};
use std::time::Duration;

use tokio::time::{Instant, sleep};
use tonic::transport::{Channel, Endpoint};

use crate::proto::staros;
use crate::runtime::global_async_runtime::data_block_on;
use crate::runtime::starlet_shard_registry::{self, S3StoreConfig, StarletShardInfo};

const STARMGR_SERVICE_NAME: &str = "starrocks";
const DEFAULT_WORKER_GROUP_ID: u64 = 0;
const GRPC_CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
const GRPC_REQUEST_TIMEOUT: Duration = Duration::from_secs(10);
const GRPC_MAX_MESSAGE_BYTES: usize = 64 * 1024 * 1024;
const STARMGR_LEADER_WAIT_TIMEOUT: Duration = Duration::from_secs(5);
const STARMGR_LEADER_WAIT_INTERVAL: Duration = Duration::from_millis(10);

#[derive(Clone, Debug, Default)]
struct StarMgrState {
    leader_addr: Option<String>,
    service_id: Option<String>,
    worker_group_id: Option<u64>,
    worker_id: Option<u64>,
}

#[derive(Clone, Debug)]
struct ResolvedRouting {
    leader_addr: String,
    service_id: String,
    worker_group_id: u64,
}

#[derive(Default)]
struct ChannelCache {
    mu: Mutex<HashMap<String, Channel>>,
}

static STARMGR_STATE: OnceLock<Mutex<StarMgrState>> = OnceLock::new();
static CHANNEL_CACHE: OnceLock<ChannelCache> = OnceLock::new();

fn state() -> &'static Mutex<StarMgrState> {
    STARMGR_STATE.get_or_init(|| Mutex::new(StarMgrState::default()))
}

fn channels() -> &'static ChannelCache {
    CHANNEL_CACHE.get_or_init(|| ChannelCache {
        mu: Mutex::new(HashMap::new()),
    })
}

fn normalize_optional_text(value: &str) -> Option<String> {
    let trimmed = value.trim();
    (!trimmed.is_empty()).then_some(trimmed.to_string())
}

fn parse_bucket_from_full_path(full_path: &str) -> Option<String> {
    let trimmed = full_path.trim();
    for scheme in ["s3://", "oss://"] {
        if let Some(rest) = trimmed.strip_prefix(scheme) {
            let bucket = rest.split('/').next().unwrap_or_default().trim();
            if !bucket.is_empty() {
                return Some(bucket.to_string());
            }
        }
    }
    None
}

fn split_object_store_path(path: &str) -> Option<(String, String)> {
    let trimmed = path.trim();
    for scheme in ["s3://", "oss://"] {
        if let Some(rest) = trimmed.strip_prefix(scheme) {
            let mut parts = rest.splitn(2, '/');
            let bucket = parts.next()?.trim();
            if bucket.is_empty() {
                return None;
            }
            let key = parts
                .next()
                .unwrap_or_default()
                .trim_matches('/')
                .to_string();
            return Some((bucket.to_string(), key));
        }
    }
    None
}

fn key_matches_root(key: &str, root: &str) -> bool {
    let root = root.trim_matches('/');
    if root.is_empty() {
        return true;
    }
    key == root
        || key
            .strip_prefix(root)
            .is_some_and(|rest| rest.starts_with('/'))
}

fn parse_s3_credential(
    credential: Option<&staros::AwsCredentialInfo>,
    endpoint: &str,
    bucket: &str,
    context_path: &str,
) -> Result<(String, String), String> {
    let credential = credential.ok_or_else(|| {
        format!(
            "StarOS S3 credential is missing: endpoint={}, bucket={}, path={}",
            endpoint, bucket, context_path
        )
    })?;
    match credential.credential.as_ref() {
        Some(staros::aws_credential_info::Credential::SimpleCredential(simple)) => {
            let ak = simple.access_key.trim();
            let sk = simple.access_key_secret.trim();
            if ak.is_empty() || sk.is_empty() {
                return Err(format!(
                    "StarOS S3 simple credential contains empty access key/secret: endpoint={}, bucket={}, path={}",
                    endpoint, bucket, context_path
                ));
            }
            Ok((ak.to_string(), sk.to_string()))
        }
        Some(staros::aws_credential_info::Credential::DefaultCredential(_)) => Err(format!(
            "StarOS S3 default credential is not supported yet: endpoint={}, bucket={}, path={}",
            endpoint, bucket, context_path
        )),
        Some(staros::aws_credential_info::Credential::ProfileCredential(_)) => Err(format!(
            "StarOS S3 instance-profile credential is not supported yet: endpoint={}, bucket={}, path={}",
            endpoint, bucket, context_path
        )),
        Some(staros::aws_credential_info::Credential::AssumeRoleCredential(_)) => Err(format!(
            "StarOS S3 assume-role credential is not supported yet: endpoint={}, bucket={}, path={}",
            endpoint, bucket, context_path
        )),
        None => Err(format!(
            "StarOS S3 credential mode is missing: endpoint={}, bucket={}, path={}",
            endpoint, bucket, context_path
        )),
    }
}

fn normalize_region(region: &str) -> Option<String> {
    let region = region
        .trim()
        .split_once('\0')
        .map(|(v, _)| v)
        .unwrap_or(region)
        .trim();
    (!region.is_empty()).then_some(region.to_string())
}

pub(crate) fn parse_s3_config_from_file_path_info(
    path_info: &staros::FilePathInfo,
) -> Result<Option<S3StoreConfig>, String> {
    let Some(fs_info) = path_info.fs_info.as_ref() else {
        return Ok(None);
    };
    if fs_info.fs_type != staros::FileStoreType::S3 as i32 {
        return Ok(None);
    }

    let s3 = fs_info
        .s3_fs_info
        .as_ref()
        .ok_or_else(|| "StarOS fs_type=S3 but s3_fs_info is missing".to_string())?;
    let endpoint = s3.endpoint.trim();
    if endpoint.is_empty() {
        return Err("StarOS S3 endpoint is empty".to_string());
    }

    let bucket = if s3.bucket.trim().is_empty() {
        parse_bucket_from_full_path(&path_info.full_path).ok_or_else(|| {
            "StarOS S3 bucket is empty and cannot be inferred from full_path".to_string()
        })?
    } else {
        s3.bucket.trim().to_string()
    };
    // `s3.path_prefix` is the FileStore-level root, not a tablet root. It is
    // intentionally dropped here: the tablet's location lives in `full_path`
    // and the OpenDAL operator is built from the bucket. Carrying it into
    // S3StoreConfig caused stale roots to be re-attached to unrelated tablet
    // paths during recovery flows.
    let (access_key_id, access_key_secret) = parse_s3_credential(
        s3.credential.as_ref(),
        endpoint,
        &bucket,
        &path_info.full_path,
    )?;

    let enable_path_style_access = match s3.path_style_access {
        1 => Some(true),
        2 => Some(false),
        _ => None,
    };
    let region = normalize_region(&s3.region);

    Ok(Some(S3StoreConfig {
        endpoint: endpoint.to_string(),
        bucket,
        access_key_id,
        access_key_secret,
        region,
        enable_path_style_access,
    }))
}

/// Parsed StarOS FileStore record used by `retrieve_s3_config_for_path_async`
/// to pick the file store whose configured path_prefix actually covers the
/// requested object key. `path_prefix` stays local to that matching step and
/// never enters [`S3StoreConfig`].
struct FileStoreS3Record {
    cfg: S3StoreConfig,
    path_prefix: String,
}

fn parse_s3_config_from_file_store_info(
    fs_info: &staros::FileStoreInfo,
) -> Result<Option<FileStoreS3Record>, String> {
    if fs_info.fs_type != staros::FileStoreType::S3 as i32 {
        return Ok(None);
    }

    let s3 = fs_info
        .s3_fs_info
        .as_ref()
        .ok_or_else(|| "StarOS file store fs_type=S3 but s3_fs_info is missing".to_string())?;
    let endpoint = s3.endpoint.trim();
    if endpoint.is_empty() {
        return Err("StarOS file store S3 endpoint is empty".to_string());
    }
    let bucket = s3.bucket.trim();
    if bucket.is_empty() {
        return Err(format!(
            "StarOS file store S3 bucket is empty: fs_key={}, fs_name={}",
            fs_info.fs_key, fs_info.fs_name
        ));
    }
    let (access_key_id, access_key_secret) = parse_s3_credential(
        s3.credential.as_ref(),
        endpoint,
        bucket,
        &format!("fs_key={} fs_name={}", fs_info.fs_key, fs_info.fs_name),
    )?;
    let enable_path_style_access = match s3.path_style_access {
        1 => Some(true),
        2 => Some(false),
        _ => None,
    };

    Ok(Some(FileStoreS3Record {
        cfg: S3StoreConfig {
            endpoint: endpoint.to_string(),
            bucket: bucket.to_string(),
            access_key_id,
            access_key_secret,
            region: normalize_region(&s3.region),
            enable_path_style_access,
        },
        path_prefix: s3.path_prefix.trim_matches('/').to_string(),
    }))
}

fn shard_info_to_registry_info(
    shard: &staros::ShardInfo,
) -> Result<(i64, StarletShardInfo), String> {
    let tablet_id = i64::try_from(shard.shard_id).map_err(|_| {
        format!(
            "StarManager returned shard_id out of i64 range: {}",
            shard.shard_id
        )
    })?;
    let path_info = shard.file_path.as_ref().ok_or_else(|| {
        format!("StarManager GetShard missing file_path for shard_id={tablet_id}")
    })?;
    let full_path = path_info.full_path.trim();
    if full_path.is_empty() {
        return Err(format!(
            "StarManager GetShard returned empty full_path for shard_id={tablet_id}"
        ));
    }
    let s3 = parse_s3_config_from_file_path_info(path_info)?;
    Ok((
        tablet_id,
        StarletShardInfo {
            full_path: full_path.to_string(),
            s3,
        },
    ))
}

pub(crate) fn observe_starlet_service(service_id: &str) {
    let Some(service_id) = normalize_optional_text(service_id) else {
        return;
    };
    if let Ok(mut guard) = state().lock() {
        guard.service_id = Some(service_id);
    }
}

pub(crate) fn observe_starlet_heartbeat(
    star_mgr_leader: &str,
    service_id: &str,
    worker_group_id: u64,
    worker_id: u64,
) {
    if let Ok(mut guard) = state().lock() {
        if let Some(leader_addr) = normalize_optional_text(star_mgr_leader) {
            guard.leader_addr = Some(leader_addr);
        }
        if let Some(service_id) = normalize_optional_text(service_id) {
            guard.service_id = Some(service_id);
        }
        guard.worker_group_id = Some(worker_group_id);
        guard.worker_id = Some(worker_id);
    }
}

fn current_state_snapshot() -> StarMgrState {
    state()
        .lock()
        .map(|guard| guard.clone())
        .unwrap_or_default()
}

async fn wait_for_routing_snapshot() -> Result<StarMgrState, String> {
    let deadline = Instant::now() + STARMGR_LEADER_WAIT_TIMEOUT;
    loop {
        let snapshot = current_state_snapshot();
        if snapshot
            .leader_addr
            .as_ref()
            .is_some_and(|leader| !leader.trim().is_empty())
        {
            return Ok(snapshot);
        }
        if Instant::now() >= deadline {
            return Err(format!(
                "StarManager leader is unknown after waiting {:?}; waiting for StarletHeartbeat to provide star_mgr_leader",
                STARMGR_LEADER_WAIT_TIMEOUT
            ));
        }
        sleep(STARMGR_LEADER_WAIT_INTERVAL).await;
    }
}

fn update_service_id_cache(service_id: String) {
    if let Ok(mut guard) = state().lock() {
        guard.service_id = Some(service_id);
    }
}

async fn get_channel(leader_addr: &str) -> Result<Channel, String> {
    if let Some(ch) = channels()
        .mu
        .lock()
        .map_err(|_| "lock StarManager channel cache failed".to_string())?
        .get(leader_addr)
        .cloned()
    {
        return Ok(ch);
    }

    let endpoint = format!("http://{leader_addr}")
        .parse::<Endpoint>()
        .map_err(|e| format!("invalid StarManager endpoint '{leader_addr}': {e}"))?
        .connect_timeout(GRPC_CONNECT_TIMEOUT)
        .timeout(GRPC_REQUEST_TIMEOUT)
        .tcp_keepalive(Some(Duration::from_secs(60)));
    let channel = endpoint
        .connect()
        .await
        .map_err(|e| format!("connect StarManager leader {leader_addr} failed: {e}"))?;
    channels()
        .mu
        .lock()
        .map_err(|_| "lock StarManager channel cache failed".to_string())?
        .insert(leader_addr.to_string(), channel.clone());
    Ok(channel)
}

fn require_ok_status(status: Option<&staros::StarStatus>, op: &str) -> Result<(), String> {
    // StarManager uses proto3 responses. Successful replies may omit the zero-value `status`
    // message entirely, so treat missing status as implicit OK instead of a protocol error.
    let Some(status) = status else {
        return Ok(());
    };
    if status.status_code == staros::StatusCode::Ok as i32 {
        return Ok(());
    }
    let code = staros::StatusCode::try_from(status.status_code)
        .map(|v| format!("{v:?}"))
        .unwrap_or_else(|_| status.status_code.to_string());
    if status.error_msg.trim().is_empty() {
        Err(format!("StarManager {op} failed with status_code={code}"))
    } else {
        Err(format!(
            "StarManager {op} failed with status_code={code}: {}",
            status.error_msg
        ))
    }
}

async fn resolve_routing() -> Result<ResolvedRouting, String> {
    let snapshot = wait_for_routing_snapshot().await?;
    let leader_addr = snapshot
        .leader_addr
        .filter(|leader| !leader.trim().is_empty())
        .ok_or_else(|| {
            "StarManager leader is unknown; waiting for StarletHeartbeat to provide star_mgr_leader"
                .to_string()
        })?;

    let service_id = match snapshot.service_id {
        Some(service_id) if !service_id.trim().is_empty() => service_id,
        _ => {
            let channel = get_channel(&leader_addr).await?;
            let mut client = staros::star_manager_client::StarManagerClient::new(channel)
                .max_decoding_message_size(GRPC_MAX_MESSAGE_BYTES)
                .max_encoding_message_size(GRPC_MAX_MESSAGE_BYTES);
            let response = client
                .get_service(staros::GetServiceRequest {
                    identifier: Some(staros::get_service_request::Identifier::ServiceName(
                        STARMGR_SERVICE_NAME.to_string(),
                    )),
                })
                .await
                .map_err(|e| format!("StarManager GetService failed: {e}"))?;
            let response = response.into_inner();
            require_ok_status(response.status.as_ref(), "GetService")?;
            let service_info = response
                .service_info
                .ok_or_else(|| "StarManager GetService missing service_info".to_string())?;
            if service_info.service_id.trim().is_empty() {
                return Err("StarManager GetService returned empty service_id".to_string());
            }
            update_service_id_cache(service_info.service_id.clone());
            service_info.service_id
        }
    };

    Ok(ResolvedRouting {
        leader_addr,
        service_id,
        worker_group_id: snapshot.worker_group_id.unwrap_or(DEFAULT_WORKER_GROUP_ID),
    })
}

async fn retrieve_shard_infos_async(
    tablet_ids: &[i64],
) -> Result<HashMap<i64, StarletShardInfo>, String> {
    let routing = resolve_routing().await?;
    let shard_ids = tablet_ids
        .iter()
        .copied()
        .map(|tablet_id| {
            u64::try_from(tablet_id)
                .map_err(|_| format!("tablet_id out of u64 range for GetShard: {tablet_id}"))
        })
        .collect::<Result<Vec<_>, _>>()?;
    let channel = get_channel(&routing.leader_addr).await?;
    let mut client = staros::star_manager_client::StarManagerClient::new(channel)
        .max_decoding_message_size(GRPC_MAX_MESSAGE_BYTES)
        .max_encoding_message_size(GRPC_MAX_MESSAGE_BYTES);
    let response = client
        .get_shard(staros::GetShardRequest {
            service_id: routing.service_id,
            shard_id: shard_ids,
            worker_group_id: routing.worker_group_id,
            no_wait_replica_allocation: false,
        })
        .await
        .map_err(|e| format!("StarManager GetShard failed: {e}"))?;
    let response = response.into_inner();
    require_ok_status(response.status.as_ref(), "GetShard")?;

    let mut recovered = HashMap::with_capacity(response.shard_info.len());
    for shard in &response.shard_info {
        let (tablet_id, info) = shard_info_to_registry_info(shard)?;
        recovered.insert(tablet_id, info);
    }
    Ok(recovered)
}

pub(crate) fn retrieve_shard_infos(
    tablet_ids: &[i64],
) -> Result<HashMap<i64, StarletShardInfo>, String> {
    let mut deduped = tablet_ids
        .iter()
        .copied()
        .filter(|tablet_id| *tablet_id > 0)
        .collect::<Vec<_>>();
    deduped.sort_unstable();
    deduped.dedup();
    if deduped.is_empty() {
        return Ok(HashMap::new());
    }

    let recovered = data_block_on(retrieve_shard_infos_async(&deduped))??;
    if !recovered.is_empty() {
        let _ = starlet_shard_registry::upsert_many_infos(
            recovered
                .iter()
                .map(|(tablet_id, info)| (*tablet_id, info.clone())),
        );
    }
    Ok(recovered)
}

pub(crate) fn retrieve_shard_info(tablet_id: i64) -> Result<Option<StarletShardInfo>, String> {
    let recovered = retrieve_shard_infos(&[tablet_id])?;
    Ok(recovered.get(&tablet_id).cloned())
}

async fn retrieve_s3_config_for_path_async(path: &str) -> Result<Option<S3StoreConfig>, String> {
    let Some((target_bucket, target_key)) = split_object_store_path(path) else {
        return Ok(None);
    };
    let routing = resolve_routing().await?;
    let channel = get_channel(&routing.leader_addr).await?;
    let mut client = staros::star_manager_client::StarManagerClient::new(channel)
        .max_decoding_message_size(GRPC_MAX_MESSAGE_BYTES)
        .max_encoding_message_size(GRPC_MAX_MESSAGE_BYTES);
    let response = client
        .list_file_store(staros::ListFileStoreRequest {
            service_id: routing.service_id,
            fs_type: staros::FileStoreType::S3 as i32,
        })
        .await
        .map_err(|e| format!("StarManager ListFileStore failed: {e}"))?;
    let response = response.into_inner();
    require_ok_status(response.status.as_ref(), "ListFileStore")?;

    // Two passes:
    //   1. The strongest signal is "the requested key sits under this file
    //      store's path_prefix". Pick the longest matching prefix.
    //   2. Otherwise fall back to any file store in the same bucket, but
    //      only if every entry agrees on the cluster-level profile.
    //
    // `path_prefix` only feeds the matching here; it is never written into
    // the returned `S3StoreConfig`.
    let mut best: Option<(usize, S3StoreConfig)> = None;
    let mut unique_bucket_cfg: Option<S3StoreConfig> = None;
    let mut bucket_conflict = false;
    for fs_info in &response.fs_infos {
        let Some(record) = parse_s3_config_from_file_store_info(fs_info)? else {
            continue;
        };
        if record.cfg.bucket != target_bucket {
            continue;
        }
        match unique_bucket_cfg.as_ref() {
            None => unique_bucket_cfg = Some(record.cfg.clone()),
            Some(existing) if existing == &record.cfg => {}
            Some(_) => bucket_conflict = true,
        }
        if !key_matches_root(&target_key, &record.path_prefix) {
            continue;
        }
        let score = record.path_prefix.len();
        match &best {
            Some((best_score, _)) if *best_score >= score => {}
            _ => best = Some((score, record.cfg)),
        }
    }
    if let Some((_, cfg)) = best {
        return Ok(Some(cfg));
    }
    if !bucket_conflict {
        return Ok(unique_bucket_cfg);
    }
    Ok(None)
}

pub(crate) fn retrieve_s3_config_for_path(path: &str) -> Result<Option<S3StoreConfig>, String> {
    data_block_on(retrieve_s3_config_for_path_async(path))?
}

#[cfg(test)]
pub(crate) fn clear_state_for_test() {
    if let Ok(mut guard) = state().lock() {
        *guard = StarMgrState::default();
    }
    if let Ok(mut guard) = channels().mu.lock() {
        guard.clear();
    }
}

#[cfg(test)]
pub(crate) fn lock_for_test() -> std::sync::MutexGuard<'static, ()> {
    static TEST_GUARD: OnceLock<Mutex<()>> = OnceLock::new();
    TEST_GUARD
        .get_or_init(|| Mutex::new(()))
        .lock()
        .expect("lock starmgr test guard")
}
