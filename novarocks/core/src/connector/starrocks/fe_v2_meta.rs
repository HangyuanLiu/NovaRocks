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
use std::collections::{BTreeMap, HashMap};
use std::sync::{Mutex, OnceLock};

use crate::common::types::UniqueId;
use crate::connector::starrocks::lake::context::get_tablet_runtime;
use crate::connector::starrocks::starmgr;
use crate::connector::starrocks::table_schema_service;
use crate::runtime::query_context::{QueryId, query_context_manager};
use crate::runtime::starlet_shard_registry;
use crate::thrift::types;

#[derive(Clone, Debug)]
pub(crate) struct LakeTableIdentity {
    pub(crate) catalog: String,
    pub(crate) db_name: String,
    pub(crate) table_name: String,
    pub(crate) db_id: i64,
    pub(crate) table_id: i64,
    pub(crate) schema_id: i64,
}

impl LakeTableIdentity {
    pub(crate) fn cache_key(&self) -> String {
        format!(
            "{}:{}:{}:{}",
            self.catalog, self.db_id, self.table_id, self.schema_id
        )
    }
}

#[derive(Copy, Clone, Debug)]
pub(crate) struct LakeScanTabletRef {
    pub(crate) tablet_id: i64,
    pub(crate) partition_id: i64,
    pub(crate) version: i64,
}

#[derive(Copy, Clone, Debug)]
pub(crate) struct LakeTabletPartitionRef {
    pub(crate) tablet_id: i64,
}

static TABLE_IDENTITY_NAME_CACHE: OnceLock<Mutex<HashMap<String, (String, String)>>> =
    OnceLock::new();

fn table_identity_name_cache() -> &'static Mutex<HashMap<String, (String, String)>> {
    TABLE_IDENTITY_NAME_CACHE.get_or_init(|| Mutex::new(HashMap::new()))
}

fn table_identity_name_key(catalog: &str, db_id: i64, table_id: i64) -> String {
    format!("{catalog}:{db_id}:{table_id}")
}

fn is_unknown_identity_name(v: &str) -> bool {
    let trimmed = v.trim();
    trimmed.is_empty()
        || trimmed.eq_ignore_ascii_case("__unknown_db__")
        || trimmed.eq_ignore_ascii_case("__unknown_table__")
}

pub(crate) fn cache_table_identity_names(table: &LakeTableIdentity) {
    if is_unknown_identity_name(&table.db_name) || is_unknown_identity_name(&table.table_name) {
        return;
    }
    if table.db_id <= 0 || table.table_id <= 0 {
        return;
    }
    if let Ok(mut guard) = table_identity_name_cache().lock() {
        guard.insert(
            table_identity_name_key(&table.catalog, table.db_id, table.table_id),
            (table.db_name.clone(), table.table_name.clone()),
        );
    }
}

pub(crate) fn find_cached_table_identity_names(
    catalog: &str,
    db_id: i64,
    table_id: i64,
) -> Option<(String, String)> {
    if catalog.trim().is_empty() || db_id <= 0 || table_id <= 0 {
        return None;
    }
    let guard = table_identity_name_cache().lock().ok()?;
    guard
        .get(&table_identity_name_key(catalog, db_id, table_id))
        .cloned()
}

#[cfg(test)]
fn clear_table_identity_name_cache_for_test() {
    if let Ok(mut guard) = table_identity_name_cache().lock() {
        guard.clear();
    }
}

pub(crate) fn resolve_tablet_paths_for_lake_scan(
    query_id: Option<QueryId>,
    table: &LakeTableIdentity,
    ranges: &[LakeScanTabletRef],
) -> Result<HashMap<i64, String>, String> {
    if ranges.is_empty() {
        return Ok(HashMap::new());
    }

    if ranges.iter().any(|r| r.version <= 0) {
        return Err("lake scan contains non-positive version".to_string());
    }
    if ranges.iter().any(|r| r.partition_id <= 0) {
        return Err("lake scan contains non-positive partition_id".to_string());
    }

    let refs = ranges
        .iter()
        .map(|r| LakeTabletPartitionRef {
            tablet_id: r.tablet_id,
        })
        .collect::<Vec<_>>();
    resolve_tablet_paths_for_refs(query_id, table, &refs)
}

pub(crate) fn lake_scan_execution_properties(
    query_id: Option<QueryId>,
    table: &LakeTableIdentity,
    ranges: &[LakeScanTabletRef],
) -> Result<BTreeMap<String, String>, String> {
    let tablet_path_map = resolve_tablet_paths_for_lake_scan(query_id, table, ranges)?;
    lake_scan_execution_properties_from_paths(ranges, &tablet_path_map)
}

pub(crate) fn lake_scan_execution_properties_from_paths(
    ranges: &[LakeScanTabletRef],
    tablet_path_map: &HashMap<i64, String>,
) -> Result<BTreeMap<String, String>, String> {
    if !ranges.is_empty() && tablet_path_map.is_empty() {
        return Err("lake scan tablet_path_map is empty".to_string());
    }
    let mut partition_paths = BTreeMap::new();
    for range in ranges {
        if range.tablet_id <= 0 {
            return Err(format!(
                "invalid tablet_id in lake scan execution properties: {}",
                range.tablet_id
            ));
        }
        if range.partition_id <= 0 {
            return Err(format!(
                "invalid partition_id in lake scan execution properties: {}",
                range.partition_id
            ));
        }
        let raw_path = tablet_path_map.get(&range.tablet_id).ok_or_else(|| {
            format!(
                "missing resolved tablet path while building partition_storage_paths for tablet_id={}",
                range.tablet_id
            )
        })?;
        let partition_path = normalize_partition_storage_path(raw_path, range.tablet_id)?;
        let key = range.partition_id.to_string();
        match partition_paths.get(&key) {
            None => {
                partition_paths.insert(key, partition_path);
            }
            Some(existing) if existing == &partition_path => {}
            Some(existing) => {
                return Err(format!(
                    "inconsistent partition storage paths for partition_id={}: existing={} new={} (tablet_id={})",
                    range.partition_id, existing, partition_path, range.tablet_id
                ));
            }
        }
    }

    let mut properties = if tablet_path_map.is_empty() {
        BTreeMap::new()
    } else {
        lake_scan_object_store_properties(tablet_path_map)?
    };
    properties.remove("tablet_root_paths");
    properties.insert(
        "partition_storage_paths".to_string(),
        serde_json::to_string(&partition_paths)
            .map_err(|err| format!("serialize partition_storage_paths json failed: {err}"))?,
    );
    Ok(properties)
}

pub(crate) fn lake_scan_object_store_properties(
    tablet_path_map: &HashMap<i64, String>,
) -> Result<BTreeMap<String, String>, String> {
    if tablet_path_map.is_empty() {
        return Err("lake scan tablet_path_map is empty".to_string());
    }
    let mut tablet_paths = BTreeMap::new();
    for (tablet_id, path) in tablet_path_map {
        if *tablet_id <= 0 {
            return Err(format!(
                "invalid tablet_id in resolved tablet path map: {tablet_id}"
            ));
        }
        let trimmed = path.trim();
        if trimmed.is_empty() {
            return Err(format!(
                "resolved tablet path is empty for tablet_id={tablet_id}"
            ));
        }
        tablet_paths.insert(tablet_id.to_string(), trimmed.to_string());
    }
    let mut properties = BTreeMap::from([(
        "tablet_root_paths".to_string(),
        serde_json::to_string(&tablet_paths)
            .map_err(|err| format!("serialize tablet_root_paths json failed: {err}"))?,
    )]);
    let selected_s3 = crate::connector::starrocks::fs_access::common_runtime_s3_config_for_paths(
        tablet_paths.values().map(String::as_str),
    )?;
    if let Some(s3) = selected_s3 {
        properties.extend(s3.to_aws_s3_properties());
    }
    Ok(properties)
}

fn normalize_partition_storage_path(path: &str, tablet_id: i64) -> Result<String, String> {
    let trimmed = path.trim().trim_end_matches('/');
    if trimmed.is_empty() {
        return Err(format!(
            "empty storage path while building partition_storage_paths for tablet_id={tablet_id}"
        ));
    }
    let standalone_tablet_suffix = format!("/tablet_{tablet_id}");
    if let Some(prefix) = trimmed.strip_suffix(&standalone_tablet_suffix)
        && !prefix.is_empty()
    {
        return Ok(prefix.trim_end_matches('/').to_string());
    }
    let tablet_id_suffix = format!("/{tablet_id}");
    if let Some(prefix) = trimmed.strip_suffix(&tablet_id_suffix)
        && !prefix.is_empty()
    {
        return Ok(prefix.trim_end_matches('/').to_string());
    }
    Ok(trimmed.to_string())
}

pub(crate) fn resolve_tablet_paths_for_lake_meta_scan(
    query_id: Option<QueryId>,
    table: &LakeTableIdentity,
    tablet_ids: &[i64],
) -> Result<HashMap<i64, String>, String> {
    if tablet_ids.is_empty() {
        return Ok(HashMap::new());
    }
    if tablet_ids.iter().any(|tablet_id| *tablet_id <= 0) {
        return Err("lake meta scan contains non-positive tablet_id".to_string());
    }
    let refs = tablet_ids
        .iter()
        .map(|tablet_id| LakeTabletPartitionRef {
            tablet_id: *tablet_id,
        })
        .collect::<Vec<_>>();
    resolve_tablet_paths_for_refs(query_id, table, &refs)
}

pub(crate) fn resolve_tablet_paths_for_olap_sink(
    query_id: Option<QueryId>,
    table: &LakeTableIdentity,
    refs: &[LakeTabletPartitionRef],
) -> Result<HashMap<i64, String>, String> {
    resolve_tablet_paths_for_refs(query_id, table, refs)
}

fn resolve_tablet_paths_for_refs(
    query_id: Option<QueryId>,
    table: &LakeTableIdentity,
    refs: &[LakeTabletPartitionRef],
) -> Result<HashMap<i64, String>, String> {
    if refs.is_empty() {
        return Ok(HashMap::new());
    }

    let cache_key = table.cache_key();
    if let Some(qid) = query_id
        && let Some(paths) = query_context_manager().lake_tablet_paths(qid, &cache_key)
        && paths_cover_refs(&paths, refs)
    {
        cache_table_identity_names(table);
        return select_paths_for_refs(paths, refs);
    }

    let requested_tablet_ids = refs.iter().map(|r| r.tablet_id).collect::<Vec<_>>();
    let mut local_paths = starlet_shard_registry::select_paths(&requested_tablet_ids);
    let recovered_paths =
        recover_missing_paths_from_runtime_registry(&requested_tablet_ids, &local_paths);
    if !recovered_paths.is_empty() {
        let upserted = starlet_shard_registry::upsert_many(
            recovered_paths
                .iter()
                .map(|(tablet_id, path)| (*tablet_id, path.clone())),
        );
        if upserted > 0 {
            tracing::warn!(
                target: "novarocks::lake",
                table = %table.cache_key(),
                recovered = upserted,
                "recovered tablet root paths from runtime registry because AddShard cache was incomplete"
            );
        }
        local_paths.extend(recovered_paths);
    }

    let mut metadata_recovery_error = None;
    let missing_after_local = collect_missing_tablet_ids(&requested_tablet_ids, &local_paths);
    if !missing_after_local.is_empty() {
        match crate::engine::recover_starrocks_tablet_paths_from_current_engine(
            table,
            &missing_after_local,
        ) {
            Ok(recovered) => {
                if !recovered.is_empty() {
                    let upserted = starlet_shard_registry::upsert_many(
                        recovered
                            .iter()
                            .map(|(tablet_id, path)| (*tablet_id, path.clone())),
                    );
                    if upserted > 0 {
                        tracing::info!(
                            target: "novarocks::lake",
                            table = %table.cache_key(),
                            recovered = upserted,
                            "recovered tablet root paths from metadata repository because local shard cache was incomplete"
                        );
                    }
                    local_paths.extend(recovered);
                }
            }
            Err(err) => {
                tracing::warn!(
                    target: "novarocks::lake",
                    table = %table.cache_key(),
                    tablet_ids = ?missing_after_local,
                    error = %err,
                    "failed to recover missing tablet root paths from metadata repository"
                );
                metadata_recovery_error = Some(err);
            }
        }
    }

    let mut starmgr_recovery_error = None;
    let missing_after_metadata = collect_missing_tablet_ids(&requested_tablet_ids, &local_paths);
    if !missing_after_metadata.is_empty() {
        match recover_missing_paths_from_starmgr(&missing_after_metadata) {
            Ok(recovered) => {
                if !recovered.is_empty() {
                    // StarManager already provides the cluster-level S3
                    // profile alongside the freshly resolved tablet path,
                    // so carry it forward directly. Re-inferring from the
                    // registry here is what historically grafted a stale
                    // `root` onto the new full_path.
                    let recovered_infos = recovered
                        .iter()
                        .map(|(tablet_id, info)| {
                            (
                                *tablet_id,
                                crate::runtime::starlet_shard_registry::StarletShardInfo {
                                    full_path: info.full_path.clone(),
                                    s3: info.s3.clone(),
                                },
                            )
                        })
                        .collect::<Vec<_>>();
                    let upserted = starlet_shard_registry::upsert_many_infos(recovered_infos);
                    if upserted > 0 {
                        tracing::info!(
                            target: "novarocks::lake",
                            table = %table.cache_key(),
                            recovered = upserted,
                            "recovered tablet root paths from StarManager GetShard because local shard cache was incomplete"
                        );
                    }
                    for (tablet_id, info) in recovered {
                        local_paths.insert(tablet_id, info.full_path);
                    }
                }
            }
            Err(err) => {
                tracing::warn!(
                    target: "novarocks::lake",
                    table = %table.cache_key(),
                    tablet_ids = ?missing_after_metadata,
                    error = %err,
                    "failed to recover missing tablet root paths from StarManager GetShard"
                );
                starmgr_recovery_error = Some(err);
            }
        }
    }

    if !paths_cover_refs(&local_paths, refs) {
        let missing = collect_missing_tablet_ids(&requested_tablet_ids, &local_paths);
        let metadata_context = metadata_recovery_error
            .map(|err| format!("; metadata_error={err}"))
            .unwrap_or_default();
        let starmgr_context = starmgr_recovery_error
            .map(|err| format!("; starmgr_error={err}"))
            .unwrap_or_default();
        return Err(format!(
            "missing shard path for tablet_ids={missing:?} after local AddShard cache, \
            runtime registry, metadata repository, and StarManager GetShard{metadata_context}{starmgr_context}"
        ));
    }

    if let Some(qid) = query_id {
        let _ = query_context_manager().set_lake_tablet_paths(qid, cache_key, local_paths.clone());
    }
    cache_table_identity_names(table);
    select_paths_for_refs(local_paths, refs)
}

fn recover_missing_paths_from_starmgr(
    tablet_ids: &[i64],
) -> Result<HashMap<i64, starlet_shard_registry::StarletShardInfo>, String> {
    let recovered = starmgr::retrieve_shard_infos(tablet_ids)?;
    let mut out = HashMap::with_capacity(recovered.len());
    for (tablet_id, info) in recovered {
        let normalized = normalize_storage_path(&info.full_path).ok_or_else(|| {
            format!(
                "StarManager GetShard returned invalid tablet path for tablet_id={tablet_id}: {}",
                info.full_path
            )
        })?;
        out.insert(
            tablet_id,
            starlet_shard_registry::StarletShardInfo {
                full_path: normalized,
                s3: info.s3,
            },
        );
    }
    Ok(out)
}

fn recover_missing_paths_from_runtime_registry(
    requested_tablet_ids: &[i64],
    resolved_paths: &HashMap<i64, String>,
) -> HashMap<i64, String> {
    let mut recovered = HashMap::new();
    for tablet_id in requested_tablet_ids {
        if resolved_paths.contains_key(tablet_id) {
            continue;
        }
        let Ok(runtime) = get_tablet_runtime(*tablet_id) else {
            continue;
        };
        let root_path = runtime.root_path.trim().trim_end_matches('/').to_string();
        if root_path.is_empty() {
            continue;
        }
        recovered.insert(*tablet_id, root_path);
    }
    recovered
}

fn collect_missing_tablet_ids(
    requested_tablet_ids: &[i64],
    resolved_paths: &HashMap<i64, String>,
) -> Vec<i64> {
    let mut missing = requested_tablet_ids
        .iter()
        .copied()
        .filter(|tablet_id| !resolved_paths.contains_key(tablet_id))
        .collect::<Vec<_>>();
    missing.sort_unstable();
    missing.dedup();
    missing
}

pub(crate) fn fetch_table_schema_for_lake_scan(
    fe_addr: Option<&types::TNetworkAddress>,
    db_id: i64,
    table_id: i64,
    schema_id: i64,
    tablet_id: Option<i64>,
    query_id: Option<UniqueId>,
    local_schema: Option<&crate::thrift::agent_service::TTabletSchema>,
) -> Result<crate::thrift::agent_service::TTabletSchema, String> {
    table_schema_service::fetch_table_schema_for_lake_scan(
        fe_addr,
        db_id,
        table_id,
        schema_id,
        tablet_id,
        query_id,
        local_schema,
    )
}

fn normalize_storage_path(path: &str) -> Option<String> {
    let normalized = path.trim().trim_end_matches('/');
    if normalized.is_empty() {
        None
    } else {
        Some(normalized.to_string())
    }
}

fn paths_cover_refs(paths: &HashMap<i64, String>, refs: &[LakeTabletPartitionRef]) -> bool {
    refs.iter().all(|r| paths.contains_key(&r.tablet_id))
}

fn select_paths_for_refs(
    full_paths: HashMap<i64, String>,
    refs: &[LakeTabletPartitionRef],
) -> Result<HashMap<i64, String>, String> {
    let mut selected = HashMap::with_capacity(refs.len());
    for r in refs {
        let raw_path = full_paths.get(&r.tablet_id).ok_or_else(|| {
            format!(
                "missing tablet path for tablet_id={} in resolved map",
                r.tablet_id
            )
        })?;
        let path = normalize_selected_tablet_path(raw_path).ok_or_else(|| {
            format!(
                "invalid tablet path for tablet_id={} in resolved map: {}",
                r.tablet_id, raw_path
            )
        })?;
        selected.insert(r.tablet_id, path);
    }
    Ok(selected)
}

fn normalize_selected_tablet_path(path: &str) -> Option<String> {
    normalize_storage_path(path)
}
