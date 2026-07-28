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
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, OnceLock};

use crate::common::types::UniqueId;
use crate::runtime::endpoint::RuntimeEndpoint;
use crate::runtime::query_context::QueryCleanupLease;
use crate::runtime::query_context::QueryId;
use crate::thrift::descriptors;
use crate::thrift::{internal_service, types};

pub(crate) use super::starrocks_fragment_dependency_resolver::StarRocksDependencyResolutionError;

#[derive(Clone)]
pub struct StarRocksPrelaunchCancellationToken {
    cancelled: Arc<AtomicBool>,
    frontend_endpoint: Option<RuntimeEndpoint>,
}

impl StarRocksPrelaunchCancellationToken {
    pub fn check(&self, dependency_id: u64) -> Result<(), StarRocksDependencyResolutionError> {
        if self.cancelled.load(Ordering::Acquire) {
            Err(StarRocksDependencyResolutionError::Cancelled { dependency_id })
        } else {
            Ok(())
        }
    }

    pub fn frontend_endpoint(&self) -> Option<&RuntimeEndpoint> {
        self.frontend_endpoint.as_ref()
    }
}

#[derive(Clone)]
struct PrelaunchEntry {
    query_id: QueryId,
    generation: u64,
    cancelled: Arc<AtomicBool>,
}

#[derive(Default)]
pub struct StarRocksPrelaunchRegistry {
    entries: Mutex<HashMap<UniqueId, PrelaunchEntry>>,
}

impl StarRocksPrelaunchRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn install<I>(
        self: &Arc<Self>,
        query_id: QueryId,
        generation: u64,
        finst_ids: I,
    ) -> Result<StarRocksPrelaunchGuard, String>
    where
        I: IntoIterator<Item = UniqueId>,
    {
        let finst_ids = finst_ids.into_iter().collect::<Vec<_>>();
        if finst_ids.is_empty() {
            return Err("prelaunch guard requires at least one fragment instance".to_string());
        }
        let cancelled = Arc::new(AtomicBool::new(false));
        let mut entries = self.entries.lock().expect("prelaunch registry lock");
        for finst_id in &finst_ids {
            if entries.contains_key(finst_id) {
                return Err(format!("fragment instance {finst_id} is already preparing"));
            }
        }
        for finst_id in &finst_ids {
            entries.insert(
                *finst_id,
                PrelaunchEntry {
                    query_id,
                    generation,
                    cancelled: Arc::clone(&cancelled),
                },
            );
        }
        Ok(StarRocksPrelaunchGuard {
            registry: Arc::clone(self),
            query_id,
            generation,
            finst_ids,
            cancelled,
            frontend_endpoint: None,
            released: false,
        })
    }

    #[cfg(test)]
    pub(crate) fn cancel(&self, finst_id: UniqueId) -> bool {
        let entries = self.entries.lock().expect("prelaunch registry lock");
        let Some(entry) = entries.get(&finst_id) else {
            return false;
        };
        entry.cancelled.store(true, Ordering::Release);
        true
    }

    pub(crate) fn cancel_or_run<F>(&self, finst_id: UniqueId, runtime_cancel: F) -> bool
    where
        F: FnOnce(),
    {
        let entries = self.entries.lock().expect("prelaunch registry lock");
        if let Some(entry) = entries.get(&finst_id) {
            entry.cancelled.store(true, Ordering::Release);
            true
        } else {
            // Keep P held until runtime cancellation has resolved and cleaned finst-keyed
            // resources. A reused finst cannot enter handoff between a missing-route decision
            // and those side effects.
            runtime_cancel();
            false
        }
    }

    #[cfg(test)]
    pub(crate) fn snapshot_count(&self) -> usize {
        self.entries.lock().expect("prelaunch registry lock").len()
    }
}

pub struct StarRocksPrelaunchGuard {
    registry: Arc<StarRocksPrelaunchRegistry>,
    query_id: QueryId,
    generation: u64,
    finst_ids: Vec<UniqueId>,
    cancelled: Arc<AtomicBool>,
    frontend_endpoint: Option<RuntimeEndpoint>,
    released: bool,
}

impl StarRocksPrelaunchGuard {
    pub fn cancellation_token(&self) -> StarRocksPrelaunchCancellationToken {
        StarRocksPrelaunchCancellationToken {
            cancelled: Arc::clone(&self.cancelled),
            frontend_endpoint: self.frontend_endpoint.clone(),
        }
    }

    pub fn set_frontend_endpoint(&mut self, endpoint: Option<RuntimeEndpoint>) {
        self.frontend_endpoint = endpoint;
    }

    pub fn handoff<T, F>(mut self, make_runtime_visible: F) -> Result<T, String>
    where
        F: FnOnce() -> Result<T, String>,
    {
        let mut entries = self
            .registry
            .entries
            .lock()
            .expect("prelaunch registry lock");
        if self.cancelled.load(Ordering::Acquire) {
            return Err("fragment preparation was cancelled".to_string());
        }
        let value = make_runtime_visible()?;
        for finst_id in &self.finst_ids {
            if entries.get(finst_id).is_some_and(|entry| {
                entry.query_id == self.query_id && entry.generation == self.generation
            }) {
                entries.remove(finst_id);
            }
        }
        self.released = true;
        Ok(value)
    }

    fn release(&mut self) {
        if self.released {
            return;
        }
        let mut entries = self
            .registry
            .entries
            .lock()
            .expect("prelaunch registry lock");
        for finst_id in &self.finst_ids {
            if entries.get(finst_id).is_some_and(|entry| {
                entry.query_id == self.query_id && entry.generation == self.generation
            }) {
                entries.remove(finst_id);
            }
        }
        self.released = true;
    }
}

impl Drop for StarRocksPrelaunchGuard {
    fn drop(&mut self) {
        self.release();
    }
}

pub fn starrocks_prelaunch_registry() -> &'static Arc<StarRocksPrelaunchRegistry> {
    static REGISTRY: OnceLock<Arc<StarRocksPrelaunchRegistry>> = OnceLock::new();
    REGISTRY.get_or_init(|| Arc::new(StarRocksPrelaunchRegistry::new()))
}

#[derive(Clone)]
struct DescriptorCacheEntry {
    generation: u64,
    descriptor: Arc<descriptors::TDescriptorTable>,
}

#[derive(Default)]
struct DescriptorCacheInner {
    next_generation: u64,
    entries: HashMap<QueryId, DescriptorCacheEntry>,
}

#[derive(Default)]
pub(crate) struct StarRocksDescriptorCache {
    inner: Arc<Mutex<DescriptorCacheInner>>,
}

#[derive(Clone)]
pub struct StarRocksDescriptorPreparation {
    query_id: QueryId,
    generation: u64,
    descriptor: Option<Arc<descriptors::TDescriptorTable>>,
    commit_descriptor: bool,
}

pub struct StarRocksDescriptorLeaseFactory {
    inner: Arc<Mutex<DescriptorCacheInner>>,
    query_id: QueryId,
    generation: u64,
}

impl StarRocksDescriptorLeaseFactory {
    pub fn into_cleanup_lease(self) -> QueryCleanupLease {
        QueryCleanupLease::new(move || {
            let mut inner = self.inner.lock().expect("descriptor cache lock");
            if inner
                .entries
                .get(&self.query_id)
                .is_some_and(|entry| entry.generation == self.generation)
            {
                inner.entries.remove(&self.query_id);
            }
        })
    }
}

impl StarRocksDescriptorPreparation {
    pub const fn generation(&self) -> u64 {
        self.generation
    }

    pub fn descriptor(&self) -> Option<&descriptors::TDescriptorTable> {
        self.descriptor.as_deref()
    }
}

impl StarRocksDescriptorCache {
    fn prepare_batch(
        &self,
        query_id: QueryId,
        common: Option<&descriptors::TDescriptorTable>,
        unique: &[Option<&descriptors::TDescriptorTable>],
    ) -> Result<StarRocksDescriptorPreparation, String> {
        let mut concrete = None;
        let mut marker = None;
        for candidate in common.into_iter().chain(unique.iter().copied().flatten()) {
            if desc_tbl_is_cached(candidate) {
                marker = Some(candidate);
                continue;
            }
            if is_desc_tbl_effectively_empty(candidate) {
                continue;
            }
            if let Some(existing) = concrete {
                if existing != candidate {
                    return Err(
                        "conflicting concrete descriptor tables in StarRocks batch".to_string()
                    );
                }
            } else {
                concrete = Some(candidate);
            }
        }
        self.prepare(query_id, concrete.or(marker), None)
    }

    fn prepare(
        &self,
        query_id: QueryId,
        incoming: Option<&descriptors::TDescriptorTable>,
        fallback: Option<&descriptors::TDescriptorTable>,
    ) -> Result<StarRocksDescriptorPreparation, String> {
        let mut inner = self.inner.lock().expect("descriptor cache lock");
        let existing = inner.entries.get(&query_id).cloned();
        let selected = incoming.or(fallback);
        let cached_marker = selected.map(desc_tbl_is_cached).unwrap_or(false);
        let concrete = selected
            .filter(|desc| !desc_tbl_is_cached(desc) && !is_desc_tbl_effectively_empty(desc));
        if cached_marker {
            let entry = existing.ok_or_else(|| {
                "Query terminates prematurely (missing desc_tbl transport cache)".to_string()
            })?;
            return Ok(StarRocksDescriptorPreparation {
                query_id,
                generation: entry.generation,
                descriptor: Some(entry.descriptor),
                commit_descriptor: false,
            });
        }
        if let Some(entry) = existing {
            if let Some(concrete) = concrete
                && entry.descriptor.as_ref() != concrete
            {
                return Err("conflicting descriptor table for active query generation".to_string());
            }
            return Ok(StarRocksDescriptorPreparation {
                query_id,
                generation: entry.generation,
                descriptor: concrete
                    .map(|value| Arc::new(value.clone()))
                    .or(Some(entry.descriptor)),
                commit_descriptor: false,
            });
        }
        inner.next_generation = inner.next_generation.wrapping_add(1).max(1);
        let generation = inner.next_generation;
        Ok(StarRocksDescriptorPreparation {
            query_id,
            generation,
            descriptor: concrete.map(|value| Arc::new(value.clone())),
            commit_descriptor: concrete.is_some(),
        })
    }

    #[cfg(test)]
    fn commit(
        &self,
        preparation: &StarRocksDescriptorPreparation,
    ) -> Result<Option<QueryCleanupLease>, String> {
        let Some(descriptor) = preparation.descriptor.as_ref() else {
            return Ok(None);
        };
        let mut inner = self.inner.lock().expect("descriptor cache lock");
        if let Some(entry) = inner.entries.get(&preparation.query_id) {
            if entry.generation != preparation.generation
                || entry.descriptor.as_ref() != descriptor.as_ref()
            {
                return Err("descriptor cache generation changed during preparation".to_string());
            }
        } else if preparation.commit_descriptor {
            inner.entries.insert(
                preparation.query_id,
                DescriptorCacheEntry {
                    generation: preparation.generation,
                    descriptor: Arc::clone(descriptor),
                },
            );
        } else {
            return Err("descriptor cache entry disappeared during preparation".to_string());
        }
        drop(inner);

        let inner = Arc::clone(&self.inner);
        let query_id = preparation.query_id;
        let generation = preparation.generation;
        Ok(Some(QueryCleanupLease::new(move || {
            let mut inner = inner.lock().expect("descriptor cache lock");
            if inner
                .entries
                .get(&query_id)
                .is_some_and(|entry| entry.generation == generation)
            {
                inner.entries.remove(&query_id);
            }
        })))
    }

    fn commit_handoff<T, F>(
        &self,
        preparation: &StarRocksDescriptorPreparation,
        make_runtime_visible: F,
    ) -> Result<T, String>
    where
        F: FnOnce(Option<StarRocksDescriptorLeaseFactory>) -> Result<T, String>,
    {
        let mut inner = self.inner.lock().expect("descriptor cache lock");
        let mut inserted = false;
        if let Some(descriptor) = preparation.descriptor.as_ref() {
            if let Some(entry) = inner.entries.get(&preparation.query_id) {
                if entry.generation != preparation.generation
                    || entry.descriptor.as_ref() != descriptor.as_ref()
                {
                    return Err(
                        "descriptor cache generation changed during preparation".to_string()
                    );
                }
            } else if preparation.commit_descriptor {
                inner.entries.insert(
                    preparation.query_id,
                    DescriptorCacheEntry {
                        generation: preparation.generation,
                        descriptor: Arc::clone(descriptor),
                    },
                );
                inserted = true;
            } else {
                return Err("descriptor cache entry disappeared during preparation".to_string());
            }
        }
        let lease_factory =
            preparation
                .descriptor
                .as_ref()
                .map(|_| StarRocksDescriptorLeaseFactory {
                    inner: Arc::clone(&self.inner),
                    query_id: preparation.query_id,
                    generation: preparation.generation,
                });
        match make_runtime_visible(lease_factory) {
            Ok(value) => Ok(value),
            Err(error) => {
                if inserted
                    && inner
                        .entries
                        .get(&preparation.query_id)
                        .is_some_and(|entry| entry.generation == preparation.generation)
                {
                    inner.entries.remove(&preparation.query_id);
                }
                Err(error)
            }
        }
    }

    #[cfg(test)]
    fn snapshot_count(&self) -> usize {
        self.inner
            .lock()
            .expect("descriptor cache lock")
            .entries
            .len()
    }
}

pub(crate) fn starrocks_descriptor_cache() -> &'static StarRocksDescriptorCache {
    static CACHE: OnceLock<StarRocksDescriptorCache> = OnceLock::new();
    CACHE.get_or_init(StarRocksDescriptorCache::default)
}

pub fn prepare_descriptor(
    query_id: QueryId,
    incoming: Option<&descriptors::TDescriptorTable>,
    fallback: Option<&descriptors::TDescriptorTable>,
) -> Result<StarRocksDescriptorPreparation, String> {
    starrocks_descriptor_cache().prepare(query_id, incoming, fallback)
}

pub fn prepare_batch_descriptor(
    query_id: QueryId,
    common: Option<&descriptors::TDescriptorTable>,
    unique: &[Option<&descriptors::TDescriptorTable>],
) -> Result<StarRocksDescriptorPreparation, String> {
    starrocks_descriptor_cache().prepare_batch(query_id, common, unique)
}

pub fn commit_descriptor_handoff<T, F>(
    preparation: &StarRocksDescriptorPreparation,
    make_runtime_visible: F,
) -> Result<T, String>
where
    F: FnOnce(Option<StarRocksDescriptorLeaseFactory>) -> Result<T, String>,
{
    starrocks_descriptor_cache().commit_handoff(preparation, make_runtime_visible)
}

pub(crate) fn desc_tbl_is_cached(desc: &descriptors::TDescriptorTable) -> bool {
    desc.is_cached.unwrap_or(false)
}

pub(crate) fn is_desc_tbl_effectively_empty(desc: &descriptors::TDescriptorTable) -> bool {
    !desc_tbl_is_cached(desc)
        && desc.tuple_descriptors.is_empty()
        && desc
            .table_descriptors
            .as_ref()
            .map(Vec::is_empty)
            .unwrap_or(true)
        && desc
            .slot_descriptors
            .as_ref()
            .map(Vec::is_empty)
            .unwrap_or(true)
}

pub fn snapshot_decode_facts(
    exec_params: &internal_service::TPlanFragmentExecParams,
) -> Result<crate::protocol::starrocks::decode::StarRocksDecodeFacts, String> {
    let mut stream_load_paths = BTreeMap::new();
    for ranges in exec_params.per_node_scan_ranges.values() {
        for params in ranges {
            let Some(broker) = params.scan_range.broker_scan_range.as_ref() else {
                continue;
            };
            for range in &broker.ranges {
                if range.file_type != types::TFileType::FILE_STREAM {
                    continue;
                }
                let load_id = range
                    .load_id
                    .as_ref()
                    .ok_or_else(|| "FILE_STREAM range is missing load_id".to_string())?;
                let path = super::stream_load_registry::resolve_stream_load_file_path(load_id)
                    .ok_or_else(|| {
                        format!(
                            "no registered local file for FILE_STREAM load_id={}:{}",
                            load_id.hi, load_id.lo
                        )
                    })?;
                stream_load_paths.insert(
                    UniqueId {
                        hi: load_id.hi,
                        lo: load_id.lo,
                    },
                    path,
                );
            }
        }
    }
    let config = crate::common::app_config::config().map_err(|error| error.to_string())?;
    let rewrite = &config.runtime.path_rewrite;
    let path_rewrite = rewrite.enable.then(|| {
        crate::protocol::starrocks::decode::StarRocksPathRewriteFacts::new(
            rewrite.from_prefix.clone(),
            rewrite.to_prefix.clone(),
        )
    });
    let datacache_available = config.runtime.cache.datacache_enable
        && crate::cache::DataCacheManager::instance()
            .block_cache()
            .is_some();
    let jdbc = config.jdbc_config().map(|jdbc| {
        crate::protocol::starrocks::decode::StarRocksJdbcFacts::new(
            jdbc.url.clone(),
            jdbc.user.clone(),
            jdbc.password.clone(),
            jdbc.default_db.clone(),
        )
    });
    let object_storage = &config.runtime.object_storage;
    let object_store_defaults =
        crate::protocol::starrocks::decode::StarRocksObjectStoreDefaults::new(
            object_storage.retry_max_times,
            object_storage.retry_min_delay_ms,
            object_storage.retry_max_delay_ms,
            object_storage.timeout_ms,
            object_storage.io_timeout_ms,
        );
    Ok(
        crate::protocol::starrocks::decode::StarRocksDecodeFacts::new(
            stream_load_paths,
            path_rewrite,
            datacache_available,
            jdbc,
            object_store_defaults,
        ),
    )
}

#[cfg(test)]
pub(crate) fn descriptor_cache_snapshot_count() -> usize {
    starrocks_descriptor_cache().snapshot_count()
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, mpsc};
    use std::time::Duration;

    use crate::common::types::UniqueId;
    use crate::runtime::query_context::{
        QueryCleanupLease, QueryExecutionKey, QueryId, StarRocksQueryGeneration,
        query_context_manager,
    };
    use crate::thrift::descriptors;

    use super::{
        StarRocksDependencyResolutionError, StarRocksDescriptorCache, StarRocksPrelaunchRegistry,
        starrocks_descriptor_cache,
    };

    fn descriptor(tuple_id: i32) -> descriptors::TDescriptorTable {
        descriptors::TDescriptorTable::new(
            vec![],
            vec![descriptors::TTupleDescriptor::new(
                tuple_id, None, None, None, None,
            )],
            None,
            None,
        )
    }

    fn cached_marker() -> descriptors::TDescriptorTable {
        descriptors::TDescriptorTable::new(vec![], vec![], None, Some(true))
    }

    fn empty_descriptor() -> descriptors::TDescriptorTable {
        descriptors::TDescriptorTable::new(vec![], vec![], None, None)
    }

    fn cached_generation(cache: &StarRocksDescriptorCache, query_id: QueryId) -> Option<u64> {
        cache
            .inner
            .lock()
            .expect("descriptor cache lock")
            .entries
            .get(&query_id)
            .map(|entry| entry.generation)
    }

    #[test]
    fn batch_descriptor_rejects_common_unique_concrete_conflict() {
        let cache = StarRocksDescriptorCache::default();
        let query_id = QueryId {
            hi: 85_181,
            lo: 85_182,
        };
        let common = descriptor(1);
        let unique = descriptor(2);

        let error = cache
            .prepare_batch(query_id, Some(&common), &[Some(&unique)])
            .err()
            .expect("conflicting common and unique descriptors must fail");

        assert!(error.contains("conflicting concrete descriptor"), "{error}");
        assert_eq!(cached_generation(&cache, query_id), None);
    }

    #[test]
    fn batch_descriptor_rejects_unique_concrete_conflict() {
        let cache = StarRocksDescriptorCache::default();
        let query_id = QueryId {
            hi: 85_183,
            lo: 85_184,
        };
        let first = descriptor(3);
        let second = descriptor(4);

        let error = cache
            .prepare_batch(query_id, None, &[Some(&first), Some(&second)])
            .err()
            .expect("conflicting unique descriptors must fail");

        assert!(error.contains("conflicting concrete descriptor"), "{error}");
        assert_eq!(cached_generation(&cache, query_id), None);
    }

    #[test]
    fn batch_descriptor_uses_common_concrete_when_unique_is_empty() {
        let cache = StarRocksDescriptorCache::default();
        let query_id = QueryId {
            hi: 85_185,
            lo: 85_186,
        };
        let common = descriptor(5);
        let empty = empty_descriptor();

        let preparation = cache
            .prepare_batch(query_id, Some(&common), &[Some(&empty)])
            .expect("common concrete descriptor");

        assert_eq!(preparation.descriptor(), Some(&common));
        assert_eq!(cached_generation(&cache, query_id), None);
    }

    #[test]
    fn batch_descriptor_marker_requires_existing_and_reuses_it_when_present() {
        let cache = StarRocksDescriptorCache::default();
        let query_id = QueryId {
            hi: 85_187,
            lo: 85_188,
        };
        let marker = cached_marker();

        let error = cache
            .prepare_batch(query_id, None, &[Some(&marker)])
            .err()
            .expect("marker without cache entry must fail");
        assert!(
            error.contains("missing desc_tbl transport cache"),
            "{error}"
        );

        let concrete = descriptor(6);
        let initial = cache
            .prepare_batch(query_id, Some(&concrete), &[])
            .expect("prepare initial concrete descriptor");
        let generation = initial.generation();
        let lease = cache
            .commit(&initial)
            .expect("commit initial descriptor")
            .expect("descriptor lease");

        let reused = cache
            .prepare_batch(query_id, None, &[Some(&marker)])
            .expect("marker reuses existing descriptor");
        assert_eq!(reused.generation(), generation);
        assert_eq!(reused.descriptor(), Some(&concrete));
        drop(lease);
    }

    #[test]
    fn descriptor_cache_survives_second_chance_for_late_cached_marker_then_expires() {
        let query_id = QueryId {
            hi: 85_211,
            lo: 85_212,
        };
        let descriptor = descriptor(1);
        let cache = starrocks_descriptor_cache();
        let preparation = cache
            .prepare(query_id, Some(&descriptor), None)
            .expect("prepare descriptor");
        let generation = preparation.generation();
        let lease = cache
            .commit(&preparation)
            .expect("commit descriptor")
            .expect("descriptor lease");
        let manager = query_context_manager();
        manager
            .get_or_register_compat(
                query_id,
                false,
                Duration::from_secs(60),
                Duration::from_secs(60),
            )
            .expect("register query");
        manager
            .attach_cleanup_lease(query_id, lease)
            .expect("attach descriptor lease");

        manager.finish_fragment(query_id);
        let late = cache
            .prepare(query_id, Some(&cached_marker()), None)
            .expect("late cached marker must reuse second-chance descriptor");
        assert_eq!(late.generation(), generation);
        assert_eq!(late.descriptor(), Some(&descriptor));

        manager.expire_delivery_for_test(query_id);
        manager.clean_expired_for_test();
        assert_eq!(cached_generation(cache, query_id), None);
        assert!(
            cache
                .prepare(query_id, Some(&cached_marker()), None)
                .is_err()
        );
    }

    #[test]
    fn descriptor_cache_releases_only_after_cancelled_fragment_finishes() {
        let query_id = QueryId {
            hi: 85_221,
            lo: 85_222,
        };
        let descriptor = descriptor(2);
        let cache = starrocks_descriptor_cache();
        let preparation = cache
            .prepare(query_id, Some(&descriptor), None)
            .expect("prepare descriptor");
        let generation = preparation.generation();
        let lease = cache
            .commit(&preparation)
            .expect("commit descriptor")
            .expect("descriptor lease");
        let manager = query_context_manager();
        manager
            .get_or_register_compat(
                query_id,
                false,
                Duration::from_secs(60),
                Duration::from_secs(60),
            )
            .expect("register query");
        manager
            .attach_cleanup_lease(query_id, lease)
            .expect("attach descriptor lease");

        manager.cancel_query(query_id, "cancelled by test".to_string());
        assert_eq!(cached_generation(cache, query_id), Some(generation));
        manager.finish_fragment(query_id);
        assert_eq!(cached_generation(cache, query_id), None);
    }

    #[test]
    fn descriptor_cache_releases_on_final_dead_fragment() {
        let query_id = QueryId {
            hi: 85_231,
            lo: 85_232,
        };
        let descriptor = descriptor(3);
        let cache = starrocks_descriptor_cache();
        let preparation = cache
            .prepare(query_id, Some(&descriptor), None)
            .expect("prepare descriptor");
        let lease = cache
            .commit(&preparation)
            .expect("commit descriptor")
            .expect("descriptor lease");
        let manager = query_context_manager();
        manager
            .get_or_register_compat(
                query_id,
                false,
                Duration::from_secs(60),
                Duration::from_secs(60),
            )
            .expect("register query");
        manager
            .with_context_mut(query_id, |context| {
                context.total_fragments = Some(1);
                Ok(())
            })
            .expect("mark final fragment");
        manager
            .attach_cleanup_lease(query_id, lease)
            .expect("attach descriptor lease");

        manager.finish_fragment(query_id);
        assert_eq!(cached_generation(cache, query_id), None);
    }

    #[test]
    fn old_generation_lease_cannot_remove_reused_query_descriptor() {
        let query_id = QueryId {
            hi: 85_241,
            lo: 85_242,
        };
        let cache = StarRocksDescriptorCache::default();
        let first_descriptor = descriptor(4);
        let first = cache
            .prepare(query_id, Some(&first_descriptor), None)
            .expect("prepare first generation");
        let first_generation = first.generation();
        let first_lease = cache
            .commit(&first)
            .expect("commit first generation")
            .expect("first lease");
        cache
            .inner
            .lock()
            .expect("descriptor cache lock")
            .entries
            .remove(&query_id);

        let second_descriptor = descriptor(5);
        let second = cache
            .prepare(query_id, Some(&second_descriptor), None)
            .expect("prepare reused query id");
        let second_generation = second.generation();
        assert!(second_generation > first_generation);
        let second_lease = cache
            .commit(&second)
            .expect("commit reused query id")
            .expect("second lease");

        drop(first_lease);
        assert_eq!(cached_generation(&cache, query_id), Some(second_generation));
        drop(second_lease);
        assert_eq!(cached_generation(&cache, query_id), None);
    }

    #[test]
    fn old_generation_finst_cancel_cannot_cancel_reused_query_generation() {
        let query_id = QueryId {
            hi: 85_243,
            lo: 85_244,
        };
        let old_finst = UniqueId {
            hi: 85_245,
            lo: 85_246,
        };
        let new_finst = UniqueId {
            hi: 85_247,
            lo: 85_248,
        };
        let cache = StarRocksDescriptorCache::default();
        let manager = query_context_manager();

        let first = cache
            .prepare(query_id, Some(&descriptor(7)), None)
            .expect("prepare old generation");
        let first_generation = first.generation();
        let first_execution = QueryExecutionKey::starrocks(
            query_id,
            StarRocksQueryGeneration::new(first_generation).expect("old generation identity"),
        );
        let first_lease = cache
            .commit(&first)
            .expect("commit old generation")
            .expect("old descriptor lease");
        manager
            .get_or_register_compat_generation(
                first_execution,
                false,
                Duration::from_secs(60),
                Duration::from_secs(60),
            )
            .expect("register old context");
        manager
            .with_context_mut(query_id, |context| {
                context.total_fragments = Some(1);
                Ok(())
            })
            .expect("mark old generation final");
        manager
            .attach_cleanup_lease(query_id, first_lease)
            .expect("attach old descriptor lease");
        manager
            .register_starrocks_finsts([old_finst], first_execution)
            .expect("register old finst routing");
        manager.finish_fragment_execution(first_execution);
        assert_eq!(cached_generation(&cache, query_id), None);

        let second = cache
            .prepare(query_id, Some(&descriptor(8)), None)
            .expect("prepare reused query generation");
        let second_generation = second.generation();
        assert!(second_generation > first_generation);
        let second_execution = QueryExecutionKey::starrocks(
            query_id,
            StarRocksQueryGeneration::new(second_generation).expect("new generation identity"),
        );
        let second_lease = cache
            .commit(&second)
            .expect("commit reused generation")
            .expect("new descriptor lease");
        let blocked = manager.get_or_register_compat_generation(
            second_execution,
            false,
            Duration::from_secs(60),
            Duration::from_secs(60),
        );
        assert!(
            blocked.is_err_and(|error| error.contains("still has fragment routing")),
            "new generation must wait for old finst cleanup"
        );
        crate::service::fragment_control::cancel(old_finst);
        manager.unregister_finst_execution(old_finst, first_execution);

        manager
            .get_or_register_compat_generation(
                second_execution,
                false,
                Duration::from_secs(60),
                Duration::from_secs(60),
            )
            .expect("register reused context");
        manager
            .attach_cleanup_lease(query_id, second_lease)
            .expect("attach new descriptor lease");
        manager
            .register_starrocks_finsts([new_finst], second_execution)
            .expect("register reused finst routing");

        let stale_cancelled = manager.cancel_query_execution(
            first_execution,
            "stale old-generation cancellation".to_string(),
        );
        manager.finish_fragment_execution(first_execution);
        let new_generation_cancelled = manager.is_query_canceled(query_id);
        let new_counts = manager.fragment_counts_for_test(query_id);

        manager.unregister_finst_execution(new_finst, second_execution);
        manager.cancel_query_execution(second_execution, "test cleanup".to_string());
        manager.finish_fragment_execution(second_execution);

        assert!(stale_cancelled.is_empty());
        assert!(
            !new_generation_cancelled,
            "old generation finst routing must not cancel the reused query generation"
        );
        assert_eq!(new_counts, Some((1, 1)));
    }

    #[test]
    fn cleanup_lease_release_is_idempotent_with_drop() {
        let releases = Arc::new(AtomicUsize::new(0));
        let releases_for_lease = Arc::clone(&releases);
        QueryCleanupLease::new(move || {
            releases_for_lease.fetch_add(1, Ordering::SeqCst);
        })
        .release();

        assert_eq!(releases.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn descriptor_handoff_rolls_back_new_entry_when_query_preflight_rejects() {
        let cache = StarRocksDescriptorCache::default();
        let query_id = QueryId {
            hi: 85_247,
            lo: 85_248,
        };
        let preparation = cache
            .prepare(query_id, Some(&descriptor(9)), None)
            .expect("prepare descriptor");

        let result: Result<(), String> = cache.commit_handoff(&preparation, |_lease_factory| {
            Err("query preflight rejected handoff".to_string())
        });

        assert!(result.is_err());
        assert_eq!(cached_generation(&cache, query_id), None);
    }

    #[test]
    fn runtime_cancel_barrier_blocks_reused_finst_handoff_until_cleanup_finishes() {
        let registry = Arc::new(StarRocksPrelaunchRegistry::new());
        let query_id = QueryId {
            hi: 85_249,
            lo: 85_250,
        };
        let finst_id = UniqueId {
            hi: 85_251,
            lo: 85_252,
        };
        let (cleanup_started_tx, cleanup_started_rx) = mpsc::channel();
        let (release_cleanup_tx, release_cleanup_rx) = mpsc::channel();
        let cancel_registry = Arc::clone(&registry);
        let cancel = std::thread::spawn(move || {
            cancel_registry.cancel_or_run(finst_id, || {
                cleanup_started_tx.send(()).expect("signal cleanup start");
                release_cleanup_rx.recv().expect("release cleanup");
            })
        });
        cleanup_started_rx.recv().expect("runtime cleanup started");

        let (handoff_done_tx, handoff_done_rx) = mpsc::channel();
        let handoff_registry = Arc::clone(&registry);
        let handoff = std::thread::spawn(move || {
            let guard = handoff_registry
                .install(query_id, 2, [finst_id])
                .expect("reused finst prelaunch");
            handoff_done_tx.send(()).expect("signal handoff installed");
            drop(guard);
        });
        assert!(
            matches!(handoff_done_rx.try_recv(), Err(mpsc::TryRecvError::Empty)),
            "reused finst must not install while old runtime cleanup holds P"
        );
        release_cleanup_tx.send(()).expect("finish runtime cleanup");

        assert!(!cancel.join().expect("cancel thread"));
        handoff_done_rx
            .recv()
            .expect("handoff completes after cleanup");
        handoff.join().expect("handoff thread");
    }

    #[test]
    fn handoff_publishes_all_fragment_mappings_and_counts() {
        let registry = Arc::new(StarRocksPrelaunchRegistry::new());
        let query_id = QueryId {
            hi: 85_251,
            lo: 85_252,
        };
        let finst_ids = [
            UniqueId {
                hi: 85_253,
                lo: 85_254,
            },
            UniqueId {
                hi: 85_255,
                lo: 85_256,
            },
        ];
        let guard = registry
            .install(query_id, 1, finst_ids)
            .expect("install prelaunch guard");
        let manager = query_context_manager();

        guard
            .handoff(|| {
                for _ in &finst_ids {
                    manager.get_or_register_compat(
                        query_id,
                        false,
                        Duration::from_secs(60),
                        Duration::from_secs(60),
                    )?;
                }
                manager.register_finsts(finst_ids, query_id);
                Ok(())
            })
            .expect("handoff batch");

        assert_eq!(registry.snapshot_count(), 0);
        assert_eq!(manager.fragment_counts_for_test(query_id), Some((2, 2)));
        assert!(
            finst_ids
                .iter()
                .all(|finst_id| manager.query_id_by_finst(*finst_id) == Some(query_id))
        );

        manager.cancel_query(query_id, "test cleanup".to_string());
        for finst_id in finst_ids {
            manager.unregister_finst(finst_id);
            manager.finish_fragment(query_id);
        }
    }

    #[test]
    fn cancelled_prelaunch_handoff_commits_no_mappings_or_counts() {
        let registry = Arc::new(StarRocksPrelaunchRegistry::new());
        let query_id = QueryId {
            hi: 85_261,
            lo: 85_262,
        };
        let finst_ids = [
            UniqueId {
                hi: 85_263,
                lo: 85_264,
            },
            UniqueId {
                hi: 85_265,
                lo: 85_266,
            },
        ];
        let guard = registry
            .install(query_id, 1, finst_ids)
            .expect("install prelaunch guard");
        assert!(registry.cancel(finst_ids[1]));
        let manager = query_context_manager();

        let result = guard.handoff(|| {
            for _ in &finst_ids {
                manager.get_or_register_compat(
                    query_id,
                    false,
                    Duration::from_secs(60),
                    Duration::from_secs(60),
                )?;
            }
            manager.register_finsts(finst_ids, query_id);
            Ok(())
        });

        assert!(result.is_err());
        assert_eq!(registry.snapshot_count(), 0);
        assert_eq!(manager.fragment_counts_for_test(query_id), None);
        assert!(
            finst_ids
                .iter()
                .all(|finst_id| manager.query_id_by_finst(*finst_id).is_none())
        );
    }

    #[test]
    fn cancel_observes_prelaunch_or_runtime_mapping_without_gap() {
        let registry = Arc::new(StarRocksPrelaunchRegistry::new());
        let query_id = QueryId {
            hi: 85_271,
            lo: 85_272,
        };
        let finst_id = UniqueId {
            hi: 85_273,
            lo: 85_274,
        };
        let guard = registry
            .install(query_id, 1, [finst_id])
            .expect("install prelaunch guard");
        let manager = query_context_manager();
        let manager_for_handoff = Arc::clone(&manager);
        let (published_tx, published_rx) = mpsc::sync_channel(1);
        let (release_tx, release_rx) = mpsc::sync_channel(1);
        let handoff = std::thread::spawn(move || {
            guard.handoff(|| {
                manager_for_handoff.get_or_register_compat(
                    query_id,
                    false,
                    Duration::from_secs(60),
                    Duration::from_secs(60),
                )?;
                manager_for_handoff.register_finsts([finst_id], query_id);
                published_tx.send(()).expect("publish handoff state");
                release_rx.recv().expect("release handoff");
                Ok(())
            })
        });
        published_rx.recv().expect("runtime mapping published");

        let registry_for_cancel = Arc::clone(&registry);
        let manager_for_cancel = Arc::clone(&manager);
        let (observed_tx, observed_rx) = mpsc::sync_channel(1);
        let cancel = std::thread::spawn(move || {
            let observed = if registry_for_cancel.cancel(finst_id) {
                Some(query_id)
            } else {
                manager_for_cancel.query_id_by_finst(finst_id)
            };
            observed_tx.send(observed).expect("send cancel observation");
        });
        assert!(
            observed_rx.recv_timeout(Duration::from_millis(20)).is_err(),
            "cancel must wait for the prelaunch handoff lock"
        );
        release_tx.send(()).expect("release handoff");
        handoff.join().expect("handoff thread").expect("handoff");
        assert_eq!(
            observed_rx.recv().expect("cancel observation"),
            Some(query_id)
        );
        cancel.join().expect("cancel thread");

        manager.cancel_query(query_id, "test cleanup".to_string());
        manager.unregister_finst(finst_id);
        manager.finish_fragment(query_id);
    }

    #[test]
    fn cancel_during_dependency_resolution_launches_nothing() {
        let registry = Arc::new(StarRocksPrelaunchRegistry::new());
        let query_id = QueryId {
            hi: 85_201,
            lo: 85_202,
        };
        let finst_id = UniqueId {
            hi: 85_203,
            lo: 85_204,
        };
        let guard = registry
            .install(query_id, 1, [finst_id])
            .expect("install prelaunch guard");
        let token = guard.cancellation_token();
        let (entered_tx, entered_rx) = mpsc::sync_channel(1);
        let (release_tx, release_rx) = mpsc::sync_channel(1);
        let worker = std::thread::spawn(move || {
            entered_tx.send(()).expect("resolver entered");
            release_rx.recv().expect("resolver released");
            token.check(7)
        });
        entered_rx.recv().expect("resolver must block");

        assert!(registry.cancel(finst_id));
        release_tx.send(()).expect("release resolver");
        let result = worker.join().expect("resolver worker");

        assert!(matches!(
            result,
            Err(StarRocksDependencyResolutionError::Cancelled { dependency_id: 7 })
        ));
        drop(guard);
        assert_eq!(registry.snapshot_count(), 0);
        assert_eq!(super::descriptor_cache_snapshot_count(), 0);
        assert_eq!(
            crate::runtime::query_context::query_context_manager().query_id_by_finst(finst_id),
            None
        );
    }
}
