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

use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};

use parquet::arrow::arrow_reader::ArrowReaderMetadata;

use crate::{DataCacheManager, DataCachePageKey, FileIdentity};

const PARQUET_METADATA_CACHE_NAMESPACE: &str = "physical.parquet.metadata";

#[derive(Clone, Debug)]
pub struct ParquetCacheOptions {
    pub enable_metadata: bool,
    pub metadata_ttl: Duration,
    pub enable_page: bool,
}

impl Default for ParquetCacheOptions {
    fn default() -> Self {
        Self {
            enable_metadata: true,
            metadata_ttl: Duration::from_secs(3600),
            enable_page: true,
        }
    }
}

#[derive(Clone, Debug)]
struct TimedArrowReaderMetadata {
    metadata: ArrowReaderMetadata,
    expire_at: Instant,
}

static PARQUET_CACHE_OPTIONS: OnceLock<ParquetCacheOptions> = OnceLock::new();

pub fn init_parquet_cache(options: ParquetCacheOptions) -> bool {
    PARQUET_CACHE_OPTIONS.set(options).is_ok()
}

pub(crate) fn metadata_get(
    request_cache_enabled: bool,
    identity: &FileIdentity,
) -> Option<ArrowReaderMetadata> {
    if !request_cache_enabled || !options().enable_metadata {
        return None;
    }
    let cache = DataCacheManager::instance().page_cache()?;
    let cached = cache.lookup::<TimedArrowReaderMetadata>(&metadata_key(identity))?;
    if cached.expire_at <= Instant::now() {
        return None;
    }
    Some(cached.metadata.clone())
}

pub(crate) fn metadata_put(
    request_cache_enabled: bool,
    identity: &FileIdentity,
    metadata: ArrowReaderMetadata,
) {
    if !request_cache_enabled || !options().enable_metadata {
        return;
    }
    let Some(cache) = DataCacheManager::instance().page_cache() else {
        return;
    };
    let value = TimedArrowReaderMetadata {
        metadata,
        expire_at: Instant::now()
            .checked_add(options().metadata_ttl)
            .unwrap_or_else(Instant::now),
    };
    let _ = cache.insert(metadata_key(identity), Arc::new(value), 1, None);
}

pub(crate) fn page_cache_enabled(request_cache_enabled: bool) -> bool {
    request_cache_enabled && options().enable_page
}

fn options() -> &'static ParquetCacheOptions {
    PARQUET_CACHE_OPTIONS.get_or_init(ParquetCacheOptions::default)
}

fn metadata_key(identity: &FileIdentity) -> DataCachePageKey {
    let key = format!(
        "{}\0{}\0{}",
        identity.path(),
        identity.file_size(),
        identity.modification_time().unwrap_or_default()
    );
    DataCachePageKey::new(PARQUET_METADATA_CACHE_NAMESPACE, key.into_bytes())
}
