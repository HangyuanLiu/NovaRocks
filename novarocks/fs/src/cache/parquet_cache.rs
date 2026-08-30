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

use std::mem::size_of;
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};

use novarocks_spi::connector::StorageAccessDomainId;
use parquet::arrow::arrow_reader::ArrowReaderMetadata;

use crate::{DataCacheManager, DataCachePageCache, DataCachePageKey, FileIdentity};

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

/// Returns a conservative byte charge for one cached Arrow reader metadata
/// value. `ParquetMetaData::memory_size` accounts for the decoded footer,
/// including page indexes, while the Arrow schema is separately allocated by
/// `ArrowReaderMetadata::try_new`.
fn metadata_cache_charge(metadata: &ArrowReaderMetadata) -> usize {
    let schema = metadata.schema();
    let schema_bytes = size_of_val(schema.as_ref())
        .saturating_add(schema.fields.size())
        .saturating_add(size_of::<(String, String)>().saturating_mul(schema.metadata.capacity()))
        .saturating_add(
            schema
                .metadata
                .iter()
                .map(|(key, value)| key.capacity().saturating_add(value.capacity()))
                .sum::<usize>(),
        );
    metadata
        .metadata()
        .memory_size()
        .saturating_add(schema_bytes)
        .saturating_add(size_of::<TimedArrowReaderMetadata>())
        .max(1)
}

fn insert_metadata(
    cache: &DataCachePageCache,
    key: DataCachePageKey,
    metadata: ArrowReaderMetadata,
    expire_at: Instant,
) -> bool {
    let charge = metadata_cache_charge(&metadata);
    cache.insert(
        key,
        Arc::new(TimedArrowReaderMetadata {
            metadata,
            expire_at,
        }),
        charge,
        None,
    )
}

static PARQUET_CACHE_OPTIONS: OnceLock<ParquetCacheOptions> = OnceLock::new();

pub fn init_parquet_cache(options: ParquetCacheOptions) -> bool {
    PARQUET_CACHE_OPTIONS.set(options).is_ok()
}

pub(crate) fn metadata_get(
    request_cache_enabled: bool,
    access_domain: StorageAccessDomainId,
    identity: &FileIdentity,
    require_page_index: bool,
) -> Option<ArrowReaderMetadata> {
    if !request_cache_enabled || !options().enable_metadata {
        return None;
    }
    let cache = DataCacheManager::instance().page_cache()?;
    let cached =
        cache.lookup::<TimedArrowReaderMetadata>(&metadata_key(access_domain, identity))?;
    if cached.expire_at <= Instant::now() {
        return None;
    }
    let metadata = cached.metadata.clone();
    // A footer-only metadata entry was decoded with `PageIndexPolicy::Skip`.
    // It is a valid cache hit for footer consumers, but cannot satisfy a
    // reader that needs both index structures to make a pruning decision.
    // Returning it here would silently turn an enabled page-index read into a
    // fallback, so require a complete capability match instead.
    if require_page_index
        && (metadata.metadata().column_index().is_none()
            || metadata.metadata().offset_index().is_none())
    {
        return None;
    }
    Some(metadata)
}

pub(crate) fn metadata_put(
    request_cache_enabled: bool,
    access_domain: StorageAccessDomainId,
    identity: &FileIdentity,
    metadata: ArrowReaderMetadata,
) {
    if !request_cache_enabled || !options().enable_metadata {
        return;
    }
    let Some(cache) = DataCacheManager::instance().page_cache() else {
        return;
    };
    let expire_at = Instant::now()
        .checked_add(options().metadata_ttl)
        .unwrap_or_else(Instant::now);
    let _ = insert_metadata(
        &cache,
        metadata_key(access_domain, identity),
        metadata,
        expire_at,
    );
}

pub(crate) fn page_cache_enabled(request_cache_enabled: bool) -> bool {
    request_cache_enabled && options().enable_page
}

fn options() -> &'static ParquetCacheOptions {
    PARQUET_CACHE_OPTIONS.get_or_init(ParquetCacheOptions::default)
}

fn metadata_key(access_domain: StorageAccessDomainId, identity: &FileIdentity) -> DataCachePageKey {
    let key = format!(
        "{}\0{}\0{}",
        identity.path(),
        identity.file_size(),
        identity.modification_time().unwrap_or_default()
    );
    DataCachePageKey::new(
        access_domain,
        PARQUET_METADATA_CACHE_NAMESPACE,
        key.into_bytes(),
    )
}

#[cfg(test)]
mod tests {
    use std::fs::File;

    use arrow::array::Int32Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use parquet::arrow::ArrowWriter;
    use parquet::arrow::arrow_reader::ArrowReaderOptions;

    use super::*;

    fn sample_metadata() -> ArrowReaderMetadata {
        let file = tempfile::NamedTempFile::new().expect("create Parquet metadata fixture");
        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int32,
            false,
        )]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
        )
        .expect("create Parquet metadata batch");
        let mut writer = ArrowWriter::try_new(
            file.reopen().expect("reopen Parquet metadata fixture"),
            schema,
            None,
        )
        .expect("create Parquet metadata writer");
        writer.write(&batch).expect("write Parquet metadata batch");
        writer.close().expect("close Parquet metadata writer");
        let reader = File::open(file.path()).expect("open Parquet metadata fixture");
        ArrowReaderMetadata::load(&reader, ArrowReaderOptions::new())
            .expect("load Parquet metadata fixture")
    }

    #[test]
    fn metadata_key_is_scoped_before_physical_identity() {
        let identity = FileIdentity::new("s3://bucket/data.parquet", 42, Some(7));
        let first = metadata_key(StorageAccessDomainId::from_bytes([1; 32]), &identity);
        let second = metadata_key(StorageAccessDomainId::from_bytes([2; 32]), &identity);
        assert_ne!(first, second);
        assert_eq!(first.namespace(), second.namespace());
        assert_eq!(first.key(), second.key());
    }

    #[test]
    fn metadata_entries_charge_their_decoded_footer_and_evict_at_byte_capacity() {
        let metadata = sample_metadata();
        let charge = metadata_cache_charge(&metadata);
        assert!(
            charge > 1,
            "metadata entries must not consume one cache byte"
        );

        let cache = DataCachePageCache::new(crate::DataCachePageCacheOptions {
            capacity: charge,
            evict_probability: 100,
        });
        let first = DataCachePageKey::new(
            StorageAccessDomainId::from_bytes([1; 32]),
            PARQUET_METADATA_CACHE_NAMESPACE,
            b"first".to_vec(),
        );
        let second = DataCachePageKey::new(
            StorageAccessDomainId::from_bytes([1; 32]),
            PARQUET_METADATA_CACHE_NAMESPACE,
            b"second".to_vec(),
        );
        let expires = Instant::now() + Duration::from_secs(60);

        assert!(insert_metadata(
            &cache,
            first.clone(),
            metadata.clone(),
            expires,
        ));
        assert_eq!(cache.stats().size, charge);
        assert!(insert_metadata(&cache, second.clone(), metadata, expires));
        assert!(
            cache.lookup::<TimedArrowReaderMetadata>(&first).is_none(),
            "the second footer must evict the first when the byte budget holds one"
        );
        assert!(cache.lookup::<TimedArrowReaderMetadata>(&second).is_some());
        assert_eq!(cache.stats().size, charge);
    }
}
