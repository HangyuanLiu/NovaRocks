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

pub mod cursor;
pub mod decode;
pub(crate) mod decode_helpers;
mod index_manifest_entry_decode;
pub(crate) mod manifest_entry_decode;
mod manifest_file_meta_decode;
pub mod ocf;
pub mod schema;

use crate::io::ReadControl;
use cursor::AvroCursor;
use decode::AvroRecordDecode;
use ocf::{parse_ocf_streaming, parse_ocf_streaming_with_control};
use schema::WriterSchema;
use std::mem::size_of;
use std::sync::{Arc, RwLock};

struct CachedWriterSchema {
    schema_json: String,
    schema: Arc<WriterSchema>,
    control: Option<Arc<dyn ReadControl>>,
}

/// Cache for parsed WriterSchemas, keyed by schema JSON string.
/// Same manifest type always produces the same schema JSON, so parsing
/// once and reusing across files within a scan saves repeated work.
/// Uses Vec instead of HashMap since Paimon tables typically have 1-2 distinct schemas.
pub struct SchemaCache {
    cache: Vec<CachedWriterSchema>,
}

impl SchemaCache {
    pub fn new() -> Self {
        Self { cache: Vec::new() }
    }

    pub fn get_or_parse(&mut self, schema_json: &str) -> crate::Result<Arc<WriterSchema>> {
        self.get_or_parse_with_control(schema_json, None)
    }

    pub(crate) fn get_or_parse_with_control(
        &mut self,
        schema_json: &str,
        control: Option<&Arc<dyn ReadControl>>,
    ) -> crate::Result<Arc<WriterSchema>> {
        if let Some(index) = self
            .cache
            .iter()
            .position(|cached| cached.schema_json == schema_json)
        {
            if let Some(control) = control {
                match self.cache[index].control.as_ref() {
                    Some(existing) if !Arc::ptr_eq(existing, control) => {
                        return Err(crate::Error::DataInvalid {
                            message: "avro schema cache cannot cross a read-control boundary"
                                .to_string(),
                            source: None,
                        });
                    }
                    Some(_) => {}
                    None => self.cache[index].control = Some(control.clone()),
                }
            }
            return Ok(Arc::clone(&self.cache[index].schema));
        }

        let ws = Arc::new(WriterSchema::parse(schema_json)?);
        if let Some(control) = control {
            control.checkpoint()?;
        }
        self.cache
            .try_reserve(1)
            .map_err(|error| crate::Error::DataInvalid {
                message: "avro schema cache cannot reserve an entry".to_string(),
                source: Some(Box::new(error)),
            })?;
        let mut owned_schema_json = String::new();
        owned_schema_json
            .try_reserve_exact(schema_json.len())
            .map_err(|error| crate::Error::DataInvalid {
                message: "avro schema cache cannot reserve its schema key".to_string(),
                source: Some(Box::new(error)),
            })?;
        owned_schema_json.push_str(schema_json);
        self.cache.push(CachedWriterSchema {
            schema_json: owned_schema_json,
            schema: Arc::clone(&ws),
            control: control.cloned(),
        });
        Ok(ws)
    }
}

impl Default for SchemaCache {
    fn default() -> Self {
        Self::new()
    }
}

/// Thread-safe schema cache for sharing across concurrent async tasks.
/// Wraps `SchemaCache` in `Arc<RwLock<_>>` so multiple tasks can reuse
/// the same parsed `WriterSchema` without re-parsing.
#[derive(Clone)]
pub struct SharedSchemaCache {
    inner: Arc<RwLock<SchemaCache>>,
}

impl SharedSchemaCache {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(RwLock::new(SchemaCache::new())),
        }
    }

    pub fn get_or_parse(&self, schema_json: &str) -> crate::Result<Arc<WriterSchema>> {
        // Fast path: read lock for cache hit
        {
            let cache = self.inner.read().unwrap_or_else(|e| e.into_inner());
            if let Some(cached) = cache
                .cache
                .iter()
                .find(|cached| cached.schema_json == schema_json)
            {
                return Ok(Arc::clone(&cached.schema));
            }
        }
        // Slow path: write lock for cache miss
        self.inner
            .write()
            .unwrap_or_else(|e| e.into_inner())
            .get_or_parse(schema_json)
    }

    pub(crate) fn get_or_parse_with_control(
        &self,
        schema_json: &str,
        control: Option<&Arc<dyn ReadControl>>,
    ) -> crate::Result<Arc<WriterSchema>> {
        // A controlled lookup takes the write lock even on a hit so checking
        // or installing the single cache-owned lease is atomic with lookup.
        self.inner
            .write()
            .unwrap_or_else(|error| error.into_inner())
            .get_or_parse_with_control(schema_json, control)
    }
}

impl Default for SharedSchemaCache {
    fn default() -> Self {
        Self::new()
    }
}

/// Read an Avro OCF file and decode records directly into `T`, bypassing
/// the intermediate `apache_avro::Value` representation.
pub fn from_avro_bytes_fast<T: AvroRecordDecode>(bytes: &[u8]) -> crate::Result<Vec<T>> {
    let mut cache = SchemaCache::new();
    from_avro_bytes_with_cache(bytes, &mut cache)
}

pub(crate) fn from_avro_bytes_fast_with_control<T: AvroRecordDecode>(
    bytes: &[u8],
    control: Option<Arc<dyn ReadControl>>,
) -> crate::Result<Vec<T>> {
    let mut cache = SchemaCache::new();
    from_avro_bytes_with_cache_and_control(bytes, &mut cache, control)
}

/// Same as `from_avro_bytes_fast` but reuses a `SchemaCache` across calls.
pub fn from_avro_bytes_with_cache<T: AvroRecordDecode>(
    bytes: &[u8],
    cache: &mut SchemaCache,
) -> crate::Result<Vec<T>> {
    from_avro_bytes_with_cache_and_control(bytes, cache, None)
}

fn from_avro_bytes_with_cache_and_control<T: AvroRecordDecode>(
    bytes: &[u8],
    cache: &mut SchemaCache,
    control: Option<Arc<dyn ReadControl>>,
) -> crate::Result<Vec<T>> {
    let (header, mut block_iter) = parse_ocf_streaming_with_control(bytes, control.clone())?;
    let writer_schema = cache.get_or_parse_with_control(&header.schema_json, control.as_ref())?;

    let mut results = Vec::new();
    while let Some(block) = block_iter.next_block()? {
        let (object_count, data) = block.into_parts();
        reserve_decode_results::<T>(&mut results, object_count, control.as_ref())?;
        let mut cursor = AvroCursor::new(data.as_ref());
        for index in 0..object_count {
            checkpoint_decode(control.as_ref(), index)?;
            let record = decode_top_level_record::<T>(&mut cursor, &writer_schema)?;
            results.push(record);
        }
    }

    Ok(results)
}

/// Decode ManifestEntry records from Avro OCF bytes with a lightweight filter.
///
/// The filter receives `(kind, partition_bytes, bucket, total_buckets)` and
/// returns true to keep the entry. Entries that fail the filter skip the
/// expensive `DataFileMeta` decoding entirely.
#[allow(dead_code)]
pub fn from_manifest_bytes_filtered<F>(
    bytes: &[u8],
    cache: &mut SchemaCache,
    filter: &mut F,
) -> crate::Result<Vec<crate::spec::ManifestEntry>>
where
    F: FnMut(crate::spec::FileKind, &[u8], i32, i32) -> bool,
{
    let (header, mut block_iter) = parse_ocf_streaming(bytes)?;
    let writer_schema = cache.get_or_parse(&header.schema_json)?;
    decode_manifest_streaming(&mut block_iter, &writer_schema, None, filter)
}

pub(crate) fn from_manifest_bytes_filtered_shared_with_control<F>(
    bytes: &[u8],
    shared_cache: &SharedSchemaCache,
    control: Option<Arc<dyn ReadControl>>,
    filter: &mut F,
) -> crate::Result<Vec<crate::spec::ManifestEntry>>
where
    F: FnMut(crate::spec::FileKind, &[u8], i32, i32) -> bool,
{
    let (header, mut block_iter) = parse_ocf_streaming_with_control(bytes, control.clone())?;
    let writer_schema =
        shared_cache.get_or_parse_with_control(&header.schema_json, control.as_ref())?;
    decode_manifest_streaming(&mut block_iter, &writer_schema, control, filter)
}

/// Decode ManifestEntry records from Avro OCF bytes using a pre-resolved shared schema.
///
/// Use this when the `WriterSchema` is shared across concurrent tasks via
/// `SharedSchemaCache`. Falls back to parsing if the OCF schema differs.
#[allow(dead_code)]
pub fn from_manifest_bytes_filtered_shared<F>(
    bytes: &[u8],
    shared_cache: &SharedSchemaCache,
    filter: &mut F,
) -> crate::Result<Vec<crate::spec::ManifestEntry>>
where
    F: FnMut(crate::spec::FileKind, &[u8], i32, i32) -> bool,
{
    let (header, mut block_iter) = parse_ocf_streaming(bytes)?;
    let writer_schema = shared_cache.get_or_parse(&header.schema_json)?;
    decode_manifest_streaming(&mut block_iter, &writer_schema, None, filter)
}

pub(crate) fn decode_manifest_streaming<F>(
    block_iter: &mut ocf::OcfBlockIter<'_>,
    writer_schema: &WriterSchema,
    control: Option<Arc<dyn ReadControl>>,
    filter: &mut F,
) -> crate::Result<Vec<crate::spec::ManifestEntry>>
where
    F: FnMut(crate::spec::FileKind, &[u8], i32, i32) -> bool,
{
    let mut results = Vec::new();
    while let Some(block) = block_iter.next_block()? {
        let (object_count, data) = block.into_parts();
        reserve_decode_results::<crate::spec::ManifestEntry>(
            &mut results,
            object_count,
            control.as_ref(),
        )?;
        let mut cursor = AvroCursor::new(data.as_ref());
        for index in 0..object_count {
            checkpoint_decode(control.as_ref(), index)?;
            if let Some(entry) = manifest_entry_decode::decode_manifest_entries_filtered(
                &mut cursor,
                writer_schema,
                writer_schema.is_union_wrapped,
                filter,
            )? {
                results.push(entry);
            }
        }
    }
    Ok(results)
}

fn reserve_decode_results<T>(
    results: &mut Vec<T>,
    object_count: usize,
    control: Option<&Arc<dyn ReadControl>>,
) -> crate::Result<()> {
    let _ = object_count
        .checked_mul(size_of::<T>().max(1))
        .ok_or_else(|| crate::Error::DataInvalid {
            message: "avro ocf: decoded object allocation size overflow".to_string(),
            source: None,
        })?;
    if let Some(control) = control {
        control.checkpoint()?;
    }
    results
        .try_reserve(object_count)
        .map_err(|error| crate::Error::DataInvalid {
            message: format!(
                "avro ocf: cannot reserve capacity for {object_count} decoded objects"
            ),
            source: Some(Box::new(error)),
        })
}

fn checkpoint_decode(
    control: Option<&Arc<dyn ReadControl>>,
    object_index: usize,
) -> crate::Result<()> {
    if object_index % 256 == 0 {
        if let Some(control) = control {
            control.checkpoint()?;
        }
    }
    Ok(())
}

/// Decode a single record from the cursor, handling the top-level union wrapper
/// that Paimon uses (`["null", record]`).
fn decode_top_level_record<T: AvroRecordDecode>(
    cursor: &mut AvroCursor,
    writer_schema: &WriterSchema,
) -> crate::Result<T> {
    if writer_schema.is_union_wrapped {
        let idx = cursor.read_union_index()?;
        if idx == 0 {
            return Err(crate::Error::UnexpectedError {
                message: "avro decode: unexpected null in top-level union".into(),
                source: None,
            });
        }
    }
    T::decode(cursor, writer_schema)
}

#[cfg(test)]
mod plain_read_tests {
    use super::*;
    use crate::spec::manifest_file_meta::MANIFEST_FILE_META_SCHEMA;
    use crate::spec::stats::BinaryTableStats;
    use crate::spec::ManifestFileMeta;
    use std::thread;

    #[test]
    fn plain_decode_accepts_supported_codecs_without_resource_authority() {
        for compression in ["null", "snappy", "zstd"] {
            let original = vec![ManifestFileMeta::new(
                format!("manifest-{compression}-{}", "x".repeat(4096)),
                128,
                1,
                0,
                BinaryTableStats::empty(),
                0,
            )];
            let bytes = crate::spec::to_avro_bytes_with_compression(
                MANIFEST_FILE_META_SCHEMA,
                &original,
                compression,
            )
            .unwrap();
            let decoded = from_avro_bytes_fast::<ManifestFileMeta>(&bytes).unwrap();
            assert_eq!(decoded, original);
        }
    }

    #[test]
    fn schema_cache_reuses_one_plain_schema() {
        let schema_json =
            r#"{"type":"record","name":"test","fields":[{"name":"value","type":"string"}]}"#;
        let mut cache = SchemaCache::new();
        let first = cache.get_or_parse(schema_json).unwrap();
        let second = cache.get_or_parse(schema_json).unwrap();
        assert!(Arc::ptr_eq(&first, &second));
    }

    #[test]
    fn shared_schema_cache_concurrent_miss_reuses_plain_schema() {
        let schema_json = Arc::<str>::from(
            r#"{"type":"record","name":"test","fields":[{"name":"value","type":"long"}]}"#,
        );
        let cache = SharedSchemaCache::new();
        let handles = (0..8)
            .map(|_| {
                let cache = cache.clone();
                let schema_json = schema_json.clone();
                thread::spawn(move || cache.get_or_parse(&schema_json).unwrap())
            })
            .collect::<Vec<_>>();
        let schemas = handles
            .into_iter()
            .map(|handle| handle.join().unwrap())
            .collect::<Vec<_>>();
        assert!(schemas
            .iter()
            .all(|schema| Arc::ptr_eq(schema, &schemas[0])));
    }

    #[test]
    fn malformed_schema_is_rejected_without_resource_authority() {
        let mut cache = SchemaCache::new();
        assert!(cache.get_or_parse("[").is_err());
    }
}
