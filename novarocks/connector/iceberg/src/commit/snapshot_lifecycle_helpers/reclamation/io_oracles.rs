// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

use super::*;
use crate::iceberg::io::{
    FileIOBuilder, FileMetadata, FileRead, FileWrite, InputFile, OutputFile, Storage,
    StorageConfig, StorageFactory,
};
use crate::iceberg::spec::{
    DataFileBuilder, ManifestListWriter, ManifestWriterBuilder, Operation, Struct, Summary,
};
use bytes::Bytes;
use futures::stream::BoxStream;
use std::sync::{Arc, Mutex};

#[derive(Debug)]
struct CounterState {
    original: FileIO,
    calls: Mutex<Vec<(String, String)>>,
}
impl CounterState {
    fn record(&self, kind: &str, path: &str) {
        self.calls.lock().unwrap().push((kind.into(), path.into()));
    }
    fn count(&self, kind: &str, path: &str) -> usize {
        self.calls
            .lock()
            .unwrap()
            .iter()
            .filter(|(k, p)| k == kind && p == path)
            .count()
    }
}
#[derive(Debug, Clone)]
struct CounterStorage(Arc<CounterState>);
#[derive(Debug)]
struct CounterFactory(Arc<CounterState>);
macro_rules! nonportable {
    ($name:ty) => {
        impl serde::Serialize for $name {
            fn serialize<S: serde::Serializer>(
                &self,
                _: S,
            ) -> std::result::Result<S::Ok, S::Error> {
                Err(serde::ser::Error::custom("I/O oracle is process local"))
            }
        }
        impl<'de> serde::Deserialize<'de> for $name {
            fn deserialize<D: serde::Deserializer<'de>>(
                _: D,
            ) -> std::result::Result<Self, D::Error> {
                Err(serde::de::Error::custom("I/O oracle is process local"))
            }
        }
    };
}
nonportable!(CounterStorage);
nonportable!(CounterFactory);
#[typetag::serde(name = "IcebergReclamationIoOracleFactory")]
impl StorageFactory for CounterFactory {
    fn build(&self, _: &StorageConfig) -> crate::iceberg::Result<Arc<dyn Storage>> {
        Ok(Arc::new(CounterStorage(Arc::clone(&self.0))))
    }
}
#[async_trait::async_trait]
#[typetag::serde(name = "IcebergReclamationIoOracleStorage")]
impl Storage for CounterStorage {
    async fn exists(&self, path: &str) -> crate::iceberg::Result<bool> {
        self.0.record("EXISTS", path);
        self.0.original.new_input(path)?.exists().await
    }
    async fn metadata(&self, path: &str) -> crate::iceberg::Result<FileMetadata> {
        self.0.record("HEAD", path);
        self.0.original.new_input(path)?.metadata().await
    }
    async fn read(&self, path: &str) -> crate::iceberg::Result<Bytes> {
        self.0.record("GET", path);
        self.0.original.new_input(path)?.read().await
    }
    async fn reader(&self, path: &str) -> crate::iceberg::Result<Box<dyn FileRead>> {
        self.0.record("RANGE_OPEN", path);
        self.0.original.new_input(path)?.reader().await
    }
    async fn write(&self, path: &str, bytes: Bytes) -> crate::iceberg::Result<()> {
        self.0.record("WRITE", path);
        self.0.original.new_output(path)?.write(bytes).await
    }
    async fn writer(&self, path: &str) -> crate::iceberg::Result<Box<dyn FileWrite>> {
        self.0.record("WRITER", path);
        self.0.original.new_output(path)?.writer().await
    }
    async fn delete(&self, path: &str) -> crate::iceberg::Result<()> {
        self.0.record("DELETE", path);
        self.0.original.delete(path).await
    }
    async fn delete_prefix(&self, path: &str) -> crate::iceberg::Result<()> {
        self.0.original.delete_prefix(path).await
    }
    async fn delete_stream(&self, paths: BoxStream<'static, String>) -> crate::iceberg::Result<()> {
        self.0.original.delete_stream(paths).await
    }
    fn new_input(&self, path: &str) -> crate::iceberg::Result<InputFile> {
        Ok(InputFile::new(Arc::new(self.clone()), path.into()))
    }
    fn new_output(&self, path: &str) -> crate::iceberg::Result<OutputFile> {
        self.0.original.new_output(path)
    }
}
async fn root(io: &FileIO, id: i64, list: &str, manifest: ManifestFile) -> Snapshot {
    let mut writer = ManifestListWriter::v2(
        io.new_output(list).unwrap().writer().await.unwrap(),
        id,
        None,
        id,
    );
    writer.add_manifests([manifest].into_iter()).unwrap();
    writer.close().await.unwrap();
    Snapshot::builder()
        .with_snapshot_id(id)
        .with_sequence_number(id)
        .with_timestamp_ms(id * 1000)
        .with_manifest_list(list)
        .with_schema_id(0)
        .with_summary(Summary {
            operation: Operation::Append,
            additional_properties: HashMap::new(),
        })
        .build()
}
#[tokio::test]
async fn shared_manifest_root_deltas_do_one_real_get_and_zero_head_and_inline_conflict_stops() {
    let original = FileIO::new_with_memory();
    let metadata = super::tests::metadata();
    let output = original.new_output("memory://gc/shared.avro").unwrap();
    let mut writer = ManifestWriterBuilder::new(
        output,
        Some(1),
        metadata.current_schema().clone(),
        metadata.default_partition_spec().as_ref().clone(),
    )
    .build_v2_data();
    let file = DataFileBuilder::default()
        .content(DataContentType::Data)
        .file_path("memory://gc/live.parquet".into())
        .file_format(DataFileFormat::Parquet)
        .partition(Struct::empty())
        .partition_spec_id(0)
        .record_count(3)
        .file_size_in_bytes(30)
        .build()
        .unwrap();
    writer.add_existing_file(file, 1, 1, Some(1)).unwrap();
    // A manifest reused by later snapshots carries its committed sequence;
    // a writer's -1 sequence is only inheritable by its creating snapshot.
    let mut manifest = writer.write_manifest_file().await.unwrap();
    manifest.sequence_number = 1;
    manifest.min_sequence_number = 1;
    let roots = vec![
        root(&original, 1, "memory://gc/a.avro", manifest.clone()).await,
        root(&original, 2, "memory://gc/b.avro", manifest.clone()).await,
        root(&original, 3, "memory://gc/c.avro", manifest.clone()).await,
    ];
    let mut changed = manifest.clone();
    changed.manifest_length += 1;
    let contradictory = root(&original, 4, "memory://gc/d.avro", changed).await;
    let state = Arc::new(CounterState {
        original,
        calls: Mutex::new(Vec::new()),
    });
    let io = FileIOBuilder::new(Arc::new(CounterFactory(state.clone()))).build();
    let mut cache = ScanCache::default();
    let mut live = Reachability::default();
    for snapshot in &roots[..2] {
        live.extend(
            &cache
                .snapshot(snapshot, FormatVersion::V2, &io)
                .await
                .unwrap()
                .live,
        );
    }
    assert_eq!(state.count("GET", "memory://gc/shared.avro"), 1);
    assert_eq!(state.count("GET", "memory://gc/a.avro"), 1);
    assert_eq!(state.count("GET", "memory://gc/b.avro"), 1);
    cache
        .snapshot(&roots[0], FormatVersion::V2, &io)
        .await
        .unwrap();
    cache
        .snapshot(&roots[2], FormatVersion::V2, &io)
        .await
        .unwrap();
    assert_eq!(state.count("GET", "memory://gc/shared.avro"), 1);
    assert_eq!(state.count("GET", "memory://gc/a.avro"), 1);
    assert_eq!(state.count("GET", "memory://gc/c.avro"), 1);
    assert!(live.paths.contains("memory://gc/live.parquet"));
    assert!(
        cache
            .snapshot(&contradictory, FormatVersion::V2, &io)
            .await
            .is_err()
    );
    assert_eq!(state.count("GET", "memory://gc/shared.avro"), 1);
    assert!(!cache.roots.contains_key(&(4, "memory://gc/d.avro".into())));
    assert!(
        state
            .calls
            .lock()
            .unwrap()
            .iter()
            .all(|(kind, _)| kind != "HEAD")
    );
}

#[tokio::test]
async fn one_complete_decode_keeps_deleted_entries_as_candidates_without_live_protection() {
    let original = FileIO::new_with_memory();
    let metadata = super::tests::metadata();
    let mut writer = ManifestWriterBuilder::new(
        original.new_output("memory://gc/mixed.avro").unwrap(),
        Some(1),
        metadata.current_schema().clone(),
        metadata.default_partition_spec().as_ref().clone(),
    )
    .build_v2_data();
    let file = |path: &str| {
        DataFileBuilder::default()
            .content(DataContentType::Data)
            .file_path(path.to_owned())
            .file_format(DataFileFormat::Parquet)
            .partition(Struct::empty())
            .partition_spec_id(0)
            .record_count(1)
            .file_size_in_bytes(10)
            .build()
            .unwrap()
    };
    writer
        .add_existing_file(file("memory://gc/alive.parquet"), 1, 1, Some(1))
        .unwrap();
    writer
        .add_delete_file(file("memory://gc/deleted.parquet"), 1, Some(1))
        .unwrap();
    let descriptor = writer.write_manifest_file().await.unwrap();
    let snapshot = root(&original, 1, "memory://gc/mixed-list.avro", descriptor).await;
    let state = Arc::new(CounterState {
        original,
        calls: Mutex::new(Vec::new()),
    });
    let io = FileIOBuilder::new(Arc::new(CounterFactory(state.clone()))).build();
    let mut cache = ScanCache::default();
    let decoded = cache
        .snapshot(&snapshot, FormatVersion::V2, &io)
        .await
        .unwrap();
    assert_eq!(
        decoded.live.paths,
        BTreeSet::from([
            "memory://gc/mixed.avro".into(),
            "memory://gc/mixed-list.avro".into(),
            "memory://gc/alive.parquet".into()
        ])
    );
    assert_eq!(
        decoded.candidates.paths,
        BTreeSet::from([
            "memory://gc/mixed.avro".into(),
            "memory://gc/mixed-list.avro".into(),
            "memory://gc/alive.parquet".into(),
            "memory://gc/deleted.parquet".into()
        ])
    );
    cache
        .snapshot(&snapshot, FormatVersion::V2, &io)
        .await
        .unwrap();
    assert_eq!(state.count("GET", "memory://gc/mixed.avro"), 1);
    assert_eq!(state.count("GET", "memory://gc/mixed-list.avro"), 1);
    assert!(
        state
            .calls
            .lock()
            .unwrap()
            .iter()
            .all(|(kind, _)| kind != "HEAD")
    );
}
