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

//! Shared metadata helpers for v3 row-lineage deletion-vector commits.

use std::collections::{BTreeMap, HashMap, HashSet};

use crate::iceberg::io::FileIO;
use crate::iceberg::spec::{
    DataContentType, DataFile, DataFileBuilder, DataFileFormat, ManifestContentType, ManifestFile,
    ManifestWriterBuilder, PartitionSpecRef, SchemaRef, TableMetadata,
};
use crate::iceberg::table::Table;

use crate::commit::WrittenFile;
use crate::commit::WrittenPuffinDv;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WrittenDvFile {
    pub path: String,
    pub referenced_data_file: String,
    pub cardinality: u64,
    pub content_offset: i64,
    pub content_size_in_bytes: i64,
    pub file_size_in_bytes: u64,
}

impl From<WrittenPuffinDv> for WrittenDvFile {
    fn from(written: WrittenPuffinDv) -> Self {
        Self {
            path: written.path,
            referenced_data_file: written.referenced_data_file,
            cardinality: written.cardinality,
            content_offset: written.content_offset,
            content_size_in_bytes: written.content_size_in_bytes,
            file_size_in_bytes: written.file_size_in_bytes,
        }
    }
}

#[derive(Clone)]
pub struct LiveFile {
    pub data_file: DataFile,
    pub partition_spec_id: i32,
    pub snapshot_id: i64,
    pub sequence_number: i64,
    pub file_sequence_number: Option<i64>,
}

pub struct SnapshotIndex {
    /// Live data files keyed by `file_path()`.
    pub data_files: HashMap<String, LiveFile>,
    /// Manifests we did NOT touch; preserved verbatim in the new manifest list.
    pub untouched_manifests: Vec<ManifestFile>,
    /// Live delete entries from touched delete manifests that the current
    /// DELETE did not affect (i.e., reference some other data file). They are
    /// rewritten into a new `*-row-delta-dv-existing-*.avro` so the DV lineage
    /// is preserved for unrelated data files.
    pub touched_delete_existing: Vec<LiveFile>,
    /// Live DV files removed because a replacement DV was written.
    pub replaced_delete_files: usize,
    /// Exact paths of the live Puffin DV files removed from touched delete
    /// manifests.  A distributed rewrite uses this to prove that its frozen
    /// input set has neither shrunk nor grown before it submits the one
    /// replacement snapshot.
    pub replaced_delete_paths: HashSet<String>,
    /// Position deletes already represented by removed DV files.
    pub replaced_delete_records: u64,
    /// Total file_size_in_bytes of replaced DV files.
    pub replaced_delete_files_size: u64,
}

pub async fn build_snapshot_index_metadata_only(
    table: &Table,
    file_io: &FileIO,
    touched_files: &HashSet<String>,
    target_ref: &str,
) -> Result<SnapshotIndex, String> {
    build_snapshot_index(table, file_io, touched_files, target_ref).await
}

async fn build_snapshot_index(
    table: &Table,
    file_io: &FileIO,
    touched_files: &HashSet<String>,
    target_ref: &str,
) -> Result<SnapshotIndex, String> {
    let mut data_files = HashMap::new();
    let mut untouched_manifests = Vec::new();
    let mut touched_delete_existing = Vec::new();
    let mut replaced_delete_files = 0usize;
    let mut replaced_delete_paths = HashSet::new();
    let mut replaced_delete_files_size = 0u64;
    let mut replaced_delete_records = 0u64;
    let m = table.metadata();
    // For branch-targeted deletes, read the manifest list from the branch head
    // snapshot (not from main's current snapshot). This ensures that files added
    // to the branch by prior branch DML are visible and carry forward correctly.
    let snapshot = if target_ref == "main" {
        m.current_snapshot()
            .ok_or_else(|| "row-lineage DELETE requires a current snapshot".to_string())?
    } else {
        let branch_snapshot_id = m.refs().get(target_ref).map(|r| r.snapshot_id).ok_or_else(
            || {
                format!(
                    "row-lineage DELETE target branch '{target_ref}' not found in table metadata"
                )
            },
        )?;
        m.snapshot_by_id(branch_snapshot_id)
            .ok_or_else(|| format!("row-lineage DELETE branch '{target_ref}' snapshot {branch_snapshot_id} not found in metadata"))?
    };
    let list = snapshot
        .load_manifest_list(file_io, table.metadata())
        .await
        .map_err(|e| format!("load manifest list failed: {e}"))?;

    for mf in list.entries() {
        match mf.content {
            ManifestContentType::Data => {
                let manifest = mf
                    .load_manifest(file_io)
                    .await
                    .map_err(|e| format!("load data manifest {} failed: {e}", mf.manifest_path))?;
                for entry in manifest.entries() {
                    if !entry.is_alive() {
                        continue;
                    }
                    let seq = entry.sequence_number().unwrap_or(mf.sequence_number);
                    let file_seq = entry.file_sequence_number;
                    let snapshot_id = entry.snapshot_id().unwrap_or(mf.added_snapshot_id);
                    let file = entry.data_file().clone();
                    data_files.insert(
                        file.file_path().to_string(),
                        LiveFile {
                            data_file: file,
                            partition_spec_id: mf.partition_spec_id,
                            snapshot_id,
                            sequence_number: seq,
                            file_sequence_number: file_seq,
                        },
                    );
                }
                untouched_manifests.push(mf.clone());
            }
            ManifestContentType::Deletes => {
                let manifest = mf.load_manifest(file_io).await.map_err(|e| {
                    format!("load delete manifest {} failed: {e}", mf.manifest_path)
                })?;
                let mut manifest_touched = false;
                let mut keep: Vec<LiveFile> = Vec::new();
                for entry in manifest.entries() {
                    if !entry.is_alive() {
                        continue;
                    }
                    let seq = entry.sequence_number().unwrap_or(mf.sequence_number);
                    let file_seq = entry.file_sequence_number;
                    let snapshot_id = entry.snapshot_id().unwrap_or(mf.added_snapshot_id);
                    let file = entry.data_file().clone();
                    validate_delete_file_for_row_lineage(&file)?;
                    let referenced = file.referenced_data_file().ok_or_else(|| {
                        format!(
                            "Puffin DV {} missing referenced_data_file",
                            file.file_path()
                        )
                    })?;
                    if touched_files.contains(&referenced) {
                        if !replaced_delete_paths.insert(file.file_path().to_string()) {
                            return Err(format!(
                                "duplicate live Puffin DV path {} in Iceberg manifest list",
                                file.file_path()
                            ));
                        }
                        replaced_delete_records = replaced_delete_records
                            .checked_add(file.record_count())
                            .ok_or_else(|| "replaced DV record_count overflow".to_string())?;
                        replaced_delete_files += 1;
                        replaced_delete_files_size = replaced_delete_files_size
                            .checked_add(file.file_size_in_bytes())
                            .ok_or_else(|| "replaced DV file_size_in_bytes overflow".to_string())?;
                        manifest_touched = true;
                    } else {
                        keep.push(LiveFile {
                            data_file: file,
                            partition_spec_id: mf.partition_spec_id,
                            snapshot_id,
                            sequence_number: seq,
                            file_sequence_number: file_seq,
                        });
                    }
                }
                if manifest_touched {
                    touched_delete_existing.extend(keep);
                } else {
                    untouched_manifests.push(mf.clone());
                }
            }
        }
    }

    Ok(SnapshotIndex {
        data_files,
        untouched_manifests,
        touched_delete_existing,
        replaced_delete_files,
        replaced_delete_paths,
        replaced_delete_records,
        replaced_delete_files_size,
    })
}

pub fn validate_delete_file_for_row_lineage(file: &DataFile) -> Result<(), String> {
    if file.content_type() == DataContentType::EqualityDeletes {
        return Err(
            "row-lineage DELETE does not support equality-delete files; compact them away first"
                .to_string(),
        );
    }
    if file.file_format() != DataFileFormat::Puffin {
        return Err(
            "row-lineage DELETE found v2 position-delete files; compact them away before writing Puffin deletion vectors"
                .to_string(),
        );
    }
    Ok(())
}

pub fn partition_spec_by_id(
    metadata: &TableMetadata,
    spec_id: i32,
) -> crate::iceberg::Result<PartitionSpecRef> {
    metadata
        .partition_spec_by_id(spec_id)
        .cloned()
        .ok_or_else(|| {
            to_iceberg_unexpected(format!(
                "row-lineage DELETE references unknown partition spec id {spec_id}"
            ))
        })
}

pub fn group_live_files_by_partition_spec(files: Vec<LiveFile>) -> BTreeMap<i32, Vec<LiveFile>> {
    let mut grouped = BTreeMap::new();
    for file in files {
        grouped
            .entry(file.partition_spec_id)
            .or_insert_with(Vec::new)
            .push(file);
    }
    grouped
}

pub fn group_written_dvs_by_partition_spec(
    dvs: &[WrittenDvFile],
    data_files: &HashMap<String, LiveFile>,
) -> Result<BTreeMap<i32, Vec<WrittenDvFile>>, String> {
    let mut grouped = BTreeMap::new();
    for dv in dvs {
        let referenced = data_files.get(&dv.referenced_data_file).ok_or_else(|| {
            format!(
                "row-lineage DELETE references data file `{}` which is not in the current snapshot",
                dv.referenced_data_file
            )
        })?;
        grouped
            .entry(referenced.partition_spec_id)
            .or_insert_with(Vec::new)
            .push(dv.clone());
    }
    Ok(grouped)
}

pub async fn write_existing_delete_manifest(
    file_io: &FileIO,
    out_path: &str,
    files: &[LiveFile],
    partition_spec: PartitionSpecRef,
    schema: SchemaRef,
    new_snapshot_id: i64,
) -> Result<ManifestFile, String> {
    let output_file = file_io
        .new_output(out_path)
        .map_err(|e| format!("FileIO::new_output({out_path}) failed: {e}"))?;
    let builder = ManifestWriterBuilder::new(
        output_file,
        Some(new_snapshot_id),
        None,
        schema,
        (*partition_spec).clone(),
    );
    let mut writer = builder.build_v3_deletes();
    for f in files {
        writer
            .add_existing_file(
                f.data_file.clone(),
                f.snapshot_id,
                f.sequence_number,
                f.file_sequence_number,
            )
            .map_err(|e| format!("ManifestWriter::add_existing_file failed: {e}"))?;
    }
    let manifest_file = writer
        .write_manifest_file()
        .await
        .map_err(|e| format!("ManifestWriter::write_manifest_file failed: {e}"))?;
    debug_assert_eq!(manifest_file.content, ManifestContentType::Deletes);
    Ok(manifest_file)
}

#[allow(clippy::too_many_arguments)]
pub async fn write_added_dv_manifest(
    file_io: &FileIO,
    out_path: &str,
    dvs: &[WrittenDvFile],
    data_files: &HashMap<String, LiveFile>,
    partition_spec: PartitionSpecRef,
    schema: SchemaRef,
    new_seq: i64,
    new_snapshot_id: i64,
) -> Result<ManifestFile, String> {
    let output_file = file_io
        .new_output(out_path)
        .map_err(|e| format!("FileIO::new_output({out_path}) failed: {e}"))?;
    let builder = ManifestWriterBuilder::new(
        output_file,
        Some(new_snapshot_id),
        None,
        schema,
        (*partition_spec).clone(),
    );
    let mut writer = builder.build_v3_deletes();
    for written in dvs {
        let referenced = data_files.get(&written.referenced_data_file).ok_or_else(|| {
            format!(
                "row-lineage DELETE references data file `{}` which is not in the current snapshot",
                written.referenced_data_file
            )
        })?;
        let df = dv_data_file(written, referenced)?;
        writer
            .add_file(df, new_seq)
            .map_err(|e| format!("ManifestWriter::add_file failed: {e}"))?;
    }
    let manifest_file = writer
        .write_manifest_file()
        .await
        .map_err(|e| format!("ManifestWriter::write_manifest_file failed: {e}"))?;
    debug_assert_eq!(manifest_file.content, ManifestContentType::Deletes);
    Ok(manifest_file)
}

pub fn dv_data_file(written: &WrittenDvFile, referenced: &LiveFile) -> Result<DataFile, String> {
    DataFileBuilder::default()
        .content(DataContentType::PositionDeletes)
        .file_path(written.path.clone())
        .file_format(DataFileFormat::Puffin)
        .partition(referenced.data_file.partition().clone())
        .partition_spec_id(referenced.partition_spec_id)
        .record_count(written.cardinality)
        .file_size_in_bytes(written.file_size_in_bytes)
        .referenced_data_file(Some(written.referenced_data_file.clone()))
        .content_offset(Some(written.content_offset))
        .content_size_in_bytes(Some(written.content_size_in_bytes))
        .build()
        .map_err(|e| format!("build DV DataFile failed: {e}"))
}

pub fn dv_total_records(
    parent_total_records: Option<u64>,
    newly_deleted_records: u64,
    added_data_records: u64,
) -> Result<Option<u64>, String> {
    parent_total_records
        .map(|parent| {
            parent
                .checked_sub(newly_deleted_records)
                .ok_or_else(|| {
                    format!(
                        "DV delete total-records underflow: parent={parent}, deleted={newly_deleted_records}"
                    )
                })?
                .checked_add(added_data_records)
                .ok_or_else(|| {
                    format!(
                        "DV delete total-records overflow: parent={parent}, deleted={newly_deleted_records}, added={added_data_records}"
                    )
                })
        })
        .transpose()
}

pub fn dv_summary(
    dvs: &[WrittenDvFile],
    written_data_files: &[WrittenFile],
    total_records: Option<u64>,
    newly_deleted_records: u64,
    removed_delete_files: usize,
    removed_position_deletes: u64,
) -> Result<HashMap<String, String>, String> {
    let mut p = HashMap::new();
    let added_position_deletes = dvs.iter().try_fold(0u64, |sum, file| {
        sum.checked_add(file.cardinality)
            .ok_or_else(|| "DV added position delete count overflow".to_string())
    })?;
    let added_data_records = written_data_files.iter().try_fold(0u64, |sum, file| {
        sum.checked_add(file.record_count)
            .ok_or_else(|| "DV added data record count overflow".to_string())
    })?;
    let total_size = dvs
        .iter()
        .map(|d| d.file_size_in_bytes)
        .chain(written_data_files.iter().map(|f| f.file_size_in_bytes))
        .try_fold(0u64, |sum, size| {
            sum.checked_add(size)
                .ok_or_else(|| "DV added file size overflow".to_string())
        })?;
    p.insert("added-delete-files".to_string(), dvs.len().to_string());
    p.insert(
        "added-position-deletes".to_string(),
        added_position_deletes.to_string(),
    );
    if !written_data_files.is_empty() {
        p.insert(
            "added-data-files".to_string(),
            written_data_files.len().to_string(),
        );
        p.insert("added-records".to_string(), added_data_records.to_string());
    }
    if newly_deleted_records > 0 {
        p.insert(
            "deleted-records".to_string(),
            newly_deleted_records.to_string(),
        );
    }
    if removed_delete_files > 0 {
        p.insert(
            "removed-delete-files".to_string(),
            removed_delete_files.to_string(),
        );
        p.insert(
            "removed-position-delete-files".to_string(),
            removed_delete_files.to_string(),
        );
    }
    if removed_position_deletes > 0 {
        p.insert(
            "removed-position-deletes".to_string(),
            removed_position_deletes.to_string(),
        );
    }
    if let Some(total_records) = total_records {
        p.insert("total-records".to_string(), total_records.to_string());
    }
    p.insert("added-files-size".to_string(), total_size.to_string());
    Ok(p)
}

pub fn to_iceberg_unexpected(s: String) -> crate::iceberg::Error {
    crate::iceberg::Error::new(crate::iceberg::ErrorKind::Unexpected, s)
}

/// Attempt-local row mutation input keyed by full logical entry identity.
pub(crate) struct RowLiveIndex {
    pub live: crate::commit::dependency::LiveSet,
    pub dvs_by_data: BTreeMap<String, crate::commit::model::EntryIdentity>,
}

impl RowLiveIndex {
    pub async fn load(
        view: &crate::commit::staging::StagedView<'_>,
    ) -> crate::iceberg::Result<Self> {
        let parent = view
            .metadata()
            .snapshot_for_ref(view.target_ref())
            .map(|s| s.snapshot_id());
        let mut inputs = crate::commit::dependency::ValidationInputs::new(
            view.metadata(),
            parent,
            view.artifacts(),
        );
        Self::from_live(inputs.live_set().await?.clone())
    }

    fn from_live(live: crate::commit::dependency::LiveSet) -> crate::iceberg::Result<Self> {
        use crate::commit::model::EntryIdentity;
        let mut dvs_by_data = BTreeMap::new();
        for (identity, entry) in &live {
            if entry.file.content_type() == DataContentType::Data {
                continue;
            }
            validate_delete_file_for_row_lineage(&entry.file).map_err(to_iceberg_unexpected)?;
            let EntryIdentity::DeletionVector {
                referenced_data_file,
                ..
            } = identity
            else {
                return Err(to_iceberg_unexpected(
                    "Row mutation requires exact deletion-vector identities".into(),
                ));
            };
            if !live.contains_key(&EntryIdentity::DataFile {
                path: referenced_data_file.clone(),
            }) {
                return Err(to_iceberg_unexpected(format!(
                    "Live deletion vector references absent data file {referenced_data_file}"
                )));
            }
            if dvs_by_data
                .insert(referenced_data_file.clone(), identity.clone())
                .is_some()
            {
                return Err(to_iceberg_unexpected(format!(
                    "More than one live deletion vector references data file {referenced_data_file}"
                )));
            }
        }
        Ok(Self { live, dvs_by_data })
    }
}

/// An individual DV accounts for its blob, rather than its shared Puffin object.
pub(crate) fn logical_file_size(file: &DataFile) -> crate::iceberg::Result<u64> {
    if file.content_type() == DataContentType::PositionDeletes
        && file.file_format() == DataFileFormat::Puffin
    {
        u64::try_from(
            file.content_size_in_bytes()
                .ok_or_else(|| to_iceberg_unexpected("Deletion vector has no blob size".into()))?,
        )
        .map_err(|_| to_iceberg_unexpected("Deletion vector has a negative blob size".into()))
    } else {
        Ok(file.file_size_in_bytes())
    }
}

/// Write immutable intent additions by their actual spec and row-ID assignment.
pub(crate) async fn write_added_entry_groups(
    view: &crate::commit::staging::StagedView<'_>,
    snapshot_id: i64,
    files: impl IntoIterator<Item = crate::commit::model::AddedContent>,
) -> crate::iceberg::Result<Vec<ManifestFile>> {
    use crate::commit::staging::{ManifestEntryWrite, write_manifest};
    let mut grouped: BTreeMap<(i32, bool, bool), Vec<crate::commit::model::AddedContent>> =
        BTreeMap::new();
    for added in files {
        let file = added.file();
        grouped
            .entry((
                added.partition_spec_id(),
                file.content_type() != DataContentType::Data,
                file.first_row_id().is_some(),
            ))
            .or_default()
            .push(added);
    }
    let mut out = Vec::new();
    for ((spec_id, deletes, assigned), files) in grouped {
        let first = files.iter().filter_map(|a| a.file().first_row_id()).min();
        let mut mf = write_manifest(
            view.artifacts(),
            view.metadata().format_version(),
            snapshot_id,
            view.metadata().current_schema().clone(),
            (*partition_spec_by_id(view.metadata(), spec_id)?).clone(),
            if deletes {
                ManifestContentType::Deletes
            } else {
                ManifestContentType::Data
            },
            files.into_iter().map(ManifestEntryWrite::Added),
        )
        .await?;
        if !deletes
            && assigned
            && view.metadata().format_version() == crate::iceberg::spec::FormatVersion::V3
        {
            mf.first_row_id = first.map(u64::try_from).transpose().map_err(|_| {
                to_iceberg_unexpected("Added file has a negative source row ID".into())
            })?;
        }
        out.push(mf);
    }
    Ok(out)
}

#[cfg(test)]
mod t8b_tests {
    use super::*;
    use crate::commit::model::*;
    use crate::commit::staging::*;
    use crate::iceberg::spec::*;
    use crate::iceberg::{NamespaceIdent, TableIdent};
    use novarocks_spi::connector::ConnectorWriteOperationId;
    use std::sync::{Arc, Mutex};

    struct Writer {
        io: FileIO,
        dir: tempfile::TempDir,
        attempt: AttemptToken,
        ledger: Mutex<ArtifactLedger>,
    }
    fn token() -> OperationToken {
        OperationToken::from_write(ConnectorWriteOperationId::from_bytes([83; 16]))
    }
    impl Writer {
        fn new() -> Self {
            let dir = tempfile::tempdir().unwrap();
            let runtime = tokio::runtime::Handle::current();
            let binding = crate::access_binding::IcebergReadBinding::new(
                None,
                novarocks_fs::FsAccessResolver::new(),
                Arc::new(novarocks_fs::TokioFileIoRuntime::new(runtime.clone())),
                Arc::new(novarocks_fs::TokioFileTaskSpawner::new(runtime)),
            );
            let io = crate::fs_io::build_file_io_for_location(
                &format!("file://{}", dir.path().display()),
                binding,
            );
            Self {
                io,
                dir,
                attempt: AttemptToken::new(token(), 0),
                ledger: Mutex::new(ArtifactLedger::new(token())),
            }
        }
    }
    impl ArtifactWriter for Writer {
        fn file_io(&self) -> &FileIO {
            &self.io
        }
        fn allocate(
            &self,
            class: ArtifactClass,
            kind: ArtifactKind,
        ) -> crate::iceberg::Result<ObjectIdentity> {
            let mut ledger = self.ledger.lock().unwrap();
            let object = ObjectIdentity::new(format!(
                "file://{}/{}.{}",
                self.dir.path().display(),
                ledger.records().count(),
                kind.extension()
            ))?;
            ledger.register(ArtifactRecord {
                object: object.clone(),
                class,
                attempt: Some(self.attempt),
                write_state: ArtifactWriteState::Allocated,
            })?;
            Ok(object)
        }
        fn attempt_token(&self) -> AttemptToken {
            self.attempt
        }
        fn check_active(&self) -> crate::iceberg::Result<()> {
            Ok(())
        }
        fn snapshot_artifacts(
            &self,
            refs: &[ObjectIdentity],
        ) -> crate::iceberg::Result<AttemptArtifacts> {
            self.ledger
                .lock()
                .unwrap()
                .snapshot_for_attempt(self.attempt, refs.iter().cloned())
        }
    }
    fn data(path: &str, count: u64, first: Option<i64>) -> DataFile {
        DataFileBuilder::default()
            .content(DataContentType::Data)
            .file_path(path.into())
            .file_format(DataFileFormat::Parquet)
            .partition(Struct::empty())
            .partition_spec_id(0)
            .record_count(count)
            .file_size_in_bytes(100)
            .first_row_id(first)
            .build()
            .unwrap()
    }
    fn dv(path: &str, offset: i64, source: &str, count: u64) -> DataFile {
        DataFileBuilder::default()
            .content(DataContentType::PositionDeletes)
            .file_path(path.into())
            .file_format(DataFileFormat::Puffin)
            .partition(Struct::empty())
            .partition_spec_id(0)
            .record_count(count)
            .file_size_in_bytes(1000)
            .content_offset(Some(offset))
            .content_size_in_bytes(Some(11))
            .referenced_data_file(Some(source.into()))
            .build()
            .unwrap()
    }
    async fn baseline(writer: &Writer, historical: bool, with_dvs: bool) -> TableMetadata {
        let version = if historical {
            FormatVersion::V2
        } else {
            FormatVersion::V3
        };
        let schema = Schema::builder()
            .with_fields(vec![Arc::new(NestedField::required(
                1,
                "id",
                Type::Primitive(PrimitiveType::Long),
            ))])
            .build()
            .unwrap();
        let metadata = TableMetadataBuilder::new(
            schema,
            PartitionSpec::unpartition_spec(),
            SortOrder::unsorted_order(),
            format!("file://{}", writer.dir.path().display()),
            version,
            HashMap::new(),
        )
        .unwrap()
        .build()
        .unwrap()
        .metadata;
        let mut manifests = vec![
            write_manifest(
                writer,
                version,
                11,
                metadata.current_schema().clone(),
                metadata.default_partition_spec().as_ref().clone(),
                ManifestContentType::Data,
                ["a.parquet", "b.parquet"].map(|p| {
                    ManifestEntryWrite::Added(
                        AddedContent::new_logical_data(data(p, 3, None), 0).unwrap(),
                    )
                }),
            )
            .await
            .unwrap(),
        ];
        if with_dvs {
            manifests.push(
                write_manifest(
                    writer,
                    version,
                    11,
                    metadata.current_schema().clone(),
                    metadata.default_partition_spec().as_ref().clone(),
                    ManifestContentType::Deletes,
                    [
                        dv("shared.puffin", 4, "a.parquet", 1),
                        dv("shared.puffin", 40, "b.parquet", 1),
                    ]
                    .map(|f| {
                        ManifestEntryWrite::Added(AddedContent::new_logical_data(f, 0).unwrap())
                    }),
                )
                .await
                .unwrap(),
            );
        }
        let list = write_manifest_list(writer, &metadata, 11, None, manifests)
            .await
            .unwrap();
        let snapshot = Snapshot::builder()
            .with_snapshot_id(11)
            .with_parent_snapshot_id(None)
            .with_sequence_number(1)
            .with_timestamp_ms(metadata.last_updated_ms() + 1)
            .with_manifest_list(list.object.path().to_owned())
            .with_schema_id(metadata.current_schema_id())
            .with_summary(Summary {
                operation: Operation::Append,
                additional_properties: HashMap::from([
                    (
                        "total-records".into(),
                        if with_dvs { "4" } else { "6" }.into(),
                    ),
                    ("total-data-files".into(), "2".into()),
                    (
                        "total-delete-files".into(),
                        if with_dvs { "2" } else { "0" }.into(),
                    ),
                    (
                        "total-position-deletes".into(),
                        if with_dvs { "2" } else { "0" }.into(),
                    ),
                ]),
            });
        let snapshot = match list.row_range {
            Some((first, count)) => snapshot.with_row_range(first, count).build(),
            None => snapshot.build(),
        };
        let metadata = metadata
            .into_builder(None)
            .add_snapshot(snapshot)
            .unwrap()
            .set_ref(
                "main",
                SnapshotReference::new(11, SnapshotRetention::branch(None, None, None)),
            )
            .unwrap()
            .build()
            .unwrap()
            .metadata;
        if historical {
            metadata
                .into_builder(None)
                .upgrade_format_version(FormatVersion::V3)
                .unwrap()
                .build()
                .unwrap()
                .metadata
        } else {
            metadata
        }
    }
    fn intent(
        metadata: &TableMetadata,
        added: Vec<DataFile>,
        removed: Vec<FrozenEntry>,
    ) -> OperationIntent {
        OperationIntent::new(OperationIntentParts {
            target: TableTarget {
                ident: TableIdent::new(NamespaceIdent::new("db".into()), "t".into()),
                uuid: Some(metadata.uuid()),
            },
            target_ref: "main".into(),
            start: Some(StartSnapshot {
                snapshot_id: 11,
                sequence_number: 1,
            }),
            changes: FileChanges {
                added: added
                    .into_iter()
                    .map(|f| AddedContent::new_logical_data(f, 0).unwrap())
                    .collect(),
                removed,
            },
            dependencies: vec![Dependency::RefUnchanged],
            isolation: IsolationLevel::Snapshot,
            shape: RequestShape::SnapshotProducing,
            summary: BTreeMap::new(),
            token: token(),
        })
        .unwrap()
    }
    async fn live(writer: &Writer, metadata: &TableMetadata) -> crate::commit::dependency::LiveSet {
        let mut inputs = crate::commit::dependency::ValidationInputs::new(
            metadata,
            metadata.current_snapshot_id(),
            writer,
        );
        inputs.live_set().await.unwrap().clone()
    }
    async fn raw_entries(writer: &Writer, metadata: &TableMetadata) -> Vec<ManifestEntry> {
        let list = metadata
            .current_snapshot()
            .unwrap()
            .load_manifest_list(writer.file_io(), metadata)
            .await
            .unwrap();
        let mut out = Vec::new();
        for mf in list.entries() {
            let bytes = writer
                .file_io()
                .new_input(&mf.manifest_path)
                .unwrap()
                .read()
                .await
                .unwrap();
            out.extend(
                Manifest::parse_avro(&bytes)
                    .unwrap()
                    .entries()
                    .iter()
                    .map(|entry| entry.as_ref().clone()),
            );
        }
        out
    }
    fn base(metadata: TableMetadata) -> StagingBase {
        StagingBase::Existing {
            metadata,
            metadata_location: "file:///base.metadata.json".into(),
        }
    }

    #[tokio::test]
    async fn t8b_shared_puffin_replaces_one_logical_dv_and_preserves_the_other() {
        let writer = Writer::new();
        let metadata = baseline(&writer, false, true).await;
        let before = live(&writer, &metadata).await;
        let old = before
            .values()
            .find(|e| e.file.referenced_data_file().as_deref() == Some("a.parquet"))
            .unwrap()
            .frozen
            .clone();
        let untouched = before
            .values()
            .find(|e| e.file.referenced_data_file().as_deref() == Some("b.parquet"))
            .unwrap()
            .clone();
        let intent = intent(
            &metadata,
            vec![dv("replacement.puffin", 4, "a.parquet", 2)],
            vec![old],
        );
        let mut engine = StagingEngine::begin(base(metadata), &intent, &writer).unwrap();
        engine
            .stage(&crate::commit::row_delta_dv_from_files::RowDeltaDvFromFilesPreparer)
            .await
            .unwrap();
        let snapshot = engine.metadata().current_snapshot().unwrap();
        assert_eq!(snapshot.summary().operation, Operation::Delete);
        assert_eq!(snapshot.added_rows_count(), Some(0));
        assert_eq!(
            snapshot.summary().additional_properties["added-files-size"],
            "11"
        );
        assert_eq!(
            snapshot.summary().additional_properties["removed-files-size"],
            "11"
        );
        let after = live(&writer, engine.metadata()).await;
        assert_eq!(after[untouched.frozen.identity()].frozen, untouched.frozen);
        assert_eq!(
            after
                .values()
                .filter(|e| e.file.file_path() == "shared.puffin")
                .count(),
            1
        );
        let replacement = after
            .values()
            .find(|e| e.file.file_path() == "replacement.puffin")
            .unwrap();
        assert_eq!(replacement.frozen.facts().data_sequence, Some(2));
        assert_eq!(replacement.frozen.facts().file_sequence, Some(2));
        let raw = raw_entries(&writer, engine.metadata()).await;
        let added = raw
            .iter()
            .find(|e| e.data_file().file_path() == "replacement.puffin")
            .unwrap();
        assert_eq!(added.status(), ManifestStatus::Added);
        assert_eq!(added.sequence_number, None);
        assert_eq!(added.file_sequence_number, None);
        let carried = raw
            .iter()
            .find(|e| e.data_file().referenced_data_file().as_deref() == Some("b.parquet"))
            .unwrap();
        assert_eq!(carried.status(), ManifestStatus::Existing);
        assert_eq!(
            carried.sequence_number,
            untouched.frozen.facts().data_sequence
        );
        assert_eq!(
            carried.file_sequence_number,
            untouched.frozen.facts().file_sequence
        );
        let removed = raw
            .iter()
            .find(|e| {
                e.data_file().referenced_data_file().as_deref() == Some("a.parquet")
                    && e.status() == ManifestStatus::Deleted
            })
            .unwrap();
        assert_eq!(removed.sequence_number, Some(1));
        assert_eq!(removed.file_sequence_number, Some(1));
        let frozen = engine.freeze(&[]).unwrap();
        assert!(frozen.has_updates());
    }
    #[tokio::test]
    async fn t8b_mor_first_assigns_historical_rows_and_new_merge_rows() {
        let writer = Writer::new();
        let metadata = baseline(&writer, true, false).await;
        let intent = intent(
            &metadata,
            vec![
                dv("new.puffin", 4, "a.parquet", 1),
                data("new.parquet", 1, None),
            ],
            vec![],
        );
        let mut engine = StagingEngine::begin(base(metadata), &intent, &writer).unwrap();
        engine
            .stage(&crate::commit::row_delta_dv_from_files::RowDeltaDvFromFilesPreparer)
            .await
            .unwrap();
        let snapshot = engine.metadata().current_snapshot().unwrap();
        assert_eq!(snapshot.summary().operation, Operation::Overwrite);
        assert_eq!(snapshot.added_rows_count(), Some(7));
        assert_eq!(engine.metadata().next_row_id(), 7);
        let after = live(&writer, engine.metadata()).await;
        let mut ranges = after
            .values()
            .filter(|e| e.file.content_type() == DataContentType::Data)
            .map(|e| {
                (
                    e.frozen.facts().first_row_id.unwrap(),
                    e.file.record_count(),
                )
            })
            .collect::<Vec<_>>();
        ranges.sort();
        assert_eq!(ranges, vec![(0, 3), (3, 3), (6, 1)]);
    }
    #[tokio::test]
    async fn t8b_data_only_mor_is_append_and_allocates_new_rows() {
        let writer = Writer::new();
        let metadata = baseline(&writer, false, true).await;
        let intent = intent(&metadata, vec![data("insert.parquet", 2, None)], vec![]);
        let mut engine = StagingEngine::begin(base(metadata), &intent, &writer).unwrap();
        engine
            .stage(&crate::commit::row_delta_dv_from_files::RowDeltaDvFromFilesPreparer)
            .await
            .unwrap();
        assert_eq!(
            engine
                .metadata()
                .current_snapshot()
                .unwrap()
                .summary()
                .operation,
            Operation::Append
        );
        assert_eq!(engine.metadata().next_row_id(), 8);
    }
    async fn mixed_historical_cow_baseline(writer: &Writer) -> TableMetadata {
        let metadata = baseline(writer, true, false).await;
        // A source with actual stored row IDs can coexist with an unassigned
        // historical manifest. The table high-water mark reserves those IDs.
        let assigned = write_manifest(
            writer,
            FormatVersion::V3,
            11,
            metadata.current_schema().clone(),
            metadata.default_partition_spec().as_ref().clone(),
            ManifestContentType::Data,
            [ManifestEntryWrite::Added(
                AddedContent::new_logical_data(data("a.parquet", 3, Some(0)), 0).unwrap(),
            )],
        )
        .await
        .unwrap();
        let historical = write_manifest(
            writer,
            FormatVersion::V2,
            11,
            metadata.current_schema().clone(),
            metadata.default_partition_spec().as_ref().clone(),
            ManifestContentType::Data,
            [ManifestEntryWrite::Added(
                AddedContent::new_logical_data(data("b.parquet", 3, None), 0).unwrap(),
            )],
        )
        .await
        .unwrap();
        let object = writer
            .allocate(ArtifactClass::Attempt, ArtifactKind::ManifestList)
            .unwrap();
        let output = writer.file_io().new_output(object.path()).unwrap();
        let mut list = ManifestListWriter::v2(output, 11, None, 1);
        list.add_manifests([assigned, historical].into_iter())
            .unwrap();
        list.close().await.unwrap();
        let mut value = serde_json::to_value(metadata).unwrap();
        value["next-row-id"] = 3.into();
        value["snapshots"][0]["manifest-list"] = object.path().into();
        serde_json::from_value(value).unwrap()
    }

    #[tokio::test]
    async fn t8b_cow_carries_unassigned_history_and_allocates_only_net_new_outputs() {
        let writer = Writer::new();
        let metadata = mixed_historical_cow_baseline(&writer).await;
        let before = live(&writer, &metadata).await;
        let old = before[&EntryIdentity::DataFile {
            path: "a.parquet".into(),
        }]
            .frozen
            .clone();
        let intent = intent(
            &metadata,
            vec![
                data("replacement.parquet", 3, Some(0)),
                data("insert.parquet", 2, None),
            ],
            vec![old],
        );
        let rewrite = crate::commit::update_cow::CowUpdateRewriteSet {
            base_snapshot_id: 11,
            target_table_uuid: metadata.uuid().to_string(),
            updated_row_ids: vec![0],
            touched_data_files: vec![crate::commit::update_cow::CowUpdateTouchedFile {
                old_file: "a.parquet".into(),
                new_files: vec!["replacement.parquet".into()],
                row_ids: vec![0],
            }],
            appended_files: vec![crate::commit::WrittenFile {
                path: "insert.parquet".into(),
                content: DataContentType::Data,
                format: DataFileFormat::Parquet,
                partition_spec_id: 0,
                partition_values: Struct::empty(),
                record_count: 2,
                file_size_in_bytes: 100,
                split_offsets: vec![],
                column_sizes: HashMap::new(),
                value_counts: HashMap::new(),
                null_value_counts: HashMap::new(),
                nan_value_counts: HashMap::new(),
                lower_bounds: HashMap::new(),
                upper_bounds: HashMap::new(),
                key_metadata: None,
                referenced_data_file: None,
                equality_ids: None,
                first_row_id: None,
                content_offset: None,
                content_size_in_bytes: None,
                cardinality: None,
            }],
        };
        let mut engine = StagingEngine::begin(base(metadata), &intent, &writer).unwrap();
        engine
            .stage(&crate::commit::update_cow::CowUpdatePreparer { rewrite })
            .await
            .unwrap();
        assert_eq!(
            engine
                .metadata()
                .current_snapshot()
                .unwrap()
                .summary()
                .operation,
            Operation::Overwrite
        );
        assert_eq!(
            engine
                .metadata()
                .current_snapshot()
                .unwrap()
                .added_rows_count(),
            Some(5)
        );
        let after = live(&writer, engine.metadata()).await;
        assert_eq!(
            after[&EntryIdentity::DataFile {
                path: "replacement.parquet".into()
            }]
                .frozen
                .facts()
                .first_row_id,
            Some(0)
        );
        assert_eq!(
            after[&EntryIdentity::DataFile {
                path: "b.parquet".into()
            }]
                .frozen
                .facts()
                .first_row_id,
            Some(3)
        );
        assert_eq!(
            after[&EntryIdentity::DataFile {
                path: "insert.parquet".into()
            }]
                .frozen
                .facts()
                .first_row_id,
            Some(6)
        );
        assert_eq!(engine.metadata().next_row_id(), 8);
    }
    #[tokio::test]
    async fn t8b_coalesces_new_blobs_without_deleting_their_owned_objects() {
        let writer = Writer::new();
        let metadata = baseline(&writer, false, false).await;
        let mut additions = Vec::new();
        let mut input_paths = Vec::new();
        for position in [0, 2] {
            let object = writer
                .allocate(ArtifactClass::Attempt, ArtifactKind::DeletionVector)
                .unwrap();
            let mut vector = crate::commit::DeletionVector::new();
            vector.insert(position).unwrap();
            let written = crate::commit::write_single_deletion_vector_puffin(
                writer.file_io(),
                object.path(),
                "a.parquet",
                &vector,
            )
            .await
            .unwrap();
            let source = LiveFile {
                data_file: data("a.parquet", 3, Some(0)),
                partition_spec_id: 0,
                snapshot_id: 11,
                sequence_number: 1,
                file_sequence_number: Some(1),
            };
            additions.push(dv_data_file(&WrittenDvFile::from(written), &source).unwrap());
            input_paths.push(object.path().to_owned());
        }
        let intent = intent(&metadata, additions, vec![]);
        let mut engine = StagingEngine::begin(base(metadata), &intent, &writer).unwrap();
        engine
            .stage(&crate::commit::row_delta_dv_from_files::RowDeltaDvFromFilesPreparer)
            .await
            .unwrap();
        let after = live(&writer, engine.metadata()).await;
        let output = after
            .values()
            .find(|e| e.file.content_type() == DataContentType::PositionDeletes)
            .unwrap();
        assert_eq!(
            after
                .values()
                .filter(|e| e.file.content_type() == DataContentType::PositionDeletes)
                .count(),
            1
        );
        let vector = crate::commit::read_deletion_vector_puffin(
            writer.file_io(),
            output.file.file_path(),
            output.file.content_offset().unwrap(),
            output.file.content_size_in_bytes().unwrap(),
        )
        .await
        .unwrap();
        assert_eq!(vector.cardinality(), 2);
        assert!(vector.contains(0));
        assert!(vector.contains(2));
        for path in input_paths {
            assert!(
                writer
                    .file_io()
                    .new_input(&path)
                    .unwrap()
                    .exists()
                    .await
                    .unwrap()
            );
        }
    }

    #[tokio::test]
    async fn t8b_position_delete_manifest_inherits_both_actual_sequences() {
        let writer = Writer::new();
        let metadata = baseline(&writer, true, false).await;
        let delete = DataFileBuilder::default()
            .content(DataContentType::PositionDeletes)
            .file_path("delete.parquet".into())
            .file_format(DataFileFormat::Parquet)
            .partition(Struct::empty())
            .partition_spec_id(0)
            .record_count(1)
            .file_size_in_bytes(30)
            .referenced_data_file(Some("a.parquet".into()))
            .build()
            .unwrap();
        let intent = intent(&metadata, vec![delete], vec![]);
        let mut engine = StagingEngine::begin(base(metadata), &intent, &writer).unwrap();
        engine
            .stage(&crate::commit::row_delta::RowDeltaPreparer)
            .await
            .unwrap();
        let after = live(&writer, engine.metadata()).await;
        let deleted = after
            .values()
            .find(|e| e.file.file_path() == "delete.parquet")
            .unwrap();
        assert_eq!(deleted.frozen.facts().data_sequence, Some(2));
        assert_eq!(deleted.frozen.facts().file_sequence, Some(2));
        assert_eq!(
            engine
                .metadata()
                .current_snapshot()
                .unwrap()
                .added_rows_count(),
            Some(6)
        );
        let raw = raw_entries(&writer, engine.metadata()).await;
        let entry = raw
            .iter()
            .find(|e| e.data_file().file_path() == "delete.parquet")
            .unwrap();
        assert_eq!(entry.sequence_number, None);
        assert_eq!(entry.file_sequence_number, None);
    }

    #[tokio::test]
    async fn t8b_rejects_two_live_dvs_for_one_data_file_even_in_distinct_objects() {
        let writer = Writer::new();
        let metadata = baseline(&writer, false, true).await;
        let mut entries = live(&writer, &metadata).await;
        let original = entries
            .values()
            .find(|e| e.file.referenced_data_file().as_deref() == Some("a.parquet"))
            .unwrap()
            .clone();
        let file = dv("other.puffin", 4, "a.parquet", 1);
        let identity = EntryIdentity::try_from(&file).unwrap();
        entries.insert(
            identity.clone(),
            crate::commit::dependency::LiveEntry {
                file,
                frozen: FrozenEntry::new(identity, original.frozen.facts().clone()).unwrap(),
            },
        );
        assert!(RowLiveIndex::from_live(entries).is_err());
    }
}
