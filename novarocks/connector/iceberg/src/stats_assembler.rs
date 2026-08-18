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

//! Iceberg Puffin statistics assembler.
//!
//! Given the per-file Theta sketches computed by the sink and the table's
//! prior Puffin statistics (if any), produce the snapshot-level
//! `StatisticsFile` to be registered with the new commit.
//!
//! The assembler implements the commit-type behavior matrix described in the
//! Puffin NDV design spec (section 5.3):
//!
//! | CommitType | Behavior                                       |
//! |------------|-----------------------------------------------|
//! | Append     | union(previous aggregate, new file sketches)   |
//! | Delete     | no new statistics file (returns `None`)        |
//! | Rewrite    | no new statistics file (returns `None`)        |
//! | Overwrite  | aggregate of new file sketches (first-commit shape) |
//!
//! "First commit" with no prior Puffin follows the Overwrite path: the new
//! files are the only live data, so the per-column aggregate over their
//! sketches is the snapshot's NDV. INSERT OVERWRITE / REPLACE shares the
//! same shape because Iceberg overwrite swaps every live data file for the
//! newly-written ones.

#![allow(dead_code)]

use std::collections::HashMap;
use std::fmt;

use crate::iceberg::io::FileIO;
use crate::iceberg::puffin::{APACHE_DATASKETCHES_THETA_V1, Blob, PuffinReader, PuffinWriter};
use crate::iceberg::spec::{BlobMetadata, StatisticsFile, TableMetadata};
use crate::iceberg::table::Table;
use bytes::Bytes;

use crate::theta_sketch::ThetaSketchHandle;

/// Provider-owned, versioned evidence blob.  It lives in a standard Iceberg
/// Puffin file, so other Iceberg readers can retain the StatisticsFile even
/// when they do not interpret NovaRocks' optional statistics payload.
pub const NOVAROCKS_STATISTICS_V1: &str = "novarocks.statistics.v1";
/// Per-table switch for best-effort write-path statistics maintenance.
pub const COLLECT_ON_WRITE_PROPERTY: &str = "novarocks.statistics.collect-on-write";

/// The kind of commit being performed. Determines how the assembler combines
/// new file sketches with the previous snapshot's aggregate Puffin.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CommitType {
    /// Append-only commit (e.g. INSERT, fast_append). Aggregate is
    /// `previous_aggregate ∪ union(new_file_sketches)`.
    Append,
    /// Delete-only commit (position-delete, equality-delete). The assembler
    /// returns `None` and does not create a statistics entry for this snapshot.
    Delete,
    /// INSERT OVERWRITE / REPLACE. Requires a full rescan; deferred for now.
    Overwrite,
    /// Compaction or other rewrite-data-files action that does not change
    /// logical row content. The assembler does not create a statistics entry.
    Rewrite,
}

/// Per-file Theta sketches produced by the sink, one entry per primitive
/// column keyed by Iceberg field id.
pub struct FileSketchSet {
    pub file_path: String,
    pub sketches: HashMap<i32, ThetaSketchHandle>,
}

/// Orchestrates Puffin statistics assembly during a snapshot commit.
pub struct StatsAssembler;

/// Typed stage at which collect-on-write statistics assembly failed.
///
/// The write path keeps this error local to post-commit maintenance: data is
/// already durable when it is observed, but the stage is still needed for a
/// stable diagnostic instead of inferring it from a formatted error message.
#[derive(Debug)]
pub enum StatisticsAssemblyFailure {
    SketchAssembly(String),
    ParentStatisticsRead(String),
    PuffinWrite(String),
}

impl fmt::Display for StatisticsAssemblyFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::SketchAssembly(error)
            | Self::ParentStatisticsRead(error)
            | Self::PuffinWrite(error) => formatter.write_str(error),
        }
    }
}

impl StatsAssembler {
    /// Assemble the Puffin statistics file for the current commit.
    ///
    /// Returns `Some(StatisticsFile)` when a fresh Puffin was written and
    /// should be registered with the metadata. Returns `None` when no
    /// statistics file should be registered for this snapshot.
    ///
    /// `current_snapshot_id` / `current_sequence_number` describe the snapshot
    /// being committed. `prev_snapshot_id`, when `Some`, identifies the
    /// snapshot whose Puffin we read for incremental APPEND merging.
    pub async fn assemble(
        table: &Table,
        commit_type: CommitType,
        new_file_sketches: Vec<FileSketchSet>,
        current_snapshot_id: i64,
        current_sequence_number: i64,
        prev_snapshot_id: Option<i64>,
        file_io: &FileIO,
    ) -> Result<Option<StatisticsFile>, StatisticsAssemblyFailure> {
        match commit_type {
            CommitType::Delete | CommitType::Rewrite => {
                // DELETE and REWRITE do not produce a new statistics file.
                Ok(None)
            }
            CommitType::Append => {
                Self::assemble_append(
                    table,
                    new_file_sketches,
                    current_snapshot_id,
                    current_sequence_number,
                    prev_snapshot_id,
                    file_io,
                )
                .await
            }
            CommitType::Overwrite => {
                // Full rescan path is deferred (requires reading every live
                // data file and re-computing Theta sketches). Returning None
                // means "no new Puffin this commit"; the optimizer will fall
                // back to manifest-derived heuristics until a follow-up agent
                // wires the rescan path.
                Self::assemble_overwrite(
                    table,
                    new_file_sketches,
                    current_snapshot_id,
                    current_sequence_number,
                    file_io,
                )
                .await
            }
        }
    }

    /// APPEND path: union the previous snapshot's aggregate sketch with the
    /// new per-file sketches and write a new Puffin file.
    async fn assemble_append(
        table: &Table,
        new_file_sketches: Vec<FileSketchSet>,
        current_snapshot_id: i64,
        current_sequence_number: i64,
        prev_snapshot_id: Option<i64>,
        file_io: &FileIO,
    ) -> Result<Option<StatisticsFile>, StatisticsAssemblyFailure> {
        // 1. Aggregate the new file sketches per field id.
        let per_column = aggregate_per_column(new_file_sketches);
        if per_column.is_empty() {
            // Nothing to write — caller can either keep the previous entry or
            // skip statistics for this snapshot.
            return Ok(None);
        }

        // 2. Retain a field only when the parent proves that its aggregate
        //    covers the pre-append rows. A missing parent field must not turn
        //    a sketch over only this append into a table-wide NDV.
        let parent_sketches = read_previous_sketches(table.metadata(), prev_snapshot_id, file_io)
            .await
            .map_err(StatisticsAssemblyFailure::ParentStatisticsRead)?;
        let merged = merge_with_parent(
            per_column,
            parent_sketches,
            parent_snapshot_is_proven_empty(table.metadata(), prev_snapshot_id),
        );
        if merged.is_empty() {
            // The parent has rows that are not covered by statistics for any
            // field written by this append. Do not create a partial Puffin.
            return Ok(None);
        }

        // 3. Serialize and write the Puffin file.
        let puffin_path = puffin_path_for_snapshot(table.metadata(), current_snapshot_id);
        write_puffin(
            file_io,
            &puffin_path,
            current_snapshot_id,
            current_sequence_number,
            &merged,
        )
        .await
        .map_err(StatisticsAssemblyFailure::PuffinWrite)
    }

    /// OVERWRITE / first-commit path.
    ///
    /// Iceberg's OVERWRITE semantics replace every live data file with the
    /// freshly-written ones, so the new aggregate is exactly the union of
    /// the `new_file_sketches` already supplied by the sink — no rescan over
    /// existing parquet payload is required (the sketches over the obsoleted
    /// files would not contribute to the new snapshot's NDV anyway). The
    /// first-commit path is identical: there is no prior Puffin and the new
    /// files are the only live ones.
    ///
    /// If the caller supplies an empty `new_file_sketches` (e.g. INSERT
    /// OVERWRITE of zero rows or a table with no primitive columns) we
    /// return `None` and skip statistics registration for this snapshot.
    async fn assemble_overwrite(
        table: &Table,
        new_file_sketches: Vec<FileSketchSet>,
        current_snapshot_id: i64,
        current_sequence_number: i64,
        file_io: &FileIO,
    ) -> Result<Option<StatisticsFile>, StatisticsAssemblyFailure> {
        let per_column = aggregate_per_column(new_file_sketches);
        if per_column.is_empty() {
            return Ok(None);
        }
        let puffin_path = puffin_path_for_snapshot(table.metadata(), current_snapshot_id);
        write_puffin(
            file_io,
            &puffin_path,
            current_snapshot_id,
            current_sequence_number,
            &per_column,
        )
        .await
        .map_err(StatisticsAssemblyFailure::PuffinWrite)
    }
}

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

/// Combine many per-file sketch sets into a per-column aggregate by taking the
/// union over each field id's individual sketches.
fn aggregate_per_column(new_file_sketches: Vec<FileSketchSet>) -> HashMap<i32, ThetaSketchHandle> {
    let mut by_field: HashMap<i32, Vec<ThetaSketchHandle>> = HashMap::new();
    for set in new_file_sketches {
        for (field_id, sketch) in set.sketches {
            by_field.entry(field_id).or_default().push(sketch);
        }
    }

    let mut out = HashMap::new();
    for (field_id, sketches) in by_field {
        let refs: Vec<&ThetaSketchHandle> = sketches.iter().collect();
        out.insert(field_id, ThetaSketchHandle::union(&refs));
    }
    out
}

/// The direct parent's statistics state. An empty `Present` map is distinct
/// from the absence of a statistics file: an existing Puffin was successfully
/// read, even if it did not contain a Theta blob for any field.
enum ParentSketches {
    /// There is no parent snapshot, so the new files are the whole table.
    NoParent,
    /// The parent snapshot has a readable statistics file.
    Present(HashMap<i32, ThetaSketchHandle>),
    /// The parent snapshot exists but has no statistics file.
    NoStatisticsFile,
}

/// Select the fields that still have whole-table coverage after an append.
///
/// A proven-empty parent is equivalent to no parent for this purpose. For a
/// nonempty parent, every published field must occur in both the parent and
/// the new append, so `merge_with_previous` deliberately drops fields unique
/// to either side.
fn merge_with_parent(
    new_per_column: HashMap<i32, ThetaSketchHandle>,
    parent: ParentSketches,
    parent_is_proven_empty: bool,
) -> HashMap<i32, ThetaSketchHandle> {
    match parent {
        ParentSketches::NoParent => new_per_column,
        ParentSketches::Present(_) | ParentSketches::NoStatisticsFile if parent_is_proven_empty => {
            new_per_column
        }
        ParentSketches::Present(previous) => merge_with_previous(new_per_column, previous),
        ParentSketches::NoStatisticsFile => HashMap::new(),
    }
}

/// Merge only fields covered by both the previous snapshot's aggregate and
/// this append. Keeping either side's unique fields would publish an NDV over
/// a strict subset of a nonempty table.
fn merge_with_previous(
    new_per_column: HashMap<i32, ThetaSketchHandle>,
    previous: HashMap<i32, ThetaSketchHandle>,
) -> HashMap<i32, ThetaSketchHandle> {
    new_per_column
        .into_iter()
        .filter_map(|(field_id, new_sketch)| {
            previous.get(&field_id).map(|previous_sketch| {
                (
                    field_id,
                    ThetaSketchHandle::union(&[&new_sketch, previous_sketch]),
                )
            })
        })
        .collect()
}

/// Read the previous snapshot's Puffin and decode each Theta blob.
///
/// A missing parent and a parent without a statistics entry are intentionally
/// separate states. Artifact I/O and decode failures remain errors so callers
/// never silently treat damaged evidence as an empty parent.
async fn read_previous_sketches(
    table_metadata: &TableMetadata,
    prev_snapshot_id: Option<i64>,
    file_io: &FileIO,
) -> Result<ParentSketches, String> {
    let Some(prev_snapshot_id) = prev_snapshot_id else {
        return Ok(ParentSketches::NoParent);
    };
    let Some(prev_stats) = table_metadata.statistics_for_snapshot(prev_snapshot_id) else {
        return Ok(ParentSketches::NoStatisticsFile);
    };

    let input_file = file_io
        .new_input(&prev_stats.statistics_path)
        .map_err(|e| format!("open previous puffin {}: {e}", prev_stats.statistics_path))?;
    let reader = PuffinReader::new(input_file);
    let file_metadata = reader
        .file_metadata()
        .await
        .map_err(|e| format!("read previous puffin metadata: {e}"))?;

    let mut sketches = HashMap::new();
    for blob_metadata in file_metadata.blobs() {
        if blob_metadata.blob_type() != APACHE_DATASKETCHES_THETA_V1 {
            continue;
        }
        let blob = reader
            .blob(blob_metadata)
            .await
            .map_err(|e| format!("read previous puffin blob: {e}"))?;
        let Some(&field_id) = blob.fields().first() else {
            // Skip blobs without a field id — the optimizer cannot key off
            // an empty column descriptor.
            continue;
        };
        match ThetaSketchHandle::deserialize(blob.data()) {
            Ok(sketch) => {
                sketches.insert(field_id, sketch);
            }
            Err(err) => {
                // Surface deserialization failures as errors rather than
                // silently dropping — the caller can choose to swallow and
                // fall back to a from-scratch rebuild.
                return Err(format!(
                    "decode previous theta sketch for field {field_id}: {err}"
                ));
            }
        }
    }
    Ok(ParentSketches::Present(sketches))
}

/// Returns true only when the direct parent explicitly records zero rows.
/// Missing snapshots, missing summaries, malformed summaries, and negative
/// values all remain conservative nonempty cases.
fn parent_snapshot_is_proven_empty(
    table_metadata: &TableMetadata,
    prev_snapshot_id: Option<i64>,
) -> bool {
    let total_records = prev_snapshot_id
        .and_then(|snapshot_id| table_metadata.snapshot_by_id(snapshot_id))
        .and_then(|snapshot| {
            snapshot
                .summary()
                .additional_properties
                .get("total-records")
        })
        .map(String::as_str);
    total_records_is_zero(total_records)
}

fn total_records_is_zero(total_records: Option<&str>) -> bool {
    total_records.and_then(|records| records.parse::<u64>().ok()) == Some(0)
}

/// Write a new Puffin file holding one Theta blob per primitive column.
pub async fn write_puffin(
    file_io: &FileIO,
    puffin_path: &str,
    snapshot_id: i64,
    sequence_number: i64,
    sketches: &HashMap<i32, ThetaSketchHandle>,
) -> Result<Option<StatisticsFile>, String> {
    write_puffin_with_provider_statistics(
        file_io,
        puffin_path,
        snapshot_id,
        sequence_number,
        sketches,
        None,
        // The only caller is the write-path assembler, which unions new data
        // into the parent's sketches rather than rescanning the table.
        StatisticsCoverageMark::IncrementalUnion,
    )
    .await
}

/// Write standard Theta blobs and, when supplied by a successful full scan,
/// one opaque provider evidence blob.  The optional blob is not used for
/// Iceberg catalog mutation semantics; it merely preserves the exact scalar
/// evidence that was collected for this snapshot and operation.
/// Blob property recording how the statistics in a file were produced.
///
/// Iceberg keeps at most one statistics file per snapshot and `set_statistics`
/// replaces it, so two writers targeting the same snapshot need a way to tell
/// whose result is worth more. Without this marker the arbitration cannot run
/// and the last writer would silently win.
pub const STATISTICS_COVERAGE_PROPERTY: &str = "novarocks.statistics.coverage";
pub const STATISTICS_COVERAGE_ALL_VISIBLE_ROWS: &str = "all-visible-rows";
pub const STATISTICS_COVERAGE_INCREMENTAL_UNION: &str = "incremental-union";

/// How completely a registered statistics entry covers its snapshot.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StatisticsCoverageMark {
    /// Produced by scanning every visible row of the snapshot.
    AllVisibleRows,
    /// Produced by merging new data into the parent's sketches.
    IncrementalUnion,
}

impl StatisticsCoverageMark {
    fn as_property(self) -> &'static str {
        match self {
            Self::AllVisibleRows => STATISTICS_COVERAGE_ALL_VISIBLE_ROWS,
            Self::IncrementalUnion => STATISTICS_COVERAGE_INCREMENTAL_UNION,
        }
    }

    /// Reads the mark off a registered entry.
    ///
    /// An entry without the property predates the marker or came from another
    /// engine; treating it as incremental is the conservative reading, since it
    /// only ever makes this side yield.
    pub fn of(file: &StatisticsFile) -> Self {
        file.blob_metadata
            .iter()
            .find_map(|blob| blob.properties.get(STATISTICS_COVERAGE_PROPERTY))
            .map(|value| {
                if value == STATISTICS_COVERAGE_ALL_VISIBLE_ROWS {
                    Self::AllVisibleRows
                } else {
                    Self::IncrementalUnion
                }
            })
            .unwrap_or(Self::IncrementalUnion)
    }
}

pub async fn write_puffin_with_provider_statistics(
    file_io: &FileIO,
    puffin_path: &str,
    snapshot_id: i64,
    sequence_number: i64,
    sketches: &HashMap<i32, ThetaSketchHandle>,
    provider_statistics: Option<&[u8]>,
    coverage: StatisticsCoverageMark,
) -> Result<Option<StatisticsFile>, String> {
    let coverage_property = HashMap::from([(
        STATISTICS_COVERAGE_PROPERTY.to_string(),
        coverage.as_property().to_string(),
    )]);
    let output_file = file_io
        .new_output(puffin_path)
        .map_err(|e| format!("open output puffin {puffin_path}: {e}"))?;
    let mut writer = PuffinWriter::new(&output_file, HashMap::new(), false)
        .await
        .map_err(|e| format!("create puffin writer: {e}"))?;

    // Sort by field id so the resulting blob ordering is deterministic across
    // re-commits with the same input.
    let mut sorted_fields: Vec<i32> = sketches.keys().copied().collect();
    sorted_fields.sort_unstable();

    let mut blob_metadata: Vec<BlobMetadata> =
        Vec::with_capacity(sorted_fields.len() + usize::from(provider_statistics.is_some()));
    for field_id in sorted_fields {
        let sketch = sketches
            .get(&field_id)
            .expect("sketch present for sorted field id");
        let data = sketch.serialize();
        let blob = Blob::builder()
            .r#type(APACHE_DATASKETCHES_THETA_V1.to_string())
            .fields(vec![field_id])
            .snapshot_id(snapshot_id)
            .sequence_number(sequence_number)
            .data(data)
            .properties(coverage_property.clone())
            .build();
        writer
            .add(blob, crate::iceberg::puffin::CompressionCodec::None)
            .await
            .map_err(|e| format!("write puffin blob field={field_id}: {e}"))?;

        blob_metadata.push(BlobMetadata {
            r#type: APACHE_DATASKETCHES_THETA_V1.to_string(),
            snapshot_id,
            sequence_number,
            fields: vec![field_id],
            properties: coverage_property.clone(),
        });
    }
    if let Some(provider_statistics) = provider_statistics {
        let blob = Blob::builder()
            .r#type(NOVAROCKS_STATISTICS_V1.to_string())
            .fields(Vec::new())
            .snapshot_id(snapshot_id)
            .sequence_number(sequence_number)
            .data(provider_statistics.to_vec())
            .properties(coverage_property.clone())
            .build();
        writer
            .add(blob, crate::iceberg::puffin::CompressionCodec::None)
            .await
            .map_err(|e| format!("write Puffin provider statistics blob: {e}"))?;
        blob_metadata.push(BlobMetadata {
            r#type: NOVAROCKS_STATISTICS_V1.to_string(),
            snapshot_id,
            sequence_number,
            fields: Vec::new(),
            properties: coverage_property.clone(),
        });
    }
    writer
        .close()
        .await
        .map_err(|e| format!("close puffin writer: {e}"))?;

    // Determine total file size and the footer size by reading back the
    // payload-length prefix from the trailing footer struct. This is the
    // canonical Iceberg approach — Puffin's writer does not expose footer
    // size directly, but the file layout makes it cheap to recover.
    let input_file = file_io
        .new_input(puffin_path)
        .map_err(|e| format!("open puffin for sizing {puffin_path}: {e}"))?;
    let file_size = input_file
        .metadata()
        .await
        .map_err(|e| format!("stat puffin {puffin_path}: {e}"))?
        .size;
    let file_footer_size = read_footer_size(&input_file, file_size).await?;

    Ok(Some(StatisticsFile {
        snapshot_id,
        statistics_path: puffin_path.to_string(),
        file_size_in_bytes: file_size as i64,
        file_footer_size_in_bytes: file_footer_size as i64,
        key_metadata: None,
        blob_metadata,
    }))
}

/// Read the optional provider evidence payload from a standard Puffin file.
/// Missing payloads are normal for Spark/other-engine statistics files.
pub async fn read_provider_statistics_blob(
    file_io: &FileIO,
    statistics_path: &str,
) -> Result<Option<Bytes>, String> {
    let input = file_io
        .new_input(statistics_path)
        .map_err(|e| format!("open Puffin statistics {statistics_path}: {e}"))?;
    let reader = PuffinReader::new(input);
    let metadata = reader
        .file_metadata()
        .await
        .map_err(|e| format!("read Puffin statistics metadata {statistics_path}: {e}"))?;
    let Some(blob_metadata) = metadata
        .blobs()
        .iter()
        .find(|blob| blob.blob_type() == NOVAROCKS_STATISTICS_V1)
    else {
        return Ok(None);
    };
    let blob = reader
        .blob(blob_metadata)
        .await
        .map_err(|e| format!("read Puffin provider statistics blob {statistics_path}: {e}"))?;
    Ok(Some(Bytes::copy_from_slice(blob.data())))
}

/// Read the footer struct trailer and compute the total footer size.
///
/// Puffin footer layout (from `vendor/iceberg-0.9.0/src/puffin/metadata.rs`):
///   `MAGIC(4) + footer_payload + payload_length(4) + flags(4) + MAGIC(4)`
/// where `payload_length` is the little-endian u32 stored at
/// `file_size - FOOTER_STRUCT_LENGTH = file_size - 12`.
async fn read_footer_size(
    input_file: &crate::iceberg::io::InputFile,
    file_size: u64,
) -> Result<u64, String> {
    const FOOTER_STRUCT_LENGTH: u64 = 12; // payload_length(4) + flags(4) + magic(4)
    const MAGIC_LENGTH: u64 = 4;

    if file_size < FOOTER_STRUCT_LENGTH + MAGIC_LENGTH {
        return Err(format!(
            "puffin file too small to contain footer: {file_size} bytes"
        ));
    }

    let reader = input_file
        .reader()
        .await
        .map_err(|e| format!("open puffin reader: {e}"))?;
    let start = file_size - FOOTER_STRUCT_LENGTH;
    let end = start + 4;
    let bytes = reader
        .read(start..end)
        .await
        .map_err(|e| format!("read footer payload length: {e}"))?;
    let mut buf = [0u8; 4];
    buf.copy_from_slice(&bytes);
    let payload_length = u32::from_le_bytes(buf) as u64;
    Ok(MAGIC_LENGTH + payload_length + FOOTER_STRUCT_LENGTH)
}

/// Puffin path for one incremental registration attempt against a snapshot.
///
/// Unreferenced files are left to the existing orphan cleanup, exactly as the
/// reference engines do with their own `snap-<id>-<seq>-<uuid>.stats` names.
pub fn puffin_path_for_snapshot(table_metadata: &TableMetadata, snapshot_id: i64) -> String {
    let location = table_metadata.location().trim_end_matches('/');
    // Distinct per attempt. A path fixed by snapshot id alone is only safe
    // while one writer can target a snapshot: two concurrent incremental
    // registrations would otherwise write different bytes to the same object
    // while the loser of the commit race is still reading it.
    let attempt = uuid::Uuid::new_v4();
    format!("{location}/metadata/snap-{snapshot_id}-statistics-incremental-{attempt}.puffin")
}

/// Operation-specific Puffin location for an explicit statistics collection.
/// A retry/reconcile keeps its operation ID, while two identical ANALYZE jobs
/// never overwrite each other's staged artifact before the catalog commit is
/// authoritatively resolved.
pub fn puffin_path_for_statistics_operation(
    table_metadata: &TableMetadata,
    snapshot_id: i64,
    operation_id: [u8; 16],
) -> String {
    let location = table_metadata.location().trim_end_matches('/');
    let operation_id = uuid::Uuid::from_bytes(operation_id);
    format!("{location}/metadata/snap-{snapshot_id}-statistics-{operation_id}.puffin")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_sketch_with_values(start: i64, count: i64) -> ThetaSketchHandle {
        let mut sketch = ThetaSketchHandle::new(12);
        for v in start..(start + count) {
            sketch.update(v);
        }
        sketch
    }

    #[test]
    fn aggregate_per_column_unions_same_field() {
        let mut a = HashMap::new();
        a.insert(1, make_sketch_with_values(0, 1000));
        let mut b = HashMap::new();
        b.insert(1, make_sketch_with_values(500, 1000));
        let aggregate = aggregate_per_column(vec![
            FileSketchSet {
                file_path: "a.parquet".to_string(),
                sketches: a,
            },
            FileSketchSet {
                file_path: "b.parquet".to_string(),
                sketches: b,
            },
        ]);
        let est = aggregate.get(&1).expect("field 1 present").estimate();
        // Union of [0,999] and [500,1499] = [0,1499] ≈ 1500 distinct.
        assert!(
            (1300.0..1700.0).contains(&est),
            "aggregate estimate {est} out of expected range"
        );
    }

    #[test]
    fn aggregate_per_column_keeps_distinct_fields() {
        let mut a = HashMap::new();
        a.insert(1, make_sketch_with_values(0, 100));
        a.insert(2, make_sketch_with_values(0, 200));
        let aggregate = aggregate_per_column(vec![FileSketchSet {
            file_path: "a.parquet".to_string(),
            sketches: a,
        }]);
        assert_eq!(aggregate.len(), 2);
        assert!(aggregate.contains_key(&1));
        assert!(aggregate.contains_key(&2));
    }

    #[test]
    fn merge_with_parent_without_parent_keeps_new_fields() {
        let new_map = HashMap::from([
            (1, make_sketch_with_values(0, 100)),
            (2, make_sketch_with_values(100, 100)),
        ]);

        let merged = merge_with_parent(new_map, ParentSketches::NoParent, false);

        assert_eq!(merged.len(), 2);
        assert!(merged.contains_key(&1));
        assert!(merged.contains_key(&2));
    }

    #[test]
    fn merge_with_parent_keeps_new_fields_when_parent_is_proven_empty() {
        let new_map = HashMap::from([
            (1, make_sketch_with_values(0, 100)),
            (2, make_sketch_with_values(100, 100)),
        ]);
        let parent =
            ParentSketches::Present(HashMap::from([(1, make_sketch_with_values(500, 100))]));

        let merged = merge_with_parent(new_map, parent, true);

        assert_eq!(merged.len(), 2);
        assert!(merged.contains_key(&1));
        assert!(merged.contains_key(&2));
    }

    #[test]
    fn merge_with_parent_unions_overlapping_fields() {
        let mut new_map = HashMap::new();
        new_map.insert(1, make_sketch_with_values(0, 5000));
        let mut prev_map = HashMap::new();
        prev_map.insert(1, make_sketch_with_values(3000, 5000));
        let merged = merge_with_parent(new_map, ParentSketches::Present(prev_map), false);
        let est = merged.get(&1).expect("field 1 present").estimate();
        // Union of [0,4999] and [3000,7999] = [0,7999] ≈ 8000 distinct.
        assert!(
            (7000.0..9500.0).contains(&est),
            "merged estimate {est} out of expected range"
        );
    }

    #[test]
    fn merge_with_parent_omits_fields_missing_from_nonempty_parent() {
        let new_map = HashMap::from([
            (1, make_sketch_with_values(0, 100)),
            (2, make_sketch_with_values(100, 100)),
        ]);
        let parent = ParentSketches::Present(HashMap::from([
            (1, make_sketch_with_values(500, 100)),
            (3, make_sketch_with_values(1_000, 100)),
        ]));

        let merged = merge_with_parent(new_map, parent, false);

        assert_eq!(merged.len(), 1);
        assert!(merged.contains_key(&1));
        assert!(!merged.contains_key(&2));
        assert!(!merged.contains_key(&3));
    }

    #[test]
    fn merge_with_parent_without_parent_statistics_skips_nonempty_parent() {
        let new_map = HashMap::from([(1, make_sketch_with_values(0, 100))]);

        let merged = merge_with_parent(new_map, ParentSketches::NoStatisticsFile, false);

        assert!(merged.is_empty());
    }

    #[test]
    fn merge_with_parent_without_parent_statistics_keeps_new_fields_when_empty() {
        let new_map = HashMap::from([(1, make_sketch_with_values(0, 100))]);

        let merged = merge_with_parent(new_map, ParentSketches::NoStatisticsFile, true);

        assert_eq!(merged.len(), 1);
        assert!(merged.contains_key(&1));
    }

    #[test]
    fn total_records_is_zero_requires_an_explicit_parseable_zero() {
        assert!(total_records_is_zero(Some("0")));
        assert!(!total_records_is_zero(None));
        assert!(!total_records_is_zero(Some("1")));
        assert!(!total_records_is_zero(Some("-1")));
        assert!(!total_records_is_zero(Some("not-a-record-count")));
    }

    /// Round-trip: write a Puffin file via `write_puffin` for one field with
    /// ~500 distinct values, then read its Theta blob back through the same
    /// `PuffinReader` path the loader uses and assert the recovered NDV is
    /// within +/-10% of 500.
    #[tokio::test]
    async fn write_puffin_then_read_ndv_roundtrips() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("rt.puffin");
        let path_str = format!("file://{}", path.display());
        let file_io = crate::fs_io::build_file_io_for_location(&path_str, None);

        let mut sketch = ThetaSketchHandle::new(12);
        for i in 0..500_i64 {
            sketch.update(i);
        }
        let mut sketches = HashMap::new();
        sketches.insert(3_i32, sketch);

        let sf = write_puffin(&file_io, &path_str, 100, 1, &sketches)
            .await
            .expect("write_puffin ok")
            .expect("statistics file present");

        // Returned StatisticsFile structure.
        assert_eq!(sf.snapshot_id, 100);
        assert_eq!(sf.blob_metadata.len(), 1);
        assert_eq!(sf.blob_metadata[0].fields, vec![3]);
        assert_eq!(sf.blob_metadata[0].r#type, APACHE_DATASKETCHES_THETA_V1);
        assert!(sf.file_size_in_bytes > 0);

        // Real NDV read-back through the same PuffinReader + ThetaSketchHandle
        // decode path the loader uses (see `read_previous_sketches`).
        let recovered = read_previous_sketches_from_path(&file_io, &sf.statistics_path).await;
        let ndv = recovered.get(&3).expect("field 3 present").estimate();
        assert!(
            (450.0..=550.0).contains(&ndv),
            "field 3 NDV {ndv} should be ~500 (within +/-10%)"
        );
    }

    #[tokio::test]
    async fn provider_statistics_blob_round_trips_without_replacing_theta_blobs() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("provider-stats.puffin");
        let path_str = format!("file://{}", path.display());
        let file_io = crate::fs_io::build_file_io_for_location(&path_str, None);
        let mut sketches = HashMap::new();
        sketches.insert(3_i32, make_sketch_with_values(0, 10));
        let payload = br#"{\"version\":1,\"data_version\":[1],\"metrics\":[]}"#;

        let file = write_puffin_with_provider_statistics(
            &file_io,
            &path_str,
            100,
            1,
            &sketches,
            Some(payload),
            StatisticsCoverageMark::AllVisibleRows,
        )
        .await
        .expect("write provider statistics")
        .expect("statistics file");

        assert!(
            file.blob_metadata
                .iter()
                .any(|blob| blob.r#type == APACHE_DATASKETCHES_THETA_V1)
        );
        assert!(
            file.blob_metadata
                .iter()
                .any(|blob| blob.r#type == NOVAROCKS_STATISTICS_V1)
        );
        assert_eq!(
            read_provider_statistics_blob(&file_io, &path_str)
                .await
                .expect("read provider statistics"),
            Some(Bytes::copy_from_slice(payload))
        );
    }

    /// Test helper: read every Theta blob from a Puffin file at `path` and
    /// decode it back into a `field_id -> ThetaSketchHandle` map. Mirrors the
    /// reader path used by `read_previous_sketches` / `StatsLoader`.
    async fn read_previous_sketches_from_path(
        file_io: &FileIO,
        path: &str,
    ) -> HashMap<i32, ThetaSketchHandle> {
        let input_file = file_io.new_input(path).expect("open puffin");
        let reader = PuffinReader::new(input_file);
        let file_metadata = reader.file_metadata().await.expect("read puffin metadata");
        let mut out = HashMap::new();
        for blob_metadata in file_metadata.blobs() {
            if blob_metadata.blob_type() != APACHE_DATASKETCHES_THETA_V1 {
                continue;
            }
            let Some(&field_id) = blob_metadata.fields().first() else {
                continue;
            };
            let blob = reader.blob(blob_metadata).await.expect("read blob");
            let sketch = ThetaSketchHandle::deserialize(blob.data()).expect("deserialize sketch");
            out.insert(field_id, sketch);
        }
        out
    }
}
