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

//! Lazy Iceberg split enumeration over one pinned snapshot.
//!
//! The source turns an already-planned file list into byte-range splits in
//! bounded batches. It never opens a data file, never reads a Parquet footer,
//! and never consults the catalog: every fact it needs was frozen when the
//! snapshot was pinned, and re-deriving any of it here would reintroduce the
//! second resolution path this stack exists to remove.

use std::collections::{BTreeMap, BTreeSet, VecDeque};

use novarocks_proto_codec::connector_read::MAX_SPLITS_PER_ASSIGNMENT;
use novarocks_spi::connector::ConnectorError;
use novarocks_spi::connector::read_stack::{
    ConnectorSplitBatch, ConnectorSplitSource, DynamicFilterSnapshot, SplitSourceProfile,
    TupleDomain,
};

use crate::iceberg::spec::{Literal, Schema, StructType, Type};
use crate::read_model::{
    IcebergReadDeleteFile, IcebergReadDeleteFormat, IcebergReadDeleteKind, IcebergReadFile,
    delete_applies_to_data_file,
};

use super::change_window::{
    IcebergAddedRows, IcebergChangeSplit, IcebergChangeWindowHandle, IcebergChangeWindowPlan,
    IcebergChangeWindowPlanOutcome, IcebergDeletedDataFileRows, IcebergEndpointVisibility,
    IcebergEqualityDeletedRows, IcebergPositionDeletedRows,
};
use super::column_handle::{IcebergColumnHandle, corrupt, invalid, unsupported};
use super::split::{
    DEFAULT_MINIMUM_ASSIGNED_SPLIT_WEIGHT, IcebergDeleteFile, IcebergDeleteFileContent,
    IcebergDeleteFileParams, IcebergFileFormat, IcebergSplit, IcebergSplitParams,
    IcebergSplitWeightParameters, ParquetFileDecryptionData, iceberg_split_weight,
};
use super::table_handle::{IcebergTableHandle, identity_partition_source_field_ids};

/// The Iceberg default target split size.
pub const DEFAULT_TARGET_SPLIT_SIZE_BYTES: u64 = 128 * 1024 * 1024;

/// The Iceberg table property that overrides the default target split size.
pub const READ_SPLIT_TARGET_SIZE_PROPERTY: &str = "read.split.target-size";

/// Manifest facts about one delete file that the read view does not carry.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IcebergDeleteFileFacts {
    pub record_count: i64,
    pub row_position_lower_bound: Option<i64>,
    pub row_position_upper_bound: Option<i64>,
    pub decryption_data: Option<ParquetFileDecryptionData>,
}

/// One data file of the pinned snapshot, with every fact split production
/// needs and nothing that would require reopening the file.
#[derive(Clone, Debug)]
pub struct IcebergPlannedDataFile {
    /// The pinned-snapshot read view. Its `deletes` are the frozen applicable
    /// closure produced by the manifest walk, not a candidate list.
    pub read_file: IcebergReadFile,
    /// The manifest's declared format. Never inferred from the path.
    pub file_format: IcebergFileFormat,
    /// The manifest's `split_offsets`, empty when the writer recorded none.
    pub split_offsets: Vec<i64>,
    /// The manifest's `key_metadata`, empty when the file is not encrypted.
    pub key_metadata: Vec<u8>,
    /// Statistics frozen from the manifest, already expressed as a domain.
    pub file_statistics_domain: TupleDomain<IcebergColumnHandle>,
    pub decryption_data: Option<ParquetFileDecryptionData>,
    /// Manifest facts for each applicable delete file, keyed by its path.
    pub delete_facts: BTreeMap<String, IcebergDeleteFileFacts>,
}

/// Session-level knobs that change how files are cut, never what they contain.
#[derive(Clone, Copy, Debug)]
pub struct IcebergSplitSourceOptions {
    /// Session `max_split_size`, when the session sets one.
    pub session_max_split_size: Option<u64>,
    /// Explicit session override permitting adjacent split-offset ranges of
    /// the *same* file to be merged up to the target size. Off by default:
    /// merging hides the writer's own row-group boundaries.
    pub merge_adjacent_split_offsets: bool,
    pub minimum_assigned_split_weight: f64,
}

impl Default for IcebergSplitSourceOptions {
    fn default() -> Self {
        Self {
            session_max_split_size: None,
            merge_adjacent_split_offsets: false,
            minimum_assigned_split_weight: DEFAULT_MINIMUM_ASSIGNED_SPLIT_WEIGHT,
        }
    }
}

/// A lazily advancing enumerator over one pinned snapshot's data files.
#[derive(Debug)]
pub struct IcebergSplitSource {
    files: Vec<IcebergPlannedDataFile>,
    next_file: usize,
    pending: VecDeque<IcebergSplit>,
    closed: bool,
    /// Set when planning already proved the scan reads nothing.
    exhausted: bool,
    effective_predicate: TupleDomain<IcebergColumnHandle>,
    /// Projected base-column field IDs, or `None` when the projection contains
    /// a nested field and the partition-only fast path can never apply.
    projected_base_field_ids: Option<BTreeSet<i32>>,
    identity_partition_source_field_ids: BTreeMap<i32, BTreeSet<i32>>,
    partition_types: BTreeMap<i32, Type>,
    schema_field_order: BTreeMap<i32, usize>,
    target_split_size: i64,
    merge_adjacent_split_offsets: bool,
    weight_parameters: IcebergSplitWeightParameters,
    profile: SplitSourceProfile,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum FilePruningDecision {
    Keep,
    Static,
    Dynamic,
}

impl IcebergSplitSource {
    pub fn try_new(
        table_handle: &IcebergTableHandle,
        files: Vec<IcebergPlannedDataFile>,
        options: IcebergSplitSourceOptions,
    ) -> Result<Self, ConnectorError> {
        let schema = table_handle.parse_table_schema()?;
        let mut partition_types = BTreeMap::new();
        let mut identity_partition_ids = BTreeMap::new();
        for spec_id in table_handle.partition_spec_jsons().keys() {
            let spec = table_handle.parse_partition_spec(*spec_id)?;
            let partition_type = spec.partition_type(&schema).map_err(|error| {
                invalid(format!(
                    "iceberg partition spec {spec_id} does not bind to the frozen table schema: {error}"
                ))
            })?;
            partition_types.insert(*spec_id, Type::Struct(partition_type));
            identity_partition_ids.insert(*spec_id, identity_partition_source_field_ids(&spec));
        }

        let target_split_size = resolve_target_split_size(table_handle, &options)?;
        let weight_parameters = IcebergSplitWeightParameters::try_new(
            target_split_size,
            options.minimum_assigned_split_weight,
        )?;

        let projected_base_field_ids = if table_handle
            .projected_columns()
            .iter()
            .all(IcebergColumnHandle::is_base_column)
        {
            Some(
                table_handle
                    .projected_columns()
                    .iter()
                    .map(IcebergColumnHandle::base_field_id)
                    .collect(),
            )
        } else {
            None
        };

        let effective_predicate = table_handle.effective_predicate()?;
        // Three planning outcomes read nothing at all. Recording them here
        // keeps `next_batch` from walking a file list that cannot contribute.
        let exhausted = table_handle.snapshot_id().is_none()
            || table_handle.limit() == Some(0)
            || effective_predicate.is_none();

        Ok(Self {
            files,
            next_file: 0,
            pending: VecDeque::new(),
            closed: false,
            exhausted,
            effective_predicate,
            projected_base_field_ids,
            identity_partition_source_field_ids: identity_partition_ids,
            partition_types,
            schema_field_order: schema_field_order(&schema),
            target_split_size: i64::try_from(target_split_size).unwrap_or(i64::MAX),
            merge_adjacent_split_offsets: options.merge_adjacent_split_offsets,
            weight_parameters,
            profile: SplitSourceProfile::default(),
        })
    }

    /// Decide whether frozen file statistics exclude a file, and distinguish
    /// the dynamic-filter contribution from ordinary static pruning. This is
    /// called only before `splits_for_file`, so no pending/returned split is
    /// ever retracted.
    fn file_pruning_decision(
        &self,
        file: &IcebergPlannedDataFile,
        dynamic_filter: &DynamicFilterSnapshot<IcebergColumnHandle>,
    ) -> Result<FilePruningDecision, ConnectorError> {
        let static_file_domain = self
            .effective_predicate
            .intersect(&file.file_statistics_domain)?;
        if static_file_domain.is_none() {
            return Ok(FilePruningDecision::Static);
        }
        if static_file_domain
            .intersect(dynamic_filter.current_predicate())?
            .is_none()
        {
            return Ok(FilePruningDecision::Dynamic);
        }
        Ok(FilePruningDecision::Keep)
    }

    fn splits_for_file(
        &self,
        file: &IcebergPlannedDataFile,
    ) -> Result<Vec<IcebergSplit>, ConnectorError> {
        // Admission first: a split we could never open must not reach a
        // scheduler, and it must fail by its declared format, not by a suffix.
        admit_readable_data_file(file)?;

        let read_file = &file.read_file;
        let partition_spec_id = read_file.partition_spec_id.ok_or_else(|| {
            corrupt(format!(
                "iceberg data file {} is missing its partition spec id",
                read_file.path
            ))
        })?;
        let file_record_count = read_file.record_count.ok_or_else(|| {
            corrupt(format!(
                "iceberg data file {} is missing its record count",
                read_file.path
            ))
        })?;
        if read_file.size < 0 {
            return Err(corrupt(format!(
                "iceberg data file {} has a negative size",
                read_file.path
            )));
        }
        let partition_data_json = self.partition_data_json(partition_spec_id, read_file)?;
        let deletes = self.deletes_for_file(file)?;

        let ranges = if deletes.is_empty() && self.is_partition_only_read(partition_spec_id) {
            // Every projected column is an identity partition constant, so the
            // reader only needs the file's record count -- one split covering
            // the whole file is both sufficient and cheapest.
            vec![(0, read_file.size)]
        } else {
            self.byte_ranges(file, read_file.size)?
        };

        let mut splits = Vec::with_capacity(ranges.len());
        for (start, length) in ranges {
            splits.push(IcebergSplit::try_new(IcebergSplitParams {
                path: read_file.path.clone(),
                start,
                length,
                file_size: read_file.size,
                file_record_count,
                file_format: file.file_format,
                partition_spec_id,
                partition_data_json: partition_data_json.clone(),
                deletes: deletes.clone(),
                file_statistics_domain: file.file_statistics_domain.clone(),
                data_sequence_number: read_file.data_sequence_number,
                file_first_row_id: read_file.first_row_id,
                decryption_data: None,
                split_weight: iceberg_split_weight(length, &deletes, self.weight_parameters)?,
                // Ranges of one file share a footer and a delete closure, so
                // co-locating them lets a worker reuse both.
                affinity_key: Some(read_file.path.clone()),
            })?);
        }
        Ok(splits)
    }

    /// Whether the projection is satisfied entirely by identity partition
    /// constants of this file's spec. A zero-column projection qualifies.
    fn is_partition_only_read(&self, partition_spec_id: i32) -> bool {
        let Some(projected) = self.projected_base_field_ids.as_ref() else {
            return false;
        };
        let Some(identity) = self
            .identity_partition_source_field_ids
            .get(&partition_spec_id)
        else {
            return false;
        };
        projected.is_subset(identity)
    }

    fn byte_ranges(
        &self,
        file: &IcebergPlannedDataFile,
        file_size: i64,
    ) -> Result<Vec<(i64, i64)>, ConnectorError> {
        Ok(data_file_byte_ranges(
            &file.split_offsets,
            file_size,
            self.target_split_size,
            self.merge_adjacent_split_offsets,
        ))
    }

    fn partition_data_json(
        &self,
        partition_spec_id: i32,
        read_file: &IcebergReadFile,
    ) -> Result<String, ConnectorError> {
        let partition_type = self
            .partition_types
            .get(&partition_spec_id)
            .ok_or_else(|| {
                corrupt(format!(
                    "iceberg data file {} references partition spec id {partition_spec_id} that the table handle does not carry",
                    read_file.path
                ))
            })?;
        encode_partition_data_json(partition_type, read_file)
    }

    fn deletes_for_file(
        &self,
        file: &IcebergPlannedDataFile,
    ) -> Result<Vec<IcebergDeleteFile>, ConnectorError> {
        let read_file = &file.read_file;
        let mut deletes = Vec::with_capacity(read_file.deletes.len());
        for read_delete in &read_file.deletes {
            // Applicability was decided by the pinned-snapshot manifest walk.
            // Re-running the same rule here is a guard, not a re-derivation: it
            // catches a closure attached from a different data file.
            if !delete_applies_to_data_file(read_delete, read_file) {
                return Err(corrupt(format!(
                    "iceberg delete file {} does not apply to data file {}",
                    read_delete.path, read_file.path
                )));
            }
            deletes.push(self.delete_descriptor(read_delete, file)?);
        }
        Ok(deletes)
    }

    fn delete_descriptor(
        &self,
        read_delete: &IcebergReadDeleteFile,
        file: &IcebergPlannedDataFile,
    ) -> Result<IcebergDeleteFile, ConnectorError> {
        delete_descriptor_of(read_delete, file, &self.schema_field_order)
    }
}

/// Reject a planned data file this read stack could never open.
///
/// It fails by the manifest's declared format and declared encryption
/// material, never by a path suffix, and it runs before any split of the file
/// is produced so an unreadable file never reaches a scheduler.
fn admit_readable_data_file(file: &IcebergPlannedDataFile) -> Result<(), ConnectorError> {
    match file.file_format {
        IcebergFileFormat::Parquet => {}
        IcebergFileFormat::Orc => {
            return Err(unsupported(
                "iceberg orc data files are not supported by the connector read stack",
            ));
        }
        IcebergFileFormat::Avro => {
            return Err(unsupported(
                "iceberg avro data files are not supported by the connector read stack",
            ));
        }
        IcebergFileFormat::Puffin => {
            return Err(corrupt(
                "an iceberg data file is never in the puffin delete-artifact format".to_string(),
            ));
        }
    }
    if !file.key_metadata.is_empty() {
        return Err(unsupported(
            "iceberg encrypted manifest key metadata is not supported by the connector read stack",
        ));
    }
    if file.decryption_data.is_some() {
        return Err(unsupported(
            "iceberg parquet modular encryption is not supported by the connector read stack",
        ));
    }
    Ok(())
}

/// Turn one applicable delete of a planned file into its wire descriptor.
///
/// The schema order is passed in rather than re-derived so a change-window
/// endpoint and an ordinary scan canonicalize equality field IDs against the
/// very same frozen schema.
fn delete_descriptor_of(
    read_delete: &IcebergReadDeleteFile,
    file: &IcebergPlannedDataFile,
    schema_field_order: &BTreeMap<i32, usize>,
) -> Result<IcebergDeleteFile, ConnectorError> {
    let facts = file.delete_facts.get(&read_delete.path).ok_or_else(|| {
        corrupt(format!(
            "iceberg delete file {} is missing its manifest facts",
            read_delete.path
        ))
    })?;
    let format = match read_delete.file_format {
        IcebergReadDeleteFormat::Parquet => IcebergFileFormat::Parquet,
        IcebergReadDeleteFormat::Puffin => IcebergFileFormat::Puffin,
    };
    let (content, equality_field_ids) = match &read_delete.kind {
        IcebergReadDeleteKind::Position => (IcebergDeleteFileContent::PositionDeletes, Vec::new()),
        IcebergReadDeleteKind::Equality { equality_field_ids } => (
            IcebergDeleteFileContent::EqualityDeletes,
            equality_field_ids_in_schema_order(equality_field_ids, schema_field_order)?,
        ),
    };
    let file_size_in_bytes = read_delete.length.ok_or_else(|| {
        corrupt(format!(
            "iceberg delete file {} is missing its file size",
            read_delete.path
        ))
    })?;
    let data_sequence_number = read_delete.sequence_number.ok_or_else(|| {
        corrupt(format!(
            "iceberg delete file {} is missing its data sequence number",
            read_delete.path
        ))
    })?;

    IcebergDeleteFile::try_new(IcebergDeleteFileParams {
        content,
        path: read_delete.path.clone(),
        format,
        record_count: facts.record_count,
        file_size_in_bytes,
        equality_field_ids,
        row_position_lower_bound: facts.row_position_lower_bound,
        row_position_upper_bound: facts.row_position_upper_bound,
        data_sequence_number,
        content_offset: read_delete.content_offset,
        content_size_in_bytes: read_delete.content_size_in_bytes,
        referenced_data_file: read_delete.referenced_data_file.clone(),
        decryption_data: facts.decryption_data.clone(),
    })
}

fn equality_field_ids_in_schema_order(
    field_ids: &[i32],
    schema_field_order: &BTreeMap<i32, usize>,
) -> Result<Vec<i32>, ConnectorError> {
    let mut seen = BTreeSet::new();
    let mut ordered = Vec::with_capacity(field_ids.len());
    for field_id in field_ids {
        if !seen.insert(*field_id) {
            return Err(corrupt(format!(
                "iceberg equality-delete file declares duplicate equality field id {field_id}"
            )));
        }
        let order = schema_field_order.get(field_id).ok_or_else(|| {
            corrupt(format!(
                "iceberg equality-delete field id {field_id} is not present in the frozen table schema"
            ))
        })?;
        ordered.push((*order, *field_id));
    }
    ordered.sort_unstable();
    Ok(ordered.into_iter().map(|(_, field_id)| field_id).collect())
}

/// Cut one data file into byte ranges that tile it exactly once.
///
/// The manifest's own `split_offsets` win when they are usable, because they
/// name the writer's row-group boundaries; otherwise the file is cut into
/// contiguous target-sized ranges. Either way the ranges cover `[0, file_size)`
/// with no gap and no overlap, which is what lets a change window prove that
/// its splits neither drop nor double a row.
fn data_file_byte_ranges(
    split_offsets: &[i64],
    file_size: i64,
    target_split_size: i64,
    merge_adjacent_split_offsets: bool,
) -> Vec<(i64, i64)> {
    if legal_split_offsets(split_offsets, file_size) {
        let mut ranges = Vec::with_capacity(split_offsets.len());
        for (index, start) in split_offsets.iter().enumerate() {
            let end = split_offsets.get(index + 1).copied().unwrap_or(file_size);
            ranges.push((*start, end - *start));
        }
        if merge_adjacent_split_offsets {
            return merge_adjacent_ranges(ranges, target_split_size);
        }
        return ranges;
    }

    // No usable offsets: cut contiguous target-sized ranges. Files are
    // never combined, so a range always names exactly one file.
    let mut ranges = Vec::new();
    let mut start = 0_i64;
    while start < file_size {
        let length = target_split_size.min(file_size - start);
        ranges.push((start, length));
        start += length;
    }
    if ranges.is_empty() {
        // A zero-byte file still yields exactly one split so its record
        // count and partition constants stay reachable.
        ranges.push((0, 0));
    }
    ranges
}

/// Encode a data file's frozen partition values against its spec's type.
fn encode_partition_data_json(
    partition_type: &Type,
    read_file: &IcebergReadFile,
) -> Result<String, ConnectorError> {
    let values = read_file.partition_values.as_ref().ok_or_else(|| {
        corrupt(format!(
            "iceberg data file {} is missing its partition values",
            read_file.path
        ))
    })?;
    let expected = match partition_type {
        Type::Struct(struct_type) => struct_type.fields().len(),
        Type::Primitive(_) | Type::List(_) | Type::Map(_) => {
            return Err(corrupt(
                "iceberg partition type must be a struct".to_string(),
            ));
        }
    };
    // The Iceberg JSON encoder zips values against fields, so a mismatched
    // arity would silently drop partition values instead of failing.
    if values.iter().len() != expected {
        return Err(corrupt(format!(
            "iceberg data file {} carries {} partition values for a spec with {expected} fields",
            read_file.path,
            values.iter().len()
        )));
    }
    let json = Literal::Struct(values.clone())
        .try_into_json(partition_type)
        .map_err(|error| {
            corrupt(format!(
                "iceberg partition values of {} cannot be encoded: {error}",
                read_file.path
            ))
        })?;
    Ok(json.to_string())
}

impl ConnectorSplitSource for IcebergSplitSource {
    type Split = IcebergSplit;
    type Column = IcebergColumnHandle;

    fn profile_snapshot(&self) -> SplitSourceProfile {
        self.profile
    }

    fn next_batch(
        &mut self,
        max_size: usize,
        dynamic_filter: &DynamicFilterSnapshot<Self::Column>,
    ) -> Result<ConnectorSplitBatch<Self::Split>, ConnectorError> {
        if max_size == 0 {
            return Err(invalid("connector split batch size must be positive"));
        }
        if self.closed || self.exhausted {
            return Ok(ConnectorSplitBatch::finished());
        }
        // An unsatisfiable snapshot finishes immediately. Otherwise each
        // unexpanded file observes this batch's immutable snapshot below.
        if dynamic_filter.current_predicate().is_none() {
            self.exhausted = true;
            return Ok(ConnectorSplitBatch::finished());
        }

        let max_size = max_size.min(MAX_SPLITS_PER_ASSIGNMENT);
        let mut produced = Vec::new();
        while produced.len() < max_size {
            if let Some(split) = self.pending.pop_front() {
                produced.push(split);
                continue;
            }
            let index = self.next_file;
            if index >= self.files.len() {
                break;
            }
            self.next_file += 1;
            self.profile.files_considered = self.profile.files_considered.saturating_add(1);
            let splits = {
                let file = &self.files[index];
                match self.file_pruning_decision(file, dynamic_filter)? {
                    FilePruningDecision::Static => continue,
                    FilePruningDecision::Dynamic => {
                        self.profile.files_pruned = self.profile.files_pruned.saturating_add(1);
                        continue;
                    }
                    FilePruningDecision::Keep => {}
                }
                self.profile.files_expanded = self.profile.files_expanded.saturating_add(1);
                self.splits_for_file(file)?
            };
            self.pending.extend(splits);
        }

        let no_more_splits = self.pending.is_empty() && self.next_file >= self.files.len();
        self.profile.splits_emitted = self
            .profile
            .splits_emitted
            .saturating_add(u64::try_from(produced.len()).unwrap_or(u64::MAX));
        Ok(ConnectorSplitBatch::new(produced, no_more_splits))
    }

    fn is_finished(&self) -> bool {
        self.closed
            || self.exhausted
            || (self.pending.is_empty() && self.next_file >= self.files.len())
    }

    /// Idempotent. A batch already returned by value cannot be retracted, so a
    /// caller that raced this close still owns the splits it was handed.
    fn close(&mut self) -> Result<(), ConnectorError> {
        self.closed = true;
        self.pending.clear();
        self.files.clear();
        self.next_file = 0;
        Ok(())
    }
}

fn resolve_target_split_size(
    table_handle: &IcebergTableHandle,
    options: &IcebergSplitSourceOptions,
) -> Result<u64, ConnectorError> {
    if let Some(session_max_split_size) = options.session_max_split_size {
        return Ok(session_max_split_size);
    }
    match table_handle
        .storage_properties()
        .get(READ_SPLIT_TARGET_SIZE_PROPERTY)
    {
        Some(value) => value.parse::<u64>().map_err(|_| {
            invalid(format!(
                "iceberg table property {READ_SPLIT_TARGET_SIZE_PROPERTY} is not a byte count"
            ))
        }),
        None => Ok(DEFAULT_TARGET_SPLIT_SIZE_BYTES),
    }
}

/// Whether a manifest's `split_offsets` can be used as range boundaries.
///
/// Offsets must be strictly increasing and land inside the file; anything else
/// would produce an empty or out-of-file range, so the target-size cut is used
/// instead. The first offset is deliberately not required to be zero: a
/// Parquet file's first row group starts after its magic header.
fn legal_split_offsets(split_offsets: &[i64], file_size: i64) -> bool {
    let Some(first) = split_offsets.first() else {
        return false;
    };
    if *first < 0 {
        return false;
    }
    if split_offsets.windows(2).any(|pair| pair[0] >= pair[1]) {
        return false;
    }
    split_offsets.last().is_some_and(|last| *last < file_size)
}

/// Merge adjacent ranges of one file while the result stays within the target.
fn merge_adjacent_ranges(ranges: Vec<(i64, i64)>, target_split_size: i64) -> Vec<(i64, i64)> {
    let mut merged: Vec<(i64, i64)> = Vec::with_capacity(ranges.len());
    for (start, length) in ranges {
        match merged.last_mut() {
            Some((previous_start, previous_length))
                if *previous_start + *previous_length == start
                    && *previous_length + length <= target_split_size =>
            {
                *previous_length += length;
            }
            _ => merged.push((start, length)),
        }
    }
    merged
}

// ---------------------------------------------------------------------------
// Change-window enumeration
// ---------------------------------------------------------------------------

/// The two endpoints of one change window, each already reduced to the data
/// files it makes visible together with their frozen delete closures.
pub struct IcebergChangeWindowEndpoints<'a> {
    /// Files visible at the exclusive lower endpoint.
    pub from_visible: &'a [IcebergPlannedDataFile],
    /// Files visible at the inclusive upper endpoint.
    pub to_visible: &'a [IcebergPlannedDataFile],
}

/// Turn two endpoint file sets into one window's proven-disjoint split set.
///
/// This is a **set difference of the two endpoints**, never a replay of the
/// snapshots between them. The distinction is the whole contract: a data file
/// written and dropped again inside the window is visible at neither endpoint,
/// so it appears in neither index below and is reached by no branch. A replay
/// would find its add and its delete in the manifests and emit both, which
/// looks entirely plausible until a materialized view stops converging.
///
/// The same rule at row granularity is what the delete closures carry: a file
/// added inside the window keeps the *upper* endpoint's closure, so rows that
/// were written and then deleted inside the window are already invisible in
/// what the forward side reads.
pub fn plan_change_window_splits(
    handle: &IcebergChangeWindowHandle,
    endpoints: IcebergChangeWindowEndpoints<'_>,
    partition_types: &BTreeMap<i32, Type>,
    options: IcebergSplitSourceOptions,
) -> Result<IcebergChangeWindowPlan, ConnectorError> {
    let schema = handle.parse_table_schema()?;
    // A change window carries no table properties of its own, so the session is
    // the only thing that can change the cut. It changes how a file is divided,
    // never which rows the difference owns.
    let target_split_size = options
        .session_max_split_size
        .unwrap_or(DEFAULT_TARGET_SPLIT_SIZE_BYTES);
    let context = ChangeWindowContext {
        partition_types,
        schema_field_order: schema_field_order(&schema),
        target_split_size: i64::try_from(target_split_size).unwrap_or(i64::MAX),
        merge_adjacent_split_offsets: options.merge_adjacent_split_offsets,
        weight_parameters: IcebergSplitWeightParameters::try_new(
            target_split_size,
            options.minimum_assigned_split_weight,
        )?,
    };

    let from_visible = index_visible_files(endpoints.from_visible)?;
    let to_visible = index_visible_files(endpoints.to_visible)?;

    let mut splits = Vec::new();
    for (path, to_file) in &to_visible {
        match from_visible.get(path) {
            None => context.push_added_rows(to_file, &mut splits)?,
            Some(from_file) => context.push_surviving_file(from_file, to_file, &mut splits)?,
        }
    }
    for (path, from_file) in &from_visible {
        if to_visible.contains_key(path) {
            continue;
        }
        context.push_deleted_data_file_rows(from_file, &mut splits)?;
    }

    match IcebergChangeWindowPlan::try_plan(
        handle.clone(),
        IcebergEndpointVisibility::Proven,
        splits,
    )? {
        IcebergChangeWindowPlanOutcome::Incremental(plan) => Ok(plan),
        // Admission already proved both endpoints, so a rebuild verdict here
        // would mean the proof and the plan disagree about the same window.
        // The arm exists because the outcome is a closed enum, and quietly
        // returning no splits would be indistinguishable from an empty window.
        IcebergChangeWindowPlanOutcome::FullRebuild(reason) => Err(corrupt(format!(
            "iceberg change window was admitted as proven but planning reported a full rebuild: {reason:?}"
        ))),
    }
}

/// One endpoint's visible files, keyed by data file path.
fn index_visible_files(
    files: &[IcebergPlannedDataFile],
) -> Result<BTreeMap<&str, &IcebergPlannedDataFile>, ConnectorError> {
    let mut indexed = BTreeMap::new();
    for file in files {
        if indexed.insert(file.read_file.path.as_str(), file).is_some() {
            // An endpoint lists one data file exactly once. Two entries would
            // make "visible here" ambiguous and could emit its rows twice.
            return Err(corrupt(format!(
                "iceberg snapshot lists data file {} more than once",
                file.read_file.path
            )));
        }
    }
    Ok(indexed)
}

/// The frozen facts every variant of one data file shares.
struct ChangeFileFacts {
    partition_spec_id: i32,
    partition_data_json: String,
    file_record_count: i64,
    /// Byte ranges that tile the file exactly once.
    ranges: Vec<(i64, i64)>,
}

/// Everything the endpoint difference needs beyond the two file sets.
struct ChangeWindowContext<'a> {
    partition_types: &'a BTreeMap<i32, Type>,
    schema_field_order: BTreeMap<i32, usize>,
    target_split_size: i64,
    merge_adjacent_split_offsets: bool,
    weight_parameters: IcebergSplitWeightParameters,
}

impl ChangeWindowContext<'_> {
    /// Rows visible at `to` for a file that did not exist at `from`.
    ///
    /// The upper endpoint's own delete closure travels with the data split, so
    /// what a reader emits is exactly the rows the file still has at `to` --
    /// never the rows it was written with.
    fn push_added_rows(
        &self,
        file: &IcebergPlannedDataFile,
        splits: &mut Vec<IcebergChangeSplit>,
    ) -> Result<(), ConnectorError> {
        let facts = self.file_facts(file)?;
        let deletes = self.closure_of(file)?.into_values().collect::<Vec<_>>();
        for split in self.data_splits(file, &facts, deletes)? {
            splits.push(IcebergChangeSplit::AddedRows(IcebergAddedRows::try_new(
                split,
                // Enumeration narrows nothing: the delete closure above already
                // states exactly which rows survive at `to`.
                Vec::new(),
            )?));
        }
        Ok(())
    }

    /// Rows visible at `from` for a file that is gone at `to`.
    fn push_deleted_data_file_rows(
        &self,
        file: &IcebergPlannedDataFile,
        splits: &mut Vec<IcebergChangeSplit>,
    ) -> Result<(), ConnectorError> {
        let facts = self.file_facts(file)?;
        let previously = self.closure_of(file)?.into_values().collect::<Vec<_>>();
        // The data split carries no exclusion closure of its own: rows that
        // were already invisible at `from` are named as typed variant facts,
        // so one split never carries two contradictory delete meanings.
        for split in self.data_splits(file, &facts, Vec::new())? {
            splits.push(IcebergChangeSplit::DeletedDataFileRows(
                IcebergDeletedDataFileRows::try_new(split, previously.clone())?,
            ));
        }
        Ok(())
    }

    /// A file visible at both endpoints: only its delete closure can differ.
    fn push_surviving_file(
        &self,
        from_file: &IcebergPlannedDataFile,
        to_file: &IcebergPlannedDataFile,
        splits: &mut Vec<IcebergChangeSplit>,
    ) -> Result<(), ConnectorError> {
        // Iceberg data files are immutable, so one path at two endpoints must
        // describe the same bytes and the same rows.
        if from_file.read_file.size != to_file.read_file.size
            || from_file.read_file.record_count != to_file.read_file.record_count
        {
            return Err(corrupt(format!(
                "iceberg data file {} describes different content at the two change-window endpoints",
                to_file.read_file.path
            )));
        }

        let from_closure = self.closure_of(from_file)?;
        let to_closure = self.closure_of(to_file)?;
        let newly = to_closure
            .iter()
            .filter(|(path, _)| !from_closure.contains_key(*path))
            .map(|(_, delete)| delete.clone())
            .collect::<Vec<_>>();
        if newly.is_empty() {
            if from_closure
                .keys()
                .any(|path| !to_closure.contains_key(path))
            {
                // With no replacement delete closure, a row previously hidden
                // at `from` may have become visible at `to`. The typed change
                // relation has no forward variant for rows from an otherwise
                // surviving file, so keep this case fail-closed.
                return Err(unsupported(format!(
                    "iceberg delete files stopped applying to data file {} inside the change window without a replacement closure",
                    to_file.read_file.path
                )));
            }
            // Present at both endpoints with the same closure: the file's
            // visible rows did not change, so the difference owns none of them.
            return Ok(());
        }

        let previously = from_closure.into_values().collect::<Vec<_>>();
        let (newly_position, newly_equality): (Vec<_>, Vec<_>) = newly
            .into_iter()
            .partition(|delete| delete.content() == IcebergDeleteFileContent::PositionDeletes);

        let facts = self.file_facts(to_file)?;
        if !newly_position.is_empty() {
            for split in self.data_splits(to_file, &facts, Vec::new())? {
                splits.push(IcebergChangeSplit::PositionDeletedRows(
                    IcebergPositionDeletedRows::try_new(
                        split,
                        newly_position.clone(),
                        previously.clone(),
                    )?,
                ));
            }
        }
        if !newly_equality.is_empty() {
            // The equality variant owns only what the position variant did
            // not, so every newly applied position delete is handed to it as
            // already applied and its rows are subtracted rather than emitted
            // a second time.
            let mut equality_previously = previously.clone();
            equality_previously.extend(newly_position.iter().cloned());
            for split in self.data_splits(to_file, &facts, Vec::new())? {
                splits.push(IcebergChangeSplit::EqualityDeletedRows(
                    IcebergEqualityDeletedRows::try_new(
                        split,
                        newly_equality.clone(),
                        equality_previously.clone(),
                    )?,
                ));
            }
        }
        Ok(())
    }

    fn file_facts(&self, file: &IcebergPlannedDataFile) -> Result<ChangeFileFacts, ConnectorError> {
        admit_readable_data_file(file)?;
        let read_file = &file.read_file;
        let partition_spec_id = read_file.partition_spec_id.ok_or_else(|| {
            corrupt(format!(
                "iceberg data file {} is missing its partition spec id",
                read_file.path
            ))
        })?;
        let file_record_count = read_file.record_count.ok_or_else(|| {
            corrupt(format!(
                "iceberg data file {} is missing its record count",
                read_file.path
            ))
        })?;
        if read_file.size < 0 {
            return Err(corrupt(format!(
                "iceberg data file {} has a negative size",
                read_file.path
            )));
        }
        let partition_type = self.partition_types.get(&partition_spec_id).ok_or_else(|| {
            corrupt(format!(
                "iceberg data file {} references partition spec id {partition_spec_id} that the change window does not carry",
                read_file.path
            ))
        })?;
        Ok(ChangeFileFacts {
            partition_spec_id,
            partition_data_json: encode_partition_data_json(partition_type, read_file)?,
            file_record_count,
            ranges: data_file_byte_ranges(
                &file.split_offsets,
                read_file.size,
                self.target_split_size,
                self.merge_adjacent_split_offsets,
            ),
        })
    }

    /// One endpoint's applicable delete closure for one file, by delete path.
    fn closure_of(
        &self,
        file: &IcebergPlannedDataFile,
    ) -> Result<BTreeMap<String, IcebergDeleteFile>, ConnectorError> {
        let read_file = &file.read_file;
        let mut closure = BTreeMap::new();
        for read_delete in &read_file.deletes {
            // Applicability was decided by the endpoint's manifest walk. This
            // is a guard, not a re-derivation: it catches a closure attached
            // from a different data file.
            if !delete_applies_to_data_file(read_delete, read_file) {
                return Err(corrupt(format!(
                    "iceberg delete file {} does not apply to data file {}",
                    read_delete.path, read_file.path
                )));
            }
            let descriptor = delete_descriptor_of(read_delete, file, &self.schema_field_order)?;
            if closure
                .insert(read_delete.path.clone(), descriptor)
                .is_some()
            {
                return Err(corrupt(format!(
                    "iceberg delete file {} is attached to data file {} more than once",
                    read_delete.path, read_file.path
                )));
            }
        }
        Ok(closure)
    }

    fn data_splits(
        &self,
        file: &IcebergPlannedDataFile,
        facts: &ChangeFileFacts,
        deletes: Vec<IcebergDeleteFile>,
    ) -> Result<Vec<IcebergSplit>, ConnectorError> {
        let read_file = &file.read_file;
        let mut splits = Vec::with_capacity(facts.ranges.len());
        for (start, length) in &facts.ranges {
            splits.push(IcebergSplit::try_new(IcebergSplitParams {
                path: read_file.path.clone(),
                start: *start,
                length: *length,
                file_size: read_file.size,
                file_record_count: facts.file_record_count,
                file_format: file.file_format,
                partition_spec_id: facts.partition_spec_id,
                partition_data_json: facts.partition_data_json.clone(),
                deletes: deletes.clone(),
                file_statistics_domain: file.file_statistics_domain.clone(),
                data_sequence_number: read_file.data_sequence_number,
                file_first_row_id: read_file.first_row_id,
                decryption_data: None,
                split_weight: iceberg_split_weight(*length, &deletes, self.weight_parameters)?,
                affinity_key: Some(read_file.path.clone()),
            })?);
        }
        Ok(splits)
    }
}

/// A bounded enumerator over one change window's proven-disjoint split set.
#[derive(Debug)]
pub struct IcebergChangeWindowSplitSource {
    pending: VecDeque<IcebergChangeSplit>,
    closed: bool,
    /// Set when a runtime predicate proved the scan reads nothing.
    exhausted: bool,
}

impl IcebergChangeWindowSplitSource {
    /// Only an admitted plan can build one.
    ///
    /// Disjointness is a property of the whole split set rather than of any one
    /// split, so a loose `Vec` must not be a constructor argument: it would let
    /// an unproven set be enumerated as if it had been checked.
    pub fn new(plan: IcebergChangeWindowPlan) -> Self {
        Self {
            pending: VecDeque::from(plan.into_splits()),
            closed: false,
            exhausted: false,
        }
    }
}

impl ConnectorSplitSource for IcebergChangeWindowSplitSource {
    type Split = IcebergChangeSplit;
    type Column = IcebergColumnHandle;

    fn next_batch(
        &mut self,
        max_size: usize,
        dynamic_filter: &DynamicFilterSnapshot<Self::Column>,
    ) -> Result<ConnectorSplitBatch<Self::Split>, ConnectorError> {
        if max_size == 0 {
            return Err(invalid("connector split batch size must be positive"));
        }
        if self.closed || self.exhausted {
            return Ok(ConnectorSplitBatch::finished());
        }
        // A change window is a proven set difference, so a runtime predicate
        // must not narrow it split by split: dropping one would lose rows the
        // difference owns. An unsatisfiable predicate is the single exception,
        // because it proves the scan reads nothing at all.
        if dynamic_filter.current_predicate().is_none() {
            self.exhausted = true;
            self.pending.clear();
            return Ok(ConnectorSplitBatch::finished());
        }

        let max_size = max_size.min(MAX_SPLITS_PER_ASSIGNMENT);
        let mut produced = Vec::with_capacity(max_size.min(self.pending.len()));
        while produced.len() < max_size {
            let Some(split) = self.pending.pop_front() else {
                break;
            };
            produced.push(split);
        }
        Ok(ConnectorSplitBatch::new(produced, self.pending.is_empty()))
    }

    fn is_finished(&self) -> bool {
        self.closed || self.exhausted || self.pending.is_empty()
    }

    /// Idempotent, exactly like the data enumerator: a batch already returned
    /// by value cannot be retracted.
    fn close(&mut self) -> Result<(), ConnectorError> {
        self.closed = true;
        self.pending.clear();
        Ok(())
    }
}

/// A deterministic pre-order index of every field ID in a schema.
///
/// Equality-delete field IDs are canonicalized into this order, which is the
/// table schema's own order rather than whatever order a writer happened to
/// record.
fn schema_field_order(schema: &Schema) -> BTreeMap<i32, usize> {
    let mut order = BTreeMap::new();
    let mut next = 0_usize;
    index_struct_fields(schema.as_struct(), &mut order, &mut next);
    order
}

fn index_struct_fields(
    struct_type: &StructType,
    order: &mut BTreeMap<i32, usize>,
    next: &mut usize,
) {
    for field in struct_type.fields() {
        order.insert(field.id, *next);
        *next += 1;
        index_type_fields(field.field_type.as_ref(), order, next);
    }
}

fn index_type_fields(value: &Type, order: &mut BTreeMap<i32, usize>, next: &mut usize) {
    match value {
        Type::Primitive(_) => {}
        Type::Struct(struct_type) => index_struct_fields(struct_type, order, next),
        Type::List(list_type) => {
            order.insert(list_type.element_field.id, *next);
            *next += 1;
            index_type_fields(list_type.element_field.field_type.as_ref(), order, next);
        }
        Type::Map(map_type) => {
            order.insert(map_type.key_field.id, *next);
            *next += 1;
            index_type_fields(map_type.key_field.field_type.as_ref(), order, next);
            order.insert(map_type.value_field.id, *next);
            *next += 1;
            index_type_fields(map_type.value_field.field_type.as_ref(), order, next);
        }
    }
}

#[cfg(test)]
mod tests {
    use novarocks_spi::connector::ConnectorErrorKind;
    use novarocks_spi::connector::read_stack::{
        ConnectorValue, ConnectorValueType, Domain, SchemaTableName, ValueSet,
    };

    use crate::iceberg::spec::{Literal as IcebergLiteral, PartitionSpec, Struct};
    use crate::read_model::iceberg_partition_key;
    use crate::typed_read::change_window::{IcebergChangeSide, IcebergChangeWindowHandleParams};
    use crate::typed_read::table_handle::tests::{
        identity_partition_spec, partitioned_schema, table_handle_params,
    };

    use super::*;

    fn read_file(path: &str, size: i64, record_count: i64) -> IcebergReadFile {
        IcebergReadFile {
            path: path.to_string(),
            size,
            record_count: Some(record_count),
            column_stats: None,
            partition_spec_id: Some(7),
            partition_key: None,
            partition_values: Some(Struct::from_iter([Some(IcebergLiteral::string("emea"))])),
            manifest_path: Some("m0.avro".to_string()),
            first_row_id: Some(500),
            data_sequence_number: Some(9),
            deletes: Vec::new(),
        }
    }

    fn planned(read_file: IcebergReadFile) -> IcebergPlannedDataFile {
        IcebergPlannedDataFile {
            read_file,
            file_format: IcebergFileFormat::Parquet,
            split_offsets: Vec::new(),
            key_metadata: Vec::new(),
            file_statistics_domain: TupleDomain::all(),
            decryption_data: None,
            delete_facts: BTreeMap::new(),
        }
    }

    fn handle_with(
        target_size: Option<u64>,
        projected: BTreeSet<IcebergColumnHandle>,
    ) -> IcebergTableHandle {
        let schema = partitioned_schema();
        let spec = identity_partition_spec(&schema);
        let mut params = table_handle_params(&schema, Some(&spec));
        params.projected_columns = projected;
        if let Some(target_size) = target_size {
            params.storage_properties.insert(
                READ_SPLIT_TARGET_SIZE_PROPERTY.to_string(),
                target_size.to_string(),
            );
        }
        IcebergTableHandle::try_new(params).expect("table handle")
    }

    fn split_source_for(
        handle: &IcebergTableHandle,
        files: Vec<IcebergPlannedDataFile>,
    ) -> IcebergSplitSource {
        IcebergSplitSource::try_new(handle, files, IcebergSplitSourceOptions::default())
            .expect("split source")
    }

    fn all_splits(source: &mut IcebergSplitSource, batch_size: usize) -> Vec<IcebergSplit> {
        let filter = DynamicFilterSnapshot::all_complete();
        let mut splits = Vec::new();
        loop {
            let batch = source.next_batch(batch_size, &filter).expect("batch");
            let finished = batch.no_more_splits();
            splits.extend(batch.into_splits());
            if finished {
                break;
            }
        }
        splits
    }

    fn region_column() -> IcebergColumnHandle {
        IcebergColumnHandle::base_column_of(&partitioned_schema(), 2).expect("region")
    }

    fn amount_column() -> IcebergColumnHandle {
        IcebergColumnHandle::base_column_of(&partitioned_schema(), 3).expect("amount")
    }

    #[test]
    fn target_size_cutting_never_crosses_a_file_boundary() {
        let handle = handle_with(Some(100), BTreeSet::from([amount_column()]));
        let mut source = split_source_for(
            &handle,
            vec![
                planned(read_file("a.parquet", 250, 10)),
                planned(read_file("b.parquet", 40, 4)),
            ],
        );
        let splits = all_splits(&mut source, 16);

        let shapes = splits
            .iter()
            .map(|split| (split.path().to_string(), split.start(), split.length()))
            .collect::<Vec<_>>();
        assert_eq!(
            shapes,
            vec![
                ("a.parquet".to_string(), 0, 100),
                ("a.parquet".to_string(), 100, 100),
                ("a.parquet".to_string(), 200, 50),
                ("b.parquet".to_string(), 0, 40),
            ]
        );
        // The 50-byte tail and the 40-byte file are never packed together.
        assert!(splits.iter().all(|split| split.length() <= 100));
    }

    #[test]
    fn a_legal_split_offset_list_defines_the_ranges() {
        let handle = handle_with(Some(1_000), BTreeSet::from([amount_column()]));
        let mut file = planned(read_file("a.parquet", 300, 30));
        file.split_offsets = vec![4, 120, 220];
        let mut source = split_source_for(&handle, vec![file]);
        let splits = all_splits(&mut source, 16);

        assert_eq!(
            splits
                .iter()
                .map(|split| (split.start(), split.length()))
                .collect::<Vec<_>>(),
            vec![(4, 116), (120, 100), (220, 80)]
        );
    }

    #[test]
    fn an_illegal_split_offset_list_falls_back_to_target_size_cutting() {
        let handle = handle_with(Some(100), BTreeSet::from([amount_column()]));
        for offsets in [
            vec![120_i64, 40], // not increasing
            vec![10, 10],      // not strictly increasing
            vec![10, 400],     // outside the file
            vec![-1, 10],      // negative
        ] {
            let mut file = planned(read_file("a.parquet", 250, 10));
            file.split_offsets = offsets;
            let mut source = split_source_for(&handle, vec![file]);
            let splits = all_splits(&mut source, 16);
            assert_eq!(
                splits
                    .iter()
                    .map(|split| (split.start(), split.length()))
                    .collect::<Vec<_>>(),
                vec![(0, 100), (100, 100), (200, 50)]
            );
        }
    }

    #[test]
    fn adjacent_offset_ranges_merge_only_under_an_explicit_session_override() {
        let handle = handle_with(Some(200), BTreeSet::from([amount_column()]));
        let mut file = planned(read_file("a.parquet", 300, 30));
        file.split_offsets = vec![0, 100, 200];

        let mut default_source = split_source_for(&handle, vec![file.clone()]);
        assert_eq!(all_splits(&mut default_source, 16).len(), 3);

        let mut merging = IcebergSplitSource::try_new(
            &handle,
            vec![file],
            IcebergSplitSourceOptions {
                merge_adjacent_split_offsets: true,
                ..IcebergSplitSourceOptions::default()
            },
        )
        .expect("split source");
        assert_eq!(
            all_splits(&mut merging, 16)
                .iter()
                .map(|split| (split.start(), split.length()))
                .collect::<Vec<_>>(),
            vec![(0, 200), (200, 100)]
        );
    }

    #[test]
    fn a_partition_only_projection_reads_the_whole_file_in_one_split() {
        let handle = handle_with(Some(100), BTreeSet::from([region_column()]));
        let mut source = split_source_for(&handle, vec![planned(read_file("a.parquet", 250, 10))]);
        let splits = all_splits(&mut source, 16);
        assert_eq!(splits.len(), 1);
        assert!(splits[0].is_whole_file());

        // A zero-column projection qualifies for the same fast path.
        let empty = handle_with(Some(100), BTreeSet::new());
        let mut source = split_source_for(&empty, vec![planned(read_file("a.parquet", 250, 10))]);
        assert_eq!(all_splits(&mut source, 16).len(), 1);

        // A projected non-partition column disqualifies it.
        let mixed = handle_with(
            Some(100),
            BTreeSet::from([region_column(), amount_column()]),
        );
        let mut source = split_source_for(&mixed, vec![planned(read_file("a.parquet", 250, 10))]);
        assert_eq!(all_splits(&mut source, 16).len(), 3);
    }

    #[test]
    fn any_delete_forces_ordinary_byte_range_splits() {
        let handle = handle_with(Some(100), BTreeSet::from([region_column()]));
        let mut read = read_file("a.parquet", 250, 10);
        read.deletes.push(IcebergReadDeleteFile {
            path: "p0.parquet".to_string(),
            file_format: IcebergReadDeleteFormat::Parquet,
            kind: IcebergReadDeleteKind::Position,
            length: Some(64),
            content_offset: None,
            content_size_in_bytes: None,
            sequence_number: Some(11),
            partition_spec_id: Some(7),
            partition_key: None,
            referenced_data_file: Some("a.parquet".to_string()),
        });
        let mut file = planned(read);
        file.delete_facts.insert(
            "p0.parquet".to_string(),
            IcebergDeleteFileFacts {
                record_count: 2,
                row_position_lower_bound: Some(0),
                row_position_upper_bound: Some(5),
                decryption_data: None,
            },
        );

        let mut source = split_source_for(&handle, vec![file]);
        let splits = all_splits(&mut source, 16);
        assert_eq!(splits.len(), 3);
        // The complete closure is copied onto every range of the file.
        for split in &splits {
            assert_eq!(split.deletes().len(), 1);
            assert_eq!(split.deletes()[0].path(), "p0.parquet");
            assert_eq!(split.deletes()[0].record_count(), 2);
            assert_eq!(split.deletes()[0].data_sequence_number(), 11);
            assert_eq!(
                split.deletes()[0].content(),
                IcebergDeleteFileContent::PositionDeletes
            );
        }
    }

    #[test]
    fn a_delete_attached_to_the_wrong_data_file_is_rejected() {
        let handle = handle_with(Some(100), BTreeSet::from([amount_column()]));
        let mut read = read_file("a.parquet", 100, 10);
        read.deletes.push(IcebergReadDeleteFile {
            path: "p0.parquet".to_string(),
            file_format: IcebergReadDeleteFormat::Parquet,
            kind: IcebergReadDeleteKind::Position,
            length: Some(64),
            content_offset: None,
            content_size_in_bytes: None,
            sequence_number: Some(11),
            partition_spec_id: Some(7),
            partition_key: None,
            // The frozen rule rejects a position delete that names another file.
            referenced_data_file: Some("other.parquet".to_string()),
        });
        let mut file = planned(read);
        file.delete_facts.insert(
            "p0.parquet".to_string(),
            IcebergDeleteFileFacts {
                record_count: 2,
                row_position_lower_bound: None,
                row_position_upper_bound: None,
                decryption_data: None,
            },
        );

        let mut source = split_source_for(&handle, vec![file]);
        let error = source
            .next_batch(16, &DynamicFilterSnapshot::all_complete())
            .expect_err("inapplicable delete must be rejected");
        assert_eq!(error.kind(), ConnectorErrorKind::CorruptData);
    }

    #[test]
    fn equality_delete_field_ids_are_canonicalized_into_schema_order() {
        let handle = handle_with(Some(1_000), BTreeSet::from([amount_column()]));
        let mut read = read_file("a.parquet", 100, 10);
        read.deletes.push(IcebergReadDeleteFile {
            path: "e0.parquet".to_string(),
            file_format: IcebergReadDeleteFormat::Parquet,
            kind: IcebergReadDeleteKind::Equality {
                equality_field_ids: vec![3, 1],
            },
            length: Some(128),
            content_offset: None,
            content_size_in_bytes: None,
            sequence_number: Some(12),
            partition_spec_id: Some(7),
            partition_key: None,
            referenced_data_file: None,
        });
        let mut file = planned(read);
        file.delete_facts.insert(
            "e0.parquet".to_string(),
            IcebergDeleteFileFacts {
                record_count: 4,
                row_position_lower_bound: None,
                row_position_upper_bound: None,
                decryption_data: None,
            },
        );

        let mut source = split_source_for(&handle, vec![file]);
        let splits = all_splits(&mut source, 16);
        assert_eq!(splits[0].deletes()[0].equality_field_ids(), &[1, 3]);
    }

    #[test]
    fn zero_snapshot_pruned_predicate_and_limit_zero_all_finish_with_no_splits() {
        let schema = partitioned_schema();
        let spec = identity_partition_spec(&schema);

        let mut no_snapshot = table_handle_params(&schema, Some(&spec));
        no_snapshot.snapshot_id = None;
        let handle = IcebergTableHandle::try_new(no_snapshot).expect("handle");
        let mut source = split_source_for(&handle, Vec::new());
        assert!(source.is_finished());
        assert!(all_splits(&mut source, 8).is_empty());

        let mut pruned = table_handle_params(&schema, Some(&spec));
        pruned.enforced_predicate = TupleDomain::none();
        let handle = IcebergTableHandle::try_new(pruned).expect("handle");
        let mut source = split_source_for(&handle, vec![planned(read_file("a.parquet", 100, 10))]);
        assert!(all_splits(&mut source, 8).is_empty());

        let mut zero_limit = table_handle_params(&schema, Some(&spec));
        zero_limit.limit = Some(0);
        let handle = IcebergTableHandle::try_new(zero_limit).expect("handle");
        let mut source = split_source_for(&handle, vec![planned(read_file("a.parquet", 100, 10))]);
        assert!(all_splits(&mut source, 8).is_empty());
    }

    #[test]
    fn a_file_whose_statistics_are_disjoint_from_the_static_predicate_is_pruned() {
        let schema = partitioned_schema();
        let spec = identity_partition_spec(&schema);
        let amount = amount_column();
        let mut params = table_handle_params(&schema, Some(&spec));
        params.unenforced_predicate = TupleDomain::with_column_domains(BTreeMap::from([(
            amount.clone(),
            Domain::new(
                ValueSet::of_values(ConnectorValueType::BigInt, vec![ConnectorValue::BigInt(5)])
                    .expect("value set"),
                false,
            ),
        )]))
        .expect("predicate");
        let handle = IcebergTableHandle::try_new(params).expect("handle");

        let mut disjoint = planned(read_file("a.parquet", 100, 10));
        disjoint.file_statistics_domain = TupleDomain::with_column_domains(BTreeMap::from([(
            amount.clone(),
            Domain::new(
                ValueSet::of_values(
                    ConnectorValueType::BigInt,
                    vec![ConnectorValue::BigInt(100)],
                )
                .expect("value set"),
                false,
            ),
        )]))
        .expect("statistics");
        let overlapping = planned(read_file("b.parquet", 100, 10));

        let mut source = split_source_for(&handle, vec![disjoint, overlapping]);
        let splits = all_splits(&mut source, 8);
        assert_eq!(splits.len(), 1);
        assert_eq!(splits[0].path(), "b.parquet");
        assert_eq!(
            source.profile_snapshot(),
            SplitSourceProfile {
                files_considered: 2,
                files_pruned: 0,
                files_expanded: 1,
                splits_emitted: 1,
            },
            "static pruning must not be reported as runtime-filter avoided work"
        );
    }

    #[test]
    fn a_completed_dynamic_filter_prunes_a_file_before_split_expansion() {
        let handle = handle_with(Some(100), BTreeSet::from([amount_column()]));
        let amount = amount_column();
        let mut disjoint = planned(read_file("a.parquet", 100, 10));
        disjoint.file_statistics_domain = TupleDomain::with_column_domains(BTreeMap::from([(
            amount.clone(),
            Domain::new(
                ValueSet::of_values(
                    ConnectorValueType::BigInt,
                    vec![ConnectorValue::BigInt(100)],
                )
                .expect("value set"),
                false,
            ),
        )]))
        .expect("statistics");
        let mut source = split_source_for(&handle, vec![disjoint]);
        let filter = DynamicFilterSnapshot::new(
            TupleDomain::with_column_domains(BTreeMap::from([(
                amount,
                Domain::new(
                    ValueSet::of_values(
                        ConnectorValueType::BigInt,
                        vec![ConnectorValue::BigInt(5)],
                    )
                    .expect("value set"),
                    false,
                ),
            )]))
            .expect("dynamic predicate"),
            true,
        );

        let batch = source.next_batch(8, &filter).expect("batch");
        assert!(batch.is_empty());
        assert!(batch.no_more_splits());
        assert_eq!(
            source.profile_snapshot(),
            SplitSourceProfile {
                files_considered: 1,
                files_pruned: 1,
                files_expanded: 0,
                splits_emitted: 0,
            }
        );
    }

    #[test]
    fn batches_are_bounded_and_an_empty_batch_is_not_the_end() {
        let handle = handle_with(Some(10), BTreeSet::from([amount_column()]));
        let mut source = split_source_for(&handle, vec![planned(read_file("a.parquet", 100, 10))]);
        let filter = DynamicFilterSnapshot::all_complete();

        let first = source.next_batch(3, &filter).expect("batch");
        assert_eq!(first.splits().len(), 3);
        assert!(!first.no_more_splits());
        assert!(!source.is_finished());

        let rest = all_splits(&mut source, 4);
        assert_eq!(rest.len(), 7);
        assert!(source.is_finished());

        assert!(source.next_batch(0, &filter).is_err());
    }

    #[test]
    fn an_unsatisfiable_dynamic_filter_finishes_without_fabricating_a_wait() {
        let handle = handle_with(Some(10), BTreeSet::from([amount_column()]));
        let mut source = split_source_for(&handle, vec![planned(read_file("a.parquet", 100, 10))]);

        let truthful = DynamicFilterSnapshot::<IcebergColumnHandle>::all_complete();
        assert!(truthful.is_complete());
        assert!(truthful.current_predicate().is_all());
        assert!(!source.next_batch(4, &truthful).expect("batch").is_empty());

        let unsatisfiable = DynamicFilterSnapshot::new(TupleDomain::none(), true);
        let batch = source.next_batch(4, &unsatisfiable).expect("batch");
        assert!(batch.is_empty());
        assert!(batch.no_more_splits());
    }

    #[test]
    fn close_is_idempotent_and_cannot_retract_a_delivered_batch() {
        let handle = handle_with(Some(10), BTreeSet::from([amount_column()]));
        let mut source = split_source_for(&handle, vec![planned(read_file("a.parquet", 100, 10))]);
        let filter = DynamicFilterSnapshot::all_complete();

        let outstanding = source.next_batch(2, &filter).expect("batch");
        source.close().expect("close");
        source.close().expect("close again");

        assert_eq!(outstanding.splits().len(), 2);
        assert!(source.is_finished());
        let after_close = source.next_batch(2, &filter).expect("batch");
        assert!(after_close.is_empty());
        assert!(after_close.no_more_splits());
    }

    #[test]
    fn orc_avro_and_encryption_are_rejected_at_split_production() {
        let handle = handle_with(Some(100), BTreeSet::from([amount_column()]));
        let filter = DynamicFilterSnapshot::all_complete();

        for format in [IcebergFileFormat::Orc, IcebergFileFormat::Avro] {
            let mut file = planned(read_file("a.orc", 100, 10));
            file.file_format = format;
            let mut source = split_source_for(&handle, vec![file]);
            let error = source
                .next_batch(4, &filter)
                .expect_err("unsupported format");
            assert_eq!(error.kind(), ConnectorErrorKind::Unsupported);
        }

        let mut encrypted_manifest = planned(read_file("a.parquet", 100, 10));
        encrypted_manifest.key_metadata = vec![1, 2, 3];
        let mut source = split_source_for(&handle, vec![encrypted_manifest]);
        assert_eq!(
            source
                .next_batch(4, &filter)
                .expect_err("encrypted manifest")
                .kind(),
            ConnectorErrorKind::Unsupported
        );

        let mut encrypted_file = planned(read_file("a.parquet", 100, 10));
        encrypted_file.decryption_data =
            Some(ParquetFileDecryptionData::try_new(vec![9], vec![]).expect("material"));
        let mut source = split_source_for(&handle, vec![encrypted_file]);
        assert_eq!(
            source
                .next_batch(4, &filter)
                .expect_err("encrypted parquet")
                .kind(),
            ConnectorErrorKind::Unsupported
        );
    }

    #[test]
    fn a_puffin_deletion_vector_keeps_its_addressed_content_range() {
        let handle = handle_with(Some(1_000), BTreeSet::from([amount_column()]));
        let mut read = read_file("a.parquet", 100, 10);
        read.deletes.push(IcebergReadDeleteFile {
            path: "dv.puffin".to_string(),
            file_format: IcebergReadDeleteFormat::Puffin,
            kind: IcebergReadDeleteKind::Position,
            length: Some(64),
            content_offset: Some(4),
            content_size_in_bytes: Some(32),
            sequence_number: Some(11),
            partition_spec_id: Some(7),
            partition_key: None,
            referenced_data_file: Some("a.parquet".to_string()),
        });
        let mut file = planned(read);
        file.delete_facts.insert(
            "dv.puffin".to_string(),
            IcebergDeleteFileFacts {
                record_count: 1,
                row_position_lower_bound: None,
                row_position_upper_bound: None,
                decryption_data: None,
            },
        );

        let mut source = split_source_for(&handle, vec![file]);
        let splits = all_splits(&mut source, 8);
        let delete = &splits[0].deletes()[0];
        assert_eq!(delete.format(), IcebergFileFormat::Puffin);
        assert_eq!(delete.content_offset(), Some(4));
        assert_eq!(delete.content_size_in_bytes(), Some(32));
    }

    #[test]
    fn a_data_file_claiming_the_puffin_format_is_rejected() {
        let handle = handle_with(Some(100), BTreeSet::from([amount_column()]));
        let mut file = planned(read_file("a.parquet", 100, 10));
        file.file_format = IcebergFileFormat::Puffin;
        let mut source = split_source_for(&handle, vec![file]);
        assert_eq!(
            source
                .next_batch(4, &DynamicFilterSnapshot::all_complete())
                .expect_err("puffin data file")
                .kind(),
            ConnectorErrorKind::CorruptData
        );
    }

    #[test]
    fn split_facts_are_copied_from_the_pinned_manifest_view() {
        let handle = handle_with(Some(1_000), BTreeSet::from([amount_column()]));
        let mut source = split_source_for(&handle, vec![planned(read_file("a.parquet", 100, 42))]);
        let splits = all_splits(&mut source, 8);
        let split = &splits[0];

        assert_eq!(split.file_record_count(), 42);
        assert_eq!(split.data_sequence_number(), Some(9));
        assert_eq!(split.file_first_row_id(), Some(500));
        assert_eq!(split.partition_spec_id(), 7);
        assert_eq!(split.partition_data_json(), "{\"1000\":\"emea\"}");
        assert_eq!(split.file_format(), IcebergFileFormat::Parquet);
        assert!(split.decryption_data().is_none());
    }

    #[test]
    fn a_missing_planning_fact_fails_closed() {
        let handle = handle_with(Some(1_000), BTreeSet::from([amount_column()]));
        let filter = DynamicFilterSnapshot::all_complete();

        let mut missing_records = read_file("a.parquet", 100, 10);
        missing_records.record_count = None;
        let mut source = split_source_for(&handle, vec![planned(missing_records)]);
        assert_eq!(
            source
                .next_batch(4, &filter)
                .expect_err("no record count")
                .kind(),
            ConnectorErrorKind::CorruptData
        );

        let mut missing_partition = read_file("a.parquet", 100, 10);
        missing_partition.partition_values = None;
        let mut source = split_source_for(&handle, vec![planned(missing_partition)]);
        assert_eq!(
            source
                .next_batch(4, &filter)
                .expect_err("no partition values")
                .kind(),
            ConnectorErrorKind::CorruptData
        );

        let mut wrong_arity = read_file("a.parquet", 100, 10);
        wrong_arity.partition_values = Some(Struct::empty());
        let mut source = split_source_for(&handle, vec![planned(wrong_arity)]);
        assert_eq!(
            source
                .next_batch(4, &filter)
                .expect_err("partition arity mismatch")
                .kind(),
            ConnectorErrorKind::CorruptData
        );

        let mut unknown_spec = read_file("a.parquet", 100, 10);
        unknown_spec.partition_spec_id = Some(99);
        let mut source = split_source_for(&handle, vec![planned(unknown_spec)]);
        assert_eq!(
            source
                .next_batch(4, &filter)
                .expect_err("unknown spec")
                .kind(),
            ConnectorErrorKind::CorruptData
        );
    }

    #[test]
    fn the_target_split_size_falls_back_from_session_to_table_property_to_default() {
        let schema = partitioned_schema();
        let spec = identity_partition_spec(&schema);
        let mut params = table_handle_params(&schema, Some(&spec));
        params.projected_columns = BTreeSet::from([amount_column()]);
        params.storage_properties.insert(
            READ_SPLIT_TARGET_SIZE_PROPERTY.to_string(),
            "64".to_string(),
        );
        let handle = IcebergTableHandle::try_new(params).expect("handle");

        let mut from_property =
            split_source_for(&handle, vec![planned(read_file("a.parquet", 128, 10))]);
        assert_eq!(all_splits(&mut from_property, 8).len(), 2);

        let mut from_session = IcebergSplitSource::try_new(
            &handle,
            vec![planned(read_file("a.parquet", 128, 10))],
            IcebergSplitSourceOptions {
                session_max_split_size: Some(128),
                ..IcebergSplitSourceOptions::default()
            },
        )
        .expect("split source");
        assert_eq!(all_splits(&mut from_session, 8).len(), 1);

        let default_handle = handle_with(None, BTreeSet::from([amount_column()]));
        let mut from_default = split_source_for(
            &default_handle,
            vec![planned(read_file("a.parquet", 128, 10))],
        );
        assert_eq!(all_splits(&mut from_default, 8).len(), 1);

        let mut bad = table_handle_params(&schema, Some(&spec));
        bad.storage_properties.insert(
            READ_SPLIT_TARGET_SIZE_PROPERTY.to_string(),
            "huge".to_string(),
        );
        let bad_handle = IcebergTableHandle::try_new(bad).expect("handle");
        assert!(
            IcebergSplitSource::try_new(
                &bad_handle,
                Vec::new(),
                IcebergSplitSourceOptions::default()
            )
            .is_err()
        );
    }

    #[test]
    fn split_weight_reflects_the_range_and_its_delete_closure() {
        let handle = handle_with(Some(100), BTreeSet::from([amount_column()]));
        let mut source = split_source_for(&handle, vec![planned(read_file("a.parquet", 50, 10))]);
        let splits = all_splits(&mut source, 8);
        assert_eq!(
            novarocks_spi::connector::read_stack::ConnectorSplit::split_weight(&splits[0])
                .raw_value(),
            50
        );
    }

    #[test]
    fn the_identity_partition_helper_only_reports_identity_fields() {
        let schema = partitioned_schema();
        let spec = identity_partition_spec(&schema);
        assert_eq!(
            identity_partition_source_field_ids(&spec),
            BTreeSet::from([2])
        );
        assert!(identity_partition_source_field_ids(&PartitionSpec::unpartition_spec()).is_empty());
    }

    #[test]
    fn schema_field_order_indexes_nested_fields_in_pre_order() {
        let schema = crate::typed_read::column_handle::tests::nested_schema();
        let order = schema_field_order(&schema);
        assert_eq!(order.get(&1), Some(&0));
        assert_eq!(order.get(&2), Some(&1));
        assert_eq!(order.get(&3), Some(&2));
        assert_eq!(order.get(&4), Some(&3));
        assert_eq!(order.get(&6), Some(&5));
        assert_eq!(order.get(&9), Some(&8));
    }

    #[test]
    fn a_partition_spec_mismatch_between_a_delete_and_its_data_file_is_rejected() {
        // The frozen applicability rule keys on the partition grouping string,
        // not on the typed partition JSON the split carries.
        let handle = handle_with(Some(1_000), BTreeSet::from([amount_column()]));
        let mut read = read_file("a.parquet", 100, 10);
        read.partition_key =
            iceberg_partition_key(&Struct::from_iter([Some(IcebergLiteral::string("emea"))]));
        read.deletes.push(IcebergReadDeleteFile {
            path: "e0.parquet".to_string(),
            file_format: IcebergReadDeleteFormat::Parquet,
            kind: IcebergReadDeleteKind::Equality {
                equality_field_ids: vec![1],
            },
            length: Some(128),
            content_offset: None,
            content_size_in_bytes: None,
            sequence_number: Some(12),
            partition_spec_id: Some(7),
            partition_key: iceberg_partition_key(&Struct::from_iter([Some(
                IcebergLiteral::string("apac"),
            )])),
            referenced_data_file: None,
        });
        let mut file = planned(read);
        file.delete_facts.insert(
            "e0.parquet".to_string(),
            IcebergDeleteFileFacts {
                record_count: 4,
                row_position_lower_bound: None,
                row_position_upper_bound: None,
                decryption_data: None,
            },
        );

        let mut source = split_source_for(&handle, vec![file]);
        assert_eq!(
            source
                .next_batch(4, &DynamicFilterSnapshot::all_complete())
                .expect_err("delete from another partition")
                .kind(),
            ConnectorErrorKind::CorruptData
        );
    }

    // -----------------------------------------------------------------------
    // Change-window enumeration
    // -----------------------------------------------------------------------

    fn change_window_handle() -> IcebergChangeWindowHandle {
        let schema = partitioned_schema();
        IcebergChangeWindowHandle::try_new(IcebergChangeWindowHandleParams {
            schema_table_name: SchemaTableName::try_new("db", "t").expect("schema table name"),
            table_schema_json: serde_json::to_string(&schema).expect("schema json"),
            columns: vec![region_column(), amount_column()],
            name_mapping_json: None,
            from_snapshot_id_exclusive: 10,
            to_snapshot_id_inclusive: 20,
            partition_spec_jsons: BTreeMap::new(),
        })
        .expect("change window handle")
    }

    fn change_partition_types() -> BTreeMap<i32, Type> {
        let schema = partitioned_schema();
        let spec = identity_partition_spec(&schema);
        BTreeMap::from([(
            spec.spec_id(),
            Type::Struct(spec.partition_type(&schema).expect("partition type")),
        )])
    }

    fn position_delete_of(
        path: &str,
        data_path: &str,
        sequence_number: i64,
    ) -> IcebergReadDeleteFile {
        IcebergReadDeleteFile {
            path: path.to_string(),
            file_format: IcebergReadDeleteFormat::Parquet,
            kind: IcebergReadDeleteKind::Position,
            length: Some(64),
            content_offset: None,
            content_size_in_bytes: None,
            sequence_number: Some(sequence_number),
            partition_spec_id: Some(7),
            partition_key: None,
            referenced_data_file: Some(data_path.to_string()),
        }
    }

    fn equality_delete_of(path: &str, sequence_number: i64) -> IcebergReadDeleteFile {
        IcebergReadDeleteFile {
            path: path.to_string(),
            file_format: IcebergReadDeleteFormat::Parquet,
            kind: IcebergReadDeleteKind::Equality {
                equality_field_ids: vec![1],
            },
            length: Some(64),
            content_offset: None,
            content_size_in_bytes: None,
            sequence_number: Some(sequence_number),
            partition_spec_id: Some(7),
            partition_key: None,
            referenced_data_file: None,
        }
    }

    /// One endpoint's view of a file, with manifest facts for every delete it
    /// carries so enumeration never fails for a missing fact.
    fn endpoint_file(path: &str, deletes: Vec<IcebergReadDeleteFile>) -> IcebergPlannedDataFile {
        let mut read = read_file(path, 100, 10);
        read.deletes = deletes;
        let mut file = planned(read);
        for delete in file.read_file.deletes.clone() {
            file.delete_facts.insert(
                delete.path.clone(),
                IcebergDeleteFileFacts {
                    record_count: 2,
                    row_position_lower_bound: Some(0),
                    row_position_upper_bound: Some(5),
                    decryption_data: None,
                },
            );
        }
        file
    }

    fn window_plan(
        from_visible: Vec<IcebergPlannedDataFile>,
        to_visible: Vec<IcebergPlannedDataFile>,
    ) -> Result<IcebergChangeWindowPlan, ConnectorError> {
        plan_change_window_splits(
            &change_window_handle(),
            IcebergChangeWindowEndpoints {
                from_visible: &from_visible,
                to_visible: &to_visible,
            },
            &change_partition_types(),
            IcebergSplitSourceOptions::default(),
        )
    }

    fn emitted(plan: &IcebergChangeWindowPlan) -> BTreeSet<(String, i8)> {
        plan.splits()
            .iter()
            .map(|split| (split.data().path().to_string(), split.change_op()))
            .collect()
    }

    #[test]
    fn a_row_written_and_deleted_inside_the_window_is_not_emitted_by_the_forward_side() {
        // `added.parquet` was written after `from` and one of its rows was
        // deleted again before `to`. That row is invisible at *both* endpoints,
        // so the difference does not own it. The forward split proves this by
        // carrying the upper endpoint's own delete closure: the reader emits
        // the rows the file still has at `to`, never the rows it was written
        // with. Replaying the snapshots in between would emit the row as an
        // insert and, on a good day, its delete too.
        let plan = window_plan(
            Vec::new(),
            vec![endpoint_file(
                "added.parquet",
                vec![position_delete_of("d0.parquet", "added.parquet", 11)],
            )],
        )
        .expect("plan");

        assert_eq!(plan.splits().len(), 1);
        let split = &plan.splits()[0];
        assert_eq!(split.side(), IcebergChangeSide::Forward);
        assert_eq!(split.data().deletes().len(), 1);
        assert_eq!(split.data().deletes()[0].path(), "d0.parquet");
        // The file did not exist at `from`, so nothing on the reverse side
        // claims those same rows a second time.
        assert!(
            plan.splits()
                .iter()
                .all(|split| split.side() == IcebergChangeSide::Forward)
        );
    }

    #[test]
    fn only_a_files_endpoint_membership_puts_it_into_the_difference() {
        // Three of the four cases are representable as endpoint input: a file
        // in both endpoints, one only at `to`, and one only at `from`. The
        // fourth -- written and dropped again inside the window -- is visible
        // at neither endpoint, so it cannot even be named here: it appears in
        // neither index and no branch can reach it. A manifest replay would
        // find its add and its delete and emit both.
        let plan = window_plan(
            vec![
                endpoint_file("kept.parquet", Vec::new()),
                endpoint_file("removed.parquet", Vec::new()),
            ],
            vec![
                endpoint_file("kept.parquet", Vec::new()),
                endpoint_file("added.parquet", Vec::new()),
            ],
        )
        .expect("plan");

        assert_eq!(
            emitted(&plan),
            BTreeSet::from([
                ("added.parquet".to_string(), 1),
                ("removed.parquet".to_string(), -1),
            ])
        );
    }

    #[test]
    fn the_change_op_of_an_enumerated_split_follows_its_variant() {
        let plan = window_plan(
            vec![
                endpoint_file("kept.parquet", Vec::new()),
                endpoint_file("removed.parquet", Vec::new()),
            ],
            vec![
                endpoint_file(
                    "kept.parquet",
                    vec![position_delete_of("d1.parquet", "kept.parquet", 12)],
                ),
                endpoint_file("added.parquet", Vec::new()),
            ],
        )
        .expect("plan");

        let mut seen = BTreeMap::new();
        for split in plan.splits() {
            let variant = match split {
                IcebergChangeSplit::AddedRows(_) => "added",
                IcebergChangeSplit::PositionDeletedRows(_) => "position",
                IcebergChangeSplit::EqualityDeletedRows(_) => "equality",
                IcebergChangeSplit::DeletedDataFileRows(_) => "removed",
            };
            seen.insert(
                split.data().path().to_string(),
                (variant, split.change_op()),
            );
        }
        assert_eq!(
            seen,
            BTreeMap::from([
                ("added.parquet".to_string(), ("added", 1_i8)),
                ("kept.parquet".to_string(), ("position", -1)),
                ("removed.parquet".to_string(), ("removed", -1)),
            ])
        );
    }

    #[test]
    fn a_reverse_side_split_names_its_deletes_instead_of_carrying_an_exclusion_closure() {
        let plan = window_plan(
            vec![endpoint_file(
                "removed.parquet",
                vec![position_delete_of("d2.parquet", "removed.parquet", 11)],
            )],
            Vec::new(),
        )
        .expect("plan");

        assert_eq!(plan.splits().len(), 1);
        let IcebergChangeSplit::DeletedDataFileRows(rows) = &plan.splits()[0] else {
            panic!("a file gone at the upper endpoint is a removed data file");
        };
        // The rows already invisible at `from` are subtracted, not emitted, and
        // they travel as typed variant facts rather than as the data split's
        // own exclusion closure.
        assert!(rows.data().deletes().is_empty());
        assert_eq!(rows.previously_applied_deletes().len(), 1);
        assert_eq!(rows.previously_applied_deletes()[0].path(), "d2.parquet");
    }

    #[test]
    fn an_equality_deleted_file_subtracts_the_position_deletes_of_the_same_window() {
        let plan = window_plan(
            vec![endpoint_file("kept.parquet", Vec::new())],
            vec![endpoint_file(
                "kept.parquet",
                vec![
                    position_delete_of("d3.parquet", "kept.parquet", 12),
                    equality_delete_of("e3.parquet", 13),
                ],
            )],
        )
        .expect("plan");

        assert_eq!(plan.splits().len(), 2);
        let equality = plan
            .splits()
            .iter()
            .find_map(|split| match split {
                IcebergChangeSplit::EqualityDeletedRows(rows) => Some(rows),
                IcebergChangeSplit::AddedRows(_)
                | IcebergChangeSplit::PositionDeletedRows(_)
                | IcebergChangeSplit::DeletedDataFileRows(_) => None,
            })
            .expect("an equality-deleted variant");
        assert_eq!(equality.newly_applied_equality_deletes().len(), 1);
        // The position variant already owns the rows `d3.parquet` removed, so
        // the equality variant is handed it as already applied and emits only
        // what is left.
        assert_eq!(
            equality
                .previously_applied_deletes()
                .iter()
                .map(|delete| delete.path().to_string())
                .collect::<Vec<_>>(),
            vec!["d3.parquet".to_string()]
        );
    }

    #[test]
    fn a_replaced_delete_closure_subtracts_its_previous_dv() {
        let plan = window_plan(
            vec![endpoint_file(
                "kept.parquet",
                vec![position_delete_of("d4.parquet", "kept.parquet", 12)],
            )],
            vec![endpoint_file(
                "kept.parquet",
                vec![position_delete_of("d5.parquet", "kept.parquet", 13)],
            )],
        )
        .expect("replacement deletion vector");
        let IcebergChangeSplit::PositionDeletedRows(rows) = &plan.splits()[0] else {
            panic!("replacement closure emits a position-delete split");
        };
        assert_eq!(rows.newly_applied_deletes()[0].path(), "d5.parquet");
        assert_eq!(rows.previously_applied_deletes()[0].path(), "d4.parquet");
    }

    #[test]
    fn a_delete_that_stopped_applying_without_replacement_fails_closed() {
        // Rows of a surviving file becoming visible again is a forward-side row
        // set of a file that was never added. None of the four typed variants
        // can express it, so it is rejected rather than dropped.
        let error = window_plan(
            vec![endpoint_file(
                "kept.parquet",
                vec![position_delete_of("d4.parquet", "kept.parquet", 12)],
            )],
            vec![endpoint_file("kept.parquet", Vec::new())],
        )
        .expect_err("a delete stopped applying without replacement");
        assert_eq!(error.kind(), ConnectorErrorKind::Unsupported);
    }

    #[test]
    fn one_data_file_listed_twice_at_an_endpoint_is_corrupt_data() {
        let error = window_plan(
            Vec::new(),
            vec![
                endpoint_file("added.parquet", Vec::new()),
                endpoint_file("added.parquet", Vec::new()),
            ],
        )
        .expect_err("duplicate endpoint entry");
        assert_eq!(error.kind(), ConnectorErrorKind::CorruptData);
    }

    #[test]
    fn change_window_batches_are_bounded_and_close_is_idempotent() {
        let plan = window_plan(
            vec![endpoint_file("removed.parquet", Vec::new())],
            vec![
                endpoint_file("added.parquet", Vec::new()),
                endpoint_file("second.parquet", Vec::new()),
            ],
        )
        .expect("plan");
        assert_eq!(plan.splits().len(), 3);

        let filter = DynamicFilterSnapshot::all_complete();
        let mut source = IcebergChangeWindowSplitSource::new(plan);
        let first = source.next_batch(2, &filter).expect("first batch");
        assert_eq!(first.into_splits().len(), 2);
        let second = source.next_batch(2, &filter).expect("second batch");
        assert!(second.no_more_splits());
        assert_eq!(second.into_splits().len(), 1);
        assert!(source.is_finished());

        assert!(source.close().is_ok());
        assert!(source.close().is_ok());
        assert!(
            source
                .next_batch(2, &filter)
                .expect("closed batch")
                .into_splits()
                .is_empty()
        );
        assert!(source.next_batch(0, &filter).is_err());
    }

    #[test]
    fn an_unsatisfiable_predicate_ends_change_window_enumeration_without_narrowing_it() {
        let plan = window_plan(Vec::new(), vec![endpoint_file("added.parquet", Vec::new())])
            .expect("plan");
        let mut source = IcebergChangeWindowSplitSource::new(plan);
        let unsatisfiable = DynamicFilterSnapshot::new(TupleDomain::none(), true);
        let batch = source.next_batch(4, &unsatisfiable).expect("batch");
        assert!(batch.no_more_splits());
        assert!(batch.into_splits().is_empty());
        assert!(source.is_finished());
    }
}
