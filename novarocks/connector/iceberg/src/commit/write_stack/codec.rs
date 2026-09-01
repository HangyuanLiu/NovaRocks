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

//! Iceberg's four directional connector-write codec facets.
//!
//! This is the only Iceberg module that turns a central-IDL write carrier into
//! an Iceberg write domain value, or the reverse. Each facet holds the
//! provider-private [`IcebergWriteAdapter`] of one exact catalog generation and
//! nothing else, so:
//!
//! * an **encoder** can only start from a neutral value its own generation
//!   minted — the adapter refuses every other one — and it has no method that
//!   accepts untrusted carrier data at all; and
//! * a **decoder** rewraps the value it builds with its own generation's
//!   binding, so a decoded value is usable only by that generation.
//!
//! Two layers of validation meet here and neither substitutes for the other.
//! `novarocks-proto-codec` proves a carrier is canonical, in bounds, and
//! structurally an Iceberg write carrier; it deliberately knows nothing about
//! Iceberg's cross-field rules. Those live in the domain constructors, so every
//! decode below routes through `try_new*` rather than assembling a struct
//! field by field. Building one by hand would let a wire value exist that the
//! provider's own rules would have rejected — for example a Puffin writer
//! carrying a Parquet row-group size, or a merge target frozen against another
//! snapshot — and nothing downstream would ever catch it.
//!
//! Where a domain fact has no faithful carrier the answer is an error carrying
//! the real field path, never a default and never a silent narrowing.

use std::sync::Arc;

use novarocks_proto_codec::connector_write::{
    ConnectorWriteCodecError, ConnectorWriteFragmentDecoder, ConnectorWriteFragmentEncoder,
    ConnectorWriteHandleDecoder, ConnectorWriteHandleEncoder, ValidatedCommitFragment,
    ValidatedWriterHandle,
};
use novarocks_proto_codec::{FieldPath, ProtocolError, ProtocolErrorKind};
use novarocks_proto_models::connector_write as dto;
use novarocks_spi::connector::write_stack::{ConnectorCommitFragment, ConnectorWriterHandle};
use novarocks_spi::connector::{ConnectorError, ConnectorErrorKind};
use parquet::basic::{Compression, GzipLevel, ZstdLevel};

use crate::commit::report::IcebergColumnStats;
use crate::commit::write_stack::domain::{
    IcebergArtifactMetrics, IcebergArtifactPartition, IcebergCommitArtifact, IcebergCommitFragment,
    IcebergContentRange, IcebergDataBranchRecipe, IcebergDataFileArtifact,
    IcebergDeletionVectorArtifact, IcebergPositionDeleteFileArtifact, IcebergWriteBranch,
    IcebergWriteTableFacts, IcebergWriterHandle, IcebergWriterOutput,
};
use crate::commit::write_stack::old_delete::{
    IcebergOldDeleteArtifactRef, IcebergOldDeleteMergeTarget, IcebergStorageRoute,
};
use crate::commit::write_stack::runtime::IcebergWriteAdapter;
use crate::delete_file::{IcebergFileContent, IcebergFileFormat};
use crate::scan_model::IcebergSchemaDef;
use crate::write_descriptor::{IcebergPartitionDescriptor, IcebergPartitionValueDescriptor};

/// The shared half of all four facets: one exact generation's adapter and the
/// owner name every rejection is attributed to.
///
/// It is deliberately not public and has no accessor for its adapter. A facet
/// is installed as an `Arc<dyn ...>` trait object, so a role host holds a codec
/// it can call and can never reach the adapter, an erased payload, or a
/// downcast behind it.
#[derive(Clone)]
struct IcebergWriteCodec {
    adapter: IcebergWriteAdapter,
    owner: Arc<str>,
}

impl IcebergWriteCodec {
    fn new(adapter: IcebergWriteAdapter) -> Self {
        Self {
            owner: Arc::from(adapter.binding().descriptor().instance_id.as_str()),
            adapter,
        }
    }

    fn invalid(&self, path: FieldPath, detail: impl Into<String>) -> ConnectorWriteCodecError {
        ConnectorWriteCodecError::invalid(&self.owner, path, detail)
    }

    /// Surface a provider constructor's own refusal verbatim, at the field it
    /// belongs to.
    ///
    /// `CorruptData` means two facts that each look legal alone contradict each
    /// other, which is exactly a conflict; anything else is an invalid value.
    /// Either way the domain's sentence is kept, because it names the rule that
    /// was broken more precisely than the codec ever could.
    fn rejected(&self, path: FieldPath, error: &ConnectorError) -> ConnectorWriteCodecError {
        match error.kind() {
            ConnectorErrorKind::CorruptData => {
                ConnectorWriteCodecError::conflict(&self.owner, path, error.message())
            }
            _ => ConnectorWriteCodecError::invalid(&self.owner, path, error.message()),
        }
    }

    /// A required message the carrier did not carry.
    ///
    /// `ValidatedWriterHandle` / `ValidatedCommitFragment` already prove every
    /// one of these is present, so reaching here means the two layers disagree.
    /// That is still an error rather than an `expect`: a decoder must not be
    /// the thing that panics on a carrier.
    fn missing(&self, path: FieldPath, detail: &'static str) -> ConnectorWriteCodecError {
        ConnectorWriteCodecError::new(
            &self.owner,
            ProtocolError::new(path, ProtocolErrorKind::MissingField, detail),
        )
    }

    // -- enums ------------------------------------------------------------

    fn encode_file_format(
        &self,
        format: IcebergFileFormat,
        path: FieldPath,
    ) -> Result<i32, ConnectorWriteCodecError> {
        match format {
            IcebergFileFormat::Parquet => Ok(dto::IcebergFileFormat::Parquet as i32),
            IcebergFileFormat::Puffin => Ok(dto::IcebergFileFormat::Puffin as i32),
            IcebergFileFormat::Unknown => Err(self.invalid(
                path,
                "an Iceberg write carrier requires an exact file format",
            )),
        }
    }

    fn decode_file_format(
        &self,
        value: i32,
        path: FieldPath,
    ) -> Result<IcebergFileFormat, ConnectorWriteCodecError> {
        match dto::IcebergFileFormat::try_from(value) {
            Ok(dto::IcebergFileFormat::Parquet) => Ok(IcebergFileFormat::Parquet),
            Ok(dto::IcebergFileFormat::Puffin) => Ok(IcebergFileFormat::Puffin),
            Ok(dto::IcebergFileFormat::Unspecified) | Err(_) => Err(self.invalid(
                path,
                "an Iceberg write carrier requires a named file format",
            )),
        }
    }

    fn encode_file_content(&self, content: IcebergFileContent) -> i32 {
        match content {
            IcebergFileContent::Data => dto::IcebergFileContent::Data as i32,
            IcebergFileContent::PositionDeletes => dto::IcebergFileContent::PositionDeletes as i32,
            IcebergFileContent::EqualityDeletes => dto::IcebergFileContent::EqualityDeletes as i32,
        }
    }

    fn decode_file_content(
        &self,
        value: i32,
        path: FieldPath,
    ) -> Result<IcebergFileContent, ConnectorWriteCodecError> {
        match dto::IcebergFileContent::try_from(value) {
            Ok(dto::IcebergFileContent::Data) => Ok(IcebergFileContent::Data),
            Ok(dto::IcebergFileContent::PositionDeletes) => Ok(IcebergFileContent::PositionDeletes),
            Ok(dto::IcebergFileContent::EqualityDeletes) => Ok(IcebergFileContent::EqualityDeletes),
            Ok(dto::IcebergFileContent::Unspecified) | Err(_) => Err(self.invalid(
                path,
                "an Iceberg write carrier requires a named file content kind",
            )),
        }
    }

    fn encode_branch(&self, branch: IcebergWriteBranch) -> i32 {
        match branch {
            IcebergWriteBranch::Data => dto::IcebergWriteBranch::Data as i32,
            IcebergWriteBranch::PositionDelete => dto::IcebergWriteBranch::PositionDelete as i32,
            IcebergWriteBranch::DeletionVector => dto::IcebergWriteBranch::DeletionVector as i32,
        }
    }

    fn decode_branch(
        &self,
        value: i32,
        path: FieldPath,
    ) -> Result<IcebergWriteBranch, ConnectorWriteCodecError> {
        match dto::IcebergWriteBranch::try_from(value) {
            Ok(dto::IcebergWriteBranch::Data) => Ok(IcebergWriteBranch::Data),
            Ok(dto::IcebergWriteBranch::PositionDelete) => Ok(IcebergWriteBranch::PositionDelete),
            Ok(dto::IcebergWriteBranch::DeletionVector) => Ok(IcebergWriteBranch::DeletionVector),
            Ok(dto::IcebergWriteBranch::Unspecified) | Err(_) => Err(self.invalid(
                path,
                "an Iceberg writer handle requires a named write branch",
            )),
        }
    }

    /// The carrier names a codec, not a codec *and* a level.
    ///
    /// A non-default level therefore has nowhere to go, and silently writing it
    /// at the default level would produce files the frontend did not ask for.
    /// Rejecting is the only honest answer; the production path freezes SNAPPY.
    fn encode_compression(
        &self,
        compression: Compression,
        path: FieldPath,
    ) -> Result<i32, ConnectorWriteCodecError> {
        match compression {
            Compression::UNCOMPRESSED => Ok(dto::IcebergCompression::None as i32),
            Compression::SNAPPY => Ok(dto::IcebergCompression::Snappy as i32),
            Compression::LZ4 => Ok(dto::IcebergCompression::Lz4 as i32),
            Compression::GZIP(level) if level == GzipLevel::default() => {
                Ok(dto::IcebergCompression::Gzip as i32)
            }
            Compression::ZSTD(level) if level == ZstdLevel::default() => {
                Ok(dto::IcebergCompression::Zstd as i32)
            }
            other => Err(self.invalid(
                path,
                format!("the Iceberg write carrier cannot express compression {other:?}"),
            )),
        }
    }

    fn decode_compression(
        &self,
        value: i32,
        path: FieldPath,
    ) -> Result<Compression, ConnectorWriteCodecError> {
        match dto::IcebergCompression::try_from(value) {
            Ok(dto::IcebergCompression::None) => Ok(Compression::UNCOMPRESSED),
            Ok(dto::IcebergCompression::Snappy) => Ok(Compression::SNAPPY),
            Ok(dto::IcebergCompression::Gzip) => Ok(Compression::GZIP(GzipLevel::default())),
            Ok(dto::IcebergCompression::Lz4) => Ok(Compression::LZ4),
            Ok(dto::IcebergCompression::Zstd) => Ok(Compression::ZSTD(ZstdLevel::default())),
            Ok(dto::IcebergCompression::Unspecified) | Err(_) => Err(self.invalid(
                path,
                "an Iceberg writer output requires a named compression codec",
            )),
        }
    }

    // -- shared value shapes ----------------------------------------------

    fn encode_content_range(&self, range: IcebergContentRange) -> dto::IcebergContentRange {
        dto::IcebergContentRange {
            offset: range.offset(),
            size_in_bytes: range.size_in_bytes(),
        }
    }

    fn decode_content_range(
        &self,
        range: &dto::IcebergContentRange,
        path: FieldPath,
    ) -> Result<IcebergContentRange, ConnectorWriteCodecError> {
        IcebergContentRange::try_new(range.offset, range.size_in_bytes)
            .map_err(|error| self.rejected(path, &error))
    }

    fn encode_partition(
        &self,
        partition: &IcebergArtifactPartition,
    ) -> dto::IcebergArtifactPartition {
        dto::IcebergArtifactPartition {
            partition_path: partition.partition_path().to_string(),
            null_fingerprint: partition.null_fingerprint().to_string(),
            partition_spec_id: partition.partition_spec_id(),
            descriptor: Some(dto::IcebergPartitionDescriptor {
                values: partition
                    .descriptor()
                    .values
                    .iter()
                    .map(|value| dto::IcebergPartitionValueDescriptor {
                        is_null: value.is_null,
                        datum_bytes: value.datum_bytes.clone(),
                    })
                    .collect(),
            }),
        }
    }

    fn decode_partition(
        &self,
        partition: Option<&dto::IcebergArtifactPartition>,
        path: FieldPath,
    ) -> Result<IcebergArtifactPartition, ConnectorWriteCodecError> {
        let partition = partition.ok_or_else(|| {
            self.missing(
                path.clone(),
                "an Iceberg write carrier requires its partition",
            )
        })?;
        let descriptor = partition.descriptor.as_ref().ok_or_else(|| {
            self.missing(
                path.field("descriptor"),
                "an Iceberg artifact partition requires its descriptor",
            )
        })?;
        let values = descriptor
            .values
            .iter()
            .map(|value| IcebergPartitionValueDescriptor {
                is_null: value.is_null,
                datum_bytes: value.datum_bytes.clone(),
            })
            .collect();
        // `IcebergArtifactPartition::try_new` owns the null/datum agreement:
        // repairing it here would move a row into a different partition.
        IcebergArtifactPartition::try_new(
            partition.partition_path.clone(),
            partition.null_fingerprint.clone(),
            partition.partition_spec_id,
            IcebergPartitionDescriptor { values },
        )
        .map_err(|error| self.rejected(path, &error))
    }

    fn encode_metrics(&self, metrics: &IcebergArtifactMetrics) -> dto::IcebergArtifactMetrics {
        dto::IcebergArtifactMetrics {
            record_count: metrics.record_count(),
            file_size_in_bytes: metrics.file_size_in_bytes(),
            split_offsets: metrics.split_offsets().to_vec(),
            column_stats: metrics.column_stats().map(|stats| dto::IcebergColumnStats {
                column_sizes: stats.column_sizes.clone(),
                value_counts: stats.value_counts.clone(),
                null_value_counts: stats.null_value_counts.clone(),
                nan_value_counts: stats.nan_value_counts.clone(),
                lower_bounds: stats.lower_bounds.clone(),
                upper_bounds: stats.upper_bounds.clone(),
            }),
        }
    }

    fn decode_metrics(
        &self,
        metrics: Option<&dto::IcebergArtifactMetrics>,
        path: FieldPath,
    ) -> Result<IcebergArtifactMetrics, ConnectorWriteCodecError> {
        let metrics = metrics.ok_or_else(|| {
            self.missing(path.clone(), "an Iceberg artifact requires its metrics")
        })?;
        let column_stats = metrics
            .column_stats
            .as_ref()
            .map(|stats| IcebergColumnStats {
                column_sizes: stats.column_sizes.clone(),
                value_counts: stats.value_counts.clone(),
                null_value_counts: stats.null_value_counts.clone(),
                nan_value_counts: stats.nan_value_counts.clone(),
                lower_bounds: stats.lower_bounds.clone(),
                upper_bounds: stats.upper_bounds.clone(),
            });
        IcebergArtifactMetrics::try_new(
            metrics.record_count,
            metrics.file_size_in_bytes,
            metrics.split_offsets.clone(),
            column_stats,
        )
        .map_err(|error| self.rejected(path, &error))
    }

    /// The carrier holds one route field, and a route's prefix is the only part
    /// of it that decides which objects the route covers.
    ///
    /// Every production route is derived from the artifact's own location, so
    /// deriving it back from the prefix is exact. A hand-assembled route whose
    /// scheme or authority does not follow from its own prefix has no faithful
    /// carrier, and is refused here rather than quietly rewritten on the far
    /// side into a route naming storage the artifact does not live in.
    fn encode_storage_route(
        &self,
        route: &IcebergStorageRoute,
        path: FieldPath,
    ) -> Result<dto::IcebergStorageRoute, ConnectorWriteCodecError> {
        let access_binding = route.prefix().to_string();
        let derived = IcebergStorageRoute::try_for_location(&access_binding)
            .map_err(|error| self.rejected(path.clone(), &error))?;
        if &derived != route {
            return Err(self.invalid(
                path,
                "Iceberg storage route scheme or authority does not follow from its own prefix",
            ));
        }
        Ok(dto::IcebergStorageRoute { access_binding })
    }

    fn decode_storage_route(
        &self,
        route: Option<&dto::IcebergStorageRoute>,
        path: FieldPath,
    ) -> Result<IcebergStorageRoute, ConnectorWriteCodecError> {
        let route = route.ok_or_else(|| {
            self.missing(
                path.clone(),
                "an Iceberg old delete reference requires its storage route",
            )
        })?;
        IcebergStorageRoute::try_for_location(&route.access_binding)
            .map_err(|error| self.rejected(path.field("access_binding"), &error))
    }

    // -- writer handle -----------------------------------------------------

    fn encode_table(&self, table: &IcebergWriteTableFacts) -> dto::IcebergWriteTableFacts {
        dto::IcebergWriteTableFacts {
            table_uuid: table.table_uuid().to_string(),
            namespace: table.namespace().to_string(),
            table_name: table.table_name().to_string(),
            table_location: table.table_location().to_string(),
            data_location: table.data_location().to_string(),
            target_ref: table.target_ref().to_string(),
            base_snapshot_id: table.base_snapshot_id(),
            base_sequence_number: table.base_sequence_number(),
            schema_id: table.schema_id(),
            default_partition_spec_id: table.default_partition_spec_id(),
            format_version: u32::from(table.format_version()),
        }
    }

    fn decode_table(
        &self,
        table: Option<&dto::IcebergWriteTableFacts>,
        path: FieldPath,
    ) -> Result<IcebergWriteTableFacts, ConnectorWriteCodecError> {
        let table = table.ok_or_else(|| {
            self.missing(
                path.clone(),
                "an Iceberg writer handle requires its table facts",
            )
        })?;
        let format_version = u8::try_from(table.format_version).map_err(|_| {
            self.invalid(
                path.field("format_version"),
                format!(
                    "Iceberg table format version {} is not a supported version",
                    table.format_version
                ),
            )
        })?;
        IcebergWriteTableFacts::try_new(
            table.table_uuid.clone(),
            table.namespace.clone(),
            table.table_name.clone(),
            table.table_location.clone(),
            table.data_location.clone(),
            table.target_ref.clone(),
            table.base_snapshot_id,
            table.base_sequence_number,
            table.schema_id,
            table.default_partition_spec_id,
            format_version,
        )
        .map_err(|error| self.rejected(path, &error))
    }

    fn encode_output(
        &self,
        output: &IcebergWriterOutput,
        path: FieldPath,
    ) -> Result<dto::IcebergWriterOutput, ConnectorWriteCodecError> {
        Ok(dto::IcebergWriterOutput {
            file_format: self
                .encode_file_format(output.file_format(), path.field("file_format"))?,
            compression: self
                .encode_compression(output.compression(), path.field("compression"))?,
            parquet_row_group_size_bytes: output.parquet_row_group_size_bytes(),
        })
    }

    fn decode_output(
        &self,
        output: Option<&dto::IcebergWriterOutput>,
        path: FieldPath,
    ) -> Result<IcebergWriterOutput, ConnectorWriteCodecError> {
        let output = output.ok_or_else(|| {
            self.missing(
                path.clone(),
                "an Iceberg writer handle requires its output settings",
            )
        })?;
        let file_format = self.decode_file_format(output.file_format, path.field("file_format"))?;
        let compression = self.decode_compression(output.compression, path.field("compression"))?;
        // `try_new` owns the rule that a Parquet row-group size belongs only to
        // a Parquet writer: the carrier can state both, and only Iceberg knows
        // the pairing is a contradiction.
        IcebergWriterOutput::try_new(
            file_format,
            compression,
            output.parquet_row_group_size_bytes,
        )
        .map_err(|error| self.rejected(path, &error))
    }

    fn encode_recipe(
        &self,
        recipe: &IcebergDataBranchRecipe,
        path: FieldPath,
    ) -> Result<dto::IcebergDataBranchRecipe, ConnectorWriteCodecError> {
        // The schema's own definition makes JSON its serialized form: the two
        // `Literal` convenience fields are `#[serde(skip)]` and their durable
        // spellings travel as `initial_default_json` / `write_default_json`.
        let input_schema_json = recipe
            .input_schema()
            .map(|schema| {
                serde_json::to_string(schema).map_err(|error| {
                    self.invalid(
                        path.field("input_schema_json"),
                        format!("encode Iceberg data writer input schema failed: {error}"),
                    )
                })
            })
            .transpose()?;
        Ok(dto::IcebergDataBranchRecipe {
            input_schema_json,
            partition_source_column_names: recipe.partition_source_column_names().to_vec(),
            partition_column_names: recipe.partition_column_names().to_vec(),
            transform_exprs: recipe.transform_exprs().to_vec(),
            row_lineage: recipe.row_lineage(),
        })
    }

    fn decode_recipe(
        &self,
        recipe: Option<&dto::IcebergDataBranchRecipe>,
        path: FieldPath,
    ) -> Result<IcebergDataBranchRecipe, ConnectorWriteCodecError> {
        let recipe = recipe.ok_or_else(|| {
            self.missing(
                path.clone(),
                "an Iceberg data branch requires its data recipe",
            )
        })?;
        let input_schema = recipe
            .input_schema_json
            .as_deref()
            .map(|json| {
                serde_json::from_str::<IcebergSchemaDef>(json).map_err(|error| {
                    self.invalid(
                        path.field("input_schema_json"),
                        format!("decode Iceberg data writer input schema failed: {error}"),
                    )
                })
            })
            .transpose()?;
        IcebergDataBranchRecipe::try_new(
            input_schema,
            recipe.partition_source_column_names.clone(),
            recipe.partition_column_names.clone(),
            recipe.transform_exprs.clone(),
            recipe.row_lineage,
        )
        .map_err(|error| self.rejected(path, &error))
    }

    fn encode_reference(
        &self,
        reference: &IcebergOldDeleteArtifactRef,
        path: FieldPath,
    ) -> Result<dto::IcebergOldDeleteArtifactRef, ConnectorWriteCodecError> {
        Ok(dto::IcebergOldDeleteArtifactRef {
            path: reference.path().to_string(),
            content: self.encode_file_content(reference.content()),
            file_format: self
                .encode_file_format(reference.file_format(), path.field("file_format"))?,
            file_size_in_bytes: reference.file_size_in_bytes(),
            // The wire field is not optional and the domain cannot hold a known
            // count of zero, so zero means exactly what `None` means: the frozen
            // manifest projection did not surface a count. The mapping is
            // therefore total in both directions with nothing invented.
            record_count: reference.record_count().unwrap_or(0),
            content_range: reference
                .content_range()
                .map(|range| self.encode_content_range(range)),
            referenced_data_file: reference.referenced_data_file().map(str::to_string),
            data_sequence_number: reference.data_sequence_number(),
            added_snapshot_id: reference.added_snapshot_id(),
            partition_spec_id: reference.partition_spec_id(),
            storage_route: Some(
                self.encode_storage_route(reference.storage_route(), path.field("storage_route"))?,
            ),
        })
    }

    fn decode_reference(
        &self,
        reference: &dto::IcebergOldDeleteArtifactRef,
        path: FieldPath,
    ) -> Result<IcebergOldDeleteArtifactRef, ConnectorWriteCodecError> {
        let content_range = reference
            .content_range
            .as_ref()
            .map(|range| self.decode_content_range(range, path.field("content_range")))
            .transpose()?;
        IcebergOldDeleteArtifactRef::try_new(
            reference.path.clone(),
            self.decode_file_content(reference.content, path.field("content"))?,
            self.decode_file_format(reference.file_format, path.field("file_format"))?,
            reference.file_size_in_bytes,
            (reference.record_count > 0).then_some(reference.record_count),
            content_range,
            reference.referenced_data_file.clone(),
            reference.data_sequence_number,
            reference.added_snapshot_id,
            reference.partition_spec_id,
            self.decode_storage_route(
                reference.storage_route.as_ref(),
                path.field("storage_route"),
            )?,
        )
        .map_err(|error| self.rejected(path, &error))
    }

    fn encode_merge_target(
        &self,
        target: &IcebergOldDeleteMergeTarget,
        path: FieldPath,
    ) -> Result<dto::IcebergOldDeleteMergeTarget, ConnectorWriteCodecError> {
        let mut references = Vec::with_capacity(target.references().len());
        for (index, reference) in target.references().iter().enumerate() {
            references
                .push(self.encode_reference(reference, path.field("references").index(index))?);
        }
        Ok(dto::IcebergOldDeleteMergeTarget {
            data_file_path: target.data_file_path().to_string(),
            data_file_record_count: target.data_file_record_count(),
            data_file_sequence_number: target.data_file_sequence_number(),
            partition: Some(self.encode_partition(target.partition())),
            base_snapshot_id: target.base_snapshot_id(),
            references,
        })
    }

    fn decode_merge_target(
        &self,
        target: &dto::IcebergOldDeleteMergeTarget,
        path: FieldPath,
    ) -> Result<IcebergOldDeleteMergeTarget, ConnectorWriteCodecError> {
        let mut references = Vec::with_capacity(target.references.len());
        for (index, reference) in target.references.iter().enumerate() {
            references
                .push(self.decode_reference(reference, path.field("references").index(index))?);
        }
        // `try_new` owns the target-wide rules — a reference that belongs to
        // another data file, a repeated artifact, a partition spec that
        // disagrees with the data file's — none of which the carrier's shape
        // can express.
        IcebergOldDeleteMergeTarget::try_new(
            target.data_file_path.clone(),
            target.data_file_record_count,
            target.data_file_sequence_number,
            self.decode_partition(target.partition.as_ref(), path.field("partition"))?,
            target.base_snapshot_id,
            references,
        )
        .map_err(|error| self.rejected(path, &error))
    }

    fn encode_writer_handle_value(
        &self,
        handle: &IcebergWriterHandle,
    ) -> Result<dto::ConnectorWriterHandle, ConnectorWriteCodecError> {
        let path = FieldPath::root("writer_handle").field("iceberg");
        let mut old_deletes = std::collections::BTreeMap::new();
        for (key, target) in handle.old_deletes() {
            old_deletes.insert(
                key.clone(),
                self.encode_merge_target(target, path.field("old_deletes").map_key(key.clone()))?,
            );
        }
        let data = handle
            .data()
            .map(|recipe| self.encode_recipe(recipe, path.field("data")))
            .transpose()?;
        Ok(dto::ConnectorWriterHandle {
            handle: Some(dto::connector_writer_handle::Handle::Iceberg(
                dto::IcebergWriterHandle {
                    branch: self.encode_branch(handle.branch()),
                    table: Some(self.encode_table(handle.table())),
                    output: Some(self.encode_output(handle.output(), path.field("output"))?),
                    data,
                    old_deletes,
                },
            )),
        })
    }

    fn decode_writer_handle_value(
        &self,
        handle: &ValidatedWriterHandle,
    ) -> Result<IcebergWriterHandle, ConnectorWriteCodecError> {
        let path = FieldPath::root("writer_handle").field("iceberg");
        let iceberg = handle.iceberg();
        let branch = self.decode_branch(iceberg.branch, path.field("branch"))?;
        let table = self.decode_table(iceberg.table.as_ref(), path.field("table"))?;
        let output = self.decode_output(iceberg.output.as_ref(), path.field("output"))?;
        match branch {
            IcebergWriteBranch::Data => {
                let recipe = self.decode_recipe(iceberg.data.as_ref(), path.field("data"))?;
                // `try_new_data` owns "a data writer produces Parquet".
                IcebergWriterHandle::try_new_data(table, output, recipe)
                    .map_err(|error| self.rejected(path, &error))
            }
            IcebergWriteBranch::PositionDelete | IcebergWriteBranch::DeletionVector => {
                let mut targets = Vec::with_capacity(iceberg.old_deletes.len());
                for (key, target) in &iceberg.old_deletes {
                    targets.push(self.decode_merge_target(
                        target,
                        path.field("old_deletes").map_key(key.clone()),
                    )?);
                }
                // `try_new_delete` owns the branch/format pairing, the frozen
                // base snapshot every target must agree with, and the exclusive
                // ownership of each referenced data file.
                IcebergWriterHandle::try_new_delete(branch, table, output, targets)
                    .map_err(|error| self.rejected(path, &error))
            }
        }
    }

    // -- commit fragment ---------------------------------------------------

    fn encode_commit_fragment_value(
        &self,
        fragment: &IcebergCommitFragment,
    ) -> Result<dto::ConnectorCommitFragment, ConnectorWriteCodecError> {
        let path = FieldPath::root("commit_fragment").field("iceberg");
        let artifact = match fragment.artifact() {
            IcebergCommitArtifact::DataFile(file) => {
                let path = path.field("data_file");
                dto::iceberg_commit_fragment::Artifact::DataFile(dto::IcebergDataFileArtifact {
                    path: file.path().to_string(),
                    file_format: self
                        .encode_file_format(file.file_format(), path.field("file_format"))?,
                    partition: Some(self.encode_partition(file.partition())),
                    metrics: Some(self.encode_metrics(file.metrics())),
                    first_row_id: file.first_row_id(),
                })
            }
            IcebergCommitArtifact::PositionDeleteFile(file) => {
                dto::iceberg_commit_fragment::Artifact::PositionDeleteFile(
                    dto::IcebergPositionDeleteFileArtifact {
                        path: file.path().to_string(),
                        partition: Some(self.encode_partition(file.partition())),
                        metrics: Some(self.encode_metrics(file.metrics())),
                        referenced_data_file: file.referenced_data_file().to_string(),
                        merged_old_references: file.merged_old_references().to_vec(),
                    },
                )
            }
            IcebergCommitArtifact::DeletionVector(file) => {
                dto::iceberg_commit_fragment::Artifact::DeletionVector(
                    dto::IcebergDeletionVectorArtifact {
                        path: file.path().to_string(),
                        partition: Some(self.encode_partition(file.partition())),
                        metrics: Some(self.encode_metrics(file.metrics())),
                        referenced_data_file: file.referenced_data_file().to_string(),
                        content_range: Some(self.encode_content_range(file.content_range())),
                        cardinality: file.cardinality(),
                        merged_old_references: file.merged_old_references().to_vec(),
                    },
                )
            }
        };
        Ok(dto::ConnectorCommitFragment {
            fragment: Some(dto::connector_commit_fragment::Fragment::Iceberg(
                dto::IcebergCommitFragment {
                    artifact: Some(artifact),
                },
            )),
        })
    }

    fn decode_commit_fragment_value(
        &self,
        fragment: &ValidatedCommitFragment,
    ) -> Result<IcebergCommitFragment, ConnectorWriteCodecError> {
        let path = FieldPath::root("commit_fragment").field("iceberg");
        let artifact = fragment.iceberg().artifact.as_ref().ok_or_else(|| {
            self.missing(
                path.clone(),
                "an Iceberg commit fragment describes exactly one artifact",
            )
        })?;
        match artifact {
            dto::iceberg_commit_fragment::Artifact::DataFile(file) => {
                let path = path.field("data_file");
                let artifact = IcebergDataFileArtifact::try_new(
                    file.path.clone(),
                    self.decode_file_format(file.file_format, path.field("file_format"))?,
                    self.decode_partition(file.partition.as_ref(), path.field("partition"))?,
                    self.decode_metrics(file.metrics.as_ref(), path.field("metrics"))?,
                    file.first_row_id,
                )
                .map_err(|error| self.rejected(path, &error))?;
                Ok(IcebergCommitFragment::data_file(artifact))
            }
            dto::iceberg_commit_fragment::Artifact::PositionDeleteFile(file) => {
                let path = path.field("position_delete_file");
                let artifact = IcebergPositionDeleteFileArtifact::try_new(
                    file.path.clone(),
                    self.decode_partition(file.partition.as_ref(), path.field("partition"))?,
                    self.decode_metrics(file.metrics.as_ref(), path.field("metrics"))?,
                    file.referenced_data_file.clone(),
                    file.merged_old_references.clone(),
                )
                .map_err(|error| self.rejected(path, &error))?;
                Ok(IcebergCommitFragment::position_delete_file(artifact))
            }
            dto::iceberg_commit_fragment::Artifact::DeletionVector(file) => {
                let path = path.field("deletion_vector");
                let content_range = file.content_range.as_ref().ok_or_else(|| {
                    self.missing(
                        path.field("content_range"),
                        "an Iceberg deletion vector requires its blob range",
                    )
                })?;
                // `try_new` owns the facts only Iceberg can check: the blob must
                // fit inside its own Puffin file, and the cardinality must be
                // the record count it claims.
                let artifact = IcebergDeletionVectorArtifact::try_new(
                    file.path.clone(),
                    self.decode_partition(file.partition.as_ref(), path.field("partition"))?,
                    self.decode_metrics(file.metrics.as_ref(), path.field("metrics"))?,
                    file.referenced_data_file.clone(),
                    self.decode_content_range(content_range, path.field("content_range"))?,
                    file.cardinality,
                    file.merged_old_references.clone(),
                )
                .map_err(|error| self.rejected(path, &error))?;
                Ok(IcebergCommitFragment::deletion_vector(artifact))
            }
        }
    }
}

/// FE half: one Iceberg write recipe becomes its carrier.
pub(crate) struct IcebergWriteHandleEncoder(IcebergWriteCodec);

impl IcebergWriteHandleEncoder {
    pub(crate) fn new(adapter: IcebergWriteAdapter) -> Self {
        Self(IcebergWriteCodec::new(adapter))
    }
}

impl ConnectorWriteHandleEncoder for IcebergWriteHandleEncoder {
    fn owner(&self) -> &str {
        &self.0.owner
    }

    fn encode_writer_handle(
        &self,
        handle: &ConnectorWriterHandle,
    ) -> Result<dto::ConnectorWriterHandle, ConnectorWriteCodecError> {
        // The adapter is the only door to the domain value, and it is bound to
        // this exact generation: a handle another generation minted cannot be
        // encoded here, so a frontend cannot launder a foreign recipe onto the
        // wire under this catalog's name.
        let handle = self
            .0
            .adapter
            .writer_handle(handle)
            .map_err(|error| self.0.rejected(FieldPath::root("writer_handle"), &error))?;
        self.0.encode_writer_handle_value(handle)
    }
}

/// BE half: a validated carrier becomes an Iceberg write recipe again.
pub(crate) struct IcebergWriteHandleDecoder(IcebergWriteCodec);

impl IcebergWriteHandleDecoder {
    pub(crate) fn new(adapter: IcebergWriteAdapter) -> Self {
        Self(IcebergWriteCodec::new(adapter))
    }
}

impl ConnectorWriteHandleDecoder for IcebergWriteHandleDecoder {
    fn owner(&self) -> &str {
        &self.0.owner
    }

    fn decode_writer_handle(
        &self,
        handle: &ValidatedWriterHandle,
    ) -> Result<ConnectorWriterHandle, ConnectorWriteCodecError> {
        let value = self.0.decode_writer_handle_value(handle)?;
        // The result is rewrapped with this decoder's own binding, so only this
        // generation's writer factory can open a writer for it.
        Ok(self.0.adapter.wrap_writer_handle(value))
    }
}

/// BE half: one staged Iceberg artifact becomes its carrier.
pub(crate) struct IcebergWriteFragmentEncoder(IcebergWriteCodec);

impl IcebergWriteFragmentEncoder {
    pub(crate) fn new(adapter: IcebergWriteAdapter) -> Self {
        Self(IcebergWriteCodec::new(adapter))
    }
}

impl ConnectorWriteFragmentEncoder for IcebergWriteFragmentEncoder {
    fn owner(&self) -> &str {
        &self.0.owner
    }

    fn encode_commit_fragment(
        &self,
        fragment: &ConnectorCommitFragment,
    ) -> Result<dto::ConnectorCommitFragment, ConnectorWriteCodecError> {
        let fragment = self
            .0
            .adapter
            .commit_fragment(fragment)
            .map_err(|error| self.0.rejected(FieldPath::root("commit_fragment"), &error))?;
        self.0.encode_commit_fragment_value(fragment)
    }
}

/// FE half: a validated carrier becomes an Iceberg artifact again.
pub(crate) struct IcebergWriteFragmentDecoder(IcebergWriteCodec);

impl IcebergWriteFragmentDecoder {
    pub(crate) fn new(adapter: IcebergWriteAdapter) -> Self {
        Self(IcebergWriteCodec::new(adapter))
    }
}

impl ConnectorWriteFragmentDecoder for IcebergWriteFragmentDecoder {
    fn owner(&self) -> &str {
        &self.0.owner
    }

    fn decode_commit_fragment(
        &self,
        fragment: &ValidatedCommitFragment,
    ) -> Result<ConnectorCommitFragment, ConnectorWriteCodecError> {
        let value = self.0.decode_commit_fragment_value(fragment)?;
        Ok(self.0.adapter.wrap_commit_fragment(value))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use novarocks_proto_codec::ProtocolErrorKind;
    use novarocks_proto_codec::connector_write::{
        MAX_COMMIT_FRAGMENT_ENCODED_BYTES, MAX_WRITER_HANDLE_ENCODED_BYTES,
    };
    use novarocks_spi::connector::{
        CatalogHandle, CatalogVersion, ConnectorInstanceDescriptor, ConnectorInstanceId,
        ConnectorProviderId,
    };

    use crate::commit::write_stack::runtime::build_write_adapter;
    use crate::commit::write_stack::test_support::{
        merge_target, parquet_ref, puffin_ref, sample_metrics, sample_partition, table_facts,
    };
    use crate::scan_model::IcebergSchemaFieldDef;

    fn adapter(catalog: &str, version: u8) -> IcebergWriteAdapter {
        let instance_id = ConnectorInstanceId::parse(catalog).expect("instance id");
        build_write_adapter(
            ConnectorInstanceDescriptor {
                provider_id: ConnectorProviderId::parse(crate::PROVIDER_ID).expect("provider id"),
                instance_id: instance_id.clone(),
            },
            CatalogHandle::new(instance_id, CatalogVersion::from_bytes([version; 32])),
        )
    }

    struct Facets {
        handle_encoder: IcebergWriteHandleEncoder,
        handle_decoder: IcebergWriteHandleDecoder,
        fragment_encoder: IcebergWriteFragmentEncoder,
        fragment_decoder: IcebergWriteFragmentDecoder,
        adapter: IcebergWriteAdapter,
    }

    fn facets(catalog: &str, version: u8) -> Facets {
        let adapter = adapter(catalog, version);
        Facets {
            handle_encoder: IcebergWriteHandleEncoder::new(adapter.clone()),
            handle_decoder: IcebergWriteHandleDecoder::new(adapter.clone()),
            fragment_encoder: IcebergWriteFragmentEncoder::new(adapter.clone()),
            fragment_decoder: IcebergWriteFragmentDecoder::new(adapter.clone()),
            adapter,
        }
    }

    fn generation() -> Facets {
        facets("catalog.iceberg", 1)
    }

    fn output(format: IcebergFileFormat) -> IcebergWriterOutput {
        IcebergWriterOutput::try_new(format, Compression::SNAPPY, None).expect("output")
    }

    fn schema() -> IcebergSchemaDef {
        IcebergSchemaDef {
            fields: vec![IcebergSchemaFieldDef {
                field_id: 1,
                name: "k1".to_string(),
                initial_default: None,
                write_default: None,
                initial_default_json: Some("7".to_string()),
                write_default_json: None,
                children: Vec::new(),
            }],
        }
    }

    fn data_handle() -> IcebergWriterHandle {
        IcebergWriterHandle::try_new_data(
            table_facts(),
            IcebergWriterOutput::try_new(
                IcebergFileFormat::Parquet,
                Compression::SNAPPY,
                Some(4096),
            )
            .expect("output"),
            IcebergDataBranchRecipe::try_new(
                Some(schema()),
                vec!["d".to_string()],
                vec!["d_day".to_string()],
                vec!["day(d)".to_string()],
                true,
            )
            .expect("recipe"),
        )
        .expect("data handle")
    }

    /// Two merge targets, each carrying two frozen references, so the map and
    /// the per-target reference vectors both have to survive the round trip.
    fn delete_handle(branch: IcebergWriteBranch) -> IcebergWriterHandle {
        let format = match branch {
            IcebergWriteBranch::DeletionVector => IcebergFileFormat::Puffin,
            _ => IcebergFileFormat::Parquet,
        };
        let first = merge_target(
            "s3://b/wh/db/t/data/a.parquet",
            100,
            vec![
                puffin_ref(
                    "s3://b/wh/db/t/data/a-dv-1.puffin",
                    Some("s3://b/wh/db/t/data/a.parquet"),
                    4096,
                    3,
                    0,
                    64,
                )
                .expect("reference"),
                parquet_ref("s3://b/wh/db/t/data/shared-1.parquet", None, 4096, 9)
                    .expect("reference"),
            ],
        );
        let second = merge_target(
            "s3://b/wh/db/t/data/b.parquet",
            200,
            vec![
                puffin_ref(
                    "s3://b/wh/db/t/data/b-dv-1.puffin",
                    Some("s3://b/wh/db/t/data/b.parquet"),
                    8192,
                    5,
                    16,
                    128,
                )
                .expect("reference"),
                parquet_ref("s3://b/wh/db/t/data/shared-2.parquet", None, 4096, 0)
                    .expect("reference"),
            ],
        );
        IcebergWriterHandle::try_new_delete(
            branch,
            table_facts(),
            output(format),
            vec![first, second],
        )
        .expect("delete handle")
    }

    fn data_file_fragment() -> IcebergCommitFragment {
        let mut stats = IcebergColumnStats::default();
        stats.column_sizes.insert(1, 128);
        stats.value_counts.insert(1, 10);
        stats.null_value_counts.insert(1, 0);
        stats.lower_bounds.insert(1, vec![0_u8]);
        stats.upper_bounds.insert(1, vec![9_u8]);
        IcebergCommitFragment::data_file(
            IcebergDataFileArtifact::try_new(
                "s3://b/wh/db/t/data/new.parquet".to_string(),
                IcebergFileFormat::Parquet,
                sample_partition(),
                IcebergArtifactMetrics::try_new(10, 4096, vec![0, 2048], Some(stats))
                    .expect("metrics"),
                Some(100),
            )
            .expect("data file"),
        )
    }

    fn position_delete_fragment() -> IcebergCommitFragment {
        IcebergCommitFragment::position_delete_file(
            IcebergPositionDeleteFileArtifact::try_new(
                "s3://b/wh/db/t/data/new-pos.parquet".to_string(),
                sample_partition(),
                sample_metrics(4, 2048),
                "s3://b/wh/db/t/data/a.parquet".to_string(),
                vec![
                    "s3://b/wh/db/t/data/old-1.parquet".to_string(),
                    "s3://b/wh/db/t/data/old-2.parquet".to_string(),
                ],
            )
            .expect("position delete file"),
        )
    }

    fn deletion_vector_fragment() -> IcebergCommitFragment {
        IcebergCommitFragment::deletion_vector(
            IcebergDeletionVectorArtifact::try_new(
                "s3://b/wh/db/t/data/new.puffin".to_string(),
                sample_partition(),
                sample_metrics(3, 4096),
                "s3://b/wh/db/t/data/a.parquet".to_string(),
                IcebergContentRange::try_new(4, 64).expect("range"),
                3,
                vec!["s3://b/wh/db/t/data/a-dv-1.puffin".to_string()],
            )
            .expect("deletion vector"),
        )
    }

    fn parse_handle(raw: dto::ConnectorWriterHandle) -> ValidatedWriterHandle {
        ValidatedWriterHandle::parse(raw, FieldPath::root("writer_handle"))
            .expect("the encoder produces a structurally valid carrier")
    }

    fn parse_fragment(raw: dto::ConnectorCommitFragment) -> ValidatedCommitFragment {
        ValidatedCommitFragment::parse(raw, FieldPath::root("commit_fragment"))
            .expect("the encoder produces a structurally valid carrier")
    }

    fn assert_same_handle(left: &IcebergWriterHandle, right: &IcebergWriterHandle) {
        assert_eq!(left.branch(), right.branch());
        assert_eq!(left.table(), right.table());
        assert_eq!(left.output().file_format(), right.output().file_format());
        assert_eq!(left.output().compression(), right.output().compression());
        assert_eq!(
            left.output().parquet_row_group_size_bytes(),
            right.output().parquet_row_group_size_bytes()
        );
        match (left.data(), right.data()) {
            (None, None) => {}
            (Some(left), Some(right)) => {
                assert_eq!(left.input_schema(), right.input_schema());
                assert_eq!(
                    left.partition_source_column_names(),
                    right.partition_source_column_names()
                );
                assert_eq!(
                    left.partition_column_names(),
                    right.partition_column_names()
                );
                assert_eq!(left.transform_exprs(), right.transform_exprs());
                assert_eq!(left.row_lineage(), right.row_lineage());
            }
            _ => panic!("one handle has a data recipe and the other does not"),
        }
        assert_eq!(left.old_deletes(), right.old_deletes());
    }

    fn assert_same_fragment(left: &IcebergCommitFragment, right: &IcebergCommitFragment) {
        assert_eq!(left.path(), right.path());
        assert_eq!(left.partition(), right.partition());
        assert_eq!(left.metrics(), right.metrics());
        assert_eq!(left.referenced_data_file(), right.referenced_data_file());
        assert_eq!(left.merged_old_references(), right.merged_old_references());
        match (left.artifact(), right.artifact()) {
            (IcebergCommitArtifact::DataFile(left), IcebergCommitArtifact::DataFile(right)) => {
                assert_eq!(left.file_format(), right.file_format());
                assert_eq!(left.first_row_id(), right.first_row_id());
            }
            (
                IcebergCommitArtifact::PositionDeleteFile(_),
                IcebergCommitArtifact::PositionDeleteFile(_),
            ) => {}
            (
                IcebergCommitArtifact::DeletionVector(left),
                IcebergCommitArtifact::DeletionVector(right),
            ) => {
                assert_eq!(left.content_range(), right.content_range());
                assert_eq!(left.cardinality(), right.cardinality());
            }
            _ => panic!("the recovered fragment describes another artifact kind"),
        }
    }

    /// Encode, validate, decode, and prove the recovered value is the original.
    ///
    /// The re-encoding check is the backstop: the field-by-field comparison can
    /// only miss a field the encoder still carries, and comparing the carriers
    /// catches exactly that.
    fn round_trip_handle(facets: &Facets, handle: &IcebergWriterHandle) -> IcebergWriterHandle {
        let neutral = facets.adapter.wrap_writer_handle(handle.clone());
        let raw = facets
            .handle_encoder
            .encode_writer_handle(&neutral)
            .expect("encode writer handle");
        let decoded = facets
            .handle_decoder
            .decode_writer_handle(&parse_handle(raw.clone()))
            .expect("decode writer handle");
        let recovered = facets
            .adapter
            .writer_handle(&decoded)
            .expect("the decoded handle belongs to this generation")
            .clone();
        assert_same_handle(handle, &recovered);
        assert_eq!(
            facets
                .handle_encoder
                .encode_writer_handle(&decoded)
                .expect("re-encode"),
            raw
        );
        recovered
    }

    fn round_trip_fragment(
        facets: &Facets,
        fragment: &IcebergCommitFragment,
    ) -> IcebergCommitFragment {
        let neutral = facets.adapter.wrap_commit_fragment(fragment.clone());
        let raw = facets
            .fragment_encoder
            .encode_commit_fragment(&neutral)
            .expect("encode commit fragment");
        let decoded = facets
            .fragment_decoder
            .decode_commit_fragment(&parse_fragment(raw.clone()))
            .expect("decode commit fragment");
        let recovered = facets
            .adapter
            .commit_fragment(&decoded)
            .expect("the decoded fragment belongs to this generation")
            .clone();
        assert_same_fragment(fragment, &recovered);
        assert_eq!(
            facets
                .fragment_encoder
                .encode_commit_fragment(&decoded)
                .expect("re-encode"),
            raw
        );
        recovered
    }

    #[test]
    fn every_facet_names_its_own_generation_as_the_owner() {
        let facets = generation();
        assert_eq!(facets.handle_encoder.owner(), "catalog.iceberg");
        assert_eq!(facets.handle_decoder.owner(), "catalog.iceberg");
        assert_eq!(facets.fragment_encoder.owner(), "catalog.iceberg");
        assert_eq!(facets.fragment_decoder.owner(), "catalog.iceberg");
    }

    #[test]
    fn a_data_branch_handle_round_trips_through_its_carrier() {
        let facets = generation();
        let recovered = round_trip_handle(&facets, &data_handle());
        let recipe = recovered.data().expect("a data branch keeps its recipe");
        assert_eq!(recipe.input_schema(), Some(&schema()));
        assert!(recipe.row_lineage());
        assert_eq!(
            recovered.output().parquet_row_group_size_bytes(),
            Some(4096)
        );
    }

    #[test]
    fn both_delete_branches_round_trip_with_every_frozen_reference() {
        let facets = generation();
        for branch in [
            IcebergWriteBranch::PositionDelete,
            IcebergWriteBranch::DeletionVector,
        ] {
            let handle = delete_handle(branch);
            let recovered = round_trip_handle(&facets, &handle);
            assert_eq!(recovered.old_deletes().len(), 2);
            for target in recovered.old_deletes().values() {
                assert_eq!(target.references().len(), 2);
            }
            // A reference whose frozen manifest projection carried no record
            // count must come back as unknown, not as a claimed count of zero.
            let unknown = recovered
                .old_deletes()
                .get("s3://b/wh/db/t/data/b.parquet")
                .expect("second target")
                .references()
                .iter()
                .find(|reference| reference.path().ends_with("shared-2.parquet"))
                .expect("shared reference");
            assert_eq!(unknown.record_count(), None);
        }
    }

    #[test]
    fn every_artifact_kind_round_trips_through_its_carrier() {
        let facets = generation();
        for fragment in [
            data_file_fragment(),
            position_delete_fragment(),
            deletion_vector_fragment(),
        ] {
            round_trip_fragment(&facets, &fragment);
        }
    }

    #[test]
    fn a_value_from_another_generation_cannot_be_encoded() {
        let mine = generation();
        let theirs = facets("catalog.iceberg", 2);

        let handle = theirs.adapter.wrap_writer_handle(data_handle());
        let error = mine
            .handle_encoder
            .encode_writer_handle(&handle)
            .expect_err("a foreign generation's handle");
        assert_eq!(error.owner(), "catalog.iceberg");
        assert_eq!(error.protocol().path().to_string(), "writer_handle");
        assert!(
            error
                .protocol()
                .detail()
                .contains("does not belong to this exact provider generation")
        );

        let fragment = theirs.adapter.wrap_commit_fragment(data_file_fragment());
        let error = mine
            .fragment_encoder
            .encode_commit_fragment(&fragment)
            .expect_err("a foreign generation's fragment");
        assert_eq!(error.protocol().path().to_string(), "commit_fragment");
    }

    #[test]
    fn a_decoded_value_belongs_only_to_the_generation_that_decoded_it() {
        let mine = generation();
        let theirs = facets("catalog.iceberg", 2);
        let raw = mine
            .handle_encoder
            .encode_writer_handle(&mine.adapter.wrap_writer_handle(data_handle()))
            .expect("encode");
        let decoded = theirs
            .handle_decoder
            .decode_writer_handle(&parse_handle(raw))
            .expect("another generation may decode the same canonical carrier");

        assert!(theirs.adapter.writer_handle(&decoded).is_ok());
        assert!(mine.adapter.writer_handle(&decoded).is_err());
    }

    #[test]
    fn a_structurally_valid_carrier_still_faces_the_domain_constructors() {
        let facets = generation();

        // A Puffin writer carrying a Parquet row-group size: the carrier can
        // state both, and only `IcebergWriterOutput::try_new` knows the pairing
        // is a contradiction.
        let mut raw = facets
            .handle_encoder
            .encode_writer_handle(
                &facets
                    .adapter
                    .wrap_writer_handle(delete_handle(IcebergWriteBranch::DeletionVector)),
            )
            .expect("encode");
        let dto::connector_writer_handle::Handle::Iceberg(iceberg) =
            raw.handle.as_mut().expect("variant");
        iceberg
            .output
            .as_mut()
            .expect("output")
            .parquet_row_group_size_bytes = Some(4096);
        let error = facets
            .handle_decoder
            .decode_writer_handle(&parse_handle(raw))
            .expect_err("a Puffin writer with a Parquet row group size");
        assert_eq!(error.protocol().kind(), ProtocolErrorKind::InvalidValue);
        assert_eq!(
            error.protocol().path().to_string(),
            "writer_handle.iceberg.output"
        );
        assert!(
            error
                .protocol()
                .detail()
                .contains("Iceberg Parquet row group size")
        );

        // A merge target frozen against a snapshot the session is not based on.
        let mut raw = facets
            .handle_encoder
            .encode_writer_handle(
                &facets
                    .adapter
                    .wrap_writer_handle(delete_handle(IcebergWriteBranch::PositionDelete)),
            )
            .expect("encode");
        let dto::connector_writer_handle::Handle::Iceberg(iceberg) =
            raw.handle.as_mut().expect("variant");
        iceberg
            .old_deletes
            .get_mut("s3://b/wh/db/t/data/a.parquet")
            .expect("target")
            .base_snapshot_id = 78;
        let error = facets
            .handle_decoder
            .decode_writer_handle(&parse_handle(raw))
            .expect_err("a target frozen against another snapshot");
        assert_eq!(error.protocol().path().to_string(), "writer_handle.iceberg");
        assert!(
            error
                .protocol()
                .detail()
                .contains("does not name the session's frozen base snapshot")
        );

        // A deletion vector whose cardinality disagrees with its record count.
        let mut raw = facets
            .fragment_encoder
            .encode_commit_fragment(
                &facets
                    .adapter
                    .wrap_commit_fragment(deletion_vector_fragment()),
            )
            .expect("encode");
        let dto::connector_commit_fragment::Fragment::Iceberg(iceberg) =
            raw.fragment.as_mut().expect("variant");
        let Some(dto::iceberg_commit_fragment::Artifact::DeletionVector(vector)) =
            iceberg.artifact.as_mut()
        else {
            unreachable!("deletion vector fixture")
        };
        vector.cardinality = 2;
        let error = facets
            .fragment_decoder
            .decode_commit_fragment(&parse_fragment(raw))
            .expect_err("a deletion vector that disagrees with itself");
        assert_eq!(error.protocol().kind(), ProtocolErrorKind::Conflict);
        assert_eq!(
            error.protocol().path().to_string(),
            "commit_fragment.iceberg.deletion_vector"
        );
        assert!(
            error
                .protocol()
                .detail()
                .contains("cardinality differs from its record count")
        );
    }

    #[test]
    fn a_compression_the_carrier_cannot_express_is_refused_rather_than_narrowed() {
        let facets = generation();
        let handle = IcebergWriterHandle::try_new_data(
            table_facts(),
            IcebergWriterOutput::try_new(
                IcebergFileFormat::Parquet,
                Compression::GZIP(GzipLevel::try_new(9).expect("level")),
                None,
            )
            .expect("output"),
            IcebergDataBranchRecipe::try_new(None, Vec::new(), Vec::new(), Vec::new(), false)
                .expect("recipe"),
        )
        .expect("handle");
        let error = facets
            .handle_encoder
            .encode_writer_handle(&facets.adapter.wrap_writer_handle(handle))
            .expect_err("a non-default gzip level");
        assert_eq!(
            error.protocol().path().to_string(),
            "writer_handle.iceberg.output.compression"
        );
        assert!(error.protocol().detail().contains("cannot express"));
    }

    #[test]
    fn an_oversized_handle_or_fragment_is_refused_by_the_codec_layer() {
        let facets = generation();

        // One path past the 16 MiB writer-handle bound. A bare path keeps the
        // fixture cheap: it is a location the domain accepts and nothing else.
        let huge = format!("/wh/db/t/data/{}.parquet", "x".repeat(17 * 1024 * 1024));
        let handle = IcebergWriterHandle::try_new_delete(
            IcebergWriteBranch::PositionDelete,
            table_facts(),
            output(IcebergFileFormat::Parquet),
            vec![merge_target(&huge, 10, Vec::new())],
        )
        .expect("handle");
        let neutral = facets.adapter.wrap_writer_handle(handle);
        let bytes = facets
            .handle_encoder
            .canonical_writer_handle_bytes(&neutral)
            .expect("encode");
        assert!(bytes.len() > MAX_WRITER_HANDLE_ENCODED_BYTES);
        let raw = facets
            .handle_encoder
            .encode_writer_handle(&neutral)
            .expect("encode");
        let error = ValidatedWriterHandle::parse(raw, FieldPath::root("writer_handle"))
            .expect_err("an oversized writer handle");
        assert_eq!(error.kind(), ProtocolErrorKind::OutOfRange);
        assert_eq!(error.path().to_string(), "writer_handle");

        // One path past the 1 MiB commit-fragment bound.
        let huge = format!("/wh/db/t/data/{}.parquet", "x".repeat(1024 * 1024 + 16));
        let fragment = IcebergCommitFragment::data_file(
            IcebergDataFileArtifact::try_new(
                huge,
                IcebergFileFormat::Parquet,
                sample_partition(),
                sample_metrics(1, 4096),
                None,
            )
            .expect("data file"),
        );
        let neutral = facets.adapter.wrap_commit_fragment(fragment);
        let bytes = facets
            .fragment_encoder
            .canonical_commit_fragment_bytes(&neutral)
            .expect("encode");
        assert!(bytes.len() > MAX_COMMIT_FRAGMENT_ENCODED_BYTES);
        let raw = facets
            .fragment_encoder
            .encode_commit_fragment(&neutral)
            .expect("encode");
        let error = ValidatedCommitFragment::parse(raw, FieldPath::root("commit_fragment"))
            .expect_err("an oversized commit fragment");
        assert_eq!(error.kind(), ProtocolErrorKind::OutOfRange);
        assert_eq!(error.path().to_string(), "commit_fragment");
    }
}
