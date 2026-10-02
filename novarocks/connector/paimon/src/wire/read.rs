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

use std::collections::BTreeSet;
use std::mem::size_of;

use bytes::Bytes;
use novarocks_spi::connector::read_stack::{
    ConnectorReadSplitFacts, ConnectorSplit, ConnectorTableHandle, SchemaTableName,
};
use novarocks_spi::connector::{
    ConnectorCodecError, ConnectorCodecErrorKind, ConnectorDecodeContext, ConnectorFieldPath,
    ConnectorPrivateDecoder, ConnectorPrivateEncoder,
};
use prost::Message;

use crate::domain::{
    PaimonBinaryTableStats, PaimonBucketMode, PaimonColumn, PaimonDataCompression, PaimonDataFile,
    PaimonDataFileFacts, PaimonDeletionFile, PaimonMergeEngine, PaimonReadView, PaimonRowRange,
    PaimonSplit, PaimonTable,
};
use crate::schema::PaimonDataType;

use super::dto;

const MAX_PRIVATE_READ_BYTES: usize = 16 * 1024 * 1024;

struct StrictSchema {
    name: &'static str,
    fields: &'static [(u32, u8)],
    repeated: &'static [u32],
    children: &'static [(u32, &'static StrictSchema)],
}

static OPTIONAL_I64_SCHEMA: StrictSchema = StrictSchema {
    name: "paimon_read_split.file.stats.null_count",
    fields: &[(1, 0)],
    repeated: &[],
    children: &[],
};
static STATS_SCHEMA: StrictSchema = StrictSchema {
    name: "paimon_read_split.file.stats",
    fields: &[(1, 2), (2, 2), (3, 2)],
    repeated: &[3],
    children: &[(3, &OPTIONAL_I64_SCHEMA)],
};
static STRING_LIST_SCHEMA: StrictSchema = StrictSchema {
    name: "paimon_read_split.file.string_list",
    fields: &[(1, 2)],
    repeated: &[1],
    children: &[],
};
static FILE_SCHEMA: StrictSchema = StrictSchema {
    name: "paimon_read_split.file",
    fields: &[
        (1, 2),
        (2, 0),
        (3, 0),
        (4, 0),
        (5, 0),
        (6, 0),
        (7, 0),
        (8, 0),
        (9, 2),
        (10, 2),
        (11, 2),
        (12, 2),
        (13, 2),
        (14, 0),
        (15, 0),
        (16, 2),
        (17, 0),
        (18, 2),
        (19, 2),
        (20, 0),
        (21, 2),
    ],
    repeated: &[13],
    children: &[
        (11, &STATS_SCHEMA),
        (12, &STATS_SCHEMA),
        (18, &STRING_LIST_SCHEMA),
        (21, &STRING_LIST_SCHEMA),
    ],
};
static DELETION_SCHEMA: StrictSchema = StrictSchema {
    name: "paimon_read_split.deletion_file",
    fields: &[(1, 2), (2, 0), (3, 0), (4, 0)],
    repeated: &[],
    children: &[],
};
static OPTIONAL_DELETION_SCHEMA: StrictSchema = StrictSchema {
    name: "paimon_read_split.optional_deletion_file",
    fields: &[(1, 2)],
    repeated: &[],
    children: &[(1, &DELETION_SCHEMA)],
};
static DELETION_LIST_SCHEMA: StrictSchema = StrictSchema {
    name: "paimon_read_split.deletion_files",
    fields: &[(1, 2)],
    repeated: &[1],
    children: &[(1, &OPTIONAL_DELETION_SCHEMA)],
};
static ROW_RANGE_SCHEMA: StrictSchema = StrictSchema {
    name: "paimon_read_split.row_range",
    fields: &[(1, 0), (2, 0)],
    repeated: &[],
    children: &[],
};
static ROW_RANGE_LIST_SCHEMA: StrictSchema = StrictSchema {
    name: "paimon_read_split.row_ranges",
    fields: &[(1, 2)],
    repeated: &[1],
    children: &[(1, &ROW_RANGE_SCHEMA)],
};
static SPLIT_SCHEMA: StrictSchema = StrictSchema {
    name: "paimon_read_split",
    fields: &[
        (1, 0),
        (2, 0),
        (3, 2),
        (4, 0),
        (5, 2),
        (6, 0),
        (7, 0),
        (8, 2),
        (9, 0),
        (10, 2),
        (11, 2),
        (12, 0),
    ],
    repeated: &[5],
    children: &[
        (5, &FILE_SCHEMA),
        (10, &DELETION_LIST_SCHEMA),
        (11, &ROW_RANGE_LIST_SCHEMA),
    ],
};
static DATA_TYPE_SCHEMA: StrictSchema = StrictSchema {
    name: "paimon_read_column.data_type",
    fields: &[(1, 0), (2, 0), (3, 0), (4, 0)],
    repeated: &[],
    children: &[],
};
static COLUMN_SCHEMA: StrictSchema = StrictSchema {
    name: "paimon_read_column",
    fields: &[(1, 0), (2, 2), (3, 2), (4, 0), (5, 0)],
    repeated: &[],
    children: &[(3, &DATA_TYPE_SCHEMA)],
};

#[derive(Clone, Copy, Debug, Default)]
pub struct PaimonReadWireCodec;

impl ConnectorPrivateEncoder<PaimonTable> for PaimonReadWireCodec {
    fn encode_private(&self, value: &PaimonTable) -> Result<Bytes, ConnectorCodecError> {
        let name = value.schema_table_name();
        Ok(Bytes::from(
            dto::PaimonTablePayload {
                schema_name: name.schema_name().to_string(),
                table_name: name.table_name().to_string(),
                table_location: value.location().to_string(),
                merge_engine: encode_merge(value.merge_engine()) as i32,
                bucket_mode: encode_bucket(value.bucket_mode()) as i32,
                primary_key_field_ids: value.primary_key_field_ids().to_vec(),
                partition_field_ids: value.partition_field_ids().to_vec(),
            }
            .encode_to_vec(),
        ))
    }
}

impl ConnectorPrivateDecoder<PaimonTable> for PaimonReadWireCodec {
    fn decode_private(
        &self,
        payload: &[u8],
        context: &mut ConnectorDecodeContext<'_>,
    ) -> Result<PaimonTable, ConnectorCodecError> {
        let result = (|| {
            let raw = decode_root::<dto::PaimonTablePayload>(
                payload,
                context,
                "paimon_read_table",
                &[1, 2, 3, 4, 5],
                &[6, 7],
            )?;
            // Existing domain validation/allocation is opaque.
            context.flush_compile_control()?;
            let value = PaimonTable::try_new(
                SchemaTableName::try_new(&raw.schema_name, &raw.table_name)
                    .map_err(domain_error)?,
                &raw.table_location,
                decode_merge(raw.merge_engine)?,
                decode_bucket(raw.bucket_mode)?,
                raw.primary_key_field_ids,
                raw.partition_field_ids,
            )
            .map_err(domain_error)?;
            charge(context, payload.len(), size_of::<PaimonTable>())?;
            Ok(value)
        })();
        // The public decode attempt (including ordinary refusal) is complete.
        context.observe_compile_step()?;
        finish_decode(result, context)
    }
}

impl ConnectorPrivateEncoder<PaimonColumn> for PaimonReadWireCodec {
    fn encode_private(&self, value: &PaimonColumn) -> Result<Bytes, ConnectorCodecError> {
        Ok(Bytes::from(
            dto::PaimonColumnPayload {
                field_id: Some(value.field_id()),
                name: value.name().to_string(),
                data_type: Some(encode_type(value.data_type())),
                nullable: Some(value.nullable()),
                output_ordinal: Some(value.output_ordinal()),
            }
            .encode_to_vec(),
        ))
    }
}

impl ConnectorPrivateDecoder<PaimonColumn> for PaimonReadWireCodec {
    fn decode_private(
        &self,
        payload: &[u8],
        context: &mut ConnectorDecodeContext<'_>,
    ) -> Result<PaimonColumn, ConnectorCodecError> {
        let result = (|| {
            let raw =
                decode_strict_root::<dto::PaimonColumnPayload>(payload, context, &COLUMN_SCHEMA)?;
            let data_type = raw
                .data_type
                .as_ref()
                .ok_or_else(|| missing("paimon_read_column.data_type"))?;
            // Existing domain validation/allocation is opaque.
            context.flush_compile_control()?;
            let value = PaimonColumn::try_new(
                raw.field_id
                    .ok_or_else(|| missing("paimon_read_column.field_id"))?,
                &raw.name,
                decode_type(data_type)?,
                raw.nullable
                    .ok_or_else(|| missing("paimon_read_column.nullable"))?,
                raw.output_ordinal
                    .ok_or_else(|| missing("paimon_read_column.output_ordinal"))?,
            )
            .map_err(domain_error)?;
            charge(
                context,
                payload.len(),
                size_of::<PaimonColumn>() + raw.name.len(),
            )?;
            Ok(value)
        })();
        // The public decode attempt (including ordinary refusal) is complete.
        context.observe_compile_step()?;
        finish_decode(result, context)
    }
}

impl ConnectorPrivateEncoder<PaimonReadView> for PaimonReadWireCodec {
    fn encode_private(&self, value: &PaimonReadView) -> Result<Bytes, ConnectorCodecError> {
        Ok(Bytes::from(
            dto::PaimonReadViewPayload {
                table_location: value.table_location().to_string(),
                snapshot_id: value.snapshot_id(),
                schema_id: Some(value.schema_id()),
                schema_fingerprint: value.schema_fingerprint().to_vec(),
                read_recipe_digest: value.read_recipe_digest().to_vec(),
                sequence_field_id: value.sequence_field_id(),
            }
            .encode_to_vec(),
        ))
    }
}

impl ConnectorPrivateDecoder<PaimonReadView> for PaimonReadWireCodec {
    fn decode_private(
        &self,
        payload: &[u8],
        context: &mut ConnectorDecodeContext<'_>,
    ) -> Result<PaimonReadView, ConnectorCodecError> {
        let result = (|| {
            let raw = decode_root::<dto::PaimonReadViewPayload>(
                payload,
                context,
                "paimon_read_view",
                &[1, 2, 3, 4, 5, 6],
                &[],
            )?;
            let schema_fingerprint = fixed_32(
                &raw.schema_fingerprint,
                "paimon_read_view.schema_fingerprint",
            )?;
            let read_recipe_digest = fixed_32(
                &raw.read_recipe_digest,
                "paimon_read_view.read_recipe_digest",
            )?;
            // Existing domain validation/allocation is opaque.
            context.flush_compile_control()?;
            let value = PaimonReadView::try_new(
                &raw.table_location,
                raw.snapshot_id,
                raw.schema_id
                    .ok_or_else(|| missing("paimon_read_view.schema_id"))?,
                schema_fingerprint,
                read_recipe_digest,
                raw.sequence_field_id,
            )
            .map_err(domain_error)?;
            charge(
                context,
                payload.len(),
                size_of::<PaimonReadView>() + raw.table_location.len(),
            )?;
            Ok(value)
        })();
        // The public decode attempt (including ordinary refusal) is complete.
        context.observe_compile_step()?;
        finish_decode(result, context)
    }
}

impl ConnectorPrivateEncoder<PaimonSplit> for PaimonReadWireCodec {
    fn encode_private(&self, value: &PaimonSplit) -> Result<Bytes, ConnectorCodecError> {
        let encoded = dto::PaimonReadSplitPayload {
            snapshot_id: Some(value.snapshot_id()),
            schema_id: Some(value.schema_id()),
            partition: value.partition().to_vec(),
            bucket: Some(value.bucket()),
            files: value.files().iter().map(encode_file).collect(),
            contains_delete_rows: Some(value.contains_delete_rows()),
            partition_arity: Some(value.partition_arity()),
            bucket_path: value.bucket_path().to_string(),
            total_buckets: Some(value.total_buckets()),
            data_deletion_files: value.data_deletion_files().map(|values| {
                dto::PaimonDeletionFilesPayload {
                    values: values
                        .iter()
                        .map(|value| dto::PaimonOptionalDeletionFilePayload {
                            value: value.as_ref().map(encode_deletion_file),
                        })
                        .collect(),
                }
            }),
            row_ranges: value
                .row_ranges()
                .map(|values| dto::PaimonRowRangesPayload {
                    values: values
                        .iter()
                        .map(|value| dto::PaimonRowRangePayload {
                            from: Some(value.from()),
                            to: Some(value.to()),
                        })
                        .collect(),
                }),
            raw_convertible: Some(value.raw_convertible()),
        }
        .encode_to_vec();
        if encoded.len() > MAX_PRIVATE_READ_BYTES {
            return Err(capacity(
                "paimon_read_split",
                "Paimon private payload exceeds 16 MiB",
            ));
        }
        Ok(Bytes::from(encoded))
    }
}

impl PaimonReadWireCodec {
    pub fn decode_split_private(
        &self,
        payload: &[u8],
        facts: &ConnectorReadSplitFacts,
        context: &mut ConnectorDecodeContext<'_>,
    ) -> Result<PaimonSplit, ConnectorCodecError> {
        let result = (|| {
            if !facts.remotely_accessible() || !facts.addresses().is_empty() {
                return Err(invalid(
                    "paimon_read_split.facts",
                    "Paimon object-store split must be remotely accessible without node addresses",
                ));
            }
            let raw =
                decode_strict_root::<dto::PaimonReadSplitPayload>(payload, context, &SPLIT_SCHEMA)?;
            let mut files = Vec::with_capacity(raw.files.len());
            for file in &raw.files {
                files.push(decode_file(file, context)?);
                context.observe_compile_step()?;
            }
            // Existing domain validation/allocation is opaque.
            context.flush_compile_control()?;
            let split = PaimonSplit::try_new(
                raw.snapshot_id
                    .ok_or_else(|| missing("paimon_read_split.snapshot_id"))?,
                raw.schema_id
                    .ok_or_else(|| missing("paimon_read_split.schema_id"))?,
                raw.partition_arity
                    .ok_or_else(|| missing("paimon_read_split.partition_arity"))?,
                raw.partition,
                raw.bucket
                    .ok_or_else(|| missing("paimon_read_split.bucket"))?,
                &raw.bucket_path,
                raw.total_buckets
                    .ok_or_else(|| missing("paimon_read_split.total_buckets"))?,
                files,
                raw.data_deletion_files
                    .map(|values| decode_deletion_files(values, context))
                    .transpose()?,
                raw.row_ranges
                    .map(|values| decode_row_ranges(values, context))
                    .transpose()?,
                raw.raw_convertible
                    .ok_or_else(|| missing("paimon_read_split.raw_convertible"))?,
                raw.contains_delete_rows
                    .ok_or_else(|| missing("paimon_read_split.contains_delete_rows"))?,
                facts.split_weight(),
            )
            .map_err(domain_error)?;
            if split.retained_size_in_bytes() != facts.retained_size_in_bytes() {
                return Err(invalid(
                    "paimon_read_split.facts.retained_size",
                    "Paimon split retained size does not match the public carrier",
                ));
            }
            charge(
                context,
                payload.len(),
                usize::try_from(split.retained_size_in_bytes()).unwrap_or(usize::MAX),
            )?;
            Ok(split)
        })();
        // The public decode attempt (including ordinary refusal) is complete.
        context.observe_compile_step()?;
        finish_decode(result, context)
    }
}

fn encode_file(value: &PaimonDataFile) -> dto::PaimonDataFilePayload {
    let facts = value.facts();
    dto::PaimonDataFilePayload {
        file_name: facts.file_name.clone(),
        file_size: Some(value.file_size()),
        schema_id: Some(value.schema_id()),
        level: Some(value.level()),
        min_sequence_number: Some(value.min_sequence_number()),
        max_sequence_number: Some(value.max_sequence_number()),
        row_count: Some(value.row_count()),
        compression: encode_compression(value.compression()) as i32,
        min_key: facts.min_key.clone(),
        max_key: facts.max_key.clone(),
        key_stats: Some(encode_stats(&facts.key_stats)),
        value_stats: Some(encode_stats(&facts.value_stats)),
        extra_files: facts.extra_files.clone(),
        creation_time_millis: facts.creation_time_millis,
        delete_row_count: facts.delete_row_count,
        embedded_index: facts.embedded_index.clone(),
        file_source: facts.file_source,
        value_stats_cols: facts.value_stats_cols.as_ref().map(|values| {
            dto::PaimonStringListPayload {
                values: values.clone(),
            }
        }),
        external_path: facts.external_path.clone(),
        first_row_id: facts.first_row_id,
        write_cols: facts
            .write_cols
            .as_ref()
            .map(|values| dto::PaimonStringListPayload {
                values: values.clone(),
            }),
    }
}

fn decode_file(
    raw: &dto::PaimonDataFilePayload,
    context: &mut ConnectorDecodeContext<'_>,
) -> Result<PaimonDataFile, ConnectorCodecError> {
    let facts = PaimonDataFileFacts {
        file_name: clone_string(&raw.file_name, context)?,
        file_size: raw
            .file_size
            .ok_or_else(|| missing("paimon_read_split.file.file_size"))?,
        row_count: raw
            .row_count
            .ok_or_else(|| missing("paimon_read_split.file.row_count"))?,
        min_key: clone_bytes(&raw.min_key, context)?,
        max_key: clone_bytes(&raw.max_key, context)?,
        key_stats: decode_stats(
            raw.key_stats
                .as_ref()
                .ok_or_else(|| missing("paimon_read_split.file.key_stats"))?,
            context,
        )?,
        value_stats: decode_stats(
            raw.value_stats
                .as_ref()
                .ok_or_else(|| missing("paimon_read_split.file.value_stats"))?,
            context,
        )?,
        min_sequence_number: raw
            .min_sequence_number
            .ok_or_else(|| missing("paimon_read_split.file.min_sequence_number"))?,
        max_sequence_number: raw
            .max_sequence_number
            .ok_or_else(|| missing("paimon_read_split.file.max_sequence_number"))?,
        schema_id: raw
            .schema_id
            .ok_or_else(|| missing("paimon_read_split.file.schema_id"))?,
        level: raw
            .level
            .ok_or_else(|| missing("paimon_read_split.file.level"))?,
        extra_files: clone_strings(&raw.extra_files, context)?,
        creation_time_millis: raw.creation_time_millis,
        delete_row_count: raw.delete_row_count,
        embedded_index: raw
            .embedded_index
            .as_deref()
            .map(|value| clone_bytes(value, context))
            .transpose()?,
        file_source: raw.file_source,
        value_stats_cols: raw
            .value_stats_cols
            .as_ref()
            .map(|values| clone_strings(&values.values, context))
            .transpose()?,
        external_path: raw
            .external_path
            .as_deref()
            .map(|value| clone_string(value, context))
            .transpose()?,
        first_row_id: raw.first_row_id,
        write_cols: raw
            .write_cols
            .as_ref()
            .map(|values| clone_strings(&values.values, context))
            .transpose()?,
        compression: decode_compression(raw.compression)?,
    };
    // Domain validation and Arc construction are opaque existing operations.
    context.flush_compile_control()?;
    let result = PaimonDataFile::try_new(facts).map_err(domain_error);
    context.observe_compile_step()?;
    finish_decode(result, context)
}

fn encode_stats(value: &PaimonBinaryTableStats) -> dto::PaimonBinaryTableStatsPayload {
    dto::PaimonBinaryTableStatsPayload {
        min_values: value.min_values().to_vec(),
        max_values: value.max_values().to_vec(),
        null_counts: value
            .null_counts()
            .iter()
            .map(|value| dto::PaimonOptionalInt64Payload { value: *value })
            .collect(),
    }
}

fn decode_stats(
    raw: &dto::PaimonBinaryTableStatsPayload,
    context: &mut ConnectorDecodeContext<'_>,
) -> Result<PaimonBinaryTableStats, ConnectorCodecError> {
    let min_values = clone_bytes(&raw.min_values, context)?;
    let max_values = clone_bytes(&raw.max_values, context)?;
    let mut null_counts = Vec::with_capacity(raw.null_counts.len());
    for value in &raw.null_counts {
        null_counts.push(value.value);
        context.observe_compile_step()?;
    }
    context.flush_compile_control()?;
    let result =
        PaimonBinaryTableStats::try_new(min_values, max_values, null_counts).map_err(domain_error);
    context.observe_compile_step()?;
    finish_decode(result, context)
}

fn encode_deletion_file(value: &PaimonDeletionFile) -> dto::PaimonDeletionFilePayload {
    dto::PaimonDeletionFilePayload {
        path: value.path().to_string(),
        offset: Some(value.offset()),
        length: Some(value.length()),
        cardinality: value.cardinality(),
    }
}

fn decode_deletion_files(
    raw: dto::PaimonDeletionFilesPayload,
    context: &mut ConnectorDecodeContext<'_>,
) -> Result<Vec<Option<PaimonDeletionFile>>, ConnectorCodecError> {
    let mut values = Vec::with_capacity(raw.values.len());
    for value in raw.values {
        let value = value
            .value
            .map(|value| {
                context.flush_compile_control()?;
                let result = PaimonDeletionFile::try_new(
                    &value.path,
                    value
                        .offset
                        .ok_or_else(|| missing("paimon_read_split.deletion_file.offset"))?,
                    value
                        .length
                        .ok_or_else(|| missing("paimon_read_split.deletion_file.length"))?,
                    value.cardinality,
                )
                .map_err(domain_error);
                context.observe_compile_step()?;
                finish_decode(result, context)
            })
            .transpose()?;
        values.push(value);
        context.observe_compile_step()?;
    }
    Ok(values)
}

fn decode_row_ranges(
    raw: dto::PaimonRowRangesPayload,
    context: &mut ConnectorDecodeContext<'_>,
) -> Result<Vec<PaimonRowRange>, ConnectorCodecError> {
    let mut values = Vec::with_capacity(raw.values.len());
    for value in raw.values {
        values.push(
            PaimonRowRange::try_new(
                value
                    .from
                    .ok_or_else(|| missing("paimon_read_split.row_range.from"))?,
                value
                    .to
                    .ok_or_else(|| missing("paimon_read_split.row_range.to"))?,
            )
            .map_err(domain_error)?,
        );
        context.observe_compile_step()?;
    }
    Ok(values)
}

fn encode_type(value: PaimonDataType) -> dto::PaimonDataTypePayload {
    use dto::PaimonTypeKind as K;
    let (kind, precision, scale, timestamp_precision) = match value {
        PaimonDataType::Boolean => (K::Boolean, None, None, None),
        PaimonDataType::Int8 => (K::Int8, None, None, None),
        PaimonDataType::Int16 => (K::Int16, None, None, None),
        PaimonDataType::Int32 => (K::Int32, None, None, None),
        PaimonDataType::Int64 => (K::Int64, None, None, None),
        PaimonDataType::Float32 => (K::Float32, None, None, None),
        PaimonDataType::Float64 => (K::Float64, None, None, None),
        PaimonDataType::Decimal128 { precision, scale } => (
            K::Decimal128,
            Some(u32::from(precision)),
            Some(u32::from(scale)),
            None,
        ),
        PaimonDataType::Utf8 => (K::Utf8, None, None, None),
        PaimonDataType::Binary => (K::Binary, None, None, None),
        PaimonDataType::Date32 => (K::Date32, None, None, None),
        PaimonDataType::TimestampMillis { precision } => {
            (K::TimestampMillis, None, None, Some(u32::from(precision)))
        }
        PaimonDataType::TimestampMicros { precision } => {
            (K::TimestampMicros, None, None, Some(u32::from(precision)))
        }
    };
    dto::PaimonDataTypePayload {
        kind: kind as i32,
        decimal_precision: precision,
        decimal_scale: scale,
        timestamp_precision,
    }
}

fn decode_type(raw: &dto::PaimonDataTypePayload) -> Result<PaimonDataType, ConnectorCodecError> {
    use dto::PaimonTypeKind as K;
    let kind = K::try_from(raw.kind).map_err(|_| {
        invalid(
            "paimon_read_column.data_type.kind",
            "unknown Paimon type kind",
        )
    })?;
    let no_parameters = || {
        if raw.decimal_precision.is_none()
            && raw.decimal_scale.is_none()
            && raw.timestamp_precision.is_none()
        {
            Ok(())
        } else {
            Err(invalid(
                "paimon_read_column.data_type",
                "Paimon type has inconsistent parameters",
            ))
        }
    };
    let plain = |value| {
        no_parameters()?;
        Ok(value)
    };
    match kind {
        K::Boolean => plain(PaimonDataType::Boolean),
        K::Int8 => plain(PaimonDataType::Int8),
        K::Int16 => plain(PaimonDataType::Int16),
        K::Int32 => plain(PaimonDataType::Int32),
        K::Int64 => plain(PaimonDataType::Int64),
        K::Float32 => plain(PaimonDataType::Float32),
        K::Float64 => plain(PaimonDataType::Float64),
        K::Utf8 => plain(PaimonDataType::Utf8),
        K::Binary => plain(PaimonDataType::Binary),
        K::Date32 => plain(PaimonDataType::Date32),
        K::Decimal128 => PaimonDataType::decimal(
            u8::try_from(
                raw.decimal_precision
                    .ok_or_else(|| missing("paimon_read_column.data_type.decimal_precision"))?,
            )
            .map_err(|_| {
                invalid(
                    "paimon_read_column.data_type.decimal_precision",
                    "precision overflows",
                )
            })?,
            u8::try_from(
                raw.decimal_scale
                    .ok_or_else(|| missing("paimon_read_column.data_type.decimal_scale"))?,
            )
            .map_err(|_| {
                invalid(
                    "paimon_read_column.data_type.decimal_scale",
                    "scale overflows",
                )
            })?,
        )
        .map_err(domain_error),
        K::TimestampMillis | K::TimestampMicros => {
            if raw.decimal_precision.is_some() || raw.decimal_scale.is_some() {
                return Err(invalid(
                    "paimon_read_column.data_type",
                    "timestamp carries decimal parameters",
                ));
            }
            let precision = u8::try_from(
                raw.timestamp_precision
                    .ok_or_else(|| missing("paimon_read_column.data_type.timestamp_precision"))?,
            )
            .map_err(|_| {
                invalid(
                    "paimon_read_column.data_type.timestamp_precision",
                    "precision overflows",
                )
            })?;
            let value = PaimonDataType::timestamp(precision).map_err(domain_error)?;
            if (kind == K::TimestampMillis)
                != matches!(value, PaimonDataType::TimestampMillis { .. })
            {
                return Err(invalid(
                    "paimon_read_column.data_type.kind",
                    "timestamp carrier does not match precision",
                ));
            }
            Ok(value)
        }
        K::Unspecified => Err(invalid(
            "paimon_read_column.data_type.kind",
            "Paimon type kind is required",
        )),
    }
}

fn encode_merge(value: PaimonMergeEngine) -> dto::PaimonMergeEngine {
    match value {
        PaimonMergeEngine::AppendOnly => dto::PaimonMergeEngine::AppendOnly,
        PaimonMergeEngine::Deduplicate => dto::PaimonMergeEngine::Deduplicate,
    }
}
fn decode_merge(value: i32) -> Result<PaimonMergeEngine, ConnectorCodecError> {
    match dto::PaimonMergeEngine::try_from(value).ok() {
        Some(dto::PaimonMergeEngine::AppendOnly) => Ok(PaimonMergeEngine::AppendOnly),
        Some(dto::PaimonMergeEngine::Deduplicate) => Ok(PaimonMergeEngine::Deduplicate),
        _ => Err(invalid(
            "paimon_read_table.merge_engine",
            "unsupported Paimon merge engine",
        )),
    }
}
fn encode_bucket(value: PaimonBucketMode) -> dto::PaimonBucketMode {
    match value {
        PaimonBucketMode::Unbucketed => dto::PaimonBucketMode::Unbucketed,
        PaimonBucketMode::Fixed => dto::PaimonBucketMode::Fixed,
        PaimonBucketMode::Dynamic => dto::PaimonBucketMode::Dynamic,
    }
}
fn decode_bucket(value: i32) -> Result<PaimonBucketMode, ConnectorCodecError> {
    match dto::PaimonBucketMode::try_from(value).ok() {
        Some(dto::PaimonBucketMode::Unbucketed) => Ok(PaimonBucketMode::Unbucketed),
        Some(dto::PaimonBucketMode::Fixed) => Ok(PaimonBucketMode::Fixed),
        Some(dto::PaimonBucketMode::Dynamic) => Ok(PaimonBucketMode::Dynamic),
        _ => Err(invalid(
            "paimon_read_table.bucket_mode",
            "unsupported Paimon bucket mode",
        )),
    }
}
fn encode_compression(value: PaimonDataCompression) -> dto::PaimonDataCompression {
    match value {
        PaimonDataCompression::Uncompressed => dto::PaimonDataCompression::Uncompressed,
        PaimonDataCompression::Snappy => dto::PaimonDataCompression::Snappy,
        PaimonDataCompression::Zstd => dto::PaimonDataCompression::Zstd,
        PaimonDataCompression::Lz4Raw => dto::PaimonDataCompression::Lz4Raw,
    }
}
fn decode_compression(value: i32) -> Result<PaimonDataCompression, ConnectorCodecError> {
    match dto::PaimonDataCompression::try_from(value).ok() {
        Some(dto::PaimonDataCompression::Uncompressed) => Ok(PaimonDataCompression::Uncompressed),
        Some(dto::PaimonDataCompression::Snappy) => Ok(PaimonDataCompression::Snappy),
        Some(dto::PaimonDataCompression::Zstd) => Ok(PaimonDataCompression::Zstd),
        Some(dto::PaimonDataCompression::Lz4Raw) => Ok(PaimonDataCompression::Lz4Raw),
        _ => Err(invalid(
            "paimon_read_split.file.compression",
            "unsupported Paimon compression",
        )),
    }
}

fn decode_strict_root<M: Message + Default>(
    payload: &[u8],
    context: &mut ConnectorDecodeContext<'_>,
    schema: &'static StrictSchema,
) -> Result<M, ConnectorCodecError> {
    if payload.len() > MAX_PRIVATE_READ_BYTES {
        return Err(capacity(
            schema.name,
            "Paimon private payload exceeds 16 MiB",
        ));
    }
    context.ledger().charge_raw(payload.len())?;
    scan_strict_message(payload, context, schema, 0)?;
    context.flush_compile_control()?;
    let decoded = M::decode(payload).map_err(|error| {
        invalid(
            schema.name,
            format!("malformed Paimon private protobuf: {error}"),
        )
    });
    context.observe_compile_step()?;
    finish_decode(decoded, context)
}

fn scan_strict_message(
    mut input: &[u8],
    context: &mut ConnectorDecodeContext<'_>,
    schema: &'static StrictSchema,
    depth: usize,
) -> Result<(), ConnectorCodecError> {
    context.ledger().check_depth(depth)?;
    let mut seen = BTreeSet::new();
    while !input.is_empty() {
        context.ledger().charge_items(1)?;
        let key = read_varint(&mut input, context, schema.name)?;
        let field = u32::try_from(key >> 3).map_err(|_| malformed(schema.name))?;
        let wire = u8::try_from(key & 7).map_err(|_| malformed(schema.name))?;
        // One field key has been parsed; the static schema lookup is bounded.
        context.observe_compile_step()?;
        let Some((_, expected_wire)) = schema.fields.iter().find(|(number, _)| *number == field)
        else {
            return Err(ConnectorCodecError::new(
                ConnectorFieldPath::root(schema.name).field(format!("field_{field}")),
                ConnectorCodecErrorKind::UnknownField,
                "Paimon private payload contains an unknown nested field",
            ));
        };
        if wire != *expected_wire {
            return Err(invalid(
                schema.name,
                format!(
                    "Paimon private field {field} uses wire type {wire}, expected {expected_wire}"
                ),
            ));
        }
        if !schema.repeated.contains(&field) && !seen.insert(field) {
            return Err(ConnectorCodecError::new(
                ConnectorFieldPath::root(schema.name).field(format!("field_{field}")),
                ConnectorCodecErrorKind::DuplicateField,
                "Paimon private payload repeats a singular nested field",
            ));
        }
        if let Some((_, child)) = schema.children.iter().find(|(number, _)| *number == field) {
            if wire != 2 {
                return Err(malformed(schema.name));
            }
            let len = usize::try_from(read_varint(&mut input, context, schema.name)?)
                .map_err(|_| malformed(schema.name))?;
            let nested = take(&mut input, len, schema.name)?;
            scan_strict_message(nested, context, child, depth + 1)?;
        } else {
            skip_value(&mut input, wire, context, schema.name)?;
        }
    }
    Ok(())
}

fn decode_root<M: Message + Default>(
    payload: &[u8],
    context: &mut ConnectorDecodeContext<'_>,
    name: &'static str,
    singular: &[u32],
    repeated: &[u32],
) -> Result<M, ConnectorCodecError> {
    if payload.len() > MAX_PRIVATE_READ_BYTES {
        return Err(capacity(name, "Paimon private payload exceeds 16 MiB"));
    }
    context.ledger().charge_raw(payload.len())?;
    scan_root(payload, context, name, singular, repeated)?;
    context.flush_compile_control()?;
    let decoded = M::decode(payload)
        .map_err(|error| invalid(name, format!("malformed Paimon private protobuf: {error}")));
    context.observe_compile_step()?;
    finish_decode(decoded, context)
}

fn scan_root(
    mut input: &[u8],
    context: &mut ConnectorDecodeContext<'_>,
    name: &'static str,
    singular: &[u32],
    repeated: &[u32],
) -> Result<(), ConnectorCodecError> {
    let mut seen = BTreeSet::new();
    while !input.is_empty() {
        context.ledger().charge_items(1)?;
        let key = read_varint(&mut input, context, name)?;
        let field = u32::try_from(key >> 3).map_err(|_| malformed(name))?;
        let wire = u8::try_from(key & 7).map_err(|_| malformed(name))?;
        context.observe_compile_step()?;
        if field == 0 || (!singular.contains(&field) && !repeated.contains(&field)) {
            return Err(ConnectorCodecError::new(
                ConnectorFieldPath::root(name).field(format!("field_{field}")),
                ConnectorCodecErrorKind::UnknownField,
                "Paimon private payload contains an unknown field",
            ));
        }
        if singular.contains(&field) && !seen.insert(field) {
            return Err(ConnectorCodecError::new(
                ConnectorFieldPath::root(name).field(format!("field_{field}")),
                ConnectorCodecErrorKind::DuplicateField,
                "Paimon private payload repeats a singular field",
            ));
        }
        skip_value(&mut input, wire, context, name)?;
    }
    Ok(())
}

fn skip_value(
    input: &mut &[u8],
    wire: u8,
    context: &mut ConnectorDecodeContext<'_>,
    name: &'static str,
) -> Result<(), ConnectorCodecError> {
    let result = match wire {
        0 => read_varint(input, context, name).map(|_| ()),
        1 => take(input, 8, name).map(|_| ()),
        2 => {
            let len =
                usize::try_from(read_varint(input, context, name)?).map_err(|_| malformed(name))?;
            context.ledger().charge_scalar(len)?;
            take(input, len, name).map(|_| ())
        }
        5 => take(input, 4, name).map(|_| ()),
        _ => Err(malformed(name)),
    };
    // Borrowed fixed/length-delimited skipping and wire dispatch are complete;
    // scalar varints additionally expose each consumed byte below.
    context.observe_compile_step()?;
    result
}
fn read_varint(
    input: &mut &[u8],
    context: &mut ConnectorDecodeContext<'_>,
    name: &'static str,
) -> Result<u64, ConnectorCodecError> {
    let mut value = 0u64;
    for shift in (0..70).step_by(7) {
        let Some((&byte, rest)) = input.split_first() else {
            return Err(malformed(name));
        };
        *input = rest;
        let overflow = shift == 63 && byte > 1;
        if !overflow {
            value |= u64::from(byte & 0x7f) << shift;
        }
        // Exactly one byte was consumed and its bounded arithmetic completed.
        context.observe_compile_step()?;
        if overflow {
            return Err(malformed(name));
        }
        if byte & 0x80 == 0 {
            return Ok(value);
        }
    }
    Err(malformed(name))
}
fn take<'a>(
    input: &mut &'a [u8],
    len: usize,
    name: &'static str,
) -> Result<&'a [u8], ConnectorCodecError> {
    if input.len() < len {
        return Err(malformed(name));
    }
    let (value, rest) = input.split_at(len);
    *input = rest;
    Ok(value)
}
fn malformed(name: &'static str) -> ConnectorCodecError {
    invalid(name, "malformed Paimon private protobuf structure")
}
fn charge(
    context: &mut ConnectorDecodeContext<'_>,
    raw: usize,
    concrete: usize,
) -> Result<(), ConnectorCodecError> {
    context
        .ledger()
        .charge_retained(concrete.saturating_add(raw.saturating_mul(2)))
}
fn fixed_32(value: &[u8], path: &'static str) -> Result<[u8; 32], ConnectorCodecError> {
    value
        .try_into()
        .map_err(|_| invalid(path, "digest must contain exactly 32 bytes"))
}
fn missing(path: &'static str) -> ConnectorCodecError {
    ConnectorCodecError::new(
        ConnectorFieldPath::root(path),
        ConnectorCodecErrorKind::MissingField,
        "required Paimon field is missing",
    )
}
fn invalid(path: &'static str, detail: impl AsRef<str>) -> ConnectorCodecError {
    ConnectorCodecError::new(
        ConnectorFieldPath::root(path),
        ConnectorCodecErrorKind::InvalidValue,
        detail,
    )
}
fn capacity(path: &'static str, detail: &'static str) -> ConnectorCodecError {
    ConnectorCodecError::new(
        ConnectorFieldPath::root(path),
        ConnectorCodecErrorKind::Capacity,
        detail,
    )
}
fn domain_error(error: impl std::fmt::Display) -> ConnectorCodecError {
    invalid("paimon_payload", error.to_string())
}

// These copies expose the work owned by this codec. Allocator growth and the
// existing domain constructors remain opaque; this is not a memory grant.
fn clone_bytes(
    value: &[u8],
    context: &mut ConnectorDecodeContext<'_>,
) -> Result<Vec<u8>, ConnectorCodecError> {
    if !context.is_compile_observed() {
        return Ok(value.to_vec());
    }
    let mut result = Vec::with_capacity(value.len());
    for byte in value {
        result.push(*byte);
        context.observe_compile_step()?;
    }
    Ok(result)
}
fn clone_string(
    value: &str,
    context: &mut ConnectorDecodeContext<'_>,
) -> Result<String, ConnectorCodecError> {
    if !context.is_compile_observed() {
        return Ok(value.to_owned());
    }
    let mut result = String::with_capacity(value.len());
    for character in value.chars() {
        result.push(character);
        context.observe_compile_step()?;
    }
    Ok(result)
}
fn clone_strings(
    values: &[String],
    context: &mut ConnectorDecodeContext<'_>,
) -> Result<Vec<String>, ConnectorCodecError> {
    if !context.is_compile_observed() {
        return Ok(values.to_vec());
    }
    let mut result = Vec::with_capacity(values.len());
    for value in values {
        result.push(clone_string(value, context)?);
        context.observe_compile_step()?;
    }
    Ok(result)
}
fn finish_decode<T>(
    result: Result<T, ConnectorCodecError>,
    context: &mut ConnectorDecodeContext<'_>,
) -> Result<T, ConnectorCodecError> {
    // A latched original control cause wins. Otherwise observe ordinary failures
    // as well as successful values before returning to the caller.
    if result
        .as_ref()
        .is_err_and(|error| error.compile_control_error().is_some())
    {
        return result;
    }
    context.flush_compile_control()?;
    result
}

#[cfg(test)]
mod compile_control_tests {
    use super::*;
    use std::sync::Mutex;

    use novarocks_spi::connector::read_stack::SplitWeight;
    use novarocks_spi::connector::{
        CatalogHandle, CatalogVersion, ConnectorCodecCategory, ConnectorCodecRevision,
        ConnectorDecodeLedger, ConnectorDecodeLimits, ConnectorEnvelopeHeader, ConnectorInstanceId,
        ConnectorProviderId,
    };
    use novarocks_type_contract::{CompileControlError, CompilePhase, PureCompileControl};

    #[derive(Default)]
    struct Control {
        trace: Mutex<Vec<(CompilePhase, u32)>>,
        refuse: Option<(usize, CompileControlError)>,
    }
    impl PureCompileControl for Control {
        fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
            let mut trace = self.trace.lock().unwrap();
            trace.push((phase, units));
            if let Some((index, cause)) = self.refuse
                && trace.len() == index
            {
                return Err(cause);
            }
            Ok(())
        }
    }
    fn causes() -> [CompileControlError; 3] {
        [
            CompileControlError::Cancelled,
            CompileControlError::DeadlineExceeded,
            CompileControlError::ResourceExhausted,
        ]
    }
    fn header() -> ConnectorEnvelopeHeader {
        ConnectorEnvelopeHeader::new(
            ConnectorProviderId::parse("paimon").unwrap(),
            CatalogHandle::new(
                ConnectorInstanceId::try_from_canonical("lake").unwrap(),
                CatalogVersion::from_bytes([9; 32]),
            ),
            ConnectorCodecCategory::ReadSplit,
            ConnectorCodecRevision::try_new(1).unwrap(),
        )
    }
    fn ledger() -> ConnectorDecodeLedger {
        ConnectorDecodeLedger::new(
            ConnectorDecodeLimits::try_new(1 << 20, 4 << 20, 1 << 20, 100_000, 32).unwrap(),
        )
    }
    fn stats() -> dto::PaimonBinaryTableStatsPayload {
        dto::PaimonBinaryTableStatsPayload {
            min_values: vec![0, 255, 19],
            max_values: vec![42, 0],
            null_counts: (0..321)
                .map(|index| dto::PaimonOptionalInt64Payload {
                    value: (index % 3 != 0).then_some(index),
                })
                .collect(),
        }
    }
    fn file() -> dto::PaimonDataFilePayload {
        dto::PaimonDataFilePayload {
            file_name: "part-你好".into(),
            file_size: Some(1000),
            schema_id: Some(0),
            level: Some(0),
            min_sequence_number: Some(1),
            max_sequence_number: Some(2),
            row_count: Some(1000),
            compression: dto::PaimonDataCompression::Uncompressed as i32,
            min_key: vec![0, 255],
            max_key: vec![255, 0],
            key_stats: Some(stats()),
            value_stats: Some(stats()),
            extra_files: vec!["索引".repeat(321)],
            embedded_index: Some(vec![17; 321]),
            value_stats_cols: Some(dto::PaimonStringListPayload {
                values: vec!["column-a".into(), "column-b".into()],
            }),
            write_cols: Some(dto::PaimonStringListPayload {
                values: vec!["column-b".into()],
            }),
            ..Default::default()
        }
    }
    fn split_fixture() -> (Vec<u8>, ConnectorReadSplitFacts) {
        let expected = header();
        let mut budget = ledger();
        let mut context = ConnectorDecodeContext::new(&expected, &mut budget);
        let file = decode_file(&file(), &mut context).unwrap();
        let split = PaimonSplit::try_new(
            1,
            0,
            0,
            Vec::new(),
            0,
            "s3://warehouse/table/bucket-0",
            -1,
            vec![file],
            None,
            None,
            true,
            false,
            SplitWeight::STANDARD,
        )
        .unwrap();
        let facts = ConnectorReadSplitFacts::new(
            true,
            Vec::new(),
            None::<&str>,
            SplitWeight::STANDARD,
            split.retained_size_in_bytes(),
        );
        (
            PaimonReadWireCodec.encode_private(&split).unwrap().to_vec(),
            facts,
        )
    }
    fn run_split(
        payload: &[u8],
        facts: &ConnectorReadSplitFacts,
        control: &Control,
    ) -> Result<PaimonSplit, ConnectorCodecError> {
        let expected = header();
        let mut budget = ledger();
        let mut context =
            ConnectorDecodeContext::try_new_for_compile(&expected, &mut budget, control)?;
        PaimonReadWireCodec.decode_split_private(payload, facts, &mut context)
    }

    #[test]
    fn actual_long_split_preserves_legacy_values_and_ledger_charges() {
        let (payload, facts) = split_fixture();
        let expected = header();
        let mut legacy_budget = ledger();
        let mut legacy = ConnectorDecodeContext::new(&expected, &mut legacy_budget);
        let old = PaimonReadWireCodec
            .decode_split_private(&payload, &facts, &mut legacy)
            .unwrap();
        let control = Control::default();
        let mut compile_budget = ledger();
        let mut compile =
            ConnectorDecodeContext::try_new_for_compile(&expected, &mut compile_budget, &control)
                .unwrap();
        let new = PaimonReadWireCodec
            .decode_split_private(&payload, &facts, &mut compile)
            .unwrap();
        assert_eq!(
            PaimonReadWireCodec.encode_private(&old).unwrap().as_ref(),
            payload
        );
        assert_eq!(
            PaimonReadWireCodec.encode_private(&new).unwrap().as_ref(),
            payload
        );
        assert_eq!(legacy_budget.raw_bytes(), compile_budget.raw_bytes());
        assert_eq!(legacy_budget.items(), compile_budget.items());
        assert_eq!(
            legacy_budget.retained_bytes(),
            compile_budget.retained_bytes()
        );
        assert_eq!(new.files()[0].facts().key_stats.null_counts()[1], Some(1));
        assert_eq!(new.files()[0].facts().key_stats.null_counts()[0], None);
        let trace = control.trace.lock().unwrap();
        assert!(trace.iter().filter(|(_, units)| *units == 256).count() >= 4);
        assert!(
            trace
                .iter()
                .all(|(phase, units)| *phase == CompilePhase::ProviderValidation && *units <= 256)
        );
    }

    #[test]
    fn actual_long_split_refuses_entry_scanner_conversion_and_publication_each_cause() {
        let (payload, facts) = split_fixture();
        let successful = Control::default();
        run_split(&payload, &facts, &successful).unwrap();
        let trace = successful.trace.lock().unwrap().clone();
        assert_eq!(trace[0].1, 0);
        assert!(trace.iter().any(|(_, units)| *units == 256));
        // Refuse every actual callback, including scanner/Prost handoffs,
        // conversion loops, opaque domain handoffs and final publication.
        for index in 1..=trace.len() {
            for cause in causes() {
                let control = Control {
                    trace: Default::default(),
                    refuse: Some((index, cause)),
                };
                let error = run_split(&payload, &facts, &control).unwrap_err();
                assert_eq!(error.compile_control_error(), Some(cause));
                assert_eq!(error.kind(), ConnectorCodecErrorKind::CompileControl(cause));
                assert_eq!(*control.trace.lock().unwrap(), trace[..index]);
            }
        }
    }

    #[test]
    fn stats_conversion_observes_actual_count_and_byte_copy_work_and_latches() {
        let raw = stats();
        let expected = header();
        let control = Control::default();
        let mut budget = ledger();
        let mut context =
            ConnectorDecodeContext::try_new_for_compile(&expected, &mut budget, &control).unwrap();
        let result = decode_stats(&raw, &mut context).unwrap();
        assert_eq!(result.min_values(), &[0, 255, 19]);
        assert_eq!(result.max_values(), &[42, 0]);
        assert_eq!(result.null_counts().len(), 321);
        assert_eq!(
            control
                .trace
                .lock()
                .unwrap()
                .iter()
                .map(|(_, units)| *units)
                .collect::<Vec<_>>(),
            vec![0, 256, 70, 1]
        );
        for cause in causes() {
            let control = Control {
                trace: Default::default(),
                refuse: Some((2, cause)),
            };
            let mut budget = ledger();
            let mut context =
                ConnectorDecodeContext::try_new_for_compile(&expected, &mut budget, &control)
                    .unwrap();
            assert_eq!(
                decode_stats(&raw, &mut context)
                    .unwrap_err()
                    .compile_control_error(),
                Some(cause)
            );
            assert_eq!(
                context
                    .flush_compile_control()
                    .unwrap_err()
                    .compile_control_error(),
                Some(cause)
            );
            assert_eq!(
                context
                    .observe_compile_step()
                    .unwrap_err()
                    .compile_control_error(),
                Some(cause)
            );
            assert_eq!(control.trace.lock().unwrap().len(), 2);
        }
    }

    #[test]
    fn varint_checkpoint_follows_the_consumed_byte_not_a_reservation() {
        let expected = header();
        for cause in causes() {
            let control = Control {
                trace: Default::default(),
                refuse: Some((2, cause)),
            };
            let mut budget = ledger();
            let mut context =
                ConnectorDecodeContext::try_new_for_compile(&expected, &mut budget, &control)
                    .unwrap();
            assert_eq!(clone_bytes(&[1; 255], &mut context).unwrap().len(), 255);
            let mut input: &[u8] = &[7];
            let error = read_varint(&mut input, &mut context, "test_varint").unwrap_err();
            assert_eq!(error.compile_control_error(), Some(cause));
            assert!(
                input.is_empty(),
                "the 256th completed operation consumed its byte"
            );
            assert_eq!(control.trace.lock().unwrap()[1].1, 256);
        }
    }

    #[test]
    fn ordinary_domain_error_flushes_original_tail_without_text_classification() {
        let mut raw = stats();
        raw.null_counts[320].value = Some(-1);
        let expected = header();
        let control = Control::default();
        let mut budget = ledger();
        let mut context =
            ConnectorDecodeContext::try_new_for_compile(&expected, &mut budget, &control).unwrap();
        let ordinary = decode_stats(&raw, &mut context).unwrap_err();
        assert_eq!(ordinary.kind(), ConnectorCodecErrorKind::InvalidValue);
        assert_eq!(ordinary.compile_control_error(), None);
        let trace = control.trace.lock().unwrap().clone();
        for cause in causes() {
            let control = Control {
                trace: Default::default(),
                refuse: Some((trace.len(), cause)),
            };
            let mut budget = ledger();
            let mut context =
                ConnectorDecodeContext::try_new_for_compile(&expected, &mut budget, &control)
                    .unwrap();
            assert_eq!(
                decode_stats(&raw, &mut context)
                    .unwrap_err()
                    .compile_control_error(),
                Some(cause)
            );
        }
    }

    #[test]
    fn malformed_prost_utf8_flushes_after_the_opaque_decode() {
        // Structurally valid field 1 String, invalid UTF-8: the private scanner
        // accepts structure, then Prost owns this ordinary decoding failure.
        let payload = [0x0a, 1, 0xff];
        let expected = header();
        let control = Control::default();
        let mut budget = ledger();
        let mut context =
            ConnectorDecodeContext::try_new_for_compile(&expected, &mut budget, &control).unwrap();
        let error = <PaimonReadWireCodec as ConnectorPrivateDecoder<PaimonTable>>::decode_private(
            &PaimonReadWireCodec,
            &payload,
            &mut context,
        )
        .unwrap_err();
        assert_eq!(error.kind(), ConnectorCodecErrorKind::InvalidValue);
        let trace = control.trace.lock().unwrap().clone();
        assert!(trace.len() >= 3);
        for cause in causes() {
            // Last callback observes the opaque decoding refusal's tail.
            let control = Control {
                trace: Default::default(),
                refuse: Some((trace.len(), cause)),
            };
            let mut budget = ledger();
            let mut context =
                ConnectorDecodeContext::try_new_for_compile(&expected, &mut budget, &control)
                    .unwrap();
            let error =
                <PaimonReadWireCodec as ConnectorPrivateDecoder<PaimonTable>>::decode_private(
                    &PaimonReadWireCodec,
                    &payload,
                    &mut context,
                )
                .unwrap_err();
            assert_eq!(error.compile_control_error(), Some(cause));
        }
    }

    #[test]
    fn legacy_unknown_duplicate_and_nested_wire_refusals_remain_ordinary() {
        let expected = header();
        for (payload, kind) in [
            (vec![0x40, 0], ConnectorCodecErrorKind::UnknownField),
            (
                vec![0x0a, 0, 0x0a, 0],
                ConnectorCodecErrorKind::DuplicateField,
            ),
        ] {
            let mut budget = ledger();
            let mut context = ConnectorDecodeContext::new(&expected, &mut budget);
            let old =
                <PaimonReadWireCodec as ConnectorPrivateDecoder<PaimonTable>>::decode_private(
                    &PaimonReadWireCodec,
                    &payload,
                    &mut context,
                )
                .unwrap_err();
            assert_eq!(old.kind(), kind);
            let control = Control::default();
            let mut budget = ledger();
            let mut context =
                ConnectorDecodeContext::try_new_for_compile(&expected, &mut budget, &control)
                    .unwrap();
            let new =
                <PaimonReadWireCodec as ConnectorPrivateDecoder<PaimonTable>>::decode_private(
                    &PaimonReadWireCodec,
                    &payload,
                    &mut context,
                )
                .unwrap_err();
            assert_eq!(old.kind(), new.kind());
            assert_eq!(old.detail(), new.detail());
            assert_eq!(new.compile_control_error(), None);
        }
        let mut budget = ledger();
        let mut context = ConnectorDecodeContext::new(&expected, &mut budget);
        assert_eq!(
            decode_strict_root::<dto::PaimonColumnPayload>(
                &[0x0a, 0],
                &mut context,
                &COLUMN_SCHEMA
            )
            .unwrap_err()
            .kind(),
            ConnectorCodecErrorKind::InvalidValue
        );
    }

    #[test]
    fn actual_row_range_and_optional_deletion_conversion_loops_are_observed() {
        let expected = header();
        for cause in causes() {
            let control = Control {
                trace: Default::default(),
                refuse: Some((2, cause)),
            };
            let mut budget = ledger();
            let mut context =
                ConnectorDecodeContext::try_new_for_compile(&expected, &mut budget, &control)
                    .unwrap();
            let raw = dto::PaimonRowRangesPayload {
                values: (0..321)
                    .map(|n| dto::PaimonRowRangePayload {
                        from: Some(n),
                        to: Some(n + 1),
                    })
                    .collect(),
            };
            assert_eq!(
                decode_row_ranges(raw, &mut context)
                    .unwrap_err()
                    .compile_control_error(),
                Some(cause)
            );
            let control = Control {
                trace: Default::default(),
                refuse: Some((2, cause)),
            };
            let mut budget = ledger();
            let mut context =
                ConnectorDecodeContext::try_new_for_compile(&expected, &mut budget, &control)
                    .unwrap();
            let raw = dto::PaimonDeletionFilesPayload {
                values: (0..321)
                    .map(|_| dto::PaimonOptionalDeletionFilePayload { value: None })
                    .collect(),
            };
            assert_eq!(
                decode_deletion_files(raw, &mut context)
                    .unwrap_err()
                    .compile_control_error(),
                Some(cause)
            );
        }
        let mut budget = ledger();
        let mut context = ConnectorDecodeContext::new(&expected, &mut budget);
        let ranges = decode_row_ranges(
            dto::PaimonRowRangesPayload {
                values: vec![dto::PaimonRowRangePayload {
                    from: Some(2),
                    to: Some(7),
                }],
            },
            &mut context,
        )
        .unwrap();
        assert_eq!((ranges[0].from(), ranges[0].to()), (2, 7));
        assert_eq!(
            decode_deletion_files(
                dto::PaimonDeletionFilesPayload {
                    values: vec![dto::PaimonOptionalDeletionFilePayload { value: None }]
                },
                &mut context
            )
            .unwrap()
            .len(),
            1
        );
    }
}
