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

//! Structural validation of the logical writer handle carrier.

use novarocks_proto_models::connector_write as dto;
use prost::Message;

use super::shared::{
    validate_artifact_partition, validate_content_range, validate_file_content,
    validate_file_format,
};
use super::{
    MAX_NAME_BYTES, MAX_OLD_DELETE_MERGE_TARGETS, MAX_OLD_DELETE_REFERENCES, MAX_PARTITION_COLUMNS,
    MAX_PATH_BYTES, MAX_SCHEMA_JSON_BYTES, MAX_TRANSFORM_EXPR_BYTES, MAX_TRANSFORM_EXPRS,
    MAX_WRITER_HANDLE_ENCODED_BYTES, bounded_count, bounded_text, inconsistent, invalid_enum,
    missing, nonnegative_i64, out_of_range,
};
use crate::{FieldPath, ProtocolError};

/// A writer handle whose carrier is canonical, in bounds, and structurally an
/// Iceberg write recipe.
///
/// Being validated says nothing about whether the recipe describes a legal
/// Iceberg write: that judgement needs the table, and belongs to the provider's
/// own constructors. What it does guarantee is that no unbounded, unnamed, or
/// self-contradictory field reaches them.
#[derive(Clone, Debug, PartialEq)]
pub struct ValidatedWriterHandle {
    raw: dto::ConnectorWriterHandle,
}

impl ValidatedWriterHandle {
    pub fn parse(raw: dto::ConnectorWriterHandle, path: FieldPath) -> Result<Self, ProtocolError> {
        // Size is checked first: an oversized carrier is rejected before it is
        // walked, not after the walk happens to allocate.
        let encoded_len = raw.encoded_len();
        if encoded_len > MAX_WRITER_HANDLE_ENCODED_BYTES {
            return Err(out_of_range(
                path,
                format!(
                    "writer handle encodes to {encoded_len} bytes, over the hard limit {MAX_WRITER_HANDLE_ENCODED_BYTES}"
                ),
            ));
        }
        let handle = raw.handle.as_ref().ok_or_else(|| {
            missing(
                path.clone(),
                "a writer handle requires one provider variant",
            )
        })?;
        match handle {
            dto::connector_writer_handle::Handle::Iceberg(iceberg) => {
                validate_iceberg_writer_handle(iceberg, path.field("iceberg"))?;
            }
        }
        Ok(Self { raw })
    }

    pub const fn as_proto(&self) -> &dto::ConnectorWriterHandle {
        &self.raw
    }

    pub fn into_proto(self) -> dto::ConnectorWriterHandle {
        self.raw
    }

    pub fn iceberg(&self) -> &dto::IcebergWriterHandle {
        match self.raw.handle.as_ref() {
            Some(dto::connector_writer_handle::Handle::Iceberg(iceberg)) => iceberg,
            None => unreachable!("a validated writer handle always carries a variant"),
        }
    }
}

fn validate_write_branch(
    value: i32,
    path: FieldPath,
) -> Result<dto::IcebergWriteBranch, ProtocolError> {
    match dto::IcebergWriteBranch::try_from(value) {
        Ok(dto::IcebergWriteBranch::Unspecified) | Err(_) => Err(invalid_enum(
            path,
            "write branch must be a named Iceberg write branch",
        )),
        Ok(branch) => Ok(branch),
    }
}

fn validate_table_facts(
    table: Option<&dto::IcebergWriteTableFacts>,
    path: FieldPath,
) -> Result<(), ProtocolError> {
    let table =
        table.ok_or_else(|| missing(path.clone(), "a writer handle requires its table facts"))?;
    bounded_text(
        &table.table_uuid,
        MAX_NAME_BYTES,
        path.clone().field("table_uuid"),
        false,
    )?;
    bounded_text(
        &table.namespace,
        MAX_NAME_BYTES,
        path.clone().field("namespace"),
        false,
    )?;
    bounded_text(
        &table.table_name,
        MAX_NAME_BYTES,
        path.clone().field("table_name"),
        false,
    )?;
    bounded_text(
        &table.table_location,
        MAX_PATH_BYTES,
        path.clone().field("table_location"),
        false,
    )?;
    bounded_text(
        &table.data_location,
        MAX_PATH_BYTES,
        path.clone().field("data_location"),
        false,
    )?;
    bounded_text(
        &table.target_ref,
        MAX_NAME_BYTES,
        path.clone().field("target_ref"),
        false,
    )?;
    nonnegative_i64(
        table.base_sequence_number,
        path.clone().field("base_sequence_number"),
        "base sequence number",
    )?;
    if table.schema_id < 0 {
        return Err(out_of_range(
            path.clone().field("schema_id"),
            "schema id must be nonnegative",
        ));
    }
    if table.default_partition_spec_id < 0 {
        return Err(out_of_range(
            path.clone().field("default_partition_spec_id"),
            "default partition spec id must be nonnegative",
        ));
    }
    // Iceberg has exactly three table format versions today. An unnamed one is
    // a rejection rather than an optimistic "probably compatible".
    if !(1..=3).contains(&table.format_version) {
        return Err(out_of_range(
            path.field("format_version"),
            format!(
                "table format version {} is outside the supported range 1..=3",
                table.format_version
            ),
        ));
    }
    Ok(())
}

fn validate_data_recipe(
    recipe: &dto::IcebergDataBranchRecipe,
    path: FieldPath,
) -> Result<(), ProtocolError> {
    if let Some(schema_json) = recipe.input_schema_json.as_ref() {
        bounded_text(
            schema_json,
            MAX_SCHEMA_JSON_BYTES,
            path.clone().field("input_schema_json"),
            false,
        )?;
    }
    for (field, label) in [
        (
            &recipe.partition_source_column_names,
            "partition_source_column_names",
        ),
        (&recipe.partition_column_names, "partition_column_names"),
    ] {
        bounded_count(
            field.len(),
            MAX_PARTITION_COLUMNS,
            path.clone().field(label),
            "partition column",
        )?;
        for (index, name) in field.iter().enumerate() {
            bounded_text(
                name,
                MAX_NAME_BYTES,
                path.clone().field(label).index(index),
                false,
            )?;
        }
    }
    bounded_count(
        recipe.transform_exprs.len(),
        MAX_TRANSFORM_EXPRS,
        path.clone().field("transform_exprs"),
        "transform expression",
    )?;
    for (index, expr) in recipe.transform_exprs.iter().enumerate() {
        bounded_text(
            expr,
            MAX_TRANSFORM_EXPR_BYTES,
            path.clone().field("transform_exprs").index(index),
            false,
        )?;
    }
    // A partition column with no transform, or the reverse, would leave the
    // writer guessing which pairing was meant.
    if recipe.partition_column_names.len() != recipe.transform_exprs.len() {
        return Err(inconsistent(
            path,
            "each partition column requires exactly one transform expression",
        ));
    }
    Ok(())
}

fn validate_old_delete_reference(
    reference: &dto::IcebergOldDeleteArtifactRef,
    path: FieldPath,
) -> Result<(), ProtocolError> {
    bounded_text(
        &reference.path,
        MAX_PATH_BYTES,
        path.clone().field("path"),
        false,
    )?;
    let content = validate_file_content(reference.content, path.clone().field("content"))?;
    if content != dto::IcebergFileContent::PositionDeletes {
        return Err(inconsistent(
            path.field("content"),
            "an old delete reference must name position deletes",
        ));
    }
    let format = validate_file_format(reference.file_format, path.clone().field("file_format"))?;
    if reference.file_size_in_bytes == 0 {
        return Err(out_of_range(
            path.clone().field("file_size_in_bytes"),
            "an old delete artifact cannot be empty",
        ));
    }
    if reference.partition_spec_id < 0 {
        return Err(out_of_range(
            path.clone().field("partition_spec_id"),
            "partition spec id must be nonnegative",
        ));
    }
    if let Some(range) = reference.content_range.as_ref() {
        validate_content_range(range, path.clone().field("content_range"))?;
    }
    // A Puffin blob is one of several in its container, so it is only
    // addressable with a range and only meaningful against a named data file.
    // A Parquet delete file is the whole file, so a range would contradict it.
    match format {
        dto::IcebergFileFormat::Puffin => {
            if reference.content_range.is_none() {
                return Err(inconsistent(
                    path.clone().field("content_range"),
                    "a Puffin deletion vector requires its blob range",
                ));
            }
            if reference.referenced_data_file.is_none() {
                return Err(inconsistent(
                    path.clone().field("referenced_data_file"),
                    "a Puffin deletion vector requires its referenced data file",
                ));
            }
        }
        dto::IcebergFileFormat::Parquet => {
            if reference.content_range.is_some() {
                return Err(inconsistent(
                    path.clone().field("content_range"),
                    "a Parquet delete file has no blob range",
                ));
            }
        }
        dto::IcebergFileFormat::Unspecified => unreachable!("validated above"),
    }
    if let Some(referenced) = reference.referenced_data_file.as_ref() {
        bounded_text(
            referenced,
            MAX_PATH_BYTES,
            path.clone().field("referenced_data_file"),
            false,
        )?;
    }
    let route = reference.storage_route.as_ref().ok_or_else(|| {
        missing(
            path.clone().field("storage_route"),
            "an old delete reference requires its storage route",
        )
    })?;
    bounded_text(
        &route.access_binding,
        MAX_NAME_BYTES,
        path.field("storage_route").field("access_binding"),
        false,
    )
}

fn validate_old_delete_target(
    target: &dto::IcebergOldDeleteMergeTarget,
    key: &str,
    path: FieldPath,
) -> Result<(), ProtocolError> {
    bounded_text(
        &target.data_file_path,
        MAX_PATH_BYTES,
        path.clone().field("data_file_path"),
        false,
    )?;
    // The map key and the target must name the same file, or a lookup and the
    // merge it drives would disagree about which data file is being rewritten.
    if target.data_file_path != key {
        return Err(inconsistent(
            path.clone().field("data_file_path"),
            "an old delete target must be keyed by its own data file path",
        ));
    }
    nonnegative_i64(
        target.base_snapshot_id,
        path.clone().field("base_snapshot_id"),
        "base snapshot id",
    )?;
    validate_artifact_partition(target.partition.as_ref(), path.clone().field("partition"))?;
    bounded_count(
        target.references.len(),
        MAX_OLD_DELETE_REFERENCES,
        path.clone().field("references"),
        "old delete reference",
    )?;
    let mut previous: Option<&str> = None;
    for (index, reference) in target.references.iter().enumerate() {
        let reference_path = path.clone().field("references").index(index);
        validate_old_delete_reference(reference, reference_path.clone())?;
        // Sorted and unique rather than repaired: two spellings of one artifact
        // would be merged twice, and an order that depends on who built the
        // handle would make the same write two different writes.
        if let Some(previous) = previous
            && previous >= reference.path.as_str()
        {
            return Err(inconsistent(
                reference_path.field("path"),
                "old delete references must be sorted and unique by path",
            ));
        }
        previous = Some(reference.path.as_str());
    }
    Ok(())
}

fn validate_iceberg_writer_handle(
    handle: &dto::IcebergWriterHandle,
    path: FieldPath,
) -> Result<(), ProtocolError> {
    let branch = validate_write_branch(handle.branch, path.clone().field("branch"))?;
    validate_table_facts(handle.table.as_ref(), path.clone().field("table"))?;
    let output = handle.output.as_ref().ok_or_else(|| {
        missing(
            path.clone().field("output"),
            "a writer handle requires its output settings",
        )
    })?;
    validate_file_format(
        output.file_format,
        path.clone().field("output").field("file_format"),
    )?;
    if let Some(size) = output.parquet_row_group_size_bytes
        && size == 0
    {
        return Err(out_of_range(
            path.clone()
                .field("output")
                .field("parquet_row_group_size_bytes"),
            "a parquet row group must hold at least one byte",
        ));
    }

    // The branch decides which of the two optional recipes must be present.
    // Accepting both, or neither, would let a writer pick.
    match branch {
        dto::IcebergWriteBranch::Data => {
            let recipe = handle.data.as_ref().ok_or_else(|| {
                inconsistent(
                    path.clone().field("data"),
                    "a data branch requires its data recipe",
                )
            })?;
            validate_data_recipe(recipe, path.clone().field("data"))?;
            if !handle.old_deletes.is_empty() {
                return Err(inconsistent(
                    path.field("old_deletes"),
                    "a data branch never merges old delete artifacts",
                ));
            }
        }
        dto::IcebergWriteBranch::PositionDelete | dto::IcebergWriteBranch::DeletionVector => {
            if handle.data.is_some() {
                return Err(inconsistent(
                    path.clone().field("data"),
                    "a delete branch has no data recipe",
                ));
            }
            bounded_count(
                handle.old_deletes.len(),
                MAX_OLD_DELETE_MERGE_TARGETS,
                path.clone().field("old_deletes"),
                "old delete merge target",
            )?;
            for (key, target) in &handle.old_deletes {
                let target_path = path.clone().field("old_deletes").map_key(key.clone());
                validate_old_delete_target(target, key, target_path)?;
            }
        }
        dto::IcebergWriteBranch::Unspecified => unreachable!("validated above"),
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ProtocolErrorKind;

    fn table() -> dto::IcebergWriteTableFacts {
        dto::IcebergWriteTableFacts {
            table_uuid: "9c2f1f66-1f0f-4c9a-9a1a-3f1d2c0b7a11".to_string(),
            namespace: "db".to_string(),
            table_name: "t".to_string(),
            table_location: "s3://bucket/db/t".to_string(),
            data_location: "s3://bucket/db/t/data".to_string(),
            target_ref: "main".to_string(),
            base_snapshot_id: Some(7),
            base_sequence_number: 3,
            schema_id: 0,
            default_partition_spec_id: 0,
            format_version: 2,
        }
    }

    fn output() -> dto::IcebergWriterOutput {
        dto::IcebergWriterOutput {
            file_format: dto::IcebergFileFormat::Parquet as i32,
            compression: dto::IcebergCompression::Zstd as i32,
            parquet_row_group_size_bytes: Some(1024),
        }
    }

    fn partition() -> dto::IcebergArtifactPartition {
        dto::IcebergArtifactPartition {
            partition_path: String::new(),
            null_fingerprint: String::new(),
            partition_spec_id: 0,
            descriptor: Some(dto::IcebergPartitionDescriptor { values: Vec::new() }),
        }
    }

    fn data_handle() -> dto::ConnectorWriterHandle {
        dto::ConnectorWriterHandle {
            handle: Some(dto::connector_writer_handle::Handle::Iceberg(
                dto::IcebergWriterHandle {
                    branch: dto::IcebergWriteBranch::Data as i32,
                    table: Some(table()),
                    output: Some(output()),
                    data: Some(dto::IcebergDataBranchRecipe {
                        input_schema_json: None,
                        partition_source_column_names: vec!["d".to_string()],
                        partition_column_names: vec!["d_day".to_string()],
                        transform_exprs: vec!["day(d)".to_string()],
                        row_lineage: false,
                    }),
                    old_deletes: std::collections::BTreeMap::new(),
                },
            )),
        }
    }

    fn puffin_reference(path: &str) -> dto::IcebergOldDeleteArtifactRef {
        dto::IcebergOldDeleteArtifactRef {
            path: path.to_string(),
            content: dto::IcebergFileContent::PositionDeletes as i32,
            file_format: dto::IcebergFileFormat::Puffin as i32,
            file_size_in_bytes: 128,
            record_count: 4,
            content_range: Some(dto::IcebergContentRange {
                offset: 4,
                size_in_bytes: 64,
            }),
            referenced_data_file: Some("s3://bucket/db/t/data/a.parquet".to_string()),
            data_sequence_number: Some(2),
            added_snapshot_id: Some(9),
            partition_spec_id: 0,
            storage_route: Some(dto::IcebergStorageRoute {
                access_binding: "s3://bucket/db/t".to_string(),
            }),
        }
    }

    fn delete_handle(
        references: Vec<dto::IcebergOldDeleteArtifactRef>,
    ) -> dto::ConnectorWriterHandle {
        let data_file = "s3://bucket/db/t/data/a.parquet".to_string();
        let mut old_deletes = std::collections::BTreeMap::new();
        old_deletes.insert(
            data_file.clone(),
            dto::IcebergOldDeleteMergeTarget {
                data_file_path: data_file,
                data_file_record_count: 100,
                data_file_sequence_number: Some(2),
                partition: Some(partition()),
                base_snapshot_id: 9,
                references,
            },
        );
        dto::ConnectorWriterHandle {
            handle: Some(dto::connector_writer_handle::Handle::Iceberg(
                dto::IcebergWriterHandle {
                    branch: dto::IcebergWriteBranch::DeletionVector as i32,
                    table: Some(table()),
                    output: Some(dto::IcebergWriterOutput {
                        file_format: dto::IcebergFileFormat::Puffin as i32,
                        compression: dto::IcebergCompression::None as i32,
                        parquet_row_group_size_bytes: None,
                    }),
                    data: None,
                    old_deletes,
                },
            )),
        }
    }

    fn parse(raw: dto::ConnectorWriterHandle) -> Result<ValidatedWriterHandle, ProtocolError> {
        ValidatedWriterHandle::parse(raw, FieldPath::root("writer_handle"))
    }

    #[test]
    fn a_well_formed_handle_round_trips_through_its_carrier() {
        let raw = data_handle();
        let validated = parse(raw.clone()).expect("valid data handle");
        assert_eq!(validated.as_proto(), &raw);
        assert_eq!(validated.into_proto(), raw);

        let raw = delete_handle(vec![
            puffin_reference("s3://bucket/db/t/data/a-dv-1.puffin"),
            puffin_reference("s3://bucket/db/t/data/a-dv-2.puffin"),
        ]);
        let validated = parse(raw.clone()).expect("valid delete handle");
        assert_eq!(validated.iceberg().old_deletes.len(), 1);
        assert_eq!(validated.as_proto(), &raw);
    }

    #[test]
    fn a_handle_without_a_provider_variant_is_a_missing_field() {
        let error = parse(dto::ConnectorWriterHandle { handle: None }).expect_err("no variant");
        assert_eq!(error.kind(), ProtocolErrorKind::MissingField);
        assert_eq!(error.path().to_string(), "writer_handle");
    }

    #[test]
    fn an_unnamed_enum_value_is_rejected_rather_than_defaulted() {
        let mut raw = data_handle();
        let dto::connector_writer_handle::Handle::Iceberg(iceberg) =
            raw.handle.as_mut().expect("variant");
        iceberg.branch = dto::IcebergWriteBranch::Unspecified as i32;
        let error = parse(raw).expect_err("unspecified branch");
        assert_eq!(error.kind(), ProtocolErrorKind::InvalidEnum);
        assert_eq!(error.path().to_string(), "writer_handle.iceberg.branch");
    }

    #[test]
    fn the_branch_decides_which_recipe_must_be_present() {
        // A data branch that also claims old deletes.
        let mut raw = data_handle();
        let dto::connector_writer_handle::Handle::Iceberg(iceberg) =
            raw.handle.as_mut().expect("variant");
        iceberg.old_deletes.insert(
            "s3://bucket/db/t/data/a.parquet".to_string(),
            dto::IcebergOldDeleteMergeTarget {
                data_file_path: "s3://bucket/db/t/data/a.parquet".to_string(),
                data_file_record_count: 1,
                data_file_sequence_number: None,
                partition: Some(partition()),
                base_snapshot_id: 1,
                references: Vec::new(),
            },
        );
        let error = parse(raw).expect_err("data branch with old deletes");
        assert_eq!(error.kind(), ProtocolErrorKind::InconsistentFields);
        assert_eq!(
            error.path().to_string(),
            "writer_handle.iceberg.old_deletes"
        );

        // A delete branch that also claims a data recipe.
        let mut raw = delete_handle(Vec::new());
        let dto::connector_writer_handle::Handle::Iceberg(iceberg) =
            raw.handle.as_mut().expect("variant");
        iceberg.data = Some(dto::IcebergDataBranchRecipe {
            input_schema_json: None,
            partition_source_column_names: Vec::new(),
            partition_column_names: Vec::new(),
            transform_exprs: Vec::new(),
            row_lineage: false,
        });
        let error = parse(raw).expect_err("delete branch with a data recipe");
        assert_eq!(error.kind(), ProtocolErrorKind::InconsistentFields);
        assert_eq!(error.path().to_string(), "writer_handle.iceberg.data");
    }

    #[test]
    fn a_puffin_reference_needs_its_blob_range_and_a_parquet_one_must_not_have_it() {
        let mut reference = puffin_reference("s3://bucket/db/t/data/a-dv.puffin");
        reference.content_range = None;
        let error = parse(delete_handle(vec![reference])).expect_err("puffin without a range");
        assert_eq!(error.kind(), ProtocolErrorKind::InconsistentFields);
        assert_eq!(
            error.path().to_string(),
            "writer_handle.iceberg.old_deletes[\"s3://bucket/db/t/data/a.parquet\"].references[0].content_range"
        );

        let mut reference = puffin_reference("s3://bucket/db/t/data/a-pos.parquet");
        reference.file_format = dto::IcebergFileFormat::Parquet as i32;
        let error = parse(delete_handle(vec![reference])).expect_err("parquet with a range");
        assert_eq!(error.kind(), ProtocolErrorKind::InconsistentFields);
    }

    #[test]
    fn old_delete_references_must_be_sorted_and_unique() {
        let duplicated = vec![
            puffin_reference("s3://bucket/db/t/data/a-dv-1.puffin"),
            puffin_reference("s3://bucket/db/t/data/a-dv-1.puffin"),
        ];
        let error = parse(delete_handle(duplicated)).expect_err("duplicate reference");
        assert_eq!(error.kind(), ProtocolErrorKind::InconsistentFields);

        let unsorted = vec![
            puffin_reference("s3://bucket/db/t/data/a-dv-2.puffin"),
            puffin_reference("s3://bucket/db/t/data/a-dv-1.puffin"),
        ];
        let error = parse(delete_handle(unsorted)).expect_err("unsorted references");
        assert_eq!(error.kind(), ProtocolErrorKind::InconsistentFields);
    }

    #[test]
    fn an_old_delete_target_must_be_keyed_by_its_own_data_file() {
        let mut raw = delete_handle(Vec::new());
        let dto::connector_writer_handle::Handle::Iceberg(iceberg) =
            raw.handle.as_mut().expect("variant");
        let target = iceberg
            .old_deletes
            .get_mut("s3://bucket/db/t/data/a.parquet")
            .expect("target");
        target.data_file_path = "s3://bucket/db/t/data/b.parquet".to_string();
        let error = parse(raw).expect_err("key and target disagree");
        assert_eq!(error.kind(), ProtocolErrorKind::InconsistentFields);
    }

    #[test]
    fn a_partition_value_carries_a_datum_exactly_when_it_is_not_null() {
        for (is_null, datum) in [(true, Some(vec![1_u8])), (false, None)] {
            let mut raw = delete_handle(Vec::new());
            let dto::connector_writer_handle::Handle::Iceberg(iceberg) =
                raw.handle.as_mut().expect("variant");
            let target = iceberg
                .old_deletes
                .get_mut("s3://bucket/db/t/data/a.parquet")
                .expect("target");
            target
                .partition
                .as_mut()
                .expect("partition")
                .descriptor
                .as_mut()
                .expect("descriptor")
                .values
                .push(dto::IcebergPartitionValueDescriptor {
                    is_null,
                    datum_bytes: datum,
                });
            let error = parse(raw).expect_err("partition value disagreement");
            assert_eq!(error.kind(), ProtocolErrorKind::InconsistentFields);
        }
    }

    #[test]
    fn an_unsupported_table_format_version_is_out_of_range() {
        for version in [0_u32, 4, 99] {
            let mut raw = data_handle();
            let dto::connector_writer_handle::Handle::Iceberg(iceberg) =
                raw.handle.as_mut().expect("variant");
            iceberg.table.as_mut().expect("table").format_version = version;
            let error = parse(raw).expect_err("unsupported format version");
            assert_eq!(error.kind(), ProtocolErrorKind::OutOfRange);
            assert_eq!(
                error.path().to_string(),
                "writer_handle.iceberg.table.format_version"
            );
        }
    }

    #[test]
    fn every_partition_column_needs_exactly_one_transform() {
        let mut raw = data_handle();
        let dto::connector_writer_handle::Handle::Iceberg(iceberg) =
            raw.handle.as_mut().expect("variant");
        iceberg
            .data
            .as_mut()
            .expect("recipe")
            .transform_exprs
            .clear();
        let error = parse(raw).expect_err("column without a transform");
        assert_eq!(error.kind(), ProtocolErrorKind::InconsistentFields);
        assert_eq!(error.path().to_string(), "writer_handle.iceberg.data");
    }
}
