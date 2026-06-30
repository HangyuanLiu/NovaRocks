use std::collections::BTreeMap;

use crate::common::engine_error::EngineError;
use crate::connector::iceberg::delete_file::IcebergFileContent;
use crate::connector::iceberg::report::{IcebergColumnStats, IcebergWriterReport};
use crate::connector::iceberg::write_descriptor::{
    IcebergPartitionDescriptor, IcebergPartitionValueDescriptor, IcebergWriteDescriptorError,
    encode_partition_descriptor,
};
use crate::thrift::types;

pub(crate) fn partition_descriptor_to_thrift(
    desc: IcebergPartitionDescriptor,
) -> types::TIcebergPartitionDescriptor {
    types::TIcebergPartitionDescriptor {
        values: Some(
            desc.values
                .into_iter()
                .map(|value| types::TIcebergPartitionValue {
                    is_null: Some(value.is_null),
                    datum_bytes: value.datum_bytes,
                })
                .collect(),
        ),
    }
}

pub(crate) fn partition_descriptor_from_thrift(
    desc: Option<types::TIcebergPartitionDescriptor>,
) -> Result<Option<IcebergPartitionDescriptor>, IcebergWriteDescriptorError> {
    let Some(desc) = desc else {
        return Ok(None);
    };
    let values = desc
        .values
        .unwrap_or_default()
        .into_iter()
        .enumerate()
        .map(|(idx, value)| {
            let is_null =
                value
                    .is_null
                    .ok_or_else(|| IcebergWriteDescriptorError::DecodeFailed {
                        index: idx,
                        message: "partition descriptor value is missing null marker".to_string(),
                    })?;
            Ok(IcebergPartitionValueDescriptor {
                is_null,
                datum_bytes: value.datum_bytes,
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(Some(IcebergPartitionDescriptor { values }))
}

pub(crate) fn writer_report_to_sink_commit_info(
    report: IcebergWriterReport,
    metadata: &iceberg::spec::TableMetadata,
) -> Result<types::TSinkCommitInfo, String> {
    let partition_values_descriptor = partition_descriptor_to_thrift(
        encode_partition_descriptor(
            &report.file.partition.partition_values,
            report.file.partition.partition_spec_id,
            metadata,
        )
        .map_err(|e| EngineError::from(e).to_bracketed_user_message())?,
    );
    Ok(types::TSinkCommitInfo {
        iceberg_data_file: Some(types::TIcebergDataFile {
            path: Some(report.file.path),
            format: Some(report.file.format),
            record_count: Some(report.file.record_count),
            file_size_in_bytes: Some(report.file.file_size_in_bytes),
            partition_path: Some(report.file.partition.partition_path),
            split_offsets: report.file.split_offsets,
            column_stats: report.file.column_stats.and_then(column_stats_to_thrift),
            partition_null_fingerprint: Some(report.file.partition.null_fingerprint),
            file_content: Some(file_content_to_thrift(report.file.content)),
            referenced_data_file: report.file.referenced_data_file,
            first_row_id: report.file.first_row_id,
            equality_ids: report.file.equality_ids,
            key_metadata: report.file.key_metadata,
            partition_values_descriptor: Some(partition_values_descriptor),
            partition_spec_id: Some(report.file.partition.partition_spec_id),
            content_offset: report.file.content_offset,
            content_size_in_bytes: report.file.content_size_in_bytes,
            cardinality: report.file.cardinality,
        }),
        hive_file_info: None,
        is_overwrite: report.is_overwrite,
        staging_dir: None,
        is_rewrite: report.is_rewrite,
    })
}

fn column_stats_to_thrift(stats: IcebergColumnStats) -> Option<types::TIcebergColumnStats> {
    if stats.is_empty() {
        return None;
    }
    Some(types::TIcebergColumnStats {
        column_sizes: non_empty(stats.column_sizes),
        value_counts: non_empty(stats.value_counts),
        null_value_counts: non_empty(stats.null_value_counts),
        nan_value_counts: non_empty(stats.nan_value_counts),
        lower_bounds: non_empty(stats.lower_bounds),
        upper_bounds: non_empty(stats.upper_bounds),
    })
}

fn non_empty<K: Ord, V>(map: BTreeMap<K, V>) -> Option<BTreeMap<K, V>> {
    (!map.is_empty()).then_some(map)
}

fn file_content_to_thrift(content: IcebergFileContent) -> types::TIcebergFileContent {
    match content {
        IcebergFileContent::Data => types::TIcebergFileContent::DATA,
        IcebergFileContent::PositionDeletes => types::TIcebergFileContent::POSITION_DELETES,
        IcebergFileContent::EqualityDeletes => types::TIcebergFileContent::EQUALITY_DELETES,
    }
}

#[cfg(test)]
pub(crate) fn expected_file_content_for_test(
    content: IcebergFileContent,
) -> types::TIcebergFileContent {
    file_content_to_thrift(content)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::connector::iceberg::delete_file::IcebergFileContent;
    use crate::connector::iceberg::report::{
        IcebergColumnStats, IcebergPartitionReport, IcebergWriterReport, IcebergWrittenFileReport,
    };
    use iceberg::TableCreation;
    use iceberg::spec::{
        FormatVersion, NestedField, PartitionSpec, PrimitiveType, Schema, Struct, TableMetadata,
        TableMetadataBuilder, Type,
    };
    use std::sync::Arc;

    fn test_unpartitioned_metadata() -> TableMetadata {
        let schema = Schema::builder()
            .with_schema_id(1)
            .with_fields(vec![Arc::new(NestedField::required(
                1,
                "id",
                Type::Primitive(PrimitiveType::Int),
            ))])
            .build()
            .expect("schema");
        let creation = TableCreation::builder()
            .name("t".to_string())
            .location("file:///warehouse/db/t".to_string())
            .schema(schema)
            .partition_spec(PartitionSpec::unpartition_spec())
            .format_version(FormatVersion::V2)
            .build();
        TableMetadataBuilder::from_table_creation(creation)
            .expect("table metadata builder")
            .build()
            .expect("table metadata")
            .metadata
    }

    #[test]
    fn partition_descriptor_from_thrift_rejects_missing_null_marker() {
        let desc = types::TIcebergPartitionDescriptor {
            values: Some(vec![types::TIcebergPartitionValue {
                is_null: None,
                datum_bytes: Some(b"west".to_vec()),
            }]),
        };

        let err =
            partition_descriptor_from_thrift(Some(desc)).expect_err("expected missing null marker");

        assert_eq!(err.code(), "IcebergWriteDescriptorMismatch");
        assert!(
            err.to_string().contains("null marker"),
            "missing null marker should be rejected, got: {err}"
        );
    }

    #[test]
    fn writer_report_to_sink_commit_info_encodes_file_content() {
        let metadata = test_unpartitioned_metadata();
        let report = IcebergWriterReport {
            file: IcebergWrittenFileReport {
                path: "file:///warehouse/t/delete-1.parquet".to_string(),
                format: "parquet".to_string(),
                content: IcebergFileContent::PositionDeletes,
                record_count: 2,
                file_size_in_bytes: 128,
                partition: IcebergPartitionReport {
                    partition_path: String::new(),
                    null_fingerprint: String::new(),
                    partition_spec_id: metadata.default_partition_spec_id(),
                    partition_values: Struct::empty(),
                },
                split_offsets: Some(vec![4]),
                column_stats: None,
                referenced_data_file: Some("file:///warehouse/t/data-1.parquet".to_string()),
                first_row_id: None,
                equality_ids: None,
                key_metadata: None,
                content_offset: None,
                content_size_in_bytes: None,
                cardinality: None,
            },
            is_overwrite: None,
            is_rewrite: None,
        };

        let info = writer_report_to_sink_commit_info(report, &metadata).expect("wire encode");
        let df = info.iceberg_data_file.expect("iceberg data file");

        assert_eq!(
            df.file_content,
            Some(crate::thrift::types::TIcebergFileContent::POSITION_DELETES)
        );
        assert_eq!(
            df.referenced_data_file.as_deref(),
            Some("file:///warehouse/t/data-1.parquet")
        );
        assert_eq!(
            df.partition_values_descriptor
                .expect("descriptor")
                .values
                .expect("values")
                .len(),
            0
        );
    }

    #[test]
    fn empty_column_stats_omit_thrift_stats() {
        assert!(column_stats_to_thrift(IcebergColumnStats::default()).is_none());
    }

    #[test]
    fn column_stats_to_thrift_filters_empty_maps() {
        let mut stats = IcebergColumnStats::default();
        stats.column_sizes.insert(1, 10);

        let thrift = column_stats_to_thrift(stats).expect("thrift stats");

        assert_eq!(
            thrift.column_sizes.expect("column sizes").get(&1),
            Some(&10)
        );
        assert!(thrift.value_counts.is_none());
        assert!(thrift.null_value_counts.is_none());
        assert!(thrift.nan_value_counts.is_none());
        assert!(thrift.lower_bounds.is_none());
        assert!(thrift.upper_bounds.is_none());
    }

    #[test]
    fn column_stats_to_thrift_preserves_nan_value_counts() {
        let mut stats = IcebergColumnStats::default();
        stats.nan_value_counts.insert(1, 2);

        let thrift = column_stats_to_thrift(stats).expect("thrift stats");

        assert_eq!(
            thrift.nan_value_counts.expect("nan value counts").get(&1),
            Some(&2)
        );
        assert!(thrift.column_sizes.is_none());
        assert!(thrift.value_counts.is_none());
        assert!(thrift.null_value_counts.is_none());
        assert!(thrift.lower_bounds.is_none());
        assert!(thrift.upper_bounds.is_none());
    }

    #[test]
    fn file_content_to_thrift_maps_domain_content() {
        assert_eq!(
            file_content_to_thrift(IcebergFileContent::Data),
            types::TIcebergFileContent::DATA
        );
        assert_eq!(
            file_content_to_thrift(IcebergFileContent::PositionDeletes),
            types::TIcebergFileContent::POSITION_DELETES
        );
        assert_eq!(
            file_content_to_thrift(IcebergFileContent::EqualityDeletes),
            types::TIcebergFileContent::EQUALITY_DELETES
        );
    }
}
