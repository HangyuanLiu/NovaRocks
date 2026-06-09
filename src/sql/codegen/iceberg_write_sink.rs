use crate::cloud_configuration::TCloudConfiguration;
use crate::data_sinks;
use crate::descriptors;
use crate::sql::catalog::{ColumnDef, IcebergTableInfo, TableDef};
use crate::types;

#[derive(Clone, Debug)]
#[allow(dead_code)]
pub(crate) struct IcebergWriteSinkSpec {
    pub target_table_id: i64,
    pub target_table: TableDef,
    pub iceberg: IcebergTableInfo,
    pub target_columns: Vec<ColumnDef>,
    pub table_location: String,
    pub data_location: String,
    pub cloud_configuration: Option<TCloudConfiguration>,
    pub file_format: String,
    pub compression: types::TCompressionType,
}

impl IcebergWriteSinkSpec {
    #[allow(dead_code)]
    pub(crate) fn build_sink(&self, tuple_id: i32) -> data_sinks::TDataSink {
        data_sinks::TDataSink::new(
            data_sinks::TDataSinkType::ICEBERG_TABLE_SINK,
            None::<data_sinks::TDataStreamSink>,
            None::<data_sinks::TResultSink>,
            None::<data_sinks::TMysqlTableSink>,
            None::<data_sinks::TExportSink>,
            None::<data_sinks::TOlapTableSink>,
            None::<data_sinks::TMemoryScratchSink>,
            None::<data_sinks::TMultiCastDataStreamSink>,
            None::<data_sinks::TSchemaTableSink>,
            Some(data_sinks::TIcebergTableSink::new(
                Some(self.table_location.clone()),
                Some(self.file_format.clone()),
                Some(self.target_table_id),
                Some(self.compression),
                Some(false),
                self.cloud_configuration.clone(),
                None::<i64>,
                Some(tuple_id),
                Some(self.data_location.clone()),
            )),
            None::<data_sinks::THiveTableSink>,
            None::<data_sinks::TTableFunctionTableSink>,
            None::<data_sinks::TDictionaryCacheSink>,
            None::<Vec<Box<data_sinks::TDataSink>>>,
            None::<i64>,
            None::<data_sinks::TSplitDataStreamSink>,
        )
    }
}

pub(crate) fn transform_to_thrift_string(transform: &iceberg::spec::Transform) -> String {
    transform.to_string()
}

#[allow(dead_code)]
pub(crate) fn partition_info_from_metadata(
    metadata: &iceberg::spec::TableMetadata,
) -> Result<Vec<descriptors::TIcebergPartitionInfo>, String> {
    let schema = metadata.current_schema();
    let spec = metadata.default_partition_spec();
    spec.fields()
        .iter()
        .map(|field| {
            let source = schema.field_by_id(field.source_id).ok_or_else(|| {
                format!(
                    "iceberg write sink partition source field id {} not found",
                    field.source_id
                )
            })?;
            Ok(descriptors::TIcebergPartitionInfo::new(
                Some(source.name.clone()),
                Some(field.name.clone()),
                Some(transform_to_thrift_string(&field.transform)),
                None::<crate::exprs::TExpr>,
            ))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn transform_to_thrift_string_matches_sink_parser_contract() {
        assert_eq!(
            transform_to_thrift_string(&iceberg::spec::Transform::Identity),
            "identity"
        );
        assert_eq!(
            transform_to_thrift_string(&iceberg::spec::Transform::Bucket(16)),
            "bucket[16]"
        );
        assert_eq!(
            transform_to_thrift_string(&iceberg::spec::Transform::Truncate(8)),
            "truncate[8]"
        );
        assert_eq!(
            transform_to_thrift_string(&iceberg::spec::Transform::Day),
            "day"
        );
    }
}
