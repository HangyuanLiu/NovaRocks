use crate::cloud_configuration::TCloudConfiguration;
use crate::data_sinks;
use crate::descriptors;
use crate::sql::catalog::{ColumnDef, IcebergTableInfo, TableDef};
use crate::types;

#[derive(Clone, Debug)]
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
                Some(source_column_slot_ref_placeholder_expr()),
            ))
        })
        .collect()
}

fn source_column_slot_ref_placeholder_expr() -> crate::exprs::TExpr {
    super::expr_compiler::build_slot_ref_texpr(
        0,
        0,
        crate::lower::type_lowering::scalar_type_desc(types::TPrimitiveType::INT),
    )
}

#[cfg(test)]
pub(crate) mod test_support {
    use arrow::datatypes::DataType;

    use super::*;
    use crate::sql::catalog::{IcebergSchemaDef, IcebergSchemaFieldDef, ScanSource};

    pub(crate) fn simple_sink_spec() -> IcebergWriteSinkSpec {
        let iceberg = IcebergTableInfo {
            catalog: "test_catalog".to_string(),
            namespace: "test_db".to_string(),
            table: "target_orders".to_string(),
            table_uuid: Some("00000000-0000-0000-0000-000000000002".to_string()),
            current_snapshot_id: Some(1),
            schema_id: 1,
            location: "file:///warehouse/target_orders".to_string(),
            schema: IcebergSchemaDef {
                fields: vec![IcebergSchemaFieldDef {
                    field_id: 1,
                    name: "id".to_string(),
                    initial_default: None,
                    write_default: None,
                    initial_default_json: None,
                    children: Vec::new(),
                }],
            },
            serialized_metadata: None,
            serialized_metadata_rows: None,
        };
        let target_table = TableDef {
            name: "target_orders".to_string(),
            columns: vec![ColumnDef {
                name: "id".to_string(),
                data_type: DataType::Int32,
                nullable: false,
                write_default: None,
                logical_type: None,
            }],
            iceberg_row_lineage_metadata_columns: Vec::new(),
            source: ScanSource::IcebergDataFiles {
                table: iceberg.clone(),
                files: Vec::new(),
                cloud_properties: Default::default(),
                binding: crate::sql::catalog::IcebergDataFileBinding::CurrentSnapshot,
            },
        };

        IcebergWriteSinkSpec {
            target_table_id: 99,
            target_table,
            iceberg,
            target_columns: vec![ColumnDef {
                name: "id".to_string(),
                data_type: DataType::Int32,
                nullable: false,
                write_default: None,
                logical_type: None,
            }],
            table_location: "file:///warehouse/target_orders".to_string(),
            data_location: "file:///warehouse/target_orders/data".to_string(),
            cloud_configuration: None,
            file_format: "parquet".to_string(),
            compression: types::TCompressionType::SNAPPY,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

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

    #[test]
    fn partition_info_from_metadata_includes_slot_ref_placeholder_expr() {
        let schema = iceberg::spec::Schema::builder()
            .with_fields(vec![Arc::new(iceberg::spec::NestedField::required(
                1,
                "id",
                iceberg::spec::Type::Primitive(iceberg::spec::PrimitiveType::Int),
            ))])
            .build()
            .expect("schema");
        let partition_spec = iceberg::spec::PartitionSpec::builder(schema.clone())
            .add_partition_field("id", "id", iceberg::spec::Transform::Identity)
            .expect("partition field")
            .build()
            .expect("partition spec");
        let metadata = iceberg::spec::TableMetadataBuilder::new(
            schema,
            partition_spec,
            iceberg::spec::SortOrder::unsorted_order(),
            "file:///warehouse/orders".to_string(),
            iceberg::spec::FormatVersion::V3,
            std::collections::HashMap::new(),
        )
        .expect("metadata builder")
        .build()
        .expect("metadata");
        let metadata = metadata.metadata;

        let partition_info = partition_info_from_metadata(&metadata).expect("partition info");

        assert_eq!(partition_info.len(), 1);
        let expr = partition_info[0]
            .partition_expr
            .as_ref()
            .expect("partition expr");
        assert_eq!(expr.nodes.len(), 1);
        assert_eq!(
            expr.nodes[0].node_type,
            crate::exprs::TExprNodeType::SLOT_REF
        );
        assert!(expr.nodes[0].slot_ref.is_some());
    }
}
