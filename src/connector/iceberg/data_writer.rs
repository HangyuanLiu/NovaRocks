use std::sync::Arc;

use crate::exec::row_position::{
    ICEBERG_LAST_UPDATED_SEQ_COL, ICEBERG_RESERVED_FIELD_ID_LAST_UPDATED_SEQUENCE_NUMBER,
    ICEBERG_RESERVED_FIELD_ID_ROW_ID, ICEBERG_ROW_ID_COL,
};
use arrow::array::Array;
use arrow::record_batch::RecordBatch;
use iceberg::arrow::{RecordBatchPartitionSplitter, schema_to_arrow_schema};
use iceberg::spec::{
    DataFile, DataFileBuilder, DataFileFormat, NestedField, PrimitiveType, SchemaRef, Type,
};
use iceberg::writer::base_writer::data_file_writer::DataFileWriterBuilder;
use iceberg::writer::file_writer::ParquetWriterBuilder;
use iceberg::writer::file_writer::location_generator::{
    DefaultFileNameGenerator, DefaultLocationGenerator,
};
use iceberg::writer::file_writer::rolling_writer::RollingFileWriterBuilder;
use iceberg::writer::{IcebergWriter, IcebergWriterBuilder};
use parquet::file::properties::WriterProperties;

use super::variant_write::{transform_variant_columns_for_write, variant_field_indices};

type IcebergDataFileWriterBuilder =
    DataFileWriterBuilder<ParquetWriterBuilder, DefaultLocationGenerator, DefaultFileNameGenerator>;

#[derive(Clone, Debug)]
pub(crate) struct RowLineageColumns {
    pub row_ids: arrow::array::Int64Array,
    pub last_updated_sequence_numbers: arrow::array::Int64Array,
}

#[derive(Clone, Debug)]
#[allow(dead_code)]
pub(crate) struct RowLineageWriteBatch {
    pub user_batch: arrow::record_batch::RecordBatch,
    pub lineage: RowLineageColumns,
}

pub(crate) async fn write_record_batches_as_data_files(
    table: &iceberg::table::Table,
    batches: impl IntoIterator<Item = RecordBatch>,
) -> Result<Vec<DataFile>, String> {
    let metadata = table.metadata();
    let writer_schema = metadata.current_schema().clone();
    let annotated_schema = Arc::new(
        schema_to_arrow_schema(&writer_schema)
            .map_err(|e| format!("convert iceberg schema to arrow failed: {e}"))?,
    );
    let data_file_builder = build_data_file_writer(table)?;
    write_record_batches_as_data_files_with_writer(
        table,
        batches,
        data_file_builder,
        annotated_schema,
    )
    .await
}

async fn write_record_batches_as_data_files_with_schema(
    table: &iceberg::table::Table,
    batches: impl IntoIterator<Item = RecordBatch>,
    writer_schema: SchemaRef,
    annotated_schema: arrow::datatypes::SchemaRef,
) -> Result<Vec<DataFile>, String> {
    let data_file_builder = build_data_file_writer_with_schema(table, writer_schema)?;
    write_record_batches_as_data_files_with_writer(
        table,
        batches,
        data_file_builder,
        annotated_schema,
    )
    .await
}

async fn write_record_batches_as_data_files_with_writer(
    table: &iceberg::table::Table,
    batches: impl IntoIterator<Item = RecordBatch>,
    data_file_builder: IcebergDataFileWriterBuilder,
    annotated_schema: arrow::datatypes::SchemaRef,
) -> Result<Vec<DataFile>, String> {
    let metadata = table.metadata();
    let variant_indices = variant_field_indices(metadata.current_schema());

    if metadata.default_partition_spec().fields().is_empty() {
        let mut writer = data_file_builder
            .build(None)
            .await
            .map_err(|e| format!("build iceberg data file writer failed: {e}"))?;
        for batch in batches {
            if batch.num_rows() == 0 {
                continue;
            }
            let staged = if variant_indices.is_empty() {
                batch
            } else {
                transform_variant_columns_for_write(&batch, &annotated_schema, &variant_indices)?
            };
            writer
                .write(annotate_batch(&staged, &annotated_schema)?)
                .await
                .map_err(|e| format!("iceberg data file write failed: {e}"))?;
        }
        let data_files = writer
            .close()
            .await
            .map_err(|e| format!("iceberg data file writer close failed: {e}"))?;
        return data_files
            .into_iter()
            .map(|data_file| {
                retag_data_file_partition_spec_id(data_file, metadata.default_partition_spec_id())
            })
            .collect();
    }

    let splitter = RecordBatchPartitionSplitter::try_new_with_computed_values(
        metadata.current_schema().clone(),
        metadata.default_partition_spec().clone(),
    )
    .map_err(|e| format!("build iceberg partition splitter failed: {e}"))?;
    let mut data_files = Vec::new();
    for batch in batches {
        if batch.num_rows() == 0 {
            continue;
        }
        let staged = if variant_indices.is_empty() {
            batch
        } else {
            transform_variant_columns_for_write(&batch, &annotated_schema, &variant_indices)?
        };
        let annotated = annotate_batch(&staged, &annotated_schema)?;
        let partitioned = splitter
            .split(&annotated)
            .map_err(|e| format!("split iceberg batch by partition spec failed: {e}"))?;
        for (partition_key, partition_batch) in partitioned {
            let mut writer = data_file_builder
                .build(Some(partition_key))
                .await
                .map_err(|e| format!("build iceberg partitioned data file writer failed: {e}"))?;
            writer
                .write(partition_batch)
                .await
                .map_err(|e| format!("iceberg partitioned data file write failed: {e}"))?;
            data_files.extend(
                writer.close().await.map_err(|e| {
                    format!("iceberg partitioned data file writer close failed: {e}")
                })?,
            );
        }
    }
    Ok(data_files)
}

fn retag_data_file_partition_spec_id(
    data_file: DataFile,
    partition_spec_id: i32,
) -> Result<DataFile, String> {
    let mut builder = DataFileBuilder::default();
    builder
        .content(data_file.content_type())
        .file_path(data_file.file_path().to_string())
        .file_format(data_file.file_format())
        .partition(data_file.partition().clone())
        .partition_spec_id(partition_spec_id)
        .record_count(data_file.record_count())
        .file_size_in_bytes(data_file.file_size_in_bytes());

    if !data_file.column_sizes().is_empty() {
        builder.column_sizes(data_file.column_sizes().clone());
    }
    if !data_file.value_counts().is_empty() {
        builder.value_counts(data_file.value_counts().clone());
    }
    if !data_file.null_value_counts().is_empty() {
        builder.null_value_counts(data_file.null_value_counts().clone());
    }
    if !data_file.nan_value_counts().is_empty() {
        builder.nan_value_counts(data_file.nan_value_counts().clone());
    }
    if !data_file.lower_bounds().is_empty() {
        builder.lower_bounds(data_file.lower_bounds().clone());
    }
    if !data_file.upper_bounds().is_empty() {
        builder.upper_bounds(data_file.upper_bounds().clone());
    }
    if let Some(key_metadata) = data_file.key_metadata() {
        builder.key_metadata(Some(key_metadata.to_vec()));
    }
    if let Some(split_offsets) = data_file.split_offsets() {
        builder.split_offsets(Some(split_offsets.to_vec()));
    }
    if let Some(equality_ids) = data_file.equality_ids() {
        builder.equality_ids(Some(equality_ids));
    }
    if let Some(sort_order_id) = data_file.sort_order_id() {
        builder.sort_order_id(sort_order_id);
    }
    builder
        .first_row_id(data_file.first_row_id())
        .referenced_data_file(data_file.referenced_data_file())
        .content_offset(data_file.content_offset())
        .content_size_in_bytes(data_file.content_size_in_bytes());

    builder.build().map_err(|e| {
        format!("failed to retag iceberg data file with partition spec id {partition_spec_id}: {e}")
    })
}

#[allow(dead_code)]
pub(crate) async fn write_row_lineage_batches_as_data_files(
    table: &iceberg::table::Table,
    batches: &[RowLineageWriteBatch],
) -> Result<Vec<iceberg::spec::DataFile>, String> {
    let writer_schema = build_row_lineage_writer_schema(table.metadata().current_schema())?;
    let annotated_schema = Arc::new(
        schema_to_arrow_schema(&writer_schema)
            .map_err(|e| format!("convert row-lineage iceberg schema to arrow failed: {e}"))?,
    );
    let mut enriched = Vec::with_capacity(batches.len());
    for batch in batches {
        enriched.push(append_row_lineage_columns(
            &batch.user_batch,
            batch.lineage.clone(),
        )?);
    }
    write_record_batches_as_data_files_with_schema(table, enriched, writer_schema, annotated_schema)
        .await
}

fn build_row_lineage_writer_schema(current_schema: &SchemaRef) -> Result<SchemaRef, String> {
    Ok(Arc::new(
        current_schema
            .as_ref()
            .clone()
            .into_builder()
            .with_fields(vec![
                NestedField::required(
                    ICEBERG_RESERVED_FIELD_ID_ROW_ID,
                    ICEBERG_ROW_ID_COL,
                    Type::Primitive(PrimitiveType::Long),
                )
                .into(),
                NestedField::optional(
                    ICEBERG_RESERVED_FIELD_ID_LAST_UPDATED_SEQUENCE_NUMBER,
                    ICEBERG_LAST_UPDATED_SEQ_COL,
                    Type::Primitive(PrimitiveType::Long),
                )
                .into(),
            ])
            .build()
            .map_err(|e| format!("build row-lineage iceberg schema failed: {e}"))?,
    ))
}

pub(crate) fn append_row_lineage_columns(
    batch: &arrow::record_batch::RecordBatch,
    lineage: RowLineageColumns,
) -> Result<arrow::record_batch::RecordBatch, String> {
    use arrow::array::ArrayRef;
    use arrow::datatypes::{DataType, Field, Schema};
    use parquet::arrow::PARQUET_FIELD_ID_META_KEY;
    use std::collections::HashMap;
    use std::sync::Arc;

    if batch.num_rows() != lineage.row_ids.len()
        || batch.num_rows() != lineage.last_updated_sequence_numbers.len()
    {
        return Err(format!(
            "row-lineage column length mismatch: rows={}, row_ids={}, last_updated={}",
            batch.num_rows(),
            lineage.row_ids.len(),
            lineage.last_updated_sequence_numbers.len()
        ));
    }
    for row in 0..lineage.row_ids.len() {
        if lineage.row_ids.is_null(row) {
            return Err(format!(
                "row-lineage {ICEBERG_ROW_ID_COL} column contains null at row {row}"
            ));
        }
        let row_id = lineage.row_ids.value(row);
        if row_id < 0 {
            return Err(format!(
                "row-lineage {ICEBERG_ROW_ID_COL} column must be non-negative: row={row}, value={row_id}"
            ));
        }
    }

    let mut fields = batch.schema().fields().iter().cloned().collect::<Vec<_>>();
    fields.push(Arc::new(
        Field::new(ICEBERG_ROW_ID_COL, DataType::Int64, false).with_metadata(HashMap::from([(
            PARQUET_FIELD_ID_META_KEY.to_string(),
            ICEBERG_RESERVED_FIELD_ID_ROW_ID.to_string(),
        )])),
    ));
    fields.push(Arc::new(
        Field::new(ICEBERG_LAST_UPDATED_SEQ_COL, DataType::Int64, true).with_metadata(
            HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                ICEBERG_RESERVED_FIELD_ID_LAST_UPDATED_SEQUENCE_NUMBER.to_string(),
            )]),
        ),
    ));

    let mut columns = batch.columns().to_vec();
    columns.push(Arc::new(lineage.row_ids) as ArrayRef);
    columns.push(Arc::new(lineage.last_updated_sequence_numbers) as ArrayRef);
    arrow::record_batch::RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)
        .map_err(|e| format!("build row-lineage record batch failed: {e}"))
}

fn build_data_file_writer(
    table: &iceberg::table::Table,
) -> Result<IcebergDataFileWriterBuilder, String> {
    build_data_file_writer_with_schema(table, table.metadata().current_schema().clone())
}

fn build_data_file_writer_with_schema(
    table: &iceberg::table::Table,
    schema: SchemaRef,
) -> Result<IcebergDataFileWriterBuilder, String> {
    let location_generator = DefaultLocationGenerator::new(table.metadata().clone())
        .map_err(|e| format!("build iceberg location generator failed: {e}"))?;
    let file_name_generator = DefaultFileNameGenerator::new(
        "novarocks".to_string(),
        Some(unique_file_suffix()),
        DataFileFormat::Parquet,
    );
    let parquet_builder = ParquetWriterBuilder::new(WriterProperties::default(), schema);
    let rolling_builder = RollingFileWriterBuilder::new_with_default_file_size(
        parquet_builder,
        table.file_io().clone(),
        location_generator,
        file_name_generator,
    );
    Ok(DataFileWriterBuilder::new(rolling_builder))
}

fn annotate_batch(
    batch: &RecordBatch,
    annotated_schema: &arrow::datatypes::SchemaRef,
) -> Result<RecordBatch, String> {
    RecordBatch::try_new(Arc::clone(annotated_schema), batch.columns().to_vec())
        .map_err(|e| format!("re-annotate batch with iceberg field ids failed: {e}"))
}

fn unique_file_suffix() -> String {
    use rand::Rng;

    let mut rng = rand::thread_rng();
    let mut bytes = [0_u8; 16];
    rng.fill(&mut bytes);
    bytes[6] = (bytes[6] & 0x0f) | 0x40;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    format!(
        "{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}",
        bytes[0],
        bytes[1],
        bytes[2],
        bytes[3],
        bytes[4],
        bytes[5],
        bytes[6],
        bytes[7],
        bytes[8],
        bytes[9],
        bytes[10],
        bytes[11],
        bytes[12],
        bytes[13],
        bytes[14],
        bytes[15],
    )
}

#[cfg(test)]
mod tests {
    use iceberg::spec::{DataContentType, Struct};

    use super::*;

    #[test]
    fn retag_unpartitioned_data_file_with_current_default_spec_id() {
        let mut builder = DataFileBuilder::default();
        builder
            .content(DataContentType::Data)
            .file_path("file:///tmp/data.parquet".to_string())
            .file_format(DataFileFormat::Parquet)
            .partition(Struct::empty())
            .partition_spec_id(0)
            .record_count(1)
            .file_size_in_bytes(128);
        let data_file = builder.build().unwrap();

        let data_file = retag_data_file_partition_spec_id(data_file, 7).unwrap();

        assert!(
            format!("{data_file:?}").contains("partition_spec_id: 7"),
            "retagged data file should carry the evolved default partition spec id"
        );
    }

    #[test]
    fn append_row_lineage_columns_sets_reserved_field_ids() {
        use arrow::array::{Int64Array, StringArray};
        use arrow::datatypes::{DataType, Field, Schema};
        use parquet::arrow::PARQUET_FIELD_ID_META_KEY;
        use std::sync::Arc;

        let batch = arrow::record_batch::RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("v", DataType::Utf8, true)])),
            vec![Arc::new(StringArray::from(vec!["a", "b"]))],
        )
        .expect("batch");
        let out = append_row_lineage_columns(
            &batch,
            RowLineageColumns {
                row_ids: Int64Array::from(vec![10, 11]),
                last_updated_sequence_numbers: Int64Array::from(vec![None, Some(3)]),
            },
        )
        .expect("append");
        assert_eq!(out.num_columns(), 3);
        assert_eq!(out.schema().field(1).name(), "_row_id");
        assert!(!out.schema().field(1).is_nullable());
        assert_eq!(
            out.schema()
                .field(1)
                .metadata()
                .get(PARQUET_FIELD_ID_META_KEY)
                .map(String::as_str),
            Some("2147483540")
        );
        assert_eq!(
            out.schema().field(2).name(),
            "_last_updated_sequence_number"
        );
        assert!(out.schema().field(2).is_nullable());
        assert_eq!(
            out.schema()
                .field(2)
                .metadata()
                .get(PARQUET_FIELD_ID_META_KEY)
                .map(String::as_str),
            Some("2147483539")
        );
    }

    #[test]
    fn append_row_lineage_columns_rejects_length_mismatch() {
        use arrow::array::{Int64Array, StringArray};
        use arrow::datatypes::{DataType, Field, Schema};
        use std::sync::Arc;

        let batch = arrow::record_batch::RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("v", DataType::Utf8, true)])),
            vec![Arc::new(StringArray::from(vec!["a", "b"]))],
        )
        .expect("batch");
        let err = append_row_lineage_columns(
            &batch,
            RowLineageColumns {
                row_ids: Int64Array::from(vec![10]),
                last_updated_sequence_numbers: Int64Array::from(vec![None, Some(3)]),
            },
        )
        .expect_err("length mismatch");

        assert_eq!(
            err,
            "row-lineage column length mismatch: rows=2, row_ids=1, last_updated=2"
        );
    }

    #[test]
    fn append_row_lineage_columns_rejects_null_row_ids() {
        use arrow::array::{Int64Array, StringArray};
        use arrow::datatypes::{DataType, Field, Schema};
        use std::sync::Arc;

        let batch = arrow::record_batch::RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("v", DataType::Utf8, true)])),
            vec![Arc::new(StringArray::from(vec!["a", "b"]))],
        )
        .expect("batch");
        let err = append_row_lineage_columns(
            &batch,
            RowLineageColumns {
                row_ids: Int64Array::from(vec![Some(10), None]),
                last_updated_sequence_numbers: Int64Array::from(vec![None, Some(3)]),
            },
        )
        .expect_err("null row id");

        assert_eq!(err, "row-lineage _row_id column contains null at row 1");
    }

    #[test]
    fn append_row_lineage_columns_rejects_negative_row_ids() {
        use arrow::array::{Int64Array, StringArray};
        use arrow::datatypes::{DataType, Field, Schema};
        use std::sync::Arc;

        let batch = arrow::record_batch::RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("v", DataType::Utf8, true)])),
            vec![Arc::new(StringArray::from(vec!["a", "b"]))],
        )
        .expect("batch");
        let err = append_row_lineage_columns(
            &batch,
            RowLineageColumns {
                row_ids: Int64Array::from(vec![10, -1]),
                last_updated_sequence_numbers: Int64Array::from(vec![None, Some(3)]),
            },
        )
        .expect_err("negative row id");

        assert_eq!(
            err,
            "row-lineage _row_id column must be non-negative: row=1, value=-1"
        );
    }

    #[test]
    fn enriched_row_lineage_columns_reannotate_with_extended_schema() {
        use arrow::array::{Int64Array, StringArray};
        use arrow::datatypes::{DataType, Field, Schema};
        use iceberg::spec::{NestedField, PrimitiveType, Type};
        use std::sync::Arc;

        let iceberg_schema = Arc::new(
            iceberg::spec::Schema::builder()
                .with_schema_id(7)
                .with_fields(vec![
                    NestedField::optional(1, "v", Type::Primitive(PrimitiveType::String)).into(),
                ])
                .build()
                .expect("schema"),
        );
        let batch = arrow::record_batch::RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("v", DataType::Utf8, true)])),
            vec![Arc::new(StringArray::from(vec!["a", "b"]))],
        )
        .expect("batch");
        let enriched = append_row_lineage_columns(
            &batch,
            RowLineageColumns {
                row_ids: Int64Array::from(vec![10, 11]),
                last_updated_sequence_numbers: Int64Array::from(vec![None, Some(3)]),
            },
        )
        .expect("append");
        let extended_schema = build_row_lineage_writer_schema(&iceberg_schema).expect("schema");
        let annotated_schema = Arc::new(schema_to_arrow_schema(&extended_schema).expect("arrow"));
        let annotated = annotate_batch(&enriched, &annotated_schema).expect("annotate");

        assert_eq!(annotated.num_columns(), 3);
        assert_eq!(annotated.schema().field(1).name(), "_row_id");
        assert_eq!(
            annotated.schema().field(2).name(),
            "_last_updated_sequence_number"
        );
    }

    #[tokio::test]
    async fn write_variant_column_round_trips_through_local_parquet() {
        use arrow::array::{Int32Array, LargeBinaryArray};
        use arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
        use iceberg::spec::{NestedField, PrimitiveType, Type};
        use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
        use std::fs::File;
        use std::sync::Arc;
        use tempfile::tempdir;

        let dir = tempdir().expect("tempdir");
        let location = format!("file://{}", dir.path().display());

        let iceberg_schema = Arc::new(
            iceberg::spec::Schema::builder()
                .with_schema_id(1)
                .with_fields(vec![
                    NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
                    NestedField::optional(2, "v", Type::Primitive(PrimitiveType::Variant)).into(),
                ])
                .build()
                .expect("schema"),
        );
        let metadata = iceberg::spec::TableMetadataBuilder::new(
            iceberg_schema.as_ref().clone(),
            iceberg::spec::PartitionSpec::unpartition_spec(),
            iceberg::spec::SortOrder::unsorted_order(),
            location.clone(),
            iceberg::spec::FormatVersion::V3,
            std::collections::HashMap::new(),
        )
        .expect("builder")
        .build()
        .expect("metadata")
        .metadata;
        let table = iceberg::table::Table::builder()
            .identifier(iceberg::TableIdent::from_strs(["db", "t"]).unwrap())
            .file_io(iceberg::io::FileIO::new_with_fs())
            .metadata(metadata)
            .build()
            .expect("table");

        // Build a 1-row record batch where `v` holds a serialized variant
        // (short string "hello").
        let payload = {
            let metadata = vec![0x01u8, 0x00, 0x00];
            let mut value = Vec::new();
            let s = b"hello";
            value.push(((s.len() as u8) << 2) | 0b01);
            value.extend_from_slice(s);
            let total = (metadata.len() + value.len()) as u32;
            let mut out = Vec::new();
            out.extend_from_slice(&total.to_le_bytes());
            out.extend_from_slice(&metadata);
            out.extend_from_slice(&value);
            out
        };
        let input_schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("v", DataType::LargeBinary, true),
        ]));
        let batch = RecordBatch::try_new(
            input_schema,
            vec![
                Arc::new(Int32Array::from(vec![1])),
                Arc::new(LargeBinaryArray::from_iter_values([payload.as_slice()])),
            ],
        )
        .expect("batch");

        let data_files = write_record_batches_as_data_files(&table, vec![batch])
            .await
            .expect("write");
        assert_eq!(data_files.len(), 1);
        let path = data_files[0].file_path().to_string();
        let on_disk = path.strip_prefix("file://").unwrap_or(&path);

        // Re-open the parquet file with the standard parquet-rs reader and
        // assert the physical layout matches the spec.
        let f = File::open(on_disk).expect("open parquet");
        let builder = ParquetRecordBatchReaderBuilder::try_new(f).expect("builder");
        let parquet_schema = builder.parquet_schema();
        let v_node = parquet_schema
            .columns()
            .iter()
            .find(|c| c.path().string().starts_with("v"))
            .expect("v column");
        assert!(
            v_node.path().string() == "v.metadata" || v_node.path().string() == "v.value",
            "expected leaf path under v.*; got {}",
            v_node.path().string()
        );
        // Look at the parent group's logical type via the parquet schema descr.
        let root = builder.parquet_schema().root_schema();
        let v_field = root
            .get_fields()
            .iter()
            .find(|f| f.name() == "v")
            .expect("v");
        assert!(
            format!("{:?}", v_field.get_basic_info().logical_type_ref())
                .to_lowercase()
                .contains("variant"),
            "v parent group must carry LogicalType::Variant; got {:?}",
            v_field.get_basic_info().logical_type_ref()
        );
    }
}
