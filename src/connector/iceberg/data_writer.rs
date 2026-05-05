use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use iceberg::arrow::{RecordBatchPartitionSplitter, schema_to_arrow_schema};
use iceberg::spec::{DataFile, DataFileBuilder, DataFileFormat};
use iceberg::writer::base_writer::data_file_writer::DataFileWriterBuilder;
use iceberg::writer::file_writer::ParquetWriterBuilder;
use iceberg::writer::file_writer::location_generator::{
    DefaultFileNameGenerator, DefaultLocationGenerator,
};
use iceberg::writer::file_writer::rolling_writer::RollingFileWriterBuilder;
use iceberg::writer::{IcebergWriter, IcebergWriterBuilder};
use parquet::file::properties::WriterProperties;

pub(crate) async fn write_record_batches_as_data_files(
    table: &iceberg::table::Table,
    batches: impl IntoIterator<Item = RecordBatch>,
) -> Result<Vec<DataFile>, String> {
    let metadata = table.metadata();
    let annotated_schema = Arc::new(
        schema_to_arrow_schema(metadata.current_schema())
            .map_err(|e| format!("convert iceberg schema to arrow failed: {e}"))?,
    );
    let data_file_builder = build_data_file_writer(table)?;

    if metadata.default_partition_spec().fields().is_empty() {
        let mut writer = data_file_builder
            .build(None)
            .await
            .map_err(|e| format!("build iceberg data file writer failed: {e}"))?;
        for batch in batches {
            if batch.num_rows() == 0 {
                continue;
            }
            writer
                .write(annotate_batch(&batch, &annotated_schema)?)
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
        let annotated = annotate_batch(&batch, &annotated_schema)?;
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

fn build_data_file_writer(
    table: &iceberg::table::Table,
) -> Result<
    DataFileWriterBuilder<ParquetWriterBuilder, DefaultLocationGenerator, DefaultFileNameGenerator>,
    String,
> {
    let location_generator = DefaultLocationGenerator::new(table.metadata().clone())
        .map_err(|e| format!("build iceberg location generator failed: {e}"))?;
    let file_name_generator = DefaultFileNameGenerator::new(
        "novarocks".to_string(),
        Some(unique_file_suffix()),
        DataFileFormat::Parquet,
    );
    let parquet_builder = ParquetWriterBuilder::new(
        WriterProperties::default(),
        table.metadata().current_schema().clone(),
    );
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
    use iceberg::spec::{DataContentType, DataFileBuilder, DataFileFormat, Struct};

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
}
