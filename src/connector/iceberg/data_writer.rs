use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use iceberg::arrow::{RecordBatchPartitionSplitter, schema_to_arrow_schema};
use iceberg::spec::{DataFile, DataFileFormat};
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
        return writer
            .close()
            .await
            .map_err(|e| format!("iceberg data file writer close failed: {e}"));
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
