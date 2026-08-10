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

use arrow::array::{BooleanArray, RecordBatch};
use arrow::compute::filter_record_batch;

use crate::connector::file_execution::read_foundation_parquet_batches;
use novarocks_connector_iceberg::delete_file::{
    IcebergDeleteFileSpec, IcebergFileContent, IcebergFileFormat,
};
use novarocks_fs::{FileProjection, FsAccessHandle};

use novarocks_connector_iceberg::file_reader::equality_delete::{
    EqualityDeleteSet, equality_delete_keep_mask, equality_delete_set_from_record_batches,
};

pub(crate) fn load_equality_delete_sets(
    specs: &[IcebergDeleteFileSpec],
    access: &FsAccessHandle,
) -> Result<Vec<EqualityDeleteSet>, String> {
    let mut sets = Vec::new();
    for spec in specs {
        if spec.file_content != IcebergFileContent::EqualityDeletes {
            continue;
        }
        if spec.file_format != IcebergFileFormat::Parquet {
            return Err(format!(
                "iceberg equality-delete file {} has unsupported format {:?}; only PARQUET is supported",
                spec.path, spec.file_format
            ));
        }
        if spec.content_offset.is_some() || spec.content_size_in_bytes.is_some() {
            return Err(format!(
                "iceberg equality-delete file {} must not carry Puffin content offsets",
                spec.path
            ));
        }
        let batches =
            read_foundation_parquet_batches(access, &spec.path, spec.length, FileProjection::All)?;
        if batches.is_empty() {
            continue;
        }
        sets.push(equality_delete_set_from_record_batches(
            &spec.path, batches,
        )?);
    }
    Ok(sets)
}

/// Legacy Core-only IVM reverse projection.  It owns older `FileScanContext`
/// construction and calls the provider for all equality semantics.
#[allow(dead_code)]
pub(crate) fn read_data_file_matching_equality_deletes_with_path_normalizer<N>(
    data_file_path: &str,
    data_file_size: Option<u64>,
    sets: &[EqualityDeleteSet],
    access: &FsAccessHandle,
    normalize_path: N,
) -> Result<Vec<RecordBatch>, String>
where
    N: Fn(&str) -> Result<String, String>,
{
    if sets.is_empty() {
        return Ok(Vec::new());
    }

    let normalized_path = normalize_path(data_file_path)?;
    let batches = read_foundation_parquet_batches(
        access,
        &normalized_path,
        data_file_size,
        FileProjection::All,
    )?;

    let mut out = Vec::new();
    for batch in batches {
        let Some(keep_mask) = equality_delete_keep_mask(&batch, sets)? else {
            continue;
        };
        let match_mask =
            BooleanArray::from(keep_mask.into_iter().map(|keep| !keep).collect::<Vec<_>>());
        let filtered = filter_record_batch(&batch, &match_mask).map_err(|e| {
            format!(
                "filter iceberg data file {data_file_path} for equality-delete reverse projection failed: {e}"
            )
        })?;
        if filtered.num_rows() > 0 {
            out.push(filtered);
        }
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::sync::Arc;

    use arrow::array::{Decimal128Array, Float32Array, Float64Array, Int32Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use parquet::arrow::{ArrowWriter, PARQUET_FIELD_ID_META_KEY};

    use novarocks_connector_iceberg::delete_file::{
        IcebergDeleteFileSpec, IcebergFileContent, IcebergFileFormat,
    };

    fn temp_dir_for(name: &str) -> std::path::PathBuf {
        let mut dir = std::env::temp_dir();
        dir.push(format!(
            "novarocks_equality_delete_tests_{}_{}",
            name,
            std::process::id()
        ));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).expect("create tmp dir");
        dir
    }

    fn factory_for_dir(dir: &std::path::Path) -> novarocks_fs::FsAccessHandle {
        novarocks_fs::FsAccessResolver::new()
            .resolve_location(dir.join("__binding__").to_string_lossy(), None)
            .expect("access")
    }

    fn field_with_id(name: &str, data_type: DataType, nullable: bool, field_id: i32) -> Field {
        Field::new(name, data_type, nullable).with_metadata(std::collections::HashMap::from([(
            PARQUET_FIELD_ID_META_KEY.to_string(),
            field_id.to_string(),
        )]))
    }

    fn write_eq_delete_parquet(path: &std::path::Path) {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("category", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![2, 4])),
                Arc::new(StringArray::from(vec!["B", "A"])),
            ],
        )
        .expect("record batch");
        let file = fs::File::create(path).expect("create");
        let mut writer = ArrowWriter::try_new(file, schema, None).expect("writer");
        writer.write(&batch).expect("write");
        writer.close().expect("close");
    }

    fn write_eq_delete_parquet_with_old_field_name(path: &std::path::Path) {
        let schema = Arc::new(Schema::new(vec![field_with_id(
            "amount",
            DataType::Int32,
            false,
            2,
        )]));
        let batch =
            RecordBatch::try_new(schema.clone(), vec![Arc::new(Int32Array::from(vec![20]))])
                .expect("record batch");
        let file = fs::File::create(path).expect("create");
        let mut writer = ArrowWriter::try_new(file, schema, None).expect("writer");
        writer.write(&batch).expect("write");
        writer.close().expect("close");
    }

    fn write_float32_eq_delete_parquet(path: &std::path::Path) {
        let schema = Arc::new(Schema::new(vec![field_with_id(
            "ratio",
            DataType::Float32,
            false,
            1,
        )]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Float32Array::from(vec![1.5_f32]))],
        )
        .expect("record batch");
        let file = fs::File::create(path).expect("create");
        let mut writer = ArrowWriter::try_new(file, schema, None).expect("writer");
        writer.write(&batch).expect("write");
        writer.close().expect("close");
    }

    fn write_decimal_eq_delete_parquet(path: &std::path::Path) {
        let schema = Arc::new(Schema::new(vec![field_with_id(
            "amount",
            DataType::Decimal128(10, 2),
            false,
            1,
        )]));
        let amount = Decimal128Array::from(vec![1234_i128])
            .with_precision_and_scale(10, 2)
            .expect("decimal delete array");
        let batch =
            RecordBatch::try_new(schema.clone(), vec![Arc::new(amount)]).expect("record batch");
        let file = fs::File::create(path).expect("create");
        let mut writer = ArrowWriter::try_new(file, schema, None).expect("writer");
        writer.write(&batch).expect("write");
        writer.close().expect("close");
    }

    fn write_data_parquet(path: &std::path::Path) {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("category", DataType::Utf8, false),
            Field::new("amount", DataType::Int32, false),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3, 4])),
                Arc::new(StringArray::from(vec!["A", "B", "B", "A"])),
                Arc::new(Int32Array::from(vec![10, 20, 30, 40])),
            ],
        )
        .expect("data batch");
        let file = fs::File::create(path).expect("create data");
        let mut writer = ArrowWriter::try_new(file, schema, None).expect("writer");
        writer.write(&batch).expect("write data");
        writer.close().expect("close data");
    }

    #[test]
    fn equality_delete_keep_mask_drops_matching_rows() {
        let dir = temp_dir_for("mask");
        let delete_path = dir.join("eq-delete.parquet");
        write_eq_delete_parquet(&delete_path);
        let spec = IcebergDeleteFileSpec {
            path: delete_path
                .file_name()
                .unwrap()
                .to_string_lossy()
                .to_string(),
            file_format: IcebergFileFormat::Parquet,
            file_content: IcebergFileContent::EqualityDeletes,
            length: None,
            content_offset: None,
            content_size_in_bytes: None,
        };
        let sets = super::load_equality_delete_sets(&[spec], &factory_for_dir(&dir)).expect("load");

        let data_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("category", DataType::Utf8, false),
            Field::new("amount", DataType::Int32, false),
        ]));
        let data = RecordBatch::try_new(
            data_schema,
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3, 4])),
                Arc::new(StringArray::from(vec!["A", "B", "B", "A"])),
                Arc::new(Int32Array::from(vec![10, 20, 30, 40])),
            ],
        )
        .expect("data batch");

        let mask =
            novarocks_connector_iceberg::file_reader::equality_delete::equality_delete_keep_mask(
                &data, &sets,
            )
            .expect("mask");

        assert_eq!(mask, Some(vec![true, false, true, false]));
    }

    #[test]
    fn equality_delete_matches_renamed_data_column_by_field_id() {
        let dir = temp_dir_for("field_id_rename");
        let delete_path = dir.join("eq-delete-renamed.parquet");
        write_eq_delete_parquet_with_old_field_name(&delete_path);
        let spec = IcebergDeleteFileSpec {
            path: delete_path
                .file_name()
                .unwrap()
                .to_string_lossy()
                .to_string(),
            file_format: IcebergFileFormat::Parquet,
            file_content: IcebergFileContent::EqualityDeletes,
            length: None,
            content_offset: None,
            content_size_in_bytes: None,
        };
        let sets = super::load_equality_delete_sets(&[spec], &factory_for_dir(&dir)).expect("load");

        let data_schema = Arc::new(Schema::new(vec![
            field_with_id("id", DataType::Int32, false, 1),
            field_with_id("total_amount", DataType::Int32, false, 2),
        ]));
        let data = RecordBatch::try_new(
            data_schema,
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(Int32Array::from(vec![10, 20, 30])),
            ],
        )
        .expect("data batch");

        let mask =
            novarocks_connector_iceberg::file_reader::equality_delete::equality_delete_keep_mask(
                &data, &sets,
            )
            .expect("mask");

        assert_eq!(mask, Some(vec![true, false, true]));
    }

    #[test]
    fn equality_delete_rejects_same_name_with_different_field_id() {
        let dir = temp_dir_for("field_id_readd");
        let delete_path = dir.join("eq-delete-old-id.parquet");
        write_eq_delete_parquet_with_old_field_name(&delete_path);
        let spec = IcebergDeleteFileSpec {
            path: delete_path
                .file_name()
                .unwrap()
                .to_string_lossy()
                .to_string(),
            file_format: IcebergFileFormat::Parquet,
            file_content: IcebergFileContent::EqualityDeletes,
            length: None,
            content_offset: None,
            content_size_in_bytes: None,
        };
        let sets = super::load_equality_delete_sets(&[spec], &factory_for_dir(&dir)).expect("load");

        let data_schema = Arc::new(Schema::new(vec![field_with_id(
            "amount",
            DataType::Int32,
            false,
            3,
        )]));
        let data = RecordBatch::try_new(data_schema, vec![Arc::new(Int32Array::from(vec![20]))])
            .expect("data batch");

        let err =
            novarocks_connector_iceberg::file_reader::equality_delete::equality_delete_keep_mask(
                &data, &sets,
            )
            .expect_err("field-id mismatch");

        assert!(err.contains("field_id=2"), "{err}");
    }

    #[test]
    fn equality_delete_matches_float_promoted_to_double() {
        let dir = temp_dir_for("float_promotion");
        let delete_path = dir.join("eq-delete-float.parquet");
        write_float32_eq_delete_parquet(&delete_path);
        let spec = IcebergDeleteFileSpec {
            path: delete_path
                .file_name()
                .unwrap()
                .to_string_lossy()
                .to_string(),
            file_format: IcebergFileFormat::Parquet,
            file_content: IcebergFileContent::EqualityDeletes,
            length: None,
            content_offset: None,
            content_size_in_bytes: None,
        };
        let sets = super::load_equality_delete_sets(&[spec], &factory_for_dir(&dir)).expect("load");

        let data_schema = Arc::new(Schema::new(vec![field_with_id(
            "ratio",
            DataType::Float64,
            false,
            1,
        )]));
        let data = RecordBatch::try_new(
            data_schema,
            vec![Arc::new(Float64Array::from(vec![1.5_f64, 2.5_f64]))],
        )
        .expect("data batch");

        let mask =
            novarocks_connector_iceberg::file_reader::equality_delete::equality_delete_keep_mask(
                &data, &sets,
            )
            .expect("mask");

        assert_eq!(mask, Some(vec![false, true]));
    }

    #[test]
    fn equality_delete_matches_decimal_precision_promotion() {
        let dir = temp_dir_for("decimal_precision_promotion");
        let delete_path = dir.join("eq-delete-decimal.parquet");
        write_decimal_eq_delete_parquet(&delete_path);
        let spec = IcebergDeleteFileSpec {
            path: delete_path
                .file_name()
                .unwrap()
                .to_string_lossy()
                .to_string(),
            file_format: IcebergFileFormat::Parquet,
            file_content: IcebergFileContent::EqualityDeletes,
            length: None,
            content_offset: None,
            content_size_in_bytes: None,
        };
        let sets = super::load_equality_delete_sets(&[spec], &factory_for_dir(&dir)).expect("load");

        let data_schema = Arc::new(Schema::new(vec![field_with_id(
            "amount",
            DataType::Decimal128(20, 2),
            false,
            1,
        )]));
        let amounts = Decimal128Array::from(vec![1234_i128, 5678_i128])
            .with_precision_and_scale(20, 2)
            .expect("decimal data array");
        let data = RecordBatch::try_new(data_schema, vec![Arc::new(amounts)]).expect("data batch");

        let mask =
            novarocks_connector_iceberg::file_reader::equality_delete::equality_delete_keep_mask(
                &data, &sets,
            )
            .expect("mask");

        assert_eq!(mask, Some(vec![false, true]));
    }

    #[test]
    fn equality_delete_reverse_projection_returns_matching_data_rows() {
        let dir = temp_dir_for("reverse");
        let delete_path = dir.join("eq-delete.parquet");
        let data_path = dir.join("data.parquet");
        write_eq_delete_parquet(&delete_path);
        write_data_parquet(&data_path);
        let factory = factory_for_dir(&dir);
        let spec = IcebergDeleteFileSpec {
            path: delete_path
                .file_name()
                .unwrap()
                .to_string_lossy()
                .to_string(),
            file_format: IcebergFileFormat::Parquet,
            file_content: IcebergFileContent::EqualityDeletes,
            length: None,
            content_offset: None,
            content_size_in_bytes: None,
        };
        let sets = super::load_equality_delete_sets(&[spec], &factory).expect("load");

        let batches = super::read_data_file_matching_equality_deletes_with_path_normalizer(
            &data_path.file_name().unwrap().to_string_lossy(),
            None,
            &sets,
            &factory,
            |path| Ok(path.to_string()),
        )
        .expect("reverse projection");

        assert_eq!(batches.len(), 1);
        let ids = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("id array");
        assert_eq!(ids.values(), &[2, 4]);
    }
}
