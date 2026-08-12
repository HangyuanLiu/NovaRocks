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

use arrow::record_batch::RecordBatch;
use novarocks_connector_iceberg::commit::data_writer::write_record_batches_as_data_files;

#[tokio::test]
async fn standalone_commit_round_trip_preserves_column_bounds() {
    use arrow::array::Int32Array;
    use arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
    use novarocks_connector_iceberg::iceberg::spec::{Datum, NestedField, PrimitiveType, Type};
    use std::sync::Arc;
    use tempfile::tempdir;

    let dir = tempdir().expect("tempdir");
    let location = format!("file://{}", dir.path().display());

    let iceberg_schema = Arc::new(
        novarocks_connector_iceberg::iceberg::spec::Schema::builder()
            .with_schema_id(1)
            .with_fields(vec![
                NestedField::required(1, "k1", Type::Primitive(PrimitiveType::Int)).into(),
            ])
            .build()
            .expect("schema"),
    );
    let metadata = novarocks_connector_iceberg::iceberg::spec::TableMetadataBuilder::new(
        iceberg_schema.as_ref().clone(),
        novarocks_connector_iceberg::iceberg::spec::PartitionSpec::unpartition_spec(),
        novarocks_connector_iceberg::iceberg::spec::SortOrder::unsorted_order(),
        location.clone(),
        novarocks_connector_iceberg::iceberg::spec::FormatVersion::V2,
        std::collections::HashMap::new(),
    )
    .expect("builder")
    .build()
    .expect("metadata")
    .metadata;
    let table = novarocks_connector_iceberg::iceberg::table::Table::builder()
        .identifier(
            novarocks_connector_iceberg::iceberg::TableIdent::from_strs(["db", "t"]).unwrap(),
        )
        .file_io(novarocks_connector_iceberg::fs_io::build_file_io_for_location(&location, None))
        .metadata(metadata)
        .build()
        .expect("table");

    let input_schema = Arc::new(ArrowSchema::new(vec![Field::new(
        "k1",
        DataType::Int32,
        false,
    )]));
    let values: Vec<i32> = (1..=1000).collect();
    let batch = RecordBatch::try_new(input_schema, vec![Arc::new(Int32Array::from(values))])
        .expect("batch");

    let data_files = write_record_batches_as_data_files(&table, vec![batch])
        .await
        .expect("write");
    assert_eq!(data_files.len(), 1);
    let df = &data_files[0];
    // The iceberg-rust ParquetWriter populates bounds from the parquet footer.
    assert_eq!(df.lower_bounds().get(&1), Some(&Datum::int(1)));
    assert_eq!(df.upper_bounds().get(&1), Some(&Datum::int(1000)));

    // Round-trip through the standalone commit path and assert the committed
    // DataFile still carries the bounds (the OQ-3.1 fix).
    let wf =
        crate::connector::iceberg::catalog::registry::data_file_to_written_file(df, 0).expect("wf");
    assert_eq!(wf.lower_bounds.get(&1), Some(&Datum::int(1)));
    assert_eq!(wf.upper_bounds.get(&1), Some(&Datum::int(1000)));

    let collector = crate::connector::iceberg::commit::IcebergCommitCollector::new(
        novarocks_connector_iceberg::commit::CommitOpKind::FastAppend,
        novarocks_connector_iceberg::iceberg::TableIdent::from_strs(["db", "t"]).unwrap(),
        None,
        0,
        table.metadata().current_schema().clone(),
        table.metadata().default_partition_spec().clone(),
        "file:///tmp/staging".to_string(),
        novarocks_types::UniqueId::new(0, 0),
    )
    .with_table_metadata(table.metadata().clone());
    let committed =
        crate::connector::iceberg::commit::written_file_to_iceberg_data_file(&wf, &collector)
            .expect("committed");
    assert_eq!(committed.lower_bounds().get(&1), Some(&Datum::int(1)));
    assert_eq!(committed.upper_bounds().get(&1), Some(&Datum::int(1000)));
}
