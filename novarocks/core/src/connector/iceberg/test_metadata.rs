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

//! Concrete Iceberg metadata fixtures owned by the connector test support.

use std::collections::HashMap;

use novarocks_connector_iceberg::iceberg::spec::{
    FormatVersion, NestedField, Operation, PartitionSpec, PrimitiveType, Schema, Snapshot,
    SnapshotReference, SnapshotRetention, SortOrder, Summary, TableMetadata, TableMetadataBuilder,
    Type,
};

pub(crate) fn base_builder() -> TableMetadataBuilder {
    let schema = Schema::builder()
        .with_fields(vec![
            NestedField::required(1, "id", Type::Primitive(PrimitiveType::Long)).into(),
        ])
        .build()
        .expect("build Iceberg test schema");
    TableMetadataBuilder::new(
        schema,
        PartitionSpec::unpartition_spec().into_unbound(),
        SortOrder::unsorted_order(),
        "file:///novarocks-test/table".to_string(),
        FormatVersion::V2,
        HashMap::new(),
    )
    .expect("build Iceberg test metadata")
}

pub(crate) fn metadata_empty() -> TableMetadata {
    base_builder()
        .build()
        .expect("build empty metadata")
        .metadata
}

pub(crate) fn metadata_with_two_snapshots() -> TableMetadata {
    let snapshot = |snapshot_id, timestamp_ms| {
        Snapshot::builder()
            .with_snapshot_id(snapshot_id)
            .with_timestamp_ms(timestamp_ms)
            .with_sequence_number(snapshot_id)
            .with_summary(Summary {
                operation: Operation::Append,
                additional_properties: HashMap::new(),
            })
            .with_manifest_list(format!(
                "file:///novarocks-test/table/metadata/snap-{snapshot_id}.avro"
            ))
            .with_schema_id(0)
            .build()
    };
    let metadata = base_builder()
        .add_snapshot(snapshot(1, 1_700_000_000_000))
        .expect("add first snapshot")
        .set_ref(
            "main",
            SnapshotReference::new(
                1,
                SnapshotRetention::Branch {
                    min_snapshots_to_keep: None,
                    max_snapshot_age_ms: None,
                    max_ref_age_ms: None,
                },
            ),
        )
        .expect("set main ref")
        .build()
        .expect("build first metadata")
        .metadata;
    metadata
        .into_builder(None)
        .add_snapshot(snapshot(2, 1_700_000_001_000))
        .expect("add second snapshot")
        .set_ref(
            "main",
            SnapshotReference::new(
                2,
                SnapshotRetention::Branch {
                    min_snapshots_to_keep: None,
                    max_snapshot_age_ms: None,
                    max_ref_age_ms: None,
                },
            ),
        )
        .expect("update main ref")
        .build()
        .expect("build second metadata")
        .metadata
}
