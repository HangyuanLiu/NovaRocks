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

// Verify the loaded COW schema projection against the original wire path.
use super::*;
use crate::iceberg::spec::{
    FormatVersion, ListType, Literal, NestedField, Operation, PartitionSpec, PrimitiveType,
    Schema as IcebergSchema, Snapshot, SortOrder, Summary, TableMetadata, TableMetadataBuilder,
    Type,
};

fn schema(extra: bool) -> IcebergSchema {
    let mut fields = vec![
        Arc::new(
            NestedField::optional(1, "tiny", PrimitiveType::Int.into())
                .with_doc("real stored column documentation")
                .with_initial_default(Literal::int(7)),
        ),
        Arc::new(NestedField::optional(
            2,
            "hidden",
            PrimitiveType::Long.into(),
        )),
        Arc::new(
            NestedField::optional(3, "label", PrimitiveType::String.into())
                .with_initial_default(Literal::string("quote \" and newline\n"))
                .with_write_default(Literal::string("write default")),
        ),
    ];
    if extra {
        fields.push(Arc::new(NestedField::optional(
            4,
            "new_column",
            PrimitiveType::Boolean.into(),
        )));
    }
    IcebergSchema::builder()
        .with_fields(fields)
        .build()
        .expect("small supported schema")
}
fn snapshot(base: &TableMetadata, id: i64, schema_id: i32, sequence: i64) -> Snapshot {
    Snapshot::builder()
        .with_snapshot_id(id)
        .with_schema_id(schema_id)
        .with_timestamp_ms(base.last_updated_ms())
        .with_sequence_number(if base.format_version() == FormatVersion::V1 {
            0
        } else {
            sequence
        })
        .with_manifest_list(format!("memory://cow-schema/snapshot-{id}.avro"))
        .with_summary(Summary {
            operation: Operation::Append,
            additional_properties: HashMap::new(),
        })
        .with_row_range(base.next_row_id(), 1)
        .build()
}
fn loaded_stock_metadata(version: FormatVersion) -> TableMetadata {
    let properties = HashMap::from([
        (
            crate::scalar_integer_domain::PROPERTY.to_string(),
            r#"{"1":"tinyint"}"#.to_string(),
        ),
        (HIDDEN_COLUMNS_PROPERTY.to_string(), "hidden".to_string()),
    ]);
    let base = TableMetadataBuilder::new(
        schema(false),
        PartitionSpec::unpartition_spec().into_unbound(),
        SortOrder::unsorted_order(),
        "memory://cow-schema".to_string(),
        version,
        properties,
    )
    .expect("stock builder")
    .build()
    .expect("base metadata")
    .metadata;
    let old_id = base.current_schema_id();
    let old_snapshot = snapshot(&base, 40, old_id, 1);
    let historical = base
        .into_builder(None)
        .add_snapshot(old_snapshot)
        .expect("historic snapshot")
        .add_schema(schema(true))
        .expect("current schema")
        .set_current_schema(-1)
        .expect("select actual new schema")
        .build()
        .expect("schema history")
        .metadata;
    let current_snapshot = snapshot(&historical, 41, historical.current_schema_id(), 2);
    let metadata = historical
        .into_builder(None)
        .add_snapshot(current_snapshot)
        .expect("current snapshot")
        .build()
        .expect("stock metadata")
        .metadata;
    // This is the EXISTING production load provenance: stock REST/HMS/Hadoop
    // decode TableMetadata JSON before the COW session obtains metadata_ref.
    serde_json::from_str(
        &serde_json::to_string(&metadata).expect("small original stock serialization"),
    )
    .expect("same actual stock metadata decoder")
}
fn payload(metadata: &TableMetadata, snapshot_id: i64) -> IcebergTablePayload {
    frozen_copy_on_write_source_payload(
        &ConnectorInstanceId::parse("iceberg").unwrap(),
        "db",
        "cow_schema",
        metadata,
        snapshot_id,
        IcebergDataFileInfo::for_test("memory://cow-schema/data.parquet", 100, 1),
    )
    .expect("actual original source payload constructor")
}
#[test]
fn stock_v1_v2_v3_current_and_historical_cow_schemas_keep_defaults_hidden_and_integer_domains() {
    for version in [FormatVersion::V1, FormatVersion::V2, FormatVersion::V3] {
        let metadata = loaded_stock_metadata(version);
        for id in [40, 41] {
            let source = payload(&metadata, id);
            let decoded = projected_schema(&source, &[]).expect("original decode path");
            let borrowed = projected_schema_with_metadata(&source, &[], &metadata)
                .expect("same loaded metadata");
            assert_eq!(borrowed, decoded);
            assert_eq!(
                borrowed.field(0).data_type(),
                &arrow::datatypes::DataType::Int8
            );
            assert!(
                borrowed
                    .field(1)
                    .metadata()
                    .contains_key(novarocks_spi::connector::CONNECTOR_FIELD_HIDDEN_FROM_SQL)
            );
            assert_eq!(
                borrowed
                    .field(0)
                    .metadata()
                    .get(crate::default_value::ICEBERG_INITIAL_DEFAULT_META_KEY)
                    .map(String::as_str),
                Some("7")
            );
            assert_eq!(
                borrowed
                    .fields()
                    .iter()
                    .any(|field| field.name() == "new_column"),
                id == 41
            );
            let selected = projected_schema_with_metadata(&source, &[2, 0], &metadata).unwrap();
            assert_eq!(selected, projected_schema(&source, &[2, 0]).unwrap());
            assert_eq!(selected.field(0).name(), "label");
        }
    }
}
#[test]
fn actual_projection_error_and_ordinary_decode_failure_are_preserved() {
    let metadata = loaded_stock_metadata(FormatVersion::V2);
    let mut source = payload(&metadata, 40);
    let original = projected_schema(&source, &[usize::MAX]).err().unwrap();
    assert_eq!(
        projected_schema_with_metadata(&source, &[usize::MAX], &metadata)
            .err()
            .unwrap(),
        original
    );
    source.table_info.as_mut().unwrap().serialized_metadata = None;
    assert_eq!(
        projected_schema(&source, &[]).err().unwrap(),
        corrupt("Iceberg table handle has no serialized metadata")
    );
}
#[test]
fn self_built_synthetic_doc_is_not_the_production_wire_normalized_provenance() {
    // A lawful in-memory library schema can contain extra synthetic child doc.
    // This normal counterexample constrains the helper's production caller;
    // it is NOT a new Unsupported condition or a product failure claim.
    let schema = IcebergSchema::builder()
        .with_fields(vec![Arc::new(NestedField::optional(
            1,
            "items",
            Type::List(ListType {
                element_field: Arc::new(
                    NestedField::list_element(2, PrimitiveType::Long.into(), false)
                        .with_doc("only exists before original Type wire normalization"),
                ),
            }),
        ))])
        .build()
        .unwrap();
    let base = TableMetadataBuilder::new(
        schema,
        PartitionSpec::unpartition_spec().into_unbound(),
        SortOrder::unsorted_order(),
        "memory://cow-schema/synthetic".to_string(),
        FormatVersion::V2,
        HashMap::new(),
    )
    .unwrap()
    .build()
    .unwrap()
    .metadata;
    let actual_snapshot = snapshot(&base, 40, base.current_schema_id(), 1);
    let memory = base
        .into_builder(None)
        .add_snapshot(actual_snapshot)
        .unwrap()
        .build()
        .unwrap()
        .metadata;
    let source = payload(&memory, 40);
    assert_ne!(
        projected_schema(&source, &[]).unwrap(),
        projected_schema_with_metadata(&source, &[], &memory).unwrap()
    );
    let loaded: TableMetadata =
        serde_json::from_str(&serde_json::to_string(&memory).unwrap()).unwrap();
    let source = payload(&loaded, 40);
    assert_eq!(
        projected_schema(&source, &[]).unwrap(),
        projected_schema_with_metadata(&source, &[], &loaded).unwrap()
    );
}
