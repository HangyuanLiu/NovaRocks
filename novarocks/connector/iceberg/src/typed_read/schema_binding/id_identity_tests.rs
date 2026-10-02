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

use super::*;
use crate::iceberg::spec::{ListType, StructType};
use arrow::array::Int64Array;
use arrow::buffer::OffsetBuffer;
use novarocks_spi::connector::ConnectorErrorKind;

fn identified(name: &str, data_type: DataType, id: i32) -> Field {
    Field::new(name, data_type, true).with_metadata(HashMap::from([(
        PARQUET_FIELD_ID_META_KEY.to_owned(),
        id.to_string(),
    )]))
}

fn schema(fields: Vec<NestedField>) -> Schema {
    Schema::builder()
        .with_fields(fields.into_iter().map(Arc::new))
        .build()
        .expect("valid current provider schema")
}

fn request<'a>(
    table: &'a Schema,
    file: &'a SchemaRef,
    columns: &'a [IcebergColumnHandle],
) -> IcebergSchemaBindingRequest<'a> {
    IcebergSchemaBindingRequest {
        table_schema: table,
        file_schema: file,
        name_mapping: None,
        partition_spec: None,
        partition_values: None,
        columns,
    }
}

fn materialize(
    binding: &IcebergSchemaBinding,
    batch: &RecordBatch,
) -> Result<Vec<ArrayRef>, ConnectorError> {
    binding.materialize(
        batch,
        None,
        &IcebergSplitFacts {
            path: "identity.parquet",
            file_first_row_id: None,
            data_sequence_number: None,
        },
    )
}

fn long_struct(field: &Field, value: i64) -> ArrayRef {
    let DataType::Struct(children) = field.data_type() else {
        panic!("test input must be a struct");
    };
    Arc::new(
        StructArray::try_new(
            children.clone(),
            vec![Arc::new(Int64Array::from(vec![value]))],
            None,
        )
        .expect("physical struct"),
    )
}

#[test]
fn id_identity_rejects_global_duplicates_wrong_parents_and_partial_complete_batches() {
    let table = schema(vec![
        NestedField::optional(
            10,
            "left",
            Type::Struct(StructType::new(vec![Arc::new(NestedField::optional(
                11,
                "value",
                Type::Primitive(PrimitiveType::Long),
            ))])),
        ),
        NestedField::optional(
            20,
            "right",
            Type::Struct(StructType::new(vec![Arc::new(NestedField::optional(
                21,
                "value",
                Type::Primitive(PrimitiveType::Long),
            ))])),
        ),
    ]);
    let root = |name: &str, id: i32, child: Field| {
        identified(name, DataType::Struct(vec![Arc::new(child)].into()), id)
    };
    let good: SchemaRef = Arc::new(ArrowSchema::new(vec![
        root("left", 10, identified("value", DataType::Int64, 11)),
        root("right", 20, identified("value", DataType::Int64, 21)),
    ]));
    // Only the left output is requested. Corruption in another subtree must
    // still be rejected at both the footer and the per-batch boundary.
    let columns = vec![IcebergColumnHandle::base_column_of(&table, 10).unwrap()];
    let binding = bind_scan_columns(request(&table, &good, &columns)).unwrap();
    let malformed = [
        vec![
            root("left", 10, identified("value", DataType::Int64, 11)),
            root("right", 20, identified("value", DataType::Int64, 11)),
        ],
        vec![
            root("left", 10, identified("value", DataType::Int64, 21)),
            root("right", 20, identified("value", DataType::Int64, 11)),
        ],
        vec![
            root("left", 10, identified("value", DataType::Int64, 11)),
            root("right", 20, Field::new("value", DataType::Int64, true)),
        ],
    ];
    for fields in malformed {
        let file: SchemaRef = Arc::new(ArrowSchema::new(fields));
        let error = bind_scan_columns(request(&table, &file, &columns)).unwrap_err();
        assert_eq!(error.kind(), ConnectorErrorKind::CorruptData);
        let batch = RecordBatch::try_new(
            file.clone(),
            vec![
                long_struct(file.field(0), 101),
                long_struct(file.field(1), 202),
            ],
        )
        .unwrap();
        assert_eq!(
            materialize(&binding, &batch).unwrap_err().kind(),
            ConnectorErrorKind::CorruptData
        );
    }
}

#[test]
fn id_identity_same_name_retired_struct_child_does_not_supply_readded_null_default_or_required() {
    let current = |required: bool| {
        let mut children = vec![
            Arc::new(NestedField::optional(
                12,
                "value",
                Type::Primitive(PrimitiveType::Long),
            )),
            Arc::new(
                NestedField::optional(13, "grade", Type::Primitive(PrimitiveType::Long))
                    .with_initial_default(Literal::long(42)),
            ),
        ];
        if required {
            children.push(Arc::new(NestedField::required(
                14,
                "required",
                Type::Primitive(PrimitiveType::Long),
            )));
        }
        schema(vec![NestedField::optional(
            10,
            "detail",
            Type::Struct(StructType::new(children)),
        )])
    };
    let old_child = identified("value", DataType::Int64, 11);
    let old_root = identified(
        "detail",
        DataType::Struct(vec![Arc::new(old_child)].into()),
        10,
    );
    let file: SchemaRef = Arc::new(ArrowSchema::new(vec![old_root.clone()]));
    let batch = RecordBatch::try_new(file.clone(), vec![long_struct(&old_root, 777)]).unwrap();
    let table = current(false);
    let columns = vec![IcebergColumnHandle::base_column_of(&table, 10).unwrap()];
    let binding = bind_scan_columns(request(&table, &file, &columns)).unwrap();
    let output = materialize(&binding, &batch).unwrap();
    let detail = output[0].as_any().downcast_ref::<StructArray>().unwrap();
    assert_eq!(detail.column(0).data_type(), &DataType::Int64);
    assert!(detail.column(0).is_null(0));
    assert_eq!(
        detail
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        42
    );

    let required_table = current(true);
    let required_columns = vec![IcebergColumnHandle::base_column_of(&required_table, 10).unwrap()];
    let required_binding =
        bind_scan_columns(request(&required_table, &file, &required_columns)).unwrap();
    let error = materialize(&required_binding, &batch).unwrap_err();
    assert_eq!(error.kind(), ConnectorErrorKind::CorruptData);
    assert!(
        error
            .to_string()
            .contains("missing required field required")
    );
}

#[test]
fn id_identity_same_name_list_element_cannot_reuse_a_retired_member_id() {
    let table = schema(vec![NestedField::optional(
        10,
        "items",
        Type::List(ListType::new(Arc::new(NestedField::optional(
            12,
            "element",
            Type::Primitive(PrimitiveType::Long),
        )))),
    )]);
    let old_child = Arc::new(identified("element", DataType::Int64, 11));
    let old_root = identified("items", DataType::List(old_child.clone()), 10);
    let file: SchemaRef = Arc::new(ArrowSchema::new(vec![old_root]));
    let list = ListArray::try_new(
        old_child,
        OffsetBuffer::new(vec![0_i32, 1].into()),
        Arc::new(Int64Array::from(vec![777])),
        None,
    )
    .unwrap();
    let batch = RecordBatch::try_new(file.clone(), vec![Arc::new(list)]).unwrap();
    let columns = vec![IcebergColumnHandle::base_column_of(&table, 10).unwrap()];
    let binding = bind_scan_columns(request(&table, &file, &columns)).unwrap();
    let error = materialize(&binding, &batch).unwrap_err();
    assert_eq!(error.kind(), ConnectorErrorKind::CorruptData);
    assert!(
        error
            .to_string()
            .contains("collection member has a different field ID")
    );
}

#[test]
fn id_identity_unidentified_files_keep_null_default_and_explicit_mapping_semantics() {
    let table = schema(vec![
        NestedField::optional(1, "value", Type::Primitive(PrimitiveType::Long)),
        NestedField::optional(2, "grade", Type::Primitive(PrimitiveType::Long))
            .with_initial_default(Literal::long(42)),
        NestedField::optional(3, "missing", Type::Primitive(PrimitiveType::Long)),
    ]);
    let unknown_root = Field::new(
        "unmapped",
        DataType::Struct(
            vec![Arc::new(Field::new(
                "unmapped_child",
                DataType::Int64,
                true,
            ))]
            .into(),
        ),
        true,
    );
    let file: SchemaRef = Arc::new(ArrowSchema::new(vec![
        Field::new("value", DataType::Int64, true),
        unknown_root.clone(),
    ]));
    let batch = RecordBatch::try_new(
        file.clone(),
        vec![
            Arc::new(Int64Array::from(vec![9])),
            long_struct(&unknown_root, 999),
        ],
    )
    .unwrap();
    let columns = [1, 2, 3].map(|id| IcebergColumnHandle::base_column_of(&table, id).unwrap());
    let unmapped = bind_scan_columns(request(&table, &file, &columns)).unwrap();
    assert_eq!(unmapped.coverage(), FileFieldIdCoverage::None);
    let null_output = materialize(&unmapped, &batch).unwrap();
    assert!(null_output[0].is_null(0));
    assert_eq!(
        null_output[1]
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        42
    );
    assert!(null_output[2].is_null(0));

    // The existing mapping owner requires every physical field to be mapped.
    // Sparse declarations must retain that refusal rather than guess identities.
    let sparse: NameMapping =
        serde_json::from_str(r#"[{"names":["value"],"field-id":1}]"#).unwrap();
    let mut sparse_request = request(&table, &file, &columns);
    sparse_request.name_mapping = Some(Arc::new(sparse));
    let error = bind_scan_columns(sparse_request).unwrap_err();
    assert_eq!(error.kind(), ConnectorErrorKind::CorruptData);
    assert!(
        error
            .to_string()
            .contains("does not contain physical field unmapped")
    );

    let mapping: NameMapping = serde_json::from_str(
        r#"[{"names":["value"],"field-id":1},{"names":["unmapped"],"field-id":99,"fields":[{"names":["unmapped_child"],"field-id":98}]}]"#,
    ).unwrap();
    let mut mapped_request = request(&table, &file, &columns);
    mapped_request.name_mapping = Some(Arc::new(mapping));
    let mapped = bind_scan_columns(mapped_request).unwrap();
    assert_eq!(mapped.coverage(), FileFieldIdCoverage::None);
    let mapped_output = materialize(&mapped, &batch).unwrap();
    assert_eq!(
        mapped_output[0]
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        9
    );
    assert_eq!(
        mapped_output[1]
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        42
    );
    assert_eq!(mapped_output[2].data_type(), &DataType::Int64);
    assert!(mapped_output[2].is_null(0));
}
