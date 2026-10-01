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

use std::collections::{BTreeMap, HashMap};
use std::num::NonZeroUsize;
use std::sync::Arc;

use arrow::array::{RecordBatch, new_null_array};
use arrow::datatypes::{DataType, FieldRef, Schema, TimeUnit};
use novarocks_execution::exec::chunk::{Chunk, ChunkSchema, ChunkSchemaRef, ChunkSlotSchema};
use novarocks_execution::exec::expr::ExprArena;
use novarocks_execution::exec::node::values::ValuesNode;
use novarocks_execution::exec::node::{ExecNode, ExecNodeKind, ExecPlan};
use novarocks_local_program::{LayoutError, StaticLayout, StaticSinkProgram};
use novarocks_native_adapter::fragment_layout::decode_output_layout;
use novarocks_plan_codec::native_type::decode_field_type;
use novarocks_proto_codec::FieldPath;
use novarocks_proto_models::common;
use novarocks_types::SlotId;
use novarocks_types::arrow_metadata_owner::FieldMetadataOrigins;
use novarocks_types::logical::{LogicalType, NR_LOGICAL_TYPE_KEY, logical_type_of_field};

fn scalar(primitive: common::PrimitiveType) -> common::TypeDesc {
    common::TypeDesc {
        kind: Some(common::type_desc::Kind::Scalar(common::ScalarType {
            r#type: primitive as i32,
            ..Default::default()
        })),
    }
}

fn column(id: u32, name: &str, data_type: common::TypeDesc) -> common::OutputColumn {
    common::OutputColumn {
        column_id: id,
        name: name.to_string(),
        nullable: true,
        r#type: Some(data_type),
        is_internal: false,
    }
}

fn nested_columns() -> Vec<common::OutputColumn> {
    let list = common::TypeDesc {
        kind: Some(common::type_desc::Kind::List(Box::new(common::ListType {
            element: Some(Box::new(scalar(common::PrimitiveType::Json))),
        }))),
    };
    let map = common::TypeDesc {
        kind: Some(common::type_desc::Kind::Map(Box::new(common::MapType {
            key: Some(Box::new(scalar(common::PrimitiveType::Varchar))),
            value: Some(Box::new(scalar(common::PrimitiveType::Json))),
        }))),
    };
    let structure = common::TypeDesc {
        kind: Some(common::type_desc::Kind::Strct(common::StructType {
            fields: vec![
                common::StructField {
                    name: "json_items".to_string(),
                    r#type: Some(list),
                },
                common::StructField {
                    name: "json_by_key".to_string(),
                    r#type: Some(map),
                },
                common::StructField {
                    name: "hll_state".to_string(),
                    r#type: Some(scalar(common::PrimitiveType::Hll)),
                },
            ],
        })),
    };
    let timestamp = common::TypeDesc {
        kind: Some(common::type_desc::Kind::Scalar(common::ScalarType {
            r#type: common::PrimitiveType::Datetime as i32,
            time_unit: Some(2),
            time_zone: Some("UTC".to_string()),
            ..Default::default()
        })),
    };
    vec![
        column(41, "nested", structure),
        column(42, "payload", scalar(common::PrimitiveType::Json)),
        column(43, "event_time", timestamp),
    ]
}

fn assert_tree_origins(field: &FieldRef, origins: &FieldMetadataOrigins) -> usize {
    assert!(
        origins.metadata_bytes_for(field).is_some(),
        "missing exact metadata owner for {}",
        field.name()
    );
    let structural_copy = Arc::new(field.as_ref().clone());
    assert_eq!(origins.metadata_bytes_for(&structural_copy), None);
    let children = match field.data_type() {
        DataType::Struct(fields) => fields
            .iter()
            .map(|child| assert_tree_origins(child, origins))
            .sum(),
        DataType::List(item) | DataType::Map(item, _) => assert_tree_origins(item, origins),
        _ => 0,
    };
    1 + children
}

// Exercise the production lowering entry rather than rebuilding a StaticLayout
// and manually attaching the receipts under test.
fn production_layout(schema: ChunkSchemaRef) -> StaticLayout {
    let arrow_schema = schema.arrow_schema_ref();
    let arrays = arrow_schema
        .fields()
        .iter()
        .map(|field| new_null_array(field.data_type(), 1))
        .collect();
    let batch = RecordBatch::try_new(arrow_schema, arrays).unwrap();
    let chunk = Chunk::try_new_with_chunk_schema(batch, schema).unwrap();
    let plan = ExecPlan {
        arena: ExprArena::default(),
        root: ExecNode {
            kind: ExecNodeKind::Values(ValuesNode { chunk, node_id: 7 }),
        },
    };
    let profile = plan
        .local_compile_profile(NonZeroUsize::new(1).unwrap(), None)
        .unwrap();
    let (program, bindings) = plan
        .into_local_program_and_bindings(
            profile,
            BTreeMap::new(),
            Vec::new(),
            StaticSinkProgram::Noop,
        )
        .unwrap();
    assert_eq!(bindings.scan_count(), 0);
    assert_eq!(bindings.writer_count(), 0);
    assert_eq!(bindings.finish_count(), 0);
    program.nodes()[program.root().index()]
        .output_layout()
        .clone()
}

#[test]
fn native_decode_and_production_lowering_preserve_nested_exact_metadata_owners() {
    let columns = nested_columns();
    let decoded = decode_output_layout(&columns, FieldPath::root("columns")).unwrap();
    let source = decoded.chunk_schema();
    let source_arrow = source.arrow_schema_ref();
    let source_origins = source.field_metadata_origins().unwrap();
    let mut nodes = 0;
    for (index, wire) in columns.iter().enumerate() {
        let field = &source_arrow.fields()[index];
        assert!(Arc::ptr_eq(
            field,
            decoded.slot_schemas()[index].field_ref()
        ));
        let legacy =
            decode_field_type(&wire.name, wire.nullable, wire.r#type.as_ref().unwrap()).unwrap();
        assert_eq!(
            field.as_ref(),
            &legacy,
            "metadata must preserve native semantics"
        );
        nodes += assert_tree_origins(field, source_origins);
    }
    assert_eq!(nodes, 10);
    assert_eq!(source_origins.owners().len(), nodes);
    assert_eq!(
        logical_type_of_field(&source_arrow.fields()[1]),
        Some(LogicalType::Json)
    );
    assert_eq!(
        source_arrow.fields()[2].data_type(),
        &DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))
    );
    let layout = production_layout(source);
    assert!(layout.has_exact_slot_metadata());
    assert!(Arc::ptr_eq(layout.schema(), &source_arrow));
    let origins = layout.field_metadata_origins().unwrap();
    for field in layout.schema().fields() {
        assert_tree_origins(field, origins);
    }
    assert_eq!(
        layout
            .schema_metadata_origin()
            .unwrap()
            .backing_bytes_for(layout.schema()),
        Some(0)
    );
}

#[test]
fn production_projection_keeps_field_identity_and_mints_exact_schema_metadata_owner() {
    let decoded = decode_output_layout(&nested_columns(), FieldPath::root("columns")).unwrap();
    let layout = production_layout(decoded.chunk_schema());
    let projected = layout
        .project_by_slots(&[SlotId::new(42), SlotId::new(41)])
        .unwrap();
    assert_eq!(projected.slots(), &[SlotId::new(42), SlotId::new(41)]);
    assert!(Arc::ptr_eq(
        &projected.schema().fields()[0],
        &layout.schema().fields()[1]
    ));
    assert!(Arc::ptr_eq(
        &projected.schema().fields()[1],
        &layout.schema().fields()[0]
    ));
    assert!(!Arc::ptr_eq(projected.schema(), layout.schema()));
    let origins = projected.field_metadata_origins().unwrap();
    for field in projected.schema().fields() {
        assert_tree_origins(field, origins);
    }
    assert_eq!(
        projected
            .schema_metadata_origin()
            .unwrap()
            .backing_bytes_for(projected.schema()),
        Some(0)
    );
    assert_eq!(
        layout
            .schema_metadata_origin()
            .unwrap()
            .backing_bytes_for(projected.schema()),
        None
    );
}

#[test]
fn equal_unknown_large_metadata_tables_cannot_inherit_native_construction_origins() {
    let decoded = decode_output_layout(
        &[column(42, "payload", scalar(common::PrimitiveType::Json))],
        FieldPath::root("columns"),
    )
    .unwrap();
    let source = decoded.chunk_schema();
    let source_arrow = source.arrow_schema_ref();
    let known = production_layout(Arc::clone(&source));
    let mut metadata = HashMap::with_capacity(8192);
    metadata.insert(NR_LOGICAL_TYPE_KEY.to_string(), "json".to_string());
    let unknown_field = source_arrow.fields()[0].clone_with_metadata(metadata);
    assert_eq!(&unknown_field, source_arrow.fields()[0].as_ref());
    let unknown = Arc::new(
        ChunkSchema::try_new(vec![ChunkSlotSchema::new_with_field(
            SlotId::new(42),
            unknown_field,
            None,
            None,
        )])
        .unwrap(),
    );
    assert!(unknown.field_metadata_origins().is_none());
    let unknown = production_layout(unknown);
    assert_eq!(known.identity().unwrap(), unknown.identity().unwrap());
    assert!(unknown.field_metadata_origins().is_none());
    assert_eq!(
        source
            .field_metadata_origins()
            .unwrap()
            .metadata_bytes_for(&unknown.schema().fields()[0]),
        None
    );
    assert_eq!(
        unknown
            .with_metadata_origins(source.field_metadata_origins().unwrap().clone(), None)
            .unwrap_err(),
        LayoutError::MetadataOwnerConflict
    );

    let unknown_schema = Arc::new(Schema::new_with_metadata(
        source_arrow.fields().clone(),
        HashMap::with_capacity(8192),
    ));
    assert_eq!(unknown_schema.as_ref(), source_arrow.as_ref());
    assert_eq!(
        source
            .schema_metadata_origin()
            .unwrap()
            .backing_bytes_for(&unknown_schema),
        None
    );
    let layout = StaticLayout::try_new(unknown_schema, Arc::from([SlotId::new(42)])).unwrap();
    assert_eq!(
        layout
            .with_metadata_origins(
                source.field_metadata_origins().unwrap().clone(),
                source.schema_metadata_origin().cloned(),
            )
            .unwrap_err(),
        LayoutError::MetadataOwnerConflict
    );
}
