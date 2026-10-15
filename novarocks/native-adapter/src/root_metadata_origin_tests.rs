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

use crate::fragment_plan_node::{NativeLoweredPlanNode, lower_project_node};
use crate::fragment_scan_output::decode_scan_output_columns;
use arrow::array::{RecordBatch, new_null_array};
use arrow::datatypes::{DataType, Field, FieldRef, Schema, TimeUnit};
use novarocks_execution::exec::chunk::{Chunk, ChunkSchemaRef, SlotLayout};
use novarocks_execution::exec::expr::ExprArena;
use novarocks_execution::exec::node::values::ValuesNode;
use novarocks_execution::exec::node::{ExecNode, ExecNodeKind, ExecPlan};
use novarocks_local_program::{
    LayoutError, LocalProgramGraph, ProgramNodeKind, StaticLayout, StaticSinkProgram,
};
use novarocks_proto_codec::FieldPath;
use novarocks_proto_models::{common, expr, plan};
use novarocks_types::SlotId;
use novarocks_types::arrow_metadata_owner::{
    ArrowMetadataOwner, FieldMetadataOrigins, MetadataOwnedField, MetadataOwnedSchema,
    MetadataOwnerLimits,
};
use novarocks_types::logical::{LogicalType, NR_LOGICAL_TYPE_KEY, logical_type_of_field};

fn column(id: u32, name: &str) -> common::OutputColumn {
    common::OutputColumn {
        column_id: id,
        name: name.to_string(),
        nullable: true,
        r#type: Some(common::TypeDesc {
            value_binding_id: Some(id - 41),
        }),
        is_internal: false,
    }
}

fn nested_columns() -> Vec<common::OutputColumn> {
    vec![
        column(41, "nested"),
        column(42, "payload"),
        column(43, "event_time"),
    ]
}

// Complete declarations and source annotations are independent fixture facts.
fn declared_value(column: &common::OutputColumn) -> novarocks_type_contract::FunctionValueType {
    use novarocks_type_contract::{FunctionValueType, ValueTypeStorageLifetime};
    use novarocks_types::schema::SqlType;
    let (declaration, carrier) = match column.column_id {
        41 => (
            SqlType::Struct(vec![
                ("json_items".into(), SqlType::Array(Box::new(SqlType::Json))),
                (
                    "json_by_key".into(),
                    SqlType::Map(Box::new(SqlType::String), Box::new(SqlType::Json)),
                ),
                ("hll_state".into(), SqlType::Hll),
            ]),
            DataType::Struct(
                vec![
                    Field::new(
                        "json_items",
                        DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
                        true,
                    ),
                    Field::new(
                        "json_by_key",
                        DataType::Map(
                            Arc::new(Field::new(
                                "entries",
                                DataType::Struct(
                                    vec![
                                        Field::new("key", DataType::Utf8, false),
                                        Field::new("value", DataType::Utf8, true),
                                    ]
                                    .into(),
                                ),
                                false,
                            )),
                            false,
                        ),
                        true,
                    ),
                    Field::new("hll_state", DataType::Binary, true),
                ]
                .into(),
            ),
        ),
        42 => (SqlType::Json, DataType::Utf8),
        43 => {
            return FunctionValueType::new(
                DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                column.nullable,
            );
        }
        _ => panic!("fixture has no independent declaration for this column"),
    };
    FunctionValueType::try_from_typed_binding(
        novarocks_types::execution_value_type_from_declaration(
            &declaration,
            &carrier,
            column.nullable,
            ValueTypeStorageLifetime::pure(),
        )
        .unwrap(),
    )
    .unwrap()
}

fn fresh_source_field(
    name: &str,
    data_type: DataType,
    nullable: bool,
    logical: Option<&str>,
    owners: &mut Vec<MetadataOwnedField>,
) -> FieldRef {
    let entries = logical
        .map(|logical| vec![(NR_LOGICAL_TYPE_KEY.to_string(), logical.to_string())])
        .unwrap_or_default();
    let owner = ArrowMetadataOwner::try_new(
        entries,
        MetadataOwnerLimits {
            entries: 1,
            construction_bytes: 4096,
        },
    )
    .unwrap()
    .into_field(name.to_string(), data_type, nullable);
    let field = owner.field().clone();
    owners.push(owner);
    field
}

fn source_schema(columns: &[common::OutputColumn]) -> (MetadataOwnedSchema, FieldMetadataOrigins) {
    let mut owners = Vec::new();
    let mut fields = Vec::new();
    for column in columns {
        let data_type = match column.column_id {
            41 => {
                let item =
                    fresh_source_field("item", DataType::Utf8, true, Some("json"), &mut owners);
                let list =
                    fresh_source_field("json_items", DataType::List(item), true, None, &mut owners);
                let key = fresh_source_field("key", DataType::Utf8, false, None, &mut owners);
                let value =
                    fresh_source_field("value", DataType::Utf8, true, Some("json"), &mut owners);
                let entries = fresh_source_field(
                    "entries",
                    DataType::Struct(vec![key, value].into()),
                    false,
                    None,
                    &mut owners,
                );
                let map = fresh_source_field(
                    "json_by_key",
                    DataType::Map(entries, false),
                    true,
                    None,
                    &mut owners,
                );
                let hll = fresh_source_field(
                    "hll_state",
                    DataType::Binary,
                    true,
                    Some("hll"),
                    &mut owners,
                );
                DataType::Struct(vec![list, map, hll].into())
            }
            42 => DataType::Utf8,
            43 => DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            _ => panic!("fixture has no source field for this column"),
        };
        fields.push(fresh_source_field(
            &column.name,
            data_type,
            column.nullable,
            (column.column_id == 42).then_some("json"),
            &mut owners,
        ));
    }
    let schema = ArrowMetadataOwner::try_new(
        Vec::new(),
        MetadataOwnerLimits {
            entries: 0,
            construction_bytes: 0,
        },
    )
    .unwrap()
    .into_schema(fields.into());
    (
        schema,
        FieldMetadataOrigins::try_new(owners, 65_536).unwrap(),
    )
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

struct OwnerControl;
impl novarocks_type_contract::PureCompileControl for OwnerControl {
    fn checkpoint(
        &self,
        _: novarocks_type_contract::CompilePhase,
        units: u32,
    ) -> Result<(), novarocks_type_contract::CompileControlError> {
        assert!(units <= 256);
        Ok(())
    }
}
// These exact fixture declarations are independent of source annotations.
// The sole SQL declaration/layout author and real Native table decoder validate
// them. No expression calls exist here, so no fake call request is supplied.
fn with_typed_columns<R>(
    columns: &[common::OutputColumn],
    operation: impl FnOnce(
        &[common::OutputColumn],
        &mut crate::fragment_type_decode::NativeTypeDecoder<'_, '_, '_>,
    ) -> R,
) -> R {
    use novarocks_plan_codec::execution_type_table::{
        ExecutionTypeRoot, ExecutionTypeTableBuilder, ExecutionTypeTableLimits,
    };
    use novarocks_type_contract::{CompileCheckpoints, CompilePhase};
    let values = columns.iter().map(declared_value).collect::<Vec<_>>();
    let roots = values
        .iter()
        .enumerate()
        .map(|(id, value)| ExecutionTypeRoot {
            id: id as u32,
            value,
        })
        .collect::<Vec<_>>();
    let mut encode = CompileCheckpoints::try_new(&OwnerControl, CompilePhase::Encode).unwrap();
    let encoded = ExecutionTypeTableBuilder::new(&roots)
        .encode_observed::<std::convert::Infallible>(
            ExecutionTypeTableLimits::for_validated_root_count(roots.len()).unwrap(),
            &mut |_| Ok(()),
            &mut encode,
        )
        .unwrap();
    let columns = columns
        .iter()
        .enumerate()
        .map(|(id, column)| {
            let mut column = column.clone();
            column.r#type = Some(
                encoded
                    .type_reference_observed(id as u32, &mut encode)
                    .unwrap(),
            );
            column
        })
        .collect::<Vec<_>>();
    encode.finish().unwrap();
    let fragment = plan::PlanFragment {
        execution_types: Some(encoded.into_wire()),
        ..Default::default()
    };
    let creation = crate::native_type_construction::NativeTypeConstruction::try_with_control(
        novarocks_execution::runtime::mem_tracker::MemTracker::new_root(
            "root metadata complete lowering",
        ),
        &OwnerControl,
    )
    .unwrap();
    let mut resources =
        crate::fragment_type_decode::NativeDecodeResources::try_new(&creation).unwrap();
    let types =
        crate::fragment_type_decode::decode_fragment_type_owner(&fragment, &mut resources).unwrap();
    let mut decoder = crate::fragment_type_decode::NativeTypeDecoder::Strong {
        types: &types.execution,
        original_requests: None,
        resources: &mut resources,
    };
    operation(&columns, &mut decoder)
}
fn typed_decoded(columns: &[common::OutputColumn]) -> crate::fragment_layout::NativeOutputLayout {
    with_typed_columns(columns, |columns, decoder| {
        decoder
            .output_layout(columns, FieldPath::root("columns"))
            .unwrap()
    })
}
fn assert_typed_domains(layout: &StaticLayout) {
    assert!(layout.is_typed());
    assert_eq!(layout.schema().metadata().capacity(), 0);
    for (index, field) in layout.schema().fields().iter().enumerate() {
        assert_eq!(field.metadata().capacity(), 0);
        assert!(layout.value_binding_at(index).is_some());
        assert_eq!(
            field.is_nullable(),
            layout.value_binding_at(index).unwrap().nullable()
        );
    }
}

// Exercise the production lowering entry rather than rebuilding a StaticLayout
// and manually attaching the receipts under test.
fn production_layout(schema: ChunkSchemaRef) -> StaticLayout {
    let plan = ExecPlan {
        arena: ExprArena::default(),
        root: ExecNode {
            kind: ExecNodeKind::Values(ValuesNode {
                chunk: null_chunk(schema),
                node_id: 7,
            }),
        },
    };
    let program = production_program(plan);
    program.nodes()[program.root().index()]
        .output_layout()
        .clone()
}

fn null_chunk(schema: ChunkSchemaRef) -> Chunk {
    let arrow_schema = schema.arrow_schema_ref();
    let arrays = arrow_schema
        .fields()
        .iter()
        .map(|field| new_null_array(field.data_type(), 1))
        .collect();
    let batch = RecordBatch::try_new(arrow_schema, arrays).unwrap();
    let factory=novarocks_execution::exec::pipeline::operator::input_construction::try_runtime_input_construction_factory(
        novarocks_execution::exec::pipeline::pipeline::representation_construction_profile().unwrap(),
        novarocks_execution::runtime::mem_tracker::MemTracker::new_root("root metadata original null batch"),
        Arc::new(novarocks_execution::runtime::runtime_state::RuntimeErrorState::default())).unwrap();
    let owner = factory.begin_typed_construction(&schema).unwrap();
    Chunk::try_new_typed_record_batch_owned_observed::<
        novarocks_execution::exec::pipeline::operator::input_construction::ConstructionFailure,
        _,
        _,
    >(
        batch,
        schema.clone(),
        schema.storage_lifetime(),
        owner.storage_lifetime(),
        |layout| owner.request(layout),
        |step| owner.observe(0, &[], step),
    )
    .unwrap()
}
fn production_program(plan: ExecPlan) -> LocalProgramGraph {
    let source =
        novarocks_execution::exec::pipeline::builder::output_chunk_schema_for_node(&plan.root)
            .unwrap();
    assert!(source.is_typed());
    let creation = crate::native_type_construction::NativeTypeConstruction::try_with_control(
        novarocks_execution::runtime::mem_tracker::MemTracker::new_root(
            "root metadata created program",
        ),
        &OwnerControl,
    )
    .unwrap();
    let retained = creation.begin_scope().unwrap();
    let temporary = creation.begin_scope().unwrap();
    let storage = retained
        .storage_lifetime()
        .retaining_observed(&source.storage_lifetime(), |layout| {
            retained.request_layout(layout)
        })
        .unwrap();
    let mut request = |layout| retained.request_layout(layout);
    let mut temporary_request = |layout| temporary.request_layout(layout);
    let mut owner = novarocks_execution::exec::node::LocalLayoutCreation::new(
        storage,
        &mut request,
        &mut temporary_request,
        &creation,
    );
    let profile = plan
        .local_compile_profile_with_creator(NonZeroUsize::new(1).unwrap(), None, &mut owner)
        .unwrap();
    let (program, bindings) = plan
        .into_local_program_and_bindings_with_creator(
            profile,
            BTreeMap::new(),
            Vec::new(),
            StaticSinkProgram::Noop,
            owner,
        )
        .unwrap();
    assert_eq!(bindings.scan_count(), 0);
    assert_eq!(bindings.writer_count(), 0);
    assert_eq!(bindings.finish_count(), 0);
    program
}

#[test]
fn native_decode_and_production_lowering_preserve_nested_exact_metadata_owners() {
    let columns = nested_columns();
    let (source, source_origins) = source_schema(&columns);
    let source_arrow = source.schema();
    let mut nodes = 0;
    for (index, wire) in columns.iter().enumerate() {
        let field = &source_arrow.fields()[index];
        assert!(Arc::ptr_eq(
            field,
            source_origins.owner_for(field).unwrap().field()
        ));
        let (expected, _) = source_schema(std::slice::from_ref(wire));
        assert_eq!(
            field.as_ref(),
            expected.schema().fields()[0].as_ref(),
            "metadata must preserve native source facts"
        );
        nodes += assert_tree_origins(field, &source_origins);
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
    let complete = typed_decoded(&columns).chunk_schema();
    let complete_arrow = complete.arrow_schema_ref();
    let layout = production_layout(complete);
    assert_typed_domains(&layout);
    assert_eq!(
        layout.value_binding_at(1).unwrap().value_type().domain(),
        Some(novarocks_type_contract::ValueLogicalType::Json)
    );
    assert!(layout.has_exact_slot_metadata());
    assert!(Arc::ptr_eq(layout.schema(), &complete_arrow));
    for field in source_arrow.fields() {
        assert_tree_origins(field, &source_origins);
    }
    assert_eq!(source.backing_bytes_for(source_arrow), Some(0));
}

#[test]
fn production_projection_keeps_field_identity_and_mints_exact_schema_metadata_owner() {
    let (original, origins) = source_schema(&nested_columns());
    let decoded = typed_decoded(&nested_columns());
    let layout = production_layout(decoded.chunk_schema());
    let creation = crate::native_type_construction::NativeTypeConstruction::try_with_control(
        novarocks_execution::runtime::mem_tracker::MemTracker::new_root("root metadata projection"),
        &OwnerControl,
    )
    .unwrap();
    let scope = creation.begin_scope().unwrap();
    let projected = layout
        .project_by_slots_typed_with_creator(
            &[SlotId::new(42), SlotId::new(41)],
            scope.storage_lifetime(),
            |request| {
                scope
                    .request_layout(request)
                    .map_err(novarocks_local_program::LayoutCreationFailure::Creator)
            },
            &creation,
        )
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
    assert_typed_domains(&projected);
    assert_eq!(projected.value_binding_at(0), layout.value_binding_at(1));
    let source_projection = original
        .derive_schema(
            vec![
                original.schema().fields()[1].clone(),
                original.schema().fields()[0].clone(),
            ]
            .into(),
            novarocks_types::arrow_metadata_owner::MetadataOwnerLimits {
                entries: 0,
                construction_bytes: 0,
            },
        )
        .unwrap();
    for field in source_projection.schema().fields() {
        assert_tree_origins(field, &origins);
    }
    assert_eq!(
        source_projection.backing_bytes_for(source_projection.schema()),
        Some(0)
    );
    assert_eq!(original.backing_bytes_for(source_projection.schema()), None);
}

#[test]
fn equal_unknown_large_metadata_tables_cannot_inherit_native_construction_origins() {
    let columns = [column(42, "payload")];
    let (source, origins) = source_schema(&columns);
    let source_arrow = source.schema();
    let known = production_layout(typed_decoded(&columns).chunk_schema());
    assert_typed_domains(&known);
    let mut metadata = HashMap::with_capacity(8192);
    metadata.insert(NR_LOGICAL_TYPE_KEY.to_string(), "json".to_string());
    let unknown_field = Arc::new(
        source_arrow.fields()[0]
            .as_ref()
            .clone()
            .with_metadata(metadata),
    );
    assert_eq!(unknown_field.as_ref(), source_arrow.fields()[0].as_ref());
    assert_eq!(origins.metadata_bytes_for(&unknown_field), None);
    let unknown_schema = Arc::new(Schema::new_with_metadata(
        source_arrow.fields().clone(),
        HashMap::with_capacity(8192),
    ));
    assert_eq!(unknown_schema.as_ref(), source_arrow.as_ref());
    assert_eq!(source.backing_bytes_for(&unknown_schema), None);
    let mut requested = 0usize;
    let error = StaticLayout::try_new_typed_for_compile(
        unknown_schema,
        Arc::from([SlotId::new(42)]),
        vec![(known.value_binding_at(0).unwrap().clone(), None)],
        novarocks_type_contract::ValueTypeStorageLifetime::pure(),
        |layout| {
            requested = requested
                .checked_add(layout.size())
                .ok_or(novarocks_type_contract::CompileControlError::ResourceExhausted)?;
            Ok(())
        },
        &OwnerControl,
    )
    .unwrap_err();
    assert!(matches!(
        error,
        novarocks_local_program::LayoutCompileError::Layout(LayoutError::ValueBindingMismatch)
    ));
    assert_eq!(
        requested, 0,
        "unknown Schema capacity rejected before construction"
    );
    // An equal foreign source Field is still not an original receipt. Likewise
    // source receipts cannot be relabeled as execution projection receipts.
    assert_eq!(
        known
            .clone()
            .with_metadata_origins(origins.clone(), None)
            .unwrap_err(),
        LayoutError::MetadataOwnerConflict
    );
    assert_eq!(
        known
            .with_metadata_origins(origins.clone(), Some(source.clone()))
            .unwrap_err(),
        LayoutError::MetadataOwnerConflict
    );
}

#[test]
fn actual_scan_decoder_preserves_selected_nested_field_owners_and_wire_semantics() {
    let original_scan = plan::ScanNode {
        columns: nested_columns(),
        required_columns: vec!["PAYLOAD".to_string(), "NESTED".to_string()],
        ..Default::default()
    };
    let (original, origins) = source_schema(&original_scan.columns[..2]);
    let decoded = with_typed_columns(&original_scan.columns, |columns, decoder| {
        let scan = plan::ScanNode {
            columns: columns.to_vec(),
            required_columns: original_scan.required_columns.clone(),
            ..Default::default()
        };
        let decoded = decode_scan_output_columns(&scan, FieldPath::root("scan"), decoder).unwrap();
        for (index, source) in decoded.provenanced().iter().enumerate() {
            assert_eq!(source.column(), &scan.columns[index]);
        }
        decoded
    });
    let schema = decoded.output_schema();
    let arrow_schema = schema.arrow_schema_ref();
    let original_arrow = original.schema();
    assert_eq!(schema.slot_ids(), &[SlotId::new(41), SlotId::new(42)]);
    assert_eq!(decoded.provenanced().len(), 2);
    let mut nodes = 0;
    for (index, source) in decoded.provenanced().iter().enumerate() {
        assert!(Arc::ptr_eq(
            &arrow_schema.fields()[index],
            source.slot_schema().field_ref()
        ));
        assert_eq!(
            source.column().column_id,
            original_scan.columns[index].column_id
        );
        assert_eq!(source.column().name, original_scan.columns[index].name);
        assert_eq!(
            source.column().nullable,
            original_scan.columns[index].nullable
        );
        let original_field = &original_arrow.fields()[index];
        let (expected, _) = source_schema(std::slice::from_ref(&original_scan.columns[index]));
        assert_eq!(
            original_field.as_ref(),
            expected.schema().fields()[0].as_ref()
        );
        nodes += assert_tree_origins(original_field, &origins);
    }
    assert_eq!(nodes, 9);
    assert_eq!(origins.owners().len(), nodes);
    assert_eq!(original.backing_bytes_for(original.schema()), Some(0));
    let frozen = production_layout(schema);
    assert!(Arc::ptr_eq(frozen.schema(), &arrow_schema));
    assert_typed_domains(&frozen);
    assert_eq!(
        frozen.value_binding_at(1).unwrap().value_type().domain(),
        Some(novarocks_type_contract::ValueLogicalType::Json)
    );
    assert_eq!(origins.owners().len(), nodes);
}

#[test]
fn actual_native_project_repeated_occurrences_keep_complete_purpose_and_finite_owner_when_frozen() {
    use crate::fragment_type_decode::{
        NativeDecodeResources, NativeTypeDecoder, decode_fragment_type_owner,
    };
    use crate::native_type_construction::NativeTypeConstruction;
    use novarocks_execution::exec::node::LocalLayoutCreation;
    use novarocks_plan_codec::execution_type_table::{
        ExecutionTypeRoot, ExecutionTypeTableBuilder, ExecutionTypeTableLimits,
    };
    use novarocks_type_contract::{
        CompileCheckpoints, CompileControlError, CompilePhase, PureCompileControl,
    };

    struct Control;
    impl PureCompileControl for Control {
        fn checkpoint(&self, _: CompilePhase, units: u32) -> Result<(), CompileControlError> {
            assert!(units <= 256);
            Ok(())
        }
    }
    let original_column = nested_columns().remove(0);
    // The original format/source owners remain independently observable. They
    // are not execution semantic authors or a receipt for an equal new Field.
    let (original_schema, original_origins) = source_schema(std::slice::from_ref(&original_column));
    let original_field = &original_schema.schema().fields()[0];
    assert_eq!(assert_tree_origins(original_field, &original_origins), 8);

    // Execution meaning comes from the exact declaration, never source attrs.
    let value = declared_value(&original_column);
    let mut encode_work = CompileCheckpoints::try_new(&Control, CompilePhase::Encode).unwrap();
    let roots = [ExecutionTypeRoot {
        id: 0,
        value: &value,
    }];
    let encoded = ExecutionTypeTableBuilder::new(&roots)
        .encode_observed::<std::convert::Infallible>(
            ExecutionTypeTableLimits::for_validated_root_count(1).unwrap(),
            &mut |_| Ok(()),
            &mut encode_work,
        )
        .unwrap();
    let mut source_column = original_column.clone();
    source_column.r#type = Some(
        encoded
            .type_reference_observed(0, &mut encode_work)
            .unwrap(),
    );
    encode_work.finish().unwrap();
    let fragment = plan::PlanFragment {
        execution_types: Some(encoded.into_wire()),
        ..Default::default()
    };
    let tracker = novarocks_execution::runtime::mem_tracker::MemTracker::new_root(
        "native CSE complete purpose fixture",
    );
    let creation = NativeTypeConstruction::try_with_control(tracker.clone(), &Control).unwrap();
    let mut resources = NativeDecodeResources::try_new(&creation).unwrap();
    let types = decode_fragment_type_owner(&fragment, &mut resources).unwrap();
    let mut decoder = NativeTypeDecoder::Strong {
        types: &types.execution,
        original_requests: None,
        resources: &mut resources,
    };
    let input_schema = decoder
        .chunk_schema(
            std::slice::from_ref(&source_column),
            FieldPath::root("columns"),
        )
        .unwrap();
    let arrays = input_schema
        .slots()
        .iter()
        .map(|slot| new_null_array(slot.data_type(), 1))
        .collect();
    let input = Chunk::try_new_typed_with_columns_owned_observed(
        input_schema.clone(),
        arrays,
        1,
        creation.storage_lifetime(),
        |step| -> Result<(), crate::fragment_error::NativeFragmentDecodeError> {
            use crate::native_type_construction::NativeTypeConstructionError;
            use novarocks_functions::array_projection::ArrayNormalizationStep as Step;
            match step {
                Step::CreateRecordBatchWrapper => {
                    let layout = novarocks_type_contract::owned_resources::layout::arc_layout(
                        std::alloc::Layout::new::<RecordBatch>(),
                    )
                    .map_err(NativeTypeConstructionError::Resource)?;
                    creation.request_layout(layout)?;
                }
                Step::InspectType(_)
                | Step::InspectField { .. }
                | Step::InspectDictionaryRows { .. } => {
                    creation
                        .checkpoint(CompilePhase::LowerProgram, 1)
                        .map_err(NativeTypeConstructionError::Control)?;
                }
                Step::InspectConstructionWork { mut units } => {
                    while units != 0 {
                        let quantum = units.min(256);
                        creation
                            .checkpoint(CompilePhase::LowerProgram, quantum as u32)
                            .map_err(NativeTypeConstructionError::Control)?;
                        units -= quantum;
                    }
                }
                Step::OpaqueOperationFinished => {
                    creation
                        .checkpoint(CompilePhase::LowerProgram, 0)
                        .map_err(NativeTypeConstructionError::Control)?;
                }
                _ => {
                    return Err(
                        crate::fragment_error::NativeFragmentDecodeError::inconsistent(
                            FieldPath::root("fixture.normalization"),
                            "explicit normalized CSE fixture unexpectedly rebuilt its source",
                        ),
                    );
                }
            }
            Ok(())
        },
    )
    .unwrap();
    let item = plan::ProjectItem {
        expr: Some(expr::Expr {
            original_definition_id: None,
            r#type: source_column.r#type.clone(),
            nullable: source_column.nullable,
            kind: Some(expr::expr::Kind::ColumnRef(expr::ColumnRef {
                column_id: source_column.column_id,
                qualifier: None,
                column: Some(source_column.name.clone()),
            })),
        }),
        output_name: source_column.name.clone(),
        output_column_id: source_column.column_id,
    };
    let project = plan::ProjectNode {
        items: vec![item.clone(), item],
        output_qualifier: None,
    };
    let child = NativeLoweredPlanNode {
        node: ExecNode {
            kind: ExecNodeKind::Values(ValuesNode {
                chunk: input,
                node_id: 7,
            }),
        },
        layout: SlotLayout::for_slots(input_schema.slot_ids().iter().copied()),
        output_schema: input_schema.clone(),
    };
    let mut arena = ExprArena::default();
    let lowered = lower_project_node(
        &plan::DistributedNode {
            node_id: 8,
            ..Default::default()
        },
        &project,
        FieldPath::root("project"),
        vec![child],
        &mut arena,
        &mut decoder,
    )
    .unwrap();
    drop(decoder);
    resources.finish().unwrap();
    let NativeLoweredPlanNode {
        node,
        output_schema,
        layout: _,
    } = lowered;
    let output_arrow = output_schema.arrow_schema_ref();
    assert_eq!(output_schema.slots().len(), 2);
    assert_ne!(output_schema.slot_ids()[0], output_schema.slot_ids()[1]);
    assert_eq!(
        output_arrow.fields()[0].as_ref(),
        output_arrow.fields()[1].as_ref()
    );
    let ExecNodeKind::Project(native_project) = &node.kind else {
        panic!("Project");
    };
    assert_eq!(
        native_project.exprs.len(),
        1,
        "repeated references share computation"
    );
    assert_eq!(native_project.output_indices.as_deref(), Some(&[0, 0][..]));
    let native_compute = native_project.expr_slot_schemas.as_ref().unwrap();
    assert_eq!(native_compute.len(), 1);
    assert!(native_compute[0].is_typed());
    let compute_field = native_compute[0].field_ref().clone();
    let binding = native_compute[0].typed_binding().clone();
    assert_eq!(binding, *value.typed_binding());
    assert!(
        novarocks_functions::array_projection::execution_field_is_normalized(&compute_field)
            .unwrap()
    );
    assert_eq!(original_origins.metadata_bytes_for(&compute_field), None);
    assert_eq!(assert_tree_origins(original_field, &original_origins), 8);

    let plan = ExecPlan { arena, root: node };
    let temporary = creation.begin_scope().unwrap();
    let mut temporary_request = |layout| temporary.request_layout(layout);
    let mut request = |layout| creation.request_layout(layout);
    let mut local_creation = LocalLayoutCreation::new(
        creation.storage_lifetime(),
        &mut request,
        &mut temporary_request,
        &creation,
    );
    let profile = plan
        .local_compile_profile_with_creator(
            NonZeroUsize::new(1).unwrap(),
            None,
            &mut local_creation,
        )
        .unwrap();
    let (program, runtime) = plan
        .into_local_program_and_bindings_with_creator(
            profile,
            BTreeMap::new(),
            Vec::new(),
            StaticSinkProgram::Noop,
            local_creation,
        )
        .unwrap();
    assert_eq!(runtime.scan_count(), 0);
    drop(temporary);
    let root = &program.nodes()[program.root().index()];
    let ProgramNodeKind::Project {
        expr_slot_schemas,
        output_indices,
        ..
    } = root.kind()
    else {
        panic!("frozen Project");
    };
    assert_eq!(output_indices.as_deref(), Some(&[0, 0][..]));
    let frozen_compute = expr_slot_schemas.as_ref().unwrap();
    assert_eq!(frozen_compute.len(), 1);
    assert!(Arc::ptr_eq(&frozen_compute[0].field, &compute_field));
    assert_eq!(&frozen_compute[0].binding, &binding);
    assert_eq!(root.output_layout().slots(), output_schema.slot_ids());
    let projection = frozen_compute[0].clone();
    drop(program);
    drop(runtime);
    drop(types);
    drop(input_schema);
    drop(output_schema);
    drop(output_arrow);
    drop(compute_field);
    drop(binding);
    drop(creation);
    assert!(
        tracker.current() > 0,
        "independent purpose keeps its finite creator"
    );
    assert_eq!(&projection.binding, value.typed_binding());
    drop(projection);
    assert_eq!(tracker.current(), 0);
    assert_eq!(assert_tree_origins(original_field, &original_origins), 8);
}
