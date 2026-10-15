// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

//! Apply the final result occurrence's owned fields after carrier execution.

use std::sync::Arc;

use novarocks_execution::exec::expr::ExprArena;
use novarocks_execution::exec::node::project::ProjectNode;
use novarocks_execution::exec::node::{ExecNode, ExecNodeKind};
use novarocks_proto_codec::FieldPath;
use novarocks_proto_models::common;
use novarocks_type_contract::ValueLogicalType;
use novarocks_types::logical::NR_LOGICAL_TYPE_KEY;

/// Legacy writer columns are a compatibility projection only when a real
/// TableFinish owns the separately validated exact Arrow result relation.
pub(crate) fn legacy_result_uses_declared_fields(
    decoded: &NativeLoweredPlanNode,
    declared: &[common::OutputColumn],
) -> Result<bool, NativeFragmentDecodeError> {
    if declared.is_empty() {
        return Ok(false);
    }
    if !declared.iter().any(|column| column.is_internal) {
        return Ok(true);
    }
    if declared.iter().all(|column| column.is_internal)
        && matches!(decoded.node.kind, ExecNodeKind::TableFinish(_))
    {
        return Ok(false);
    }
    Err(NativeFragmentDecodeError::inconsistent(
        FieldPath::root("plan_fragment").field("output_columns"),
        "internal result projection requires an exact TableFinish result relation",
    ))
}

fn strict_source_domain(
    field: &arrow::datatypes::Field,
) -> Result<Option<ValueLogicalType>, NativeFragmentDecodeError> {
    let Some(value) = field.metadata().get(NR_LOGICAL_TYPE_KEY) else {
        return Ok(None);
    };
    let value = value.trim();
    for domain in [
        ValueLogicalType::Json,
        ValueLogicalType::Hll,
        ValueLogicalType::Bitmap,
        ValueLogicalType::Object,
        ValueLogicalType::Percentile,
    ] {
        if value.eq_ignore_ascii_case(domain.metadata_value().expect("nominal domain")) {
            return Ok(Some(domain));
        }
    }
    Err(NativeFragmentDecodeError::inconsistent(
        FieldPath::root("plan_fragment").field("output_columns"),
        "decoded root contains an unknown logical domain marker",
    ))
}

use crate::fragment_error::NativeFragmentDecodeError;

use crate::fragment_plan_node::NativeLoweredPlanNode;

/// Publish only the final result's exact slot order and field semantics.
///
/// Expression bindings retain their original carrier types and slot facts.
/// This adapter permits a declared nullable output to receive a non-null
/// source, but never narrows nullability or casts a carrier. Its subordinate
/// processor shares the root's reporting owner, just like existing internal
/// join and set-operation projections; LocalProgram assigns its own node ID.
pub(crate) fn apply_final_root_output_layout(
    decoded: NativeLoweredPlanNode,
    declared: &[common::OutputColumn],
    arena: &mut ExprArena,
    root_node_id: i32,

    type_decoder: &mut crate::fragment_type_decode::NativeTypeDecoder<'_, '_, '_>,
) -> Result<NativeLoweredPlanNode, NativeFragmentDecodeError> {
    let path = FieldPath::root("plan_fragment").field("output_columns");
    if declared.len() != decoded.layout.order().len()
        || decoded.output_schema.slot_ids() != decoded.layout.order()
    {
        return Err(NativeFragmentDecodeError::inconsistent(
            path,
            "final result output width differs from the decoded root layout",
        ));
    }
    // Validate slot identity before constructing any declared field owners.
    for (index, (column, slot)) in declared.iter().zip(decoded.layout.order()).enumerate() {
        if column.column_id != slot.as_u32() {
            return Err(NativeFragmentDecodeError::inconsistent(
                path.clone().index(index).field("column_id"),
                "final result output slot order differs from the decoded root layout",
            ));
        }
    }
    let actual =
        novarocks_execution::exec::pipeline::builder::output_chunk_schema_for_node(&decoded.node)
            .ok_or_else(|| {
            NativeFragmentDecodeError::inconsistent(
                path.clone(),
                "decoded root has no output schema",
            )
        })?;
    if actual.as_ref() != decoded.output_schema.as_ref() {
        return Err(NativeFragmentDecodeError::inconsistent(
            path,
            "decoded root schema differs from its actual execution node",
        ));
    }
    let output = type_decoder
        .output_layout(declared, path.clone())
        .map_err(NativeFragmentDecodeError::from)?;
    let output_schema = output.chunk_schema();
    for (index, (source, target)) in decoded
        .output_schema
        .slots()
        .iter()
        .zip(output_schema.slots())
        .enumerate()
    {
        if source.data_type() != target.data_type() {
            return Err(NativeFragmentDecodeError::inconsistent(
                path.clone().index(index).field("type"),
                "final result output carrier differs from the decoded root carrier",
            ));
        }
        if matches!(
            type_decoder,
            crate::fragment_type_decode::NativeTypeDecoder::Strong { .. }
        ) {
            if !source.is_typed()
                || !target.is_typed()
                || !type_decoder.bindings_match(
                    source.typed_binding(),
                    target.typed_binding(),
                    true,
                )?
            {
                return Err(NativeFragmentDecodeError::inconsistent(
                    path.clone().index(index).field("type"),
                    "final result output differs from the complete decoded source binding",
                ));
            }
        } else {
            let target_domain = target.field_schema().logical_type();
            // A carrier-only source may acquire the exact frozen root domain, but
            // an already known source domain cannot be erased or reinterpreted.
            for source_domain in [
                source.field_schema().logical_type(),
                strict_source_domain(source.field())?,
            ] {
                if source_domain.is_some() && source_domain != target_domain {
                    return Err(NativeFragmentDecodeError::inconsistent(
                        path.clone().index(index).field("type"),
                        "final result output logical domain differs from the decoded root domain",
                    ));
                }
            }
        }
        if source.nullable() && !target.nullable() {
            return Err(NativeFragmentDecodeError::inconsistent(
                path.clone().index(index).field("nullable"),
                "final result output cannot narrow source nullability",
            ));
        }
    }
    // Static schema equality does not prove an upstream operator publishes
    // the logical fields in its actual chunks. Materialize semantic/container
    // layouts at this final boundary even when the decoded facts already agree.
    let plain_leaf_layout = output_schema.slots().iter().all(|slot| {
        slot.field_schema().logical_type().is_none()
            && slot.field_schema().value_type().child_count() == 0
    });
    if plain_leaf_layout && decoded.output_schema.as_ref() == output_schema.as_ref() {
        return Ok(decoded);
    }

    // Slot expressions read the source's facts. Only the materialized output
    // takes the declared occurrence's field names and logical markers.
    let exprs = decoded
        .output_schema
        .slots()
        .iter()
        .map(|source| type_decoder.source_slot_expression(source, arena))
        .collect::<Result<Vec<_>, NativeFragmentDecodeError>>()?;
    let slots = decoded.layout.order().to_vec();
    Ok(NativeLoweredPlanNode {
        node: ExecNode {
            kind: ExecNodeKind::Project(ProjectNode {
                input: Box::new(decoded.node),
                node_id: root_node_id,
                is_subordinate: true,
                validate_final_result_input: true,
                exprs,
                expr_slot_ids: slots,
                expr_slot_schemas: Some(decoded.output_schema.slots().to_vec()),
                output_indices: None,
                output_chunk_schema: Arc::clone(&output_schema),
            }),
        },
        layout: decoded.layout,
        output_schema,
    })
}

#[cfg(test)]
mod tests {
    // Exercise the installed BE receive boundary with the fixture's complete
    // frozen declaration. Returned chunks retain finite creation responsibility.
    fn test_receive_root_chunks(
        payload: &[u8],
        schema: &novarocks_execution::exec::chunk::ChunkSchemaRef,
    ) -> Result<Vec<novarocks_execution::exec::chunk::Chunk>, String> {
        use novarocks_execution::runtime::exchange::{
            ExchangeColumnBinding, ExchangeKey, ExchangeSenderIdentity, ExecutionExchangeRegistry,
        };
        let registry = ExecutionExchangeRegistry::default();
        let key = ExchangeKey {
            finst_id_hi: 47,
            finst_id_lo: 83,
            node_id: 1,
        };
        let factory = novarocks_execution::exec::pipeline::operator::input_construction::try_runtime_input_construction_factory(
            novarocks_execution::exec::pipeline::pipeline::representation_construction_profile().map_err(|e|e.to_string())?,
            novarocks_execution::runtime::mem_tracker::MemTracker::new_root("root consumer fixture"),
            std::sync::Arc::new(novarocks_execution::runtime::runtime_state::RuntimeErrorState::default()),
        ).map_err(|e|e.to_string())?;
        registry.try_register_expected_chunk_schema_with_binding(
            key,
            1,
            schema.clone(),
            ExchangeColumnBinding::Positional,
            factory,
        )?;
        registry
            .decode_chunks_for_sender(key, ExchangeSenderIdentity::local(1, 1), payload)
            .map_err(|e| e.to_string())
    }

    use super::*;
    use crate::fragment_layout::decode_output_layout;
    use arrow::array::{ArrayRef, BinaryArray, StringArray};
    use arrow::datatypes::DataType;
    use novarocks_execution::exec::chunk::{Chunk, SlotLayout};
    use novarocks_execution::exec::node::values::ValuesNode;
    use novarocks_execution::exec::node::{ExecPlanBuilder, ExternalSinkRequirement};
    use novarocks_execution::exec::operators::ProjectProcessorFactory;
    use novarocks_execution::exec::pipeline::operator_factory::OperatorFactory;
    use novarocks_execution::runtime::runtime_state::RuntimeState;
    use novarocks_types::logical::{LogicalType, logical_type_of_field};

    fn column(id: u32, primitive: common::PrimitiveType, nullable: bool) -> common::OutputColumn {
        use crate::fragment_expression::fixture_sources as sources;
        use novarocks_type_contract::FunctionValueType as V;
        let declaration = match primitive {
            common::PrimitiveType::Varbinary => V::new(DataType::Binary, nullable),
            common::PrimitiveType::Varchar => V::new(DataType::Utf8, nullable),
            common::PrimitiveType::Json => sources::json_type().with_nullable(nullable).unwrap(),
            common::PrimitiveType::Bitmap => sources::binary_domain(ValueLogicalType::Bitmap)
                .with_nullable(nullable)
                .unwrap(),
            common::PrimitiveType::Hll => sources::binary_domain(ValueLogicalType::Hll)
                .with_nullable(nullable)
                .unwrap(),
            _ => panic!("fixture has no original declaration for this primitive"),
        };
        common::OutputColumn {
            column_id: id,
            name: format!("field_{id}"),
            r#type: Some(sources::type_desc_with_nullable(&declaration, nullable)),
            nullable,
            is_internal: false,
        }
    }

    fn values(columns: &[common::OutputColumn]) -> (NativeLoweredPlanNode, Chunk) {
        use crate::fragment_expression::fixture_sources as sources;
        sources::with_decoder(|decoder| -> Result<_, NativeFragmentDecodeError> {
            let values = columns
                .iter()
                .map(|column| {
                    let ty = sources::declaration_for_reference(column.r#type.as_ref().unwrap());
                    let value = if ty.data_type() == &DataType::Utf8 {
                        common::literal_value::Value::StringValue("{\"v\":1}".into())
                    } else {
                        common::literal_value::Value::BinaryValue(b"state".to_vec())
                    };
                    let literal = novarocks_proto_models::expr::LiteralExpr {
                        value: Some(common::LiteralValue { value: Some(value) }),
                    };
                    novarocks_proto_models::expr::Expr {
                        original_definition_id: None,
                        r#type: Some(sources::type_desc_with_nullable(&ty, false)),
                        nullable: false,
                        kind: Some(novarocks_proto_models::expr::expr::Kind::ConstantReference(
                            sources::constant_reference(&ty, &literal),
                        )),
                    }
                })
                .collect();
            let output = crate::fragment_plan_node::lower_values_node(
                &novarocks_proto_models::plan::DistributedNode {
                    node_id: 37,
                    limit: -1,
                    ..Default::default()
                },
                &novarocks_proto_models::plan::PlanNode {
                    output_columns: columns.to_vec(),
                    ..Default::default()
                },
                &novarocks_proto_models::plan::ValuesNode {
                    columns: columns.to_vec(),
                    rows: vec![novarocks_proto_models::plan::ExprList { values }],
                },
                FieldPath::root("source").field("values"),
                FieldPath::root("source").field("columns"),
                Vec::new(),
                &mut ExprArena::default(),
                decoder,
            )?;
            let ExecNodeKind::Values(values) = &output.node.kind else {
                panic!("values source")
            };
            let chunk = values.chunk.clone();
            Ok((output, chunk))
        })
        .unwrap()
    }
    fn apply_fixture_root_layout(
        source: NativeLoweredPlanNode,
        columns: &[common::OutputColumn],
        arena: &mut ExprArena,
    ) -> Result<NativeLoweredPlanNode, NativeFragmentDecodeError> {
        crate::fragment_expression::fixture_sources::with_decoder(|decoder| {
            apply_final_root_output_layout(source, columns, arena, 37, decoder)
        })
    }

    struct ProjectControl;
    impl novarocks_type_contract::PureCompileControl for ProjectControl {
        fn checkpoint(
            &self,
            _: novarocks_type_contract::CompilePhase,
            _: u32,
        ) -> Result<(), novarocks_type_contract::CompileControlError> {
            Ok(())
        }
    }
    fn typed_fixture_values(
        ty: &novarocks_type_contract::FunctionValueType,
    ) -> (NativeLoweredPlanNode, Chunk) {
        use crate::fragment_expression::fixture_sources as sources;
        sources::with_decoder(|decoder| -> Result<_, NativeFragmentDecodeError> {
            let column = common::OutputColumn {
                column_id: 1,
                name: "field_1".into(),
                r#type: Some(sources::type_desc_with_nullable(ty, ty.nullable())),
                nullable: ty.nullable(),
                is_internal: false,
            };
            let value = if ty.data_type() == &DataType::Utf8 {
                common::literal_value::Value::StringValue("{\"v\":1}".into())
            } else {
                common::literal_value::Value::BinaryValue(b"state".to_vec())
            };
            let literal = novarocks_proto_models::expr::LiteralExpr {
                value: Some(common::LiteralValue { value: Some(value) }),
            };
            let row = novarocks_proto_models::expr::Expr {
                original_definition_id: None,
                r#type: Some(sources::type_desc_with_nullable(ty, false)),
                nullable: false,
                kind: Some(novarocks_proto_models::expr::expr::Kind::ConstantReference(
                    sources::constant_reference(ty, &literal),
                )),
            };
            let output = crate::fragment_plan_node::lower_values_node(
                &novarocks_proto_models::plan::DistributedNode {
                    node_id: 37,
                    limit: -1,
                    ..Default::default()
                },
                &novarocks_proto_models::plan::PlanNode {
                    output_columns: vec![column.clone()],
                    ..Default::default()
                },
                &novarocks_proto_models::plan::ValuesNode {
                    columns: vec![column],
                    rows: vec![novarocks_proto_models::plan::ExprList { values: vec![row] }],
                },
                FieldPath::root("source").field("values"),
                FieldPath::root("source").field("columns"),
                Vec::new(),
                &mut ExprArena::default(),
                decoder,
            )?;
            let ExecNodeKind::Values(values) = &output.node.kind else {
                panic!("values source")
            };
            let chunk = values.chunk.clone();
            Ok((output, chunk))
        })
        .unwrap()
    }
    fn typed_final_project(
        source: NativeLoweredPlanNode,
        ty: &novarocks_type_contract::FunctionValueType,
        arena: &mut ExprArena,
    ) -> NativeLoweredPlanNode {
        crate::fragment_expression::fixture_sources::with_decoder(|decoder| {
            apply_final_root_output_layout(
                source,
                &[common::OutputColumn {
                    column_id: 1,
                    name: "opaque_alias_without_a_function_name".into(),
                    r#type: Some(
                        crate::fragment_expression::fixture_sources::type_desc_with_nullable(
                            ty,
                            ty.nullable(),
                        ),
                    ),
                    nullable: ty.nullable(),
                    is_internal: false,
                }],
                arena,
                37,
                decoder,
            )
        })
        .unwrap()
    }
    fn prepared_typed_project(
        project: &novarocks_execution::exec::node::project::ProjectNode,
        arena: ExprArena,
    ) -> Box<dyn novarocks_execution::exec::pipeline::operator::Operator> {
        use novarocks_local_program::{
            LayoutCreationFailure, ProjectExpressionSlot, ProjectExpressionSlots, StaticLayout,
        };
        use novarocks_type_contract::owned_resources::layout::{LayoutResourceError, arc_layout};
        let creator = crate::native_type_construction::NativeTypeConstruction::try_with_control(
            novarocks_execution::runtime::mem_tracker::MemTracker::new_root(
                "Native final Project fixture",
            ),
            &ProjectControl,
        )
        .unwrap();
        let scope = creator.begin_scope().unwrap();
        let source = project
            .expr_slot_schemas
            .as_ref()
            .expect("actual complete expression purposes");
        assert!(source.iter().all(|slot| slot.is_typed()));
        let observe = |layout| scope.request_layout(layout).unwrap();
        observe(std::alloc::Layout::array::<ProjectExpressionSlot>(source.len()).unwrap());
        let purposes = source
            .iter()
            .map(|slot| ProjectExpressionSlot {
                slot_id: slot.slot_id(),
                field: slot.field_ref().clone(),
                binding: slot.typed_binding().clone(),
                storage: slot.storage_lifetime(),
                unique_id: slot.unique_id(),
            })
            .collect::<Vec<_>>()
            .into_boxed_slice();
        let purposes = ProjectExpressionSlots::try_from_boxed_observed::<LayoutResourceError>(
            purposes,
            scope.storage_lifetime(),
            |layout| {
                observe(layout);
                Ok(())
            },
        )
        .unwrap();
        let output = &project.output_chunk_schema;
        observe(
            arc_layout(
                std::alloc::Layout::array::<novarocks_types::SlotId>(output.slot_ids().len())
                    .unwrap(),
            )
            .unwrap(),
        );
        let slots = Arc::from(output.slot_ids());
        observe(
            std::alloc::Layout::array::<(novarocks_type_contract::TypedValueBinding, Option<i32>)>(
                output.slots().len(),
            )
            .unwrap(),
        );
        let bindings = output
            .slots()
            .iter()
            .map(|slot| (slot.typed_binding().clone(), slot.unique_id()))
            .collect();
        let storage = scope
            .storage_lifetime()
            .retaining_observed(&output.storage_lifetime(), |layout| {
                scope.request_layout(layout)
            })
            .unwrap();
        let layout = StaticLayout::try_new_typed_with_creator::<
            crate::native_type_construction::NativeTypeConstructionError,
        >(
            output.arrow_schema_ref(),
            slots,
            bindings,
            storage,
            |layout| {
                scope
                    .request_layout(layout)
                    .map_err(LayoutCreationFailure::Creator)
            },
            &creator,
        )
        .unwrap();
        let factory = ProjectProcessorFactory::new(
            project.node_id,
            project.is_subordinate,
            Arc::new(arena),
            project.exprs.clone(),
            project.expr_slot_ids.clone(),
            Some(purposes),
            project.output_indices.clone(),
            layout,
        )
        .with_final_result_input_validation(project.validate_final_result_input);
        let mut operator = factory.create(1, 0);
        operator.set_construction_factory(novarocks_execution::exec::pipeline::operator::input_construction::try_runtime_input_construction_factory(novarocks_execution::exec::pipeline::pipeline::representation_construction_profile().unwrap(), novarocks_execution::runtime::mem_tracker::MemTracker::new_root("Native Project runtime fixture"), Arc::new(novarocks_execution::runtime::runtime_state::RuntimeErrorState::default())).unwrap());
        operator.prepare().unwrap();
        operator
            .bind_runtime_state(&RuntimeState::default())
            .unwrap();
        operator
    }

    #[test]
    fn nonempty_final_domain_is_owned_in_local_program_and_actual_ipc_output() {
        use crate::fragment_expression::fixture_sources as sources;
        for logical in [
            ValueLogicalType::Bitmap,
            ValueLogicalType::Hll,
            ValueLogicalType::Percentile,
            ValueLogicalType::Object,
            ValueLogicalType::Json,
        ] {
            let ty = if logical == ValueLogicalType::Json {
                sources::json_type()
            } else {
                sources::binary_domain(logical)
            }
            .with_nullable(false)
            .unwrap();
            let (source, input) = typed_fixture_values(&ty);
            let original_array = input.columns()[0].clone();
            let mut arena = ExprArena::default();
            let adapted = typed_final_project(source, &ty, &mut arena);
            let ExecNodeKind::Project(project) = &adapted.node.kind else {
                panic!("final presentation requires Project")
            };
            assert!(project.is_subordinate && project.validate_final_result_input);
            assert_eq!(
                arena.value_type(project.exprs[0]).unwrap().value_type(),
                ty.value_type()
            );
            assert_eq!(
                project.expr_slot_schemas.as_ref().unwrap()[0]
                    .typed_binding()
                    .value_type(),
                ty.value_type()
            );
            let mut operator = prepared_typed_project(project, arena.clone());
            let processor = operator.as_processor_mut().unwrap();
            processor
                .push_chunk(&RuntimeState::default(), input)
                .unwrap();
            let output = processor
                .pull_chunk(&RuntimeState::default())
                .unwrap()
                .unwrap();
            assert_eq!(output.len(), 1);
            assert_eq!(output.columns()[0].to_data(), original_array.to_data());
            for (source, target) in original_array
                .to_data()
                .buffers()
                .iter()
                .zip(output.columns()[0].to_data().buffers())
            {
                assert!(source.ptr_eq(target));
            }
            assert_eq!(
                output.chunk_schema().slots()[0]
                    .typed_binding()
                    .value_type(),
                ty.value_type()
            );
            assert_eq!(
                output.chunk_schema().slots()[0]
                    .field()
                    .metadata()
                    .capacity(),
                0
            );
            let schema = output.chunk_schema_ref();
            let ipc =
                novarocks_execution::runtime::exchange::encode_chunks(&[output], true).unwrap();
            let received = test_receive_root_chunks(&ipc, &schema).unwrap();
            assert_eq!(received[0].len(), 1);
            assert_eq!(
                received[0].chunk_schema().slots()[0]
                    .typed_binding()
                    .value_type(),
                ty.value_type()
            );
            assert_eq!(
                received[0].chunk_schema().slots()[0]
                    .field()
                    .metadata()
                    .capacity(),
                0
            );
            assert_eq!(received[0].columns()[0].to_data(), original_array.to_data());
            let actual_schema = project.output_chunk_schema.clone();
            let exec = ExecPlanBuilder::new(arena, adapted.node).finish().unwrap();
            let creation =
                crate::native_type_construction::NativeTypeConstruction::try_with_control(
                    novarocks_execution::runtime::mem_tracker::MemTracker::new_root(
                        "complete final result lowering",
                    ),
                    &ProjectControl,
                )
                .unwrap();
            let retained = creation.begin_scope().unwrap();
            let temporary = creation.begin_scope().unwrap();
            let storage = retained
                .storage_lifetime()
                .retaining_observed(&actual_schema.storage_lifetime(), |layout| {
                    retained.request_layout(layout)
                })
                .unwrap();
            let mut request = |layout| retained.request_layout(layout);
            let mut temporary_request = |layout| temporary.request_layout(layout);
            let mut layout_creation = novarocks_execution::exec::node::LocalLayoutCreation::new(
                storage,
                &mut request,
                &mut temporary_request,
                &creation,
            );
            let profile = exec
                .local_compile_profile_with_creator(
                    std::num::NonZeroUsize::new(1).unwrap(),
                    None,
                    &mut layout_creation,
                )
                .unwrap();
            let (local, _) = exec
                .into_local_program_and_bindings_with_creator(
                    profile,
                    std::collections::BTreeMap::new(),
                    vec![ExternalSinkRequirement::Result],
                    novarocks_local_program::StaticSinkProgram::Result,
                    layout_creation,
                )
                .unwrap();
            let root = &local.nodes()[local.root().index()];
            assert!(matches!(
                root.kind(),
                novarocks_local_program::ProgramNodeKind::Project {
                    validate_final_result_input: true,
                    ..
                }
            ));
            assert_eq!(
                root.output_layout().slot_metadata_at(0).unwrap().0,
                ty.value_type()
            );
            assert_eq!(
                root.output_layout().schema().fields()[0]
                    .metadata()
                    .capacity(),
                0
            );
            assert_eq!(local.nodes().len(), 2);
        }
    }

    #[test]
    fn internal_flags_cannot_bypass_an_ordinary_result_layout() {
        use common::PrimitiveType as P;
        let columns = [
            column(1, P::Varbinary, false),
            column(2, P::Varbinary, false),
        ];
        let (source, _) = values(&columns);
        assert!(legacy_result_uses_declared_fields(&source, &columns).unwrap());
        assert!(!legacy_result_uses_declared_fields(&source, &[]).unwrap());
        let mut internal = columns.clone();
        internal[0].is_internal = true;
        assert!(legacy_result_uses_declared_fields(&source, &internal).is_err());
        internal[1].is_internal = true;
        assert!(legacy_result_uses_declared_fields(&source, &internal).is_err());
    }

    #[test]
    fn unknown_source_markers_cannot_be_erased_or_reassigned() {
        use crate::fragment_expression::fixture_sources as sources;
        let plain = novarocks_type_contract::FunctionValueType::new(DataType::Binary, false);
        for _ in 0..3 {
            let (mut columns, logical) = sources::relation_fields(&[(1, "state", plain.clone())]);
            columns[0].field.as_mut().unwrap().logical_type_id = Some(u32::MAX);
            let arena = ExprArena::default();
            let error = sources::with_decoder(|decoder| {
                decoder.execution_relation_schema(&columns, &logical, FieldPath::root("source"))
            })
            .unwrap_err();
            assert!(
                error.contains("logical") || error.contains("reference"),
                "{error}"
            );
            assert!(
                arena
                    .node(novarocks_execution::exec::expr::ExprId(0))
                    .is_none()
            );
        }
    }

    #[test]
    fn known_source_domains_cannot_be_erased_or_reinterpreted() {
        use common::PrimitiveType as P;
        for (source_type, target_type) in [
            (P::Bitmap, P::Hll),
            (P::Bitmap, P::Varbinary),
            (P::Json, P::Varchar),
        ] {
            let (source, _) = values(&[column(1, source_type, false)]);
            let mut arena = ExprArena::default();
            let error =
                apply_fixture_root_layout(source, &[column(1, target_type, false)], &mut arena)
                    .unwrap_err();
            assert!(
                error
                    .to_string()
                    .contains("complete decoded source binding")
            );
            assert!(
                arena
                    .node(novarocks_execution::exec::expr::ExprId(0))
                    .is_none()
            );
        }
    }

    #[test]
    fn final_layout_refuses_runtime_domain_conflict_before_retagging() {
        use crate::fragment_expression::fixture_sources as sources;
        let target = sources::binary_domain(ValueLogicalType::Bitmap)
            .with_nullable(false)
            .unwrap();
        for domain in [ValueLogicalType::Hll, ValueLogicalType::Percentile] {
            let (source, _) = typed_fixture_values(&target);
            let (_, conflicting_input) =
                typed_fixture_values(&sources::binary_domain(domain).with_nullable(false).unwrap());
            let mut arena = ExprArena::default();
            let adapted = typed_final_project(source, &target, &mut arena);
            let ExecNodeKind::Project(project) = &adapted.node.kind else {
                panic!("semantic result requires runtime validation")
            };
            let mut operator = prepared_typed_project(project, arena);
            let processor = operator.as_processor_mut().unwrap();
            assert!(
                processor
                    .push_chunk(&RuntimeState::default(), conflicting_input)
                    .is_err()
            );
            assert!(
                processor
                    .pull_chunk(&RuntimeState::default())
                    .unwrap()
                    .is_none()
            );
        }
    }
    #[test]
    fn equal_static_semantic_layout_still_materializes_actual_output_fields() {
        use crate::fragment_expression::fixture_sources as sources;
        let target = sources::binary_domain(ValueLogicalType::Bitmap)
            .with_nullable(false)
            .unwrap();
        let (source, _) = typed_fixture_values(&target);
        let (_, input) = typed_fixture_values(&target);
        let original = input.columns()[0].to_data();
        let mut arena = ExprArena::default();
        let adapted = typed_final_project(source, &target, &mut arena);
        let ExecNodeKind::Project(project) = &adapted.node.kind else {
            panic!("complete static facts do not prove runtime publication")
        };
        let mut operator = prepared_typed_project(project, arena);
        let processor = operator.as_processor_mut().unwrap();
        processor
            .push_chunk(&RuntimeState::default(), input)
            .unwrap();
        let output = processor
            .pull_chunk(&RuntimeState::default())
            .unwrap()
            .unwrap();
        assert_eq!(
            output.chunk_schema().slots()[0]
                .typed_binding()
                .value_type(),
            target.value_type()
        );
        assert_eq!(
            output.chunk_schema().slots()[0]
                .field()
                .metadata()
                .capacity(),
            0
        );
        assert_eq!(output.columns()[0].to_data(), original);
        for (source, target) in original
            .buffers()
            .iter()
            .zip(output.columns()[0].to_data().buffers())
        {
            assert!(source.ptr_eq(target));
        }
        let (_, plain) = typed_fixture_values(&novarocks_type_contract::FunctionValueType::new(
            DataType::Binary,
            false,
        ));
        assert!(
            processor
                .push_chunk(&RuntimeState::default(), plain)
                .is_err()
        );
        assert!(
            processor
                .pull_chunk(&RuntimeState::default())
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn exact_schema_retains_the_original_node_and_schema_owner() {
        let columns = [column(1, common::PrimitiveType::Varbinary, false)];
        let (source, _) = values(&columns);
        let schema = Arc::clone(&source.output_schema);
        let mut arena = ExprArena::default();
        let adapted = apply_fixture_root_layout(source, &columns, &mut arena).unwrap();
        assert!(matches!(adapted.node.kind, ExecNodeKind::Values(_)));
        assert!(Arc::ptr_eq(&schema, &adapted.output_schema));
        assert!(
            arena
                .node(novarocks_execution::exec::expr::ExprId(0))
                .is_none()
        );
    }

    #[test]
    fn final_layout_can_widen_but_cannot_narrow_nullable() {
        let source_columns = [column(1, common::PrimitiveType::Varbinary, false)];
        let (source, _) = values(&source_columns);
        let mut arena = ExprArena::default();
        let adapted = apply_fixture_root_layout(
            source,
            &[column(1, common::PrimitiveType::Varbinary, true)],
            &mut arena,
        )
        .unwrap();
        assert!(adapted.output_schema.slots()[0].nullable());

        let (source, _) = values(&[column(1, common::PrimitiveType::Varbinary, true)]);
        assert!(
            apply_fixture_root_layout(
                source,
                &[column(1, common::PrimitiveType::Varbinary, false)],
                &mut ExprArena::default()
            )
            .unwrap_err()
            .contains("cannot narrow")
        );
    }

    #[test]
    fn missing_wrong_or_reordered_slots_are_rejected_before_expressions() {
        use common::PrimitiveType as P;
        let source_columns = [
            column(1, P::Varbinary, false),
            column(2, P::Varbinary, false),
        ];
        for declared in [
            vec![column(1, P::Bitmap, false)],
            vec![column(1, P::Bitmap, false), column(3, P::Hll, false)],
            vec![column(2, P::Bitmap, false), column(1, P::Hll, false)],
            vec![column(1, P::Bitmap, false), column(1, P::Hll, false)],
        ] {
            let (source, _) = values(&source_columns);
            let mut arena = ExprArena::default();
            assert!(apply_fixture_root_layout(source, &declared, &mut arena).is_err());
            assert!(
                arena
                    .node(novarocks_execution::exec::expr::ExprId(0))
                    .is_none()
            );
        }
    }

    #[test]
    fn carrier_changes_and_missing_wire_types_are_rejected() {
        use common::PrimitiveType as P;
        let mut missing = column(1, P::Bitmap, false);
        missing.r#type = None;
        for declared in [column(1, P::Json, false), missing] {
            let (source, _) = values(&[column(1, P::Varbinary, false)]);
            let mut arena = ExprArena::default();
            assert!(apply_fixture_root_layout(source, &[declared], &mut arena).is_err());
            assert!(
                arena
                    .node(novarocks_execution::exec::expr::ExprId(0))
                    .is_none()
            );
        }
    }
}
