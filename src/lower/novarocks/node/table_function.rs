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

use std::collections::HashSet;
use std::sync::Arc;

use arrow::datatypes::DataType;

use super::super::expr::lower_proto_expr;
use super::super::layout::{Layout, slot_schemas_from_output_columns};
use super::LoweredNode;
use super::common::check_exact_arity;
use crate::common::ids::SlotId;
use crate::exec::chunk::{ChunkSchema, ChunkSlotSchema};
use crate::exec::expr::{ExprArena, ExprNode};
use crate::exec::node::project::ProjectNode;
use crate::exec::node::table_function::{TableFunctionNode, TableFunctionOutputSlot};
use crate::exec::node::{ExecNode, ExecNodeKind};
use crate::proto::{common as proto_common, expr, plan};

pub(super) fn lower_table_function_node(
    node: &plan::DistributedNode,
    table_function: &plan::TableFunctionNode,
    mut children: Vec<LoweredNode>,
    arena: &mut ExprArena,
) -> Result<LoweredNode, String> {
    check_exact_arity("TableFunctionNode", 1, children.len())?;
    let child = children.pop().expect("child");
    validate_table_function_signature(table_function)?;

    let param_slots = table_function_param_slots(
        &child.layout,
        &table_function.output_columns,
        &table_function.args,
    )?;
    let (param_types, param_slot_schemas) =
        table_function_param_schemas(table_function, &param_slots)?;
    let result_slot_schemas = slot_schemas_from_output_columns(&table_function.output_columns)?;
    let ret_types = table_function_result_types(table_function)?;

    let mut project_exprs = Vec::with_capacity(child.layout.order().len() + param_slots.len());
    let mut project_slot_ids = Vec::with_capacity(project_exprs.capacity());
    let mut project_slot_schemas =
        Vec::with_capacity(child.output_schema.slots().len() + param_slot_schemas.len());
    for slot_schema in child.output_schema.slots() {
        let slot_id = slot_schema.slot_id();
        project_exprs
            .push(arena.push_typed(ExprNode::SlotId(slot_id), slot_schema.data_type().clone()));
        project_slot_ids.push(slot_id);
        project_slot_schemas.push(slot_schema.clone());
    }
    for ((idx, arg), slot_schema) in table_function
        .args
        .iter()
        .enumerate()
        .zip(param_slot_schemas.iter())
    {
        let expr = lower_proto_expr(arg, arena, &child.layout)
            .map_err(|err| format!("TableFunctionNode arg {idx}: {err}"))?;
        project_exprs.push(expr);
        project_slot_ids.push(slot_schema.slot_id());
        project_slot_schemas.push(slot_schema.clone());
    }
    let project_output_schema = Arc::new(ChunkSchema::try_new(project_slot_schemas)?);

    let mut output_slot_schemas =
        Vec::with_capacity(child.output_schema.slots().len() + result_slot_schemas.len());
    let mut output_slot_sources =
        Vec::with_capacity(child.output_schema.slots().len() + result_slot_schemas.len());
    let mut outer_slots = Vec::with_capacity(child.output_schema.slots().len());
    for slot_schema in child.output_schema.slots() {
        let slot_id = slot_schema.slot_id();
        outer_slots.push(slot_id);
        output_slot_schemas.push(slot_schema.clone());
        output_slot_sources.push(TableFunctionOutputSlot::Outer { slot: slot_id });
    }
    let mut fn_result_slots = Vec::with_capacity(result_slot_schemas.len());
    for (idx, slot_schema) in result_slot_schemas.iter().enumerate() {
        let slot_id = slot_schema.slot_id();
        fn_result_slots.push(slot_id);
        output_slot_schemas.push(slot_schema.clone());
        output_slot_sources.push(TableFunctionOutputSlot::Result { index: idx });
    }
    let output_schema = Arc::new(ChunkSchema::try_new(output_slot_schemas)?);
    let layout = Layout::for_slots(output_schema.slot_ids().iter().copied());

    Ok(LoweredNode {
        node: ExecNode {
            kind: ExecNodeKind::TableFunction(TableFunctionNode {
                input: Box::new(ExecNode {
                    kind: ExecNodeKind::Project(ProjectNode {
                        input: Box::new(child.node),
                        node_id: node.node_id,
                        is_subordinate: true,
                        exprs: project_exprs,
                        expr_slot_ids: project_slot_ids,
                        expr_slot_schemas: Some(project_output_schema.slots().to_vec()),
                        output_indices: None,
                        output_chunk_schema: project_output_schema,
                    }),
                }),
                node_id: node.node_id,
                function_name: table_function.function_name.clone(),
                param_slots,
                outer_slots,
                fn_result_slots,
                fn_result_required: true,
                is_left_join: table_function.is_left_join,
                param_types,
                ret_types,
                output_chunk_schema: output_schema.clone(),
                output_slot_sources,
            }),
        },
        layout,
        output_schema,
    })
}

fn validate_table_function_signature(
    table_function: &plan::TableFunctionNode,
) -> Result<(), String> {
    let function_name = table_function.function_name.to_ascii_lowercase();
    let param_types = table_function_arg_types(table_function)?;
    let ret_types = table_function_result_types(table_function)?;
    match function_name.as_str() {
        "unnest" => validate_unnest_table_function(&param_types, &ret_types),
        "unnest_bitmap" => {
            validate_table_function_arity("unnest_bitmap", &param_types, &ret_types, 1, 1)?;
            if !matches!(param_types.first(), Some(DataType::Binary)) {
                return Err(format!(
                    "table function unnest_bitmap param 0 expects Binary, got {:?}",
                    param_types.first()
                ));
            }
            if !matches!(ret_types.first(), Some(DataType::Int64)) {
                return Err(format!(
                    "table function unnest_bitmap return type expects Int64, got {:?}",
                    ret_types.first()
                ));
            }
            Ok(())
        }
        "subdivide_bitmap" => {
            validate_table_function_arity("subdivide_bitmap", &param_types, &ret_types, 2, 1)?;
            if !matches!(param_types.first(), Some(DataType::Binary)) {
                return Err(format!(
                    "table function subdivide_bitmap param 0 expects Binary, got {:?}",
                    param_types.first()
                ));
            }
            if !matches!(ret_types.first(), Some(DataType::Binary)) {
                return Err(format!(
                    "table function subdivide_bitmap return type expects Binary, got {:?}",
                    ret_types.first()
                ));
            }
            Ok(())
        }
        "generate_series" => {
            if !(param_types.len() == 2 || param_types.len() == 3) || ret_types.len() != 1 {
                return Err(format!(
                    "table function generate_series expects 2 or 3 args and 1 output, got args={} outputs={}",
                    param_types.len(),
                    ret_types.len()
                ));
            }
            if !ret_types.iter().all(is_table_function_integer_type) {
                return Err(format!(
                    "table function generate_series return type expects integer, got {:?}",
                    ret_types.first()
                ));
            }
            for (idx, param_type) in param_types.iter().enumerate() {
                if !is_table_function_integer_type(param_type) {
                    return Err(format!(
                        "table function generate_series param {idx} expects integer, got {param_type:?}"
                    ));
                }
            }
            Ok(())
        }
        _ => Err(format!(
            "unsupported native table function: {}",
            table_function.function_name
        )),
    }
}

fn validate_unnest_table_function(
    param_types: &[DataType],
    ret_types: &[DataType],
) -> Result<(), String> {
    if param_types.is_empty() {
        return Err("table function unnest requires at least one argument".to_string());
    }
    if param_types.len() != ret_types.len() {
        return Err(format!(
            "table function unnest output column count mismatch: args={} outputs={}",
            param_types.len(),
            ret_types.len()
        ));
    }
    for (idx, (param_type, ret_type)) in param_types.iter().zip(ret_types.iter()).enumerate() {
        let DataType::List(item_field) = param_type else {
            return Err(format!(
                "table function unnest param {idx} expects List, got {param_type:?}"
            ));
        };
        if item_field.data_type() != ret_type {
            return Err(format!(
                "table function unnest result type mismatch for param {idx}: item={:?} output={:?}",
                item_field.data_type(),
                ret_type
            ));
        }
    }
    Ok(())
}

fn validate_table_function_arity(
    name: &str,
    param_types: &[DataType],
    ret_types: &[DataType],
    expected_params: usize,
    expected_results: usize,
) -> Result<(), String> {
    if param_types.len() != expected_params || ret_types.len() != expected_results {
        return Err(format!(
            "table function {name} expects {expected_params} args and {expected_results} outputs, got args={} outputs={}",
            param_types.len(),
            ret_types.len()
        ));
    }
    Ok(())
}

fn is_table_function_integer_type(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64
    )
}

fn table_function_arg_types(
    table_function: &plan::TableFunctionNode,
) -> Result<Vec<DataType>, String> {
    table_function
        .args
        .iter()
        .enumerate()
        .map(|(idx, arg)| {
            let type_desc = arg
                .r#type
                .as_ref()
                .ok_or_else(|| format!("TableFunctionNode arg {idx} type missing"))?;
            super::super::decode_type(type_desc)
                .map_err(|err| format!("TableFunctionNode arg {idx} type decode failed: {err}"))
        })
        .collect()
}

fn table_function_result_types(
    table_function: &plan::TableFunctionNode,
) -> Result<Vec<DataType>, String> {
    table_function
        .output_columns
        .iter()
        .enumerate()
        .map(|(idx, column)| {
            let type_desc = column.r#type.as_ref().ok_or_else(|| {
                format!(
                    "TableFunctionNode output column {} '{}' type missing",
                    idx, column.name
                )
            })?;
            super::super::decode_type(type_desc).map_err(|err| {
                format!(
                    "TableFunctionNode output column {} '{}' type decode failed: {err}",
                    idx, column.name
                )
            })
        })
        .collect()
}

fn table_function_param_schemas(
    table_function: &plan::TableFunctionNode,
    param_slots: &[SlotId],
) -> Result<(Vec<DataType>, Vec<ChunkSlotSchema>), String> {
    let mut param_types = Vec::with_capacity(table_function.args.len());
    let mut slot_schemas = Vec::with_capacity(table_function.args.len());
    for (idx, (arg, slot_id)) in table_function
        .args
        .iter()
        .zip(param_slots.iter())
        .enumerate()
    {
        let type_desc = arg
            .r#type
            .as_ref()
            .ok_or_else(|| format!("TableFunctionNode arg {idx} type missing"))?;
        let data_type = super::super::decode_type(type_desc)
            .map_err(|err| format!("TableFunctionNode arg {idx} type decode failed: {err}"))?;
        let field =
            super::super::decode_field_type(&format!("__tf_arg_{idx}"), arg.nullable, type_desc)
                .map_err(|err| format!("TableFunctionNode arg {idx} field decode failed: {err}"))?;
        slot_schemas.push(ChunkSchema::slot_schema_from_arrow_field(*slot_id, &field)?);
        param_types.push(data_type);
    }
    Ok((param_types, slot_schemas))
}

fn table_function_param_slots(
    input_layout: &Layout,
    output_columns: &[proto_common::OutputColumn],
    args: &[expr::Expr],
) -> Result<Vec<SlotId>, String> {
    let mut used = input_layout
        .order()
        .iter()
        .map(|slot| slot.as_u32())
        .collect::<HashSet<_>>();
    used.extend(output_columns.iter().map(|column| column.column_id));
    let mut slot = u32::MAX;
    let mut slots = Vec::with_capacity(args.len());
    while slots.len() < args.len() {
        if used.insert(slot) {
            slots.push(SlotId::new(slot));
        }
        slot = slot
            .checked_sub(1)
            .ok_or_else(|| "TableFunctionNode could not allocate internal slots".to_string())?;
    }
    Ok(slots)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::datatypes::{DataType, Field};

    use super::super::{NodeLoweringContext, lower_proto_node};
    use super::*;
    use crate::common::ids::SlotId;
    use crate::exec::expr::ExprArena;
    use crate::proto::{common, expr, plan};
    use crate::types::native_proto::encode_type;

    fn type_desc(data_type: &DataType) -> common::TypeDesc {
        encode_type(data_type).expect("encode type")
    }

    fn output_column(column_id: u32, name: &str, data_type: DataType) -> common::OutputColumn {
        common::OutputColumn {
            column_id,
            name: name.to_string(),
            r#type: Some(type_desc(&data_type)),
            nullable: true,
            is_internal: false,
        }
    }

    fn column_ref(column_id: u32, data_type: DataType) -> expr::Expr {
        expr::Expr {
            r#type: Some(type_desc(&data_type)),
            nullable: true,
            kind: Some(expr::expr::Kind::ColumnRef(expr::ColumnRef {
                column_id,
                qualifier: None,
                column: None,
            })),
        }
    }

    fn physical_node(
        node_id: i32,
        kind: plan::plan_node::Kind,
        output_columns: Vec<common::OutputColumn>,
        children: Vec<plan::DistributedNode>,
    ) -> plan::DistributedNode {
        plan::DistributedNode {
            node_id,
            fragment_id: 1,
            tuple_ids: Vec::new(),
            nullable_tuple_ids: Vec::new(),
            limit: -1,
            build_runtime_filters: Vec::new(),
            probe_runtime_filters: Vec::new(),
            children,
            payload: Some(plan::distributed_node::Payload::Physical(plan::PlanNode {
                output_columns,
                kind: Some(kind),
            })),
        }
    }

    fn lower(node: &plan::DistributedNode) -> LoweredNode {
        let mut arena = ExprArena::default();
        lower_proto_node(node, &mut arena, &NodeLoweringContext::default()).expect("lower node")
    }

    #[test]
    fn lowers_native_table_function_with_outer_and_result_slots() {
        let array_type = DataType::List(Arc::new(Field::new("item", DataType::Int64, true)));
        let child_columns = vec![
            output_column(1, "id", DataType::Int64),
            output_column(2, "arr", array_type.clone()),
        ];
        let child = physical_node(
            10,
            plan::plan_node::Kind::Values(plan::ValuesNode {
                rows: Vec::new(),
                columns: child_columns.clone(),
            }),
            child_columns,
            Vec::new(),
        );
        let result_columns = vec![output_column(3, "unnest", DataType::Int64)];
        let node = physical_node(
            20,
            plan::plan_node::Kind::TableFunction(plan::TableFunctionNode {
                function_name: "unnest".to_string(),
                args: vec![column_ref(2, array_type.clone())],
                output_columns: result_columns.clone(),
                alias: Some("u".to_string()),
                is_left_join: false,
            }),
            result_columns,
            vec![child],
        );

        let lowered = lower(&node);
        assert_eq!(
            lowered.layout.order(),
            &[SlotId::new(1), SlotId::new(2), SlotId::new(3)]
        );
        assert_eq!(
            lowered.output_schema.slot_ids(),
            &[SlotId::new(1), SlotId::new(2), SlotId::new(3)]
        );

        let ExecNodeKind::TableFunction(table_function) = lowered.node.kind else {
            panic!("expected TableFunction");
        };
        assert_eq!(table_function.node_id, 20);
        assert_eq!(table_function.function_name, "unnest");
        assert_eq!(table_function.param_types, vec![array_type]);
        assert_eq!(table_function.ret_types, vec![DataType::Int64]);
        assert_eq!(
            table_function.outer_slots,
            vec![SlotId::new(1), SlotId::new(2)]
        );
        assert_eq!(table_function.fn_result_slots, vec![SlotId::new(3)]);
        assert!(table_function.fn_result_required);
        assert!(!table_function.is_left_join);
        assert_eq!(table_function.param_slots.len(), 1);
        assert_ne!(table_function.param_slots[0], SlotId::new(1));
        assert_ne!(table_function.param_slots[0], SlotId::new(2));
        assert_ne!(table_function.param_slots[0], SlotId::new(3));
        assert_eq!(
            table_function.output_chunk_schema.slot_ids(),
            &[SlotId::new(1), SlotId::new(2), SlotId::new(3)]
        );
        assert_eq!(table_function.output_slot_sources.len(), 3);
        match &table_function.output_slot_sources[0] {
            TableFunctionOutputSlot::Outer { slot } => assert_eq!(*slot, SlotId::new(1)),
            other => panic!("expected first outer slot, got {other:?}"),
        }
        match &table_function.output_slot_sources[1] {
            TableFunctionOutputSlot::Outer { slot } => assert_eq!(*slot, SlotId::new(2)),
            other => panic!("expected second outer slot, got {other:?}"),
        }
        match &table_function.output_slot_sources[2] {
            TableFunctionOutputSlot::Result { index } => assert_eq!(*index, 0),
            other => panic!("expected result slot, got {other:?}"),
        }

        let ExecNodeKind::Project(project) = table_function.input.kind else {
            panic!("expected derived Project input");
        };
        assert!(project.is_subordinate);
        assert_eq!(
            project.expr_slot_ids,
            vec![
                SlotId::new(1),
                SlotId::new(2),
                table_function.param_slots[0],
            ]
        );
        assert_eq!(
            project.output_chunk_schema.slot_ids(),
            &[
                SlotId::new(1),
                SlotId::new(2),
                table_function.param_slots[0],
            ]
        );
        assert!(matches!(project.input.kind, ExecNodeKind::Values(_)));
    }
}
