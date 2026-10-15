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

//! Fragment aggregate-node decoding.

use std::sync::Arc;

use arrow::datatypes::DataType;

use crate::fragment_decode_context::NativePlanDecodeContext;
use crate::fragment_error::NativeFragmentDecodeError;
use crate::fragment_expression::decode_expr_for_slot_layout_with_types;

use crate::fragment_plan_node::NativeLoweredPlanNode as DecodedNode;
use crate::fragment_plan_node::build_slot_projection;
use novarocks_execution::exec::chunk::ChunkSchema;
use novarocks_execution::exec::chunk::SlotLayout as Layout;
use novarocks_execution::exec::expr::ExprArena;
use novarocks_execution::exec::node::aggregate::{
    AggFunction, AggOrderSpec, AggTypeSignature, AggregateNode, AggregateRuntimeFilterSpec,
};
use novarocks_execution::exec::node::{ExecNode, ExecNodeKind};
use novarocks_functions::aggregate_types::mangle_distinct_aggregate_name;
use novarocks_functions::{
    AggregateOverloadIdentity, AggregateStateFormatIdentity, ResolvedAggregateSignature,
};
use novarocks_proto_codec::FieldPath;
use novarocks_proto_models::plan;

#[expect(
    clippy::too_many_arguments,
    reason = "The frozen native boundary keeps independently validated inputs explicit."
)]
pub(crate) fn lower_hash_aggregate_node(
    node: &plan::DistributedNode,
    physical: &plan::PlanNode,
    aggregate: &plan::HashAggregateNode,
    path: FieldPath,
    physical_output_path: FieldPath,
    mut children: Vec<DecodedNode>,
    arena: &mut ExprArena,
    ctx: &NativePlanDecodeContext,

    type_decoder: &mut crate::fragment_type_decode::NativeTypeDecoder<'_, '_, '_>,
) -> Result<DecodedNode, NativeFragmentDecodeError> {
    let child = children.pop().expect("child");
    if aggregate.is_merge.len() != aggregate.aggregates.len() {
        return Err(NativeFragmentDecodeError::inconsistent(
            path.clone().field("is_merge"),
            format!(
                "HashAggregateNode is_merge length mismatch: is_merge={} aggregates={}",
                aggregate.is_merge.len(),
                aggregate.aggregates.len()
            ),
        ));
    }
    let mode = plan::AggMode::try_from(aggregate.mode).map_err(|_| {
        NativeFragmentDecodeError::invalid_enum(
            path.clone().field("mode"),
            format!("HashAggregateNode unknown mode {}", aggregate.mode),
        )
    })?;
    if mode == plan::AggMode::Unspecified {
        return Err(NativeFragmentDecodeError::invalid_enum(
            path.clone().field("mode"),
            "HashAggregateNode mode is unspecified",
        ));
    }
    let output_layout = aggregate.output_layout.as_ref().ok_or_else(|| {
        NativeFragmentDecodeError::missing(
            path.clone().field("output_layout"),
            "HashAggregateNode output_layout missing",
        )
    })?;
    let group_key_path = path
        .clone()
        .field("output_layout")
        .field("group_key_columns");
    let aggregate_columns_path = path
        .clone()
        .field("output_layout")
        .field("aggregate_columns");
    let decoded_group_key_columns =
        type_decoder.output_layout(&output_layout.group_key_columns, group_key_path.clone())?;
    let decoded_aggregate_columns = type_decoder.output_layout(
        &output_layout.aggregate_columns,
        aggregate_columns_path.clone(),
    )?;
    let mut aggregate_slot_schemas = decoded_group_key_columns.slot_schemas().to_vec();
    aggregate_slot_schemas.extend(decoded_aggregate_columns.slot_schemas().iter().cloned());
    let aggregate_layout =
        Layout::for_slots(aggregate_slot_schemas.iter().map(|slot| slot.slot_id()));
    let aggregate_output_schema = Arc::new(ChunkSchema::try_new(aggregate_slot_schemas).map_err(
        |error| NativeFragmentDecodeError::inconsistent(path.clone().field("output_layout"), error),
    )?);
    if output_layout.aggregate_columns.len() != aggregate.aggregates.len() {
        return Err(NativeFragmentDecodeError::inconsistent(
            path.clone()
                .field("output_layout")
                .field("aggregate_columns"),
            format!(
                "HashAggregateNode output_layout aggregate column mismatch: columns={} aggregates={}",
                output_layout.aggregate_columns.len(),
                aggregate.aggregates.len()
            ),
        ));
    }
    if output_layout.group_key_columns.len() != aggregate.group_by.len() {
        return Err(NativeFragmentDecodeError::inconsistent(
            path.clone()
                .field("output_layout")
                .field("group_key_columns"),
            format!(
                "HashAggregateNode output_layout group key mismatch: columns={} group_by={}",
                output_layout.group_key_columns.len(),
                aggregate.group_by.len()
            ),
        ));
    }

    let group_by = aggregate
        .group_by
        .iter()
        .enumerate()
        .map(|(idx, expr)| {
            decode_expr_for_slot_layout_with_types(
                expr,
                path.clone().field("group_by").index(idx),
                arena,
                &child.layout,
                type_decoder,
            )
        })
        .collect::<Result<Vec<_>, NativeFragmentDecodeError>>()?;
    for (idx, expr_id) in group_by.iter().enumerate() {
        let expression_type = arena.data_type(*expr_id).ok_or_else(|| {
            NativeFragmentDecodeError::missing(
                path.clone().field("group_by").index(idx),
                format!("HashAggregateNode group key {idx} has no decoded type"),
            )
        })?;
        let layout_type = decoded_group_key_columns
            .slot_schemas()
            .get(idx)
            .expect("group key output arity was validated")
            .data_type();
        if expression_type != layout_type {
            return Err(NativeFragmentDecodeError::invalid_value(
                group_key_path.clone().index(idx).field("type"),
                format!(
                    "HashAggregateNode group key {idx} output type drift: expression={expression_type:?} output_layout={layout_type:?}"
                ),
            ));
        }
        if matches!(expression_type, DataType::LargeBinary) {
            return Err(NativeFragmentDecodeError::unsupported(
                path.clone().field("group_by").index(idx),
                "VARIANT is not supported in GROUP BY",
            ));
        }
    }

    let need_finalize = matches!(mode, plan::AggMode::Single | plan::AggMode::Global);
    let mut functions = Vec::with_capacity(aggregate.aggregates.len());
    let mut resolved_aggregates = Vec::with_capacity(aggregate.aggregates.len());
    for (idx, call) in aggregate.aggregates.iter().enumerate() {
        let is_merge = aggregate.is_merge[idx];
        if call.name.eq_ignore_ascii_case("count_if") && !call.order_by.is_empty() {
            return Err(NativeFragmentDecodeError::unsupported(
                path.clone()
                    .field("aggregates")
                    .index(idx)
                    .field("order_by"),
                format!("HashAggregateNode aggregate {idx} count_if does not support ORDER BY"),
            ));
        }
        let result_type = call.result_type.as_ref().ok_or_else(|| {
            NativeFragmentDecodeError::missing(
                path.clone()
                    .field("aggregates")
                    .index(idx)
                    .field("result_type"),
                format!("HashAggregateNode aggregate {idx} result_type missing"),
            )
        })?;
        let result_type = type_decoder.data_type(
            result_type,
            path.clone()
                .field("aggregates")
                .index(idx)
                .field("result_type"),
        )?;
        let function_name = aggregate_function_name(call);
        let call_path = path.clone().field("aggregates").index(idx);
        let frozen = type_decoder.frozen_aggregate(
            call.resolved_signature.as_ref(),
            ctx,
            crate::fragment_original_requests::OriginalOccurrence::Aggregate {
                node: node.node_id,
                call: idx,
            },
            call_path.clone().field("resolved_signature"),
        )?;
        let planned = if let Some(frozen) = frozen.as_deref() {
            type_decoder.validate_aggregate_name(
                frozen,
                ctx,
                &function_name,
                call_path.clone().field("name"),
            )?;
            type_decoder.aggregate_projection(frozen)?
        } else {
            // Explicit legacy inline expansion only; Strong never reaches this adapter.
            decode_resolved_aggregate_signature(
                call.resolved_signature.as_ref(),
                &call.name,
                call_path.clone().field("resolved_signature"),
                type_decoder,
            )?
        };
        if !is_merge {
            type_decoder.validate_actual_aggregate_arguments(
                call.resolved_signature.as_ref().expect("checked signature"),
                call.args
                    .iter()
                    .chain(call.order_by.iter().filter_map(|key| key.expr.as_ref())),
                call_path.clone(),
            )?;
        }
        let logical_arg_types = aggregate_logical_arg_types(call, call_path.clone(), type_decoder)?;
        let update_arg_types =
            aggregate_signature_arg_types(call, call_path.clone(), type_decoder)?;
        if !is_merge && update_arg_types != planned.argument_types {
            return Err(NativeFragmentDecodeError::invalid_value(
                call_path
                    .clone()
                    .field("resolved_signature")
                    .field("argument_types"),
                format!(
                    "HashAggregateNode aggregate {idx} update argument type drift: expressions={update_arg_types:?} planned={:?}",
                    planned.argument_types
                ),
            ));
        }
        let function_catalog = ctx.function_catalog().ok_or_else(|| {
            NativeFragmentDecodeError::missing(
                call_path.clone(),
                "HashAggregateNode requires the process engine function catalog",
            )
        })?;
        if frozen.is_none() {
            let selected = if is_merge {
                function_catalog.resolve_selected_aggregate_update_trusted(
                    &function_name,
                    &planned.overload,
                    &planned.argument_types,
                )
            } else {
                function_catalog.resolve_aggregate_update_trusted(
                    &function_name,
                    &logical_arg_types,
                    &update_arg_types,
                )
            }
            .map_err(|error| {
                NativeFragmentDecodeError::invalid_value(
                    call_path.clone(),
                    format!("HashAggregateNode aggregate {idx} resolution: {error}"),
                )
            })?;
            if selected != planned {
                return Err(NativeFragmentDecodeError::invalid_value(
                    call_path.clone().field("resolved_signature"),
                    format!(
                        "HashAggregateNode aggregate {idx} resolved signature drift: planned={planned:?} catalog={selected:?}"
                    ),
                ));
            }
        }
        let selected = planned;
        if result_type != selected.output_type {
            return Err(NativeFragmentDecodeError::invalid_value(
                call_path.clone().field("result_type"),
                format!(
                    "HashAggregateNode aggregate {idx} SQL result type drift: wire={result_type:?} expected={:?}",
                    selected.output_type
                ),
            ));
        }
        let expected_phase_type = if need_finalize {
            &selected.output_type
        } else {
            &selected.intermediate_type
        };
        let phase_output_type = decoded_aggregate_columns
            .slot_schemas()
            .get(idx)
            .expect("aggregate output arity was validated")
            .data_type();
        if phase_output_type != expected_phase_type {
            return Err(NativeFragmentDecodeError::invalid_value(
                aggregate_columns_path.clone().index(idx).field("type"),
                format!(
                    "HashAggregateNode aggregate {idx} phase output type drift: output_layout={phase_output_type:?} expected={expected_phase_type:?}"
                ),
            ));
        }
        if let Some(frozen) = frozen.as_deref() {
            let selection = frozen.selected();
            let novarocks_functions::FunctionResultType::Scalar(result) = &selection.result_type
            else {
                unreachable!("checked frozen scalar result");
            };
            type_decoder.require_descriptor_binding(
                call.result_type.as_ref().expect("checked result type"),
                result,
                call_path.clone().field("result_type"),
            )?;
            let phase = if need_finalize {
                result
            } else {
                &selection
                    .aggregate
                    .as_ref()
                    .expect("checked aggregate")
                    .intermediate_type
            };
            type_decoder.require_slot_binding(
                &decoded_aggregate_columns.slot_schemas()[idx],
                phase,
                true,
                aggregate_columns_path.clone().index(idx),
            )?;
            if !is_merge {
                for (index, expr) in call
                    .args
                    .iter()
                    .chain(call.order_by.iter().filter_map(|item| item.expr.as_ref()))
                    .enumerate()
                {
                    let desc = expr.r#type.as_ref().ok_or_else(|| {
                        NativeFragmentDecodeError::missing(
                            call_path.clone().field("args").index(index),
                            "aggregate argument type is absent",
                        )
                    })?;
                    let novarocks_functions::FunctionArgumentType::Value(expected) =
                        &selection.argument_types[index]
                    else {
                        return Err(NativeFragmentDecodeError::unsupported(
                            call_path.clone(),
                            "aggregate cannot consume a lambda",
                        ));
                    };
                    type_decoder.require_argument_descriptor_binding(
                        desc,
                        expected,
                        call_path.clone().field("args").index(index),
                    )?;
                }
            } else if let Some(expr) = call.args.first() {
                let desc = expr.r#type.as_ref().ok_or_else(|| {
                    NativeFragmentDecodeError::missing(
                        call_path.clone().field("args"),
                        "merge state type is absent",
                    )
                })?;
                type_decoder.require_merge_state_descriptor_binding(
                    desc,
                    &selection
                        .aggregate
                        .as_ref()
                        .expect("checked aggregate")
                        .intermediate_type,
                    call_path.clone().field("args"),
                )?;
            }
        }
        let signature_input_arg_type = selected.argument_types.first().cloned();
        let intermediate_type = Some(selected.intermediate_type.clone());
        let signature_output_type = selected.output_type.clone();

        // Every phase reads what the plan says it reads. A merge phase says
        // one state input, and the sender states it rather than leaving the
        // receiver to reconstruct it from an output column identity.
        if is_merge && (call.args.len() != 1 || !call.order_by.is_empty()) {
            return Err(NativeFragmentDecodeError::invalid_value(
                path.clone().field("aggregates").index(idx),
                format!(
                    "HashAggregateNode merge aggregate {idx} requires exactly one state input and no ORDER BY"
                ),
            ));
        }
        let raw_args = lower_aggregate_update_inputs(
            call,
            idx,
            path.clone().field("aggregates").index(idx),
            &child,
            arena,
            type_decoder,
        )?;
        let inputs = select_aggregate_inputs(
            &call.name.to_ascii_lowercase(),
            is_merge,
            raw_args,
            arena,
            type_decoder,
            path.clone().field("aggregates").index(idx),
        )?;
        functions.push(AggFunction {
            frozen,
            name: function_name,
            inputs,
            input_is_intermediate: is_merge,
            types: Some(AggTypeSignature {
                intermediate_type,
                output_type: Some(signature_output_type),
                input_arg_type: signature_input_arg_type,
            }),
            order: aggregate_order_spec(call, ctx),
        });
        resolved_aggregates.push(selected);
    }

    let input_is_intermediate = functions.iter().all(|f| f.input_is_intermediate);
    let aggregate_node = DecodedNode {
        node: ExecNode {
            kind: ExecNodeKind::Aggregate(AggregateNode {
                input: Box::new(child.node),
                node_id: node.node_id,
                group_by,
                functions,
                resolved_aggregates,
                need_finalize,
                input_is_intermediate,
                output_chunk_schema: aggregate_output_schema.clone(),
                runtime_filter_spec: AggregateRuntimeFilterSpec::empty(),
                streaming_preaggregation_mode: None,
            }),
        },
        layout: aggregate_layout,
        output_schema: aggregate_output_schema,
    };
    let Some((visible_output_columns, visible_path)) = (if !aggregate.output_columns.is_empty() {
        Some((
            aggregate.output_columns.as_slice(),
            path.clone().field("output_columns"),
        ))
    } else if !physical.output_columns.is_empty() {
        Some((physical.output_columns.as_slice(), physical_output_path))
    } else {
        None
    }) else {
        return Ok(aggregate_node);
    };
    let visible_layout = Layout::for_slots(
        type_decoder
            .output_layout(visible_output_columns, visible_path.clone())?
            .slot_ids()
            .iter()
            .copied(),
    );
    if visible_layout.order() == aggregate_node.layout.order() {
        return Ok(aggregate_node);
    }
    build_slot_projection(
        "HashAggregateNode",
        aggregate_node,
        visible_output_columns,
        visible_path,
        node.node_id,
        arena,
        type_decoder,
    )
}

fn aggregate_function_name(call: &plan::PlanAggregateCall) -> String {
    // Delegate the DISTINCT-mangling table to the single source of truth; this
    // proto-typed wrapper differs from the planner-typed one only in its input.
    mangle_distinct_aggregate_name(&call.name, call.distinct)
}

pub(crate) fn decode_resolved_aggregate_signature(
    signature: Option<&plan::ResolvedAggregateSignature>,
    function_name: &str,
    signature_path: FieldPath,

    type_decoder: &mut crate::fragment_type_decode::NativeTypeDecoder<'_, '_, '_>,
) -> Result<ResolvedAggregateSignature, NativeFragmentDecodeError> {
    let signature = signature.ok_or_else(|| {
        NativeFragmentDecodeError::missing(
            signature_path.clone(),
            format!("aggregate {function_name} exact resolved signature missing"),
        )
    })?;
    let overload =
        AggregateOverloadIdentity::try_new(&signature.overload_identity).map_err(|error| {
            NativeFragmentDecodeError::invalid_value(
                signature_path.clone().field("overload_identity"),
                error.to_string(),
            )
        })?;
    let argument_types = signature
        .argument_types
        .iter()
        .enumerate()
        .map(|(idx, data_type)| {
            type_decoder.data_type(
                data_type,
                signature_path.clone().field("argument_types").index(idx),
            )
        })
        .collect::<Result<Vec<_>, _>>()?;
    let intermediate_type = signature.intermediate_type.as_ref().ok_or_else(|| {
        NativeFragmentDecodeError::missing(
            signature_path.clone().field("intermediate_type"),
            format!("aggregate {function_name} intermediate type missing"),
        )
    })?;
    let intermediate_type = type_decoder.data_type(
        intermediate_type,
        signature_path.clone().field("intermediate_type"),
    )?;
    let output_type = signature.output_type.as_ref().ok_or_else(|| {
        NativeFragmentDecodeError::missing(
            signature_path.clone().field("output_type"),
            format!("aggregate {function_name} output type missing"),
        )
    })?;
    let output_type =
        type_decoder.data_type(output_type, signature_path.clone().field("output_type"))?;
    let state_format = AggregateStateFormatIdentity::try_new(&signature.state_format_identity)
        .map_err(|error| {
            NativeFragmentDecodeError::invalid_value(
                signature_path.clone().field("state_format_identity"),
                error.to_string(),
            )
        })?;
    Ok(ResolvedAggregateSignature {
        overload,
        argument_types,
        intermediate_type,
        output_type,
        state_format,
    })
}

fn aggregate_signature_arg_types(
    call: &plan::PlanAggregateCall,
    path: FieldPath,

    type_decoder: &mut crate::fragment_type_decode::NativeTypeDecoder<'_, '_, '_>,
) -> Result<Vec<DataType>, NativeFragmentDecodeError> {
    let mut types = call
        .args
        .iter()
        .enumerate()
        .map(|(idx, expr)| {
            let ty = expr.r#type.as_ref().ok_or_else(|| {
                NativeFragmentDecodeError::missing(
                    path.clone().field("args").index(idx).field("type"),
                    format!("aggregate {} argument {idx} type missing", call.name),
                )
            })?;
            type_decoder.data_type(ty, path.clone().field("args").index(idx).field("type"))
        })
        .collect::<Result<Vec<_>, _>>()?;
    for (idx, item) in call.order_by.iter().enumerate() {
        let expr = item.expr.as_ref().ok_or_else(|| {
            NativeFragmentDecodeError::missing(
                path.clone().field("order_by").index(idx).field("expr"),
                format!("aggregate {} order_by[{idx}] expr missing", call.name),
            )
        })?;
        let data_type = expr.r#type.as_ref().ok_or_else(|| {
            NativeFragmentDecodeError::missing(
                path.clone()
                    .field("order_by")
                    .index(idx)
                    .field("expr")
                    .field("type"),
                format!("aggregate {} order_by[{idx}] type missing", call.name),
            )
        })?;
        types.push(
            type_decoder.data_type(
                data_type,
                path.clone()
                    .field("order_by")
                    .index(idx)
                    .field("expr")
                    .field("type"),
            )?,
        );
    }
    Ok(types)
}

fn aggregate_logical_arg_types(
    call: &plan::PlanAggregateCall,
    path: FieldPath,

    type_decoder: &mut crate::fragment_type_decode::NativeTypeDecoder<'_, '_, '_>,
) -> Result<Vec<DataType>, NativeFragmentDecodeError> {
    call.args
        .iter()
        .enumerate()
        .map(|(idx, expr)| {
            let ty = expr.r#type.as_ref().ok_or_else(|| {
                NativeFragmentDecodeError::missing(
                    path.clone().field("args").index(idx).field("type"),
                    format!("aggregate {} argument {idx} type missing", call.name),
                )
            })?;
            type_decoder.data_type(ty, path.clone().field("args").index(idx).field("type"))
        })
        .collect()
}

fn lower_aggregate_update_inputs(
    call: &plan::PlanAggregateCall,
    aggregate_idx: usize,
    path: FieldPath,
    child: &DecodedNode,
    arena: &mut ExprArena,

    type_decoder: &mut crate::fragment_type_decode::NativeTypeDecoder<'_, '_, '_>,
) -> Result<Vec<novarocks_execution::exec::expr::ExprId>, NativeFragmentDecodeError> {
    if call.name.eq_ignore_ascii_case("count_if") && !call.order_by.is_empty() {
        return Err(NativeFragmentDecodeError::unsupported(
            path.clone().field("order_by"),
            format!(
                "HashAggregateNode aggregate {aggregate_idx} count_if does not support ORDER BY"
            ),
        ));
    }
    let mut inputs = Vec::with_capacity(call.args.len() + call.order_by.len());
    for (arg_idx, expr) in call.args.iter().enumerate() {
        inputs.push(decode_expr_for_slot_layout_with_types(
            expr,
            path.clone().field("args").index(arg_idx),
            arena,
            &child.layout,
            type_decoder,
        )?);
    }
    for (order_idx, item) in call.order_by.iter().enumerate() {
        let item_path = path.clone().field("order_by").index(order_idx);
        let expr = item.expr.as_ref().ok_or_else(|| {
            NativeFragmentDecodeError::missing(
                item_path.clone().field("expr"),
                format!(
                    "HashAggregateNode aggregate {aggregate_idx} order_by[{order_idx}] expr missing"
                ),
            )
        })?;
        inputs.push(decode_expr_for_slot_layout_with_types(
            expr,
            item_path.field("expr"),
            arena,
            &child.layout,
            type_decoder,
        )?);
    }
    Ok(inputs)
}

fn aggregate_order_spec(
    call: &plan::PlanAggregateCall,
    ctx: &NativePlanDecodeContext,
) -> AggOrderSpec {
    AggOrderSpec {
        is_asc_order: call.order_by.iter().map(|item| item.asc).collect(),
        nulls_first: call.order_by.iter().map(|item| item.nulls_first).collect(),
        is_distinct: call.distinct,
        group_concat_max_len: if call.name.eq_ignore_ascii_case("group_concat")
            || call.name.eq_ignore_ascii_case("string_agg")
        {
            ctx.query_options()
                .and_then(|options| options.group_concat_max_len())
        } else {
            None
        },
    }
}

fn select_aggregate_inputs(
    fn_name: &str,
    is_merge: bool,
    args: Vec<novarocks_execution::exec::expr::ExprId>,
    arena: &mut ExprArena,
    type_decoder: &mut crate::fragment_type_decode::NativeTypeDecoder<'_, '_, '_>,
    path: FieldPath,
) -> Result<Vec<novarocks_execution::exec::expr::ExprId>, NativeFragmentDecodeError> {
    if is_merge {
        return args
            .into_iter()
            .next()
            .map(|expr| vec![expr])
            .ok_or_else(|| {
                NativeFragmentDecodeError::invalid_value(
                    path.clone(),
                    format!("{fn_name} merge input missing"),
                )
            });
    }
    if fn_name == "count_if" {
        return match args.len() {
            1 => Ok(args),
            2 => Ok(vec![args[1]]),
            other => Err(NativeFragmentDecodeError::invalid_value(
                path.clone(),
                format!("count_if expects 1 or 2 arguments, got {other}"),
            )),
        };
    }
    type_decoder.packed_struct_expression(args, arena, false, path)
}
