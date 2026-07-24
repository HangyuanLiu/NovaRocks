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
use crate::exec::expr::ExprArena;
use crate::exec::node::aggregate::{
    AggFunction, AggOrderSpec, AggTypeSignature, AggregateNode, AggregateRuntimeFilterSpec,
    StreamingPreaggregationMode,
};
use crate::exec::node::{ExecNode, ExecNodeKind};

use crate::protocol::common::error::FieldPath;
use crate::protocol::starrocks::decode::error::StarRocksFragmentDecodeError;
use crate::protocol::starrocks::decode::expr::{lower_expr_node_at, lower_t_expr_at};
use crate::protocol::starrocks::decode::layout::{Layout, chunk_schema_for_layout};
use crate::protocol::starrocks::decode::node::Lowered;
use crate::runtime::query_options::QueryOptions;
use crate::thrift::descriptors;

use crate::thrift::{exprs, plan_nodes};
use arrow::datatypes::{DataType, Field, Fields};

/// Lower an AGGREGATION_NODE plan node to a `Lowered` ExecNode.
pub(crate) fn lower_aggregate_node(
    child: Lowered,
    node: &plan_nodes::TPlanNode,
    arena: &mut ExprArena,
    desc_tbl: Option<&descriptors::TDescriptorTable>,
    query_opts: &QueryOptions,
    out_layout: &Layout,
    last_query_id: Option<&str>,
    fe_addr: Option<&crate::protocol::starrocks::decode::StarRocksExternalDependencyDraft>,
    node_path: FieldPath,
) -> Result<Lowered, StarRocksFragmentDecodeError> {
    let payload_path = node_path.field("agg_node");
    let Some(agg) = node.agg_node.as_ref() else {
        return Err(StarRocksFragmentDecodeError::missing(
            payload_path,
            "AGGREGATION_NODE missing agg_node payload",
        ));
    };

    // Grouping keys
    let mut group_by =
        Vec::with_capacity(agg.grouping_exprs.as_ref().map(|v| v.len()).unwrap_or(0));
    if let Some(exprs) = &agg.grouping_exprs {
        for (expr_index, e) in exprs.iter().enumerate() {
            group_by.push(lower_t_expr_at(
                e,
                arena,
                &child.layout,
                last_query_id,
                fe_addr,
                payload_path
                    .clone()
                    .field("grouping_exprs")
                    .index(expr_index),
            )?);
        }
    }
    for expr_id in &group_by {
        if let Some(dt) = arena.data_type(*expr_id)
            && matches!(dt, arrow::datatypes::DataType::LargeBinary)
        {
            return Err(StarRocksFragmentDecodeError::unsupported(
                payload_path.clone().field("grouping_exprs"),
                "VARIANT is not supported in GROUP BY",
            ));
        }
    }

    // Agg functions
    let mut functions = Vec::new();
    for (function_index, e) in agg.aggregate_functions.iter().enumerate() {
        let function_path = payload_path
            .clone()
            .field("aggregate_functions")
            .index(function_index);
        let root = e.nodes.first().ok_or_else(|| {
            StarRocksFragmentDecodeError::missing(
                function_path.clone().field("nodes").index(0),
                "empty agg expr",
            )
        })?;
        let is_merge = root
            .agg_expr
            .as_ref()
            .map(|agg_expr| agg_expr.is_merge_agg)
            .unwrap_or(false);
        let fn_name_raw = root
            .fn_
            .as_ref()
            .map(|f| f.name.function_name.to_lowercase())
            .ok_or_else(|| {
                StarRocksFragmentDecodeError::missing(
                    function_path.clone().field("nodes").index(0).field("fn"),
                    "agg expr missing function name",
                )
            })?;
        let (fn_name, order) =
            encode_aggregate(root, &fn_name_raw, query_opts).map_err(|error| {
                StarRocksFragmentDecodeError::invalid_value(
                    function_path.clone().field("nodes").index(0).field("fn"),
                    error,
                )
            })?;
        let rewrite_ds_hll_merge_to_union =
            !agg.need_finalize && fn_name_raw == "ds_hll_count_distinct_merge";
        let fn_name = if rewrite_ds_hll_merge_to_union {
            "ds_hll_count_distinct_union".to_string()
        } else {
            fn_name
        };
        let mut type_sig = agg_type_signature_from_node(root).map_err(|error| {
            StarRocksFragmentDecodeError::invalid_value(
                function_path.clone().field("nodes").index(0).field("fn"),
                error,
            )
        })?;
        if rewrite_ds_hll_merge_to_union
            && let Some(intermediate_type) = type_sig.intermediate_type.clone()
        {
            type_sig.output_type = Some(intermediate_type);
        }

        // Lower arguments
        let mut args = Vec::new();
        let mut idx = 1; // Skip root
        for _ in 0..root.num_children {
            args.push(lower_expr_node_at(
                &e.nodes,
                &mut idx,
                arena,
                &child.layout,
                last_query_id,
                fe_addr,
                function_path.clone(),
            )?);
        }

        let inputs =
            select_aggregate_inputs(&fn_name_raw, is_merge, args, arena).map_err(|error| {
                StarRocksFragmentDecodeError::invalid_value(function_path.clone(), error)
            })?;
        let func = AggFunction {
            name: fn_name.clone(),
            inputs,
            input_is_intermediate: is_merge,
            types: Some(type_sig),
            order,
        };
        functions.push(func);
    }
    let input_is_intermediate = functions.iter().all(|f| f.input_is_intermediate);
    let desc_tbl = desc_tbl.ok_or_else(|| {
        StarRocksFragmentDecodeError::missing(
            payload_path.clone(),
            "aggregate node lowering requires descriptor table for output chunk schema",
        )
    })?;
    let output_chunk_schema = chunk_schema_for_layout(desc_tbl, out_layout).map_err(|error| {
        StarRocksFragmentDecodeError::invalid_value(payload_path.clone(), error)
    })?;

    let streaming_preaggregation_mode = agg.streaming_preaggregation_mode.map(|mode| {
        use crate::thrift::plan_nodes::TStreamingPreaggregationMode;
        match mode {
            TStreamingPreaggregationMode::AUTO => StreamingPreaggregationMode::Auto,
            TStreamingPreaggregationMode::FORCE_STREAMING => {
                StreamingPreaggregationMode::ForceStreaming
            }
            TStreamingPreaggregationMode::FORCE_PREAGGREGATION => {
                StreamingPreaggregationMode::ForcePreaggregation
            }
            TStreamingPreaggregationMode::LIMITED_MEM => StreamingPreaggregationMode::LimitedMem,
            _ => StreamingPreaggregationMode::Auto,
        }
    });

    Ok(Lowered {
        node: ExecNode {
            kind: ExecNodeKind::Aggregate(AggregateNode {
                input: Box::new(child.node),
                node_id: node.node_id,
                group_by,
                functions,
                need_finalize: agg.need_finalize,
                input_is_intermediate,
                output_chunk_schema,
                runtime_filter_spec: AggregateRuntimeFilterSpec::Native {
                    topn_producers: Vec::new(),
                },
                streaming_preaggregation_mode,
            }),
        },
        layout: out_layout.clone(),
    })
}

fn agg_type_signature_from_node(node: &exprs::TExprNode) -> Result<AggTypeSignature, String> {
    let fn_ = node
        .fn_
        .as_ref()
        .ok_or_else(|| "agg expr missing function".to_string())?;
    let intermediate_type = fn_
        .aggregate_fn
        .as_ref()
        .and_then(|agg_fn| arrow_type_from_desc(&agg_fn.intermediate_type));
    let output_type = arrow_type_from_desc(&fn_.ret_type)
        .ok_or_else(|| "agg ret_type missing/unsupported".to_string())?;
    let input_arg_type = fn_.arg_types.first().and_then(arrow_type_from_desc);
    Ok(AggTypeSignature {
        intermediate_type,
        output_type: Some(output_type),
        input_arg_type,
    })
}

/// Resolve an aggregate's execution-layer base name and its structured ORDER BY /
/// DISTINCT metadata from the thrift node. array_agg's DISTINCT folds into the
/// base name (`array_agg_distinct`); group_concat's DISTINCT and max-length, and
/// both functions' ORDER BY, are carried structurally in [`AggOrderSpec`].
fn encode_aggregate(
    node: &exprs::TExprNode,
    fn_name: &str,
    query_opts: &QueryOptions,
) -> Result<(String, AggOrderSpec), String> {
    if matches!(
        fn_name,
        "array_agg" | "array_agg_distinct" | "array_unique_agg"
    ) {
        let aggregate_fn = node.fn_.as_ref().and_then(|f| f.aggregate_fn.as_ref());
        let base = match fn_name {
            "array_agg" => {
                let is_distinct = aggregate_fn
                    .and_then(|agg| agg.is_distinct)
                    .unwrap_or(false);
                if is_distinct {
                    "array_agg_distinct"
                } else {
                    "array_agg"
                }
            }
            "array_agg_distinct" => "array_agg_distinct",
            "array_unique_agg" => "array_unique_agg",
            _ => unreachable!("unexpected array_agg variant: {fn_name}"),
        };
        let is_asc_order = aggregate_fn
            .and_then(|agg| agg.is_asc_order.clone())
            .unwrap_or_default();
        let nulls_first = aggregate_fn
            .and_then(|agg| agg.nulls_first.clone())
            .unwrap_or_default();
        if is_asc_order.len() != nulls_first.len() {
            return Err(format!(
                "array_agg order metadata length mismatch: is_asc_order={} nulls_first={}",
                is_asc_order.len(),
                nulls_first.len()
            ));
        }
        return Ok((
            base.to_string(),
            AggOrderSpec {
                is_asc_order,
                nulls_first,
                is_distinct: false,
                group_concat_max_len: None,
            },
        ));
    }

    if fn_name != "group_concat" && fn_name != "string_agg" {
        return Ok((fn_name.to_string(), AggOrderSpec::default()));
    }

    let aggregate_fn = node.fn_.as_ref().and_then(|f| f.aggregate_fn.as_ref());
    let is_distinct = aggregate_fn
        .and_then(|agg| agg.is_distinct)
        .unwrap_or(false);
    let is_asc_order = aggregate_fn
        .and_then(|agg| agg.is_asc_order.clone())
        .unwrap_or_default();
    let nulls_first = aggregate_fn
        .and_then(|agg| agg.nulls_first.clone())
        .unwrap_or_default();
    if is_asc_order.len() != nulls_first.len() {
        return Err(format!(
            "group_concat order metadata length mismatch: is_asc_order={} nulls_first={}",
            is_asc_order.len(),
            nulls_first.len()
        ));
    }
    let group_concat_max_len = query_opts.group_concat_max_len.unwrap_or(1024).max(4);
    Ok((
        fn_name.to_string(),
        AggOrderSpec {
            is_asc_order,
            nulls_first,
            is_distinct,
            group_concat_max_len: Some(group_concat_max_len),
        },
    ))
}

fn select_aggregate_inputs(
    fn_name: &str,
    is_merge: bool,
    args: Vec<crate::exec::expr::ExprId>,
    arena: &mut ExprArena,
) -> Result<Vec<crate::exec::expr::ExprId>, String> {
    let select_first_for_merge = |args: Vec<crate::exec::expr::ExprId>,
                                  name: &str|
     -> Result<Vec<crate::exec::expr::ExprId>, String> {
        let first = args
            .into_iter()
            .next()
            .ok_or_else(|| format!("{name} merge input missing"))?;
        Ok(vec![first])
    };

    match fn_name {
        // FE rewrites count_if(expr) to count_if(1, expr). Keep only the effective input:
        // predicate for update, intermediate count for merge.
        "count_if" => {
            if is_merge {
                let first = args
                    .into_iter()
                    .next()
                    .ok_or_else(|| "count_if merge input missing".to_string())?;
                return Ok(vec![first]);
            }
            if args.len() == 1 {
                return Ok(args);
            }
            if args.len() == 2 {
                let mut it = args.into_iter();
                let _ = it.next();
                let predicate = it
                    .next()
                    .ok_or_else(|| "count_if predicate input missing".to_string())?;
                return Ok(vec![predicate]);
            }
            return Err(format!(
                "count_if expects 1 or 2 arguments, got {}",
                args.len()
            ));
        }
        // FE may still keep constant arguments when building merge-stage aggregate calls.
        // These aggregates only consume the first intermediate state argument during merge.
        "count_distinct" | "multi_distinct_count" if is_merge => {
            return select_first_for_merge(args, "count_distinct");
        }
        "ds_theta_count_distinct"
        | "ds_hll_count_distinct"
        | "ds_hll_count_distinct_union"
        | "ds_hll_count_distinct_merge"
        | "approx_count_distinct_hll_sketch"
            if is_merge =>
        {
            return select_first_for_merge(args, fn_name);
        }
        // Merge group_concat consumes intermediate state; FE may still carry separator in args.
        "group_concat" if is_merge => {
            return select_first_for_merge(args, "group_concat");
        }
        // Merge array_agg consumes intermediate state only.
        "array_agg" | "array_agg_distinct" | "array_unique_agg" if is_merge => {
            return select_first_for_merge(args, fn_name);
        }
        // Merge map_agg consumes intermediate map state only.
        "map_agg" if is_merge => {
            return select_first_for_merge(args, "map_agg");
        }
        // Merge approx_top_k consumes intermediate binary state only.
        "approx_top_k" if is_merge => {
            return select_first_for_merge(args, "approx_top_k");
        }
        // Merge min_n/max_n consumes serialized intermediate state only.
        "min_n" | "max_n" if is_merge => {
            return select_first_for_merge(args, fn_name);
        }
        // Merge dict_merge consumes intermediate state; FE may still carry threshold in args.
        "dict_merge" if is_merge => {
            return select_first_for_merge(args, "dict_merge");
        }
        // Merge max_by/min_by consumes serialized intermediate state only; FE may still keep
        // original value/key arguments on the merge node.
        "max_by" | "max_by_v2" | "min_by" | "min_by_v2" if is_merge => {
            return select_first_for_merge(args, fn_name);
        }
        // Merge percentile_approx consumes serialized intermediate state; FE may still carry
        // constant quantile/compression arguments in the merge-stage function call.
        "percentile_approx" | "percentile_approx_weighted" if is_merge => {
            return select_first_for_merge(args, fn_name);
        }
        "mann_whitney_u_test" | "percentile_cont" | "percentile_disc" | "percentile_disc_lc"
            if is_merge =>
        {
            return select_first_for_merge(args, fn_name);
        }
        _ => {}
    }

    pack_struct_inputs(args, arena)
}

fn pack_struct_inputs(
    args: Vec<crate::exec::expr::ExprId>,
    arena: &mut ExprArena,
) -> Result<Vec<crate::exec::expr::ExprId>, String> {
    if args.len() <= 1 {
        return Ok(args);
    }

    let mut fields = Vec::with_capacity(args.len());
    for (idx, expr_id) in args.iter().enumerate() {
        let data_type = arena
            .data_type(*expr_id)
            .ok_or_else(|| "aggregate input type missing".to_string())?;
        fields.push(Field::new(format!("f{idx}"), data_type.clone(), true));
    }
    let struct_type = DataType::Struct(Fields::from(fields));
    let struct_expr = arena.push_typed(
        crate::exec::expr::ExprNode::StructExpr { fields: args },
        struct_type,
    );
    Ok(vec![struct_expr])
}

#[cfg(all(test, feature = "compat"))]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use arrow::datatypes::Schema;
    use arrow::record_batch::RecordBatch;

    use super::lower_aggregate_node;
    use crate::exec::chunk::{Chunk, ChunkSchema};
    use crate::exec::expr::ExprArena;
    use crate::exec::node::aggregate::AggregateRuntimeFilterSpec;
    use crate::exec::node::values::ValuesNode;
    use crate::exec::node::{ExecNode, ExecNodeKind};
    use crate::protocol::common::error::FieldPath;
    use crate::protocol::starrocks::decode::layout::Layout;
    use crate::protocol::starrocks::decode::node::Lowered;
    use crate::runtime::query_options::QueryOptions;
    use crate::thrift::{descriptors, plan_nodes};

    fn empty_plan_node() -> plan_nodes::TPlanNode {
        plan_nodes::TPlanNode::new(
            20,
            plan_nodes::TPlanNodeType::AGGREGATION_NODE,
            1,
            -1,
            vec![],
            vec![],
            None,
            false,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
        )
    }

    #[test]
    fn compat_aggregate_lowering_ignores_runtime_filters() {
        let mut node = empty_plan_node();
        node.agg_node = Some(plan_nodes::TAggregationNode::new(
            None,
            Vec::new(),
            0,
            0,
            true,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            Some(Vec::new()),
            None,
        ));
        let layout = Layout {
            order: Vec::new(),
            index: HashMap::new(),
        };
        let schema = Arc::new(Schema::empty());
        let child = Lowered {
            node: ExecNode {
                kind: ExecNodeKind::Values(ValuesNode {
                    chunk: Chunk::new_with_chunk_schema(
                        RecordBatch::new_empty(schema),
                        Arc::new(ChunkSchema::empty()),
                    ),
                    node_id: 10,
                }),
            },
            layout: layout.clone(),
        };
        let descriptors = descriptors::TDescriptorTable::new(Vec::new(), Vec::new(), None, None);

        let lowered = lower_aggregate_node(
            child,
            &node,
            &mut ExprArena::default(),
            Some(&descriptors),
            &QueryOptions::default(),
            &layout,
            None,
            None,
            FieldPath::root("plan_node"),
        )
        .expect("compat aggregate lowering");
        let ExecNodeKind::Aggregate(aggregate) = lowered.node.kind else {
            panic!("compat aggregate node")
        };
        let AggregateRuntimeFilterSpec::Native { topn_producers } = aggregate.runtime_filter_spec
        else {
            panic!("compat lowering must construct empty native aggregate producer specs")
        };
        assert!(topn_producers.is_empty());
    }
}
