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
use arrow::datatypes::DataType;

use novarocks::exec::expr::ExprArena;
use novarocks::exec::expr::ExprNode;
use novarocks::exec::node::project::ProjectNode;
use novarocks::exec::node::sort::{SortExpression, SortNode, SortTopNType};
use novarocks::exec::node::{ExecNode, ExecNodeKind};

use crate::protocol::starrocks::decode::error::StarRocksFragmentDecodeError;
use crate::protocol::starrocks::decode::expr::lower_t_expr_at;
use crate::protocol::starrocks::decode::layout::{Layout, chunk_schema_for_layout};
use crate::protocol::starrocks::decode::node::Lowered;
use crate::protocol::starrocks::decode::type_lowering::arrow_type_from_desc;
use crate::thrift::descriptors;
use novarocks::common::ids::SlotId;
use novarocks::protocol::FieldPath;

use crate::thrift::{exprs, plan_nodes, types};

/// Lower a SORT_NODE plan node to a `Lowered` ExecNode.
pub(crate) fn lower_sort_node(
    children: Vec<Lowered>,
    node: &plan_nodes::TPlanNode,
    arena: &mut ExprArena,
    out_layout: Layout,
    desc_tbl: Option<&descriptors::TDescriptorTable>,
    last_query_id: Option<&str>,
    fe_addr: Option<&crate::protocol::starrocks::decode::StarRocksExternalDependencyDraft>,
    node_path: FieldPath,
) -> Result<Lowered, StarRocksFragmentDecodeError> {
    let payload_path = node_path.clone().field("sort_node");
    if children.len() != 1 {
        return Err(StarRocksFragmentDecodeError::inconsistent(
            node_path,
            format!("SORT_NODE expected 1 child, got {}", children.len()),
        ));
    }
    let child = children.into_iter().next().expect("child");
    let Some(sort) = node.sort_node.as_ref() else {
        return Err(StarRocksFragmentDecodeError::missing(
            payload_path,
            "SORT_NODE missing sort_node payload",
        ));
    };
    let info = &sort.sort_info;

    // StarRocks' `sort_tuple_slot_exprs` is used to materialize an internal tuple for sorting.
    // It should not change the Sort node's visible output columns.
    //
    // FE may assign a new output tuple_id and/or reorder the output slots for a Sort node.
    // novarocks's Sort operator does not reorder columns by itself, so we insert a Project to
    // permute the child columns to match `out_layout` when needed.
    let (child_for_sort, sort_input_layout, sort_output_layout) = normalize_sort_input(
        child,
        arena,
        &out_layout,
        node.node_id,
        desc_tbl,
        sort,
        last_query_id,
        fe_addr,
        payload_path.clone(),
    )?;

    let order_by = build_sort_order_by(
        info,
        arena,
        &sort_input_layout,
        &format!("SORT_NODE node_id={}", node.node_id),
        last_query_id,
        fe_addr,
        payload_path.clone().field("sort_info"),
    )?;

    let limit = if node.limit >= 0 {
        Some(node.limit as usize)
    } else {
        None
    };

    let offset = match sort.offset.unwrap_or(0) {
        v if v < 0 => {
            return Err(StarRocksFragmentDecodeError::out_of_range(
                payload_path.clone().field("offset"),
                format!("SORT_NODE offset must be >= 0, got {v}"),
            ));
        }
        v => v as usize,
    };

    let use_top_n = sort.use_top_n;
    let topn_type = parse_sort_topn_type(sort, node.node_id).map_err(|error| {
        StarRocksFragmentDecodeError::invalid_enum(payload_path.clone().field("topn_type"), error)
    })?;
    // StarRocks enforces `offset == 0` for rank-based topn semantics.
    // Keep the same invariant so execution does not need fallback behavior.
    if use_top_n && topn_type != SortTopNType::RowNumber && offset != 0 {
        return Err(StarRocksFragmentDecodeError::inconsistent(
            payload_path.clone().field("offset"),
            format!(
                "SORT_NODE node_id={} topn_type {:?} requires offset=0, got {}",
                node.node_id, topn_type, offset
            ),
        ));
    }

    let max_buffered_rows =
        parse_optional_positive_i64(sort.max_buffered_rows, node.node_id, "max_buffered_rows")
            .map_err(|error| {
                StarRocksFragmentDecodeError::out_of_range(
                    payload_path.clone().field("max_buffered_rows"),
                    error,
                )
            })?;
    let max_buffered_bytes =
        parse_optional_positive_i64(sort.max_buffered_bytes, node.node_id, "max_buffered_bytes")
            .map_err(|error| {
                StarRocksFragmentDecodeError::out_of_range(
                    payload_path.clone().field("max_buffered_bytes"),
                    error,
                )
            })?;

    // Per-partition TopN (StarRocks PartitionSort). partition_exprs are grouping
    // keys; partition_limit caps rows per group. Compiled like ordering exprs but
    // grouping-only — we mark them asc/nulls_first so exec makes groups adjacent.
    let partition_exprs = match sort.partition_exprs.as_ref() {
        None => Vec::new(),
        Some(exprs) => {
            let mut out = Vec::with_capacity(exprs.len());
            for (expr_index, e) in exprs.iter().enumerate() {
                let expr_id = lower_t_expr_at(
                    e,
                    arena,
                    &sort_input_layout,
                    last_query_id,
                    fe_addr,
                    payload_path
                        .clone()
                        .field("partition_exprs")
                        .index(expr_index),
                )?;
                out.push(SortExpression {
                    expr: expr_id,
                    asc: true,
                    nulls_first: true,
                });
            }
            out
        }
    };
    let partition_limit = match sort.partition_limit {
        None => None,
        // StarRocks uses partition_limit = -1 as the "no per-partition cap"
        // sentinel. It emits the field whenever partition_exprs is present
        // (SortNode.java setPartition_limit is guarded by getPartitionExprs() !=
        // null), so an ordinary global TopN still arrives with partition_limit =
        // -1 and an empty partition_exprs list. Treat any negative value as
        // absent rather than rejecting it — StarRocks itself gates on
        // `partitionLimit >= 0`. Rejecting it broke every FE-issued
        // `ORDER BY ... LIMIT n`.
        Some(v) if v < 0 => None,
        Some(v) => Some(v as usize),
    };
    if partition_limit.is_some() && !use_top_n {
        return Err(StarRocksFragmentDecodeError::inconsistent(
            payload_path.clone().field("partition_limit"),
            format!(
                "SORT_NODE node_id={} partition_limit requires use_top_n=true",
                node.node_id
            ),
        ));
    }
    // Partition-TopN is intentionally decoupled from the global limit: the per-partition
    // cap (partition_limit) replaces the global row cap, so use_top_n=true is valid even
    // when there is no global limit. Only reject the combination when BOTH partition_limit
    // and global limit are absent — that would be an unconstrained TopN with no limit at all.
    if use_top_n && limit.is_none() && partition_limit.is_none() {
        return Err(StarRocksFragmentDecodeError::inconsistent(
            payload_path.clone().field("use_top_n"),
            format!(
                "SORT_NODE node_id={} use_top_n=true requires node.limit >= 0",
                node.node_id
            ),
        ));
    }

    Ok(Lowered {
        node: ExecNode {
            kind: ExecNodeKind::Sort(SortNode {
                input: Box::new(child_for_sort),
                node_id: node.node_id,
                use_top_n,
                order_by,
                limit,
                offset,
                topn_type,
                max_buffered_rows,
                max_buffered_bytes,
                partition_exprs,
                partition_limit,
            }),
        },
        layout: sort_output_layout,
    })
}

fn parse_sort_topn_type(
    sort: &plan_nodes::TSortNode,
    node_id: i32,
) -> Result<SortTopNType, String> {
    let Some(topn_type) = sort.topn_type else {
        return Ok(SortTopNType::RowNumber);
    };
    // Keep explicit mapping for `DENSE_RANK` even though current StarRocks FE
    // ranking-window pushdown normally emits ROW_NUMBER/RANK only.
    match topn_type {
        plan_nodes::TTopNType::ROW_NUMBER => Ok(SortTopNType::RowNumber),
        plan_nodes::TTopNType::RANK => Ok(SortTopNType::Rank),
        plan_nodes::TTopNType::DENSE_RANK => Ok(SortTopNType::DenseRank),
        other => Err(format!(
            "SORT_NODE node_id={} has unknown topn_type value {}",
            node_id, other.0
        )),
    }
}

fn parse_optional_positive_i64(
    value: Option<i64>,
    node_id: i32,
    field_name: &str,
) -> Result<Option<usize>, String> {
    let Some(v) = value else {
        return Ok(None);
    };
    if v <= 0 {
        return Err(format!(
            "SORT_NODE node_id={} {} must be > 0 when set, got {}",
            node_id, field_name, v
        ));
    }
    Ok(Some(v as usize))
}

fn build_sort_order_by(
    info: &plan_nodes::TSortInfo,
    arena: &mut ExprArena,
    input_layout: &Layout,
    node_label: &str,
    last_query_id: Option<&str>,
    fe_addr: Option<&crate::protocol::starrocks::decode::StarRocksExternalDependencyDraft>,
    sort_info_path: FieldPath,
) -> Result<Vec<SortExpression>, StarRocksFragmentDecodeError> {
    let key_count = info.ordering_exprs.len();
    if info.is_asc_order.len() != key_count {
        return Err(StarRocksFragmentDecodeError::inconsistent(
            sort_info_path.clone().field("is_asc_order"),
            format!(
                "{node_label} sort_info.is_asc_order length mismatch: ordering_exprs={} is_asc_order={}",
                key_count,
                info.is_asc_order.len()
            ),
        ));
    }
    if info.nulls_first.len() != key_count {
        return Err(StarRocksFragmentDecodeError::inconsistent(
            sort_info_path.clone().field("nulls_first"),
            format!(
                "{node_label} sort_info.nulls_first length mismatch: ordering_exprs={} nulls_first={}",
                key_count,
                info.nulls_first.len()
            ),
        ));
    }

    let mut order_by = Vec::with_capacity(key_count);
    for (i, expr) in info.ordering_exprs.iter().enumerate() {
        let expr_id = lower_t_expr_at(
            expr,
            arena,
            input_layout,
            last_query_id,
            fe_addr,
            sort_info_path.clone().field("ordering_exprs").index(i),
        )?;
        order_by.push(SortExpression {
            expr: expr_id,
            asc: info.is_asc_order[i],
            nulls_first: info.nulls_first[i],
        });
    }
    Ok(order_by)
}

fn normalize_sort_input(
    child: Lowered,
    arena: &mut ExprArena,
    out_layout: &Layout,
    node_id: i32,
    desc_tbl: Option<&descriptors::TDescriptorTable>,
    sort: &plan_nodes::TSortNode,
    last_query_id: Option<&str>,
    fe_addr: Option<&crate::protocol::starrocks::decode::StarRocksExternalDependencyDraft>,
    sort_path: FieldPath,
) -> Result<(ExecNode, Layout, Layout), StarRocksFragmentDecodeError> {
    let effective_out_layout = normalize_sort_output_layout(&child.layout, out_layout)
        .map_err(|error| StarRocksFragmentDecodeError::invalid_value(sort_path.clone(), error))?;
    let original_child_layout = child.layout.clone();
    let mut child = child;

    if child.layout.order.len() != effective_out_layout.order.len() {
        child = build_sort_tuple_projection(
            child,
            arena,
            sort,
            &effective_out_layout,
            node_id,
            desc_tbl,
            last_query_id,
            fe_addr,
            sort_path.clone(),
        )?;
    }

    if child.layout.order == effective_out_layout.order {
        let sort_input_layout = add_slot_aliases(child.layout.clone(), &original_child_layout);
        return Ok((child.node, sort_input_layout, effective_out_layout));
    }

    // Map output slot_id -> child physical index. We require a 1:1 mapping to avoid guessing.
    let mut child_slot_set = std::collections::HashSet::<types::TSlotId>::new();
    for (_t, slot_id) in &child.layout.order {
        if !child_slot_set.insert(*slot_id) {
            return Err(StarRocksFragmentDecodeError::invalid_value(
                sort_path.clone(),
                format!(
                    "SORT_NODE child layout has duplicate slot_id={}, cannot build a stable mapping",
                    slot_id
                ),
            ));
        }
    }

    // Build a project that reorders columns into `out_layout` order using slot ids.
    let mut exprs = Vec::with_capacity(effective_out_layout.order.len());
    for (tuple_id, slot_id) in &effective_out_layout.order {
        if !child_slot_set.contains(slot_id) {
            return Err(StarRocksFragmentDecodeError::invalid_value(
                sort_path.clone(),
                format!(
                    "SORT_NODE output layout refers to missing child slot: tuple_id={} slot_id={}",
                    tuple_id, slot_id
                ),
            ));
        }
        let slot_id = SlotId::try_from(*slot_id).map_err(|error| {
            StarRocksFragmentDecodeError::invalid_value(sort_path.clone(), error)
        })?;
        exprs.push(arena.push(ExprNode::SlotId(slot_id)));
    }

    let projected = ExecNode {
        kind: ExecNodeKind::Project(ProjectNode {
            input: Box::new(child.node),
            node_id,
            is_subordinate: true,
            exprs,
            expr_slot_ids: effective_out_layout
                .order
                .iter()
                .map(|(_, slot_id)| SlotId::try_from(*slot_id))
                .collect::<Result<Vec<_>, _>>()
                .map_err(|error| {
                    StarRocksFragmentDecodeError::invalid_value(sort_path.clone(), error)
                })?,
            expr_slot_schemas: None,
            output_indices: None,
            output_chunk_schema: chunk_schema_for_layout(
                desc_tbl.ok_or_else(|| {
                    StarRocksFragmentDecodeError::missing(
                        sort_path.clone(),
                        "SORT_NODE requires desc_tbl for projection chunk schema",
                    )
                })?,
                &effective_out_layout,
            )
            .map_err(|error| {
                StarRocksFragmentDecodeError::invalid_value(sort_path.clone(), error)
            })?,
        }),
    };

    // Expressions (e.g. ordering_exprs) may still reference the original child tuple_id.
    // Add alias entries so slot refs can be resolved deterministically without falling back.
    let mut sort_input_layout = effective_out_layout.clone();
    sort_input_layout = add_slot_aliases(sort_input_layout, &child.layout);
    sort_input_layout = add_slot_aliases(sort_input_layout, &original_child_layout);

    Ok((projected, sort_input_layout, effective_out_layout))
}

fn build_sort_tuple_projection(
    child: Lowered,
    arena: &mut ExprArena,
    sort: &plan_nodes::TSortNode,
    out_layout: &Layout,
    node_id: i32,
    desc_tbl: Option<&descriptors::TDescriptorTable>,
    last_query_id: Option<&str>,
    fe_addr: Option<&crate::protocol::starrocks::decode::StarRocksExternalDependencyDraft>,
    sort_path: FieldPath,
) -> Result<Lowered, StarRocksFragmentDecodeError> {
    let sort_tuple_exprs = sort.sort_info.sort_tuple_slot_exprs.as_ref().ok_or_else(|| {
        StarRocksFragmentDecodeError::missing(
            sort_path.clone().field("sort_info").field("sort_tuple_slot_exprs"),
            format!(
                "SORT_NODE node_id={} output column count mismatch: child={} sort={} and sort_tuple_slot_exprs is missing",
                node_id,
                child.layout.order.len(),
                out_layout.order.len()
            ),
        )
    })?;

    if sort_tuple_exprs.len() > out_layout.order.len() {
        return Err(StarRocksFragmentDecodeError::inconsistent(
            sort_path
                .clone()
                .field("sort_info")
                .field("sort_tuple_slot_exprs"),
            format!(
                "SORT_NODE node_id={} sort_tuple_slot_exprs longer than output layout: exprs={} out_layout={}",
                node_id,
                sort_tuple_exprs.len(),
                out_layout.order.len()
            ),
        ));
    }

    let mut exprs = Vec::with_capacity(out_layout.order.len());
    for (expr_index, expr) in sort_tuple_exprs.iter().enumerate() {
        exprs.push(lower_t_expr_at(
            expr,
            arena,
            &child.layout,
            last_query_id,
            fe_addr,
            sort_path
                .clone()
                .field("sort_info")
                .field("sort_tuple_slot_exprs")
                .index(expr_index),
        )?);
    }

    if exprs.len() < out_layout.order.len() {
        let pre_agg_exprs = sort.pre_agg_exprs.as_ref().ok_or_else(|| {
            StarRocksFragmentDecodeError::missing(
                sort_path.clone().field("pre_agg_exprs"),
                format!(
                    "SORT_NODE node_id={} has {} output slots but only {} sort_tuple_slot_exprs and missing pre_agg_exprs",
                    node_id,
                    out_layout.order.len(),
                    exprs.len()
                ),
            )
        })?;
        let pre_agg_slots = sort.pre_agg_output_slot_id.as_ref().ok_or_else(|| {
            StarRocksFragmentDecodeError::missing(
                sort_path.clone().field("pre_agg_output_slot_id"),
                format!(
                    "SORT_NODE node_id={} has {} output slots but only {} sort_tuple_slot_exprs and missing pre_agg_output_slot_id",
                    node_id,
                    out_layout.order.len(),
                    exprs.len()
                ),
            )
        })?;
        if pre_agg_exprs.len() != pre_agg_slots.len() {
            return Err(StarRocksFragmentDecodeError::inconsistent(
                sort_path.clone().field("pre_agg_exprs"),
                format!(
                    "SORT_NODE node_id={} pre_agg length mismatch: pre_agg_exprs={} pre_agg_output_slot_id={}",
                    node_id,
                    pre_agg_exprs.len(),
                    pre_agg_slots.len()
                ),
            ));
        }

        let mut passthrough_by_slot = std::collections::HashMap::<types::TSlotId, _>::new();
        for (expr_index, (slot_id, agg_expr)) in
            pre_agg_slots.iter().zip(pre_agg_exprs.iter()).enumerate()
        {
            let passthrough = lower_pre_agg_fallback_expr(
                agg_expr,
                arena,
                &child.layout,
                last_query_id,
                fe_addr,
                sort_path.clone().field("pre_agg_exprs").index(expr_index),
            )?;
            if passthrough_by_slot.insert(*slot_id, passthrough).is_some() {
                return Err(StarRocksFragmentDecodeError::inconsistent(
                    sort_path
                        .clone()
                        .field("pre_agg_output_slot_id")
                        .index(expr_index),
                    format!(
                        "SORT_NODE node_id={} duplicate pre_agg_output_slot_id={}",
                        node_id, slot_id
                    ),
                ));
            }
        }

        for (_tuple_id, slot_id) in out_layout.order.iter().skip(exprs.len()) {
            let passthrough = passthrough_by_slot.remove(slot_id).ok_or_else(|| {
                StarRocksFragmentDecodeError::invalid_value(
                    sort_path.clone().field("pre_agg_output_slot_id"),
                    format!(
                        "SORT_NODE node_id={} cannot materialize output slot_id={} from pre_agg metadata",
                        node_id, slot_id
                    ),
                )
            })?;
            exprs.push(passthrough);
        }

        if !passthrough_by_slot.is_empty() {
            return Err(StarRocksFragmentDecodeError::invalid_value(
                sort_path.clone().field("pre_agg_output_slot_id"),
                format!(
                    "SORT_NODE node_id={} has unused pre_agg_output_slot_id values: {:?}",
                    node_id,
                    passthrough_by_slot.keys().collect::<Vec<_>>()
                ),
            ));
        }
    }

    let output_slots = out_layout
        .order
        .iter()
        .map(|(_, slot_id)| SlotId::try_from(*slot_id))
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| StarRocksFragmentDecodeError::invalid_value(sort_path.clone(), error))?;

    let projected = ExecNode {
        kind: ExecNodeKind::Project(ProjectNode {
            input: Box::new(child.node),
            node_id,
            is_subordinate: true,
            exprs,
            expr_slot_ids: output_slots.clone(),
            expr_slot_schemas: None,
            output_indices: None,
            output_chunk_schema: chunk_schema_for_layout(
                desc_tbl.ok_or_else(|| {
                    StarRocksFragmentDecodeError::missing(
                        sort_path.clone(),
                        "SORT_NODE requires desc_tbl for tuple projection chunk schema",
                    )
                })?,
                out_layout,
            )
            .map_err(|error| {
                StarRocksFragmentDecodeError::invalid_value(sort_path.clone(), error)
            })?,
        }),
    };

    Ok(Lowered {
        node: projected,
        layout: out_layout.clone(),
    })
}

fn lower_pre_agg_fallback_expr(
    agg_expr: &exprs::TExpr,
    arena: &mut ExprArena,
    input_layout: &Layout,
    last_query_id: Option<&str>,
    fe_addr: Option<&crate::protocol::starrocks::decode::StarRocksExternalDependencyDraft>,
    expr_path: FieldPath,
) -> Result<novarocks::exec::expr::ExprId, StarRocksFragmentDecodeError> {
    let Some(root) = agg_expr.nodes.first() else {
        return Err(StarRocksFragmentDecodeError::missing(
            expr_path.clone().field("nodes").index(0),
            "SORT_NODE pre_agg_expr has empty nodes",
        ));
    };
    if root.num_children <= 0 {
        let fn_name = root
            .fn_
            .as_ref()
            .map(|f| f.name.function_name.to_ascii_lowercase())
            .unwrap_or_default();
        if fn_name == "count" {
            return Ok(arena.push(ExprNode::Literal(
                novarocks::exec::expr::LiteralValue::Int8(1),
            )));
        }
        return Err(StarRocksFragmentDecodeError::invalid_value(
            expr_path
                .clone()
                .field("nodes")
                .index(0)
                .field("num_children"),
            "SORT_NODE pre_agg_expr root has no children",
        ));
    }

    let first_child_start = 1usize;
    let first_child_end = subtree_end(&agg_expr.nodes, first_child_start)
        .map_err(|error| StarRocksFragmentDecodeError::invalid_value(expr_path.clone(), error))?;
    let child_expr = exprs::TExpr {
        nodes: agg_expr.nodes[first_child_start..first_child_end].to_vec(),
    };
    let child_id = lower_t_expr_at(
        &child_expr,
        arena,
        input_layout,
        last_query_id,
        fe_addr,
        expr_path.clone(),
    )?;

    // Cast the passthrough to the aggregate's declared output type if they differ.
    // For example, sum(INT) has BIGINT output type but the raw passthrough is INT.
    // The exchange receiver uses the slot descriptor type (BIGINT), so we must upcast here.
    //
    if let Some(agg_output_type) = arrow_type_from_desc(&root.type_) {
        let child_type = arena.data_type(child_id).cloned().unwrap_or(DataType::Null);
        if child_type != agg_output_type {
            if matches!(
                agg_output_type,
                DataType::Binary | DataType::LargeBinary | DataType::Utf8 | DataType::LargeUtf8
            ) {
                return Err(StarRocksFragmentDecodeError::unsupported(
                    expr_path.clone().field("nodes").index(0).field("type"),
                    format!(
                        "SORT_NODE pre-agg passthrough declares opaque aggregate state {:?} but child expression is {:?}; source descriptor must be raw/return-typed or runtime must serialize",
                        agg_output_type, child_type
                    ),
                ));
            }
            if !matches!(agg_output_type, DataType::Null) {
                return Ok(arena.push_typed(ExprNode::Cast(child_id), agg_output_type));
            }
        }
    }

    Ok(child_id)
}

fn walk_subtree_end(nodes: &[exprs::TExprNode], idx: &mut usize) -> Result<(), String> {
    let node = nodes
        .get(*idx)
        .ok_or_else(|| format!("invalid expr node index {}", *idx))?;
    *idx += 1;
    for _ in 0..node.num_children {
        walk_subtree_end(nodes, idx)?;
    }
    Ok(())
}

fn subtree_end(nodes: &[exprs::TExprNode], start: usize) -> Result<usize, String> {
    let mut idx = start;
    walk_subtree_end(nodes, &mut idx)?;
    Ok(idx)
}

fn add_slot_aliases(mut layout: Layout, source_layout: &Layout) -> Layout {
    let mut out_slot_to_out_idx = std::collections::HashMap::<types::TSlotId, usize>::new();
    for (idx, (_t, slot_id)) in layout.order.iter().enumerate() {
        out_slot_to_out_idx.entry(*slot_id).or_insert(idx);
    }
    for (tuple_id, slot_id) in &source_layout.order {
        if let Some(out_idx) = out_slot_to_out_idx.get(slot_id).copied() {
            layout.index.insert((*tuple_id, *slot_id), out_idx);
        }
    }
    layout
}

fn normalize_sort_output_layout(
    _child_layout: &Layout,
    out_layout: &Layout,
) -> Result<Layout, String> {
    Ok(out_layout.clone())
}
