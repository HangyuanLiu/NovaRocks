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
use crate::protocol::starrocks::decode::error::StarRocksFragmentDecodeError;
use crate::protocol::starrocks::decode::expr::lower_t_expr_at;
use crate::protocol::starrocks::decode::layout::{
    Layout, chunk_schema_for_layout, layout_from_slot_ids,
};
use crate::protocol::starrocks::decode::node::Lowered;
use crate::thrift::descriptors;
use crate::thrift::exprs;
use crate::thrift::{plan_nodes, types};
use novarocks::common::ids::SlotId;
use novarocks::exec::expr::ExprArena;
use novarocks::exec::node::project::ProjectNode;
use novarocks::exec::node::set_op::{SetOpKind, SetOpNode};
use novarocks::exec::node::{ExecNode, ExecNodeKind};
use novarocks::protocol::FieldPath;

pub(crate) fn lower_intersect_node(
    children: Vec<Lowered>,
    node: &plan_nodes::TPlanNode,
    out_layout: Layout,
    arena: &mut ExprArena,
    desc_tbl: Option<&descriptors::TDescriptorTable>,
    last_query_id: Option<&str>,
    fe_addr: Option<&crate::protocol::starrocks::decode::StarRocksExternalDependencyDraft>,
    node_path: FieldPath,
) -> Result<Lowered, StarRocksFragmentDecodeError> {
    let payload_path = node_path.field("intersect_node");
    let Some(intersect) = node.intersect_node.as_ref() else {
        return Err(StarRocksFragmentDecodeError::missing(
            payload_path,
            "INTERSECT_NODE missing intersect_node payload",
        ));
    };
    lower_distinct_set_node(
        children,
        node,
        out_layout,
        arena,
        desc_tbl,
        last_query_id,
        fe_addr,
        intersect.tuple_id,
        &intersect.result_expr_lists,
        "INTERSECT_NODE",
        SetOpKind::Intersect,
        payload_path,
    )
}

pub(crate) fn lower_except_node(
    children: Vec<Lowered>,
    node: &plan_nodes::TPlanNode,
    out_layout: Layout,
    arena: &mut ExprArena,
    desc_tbl: Option<&descriptors::TDescriptorTable>,
    last_query_id: Option<&str>,
    fe_addr: Option<&crate::protocol::starrocks::decode::StarRocksExternalDependencyDraft>,
    node_path: FieldPath,
) -> Result<Lowered, StarRocksFragmentDecodeError> {
    let payload_path = node_path.field("except_node");
    let Some(except) = node.except_node.as_ref() else {
        return Err(StarRocksFragmentDecodeError::missing(
            payload_path,
            "EXCEPT_NODE missing except_node payload",
        ));
    };
    lower_distinct_set_node(
        children,
        node,
        out_layout,
        arena,
        desc_tbl,
        last_query_id,
        fe_addr,
        except.tuple_id,
        &except.result_expr_lists,
        "EXCEPT_NODE",
        SetOpKind::Except,
        payload_path,
    )
}

fn lower_distinct_set_node(
    children: Vec<Lowered>,
    node: &plan_nodes::TPlanNode,
    mut out_layout: Layout,
    arena: &mut ExprArena,
    desc_tbl: Option<&descriptors::TDescriptorTable>,
    last_query_id: Option<&str>,
    fe_addr: Option<&crate::protocol::starrocks::decode::StarRocksExternalDependencyDraft>,
    tuple_id: types::TTupleId,
    result_expr_lists: &[Vec<exprs::TExpr>],
    op_name: &'static str,
    set_op_kind: SetOpKind,
    payload_path: FieldPath,
) -> Result<Lowered, StarRocksFragmentDecodeError> {
    if children.len() < 2 {
        return Err(StarRocksFragmentDecodeError::inconsistent(
            payload_path.clone().field("result_expr_lists"),
            format!("{op_name} expected >=2 children, got {}", children.len()),
        ));
    }
    if out_layout.order.is_empty() {
        let col_count = result_expr_lists.first().map(|r| r.len()).unwrap_or(0);
        if col_count == 0 {
            return Err(StarRocksFragmentDecodeError::missing(
                payload_path.clone().field("result_expr_lists"),
                format!("{op_name} cannot infer output columns"),
            ));
        }
        out_layout = layout_from_slot_ids(tuple_id, (0..col_count).map(|i| i as types::TSlotId));
    }

    if result_expr_lists.len() != children.len() {
        return Err(StarRocksFragmentDecodeError::inconsistent(
            payload_path.clone().field("result_expr_lists"),
            format!(
                "{op_name} result_expr_lists size mismatch: expr_lists={} children={}",
                result_expr_lists.len(),
                children.len()
            ),
        ));
    }

    let output_slots = out_layout
        .order
        .iter()
        .map(|(_, slot_id)| SlotId::try_from(*slot_id))
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| {
            StarRocksFragmentDecodeError::invalid_value(
                payload_path.clone().field("tuple_id"),
                error,
            )
        })?;
    let desc_tbl = desc_tbl.ok_or_else(|| {
        StarRocksFragmentDecodeError::missing(
            payload_path.clone(),
            format!("{op_name} lowering requires descriptor table"),
        )
    })?;
    let output_chunk_schema = chunk_schema_for_layout(desc_tbl, &out_layout).map_err(|error| {
        StarRocksFragmentDecodeError::invalid_value(payload_path.clone(), error)
    })?;

    let mut inputs = Vec::with_capacity(children.len());
    for (child_index, (child, expr_list)) in children
        .into_iter()
        .zip(result_expr_lists.iter())
        .enumerate()
    {
        let mut exprs = Vec::with_capacity(expr_list.len());
        for (expr_index, e) in expr_list.iter().enumerate() {
            exprs.push(lower_t_expr_at(
                e,
                arena,
                &child.layout,
                last_query_id,
                fe_addr,
                payload_path
                    .clone()
                    .field("result_expr_lists")
                    .index(child_index)
                    .index(expr_index),
            )?);
        }
        inputs.push(ExecNode {
            kind: ExecNodeKind::Project(ProjectNode {
                input: Box::new(child.node),
                node_id: node.node_id,
                is_subordinate: true,
                exprs,
                expr_slot_ids: output_slots.clone(),
                expr_slot_schemas: None,
                output_indices: None,
                output_chunk_schema: output_chunk_schema.clone(),
            }),
        });
    }

    Ok(Lowered {
        node: ExecNode {
            kind: ExecNodeKind::SetOp(SetOpNode {
                kind: set_op_kind,
                inputs,
                node_id: node.node_id,
                output_chunk_schema,
            }),
        },
        layout: out_layout,
    })
}
