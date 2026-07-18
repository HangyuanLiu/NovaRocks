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

use super::super::NativeFragmentDecodeError;
use super::super::expr::decode_expr_at;
use super::super::layout::{chunk_schema_from_output_columns, layout_from_output_columns};
use super::DecodedNode;
use super::common::check_exact_arity;
use crate::exec::expr::ExprArena;
use crate::proto::plan;
use crate::protocol::common::error::FieldPath;

pub(super) fn lower_redistribute_node(
    physical: &plan::PlanNode,
    redistribute: &plan::RedistributeNode,
    path: FieldPath,
    mut children: Vec<DecodedNode>,
    arena: &mut ExprArena,
) -> Result<DecodedNode, NativeFragmentDecodeError> {
    NativeFragmentDecodeError::map_invalid(
        path.clone(),
        check_exact_arity("RedistributeNode", 1, children.len()),
    )?;
    let child = children.pop().expect("child");
    let mode = redistribute
        .mode
        .as_ref()
        .and_then(|mode| mode.mode.as_ref())
        .ok_or_else(|| {
            NativeFragmentDecodeError::missing(
                path.clone().field("mode.mode"),
                "RedistributeNode mode missing",
            )
        })?;
    match mode {
        plan::redistribute_mode::Mode::Gather(true)
        | plan::redistribute_mode::Mode::Broadcast(true) => {}
        plan::redistribute_mode::Mode::Hash(hash) => {
            if hash.cols.is_empty() {
                return Err(NativeFragmentDecodeError::missing(
                    path.clone().field("mode.hash.cols"),
                    "RedistributeNode hash mode requires cols",
                ));
            }
            for col in &hash.cols {
                NativeFragmentDecodeError::map_invalid(
                    path.clone().field("mode.hash.cols"),
                    child.layout.resolve_column_id(*col),
                )?;
            }
        }
        plan::redistribute_mode::Mode::Gather(false)
        | plan::redistribute_mode::Mode::Broadcast(false) => {
            return Err(NativeFragmentDecodeError::invalid_value(
                path.clone().field("mode"),
                "RedistributeNode boolean mode must be true",
            ));
        }
    }
    for (idx, expr) in redistribute.partition_exprs.iter().enumerate() {
        decode_expr_at(
            expr,
            path.clone().field("partition_exprs").index(idx),
            arena,
            &child.layout,
        )?;
    }
    let output_columns = if redistribute.output_columns.is_empty() {
        &physical.output_columns
    } else {
        &redistribute.output_columns
    };
    if output_columns.is_empty() {
        return Ok(child);
    }
    let output_path = path.clone().field("output_columns");
    let layout = NativeFragmentDecodeError::map_invalid(
        output_path.clone(),
        layout_from_output_columns(output_columns),
    )?;
    if layout.order() != child.layout.order() {
        return Err(NativeFragmentDecodeError::inconsistent(
            output_path.clone(),
            format!(
                "RedistributeNode output columns must preserve child order: child={:?} output={:?}",
                child.layout.order(),
                layout.order()
            ),
        ));
    }
    let output_schema = NativeFragmentDecodeError::map_invalid(
        output_path,
        chunk_schema_from_output_columns(output_columns),
    )?;
    Ok(DecodedNode {
        node: child.node,
        layout,
        output_schema,
    })
}
