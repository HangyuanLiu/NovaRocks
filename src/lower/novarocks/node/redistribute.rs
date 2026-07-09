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

use super::super::expr::lower_proto_expr;
use super::super::layout::{chunk_schema_from_output_columns, layout_from_output_columns};
use super::LoweredNode;
use super::common::check_exact_arity;
use crate::exec::expr::ExprArena;
use crate::proto::plan;

pub(crate) fn lower_redistribute_node(
    physical: &plan::PlanNode,
    redistribute: &plan::RedistributeNode,
    mut children: Vec<LoweredNode>,
    arena: &mut ExprArena,
) -> Result<LoweredNode, String> {
    check_exact_arity("RedistributeNode", 1, children.len())?;
    let child = children.pop().expect("child");
    let mode = redistribute
        .mode
        .as_ref()
        .and_then(|mode| mode.mode.as_ref())
        .ok_or_else(|| "RedistributeNode mode missing".to_string())?;
    match mode {
        plan::redistribute_mode::Mode::Gather(true)
        | plan::redistribute_mode::Mode::Broadcast(true) => {}
        plan::redistribute_mode::Mode::Hash(hash) => {
            if hash.cols.is_empty() {
                return Err("RedistributeNode hash mode requires cols".to_string());
            }
            for col in &hash.cols {
                child.layout.resolve_column_id(*col)?;
            }
        }
        plan::redistribute_mode::Mode::Gather(false)
        | plan::redistribute_mode::Mode::Broadcast(false) => {
            return Err("RedistributeNode boolean mode must be true".to_string());
        }
    }
    for (idx, expr) in redistribute.partition_exprs.iter().enumerate() {
        lower_proto_expr(expr, arena, &child.layout)
            .map_err(|err| format!("RedistributeNode partition_exprs[{idx}]: {err}"))?;
    }
    let output_columns = if redistribute.output_columns.is_empty() {
        &physical.output_columns
    } else {
        &redistribute.output_columns
    };
    if output_columns.is_empty() {
        return Ok(child);
    }
    let layout = layout_from_output_columns(output_columns)?;
    if layout.order() != child.layout.order() {
        return Err(format!(
            "RedistributeNode output columns must preserve child order: child={:?} output={:?}",
            child.layout.order(),
            layout.order()
        ));
    }
    let output_schema = chunk_schema_from_output_columns(output_columns)?;
    Ok(LoweredNode {
        node: child.node,
        layout,
        output_schema,
    })
}
