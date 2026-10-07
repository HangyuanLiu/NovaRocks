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

//! Native typed quota-owner projection.
use super::quota_preclaim::{column, limits, need, output};
use crate::fragment_error::NativeFragmentDecodeError;
use crate::fragment_plan_node::NativeLoweredPlanNode;
use novarocks_execution::exec::node::quota_trim::{QuotaTrimNode, QuotaTrimSpec};
use novarocks_execution::exec::node::{ExecNode, ExecNodeKind};
use novarocks_proto_codec::FieldPath;
use novarocks_proto_models::plan;

pub(super) fn lower(
    node: &plan::DistributedNode,
    physical: &plan::PlanNode,
    wire: &plan::QuotaTrimNode,
    path: FieldPath,
    output_path: FieldPath,
    mut children: Vec<NativeLoweredPlanNode>,
) -> Result<NativeLoweredPlanNode, NativeFragmentDecodeError> {
    if children.len() != 2 {
        return Err(NativeFragmentDecodeError::inconsistent(
            path,
            "QuotaTrim requires seed and candidate inputs",
        ));
    }
    let candidates = children.pop().expect("two validated inputs");
    let seeds = children.pop().expect("two validated inputs");
    let max_state_bytes = limits(
        wire.content_equivalence,
        wire.preselection_domain_node_id,
        wire.max_state_bytes,
        path.clone(),
    )?;
    let spec = QuotaTrimSpec {
        seed_entry_id_column: column(
            &seeds.output_schema,
            wire.seed_entry_id_column_id,
            path.clone().field("seed_entry_id_column_id"),
        )?,
        seed_need: need(
            wire.seed_need.as_ref(),
            &seeds.output_schema,
            path.clone().field("seed_need"),
        )?,
        candidate_entry_id_column: column(
            &candidates.output_schema,
            wire.candidate_entry_id_column_id,
            path.clone().field("candidate_entry_id_column_id"),
        )?,
        candidate_file_column: column(
            &candidates.output_schema,
            wire.candidate_file_column_id,
            path.clone().field("candidate_file_column_id"),
        )?,
        candidate_position_column: column(
            &candidates.output_schema,
            wire.candidate_position_column_id,
            path.clone().field("candidate_position_column_id"),
        )?,
        preselection_domain: novarocks_local_program::QuotaDomainId::try_new(
            wire.preselection_domain_node_id,
        )
        .map_err(|_| {
            NativeFragmentDecodeError::invalid_value(
                path.clone().field("preselection_domain_node_id"),
                "quota requires a nonnegative exact domain reference",
            )
        })?,
        max_state_bytes,
    };
    let (layout, output_schema) = output(physical, output_path)?;
    if !spec.validate(
        &seeds.output_schema.arrow_schema_ref(),
        &candidates.output_schema.arrow_schema_ref(),
        &output_schema.arrow_schema_ref(),
    ) || output_schema
        .slots()
        .iter()
        .any(|slot| slot.field().is_nullable())
    {
        return Err(NativeFragmentDecodeError::inconsistent(
            path,
            "QuotaTrim has an invalid exact input/output schema",
        ));
    }
    Ok(NativeLoweredPlanNode {
        node: ExecNode {
            kind: ExecNodeKind::QuotaTrim(QuotaTrimNode {
                node_id: node.node_id,
                seeds: Box::new(seeds.node),
                candidates: Box::new(candidates.node),
                spec,
                output_chunk_schema: output_schema.clone(),
            }),
        },
        layout,
        output_schema,
    })
}
