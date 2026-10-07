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

//! Ordinary row-count TopN uses the sole ordered-key projection and local sort owner.
//!
//! Every phase of a `Rows` reduction lowers to the same local operation: the
//! rows `[offset, offset + limit)` of this instance's input under the frozen
//! ordering. The phase is a cross-instance fact the checked physical contract
//! already discharges, so no phase or sequence identity reaches the program:
//!
//! - `Single` and `Final` read one Singleton, single-copy input, so their
//!   instance input is the whole relation and the window is the answer.
//! - `Partial` keeps its input distribution and carries offset 0 with the
//!   limit `final.limit + final.offset` its sequence trace proves. Each
//!   instance keeps its own top `limit` rows. A row with fewer than `limit`
//!   rows strictly ahead of it in the relation has fewer in its own instance,
//!   so that prefix keeps it or an equal-keyed row in its place. The union of
//!   the instance prefixes therefore holds a complete window under the frozen
//!   ordering, ties resolved per instance, and the Final over the gathered
//!   union selects it.
//!
//! `GroupedStates` merges partial states by the full group key (spec §5.8)
//! and is refused here; it has no local owner yet.

use crate::{lowering::FragmentCompileError, sort::lower_sort_keys};
use novarocks_local_program::{
    ProgramExprId, ProgramNodeId, ProgramNodeKind, SortTopNType, StaticLayout,
};
use novarocks_physical_plan::{ExprId, NodeKind, PhysicalNode, TopNPhase, TopNReduction};
use novarocks_type_contract::{CompileCheckpoints, CompilePhase, PureCompileControl};
use std::collections::BTreeMap;

pub(crate) fn lower_topn(
    node: &PhysicalNode,
    input: ProgramNodeId,
    layout: &StaticLayout,
    expressions: &BTreeMap<ExprId, ProgramExprId>,
    control: &dyn PureCompileControl,
) -> Result<(ProgramNodeKind, StaticLayout), FragmentCompileError> {
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::LowerProgram)?;
    let result = lower_core(node, input, layout, expressions, &mut work);
    if matches!(&result, Err(FragmentCompileError::Control(_))) {
        return result;
    }
    work.finish()?;
    result
}
fn lower_core(
    node: &PhysicalNode,
    input: ProgramNodeId,
    layout: &StaticLayout,
    expressions: &BTreeMap<ExprId, ProgramExprId>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(ProgramNodeKind, StaticLayout), FragmentCompileError> {
    let NodeKind::TopN {
        order_by,
        limit,
        offset,
        phase,
        reduction: TopNReduction::Rows,
    } = &node.kind
    else {
        return Err(FragmentCompileError::Unsupported {
            node: Some(node.id),
            feature: "grouped-state TopN",
        });
    };
    // A partial prefix is cut before global completion, so an offset there
    // would drop rows the final window may need.
    let partial_offset = matches!(phase, TopNPhase::Partial { .. }) && *offset != 0;
    let shape = node.inputs.len() == 1
        && !order_by.is_empty()
        && node.output.columns.len() == layout.slots().len();
    let limit = usize::try_from(*limit);
    let offset = usize::try_from(*offset);
    work.step()?;
    if !shape {
        return Err(FragmentCompileError::Invalid(
            "ordinary TopN input, keys or output width differs",
        ));
    }
    if partial_offset {
        return Err(FragmentCompileError::Invalid(
            "partial TopN carries an offset before global completion",
        ));
    }
    let limit =
        limit.map_err(|_| FragmentCompileError::Invalid("TopN limit exceeds host range"))?;
    let offset =
        offset.map_err(|_| FragmentCompileError::Invalid("TopN offset exceeds host range"))?;
    let extent = limit.checked_add(offset);
    work.step()?;
    extent.ok_or(FragmentCompileError::Invalid(
        "TopN limit and offset exceed host range",
    ))?;
    let keys = lower_sort_keys(order_by, expressions, work)?;
    // These are the original ordinary row-count TopN fields for every phase.
    // No buffering cap, partition rank semantics or task parallelism is
    // authored here; the phase stays a checked physical fact.
    Ok((
        ProgramNodeKind::Sort {
            input,
            use_top_n: true,
            order_by: keys,
            limit: Some(limit),
            offset,
            topn_type: SortTopNType::RowNumber,
            max_buffered_rows: None,
            max_buffered_bytes: None,
            partition_exprs: Vec::new(),
            partition_limit: None,
        },
        layout.clone(),
    ))
}
