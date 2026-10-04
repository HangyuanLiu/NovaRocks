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

//! Ordinary Single TopN uses the sole ordered-key projection and local sort owner.

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
        phase: TopNPhase::Single,
        reduction: TopNReduction::Rows,
    } = &node.kind
    else {
        return Err(FragmentCompileError::Unsupported {
            node: Some(node.id),
            feature: "non-Single or grouped TopN",
        });
    };
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
    // These are the original ordinary row-count TopN fields. No buffering cap,
    // partition rank semantics or task parallelism is authored here.
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
