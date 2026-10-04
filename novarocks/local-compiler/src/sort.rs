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

//! Lower the admitted global-sort representation into the original local sort owner.

use crate::{assert_rows::reserve_vec, lowering::FragmentCompileError};
use novarocks_local_program::{
    ProgramExprId, ProgramNodeId, ProgramNodeKind, SortExpression, SortTopNType, StaticLayout,
};
use novarocks_physical_plan::{
    ExprId, NodeKind, NullOrdering, PhysicalNode, SortDirection, SortMode,
};
use novarocks_type_contract::{CompileCheckpoints, CompilePhase, PureCompileControl};
use std::collections::BTreeMap;

pub(crate) fn lower_sort(
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
    let NodeKind::Sort {
        order_by,
        mode: SortMode::Global,
    } = &node.kind
    else {
        return Err(FragmentCompileError::Unsupported {
            node: Some(node.id),
            feature: "non-global sort mode",
        });
    };
    if node.inputs.len() != 1
        || order_by.is_empty()
        || node.output.columns.len() != layout.slots().len()
    {
        return Err(FragmentCompileError::Invalid(
            "global sort input, keys or output width differs",
        ));
    }
    let mut keys = Vec::new();
    reserve_vec(&mut keys, order_by.len(), work)?;
    for key in order_by {
        let expr = expressions
            .get(&key.expr)
            .copied()
            .ok_or(FragmentCompileError::Invalid(
                "missing global sort expression",
            ));
        work.step()?;
        keys.push(SortExpression {
            expr: expr?,
            asc: matches!(key.direction, SortDirection::Ascending),
            nulls_first: matches!(key.null_ordering, NullOrdering::First),
        });
    }
    // These inactive fields express Global mode, not a guessed buffering cap.
    // The existing full-sort execution path does not consume ranking semantics.
    Ok((
        ProgramNodeKind::Sort {
            input,
            use_top_n: false,
            order_by: keys,
            limit: None,
            offset: 0,
            topn_type: SortTopNType::RowNumber,
            max_buffered_rows: None,
            max_buffered_bytes: None,
            partition_exprs: Vec::new(),
            partition_limit: None,
        },
        layout.clone(),
    ))
}
