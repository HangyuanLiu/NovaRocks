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

use crate::sql::analysis::{BinOp, ExprKind, TypedExpr};
use crate::sql::planner::payload::WindowExpr;

/// Split a TypedExpr on AND into a flat list of conjuncts.
pub(crate) fn split_and_conjuncts_typed(expr: &TypedExpr) -> Vec<&TypedExpr> {
    let mut result = Vec::new();
    collect_and_conjuncts_typed(expr, &mut result);
    result
}

fn collect_and_conjuncts_typed<'a>(expr: &'a TypedExpr, out: &mut Vec<&'a TypedExpr>) {
    match &expr.kind {
        ExprKind::BinaryOp {
            left,
            op: BinOp::And,
            right,
        } => {
            collect_and_conjuncts_typed(left, out);
            collect_and_conjuncts_typed(right, out);
        }
        _ => {
            out.push(expr);
        }
    }
}

/// Group window expressions by their (partition_by, order_by, frame) signature.
pub(crate) fn group_win_exprs_by_sig(exprs: &[WindowExpr]) -> Vec<Vec<usize>> {
    let sig = |e: &WindowExpr| -> String {
        format!(
            "{:?}|{:?}|{:?}",
            e.partition_by
                .iter()
                .map(|p| format!("{:?}", p.kind))
                .collect::<Vec<_>>(),
            e.order_by
                .iter()
                .map(|o| format!("{:?}:{}", o.expr.kind, o.asc))
                .collect::<Vec<_>>(),
            e.window_frame,
        )
    };
    let mut groups: Vec<(String, Vec<usize>)> = Vec::new();
    for (i, e) in exprs.iter().enumerate() {
        let s = sig(e);
        if let Some(g) = groups.iter_mut().find(|(gs, _)| *gs == s) {
            g.1.push(i);
        } else {
            groups.push((s, vec![i]));
        }
    }
    groups.into_iter().map(|(_, indices)| indices).collect()
}
