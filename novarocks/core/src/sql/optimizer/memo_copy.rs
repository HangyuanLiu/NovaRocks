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

use crate::sql::optimizer::memo::{GroupId, MExpr, Memo};
use crate::sql::optimizer::operator::Operator;
use crate::sql::optimizer::opt_expr::OptExpr;

pub(crate) fn opt_expr_to_memo(expr: &OptExpr, memo: &mut Memo) -> GroupId {
    let children: Vec<GroupId> = expr
        .children
        .iter()
        .map(|child| opt_expr_to_memo(child, memo))
        .collect();
    let mexpr = MExpr {
        id: memo.next_expr_id(),
        op: expr.op.clone(),
        children,
    };
    let group_id = memo.new_group(mexpr);
    if let Operator::LogicalCTEProduce(op) = &expr.op {
        memo.cte_produce_groups.insert(op.cte_id, group_id);
    }
    group_id
}
