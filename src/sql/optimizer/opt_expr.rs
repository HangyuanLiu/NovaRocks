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

//! `OptExpr` — the optimizer's concrete logical operator tree.
//!
//! Mirrors StarRocks `OptExpression`: an `Operator` payload plus child
//! `OptExpr`s. Scalars inside the operator are already interned `ScalarId`
//! handles into the owning `ScalarArena`. This is the tree the RBO rewrite
//! phase operates on; `memo_copy::opt_expr_to_memo` copies it into the Memo for
//! CBO.

use std::collections::HashSet;

use super::operator::Operator;
use crate::sql::column_id::ColumnId;

#[derive(Clone, Debug)]
pub(crate) struct OptExpr {
    pub op: Operator,
    pub children: Vec<OptExpr>,
    /// Mirrors `LogicalPlanNode.required_output_columns` — the column-pruning
    /// annotation that rewrite rules read and propagate. `None` means all
    /// columns are required. Carried through Bridge 1; ignored by copy-in
    /// (the Memo does not use it).
    pub required_output_columns: Option<HashSet<ColumnId>>,
}

impl OptExpr {
    pub(crate) fn new(op: Operator, children: Vec<OptExpr>) -> Self {
        Self {
            op,
            children,
            required_output_columns: None,
        }
    }

    pub(crate) fn leaf(op: Operator) -> Self {
        Self {
            op,
            children: Vec::new(),
            required_output_columns: None,
        }
    }

    pub(crate) fn child(&self, index: usize) -> &OptExpr {
        &self.children[index]
    }

    pub(crate) fn unary_input(&self) -> &OptExpr {
        &self.children[0]
    }

    pub(crate) fn left(&self) -> &OptExpr {
        &self.children[0]
    }

    pub(crate) fn right(&self) -> &OptExpr {
        &self.children[1]
    }
}
