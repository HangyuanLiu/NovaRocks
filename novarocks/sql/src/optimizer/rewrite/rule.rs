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

use crate::compiler::SqlCompileError;
use crate::optimizer::opt_expr::OptExpr;
use crate::optimizer::rewrite::context::RewriteContext;
use crate::optimizer::rewrite::phase::RewritePhase;
use crate::optimizer::rewrite::result::RewriteResult;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum RewriteTraversal {
    TopDown,
    BottomUp,
}

pub(crate) trait LogicalRewriteRule: Send + Sync {
    fn name(&self) -> &'static str;
    fn phase(&self) -> RewritePhase;
    fn traversal(&self) -> RewriteTraversal {
        RewriteTraversal::BottomUp
    }
    /// Declarative structural match shape, consumed by the tree driver's bind_tree
    /// pre-gate. Default `Leaf` = root wildcard -> bind_tree always matches -> the
    /// rule's own `matches`/`apply` decide exactly as before.
    fn pattern(&self) -> crate::optimizer::pattern::Pattern {
        crate::optimizer::pattern::Pattern::Leaf
    }
    /// Symmetry with the memo Rule trait; degenerate on the tree (<=1 binding/node).
    #[allow(dead_code)]
    fn first_match_only(&self) -> bool {
        false
    }
    fn matches(&self, expr: &OptExpr, ctx: &RewriteContext) -> bool;
    fn apply(
        &self,
        expr: OptExpr,
        ctx: &mut RewriteContext,
    ) -> Result<RewriteResult, SqlCompileError>;
}
