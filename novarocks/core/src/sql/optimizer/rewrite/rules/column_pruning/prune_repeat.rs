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

//! PruneRepeatColumns — Phase 2 rule for Repeat (ROLLUP/CUBE/GROUPING SETS) nodes.
//!
//! This is a documented NO-OP. The Repeat node has keep-all-child semantics
//! assigned by the Phase-1 tagging pass: all input columns are needed because
//! the ROLLUP/CUBE/GROUPING SETS grouping logic references arbitrary subsets
//! of columns at runtime. No `output_columns` list is maintained on
//! `RepeatOp` — pruning is not applicable here.
//!
//! Kept for architectural symmetry and to allow per-operator
//! `disable_optimizer_rules` control in the future.

use crate::sql::optimizer::opt_expr::OptExpr;
use crate::sql::optimizer::pattern::{OpKind, Pattern};
use crate::sql::optimizer::rewrite::context::RewriteContext;
use crate::sql::optimizer::rewrite::phase::RewritePhase;
use crate::sql::optimizer::rewrite::result::RewriteResult;
use crate::sql::optimizer::rewrite::rule::LogicalRewriteRule;

pub(crate) struct PruneRepeatColumns;

impl LogicalRewriteRule for PruneRepeatColumns {
    fn name(&self) -> &'static str {
        "PruneRepeatColumns"
    }

    fn phase(&self) -> RewritePhase {
        RewritePhase::StructuralRewrite
    }

    fn pattern(&self) -> Pattern {
        Pattern::Op {
            kind: OpKind::Repeat,
            children: vec![Pattern::MultiLeaf],
        }
    }

    fn matches(&self, _expr: &OptExpr, _ctx: &RewriteContext) -> bool {
        true
    }

    fn apply(&self, _expr: OptExpr, _ctx: &mut RewriteContext) -> Result<RewriteResult, String> {
        // No-op: Repeat (ROLLUP/CUBE/GROUPING SETS) was assigned keep-all-child
        // semantics by the Phase-1 tagging pass. No output_columns list to prune.
        // Kept for architectural symmetry + per-operator disable_optimizer_rules
        // control.
        Ok(RewriteResult::Unchanged)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::optimizer::operator::{Operator, RepeatOp, ValuesOp};
    use crate::sql::optimizer::opt_expr::OptExpr;
    use crate::sql::optimizer::rewrite::context::{RewriteConsumer, RewriteContext};

    fn ctx() -> RewriteContext {
        RewriteContext::new(
            RewriteConsumer::Query,
            crate::sql::optimizer::options::SessionOptimizerSettings::default(),
        )
    }

    #[test]
    fn prune_repeat_is_always_unchanged() {
        let expr = OptExpr::new(
            Operator::LogicalRepeat(RepeatOp {
                repeat_column_ref_list: vec![],
                repeat_column_ref_ids: vec![],
                grouping_ids: vec![],
                all_rollup_columns: vec![],
                all_rollup_column_ids: vec![],
                grouping_key_aliases: vec![],
                grouping_fn_args: vec![],
                grouping_fn_arg_ids: vec![],
                grouping_fn_ids: vec![],
            }),
            vec![OptExpr::leaf(Operator::LogicalValues(ValuesOp {
                rows: vec![],
                columns: vec![],
            }))],
        );

        let rule = PruneRepeatColumns;

        // pattern gates the structural operator kind.
        assert!(
            crate::sql::optimizer::rewrite::tree_binder::bind_tree(&rule.pattern(), &expr)
                .is_some()
        );

        // apply always returns Unchanged
        let result = rule.apply(expr, &mut ctx()).unwrap();
        assert!(
            matches!(result, RewriteResult::Unchanged),
            "PruneRepeatColumns must always return Unchanged"
        );
    }
}
