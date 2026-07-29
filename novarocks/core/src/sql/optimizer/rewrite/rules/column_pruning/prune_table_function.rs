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

//! PruneTableFunctionColumns — Phase 2 rule for TableFunction nodes.
//!
//! This is a documented NO-OP. The TableFunction node was assigned keep-all-child
//! semantics by the Phase-1 tagging pass: all output columns (both input pass-through
//! columns and the lateral table function result columns) are treated as required.
//! Pruning lateral table function outputs would require re-evaluating which
//! function arguments and output slots are actually used, which is deferred.
//!
//! Kept for architectural symmetry and to allow per-operator
//! `disable_optimizer_rules` control in the future.

use crate::sql::optimizer::opt_expr::OptExpr;
use crate::sql::optimizer::pattern::{OpKind, Pattern};
use crate::sql::optimizer::rewrite::context::RewriteContext;
use crate::sql::optimizer::rewrite::phase::RewritePhase;
use crate::sql::optimizer::rewrite::result::RewriteResult;
use crate::sql::optimizer::rewrite::rule::LogicalRewriteRule;

pub(crate) struct PruneTableFunctionColumns;

impl LogicalRewriteRule for PruneTableFunctionColumns {
    fn name(&self) -> &'static str {
        "PruneTableFunctionColumns"
    }

    fn phase(&self) -> RewritePhase {
        RewritePhase::StructuralRewrite
    }

    fn pattern(&self) -> Pattern {
        Pattern::Op {
            kind: OpKind::TableFunction,
            children: vec![Pattern::MultiLeaf],
        }
    }

    fn matches(&self, _expr: &OptExpr, _ctx: &RewriteContext) -> bool {
        true
    }

    fn apply(&self, _expr: OptExpr, _ctx: &mut RewriteContext) -> Result<RewriteResult, String> {
        // No-op: TableFunction was assigned keep-all-child semantics by the
        // Phase-1 tagging pass. Kept for architectural symmetry + per-operator
        // disable_optimizer_rules control.
        Ok(RewriteResult::Unchanged)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::analysis::OutputColumn;
    use crate::sql::column_id::ColumnId;
    use crate::sql::optimizer::operator::{Operator, TableFunctionOp, ValuesOp};
    use crate::sql::optimizer::opt_expr::OptExpr;
    use crate::sql::optimizer::rewrite::context::{RewriteConsumer, RewriteContext};
    use arrow::datatypes::DataType;

    fn ctx() -> RewriteContext {
        RewriteContext::new(
            RewriteConsumer::Query,
            crate::sql::optimizer::options::SessionOptimizerSettings::default(),
        )
    }

    #[test]
    fn prune_table_function_is_always_unchanged() {
        let expr = OptExpr::new(
            Operator::LogicalTableFunction(TableFunctionOp {
                function_name: "generate_series".to_string(),
                args: vec![],
                output_columns: vec![OutputColumn {
                    column_id: ColumnId::new_for_test(1),
                    name: "v".to_string(),
                    data_type: DataType::Int64,
                    nullable: false,
                    is_internal: false,
                }],
                alias: None,
                is_left_join: false,
            }),
            vec![OptExpr::leaf(Operator::LogicalValues(ValuesOp {
                rows: vec![],
                columns: vec![],
            }))],
        );

        let rule = PruneTableFunctionColumns;

        // pattern gates the structural operator kind.
        assert!(
            crate::sql::optimizer::rewrite::tree_binder::bind_tree(&rule.pattern(), &expr)
                .is_some()
        );

        // apply always returns Unchanged
        let result = rule.apply(expr, &mut ctx()).unwrap();
        assert!(
            matches!(result, RewriteResult::Unchanged),
            "PruneTableFunctionColumns must always return Unchanged"
        );
    }
}
