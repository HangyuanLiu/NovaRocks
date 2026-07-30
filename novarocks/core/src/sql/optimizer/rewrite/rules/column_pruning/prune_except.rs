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

//! PruneExceptColumns — Phase 2 rule for Except nodes.
//!
//! EXCEPT is a DISTINCT set operation: every output position participates in
//! row equality. Column pruning cannot remove set-key positions even when the
//! parent only needs row existence (for example `COUNT(*)` over the set).

use crate::sql::optimizer::operator::Operator;
use crate::sql::optimizer::opt_expr::OptExpr;
use crate::sql::optimizer::pattern::{OpKind, Pattern};
use crate::sql::optimizer::rewrite::context::RewriteContext;
use crate::sql::optimizer::rewrite::phase::RewritePhase;
use crate::sql::optimizer::rewrite::result::RewriteResult;
use crate::sql::optimizer::rewrite::rule::LogicalRewriteRule;

pub(crate) struct PruneExceptColumns;

impl LogicalRewriteRule for PruneExceptColumns {
    fn name(&self) -> &'static str {
        "PruneExceptColumns"
    }

    fn phase(&self) -> RewritePhase {
        RewritePhase::StructuralRewrite
    }

    fn pattern(&self) -> Pattern {
        Pattern::Op {
            kind: OpKind::Except,
            children: vec![Pattern::MultiLeaf],
        }
    }

    fn matches(&self, _expr: &OptExpr, _ctx: &RewriteContext) -> bool {
        true
    }

    fn apply(&self, expr: OptExpr, _ctx: &mut RewriteContext) -> Result<RewriteResult, String> {
        let Operator::LogicalExcept(_) = expr.op else {
            unreachable!()
        };

        Ok(RewriteResult::Unchanged)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::analysis::OutputColumn;
    use crate::sql::column_id::ColumnId;
    use crate::sql::optimizer::operator::{ExceptOp, Operator, ValuesOp};
    use crate::sql::optimizer::opt_expr::OptExpr;
    use crate::sql::optimizer::rewrite::context::{RewriteConsumer, RewriteContext};
    use arrow::datatypes::DataType;
    use std::collections::HashSet;

    fn ctx() -> RewriteContext {
        RewriteContext::new(
            RewriteConsumer::Query,
            crate::sql::optimizer::options::SessionOptimizerSettings::default(),
        )
    }

    fn make_output_column(id: ColumnId, name: &str) -> OutputColumn {
        OutputColumn {
            column_id: id,
            name: name.to_string(),
            data_type: DataType::Int32,
            nullable: false,
            is_internal: false,
        }
    }

    fn dummy_input() -> OptExpr {
        OptExpr::leaf(Operator::LogicalValues(ValuesOp {
            rows: vec![],
            columns: vec![],
        }))
    }

    #[test]
    fn prune_except_preserves_all_set_key_columns() {
        let id_a = ColumnId::new_for_test(1);
        let id_b = ColumnId::new_for_test(2);
        let id_c = ColumnId::new_for_test(3);

        let mut needed = HashSet::new();
        needed.insert(id_b);

        let mut expr = OptExpr::new(
            Operator::LogicalExcept(ExceptOp {
                output_columns: vec![
                    make_output_column(id_a, "a"),
                    make_output_column(id_b, "b"),
                    make_output_column(id_c, "c"),
                ],
                child_output_columns: vec![],
            }),
            vec![dummy_input(), dummy_input()],
        );
        expr.required_output_columns = Some(needed);

        let rule = PruneExceptColumns;
        let result = rule.apply(expr, &mut ctx()).unwrap();

        assert!(
            matches!(result, RewriteResult::Unchanged),
            "EXCEPT must retain every output column because all positions are part of the set key"
        );
    }

    #[test]
    fn prune_except_noop_when_required_output_columns_is_none() {
        let id_a = ColumnId::new_for_test(1);

        let expr = OptExpr::new(
            Operator::LogicalExcept(ExceptOp {
                output_columns: vec![make_output_column(id_a, "a")],
                child_output_columns: vec![],
            }),
            vec![dummy_input()],
        );
        // required_output_columns = None (default)

        let rule = PruneExceptColumns;
        let result = rule.apply(expr, &mut ctx()).unwrap();

        assert!(
            matches!(result, RewriteResult::Unchanged),
            "must be no-op when required_output_columns is None"
        );
    }

    #[test]
    fn prune_except_keeps_all_columns_when_needed_empty() {
        let id_a = ColumnId::new_for_test(1);
        let id_b = ColumnId::new_for_test(2);

        let mut expr = OptExpr::new(
            Operator::LogicalExcept(ExceptOp {
                output_columns: vec![make_output_column(id_a, "a"), make_output_column(id_b, "b")],
                child_output_columns: vec![],
            }),
            vec![dummy_input()],
        );
        expr.required_output_columns = Some(HashSet::new());

        let rule = PruneExceptColumns;
        let result = rule.apply(expr, &mut ctx()).unwrap();

        assert!(
            matches!(result, RewriteResult::Unchanged),
            "EXCEPT cannot collapse the set key even when the parent only needs row existence"
        );
    }
}
