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

use crate::sql::analysis::cte::CTERegistry;
use crate::sql::analysis::*;
use crate::sql::column_id::ColumnRefFactory;
use crate::sql::planner::plan::*;

use super::output::plan_output_columns;
use super::query::plan_scoped_query;

// ---------------------------------------------------------------------------
// Apply spec wrapping helpers
// ---------------------------------------------------------------------------

/// Wrap `input` in a left-deep chain of `LogicalPlanKind::Apply` nodes, one per
/// spec whose clause matches `clause`. Each Apply's right child is the planned
/// inner subquery. Matching specs are consumed (removed) from `specs`; the
/// remaining specs are preserved for the other clause insertion points.
pub(super) fn wrap_scalar_applies(
    input: LogicalPlanNode,
    specs: &mut Vec<ApplyScalarSpec>,
    clause: ApplyClause,
    cte_registry: &CTERegistry,
    factory: &mut ColumnRefFactory,
) -> Result<LogicalPlanNode, String> {
    let mut current = input;
    let mut remaining = Vec::new();
    for spec in specs.drain(..) {
        if spec.clause != clause {
            remaining.push(spec);
            continue;
        }
        let right = plan_scoped_query(spec.inner, cte_registry, factory)?;
        // Capture the inner's single scalar output column id before right is
        // moved into the LogicalApplyNode. This id is stable across M1b pushdown rules
        // (which may add group-by keys), so it is the reliable way to find the
        // scalar result in ScalarApplyToJoin (Task 3).
        let inner_output_column_id = plan_output_columns(&right)?
            .first()
            .map(|c| c.column_id)
            .ok_or_else(|| "scalar subquery inner has no output column".to_string())?;
        // Copy output-column fields before spec.output_column is moved into the LogicalApplyNode.
        let col_id = spec.output_column.column_id;
        let col_name = spec.output_column.name.clone();
        let col_type = spec.output_column.data_type.clone();
        current = LogicalPlanNode::new(
            LogicalPlanKind::Apply(LogicalApplyNode {
                kind: ApplyKind::Scalar,
                inner_output_column_id: inner_output_column_id,
                subquery_expr: TypedExpr {
                    kind: ExprKind::ColumnRef {
                        column_id: col_id,
                        qualifier: None,
                        column: col_name,
                    },
                    data_type: col_type,
                    nullable: true,
                },
                output_column: spec.output_column,
                correlation_column_ids: spec.correlation_column_ids,
                correlation_conjuncts: Vec::new(),
                residual_predicate: None,
                need_check_max_rows: spec.need_check_max_rows,
                use_semi_anti: false,
                uncorrelated_outer_predicate_columns: std::collections::HashSet::new(),
            }),
            vec![current, right],
            None,
        );
    }
    *specs = remaining;
    Ok(current)
}

/// Wrap `input` in a left-deep chain of `LogicalPlanKind::Apply` nodes for each
/// EXISTS/IN predicate spec whose clause matches `clause`. Mirrors
/// `wrap_scalar_applies` but builds `ApplyKind::Exists` / `ApplyKind::In`
/// semi/anti-collapsing applies. The M3 to-join rules read correlation and
/// residual predicates directly from the inner Filter, so construction leaves
/// `correlation_conjuncts` empty.
pub(super) fn wrap_predicate_applies(
    input: LogicalPlanNode,
    specs: &mut Vec<ApplyPredicateSpec>,
    clause: ApplyClause,
    cte_registry: &CTERegistry,
    factory: &mut ColumnRefFactory,
) -> Result<LogicalPlanNode, String> {
    use crate::sql::analysis::SubqueryKind;

    let mut current = input;
    let mut remaining = Vec::new();
    for spec in specs.drain(..) {
        if spec.clause != clause {
            remaining.push(spec);
            continue;
        }
        let right = plan_scoped_query(spec.inner, cte_registry, factory)?;
        let inner_output_column_id = plan_output_columns(&right)?
            .first()
            .map(|c| c.column_id)
            .ok_or_else(|| "EXISTS/IN subquery inner has no output column".to_string())?;

        let kind = match spec.kind {
            SubqueryKind::Exists { negated } => ApplyKind::Exists { negated },
            SubqueryKind::InSubquery { negated } => ApplyKind::In { negated },
            SubqueryKind::Scalar => {
                return Err("scalar spec routed to wrap_predicate_applies".to_string());
            }
        };

        let subquery_expr = match (&kind, spec.in_lhs.clone()) {
            (ApplyKind::In { .. }, Some(lhs)) => lhs,
            (ApplyKind::In { .. }, None) => {
                return Err("IN spec missing analyzed LHS".to_string());
            }
            _ => TypedExpr {
                kind: ExprKind::ColumnRef {
                    column_id: spec.output_column.column_id,
                    qualifier: None,
                    column: spec.output_column.name.clone(),
                },
                data_type: spec.output_column.data_type.clone(),
                nullable: spec.output_column.nullable,
            },
        };

        current = LogicalPlanNode::new(
            LogicalPlanKind::Apply(LogicalApplyNode {
                kind: kind,
                subquery_expr: subquery_expr,
                output_column: spec.output_column,
                inner_output_column_id: inner_output_column_id,
                correlation_column_ids: spec.correlation_column_ids,
                correlation_conjuncts: Vec::new(),
                residual_predicate: None,
                need_check_max_rows: false,
                use_semi_anti: spec.use_semi_anti,
                uncorrelated_outer_predicate_columns: std::collections::HashSet::new(),
            }),
            vec![current, right],
            None,
        );
    }
    *specs = remaining;
    Ok(current)
}

// ---------------------------------------------------------------------------
// SELECT planning
// ---------------------------------------------------------------------------
