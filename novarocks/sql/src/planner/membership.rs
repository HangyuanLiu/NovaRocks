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

//! SQL-owned slot membership facts. No expressions or live build owners.
use crate::analysis::OutputColumn;
use crate::column_id::ColumnId;
use novarocks_physical_plan::{MembershipComparison, MembershipDistribution};

#[derive(Clone, Debug)]
pub(crate) struct PlanMembershipNode {
    pub probe: ColumnId,
    pub build: ColumnId,
    pub result: OutputColumn,
    pub output_columns: Vec<OutputColumn>,
    pub negated: bool,
    pub comparison: MembershipComparison,
    pub distribution: MembershipDistribution,
}

#[cfg(test)]
mod tests {
    use crate::analysis::{ApplyClause, PredicateExecutionKind, QueryBody};
    use crate::catalog::memory::PlannerMemoryCatalog;
    use crate::planner::logical::{LogicalPlanKind, LogicalPlanNode};
    use crate::planner::physical::{PhysicalPlanKind, PhysicalPlanNode};
    use novarocks_parser::ast::Statement;
    use novarocks_physical_plan::ProjectRetentionAdmission;

    fn analyzed(
        sql: &str,
    ) -> Result<
        (
            crate::analysis::ResolvedQuery,
            crate::analysis::cte::CTERegistry,
            crate::column_id::ColumnRefFactory,
        ),
        crate::analyze_error::AnalyzeError,
    > {
        let statements = novarocks_parser::parse(sql).unwrap();
        let [Statement::Query(query)] = statements.as_slice() else {
            panic!("query expected");
        };
        crate::analyzer::analyze_json_membership_candidate(
            query,
            &PlannerMemoryCatalog::default(),
            "default",
        )
    }

    fn logical(sql: &str) -> LogicalPlanNode {
        let (query, ctes, mut factory) = analyzed(sql).unwrap();
        crate::planner::logical::build::plan_query(query, ctes, &mut factory).unwrap()
    }

    fn memberships(plan: &LogicalPlanNode) -> Vec<&LogicalPlanNode> {
        let mut output = Vec::new();
        fn walk<'a>(plan: &'a LogicalPlanNode, output: &mut Vec<&'a LogicalPlanNode>) {
            if matches!(plan.kind, LogicalPlanKind::Membership(_)) {
                output.push(plan);
            }
            for child in &plan.children {
                walk(child, output);
            }
        }
        walk(plan, &mut output);
        output
    }

    #[test]
    fn membership_candidate_admits_only_proven_json_value_clauses() {
        for (sql, clause) in [
            (
                "SELECT parse_json('1') IN (SELECT parse_json('1'))",
                ApplyClause::Projection,
            ),
            (
                "SELECT x FROM (VALUES (1), (2)) t(x) WHERE parse_json('1') IN (SELECT parse_json('1')) OR x = 2",
                ApplyClause::Where,
            ),
            (
                "SELECT x, count(*) FROM (VALUES (1), (2)) t(x) GROUP BY x HAVING parse_json('1') NOT IN (SELECT parse_json('2')) OR count(*) = 0",
                ApplyClause::Having,
            ),
            (
                "SELECT CASE WHEN x > 0 THEN parse_json('1') ELSE parse_json('2') END IN (SELECT parse_json('1')) FROM (VALUES (1), (2)) t(x)",
                ApplyClause::Projection,
            ),
            (
                "WITH rhs AS (SELECT parse_json('1') AS j) SELECT parse_json('1') IN (SELECT j FROM rhs)",
                ApplyClause::Projection,
            ),
        ] {
            let (query, _, _) = analyzed(sql).unwrap_or_else(|error| panic!("{sql}: {error:?}"));
            let QueryBody::Select(select) = query.body else {
                panic!("select expected");
            };
            let [spec] = select.predicate_apply_specs.as_slice() else {
                panic!("one membership expected: {sql}");
            };
            assert_eq!(spec.clause, clause);
            assert!(matches!(
                spec.execution_kind,
                PredicateExecutionKind::JsonMembership { .. }
            ));
            assert!(select.apply_specs.is_empty());
            assert!(spec.output_column.nullable);
        }
    }

    #[test]
    fn membership_public_analyzer_guard_remains_closed() {
        let statements =
            novarocks_parser::parse("SELECT parse_json('1') IN (SELECT parse_json('1'))").unwrap();
        let [Statement::Query(query)] = statements.as_slice() else {
            panic!("query expected");
        };
        assert!(
            crate::analyzer::analyze(query, &PlannerMemoryCatalog::default(), "default").is_err()
        );
        // Utf8 alone is not JSON evidence; the existing String path remains available.
        let (query, _, _) = analyzed("SELECT 'a' IN (SELECT 'a')").unwrap();
        let QueryBody::Select(select) = query.body else {
            panic!("select expected");
        };
        assert!(
            select
                .predicate_apply_specs
                .iter()
                .all(|spec| matches!(spec.execution_kind, PredicateExecutionKind::Apply))
        );
    }

    #[test]
    fn membership_rejects_excluded_clauses_and_mixed_carriers() {
        for sql in [
            "SELECT 1 WHERE parse_json('1') IN (SELECT parse_json('1'))",
            "SELECT 1 WHERE NOT (parse_json('1') IN (SELECT parse_json('1')))",
            "SELECT 1 WHERE CASE WHEN parse_json('1') IN (SELECT parse_json('1')) THEN true ELSE false END",
            "SELECT sum(CASE WHEN parse_json('1') IN (SELECT parse_json('1')) THEN 1 ELSE 0 END)",
            "SELECT parse_json('1') IN (SELECT '1')",
            "SELECT '1' IN (SELECT parse_json('1'))",
            "SELECT parse_json('1') IN (SELECT parse_json('1'), parse_json('2'))",
            "SELECT (parse_json('1'), parse_json('2')) IN (SELECT parse_json('1'), parse_json('2'))",
            "SELECT t.x FROM (VALUES (1)) t(x) JOIN (VALUES (1)) u(x) ON parse_json('1') IN (SELECT parse_json('1'))",
        ] {
            assert!(analyzed(sql).is_err(), "excluded query accepted: {sql}");
        }
    }

    #[test]
    fn membership_rejects_rhs_correlation_beyond_where() {
        for sql in [
            "SELECT parse_json('1') IN (SELECT parse_json(CAST(t.x AS VARCHAR))) FROM (VALUES (1)) t(x)",
            "SELECT parse_json('1') IN (SELECT parse_json('1') ORDER BY t.x) FROM (VALUES (1)) t(x)",
            "SELECT parse_json('1') IN (SELECT CASE WHEN t.x = 1 THEN parse_json('1') ELSE parse_json('2') END) FROM (VALUES (1)) t(x)",
            "SELECT parse_json('1') IN (SELECT parse_json('1') FROM (VALUES (t.x)) u(x)) FROM (VALUES (1)) t(x)",
            "SELECT parse_json('1') IN (WITH c AS (SELECT parse_json(CAST(t.x AS VARCHAR)) AS j) SELECT j FROM c) FROM (VALUES (1)) t(x)",
            "SELECT parse_json('1') IN (SELECT j FROM (SELECT parse_json(CAST(t.x AS VARCHAR)) AS j) u) FROM (VALUES (1)) t(x)",
        ] {
            assert!(analyzed(sql).is_err(), "correlated query accepted: {sql}");
        }
    }

    #[test]
    fn membership_logical_materializers_own_clause_rows_and_empty_rhs_probe() {
        let plan = logical(
            "SELECT CASE WHEN random() > 0.5 THEN parse_json('1') ELSE parse_json('2') END IN (SELECT parse_json('1') WHERE false) FROM (VALUES (1), (1)) t(x)",
        );
        let nodes = memberships(&plan);
        assert_eq!(nodes.len(), 1);
        let membership = nodes[0];
        let LogicalPlanKind::Membership(spec) = &membership.kind else {
            unreachable!();
        };
        for child in &membership.children {
            let LogicalPlanKind::Project(project) = &child.kind else {
                panic!("materializer expected");
            };
            assert_eq!(
                project.retention_admission,
                ProjectRetentionAdmission::CheckedTask
            );
        }
        let LogicalPlanKind::Project(probe) = &membership.children[0].kind else {
            unreachable!();
        };
        let operand = probe
            .items
            .iter()
            .find(|item| item.output_column_id == spec.probe)
            .unwrap();
        assert!(format!("{:?}", operand.expr.kind).contains("Volatile"));
        assert_eq!(spec.output_columns.len(), probe.items.len() + 1);
        assert!(spec.result.nullable);
        let having = logical(
            "SELECT x, count(*) FROM (VALUES (1), (2)) t(x) GROUP BY x HAVING CASE WHEN count(*) > 0 THEN parse_json('1') ELSE parse_json('2') END IN (SELECT parse_json('1')) OR count(*) = 0",
        );
        let nodes = memberships(&having);
        assert!(matches!(
            nodes[0].children[0].children[0].kind,
            LogicalPlanKind::Aggregate(_)
        ));
    }

    #[test]
    fn membership_having_rejects_aggregate_arguments_but_keeps_post_aggregate_probe() {
        let aggregate_input = "SELECT count(*) FROM (VALUES (1)) t(x) HAVING sum(CASE WHEN parse_json('1') IN (SELECT parse_json('1')) THEN 1 ELSE 0 END) > 0 OR count(*) = 0";
        let error = analyzed(aggregate_input).unwrap_err();
        assert!(error.to_string().contains("aggregate arguments"), "{error}");

        let post_aggregate = "SELECT count(*) FROM (VALUES (1), (1)) t(x) HAVING CASE WHEN count(*) > 0 THEN parse_json('1') ELSE parse_json('2') END IN (SELECT parse_json('1')) OR count(*) = 0";
        let (query, _, _) = analyzed(post_aggregate).unwrap();
        let QueryBody::Select(select) = query.body else {
            panic!("select expected");
        };
        assert_eq!(select.predicate_apply_specs.len(), 1);
        assert_eq!(select.predicate_apply_specs[0].clause, ApplyClause::Having);
        assert!(matches!(
            select.predicate_apply_specs[0].execution_kind,
            PredicateExecutionKind::JsonMembership { .. }
        ));
        let plan = logical(post_aggregate);
        let nodes = memberships(&plan);
        assert_eq!(nodes.len(), 1);
        assert!(matches!(
            nodes[0].children[0].children[0].kind,
            LogicalPlanKind::Aggregate(_)
        ));
    }

    #[test]
    fn membership_optimizer_preserves_two_checked_materializers_and_json_evidence() {
        let (query, ctes, mut factory) = analyzed("WITH rhs AS (SELECT parse_json('1') AS j) SELECT CASE WHEN random() > 0.5 THEN parse_json('1') ELSE parse_json('2') END IN (SELECT j FROM rhs)").unwrap();
        let logical =
            crate::planner::logical::build::plan_query(query, ctes, &mut factory).unwrap();
        let mut scalars = crate::optimizer::scalar::ScalarArena::new();
        let opt = crate::planner::optimizer_bridge::logical::try_to_optimizer_expr(
            &logical,
            &mut scalars,
        )
        .unwrap();
        let optimized = crate::optimizer::optimize_with_test_table_statistics(
            opt,
            scalars,
            &Default::default(),
            factory,
            Vec::new(),
            &Default::default(),
        )
        .unwrap();
        let physical = crate::planner::optimizer_bridge::to_physical_plan(&optimized).unwrap();
        fn walk(plan: &PhysicalPlanNode, checked: &mut usize, members: &mut usize) {
            if let PhysicalPlanKind::Project(op) = &plan.kind {
                *checked +=
                    usize::from(op.retention_admission == ProjectRetentionAdmission::CheckedTask);
            }
            if let PhysicalPlanKind::Membership(op) = &plan.kind {
                *members += 1;
                for (child, id) in [(0, op.probe), (1, op.build)] {
                    assert_eq!(
                        plan.children[child].logical_kinds.get(&id),
                        Some(&novarocks_physical_plan::ValueLogicalKind::Json)
                    );
                }
            }
            assert!(!matches!(
                plan.kind,
                PhysicalPlanKind::HashJoin(_) | PhysicalPlanKind::NestLoopJoin(_)
            ));
            for child in &plan.children {
                walk(child, checked, members);
            }
        }
        let mut checked = 0;
        let mut members = 0;
        walk(&physical, &mut checked, &mut members);
        assert_eq!((checked, members), (2, 1));
    }
}
