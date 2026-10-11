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

//! Real SQL binding and optimizer rules preserve whole-call Data in both modes.
use super::*;
use crate::optimizer::{OptimizedOperatorNode, operator::Operator, scalar::ScalarNode};
use arrow::datatypes::DataType;
use novarocks_functions::PureKernelAbi;
use novarocks_type_contract::FunctionIntrinsicRowError;

fn compile(sql: &str, mode: SqlPhysicalEmissionMode) -> SqlOptimizedOutput {
    let source = crate::planner::table::test_sql_scan_source(
        crate::planner::table::SqlScanKind::ConnectorRead,
    );
    let crate::planner::table::ScanSource::Sql(scan) = &source else {
        unreachable!()
    };
    let statistics = crate::planning::dml::DmlStatisticsSnapshot::from_evidence([
        crate::planning::dml::DmlStatisticsEvidence::Missing {
            binding: scan.binding,
            label: format!(
                "{}.{}.{}",
                scan.table.catalog, scan.table.namespace, scan.table.table
            ),
            reason: "test fixture has no published statistics".into(),
        },
    ]);
    let mut catalog = crate::planning::catalog::PlannerMemoryCatalog::default();
    catalog.create_database("db").unwrap();
    catalog
        .register(
            "db",
            crate::planner::table::TableDef {
                name: "t".into(),
                columns: ["id", "n"]
                    .into_iter()
                    .map(|name| novarocks_types::schema::ColumnDef {
                        name: name.into(),
                        data_type: DataType::Int64,
                        nullable: true,
                        write_default: None,
                        logical_type: None,
                    })
                    .collect(),
                iceberg_row_lineage_metadata_columns: Vec::new(),
                source,
            },
        )
        .unwrap();
    let snapshot = SqlPlannerTableSnapshot::new(&catalog);
    let control = SqlCompileControl::unbounded();
    let analyzed = SqlCompiler::analyze(SqlAnalyzeRequest::new(
        SqlStatementInput::sql(sql),
        SqlCompileIntent::Query,
        SqlSessionContext {
            sql_semantics: crate::sql_mode::SqlSemanticSettings::default(),
            current_catalog: None,
            current_database: "db".into(),
            optimizer_settings: SessionOptimizerSettings {
                enable_materialized_view_rewrite: Some(false),
                enable_common_subexpr_reuse: Some(false),
                ..SessionOptimizerSettings::default()
            },
        },
        SqlPlanningEnvironment::Distributed,
        &snapshot,
        crate::functions::builtin_sql_function_catalog(),
        noop_constant_evaluator(),
        None,
        crate::constant::test_constant_policy(),
        mode,
        control.clone(),
    ))
    .unwrap()
    .into_pending()
    .unwrap();
    SqlCompiler::optimize(SqlOptimizeRequest::new(analyzed, &statistics, control))
        .unwrap()
        .into_optimized_output()
        .unwrap()
}

fn whole_data_project(node: &OptimizedOperatorNode, mode: SqlPhysicalEmissionMode) -> bool {
    let Operator::PhysicalProject(project) = &node.op else {
        return false;
    };
    let arena = node.execution_props.scalar_arena.as_ref().unwrap();
    project.items.iter().any(|item| {
        let ScalarNode::FunctionCall { binding, .. } = arena.node(item.expr) else {
            return false;
        };
        if binding.function_id.as_str() != "builtin.scalar/json_object/v1" {
            return false;
        }
        assert!(binding.declared_scalar_invocation_data);
        assert_eq!(
            binding.semantics.intrinsic_row_error,
            FunctionIntrinsicRowError::NoRowError
        );
        assert_eq!(
            binding.admitted_execution_abi,
            match mode {
                SqlPhysicalEmissionMode::OriginalNativeV1 => None,
                SqlPhysicalEmissionMode::ExactComputedWithOriginalDeclaration =>
                    Some(PureKernelAbi::ScalarInvocationV1),
            }
        );
        assert!(crate::optimizer::scalar_expr::can_fail(arena, item.expr));
        assert_eq!(arena.data_type(item.expr), &DataType::Utf8);
        true
    })
}
fn subtree_has_whole_data(node: &OptimizedOperatorNode, mode: SqlPhysicalEmissionMode) -> bool {
    whole_data_project(node, mode)
        || node
            .children
            .iter()
            .any(|child| subtree_has_whole_data(child, mode))
}
fn count_preserved_filter(node: &OptimizedOperatorNode, mode: SqlPhysicalEmissionMode) -> usize {
    let own = match &node.op {
        Operator::PhysicalFilter(_) => usize::from(
            node.children
                .iter()
                .any(|child| subtree_has_whole_data(child, mode)),
        ),
        Operator::PhysicalScan(scan) => {
            assert!(
                scan.predicates.is_empty(),
                "no predicate hides the throwing project"
            );
            0
        }
        _ => 0,
    };
    own + node
        .children
        .iter()
        .map(|child| count_preserved_filter(child, mode))
        .sum::<usize>()
}

#[test]
fn c16_p1_json_object_real_sql_pruning_and_filter_rules_preserve_whole_data_in_both_modes() {
    for mode in [
        SqlPhysicalEmissionMode::OriginalNativeV1,
        SqlPhysicalEmissionMode::ExactComputedWithOriginalDeclaration,
    ] {
        for filtered in [false, true] {
            let suffix = if filtered {
                "WHERE id = 2 ORDER BY id"
            } else {
                "ORDER BY id"
            };
            let sql = format!(
                "SELECT id FROM (SELECT id, json_object(IF(id = 2, NULL, [id, n]), n) AS unused_error FROM t) s {suffix}"
            );
            let output = compile(&sql, mode);
            assert!(
                subtree_has_whole_data(&output.optimized_tree, mode),
                "PruneProjectColumns must retain the unreferenced whole-call Data use"
            );
            if filtered {
                assert!(
                    count_preserved_filter(&output.optimized_tree, mode) > 0,
                    "PushFilterThroughProject must retain the outer filter above Data"
                );
            }
        }
    }
}
