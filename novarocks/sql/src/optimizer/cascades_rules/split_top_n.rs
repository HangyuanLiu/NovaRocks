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

//! Transformation rule: LogicalTopN(FINAL, !split) -> LogicalTopN(FINAL, split=true) over LogicalTopN(PARTIAL).
//!
//! Mirrors StarRocks's SplitTopNRule.java. Cost search picks between the
//! single-stage TopN (original) and this two-stage alternative.

use crate::optimizer::memo::{MExpr, Memo};
use crate::optimizer::operator::{Operator, TopNOp, TopNPhase};
use crate::optimizer::rule::{NewExpr, Rule, RuleType};

pub(crate) struct SplitTopN;

impl Rule for SplitTopN {
    fn name(&self) -> &str {
        "SplitTopN"
    }

    fn rule_type(&self) -> RuleType {
        RuleType::Transformation
    }

    fn matches(&self, op: &Operator) -> bool {
        matches!(
            op,
            Operator::LogicalTopN(t) if t.phase == TopNPhase::Final && !t.is_split
        )
    }

    fn apply(
        &self,
        expr: &MExpr,
        memo: &mut Memo,
        control: &dyn novarocks_type_contract::PureCompileControl,
    ) -> Result<Vec<NewExpr>, crate::compiler::SqlCompileError> {
        let _ = control;

        let Operator::LogicalTopN(src) = &expr.op else {
            return Ok(vec![]);
        };
        // Finite limit required; plain ORDER BY without LIMIT is out of scope.
        let limit = match src.limit {
            Some(l) if l >= 0 => l,
            _ => return Ok(vec![]),
        };
        let offset = src.offset.unwrap_or(0).max(0);
        if offset > 0 {
            // Standalone execution already preserves LIMIT/OFFSET correctly for
            // single-stage TopN. Split TopN with a non-zero final offset still
            // needs tighter parity work in the merging exchange path, so keep
            // the conservative single-stage plan for semantic correctness.
            return Ok(vec![]);
        }
        // Saturating add: if L+O would overflow, cap at i64::MAX (effectively
        // means "partial passes everything through"; cost search will prefer
        // single-stage in that corner case).
        let partial_limit = limit.saturating_add(offset);

        // PARTIAL child: same sort items, larger limit, zero offset.
        let partial_mexpr = MExpr {
            id: memo.next_expr_id(),
            op: Operator::LogicalTopN(TopNOp {
                items: src.items.clone(),
                limit: Some(partial_limit),
                offset: Some(0),
                phase: TopNPhase::Partial,
                is_split: false,
            }),
            children: expr.children.clone(),
        };
        let partial_group = memo.new_group(partial_mexpr);

        // FINAL with split flag, original limit/offset. Child = new partial group.
        let final_expr = NewExpr {
            op: Operator::LogicalTopN(TopNOp {
                items: src.items.clone(),
                limit: src.limit,
                offset: src.offset,
                phase: TopNPhase::Final,
                is_split: true,
            }),
            children: vec![partial_group],
        };
        Ok(vec![final_expr])
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::optimizer::memo::Memo;
    use crate::optimizer::operator::{ScanOp, TopNOp};

    fn mk_scan_group(memo: &mut Memo) -> usize {
        let m = MExpr {
            id: memo.next_expr_id(),
            op: Operator::LogicalScan(ScanOp {
                database: "db".into(),
                table: crate::planner::table::TableDef {
                    name: "t".into(),
                    columns: vec![],
                    iceberg_row_lineage_metadata_columns: vec![],
                    source: crate::compiler::mv_rewrite::test_scan_source(
                        crate::planner::table::SqlScanKind::ConnectorRead,
                    ),
                },
                alias: None,
                stats_ref: None,
                columns: vec![],
                predicates: vec![],
                required_columns: None,
                variant_columns: vec![],
                mv_rewritten_from: None,
            }),
            children: vec![],
        };
        memo.new_group(m)
    }

    #[test]
    fn fires_on_final_unsplit_with_limit() {
        let mut memo = Memo::new();
        let scan_group = mk_scan_group(&mut memo);
        let topn_mexpr = MExpr {
            id: memo.next_expr_id(),
            op: Operator::LogicalTopN(TopNOp {
                items: vec![],
                limit: Some(100),
                offset: Some(0),
                phase: TopNPhase::Final,
                is_split: false,
            }),
            children: vec![scan_group],
        };
        let out = SplitTopN
            .apply(
                &topn_mexpr,
                &mut memo,
                &crate::compiler::SqlCompileControl::unbounded(),
            )
            .unwrap();
        assert_eq!(out.len(), 1, "expected one split alternative");
        match &out[0].op {
            Operator::LogicalTopN(t) => {
                assert_eq!(t.phase, TopNPhase::Final);
                assert!(t.is_split);
                assert_eq!(t.limit, Some(100));
                assert_eq!(t.offset, Some(0));
            }
            other => panic!("expected LogicalTopN final+split, got {:?}", other),
        }
        // FINAL's child is a new group containing the PARTIAL TopN.
        assert_eq!(out[0].children.len(), 1);
        let partial_group = &memo.groups[out[0].children[0]];
        match &partial_group.logical_exprs[0].op {
            Operator::LogicalTopN(t) => {
                assert_eq!(t.phase, TopNPhase::Partial);
                assert!(!t.is_split);
                assert_eq!(t.limit, Some(100), "partial limit must be L+O = 100+0");
                assert_eq!(t.offset, Some(0));
            }
            other => panic!("expected LogicalTopN partial, got {:?}", other),
        }
        assert_eq!(partial_group.logical_exprs[0].children, vec![scan_group]);
    }

    #[test]
    fn does_not_fire_with_non_zero_offset() {
        let mut memo = Memo::new();
        let scan_group = mk_scan_group(&mut memo);
        let topn_mexpr = MExpr {
            id: memo.next_expr_id(),
            op: Operator::LogicalTopN(TopNOp {
                items: vec![],
                limit: Some(100),
                offset: Some(10),
                phase: TopNPhase::Final,
                is_split: false,
            }),
            children: vec![scan_group],
        };
        let out = SplitTopN
            .apply(
                &topn_mexpr,
                &mut memo,
                &crate::compiler::SqlCompileControl::unbounded(),
            )
            .unwrap();
        assert!(
            out.is_empty(),
            "non-zero offset should stay single-stage for now"
        );
    }

    #[test]
    fn does_not_fire_on_partial() {
        let mut memo = Memo::new();
        let scan_group = mk_scan_group(&mut memo);
        let topn_mexpr = MExpr {
            id: memo.next_expr_id(),
            op: Operator::LogicalTopN(TopNOp {
                items: vec![],
                limit: Some(100),
                offset: None,
                phase: TopNPhase::Partial,
                is_split: false,
            }),
            children: vec![scan_group],
        };
        assert!(
            !SplitTopN.matches(&topn_mexpr.op),
            "rule must not match on PARTIAL phase"
        );
    }

    #[test]
    fn does_not_fire_when_already_split() {
        let mut memo = Memo::new();
        let scan_group = mk_scan_group(&mut memo);
        let topn_mexpr = MExpr {
            id: memo.next_expr_id(),
            op: Operator::LogicalTopN(TopNOp {
                items: vec![],
                limit: Some(100),
                offset: None,
                phase: TopNPhase::Final,
                is_split: true,
            }),
            children: vec![scan_group],
        };
        assert!(
            !SplitTopN.matches(&topn_mexpr.op),
            "rule must not match when already split"
        );
    }

    #[test]
    fn does_not_fire_without_limit() {
        let mut memo = Memo::new();
        let scan_group = mk_scan_group(&mut memo);
        let topn_mexpr = MExpr {
            id: memo.next_expr_id(),
            op: Operator::LogicalTopN(TopNOp {
                items: vec![],
                limit: None,
                offset: Some(5),
                phase: TopNPhase::Final,
                is_split: false,
            }),
            children: vec![scan_group],
        };
        let out = SplitTopN
            .apply(
                &topn_mexpr,
                &mut memo,
                &crate::compiler::SqlCompileControl::unbounded(),
            )
            .unwrap();
        assert!(out.is_empty(), "no limit => out of scope");
    }

    #[test]
    fn grouped_topn_completes_with_rows_budgets_after_global_aggregation() {
        use novarocks_physical_plan::{AggregateGrouping, NodeKind, TopNReduction};

        for sql in [
            "SELECT order_key, COUNT(*), MIN(order_key), MAX(order_key) FROM orders GROUP BY order_key ORDER BY order_key LIMIT 2",
            "SELECT DISTINCT order_key FROM orders ORDER BY order_key DESC LIMIT 2",
        ] {
            // This drives the real optimizer and physical completion with a
            // runtime-split provider and a distributed DOP domain. Completion
            // must prove the TopN and aggregate sequences, not just memo shape.
            let owner = crate::compiler::compile_authored_aggregate_for_test(sql);
            let plan = owner.plan();
            assert!(plan.fragments().values().any(|fragment| {
                fragment.nodes().values().any(|node| {
                    matches!(
                        node.kind,
                        NodeKind::Aggregate {
                            grouping: AggregateGrouping::Complete,
                            ..
                        }
                    )
                })
            }));
            let mut topn_count = 0;
            for fragment in plan.fragments().values() {
                for node in fragment.nodes().values() {
                    if let NodeKind::TopN {
                        reduction, phase, ..
                    } = &node.kind
                    {
                        topn_count += 1;
                        assert!(matches!(reduction, TopNReduction::Rows));
                        if matches!(phase, novarocks_physical_plan::TopNPhase::Partial { .. }) {
                            let input = &fragment.nodes()[&node.inputs[0]];
                            assert!(
                                matches!(
                                    input.kind,
                                    NodeKind::Aggregate {
                                        grouping: AggregateGrouping::Complete,
                                        ..
                                    }
                                ),
                                "a grouped row budget must read complete groups"
                            );
                            assert!(plan.runtime_filters().values().any(|filter| {
                                filter.producers.iter().any(|producer| {
                                    producer.endpoint.fragment == fragment.id()
                                        && producer.endpoint.node == input.id
                                        && matches!(producer.target,
                                            novarocks_physical_plan::RuntimeFilterProducerTarget::AggregateTopNKey { topn, .. }
                                                if topn == node.id)
                                }) && filter.consumers.iter().all(|consumer| {
                                    matches!(consumer.activation,
                                        novarocks_physical_plan::RuntimeFilterConsumerActivation::NonBlockingLive { .. })
                                })
                            }), "the normal planner must publish a nonblocking key bound from the complete aggregate");
                        }
                    }
                }
            }
            assert!(topn_count > 0, "the grouped query must retain its TopN");
        }
    }
}
