//! LowCardinalityDictionaryRewrite — the rule wrapper.

use crate::sql::optimizer::rewrite::context::RewriteContext;
use crate::sql::optimizer::rewrite::phase::RewritePhase;
use crate::sql::optimizer::rewrite::result::RewriteResult;
use crate::sql::optimizer::rewrite::rule::{LogicalRewriteRule, RewriteTraversal};
use crate::sql::planner::plan::LogicalPlan;

use super::{collector, rewriter};

pub(crate) struct LowCardinalityDictionaryRewriteRule;

impl LogicalRewriteRule for LowCardinalityDictionaryRewriteRule {
    fn name(&self) -> &'static str {
        "LowCardinalityDictionaryRewrite"
    }

    fn phase(&self) -> RewritePhase {
        RewritePhase::StructuralRewrite
    }

    fn traversal(&self) -> RewriteTraversal {
        RewriteTraversal::TopDown
    }

    fn matches(&self, plan: &LogicalPlan, ctx: &RewriteContext) -> bool {
        ctx.dictionary_provider().is_some() && contains_scan(plan)
    }

    fn apply(&self, plan: LogicalPlan, ctx: &mut RewriteContext) -> Result<RewriteResult, String> {
        let mut dict_ctx = collector::collect(&plan, ctx)?;
        if !dict_ctx.has_any_scan_column() {
            return Ok(RewriteResult::Unchanged);
        }
        let rewritten = rewriter::rewrite(plan, &mut dict_ctx)?;
        if dict_ctx.changed() {
            Ok(RewriteResult::Changed(rewritten))
        } else {
            Ok(RewriteResult::Unchanged)
        }
    }
}

fn contains_scan(plan: &LogicalPlan) -> bool {
    match plan {
        LogicalPlan::Scan(_) => true,
        LogicalPlan::Filter(node) => contains_scan(&node.input),
        LogicalPlan::Project(node) => contains_scan(&node.input),
        LogicalPlan::Aggregate(node) => contains_scan(&node.input),
        LogicalPlan::Sort(node) => contains_scan(&node.input),
        LogicalPlan::Limit(node) => contains_scan(&node.input),
        LogicalPlan::Window(node) => contains_scan(&node.input),
        LogicalPlan::TableFunction(node) => contains_scan(&node.input),
        LogicalPlan::SubqueryAlias(node) => contains_scan(&node.input),
        LogicalPlan::Repeat(node) => contains_scan(&node.input),
        LogicalPlan::CTEProduce(node) => contains_scan(&node.input),
        LogicalPlan::Decode(node) => contains_scan(&node.input),
        LogicalPlan::Join(node) => contains_scan(&node.left) || contains_scan(&node.right),
        LogicalPlan::CTEAnchor(node) => {
            contains_scan(&node.produce) || contains_scan(&node.consumer)
        }
        LogicalPlan::Union(node) => node.inputs.iter().any(contains_scan),
        LogicalPlan::Intersect(node) => node.inputs.iter().any(contains_scan),
        LogicalPlan::Except(node) => node.inputs.iter().any(contains_scan),
        LogicalPlan::Values(_) | LogicalPlan::GenerateSeries(_) | LogicalPlan::CTEConsume(_) => {
            false
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use arrow::datatypes::DataType;

    use crate::engine::dictionary::model::{
        DictionaryOwner, DictionarySnapshot, DictionaryState, DictionaryValue, DictionaryWatermark,
    };
    use crate::sql::analysis::{ExprKind, OutputColumn, ProjectItem, SortItem, TypedExpr};
    use crate::sql::catalog::{ColumnDef, ScanSource, TableDef};
    use crate::sql::column_id::ColumnId;
    use crate::sql::optimizer::rewrite::context::{QueryDictionaryProvider, RewriteContext};
    use crate::sql::optimizer::rewrite::registry::query_rewrite_pipeline;
    use crate::sql::planner::plan::{
        AggregateCall, AggregateNode, DecodeNode, LogicalPlan, ScanNode, SortNode,
    };

    struct StaticProvider {
        snapshot: DictionarySnapshot,
    }

    impl QueryDictionaryProvider for StaticProvider {
        fn load_active_snapshot(
            &self,
            _table: &TableDef,
            _database: &str,
            column_name: &str,
        ) -> Result<Option<DictionarySnapshot>, String> {
            if column_name.eq_ignore_ascii_case(&self.snapshot.column_name) {
                Ok(Some(self.snapshot.clone()))
            } else {
                Ok(None)
            }
        }
    }

    fn sample_snapshot(order_preserving: bool) -> DictionarySnapshot {
        DictionarySnapshot {
            dictionary_id: 1,
            owner: DictionaryOwner::StarRocksTable {
                database: "db".to_string(),
                table: "t".to_string(),
                db_id: 1,
                table_id: 2,
            },
            column_id: Some(10),
            column_name: "s".to_string(),
            data_type: DataType::Utf8,
            version: 1,
            watermark: DictionaryWatermark::Iceberg {
                snapshot_id: None,
                schema_id: 0,
            },
            values: vec![DictionaryValue {
                id: 1,
                bytes: b"a".to_vec(),
            }],
            null_id: 0,
            state: DictionaryState::Active,
            order_preserving,
        }
    }

    fn make_table() -> TableDef {
        TableDef {
            name: "t".to_string(),
            columns: vec![ColumnDef {
                name: "s".to_string(),
                data_type: DataType::Utf8,
                nullable: false,
                write_default: None,
                logical_type: None,
            }],
            iceberg_row_lineage_metadata_columns: vec![],
            source: ScanSource::StarRocks,
        }
    }

    fn s_output_column() -> OutputColumn {
        OutputColumn {
            column_id: ColumnId::UNSET,
            name: "s".to_string(),
            data_type: DataType::Utf8,
            nullable: false,
        }
    }

    fn s_column_ref() -> TypedExpr {
        TypedExpr {
            kind: ExprKind::ColumnRef {
                column_id: ColumnId::UNSET,
                qualifier: None,
                column: "s".to_string(),
            },
            data_type: DataType::Utf8,
            nullable: false,
        }
    }

    fn install_provider(ctx: &mut RewriteContext, order_preserving: bool) {
        let provider = StaticProvider {
            snapshot: sample_snapshot(order_preserving),
        };
        ctx.set_dictionary_provider(Arc::new(provider));
    }

    #[test]
    fn group_by_string_rewrites_to_dict_column_and_decode() {
        let scan = LogicalPlan::Scan(ScanNode {
            database: "db".to_string(),
            table: make_table(),
            alias: None,
            columns: vec![s_output_column()],
            predicates: vec![],
            required_columns: None,
            dict_columns: vec![],
        });
        let aggregate = LogicalPlan::Aggregate(AggregateNode {
            input: Box::new(scan),
            group_by: vec![s_column_ref()],
            aggregates: vec![AggregateCall {
                name: "count".to_string(),
                args: vec![],
                distinct: false,
                result_type: DataType::Int64,
                order_by: vec![],
            }],
            output_columns: vec![
                s_output_column(),
                OutputColumn {
                    column_id: ColumnId::UNSET,
                    name: "cnt".to_string(),
                    data_type: DataType::Int64,
                    nullable: false,
                },
            ],
            already_pushed: false,
        });
        let mut ctx = RewriteContext::for_query(Vec::<String>::new());
        install_provider(&mut ctx, true);
        let table_stats = HashMap::new();
        let pipeline = query_rewrite_pipeline(&table_stats);
        let rewritten = pipeline.rewrite(aggregate, &mut ctx).unwrap();
        let LogicalPlan::Decode(decode) = rewritten else {
            panic!("expected decode root, got {rewritten:?}");
        };
        assert_eq!(decode.mappings.len(), 1);
        assert_eq!(decode.mappings[0].dict_column, "__nr_dict_t_s");
        assert_eq!(decode.mappings[0].string_column, "s");
        let LogicalPlan::Aggregate(agg) = *decode.input else {
            panic!("expected aggregate under decode");
        };
        // Group-by must reference the dict column now.
        let key = agg.group_by.first().expect("group by present");
        let ExprKind::ColumnRef { column, .. } = &key.kind else {
            panic!("group-by must be a column ref");
        };
        assert_eq!(column, "__nr_dict_t_s");
        assert_eq!(key.data_type, DataType::Int32);
        // Scan must carry the dict_columns hint and a hidden Int32
        // OutputColumn.
        let LogicalPlan::Scan(scan) = *agg.input else {
            panic!("expected scan under aggregate");
        };
        assert_eq!(scan.dict_columns.len(), 1);
        assert_eq!(scan.dict_columns[0].dict_column, "__nr_dict_t_s");
        assert_eq!(scan.dict_columns[0].source_column, "s");
        assert!(
            scan.columns
                .iter()
                .any(|c| c.name == "__nr_dict_t_s" && matches!(c.data_type, DataType::Int32))
        );
    }

    #[test]
    fn topn_non_order_preserving_decodes_before_sort() {
        let scan = LogicalPlan::Scan(ScanNode {
            database: "db".to_string(),
            table: make_table(),
            alias: None,
            columns: vec![s_output_column()],
            predicates: vec![],
            required_columns: None,
            dict_columns: vec![],
        });
        let sort = LogicalPlan::Sort(SortNode {
            input: Box::new(scan),
            items: vec![SortItem {
                expr: s_column_ref(),
                asc: true,
                nulls_first: false,
            }],
            analytic_partition_by: vec![],
        });
        let mut ctx = RewriteContext::for_query(Vec::<String>::new());
        // Non-order-preserving snapshot — sort must decode first.
        install_provider(&mut ctx, false);
        let table_stats = HashMap::new();
        let pipeline = query_rewrite_pipeline(&table_stats);
        let rewritten = pipeline.rewrite(sort, &mut ctx).unwrap();
        let LogicalPlan::Sort(sort) = rewritten else {
            panic!("expected sort root, got {rewritten:?}");
        };
        // Sort's input is a Decode now.
        let LogicalPlan::Decode(decode) = *sort.input else {
            panic!("expected decode under sort");
        };
        assert_eq!(decode.mappings.len(), 1);
        assert_eq!(decode.mappings[0].dict_column, "__nr_dict_t_s");
    }

    #[test]
    fn disable_rule_skips_dictionary_rewrite() {
        let scan = LogicalPlan::Scan(ScanNode {
            database: "db".to_string(),
            table: make_table(),
            alias: None,
            columns: vec![s_output_column()],
            predicates: vec![],
            required_columns: None,
            dict_columns: vec![],
        });
        let aggregate = LogicalPlan::Aggregate(AggregateNode {
            input: Box::new(scan),
            group_by: vec![s_column_ref()],
            aggregates: vec![AggregateCall {
                name: "count".to_string(),
                args: vec![],
                distinct: false,
                result_type: DataType::Int64,
                order_by: vec![],
            }],
            output_columns: vec![
                s_output_column(),
                OutputColumn {
                    column_id: ColumnId::UNSET,
                    name: "cnt".to_string(),
                    data_type: DataType::Int64,
                    nullable: false,
                },
            ],
            already_pushed: false,
        });
        let mut ctx =
            RewriteContext::for_query(vec!["LowCardinalityDictionaryRewrite".to_string()]);
        install_provider(&mut ctx, true);
        let table_stats = HashMap::new();
        let pipeline = query_rewrite_pipeline(&table_stats);
        let rewritten = pipeline.rewrite(aggregate, &mut ctx).unwrap();
        // With the rule disabled the plan must not contain a Decode
        // boundary or any dict-encoded scan output.
        assert!(
            !matches!(rewritten, LogicalPlan::Decode(_)),
            "expected rule disabled to suppress Decode insertion"
        );
        let LogicalPlan::Aggregate(agg) = rewritten else {
            panic!("expected aggregate root");
        };
        let LogicalPlan::Scan(scan) = *agg.input else {
            panic!("expected scan child");
        };
        assert!(scan.dict_columns.is_empty());
        assert!(scan.columns.iter().all(|c| c.name == "s"));
    }

    // --- Item 1 (Critical) regression: bare column name collision ---

    /// Provider that exposes a dictionary only when the scan's table
    /// AND column match. Lets a test register dict for `t1.name` but
    /// not `t2.name`.
    struct PerTableProvider {
        snapshot: DictionarySnapshot,
        table: String,
    }

    impl QueryDictionaryProvider for PerTableProvider {
        fn load_active_snapshot(
            &self,
            table: &TableDef,
            _database: &str,
            column_name: &str,
        ) -> Result<Option<DictionarySnapshot>, String> {
            if table.name.eq_ignore_ascii_case(&self.table)
                && column_name.eq_ignore_ascii_case(&self.snapshot.column_name)
            {
                Ok(Some(self.snapshot.clone()))
            } else {
                Ok(None)
            }
        }
    }

    fn make_named_table(name: &str, column: &str) -> TableDef {
        TableDef {
            name: name.to_string(),
            columns: vec![ColumnDef {
                name: column.to_string(),
                data_type: DataType::Utf8,
                nullable: false,
                write_default: None,
                logical_type: None,
            }],
            iceberg_row_lineage_metadata_columns: vec![],
            source: ScanSource::StarRocks,
        }
    }

    fn named_output_column(name: &str) -> OutputColumn {
        OutputColumn {
            column_id: ColumnId::UNSET,
            name: name.to_string(),
            data_type: DataType::Utf8,
            nullable: false,
        }
    }

    fn named_snapshot(column: &str, order_preserving: bool) -> DictionarySnapshot {
        let mut snap = sample_snapshot(order_preserving);
        snap.column_name = column.to_string();
        snap
    }

    #[test]
    fn join_with_same_column_name_only_decodes_dict_side() {
        // Two scans, each producing an output column called `name`.
        // Only `t1` has an active dictionary; `t2` has none. After the
        // rewrite, ONLY the `t1` branch must wear a Decode boundary,
        // and the `t2` branch must be untouched.
        let scan_t1 = LogicalPlan::Scan(ScanNode {
            database: "db".to_string(),
            table: make_named_table("t1", "name"),
            alias: None,
            columns: vec![named_output_column("name")],
            predicates: vec![],
            required_columns: None,
            dict_columns: vec![],
        });
        let scan_t2 = LogicalPlan::Scan(ScanNode {
            database: "db".to_string(),
            table: make_named_table("t2", "name"),
            alias: None,
            columns: vec![named_output_column("name")],
            predicates: vec![],
            required_columns: None,
            dict_columns: vec![],
        });
        let join = LogicalPlan::Join(crate::sql::planner::plan::JoinNode {
            left: Box::new(scan_t1),
            right: Box::new(scan_t2),
            join_type: crate::sql::analysis::JoinKind::Cross,
            condition: None,
        });
        let mut ctx = RewriteContext::for_query(Vec::<String>::new());
        ctx.set_dictionary_provider(Arc::new(PerTableProvider {
            snapshot: named_snapshot("name", true),
            table: "t1".to_string(),
        }));
        let table_stats = HashMap::new();
        let pipeline = query_rewrite_pipeline(&table_stats);
        let rewritten = pipeline.rewrite(join, &mut ctx).unwrap();
        let LogicalPlan::Join(join) = rewritten else {
            panic!("expected join root, got {rewritten:?}");
        };
        // Left side: must be Decode(Scan with dict_columns).
        let LogicalPlan::Decode(left_decode) = *join.left else {
            panic!("expected left side to be Decode, got {:?}", *join.left);
        };
        assert_eq!(left_decode.mappings.len(), 1);
        assert_eq!(left_decode.mappings[0].dict_column, "__nr_dict_t1_name");
        let LogicalPlan::Scan(left_scan) = *left_decode.input else {
            panic!("expected scan under left decode");
        };
        assert_eq!(left_scan.dict_columns.len(), 1);
        // Right side: must be a plain Scan, no Decode, no dict_columns.
        let LogicalPlan::Scan(right_scan) = *join.right else {
            panic!(
                "expected right side to be plain Scan, got {:?}",
                *join.right
            );
        };
        assert!(
            right_scan.dict_columns.is_empty(),
            "t2.name has no dict snapshot — must not be dict-encoded"
        );
        assert!(
            right_scan.columns.iter().all(|c| c.name == "name"),
            "t2 scan output must contain only the original `name` column"
        );
    }

    // --- Item 3 (Important) regression: project rename propagates dict mapping ---

    #[test]
    fn project_alias_propagates_dict_through_join_boundary() {
        // SELECT s AS t FROM dict_table feeding a cross join into a
        // no-dict scan. After rewrite, the alias-side branch must wrap
        // the Project with a Decode driven by the dict slot.
        let scan_left = LogicalPlan::Scan(ScanNode {
            database: "db".to_string(),
            table: make_table(),
            alias: None,
            columns: vec![s_output_column()],
            predicates: vec![],
            required_columns: None,
            dict_columns: vec![],
        });
        // Project: SELECT s AS t.
        let project = LogicalPlan::Project(crate::sql::planner::plan::ProjectNode {
            input: Box::new(scan_left),
            items: vec![ProjectItem {
                expr: s_column_ref(),
                output_name: "t".to_string(),
            }],
        });
        // Right side: a no-dict scan over a different table.
        let scan_right = LogicalPlan::Scan(ScanNode {
            database: "db".to_string(),
            table: make_named_table("other", "x"),
            alias: None,
            columns: vec![named_output_column("x")],
            predicates: vec![],
            required_columns: None,
            dict_columns: vec![],
        });
        let join = LogicalPlan::Join(crate::sql::planner::plan::JoinNode {
            left: Box::new(project),
            right: Box::new(scan_right),
            join_type: crate::sql::analysis::JoinKind::Cross,
            condition: None,
        });
        let mut ctx = RewriteContext::for_query(Vec::<String>::new());
        install_provider(&mut ctx, true);
        let table_stats = HashMap::new();
        let pipeline = query_rewrite_pipeline(&table_stats);
        let rewritten = pipeline.rewrite(join, &mut ctx).unwrap();
        let LogicalPlan::Join(join) = rewritten else {
            panic!("expected join root, got {rewritten:?}");
        };
        // Left side: Decode wrapping a Project wrapping the dict-enabled Scan.
        let LogicalPlan::Decode(left_decode) = *join.left else {
            panic!(
                "expected left to be Decode(Project(Scan)), got {:?}",
                *join.left
            );
        };
        assert_eq!(left_decode.mappings.len(), 1);
        assert_eq!(left_decode.mappings[0].dict_column, "__nr_dict_t_s");
        // The decode's string_column must reference the alias name `t`,
        // not the underlying `s` — otherwise downstream consumers
        // looking up by alias would not match.
        assert_eq!(left_decode.mappings[0].string_column, "t");
    }
}
