# MV 查询透明改写（单表 SPJG + 聚合上卷）实施计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 单 base table 的 SPJG 查询能被透明改写为扫描 Iceberg MV 目标表的替代计划，注入 Cascades memo 由 CBO 择优。

**Architecture:** engine 层候选准备（`mv_rewrite_prep.rs`：MV 发现 + 严格 snapshot 新鲜度 + select_sql 重分析 + 目标表 TableDef/统计构造）产出 `MvRewriteCandidate`，经 `optimize()` 新参数传入；优化器层新增 Cascades 变换规则 `MvRewrite`（`cascades_rules/mv_rewrite/`：SPJG 描述符抽取 → 谓词三分类与区间蕴含 → 列映射 → 聚合上卷 → 替代表达式构造）。设计规格见 `docs/design/specs/2026-06-10-mv-query-rewrite-design.md`。

**Tech Stack:** Rust；现有 Cascades 优化器（`src/sql/optimizer/`）；sqlparser；Iceberg connector；sql-test-runner。

**Build/Test 约定:** 单元测试用 `cargo test --lib <module_path>`；全量回归前先 `cargo fmt && cargo clippy`。提交信息英文、无 Co-Authored-By trailer。

**关键既有类型（实现时直接引用，勿重定义）:**

- `TypedExpr { kind: ExprKind, data_type: DataType, nullable: bool }`、`ExprKind`（`ColumnRef{column_id,qualifier,column}` / `Literal(LiteralValue)` / `BinaryOp{left,op,right}` / `FunctionCall{name,args,distinct}` / `Between` / `IsNull` / `InList` / `Like` / `Cast` …）、`BinOp`、`LiteralValue` — `src/sql/analysis/mod.rs:287-497`
- `OutputColumn { column_id, name, data_type, nullable, is_internal }` — `src/sql/analysis/mod.rs:29-38`
- `ProjectItem { expr, output_name, output_column_id }` — `src/sql/analysis/mod.rs:100-109`
- `AggregateCall { name: String, args: Vec<TypedExpr>, distinct: bool, result_type: DataType, order_by: Vec<SortItem>, output_column_id: ColumnId }` — `src/sql/planner/plan.rs:296-309`
- planner 节点 `ScanNode/FilterNode/ProjectNode/AggregateNode` — `src/sql/planner/plan.rs:216-294`
- 优化器算子 `LogicalScanOp/LogicalFilterOp/LogicalProjectOp/LogicalAggregateOp`（`LogicalAggregateOp::single(group_by, aggregates, output_columns)` 构造器）— `src/sql/optimizer/operator.rs:73-143`
- `Memo`（`new_group`/`add_expr_to_group`/`next_expr_id`/`factory`）、`MExpr{id,op,children}` — `src/sql/optimizer/memo.rs`
- `Rule` trait（`name`/`rule_type`/`matches(&Operator)`/`apply(&MExpr,&mut Memo)->Vec<NewExpr>`）、`NewExpr{op,children}` — `src/sql/optimizer/rule.rs:30-41`
- `TableDef{name,columns,iceberg_row_lineage_metadata_columns,source}`、`ScanSource::IcebergDataFiles{table,files,cloud_properties,binding}`、`IcebergTableInfo{catalog,namespace,table,table_uuid,...}`、`IcebergDataFileBinding::CurrentSnapshot`、`ColumnDef` — `src/sql/catalog.rs`
- `ColumnRefFactory::create(qualifier,name,data_type,nullable)->ColumnId`、`peek_next_id()` — `src/sql/column_id.rs:80-162`
- `StoredMvDefinition`（`base_table_refs: Vec<String>` 为 `"catalog.namespace.table"` FQN，由 `IcebergTableRef::fqn()` 生成；`last_refresh_snapshots: BTreeMap<String,i64>` 同键格式；`last_refresh_table_uuids: BTreeMap<String,String>`）— `src/meta/repository/mv.rs:24-59`；`MvMetaRepository::list_definitions(&dyn MetaReadTxn)` — `mv.rs:566-575`

---

### Task 1: 会话变量 `enable_materialized_view_rewrite`

**Files:**
- Modify: `src/sql/optimizer/options.rs`（`SessionOptimizerSettings` 加字段，~line 8-27）
- Modify: `src/server/mod.rs`（`parse_set_boolean` 分发 match，~line 977-990）

- [ ] **Step 1: 写失败的单元测试**

在 `src/sql/optimizer/options.rs` 的 `#[cfg(test)] mod tests` 中追加：

```rust
#[test]
fn mv_rewrite_enabled_defaults_to_true() {
    let settings = SessionOptimizerSettings::default();
    assert!(settings.mv_rewrite_enabled());
    let mut off = SessionOptimizerSettings::default();
    off.enable_materialized_view_rewrite = Some(false);
    assert!(!off.mv_rewrite_enabled());
    let mut on = SessionOptimizerSettings::default();
    on.enable_materialized_view_rewrite = Some(true);
    assert!(on.mv_rewrite_enabled());
}
```

- [ ] **Step 2: 运行测试确认失败**

Run: `cargo test --lib sql::optimizer::options::tests::mv_rewrite_enabled_defaults_to_true`
Expected: 编译错误 `no field enable_materialized_view_rewrite`。

- [ ] **Step 3: 实现**

`SessionOptimizerSettings` 增加字段（遵循已有 `rf_*: Option<...>` 的「None = 默认值」模式，避免改 `#[derive(Default)]`）：

```rust
/// Session override for transparent MV query rewrite.
/// `None` means the default (enabled).
pub enable_materialized_view_rewrite: Option<bool>,
```

并加方法：

```rust
impl SessionOptimizerSettings {
    pub(crate) fn mv_rewrite_enabled(&self) -> bool {
        self.enable_materialized_view_rewrite.unwrap_or(true)
    }
}
```

`src/server/mod.rs` 的 `parse_set_boolean` 分发 match（line ~977）加一臂（放在 `_ =>` 之前）：

```rust
"enable_materialized_view_rewrite" => {
    shim.optimizer_settings.enable_materialized_view_rewrite = Some(enabled)
}
```

- [ ] **Step 4: 运行测试确认通过**

Run: `cargo test --lib sql::optimizer::options`
Expected: PASS（含既有测试）。

- [ ] **Step 5: Commit**

```bash
git add src/sql/optimizer/options.rs src/server/mod.rs
git commit -m "feat(optimizer): add enable_materialized_view_rewrite session variable"
```

---

### Task 2: `analyze_with_factory` 变体

**Files:**
- Modify: `src/sql/analyzer/mod.rs:49-81`（重构 `analyze()` 委托新变体）

- [ ] **Step 1: 写失败的单元测试**

在 `src/sql/analyzer/mod.rs` 的 `#[cfg(test)] mod tests`（无则新建）中：

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::catalog::{CatalogProvider, TableDef};
    use crate::sql::column_id::ColumnRefFactory;
    use crate::sql::DataType;

    struct EmptyCatalog;
    impl CatalogProvider for EmptyCatalog {
        fn get_table(&self, _db: &str, _table: &str) -> Result<TableDef, String> {
            Err("no tables".to_string())
        }
    }

    fn parse_query(sql: &str) -> sqlparser::ast::Query {
        let normalized =
            crate::sql::parser::dialect::normalize_for_raw_parse(sql).expect("normalize");
        let stmt =
            crate::sql::parser::parse_normalized_sql_raw(&normalized).expect("parse");
        let sqlparser::ast::Statement::Query(q) = stmt else {
            panic!("not a query");
        };
        *q
    }

    #[test]
    fn analyze_with_factory_threads_column_ids() {
        // Pre-seed the factory with 3 ids so threaded analysis must start at 4.
        let mut factory = ColumnRefFactory::new();
        for i in 0..3 {
            factory.create(None, format!("seed{i}"), DataType::Int64, false);
        }
        assert_eq!(factory.peek_next_id(), 4);

        let query = parse_query("SELECT 1 + 1 AS x");
        let (_resolved, _ctes, out_factory) =
            analyze_with_factory(&query, &EmptyCatalog, "db", factory).expect("analyze");
        // The analysis must have allocated its ids on top of the seeded ones.
        assert!(out_factory.peek_next_id() > 4);
        assert_eq!(out_factory.get(crate::sql::column_id::ColumnId(1)).name, "seed0");
    }
}
```

注意：若 `CatalogProvider` trait 还有无默认实现的必需方法，按 `src/sql/catalog.rs:395-430` 的实际 trait 定义补齐 `EmptyCatalog`（只需返回 Err 的桩实现）。

- [ ] **Step 2: 运行测试确认失败**

Run: `cargo test --lib sql::analyzer::tests::analyze_with_factory_threads_column_ids`
Expected: 编译错误 `cannot find function analyze_with_factory`。

- [ ] **Step 3: 实现**

把 `analyze()` 体内硬编码的 `ColumnRefFactory::new()` 抽出：

```rust
pub(crate) fn analyze(
    query: &sqlast::Query,
    catalog: &dyn CatalogProvider,
    current_database: &str,
) -> Result<
    (
        ResolvedQuery,
        crate::sql::analysis::cte::CTERegistry,
        crate::sql::column_id::ColumnRefFactory,
    ),
    String,
> {
    analyze_with_factory(
        query,
        catalog,
        current_database,
        crate::sql::column_id::ColumnRefFactory::new(),
    )
}

/// Like [`analyze`], but threads an existing [`ColumnRefFactory`] so that
/// ColumnIds allocated by this analysis never collide with ids the caller
/// already minted (used by MV rewrite candidate preparation, which analyzes
/// the MV defining SQL inside an already-planned user query).
pub(crate) fn analyze_with_factory(
    query: &sqlast::Query,
    catalog: &dyn CatalogProvider,
    current_database: &str,
    factory: crate::sql::column_id::ColumnRefFactory,
) -> Result<
    (
        ResolvedQuery,
        crate::sql::analysis::cte::CTERegistry,
        crate::sql::column_id::ColumnRefFactory,
    ),
    String,
> {
    let factory = std::rc::Rc::new(std::cell::RefCell::new(factory));
    let ctx = AnalyzerContext {
        catalog,
        current_database,
        factory: factory.clone(),
        ctes: std::collections::HashMap::new(),
        pending_ctes: std::collections::HashSet::new(),
        next_subquery_id: std::cell::Cell::new(0),
        next_lambda_slot_id: std::cell::Cell::new(0),
        collected_subqueries: std::cell::RefCell::new(Vec::new()),
        cte_registry: std::cell::RefCell::new(crate::sql::analysis::cte::CTERegistry::new()),
    };
    let resolved = ctx.analyze_query(query)?;
    let registry = ctx.cte_registry.into_inner();
    let col_factory = std::rc::Rc::try_unwrap(factory)
        .map(|cell| cell.into_inner())
        .unwrap_or_else(|rc| rc.borrow().clone());
    Ok((resolved, registry, col_factory))
}
```

（即原 `analyze()` 体整体移入 `analyze_with_factory`，仅 factory 来源改为参数。）

- [ ] **Step 4: 运行测试确认通过**

Run: `cargo test --lib sql::analyzer`
Expected: PASS。

- [ ] **Step 5: Commit**

```bash
git add src/sql/analyzer/mod.rs
git commit -m "feat(analyzer): add analyze_with_factory for ColumnId-threaded re-analysis"
```

---

### Task 3: SPJG 描述符与形状校验（`descriptor.rs`）

**Files:**
- Create: `src/sql/optimizer/cascades_rules/mv_rewrite/mod.rs`
- Create: `src/sql/optimizer/cascades_rules/mv_rewrite/descriptor.rs`
- Modify: `src/sql/optimizer/cascades_rules/mod.rs`（声明子模块）

描述符是 MV 侧（prep 时从 planner `LogicalPlan` 抽取）与查询侧（规则内从 memo 抽取，Task 8）的统一中间表示。

- [ ] **Step 1: 建模块骨架**

`src/sql/optimizer/cascades_rules/mod.rs` 加：

```rust
pub(crate) mod mv_rewrite;
```

`src/sql/optimizer/cascades_rules/mv_rewrite/mod.rs`：

```rust
//! Transparent MV query rewrite (single-table SPJG + aggregate rollup).
//!
//! Design spec: docs/design/specs/2026-06-10-mv-query-rewrite-design.md
//! StarRocks counterparts: MaterializedViewRewriter / AggregatedMaterializedViewRewriter.

pub(crate) mod aggregate_rollup;
pub(crate) mod column_mapping;
pub(crate) mod descriptor;
pub(crate) mod predicate_split;

use crate::sql::catalog::TableDef;
use descriptor::SpjgDescriptor;

pub(crate) const RULE_NAME: &str = "MvRewrite";

/// One usable MV candidate, fully prepared by the engine layer
/// (`src/engine/mv_rewrite_prep.rs`). Everything the optimizer rule needs;
/// no engine/catalog handles cross this boundary.
#[derive(Clone, Debug)]
pub(crate) struct MvRewriteCandidate {
    /// MV name, for logging and the EXPLAIN annotation.
    pub mv_name: String,
    /// SPJG decomposition of the MV defining query, expressed over the
    /// base table's ColumnIds (allocated in the shared ColumnRefFactory).
    pub mv: SpjgDescriptor,
    /// Database (namespace) of the MV target table, for LogicalScanOp.
    pub target_database: String,
    /// Executable TableDef of the MV target table
    /// (ScanSource::IcebergDataFiles, binding = CurrentSnapshot).
    pub target_table: TableDef,
}
```

（`aggregate_rollup`/`column_mapping`/`predicate_split` 先建空文件占位编译，各自任务填充；空文件只含模块注释。）

- [ ] **Step 2: 写失败的描述符抽取测试**

`descriptor.rs` 末尾 `#[cfg(test)] mod tests`。测试夹具构造 planner `LogicalPlan`（仿照 `src/sql/optimizer/logical_props.rs:322-362` 的 `lit`/`eq`/`output` helper 风格）：

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::DataType;
    use crate::sql::analysis::{ExprKind, LiteralValue, OutputColumn, TypedExpr};
    use crate::sql::catalog::{ColumnDef, ScanSource, TableDef};
    use crate::sql::column_id::ColumnId;
    use crate::sql::planner::plan::{
        AggregateCall, AggregateNode, FilterNode, LogicalPlan, ScanNode,
    };

    fn col(id: u32, name: &str) -> OutputColumn {
        OutputColumn {
            column_id: ColumnId(id),
            name: name.to_string(),
            data_type: DataType::Int64,
            nullable: true,
            is_internal: false,
        }
    }

    fn col_ref(c: &OutputColumn) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::ColumnRef {
                column_id: c.column_id,
                qualifier: None,
                column: c.name.clone(),
            },
            data_type: c.data_type.clone(),
            nullable: c.nullable,
        }
    }

    fn int_lit(v: i64) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::Literal(LiteralValue::Int(v)),
            data_type: DataType::Int64,
            nullable: false,
        }
    }

    fn cmp(left: TypedExpr, op: crate::sql::analysis::BinOp, right: TypedExpr) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::BinaryOp {
                left: Box::new(left),
                op,
                right: Box::new(right),
            },
            data_type: DataType::Boolean,
            nullable: true,
        }
    }

    // NOTE: build the test TableDef's ScanSource from whatever variant is
    // simplest to construct in unit tests. Check src/sql/catalog.rs for a
    // local/in-memory variant first; IcebergDataFiles requires
    // IcebergTableInfo + IcebergSchemaDef which existing tests construct —
    // grep `ScanSource::` in src/ tests for the established fixture and
    // reuse it. The descriptor logic itself never inspects ScanSource.
    fn scan(cols: &[OutputColumn]) -> ScanNode {
        ScanNode {
            database: "db".to_string(),
            table: TableDef {
                name: "t".to_string(),
                columns: cols
                    .iter()
                    .map(|c| ColumnDef {
                        name: c.name.clone(),
                        data_type: c.data_type.clone(),
                        nullable: c.nullable,
                        write_default: None,
                        logical_type: None,
                    })
                    .collect(),
                iceberg_row_lineage_metadata_columns: vec![],
                source: test_scan_source(),
            },
            alias: None,
            columns: cols.to_vec(),
            predicates: vec![],
            required_columns: None,
            dict_columns: vec![],
        }
    }

    #[test]
    fn extracts_filter_scan_shape() {
        let a = col(1, "a");
        let b = col(2, "b");
        let plan = LogicalPlan::Filter(FilterNode {
            input: Box::new(LogicalPlan::Scan(scan(&[a.clone(), b.clone()]))),
            predicate: cmp(col_ref(&a), crate::sql::analysis::BinOp::Ge, int_lit(5)),
            required_output_columns: None,
        });
        let d = SpjgDescriptor::from_logical_plan(&plan).expect("spjg");
        assert_eq!(d.table.name, "t");
        assert_eq!(d.predicates.len(), 1);
        assert!(d.aggregate.is_none());
        assert_eq!(d.outputs.len(), 2); // pass-through scan columns
    }

    #[test]
    fn extracts_aggregate_shape_and_rejects_join() {
        let a = col(1, "a");
        let v = col(2, "v");
        let sum_out = col(3, "s");
        let plan = LogicalPlan::Aggregate(AggregateNode {
            input: Box::new(LogicalPlan::Scan(scan(&[a.clone(), v.clone()]))),
            group_by: vec![col_ref(&a)],
            aggregates: vec![AggregateCall {
                name: "sum".to_string(),
                args: vec![col_ref(&v)],
                distinct: false,
                result_type: DataType::Int64,
                order_by: vec![],
                output_column_id: sum_out.column_id,
            }],
            output_columns: vec![col(1, "a"), sum_out.clone()],
            already_pushed: false,
            required_output_columns: None,
        });
        let d = SpjgDescriptor::from_logical_plan(&plan).expect("spjg");
        let agg = d.aggregate.as_ref().expect("aggregate present");
        assert_eq!(agg.group_by.len(), 1);
        assert_eq!(agg.aggregates.len(), 1);
        // outputs: Dimension(a) then Aggregate(sum(v))
        assert_eq!(d.outputs.len(), 2);
    }

    #[test]
    fn rejects_sort_and_window() {
        // Any node outside {Scan, Filter, Project, Aggregate} must yield Err.
        // Build Sort over Scan (see plan.rs SortNode fields) and assert
        // from_logical_plan returns Err.
        // (Construct SortNode with empty keys per its struct definition.)
    }
}
```

`rejects_sort_and_window` 按 `plan.rs` 中 `SortNode` 实际字段补全构造（核心断言：`from_logical_plan(...).is_err()`）。

- [ ] **Step 3: 运行测试确认失败**

Run: `cargo test --lib cascades_rules::mv_rewrite::descriptor`
Expected: 编译失败（`SpjgDescriptor` 未定义）。

- [ ] **Step 4: 实现 `descriptor.rs`**

```rust
//! SPJG (select-project-join-group-by, single-table subset) decomposition.
//!
//! Both sides of MV rewrite matching are normalized into this shape:
//! the MV defining plan (built at candidate-prep time from the planner
//! LogicalPlan) and the query subtree (rebuilt from memo MExprs by the rule).

use std::collections::HashMap;

use crate::sql::analysis::{ExprKind, OutputColumn, TypedExpr};
use crate::sql::catalog::TableDef;
use crate::sql::column_id::ColumnId;
use crate::sql::planner::plan::{AggregateCall, LogicalPlan};

/// One visible output of the SPJG subtree, in output order.
#[derive(Clone, Debug)]
pub(crate) struct SpjgOutput {
    pub name: String,
    /// The ColumnId this output is addressed by at the subtree top.
    pub column_id: ColumnId,
    pub expr: SpjgOutputExpr,
}

#[derive(Clone, Debug)]
pub(crate) enum SpjgOutputExpr {
    /// Expression over base-table columns (projection item or group key).
    Dimension(TypedExpr),
    /// Aggregate call over base-table columns.
    Aggregate(AggregateCall),
}

#[derive(Clone, Debug)]
pub(crate) struct SpjgAggregate {
    /// Group keys, composed down to base-table column expressions.
    pub group_by: Vec<TypedExpr>,
    /// Aggregate calls, args composed down to base-table columns.
    pub aggregates: Vec<AggregateCall>,
}

#[derive(Clone, Debug)]
pub(crate) struct SpjgDescriptor {
    pub database: String,
    pub table: TableDef,
    /// Scan output columns: ColumnId -> base column binding.
    pub scan_columns: Vec<OutputColumn>,
    /// All conjuncts below the aggregate (scan predicates + filter, CNF-split).
    pub predicates: Vec<TypedExpr>,
    pub aggregate: Option<SpjgAggregate>,
    /// Visible outputs in order (the subtree's output schema).
    pub outputs: Vec<SpjgOutput>,
}

impl SpjgDescriptor {
    /// Map from scan ColumnId to base column name (for cross-side matching:
    /// the two sides see the same physical table through different ids).
    pub(crate) fn base_name_of(&self) -> HashMap<ColumnId, String> {
        self.scan_columns
            .iter()
            .map(|c| (c.column_id, c.name.clone()))
            .collect()
    }

    pub(crate) fn from_logical_plan(plan: &LogicalPlan) -> Result<SpjgDescriptor, String> {
        // Accepted normal form, peeled top-down:
        //   [Project] -> [Aggregate] -> [Project] -> [Filter]* -> Scan
        // Anything else (Join/Sort/Limit/Window/Union/CTE/...) is rejected.
        let mut node = plan;

        // Optional top project (rebinding of aggregate/scan outputs).
        let top_project = match node {
            LogicalPlan::Project(p) => {
                node = &p.input;
                Some(p)
            }
            _ => None,
        };

        let aggregate = match node {
            LogicalPlan::Aggregate(a) => {
                node = &a.input;
                Some(a)
            }
            _ => None,
        };

        // Optional pre-aggregate project (planner may compute group-key /
        // agg-arg expressions in a project below the aggregate).
        let mid_project = match node {
            LogicalPlan::Project(p) => {
                node = &p.input;
                Some(p)
            }
            _ => None,
        };

        let mut predicates: Vec<TypedExpr> = Vec::new();
        while let LogicalPlan::Filter(f) = node {
            split_conjuncts(&f.predicate, &mut predicates);
            node = &f.input;
        }

        let LogicalPlan::Scan(scan) = node else {
            return Err(format!(
                "not a single-table SPJG shape: unexpected node {:?}",
                std::mem::discriminant(node)
            ));
        };
        predicates.extend(scan.predicates.iter().cloned());

        // Composition map: ColumnId -> defining expr over scan columns
        // (from the mid project). Identity for scan columns themselves.
        let mut defs: HashMap<ColumnId, TypedExpr> = HashMap::new();
        if let Some(p) = mid_project {
            for item in &p.items {
                let composed = substitute(&item.expr, &defs);
                defs.insert(item.output_column_id, composed);
            }
        }

        let compose = |e: &TypedExpr| substitute(e, &defs);

        let (agg, outputs) = match aggregate {
            Some(a) => {
                let group_by: Vec<TypedExpr> = a.group_by.iter().map(|e| compose(e)).collect();
                let aggregates: Vec<AggregateCall> = a
                    .aggregates
                    .iter()
                    .map(|c| AggregateCall {
                        args: c.args.iter().map(|e| compose(e)).collect(),
                        ..c.clone()
                    })
                    .collect();
                // Aggregate output convention: [group keys..., agg results...]
                if a.output_columns.len() != a.group_by.len() + a.aggregates.len() {
                    return Err(format!(
                        "aggregate output layout {} != group_by {} + aggs {}",
                        a.output_columns.len(),
                        a.group_by.len(),
                        a.aggregates.len()
                    ));
                }
                // Binding map at the aggregate's outputs.
                let mut agg_outputs: Vec<SpjgOutput> = Vec::new();
                for (i, oc) in a.output_columns.iter().enumerate() {
                    let expr = if i < a.group_by.len() {
                        SpjgOutputExpr::Dimension(group_by[i].clone())
                    } else {
                        SpjgOutputExpr::Aggregate(aggregates[i - a.group_by.len()].clone())
                    };
                    agg_outputs.push(SpjgOutput {
                        name: oc.name.clone(),
                        column_id: oc.column_id,
                        expr,
                    });
                }
                let outputs = apply_top_project(top_project, agg_outputs)?;
                (
                    Some(SpjgAggregate { group_by, aggregates }),
                    outputs,
                )
            }
            None => {
                let scan_outputs: Vec<SpjgOutput> = scan
                    .columns
                    .iter()
                    .map(|c| SpjgOutput {
                        name: c.name.clone(),
                        column_id: c.column_id,
                        expr: SpjgOutputExpr::Dimension(TypedExpr {
                            kind: ExprKind::ColumnRef {
                                column_id: c.column_id,
                                qualifier: None,
                                column: c.name.clone(),
                            },
                            data_type: c.data_type.clone(),
                            nullable: c.nullable,
                        }),
                    })
                    .collect();
                // mid_project without aggregate is just "the" project.
                let scan_outputs = match mid_project {
                    Some(p) => p
                        .items
                        .iter()
                        .map(|item| SpjgOutput {
                            name: item.output_name.clone(),
                            column_id: item.output_column_id,
                            expr: SpjgOutputExpr::Dimension(substitute(&item.expr, &defs)),
                        })
                        .collect(),
                    None => scan_outputs,
                };
                let outputs = apply_top_project(top_project, scan_outputs)?;
                (None, outputs)
            }
        };

        Ok(SpjgDescriptor {
            database: scan.database.clone(),
            table: scan.table.clone(),
            scan_columns: scan.columns.clone(),
            predicates,
            aggregate: agg,
            outputs,
        })
    }
}

/// Rebind outputs through an optional top project. MVP: top project items
/// must be bare ColumnRefs into the inputs (renames only); complex exprs
/// over aggregate results reject the shape.
fn apply_top_project(
    project: Option<&crate::sql::planner::plan::ProjectNode>,
    inputs: Vec<SpjgOutput>,
) -> Result<Vec<SpjgOutput>, String> {
    let Some(p) = project else {
        return Ok(inputs);
    };
    let by_id: HashMap<ColumnId, &SpjgOutput> =
        inputs.iter().map(|o| (o.column_id, o)).collect();
    p.items
        .iter()
        .map(|item| match &item.expr.kind {
            ExprKind::ColumnRef { column_id, .. } => by_id
                .get(column_id)
                .map(|o| SpjgOutput {
                    name: item.output_name.clone(),
                    column_id: item.output_column_id,
                    expr: o.expr.clone(),
                })
                .ok_or_else(|| "top project references unknown column".to_string()),
            // A computed top-project item over a pure-dimension input can be
            // composed; over aggregate outputs it is rejected (MVP).
            _ => {
                let mut defs: HashMap<ColumnId, TypedExpr> = HashMap::new();
                for o in &inputs {
                    match &o.expr {
                        SpjgOutputExpr::Dimension(e) => {
                            defs.insert(o.column_id, e.clone());
                        }
                        SpjgOutputExpr::Aggregate(_) => {}
                    }
                }
                let composed = substitute(&item.expr, &defs);
                if references_any(&composed, &inputs_agg_ids(&inputs)) {
                    Err("computed top-project over aggregate output (unsupported)".to_string())
                } else {
                    Ok(SpjgOutput {
                        name: item.output_name.clone(),
                        column_id: item.output_column_id,
                        expr: SpjgOutputExpr::Dimension(composed),
                    })
                }
            }
        })
        .collect()
}

fn inputs_agg_ids(inputs: &[SpjgOutput]) -> Vec<ColumnId> {
    inputs
        .iter()
        .filter(|o| matches!(o.expr, SpjgOutputExpr::Aggregate(_)))
        .map(|o| o.column_id)
        .collect()
}

fn references_any(e: &TypedExpr, ids: &[ColumnId]) -> bool {
    let mut found = false;
    walk(e, &mut |x| {
        if let ExprKind::ColumnRef { column_id, .. } = &x.kind {
            if ids.contains(column_id) {
                found = true;
            }
        }
    });
    found
}

/// Split a conjunction into CNF conjuncts.
pub(crate) fn split_conjuncts(e: &TypedExpr, out: &mut Vec<TypedExpr>) {
    if let ExprKind::BinaryOp {
        left,
        op: crate::sql::analysis::BinOp::And,
        right,
    } = &e.kind
    {
        split_conjuncts(left, out);
        split_conjuncts(right, out);
    } else {
        out.push(e.clone());
    }
}

/// Replace ColumnRefs by their defining exprs (identity when absent).
pub(crate) fn substitute(e: &TypedExpr, defs: &HashMap<ColumnId, TypedExpr>) -> TypedExpr {
    if let ExprKind::ColumnRef { column_id, .. } = &e.kind {
        if let Some(d) = defs.get(column_id) {
            return d.clone();
        }
    }
    map_children(e, &|child| substitute(child, defs))
}

/// Structural walk over all sub-expressions (pre-order).
pub(crate) fn walk(e: &TypedExpr, f: &mut impl FnMut(&TypedExpr)) {
    f(e);
    for_each_child(e, &mut |c| walk(c, f));
}
```

另需两个表达式遍历辅助 `for_each_child(e, f)` 与 `map_children(e, f)`：对 `ExprKind` 每个含子表达式的 variant（`BinaryOp`/`UnaryOp`/`FunctionCall`/`AggregateCall`/`Cast`/`IsNull`/`InList`/`Between`/`Like`/`Case`/`Nested`/`IsTruthValue`）做完整 match 实现遍历/重建；遇到 `WindowCall`/`SubqueryPlaceholder`/`Lambda*` 等不支持 variant 时 `map_children` 原样返回（后续 normalize 阶段会因不可规范化而安全拒绝）。先在 `descriptor.rs` 内实现，约 80 行机械 match（对照 `src/sql/analysis/mod.rs:303-411` 的全部 variant 逐一写出）。

- [ ] **Step 5: 运行测试确认通过**

Run: `cargo test --lib cascades_rules::mv_rewrite::descriptor`
Expected: PASS。

- [ ] **Step 6: Commit**

```bash
git add src/sql/optimizer/cascades_rules/
git commit -m "feat(optimizer): add SPJG descriptor extraction for MV rewrite"
```

---

### Task 4: 谓词三分类、区间蕴含与补偿（`predicate_split.rs`）

**Files:**
- Modify: `src/sql/optimizer/cascades_rules/mv_rewrite/predicate_split.rs`

- [ ] **Step 1: 写失败的单元测试**

```rust
#[cfg(test)]
mod tests {
    use super::*;
    // reuse the col/col_ref/int_lit/cmp helpers pattern from descriptor tests
    // (duplicate them here; unit-test helpers stay file-local by convention).

    #[test]
    fn equal_ranges_need_no_compensation() {
        // MV: a >= 5      query: a >= 5
        let n = names();
        let r = check_containment(&[ge_a(5)], &[ge_a(5)], &n, &n).expect("contained");
        assert!(r.compensation.is_empty());
    }

    #[test]
    fn tighter_query_range_compensates() {
        // MV: a >= 5      query: a >= 10  -> contained, compensation [a >= 10]
        let n = names();
        let r = check_containment(&[ge_a(10)], &[ge_a(5)], &n, &n).expect("contained");
        assert_eq!(r.compensation.len(), 1);
    }

    #[test]
    fn wider_query_range_fails() {
        // MV: a >= 10     query: a >= 5  -> NOT contained
        let n = names();
        assert!(check_containment(&[ge_a(5)], &[ge_a(10)], &n, &n).is_none());
    }

    #[test]
    fn mv_residual_must_appear_in_query() {
        let n = names();
        // MV: a LIKE 'x%'   query: (no like) -> fail
        assert!(check_containment(&[], &[like_a("x%")], &n, &n).is_none());
        // MV: a LIKE 'x%'   query: a LIKE 'x%' AND a >= 5 -> ok, comp [a >= 5]
        let r = check_containment(&[like_a("x%"), ge_a(5)], &[like_a("x%")], &n, &n)
            .expect("contained");
        assert_eq!(r.compensation.len(), 1);
    }

    #[test]
    fn ne_is_residual_not_range() {
        let n = names();
        // MV: a != 5    query: a > 5 -> must FAIL (no punctured-interval logic)
        assert!(check_containment(&[gt_a(5)], &[ne_a(5)], &n, &n).is_none());
        // exact match passes
        assert!(check_containment(&[ne_a(5)], &[ne_a(5)], &n, &n).is_some());
    }

    #[test]
    fn between_expands_to_range() {
        let n = names();
        // MV: a BETWEEN 0 AND 100   query: a BETWEEN 10 AND 20 -> contained
        let r = check_containment(&[between_a(10, 20)], &[between_a(0, 100)], &n, &n)
            .expect("contained");
        assert_eq!(r.compensation.len(), 1); // the tighter BETWEEN re-applied
    }

    #[test]
    fn incomparable_literals_fail_closed() {
        let n = names();
        // MV: a >= 5 (Int)   query: a >= 'x' (String) -> cannot compare -> fail
        assert!(check_containment(&[ge_a_str("x")], &[ge_a(5)], &n, &n).is_none());
    }
}
```

（helper `ge_a`/`gt_a`/`ne_a`/`like_a`/`between_a`/`names` 在测试模块内用 Task 3 同款构造写全。）

- [ ] **Step 2: 运行测试确认失败**

Run: `cargo test --lib cascades_rules::mv_rewrite::predicate_split`
Expected: 编译失败（`check_containment` 未定义）。

- [ ] **Step 3: 实现**

```rust
//! Predicate classification (equality/range vs residual), per-column
//! interval containment, and compensation computation.
//! StarRocks counterpart: PredicateSplit / RangePredicate.

use std::collections::HashMap;

use crate::sql::analysis::{BinOp, ExprKind, LiteralValue, TypedExpr};
use crate::sql::column_id::ColumnId;

use super::column_mapping::{normalize, NormExpr};

/// Inclusive/exclusive bound on one column.
#[derive(Clone, Debug, PartialEq)]
struct Bound {
    value: LiteralValue,
    inclusive: bool,
}

/// Conjunct-merged interval for one base column.
#[derive(Clone, Debug, Default, PartialEq)]
struct ColumnRange {
    low: Option<Bound>,
    high: Option<Bound>,
}

#[derive(Debug)]
pub(crate) struct ContainmentResult {
    /// Query conjuncts the MV does not already guarantee; to be re-applied
    /// as a Filter above the MV scan (in original TypedExpr form, still
    /// over base-table columns — the caller rewrites them to MV columns).
    pub compensation: Vec<TypedExpr>,
}

struct Classified {
    /// base column name -> (merged range, original conjuncts on the column)
    ranges: HashMap<String, (ColumnRange, Vec<TypedExpr>)>,
    /// normalized residual -> original conjunct
    residuals: Vec<(NormExpr, TypedExpr)>,
}

/// Classify conjuncts. Returns None when any conjunct cannot be classified
/// safely (e.g. un-normalizable residual) — fail closed.
fn classify(
    conjuncts: &[TypedExpr],
    base_names: &HashMap<ColumnId, String>,
) -> Option<Classified> {
    let mut ranges: HashMap<String, (ColumnRange, Vec<TypedExpr>)> = HashMap::new();
    let mut residuals = Vec::new();
    for c in conjuncts {
        match as_range_conjunct(c, base_names) {
            Some((col, low, high)) => {
                let entry = ranges.entry(col).or_default();
                if let Some(b) = low {
                    tighten_low(&mut entry.0, b)?;
                }
                if let Some(b) = high {
                    tighten_high(&mut entry.0, b)?;
                }
                entry.1.push(c.clone());
            }
            None => {
                let n = normalize(c, base_names)?;
                residuals.push((n, c.clone()));
            }
        }
    }
    Some(Classified { ranges, residuals })
}

/// `col op literal` / `literal op col` / BETWEEN -> (column, low?, high?).
/// op ∈ {<, <=, >, >=, =}. `!=`, IS NULL, IN, LIKE etc. are residuals.
fn as_range_conjunct(
    e: &TypedExpr,
    base_names: &HashMap<ColumnId, String>,
) -> Option<(String, Option<Bound>, Option<Bound>)> {
    let col_of = |x: &TypedExpr| -> Option<String> {
        if let ExprKind::ColumnRef { column_id, .. } = &x.kind {
            base_names.get(column_id).cloned()
        } else {
            None
        }
    };
    let lit_of = |x: &TypedExpr| -> Option<LiteralValue> {
        if let ExprKind::Literal(v) = &x.kind {
            Some(v.clone())
        } else {
            None
        }
    };
    match &e.kind {
        ExprKind::BinaryOp { left, op, right } => {
            let (col, lit, op) = if let (Some(c), Some(l)) = (col_of(left), lit_of(right)) {
                (c, l, *op)
            } else if let (Some(l), Some(c)) = (lit_of(left), col_of(right)) {
                // literal op col  ==  col flipped-op literal
                let flipped = match op {
                    BinOp::Lt => BinOp::Gt,
                    BinOp::Le => BinOp::Ge,
                    BinOp::Gt => BinOp::Lt,
                    BinOp::Ge => BinOp::Le,
                    BinOp::Eq => BinOp::Eq,
                    _ => return None,
                };
                (c, l, flipped)
            } else {
                return None;
            };
            match op {
                BinOp::Eq => Some((
                    col,
                    Some(Bound { value: lit.clone(), inclusive: true }),
                    Some(Bound { value: lit, inclusive: true }),
                )),
                BinOp::Ge => Some((col, Some(Bound { value: lit, inclusive: true }), None)),
                BinOp::Gt => Some((col, Some(Bound { value: lit, inclusive: false }), None)),
                BinOp::Le => Some((col, None, Some(Bound { value: lit, inclusive: true }))),
                BinOp::Lt => Some((col, None, Some(Bound { value: lit, inclusive: false }))),
                _ => None,
            }
        }
        ExprKind::Between { expr, low, high, negated: false } => {
            let col = col_of(expr)?;
            let lo = lit_of(low)?;
            let hi = lit_of(high)?;
            Some((
                col,
                Some(Bound { value: lo, inclusive: true }),
                Some(Bound { value: hi, inclusive: true }),
            ))
        }
        _ => None,
    }
}

/// Compare two literals of compatible kinds. None = incomparable (fail closed).
fn cmp_literal(a: &LiteralValue, b: &LiteralValue) -> Option<std::cmp::Ordering> {
    use LiteralValue::*;
    match (a, b) {
        (Int(x), Int(y)) => Some(x.cmp(y)),
        (LargeInt(x), LargeInt(y)) => Some(x.cmp(y)),
        (Int(x), LargeInt(y)) => Some(i128::from(*x).cmp(y)),
        (LargeInt(x), Int(y)) => Some(x.cmp(&i128::from(*y))),
        (Float(x), Float(y)) => x.partial_cmp(y),
        (Int(x), Float(y)) => (*x as f64).partial_cmp(y),
        (Float(x), Int(y)) => x.partial_cmp(&(*y as f64)),
        (String(x), String(y)) => Some(x.cmp(y)),
        (Bool(x), Bool(y)) => Some(x.cmp(y)),
        // Decimal / Null / mixed kinds: refuse to compare.
        _ => None,
    }
}

fn tighten_low(r: &mut ColumnRange, b: Bound) -> Option<()> {
    match &r.low {
        None => r.low = Some(b),
        Some(cur) => match cmp_literal(&b.value, &cur.value)? {
            std::cmp::Ordering::Greater => r.low = Some(b),
            std::cmp::Ordering::Equal if !b.inclusive => r.low = Some(b),
            _ => {}
        },
    }
    Some(())
}

fn tighten_high(r: &mut ColumnRange, b: Bound) -> Option<()> {
    match &r.high {
        None => r.high = Some(b),
        Some(cur) => match cmp_literal(&b.value, &cur.value)? {
            std::cmp::Ordering::Less => r.high = Some(b),
            std::cmp::Ordering::Equal if !b.inclusive => r.high = Some(b),
            _ => {}
        },
    }
    Some(())
}

/// query_low >= mv_low (with inclusivity)?  i.e. query interval starts inside MV's.
fn low_contained(query: &Option<Bound>, mv: &Option<Bound>) -> Option<bool> {
    match (query, mv) {
        (_, None) => Some(true),
        (None, Some(_)) => Some(false),
        (Some(q), Some(m)) => Some(match cmp_literal(&q.value, &m.value)? {
            std::cmp::Ordering::Greater => true,
            std::cmp::Ordering::Less => false,
            std::cmp::Ordering::Equal => m.inclusive || !q.inclusive,
        }),
    }
}

fn high_contained(query: &Option<Bound>, mv: &Option<Bound>) -> Option<bool> {
    match (query, mv) {
        (_, None) => Some(true),
        (None, Some(_)) => Some(false),
        (Some(q), Some(m)) => Some(match cmp_literal(&q.value, &m.value)? {
            std::cmp::Ordering::Less => true,
            std::cmp::Ordering::Greater => false,
            std::cmp::Ordering::Equal => m.inclusive || !q.inclusive,
        }),
    }
}

/// Core check: MV data ⊇ query data. Returns None when not contained (or
/// not provably contained). On success returns the compensation conjuncts.
pub(crate) fn check_containment(
    query_conjuncts: &[TypedExpr],
    mv_conjuncts: &[TypedExpr],
    // base ColumnId -> base column name maps for EACH side
    // (the two sides allocate different ColumnIds for the same table).
    query_base_names: &HashMap<ColumnId, String>,
    mv_base_names: &HashMap<ColumnId, String>,
) -> Option<ContainmentResult> {
    let q = classify(query_conjuncts, query_base_names)?;
    let m = classify(mv_conjuncts, mv_base_names)?;

    let mut compensation: Vec<TypedExpr> = Vec::new();

    // Ranges: every MV-constrained column must be at least as wide as the
    // query's. Query columns unconstrained by MV compensate fully.
    for (col, (mv_range, _)) in &m.ranges {
        let (q_range, _) = q.ranges.get(col)?; // MV constrains a column the query doesn't -> fail
        if !(low_contained(&q_range.low, &mv_range.low)?
            && high_contained(&q_range.high, &mv_range.high)?)
        {
            return None;
        }
    }
    for (col, (q_range, originals)) in &q.ranges {
        match m.ranges.get(col) {
            // Identical range: fully implied, no compensation.
            Some((mv_range, _)) if mv_range == q_range => {}
            // Wider MV range (already verified) or unconstrained: re-apply.
            _ => compensation.extend(originals.iter().cloned()),
        }
    }

    // Residuals: MV residual set ⊆ query residual set (by normalized form).
    let q_norms: Vec<&NormExpr> = q.residuals.iter().map(|(n, _)| n).collect();
    for (mn, _) in &m.residuals {
        if !q_norms.contains(&mn) {
            return None;
        }
    }
    // Query residuals not present in the MV compensate.
    let m_norms: Vec<&NormExpr> = m.residuals.iter().map(|(n, _)| n).collect();
    for (qn, orig) in &q.residuals {
        if !m_norms.contains(&qn) {
            compensation.push(orig.clone());
        }
    }

    Some(ContainmentResult { compensation })
}
```

- [ ] **Step 4: 运行测试确认通过**

Run: `cargo test --lib cascades_rules::mv_rewrite::predicate_split`
Expected: PASS。

- [ ] **Step 5: Commit**

```bash
git add src/sql/optimizer/cascades_rules/mv_rewrite/predicate_split.rs
git commit -m "feat(optimizer): predicate classification and interval containment for MV rewrite"
```

---

### Task 5: 表达式规范化与列映射（`column_mapping.rs`）

**Files:**
- Modify: `src/sql/optimizer/cascades_rules/mv_rewrite/column_mapping.rs`

- [ ] **Step 1: 写失败的单元测试**

```rust
#[cfg(test)]
mod tests {
    use super::*;
    // helpers as in Task 3/4 tests

    #[test]
    fn normalize_is_column_id_independent() {
        // a(id=1) + 1  on side A   vs   a(id=9) + 1  on side B  -> equal NormExpr
        // because both resolve ColumnRef through their own base-name maps.
    }

    #[test]
    fn normalize_sorts_commutative_args() {
        // a + b  ==  b + a ;  a < 5  ==  5 > a (comparison canonicalization)
    }

    #[test]
    fn rewrite_replaces_matched_subtrees() {
        // MV outputs: [d := date_col, s := a + b]
        // query expr: (a + b) * 2  -> rewritten to  mv_s * 2
    }

    #[test]
    fn rewrite_fails_on_unmapped_leaf() {
        // query expr references base column c not derivable from MV outputs -> None
    }
}
```

各测试体按注释语义用 Task 3 的表达式构造 helper 写全断言（构造两侧 names map、调用 `normalize` / `rewrite_to_mv_columns` 并断言相等/替换/None）。

- [ ] **Step 2: 运行测试确认失败**

Run: `cargo test --lib cascades_rules::mv_rewrite::column_mapping`
Expected: 编译失败。

- [ ] **Step 3: 实现**

```rust
//! Expression normalization (ColumnId-independent comparable form) and
//! query-expression rewriting onto MV output columns.
//! StarRocks counterpart: EquationRewriter / ColumnRewriter (single-table cut).

use std::collections::HashMap;

use crate::sql::analysis::{BinOp, ExprKind, OutputColumn, TypedExpr, UnOp};
use crate::sql::column_id::ColumnId;

/// Canonical, ColumnId-independent expression form. Two exprs over the same
/// base table (through different ColumnId spaces) compare equal iff they are
/// structurally identical after base-name resolution.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) enum NormExpr {
    Column(String),
    Literal(String),
    Call {
        name: String,
        distinct: bool,
        args: Vec<NormExpr>,
    },
}

/// Returns None for unsupported expression kinds (window calls, subqueries,
/// lambdas) — callers must treat None as "cannot match" (fail closed).
pub(crate) fn normalize(
    e: &TypedExpr,
    base_names: &HashMap<ColumnId, String>,
) -> Option<NormExpr> {
    let call = |name: &str, args: Vec<NormExpr>| NormExpr::Call {
        name: name.to_string(),
        distinct: false,
        args,
    };
    Some(match &e.kind {
        ExprKind::ColumnRef { column_id, .. } => {
            NormExpr::Column(base_names.get(column_id)?.clone())
        }
        ExprKind::Literal(v) => NormExpr::Literal(format!("{v:?}")),
        ExprKind::BinaryOp { left, op, right } => {
            let mut l = normalize(left, base_names)?;
            let mut r = normalize(right, base_names)?;
            // Canonicalize comparisons: Gt/Ge become flipped Lt/Le.
            let (name, commutative) = match op {
                BinOp::Add => ("add", true),
                BinOp::Mul => ("mul", true),
                BinOp::Sub => ("sub", false),
                BinOp::Div => ("div", false),
                BinOp::Mod => ("mod", false),
                BinOp::Eq => ("eq", true),
                BinOp::Ne => ("ne", true),
                BinOp::EqForNull => ("eq_for_null", true),
                BinOp::And => ("and", true),
                BinOp::Or => ("or", true),
                BinOp::Lt => ("lt", false),
                BinOp::Le => ("le", false),
                BinOp::Gt => {
                    std::mem::swap(&mut l, &mut r);
                    ("lt", false)
                }
                BinOp::Ge => {
                    std::mem::swap(&mut l, &mut r);
                    ("le", false)
                }
            };
            let mut args = vec![l, r];
            if commutative {
                args.sort_by(|a, b| format!("{a:?}").cmp(&format!("{b:?}")));
            }
            call(name, args)
        }
        ExprKind::UnaryOp { op, expr } => {
            let name = match op {
                UnOp::Not => "not",
                UnOp::Neg => "neg",
            };
            call(name, vec![normalize(expr, base_names)?])
        }
        ExprKind::FunctionCall { name, args, distinct } => NormExpr::Call {
            name: format!("fn:{}", name.to_ascii_lowercase()),
            distinct: *distinct,
            args: args
                .iter()
                .map(|a| normalize(a, base_names))
                .collect::<Option<Vec<_>>>()?,
        },
        ExprKind::AggregateCall { name, args, distinct, .. } => NormExpr::Call {
            name: format!("agg:{}", name.to_ascii_lowercase()),
            distinct: *distinct,
            args: args
                .iter()
                .map(|a| normalize(a, base_names))
                .collect::<Option<Vec<_>>>()?,
        },
        ExprKind::Cast { expr, target } => call(
            &format!("cast:{target:?}"),
            vec![normalize(expr, base_names)?],
        ),
        ExprKind::IsNull { expr, negated } => call(
            if *negated { "is_not_null" } else { "is_null" },
            vec![normalize(expr, base_names)?],
        ),
        ExprKind::InList { expr, list, negated } => {
            let mut args = vec![normalize(expr, base_names)?];
            let mut items = list
                .iter()
                .map(|x| normalize(x, base_names))
                .collect::<Option<Vec<_>>>()?;
            items.sort_by(|a, b| format!("{a:?}").cmp(&format!("{b:?}")));
            args.extend(items);
            call(if *negated { "not_in" } else { "in" }, args)
        }
        ExprKind::Between { expr, low, high, negated } => call(
            if *negated { "not_between" } else { "between" },
            vec![
                normalize(expr, base_names)?,
                normalize(low, base_names)?,
                normalize(high, base_names)?,
            ],
        ),
        ExprKind::Like { expr, pattern, negated } => call(
            if *negated { "not_like" } else { "like" },
            vec![
                normalize(expr, base_names)?,
                normalize(pattern, base_names)?,
            ],
        ),
        ExprKind::Nested(inner) => return normalize(inner, base_names),
        // Case / IsTruthValue: normalizable but rare in MV predicates — add
        // straightforward Call encodings; everything else fails closed.
        _ => return None,
    })
}

/// Rewrite table: normalized MV dimension expr -> MV-scan column ref.
pub(crate) struct MvColumnMap {
    by_norm: HashMap<NormExpr, OutputColumn>,
}

impl MvColumnMap {
    /// `dims`: (normalized MV dimension expr, the MV-scan output column that
    /// materializes it). Built by the rule from candidate outputs + the new
    /// MV-scan column ids.
    pub(crate) fn new(dims: Vec<(NormExpr, OutputColumn)>) -> Self {
        Self { by_norm: dims.into_iter().collect() }
    }

    /// Rewrite a query-side expression so that every subtree matching an MV
    /// dimension becomes a ColumnRef to the MV scan column. Returns None if
    /// any base-table leaf remains unmapped.
    pub(crate) fn rewrite(
        &self,
        e: &TypedExpr,
        query_base_names: &HashMap<ColumnId, String>,
    ) -> Option<TypedExpr> {
        if let Some(n) = normalize(e, query_base_names) {
            if let Some(col) = self.by_norm.get(&n) {
                return Some(TypedExpr {
                    kind: ExprKind::ColumnRef {
                        column_id: col.column_id,
                        qualifier: None,
                        column: col.name.clone(),
                    },
                    data_type: col.data_type.clone(),
                    nullable: col.nullable,
                });
            }
        }
        // Not a whole-tree match: recurse; a remaining bare base ColumnRef
        // means the MV does not materialize this column -> fail.
        match &e.kind {
            ExprKind::ColumnRef { .. } => None,
            ExprKind::Literal(_) => Some(e.clone()),
            _ => super::descriptor::try_map_children(e, &mut |c| {
                self.rewrite(c, query_base_names)
            }),
        }
    }
}
```

需要在 `descriptor.rs` 增加 `try_map_children(e, f) -> Option<TypedExpr>`（与 `map_children` 同构，f 返回 `Option`，任一子失败整体 None；同样逐 variant match）。

- [ ] **Step 4: 运行测试确认通过**

Run: `cargo test --lib cascades_rules::mv_rewrite::column_mapping`
Expected: PASS。

- [ ] **Step 5: Commit**

```bash
git add src/sql/optimizer/cascades_rules/mv_rewrite/
git commit -m "feat(optimizer): expression normalization and MV column mapping"
```

---

### Task 6: 聚合上卷判定（`aggregate_rollup.rs`）

**Files:**
- Modify: `src/sql/optimizer/cascades_rules/mv_rewrite/aggregate_rollup.rs`

- [ ] **Step 1: 写失败的单元测试**

覆盖：白名单逐项（sum/min/max/count(*)/count(e)）、group-by 相等直接映射、子集上卷、拒绝矩阵（DISTINCT、AVG、白名单外、MV 缺对应物化列）、标量聚合 COUNT 需 COALESCE 标记：

```rust
#[test]
fn rollup_plan_for_groupby_subset() {
    // MV: GROUP BY a, b -> [a, b, sum(v) as s, count(*) as c]
    // query: SELECT a, sum(v), count(*) GROUP BY a
    // expect: RollupPlan { kind: Rollup, items: [Sum(over mv s), SumOfCount(over mv c)] }
}

#[test]
fn direct_mapping_when_groupby_equal() { /* kind: Direct, agg outputs map 1:1 */ }

#[test]
fn distinct_agg_rejected() { /* count(distinct x) vs SPJG MV -> None */ }

#[test]
fn avg_rejected_for_rollup_but_direct_ok() { /* avg in whitelist-miss for subset; equal-group-by direct match ok when MV has same avg call */ }

#[test]
fn scalar_count_flags_coalesce() {
    // query group-by empty -> RollupItem for COUNT carries needs_coalesce=true
}
```

各测试体用前述 helper 风格构造 `SpjgAggregate`/查询侧 group_by + `AggregateCall` 并断言 `plan_rollup` 返回结构。

- [ ] **Step 2: 运行测试确认失败**

Run: `cargo test --lib cascades_rules::mv_rewrite::aggregate_rollup`
Expected: 编译失败。

- [ ] **Step 3: 实现**

```rust
//! Aggregate rollup decision for SPJG-MV rewrites.
//! StarRocks counterpart: AggregatedMaterializedViewRewriter +
//! AggregateFunctionRollupUtils.

use std::collections::HashMap;

use crate::sql::analysis::TypedExpr;
use crate::sql::column_id::ColumnId;
use crate::sql::planner::plan::AggregateCall;

use super::column_mapping::{normalize, NormExpr};
use super::descriptor::{SpjgAggregate, SpjgOutput, SpjgOutputExpr};

#[derive(Debug)]
pub(crate) enum RollupKind {
    /// Query group-by == MV group-by: each query aggregate maps 1:1 to an
    /// MV output column, no re-aggregation.
    Direct,
    /// Query group-by ⊂ MV group-by: re-aggregate MV rows.
    Rollup,
}

#[derive(Debug)]
pub(crate) struct RollupItem {
    /// Index into the MV outputs (the materialized aggregate column to read).
    pub mv_output_index: usize,
    /// Rollup function name ("sum"/"min"/"max"); for Direct this is unused.
    pub rollup_fn: &'static str,
    /// True when the query aggregate is COUNT-like and the query has no
    /// group-by: SUM over an empty input yields NULL where COUNT must
    /// yield 0, so the result needs COALESCE(_, 0).
    pub needs_coalesce: bool,
}

#[derive(Debug)]
pub(crate) struct RollupPlan {
    pub kind: RollupKind,
    /// One entry per query aggregate, in order.
    pub items: Vec<RollupItem>,
}

fn norm_agg(
    call: &AggregateCall,
    base_names: &HashMap<ColumnId, String>,
) -> Option<NormExpr> {
    Some(NormExpr::Call {
        name: format!("agg:{}", call.name.to_ascii_lowercase()),
        distinct: call.distinct,
        args: call
            .args
            .iter()
            .map(|a| normalize(a, base_names))
            .collect::<Option<Vec<_>>>()?,
    })
}

/// Decide whether (and how) the query aggregate can be answered from the MV.
/// Returns None when not rewritable.
pub(crate) fn plan_rollup(
    query_group_by: &[TypedExpr],
    query_aggregates: &[AggregateCall],
    query_base_names: &HashMap<ColumnId, String>,
    mv_agg: &SpjgAggregate,
    mv_outputs: &[SpjgOutput],
    mv_base_names: &HashMap<ColumnId, String>,
) -> Option<RollupPlan> {
    // Normalized group-key sets.
    let q_keys: Vec<NormExpr> = query_group_by
        .iter()
        .map(|e| normalize(e, query_base_names))
        .collect::<Option<Vec<_>>>()?;
    let m_keys: Vec<NormExpr> = mv_agg
        .group_by
        .iter()
        .map(|e| normalize(e, mv_base_names))
        .collect::<Option<Vec<_>>>()?;
    if !q_keys.iter().all(|k| m_keys.contains(k)) {
        return None; // query groups by something the MV did not preserve
    }
    let equal = q_keys.len() == m_keys.len() && m_keys.iter().all(|k| q_keys.contains(k));

    // MV aggregate outputs by normalized call.
    let mut mv_agg_by_norm: HashMap<NormExpr, usize> = HashMap::new();
    for (i, out) in mv_outputs.iter().enumerate() {
        if let SpjgOutputExpr::Aggregate(call) = &out.expr {
            if let Some(n) = norm_agg(call, mv_base_names) {
                mv_agg_by_norm.insert(n, i);
            }
        }
    }

    let scalar_query = query_group_by.is_empty();
    let mut items = Vec::with_capacity(query_aggregates.len());
    for q in query_aggregates {
        if q.distinct {
            return None; // DISTINCT aggregates never rewrite onto SPJG MVs
        }
        let qn = norm_agg(q, query_base_names)?;
        let mv_idx = *mv_agg_by_norm.get(&qn)?; // exact same call materialized?
        if equal {
            items.push(RollupItem { mv_output_index: mv_idx, rollup_fn: "", needs_coalesce: false });
            continue;
        }
        // Rollup whitelist.
        let (rollup_fn, is_count) = match q.name.to_ascii_lowercase().as_str() {
            "sum" => ("sum", false),
            "min" => ("min", false),
            "max" => ("max", false),
            "count" => ("sum", true),
            _ => return None, // includes avg and everything exotic
        };
        items.push(RollupItem {
            mv_output_index: mv_idx,
            rollup_fn,
            needs_coalesce: is_count && scalar_query,
        });
    }

    Some(RollupPlan {
        kind: if equal { RollupKind::Direct } else { RollupKind::Rollup },
        items,
    })
}
```

- [ ] **Step 4: 运行测试确认通过**

Run: `cargo test --lib cascades_rules::mv_rewrite::aggregate_rollup`
Expected: PASS。

- [ ] **Step 5: Commit**

```bash
git add src/sql/optimizer/cascades_rules/mv_rewrite/aggregate_rollup.rs
git commit -m "feat(optimizer): aggregate rollup planning for MV rewrite"
```

---

### Task 7: scan 注记字段 `mv_rewritten_from` + EXPLAIN 输出

**Files:**
- Modify: `src/sql/optimizer/operator.rs`（`LogicalScanOp` ~:74、`PhysicalScanOp` ~:287）
- Modify: `src/sql/optimizer/convert.rs:17-35`（`LogicalPlan::Scan` 转换臂）
- Modify: `src/sql/optimizer/cascades_rules/implement.rs`（`ScanToPhysical`）
- Modify: `src/sql/explain.rs:433-474`（SCAN 输出臂）
- Modify: 编译器报出的所有其他 `LogicalScanOp`/`PhysicalScanOp` 结构体字面量构造点（补 `mv_rewritten_from: None`）

- [ ] **Step 1: 加字段**

`LogicalScanOp` 与 `PhysicalScanOp` 各加：

```rust
/// When this scan was injected by the MvRewrite rule, the source MV name
/// (shown in EXPLAIN as `rewritten with mv: <name>`). None for all
/// user-written scans.
pub mv_rewritten_from: Option<String>,
```

- [ ] **Step 2: `cargo build` 修复所有构造点**

Run: `cargo build 2>&1 | grep -c "missing field"`，对报出的每个构造点补 `mv_rewritten_from: None`；`ScanToPhysical` 处改为 `mv_rewritten_from: op.mv_rewritten_from.clone()`（logical → physical 透传）；`convert.rs` 的 `LogicalPlan::Scan` 臂补 `mv_rewritten_from: None`（planner ScanNode 无此概念）。

- [ ] **Step 3: EXPLAIN 输出**

`src/sql/explain.rs` SCAN 臂（`TABLE:` 行之后）加：

```rust
if let Some(ref mv) = op.mv_rewritten_from {
    out.push(format!("{pad}     rewritten with mv: {mv}"));
}
```

- [ ] **Step 4: 构建 + 既有测试**

Run: `cargo build && cargo test --lib sql::optimizer`
Expected: 全部 PASS（行为无变化，字段恒 None）。

- [ ] **Step 5: Commit**

```bash
git add src/sql/optimizer/ src/sql/explain.rs
git commit -m "feat(optimizer): carry mv_rewritten_from annotation on scan operators"
```

---

### Task 8: `MvRewriteRule`（memo 侧匹配 + 替代表达式注入）+ `optimize()` 接线

**Files:**
- Create: `src/sql/optimizer/cascades_rules/mv_rewrite/rule.rs`
- Modify: `src/sql/optimizer/cascades_rules/mv_rewrite/mod.rs`（导出 rule）
- Modify: `src/sql/optimizer/cascades_rules/mv_rewrite/descriptor.rs`（加 `from_memo`）
- Modify: `src/sql/optimizer/mod.rs`（`optimize()` 第 5 参 + 规则追加 + `is_known_rule_name`）
- Modify: `src/engine/mod.rs` 4 处 `optimize(...)` 调用点 + `src/sql/optimizer/mod.rs` 测试中的调用点（追加 `Vec::new()`）

- [ ] **Step 1: 描述符 memo 侧抽取**

`descriptor.rs` 增加（与 `from_logical_plan` 同构，但走 `Operator` + 子 group 首个逻辑表达式）：

```rust
use crate::sql::optimizer::memo::{MExpr, Memo};
use crate::sql::optimizer::operator::Operator;

/// What the alternative must reproduce at the matched group's top.
#[derive(Clone, Debug)]
pub(crate) enum MatchedShape {
    /// Top is the scan itself or Filter(Scan): outputs are scan columns.
    Spj,
    /// Top is LogicalAggregate: original op cloned for output reuse.
    Spjg {
        original_agg: crate::sql::optimizer::operator::LogicalAggregateOp,
    },
}

impl SpjgDescriptor {
    /// Rebuild the SPJG view of the subtree rooted at `expr`. Follows the
    /// FIRST logical expression of each child group (the original shape;
    /// alternatives injected later in the group do not affect extraction).
    /// Returns None for any non-SPJG operator in the chain.
    pub(crate) fn from_memo(expr: &MExpr, memo: &Memo) -> Option<(SpjgDescriptor, MatchedShape)> {
        // Walk: Aggregate -> [Project] -> [Filter]* -> Scan, or [Filter]* -> Scan.
        // Reuses the same composition rules as from_logical_plan, but over
        // Operator variants. Implementation mirrors from_logical_plan arm by
        // arm (LogicalAggregateOp.group_by/aggregates use the planner
        // AggregateCall type already).
        ...
    }
}
```

实现要点（逐臂写出，无新逻辑，只是 `Operator` 版本的 `from_logical_plan`）：
- `Operator::LogicalAggregate(a)`：仅接受 `a.stage == AggStage::Single && !a.is_split`（拆分后的 Local/Global 形不匹配——MvRewrite 在 explore 与 SplitAggregateRule 同轮运行，原始 Single 形恒在 group 首位，见 `convert.rs` Aggregate 臂）；
- 子 group 取 `memo.groups[child].logical_exprs.first()?`；
- `MatchedShape::Spjg` 保存原 `LogicalAggregateOp` 整体 clone（输出 ColumnId 复用之源）；
- Scan 臂拒绝 `mv_rewritten_from.is_some()` 的 scan（防 MV-on-MV 自反）。

单测（`descriptor.rs` tests 内）：构造 `LogicalPlan`，`convert::logical_plan_to_memo` 进 memo，对 root group 首表达式调 `from_memo`，断言与 `from_logical_plan` 等价的字段。

- [ ] **Step 2: 写失败的规则级测试**

`rule.rs` 的 tests：构造 base 表 `t(a,b,v)` 的查询计划 `Aggregate(group_by=[a], sum(v))(Filter(a>=10)(Scan t))` → memo；构造候选：MV 定义 `SELECT a, b, sum(v) s FROM t WHERE a >= 0 GROUP BY a, b`（手工搭 `SpjgDescriptor`，base ColumnIds 用独立编号段模拟共享 factory），target_table 为两列 `(a, b, s)` 的 TableDef。断言：

```rust
#[test]
fn injects_rollup_alternative() {
    // ... build memo + candidate ...
    let rule = MvRewriteRule::new(vec![candidate]);
    let root_expr = memo.groups[root].logical_exprs[0].clone();
    let alts = rule.apply(&root_expr, &mut memo);
    assert_eq!(alts.len(), 1);
    let Operator::LogicalAggregate(agg) = &alts[0].op else { panic!("rollup agg") };
    // output ColumnIds must be the ORIGINAL aggregate's
    assert_eq!(agg.output_columns[0].column_id, original_group_key_id);
    // child chain: [compensation Filter] -> Scan(target_table)
    let child = &memo.groups[alts[0].children[0]];
    // ... walk down to scan, assert table name == target and
    //     mv_rewritten_from == Some("mv1") and predicates rewritten ...
    // idempotency: second apply returns nothing
    assert!(rule.apply(&root_expr, &mut memo).is_empty());
}

#[test]
fn no_injection_when_predicate_not_contained() { /* MV WHERE a >= 100 -> empty */ }

#[test]
fn spj_query_on_spj_mv_injects_project() { /* top NewExpr is LogicalProject binding original ids */ }
```

- [ ] **Step 3: 运行测试确认失败**

Run: `cargo test --lib cascades_rules::mv_rewrite::rule`
Expected: 编译失败。

- [ ] **Step 4: 实现 `rule.rs`**

```rust
//! The MvRewrite Cascades transformation rule.

use std::collections::HashMap;
use std::sync::Mutex;

use crate::sql::analysis::{ExprKind, OutputColumn, ProjectItem, TypedExpr};
use crate::sql::analysis::LiteralValue;
use crate::sql::column_id::ColumnId;
use crate::sql::optimizer::memo::{MExpr, MExprId, Memo};
use crate::sql::optimizer::operator::{
    LogicalAggregateOp, LogicalFilterOp, LogicalProjectOp, LogicalScanOp, Operator,
};
use crate::sql::optimizer::rule::{NewExpr, Rule, RuleType};
use crate::sql::planner::plan::AggregateCall;

use super::aggregate_rollup::{plan_rollup, RollupKind};
use super::column_mapping::{normalize, MvColumnMap};
use super::descriptor::{MatchedShape, SpjgDescriptor, SpjgOutputExpr};
use super::predicate_split::check_containment;
use super::{MvRewriteCandidate, RULE_NAME};

pub(crate) struct MvRewriteRule {
    candidates: Vec<MvRewriteCandidate>,
    /// (matched MExpr id, candidate index) pairs already attempted. The
    /// explore loop re-visits expressions every round; without this guard
    /// each round would mint fresh child groups forever.
    applied: Mutex<std::collections::HashSet<(MExprId, usize)>>,
}

impl MvRewriteRule {
    pub(crate) fn new(candidates: Vec<MvRewriteCandidate>) -> Self {
        Self { candidates, applied: Mutex::new(Default::default()) }
    }
}

impl Rule for MvRewriteRule {
    fn name(&self) -> &str {
        RULE_NAME
    }
    fn rule_type(&self) -> RuleType {
        RuleType::Transformation
    }
    fn matches(&self, op: &Operator) -> bool {
        matches!(
            op,
            Operator::LogicalAggregate(_)
                | Operator::LogicalFilter(_)
                | Operator::LogicalScan(_)
        )
    }

    fn apply(&self, expr: &MExpr, memo: &mut Memo) -> Vec<NewExpr> {
        let Some((query, shape)) = SpjgDescriptor::from_memo(expr, memo) else {
            return vec![];
        };
        let mut out = Vec::new();
        for (idx, cand) in self.candidates.iter().enumerate() {
            {
                let mut applied = self.applied.lock().expect("mv rewrite applied set");
                if !applied.insert((expr.id, idx)) {
                    continue;
                }
            }
            if let Some(alt) = try_rewrite(&query, &shape, cand, memo) {
                out.push(alt);
            }
        }
        out
    }
}

fn try_rewrite(
    query: &SpjgDescriptor,
    shape: &MatchedShape,
    cand: &MvRewriteCandidate,
    memo: &mut Memo,
) -> Option<NewExpr> {
    // 1. Same physical base table (compare Iceberg identity, not names).
    if !same_iceberg_table(&query.table, &cand.mv.table) {
        return None;
    }
    let q_names = query.base_name_of();
    let m_names = cand.mv.base_name_of();

    // 2. Predicate containment + compensation (still over base columns).
    let containment =
        check_containment(&query.predicates, &cand.mv.predicates, &q_names, &m_names)?;

    // 3. Allocate the MV scan: one new ColumnId per MV visible output,
    //    bound by NAME to the target table columns.
    let mut scan_columns: Vec<OutputColumn> = Vec::new();
    let mut dims: Vec<(super::column_mapping::NormExpr, OutputColumn)> = Vec::new();
    let mut agg_cols: Vec<Option<OutputColumn>> = vec![None; cand.mv.outputs.len()];
    for (i, mv_out) in cand.mv.outputs.iter().enumerate() {
        let col_def = cand
            .target_table
            .columns
            .iter()
            .find(|c| c.name == mv_out.name)?; // visible-by-name mapping (spec §5)
        let id = memo.factory.create(
            Some(cand.target_table.name.clone()),
            col_def.name.clone(),
            col_def.data_type.clone(),
            col_def.nullable,
        );
        let oc = OutputColumn {
            column_id: id,
            name: col_def.name.clone(),
            data_type: col_def.data_type.clone(),
            nullable: col_def.nullable,
            is_internal: false,
        };
        scan_columns.push(oc.clone());
        match &mv_out.expr {
            SpjgOutputExpr::Dimension(e) => {
                dims.push((normalize(e, &m_names)?, oc));
            }
            SpjgOutputExpr::Aggregate(_) => agg_cols[i] = Some(oc),
        }
    }
    let col_map = MvColumnMap::new(dims);

    // 4. Compensation predicates rewritten onto MV columns. For SPJG MVs
    //    they may only land on group-key columns (spec §6.3): aggregate
    //    columns are not row-filterable. MvColumnMap only contains
    //    Dimension outputs, so any compensation touching an aggregate
    //    column simply fails to rewrite -> candidate dropped. 
    let compensation: Vec<TypedExpr> = containment
        .compensation
        .iter()
        .map(|p| col_map.rewrite(p, &q_names))
        .collect::<Option<Vec<_>>>()?;

    // 5. Build the operator chain bottom-up.
    let scan_group = memo.new_group(MExpr {
        id: memo.next_expr_id(),
        op: Operator::LogicalScan(LogicalScanOp {
            database: cand.target_database.clone(),
            table: cand.target_table.clone(),
            alias: None,
            columns: scan_columns,
            predicates: vec![],
            required_columns: None,
            dict_columns: vec![],
            mv_rewritten_from: Some(cand.mv_name.clone()),
        }),
        children: vec![],
    });
    let mut child_group = scan_group;
    if !compensation.is_empty() {
        let predicate = combine_and(compensation);
        child_group = memo.new_group(MExpr {
            id: memo.next_expr_id(),
            op: Operator::LogicalFilter(LogicalFilterOp { predicate }),
            children: vec![scan_group],
        });
    }

    // 6. Top operator: reproduce the matched group's output ColumnIds.
    match (shape, &cand.mv.aggregate) {
        // SPJ query on SPJ MV: Project binding original output ids.
        (MatchedShape::Spj, None) => {
            let items = query
                .outputs
                .iter()
                .map(|o| {
                    let SpjgOutputExpr::Dimension(e) = &o.expr else { return None };
                    Some(ProjectItem {
                        expr: col_map.rewrite(e, &q_names)?,
                        output_name: o.name.clone(),
                        output_column_id: o.column_id,
                    })
                })
                .collect::<Option<Vec<_>>>()?;
            Some(NewExpr {
                op: Operator::LogicalProject(LogicalProjectOp {
                    items,
                    output_qualifier: None,
                }),
                children: vec![child_group],
            })
        }
        // SPJ query cannot read an aggregated MV (detail rows are gone).
        (MatchedShape::Spj, Some(_)) => None,
        // SPJG query on SPJ MV: keep the query aggregate, args rewritten.
        (MatchedShape::Spjg { original_agg }, None) => {
            let group_by = original_agg
                .group_by
                .iter()
                .map(|e| col_map.rewrite(e, &q_names))
                .collect::<Option<Vec<_>>>()?;
            let aggregates = original_agg
                .aggregates
                .iter()
                .map(|c| {
                    Some(AggregateCall {
                        args: c
                            .args
                            .iter()
                            .map(|a| col_map.rewrite(a, &q_names))
                            .collect::<Option<Vec<_>>>()?,
                        ..c.clone()
                    })
                })
                .collect::<Option<Vec<_>>>()?;
            if original_agg.aggregates.iter().any(|c| c.distinct) {
                // DISTINCT over an SPJ MV is sound (detail rows preserved),
                // args were rewritten like any other.
            }
            Some(NewExpr {
                op: Operator::LogicalAggregate(LogicalAggregateOp::single(
                    group_by,
                    aggregates,
                    original_agg.output_columns.clone(),
                )),
                children: vec![child_group],
            })
        }
        // SPJG query on SPJG MV: direct mapping or rollup.
        (MatchedShape::Spjg { original_agg }, Some(mv_agg)) => {
            let plan = plan_rollup(
                &original_agg.group_by,
                &original_agg.aggregates,
                &q_names,
                mv_agg,
                &cand.mv.outputs,
                &m_names,
            )?;
            let n_keys = original_agg.group_by.len();
            match plan.kind {
                RollupKind::Direct => {
                    // One row per group already: Project binding the original
                    // output ids (group keys then agg results).
                    let mut items: Vec<ProjectItem> = Vec::new();
                    for (i, oc) in original_agg.output_columns.iter().enumerate() {
                        let expr = if i < n_keys {
                            col_map.rewrite(&original_agg.group_by[i], &q_names)?
                        } else {
                            let item = &plan.items[i - n_keys];
                            let mv_col = agg_cols[item.mv_output_index].clone()?;
                            column_ref(&mv_col)
                        };
                        items.push(ProjectItem {
                            expr,
                            output_name: oc.name.clone(),
                            output_column_id: oc.column_id,
                        });
                    }
                    Some(NewExpr {
                        op: Operator::LogicalProject(LogicalProjectOp {
                            items,
                            output_qualifier: None,
                        }),
                        children: vec![child_group],
                    })
                }
                RollupKind::Rollup => {
                    let group_by = original_agg
                        .group_by
                        .iter()
                        .map(|e| col_map.rewrite(e, &q_names))
                        .collect::<Option<Vec<_>>>()?;
                    let needs_coalesce = plan.items.iter().any(|i| i.needs_coalesce);
                    // Aggregate outputs: reuse original ids directly unless a
                    // COALESCE wrapper project is needed (then mint fresh ids
                    // for the aggregate and bind originals in the project).
                    let mut agg_outputs = original_agg.output_columns.clone();
                    if needs_coalesce {
                        for oc in agg_outputs.iter_mut().skip(n_keys) {
                            oc.column_id = memo.factory.create(
                                None,
                                oc.name.clone(),
                                oc.data_type.clone(),
                                oc.nullable,
                            );
                        }
                    }
                    let aggregates = plan
                        .items
                        .iter()
                        .enumerate()
                        .map(|(i, item)| {
                            let mv_col = agg_cols[item.mv_output_index].clone()?;
                            let orig = &original_agg.aggregates[i];
                            Some(AggregateCall {
                                name: item.rollup_fn.to_string(),
                                args: vec![column_ref(&mv_col)],
                                distinct: false,
                                result_type: orig.result_type.clone(),
                                order_by: vec![],
                                output_column_id: agg_outputs[n_keys + i].column_id,
                            })
                        })
                        .collect::<Option<Vec<_>>>()?;
                    let agg_op = Operator::LogicalAggregate(LogicalAggregateOp::single(
                        group_by,
                        aggregates,
                        agg_outputs.clone(),
                    ));
                    if !needs_coalesce {
                        return Some(NewExpr { op: agg_op, children: vec![child_group] });
                    }
                    // Scalar COUNT rollup: wrap with COALESCE(sum, 0).
                    let agg_group = memo.new_group(MExpr {
                        id: memo.next_expr_id(),
                        op: agg_op,
                        children: vec![child_group],
                    });
                    let items = original_agg
                        .output_columns
                        .iter()
                        .enumerate()
                        .map(|(i, oc)| {
                            let inner = column_ref(&agg_outputs[i]);
                            let expr = if i >= n_keys && plan.items[i - n_keys].needs_coalesce {
                                TypedExpr {
                                    kind: ExprKind::FunctionCall {
                                        name: "coalesce".to_string(),
                                        args: vec![
                                            inner,
                                            TypedExpr {
                                                kind: ExprKind::Literal(LiteralValue::Int(0)),
                                                data_type: oc.data_type.clone(),
                                                nullable: false,
                                            },
                                        ],
                                        distinct: false,
                                    },
                                    data_type: oc.data_type.clone(),
                                    nullable: false,
                                }
                            } else {
                                inner
                            };
                            ProjectItem {
                                expr,
                                output_name: oc.name.clone(),
                                output_column_id: oc.column_id,
                            }
                        })
                        .collect();
                    Some(NewExpr {
                        op: Operator::LogicalProject(LogicalProjectOp {
                            items,
                            output_qualifier: None,
                        }),
                        children: vec![agg_group],
                    })
                }
            }
        }
    }
}

fn column_ref(c: &OutputColumn) -> TypedExpr {
    TypedExpr {
        kind: ExprKind::ColumnRef {
            column_id: c.column_id,
            qualifier: None,
            column: c.name.clone(),
        },
        data_type: c.data_type.clone(),
        nullable: c.nullable,
    }
}

fn combine_and(mut preds: Vec<TypedExpr>) -> TypedExpr {
    let first = preds.remove(0);
    preds.into_iter().fold(first, |l, r| TypedExpr {
        nullable: l.nullable || r.nullable,
        data_type: crate::sql::DataType::Boolean,
        kind: ExprKind::BinaryOp {
            left: Box::new(l),
            op: crate::sql::analysis::BinOp::And,
            right: Box::new(r),
        },
    })
}

fn same_iceberg_table(a: &crate::sql::catalog::TableDef, b: &crate::sql::catalog::TableDef) -> bool {
    use crate::sql::catalog::ScanSource;
    match (&a.source, &b.source) {
        (
            ScanSource::IcebergDataFiles { table: ta, .. },
            ScanSource::IcebergDataFiles { table: tb, .. },
        ) => ta.catalog == tb.catalog && ta.namespace == tb.namespace && ta.table == tb.table,
        _ => false,
    }
}
```

实现备注（写入代码注释）：
- `MatchedShape::Spj` 时 `query.outputs` 全为 Dimension（来自 scan/filter 顶），Project 绑定原 scan ColumnIds —— memo group 输出列约束由此满足；
- 注入的子 group（scan/filter/agg 中间组）在 `optimize()` 第 9 步 `derive_group_statistics` 全量重推统计（`stats.rs:636`），MV scan 统计经 `table_stats` 按表名查到（Task 9 注入）。

- [ ] **Step 5: `optimize()` 接线**

`src/sql/optimizer/mod.rs`：

```rust
pub(crate) fn optimize(
    plan: LogicalPlan,
    table_stats: &HashMap<String, TableStatistics>,
    factory: ColumnRefFactory,
    dictionary_provider: Option<std::sync::Arc<dyn rewrite::context::QueryDictionaryProvider>>,
    mv_candidates: Vec<cascades_rules::mv_rewrite::MvRewriteCandidate>,
) -> Result<PhysicalPlanNode, String> {
```

step 7 处：

```rust
let mut transform_rules = cascades_rules::all_transformation_rules();
if !mv_candidates.is_empty() {
    transform_rules.push(Box::new(
        cascades_rules::mv_rewrite::rule::MvRewriteRule::new(mv_candidates),
    ));
}
explore(&mut memo, &transform_rules, &options, deadline)?;
```

`is_known_rule_name`（mod.rs:175）加一行：

```rust
|| name == cascades_rules::mv_rewrite::RULE_NAME
```

更新全部调用点追加 `Vec::new()`：`src/engine/mod.rs:2600`、`:2640`、`:2844`、`:4747`，及 `src/sql/optimizer/mod.rs` 测试模块内的 `optimize(...)` 调用（`cargo build` 枚举遗漏点）。

- [ ] **Step 6: 运行测试确认通过**

Run: `cargo test --lib cascades_rules::mv_rewrite && cargo test --lib sql::optimizer`
Expected: PASS；`is_known_rule_name("MvRewrite")` 断言可补进 mod.rs 既有测试（mod.rs:349 同款）。

- [ ] **Step 7: Commit**

```bash
git add src/sql/optimizer/ src/engine/mod.rs
git commit -m "feat(optimizer): MvRewrite cascades rule injecting MV-scan alternatives"
```

---

### Task 9: engine 层候选准备（`mv_rewrite_prep.rs`）+ 执行/EXPLAIN 路径接线

**Files:**
- Create: `src/engine/mv_rewrite_prep.rs`
- Modify: `src/engine/mod.rs`（模块声明；`execute_query_with_options_and_imv_validator_with_catalog_provider` :2804、`execute_query_with_catalog_provider` :2711、`execute_query_with_catalog_mgr` :2675、`explain_query` :2626、`explain_analyze_query` :2578、EXPLAIN 调用点 :839/:867/:885/:906）
- Modify: `src/engine/mv/iceberg_refresh.rs:8257`（`parse_mv_select_query` 改 `pub(crate)`）

- [ ] **Step 1: 实现 prep 模块**

```rust
//! MV rewrite candidate preparation (engine side).
//!
//! Runs after plan_query and before optimize(): discovers fresh Iceberg MVs
//! related to the query's base tables, re-analyzes their defining SQL with
//! the query's ColumnRefFactory, validates the SPJG shape, builds the
//! executable target TableDef, and loads target-table statistics.
//! Every failure is a warn-and-skip: rewrite is an optional optimization.

use std::collections::HashMap;
use std::sync::Arc;

use crate::sql::catalog::{CatalogProvider, ScanSource};
use crate::sql::column_id::ColumnRefFactory;
use crate::sql::optimizer::cascades_rules::mv_rewrite::{
    descriptor::SpjgDescriptor, MvRewriteCandidate,
};
use crate::sql::optimizer::statistics::TableStatistics;
use crate::sql::planner::plan::LogicalPlan;

use super::StandaloneState;

/// Upper bound on candidates per query; aligned with the StarRocks default
/// cbo_materialized_view_rewrite_related_mvs_limit = 16.
const MAX_MV_CANDIDATES: usize = 16;

pub(crate) fn prepare_mv_rewrite_candidates(
    state: &Arc<StandaloneState>,
    analyzer_catalog: &dyn CatalogProvider,
    current_database: &str,
    logical: &LogicalPlan,
    factory: &mut ColumnRefFactory,
    table_stats: &mut HashMap<String, TableStatistics>,
) -> Vec<MvRewriteCandidate> {
    if !crate::sql::optimizer::options::current_session_optimizer_settings()
        .mv_rewrite_enabled()
    {
        return Vec::new();
    }
    match try_prepare(state, analyzer_catalog, current_database, logical, factory, table_stats)
    {
        Ok(c) => c,
        Err(e) => {
            log::warn!("mv rewrite candidate preparation failed: {e}");
            Vec::new()
        }
    }
}

fn try_prepare(
    state: &Arc<StandaloneState>,
    analyzer_catalog: &dyn CatalogProvider,
    current_database: &str,
    logical: &LogicalPlan,
    factory: &mut ColumnRefFactory,
    table_stats: &mut HashMap<String, TableStatistics>,
) -> Result<Vec<MvRewriteCandidate>, String> {
    // 1. Iceberg base tables referenced by the query, as "cat.ns.tbl" FQNs
    //    (the exact format of StoredMvDefinition.base_table_refs, produced
    //    by IcebergTableRef::fqn at MV creation).
    let mut query_fqns: Vec<String> = Vec::new();
    collect_iceberg_fqns(logical, &mut query_fqns);
    if query_fqns.is_empty() {
        return Ok(Vec::new());
    }

    // 2. List stored MVs.
    let Some(provider) = state.metadata_provider.as_ref() else {
        return Ok(Vec::new());
    };
    let read = provider
        .begin_read()
        .map_err(|e| format!("mv metadata read txn: {e}"))?;
    let definitions = state
        .mv_repo
        .list_definitions(read.as_ref())
        .map_err(|e| format!("list mv definitions: {e}"))?;

    let mut candidates = Vec::new();
    for def in definitions {
        if candidates.len() >= MAX_MV_CANDIDATES {
            log::warn!("mv rewrite: candidate cap {MAX_MV_CANDIDATES} reached, rest skipped");
            break;
        }
        // Storage filter only. In-flight refresh does NOT disqualify a
        // candidate: pins always point at committed snapshots.
        if def.storage_engine != "iceberg" {
            continue;
        }
        if !def.base_table_refs.iter().any(|r| query_fqns.contains(r)) {
            continue;
        }
        match build_candidate(state, analyzer_catalog, current_database, &def, factory) {
            Ok(Some(c)) => {
                // 5. Inject target-table statistics (bare lowercase name key;
                //    see collect_scan_stats insert / derive_scan_statistics
                //    lookup). A name collision with a query table makes stats
                //    ambiguous: drop the candidate (spec §5.5).
                let key = c.target_table.name.to_ascii_lowercase();
                if table_stats.contains_key(&key) {
                    log::warn!(
                        "mv rewrite: target table name {key} collides with a query table; skipping {}",
                        c.mv_name
                    );
                    continue;
                }
                if let Some(ts) = load_target_stats(&c.target_table) {
                    table_stats.insert(key, ts);
                }
                candidates.push(c);
            }
            Ok(None) => {}
            Err(e) => log::warn!("mv rewrite: skipping mv {}: {e}", def.mv_id),
        }
    }
    Ok(candidates)
}

fn build_candidate(
    state: &Arc<StandaloneState>,
    analyzer_catalog: &dyn CatalogProvider,
    current_database: &str,
    def: &crate::meta::repository::mv::StoredMvDefinition,
    factory: &mut ColumnRefFactory,
) -> Result<Option<MvRewriteCandidate>, String> {
    // 2b. Strict freshness: every base table's CURRENT snapshot must equal
    //     the pinned snapshot from the last refresh. Never refreshed -> skip.
    if def.last_refresh_snapshots.is_empty() {
        return Ok(None);
    }
    let base_refs =
        crate::engine::mv::iceberg_refresh::parse_iceberg_table_refs(&def.base_table_refs)?;
    for r in &base_refs {
        let fqn = r.fqn();
        let Some(pinned) = def.last_refresh_snapshots.get(&fqn) else {
            return Ok(None);
        };
        let current = current_snapshot_id(state, r)?;
        if current != Some(*pinned) {
            return Ok(None); // stale (or unreadable) -> strict mode skips
        }
        if let Some(pinned_uuid) = def.last_refresh_table_uuids.get(&fqn) {
            if current_table_uuid(state, r)?.as_deref() != Some(pinned_uuid.as_str()) {
                return Ok(None); // table was dropped & recreated
            }
        }
    }

    // 3. Re-analyze the defining SQL with the query's factory; validate SPJG.
    let select = crate::engine::mv::iceberg_refresh::parse_mv_select_query(&def.select_sql)?;
    let owned = std::mem::replace(factory, ColumnRefFactory::new());
    let (resolved, ctes, returned) = crate::sql::analyzer::analyze_with_factory(
        &select,
        analyzer_catalog,
        current_database,
        owned,
    )?;
    let mut returned = returned;
    let mv_logical =
        crate::sql::planner::plan_query(resolved, ctes, &mut returned)?;
    *factory = returned;
    let mv_desc = SpjgDescriptor::from_logical_plan(&mv_logical)?;

    // 3b. Fail closed on name-resolution drift: the analyzed scan must be
    //     one of the recorded base tables.
    let ScanSource::IcebergDataFiles { table, .. } = &mv_desc.table.source else {
        return Ok(None);
    };
    let scan_fqn = format!("{}.{}.{}", table.catalog, table.namespace, table.table);
    if !def.base_table_refs.contains(&scan_fqn) {
        return Err(format!(
            "mv select resolved to {scan_fqn}, not in recorded base refs"
        ));
    }

    // 4. Build the executable target TableDef via the iceberg connector pair
    //    (same mechanism as register_external_table_by_name; no global
    //    registration needed — LogicalScanOp embeds the TableDef).
    let (Some(cat), Some(ns), Some(tbl)) =
        (&def.target_catalog, &def.target_namespace, &def.target_table)
    else {
        return Ok(None);
    };
    let (catalog_backend, table_source) = {
        let registry = state
            .connectors
            .read()
            .expect("standalone connector registry read lock");
        (registry.catalog_backend("iceberg")?, registry.table_source("iceberg")?)
    };
    let resolved_tbl = catalog_backend.load_table(cat, ns, tbl)?;
    let target_table = table_source.build_schema_table_def(&resolved_tbl)?;

    // Duplicate output names break the by-name visible-column mapping.
    let mut names: Vec<&str> = mv_desc.outputs.iter().map(|o| o.name.as_str()).collect();
    names.sort_unstable();
    if names.windows(2).any(|w| w[0] == w[1]) {
        return Ok(None);
    }

    Ok(Some(MvRewriteCandidate {
        mv_name: tbl.clone(),
        mv: mv_desc,
        target_database: ns.clone(),
        target_table,
    }))
}
```

剩余三个辅助函数同文件实现：
- `collect_iceberg_fqns(plan, out)`：仿 `collect_scan_stats`（engine/mod.rs:3042）的全节点递归 match，对 `LogicalPlan::Scan` 且 `ScanSource::IcebergDataFiles` 取 `format!("{}.{}.{}", t.catalog, t.namespace, t.table)`；
- `current_snapshot_id(state, r) -> Result<Option<i64>, String>`：`state.iceberg_catalogs.read()` → `registry.get(r.catalog)` → `crate::connector::iceberg::catalog::load_table(&entry, &r.namespace, &r.table)` → `metadata().current_snapshot().map(|s| s.snapshot_id())`（参照 engine/mod.rs:6917 的现有模式，但**不**调 `invalidate_table_cache`——与本查询 scan 解析共享同一缓存视图，注释说明）；`current_table_uuid` 同路径取 `metadata().uuid().to_string()`；
- `load_target_stats(table_def) -> Option<TableStatistics>`：对 `ScanSource::IcebergDataFiles { table, files, cloud_properties, .. }` 调 `load_iceberg_puffin_ndv(Some(table), cloud_properties)` + `build_table_statistics_with_ndv(files, &table_def.columns, ...)`（与 collect_scan_stats :3050-3074 同款；`load_iceberg_puffin_ndv` 在 engine/mod.rs，需要改 `pub(crate)` 或移挂到本模块可见）。注意 `files` 为空（CurrentSnapshot 绑定）时 `build_table_statistics_with_ndv` 可能返回 None——此时退化为无统计（CBO 用 fallback 行数），记 debug 日志即可，不丢候选。
- `parse_iceberg_table_refs` / `parse_mv_select_query`（iceberg_refresh.rs 私有函数）改 `pub(crate)`。

另外把 `if def.storage_engine != "iceberg" || def.refresh_in_progress && false {...}` 这个草稿块删掉，只保留 `if def.storage_engine != "iceberg" { continue; }`（上文代码已含正确版本，删除占位行即可）。

- [ ] **Step 2: 主路径与 EXPLAIN 接线**

`execute_query_with_options_and_imv_validator_with_catalog_provider`（:2804）：
- 签名追加 `mv_rewrite_state: Option<&Arc<StandaloneState>>`（最后一个参数）；
- `let table_stats = build_table_stats_from_plan(&logical);` 改 `let mut table_stats = ...`，其后插入：

```rust
let mv_candidates = match mv_rewrite_state {
    Some(state) if mv_refresh_ctx.is_none() => {
        crate::engine::mv_rewrite_prep::prepare_mv_rewrite_candidates(
            state,
            analyzer_catalog,
            current_database,
            &logical,
            &mut factory,
            &mut table_stats,
        )
    }
    _ => Vec::new(),
};
let mut physical =
    crate::sql::optimizer::optimize(logical, &table_stats, factory, None, mv_candidates)?;
```

包装链：`execute_query_with_catalog_provider`（:2711）与 `execute_query_with_options_and_imv_validator`（:2776）各追加同名参数透传；`execute_query`（:2656）、`execute_query_with_options`（:2750）传 `None`；**`execute_query_with_catalog_mgr`（:2675）传 `Some(state)`**。

`explain_query`（:2626）与 `explain_analyze_query`（:2578）：同样追加 `mv_rewrite_state: Option<&Arc<StandaloneState>>`，体内 `build_table_stats_from_plan` 改 mut + 同款 prep 块（无 `mv_refresh_ctx`，条件只看 `mv_rewrite_state`），`optimize` 调用追加 `mv_candidates`；调用点 :867 / :906 传 `Some(&self.inner)`，其余调用点（如 tests :3976 附近）传 `None`。

`src/engine/mod.rs` 顶部声明 `pub(crate) mod mv_rewrite_prep;`。

- [ ] **Step 3: 构建与全量单元测试**

Run: `cargo build && cargo test --lib`
Expected: 通过；既有 engine 测试不受影响（新参数全 None 路径零行为变化）。

- [ ] **Step 4: Commit**

```bash
git add src/engine/ src/sql/
git commit -m "feat(engine): MV rewrite candidate preparation wired into query and explain paths"
```

---

### Task 10: sql-tests 新 suite `mv-rewrite`

**Files:**
- Create: `sql-tests/mv-rewrite/sql/*.sql`（5 个用例文件）+ `sql-tests/mv-rewrite/result/`（record 生成）

套件目录创建后 runner 自动发现（`tests/sql-test-runner/src/config.rs:429`）。环境：`source docker/iceberg-rest/runtime/current/env.sh && docker/iceberg-rest/up.sh`，server 用 `dev-opt` 构建启动（CLAUDE.md 的 NOVAROCKS_READY 流程）。

- [ ] **Step 1: 命中类用例 `mv_rewrite_hit_basic.sql`**

```sql
-- @sequential=true
-- @order_sensitive=true
-- @tags=optimizer,mv,rewrite,iceberg
-- Test Objective:
-- 1. SPJ exact match: query rewritten to scan the MV target table.
-- 2. Predicate compensation: tighter query range still hits with a Filter.
-- 3. Group-by subset rollup: SUM/COUNT(*) re-aggregated from the MV.
-- 4. Scalar COUNT rollup over empty compensation result returns 0, not NULL.

-- query 1
-- @skip_result_check=true
CREATE EXTERNAL CATALOG mvrw_${uuid0}
PROPERTIES (
  "type" = "iceberg",
  "iceberg.catalog.type" = "hadoop",
  "iceberg.catalog.warehouse" = "${iceberg_catalog_warehouse}/mvrw_${uuid0}",
  "aws.s3.endpoint" = "${oss_endpoint}",
  "aws.s3.access_key" = "${oss_ak}",
  "aws.s3.secret_key" = "${oss_sk}",
  "aws.s3.enable_path_style_access" = "true"
);
CREATE DATABASE mvrw_${uuid0}.ns_${uuid0};
CREATE TABLE mvrw_${uuid0}.ns_${uuid0}.orders (
  id BIGINT NOT NULL,
  region STRING,
  day STRING,
  amount BIGINT
) TBLPROPERTIES ("format-version" = "3", "write.row-lineage" = "true");
INSERT INTO mvrw_${uuid0}.ns_${uuid0}.orders VALUES
  (1, 'east', 'd1', 10), (2, 'east', 'd2', 20),
  (3, 'west', 'd1', 30), (4, 'west', 'd2', 40), (5, 'north', 'd1', 50);
SET CATALOG mvrw_${uuid0};
USE ns_${uuid0};
CREATE MATERIALIZED VIEW agg_mv
DISTRIBUTED BY HASH(region) BUCKETS 1
PROPERTIES ('storage_engine' = 'iceberg')
AS SELECT region, day, COUNT(*) AS c, SUM(amount) AS s
FROM orders GROUP BY region, day;

-- query 2
-- @skip_result_check=true
REFRESH MATERIALIZED VIEW agg_mv WITH SYNC MODE;

-- query 3
-- group-by subset rollup hits the MV
-- @explain_contains=rewritten with mv: agg_mv
EXPLAIN VERBOSE SELECT region, SUM(amount), COUNT(*) FROM orders GROUP BY region;

-- query 4
SELECT region, SUM(amount) AS s, COUNT(*) AS c FROM orders GROUP BY region ORDER BY region;

-- query 5
-- group-by equal + compensation predicate on a group key
-- @explain_contains=rewritten with mv: agg_mv
EXPLAIN VERBOSE SELECT region, day, SUM(amount) FROM orders WHERE region = 'east' GROUP BY region, day;

-- query 6
SELECT region, day, SUM(amount) AS s FROM orders WHERE region = 'east' GROUP BY region, day ORDER BY day;

-- query 7
-- scalar COUNT rollup with empty matching groups must return 0
-- @explain_contains=rewritten with mv: agg_mv
EXPLAIN VERBOSE SELECT COUNT(*) FROM orders WHERE region = 'nosuch';

-- query 8
SELECT COUNT(*) AS c FROM orders WHERE region = 'nosuch';

-- query 9
-- @skip_result_check=true
DROP MATERIALIZED VIEW agg_mv;
```

注意：query 5/7 的补偿谓词 `region = 'nosuch'` 落在 group-by 列上（合法）；若实现把等值谓词判为「范围相同免补偿」也不影响断言（断言只看命中）。

- [ ] **Step 2: 不命中类 `mv_rewrite_miss.sql`**

同样的 catalog/表/MV 前奏（独立 `${uuid0}`），随后逐查询断言 `@explain_not_contains=rewritten with mv`：
- `SELECT region, AVG(amount) FROM orders GROUP BY region`（AVG 上卷拒绝）；
- `SELECT region, COUNT(DISTINCT day) FROM orders GROUP BY region`（DISTINCT 拒绝）；
- `SELECT id FROM orders`（列不覆盖：MV 无 id）；
- `SELECT region, SUM(amount) FROM orders WHERE amount > 5 GROUP BY region`（补偿谓词 amount 非 group-by 列 → 拒绝）；
- 另建一个带 WHERE 的 MV `AS SELECT ... WHERE day = 'd1' GROUP BY ...`，查询无该谓词 → 范围不蕴含拒绝。

- [ ] **Step 3: 新鲜度生命周期 `mv_rewrite_freshness.sql`**

前奏同上，然后：

```sql
-- query N: fresh after refresh -> hit
-- @explain_contains=rewritten with mv: agg_mv
EXPLAIN VERBOSE SELECT region, SUM(amount) FROM orders GROUP BY region;

-- query N+1: write to the base table -> snapshot advances -> no rewrite
-- @skip_result_check=true
INSERT INTO mvrw_${uuid0}.ns_${uuid0}.orders VALUES (6, 'east', 'd3', 60);

-- query N+2
-- @explain_not_contains=rewritten with mv
EXPLAIN VERBOSE SELECT region, SUM(amount) FROM orders GROUP BY region;

-- query N+3: results still correct from base table
SELECT region, SUM(amount) AS s FROM orders GROUP BY region ORDER BY region;

-- query N+4
-- @skip_result_check=true
REFRESH MATERIALIZED VIEW agg_mv WITH SYNC MODE;

-- query N+5: hit restored
-- @explain_contains=rewritten with mv: agg_mv
EXPLAIN VERBOSE SELECT region, SUM(amount) FROM orders GROUP BY region;
```

- [ ] **Step 4: 开关类 `mv_rewrite_switches.sql`**

前奏后：`SET enable_materialized_view_rewrite = off;` → `@explain_not_contains` → `SET ... = on;` → `@explain_contains` → `SET disable_optimizer_rules = 'MvRewrite';` → `@explain_not_contains` → `SET disable_optimizer_rules = '';` → `@explain_contains`。

- [ ] **Step 5: SPJ 命中类 `mv_rewrite_spj.sql`**

SPJ MV `AS SELECT region, day, amount FROM orders WHERE amount > 0`：
- SPJ 查询精确/收紧命中（`@explain_contains`）+ 结果比对；
- SPJG 查询（`SELECT region, SUM(amount) ... GROUP BY region`）保留聚合命中 SPJ MV；
- 查询 `WHERE amount >= -5`（更宽）不命中。

- [ ] **Step 6: record 模式生成 golden 并人工核对**

```bash
source docker/iceberg-rest/runtime/current/env.sh
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite mv-rewrite --mode record --record-from target
```

核对每个 `.result`：命中用例的 EXPLAIN 里 SCAN 的表名是 MV 目标表且带 `rewritten with mv:` 行；结果集与不改写时一致（对照 freshness 用例 N+3 的 base 结果）。然后 verify 模式复跑确认稳定：

```bash
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" --suite mv-rewrite --mode verify
```

Expected: 全部 PASS。

- [ ] **Step 7: Commit**

```bash
git add sql-tests/mv-rewrite/
git commit -m "test: add mv-rewrite sql suite (hit/miss/freshness/switches/spj)"
```

---

### Task 11: 回归与收尾

- [ ] **Step 1: 代码质量**

Run: `cargo fmt && cargo clippy --all-targets 2>&1 | grep -E "^(warning|error)" | head -30 && cargo test --lib`
Expected: clippy 无新告警；单测全过。

- [ ] **Step 2: 既有 SQL 套件回归（dev-opt server）**

```bash
source docker/iceberg-rest/runtime/current/env.sh
# optimizer / materialized-view / iceberg / iceberg-ivm 四个最相关套件
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" --suite optimizer --mode verify
# ... 同款依次跑 materialized-view、iceberg、iceberg-ivm
```

Expected: 与 main 基线相同的通过集（默认开启下，无匹配 MV 的查询计划零变化；`materialized-view` 套件的 MV 是非 iceberg base 表，prep 阶段即过滤）。若出现 diff，逐个分析：只接受「确属改写命中且结果等价」的 diff（不应有——该套件 base 表非 Iceberg），其余视为回归修复。

- [ ] **Step 3: 自检对照 spec**

逐节核对 `docs/design/specs/2026-06-10-mv-query-rewrite-design.md` §2 目标全部落地：SPJ 精确/补偿/上卷/标量 COALESCE/CBO 择优/严格新鲜度/双开关/EXPLAIN 注记/测试矩阵。

- [ ] **Step 4: Commit（如有收尾修改）并汇报**

```bash
git add -A && git commit -m "chore: mv rewrite regression fixes and cleanup"
```

---

## 已知风险与执行注意

1. **`from_memo` 与 `SplitAggregateRule` 的共存**：MvRewrite 与 SplitAggregate 同在 explore 轮内；MvRewrite 只匹配 `AggStage::Single && !is_split` 的原始形（convert 产物恒为 Single，见 `convert.rs` Aggregate 臂），拆分形不会误匹配。注入的 rollup Aggregate 同为 Single 形，后续轮次会被 SplitAggregate 正常二阶段化——这是期望行为。
2. **测试夹具的 `ScanSource`**：优化器单测需构造 TableDef；`same_iceberg_table` 只认 `IcebergDataFiles`。单测构造 `IcebergTableInfo` 时字段较多——grep 现有单测中 `IcebergTableInfo {` 的构造例直接复用（`src/sql/` 与 `src/engine/` tests 均有）。
3. **聚合输出列顺序假设**（`[group keys..., aggs...]`）：descriptor 抽取处已有显式长度校验，不符即拒绝该侧（fail closed），不会产生错误改写。
4. **EXPLAIN 路径无 TLS 设置时**：`explain_query` 经 `execute_in_context` 的 `with_session_optimizer_settings` 包裹调用（server/mod.rs:1106），TLS 在位；engine 单测直接调用时 TLS 为默认值（mv rewrite 默认开但 `mv_rewrite_state=None`，prep 不运行）。
5. **`parse_iceberg_table_refs` 可见性**：iceberg_refresh.rs 内多处私有 helper 需要 `pub(crate)` 化（`parse_mv_select_query`、`parse_iceberg_table_refs`、`IcebergTableRef`），不要复制实现。
6. **统计缺失时的 CBO 行为**：MV scan 无统计（files 为空导致 build 返回 None）时走 fallback 行数，可能不被选中——功能正确性不受影响；sql-tests 用例数据量小，EXPLAIN 断言以实测 record 为准，若 CurrentSnapshot 绑定导致统计恒缺失，改为在 `load_target_stats` 里经 registry 拉当前 snapshot 的文件列表构造统计（参照 ANALYZE 路径的文件枚举），作为该任务内的修正项处理。
