# Cost Model Redesign Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 把 NovaRocks optimizer cost 模型从单一 `f64` self-cost 升级为 CPU / memory / network 三维、property-aware、可解释、可测试的 CBO cost 主路径。

**Architecture:** `statistics.rs` 提供 size/weight 基础能力；`cost.rs` 持有 `CostInput`、`CostOptions` 和所有 cost kernel；`search.rs` 继续用 `f64` total 选择 winner，但 total 来自 `CostEstimate`；`distributed_node.rs` / `distributed_build.rs` / `explain.rs` 负责把 self-cost 暴露到 `EXPLAIN COSTS`；multi-join reorder proxy 只做轻量剪枝，不替代 memo search。

**Tech Stack:** Rust, Cargo unit tests, NovaRocks Cascades optimizer, `src/sql/optimizer/*`, `src/sql/planner/*`, `src/sql/codegen/ir/explain.rs`, SQL golden tests.

---

## Scope And Dirty-Worktree Guard

当前 worktree 已知存在未提交代码改动：

- `src/sql/optimizer/cascades_rules/multi_join_reorder/algo.rs`
- `src/sql/optimizer/cascades_rules/multi_join_reorder/flatten.rs`
- `src/sql/optimizer/cascades_rules/multi_join_reorder/mod.rs`
- `src/sql/optimizer/estimate/join_condition.rs`
- `src/sql/optimizer/estimate/ndv.rs`
- `src/sql/optimizer/estimate/selectivity.rs`
- `src/sql/optimizer/stats.rs`

执行任何任务前先运行：

```bash
git status --short --branch
```

Expected: 只看到上述既有 dirty 文件，或者看到执行者在前一个任务中刚提交后的干净状态。不要用 `git add .`。每次 commit 只 stage 本任务明确列出的文件。任务 8 会触碰已 dirty 的 `multi_join_reorder/algo.rs`，执行前必须先读该文件当前 diff；如果 diff 不是本轮任务生成的，先停止并报告，避免把用户改动混入提交。

## File Structure

- Modify: `src/sql/optimizer/statistics.rs`
  - 增加 `Statistics::compute_size_for_columns`、safe row/size helper、`CostEstimate::weighted_total`。
- Modify: `src/sql/optimizer/cost.rs`
  - 增加 `CostInput`、扩展 `CostOptions`、新增 `compute_cost_estimate` 主入口和 per-operator kernels。
- Modify: `src/sql/optimizer/search.rs`
  - search 继续返回 `Cost = f64`，但通过 `CostEstimate` weighted total 计算 winner cost。
- Modify: `src/sql/optimizer/derive/mod.rs`
  - enforcer cost 走 `CostEstimate` kernel，保留现有 `estimate_enforcer_cost` total wrapper。
- Modify: `src/sql/planner/distributed_node.rs`
  - `PlanNodeStats` 增加可选 self-cost copy。
- Modify: `src/sql/planner/distributed_build.rs`
  - 构建 distributed node stats 时计算并携带 self-cost。
- Modify: `src/sql/codegen/ir/explain.rs`
  - `EXPLAIN COSTS` 输出 rows、cost dimensions、total、confidence 和关键 decision 字段。
- Modify: `src/sql/optimizer/cascades_rules/multi_join_reorder/algo.rs`
  - join reorder proxy 与新 cost 因子命名对齐。
- Add/Modify: `sql-tests/optimizer/sql/*.sql`
  - 增加可稳定断言的 optimizer plan-shape golden。

## Task 1: Statistics Helpers And Weighted Cost Foundation

**Files:**
- Modify: `src/sql/optimizer/statistics.rs`

- [ ] **Step 1: Write failing tests for statistics size helpers and weighted totals**

Append these tests inside the existing `#[cfg(test)] mod tests` in `src/sql/optimizer/statistics.rs`:

```rust
#[test]
fn statistics_compute_size_for_requested_columns() {
    let mut stats = Statistics {
        output_row_count: 10.0,
        ..Default::default()
    };
    stats.column_statistics.insert(
        ColumnId::new_for_test(1),
        ColumnStatistic {
            average_row_size: 4.0,
            ..Default::default()
        },
    );
    stats.column_statistics.insert(
        ColumnId::new_for_test(2),
        ColumnStatistic {
            average_row_size: 16.0,
            ..Default::default()
        },
    );

    assert_eq!(
        stats.compute_size_for_columns(&[ColumnId::new_for_test(2)]),
        160.0
    );
    assert_eq!(
        stats.compute_size_for_columns(&[
            ColumnId::new_for_test(1),
            ColumnId::new_for_test(2)
        ]),
        200.0
    );
}

#[test]
fn statistics_compute_size_for_missing_columns_uses_default_width() {
    let stats = Statistics {
        output_row_count: 5.0,
        ..Default::default()
    };

    assert_eq!(
        stats.compute_size_for_columns(&[ColumnId::new_for_test(99)]),
        40.0
    );
}

#[test]
fn cost_estimate_weighted_total_uses_explicit_weights() {
    let cost = CostEstimate {
        cpu_cost: 100.0,
        memory_cost: 10.0,
        network_cost: 20.0,
    };

    assert_eq!(cost.weighted_total(0.5, 2.0, 1.5), 100.0);
}
```

- [ ] **Step 2: Run the new tests and verify they fail**

Run each filter separately:

```bash
cargo test --lib statistics_compute_size_for_requested_columns
cargo test --lib statistics_compute_size_for_missing_columns_uses_default_width
cargo test --lib cost_estimate_weighted_total_uses_explicit_weights
```

Expected: first two fail with no method named `compute_size_for_columns`; third fails with no method named `weighted_total`.

- [ ] **Step 3: Implement statistics helpers**

Add these methods to the existing `impl Statistics`:

```rust
    pub fn safe_output_row_count(&self) -> f64 {
        if self.output_row_count.is_finite() && self.output_row_count > 0.0 {
            self.output_row_count
        } else {
            1.0
        }
    }

    pub fn compute_size_for_columns(&self, columns: &[ColumnId]) -> f64 {
        if columns.is_empty() {
            return self.compute_size();
        }
        let row_width: f64 = columns
            .iter()
            .map(|column_id| {
                self.column_statistics
                    .get(column_id)
                    .map(|c| c.average_row_size)
                    .filter(|v| v.is_finite() && *v > 0.0)
                    .unwrap_or(8.0)
            })
            .sum();
        self.safe_output_row_count() * row_width
    }
```

Add this method to the existing `impl CostEstimate`:

```rust
    pub fn weighted_total(&self, cpu_weight: f64, memory_weight: f64, network_weight: f64) -> f64 {
        fn finite_or_zero(v: f64) -> f64 {
            if v.is_finite() && v > 0.0 { v } else { 0.0 }
        }
        finite_or_zero(self.cpu_cost) * cpu_weight
            + finite_or_zero(self.memory_cost) * memory_weight
            + finite_or_zero(self.network_cost) * network_weight
    }
```

- [ ] **Step 4: Run the focused tests and existing CostEstimate tests**

```bash
cargo test --lib statistics_compute_size_for_requested_columns
cargo test --lib statistics_compute_size_for_missing_columns_uses_default_width
cargo test --lib cost_estimate_weighted_total_uses_explicit_weights
cargo test --lib cost_estimate_total
cargo test --lib cost_estimate_add
```

Expected: all five pass.

- [ ] **Step 5: Commit Task 1**

```bash
git add src/sql/optimizer/statistics.rs
git commit -m "feat(optimizer): add cost statistics size helpers"
```

Expected: commit includes only `src/sql/optimizer/statistics.rs`.

## Task 2: CostInput And CostOptions Main Entry

**Files:**
- Modify: `src/sql/optimizer/cost.rs`

- [ ] **Step 1: Add failing tests for `CostInput` and option-weighted totals**

Append these tests inside `src/sql/optimizer/cost.rs` tests:

```rust
#[test]
fn compute_cost_estimate_returns_dimensions_for_scan() {
    let s = stats(1000.0, 100.0);
    let op = scan_op();
    let child_stats: [&Statistics; 0] = [];
    let child_outputs: [&PhysicalPropertySet; 0] = [];
    let required = PhysicalPropertySet::any();
    let options = CostOptions::default();
    let input = CostInput {
        op: &op,
        own_stats: &s,
        child_stats: &child_stats,
        child_outputs: &child_outputs,
        required_output: &required,
        alt_kind: &PropertyAlternativeKind::Default,
        scalars: None,
        options: &options,
    };

    let estimate = compute_cost_estimate(&input);
    assert!(estimate.cpu_cost > 0.0);
    assert_eq!(estimate.memory_cost, 0.0);
    assert_eq!(estimate.network_cost, 0.0);
}

#[test]
fn cost_options_weights_drive_total_cost() {
    let options = CostOptions {
        cpu_weight: 1.0,
        memory_weight: 10.0,
        network_weight: 100.0,
        ..Default::default()
    };
    let estimate = CostEstimate {
        cpu_cost: 1.0,
        memory_cost: 2.0,
        network_cost: 3.0,
    };

    assert_eq!(estimate.total_with_options(&options), 321.0);
}
```

Add this helper in the test module:

```rust
fn scan_op() -> Operator {
    Operator::PhysicalScan(ScanOp {
        database: String::new(),
        table: crate::sql::catalog::TableDef {
            name: "t".into(),
            columns: vec![],
            iceberg_row_lineage_metadata_columns: vec![],
            source: crate::sql::catalog::ScanSource::StarRocks {
                db_id: 0,
                table_id: 0,
            },
        },
        alias: None,
        columns: vec![],
        predicates: vec![],
        required_columns: None,
        dict_columns: vec![],
        variant_columns: vec![],
        mv_rewritten_from: None,
    })
}
```

- [ ] **Step 2: Run the tests and verify they fail**

```bash
cargo test --lib compute_cost_estimate_returns_dimensions_for_scan
cargo test --lib cost_options_weights_drive_total_cost
```

Expected: fail because `CostInput`, `compute_cost_estimate`, `cpu_weight`, `memory_weight`, `network_weight`, or `total_with_options` do not exist.

- [ ] **Step 3: Add CostInput, extend CostOptions, and keep f64 wrappers**

Update imports at the top of `cost.rs`:

```rust
use super::scalar::{ScalarArena, ScalarId, ScalarNode};
use crate::sql::optimizer::statistics::{CostEstimate, Statistics};
```

Add `CostInput` before `compute_cost`:

```rust
pub(crate) struct CostInput<'a> {
    pub op: &'a Operator,
    pub own_stats: &'a Statistics,
    pub child_stats: &'a [&'a Statistics],
    pub child_outputs: &'a [&'a PhysicalPropertySet],
    pub required_output: &'a PhysicalPropertySet,
    pub alt_kind: &'a PropertyAlternativeKind,
    pub scalars: Option<&'a ScalarArena>,
    pub options: &'a CostOptions,
}
```

Extend `CostOptions`:

```rust
#[derive(Clone, Debug)]
pub(crate) struct CostOptions {
    pub cpu_weight: f64,
    pub memory_weight: f64,
    pub network_weight: f64,
    pub backend_factor: f64,
    pub broadcast_row_limit: f64,
    pub broadcast_byte_limit: f64,
    pub broadcast_right_table_scale_factor: f64,
    pub fallback_broadcast_row_limit: f64,
    pub network_cost: f64,
    pub memory_cost_weight: f64,
    pub predicate_cost_factor: f64,
    pub projection_cost_factor: f64,
    pub hash_cost_factor: f64,
    pub sort_cost_factor: f64,
    pub topn_cost_factor: f64,
    pub aggregate_cost_factor: f64,
    pub exchange_startup_cost: f64,
    pub fallback_cpu_factor: f64,
}
```

Update `Default`:

```rust
            cpu_weight: 0.5,
            memory_weight: 2.0,
            network_weight: 1.5,
            backend_factor: 3.0,
            broadcast_row_limit: 15_000_000.0,
            broadcast_byte_limit: 512.0 * 1024.0 * 1024.0,
            broadcast_right_table_scale_factor: 10.0,
            fallback_broadcast_row_limit: 500_000.0,
            network_cost: NETWORK_COST,
            memory_cost_weight: 0.25,
            predicate_cost_factor: 0.02,
            projection_cost_factor: 0.01,
            hash_cost_factor: 1.0,
            sort_cost_factor: 1.0,
            topn_cost_factor: 1.0,
            aggregate_cost_factor: 1.0,
            exchange_startup_cost: DISTRIBUTION_STARTUP_COST,
            fallback_cpu_factor: 0.01,
```

Add this impl in `cost.rs`:

```rust
impl CostEstimate {
    pub(crate) fn total_with_options(&self, options: &CostOptions) -> Cost {
        self.weighted_total(
            options.cpu_weight,
            options.memory_weight,
            options.network_weight,
        )
    }
}
```

Add wrappers:

```rust
pub(crate) fn compute_cost_estimate(input: &CostInput<'_>) -> CostEstimate {
    match input.op {
        Operator::PhysicalScan(_) => CostEstimate {
            cpu_cost: input.own_stats.compute_size(),
            memory_cost: 0.0,
            network_cost: 0.0,
        },
        _ => CostEstimate {
            cpu_cost: compute_cost(input.op, input.own_stats, input.child_stats),
            memory_cost: 0.0,
            network_cost: 0.0,
        },
    }
}

pub(crate) fn compute_cost_from_input(input: &CostInput<'_>) -> Cost {
    compute_cost_estimate(input).total_with_options(input.options)
}
```

Do not remove existing `compute_cost` and `compute_cost_with_properties` in this task. They remain compatibility wrappers until Task 3.

- [ ] **Step 4: Run focused tests**

```bash
cargo test --lib compute_cost_estimate_returns_dimensions_for_scan
cargo test --lib cost_options_weights_drive_total_cost
```

Expected: both pass.

- [ ] **Step 5: Commit Task 2**

```bash
git add src/sql/optimizer/cost.rs
git commit -m "feat(optimizer): introduce dimensional cost input"
```

Expected: commit includes only `src/sql/optimizer/cost.rs`.

## Task 3: Scalar Complexity And Basic Operator Kernels

**Files:**
- Modify: `src/sql/optimizer/cost.rs`

- [ ] **Step 1: Add failing tests for filter/project/topn dimensions**

Append:

```rust
#[test]
fn filter_cost_uses_input_rows_not_output_rows() {
    let mut arena = ScalarArena::new();
    let predicate = intern_typed(
        &mut arena,
        &crate::sql::analysis::TypedExpr {
            kind: crate::sql::analysis::ExprKind::Literal(
                crate::sql::analysis::LiteralValue::Bool(true),
            ),
            data_type: arrow::datatypes::DataType::Boolean,
            nullable: false,
        },
    );
    let input_stats = stats(1_000_000.0, 16.0);
    let output_stats = stats(10.0, 16.0);
    let op = Operator::PhysicalFilter(FilterOp {
        predicate,
    });
    let child_stats = [&input_stats];
    let child_outputs = [PhysicalPropertySet::any()];
    let required = PhysicalPropertySet::any();
    let options = CostOptions::default();
    let input = CostInput {
        op: &op,
        own_stats: &output_stats,
        child_stats: &child_stats,
        child_outputs: &[&child_outputs[0]],
        required_output: &required,
        alt_kind: &PropertyAlternativeKind::Default,
        scalars: Some(&arena),
        options: &options,
    };

    let estimate = compute_cost_estimate(&input);
    assert!(estimate.cpu_cost > output_stats.compute_size());
}

#[test]
fn topn_estimate_is_cheaper_than_full_sort_for_small_limit() {
    let input_stats = stats(10_000_000.0, 50.0);
    let output_stats = stats(100.0, 50.0);
    let sort = Operator::PhysicalSort(SortOp {
        items: vec![],
        analytic_partition_exprs: Vec::new(),
        partition_limit: None,
        topn_type: None,
    });
    let topn = Operator::PhysicalTopN(TopNOp {
        items: vec![],
        limit: Some(100),
        offset: None,
        phase: TopNPhase::Final,
        is_split: false,
    });
    let options = CostOptions::default();
    let required = PhysicalPropertySet::any();
    let child_outputs = [PhysicalPropertySet::any()];
    let sort_input = CostInput {
        op: &sort,
        own_stats: &input_stats,
        child_stats: &[&input_stats],
        child_outputs: &[&child_outputs[0]],
        required_output: &required,
        alt_kind: &PropertyAlternativeKind::Default,
        scalars: None,
        options: &options,
    };
    let topn_input = CostInput {
        op: &topn,
        own_stats: &output_stats,
        child_stats: &[&input_stats],
        child_outputs: &[&child_outputs[0]],
        required_output: &required,
        alt_kind: &PropertyAlternativeKind::Default,
        scalars: None,
        options: &options,
    };

    assert!(
        compute_cost_estimate(&topn_input).total_with_options(&options)
            < compute_cost_estimate(&sort_input).total_with_options(&options)
    );
}
```

- [ ] **Step 2: Run the new tests and verify they fail**

```bash
cargo test --lib filter_cost_uses_input_rows_not_output_rows
cargo test --lib topn_estimate_is_cheaper_than_full_sort_for_small_limit
```

Expected: fail because the formulas still route through legacy fallback.

- [ ] **Step 3: Add scalar complexity helper**

Add this helper in `cost.rs` outside the test module:

```rust
fn scalar_complexity(arena: Option<&ScalarArena>, expr: ScalarId) -> f64 {
    let Some(arena) = arena else {
        return 1.0;
    };
    match arena.node(expr) {
        ScalarNode::ColumnRef(_) | ScalarNode::LambdaParamRef { .. } | ScalarNode::Literal(_) => 0.1,
        ScalarNode::Nested(child) | ScalarNode::Cast { child, .. } => {
            0.2 + scalar_complexity(Some(arena), *child)
        }
        ScalarNode::UnaryOp { child, .. }
        | ScalarNode::IsNull { child, .. }
        | ScalarNode::IsTruthValue { child, .. } => 0.5 + scalar_complexity(Some(arena), *child),
        ScalarNode::BinaryOp { left, right, .. } => {
            1.0 + scalar_complexity(Some(arena), *left) + scalar_complexity(Some(arena), *right)
        }
        ScalarNode::FunctionCall { args, .. } => {
            3.0 + args
                .iter()
                .map(|arg| scalar_complexity(Some(arena), *arg))
                .sum::<f64>()
        }
        ScalarNode::LambdaFunction { body, .. } | ScalarNode::Lambda { body, .. } => {
            2.0 + scalar_complexity(Some(arena), *body)
        }
        ScalarNode::AggregateCall { args, order_by, .. } => {
            2.0 + args
                .iter()
                .map(|arg| scalar_complexity(Some(arena), *arg))
                .sum::<f64>()
                + order_by.len() as f64
        }
        ScalarNode::InList { child, list, .. } => {
            1.0 + scalar_complexity(Some(arena), *child) + list.len() as f64 * 0.2
        }
        ScalarNode::Between { child, low, high, .. } => {
            1.0
                + scalar_complexity(Some(arena), *child)
                + scalar_complexity(Some(arena), *low)
                + scalar_complexity(Some(arena), *high)
        }
        ScalarNode::Like { child, pattern, .. } => {
            3.0 + scalar_complexity(Some(arena), *child) + scalar_complexity(Some(arena), *pattern)
        }
        ScalarNode::Case {
            operand,
            when_then,
            else_expr,
        } => {
            operand.map(|e| scalar_complexity(Some(arena), e)).unwrap_or(0.0)
                + when_then
                    .iter()
                    .map(|(w, t)| {
                        scalar_complexity(Some(arena), *w) + scalar_complexity(Some(arena), *t)
                    })
                    .sum::<f64>()
                + else_expr
                    .map(|e| scalar_complexity(Some(arena), e))
                    .unwrap_or(0.0)
                + 1.0
        }
        ScalarNode::WindowCall {
            args,
            partition_by,
            order_by,
            ..
        } => {
            4.0 + args
                .iter()
                .chain(partition_by.iter())
                .map(|arg| scalar_complexity(Some(arena), *arg))
                .sum::<f64>()
                + order_by.len() as f64
        }
    }
}

fn scalar_list_complexity(arena: Option<&ScalarArena>, exprs: &[ScalarId]) -> f64 {
    exprs
        .iter()
        .map(|expr| scalar_complexity(arena, *expr))
        .sum::<f64>()
        .max(1.0)
}
```

- [ ] **Step 4: Replace basic operator arms in `compute_cost_estimate`**

Use these arms in `compute_cost_estimate`:

```rust
        Operator::PhysicalFilter(filter) => {
            let input_rows = input
                .child_stats
                .first()
                .map(|s| s.safe_output_row_count())
                .unwrap_or_else(|| input.own_stats.safe_output_row_count());
            let complexity = scalar_complexity(input.scalars, filter.predicate);
            CostEstimate {
                cpu_cost: input_rows * complexity * input.options.predicate_cost_factor,
                memory_cost: input.own_stats.compute_size() * 0.05,
                network_cost: 0.0,
            }
        }
        Operator::PhysicalProject(project) => {
            let input_rows = input
                .child_stats
                .first()
                .map(|s| s.safe_output_row_count())
                .unwrap_or_else(|| input.own_stats.safe_output_row_count());
            let exprs: Vec<_> = project.items.iter().map(|item| item.expr).collect();
            CostEstimate {
                cpu_cost: input_rows
                    * scalar_list_complexity(input.scalars, &exprs)
                    * input.options.projection_cost_factor,
                memory_cost: input.own_stats.compute_size() * 0.02,
                network_cost: 0.0,
            }
        }
        Operator::PhysicalSort(_) => {
            let rows = input
                .child_stats
                .first()
                .map(|s| s.safe_output_row_count())
                .unwrap_or_else(|| input.own_stats.safe_output_row_count());
            CostEstimate {
                cpu_cost: rows * rows.log2().max(1.0) * input.options.sort_cost_factor,
                memory_cost: input.own_stats.compute_size(),
                network_cost: 0.0,
            }
        }
        Operator::PhysicalTopN(topn) => {
            let input_rows = input
                .child_stats
                .first()
                .map(|s| s.safe_output_row_count())
                .unwrap_or_else(|| input.own_stats.safe_output_row_count());
            let k = match (topn.limit, topn.offset) {
                (Some(l), Some(o)) => ((l as f64) + (o as f64)).min(input_rows).max(1.0),
                (Some(l), None) => (l as f64).min(input_rows).max(1.0),
                _ => input_rows,
            };
            CostEstimate {
                cpu_cost: input_rows * k.log2().max(1.0) * input.options.topn_cost_factor,
                memory_cost: input.own_stats.compute_size(),
                network_cost: 0.0,
            }
        }
        Operator::PhysicalLimit(_) | Operator::PhysicalAssertOneRow(_) => CostEstimate {
            cpu_cost: input.own_stats.safe_output_row_count() * 0.001,
            memory_cost: 0.0,
            network_cost: 0.0,
        },
```

- [ ] **Step 5: Run focused tests**

```bash
cargo test --lib filter_cost_uses_input_rows_not_output_rows
cargo test --lib topn_estimate_is_cheaper_than_full_sort_for_small_limit
cargo test --lib top_n_cheaper_than_sort_for_small_limit
cargo test --lib top_n_falls_back_to_sort_cost_when_limit_exceeds_rows
```

Expected: all pass after updating existing assertions if they depended on legacy exact `f64` values.

- [ ] **Step 6: Commit Task 3**

```bash
git add src/sql/optimizer/cost.rs
git commit -m "feat(optimizer): model basic physical operator costs"
```

Expected: commit includes only `src/sql/optimizer/cost.rs`.

## Task 4: Join, Aggregate, And Distribution Kernels

**Files:**
- Modify: `src/sql/optimizer/cost.rs`
- Modify: `src/sql/optimizer/derive/mod.rs`

- [ ] **Step 1: Add failing tests for join and enforcer dimensions**

Append to `cost.rs` tests:

```rust
#[test]
fn broadcast_join_estimate_charges_backend_fanout() {
    let probe = stats(1_000_000.0, 64.0);
    let build = stats(10_000.0, 32.0);
    let own = stats(100_000.0, 96.0);
    let op = Operator::PhysicalHashJoin(PhysicalHashJoinOp {
        join_type: JoinKind::Inner,
        eq_conditions: vec![],
        other_condition: None,
        distribution: JoinDistribution::Unknown,
    });
    let options = CostOptions::default();
    let required = PhysicalPropertySet::any();
    let child_outputs = [PhysicalPropertySet::any(), PhysicalPropertySet::broadcast()];
    let input = CostInput {
        op: &op,
        own_stats: &own,
        child_stats: &[&probe, &build],
        child_outputs: &[&child_outputs[0], &child_outputs[1]],
        required_output: &required,
        alt_kind: &PropertyAlternativeKind::BroadcastJoin,
        scalars: None,
        options: &options,
    };

    let estimate = compute_cost_estimate(&input);
    assert!(estimate.memory_cost >= build.compute_size() * options.backend_factor);
    assert!(estimate.network_cost >= build.compute_size() * options.backend_factor);
}

#[test]
fn shuffle_join_estimate_charges_both_sides_network() {
    let probe = stats(1_000_000.0, 64.0);
    let build = stats(1_000_000.0, 64.0);
    let own = stats(100_000.0, 128.0);
    let op = Operator::PhysicalHashJoin(PhysicalHashJoinOp {
        join_type: JoinKind::Inner,
        eq_conditions: vec![],
        other_condition: None,
        distribution: JoinDistribution::Unknown,
    });
    let options = CostOptions::default();
    let required = PhysicalPropertySet::any();
    let child_outputs = [PhysicalPropertySet::any(), PhysicalPropertySet::any()];
    let input = CostInput {
        op: &op,
        own_stats: &own,
        child_stats: &[&probe, &build],
        child_outputs: &[&child_outputs[0], &child_outputs[1]],
        required_output: &required,
        alt_kind: &PropertyAlternativeKind::ShuffleJoin,
        scalars: None,
        options: &options,
    };

    let estimate = compute_cost_estimate(&input);
    assert!(estimate.network_cost >= probe.compute_size() + build.compute_size());
}
```

Append to `derive.rs` tests:

```rust
#[test]
fn distribution_enforcer_cost_estimate_has_network_dimension() {
    let stats = Statistics {
        output_row_count: 1000.0,
        column_statistics: Default::default(),
        ..Default::default()
    };
    let estimate = estimate_enforcer_cost_estimate(
        &EnforcerKind::Distribution(DistributionSpec::Gather),
        &stats,
        &crate::sql::optimizer::cost::CostOptions::default(),
    );

    assert!(estimate.memory_cost > 0.0);
    assert!(estimate.network_cost > 0.0);
}
```

- [ ] **Step 2: Run the new tests and verify they fail**

```bash
cargo test --lib broadcast_join_estimate_charges_backend_fanout
cargo test --lib shuffle_join_estimate_charges_both_sides_network
cargo test --lib distribution_enforcer_cost_estimate_has_network_dimension
```

Expected: fail until join and enforcer dimensional kernels exist.

- [ ] **Step 3: Add join, aggregate, distribution helpers in `cost.rs`**

Add these helper functions:

```rust
fn estimate_hash_join_cost(input: &CostInput<'_>, join: &PhysicalHashJoinOp) -> CostEstimate {
    let probe = input.child_stats.first().copied();
    let build = input.child_stats.get(1).copied();
    let probe_rows = probe.map(|s| s.safe_output_row_count()).unwrap_or(1.0);
    let build_rows = build.map(|s| s.safe_output_row_count()).unwrap_or(1.0);
    let probe_size = probe.map(|s| s.compute_size()).unwrap_or(0.0);
    let build_size = build.map(|s| s.compute_size()).unwrap_or(0.0);
    let output_size = input.own_stats.compute_size();
    let key_factor = (join.eq_conditions.len() as f64).max(1.0);
    let residual_factor = if join.other_condition.is_some() { NON_EQUI_JOIN_COST_PENALTY } else { 1.0 };

    let mut estimate = match input.alt_kind {
        PropertyAlternativeKind::BroadcastJoin => CostEstimate {
            cpu_cost: (build_rows + probe_rows) * key_factor * input.options.hash_cost_factor
                + output_size,
            memory_cost: build_size * input.options.backend_factor,
            network_cost: build_size * input.options.backend_factor,
        },
        PropertyAlternativeKind::ShuffleJoin => CostEstimate {
            cpu_cost: (build_rows + probe_rows) * key_factor * input.options.hash_cost_factor
                + output_size,
            memory_cost: build_size / input.options.backend_factor.max(1.0),
            network_cost: probe_size + build_size,
        },
        PropertyAlternativeKind::Default => match join.distribution {
            JoinDistribution::Broadcast => CostEstimate {
                cpu_cost: (build_rows + probe_rows) * key_factor * input.options.hash_cost_factor
                    + output_size,
                memory_cost: build_size * input.options.backend_factor,
                network_cost: build_size * input.options.backend_factor,
            },
            JoinDistribution::Shuffle => CostEstimate {
                cpu_cost: (build_rows + probe_rows) * key_factor * input.options.hash_cost_factor
                    + output_size,
                memory_cost: build_size / input.options.backend_factor.max(1.0),
                network_cost: probe_size + build_size,
            },
            JoinDistribution::Colocate => CostEstimate {
                cpu_cost: (build_rows + probe_rows) * key_factor * input.options.hash_cost_factor
                    + output_size,
                memory_cost: build_size,
                network_cost: 0.0,
            },
            JoinDistribution::Unknown => CostEstimate {
                cpu_cost: (build_rows + probe_rows) * key_factor * input.options.hash_cost_factor
                    + output_size,
                memory_cost: build_size,
                network_cost: 0.0,
            },
        },
    };

    if join.join_type == crate::sql::analysis::JoinKind::Cross {
        estimate.cpu_cost *= CROSS_JOIN_COST_PENALTY;
        estimate.memory_cost *= CROSS_JOIN_COST_PENALTY;
    }
    estimate.cpu_cost *= residual_factor;
    estimate
}

fn estimate_nested_loop_join_cost(input: &CostInput<'_>) -> CostEstimate {
    let left_rows = input
        .child_stats
        .first()
        .map(|s| s.safe_output_row_count())
        .unwrap_or(1.0);
    let right_rows = input
        .child_stats
        .get(1)
        .map(|s| s.safe_output_row_count())
        .unwrap_or(1.0);
    CostEstimate {
        cpu_cost: left_rows * right_rows * NEST_LOOP_COST_PENALTY,
        memory_cost: input.child_stats.get(1).map(|s| s.compute_size()).unwrap_or(0.0),
        network_cost: 0.0,
    }
}

fn estimate_aggregate_cost(input: &CostInput<'_>, agg: &PhysicalHashAggregateOp) -> CostEstimate {
    let input_size = input.child_stats.first().map(|s| s.compute_size()).unwrap_or(0.0);
    let group_key_width = input.own_stats.compute_size().max(1.0);
    let function_factor = (agg.aggregates.len() as f64).max(1.0);
    let phase_factor = match agg.mode {
        AggMode::Single => 1.0,
        AggMode::Local => 0.6,
        AggMode::Global | AggMode::DistinctGlobal | AggMode::DistinctLocal => 0.4,
    };
    CostEstimate {
        cpu_cost: input_size * phase_factor * function_factor * input.options.aggregate_cost_factor,
        memory_cost: group_key_width,
        network_cost: 0.0,
    }
}

pub(crate) fn estimate_distribution_cost_estimate(
    stats: &Statistics,
    options: &CostOptions,
) -> CostEstimate {
    CostEstimate {
        cpu_cost: options.exchange_startup_cost,
        memory_cost: stats.compute_size() * 0.05,
        network_cost: stats.compute_size(),
    }
}
```

- [ ] **Step 4: Route operator arms to the new helpers**

In `compute_cost_estimate`, add these arms before fallback:

```rust
        Operator::PhysicalHashJoin(join) => estimate_hash_join_cost(input, join),
        Operator::PhysicalNestLoopJoin(_) => estimate_nested_loop_join_cost(input),
        Operator::PhysicalHashAggregate(agg) => estimate_aggregate_cost(input, agg),
        Operator::PhysicalDistribution(_) => {
            estimate_distribution_cost_estimate(input.own_stats, input.options)
        }
```

Update `compute_cost_with_properties` so it builds a `CostInput` and returns `compute_cost_from_input(&input)`.

- [ ] **Step 5: Add enforcer dimensional wrapper in `derive/mod.rs`**

Update imports:

```rust
use super::cost::{CostOptions, estimate_distribution_cost_estimate};
use super::statistics::CostEstimate;
```

Add:

```rust
pub(crate) fn estimate_enforcer_cost_estimate(
    enforcer: &EnforcerKind,
    stats: &Statistics,
    options: &CostOptions,
) -> CostEstimate {
    match enforcer {
        EnforcerKind::Distribution(_) => estimate_distribution_cost_estimate(stats, options),
        EnforcerKind::Sort(_) => {
            let n = stats.safe_output_row_count();
            CostEstimate {
                cpu_cost: n * n.log2().max(1.0) * options.sort_cost_factor,
                memory_cost: stats.compute_size(),
                network_cost: 0.0,
            }
        }
    }
}
```

Change existing `estimate_enforcer_cost` to:

```rust
pub(crate) fn estimate_enforcer_cost(enforcer: &EnforcerKind, stats: &Statistics) -> Cost {
    let options = CostOptions::default();
    estimate_enforcer_cost_estimate(enforcer, stats, &options).total_with_options(&options)
}
```

- [ ] **Step 6: Run focused tests**

```bash
cargo test --lib broadcast_join_estimate_charges_backend_fanout
cargo test --lib shuffle_join_estimate_charges_both_sides_network
cargo test --lib distribution_enforcer_cost_estimate_has_network_dimension
cargo test --lib broadcast_join_alternative_charges_fanout_and_memory_pressure
cargo test --lib non_equi_hash_join_uses_optimizer_execute_cost_penalty
cargo test --lib distribution_enforcer_cost_includes_startup_overhead
```

Expected: all pass after legacy exact-number assertions are adjusted to dimensional totals.

- [ ] **Step 7: Commit Task 4**

```bash
git add src/sql/optimizer/cost.rs src/sql/optimizer/derive/mod.rs
git commit -m "feat(optimizer): add dimensional join and enforcer costs"
```

Expected: commit includes only the two listed files.

## Task 5: Search Integration With Dimensional Cost

**Files:**
- Modify: `src/sql/optimizer/search.rs`
- Modify: `src/sql/optimizer/cost.rs`

- [ ] **Step 1: Add a failing search test that uses custom cost weights**

Add a unit test in `src/sql/optimizer/search.rs` near existing search tests:

```rust
#[test]
fn search_uses_cost_estimate_total_for_winner_cost() {
    let options = CostOptions {
        cpu_weight: 1.0,
        memory_weight: 0.0,
        network_weight: 0.0,
        ..Default::default()
    };
    let estimate = crate::sql::optimizer::statistics::CostEstimate {
        cpu_cost: 10.0,
        memory_cost: 1000.0,
        network_cost: 1000.0,
    };

    assert_eq!(estimate.total_with_options(&options), 10.0);
}
```

This test is intentionally narrow: it proves search-visible `Cost` can be derived from `CostEstimate` through `CostOptions`.

- [ ] **Step 2: Run the test**

```bash
cargo test --lib search_uses_cost_estimate_total_for_winner_cost
```

Expected: pass if Task 2 exposed `total_with_options`; fail if visibility is too narrow.

- [ ] **Step 3: Update `search.rs` to build `CostInput`**

Change the import:

```rust
use super::cost::{CostInput, CostOptions, compute_cost_from_input};
```

Replace the `compute_cost_with_properties` call with:

```rust
                let options = CostOptions::default();
                let cost_input = CostInput {
                    op: &expr.op,
                    own_stats: &own_stats,
                    child_stats: &child_stats_refs,
                    child_outputs: &child_output_refs,
                    required_output: required,
                    alt_kind: &alt.kind,
                    scalars: Some(&memo.scalars),
                    options: &options,
                };
                let own_cost = compute_cost_from_input(&cost_input);
```

Replace enforcer total calculation with the dimensional wrapper:

```rust
                    let options = CostOptions::default();
                    let enforcer_cost: Cost = enforcers
                        .iter()
                        .map(|e| {
                            super::derive::estimate_enforcer_cost_estimate(
                                e,
                                &group_stats,
                                &options,
                            )
                            .total_with_options(&options)
                        })
                        .sum();
```

- [ ] **Step 4: Keep the legacy wrapper tested**

In `cost.rs`, keep `compute_cost_with_properties` as a wrapper because other tests call it:

```rust
pub(crate) fn compute_cost_with_properties(
    op: &Operator,
    own_stats: &Statistics,
    child_stats: &[&Statistics],
    child_outputs: &[&PhysicalPropertySet],
    alt_kind: &PropertyAlternativeKind,
    options: &CostOptions,
) -> Cost {
    let required_output = PhysicalPropertySet::any();
    let input = CostInput {
        op,
        own_stats,
        child_stats,
        child_outputs,
        required_output: &required_output,
        alt_kind,
        scalars: None,
        options,
    };
    compute_cost_from_input(&input)
}
```

- [ ] **Step 5: Run search and cost tests**

```bash
cargo test --lib search_uses_cost_estimate_total_for_winner_cost
cargo test --lib child_output_aware_shuffle_join_does_not_charge_network_exchange_twice
cargo test --lib broadcast_gate_rejects_fallback_build_above_fallback_limit
cargo test --lib top_n_cheaper_than_sort_for_small_limit
```

Expected: all pass.

- [ ] **Step 6: Commit Task 5**

```bash
git add src/sql/optimizer/search.rs src/sql/optimizer/cost.rs
git commit -m "feat(optimizer): route search through dimensional cost"
```

Expected: commit includes only the two listed files.

## Task 6: EXPLAIN COSTS Propagation

**Files:**
- Modify: `src/sql/planner/distributed_node.rs`
- Modify: `src/sql/planner/distributed_build.rs`
- Modify: `src/sql/codegen/ir/explain.rs`

- [ ] **Step 1: Add failing PlanNodeStats and explain tests**

In `src/sql/planner/distributed_node.rs`, add:

```rust
#[test]
fn plan_node_stats_can_carry_cost_estimate() {
    let stats = Statistics {
        output_row_count: 7.0,
        ..Default::default()
    };
    let cost = crate::sql::optimizer::statistics::CostEstimate {
        cpu_cost: 1.0,
        memory_cost: 2.0,
        network_cost: 3.0,
    };
    let s = PlanNodeStats::from_statistics_with_cost(&stats, Some(cost.clone()));

    assert_eq!(s.cost_estimate.unwrap().network_cost, 3.0);
}
```

In `src/sql/codegen/ir/explain.rs`, add a focused test near the existing cost explain tests:

```rust
#[test]
fn costs_level_renders_dimensional_costs() {
    let mut dp = build_distributed_plan(&scan_plan()).expect("build DistributedPlan");
    let root = dp
        .fragments
        .iter_mut()
        .find(|fragment| fragment.fragment_id == dp.root_fragment_id)
        .expect("root fragment");
    root.root.stats.cost_estimate = Some(crate::sql::optimizer::statistics::CostEstimate {
        cpu_cost: 10.0,
        memory_cost: 2.0,
        network_cost: 3.0,
    });

    let costs = explain_distributed_plan(&dp, ExplainLevel::Costs).join("\n");
    assert!(costs.contains("cost={cpu=10"));
    assert!(costs.contains("memory=2"));
    assert!(costs.contains("network=3"));
    assert!(costs.contains("total="));
}
```

- [ ] **Step 2: Run the failing tests**

```bash
cargo test --lib plan_node_stats_can_carry_cost_estimate
cargo test --lib costs_level_renders_dimensional_costs
```

Expected: fail because `cost_estimate` and `from_statistics_with_cost` do not exist.

- [ ] **Step 3: Extend PlanNodeStats**

Update imports:

```rust
use crate::sql::optimizer::statistics::{ColumnStatistic, Confidence, CostEstimate, Statistics};
```

Change `PlanNodeStats`:

```rust
pub(crate) struct PlanNodeStats {
    pub output_row_count: f64,
    pub row_count_confidence: Confidence,
    pub column_statistics: HashMap<ColumnId, ColumnStatistic>,
    pub cost_estimate: Option<CostEstimate>,
}
```

Update constructors:

```rust
    pub fn from_statistics(stats: &Statistics) -> Self {
        Self::from_statistics_with_cost(stats, None)
    }

    pub fn from_statistics_with_cost(stats: &Statistics, cost_estimate: Option<CostEstimate>) -> Self {
        Self {
            output_row_count: stats.output_row_count,
            row_count_confidence: stats.row_count_confidence,
            column_statistics: stats.column_statistics.clone(),
            cost_estimate,
        }
    }
```

- [ ] **Step 4: Compute cost in distributed build**

In `src/sql/planner/distributed_build.rs`, add imports:

```rust
use crate::sql::optimizer::cost::{CostInput, CostOptions, compute_cost_estimate};
use crate::sql::optimizer::derive::PropertyAlternativeKind;
use crate::sql::optimizer::property::PhysicalPropertySet;
use crate::sql::optimizer::statistics::Statistics;
```

Add helper functions near the visitor implementation:

```rust
fn stats_for_physical_node(node: &PhysicalPlanNode) -> PlanNodeStats {
    let child_stats: Vec<&Statistics> = node.children.iter().map(|child| &child.stats).collect();
    let child_outputs: Vec<&PhysicalPropertySet> = node
        .execution_props
        .child_output_properties
        .iter()
        .collect();
    let options = CostOptions::default();
    let alt_kind = match node.execution_props.join_distribution {
        Some(crate::sql::optimizer::physical_plan::JoinExecutionDistribution::Broadcast) => {
            PropertyAlternativeKind::BroadcastJoin
        }
        Some(crate::sql::optimizer::physical_plan::JoinExecutionDistribution::Partitioned) => {
            PropertyAlternativeKind::ShuffleJoin
        }
        _ => PropertyAlternativeKind::Default,
    };
    let input = CostInput {
        op: &node.op,
        own_stats: &node.stats,
        child_stats: &child_stats,
        child_outputs: &child_outputs,
        required_output: &node.execution_props.output_property,
        alt_kind: &alt_kind,
        scalars: node.execution_props.scalar_arena.as_deref(),
        options: &options,
    };
    PlanNodeStats::from_statistics_with_cost(&node.stats, Some(compute_cost_estimate(&input)))
}
```

Replace `PlanNodeStats::from_statistics(&node.stats)` in `distributed_build.rs` with `stats_for_physical_node(node)` when the stats belong to the current physical node. For places that intentionally copy parent stats into a rewritten child, keep `PlanNodeStats::from_statistics(&node.stats)` until the surrounding helper can identify the correct physical operator.

- [ ] **Step 5: Render cost suffix**

In `src/sql/codegen/ir/explain.rs`, update `costs_suffix`:

```rust
fn costs_suffix(stats: &PlanNodeStats, level: ExplainLevel) -> String {
    if matches!(level, ExplainLevel::Costs) {
        let row_part = format!("rows={:.0}", stats.output_row_count);
        let cost_part = stats
            .cost_estimate
            .as_ref()
            .map(format_cost_estimate)
            .unwrap_or_default();
        let colstats = format_column_stats_costs(stats);
        match (cost_part.is_empty(), colstats.is_empty()) {
            (true, true) => format!(" ({row_part})"),
            (false, true) => format!(" ({row_part} {cost_part})"),
            (true, false) => format!(" ({row_part}) {colstats}"),
            (false, false) => format!(" ({row_part} {cost_part}) {colstats}"),
        }
    } else {
        String::new()
    }
}

fn format_cost_estimate(cost: &crate::sql::optimizer::statistics::CostEstimate) -> String {
    let options = crate::sql::optimizer::cost::CostOptions::default();
    format!(
        "cost={{cpu={} memory={} network={} total={}}}",
        fmt_f64(cost.cpu_cost),
        fmt_f64(cost.memory_cost),
        fmt_f64(cost.network_cost),
        fmt_f64(cost.total_with_options(&options)),
    )
}
```

- [ ] **Step 6: Run focused tests**

```bash
cargo test --lib plan_node_stats_can_carry_cost_estimate
cargo test --lib costs_level_renders_dimensional_costs
cargo test --lib costs_renders_colstats_from_ir_stats_only_at_costs_level
```

Expected: all pass. Existing cost explain output should keep `rows=` and `colstats=` stable.

- [ ] **Step 7: Commit Task 6**

```bash
git add src/sql/planner/distributed_node.rs src/sql/planner/distributed_build.rs src/sql/codegen/ir/explain.rs
git commit -m "feat(optimizer): expose dimensional costs in explain"
```

Expected: commit includes only the three listed files.

## Task 7: Optimizer Plan Golden Coverage

**Files:**
- Add: `sql-tests/optimizer/sql/cost_model_explain.sql`

- [ ] **Step 1: Inspect optimizer SQL test conventions**

Run:

```bash
ls sql-tests/optimizer
rg -n "@explain_contains|EXPLAIN COSTS|broadcast|shuffle|TOP-N|SORT" sql-tests/optimizer
```

Expected: find existing optimizer plan-shape cases and annotation style.

- [ ] **Step 2: Add cost model explain cases**

Create `sql-tests/optimizer/sql/cost_model_explain.sql` with this content:

```sql
-- @tags=optimizer,cost-model,explain
-- Test Objective:
-- Lock in dimensional CBO cost output for scan/filter, join, and TopN plans.
DROP TABLE IF EXISTS ${case_db}.cost_model_l;
DROP TABLE IF EXISTS ${case_db}.cost_model_r;
CREATE TABLE ${case_db}.cost_model_l (k INT, v INT);
CREATE TABLE ${case_db}.cost_model_r (k INT, w INT);
INSERT INTO ${case_db}.cost_model_l
    SELECT generate_series, generate_series * 10
    FROM TABLE(generate_series(1, 1000));
INSERT INTO ${case_db}.cost_model_r
    SELECT generate_series, generate_series * 20
    FROM TABLE(generate_series(1, 100));
ANALYZE TABLE ${case_db}.cost_model_l;
ANALYZE TABLE ${case_db}.cost_model_r;

-- @explain_contains=cost={cpu=
-- @explain_contains=memory=
-- @explain_contains=network=
-- @explain_contains=total=
EXPLAIN COSTS
SELECT * FROM ${case_db}.cost_model_l WHERE v > 10;

-- @explain_contains=HASH JOIN
-- @explain_contains=cost={cpu=
EXPLAIN COSTS
SELECT l.k, l.v, r.w
FROM ${case_db}.cost_model_l l
JOIN ${case_db}.cost_model_r r ON l.k = r.k;

-- @explain_contains=TOP-N
-- @explain_contains=cost={cpu=
EXPLAIN COSTS
SELECT * FROM ${case_db}.cost_model_l ORDER BY v DESC LIMIT 10;
```

- [ ] **Step 3: Run optimizer suite focused on the new file**

Use the generated SQL-test config if present:

```bash
if [ -f docker/iceberg-rest/runtime/current/env.sh ]; then
  source docker/iceberg-rest/runtime/current/env.sh
  cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
    --config "$NOVAROCKS_SQL_TEST_CONFIG" \
    --suite optimizer --only cost_model_explain --mode verify
else
  cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
    --suite optimizer --only cost_model_explain --mode verify
fi
```

Expected: pass after table names and expected substrings match the existing optimizer fixture.

- [ ] **Step 4: Commit Task 7**

```bash
git add sql-tests/optimizer/sql/cost_model_explain.sql
git commit -m "test(optimizer): cover cost explain output"
```

Expected: commit includes only the new SQL test file.

## Task 8: Join Reorder Proxy Alignment

**Files:**
- Modify: `src/sql/optimizer/cascades_rules/multi_join_reorder/algo.rs`

- [ ] **Step 1: Dirty-file guard**

Run:

```bash
git status --short -- src/sql/optimizer/cascades_rules/multi_join_reorder/algo.rs
git diff -- src/sql/optimizer/cascades_rules/multi_join_reorder/algo.rs
```

Expected: if the file is dirty before this task, stop and report the diff summary before editing. Do not stage or commit pre-existing changes with this task.

- [ ] **Step 2: Add failing proxy-cost tests**

Add these tests inside the existing `mod tests`:

```rust
#[test]
fn join_self_cost_penalizes_cross_join_above_equi_join() {
    let left = atom_stats(0, 10_000.0, 10_000.0);
    let right = atom_stats(1, 10_000.0, 10_000.0);
    let output = atom_stats(2, 1_000.0, 1_000.0);

    let equi_cost = join_self_cost(&left, &right, &output, JoinKind::Inner);
    let cross_cost = join_self_cost(&left, &right, &output, JoinKind::Cross);

    assert!(cross_cost > equi_cost * 10.0);
}

#[test]
fn join_self_cost_accounts_for_output_size() {
    let left = atom_stats(0, 10_000.0, 10_000.0);
    let right = atom_stats(1, 10_000.0, 10_000.0);
    let small_output = atom_stats(2, 100.0, 100.0);
    let large_output = atom_stats(3, 100_000.0, 100_000.0);

    assert!(
        join_self_cost(&left, &right, &large_output, JoinKind::Inner)
            > join_self_cost(&left, &right, &small_output, JoinKind::Inner)
    );
}
```

- [ ] **Step 3: Run the tests**

```bash
cargo test --lib join_self_cost_penalizes_cross_join_above_equi_join
cargo test --lib join_self_cost_accounts_for_output_size
```

Expected: pass if the current proxy already satisfies these properties; fail if the proxy constants need alignment.

- [ ] **Step 4: Align proxy naming with `CostOptions`**

If Step 3 fails or if the current code still has hard-coded unexplained constants, update `join_self_cost` to use local names that mirror `CostOptions`:

```rust
const REORDER_CROSS_CPU_FACTOR: f64 = 2.0;
const REORDER_CROSS_MEMORY_FACTOR: f64 = 200.0;
const REORDER_HASH_BUILD_FACTOR: f64 = 1.0;
const REORDER_OUTPUT_FACTOR: f64 = 1.0;
```

Then rewrite the non-cross branch:

```rust
let right_rows = right.output_row_count.max(1.0);
let probe_penalty = (right_rows / 100_000.0).ln().clamp(1.0, 12.0);
CostEstimate {
    cpu_cost: finite_cost(
        right.compute_size() * REORDER_HASH_BUILD_FACTOR
            + left.compute_size() * probe_penalty
            + output.compute_size() * REORDER_OUTPUT_FACTOR,
    ),
    memory_cost: finite_cost(right.compute_size()),
    network_cost: 0.0,
}
```

Keep the proxy local to `algo.rs`. Do not make it read `child_outputs` or `required_output`.

- [ ] **Step 5: Run join reorder tests**

```bash
cargo test --lib join_self_cost_penalizes_cross_join_above_equi_join
cargo test --lib join_self_cost_accounts_for_output_size
cargo test --lib enumerate_orders_produces_candidates_and_dedups
cargo test --lib greedy_produces_bushy_orders
```

Expected: all pass.

- [ ] **Step 6: Commit Task 8 if dirty-file guard allows it**

If Step 1 showed a clean file before editing:

```bash
git add src/sql/optimizer/cascades_rules/multi_join_reorder/algo.rs
git commit -m "refactor(optimizer): align join reorder proxy cost"
```

If Step 1 showed pre-existing dirty changes, do not commit this file. Report the task output and ask whether to absorb the existing diff into this feature or split it first.

## Task 9: Final Validation

**Files:**
- No planned source edits unless validation exposes a failure.

- [ ] **Step 1: Run formatter**

```bash
cargo fmt
```

Expected: exit 0.

- [ ] **Step 2: Run focused optimizer cost tests**

```bash
cargo test --lib cost_estimate_weighted_total_uses_explicit_weights
cargo test --lib compute_cost_estimate_returns_dimensions_for_scan
cargo test --lib filter_cost_uses_input_rows_not_output_rows
cargo test --lib broadcast_join_estimate_charges_backend_fanout
cargo test --lib shuffle_join_estimate_charges_both_sides_network
cargo test --lib costs_level_renders_dimensional_costs
```

Expected: all pass.

- [ ] **Step 3: Run broader optimizer unit tests**

```bash
cargo test --lib sql::optimizer::cost
cargo test --lib sql::optimizer::search
cargo test --lib sql::optimizer::derive
```

Expected: all pass. If `cargo test --lib sql::optimizer::cost` does not match tests in this repo, run `cargo test --lib cost` and record the exact command that matched.

- [ ] **Step 4: Run optimizer SQL explain case**

```bash
if [ -f docker/iceberg-rest/runtime/current/env.sh ]; then
  source docker/iceberg-rest/runtime/current/env.sh
  cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
    --config "$NOVAROCKS_SQL_TEST_CONFIG" \
    --suite optimizer --only cost_model_explain --mode verify
else
  cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
    --suite optimizer --only cost_model_explain --mode verify
fi
```

Expected: pass.

- [ ] **Step 5: Inspect final diff**

```bash
git status --short --branch
git log --oneline -n 8
```

Expected: only intentionally uncommitted pre-existing user files remain dirty. All cost model implementation files from completed tasks are committed.

- [ ] **Step 6: Completion summary**

Report:

- commits created,
- tests run with pass/fail status,
- any dirty files that were intentionally left untouched,
- any plan task that was blocked by pre-existing local changes.
