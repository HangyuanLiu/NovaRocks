# Runtime Filter StarRocks Alignment Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 对齐 NovaRocks standalone runtime filter 与 StarRocks 的 exchange crossing、descriptor、distributed merge/broadcast 语义，并用独立 1FE+3BE NovaRocks 部署验收。

**Architecture:** optimizer 负责只在 StarRocks 安全规则允许时把 probe RF 推过 exchange；fragment/codegen 保证 RF thrift descriptor 携带 `build_join_mode`、remote target 和 layout；coordinator 以 descriptor 为权威计算 builder number；runtime worker 按 builder number merge partial filter 后广播 final filter。验收只使用 SQL runner `cross-process` 模式启动 1 个 NovaRocks FE 和 3 个 NovaRocks BE。

**Tech Stack:** Rust, NovaRocks standalone optimizer, thrift `TRuntimeFilterDescription` / `TRuntimeFilterParams`, tonic/gRPC runtime filter RPC, SQL test runner cross-process cluster.

**Spec:** `docs/superpowers/specs/2026-06-13-runtime-filter-starrocks-alignment-design.md`

---

## Scope Check

本计划覆盖一个端到端系统能力：optimizer placement、fragment descriptor、coordinator params、runtime merge/delivery、1FE+3BE SQL 验收。这些步骤强依赖同一个 RF descriptor contract，不能拆成互不相关的 PR；但每个 task 都能单独红测、实现、复测。

当前工作区已有探索性 diff。执行本计划前先由用户决定保留还是回滚该 diff；如果保留，仍要按下面的测试步骤证明每个行为。

## File Structure

- Modify `src/sql/optimizer/runtime_filter_pass.rs`
  实现 StarRocks-compatible exchange crossing，并用单键、多键、禁用开关测试固定语义。
- Modify `src/sql/optimizer/options.rs`
  更新 `allow_cross_exchange_rf` 的语义说明。
- Modify `src/sql/codegen/fragment_builder.rs`
  补 RF descriptor lower 的覆盖测试，确认 `build_join_mode`、`has_remote_targets`、`layout` 与执行分布一致。
- Modify `src/runtime/coordinator.rs`
  用 `RuntimeFilterPlanResult.all_filters[filter_id].build_join_mode` 计算 builder number。
- Modify `src/runtime/runtime_filter_worker.rs`
  补 `expected_builders` 和 duplicate partial 行为测试。
- No code change expected in `src/exec/operators/hashjoin/hash_join_build_sink.rs` unless tests prove send path still violates spec.
- Use `tests/sql-test-runner/src/cluster.rs` as the acceptance harness; do not modify it unless 1FE+3BE orchestration itself is broken.

---

### Task 1: Optimizer Exchange Crossing Red Tests

**Files:**
- Modify: `src/sql/optimizer/runtime_filter_pass.rs`
- Test: `src/sql/optimizer/runtime_filter_pass.rs`

- [ ] **Step 1: Add a helper that builds a two-key shuffle join with one partition key**

In `test_support`, add `leaf_many` and `two_key_shuffle_join_with_single_partition_probe_exchange`:

```rust
fn leaf_many(rows: f64, output_columns: Vec<OutputColumn>) -> PhysicalPlanNode {
    PhysicalPlanNode {
        op: Operator::PhysicalValues(PhysicalValuesOp {
            rows: vec![],
            columns: vec![],
        }),
        children: vec![],
        stats: Statistics {
            output_row_count: rows,
            column_statistics: Default::default(),
            ..Default::default()
        },
        output_columns,
        execution_props: crate::sql::optimizer::physical_plan::PlanExecutionProps::default(),
        build_runtime_filters: vec![],
        probe_runtime_filters: vec![],
    }
}

pub(crate) fn two_key_shuffle_join_with_single_partition_probe_exchange() -> PhysicalPlanNode {
    use crate::sql::optimizer::operator::PhysicalDistributionOp;
    use crate::sql::optimizer::property::{DistributionSpec, HashSource};
    let (l1, l1_expr) = col(1, "l1");
    let (l2, l2_expr) = col(2, "l2");
    let (r1, r1_expr) = col(3, "r1");
    let (r2, r2_expr) = col(4, "r2");
    let scan = leaf_many(1_000_000.0, vec![l1.clone(), l2.clone()]);
    let exch = PhysicalPlanNode {
        op: Operator::PhysicalDistribution(PhysicalDistributionOp {
            spec: DistributionSpec::HashPartitioned {
                cols: vec![l1.column_id],
                source: HashSource::ShuffleJoin,
            },
        }),
        children: vec![scan],
        stats: Statistics {
            output_row_count: 1_000_000.0,
            row_count_confidence: crate::sql::optimizer::statistics::Confidence::Estimated,
            column_statistics: Default::default(),
            ..Default::default()
        },
        output_columns: vec![l1.clone(), l2.clone()],
        execution_props: crate::sql::optimizer::physical_plan::PlanExecutionProps::default(),
        build_runtime_filters: vec![],
        probe_runtime_filters: vec![],
    };
    let build = leaf_many(100.0, vec![r1.clone(), r2.clone()]);
    PhysicalPlanNode {
        op: Operator::PhysicalHashJoin(PhysicalHashJoinOp {
            join_type: JoinKind::Inner,
            eq_conditions: vec![
                PhysicalHashJoinEqCondition {
                    left: l1_expr,
                    right: r1_expr,
                    null_safe: false,
                },
                PhysicalHashJoinEqCondition {
                    left: l2_expr,
                    right: r2_expr,
                    null_safe: false,
                },
            ],
            other_condition: None,
            distribution: JoinDistribution::Shuffle,
        }),
        children: vec![exch, build],
        stats: Statistics {
            output_row_count: 100.0,
            row_count_confidence: crate::sql::optimizer::statistics::Confidence::Estimated,
            column_statistics: Default::default(),
            ..Default::default()
        },
        output_columns: vec![l1, l2, r1, r2],
        execution_props: crate::sql::optimizer::physical_plan::PlanExecutionProps::default(),
        build_runtime_filters: vec![],
        probe_runtime_filters: vec![],
    }
}
```

- [ ] **Step 2: Add red tests for StarRocks crossing rules**

Add these tests:

```rust
#[test]
fn partitioned_rf_crosses_hash_exchange_for_single_key_when_flag_enabled() {
    let mut j = super::test_support::shuffle_join_with_probe_exchange();
    let mut opts = OptimizerOptions::default_settings();
    opts.allow_cross_exchange_rf = true;

    annotate(&mut j, &opts);

    assert_eq!(j.build_runtime_filters.len(), 1, "build RF expected");
    let exch = &j.children[0];
    assert!(exch.probe_runtime_filters.is_empty());
    assert_eq!(exch.children[0].probe_runtime_filters.len(), 1);
}

#[test]
fn partitioned_rf_crosses_exchange_only_for_matching_partition_key() {
    let mut j =
        super::test_support::two_key_shuffle_join_with_single_partition_probe_exchange();
    let mut opts = OptimizerOptions::default_settings();
    opts.allow_cross_exchange_rf = true;

    annotate(&mut j, &opts);

    assert_eq!(j.build_runtime_filters.len(), 2, "two build RFs expected");
    let exch = &j.children[0];
    assert!(exch.probe_runtime_filters.is_empty());
    let scan_filters = &exch.children[0].probe_runtime_filters;
    assert_eq!(scan_filters.len(), 1);
    assert_eq!(scan_filters[0].filter_id, j.build_runtime_filters[0].filter_id);
}

#[test]
fn probe_stays_within_fragment_when_cross_exchange_disabled() {
    let mut j = super::test_support::shuffle_join_with_probe_exchange();
    let mut opts = OptimizerOptions::default_settings();
    opts.allow_cross_exchange_rf = false;

    annotate(&mut j, &opts);

    assert_eq!(j.build_runtime_filters.len(), 1);
    let exch = &j.children[0];
    assert!(exch.probe_runtime_filters.is_empty());
    assert!(exch.children[0].probe_runtime_filters.is_empty());
}
```

- [ ] **Step 3: Run red tests**

Run:

```bash
cargo test -q sql::optimizer::runtime_filter_pass::tests::partitioned_rf_crosses_hash_exchange_for_single_key_when_flag_enabled
cargo test -q sql::optimizer::runtime_filter_pass::tests::partitioned_rf_crosses_exchange_only_for_matching_partition_key
```

Expected before implementation: at least one test fails because partitioned RF does not cross the exchange to the scan.

---

### Task 2: Implement StarRocks-Compatible Probe Push Policy

**Files:**
- Modify: `src/sql/optimizer/runtime_filter_pass.rs`
- Modify: `src/sql/optimizer/options.rs`
- Test: `src/sql/optimizer/runtime_filter_pass.rs`

- [ ] **Step 1: Replace build-complete-only crossing policy**

In `src/sql/optimizer/runtime_filter_pass.rs`, replace the old `ProbePushPolicy` with:

```rust
#[derive(Clone, Debug)]
struct ProbePushPolicy {
    allow_cross_exchange: bool,
    distribution: JoinDistribution,
    equal_count: usize,
}
```

- [ ] **Step 2: Add StarRocks-compatible `can_cross_exchange`**

Add:

```rust
fn probe_matches_single_partition_column(
    probe: &RuntimeFilterProbe,
    partition_col: ColumnId,
) -> bool {
    let ids = column_id_vec(&probe.probe_expr);
    ids.len() == 1 && ids[0] == partition_col
}

fn can_cross_exchange(
    node: &PhysicalPlanNode,
    probe: &RuntimeFilterProbe,
    policy: &ProbePushPolicy,
) -> bool {
    if !policy.allow_cross_exchange {
        return false;
    }
    let Operator::PhysicalDistribution(op) = &node.op else {
        return false;
    };
    if matches!(policy.distribution, JoinDistribution::Broadcast) || policy.equal_count == 1 {
        return true;
    }

    let crate::sql::optimizer::property::DistributionSpec::HashPartitioned { cols, .. } = &op.spec
    else {
        return false;
    };
    cols.len() == 1 && probe_matches_single_partition_column(probe, cols[0])
}
```

- [ ] **Step 3: Thread policy by reference through `push_probe_down`**

Change the signature:

```rust
fn push_probe_down(
    node: &mut PhysicalPlanNode,
    probe: &RuntimeFilterProbe,
    policy: &ProbePushPolicy,
) -> bool
```

At the start of the function, use:

```rust
if can_cross_exchange(node, probe, policy) {
    if let Some(child) = node.children.first_mut() {
        return push_probe_down(child, probe, policy);
    }
    return false;
}
```

- [ ] **Step 4: Build the policy from join distribution and eq count**

In `annotate_node`, create:

```rust
let policy = ProbePushPolicy {
    allow_cross_exchange: options.allow_cross_exchange_rf,
    distribution: distribution.clone(),
    equal_count: eq_conditions.len(),
};
```

Call:

```rust
let _ = push_probe_down(&mut node.children[sides.probe_child], &probe, &policy);
```

- [ ] **Step 5: Update option docs**

In `src/sql/optimizer/options.rs`, update `allow_cross_exchange_rf` docs:

```rust
/// Whether probe runtime filters may be placed across exchange boundaries
/// using StarRocks-compatible safety rules. Broadcast and single-key RFs
/// may cross; multi-key partitioned RFs cross only when the probe key
/// matches the single partition column. Probe pushdown still stops at
/// outer/anti/null-preserving semantic boundaries.
pub allow_cross_exchange_rf: bool,
```

- [ ] **Step 6: Run optimizer tests**

Run:

```bash
cargo test -q sql::optimizer::runtime_filter_pass::tests::
```

Expected: all runtime filter optimizer tests pass.

- [ ] **Step 7: Commit**

```bash
git add src/sql/optimizer/runtime_filter_pass.rs src/sql/optimizer/options.rs
git commit -m "Align runtime filter exchange crossing with StarRocks"
```

---

### Task 3: Verify Fragment Descriptor Uses Execution Distribution

**Files:**
- Modify: `src/sql/codegen/fragment_builder.rs`
- Test: `src/sql/codegen/fragment_builder.rs`

- [ ] **Step 1: Add descriptor assertions for partitioned RF**

In the existing `runtime_filter_uses_execution_distribution_metadata` test, assert all descriptor fields:

```rust
assert_eq!(
    rf.build_join_mode,
    Some(crate::runtime_filter::TRuntimeFilterBuildJoinMode::PARTITIONED)
);
assert_eq!(rf.has_remote_targets, Some(true));
assert_eq!(
    rf.layout.as_ref().and_then(|layout| layout.global_layout),
    Some(crate::runtime_filter::TRuntimeFilterLayoutMode::GLOBAL_SHUFFLE_1L)
);
assert!(
    rf.plan_node_id_to_target_expr
        .as_ref()
        .is_some_and(|targets| !targets.is_empty()),
    "remote RF must retain probe target expressions"
);
```

- [ ] **Step 2: Add descriptor assertions for broadcast RF**

In `runtime_filter_unknown_without_execution_metadata_falls_back_to_broadcast`, assert:

```rust
assert_eq!(
    rf.build_join_mode,
    Some(crate::runtime_filter::TRuntimeFilterBuildJoinMode::BORADCAST)
);
assert_eq!(
    rf.layout.as_ref().and_then(|layout| layout.global_layout),
    Some(crate::runtime_filter::TRuntimeFilterLayoutMode::SINGLETON)
);
```

- [ ] **Step 3: Run targeted codegen tests**

Run:

```bash
cargo test -q sql::codegen::fragment_builder::tests::runtime_filter_uses_execution_distribution_metadata
cargo test -q sql::codegen::fragment_builder::tests::runtime_filter_unknown_without_execution_metadata_falls_back_to_broadcast
```

Expected: both tests pass. If `has_remote_targets` is false for a genuinely remote target, inspect `record_probe_targets` and `build_rf_descriptors` before changing runtime code.

- [ ] **Step 4: Commit**

```bash
git add src/sql/codegen/fragment_builder.rs
git commit -m "Test runtime filter descriptor distribution metadata"
```

---

### Task 4: Coordinator Builder Number by Join Mode

**Files:**
- Modify: `src/runtime/coordinator.rs`
- Test: `src/runtime/coordinator.rs`

- [ ] **Step 1: Add red test**

Add this test inside `src/runtime/coordinator.rs` test module:

```rust
#[test]
fn runtime_filter_builder_number_follows_join_distribution() {
    let mut all_filters = std::collections::HashMap::new();
    all_filters.insert(
        10,
        runtime_filter::TRuntimeFilterDescription {
            filter_id: Some(10),
            build_expr: None,
            expr_order: None,
            plan_node_id_to_target_expr: None,
            has_remote_targets: Some(true),
            bloom_filter_size: None,
            runtime_filter_merge_nodes: None,
            build_join_mode: Some(runtime_filter::TRuntimeFilterBuildJoinMode::BORADCAST),
            sender_finst_id: None,
            build_plan_node_id: None,
            broadcast_grf_senders: None,
            broadcast_grf_destinations: None,
            bucketseq_to_instance: None,
            plan_node_id_to_partition_by_exprs: None,
            filter_type: None,
            layout: None,
            build_from_group_execution: None,
            is_broad_cast_join_in_skew: None,
            skew_shuffle_filter_id: None,
            is_asc: None,
            is_nulls_first: None,
            limit: None,
        },
    );
    all_filters.insert(
        20,
        runtime_filter::TRuntimeFilterDescription {
            filter_id: Some(20),
            build_expr: None,
            expr_order: None,
            plan_node_id_to_target_expr: None,
            has_remote_targets: Some(true),
            bloom_filter_size: None,
            runtime_filter_merge_nodes: None,
            build_join_mode: Some(runtime_filter::TRuntimeFilterBuildJoinMode::PARTITIONED),
            sender_finst_id: None,
            build_plan_node_id: None,
            broadcast_grf_senders: None,
            broadcast_grf_destinations: None,
            bucketseq_to_instance: None,
            plan_node_id_to_partition_by_exprs: None,
            filter_type: None,
            layout: None,
            build_from_group_execution: None,
            is_broad_cast_join_in_skew: None,
            skew_shuffle_filter_id: None,
            is_asc: None,
            is_nulls_first: None,
            limit: None,
        },
    );

    let rf_plan = RuntimeFilterPlanResult {
        all_filters,
        build_side_filters: std::collections::HashMap::from([(3, vec![10, 20])]),
        probe_side_filters: std::collections::HashMap::new(),
    };
    let params = build_instance_runtime_filter_params(
        &rf_plan,
        &BTreeMap::new(),
        &BTreeMap::from([(3, 3)]),
    );
    let builder_number = params
        .runtime_filter_builder_number
        .as_ref()
        .expect("builder number map");

    assert_eq!(builder_number.get(&10), Some(&1));
    assert_eq!(builder_number.get(&20), Some(&3));
}
```

- [ ] **Step 2: Run red test**

Run:

```bash
cargo test -q runtime::coordinator::tests::runtime_filter_builder_number_follows_join_distribution
```

Expected before implementation: FAIL with broadcast builder number reported as build fragment instance count.

- [ ] **Step 3: Implement builder number selection**

In `build_instance_runtime_filter_params`, compute:

```rust
let fragment_builders = instance_counts
    .get(build_frag_id)
    .map(|&n| n as i32)
    .unwrap_or(1);
for fid in filter_ids {
    let n_builders = rf_plan
        .all_filters
        .get(fid)
        .and_then(|desc| desc.build_join_mode)
        .map(|mode| match mode {
            runtime_filter::TRuntimeFilterBuildJoinMode::BORADCAST => 1,
            _ => fragment_builders,
        })
        .unwrap_or(fragment_builders);
    builder_number.insert(*fid, n_builders);
}
```

- [ ] **Step 4: Run green test**

Run:

```bash
cargo test -q runtime::coordinator::tests::runtime_filter_builder_number_follows_join_distribution
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/runtime/coordinator.rs
git commit -m "Fix distributed runtime filter builder counts"
```

---

### Task 5: Runtime Worker Builder Count and Duplicate Partial Tests

**Files:**
- Modify: `src/runtime/runtime_filter_worker.rs`
- Test: `src/runtime/runtime_filter_worker.rs`

- [ ] **Step 1: Add `expected_builders` test**

Add a test module at the end of `src/runtime/runtime_filter_worker.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::exec::pipeline::dependency::DependencyManager;
    use std::collections::BTreeMap;
    use std::sync::Arc;

    fn worker_with_builder_numbers(
        builder_number: Option<BTreeMap<i32, i32>>,
    ) -> RuntimeFilterWorker {
        RuntimeFilterWorker::new(
            crate::runtime::query_context::QueryId { hi: 1, lo: 2 },
            runtime_filter::TRuntimeFilterParams {
                id_to_prober_params: None,
                runtime_filter_builder_number: builder_number,
                runtime_filter_max_size: Some(16_i64 * 1024 * 1024),
                skew_join_runtime_filters: None,
            },
            Arc::new(RuntimeFilterHub::new(DependencyManager::new())),
        )
    }

    #[test]
    fn expected_builders_defaults_to_one_and_respects_params() {
        let worker = worker_with_builder_numbers(None);
        assert_eq!(worker.expected_builders(7), 1);

        let worker = worker_with_builder_numbers(Some(BTreeMap::from([(7, 3), (8, 0)])));
        assert_eq!(worker.expected_builders(7), 3);
        assert_eq!(worker.expected_builders(8), 1);
        assert_eq!(worker.expected_builders(9), 1);
    }
}
```

- [ ] **Step 2: Run test**

Run:

```bash
cargo test -q runtime::runtime_filter_worker::tests::expected_builders_defaults_to_one_and_respects_params
```

Expected: PASS. If it fails to construct `RuntimeFilterHub`, use the existing constructor signature in `src/runtime/runtime_filter_hub.rs` and keep the assertions identical.

- [ ] **Step 3: Inspect send path before changing it**

Run:

```bash
rg -n "send_runtime_filters_remote|runtime_filter_merge_nodes|build_be_number|is_partial" src/exec/operators/hashjoin/hash_join_build_sink.rs src/runtime/runtime_filter_worker.rs src/service/internal_rpc.rs
```

Expected finding:

```text
HashJoinBuildSink sends partial filters when merge_nodes is non-empty.
RuntimeFilterWorker deduplicates by build_be_number.
Internal RPC delivers pending remote filters into QueryContext when needed.
```

If the finding is different, stop and update the spec before implementation.

- [ ] **Step 4: Commit**

```bash
git add src/runtime/runtime_filter_worker.rs
git commit -m "Test runtime filter merge builder counts"
```

---

### Task 6: 1FE+3BE Fast-Fail SQL Acceptance

**Files:**
- No source files should be modified in this task.
- Logs are generated under SQL runner runtime directories and `target/`.

- [ ] **Step 1: Build the dev-opt binary**

Run:

```bash
cargo fmt --check
cargo build --profile dev-opt
```

Expected: both commands pass. Existing warnings are acceptable only if the command exits 0.

- [ ] **Step 2: Source the generated Iceberg REST runtime**

Run:

```bash
source docker/iceberg-rest/runtime/current/env.sh
printf '%s\n' "$NOVAROCKS_SQL_TEST_CONFIG"
```

Expected: prints an absolute `sql-test.conf` path under `/Users/harbor/project/NovaRocks/docker/iceberg-rest/runtime/`.

- [ ] **Step 3: Run TPC-DS q72 in independent 1FE+3BE mode**

Run:

```bash
NO_PROXY=127.0.0.1,localhost \
NOVAROCKS_BIN="$PWD/target/dev-opt/novarocks" \
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests --profile dev-opt -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite tpc-ds --only q72 \
  --mode verify \
  --query-timeout 180 \
  --cluster-mode cross-process \
  --cluster-size 3 \
  --fail-fast \
  -j 1
```

Expected output contains:

```text
started cross-process BE[0]
started cross-process BE[1]
started cross-process BE[2]
started cross-process FE
PASS
```

- [ ] **Step 4: Run benchmark suites fast-fail**

Run:

```bash
for suite in ssb tpc-h tpc-ds; do
  NO_PROXY=127.0.0.1,localhost \
  NOVAROCKS_BIN="$PWD/target/dev-opt/novarocks" \
  cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests --profile dev-opt -- \
    --config "$NOVAROCKS_SQL_TEST_CONFIG" \
    --suite "$suite" \
    --mode verify \
    --query-timeout 180 \
    --cluster-mode cross-process \
    --cluster-size 3 \
    --fail-fast \
    -j 1 || exit 1
done
```

Expected: `ssb`, `tpc-h`, and `tpc-ds` all pass. If any suite fails, stop immediately, capture the failing suite/case/log, and discuss the fix direction before editing.

- [ ] **Step 5: Check for leftover processes from this run**

Run:

```bash
pgrep -af "target/dev-opt/novarocks.*standalone-server|novarocks.*standalone-server" || true
```

Expected: no process from the just-finished SQL runner remains. Do not kill unrelated standalone-server processes from other worktrees.

- [ ] **Step 6: Final commit after user approval**

Only after the user approves the implementation result, run:

```bash
git status --short
git add src/sql/optimizer/runtime_filter_pass.rs src/sql/optimizer/options.rs src/sql/codegen/fragment_builder.rs src/runtime/coordinator.rs src/runtime/runtime_filter_worker.rs
git commit -m "Align distributed runtime filter semantics with StarRocks"
```

Expected: commit succeeds and includes only runtime filter implementation/test files.

---

## Self-Review

- Spec coverage: join-side safety, exchange crossing, descriptor metadata, distributed builder number, runtime worker merge, and 1FE+3BE acceptance each have a task.
- Placeholder scan: no task uses a deferred implementation placeholder; every code-changing step includes concrete snippets or exact commands.
- Type consistency: plan uses existing NovaRocks types `RuntimeFilterPlanResult`, `TRuntimeFilterDescription`, `TRuntimeFilterBuildJoinMode`, `TRuntimeFilterParams`, `RuntimeFilterWorker`, and `RuntimeFilterHub`.
