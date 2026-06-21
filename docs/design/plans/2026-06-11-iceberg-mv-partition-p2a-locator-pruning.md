# Iceberg MV Partition P2-a (Delete-side Locator Pruning) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: superpowers:executing-plans (or subagent-driven-development). Steps use checkbox (`- [ ]`) syntax.

**Goal:** Thread the already-computed `affected_partitions` allow-list into the merge-sink delete-side target-row locator, so partitioned aggregate / single-base PF refreshes prune target file scans on the DELETE side too (today only the target-state READ side is pruned). Pure performance; identical results.

**Architecture:** P1 left the four `locate_target_rows_by_*` functions (`src/engine/mv/iceberg_target_apply.rs`) already accepting `partition_filter: &TargetPartitionFilter` with working file-pruning logic (`locate_target_rows_by_apply_key_impl:737`), but `IcebergMergeSinkOperator::handle_delete_batch` (`src/engine/mv/iceberg_merge_sink.rs:195`) calls all four with `TargetPartitionFilter::None`. `IcebergMvRefreshContext.affected_partitions` (the plan-time, manifest-derived `AffectedTargetPartitions`) is in scope at the single merge-sink construction site (`incremental_refresh_iceberg_mv_with_changes`, `iceberg_refresh.rs:10301`). This plan converts that to a `TargetPartitionFilter` and threads it through `IcebergMergeSinkPlan` to the four call sites.

**Correctness:** The filter is `AllowList` only when `affected_partitions == Known` — which the planner returns only when it proved the base-partition→MV-partition mapping (`map_file_partition_to_mv_key`). Every target row a delete references (an old-state row of a touched group / changed base row) lives in an MV partition derived from the same changed base files, hence in the `Known` (new∪old) set. This is the exact invariant the existing target-state-read allow-list (`refresh_context.rs:814`) already relies on. For every other shape (join/union/unpartitioned/NotDerived) the filter is `None` → no behavior change.

**Tech Stack:** Rust; iceberg-rust 0.9; the existing MV merge-sink + locator.

---

### Task 1: `AffectedTargetPartitions::to_target_partition_filter`

**Files:**
- Modify: `src/engine/mv/partition/derivation.rs` (add method + tests)

- [ ] **Step 1: Write the failing test**

Append to the `tests` module in `derivation.rs`:

```rust
    #[test]
    fn to_target_partition_filter_maps_known_to_allow_list() {
        let result = AffectedTargetPartitions::known([key("a"), key("b")]);
        let filter = result.to_target_partition_filter();
        let crate::engine::mv::partition::TargetPartitionFilter::AllowList(set) = filter else {
            panic!("expected AllowList");
        };
        assert_eq!(set.len(), 2);
        assert!(set.contains(&key("a")));
        assert!(set.contains(&key("b")));
    }

    #[test]
    fn to_target_partition_filter_maps_unpartitioned_and_not_derived_to_none() {
        assert_eq!(
            AffectedTargetPartitions::Unpartitioned.to_target_partition_filter(),
            crate::engine::mv::partition::TargetPartitionFilter::None
        );
        assert_eq!(
            AffectedTargetPartitions::not_derived("x").to_target_partition_filter(),
            crate::engine::mv::partition::TargetPartitionFilter::None
        );
    }
```

(`key(..)` is the existing test helper in this module.)

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test --lib engine::mv::partition::derivation`
Expected: compile FAIL — no method `to_target_partition_filter`.

- [ ] **Step 3: Implement**

Add to `impl AffectedTargetPartitions` in `derivation.rs`:

```rust
    /// Convert to a `TargetPartitionFilter` for file-scan pruning. `Known`
    /// becomes an `AllowList`; `Unpartitioned` and `NotDerived` become `None`
    /// (no pruning). Pruning is an optimization (umbrella spec §4.3 / D5
    /// BestEffort): a NotDerived outcome must never restrict the scan. The
    /// empty `Known` set legitimately produces an empty `AllowList` (nothing
    /// affected), which the locator honors by scanning zero files.
    pub(crate) fn to_target_partition_filter(
        &self,
    ) -> crate::engine::mv::partition::TargetPartitionFilter {
        use crate::engine::mv::partition::TargetPartitionFilter;
        match self {
            Self::Known { partitions } => TargetPartitionFilter::AllowList(partitions.clone()),
            Self::Unpartitioned | Self::NotDerived { .. } => TargetPartitionFilter::None,
        }
    }
```

- [ ] **Step 4: Run to verify it passes**

Run: `cargo test --lib engine::mv::partition::derivation`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/engine/mv/partition/derivation.rs
git commit -m "feat(mv): add AffectedTargetPartitions::to_target_partition_filter"
```

---

### Task 2: thread the filter through the merge sink

**Files:**
- Modify: `src/engine/mv/iceberg_merge_sink.rs` (struct field + 4 call sites)
- Modify: `src/engine/mv/iceberg_refresh.rs:10301` (set field at construction)

- [ ] **Step 1: Add the field to `IcebergMergeSinkPlan`**

In `iceberg_merge_sink.rs` (struct at line ~48), add:

```rust
pub struct IcebergMergeSinkPlan {
    pub target_table: iceberg::table::Table,
    pub collector: Arc<IcebergCommitCollector>,
    pub locator_state: Option<TargetLocatorState>,
    pub apply_key_column: String,
    pub apply_key_value_type: ApplyKeyValueType,
    /// Partition allow-list for the delete-side locator. `None` = no pruning
    /// (join / union / unpartitioned / NotDerived). Derived from the refresh
    /// context's `affected_partitions` at construction.
    pub partition_filter: crate::engine::mv::partition::TargetPartitionFilter,
}
```

- [ ] **Step 2: Use the field at the four locator call sites**

In `handle_delete_batch` (lines 217, 236, 251, 270), replace each
`&crate::engine::mv::partition::TargetPartitionFilter::None,`
with
`&self.plan.partition_filter,`.

- [ ] **Step 3: Set the field at construction**

In `iceberg_refresh.rs` at the `IcebergMergeSinkPlan { ... }` literal (line ~10301, inside `incremental_refresh_iceberg_mv_with_changes` where `ctx: &IcebergMvRefreshContext` is in scope), add:

```rust
        partition_filter: ctx.affected_partitions.to_target_partition_filter(),
```

- [ ] **Step 4: Fix any other construction sites**

Run `rg -n "IcebergMergeSinkPlan \{" src/`. Expected: only `iceberg_refresh.rs:10301` and possibly test fixtures in `iceberg_merge_sink.rs`. Add `partition_filter: crate::engine::mv::partition::TargetPartitionFilter::None,` to any test fixture literal so it compiles.

- [ ] **Step 5: Build + lib tests**

Run: `cargo build --lib 2>&1 | tail -20 && cargo test --lib engine::mv 2>&1 | tail -8`
Expected: clean build; MV lib tests PASS.

- [ ] **Step 6: Commit**

```bash
git add src/engine/mv/iceberg_merge_sink.rs src/engine/mv/iceberg_refresh.rs
git commit -m "feat(mv): prune delete-side locator scans by affected-partition allow-list"
```

---

### Task 3: optional DRY refactor of the target-state allow-list

**Files:**
- Modify: `src/engine/mv/refresh_context.rs:814` (`target_state_partition_allow_list`)

- [ ] **Step 1: Reuse the new method (only if it does not change behavior)**

`target_state_partition_allow_list` returns `Option<BTreeSet<MvPartitionKey>>` and additionally **warns** on `NotDerived`. The new method returns a `TargetPartitionFilter` without warning. To avoid losing the warn and avoid churn, **leave `target_state_partition_allow_list` as-is** for P2-a. (Recorded here so a reviewer does not flag the apparent duplication as an oversight: the two consumers differ — one needs a logged `Option<set>`, the other a silent `TargetPartitionFilter` — and unifying them is deferred.) No code change in this task.

- [ ] **Step 2: (no commit — documentation-only decision)**

---

### Task 4: verification

- [ ] **Step 1: fmt + clippy + full lib tests**

```bash
cargo fmt
cargo clippy --lib 2>&1 | grep -c "warning:"   # compare to base count (242)
cargo test --lib 2>&1 | tail -3
```
Expected: no new warnings; `cargo test --lib` 4324+ passed (P1 was 4323; +1 new test from Task 1, +2 actually → 4325), 0 failed.

- [ ] **Step 2: iceberg-ivm SQL suite (behavior lock)**

```bash
source docker/iceberg-rest/runtime/current/env.sh
docker/iceberg-rest/up.sh
cargo build --profile dev-opt
LOG=/tmp/novarocks-p2a-server.log
NO_PROXY=127.0.0.1,localhost target/dev-opt/novarocks standalone-server \
  --config "$NOVAROCKS_STANDALONE_CONFIG" >"$LOG" 2>&1 &
SRV_PID=$!
for i in $(seq 1 60); do
  grep -q '^NOVAROCKS_READY ' "$LOG" && break
  kill -0 "$SRV_PID" 2>/dev/null || { tail -20 "$LOG"; exit 1; }
  sleep 1
done
grep -q '^NOVAROCKS_READY ' "$LOG" || { echo timeout; kill -9 "$SRV_PID"; exit 1; }
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" --suite iceberg-ivm --mode verify
kill "$SRV_PID" 2>/dev/null || true
```
Expected: identical failed-case set vs the PR #291 baseline (the 13 pre-existing unrelated failures; 57/70 pass). The DELETE-heavy partitioned aggregate cases must still produce identical results — pruning only removes provably-irrelevant target files.

- [ ] **Step 3: Open PR**

Title: `feat(mv): prune delete-side locator scans by affected-partition allow-list (P2-a)`. Body: links umbrella spec #288 §4.4/§5.2 and PR #291; states pure-performance, correctness invariant (Known = proven new∪old superset), iceberg-ivm parity.

---

## Scope note

P2-a is the first of the P2 slices. Remaining P2 work (separate plans, each needs first-hand reading of the join-delta execution + a design decision):
- **P2-b — join PF derivation:** produce `AffectedTargetPartitions::Known` for join projection/filter by evaluating the partition spec over join coalescer delta chunks. Open design question: deriving the allow-list from the streamed delta chunks vs. a plan-time pass, given the delete-side locator needs the filter before/as deletes stream.
- **P2-c — UNION ALL branch merge:** resolve one `PartitionDerivationSpec` per branch into `ImvPartitionAnnotation::Derivable.specs`, evaluate per `__branch_id__`, union into one allow-list. v1: any branch NotDerivable → whole-union NotDerivable.
