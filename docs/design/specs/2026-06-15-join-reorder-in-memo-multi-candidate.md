# Design: In-Memo Multi-Candidate Join Reorder (Align with StarRocks)

Date: 2026-06-15
Status: Design (drives implementation; no short-term band-aid)
Scope: standalone optimizer `src/sql/optimizer/**`
Reference: StarRocks `fe/fe-core/.../sql/optimizer/rule/join/**`, `Memo.java`, `QueryOptimizer.java`
Motivated by: `docs/design/specs/2026-06-13-starrocks-fe-benchmark-plan-gap.md` (P0 cardinality)
 and the two follow-up findings below.

This design was produced by an exhaustive read of both codebases and hardened by two
adversarial reviews (NovaRocks feasibility + StarRocks fidelity). Sections marked
**[review-corrected]** record where the first-pass design was wrong and why.

---

## 0. Background: how we got here

1. The P0 cardinality explosion (tpc-ds q4/q11/q31/q74, ~1e14) was root-caused to
   `CTEConsume` dropping producer column statistics, and fixed in the memo estimator
   (`stats.rs`), confirmed by a real TPC-DS SF1 EXPLAIN A/B (1e14 → ~2.6M, exact match
   to StarRocks FE magnitude). PR #315.

2. Auditing for the same class revealed **two separate cardinality estimators**:
   - `src/sql/optimizer/stats.rs` — the memo estimator (walks `MExpr`, resolves CTE
     producers via `memo.cte_produce_groups`). The fix landed here.
   - `src/sql/optimizer/rewrite/rules/join_reorder/cardinality.rs` — a second
     `LogicalPlan` estimator used only by the RBO join-reorder pass. It still has the
     CTEConsume gap (hardcoded `1000.0`, no NDV) plus AggregateStateMerge/Values/
     GenerateSeries/TableFunction gaps.

3. Investigating *why two*: NovaRocks does join reorder as a **single-result RBO
   pre-pass** (`JoinReorderRule` → `reorder_joins_cbo` returns one `LogicalPlan`), which
   seeds the memo; Cascades then only does limited `JoinCommutativity` (build/probe swap)
   and restricted `JoinAssociativity` (skipped above 200 memo groups). Because the reorder
   runs *before* the memo exists, it cannot use the memo estimator — hence the second
   estimator. And because it commits to one order, join-order quality depends entirely on
   that second (lossier) estimator, which Cascades cannot recover.

4. Verifying against StarRocks: **StarRocks does NOT pick one order.** Its
   `ReorderJoinRule` enumerates **multiple algorithms × Top-K candidates** and injects
   them **all into the memo** as alternatives (`Memo.copyIn`), letting the single
   cost-based search choose. This is the model we align to.

**Conclusion that ties it together:** moving join reorder *into the memo* as a
multi-candidate producer (a) deletes the second estimator (candidates are costed by the
one memo estimator), and (b) makes join order a cost-based decision over many candidates
instead of a single early commit. Two problems, one root.

---

## 1. Goal & Non-Goals

### Goal

Replace the single-result RBO join-reorder pre-pass with a **StarRocks-faithful, in-memo,
multi-candidate** producer: enumerate multiple candidate orders (LeftDeep always; DP and
Greedy-TopK subject to caps), inject all of them as logically-equivalent alternatives into
the join's memo group, and let the existing memo cost search (`search::optimize_group`)
pick the winner with full distribution/exchange awareness. Delete the second estimator.

### Non-Goals (explicitly NO band-aid — per user directive)

- **No** routing the pre-memo RBO reorder through `derive_logical_plan_statistics` (a
  dedup band-aid that keeps the single-order commit). We move reorder into the memo.
- **No** keeping the bespoke `LogicalPlan` DP engine alive in parallel. Its enumeration
  *cores* are reused, re-expressed over `GroupId`/`JoinTree`.
- **No** FE-side flags/guards (project rule: fix the BE capability). Standalone-only.
- **No** dual-format / compat shims (NovaRocks has no historical users). The old RBO
  reorder path is deleted after cutover, not feature-flagged into permanent coexistence.
- **Scope = inner/cross chains only**, matching StarRocks (`MultiJoinNode.flattenJoinNode`
  stops at non-inner/cross). Outer/semi/anti joins are opaque atom boundaries.

---

## 2. Key architectural decision **[review-corrected]**

### First-pass design (rejected): a registered `explore()` transformation rule

The first design made `MultiJoinReorder` a `Rule` in `all_transformation_rules()`, firing
inside the `explore()` fixpoint loop "where JoinAssociativity runs." The adversarial
reviews showed this is both **unfaithful** and **hazardous**:

- **Unfaithful (D1):** StarRocks `ReorderJoinRule` is **not** a scheduled rule. `TF_MULTI_JOIN_ORDER`
  is never added to any `RuleSet`; it is invoked imperatively once at
  `QueryOptimizer.java:971` (`new ReorderJoinRule().transform(tree, context)`), *after* the
  memo is built and *before* the cost-search scheduler runs. It pre-seeds the memo in a
  single pass.
- **Non-terminating (P2):** NovaRocks `explore()` re-scans every group each round
  (`mod.rs:230-289`). A registered reorder rule would re-fire on its own injected joins
  every round, minting fresh groups, never reaching the explore dedup, growing the memo
  until `EXPLORE_MAX_GROUPS=5000` silently truncates (`mod.rs:282-284`) — a
  non-deterministic, partially-explored memo. Avoiding this required an `already_reordered`
  marker + a dedup index purely to tame a self-inflicted fixpoint.

### Adopted design: a one-shot in-memo pre-population pass (faithful to StarRocks)

`MultiJoinReorder` is a **one-shot pass invoked imperatively once** in `optimize()`
(`src/sql/optimizer/mod.rs`), immediately after `derive_group_statistics` (`mod.rs:130`,
so atom group stats exist) and before `explore()` (`mod.rs:143`). It walks the freshly
converted memo, finds inner/cross join-chain roots, flattens each chain over `GroupId`s,
enumerates candidate orders, and `copy_in`s each candidate into the chain-root group as an
alternative. Then `explore()`/`implement()`/`search()` proceed over all candidates.

This single decision eliminates, by construction:
- P2 (re-firing / non-termination) — the pass runs once; no fixpoint.
- P3 (dual-dedup consistency) — only a within-pass dedup index is needed.
- P5 (fixpoint interaction with JoinAssociativity) — see D2 hard branch below.
- D1 (fidelity) — this *is* StarRocks's structure.
- G8 (`already_reordered` marker) — not needed; dropped.

It still requires P1 (stamp stats on each new group at creation — see §3 G3), which is
mandatory regardless of pass vs rule.

---

## 3. Gap Analysis (what to build)

Each gap: what exists / what's missing / what to build / file:line. **[review-corrected]**
items reflect the adversarial findings.

### G1 — `copy_in` primitive (recursive subtree materialization)
- Exists: `memo.new_group(MExpr)` (`memo.rs:51`), `memo.add_expr_to_group` (`memo.rs:70`);
  `JoinAssociativity` open-codes a single-intermediate-group injection
  (`join_associativity.rs:136-145`).
- Missing: a recursive, bottom-up, dedup-aware materializer ≈ `Memo.copyIn`
  (`Memo.java:134-161`).
- Build: `memo.copy_in_join_tree(tree: &JoinTree) -> GroupId`. Leaf → existing GroupId.
  Join → recurse children first (bottom-up; G5 invariant), dedup via the within-pass index
  (G2), else `new_group` + **immediately** `derive_group_statistics_for` (G3, mandatory).
  **[review-corrected P6]** It materializes the **strict descendants** of the candidate
  root and returns the root's two child GroupIds + the root `LogicalJoinOp`; the *root* is
  added to the chain-root group as the alternative `NewExpr` by the caller (mirrors
  `JoinAssociativity`: `new_group` the inner, return the outer). It does NOT materialize
  the top into its own group (that would double-create it).

### G2 — Within-pass dedup index
- Exists: explore-loop dedup by `existing.children == new.children && op_equal` where
  `op_equal` is `format!("{:?}")` (`mod.rs:264-267`, `:353-355`); `find_existing_logical_group`
  (`split_aggregate.rs:174-182`).
- Missing: a hash index so candidates from different algorithms sharing intermediate
  sub-joins reuse the same GroupId (≈ StarRocks `groupExpressions`, `Memo.java:99-121`).
- Build: `HashMap<(OpKey, Vec<GroupId>), GroupId>` consulted by `copy_in_join_tree`.
  `OpKey` = structural key (join kind + canonicalized equi-key column-set), **not** the
  `Debug` string. **[review-corrected: scope]** Because the pass is one-shot, this is a
  within-pass index for sharing intermediates across the candidate set — *not* a
  correctness-for-termination device (that concern died with the fixpoint rule). Still
  worth building for memo-size economy.

### G3 — Per-new-group stats stamping — **MANDATORY [review-corrected P1]**
- Exists: `derive_group_statistics` runs at `mod.rs:130` and `:152`; assumes child
  index < parent index (`stats.rs:668-673`); falls back to a `Fallback` 10k-row default
  when a child's `logical_props` is `None` (`stats.rs:733`).
- Missing: stamping stats on each new group at creation.
- **Why mandatory (not optional):** `implement()` runs at `mod.rs:149`, *before* the
  re-derive at `mod.rs:152`. `JoinToHashJoin::apply` reads
  `get_group_column_ids(memo, child)` (`implement.rs:576`), which returns **empty** when
  the child group's `logical_props` is `None` (`implement.rs:18-30`) → `orient_eq_pair`
  fails on every equi-key → `JoinToHashJoin` returns `vec![]` and `JoinToNestLoop` takes
  over. **Without per-group stamping, every multi-level (bushy) candidate is silently
  implemented as a NestLoop join — defeating the entire rule.**
- Build: `stats::derive_group_statistics_for(memo, group_id, table_stats)`, called inside
  `copy_in_join_tree` immediately after `new_group`, for **every** new intermediate group.
  This is the analog of StarRocks `Memo.java:154-158`.

### G4 — Greedy Top-K
- Exists: `greedy_join_reorder` returns one best plan (`reorder.rs:1033`). DP/LeftDeep too.
- Missing: StarRocks Greedy keeps a bounded `MinMaxPriorityQueue` of the 10 lowest-cost
  full-join expressions and drains cheapest-first (`JoinReorderGreedy.java:36-79`,
  `:174-190`). This is the multi-candidate core.
- Build: greedy core's full-mask cell accumulates a bounded Top-K (`cbo_max_reorder_topk`,
  default 10); returns `Vec<JoinTree>`. LeftDeep/DP return `vec![best]`.

### G5 — Re-express enumeration cores over `GroupId`/`JoinTree`
- Exists: `dp_join_reorder` (`reorder.rs:640`), `greedy_join_reorder` (`:868`),
  `left_deep_join_reorder` (`:1049`) on `u32` masks + `DpEntry { plan: LogicalPlan, ... }`;
  pure helpers `find_connecting_predicates` (`:813`), `has_equijoin_predicate` (`:53`),
  OR-factoring (`:1262`), `SubsetIter` (`:1157`) — reusable as-is.
- Missing: memo equivalents (leaves are `GroupId`; stats from `logical_props`).
- Build: `enum JoinTree { Leaf(GroupId), Join { left, right, op: LogicalJoinOp } }`.
  `DpEntry`/greedy cell carry `JoinTree` + `Statistics`. Per-candidate join stat via
  `estimate::cardinality::estimate_join_cardinality(JoinCardInput{..})` on the two child
  trees' cached `Statistics` — the same kernel `stats.rs` uses. Leaf stat =
  `memo.groups[gid].logical_props`. Mask cap ≤62 (DP) consistent with StarRocks long-mask.
  **[review-corrected D7]** Preserve LeftDeep's same-table-self-join avoidance + equi-join
  preference heuristic (`JoinReorderLeftDeep.java:50-86`) in the port.
  **[review-corrected D6]** Port saturating add/mul against a `MAX_REORDER_COST` ceiling
  (≈ `JoinOrder.saturatingAdd/Mul`) so DP branch-and-bound on cross chains cannot overflow
  to inf/NaN and break the comparator.

### G6 — Flatten join chain over child groups
- Exists: `flatten_inner_joins` / `extract_join_graph` (`reorder.rs:463-635`) on
  `LogicalPlan`; absorb top Filter; push single-relation predicates; popcount predicate
  classification.
- Missing: a memo-side flattener over `MExpr.children` (peeking child groups'
  `logical_exprs[0]` as `JoinAssociativity` does, `join_associativity.rs:62-83`).
- Build: `flatten_join_chain(memo, root_expr) -> MultiJoinGraph { atoms: Vec<GroupId>,
  predicates: Vec<(TypedExpr, u64 mask)> }`. Atom = a child group that is not itself an
  inner/cross join. Per-relation column sets via `get_group_column_ids(memo, gid)`
  (`implement.rs:18`). Single-side predicates → wrap atom in a `new_group(LogicalFilter)`.
  **[review-corrected D3/D4]** NovaRocks joins carry **no projection** and the flattener
  treats any `LogicalProject` as an opaque atom (`reorder.rs:631-633`), so no derived
  column can be dropped by reordering. Therefore: **no `expr_map` / `expressionMap` port
  (drop it), and no `checkDependsPredicate` guard (G7 deleted)** — both exist in StarRocks
  only to handle projection-flattening hazards NovaRocks structurally cannot have. This
  resolves the first-pass contradiction (porting `expr_map` while claiming pruning is
  unneeded). A one-line invariant test asserts the flattener never descends through a
  `LogicalProject`.

### G7 — *(deleted [review-corrected D4])* `checkDependsPredicate` guard
NovaRocks has no `expressionMap` and never flattens through projections, so the
chained-derived-column hazard cannot arise. No guard needed.

### G8 — *(deleted [review-corrected D1])* `already_reordered` marker
Only needed for a fixpoint rule. The one-shot pass does not re-fire. Dropped.
`LogicalJoinOp` is unchanged; the `join_reorder_global_applied` RewriteContext flag is
deleted with the RBO path.

### G9 — Blow-up bound
- Build: the pass injects a bounded set per chain (`1 LeftDeep + 1 DP + K Greedy`), gated
  by StarRocks-matched caps (DP ≤ `cbo_max_reorder_node_use_dp`/62; Greedy ≤
  `cbo_max_reorder_node_use_greedy`; master `cbo_max_reorder_node`). Above the master cap
  the pass skips the chain (LeftDeep-only fallback). **[review-corrected P5]** Phase 7 must
  assert no `EXPLORE_MAX_GROUPS` truncation on 12–16-table joins as a gating criterion.

### G10 — Two estimators (unification) — see §4.

---

## 4. Estimator Unification

### 4.1 Why in-memo reorder lets the second estimator be deleted
Once reorder runs after `derive_group_statistics` (`mod.rs:130`), every atom group already
has `logical_props.{row_count, column_statistics}`. Per-candidate join cardinality is a
direct `estimate_join_cardinality` call on cached child stats — same kernel as `stats.rs`.
No caller needs the from-scratch `LogicalPlan` walk anymore.

### 4.2 Deleted vs kept
- **Deleted:** `src/sql/optimizer/rewrite/rules/join_reorder/cardinality.rs` (the second
  estimator) and its 5 call sites (`reorder.rs:654,785,889,986,1062`).
- **Kept:** `derive_logical_plan_statistics` (`stats.rs:603`) — it builds a throwaway memo
  and routes through the *memo* estimator; it is the bridge *onto* the kept estimator, used
  by aggregate-pushdown (still pre-memo). Not the second estimator.
- **Kept unchanged:** shared kernels `estimate/{cardinality,selectivity,ndv,join_condition}.rs`.

### 4.3 Per-operator gaps fixed once, in `stats.rs`
Deleting the lossy walker auto-fixes reorder-time divergences; but these gaps live in the
memo estimator too and must be closed so the single estimator is correct for all callers:

| Operator | Current `stats.rs` | Fix |
|---|---|---|
| CTEConsume | already correct (`stats.rs:78-106/:501-527`) | none (deletion is the fix) |
| AggregateStateMerge | rows summed, empty col stats (`:240-249/:582-591`) | propagate merged child col stats by `output_columns` |
| Values | exact rows, empty col stats (`:65-69/:562-566`) | synthesize exact per-column NDV/min/max from literal rows |
| GenerateSeries | exact rows, empty col stats (`:70-74/:568-572`) | synthesize exact NDV(=rows) + min/max |
| TableFunction | ×3 rows, empty col stats (`:741-753`) | start from child col stats (passthrough); only TF-generated columns unknown |

---

## 5. What gets retired / changed

| Component | Disposition |
|---|---|
| `JoinReorderRule` (`join_reorder/rule.rs`) + `join_reorder_global_applied` | Retired; removed from `rewrite/registry.rs`. |
| `reorder_joins_cbo` tree driver, `extract_join_graph`/`flatten_inner_joins`, `reorder_joins_heuristic`, `estimate_size` | Retired (memo always has stats; heuristic obsolete). |
| `reorder.rs` mask cores + helpers, `cost.rs` join-cost arithmetic | Moved & adapted into `cascades_rules/multi_join_reorder/` over `JoinTree`; Greedy → Top-K; saturating cost. |
| `join_reorder/cardinality.rs` | Deleted (§4). |
| `JoinCommutativity` | Kept, unchanged (build/probe swap inside memo; StarRocks keeps `addJoinCommutativityWithoutInnerRule`). |
| `JoinAssociativity` | **[review-corrected D2]** Hard-branched, not co-active: chains with atom count > `cbo_max_reorder_node_use_exhaustive` (4) are handled by `MultiJoinReorder` and inner-associativity is **disabled** for them; chains ≤4 are left to `JoinAssociativity` exhaustively and the reorder pass skips them. Mirrors StarRocks `QueryOptimizer.java:967-981` / `RuleSet.java:481-494` (mutual exclusion, not a soft 200-group throttle). |
| New session flags (`options.rs`) | `cbo_enable_dp_join_reorder`(t), `cbo_enable_greedy_join_reorder`(t), `cbo_max_reorder_node`(50), `cbo_max_reorder_node_use_exhaustive`(4), `cbo_max_reorder_node_use_dp`(10, cap 62), `cbo_max_reorder_node_use_greedy`(16), `cbo_max_reorder_topk`(10). Threaded via `SessionOptimizerSettings`→`OptimizerOptions::from_session`. Disable via `SET disable_optimizer_rules='MultiJoinReorder'`. |

---

## 6. Correctness must-haves (from adversarial review)

These are hard requirements, each with a test obligation:
- **M1 (P1):** every new intermediate group gets `derive_group_statistics_for` at creation,
  before `implement()`. Test: build a ≥3-level bushy `JoinTree`, run through implement,
  assert HashJoin (not NestLoop) survives on every level.
- **M2 (G5 invariant / D5):** `copy_in_join_tree` allocates strictly bottom-up; assert
  child GroupId < parent GroupId (`debug_assert`) so the index-order re-derive
  (`stats.rs:668-673`) stays valid and no `Fallback` 10k row count (`stats.rs:733`) leaks.
- **M3 (D2):** reorder and inner-associativity are mutually exclusive per chain (hard
  branch on atom-count vs exhaustive threshold). Test: a 6-table inner chain is reordered
  by `MultiJoinReorder` and `JoinAssociativity` produces zero alternatives for it.
- **M4 (D3):** flattener never descends through a `LogicalProject`; opaque-atom test.
- **M5 (D6):** enumeration cost saturates; cross-join chain test asserts no inf/NaN in the
  pruning comparator.
- **M6 (search bridge, P4):** injected logical alternatives become physical via
  `implement()` and are compared by `search::optimize_group` (costs only physical exprs,
  `search.rs:111-125`). Covered transitively by M1 + the Phase-5 A/B.

---

## 7. Phased implementation plan (TDD; engine green throughout)

The pass is wired into `optimize()` only at Phase 5. Until then the old path runs.

- **Phase 0 — Baseline (no code).** Record golden EXPLAIN for q4/q11/q31/q74 + an
  inner-join-heavy set (ssb, tpc-h q5/q8, `sql-tests/optimizer/`) under the current path.
  This is the A side of every later A/B. Use `-- @explain_contains` / `--mode record`.
- **Phase 1 — Estimator gap fixes in `stats.rs`** (§4.3; AggregateStateMerge/Values/
  GenerateSeries/TableFunction). Unit tests per operator. Suite green; goldens for plans
  where these feed joins.
- **Phase 2 — Memo primitives** (G1+G2+G3): `JoinTree`, `copy_in_join_tree`,
  within-pass index, `derive_group_statistics_for`. Tests: 3-leaf tree creates expected
  groups, reuses leaf GroupIds, bottom-up invariant (M2), per-group stats stamped (M1
  unit). Primitives unused by production yet.
- **Phase 3 — Port enumeration cores** (G4+G5+G6): `multi_join_reorder/{algo,flatten}.rs`
  over `JoinTree`; Greedy Top-K; saturating cost (M5); LeftDeep heuristic (D7); flattener
  with opaque-projection invariant (M4). Plain functions + golden-tree unit tests.
- **Phase 4 — The one-shot pass** (assemble): `run_multi_join_reorder(memo, opts,
  table_stats)` — walk memo, find inner/cross chain roots > exhaustive threshold, flatten,
  unknown-stats→LeftDeep-only degrade, select algos, enumerate, `copy_in` each, add root
  alternative. Session flags into `OptimizerOptions`. Built but **not invoked** in
  `optimize()`. Tests: hand-built memo → pass adds N alternatives; bushy implement→HashJoin
  (M1 integration); caps/degradation/flag-off behavior.
- **Phase 5 — Cutover + A/B.** Invoke the pass in `optimize()` after `mod.rs:130`; apply
  the D2 hard branch with `JoinAssociativity` (M3). The old RBO path stays registered this
  phase (double-cover result correctness; instant revert via
  `SET disable_optimizer_rules='MultiJoinReorder'`). Full A/B: q4/q11/q31/q74 + ssb/tpc-h/
  tpc-ds plan goldens + `sql-tests/optimizer/`. Expect match-or-improve; record intentional
  distribution-aware improvements the pre-memo path could not see.
- **Phase 6 — Retire RBO + delete second estimator.** Remove `JoinReorderRule`, delete
  `cardinality.rs`, delete the retired `reorder.rs`/`cost.rs` LogicalPlan dispatch, delete
  `join_reorder_global_applied`. Verify aggregate-pushdown bridge intact. Suite green with
  only the in-memo path; `cargo clippy` dead-code clean.
- **Phase 7 — Perf hardening.** Validate in-memo enumeration cost; tune Top-K/caps vs
  `EXPLORE_MAX_GROUPS`/timeout; **gate: no truncation on 12–16-table joins (M3/P5)**;
  release-build planning-time benchmark on ssb/tpc-h.

---

## 8. Remaining risks & open questions

- **R1 (D5 divergence, intentional):** StarRocks `Memo.copyIn` seeds-and-trusts
  reorder-time stats (`Memo.java:158`); we re-derive via the canonical kernel
  (`derive_group_statistics_for`). This is safer (no proxy/canonical divergence) but is a
  deliberate divergence — documented, gated by the M2 bottom-up invariant.
- **R2 (output columns, §7.3 of review / D3):** confirmed no separate prune pass needed
  (no join projections; per-group `output_columns` re-derived). Phase-5 wide-table A/B
  (q11/q74) verifies intermediate joins are projected to required columns under a different
  order.
- **R3 (distribution-aware plan changes):** moving in-memo means search costs each order
  with exchange awareness the pre-memo path lacked — Phase-5 A/B will show *intentional*
  changes; review as improvements, not regressions. Confirm `JoinToHashJoin`/`JoinToNestLoop`
  + property enforcement produce both build-side orientations per injected order
  (`JoinCommutativity` supplies the swap).
- **R4 (iteration budget):** new groups are created pre-`explore()` (one-shot), so they are
  present for the very first explore round and all `EXPLORE_MAX_ITERATIONS=16` rounds —
  strictly better than the rejected fixpoint design. Phase 7 instruments actual round usage.

---

## Appendix: authoritative file:line index

NovaRocks: `rule.rs:20-40`; `memo.rs:51-86`; `mod.rs:127/130/143/149/152/224/255/282`;
`cascades_rules/mod.rs:45-61`; `join_associativity.rs:43/62-83/136`; `split_aggregate.rs:174-182`;
`operator.rs:162-165`; `options.rs:60-146`; `join_reorder/rule.rs:46-57`;
`join_reorder/reorder.rs:307/463/640/868/1049 (+stats calls 654,785,889,986,1062)`;
`join_reorder/cardinality.rs (delete)`; `stats.rs:57/603/668-673/674/733/1336`;
`implement.rs:18-30/576-577/606-608/653-666`; `search.rs:87-268 (cost 111-125)`; `cost.rs`.

StarRocks: `ReorderJoinRule.java:103-180/243-281 (+OutputColumnsPrune 288-409, degrade 260-263)`;
`Memo.java:99-121/134-161`; `JoinReorderGreedy.java:36-79/174-190`;
`JoinOrder.java:243-262/285-311/488-502/546-563`; `JoinReorderLeftDeep.java:35-108`;
`JoinReorderFactory.java:46-65`; `MultiJoinNode.java:63-132`; `SessionVariable.java:1840-1856`;
`QueryOptimizer.java:967-981/971/1006`; `RuleSet.java:481-494`; `StatisticsEstimateCoefficient.java:59/62`.
