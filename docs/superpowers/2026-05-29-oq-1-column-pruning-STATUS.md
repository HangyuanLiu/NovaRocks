# OQ-1 Column Pruning Refactor — Current Status (2026-05-29)

> **⚠️ DO NOT MERGE.** This branch's new column pruning is **ACTIVE but produces wrong
> results on several real query shapes** (see §3). It is a WIP checkpoint preserving the
> architecture + investigation. Either finish §4 (make per-operator rules production-correct)
> or revert the atomic switch (commit `4ece15a5`) to re-enable the old `PruneColumns` before
> any merge.

Spec: `docs/superpowers/specs/2026-05-28-oq-1-column-pruning-arch-refactor-design.md`
Plan: `docs/superpowers/plans/2026-05-28-oq-1-column-pruning-arch-refactor.md`

## 1. Goal (unchanged)

Replace the single 562-line name-based `PruneColumns` rewrite rule with a per-operator,
ColumnId-based column-pruning architecture (mirroring StarRocks `Prune*ColumnsRule`):
a Phase-1 "tagging pass" that writes `required_output_columns: Option<HashSet<ColumnId>>`
on every operator, then 18 per-operator `Prune*Columns` rules that consume it. Closes the
5 column-pruning gaps from the OQ-1 preflight (SubqueryAlias propagation, Project items
pruning, CTE inline ordering, set-op branch alignment, Aggregate/Window output pruning) and
should cut join-suite wall_time from ~33min toward minutes by eliminating wide-table
`SELECT *`-through-CTE/subquery bandwidth amplification.

## 2. What landed (24 commits on `claude/oq-1-column-pruning`)

All of Phases A–D of the plan. **Lib tests: 3226 passing.** Each piece passed a two-stage
(spec + code-quality) subagent review.

- **Phase A — Foundation** (`679425ae`,`1a55523b`,`cc53207e`,`d93eaf6a`,`97d38f83`,`b40f58ea`,`6a3b24a9`,`8cde1dec`,`6a71b92e`):
  - `ColumnRefFactory` threaded through `RewriteContext` (for auto-fill id minting).
  - `output_columns: Vec<OutputColumn>` added to `UnionNode`/`IntersectNode`/`ExceptNode`,
    carrying the **fresh parent-referenced set-op ColumnIds** (a Task-2 bug — first-branch ids — was caught + fixed).
  - `required_output_columns: Option<HashSet<ColumnId>>` added to all 20 plan variants.
  - `output_column_id: ColumnId` added to `ProjectItem` (keystone: makes computed Project outputs ColumnId-addressable).
  - Helpers `collect_column_id_refs`, `collect_output_ids[_ordered]`.
- **Phase B — Tagging pass** (`b2f2274a`,`b3b3e337`,`1fa0edc3`,`d8b3ac33`,`892c4824`):
  - `src/sql/optimizer/rewrite/required_columns.rs::tag_required_columns` with handlers for
    all 20 operators; 6 under-tagging bugs + 1 termination bug caught + fixed in review.
  - Registered as a `TagRequiredColumns` (TopDown) stage before `ColumnPruning`.
- **Phase C — 18 per-operator rules** (`6d6729da`,`13db721b`,`35f80967`):
  - `src/sql/optimizer/rewrite/rules/column_pruning/` — 11 real-filter rules + 7 no-op rules.
- **Phase D — Atomic switch** (`4ece15a5`,`56b4b31e`,`c9658049`):
  - `4ece15a5` registered the 18 rules as the active pruner, deleted the old `PruneColumns`.
    This commit ALSO fixed regressions it surfaced (Iceberg scan id-threading; aggregate keep-all guard).
  - `56b4b31e` fixed 3 SQL-suite regressions: aggregate layout (conservative), `build_distinct`
    id reuse, strip-project id reuse.
  - `c9658049` fixed C.1 (multi-consumer CTE id mapping → CTE prune rules made no-op) and
    C.2 (extra-sort column threading via real `output_column_id`).

## 3. Current correctness state — BROKEN on real queries

`cargo test --lib` is green (3226), but the **SQL suite (`cte`+`join` `-j 1`)** — the only
gate that exercises the full analyzer→planner→pipeline→exec path on real query shapes —
reveals the new pruning drops/mis-maps columns. lib tests + reviews did NOT catch this class
because the bugs live in the planner/analyzer's ColumnId allocation that the rules *consume*,
not in the rules' own logic.

### Suite results (post-`c9658049`, stopped early at join 18/60)

- **cte: 2/3** (was 0/3 before the C.1/C.2 fixes). Remaining: 1 NOT-IN case returns wrong
  rows (`row count mismatch actual=2 expected=0`).
- **join: 13 pass / 5 fail in first 18 cases.** OQ-1-introduced failures observed:
  - `row count mismatch actual=0 expected=3` (silent wrong result — empty)
  - `row count mismatch actual=0 expected=10` (silent wrong result — empty)
  - `empty window_exprs` (codegen error — `PruneWindowColumns` pruned all window_exprs away)
  - 1 more VERIFY/exec fail (not yet isolated)
  - (the 180s timeout on `force_partition_hash` is a **pre-existing** slow case, NOT OQ-1 —
    baseline join `-j1` also timed out there)
- **Baseline reference** (pre-OQ-1, old pruner): join `-j1` = 57/60 (3 pre-existing fails:
  `array_type`, `force_partition_hash`, `full_outer_with_using`).
- Extrapolating ~3-4 OQ-1 regressions per 18 join cases → likely **~10+ OQ-1 regressions** over 60.

### Root-cause classes (the recurring theme)

Every bug is the same shape: **a planner/analyzer site allocates ColumnIds for an operator's
outputs that don't match the ids the parent references / the tagging pass uses, OR the
operator's `output_columns` layout isn't what the rules assume.** Found + fixed so far:
set-op (first-branch vs fresh ids), Project (no output id → added), Aggregate
(`output_columns` is SELECT-ordered, NOT `[group_by ++ aggregates]`), `build_distinct`
(ignored `output_column_id`), strip-project / extra-sort (fresh ids), multi-consumer CTE
(consume↔produce positional mapping). **Still open:** the remaining join wrong-results,
`empty window_exprs`, and the cte NOT-IN wrong result (see §4).

## 4. Remaining work to make it production-correct

Each is a concrete, reproducible bug. Minimal repros use table `t(id bigint, x int, y int)`
with rows (1,10,100),(2,20,200),(3,30,300).

1. **`PruneWindowColumns` prunes all window_exprs → `empty window_exprs`.** The name-matching
   between `output_columns[j].name` and `window_exprs[i].output_name` over-prunes in some
   shapes, leaving zero window exprs (codegen rejects). Likely needs: make `PruneWindowColumns`
   a NO-OP (conservative, like CTE/aggregate were made) until a correct Window output↔expr
   correlation exists. (Window output_columns also use fresh factory ids — see Phase-B notes.)
2. **Join wrong-results (`actual=0`, empty)** — at least 2 join cases return empty where rows
   are expected. Not yet isolated; reproduce from the `join` suite (`-j1`, the first failing
   case ~#6 and ~#10) and trace which column/id the pruning dropped. Likely another
   id-mismatch class (possibly around join + subquery-rewrite synthetic columns, or
   set-op/CTE interaction).
3. **cte NOT-IN wrong result (`actual=2 expected=0`)** — `cte_in_where_subquery.sql`. The
   NOT-IN / null-aware-anti indicator (`__match_0`) interaction with pruning yields extra
   rows. Trace the `__match_0` / NullAwareLeftAnti path under the new pruning.
4. **Deferred optimizations (intentional, not bugs):**
   - Gap-3 CTE column pruning: `PruneCTEConsumeColumns`/`PruneCTEProduceColumns` are currently
     **no-ops** and `tag_cte_anchor` passes `None` to the produce body (keep-all). Re-enabling
     requires a correct consume↔produce positional id contract.
   - Gap-5 aggregate output pruning: `PruneAggregateColumns` is a **no-op**; `tag_aggregate`
     keeps all aggregate inputs. Re-enabling requires an `output_column_id` on `AggregateCall`
     (mirroring the ProjectItem fix) so aggregate outputs are ColumnId-addressable.

### Recommended approach for the next session

Given 3 fix rounds did not converge (each revealed new wrong-results), the safest path to a
mergeable state is: **make every remaining "real" prune rule conservative (no-op / keep-all)
where its output↔id correlation isn't provably safe** (Window, and re-audit set-op + the
subquery-rewrite Project path), get the full `cte`+`join`+`tpc-ds` suites GREEN first, THEN
re-enable pruning operator-by-operator with a golden-plan + full-suite gate per operator.
Do NOT trust lib tests or review alone for this class — the SQL suite is the only real gate.

Alternatively, revert the atomic switch (`4ece15a5`) to restore the old pruner immediately
and keep the tagging pass + rules dormant until the above is done.

## 5. Process lesson

The lib-test + two-stage-review gates verified rule *logic* but not the planner's *ColumnId
contracts* that the rules depend on. For a plan-rewrite that re-keys columns, the end-to-end
SQL suite must be the gate from the first behavior-changing commit — not deferred to the end.
The plan should have run the SQL suite immediately after the atomic switch (Task 24), not
after also writing golden tests.
