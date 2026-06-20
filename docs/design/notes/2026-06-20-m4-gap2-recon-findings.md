# M4 / gap2 Recon Findings (Task 1, load-bearing)

> Read-only investigation done by three parallel recon agents on
> `claude/m4-gap2-transitive-predicates` (from `origin/main`, full optimizer arc merged).
> Goal of Task 1: pin the **precise** gap, replay the rolled-back gap2 blowup, and revise
> Task 2-4 scope. This file = the Task 1 deliverable (feeds the eventual PR body).

## TL;DR

- The transitive **column** equivalence closure **already exists** as an **O(k) union-find
  `EquivalenceClasses`** on `LogicalProperties` (computed by `derive_group_statistics`,
  the step *immediately before* `run_multi_join_reorder`).
- **Constant/literal propagation (`a=b ∧ a=5 ⟹ b=5`) is already fully implemented — twice**
  (RBO `deriver.rs`+`move_around.rs`, in-memo `equivalence_predicate.rs`). M4 must **NOT**
  re-implement it.
- The **single genuine gap**: the reorder graph (`MultiJoinGraph` built by `flatten.rs`) is
  **blind to transitive equi-edges**. It only reads literal conjuncts physically present in
  the chain; it **never consults `equivalence_classes`**. So for `WHERE a.x=b.y AND b.y=c.z`
  it builds edges A–B and B–C but **never A–C**, and any order joining A⋈C directly is costed
  as a penalized cross-join.
- The rolled-back gap2 (`3a368446`, `git reset`-discarded) blew up because it computed its
  **own full C(k,2) closure** of **deep-cloned `TypedExpr`** equalities **inside flatten**,
  with no interning. All three blowup vectors (C(k,2), deep-clone, per-candidate) are now
  closeable: the O(k) closure already exists, M1 interns scalars, and the current
  `run_multi_join_reorder` flattens **once per chain root** (not per candidate).

## Evidence

### Existing derivation machinery (Agent A)
- `rewrite/rules/predicate_pushdown/deriver.rs::derive_inner_join_predicates` (RBO, on
  `OptExpr`/`TypedExpr`) and `cascades_rules/equivalence_predicate.rs`
  (`InnerJoinEquivalencePredicateRule`, in-memo, on `ScalarId`) are **two implementations of
  the same logical rule** — both named `"JoinPredicateMoveAround"`, so one
  `SET disable_optimizer_rules='JoinPredicateMoveAround'` disables both.
- **Already covered:** literal/constant propagation across equi-join keys (RBO also does
  IN-list / one-sided range / BETWEEN / OR-envelope); pushing every derived predicate **down
  onto the child/scan** as a `LogicalFilter`. All single-column-restricted,
  determinism-guarded, type-matched, idempotent (canonical commutative-aware `PredicateKey` +
  "already present in this equivalence class" checks).
- **GAP:** no rule emits a derived transitive **column** equality `a=c`; no rule ANDs a
  derived predicate **into a join's on-condition** (both always rebuild the join with the
  unchanged condition and push derived predicates to children).

### Rolled-back gap2 (Agent B)
- Commit `3a368446` "feat(optimizer): derive transitive equi-join edges for reorder",
  touched **only** `cascades_rules/multi_join_reorder/flatten.rs` (+178/−2). `git reset`-discarded
  (reflog-only; no revert commit). Predates ScalarArena (M0 #331 / M1 #335 are *after* it).
- Blowup mechanism (recovered, quoted): `add_transitive_equi_edges()` ran inside the
  flattener and emitted the **full C(k,2)** pairwise closure as fresh **deep-cloned**
  `TypedExpr` equalities (`Box::new(col_expr[&c1].clone())`). For a class of k columns →
  ~k²/2 deep-cloned predicates; flatten with no interning → cost compounded.
- The join-reorder spec (`2026-06-15-...`) contains **no** "gap2"/"transitive" section; the
  gap2 naming lives only in the memory note + the M4 plan.

### Reorder graph + equivalence classes (Agent C)
- Reorder is **in-memo, one-shot**: `run_multi_join_reorder` at `optimizer/mod.rs:189-195`,
  **after** the full rewrite pipeline (3× predicate pushdown, all pre-memo) and **after**
  `derive_group_statistics` (`mod.rs:180`, which populates `equivalence_classes`).
- `flatten.rs` builds edges **only** from literal `LogicalJoin.condition` /
  `LogicalFilter.predicate` scalars in the chain (`flatten.rs:89-91`, `:99`); an edge exists
  iff a conjunct references columns from ≥2 relations (`relation_mask`). It **never**
  references `equivalence_classes` (grep-confirmed zero hits).
- `EquivalenceClasses` = `Vec<ColumnIdSet>` union-find (`property.rs:77-120`), stored on
  `LogicalProperties` per memo group (`memo.rs:143-148`), populated by
  `logical_props.rs:40-147` for every col=col equality in filters / inner-join conditions /
  hash-join eq_conditions. For `a.x=b.y AND b.y=c.z` the top group's class **already**
  contains `{a.x, b.y, c.z}` — but nothing in flatten/reorder reads it.

## Pinned gap (revises Task 2-4)

The closure exists; the constant propagation exists; the child-pushdown exists. The **only**
missing capability is: **make the reorder graph treat a transitive equivalence class as
mutual joinability, and attach the correct `colA=colC` predicate when a transitive edge is
realized into an actual join** — using the **already-computed O(k) `equivalence_classes`**,
**interned** via M1, **once**, with **no C(k,2)** and **no deep clone** anywhere.

This means Task 2's "constant propagation" sub-item is **already done — drop it**. The work
collapses to: transitive **column**-equality joinability for reorder.

## Design fork (the open decision — see PR/chat)

Two safe ways to surface the transitive edge to reorder; both reuse the existing O(k) class,
both intern, both derive once, both rely on the existing enumeration caps (#321 bushy bound,
`cbo_max_groups`, explore-cap) for search-space bound:

1. **Condition-enrichment before reorder** (StarRocks `equivalenceDerive`-faithful): an
   in-memo pass (extend `equivalence_predicate.rs`) that, before `run_multi_join_reorder`,
   ANDs the O(k) generator transitive equalities into inner-join conditions; flatten
   (unchanged) then sees them as edges. Pro: StarRocks-aligned, also helps non-reorder
   pushdown; Con: emits conjuncts into the plan → wider golden churn + larger blast radius
   (conjuncts flow through explore/cost).
2. **Reorder-graph-internal** (most surgical, what task #30 "喂进 reorder 图" was framed as):
   `flatten`/`MultiJoinGraph` consults `equivalence_classes` to add joinability edges, and
   synthesizes the specific O(k) generator `colA=colC` predicates (interned) **only** when an
   edge is realized into an actual join during injection. Pro: minimal blast radius (transient
   graph + O(k) realized predicates), reuses our O(k) class directly; Con: it is at the
   original blowup site (done safely now: existing O(k) class, interned, once).

Both are memory-safe by construction. Recommendation: **option 2** (most contained, reuses
existing infrastructure, exactly closes the pinned gap). Option 1 is the StarRocks-faithful
alternative if plan-wide transitive pushdown is also wanted.
