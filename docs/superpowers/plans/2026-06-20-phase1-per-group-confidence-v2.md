# Phase 1: per-group 统计切换 + 可信度模型 v2 — 实现计划

> **For agentic workers:** REQUIRED SUB-SKILL: superpowers:subagent-driven-development。Steps use checkbox (`- [ ]`).

**Goal:** 把统计模型切到 per-group(search/cost 读 group 单一统计),坍缩从「取第一个成员」改成「按可信度字典序 argmax」,并落地可信度模型 v2(来源轴加 `Measured` 档 stub + 可推导性轴 `DerivePromise`)。

**Architecture:** group 的代表统计由 `representative_member` 按字典序 `(source_confidence, derive_promise)` argmax 选出(FFewerConj inner-only sub-tie、ties→first,全平局退化成今天的 first(),strict refinement)。search 的 per-expr `own_stats` 改读 group 缓存(`stats_for_group`)并删除——已验证对 agg **plan-inert**(agg cost 纯 child-driven)、对其它读 own_stats 的算子 **value-identical**(同组成员逻辑等价)。预期 golden churn 近零(roadmap:Phase 1 inert until Phase 2 加 agg memory cost)。

**Baseline:** worktree `worktree-phase1-per-group-confidence` from 最新 origin/main(含 #327 A1+B'、#334 Phase 0 cap、#335 ScalarArena cutover、#337-342 OptExpr/Bridge 重构)。已确认:agg/B'/A1 测试 baseline 绿;ScalarArena cutover 不阻碍(join 算子仍 `join_type`+children-GroupIds,promise 只需算子 KIND + child 形状)。

**Spec:** `docs/design/specs/2026-06-16-optimizer-statistics-model-roadmap.md`(在 zen-clarke worktree,未 push)§4.2/§4.5/§5。

---

## 落地点(current main,已 workflow 重新映射)

| 设计点 | 位置 |
| --- | --- |
| 坍缩选择点(取 first) | `stats.rs:1103-1108` derive_group_statistics_for;`search.rs:301-305` stats_for_group;`logical_props.rs:23` derive_for_group |
| A1 记忆化 guard | `stats.rs:1080`(is_some skip);两次 derive `mod.rs:180`/`mod.rs:217` |
| append 到已有 group | `memo.rs:106` add_expr_to_group;explore `mod.rs:345`、implement |
| search own_stats | `search.rs:133`(用于 :207);`stats_for_group` `search.rs:286` |
| Confidence | `statistics.rs:13`;consumer:`cost.rs:231`(!=Exact)、`aggregate_pushdown/cost.rs:62`、`explain.rs:947`(match arms) |
| join 算子(promise 读形状) | `operator.rs:193-196`/`376-381`(join_type + children) |

---

## Task 1: Confidence 加 `Measured` 档 + 强制 consumer 编辑

**Files:** `statistics.rs`、`cost.rs`、`rewrite/rules/aggregate_pushdown/cost.rs`、`explain.rs`

- [ ] **Step 1**: `statistics.rs:13` 加顶档(derive(Ord) 定序):
```rust
pub enum Confidence { Fallback, Estimated, Exact, Measured }
```
doc 注明:`Measured` = MV 物化行数/runtime-feedback/采样,**当前无 producer(stub)**,inert until 实测来源落地。
- [ ] **Step 2**: 强制 consumer 编辑(加档即编译/逻辑必需):
  - `cost.rs:231` `!= Confidence::Exact` → `< Confidence::Exact`(否则未来 Measured build 被当不可信)。
  - `aggregate_pushdown/cost.rs:62` 和 `explain.rs:947` 的 `match` on Confidence 加 `Measured` 臂(folded with `Exact` 或独立,按各处语义)。
- [ ] **Step 3**: 加单测 `assert!(Measured > Exact && Exact > Estimated && Estimated > Fallback)`(防未来 enum 重排破坏 `< Exact`)。
- [ ] **Step 4**: `cargo test --lib sql::optimizer` 绿。
- [ ] **Step 5**: commit `feat(optimizer): add Measured confidence rung (stub) for source-axis collapse`

## Task 2: `DerivePromise` + `promise(op)`(可推导性轴)

**Files:** `statistics.rs`(enum)、`stats.rs` 或新 helper(promise fn)

- [ ] **Step 1**: 加 enum(与 Confidence 分开,不 fold):
```rust
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Debug)]
pub(crate) enum DerivePromise { Low, Medium, High }
```
- [ ] **Step 2**: 加 `promise(op: &Operator, children: &[GroupId], memo: &Memo) -> DerivePromise`:默认 `High`;join(Logical/PhysicalHashJoin/PhysicalNestLoopJoin)的 child group 首表达式本身是 join(bushy/reorder-展开)→ `Medium`,否则 `High`;`Low` 保留 typed-but-unproduced(注释:子查询 pre-memo decorrelate 不进 memo,首个未来消费者=in-memo decorrelation)。
```rust
match op {
    Operator::LogicalJoin(_) | Operator::PhysicalHashJoin(_) | Operator::PhysicalNestLoopJoin(_) => {
        let bushy = children.iter().any(|&c| matches!(
            memo.groups[c].logical_exprs.first().map(|e| &e.op),
            Some(Operator::LogicalJoin(_))
        ));
        if bushy { DerivePromise::Medium } else { DerivePromise::High }
    }
    _ => DerivePromise::High,
}
```
- [ ] **Step 3**: 单测:bushy join(child 是 join)→ Medium;base join → High;非 join → High。
- [ ] **Step 4**: `cargo test --lib` 绿。commit `feat(optimizer): add DerivePromise derivability axis (promise by join shape)`

## Task 3: 坍缩字典序 argmax + 共享 helper + member-consistency

**Files:** `stats.rs`(helper + derive_group_statistics_for)、`logical_props.rs`、`search.rs`(stats_for_group)

- [ ] **Step 1**: 加共享 helper(放 stats.rs 或 memo.rs),扫 `logical_exprs ++ physical_exprs`,对每个成员算 key `(member_stats.row_count_confidence, promise(op, children, memo))`,取字典序 max;FFewerConj(**inner-join only**)sub-tie;ties→最低 index(canonical-first)。返回 chosen member(index/ref)+ 其完整 Statistics。
  - 注意:argmax key 的 source_confidence 需 per-member derive(O(成员数) — winner 的完整 Statistics 复用)。strict-greater 替换从 none 起 → 全平局保留最低 index = 退化成今天 first()。
- [ ] **Step 2**: `derive_group_statistics_for`(stats.rs:1103)用 helper 取 chosen member + Statistics。
- [ ] **Step 3**: `logical_props::derive_for_group`(logical_props.rs:23)**接收 chosen member**(从 derive_group_statistics_for thread 进去),不再自己 re-pick first()(**member-consistency**:统计与结构属性同源)。
- [ ] **Step 4**: `stats_for_group`(search.rs:301)的 fallback 路径(logical_props 为 None 时)也用 helper。
- [ ] **Step 5**: 单测:构造一个 group 含 high-promise 与 low-promise 成员,断言 argmax 选 high-promise;全同则选 first(退化);member-consistency 测试(derive_for_group 用 argmax 同一成员)。
- [ ] **Step 6**: `cargo test --lib sql::optimizer` 绿。commit `feat(optimizer): collapse group stats by lexicographic confidence argmax (shared helper)`

## Task 4: A1 guard 共存(invalidate on append)

**Files:** `memo.rs`(add_expr_to_group)或 explore/implement append 站点

- [ ] **Step 1**: 当往一个 `logical_props.is_some()` 的 group append 成员时,reset 该 group `logical_props = None`(在 `add_expr_to_group` memo.rs:106,或 explore mod.rs:345 / implement append 站点)。这样 `mod.rs:217` 的 post-implement derive 会用全成员重跑 argmax,看到 late-appended 的更高可信度成员。保留 #327 记忆化(没 gain 成员的 group 不重算)。更新 stats.rs:1077-1079 的 INVARIANT 注释。
- [ ] **Step 2**: 单测:group derive 后 append 一个更高 promise 成员 → logical_props 被 reset → 重 derive 后 argmax 选新成员。
- [ ] **Step 3**: `cargo test --lib sql::optimizer` 绿。commit `fix(optimizer): invalidate group stats on member append so argmax sees late members`

## Task 5: search own_stats 改读 group 缓存(删 per-expr,在 T3/T4 后)

**Files:** `search.rs`

- [ ] **Step 1**: `search.rs:125-133` 的 `for expr_idx` 循环里,`let own_stats = derive_statistics(expr, memo, &self.table_stats)` 改成 `stats_for_group(&memo.groups[group_id], memo, &self.table_stats)` 并**hoist 到 expr 循环外**(per group 一次)。删除 per-expr derive 路径 + 旧注释(search.rs:128-132)。
  - 已验证 inert:agg cost(cost.rs:129-141)纯 child-driven 不读 own_stats;读 own_stats 的算子(Scan/Filter/Project/Sort/Distribution/Union,cost.rs:80-189)同组成员逻辑等价 → group 代表统计 value-identical。
- [ ] **Step 2**: `cargo test --lib sql::optimizer` 绿(search 的 cost/winner 测试不变)。commit `perf(optimizer): read per-group stat in search instead of per-expr own_stats`

## Task 6: 测试整理 + golden 回归

**Files:** 测试 + golden

- [ ] **Step 1**: B' 测试(stats.rs:3376,`physical_hash_aggregate_own_stats_are_per_expr_not_per_group`)现在已适配为 between-group child-sensitivity(big NDV=100 vs small NDV=50)。它验证的是 deriver child-sensitive(不是 within-group)。**重命名**为 `derive_statistics_is_child_sensitive_across_groups` + 更新注释说明它是 between-group(不再暗示 per-expr within-group)。
- [ ] **Step 2**: 全 lib:`cargo test --lib` 绿(逐个核对任何 stats 断言 shift 是合理后果)。
- [ ] **Step 3**: optimizer golden:启动 server + `sql-tests --suite optimizer --mode diff`。预期 **near-zero churn**(Phase 1 inert)。任何 diff 人工确认合理(应几乎没有)。若有,record 重录。
- [ ] **Step 4**: commit 测试整理(+ golden 若有)。

---

## 依赖顺序

T1(Measured) + T2(DerivePromise)独立 → **T3(argmax,用 T1/T2)** → T4(invalidate,配 T3)→ **T5(swap,必须在 T3/T4 后)** → T6(测试 + golden)。

## Notes

- dev profile;commit message 英文;**无 `Co-Authored-By`**。
- ScalarArena cutover 不影响:promise/derive_statistics 只需算子 KIND + child-group 形状,不碰标量内容。
- **不做**(留未来):Measured 的 producer(无实测来源)、DerivePromise::Low 的 producer、agg memory cost(Phase 2)、真正用 Measured 的 plan 翻转。
- 核心 inert 论据:同 group 成员逻辑等价 → 任何读 own_stats 的算子拿 group 代表统计 value-identical;agg 不读 own_stats。所以 T5 swap 近零 churn。
