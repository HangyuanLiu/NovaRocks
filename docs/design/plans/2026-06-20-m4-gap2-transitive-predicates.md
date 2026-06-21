# M4 — gap2 重做:传递等值谓词(安全,StarRocks 对齐)Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: superpowers:subagent-driven-development 或 executing-plans。**执行纪律**:串行受控 on-branch;**不放 isolated-worktree swarm**(A2 教训)。**内存安全是硬约束**(gap2 上一版在 TPC-DS q72 打满宿主机内存被回滚——见 [[project_join_reorder_in_memo_multi_candidate]])。

**Goal:** 让 join reorder 能用上**传递等值谓词**(`a=b ∧ b=c ⟹ a=c`、`a=b ∧ a=5 ⟹ b=5`),从而开出更优 join 序 + 更多 filter 下推——但**绝不重蹈 q72 内存爆炸**。这是整条 scalar-IR/unified-plannode arc 的初始动机与 payoff(arc 已为它打好地基:M1 让派生谓词 intern 去重不深拷、A2 让 rewrite 在 OptExpr 上)。

**为什么现在安全(arc 已解锁):** q72 当年爆 = 在 **flatten(in-memo reorder)里 per-候选 把全 C(k,2) 闭包 AND 进 join 条件 + 深拷贝** → 派生数 × 候选数 × 深拷。现在:① M1 标量 intern(派生谓词是共享 `ScalarId`,不深拷);② 改对齐 StarRocks——**在 rewrite 阶段派生一次(reorder 之前),reorder 只引用不复制**。

**StarRocks 对齐(调研结论,2026-06-20):** `ScalarEquivalenceExtractor` + `JoinPredicatePushdown::equivalenceDerive`(在 `PUSH_DOWN_PREDICATE_RULES`,**ReorderJoinRule 之前**一次性做)。控规模 6 招:**① 闭包用 O(k) 生成集形式(canonical 代表:`ref=col_i`,k-1 条)而非全 C(k,2);② commutative 去重(a=b≡b=a);③ 单列限制(不发 f(多列)=const);④ 派生/reorder 两阶段分离(派生在 reorder 前固定,reorder 不重算/不复制);⑤ Hash-set 去重;⑥ 按 join 类型应用(INNER/SEMI:AND 进 on-condition 一次;OUTER:单边下推为独立 Filter)**。常量沿等值类传播(`a=b ∧ a=5 ⟹ b=5`)是同一机制。

**Tech Stack:** Rust;`cargo build/test --lib`;sql-test(optimizer golden + **tpc-ds 全 99,尤其 q72**);内存哨兵(ulimit 或 q72 监控)。

---

## 关键现状(recon 已发现 —— M4 不是从零)
NovaRocks **已有相当多传递谓词机器**,M4 须先审清、避免重复/冲突:
- `rewrite/rules/predicate_pushdown/deriver.rs::derive_inner_join_predicates(...)` —— rewrite 阶段 inner-join 谓词派生(已存在!)。
- `cascades_rules/equivalence_predicate.rs` —— in-memo "inner join equivalence predicate propagation"。
- `rewrite/rules/predicate_pushdown/move_around.rs::JoinPredicateMoveAround` —— 谓词在 join 间搬运。
- `rewrite/rules/predicate_pushdown/{predicate_group.rs, classifier.rs}` —— `PredicateGroup`/`PredicateOrigin`/`PredicateDerivedKind`(origin/derived 追踪 = **幂等基础**)。
- `property.rs::EquivalenceClasses` + memo/logical_props —— memo 侧等值类。
- reorder 在 memo 内(`mod.rs:190` `run_multi_join_reorder`,在 convert+derive_stats 之后、rewrite 之后)。flatten 建 `MultiJoinGraph`。

**所以 gap2 的精确缺口需要先定**(Task 1):很可能不是"加派生",而是"**让 reorder 的 join 图(MultiJoinGraph)看见传递等值边**"——现有 deriver/equivalence_predicate 把传递等值派生到哪一层?是否已进 join 条件让 flatten 看见?gap2 想补的到底是 reorder 图的哪条边?

---

## Task 1（recon,load-bearing):定准缺口 + 复盘 q72 爆点
**Files:** 只读 + 写一份 findings(可放 PR 描述)。
- [ ] **审现有派生链**:读 `deriver.rs::derive_inner_join_predicates`(它派生什么:全闭包?O(k)?常量?应用到哪——join 条件 / 下推 scan?)、`equivalence_predicate.rs`(in-memo 传播什么)、`move_around.rs`、`predicate_group.rs`(幂等/dedup 如何做)。登记"已覆盖 vs 缺口"。
- [ ] **复盘 gap2**:读 join-reorder spec `docs/design/specs/2026-06-15-join-reorder-in-memo-multi-candidate.md` 的 gap2 段 + 尝试 `git log --all --oneline | grep -i 'gap2\|transitive'` 找回滚前的 gap2 commit(`git show <sha>`)看它**具体改了哪(flatten?加边?AND 条件?)**——确认爆点 = flatten per-候选 AND 全闭包 + 深拷。
- [ ] **产出**:一页 findings——(a) 现有派生覆盖到哪;(b) gap2 想补的精确缺口(大概率:reorder 的 `MultiJoinGraph` 需把等值类内的传递对当作可用 join 边,以开出 `a⋈c` 这类直接序);(c) 安全设计落点(rewrite 阶段 vs flatten;**强烈倾向 rewrite 阶段派生 + reorder 只读**,对齐 StarRocks)。
- [ ] **据 findings 修订下面 Task 2-4 的精确范围**(本 plan 给的是 StarRocks 对齐的目标形态;Task 1 把它锚到 NovaRocks 现状)。

## Task 2:rewrite 阶段一次性派生(O(k) 生成集 + 常量传播 + intern)
**Files:** `predicate_pushdown/deriver.rs`(扩展或新增)、`predicate_group.rs`(幂等)、复用 `EquivalenceClasses`/union-find
- [ ] **Step 1:** 在 rewrite 阶段(predicate_pushdown stage,**memo/reorder 之前**)对等值谓词建等值类(union-find over `ColumnId`);每类发 **O(k) 生成集**(挑 canonical 代表,发 `member_i = canonical`,k-1 条)——**不发全 C(k,2)**;常量沿类传播(`a=b ∧ a=5 ⟹ b=5`);单列限制(不发 f(多列)=const)。派生谓词经 **`ctx.scalar_arena().intern_typed`** 入 arena(M1:相同边去重、共享 `ScalarId`、不深拷)。
- [ ] **Step 2:** 按 join 类型应用:INNER/SEMI → AND 进 on-condition(一次);可下推的单列/常量谓词 → 下推 scan;OUTER → 单边下推为 Filter。**用 `predicate_group.rs` 的 origin/derived-kind 标记保证幂等**(rewrite fixpoint 不重复派生、不无限增长)。
- [ ] **Step 3:** build + `cargo test --lib sql::optimizer::rewrite`(+ 新增单测:等值类派生 O(k)、常量传播、幂等不重复、单列限制)。commit。

## Task 3:reorder 图消费传递边(若 Task 1 判定需要)
**Files:** `cascades_rules/multi_join_reorder/flatten.rs`(`MultiJoinGraph` 构建)
- [ ] **仅当 Task 1 findings 表明 reorder 图仍看不到 Task 2 派生的边时才做**:让 flatten 从(已被 Task 2 enriched 的)join 条件 + 等值类**读取**传递等值边作为可用 join 边——**只读引用 `ScalarId`,绝不在 flatten 里重新计算闭包、绝不 per-候选 AND/深拷**(这正是 q72 爆点,严禁)。
- [ ] 若 Task 2 的 rewrite 派生已让 flatten 自然看见(因边已在 join 条件里),**本 task 可空**。
- [ ] build + 单测 + commit。

## Task 4:安全 + 规模哨兵(硬约束)
- [ ] **大等值类有界性单测**:构造 k=8/12/16 列的等值类,断言派生谓词数 = O(k)(≤ ~k,非 ~k²/2);断言 reorder 候选注入不随 k 组合爆炸(复用 join-reorder 的 enumerate 有界性测试模式)。
- [ ] **可选硬 backstop**:等值类列数超阈值(如 >32)时降级(只发部分/跳过派生)——纯防御,O(k)+阶段分离本应足够;log 降级。
- [ ] commit。

## Task 5:验收(q72 是非负不可)
- [ ] `cargo fmt && cargo clippy --lib`(无 error);`cargo test --lib`(全绿)。
- [ ] **TPC-DS SF1 全 99(dev-opt,串行 -j1),尤其 q72 —— 在内存哨兵下跑**(`ulimit -v` 限内存 或 监控 RSS):**q72 必须不 OOM、正常完成**。这是 gap2 的核心回归门。
- [ ] optimizer golden + TPC-DS verify:确认 plan 变化都是**改善**(更多下推/更优序),无正确性回归;有 plan 改善则 re-record golden。
- [ ] A/B:开/关本派生(session var gate,如 `enable_transitive_predicate`)对比 q4/q11/q31/q72 的 EXPLAIN + 中间行数,佐证收益。
- [ ] push fork + PR(`--base main --head HangyuanLiu:claude/m4-gap2-transitive-predicates`),body:StarRocks 对齐(O(k)+阶段分离+intern)、为何这次内存安全、q72 实测不 OOM + 收益。

---

## Self-Review
**动机/对齐:** gap2 重做 = arc payoff;StarRocks `equivalenceDerive`(O(k) 生成集 + 派生/reorder 两阶段分离 + 按 join 类型应用 + 常量传播)是设计基准;M1 intern + rewrite-阶段-一次性 = 内存安全根因。✓
**recon-first(诚实):** NovaRocks 已有 deriver/equivalence_predicate/move_around 等机器,M4 的精确缺口必须先审清(Task 1),否则重复造轮子或定错落点。本 plan 给目标形态,Task 1 锚定现状。
**内存安全(硬约束):** 严禁在 flatten per-候选 AND 全闭包/深拷(q72 爆点);派生在 rewrite 一次、O(k)、intern、reorder 只读;Task 4 有界性单测 + 可选硬 backstop;Task 5 q72 内存哨兵门非负。
**风险:** ① Task 1 可能发现现有 deriver 已覆盖大半 → M4 缩为"补 reorder 图可见性"或"补常量传播",范围据 findings 收;② 幂等(rewrite fixpoint 不无限派生)——靠 `predicate_group` 的 origin/derived 标记;③ 派生引入的 plan 变化要逐一确认是改善非回归(golden + A/B)。

## Execution Handoff
串行受控。**Task 1(recon)先行且 load-bearing**——它把"StarRocks 目标形态"锚到 NovaRocks 现状、定准缺口、复盘 q72 爆点;之后 Task 2-5 按定准的缺口实施。**q72 内存哨兵门(Task 5)是 gap2 的非负红线。**
