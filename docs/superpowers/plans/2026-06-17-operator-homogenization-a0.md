# Operator 同构化（A0，Level 1）Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 把 `optimizer/operator.rs` 里 19 对逐字段完全相同的 `Logical*Op`/`Physical*Op` 合并成单一共享 payload struct，消除重复定义与构造/类型站点的重复，为后续 M3 规则迁移铺平地基。

**Architecture:** Level 1 同构化——**只合并 payload struct，保留 `LogicalX`/`PhysicalX` 两个 enum 变体**。memo 核心（`is_logical()`/`is_physical()` 按变体 `matches!`）完全不动。这是**行为保持的纯重构**：不加新测试，验收 = 现有套件 + optimizer golden + plan 逐字节保持全绿；任何一步若改变了可观测 plan 输出，即是重构 bug。divergent 算子（Join / Aggregate / Distribution）不在本计划范围。

**Tech Stack:** Rust；`cargo build`（dev profile，增量 ~18s）；`cargo test --lib`；sql-test runner（optimizer 套件 golden）；`sed`（macOS：`sed -i ''`）。

**关键事实（已核实）：**
- 19 对逐字段完全相同：Scan、Filter、Project、Sort、Limit、TopN、Window、Union、Intersect、Except、Values、GenerateSeries、TableFunction、Repeat、AssertOneRow、CTEAnchor、CTEProduce、CTEConsume、Decode。
- `is_logical()`（`operator.rs:594`）用 `matches!` 列举所有 `Logical*` 变体；`is_physical = !is_logical`。**保留变体名 = 守恒。**
- `Derive*` trait impl 只在 `Physical*Op` 上（`derive/*.rs` 里 `impl DeriveOutput/DeriveRequired for Physical{X}Op`）；`Logical*Op` 无 Derive impl。合并后这些 impl 平移到共享 struct，**无重复 impl 冲突**。
- divergent（不碰）：`LogicalJoinOp` / `PhysicalHashJoinOp` / `PhysicalNestLoopJoinOp`；`LogicalAggregateOp`（含 inherent impl，`operator.rs:143`）/ `PhysicalHashAggregateOp`；`PhysicalDistributionOp`（物理独有）。

---

## File Structure

- `src/sql/optimizer/operator.rs` — struct 定义 + `enum Operator` + `is_logical()`。合并的主战场。
- 构造站点（`Physical{X}Op { .. }` / `Logical{X}Op { .. }` 字面量）散落在：
  `src/sql/optimizer/convert.rs`（logical 构造）、`src/sql/optimizer/derive/*.rs`（physical 构造 + Derive impl）、
  `src/sql/optimizer/extract.rs`、`src/sql/optimizer/cascades_rules/**`、`src/sql/optimizer/rewrite/**`、
  `src/sql/codegen/**`、`src/sql/optimizer/stats.rs` 等。**由编译器逐一报错引导，无需手工枚举。**
- 不新增文件。共享 struct 留在 `operator.rs` 原位。

---

## Per-Operator Merge Recipe（可复用过程，参数 = `{X}` 算子名）

> 下面每个算子任务都调用此 Recipe。`{X}` 替换为算子名（如 `Filter`），`{shared}` = 去前缀名（如 `FilterOp`）。

- **R1 预检**：确认共享名未被占用。
  Run: `grep -rn "struct {shared}\b" src/`
  Expected: 无输出（仅 `Logical{X}Op` / `Physical{X}Op` 存在）。

- **R2 全局改名**：把两个旧名都改成共享名。
  ```bash
  grep -rl -e 'Logical{X}Op' -e 'Physical{X}Op' src/ \
    | xargs sed -i '' -e 's/Logical{X}Op/{shared}/g' -e 's/Physical{X}Op/{shared}/g'
  ```
  这会把 `enum Operator` 的 `LogicalX(Logical{X}Op)` / `PhysicalX(Physical{X}Op)` 自动变成
  `LogicalX({shared})` / `PhysicalX({shared})`，并把所有构造站点、类型注解、`impl ... for Physical{X}Op` 一并改名。

- **R3 删重复 struct 定义**：`operator.rs` 现在有两个完全相同的 `struct {shared} { .. }`（原 Logical/Physical 各一）。删掉其中一个，只留一份。

- **R4 编译，修残余**：
  Run: `cargo build 2>&1 | tail -20`
  Expected: PASS。若报 `duplicate definition`，回到 R3 删干净；若报个别站点，按提示机械修正（字段完全相同，仅名字问题）。

- **R5 跑测试**（行为不变守门）：
  Run: `cargo test --lib sql::optimizer 2>&1 | tail -20`
  Expected: 全 PASS，数量与基线一致。

- **R6 提交**：
  ```bash
  git add -A && git commit -m "refactor(optimizer): merge Logical{X}Op/Physical{X}Op into shared {shared}"
  ```

---

## Task 1: 准备 + 基线绿 + 分支

**Files:**
- 无代码改动（建分支 + 记录基线）。

- [ ] **Step 1: 从 main 建分支**

```bash
git fetch origin && git switch -c claude/operator-homogenization-a0 origin/main
```

- [ ] **Step 2: 记录基线 optimizer 单测数量（后续每步比对）**

Run: `cargo test --lib sql::optimizer 2>&1 | tail -5`
Expected: 全 PASS。记下 `test result: ok. N passed` 的 N。

- [ ] **Step 3: 确认 19 个共享名当前都不存在（避免改名撞车）**

Run: `for X in Scan Filter Project Sort Limit TopN Window Union Intersect Except Values GenerateSeries TableFunction Repeat AssertOneRow CTEAnchor CTEProduce CTEConsume Decode; do grep -rqn "struct ${X}Op\b" src/ && echo "CONFLICT: ${X}Op"; done; echo done`
Expected: 仅输出 `done`（无 CONFLICT）。若有冲突，该算子改用 `{X}NodeOp` 等替代共享名并在对应任务注明。

---

## Task 2: Filter（完整样板，验证 Recipe 端到端）

**Files:**
- Modify: `src/sql/optimizer/operator.rs`（struct + enum）
- Modify: 构造/类型站点（由编译器引导，预计 `convert.rs`、`derive/passthrough.rs` 等）

- [ ] **Step 1: 预检共享名**

Run: `grep -rn "struct FilterOp\b" src/`
Expected: 无输出。

- [ ] **Step 2: 全局改名**

```bash
grep -rl -e 'LogicalFilterOp' -e 'PhysicalFilterOp' src/ \
  | xargs sed -i '' -e 's/LogicalFilterOp/FilterOp/g' -e 's/PhysicalFilterOp/FilterOp/g'
```

- [ ] **Step 3: 删重复 struct 定义**

`operator.rs` 现有两个相同的 `struct FilterOp { pub predicate: ScalarId }`（原 Logical/Physical 处各一）。删掉一个，只留一份。enum 此时应为：

```rust
    LogicalFilter(FilterOp),
    // ...
    PhysicalFilter(FilterOp),
```

- [ ] **Step 4: 编译**

Run: `cargo build 2>&1 | tail -20`
Expected: PASS。（若 `duplicate definition for FilterOp` → Step 3 没删干净。）

- [ ] **Step 5: 跑 optimizer 单测**

Run: `cargo test --lib sql::optimizer 2>&1 | tail -10`
Expected: 全 PASS，N 与基线一致。

- [ ] **Step 6: 确认变体仍在、memo 核心未动**

Run: `grep -n 'LogicalFilter(\|PhysicalFilter(' src/sql/optimizer/operator.rs`
Expected: `LogicalFilter(FilterOp)` 与 `PhysicalFilter(FilterOp)` 两个变体都在；`is_logical()` 里 `Operator::LogicalFilter(_)` 仍列举。

- [ ] **Step 7: 提交**

```bash
git add -A && git commit -m "refactor(optimizer): merge LogicalFilterOp/PhysicalFilterOp into shared FilterOp"
```

---

## Task 3: AssertOneRow + Decode（trivial 叶/一元，各一次 Recipe）

**Files:** `src/sql/optimizer/operator.rs` + 构造站点（编译器引导）。

- [ ] **Step 1: AssertOneRow** — 按 Recipe，`{X}=AssertOneRow`、`{shared}=AssertOneRowOp`。R1→R6。
- [ ] **Step 2: Decode** — 按 Recipe，`{X}=Decode`、`{shared}=DecodeOp`。R1→R6。

提交信息分别：
```
refactor(optimizer): merge LogicalAssertOneRowOp/PhysicalAssertOneRowOp into shared AssertOneRowOp
refactor(optimizer): merge LogicalDecodeOp/PhysicalDecodeOp into shared DecodeOp
```

---

## Task 4: CTEAnchor + CTEProduce + CTEConsume

**Files:** `src/sql/optimizer/operator.rs`、`src/sql/optimizer/derive/cte.rs`（`impl DeriveOutput/DeriveRequired for PhysicalCTEAnchorOp` 会被 R2 自动改名到 `CTEAnchorOp`，无需手动）+ 构造站点。

- [ ] **Step 1: CTEAnchor** — Recipe，`{X}=CTEAnchor`、`{shared}=CTEAnchorOp`。R1→R6。
  注：R2 后 `derive/cte.rs` 的两条 impl 变成 `impl ... for CTEAnchorOp`（单一类型，不冲突，因 Logical 侧无 Derive impl）。R4 编译确认无 `conflicting implementations`。
- [ ] **Step 2: CTEProduce** — Recipe，`{X}=CTEProduce`、`{shared}=CTEProduceOp`。R1→R6。
- [ ] **Step 3: CTEConsume** — Recipe，`{X}=CTEConsume`、`{shared}=CTEConsumeOp`。R1→R6。

---

## Task 5: GenerateSeries + Limit

**Files:** `src/sql/optimizer/operator.rs` + 构造站点。

- [ ] **Step 1: GenerateSeries** — Recipe，`{X}=GenerateSeries`、`{shared}=GenerateSeriesOp`。R1→R6。
- [ ] **Step 2: Limit** — Recipe，`{X}=Limit`、`{shared}=LimitOp`。R1→R6。

---

## Task 6: Project + Window

**Files:** `src/sql/optimizer/operator.rs` + 构造站点（Project 在 codegen 多处；Window 有 `impl Derive* for PhysicalWindowOp`）。

- [ ] **Step 1: Project** — Recipe，`{X}=Project`、`{shared}=ProjectOp`。R1→R6。
- [ ] **Step 2: Window** — Recipe，`{X}=Window`、`{shared}=WindowOp`。R1→R6。R4 确认 `derive/window.rs` 的 impl 平移无冲突。

---

## Task 7: Union + Intersect + Except

**Files:** `src/sql/optimizer/operator.rs`、`src/sql/optimizer/derive/set_op.rs` + 构造站点。

- [ ] **Step 1: Union** — Recipe，`{X}=Union`、`{shared}=UnionOp`。R1→R6。
- [ ] **Step 2: Intersect** — Recipe，`{X}=Intersect`、`{shared}=IntersectOp`。R1→R6。
- [ ] **Step 3: Except** — Recipe，`{X}=Except`、`{shared}=ExceptOp`。R1→R6。

---

## Task 8: TableFunction + Repeat

**Files:** `src/sql/optimizer/operator.rs` + 构造站点。

- [ ] **Step 1: TableFunction** — Recipe，`{X}=TableFunction`、`{shared}=TableFunctionOp`。R1→R6。
- [ ] **Step 2: Repeat** — Recipe，`{X}=Repeat`、`{shared}=RepeatOp`。R1→R6。

---

## Task 9: Sort + TopN（带 Derive impl，高构造量）

**Files:** `src/sql/optimizer/operator.rs`、`src/sql/optimizer/derive/sort.rs`、`src/sql/optimizer/derive/top_n.rs` + 构造站点（Sort physical 19 处、TopN 14 处）。

- [ ] **Step 1: Sort** — Recipe，`{X}=Sort`、`{shared}=SortOp`。R1→R6。R4 确认 `derive/sort.rs` 的 `impl DeriveOutput/DeriveRequired for SortOp` 单一无冲突。
- [ ] **Step 2: TopN** — Recipe，`{X}=TopN`、`{shared}=TopNOp`。R1→R6。R4 确认 `derive/top_n.rs` impl 平移无冲突。

---

## Task 10: Scan（最高构造量，单独一任务）

**Files:** `src/sql/optimizer/operator.rs`、`src/sql/optimizer/derive/scan.rs` + 构造站点（physical 35 处 / 12 文件）。

- [ ] **Step 1** — Recipe，`{X}=Scan`、`{shared}=ScanOp`。R1→R6。R2 改名量大但纯机械；R4 编译逐个修残余。

---

## Task 11: Values（最高 logical 构造量，单独一任务）

**Files:** `src/sql/optimizer/operator.rs` + 构造站点（logical 39 处 / 9 文件，含多处测试 fixture）。

- [ ] **Step 1** — Recipe，`{X}=Values`、`{shared}=ValuesOp`。R1→R6。

---

## Task 12: or-pattern 收口（可选 polish，谨慎）

合并完 struct 后，部分 `match` 仍有 `Operator::LogicalX(op) => BODY` 与 `Operator::PhysicalX(op) => BODY` **两条 body 完全相同**的臂——可收成 or-pattern。**注意**：多数 handler 对 logical/physical 处理不同（`derive/*` 只处理 physical、`logical_props` 只处理 logical），这类**不能**合并；只收 body 逐字相同的臂。这是 polish，价值有限（主要 logic 减少来自 struct 去重，已在 Task 2-11 完成）。

**Files:** 在 Task 2-11 过程中遇到的、有 `LogicalX`/`PhysicalX` 双臂且 body 相同的 `match`（如某些 explain 名称、output_columns 提取 helper）。

- [ ] **Step 1: 找候选**

Run: `grep -rnB1 -A3 'Operator::Logical' src/sql/optimizer/*.rs src/sql/codegen/**/*.rs | grep -A3 -B1 'Operator::Physical' | head -60`
人工挑出 body 逐字相同的双臂。

- [ ] **Step 2: 逐个收 or-pattern**（仅 body 完全相同者）

```rust
// before
Operator::LogicalSort(op) => op.items.len(),
Operator::PhysicalSort(op) => op.items.len(),
// after
Operator::LogicalSort(op) | Operator::PhysicalSort(op) => op.items.len(),
```

- [ ] **Step 3: 编译 + optimizer 单测**

Run: `cargo build 2>&1 | tail -5 && cargo test --lib sql::optimizer 2>&1 | tail -5`
Expected: 全 PASS。

- [ ] **Step 4: 提交**

```bash
git add -A && git commit -m "refactor(optimizer): collapse identical logical/physical match arms into or-patterns"
```

---

## Task 13: 全量验收门

**Files:** 无改动，纯验证。

- [ ] **Step 1: fmt + clippy**

Run: `cargo fmt && cargo clippy --lib 2>&1 | tail -20`
Expected: fmt 无 diff；clippy 无 error（warning 与基线一致）。

- [ ] **Step 2: 全 lib 单测**

Run: `cargo test --lib 2>&1 | tail -20`
Expected: 全 PASS（含 codegen / stats / derive 模块）。

- [ ] **Step 3: 确认 19 对旧名已彻底消失，divergent 仍在**

Run: `grep -rn -e 'Logical\(Scan\|Filter\|Project\|Sort\|Limit\|TopN\|Window\|Union\|Intersect\|Except\|Values\|GenerateSeries\|TableFunction\|Repeat\|AssertOneRow\|CTEAnchor\|CTEProduce\|CTEConsume\|Decode\)Op\b' -e 'Physical\(Scan\|Filter\|Project\|Sort\|Limit\|TopN\|Window\|Union\|Intersect\|Except\|Values\|GenerateSeries\|TableFunction\|Repeat\|AssertOneRow\|CTEAnchor\|CTEProduce\|CTEConsume\|Decode\)Op\b' src/`
Expected: 无输出（旧名全消）。
Run: `grep -rn 'LogicalJoinOp\|PhysicalHashJoinOp\|PhysicalNestLoopJoinOp\|LogicalAggregateOp\|PhysicalHashAggregateOp\|PhysicalDistributionOp' src/ | wc -l`
Expected: > 0（divergent 算子原样保留）。

- [ ] **Step 4: optimizer golden 套件（plan 逐字节不变）**

先按 CLAUDE.md 起 standalone-server（`source docker/iceberg-rest/runtime/current/env.sh` 后启动并等 `NOVAROCKS_READY`），再：
```bash
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" --suite optimizer --mode verify
```
Expected: 全 PASS（plan-golden 逐字节不变——A0 行为保持的核心证据）。

- [ ] **Step 5: TPC-DS SF1 全量（dev-opt，串行）**

```bash
cargo build --profile dev-opt
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" --suite tpc-ds --mode verify -j 1
```
Expected: 99/99 PASS。

- [ ] **Step 6: 推 fork + 开 PR**

```bash
git push fork claude/operator-homogenization-a0
gh pr create --repo NovaRocks/NovaRocks --base main --head HangyuanLiu:claude/operator-homogenization-a0 \
  --title "refactor(optimizer): A0 — homogenize identical Logical*/Physical* operator structs" \
  --body "Level-1 homogenization (see docs/design/specs/2026-06-17-unified-plan-node-and-optimizer-encapsulation.md §4.8): merge 19 field-identical Logical*Op/Physical*Op into shared payload structs; keep Logical/Physical enum variants (memo core untouched). Behavior-preserving: optimizer golden byte-identical, TPC-DS 99/99."
```

---

## Self-Review

**1. Spec coverage（对 §4.8 / 决策⑧⑨）：**
- §4.8 Level 1「19 个透传算子各只保留一个 payload struct、保留两变体、memo 核心不动」→ Task 2-11 逐个落实，Task 6/9 Step 验证 Derive impl 平移无冲突，Task 2 Step 6 验证变体与 `is_logical()` 守恒。✓
- §4.8「struct ~46→~25」→ Task 13 Step 3 验证 19 对旧名全消、divergent 保留。✓
- §4.8「透传 handler 用 or-pattern 合并」→ Task 12。✓
- 决策⑧「Join/Aggregate/Distribution 保留分歧」→ 全程不碰，Task 13 Step 3 验证仍在。✓
- 决策⑨「A0 先行，规则一次性落在已同构 Operator」→ 本计划即 A0，A1+ 另出 plan。✓

**2. Placeholder 扫描：** Recipe 与各任务均为具体命令 + 具体 struct 名；无 TBD/“类似 Task N”（Recipe 是显式可复用过程，参数化而非省略）。✓

**3. 类型一致性：** 共享名统一为去前缀 `{X}Op`（`FilterOp`/`ScanOp`/…）；变体名 `LogicalX`/`PhysicalX` 全程不变（守恒 `is_logical()`）；divergent 名单（Join/Aggregate/Distribution）三处一致。✓

**4. 风险点复核：** 唯一非纯机械处 = Derive impl 合并后是否撞重复 impl —— 已核实 Logical 侧无 Derive impl，故无冲突（Task 4/6/9 显式 R4 编译验证）。高构造量算子（Scan/Values/Sort/TopN）单列任务，编译器逐个引导。✓

---

## Execution Handoff

**Plan complete and saved to `docs/superpowers/plans/2026-06-17-operator-homogenization-a0.md`. Two execution options:**

**1. Subagent-Driven (recommended)** — 每个 Task 派新 subagent，任务间 review，快速迭代。

**2. Inline Execution** — 本会话内按 executing-plans 批量执行 + 检查点。

**Which approach?**
