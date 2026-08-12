---
id: ADR-0058
title: "Crate boundaries enforce architectural isolation, not source-shape guards"
domain: [crate-boundary]
status: active
supersedes: []
superseded-by: null
date: 2026-08-12
provenance:
  - "discussion: 2026-08-12 aggregate-core residue retirement, on deleting novarocks/core/tests/architecture_guard.rs"
  - "PR: pending — deletion of the source-shape guard ships with the aggregate-core residue owner cut"
code-anchors: []
---

## 问题

「A 不许依赖 B」这类架构隔离约束，应该由什么机制强制？是扫描源码形状的测试，
还是 crate 依赖图本身？

## 背景与执行事实

`novarocks/core/tests/architecture_guard.rs` 曾用 539 行手写 Rust tokenizer 强制三条隔离：
Backend 生产代码不依赖 SQL、Execution 生产代码不依赖 SQL、native encoder 只经
`plan_read` 门面读 SQL。它自己实现了词法扫描、`#[cfg(test)]` 区间剔除、
raw identifier 与 brace-group use 路径的识别。

两个执行事实促成本裁决：

1. **10 个测试里有 7 个在测 tokenizer 自己**（分类器接受/拒绝哪些 use 形态、
   是否正确剔除 `#[cfg(test)]`），只有 3 个在断言架构约束。维护成本主要花在
   扫描器而非架构上。
2. **`execution_production_source_does_not_depend_on_sql` 扫描 `core/src/exec`，
   而该目录在 single-fragment kernel 迁入 `novarocks-execution`（PR #859）后已不存在。**
   guard 从那时起就以「目录读不到」持续失败，直到一次完整 `cargo test --workspace`
   才被发现——期间的 `cargo check --workspace --all-targets` 只编译不运行它。
   一个在被守护对象消失后仍「失败着」而无人察觉的守卫，无法证明它能发现回归。

与此同时，同一条约束已经被 crate 图无成本地强制：`novarocks-execution` 的依赖
只有 `novarocks-types` 与 `novarocks-spi`，Cargo 根本不允许它命名 SQL。

## 考虑过的选项

1. **修复路径并继续维护 tokenizer guard。** 让它指向 `novarocks/execution/src`。
   保留了三条断言，但继续为一个只在完整测试运行中生效、且其多数测试在测自身的
   扫描器付出维护成本；也不解决「源码形状检查会随目录搬迁静默失效」的根因。
2. **换成依赖图断言**（`cargo-deny` / `cargo metadata` 校验）。比 token 扫描健壮，
   但当前三条约束里已有一条被 Cargo 天然覆盖，另两条要等 SQL compiler 物化成独立
   crate 后才存在可断言的依赖边——现在建这套机制会先服务于一个空集。
3. **删除，隔离交给 crate 边界。**（采纳）

## 裁决

删除 `novarocks/core/tests/architecture_guard.rs`。架构隔离由 crate 依赖图强制：
一个 crate 不能命名它没有依赖的 crate，这条约束由编译器而非测试保证，且不会因为
目录改名或模块搬迁而静默失效。

推论：**当一条隔离约束需要靠扫描源码形状才能表达时，正确的反应是把边界物理化成
crate，而不是写扫描器。** 扫描器只能描述当前的文件布局，crate 图描述的是依赖事实。

## 接受的妥协（诚实记录）

删除时，三条约束中只有 Execution ⊥ SQL 已由 Cargo 强制。另外两条——Backend 不依赖
SQL、native encoder 只经只读门面读 SQL——在 SQL compiler 仍位于聚合 core 内的当下，
**Cargo 无法表达**，因此从删除到 `novarocks-sql` 物化之间存在一个无自动化防护的窗口。

接受这个窗口，理由不是它无风险，而是被删掉的守卫**已经不能履行职责**：它扫描的目录
已消失数周而持续失败，说明它既没能阻止回归，也没能引起注意。用一个失效的守卫换取
「有防护」的感觉，比明确承认窗口更危险。

同时承认：本裁决把「隔离是否成立」的保证时点，从每次测试运行推迟到了 SQL compiler
物理拆分完成之时。这是一个真实的时间成本，选择它是因为修好扫描器并不会缩短那条路径。

## 何时重新评估

1. 在 `novarocks-sql` 物化之前，若出现真实的 Backend → SQL 或 encoder 绕过只读门面的
   回归，说明窗口期风险已兑现，应立即以依赖图断言（选项 2）补上，而不是恢复 token 扫描。
2. 若将来需要强制**同一 crate 内部**的模块级隔离——Cargo 无法表达的粒度——则本裁决
   不适用，应先问该边界是否本就该成为一个 crate；确属不该拆分时，再选择基于编译期
   可见性（`pub(crate)`、模块私有）的机制，仍不回到源码 token 扫描。
