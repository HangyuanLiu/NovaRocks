---
id: ADR-0088
title: "Domain owners emit SQL error contracts before cross-domain fallback"
domain: [error-contracts]
status: active
supersedes: []
superseded-by: null
date: 2026-08-18
provenance:
  - "discussion: 2026-08-18 domain-owned SQL error contracts"
  - "PR: pending — domain-owned SQL error contract convergence"
code-anchors:
  - "novarocks/sql/src/parser/recursive_cte.rs (try_unroll_cte)"
  - "novarocks/connector/iceberg/src/catalog_control/catalog_mutation.rs (alter_schema)"
  - "novarocks/sql/src/syntax.rs (parse_optional_mv_admitted_statement)"
  - "novarocks/frontend/src/mv/command.rs (MvCommandExecutor::try_execute)"
---

## 问题

当 SQL 的错误语义在 parser、Iceberg schema mutation 与 Frontend MV routing 之间跨越时，哪个 domain 必须在 fallback
发生前产生稳定的用户可见错误，而不是由 catalog、router 或测试框架补偿？

## 背景与执行事实

recursive CTE 的 mixed UNION quantifier 和 anchor self-reference 都能由 SQL AST 判断。若 rewrite 将它们留给 analyzer，
当前 CTE 名会被当作外部表查询，catalog 再按 session namespace 生成不稳定的 NotFound。该 namespace-qualified 文案对真实
外部缺失表是正确的，却不是已被 SQL rewrite 识别的 recursive-shape 诊断。

Iceberg schema mutation 同时负责 reserved column、identifier field、equality-delete field 与 STRUCT/LIST/MAP path 的
admission。若保护性校验先做狭窄 field lookup，reserved name 或 LIST fixed child 会在真正的 format invariant 前失败。
同一个 provider 已经为缺失 nested leaf 产生 canonical leaf NotFound，测试断言不得要求另一套 historical full-path 文案。

MV command routing 需要区分非 MV statement 与已识别但非法的 MV statement。把 parser 的所有 `Err` 转为 route miss，
会让 capability fallback 遮蔽诸如空 PRIMARY KEY 的具体 SQL error；根据 SQL 文本前缀或 error string 在 Frontend 重猜
statement family 同样会制造第二个 parser authority。

## 考虑过的选项

1. **在 catalog、MySQL encoder 或 SQL runner 统一重写错误。** 局部改动少，但跨 domain 复制语义，且会破坏真实
   external-table NotFound、provider error kind 与测试可信度。
2. **保留 fallback 行为并放宽或批量更新 golden。** 能快速让 CI 绿，但把错误 owner 的缺口固化为契约，也无法证明
   生产 topology 的出域路径正确。
3. **每个最早拥有完整语义的 domain 产生错误，边界层只传递或编码。**（采纳）

## 裁决

SQL parser/rewrite 对已经通过 AST 识别的 recursive CTE shape 在 catalog 前 fail fast：mixed quantifier 返回 explicit
unsupported error；anchor self-reference 返回当前 CTE 名的稳定诊断。该规则不匹配真实外部缺失表，因此 catalog 继续保留
namespace-qualified NotFound。

Iceberg Connector 的 `alter_schema` 在 field existence、identifier 与 equality-delete checks 前处理 reserved column，
并用同一 STRUCT/LIST/MAP child semantics 做保护性 lookup 与实际 mutation。缺失 nested leaf 采用 provider canonical
leaf NotFound；equality-delete 的拒绝 predicate、commit 和 field-ID 语义保持不变，只由同一 mutation owner 产生带 field
名的具体诊断。

SQL syntax facade 以 typed optional admission 表达 valid MV、non-MV route miss 与 malformed MV。Frontend MV router 只对
route miss 返回 `None`，其余 parser error 原样传播。MySQL 仅编码这些 domain-owned errors，测试 runner 仅验证，不增加
兼容匹配或错误改写。

## 接受的妥协（诚实记录）

**部分 parser error 继续使用 `unknown table` 文案。** 对 anchor self-reference，保留现有稳定 SQL 文案比发明新的
recursive-shape wording 更符合已建立的客户端断言；代价是该文本本身不如 explicit unsupported error 自描述。我们接受
它，因为它仍在 parser/rewrite owner 产生，并且不会污染真实 catalog NotFound。

**不同 domain 的错误文本不会统一成一个全局码表。** 这让调用方必须尊重 SQL、Connector 和 Frontend 的各自错误词表，
表面上不如统一 formatter 简单。我们接受它，因为错误的语义和生命周期确实不同；统一 formatter 会把 owner 边界换成
跨模块条件分支。

**少量 SQL oracle 会随 canonical provider text 更新。** 这要求每次更新先证明同一 owner 的语义，而不能把 golden 当作
不可变实现细节。代价是 CI 的历史文本不总能保留；我们接受它，因为 provider canonical leaf error 比历史 path 拼接更稳定。

## 何时重新评估

1. 若 recursive CTE 引入 fix-point execution 或新增可支持的 set-operation shapes，应重新定义 parser admission 与
   anchor self-reference 的完整错误分类。
2. 若 Iceberg spec 或 Connector capability 开始允许 nested equality-delete references，必须重新审查 detailed DROP
   diagnostics 与 path-to-field-ID mapping，但不得用字符串兼容绕过 capability 设计。
3. 若 SQL command admission 被抽象为跨 command-family 的 typed protocol，应评估是否将 MV optional admission 纳入该
   protocol；在此之前 Frontend 不得根据文本或 error string 猜测 MV 语义。
4. 若客户端需要 machine-readable error codes，应由各 owner 提供版本化 typed facts 后再设计 wire contract；不得先在
   MySQL encoder 建立全局重写层。
