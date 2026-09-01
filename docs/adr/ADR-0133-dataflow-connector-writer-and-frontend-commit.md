---
id: ADR-0133
title: "Connector writers are dataflow operators and only the frontend commits"
domain: [provider-spi]
status: active
supersedes: [ADR-0023, ADR-0030, ADR-0051]
superseded-by: null
date: 2026-09-01
provenance:
  - "discussion: 2026-09-01 distributed writer data plane and frontend commit boundary"
  - "PR: pending — backfill the number once the writer cutover merges"
code-anchors:
  - "novarocks/spi/src/connector/write_stack/ (ConnectorWriteControl, ConnectorWriterHandle, ConnectorCommitFragment, WriteTargetOrdinal)"
  - "novarocks/spi/src/connector/write_stack/relation.rs (writer_output_schema, root_output_schema)"
  - "novarocks/execution/src/exec/operators/table_writer.rs (TableWriterOperatorFactory)"
  - "novarocks/execution/src/exec/operators/table_finish.rs (TableFinishOperatorFactory)"
  - "novarocks/backend/src/connector/write_data_plane.rs (RootCommitFragmentCarrierValidator)"
  - "novarocks/frontend/src/query_execution/write_barrier.rs (WriteCommitBarrier)"
  - "novarocks/frontend/src/query_execution/write_session.rs (ConnectorWriteSession)"
  - "novarocks/connector/iceberg/src/commit/write_stack/control.rs (IcebergWriteSessionControl)"
---

## 问题

一次分布式写入产生的文件，应该沿执行数据流回到 FE，还是沿 query lifecycle 的 terminal 回到 FE？
谁有权提交外部 catalog snapshot，以及「现在可以提交了」这件事由什么证据构成？

## 背景与执行事实

改造前，writer handle 绑定在物理 placement 上。FE 必须先冻结调度，才能为每个
`{operation, cohort, execution attempt, fragment instance, backend, sink}` 组合生成一个 handle；
fragment 编码期再按 placement 精确查表，并明确禁止复用同一个 handle。一个 sink factory 内的所有
pipeline driver 共享一把 `Mutex` 和一个 writer，由最后一个 finish 的 driver 负责收尾。writer 的产物
不进入执行数据流，而是切成定长帧塞进 lifecycle terminal 的 multipart report，FE 再重组、校验
writer 覆盖度，然后做 aggregate commit。

这套结构把四件本应独立的事实绑在同一个对象上：逻辑写入配方、物理执行位置、本地 writer 生命周期、
以及 FE 的外部提交生命周期。它能表达很复杂的身份，但没有带来产品需要的能力——同一个不可变配方因
placement 数量被重复规划，pipeline DOP 被共享 writer 串行化，而普通执行图明明已经有 Exchange、
root fragment、result buffer 和 FetchResult。

对照基线是 Trino 的 `TableWriterOperator -> Exchange -> TableFinishOperator -> metadata.finish*`。
它的价值不在类名，而在三条边界：writer 是普通执行算子，fragments 是普通数据流，metadata commit 是
coordinator 的权限。NovaRocks 的 FE 不承载 Arrow pipeline，所以不能完全照搬。

## 考虑过的选项

**A. 保留旧身份体系，只把 opaque report 换成 typed carrier。** 改动面最小。但它把错误的边界固化得
更深：handle 仍随 placement 重复规划，DOP 仍被共享 writer 串行化，lifecycle 仍同时承担「执行是否
成功」和「业务数据运输」两件事。换编码不改变任何一条。

**B. 完全照搬 Trino，让 FE 执行 finish 算子。** 这要求 FE 承载 Arrow pipeline，直接打破 FE/BE 的
角色边界——那条边界的价值远大于省掉一次 result uplink。

**C（选中）. 借用 Trino 的执行形状，但把聚合放在一个 Root BE、把 external commit 留在 FE。**
代价是多一次 result uplink 和一个显式的双 barrier；收益是两条长期边界同时成立：BE 永远没有
metadata mutation 权限，FE 永远不执行 Arrow pipeline。

## 裁决

writer 是普通执行算子，每个 pipeline driver 独占一个 batch writer，没有共享锁，也没有「最后一个
driver 负责 finish」的协议。产物作为普通 Arrow 行经 Exchange 汇聚到**一个** Root BE 上的 finish
算子；Root BE 只做有界聚合，经既有的 result sink 与 FetchResult 把完整的 prepared write set 交给
FE；只有 FE 能调用 provider 的 finish 并提交 snapshot。

三层身份保持分离，不融成一个万能 writer id：exact provider generation、query-local 的逻辑写入目标
序号、以及 attempt-local 的物理位置。它们各自只被自己的 owner 使用，commit fragment 不重复携带
其中任何一层。

**提交门是两个互不蕴含的事实的合取。** 读到 result 的 end-of-stream 证明写数据面闭合了；lifecycle
terminal 证明这次 attempt 的每个 participant 都成功了。前者不能证明别的 participant 没失败；后者
因为 terminal 不再携带产物，也不能证明 FE 收到了数据。两者缺一都不提交。

预算按能看见它的 owner 记账：单 handle 的上限在 FE 编码出口与 BE 入口各验一次；一次查询内所有
**唯一**逻辑 handle 的总量只有 FE 能算（BE 一次只看到一个 carrier，无从知道唯一集合）；单 fragment
与 prepared set 的上限在生产端、Root 入口、FE 的 FetchResult 入口各验一次。

写入 flavor（普通 / managed publication / row mutation / distributed rewrite）由 begin session 表达，
provider 据此决定分支结构。row mutation 的分支路由事实（接受哪些 change event、列在输入行的哪个
位置）随目标一起返回给 SQL；它们是路由事实而非写入身份，分支的身份是它的目标序号。

## 接受的妥协（诚实记录）

**单 Root BE 是当前规模下的单点。** 它限制单查询的汇聚吞吐与内存，Root 进程失败即整个 attempt
失败，且 attempt 内不做 failover。选它不是因为它更好，而是因为它复用现有 query shape、删掉了大量
独立的 control plane，并且硬上界让风险可计算。分层 finish、durable manifest 与 failover 会引入目前
没有收益的复杂度。

**同一个 handle 会在多个 physical submission 中重复出现。** 按唯一逻辑 handle 计预算消除了错误的
provider 重复规划，但 wire bytes 仍可能因 fanout 重复。这是为了避免引入共享 handle registry 及其
lookup/retention 生命周期而接受的代价，不是因为重复更省。

**失败的 attempt 没有 partial cleanup manifest。** 删掉 terminal report 后，失败的 attempt 不保证 FE
拿到全部已 staged 的路径，只能依赖 writer 本地 abort、provider 的 begin-session abort 与既有的
crash-only orphan GC。接受它是因为「为了清理而可靠运送 partial result」本质上就是一个 durable
staged-manifest/ACK 项目，不能伪装成一个简单的 terminal 字段。

**冻结的旧 delete 引用带的是可选而非必需的 record count。** provider 的 read-model 投影本身不暴露
per-delete-file 的 record count，改动它超出本次范围，所以冻结「未知」而不是编造一个数字。
fail-closed 检查不依赖它，但这确实是一处比设计意图更弱的约束。

**一条语句可以驱动多个 query，提交仍只有一次。** copy-on-write mutation 与 distributed rewrite 今天
就是「一个 provider write session 驱动 N 个 distributed query，最后提交一次」。把它们重构成单 query、
N 个 writer fragment 更贴合上面描述的拓扑，但需要一个接受 N 个独立冻结 source 的 plan builder 并重写
两处驱动循环。选择让 session 累积这 N 个 prepared set 并提交一次：每个 query 仍产出对自己执行图完整的
集合，累积的是语句级并集，冻结上界改由 session 跨 query 承担——按 query 收费会让语句在提交前持有无界
数据而每个 query 看起来都在限内。代价是外部提交接受的是「本 session 驱动的并集」而不是单个集合；本文
描述的拓扑是**单个 query** 的，不是单条语句的。这是为了不重写两条已验证的复杂流程而接受的，不是因为
它更干净。

**这是一次大规模不兼容切换。** 旧的 operation/cohort/activation/report 贯穿多个 crate 与全部
DML/MV/maintenance caller。因为没有历史用户与兼容承诺，选择一次原子切换而不是长期双栈——如果存在
历史用户，这个选择会完全不同。

**一条相邻裁决被收窄而非推翻，但收窄得比第一眼更多。** ADR-0049 的裁决句把 cohort 与 row identity、
match contract、strategy、route id 并列为「provider 拥有的事实」。本决策**删除了 distributed writer
数据面上的 cohort**：分支身份改为 query-local 的目标序号。它的所有权立场没有变——这些事实仍归 provider
签发、不归 SQL 或 Core——所以那条 ADR 保持 active 而不是被 supersede；但今天照字面读它的人会以为 cohort
仍存在于 writer 数据面，而它不再存在。把这句写在这里，是因为让下一个人自己去撞这个不一致，正是 ADR 库
存在要避免的事。

## 何时重新评估

- 真实 workload 经常逼近 prepared set 的字节或条目上界，或 Root BE 的聚合成为可测量的瓶颈；
- 实测证明 handle fanout 的 wire bytes 成为瓶颈——此时才设计共享 handle table 或间接引用；
- 出现「失败 attempt 后必须可靠回收已 staged 对象」的运维需求——此时才立 durable staged manifest 项目；
- 需要在一个 attempt 内容忍 Root BE 失败——此时才设计 failover 与 partial-result recovery；
- provider 的 read model 开始暴露 per-delete-file record count，届时应把那处可选约束收紧为必需。
