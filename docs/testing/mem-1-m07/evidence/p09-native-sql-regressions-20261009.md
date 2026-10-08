# MEM-1-M07 P09 原生 SQL 回归进展

2026-10-09，消费 accepted spec / approved plan 第 7 版。完整源码版本、二进制 SHA256、实际 build identity、各套件原始日志 SHA256 与失败分类见 [JSON 收据](p09-native-sql-regressions-20261009.json)。各次运行使用独立进程 1FE+3BE、串行执行；不能将不同版本的结果合并称为最终同 HEAD 全量验收。

本轮修复了生产规划线程的栈容量、前台 MV refresh/repartition 的 Internal 结果用途、root read 的整毫秒等待与原绝对 deadline、有限编码错误诊断，以及 Decimal/整数共同类型的精度。BE retained/process 防护、renderer 的精度校验和 64 KiB Scalar 整条记录界保持有效。Scalar 用例保留原 10,000 行超限输入，核验拒绝后旧 session 值仍可读取；没有截断或提高容量。

最近一次干净测试源码 `2494960e4` 使用实际 build identity `dae50b53e` 的同一 debug 产品二进制，完整 Decimal overflow 契约用例、complex-type 44 项和 low-cardinality 5 项通过。两者之间只有测试与 runner 调整；仍明确记录不同身份。此前独立运行已通过 REST 16、runtime-filter-distributed 8、materialized-view 4、repartition 1、DML 51、DDL 49、MV apply 2、MV scheduler 4、CTE 3、subquery 1。types 121、SQL 2591、SQL runner 267 及对应定向组件测试通过。

共享 REST fixture 虽可响应配置请求，namespace mutation 因 SQLite busy 失败。本轮没有重启、删除或修复共享状态；改用 canonical harness 管理的任务私有 fixture，完成的私有矩阵均以 exit 0 清理。早期 Spark 路径、强制 60 秒 timeout、release 故障环境和未解析凭证等工具前提错误保留在收据中，不归为产品行为证据。

尚未通过的真实检查：

- IVM 修正 Spark 路径后的完整矩阵为 72 PASS / 6 FAIL。repartition 失败已修复并完整复验；另外五个 rename 用例触发现有 occurrence-aware field rebinding 拒绝，旧 main 源码也保留该 guard。未绕过 guard，未在 M07 内实施 UEA-7B7 draft。
- candidate Decimal 完整矩阵为 13 PASS / 7 FAIL。旧 main `9c7723bbe` release 在真实 1FE+3BE 对照中为 14 PASS / 6 FAIL，六个相同高精度用例的旧预期带浮点舍入，而两路径均返回精确小数。candidate 额外的 JOIN 共同精度预期已修正，完整契约用例通过；六个旧 golden 问题尚未解决，没有直接 record 覆盖或增加 epsilon。
- P08 ordinary ingress 的有限承载方案、checked transport envelope、P00b 有效相对基线及 transport 系数、P09 CL/集中 root/所有合法 FE 与控制进展、最终同 HEAD C0/native 和 P10 仍开放。Linux 正式 CM/CP 仍由用户手工执行。

本轮只有本地检查点，没有 push、PR 或 workflow 归档；完整 execute goal 保持 active。
