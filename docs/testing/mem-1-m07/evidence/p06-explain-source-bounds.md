# P06 EXPLAIN source 与本地 helper 的前置边界

本切片仍为 local-only，不是 P07/P08 生产切换或原生验收收据。

required/nullable UTF-8 helper 在创建 column tuple Vec 前核对 4,096 列；required helper 在将 owned row 转为 Option Vec 前核对行宽。空 schema、名称、类型、必填值、总行数与 bytes 仍由 LocalTableBuilder 统一裁决。已由调用者创建的 input Vec 不因此获得源头事前受界证明；consumer 必须在 finish 前消费并退出 source scratch。

逻辑、物理树与 Contract EXPLAIN 共用单 pass ExplainRenderOutput：最多 65,536 行 / 8 MiB 文本（含行间 separator），显式拒绝更松的预算。String 在 write 前核对 remaining，reserve_exact 的目标容量不超过剩余逻辑界；每条完成的 line 经公开 Box<str> 接口收敛为 exact capacity 后入 Vec，不再次格式化文本。行 Vec 扩容目标不超过 frozen count。所有完成行加当前行的实际 String capacity ≤8 MiB，String old/new 复制峰值 ≤16 MiB；行 Vec old/new 槽 ≤3 MiB，按每行 allocation allowance 64 B 计 ≤4 MiB。保留辅助索引必须另有前置边界；formatter 不能将 plan 的存在当作自己复制工作集的许可。

物理 Contract 的 AND/OR expression definition 改为借用逐参数输出，metadata relation kind 借用写出；Profile coverage 使用已有有序 maps 的借用比较/计数，不再创建四个完整 BTreeSet。annotation lookup/subject iteration/value-ID 顺序及 edge attachments 都借用读取，不再生成同内容的索引 Vec/maps。正常表达式、注释的 first/last 规则、ValueId 排序与 missing/extra 诊断语义保持。

逻辑树借用逐行输出；每 node 的非分配结构预检限制 expression visits，逻辑 plan/expr/type 在下降前检查内部 64 层 stack 界。物理树的 annotation/node 索引在分配前按实际精确容量与 stable-sort scratch 核对 4 MiB；fragment 顺序、名字、distribution、stats/broadcast、expressions/runtime-filter endpoints 均借用。物理树最多 128 层、expr 最多 64 层，cycle/back edge 明确拒绝，DAG numbering 保留旧重复遇见规则。PROJECT 边向真正的 bounded writer 输出、边与 alias 比较，然后按需写 AS；默认 vID 名用 16 B 栈 buffer，不先完整格式化一次再输出第二次。

输出/stack 超界明确拒绝整份 EXPLAIN，不截断成功，也不把深层表达式变成省略号。内部 bounded stack 是 formatter 的工作区限制，不宣称逻辑计划原来存在同名 frozen 协议。正常 SQL EXPLAIN golden 未修改。

Contract RenderContext 现使用 PhysicalPlan owner 的 FragmentIoCutIndex，分配前核对独立 4 MiB auxiliary ceiling。registry 借用 provider read/selection digest，CompactSourceBindingSet 与原 cut 派生共用同一 provenance kernel；imports/projection/type、source bindings、partitioning、router/writer field/name 以 iterator 借用格式化，不建立 proof hull、runtime-filter graph 或 artifact payload 的第二份。checked preflight 计入拓扑/registry、局部和 parent union 的旧新 Vec/Arc、pairwise merge 与 temporary vectors；构建失败或超界整体拒绝。旧独立 fragment/cut API 保留其原 owned 物化语义，不以 Local ceiling 取代它的既有界。生产 Local window funding、实际 Arrow/renderer/socket 别名、source deadline/退出以及 P07/P08 sole cutover 仍 OPEN。不能根据这份 formatter 收据删除旧 FE 保护。

Query Application lib **516 PASS**，`logs/mem-1-m07/p06-local-helper-preflight-20261007.log`；新增 wide schema / required ragged row 反例。

SQL lib **2,588 PASS**，`logs/mem-1-m07/p06-explain-source-lib-20261007.log`；此前 explain filter 44 PASS（随后又加 PROJECT single-pass probe）。Frontend lib **1,421 PASS**，`logs/mem-1-m07/p06-explain-source-fe-20261007.log`。fmt/diff 通过。新反例覆盖实际借用指针、hex expansion/wide args、name/alias/decimal/String、exact output界、capacity增长前拒绝、source index/cycle/depth、MV constraints及正常字节相等。

首次新 tree joined helper 的返回 lifetime 缺失，已显式命名修复；日志 `p06-explain-source-first-20261007.log` 保留。没有修改已有 golden 或放宽断言，没有运行原生 SQL/性能/传输测量。


借用 cut 接线验证（local-only）：physical-plan lib 183/183，SQL EXPLAIN filter 55/55，SQL lib 2,588/2,588，Frontend Application lib 1,421/1,421 均通过；日志分别为 `logs/mem-1-m07/p06-borrowed-cut-{physical,sql-first,sql-lib,fe-lib}-20261007.log`。physical owner 新测试逐项对照原 owned cuts 的 DAG/multi-source/重复 provenance/source-free/router/writer facts，核对大 payload/type/name 为原 owner 指针、预算边界和无效拓扑的拒绝。独立只读 review 未发现具体 correctness gap；该 review 不替代上述测试，也不证明生产窗口与实际退出。
