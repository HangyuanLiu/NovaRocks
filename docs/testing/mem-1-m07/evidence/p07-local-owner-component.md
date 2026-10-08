# MEM-1 M07 P07：Local 封闭 owner 组件与 PROCESSLIST 容量

2026-10-08；基线 `b34861d9f`。这是组件/源头检查点，尚未接入三处governed immediate汇合点或生产MySQL writer；不能当Local完整生产证明。

`LocalResultProducer`核对准确host/scope、Local或Internal窗口与冻结93MiB最大共存包络，再调用封闭应用源。它的callback合同要求fresh graph且不对外发布Arrow aliases；任意QueryResult的capacity不证明排他性。`OwnedLocalResult`封闭结果、schema与整窗alias，没有Clone/into_batches/raw Arrow或可Clone的ResultField/schema getter。metadata只借用名字并返回闭合Copy类型；类型只有已审计源声明的Utf8/Boolean/Int32/Int64，未知或带逻辑domain的字段准确拒绝。

所有字段按实际声明顺序先销毁graph/schema/cursor/scratch，最后销毁window。move-only `LocalRenderCursor`消费private batches，通过既有纯`ArrowMysqlTextEncoder`产生同一flat ClientRows body；一轮只推进一个input，不在空batch间无界循环。取消在首轮之前和计数轮之后都锁住后续编码，originating render错误不被取消覆盖。payload/metadata实际owner需要持`retain_physical_guard`，不能把游标析构等同于所有writer退出。

实际source审计：LocalTableBuilder、SHOW PROCESSLIST、MV information_schema、maintenance/SHOW OPTIMIZE、stateless rebuild、MV display、topology/catalog/view/statistics SHOW与EXPLAIN均新建结果Arrow graph，没有找到持久外部Buffer/Schema/ArrayRef alias。internal system_catalog、MV aggregate read和legacy distributed QueryResult不进入这个factory。现有CPU/blocking receipt持alias，await后原statement grant仍存活；生产接线必须在该连续owner链中consume结果，不重新申请另一窗口。寿命审计不是增长证明，其他源头仍要逐项核验。

PROCESSLIST原StringArray iterator不知道总文本bytes，会在接近满容量时几何替换values。现在整表borrowed preflight之后，各列先计bytes，再用公开StringBuilder.with_capacity一次申请；numeric builder亦预置rows。三条65537-byte FULL Info反例核对实际values capacity与公开预分配builder一致，并证明原iterator对此输入有更大spare；同时保持文本、NULL、Int64 metadata与非FULL截断语义。没有猜Arrow对齐或修改vendor。

第一方依赖只增加已有纯`novarocks-result-render` normal edge，root lock无版本变化。application-domain dependency guard通过，无新第三方patch。

验证：PROCESSLIST定向3 PASS；准确最终Query Application lib525 PASS（含Local owner6项），raw extraction compile-fail doctest1 PASS；最终Frontend lib1446 PASS，fmt/diff/依赖边界PASS。首次Frontend全lib1445PASS/1FAIL：既有`submitting_to_a_closed_runtime_settles_once_and_returns_reservations`在shutdown_background后的立即drain取到空列表；单独原样重跑1PASS，再完整原样重跑1446PASS。按execute时序用例复跑规则记录一次噪声，不修改该产品/测试路径、不放宽deadline。初始失败日志保留。

| 原始日志 | SHA256 |
|---|---|
| `logs/mem-1-m07/p07-local-process-list-final.log` | `75164ce57a8d61544b13f1403381480595f48253a0a6f8c623a89bb017354182` |
| `logs/mem-1-m07/p07-local-owned-query-application-final.log` | `15ddee9cac902377970a5bed57c0f97266a85c4258440e96e3784d5cc14bfc29` |
| `logs/mem-1-m07/p07-local-owned-doctest-final.log` | `6e3a7bf02571a44e4e0aec643df30f7c52d6aa135b3daae03db2a791b49329b6` |
| `logs/mem-1-m07/p07-local-source-frontend.log` | `9b31cb8688c5d3c827614719072a5ff5f830762dd924460d13586759854e22fd` |
| `logs/mem-1-m07/p07-local-closed-runtime-replay.log` | `770bd6020519063d586b09c2b46c4cf92398fbdca450df416de65bb7d24cedc0` |
| `logs/mem-1-m07/p07-local-source-frontend-final.log` | `4a1fbc12d38b807d38e9a03cd2f09a3c8c9d6356947dcb4d493a26cf883a77cc` |
| `logs/mem-1-m07/p07-local-owned-dependency.log` | `425e2f74e0e6d9d4edcde7a6d653c29bab1f2fddd3d2d53a9f326d1f9900f757` |
| `logs/mem-1-m07/p07-local-owned-fmt.log` | `e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855` |

后续：governed immediate必须转移opaque结果；MySQL本地共享pure renderer/有限framing/独立closing接线，窗口跟writer/segment最后owner。旧LRA保持到P08完整替代保护成立；新组件不证明现有legacy Arrow aliases已闭合。新公共API/normal依赖边在下一生产接线wave收敛触发workspace验证，旧`bc12f9ff4`收据不覆盖此后代码。P08/P00b/P09/P10、finite Internal域与native1FE+3BE仍OPEN。
