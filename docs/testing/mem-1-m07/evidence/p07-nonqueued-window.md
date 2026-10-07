# P07 非排队完整结果窗口准入检查点

日期：2026-10-07。只闭合 workload/Query Application 准入 API；生产 caller 与
Local backing alias 接线仍 OPEN，不是 P08 切换或产品验收。

RootAdmissionHandle::try_begin_root_with_result 在同一 authority transaction
创建 root/business 与完整 result window；窗口不足、未配置、已关闭/取消时没有
root/business 逃逸。拒绝 Closing 与所有需要 warehouse queue 的工作类型，避免
Query/统计/MV/维护通过本接口绕过计算准入。既有 queued 接口维持原 dequeue 原子性。

QueryControlService::begin_governed_statement_with_result 只在成功取得两者后登记
statement generation。注册或取消 owner 构造失败会释放全部窗口/业务/root责任；
窗口不足不启动 protocol generation。成功 owner 持窗至协议结束及最后 alias 实际退出。

验证：Workload 23 unit + 74 integration + 8 doc tests PASS；Query Application lib
517 PASS。新增反例覆盖满池拒绝不增长 root/business/nodes、未配置/Closing/Query
拒绝、重复 session 注册失败回滚、晚到 alias 保留 root/窗口，以及满池失败后原
session 能继续开始 statement。日志：
`logs/mem-1-m07/p07-nonqueued-window-workload-final-20261007.log`、
`logs/mem-1-m07/p07-nonqueued-window-query-third-20261007.log`。
首次 fixture 用了不存在的 ResourceConfig::default 与漏掉 WorkClass import；
随后 fixture 的 query_concurrency_limit 超过缩小 client capacity，按既有检查拒绝。
这些 fixture 已修正，未改变产品限额。

尚未运行本切片 workspace、Native/MySQL socket 或 1FE+3BE。管理/local producer
及真实 backing/aliases/deadline 接线、production root purpose、domain有限执行位置、
旧额度退出、P00b/P08/P09/P10继续按同一 accepted spec/plan执行。
