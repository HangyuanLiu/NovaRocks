# P07 命令生产者窗口接入（2026-10-07）

本切片在 typed management 语句生产结果前，复用 P07 nonqueued atomic admission 同时取得
root/business 与 Local whole-window；满池拒绝发生在 statement generation 注册与生产者启动前。
queued read/DML/EXPLAIN 的显式 purpose 与 window 接线仍属于后续 P07/P08，不据此声称全产品切换。

CommandContext 只能绑定同一 scope 的已准入非 Closing alias，拒绝重复、外来、已完成与取消 scope。
它不申请第二个窗口。产品阻塞边界将 alias 保留到实际闭包与结果回执退出；即使等待者消失，
回执中的结果和 alias 仍共同退出。SHOW PROCESSLIST 的待执行 future 持有 snapshot 与 alias。
DML prepare/dispatch 两个阻塞阶段显式移交可选 alias，涵盖 management 路由中的 TRUNCATE/ADD FILES；
尚未接窗口的 queued DML 保持原保护，不能把 None 描述为已覆盖。

验证：Query Application lib 519 PASS，Frontend Application lib 1436 PASS。
新增 abandoned_synchronous_command_retains_its_window_until_actual_worker_exit 在 worker gate 内取消
等待任务，确认 Local position 仍占用，实际 worker/结果回执退出后才归还；CommandContext 用例确认
最后 consumer alias 与 actual root 责任同步退出、completed scope 拒绝迟绑定。
日志：logs/mem-1-m07/p07-command-window-query-final-20261007.log、
logs/mem-1-m07/p07-command-window-fe-20261007.log。

未覆盖：source/Arrow backing 与任意 schema/array alias 的完整移交、InternalFacts 专用有限 CPU 域、
queued class/purpose、真实 socket/C7/C8 验收、native 1FE+3BE、transport envelope 与性能门。
本切片不删除旧 FE LRA/result-credit 防护，不修改 BE retained guard。
