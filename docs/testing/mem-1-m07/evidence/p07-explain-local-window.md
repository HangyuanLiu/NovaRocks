# P07 普通 EXPLAIN 的 Local 窗口交接（2026-10-08）

普通 EXPLAIN（不含 ANALYZE）在编译前沿用 warehouse 排队准入，同时取得计算许可与
Local whole-window。它不走 nonqueued management 入口，不申请第二个窗口。
CommandContext 的 exact alias 随 CPU preparation、阻塞 dispatch 及各自结果回执移交。
等待 future 被取消不替代实际 producer 退出：阻塞闭包及未领取回执共同持有 alias；
正常返回后外层 governed statement 继续持有窗口直到协议结果完成。

新反例 abandoned_explain_dispatch_retains_local_window_until_actual_worker_exit 使用真实
begin_query_root / admit_query_with_result 排队 API，阻塞 producer 在可控 gate 内运行。
归还计算许可、丢弃 grant 并取消等待者后 Local position 仍为1；实际 worker/回执退出后才为0。
它不是物理 Arrow backing/任意外部 alias 的完整证明。
原 management 弃置用例和 profile native-first-send / cancelled-before-dispatch 用例保持。
首轮新 fixture 错用了拒绝 Query 的 nonqueued root/window API，出现 Conflict；修正 fixture
到生产排队路径，未改变准入合同或放宽断言。

定向：Frontend query::tests 43 PASS、0 FAIL；完整 Frontend lib 1437 PASS、0 FAIL。
fmt 与 diff check PASS。既有编译 warnings 保留，未运行自动清理或放宽检查。
日志 logs/mem-1-m07/p07-explain-window-targeted-20261008.log 与
logs/mem-1-m07/p07-explain-window-fe-20261008.log。此切片发生在
543759140 的共享接口 cargo-only 收敛之后，不能冒用该 HEAD 的全量 workspace 收据。

仍 OPEN：EXPLAIN ANALYZE 的 CountOnly/internal purpose 与窗口、Client/Internal 生产编译入口、
finite InternalFacts CPU 域、Local 实际 backing/alias/source 退出、P08 唯一生产切换、P00b 参数
与实测、P09 native 1FE+3BE / socket / 性能、P10 最终验证。旧 FE/BE 保护保持。
