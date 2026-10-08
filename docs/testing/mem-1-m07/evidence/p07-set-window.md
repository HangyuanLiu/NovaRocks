# P07 SET 整窗与真实 compiler 退出（2026-10-08）

SET 含查询赋值时沿唯一 warehouse 队列放行，同事务取得计算许可与 Internal whole-window；
无查询赋值的 SET 同样排队并取得 Local whole-window。两者均在 staged session 副本与编译之前
取得。scalar 子查询沿既有 exact child delegation 复用窗口，不申请第二位置。

cancellable preparation 闭包与 unclaimed CPU result receipt 持有窗口 alias。取消等待者仍会
及时返回，正在运行的同步 compiler 不因此退出；最后 actual worker / result alias 退出才还位。
新行为反例 cancelled_scalar_preparation_keeps_internal_window_until_worker_exit 在真实 CPU
executor 的可控 gate 内取消 waiter，丢 permit、完成 root 并丢 grant 后 Internal 仍占1；
实际 worker 退出后才归零。普通 read preparation 也传递它已经取得的可选 alias，未新增
read 的窗口或 root output 切换。

Frontend query::tests：44 PASS；完整 Frontend lib：1438 PASS、0 FAIL；fmt/diff PASS。
第一次编译的 MutexGuard 借用类型错误已修正，未改变测试或产品限制。既有 warnings 保留。
本次没有 native SQL、socket、完整 workspace、性能或 transport 测量证据。

仍 OPEN：production Scalar/Client/CountOnly purpose 冻结，普通 read/DML/Internal 编译入口与
Local 实际 Arrow/schema alias 完整覆盖，finite InternalFacts CPU 执行 owner，membership ingress，
P06s、P08 唯一切换、P00b 实测、P09 V1-V11 原生验收与 P10。旧 FE/BE 防护保留；不宣称
旧 decoded scalar scratch 已因取得窗口而证明硬界。

## 原始日志 SHA256

- `logs/mem-1-m07/p07-set-window-targeted-20261008.log`: `94a9b2b1047b73b8a5f328dd30fe3128925d071a32ac685863e23d9280963ee7`
- `logs/mem-1-m07/p07-set-window-fe-20261008.log`: `399979afa5375f330101c35025be47c9b7b9e438c053cca1fac90f77cbf6044d`
- `logs/mem-1-m07/p07-set-window-fmt-20261008.log`: `e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855`
