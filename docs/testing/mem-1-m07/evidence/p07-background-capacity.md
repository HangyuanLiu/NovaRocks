# P07 后台原子 Internal 准入与原 root 接线（2026-10-08）

MV scheduled refresh、MV 自动维护与 OPTIMIZE 只在 warehouse 同次取得 Query permit 与完整 Internal 窗口后执行。runtime binding 贯穿后台 context、planning、coordinator 和实际 callback；durable job/semantic description/native DTO 不含 live capacity。逻辑完成只归还计算 permit，原窗口由最后 alias 退出归还。

Table Maintenance 的 execution lease 在目标 rebind 之前绑定 admitted root。Background engine 在 topology capture 前预检完整 Internal 包络，创建 context 时沿用该 root 的取消身份，deadline 取 root 与既有 max attempt 的较早者。Request-scoped engine 核对 exact scope 和实际 allowance，不允许换窗口。绑定失败发布 PreDispatchFailed，当前进程 worker 继续运作；metadata-only 入口仍以显式无容量 context 执行，不能进入 distributed rewrite。MV 现有自动 coordinator-disabled 产品门保持。

验证：Table Maintenance 36 PASS（包含绑定拒绝在 rebind 前结束 claimed job）；FE background context 两项反例 PASS（Local 在 topology capture 前拒绝，原 deadline/cancel/最后窗口 alias）；FE 串行全量1460 PASS。初次新增 fixture 的 check_active 方法名与 Duration import 编译错误已修复后重跑。上一轮 FE 并行全量1457 PASS / 1 FAIL 的 unchanged queued_actor_abort_expiry 时序失败，isolated 与完整串行均通过；失败日志保留，不计为通过。

dev 组件验证不替代 native SQL、release 性能/传输测量。旧 FE/BE 防护仍在；P08 唯一切换、P00b/P09/P10 仍 OPEN。同候选 HEAD C0 另附收据。Cargo.lock 只增加 Table Maintenance 现有 workspace dev-dependency，没有 registry 版本变更。

日志哈希：

- `logs/mem-1-m07/p07-maintenance-bound-acquire.log`: `f01259af1a9d637f06ba762d147210dca0e2315134b2d1ee6e0f9b88817e60aa`
- `logs/mem-1-m07/p07-admitted-background-context.log`: `317b4129f9e902fddc48156e1d78e68439fe4c60924e2d6935250d5adc6a8716`
- `logs/mem-1-m07/p07-background-capacity-serial-frontend.log`: `965f722e21b4190badd6850339a48e5d3119bca75f28e5979a11b07a7d52b020`
- `logs/mem-1-m07/p07-background-capacity-full-frontend.log`: `d831b3b8bfb0955e2d20791c725c1c461e5443d257e99e3bc341736345ef8bd6`
- `logs/mem-1-m07/p07-background-capacity-actor-retry.log`: `469a7e8ef9415de4ab7810356272a15bd9fdcb62ce32f4c70017f0c020f3af70`
