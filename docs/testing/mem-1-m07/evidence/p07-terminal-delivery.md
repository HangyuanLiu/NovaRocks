# P07 ordinary terminal 与 COM_INIT_DB（2026-10-08）

状态：本切片定向通过；P07 整体、原生与性能验收仍 OPEN。代码基线 c4cd2a85e；没有 push、PR 或归档。

## 实际调用与退出

- Governed ordinary OK 不再借 deferred complete_one 提前完成 statement。按协商 capability 显式编码既有 OK 字段和 more-results status，取得既有有限 StreamingResponseLease；实际写包和 socket flush 退出后才 complete。失败/超时先销毁 writer 与 payload，再结算断连。绝对普通写期限 30 秒不因 partial/drip 重置。
- Typed ERR 同步验证真实 cancellation/failure cut，单次 try Closing 并覆盖 8 MiB 整包；诊断限额内复制后销毁原 QueryServiceError，随后把 pending finalizer/准确 packet state/ERR/flush 转交 ClosingDelivery，归还普通窗口。独立 5 秒绝对 Closing 期限到期只销毁写出 future，实际 backing 退出才还位；池满/连接关闭立即断连。错误码、SQLSTATE、publication 和 deadline 映射保持。
- COM_INIT_DB 在 resolver/session 副本之前原子取得 Management+Local；InitWriter 增加只读协议事实和准确空边界 into_streaming seam，复用同一有限 writer。普通 session OK 不提前 complete_execution；含 scalar SET 已 sealed effect 的原提交次序保留。
- schema normalize 改为一次按输入长度预分配的 String 和 borrowed split，避免准入前构造不受限 Vec<String>，既有 dotted/trim 语义保持。
- 旧 FE/BE LRA、防护与 decoded transition 路径仍保留；本切片不触发 V1 advertise，不证明剩余 distributed/domain 生产接线完成。

## 定向验证

6 个新增真实 TCP/受控写反例：显式多结果 OK/无 deferred owner、错误码与下一命令、query/init 半包 OK 保留 Local 与 generation、typed ERR 半包归还 Local 但保留 Closing/generation、完整 packet 后真实 flush 阻塞仍持 owner、已取消终态 ERR 复用连接。Gate/flush waker 是确定性 oracle，外层 watchdog 只限定测试失效；不靠 sleep 选择 cut。

| Check | Passed | Raw log | SHA-256 |
|---|---:|---|---|
| mysql | 77 | `logs/mem-1-m07/p07-terminal-mysql-final.log` | `75a530909bdb05ec72f9bb4174711a61488a2612db54c770b33368228c2c82be` |
| frontend | 1446 | `logs/mem-1-m07/p07-terminal-frontend-final.log` | `9e23d4c9d3ec66aac6820373afca1b4d8797f45ba942e26ad98f651e96c56478` |
| vendor standalone lib | 150 | `logs/mem-1-m07/p07-terminal-vendor-offline-pinned.log` | `4f59e1f10980bc74786dc0aa009eeae153057265bdf1513c5adf9ad59c2c26d7` |
| fmt | PASS | `logs/mem-1-m07/p07-terminal-fmt.log` | `e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855` |
| dependency | PASS | `logs/mem-1-m07/p07-terminal-dependency.log` | `425e2f74e0e6d9d4edcde7a6d653c29bab1f2fddd3d2d53a9f326d1f9900f757` |

初始 root cargo test -p opensrv-mysql --lib 由于 patched dependency 不是 workspace member 而未启动。独立 manifest 在线解析停在 registry index，停止后 offline 初次解析尝试未缓存 cc 1.6.0；使用 workspace Cargo.lock 作为临时独立测试锁种子、offline 解析准确 vendor dev dependencies 后 lib 150 PASS。临时 vendor Cargo.lock 不提交；root Cargo.lock 未改。失败/中止日志保留为 p07-terminal-vendor-final.log、p07-terminal-vendor-standalone.log、p07-terminal-vendor-offline.log。本切片无禁用测试、版本调整或期限放宽。

共享 opensrv seam/FE 准入 API 的同 HEAD workspace convergence 待下一干净检查点单独记录，不从 9a7f316e6 推断新代码。下一工作为 FrozenRootOutput、有限 Internal CPU/collector 和完整领域矩阵；P08/P00b/P09/P10 继续 OPEN。
