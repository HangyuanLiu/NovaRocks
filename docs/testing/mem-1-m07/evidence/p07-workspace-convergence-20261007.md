# P07 workspace 收敛收据

2026-10-07，精确 HEAD `ed723f6df43e133e4237cf38a5097d193f8d7888`，执行前后工作树干净。此为用户 handover 中共享 Connector interface 改动后的 Cargo-only 里程碑收敛，不是 P08/P09/P10 最终验收。

`tools/ci/local-full-ci.sh --cargo-only` PASS，1,517 s。全部 dependency/source/ownership/fixture guards、fmt、workspace all-target check、无 jemalloc server check、warning-only clippy、workspace build、SQL error manifest freshness 均 PASS。

| 阶段 | targets | passed | failed | ignored |
|---|---:|---:|---:|---:|
| workspace components（含 doctests） | 181 | 11,895 | 0 | 7 |
| server owner | 3 | 178 | 0 | 0 |
| server binary smoke | 1 | 4 | 0 | 0 |
| 合计 | 185 | 12,077 | 0 | 7 |

7 个 ignored 均保留现有原因：两项 Docker REST fixture、Native proxy cluster 需显式 binary、MinIO fencing fixture、手动 release fixture recorder、live UEA-7 fixture、Java frozen delete receipt。没有增加 ignore 来通过此次验证。完整名字见原始 component log。

运行参数：Cargo jobs=2、incremental=0、dev/test debug=0，Rust test threads=1。使用任务临时 cargo-deny 0.20.2 与已安装 Homebrew Python 3.11/Bash 5，遵循现有 CI 前置工具要求。执行前 `cargo clean` 本 worktree，结束约 20 GiB 可用。

原始日志保存在忽略目录 `logs/ci-full/20261007-185124/`，主要文件 SHA-256 见 [JSON 收据](p07-workspace-convergence-20261007.json)。前置失败日志保留在 184028/184400/184508/184631/184906 各次目录；workspace 依赖身份归一和已退役 probe manifest 归档分别由 `538169c81`、`ed723f6df` 修复，未放宽守卫。

本轮未运行 System scenarios、SQL suites、性能/传输测量或 native 1FE+3BE M07 effect 矩阵。production V1 sink/window 切换与 P00b/P08/P09/P10 仍 OPEN，不能将该 Cargo-only PASS 描述为 M07 完成。
