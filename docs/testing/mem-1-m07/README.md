# MEM-1 M07 执行证据

本目录记录已批准的 spec / plan 第 5 版的本地实施与验收。P00 本地准备已完成，进入 P01/P03；这里的目标参数或源码审查不代表产品已经实现或通过验收。

## 批准与基线

- 2026-10-01 用户批准已落盘 plan 第 5 版，并授权 sub-agent。
- 实施分支：`codex/mem-1-m07-be-encoded-results`。
- 设计审查基线：`b1d13989c50231ee7a5031163255b2607c5176c0`。
- 执行 / 旧版测量基线：`eb35251de575e071ad3657d0ce0fc1fc95d1a91a`，启动时工作树干净，已核实与 `origin/main` 一致。
- 两基线之间的相关变化：M02a 已替换 memory core 并删除 memory-arrow；SQL / decimal / complex-type golden 有独立语义修复。M07 必须消费当前代码，不恢复旧 Charge / Reservation API，也不改写既有 golden 来掩盖结果差异。
- 持久 goal 包含 P00–P10 的本地实现、1FE+3BE 功能与冻结性能门、最终同 SHA workspace / SQL / system 和交接。当前没有 push / PR 授权。

## 证据状态

| 内容 | 状态 | 说明 |
|---|---|---|
| producer / transport / MySQL census | 已调查 | 三个只读 sub-agent 独立调查，已归入 coverage；准确产品接线仍待实现 |
| profile / 完整 checked 包络 | 冻结目标 v1，算术 PASS | profile-v1.json / owner-envelopes-v1.md；actual allocation/holder 防护是后续实施与验收门，尚未产品实现 |
| 旧版 release / dev-opt 构建 | PASS | 原始构建与 binary hash 见旧基线 index |
| macOS 原生 1FE+3BE 旧路径 | 部分 PASS | 短查询/慢读/type smoke 和四个功能场景已留存；扩展 wire 测量准备中 |
| Linux 正式测试 | 用户后续手动执行 | 用户明确取消本次 agent 的 Linux 测试要求；不阻塞本地实施，保留复现材料 |
| 共享 REST / MinIO fixture | READY | `DOCKER_CONTEXT=desktop-linux` 的 BOM 离线校验/up 通过；原 OrbStack 空 daemon 判断已纠正，无需下载输入 |
| P01 / P03 | 进入实施 | 共享合同主agent；vendor独占sub-agent；其他阶段仍待前置 |

## 验证口径

每份运行收据绑定完整 source SHA、binary hash、实际 role 配置语义、机器、三 BE 身份、fixture publication / 数据身份、负载与冻结门。原始错误、拒绝和超时保留，不只统计成功子集。功能用 dev-opt，正式性能旧版与候选使用同 toolchain release；macOS scratch 与 Linux 正式结果分别记录。

最终 CI 使用 `tools/ci/local-full-ci.sh --tier full --all-discovered --cluster-mode cross-process --cluster-size 3`。其默认 system discovery 是 `--list-default`；`--list` 用于定位显式性能 / 外部 fixture 场景。M07 的定向性能另按准确场景执行，不扩大默认功能 CI。

执行期间保留旧 FE 保护与现有 BE retained 防护；只有 P08 的完整生产接线验证成立后删除 FE LRA。M03a/M03b 的真实 Query@BE 分配归属和自身上限、M04b 的 MEM-backed 窗口授权仍是后续独立交接。

2026-10-01 Docker 入口纠正：本机默认 context 为 OrbStack（0 images），预备好的输入在 Docker Desktop。后续 fixture/SQL/system 命令显式设 `DOCKER_CONTEXT=desktop-linux`，不改变全局 Docker context。已核验锁定的 Spark/Paimon/REST/MinIO/mc 和派生镜像/BOM。先前对 OrbStack 执行 provision 的拉取超时保留为错误 daemon 的准备失败，不能归为当前 fixture 缺失。

P00 本地收据：release旧server未改变；原始wire smoke 22/22成功，16/64/256各3轮共11088样本全部保留。16客户端528/528成功；64客户端1016/2112成功；256客户端2196/8448成功，其余服务端1105（尚未细分内部原因，不能断言全来自某一gate）。deterministic结果无重复不一致。旧REST混合场景（MV/ANALYZE/OPTIMIZE+foreground）通过；此smoke不当作正式吞吐/容量验收。测量器四个定向协议测试和格式检查通过。P00的Linux正式集中root、RTT/control、完整CPU/network对照收据按用户指示由用户后补；保留版本化门，不阻塞本次本地实施，不称已性能通过。
