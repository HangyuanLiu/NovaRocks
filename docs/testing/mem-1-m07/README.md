# MEM-1 M07 执行证据

本目录记录已批准的 spec / plan 第 5 版的本地实施与验收。P00/P01/P02/P03 本地检查点已保存，P04 正在执行；这里的目标参数或源码审查不代表产品已经实现或通过验收。

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
| P01 / P02 / P03 | 已完成对应切片 | 共享合同/纯 renderer/vendor framing 已留收据；完整产品切换仍待 P04–P08 |
| P04 context-root core | 定向通过，P04 继续 | [收据](evidence/p04-context-root/README.md)：301 Worker、54 Native host、3 Observable、28 Renderer；尚未接入真实 Native producer/fetch/lane |

## 验证口径

每份运行收据绑定完整 source SHA、binary hash、实际 role 配置语义、机器、三 BE 身份、fixture publication / 数据身份、负载与冻结门。原始错误、拒绝和超时保留，不只统计成功子集。功能用 dev-opt，正式性能旧版与候选使用同 toolchain release；macOS scratch 与 Linux 正式结果分别记录。

最终 CI 使用 `tools/ci/local-full-ci.sh --tier full --all-discovered --cluster-mode cross-process --cluster-size 3`。其默认 system discovery 是 `--list-default`；`--list` 用于定位显式性能 / 外部 fixture 场景。M07 的定向性能另按准确场景执行，不扩大默认功能 CI。

执行期间保留旧 FE 保护与现有 BE retained 防护；只有 P08 的完整生产接线验证成立后删除 FE LRA。M03a/M03b 的真实 Query@BE 分配归属和自身上限、M04b 的 MEM-backed 窗口授权仍是后续独立交接。

2026-10-01 Docker 入口纠正：本机默认 context 为 OrbStack（0 images），预备好的输入在 Docker Desktop。后续 fixture/SQL/system 命令显式设 `DOCKER_CONTEXT=desktop-linux`，不改变全局 Docker context。已核验锁定的 Spark/Paimon/REST/MinIO/mc 和派生镜像/BOM。先前对 OrbStack 执行 provision 的拉取超时保留为错误 daemon 的准备失败，不能归为当前 fixture 缺失。

P00 本地收据：release旧server未改变；原始wire smoke 22/22成功，16/64/256各3轮共11088样本全部保留。16客户端528/528成功；64客户端1016/2112成功；256客户端2196/8448成功，其余服务端1105（尚未细分内部原因，不能断言全来自某一gate）。deterministic结果无重复不一致。旧REST混合场景（MV/ANALYZE/OPTIMIZE+foreground）通过；此smoke不当作正式吞吐/容量验收。测量器四个定向协议测试和格式检查通过。P00的Linux正式集中root、RTT/control、完整CPU/network对照收据按用户指示由用户后补；保留版本化门，不阻塞本次本地实施，不称已性能通过。

P01环境复核：`--consumer iceberg-rest`再次通过；`--consumer all`发现已有`paimon-writer`派生镜像定义收据不匹配当前源码（prepared `320bd65a…`，current `23dc030b…`），不是镜像缺失。完整CI的Paimon输入尚未通过当前版本核验；独立准备/准确版本验证留在P09/P10入口，不能沿用REST通过冒称全BOM通过。

Paimon定义差异已只读定位：prepared版本对应`3af07efbb`，当前变化来自已合入`375c9e0ea`的host env parser（unset/变量名/shlex quoting）；Dockerfile/versions/JAR lock/SQL/golden未变。全部5个canonical image、6个JAR已验证存在且hash正确，当前派生image仍带旧definition。完整CI无条件all verify+Paimon准备，故需在独立测试准备阶段用已验证本地输入真实重建并经owner发布，不能仅改BOM/label；现有provision无条件重新下载JAR，不直接使用其联网流程。当前REST运行实例固定image ID不受current alias重建影响。

P04 context-root 核心收据已保存：新增 immutable 通道/物理 backing owner、context fence 接管与读取、正常 seal/abort/lease 收敛；allocation failure callback、schema spare 合并容量和默认并行 allocator probe 已修复且反例通过。Native host 仍为 root=None、V1 未 advertise；P04/P05–P10 继续。此前 Paimon definition 不匹配已在 P02 离线修正，当前 all-consumer 校验通过，详见 [fixture-ready](evidence/fixture-ready/README.md)。

P04最后pull/Execution生命周期检查点：唯一事前input grant、original-carrier直移、built DOP绑定、explicit RootRegistration与actual-exit成功门已定向通过；[收据](evidence/p04-last-pull/README.md)。P04仍执行，原始Arrow/schema metadata实际backing证明与Native producer/fetch/lane产品接线继续。

P04 metadata 来源已接真实 Native output / ChunkSchema / ExecPlan lowering / LocalProgram projection，且覆盖嵌套 carrier 派生和独立 work 上限；[定向收据](evidence/p04-source-origin/README.md)。完整 actual Arrow/Chunk backing proof 与 Native producer 仍继续，未宣称产品验收。
