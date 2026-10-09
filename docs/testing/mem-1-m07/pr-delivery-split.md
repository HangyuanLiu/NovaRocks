# M07 分两次 PR 交付

用户于 2026-10-09 授权先发布已实现的代码，继续测试，并将之后的修复和收尾交给第二个 PR。
本记录调整发布批次，不改变 accepted spec / approved plan revision 7 的设计、容量、期限或最终验收条件。
第一个 PR 创建后，M07 整体仍为进行中；spec / plan 不归档。
第三方库边界 ADR 与 main 编号冲突，按仓库规则从原 ADR-0168 重编号为 ADR-0170；
历史 JSON 收据中的编号、源 hash 与原 bytes 保留，当前代码锚点和文档引用指向新编号。

## 第一个 PR：实现与已有证据

实现检查点为 `64e459efcd7e3c5daffb2eb22cdbca0163041c21`；发布前合入当前 main 的
IRU-7、Rust / Arrow 升级及已有 Worker 修复，修正集成冲突，并记录新检查结果。
包含 BE 编码 root 输出、FE 有界中继、MySQL framing / cancellation、显式内部领域消费者、
Native lane 前置准入、外部列表边界及已经提交的测试和证据。
新旧状态按实际代码评审；本批次不宣称 P08 全部切换条件或 M07 最终验收已经闭合。

已存在的证据各自绑定其原始 SHA，不跨版本挪用：

- [原生精确十场景](evidence/p09-exact-native-ten-cases-1fb1319df-20261009.json)：
  实际 1FE+3BE、十场景通过，原 runner 退出与四个 role 的退出已记录；源版本为 `1fb1319df`。
- [完整 backing / 末 alias 组件](evidence/p07-physical-backing-components-20261009.json)：
  Worker integration binary 7 PASS，Native reader integration binary 27 PASS。
- [held-response / late ACK 组件](evidence/p09-held-scene-components-20261009.json)：
  runner 组件里程碑 299 PASS / 0 FAIL / 2 既有 ignored；尚无此场景的真实 Native 验收。
- [独立 FE 来源与启动时钟](evidence/p09-neutral-fe-source-components-20261009.json)、
  [SDK 参数启动校验](evidence/p08-sdk-listing-startup-components-20261009.json)
  及目录内其余收据均以各自记录的实际范围解释。

原始日志保存在 Git 外；证据文件中的 hash / local path 是溯源信息，不能当作远端可下载产物。
最终同 SHA 的 workspace / SQL / system 全量报告留给收尾，不把历史全量运行写为本批次通过。

## 第二个 PR：继续实现、测试与收尾

从第一个 PR 的已发布 HEAD 创建独立分支；第一个 PR 发布后不再混入新的测试开发。
第一个 PR 未合并时，第二个 PR 使用第一批分支作为基线；合并后迁移到 main。
保留原计划未闭合的全部工作，至少包括：

1. held-response 的新冻结输入、实际启动准入、独立 LIVE 来源采集和最终验证器；
   完成审查发现的 context identity、严格整数、原始日志 LF 边界和完整 DTO 校验修正，
   再跑实际 1FE+3BE、故障路径及原四个 role 退出验证。
2. 真实 Closing 64 占位与第 65 个拒绝，复制峰值 / 失败回滚、其余最后 alias、flush / 缺尾 / 短写，
   Client / Compute / short-tail / Control 的压力与取消恢复。
3. HMS **只读**及 Paimon 大列表、SDK 八位置的真实调用退出 / 取消 / 复用，
   REST 大 catalog 的自有源头与峰值归属审计。
4. P08 完整 checked 启动包络、运行期增容、全部 Host drain 和保护替换 / 退出条件。
5. P00b 参数与传输系数冻结、重复校准、soak、macOS 内存门、新旧 release 性能对照，
   Linux 用户执行材料；结构算术、空系数和逻辑归还不能代替测量 / 物理退出证据。
6. P10 ADR / 部署与 owner 交接、V1–V11 映射、最终同 HEAD 的 workspace / SQL / system 验证。

Ordinary 饱和策略与 30 秒写期限是否包含 producer 等待仍按原待裁决状态处理；本次不改语义。
IRU-7 已决定 HMS 只读，HMS 非只读正确性不属于 M07 验收，触及共享代码只保证编译。
第一个 PR 的创建不代表第二个 PR 已发布，也不代表 M07 / 持久 goal 已完成。
