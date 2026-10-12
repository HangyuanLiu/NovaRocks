<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

# 测试准入、尝试材料与证据边界

开发规则由 [workflow-contract](../../../.agents/skills/workbench/skills/workbench/references/workflow-contract.md) §8.5–§8.7 定义；本指南落实 NovaRocks 的位置、操作责任和现有报告解释，不建立第二套运行协议或结果 schema。新资产及实质扩展从规则合入起消费合同；存量按领域迁移，文件存在或一次通过不表示已经合规。本文不改变 Cargo、runner、fixture、默认输出位置或产品验收门。

## 准入与领域迁移

新增测试、workload、采样/比较定义、SQL 指令或架构检查前，先确认现有覆盖为何不足、产品性质、资产性质和最低充分层级、独立预期，以及输入、隔离、真实退出、维护 owner、成本、消费链路和频率。答案靠近原生定义；简单测试可由函数、断言和模块说明承载，无需逐例表单。一次性探索只有明确长期消费者后才按同样条件晋升源码资产。

SQL 文件保留必要的语义输入和断言，多步生命周期编排归 system owner；未知、非法或未实现指令不得静默成为无断言通过。现有指令拒绝行为由 TST-G13 迁移。架构规则保护依赖、类型与模块责任，优先复用声明式工具；存量检查由 TST-G14 收敛，不为本文添加源码 token、精确路径或任务编号门禁。

产品修复与治理场景可以成对迁移，但先接受唯一领域 spec；产品 plan 与治理 plan 引用同一设计，划分切片并反链完成情况。没有领域 spec 时先细化并接受，不在两个 plan 各造一套测试设计。M04b 成对迁移先接受 G3 spec，NIG-1 成对迁移先接受 G12 spec；这不要求先完成整个领域。消费未交付接口时记录具体实现硬前置，原有 MEM/M07 等产品验收继续生效。

## 获批判据的 Git 快照

原生执行 owner 负责 workload、动作、合法输入、采样有效性、正确性断言、资源保护停止条件与实际退出；分析 owner 负责方法适用性、比较、效应量和不确定度。runner 不读取任务验收门限，不以历史报告判断当前运行资格。常设协议与方法引用已有不可变版本；持续基准不需要每轮新建 plan。任务特有对照、门限和适用范围写入获批 plan，在候选确认性测量前固定。需要探索的先声明探索及后续判据确认，不能测后挑门限。

NovaRocks 的工作文档根按 [AGENTS §12](../../../AGENTS.md#12-project-development-workflow-and-knowledge-base) 解析；vault Git 仓库可能位于该项目文档根的上级。操作时先确认真实仓库根和仓库相对路径，不把机器路径固化为项目接口。

1. 用户明确批准准确磁盘版本后，将该 plan 标为 approved，核对正文及判据引用与批准内容一致。仅选该 plan，以及直接承载判据、需要固定的已接受设计/方法文件；源码协议可引用已有 commit。背景链接不递归快照。
2. 读取 vault 状态和索引，保留无关笔记及无关暂存内容。限定路径提交，不要求整库 clean，不做全库 add、reset、stash 或自动 push。若选定文件原来包含其他人的暂存修改，先区分该文件的授权内容，不能顺手覆盖。已有仓库可使用下面的路径限定操作；新文件仅对选定路径 add。
3. 提交成功后查看完整 commit 的文件清单和每个选定文件，确认文本符合批准版本，并核对无关工作树/索引与之前一致。提交失败或内容不符时，不能开展声称遵循获批判据的确认性测量；探索仍可如实记录。
4. 后续运行记录/报告引用 vault 仓库身份、完整 commit 和仓库相对路径。不要把 commit 回填到该快照自身制造自引用；后续进度可写工作文档，但旧判据引用保持不可变。实质修订重新批准并形成新 commit。

以下变量由实际仓库与选定文件填入，示意命令不代替上述内容核对：

```bash
git -C "$vault_root" status --short
git -C "$vault_root" diff --cached
git -C "$vault_root" commit --only -m "Record approved test criteria" -- "$plan_path" "$criteria_path"
# Record the full result in subsequent execution materials.
git -C "$vault_root" rev-parse HEAD
git -C "$vault_root" show --format=fuller --stat "$approval_commit"
git -C "$vault_root" show "$approval_commit:$plan_path"
git -C "$vault_root" diff --cached
```

选定文件集合可以只含 plan；不要为了示例中的第二个变量新增文件。被引用判据必须能从该快照或明确的不可变外部版本恢复，不能指向工作树“最新版”。本地 commit 仅固定批准文本，既不是 runner 准入锁也不是共享交付；正式证据 owner 还需保证相关 Git 对象和引用文件可恢复。

## 每次尝试的位置与独占

一次性实验默认放在源码版本库外：

```text
${XDG_STATE_HOME:-$HOME/.local/state}/novarocks/experiments/<topic>/<attempt-id>/
```

先展开为实际绝对路径；topic 可以是产品问题或任务号。attempt-id 使用 UTC 时间加随机后缀，子目录以独占 mkdir 创建，已存在即失败，不复用或清空旧目录。下面是一次性的创建示例，不是要入库的目录分配器：

```bash
python3 - <<'PY_CREATE'
from datetime import datetime, timezone
from pathlib import Path
import os
import secrets

state = Path(os.environ.get("XDG_STATE_HOME") or Path.home() / ".local/state")
parent = (state / "novarocks/experiments" / "your-topic").expanduser().resolve()
parent.mkdir(parents=True, exist_ok=True)
attempt = parent / (datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "-" + secrets.token_hex(4))
attempt.mkdir(exist_ok=False)
print(attempt)
PY_CREATE
```

在材料中记录实际路径，将临时脚本、输入/环境说明、实际命令与输出一起保存。可以明确选择其它实验根并承担同样保留责任；这不是新增环境变量、CLI 或目录管理机制。不默认写 `/tmp`，不在 `tests/benchmarks/<任务>` 或 `docs/testing/<任务>` 新建永久验收机器。

现有 runner 的原生 artifact-root、checkout 内 ignored logs/reports 仍允许。可通过新根满足独占的使用原生参数，如 system 的 `--artifact-root`；不能独占的明确记录存量缺口并交对应迁移。system `run_one` 对固定场景子目录用 `create_dir_all`，SQL benchmark 也对毫秒时间子目录用 `create_dir_all`；二者都没有原子拒绝已存在目录的保证。调用者选新根是目前操作责任，本文不声称 runner 已强制执行。关联所有输出的真实位置，不提交运行产物进源码 Git。

## 保留、共享交接与清理

材料默认不自动过期，未解决失败继续保留。只有任务结论已审阅、正式引用和重分析所需脚本/输入说明已共享交接且核实可恢复后，才按记录的处置清理本地副本。未被正式引用的纯探索可在明确结束并记录取舍后清理。正式材料由存储 owner 记录访问、保留依据与处置；不能用删除 worktree 或定时清理代替。

本地路径只称本地证据。正式共享引用需有真实共享访问、保留 owner 以及所引用 Git 对象/输入/脚本的恢复条件；尚无共享交接时如实声明。本指南不自动上传、创建云服务或删除材料，凭据继续按既有规则脱敏。历史失败可显式用于分析与定位，不能成为新采集的隐藏输入；声明为长期 fixture 时按 fixture 自身合同维护。TST-G6b 的隔离重跑验证与 TST-G7b 的持续历史/共享服务仍需实际交付，干净工作树或本地 commit 不替代这些证据。

## 代表性原生报告映射

下表是当前写出端的维护说明，不是全仓资产索引或另一份运行权威。字段可表达某个事实与某次运行已写出该事实必须分开；以下仅核验源码，没有生成通过样例或运行集群/性能实验。正式报告优先引用实际原生输出，补充判据版本、缺失事实、材料关联和结论边界。

| 写出端/材料 | 当前能表达的事实 | 尚不能由它单独证明、后续责任 |
|---|---|---|
| system [scenario.rs `write_evidence`](../../../tests/system-test-runner/src/scenario.rs) → 场景根 `scenario-evidence.json` | `scenario`、`command`、开始/结束时间、`source_revision/source_dirty/source_tree_sha256`、runner identity/路径、Cargo.lock hash、平台、`actions/phase_observations`、runtime/binary/config 路径及 config hash、`cluster_size`、`launch_profile`、进程 launch identities、有效配置及 hash、`outcome/exit_code`、失败 diagnostics；当前 schema 5 | `launch_profile` 是 harness 的 `fault-scenario/performance` 配置，不是 Cargo profile。binary 路径与 runner identity 不证明各 FE/BE 实际能力。字段不保证每个阶段均被观察或此次已运行。G2 补实际构建自报；各领域补缺少的性质/退出观察。 |
| system [runner.rs `run_one`](../../../tests/system-test-runner/src/runner.rs) → stdout/stderr、场景 evidence 与 runtime/log 根 | 选择由 registry/`--only` 决定；失败保留材料、动作、诊断和 rerun 命令。正常路径先检查 cluster shutdown 与 fixture teardown，追加 cleanup action，再写 Passed；失败路径先写 Failed，随后处理 cleanup 错误 | evidence 的 `exit_code` 按 outcome 合成 0/1，不是每个子进程退出码。Failed 写出时尚未完成 cleanup；cleanup 出错或启动失败可能缺最终 evidence，须保留 runner 原始错误与实际退出事实。打印的旧 rerun 根不能直接复用，复跑换新根。现有 G6b/领域链路承接隔离与终态缺口。 |
| CI [logging.sh `ci_render_summary`](../../../tools/ci/lib/logging.sh) → `CI_SUMMARY` | 外层传入的 Status、开始/结束/耗时、Repo/Branch/Commit/Run dir、runtime/SQL config 与 MySQL port；stage、system scenario、SQL suite/case rows 的原生 status/耗时/log/artifact；可选 blocked、SQL failure classification、failure tail | summary 是外层记录的汇总，未写入的行不能补为通过；Commit 不等于二进制来源或能力，config 路径不证明有效配置。状态不代替实际进程退出/清理日志、不可变判据引用或共享保留。G2 补构建事实；实际 CI/领域 owner 与 G6b 保留运行边界，G7b 承接持续共享历史。 |
| 系统实验 [performance/provenance.rs](../../../tests/system-test-runner/src/performance/provenance.rs) → 场景根 `run-manifest.json` 及引用材料 | `kind/formal/run_id/scenario/command`、时间与 `status/exit_code`；源码 revision/tree hash/dirty、runner 路径、live BE build identity 和进程身份；配置/workload/fixture/tool/Cargo.lock/构建图/descriptor/effective-config/资源与 realization/inventory 的 hash；工具链、平台及 `build_profile`。完成路径验证输入未改变后记录 completed/0；未完成 Drop 尝试写 incomplete | 当前 writer schema 6，而 [UEA1 artifact_protocol.py](../../../tests/benchmarks/uea1/artifact_protocol.py) 的 `RUN_MANIFEST_SCHEMA_VERSION` 为 5，不能称 reader/writer 已对齐。`build_profile` 由 binary 路径分量推测，FE build identity 取一致的 BE identity；不等于逐二进制自报。Drop 的 best-effort 写入不保证崩溃时终态持久化或资源退出。G7a 设计共享采样/分析与记录接线，G2 补自报，UEA1 领域/G7c 承接漂移及旧实验迁移，不把旧 schema 或准入白名单冻结为共享合同。 |

构建身份的当前来源见 [version/build.rs](../../../novarocks/version/build.rs)：`NOVAROCKS_NATIVE_BUILD_IDENTITY` 显式字符串或完整 Git commit，以及短 hash/full commit/time；未自报实际传递依赖合并的能力、profile 和有效编译设置。G2 交付前记录当前可确认的构建材料与缺口，不能从源码 HEAD、target 路径、文件名或 identity 推定“未编入测试能力”。G7a 原样关联各 FE/BE 和 runner 实际使用的自报材料，G2 交付时验证接线；本文没有实现该机制。

SQL benchmark 是补充输出位置，见 [benchmark.rs](../../../tests/sql/runner/src/benchmark.rs)：默认 `reports/sql-benchmarks/` 下 `run-<millis>/<workload>/` 写 `run.json`、`SUMMARY.md`、测量 CSV 和 profile 材料。`WorkloadReport` 表达 workload/scenario/READY fixture、verification、warmup/measured runs、concurrency、cross-process topology、binary 路径以及 controller 提供的 environment/comparison key；这些字段不证明来源/能力或跨机器可比性，`verification: passed` 也不是性能达标。它不构成共享分析协议，供给、失败材料与测量迁移由 G7c 承接；现有命令和默认位置不在 G1 改动范围。

## 失败与结论边界

原始失败、复跑结果、归因和已批准处置分别记录。单跑通过只证明该次未复现；时间常量加并发负载也不能单独排除产品竞争。调查应保留实际负载/进程观察、失败阶段和相同条件下的比较证据；未知原因就保留未知，不以测试名或一次成功归因噪声。全量原始失败不能被定向通过覆盖，历史已批准失败处置也不能改写本次原始结果。

缺终态保持未完成；清理失败、必跑覆盖未执行、另行豁免及构建事实缺失均保留原状并缩小结论。采集成功只证明取得数据，没有适用比较方法时明确未评估。all-in-one smoke、定向功能检查、本地机制运行与真实 1FE+3BE/Linux 性能验收分别表达，不用全局 PASS 掩盖它们的边界。
