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

开发规则由 [workflow-contract](../../../.agents/skills/workbench/skills/workbench/references/workflow-contract.md) §8.5–§8.7 定义；本指南落实 NovaRocks 的位置、操作责任和现有报告解释，不建立第二套运行协议或结果 schema。新资产及实质扩展从规则合入起消费合同；存量按领域迁移，文件存在或一次通过不表示已经合规。当前阶段仅维护功能和数据正确性；SQL 原生入口仅发现 correctness suite，旧常设测量载体已退出。

## 准入与领域迁移

新增测试、workload、采样/比较定义、SQL 指令或架构检查前，先确认现有覆盖为何不足、产品性质、资产性质和最低充分层级、独立预期，以及输入、隔离、真实退出、维护 owner、成本、消费链路和频率。答案靠近原生定义；简单测试可由函数、断言和模块说明承载，无需逐例表单。一次性探索只有明确长期消费者后才按同样条件晋升源码资产。

SQL 文件保留必要的语义输入和断言，多步生命周期编排归 system owner；未知、非法或未实现指令不得静默成为无断言通过。现有指令拒绝行为由 TST-G13 迁移。架构规则保护依赖、类型与模块责任，优先复用声明式工具；存量检查由 TST-G14 收敛，不为本文添加源码 token、精确路径或任务编号门禁。

产品修复与治理场景可以成对迁移，但先接受唯一领域 spec；产品 plan 与治理 plan 引用同一设计，划分切片并反链完成情况。没有领域 spec 时先细化并接受，不在两个 plan 各造一套测试设计。M04b 成对迁移先接受 G3 spec，NIG-1 成对迁移先接受 G12 spec；这不要求先完成整个领域。消费未交付接口时记录具体实现硬前置，原有 MEM/M07 等产品验收继续生效。

## 获批判据的 Git 快照

原生执行 owner 负责 workload、动作、合法输入、采样有效性、正确性断言、资源保护停止条件与实际退出；分析 owner 负责方法适用性、比较、效应量和不确定度。runner 不读取任务验收门限，不以历史报告判断当前运行资格。一次性实验的方法与输入引用已有不可变版本；本阶段不维护常设基准或持续测量协议。任务特有对照、门限和适用范围写入获批 plan，在候选确认性测量前固定。需要探索的先声明探索及后续判据确认，不能测后挑门限。

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

现有 runner 的原生 artifact-root、checkout 内 ignored logs/reports 仍允许。可通过新根满足独占的使用原生参数，如 system 的 `--artifact-root`；不能独占的明确记录存量缺口并交对应迁移。system `run_one` 对固定场景子目录用 `create_dir_all`，没有原子拒绝已存在目录的保证。调用者选新根是目前操作责任，本文不声称 runner 已强制执行。关联所有输出的真实位置，不提交运行产物进源码 Git。

## 保留、共享交接与清理

材料默认不自动过期，未解决失败继续保留。只有任务结论已审阅、正式引用和重分析所需脚本/输入说明已共享交接且核实可恢复后，才按记录的处置清理本地副本。未被正式引用的纯探索可在明确结束并记录取舍后清理。正式材料由存储 owner 记录访问、保留依据与处置；不能用删除 worktree 或定时清理代替。

本地路径只称本地证据。正式共享引用需有真实共享访问、保留 owner 以及所引用 Git 对象/输入/脚本的恢复条件；尚无共享交接时如实声明。本指南不自动上传、创建云服务或删除材料，凭据继续按既有规则脱敏。历史失败可显式用于分析与定位，不能成为新采集的隐藏输入；声明为长期 fixture 时按 fixture 自身合同维护。周期调度和共享证据存储尚未交付；TST-G7a/G7b 已取消，没有新的持续测量/共享历史服务承诺。干净工作树或本地 commit 不替代这些证据。

## 代表性原生报告映射

下表是当前写出端的维护说明，不是全仓资产索引或另一份运行权威。字段可表达某个事实与某次运行已写出该事实必须分开；以下仅核验源码，没有生成通过样例或运行集群/性能实验。正式报告优先引用实际原生输出，补充判据版本、缺失事实、材料关联和结论边界。

| 写出端/材料 | 当前能表达的事实 | 尚不能由它单独证明、后续责任 |
|---|---|---|
| system [scenario.rs `write_evidence`](../../../tests/system-test-runner/src/scenario.rs) → 场景根 `scenario-evidence.json` | `scenario`、`command`、开始/结束时间、`source_revision/source_dirty/source_tree_sha256`、runner identity/路径、Cargo.lock hash、平台、`actions/phase_observations`、runtime/binary/config 路径及 config hash、`cluster_size`、`launch_profile`、进程 launch identities、有效配置及 hash、`outcome/exit_code`、失败 diagnostics；当前 schema 5 | `launch_profile` 是 harness 的 `fault-scenario/performance` 配置，不是 Cargo profile。binary 路径与 runner identity 不证明各 FE/BE 实际能力；实际 role-build-information 单独保留。字段不保证每个阶段均被观察或此次已运行，各领域仍须提供性质/退出观察。 |
| system [runner.rs `run_one`](../../../tests/system-test-runner/src/runner.rs) → stdout/stderr、场景 evidence 与 runtime/log 根 | 选择由 registry/`--only` 决定；失败保留材料、动作、诊断和 rerun 命令。正常路径先检查 cluster shutdown 与 fixture teardown，追加 cleanup action，再写 Passed；失败路径先写 Failed，随后处理 cleanup 错误 | evidence 的 `exit_code` 按 outcome 合成 0/1，不是每个子进程退出码。Failed 写出时尚未完成 cleanup；cleanup 出错或启动失败可能缺最终 evidence，须保留 runner 原始错误与实际退出事实。打印的旧 rerun 根不能直接复用，复跑换新根。现有 G6b/领域链路承接隔离与终态缺口。 |
| CI [logging.sh `ci_render_summary`](../../../tools/ci/lib/logging.sh) → `CI_SUMMARY` | 外层传入的 Status、开始/结束/耗时、Repo/Branch/Commit/Run dir、runtime/SQL config 与 MySQL port；stage、system scenario、SQL suite/case rows 的原生 status/耗时/log/artifact；可选 blocked、SQL failure classification、failure tail | summary 是外层记录的汇总，未写入的行不能补为通过；Commit 不等于二进制来源或能力，config 路径不证明有效配置。状态不代替实际进程退出/清理日志、不可变判据引用或共享保留。G2 补构建事实；实际 CI/领域 owner 与 G6b 保留运行边界；共享历史服务尚未交付。 |

Native build identity 仍由 [version/build.rs](../../../novarocks/version/build.rs) 提供，不是能力或发布认证。实际产物的 `--build-info` 由 [Server build_information](../../../novarocks-server/src/build_information.rs) 与两 runner 各自写出；owner 能力来自实际 crate 的编译事实，binary 的 `actual_cfg` 与 build-script 的 package 局部事实分列。未声明 profile、不可观测的最终 LTO/codegen/dependency overrides/额外 flags 保留 null/unknown；不能从源码 HEAD、target 路径、文件名、identity 或 runner profile 补造。`release_certification` 明确为 `not-certified-by-self-report`，字段存在不证明 G2 全部验收通过。

## 构建选择与控制授权

[CI build_capabilities](../../../tools/ci/lib/build_capabilities.sh) 复用既有 Cargo 构建入口，普通、控制和兼容性变体各用独立 target。默认 `target/normal`、`target/test-controls`、`target/variants`，可通过 `NOVA_CI_NORMAL_TARGET_DIR`、`NOVA_CI_CONTROL_TARGET_DIR`、`NOVA_CI_VARIANT_TARGET_DIR` 指向已有 owner 管理的缓存；三根不能相同或互相包含。缓存复用不意味着证据根复用，不覆盖普通 binary 后再恢复，也不以清理缓存解决产物选择。

CI 支持 `dev`、`dev-opt`、`release`，默认 `NOVA_CI_CARGO_PROFILE=dev-opt`；每次构建同时显式传 `--profile` 和 `NOVAROCKS_BUILD_PROFILE`。实际 `--build-info` 在无 config/fixture 时读取并先保存原始双流与退出，随后校验本次已声明的 kind/profile/capability/identity；未知事实不被补成构建参数。受支持 flags 检查与实际 cfg 校验是当前构建约束，不能认证所有依赖的最终设置。

普通 Rust workspace 测试保持普通构建。full CI 的 `test-control owner tests` 单独显式选择 failpoint（含 `typed`）、MV、FE、Native、Worker、fs、Execution 七 owner 的 `--lib`，使用 control target 和原 profile，串行运行；不能同次选择普通 Server 或用 `--workspace --all-features` 合并两种身份。`--skip-cargo-test` 记录此组为 SKIP，不能据此宣称机制保护通过。HMS/MySQL 的旧 feature 专属模块另选各 owner，不包含在基础七包覆盖内，例如：

```bash
NOVAROCKS_BUILD_PROFILE=dev-opt cargo test -p novarocks-connector-iceberg --lib \
  --features test-controls,mem-1-m07-hms-listing-observe --profile dev-opt --locked \
  --target-dir "$NOVA_CI_VARIANT_TARGET_DIR/hms" -- --test-threads=1
NOVAROCKS_BUILD_PROFILE=dev-opt cargo test -p novarocks-mysql-adapter --lib \
  --features test-controls,mem-1-m07-exact-mysql-write --profile dev-opt --locked \
  --target-dir "$NOVA_CI_VARIANT_TARGET_DIR/exact-mysql" -- --test-threads=1
NOVAROCKS_BUILD_PROFILE=dev-opt cargo test -p novarocks-mysql-adapter --lib \
  --features test-controls,mem-1-m07-closing-pressure --profile dev-opt --locked \
  --target-dir "$NOVA_CI_VARIANT_TARGET_DIR/closing" -- --test-threads=1
```

上述变量须先绑定真实、互斥的 owned cache，命令本身不是已运行证据。历史私有 exact/held/HMS 完整矩阵没有原输入时记录 `NOT_RUN`，不制造历史 SHA/receipt；局部 owner 测试、授权动作/退出与完整矩阵分开表达。

System 使用实际 `--binary`、`--ordinary-binary`、`--test-control-binary`；兼容性场景冻结用户选中的各角色变体，CI 明确选择 controls Primary，ordinary 独立保留。SQL 使用 `NOVAROCKS_BIN` 与 `NOVAROCKS_TEST_CONTROL_BIN`，从实际选中的有限步骤生成必要授权；feature、路径、debug/profile、FaultScenario 名称及旧 env 不授予动作权。普通步骤继续普通产物，需要内部断言的步骤缺能力就失败，不能静默省略断言。裸测试 CALL 不授予权限，非破坏观察也不授予 wipe。

当前 MV 测试保持受控过渡，LNP-3A/LNP-3D、storage-contract 和 system 现有 SQL/expected/独立 oracle 不重写，旧 CALL/directive 禁止扩展。whole-family wipe 绑定本次 FE launch 与 MV Accelerator family，一次认领、隔离串行，先由原 MV owner 停止 admission 并实际 join 后台；不能把传入 MV 名当逐 MV 权限，也不能删 whole StateStore 代替。

| 唯一后续产品 owner | 成对迁移的现有性质与载体 |
|---|---|
| 7B3 | LNP-3A、LNP-3D 对应 case、`mv/lake-publication-restart-rebuild`：非聚合冷恢复、配置/paused、known-committed、Unknown/迟到提交与管理接续 |
| 7B4 | spine SUM/COUNT、T/S/P 聚合状态、聚合冷恢复及后续刷新；保留缓存 restart 不冒充缺失缓存恢复 |
| 7B5 | V/S 身份、view 发现与 CREATE/DROP、fixture/REST 路由、实际 materialized S 读取与 SQL fallback；适配前序已迁移场景 |
| 7B11 | SQL/system 独立 document graph、shared-document-revisions、文档格式；替代保护通过后退出只服务旧图形的 package/provenance 消费者 |

每个 owner 按自身 accepted spec/approved plan 取得替代保护后逐项退役，最后消费者退出再删共享载体。G2 不新增启动期缓存缺失、管理收束或通用擦除 API，不代表上述产品任务已完成。

## 当前功能资产、成本与频率

| 性质 / owner | 入口与成本 | 相关改动与频率 |
|---|---|---|
| result support、FS corpus、MV owner | 对应 Rust crate；本地、无集群 | 模块/helper/输入改动必跑，沿已有 Rust CI |
| 慢端、并发结果与原查询期限 | system 精确 selector；1FE+3BE、显式 | writer/buffer/ACK/取消/helper 改动必跑；领域维护者每周手工轮次 |
| range/delete/planning/DOP | system 显式功能组；真实 fixture 和 1FE+3BE，成本较高 | provider/cache/delete/range/admission/fixture 改动必跑；每周相关领域轮次，缺输入如实记录 |
| MV/OPTIMIZE | SQL correctness 的具体 case | publication/maintenance 改动必跑，沿既有 suite |
| SSB/TPC-H/TPC-DS | 原生 SQL 三套 `explicit_only`；高成本 | runner/输入绑定改动必跑三套；领域 SQL 按相关性选择，每周手工完整数据集轮次 |

这些周期是维护责任，尚无自动调度；没有真实运行就不记录周期通过。完整发现与默认执行分别由 `--list/--list-default`（system）和 `--list-suites`/suite manifest（SQL）定义。`performance` launch profile 只选择干净的原生 harness 配置，支持 release binary 并拒绝 fault 注入，不是性能实验。相关指南见 [system](../../../tests/system-test-runner/README.md) 与 [SQL](../../../tests/sql/README.md)。

SSB/TPC 输入 owner 显式供给 READY，消费者仅 check，不在失败后自动 ensure。SQL 正常成功、语句错误、结果 mismatch 和清理失败分别保留原生返回值、case/expected/actual/diff、FE/BE 日志和实际退出；耗时只作普通运行说明，不转成测量 CSV 或比较门。FE preparation 运行级路由和计时钩子退出，独立鉴权的 statement observation 保留功能消费者；这不表示 G2 发布能力门控完成。

## 失败与结论边界

原始失败、复跑结果、归因和已批准处置分别记录。单跑通过只证明该次未复现；时间常量加并发负载也不能单独排除产品竞争。调查应保留实际负载/进程观察、失败阶段和相同条件下的比较证据；未知原因就保留未知，不以测试名或一次成功归因噪声。全量原始失败不能被定向通过覆盖，历史已批准失败处置也不能改写本次原始结果。

缺终态保持未完成；清理失败、必跑覆盖未执行、另行豁免及构建事实缺失均保留原状并缩小结论。采集成功只证明取得数据，没有适用比较方法时明确未评估。all-in-one smoke、定向功能检查、本地机制运行与真实 1FE+3BE/Linux 性能验收分别表达，不用全局 PASS 掩盖它们的边界。

G7c 本次批准的剩余 SQL 与单测失败例外只影响本次退役推进条件：保留各次 raw FAIL、具体失败集合、来源和批准处置，不改 golden、断言或原始退出码，不称全套 SQL/UT 已通过。该例外不豁免新的语句观测、生命周期、输入校验与清理性质，也不构成后续任务的全局 baseline 豁免。最终 P09/workspace 结论由真实独立结果填写。
