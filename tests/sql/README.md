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

# SQL correctness entrypoint

原生 runner 仅发现 `tests/sql/correctness/`。当前阶段只维护功能与数据正确性，SQL benchmark binary、专用 workload、比较/采样协议及隐藏参数已退出；普通 elapsed 输出是运行说明。资产准入、失败保留和批准例外见 [测试治理](../../docs/guides/development/test-governance.md)，suite 覆盖见 [correctness map](correctness/README.md)。

## 原生运行与输入

生产验收使用 cross-process 1FE+3BE。runner 通过唯一显式 `NOVAROCKS_BIN` 指定主 Server，原生 owner 为实际选中有限动作建立准确授权并拥有 spawn、READY、执行和实际 shutdown；`all-in-one` 只作 smoke。标准 dev 的能力由实际 cfg 编入，能力不授予控制权。

先用规范 [dev-test.sh](../../tools/ci/dev-test.sh) 同次准备 workspace tests/bins，真实 Cargo JSON 绑定路径、自报和 hash。CLI 保留原 `tests/sql/runner/Cargo.toml` 与包身份，库位于其子目录；裸 `cargo run --manifest-path tests/sql/runner/Cargo.toml -- ...` 仍明确运行 SQL CLI。日常/CI 使用已准备程序，退出次级构建。完整合同见 [构建选择](../../docs/guides/development/test-governance.md#构建选择与控制授权)。

非隔离套件先绑定验证过 BOM 的 fixture，再解析一次 immutable publication；不猜固定端点和凭据：

```bash
docker/iceberg-rest/up.sh
fixture_publication="$(python3 -c 'from pathlib import Path; print(Path("docker/iceberg-rest/runtime/current/published").resolve(strict=True))')"
source "$fixture_publication/env.sh"
tools/ci/dev-test.sh prepare logs/sql-attempt/artifacts.json
source tools/ci/lib/build_capabilities.sh
export NOVA_CI_ARTIFACT_RECEIPT="$PWD/logs/sql-attempt/artifacts.json"
export NOVAROCKS_BIN="$(ci_bound_binary novarocks)"
sql_runner="$(ci_bound_binary novarocks-sql-test)"
"$sql_runner" \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" --suite filter --mode verify \
  --cluster-mode cross-process --cluster-size 3 -j 1
```

`--list-suites` 列出所有注册套件，包括 explicit-only；`--suite all` 按 suite manifest 排除 explicit-only。`--list-extensions` 不启动集群。默认配置不提供完整 fixture 端点，不能用旧环境补造缺项。隔离 REST 套件由 runner 启停私有 fixture，准备和测试分离，见 [fixture 操作](../../docker/iceberg-rest/README.md)。

能力需求来自 merge/过滤后真正选中的有限步骤、已有指令与原生 capability tag；普通 SQL 仍使用同一主产物，内部断言不能因缺测试产物而跳过。feature 编入、profile、suite 名或旧环境不授予控制，裸测试 CALL 也没有自动权限。`test-capability:mv-document-observation` 只声明非破坏观察，不授予 wipe；未知 `test-capability:` 值明确拒绝。`mv_storage_contract_shared_document_revisions.sql` query 5/6 已就近声明此 tag，并保留原六个普通标签；两步只请求非破坏观察，SQL/expected/oracle 不变。G2 的完整 storage-contract 十个 case 已通过，验证范围与未运行项见 [G2 验证记录](../../docs/guides/development/tst-g2-validation-status.md)。

每次 FE/BE launch/restart 在 spawn 前读真实产物自报，保存原双流、实际退出和角色映射；会话绑定实际 Native process 与准确 statement/动作目标。自报的 profile 声明、binary cfg、package 局部事实及 null/unknown 分列，明确不作发布认证。缺授权、错误产物、原期限到期和实际资源/进程清理失败都保留原始失败，不能用未命中注入的正常结果代替通过。

LNP-3A/LNP-3D 与 `mv-storage-contract` 保留原 SQL、`.result` 和独立文档图，旧 CALL/directive 禁止扩展。当前 whole-family wipe 必须是显式 FE launch/family 授权、一次认领、隔离串行且 MV owner 停止 admission/实际 join 后台；普通裸 CALL 不授予擦除权。唯一后续迁移责任见 [correctness map](correctness/README.md#mv-受控过渡与后续迁移)，不在本次新增恢复或擦除 API。

## SSB/TPC 数据正确性

`ssb`、`tpc-h`、`tpc-ds` 与其他 suite 共用正常 case discovery、suite init/cleanup、结果比对和 server cleanup；分别消费 scale `1`、`1`、`1GB` 的共享 READY 数据。仅以完整 suite manifest 的 `prepared_dataset` 绑定输入，不读取旧测量 scenario/window 定义。

输入 owner 先显式准备。下例变量需要替换成仓库外独占尝试目录和真实 suite/scale，不在 verify 的错误恢复路径自动供给：

```bash
python3 tests/sql/fixtures/benchmarks/resolve_benchmark_fixture.py \
  --suite "$suite" --scale "$scale" --shared-root "$NOVA_ENV_SHARED_BENCHMARK_ROOT" > "$attempt/resolved-dataset.json"
tests/sql/fixtures/benchmarks/bootstrap_benchmark_data.sh \
  --suite "$suite" --scale "$scale" --resolved-dataset "$attempt/resolved-dataset.json" --ensure
```

同次 publication、真实 Spark image/BOM、schema与对象布局由既有输入 owner 核对。消费者只运行 `--check`，核对 READY 的 dataset key、manifest URI、publication identity/ETag及实际 warehouse；缺失、失效或不匹配即非零退出，不启动集群或重写数据。新对象存储需重新显式供给，共享 READY 不属于 worktree purge。

```bash
"$sql_runner" \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" --suite ssb,tpc-h,tpc-ds --mode verify \
  --cluster-mode cross-process --cluster-size 3 -j 1 --cluster-launch-profile performance
```

`performance` 是 clean harness 启动配置，支持 release binary、拒绝 fault 注入；本命令不测吞吐或延迟分位数。三套高成本 suite 是 explicit-only，不扩默认 CI。runner/输入绑定变更必跑三套；领域 SQL 变更按相关性选择；维护者每周手工完整轮次，自动调度尚未交付。邻近说明见 [SSB](correctness/ssb/README.md)、[TPC-H](correctness/tpc-h/README.md)、[TPC-DS](correctness/tpc-ds/README.md)。

## 失败、材料与退出

语句失败与 mismatch 非零退出，原始 FAIL 保留。未传 `--write-actual-dir` 也由失败材料根保存 case/expected/actual/diff；默认根为 `logs/sql-test-failures`，可用 `--failure-artifact-dir` 或现有环境变量选择真实独占材料位置。FE/BE 日志、suite hook错误、primary error和cleanup error分别记录，cleanup失败不能被结果 PASS 覆盖。每次重跑换新材料根，不自动复跑或删除旧失败。

suite init失败仍执行所属cleanup一次；case失败不跳过正常cleanup；runner拥有实际cluster shutdown和残留进程核对。执行成功、SQL ERR、取消、owner排空、进程退出是不同事实。定向通过不覆盖全量 raw FAIL，批准的本次例外与运行结果分别表达。
