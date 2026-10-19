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

# 测试进程、日志与材料所有权

`novarocks-test-support` 是不依赖产品 crate 的测试机械叶子。领域配置、SQL oracle、fixture 和 FE/BE 拓扑仍由各自 owner 维护；本库提供端口预留、受管进程、准确退出、日志观察和材料保留。设计见 [ADR-0069](../../docs/adr/ADR-0069-shared-test-mechanics-owner.md) 与 [ADR-0184](../../docs/adr/ADR-0184-native-test-execution-and-process-monitor-ownership.md)。

## 构造与实际退出

从 `ManagedCommand::new(payload)` 开始构造请求，再设置 `arg/args`、`env/envs/env_remove/env_clear` 与 `current_dir`。它从一开始以辅助 monitor 为实际执行程序，保留 payload 的原生 `OsString` 参数；不能把任意已配置的 `std::process::Command` 原地转换进来。任意 `pre_exec`、stdin 协议、凭据、rlimit 或 Stdio 配置不在透明转发合同内。

```rust
use novarocks_test_support::{ManagedCommand, ManagedProcess, ReadyMarker};
use std::{path::Path, time::Duration};

fn check_service(binary: &Path, config: &Path, locator: &Path) -> anyhow::Result<()> {
    let mut command = ManagedCommand::new(binary);
    command.arg("--config").arg(config);
    let mut process = ManagedProcess::spawn(
        "service fixture".into(), command,
        ReadyMarker::StdoutContains("READY".into()),
        Duration::from_secs(30), locator.to_owned(),
    )?;
    process.assert_log_contains("READY")?;
    process.stop()?;
    // Consume all lifecycle and output assertions before releasing diagnostics.
    process.release_diagnostics()?;
    Ok(())
}
```

readiness 必须由 child 发布 marker。`StdoutContains` 只观察 stdout；stderr 同名文本不能建立 ready。`FileContains` 在启动前保存 generation，拒绝未更新的旧 marker 和只追加无关内容的文件；跨追加边界的新 marker 与重写后的新 generation 可以建立 ready。原操作 deadline 覆盖启动、交接及观察；one-shot 的 ready 不续一份完成预算。restart 先实际收束旧实例，再创建新 PID 和独立日志。

monitor 拥有 payload 的真实 `Child`，在独立 session 内存活；payload 另建进程组。控制方通过私有 FIFO 发操作，控制方退出关闭 lifeline，monitor 继续完成组信号、退出观察、最后组信号和实际 wait。`stop`、`kill_now`、SIGINT、SIGTERM 等正常 API 必须检查结果；Drop 仍会关闭/收束，但不能替代成功路径的错误检查。控制方失联后即使清理成功，失联事实仍保留。nextest 负责测试进程，monitor 负责这个明确创建的 payload；它们是不同的 Child owner。

`request_termination()` 只发送 SIGTERM，由调用方的 `wait_for_successful_exit_until()` 维护排空期限；`interrupt_and_wait()` 同样保留调用方传入的原期限。请求信号不会提前套用 `stop()` 的 5 秒 TERM 宽限；显式 stop/kill、控制方失联或监护错误才进入独立清理流程。

辅助程序由本库 `build.rs` 使用 Cargo 提供的原生编译器、target 和实际构建参数编译到自己的 `OUT_DIR`，通过编译期路径交付。测试执行期间不运行 Cargo，也不从 PATH 查找 monitor。当前交付拒绝 cross-target；不能将辅助程序单独搬到另一台机器后假定原路径继续有效。

## 原始日志与有界观察

payload 直接写 run-owned `stdout.log`、`stderr.log`，monitor/guard 不持 nextest 的捕获管道。`spawn` 的最后一个路径参数写出 stdout/stderr/record 的定位信息，**不是 combined log**。consumer 使用 `stdout_log_source()`、`stderr_log_source()` 或 `log_contents()` 等 API 读取，不能假定 locator 文件包含进程输出。

| API / 材料 | 合同 |
|---|---|
| 原始 stdout/stderr | 保存完整字节；有限 tail、行观察和展示截断不改写原始文件 |
| `read_tail(max_bytes)` | 返回至多请求字节数及原始长度；不替换为另一个路径的新文件 |
| `with_bounded_snapshot_reader` | 访问原文件的有界时点快照；允许同文件追加，拒绝替换/截短；保留 visitor 原错误及独立完整性错误 |
| `original_durable_log_identity` | 启动时冻结的 device/inode 标量事实，不授予读取替换文件的权限 |
| `live_log_line` | 增量观察完整 UTF-8 行，观察前后确认 payload 存活；完整行诊断缓冲上限 **1 MiB**，超限或非法 UTF-8 显式失败并保留原日志 |
| `capture_command_until` | 保留实际 exit、`timed_out` 和独立 failures；完整 golden 输出从原始文件读取，读取/复制也消费原 deadline；失败或超时时原始 run-owned 文件保留 |

1 MiB 仅约束实时完整行诊断接口，不是 payload 输出、stdout readiness 或完整 golden capture 的大小上限。不得为了消除错误截断原始日志或把失败当作没有匹配行。输出 source 的 clone pin 住普通材料，跨 stop 继续可读；所有需要原日志的断言必须先完成，再显式释放诊断。正常 stop 只证明生命周期已结束，不证明整个测试通过。

## 普通调用、懒建与嵌套 runner

普通材料默认位于 `${XDG_STATE_HOME:-$HOME/.local/state}/novarocks/test-runs/`。`XDG_STATE_HOME` 必须是绝对路径。纯测试和原生列举不创建材料根或 guard；首次受管操作才检查平台、原生执行身份和存储能力。普通调用的这条 Rust 路径不依赖 Python。

nextest 以实际 `NEXTEST_RUN_ID` 归属整次调用，记录原生 `NEXTEST_BINARY_ID`、`NEXTEST_TEST_NAME` 和版本，不创建另一套测试身份。首次懒建必须能确认直接父映像是实际 nextest，并绑定其内核退出观察；Linux 使用 pidfd，Darwin 使用 kqueue。缺身份/能力、错误父关系或 guard Ready 交接失败明确拒绝，不能降级成普通 Cargo。PID 1 本身不被拒绝，容器中的 nextest 仍须满足同一父子与能力合同。

嵌套 runner 首次接触受管能力前，外层 nextest 测试先显式建立租约，然后保持原生上下文和状态目录启动 runner：

```rust
use std::{io, path::Path, process::Command, time::{Duration, Instant}};

fn run_nested_runner(program: &Path) -> io::Result<()> {
    novarocks_test_support::ensure_run_retention(
        Instant::now() + Duration::from_secs(10),
    )?;
    let status = Command::new(program).status()?;
    if !status.success() {
        return Err(io::Error::other("nested runner failed"));
    }
    Ok(())
}
```

这里的 raw runner `Child` 由外层 `Command::status` 实际 wait；其后创建的受管 payload 仍归 monitor。不要伪造 nextest 字段、清除继承上下文，或在嵌套子进程中接管失败/已关闭的租约。并发首用、重复 ensure 和同一原生调用中的 stress 复用同一个 Ready root。没有 nextest 身份的直接 Cargo/IDE/SQL/System 调用按调用进程归属，不自动取得 nextest 逐测试隔离或正式阶段汇总。普通进程 root 只有完成全部初始化和登记检查后才发布库拥有的 `.process-ready`；失败或半写的根不被后续调用接纳或当作已收束材料回收。

成功消费退出和所有日志断言后，持句柄的 consumer 调用 `release_diagnostics()`；最后一个输出 pin 退出后可删除普通成功材料。未释放、异常退出或失败材料在同一原生调用结束、登记封闭且所有相关监护记录完整后，进入最近调用窗口；默认保留 **3 次**，`NOVAROCKS_TEST_RUN_RETENTION_COUNT` 仅可设正数。这是调用数量窗口，不是磁盘硬上限。同次 nextest 的后续测试不能提前淘汰早期失败；活动 pin、缺 Final、未完成清理或未知结束的材料不为达标删除。需要长期引用的失败先按[测试治理](../../docs/guides/development/test-governance.md#保留共享交接与清理)交接保存。

## 正式入口与本地文件系统

正式入口创建独占材料目录，通过 `NOVAROCKS_TEST_RUN_ROOT` 注入受管库，持 pin，封闭登记并汇总全部记录。不能手工设置一个普通临时目录冒充正式上下文。登记锁覆盖 Pending 到 payload spawn/拒绝；Closed 后禁止新 payload。完整 Final、实际退出与清理事实分别校验，锁空闲和目录暂时不变都不是成功。正式材料不受普通 release/三次窗口淘汰影响。

规范命令、原始 nextest/JUnit/Cargo/doctest 输出及 `execution-index.json` schema 1 见 [CI 执行接口](../../tools/ci/README.md)。日常 `dev-test.sh test` 在准备后前台 exec nextest，保留 TTY；正式批处理单独执行信号转交、原始文件转显、登记封闭和汇总。取消仍是取消，普通测试失败与回收违约不能互相抵销。

运行中的 pin/gate/记录和普通保留根只接受已检查的本地文件系统：Darwin 本地 APFS；Linux ext、XFS、Btrfs、tmpfs。NFS/SMB/FUSE、overlay 与未知类型拒绝受管登记。容器需把受支持的本机状态目录 bind-mount 进来，例如把本机准备好的绝对路径挂到 `/state` 并设置 `XDG_STATE_HOME=/state`；正式 `--output` 也放在该本地挂载下。材料归档存储可以另选，不把跨文件系统挂载塞进活跃 root 的控制或输出路径。

## 适用范围与验证

已有受管消费者包括 cluster-harness 的 FE/BE、Server smoke、SQL 的有原期限 shell，以及 System/Provider 已有受管调用。这次共享能力不接管所有 raw spawn：

- Server 的低 FD/rlimit 拒绝测试保留原 owner 的 raw `pre_exec`，避免限制唯一 monitor。
- FDB/MySQL stdin 协议 helper、Hadoop 两进程 fencing helper 保留原 owner 的真实 Child 与 wait。
- cluster build-info probe 保留自身 64 KiB 输出界限和有界回收；listing observer、Docker/fixture owner、普通无原期限 SQL shell 不自动变为受管 payload。
- 容器、外部 MySQL 实例、远端 fixture、主动逃离进程组的后代及作业级 containment 不由本库保证。Linux monitor 死亡保护只覆盖其直接 child，不证明完整树已清理。

相关测试在 `tests/process_monitor.rs`、`tests/process_retention.rs` 及共享 Unix 原语层，改动相应 owner/helper 时运行定向用例；Loom/Miri 继续各自专用入口。完整产品接线仍要求 native 1FE+3BE。**截至 2026-10-19，已有 macOS arm64 定向机制证据，native Linux x86_64 的生命周期/FD 和真实消费者验收仍未验证；Linux arm64 探针、编译通过或本指南本身都不能补齐该缺口。** 完整 workspace、专项 fixture 与产品结果按实际原始记录分别报告。

macOS 上实际观察到简单测试启动异常缓慢时，按[官方 nextest macOS 指南](https://nexte.st/docs/installation/macos/)检查 Developer Tools 等环境因素；不自动更改授权，不以单次耗时判定原因，也不增加常设性能门。
