---
id: ADR-0194
title: "CI Clippy is strict without making every Cargo warning an error"
domain: [crate-boundary]
status: active
supersedes: []
superseded-by: null
partially-supersedes:
  - id: ADR-0190
    scope: "Only its statement that global Clippy remains warning-only and strictness is deferred; dependency governance, resource estimation, and other conclusions remain active"
date: 2026-10-22
provenance:
  - "discussion: 2026-10-22 strict Clippy scope, shared baseline, and exception policy"
code-anchors:
  - "tools/ci/local-full-ci.sh (cargo clippy stage)"
  - "Cargo.toml ([workspace.lints.clippy])"
  - "clippy.toml (too-many-arguments-threshold)"
  - "novarocks/native-trust/clippy.toml (nearest Clippy configuration)"
---

## 问题

如何在合入前阻止 workspace Clippy 警告继续积累，同时不让每次 Cargo 编译都采用全局 warnings-as-errors 策略？

## 背景与执行事实

`tools/ci/local-full-ci.sh` 的 Clippy 阶段通过 `ci_main_cargo` 运行 workspace、dev profile、locked、all-targets 检查。`-D warnings` 写在 Clippy 分隔符 `--` 之后，只作用于该 Clippy 命令；不需要改变其它 Cargo 命令、构建指纹或外部依赖的编译方式。这个阶段不等同于 rustdoc 检查。

Clippy 级别与 Clippy 配置属于不同的配置面：

| 事实 | 维护位置 | 规则 |
|---|---|---|
| workspace 的 lint 级别 | 根 `Cargo.toml` 的 `[workspace.lints.clippy]` | 只集中放宽 `result_large_err = "allow"`；理由是 typed failure 事实有意保持完整，当前阶段不以错误类型栈大小作为优化目标 |
| 参数数量阈值 | 根 `clippy.toml` | `too-many-arguments-threshold = 10` |
| native-trust 的 API 限制 | `novarocks/native-trust/clippy.toml` 与该 crate 的 deny 属性 | 本地配置禁止从信任 crate 读取配置来源、文件系统及绕开连接 owner 的接口，并重复根阈值 |
| 局部例外 | 具体 lint 触发位置 | 新增例外必须使用带 `reason` 的 `#[expect(...)]`，不新增 `#[allow]` |

`clippy.toml` 只读取离 crate 最近的一份，不合并。因而 native-trust 的局部文件必须保留自己的 API 限制，并同步根配置中的共享阈值。严格参数覆盖 workspace 的默认 feature、dev profile、all-targets 与 locked 构建；非 workspace 依赖没有因此被重新定义为 Clippy 检查目标。

## 考虑过的选项

1. **只在 CI Clippy 阶段使用 `-D warnings`，并维护窄基线（采用）。** 这让本地 `cargo build` / `cargo test` 的现有行为不变，同时使 CI Clippy 对 workspace 警告 fail-fast。代价是开发者必须运行 Clippy 才能把警告作为错误处理。
2. **在 workspace 设 `warnings = "deny"`、源码写 `#![deny(warnings)]` 或设置 `RUSTFLAGS=-D warnings`（成本否决）。** 这会把严格性扩展到不属于 Clippy gate 的 Cargo 命令；环境变量还会改变整个构建图并影响后续编译。若将来明确要求所有 Cargo 构建都拒绝警告，应重新评估其构建与依赖范围，而不是暗中扩大当前 gate。
3. **通过更多全局关闭或静默的 `#[allow]` 清除新诊断（成本否决）。** 这会让真实的新诊断与不再需要的例外都更难发现。若 toolchain 的新 lint 造成系统性误报，应按共享对齐规则审核基线，不以无理由的局部屏蔽代替裁决。

## 裁决

**严格阶段规则。** `local-full-ci.sh` 的 Clippy 阶段使用 `ci_main_cargo clippy --all-targets -- -D warnings`。`ci_main_cargo` 继续固定 workspace、dev profile、locked 与本 worktree 的 `target/`。不把严格性移入 workspace `warnings` lint、crate 属性或 `RUSTFLAGS`。

**最小共享基线规则。** 只保留与 Databend、RisingWave 共同放宽的配置：`result_large_err` 在 `[workspace.lints.clippy]` 设为 `allow`，参数阈值在根 `clippy.toml` 设为 10。`large_enum_variant`、`type_complexity` 及仅由其中一方采用的放宽项保持 Clippy 默认。

**最近配置同步规则。** `clippy.toml` 不合并。修改根阈值或增加共享配置时，必须同步 native-trust 的近邻配置；不得覆盖该 crate 的 `disallowed-methods` / `disallowed-types` 限制。

**诊断处置规则。** 机械诊断修复其表达；无调用方的代码删除，只在测试或非默认配置使用的代码按真实使用面收窄 `cfg`；揭示语义缺陷的诊断按根因修复并补独立测试；不再触发的 `#[expect]` 删除。除集中基线调整涉及的项外，不批量转换存量 `#[allow]`；新增局部例外必须有 `reason` 的 `#[expect(lint, reason = "...")]`，不得新增 `#[allow]`。

**工具链升级规则。** 修改 `rust-toolchain.toml` 的变更负责处理新 Clippy 诊断：优先修复；确需调整基线时，遵循最小共享对齐规则并说明理由。升级不能通过回退严格 gate 或把新警告静默列入 allowlist 完成。

## 接受的妥协（诚实记录）

普通开发构建不把所有警告变为错误；未运行 Clippy 的本地检查不会提供同等拦截。`result_large_err` 的性能类诊断继续放宽，错误类型仍可能较大；超过参数阈值、复杂类型和大枚举则由逐处修复或有理由的 `expect` 承担。严格 gate 只针对 workspace Clippy 目标，不承诺第三方依赖也经过相同 lint。

## 何时重新评估

- 如果产品政策要求 `cargo check`、`build` 与 `test` 等所有开发构建均拒绝警告，重新评估全局 warning policy，并测量对构建图和依赖编译的影响。
- 如果性能重新成为当前阶段目标，重新评估 `result_large_err` 及错误类型大小的运行时成本。
- 如果 Databend 与 RisingWave 的共同 Clippy 基线发生变化，或固定 toolchain 的升级持续引入同类系统性误报，重新审查共享放宽项与参数阈值；保留逐项理由及最近配置同步规则。
