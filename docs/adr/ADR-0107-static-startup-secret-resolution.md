---
id: ADR-0107
title: "Static startup secret resolution and direct provider credentials"
domain: [configuration]
status: active
supersedes: []
superseded-by: null
date: 2026-08-25
provenance:
  - "discussion: 2026-08-25 static startup secret and credential ownership"
  - "implementation: pending PR"
code-anchors:
  - "novarocks-server/src/app_config.rs (NovaRocksConfig::load_from_file)"
  - "novarocks/secret/src/lib.rs (SecretValue)"
  - "novarocks-server/src/composition.rs (state_store_provider_registry)"
---

## 问题

应用启动所需的 secret 应在何处解析、如何避免日志泄漏，以及 Object Store、MySQL 和 FoundationDB provider 应接收环境变量名称还是已经冻结的凭据值？

## 背景与执行事实

Server 是完整应用 TOML wire 与具体 composition 的唯一 owner（ADR-0072），StateStore provider 是只消费 typed input 的 leaf crate（ADR-0093）。此前 Object Store 在 Server 和 FS 间传递裸 `String`，而 MySQL 与 FoundationDB provider 会在运行期读取环境变量；FoundationDB 还会在 Server 校验与 network startup 两次读取同一来源。这让启动校验无法证明实际使用的值，且 secret-bearing 中间值可能经常规 `Debug` 泄漏。

启动配置不需要运行期更新：当前部署能够 provision 环境变量，并可通过重启相关 FE 或 BE 进程更换静态 credential。Native fragment、plan 和 StateStore durable record 也不是 credential carrier；每个角色进程必须从自己的启动配置构造本地 Connector binding。

## 考虑过的选项

1. **继续让各 provider 读取环境变量。** 改动最小，但把应用配置 source owner 下沉到 leaf crate，允许校验与实际连接使用不同值，也无法统一处理缺失、空和非 UTF-8 来源。
2. **只在各 secret 字段实现单独的环境变量 parser。** 可避免部分裸值，却会复制语法、错误和加载路径，并让新增 secret 字段容易漏接。
3. **支持插值、默认值、递归展开或运行期 reload。** 对部署模板更灵活，但会把静态配置变成顺序敏感的 source language，并要求定义旧新值重叠、失败恢复和跨角色漂移语义。
4. **Server 在 typed deserialize 前统一解析 exact reference，并向所有 consumer 投影 `SecretValue`。**（采纳）

## 裁决

Server 将 TOML 先解析为 structure，递归解析所有 exact `${ENV:VAR}` string scalar，再执行现有 typed deserialize 与跨 section 校验。只有 ASCII `[A-Za-z_][A-Za-z0-9_]*` 名称与完整 scalar 匹配；插值、无效名称、缺失、空值和非 UTF-8 来源在启动时 fail closed。替换值不递归解析，配置 path 与失败类别可以诊断，secret value 不能出现于 error、log 或 panic 文本。

`novarocks-secret::SecretValue` 是无 Serde、无 `Display`/`Deref` 的最小 redacted scalar。它只持有已经解析的值；不拥有来源、credential kind、runtime registry 或 rotation。Server wire 先使用普通 string deserialize，再根据 schema 把 Object Store access key/secret、MySQL password 和 optional FoundationDB TLS password 构造成 `SecretValue`。FS 与 provider 只在实际 SDK/client builder 处显式 expose。

MySQL 与 FoundationDB production config 不再接受环境变量名称，也不读取环境变量。Server composition 将一个启动快照直接传给 provider。literal secret 继续兼容；`${ENV:VAR}` 是推荐的部署写法。没有 alias、precedence、plaintext fallback、FE-to-BE credential carrier、live reload、旧新重叠窗口或 mixed-version compatibility path。

## 接受的妥协（诚实记录）

**环境来源只支持 exact reference。** 这放弃了模板常见的插值、默认值和文件/Vault/KMS source；选择它不是因为这些能力没有价值，而是因为本系统尚未定义它们的生命周期、权限、失败恢复和跨角色一致性。部署需要更复杂 secret broker 时必须另立 owner 与协议，而不能把复杂度偷偷塞进 TOML parser。

**literal secret 仍然允许。** 禁止 literal 会更严格，却会破坏本地开发和现有简单部署，并不能单靠类型保证外部文件权限。我们选择以默认脱敏与 exact reference 提供安全迁移方向，而不是在没有 secret manager 的前提下制造不可用配置。

**`SecretValue` 不承诺 zeroization。** Rust `String` 的内存生命周期和下游 SDK copies 仍存在；本次解决来源 owner、单次 snapshot 与常规诊断泄漏，不把一个轻量 wrapper 误称为完整内存安全方案。

**rotation 需要重启。** 这增加运维步骤，但避免同时接受新旧凭据、运行期读取环境或让 FE/BE 使用不同 snapshot 的长期兼容分支。当前静态 deployment 模型下，这一成本低于引入未经验证的动态 secret control plane。

## 何时重新评估

1. 若产品需要 Vault、KMS、file watch、动态 token 或短期 credential，先定义 source owner、缓存、失效、rotation 与跨角色一致性，再扩展输入模型。
2. 若部署需要 rolling upgrade 或同一集群 mixed-version 互操作，必须先定义 credential/config compatibility protocol；不能由 provider 保留旧环境变量读取作为临时桥。
3. 若可验证的 memory-protection requirement 出现，应独立评估 zeroization、SDK exposure 生命周期与 crash diagnostics，而不是改变此 scalar 的 source-owner 结论。
4. 若新的 startup binding 包含 session credential，保持 Server schema classification 和 FS `SecretValue` 投影，不得恢复裸 string 中间值或 transport carrier。
