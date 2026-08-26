---
id: ADR-0110
title: "Native caller authentication with authenticated plaintext and optional TLS"
domain: [native-transport-security, configuration, runtime-role]
status: active
supersedes: []
superseded-by: null
date: 2026-08-26
provenance:
  - "discussion: 2026-08-26 native caller authentication and optional TLS"
  - "implementation: pending local branch"
code-anchors:
  - "novarocks-server/src/launch.rs (resolve_server_launch)"
  - "novarocks/frontend/src/native/transport.rs (Client)"
  - "novarocks/backend/src/rpc/server.rs (BackendRpcServerHandle::start)"
---

## 问题

NovaRocks 的 FE/BE Native RPC 应如何在不把可信内网假设伪装成加密通道的前提下，统一证明 caller 属于同一 deployment，并在需要时增加严格的 TLS 保护？

## 背景与执行事实

Native FE/BE 是独立进程和故障域。NWT-2 已把 Native gRPC 与 management HTTP listener 分离，并使 all-in-one
只组合两份正常 role config；但现有 Frontend/Backend Channel factory 固定使用明文 `http://`，Native listener
在 bare TCP 上路由完整 gRPC service，caller 没有 deployment credential。能访问 Native 端口的请求会进入 route
和 protobuf decode。

ADR-0107 已裁决 Server 在 typed config 前解析 exact environment reference，并向 consumer 投影 redacted
`SecretValue`；ADR-0108 已裁决 Native 与 management surface 分离以及 all-in-one 的双配置启动路径。二者都未
定义 Native caller proof、TLS profile、endpoint identity 或 plaintext 的风险边界。

## 考虑过的选项

1. **继续依赖内网和端口隔离。** 部署最短，但没有可验证 caller identity；错误配置、横向访问或未来 listener
   扩展都能直接抵达 Native service。
2. **默认强制 mTLS 或 TLS。** 能同时提供 channel confidentiality/integrity 和 server identity，但为所有可信
   内网部署引入证书 provisioning、握手成本和运维门槛，也会把 caller authentication 与 certificate identity
   绑定为同一权威。
3. **每条 Native RPC 使用 deployment JWT，默认 authenticated plaintext，TLS 是统一可选附加层。**（采纳）
   shared secret 通过短期 HS256 token 证明 caller deployment；`disabled` 是明确 h2c 模式，automatic/PEM
   则在同一 RPC/application path 上增加 TLS 1.3 server authentication。
4. **让各 role 自己读取 secret/PEM、各自实现 token 或保留 no-auth compatibility。** 迁移看似容易，但会产生
   多个 source owner、不同验证规则和长期 bypass，无法证明全服务完成 hard cut。

## 裁决

每份 deployable FE/BE config 必须有同一 deployment identity 与 static shared secret。Server 在 ENV resolution、
typed validation、PEM read 后、任何 listener bind/outbound connect 前，分别为每个 role 构造 immutable
`Arc<NativeTrust>`；all-in-one 构造两个独立 role instance，不使用 process-global trust 或 direct-call path。

所有 FE→BE、BE→BE、BE→FE Native RPC 都只接受一个 strict HS256 compact JWT。token 只证明 deployment
membership：`aud` 精确绑定 deployment、`sub` 仅作诊断、`iat`/`exp` 受短期和clock-skew规则限制；它不代表
topology membership、backend generation、query identity 或 authorization。listener-wide gate 位于 TLS handshake
之后、route/fallback和message decode之前；unknown path及新增 RPC 默认受保护。远端失败统一为低信息量
`Unauthenticated`，role-local log/metric 只使用 bounded failure kind且不得记录credential、token或raw claims。

TLS 是 closed mode：未配置时为 authenticated h2c，`automatic` 或 `pem` 时为 TLS 1.3 + ALPN h2；仍强制 JWT。
TLS v1 不接受 TLS 1.2/h2c fallback、0-RTT、session resumption、client certificate、system roots或mode negotiation。
automatic mode 从 deployment secret和identity派生 deployment-wide Ed25519 key，但按每个精确 advertised IP/DNS
reference生成自签 server leaf；PEM mode只信任 operator 明确 roots。typed endpoint必须保留原始 IP/DNS reference，
DNS resolution不能替换 SAN identity或Channel cache key。

## 接受的妥协（诚实记录）

**默认 plaintext 仍有真实网络风险。** JWT 阻止不知道 shared secret 的普通 caller，却不提供 protobuf body
confidentiality/integrity、server cryptographic identity，且被动监听者可在 token 有效期内重放，主动中间人可修改
body。选择它是为了匹配内部分析引擎的可信网络部署和避免默认强制证书运维，不是因为它与 TLS 等价。需要抵御
监听、重放或主动中间人时，operator 必须选择 automatic 或 PEM TLS。

**shared secret 是粗粒度 deployment authority。** 合法节点泄漏 secret 会影响整个 deployment，DROP BACKEND
不能即时撤销 transport trust；v1 只提供 secret rotation + homogeneous restart，不提供 per-node revocation、
old/new overlap或动态 broker。这以较低实现和运维复杂度换取较弱的撤销能力。

**TLS v1 禁用 resumption 并不提供 mTLS。** 这增加新连接成本，也无法给每个 node 独立证书身份；我们以Channel
reuse降低正常 RPC成本，换取易审计的初始 handshake/state contract，并保持 topology/domain owner不被certificate
挤占。

## 何时重新评估

1. 部署不再能保证可信内网，或安全评审要求payload confidentiality、integrity、replay resistance或server identity
   时，应把 TLS 设为部署策略或默认，而不是在 h2c 上补私有 body MAC。
2. 需要per-node revocation、workload identity、跨集群 federation、dynamic secret/certificate rotation或rolling
   upgrade时，先定义新的 identity/source/compatibility protocol；不得在现有 JWT verifier中偷加 role/backend
   authorization。
3. TLS handshake成本在连接短生命周期工作负载中成为可测瓶颈时，可以评估受约束的resumption；必须先定义ticket
   key、0-RTT policy、replay与cross-role一致性，不能直接打开provider默认值。
4. Native service从单一内部deployment扩展到operator或external caller时，必须单独设计authorization、audit和
   credential delegation；不可复用deployment JWT subject作为权限模型。
