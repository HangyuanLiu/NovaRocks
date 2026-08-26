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

# Native trust、JWT 与可选 TLS

本指南定义 NovaRocks Native RPC 的部署边界。它适用于所有 deployable `fe` 与 `be`
进程，以及 all-in-one 中的两份正常 role config。设计裁决见
[ADR-0110](../../adr/ADR-0110-native-trust-authenticated-plaintext-and-optional-tls.md)。

## 必须配置的 deployment trust

所有 FE→BE、BE→BE、BE→FE Native RPC 都要求 deployment JWT。每份 deployable config
必须包含以下内容，且整个 deployment 的值必须完全一致：

```toml
[native_trust]
deployment_id = "analytics-prod"
shared_secret = "${ENV:NOVAROCKS_NATIVE_SHARED_SECRET}"
```

`deployment_id` 是公开的 stable trust-domain identifier，不是 StateStore `cluster_id`、
backend identity、generation、role 或 query identity。它只能使用 1–64 bytes 的
`[a-z0-9](?:[a-z0-9._-]{0,62}[a-z0-9])?`，不会自动 trim/lowercase。shared secret 是
32–4096 bytes 的 ASCII graphic secret，推荐为每个 deployment 使用：

```bash
openssl rand -base64 32
```

将生成结果安全地 provision 到每台 role 主机的 `NOVAROCKS_NATIVE_SHARED_SECRET`，而不是
写入版本库、TOML 或日志。Server 只接受精确 `${ENV:VAR}` 引用并在启动时解析；missing、
empty、malformed 或非 UTF-8 的值都会在 listener bind 之前失败，错误和日志不得回显 secret。

JWT 只证明 caller 知道该 deployment secret。它不是 SQL authorization、backend membership、
topology generation、query identity 或 protobuf message MAC；终止或移除一个 Backend 也不会撤销已泄露的
deployment secret。

## 传输模式

TLS 是可选的额外传输安全层，JWT 在三种模式下都强制存在。整个 deployment 只能选择一个
mode；系统不做 mode negotiation、h2c fallback 或 TLS 1.2 fallback。

| mode | 配置 | Native transport | 适用边界 |
| --- | --- | --- | --- |
| `disabled` | 省略 `[native_trust.transport]` 或显式 `mode = "disabled"` | authenticated plaintext h2c | 仅限可信内网 |
| `automatic` | 下方 automatic 示例 | TLS 1.3 + ALPN `h2` | 需要自动派生的 server identity |
| `pem` | 下方 PEM 示例 | TLS 1.3 + ALPN `h2` | 使用 operator/企业 PKI |

### 默认：authenticated plaintext h2c

未配置 transport section 的默认模式是 `disabled`。它仍拒绝没有有效 JWT 的 Native RPC，适合
内部可信网络的低运维部署；但它**不提供** body/query/result 的保密性、gRPC body integrity、
server cryptographic identity、监听后的 bearer-token replay 防护或主动中间人修改防护。JWT
签署 claims，而不是 protobuf body。存在被动监听者、重放风险或 active on-path attacker 时必须启用
TLS，不能通过给 plaintext RPC 补 private request MAC 来替代。

### automatic TLS

automatic mode 从同一 deployment id 与 shared secret 派生 deployment-wide Ed25519 identity，
并为每个 role 的精确 advertised endpoint 生成自己的 self-signed server leaf：

```toml
[native_trust]
deployment_id = "analytics-prod"
shared_secret = "${ENV:NOVAROCKS_NATIVE_SHARED_SECRET}"

[native_trust.transport]
mode = "automatic"
```

不需要 operator 提供 PEM。自动证书只覆盖精确 advertised IP SAN 或 canonical DNS SAN；不要把
wildcard/unspecified advertise address 或 load-balanced service DNS 当作一个 backend identity。DNS
reference 以 lowercase ASCII A-label/punycode 比较，不能用 U-label、trailing dot 或 wildcard。

### PEM TLS

PEM mode 使用 operator 显式提供的 server chain、private key 与 outbound trust roots。三个 path
都必须存在；没有 system-root fallback、inline PEM、combined-file guessing、hot reload 或 password
prompt：

```toml
[native_trust]
deployment_id = "analytics-prod"
shared_secret = "${ENV:NOVAROCKS_NATIVE_SHARED_SECRET}"

[native_trust.transport]
mode = "pem"
certificate_chain_path = "/run/novarocks/tls/server-chain.pem"
private_key_path = "/run/novarocks/tls/server-key.pem"
trust_roots_path = "/run/novarocks/tls/native-roots.pem"
```

每个 role 可有不同 server leaf/key，但各 role 的 roots 必须让对端的 exact reference-host
SAN 通过验证。PEM certificate CN 不必等于 deployment id；JWT 仍负责 deployment caller
authentication。使用受保护 mount 与正确 file permissions 是 operator 的责任。

automatic 与 PEM 都固定为 TLS 1.3 和 `h2`：无 TLS 1.2、h2c、0-RTT、session resumption、mTLS 或
client certificate；每条新连接验证 server certificate，每个 RPC 仍独立携带和验证 JWT。

## 安装、迁移与轮换 runbook

首次为一个已有 deployment 启用 NWT-3 时，不能逐节点混用旧 no-auth 与新 Native trust：新配置在
启动前必须已经存在于所有 role。推荐维护窗口流程：

1. 选定 `deployment_id`，生成一个 deployment-unique shared secret，并安全 provision 到每台 FE/BE。
2. 用同一 `[native_trust]` 更新所有 role config；先不写 transport table 即为 trusted-network h2c，或为
   所有 role 一起选择 `automatic`/`pem`。
3. 若使用 TLS，核对每个 BE `advertise_host` 和 FE/BE peer endpoint 是精确 IP/DNS reference；automatic
   必须能生成该 SAN，PEM 的 explicit roots 必须验证它。
4. 停止所有 Native role，替换配置和受保护的 secret/PEM material，然后启动全部 BE，最后启动 FE。
5. 检查每个 role 的 `native_trust_ready` startup log（可出现 deployment id、transport mode，绝不能
   出现 secret 或 token），执行 `SHOW BACKENDS` 和一条真实分布式查询。

secret、deployment id、transport mode 或 PEM material 的变更都是 **restart-only**。当前版本没有
secret/certificate hot reload、old/new secret overlap、rolling upgrade 或 per-node transport revocation。轮换时
必须在维护窗口停止全部 FE/BE、把新材料同时部署、再 homogeneous restart；如果任一旧 role 仍在运行，连接会
按 JWT authentication 或 TLS handshake fail-close。不能通过暂时移除 trust、切换到另一个 mode 或接受旧 secret
来“平滑”轮换。

## 运维排障

| 症状 | 检查与处理 |
| --- | --- |
| startup 在 bind 前拒绝 native trust | 检查 `[native_trust]` 是否完整、deployment id/secret 是否满足格式、exact ENV 是否存在且非空；不要把 secret 输出到终端。 |
| all-in-one preflight mismatch | FE/BE deployment id、resolved secret bytes 和 transport mode 必须一致；修复两份 config 后整体重启。 |
| `Unauthenticated` Native RPC | 检查所有 role 是否使用同一 deployment secret、时钟是否合理、没有重复/格式错误 authorization metadata；不要以 subject 做 membership 或 authorization 判断。 |
| automatic TLS handshake failure | 检查 advertise/peer endpoint 不是 wildcard，IP/DNS reference 与 automatic SAN 精确匹配，且所有 role 都为 `automatic`。 |
| PEM TLS handshake failure | 检查 cert/key match、chain、显式 roots、serverAuth EKU、有效期与 exact IP/DNS SAN；不要依赖系统 roots 或 CN fallback。 |
| 想立即拒绝已移除/泄露的节点 | Backend drain/终止只改变 future admission。轮换 deployment secret 并 homogeneous restart 才是当前完整 transport-trust revoke。 |

NWT-3 只保护 Native gRPC surface。MySQL SQL entrypoint、management HTTP、metrics 和现有 debug route
不读取这个 JWT/TLS config；它们需要各自的网络隔离、proxy 或后续安全设计。不要把 Native JWT 的存在解释为
全产品 HTTPS、端到端 authorization 或 protocol slimming；后续 NWT-4 仍是独立工作。
