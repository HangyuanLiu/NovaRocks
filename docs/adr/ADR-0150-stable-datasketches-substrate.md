---
id: ADR-0150
title: "Pin the probabilistic substrate to an exact stable registry release under a version-agnostic upgrade gate"
domain: [crate-boundary]
status: active
supersedes: [ADR-0134]
superseded-by: null
date: 2026-09-17
provenance:
  - "discussion: 2026-09-17 DataSketches stable release adoption"
  - "PR: <backfill after merge>"
code-anchors:
  - "Cargo.toml ([workspace.dependencies] datasketches)"
  - "novarocks/execution/src/exec/hll.rs (HllHandle allocation admission)"
  - "novarocks/connector/iceberg-functions/src/theta.rs (IcebergThetaAggregateFamily)"
  - "tools/ci/check-datasketches-source.py (resolved graph identity guard)"
---

## 问题

ADR-0134 为预发布阶段裁决了 DataSketches 的依赖方式。上游现已发布正式 `0.5.0`，该裁决自身列出的重新评估
条件已经触发。NovaRocks 应如何表达这条依赖，才能既脱离预发布身份，又不把「当前是哪个版本」写成裁决本身——
否则每次上游发版都要新开一条 ADR，而 ADR 目录会退化成 changelog。

## 背景与执行事实

ADR-0134 关于**为什么**这样依赖的论证全部继续成立，不在此重复：HLL/Theta 的 seed、hash 输入域、状态迁移、
压缩布局、序列化版本与集合运算共同决定估值和 wire 兼容性；复用算法却自行解释二进制格式会形成第二个正确性
owner；Cargo 能强制 crate 边界，却不能仅凭 import 证明最终图中只有一个版本、来源是 crates.io、内容对应预期
checksum。本条裁决继承这些前提，只改变两件事：绑定的是正式版而非预发布版，以及升级门写成版本无关的形式。

正式包与预发布包的关系是本次迁移的关键事实，也是它风险较低的原因：上游把 `0.5.0` 与 `0.5.0-rc.1` 两个 tag
指向同一提交 `77f5652016b3859c23b60c5b8b9e94578ef484f0`，源码树相同，差异限于包版本与打包元数据。但 registry
package 的版本与 checksum 确实变了，而 NovaRocks 的永久守卫、fixture provenance 与本 ADR 都把这些字段当作
权威身份，因此它不是锁文件噪声，必须走完整的升级门。

迁移时实测的证据：以正式包重建 TCK 的 Rust 固定向量，与预发布包生成并已入库的字节逐一相同；Java 6.2.0 与
C++ 生产者的向量一并全量重建比对，共 51 个固定向量全部一致。size-profile 的 56 个分配观测点逐字段相同，
结构性 benchmark 的分配形状在两侧完全一致。这些是「同一提交」这一说法的实测确认，而不是它的替代。

## 考虑过的选项

**A. 继续停留在预发布 `0.5.0-rc.1`。** 不产生任何迁移成本，但让产品长期绑定一个上游随时可能撤回或替换的
预发布身份，也使 ADR-0134 自己写下的重新评估条件形同虚设。否决。

**B. 放宽为 `0.5` 或 `^0.5.0` 之类的 semver 范围。** 省掉未来的手工升级，但把概率结构的格式与估值行为交给
自动漂移决定：一次例行 `cargo update` 就可能改变历史数据的可读性，而编译与普通单元测试都不会察觉。
对可持久化的概率结构而言这是错误的默认，否决。

**C. vendor 正式版源码或建立私有 fork。** 可以完全掌控节奏，但同时取得发布、漏洞响应、格式兼容与长期 rebase
的全部责任，与「不维护第二实现」的目标相反。ADR-0134 已就此详述，结论不变，否决。

**D（选中）. 精确锁定 crates.io 的正式版，并把升级门写成版本无关的政策。** 当前身份是
`datasketches = "=0.5.0"`，checksum `11c0bd7d22989969a619bae09147b992a6aa7f31dd4f9d9af6345f533744781f`；
但裁决的内容是「精确锁定 + 单一解析身份 + 五道升级门」，而不是这两个字面值。

## 裁决

所有产品 consumer 与 NovaRocks 自有工具经 root `[workspace.dependencies]` 精确依赖 crates.io 上的一个
DataSketches 正式版，不使用宽松 semver、Git、path、vendor、patch 或私有 fork。当前实例是 `=0.5.0`，其
registry checksum 为 `11c0bd7d22989969a619bae09147b992a6aa7f31dd4f9d9af6345f533744781f`；版本与 checksum
是**当前事实**，随升级更新，不需要为此另开 ADR。每个 consumer 只启用自身需要的最小 feature。

格式所有权分层不变：上游 DataSketches 是 sketch body、状态机、codec 与集合运算的唯一 owner；NovaRocks 只拥有
产品载体、输入 hash/value domain、admission、内存 reservation 与产品生命周期。Theta partial carrier 固定为
`V2 | lg_k | opaque ordered v3 compact body`。HLL 内存 admission 的 preflight 只解析所锁定版本固定头部中影响
分配的字段，并保留 `lg_k=5` 时 LIST 与 HLL8 dense 的歧义、对 lower-`lg_k` dense merge 取两条分配路径的较大值。
NovaRocks 不解析 body 字段、不复制上游 codec、不以兼容 shim 接受第二种权威表示。

永久守卫检查实际 Cargo 图与 lockfile 而非源码形状：动态发现 workspace，运行 locked Cargo metadata，关联精确
版本、crates.io source 与 checksum，拒绝其它版本、双版本、Git/path/vendor/patch 与 checksum 漂移。守卫必须
按精确版本拒绝已退役的旧身份，而不只是笼统地拒绝「非当前版本」。

**版本变化的升级门（版本无关，每次都要重跑）：**

1. **上游包身份** —— 精确版本、crates.io source 与 checksum 在 metadata 与 lockfile 中一致；
2. **跨语言 TCK** —— Java / C++ / Rust 固定向量的反序列化、集合运算、HLL union 与 downsampling、malformed
   拒绝域；Rust 自身 round-trip 不能替代这组证据；
3. **Fixture 字节等价** —— 用新包重建 Rust 向量并与已入库字节逐字节比较。任何差异都推翻「格式未变」这一
   前提，必须停止升级并回到设计讨论，不得更新 golden；
4. **Consumer 与 allocation** —— Execution 的 HLL consumer、Iceberg Functions 的 Theta consumer，以及
   allocation header parser、状态推导与全局 allocator 峰值边界测试。不得以放宽上界或改 timeout 换取通过；
5. **结构性 benchmark** —— 同 workload、同参数、同 profile、同机器下比较分配形状与时延。分配形状是确定性
   信号，必须逐项相同；时延只在可复现地超出同机噪声带时才算退化。

任一门缺失都不能宣称兼容迁移完成。

## 接受的妥协（诚实记录）

**精确锁定不会自动获得 patch 修复。** 正式版消除了预发布身份的风险，但上游后续的修复不会自动进入，每次都要
主动升级并重跑五道门。对概率结构与持久化兼容而言，这个维护成本优于 semver 自动漂移；但它确实意味着安全
修复的引入速度取决于人是否主动去做。

**registry 与缓存可用性仍是构建条件。** 不保存 vendored 副本意味着首次构建需要从 registry 取得精确包，
离线环境必须预热 Cargo cache。这个供应可用性风险是真实的，选择它是为了避免 NovaRocks 同时成为分发者与补丁
维护者。

**升级门比普通库升级昂贵，而且这个成本不随「源码相同」减免。** 本次迁移中上游两个 tag 指向同一提交，仍然
跑完了全部五道门。这是有意的：如果允许「源码相同就跳过验证」，那么下一次真正有变化的升级也会被同样的理由
说服跳过。

**本次迁移未能取得运行时的跨引擎证据。** Theta 侧唯一的第三方引擎读取验证是 `statistics` 套件中的 Spark 与
Trino 用例，它们在迁移时于 main 上已经失败（同一失败在正式版与预发布版两侧逐用例相同，且同批失败中包含不走
Theta 的 min/max 用例，表明根因在 ANALYZE 作业机制）。因此本次只取得了静态的跨语言格式证据——Java 与 C++
生产的固定向量在新包下全量重建一致——而没有取得第三方引擎运行时读取 Puffin 的证据。接受这个缺口是因为
格式契约由固定向量覆盖，但必须明确：它不等于跨引擎运行时已验证，`statistics` 套件恢复后应补跑。

**最小 feature 是逐 consumer 的显式维护负担。** 新调用能力不会自动可用，manifest 必须随真实需求调整。

## 何时重新评估

- 上游发布新的正式版本，且它能在保持标准 body 兼容的前提下通过全部五道升级门；
- 当前版本出现 CVE、供应链撤包、长期无人维护或无法获得必要安全修复；
- 相同 workload、参数与构建 profile 下出现可复现的结构性 benchmark 退化，而非单次时延噪声；
- DataSketches 引入新的格式 major version、seed/hash domain 变化或集合语义变化，使现有 Java/C++/Rust 向量
  不再代表目标互操作契约；
- DataSketches 改变 HLL list/set/dense、HLL4 aux map、decode 或 union transition 的分配形状；升级前必须重审
  allocation header parser、状态推导与全局 allocator 峰值测试；
- 上游出现由官方维护的 Rust Alpha 路线，且 NovaRocks 有当前实现无法满足的真实产品需求；届时比较 upstream
  贡献、官方 Alpha 与独立实现，而不是默认建立私有 fork。
