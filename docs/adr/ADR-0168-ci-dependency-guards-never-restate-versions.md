---
id: ADR-0168
title: "Keep dependency versions under Cargo authority and guard identity without restating releases"
domain: [crate-boundary]
status: active
supersedes: []
superseded-by: null
partially-supersedes: [ADR-0150]
date: 2026-10-09
provenance:
  - "discussion: 2026-10-09 CI dependency version authority"
code-anchors:
  - "Cargo.toml ([workspace.dependencies])"
  - "tools/ci/check-datasketches-source.py (expected_version, verify_workspace)"
  - "tools/ci/check-physical-plan-dependency-boundary.py (Graph.external_packages, verify_external_identity_uniqueness)"
  - "tools/ci/tests/datasketches-source-test.sh"
  - "tools/ci/tests/physical-plan-dependency-boundary-test.sh"
---

## 问题

依赖升级时，CI 如何继续保护来源、解析身份与 crate 边界，同时让版本及包内容只有一处权威，避免守卫成为第二份版本清单？

## 背景与执行事实

ADR-0142 让 NovaRocks 自有 package 共享根 workspace、resolver 与 lock authority。根 manifest 表达共同依赖要求，Cargo.lock 固化解析结果与 registry checksum；member manifest 表达消费关系与 feature。CI 使用 locked Cargo metadata 和 package-selected dependency tree 检查实际图。

| 事实 | 权威 | 守卫承担的规则 |
| --- | --- | --- |
| 依赖版本要求 | 根 manifest | 要求形式是否合法，实际解析版本是否满足规定的一致性 |
| 解析版本、来源与 registry checksum | Cargo.lock 与 Cargo metadata | 两者一致，同名身份数量和跨 workspace 内容一致 |
| 物理计划依赖闭包 | 所选 package 的 Cargo tree | 名称白名单、真实 package 身份和封闭依赖面 |
| 某版本的行为与格式兼容性 | 升级验收证据 | 格式、consumer、allocation 与性能门，不从版本号推断 |

DataSketches 的精确正式版要求与五道升级门由 ADR-0150 裁决。物理计划守卫允许少量中立类型依赖；同名的 path、Git 或 patch package 不能冒充 crates.io 包。单靠 package 名称无法证明这些边界，单靠 workspace-wide metadata 也无法区分其它 consumer 启用的 feature 与所选闭包。

## 考虑过的选项

**A. 在守卫再次保存精确版本和 checksum。设计否决。** 这让守卫独立表达发布身份，每次升级必须同时修改 manifest、lock 与校验预期，失去单一权威。审查者会把机械同步当作正常升级步骤，守卫中的字面值并不能独立证明兼容性。

**B. 只检查 package 名称。设计否决。** 不重复版本，但允许同名的不同来源、双版本或本地替身通过，丢失真实身份与依赖隔离约束。

**C（选中）. 从 Cargo 权威派生值，守卫固定规则。** 根 manifest 决定要求，metadata 与 lock 决定当前身份；守卫固定来源、要求形式、唯一性、闭包和一致性规则。版本变化无需改校验代码，保护规则变化仍需审查代码与变异测试。

## 裁决

**版本权威规则。** 永久依赖守卫不保存当前 registry release 的版本或 checksum 字面值。它们可以固定允许的 package 名称与来源、精确正式版要求的语法、依赖边界和身份数量。DataSketches 根要求必须是 `=X.Y.Z` 的正式版；不接受范围、预发布或缺失声明。

**DataSketches 身份规则。** 动态发现每个 NovaRocks workspace，在 locked metadata 与 lock 中分别最多出现一个 DataSketches 身份，两侧必须同时存在或同时缺失。出现时版本等于根精确要求，source 是 crates.io，lock checksum 存在；metadata 若提供 checksum，必须等于 lock。所有包含它的 workspace 必须使用相同 checksum，且至少一个 workspace 实际解析它。Git、path、vendor 或 patch 不能以同名包代替这个身份。

**物理计划身份规则。** 外部白名单保存 package 名称和 crates.io source，允许身份由包自身版本派生，同时校验 Cargo ID、source 和存在的 `registry/src/<registry>/<name>-<version>/Cargo.toml` 解压路径。对所选物理计划闭包，每个允许外部名称最多出现一个身份；移除依赖合法，同一身份被多次遍历合法，同名不同版本或来源不合法。直接依赖、传递闭包、feature、optional、target、build/dev dependency 和 build script 的原有边界检查继续有效。

**测试权威规则。** 需要真实 registry package 的变异 fixture，从生产所选闭包读取版本，无默认值，使用精确要求避免离线解析漂移。身份唯一性与要求语法使用合成数据即可，测试中的合成版本与 checksum 不承担产品版本权威。

**部分替代范围。** 本条只替代 ADR-0150 中守卫独立保存当前版本/checksum、按写死的退役版本字面值拒绝旧身份的要求。旧身份通过与根正式版要求不一致而被拒绝。ADR-0150 的精确正式版锁定、单一 crates.io 身份、上游格式所有权、最小 feature 与五道升级门全部继续有效；本条不减免任何 DataSketches 升级验收。

## 接受的妥协（诚实记录）

**一致性不能阻止同步降级或证明兼容性。** manifest 与 lock 同步改成另一个正式版会通过身份守卫，包括较旧的正式版。是否允许该变化由代码审查与对应升级门决定；守卫不把版本大小当作格式与行为证明。

**供应链内容验证依赖 Cargo 与 registry。** lock checksum 是当前内容权威；metadata 不提供 checksum 时，守卫无法自行获取一个独立摘要，只能要求 lock 中存在且各图一致。包内容校验仍由 Cargo 负责。这里不维护第二份离线 checksum 清单。

**Registry 路径是外部身份验证的一部分。** 物理计划守卫依赖 Cargo 标准解压结构，但不固定 Cargo home 或 registry 目录的机器专属前缀。Cargo 改变布局时需要调整身份规则，不能降级为只认 package 名称。

**真实 fixture 要求缓存具备当前依赖。** 生产 lock 更新后，离线变异测试需要新包已经缓存；缺失应明确失败，不能借用旧版本或猜默认版本。

## 何时重新评估

- 外部审计明确要求独立于项目 manifest/lock 的发布 allow-list；届时比较受审计清单与当前单一权威，而不在脚本中零散恢复常量。
- Cargo metadata 的 package ID 或 registry 解压布局改变，现有来源证明不能继续成立。
- 项目引入合法的镜像 registry 或多 workspace 独立版本政策，使 crates.io 单一来源或跨图内容一致性不再是目标。
- 某边界明确需要同名多个版本，且已有设计证明不产生第二种权威表示；届时重新裁决闭包唯一性，不以放宽守卫临时通过。
