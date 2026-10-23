---
id: ADR-0195
title: "Public value algorithms separate decoded computation from storage and caller authority"
domain: [low-cardinality, memory-governance]
status: active
supersedes: []
superseded-by: null
date: 2026-10-23
provenance:
  - "discussion: 2026-10 public value access, row-atomic construction and caller-owned conversion lifecycle"
code-anchors:
  - "novarocks/type-contract/src/value_layout_match.rs (dictionary_value_layout)"
  - "novarocks/value-arrays/src/lib.rs"
  - "novarocks/value-arrays/src/decode/mod.rs (ValueConversionContext, DecodedBytes, PeelResult)"
  - "novarocks/value-arrays/src/build/mod.rs (ValueBuilder)"
  - "novarocks/value-arrays/src/hydrate/mod.rs (HydratePlan)"
  - "novarocks/execution/src/exec/pipeline/driver.rs (PipelineDriver)"
---

## 问题

执行引擎如何共享字符串和字节值的访问、展开与构造算法，同时保持完整类型、原始行坐标、来源寿命和调用方的资源控制？

## 背景与执行事实

[ADR-0005](ADR-0005-low-cardinality-runtime-carrier-first.md) 让自描述字典载体承担值正确性，消费 owner 决定是否展开；[ADR-0176](ADR-0176-recursive-execution-types-and-independent-storage-ownership.md) 将完整递归语义、准确布局、每批编码和来源责任分开。公共值能力继承这些边界，不能由数组载体反签语义，也不把本地快路径写入 FE 的类型身份。

| 实体 | 所有权与用途 |
|---|---|
| 完整 typed binding | 递归领域、NULL、准确布局；由原语义作者提供 |
| 存储数组 | 实际 offsets/view/dictionary、有效位与物理区域 |
| `DecodedBytes` | 普通字节计算的读取接口；底值、逻辑行映射、可读行域分别保留 |
| `SelectionSet` 与行映射 | 前者描述成员集合，后者保留顺序与重复；均不等于物理可访问区间 |
| `ValueConversionContext` | 调用 owner 内的转换复用及效率政策，不是容量 authority |
| `ValueBuilder` | 指定输出布局的行原子构造，支持调用方 allocator、增长拒绝和完整前缀 |
| `HydratePlan` / `CompactPlan` | 先确定表示、实际需求和区域事实，再按冻结计划执行 |
| storage lifetime | 有限来源及创建责任的最后引用保活，不授予后续分配权 |

`novarocks-value-arrays` 只依赖类型、失败合同、Arrow 和 allocator 基础；不依赖 Task、Execution、Connector runtime 或资源 host。Arrow 内部访问集中在私有 `arrow_raw` 适配边界。一般 Arrow 结构/物理几何操作保留既有 Union、run-end 等用途；引擎入口另行验证已冻结的编码集合，不能把 Arrow 支持等同于产品支持。

## 考虑过的选项

**各消费点分别 cast、递归和检查 NULL：设计否决。** 路径会分别决定可达性、表示边界、失败事实及来源持有，产生多个语义作者；共享结构遍历不能修复这些独立决策。

**普通计算直接覆盖所有存储布局：设计否决。** 这要求每个 kernel 同时解释 offsets、view、字典和有效位映射，破坏统一计算接口。优化专用路径仍可显式处理其已声明编码，普通值计算消费解码视图。

**总是完整解码或总是按逻辑行压平：设计否决。** 前者把未引用的大字典强加给稀疏输入，后者丢失原始字典下标底和可复用计算域。转换选择必须同时保留源到目标的表示合法性及原始坐标。

**全局缓存或公共层资源调度器：设计否决。** 进程/查询全局复用会跨越真实来源与串行调用域；让值层决定等待、容量或 Task 终止则越过原 owner。公共算法只报告需求和原失败。

**立即切换全部生产 view 输出：待评估。** 单个公共算法具备能力不代表每个消费边界、结果出口和来源 owner 已完成迁移；默认 producer 的切换应按真实消费者另行验证。

## 裁决

1. **分离存储与计算。** 公共接口以完整 binding 验证语义，公共普通 STRING/BINARY 计算读取解码接口，输出按冻结布局构造。领域 codec 不因载体相同而进入普通字节算法。
2. **唯一字典布局作者。** Type Contract 的 `dictionary_value_layout` 定义 D(L)：Utf8 与 Utf8View 对应 Utf8，LargeUtf8 对应 LargeUtf8；其他布局没有字符串字典值布局。D(L) 是存储事实，不是本地消费能力。已有 Utf8View 字典值原语的读取不改变新输出的 D(L)。
3. **转换保持原坐标。** 整份转换和稀疏填充均使用原始字典下标底；未转换位置不可读取。逐行压平保留实际顺序、重复和 NULL，不参与按字典底身份的结果复用。直接规划源到目标，不能强制经过一个更窄的中间 view 表示而拒绝合法输入。
4. **peel 分离有效性与已计算域。** 键 NULL 不解引用；底值 NULL、映射有效位和结果 `defined_rows` 分开保留。rewrap 不得覆盖新结果 NULL，也不得读取未计算占位；两轮复用只消费覆盖交集，差集必须补算。
5. **效率预算不拒绝输入。** 缓存以实际存储几何、有效位偏移等完整身份命中；预算不足只跳过或淘汰。BE driver 持有一份上下文，在同步调用域短借，退出开始时清缓存，包括 PendingFinish；已发布 alias 继续持有自身来源。FE COW 两条操作分别持有上下文，现有整体预算不含跨记录缓存收据，因此显式禁用复用；操作退出仍释放其临时状态。
6. **行原子与规划先行。** builder 在当前行失败时保留已提交前缀，重试不丢行或重复。hydrate 对所有子路径求一个完整顶层前缀；整批适配器要求完整计划后再执行。compact 只报告复制、输出引用和已知区域，不能报告物理释放，也不裁剪字典。
7. **原 owner 授权。** 值层的 Growth、Work 和结构观察分别交回调用方。小结构请求不能成为 payload、offsets、view descriptors、region table 或 scratch 的累计产品上限。BE 普通增长不新增容量端口；FE 既有 whole-operation preflight 和安全独立复制发布继续由原 owner 负责。不设置任意单值大小上限，真实 offset/extent 极限产生 Representation。
8. **原失败不重分类。** 自有错误由值领域产生完整 fact/reason/layout/extent/完整前缀；Caller 原样传回。Functions、Execution 和 Native 只转发事实并追加准确 site；本地保留 source，wire 只传中立事实与安全诊断，不根据 Display 猜原因。

## 接受的妥协（诚实记录）

解码视图会持有描述符、覆盖位图和来源，稀疏补齐需要在已有 alias 存活时生成新的底；效率政策只估计复用成本，不冒充物理全容量。Unknown 来源仍为 Unknown，共享区间和声明容量不能自动证明最后 owner 已退出或实际释放。

公共能力与生产消费迁移分开交付；当前生产接线收口既有展开边界，表达式等普通 kernel 的后续迁移不能据此视为完成。现有默认 producer、hash 算法/seed/NULL 策略、分布语义及 root 的原输入许可保持；公共 hash/eq 接受显式策略，不成为新的分布策略作者。功能和生命周期测试不证明吞吐、RSS 或完整内存治理达标。

FE 的安全复制发布可能与中间转换同时占用内存，因此仍需既有整体峰值预检；本裁决没有借公共 builder 消除这个发布边界，也没有引入逐 Buffer hold 或通用等待状态。

## 何时重新评估

- 将 view 作为生产默认输出前，逐一验证实际 kernel、exchange、Connector 和结果出口的完整 typed 边界、来源保活与 native 分布式行为；具备原语不构成切换授权。
- 测量确认转换或缓存为主要成本时，可调整 owner 内效率策略；不得把预算不足改成输入失败。跨 owner 复用须先重新裁决来源、并发、淘汰及最后引用责任。
- 新字节表示、编码或领域进入产品集合时，扩展原语及 D(L) 的单一作者，验证真实表示极限、可达域和失败转发；不在 kernel 里增加第二份布局规则。
- 原资源 owner 改变容量或发布合同后，重新核对规划需求、完整前缀与异步等待边界，不能直接让公共值层承担调度。
