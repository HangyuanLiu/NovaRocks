---
id: ADR-0056
title: "Test assertion ownership when neutralizing Core provider consumption"
domain: [connector]
status: active
supersedes: []
superseded-by: null
date: 2026-08-12
provenance:
  - "PR: non-MV Iceberg test scaffold retirement and neutral DTO consumption closeout"
  - "discussion: 2026-08-11 Iceberg provider owner cut 子任务重新切分"
code-anchors:
  - "novarocks/core/src/connector/scan_model/mod.rs (planned_files_fixture_binding)"
  - "novarocks/core/src/connector/iceberg/file_pruning.rs (file_may_satisfy_physical_predicates)"
---

## 问题

把 Core 的测试脚手架从某个 provider crate 上摘下来时，一个测试如果无法用冻结的 SPI facts 表达它的断言，
应该怎么处置？

## 背景与执行事实

Core 曾经只有一个 fixture connector，它位于 Core legacy 的 Iceberg 实现内部
（`connector::iceberg::provider::register_planned_files_fixture`），并在 `plan_splits` 里调用 Core 自有的
`file_pruning::file_may_satisfy_physical_predicates`。因此所有经它构造输入的 Core 测试都同时继承了两件事：
provider 的 DTO 词汇，和 provider 的裁剪语义。

摘除时暴露出三个客观事实：

1. **SPI 刻意不建模某些维度。** 冻结的 `novarocks_spi::connector` 契约里没有「带统计信息的数据文件」，
   也没有字段 ID 概念——`ConnectorTableMetadata` 只有 Arrow `SchemaRef` 与 planning facts，Arrow field 有
   名字、类型、可空性和自由字符串 metadata，但没有稳定数字标识。opaque split 的全部意义就是让 Core 无法
   解释这些事实。所以「谓词 X 是否裁掉文件 Y」这类断言在中立面天然不可表达，不是工具缺陷。
2. **能力可能只存在于一侧。** Core legacy 有 701 行的 Iceberg 文件裁剪实现（min/max 统计、分区值、离散集、
   多种区间），provider crate 则完全没有谓词裁剪（`ConnectorSplitPlanningMetrics::candidate_units_pruned`
   三处均为字面量 `0`）。「迁到 provider」在当时无处可迁。
3. **部分测试挂错了地方。** 有测试位于某文件的 `mod tests` 内，函数体却整个在调用另一个文件的函数；
   也有测试通过一个下划线前缀的死参数接收 provider DTO，其精心构造的输入从未参与任何断言。

## 考虑过的选项

- **A：在中立 Core 测试代码里复刻 provider 语义**（让中立 fixture 也做裁剪）。断言得以留在原地，
  但会把 Iceberg 裁剪逻辑复制进 Core，并且断言退化为「测试 fixture 自己」——被测对象消失了。
- **B：一律删除，以「provider 侧应该覆盖了」为由**。代价是不可逆的覆盖损失，且当能力只存在于 Core 一侧时
  （事实 2），根本不存在可具名的等价覆盖。
- **C：为不可表达的维度扩充 SPI**（把统计边界、字段 ID 提升为冻结 facts）。会为了测试便利撕开契约面，
  与 opaque split 的设计意图直接冲突。
- **D：按「断言主体的所有者」归位**——测试搬到它真正驱动的那段实现旁边，随该实现一起被后续所有权切换处置。

## 裁决

采用 **D**，并配套两条约束：

1. **中立 fixture connector 刻意不实现 provider 语义。** Core 的中立 read fixture
   （`connector::scan_model`）无条件为每个输入单元产出一个 split，`candidate_units_pruned` 恒为 `0`，
   不调用任何裁剪或 provider 侧校验。它只用冻结 SPI facts 与 Arrow 表达，并保证 opaque payload 逐字往返。
   断言「Core 不重解释 provider 事实」应通过这条往返表达——它比「解回 provider DTO 再断言」更强，
   因为它不给 Core 任何解释余地。
2. **归位不等于中立化，必须分开陈述。** 归位后的测试仍然引用 provider crate；它只是移动到了正确的所有者
   身旁，等待该所有者被整体迁移或删除。任何声称「已中立化」的说法都必须排除这些文件。

判据顺序：断言 Core 中立行为 → 留在 Core 用中立 fixture；断言 provider/外部系统语义 → 归位到该实现旁；
已被具名用例等价覆盖 → 删除；其余一律不得以「难迁」为由原地保留。

## 接受的妥协（诚实记录）

- **归位不减少 Cargo 依赖。** 被归位的测试仍然引用 provider crate，只是集中到了本就会被整体处置的
  legacy 子树内。选择 D 的真实理由是「保住覆盖 + 不复制语义」，**不是**因为它推进了依赖切断——
  它对最终的 crate cut 没有任何直接贡献。用它衡量「去 provider 化进度」会得到虚高的数字。
- **共享执行底座可能完全无法处置。** Core 的 in-process 测试 harness 硬编码了一个 provider execution
  installer；清空它会让 60 个测试失败，其中 59 个属于其它未完成子任务的范围。本次接受了「13 个消费文件中
  12 个收口、第 1 个连同证据移交下游」的结果。这类共享底座只能在它的消费者全部中立化之后才动，
  这是排期约束，不是技术选择。
- **归位会暴露能力缺口，但不负责补。** 把裁剪断言归位到 Core legacy 的实现旁，等于把「provider 侧缺少
  谓词裁剪」这件事变成显式待办：一旦 owner 切换到 provider，谓词级 data-file pruning 会静默消失
  （fail-open，无任何测试能抓到）。归位动作本身只提供 parity 基线，不修复缺口。
- **中立 fixture 不裁剪，意味着它无法服务于任何裁剪相关的 Core 侧断言。** 依赖裁剪结果收缩输入集的测试
  必须改为直接喂入收缩后的集合。这保持了断言语义，但让「输入为什么是这个形状」不再自明，需要注释支撑。

## 何时重新评估

- **SPI 开始建模统计或字段身份时**：若未来某个冻结 fact 能无损承载列统计边界或稳定字段 ID，
  第 1 类与第 2 类的分界线会移动，现在归位出去的一部分断言可以回到中立面。届时应重新审视本 ADR 的判据顺序。
- **provider crate 补齐谓词裁剪后**：`candidate_units_pruned` 不再恒为 `0` 时，裁剪断言的正确归属从
  Core legacy 变为 provider conformance，需要把 parity 基线迁过去并更新本 ADR。
- **共享测试 harness 的消费者全部中立化后**：届时 harness 可以去 provider 化，「12/13」这类部分收口的
  表述应当消失；若那时仍需保留某个 provider installer，说明本 ADR 低估了执行底座的耦合，需要重新裁决。
- **若归位成为常态而非例外**：当某个 arc 中归位数量显著超过真正中立化的数量时，说明契约面切得不对——
  应该回到 SPI 设计而不是继续搬运测试。
