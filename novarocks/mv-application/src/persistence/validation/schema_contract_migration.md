# V3 schema contract 事实分类

本文说明新的定义文档（D）与解释文档（L）为何不把旧
`MvSchemaContract` 作为 opaque payload 持久化。它是实施映射，不是兼容性承诺：
V3 明确不是受支持的输入格式。

| 旧事实 | 分类 | 新来源或重建依据 |
|---|---|---|
| 基表 FQN 与别名 | 解析证据 | D 为每个语法 occurrence 冻结 catalog、namespace、relation 与 qualifier。有效 SQL 只在该冻结上下文中重新解析。 |
| 基表 object ID | 稳定绑定 | D 按 occurrence 保存 provider-opaque object ID。实时重绑定会拒绝名称相同但 object ID 不同的对象。 |
| 基表 schema ID | 稳定版本 | D 按 occurrence 保存 provider-opaque schema version，不与文档 revision 混用。 |
| 完整基表 schema snapshot | 可重算镜像 | D 只保留定义实际绑定的字段；当前名称与未引用字段来自新的 provider observation。 |
| 已绑定 field ID、类型、nullability | 稳定绑定与解释 | D 保存这些事实。重绑定以 field ID 为准，因此 rename/reorder 可以通过，而字段消失或类型/nullability 不兼容会失败。 |
| 输出表达式血缘 | 部分稳定 | D 保存 output identity、类型/nullability、expression kind 及准确的 occurrence/field 引用。AST 与 planner expression 从唯一的有效 SQL 来源重建，并对照这些绑定校验。 |
| filter 与 join 血缘索引 | 可重算索引 | 有效 SQL 与各 occurrence 的稳定绑定可以重建它们；不再持久化第二份 rewritten SQL、AST 或 plan。 |
| 目标可见列名与 ordinal | 可重算展示 | D 拥有有序输出名称。L 将每个稳定 output ID 绑定到 provider field ID；compiler 或 storage ordinal 不承担身份。 |
| 目标表 UUID、schema/spec | 稳定物理绑定 | L 保存 provider-opaque target object、schema 与 partition-spec version；其详细 schema/spec 表示仍以 provider metadata 为权威。 |
| 目标 field ID、类型、nullability | 稳定物理解释 | L 为每个 output 与 state slot 记录一条 canonical physical binding，并拒绝重复的逻辑与物理事实彼此不一致。 |
| 隐藏 apply key | 稳定解释 | L 保存 key algorithm，以及有序、类型安全的逻辑 key ID 与 provider field ID 精确配对；两种 ID 不可互换，隐藏列的拼写不承担身份。 |
| 聚合 state layout | 稳定解释 | L 保存 aggregate identity/function、有序 state-slot ID、slot role、target field binding、类型/nullability，以及显式受支持的 encoding。因此 AVG 必须具有独立的 sum 与 count slot。 |
| UNION branch layout | 稳定解释 | L 保留有序 branch identity 及其 occurrence/output 引用；临时 compiler ordinal 永不作为 branch identity 持久化。 |
| partition transform 与字段名镜像 | Provider-owned 物理镜像 | L 固定准确的 provider partition-spec version。详细 transform 与名称通过该准确版本重新读取，不在应用侧维护第二份权威。 |

发布文档（P）引用准确且不可变的 D/L revision，并携带各实际 occurrence
的 object/data version，以及准确的输出 object/data version。Provider-native
storage row count 留在 provider envelope 或 summary；P 只拥有 logical result
rows 与 processed input rows。配置文档（C）保持独立。因此 refresh 或
repartition 不能用最新 D/L 重新解释旧数据，而调度或暂停变更也不能改变
computation identity。
