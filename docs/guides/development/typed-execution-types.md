# 执行类型、来源投影与构造责任

长期裁决见 [ADR-0176](../../adr/ADR-0176-recursive-execution-types-and-independent-storage-ownership.md)。本指南说明调用方如何选择已有类型 API；功能验收与性能结论由实际验证材料提供。

## 先选择事实作者

| 调用目的 | 输入与 API | 必须保留的事实 |
|---|---|---|
| 声明或比较逻辑类型 | `ValueTypeRef`、`ValueField`、`ValueKind` | 完整递归 NULL、领域、Struct/Union 名字与顺序 |
| 比较准确执行绑定 | `TypedValueBinding`；函数使用 `FunctionValueType` | 语义与 `ValueLayout`，包括 offset 宽度 |
| 声明有序语义字段 | `ValueSchema` | occurrence 位置与完整语义；本体不包含物理布局 |
| 绑定有序批或程序输出 | `ChunkSchema`/`ChunkSlotSchema`、`StaticLayout` | 完整 typed binding 与准确值布局，由实际输出作者保留 |
| 验证实际编码 | 真实 Arrow 数组和所属消费边界 | 字典/其他合法包装、索引、buffer、offset、NULL |
| 创建来源或 target 投影 | 来源字段 owner、`DecodedSourceFieldTable` 及 Connector target schema | provider ID/default/lineage、来源名字和 metadata |

`FunctionValueType::new` 声明普通 Physical 值，不能用于恢复嵌套领域。完整语义已经存在时，传递 `TypedValueBinding` 或完整 `ValueTypeRef`；领域作者使用相应完整声明入口。不能先构造普通 Binary，再从 metadata 或函数名字猜 bitmap、JSON 或状态格式。

逻辑等义与物理完全相等是不同问题。以 `TypedValueBinding::same_semantics` 或其借用 `value_type` 判断逻辑语义；执行绑定的 equality 同时包含 layout。不能因为 Utf8/LargeUtf8 的值语义相同就换掉准确 offset 布局。独立构造的等义 schema 可以绑定；共享指针只作为已验证不可变对象的快速路径。

## 构造与持有分开

纯 FE 入口保持已有结构限额、取消与 deadline 控制，不提供 memory creator。调用方选择纯入口时传 `None`，不能创建无限配额、成功 no-op scope 或 FE query lease 来满足类型签名。

BE 已有 owner 提供 `ValueSchemaCreationFactory<H>`。一次实体或操作通过 `begin_scope` 获得 `ValueSchemaCreationScope<H>`；该 scope 在作者分配前处理 `request_layout` 或有根据的 `observe_opaque_upper_bound`。需要观察 scope 自身 holder 时使用 `begin_scope_observed`，其观察错误和 creator 错误仍是两种原始类型。第三方内部对象的有界估计遵循 [ADR-0170](../../adr/ADR-0170-third-party-crates-are-bounded-by-public-configuration-not-forked.md)，不另建容器布局副本。

`storage_lifetime` 是小型有限责任保活，不能授权后续 allocation。独立 schema、field、child、FVT 或 Arrow 投影引用必须保留实际 backing 所需的责任；不持有整个 query、factory 或无关主账。持有对象按原 owner 回收，不能在 terminal/cancel 发布时提前归还。临时 expected 类型和只用于比较的拥有对象优先改为借用原完整声明；确实创建临时结构时使用临时 scope，不能让准备产物保留它。

cheap clone 只保留已有不可变对象，不再次收取共享 payload；新 Field/name/container 或 projection holder 仍属于实际新构造。容量、请求责任、allocator/cgroup/RSS 分别观察，不能相加或互相冒充。详见 [Memory 核心边界](memory-boundary.md)。

## 函数冻结与准备

冻结的 selected binding 必须由原 installed declaration/resolver 依据原完整请求验证，不能把 selected 结果当作 expected 的来源。`EngineFunctionCatalog::validate_frozen_selection_with_factory_in` 在原 catalog/selection/request 校验后开启实际 resolver 构造 scope；纯入口继续使用同一个校验 body。

`PureEngineFunctionCatalog::prepare_frozen_with_creator_in` 接收实际 source `ConstantPolicy` 和独立准备 scope。`CheckedCallPreparationFailure<H>` 保留原 creator `H`、资源分类和原准备失败；creator/control 首因后不执行完成回调，不根据错误文本映射或退回 fresh resolution。未转换的 owner 明确拒绝 checked 路径，不能暗中调用纯路径。

writer 的 scalar input 是原物化 Value context，不是从中间状态重建的逻辑请求。使用 `prepare_frozen_writer_aggregate_with_creator_in`；原调用方已经创建空 order owner 时使用 `prepare_frozen_writer_aggregate_with_empty_order_keys_and_creator_in`。Partial/Final 的原 phase、完整逻辑类型、state format、NULL covariance 和 namespace 校验仍由原准备 body 完成。

## 接到真实边界

执行 Arrow schema/Field 的 metadata 容量为零。来源 metadata 只在来源 owner 内投影；写入从实际 target facts 取得 ID/default/lineage，不向执行字段写回业务属性。

exchange 核对冻结语义、准确值布局和本批真实编码，保留合法嵌套字典；接收端绑定不等于展开。缺少直接消费能力的真实下游在原资源许可内适配。root 根据具体用途核对完整声明，SQL 公开 NULL 保证由实际表达式和错误策略 author 发布。

改动类型作者、保活或消费接缝时，优先验证原 Host/Control 首因、失败不发布、最后独立引用退出、完整领域和 child NULL、metadata 空容量及实际 buffer 共享。结果变化用所属 SQL suite 验证；跨进程冻结与安装/退出由现有 1FE+3BE fixture/cluster owner 验证，不在黑盒 runner 中复制 Execution 作者或建立新测试产品功能。
