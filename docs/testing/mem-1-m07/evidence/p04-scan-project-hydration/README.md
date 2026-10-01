# P04 scan、Project、hydration 来源传播

Native scan 在筛列与复制 wire column 前检查完整字段树及 required names；owned decoder 的准确 Field Arc/maps 直接进入 output_schema。普通 Connector stream 已经通过 SourcePageConverter::with_output_schema 使用该 schema；synthetic/materialization 支线仍另需闭合。

Native Project 在复制描述和创建字段前按 borrowed project occurrences 检查整树；计算 slot 与最终 occurrence 保留各自准确名字/nullability/logical facts。ProjectExpressionSlot 用 FieldRef 与可选来源，ExecPlan lowering 和生产 builder 不再深复制字段。运行时 CSE 替换/扩列、final output、empty output 从声明或实际 source slot 派生；未变化字段共享 actual Arc，metadata 和 slot unique ID 保留，不再先创建裸临时 Field/Vec。顶层 schema 从已知 owner 派生，Chunk columns/batch alignment 保留这个准确 map。

dictionary hydration 在实际 Arrow cast 前先派生目标 slot；已知 attached metadata map 独立复制并获得新 field owner。只有原 schema receipt 对应实际 input schema Arc 时派生 top map；结构相等的外来大 table 仍 unknown，不能用 declared 元数据推出 actual 来源。这里没有宣称 generic cast 已被有限 producer 接管或已证明其峰值。

Readonly审查找到并修复两类反例：alignment 原本会清空刚派生的非空 schema map；generic Project 的 source receipt 不能改变 intentional dtype overwrite 或 synthetic nested semantics。compatible known carrier 使用递归来源派生；合法任意 dtype replacement 引入未知 child 时保留新 Field Arc/既有结果、标为 unknown。synthetic nested child 不据 fresh root map获证明。原未知来源生产路径仍未向 Root 颁发可编码证明。

验证：Chunk67、Project10（包含 known/unknown Int32→Int64覆盖followup相同11/12结果、synthetic Struct unknown child实际7、重复occurrence实际字段/共享buffer/empty输出）、真实Native integration5（scan/Project/CSE/lowering）、Native node12、LocalProgram28、lowering4、codec3、storage19、original carrier2通过。Native all-target check/Clippy、fmt/diff通过，依赖既有warning保留。一个新增测试最初误用不存在的Chunk::slot_ids API，改为chunk_schema().slot_ids并复跑通过；这不是产品失败。两个readonly审查反例都有定向可失败测试。

此检查点仅推进 metadata来源传播，不是完整 name/DataType/Arc/Fields/ChunkSchema/scaffold/accounting/原Arrow backing proof 或 source growth grant。实际 RecordBatch/Array origins、exchange、special source/collector，以及 Native finite producer/read ingress和三类用途完整接线继续；P04 executing，不advertise V1，不称候选1FE+3BE或性能验收，无push/PR。Linux由用户后续手动执行。
