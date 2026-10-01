# P04 metadata 来源生产传播

Native output layout 在构造前检查整张有限字段树，再用 fresh immutable map builder 为实际 root/nested Field Arc 颁发 metadata 收据。ChunkSlotSchema/ChunkSchema 与生产 ExecPlan lowering 把准确来源带入 LocalProgram；投影共享真实 Field Arc，只为新 Schema 的已知 map 派生收据。nullable / dictionary carrier 协调直接从已知 source owner 派生所有变化的子字段，未变化字段保留 Arc；没有先 clone 未知 map 的临时 Field。

FieldMetadataOrigins 是过程内 immutable sorted index，不改变 wire、语义 authority、钱包或 MEM grant。查询实际 Arc identity；结构相等、Field clone、Arc::make_mut、裸 Field/Schema 不继承证明。有限树收窄分别限制 owner 数与 DataType 访问次数；Dictionary 只有一份 Field 不能绕开 work 上限，Map entries 物理 Struct 不额外消耗语义深度。每 slot 只保留可达来源，257 个字段重建不会制造 66,049 个重复 index 项。

定向结果：Types 实际 allocation/identity/derivation 11；Native codec 3；LocalProgram layout 7；Chunk schema 17；真实 Native decode → Values ExecPlan → production lowering / projection 集成 3；Native adapter 旧错误路径 4；原 carrier 2 / storage 17，全部通过。Types strict Clippy -D warnings、Native all-target check/Clippy 通过（依赖与既有 warning 保留）。第一次把 Types+LocalProgram 一起设 -D warnings 被未改 Functions 的两项 doc_lazy_continuation 拒绝；原日志保留，随后 Types 单独 strict 通过，Native all-target Clippy 包含 LocalProgram 正常通过。fmt/diff 检查通过，readonly agent 复核前报的 child receipt 与 Dictionary work 缺口均已覆盖。

这里只证明被附着的 metadata maps 及其来源传播；name、Field/Schema/Arc/Fields/DataType/index/scaffolds、实际 RecordBatch/Array 的独立来源与全 Chunk backing 仍是完整 input proof 的独立义务。来源未知 map 不迭代、不 canonicalize；expected 来源不能推断 actual carrier 来源。Native producer、finite pool、Root read endpoint 和完整产品接线仍未完成，不 advertise V1，P04 继续 executing。Linux 测试按用户授权由用户后续手动执行。
