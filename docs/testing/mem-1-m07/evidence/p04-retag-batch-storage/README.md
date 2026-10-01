# P04 retag身份与整批storage工作界

Arrow nested retag 在准确 target 字段的 type/nullability 未变时直接共享其 Field Arc；只对真正变化字段保留原 generic 行为。这样真实 Chunk::try_new_with_columns 建出的 Struct array 指向已知 target child metadata owner，而不是生成结构相等但无来源的新 map。nullable/null-count 与值buffer规则保持；变化/未知字段没有推断收据。

新增 borrowed_root_batch_storage 对整张 RecordBatch 原始 columns Vec capacity、标准 array/object/null/buffer 全 backing 使用同一 work/byte budget，避免逐列重置 work 限额。宽输入先拒绝；alias可重复保守计费，不建identity集合。它仍只涵盖storage，不涵盖schema/Field/DataType metadata、ChunkSchema/所有source scaffolds，不构成完整输入证明或钱包授权。

Chunk 相关66测试、真实storage容量/零分配19 probes、原carrier2通过；Execution all-target Clippy通过，既有warning保留。新probe实际8192个ArrayRef spare backing在byte界前拒绝，2列共享array在nodes=1时拒绝且nodes=2成功，4097列在检查列前拒绝，所有borrowed success/error路径零分配。生产 retag probe检查真实Array.data_type子字段与准确 target Arc相同，外来原field仍来源未知。

Native producer/完整input source proof继续；不advertise Root V1，不声称MEM账本或产品验收。
