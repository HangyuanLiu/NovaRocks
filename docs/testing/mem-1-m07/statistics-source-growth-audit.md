# P04 Statistics 源头增长接缝

2026-10-02 在 `f65f83969eed245f8fb91a2700749057496e1f78` 基础上只读核验；本文件记录未闭合的实施入口，不是源头容量或产品验收收据。STA1 codec 的独立证明见 [收据](evidence/p04-statistics-codec/README.md)。

## 当前代码事实

- `novarocks/sql/src/planning/dml.rs:2494` 先按 body/input_fields/blob_type/properties 创建 Unpivot，再用末尾 Project 重排为领域四列。Project 只重排，但 Unpivot 的增长发生在前一条 edge。
- `novarocks/execution/src/exec/pipeline/driver.rs:1573` 只向直接 downstream 取得 pre-pull coverage。当前 Unpivot→Project edge 没有取得 RootInputPermit；后续 Project→Root 的 grant 不能反向证明先前增长。
- `novarocks/execution/src/exec/operators/root_result_sink.rs:197` 把唯一原 permit 留在 sink 私有 Mutex。上游 pull 只拿 RuntimeState；这个 DOP 共享状态不能承担可变的当前 permit。
- `novarocks/execution/src/exec/operators/unpivot_processor.rs:192` 在候选已分配后测量，重复候选及完整 IPC 探针会产生共存 backing。`:391` 的 List/Map 常量 materialization 又重建 Field；结构相等不证明准确 metadata origin。
- 锁定 Arrow 58.2 `MutableArrayData::with_capacities` 初始 match 只支持 Array/Binary/List。Struct capacity 和 Map+List capacity 会在 child 分支之前 panic；不能把 `Capacities` enum 的存在当作支持证据。
- `vendor/arrow-array-58.2.0/src/builder/map_builder.rs:266` 自建 entries Field，设置 key/value Field 仍不能保留原 entries Arc。

## 下一实施切片

让统计 Unpivot 直接按原 `root_columns` 顺序输出，消除纯重排 Project；保持四列值、slot 与次序。由 `StaticSinkProgram::RootResult(contract)` 的准确用途选择专用 materializer，不从列名猜用途。把同一个 move-only 原 permit 沿最后 driver edge 显式传入 materializer，不能另发钱包或把可变 permit 放进 RuntimeState，也不能持 sink Mutex 执行分配/通知。

使用准确输出 ChunkSchema 的 List item / Map entries / Struct Fields Arc，直接构造有限、准确预授的 offsets/value buffers。避开普通 Unpivot 的 parts/to_data/MutableArrayData/IPC 往返。Arrow MutableBuffer 把请求舍入到 64 字节，超容量增长可能翻倍；必须先 checked 算完整真实 capacity 与 workspace，再填充到该界内。

```text
r64(x) = checked round-up-to-64(x)
buffer capacity = 4*r64(4*(rows+1))
                + r64(4*total_field_ids)
                + r64(total_blob_bytes)
                + r64(total_body_bytes)
                + 2*r64(4)
actual peak = buffer capacity + Array/Buffer Arc layouts + Vec spare
            + schema/origin-index/Chunk scaffolds + coexisting temporaries
```

32MiB 必须覆盖这个最终 batch 及其构造 workspace，公式的 buffer 小计尚不构成证明。挑选 batch 行数也须在分配前完成；单记录先核验。普通 Unpivot 与上游宽 global aggregate 继续由其原 owner 管理，不能把合法上游聚合强行缩到 32MiB。若分多 turn，Yielded 时原 permit 必须继续覆盖 workspace，不能沿现有 `Ok(None)` 分支提前归还。事后的 borrowed backing oracle 只用于验证，不能替代 pregrant。
