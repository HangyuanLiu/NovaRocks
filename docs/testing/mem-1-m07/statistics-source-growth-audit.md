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


## 2026-10-02：原 input permit 的最后 edge 接缝

本轮只闭合 driver 接缝和 Statistics 计划列序，不宣称专用 materializer 或 32MiB 事前增长证明完成。新 `RootPreparedPull` 区分 Chunk / CPU Yielded / Empty；driver 把 sink 的同一 move-only permit 移到唯一 terminal edge，向上游借用。Yielded 保持同 generation 与真实 workspace，并立即 Ready；下次推进由 edge continuation 决定，不要求普通 `has_output()` 已有完整 Chunk，也不等待不存在的外部事件。成功交给 host 前同步销毁 construction scratch；空/失败/取消/Drop 先销毁真实 Chunk/workspace，最后归还原 permit。sink 入口 owning pair 保证状态检查 panic 也按 Chunk→permit 退出；pull panic 保留 grant 至 executor failure cleanup。

Statistics SQL 直接以原四列顺序创建 Unpivot；physical-plan 保留输出端口为排序权威，专用 validator 检查实际 distinct port set 与独立角色 set 精确相等，继续核验 produced value 的 port ordinal/type/nullability。wire 原有唯一 `output_schema`、Native/Local 的 SlotId 角色关联和普通 runtime 的 schema 顺序没有改变。来源、缺列、额外列、重复 reused child 和错误 ordinal 都有定向反例。详情和日志见本轮 `evidence/p04-root-input-edge/`。

下一步的无分配预授接点仍是：Arrow Buffer 私有标准 owner Layout 的 static getter，以及可在创建数组前借用的准确 ChunkSchema/origin/scaffold footprint。四列直接构造需要计入 11 个 Buffer metadata owners（含两个零容量 values owner）、7 个实际 Arc<Array>、RecordBatch columns Vec4 与 Map entries children Vec2；Map 的 StructArray 本身 inline。必须把实际 schema/index、workspace 和共同临时 holder 加入先前 buffer 公式。最终 safe constructor 的 UTF8/offset 检查同样消耗 quantum，不能只给复制计量；任何批大小选择需覆盖合法单条最大 label/body，不能靠收窄领域合同规避。未接入原 session 前 InternalFacts 仍明确拒绝。
