# P04 借用标准 Arrow storage 的有限检查

只覆盖 array 对象、标准 Buffer backing/descriptor、nulls、完整 dictionary values、StructArray 原始 children Vec spare。字段名称、DataType/Field/schema metadata、ChunkSchema/RecordBatch 容器和构造来源仍需独立证明；此函数不能作为完整 Chunk input proof。尚未接入 Native producer。

17 个真实 allocation/owner 探针通过，包含原6个getter探针和新增11个storage检查；成功和错误路径均使用线程内 allocator 验证零分配，不调用to_data/hydrate，不检查row/cell/offset/key值，不计算logical nulls。未知Custom、pool reservation或未封闭的carrier拒绝。别名重复累计为保守上界，不建立growable去重表。

新增standard_owner_metadata_size从private Bytes实际固定类型推导Arc metadata空间，Custom及任何pool-enabled build返回None，避免猜固定descriptor大小或调用opaque owner。UPSTREAM.json补充原registry source/checksum及全部未修改文件hash；此前本地registry实际没有cargo_vcs_info，原PATCH文字已修正。

readonly review发现Map entries Struct及dictionary keys/values是额外物理节点，不能额外消耗语义深度；实现计入节点工作而保留语义深度，64层Map、64层List+dictionary leaf成功，63层预算明确拒绝64层Map。外层wrapper即便转发as_any也不能隐藏不同对象地址/尺寸。callers不能扩大冻结node/depth/backing ceiling。

Execution focused Clippy通过（19个既有lib warnings），Native all-target offline locked check、原original-carrier2通过；fmt/diff check通过。source wrapper/codec/IPC/Connector origin 接线仍待完成；不能通过结构相等、expected schema或迭代未知HashMap补造actual carrier的来源收据。
