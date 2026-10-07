# P06 Local collector 真实 buffer capacity

2026-10-07，local-only 检查点；不代替生产 window/renderer/native 验收。

`LocalTableBuilder` 保留 frozen 65,536 行 / 32 MiB logical cells（每 cell 5 B offset/validity）/4,096 列；明确拒绝比 frozen profile 更松的 constructor 参数。借用的 String schema 名字/类型/header 先按 compiler 相同的 name+16/leaf+32、schema wire/backing ceiling 检查，再构造 ResultField/Arrow/schema 副本。

collector 使用自己的分段 `LocalBuffer<u8/i32>`，每块最大 64 KiB。只有尾块可有 spare capacity；整行在 append 前先承诺全部 mandatory growth，额外 slack 只花剩余 collector bytes。实际 values/offsets/validity buffer capacities 合计 ≤32 MiB，leading zero offset属于有限 root metadata。空间或拷贝峰值不足时 compact各 buffer 的末块，不复制完整累计列；reserve/compact前检查 incoming cells + old/new tail copy ≤32 MiB workspace，leading offset元数据另在root share。allocation failure把builder标记failed，不能publish部分row；普通shape/logical预算拒绝仍不追加。

新列在 finish 时经公开 Arrow Buffer/ScalarBuffer/OffsetBuffer/BooleanBuffer/StringArray 接口构造；单块直接移交 Vec，无第二payload分配，多块逐buffer合并，旧块随消费实际退出。consolidate 前 source conversion workspace 必须退休；现有 consumed-row helper 和逐row SHOW loop在finish前退出当前row，其他source与实际生产window alias接线仍须逐项核对。没有第三方源码修改或fork。

结构 metadata 上界来自：completed块满64 KiB，所有payload受32 MiB界，至多512个completed块加每列的3个tail；block Vec growth旧/新槽、有限column/field/schema headers和预检后的names属于root metadata，不能按全行数生成无限Vec<Row>。本切片不将完整96 MiB生产包络、renderer成本或最终owner funding描述成已经验收。

Query Application lib **515 PASS**，日志 `logs/mem-1-m07/p06-local-capacity-lib-final-20261007.log`。新增反例：精确whole logical limit在capacity压力compact后仍可接受；拒绝后已有行/容量不变；大incoming row触发旧tail slack收敛；65,536行unicode/NULL/offset/bitmap跨slab与Arrow oracle相等；大name/whole schema/松profile拒绝；独立denied growth在reserve/copy前保持原buffer；单block实际pointer移交Arrow不复制payload。

Frontend lib **1,421 PASS**，日志 `logs/mem-1-m07/p06-local-capacity-fe-20261007.log`。diff/fmt通过。

开发首轮新metadata测试把`&String`数组当`&str`数组，编译失败日志 `p06-local-capacity-adversarial-20261007.log` 保留，修复类型后完整lib通过；没有放宽断言。只读复核无新actionable finding，未运行release性能/1FE+3BE，也未改变生产V1 advertise或删旧FE防护。
