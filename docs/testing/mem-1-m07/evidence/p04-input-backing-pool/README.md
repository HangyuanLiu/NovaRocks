# P04 原始输入 backing 与进程池预留接缝

`borrowed_root_chunk_storage` 在 cursor/clone/hydrate 之前借用原始 Chunk：先校验 actual RecordBatch Schema Arc 的准确 map owner，再覆盖每个 slot 及 aggregate compact index 的全部 retained Field owner（包括投影后未达字段）、Field name实际capacity、Map/Struct Fields Arc、Dictionary独立Box、Timestamp时区Arc、known metadata table及String backing。actual standard array自己的DataType也分别检查，不用声明schema的结构相等推定来源。全部typed拒绝路径零分配；无行、offset、key扫描，未知HashMap不迭代、不规范化复制。index cache借用来源保活，避免指针复用ABA。

完整标准Array/Buffer/RecordBatch Vec原容量检查沿用前一切片，新增actual DataType visitor只在exact standard downcast之后运行。ChunkSchema私有slots/ids/logicalchildren Vec spare、private never-deleted fresh index table、Chunk/固定accounting和provider holder Arc壳都在同一96MiB原始输入总界内；别名允许保守重复计数。Map entries/Dictionary key-value物理节点与语义深度区分，源载体树最多2×8192物理节点、aux工作65536，root列数4096只限制顶层、不误限制合法nested Struct宽度；全4096列dictionary回归通过。Arc Layout按已核实pinned Rust1.92 ArcInner两个AtomicUsize及checked extension/padding计算。

边界：这证明输入payload、metadata及固定holder/scaffolds的backing上界，不是source增长前funding能力，也不证明hydration/encoder峰值或整体query heap。共享MEM tracker/provider治理图及opaque lease保持既有admitted accounting owner作用域，不从其logical bytes推导物理容量，不把治理图所有后代重新归入结果payload。未知Custom Buffer/array或field/schema metadata来源明确拒绝；其他source需按既有契约补准确construction origin。

`ResultRetainedBudget::try_reserve_process` 为固定编码池/线程stack预留显式进程费用，与现有root stream共享唯一process cap/total/highwater，无伪Task identity、无第二钱包。checked守恒、与真实Task双向竞争、shrink/drop、root退休后池仍存、Weak不保活budget、通知在锁退出后等6项新测试通过；顺便修复Task credit缩到0后Drop再次找已删除key的panic。实际线程/池必须把credit保留到物理退出，此切片尚未建立或接线Native finite producer。

验证：capacity/zero-allocation24（5个新wholeChunk反例）、schema12、Chunk79、originalcarrier2、rootchannel19、budget29通过；Worker all-target Clippy（依赖既有warning）、Native all-target check、fmt/diff通过。初次新测试错用整数Into<SlotId>导致测试编译失败，改用SlotId::new后全部复跑通过；未修改产品语义或放宽profile以绕测试。

P04仍executing：Native producer/session/read ingress、exchange/special source来源传播、显式domain/count接线继续；V1未advertise，无候选native功能/性能结论、无push/PR。Linux由用户后续手动测试。
