# P04：最后 driver edge 的原始 input grant

parent `0841ffcfa694d8088687c04411b050000115bb0d`。这是本地模块行为切片；P04 继续，P05–P10、完整 Statistics materializer/codec/session/FE 接线、Native listener/lane/H2 独立副本仍未完成，V1 未 advertise，无分布式或性能验收结论。Linux 正式测试按用户安排后续手动执行。

Processor API 把 sink 预授的同一 move-only RootInputPermit 移到唯一 terminal edge；source 只借用该 permit，不经 RuntimeState/Mutex 执行增长或 mint 新钱包。Chunk / CPU Yielded / Empty 分别代表交接、有限工作后的 Ready 下一 turn、无未发布输出。Yielded 保留原 generation 和 workspace，继续执行由 driver edge 原 grant 决定，即使普通 has_output 仍没有完整 Chunk。其它普通 edge 仍使用原 processor 契约。

成功移交前同步销毁 construction scratch；empty/error/cancel/driver Drop 先退出真实 Chunk 与 root workspace，最后释放 permit。driver 字段 Drop 顺序也保留 operator/edge owners 在 permit 前。RootSink 第一条语句用 owning pair 固定 Chunk→permit 的 Drop 顺序，避免 host 状态检查 panic 反向释放；pull panic 保留原 grant 至 executor failure cleanup。每 driver 新 inline holder Layout 加入已有 RootSink metadata pregrant。

Statistics SQL 去掉纯重排 Project，Unpivot 直接输出 [input_fields, blob_type, body, properties]。物理计划端口显式列序仍准确：独立角色必须构成输出端口的精确双射，所有 produced value 的来源 ordinal/type/nullability 继续验证。现有 wire output_schema、常量与 literal role 的关联、Native/Local SlotId 投影不变。

新增5个 real-driver/host 测试覆盖同 generation 跨 turn、has_output=false continuation、success/empty/error、cancel/Drop、host state panic、pull panic。弱 Arrow backing 观测与实际信用退出 callback 证明物理退出顺序；总15 root sink测试通过。省略移交前 workspace 清理的 mutant 会在真实 backing 仍活着时失败；恢复原源后通过。PhysicalPlan新增显式 permutation、缺/额外/重复列、错误 origin ordinal 与 reused-child duplicate 反例。恢复旧 membership-only 角色校验且 fixture 无未使用错误 origin 时，整个非法 Fragment 被接受，negative oracle失败；恢复后拒绝。第一次 mutant 的旧 fixture 有一项未使用的错误 origin，全局校验仍拒绝，只遗漏 role 诊断；该历史日志保留，不能当作整个 Fragment 被接受的证明。

首次 quantum 测试还发现 driver 在 has_output=false 时误走外部 readiness：修复为显式 CPU Ready continuation。只读审查随后发现 state-check panic 的参数反向 Drop，以及下一 turn continuation 仍受 has_output 限制，两项都以真实 oracle 修复。初次 Statistics SQL 回归定位重复的 value-first 物理顺序校验；保留 domain 四列合同，未改顺序或降级类型。

完整源码 hash、命令、退出码、原始/gzip日志 hash 和 mutant/restoration hash 见 index.json。Execution 1528、PhysicalPlan178、SQL DML13、Native Unpivot4、PlanCodec33、Native Session12、Host150通过（合计1918，root sink子集不重复计）；workspace all-target check、目标三包 all-target Clippy（既有 warnings）、fmt/diff通过。完整切换后的最终同 SHA 1FE+3BE CI 尚待后续阶段。
