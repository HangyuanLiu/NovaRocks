# P04 receive header table：独立证据准备

状态：prepare-only，等待产品源码冻结；未复制源码、未运行 Cargo/Miri、未生成执行 receipt。只写本新目录，沿用前一探针18个production-pinned依赖身份。

最小 scratch helper 只包装实际实现：

- opaque BoundTable lease，转发真实 bind/get(newest0)/back/pop_back/push_front/len/capacity；get返回opaque actual Header clone或借用视图，不构造第二套ring。
- opaque actual hpack::Header，用实际field arena Bytes构造；保留clone及name/value pointer，用于识别数据alias和真正退出，避免DTO复制。
- actual typed slot Layout getter，调用 `Layout::array::<Option<actual Header>>(floor(max_table/32))`；Core/Arc按产品公开完整bound核实，不猜Header大小。
- actual Decoder helper，沿最终实际参数接入fixed table，调用真实decode_source与table状态；初始4096、ACK/min/start/CONT size-update语义oracle沿独立readonly审查结论，不在证据里创造协议模型。

预定独立实际源码验证：

1. 原授Core/Arc与完整typed Vec实际Layout、capacity与spare；构造及填满typed positions的requested峰值≤公开bound，ring操作不alloc/realloc。Header字段backing与原ownership carrier另授并分开计量。
2. 真实ring newest0/back、wraparound、evict/index、push满拒绝；拒绝不增长、不丢已有entry。变更max_table时不缩短原Vec backing，真实allocation capacity持续由同一原grant覆盖。
3. Bound lease Drop先清所有Header，再把空typed Vec归还Core；最后pool handle退出时才物理释放Vec/Core并退出原ownership。Core.storage在bound期间不持live Header，避免Core与field-pool owner互相保留。
4. actual Header clone跨table eviction、Decoder/table Drop继续持field arena原owner；table typed backing与field backing分别等待各自真正退出，不把数据alias当新的副本。
5. 一次绑定、并发bind、无效/溢出geometry与addressable Layout拒绝；constructor或bind拒绝不得先增长。检查cloneable public pool与noncloneable lease的实际能力边界。
6. clear期间Header owner Drop panic的实际退出顺序：不要假定返回Vec或最终credit依靠普通field order已经成立；按最终实现判断并加可失败的物理哨兵。默认路径保持原VecDeque语义。
7. actual fixed/default decoder对照：真正wire index与table eviction/resize state，独立review明确的size-update时序。thin helper不重写HPACK或ring。

源码冻结后完整h2/http/bytes immutable普通依赖复制，normal/offline/locked；所有target明确在自身scratch。保留完整source hashes、helper diff、ordinary/restored/quality/Miri及少量真正runtime FAILED的actualsource mutants，finally字节恢复。编译失败不算负向。不会操作生产target或旧证据。

范围只覆盖原授typed table backing与实际消费者接点；字段arena独立，HeaderMap/pseudo-header/framework/完整连接包络及Native安装仍不在本目录证明范围。
