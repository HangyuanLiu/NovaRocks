# P04 pseudo-header 的原字段 arena 转换

本切片闭合已有 bounded HPACK 路径上的长 Method 和 custom Scheme 额外副本。HTTP 新增 Method::from_owned_bytes(Bytes) 与 Scheme::from_owned_bytes(Bytes)，不另取额度或重建 backing：输入由调用者提供，长 Method/custom Scheme 持同一个 immutable Bytes；标准 Method 与至多15字节 inline extension、准确 lowercase http/https 沿旧 static/inline 表示，释放原输入位置。错误同样释放输入。h2 的 literal new-name 与 indexed Name::Method 两个 shared hooks 都接新 API；Peer request URI 转换直接接管 Scheme Bytes，保持原诊断。default h2 的 custom Scheme 也转移其真实 Bytes，内容语义不变；HTTP 普通 from_bytes/parse/URI parse_full 的 Box 路径和默认 Clone 行为仍保留。

Method 的私有 Owned(Box) / Shared(Bytes) storage 按内容 Eq/Hash，不因 storage variant 改方法身份；Method 保持 case-sensitive。Scheme 内联 Shared(ByteStr) 避免新 Box，其原 case-insensitive custom Eq/Hash、parse_exact 字符表、empty/numeric/~、64B上限与标准识别原样复用，未额外按RFC收紧。既有 uppercase HTTP 与 lowercase standard HTTP 的跨variant规则亦未更改。Scheme/Method/Uri实际Rust布局可能改变；已有typed backing bound继续从实际 Layout 计算，不将协议32B entry overhead当Rust allocation，不猜平台字节数。

实际HTTP/Bytes全部normal production sources的33项普通/Miri探针通过（原23＋pseudo10），包含原指针/shared clone/最后position、default eager Method Box clone、同内容hash、标准/inline/invalid与64/65边界、UriParts→Uri→Request别名和原grant退出/unwind。只验证实际HTTP/Bytes及3个production依赖，不外推Miri完整h2/Hyper。首driver使用Scheme==&str触发E0277，改用原文档的PartialEq<str>（*str）保留原case-insensitive oracle，未修改产品API。

最终24个protocol targets共189通过；新增实际h2/Hyper target4首次通过，覆盖literal/indexed Method、Huffman long a16、custom Scheme独立alias、跨stream1/3动态index63/62。真实Worker原raw/block/field/table/map及各carrier先于构造预授，所有config/public pool/actualIO与executor/task退出后，仅一种Method/Scheme alias仍阻止原field额度重授，最后alias退出才能全额重授。测试只声明这些具体owner的原授寿命，不声称完整连接包络。

公开System/Worker预算探针4项通过：原arena/bitmap/Core三次分配、全部8个position的wrapper完整界、物理System dealloc先于原credit退出、shared构造/clone/hash/Uri/Request无新分配、default Method eager Box clone与普通Scheme首次clone的24B Bytes promotion＋Box均按实际行为验证。首回放2项失败准确捕获Scheme首clone，以及普通Authority/Path fixture在Uri clone中的两个24B promotion；最终fixture让Authority/Path也持真实原arena input，明确区分已有共享owner与任意promotable Bytes，未修改产品或弱化oracle。

6个actual-source runtime negatives均编译后101/test FAILED，覆盖两个HPACK Method hooks、Peer Scheme未授转换、公开Method payload复制、Scheme额外Box以及storage-dependent Method hash；全部源码精确恢复，恢复后189项协议检查通过。首次前三个negative已运行失败；第四个初版仅被dead-code lint拒绝，未计runtime proof。修正mutation保留真实Shared helper后，后续三项全部触发运行时失败；初版和最终完整log/diff均保留。

Native basic check与四vendor strict libClippy通过；Native两个新增target Clippy通过，只有已有依赖/lib warnings、新增target无warning；root/vendor/driver fmt与diffcheck通过。Hyper client/http2、server/http2、Tonic channel-only独立编译0，63生产lock依赖身份逐项核验。reproduce.py --miri 校验67 HTTP/Bytes pins，reproduce_features.py校验5vendor264 source pins；只复制实际normal sources，不下载输入或以替代算法代替生产实现。

后续继续自动header payload、普通Status/response、framework/body/error/task/socket/TLS/绝对deadline与完整2MiB connection包络及Native listener/client/profile/lane/predecode真实安装，再推进FE整窗和P04–P10。P04仍executing、P05–P10 open、V1未advertise；无完整Native1FE+3BE/SQL/system/性能结论。Linux按用户手动后补，无push/PR/archive，persistent goal保持active。
