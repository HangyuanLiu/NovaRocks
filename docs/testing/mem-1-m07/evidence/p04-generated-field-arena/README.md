# P04 自动 Date/Content-Length 的原字段 arena

HTTP HeaderValue::try_from_u64_with_pool 沿用实际 itoa 栈 formatter，按准确 digits.len 填原 field extent，不另取额度、不制造数字 payload 副本；static/普通 From<u64> 保留旧行为。Hyper 两端先取得实际 map 的原能力，只有 Vacant 才生成 Content-Length；server Date 同理，所有已安装能力的 shortage 沿现有 callback/reset 错误退出，无 heap fallback。已有字段在原池满时仍不请求新 position。

Date 使用同一个原 CACHED/check/next_update 秒缓存；其普通 HeaderValue 延迟到普通 producer 真正使用时才 materialize，render 仅使 Option cache 失效。有界 producer 在原 try_fill 成功后 check 并复制准确29B inline缓存，不新建普通 HeaderValue/共享控制块。首次TLS cache访问与 host clock回拨策略沿原实现；HTTP/1 仍读同一个 buffer。无 field capability 的普通 map 保持原 producer；普通 Native outgoing maps/Status 还需后续实际接线，不能称全 Native 路径已覆盖。

公开 Worker/System target5覆盖0/9/10/99/100/u64MAX独立golden、1/2/19/20 digit边界、所有position原完整bound、真实无contiguous extent拒绝、0额外payload分配、最后独立 HeaderValue alias持原额度和 System dealloc先credit；ordinary From<u64>原20B payload/first Bytes promotion/后续clone保持。实际完整HTTP/Bytes normal sources普通/Miri各3，复用实际field primitive，不是替代算法。

私有 Date probe复制实际完整 normal Hyper/HTTP/Bytes/h2 sources，公开forwarding只调用实际private helper与ordinary producer；另有明确的测试cache state injection（epoch bytes/未来next_update）来检验原check回拨政策，不替换render/clock算法。普通/Miri各4覆盖coldTLS仅一原预付wrapper、原Date值独立alias的最后owner退出、准确29B/位置/extent拒绝及ordinary懒缓存真实payload行为，23正常依赖身份核对production lock。Miri执行真实REALTIME必须使用-Zmiri-disable-isolation；只证被调用Date/field/缓存/alias primitive，不外推完整异步Hyper连接。date-copy和cold-cache私有actual-source mutants均编译后101/test FAILED，完整source/diff/log保留；原工作树未修改。

真实wire target3（18条连接）和public5通过；position满与aggregate满因果分别隔离，已有Date/CL满池跳过及拒绝后同connection/dispatch的stream3继续成功。恢复后26相关protocol targets共197通过；5 root actual-source及2 private normal-source mutants均编译后101/test FAILED，root字节精确恢复。Native basic check、HTTP/Hyper strict libClippy、两个新增Native targets Clippy（仅已有lib/dependency warnings、新target无warnings）、root/vendor/driver fmt/diffcheck均0；HTTP2双端、HTTP1双端、Tonicchannel五feature编译0，63生产依赖身份核验。6修改产品/测试hash、264vendor/67HTTPBytes源码pins与41完整lossless日志/diff已核验。首次wire Date通过，两个CL失败源于测试错误假设HPACK动态表保留Content-Length；源码既有test_content_length_value_not_indexed证明它采用不索引策略，不改产品来满足测试。CL原生成值alias证据由公开primitive提供；对端HPACK解码字段是独立副本，不能冒称发送原owner。

初始私有HTTP fixture64B/2positions违反既有geometry，改1position而不改产品；private Hyper forwarder Result命名冲突导致首次compile拒绝，改std::result::Result。首次Miri拒绝隔离环境REALTIME，随后启用上述实际clock执行。width mutant首次在allocator测量内unwrap引起panic-report allocations被算入family并触发cleanup abort；不计正常test FAILED，改为测量Result后在TRACK关闭时unwrap，保留原oracle，最终negative正常运行失败。首fmt检查碰到执行中的暂时mutant，仅记录该实验状态；源码恢复后独立fmt全通过。所有失败日志准确保留。

后续继续普通response/Status及initial/trailer实际capability、framework/body/error/task/socket/TLS/绝对deadline、完整2MiB connection包络与Native listener/client/profile/lane/predecode安装，再推进FE整窗和后续P04–P10。P04仍executing，P05–P10 open、V1未advertise；没有完整Native1FE+3BE/SQL/system/性能结论。desktop-linux完整fixture BOM live0，image/JAR齐全，未pull/build输入或改变global context；Linux由用户手动后补，无push/PR/archive，persistent goal active。
