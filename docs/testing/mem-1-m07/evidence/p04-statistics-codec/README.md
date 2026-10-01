# P04：StatisticsArtifactV1 独立流式 codec

parent `b42347c0a36c43be4c63672066b2695d84f94b6c`；完整 source/log hash 与实际命令见 [index.json](index.json)。本地 macOS arm64 / Cargo dev，只证明独立模块。

记录为固定 24B `STA1` 前缀：完整记录长度（含前缀）、field ID 数、blob type 字节数、body 字节数、properties 数（必须 0），五个 u32 little endian；后接保持原顺序的 i32LE IDs、UTF8 blob type 与原始 body。Header/body 可跨段，无 padding，无 sequence/ACK；使用既有 Root 通道内核。纯前缀 parser 在复制/assembly 前核验各声明和总长度，完整记录必须精确匹配长度，End 截断不可当作完成。

Encoder 接 owned RecordBatch，不创建 record Vec/protobuf/IPC 副本。constructor 只审查四列准确 schema/carrier，数据验证与输出每 turn 最多 64KiB examined bytes（读取及复制合计）、1024 工作（含每次 hash probe）。查重使用 inline 2048×8B generation 表，不为每行分配/清表；行完整输出后计 completed_rows，恰好填满输出也能发现完成。Caller 仍须在 clone/Box/input/segment 之前取得原预算并保持真实 Chunk owner。

声明限额：正且唯一 field IDs≤1024、非空 blob type≤64KiB、body≤16MiB、properties 为空且实际 NULL 拒绝；累计 artifact≤4096、body≤128MiB、已声明 header/IDs/blob metadata≤16MiB。后者是 wire declaration ceiling，不是 FE BTree/Arc/String 等实际 metadata backing 的分配证明，FE collector 必须另在增长前覆盖实际容量。

sub-agent 首次 test 11/11、首次目标 Clippy 退出 0，无初次失败或修复重跑。main 独立审查后接公开 export，并将测试改为真实 library consumer，新增非零 List/String/Binary/Map offsets 的独立 literal golden：被 slice 排除的非法/NULL/property 行不得参与编码；12/12 通过。覆盖完整/短/非法前缀、完整 record 截断、16MiB±1、逐字节与跨 segment、累计 row/body/metadata、hash 碰撞最坏 work、取消保活/释放、constructor/step/header/cancel 的 TLS 隔离实际 allocator 零分配。目标 Clippy、Native all-target check、fmt/diff 通过；既有依赖 warnings 原样保留，新模块无 warning。

P04 仍 executing。本模块尚未安装到 Session/SQL用途，也未接 FE assembly/collector/membership/all-success；Unpivot 源头增长与 32MiB 实际原始 input/backing 证明仍继续。其它 InternalFacts 仍明确拒绝。没有改变 frozen Native 方法 manifest，没有 V1 advertise、C4/native/performance 验收或 MEM 实际计费声明。Linux 正式测试按用户后补。
