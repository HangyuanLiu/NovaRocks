# P04：H2 header 分配前检查

本地切片基于 `084a5283352332d1e33c487646dba302bc553ca7`，仍处于 approved v5 P04 执行中。没有安装 Native profile、advertise V1 或产品/性能验收结论。用户明确要求 Linux 测试由其后续手动执行；本轮只做 macOS 本地验证。`desktop-linux` fixture 输入 BOM live 检查通过，无缺 image/JAR，未拉取/构建输入或改全局 context。

## 行为

h2/Hyper client/server 新增默认 `None` 的 `max_receive_header_block_size`。只有显式启用时：

- 对 HEADERS/PUSH_PROMISE/CONTINUATION 累计全部 HPACK 字节，已经解码消费的字段不漏计；在 continuation extend 前核对 checked 总量。padding、priority 和 promised stream id 不是 HPACK 字节。
- 声明的 encoded string 长度在等待 body 前与 compressed block 界核对；plain string 另按 local header-list 界检查。Huffman 必须按 decoded 长度检查，合法 encoded 字节数大于 decoded 字段预算仍能接受。
- Huffman 无分配验证/计数后才申请准确 output Vec，跳过旧 `encoded_len*2` reserve。`name+value+32` 在 HTTP name/value 复制与 table insert 前核对；indexed 字段亦核验。
- plain 字符串 compact 后进入 pseudo/table/URI，Huffman 采用独立准确输出，避免 tiny pseudo table alias 钉住整块 raw backing。超限的部分 HPACK 解码以 connection COMPRESSION_ERROR 退出，不能 reset 后继续使用不同步 table。

默认 decode/scratch/alias 路径保持现状；h2/Hyper 两端 clone/config/handshake 转发完整。Huffman 两次遍历和 compact 副本尚未性能验收。

## 验证与反例

最终 workspace 协议测试共 36 项：新增 header 8、既有 count 7、retained backing 6、retained send 6、Hyper count/backing 9。Native lib 574、Worker lib 313，合计 **923 项非重复 workspace 测试通过**。新增测试覆盖不提供 body 的超大声明立即拒绝、已消费字段仍计入 continuation 总量、未完成字段扩容前拒绝、准确 block 边界/跨帧字段值、client 与 cloned Hyper 两端转发、服务未调用，以及默认 decoder 仍等待不完整字段。

隔离探针编译当前实际 decoder/header/Huffman/ext 源码和 patched bytes，18 项通过（11 既有 private unit tests + 7 新探针）；新探针 Miri 7 项通过。实际 System allocator 记录显示 plain 超大声明、组合超限、indexed-name 组合超限，以及 Huffman 超限/非法输入在对应被检查阶段不申请输出；Huffman 准确输出 capacity、encoded expansion 合法输入、raw dealloc 晚 alias 行为均检查。动态表/HTTP head 仍存活时，bounded plain pseudo 已释放原 64KiB raw allocation；默认 alias 保留直到最后 owner 退出。

**组合 Huffman 超限不能声称零分配**：两个已验证 marker 可各自申请有限 output Vec 后才发现 `name+value+32` 超限。第 7 项探针明确接受这两份 temporary outputs 并检查 requested allocation 最大值，同时验证未插入 table。普通 HTTP 构造仍在组合检查之后。

8 类反例均真实触发 runtime oracle，逐字恢复源码：删 declared gate、删 combined gate、恢复旧 Huffman reserve、取消 compact、漏 continuation 总量、漏 initial block gate、Hyper client/server 各漏转发。private probe wrapper exit=1（内部 Cargo test=101），协议反例 exit=101；没有把编译失败当反例成功。反例当时使用 6 个新 private probes；其后只增加第 7 项共存探针，最终 restored source 18+Miri7 与协议36重跑通过。初始 encoded expansion 例只证明 compressed 比 decoded 大，随后改为 180 个 `#`，同时证明 compressed 大于 decoded-string 预算仍合法；两个阶段原始日志保留。

两 vendor lib strict Clippy 零 warnings，Native all-target Clippy 既有 warning 基线、workspace all-target check、root/vendor fmt、diff 检查通过。公开 builder 与共享 header codec 的 wave 收敛触发 workspace check；没有把每个探针升级全仓 test。未尝试 standalone upstream dev suite；该隔离探针仅覆盖 decoder/Huffman，frame::Error conversion 使用未调用的 neutral stub，真实 frame/transport 则由生产锁下协议测试覆盖。

## 复现与来源

```bash
cargo test -p novarocks-native-adapter \
  --test native_h2_header_preflight --test native_h2_bounded_receive \
  --test native_h2_retained_backing --test native_h2_retained_send \
  --test native_hyper_bounded_receive --offline -- --test-threads=1
python3 docs/testing/mem-1-m07/evidence/p04-h2-header-preflight/reproduce.py --miri
```

脚本只在临时 workspace 编译当前实际源码，使用保存的精确 lock 与本地 patched bytes，不安装 nightly/Miri 或下载依赖。isolated frame stub 不证明完整 h2；协议测试才消费真实 h2/Hyper。

`index.json` 保存 20 项当前源文件 hash、16 份完整命令输出、8 份完整 mutant diff/log 及实际恢复 hash。h2 registry 66 / Hyper 68 原始文件逐项 live 校验；当前原文件变更 h2 13 / Hyper 4 包含前面已保存的 count/pool/send 切片。Cargo 版本与 root lock 没改。

## 未闭合边界

当前只限制 header 解码增长并去掉 raw/pseudo table pin。原始 read BytesMut、continuation 的 capacity/spare、HeaderMap indices/buckets/duplicate metadata、table VecDeque spare、HTTP name/value/URI 逃逸后的原 carrier 租期，以及 stream/task/socket/握手等仍需准确 owner/原 grant。local header-list 的 `name+value+32` 是逻辑长度，不是 allocator footprint；不能由 16KiB 或 count 推断物理容量。完整 **2MiB connection envelope 尚未证明**。Native listener/FE Tonic Endpoint/Channel 的 owner、预 decode admission、lane 及最后原子切换仍未完成；P04 与 P05–P10 不关闭，无 push/PR/archive。

文档目录检查：当前仓库没有 `docs/design` 或根 `src`，机械执行 docs/AGENTS 模板命令曾报 missing path，不能记为通过。设计/plan 使用已解析的外部 DOC_ROOT；未创建 legacy 目录。对实际 `docs/novarocks/tests` 的遗留引用扫描无命中（rg exit=1），`docs/superpowers` 不存在。
