# P04 fixed header encoder：独立 actual-source 证据

`reproduce.py` 从当前 vendor 复制完整 HPACK 源到隔离 crate；只省略外部 fixture/fuzz 模块接线，保留全部 inline upstream tests，向实际 encoder 追加10个探针。frame Error enum 逐字抽取；`block_capacity` 逐字抽取，仅扩大为 crate 可见供测试调用。两者都不是重写模型。所有算法文件 SHA 在 `source-sha256.json`；依赖 identities 核验产品 Cargo.lock，保留原 bytes vendor，全部离线。旧 send-header-table evidence 不修改。

```bash
python3 docs/testing/mem-1-m07/evidence/p04-h2-fixed-header-encoder/reproduce.py --negative-fixed-growth --quality --miri
```

10个探针覆盖实际 encode_str 对全256 octets、multi-byte length-prefix 搬移和30-bit最大 Huffman code；合法 HeaderValue 的重复/nameless/static/sensitive 表达与实际 Decoder；六 pseudo vocabulary（混合 request/response仅测试HPACK字节，不冒充合法HTTP请求）；初始zero update与多次peer SETTINGS；固定Vec spare/full/zero-capacity和越界 advance 的panic前len不变；实际geometry helper的checked范围；System测量positive control。输入header及clone、Vec/BytesMut、实际decoder都在计量scope外构造。scope内固定编码不得alloc/realloc，并断言原pointer/capacity保持；这不是pool/wrapper物理退出、原始credit归还、完整HTTP连接或Native安装的证据。

隔离副本的唯一negative修改在 FixedEncodeBuffer构造时增加reserve；目标实际runtime观测realloc1，原固定encoder的allocation assertion失败101。保留完整diff/stdout；finally字节一致恢复，重新运行全部ordinary。negative必须匹配实际目标测试、FAILED汇总和runtime assertion，compile failure不算。

首轮2个测试oracle错误完整保留：空字符串合法编码0x00、不设置Huffman flag；0xff是26-bit，而0x0a是30-bit Huffman symbol。修正只涉及探针，未改产品。最终结果、Clippy/fmt/Miri范围见receipt.json与final-run.log.gz；Clippy包含实际 copied-source/fixture warnings，不声称strict clean。Miri仅执行10个新探针。
