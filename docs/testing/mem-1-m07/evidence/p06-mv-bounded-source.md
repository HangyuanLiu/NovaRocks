# P06 MV local source 收敛

日期 2026-10-07。逐源修复记录，不代替生产 V1/1FE+3BE 验收。

## Codec 前置预检

在已核对 NRMA envelope、record kind、注册 schema ID/fingerprint 后，V3 projection 先以借用切片读完固定 Avro 结构，再对 D/L/P/C 作为一组执行原 protobuf structural preflight；此时没有 owned Avro Value/ByteBuf/Current model。外层无 collections，按四倍 payload + 4 KiB 固定 allowance 从同一 decode 工作集扣除，剩余才交给文档 codec 的六-copy/item/depth envelope。

`decode_projection_with_budget` 接收显式预算，完整语义与 exact source revision/CAS version 仍由原实现校验；没有修改持久化格式、schema 或版本。旧 projection reads 使用原 default ceiling；local caller 的更小预算接线仍待下一切片。

定向 StateStore repository 23 PASS；完整 MV lib 274 PASS。新增用例覆盖：合法 projection 4 MiB 预算 roundtrip；1-byte outer/document refusal；巨大正 Avro byte 声明在 owned decode 前以 truncated refusal 退出；outer + documents 共用一个工作集。日志 `logs/mem-1-m07/p06-mv-codec-{preflight,all}-20261007.log`。

## 尚未闭合

repository 全量 raw/projection scan、readiness 全量 Vec、SHOW/info_schema 逐项源头接线、dependency display 的独立 full scan 与 row clone/format 前预检仍 OPEN。不能根据本 codec 测试宣称 P06 全链完成。
