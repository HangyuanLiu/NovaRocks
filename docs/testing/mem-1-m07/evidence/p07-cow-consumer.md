# P07 COW 专用消费者接线检查点

日期：2026-10-07。基于 `5bf78e514` 后的本地修改；生产 V1 sink/window 尚未切换，不作为 1FE+3BE 或完整 M07 验收。

UPDATE/MERGE 的 exact match query 改为 `CowMatch` intent，持有从 Connector 签名 preparation 建立的有界 consumer。旧 decoded carrier 逐 batch cast 到 signed layout 后交给 collector；V1 carrier 逐 body 接入领域 assembly/decoder。两者均不先构造完整 QueryResult。外部 COW 写会话只在已验证 selection 返回后打开。

领域 assembly 长度先检查 min(collector budget, codec bound, 32 MiB)，声明越界在扩大 buffer 前拒绝。V1 End 检查完整记录与已收集 row count；EOF 执行 selection/control、signed contract、effect 与目标唯一性校验，然后才能申请 Root success seal。空流保留签名 schema。receipt 在 domain decoder 接管 body 后、原 reply backing 退出后发出；验证失败走 read seal/cut、abort 和原 failure classification。success-seal 请求本身拒绝也改为统一 cut，避免 BE context 只能等 lease expiry。

验证日志：`logs/mem-1-m07/p07-cow-consumer-20261007.log`。Frontend lib 1,415 项通过；只读复核指出 success-seal refusal 漏口后已修复，最终复跑 1,415 PASS。新增 consumer 测试覆盖正确 selection、End 行数不符、半记录 End 与重复目标。现有 relayed split/digest/budget/cancel、COW DML flow tests 继续通过。

尚未证明 domain decode/cast、retained batches、RowConverter/uniqueness/digest 共存全部事前授权。P08 必须接完整 Internal window/有限位置；C7/P09 仍需真实 SQL COW effect、取消与 owner 退出。未运行全 workspace、本轮 Native socket 或 1FE+3BE。

后续冻结 profile 检查：`47ceac6ed` 提交接线。随后 collector 显式限制 64 MiB（原 ConnectorRequestContext 已间接限制总预算）、1,048,576 行与 4,096 batches；行/batch 检查在 legacy cast 前、relay Arrow decode 前进行，零行 batch 也不能无限保留。`p07-cow-profile-final-20261007.log` 的 10 项测试 PASS；新增过大声明行数（payload 仍为单行）的反例通过 ResourceExhausted 与 decoder MalformedBatch 区分拒绝顺序。只读复核未发现新问题。第一次新增用例误用 256 MiB Connector context，超出既有 SPI 预算导致 setup 拒绝；改为合法 64 MiB，未改产品界限。


## P06 conversion, uniqueness and cold-contract preflight (2026-10-07)

The registry Arrow 58.2 RowConverter now has a borrowed shape/value preflight.
It covers constructor synthetic null arrays, nested/dictionary child Rows,
length/offset vectors, parent encoded Rows and copying coexistence before any
SortField clone, converter construction or conversion. Digest uses the same
preflight across the complete batch set and preserves its v2 canonical bytes.
The fixed conversion workspace is 256 MiB; composition includes the retained
selection, bookkeeping and canonical uniqueness storage.

The selection source measure includes root schema and each independently held
batch schema, Field/type/name/metadata storage and sparse metadata capacity,
as well as public Arrow array/buffer capacities. Outer batch Vec capacity and
its offsets are checked separately before growth. Collector schema publication
is atomic with successful retention. Empty batches still consume the frozen
4,096 batch positions. Schema metadata is not free even for empty selections.

Uniqueness retains canonical byte keys in a hashbrown table whose allocator
checks the actual Layout before allocation. Old/new table generations, key
payload/headers, exact key copies and the current conversion peak share the
fixed workspace. Duplicate checks borrow existing bytes and allocate no key.
The key/table limits do not change signed target identity or effect semantics.

Match contracts are checked by borrowing before HashSet allocation: 4,096
fields/uniqueness tokens, 16 MiB aggregate metadata/capacities, 64 KiB names,
depth 64 and a bounded traversal. Validation no longer clones the contract.
Field Debug bytes are counted and streamed into the original v1 digest,
preserving its existing length prefixes and metadata iteration order.

Validation: SPI library 334 PASS; Frontend library 1,424 PASS; the dedicated
canonical-key tests 3 PASS and COW consumer tests 10 PASS. Logs are
`logs/mem-1-m07/p06-cow-row-preflight-{spi-fourth,fe-final,keys-second,fe-third}-20261007.log`.
Earlier failing fixtures used array-only byte budgets; they now use the full
source measure and assert refusal leaves the original retained schema unchanged.
No product budget was widened.

**Still open:** arbitrary custom Arrow Buffer owners and private container spare
capacities cannot be proved by public capacity inspection. The source owner
receipt and bounded cast path are separate pending slices; this checkpoint
does not authorize arbitrary Arrow ingress or retire the old FE quota. Domain
execution positions, whole-window production funding, socket/1FE+3BE COW
effect and P09 performance/transport gates also remain pending.


## P06 构造 owner 与 signed cast（2026-10-07，当前切片）

SPI 新增 opaque source builder/buffer/array/children/batch。只能由受限复制与组装工厂
铸造 receipt；没有接受任意 RecordBatch、ArrayRef、Buffer 或 bool 的 adopter。
每个 source 总计 32 MiB、schema backing 1 MiB、4096 nodes、depth 64、1,048,576 行。
标准 buffer 按 64 B 对齐和 header 计量；私有 child/column 容器按实际 capacity 与
构造重叠计量。预算不在构造过程中返还。借用 ArrayData 的整树复制预检在第一个
payload copy 前完成，offset 非零拒绝；copy_data 不保存原 Buffer/ArrayData 别名。

领域 codec 保持原 wire grammar。owned schema 先借用预检名称、节点、深度和
构造 backing，再调用既有 schema decoder。batch directory 直接借用记录，buffer
与 children 经工厂创建。只有成功 batch 才推进 count。旧 decode API 是切换期接口。

FE 在调用任何 cast 前先检查 signed target 的 source-owner eligibility，再借用
计算已有 source、assembly、retained selection、validator/bookkeeping、全部列的
kernel output/temporary/copy 共存。支持的类型对使用原 cast，未证明的类型对明确
拒绝；signed 输出经工厂重新复制，切断可能的 foreign/private backing。
owned selection 只接收 source receipt，复制/验证保留 proof，不允许混用 carrier。
validator converter 单独限定 32 MiB，digest 预检同时留出其与 bookkeeping 的
40 MiB 共存额度；转换总 workspace 仍为 256 MiB，产品总界未提高。

Source 闭集为普通 scalar、Utf8/Binary（含 Large）、Decimal128/256、合法
Time/Timestamp、非负宽度 FixedSizeBinary、List/LargeList/Struct，以及
Int32→Utf8/LargeUtf8 Dictionary。Map、FixedSizeList、view、union、run-end
目标在 cast 之前拒绝。转换闭集独立更窄，不能因为 source 类型可表示就放行
未证明的 kernel。已复制输出的 factory 拒绝不会改变已收集 selection。

本切片的 SPI 341、Native owned codec 3、既有 codec integration 8 项通过。
Frontend 全库 1,435 项通过，COW 定向 24 项通过。日志为
`logs/mem-1-m07/p06-cow-source-{spi,native-unit,native-codec,fe-targeted,fe-final-trace}-20261007.log`。
新增 cast footprint 未引入 dead-field warning：组件上界用于 trace 观察。该记录仍不证明完整 Internal window、
有限 domain execution 位置、生产 V1 sink、1FE+3BE COW effect 或 P09 测量门。
