# P06 MV local source 收敛

日期 2026-10-07。逐源修复记录，不代替生产 V1/1FE+3BE 验收。

## Codec 前置预检

在已核对 NRMA envelope、record kind、注册 schema ID/fingerprint 后，V3 projection 先以借用切片读完固定 Avro 结构，再对 D/L/P/C 作为一组执行原 protobuf structural preflight；此时没有 owned Avro Value/ByteBuf/Current model。外层无 collections，按四倍 payload + 4 KiB 固定 allowance 从同一 decode 工作集扣除，剩余才交给文档 codec 的六-copy/item/depth envelope。

`decode_projection_with_budget` 接收显式预算，完整语义与 exact source revision/CAS version 仍由原实现校验；没有修改持久化格式、schema 或版本。旧 projection reads 使用原 default ceiling；local caller 的更小预算已由下方 inventory 切片接线。

定向 StateStore repository 23 PASS；完整 MV lib 274 PASS。新增用例覆盖：合法 projection 4 MiB 预算 roundtrip；1-byte outer/document refusal；巨大正 Avro byte 声明在 owned decode 前以 truncated refusal 退出；outer + documents 共用一个工作集。日志 `logs/mem-1-m07/p06-mv-codec-{preflight,all}-20261007.log`。

## Inventory / readiness / local row 接线

两个 repository 直接实现显式 `MvProjectionInventoryBound`，没有 full-list 默认 fallback。StateStore 在一个 read snapshot 内以 page_size=1 读取，逐记录校验 raw bound 和较小的 complete decode budget，随即只留下 target/object identity；thin vector 增长含旧/新 backing 的重叠检查。正常、损坏、超限都等待同一 transaction abort，失败不返回截断 inventory。

local caller 使用 65,536 entries / 16 MiB snapshot / 1 MiB raw page / 4 MiB complete decode。readiness cursor 逐 target fresh-load 并核对准确 StateStore version；deleted target 跳过，replacement 不能继承旧 installed version。Ready/ReadOnly/Unavailable/Unobserved 的原语义保留；大 reason 在 clone 前检查并从该 page decode 工作集扣除。StateStore/decoder 失败传播，不当作 readiness 不可见来吞掉。

SHOW 先稳定排序 thin target 的 namespace/name，再逐一 fresh-load 并直接 append 现有 LocalTableBuilder，保留原排序且没有完整 MvListRow Vec。变长 SQL/name/BaseTables/dependency/reason 在 domain row 构造前计逻辑字节和单行 workspace，join 一次预分配写入，不用 intermediate Vec<String>。information_schema 只保留有界的薄结果行，Vec growth/filter/sort headers 先计 workspace，compare/filter 借用文字，全部投影列一起预检（含重复列）后按已知字符串总长预分配 Arrow builder。

定向证据：完整 MV lib **284 PASS**（含新增 inventory 六项、readiness 四项）；完整 Frontend lib **1,420 PASS**（新增借用 row byte oracle、exact/cumulative refusal、borrowed compare、整结果重复列/列数拒绝）。日志 `logs/mem-1-m07/p06-mv-inventory-integrated-final-20261007.log` / `p06-mv-fe-integration-20261007.log`。只读复核未发现新 actionable finding。

两次测试首轮失败如实保留：readiness 测试误预期未变的重新安装会产生新 version，修正为原精确 token；分页 recording wrapper 将并发 create 返回时的回读算入 scanner，改为同一底层 store 的独立 mutation repository，保持 one-snapshot/range/abort 与成员断言未放宽。

## Dependency display 的独立源头

SHOW 的 manageable row 改用 required `list_dependencies_by_downstream_bounded`，没有 full-list 默认 fallback。一个 StateStore read transaction 先核对当前 downstream 的 exact version、raw/decode 预算和 canonical D，再逐 index record 解码/接管；比较 D 的借用 occurrences，不构造第二份 expected dependency Vec。缺失、重复 occurrence、非 canonical key/值都拒绝。canonical root 在 classification scan 前退出；所有预算、provider、codec、校验失败及成功都等待同一 abort，关闭失败也不发布结果。

classification 在同一 snapshot 内读完整 thin target/object identity inventory，以应用的 exact-object envelope + catalog scope 比较，不用 FQN 猜测。原 binding name、重复 relation 的独立 occurrence、`mv:` 前缀和 storage/object type 语义保持；unknown identity 留 unclassified。索引排序仍按原 display 字节和 occurrence 顺序，比较借用字节，不在 comparator 中构造 String。

生产 local 参数为：index ≤4,096 条 / 4 MiB collector（实际 ByteBuf/String capacities + allocation allowance + 双倍 vector capacity，覆盖 grow 或 stable-sort scratch），classification thin inventory ≤16 MiB，单次额外 decoded page ≤4 MiB，raw page/cursor 合计 ≤1 MiB，单 name ≤65,536 bytes、continuation token ≤4,096 bytes。借用 V3 dependency Avro 预检在 owned Value/ByteBuf 前拒绝巨大长度和 decode 超限；格式与 schema 未改。新 range helper 校验 continuation 指向准确末记录、key 严格向前且在范围内；cursor 只复制自身 bytes，不保留 provider 大 backing。这些额外工作属于 Local 的 workspace，主 SHOW inventory 和当前 ready model 仍各由 snapshot/decode share 持有。classification/临时 root 退出后才 join，4 MiB collector 的名称加 separators/prefix 低于另一个 4 MiB；无第二个全量 String Vec。

InMemory 是 test-only repository：借用已有 canonical state，在 clone 前检查有限 collector 和 thin inventory；不宣称 mock 对编码前 provider 分配给出生产 predecode 证据。StateStore 是生产实现。planning/DDL/background 原 dependency read 合同保留，与 local display 入口分开。

新增验证覆盖：与旧 classification/sort oracle 相等（重命名 binding 仍按 exact object 标记 MV）；entry/collector/raw/decode 超限在对应页停止；不完整 classification 不降为 external；stale downstream token 拒绝；在第一页后并发删除 upstream，index/classification 仍见原 snapshot，下一次读见 table；缺 occurrence 拒绝；错误 continuation、provider range/abort 失败不发布结果；readiness 与 installed token 门；Avro huge length/refusal 和借用 comparator 顺序。MV lib **294 PASS**，Frontend lib **1,421 PASS**（含展示字符串 exact/one-byte-short/empty 反例）；最终日志 `logs/mem-1-m07/p06-mv-dependency-bounds-final-20261007.log` / `p06-mv-dependency-fe-final-20261007.log`。diff/fmt 检查通过，只读复核无新 actionable finding。

## 尚未闭合

Local collector/renderer 的 whole-window 生产 funding、实际 capacity 峰值与 1FE+3BE/SOCKET 验收仍留在 P07/P08/P09；本切片不声明整个 96 MiB 物理包络或 P06 全链产品验收。
