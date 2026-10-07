# P06 MV local source 收敛

日期 2026-10-07。逐源修复记录，不代替生产 V1/1FE+3BE 验收。

## Codec 前置预检

在已核对 NRMA envelope、record kind、注册 schema ID/fingerprint 后，V3 projection 先以借用切片读完固定 Avro 结构，再对 D/L/P/C 作为一组执行原 protobuf structural preflight；此时没有 owned Avro Value/ByteBuf/Current model。外层无 collections，按四倍 payload + 4 KiB 固定 allowance 从同一 decode 工作集扣除，剩余才交给文档 codec 的六-copy/item/depth envelope。

`decode_projection_with_budget` 接收显式预算，完整语义与 exact source revision/CAS version 仍由原实现校验；没有修改持久化格式、schema 或版本。旧 projection reads 使用原 default ceiling；local caller 的更小预算接线仍待下一切片。

定向 StateStore repository 23 PASS；完整 MV lib 274 PASS。新增用例覆盖：合法 projection 4 MiB 预算 roundtrip；1-byte outer/document refusal；巨大正 Avro byte 声明在 owned decode 前以 truncated refusal 退出；outer + documents 共用一个工作集。日志 `logs/mem-1-m07/p06-mv-codec-{preflight,all}-20261007.log`。

## Inventory / readiness / local row 接线

两个 repository 直接实现显式 `MvProjectionInventoryBound`，没有 full-list 默认 fallback。StateStore 在一个 read snapshot 内以 page_size=1 读取，逐记录校验 raw bound 和较小的 complete decode budget，随即只留下 target/object identity；thin vector 增长含旧/新 backing 的重叠检查。正常、损坏、超限都等待同一 transaction abort，失败不返回截断 inventory。

local caller 使用 65,536 entries / 16 MiB snapshot / 1 MiB raw page / 4 MiB complete decode。readiness cursor 逐 target fresh-load 并核对准确 StateStore version；deleted target 跳过，replacement 不能继承旧 installed version。Ready/ReadOnly/Unavailable/Unobserved 的原语义保留；大 reason 在 clone 前检查并从该 page decode 工作集扣除。StateStore/decoder 失败传播，不当作 readiness 不可见来吞掉。

SHOW 先稳定排序 thin target 的 namespace/name，再逐一 fresh-load 并直接 append 现有 LocalTableBuilder，保留原排序且没有完整 MvListRow Vec。变长 SQL/name/BaseTables/dependency/reason 在 domain row 构造前计逻辑字节和单行 workspace，join 一次预分配写入，不用 intermediate Vec<String>。information_schema 只保留有界的薄结果行，Vec growth/filter/sort headers 先计 workspace，compare/filter 借用文字，全部投影列一起预检（含重复列）后按已知字符串总长预分配 Arrow builder。

定向证据：完整 MV lib **284 PASS**（含新增 inventory 六项、readiness 四项）；完整 Frontend lib **1,420 PASS**（新增借用 row byte oracle、exact/cumulative refusal、borrowed compare、整结果重复列/列数拒绝）。日志 `logs/mem-1-m07/p06-mv-inventory-integrated-final-20261007.log` / `p06-mv-fe-integration-20261007.log`。只读复核未发现新 actionable finding。

两次测试首轮失败如实保留：readiness 测试误预期未变的重新安装会产生新 version，修正为原精确 token；分页 recording wrapper 将并发 create 返回时的回读算入 scanner，改为同一底层 store 的独立 mutation repository，保持 one-snapshot/range/abort 与成员断言未放宽。

## 尚未闭合

SHOW dependency display 仍经旧 `list_ready_dependencies_by_downstream`，StateStore classified dependency path 先 full-scan rows/roots；这条独立源头要继续改。Local collector/renderer 的 whole-window 生产 funding、实际 capacity 峰值与 1FE+3BE/SOCKET 验收仍留在 P07/P08/P09；本切片不声明整个 96 MiB 物理包络或 P06 全链产品验收。
