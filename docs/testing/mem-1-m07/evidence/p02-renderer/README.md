# MEM-1 M07 P02 renderer C2 证据

2026-10-01，基于 P01 检查点 `555b294cc` 的冻结合同完成纯 `ArrowMysqlTextEncoder`。源码仅新增 `result-render` encoder/helper/tests；公共 `lib.rs` 导出和 `chrono` 依赖由主 agent 接入。本证据是 P02 纯库 C2，不表示 P04/P07/P08 已接生产，也不是 P09 产品性能验收。没有修改 SQL golden，没有 commit/push；Linux 验收由用户后续手工执行。

## 验证收据

| 检查 | 命令 / 文件 | 结果 |
| --- | --- | --- |
| 首次编译 | `cargo check -p novarocks-result-render`；[first-check.log](first-check.log.gz) | PASS |
| 最终 C2 | `cargo test -p novarocks-result-render -- --test-threads=1`；[c2-tests.log](c2-tests.log.gz) | 27 PASS：3 unit、4 actual allocator/lifetime、20 independent bytes/limits integration；0 ignored |
| 主 agent 独立复跑 | 同一 C2 命令；[main-c2-tests.log](main-c2-tests.log.gz) | 27/27 PASS |
| 严格 clippy | `cargo clippy -p novarocks-result-render --all-targets -- -D warnings`；[clippy.log](clippy.log.gz)、[main-clippy.log](main-clippy.log.gz) | 两次 PASS，没有 allow 新 warning |
| 定向格式 | `rustfmt --edition 2024 --check` 对本次六个新 Rust 文件；[fmt.log](fmt.log.gz) | PASS，空输出 |
| 差异格式 | `git diff --check`；[diff-check.log](diff-check.log.gz) | PASS，空输出 |
| 实际 scratch | `cargo test -p novarocks-result-render --lib actual_scratch_capacity_layout -- --nocapture --test-threads=1`；[scratch-capacity.log](scratch-capacity.log.gz) | 187,696 bytes |

当前 64-bit Rust 布局测得 `scratch = 65,536 staging + 16,384 cell lengths + 196 × 536 Task = 105,056 stack + 720 control = 187,696 bytes`。三个固定 heap allocation 合计 186,976 bytes；720 bytes control 包括 move-owned encoder 状态。运行时 oracle 取 `size_of` 与 `Vec.capacity()`，不能把这次编译器布局数字当跨平台 ABI。完整 scratch 小于 profile 2MiB，schema 与原始 input backing 的容量另由 host 保护。

allocation probe 在预先建立 schema/input/output 后测量：constructor 恰好三个固定 allocation；4MiB String、超过 T 的 JSON、nested lossy Binary、合法最大 Native depth、日期/时间与 Variant tag12（包括 odd-second offset）的所有 `step` 都零 Rust heap allocation。scratch 在推进、深层栈和 `cancel` 后不增长。Weak input oracle 证明 `cancel` 后 input 仍存在，`drop(encoder)` 后实际最后 backing 引用才退出。

## 接口与调用者义务

公开构造签名为 `ArrowMysqlTextEncoder::try_new(Arc<ClientRenderSchema>, RecordBatch) -> Result<Self, RenderError>`，实现已有 `BoundedMysqlTextEncoder`：`step(&mut [u8])`、`cancel()`、`scratch_capacity_bytes()`。构造准备时核对准确 occurrence、carrier、nullability 和呈现能力；保留重复/重排列 occurrence，不以列名或 Arrow runtime width 猜逻辑语义。

`step` 的输出仍是未发布 segment；遇到 Err 必须丢弃这次未提交的输出，不能发布部分成功前缀。host 必须在 original input、hydrate overlap、schema、scratch 或 segment 创建/复制之前取得完整 pregrant，且保留到真正最后 owner/alias 退出。库不取得输入 permit，不实施 hydrate，不创建 socket/packet，不 spawn，不提供 allocator/MEM 钱包。`cancel` 只封推进并清遍历 alias，仍持 schema 与 batch；不是 capacity release 或 producer actual exit。P04 必须 drop encoder 后再报告真实退出，P07 管理 MySQL packet 与 delivery 前沿。

每次 turn 的 `emitted_bytes + examined_bytes <= 65,536`，`visited_cells <= 1,024`；examined 包含 count、escape/JSON/metadata scan、fixed atom work 与 staging 搬移，不能只看输出量。`completed_rows` 是本次 turn 完成的行数增量。元素上限按同一 immutable row 的语义节点计数，跨 occurrence 累加；small 尝试重启 count 与后续 emit 时重置该语义计数，CPU quantum 仍计算实际重访。错误标注 output occurrence；值级 malformed/out-of-range 在整行未发布 small staging 或完整 count 中失败。

小行 ≤64KiB 在固定未发布 staging 单遍处理，float 小行仅格式化一次。每个 cell 先留一字节占位，完成后 checked 确定 canonical lenenc prefix，有限分块单次搬移该 cell；不反复移动已有完整行。超过 T 立即撤销未发布尝试，用原始 immutable 值 resumable count→emit；不构造完整大行/cell String。固定 cell-length 表供 emit，rowTotal 是准确 u32。首 prefix 与第一 lenenc/NULL payload byte 同一步提交；输出余量 1..4 返回 NeedsOutput，短 continuation 合法，不补 padding。raw/atom/escape 与 metadata 游标原地或有界批量推进，避免每字节复制整个 Task。

## 支持矩阵

| 冻结语义 | 顶层 MySQL text | MysqlContainer 祖先内 |
| --- | --- | --- |
| null / opaque | 0xfb；opaque 只依据 OpaqueNull | `null`；不用 aggregate 列名猜 opaque |
| bool / signed / unsigned / LargeInt | bool 0/1，准确位宽整数、Fixed16BE i128 | 同一数值字节，非引用文本 |
| float32/64 | 原 Rust Display，包括极值/subnormal/-0/NaN/Inf，固定 512-byte atom | 同一数值字节 |
| Decimal128 | 原准确 precision/scale，unsigned magnitude，不经 f64 | 同一准确小数 |
| Decimal256 | 准备阶段 UnsupportedPresentation，保留旧顶层集合 | 准确 limb 十进制输出，保留既有 nested 支持；不调用有 BigInt allocation 的 i256 Display |
| Date32 | 真实 year=-1, month=11, day=30 sentinel→0000-00-00；其他 checked Gregorian | 同一 sentinel；引用文本；chrono-style 负/extended year |
| TIME | 原微秒政策，负值明确拒绝；ns 截断至 micros | 原 sign/unsigned magnitude，引用文本，保留 negative TIME |
| TimestampUtcMicros | UTC、无 tz 后缀，fraction 非零时六位，ns 截断至 micros | 如显式声明则按该 presentation |
| TimestampContainerText | 产品 freezer 只用于 nested | unit 决定 0/3/optional6/9 位，引用文本，保留准确 timezone 字面后缀 |
| String / JsonText | Utf8 或 LargeUtf8 原 bytes；JSON 有界语法检查 | String 双引号 escape quote/backslash；JSON 单引号 escape apostrophe/backslash，不凭 Arrow metadata 猜 Json |
| Binary | 原 bytes | resumable UTF8-lossy 与双引号 escape |
| VariantSerializedBytes | 原 serialized bytes，保留旧 pass-through；不声称结构解析 | 仅按显式 presentation |
| VariantJson | 显式 frozen offset 的零拷贝结构解析 | 产品 nested Variant 使用；tag12保留 offset，18/19既有 unsupported；metadata/offset malformed 明确失败 |
| List / Map / Struct | 准确 child schema 的容器 text | 空 `[]`/`{}`、nested null；Map 插入序，Struct 名称准确 escape；不 sort |

普通平铺 carrier 与 `Dictionary(Int32, Utf8|LargeUtf8)` String/Json 可直接解析，无展开 copy；List/LargeList 和 Utf8/LargeUtf8 是显式 runtime 能力，未把 carrier 编码写入 Native 类型。其他 dictionary/view/RLE 明确 UnsupportedCarrier，需由 Execution 显式有界扩展/hydrate，不能 fallback。

VariantJson 刻意保留旧 `escape_json_string` 的逐 UTF8-byte→Unicode char 映射，因此中文仍有旧 mojibake bytes；ASCII/中文、object key/value 均有 independent expected bytes。修正 Unicode 属于另行授权任务。FE freeze一次 `timezone_offset_seconds`，纯库不读取 Local/timezone/clock；±30/±59 seconds 复现 chrono `%:z` 的分钟 rounding。代码不用含 String 的 DelayedFormat Display / DateTime offset formatting，探针证明上述路径零 allocation。

主 agent 明确裁决了两项局部准确性修正：Date32 sentinel 使用真实 Gregorian 日期判定，不能把 days=-719223 误作 sentinel；有效 negative millis/nanos container Timestamp 使用 checked Euclidean 算术，不能猜 epoch fallback。Variant Date 从 Gregorian 日期独立转换，不套 Date32 sentinel。均有 literal expected tests，不改 golden。

## 对抗覆盖与修复收据

C2 独立预期包括 T−1/T/T+1 精确路径、cell lenenc 250/251/65535/65536/U24 边界、64MiB row exact/+1、output0..6与合法一字节续段、4096列、多小行 packing、累计元素 exact/+1与重复 occurrence、Native63-container+scalar合法 schema / dynamic64-container / mixed深度边界、JSON >1024 values 跨 turn、count==emit、nested empty/null/Decimal256/日期/时间/Variant、取消和 physical input exit。

早期只读 review 和首次定向测试闭合了以下具体缺口：JSON object identity 与期望状态分开保存；JSON 在 cell quantum 满时先 yield 再 feed，不能只回退 byte index；large prefix 延后到真实首 payload 已准备，避免 count→emit 在 quantum 尾部只发4 bytes；physical stack按最大 semantic depth 的保留帧取196，而非68；fixed scalar atom扩大到512以覆盖 f64 Display 极值；Date32/Variant sentinel、负/extended Gregorian year 与 odd-second offset分开准确处理。单测预期 subnormal 先前手写零数不足，改成独立 323 个零的固定构造。严格 clippy 的两个 collapsible-if 和测试不可见 soft-hyphen 均已修正，没有 suppression。

P02 没有执行 Native 1FE+3BE、SQL、全 workspace CI、Linux 或 P09 throughput/goodput/CPU 验收。pure micro test耗时只是局部实现反馈，不能替代产品性能门。P04/P07/P08 接入与外部 holder/queue/pregrant 证明仍由后续 owner 完成。

日志使用 gzip 原样归档，保留原始输出末尾换行；index 同时记录 compressed 与原始 SHA256。
