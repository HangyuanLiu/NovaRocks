# P04 原授 HeaderMap 存储

本切片实现显式 opt-in 的 `HeaderMapAllocationPool`。它事前计算 pool Core/Arc 和最多 `max_maps` 份完整 typed backing：索引、普通 entry、重复值。构造/复制在任何存储分配前取得同一原授池的位置，固定 map 不增长，也没有 heap fallback。键容量沿用 HTTP 原有 power-of-two/usable-capacity 几何；重复值容量准确固定。

`try_clone` 保持 eager copy，先取得位置再分配；泛型 `HeaderMap<T>` 的逐值 Clone、副作用、Cell 隔离和 panic 清理保持。HeaderName/HeaderValue payload 及其 Bytes promotion metadata 是独立 owner，不属于此存储 bound。没有 COW 或 sensitivity 跨 clone 共享。默认未附池的 map 沿用原有构造/增长。

满 key 容量时，已有 key 的替换、Entry 和合法重复字段仍可执行。新 key、重复值、reserve、copy 的耗尽在增长前拒绝。不可失败 API 明确 panic；`insert_mult`/`remove_entry_mult` 的独立 ValueDrain Vec 从原池保守占另一完整位置，耗尽先于 mutation；Native 接线不能在未预留位置时使用这两个不可失败入口。

Map 和 IntoIter 的三个 backing 先于位置/原 owner 退出。IntoIter 使用 layout 相同的 `ManuallyDrop` slots，读仍初始化的链接并只移出每个 value 一次，Debug 不读取已移出的字段；一个 payload destructor panic 后，Finish guard 继续退休其余值，避免原 raw-read 路径的 double drop。实际多值替换另修正旧 `links.take()`：必须保留链头到逐项 unlink 完成，最后一个 extra 自动清空 entry link；两重复值的旧路径会断言失败。

验证由公开实际 HTTP API、真实 Worker 原 wallet/System allocator 和独立完整 HTTP/Bytes normal dependency 组成；后者没有替代 map、源码测试算法或上游 dev dependency 下载。普通/Miri 各 7 项通过。`reproduce.py --miri` 校验全部源文件 SHA 和 3 个生产锁定 dependency identity 后从完整新 snapshot 重放。6 个 actual-source runtime mutants 均 101/test FAILED，分别覆盖 extra 未授增长、copy 绕过原授、IntoIter 提前退位置、panic cleanup 缺失、提前 unlink、ValueDrain 未授；每次 finally 字节恢复，恢复后普通测试再通过。

独立只读审核未发现池绕过或新的 unsafe blocker；追加核实了多值 unlink 修正。完整 Native+Worker lib 串行 574+313=887 项通过，相关旧公开 HTTP/入站 arena/table 共 7+9+8=24 项通过。workspace 全目标编译及 HTTP 严格 Clippy 通过；最终公开 allocator target、格式与 source pins 见 `verification.json` 和完整 gzip 日志。

失败历史保留：首次 scratch 未排除 dependency workspace members，offline resolver 请求未缓存上游 doc-comment；补 exclude 后不下载即可构建。首 driver 误用只对 HeaderValue 提供的 `new`，改 generic `default`。首次 nightly 因 `fetch_update` 新 deprecation 被 vendor deny(warnings) 拒绝，改稳定 compare-exchange loop 后 Miri 通过。第一版 cleanup mutant 只触发 unused helper 编译失败，未算 runtime 证据；修正后真实 6 个 runtime mutant 均失败。公开 fixture 初版将 custom &str→HeaderName 及首次 Bytes clone 的独立 payload metadata 混进存储量测；保留全部失败/cleanup abort 日志，准备这些独立输入后重跑。实际多值链断言另有产品修正和两 extra/Miri oracle，不能归为 fixture 问题。

这是 HTTP 库存储基础设施，尚未安装到 Native/h2 metadata producer、Hyper/Tonic copy/merge/status 路径；不声称完整连接 2MiB、stream/socket/task/TLS/deadline、lane/predecode/profile/V1 或 P04 完成。整个 M07 P00–P10 目标保持；Linux 验收由用户后续手动执行，Docker desktop-linux fixture BOM live 通过，无缺 image/JAR，无 push/PR/archive。
