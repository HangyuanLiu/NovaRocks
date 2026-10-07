# P06 Hadoop filesystem listing 源头（2026-10-07）

使用 OpenDAL 公开 lister/limit 流取代完整 list Vec；借用过滤、sorted dedup 后在 String copy
与 Vec reserve_exact 前检查实际 capacity 与 old+new peak。冻结 workspace 32 MiB 不变。
namespace marker、层级与 fallback 完整枚举共用 remaining workspace；root namespace output bound
不约束内部 table fallback 的条数/name bound，后者使用 V1，保留原有 lower caller bound 语义。

普通 delegate 与 admitted read 两条生产路径按 exact provider binding 重建 bounded FileIO。
公共 new(custom FileIO) 没有可证明的 source owner，namespace/table listing 事前 Unsupported；
new_with_binding 的 listing 使用 binding，外部 custom/decorator IO 若与 binding 不一致会改变行为。
仓库生产构造全部用同一 binding 创建 native FileIO；未找到此类外部 custom 调用。

HEAD/listing/path resolution 错误不复制任意 SDK Display；最终 Hadoop bounded listing projection
只借用 error.message 并在复制前检查 4 KiB，超界使用固定 diagnostic，保留原 typed read 分类及
ResourceExhausted/Cancelled/DeadlineExceeded。source/context 不被格式化。普通 catalog mapper 保持。

验证：Iceberg connector 全 lib 1176 PASS，日志
`logs/mem-1-m07/p06-hadoop-listing-qualified-final-20261007.log`。
包括 grow peak、stream过滤/超界、namespace marker/层级/fallback、root=1且内部2table、qualified name、
custom IO拒绝、超大/panic Display不调用和4KiB UTF8/control error边界。
初次运行的entries diagnostic与S3 path diagnostic断言失败已修正并在上述全库复跑通过；后者的exact
credential cause仍保留在source，诊断顶层改为固定文本。曾有String不能with_source的编译失败，
修正为移动既有String进anyhow::Error::msg，没有额外format复制。

page limit 仅约束请求，不证明忽略 limit 的远端 response body/XML decode 有硬界。
P09原生1FE+3BE、external fixture与Local whole-window backing/alias完整包络尚未验收。
