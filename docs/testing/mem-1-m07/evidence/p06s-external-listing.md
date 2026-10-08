# MEM-1 M07 P06s：外部 SDK 列表的受信端点约束

2026-10-08；消费 spec/plan 第 7 版 D15、D16、§2.9、C6s、V11。基线 `c313bc1bbae32ce94510918f13c675ff650708b2`，独立任务分支 `codex/mem-1-m07-v7-listing`。这是 P06s 实现与定向检查收据；P09 CL、原生 1FE+3BE 和最终 workspace 收敛仍由主任务执行。

## 实现与所有权

Iceberg 和 Paimon 各自维护 catalog generation 所有的 8 位置准入，不合并调用。等待准入与 SDK 工作都使用实际 `ConnectorRequestContext` 的绝对 deadline/stop；到期返回 `DeadlineExceeded`，停止返回 `Cancelled`。SDK future 的作用域先退出，随后才归还位置。Iceberg 的位置保守地覆盖完整分页/流式列表流程；Paimon 的位置也覆盖 SDK 返回后的有界 retain。新测试在 SDK future 的 Drop 内核验位置尚未归还，并确认满池调用不会 poll SDK。

REST namespaces/views/tables 均由 NovaRocks 循环调用单页接口。每页先借用 SDK 项检查页数、累计条目、名字字节、token 长度、全部历史 token；表标识另检查重复/越域，视图标识检查越域，再复制 NovaRocks 名字。`a→b→a` 在第四个请求之前准确拒绝。忽略 `pageSize` 的服务端仍可返回超过 256 项的最终页，但只有整页在剩余条目/名字边界内才接受；带 continuation 的超请求页拒绝，失败不返回部分列表。历史 token 集最多 1,024×4,096 字节，加有限集合元数据；没有无界历史。

REST 客户端仅使用公开 `with_client`：连接期限 5 s、读取期限 30 s，不设整体 client timeout；整体时间由调用方绝对 deadline 控制。HMS 保留 `get_all_*`、现有 framed 开关与 volo 默认 16 MiB frame，不增加 HMS patch。Hadoop 读取列表同样使用实际 binding 的 request context 与共享 catalog gate。

所有对象存储 endpoint 安装公开 `HttpFetch` 包装，远端保留 OpenDAL 默认客户端，本地保留 `no_proxy`。只在 `Operation::List` 按实际响应体累计字节，超过 16 MiB 的 chunk 在进入 SDK `read_all`/XML 前拒绝；错误非 temporary，RetryLayer 不重试。Read 与没有 Operation 扩展的请求不受这个 cap 影响。凭证获取仍使用独立 authority 客户端。新钉住测试同时依赖公开 HttpFetch 签名、HttpClientLayer 与实际 List Operation 扩展，公开接缝变化会失败。

FS 最小公共 `map_object_store_listing_error` 按 typed source 识别 List overflow，保留 `ResourceExhausted`，不匹配错误字符串、不复制 opaque SDK 诊断。ADD FILES、CTAS、Hadoop raw lister 都使用它，不能把超界降为 Internal/Unavailable。FS 普通 map、流错误 map 与 bounded map 的分类也一致。

ADD FILES 改为流式 `lister_with`，所有物理条目（含隐藏项）先计条目与名字/工作区，合法文件不超过 4,096，拒绝后不提交截断列表。CTAS 清理流式遍历，最多 256 项一个删除批次，整体 discovery 仍受 V1 边界约束；marker/root 最后删除。第一次删除之前列表失败保留准确错误，已产生删除作用后保留原 `CommitUnknown` 语义与 marker 供重试。Paimon 存在性判断消费准确列表结果，列表错误不再变成 false。

Frontend 三组真实入口反例覆盖 DROP DATABASE FORCE、information_schema 与 SHOW VIEWS。DROP 的 namespace/table/view 枚举失败时所有 destructive mutation counter 均为零；information_schema 在第一库成功、第二库失败时整个查询失败；SHOW VIEWS 经实际 view engine。三种错误均检查完整 ConnectorError 文本。Frontend 既有 API 返回 String，证据只证明该边界准确文本传播，不声称结构化 kind 穿过 String API。

## 参数与 D15 边界

| 参数 | 值 |
|---|---|
| 每 catalog generation 列表并发 | 8 |
| REST connect/read timeout | 5,000 / 30,000 ms |
| REST client 整体 timeout | 无；使用 request 绝对 deadline |
| REST 每页请求项数 | V1：256 |
| OpenDAL List 实际响应体 cap | 16,777,216 bytes；非 temporary |
| 列表 V1 | 65,536 项；1,024 页；单名 65,536 bytes；累计名字 16 MiB；token 4,096 bytes |
| ADD FILES 合法文件数 | 4,096 |
| CTAS 删除 batch | 256 |

这些参数由主 agent 在本次验证之前写入共享 profile，测量状态保持未测量。Admin 配置的 catalog/metastore/object-store endpoint 是 D15 受信端点；REST/HMS/Paimon SDK 反序列化、HTTP/Thrift/对象存储内部分配仍为第三方增长。List cap 限制输入实际 body，不证明 transport chunk、XML 或 SDK 分配事前硬内存上界。HMS pilota 按声明长度预分配的已知缺口仍保留，P10 ADR 必须记录并跟踪上游。不得将这些测试改述为所有 SDK 字节受硬界。

## 必要文件范围扩展

没有改共享 SPI/query 入口、mysql-adapter 或新增第三方 fork/patch 清单。为携带真实 context 并让 gate 覆盖现有 production consumer，增加 provider-private trait/context 接点：`access_binding.rs`、`catalog/{mod,hadoop}.rs`、`catalog_control/{views,data_mutation}.rs`、`document_storage/discovery.rs`；对应 factory/admission spy 测试显式附上实际 context，旧 consumer 搜索没有用无限期限 default 掩盖生产调用。Paimon `resources.rs` 将 generation gate 注入 request-local catalog，`role_binding.rs` 消费存在性错误。

`fs_io.rs` 是 Hadoop raw lister 保留 typed overflow 的最小 source seam；`fs/lib.rs` 导出分类函数，`fs/Cargo.toml` 只增加已有 workspace http 依赖。Iceberg 把已有 reqwest 从 dev dependency 移到 normal dependency，使公开 with_client 的 production 构造可编译，版本不变。root Cargo.lock 只为 novarocks-fs 增加已有 http 依赖边，没有新的包版本或 patch；该公共 FS seam/normal dependency 调整由 main 在最终候选上跑 workspace 收敛。

vendored REST standalone Cargo.lock 原先选 registry iceberg 0.9.1，忽略 CLI sibling 0.9.0 patch，导致既有测试与 frozen sibling API 不兼容。独立锁修复只锁住当前 sibling iceberg 0.9.0 的可复现测试前提；root 正常消费同一个 patched 0.9.0。两条既有 transaction 测试补 await 并按 frozen-base 不 reload 设 GET expect(0)，POST/404 断言保留。client.rs/types.rs 只按 rustfmt 修复既有格式，以完成 C6s；不扩大 D16 产品补丁语义。

## 检查收据

源码编辑静止后串行执行，使用 `CARGO_INCREMENTAL=0`。独立 REST crate 位于自己的 manifest 目录，使用 `CARGO_TARGET_DIR=<worktree>/target` 与 `--offline --locked --config 'patch.crates-io.iceberg.path="../iceberg-0.9.0"'`；这是选择既有 sibling patch 的测试参数，没有增加产品 patch 清单。

| 检查 | 准确结果/日志 |
|---|---|
| vendor `cargo test --lib` | 58 PASS；`/tmp/mem1-p06s-vendor4.log` |
| vendor `cargo fmt -- --check` | exit 0；`/tmp/mem1-p06s-vendor-fmt.log` |
| vendor `cargo clippy --lib` | exit 0；既有 `server_properties` dead_code warning 1；`/tmp/mem1-p06s-vendor-clippy-lib.log` |
| vendor `cargo clippy --all-targets` | exit 101；`/tmp/mem1-p06s-vendor-clippy.log`。lib 与 lib-test target 无新 warning，all-targets 被既有 integration 测试 E0432 阻挡 |

all-targets 首错是 `tests/rest_catalog_test.rs:31` 的未声明 `iceberg_test_utils`；`:396-397` 还使用 sibling 已变为 async 的旧 `.apply(tx).unwrap()`。对比基线确认 integration 源与 manifest 完全未改（源码 SHA256 `ad9a9ce4a10ee92f196d6882ccda121f9f65f8716f84344b3d20670d3b2a40ca`；manifest `79cfa175bf3c6efa267cba85c0fb3f7e8357a8cc0ad72b598ea4190b49c52380`），manifest 只声明 mockito/tokio dev dependency，没有 test-utils。核对 `/tmp/mem1-p06s-vendor-clippy-baseline.txt`。主 agent 明确保留此基线限制，不扩 SDK patch/新第三方 fork，也不禁用自动 integration tests 掩盖失败。故 C6s 不能称全部通过。

| 最终准确树第一方检查 | 结果 |
|---|---|
| `cargo test --locked -p novarocks-connector-iceberg -p novarocks-connector-paimon -p novarocks-fs -- --test-threads=1` | exit 0；Iceberg lib 1,192 + integration 3 PASS / 1 既有 ignored，Paimon lib 61 + integration 25 PASS，FS lib 149 + integration 62 PASS；合计 1,492 PASS / 0 FAIL / 1 ignored；`/tmp/mem1-p06s-crates-final.log` |
| `cargo test --locked -p novarocks-frontend-application listing_errors -- --test-threads=1` | exit 0；3 PASS，1,437 filtered；`/tmp/mem1-p06s-frontend-final.log` |
| `cargo fmt --all -- --check` | exit 0；`/tmp/mem1-p06s-root-fmt.log` |
| `git diff --check` | PASS |

最后一轮包含 public typed FS map、ADD FILES/CTAS/Hadoop raw-lister 使用它，以及 REST borrowed-name copy。第一方 lib/integration tests 没有失败；没有执行真实 fixture integration、workspace 全量或 P09 测量。旧 `/tmp/mem1-p06s-crates4.log` full package PASS 先于最后 typed raw-lister 映射，不代替此最终收据。

日志 SHA256（准确检查后冻结）：

| 日志 | SHA256 |
|---|---|
| `/tmp/mem1-p06s-vendor4.log` | `082f250cc57f5f39e81e027dc530870649736c47d49f20ad5b900bf2c0792088` |
| `/tmp/mem1-p06s-vendor-fmt.log` | `e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855` |
| `/tmp/mem1-p06s-vendor-clippy.log` | `f90aa62f6f92d894563e07efc6a017df870bdd274587366e04e86e03788d90e1` |
| `/tmp/mem1-p06s-vendor-clippy-lib.log` | `e429a3caffc8d435c3bb2c119ee5c5c7aa45925f4a915278171a2b25f96bbf28` |
| `/tmp/mem1-p06s-vendor-clippy-baseline.txt` | `9f1ef015d1ca78dc3b215bc9a0addd968d9b81fb974190c10738820e370ce1f1` |
| `/tmp/mem1-p06s-crates-final.log` | `6a9c0fab74c6ed781910cb99048037da0bc679b163f493736e89636100a2c9ce` |
| `/tmp/mem1-p06s-frontend-final.log` | `29aed75f080e463d030c7c8656be63e805da07884897e769fb037c33438769de` |
| `/tmp/mem1-p06s-root-fmt.log` | `e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855` |


## P09 CL 场景输入（执行前冻结）

建议固定正常场景 N=32 个 namespace，每库 M=512 个 table 和 512 个 view，命名 `cl_ns_%04d` / `cl_table_%06d` / `cl_view_%06d`，按 256 每页服务；每库表/视图各 2 页。以实际 SQL information_schema、SHOW VIEWS、DROP DATABASE FORCE 的独立 catalog 副本与 MV lake 重建发现运行，分别记录接收/保留条目、名字 bytes、页数和调用期间 jemalloc 高水位。记录 1/8/16 并发调用，验证 gate 最大活跃 SDK 工作为 8。HMS/Paimon 不支持 view listing 时保留 Unsupported，仅运行各自支持的 namespace/table 场景。

超界场景固定单库 65,537 个 table，另设单个忽略 pageSize 的 terminal response 返回 65,537 项，保证整批失败；保留 65,536 项 exact-bound 对照。名字超界独立注入累计 16 MiB+1 字节，token 注入 4,097 字节与 `a→b→a`，页数注入连续 1,025 页。OpenDAL List 实际 body 注入 16 MiB+1，并核对 endpoint 请求次数为 1。期限场景在第一个列表响应前延迟超过实际 request deadline，并注入显式 stop；失败后复用全部 8 位置，DROP 失败场景核对首次删除前 mutation=0。

以上 N/M、路径与注入方式供 main 在 P09 前冻结；当前只已执行本地 TCP/mockito/Memory OpenDAL 单元反例，没有执行大 catalog jemalloc 测量或真实 1FE+3BE SQL。CL 不设字节通过门，若列表峰值成为 FE 内存主要来源则按 D15 返回设计重审，不能事后放宽 cap。

主agent集成：原模块commit `576eec30f` 已 cherry-pick 为 `41481781a`。上述8份原始日志
逐字节复制到主worktree `logs/mem-1-m07/p06s/`（沿用原basename与SHA256），不只依赖/tmp。
coverage/profile按D15/D16整合；集成workspace与CL/native测量待完成，没有把模块证据升级成终态。
