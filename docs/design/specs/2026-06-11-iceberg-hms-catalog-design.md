# Iceberg Hive Metastore（HMS）Catalog 支持 — 设计

- 日期: 2026-06-11
- 状态: 设计已评审，待写实施计划
- 范围标签: iceberg, catalog, connector, hive-metastore, standalone

## 1. 背景与现状

NovaRocks standalone 模式已支持三种 Iceberg catalog 类型，均收敛在
`src/connector/iceberg/catalog/registry.rs`：

- `hadoop`（默认）：自写的 `HadoopFileSystemCatalog`（`src/connector/iceberg/catalog/hadoop_catalog.rs`，
  ~430 行），实现 `iceberg::Catalog` trait，用 `version-hint.text` 管理"当前
  metadata.json 指针"，真正的 metadata 读写走 `file_io`（`S3StorageFactory` / `LocalFsStorageFactory`）。
- `memory`：测试用，复用 Hadoop 实现。
- `rest`：使用 vendored 的官方 `iceberg-catalog-rest 0.9.0` crate，
  通过 `RestCatalogBuilder::default().with_storage_factory(...).load(...)` 构造，
  注入 NovaRocks 自己的 `S3StorageFactory`。

**缺失**：`iceberg.catalog.type = hive`（Hive Metastore）。当前
`build_catalog_entry`（registry.rs:1308）对 `iceberg.catalog.type` 只接受
`memory|hadoop|rest`，其余直接报错。

### 1.1 关键集成事实（已验证）

- **唯一集成点**：所有引擎 DML/DDL 流程都经
  `build_iceberg_catalog(&entry) -> Arc<dyn iceberg::Catalog>`（registry.rs:1597）
  取用 catalog —— mutation_flow / delete_flow / iceberg_writer / iceberg_truncate /
  equality_delete_flow / iceberg_maintenance / compact / analyze / views /
  schema_update / mv 等 20+ 处已统一到它。因此**只要新增一个能产出合规
  `iceberg::Catalog` 实例的 catalog kind，整条读写路径自动点亮**，无需改任何上层流程。
  （少数遗留点 `engine/mod.rs:6911,8295`、`catalog/add_files.rs:63` 仍直接调
  `build_hadoop_catalog`，与 HMS 无关，不在本次改动范围。）
- **storage factory 注入是 trait 能力**：`with_storage_factory(Arc<dyn StorageFactory>)`
  是核心 `iceberg::CatalogBuilder` trait 的必有方法（vendor/iceberg-0.9.0/src/catalog/mod.rs:168），
  REST 与 HMS 的 builder 都实现该 trait，故 HMS 可与 REST 同样注入 `S3StorageFactory`。
- **没有 embedded JVM**：代码里不存在任何 jni/j4rs/jvm 设施；CLAUDE.md/AGENTS.md
  中 `[iceberg]` "Embedded-JVM" 描述与实际不符（实际只有 standalone maintenance 配置项）。
  因此"走 Java Hive client"不可行。
- **Thrift 现状**：`thrift = "0.17"`（同步 Apache Thrift）仅用于 StarRocks FE/BE 协议，
  与 Iceberg 无关。

## 2. 目标与范围

让 NovaRocks 支持 `iceberg.catalog.type = hive` 外部 catalog，**读 + 写全功能对齐**
现有 Hadoop/REST catalog：`SELECT` / `CREATE TABLE` / `INSERT` / `INSERT OVERWRITE` /
`UPDATE` / `DELETE` / `MERGE` / `DROP` / compaction / MV 刷新全部点亮（凭借 §1.1 的单一集成点）。

### 2.1 非目标（v1 边界，YAGNI）

- **鉴权**：仅明文 thrift（无鉴权）。Kerberos/SASL、PLAIN/LDAP 列为后续。
- **namespace**：HMS database 单层 = Iceberg 单层 namespace。多层 namespace 不支持。
- **HA**：`hive.metastore.uris` 多个逗号分隔地址时，v1 取第一个；多 metastore 故障切换列为后续。

以上不支持的语义，在 catalog 构造阶段 **fail fast 显式报错**（遵循 CLAUDE.md "fail fast on
unsupported or ambiguous semantics"），不做"尽力而为"。

## 3. 方案决策

| 决策点 | 选择 | 理由 |
|---|---|---|
| 功能范围 | 读 + 写全功能 | 经 §1.1 单一集成点，写能力一次到位，所有 DML/DDL 自动点亮 |
| 鉴权 | v1 明文 thrift | Kerberos 需 SASL/GSSAPI 传输层 + keytab，独立大工作量，留后续 |
| 测试 | 独立 HMS 容器 + 两个 suite（含跨引擎） | 与现有 `iceberg-rest` + `iceberg-compatibility` 测试方式对齐 |
| HMS 通信层 | **方案 A：官方 `iceberg-catalog-hms` crate** | 见 §3.1 |

### 3.1 通信层为何选 A（用 crate）而非 B（手写同步 thrift）

候选：A = vendored/直接依赖官方 `iceberg-catalog-hms 0.9.0`；B = 手写同步 Thrift HMS
客户端 + 自写 `HmsCatalog`（复用 `thrift=0.17` + build.rs codegen + frontend_rpc.rs 范式）。

选 A 的决定性理由：

1. **commit 协议正确性**：写能力的核心难点是 HMS `lock` + `alter_table` 原子换
   `metadata_location` 的乐观并发提交协议。这是最易埋并发 bug（丢提交 / 覆盖）、最难测的一环。
   上游 crate 已实现并实测；手写需自己保证正确性。
2. **storage factory 注入已确认可用**（§1.1），曾经选自写 `HadoopFileSystemCatalog`
   的最大顾虑（注入 + metadata 命名）在 HMS 不复存在 —— HMS 用标准
   `{version}-{uuid}.metadata.json` 命名，无 Hadoop `vN` 怪癖要绕过。
3. **与 REST 决策一致**：项目已确立"用官方 vendored catalog crate"路线（REST 即如此）。
4. **原生 async**，贴合 `iceberg::Catalog` async trait，无 sync→async 桥接问题。

A 的代价（已认可）：拉入整套 volo 异步 Thrift 生态作为传递依赖（见 §4.1），
增加编译时间与依赖面。这是 build 成本，非正确性/架构契合度问题。

## 4. 组件与代码改动

全部 catalog 侧改动收敛在 `Cargo.toml` + `src/connector/iceberg/catalog/registry.rs`，
外加 docker 夹具与测试。上层引擎流程**零改动**。

### 4.1 依赖（`Cargo.toml`）

- 新增 `iceberg-catalog-hms = "0.9.0"`，**直接 crates.io 依赖，先不 vendor**。
  - 其 `iceberg ^0.9.0` 传递依赖会被现有 `[patch]`（指向 `vendor/iceberg-0.9.0`）
    自动重定向；vendored iceberg 的可见性补丁是叠加式，与 crate 期望的 0.9.0 API 兼容。
  - **仅当**后续需要打补丁时才转 vendor（与 REST 同策略）。
- 接受的传递依赖：`hive_metastore 0.2`、`volo`、`volo-thrift`、`pilota`、
  `motore-macros`、`metainfo`、`linkedbytes`、`faststr`、`tokio` 等。

### 4.2 catalog 类型与解析（`registry.rs`）

- `enum IcebergCatalogKind` 增加 `Hive` 变体（含 doc 注释说明语义）。
- `struct IcebergCatalogEntry` 增加 `hms_uris: Option<String>` 字段（类比现有 `rest_uri`）。
- `build_catalog_entry`：`iceberg.catalog.type = hive` → `IcebergCatalogKind::Hive`，
  并提前分流到 `build_hms_catalog_entry`（类比对 `Rest` 的处理）。
  错误信息 `memory|hadoop|rest` → `memory|hadoop|rest|hive`。
- 新增 `build_hms_catalog_entry(props)`（对照 `build_rest_catalog_entry`，registry.rs:1429）：
  - 要求 `hive.metastore.uris`（缺失即报错）。
  - 解析 S3 属性 → `s3_config`（复用现有 `S3StorageFactory::from_catalog_properties`）。
  - 校验非目标语义（Kerberos 相关属性、多层 namespace 迹象）→ fail fast 报错。
  - `warehouse_path` 用占位（HMS 无本地 warehouse，类比 REST 的占位路径）。

### 4.3 catalog 构造（`registry.rs`）

- 新增 `async fn build_hms_catalog(entry) -> Result<HmsCatalog, String>`
  （对照 `build_rest_catalog`，registry.rs:1527）：
  ```text
  HmsCatalogBuilder::default()
      .with_storage_factory(build_storage_factory_for_entry(entry)?)
      .load("hms", props)   // props 见 §4.4
      .await
  ```
- `build_iceberg_catalog`（registry.rs:1597）的 `match entry.kind` 增加 `Hive` 分支，
  用 `block_on_iceberg(async { build_hms_catalog(entry).await })`（REST 已是同样桥接）。

### 4.4 属性映射

NovaRocks `CREATE EXTERNAL CATALOG` 属性 → HMS crate `.load("hms", props)` 键：

| NovaRocks 属性 | → HMS crate 键 / 处理 |
|---|---|
| `hive.metastore.uris = thrift://host:9083` | `HMS_CATALOG_PROP_URI = host:9083`（**剥掉 `thrift://` 前缀**；逗号分隔多地址取第一个） |
| `iceberg.catalog.warehouse` 或 `hive.metastore.warehouse.dir` | `HMS_CATALOG_PROP_WAREHOUSE` |
| `hive.metastore.thrift.framed`（可选，默认 false） | `HMS_CATALOG_PROP_THRIFT_TRANSPORT = buffered`（false）/ `framed`（true） |
| `aws.s3.endpoint` / `aws.s3.access_key` / `aws.s3.secret_key` / `aws.s3.enable_path_style_access` | 注入 `S3StorageFactory`（经 `with_storage_factory`，不进 props） |

HMS crate 常量来源：`iceberg_catalog_hms::{HMS_CATALOG_PROP_URI, HMS_CATALOG_PROP_WAREHOUSE,
HMS_CATALOG_PROP_THRIFT_TRANSPORT, THRIFT_TRANSPORT_BUFFERED, THRIFT_TRANSPORT_FRAMED}`。

### 4.5 文档

- `docs/guides/iceberg-v3/catalog.md`：HMS 那行 `❌` → `✅`。
- CLAUDE.md / AGENTS.md §5.3："supported catalog types are memory, hadoop, and rest" 补 `hive`。

## 5. 数据流

- **读（load_table）**：crate 调 HMS `get_table(db, table)` → 读表参数 `metadata_location`
  → 经注入的 `file_io`（S3/LocalFs）从对象存储读 `metadata.json` → 构建 `iceberg::table::Table`。
  映射：HMS database = Iceberg namespace，HMS table = Iceberg table。
- **写/提交（update_table）**：crate 内部完成 `lock` + `alter_table` 原子换
  `metadata_location` 的乐观并发协议（选 A 的核心收益）。新表参数含
  `table_type=iceberg`、`metadata_location`、`previous_metadata_location`。
- **建表（create_table）/ 删表（drop_table）**：crate 写 `metadata.json` + HMS
  `create_table`（打上 iceberg 表参数）/ `drop_table`。
- **三段式名解析** `catalog.db.table`：复用现有 REST/Hadoop 路径（`src/engine/query_prep.rs`）。

## 6. 异步 / 线程

- crate 是 tokio 原生 async，经 `block_on_iceberg`（即 `data_block_on`，REST 已用，tokio 基）
  桥接到同步引擎流程。
- **早期需证伪的风险**：volo-thrift 客户端对 NovaRocks `data_block_on` 所用 runtime
  的兼容性（volo 是否要求多线程 tokio runtime / 特定 feature）。实施第一步即跑一个最小
  `load_table` 冒烟来证伪；若不兼容，再评估在 `spawn_blocking` / 专用多线程 runtime 上隔离。

## 7. 错误处理

- 连接失败、lock 争用 / 提交冲突 → 透传 crate 的 `iceberg::Error`，沿用现有 `map_err`
  风格映射为 NovaRocks 错误字符串。
- 请求 Kerberos/SASL、多层 namespace、空 `hive.metastore.uris` → 在
  `build_hms_catalog_entry` 阶段 fail fast 显式报错。

## 8. 测试

### 8.1 docker 夹具（`docker/iceberg-hive/`，跨 worktree 共享）

- HMS 不放进 `docker/iceberg-rest/compose.yml`。REST 夹具继续只负责
  MinIO / REST Catalog / Spark；HMS 用独立 `docker/iceberg-hive/` 夹具和独立
  Compose project（默认 `nr-iceberg-hive`）。
- `docker/iceberg-hive/compose.yml` 只包含 `hms` 服务，并加入 REST 夹具的外部
  Docker network（默认 `nr-iceberg-rest_iceberg_net`）。这样 HMS 容器内仍可用
  `http://minio:9000` 访问 MinIO，但生命周期与 REST 夹具解耦。
- standalone Hive Metastore 配 S3A → MinIO（`fs.s3a.endpoint=http://minio:9000`、
  path-style、`fs.s3a.access.key/secret.key` = MinIO 凭据），thrift host 端口默认
  `9083`，网络别名 `hms`。
- 镜像：基于 `apache/hive:4.0.0` metastore 模式 + `hadoop-aws` /
  `aws-java-sdk-bundle` 的自建镜像（`docker/iceberg-hive/Dockerfile`；
  embedded Derby 作元库，容器重启清空，测试可接受）。
- `docker/iceberg-hive/up.sh` 生成独立 `runtime/current/env.sh`，导出
  `NOVA_ENV_HMS_PORT`、`NOVAROCKS_ICEBERG_HMS_URI`、`NOVA_ENV_SHARED_HMS_WAREHOUSE_URI`
  `NOVAROCKS_ICE_HMS_CATALOG_SQL` 和 `NOVAROCKS_SPARK_EXTRA_DEFAULTS`。运行 HMS
  suite 前需要先 source `docker/iceberg-rest/runtime/current/env.sh`，再 source
  `docker/iceberg-hive/runtime/current/env.sh`。
- 跨引擎对拍时，Spark 仍运行在 REST 夹具网络中；Spark 用容器内端点
  `thrift://hms:9083` 与 `http://minio:9000`，NovaRocks 用 host 端点。REST
  Spark 默认配置保持 REST-only；`docker/iceberg-rest/spark-sql.sh` 只在
  `NOVAROCKS_SPARK_EXTRA_DEFAULTS` 存在时追加 HMS catalog 配置。

### 8.2 SQL suites（`sql-tests/`）

- `sql-tests/iceberg-hms`（对照 `sql-tests/iceberg-rest`）：NovaRocks 自环 ——
  `CREATE EXTERNAL CATALOG ... iceberg.catalog.type=hive`，CREATE / INSERT / SELECT /
  UPDATE / DELETE / MERGE / DROP 写读回。
- `sql-tests/iceberg-hms-compatibility`（对照 `sql-tests/iceberg-compatibility`）：
  Spark 经 HMS 写 → NovaRocks 读；NovaRocks 经 HMS 写 → Spark 读。
- 在 `tests/sql-test-runner/src/config.rs` 注册两个 suite，并在 `up.sh` 生成的 runner 配置
  （`$NOVAROCKS_SQL_TEST_CONFIG`）中纳入。

### 8.3 单测（`registry.rs`）

- `build_catalog_entry` / `build_hms_catalog_entry` 对 hive 的解析用例：
  `hive.metastore.uris` 必填、`thrift://` 前缀剥离、transport 默认 buffered、
  Kerberos 属性 fail fast —— 对照现有 rest/hadoop 解析单测。

## 9. 实施顺序（给后续 plan 的骨架）

1. 加依赖 + 跑通 volo / `block_on` 兼容性最小 `load_table` 冒烟（证伪 §6 风险，先于其余）。
2. `registry.rs`：`Kind` / `Entry` / 解析 / 构造 / 属性映射 + 单测（§4.2–4.4、§8.3）。
3. docker 夹具 HMS 服务 + 自建镜像 + env 生成（§8.1）。
4. `iceberg-hms` 自环 suite（§8.2）。
5. `iceberg-hms-compatibility` 跨引擎 suite + Spark HMS 配置（§8.2）。
6. 文档更新（§4.5）。

## 10. 风险与开放项

- **HMS 镜像选型**：`apache/hive:4.0.0` metastore + hadoop-aws 为首选，需确认 S3A jar
  版本与 MinIO 兼容；自建镜像有一次性构建成本。若 4.0.0 metastore + Derby + S3A 组合
  受阻，备选为社区 metastore 镜像或在镜像内附 Postgres 元库。
- **volo 编译时间**：pilota 有 build-time codegen，会拉长冷构建（与项目对 build profile
  的敏感性相关，见 CLAUDE.md §8.2）。
- **thrift transport 默认值**：HMS 默认 buffered；若所选镜像启用 framed，需对齐
  `hive.metastore.thrift.framed`。

## 11. 参考实现位置

- NovaRocks catalog：`src/connector/iceberg/catalog/registry.rs`
  （`IcebergCatalogKind`:47、`build_catalog_entry`:1308、`build_rest_catalog_entry`:1429、
  `build_rest_catalog`:1527、`build_iceberg_catalog`:1597）；
  `src/connector/iceberg/catalog/hadoop_catalog.rs`（自写 catalog 范式）。
- vendored crate 契约：`vendor/iceberg-0.9.0/src/catalog/mod.rs:168`（`CatalogBuilder` trait）；
  `vendor/iceberg-catalog-rest-0.9.0/src/catalog.rs`（builder + storage factory 用法）。
- StarRocks 参考（FE，仅看 planning 逻辑）：
  `fe/fe-core/.../connector/iceberg/hive/IcebergHiveCatalog.java`（委托 Apache
  `HiveCatalog`，loadTable/create/register）；`HiveTableValidator.java`
  （`table_type=iceberg`、`metadata_location` 常量）；`UnifiedMetadata.java`
  （按 `table_type` 识别 Iceberg 表）；catalog 属性 `iceberg.catalog.type=hive`、
  `hive.metastore.uris`。
- 现有 docker 夹具：`docker/iceberg-rest/compose.yml`、`shared.env`、`up.sh`、`spark/`。
