# 把湖接进来：Iceberg 集成与 Format v3

> NovaRocks 技术分析系列 · 第 3 篇

前两篇我们把一句 SQL（或一棵 thrift 计划）一路送到了 `ExecPlan`、跑在了 pipeline 上，但始终回避了一个问题：**数据从哪来？** 真实世界里 NovaRocks 的主战场是数据湖——还记得第 0 篇那个 fail-fast 的例子吗，`OLAP_SCAN_NODE` 被直接拒绝、报错信息写着"Phase 1 only supports shared-data"。存算分离、以湖为底座，是这个引擎的定位。

这一篇就看它怎么把 Iceberg 接进来：三种 catalog、读写路径，以及它在开源引擎里都算少见的 **format v3** 完整度，还有怎么和 Spark 跨引擎互通。

## 三种 catalog，一个统一句柄

Iceberg 的 catalog 决定了"表的元数据在哪、由谁管"。NovaRocks 在 `CREATE EXTERNAL CATALOG` 时按 `iceberg.catalog.type` 属性确定类型：

```rust
// src/connector/iceberg/catalog/registry.rs:47
pub(crate) enum IcebergCatalogKind {
    Hadoop,   // 默认：NovaRocks 直接管 warehouse 目录下的 metadata
    Memory,   // 仅测试用的内存注册表
    Rest,     // 走 Iceberg REST Catalog 协议对接外部服务（Lakekeeper/Polaris/...）
}

// src/connector/iceberg/catalog/registry.rs:1324
None => IcebergCatalogKind::Hadoop,
Some(v) if v.eq_ignore_ascii_case("hadoop") => IcebergCatalogKind::Hadoop,
Some(v) if v.eq_ignore_ascii_case("memory") => IcebergCatalogKind::Memory,
Some(v) if v.eq_ignore_ascii_case("rest") => IcebergCatalogKind::Rest,
```

不论哪种类型，引擎内部都通过一个统一分派器 `build_iceberg_catalog` 拿到 Iceberg 表句柄——上层的读、写、DDL 不关心底下是 Hadoop 还是 REST。REST catalog 已经接入真实的 catalog 操作（`build_rest_catalog` 在 registry 多处被调用，因为 REST 规范要求先做一次 `GET /v1/config` 握手，所以这条路径是异步的、再用 `block_on_iceberg` 收口）；从早期直接用 `build_hadoop_catalog` 到这个统一分派器的迁移仍在收尾（代码里有注释明说这件事）。Memory 则是纯测试用途的占位。

读路径上，一次扫描会先做元数据规划，得到一批 data file 以及作用其上的 delete file，再在读时把删除应用上去。哪条 delete 作用于哪个 data file，由 `src/connector/iceberg/read.rs` 里的 `delete_applies_to_data_file` 判定——它综合比对 sequence number（delete 必须比 data 新）、`referenced_data_file`、以及分区 spec 是否一致。写路径则按操作类型路由到不同的 commit action：普通 INSERT 走 `FastAppendCommit`，v3 的 DELETE 走 `RowDeltaDvCommit`，它们都实现统一的 `IcebergCommitAction` trait，负责生成新的 snapshot 与 manifest。

## Format v3：四件套的真实状态

NovaRocks 对 Iceberg v3 的支持是这一篇最有看头的地方。但"支持 v3"不是一句营销话术——下面逐项给出**诚实的实现状态**。

### Deletion Vector（✅ 已实现）

v2 的 position-delete 是一个个独立的 delete 文件，v3 引入了更紧凑的 deletion vector。NovaRocks 用 Roaring bitmap 实现：

```rust
// src/connector/iceberg/commit/puffin_dv.rs:31
pub struct DeletionVector {
    bitmaps: BTreeMap<u32, RoaringBitmap>,
}
```

注意它不是一个扁平 bitmap，而是 `BTreeMap<u32, RoaringBitmap>`——把 64 位的行位置空间按高 32 位分段，每段一个 Roaring bitmap。被删行可能稀疏地散布在整个位置空间里，二层结构让序列化时只记录非空段、查询时只查对应段，避免在巨大空间上做无谓遍历。多次 DELETE 通过 `merge` 合并进同一个 Puffin 文件，最终 `to_iceberg_payload` 按 Iceberg 规范（magic + 分段 bitmap + CRC32）序列化。

### Row Lineage（✅ 已实现）

v3 的行血缘要求每行带稳定的 `_row_id` 和 `_last_updated_sequence_number`。NovaRocks 给它们分配了 Iceberg 保留的 field id，并把这两列**物理写进 Parquet**：

```rust
// src/exec/row_position.rs:78
pub const ICEBERG_ROW_ID_COL: &str = "_row_id";
pub const ICEBERG_LAST_UPDATED_SEQ_COL: &str = "_last_updated_sequence_number";

pub const ICEBERG_RESERVED_FIELD_ID_ROW_ID: i32 = i32::MAX - 107;
```

为什么要物理写、而不是读时用 `first_row_id + 行偏移` 虚拟合成？因为 OPTIMIZE 会重写 data file，如果行身份是虚拟的，重写后再被 DELETE 就找不到原行了。物理存储让行身份跨 snapshot 稳定——这恰恰是后面增量物化视图（第 5 篇）能把"变化的那一行"对应回去的前提。读端优先从物理列读，缺失时才回退到虚拟合成。

### 纳秒时间戳（✅ 已实现）

v3 新增了纳秒精度时间戳。NovaRocks 在 Arrow ↔ Iceberg 类型映射里把它对上了：

```rust
// src/formats/parquet/mod.rs:1744
DataType::Timestamp(TimeUnit::Nanosecond, None) => {
    Type::Primitive(PrimitiveType::TimestampNs)
}
DataType::Timestamp(TimeUnit::Nanosecond, Some(_)) => {
    Type::Primitive(PrimitiveType::TimestamptzNs)
}
```

围绕它还有一串配套工作：纳秒精度的 cast（含向微秒收窄时的截断、向上溢出报错）、纳秒级的 min/max 谓词下推、以及对"在纳秒时间戳列上做分区 transform"的 fail-fast 拒绝——后者又是那个一以贯之的态度：能力边界处宁可明确报错。

### Variant（读 ✅ / 写 🚧 部分）

variant（半结构化类型）的**读**是完整的；**写**目前只覆盖 INSERT 的 happy path。行级变更（OVERWRITE/DELETE/UPDATE/MERGE）在 variant 表上会被直接挡掉：

```rust
// src/connector/iceberg/commit/validation.rs:70
pub fn ensure_no_variant_columns_for_row_level_mutation(table: &Table) -> Result<(), String> {
    // ...
    return Err(format!(
        "iceberg table column '{name}' is variant; row-level mutation of variant tables is not supported in this release. \
         INSERT (without OVERWRITE) is supported.",
        name = f.name,
    ));
    // ...
}
```

为什么不硬撑着支持？因为 variant 列在行级删除/更新语义下如何参与（它不进分区、不进排序、不做 equality-delete）尚未有清晰定义，与其产出可疑结果，不如在 commit 前就拒绝、并在报错信息里说清楚边界。

## 跨引擎：让 Spark 写、NovaRocks 读

光自己读写不够，数据湖的意义在于多引擎共享。NovaRocks 的 CI 里有一套 `docker/iceberg-rest/` 固定环境——Iceberg REST Catalog + MinIO + Spark，三件套共享。在它之上有两类验证：`iceberg-compatibility` 套件让 **Spark 通过 REST Catalog + MinIO 写表、NovaRocks 再读回来**，直接对账跨引擎的兼容性；`iceberg-rest` 套件则是 NovaRocks 自己既写又读，覆盖 namespace API、commit 协议、metadata 表、v3 默认列、时间旅行。这套固定环境同时也是第 6 篇要讲的"正确性闭环"的一部分。

## 取舍与对照

- **happy-path 优先，边界 fail-fast**。variant 写只做 INSERT、纳秒列分区 transform 直接拒绝、v2 的 `OLAP_SCAN` 不收——同一种哲学贯穿全篇：宁可把"还没做"明确报出来，也不无声地产出错误结果。
- **行身份物理化而非虚拟化**。多花存储把 `_row_id` 写进文件，换来 OPTIMIZE 之后行身份依然稳定——这是为增量物化视图付的"预付款"。
- **紧凑表示的工程感**。deletion vector 用 Roaring bitmap 的二层分段结构，针对稀疏删除位置做了内存与序列化优化，而不是图省事用一个大 bitmap。
- **复用而非重造**。底层 Iceberg 操作复用 iceberg-rust，列式表示复用 Arrow，NovaRocks 把精力放在"把湖的语义正确接到自己的执行/写入路径上"，而不是重写一套 Iceberg。

## 小结：下一站，自管一个湖

这一篇讲的是 NovaRocks 怎么接**别人的**湖——Iceberg 表由外部 catalog（甚至外部引擎 Spark）管理，NovaRocks 做读写参与方。但它还有另一种玩法：**自管一个湖**。在 standalone 模式下，没有外部 metastore、没有 FE，NovaRocks 用一个 SQLite 当元数据库、对象存储放数据，自己维护表、分区、版本和事务。

下一篇就进入这个 managed-lake：它怎么用 SQLite + 对象存储保证一致性，事务的"可见性"是怎么靠版本号原子推进的，以及那个在后台默默回收垃圾的 erase worker。
