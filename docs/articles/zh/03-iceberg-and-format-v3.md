# 把湖接进来：Iceberg 集成与 Format v3

> NovaRocks 技术分析系列 · 第 3 篇

前两篇我们把一句 SQL（或一棵 thrift 计划）一路送到了 `ExecPlan`、跑在了 pipeline 上，但始终回避了一个问题：**数据从哪来？** 真实世界里 NovaRocks 的主战场是数据湖——还记得第 0 篇那个 fail-fast 的例子吗，`OLAP_SCAN_NODE` 被直接拒绝、报错信息写着"Phase 1 only supports shared-data"。存算分离、以湖为底座，是这个引擎的定位。

这一篇就看它怎么把 Iceberg 接进来：三种 catalog、读怎么把删除应用上去、写怎么提交一个 snapshot，以及它在开源引擎里都算少见的 **format v3** 完整度，最后是怎么和 Spark 跨引擎互通。

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

三种类型语义不同：**Hadoop**（默认）由 NovaRocks 自己按 Hadoop 命名约定管理 warehouse 目录下的 `v{N}.metadata.json`；**REST** 则按 Iceberg REST Catalog 协议对接外部服务，规范要求先做一次 `GET /v1/config` 握手，所以这条构造路径是异步的、再用 `block_on_iceberg` 收口；**Memory** 是纯测试占位。

但不论哪种类型，引擎内部都通过一个统一分派器 `build_iceberg_catalog` 拿到 Iceberg 表句柄——上层的读、写、DDL 不关心底下是 Hadoop 还是 REST。代码里有注释明说，引擎流程正从早期直接用 `build_hadoop_catalog` 迁移到这个统一分派器；REST 已经接入真实的 catalog 操作（registry 多处调用 `build_rest_catalog`），迁移仍在收尾。把"catalog 种类"收敛到一个分派器后面，是让上层逻辑与具体 catalog 实现解耦的关键。

## 读路径：把删除正确地应用上去

一次扫描会先做元数据规划，得到一批 data file 以及一批作用其上的 delete file。难点在于：**哪条 delete 作用于哪个 data file？** 这件事错一点，就会少删或多删行。判定逻辑在 `delete_applies_to_data_file`，三道闸门层层把关：

```rust
// src/connector/iceberg/read.rs:74
pub(crate) fn delete_applies_to_data_file(
    delete_file: &IcebergReadDeleteFile,
    data_file: &IcebergReadFile,
) -> bool {
    // 闸 1：delete 必须比 data 新（按 sequence number）
    if let (Some(delete_sequence), Some(data_sequence)) =
        (delete_file.sequence_number, data_file.data_sequence_number)
        && delete_sequence <= data_sequence
    {
        return false;
    }
    // 闸 2：若 delete 指定了 referenced_data_file，必须正是这个 data file
    if let Some(referenced) = delete_file.referenced_data_file.as_deref()
        && referenced != data_file.path
    {
        return false;
    }
    // 闸 3：分区 spec / 分区值必须一致
    if let Some(delete_partition) = delete_file.partition_key.as_deref() {
        let Some(delete_spec_id) = delete_file.partition_spec_id else { return false; };
        let Some(data_spec_id) = data_file.partition_spec_id else { return false; };
        if delete_spec_id != data_spec_id { return false; }
        if data_file.partition_key.as_deref() != Some(delete_partition) { return false; }
    }
    // ...
    true
}
```

Iceberg 的删除语义本就靠 sequence number 来表达"删除发生在哪些数据之后"，所以**只有比某条 data 更新的 delete 才作用于它**（闸 1）；deletion vector / positional delete 又会精确引用某个 data file（闸 2）；最后分区也必须对上（闸 3）。三道闸缺一不可。

## 写路径：选对写模式，再走 commit action

写入按操作类型路由到不同的 commit action，它们实现统一的 trait：

```rust
// src/connector/iceberg/commit/action.rs:67
#[async_trait]
pub trait IcebergCommitAction: Send + Sync {
    /// Stage any manifests required, build a `TableCommit`, and submit it via
    /// `Catalog::update_table`. Implementations must record every staged
    /// manifest path on `ctx.abort_handle` so that a later failure can clean
    /// them up.
    async fn commit(&self, ctx: CommitCtx<'_>) -> Result<CommitOutcome, String>;
}
```

注意那条 abort 约定：每个 action 都要把暂存的 manifest 路径记到 `abort_handle` 上，失败时好清理——一个写入失败不能在对象存储上留下垃圾。普通 INSERT 走 `FastAppendCommit`，v3 的 DELETE 走 `RowDeltaDvCommit`。走哪条，取决于表的写模式，而写模式直接由表元数据决定：

```rust
// src/connector/iceberg/commit/validation.rs:40
pub fn classify_iceberg_write_mode(table: &Table) -> IcebergWriteMode {
    // ...
    if format_version == FormatVersion::V3 || row_lineage_property_enabled(props) {
        IcebergWriteMode::RowLineageV3
    } else {
        IcebergWriteMode::LegacyPositionDeletes
    }
}
```

v3（或显式开了 row-lineage）的表，删除就走 deletion vector + row-lineage 那套；否则退回 v2 的 position delete。下面这些 v3 能力，都挂在 `RowLineageV3` 这条路上。

## Format v3：四件套的真实状态

NovaRocks 对 Iceberg v3 的支持是这一篇最有看头的地方。但"支持 v3"不是营销话术——下面逐项给出**诚实的实现状态**。

### Deletion Vector（✅ 已实现）

v2 的 position-delete 是一个个独立的 delete 文件，v3 引入了更紧凑的 deletion vector。NovaRocks 用 Roaring bitmap 实现，并按 Iceberg 的 Puffin blob 规范序列化：

```rust
// src/connector/iceberg/commit/puffin_dv.rs:31
pub struct DeletionVector {
    bitmaps: BTreeMap<u32, RoaringBitmap>,
}

// src/connector/iceberg/commit/puffin_dv.rs:103
pub fn to_iceberg_payload(&self) -> Result<Vec<u8>> {
    let mut body = Vec::new();
    body.extend_from_slice(&MAGIC);
    body.extend_from_slice(&(self.bitmaps.len() as u64).to_le_bytes());
    for (key, bitmap) in &self.bitmaps {
        body.extend_from_slice(&key.to_le_bytes());
        bitmap.serialize_into(&mut body)?;
    }
    let body_len = u32::try_from(body.len())?;
    let crc = crc32fast::hash(&body);
    let mut payload = Vec::with_capacity(4 + body.len() + 4);
    payload.extend_from_slice(&body_len.to_be_bytes());  // 大端长度前缀
    payload.extend_from_slice(&body);
    payload.extend_from_slice(&crc.to_be_bytes());        // CRC32 校验
    Ok(payload)
}
```

两个设计点。其一，存储不是一个扁平 bitmap，而是 `BTreeMap<u32, RoaringBitmap>`——把 64 位行位置空间按高 32 位分段，每段一个 Roaring bitmap。被删行可能稀疏地散布在整个空间里，二层结构让序列化时只记录非空段、查询时只查对应段。其二，payload 严格按 Iceberg 规范布局（magic + 分段 bitmap + 大端长度前缀 + CRC32），所以 Spark/Trino 也能读。多次 DELETE 通过 `merge` 合并进同一个 Puffin 文件。

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

v3 新增了纳秒精度时间戳。NovaRocks 在 Arrow ↔ Iceberg 类型映射里把全部四种组合都对上了：

```rust
// src/formats/parquet/mod.rs:1738
DataType::Timestamp(TimeUnit::Microsecond, None)    => Type::Primitive(PrimitiveType::Timestamp),
DataType::Timestamp(TimeUnit::Microsecond, Some(_)) => Type::Primitive(PrimitiveType::Timestamptz),
DataType::Timestamp(TimeUnit::Nanosecond, None)     => Type::Primitive(PrimitiveType::TimestampNs),
DataType::Timestamp(TimeUnit::Nanosecond, Some(_))  => Type::Primitive(PrimitiveType::TimestamptzNs),
```

而且这不是一个孤立的类型映射——围绕纳秒还有一整套配套：纳秒精度的 cast（含向微秒收窄时的截断、向上溢出报错）、纳秒级的 min/max 谓词下推、以及对"在纳秒时间戳列上做分区 transform"的 fail-fast 拒绝。最后那个又是那个一以贯之的态度：能力边界处宁可明确报错。（第 0 篇里 lowering 的 DATETIME 也按 `time_unit` 落成微秒或纳秒，那一头和这一头是对齐的。）

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

为什么不硬撑着支持？因为 variant 列在行级删除/更新语义下如何参与（它不进分区、不进排序、不做 equality-delete）尚未有清晰定义，与其产出可疑结果，不如在 commit 前就拒绝、并在报错信息里说清楚边界。这把"还没想清楚的"和"已经能正确做的"用一道显式检查分开。

## 跨引擎：让 Spark 写、NovaRocks 读

光自己读写不够，数据湖的意义在于多引擎共享。NovaRocks 的 CI 里有一套 `docker/iceberg-rest/` 固定环境——Iceberg REST Catalog + MinIO + Spark，三件套共享。在它之上有两类验证：`iceberg-compatibility` 套件让 **Spark 通过 REST Catalog + MinIO 写表、NovaRocks 再读回来**，直接对账跨引擎的兼容性；`iceberg-rest` 套件则是 NovaRocks 自己既写又读，覆盖 namespace API、commit 协议、metadata 表、v3 默认列、时间旅行。前者用一个成熟引擎当裁判，是验证"我和别人理解一致"的最直接办法。这套固定环境同时也是第 6 篇要讲的"正确性闭环"的一部分。

## 取舍与对照

- **happy-path 优先，边界 fail-fast**。variant 写只做 INSERT、纳秒列分区 transform 直接拒绝、v2 的 `OLAP_SCAN` 不收——同一种哲学贯穿全篇：宁可把"还没做"明确报出来，也不无声地产出错误结果。
- **行身份物理化而非虚拟化**。多花存储把 `_row_id` 写进文件，换来 OPTIMIZE 之后行身份依然稳定——这是为增量物化视图付的"预付款"，第 5 篇会兑现。
- **紧凑表示 + 规范兼容**。deletion vector 用 Roaring bitmap 的二层分段结构省内存，又严格按 Puffin 规范（大端长度 + CRC32）序列化，保证 Spark/Trino 能读。性能与互通两头都要。
- **写模式由元数据驱动**。是走 v3 row-lineage 还是 v2 position-delete，不靠开关猜，而是从表的 `format-version` / row-lineage 属性里读出来——让"这张表该怎么写"成为表自身的确定性事实。
- **复用而非重造**。底层 Iceberg 操作复用 iceberg-rust，列式表示复用 Arrow，NovaRocks 把精力放在"把湖的语义正确接到自己的执行/写入路径上"，而不是重写一套 Iceberg。

## 小结：下一站，自管一个湖

这一篇讲的是 NovaRocks 怎么接**别人的**湖——Iceberg 表由外部 catalog（甚至外部引擎 Spark）管理，NovaRocks 做读写参与方。但它还有另一种玩法：**自管一个湖**。在 standalone 模式下，没有外部 metastore、没有 FE，NovaRocks 用一个 SQLite 当元数据库、对象存储放数据，自己维护表、分区、版本和事务。

下一篇就进入这个 managed-lake：它怎么用 SQLite + 对象存储保证一致性，事务的"可见性"是怎么靠版本号原子推进的，以及那个在后台默默回收垃圾的 erase worker。
