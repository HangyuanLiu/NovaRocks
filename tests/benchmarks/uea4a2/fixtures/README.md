# UEA-4A-2 writer corpus

这组语料验证 Parquet 格式实现的异源输入。每个写入端必须有真实运行的写入命令、版本、文件 SHA-256、footer 信息和逐行 oracle；单个 writer 改标签不算另一种输入。`verify` 路径只读本地文件，不下载或生成缺项。测试前按 manifest 中的 hash 核对输入；任一文件缺失时不得宣布 writer matrix 通过。

## 当前供给入口

- **Spark/parquet-mr**：使用共享 SSB scale-1 的 `READY.json` 发布物，核对其 SHA-256、Spark 3.5.5/Iceberg 1.11.0 生产指纹，以及选中 lineorder Parquet 的固定 SHA-256 和 parquet-mr 1.17.1 footer。将同一已发布对象复制到一个本地目录，命名为 `spark-parquet-mr-1.17.1.parquet`，将 `READY.json` 放在同目录，然后运行：

  ```bash
  python3 fixtures/writer_corpus.py register-spark \
    --input-dir "$SPARK_CORPUS_DIR" --source-object "$EXACT_PUBLISHED_S3_OBJECT"
  python3 fixtures/writer_corpus.py verify \
    --manifest "$SPARK_CORPUS_DIR/spark-manifest.json"
  ```

- **Trino**：`provision_trino.sh` 使用已在本机供给且与固定 manifest digest 匹配的 Trino 483 镜像，在本工作树生成的 REST/MinIO fixture 中创建私有 Iceberg 表，CTAS 写入 Parquet，核对行数、复制数据文件并清理自身表/容器。`mc` 和 PyArrow 是供给端工具。输出 manifest 保存精确 SQL、镜像 digest、源对象和文件/逐行 hash。

  ```bash
  bash fixtures/provision_trino.sh "$TRINO_CORPUS_DIR"
  python3 fixtures/writer_corpus.py verify \
    --manifest "$TRINO_CORPUS_DIR/trino-manifest.json"
  ```

- **PyArrow**：`writer_corpus.py` 用本机固定的 PyArrow 23.0.1 `write_table` API 生成包含 nullable、dictionary、nested、timestamp、decimal 的 4096 行文件，记录页大小、行组、压缩、索引选项和逐行 hash。

  ```bash
  python3 fixtures/writer_corpus.py provision-pyarrow --output-dir "$PYARROW_CORPUS_DIR"
  python3 fixtures/writer_corpus.py verify \
    --manifest "$PYARROW_CORPUS_DIR/pyarrow-manifest.json"
  ```

- **Flink**：使用 `provision_flink.sh` 的独立真实 Flink SQL filesystem sink；只有固定镜像和 Parquet bundle 的 digest 核对、实际写入、离线 `--verify` 均通过后才列入正式 corpus manifest。

仓库内发布的 Flink 消费端语料用 `python3 fixtures/verify_flink_published.py` 离线核验：它检查总清单绑定的 Parquet、SQL、原始 writer receipt、物理位置序列、schema、4096 行和逐行 oracle。原始 Flink Parquet/Hadoop JAR 的 SHA 只作为生成来源记录；复现写入时需另备这两个 JAR，并在完整生产者目录运行 `provision_flink_local.sh --verify <目录>`。仓库发布目录不包含约 49 MB 的生产者 JAR。

这些独立文件还不能替代 Iceberg split/delete/DV 的原生 SQL 场景；其值与物理位置对照应在 FS conformance 和 I04 的 1FE+3BE 测试中分别使用。正式冻结 manifest 需要四类全部 READY，并绑定相同一套测试输入与配置供 G0–G3 对照。

## G0 短查询 Iceberg 输入

`provision_short_iceberg.sql` 在当前托管 REST/MinIO 环境中创建任务私有 Spark/Iceberg 表，`compact_short_iceberg.sql` 将当前快照固定为单个 4096 行 Parquet 数据文件。`publish_short_iceberg.py provision` 检查当前快照的唯一文件、逐行 `id/value` oracle、对象长度与 ETag，并发布 `manifest.json`、`objects.json`、本地数据副本和 `READY`。已发布的 40 KiB 输入保存在 `short_iceberg/`；其 `verify` 入口完全离线且不修改输入。首次供给后不再重复运行 SQL 或替换 READY；G0 A/A 使用同一发布物及其声明的实时 REST/S3 对象。A4 的 240 文件输入独立保留作为多文件负载。

## RF 后到的 whole-file Iceberg 输入

`rf_late/probe.parquet` 含同一文件内 32 个物理行组。原 `probe_v1` 快照在 Iceberg DataFile 中带 32 个 `split_offsets`，原生调度会拆成多个 split；新发布的 `probe_whole_v1` 使用完全相同哈希的对象，在新快照中显式将 DataFile 的 `split_offsets` 设为 `null`，供同一 split 内的行组时序试验。`rf_late/whole_published.json` 固定新旧 snapshot、对象 SHA-256、131072 行及 32 个物理行组。`verify_manifest.py --verify` 离线交叉核对该收据、旧发布收据、本地物料和顶层 `manifest.json`；在线对象与当前快照另由 `rf_late/publish_whole.py verify-published` 检查。

```bash
PYTHONDONTWRITEBYTECODE=1 python3 tests/benchmarks/uea4a2/fixtures/verify_manifest.py --verify
source docker/iceberg-rest/runtime/current/env.sh
PYTHONDONTWRITEBYTECODE=1 python3 tests/benchmarks/uea4a2/fixtures/rf_late/publish_whole.py verify-published
```

以上只固定物理输入及 Iceberg 元数据；RF 到达、probe 行组消费与后续剪枝的原生因果顺序仍须由独立场景验证。
