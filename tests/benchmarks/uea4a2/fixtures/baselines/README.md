# UEA-4A-2 已发布 Iceberg 输入基线

`baseline.json` 冻结本次只读核验的表、快照、对象与 SQL oracle。`a4/` 原样复制 UEA-4A-4 已发布的 READY、manifest、对象清单、Spark SQL、writer log 和 oracle；`ssb/` 原样复制共享 SSB READY、发布 manifest、选中 Parquet 文件的写入收据，以及现有 Q1.1 SQL/result。`verify.py` 不生成数据或修改对象存储。

```bash
python3 tests/benchmarks/uea4a2/fixtures/baselines/verify.py

source docker/iceberg-rest/runtime/current/env.sh
python3 tests/benchmarks/uea4a2/fixtures/baselines/verify.py --online \
  --ssb-file /private/tmp/uea4a2-spark-20260923/spark-parquet-mr-1.17.1.parquet
```

离线模式核对附件、A4 的 240 个数据对象清单、READY、SQL oracle，以及 SSB 的发布表/快照指针。可选 `--ssb-file` 还核对本地所选宽文件的 SHA-256、832,000 行、17 列、单行组及最大列块。在线模式只读 MinIO：逐项比较 A4 清单并从当前快照 manifest 确认 240 个文件、768 行；核对 SSB READY、发布 manifest、当前 `ssb.lineorder` 快照包含所选文件，再核对该文件的 S3 size/ETag。在线核对需要 Python `minio` 和 `fastavro`。

A4 的 240 个 Parquet 文件未复制进仓库，清单保存 size/ETag，不能离线证明每个文件的内容 SHA-256。SSB 的所选 20.3 MB 文件也未复制；收据的 SHA-256 来自已有的本地精确副本，在线模式仅用当前 S3 size/ETag 和 Iceberg manifest 证明对象身份。SSB 当前表有八个数据文件，所选文件的 832,000 行 oracle 只对这个文件成立。表级 `COUNT(*) = 6,001,171` 来自已发布 manifest，Q1.1 `revenue = 219159726134` 来自现有 SSB SQL benchmark 结果；本次未重新运行 Spark 或 NovaRocks SQL。正式性能场景需再绑定实际执行 SQL、表级 oracle 和 Task/BE 布局。
