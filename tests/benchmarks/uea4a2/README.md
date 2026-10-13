# Range 与短查询功能输入

本目录名为历史布局，当前仅保留功能fixture及显式producer；release测量controller、pilot/calibration/window、比较器和workload门限已退出。四writer本地conformance数据的canonical owner见 [FS writer corpus](../../../novarocks/fs/tests/data/writer-corpus/README.md)。旧receipt/路径只解释来源，不证明当前远端健康。

`uea4/iceberg-range-functional`使用至少16文件的A4输入，准确覆盖id=0..767、独立行值与LIMIT后退出；`connector/iceberg-short-range-concurrent`使用4096行单文件短表，四actor各查id1..10/value=3×id。输入由环境变量 `NOVAROCKS_UEA4A2_FIXTURE_MANIFEST` 和 `NOVAROCKS_ICEBERG_SHORT_FIXTURE_MANIFEST`明确绑定；对象凭据通过既有 `NOVAROCKS_UEA4A2_S3_ACCESS_KEY_ID`/`NOVAROCKS_UEA4A2_S3_SECRET_ACCESS_KEY`显式供给，manifest不保存secret。缺READY/object/hash/oracle直接失败，不从测量workload或旧校准receipt推导默认值。

多文件输入可以由保留的 [Iceberg fixture producer](../uea4a4/README.md)在任务私有namespace供给并独立verify。短表使用FS owner的原始Spark SQL模板，由输入owner按准确task namespace渲染并通过正常Spark写入/compact；`fixtures/publish_short_iceberg.py provision`要求明确 `--output`、`--database`、`--table`、`--spark-image-id`、`--spark-output`（官方Spark files输出）。publisher核对REST snapshot与4096行唯一datafile，独立PyArrow核对每行，并发布manifest/objects/READY；拒绝覆盖已有output。

```bash
python3 tests/benchmarks/uea4a2/fixtures/publish_short_iceberg.py verify --output "$short_input"
```

该verify只核对已发布本地附件，不写对象或补造输入；当前远端catalog/S3健康和snapshot绑定由实际功能消费验证。prepare与verify分开；任何输入变化换新namespace和独占外部attempt，保留原FAIL/producer SQL/log/hash。相邻 [fixtures说明](fixtures/README.md)区分历史输入与当前consumer。

场景仅以clean `--launch-profile performance --cluster-size 3`配置运行有限功能步骤，失败取消live actors、释放hold并实际join，客户端结果与FE/BE owner收敛、cluster shutdown分别记录。相关range/provider/admission/fixture改动必跑；每周领域手工轮次，自动调度未交付。完整命令骨架与材料规则见 [system指南](../../system-test-runner/README.md)。
