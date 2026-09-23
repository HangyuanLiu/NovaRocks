# UEA-4A-4 Catalog planning short regression

本目录定义 FE Connector metadata/planning 的短时原生 `1FE+3BE` 对照负载。`workload.json` 的
SHA-256、两个真实 provider 的 READY manifest、对象清单、运行二进制和渲染配置都必须随运行证据保存。
同一 controller 在 B0 与 candidate 上运行；B0 必须包含 UEA-4A-1，且两次只替换 server binary。
此检查只用于排除大幅性能回退，不能代替手动性能测试或作为精确基准。

场景名为 `uea4/catalog-planning-performance`，需要 `--launch-profile performance` 和
`--cluster-size 3`。它从 `NOVAROCKS_UEA4A4_WORKLOAD_MANIFEST` 读取负载；未指定时使用本目录的
`workload.json`。`NOVAROCKS_UEA4A4_ICEBERG_FIXTURE_MANIFEST` 与
`NOVAROCKS_UEA4A4_PAIMON_FIXTURE_MANIFEST` 必须指向预先发布的不可变真实输入。
本目录的 `iceberg_fixture.py` 与 `paimon_fixture.py` 分别通过已验证的 Spark/Iceberg REST 和
Spark/Paimon 写入任务私有表。两个 manifest 均需原始字节 SHA-256 的 `READY`、校验后的 `objects.json`、真实
snapshot/schema ID、至少 16 个数据文件和非零行。Iceberg manifest 还需 `rest_uri`。
两者都包含 `warehouse_uri`、`s3_endpoint`、`region`、`credential_name`、
`credential_generation`、`database`、`table`、`snapshot_id`、`schema_id`、
`data_file_count`、`row_count` 与 `objects_sha256`。`s3_endpoint` 必须是本机 HTTP；
controller 在此基础上建立任务私有延迟代理，并只让慢远端 catalog 使用它。

先启动各 fixture 已验证的容器环境，再发布输入。两个发布器均支持 `prepare`、`verify`、
`cleanup`；`cleanup` 需要与 READY manifest 完全一致的 `--run-id`，只删除该次任务的对象前缀。
Iceberg 默认做 24 次确定性追加，至少留下 16 个真实 Parquet 文件，并用 Spark 聚合生成行数、
键范围及校验和 oracle。`--dry-run` 仅渲染并检查作用域，不写入远端。发布后保存两个
`manifest.json` 的绝对路径，供上述环境变量使用。示例：

```bash
python3 tests/benchmarks/uea4a4/iceberg_fixture.py prepare \
  --run-id uea4a4-c00-example --output-dir /tmp/uea4a4-iceberg-example
python3 tests/benchmarks/uea4a4/iceberg_fixture.py verify \
  --output-dir /tmp/uea4a4-iceberg-example

python3 tests/benchmarks/uea4a4/paimon_fixture.py prepare \
  --run-id uea4a4-c00-example --output-dir /tmp/uea4a4-paimon-example
python3 tests/benchmarks/uea4a4/paimon_fixture.py verify \
  --output-dir /tmp/uea4a4-paimon-example
```

本工作树的一次真实 smoke 输入已发布在
`/tmp/uea4a4-iceberg-c00-20260923-7dfc/manifest.json` 和
`/tmp/uea4a4-paimon-smoke/manifest.json`。可分别对对应 output directory 运行上面的
`verify` 命令，再用于 B0 config smoke；这两个 READY 输入应保留供 B0/candidate 对照。
新一轮对照建议换用新的任务私有 run ID 与 output directory；既有 smoke 输入可用于验证场景接线。

运行前将两个 fixture manifest 的对象仓库静态凭据放入
`AWS_S3_ACCESS_KEY_ID`、`AWS_S3_SECRET_ACCESS_KEY`；性能启动会清空继承环境，controller
通过 FE/BE child environment 显式投射这两个值。两个 fixture 必须使用同一组凭据。
先用 `uea4/catalog-planning-smoke` 对 B0 和 candidate 运行同一套配置与真实输入，核对
`scenario-evidence.json` 中的原生拓扑和成功退出。然后各运行一次短时场景：每个 provider
的正常与 5 ms 慢远端模式各预热至少 3 秒、测量 20 秒，总计四个窗口。远端查询
可能使最后一次预热请求超出目标时间；报告会记录实际预热耗时。两次运行必须使用同一
runner、workload、fixture、基础配置和平台，且顺序执行。

```bash
source docker/iceberg-rest/runtime/current/env.sh
export NOVAROCKS_UEA4A4_ICEBERG_FIXTURE_MANIFEST=/tmp/uea4a4-iceberg-c00-20260923-7dfc/manifest.json
export NOVAROCKS_UEA4A4_PAIMON_FIXTURE_MANIFEST=/tmp/uea4a4-paimon-smoke/manifest.json

RUNNER=target/debug/novarocks-system-tests
COMMON_ARGS=(--config "$PWD/tools/ci/fixtures/system-scenarios-base.toml"
  --cluster-size 3 --timeout-secs 300 --launch-profile performance
  --only uea4/catalog-planning-performance)
"$RUNNER" --binary "$UEA4A4_B0_BINARY" --artifact-root "$PWD/reports/uea4a4-b0" "${COMMON_ARGS[@]}"
"$RUNNER" --binary "$PWD/target/release/novarocks" --artifact-root "$PWD/reports/uea4a4-candidate" "${COMMON_ARGS[@]}"

python3 tests/benchmarks/uea4a4/compare.py \
  --baseline "$PWD/reports/uea4a4-b0/uea4-catalog-planning-performance" \
  --candidate "$PWD/reports/uea4a4-candidate/uea4-catalog-planning-performance"
```

运行报告记录四个窗口的吞吐、p95、完成数、错误数、窗口后完成、预热时长、慢远端
S3 GET/HEAD 增量和 FE/三个 BE 的 RSS 峰值；`process-resources.json` 留存原始采样。
每个慢窗口必须真的命中延迟代理，所有窗口均须有完成请求且无错误。比较器逐个
provider/mode 检查 candidate 吞吐不低于 B0 的 50%、p95 不高于 B0 的两倍；RSS
只记录供人工判断。它要求场景成功、同一 workload/fixture/基础配置/lockfile/平台/runner，
并核对每次报告与自身渲染配置的 SHA。两次渲染配置 SHA 可因任务私有慢速 S3 代理端口
而不同。比较器还验证两个不同 server binary 的 SHA。短窗口的波动可能较大，比较通过只表示未发现
大幅回退；后续性能细节由人工测试。
