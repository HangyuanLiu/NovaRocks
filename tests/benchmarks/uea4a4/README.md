# Catalog planning 功能fixture

保留的Iceberg/Paimon producer为原生planning/queue/取消功能场景提供真实READY输入；旧计时workload、B0/candidate比较器和短窗口协议已退出。目录名是历史布局，当前只维护功能与数据正确性。

`iceberg_fixture.py`、`paimon_fixture.py`经已验证的本机fixture publication和真实Spark写入任务私有表。manifest绑定snapshot/schema、至少16真实datafiles、行数及独立oracle、objects.json hash和READY。Iceberg还记录REST URI；两者记录warehouse、S3 endpoint与credential身份，不保存密钥。不能从旧smoke绝对路径推定当前健康。

```bash
python3 tests/benchmarks/uea4a4/iceberg_fixture.py prepare \
  --run-id "$run_id" --output-dir "$attempt/iceberg-input" --env-file "$NOVA_ENV_REST_ENV_FILE"
python3 tests/benchmarks/uea4a4/iceberg_fixture.py verify \
  --output-dir "$attempt/iceberg-input" --env-file "$NOVA_ENV_REST_ENV_FILE"
python3 tests/benchmarks/uea4a4/paimon_fixture.py prepare \
  --run-id "$run_id" --output-dir "$attempt/paimon-input" --env-file "$NOVA_ENV_REST_ENV_FILE"
python3 tests/benchmarks/uea4a4/paimon_fixture.py verify \
  --output-dir "$attempt/paimon-input" --env-file "$NOVA_ENV_REST_ENV_FILE"
```

`attempt`为仓库外独占目录，`run_id`使用新的合法私有身份。prepare和verify是分开的输入owner动作；verify不补供给。cleanup要求与READY完全一致的run-id，只删除所属对象，失败保留原材料。`--dry-run`只渲染作用域，不算供给成功。

消费环境变量为 `NOVAROCKS_UEA4A4_ICEBERG_FIXTURE_MANIFEST`、`NOVAROCKS_UEA4A4_PAIMON_FIXTURE_MANIFEST`，以及既有 `NOVAROCKS_UEA4A4_S3_ACCESS_KEY_ID`/`NOVAROCKS_UEA4A4_S3_SECRET_ACCESS_KEY`。功能consumer拒绝缺READY/hash/对象/凭据，使用私有proxy明确区分metadata与data请求，不读取旧workload时间窗或门限。

保留selector：`uea4/catalog-planning-smoke`、`uea4/catalog-planning-responsibility`、`uea4/catalog-planning-no-fe-quota`、`uea4/iceberg-held-read-cancellation`、`catalog/provider-read-queued-progress`。它们使用clean `--launch-profile performance --cluster-size 3`，验证真实请求、provider进展、精确hold终态/取消与FE/BE owner收敛；profile名称不是性能实验。所有actor失败须cancel、release hold并实际join，原始错误与cleanup错误保留。

FE/planning/provider/admission或fixture修改必跑对应功能组；成本为真实两个provider输入和独立1FE+3BE，每周领域维护者手工轮次，不扩默认CI，自动调度尚未交付。命令骨架与材料位置见 [system指南](../../system-test-runner/README.md)。
