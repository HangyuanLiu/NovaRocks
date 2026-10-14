# Catalog planning 功能fixture

Paimon 产品测试暂停，等待详细 connector 设计；本目录保留 Iceberg/Paimon producer、历史配方和独立 oracle。原有五个 mixed catalog-planning consumer 已关闭；连同六个 connector 场景、两个 scan-readiness 场景，共 13 个依赖 Paimon 的 system 场景均明确拒绝选择，暂停不计为验收通过。生产 Paimon provider 保留。旧计时 workload、B0/candidate 比较器和短窗口协议已退出；目录名是历史布局。

`iceberg_fixture.py`、`paimon_fixture.py`经已验证的本机fixture publication和真实Spark写入任务私有表。manifest绑定snapshot/schema、至少16真实datafiles、行数及独立oracle、objects.json hash和READY。Iceberg还记录REST URI；两者记录warehouse、S3 endpoint与credential身份，不保存密钥。不能从旧smoke绝对路径推定当前健康。

以下是保留的手工输入配方，不启动已暂停的 product test。prepare/verify/cleanup 工具仍可显式调用；CI 不再准备 Paimon 输入。

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

消费环境变量为 `NOVAROCKS_UEA4A4_ICEBERG_FIXTURE_MANIFEST`、`NOVAROCKS_UEA4A4_PAIMON_FIXTURE_MANIFEST`，以及既有 `NOVAROCKS_UEA4A4_S3_ACCESS_KEY_ID`/`NOVAROCKS_UEA4A4_S3_SECRET_ACCESS_KEY`。保留的 consumer 实现拒绝缺 READY/hash/对象/凭据，使用私有 proxy 明确区分 metadata 与 data 请求；当前 registry 在输入准备前拒绝启动它们，不读取旧 workload 时间窗或门限。

已关闭、仅保留实现的 selector：`uea4/catalog-planning-smoke`、`uea4/catalog-planning-responsibility`、`uea4/catalog-planning-no-fe-quota`、`uea4/iceberg-held-read-cancellation`、`catalog/provider-read-queued-progress`。历史场景使用 clean `--launch-profile performance --cluster-size 3`，验证真实请求、provider进展、精确hold终态/取消与FE/BE owner收敛；profile名称不是性能实验。所有actor失败须cancel、release hold并实际join，原始错误与cleanup错误保留。

原先要求对应修改必跑本功能组的规则随 Paimon 测试一并暂停，当前不执行手工维护轮次或自动调度；后续详细设计再决定恢复范围。共享输入工具与私有 fixture cleanup 继续保留。命令骨架与材料位置见 [system指南](../../system-test-runner/README.md)。
