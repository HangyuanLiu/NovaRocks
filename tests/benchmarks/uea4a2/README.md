# UEA-4A-2 release benchmark protocol

本目录是 I00/V4 的离线驱动与比较门。`workload.json` 只写入 plan v7 已固定的判据。`frozen` 下的 `null` 不是默认值：I00 须在首次 G1 测量前基于 G0 A/A、真实 fixture、平台和负载观测填入、说明理由，并提交冻结版本。当前脚本会明确拒绝运行。不得从 G1–G3 结果倒推数值。

先由 I00 发布独立的 Spark/parquet-mr、Trino、Flink、PyArrow 真正写入的 fixture 和逐行/绝对位置 oracle；不能用一个 writer 改标签。`fixture_manifest_path` 及 SHA 指向这个不可变输入。`base_config_path` 及 SHA 指向共同基础配置。填定 B、N、tail probe、暂停释放、恢复滞回、观察桶、RSS 各负载数值门、固定机器容量和采样方法。`sample_extension_rule` 要在正式测量前明确不足 1000 样本时如何补采，并对各组一致适用。`short_query_workloads` 至少包含普通短查询及慢扫混合中的实际文件 I/O 短查询；`rss_workloads` 覆盖宽表大行组、慢下游、多 Task、RF 后到和小文件。当前没有可核实的 SQL、数据规模和数值门，因此这些字段均为空。

先运行 G0 A/A pilot 来冻结 RSS 数值门。pilot 使用顶层 `pilot` 中明确的单文件短查询 Iceberg SQL/oracle、客户端数、最小请求间隔和预热时间，`--fixture-manifest` 必须指向 READY 且附件哈希匹配的真实 UEA-4A-2 Iceberg 输入；正式 `frozen.*` 和补采规则可保持 `null`。这张表由 Spark 真实写入 4096 行，当前快照一个 Parquet 数据文件，`id` 为 0–4095；谓词 `id BETWEEN 1 AND 10` 的预期 count 为 10，runner 仍逐次核验结果。原 A4 的 240 文件表留作多文件/宽扫描负载。G0 四客户端无间隔试跑在约 8000 次查询后遇到 Worker admission 拒绝；单客户端 20 秒校准仍达到 1342 次，因此固定单客户端、25 ms 最小请求间隔，使 120 秒请求数低于既有的 8192 条 admission 保留记录上限，并保留不少于 1000 个样本。间隔只控制下一次查询的起始时间，不计入查询延迟。传入同一个 f07 release binary 两次，分别输出 `g0-a`、`g0-b` 的原始查询、代理事件及资源 trace。pilot 检查两次均有完整 3×120 秒、每窗至少 1000 条在窗内成功完成的查询、零错误、各 BE 三轮 RSS 及哈希附件；它从原始 trace 重算吞吐、p95，并在 `pilot-aa.json` 检查 A/A 波动不超过 10%/20%。pilot 不判定 candidate 通过，也不生成正式 `input-hashes.json`。主 agent 用该证据选择 RSS 门限并记录理由，随后冻结 manifest 并重新做正式五次运行。

v2 pilot 收据给每个窗口保存 RSS 的 idle、预热、测量与 drain 后精确时间边界。离线门从带哈希的原始进程 trace 重算逐 BE idle、全过程峰值、后半窗稳态和 drain 后中位数，并核对进程身份。代理原始事件记录其自身的单调时钟，窗口另有代理时间边界和请求/上游字节计数；同毫秒边界仍以 snapshot 计数为准。当前 FE metadata 与 BE data 共用代理端点，连接计数代表混合来源，不能用作纯 BE 连接数。

`analyze_g0.py` 从 v2 pilot 的哈希原始附件重算每轮逐 BE 进程 CPU 秒数与核心等效利用率，并按已标记数据对象将代理连接分为仅数据、仅 metadata、混合三类。只有全部数据对象已标记、原生 BE placement 已核对、且没有混合连接时，才可把数据连接解释为 BE-facing 连接。进程 CPU 不是解码 CPU；旧 reader 的 `ConnectorFileDecodeTime` 会包含同步 I/O 等待，不能用于 CPU/等待分栏。

旧 v2 pilot 的代理和客户端驱动位于同一个 runner 进程，资源 trace 未采该进程；不能从旧收据推断 proxy CPU/RSS 或 proxy→MinIO 新建连接。

v3 pilot 把 `runner-proxy` 混合进程加入资源 trace，并由代理的 reqwest connector 层直接计数 proxy→MinIO 建连尝试和成功次数。它们不等于 GET 次数；`runner-proxy` CPU/RSS 仍混有客户端和编排成本，不能称为纯代理成本。先前 v2 A/A 收据保留原样作历史校准。最终 v3 A/A 在 `reports/uea4a2/g0-pilot-v3/`：两次独立原生 1FE+3BE、各 3×120 秒均通过；六窗吞吐波动 0.835%、p95 波动 14.832%，小于冻结的 10%/20% pilot 门；每轮 proxy→MinIO 生命周期仅新建 1 条 HTTP/1.1 连接，测量窗内没有新建。此结论只覆盖单文件短查询，不代替后续正式负载与 RSS/control 门。

```bash
python3 tests/benchmarks/uea4a2/analyze_g0.py \
  reports/uea4a2/g0-pilot-v2/g0-a/uea4-iceberg-range-performance/uea4a2-performance.json \
  reports/uea4a2/g0-pilot-v2/g0-b/uea4-iceberg-range-performance/uea4a2-performance.json
```

```bash
python3 tests/benchmarks/uea4a2/run.py --baseline-pilot \
  --g0 "$UEA4A2_G0_BIN" --runner "$UEA4A2_RUNNER" \
  --manifest tests/benchmarks/uea4a2/workload.json \
  --fixture-manifest "$UEA4A2_FIXTURE_MANIFEST" \
  --config "$UEA4A2_BASE_CONFIG" \
  --output reports/uea4a2/g0-pilot
```

同一最终 runner 已注册 `uea4/iceberg-range-performance`，接受 `NOVAROCKS_UEA4A2_WORKLOAD_MANIFEST` 与 `NOVAROCKS_UEA4A2_FIXTURE_MANIFEST`，在 `--launch-profile performance --cluster-size 3` 下运行。当前只实现 G0 pilot；正式 G0–G3 路径在 I03 归因及四写入端 oracle 完成前明确拒绝。完成后 G0、G1、G2、G3 使用各自 release 检查点 binary，脚本先对 G0 两次独立运行，再依次运行三个 candidate。runner 必须真正执行每负载三轮 120 秒，先排空预热；正常组的错误/超时为零。

```bash
python3 tests/benchmarks/uea4a2/run.py \
  --g0 "$UEA4A2_G0_BIN" --g1 "$UEA4A2_G1_BIN" \
  --g2 "$UEA4A2_G2_BIN" --g3 "$UEA4A2_G3_BIN" \
  --runner "$UEA4A2_RUNNER" \
  --manifest tests/benchmarks/uea4a2/workload.json \
  --output reports/uea4a2/performance
python3 tests/benchmarks/uea4a2/compare.py \
  --manifest tests/benchmarks/uea4a2/workload.json \
  --output reports/uea4a2/performance
```

每次 runner 运行须在 `uea4-iceberg-range-performance/` 写 `scenario-evidence.json` 和 `uea4a2-performance.json`。后者的 version-1 协议包含 `group`、`topology`、workload/fixture/base-config/runner/binary SHA、`effective_config_sha256`、`oracle_passed`、`attribution_complete`，以及下列原始结果：

- `windows`: 每个短查询负载三个 `{workload,repetition,duration_ms,started_ms,warmup_drained,tail_drained,tail_drain_ms,queries}`。每个 query 含 `started_ms,ended_ms,status`。在窗口内发起、窗口内成功结束才进入吞吐分子；窗口内发起但晚完成仍进入完整延迟样本和尾部记录。比较器拒绝缺终态、失败和超时；按 nearest-rank 从完整成功样本重算 p95/p99。
- `controls`: 每个控制类型分别有 `no-scan` 和 `saturated` 的至少 1000 个 `latency_ms`。满载记录 `hold_filled_window`、`flow_has_pending_demand`、正的 `flow_bytes_growth`。这两种证据分别代表等待满窗和持续数据返回；控制结果不能充当取消后真实 drain。
- `rss`: 每个冻结负载、每个 `be-0..2` 各三轮，含 `idle_bytes,peak_bytes,steady_bytes,post_drain_bytes,observed_post_drain_seconds` 和归零的 current/next/claim/undrained 数。RSS 样本须由预热到 drain 后观察期的原始 trace 支持；稳态是测量后半段中位数，idle 是负载前等长采样。峰值三轮取最大，稳态三轮取中位数；不跨 BE 平均。
- `attachments`: `query_trace,resource_trace,control_trace,operation_trace` 均为相对于 receipt 目录的 `{path,sha256}`；附件保存原始事件、每 BE CPU/等待与连接观测、proxy 分栏、真实读退出、范围/请求/复制、RF/后继与慢下游因果证据。`attribution_complete` 只能在这些原始附件及语义/位置 oracle 均核验后置 true；具体事件 schema 由 I00 与 runner 一起冻结。`scenario-evidence.json` 提供真实原生拓扑、performance profile、成功退出和有效配置 SHA。

`run.py` 在五组 receipt 到齐后调用比较器并写 `comparison.json`。比较输出保留每项门的原值，退出码 `0` 为全部通过、`1` 为有实测失败、`2` 为证据无效/缺失。G0 A/A 对两次运行合计六轮的原始吞吐与 p95 检查 `(max-min)/median` ≤10%/20%，不能先取两组中位数掩盖组内波动；最终 G3 对 G0 的三轮中位数比较吞吐退化≤15%、p95 增长≤25%。G1、G2、G3 每组均检查冻结 RSS 门；控制 p99 每类型≤2 秒且≤同组无扫描压力的 3 倍。G1/G2 的性能差值仅用于分项归因，不选择最佳一轮。缺 fixture、场景、hash、样本、tail drain 或 BE 观测不能标为通过。

离线自检：`python3 -m unittest discover -s tests/benchmarks/uea4a2 -p 'test_*.py'`。这只验证驱动逻辑，不是 V4 产品证据。
