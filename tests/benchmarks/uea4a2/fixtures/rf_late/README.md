# UEA-4A-2 RF 后到夹具

本目录固定一份由 **PyArrow 23.0.1 实际写出**的 Parquet 输入。`probe.parquet` 是同一个文件中的 32 个真实行组，每组 4096 行；第 0、31 组的 `k=3`，中间 30 组各有不同且不匹配的 `k`。`build.parquet` 只有 4 行，其中 `k=3, flag='Y'` 是唯一有效的 build 行。两文件字段均有 Parquet field ID，可分别作为 Iceberg `probe_v1`、`build_v1` 表的单个 data file。`manifest.json` 固定每组的行数、压缩字节、key 上下界、文件 SHA-256、writer footer 和生成脚本 SHA-256；`READY` 绑定 manifest。

只读离线校验：

```bash
PYTHONDONTWRITEBYTECODE=1 python3 tests/benchmarks/uea4a2/fixtures/rf_late/fixture.py verify
```

复现生成必须在空目录显式执行 `fixture.py generate --directory <new-directory>`，要求同版 PyArrow。校验器逐行重新构造预期数据，并比较物理行组与 field ID。固定 join oracle：`COUNT(*) = 8192`、`SUM(p.id) = 536866816`；`SUM(p.payload % 997)` 的数值在 `oracle.json` 中。查询形式为 `probe p JOIN build b ON p.k=b.k WHERE b.flag='Y'`，应在原生场景中使用固定的完整表名。

本地 Parquet 和离线 READY **不是 Iceberg snapshot**。本工作树的 REST/MinIO 发布已完成，`published.json` 固定 probe snapshot `3873451287235817704` 与 build snapshot `7646664860656143673`，绑定两份远端对象 SHA-256。原生试验前先在线验证：

```bash
source docker/iceberg-rest/runtime/current/env.sh
PYTHONDONTWRITEBYTECODE=1 python3 tests/benchmarks/uea4a2/fixtures/rf_late/publish.py verify-published \
  --receipt tests/benchmarks/uea4a2/fixtures/rf_late/published.json
```

发布器只创建任务私有 namespace `uea4a2_rf_late_20260923` 下的两张新表，上传已校验的文件并用 PyIceberg `add_files` 形成快照；已存在同名表时拒绝修改。若需在独立环境重新发布，显式选择新 namespace 和新 receipt，先运行 `publish`，再运行 `verify-published`，并登记新的实验版本。已发布和在线校验仍不代表原生 RF 到达时序已经验证。

`probe_v1` 的 DataFile 包含 32 个 `split_offsets`，因而原生调度会把一份物理 Parquet 拆成 32 个 split。为同一 split 内的 RF 后到验证，`publish_whole.py` 在同一 namespace 另建 `probe_whole_v1`，上传与原 probe **完全同 SHA-256** 的文件，保留物理 32 行组，仅在新 Iceberg DataFile 中设 `split_offsets=null`。它不修改 `probe_v1` 或 `build_v1`。新 snapshot 为 `4264844081461962022`；`whole_published.json` 同时绑定原 snapshot `3873451287235817704`、READY、对象哈希与行组布局。离线顶层 `verify_manifest.py --verify` 对两份发布收据交叉核对；在线核验命令：

```bash
source docker/iceberg-rest/runtime/current/env.sh
PYTHONDONTWRITEBYTECODE=1 python3 tests/benchmarks/uea4a2/fixtures/rf_late/publish_whole.py verify-published
```

行组布局只提供 RF 后到的物理条件。场景仍须在同一个 probe split 内观测第 0 行组已消费、RF subscription 到达，并控制 build 侧精确 Range 的释放时点；仅比较最终 join 行数不能证明 RF 后到或后续行组剪枝。
