# M07 fixture 输入准备记录

2026-10-01，`DOCKER_CONTEXT=desktop-linux`。本记录仅证明本地 fixture 输入及镜像准备完成，不代表产品测试或 Linux 验收通过。

原 Paimon derived definition `320bd65abd65fbdc137859f2f6aa8662a3363bb66dd3b05fd8785ab75f9d7abd` 与当前 `23dc030b71d3b3f354bb90102e71078dda857d53a2475b3cd744f5ef5feb8187` 的差异来自已合入的 fixture.py 环境解析改动；Dockerfile、versions.env、JAR lock 与 golden 均未修改。

此次使用一次性 helper（源码留存在 `offline-refresh-helper.py.txt`），复用现有 fixture owner 的 exclusive lock、load_lock、verify_image、validate_artifact 与 atomic_json。旧 READY/BOM 的 lock、5 个本地 canonical image 的 digest/platform/alias、6 个 JAR 的长度/SHA1/SHA256 receipt 先全部核验；仅允许已核实的旧 Paimon 定义漂移。JAR 复制到新 staging 后再次核验，没有硬链接。

真实构建通过 `DOCKER_BUILDKIT=0` 的 classic builder 执行 `docker build --network none --pull=false --platform linux/amd64`，使用本地锁定 Spark base 和两个本地 JAR；无 download、pull 或网络操作。完整具体命令和 digest 见 `offline-refresh.json`，Docker COPY、容器内尺寸/SHA1 验证及构建成功记录见 `offline-build.log`。

Paimon 新 image ID：`sha256:e2e6b0f55c4c2066bd1e152e47105284fcbf72c51a68cd88f3019cf480b04473`。旧 image ID 保留为 `novarocks/fixture-paimon-writer:m07-before-603e93db17fa4ccca991c22f7f38445c`。Iceberg derived image ID `sha256:fd53681a8c2bc0cc9c684c0f688e98d199ac1b654438d1f2ab61c41218c7f153` 未变化。

新 artifact generation：`generation-ccab305f1f7a46d4977fd9ccbd1c9a1a`；原 generation `generation-27fe20159d284a7fad6118c3518cea46` 保留。新 BOM 含 `prepared_at`，由 owner 的原子 JSON publication 发布；生成目录还保存 previous-bom.json 和 previous-READY。Helper 在发布失败时恢复原 current alias/BOM/READY。

最终执行：

```bash
DOCKER_CONTEXT=desktop-linux docker/fixture-inputs/verify.sh --consumer all --json
```

退出码 0。结果见 `verify-all.json` 和 `verification-receipt.json`。全部 7 个预先存在的容器，其 ID、image ID、运行状态及 StartedAt 完全一致，见 `containers-preserved.json`。没有修改 product source、golden 或锁定义，没有运行 Cargo、SQL、workspace 测试，也没有 commit/push。

`bom.json` / `identity.json` 是重建前已通过的 Iceberg REST 输入与当前运行 publication 快照；`all-consumer-definition-check.txt` 保留首次全 consumer 因 Paimon 定义过期而拒绝的原始结果。它们属于历史证据；当前全 consumer 输入结论以 `verify-all.json` / `verification-receipt.json` 为准。主 agent 独立复核相同 strict all 命令也退出0，保存为 `verify-all-main-confirm.json`。
