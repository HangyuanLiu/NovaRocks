# P09 私有 stock HMS capability preflight

本工具只调查真实 stock Java HiveCatalog 的 capability：在一个 task-private UUID namespace 中创建一个空 Iceberg table 和一个真实 Iceberg view，第二个独立 Spark invocation 实际 list/load，再实际 drop 和恢复 namespace 基线。它不启动 NovaRocks、不产生 bulk READY，也不替代原 CL 的 32×512 tables+512 views/page256/clients1,8,16；Native HMS mixed 分类及 Rust view Unsupported 的验收状态由主线独立记录。

固定入口：

- helper：`docs/testing/mem-1-m07/scripts/prepare_real_hms_capability.py`
- host-only 边界测试：`docs/testing/mem-1-m07/scripts/test_prepare_real_hms_capability.py`
- freeze 模板：`docs/testing/mem-1-m07/inputs/real-hms-capability-preflight-freeze-v1.json`

## 模板与执行副本

模板仍为 `review_status=draft`、`frozen_before_execution=false`，直接使用会在 owner 创建前拒绝。`source_revision=null` 是唯一未绑定的源码身份字段：主 agent 先审阅、应用并提交 promotion，然后从模板生成 ignored 的独立不可变执行副本，将它绑定到实际干净已提交的 HEAD；不要向 tracked 模板写入包含其自身的尚未生成 commit。

模板的 helper SHA256 是推广后源文件的真实 hash `68680324046fac51becbee6b389c719be44f39a26e7de1f7d576547c01553c23`；Scala 模板 SHA256 `265c62c47b6b62a487e2bd9163ad6fb6b6f1d2deb60034a4fa3a58365e658933`。execution copy 必须复核 helper、Scala、17 个源文件 pins、实际 BOM/canonical lock、实际 stock HMS/writer image、JAR 长度/SHA1 和本机绝对路径，最后才设置 `review_status=reviewed`、`frozen_before_execution=true`。模板的 `source.docker_binary` / `fixture_store` 是既有本机锁定输入路径，不是环境猜测；换主机时必须明确审阅并绑定实际输入。schema 使用 exact keys，没有额外 `helper_path` 字段；source 路径由本说明和实际 CLI 文件固定，helper 自身核对 `__file__` hash。

CLI（仅示例，不表示已执行 provider）：

```text
python3 docs/testing/mem-1-m07/scripts/prepare_real_hms_capability.py \
  --freeze logs/mem-1-m07/<reviewed-immutable-freeze>.json \
  --output logs/mem-1-m07/<new-private-artifact-dir>
```

输出目录须新建且位于本 worktree ignored `logs/mem-1-m07/`。不接受已有共享 owner、猜测 endpoint/warehouse、未知 JAR/image 或 create retry。工具按 exact canonical RuntimeOwner/HiveOwner 创建唯一私有 owner，解析一次真实 publication 并核对实际 manifest/daemon/container/image/binding；endpoint、actual HMS warehouse 和所有资源身份来自 owner 事实。

## 冻结范围与资源退出

work absolute deadline 1200s，cleanup reserve 240s，同一开始时间派生总 wall 1440s；owner command180s、Spark stage240s、inspection15s、writer exit45s、host reap5s。所有派生时钟受剩余 stage/work/wall 限制，不刷新期限。stdout/stderr 每条 child ≤1MiB、stdin ≤4096B、Scala ≤32768B、单 marker ≤65536B、每 stage ≤16 markers、baseline namespaces ≤128、raw metadata ≤1MiB、local file ≤1MiB、JAR streaming block65536B。端口范围28250..28499；数值与 v5 相同。

四个独立 stock Spark JVM invocation：create/oracle/drop/restored。实际 metadata oracle 核对 required long id、fieldID1/schemaID0、table format2/UUID/location、actual partition specID0/empty fields/count1/defaultSpecID0、snapshot null/count0，以及 view format1/UUID/currentVersion1/schema0/defaultCatalog/defaultNamespace/唯一 Spark SQL representation。同一个 oracle invocation 通过公开第二个 HiveCatalog `LIST_ALL_TABLES=true` 核对 all objects 恰为 {cap_table,cap_view}，不能当作 Native mixed 分类 PASS。实际 JAR API/长度47964645/SHA1 `86eb12917658be2c8dd8982ee0c57cfece862591` 锁定，Java source HEAD 不冒充 JAR build。无反射/private API、fork 或伪造空 views。

所有直接 owned child 的退出事实先收敛，再 exact HMS purge→REST unbind/purge→catalog/object cleanup。host leader 已退出不等于 whole group 退出，wrapper group gone 也不覆盖实际 `Docker.command(start_new_session=True)` 的 detached child。只有完整 bounded 正常 exit0 + owned group gone 才可沿 pinned owner 同步内部调用的正常返回依据继续；timeout/signal/overflow/unknown/cancellation 等缺少 detached settlement 事实时保存 whole-failure retention barrier，停止后续 writer/fixture destructive cleanup，不以空 children 清除 barrier，不自动重发 unknown create。

捕获 BaseException，包括 KeyboardInterrupt/SystemExit：保留第一主因的安全 class/reason hash、bounded 真实 partial bytes/hash 和有限 reap/group facts；不能因 secondary reap/selectorclose 覆盖第一主因或绕过 sticky。不能证明退出即 resource-retained whole failure。重复取消/OS hard kill 可能使失败收据无法完整落盘，只有实际正常 exit0 与完整成功收据同时存在才可记录 capability 通过。失败保留 exact private binding/identity；不得误删共享 fixture，也不扫描全机 PID 或杀猜测 detached group。

## host-only 检查的范围

```text
python3 docs/testing/mem-1-m07/scripts/test_prepare_real_hms_capability.py
```

三个测试只启动 Python 小 child：正常 capture/no kill、128B output cap 超界的真实 partial+sticky、Popen 后 selector KeyboardInterrupt 的实际 class+sticky。它们不启动 Docker/provider；即使通过也只证明 host capture 边界，不是 HMS capability、Native 或 CL acceptance。promotion 本身仅进行 AST/JSON/hash/diff/apply-check 静态核对，主 agent 负责随后串行执行。

## v5 promotion 机械差异

原 helper SHA256 `c6a23e85402eed40c0538da8c1fc4f075de0127b569d0a3dedb4b8a1c66df6c8`，原 freeze SHA256 `170f11b6f27b0b47fffd74e98c3041526122e0018a1ea0b190b0dec124f0b248`，原 host-only test SHA256 `5340b5cac1600e3aa3614363f4f19fe00f0edc5a4e1b8cd55a46313748bbb41b`；原文件字节保留。

helper 仅改 Apache header 的句间空格/空行、docstring 去除 draft 标签、REPO 从 ignored 路径的 `parents[2]` 改为 scripts 路径的 `parents[4]`。host-only test 仅增加相同 Apache header/shebang，loader 改成实际同目录文件名；测试断言不变。freeze 仅更新 helper hash、将旧 efb 源码身份替换为显式未绑定 null；其他字段逐字段相同。Scala/API/限额/cleanup/BaseException 语义不变。本说明是新的实际路径文档，历史 v1–v5 notes 不覆盖。

## 首次真实运行纠正：一次连接而非零次

clean 5fe6f4dfd首次预检只产生RuntimeMetaException failure marker，create没有成功mutation记录；完整private cleanup成功，失败收据保留于evidence/p09-hms-capability-preflight-v1-failed-20261009.json。实际stock Spark image内hive-metastore-2.3.9.jar SHA256为224b4a59344ff8136a68c0033801390f20d4d01c30ea5fd5dd9c4592f9c8a9ef。javap ctor/open证明METASTORETHRIFTCONNECTIONRETRIES是总连接轮数，原0在初始attempt0时直接退出连接loop。

当前input明确hms_connect_attempts=1，映射Hive属性hive.metastore.connect.retries=1并typed readback为1；failure retries仍0。只允许一个初始connection round，没有额外retry。仓库helper原先误读第三方字段，纠正该测试工具不更改产品或原CL输入；所有其他scope/bounds/pins/image/JAR/API相同。旧immutable execution freeze不覆盖，后续运行绑定新的clean HEAD、新UUID private root与新immutable freeze，不重试上次未知创建。上文v5 promotion段落是历史机械推广边界；当前helper/Scala hashes反映此纠正。
