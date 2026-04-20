---
name: starrocks-fe-on-novarocks
description: Use when StarRocks FE Java changes need to be built, deployed into a NovaRocks runtime, and validated end to end.
---

# FE 开发与 NovaRocks 联调

目标：修改 StarRocks FE 代码后，在 NovaRocks 环境中完成构建、部署、验证的完整闭环。

本 skill 统一使用环境变量表达路径；如果未显式设置，则按下面的默认规则初始化：

```bash
CURRENT_DIR_NAME=$(basename "$(pwd)" | tr '[:upper:]' '[:lower:]')

if [ "${CURRENT_DIR_NAME}" = "starrocks" ]; then
  STARROCKS_ROOT=$(pwd)
else
  STARROCKS_ROOT="${STARROCKS_ROOT:-$HOME/project/starrocks}"
fi

if [ "${CURRENT_DIR_NAME}" = "novarocks" ]; then
  NOVAROCKS_ROOT=$(pwd)
else
  NOVAROCKS_ROOT="${NOVAROCKS_ROOT:-$HOME/project/NovaRocks}"
fi

DEPLOY_ROOT="${DEPLOY_ROOT:-$HOME/starrocks-on-novarocks}"
FE_RUNTIME_ROOT="${FE_RUNTIME_ROOT:-${DEPLOY_ROOT}/fe}"
BE_RUNTIME_ROOT="${BE_RUNTIME_ROOT:-${DEPLOY_ROOT}/novarocks}"
FE_CONF="${FE_CONF:-${FE_RUNTIME_ROOT}/conf/fe.conf}"
BE_CONF="${BE_CONF:-${BE_RUNTIME_ROOT}/conf/novarocks.toml}"
STARROCKS_THIRDPARTY_41="${STARROCKS_THIRDPARTY_41:-$HOME/project/thirdparty-4.1}"
STARROCKS_THIRDPARTY_MAIN="${STARROCKS_THIRDPARTY_MAIN:-$HOME/project/thirdparty}"
```

如果调用本 skill 时当前目录名是 `starrocks` 或 `novarocks`，则对应的 `*_ROOT` 自动取 `pwd`。否则分别回退到默认值：

- `STARROCKS_ROOT`: `~/project/starrocks`
- `NOVAROCKS_ROOT`: `~/project/NovaRocks`

后续命令都应基于这些环境变量展开，不再直接写死绝对路径。

## 1) 目录约定

| 角色 | 路径 | 说明 |
|------|------|------|
| StarRocks FE 源码 | `${STARROCKS_ROOT}` | FE Java 代码在此修改和构建 |
| NovaRocks BE 源码 | `${NOVAROCKS_ROOT}` | Rust BE 代码在此修改和构建 |
| 部署根目录 | `${DEPLOY_ROOT}` | FE/BE 运行时统一部署在此 |
| FE 运行目录 | `${FE_RUNTIME_ROOT}` | StarRocks FE 运行目录，jar 从 StarRocks 构建产物复制到此 |
| BE 运行目录 | `${BE_RUNTIME_ROOT}` | NovaRocks runtime package 目录 |
| FE 构建产物 | `${STARROCKS_ROOT}/output/fe/lib/` | `./build.sh --fe` 的输出 |
| FE 配置 | `${FE_CONF}` | 端口定义（`query_port`、`http_port`） |
| BE 配置 | `${BE_CONF}` | 端口定义（`heartbeat_port`、`be_port` 等） |

## 2) FE 构建与部署

### 构建 FE

先显式设置 `STARROCKS_THIRDPARTY`，不要假设非交互 shell 会自动加载 `~/.zshrc`：

```bash
# StarRocks 4.1 及更早分支（包括 4.1）
export STARROCKS_THIRDPARTY="${STARROCKS_THIRDPARTY_41}"

# StarRocks 4.1 之后的分支（包括 main）
# export STARROCKS_THIRDPARTY="${STARROCKS_THIRDPARTY_MAIN}"
```

规则：
- `branch-4.1` 及更早分支（包括 `branch-4.1`）使用 `${STARROCKS_THIRDPARTY_41}`
- `branch-4.1` 之后的分支和 `main` 使用 `${STARROCKS_THIRDPARTY_MAIN}`

这是为了和分支所依赖的 thirdparty/toolchain 保持一致，尤其是 `thrift` 版本必须匹配，否则 FE 构建可能生成错误的 thrift Java 源码。

```bash
./build.sh --fe
```

构建产物在 `${STARROCKS_ROOT}/output/fe/lib/` 下，关键文件：
- `fe-core-main.jar`（核心逻辑，~28MB）
- `starrocks-fe.jar`（启动入口，~5KB）
- 其他依赖 jar

### 部署到 FE 运行目录

**必须复制全部 jar**，不能只复制 `starrocks-fe.jar`（它只是个 wrapper，真正的代码在 `fe-core-main.jar`）：

```bash
cp "${STARROCKS_ROOT}"/output/fe/lib/*.jar \
  "${FE_RUNTIME_ROOT}"/lib/
```

### 验证部署

```bash
# 比对关键 jar 的大小和时间戳
ls -la "${FE_RUNTIME_ROOT}/lib/fe-core-main.jar"
ls -la "${STARROCKS_ROOT}/output/fe/lib/fe-core-main.jar"
```

### （可选）修改 NovaRocks BE 后重新打包到部署目录

如果联调同时涉及 NovaRocks Rust 代码，修改源码后用 `--package --output` 直接覆盖 BE 运行目录。

**关键：必须加 `--features compat`**，否则 brpc 通信层不会编译，FE 无法与 BE 正常交互（heartbeat、plan submission 等全部走 brpc/C++ shim）。没有 `compat` feature 的 binary 只能用于 standalone-server 模式。

```bash
cd "${NOVAROCKS_ROOT}"
./build.sh --release --package --output "${BE_RUNTIME_ROOT}" --features compat
```

等价的直接 cargo 命令：
```bash
cargo build --release --features compat
```

## 3) 集群启停

### 端口约束

从配置文件读取，**不要硬编码**：

```bash
QUERY_PORT=$(
  grep -E '^[[:space:]]*query_port[[:space:]]*=' \
    "${FE_CONF}" |
  awk -F= '{gsub(/[[:space:]]/, "", $2); print $2}'
)

HEARTBEAT_PORT=$(
  grep -E '^[[:space:]]*heartbeat_port[[:space:]]*=' \
    "${BE_CONF}" |
  awk -F= '{gsub(/[[:space:]]/, "", $2); print $2}'
)
```

### 启动顺序

1. **启动 FE**：
```bash
cd "${FE_RUNTIME_ROOT}"
bin/start_fe.sh --daemon
# 等待 FE 就绪（约 10-15 秒）
sleep 15
mysql -h 127.0.0.1 -P"${QUERY_PORT}" -u root -e "select 1"
```

2. **启动 NovaRocks BE**：
```bash
cd "${BE_RUNTIME_ROOT}"
./bin/novarocksctl start --daemon
```

3. **注册 BE**（仅 FE meta 清理后首次需要）：
```bash
mysql -h 127.0.0.1 -P"${QUERY_PORT}" -u root \
  -e "ALTER SYSTEM ADD BACKEND '127.0.0.1:${HEARTBEAT_PORT}'"
```

4. **验证集群就绪**：
```bash
mysql -h 127.0.0.1 -P"${QUERY_PORT}" -u root -e "SHOW BACKENDS"
# 确认 Alive = true
```

### 停止顺序

```bash
cd "${BE_RUNTIME_ROOT}"
./bin/novarocksctl stop

cd "${FE_RUNTIME_ROOT}"
bin/stop_fe.sh
```

### FE Meta 清理（新 jar 与旧 journal 不兼容时）

如果 FE 启动后 `fe.log` 报 journal replay 错误（如 `JournalInconsistentException`），需清理 meta：

```bash
cd "${FE_RUNTIME_ROOT}"
bin/stop_fe.sh
rm -rf "${FE_RUNTIME_ROOT}"/meta/* \
       "${FE_RUNTIME_ROOT}"/log/*
bin/start_fe.sh --daemon
# 重新注册 BE
```

**注意**：清理 meta 后所有 catalog、database、table 都会丢失，需重新创建。

## 4) 代理设置

在运行任何 SQL 测试或涉及 Iceberg/MinIO 的操作前，**必须禁用本地代理**：

```bash
export NO_PROXY=127.0.0.1,localhost
export no_proxy=127.0.0.1,localhost
unset HTTP_PROXY HTTPS_PROXY ALL_PROXY http_proxy https_proxy all_proxy
```

## 5) SQL 端到端测试

### 使用 sql-test-runner（推荐）

```bash
cd "${NOVAROCKS_ROOT}"

# 运行单个 case
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --suite <suite> \
  --only <case_name> \
  --mode verify

# 运行整个 suite
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --suite <suite> \
  -j 4 \
  --mode verify

# 运行多个 suite
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --suite iceberg,mv-on-iceberg \
  -j 4 \
  --mode verify
```

### Record 模式（生成/更新 result 文件）

Record 模式需要 `--ref-port`（参考 FE 端口，通常是另一个未修改的 FE）：

```bash
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --suite <suite> \
  --only <case_name> \
  --mode record \
  --update-expected \
  --ref-port <reference_fe_port>
```

如果没有参考 FE，可用当前 FE 自身的端口（适用于新增 case 而非行为对比）。

### 常用 suite

| Suite | 内容 | 说明 |
|-------|------|------|
| `iceberg` | Iceberg 读写、分区、裁剪 | 依赖 init.sql 创建 catalog |
| `mv-on-iceberg` | MV + Iceberg 联合测试 | 每个 case 自建 catalog |
| `materialized-view` | 本地 OLAP MV | 不涉及 Iceberg |

### Iceberg suite 的 UUID 机制

- `init.sql` 在 **suite 级别**执行，`${uuid0}` 是 suite 级别的值
- 每个 case 的 `${uuid0}` 是 **case 级别**的值（包含 case hash 后缀），与 suite 级别不同
- 引用 init.sql 创建的资源（如 Iceberg catalog）时，case 中应使用 `${suite_uuid0}`
- 引用 case 独有的资源（如 database、table）时，case 中使用 `${uuid0}`

```sql
-- 引用 init.sql 创建的共享 catalog
CREATE DATABASE iceberg_cat_${suite_uuid0}.my_db_${uuid0};
--             ^^^^^^^^^^^^^^^^^^^^^^^^      ^^^^^^^^
--             suite 级别（共享 catalog）     case 级别（隔离 db）
```

## 6) 典型开发闭环

```
1. 修改 FE Java 代码
   └─ vi ${STARROCKS_ROOT}/fe/fe-core/src/main/java/...

2. 构建 FE
   └─ cd ${STARROCKS_ROOT} && ./build.sh --fe

3. 部署到 FE 运行目录
   └─ cp ${STARROCKS_ROOT}/output/fe/lib/*.jar \
        ${FE_RUNTIME_ROOT}/lib/

4. 重启 FE（如需要）
   └─ cd ${FE_RUNTIME_ROOT}
   └─ bin/stop_fe.sh && bin/start_fe.sh --daemon

5. （可选）修改 NovaRocks BE Rust 代码
   └─ vi ${NOVAROCKS_ROOT}/src/...
   └─ cd ${NOVAROCKS_ROOT}
   └─ ./build.sh --release --package --output ${BE_RUNTIME_ROOT} --features compat
   └─ cd ${BE_RUNTIME_ROOT} && ./bin/novarocksctl restart --daemon

6. 运行测试验证
   └─ cd ${NOVAROCKS_ROOT}
   └─ cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
        --suite mv-on-iceberg --mode verify

7. 如果测试不过，检查 FE 日志
   └─ tail -100 ${FE_RUNTIME_ROOT}/log/fe.log | grep -i error
```

## 7) 常见问题排查

| 症状 | 原因 | 解决 |
|------|------|------|
| FE 启动后 `fe.log` 报 `JournalInconsistentException` | 新 jar 与旧 meta 不兼容 | 清理 meta：`rm -rf ${FE_RUNTIME_ROOT}/meta/* ${FE_RUNTIME_ROOT}/log/*` |
| `missing shard registry config for path=s3://...` | BE shard registry 缺少 S3 凭证 | 重启 BE：`cd ${BE_RUNTIME_ROOT} && ./bin/novarocksctl restart --daemon` |
| `Unknown catalog 'iceberg_cat_...'` | Iceberg test case 用了 case 级别 uuid 引用 suite 级别 catalog | 将 `iceberg_cat_${uuid0}` 改为 `iceberg_cat_${suite_uuid0}` |
| `Can't connect to MySQL server on 127.0.0.1:XXXX` | FE 未启动或端口不对 | 检查 `${FE_CONF}` 中的 `query_port`，确认 FE 进程存活 |
| REFRESH MV 失败 `fail to create tablet` | BE 刚注册，StarManager 尚未完成 shard 分配 | 等几秒后重试，或重启 BE |
| Iceberg INSERT 报 502/空响应 | 本地代理拦截了 localhost 请求 | 执行代理禁用命令（见第 4 节） |
| `cp` jar 后 FE 行为没变 | 只复制了 `starrocks-fe.jar`（5KB wrapper） | **必须复制全部 jar**：`cp ${STARROCKS_ROOT}/output/fe/lib/*.jar ${FE_RUNTIME_ROOT}/lib/` |
| BE 启动正常但 `SHOW WAREHOUSES` 显示 NodeCount=0，query 报 `No alive backend` | BE binary 编译时缺少 `--features compat`，brpc/C++ shim 未启用，FE 无法与 BE 正常通信 | **必须用 `--features compat` 编译**：`./build.sh --release --package --output ... --features compat` |
| BE Alive=true 但 query 报 `No alive backend`，`SHOW WAREHOUSES` NodeCount=0 | BE 曾因连接失败被 FE 自动拉黑（如非 compat build 期间） | 1. `SHOW BACKEND BLACKLIST` 确认 2. `DELETE BACKEND BLACKLIST <id>` 移除 3. 确保 compat build 后重启 BE，否则会被持续自动拉黑 |
