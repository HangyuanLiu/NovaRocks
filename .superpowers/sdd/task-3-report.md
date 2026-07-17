# CI-G1 Task 3 Report

基线提交：`f6934d5a9`。

## 变更

- 从已退役的 `architecture_guard` 提取 protobuf schema compatibility 契约到独立的 `proto_schema_compatibility` target。
- 保留 baseline 读取、比较、合并和受控写入语义，移除 `NIDL_D3B` 命名；不保留 current-schema exact-shape parser test。
- 删除旧 architecture guard、EBD 子模块和 NIDL non-compat IDL ledger，不创建替代的通用 guard。
- 将 state-store FoundationDB provider-isolation 测试改为现有 lexical/token helper：它精确定位 `StateStoreProviderConfig` enum 的顶层 `Foundationdb` variant，并拒绝该 variant 上的直接 `cfg` 属性。
- 移除 `quote` 和 `syn` 的直接 dev-dependencies；Cargo lockfile 保留它们可能存在的传递依赖。

## 验证

- `cargo fmt --all -- --check`: PASS
- `cargo test --test proto_schema_compatibility -- --test-threads=1`: PASS (`41 passed`)
- `cargo test --test state_store_boundary -- --test-threads=1`: PASS (`16 passed`)
- `cargo check --tests`: PASS
- `git diff --exit-code -- tests/proto_schema_baseline/novarocks_schema.json`: PASS，baseline JSON 未修改。
- 三个旧 guard/ledger 删除路径 absence checks: PASS。
- `git diff --check`: PASS。
- `Cargo.toml` 和 `Cargo.lock` 的 `novarocks` direct dependencies 不再含 `quote` 或 `syn`: PASS。
