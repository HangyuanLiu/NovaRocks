# RFD-6D Final Review Fix Report

## 状态

DONE

## 修复摘要

### 1. Aggregate prepare 校验 frozen membership contract

- `FinalDomainCompletionSession` 从已安装的
  `RuntimeCompletionFenceContract::membership_schema()` 中只携带 aggregate
  adapter 必需的 `DataType`，没有扩大 session/builder 的协议所有权。
- `AggregateProcessorFactory` 在领取 partition committer 前固化 prepare
  错误，完整校验：
  - aggregate 恰好一个 group key；
  - group key 属于 `MembershipValues` 支持的 membership 类型矩阵；
  - group key 类型与已安装 session schema 的 key 类型完全相等。
- 不支持的 `Decimal256` 与 installed/aggregate 类型不匹配均由真实
  `AggregateProcessorOperator::prepare()` 返回错误。错误 operator 不领取
  partition committer、不产生 output，也不向 Service 提交 shard；RF session
  仅 fail 一次。
- 新增两条真实 operator 测试，分别覆盖 unsupported type 与 schema mismatch，
  并断言 `has_output == false`、accepted partitions 为空、failure count 为 1。

### 2. Final-domain completion 使用语义化 submit fault seam

- 删除依赖“第 3 次 memory reservation”的脆弱故障定位。
- 新的仅测试 seam 按 `(PartitionId, ProducerSequence)` 选择一次
  `FinalDomainProducerAdapter::complete` 失败，故障发生在 core mutation 前。
- 回归测试显式选择 partition 1 / sequence 0，证明 partition 0 已被接受后，所选
  submit 失败会立即终止后续提交，且不会 materialize/publish 部分 artifact。

### 3. 外部边界测试绑定当前 Cargo artifact

- 不再扫描 `target/deps` 并取第一个 `libnovarocks-*.rlib`。
- 测试用当前 Cargo、workspace manifest、当前 feature set、当前 target directory
  执行 locked/offline library build，并从 Cargo
  `compiler-artifact` JSON 中要求精确得到唯一一个当前 `novarocks` rlib。
- 外部 caller 仍由 `rustc --extern novarocks=<exact artifact>` 编译，因此验证的是
  Rust visibility/public API boundary，不依赖源码文本或布局。

## TDD 证据

### Finding 1 RED / GREEN

先写两条 operator 测试：

```text
cargo test -p novarocks --lib aggregate_prepare_rejects_ -- --test-threads=1
```

RED：exit 101；2 条测试均因 `prepare()` 意外返回 `Ok(())` 而失败。

实现 typed contract 校验后原样重跑：

```text
2 passed; 0 failed
```

### Finding 2 RED / GREEN

先把测试改为调用语义化 selected-submit seam：

```text
cargo test -p novarocks --lib \
  selected_partition_submit_failure_stops_and_fails_without_materializing_subset \
  -- --test-threads=1
```

RED：exit 101，`RuntimeFilterService` 不存在
`inject_final_domain_submit_failure_for_test`。

实现 seam 后原样重跑：

```text
1 passed; 0 failed
```

### Finding 3 RED / GREEN

最初尝试临时外部 Cargo path dependency：

- 未携带 lockfile 时，dependency resolution 命中已 yanked `lz4 0.11.5`；
- 携带 lockfile 后，临时 crate 仍无法继承 workspace
  `[patch.crates-io]`，命中已 yanked `lz4 0.12`。

这两个 RED 证明临时 crate 并不是可靠的当前 workspace artifact 绑定。改用 Cargo
`compiler-artifact` JSON 后：

```text
cargo test -p novarocks --test final_domain_public_boundary -- --test-threads=1
2 passed; 0 failed
```

两条 external caller 均被当前精确 rlib 的 visibility boundary 拒绝。

## 最终验证

```text
cargo test -p novarocks --lib exec::operators::aggregate::tests -- --test-threads=1
21 passed; 0 failed

cargo test -p novarocks --lib \
  runtime_filter::service::final_domain_completion::tests -- --test-threads=1
11 passed; 0 failed

cargo test -p novarocks --lib runtime_filter::port::final_domain -- --test-threads=1
6 passed; 0 failed

cargo test -p novarocks --test final_domain_public_boundary -- --test-threads=1
2 passed; 0 failed

cargo check -p novarocks --lib
exit 0

cargo check -p novarocks --lib --features compat
exit 0
```

两套 `cargo check` 仅输出仓库既有 warning（default 703、compat 613）。

提交前还执行：

```text
cargo fmt --all -- --check
git diff --check
```

## 范围复核

- 未新增或修改 B0a/B1 activation。
- 未修改 planner、lowering、SQL case/golden 或 production deployment binding。
- production 变更限于 aggregate session typed contract 校验；selected-submit fault
  seam 及其状态均受 `#[cfg(test)]` 约束。
- 外部边界测试只依赖 Cargo artifact metadata 与 Rust 编译器结果，不做源码扫描。
