# M07 SQL logical-domain 最终只读审查

本审查没有修改仓库源码、运行 Cargo 或提交。当前条件表达式/合并切片未发现剩余的具体 correctness blocker。

## 已核对的边界

- `logical_output.rs` 按实际 scalar binding identity 保留 coalesce/ifnull/nvl/if/nullif 的返回域；CASE 只合并返回值，条件和 NULLIF comparator 不供应结果身份。
- NULL 中性检查追溯真实 Null literal、原 Null DataType 和 Cast/Nested 内部来源；nullable 普通值不会被当成 Null。
- 已建立的逻辑事实须与实际载体兼容；隐式 offset-width 转换不会凭 Utf8/Binary 建立身份。显式 CAST 维持原 Json→Json 证明范围。
- `scope.rs` 保留 catalog 的完整现有 SqlType；没有新增 Object/Percentile 顶层 SqlType 或通过最终存储猜测 opaque。
- `coercion.rs:254–277` 保留非 Null 非法 marker 和 LargeBinary opaque 域擦除的固定拒绝标记；重复合并仍保留拒绝，equal-type 路径保留原 marker。
- Map entries 自身不能持有任何 scalar logical marker。`coercion.rs:104–117` 的 undecorated 路径与 `:322–340` 的 wider 路径均通过固定 `invalid` witness 保留拒绝；单纯 provider decoration 仍可删除。
- `root_scalar_type.rs` 在构造 neutral schema 前完整 borrowed preflight；未知/错载体 marker 被拒绝，top Variant 需要 trusted fact，nested LargeBinary 遵守现有 closed Native Variant 规则。
- 原实际 nullable 与 Map key nullable 被保留；source profile 检查不是运行时原授额度证明。

## Oracle 范围

联合测试 `m07_scalar_common_type_cannot_erase_an_invalid_domain_into_a_value` 覆盖非法 leaf 与 opaque→Variant 拒绝，同时保留合法 Json/plain→String。

`m07_scalar_map_container_marker_survives_normalization_as_a_refusal` 对 unknown 与全部 recognized container marker 覆盖 raw、undecorated、双向 wider 拒绝，另有 canonical/provider-only positive。

六项 analyzer provenance tests 验证 facts forwarding；其中无真实 nested marker 的 m/r fixture 不能独自证明 Scalar freeze 成功。最终 proof 与最终 actual root 的比对由 root integration tests 负责。

普通 Native generic encoder 对 unknown marker 的拒绝、未来其他 builtin/wrapper 的 provenance、执行端 ScalarValueV1 producer/collector 安装与整体内存授额不包含在本审查结论中。

## 审查输入 SHA256

```text
062e9904ea7a662e4295d820eb8c54d9babdcd855d72134423d3a74ca44d0c9b  novarocks/sql/src/analyzer/logical_output.rs
972f81246c9b3bc864b67b83ab5f938f46d844a75171845c32f4f282b69ec021  novarocks/sql/src/analyzer/scope.rs
1c6170179e7ffae6ff460e3d9eae3744662a69df8b01ac8b9cc50baf65682417  novarocks/types/src/coercion.rs
551af394ee06189c53cbf8cb92441d9fb8d95d29b201444f02f3688eab540b91  novarocks/sql/src/compiler/root_scalar_type.rs
```
