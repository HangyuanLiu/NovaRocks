# Decimal read/write 独立预期核验

`decimal-read-write-v1.py` 只识别现有 `test_read_write_decimal256.sql` 的固定形状：86 张表、301 次 literal INSERT 和 88 个读取查询。它不是通用 SQL 解释器；DDL、插入数、读取形状或未知表达式变化时直接拒绝。

预期值由原始 SQL 的十进制字面量和实际 DDL 计算，使用 Python 标准库 Decimal（128 位十进制精度），不从 old main 或 candidate 输出录制。历史表名和注释中的 p39/p76 等数字不作为类型依据；此 fixture 的持久列实际声明为 DECIMAL(38, scale)。保留原 SQL、列名、行数、NULL 和查询形状。独立算术应用 fixture 明确要求的 HALF_UP（包括负数）与 nullable overflow；对应既有冻结 policy 为 `type-contract/src/arithmetic.rs` 的 OutputNull/ReportError，既有 `execution/src/exec/expr/cast.rs` 的 downscale HALF_UP 回归单独钉住舍入合同。显式 ReportError case 不在这个 helper 内。

运行示例（只写独立候选文件，不修改原 golden）：

```bash
python3 docs/testing/mem-1-m07/oracles/decimal-read-write-v1.py \
  --sql tests/sql/correctness/decimal/sql/test_read_write_decimal256.sql \
  --output logs/mem-1-m07/decimal-oracle/test_read_write_decimal256.result \
  --audit logs/mem-1-m07/decimal-oracle/audit.json
```

生成候选不是验收。必须通过 SQL runner 的 `--result-dir` 指向独立目录，在 native 1FE+3BE 的 old main 和 candidate 上分别核验完整 case，再审阅差异后决定是否修订原 golden。该 helper 不为其他五个失败 case 提供预期，也不证明更高精度持久列能力、完整 Decimal suite 或最终 M07 验收。
