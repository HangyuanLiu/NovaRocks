# External StarRocks FE batch-fragment fixture

- Producer revision: StarRocks upstream `fe0bed0bdcb520a758a34f572f445f398ca7d5a3`
- Producer artifact: deployed `starrocks-fe.jar`, SHA-256 `f2950e2f8e6d2db9091a2eb0c1ad25318f4385c1d3c586926aa414f9a97d9346`
- Fixture: `select_1_exec_batch_plan_fragments_v1.bin`
- Capture date: 2026-07-11
- SQL: `SELECT /*+SET_VAR(enable_constant_execute_in_fe=false)*/ 1`
- Session setup: `SET enable_single_node_schedule=true` selects StarRocks FE's real batch-fragment deployment path; it does not alter the SQL or plan semantics.
- RPC: `exec_batch_plan_fragments`
- Protocol: Thrift Binary
- Query ID: `019f51afb5c37c92-9c5da9f1a67bbebf`
- Fragment instance ID: `019f51afb5c37c92-9c5da9f1a67bbec0`
- Normalization: none. The attachment is preserved byte-for-byte, including the producer-assigned IDs, coordinator address, and backend number.
- SHA-256: `1c7bda906c9828d7999f93c36197f5f896e8611c27953510e07a18966e624095`

Before capture, `EXPLAIN` produced `PLAN FRAGMENT 0` with a `RESULT SINK`, `Project`, and `UNION`; it did not contain `EXECUTE IN FE`. The instrumented NovaRocks BE logged receipt of one 3,456-byte batch attachment and returned the query row followed by EOS. An out-of-repository inspector using the StarRocks upstream generated Thrift classes decoded one instance with a `RESULT_SINK`, two plan nodes, and the fragment instance ID above.

The one-time capture instrumentation and inspector were removed after copying the bytes. This directory intentionally contains no builder, serializer, normalizer, or generator. The integration test only includes and consumes this external input and runs without a live StarRocks FE.
