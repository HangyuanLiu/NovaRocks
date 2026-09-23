# Filesystem conformance coverage

Run these targets from the repository root with `cargo test --locked -p
novarocks-fs --test <target> -- --test-threads=1`.

| Target | Behavior protected |
| --- | --- |
| `access_conformance` | Bound file identity, access domains, authorized ranges, and error redaction. |
| `lifecycle_conformance` | Cancellation, deadlines, close, and prevention of I/O after termination. |
| `reader_conformance` | Parquet projection, row group/page selection, absolute positions, budgets, and existing cache behavior; also ORC projection. |
| `parquet_probe_conformance` | Object-store Parquet read and metadata probe. |
| `writer_corpus_conformance` (UEA-4A-2 I00) | Frozen Spark/parquet-mr, PyArrow, Trino, and Flink files read through the current production FS reader with row-value and absolute-position oracles. |
| `parquet_session_conformance` (UEA-4A-2) | One identity-bound footer across runs, incremental index capability, single-projection values and positions, and writer-file compatibility. |
| `range_read_conformance` (UEA-4A-2) | Demand priority, query/source fairness, bounded request dispatch, exact segment assembly, access, and actual drain. |
| `prefetch_conformance` (UEA-4A-2) | Byte and candidate windows, partial promotion, retained and reclaimed inputs, operation generations, and late completion. |

The session, range, and prefetch targets are added with their corresponding production slices.
The existing targets are retained as regression checks; a new target must
exercise behavior through the public filesystem boundary rather than assert an
implementation symbol or a source-file shape. Iceberg run, runtime-filter,
delete, and split-claim behavior is verified in its connector and Worker tests;
native 1FE+3BE behavior is verified by the system-test runner.
