# NovaRocks patch: negotiated multi-result capability

This is Apache-2.0 `opensrv-mysql` 0.7.0, vendored from the crates.io release.

NovaRocks requires standards-compliant negotiated COM_QUERY multi-statement
execution. Upstream 0.7.0 already provides the multi-result writer API, but
its server handshake does not advertise `CLIENT_MULTI_STATEMENTS` or
`CLIENT_MULTI_RESULTS`, and it stores unfiltered client flags after the
handshake. The local patch:

1. advertises both server capabilities;
2. stores only the intersection of client and server capabilities; and
3. exposes those negotiated capabilities to `QueryResultWriter` consumers; and
4. separates `RowWriter`'s socket and column-schema borrow lifetimes, so a
   completed result can return its connection writer without retaining the
   caller's temporary column conversion; and
5. emits `SERVER_MORE_RESULTS_EXISTS` for both EOF and OK result terminators.

The NovaRocks MySQL adapter uses this read-only fact to reject multi-statement
input unless both capabilities were negotiated. The patch preserves default
single-result writer behavior and authentication semantics.

## NovaRocks patch: bounded streaming framing and input limits (MEM-1 M07)

MEM-1 M07 moves client row encoding to the backend root and has the frontend
relay already-encoded MySQL text rows. Upstream 0.7.0 builds each row in a
complete in-memory buffer before splitting it into packets, and its packet
reader grows input buffers without an explicit limit. This patch adds:

6. `streaming.rs`: an owned streaming writer that frames a row of known total
   length from arbitrary byte slices into U24 packets with u8 sequence wrap and
   a zero-length terminal packet at exact multiples, through a fixed coalescing
   buffer, without materializing the row; it records partial-write progress,
   poisons the connection after an unrecoverable partial write, and hands an
   in-progress row or metadata block to a bounded closing writer.
7. `limits.rs` / `input.rs` / `packet_reader.rs` / `params.rs`: explicit
   protocol limits for single packets, continuation totals, handshake and
   authentication input, and prepared-statement long data, checked before any
   buffer grows; client-triggered assertions become protocol errors.
8. `resultset.rs`, `writers.rs`, `lib.rs`, `tls.rs`: entry points
   (`into_streaming`, `run_with_limits`, `init_before_ssl_with_limits`) that
   expose the streaming lease and limits while leaving the existing APIs and
   their behavior unchanged.

The patch concerns MySQL protocol behavior and NovaRocks-owned buffers only.
It does not instrument or account allocator use inside the crate. Exit
condition: drop the vendored copy when an upstream release provides a
streaming row writer with partial-write state and configurable input limits
that pass the NovaRocks socket-level tests in `src/tests/streaming.rs` and
`src/tests/input_bounds.rs`.
