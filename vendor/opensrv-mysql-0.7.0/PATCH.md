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
   caller's temporary column conversion.

The NovaRocks MySQL adapter uses this read-only fact to reject multi-statement
input unless both capabilities were negotiated. The patch preserves default
single-result writer behavior and authentication semantics.
