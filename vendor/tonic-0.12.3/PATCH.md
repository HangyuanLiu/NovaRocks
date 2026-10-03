# NovaRocks patches to Tonic 0.12.3

The exact pinned registry source is vendored. UPSTREAM.json records its crate checksum and hashes of all 73 original source files. Package version and dependency versions are unchanged. The channel feature enables the existing optional h2 dependency because the additive configuration contains its owned types.

## Fresh HTTP/2 configuration per physical attempt

Endpoint::http2_connection_factory accepts a shared synchronous factory. Endpoint clones, lazy connections, balanced endpoints and reconnect retain the factory, while every MakeSendRequestService::call obtains a fresh Http2ConnectionConfig. The result is deliberately not Clone. It forwards frame/header/event/send bounds, original DATA retention and fresh DATA/raw-input/GOAWAY/fixed-writer backings to the paired patched Hyper builder.

Scalar and independent backing geometry are validated before builder mutation and connector.call. Factory or configuration refusal creates no dial future. The returned builder moves into the actual attempt future; dial failure or cancellation drops its owners, and a successful handshake transfers their retention to the connection and escaped DATA/error aliases. Once-bound pool reuse is refused by h2 after dial but before its first handshake I/O. A reused pool is not silently replaced.

The default factory is None, preserving upstream behavior. Option fields in a returned configuration override the corresponding Endpoint base settings only when present; retain_data_payloads defaults false, matching the existing base builder. Actual Channel tests exercise reconnect, original Worker grants through late aliases, refusal, dial failure, pending-dial Drop, once-bound reuse and the default path. A separately locked offline channel-only probe checks the public API and feature dependency.

Custom connector.poll_ready can run before the factory and requires its own ownership. A request cancellation does not itself prove that a background connection attempt exited. Tonic connect_timeout covers the connector future, not the subsequent Hyper handshake. Socket/TLS/task/queue/stream/header/error-box metadata, deadlines and the complete Native connection envelope are separate. This patch does not install the factory in Native clients, change listener defaults or advertise a complete connection budget or product/performance acceptance.

## Source preservation

The upstream benchmark README contains trailing spaces and a final blank line. Its bytes remain identical to UPSTREAM.json. A file-specific .gitattributes whitespace setting preserves that original source without disabling checks for patched code. Evidence diffs are stored as lossless gzip files so their unified-diff context prefixes are not interpreted as documentation whitespace.

The per-attempt configuration also forwards an optional original-funded send_frame_buffer. Its constructor validates independent capacity/frame geometry before the factory returns it; h2 binds once before handshake I/O. Actual Channel tests include this writer in the same original attempt grant. Header/HPACK/queue owners remain outside that scope.

Http2ConnectionConfig also forwards an optional max_send_header_table_size. Zero configures a fresh encoder without dynamic table backing; it is independent of inbound table settings. Defaults follow the peer. The actual factory regression uses zero alongside its original fixed writer/pools; this does not fund encoded header blocks or HTTP metadata and has no product/performance acceptance claim.


Http2ConnectionConfig also forwards send_header_block_pool for each physical attempt. A present pool requires explicit max_send_header_table_size=Some(0); conflicting configuration is rejected before builder mutation or connector.call. Actual Channel regressions fund this backing in the same original attempt grant and refuse an oversized block before any HEADERS/CONTINUATION reaches the wire. Reconnect creates a fresh pool; HTTP metadata and whole Native profile installation remain separate.


Http2ConnectionConfig also carries a fresh receive_header_block_buffer per physical attempt. apply validates fixed raw input, explicit positive encoded maximum and workspace capacity before any builder mutation or connector.call, then forwards the buffer. h2 separately enforces once binding before its preface. Encoded input original ownership is separate from decoded header/HPACK metadata, incoming table settings and complete Native connection funding.

Each HTTP/2 connection factory attempt can provide fresh receive_header_field_pool
backing. Before dialing, its fixed raw/encoded/block dependencies and effective
header-list cap are checked. The attempt override takes priority over Endpoint's
explicit cap, then Hyper's existing 16KiB default; the chosen cap is explicitly
forwarded. A conflicting cap is rejected before a dial future. Default None and
existing Status/metadata semantics are unchanged.


Each connection factory attempt can also supply receive_header_table_buffer
and an incoming header_table_size advertisement. Before dialing, apply
requires the original decoded field pool and table capacity covering the
advertised ceiling (4096 by default); the constructor separately requires
capacity for the initial 4096-byte protocol table. Both options move into the
actual Hyper attempt. Endpoint has no incoming table-size API to inherit.
Defaults are None, and the outbound table ceiling remains independent.

## Per-attempt original lifecycle verdict

`Http2ConnectionConfig.connection_lifecycle` requires a positive initial SETTINGS timeout and forwards the original capability. Final acquisition publication follows the actual Hyper handshake and the same absolute deadline's late-Ready check; callback work is followed by another check against that deadline. An inline attempt guard permanently retires refused, timed-out or canceled attempts even while factory aliases remain, and disarms only after final success. The existing ordered acquisition owner wrapper still retains its original position through actual inner-future exit. These additions do not fund the outer boxed future, Channel/executor task, socket or TLS backings.

A supplied lifecycle also retains the original acquisition owner before dialing. This closes the failed-final-verdict window where Hyper has already spawned its independent protocol task: returning an error and dropping SendRequest do not prove that task/IO exited. Successful post-deadline verdict explicitly releases the extra alias; failure keeps it in the actual H2 bound lease until IO exits, while the original wrapper covers future exit.
