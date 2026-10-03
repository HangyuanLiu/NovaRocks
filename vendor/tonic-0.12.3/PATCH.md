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

## Typed original-owner connector attempts

`Endpoint::connect_with_attempt_connector` and its lazy counterpart accept
`Service<Http2ConnectionAttempt>`. After obtaining and validating each fresh
factory configuration, the actual make-service call passes the original URI
and a strong alias of the same `io_owner` before invoking the connector. The
private attempt constructor prevents callers from fabricating a Tonic attempt;
`uri()` and `into_parts()` expose the exact facts needed for socket setup. Legacy
`Service<Uri>` APIs remain on their existing path. A connector's readiness,
allocation and escaping aliases still require its own original-owner proof.

The typed connect-only timeout awaits the existing connector, including Tonic
TLS when enabled, and returns its original IO response type. It does not create
a `TimeoutStream` IO-response box or introduce read/write timeouts. Expiration
preserves `io::ErrorKind::TimedOut` and its original Tokio `Elapsed` error payload,
matching the legacy timeout's error cause. URI scheme handling remains in the
shared actual Connector: HTTPS uses configured Tonic TLS when enabled; a
connector providing its own TLS uses HTTP to avoid a second TLS layer.

The ordered acquisition wrapper retains the original owner through refused,
failed, canceled and never-polled inner attempts. Success transfers it to the
inline final IO wrapper, whose concrete IO destruction and box deallocation
precede the original capability's release, including destructor unwind. Actual
public Channel tests exercise the typed handoff, eager cancellation, pending
connect deadline and cause, factory refusal before connector call, None owner,
lazy reconnect, and successful boxing with and without a connect timeout.

These additions prepare the typed connector boundary; they do not install it
in Native outbound clients or close outgoing DNS, socket registration, private
TLS buffers, enclosing futures/tasks, or the whole Native connection envelope.
The optional private `cfg(tls)` parity tests and separate public TLS parity
probe have distinct execution receipts and are not implied by the Native
transport-only target's results.

## Originally owned live connection driver

A factory may supply `Http2ConnectionConfig.connection_driver`. Its strong-only
`OriginalConnectionDriver` carries a single prepaid task position and the same
physical IO capability. Construction prewarms its handle mutex; metadata queries
cover the actual Arc and supported platform mutex allocation. The static task
query uses the real `run_driver` constructor for both Endpoint connector output
types, including the legacy timeout wrapper through its exact associated type.
No future, task, or IO is constructed by that query.

Before connector.call, make-service checks the actual response's driver bound
and reserves the once-only token. Clones cannot create a second physical task.
Cancellation, refusal, and unwind abandon that reservation. After successful
acquisition the actual future goes directly to Tokio's original-owner spawn,
without either legacy executor erasure Box. Spawn occurs without the handle
mutex held, so synchronous runtime hooks can inspect the token. Publication
stores one JoinHandle; its optional observation/control API creates no task.
A separate lifecycle guard retires failed or unwinding dispatch and disarms
only after successful spawn. The acquisition position does not move into the
live task.

The original owner stays in the real TaskCell through its final handle/Waker
alias and actual deallocation, beyond future completion and IO exit. The last
strong metadata holder deallocates its Arc before dropping its mutex/handle and
physical capability. Default None keeps the caller-selected executor. This
closes only the live driver Cell and its ordinary automatic Future Box; Hyper
protocol tasks, Channel buffer workers, queues, TimeoutStream allocations,
TLS/authentication, DNS, scheduler metadata, and whole-connection limits remain
separate. Native outgoing factories install this token from their existing
prepaid physical stock; incoming server configurations do not create it.

The channel feature explicitly enables Tokio io-util, which owns the pinned
original-task API. Workspace feature unification is not required for the
standalone channel consumer to compile this opt-in carrier.


## Separate original Hyper internal connection task

A factory may also supply `Http2ConnectionConfig.protocol_task`, an
`OriginalHttp2ProtocolTask` with a fresh one-shot position independent of the
live Tonic driver. Before connector.call it checks the actual connector
response type and reserves that position. Hyper extracts a prepared executor
before dispatch/H2 allocations and sends the same actual internal enum future
directly to Tokio original-owner spawn. The base executor loses this token,
so request Pipe/Send tasks cannot consume or replay the connection position.
None retains the selected legacy executor path.

Its metadata reuses the strong-only Core and prewarmed handle mutex backing,
with no IO/Channel/Weak backlink. Clones share the once-only election. Errors
and cancellation never reset a reserved/prepared position; spawn unwind marks
it abandoned. Completion alone returns no credit: the actual unpolled Join,
Abort or Waker alias retains the TaskCell original owner through deallocation.
The handle is published under a short lock after spawn, without holding that
lock across runtime hooks. Queries cover the same closed Endpoint IO set as
the real factory. Native outgoing factories pregrant the task and metadata
from their existing physical stock; incoming configurations leave it None.
Pipe/Send, Channel worker/queues, DNS/TLS/auth, shared scheduler and the complete
connection budget remain separate.
