# NovaRocks patches to Hyper 1.8.1

The exact pinned registry source is vendored; UPSTREAM.json records the registry checksum and all original file hashes. Versions and dependencies remain unchanged.

## Optional H2 receive event count

Client and server conn::http2::Builder expose max_receive_buffered_events(max). A positive limit is preserved by builder clone and forwarded through the private H2 Config to the patched h2 Builder before handshake. The default is None, preserving upstream behavior. Zero is refused at the public setter. Hyper clients already explicitly disable server push, as required by the bounded h2 client API.

This only propagates the connection-wide event-node gate. It neither limits full retained DATA/read backing nor installs Native lane, admission, socket, task or physical-exit owners. Native product support remains disabled until those owners and settings are installed. No listener default changes in this patch.

## Optional retained DATA pool

Client and server conn::http2::Builder also expose receive_buffer_pool(pool), forwarding the same strong-only h2::ReceiveBufferPool through Config and builder clone before handshake. Defaults remain None. One supplied pool can bind only once; an explicit event limit is required by h2, and Hyper's client already disables push. This additive API requires the paired patched h2 and bytes sources.

Real Hyper Incoming tests release flow credit when returning a DATA frame, retain that Bytes through body/connection/executor/builder exit, and confirm the original funding remains held until the final alias physically exits. The pool bounds escaped retained DATA backings and wrappers only. Original codec read/header/write allocations and complete Native lane/listener/connection owners remain separate; this forwarding patch does not advertise product support or change listener defaults.

## Original DATA send owner forwarding

Client/server connection builders forward retain_data_payloads(bool), default false, through the cloned H2 Config. The paired h2 mode skips payload/prefix copying and retains original DATA objects through successful upstream flush. Actual partial-write pointer and credit probes cover both Hyper directions; Native SendBuf<Bytes> preserves the original owner on advance. Header/TLS/task backing, queued Body count and Native profile installation remain separate. No product listener default changes or performance claims are made.
