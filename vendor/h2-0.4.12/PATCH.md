# NovaRocks patches to h2 0.4.12

The vendored source is the exact pinned Cargo registry copy. UPSTREAM.json records its registry checksum and every original file hash. Existing public HTTP/2 builders keep their default behavior; the new receive count bound is opt-in.

## Bounded receive event nodes

Both client and server Builder expose max_receive_buffered_events(max), with zero refused at configuration. Config passes this local limit to the connection's receive Buffer. Bounded mode preallocates its complete Slab node backing before frame parsing. The limit counts DATA (including empty nonterminal payloads), request/response headers, push-promise headers and trailers across the whole connection; it is independent of byte flow control and never derives frame count from maximum frame size.

Before reading the next frame, Connection checks for a free receive event node. A full buffer registers the connection waker and returns Pending; the existing outer Pending path still flushes outbound writes and flow-control updates. Removing an actual node wakes that connection; header consumption, data/trailer consumption, stream buffer clear, reset cleanup and RecvStream Drop share the original pop path. A queued event is not truncated or rejected for being small. An assertion before insertion prevents an internal missing-admission defect from growing the fixed slab. Default/unbounded mode retains the upstream allocation path.

This patch bounds event-node count and node-slab high water only. It does not prove full retained DATA/read-buffer capacity, grant lifetimes of aliases, outbound generic Body chunk count, complete connection Layout, Native lane/stream/handshake positions, authentication or production listener integration. Those require their separate concrete owners and settings. A 16 KiB frame maximum alone does not bound the number of 1-byte or empty DATA frames.


Bounded clients require an explicit enable_push(false), checked before writing the client preface. PushPromise can precede its parent's response while no push consumer exists (or after the push reader was dropped), so allowing that configuration could permanently strand the count gate. The opt-in API refuses this conflict with InvalidInput rather than changing push implicitly. Default clients keep their original push setting/path; Hyper's Native client already configures push=false. Bounded mode also clears zero-byte receive events when the last original stream reference exits: a canceled, never-polled Headers-only ResponseFuture has no RecvStream and no flow bytes, but its header node must still physically leave the queue.
