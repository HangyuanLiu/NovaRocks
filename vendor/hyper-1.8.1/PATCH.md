# NovaRocks patches to Hyper 1.8.1

The exact pinned registry source is vendored; UPSTREAM.json records the registry checksum and all original file hashes. Versions and dependencies remain unchanged.

## Optional H2 receive event count

Client and server conn::http2::Builder expose max_receive_buffered_events(max). A positive limit is preserved by builder clone and forwarded through the private H2 Config to the patched h2 Builder before handshake. The default is None, preserving upstream behavior. Zero is refused at the public setter. Hyper clients already explicitly disable server push, as required by the bounded h2 client API.

This only propagates the connection-wide event-node gate. It neither limits full retained DATA/read backing nor installs Native lane, admission, socket, task or physical-exit owners. Native product support remains disabled until those owners and settings are installed. No listener default changes in this patch.
