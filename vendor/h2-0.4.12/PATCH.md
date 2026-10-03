# NovaRocks patches to h2 0.4.12

The vendored source is the exact pinned Cargo registry copy. UPSTREAM.json records its registry checksum and every original file hash. Existing public HTTP/2 builders keep their default behavior; the new receive count bound is opt-in.

## Bounded receive event nodes

Both client and server Builder expose max_receive_buffered_events(max), with zero refused at configuration. Config passes this local limit to the connection's receive Buffer. Bounded mode preallocates its complete Slab node backing before frame parsing. The limit counts DATA (including empty nonterminal payloads), request/response headers, push-promise headers and trailers across the whole connection; it is independent of byte flow control and never derives frame count from maximum frame size.

Before reading the next frame, Connection checks for a free receive event node. A full buffer registers the connection waker and returns Pending; the existing outer Pending path still flushes outbound writes and flow-control updates. Removing an actual node wakes that connection; header consumption, data/trailer consumption, stream buffer clear, reset cleanup and RecvStream Drop share the original pop path. A queued event is not truncated or rejected for being small. An assertion before insertion prevents an internal missing-admission defect from growing the fixed slab. Default/unbounded mode retains the upstream allocation path.

This patch bounds event-node count and node-slab high water only. It does not prove full retained DATA/read-buffer capacity, grant lifetimes of aliases, outbound generic Body chunk count, complete connection Layout, Native lane/stream/handshake positions, authentication or production listener integration. Those require their separate concrete owners and settings. A 16 KiB frame maximum alone does not bound the number of 1-byte or empty DATA frames.


Bounded clients require an explicit enable_push(false), checked before writing the client preface. PushPromise can precede its parent's response while no push consumer exists (or after the push reader was dropped), so allowing that configuration could permanently strand the count gate. The opt-in API refuses this conflict with InvalidInput rather than changing push implicitly. Default clients keep their original push setting/path; Hyper's Native client already configures push=false. Bounded mode also clears zero-byte receive events when the last original stream reference exits: a canceled, never-polled Headers-only ResponseFuture has no RecvStream and no flow bytes, but its header node must still physically leave the queue.

## Fixed retained DATA backing

ReceiveBufferPool is an additive opt-in API paired with NovaRocks' pinned bytes 1.11.0 physical-exit guard patch. The caller obtains the full allocation_capacity_bound before construction and supplies the original ownership carrier. The pool has fixed Vec backing and a fixed slot array, with no return-path allocation. One pool binds once to one connection, before handshake I/O. Both builders require an explicit receive event limit; the client also requires explicit push=false. Local receive MAX_FRAME_SIZE must fit a block, and decoder settings updates check that bound.

FramedRead checks actual free positions before reading the next frame. DATA payloads, including empty DATA, move to a fixed pool block before escaping; the original codec-buffer alias is dropped. Flow-control release cannot return a block. The final Bytes alias returns its Vec to RETIRING, and only the paired bytes post-deallocation exit guard publishes FREE and wakes the same parser. This bounds simultaneously live/retiring owner wrappers as well as Vec backings. Strong-only pool handles use Arc::into_inner; final Core allocation, slot array, buffers and registered task reference exit before the original ownership carrier. Decoder Drop detaches its waker. Notification panic cleanup completes before propagation.

The allocation bound includes Rust-requested pool Core/Arc/slot/block/Bytes-owner layouts; caller carrier metadata, Waker targets/framework tasks and allocator caches/RSS are separate. This bounds escaped retained DATA only, not the codec's raw read BytesMut, HPACK/header/continuation allocations, outbound DATA copies, stream state or the complete Native connection budget. A full pool pauses connection-wide parsing, including control frames; subsequent Native integration needs separate control connections and local cancellation. Defaults remain None and no production listener is changed.

## Original DATA send owners through flush

Both builders expose retain_data_payloads(bool), default false. Opt-in writes only DATA headers into the codec buffer; tiny payload and large-payload prefix copies are skipped. The original Data<B> stays in Next through all partial writes and a successful upstream poll_flush before it can be reclaimed/requeued. Header bytes are included in the completion check, including empty DATA. Native's SendBuf<Bytes> retains its original owner when advanced; arbitrary generic Buf implementations may release their own chunks on advance, so no blanket claim about all generic internal allocations is made. Header/TLS/other I/O backing remains separately owned. This mode may increase scalar writes and per-frame flushing; it is not installed in Native or performance-accepted yet. Without the separate send_frame_buffer option, peer settings still determine maximum outbound frame size; retain_data_payloads alone does not impose the frozen Native frame ceiling.

With actual remaining encoded bytes, a zero write now returns WriteZero rather than spinning forever. Default DATA batching/copy thresholds remain unchanged. A transport error/reset does not count as physical frame exit: retained frames still own their original credit until successful flush/reclaim or actual codec teardown.


## Optional header allocation prechecks

Client/server Builder expose max_receive_header_block_size(max), default None. A complete HPACK block is counted across HEADERS/PUSH_PROMISE/CONTINUATION, including fields already decoded; stripped padding/priority bytes are not HPACK bytes. The total is checked before continuation growth. Each declared encoded string is checked against that block limit before waiting for more bytes. The local max_header_list_size additionally limits decoded fields (name + value + 32), before HTTP name/value copies or dynamic-table insertion. A decode-limit violation closes the connection with COMPRESSION_ERROR, avoiding unsynchronized HPACK state after a refused partial decode. Existing header-list handling remains in force.

In opt-in mode, Huffman validity and output length are counted without allocation before an exact output Vec is constructed; the encoded-length times two reserve heuristic is bypassed. Plain strings are compacted before pseudo headers/dynamic table/URI aliases escape. Consequently a tiny pseudo header cannot pin a large raw input block through the dynamic table. Generic default clients/servers preserve their previous decoding, scratch and alias behavior. Hyper propagates the option through both cloned configs. Native has not installed it yet.

The combined field check can occur after two individually bounded Huffman output markers exist. It does not promise zero temporary allocation for every combined-field refusal. This patch limits decoding growth, not complete physical connection ownership: HeaderMap indices/buckets/duplicate metadata, raw BytesMut and continuation capacity/spare, HPACK table VecDeque capacity, header/URI carrier lifetimes and all other transport owners still require their own proof and original grants. The complete 2 MiB Native connection envelope remains unverified. Double-pass Huffman/compact copies are not performance-accepted.


## Optional fixed raw frame input

Client/server Builder expose receive_frame_buffer(ReceiveFrameBuffer), default None. The caller obtains allocation_capacity_bound(max_payload) and grants the raw Vec/Core/Arc plus its separately covered ownership-carrier metadata before construction. The opaque strong-only handle can bind only once to one connection; incompatible local geometry and reuse fail before handshake I/O. Its sole non-cloneable mutable lease owns fixed max_payload + 9 storage. No Weak/raw Arc/buffer access escapes. Final Arc allocation and input Vec physically exit before the original ownership carrier. Builder clones preserve that same owner, and do not provide a fresh connection buffer.

The opt-in codec directly constructs the fixed reader without temporarily constructing Tokio's default 8 KiB read backing. It reads exactly nine header bytes, rejects oversized declared payloads before a body read, then reads precisely that frame. Pending/partial input resumes without replay; truncated header/body returns sticky UnexpectedEof; clean EOF is terminal; zero payload does not poll an empty ReadBuf. Oversized input maps to FRAME_SIZE_ERROR. Default clients/servers keep their prior length-delimited codec. Hyper forwards this option through both cloned configurations.

The emitted BytesMut is an independent exact frame copy, allocated only after the complete accepted frame is read. That copy, error diagnostic wrappers, header/HPACK/continuation and GOAWAY backing, retained DATA, socket/task metadata and allocator caches/RSS are outside this raw grant. Read-frame count and raw-buffer ownership are distinct from escaped retained DATA pool ownership. Native has not installed this API or proved the full 2 MiB connection envelope. Reading header and body separately may affect system-call cost; no performance acceptance is claimed.


## Optional independent GOAWAY debug backing

Client/server Builder expose receive_goaway_buffer_pool(ReceiveBufferPool), default None. This pool binds once before I/O, independently of the retained DATA pool, and its complete block capacity covers the local receive frame maximum. The same pool cannot be installed for both roles: the second bind fails before handshake I/O. Each nonempty GOAWAY debug copy obtains a fixed position first; empty debug uses Bytes::new, and malformed payloads under eight bytes are refused without checkout. The full debug bytes are preserved. Exact owned debug is constructed without an earlier ordinary copy.

The pool uses the existing original Core/blocks/owner-wrapper physical-exit chain; returned error clones retain one slot through their last alias. A full pool refuses another copy with a local ENHANCE_YOUR_CALM instead of waiting on errors that the connection itself may retain. This does not change h2's existing error precedence: an earlier non-NO_ERROR remote GOAWAY can remain the final public connection error even when the actual outbound local GOAWAY is ENHANCE_YOUR_CALM. Writer flush and the independent DATA gate may still wait; Native cancellation/deadline/physical-close owners must resolve those waits.

The shared parser helper checks available before CAS, including the FREE-published/count-not-yet-incremented window, so it cannot decrement zero. Only the single bound parser consumes positions. Full-pool refusal makes no payload/wrapper allocation. Control/error metadata and caller error boxes, raw/frame/header copies, tasks/socket and complete connection funding remain separate. Three fixed diagnostic slots can support replacement progress with the first stream error and latest connection error retained; the installed Native profile must fund its chosen supported geometry. No production setup or performance acceptance is claimed.


## Optional original-funded fixed writer

Client/server Builder expose send_frame_buffer(SendFrameBuffer), default None. The caller obtains allocation_capacity_bound(capacity, max_payload) before construction and separately covers its original ownership-carrier metadata. Construction allocates only Core; handshake binds the same strong-only handle once before preface/SETTINGS I/O, and directly constructs the selected private BytesMut writer backing without a temporary default write Vec. Builder clones preserve the original owner rather than funding another connection. The sole mutable lease drops after the actual encoder buffer; final Core/Arc deallocation precedes its original carrier. There is no Weak/raw Arc escape, writer buffer split/freeze or escaped backing alias.

A fixed writer reserves room for a complete local maximum frame plus its nine-byte header before append. Its effective send maximum is min(peer SETTINGS maximum, local maximum); DATA, HEADERS/PUSH_PROMISE and CONTINUATION use that value. Clear/flush reuses the complete capacity. GOAWAY debug length plus its eight-byte payload prefix is checked before append; oversized diagnostics return PayloadTooBig without truncation. The actual GoAway caller now propagates that failure as InvalidInput rather than invoking its old expect. Defaults preserve upstream writer behavior.

Actual protocol tests exercise continuous distinct large headers and CONTINUATION, scalar/vectored and partial writes, peer SETTINGS, repeated Pending flush, errors/cancellation/WriteZero and one-time binding. Independent System allocator probes observe the actual selected writer Vec and Core deallocation before original ownership exit. This covers only fixed writer Vec/Core; whole HPACK blocks/table spare, queued headers/events/DATA, HTTP metadata, sockets/tasks/TLS, carrier/error metadata and allocator caches/RSS remain separate. Native has not installed this option or proved its complete connection envelope; no performance acceptance is claimed.


## Local outbound HPACK table ceiling

Client/server Builder expose max_send_header_table_size(u32), default None. This is the encoder direction, independently of the existing inbound header_table_size setting. The encoder tracks the peer permission and clamps each update to the local ceiling, retaining the original minimum/final size-update transition rules. Installing zero on a fresh encoder queues the required initial size update, uses static indices/literal fields, and never allocates its dynamic indices Vec or slots VecDeque even under subsequent peer u32::MAX permissions. Sensitive values remain never-indexed. Changing an already populated table to zero evicts entries but can retain spare capacity; that private transition is not a physical reclamation proof.

Positive limits bound the logical HPACK table size, not actual spare capacity, entry backing or original funding. The whole encoded header block, queued headers, HeaderMap storage, caller metadata and the full Native connection envelope remain separately owned. Default None preserves the peer-directed path. Native has not installed this setting; protocol correctness and zero-table storage proof do not imply performance acceptance.


## Originally funded complete outbound HPACK block

Client/server builders expose send_header_block_pool(SendHeaderBlockPool), default None. The opaque one-slot pool preallocates checked 4 * max_decoded_header_list_size + 20 bytes, with its complete Core/slot/Vec/wrapper allocation bound granted before construction. Decoded maximum is positive and fits u32; actual layout and arithmetic are checked. Shared internal payload construction preserves public ReceiveBufferPool frame geometry. Clones bind once across the same pool, and both handshakes require explicit max_send_header_table_size(0) before any binding or I/O.

Complete borrowed preflight counts every pseudo field (including :protocol), every duplicate field, and RFC header-list name/value/32-byte overhead with checked arithmetic. Refusal precedes iterator consumption, pool checkout, HPACK table/pending-update mutation and frame-head output. The original encoder and Huffman/length-prefix shift algorithm now accept a fixed BufMut exposing only the originally allocated Vec spare range; there is no reserve/growth path. HEADERS and PUSH_PROMISE share this path. Connection prioritization propagates a local refusal as InvalidInput instead of panicking after dequeue.

The block Bytes owner follows unfinished CONTINUATION through partial writes. When the last piece has been copied into the separately funded writer, its physical wrapper can exit and return the slot; pending wire I/O references the writer's own allocation. The full block Vec/Core original grant still lasts through final pool/connection/config handle exit. Callback panic restores the original slot before publication; busy checkout never invokes encoding. Default growable writers are outside the block grant. HeaderMap/name/value/table metadata, inbound workspace, queues/tasks/socket/TLS/carrier metadata and whole connection funding remain separate. Native has not installed these options and no product or performance acceptance is claimed.


## Borrow complete fixed input frames before payload ownership

The fixed raw reader now invokes a synchronous lifetime-independent callback over its original complete frame storage; successful control-frame parsing creates no independent raw BytesMut. It resets frame state before the callback, so unwind cannot replay a consumed frame. Exact partial-read, size, EOF/error and no-read-ahead behavior is unchanged. The test-only old copy adapter delegates to the same reader for existing source probes.

The actual shared frame decoder accepts borrowed or owned input and keeps the common CONTINUATION/protocol gate and error mapping. Installed fixed DATA pools validate stream ID and padding before copying only the stripped payload directly into one fixed position; flags, END_STREAM and Some(0) padding survive. Padding contents are not newly validated. Existing receive flow/content-length accounting continues to count stripped payload length in both modes; this slice does not change it to total padded wire length. GOAWAY diagnostics use their independent original pool; SETTINGS/PING/WINDOW_UPDATE/RESET/PRIORITY/unknown frames operate on the borrowed slice.

Default length-delimited decoding remains on its existing owned path. A fixed reader without an installed DATA/diagnostic pool can still create independent payload backing. HEADERS/PUSH_PROMISE/CONTINUATION keep the owned fallback and remain outside this raw/payload proof until their original workspace and HTTP carriers are funded. The pre-read full-DATA-pool gate still blocks every frame; independent control connections and actual cancellation/deadline owners remain required for Native progress. No Native options are installed, and no whole-connection or product/performance acceptance is claimed.


## Original fixed inbound encoded header workspace

Client/server builders expose receive_header_block_buffer(ReceiveHeaderBlockBuffer), default None. The opaque buffer preallocates its complete encoded Vec/Core/Arc under an original physical-exit grant; clone handles share one once-bound non-cloneable mutable lease. Both handshakes require fixed raw frame input and an explicit positive encoded block maximum within workspace capacity, before any binding or I/O. The bound workspace appends only within that fixed capacity, retaining the complete cumulative HPACK block, including already decoded fields; there is no reserve, reallocation or encoded input alias escape.

HEADERS and PUSH_PROMISE now share borrowed prefix/padding/dependency parsing with their owned compatibility loaders. Fixed CONTINUATION borrows raw payload directly after the original type/stream/flood/cumulative gates. HPACK and HeaderBlock use one generic source algorithm: owned Cursor retains split/freeze/commit semantics; borrowed source copies decoded strings to independent owners and records only the last complete representation offset. A failed literal, including a complete Huffman name followed by an incomplete value, retries from that offset on the next continuation. Decoding remains incremental, so declared oversize literals still fail before END_HEADERS. Existing size-update-per-decode behavior and all field/error rules are preserved. New blocks reset position and reuse the same original backing.

The encoded workspace can physically exit while escaped header/URI/table aliases remain, because none refer to it. Those decoded compact strings, Huffman output, HeaderMap/table storage, initial decoder scratch, Method/URI metadata and whole Native connection funding are separate unresolved scopes. Installing the workspace does not change incoming SETTINGS_HEADER_TABLE_SIZE or its ACK transition, and does not claim the complete connection 2 MiB envelope. Default None retains the existing owned path. Native profile/lane/deadline installation and performance acceptance remain separate.

The shared HeaderBlock also persists malformed-field evidence across NeedMore. Previously a complete illegal field followed by an incomplete literal could lose its local malformed flag on the next continuation. Both owned and fixed inputs now finish decoding the block for connection-level HPACK consistency, then refuse the malformed stream instead of publishing it. This fixes existing invalid-input acceptance; successful header behavior is unchanged.

## Preserve independent decoded field owners

BorrowedSource converts regular literal names with the patched HTTP owned
lowercase constructor and values with the existing from_maybe_shared(Bytes).
Literal values with indexed regular names use the same owned value seam.
These paths retain their independently owned decoded bytes through HeaderMap,
dynamic-table and field clones instead of making a second unowned HTTP payload
copy. The owned compatibility source retains its original copying constructors.
Pseudo-header conversion and error rules remain shared and unchanged. This is
an ownership-preserving seam, not proof that the original decoded plain/Huffman
string allocations, table/HeaderMap storage, Method/Scheme metadata, Status or
whole Native connection have been funded; those scopes remain open.

## Original decoded-field arena

The opt-in ReceiveHeaderFieldPool uses a fixed aggregate byte arena and atomic
64-byte extent map, with a separately bounded number of simultaneous owner
wrappers. Its checked original allocation bound includes arena, extent map,
Core/Arc and every possible Bytes wrapper. No per-field maximum-sized Vec or
implicit Mutex allocation is used. Checkout is nonwaiting, including reentry;
fragmentation/capacity/position refusal is a connection decode error, with no
heap fallback. The final Bytes wrapper exits before its extent/position returns;
all fields and clones retain the original pool until physical exit.

Both builders require fixed raw and encoded input, a valid explicit block
maximum and explicit header-list cap fitting the pool's single-field maximum.
The selected FramedRead constructs a bounded Decoder directly, before legacy
4096-byte scratch allocation. Plain fields copy directly to claimed extents.
Pooled Huffman markers validate/count without output allocation; both name/value
markers and their combined field bound pass before either checkout. The existing
Huffman state table decodes into the exact claimed slice. NeedMore does not
acquire a pooled field. Existing default owned decoding is unchanged.

This covers original decoded string backing and wrappers only. Dynamic table
storage, HeaderMap containers, Method/Scheme/Status and complete connection
metadata remain independently required; Native has not installed this profile.


## Original typed incoming dynamic-table backing and block state

ReceiveHeaderTableBuffer is an additive once-bound incoming-table owner. Its
checked allocation_capacity_bound uses the actual Layout of Option<Header>
slots, Core and Arc, rather than treating HPACK's logical 32-byte entry
charge as Rust metadata. Capacity covers the initial 4096-byte table and the
advertised incoming ceiling. Client/server configuration requires the fixed
field pool and checks geometry before binding or handshake I/O. The selected
decoder uses a fixed newest-first ring; insertion, eviction and index lookup
never grow that typed backing or fall back to VecDeque. Defaults retain the
upstream VecDeque path. Field payload aliases retain their independent arena.

The once-only bind CAS exclusively extracts the original typed Vec from Core
into a noncloneable lease. No live table entry is stored in the shared Core.
The lease declares its typed Vec before its original owner: Vec drop glue
clears entries and physically frees backing before table credit exits, also
when a field destructor unwinds. Final strong handles use Arc::into_inner.
The buffer is not returned or rebound; no table-owner cycle is created.

SETTINGS ACK updates the permitted incoming ceiling without evicting the
peer-selected table. A necessary reduction preserves both the lowest pending
ceiling and the latest ceiling. The peer must send a sufficiently small size
update at the next block start; partial integers do not discharge it. A peer
already using a smaller selected maximum needs no artificial update.
HEADERS/PUSH_PROMISE begin block state exactly once; CONTINUATION cannot
re-enable resizing after a complete field. Only END_HEADERS finishes a block,
including an empty fragment. Missing, out-of-range or late size updates are
connection COMPRESSION_ERROR. Existing field-pool resource exhaustion keeps
its prior PROTOCOL_ERROR mapping. A sticky semantic malformed block continues
HPACK/table decoding through END_HEADERS before its stream reset.

Only typed table backing is funded here. HTTP HeaderMap, pseudo-header and
framework/stream/socket/TLS metadata, full Native connection capacity and
production profile/lane/deadline installation remain separate.


## Registry advisory identity and pending source audit

The upstream registry `h2 0.4.12` advisory `RUSTSEC-2026-0258` remains
tracked for source audit/remediation. M07 does not claim to fix that advisory.
The path-patched lock entry has no registry source/checksum; cargo-deny 0.20.2
skips RustSec matching for source=None. Its inactive registry ignore was
removed under the unchanged `unused-ignored-advisory=deny` policy. This is a
package source identity migration, not a security verdict. Restoring registry
identity does not restore an advisory waiver.

M07's opt-in bounded event/storage paths have scoped ownership and protocol
receipts. The default None paths preserve upstream behavior; those receipts
do not establish a general upstream advisory fix. Original registry provenance
remains in UPSTREAM.json. A passing registry policy check does not discharge
local vendor source audit responsibility.
