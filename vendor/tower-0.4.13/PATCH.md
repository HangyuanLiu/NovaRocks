# Originally owned response cells

This patch starts from the exact locally cached Tower 0.4.13 source. Only
`Cargo.toml`, `src/buffer/service.rs`, and `src/buffer/message.rs` change in the
response-cell slice. The common-metadata slice also changes `src/buffer/worker.rs`.
The additive `original-response-cells` feature requires the paired patched
Tokio 1.52.3 owned oneshot API and the production Bytes physical-exit guard API.
It is not enabled by the old default, `buffer`, or `full` feature.

`Buffer::response_cell_allocation_capacity_bound()` queries the actual
`Result<T::Future, ServiceError>` oneshot Arc. The separate
`response_cell_total_capacity_bound()` adds the exact physical-exit carrier
metadata. `pair_with_original_response_cells(service, bound, cell_bound,
original)` checks typed geometry and checked position multiplication before
constructing the existing queue, semaphore or Handle.

No new count or funding authority is created. Only original mode moves the
existing semaphore permit from the queued Message to its response cell's
physical-exit carrier. This intentionally retains a pending position while a
caller leaves the response cell unpolled, even after the Worker sends the inner
service future. Buffer clones do not clone an acquired permit. None mode keeps
the original message-scoped permit lifetime. The original Bytes handle must
refer only to its original generation/stock, without a Worker/Channel/JoinHandle
backlink. The common original is the final Buffer field and final exit-guard
field, retaining it through earlier backing destruction and permit wakeups.

The owned cell frees its actual Arc and retained value/Wakers before releasing
the carrier. Bytes then frees the carrier's actual wrapper before dropping its
permit and common original. A service future moved out of the cell is a distinct
owner: this cell receipt does not fund that future's external backing, request,
response Body, queue, Semaphore, Handle, readiness waiter, error or Worker task.
An actual constructor refusal is preserved with the existing failed-future
path; it never falls back to an unfunded ordinary channel. That error's heap is
part of the still-open error graph.

The Message permit becomes Option and original-enabled Buffer adds inline
fields. The paired Tokio Sender/Receiver also change inline shape. All affected
actual TaskCell/Future constructor bounds must be re-queried after installation;
historical fixed byte values are not valid proofs. Native opts into this mode from its original logical Channel Worker stock.
Verification and remaining full-graph limits are recorded separately in M07
evidence; this patch document is not an acceptance receipt.

## Common Semaphore and Handle metadata

The same opt-in feature adds `Buffer::common_metadata_capacity_bound()` for the
actual shared `Arc<Semaphore>` and `Arc<std::sync::Mutex<Option<ServiceError>>>`,
including their private platform mutex backing. Tokio's Semaphore query follows
its effective std/parking_lot selection. Handle always uses std Mutex, so its
private PAL is independent of that Tokio feature. Unknown std ABIs, Loom and
active unstable Tokio tracing are refused before pair growth.

Only original pair construction prewarms the final, unpublished mutexes, without
changing permits, waiters, closed state or the error value. Buffer, response
cells, and the final unpinned Worker field retain clones of the same original
Bytes. Original remains through the Worker's earlier Handle and Semaphore Weak
destruction. pin-project-lite uses a cfg-selected private field type: Option<Bytes>
with the feature, and the zero-sized unit otherwise. Ordinary pairs store no owner
and do not prewarm.

This is metadata geometry, not a funding authority or complete Buffer allocation
bound. Queue Chan/blocks, readiness futures/waiters, shared parking resources,
external Wakers and ServiceError payload remain separate. The actual Worker
layout changes; callers query its real TaskCell again and supply independent
task backing ownership through physical task exit.

## Originally owned fixed FIFO backing

The original-response-cells pair now constructs a private fixed FIFO directly;
ordinary pairs retain the upstream MPSC. `src/buffer/queue.rs` is new, and the
Buffer/Worker private endpoints use that module. There is no additional semaphore
or admission wallet. Every queued Message retains one of the same pending permits,
so the preallocated slot count is the existing bound.

`Buffer::queue_metadata_capacity_bound(bound)` queries the actual Core Arc,
`Option<Message<Request, T::Future>>` slots and the exact private std Mutex PAL.
The caller obtains this complete amount before construction, together with common
metadata and all response cells. The constructor checks the typed array Layout,
checked additions and exact Vec capacity, prewarms the final unpublished mutex,
and never grows slots after publication. Request/future/Span/Waker target backing
is separate. Ordinary None constructs no fixed FIFO.

Private strong-only handles export no Weak/raw Arc. The last Arc::into_inner
physically frees Core's Arc before returning its value. Core takes its entire
Option<Mutex<State>> into a local drop frame, retaining original through Vec/value,
Waker and PAL cleanup, including unwind. Receiver Drop moves slots and Waker out
under a short lock, then drops them outside it. User Waker clone/drop/wake and
value destruction all occur outside queue locks. Pending registration rechecks
state after cloning outside the lock, so a concurrent send cannot lose its wake.
Ready receive does not clone a Waker. Close refuses new sends but drains accepted
messages; last sender wakes EOF; these cuts clear the stored Waker's task cycle.

The ordinary/original endpoint enum changes actual Buffer/Worker inline layouts;
production admission and spawn query their exact current TaskCell again. Readiness
future/clone counts, external backing, errors, shared runtime and complete Native
capacity remain separate. This patch description is not validation evidence.
