# Originally owned response cells

This patch starts from the exact locally cached Tower 0.4.13 source. Only
`Cargo.toml`, `src/buffer/service.rs`, and `src/buffer/message.rs` change.
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
