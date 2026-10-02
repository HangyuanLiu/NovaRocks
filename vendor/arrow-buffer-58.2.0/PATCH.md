# NovaRocks patches to arrow-buffer 58.2.0

Upstream source is the exact Cargo registry copy previously pinned in Cargo.lock; its original registry source/checksum and unmodified file hashes are in UPSTREAM.json. No buffer layout, ownership, existing capacity(), allocation, or encoding behavior changes.

## 1. Borrowed standard allocation capacity

Buffer::standard_allocation_capacity() forwards the private Bytes allocation descriptor: Standard(Layout) returns the complete original layout size, including a sliced-away prefix/suffix; Custom returns None. A custom owner can hold more than its declared buffer region, so callers must use a separately issued source-owner backing proof. The getter does not clone, allocate, read cells, or inspect private memory layout.

## 2. Fixed metadata of a closed standard owner

Buffer::standard_owner_metadata_size() reads the known fixed Bytes type size plus conservative Arc header/alignment. Unpooled Standard returns Some; Custom or a pool-enabled build returns None because an opaque deallocator/reservation can retain additional objects. This additive getter never locks/invokes an opaque owner and does not change standard_allocation_capacity() or capacity(). Source receipts remain necessary for unknown owners.

Buffer::standard_unpooled_owner_metadata_capacity() exposes the same conservative fixed bound before allocation. The borrowed Standard query delegates to this static bound. A pool-enabled build returns None; the static method does not certify any existing Custom buffer or its backing.
