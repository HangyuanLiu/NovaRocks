# NovaRocks patches to arrow-buffer 58.2.0

Upstream source is the exact Cargo registry copy previously pinned in Cargo.lock; its original source revision is recorded in .cargo_vcs_info.json. No buffer layout, ownership, existing capacity(), allocation, or encoding behavior changes.

## 1. Borrowed standard allocation capacity

Buffer::standard_allocation_capacity() forwards the private Bytes allocation descriptor: Standard(Layout) returns the complete original layout size, including a sliced-away prefix/suffix; Custom returns None. A custom owner can hold more than its declared buffer region, so callers must use a separately issued source-owner backing proof. The getter does not clone, allocate, read cells, or inspect private memory layout.
