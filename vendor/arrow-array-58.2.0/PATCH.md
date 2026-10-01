# NovaRocks patches to arrow-array 58.2.0

Upstream source is the exact Cargo registry copy previously pinned in Cargo.lock; its original source revision is recorded in .cargo_vcs_info.json. No array layout, data type, ownership, validation, encoding, or mutation behavior changes.

## 1. Borrowed column-vector allocation capacity

RecordBatch::columns_capacity() and StructArray::columns_capacity() return their private owned Vec capacity in elements. A root input owner uses checked multiplication by the element layout to cover spare storage without cloning or normalizing the original vectors. Existing columns() and all array behavior remain unchanged. These getters do not prove schema metadata HashMap backing or foreign/custom allocation capacity; those remain source-owner responsibilities.
