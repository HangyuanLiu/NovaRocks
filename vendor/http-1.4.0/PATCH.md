# NovaRocks patch to http 1.4.0

The cached registry crate SHA-256 is
`e3ba2a386d7f85a81f119ad7498ebe444d2e22c2af0b86b069416ace48b3311a`.
All 35 original archive files were byte-for-byte checked against the cached
source before vendoring. Version and dependencies are unchanged.

`HeaderName::from_lowercase_bytes(Bytes)` validates the existing HTTP/2
character table and maximum name length before moving a custom name's owned
Bytes into its existing ByteStr representation. It performs no normalization,
payload copy or new allocation. Standard names keep their static enum and can
release the supplied input immediately. Invalid input is rejected without a
copy. Existing constructors and generic Clone/move semantics are unchanged.

The caller must provide an already-owned immutable payload whose original
backing and physical-exit credit survive every alias. This constructor does not
acquire funding and cannot make a reusable raw workspace safe to retain.
HeaderMap storage/clones/iterators, Status/message/details, Method/Scheme and
the complete Native connection envelope remain separate ownership obligations.
