# NovaRocks patches to arrow-schema 58.2.0

Upstream source is the exact Cargo registry copy previously pinned in Cargo.lock; the original source revision is in .cargo_vcs_info.json. No existing Arrow schema/type/serialization behavior changes.

## 1. Clone with caller-owned replacement metadata

Field::clone_with_metadata(map) copies the same name/type/nullability/dictionary IPC properties as Clone, but directly takes the supplied metadata. It avoids first allocating a duplicate of an original map whose retained table/tombstone history is unknown. Source construction owners can build a fresh, bounded metadata map with a construction receipt before transferring it into an immutable field. This helper alone issues no backing proof and changes neither existing Clone nor with_metadata behavior.
