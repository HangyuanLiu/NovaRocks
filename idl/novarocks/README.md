# idl/novarocks — NovaRocks-native IDL

The single active evolution area for NovaRocks's own wire contract. StarRocks
IDL (thrift + StarRocks proto) stays outside `idl/novarocks` and is not the
active evolution surface; remaining StarRocks imports here are transitional.

Arc: NIDL (NovaRocks-native IDL & StarRocks-protocol retirement). This directory
is the NIDL-0 baseline; later NIDL tasks add the staged packages below.

## Package layout

- `novarocks` (service.proto) — RPC envelope package: the `NovaRocksGrpc`
  service and its envelope messages only. The package name is fixed: it is the
  gRPC wire path (`/novarocks.NovaRocksGrpc/*`); renaming it is a wire change.
- `novarocks.common` (common.proto) — UniqueId, Status, TypeDesc. [NIDL-3]
- `novarocks.expr` (expr.proto) — recursive Expr. [NIDL-3]
- `novarocks.plan` (plan.proto) — DistributedPlan/PlanFragment/PlanNode. [NIDL-3]
- `novarocks.filter` (filter.proto) — runtime filter / lookup. [NIDL-2]
- `novarocks.spike` (spike.proto) — TEMPORARY conversion-layer probe. Deleted at
  NIDL-3. Do not build on it.

## Tag discipline

- Field numbers are append-only. Never reuse or renumber a tag.
- A semantic change is a NEW field plus deprecating the old one:
  `[deprecated = true]` + a tombstone comment
  `// DEPRECATED(YYYY-MM-DD): superseded by <field>, remove after <milestone>`.
- Do not recycle field numbers. When a field is removed, explicitly reserve its
  number/name.

## Comment discipline

- Every message, field, and RPC MUST carry a semantic comment.
- Each RPC/message notes its producer and consumer code paths (owner note).
- No-comment fields do not merge. This is review checklist item #1 — it directly
  answers the "nobody knows what this StarRocks field means" problem.

## proto3 conventions

- Enum first value MUST be `*_UNSPECIFIED = 0`, so a missing/default-decoded
  value is never a meaningful state. (service.proto's `FetchResultResponse.Status
  READY = 0` predates this rule and is fixed in NIDL-1.)
- Presence checks for message fields are centralized in the two conversion layers
  (FE encode, BE prepare), at the decode boundary via `ok_or(...)`. Business code
  never re-checks `Option`. Generated wire types must not escape those layers
  (planner and exec code do not import `crate::proto`; to be enforced by the
  NIDL-D2 guard).
- proto2-only features are not used.

## Compatibility stance

No cross-version compatibility is promised: a NovaRocks cluster upgrades as a
whole. Tag discipline exists only to leave the door open for future rolling
upgrades. Any wire change MUST be called out explicitly in the PR description.
