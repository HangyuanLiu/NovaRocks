# Connector SPI dependency boundary

`novarocks-spi` owns Connector contracts. Providers and application hosts consume
those contracts; the contracts must not acquire their runtime, storage, or
execution capabilities.

The finite neutral internal allow-list contains:

| Package | Shared responsibility |
|---|---|
| `novarocks-connector-contract` | Read/write vocabulary and pure provider preparation contracts. |
| `novarocks-secret` | Secret value handling without provider or application ownership. |
| `novarocks-type-contract` | Semantic types, deterministic type rules, source geometry and borrowed control. |
| `novarocks-result-contract` | Result vocabulary, pure record validation/encoding and caller-buffer operations. |

These are allowed roles, not a required dependency set. Arrow vocabulary is
allowed because Connector contracts are columnar. Neutral third-party utilities
are not frozen into an exact dependency array.

## Reviewing dependency direction

Review both declared production dependencies, including disabled optional
entries, and their transitive normal closures. Connector contracts must remain
free of async runtimes, StateStore contracts, provider implementations and
application/execution owners. Review any additional `novarocks-*` dependency
against the neutral roles above; Memory account ownership does not belong here.
A neutral direct dependency does not make its own normal dependencies neutral.

Dev/build edges have separate purposes: SPI's test-only Tokio dependency grants
no production runtime, and a build-host toolchain check grants no product
runtime capability. Review non-default and target-specific features when a
manifest change enables them.

Ordinary container construction, borrowed control and pure record codecs do not
grant scheduler, socket, application or Memory account ownership. Review the API
itself as well as its dependencies: a neutral graph cannot establish that a
particular contract belongs in SPI.

CI does not check dependency direction. Crate structure and review maintain
this boundary; supply-chain policy and generic lints have separate roles under
[ADR-0190](../../adr/ADR-0190-dependency-direction-by-structure-and-review.md).

Dependency direction follows [ADR-0058](../../adr/ADR-0058-crate-boundaries-enforce-isolation-not-source-shape-guards.md).
The separate storage domain follows [ADR-0140](../../adr/ADR-0140-state-store-contract-and-testkit-crates.md)
and has its own [StateStore boundary](state-store-boundary.md).
