# MEM-M02a behavior and evidence map

This map follows accepted spec revision 3 and approved implementation-plan
revision 3. The candidate suites below are implemented. Execution receipts are
recorded in the approved plan; a suite name alone is not a passing receipt. Retain the
behavior of explicit grants, external bounds and shared-holder exposure; do not
retain unused APIs merely to keep historical round trips compiling.

## Baselines and P00 receipt

- Current upstream execution baseline:
  `b1d13989c50231ee7a5031163255b2607c5176c0`.
- Historical Reservation performance comparator:
  `af9d0591676c75ede5fb53c2d64949e2d80667c5`. Build this exact revision in an
  external measurement checkout; do not copy its mechanism into the new core.
- P00 ran `cargo test -p novarocks-memory` on the current baseline on
  2026-09-30: exit code 0; 147 tests passed across 13 targets; zero failures.
  Local receipt: `/tmp/mem-m02a-baseline.log`. This was the existing core and
  observation baseline, not the candidate protocol.
- P00 ran `python3 tools/ci/check-memory-dependency-boundary.py`: exit code 0,
  PASS. The core declared/resolved normal closure was empty; its dev closure
  contained no first-party, Arrow or Tokio capability. No memory crate appeared
  in the StateStore API normal closure.
- Linux performance collection, new concurrency models, workspace validation
  and native 1FE+3BE verification have not run at P00.

| Existing baseline target | Passed | Long-term behavior to preserve or replace |
|---|---:|---|
| Library unit tests | 74 | Identity, policy, metadata, observation and core transitions; review old-mechanism tests individually |
| `allocator_observation` | 6 | CountingAllocator facts, coverage and source quality remain production contracts |
| `charge_ownership` | 3 | One live allocation has one capacity owner; replace the old Charge layout |
| `conservation` | 7 | Root and ancestor conservation; add independent-domain and root debt counterexamples |
| `control_branch` | 5 | Separate control capability; replace policy-only installation with actual prepaid floor |
| `floor_and_idle` | 8 | Protected floor, idle returns and truthful lowered-limit reporting |
| `holders_contracts` | 8 | Shared holder exposure does not multiply live backing |
| `idle_return` | 1 | Return genuine idle rights, without revoking active rights |
| `metadata_bounds` | 11 | Typed metadata limits; separate activity count from residual record bytes |
| `policy_projection` | 1 | Product dimensions and policy version are distinct from physical capacity |
| `quantized_refill` | 2 | Watermark/hysteresis behavior; separately count growth-gate visits |
| `reservation` | 10 | Historical shared-leaf mechanism is removed; its round trips are not a candidate oracle |
| `transitions` | 11 | Explicit authorization, O-to-L conversion, refusal atomicity and settlement after close |

Passing this baseline does not close the known root-capacity counterexample:
capacity 4, fulfilled 4, uncovered allocation 2, all released, then a new
account must not obtain 6. That target belongs to the new conservation suites.
P00 did not run this counterexample on the old implementation.

## Implemented candidate suites

| Stage / acceptance | Suite | Required behavior |
|---|---|---|
| P01 / V1 | `ledger_domains` | Independent domains retain independently redeemable F; split commits source and destination atomically; O-to-L does not double count |
| P01 / V1 | `ledger_debt` | Capacity 4-to-6 counterexample; one domain E cannot be offset by another domain F; accepting real facts is separate from requesting optional new F |
| P01 / V4 | `dynamic_capacity` | Unique CapacityWriter; target zero/recovery; checked invalid targets; floor is charged once and survives shrink/drain |
| P01 / V2,V4 | `hierarchical_control` | Funded boundaries have zero parent/root interaction; refill stops balance mutation at adequate parent slack; ancestor freeze prevents new down-grants; growth-gate/shrink handshake |
| P02 / V3,V5 | `owner_lifetime` | Stable origin and final access; origin thread exit followed by remote free; generations do not replace lifetime proof |
| P02 / V3 | `residual_handoff` | Teardown and seal are both required; payload survives account retirement; unique cursor; target close and handoff race; common-ancestor commitment remains unchanged |
| P02 / V3 | `metadata_bounds` | Bounded active slots; separately byte-charged stable record/index storage; checked overflow and typed exhaustion |
| P02 / V3,V6 | `residual_metadata` | Many historical residuals do not consume active slots; payload plus metadata transfer without new capacity, storage or slot acquisition; slab backing keeps an owner |
| P02 / V3 | `teardown_deadlines` | Task terminal, future Drop and timeout cannot imply real I/O exit; deadline exceeded preserves responsibility; deterministic exit events |
| P03 / V2,V5 | `thread_stock` | Stock derives from local domain rights; stock miss is distinct from query-limit denial; scope detach before await; no per-allocation query Arc traffic |
| P03 / V1,V2 | `step_settlement` | Workset 4 / stock 1 / allocation 1.5 covered locally; genuine E reported below quantum threshold; funded attach/settle/detach remain local |
| P03 / V5 | `hook_contract` | Every successful, failed, miss, threshold, remote-free and late-free branch has no heap allocation, blocking lock, parent traversal or callback |
| P03 / V4,V6 | `idle_drain` | Capacity reduction, shortage candidate and explicit reclaim all progress; no later poll required; active rights/floor protected; one static scan plus bottom-up round converges |
| P03 / V6 | `shortage_settlement` | Previously published covered free is settled before recheck; incomplete coverage gives Pending; completed shortage carries exact versions, coverage and age; new free starts follow-up |
| P04 / V6 | `refusal_classification` | QueryLimit, Pool, ImpossibleRequest, Closed, Invalid and MetadataExhausted remain distinct; request and constraint identities/versions survive |
| P04 / V6 | `snapshot_consistency` | Settled commitment and active live samples are separate; classification is one committed version; no fake instantaneous full-tree sample |
| P04 / V6 | `pressure_projection` | Query-to-residual transfer includes metadata; U and N do not drop on handoff; root already includes residual and must not add it twice |
| P04 / V8 | `allocator_observation` | Existing physical allocator and coverage facts survive core replacement |
| P05 / V7 | `stock_races` | Production-thread stress complements explicit loom exploration without replacing it |
| P05 / V7 | `ledger_loom`, `owner_loom` library models | Real synchronization seam or documented field mapping; growth/shrink, activate/seal, free/handoff/cursor, coverage/recheck and last-free/reclaim |

All behavior tests use independently issued rights and real transitions as
their oracle. Do not infer expected authorization from the candidate's own
aggregate result. Explicitly reject arithmetic overflow or invalid identity;
do not use saturating subtraction to conceal lost responsibility.

Models must report search bounds and completion. A timeout is not exhaustive
proof. Raw-address publication, final access and reuse require a separate
lifetime review and Miri/ASan where supported; logical models alone do not prove
address safety.

## Consumer inventory and removal boundary

| Current consumer | Required neutral interface/behavior |
|---|---|
| Worker `query_context.rs` | Store/clone AccountHandle; `create_account(Work, ExternalRef)`; one account per query execution |
| Native Adapter `native_fragment_query.rs` | Install Work policy from the existing query limit; the existing MemTracker remains the actual allocation mechanism in this slice |
| Frontend `metrics/management.rs` | Authority/account snapshot, P/B/H and C/L/F/O projections, capacity remainder, bound quality, activity count and control capability presence |
| Server `main.rs`, `app_config.rs` | One injected authority per process; B+H within P; real prepaid control floor before work admission; explicit composition/configuration refusal |
| Server `memory_observation.rs` and Native Adapter `backend_metrics.rs` | CountingAllocator, allocator/physical readings, coverage and unknown-source quality; existing jemalloc/cgroup facts |
| Role composition, BackendApplication and ExecutionRuntime | Inject/forward the same Arc authority; no second capacity authority |

No production allocator outside the memory crates consumes the historical
Grant/Charge/ExternalBound/Reservation/Holder/wait/reclaim/pressure mechanisms.
Their long-term grant/bound/holder semantics still need candidate coverage.
`novarocks-memory-arrow` has no downstream dependency and can be removed with
its workspace/Cargo entries, tests and benchmarks. Production Execution
Arrow/Chunk types are outside that deletion scope.

The Native Adapter test in `native_fragment_query_tests.rs` uses
`request_grant(4096)` only to check that tracker charges have not migrated into
the account. Update its neutral-interface assertion; do not keep a dead grant
API for that test. Other business types named reservation or holder are not
automatically deletion targets.

Remove the old shared Reservation, its protocol/model/benchmark and unused
wait/reclaim/pressure framework. Update exports, Cargo.lock, the dependency
guard and mutation fixtures, memory boundary/performance guides, and final ADR
status coherently. Keep the zero normal dependency rule, neutral dev closure,
and StateStore API boundary as actual Cargo graph checks.

## Frozen cost inputs and outstanding evidence

Manifest: `../benches/stock_cost_manifest.json`.

SHA256 of its exact UTF-8 bytes at P00:
`cd3f06768bd6b757860b8c17c724a31469eb880ab2674cd608d7b13693a0d676`.

The protocol defaults are frozen before candidate measurement: quantum
256 KiB, low/high watermarks q/3q, maximum depth 16, 65,536 active owners,
64 MiB metadata budget and maintenance budget 64 records per turn. The
manifest records deterministic per-cell workset and high/low target recipes,
all plan section 4.4 matrices, minimum rounds/samples and pass thresholds.
Assembly storage in those recipes is prepaid root/index/Shared backing plus
the actual account-tree Arc-box fees, checked against pressure.storage_metadata
after tree creation. Historical residual payload/record metadata is separately
included in target recipes. These amounts are derived from configuration and type sizes;
it is not performance calibration. The benchmark must save each resolved numeric cell input together with the
manifest hash. Residual counts and active-owner counts are separate axes.

The Linux host, N, CPU affinity, compiler/cgroup details and once-only
supply-work calibration are unresolved. Freeze those before collecting any
candidate gate data. Neither a local macOS smoke nor this manifest is Linux
cost acceptance. Do not select quantum, calibration, cells or thresholds after
examining candidate performance. Historical Reservation is an external static
equivalent-responsibility comparator; it has no invented residual or dynamic
capacity counterpart.

The current M08a regression targets include Server `memory_observation`,
`memory_limit`, `cgroup_memory`, authority composition/configuration tests,
Native Adapter allocator/physical metrics and admission policy tests, Frontend
management JSON tests, and the core `allocator_observation` target.

Final integration must separately record workspace check/test and the native
1FE+3BE scenarios `query-lifecycle/distributed-baseline`,
`query-lifecycle/mysql-disconnect`, and
`query-concurrency/16-64-256-governance`. These verify current consumer
regression; they do not prove new stock/residual allocator wiring.

M02a defines the bounded teardown input and rejection protocol. Real push,
pull, poll, network/object-store, blocking/JNI and cleanup owners must supply
I/O kind/identity, cancel origin, maximum inflight work, actual exit condition,
D_io and its assumptions before integration/G4. A mocked deadline proves the
core preserves responsibility; it does not establish a production I/O bound.
M02b/M03/M08b/M10/M11 retain their own downstream acceptance duties.

The user explicitly waived agent-run Linux validation and will collect formal
Linux evidence manually. P06 still delivers the frozen inputs and runnable
benchmark entrypoint. Agent-run Linux acceptance is not a local goal completion
condition; formal performance gates remain unverified until that handoff.

The process authority must outlive active scopes, external bounds (including
zero-byte bounds), and all outstanding allocation origins. Shutdown detaches
only records without publishing or allocation access rights; it does not prove
application or I/O exit. Stable metadata fees include these publication owners.
Inactive settled authorization and pending drain domains are observed separately
from active scopes and hook live samples.
