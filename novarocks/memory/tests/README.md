# Memory behavior and evidence map

The historical M02a map below follows accepted spec revision 3 and approved
implementation-plan revision 3. Current attribution/lifecycle changes are described
in the M02b section; historical receipts are not current-HEAD receipts. Execution receipts are
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
| P02 / V3,V5 | `owner_lifetime` | FactToken identity and final access; origin thread exit followed by remote free; generations do not replace allocation count/slot pin lifetime proof |
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
| P04 / V6 | `pressure_projection` | Historical M02a rule: handoff preserved U/N. Current rule is U=ΣC_query; actual Work teardown can lower U while reclassification preserves root C/N; residual origin is diagnostic |
| P04 / V8 | `allocator_observation` | Existing physical allocator and coverage facts survive core replacement |
| P05 / V7 | `stock_races` | Production-thread stress complements explicit loom exploration without replacing it |
| P05 / V7 | `ledger_loom`, `owner_loom` library models | Real synchronization seam or documented field mapping; growth/shrink, activate/seal, free/handoff/cursor, coverage/recheck and last-free/reclaim |

All behavior tests use independently issued rights and real transitions as
their oracle. Do not infer expected authorization from the candidate's own
aggregate result. Explicitly reject arithmetic overflow or invalid identity;
do not use saturating subtraction to conceal lost responsibility.

Models must report search bounds and completion. A timeout is not exhaustive
proof. Token identity, final access and record reuse require a separate
lifetime review and Miri/ASan where supported; logical models alone do not prove
address safety.

## Consumer inventory and removal boundary

| Current consumer | Required neutral interface/behavior |
|---|---|
| Worker `query_context.rs` | Store/clone AccountHandle; `create_account(Work, ExternalRef)`; one account per query execution |
| Native Adapter `native_fragment_query.rs` | Install Work policy from the existing query limit; the existing MemTracker remains the actual allocation mechanism in this slice |
| Frontend `metrics/management.rs` | Authority/account snapshot, P/B/H and C/L/F/O projections, capacity remainder, bound quality, activity count and control capability presence |
| Server `main.rs`, `app_config.rs` | One injected authority per process; B+H within P; real prepaid control floor before work admission; explicit composition/configuration refusal |
| Server `memory_observation.rs` and Native Adapter `backend_metrics.rs` | AttributingAllocator, retained CountingAllocator comparator, allocator/physical readings, coverage and unknown-source quality; existing jemalloc/cgroup facts |
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

Active scopes and external bounds retain their required control owners. A
production FactToken uses process-lifetime record storage and may survive the
authority facade's drop; it does not retain a query/account pointer. Authority
drop never substitutes for actual application or I/O exit. Final free and exact
reclaim, rather than origin identity or a zero-byte sample, govern storage safety.
Inactive settled authorization and pending drain domains are observed separately
from active scopes and hook live samples.

## M02b：尺寸分段归属的行为与证据映射

本节记录当前实现的测试入口；执行收据由 approved plan 保存，列出测试名不代表最终验收通过。[ADR-0167](../../../docs/adr/ADR-0167-size-banded-allocation-attribution.md) 是当前长期合同：64 B lane、8 B RecordRef/FactToken、512 B 阈值、尾部 8 B、Q=1 MiB；观测 lane 不授予资金，不消费 stock。

| 验收面 | 真实测试/模型 | 应证明的性质 |
|---|---|---|
| V1 格式 | `attribution_format`、lib `lane::token` / `attribution::tls`、Server `server_binary_smoke` | 非对齐尾部、原对齐/原 Layout、溢出失败、small 无 TLS；真实 GLOBAL 在 jemalloc/System 两构建 |
| V2 释放/保活 | `attribution_release`、`owner_lifetime`、`lane_exhaustion`；L1/L5、`owner_loom` | 同/远端晚 free 一次；槽 pin 和强 owner 保活；回收后不访问；旧代次拒绝 |
| V3 resize | `attribution_resize`、`attribution_explicit`；L2/L3 | 固定地址与必然搬迁后端各覆盖六条尺寸路线，断言地址关系、用户前缀、来源/count/两段事实；受控失败保持旧块，R1 切换不重不漏 |
| V4 作用域/explicit | `attribution_scope`、`attribution_explicit` | 嵌套/unwind、跨线程 poll、Pending/Ready/Drop、spawn 不继承；explicit 优先且恢复 outer |
| V5 政策隔离 | `domain_split`、`lane_lifecycle`、`attribution_scope`、`attribution_hook_contract`；Server/Worker 回归 | 创建/绑定观测 lane 不 qualify、不消费 stock；失败可见，不制造 SQL 拒绝 |
| V6 原生装配 | system `memory-attribution/observation-families`、`query-lifecycle/distributed-baseline`、`query-lifecycle/mysql-disconnect`、`query-concurrency/16-64-256-governance`；Server smoke | cross-process 1FE+3BE 独立进程观测/取消/退出；all-in-one 仅 smoke |
| V7 组成/对账 | `attribution_reconcile`（harness=false）、`attribution_explicit`、`domain_split`；lib `attribution::readout`、Native Adapter `backend_metrics` | 环境 small 只计进程；R1 small 成功后且仅一次；零尺寸无事实；flush 后 tagged 对账，盲区 signed |
| V8 分类 | `lane_lifecycle`、`residual_handoff`、`pressure_projection`；L4、嵌套祖先 `owner_loom` | stop 不改 class，Task→存活 Work 仍 Query；Work teardown U 可降，root C/N 重分类不降；来源不改 |
| V9 关闭规模 | `lane_membership` | 按目标成员计访问步骤，与其他账户/历史 lane 无关；不用耗时充当 oracle |
| V10 批量 | `attribution_batching`、lib `lane::slot` / `attribution::readout`；L1/L2/L3/L4 | hook 返回后余额 <Q，字节先于 count/unpin；在途无先验上界，Q×sampled pins 不是结清证明 |

L1–L5 直接执行生产 `SlotCore`、状态字和回收接口的 loom 原子实现：L1 未 flush +1/远端 −1；L2 计数不变 resize；L3 R1 跨阈值两段发布；L4 teardown/晚 free/flush/reclaim；L5 reuse/陈旧身份。`owner_loom` 覆盖账户/祖先交接与归还，`ledger_loom` 覆盖增长/债务/容量目标。每个 builder 为 max_threads=4、preemption_bound=2、max_branches=20000；完成仅说明有界搜索完成，超时或分支错误不是 PASS。

本地模型入口不接默认 CI，不下载工具链或组件：

```bash
tools/ci/memory-model-checks.sh --loom
tools/ci/memory-model-checks.sh --miri
tools/ci/memory-model-checks.sh --all
# Optional: select an already installed nightly, never install one here.
MIRI_TOOLCHAIN=nightly-YYYY-MM-DD tools/ci/memory-model-checks.sh --miri
```

脚本以 `--locked --offline` 执行 Cargo，Miri 预检已安装 nightly、miri、rust-src；缺项非零退出并报告，`--all` 在缺项时不先跑 Loom。Miri 使用 System 后端与 strict provenance/symbolic alignment，覆盖 lib lane/TLS/readout、全部 `attribution_*` 和旧 allocator observation；验证非对齐 token、realloc/受控失败、System segment、TLS 及最终访问。token 不含指针，不提供 provenance 豁免。独立 `attribution_reconcile` 避免 libtest 后台分配污染对账；压力测试不代替 Loom/Miri。

收敛边界：Miri 组件安装确认尚未取得，C3 未完成；正式 Linux 成本由用户手动执行，G1 待验证。P00 同一 33 套件选择集记录 822 cases /806 PASS /16 FAIL /0 SKIP，最终 SQL 必须与该失败集合比较，不能声称 all 全通过。当前指标/模型/定向测试不能证明所有生产 query/R1 已接线或硬内存治理交付。
