# MEM-M02a protocol cost collection

`stock_cost` calls the candidate's actual FundingDomain, ScopeLease,
AllocationOrigin, capacity writer, retirement and maintenance APIs. It never
allocates payload buffers: this is **protocol-only** evidence. Allocator,
header and size-class costs remain M02b evidence.

The user will perform Linux validation manually and explicitly waived
agent-run Linux collection. This handoff supplies frozen inputs and a runnable
entrance; it does not claim formal performance acceptance. All output has
`matrix_complete: false` and `formal_acceptance: false` until a separate
collector verifies every required cell and receipt. Do not change those flags
because a smoke run succeeds.

## Build and smoke

```bash
cargo bench -p novarocks-memory --bench stock_cost --no-run
cargo bench -p novarocks-memory --bench stock_cost -- \
  --manifest novarocks/memory/benches/stock_cost_manifest.json \
  --threads 2 --pairs 1000 --rounds 1 --bytes 4096 --work-us 0 \
  --scope-interval 16 --mode round-robin --variant stock
cargo bench -p novarocks-memory --bench stock_cost -- \
  --manifest novarocks/memory/benches/stock_cost_manifest.json \
  --threads 2 --pairs 1000 --rounds 1 --bytes 4096 --work-us 0 \
  --scope-interval 16 --mode round-robin --variant baseline
```

For formal collection invoke the built release benchmark executable directly
for each independent round. `--rounds 7` executes seven rounds inside one
process and does **not** meet seven independent-process rounds. Cargo's
`--no-run` output reports the executable path; do not guess its hash suffix.
Save stdout JSONL and stderr separately and record process exit codes.

`baseline` means ungoverned equal work. Historical Reservation is the frozen
external checkout at `af9d0591676c75ede5fb53c2d64949e2d80667c5`; it is neither
this runner's baseline variant nor a migrated Reservation implementation.

The exact manifest hash is checked before every invocation:
`cd3f06768bd6b757860b8c17c724a31469eb880ab2674cd608d7b13693a0d676`.
Manifest changes require review before candidate measurement, a new recorded
hash and a synchronized runner hash. The manifest remains a performance input
document, not a performance receipt.

## What one iteration measures

Timing starts before supply work and any scope activation. It ends after the
actual origin deallocation, the exact free acknowledgement, and scope finish
when this is a switching boundary. Boundary objects escape their scope before
release. Modes use the same channels, message ordering, work and timing in the
ungoverned and stock variants:

- `same-thread`: release on the allocating thread and acknowledge through its
  local channel.
- `round-robin`: each worker releases its predecessor's publication and waits
  for its own exact acknowledgement. T=1 has no distinct remote thread and is
  only a degenerate matrix cell.
- `fan-in`: one additional consumer releases every producer's publication and
  acknowledges the specific producer. Record this additional runnable thread
  when defining affinity and available CPUs.

At most one publication per producer waits for acknowledgement; unbounded
channel storage is bounded by this protocol to the producer count. Channel
allocation/transfer and clock costs are included identically in both variants.
The free packet uniquely owns its origin and no code accesses it after free.
Publication count and acknowledged bytes are checked; a mismatch fails the
run. `actual_protocol_released_bytes` is zero in the ungoverned variant, which
has no candidate-origin publications; `free_acknowledged_bytes` still checks
the identical simulated-storage lifetime in both variants.

JSONL records report complete-iteration p99/p99.9/min/max, exact sample count,
checksum, acknowledged and actual protocol release bytes, scope count,
threshold flags, elapsed throughput, idle workset, assembly storage and floor,
parent/root interaction differences and source/dirty provenance. To save all
sorted latency samples, set `STOCK_COST_SAVE_SAMPLES` to an output directory;
the resulting filename includes cell coordinates and round. Preserve raw
samples to judge dispersion and tail population, not only aggregate JSONL.

Initialization and retirement/maintenance cleanup happen outside the timed
static interval. `parent_interactions` sums non-root account counters;
`root_interactions` reads the root counter. These are committed ledger
interaction counters, **not** every growth-gate load or lock wait. Actual gate/ledger counts and summed wait/hold durations come from production
InteractionSnapshot counter deltas. Raw logarithmic wait/hold bins are saved
separately for parent and root. Bin i<31 has upper bound 2^i ns; bin31 is
unbounded overflow. p99/p99.9 are labeled bucket upper bounds. The queue-plus-
hold p99 bound adds the two marginal 99.5% upper bounds; p99.9 uses 99.95%,
which is a conservative union bound for matched populations, not an exact
paired sample. Overflow or an empty/mismatched population yields no finite
bound. Hardware cache-line/atomic instruction cost still needs Linux tooling.

## Hierarchy and input resolution

Select the actual authorization case with `--case`:

- `funded`: retain local workset rights between scopes.
- `refill`: start with W-q rights, refill q at each scope, and trim q only
  after the exact free acknowledgement. This is bounded refill/return churn.
- `debt`: start with bytes/2 authorization; publish the actual successful
  bytes, accept uncovered debt before true free, then settle released facts.
- `refusal`: request `u64::MAX` through the real refill entrypoint every
  iteration, verify denial leaves A unchanged, and separately complete the
  normal bounded publication/free pair. Denied requests and successful pairs
  are separate reported counts.
- `oscillation`: start with W-2q and alternate required free around W by q/16
  through production `ensure_workset`. Fixed quantum refill retains rights;
  scope boundaries do not force repeated return/refill within hysteresis.
- `alternating-sponsor`: cycle each thread through distinct real account/domain
  capabilities; requires at least two accounts and two domains per thread.

Real debt/refusal groups are absolute service/progress groups when no comparator
expresses equivalent responsibility. An ungoverned pair is not a historical
Reservation debt model. Select `--accounts
1|16|64`, `--depth 2|4|8` and `--domains-per-thread 1|4|16`. Accounts are shared
round-robin across threads; each thread cycles its own distinct domains at
scope boundaries. Work query accounts have Service descendants up to the
requested depth. Maximum configured account slots are derived before timing:
`accounts * depth + 2` (root and control). The 65,536 active-owner limit and
64 MiB metadata budget are unchanged from the manifest.

Per-domain W is `max(4 * bytes, 3 * quantum)`. The root ceiling/high target is
`assembly_storage + control_floor + 2 * active_domains * W`. Assembly storage
is configured root/index/Shared backing plus every control/work account
Arc-box fee (`ACCOUNT_METADATA_BYTES * (accounts * depth + 1)`). The root
part comes from an identically configured temporary authority, which is
destroyed before actual assembly. After the real account tree is created,
`pressure.storage_metadata` must equal the predicted total. Historical
payload/record metadata adds a separate actual-byte obligation to high/low
targets. These are size/configuration facts, not candidate timing calibration.
All arithmetic is checked. The control floor is 4q and defaults to 1 MiB.

`--quantum 65536|262144|1048576` selects the frozen sensitivity values. Changing
quantum also changes domain workset and floor according to the frozen recipes;
it does not override the manifest after observing performance. Threshold-only
stock misses do not issue additional rights or fabricate query-limit denial.

## Linux collection and outstanding matrix

Before candidate gate collection record the named Linux host, CPU/NUMA,
compiler, allocator and cgroup, available N and identical affinity. Resolve
once-only supply-work calibration without inspecting candidate results.
For smoke, `--work-us` uses a wall-clock work probe. Formal collection instead
uses `--work-iterations` from the once-only calibration receipt. Run the
release binary with `--calibrate-supply` before candidate measurements, save
its JSON, and pass that file as `--calibration-receipt` to the driver. The
fixed xorshift/black_box instruction recipe is identical in every variant.
The driver rejects a receipt whose manifest hash differs; it never recalibrates
after a candidate result. Clock/burn overhead remains in the smoke variants.

Example for one static cell, after locating the executable and resolving the
Linux CPU list; repeat with alternating comparator order for seven processes:

```bash
STOCK_COST_SAVE_SAMPLES=/path/to/results/samples \
taskset -c "$BENCH_CPUS" "$STOCK_COST_BINARY" \
  --manifest novarocks/memory/benches/stock_cost_manifest.json \
  --threads "$T" --pairs 100000 --rounds 1 --bytes 4096 --work-us 5 \
  --scope-interval 16 --mode fan-in --variant stock \
  --work-iterations "$CALIBRATED_5US_ITERATIONS" \
  --quantum 262144 --accounts 16 --depth 4 --domains-per-thread 4
```

The frozen static axes are T={1,min(8,N),N,2N} deduplicated;
bytes={64,4096,65536,1048576,16777216}; work={0 saturation,1,5,20} us;
free={same-thread,round-robin,fan-in}; scope interval={1,16,256};
quantum={65536,262144,1048576}; accounts={1,16,64}; depth={2,4,8};
domains={T,4T,16T}. Each qualifying cell needs at least 100,000 successful
pairs and seven independent processes with alternating comparator order.
T=2N and saturation remain separately reported; they cannot substitute for
normal-path gates. Apply manifest thresholds separately to each free mode.

The following evidence remains outstanding in a formal result index:

- Collected historical Reservation measurements from the external harness
  described below, with matching full iteration timing and handoff queues.
- Hardware growth-gate cache-line/atomic instruction cost requires Linux
  tooling; software counters alone do not measure cache coherence traffic.
- Machine/source/build and verified affinity receipts must be supplied before
  formal collection. A field named profile is not proof of build configuration.

Do not substitute a subset, shorten a round, change quantum or delete failed
cells to claim a frozen gate passed. Report failures and retain all raw data.

## Deterministic service entrance

```bash
cargo bench -p novarocks-memory --bench stock_cost -- \
  --manifest novarocks/memory/benches/stock_cost_manifest.json \
  --threads 4 --bytes 4096 --accounts 4 --depth 2 \
  --domains-per-thread 4 --variant stock \
  --service dynamic-drain-residual
```

This service probe publishes escaped allocations, finishes scopes, retires
accounts, exercises the unique capacity writer through low/zero/high, frees
origins on a new thread after handoff, and drives all three maintenance
triggers to completed coverage. It records actual returned/transferred
responsibility and progress timing. Empty application I/O evidence means no
real I/O exits are being modeled. It is deliberately labeled a deterministic
protocol probe: it does not meet dynamic cadence, duration, contention or
production teardown acceptance.

## Independent-process matrix driver

`stock_cost_matrix.py` enumerates every supported static axis from the frozen
manifest, including all six authorization cases. It runs independent processes
serially, alternates comparator order, preserves failures/timeouts, saves sorted
samples and publishes an honest result index with missing families. It does
not evaluate formal gates or claim the full matrix passed. A default invocation
only persists the job plan; the full Cartesian product is intentionally large.

```bash
python3 novarocks/memory/benches/stock_cost_matrix.py \
  --binary "$STOCK_COST_BINARY" --available-cpus "$N" \
  --output /path/to/results/job-plan
python3 novarocks/memory/benches/stock_cost_matrix.py \
  --binary "$STOCK_COST_BINARY" --available-cpus "$N" \
  --affinity "$BENCH_CPUS" --machine-receipt /path/to/machine.json \
  --calibration-receipt /path/to/calibration.json \
  --historical-binary "$HISTORICAL_BINARY" \
  --historical-receipt /path/to/historical-adapter/preparation.json \
  --output /path/to/results/full-matrix --execute
```

Use `--case funded` to collect a labeled subset, or `--smoke --execute` for
100-pair entrance checks. Such subsets remain incomplete. The driver marks
alternating-sponsor cells without two sponsors/lanes as not applicable rather
than fabricating sponsor changes. The index keeps the external historical comparator and hardware instruction
measurement visibly outstanding. `--family static|dynamic|residual|all` selects
actual job families; selecting a subset cannot make the full matrix complete.

## Timed dynamic and residual families

```bash
"$STOCK_COST_BINARY" --service dynamic --threads 4 --work-us 5 \
  --work-iterations "$CALIBRATED_5US_ITERATIONS" --mode fan-in \
  --duration-ms 30000 --historical-residuals 65536 \
  --late-free-phase during --accounts 16 --depth 4 --domains-per-thread 4
```

A timed dynamic round uses actual stock scopes, opaque origin free, the unique
writer and bounded maintenance. It starts from committed worksets, waits for
the first low-target revision before letting maintenance drain those rights,
and checks that this revision creates genuine preexisting excess. The writer
then cycles low/zero/high every 10 ms. Maintenance runs independently every
1 ms with budget 64. Successful pairs and refused requests are separately
counted; success throughput must not treat refusals as allocation progress.
The full duration defaults to 30 seconds; shorter `--duration-ms` is explicitly
marked as smoke in each record.

Dynamic remote modes have independent free consumers so an allocation refusal
cannot deadlock a ring whose peer never published an allocation. Round-robin
uses T extra consumer threads; fan-in uses one. This is an absolute concurrent
service group, with recorded runnable-thread count, not a historical throughput
ratio against an unequal free-thread topology. Fixed-iteration supply is
identical across comparable groups.

Historical residuals are created using one reusable activity slot, then moved
in one batch when their execution account retires. Their origins retain actual
payload/record obligations while activity slots are available to new domains.
The registry budget remains 64 MiB. Residual counts {0,1024,65536,131072} and
active-owner counts {1,16,64} are separately executable axes; a residual
record never becomes an activity slot charge. Both high and low targets retain
the actual historical payload plus OWNER_METADATA_BYTES backing in their
recipe. The all-zero target still exposes the entire physical target gap.

Late free uses deterministic coverage events: `before` releases before the
first batch, `during` after the first incomplete batch, and `after` after
coverage completion. No sleep is used to establish correctness ordering. A
nonempty during-coverage set must exceed one batch. No-residual cells explicitly
have no historical publication to inject. Short smoke duration can require
after-coverage injection during cleanup; this is disclosed separately from
an injection completed during load.

The output preserves every capacity revision/service duration and maintenance
coverage/progress counters. Iteration tails use a deterministic uniform
reservoir capped at 100,000 samples per producer, with represented population
and sample count; actual maximum is tracked independently. Cleanup finishes
exact frees and a covered sweep after producers exit. These protocol probes
provide no production I/O exit-bound proof and no formal Linux acceptance.

## Exact-source external historical comparator

The shipped `historical_stock_cost.rs.in` is a benchmark adapter template. It
is not a candidate Cargo target and contains no production compatibility code.
`prepare_historical_stock_cost.py` validates an existing historical checkout at
`af9d0591676c75ede5fb53c2d64949e2d80667c5` and clean core/manifests, then creates
a separate Cargo package outside that checkout. The historical core and its
Cargo files remain unchanged. The adapter depends on that exact source path,
records its template/materialized hashes, and checks HEAD at every invocation.

```bash
python3 novarocks/memory/benches/prepare_historical_stock_cost.py \
  --baseline-checkout /path/to/exact-historical-checkout \
  --output /path/to/historical-adapter --build
HISTORICAL_BINARY=/path/to/historical-adapter/target/release/mem-m02a-historical-comparator
"$HISTORICAL_BINARY" --manifest novarocks/memory/benches/stock_cost_manifest.json \
  --threads 2 --pairs 100 --rounds 1 --bytes 4096 --work-us 0 \
  --work-iterations 0 --scope-interval 16 --mode round-robin \
  --variant historical --accounts 2 --depth 2 --domains-per-thread 2
```

The adapter shares the candidate's exact fixed xorshift instruction recipe,
per-producer tags/checksum, packet/ack queues and ordering, complete iteration
timer, account paths, private workset sizing and lane rotation at each scope
interval. Each real historical `ReservationLease` is dropped exactly once by
the designated free worker before its acknowledgement. Scope boundaries are
lane-switch points; the old core has no `ScopeLease::finish` operation. No
payload allocation, new historical scope API or debt semantics are invented.
The old per-pair `reservation_cost` timer excluded its burn-work function;
that original timer is not used for this comparator.

The driver takes `--historical-binary` and `--historical-receipt` and alternates
three-way baseline/historical/stock order for funded, refill and alternating-sponsor
cells. It verifies pair/free-byte conservation and equal checksums after every
independent process, failing the job while retaining evidence on a mismatch.
Funded and alternating-sponsor are shared protocols that the old API can
actually express. The historical refill run is an explicit native-policy
diagnostic: it installs required W through `try_grow(W)` and a native lease
release, then native `trim()` returns all idle rights after each scope. The
candidate returns q while retaining W-q. Those control operations and backing
retention differ. Historical refill records and jobs set
`semantic_equivalence: false` and `relative_cost_gate_applicable: false`; they
are excluded from relative pass evaluation. Gscope_refill has no equivalent
old partial-return operation, so its comparison remains unavailable and
absolute service/progress gates apply.
Debt/refusal/oscillation are candidate protocol families with an
ungoverned equal-work comparator; an old debt-publication equivalent is not
fabricated. Historical parent top-up/return calls and free-CAS retries are
actual counter deltas, while old gate wait/hold telemetry is unavailable in
that exact core. The output records those limits and remains protocol-only.
Formal collection requires the external comparator preparation receipt;
providing it does not itself prove measured matrix coverage.

## Manual Linux hardware counters

Software gate/ledger deltas do not quantify coherence or atomic-instruction
cost. Record the named CPU model and available events (`perf list`), then use
identical affinity and fixed work iterations for each comparator, preserving
stdout and perf's separate stderr output. For example:

```bash
perf stat -x, -o /path/to/results/stock.perf.csv \
  -e cycles,instructions,cache-references,cache-misses \
  taskset -c "$BENCH_CPUS" "$STOCK_COST_BINARY" \
    --manifest novarocks/memory/benches/stock_cost_manifest.json \
    --threads "$T" --pairs 100000 --rounds 1 --bytes 4096 \
    --work-us 5 --work-iterations "$CALIBRATED_5US_ITERATIONS" \
    --scope-interval 16 --mode fan-in --variant stock --case refill
```

These generic events supply machine context; they do not alone prove gate
atomic or cache-line traffic. Choose and record CPU-specific locked-operation
and coherence events supported by that exact CPU/PMU, plus event scaling,
permissions and unavailable-event errors. Never substitute a guessed raw
PMU code or software acquisition count for measured hardware traffic. This
manual hardware receipt remains outside this protocol runner's acceptance.
