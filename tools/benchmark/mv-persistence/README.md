# UEA-7 MV persistence measurements

These task-local tools collect reproducible command-level timing and peak-memory
samples for the UEA-7 baseline/candidate comparison. They do not enable any
fault injection and they do not own a workload; the exact workload command is
an explicit argument so the same revision of the tool can measure the adjacent
handoff baseline and the UEA-7 candidate.

Use a clean checkpoint, keep heavyweight builds and clusters idle, and pass at
least seven measured samples. Record non-secret topology, data-size, profile,
and workload facts with repeated `--dimension key=value` arguments.

```bash
tools/benchmark/mv-persistence/measure-command.py \
  --label uea7-b0 \
  --role baseline \
  --profile release \
  --samples 7 \
  --warmups 1 \
  --dimension topology=1fe+3be \
  --dimension dataset=metadata-only-100-manifests \
  --output /tmp/uea7-b0-metadata-100 \
  -- cargo run --locked --release --manifest-path tests/sql/runner/Cargo.toml -- \
       --config "$NOVAROCKS_SQL_TEST_CONFIG" --suite lake-publication \
       --cluster-mode cross-process --cluster-size 3 --mode verify -j 1
```

Run the same command and dimensions for the candidate, then compare the sealed
reports. `--noise-ratio` is the upper edge measured and fixed from repeated B0
runs; it must be chosen before collecting the candidate.

```bash
tools/benchmark/mv-persistence/compare-reports.py \
  --baseline /tmp/uea7-b0-metadata-100/report.json \
  --candidate /tmp/uea7-candidate-metadata-100/report.json \
  --max-ratio 1.10 \
  --noise-ratio 1.03 \
  --output /tmp/uea7-metadata-100-comparison.json
```

`measure-command.py` never invokes a shell: arguments after `--` are executed
directly. Each warmup/sample gets bounded stdout and stderr artifact files. The
report records the exact command, Git state, toolchain, platform, dimensions,
wall time, child CPU time, peak RSS, and exit status. A timeout or non-zero
sample still produces a report but makes the command fail. Peak RSS is reported
in bytes on macOS and KiB on Linux, matching `wait4(2)` on each platform.
Measurements reject a dirty checkout by default; `--allow-dirty` exists only
for developing the tool, and comparison rejects such reports.

The comparison rejects different commands, dimensions, profiles, toolchains,
platforms, sample counts, or RSS units. It checks both median and nearest-rank
p95 wall time, and reports peak RSS without turning a lower legacy-semantic cost
into an acceptance claim. Workload-specific request counts, metadata sizes,
manifest counts, retention outcomes, and codec budgets remain workload output
and must be preserved alongside this report.
