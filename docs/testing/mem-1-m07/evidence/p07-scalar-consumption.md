# P07 typed Scalar consumer checkpoint

The governed SET consumer accepts the declared ScalarValueV1 carrier only with
its exact frozen ScalarSchema. It requires exactly one explicit record, including
NoRows, checks the delivery row count, and returns an assignment only after a
successful End. Duplicate, missing, foreign-domain and late-failure records are
refused. A completed record is decoded and formatted before its receipt completes.

Owned scalar decoding charges every container Vec capacity and variable leaf
before allocation against the cumulative 128 KiB child limit. Small wire records
containing many NULLs cannot expand into an unbounded owned value. Parent vectors
and already decoded siblings remain charged together.

Typed literal conversion walks borrowed values twice: count first, allocate the
single exact output String second. It does not rebuild a container Arrow value,
Literal tree or child-string vector. Output plus the conservative maximum live
fixed-leaf bridge allowance must fit the 128 KiB assignment scratch bound.
Top-level UTF-8-lossy binary and nested Latin-1 binary formatting, nested decimal
formatting and existing unsupported-type refusals follow the previous Arrow path.

Validation on 2026-10-07:

- Result Contract: 49 tests passed (19 unit, 17 scalar leaf, 13 scalar schema).
- Query Application: 503 library tests passed, including 10 literal/session tests.
- Frontend Application: 1,405 library tests passed, including 7 governed Scalar
  stream tests for values, NULL, NoRows, missing/duplicate/foreign records and
  late failure.
- Logs: `logs/mem-1-m07/p07-scalar-{owned-decode,integration}-20261007.log`.

This checkpoint does not switch production SET preparation to ScalarValueV1.
The legacy decoded-batch carrier remains until the P07 source-purpose wiring and
P08 production switch. Immediate/legacy Arrow literal conversion still uses the
existing conversion path; its private scratch bound is not established by these
typed-consumer tests. Native socket, source-purpose and 1FE+3BE evidence remains
pending. Nothing here proves the P09 performance or transport measurement gates.

## Exact child window delegation (2026-10-07)

SET delegates its admitted window to the exact live direct child WorkScope.
The workload owner checks both the authority and parent identity before retaining
the child. Delegation preserves the original window class, envelope and position;
it does not acquire another pool position. Clones share one child responsibility
holder, released only when the final alias exits, even after child completion.
Foreign hosts, siblings, completed scopes and Closing windows are refused.

Validation: Workload Control 21 unit, 74 integration and 8 doc tests passed in
`logs/mem-1-m07/p07-child-window-workload-all-20261007.log`. Frontend COW
consumer tests passed (10) in
`logs/mem-1-m07/p06-cow-row-preflight-fe-third-20261007.log`; compilation covers
the SET caller. Independent read-only lifetime review found no concrete issue.
This is a delegation foundation: production SET root-purpose/window acquisition
and native socket verification remain pending. No result-window cutover, full
workspace, performance or transport gate is claimed.
