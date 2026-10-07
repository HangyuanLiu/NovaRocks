# P07 root purpose, carrier and End facts checkpoint

SQL exposes bounded ClientRenderSchema construction from the exact final result
fields, preserving ordinal, alias, nullable and semantic domain. A borrowed
preflight checks wire/backing/metadata budgets, depth and node counts before
allocating the owned schema. Container markers follow the frozen native type
vocabulary; a top-level LargeBinary without its Variant domain is refused rather
than inferred from an alias. Unsupported top-level Decimal256 and invalid TIME
units fail before publishing a schema.

CompletedPhysicalPlanCandidate can install one FrozenRootOutput only while it
uniquely owns the physical plan. It retains final-plan version, display intent and
annotations; the paired runtime-access operation returns the exact access on
refusal. It neither replans nor copies provider access.

The production native read projection selects the declared carrier before
creating an attempt. A relayed ClientRows root requires a Client window; Scalar,
other internal facts and CountOnly require an Internal window. It binds the exact
root task to the bounded ResultData reader and the existing relay/actor driver.
A missing or foreign window is refused. Both launch callers pass their statement
window through this boundary.

ScalarValueV1 publishes exactly one explicit record together with sealed End.
The relay preserves that End's zero-or-one row count, and the typed consumer
checks it against NoRows or Value before completing the segment receipt. Missing
End, a second record and counts above one are refused. The actor carries the
original root output count into EndDelivery only after local consumption, root
Finished and success seal. A conflicting duplicate End is refused; the decoded
transition path carries no root count. CountOnly produces no data segment and
preserves a nonzero count at the same success gate.

Validation on 2026-10-07:

- SQL: 2,571 library tests passed, including four render-schema tests.
- Frontend Application: 1,406 library tests passed, including exact plan freeze
  and result-window class tests.
- Query Application: the initial run passed 508 tests and failed one new test's
  assertion that omitted the permitted best-effort ACK-only request. That
  assertion was corrected to allow ACK-only requests without another fetch;
  the fresh Query Application rerun passed all 509 library tests.
- Logs: `logs/mem-1-m07/p07-root-end-facts-20261007.log` and the fresh Query
  Application rerun `p07-root-end-facts-query-20261007.log`.
- A read-only review found no actionable issue in Scalar receipt/seal ordering,
  duplicate End rejection or legacy carrier handling.

Production compilation still chooses the legacy result sink. Installing the
source-purpose matrix and changing statement admission remain necessary; P08 is
the sole production cutover. These tests do not establish socket/1FE+3BE behavior,
whole-workspace convergence or performance/transport measurement acceptance.
