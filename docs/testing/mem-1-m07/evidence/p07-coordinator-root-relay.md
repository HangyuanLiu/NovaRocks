# P07 internal coordinator relay checkpoint

The remaining synchronous coordinator now binds the declared root carrier and
admitted window before initializing an attempt. Its closed intent/kind matrix
accepts CountOnly for Profile, PreparedWriteCommitV1 for Write and
StatisticsArtifactV1 for Statistics. A foreign kind or missing/wrong window is
refused. Runtime capacity is carried by the request, outside its frozen semantic
description; scope checks include both the local identity and host runtime.

The V1 domain reader uses the same RootRelayFrontier as the client actor. It
retains one outstanding body, validates every reply before handing it to the
serial coordinator, and waits for the exact receipt before the next fetch. Domain
validation completes and the retained body drops before recording local
consumption. Complete End requires every receipt; success does not wait for a
final Backend ACK. Write/statistics End counts are checked against the decoded
relation record count, independently of affected-row summary values. Partial
records refuse End.

CountOnly retains no QueryResult batches. Its exact End count and collected
fragment diagnostics form the profile outcome. Normal read seal requires local
End and the exact root Finished observation and precedes drain/Release. Accepted
failure/cancellation seals reads and revokes the poller before abort or topology
classification. Existing in-flight request/body aliases retain their window until
actual exit. The typed fetch failure, including kind, class and topology
requirement, remains attached to the coordinator error; it grants no automatic
or pre-ready retry authority.

Validation on 2026-10-07:

- Frontend Application: 1,414 library tests passed.
- Workload Control: 20 library tests passed, including same numeric scope on a
  foreign host being refused.
- Five new reader tests cover exact receipt ordering, no final Backend ACK,
  CountOnly nonzero count, wrong receipt, late body ownership, terminal cut and
  typed fetch verdict preservation.
- Log: `logs/mem-1-m07/p07-coordinator-scope-integration-20261007.log`.
- Read-only review identified typed failure flattening and a cut-to-seal dispatch
  race. Both were fixed; the follow-up found no remaining actionable issue.

Production compilation still declares legacy sinks; the new request window
binding and source-purpose matrix have no production cutover yet. COW integration,
P07 socket/native 1FE+3BE/effect evidence, P08 deletion, P00b/P09 measurement and
performance gates and same-HEAD workspace convergence remain open.
