# P07 internal consumers and profile outcome checkpoint

Write and statistics decoders now own their bounded record assembly. Bodies feed
complete records directly into the existing typed validators; split records may
retain at most the frozen 32 MiB assembly share. EOF refuses a partial record
before changing the decoder's EOF state. The write prepared-set membership and
summary checks and the statistics exact artifact membership/all-success gate
remain required. This does not grant an external commit or publish partial facts.

ProfileExecutionOutcome now contains only an exact u64 output count and fragment
diagnostics. Completion formatting reads that count. The legacy coordinator
still materializes rows before extracting their count; CountOnly production
selection and the bounded coordinator reader remain open.

Validation on 2026-10-07: Frontend Application 1,409 library tests passed in
`logs/mem-1-m07/p07-internal-consumers-20261007.log`. Tests feed Backend-encoded
records in three-byte bodies, compare the resulting typed facts to the original
relation, refuse partial EOF, and retain the statistics all-success requirement.
A test initially supplied a zero summary while expecting five affected rows; the
fixture was corrected to use the authoritative summary count. An exact u64::MAX
profile count requires no QueryResult payload.

This checkpoint does not switch production internal root sinks, grant their
statement windows or prove socket/native 1FE+3BE/effect behavior. Those P07/P08
integration items and P09 performance/transport gates remain open.
