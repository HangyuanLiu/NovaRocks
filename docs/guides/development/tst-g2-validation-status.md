<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

# TST-G2 validation status

This is the local validation record for the TST-G2 implementation as of
2026-10-14. The user authorized PR delivery with the remaining validation
deferred and known failures recorded. The original acceptance criteria remain
unchanged; this delivery does not claim that every G2 acceptance gate passed.

The completed runs used the frozen Source24 inputs: 434 files (357 Rust,
62 TOML, one lockfile, seven shell files and seven Markdown documents),
including the recorded 368-file modified overlay, on checkout HEAD
`5aadca36dcc0e0b6673e4eda5e492599d7104eb3` with local implementation changes.
The two later SQL capability-comment changes have a separate runtime-input
bridge confirming that the compiled inputs remained unchanged. This record
describes those run boundaries, rather than certification of a clean Git commit.
Final README/status-document edits are documentation-only deltas from that
frozen run boundary; they do not create a new build or test result.

The accepted design is revision 2 and the approved plan is revision 3. The
original approval snapshot is `50438599edba9db7b2fe20ddc627383d85693105`.
The separately approved, exact 26-test baseline disposition is snapshot
`ce9b46a2aa106ddc60cef87e0fd83842efebad2b`, with criterion
`NovaRocks/workflow/plans/2026-10-14-tst-g2-named-baseline-disposition-v1.md`
and SHA-256
`9f0dc971543ebfb0c962135fee2b2c64d61a82a03bcd6ccc753683181da66228`.
These are local workflow-repository references, not commits in this source
repository or evidence uploaded with the PR.

## Completed verification

| Scope | Actual result | Evidence boundary |
| --- | --- | --- |
| Normal Server, normal System runner, normal SQL runner, and Primary controls Server | Four actual `dev-opt` builds and build-information commands passed. | Normal products reported no test capabilities. Primary reported the 12 owner capabilities and no M07 variant. Actual command waits, source receipts, product hashes, and final raw-file retention were checked. |
| Five independent variants | All five final-source build, self-report, normal-conflict/metadata, and grantless smoke groups passed. | The variants are `mem-1-m07-exact-mysql-write`, `mem-1-m07-closing-pressure`, `mem-1-m07-root-observation`, `mem-1-m07-hms-listing-observe`, and `native-compatibility-test-fixture`. This proves the recorded finite groups, not the full private experiment matrices. |
| Compatible build and native incompatibility case | Actual Compatible build passed; `native-compatibility/other-island-hard-cut` passed. | Compatible has the same epoch and a distinct build identity. The original native case ran with 1FE+3BE, exercised refusal before Worker reservation, and retained role/actual-exit evidence. |
| Owner compile guards | All 12 original final-target guards were actually reached across retained attempts and an exact supplement. | The original stage's failure/unknown facts remain recorded; reaching all guards does not rewrite that stage as a whole PASS. |
| Variant leaf compile guards | All nine original final-target guard facts were verified. | Supplemental actual final-target cfg checks are distinct from the earlier Cargo feature-propagation checks. |
| Representative native controls and baseline/result groups (V3) | All 13 original representative scenarios completed successfully. | The original action and role scopes were reviewed, without broadening the scenarios. |
| Native MV recovery group | All eight original cases passed. | Original assertions, normal BE processes, minimum FE authorization per generation, stop/admission closure, actual worker join before wipe, restart, and cleanup propagation were reviewed. |
| Explicit SQL suites | `lnp-3a-mv-rebuild`: 1/1; `lnp-3d-mv-accelerator`: 7/7; `mv-storage-contract`: 10/10. All three commands exited 0. | Complete original suites used native 1FE+3BE and verify mode, with no case filter or recording. SQL and expected results were preserved; only query 5/6 capability comments changed. |
| Shell entrypoints | Two behavior checks and six Bash syntax checks passed. | These checks do not establish a completed full CI run. |

The reviewed MV and SQL runs retained role/fixture/proxy cleanup errors as
failures and required actual exit before successful completion. Normal BE roles
had no control declarations. Reviewed process and socket absence observations
cover the recorded runs, including FE restarts; a terminal status alone was not
used as physical-exit evidence.

The 12 owner guard targets are Frontend Application, Native Adapter, MySQL
Adapter, FS, Failpoint, Server, Query Application, Execution, Iceberg Connector,
Worker, MV Application, and Version. The nine leaf targets cover four Frontend
M07 leaves, two MySQL leaves, Query Application exact-MySQL-write, Iceberg
HMS-listing-observe, and Version native-compatibility-test-fixture.

The existing variant owner runs also cover the necessary G2 authorization and
exit wiring: Exact 166 passed, Closing 200 passed, and HMS 1,498 passed with
four original ignored interoperability tests. These are retained Source22 runs
with explicit formatter-only bridges to the recorded current owner files, not
new Source24 executions. Their real authorized owner actions and joins were
reviewed; the full private matrices remain NOT_RUN.

Two existing V3 evidence boundaries remain explicit: successful result-delivery
runtime wire/census files were removed by the original harness cleanup, so the
retained phase summaries and executed original oracles are not a claim that the
complete wire files remain available. The vended proxy Drop joins its thread
but discards the JoinHandle error; this delivery does not certify general error
propagation for that existing fixture.

## Full component runs and the approved baseline disposition

Both original full library commands ran with `--test-threads=1`, explicit
`dev-opt` controls targets, and no filter or skip. Both had actual Cargo exit
code 101. Their raw results remain FAIL:

| Package | Passed | Failed | Ignored | Filtered out | Separate disposition |
| --- | ---: | ---: | ---: | ---: | --- |
| `novarocks-frontend-application` | 1,649 | 3 | 0 | 0 | Exact three approved baseline failures |
| `novarocks-execution` | 3,593 | 23 | 0 | 0 | Exact 23 approved baseline failures |

The independent analysis returned
`ACCEPTED_WITH_EXPLICIT_BASELINE_EXCEPTIONS_RAW_FAIL`. It verified the exact
approved names, unchanged original panic causes, original test-name coverage,
approval snapshot, and final retained raw-file hashes. No new failure was
accepted by name alone. Only standard UUIDs in diagnostic causes were normalized;
source locations were retained separately. The six shared-lock poison failures
also required the same original preceding percentile ABI failure.

This disposition applies only to the following 26 existing failures. It does
not waive build, authorization, native, MV, SQL, cleanup, or any other G2 gate,
and does not inherit earlier G7c exceptions.

Frontend Application (3):

```text
query_execution::dml::cow_closed_ast::tests::small_values_keep_original_literal_caster_and_parser_intersection
query_execution::dml::mutation_flow::tests::admitted_cow_consumer_tests::real_cow_consumer_retains_internal_window_through_session_ast_and_last_buffer
query_execution::dml::mutation_flow::tests::cow_rewrite_query_casts_variant_values_payloads
```

Execution (23):

```text
exec::expr::approx_top_k_actual_sql_source_tests::approx_top_k_actual_original_required_sql_complete_compilation
exec::expr::array_struct_subfield_actual_sql_source_tests::array_struct_subfield_actual_required_sql_compiler_closure
exec::expr::between_actual_sql_compiler_tests::between_actual_compiler_decimal_literals_without_integral_cast_source
exec::expr::by_window_required_actual_sql_tests::by_window_actual_compiler_required_nonnull_int_expression_key
exec::expr::by_window_required_actual_sql_tests::by_window_actual_compiler_required_nullable_decimal_value_utf8_key
exec::expr::by_window_required_actual_sql_tests::by_window_actual_compiler_required_running_int32_decimal_key
exec::expr::compiled_program::guarded::temporal_phase_tests::long_zone_data_error_is_full_until_owner_row_projection
exec::expr::inlist_required_actual_sql_tests::inlist_actual_compiler_utf8_case_truthonly_original_required_source
exec::expr::inlist_required_actual_sql_tests::inlist_actual_compiler_utf8_truthonly_original_required_predicate
exec::expr::inlist_required_actual_sql_tests::inlist_actual_compiler_utf8_value_nullable_original_region_source
exec::expr::legacy_decimal_float_cast_baseline_tests::legacy_decimal_float_original_min_scale_debug_panic_or_release_wrapping_not_fixed
exec::expr::legacy_time_source_errors_baseline_tests::original_unknown_zone_deepest_error_retains_entire_long_string
exec::expr::numeric_unary_owned_transaction_tests::numeric_unary_owned_transaction_actual_coalesce_isnull_and_if_are_consumers_of_computed_domain
exec::expr::numeric_unary_owned_transaction_tests::numeric_unary_owned_transaction_actual_sql_three_widths_frame_and_exchange_keep_public_receipt
exec::expr::numeric_unary_public_transport_tests::numeric_unary_public_transport_observed_receipt_preserves_first_control_cause
exec::expr::percentile_hash_native_n1_availability_tests::hash_native_n1_exact_admission_preserves_original_variadic_binding_and_refuses_before_fold
exec::expr::sql_scalar_presence_candidate_tests::sql_presence_candidate_completion_uses_actual_request_mode_before_fold_or_publication
exec::expr::sql_scalar_presence_candidate_tests::sql_presence_candidate_hour_literal_and_projected_column_refuse_before_fold
exec::expr::sql_scalar_presence_candidate_tests::sql_presence_candidate_invalid_pattern_is_not_prepared_or_evaluated_by_admission
exec::expr::sql_scalar_presence_candidate_tests::sql_presence_candidate_snapshot_keeps_exact_bindings_and_table_owner_separate
exec::expr::sql_scalar_presence_original_tests::sql_presence_original_hour_literal_is_bound_without_fold_or_owner_admission
exec::expr::sql_scalar_presence_original_tests::sql_presence_original_hour_projected_column_is_bound_without_fold
exec::expr::sql_scalar_presence_original_tests::sql_presence_original_invalid_regexp_literal_is_not_a_binding_error
```

The Internal COW component coverage gap remains explicit: its original fixture
lacked the original provider-read receipt, so the required consumer/window
property is unproved. Passing MV or SQL cases do not establish equivalent
component coverage.

## Failed attempt and deferred validation

| Scope | Status at delivery | Boundary |
| --- | --- | --- |
| System allocator build/profile | FAIL; no completed qualification | The attempt stopped under the user's 25 GiB disk protection requirement. Cargo actually returned -2, but its original process group still contained compiler descendants after the direct-child wait. |
| Unset-profile behavior | NOT_RUN | The failed preceding System allocator attempt prevented this selected case from starting. |
| Physical wire and restrictions | NOT_RUN | The original positive/restriction checks and four wire tests were not completed for this delivery. |
| Release normal and controls builds | NOT_RUN | Actual release builds/self-reports and the required `debug_assertions=false` checks remain outstanding. |
| `runtime-filter-distributed` | NOT_RUN | The original eight cases remain outstanding. |
| Dataset SQL verification | NOT_RUN | SSB 13 + TPC-H 22 + TPC-DS 99 = 134 cases remain outstanding. READY input validation does not establish SQL verification. |
| Final `local-full-ci.sh` run | NOT_RUN | No final once-through full CI result is claimed; the default SQL, normal component/Server, smoke, and default System stages have not all been demonstrated by that entrypoint. |
| Full private variant experiment matrices | NOT_RUN | No historical private bindings, qualification hashes, or receipts were copied into the current attempt to claim execution. |

The System allocator attempt retained its low-disk/resource failure and original
group-settlement failure. A separate cleanup receipt records exact recorded-argv
verification for residual compiler PIDs 437 and 1156, SIGKILL dispatch, and their
subsequent absence. This cleanup does not rewrite the original FAIL, create a
wait receipt for reparented descendants, or certify the unfinished build.

The user instructed that the remaining validation be deferred before PR
publication. These NOT_RUN entries are outstanding evidence, not newly approved
test-failure exceptions. The observed `dev-opt` product information does not
prove release behavior, final LTO/codegen settings, all dependency overrides or
compiler effects, a cache-free build, Linux performance, or full G2 acceptance.

## Local evidence index

Raw output, actual wait receipts, source bindings, process observations, and
separate analyses are retained locally under the standard experiments directory:

```text
${XDG_STATE_HOME:-$HOME/.local/state}/novarocks/experiments/tst-g2/20261013T074919Z-d8ae151e/
```

This is a locator for the producing machine. Those files and the local workflow
Git objects are not uploaded with this document; their shared availability is
not established by the PR.

| Evidence | Relative location under the attempt directory |
| --- | --- |
| Source24 binding | `p06-final-source24-binding-01.json` |
| Runtime-only SQL comment bridge and scope clarification | `p05-runtime-tags-compiled-input-bridge-01.json`; `p05-runtime-tag-bridge-scope-clarification-01.json` |
| Four current products and final retention | `p06-current-products-refresh-02/summary.json`; `p06-current-products-refresh-02-final-retention-audit-01.json` |
| Five variants | `p06-final-source-variant-independent-12/summary.json` |
| Compatible and native case | `p06-compatible-build-final-source-08/result.json`; `p06-source24-native-other-island-command-12/native-case-reviewed-result-01.json` |
| Owner and leaf guard review | `p06-final-owner-twelve-reviewed-02.json`; `p06-final-leaf-nine-reviewed-01.json` |
| Full FE/Execution raw runs and separate disposition | `p04-current-owner-facts-and-disposition-r2-20261014T021622Z/actual-run/`; `p04-current-owner-facts-and-disposition-r2-20261014T021622Z/approved-analysis/approved-verdict.json` |
| Exact 26-test approval receipt | `p04-named-baseline-disposition-approved-01.json` |
| Representative native groups | `p04-source24-native-control-group-12/command-result.json`; `p04-source24-native-baseline-result-group-12/command-result.json` |
| MV8 and SQL18 | `p05-source24-native-mv-group-12/command-result.json`; `p05-source24-mv-sql-{lnp-3a-mv-rebuild,lnp-3d-mv-accelerator,mv-storage-contract}-12/command-result.json` |
| MV/SQL scope and lifecycle review | `p05-mv8-and-explicit18-readonly-review-20261014T052746Z/REVIEW.md` |
| System allocator raw failure | `p06-remaining-matrix-facts-03/system-and-unset-profile/summary.json`; `system-allocator/actual-build.exit.json` within that stage |
| Separate residual cleanup | `p06-system-profile-residual-actor-cleanup-01.json` |
| Shell checks | `p06-current-shell-entrypoint-results-12.json` |
