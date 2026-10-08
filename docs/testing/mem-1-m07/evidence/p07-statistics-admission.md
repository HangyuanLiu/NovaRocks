# P07 independent statistics result admission

StatisticsJobAdmission is the sole move-only submission receipt. It verifies exact host/root query permit, exact original window, Internal class and full frozen backing maximum. FE ANALYZE job obtains permit+window in one warehouse dequeue before publication/phase dispatch. Plain unadmitted create/submit paths have been removed; all fixtures use real atomic Internal admission.

StatisticsAttemptContext explicitly separates the phase child from the independently live root binding. FE request preparation binds the root window, which remains live after the preparation child exits; pending provider session/artifacts also retain it, including empty collections. LiveJob drops its capacity only on queued cancellation/stop or actual recorded convergence, not on conclusion/phase transition. SQL job observation remains pure facts.

Statistics Application18 PASS (two new counterexamples: cross-host query permit refusal despite equal numeric root IDs; queued cancel and last independent alias exit). Existing RecordingExecutor phases now assert live root Internal binding distinct from stage. FE statistics adapter2 PASS; complete FE1452 PASS. No new wire activation. Domain CPU cancellation's physical exit must still be integrated with the phase convergence contract; the independent CPU component is not yet used by these production phases. No workspace/native/SQL/performance acceptance at this slice.

Logs:
- `logs/mem-1-m07/p07-statistics-admission.log` SHA256 `d5ad4abce2e7be3cd13b39804bb9e2d21e7b9ff6d62fb836f7f2b86df2eb802e`
- `logs/mem-1-m07/p07-statistics-admission-final.log` SHA256 `574eb5abddb1ba28b580d45142657216a4d34db9e3ef2a0ffac27992028cf31f`
- `logs/mem-1-m07/p07-statistics-admission-frontend.log` SHA256 `4ddf187222a608cd160e66d373d2d49e40556838a2aa7f6d8ec65a451c7d4f9a`
- `logs/mem-1-m07/p07-statistics-admission-frontend-full.log` SHA256 `b590f128b9e17a3df422f0987038ce0a6b4c2d3619a5930c52fa87614ad937df`
