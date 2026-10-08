# P07 explicit runtime result capacity

Runtime-only QueryResultCapacityBinding carries admission's exact scope and original window through preparation, typed statement execution, finalized distributed requests and profile topology retry. It never enters frozen semantic/native DTOs and acquires no capacity. Child stages delegate the same window. The old decoded delivery/LRA remains active; background sources and domain consumers still require migration before P08.

Validation: Query Application admission tests 10 PASS including two lifecycle counterexamples. FE lib compiled; 1449 PASS, one catalog scheduler materialization test failed then passed alone. Keep this failure pending next whole-tree convergence. Initial new test compile failed for a nonexistent fixture method; the next fixture used WorkClass::Query without its query permit and refused admission. Both fixtures corrected, final 10 PASS. No native/SQL/performance acceptance.

Logs:
- `logs/mem-1-m07/p07-capacity-binding-query.log` SHA256 `ab7aa84b29817e6e98637da5d348f8d3d1dedd4a0f66640aaee8bd09e93d71e6`
- `logs/mem-1-m07/p07-capacity-binding-query-rerun.log` SHA256 `c5b17ecbb46a1f20a8dd1c913e0d559467dd6224fcceb75dd3b7aae31487c0e0`
- `logs/mem-1-m07/p07-capacity-binding-query-final.log` SHA256 `c92ee4bd80dd4b9417ad71da735571d7cf95ced869f26345ae5d1cde5aefa6e6`
- `logs/mem-1-m07/p07-capacity-binding-frontend.log` SHA256 `778a00b74077ddd9b6aa00108c7766167a46dc87bedc67241a92048fc15018d3`
- `logs/mem-1-m07/p07-capacity-binding-catalog-rerun.log` SHA256 `5386f31cb01234f2e7c10e806fc2923cad048dba4c7eca5c72abc32a3bf7dde8`
