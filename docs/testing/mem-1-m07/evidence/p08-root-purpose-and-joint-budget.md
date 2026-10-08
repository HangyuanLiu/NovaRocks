# P08 root purpose 与 joint capacity 装配（部分收据）

共享入口用途冻结已在 `d248965a2` 提交，仍保留旧防护。最初原生1FE+3BE smoke 22样本/20失败，失败准确保留，不是性能或P08完成。真实生产默认16MiB与组件fixture256MiB的差异暴露V1 RootResultChannel误用legacy单根cap。修复V1采用frozen 256MiB joint per-root、同一4GiB共享process budget；旧BE Arrow单根16MiB保留，绝无第二process wallet。Host在advertise/listener前拒绝过程capacity不足。

```json
{
  "schema_version": 1,
  "status": "P08_PARTIAL_COMPONENT_CHECKS_WITH_RETAINED_NATIVE_FAILURE",
  "initial_purpose_head": "d248965a227239f1761fbcbe13cc02fd28279948",
  "profile": "dev correctness only",
  "initial_native_smoke": {
    "samples": 22,
    "failures": 20,
    "root_cause": "V1 root channel incorrectly reused legacy 16MiB per-root cap; full input reservation exceeds it",
    "log_dir": "logs/mem-1-m07/p08-v1-native-dev-smoke-20261008"
  },
  "fix": "V1 per-root allowance from frozen joint profile, legacy 16MiB unchanged, same process budget; insufficient process cap refuses before listeners",
  "checks": {
    "frontend_serial": 1462,
    "initial_host": 10,
    "fixed_host_startup": 11,
    "production_legacy_16MiB_regression": 1,
    "bounded_root_driver_and_context": 8
  },
  "logs_sha256": {
    "p08-root-purpose-check.log": "e7c1643ca51937e877a2de1781c8dc36d3770afe5362650ef73e621842a20f70",
    "p08-root-purpose-frontend.log": "b228425dfaac0f6b11445a00d751e008825f9bb24b82cc51823b8c11074d58df",
    "p08-host-advertise-native.log": "9e51fb79939dd5f20cd0968e59dd2fdbe78f54bb2fb90e4b1a160e5657bcee87",
    "p08-root-joint-budget-regression.log": "7f34b5a62750ce8abedf22f02a409ed8e2a0f03a394610168b79feefdd74a4ce",
    "p08-root-joint-budget-native.log": "e96d52e086bac8b22b6a0374ec3897dd8f071ba3497236c4d9cd2f4aa0422980",
    "p08-root-joint-budget-host.log": "e93dbd8be56c21866b223624576c648e2b52aff6326f9756e8bf81b2013de01a",
    "p08-v1-native-dev-smoke-driver.log": "283bd71ad8994bc61ef66d318fca26f61f0af33a5ef88eebcf18fd3c7325772a"
  },
  "remaining": [
    "Re-run same native smoke on fixed clean source",
    "Host/lane/ingress capacity convergence",
    "Complete FE legacy retirement",
    "P00b coefficients and passing baseline quality",
    "P07/P09 native/socket/domain matrix and release performance",
    "P10 and same-HEAD final convergence",
    "Linux user manual gate"
  ]
}
```
