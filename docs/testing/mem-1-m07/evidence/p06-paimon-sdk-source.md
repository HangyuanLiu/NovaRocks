# P06 Paimon SDK 收集入口（2026-10-07）

每个 listing 用公开 `FileIO::from_read_only` 建 request-local decorator，在 FileStatus yield
进入 SDK collect 前检查累计 source workspace。冻结 32 MiB 保持不变；构造预检、实际 path
capacity、physical entries（含最终过滤掉的项）、SDK predecessor 与增长 backing、schema fallback
均按同一次 call 计入保守结构界。使用 SDK 原有 database/table/schema 判断和排序，无 vendor 改动。

超界拒绝完整结果，stop/deadline 保持 typed ConnectorError；stream 在拒绝后不再 poll、随调用退出。
本证明止于 host 已创建 path 的公开 FileIO 边界：更早 FS URI format、OpenDAL page body/XML decode
未覆盖。source wrapper 是 SDK 收集结构界，不是 BE scan 预付、内存账本或完整 FE 包络。

验证：Paimon connector 全部 lib 47 PASS，日志
`logs/mem-1-m07/p06-paimon-listing-full-20261007.log`；定向 catalog 14 PASS，日志
`logs/mem-1-m07/p06-paimon-listing-20261007.log`。
包含与 SDK 的 exact ordinary result oracle、根与嵌套累计界、忽略条目与 spare capacity、N+1 poll/drop、
cancel/deadline，以及原先 65,536 项恰好成功和 65,537 项整批拒绝用例。
生产 P09 外部 fixture/1FE+3BE、Local whole-window 完整包络尚未验收。
