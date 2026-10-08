# P07 领域产物保留（2026-10-08）

Root write/statistics 的 guarded factory 在 decoder 分配前核对原 Internal 完整包络。decoded prepared set 的私有最后字段保留原 binding；StatisticsArtifactDraft 的 body 与 identity 使用 SPI actual-backing guard，因此独立 Bytes clone 的实际销毁仍持有窗口。write session union 仅接收同一物理 allowance 与准确归属，保留一个常量 guard，拒绝 foreign/mixed payload；finish callback 实际返回前保留该 guard。

验证：write focused 59 PASS，Statistics decoder 7 PASS，workload result-window 10 PASS；日志分别为 `logs/mem-1-m07/p07-domain-payload-retention-write.log`、`p07-domain-payload-retention-statistics.log`、`p07-window-identity.log`。新反例验证 fragment-only graph、最后 body clone、foreign union（拒绝后 union 不变）及跨 child 的物理窗口身份。首次 helper 引用类型编译错误已修复后重跑。

此为本地组件 checkpoint，生产 coordinator factory/CPU、provider 最终 publication handoff、P08、P00b、P09、P10 仍 OPEN；旧 Arrow/LRA 防护保留。C0 上一同树收敛为 d1c4e6401，本 checkpoint 不冒充 full workspace/native/performance 验收。
