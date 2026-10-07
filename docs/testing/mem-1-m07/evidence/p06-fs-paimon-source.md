# P06 FS / Paimon listing 与 schema probe 接入（2026-10-07）

`FsListingBound` 显式限定 page request、完整 URI 与 source workspace。有限 listing 在
parse/relative-path/context/URI copy 前检查借用输入与 root/authority 的实际 capacity；
URI 按准确长度 reserve，`FsListEntry::into_parts` 移交原 String，避免第二次 provider copy。
generic FS/read 行为保持原入口；bounded stream 拒绝后不再 poll，并退出 lister。
打开与每次 next 均竞争 `FileCancellation::ended()`，SDK future 已 Pending 时也能因 stop/deadline
退出；丢弃 losing future，next 返回一次 typed error 后 EOF，不能只在 await 前后检查取消。
生产 `novarocks-server/src/paimon_access.rs` 和 `connector/paimon/src/role_binding.rs` 分别从同一
ConnectorRequestContext 构造 host FileCancellation 与 SDK PaimonRequestControl，stop/deadline 同源。

FS source auxiliary 的保守式为 `12P + 4(R+A) + 4L + 64KiB`，P/R/A/L 各不超过 64KiB，
合计小于 2MiB。这 2MiB 属于同一次 Paimon listing 的冻结 32MiB source workspace，
不是额外预算。root statuses、SDK dirs/names、嵌套 schema fallback 仍累计使用剩余部分。
行数与 workspace 是独立检查：65,536 项 oracle 使用与 bounded host 相同的 exact URI backing；
额外 spare capacity 的拒绝另有用例，不能据此保证任意路径的 65,536 项均可接受。

Paimon catalog 保留具体 host，用 request-local clone 显式选择 bounded FS 入口。
clone 共享 admitted access/warehouse 的 Arc，不复制整个 authorized paths inventory。
SDK schema-0/schema fallback 的 metadata HEAD 先检查借用路径，经同一授权 client 作 fresh
bounded stat；不读写 scan-size cache，不构造 BoundFile/inventory clone，不接受 BE range binding
或 frozen read size。named-table/BE reader 仍使用原 host，既有 frozen-size 语义保留。

bounded FS 错误投影保持 typed kind，但不 render 不透明 SDK message/context/source chain；
只能借用公开 operation，最多复制 4KiB，超长则固定文本。generic 错误映射保留原诊断。

验证：FS lib 144 PASS、Paimon lib 55 PASS
（`p06-fs-paimon-pending-final-20261008.log`），日志均在 `logs/mem-1-m07/`。
共享 host/FS API 的依赖面复跑：Iceberg lib 1176 PASS、Frontend lib 1436 PASS，
日志 `logs/mem-1-m07/p06-fs-host-dependent-convergence-20261007.log`。
包括 page=1 完整性、same-backing move、组合 URI 超界、拒绝后 EOF、source Display panic、
任意多 paths inventory、fresh replace/delete/cache 不增长、schema 长路径、cancel/deadline、
BE range 两种设置顺序、Pending open/next 的 stop/deadline/drop，以及 SDK ordinary result 与独立 row/workspace oracle。

仍未闭合：OpenDAL 远端 page body/XML 反序列化，REST/Hive 的 SDK 接缝裁决，整个 Local window
的 source/Arrow alias 移交及 P09 原生 1FE+3BE 验收。公开 host 的 opt-in 限定 listing/stat，
不声称任意 GET 也由此受界；当前私有 SDK listing 只调用 list/exists/stat。
本切片没有扩大 ADR-0138 vendor 修改范围，也不证明 P06 或整个 M07 完成。
