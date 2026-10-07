# P07 MySQL framing 与 closing 接管切片

2026-10-07，本地分支 `codex/mem-1-m07-be-encoded-results`。此收据证明已实现切片的定向行为；P07 内部消费者与连接准入仍待实施，P08 尚未生产切换，不能据此声称 SQL 产品验收完成。

## 行为

- preparation 从已冻结 root sink 导出 carrier，supervisor 声明给 actor。过渡旧 Result sink 仍为 DecodedBatches；P08 才统一切换。
- MySQL relay 消费已验证 spans，仅做 packet framing。64 KiB 固定 coalescer 在每个 body 结束时 flush；不重组完整宽行。metadata 的借用 schema/name 在转换分配前核对列数和 512 KiB metadata 界。
- 正常 EOF/OK 包与 socket flush 成功后才完成 End receipt/语句 owner；多结果标志保持协商事实。流式结果前的 legacy pending terminator 先带 more 标志写出。
- 取消/准确失败 cut 非阻塞申请一次独立 closing grant。W=2 的 delivering/ready typed owners 覆盖 actor mailbox、pending、stream、protocol 交付及 receipt 间隙，取消冻结与 publisher 共用锁。只复制当前行未写尾，排除 coalescer 已有字节和同段后续行；不 fetch，不 ACK 丢弃的段。缺尾、满池、移交拒绝或坏 IO 关闭连接。
- old/new 共存以冻结 8 MiB complete closing object allowance 在复制前核对（两个至多 1 MiB body、至多 2 MiB staging 与 Arc compaction 共存、512 KiB metadata、64 KiB coalescer/索引/诊断/owner）。普通 grant 转交后释放；残留 Native aliases 仍保留原普通窗至实际退出。独立 closing 绝对期限 5 s，成功 ERR flush 后才恢复连接并结算 generation。外层连接 termination 仍可丢弃正在 closing 的 IO。

## 定向验证

使用 `CARGO_BUILD_JOBS=2 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0`，日志位于本 worktree `logs/mem-1-m07/`，不入库。

| 验证 | 结果 | 原始日志 |
|---|---|---|
| `cargo test -p novarocks-query-application -p novarocks-workload-control --lib` | 502 + 19 PASS | `p07-query-workload-20261007.log` |
| `cargo test -p novarocks-mysql-adapter --lib` | 60 PASS | `p07-window-mysql-20261007.log` |
| opensrv 隔离副本 `cargo test --offline --no-default-features --lib streaming` | 23 PASS | `p07-opensrv-closing-20261007.log` |
| workspace fmt、修改的 vendor Rust fmt、diff whitespace | PASS | 命令终态 |

opensrv 是非 workspace path dependency，直接 `cargo test -p opensrv-mysql` 拒绝测试 dev dependencies，直接 manifest fmt 还会误识别外层 project workspace。验证副本保留本次实际 `src/**`，只将 test module 限为 `streaming`、dev dependencies 限为 Tokio，并加独立 `[workspace]`；正常 dependencies 不改，使用 workspace lock 与共享 target，关闭 TLS 与生产 mysql-adapter 相同。未把这次结果描述为 vendor 全 suite 或 TLS 验证。副本在测试后删除。

新增用例：active delivery 与 receipt 后 actor handoff 均冻结已验证第二段且没有消费 ACK；前段 retire 后仍补 continuation；排除后续行及 coalescer 重叠；U24 精确倍数零包、sequence 回绕、短行不等下一 body；closing ERR flush 挂起/被丢弃时 return slot 保持 detached；普通窗最后 alias 退出后可复用，closing 独立持位且 generation 保持至真实退出。

## 待收敛

生产 query preparation/root install、internal scalar/write/stats/COW、EXPLAIN ANALYZE CountOnly、连接/auth/input/control reserve/期限接线仍待 P07/P08；真实 socket 与 1FE+3BE cancellation/effect/性能验证、SPI 改动后的 workspace 全量、P00b/P09/P10 尚未完成。本收据不替代这些条件。
