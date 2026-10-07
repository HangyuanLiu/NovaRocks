# P06 共享接口收敛（2026-10-08）

准确 HEAD：`543759140795a86e400fd263cb1db91471aa558c`，开始时工作树干净。
触发原因：FS 公共 bounded listing/stat、Paimon host/listing trait 和此前 source ownership SPI
共同改变跨模块消费面；各切片定向验证通过后运行仓库现有 cargo-only CI。

命令：`NOVA_CI_CARGO_PROFILE=dev tools/ci/local-full-ci.sh --cargo-only`，用 Homebrew bash。
CARGO_BUILD_JOBS=2、CARGO_INCREMENTAL=0、DEV/TEST DEBUG=0、RUST_TEST_THREADS=1。
cargo-deny 使用既有 0.20.2 helper；完整日志在 `logs/ci-full/20261008-000452/`。
没有并行 Cargo 或性能测量，也没有更改仓库 guard 或放宽检查。

结果 PASS，978 秒：component 12,050 / Server owner 178 / Server smoke 4，合计 12,232 PASS、
0 FAIL、7 项既有 ignored。依赖政策及 mutation guards、fixture/environment guards、fmt、
all-target check、无 jemalloc Server check、workspace build、error manifest freshness 通过。
Clippy 按仓库既有 warning-only 方式执行，PASS 不表示零 warning。
逐日志 SHA256 与机器可复算测试汇总见同名 JSON。

此收据不包含 SQL、native 1FE+3BE、C7 socket、performance、transport coefficient 或 soak。
它是切换前的共享接口收敛，不是 P06/P07/P08/P09/P10 或整个 M07 完成证据。
之后的普通 EXPLAIN 窗口接线必须引用自己的定向验证，不能冒用本 HEAD 的全量收据。
