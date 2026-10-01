# P03 vendor 模块收据

本地 vendor 流式 MySQL framing/input 已通过独立模块验证；尚未由 P07 接入生产 listener，不是 M07 产品完成或内存/性能验收。

默认 TLS 与 `--no-default-features` 各141项通过。原始日志压缩并保存原始与存储 SHA256；index 绑定整个 vendor `src/**/*.rs` 的内容指纹。递归 rustfmt 与 diff check 通过。vendor manifest 列出三个不存在的 examples，`cargo fmt --manifest-path` 仍按该基线失败；直接校验现存源模块。clippy 只允许既有 value 编码/测试的 `needless_lifetimes/io_other_error/legacy_numeric_constants`，其余按 `-D warnings` 通过。

新 writer 独占 W，64KiB 固定合包，不收集完整 logical row；U24 continuation/zero terminal/u8 wrap 与同一次 socket poll 的 FramingCursor 已验证。lease 只有 terminal 和 flush 实际成功才恢复连接；失败/取消/移交保持 Detached，callback 返回后不读取下一命令。closing 仅使用 frozen metadata 和完整 resident 本行 tail，计完整 backing；不能用小 slice 隐藏大分配。

review 的四个反例已修复并回归：temporal 长度/内容在 shim 前检查；合法 wire 但 conversion 不支持的 zero DATE/DATETIME 与 negative TIME 明确 Unsupported（TIME zero 可用）；prepared/long-data 使用预留 Vec index 和聚合4096位置及实际旧、新 capacity峰值；empty long-data保留空binding语义；legacy send/flush取消永久poison，不复用socket。

新增 ProtocolInputUsage 观察的是 input owner，独立2MiB默认上限不证明完整FE connection的所有物件仍在2MiB内。P07须按已冻结 profile 组合 input/session/statement/writer 的完整保护，绝对期限、8MiB closing能力与全部 IO/Arc aliases 保留至真实退出。这里只完成 vendor 接缝，不移除旧 FE LRA。
