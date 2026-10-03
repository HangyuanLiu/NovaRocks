# P04 Signed Native process identity 本地切片

NativeTrust 在角色 composition 提供的唯一 UUIDv7 上绑定 typed Frontend/Backend 身份，JWT 签入 role/process_id；subject 仍只诊断，不是 membership 或 query-operation authority。首次 bind 与 token cache 同锁，清除旧缓存；同身份幂等，跨角色/进程冲突，Trust/TLS clones 共享同一个 inner。null、部分字段、未知角色、nil/non-v7、duplicate/unknown claims 拒绝；已有 unbound API 保留 legacy None，要求准确身份的消费者必须拒绝 None。

FE 将同一 PID 传 logical execution，BE 将同一 PID 传 registry/descriptor，并绑定 listener 与 runtime Trust。Backend 测试只共享 executor threads，各 role 的 trust/cache 独立。JWT closed 字段变更使 epoch 5→6，alternate fixture 6→7，Docker BOM 无需变化。

定向库测试 2,065 项通过：Frontend 1,368、Native Adapter 673、Native Trust 14、Version 10；alternate epoch Version 10 另外通过。新增五项实际签名/续签/篡改/并发绑定测试与 compatibility 隔离回归。fmt/diff 通过。原始日志 lossless gzip 与 source SHA 在 manifest；证据为 parent 741254c8d 上 dirty candidate，不是最终 M07 SHA。

本切片未实施入站 peer/lane seal，也不宣称 bounded auth 全分配图、完整 P04、V1、SQL/default System/最终全量或 Linux 性能通过。入站固定台账、HEADERS 实际关闭和物理退出继续；DNS 的重大路线等待用户。无 push/PR/archive。
