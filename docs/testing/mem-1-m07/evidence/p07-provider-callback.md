# P07 provider callback 实际窗口保留（2026-10-08）

写提交的 AccumulatedWriteSet 将唯一原 guard 移入 commit callback 的局部最后 owner。Statistics pending attempt 将同一 root 容量保留到实际 finish/abort 返回；publication 的私有 helper 明确先等待同步 finish，然后销毁 request 和 root binding。root 逻辑完成与计算 permit 归还不释放这些 owner。

实际 backing 审查：SPI draft identity/body 及其独立 Bytes clone 带原 guard；provider validate/Theta normalization 保留该 guard。Iceberg statistics finish 的原 artifacts 跨整个 conflict retry、Puffin write、stage、commit、cache invalidation 活着；write 的 ReusableEagerWriteInputs 跨 OCC loop 活着。IcebergCatalogRuntime::block_on 通过加入桥接线程等待实际 future 返回，没有只停止 waiter 的取消捷径。SDK StatisticsFile/BlobMetadata 新事实在此 callback 内受原整窗覆盖；最终 provider publication/cache authority handoff 后属于 provider 元数据，不能把 result guard 永久放进 catalog cache。D13 不承诺第三方 SDK 私有 backing 的字节授权。

新反例通过 production helper/fake provider callback 验证：root 已逻辑完成、原调用方 binding 丢弃，fragment-only write 与 empty Statistics finish/abort 内仍恰有一个 Internal position；callback 返回后归零。写侧测试走实际 outer commit-fragment protobuf codec。第一次错误 fixture vec![1] 被 codec 拒绝，focused 和初次 serial full 因此 FAIL；已改为 canonical encoder 产物并完整重跑，不改 codec/golden。Statistics callback 1 PASS，WriteSession 全34 PASS，FE serial full1462 PASS（dev）。

P07 程序入口/原窗口与 provider handoff 组件已收敛；native matrix/socket/end-to-end 验证仍待 P08/P09。旧保护保留，P08/P00b/P09/P10 与完整 goal OPEN；不是 native/performance 或最终同 HEAD full workspace 收据。

日志哈希：

- `logs/mem-1-m07/p07-provider-callback-statistics.log`: `f68c52532bd746290532408901686240464dad0bce1957d0372d508a9489d953`
- `logs/mem-1-m07/p07-provider-callback-write.log`: `ea17efddf01aeb3e758bb305346ee1668e22bf44feda0762e0ff690291b0ae11`
- `logs/mem-1-m07/p07-provider-callback-serial-frontend.log`: `c5190a48bdcbc84a1a13af4fc342f5e792d7840cb2b8a9729986d9a6eb6e4908`
- `logs/mem-1-m07/p07-provider-callback-write-final.log`: `2498976497161ad7bc6c139505385a4bccc40ef8786d4ab34a93bb7724c9baf8`
- `logs/mem-1-m07/p07-provider-callback-serial-frontend-final.log`: `d6ffc54b425317e6c54cd78ccffffa31ea5f819c7a8137f292ff8095de14a668`
