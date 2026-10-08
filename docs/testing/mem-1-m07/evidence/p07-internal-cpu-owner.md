# P07 Internal CPU process owner 组件（2026-10-08）

状态：组件和 FE Host composition/shutdown 接线通过；领域 collector/最终 payload guards 与 coordinator 接线仍在进行，P07 整体及原生验收 OPEN。基线 a91441d33，没有 push、PR 或归档。

- FE Host 持独立 InternalResultCpuOwner，复用既有有限固定 worker 内核实现但不共享 command 的实例。按冻结 Internal positions=4 设置4 workers/4 queue positions，正常 drain 显式关闭/观察同一 worker set；process exit 只关闭 intake，保留实际 job 生命周期。后续 P08 退休旧 Arrow pool，此切片不删除它。
- Private InternalResultValue 在 source/decoder constructor 前检查 exact scope+host、live WorkScope、Internal class 和完整1008 MiB最大共存包络，再调用 producer。transform/input/output/未领取 receipt 始终带原 alias，不申请第二 Internal 窗口；字段次序使 payload先于 guard析构。向 provider/独立clonable backing转交还须将alias明确带入实际payload owner，本组件不证明任意generic T的内部别名排他。
- Runtime forwarder等待有界receipt并唤醒 serial control loop；cancel/Drop只停止 waiter，queued/running closure仍保持 input/guard。worker尚未开始时观察取消并不再调用 codec；成功未领取结果保留完整guard到实际Drop。关闭等待期限不等于工作退出。
- 4项新反例：foreign host/class与不足整包在constructor前拒绝；单线程Tokio下control仍运行、transform复用原窗；取消waiter后running input保持至实际worker退出；成功未领取result保持位置，payload Drop时位置仍保持然后归还。channel/发布waker为oracle，无时序sleep。

完整 FE lib1450 PASS/0 FAIL，包含上述4项和Host drain/shutdown回归；原日志 `logs/mem-1-m07/p07-domain-cpu-frontend.log` SHA256 `a38b7e50cd30f0cd84f670aed66901f1174844731c9dbe16f3701fa62492866d`。初次component test用了错误测试API名称而编译失败，已改为现有request/ExplicitKill，未改产品合同/期限或禁用测试，原日志保留。

当前没有把legacy decoded路径改成新的domain输出；下一步是显式runtime容量接线、COW/统计/写提交最后payload alias与coordinator每流单pending job，再完整生产切换。旧FE/BE保护、P08/P00b/P09/P10及原完整goal保持。
