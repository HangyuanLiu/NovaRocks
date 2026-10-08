# P07 coordinator 领域 CPU（2026-10-08）

Frontend Host 唯一 Internal CPU runtime 注入 coordinator。已冻结为 InternalFacts 的请求在 contract/expected set clone 和 decoder factory 前核对原完整包络；move-only runtime binding（含实际 activity fence）贯穿 RequestParts，不进入 FrozenExecutionDescription/native DTO。

每个 root 仅一个 serial domain slot 与一个 pending CPU job。body/EOF/COW digest 和 uniqueness/Write membership 均在独立 4 worker / 4 queue 执行器内处理；原 retained reply 在 CPU 内实际 drop 后才产生 fact receipt，coordinator 领取后 consume + ACK。pending 期间仍处理取消、deadline、task/status、split failure 与 lease/control；EOF job 结束前不发布 EOS 或 root seal。Statistics EOF 与所有任务 success 后的最终成员校验为两次独立 job；write 的原 commit barrier、外部 commit/drain 顺序与 Statistics drain 后 all-success 再核对保留。

actual activity lease 在 dispatch 前登记，与 input / 未领取 output 同寿，领取凭据才解除。取消前台 waiter 不等待 worker；Statistics 专用后台 phase 在 coordinator 已关闭 producer 后等实际 activity 销毁才返回失败，维持现有 physical convergence 契约。Lease 自身窗口先 drop 再通知实际 exit。root/child binding 共用该 runtime fence，不重获窗口。

验证：coordinator 44 PASS（新增坏 body 不发 ACK、Statistics EOF/最终校验分离），admitted context 10 PASS，Frontend 全1458 PASS（含实际阻塞 CPU 取消/fence）。日志 `logs/mem-1-m07/p07-domain-coordinator.log`、`p07-runtime-activity-context.log`、`p07-domain-cpu-full-frontend.log`。首次新测试 StageId import 编译错误修复后重跑。focused 与 full 均为 dev；不证明 native/performance。

这是 selected InternalFacts 分支接线；P08 尚未执行 FrozenRootOutput/advertise 唯一生产切换，旧 Decoded/LRA 保持。MV/maintenance 原子准入、provider publication handoff、P00b/P09/P10 仍待完整目标工作。此波同 HEAD C0 在干净本地提交后执行，结果单独持久化。
