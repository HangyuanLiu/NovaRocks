# P04 SQL scalar semantic handoff

范围：已声明完整 logical type 经条件表达式、嵌套合并、compiler/provider/DML owner 移交到唯一最终 ResultPort；独立 scalar schema proof 必须存在，最终 nested marker 与存储同时一致。单列有界 tree 只构造一次并 move，多列只存 occurrence/domain；不从物理载体猜 top-level Json/opaque。

初始五包回归 3,989 项通过；审查追加 final marker 与 Map entries witness 后，types/SQL 恢复全包 2,649 项通过，定向 SQL 13 / schema projection 11 / nested merge 12 项通过。7 类源码变异全部编译成功后在精确 runtime oracle 失败，并逐字恢复；日志保留初始编译错误和不支持的 nvl fixture，修复记录见 plan。

源码 pin 22 项，dirty parent 为 `45a04a2724d42626ca245ef97d4af307e710c306`。本 wave 因 ResultField/ResultPort 公共合同触发 Cargo-only full CI 收敛。首次 CI 在 peer 握手 ConnectionReset 时序用例失败；精确单跑通过，保留原失败与复跑证据。第二次 `logs/ci-full/20261004-042204` 完整通过，12,139 项通过、7 项既有忽略，431 秒；fmt、Clippy 和构建通过。最终本地独立 1FE+3BE 九项 trust/ingress/compatibility/query baseline 回归全部通过；canonical binary 等于保存的 primary，源码 pins 未变。System evidence 只保留显式 allowlist 投影，原配置/diagnostics/JWT/key 不复制。

仍待实现：内建/聚合/嵌套 wrapper/selector 完整领域事实，QueryApplication/renderer 领域投影，完整 scalar producer/source-growth grant、bounded turn、Host 安装、FE collector 与 session assignment/live/staged。当前 Host 继续拒绝 ScalarValueV1，V1 未发布，P04 executing / P05–P10 open。这些检查不是产品 Scalar E2E、SQL/default System CI、Linux 或 release 性能验收；Linux 由用户手测。没有 push/PR/archive。
