# UEA-5E session 交接 prompt

请继续实施 UEA-5E。用户已经批准 accepted spec v7 和 approved plan v7，允许使用 sub-agent。请先读取当前 workbench router、execute skill、AGENTS.md，以及下面的计划和最近证据，再继续实现。不要重新询问已经裁决的语义，不要把局部 helper 通过视为整体完成。

## 工作区与授权

- 原工作区：`/Users/harbor/.codex/worktrees/46e8/NovaRocks`。
- 分支：`codex/uea-5e-physical-wire-v2`。
- fork：`git@github.com:HangyuanLiu/NovaRocks.git`；origin：`git@github.com:NovaRocks/NovaRocks.git`。
- 本次交接 WIP 与此文件在同一提交。新 session 先读取实际 HEAD、远端分支及工作区状态，不依赖旧 session 的运行对象。
- 开发分支已经包含当时更新后的 origin/main `b1d13989c`，其祖先关系已核对。保留全部实现历史，不要重置到 main。后续 main 更新先检查兼容与差异，不能覆盖已做工作。
- 用户本次只授权保存代码并推送 fork 快照；没有要求 PR 或归档。继续本地实现已获授权，后续发布按用户的新指令处理。
- 原 session goal 当前为 paused，任务并未完成。新 session 按 execute skill建立自己的持久目标，完整保留下述范围。
- 用户已关闭 fast mode，使用正常模式。

## 完整目标及文档

基于 accepted spec v7 / approved plan v7 完成全部 E00–E08 及内部 DAG：唯一完整 FragmentPackage、双向有界且无损 codec、准确语义/参数/效应、纯 kernel catalog、独立 LocalProgram compiler、selected-row 求值、FE 合法优化及按完整分组键合并的 partial TopN；保留 Task 生命周期和现有生产者。最终需要真实 1FE+3BE、完整 NativeCompatibilityId、正式 MEM 接线收据，以及候选性能测量前冻结的 B0 成本证据。不能用 all-in-one、静态检查或旧 SHA 测试替代最终验收。

- DOC_ROOT：`/Users/harbor/Documents/Obsidian/NovaRocks`。
- Spec：`workflow/specs/2026-09-20-uea-5e-physical-wire-v2-design.md`。
- Plan：`workflow/plans/2026-09-20-uea-5e-physical-wire-v2-plan.md`。
- 上面两项相对 DOC_ROOT；plan frontmatter 为 approved、plan_revision=7、spec_revision=7、verification_status=source-review-only。
- 文档很长；先读目标、阶段依赖、最新追加收据，按问题定向查找。不要把老 evidence_commit 当成当前最终 SHA。
- 生产链仍是 FE v1 codec / BE construction ExecPlan→LocalProgram。完整 22 字段 typed sender/receiver 未完成，独立编译器仍有 Unsupported 家族；整体尚未关闭。此前约 65% 是主观估计，不能作为验收结论。

## 已确定和未确定的语义

用户已明确裁决：

1. 非恒定 RAND seed：每个实际选中行按该行 seed 初始化并取样；恒定 seed：实例内跨 batch 连续序列。
2. LargeInt 比较统一有符号 i128 顺序。
3. IntervalMonthDayNano 使用显式 `months: i32`、`days: i32`、`nanoseconds: i64`。

仍未裁决的 SUM overflow、COUNT OVER 逻辑 NULL、guarantee-only proof 设计，需要在触及相应实现时回到设计讨论，不得猜语义。

## 最后已验证代码检查点

最后已验证代码提交：`6e988a3f03d7179d251f42dc1d9967ca706ebb2f`，提交名 `Collect original provider and schema sources for package encoding`。前两个检查点是 `8e06cf5d4b5042b199f8f2e9bd0723bf3e27be48`（binding sources）和 `a58da10b4c4f8b0cbf5c83a42560fa0925d09a19`（borrowed type views）。

证据目录 E：`/Users/harbor/.codex/uea5e-evidence/46e8/2026-10-06`。这些证据及 Obsidian 文档保留在本机，未随 fork 推送；换机器时不能假定存在。

- `e02-package-wire-node-source-manifest-provider-source-quality-final.json`：5 个任务 Rust 文件、882 依赖、14 packages、3 protected files 的冻结来源；SHA256 `709c3cf45ff44dab59ef11ea2fdba89cdece9c4961fa1cf22381f3492923f40c`。
- `e02-provider-sources-tests-verification-provider-source-quality-final.json`：实际终结 session 16291，exit 0、build true、error 0；`physical_package_v2::` 下 38 个 distinct PASS（7 新增、31 原有）。SHA256 `e79b87837fae86c339c1ca53adfd6018eb93097730c5b0311469ea1586e3ca51`。
- `e02-provider-sources-clippy-verification-provider-source-quality-final-clippy.json`：实际终结 session 40124，exit 0、build true、error 0；275 raw / 244 unique warnings，任务来源 50 个均为尚无完整消费者的 dead_code。不是零 warning。SHA256 `7d3e7bad034a0f27270c67bccd4564ef3779e77e4ec398116a04dcf4848c0044`。
- `e02-provider-sources-checkpoint-receipt.json`：上述 5 个提交 blob 与冻结来源相符；SHA256 `5816604ddd459aa0bee74fa54785eb0cc156dc7fbb7ca451de4d6c2fd10ec5b0`。
- `e02-provider-source-ports-independent-review.md`：独立复核来源及结果，SHA256 `895f87b6ad07b6ade6ec9ade4e03ebc6ecbb29f23419d1256f4b1693723fd004`。

该轮动态覆盖 Data/Metadata Read + Data1 Writer，不能扩大为全部 6 Read / 5 Write / 22 字段 / native 覆盖。当前 WIP 改动已使这份旧冻结清单不能代表当前 HEAD。

## 本次保存的未验证 WIP

以下路径相对工作区：

1. `novarocks/plan-codec/src/physical_package_v2/binding_sources.rs`：增加 crate-private `check_package_in`，用于准确 package 指针、control、budget 和累计 floor 的原有校验，未经过本轮 Cargo。
2. `novarocks/plan-codec/src/physical_package_v2/definition_sources.rs`：作者交付的源收集器。收集 Constants/Values/Expressions/Requests/Cuts/Result/Writer 原始对象；11 个可移动缓冲与 11 个显式限额；复用 TypeViewBudget 和 SourceInputPrefix 的实际 count/fill 观察。提供原始 borrowed type-ID / request argument / writer type views，不克隆或重新构造语义对象。作者 FINAL SHA256 为 `2c73fd7ac4af1f1d42f8c575514b580a1e3fcb66735990b0d7d93866279b0ff4`。独立 review 没发现静态 blocker，但还未注册到父模块，也没有 Cargo 验证。
3. `novarocks/plan-codec/src/physical_package_v2/definition_sources/tests.rs`：中断时的测试草稿。只有 fixture/helpers，没有 `#[test]`。已知两处 `StaticFunctionArgument::Value { ty: ... }` 字段名应为 `value_type`，尚未修复；不要视为可编译测试。含真实 scalar/lambda/aggregate/table publication chain、typed NULL vs None、稀疏 ID、NUL 字段名、writer fixture。
4. `novarocks/plan-codec/src/physical_package_v2/admission.rs`：root 的整体编码累计数值账本草稿。四轴 limits，固定 23 Component slots，原始 source/inline frame 一次，component snapshot 单调替换避免 prepare/emit 重复计费，同一个实际 controller。未注册、未经过 Cargo、还没有实际 whole encoder 消费者。其 numerical bound 不能替代正式 MEM grant、allocator receipt 或 opaque CPU 许可。

四文件交接冻结 SHA256：

| 文件 | SHA256 |
|---|---|
| binding_sources.rs | `1bc9c4ce6b7ebed38c908aad9161a9352de7f422957dadfd9652d8bf6fc0f21a` |
| admission.rs | `98edcdb4e3e0ebde7061c894ca48f1906de1369ac4800fb591b9c7dbf9863734` |
| definition_sources.rs | `2c73fd7ac4af1f1d42f8c575514b580a1e3fcb66735990b0d7d93866279b0ff4` |
| definition_sources/tests.rs | `6590578fb0f378669f1e8da0692d70ad5c156cc321376bbfc95de4257bc412b6` |

本次仅 scoped rustfmt --check 与 git diff --check 通过；没有对 WIP 运行编译、测试或 Clippy。父 `physical_package_v2.rs` 没有注册 admission/definition_sources，因此旧模块测试通过也不能证明它们可编译。`encode.rs` 尚不存在。

补充交接：E 下 `e02-definition-sources-owner-handoff.md`、`e02-definition-sources-independent-review.md`。优先核对当前代码和作者来源，避免旧 review 被误用为动态验证。

## 建议接续顺序

1. 查看 Git 状态及实际代码；旧 agents 已停止，不要依赖它们的内存。可以重新分派：一位完成 DefinitionSources 定向测试，一位独立审查账本与完整 DAG，root 负责接入及唯一 Cargo 执行。
2. 修复并完成测试草稿，审阅 DefinitionSources 和 binding guard，注册需要进入编译的模块。全部 Rust 作者冻结后，由 root 跑一次强相关模块测试和 owner all-target Clippy，保留实际终结日志、准确 source manifest 与 SHA；只在新错误/新改动时复跑。
3. 阅读 E 下 `e02-whole-sender-assembly-next.md`（SHA256 `12c52e86f53d8de10b5fa47eed013372364a168330b7abf30503413d325d59bb`），它包含完整 22 字段 sender 的原始 API 和依赖顺序。Constants 使用真实 ConstantRecordTypeIds 和 original pools，没有 ConstantNamespaceSource。继续 whole sender，而非继续扩大无消费者 helper 波次。
4. 接整体编码累计 admission：同一源对象、同一控制器、同一累计账本；准备与 emit 的 snapshot 替换不能双计，完成节点的 child contributions 累加后替换，稀疏 ID 不需要新 map。首次任意 parent callback 前已知 output-node Vec 的申请几何必须入账；原始 owned inline headers 与 source union 要清楚；TypeViewBudget 持有 parent FnMut 时的借用协调尚未解决。不要新增伪 `_in` facade、validator 或隐含默认。
5. 阅读 E 下 `e02-whole-receiver-assembly-next.md`：真实 Values/Expr/Node/Envelope → 原始 Fragment::try_from_structure_in → 必需 with_call_requests_in → Control/RootUses/Calls/Pruning → 唯一 Package::try_new_in。保留原始 laws。结构/request/property 的 scratch 数值与 sparse BTree 插入前首 gate 等定量缺口仍未关闭。原始 Prost raw bytes author 也需完成准确有界路径。
6. 再沿计划完成其余 compiler/kernel/selected-row/FE legality/生产链替换与正式验收，不缩水为 codec helper 交付。

既有 provider fixture 的经验：最初 source B 一次；后续原始 namespace 已准入的 request-byte 上界与实际 token inline 进入下一阶段，允许保守包含 temporary requests；不能累加已经各自含 B 的 lower floors。source/preorder ID `[0,1,2]` 和原始 emitter child-before-parent wire 顺序 `[1,2,0]` 都合法；按 ID 核对节点，保留原 emitter 布局。

## 验证与协作约束

- 用户明确禁止全量 SQL test，也禁止把每次改动都变成全量 SQL/SQL-crate/workspace test。运行强相关 case。必要的公共契约收敛可以做 workspace compile-only，不能每个私有 helper 都做。
- Cargo 仅 root 执行，`-j 4`；共享 target：`/Users/harbor/.codex/uea5e-build/46e8`。先冻结所有 Rust 作者，不能让 sub-agent 并行运行 Cargo 或共享 Git commit。实际 running session 先等到 terminal，不能启动重复任务。交接时没有本轮新启动且仍运行的 Cargo。
- 定向 owner 为 `novarocks-plan-codec`，模块 filter `physical_package_v2::`；先核对当前 Cargo/package 与 manifest，再执行。源收据、检查日志与 commit 必须对应相同来源。
- 所有历史失败保留原判定，不合并成当前 PASS：session 32348（3 cfg API 错误）、63042（34 PASS/4 FAIL，来源计费复用错误）、91468（35 PASS/3 FAIL，golden wire 顺序）、6189 和 32240 是更早来源；最终已验证基准为 16291/40124。
- 用户交流及设计文档中文，代码注释/日志/错误/commit/PR 英文。现有 native frozen contracts、type metadata、FE/BE 与 connector 所有权都保持准确；没有 fallback、猜默认、隐式降级或全局桥。

## 必须保护的无关用户文件

这三个文件在原工作区是未跟踪文件，不属于 UEA-5E，未被此次提交推送。不要编辑、删除或顺手加入提交：

| 路径 | SHA256 |
|---|---|
| novarocks/functions/src/builtin/vector.rs | `3444e195f7941dca9a7a5a5bd6715db6607c00ee936fe7f8ee3d78672816fb2d` |
| novarocks/functions/src/builtin/vector_owner.rs | `1f5d78b8e387c4825f7715e99a880872d5aa708c09d5e669f47c6ff88c5386a8` |
| novarocks/functions/src/builtin/vector_tests.rs | `2826a5ea3aacbb20d81d65efe200dfa3a7b654039cbca781636c6fea8944d473` |

请据此继续工作，首先说明实际恢复到的 SHA、WIP 验证缺口和当前紧邻任务，然后持续实现；无需重新询问已批准的 plan 或上述已裁决语义。
