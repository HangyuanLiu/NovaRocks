# P07 Local governed delivery（2026-10-08）

状态：Local 生产接线与组件/TCP 反例通过；M07/P07 整体、原生 1FE+3BE 与性能验收仍 OPEN。
代码基线 `cbf471c81`；本检查点新增以下调用与所有权边界。没有推送、PR 或归档。

## 实际生产调用

- FE session Query、plain read 的准确 Local immediate、typed statement Query 三个出口共用 `governed_local_result`。原 statement 的完整 Local/Internal window 在 source/CPU receipt 后仍持有；`GovernedImmediateStatementResult::try_new` 先封闭已审计的 fresh Local graph，再计算旧 LRA 的标量 compatibility charge。拒绝返回原 statement，交原 governed typed-error 路径，不提前结束 generation。
- `OwnedLocalResult` 无 graph/Arrow/schema extraction 或 Clone；adapter只借 metadata name 与准确闭合 kind。Local cursor使用既有 pure renderer，64 KiB/1,024-cell turn；计长、空 batch 与输出 turn 都 yield。fixed 1 MiB segment与 cursor/metadata实际对象声明在 guard 前。
- MySQL governed immediate 进入 `local_result_writer`；逐 cell/whole-batch protocol materialization退出此入口，legacy decoded streaming 路径及旧 ResultCredit API仍保留。借用name先预检再复制Column；与BE relay共用FrozenMetadata、validated spans、row framing、first-row flush、success payload与resident-tail规则。新normal dependency仅指向已有pure result-render，lock版本不变。
- session Query不再在封闭结果之前调用 `complete_execution`：实测该旧次序会使 WorkScope Released、后续旧LRA reserve失效。Local scope现在保留到协议实际终态。

## 取消、实际退出与旧保护

- 从source到普通cursor/socket工作沿同一个exact window；成功EOF写/flush完成、segment/encoder/schema/terminal析构后才完成protocol。没有另取Local窗，没有raw Arrow escape。
- 过渡旧LRA按完整已封闭graph的decoded charge和8 MiB frozen Local root/render overlap保留；初始 reserve/decode/protocol阶段都有现有保护。`LocalProtocolBuffers`先销毁source/segment/metadata，再销毁alias。Closing时取出旧credit交私有closing writer，writer包内实际packets/coalescer/ERR退出后才归还credit；P08再统一移除。
- 初始响应/metadata/row中断单次try独立Closing位置；准确取消事实选AcceptedCancellation，否则选OriginatingFailure，不凭DeadlineExceeded标签伪造已接受取消。KILL CONNECTION/shutdown/IO失败断连；普通取消在合法边界受独立5秒绝对deadline发送ERR。
- cancel先停止renderer，不再渲染缺失尾。只从当前已验证且驻留body按真实partial cursor减去已送/已buffer字节，有限compact当前行尾；同段后续行撤销。当前行跨下一render turn而缺尾则断连。
- 原schema/source/cursor/普通segment都实际退出后，Closing独占metadata/coalescer/row-tail/ERR。原Local位置归还，Closing及语句generation保持到实际writer退出。初始路径的前序pending OK/finalize future也先移入Closing再poll，避免慢前序终结包拖住本语句Local窗。
- source排他性来自P07源码审查的封闭fresh生产函数；该API不证明任意第三方/共享ArrayRef排他，也不替代source增长前保护。前置见 `p07-local-owner-component.md` 与P06各source收据。

## 验证

MySQL新增7项包含真实TCP握手/协议字节与受控partial write：短行/128 KiB跨turn行/下一命令；多结果status和sequence；初始KILL ERR复用；半row header驻留小行补尾ERR、普通Local已归还但Closing/generation与旧LRA仍保持；跨turn缺尾断连不再发布；前序pending OK慢写前即Closing接管；旧LRA/原window实际Drop检查。外层5秒只限定测试失效，不靠睡眠决定取消时机。

| Check | Passed | Raw log | SHA-256 |
|---|---:|---|---|
| query | 525 | `logs/mem-1-m07/p07-local-writer-query-final.log` | `0a7ad0d7ba6df18b009ba5d6aeb3c998d4e9207a0d8995476d83aecd711ff4a8` |
| frontend | 1446 | `logs/mem-1-m07/p07-local-writer-frontend-final.log` | `d0ce09f17ce17c01953385dafa16984da6b4c91c0e4ae6a9cc9ad7ba6f7f1c22` |
| mysql | 71 | `logs/mem-1-m07/p07-local-writer-mysql-final.log` | `9ff2875a20af8ca76f94e505cc159f6624c620620ee207150b5b00982fdb12d4` |
| opaque_doctest | 1 | `logs/mem-1-m07/p07-local-writer-doctest.log` | `a42c4b6a178fd70a6f815815ccbc6a3e7301b4dad830f01edffe55df72999adb` |
| dependency | PASS | `logs/mem-1-m07/p07-local-writer-dependency.log` | `425e2f74e0e6d9d4edcde7a6d653c29bab1f2fddd3d2d53a9f326d1f9900f757` |
| fmt | PASS | `logs/mem-1-m07/p07-local-writer-fmt.log` | `e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855` |

上述定向测试第一次暴露fixture及session Query过早完成scope，已修真实调用次序；无禁用测试或放宽deadline。当前全部定向通过。共享API/normal依赖边的同HEAD workspace convergence另列收据，不从历史bc12收据推断此源码已全量通过。

## 仍需完成

- P07 ordinary completion/error/COM_INIT_DB的finite实际packet/flush/closing接线：当前terminal.rs deferred OK与若干取消分支提前settlement仍需替换；本Local路径的pending finalize交接不能替它证明原OK owner正确。
- distributed FrozenRootOutput、Internal领域有限CPU/collector及write/COW/stats/scalar完整生产矩阵；旧Client decoded carrier和旧LRA不在本slice退休。
- P08唯一完整切换；P00b实际测量；P09原生1FE+3BE功能/性能/尾部；P10 ADR/最终同HEAD证据及Linux用户手工门。

## 同 HEAD workspace 收敛

干净HEAD `9a7f316e6f330494c5b310ce8ed0705054f49c86`，dev cargo-only完整脚本PASS，384秒；12287 passed / 0 failed / 7 existing ignored。guard/mutation/fixture contract、fmt/all-targets/System allocator/Clippy/build/error manifest、组件及Server owner/binary smoke均通过。机器收据 `p07-local-delivery-convergence-20261008.json` 保存各阶段计数/日志sha，原日志 `logs/ci-full/20261008-135850`。未运行SQL、System场景或原生/性能/传输测量；不代替P08之后最终同HEAD验收。
