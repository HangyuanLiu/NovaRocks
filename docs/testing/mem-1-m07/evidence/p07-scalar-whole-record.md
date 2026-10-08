# P07 Scalar 整记录界与 borrowed 消费（2026-10-08）

按 accepted spec §9 的 nested Scalar 既有裁决，整个带类型记录（包含24B头及nested长度/presence）不得超过64KiB。修复原先payload64KiB再加24B头的偏差；完整上限65536、payload上限65512，encoder每次append和header-only接收预检共同约束，超一字节在发布/复制前拒绝。session单值/总量/staged与赋值scratch原上限不变，profile明确分列record/header。

另修复生产消费的owned tree额外限制：合法紧凑记录可能有大量NULL子值，先构造128KiB owned tree会额外拒绝已接受的输入。新增BorrowedScalarRecord，先完整验证，再按冻结字段顺序借用遍历；只有有限schema深度的栈，不复制变长leaf或分配nested容器。生产SET改走该路径，独立SQL literal计量与一次精确分配仍受原128KiB scratch；Optional owned utility显式改名decode_owned，仅用于测试/非生产物化，不作为Scalar语义准入门。

本地准确MV inventory shortcut只接受其closed UTF-8/Boolean字段，先按同一完整typed size预检再格式化，不通用猜用途/类型。保留字段/cell准确性、零行null、一行NULL、第二行拒绝、既有binary Latin1/UTF8与SQL literal规则；没有新窗口或SDK/通用Arrow decoder旁路。现有legacy decoded支路仍保护至P08退出。

验证：ResultContract全51 PASS（含新TLS allocation probe：恰64KiB且65508 NULL子值，validation+walk零分配；坏presence/尾部/超界声明同样零分配拒绝）；Native scalar leaf21 PASS（完整64KiB和超一字节）；Native container12与真实RootSession26 PASS；Query SQL12 PASS（借用leaf/Map/Struct、原语义、3000 NULL不再受owned tree限制、25000 NULL由既有SQL scratch拒绝、本地65512/65513边界）；Frontend串行全1462 PASS。模块拆分后的Query SQL12 PASS。第一次contract全量的4项既有测试原期望payload64KiB+header，因此FAIL；按已接受合同更新边界期望后重跑，未改SQL golden或放宽门。

dev/focused证据不替代native、release性能或最终C0；本次公共API波在干净检查点后另跑workspace收敛。P08/P00b/P09/P10仍OPEN，完整goal active，旧FE/BE保护保持，无push/PR/归档。

日志哈希：

- `logs/mem-1-m07/p07-scalar-whole-record-contract.log`: `1441f04c2bd299ed6628b61d3769c7d066c0f8e5f1a03098f525b89fe3d2b511`
- `logs/mem-1-m07/p07-scalar-whole-and-borrowed-contract.log`: `10580a3735da5f998066e1fbefabf11226305253f5f30f2156b74c58646ec167`
- `logs/mem-1-m07/p07-scalar-whole-record-native.log`: `fb8698a5dec48e18bd96424b87ecdd17a5871b728a579f49d63f3380c81c2292`
- `logs/mem-1-m07/p07-scalar-whole-record-sessions.log`: `56d2f0b5691e2c4162b4ae349f0d4bdb6b19e2489e2af204033f95ef00549724`
- `logs/mem-1-m07/p07-scalar-borrowed-closed-module.log`: `e1abd5a125fb3b44d3b7a1a68a6f5dcda5be9a129496b939316f754101bd14e0`
- `logs/mem-1-m07/p07-scalar-borrowed-full-frontend.log`: `eae7750fbd52513c47d5532fa51177a5ef097c07751c82e3043787693a371d88`
- `logs/mem-1-m07/p07-scalar-profile-arithmetic.json`: `f3ee63bc44d934f835717abde1d9c96424acd79566c80bb2d813658f26ebc340`
