# MEM-1 M07 P07：调用用途与完整窗口准入

2026-10-08；基线 `6bc1dfb40`。这是生产入口准入切片，尚未冻结/advertise新wire，也未移除旧FE/BE防护。

`FrontendQueryPurpose`由应用caller显式提供：ClientRows、LocalRows、ScalarValue、ProfileCountOnly。compiler在任何Connector planning context或生产构造前核对用途与statement形状；单列SELECT不会被推断为scalar。客户端只有准确的`information_schema.materialized_views` immediate shape选择Local；join、union、catalog-qualified或其他system catalog查询仍为distributed Client。admission和实际immediate producer共用只借用AST的谓词，不分配名字副本。SET scalar即使命中immediate shape仍保持Internal，不带Client窗等待另一Local窗。

普通read在warehouse同一放行事务取得compute许可和完整Client/Local窗口，随后才复制session state/准备查询。typed DML和ANALYZE取得Internal，普通EXPLAIN取得Local，仍使用原warehouse queue；其他typed management和普通session SET/USE取得Local。KILL仍在普通准入之前走原授权control路由。typed session副本和用户变量替换也位于窗口取得之后。含query的SET保留既有整窗与exact child alias。先前SET收据中的无query Local分支是helper行为；本切片补齐实际非query session生产入口。

CPU preparation及被弃置的结果回执继续持有alias到实际worker退出。新的Client与Local取消反例复用真实warehouse准入和受控CPUworker：caller取消、compute permit/root grant归还后位置仍占用，worker实际退出后才释放；既有Scalar/Internal反例保留。

检查：`CARGO_INCREMENTAL=0 cargo test --locked -p novarocks-frontend-application --lib query:: -- --test-threads=1`，48 PASS/0 FAIL；完整Frontend lib1445 PASS/0 FAIL；fmt/diff PASS。已有编译warning仍在。组件测试不代替native1FE+3BE SQL/socket验收。

| 原始日志 | SHA256 |
|---|---|
| `logs/mem-1-m07/p07-purpose-query.log` | `05ace9115b1511fc82768b2cede8b068bd6cc8684a73ae189c1fec77544e9045` |
| `logs/mem-1-m07/p07-purpose-frontend.log` | `ba7c5d10364fe63660865dd11eaf86aef79d23b7df09a6ae8237ea3828a0090e` |
| `logs/mem-1-m07/p07-purpose-fmt.log` | `e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855` |

仍OPEN：ClientRows/Scalar/CountOnly准确freeze与request runtime window传递、finite Internal CPU/collector及最终draft/selection owner、Local实际buffer/schema/renderer aliases、MySQL共享pure Local renderer/closing、P08唯一生产切换、P00b测量、P09与P10。当前legacy decoded read仍由原LRA保护；此窗口切片不声称已证明全部Arrow backing或移除旧保护。
