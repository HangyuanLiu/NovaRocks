# P07 COW 专用消费者接线检查点

日期：2026-10-07。基于 `5bf78e514` 后的本地修改；生产 V1 sink/window 尚未切换，不作为 1FE+3BE 或完整 M07 验收。

UPDATE/MERGE 的 exact match query 改为 `CowMatch` intent，持有从 Connector 签名 preparation 建立的有界 consumer。旧 decoded carrier 逐 batch cast 到 signed layout 后交给 collector；V1 carrier 逐 body 接入领域 assembly/decoder。两者均不先构造完整 QueryResult。外部 COW 写会话只在已验证 selection 返回后打开。

领域 assembly 长度先检查 min(collector budget, codec bound, 32 MiB)，声明越界在扩大 buffer 前拒绝。V1 End 检查完整记录与已收集 row count；EOF 执行 selection/control、signed contract、effect 与目标唯一性校验，然后才能申请 Root success seal。空流保留签名 schema。receipt 在 domain decoder 接管 body 后、原 reply backing 退出后发出；验证失败走 read seal/cut、abort 和原 failure classification。success-seal 请求本身拒绝也改为统一 cut，避免 BE context 只能等 lease expiry。

验证日志：`logs/mem-1-m07/p07-cow-consumer-20261007.log`。Frontend lib 1,415 项通过；只读复核指出 success-seal refusal 漏口后已修复，最终复跑 1,415 PASS。新增 consumer 测试覆盖正确 selection、End 行数不符、半记录 End 与重复目标。现有 relayed split/digest/budget/cancel、COW DML flow tests 继续通过。

尚未证明 domain decode/cast、retained batches、RowConverter/uniqueness/digest 共存全部事前授权。P08 必须接完整 Internal window/有限位置；C7/P09 仍需真实 SQL COW effect、取消与 owner 退出。未运行全 workspace、本轮 Native socket 或 1FE+3BE。

后续冻结 profile 检查：`47ceac6ed` 提交接线。随后 collector 显式限制 64 MiB（原 ConnectorRequestContext 已间接限制总预算）、1,048,576 行与 4,096 batches；行/batch 检查在 legacy cast 前、relay Arrow decode 前进行，零行 batch 也不能无限保留。`p07-cow-profile-final-20261007.log` 的 10 项测试 PASS；新增过大声明行数（payload 仍为单行）的反例通过 ResourceExhausted 与 decoder MalformedBatch 区分拒绝顺序。只读复核未发现新问题。第一次新增用例误用 256 MiB Connector context，超出既有 SPI 预算导致 setup 拒绝；改为合法 64 MiB，未改产品界限。
