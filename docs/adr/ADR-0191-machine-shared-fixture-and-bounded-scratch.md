---
id: ADR-0191
title: "One machine fixture activation owns shared services and bounded scratch"
domain: [test-fixtures]
status: active
supersedes: [ADR-0165]
superseded-by: null
date: 2026-10-21
provenance:
  - "accepted design: TST-G18 revision 3 and approved implementation plan revision 1"
code-anchors:
  - "docker/iceberg-rest/machine_runtime.py (MachineOwner, MachineDocker)"
  - "docker/iceberg-rest/machine_operation.py (CommandOwner)"
  - "docker/iceberg-rest/entry_publication.py"
  - "docker/iceberg-rest/runtime_entry.py"
  - "docker/iceberg-rest/scratch.py"
  - "docker/iceberg-rest/fixture_run.py"
  - "tests/cluster-harness/src/fixture_run.rs"
  - "tests/sql/runner/library/src/fixture_context.rs"
  - "tests/test-support/src/monitor/main.rs"
---

## 问题

共享fixture按worktree输入创建多版本资源，导致项目、网络、端口和状态卷持续增长。如何在每个daemon只运行一组核心服务，同时保留明确的维护权限、benchmark数据、不可变消费者输入和真实writer退出责任？

## 裁决

机器owner按准确Docker daemon拥有固定 `nr-fixture-machine` 项目、固定网络及一组MinIO/REST/rest-mv/Spark/mc。控制目录是状态位置，不能换目录创建第二套；daemon上的authority卷保护唯一性。端口来自固定机器配置，冲突失败，不扫描替代值。HMS与私有catalog仍由自己的owner管理，不隐藏计入核心栈。

完整version由验证后的有效BOM、derived定义、实际image ID、模板、协议及服务配置产生，排除供给时间、路径和可变tag。activation generation区分实际切换。ADR-0155继续拥有联网供给与本地verify；普通worktree up只附着健康ready同版本机器，不能provision、启动、修复或升级。

维护者显式start/upgrade。升级先保存maintenance，停止共享REST/Spark写者，持久化新访问材料并轮换旧root、撤销run IAM/STS；确认旧root确实拒绝后，在准确MinIO在线时清理已分类scratch。整个MinIO卷和benchmark全部对象保留，REST状态卷按准确归属删除，再切换服务、验证资源/镜像/端点/健康，最后提交ready。普通attach不能越过提交点。失败保留准确意图和maintenance；真正未知的mutation不能凭单次不存在或新snapshot猜成功。

维护锁由原机器owner及短命命令监护者持有。owner消失后，监护者停止后续派发，并保持CLI leader未reap直至原进程组实际退出；mc还须确认准确临时容器消失。超时和发出KILL均不代替实际退出。明确的daemon端口终态拒绝仅能在同一保存维护意图、真实CLI组退出、准确source/target资源重新核验后允许重试，不直接ready。

worktree publication以完整文件集合的内容身份保存，落盘后只原子替换一次published。锁序worktree→machine，机器维护不拿worktree锁。消费者固定一次不可变env/config/manifest，机器version/generation随输入保存。SQLite等稳定数据不在publication中；离线prepare仅发布不可用入口。解绑保留旧publication，没有消费者租约、在线切换或按旧data_locations递归删除的权威。

每次共享运行在首次副作用前准入整次预算，默认3600秒、max-run86400、清理预留300、TTL604800。嵌套沿用完整run身份和原期限，工作期限只能收窄，不能借重启/重试/shell重置；清理使用原完整期限。超预算在零副作用点BLOCKED，已准入超时为FAIL。Rust角色进程、Docker CLI及容器内Spark/JVM均由原child owner监督真实退出，期限不能转交为产品控制协议。

实际writer启动显式做有界过期sweep，使用原预算与固定publication；通用预算准入和READY只读检查不做清扫。

scratch名称保留13位epoch毫秒及32位随机token，资源suffix最多5字符；S3对象在原worktree/run root内，REST实际namespace/table/view location须验证。IAM compact user由完整run/purpose确定推导，policy保留完整身份及准确scope。正常结束精确清理并保留错误；TTL sweep只回收可证明的遗留，不按worktree活性猜测。未知shape、歧义映射、权限和部分枚举都不能扩大删除权。自定义benchmark根、祖先/后代及全部对象先保护。

SQL的原CREATE及golden仍执行；runner在首个表/view写入前设置并GET核实scratch namespace location，再核实实际table位置，不增加产品SQL语法。可变Hadoop warehouse绑定run root；只读prepared dataset继续按ADR-0173 check准确READY与原warehouse，不自动供给或重绑定。

私有stock/publication-hook保留独立REST及其原生命周期。vended只共享准确MinIO，私有网络避免REST DNS alias冲突；真实STS双身份及原key/token/expiry语义保留。私有writer先退出，才清原run IAM/对象；未确认退出时保留失败登记。HMS只接入机器网络并保留自己的Derby卷；ADR-0169的HMS永久只读裁决不变。

## 否决的替代与接受的代价

普通up自动按当前checkout重建固定服务会中断未知消费者，因此维护必须显式。继续并存多版本共享栈不能约束资源增长，因此退役其locator/绑定/stop-delete管理协议。所有测试改私有栈会丢失共享并发义务，因此只保留已明确需要的私有profile。持有者租约、统一reaper和自动历史删除引入另一个活性/数据权威，当前不采用。

升级会使旧消费者失败，scratch不能靠git回滚；没有透明迁移。benchmark与整个MinIO保留卷须以真实对象清单及内容校验核对，READY标记本身不够。旧项目/卷不自动认领或删除，首次迁移要有准确保留清单和维护窗口。新机器若使用空卷，需要显式供给所需READY；输入缺失不改称测试成功。

## 验证与重新评估

定向fake/backend、真实child进程、壳层与Rust测试分别保护协议、期限、取消与发布。机器升级、TTL/S3/IAM、旧凭证撤销、Spark/JVM真实退出、同/跨worktree两个原生1FE+3BE并发以及约定SQL/System/benchmark完整正确性，须在授权环境另有真实执行证据；文档、mock或Compose parse不替代它们。

若需要不中断在线消费者的升级、跨机器runtime、持有者租约或历史卷自动迁移，重新讨论所有权与失败语义。不能把自动fallback、延长期限或扩大清扫范围作为局部修复。
