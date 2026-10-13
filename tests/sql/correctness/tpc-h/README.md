<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

# TPC-H 数据正确性

本 suite 在正常 SQL correctness runner 中执行完整查询及现有独立 expected；不是性能实验。`suite.toml` 声明 `explicit_only = true` 与 `tpc-h` / scale `1` 的 prepared dataset。查询数量与选择只以当前 suite manifest 和实际 discovery 为准。

输入由既有 Spark/REST/MinIO producer 显式准备，消费者只 check READY，核对准确 dataset key、manifest、warehouse与publication identity/ETag。缺输入或校验失败非零退出，不自动 ensure。详细准备、失败材料和cleanup步骤见 [SQL入口](../../README.md)。

```bash
cargo run --locked --profile dev-opt --manifest-path tests/sql/runner/Cargo.toml --bin novarocks-sql-test -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" --suite tpc-h --mode verify \
  --cluster-mode cross-process --cluster-size 3 -j 1 --cluster-launch-profile performance
```

命令从仓库根运行，`NOVAROCKS_BIN` 指向本次实际构建；`performance` 仅为clean启动配置。suite init建立正常catalog并访问已准备表，全部case依次比对结果，suite cleanup与FE/BE实际shutdown由原生owner执行。语句错误、mismatch与cleanup错误保留raw FAIL和材料，不重录expected或降低断言。

维护owner为SQL correctness领域；成本为共享全量数据和独立1FE+3BE。runner/输入绑定修改必跑三套数据集suite；相关planner/operator语义变更选择本suite；每周完整轮次由维护者手工执行，自动调度尚未交付。当前文档不是本次或P09通过收据。
