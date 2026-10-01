# P04 原始 Arrow owner 的窄容量接口

本检查点只补齐 borrowed 容量访问接口，没有实现 Native producer，也没有发布 root 输入证明。P04 继续执行。

- 标准 Buffer 返回实际 Standard deallocation Layout 的完整 size，包括 slice 未显示的 backing；Custom allocation 返回 None，需要来源 owner 的单独证明。原 capacity() 行为保持。
- RecordBatch / StructArray 读取原始列 Vec 的 capacity，不克隆数组或容器。
- Field::clone_with_metadata 直接接收调用方的替换 map，保留名称、类型、nullability 和 dictionary IPC 属性，不先复制未知原始 map 的分配历史。它本身不颁发任何 metadata backing 证明。
- 三份 vendor 源于 Cargo.lock 原先固定的本地 registry 58.2.0 副本；PATCH.md 记录每个窄增量，不修改既有行为。

六个真实 allocator / owner / spare-capacity 探针通过；workspace all-target offline locked check、renderer 28、root sink 10、original-carrier 2 均通过。曾尝试直接运行非 workspace Arrow vendor 的 lib test，Cargo 明确拒绝其 dev-dependencies；失败日志保留，测试改由 Execution integration probe 覆盖，没有下载依赖。

任意 HashMap.capacity() 仍不是带删除历史的完整 backing 证明；下一步在 source owner 构造点建立受控 metadata 和 origin receipt。最终原始 Chunk、nested array metadata、标准 carrier 闭集和 Custom backing 的输入 oracle 尚待完成；此记录不表示产品验收或 MEM allocation 计费。
