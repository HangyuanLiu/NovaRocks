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

# docs/blog —— NovaRocks 技术博客 / 设计深挖(写作约定)

本目录收录 NovaRocks 的**对外**技术分享文章。动笔前先认清定位:

> 这里的文章**不是**内部文档、不是贡献者参考、不是 API 手册,而是**讲给外部读者听的「设计思路」文章**。
> 目标读者:想理解 NovaRocks **为什么这么设计**的人。

下面是给后续 agent 的写作约定,源自多轮真实评审的偏好,请严格遵循。

---

## 1. 选题:只写「成熟且稳定」的部分

- 只写**已经落地、短期不会大改**的设计。演进中 / 路线图上的东西**不写**。
- 一篇讲透一个主题。若是**总览型**文章,**不要锁死在某个具体算子**——具体算子的代数 / 细节留给后续**专题**文章。
  总览里可以拿某个算子当**例子**讲通用机制,但叙述重点始终是「机制本身」,不是那个算子。

## 2. 角度:讲「原理 + 为什么」,不讲「实现细节」

- 主线是**设计思路**:原理是什么、**为什么这么设计**、做了哪些**选择与权衡**。
- **不**逐行讲代码;**不**堆数据结构 / 文件路径 / 函数名 / 字段保留值等实现细节。
- 设计上的**选择和权衡要写**(它们正是「思路」);**实现层的语义边界 / 规范细节不要写**
  (例如「某操作底层被当成删+插」「某保留字段的具体数值」——这是文档/规范细节,对讲思路没意义)。

## 3. 必须有:具体 case + 数据的逐步变化

- 讲一个机制,**配一个可对照的具体 SQL case**,并**走一遍数据每一步怎么变**(用表格展示 before / 增量流 / after)。
- 抽象的「它会处理好」没有说服力;让读者**看见数据在每个阶段的样子**。
- 一个机制若有「多种情况」,举**最能体现其价值**的那种(例如 join 要举**两侧同时变化**的例子,而不是退化的单侧;
  能引出冲突 / 难点的 case 优先,比如 union 两分支聚出同一个 key 而相撞)。
- 必要时配图(见 §6)。

## 4. 结构与过渡:问题驱动,自然引出

- 开篇先立**问题与定位**(这件事难在哪、跟别的做法什么关系),再展开。
- 章节之间**顺着引出**:讲完 A,让下一个问题**自然冒出来**,再引入 B,不要生硬跳转。
  例:讲完「各算子各有各的 row id」→ 自然发问「那它们组合时 row id 听谁的?」→ 点出难点 → 给出机制。
- 用到读者可能不熟的底层概念(如 Iceberg 快照模型),**先补一小段背景**,让不熟悉的人也能跟上。
- 结尾**呼应主线**,用一句话收束全文的核心取舍。
- 历史只当**反面教材**:**不要**平铺直叙「以前有 N 个版本 / 后来改成 X」这类流水账;
  可以用「顺着某种直觉做会走不通」来**反衬难点的本质**,并对照例子解释。一切表述**以当前实现为准**。

## 5. 明确不要写(do-not)

- ❌ 历史实现流水账(「早期有 X 个变体」「后来改成 Y」)——以当前实现为准。
- ❌ 实现层语义边界 / 规范细节——对讲思路无意义。
- ❌ 单独的「能力边界 / 已支持 vs 路线图」章节,以及「友商横向对比」章节。
- ❌ 元说明 / 免责声明(「本文截至 X 月」「不给性能数字因为……」这类话术)。
- ❌ 编造或堆砌性能 / 延迟 / 吞吐数字。
- ❌ 范围限定话术(如「本文只覆盖 X 路径,另一条路径是别的机制」)——不必为非主线方向加 scoping。

## 6. 格式与图

- 中文;Markdown;每篇一个 `.md`,文件名用英文 kebab-case(便于发到博客平台)。
- 图用 **Mermaid**(GitHub 与多数平台可直接渲染)。
  **所有节点 / 决策节点的标签都要用双引号包起来**(`A["文本"]`、`D{"判定?"}`),
  否则标签里有括号 / 中文标点时不渲染。
- 数据演化优先用 **Markdown 表格**(最直观);流程、树结构用 Mermaid。

## 7. 写作流程(给 agent)

1. **先深度调研当前实现**(代码 + spec),确保技术准确;**主动核实**,别信过期的 memory / spec
   ——很多标记「未实施」的功能其实早已合并。
2. 先和用户对齐**大纲 + 配图构想**,再落笔(不要直接写全文)。
3. 落笔后自检:有没有混进 §5 里任何一条该删的东西。

---

## 现有文章

- [NovaRocks 是什么:从一个 mock BE,到 Iceberg-原生的 OLAP 引擎](what-is-novarocks.md)
  —— 总览 / 入口文章:项目是什么、起源(StarRocks BE 在 Mac 上难开发 → mock BE → 验证思路的平台)、三种部署模式、当前已是完整独立 OLAP 引擎,以及「Iceberg v3 强绑定 + IVM 核心特色」的定位。
- [用 Iceberg v3 实现增量物化视图的原理](incremental-materialized-views-on-iceberg-v3/incremental-materialized-views-on-iceberg-v3.md)
  —— **可作为风格范例**:问题引入(MV 为何难增量)→ Iceberg v3 三块地基 → `__change_op` 增量流
  → apply 到目标表(配 SQL case + 数据表)→ Delta / Version 算子(join 两侧都变的 case + 图)
  → 刷新属性框架(从「唯一行 id」问题切入,配 case + 综合树图)→ staging 分支原子发布(先补 Iceberg 背景)
  → 收尾落到价值:把 MV 也存成 Iceberg 表换来的红利(跨引擎兼容 + 明细/汇总统一入口)。
- [一条 SQL 把增量刷新算清楚:变更怎么落到 Iceberg 物化视图上](applying-incremental-changes-to-iceberg-materialized-views.md)
  —— 上文的**专题续作**,围着「变更怎么落到 MV 表」的**完整 SQL** 讲、逐 CTE 走数据:逻辑身份 vs 物理位置的桥 →
  join 两侧都变的**完整查询**(`两支 telescoping UNION ALL → GROUP BY 身份 SUM(±1) → HAVING net≠0 → LEFT JOIN 取 _file/_pos`),
  逐段数据表 + 反面推演瞬态行为何必须**写前净化** → 聚合的**完整查询**(`delta 带符号状态 → LEFT JOIN mv_state state_merge → 产 __change_op 流`),
  退场看**计数态**非可见值 + 快照钉定为何尤其致命 → 两条 SQL 收敛为同一骨架(+ 主干 Mermaid)→ 收尾:把刷新做成一条关系查询、与普通查询同源。
