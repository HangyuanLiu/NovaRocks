# P06 SDK listing 公开接口边界待裁决（2026-10-07）

本记录是当前代码与SDK公开API的只读证据，不改变provider能力或accepted spec。
Hadoop/Paimon可在既有provider/FileIO接缝中限制自有String/Vec与SDK全量收集；后续主agent
已完成定向和共享接口验证。下表中的REST/Hive调用不能仅用事后collector证明事前构造界。

| 接口 | 当前SDK实际行为 / 公开接缝 | 产品影响 |
|---|---|---|
| REST list namespaces | vendor/iceberg-catalog-rest-0.9.0/src/catalog.rs:1477-1506 连续分页并extend完整Vec；client.rs:285-287完整response.bytes再serde；RestCatalogBuilder::with_client只接reqwest::Client，无namespace单页/response body bound hook | system_catalog_facts.rs:141/152全external snapshot；mv/domain/lake_rebuild.rs:423 discovery；SHOW databases等 |
| REST list views | 同SDKcatalog.rs:2150-2170全分页收集；同body路径 | view/engine.rs:309 SHOW VIEWS；catalog_application/statement.rs:602 FORCE DROP DATABASE在删除前必需完整view列表 |
| HMS namespaces/tables | vendor/iceberg-catalog-hms-0.9.0/src/catalog.rs:250/435 无数量参数的get_all_*；201-209私有Thrift client；公开builder仅framed/buffered，无可注入受限decoder | external system facts、MV discovery、SHOW/database DDL |
| Hive/Hadoop views | vendor/iceberg-0.9.0/src/catalog/mod.rs:159-163默认FeatureUnsupported，实际上没有SDK全量view枚举 | coverage历史分类应修正，不能伪造空列表 |

catalog_control/views.rs:335禁止把Unsupported转换为empty list。无界listing拒绝会使对应发现
不完整；lake rebuild应保留Unavailable语义，不能当成全部MV消失。精确namespace_exists和
named table load有独立SDK入口，不能把listing拒绝误写为所有SELECT能力退休。

v6计划P06约束不修改第三方SDK，使用公开参数或Nova owner调用前后界；spec§5.6同时要求
SDK反序列化与source增长事前有界。上述公开API没有所需接缝，简单timeout、HTTP/2 frame/header
大小、事后name/Vec collector均不能替代body和全列表峰值。直接禁用将改变既有产品能力。

需明确取舍之一：取得/增加SDK公开有界listing与response接缝并调整SDK修改约束；接受对相关
listing能力的事前拒绝及产品限制；或者明确第三方SDK内部列表例外并缩小M07源头硬界声明。
本记录不选择、不实施任何一种，也不宣称P06/整个M07完成。REST tables的pageSize兼容修复保持。

OpenDAL公开lister limit只是每次请求的page limit；忽略limit的服务端仍可能返回更大body。
初始Paimon provider stream gate未覆盖更早FS URI format；后续
[FS / Paimon source接入](p06-fs-paimon-source.md)补齐URI与schema HEAD。底层page/XML反序列化
仍未闭合。公开HttpFetch/HttpClient接缝若用于此缺口，需独立body/shape预检证据，
不扩ADR-0138 vendor patch。

2026-10-08验证补充：FS144 / Paimon55 / Iceberg1176 / Frontend1436的相关定向全库通过；
`543759140` cargo-only CI 12232 PASS。此证据验证已有实现兼容，不补齐以上 SDK 源头接缝，
不改变待裁决状态。FE membership incoming数量门的独立实现缺口另见
[P00b只读审查](p00b-membership-ingress-gap.md)。
