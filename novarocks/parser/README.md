# novarocks-parser

`novarocks-parser` 是 NovaRocks SQL 语言事实的唯一 owner。它只负责 token、Span、AST、词法/语法/纯结构
校验错误、canonical printer 与 parser 入口；所有位置均以原始 SQL 的 UTF-8 byte offset 表示，并派生为
1-based byte column。

该 crate 不得放入 catalog、session、统计信息、connector、协议映射或任何执行/规划语义。这些事实分别属于
analyzer、frontend 或 provider owner。它也不接管现有生产 SQL 路径；生产 family 的迁移从 SQLP-3 开始。

依赖契约是刻意严格的：它唯一依赖 `novarocks-user-error`。parser-domain 的
`LexError`、`ParseError`、`ValidateError` 经 `ParserError::to_user_error` 在边界转换为用户错误；code 与
phase 只能来自 parser 导出的 descriptor。跨 domain 的 descriptor 聚合、manifest 与生命周期 ledger 由独立的
`tools/error-manifest` package 负责，绝不能反向放入本 crate 或 `novarocks-user-error`。
