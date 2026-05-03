use sqlparser::keywords::Keyword;
use sqlparser::parser::Parser;
use sqlparser::tokenizer::Token;

use super::{convert_object_name, convert_sql_type, peek_word_eq};
use crate::engine::catalog::normalize_identifier;
use crate::sql::parser::ast::{
    ColumnAggregation, CreateTableKind, CreateTableStmt, IcebergPartitionFieldExpr, SqlType,
    TableColumnDef, TableKeyDesc, TableKeyKind,
};

/// Parse StarRocks CREATE TABLE statement:
/// CREATE TABLE [IF NOT EXISTS] <name> (
///   col1 type [NOT NULL] [DEFAULT ...] [COMMENT '...'],
///   ...
/// )
/// [ENGINE = OLAP|...]
/// [key_desc]
/// [COMMENT '...']
/// [PARTITION BY ...]
/// [DISTRIBUTED BY HASH(...) [BUCKETS n]]
/// [PROPERTIES (...)]
/// [TBLPROPERTIES (...)]
pub(crate) fn parse_create_table_statement(
    parser: &mut Parser<'_>,
) -> Result<CreateTableStmt, String> {
    parser
        .expect_keyword(Keyword::CREATE)
        .map_err(|e| e.to_string())?;
    // Skip EXTERNAL / TEMPORARY
    let _ = parser.parse_keyword(Keyword::EXTERNAL);
    let _ = parser.parse_keyword(Keyword::TEMPORARY);
    parser
        .expect_keyword(Keyword::TABLE)
        .map_err(|e| e.to_string())?;

    // IF NOT EXISTS
    let _if_not_exists = parser.parse_keywords(&[Keyword::IF, Keyword::NOT, Keyword::EXISTS]);

    let name = convert_object_name(parser.parse_object_name(false).map_err(|e| e.to_string())?)?;

    // Parse column definitions
    parser
        .expect_token(&Token::LParen)
        .map_err(|e| e.to_string())?;
    let columns = parse_column_definitions(parser)?;

    // Parse trailing clauses: ENGINE, KEY type, COMMENT, PARTITION, DISTRIBUTED, ORDER BY, PROPERTIES
    let mut _engine = None;
    let mut key_desc = None;
    let mut bucket_count = None;
    let mut partition_fields = Vec::new();
    let mut parsed_iceberg_partition_clause = false;
    let mut properties = Vec::new();

    // Consume all remaining clauses until EOF or semicolon
    loop {
        if parser.peek_token_ref().token == Token::EOF
            || parser.peek_token_ref().token == Token::SemiColon
        {
            break;
        }
        if peek_word_eq(parser, 0, "ENGINE") {
            parser.next_token(); // ENGINE
            let _ = parser.consume_token(&Token::Eq);
            let eng_name = parser
                .parse_identifier()
                .map_err(|e| e.to_string())?
                .value
                .to_lowercase();
            _engine = Some(eng_name);
        } else if peek_word_eq(parser, 0, "DUPLICATE") {
            key_desc = Some(parse_key_desc(parser, TableKeyKind::Duplicate)?);
        } else if peek_word_eq(parser, 0, "AGGREGATE") {
            key_desc = Some(parse_key_desc(parser, TableKeyKind::Aggregate)?);
        } else if peek_word_eq(parser, 0, "UNIQUE") {
            key_desc = Some(parse_key_desc(parser, TableKeyKind::Unique)?);
        } else if peek_word_eq(parser, 0, "PRIMARY") {
            key_desc = Some(parse_key_desc(parser, TableKeyKind::Primary)?);
        } else if peek_word_eq(parser, 0, "COMMENT") {
            parser.next_token(); // COMMENT
            parser.next_token(); // string
        } else if peek_word_eq(parser, 0, "PARTITION") {
            if is_legacy_partition_clause(parser) {
                skip_until_keyword_or_eof(
                    parser,
                    &["DISTRIBUTED", "ORDER", "PROPERTIES", "TBLPROPERTIES"],
                );
            } else {
                partition_fields = parse_partition_by_clause(parser)?;
                parsed_iceberg_partition_clause = true;
                ensure_partition_clause_boundary(parser)?;
            }
        } else if peek_word_eq(parser, 0, "DISTRIBUTED") {
            bucket_count = if parsed_iceberg_partition_clause {
                parse_bucket_count_strict(parser)?
            } else {
                parse_bucket_count(parser)?
            };
        } else if parser.parse_keyword(Keyword::ORDER) {
            // ORDER BY (...)
            let _ = parser.parse_keyword(Keyword::BY);
            skip_parenthesized(parser);
        } else if peek_word_eq(parser, 0, "PROPERTIES") || peek_word_eq(parser, 0, "TBLPROPERTIES")
        {
            parser.next_token(); // PROPERTIES / TBLPROPERTIES
            properties = parse_kv_properties_vec(parser)?;
        } else {
            if parsed_iceberg_partition_clause {
                return Err(format!(
                    "unexpected token after PARTITION BY clause: {}",
                    parser.peek_token_ref().token
                ));
            }
            // Skip unknown token
            parser.next_token();
        }
    }

    if parsed_iceberg_partition_clause {
        ensure_partitioned_create_table_end(parser)?;
    }

    let kind = CreateTableKind::Iceberg {
        columns,
        key_desc,
        bucket_count,
        partition_fields,
        properties,
    };

    Ok(CreateTableStmt { name, kind })
}

fn is_legacy_partition_clause(parser: &Parser<'_>) -> bool {
    if !peek_word_eq(parser, 0, "PARTITION") || !peek_word_eq(parser, 1, "BY") {
        return false;
    }
    let first_field_offset = if parser.peek_nth_token_ref(2).token == Token::LParen {
        3
    } else {
        2
    };
    let next_offset = first_field_offset + 1;
    matches!(
        parser.peek_nth_token_ref(first_field_offset).token,
        Token::Word(ref word)
            if word.quote_style.is_none()
                && parser.peek_nth_token_ref(next_offset).token == Token::LParen
                && matches!(
                    word.value.to_ascii_lowercase().as_str(),
                    "range" | "list" | "date_trunc" | "time_slice"
                )
    )
}

fn ensure_partition_clause_boundary(parser: &mut Parser<'_>) -> Result<(), String> {
    if parser.peek_token_ref().token == Token::EOF
        || parser.peek_token_ref().token == Token::SemiColon
        || peek_word_eq(parser, 0, "DISTRIBUTED")
        || peek_word_eq(parser, 0, "ORDER")
        || peek_word_eq(parser, 0, "PROPERTIES")
        || peek_word_eq(parser, 0, "TBLPROPERTIES")
    {
        return Ok(());
    }
    Err(format!(
        "unexpected token after PARTITION BY clause: {}",
        parser.peek_token_ref().token
    ))
}

fn ensure_partitioned_create_table_end(parser: &mut Parser<'_>) -> Result<(), String> {
    if parser.consume_token(&Token::SemiColon) {
        return match parser.peek_token_ref().token {
            Token::EOF => Ok(()),
            ref other => Err(format!(
                "unexpected token after PARTITION BY clause: {other}"
            )),
        };
    }
    match parser.peek_token_ref().token {
        Token::EOF => Ok(()),
        ref other => Err(format!(
            "unexpected token after PARTITION BY clause: {other}"
        )),
    }
}

fn parse_partition_by_clause(
    parser: &mut Parser<'_>,
) -> Result<Vec<IcebergPartitionFieldExpr>, String> {
    parser.next_token(); // PARTITION
    parser
        .expect_keyword(Keyword::BY)
        .map_err(|e| format!("expected BY after PARTITION: {e}"))?;

    if parser.consume_token(&Token::LParen) {
        let mut fields = Vec::new();
        loop {
            if parser.consume_token(&Token::RParen) {
                break;
            }
            if !fields.is_empty() {
                parser
                    .expect_token(&Token::Comma)
                    .map_err(|e| format!("expected , in PARTITION BY: {e}"))?;
            }
            fields.push(parse_partition_field_expr(parser)?);
            if parser.consume_token(&Token::RParen) {
                break;
            }
        }
        if fields.is_empty() {
            return Err("PARTITION BY requires at least one field".to_string());
        }
        return Ok(fields);
    }

    let first = parse_partition_field_expr(parser)?;
    let mut fields = vec![first];
    while parser.consume_token(&Token::Comma) {
        fields.push(parse_partition_field_expr(parser)?);
    }
    Ok(fields)
}

pub(crate) fn parse_partition_field_expr(
    parser: &mut Parser<'_>,
) -> Result<IcebergPartitionFieldExpr, String> {
    let name = parser
        .parse_identifier()
        .map_err(|e| format!("expected partition field column or transform: {e}"))?
        .value;

    if !parser.consume_token(&Token::LParen) {
        return Ok(IcebergPartitionFieldExpr::Identity {
            column: normalize_identifier(&name)?,
        });
    }

    let transform = name.to_ascii_lowercase();
    let column = parser
        .parse_identifier()
        .map_err(|e| format!("expected column argument for partition transform `{name}`: {e}"))?
        .value;
    let column = normalize_identifier(&column)?;

    let field = match transform.as_str() {
        "year" => {
            parser
                .expect_token(&Token::RParen)
                .map_err(|e| format!("expected ) after year argument: {e}"))?;
            IcebergPartitionFieldExpr::Year { column }
        }
        "month" => {
            parser
                .expect_token(&Token::RParen)
                .map_err(|e| format!("expected ) after month argument: {e}"))?;
            IcebergPartitionFieldExpr::Month { column }
        }
        "day" => {
            parser
                .expect_token(&Token::RParen)
                .map_err(|e| format!("expected ) after day argument: {e}"))?;
            IcebergPartitionFieldExpr::Day { column }
        }
        "hour" => {
            parser
                .expect_token(&Token::RParen)
                .map_err(|e| format!("expected ) after hour argument: {e}"))?;
            IcebergPartitionFieldExpr::Hour { column }
        }
        "void" => {
            parser
                .expect_token(&Token::RParen)
                .map_err(|e| format!("expected ) after void argument: {e}"))?;
            IcebergPartitionFieldExpr::Void { column }
        }
        "bucket" => {
            parser
                .expect_token(&Token::Comma)
                .map_err(|e| format!("expected bucket column and bucket count: {e}"))?;
            let num_buckets = parse_positive_u32(parser, "bucket count")?;
            parser
                .expect_token(&Token::RParen)
                .map_err(|e| format!("expected ) after bucket arguments: {e}"))?;
            IcebergPartitionFieldExpr::Bucket {
                column,
                num_buckets,
            }
        }
        "truncate" => {
            parser
                .expect_token(&Token::Comma)
                .map_err(|e| format!("expected truncate column and width: {e}"))?;
            let width = parse_positive_u32(parser, "truncate width")?;
            parser
                .expect_token(&Token::RParen)
                .map_err(|e| format!("expected ) after truncate arguments: {e}"))?;
            IcebergPartitionFieldExpr::Truncate { column, width }
        }
        _ => {
            return Err(format!("unsupported Iceberg partition transform `{name}`"));
        }
    };

    Ok(field)
}

fn parse_positive_u32(parser: &mut Parser<'_>, label: &str) -> Result<u32, String> {
    let token = parser.next_token();
    match token.token {
        Token::Number(value, _) => {
            let parsed = value
                .parse::<u32>()
                .map_err(|e| format!("invalid {label} `{value}`: {e}"))?;
            if parsed == 0 {
                return Err(format!("{label} must be positive"));
            }
            Ok(parsed)
        }
        other => Err(format!("expected numeric {label}, got {other}")),
    }
}

fn parse_bucket_count(parser: &mut Parser<'_>) -> Result<Option<u32>, String> {
    parser.next_token(); // DISTRIBUTED
    loop {
        if parser.peek_token_ref().token == Token::EOF
            || parser.peek_token_ref().token == Token::SemiColon
            || peek_word_eq(parser, 0, "ORDER")
            || peek_word_eq(parser, 0, "PROPERTIES")
            || peek_word_eq(parser, 0, "TBLPROPERTIES")
        {
            return Ok(None);
        }
        if peek_word_eq(parser, 0, "BUCKETS") {
            parser.next_token(); // BUCKETS
            let token = parser.next_token();
            return match token.token {
                Token::Number(value, _) => {
                    Ok(Some(value.parse::<u32>().map_err(|e| {
                        format!("invalid BUCKETS value `{value}`: {e}")
                    })?))
                }
                other => Err(format!("expected numeric BUCKETS value, got {other}")),
            };
        }
        if parser.peek_token_ref().token == Token::LParen {
            skip_parenthesized(parser);
        } else {
            parser.next_token();
        }
    }
}

fn parse_bucket_count_strict(parser: &mut Parser<'_>) -> Result<Option<u32>, String> {
    parser.next_token(); // DISTRIBUTED
    parser
        .expect_keyword(Keyword::BY)
        .map_err(|e| format!("expected BY after DISTRIBUTED: {e}"))?;
    expect_word(parser, "HASH")?;
    skip_parenthesized(parser);

    let bucket_count = if peek_word_eq(parser, 0, "BUCKETS") {
        parser.next_token(); // BUCKETS
        let token = parser.next_token();
        match token.token {
            Token::Number(value, _) => Some(
                value
                    .parse::<u32>()
                    .map_err(|e| format!("invalid BUCKETS value `{value}`: {e}"))?,
            ),
            other => return Err(format!("expected numeric BUCKETS value, got {other}")),
        }
    } else {
        None
    };

    if parser.peek_token_ref().token == Token::EOF
        || parser.peek_token_ref().token == Token::SemiColon
        || peek_word_eq(parser, 0, "ORDER")
        || peek_word_eq(parser, 0, "PROPERTIES")
        || peek_word_eq(parser, 0, "TBLPROPERTIES")
    {
        return Ok(bucket_count);
    }
    Err(format!(
        "unexpected token after DISTRIBUTED clause: {}",
        parser.peek_token_ref().token
    ))
}

fn expect_word(parser: &mut Parser<'_>, word: &str) -> Result<(), String> {
    let token = parser.next_token();
    match token.token {
        Token::Word(token_word) if token_word.value.eq_ignore_ascii_case(word) => Ok(()),
        other => Err(format!("expected {word}, got {other}")),
    }
}

fn parse_column_definitions(parser: &mut Parser<'_>) -> Result<Vec<TableColumnDef>, String> {
    let mut columns = Vec::new();
    loop {
        if parser.consume_token(&Token::RParen) {
            break;
        }
        if !columns.is_empty() {
            let _ = parser.consume_token(&Token::Comma);
            if parser.consume_token(&Token::RParen) {
                break;
            }
        }
        let col_name = parser.parse_identifier().map_err(|e| e.to_string())?.value;
        let sql_type = parse_sql_type_definition(parser)?;

        let mut aggregation = None;
        let mut nullable = true;
        let mut _comment = None;

        // Parse optional NOT NULL, NULL, DEFAULT, COMMENT, AUTO_INCREMENT, etc.
        loop {
            if aggregation.is_none()
                && let Some(parsed) = parse_column_aggregation(parser)
            {
                aggregation = Some(parsed);
            } else if parser.parse_keywords(&[Keyword::NOT, Keyword::NULL]) {
                nullable = false;
            } else if parser.parse_keyword(Keyword::NULL) {
                nullable = true;
            } else if parser.parse_keyword(Keyword::DEFAULT) {
                // Skip the default value expression
                skip_default_value(parser);
            } else if peek_word_eq(parser, 0, "COMMENT") {
                parser.next_token();
                let tok = parser.next_token();
                if let Token::SingleQuotedString(s) | Token::DoubleQuotedString(s) = tok.token {
                    _comment = Some(s);
                }
            } else if peek_word_eq(parser, 0, "AUTO_INCREMENT") {
                parser.next_token();
            } else if peek_word_eq(parser, 0, "AS") {
                // Generated column
                parser.next_token();
                skip_default_value(parser);
            } else {
                break;
            }
        }

        columns.push(TableColumnDef {
            name: col_name,
            data_type: sql_type,
            nullable,
            aggregation,
        });
    }
    Ok(columns)
}

fn parse_column_aggregation(parser: &mut Parser<'_>) -> Option<ColumnAggregation> {
    let aggregation = if peek_word_eq(parser, 0, "SUM") {
        Some(ColumnAggregation::Sum)
    } else if peek_word_eq(parser, 0, "MIN") {
        Some(ColumnAggregation::Min)
    } else if peek_word_eq(parser, 0, "MAX") {
        Some(ColumnAggregation::Max)
    } else if peek_word_eq(parser, 0, "REPLACE") {
        Some(ColumnAggregation::Replace)
    } else {
        None
    }?;
    parser.next_token();
    Some(aggregation)
}

fn parse_sql_type_definition(parser: &mut Parser<'_>) -> Result<SqlType, String> {
    if peek_word_eq(parser, 0, "ARRAY") {
        parse_array_sql_type(parser)
    } else if peek_word_eq(parser, 0, "MAP") {
        parse_map_sql_type(parser)
    } else if peek_word_eq(parser, 0, "STRUCT") {
        parse_struct_sql_type(parser)
    } else {
        let data_type = parser.parse_data_type().map_err(|e| e.to_string())?;
        convert_sql_type(data_type)
    }
}

fn parse_array_sql_type(parser: &mut Parser<'_>) -> Result<SqlType, String> {
    parser.next_token(); // ARRAY
    if parser.consume_token(&Token::Lt) {
        let element_type = parse_sql_type_definition(parser)?;
        parser.expect_token(&Token::Gt).map_err(|e| e.to_string())?;
        Ok(SqlType::Array(Box::new(element_type)))
    } else {
        convert_sql_type(sqlparser::ast::DataType::Array(
            sqlparser::ast::ArrayElemTypeDef::AngleBracket(Box::new(
                parser.parse_data_type().map_err(|e| e.to_string())?,
            )),
        ))
    }
}

fn parse_map_sql_type(parser: &mut Parser<'_>) -> Result<SqlType, String> {
    parser.next_token(); // MAP
    parser.expect_token(&Token::Lt).map_err(|e| e.to_string())?;
    let key_type = parse_sql_type_definition(parser)?;
    parser
        .expect_token(&Token::Comma)
        .map_err(|e| e.to_string())?;
    let value_type = parse_sql_type_definition(parser)?;
    parser.expect_token(&Token::Gt).map_err(|e| e.to_string())?;
    Ok(SqlType::Map(Box::new(key_type), Box::new(value_type)))
}

fn parse_struct_sql_type(parser: &mut Parser<'_>) -> Result<SqlType, String> {
    parser.next_token(); // STRUCT
    parser.expect_token(&Token::Lt).map_err(|e| e.to_string())?;
    let mut fields = Vec::new();
    loop {
        if parser.consume_token(&Token::Gt) {
            break;
        }
        if !fields.is_empty() {
            parser
                .expect_token(&Token::Comma)
                .map_err(|e| e.to_string())?;
        }
        let field_name = parser.parse_identifier().map_err(|e| e.to_string())?.value;
        let field_type = parse_sql_type_definition(parser)?;
        fields.push((field_name, field_type));
    }
    Ok(SqlType::Struct(fields))
}

fn parse_key_desc(parser: &mut Parser<'_>, kind: TableKeyKind) -> Result<TableKeyDesc, String> {
    parser.next_token(); // DUPLICATE/AGGREGATE/UNIQUE/PRIMARY
    parser
        .expect_keyword(Keyword::KEY)
        .map_err(|e| e.to_string())?;
    parser
        .expect_token(&Token::LParen)
        .map_err(|e| e.to_string())?;
    let mut key_columns = Vec::new();
    loop {
        if parser.consume_token(&Token::RParen) {
            break;
        }
        if !key_columns.is_empty() {
            let _ = parser.consume_token(&Token::Comma);
            if parser.consume_token(&Token::RParen) {
                break;
            }
        }
        let col = parser.parse_identifier().map_err(|e| e.to_string())?.value;
        key_columns.push(col);
    }
    Ok(TableKeyDesc {
        kind,
        columns: key_columns,
    })
}

fn parse_kv_properties_vec(parser: &mut Parser<'_>) -> Result<Vec<(String, String)>, String> {
    let mut props = Vec::new();
    if !parser.consume_token(&Token::LParen) {
        return Ok(props);
    }
    loop {
        if parser.consume_token(&Token::RParen) {
            break;
        }
        if !props.is_empty() {
            let _ = parser.consume_token(&Token::Comma);
            if parser.consume_token(&Token::RParen) {
                break;
            }
        }
        let key = parse_string_or_ident(parser)?;
        let _ = parser.consume_token(&Token::Eq);
        let value = parse_string_or_ident(parser)?;
        props.push((key, value));
    }
    Ok(props)
}

fn parse_string_or_ident(parser: &mut Parser<'_>) -> Result<String, String> {
    let token = parser.next_token();
    match token.token {
        Token::SingleQuotedString(s) | Token::DoubleQuotedString(s) => Ok(s),
        Token::Word(w) => Ok(w.value),
        Token::Number(n, _) => Ok(n),
        other => Err(format!("expected string or identifier, got {other}")),
    }
}

fn skip_until_keyword_or_eof(parser: &mut Parser<'_>, stop_words: &[&str]) {
    loop {
        if parser.peek_token_ref().token == Token::EOF
            || parser.peek_token_ref().token == Token::SemiColon
        {
            break;
        }
        let should_stop = stop_words.iter().any(|w| peek_word_eq(parser, 0, w));
        if should_stop {
            break;
        }
        // Handle parenthesized groups
        if parser.peek_token_ref().token == Token::LParen {
            skip_parenthesized(parser);
        } else {
            parser.next_token();
        }
    }
}

fn skip_parenthesized(parser: &mut Parser<'_>) {
    if !parser.consume_token(&Token::LParen) {
        return;
    }
    let mut depth = 1;
    loop {
        let tok = parser.next_token();
        match tok.token {
            Token::LParen => depth += 1,
            Token::RParen => {
                depth -= 1;
                if depth == 0 {
                    break;
                }
            }
            Token::EOF => break,
            _ => {}
        }
    }
}

fn skip_default_value(parser: &mut Parser<'_>) {
    // Skip until we hit a comma, RParen, or a known keyword
    let mut depth = 0;
    loop {
        match parser.peek_token_ref().token {
            Token::EOF | Token::SemiColon => break,
            Token::Comma | Token::RParen if depth == 0 => break,
            Token::LParen => {
                depth += 1;
                parser.next_token();
            }
            Token::RParen => {
                depth -= 1;
                parser.next_token();
            }
            _ => {
                if depth == 0
                    && (peek_word_eq(parser, 0, "COMMENT")
                        || peek_word_eq(parser, 0, "NOT")
                        || peek_word_eq(parser, 0, "NULL")
                        || peek_word_eq(parser, 0, "AUTO_INCREMENT"))
                {
                    break;
                }
                parser.next_token();
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use sqlparser::parser::Parser;

    use super::parse_create_table_statement;
    use crate::sql::parser::ast::{CreateTableKind, IcebergPartitionFieldExpr};
    use crate::sql::parser::dialect::StarRocksDialect;

    #[test]
    fn parse_create_table_accepts_map_and_struct_columns() {
        let sql = r#"
            CREATE TABLE t1 (
                c12 map<varchar(5), double>,
                c13 struct<a bigint, b string>
            )
            DUPLICATE KEY(c12)
            DISTRIBUTED BY HASH(c12) BUCKETS 3
            PROPERTIES ("replication_num" = "1")
        "#;

        let mut parser = sqlparser::parser::Parser::new(&StarRocksDialect)
            .try_with_sql(sql)
            .expect("build parser");
        let stmt = parse_create_table_statement(&mut parser);
        assert!(stmt.is_ok(), "expected complex type DDL to parse: {stmt:?}");
    }

    #[test]
    fn parse_create_table_accepts_nested_array_complex_columns() {
        let sql = r#"
            CREATE TABLE t1 (
                c1 array<array<int>>,
                c2 array<map<string, int>>,
                c3 array<struct<f1 int, f2 string>>
            )
            DUPLICATE KEY(c1)
            DISTRIBUTED BY HASH(c1) BUCKETS 3
            PROPERTIES ("replication_num" = "1")
        "#;

        let normalized = crate::sql::parser::dialect::normalize_for_raw_parse(sql)
            .expect("normalize should succeed");
        let mut parser = sqlparser::parser::Parser::new(&StarRocksDialect)
            .try_with_sql(&normalized)
            .expect("build parser");
        let stmt = parse_create_table_statement(&mut parser);
        assert!(
            stmt.is_ok(),
            "expected nested complex type DDL to parse: {stmt:?}"
        );
    }

    #[test]
    fn create_table_parser_preserves_bucket_count() {
        let dialect = StarRocksDialect;
        let mut parser = Parser::new(&dialect)
            .try_with_sql(
                "create table tbl (id int) duplicate key(id) distributed by hash(id) buckets 3",
            )
            .expect("parser");
        let stmt = parse_create_table_statement(&mut parser).expect("create table stmt");
        let CreateTableKind::Iceberg { bucket_count, .. } = stmt.kind;
        assert_eq!(bucket_count, Some(3));
    }

    #[test]
    fn create_table_parser_preserves_column_nullability() {
        let dialect = StarRocksDialect;
        let mut parser = Parser::new(&dialect)
            .try_with_sql(
                "create table tbl (id int not null, note string null) duplicate key(id) distributed by hash(id) buckets 3",
            )
            .expect("parser");
        let stmt = parse_create_table_statement(&mut parser).expect("create table stmt");
        let CreateTableKind::Iceberg { columns, .. } = stmt.kind;
        assert_eq!(columns.len(), 2);
        assert!(!columns[0].nullable);
        assert!(columns[1].nullable);
    }

    #[test]
    fn create_table_parser_preserves_tblproperties() {
        let dialect = StarRocksDialect;
        let mut parser = Parser::new(&dialect)
            .try_with_sql(
                r#"create table tbl (id bigint) partition by(id) tblproperties("format-version"="3","write.row-lineage"="true")"#,
            )
            .expect("parser");
        let stmt = parse_create_table_statement(&mut parser).expect("create table stmt");
        let CreateTableKind::Iceberg { properties, .. } = stmt.kind;
        assert_eq!(
            properties,
            vec![
                ("format-version".to_string(), "3".to_string()),
                ("write.row-lineage".to_string(), "true".to_string()),
            ]
        );
    }

    #[test]
    fn create_table_parser_preserves_partition_transforms() {
        let dialect = StarRocksDialect;
        let mut parser = Parser::new(&dialect)
            .try_with_sql(
                "create table tbl (id bigint, ts datetime, name string) \
                 partition by (month(ts), bucket(id, 16), truncate(name, 8))",
            )
            .expect("parser");
        let stmt = parse_create_table_statement(&mut parser).expect("create table stmt");
        let CreateTableKind::Iceberg {
            partition_fields, ..
        } = stmt.kind;
        assert_eq!(
            partition_fields,
            vec![
                IcebergPartitionFieldExpr::Month {
                    column: "ts".to_string()
                },
                IcebergPartitionFieldExpr::Bucket {
                    column: "id".to_string(),
                    num_buckets: 16
                },
                IcebergPartitionFieldExpr::Truncate {
                    column: "name".to_string(),
                    width: 8
                },
            ]
        );
    }

    #[test]
    fn create_table_parser_rejects_invalid_partition_transform_args() {
        for sql in [
            "create table tbl (id bigint) partition by bucket(id, 0)",
            "create table tbl (name string) partition by truncate(name, 0)",
            "create table tbl (id bigint) partition by unknown(id)",
            "create table tbl (ts datetime) partition by month(date_trunc(ts))",
            "create table tbl (id bigint) partition by bucket(1, 16)",
            "create table tbl (id bigint) partition by ()",
        ] {
            let dialect = StarRocksDialect;
            let mut parser = Parser::new(&dialect).try_with_sql(sql).expect("parser");
            assert!(
                parse_create_table_statement(&mut parser).is_err(),
                "expected partition transform parse failure for {sql}"
            );
        }
    }

    #[test]
    fn create_table_parser_rejects_trailing_junk_after_partition_by() {
        for sql in [
            "create table tbl (id bigint) partition by id bogus",
            "create table tbl (id bigint) partition by bucket(id, 16) unknown_clause",
            "create table tbl (id bigint) partition by (id) bogus",
            "create table tbl (id bigint) partition by id; select 1",
            "create table tbl (id bigint) partition by id;;",
            r#"create table tbl (id bigint) partition by id tblproperties("format-version"="2"); select 1"#,
            r#"create table tbl (id bigint) partition by id tblproperties("format-version"="2");;"#,
            "create table tbl (id bigint) partition by id distributed by hash(id) buckets 3; select 1",
            r#"create table tbl (id bigint) partition by id tblproperties("format-version"="2") select 1"#,
            "create table tbl (id bigint) partition by id distributed by hash(id) buckets 3 bogus",
            "create table tbl (id bigint) partition by id distributed by hash(id) bogus",
        ] {
            let dialect = StarRocksDialect;
            let mut parser = Parser::new(&dialect).try_with_sql(sql).expect("parser");
            let err = parse_create_table_statement(&mut parser).expect_err("partition parse error");
            assert!(
                err.contains("unexpected token after PARTITION BY clause")
                    || err.contains("unexpected token after DISTRIBUTED clause"),
                "unexpected error for {sql}: {err}"
            );
        }
    }

    #[test]
    fn create_table_parser_accepts_single_final_semicolon_after_partition_by() {
        for sql in [
            "create table tbl (id bigint) partition by id;",
            r#"create table tbl (id bigint) partition by id tblproperties("format-version"="2");"#,
        ] {
            let dialect = StarRocksDialect;
            let mut parser = Parser::new(&dialect).try_with_sql(sql).expect("parser");
            let stmt = parse_create_table_statement(&mut parser).expect("create table stmt");
            let CreateTableKind::Iceberg {
                partition_fields, ..
            } = stmt.kind;
            assert_eq!(
                partition_fields,
                vec![IcebergPartitionFieldExpr::Identity {
                    column: "id".to_string()
                }]
            );
        }
    }

    #[test]
    fn create_table_parser_preserves_bucket_count_after_partition_by() {
        let dialect = StarRocksDialect;
        let mut parser = Parser::new(&dialect)
            .try_with_sql(
                "create table tbl (id bigint) partition by id distributed by hash(id) buckets 3",
            )
            .expect("parser");
        let stmt = parse_create_table_statement(&mut parser).expect("create table stmt");
        let CreateTableKind::Iceberg {
            partition_fields,
            bucket_count,
            ..
        } = stmt.kind;
        assert_eq!(
            partition_fields,
            vec![IcebergPartitionFieldExpr::Identity {
                column: "id".to_string()
            }]
        );
        assert_eq!(bucket_count, Some(3));
    }

    #[test]
    fn create_table_parser_keeps_tblproperties_after_identity_partition() {
        let dialect = StarRocksDialect;
        let mut parser = Parser::new(&dialect)
            .try_with_sql(
                r#"create table tbl (city string) partition by city tblproperties("format-version"="2")"#,
            )
            .expect("parser");
        let stmt = parse_create_table_statement(&mut parser).expect("create table stmt");
        let CreateTableKind::Iceberg {
            partition_fields,
            properties,
            ..
        } = stmt.kind;
        assert_eq!(
            partition_fields,
            vec![IcebergPartitionFieldExpr::Identity {
                column: "city".to_string()
            }]
        );
        assert_eq!(
            properties,
            vec![("format-version".to_string(), "2".to_string())]
        );
    }

    #[test]
    fn create_table_parser_treats_legacy_marker_names_as_identity_columns() {
        for sql in [
            "create table tbl (range bigint) partition by range",
            "create table tbl (`range` bigint) partition by `range`",
        ] {
            let dialect = StarRocksDialect;
            let mut parser = Parser::new(&dialect).try_with_sql(sql).expect("parser");
            let stmt = parse_create_table_statement(&mut parser).expect("create table stmt");
            let CreateTableKind::Iceberg {
                partition_fields, ..
            } = stmt.kind;
            assert_eq!(
                partition_fields,
                vec![IcebergPartitionFieldExpr::Identity {
                    column: "range".to_string()
                }]
            );
        }
    }

    #[test]
    fn create_table_parser_skips_legacy_range_partition_clause() {
        let dialect = StarRocksDialect;
        let mut parser = Parser::new(&dialect)
            .try_with_sql(
                r#"
                create table tbl (k1 int, v int)
                partition by range(k1) (
                    partition p1 values less than (10),
                    partition p2 values less than (20)
                )
                properties ("replication_num"="1")
                "#,
            )
            .expect("parser");
        let stmt = parse_create_table_statement(&mut parser).expect("create table stmt");
        let CreateTableKind::Iceberg {
            partition_fields,
            properties,
            ..
        } = stmt.kind;
        assert!(partition_fields.is_empty());
        assert_eq!(
            properties,
            vec![("replication_num".to_string(), "1".to_string())]
        );
    }

    #[test]
    fn create_table_parser_skips_legacy_list_and_function_partition_clauses() {
        for sql in [
            r#"
            create table tbl (k1 int, v int)
            partition by list(k1) (
                partition p1 values in ("a"),
                partition p2 values in ("b")
            )
            tblproperties ("format-version"="2")
            "#,
            r#"
            create table tbl (ts datetime, v int)
            partition by date_trunc("day", ts)
            properties ("replication_num"="1")
            "#,
        ] {
            let dialect = StarRocksDialect;
            let mut parser = Parser::new(&dialect).try_with_sql(sql).expect("parser");
            let stmt = parse_create_table_statement(&mut parser).expect("create table stmt");
            let CreateTableKind::Iceberg {
                partition_fields,
                properties,
                ..
            } = stmt.kind;
            assert!(partition_fields.is_empty());
            assert!(!properties.is_empty());
        }
    }
}
