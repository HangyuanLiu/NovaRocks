use crate::sql::parser::dialect::StarRocksDialect;
use sqlparser::parser::Parser;

/// Parse SQL into a raw sqlparser AST without converting to the custom AST.
/// This is used by the standalone ThriftPlanBuilder which works directly
/// with sqlparser types to avoid the limitations of the custom AST.
pub(crate) fn parse_sql_raw(sql: &str) -> Result<sqlparser::ast::Statement, String> {
    let normalized = crate::sql::parser::dialect::normalize_for_raw_parse(sql)?;
    parse_normalized_sql_raw(&normalized)
}

pub(crate) fn parse_normalized_sql_raw(sql: &str) -> Result<sqlparser::ast::Statement, String> {
    let dialect = StarRocksDialect;
    let mut parser = Parser::new(&dialect)
        .try_with_sql(sql)
        .map_err(|e| e.to_string())?;
    let stmt = parser
        .parse_statement()
        .map_err(|e| normalize_raw_parse_error(sql, &e.to_string()))?;
    Ok(stmt)
}

fn normalize_raw_parse_error(sql: &str, err: &str) -> String {
    normalize_array_agg_parse_error(sql, err).unwrap_or_else(|| err.to_string())
}

fn normalize_array_agg_parse_error(sql: &str, err: &str) -> Option<String> {
    let normalized_sql = sql
        .chars()
        .map(|ch| {
            if ch.is_ascii_whitespace() {
                ' '
            } else {
                ch.to_ascii_lowercase()
            }
        })
        .collect::<String>();
    let rest = array_agg_call_body(&normalized_sql)?;
    let trimmed = rest.trim_start();

    if trimmed.starts_with("order by") {
        return Some("Unexpected input '(', the most similar input is {<EOF>, ';'}.".to_string());
    }
    if trimmed.starts_with("separator null") {
        return Some("No viable statement for input 'array_agg(separator NULL'.".to_string());
    }
    if let Some(after_distinct) = trimmed.strip_prefix("distinct")
        && after_distinct.trim_start().starts_with("order by")
    {
        return Some(
            "Unexpected input 'order', the most similar input is {a legal identifier}.".to_string(),
        );
    }
    if err.contains("Expected: ), found: separator")
        && trimmed.contains(" order by ")
        && trimmed.contains(" separator ")
    {
        return Some(
            "Unexpected input 'separator', the most similar input is {',', ')'}.".to_string(),
        );
    }

    None
}

fn array_agg_call_body(sql: &str) -> Option<&str> {
    let array_agg = sql.find("array_agg")?;
    let after_name = &sql[array_agg + "array_agg".len()..];
    let open = after_name.find('(')?;
    Some(&after_name[open + 1..])
}
