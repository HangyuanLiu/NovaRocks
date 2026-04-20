#![allow(dead_code)]

pub(crate) mod ast;
pub(crate) mod dialect;
mod raw;

/// Parse SQL into a raw sqlparser AST (no custom AST conversion).
/// Used by the standalone ThriftPlanBuilder.
pub(crate) fn parse_sql_raw(sql: &str) -> Result<sqlparser::ast::Statement, String> {
    raw::parse_sql_raw(sql)
}

pub(crate) fn parse_normalized_sql_raw(sql: &str) -> Result<sqlparser::ast::Statement, String> {
    raw::parse_normalized_sql_raw(sql)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_sql_raw_rewrites_typed_array_literals() {
        let stmt = parse_sql_raw("SELECT array<double>[0.25, 0.5]").expect("parse should succeed");
        let sqlparser::ast::Statement::Query(query) = stmt else {
            panic!("expected query statement");
        };
        let sqlparser::ast::SetExpr::Select(select) = query.body.as_ref() else {
            panic!("expected select body");
        };
        let sqlparser::ast::SelectItem::UnnamedExpr(expr) = &select.projection[0] else {
            panic!("expected unnamed projection");
        };
        let sqlparser::ast::Expr::Cast {
            expr: inner,
            data_type,
            ..
        } = expr
        else {
            panic!("expected CAST wrapper, got {expr:?}");
        };
        let sqlparser::ast::Expr::Array(array) = inner.as_ref() else {
            panic!("expected array literal, got {inner:?}");
        };
        assert_eq!(array.elem.len(), 2);
        assert!(matches!(
            data_type,
            sqlparser::ast::DataType::Array(sqlparser::ast::ArrayElemTypeDef::AngleBracket(inner))
                if matches!(inner.as_ref(), sqlparser::ast::DataType::Double(_) | sqlparser::ast::DataType::DoublePrecision)
        ));
    }

    #[test]
    fn parse_sql_raw_normalizes_array_agg_separator_error() {
        let err =
            parse_sql_raw(r#"SELECT array_agg("中国" order by 2, id separator NULL) from ss"#)
                .expect_err("malformed array_agg should fail");
        assert_eq!(
            err,
            "Unexpected input 'separator', the most similar input is {',', ')'}.",
        );
    }

    #[test]
    fn parse_sql_raw_normalizes_array_agg_missing_argument_error() {
        let err =
            parse_sql_raw("SELECT array_agg(order by 1 separator '')").expect_err("should fail");
        assert_eq!(
            err,
            "Unexpected input '(', the most similar input is {<EOF>, ';'}.",
        );
    }

    #[test]
    fn parse_sql_raw_normalizes_array_agg_distinct_missing_argument_error() {
        let err = parse_sql_raw("SELECT array_agg(distinct  order by score) from ss order by 1")
            .expect_err("should fail");
        assert_eq!(
            err,
            "Unexpected input 'order', the most similar input is {a legal identifier}.",
        );
    }
}
