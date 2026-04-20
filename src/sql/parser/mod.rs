#![allow(dead_code)]

pub(crate) mod ast;
pub(crate) mod dialect;
mod raw;

/// Parse SQL into a raw sqlparser AST (no custom AST conversion).
/// Used by the standalone ThriftPlanBuilder.
pub(crate) fn parse_sql_raw(sql: &str) -> Result<sqlparser::ast::Statement, String> {
    raw::parse_sql_raw(sql)
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
}
