// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! SQL admission helpers over native typed parser nodes.

pub fn query_allows_throw_exception_hint(query: &novarocks_parser::ast::Query) -> bool {
    use novarocks_parser::ast::{BinaryOperator, Expr, LiteralKind, SelectHintValue, SetExpr};

    let mut body = query.body.as_ref();
    while let SetExpr::Query(nested) = body {
        body = nested.body.as_ref();
    }
    let SetExpr::Select(select) = body else {
        return false;
    };
    select.hints.iter().any(|hint| {
        hint.name.value.eq_ignore_ascii_case("set_var")
            && matches!(&hint.value, SelectHintValue::Call { arguments } if arguments.iter().any(|argument| {
                matches!(argument,
                    Expr::Binary(binary)
                        if binary.operator == BinaryOperator::Equal
                            && matches!(binary.left.as_ref(), Expr::Identifier(name) if name.value.eq_ignore_ascii_case("sql_mode"))
                            && matches!(binary.right.as_ref(), Expr::Literal(literal) if matches!(&literal.kind, LiteralKind::String(value) if value.to_ascii_lowercase().contains("allow_throw_exception")))
                )
            }))
    })
}

#[cfg(test)]
mod tests {
    use super::query_allows_throw_exception_hint;

    #[test]
    fn allow_throw_exception_uses_typed_set_var_hints() {
        let mut statements =
            novarocks_parser::parse("SELECT /*+ SET_VAR(sql_mode = 'ALLOW_THROW_EXCEPTION') */ 1")
                .expect("typed hint fixture parses");
        let [novarocks_parser::ast::Statement::Query(query)] = statements.as_mut_slice() else {
            panic!("expected query");
        };
        assert!(query_allows_throw_exception_hint(query));
    }
}
