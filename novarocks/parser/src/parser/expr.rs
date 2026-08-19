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

//! Public entry point for the foundation expression grammar.

use crate::{ParseError, Token, ast::Expr};

use super::pratt::PrattParser;

/// `expression ::= prefix-expression { infix-operator prefix-expression } EOF`
///
/// Parses one complete expression from a lexer token stream. Trivia is ignored
/// syntactically while every AST node retains the exact byte span of its source.
pub fn parse_expression(source: &str, tokens: &[Token]) -> Result<Expr, ParseError> {
    PrattParser::new(source, tokens).parse()
}

#[cfg(test)]
mod tests {
    use crate::{
        ParseError, ParserError, Span,
        ast::{BinaryOperator, Expr, LiteralKind, UnaryOperator},
        lex,
    };

    use super::parse_expression;

    fn parse(source: &str) -> Expr {
        let tokens = lex(source).expect("test source must lex");
        parse_expression(source, &tokens).expect("test expression must parse")
    }

    #[test]
    fn precedence_and_left_associativity_are_table_driven() {
        let expression = parse("1 + 2 * 3 - 4");
        let Expr::Binary(subtract) = expression else {
            panic!("expected subtraction");
        };
        assert_eq!(subtract.operator, BinaryOperator::Subtract);
        let Expr::Binary(add) = *subtract.left else {
            panic!("expected addition on the left");
        };
        assert_eq!(add.operator, BinaryOperator::Add);
        let Expr::Binary(multiply) = *add.right else {
            panic!("expected multiplication to bind most tightly");
        };
        assert_eq!(multiply.operator, BinaryOperator::Multiply);

        let expression = parse("8 - 3 - 1");
        let Expr::Binary(outer) = expression else {
            panic!("expected subtraction");
        };
        assert_eq!(outer.operator, BinaryOperator::Subtract);
        assert!(matches!(*outer.left, Expr::Binary(_)));

        let expression = parse("NOT a = b AND c");
        let Expr::Binary(and) = expression else {
            panic!("expected conjunction");
        };
        assert_eq!(and.operator, BinaryOperator::And);
        let Expr::Unary(not) = *and.left else {
            panic!("expected NOT to bind below comparison and above AND");
        };
        assert_eq!(not.operator, UnaryOperator::Not);
        assert!(matches!(*not.expression, Expr::Binary(_)));
    }

    #[test]
    fn parentheses_unary_and_function_arguments_retain_precise_spans() {
        let source = "NOT (a = b) AND coalesce(-c, 'it''s')";
        let expression = parse(source);
        assert_eq!(expression.span(), Span::new(0, source.len()));

        let Expr::Binary(and) = expression else {
            panic!("expected conjunction");
        };
        assert_eq!(and.operator, BinaryOperator::And);
        let Expr::Unary(not) = *and.left else {
            panic!("expected unary NOT");
        };
        assert_eq!(not.operator, UnaryOperator::Not);
        assert_eq!(not.span, Span::new(0, 11));
        assert!(matches!(*not.expression, Expr::Nested(_)));

        let Expr::FunctionCall(call) = *and.right else {
            panic!("expected function call");
        };
        assert_eq!(call.span, Span::new(16, source.len()));
        assert_eq!(call.arguments.len(), 2);
        assert!(matches!(call.arguments[0], Expr::Unary(_)));
        let Expr::Literal(literal) = &call.arguments[1] else {
            panic!("expected string literal");
        };
        assert_eq!(literal.kind, LiteralKind::String("it's".to_owned()));
    }

    #[test]
    fn malformed_expressions_return_typed_code_and_found_token_span() {
        let source = "1 + * 2";
        let tokens = lex(source).expect("test source must lex");
        let error = parse_expression(source, &tokens).expect_err("expression must fail");
        assert_eq!(
            error,
            ParseError::UnexpectedToken {
                expected: "expression",
                found: "`*`".to_owned(),
                span: Span::new(4, 5),
            }
        );
        let user_error = ParserError::from(error).to_user_error(source);
        assert_eq!(user_error.code().as_str(), "sql.parse.unexpected_token");
        assert_eq!(
            user_error.to_string(),
            "[sql.parse.unexpected_token] expected expression, found `*` at line 1 column 5"
        );

        let source = "f(1, )";
        let tokens = lex(source).expect("test source must lex");
        let error = parse_expression(source, &tokens).expect_err("function argument must fail");
        assert_eq!(
            error,
            ParseError::UnexpectedToken {
                expected: "expression after ','",
                found: "`)`".to_owned(),
                span: Span::new(5, 6),
            }
        );
    }
}
