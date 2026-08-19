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

//! Canonical SQL rendering for the parser-owned syntax tree.

use crate::ast::{
    BinaryExpr, BinaryOperator, Expr, FunctionCall, Ident, Literal, LiteralKind, NestedExpr,
    ObjectName, RawQuerySlice, Statement, TypeName, TypeNameArgument, UnaryExpr, UnaryOperator,
};

/// Renders parser AST nodes into one stable SQL spelling.
///
/// The printer intentionally keeps identifier spelling and numeric literal
/// spelling supplied by the AST. Those are syntax facts, while identifier
/// comparison and numeric normalization belong to later semantic owners.
#[derive(Default)]
pub struct Printer {
    output: String,
}

impl Printer {
    /// Creates an empty canonical SQL printer.
    pub fn new() -> Self {
        Self::default()
    }

    /// Renders one top-level statement.
    pub fn statement(mut self, statement: &Statement) -> String {
        self.write_statement(statement);
        self.output
    }

    /// Renders a semicolon-separated statement sequence.
    pub fn statements(mut self, statements: &[Statement]) -> String {
        for (index, statement) in statements.iter().enumerate() {
            if index != 0 {
                self.output.push_str("; ");
            }
            self.write_statement(statement);
        }
        self.output
    }

    /// Renders one expression.
    pub fn expression(mut self, expression: &Expr) -> String {
        self.write_expr(expression);
        self.output
    }

    /// Renders a qualified object name.
    pub fn object_name(mut self, name: &ObjectName) -> String {
        self.write_object_name(name);
        self.output
    }

    /// Renders a syntax-level type name.
    pub fn type_name(mut self, type_name: &TypeName) -> String {
        self.write_type_name(type_name);
        self.output
    }

    fn write_statement(&mut self, statement: &Statement) {
        match statement {
            Statement::Backend(statement) => {
                crate::ast::backend::write_sql(statement, &mut self.output)
            }
            Statement::Statistics(statement) => {
                crate::ast::statistics::write_sql(statement, &mut self.output)
            }
            Statement::Catalog(statement) => {
                crate::ast::catalog::write_sql(statement, &mut self.output)
            }
            Statement::Iceberg(statement) => {
                crate::ast::iceberg::write_sql(statement, &mut self.output)
            }
            Statement::Maintenance(statement) => {
                crate::ast::maintenance::write_sql(statement, &mut self.output)
            }
            Statement::MaterializedView(statement) => {
                crate::ast::materialized_view::write_sql(statement, &mut self.output)
            }
            Statement::RawQuery(query) => self.write_raw_query(query),
        }
    }

    fn write_raw_query(&mut self, query: &RawQuerySlice) {
        self.output.push_str(&query.text);
    }

    fn write_expr(&mut self, expression: &Expr) {
        match expression {
            Expr::Identifier(ident) => self.write_ident(ident),
            Expr::Literal(literal) => self.write_literal(literal),
            Expr::FunctionCall(call) => self.write_function_call(call),
            Expr::Unary(expression) => self.write_unary_expr(expression),
            Expr::Binary(expression) => self.write_binary_expr(expression),
            Expr::Nested(expression) => self.write_nested_expr(expression),
        }
    }

    fn write_ident(&mut self, ident: &Ident) {
        if ident.quoted {
            self.output.push('`');
            self.output.push_str(&ident.value.replace('`', "``"));
            self.output.push('`');
        } else {
            self.output.push_str(&ident.value);
        }
    }

    fn write_object_name(&mut self, name: &ObjectName) {
        for (index, part) in name.parts.iter().enumerate() {
            if index != 0 {
                self.output.push('.');
            }
            self.write_ident(part);
        }
    }

    fn write_type_name(&mut self, type_name: &TypeName) {
        self.write_object_name(&type_name.name);
        if type_name.arguments.is_empty() {
            return;
        }
        let generic = matches!(
            type_name
                .name
                .parts
                .last()
                .map(|part| part.value.to_ascii_uppercase())
                .as_deref(),
            Some("ARRAY" | "MAP" | "STRUCT")
        );
        self.output.push(if generic { '<' } else { '(' });
        for (index, argument) in type_name.arguments.iter().enumerate() {
            if index != 0 {
                self.output.push_str(", ");
            }
            match argument {
                TypeNameArgument::Type(data_type) => self.write_type_name(data_type),
                TypeNameArgument::Literal(literal) => self.write_literal(literal),
                TypeNameArgument::Field(field) => {
                    self.write_ident(&field.name);
                    self.output.push(' ');
                    self.write_type_name(&field.data_type);
                }
            }
        }
        self.output.push(if generic { '>' } else { ')' });
    }

    fn write_literal(&mut self, literal: &Literal) {
        match &literal.kind {
            LiteralKind::Null => self.output.push_str("NULL"),
            LiteralKind::Boolean(value) => {
                self.output.push_str(if *value { "TRUE" } else { "FALSE" })
            }
            LiteralKind::Number(value) => self.output.push_str(value),
            LiteralKind::HexString(value) => self.output.push_str(value),
            LiteralKind::String(value) => self.write_quoted_string(value),
        }
    }

    fn write_quoted_string(&mut self, value: &str) {
        self.output.push('\'');
        for character in value.chars() {
            match character {
                '\\' => self.output.push_str("\\\\"),
                '\'' => self.output.push_str("''"),
                _ => self.output.push(character),
            }
        }
        self.output.push('\'');
    }

    fn write_function_call(&mut self, call: &FunctionCall) {
        self.write_ident(&call.name);
        self.output.push('(');
        for (index, argument) in call.arguments.iter().enumerate() {
            if index != 0 {
                self.output.push_str(", ");
            }
            self.write_expr(argument);
        }
        self.output.push(')');
    }

    fn write_unary_expr(&mut self, expression: &UnaryExpr) {
        match expression.operator {
            UnaryOperator::Not => self.output.push_str("NOT "),
            UnaryOperator::Plus => self.output.push('+'),
            UnaryOperator::Minus => self.output.push('-'),
        }

        let requires_separator = matches!(expression.expression.as_ref(), Expr::Unary(_))
            && !matches!(expression.operator, UnaryOperator::Not);
        if requires_separator {
            self.output.push(' ');
        }
        self.write_unary_operand(&expression.expression);
    }

    fn write_unary_operand(&mut self, expression: &Expr) {
        if matches!(expression, Expr::Binary(_)) {
            self.output.push('(');
            self.write_expr(expression);
            self.output.push(')');
        } else {
            self.write_expr(expression);
        }
    }

    fn write_binary_expr(&mut self, expression: &BinaryExpr) {
        self.write_binary_operand(&expression.left, expression.operator, BinarySide::Left);
        self.output.push(' ');
        self.output.push_str(expression.operator.sql());
        self.output.push(' ');
        self.write_binary_operand(&expression.right, expression.operator, BinarySide::Right);
    }

    fn write_binary_operand(&mut self, operand: &Expr, parent: BinaryOperator, side: BinarySide) {
        let requires_parentheses = match operand {
            Expr::Binary(child) => {
                child.operator.precedence() < parent.precedence()
                    || (child.operator.precedence() == parent.precedence()
                        && matches!(side, BinarySide::Right))
            }
            _ => false,
        };

        if requires_parentheses {
            self.output.push('(');
        }
        self.write_expr(operand);
        if requires_parentheses {
            self.output.push(')');
        }
    }

    fn write_nested_expr(&mut self, expression: &NestedExpr) {
        self.output.push('(');
        self.write_expr(&expression.expression);
        self.output.push(')');
    }
}

/// Renders one statement into canonical SQL.
pub fn print_statement(statement: &Statement) -> String {
    Printer::new().statement(statement)
}

/// Renders statements separated by one semicolon and one space.
pub fn print_statements(statements: &[Statement]) -> String {
    Printer::new().statements(statements)
}

/// Renders one expression into canonical SQL.
pub fn print_expr(expression: &Expr) -> String {
    Printer::new().expression(expression)
}

/// Renders an object name into canonical SQL.
pub fn print_object_name(name: &ObjectName) -> String {
    Printer::new().object_name(name)
}

/// Renders a syntax-level type name into canonical SQL.
pub fn print_type_name(type_name: &TypeName) -> String {
    Printer::new().type_name(type_name)
}

/// Renders one syntax literal into canonical SQL.
pub fn print_literal(literal: &Literal) -> String {
    match &literal.kind {
        LiteralKind::Null => "NULL".to_owned(),
        LiteralKind::Boolean(value) => if *value { "TRUE" } else { "FALSE" }.to_owned(),
        LiteralKind::Number(value) | LiteralKind::HexString(value) => value.clone(),
        LiteralKind::String(value) => {
            let mut output = String::from("'");
            for character in value.chars() {
                match character {
                    '\\' => output.push_str("\\\\"),
                    '\'' => output.push_str("''"),
                    _ => output.push(character),
                }
            }
            output.push('\'');
            output
        }
    }
}

#[derive(Clone, Copy)]
enum BinarySide {
    Left,
    Right,
}

impl BinaryOperator {
    const fn precedence(self) -> u8 {
        match self {
            Self::Or => 1,
            Self::And => 2,
            Self::Equal
            | Self::NotEqual
            | Self::LessThan
            | Self::LessThanOrEqual
            | Self::GreaterThan
            | Self::GreaterThanOrEqual => 3,
            Self::Add | Self::Subtract => 4,
            Self::Multiply | Self::Divide => 5,
        }
    }

    const fn sql(self) -> &'static str {
        match self {
            Self::Or => "OR",
            Self::And => "AND",
            Self::Equal => "=",
            Self::NotEqual => "!=",
            Self::LessThan => "<",
            Self::LessThanOrEqual => "<=",
            Self::GreaterThan => ">",
            Self::GreaterThanOrEqual => ">=",
            Self::Add => "+",
            Self::Subtract => "-",
            Self::Multiply => "*",
            Self::Divide => "/",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        Span,
        ast::{BackendStatement, LiteralKind, ShowBackends},
    };

    fn span() -> Span {
        Span::new(0, 0)
    }

    fn ident(value: &str) -> Expr {
        Expr::Identifier(Ident {
            value: value.to_owned(),
            quoted: false,
            span: span(),
        })
    }

    fn binary(left: Expr, operator: BinaryOperator, right: Expr) -> Expr {
        Expr::Binary(BinaryExpr {
            left: Box::new(left),
            operator,
            right: Box::new(right),
            span: span(),
        })
    }

    #[test]
    fn renders_every_seed_ast_node() {
        let quoted = Ident {
            value: "strange`name".to_owned(),
            quoted: true,
            span: span(),
        };
        let function = Expr::FunctionCall(FunctionCall {
            name: Ident {
                value: "Coalesce".to_owned(),
                quoted: false,
                span: span(),
            },
            arguments: vec![
                Expr::Identifier(quoted),
                Expr::Literal(Literal {
                    kind: LiteralKind::String("a'b\\c".to_owned()),
                    span: span(),
                }),
                Expr::Literal(Literal {
                    kind: LiteralKind::HexString("0xCAFE".to_owned()),
                    span: span(),
                }),
            ],
            span: span(),
        });
        let expression = Expr::Nested(NestedExpr {
            expression: Box::new(Expr::Unary(UnaryExpr {
                operator: UnaryOperator::Not,
                expression: Box::new(binary(
                    function,
                    BinaryOperator::Or,
                    Expr::Literal(Literal {
                        kind: LiteralKind::Boolean(false),
                        span: span(),
                    }),
                )),
                span: span(),
            })),
            span: span(),
        });

        assert_eq!(
            print_expr(&expression),
            "(NOT (Coalesce(`strange``name`, 'a''b\\\\c', 0xCAFE) OR FALSE))"
        );
        assert_eq!(
            print_expr(&Expr::Literal(Literal {
                kind: LiteralKind::Null,
                span: span(),
            })),
            "NULL"
        );
    }

    #[test]
    fn parenthesizes_right_nested_and_lower_precedence_operands() {
        let expression = binary(
            ident("a"),
            BinaryOperator::Multiply,
            binary(ident("b"), BinaryOperator::Add, ident("c")),
        );
        assert_eq!(print_expr(&expression), "a * (b + c)");

        let expression = binary(
            binary(ident("a"), BinaryOperator::Subtract, ident("b")),
            BinaryOperator::Subtract,
            ident("c"),
        );
        assert_eq!(print_expr(&expression), "a - b - c");

        let expression = binary(
            ident("a"),
            BinaryOperator::Subtract,
            binary(ident("b"), BinaryOperator::Subtract, ident("c")),
        );
        assert_eq!(print_expr(&expression), "a - (b - c)");
    }

    #[test]
    fn renders_raw_query_without_normalizing_its_text() {
        let statement = Statement::RawQuery(RawQuerySlice {
            text: "SELECT /*+ SET_VAR(x = 1) */ 1".to_owned(),
            span: span(),
        });
        assert_eq!(
            print_statement(&statement),
            "SELECT /*+ SET_VAR(x = 1) */ 1"
        );
    }

    #[test]
    fn renders_vertical_slice_and_statement_sequences() {
        let show = Statement::Backend(BackendStatement::ShowBackends(ShowBackends {
            span: span(),
        }));
        assert_eq!(print_statement(&show), "SHOW BACKENDS");
        assert_eq!(
            print_statements(&[show.clone(), show]),
            "SHOW BACKENDS; SHOW BACKENDS"
        );
    }

    #[test]
    fn renders_object_and_type_names() {
        let name = ObjectName {
            parts: vec![
                Ident {
                    value: "catalog".to_owned(),
                    quoted: false,
                    span: span(),
                },
                Ident {
                    value: "table".to_owned(),
                    quoted: true,
                    span: span(),
                },
            ],
            span: span(),
        };
        let type_name = TypeName {
            name,
            arguments: Vec::new(),
            span: span(),
        };

        assert_eq!(print_type_name(&type_name), "catalog.`table`");
        assert_eq!(print_object_name(&type_name.name), "catalog.`table`");
    }
}
