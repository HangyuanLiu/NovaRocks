// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to you under the Apache License, Version 2.0.

//! Seed AST contracts for the canonical printer.
//!
//! T6 wires `parse()` and extends this test module with
//! `parse(print(ast))` structural-equivalence assertions. Keeping these
//! printer-only contracts independent from that pending public API lets the
//! canonical rendering rules stay covered before parser dispatch exists.

use novarocks_parser::{
    Span,
    ast::{
        BinaryExpr, BinaryOperator, Expr, Ident, Literal, LiteralKind, RawQuerySlice, ShowBackends,
        Statement,
    },
    printer::{print_expr, print_statement},
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
fn roundtrip_seed_ast_printer_contracts_cover_vertical_slice_and_expressions() {
    let show_backends = Statement::ShowBackends(ShowBackends { span: span() });
    assert_eq!(print_statement(&show_backends), "SHOW BACKENDS");

    let raw_query = Statement::RawQuery(RawQuerySlice {
        text: "SELECT /* retained verbatim */ 1".to_owned(),
        span: span(),
    });
    assert_eq!(
        print_statement(&raw_query),
        "SELECT /* retained verbatim */ 1"
    );

    let expression = binary(
        ident("a"),
        BinaryOperator::Multiply,
        binary(
            Expr::Literal(Literal {
                kind: LiteralKind::Number("01.5e+2".to_owned()),
                span: span(),
            }),
            BinaryOperator::Add,
            ident("c"),
        ),
    );
    assert_eq!(print_expr(&expression), "a * (01.5e+2 + c)");
}
