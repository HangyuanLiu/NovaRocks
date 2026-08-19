// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to you under the Apache License, Version 2.0.

//! Recursive traversal and rebuilding helpers for every AST node.

use super::{
    BinaryExpr, Expr, FunctionCall, Ident, Literal, NestedExpr, ObjectName, RawQuerySlice,
    ShowBackends, Statement, TypeName, UnaryExpr,
};

/// Visits AST nodes by shared reference.
pub trait Visit {
    fn visit_statement(&mut self, statement: &Statement) {
        walk_statement(self, statement);
    }

    fn visit_show_backends(&mut self, statement: &ShowBackends) {
        walk_show_backends(self, statement);
    }

    fn visit_raw_query_slice(&mut self, query: &RawQuerySlice) {
        let _ = query;
    }

    fn visit_ident(&mut self, ident: &Ident) {
        walk_ident(self, ident);
    }

    fn visit_object_name(&mut self, name: &ObjectName) {
        walk_object_name(self, name);
    }

    fn visit_type_name(&mut self, type_name: &TypeName) {
        walk_type_name(self, type_name);
    }

    fn visit_literal(&mut self, literal: &Literal) {
        walk_literal(self, literal);
    }

    fn visit_expr(&mut self, expression: &Expr) {
        walk_expr(self, expression);
    }

    fn visit_function_call(&mut self, call: &FunctionCall) {
        walk_function_call(self, call);
    }

    fn visit_unary_expr(&mut self, expression: &UnaryExpr) {
        walk_unary_expr(self, expression);
    }

    fn visit_binary_expr(&mut self, expression: &BinaryExpr) {
        walk_binary_expr(self, expression);
    }

    fn visit_nested_expr(&mut self, expression: &NestedExpr) {
        walk_nested_expr(self, expression);
    }
}

pub fn walk_statement<V: Visit + ?Sized>(visitor: &mut V, statement: &Statement) {
    match statement {
        Statement::ShowBackends(statement) => visitor.visit_show_backends(statement),
        Statement::RawQuery(query) => visitor.visit_raw_query_slice(query),
    }
}

pub fn walk_show_backends<V: Visit + ?Sized>(_: &mut V, _: &ShowBackends) {}

pub fn walk_ident<V: Visit + ?Sized>(_: &mut V, _: &Ident) {}

pub fn walk_object_name<V: Visit + ?Sized>(visitor: &mut V, name: &ObjectName) {
    for part in &name.parts {
        visitor.visit_ident(part);
    }
}

pub fn walk_type_name<V: Visit + ?Sized>(visitor: &mut V, type_name: &TypeName) {
    visitor.visit_object_name(&type_name.name);
}

pub fn walk_literal<V: Visit + ?Sized>(_: &mut V, _: &Literal) {}

pub fn walk_expr<V: Visit + ?Sized>(visitor: &mut V, expression: &Expr) {
    match expression {
        Expr::Identifier(ident) => visitor.visit_ident(ident),
        Expr::Literal(literal) => visitor.visit_literal(literal),
        Expr::FunctionCall(call) => visitor.visit_function_call(call),
        Expr::Unary(expression) => visitor.visit_unary_expr(expression),
        Expr::Binary(expression) => visitor.visit_binary_expr(expression),
        Expr::Nested(expression) => visitor.visit_nested_expr(expression),
    }
}

pub fn walk_function_call<V: Visit + ?Sized>(visitor: &mut V, call: &FunctionCall) {
    visitor.visit_ident(&call.name);
    for argument in &call.arguments {
        visitor.visit_expr(argument);
    }
}

pub fn walk_unary_expr<V: Visit + ?Sized>(visitor: &mut V, expression: &UnaryExpr) {
    visitor.visit_expr(&expression.expression);
}

pub fn walk_binary_expr<V: Visit + ?Sized>(visitor: &mut V, expression: &BinaryExpr) {
    visitor.visit_expr(&expression.left);
    visitor.visit_expr(&expression.right);
}

pub fn walk_nested_expr<V: Visit + ?Sized>(visitor: &mut V, expression: &NestedExpr) {
    visitor.visit_expr(&expression.expression);
}

/// Rebuilds AST nodes by value, recursively folding all children by default.
pub trait Fold {
    fn fold_statement(&mut self, statement: Statement) -> Statement {
        fold_statement(self, statement)
    }

    fn fold_show_backends(&mut self, statement: ShowBackends) -> ShowBackends {
        fold_show_backends(self, statement)
    }

    fn fold_raw_query_slice(&mut self, query: RawQuerySlice) -> RawQuerySlice {
        query
    }

    fn fold_ident(&mut self, ident: Ident) -> Ident {
        fold_ident(self, ident)
    }

    fn fold_object_name(&mut self, name: ObjectName) -> ObjectName {
        fold_object_name(self, name)
    }

    fn fold_type_name(&mut self, type_name: TypeName) -> TypeName {
        fold_type_name(self, type_name)
    }

    fn fold_literal(&mut self, literal: Literal) -> Literal {
        fold_literal(self, literal)
    }

    fn fold_expr(&mut self, expression: Expr) -> Expr {
        fold_expr(self, expression)
    }

    fn fold_function_call(&mut self, call: FunctionCall) -> FunctionCall {
        fold_function_call(self, call)
    }

    fn fold_unary_expr(&mut self, expression: UnaryExpr) -> UnaryExpr {
        fold_unary_expr(self, expression)
    }

    fn fold_binary_expr(&mut self, expression: BinaryExpr) -> BinaryExpr {
        fold_binary_expr(self, expression)
    }

    fn fold_nested_expr(&mut self, expression: NestedExpr) -> NestedExpr {
        fold_nested_expr(self, expression)
    }
}

pub fn fold_statement<F: Fold + ?Sized>(folder: &mut F, statement: Statement) -> Statement {
    match statement {
        Statement::ShowBackends(statement) => {
            Statement::ShowBackends(folder.fold_show_backends(statement))
        }
        Statement::RawQuery(query) => Statement::RawQuery(folder.fold_raw_query_slice(query)),
    }
}

pub fn fold_show_backends<F: Fold + ?Sized>(_: &mut F, statement: ShowBackends) -> ShowBackends {
    statement
}

pub fn fold_ident<F: Fold + ?Sized>(_: &mut F, ident: Ident) -> Ident {
    ident
}

pub fn fold_object_name<F: Fold + ?Sized>(folder: &mut F, mut name: ObjectName) -> ObjectName {
    name.parts = name
        .parts
        .into_iter()
        .map(|part| folder.fold_ident(part))
        .collect();
    name
}

pub fn fold_type_name<F: Fold + ?Sized>(folder: &mut F, mut type_name: TypeName) -> TypeName {
    type_name.name = folder.fold_object_name(type_name.name);
    type_name
}

pub fn fold_literal<F: Fold + ?Sized>(_: &mut F, literal: Literal) -> Literal {
    literal
}

pub fn fold_expr<F: Fold + ?Sized>(folder: &mut F, expression: Expr) -> Expr {
    match expression {
        Expr::Identifier(ident) => Expr::Identifier(folder.fold_ident(ident)),
        Expr::Literal(literal) => Expr::Literal(folder.fold_literal(literal)),
        Expr::FunctionCall(call) => Expr::FunctionCall(folder.fold_function_call(call)),
        Expr::Unary(expression) => Expr::Unary(folder.fold_unary_expr(expression)),
        Expr::Binary(expression) => Expr::Binary(folder.fold_binary_expr(expression)),
        Expr::Nested(expression) => Expr::Nested(folder.fold_nested_expr(expression)),
    }
}

pub fn fold_function_call<F: Fold + ?Sized>(
    folder: &mut F,
    mut call: FunctionCall,
) -> FunctionCall {
    call.name = folder.fold_ident(call.name);
    call.arguments = call
        .arguments
        .into_iter()
        .map(|argument| folder.fold_expr(argument))
        .collect();
    call
}

pub fn fold_unary_expr<F: Fold + ?Sized>(folder: &mut F, mut expression: UnaryExpr) -> UnaryExpr {
    expression.expression = Box::new(folder.fold_expr(*expression.expression));
    expression
}

pub fn fold_binary_expr<F: Fold + ?Sized>(
    folder: &mut F,
    mut expression: BinaryExpr,
) -> BinaryExpr {
    expression.left = Box::new(folder.fold_expr(*expression.left));
    expression.right = Box::new(folder.fold_expr(*expression.right));
    expression
}

pub fn fold_nested_expr<F: Fold + ?Sized>(
    folder: &mut F,
    mut expression: NestedExpr,
) -> NestedExpr {
    expression.expression = Box::new(folder.fold_expr(*expression.expression));
    expression
}
