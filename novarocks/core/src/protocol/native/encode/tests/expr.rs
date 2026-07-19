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

use arrow::datatypes::DataType;
use prost::Message;

use super::super::expr::encode_expr;
use super::{column_expr, int_expr};
use crate::proto::{common, expr};
use crate::sql::analysis::{ExprKind, SortItem, SubqueryKind, TypedExpr};
use crate::sql::common::{
    BinOp, LambdaParam, LiteralValue, UnOp, WindowBound, WindowFrame, WindowFrameType,
};

fn literal_expr(value: LiteralValue, data_type: DataType) -> TypedExpr {
    TypedExpr {
        kind: ExprKind::Literal(value),
        data_type,
        nullable: false,
    }
}

fn bool_expr(value: bool) -> TypedExpr {
    literal_expr(LiteralValue::Bool(value), DataType::Boolean)
}

fn string_expr(value: &str) -> TypedExpr {
    literal_expr(LiteralValue::String(value.to_string()), DataType::Utf8)
}

fn assert_expr_roundtrip(encoded: expr::Expr) -> expr::Expr {
    let decoded =
        expr::Expr::decode(encoded.encode_to_vec().as_slice()).expect("decode proto message");
    assert_eq!(encoded, decoded);
    decoded
}

#[test]
fn decimal_date_and_largeint_literals_are_structured() {
    let decimal = literal_expr(
        LiteralValue::Decimal("-123.45".to_string()),
        DataType::Decimal128(10, 2),
    );
    let encoded = encode_expr(&decimal).expect("encode decimal literal");
    let decoded = assert_expr_roundtrip(encoded);
    let Some(expr::expr::Kind::Literal(lit)) = decoded.kind else {
        panic!("expected literal");
    };
    let Some(common::literal_value::Value::DecimalValue(value)) = lit.value.and_then(|v| v.value)
    else {
        panic!("expected decimal literal");
    };
    assert_eq!(value.precision, 10);
    assert_eq!(value.scale, 2);
    assert_eq!(value.value, (-12_345i128).to_be_bytes().to_vec());

    let date = literal_expr(LiteralValue::Int(19_000), DataType::Date32);
    let encoded = encode_expr(&date).expect("encode date32 literal");
    let Some(expr::expr::Kind::Literal(lit)) = assert_expr_roundtrip(encoded).kind else {
        panic!("expected literal");
    };
    assert_eq!(
        lit.value.and_then(|v| v.value),
        Some(common::literal_value::Value::Date32Value(19_000))
    );

    let largeint = literal_expr(
        LiteralValue::LargeInt(-170_141_183_460_469_231_731_687_303_715_884_105_728i128),
        DataType::FixedSizeBinary(16),
    );
    let encoded = encode_expr(&largeint).expect("encode largeint literal");
    let Some(expr::expr::Kind::Literal(lit)) = assert_expr_roundtrip(encoded).kind else {
        panic!("expected literal");
    };
    assert_eq!(
        lit.value.and_then(|v| v.value),
        Some(common::literal_value::Value::LargeintValue(
            i128::MIN.to_be_bytes().to_vec()
        ))
    );
}

#[test]
fn decimal256_literal_encodes_full_32_byte_range() {
    let decimal = literal_expr(
        LiteralValue::Decimal("170141183460469231731687303715884105728.99".to_string()),
        DataType::Decimal256(41, 2),
    );

    let encoded = encode_expr(&decimal).expect("encode decimal256 literal");
    let Some(expr::expr::Kind::Literal(lit)) = assert_expr_roundtrip(encoded).kind else {
        panic!("expected literal");
    };
    let Some(common::literal_value::Value::DecimalValue(value)) = lit.value.and_then(|v| v.value)
    else {
        panic!("expected decimal literal");
    };
    assert_eq!(value.precision, 41);
    assert_eq!(value.scale, 2);
    assert_eq!(value.value.len(), 32);
    assert_eq!(
        hex::encode(value.value),
        "0000000000000000000000000000003200000000000000000000000000000063"
    );
}

#[test]
fn invalid_decimal_literal_widths_are_rejected() {
    let decimal128 = literal_expr(
        LiteralValue::Decimal("1".to_string()),
        DataType::Decimal128(39, 0),
    );
    let err = encode_expr(&decimal128).expect_err("Decimal128 literal width must fail");
    assert!(err.contains("Decimal128"));
    assert!(err.contains("precision"));

    let decimal256 = literal_expr(
        LiteralValue::Decimal("1".to_string()),
        DataType::Decimal256(77, 0),
    );
    let err = encode_expr(&decimal256).expect_err("Decimal256 literal width must fail");
    assert!(err.contains("Decimal256"));
    assert!(err.contains("precision"));
}

#[test]
fn typed_expr_variants_encode_to_expected_oneof_arms() {
    let col = column_expr(7, "a", DataType::Int64);
    let lit = int_expr(2);
    let lambda_body = TypedExpr {
        kind: ExprKind::BinaryOp {
            left: Box::new(TypedExpr {
                kind: ExprKind::LambdaParamRef {
                    name: "x".to_string(),
                    slot_id: 3,
                },
                data_type: DataType::Int64,
                nullable: true,
            }),
            op: BinOp::Add,
            right: Box::new(int_expr(1)),
        },
        data_type: DataType::Int64,
        nullable: true,
    };
    let sort_item = SortItem {
        expr: col.clone(),
        asc: false,
        nulls_first: true,
    };
    let lambda_expr = TypedExpr {
        kind: ExprKind::LambdaFunction {
            params: vec![LambdaParam {
                name: "x".to_string(),
                slot_id: 3,
                data_type: DataType::Int64,
                nullable: true,
            }],
            body: Box::new(lambda_body.clone()),
        },
        data_type: DataType::Int64,
        nullable: true,
    };
    let Some(expr::expr::Kind::Lambda(lambda)) =
        encode_expr(&lambda_expr).expect("encode lambda").kind
    else {
        panic!("expected lambda");
    };
    assert_eq!(lambda.params[0].name.as_deref(), Some("x"));
    assert_eq!(lambda.params[0].slot_id, 3);
    let common::type_desc::Kind::Scalar(scalar) = lambda.params[0]
        .r#type
        .as_ref()
        .and_then(|desc| desc.kind.as_ref())
        .expect("type kind")
    else {
        panic!("expected scalar TypeDesc");
    };
    assert_eq!(
        common::PrimitiveType::try_from(scalar.r#type).expect("known primitive"),
        common::PrimitiveType::Bigint
    );
    let variants = vec![
        column_expr(1, "c1", DataType::Int64),
        TypedExpr {
            kind: ExprKind::LambdaParamRef {
                name: "x".to_string(),
                slot_id: 3,
            },
            data_type: DataType::Int64,
            nullable: true,
        },
        lit.clone(),
        TypedExpr {
            kind: ExprKind::BinaryOp {
                left: Box::new(col.clone()),
                op: BinOp::Gt,
                right: Box::new(lit.clone()),
            },
            data_type: DataType::Boolean,
            nullable: false,
        },
        TypedExpr {
            kind: ExprKind::UnaryOp {
                op: UnOp::Not,
                expr: Box::new(bool_expr(true)),
            },
            data_type: DataType::Boolean,
            nullable: false,
        },
        TypedExpr {
            kind: ExprKind::FunctionCall {
                name: "abs".to_string(),
                args: vec![col.clone()],
                distinct: false,
            },
            data_type: DataType::Int64,
            nullable: true,
        },
        TypedExpr {
            kind: ExprKind::LambdaFunction {
                params: vec![LambdaParam {
                    name: "x".to_string(),
                    slot_id: 3,
                    data_type: DataType::Int64,
                    nullable: true,
                }],
                body: Box::new(lambda_body.clone()),
            },
            data_type: DataType::Int64,
            nullable: true,
        },
        TypedExpr {
            kind: ExprKind::AggregateCall {
                name: "sum".to_string(),
                args: vec![col.clone()],
                distinct: true,
                order_by: vec![sort_item.clone()],
            },
            data_type: DataType::Int64,
            nullable: true,
        },
        TypedExpr {
            kind: ExprKind::Cast {
                expr: Box::new(col.clone()),
                target: DataType::Float64,
            },
            data_type: DataType::Float64,
            nullable: true,
        },
        TypedExpr {
            kind: ExprKind::IsNull {
                expr: Box::new(col.clone()),
                negated: true,
            },
            data_type: DataType::Boolean,
            nullable: false,
        },
        TypedExpr {
            kind: ExprKind::InList {
                expr: Box::new(string_expr("x")),
                list: vec![string_expr("a"), string_expr("b")],
                negated: false,
            },
            data_type: DataType::Boolean,
            nullable: false,
        },
        TypedExpr {
            kind: ExprKind::Between {
                expr: Box::new(col.clone()),
                low: Box::new(int_expr(1)),
                high: Box::new(int_expr(9)),
                negated: true,
            },
            data_type: DataType::Boolean,
            nullable: false,
        },
        TypedExpr {
            kind: ExprKind::Like {
                expr: Box::new(string_expr("abc")),
                pattern: Box::new(string_expr("a%")),
                negated: true,
            },
            data_type: DataType::Boolean,
            nullable: false,
        },
        TypedExpr {
            kind: ExprKind::Case {
                operand: None,
                when_then: vec![(bool_expr(true), int_expr(1))],
                else_expr: Some(Box::new(int_expr(0))),
            },
            data_type: DataType::Int64,
            nullable: true,
        },
        TypedExpr {
            kind: ExprKind::IsTruthValue {
                expr: Box::new(bool_expr(false)),
                value: false,
                negated: true,
            },
            data_type: DataType::Boolean,
            nullable: false,
        },
        TypedExpr {
            kind: ExprKind::Nested(Box::new(col.clone())),
            data_type: DataType::Int64,
            nullable: true,
        },
        TypedExpr {
            kind: ExprKind::WindowCall {
                name: "rank".to_string(),
                args: vec![],
                distinct: false,
                partition_by: vec![col.clone()],
                order_by: vec![sort_item],
                window_frame: Some(WindowFrame {
                    frame_type: WindowFrameType::Rows,
                    start: WindowBound::UnboundedPreceding,
                    end: WindowBound::CurrentRow,
                }),
                ignore_nulls: false,
            },
            data_type: DataType::Int64,
            nullable: false,
        },
    ];

    let names = variants
        .iter()
        .map(|expr| {
            let encoded = encode_expr(expr).expect("encode variant");
            let decoded = assert_expr_roundtrip(encoded);
            match decoded.kind.expect("expr kind") {
                expr::expr::Kind::ColumnRef(_) => "column_ref",
                expr::expr::Kind::LambdaParamRef(_) => "lambda_param_ref",
                expr::expr::Kind::Literal(_) => "literal",
                expr::expr::Kind::BinaryOp(_) => "binary_op",
                expr::expr::Kind::UnaryOp(_) => "unary_op",
                expr::expr::Kind::FunctionCall(_) => "function_call",
                expr::expr::Kind::Lambda(_) => "lambda",
                expr::expr::Kind::AggregateCall(_) => "aggregate_call",
                expr::expr::Kind::Cast(_) => "cast",
                expr::expr::Kind::IsNull(_) => "is_null",
                expr::expr::Kind::InList(_) => "in_list",
                expr::expr::Kind::Between(_) => "between",
                expr::expr::Kind::Like(_) => "like",
                expr::expr::Kind::CaseExpr(_) => "case",
                expr::expr::Kind::IsTruth(_) => "is_truth",
                expr::expr::Kind::Nested(_) => "nested",
                expr::expr::Kind::WindowCall(_) => "window_call",
            }
        })
        .collect::<Vec<_>>();

    assert_eq!(names.len(), 17);
    assert!(names.contains(&"lambda_param_ref"));
    assert!(names.contains(&"window_call"));
    assert!(names.contains(&"aggregate_call"));
    assert!(names.contains(&"case"));
}

#[test]
fn representative_nested_expr_fields_are_preserved() {
    let col = column_expr(7, "amount", DataType::Int64);

    let cast = TypedExpr {
        kind: ExprKind::Cast {
            expr: Box::new(col.clone()),
            target: DataType::Float64,
        },
        data_type: DataType::Float64,
        nullable: true,
    };
    let Some(expr::expr::Kind::Cast(cast)) = encode_expr(&cast).expect("encode cast").kind else {
        panic!("expected cast");
    };
    let common::type_desc::Kind::Scalar(scalar) = cast
        .target
        .as_ref()
        .and_then(|desc| desc.kind.as_ref())
        .expect("target type kind")
    else {
        panic!("expected scalar TypeDesc");
    };
    assert_eq!(
        common::PrimitiveType::try_from(scalar.r#type).expect("known primitive"),
        common::PrimitiveType::Double
    );

    let lambda_param = TypedExpr {
        kind: ExprKind::LambdaParamRef {
            name: "x".to_string(),
            slot_id: 3,
        },
        data_type: DataType::Int64,
        nullable: true,
    };
    let Some(expr::expr::Kind::LambdaParamRef(param)) = encode_expr(&lambda_param)
        .expect("encode lambda param")
        .kind
    else {
        panic!("expected lambda param");
    };
    assert_eq!(param.slot_id, 3);
    assert_eq!(param.name.as_deref(), Some("x"));

    let sort_item = SortItem {
        expr: col.clone(),
        asc: false,
        nulls_first: true,
    };
    let window = TypedExpr {
        kind: ExprKind::WindowCall {
            name: "rank".to_string(),
            args: vec![],
            distinct: false,
            partition_by: vec![col],
            order_by: vec![sort_item],
            window_frame: Some(WindowFrame {
                frame_type: WindowFrameType::Rows,
                start: WindowBound::UnboundedPreceding,
                end: WindowBound::CurrentRow,
            }),
            ignore_nulls: false,
        },
        data_type: DataType::Int64,
        nullable: false,
    };
    let Some(expr::expr::Kind::WindowCall(window)) =
        encode_expr(&window).expect("encode window").kind
    else {
        panic!("expected window call");
    };
    assert_eq!(window.function_name, "rank");
    assert_eq!(window.order_by.len(), 1);
    assert!(!window.order_by[0].asc);
    assert!(window.order_by[0].nulls_first);
    assert_eq!(
        window.frame.as_ref().expect("window frame").frame_type,
        expr::WindowFrameType::Rows as i32
    );
}

#[test]
fn subquery_placeholder_is_rejected_by_expr_encoder() {
    let subquery = TypedExpr {
        kind: ExprKind::SubqueryPlaceholder {
            id: 42,
            kind: SubqueryKind::Scalar,
            data_type: DataType::Int64,
        },
        data_type: DataType::Int64,
        nullable: true,
    };

    let err = encode_expr(&subquery).expect_err("subquery must fail fast");
    assert!(err.contains("SubqueryPlaceholder"));
    assert!(err.contains("42"));
}

#[test]
fn unsupported_decimal_literal_reports_clear_error() {
    let malformed = literal_expr(
        LiteralValue::Decimal("12.345".to_string()),
        DataType::Decimal128(10, 2),
    );

    let err = encode_expr(&malformed).expect_err("scale mismatch");
    assert!(err.contains("scale"));
}
