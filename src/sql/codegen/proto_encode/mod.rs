pub(crate) mod expr;
pub(crate) mod types;

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::datatypes::{DataType, Field, Fields, TimeUnit};
    use prost::Message;

    use super::expr::encode_expr;
    use super::types::{decode_field_type, decode_type, encode_field_type, encode_type};
    use crate::proto::{common, expr};
    use crate::sql::analysis::{ExprKind, SortItem, SubqueryKind, TypedExpr};
    use crate::sql::column_id::ColumnId;
    use crate::sql::common::{
        BinOp, LambdaParam, LiteralValue, UnOp, WindowBound, WindowFrame, WindowFrameType,
    };
    use crate::types::logical::{LogicalType, field_with_logical_type, logical_type_of_field};

    fn roundtrip_message<M>(value: &M) -> M
    where
        M: Message + Default,
    {
        M::decode(value.encode_to_vec().as_slice()).expect("decode proto message")
    }

    fn scalar_primitive(desc: &common::TypeDesc) -> common::PrimitiveType {
        let common::type_desc::Kind::Scalar(scalar) = desc.kind.as_ref().expect("type kind") else {
            panic!("expected scalar TypeDesc");
        };
        common::PrimitiveType::try_from(scalar.r#type).expect("known primitive")
    }

    fn literal_expr(value: LiteralValue, data_type: DataType) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::Literal(value),
            data_type,
            nullable: false,
        }
    }

    fn int_expr(value: i64) -> TypedExpr {
        literal_expr(LiteralValue::Int(value), DataType::Int64)
    }

    fn bool_expr(value: bool) -> TypedExpr {
        literal_expr(LiteralValue::Bool(value), DataType::Boolean)
    }

    fn string_expr(value: &str) -> TypedExpr {
        literal_expr(LiteralValue::String(value.to_string()), DataType::Utf8)
    }

    fn column_expr(id: u32, name: &str, data_type: DataType) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::ColumnRef {
                column_id: ColumnId::new_for_test(id),
                qualifier: Some("t".to_string()),
                column: name.to_string(),
            },
            data_type,
            nullable: true,
        }
    }

    fn assert_expr_roundtrip(encoded: expr::Expr) -> expr::Expr {
        let decoded = roundtrip_message(&encoded);
        assert_eq!(encoded, decoded);
        decoded
    }

    #[test]
    fn recursive_arrow_type_round_trips_through_type_desc() {
        let data_type = DataType::List(Arc::new(Field::new(
            "item",
            DataType::Struct(Fields::from(vec![
                Arc::new(Field::new("amount", DataType::Decimal128(18, 2), true)),
                Arc::new(Field::new(
                    "ids",
                    DataType::List(Arc::new(Field::new("item", DataType::Int64, true))),
                    true,
                )),
            ])),
            true,
        )));

        let encoded = encode_type(&data_type).expect("encode recursive type");
        let decoded_proto: common::TypeDesc = roundtrip_message(&encoded);
        assert_eq!(encoded, decoded_proto);
        assert_eq!(
            decode_type(&decoded_proto).expect("decode recursive type"),
            data_type
        );
    }

    #[test]
    fn map_type_round_trips_through_type_desc() {
        let data_type = DataType::Map(
            Arc::new(Field::new(
                "entries",
                DataType::Struct(Fields::from(vec![
                    Arc::new(Field::new("key", DataType::Utf8, true)),
                    Arc::new(Field::new("value", DataType::Decimal128(12, 4), true)),
                ])),
                false,
            )),
            false,
        );

        let encoded = encode_type(&data_type).expect("encode map type");
        let decoded_proto: common::TypeDesc = roundtrip_message(&encoded);
        assert_eq!(encoded, decoded_proto);
        assert_eq!(
            decode_type(&decoded_proto).expect("decode map type"),
            data_type
        );
    }

    #[test]
    fn metadata_logical_fields_encode_to_logical_primitives() {
        let cases = [
            (
                field_with_logical_type(
                    Field::new("json_payload", DataType::Utf8, true),
                    LogicalType::Json,
                ),
                common::PrimitiveType::Json,
                DataType::Utf8,
                Some(LogicalType::Json),
            ),
            (
                field_with_logical_type(
                    Field::new("hll_state", DataType::Binary, true),
                    LogicalType::Hll,
                ),
                common::PrimitiveType::Hll,
                DataType::Binary,
                Some(LogicalType::Hll),
            ),
            (
                field_with_logical_type(
                    Field::new("bitmap_state", DataType::Binary, true),
                    LogicalType::Bitmap,
                ),
                common::PrimitiveType::Bitmap,
                DataType::Binary,
                Some(LogicalType::Bitmap),
            ),
            (
                field_with_logical_type(
                    Field::new("object_state", DataType::Binary, true),
                    LogicalType::Object,
                ),
                common::PrimitiveType::Object,
                DataType::Binary,
                Some(LogicalType::Object),
            ),
            (
                field_with_logical_type(
                    Field::new("percentile_state", DataType::Binary, true),
                    LogicalType::Percentile,
                ),
                common::PrimitiveType::Percentile,
                DataType::Binary,
                Some(LogicalType::Percentile),
            ),
            (
                Field::new("variant_payload", DataType::LargeBinary, true),
                common::PrimitiveType::Variant,
                DataType::LargeBinary,
                None,
            ),
            (
                Field::new("large_int", DataType::FixedSizeBinary(16), true),
                common::PrimitiveType::Largeint,
                DataType::FixedSizeBinary(16),
                None,
            ),
        ];

        for (field, expected_primitive, expected_type, expected_logical) in cases {
            let encoded = encode_field_type(&field).expect("encode logical field");
            assert_eq!(scalar_primitive(&encoded), expected_primitive);

            let decoded = decode_field_type(field.name(), field.is_nullable(), &encoded)
                .expect("decode field");
            assert_eq!(decoded.data_type(), &expected_type);
            assert_eq!(logical_type_of_field(&decoded), expected_logical);
        }
    }

    #[test]
    fn nested_logical_field_metadata_survives_decode_type() {
        let data_type = DataType::Struct(Fields::from(vec![Arc::new(field_with_logical_type(
            Field::new("payload", DataType::Utf8, true),
            LogicalType::Json,
        ))]));

        let encoded = encode_type(&data_type).expect("encode struct with logical child");
        let decoded = decode_type(&roundtrip_message(&encoded)).expect("decode logical child");

        let DataType::Struct(fields) = decoded else {
            panic!("expected struct");
        };
        assert_eq!(fields[0].data_type(), &DataType::Utf8);
        assert_eq!(
            logical_type_of_field(fields[0].as_ref()),
            Some(LogicalType::Json)
        );
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
        let Some(common::literal_value::Value::DecimalValue(value)) =
            lit.value.and_then(|v| v.value)
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
        let Some(common::literal_value::Value::DecimalValue(value)) =
            lit.value.and_then(|v| v.value)
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
    fn invalid_decimal_type_widths_are_rejected() {
        let err = encode_type(&DataType::Decimal128(39, 0))
            .expect_err("Decimal128 precision above 38 must fail");
        assert!(err.contains("Decimal128"));
        assert!(err.contains("precision"));

        let err = encode_type(&DataType::Decimal256(77, 0))
            .expect_err("Decimal256 precision above 76 must fail");
        assert!(err.contains("Decimal256"));
        assert!(err.contains("precision"));
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
            TypedExpr {
                kind: ExprKind::Lambda {
                    params: vec!["x".to_string()],
                    body: Box::new(lambda_body),
                },
                data_type: DataType::Int64,
                nullable: true,
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

        assert_eq!(names.len(), 18);
        assert_eq!(names.iter().filter(|&&name| name == "lambda").count(), 2);
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
        let Some(expr::expr::Kind::Cast(cast)) = encode_expr(&cast).expect("encode cast").kind
        else {
            panic!("expected cast");
        };
        assert_eq!(
            scalar_primitive(cast.target.as_ref().expect("target type")),
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

    #[test]
    fn unsupported_timestamp_unit_reports_clear_error() {
        let err = encode_type(&DataType::Timestamp(TimeUnit::Second, None))
            .expect_err("second timestamp rejected");

        assert!(err.contains("unsupported timestamp unit"));
    }

    #[test]
    fn unsupported_time64_unit_reports_clear_error() {
        let err = encode_type(&DataType::Time64(TimeUnit::Nanosecond))
            .expect_err("nanosecond Time64 rejected");

        assert!(err.contains("unsupported Time64 unit"));
    }
}
